//! The queue module behind a trait (task §13): the in-process implementation is
//! replaceable **without touching the actor**, which is what keeps multi-replica
//! durability a local change rather than a rewrite.
//!
//! # Backend mapping (documented, not implemented here)
//!
//! | Method | Redis Streams | Postgres | NATS JetStream |
//! | --- | --- | --- | --- |
//! | [`QueueBackend::enqueue`] | `XADD` | `INSERT` | `publish` |
//! | [`QueueBackend::claim`] | `XREADGROUP` | `SELECT … FOR UPDATE SKIP LOCKED` | pull `NoAck` |
//! | [`QueueBackend::requeue`] | `XACK` + `XADD` | `UPDATE … RETURNING` | `nack` / redeliver |
//! | lease expiry | `XAUTOCLAIM` | `WHERE lease_until < now` | `AckExplicit` + ack-wait |
//!
//! Those backends are I/O, so a real adapter is *async*; this trait is
//! deliberately synchronous and non-blocking so the actor never awaits inside a
//! state transition. An async backend satisfies it by executing one round-trip
//! per call off the actor thread (`spawn_blocking` / a pre-fetched claim
//! batch), which is exactly how the native runtime would drive Redis. The
//! actor's claim+lease semantics stay identical either way.

use std::collections::BTreeMap;

use crate::ids::JobId;
use crate::model::Priority;
use crate::time::Timestamp;

/// Why an enqueue was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueError {
    /// The bounded depth was reached (task §17.3: answer `429`, never grow
    /// without bound).
    Full {
        /// Configured depth.
        limit: usize,
    },
    /// The job is already queued; queuing it twice would double-dispatch it.
    Duplicate,
}

/// Read-only queue view for metrics and `/healthz`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueueSnapshot {
    /// Total queued jobs.
    pub depth: usize,
    /// Depth per priority class.
    pub by_class: BTreeMap<Priority, usize>,
    /// Earliest dispatch deadline among queued jobs, for the 503-on-timeout path.
    pub oldest_deadline_ms: Option<Timestamp>,
    /// Configured depth, so a consumer of metrics can compute saturation.
    pub limit: usize,
}

/// A pool's queue. One instance per [`crate::actor::PoolActor`].
///
/// `Send + Sync` is part of the contract, not an implementation detail: the backend
/// is boxed inside the actor, and the native runtime spawns that actor as a tokio
/// task on a multi-thread runtime. The task holds `Box<dyn QueueBackend>` across
/// await points (so it must be `Send`), and the effect dispatcher borrows
/// `&PoolActor` across awaits (so the actor, and therefore the box, must also be
/// `Sync`). A single-threaded WASM host satisfies both trivially, and a
/// Redis/Postgres/NATS backend client is `Send + Sync` anyway; an exotic backend
/// that is neither has to be wrapped before it can be plugged in.
pub trait QueueBackend: Send + Sync {
    /// Add a job. Implementations must reject when full and when duplicated.
    ///
    /// `seq` is the arrival sequence the actor assigns, so FIFO order is
    /// decided by the actor (single authority) rather than by each backend.
    fn enqueue(
        &mut self,
        job: &JobId,
        priority: Priority,
        seq: u64,
        deadline_ms: Timestamp,
    ) -> Result<(), QueueError>;

    /// Take the next job: highest class, then FIFO. `None` when empty.
    fn claim(&mut self) -> Option<JobId>;

    /// Put a job back after a requeue. Appends at the **back** of its class so
    /// a worker dying cannot jump the line ahead of jobs that waited longer.
    fn requeue(
        &mut self,
        job: &JobId,
        priority: Priority,
        seq: u64,
        deadline_ms: Timestamp,
    ) -> Result<(), QueueError>;

    /// Remove a specific job (cancel while queued). `true` if it was queued.
    fn remove(&mut self, job: &JobId) -> bool;

    /// Whether this job is currently queued.
    fn contains(&self, job: &JobId) -> bool;

    /// The first `limit` queued jobs in dispatch order (class, then FIFO). The
    /// actor uses this to match a pulling worker against the head of the queue
    /// without removing jobs it cannot serve yet.
    fn ordered(&self, limit: usize) -> Vec<JobId>;

    /// Current depth.
    fn depth(&self) -> usize;

    /// Metrics view.
    fn snapshot(&self, now_ms: Timestamp) -> QueueSnapshot;

    /// Drop jobs whose dispatch deadline has passed, returning them so the
    /// actor can emit their `503`.
    fn purge_expired(&mut self, now_ms: Timestamp) -> Vec<JobId>;

    /// Remove and return everything (pool teardown / hub shutdown).
    fn drain(&mut self) -> Vec<JobId>;
}

/// Bounded, deterministic in-process queue: the M0–M3 implementation, and the
/// reference semantics any remote backend must reproduce.
#[derive(Debug, Clone)]
pub struct InMemoryQueue {
    entries: BTreeMap<(u8, u64), JobId>,
    index: BTreeMap<JobId, (u8, u64, Timestamp)>,
    limit: usize,
}

impl InMemoryQueue {
    /// Queue with a hard depth of `limit`.
    #[must_use]
    pub fn new(limit: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            index: BTreeMap::new(),
            limit: limit.max(1),
        }
    }

    /// Configured bound.
    #[must_use]
    pub fn limit(&self) -> usize {
        self.limit
    }

    fn insert(
        &mut self,
        job: &JobId,
        priority: Priority,
        seq: u64,
        deadline_ms: Timestamp,
    ) -> Result<(), QueueError> {
        if self.index.contains_key(job) {
            return Err(QueueError::Duplicate);
        }
        if self.entries.len() >= self.limit {
            return Err(QueueError::Full { limit: self.limit });
        }
        let key = (priority as u8, seq);
        self.entries.insert(key, job.clone());
        self.index
            .insert(job.clone(), (priority as u8, seq, deadline_ms));
        Ok(())
    }
}

impl QueueBackend for InMemoryQueue {
    fn enqueue(
        &mut self,
        job: &JobId,
        priority: Priority,
        seq: u64,
        deadline_ms: Timestamp,
    ) -> Result<(), QueueError> {
        self.insert(job, priority, seq, deadline_ms)
    }

    fn claim(&mut self) -> Option<JobId> {
        let (&key, _) = self.entries.iter().next()?;
        let job = self.entries.remove(&key)?;
        self.index.remove(&job);
        Some(job)
    }

    fn requeue(
        &mut self,
        job: &JobId,
        priority: Priority,
        seq: u64,
        deadline_ms: Timestamp,
    ) -> Result<(), QueueError> {
        // A requeue must not duplicate an entry that was never removed.
        self.insert(job, priority, seq, deadline_ms)
    }

    fn remove(&mut self, job: &JobId) -> bool {
        match self.index.remove(job) {
            Some((class, seq, _)) => {
                self.entries.remove(&(class, seq));
                true
            }
            None => false,
        }
    }

    fn contains(&self, job: &JobId) -> bool {
        self.index.contains_key(job)
    }

    fn ordered(&self, limit: usize) -> Vec<JobId> {
        self.entries.values().take(limit).cloned().collect()
    }

    fn depth(&self) -> usize {
        self.entries.len()
    }

    fn snapshot(&self, _now_ms: Timestamp) -> QueueSnapshot {
        let mut by_class: BTreeMap<Priority, usize> = BTreeMap::new();
        let mut oldest: Option<Timestamp> = None;
        // Index values are `(priority, seq, deadline)`; only the deadline matters
        // for the oldest-deadline gauge, so the other two are skipped by name.
        for (_, _, deadline) in self.index.values() {
            // class is recovered from the entry key below; deadlines scanned here
            oldest = Some(oldest.map_or(*deadline, |o: Timestamp| o.min(*deadline)));
        }
        for (class, _seq) in self.entries.keys() {
            let priority = match class {
                0 => Priority::Realtime,
                2 => Priority::Bulk,
                _ => Priority::Default,
            };
            *by_class.entry(priority).or_insert(0) += 1;
        }
        QueueSnapshot {
            depth: self.entries.len(),
            by_class,
            oldest_deadline_ms: oldest,
            limit: self.limit,
        }
    }

    fn purge_expired(&mut self, now_ms: Timestamp) -> Vec<JobId> {
        let stale: Vec<JobId> = self
            .index
            .iter()
            .filter(|(_, (_, _, deadline))| *deadline <= now_ms)
            .map(|(job, _)| job.clone())
            .collect();
        for job in &stale {
            self.remove(job);
        }
        stale
    }

    fn drain(&mut self) -> Vec<JobId> {
        let mut jobs: Vec<JobId> = self.entries.values().cloned().collect();
        jobs.sort();
        self.entries.clear();
        self.index.clear();
        jobs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(id: &str) -> JobId {
        JobId::new_unchecked(id)
    }

    #[test]
    fn order_is_class_then_fifo_exactly() {
        let mut q = InMemoryQueue::new(8);
        // Arrival order deliberately interleaves classes.
        q.enqueue(&job("a"), Priority::Default, 1, 1_000)
            .expect("ok");
        q.enqueue(&job("b"), Priority::Bulk, 2, 1_000).expect("ok");
        q.enqueue(&job("c"), Priority::Realtime, 3, 1_000)
            .expect("ok");
        q.enqueue(&job("d"), Priority::Default, 4, 1_000)
            .expect("ok");
        let order = std::iter::from_fn(|| q.claim()).collect::<Vec<_>>();
        assert_eq!(
            order,
            vec![job("c"), job("a"), job("d"), job("b")],
            "realtime first, then FIFO inside each class"
        );
        assert_eq!(q.depth(), 0);
    }

    #[test]
    fn depth_bound_is_exact_and_reports_the_limit() {
        let mut q = InMemoryQueue::new(2);
        assert_eq!(q.enqueue(&job("a"), Priority::Default, 1, 0), Ok(()));
        assert_eq!(q.enqueue(&job("b"), Priority::Default, 2, 0), Ok(()));
        assert_eq!(
            q.enqueue(&job("c"), Priority::Default, 3, 0),
            Err(QueueError::Full { limit: 2 })
        );
        assert_eq!(q.depth(), 2);
    }

    #[test]
    fn duplicate_enqueue_is_refused() {
        let mut q = InMemoryQueue::new(4);
        q.enqueue(&job("a"), Priority::Default, 1, 0).expect("ok");
        assert_eq!(
            q.enqueue(&job("a"), Priority::Default, 2, 0),
            Err(QueueError::Duplicate)
        );
        assert_eq!(
            q.requeue(&job("a"), Priority::Default, 3, 0),
            Err(QueueError::Duplicate)
        );
        assert_eq!(q.depth(), 1, "the refusal must not have added anything");
    }

    #[test]
    fn requeue_appends_behind_older_same_class_work() {
        let mut q = InMemoryQueue::new(4);
        q.enqueue(&job("first"), Priority::Default, 1, 0)
            .expect("ok");
        // "second" was dispatched (never in the queue), then requeued with a
        // later seq than "first": it must land behind it.
        q.requeue(&job("second"), Priority::Default, 2, 0)
            .expect("ok");
        assert_eq!(
            std::iter::from_fn(|| q.claim()).collect::<Vec<_>>(),
            vec![job("first"), job("second")]
        );
    }

    #[test]
    fn purge_and_remove_and_snapshot() {
        let mut q = InMemoryQueue::new(4);
        q.enqueue(&job("soon"), Priority::Realtime, 1, 100)
            .expect("ok");
        q.enqueue(&job("later"), Priority::Bulk, 2, 5_000)
            .expect("ok");
        assert_eq!(q.purge_expired(200), vec![job("soon")]);
        assert_eq!(q.depth(), 1);
        assert!(q.contains(&job("later")));
        assert!(q.remove(&job("later")));
        assert!(!q.remove(&job("later")));
        assert_eq!(
            q.snapshot(0),
            QueueSnapshot {
                depth: 0,
                by_class: BTreeMap::new(),
                oldest_deadline_ms: None,
                limit: 4
            }
        );

        // Snapshot with content, exactly.
        let mut q2 = InMemoryQueue::new(3);
        q2.enqueue(&job("x"), Priority::Bulk, 1, 9_000).expect("ok");
        q2.enqueue(&job("y"), Priority::Bulk, 2, 7_000).expect("ok");
        let snap = q2.snapshot(0);
        let mut by_class = BTreeMap::new();
        by_class.insert(Priority::Bulk, 2);
        assert_eq!(
            snap,
            QueueSnapshot {
                depth: 2,
                by_class,
                oldest_deadline_ms: Some(7_000),
                limit: 3
            }
        );
    }

    #[test]
    fn drain_returns_sorted_ids_and_empties() {
        let mut q = InMemoryQueue::new(4);
        q.enqueue(&job("z"), Priority::Default, 1, 0).expect("ok");
        q.enqueue(&job("a"), Priority::Default, 2, 0).expect("ok");
        assert_eq!(q.drain(), vec![job("a"), job("z")]);
        assert_eq!(q.depth(), 0);
        assert_eq!(q.claim(), None);
    }
}
