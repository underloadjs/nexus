//! Worker selection (task §8): capability match → ready → declared capacity →
//! KV budget → tier → conversation affinity → least loaded, with a deterministic
//! tie-break.
//!
//! The scheduler is a **pure function of actor state**: it never mutates, so the
//! actor remains the only place job state changes (§5). Determinism matters twice
//! over here — for reproducible tests, and because a scheduler that flips between
//! two equal workers on every call destroys the prompt-cache affinity that task
//! §8 calls "the largest quality lever".

use std::collections::BTreeMap;

use crate::ids::PoolId;
use crate::ids::WorkerId;
use crate::model::JobRecord;
use crate::model::Priority;
use crate::model::WorkerRecord;
use crate::model::WorkerTier;
use crate::util::fnv1a;

/// Context size at or above which traffic is treated as long-context and steered
/// to dedicated hosts (task §8: prefer dedicated for long-context traffic).
pub const LONG_CONTEXT_TOKENS: u64 = 128_000;

/// One worker that survived every hard filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    /// The worker.
    pub worker: WorkerId,
    /// True when this worker is the affinity target for the job's session.
    pub affinity: bool,
    /// True when this worker is dedicated *and* the job prefers dedicated.
    pub preferred_tier: bool,
    /// Slots the worker can still take.
    pub free_slots: u32,
    /// Free KV budget, when reported.
    pub free_kv_tokens: Option<u64>,
    /// Availability tier.
    pub tier: WorkerTier,
}

/// Deterministic affinity score for (session, worker) via rendezvous hashing.
///
/// Rendezvous (highest-random-score) hashing is chosen over a hash ring because
/// its mapping changes only for the keys of a *removed* worker. That is exactly
/// §8's "invalidate the mapping on worker death": every other session keeps its
/// worker and therefore its warm prompt cache.
#[must_use]
pub fn rendezvous_score(session: &str, worker: &str) -> u64 {
    // Domain-separate the two inputs so (a,b) and (b,a) cannot collide.
    let mut bytes = Vec::with_capacity(session.len() + worker.len() + 1);
    bytes.extend_from_slice(session.as_bytes());
    bytes.push(0xff);
    bytes.extend_from_slice(worker.as_bytes());
    fnv1a(&bytes)
}

/// The worker a session should land on if it is still connected.
#[must_use]
pub fn affinity_target(
    session: &str,
    workers: &BTreeMap<WorkerId, WorkerRecord>,
) -> Option<WorkerId> {
    let mut best: Option<(u64, &WorkerId)> = None;
    for (id, w) in workers {
        if !w.dispatchable() {
            continue;
        }
        let score = rendezvous_score(session, id.as_ref());
        let better = match best {
            // Higher score wins; equal scores fall back to the lowest id so the
            // target is a total function of the worker set.
            Some((b, bid)) => score > b || (score == b && id < bid),
            None => true,
        };
        if better {
            best = Some((score, id));
        }
    }
    best.map(|(_, id)| id.clone())
}

/// Whether this job should be steered to a dedicated host.
#[must_use]
pub fn prefers_dedicated(job: &JobRecord) -> bool {
    job.priority == Priority::Realtime
        || job
            .requires
            .kv_estimate_tokens
            .is_some_and(|v| v >= LONG_CONTEXT_TOKENS)
        || job
            .requires
            .min_context_tokens
            .is_some_and(|v| v >= LONG_CONTEXT_TOKENS)
}

/// Hard filters: live + ready + in this pool + model match + capacity +
/// tier policy + "prefer a different worker" on requeue. Routing is by model
/// alone: the payload is the consumer's, and whatever the engine cannot serve
/// it answers itself (the hub relays that error).
///
/// `queue_depth` implements §11's "ephemeral workers are burst capacity": an
/// ephemeral worker is only used when there is work waiting or when it is the
/// only capable worker.
#[must_use]
pub fn eligible(
    workers: &BTreeMap<WorkerId, WorkerRecord>,
    pool: &PoolId,
    job: &JobRecord,
    queue_depth: usize,
) -> Vec<Candidate> {
    let prefer_dedicated = prefers_dedicated(job);
    // Computed once: the affinity target for this job's session, if any.
    let affinity = job
        .session_hint
        .as_deref()
        .and_then(|session| affinity_target(session, workers));

    let all: Vec<Candidate> = workers
        .values()
        .filter(|w| w.pools.contains(pool))
        .filter(|w| w.dispatchable())
        // Model-only routing: the supplier registered its model, the consumer
        // requested it, and the payload is proxied verbatim - what the engine
        // cannot serve it answers itself, and the hub relays that error.
        .filter(|w| job.requires.serves_model(&w.requires))
        // Declared capacity (acceptance test 12).
        .filter(|w| w.available_slots() > 0)
        .map(|w| Candidate {
            // The candidate identity is part of the ranking payload: `without_failed`
            // below filters on it, and the actor dispatches to it by name.
            worker: w.id.clone(),
            affinity: affinity.as_ref() == Some(&w.id),
            preferred_tier: prefer_dedicated && w.tier == WorkerTier::Dedicated,
            free_slots: w.available_slots(),
            free_kv_tokens: w.free_kv_tokens,
            tier: w.tier,
        })
        .collect();

    // Requeue preference: "prefer a different worker". A preference, not a rule —
    // if the failed worker is the only capable one, retrying it still beats
    // failing the job, and `Limits::max_attempts` bounds the loop.
    let without_failed: Vec<Candidate> = all
        .iter()
        .filter(|c| Some(&c.worker) != job.last_worker.as_ref())
        .cloned()
        .collect();
    let mut raw = if without_failed.is_empty() {
        all.clone()
    } else {
        without_failed
    };

    // Ephemeral spill rule: with an empty queue, keep ephemeral hosts out — but
    // only when a dedicated worker is actually eligible, so an ephemeral-only
    // pool (a Kaggle burst host with no dedicated peer) is never starved.
    if queue_depth == 0 && all.iter().any(|c| c.tier == WorkerTier::Dedicated) {
        raw.retain(|c| c.tier != WorkerTier::Ephemeral);
    }
    raw
}

/// Rank candidates. Earlier entries win. The order is:
/// affinity → tier preference → most free slots (least loaded) → worker id.
#[must_use]
pub fn rank(candidates: &[Candidate]) -> Vec<Candidate> {
    let mut sorted = candidates.to_vec();
    sorted.sort_by(|a, b| {
        b.affinity
            .cmp(&a.affinity)
            .then(b.preferred_tier.cmp(&a.preferred_tier))
            .then(b.free_slots.cmp(&a.free_slots))
            // Ephemeral-first as a spill tie-break: keep dedicated hosts idle
            // for the latency-sensitive traffic they are there for.
            .then_with(|| match (a.tier, b.tier) {
                (WorkerTier::Ephemeral, WorkerTier::Dedicated) => std::cmp::Ordering::Less,
                (WorkerTier::Dedicated, WorkerTier::Ephemeral) => std::cmp::Ordering::Greater,
                _ => std::cmp::Ordering::Equal,
            })
            .then_with(|| a.worker.cmp(&b.worker))
    });
    sorted
}

/// Best worker for a job, or `None` when nobody can take it right now.
#[must_use]
pub fn pick(
    workers: &BTreeMap<WorkerId, WorkerRecord>,
    pool: &PoolId,
    job: &JobRecord,
    queue_depth: usize,
) -> Option<WorkerId> {
    rank(&eligible(workers, pool, job, queue_depth))
        .into_iter()
        .next()
        .map(|c| c.worker)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture;
    use std::collections::BTreeSet;

    /// Worker with the reference capability offer (qwen38, 512K, tools).
    fn w(
        id: &str,
        tier: WorkerTier,
        free_slots: u32,
        free_kv: Option<u64>,
        ready: bool,
    ) -> WorkerRecord {
        fixture::worker(id, tier, 4, free_slots, free_kv, ready)
    }

    fn job() -> JobRecord {
        fixture::job_record("job-1")
    }

    fn pool() -> PoolId {
        fixture::pool_id()
    }

    #[test]
    fn loading_and_full_workers_are_never_candidates() {
        let workers = BTreeMap::from([
            (
                WorkerId::new_unchecked("w-loading"),
                w("w-loading", WorkerTier::Dedicated, 4, None, false),
            ),
            (
                WorkerId::new_unchecked("w-full"),
                w("w-full", WorkerTier::Dedicated, 0, None, true),
            ),
        ]);
        assert_eq!(eligible(&workers, &pool(), &job(), 1).len(), 0);
        assert_eq!(pick(&workers, &pool(), &job(), 1), None);
    }

    #[test]
    fn least_loaded_wins_and_the_tie_break_is_the_worker_id() {
        let workers = BTreeMap::from([
            (
                WorkerId::new_unchecked("w-busy"),
                w("w-busy", WorkerTier::Dedicated, 1, Some(10), true),
            ),
            (
                WorkerId::new_unchecked("w-idle"),
                w("w-idle", WorkerTier::Dedicated, 3, Some(10), true),
            ),
        ]);
        assert_eq!(
            pick(&workers, &pool(), &job(), 1),
            Some(WorkerId::new_unchecked("w-idle"))
        );

        // Perfect tie → lowest id, deterministically.
        let tied = BTreeMap::from([
            (
                WorkerId::new_unchecked("w-z"),
                w("w-z", WorkerTier::Dedicated, 2, None, true),
            ),
            (
                WorkerId::new_unchecked("w-a"),
                w("w-a", WorkerTier::Dedicated, 2, None, true),
            ),
        ]);
        for _ in 0..32 {
            assert_eq!(
                pick(&tied, &pool(), &job(), 1),
                Some(WorkerId::new_unchecked("w-a"))
            );
        }
    }

    #[test]
    fn kv_budget_is_advertisement_not_a_gate() {
        let mut j = job();
        j.requires.kv_estimate_tokens = Some(300_000);
        let workers = BTreeMap::from([
            (
                WorkerId::new_unchecked("w-tight"),
                w("w-tight", WorkerTier::Dedicated, 4, Some(120_000), true),
            ),
            (
                WorkerId::new_unchecked("w-wide"),
                w("w-wide", WorkerTier::Dedicated, 1, Some(400_000), true),
            ),
        ]);
        // Routing is model-only: the KV estimate is metadata, not a filter. The
        // choice falls to declared capacity (4 free slots beat 1); a prompt the
        // engine cannot fit is the ENGINE's error, and the hub relays it.
        assert_eq!(
            pick(&workers, &pool(), &j, 1),
            Some(WorkerId::new_unchecked("w-tight"))
        );

        // A worker that reports no KV figure is not excluded outright.
        let blind = BTreeMap::from([(
            WorkerId::new_unchecked("w-blind"),
            w("w-blind", WorkerTier::Dedicated, 1, None, true),
        )]);
        assert_eq!(
            pick(&blind, &pool(), &j, 1),
            Some(WorkerId::new_unchecked("w-blind"))
        );
    }

    #[test]
    fn long_context_prefers_dedicated_even_when_a_bigger_ephemeral_exists() {
        let mut j = job();
        j.requires.kv_estimate_tokens = Some(200_000);
        let workers = BTreeMap::from([
            (
                WorkerId::new_unchecked("w-eph"),
                w("w-eph", WorkerTier::Ephemeral, 4, Some(900_000), true),
            ),
            (
                WorkerId::new_unchecked("w-ded"),
                w("w-ded", WorkerTier::Dedicated, 2, Some(900_000), true),
            ),
        ]);
        assert!(prefers_dedicated(&j));
        assert_eq!(
            pick(&workers, &pool(), &j, 4),
            Some(WorkerId::new_unchecked("w-ded"))
        );

        // Realtime priority also prefers dedicated.
        let mut rt = job();
        rt.priority = Priority::Realtime;
        assert_eq!(
            pick(&workers, &pool(), &rt, 4),
            Some(WorkerId::new_unchecked("w-ded"))
        );
    }

    #[test]
    fn ephemeral_is_burst_only_when_the_queue_is_empty() {
        let workers = BTreeMap::from([
            (
                WorkerId::new_unchecked("w-eph"),
                w("w-eph", WorkerTier::Ephemeral, 4, None, true),
            ),
            (
                WorkerId::new_unchecked("w-ded"),
                w("w-ded", WorkerTier::Dedicated, 4, None, true),
            ),
        ]);
        // Queue empty → dedicated carries the floor.
        assert_eq!(
            pick(&workers, &pool(), &job(), 0),
            Some(WorkerId::new_unchecked("w-ded"))
        );
        // Queue non-empty and same free capacity → ephemeral takes the spill so
        // the dedicated host stays free for latency-sensitive work.
        assert_eq!(
            pick(&workers, &pool(), &job(), 3),
            Some(WorkerId::new_unchecked("w-eph"))
        );
        // Dedicated alone in the pool with an empty queue still gets the job.
        let only_ded = BTreeMap::from([(
            WorkerId::new_unchecked("w-ded"),
            w("w-ded", WorkerTier::Dedicated, 4, None, true),
        )]);
        assert_eq!(
            pick(&only_ded, &pool(), &job(), 0),
            Some(WorkerId::new_unchecked("w-ded"))
        );
        // Ephemeral-only pool: the tier rule must not starve it.
        let only_eph = BTreeMap::from([(
            WorkerId::new_unchecked("w-eph"),
            w("w-eph", WorkerTier::Ephemeral, 4, None, true),
        )]);
        assert_eq!(
            pick(&only_eph, &pool(), &job(), 0),
            Some(WorkerId::new_unchecked("w-eph"))
        );
    }

    #[test]
    fn session_affinity_overrides_least_loaded_and_follows_the_worker() {
        let workers = BTreeMap::from([
            (
                WorkerId::new_unchecked("w-cache"),
                w("w-cache", WorkerTier::Dedicated, 1, None, true),
            ),
            (
                WorkerId::new_unchecked("w-free"),
                w("w-free", WorkerTier::Dedicated, 4, None, true),
            ),
        ]);
        let session = "sess-42";
        let target = affinity_target(session, &workers).expect("a target");
        let mut j = job();
        j.session_hint = Some(session.to_string());
        // Whichever worker the hash picked, that is the choice despite less free
        // capacity: re-prefilling 100K tokens costs far more than the queue hop.
        assert_eq!(pick(&workers, &pool(), &j, 1), Some(target.clone()));
        assert!(
            target == WorkerId::new_unchecked("w-cache")
                || target == WorkerId::new_unchecked("w-free")
        );

        // Rendezvous stability: removing a worker moves only the sessions that
        // were hashed to it. With two workers, the sessions on the *other*
        // worker must keep their worker after the target leaves.
        let other = if target == WorkerId::new_unchecked("w-cache") {
            WorkerId::new_unchecked("w-free")
        } else {
            WorkerId::new_unchecked("w-cache")
        };
        let without_target: BTreeMap<WorkerId, WorkerRecord> = workers
            .iter()
            .filter(|(id, _)| *id != &target)
            .map(|(id, rec)| (id.clone(), rec.clone()))
            .collect();
        let probe = (0u32..64)
            .map(|i| format!("sess-{i}"))
            .find(|s| affinity_target(s, &workers).as_ref() == Some(&other))
            .expect("some session must land on the non-target worker");
        assert_eq!(
            affinity_target(&probe, &without_target).as_ref(),
            Some(&other)
        );
        // The evicted session is the only one that moves, and it moves to the
        // surviving worker (§8: "invalidate the mapping on worker death").
        assert_eq!(
            affinity_target(session, &without_target).as_ref(),
            Some(&other)
        );
        assert_eq!(affinity_target(session, &BTreeMap::new()), None);
    }

    #[test]
    fn capability_requirements_are_not_a_gate() {
        let mut j = job();
        j.requires.capabilities = BTreeSet::from(["vision".to_string()]);
        let workers = BTreeMap::from([(
            WorkerId::new_unchecked("w-tools"),
            w("w-tools", WorkerTier::Dedicated, 4, None, true),
        )]);
        // Model-only routing: the payload is proxied even when the consumer asks
        // for capabilities the worker never advertised - an engine that cannot
        // serve it answers the error itself, and the hub relays it.
        assert_eq!(
            pick(&workers, &pool(), &j, 1),
            Some(WorkerId::new_unchecked("w-tools"))
        );
    }

    #[test]
    fn requeue_avoids_the_failed_worker_but_not_the_only_option() {
        let workers = BTreeMap::from([
            (
                WorkerId::new_unchecked("w-dead"),
                w("w-dead", WorkerTier::Dedicated, 4, None, true),
            ),
            (
                WorkerId::new_unchecked("w-live"),
                w("w-live", WorkerTier::Dedicated, 4, None, true),
            ),
        ]);
        let mut j = job();
        j.last_worker = Some(WorkerId::new_unchecked("w-dead"));
        assert_eq!(
            pick(&workers, &pool(), &j, 1),
            Some(WorkerId::new_unchecked("w-live"))
        );

        // Only the failed worker remains: retrying beats failing for an
        // idempotent kind.
        let only_failed = BTreeMap::from([(
            WorkerId::new_unchecked("w-dead"),
            w("w-dead", WorkerTier::Dedicated, 4, None, true),
        )]);
        assert_eq!(
            pick(&only_failed, &pool(), &j, 1),
            Some(WorkerId::new_unchecked("w-dead"))
        );
    }

    #[test]
    fn score_is_stable_and_sensitivity_checked() {
        assert_eq!(rendezvous_score("s", "w1"), rendezvous_score("s", "w1"));
        assert_ne!(rendezvous_score("s", "w1"), rendezvous_score("w1", "s"));
        assert_ne!(rendezvous_score("s", "w1"), rendezvous_score("s2", "w1"));
    }
}
