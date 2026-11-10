//! `GET /v1/observe` — one read-only document describing the whole hub.
//!
//! # Why this endpoint exists
//!
//! The monitoring UI is a **static** bundle hosted on GitHub Pages, so it calls
//! this hub cross-origin with no session and no API key. It used to have to
//! reverse-engineer hub state from three places at once — `/healthz` (counts
//! only), `/metrics` (a Prometheus text exposition the browser has to parse) and
//! a per-job `GET /v1/jobs/{id}` probe that costs a mailbox round trip each.
//! This endpoint gives the dashboard one stable request that answers every
//! question the dashboard asks: is the hub healthy, which pools are up, which
//! workers are attached and how loaded are they, which jobs are running, and
//! which consumer sockets are attached to which jobs.
//!
//! # Why it is open
//!
//! The payload is aggregated pool state plus job *metadata* — no prompt text, no
//! streamed content, no artifact bytes, no credentials. It is the same class of
//! information `/healthz` and `/metrics` already publish without a key
//! (`main.rs`, the "operations: open on purpose" route group), and the README's
//! security section carries the matching privacy note: worker ids, job ids, kind
//! names and tenant ids become readable by whoever can reach the port.
//!
//! # Why the actor is asked rather than the hub's own state read
//!
//! [`nexus_core::actor`] is the **single owner** of worker liveness, leases and
//! job phase; the hub mirrors gauges only so `/healthz` and `/metrics` never
//! disagree with it (`state.rs`, `PoolHandle::gauges`). Reading the mirrors here
//! too would mean a document that mixes two clocks and two generations, and a
//! worker that has just dropped can still appear in a mirror for one dispatch.
//! So [`Hub::pool_snapshots`] sends one [`nexus_core::actor::ActorMsg::Observe`]
//! per pool — a read *through* the mailbox, which is the identical serialized
//! read `GET /v1/jobs/{id}` already relies on (`state.rs`, `Hub::job_snapshot`) —
//! and the actor answers [`nexus_core::actor::Effect::Observed`] built by
//! [`nexus_core::actor::PoolActor::pool_snapshot`] (`actor.rs:784`).
//!
//! # Deadline model, and why it never blocks the dashboard
//!
//! A pool actor under a burst may not reach an `Observe` for a while, and a hub
//! that waits for it turns a monitoring request into a hang. So
//! `Hub::pool_snapshot` wraps the whole probe (mailbox send *and* answer) in one
//! `PROBE_TIMEOUT_MS` deadline (`state.rs:683`) and returns `None` for "did not
//! answer" — and the handler **always** answers `200`: a dashboard polling a hub
//! with zero ready workers, or a wedged actor, still gets the pools it can see,
//! the mirrored gauges for the rest, and an honest `unresponsive_pools` list
//! naming what is missing. `None` deliberately does not mean "empty pool":
//! a monitor that gets nothing cannot tell a busy actor from a dead hub.
//!
//! # Clock translation
//!
//! The actor is clock-injected (`nexus-core/src/time.rs`) and the native hub
//! feeds it [`crate::state::monotonic_ms`], i.e. milliseconds since an `Instant`
//! taken at process start — **not** epoch time. So `last_seen_ms`, `accept_ms`,
//! `first_frame_ms` and a lease `deadline_ms` are all process-relative. A
//! document that mixed epoch `now_ms` with them would render "last seen 56 years
//! ago", so [`Clock`] translates every actor timestamp to epoch on the way out,
//! through the one implementation of that arithmetic ([`crate::state::actor_to_wall_ms`],
//! which is also what `Hub::to_wall_ms` calls).
//!
//! # The wire contract
//!
//! Fixed by the monitoring UI (`frontend/src/api/nexus-client.ts` is its other
//! half): `protocol` is exactly `nexus-hub-observe/v1`; the eight top-level keys
//! `now_ms`, `readiness`, `pools`, `workers`, `jobs`, `leases`,
//! `consumer_connections`, `unresponsive_pools` are always present (empty arrays,
//! never `null`); `readiness` re-uses `/healthz`'s field names
//! (`consumer.rs:928-937`) so a client can read one rule for both. Extra fields are
//! additive and allowed; missing or renamed contract fields are a breaking change.
//! The contract field names are exactly what the UI reads — `pools[].id`,
//! `workers[].free_slots`/`free_kv_tokens`/`in_flight`(array of job ids)/
//! `outstanding_claims`/`telemetry`, `jobs[].lease`, and
//! `consumer_connections[].attached_jobs` — so those names are load-bearing.
//!
//! Deliberate additions and departures from the contract *example*, all documented
//! rather than silent:
//!   * `jobs[].phase` keeps the actor's vocabulary (`pending|queued|assigned|
//!     streaming|finished|failed|cancelled`, `model.rs:477`) instead of
//!     collapsing to `"running"`, because the distinction between queued and
//!     leased is the dashboard's whole point; `jobs[].running` is the boolean a
//!     client that only wants "in flight" can read. The UI styles unknown phases
//!     as a neutral chip, so `assigned` renders rather than breaking.
//!   * `leases` is a whole extra section (one row per live lease, keyed by job) so
//!     a graph view can join worker→job without re-reading each job row;
//!   * `workers[].resumable` and `workers[].available_slots` are the actor's own
//!     derived numbers (`WorkerRecord::available_slots` — a heartbeat that claims
//!     all slots free cannot over-report), and `jobs[]` carries `kind`, `attempt`,
//!     `priority`, `streaming`, `payload_bytes`, `delivered_bytes` and the frame
//!     timings for the drill-down table;
//!   * a connection row's `peer` is `null` because `axum::serve` does not install
//!     `ConnectInfo` (`state.rs`, `ConsumerConnection::peer`).

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use nexus_core::model::{JobPhase, JobView, Lease, LinkState, PoolSnapshot, WorkerView};
use serde_json::{json, Value};

use crate::consumer;
use crate::state::{ConsumerConnection, Hub, PoolGauges};

/// Version marker the dashboard checks before parsing.
///
/// A second hub would only break the dashboard by renaming or dropping a key, so
/// the hub announces which contract it is serving rather than guessing.
pub const OBSERVE_PROTOCOL: &str = "nexus-hub-observe/v1";

/// The actor's own **active** job set, restated here because the core exposes only
/// the complement ([`JobPhase::is_terminal`], `model.rs:497`). `PoolActor::
/// pool_snapshot` includes exactly the non-terminal records in its `jobs` list, so
/// `running` below is the same predicate the snapshot was filtered by — not a second
/// opinion about what counts as in flight.
const RUNNING_PHASES: [JobPhase; 4] = [
    JobPhase::Pending,
    JobPhase::Queued,
    JobPhase::Assigned,
    JobPhase::Streaming,
];

/// `GET /v1/observe`: a whole-hub snapshot for the monitoring UI.
///
/// Open by design (module docs) and **always** `200`, even when nothing is ready:
/// readiness is a field, not a status code, and a browser dashboard that sees a
/// non-200 while a pool is mid-drain stops rendering state it still has.
#[allow(clippy::too_many_lines)] // one flat pass: probe, then project each section into `json!`
pub async fn observe(State(hub): State<Arc<Hub>>) -> Response {
    // Captured **before** the fan-out, so every translated timestamp in this
    // document is relative to the same instant; the fan-out can take up to
    // `PROBE_TIMEOUT_MS` and a `now` taken afterwards would disagree with the
    // `last_seen_ms` values that were read earlier.
    let clock = Clock::from_hub(&hub);
    let snapshots = hub.pool_snapshots().await;
    // The mirrored gauges `/healthz` reports, read once after the fan-out (no
    // added latency: the mirrors are hub-side and never touch a mailbox).
    let readiness = hub.readiness().await;

    // Job id → the consumer socket ids currently streaming it, derived from the
    // hub-side registry. That registry is the only place the *socket* exists:
    // the actor keys its consumer sets by the per-(connection, task) `ConsumerId`
    // and cannot answer "which sockets are up".
    let connections = hub.consumer_connections();
    let mut watchers: HashMap<String, Vec<String>> = HashMap::new();
    for conn in &connections {
        for job in &conn.attached_jobs {
            watchers
                .entry(job.as_ref().to_owned())
                .or_default()
                .push(conn.id.as_ref().to_owned());
        }
    }

    let mut unresponsive: Vec<String> = Vec::new();
    let mut pools: Vec<Value> = Vec::new();
    // `BTreeMap`s throughout: the actor's own collections are already id-ordered,
    // but a hub-side union across pools is only stable if the hub orders it too,
    // and byte-stable JSON is what makes two polls comparable in a diff.
    let mut workers: BTreeMap<String, Value> = BTreeMap::new();
    let mut jobs: BTreeMap<String, Value> = BTreeMap::new();
    let mut leases: BTreeMap<String, Value> = BTreeMap::new();
    // Per-pool mailbox depth: the leading indicator of a wedged actor, and the
    // only per-pool number that is available even when that actor never answers.
    let mut mailbox_depth: Vec<Value> = Vec::new();
    let mut connected_workers = 0_u64;

    // `pool_snapshots` returns one entry per configured pool, sorted by id, so
    // the rendered arrays are ordered without a second sort.
    for (pool, snapshot) in &snapshots {
        let Some(handle) = hub.pool(pool) else {
            continue; // a pool removed between the two calls; nothing to report
        };
        let gauges = handle.gauges.lock().await.clone();
        let live = u64::try_from(handle.workers.lock().await.len()).unwrap_or(u64::MAX);
        let mailbox = handle.mailbox_pending() as u64;
        connected_workers = connected_workers.saturating_add(live);
        pools.push(pool_row(
            pool.as_ref(),
            snapshot.as_ref(),
            &gauges,
            live,
            mailbox,
        ));
        mailbox_depth.push(json!({
            "pool": pool.as_ref(),
            "mailbox_pending": mailbox,
        }));

        let Some(snapshot) = snapshot else {
            unresponsive.push(pool.as_ref().to_owned());
            // No workers/jobs/leases rows for this pool: their only source is the
            // actor, and inventing rows from a mirror that never held them would
            // be a guess the monitor cannot tell apart from measurement.
            continue;
        };

        for worker in &snapshot.workers {
            let id = worker.id.as_ref().to_owned();
            let row = worker_row(worker, &clock);
            match workers.entry(id) {
                Entry::Vacant(slot) => {
                    slot.insert(row);
                }
                Entry::Occupied(mut slot) => {
                    // A worker in two pools is listed once, from whichever answer
                    // is the freshest. Without this, a dual-host worker appears
                    // twice and the dashboard counts capacity it cannot use twice.
                    if keep_fresher(&row, slot.get()) {
                        slot.insert(row);
                    }
                }
            }
        }

        for view in &snapshot.jobs {
            if let Some(lease) = &view.lease {
                leases.insert(
                    view.id.as_ref().to_owned(),
                    lease_row(lease, view.phase, &clock),
                );
            }
            jobs.insert(
                view.id.as_ref().to_owned(),
                job_row(
                    view,
                    watchers
                        .get(view.id.as_ref())
                        .map_or(&[][..], Vec::as_slice),
                    &clock,
                ),
            );
        }
    }

    let body = json!({
        "protocol": OBSERVE_PROTOCOL,
        "now_ms": clock.epoch_origin_ms,
        "readiness": {
            // Field names mirror `GET /healthz` (`consumer.rs:926`) so one rule
            // covers both. `status` stays `"degraded"` at zero ready workers —
            // same rule as healthz — and the endpoint still answers `200`.
            "status": if readiness.ready_workers == 0 { "degraded" } else { "ok" },
            "ready_workers": readiness.ready_workers,
            "loading_workers": readiness.loading_workers,
            // Hub-side, from each pool's live socket map: includes a worker whose
            // actor is wedged, which the mirrored gauges may be a dispatch behind.
            "connected_workers": connected_workers,
            "queue_depth": readiness.queue_depth,
            "mailbox_pending": readiness.mailbox_pending,
            "warm_models": readiness.models,
            // Additions, documented rather than silently changing `/healthz`:
            "active_jobs": jobs.len(),
            "pool_mailbox_depth": mailbox_depth,
            "capacity_signal": if readiness.ready_workers == 0 && readiness.loading_workers > 0 { "cold_start" } else if readiness.ready_workers == 0 { "no_workers" } else { "warm" },
        },
        "pools": pools,
        "workers": workers.into_values().collect::<Vec<Value>>(),
        "jobs": jobs.into_values().collect::<Vec<Value>>(),
        "leases": leases.into_values().collect::<Vec<Value>>(),
        "consumer_connections": connections
            .iter()
            .map(|conn| connection_row(conn, &clock))
            .collect::<Vec<Value>>(),
        "unresponsive_pools": unresponsive,
    });

    consumer::json_response(StatusCode::OK, &body)
}

/// The two clock readings one request needs, captured once.
///
/// `epoch_origin_ms` is the epoch time the document reports as `now_ms`, and
/// `actor_now_ms` is what the actor's clock reads right now; together they map any
/// actor timestamp to epoch through [`crate::state::actor_to_wall_ms`], the single
/// implementation of that arithmetic. Holding the pair is also what keeps every
/// number in one response consistent with one another.
#[derive(Debug, Clone, Copy)]
struct Clock {
    /// Epoch milliseconds, sampled at the start of the request.
    epoch_origin_ms: u64,
    /// The actor's own clock (`state::monotonic_ms`) at the same instant.
    actor_now_ms: u64,
}

impl Clock {
    /// Sample both clocks. Called once per request, before the fan-out.
    fn from_hub(hub: &Hub) -> Self {
        Self {
            epoch_origin_ms: hub.wall_ms(),
            actor_now_ms: crate::state::monotonic_ms(),
        }
    }

    /// An actor timestamp as epoch ms.
    fn at(self, actor_ms: u64) -> u64 {
        crate::state::actor_to_wall_ms(self.epoch_origin_ms, self.actor_now_ms, actor_ms)
    }

    /// An optional actor timestamp as optional epoch ms.
    fn at_opt(self, actor_ms: Option<u64>) -> Option<u64> {
        actor_ms.map(|value| self.at(value))
    }

    /// An actor timestamp that is `0` when unset (`JobTimings::accept_ms` is a
    /// plain `Timestamp`, not an `Option`), rendered as `null` rather than as
    /// 1970.
    fn at_nonzero(self, actor_ms: u64) -> Option<u64> {
        if actor_ms == 0 {
            return None;
        }
        Some(self.at(actor_ms))
    }
}

/// One pool row: the actor's counts when it answered, the hub's mirrored gauges
/// when it did not, and `mailbox_pending`/`connected_workers` from the hub either
/// way (a wedged actor cannot report its own queue depth — `PoolHandle::
/// mailbox_pending` reads the channel from outside, `state.rs:969`).
///
/// The id key is `id`, matching `pools[].id` in the contract; the row is keyed by id
/// because the dashboard's pool table and its graph both index on it.
fn pool_row(
    pool: &str,
    snapshot: Option<&PoolSnapshot>,
    gauges: &PoolGauges,
    connected_workers: u64,
    mailbox_pending: u64,
) -> Value {
    json!({
        "id": pool,
        "ready_workers": snapshot.map_or(gauges.ready_workers, |value| u64_cast(value.ready_workers)),
        "loading_workers": snapshot.map_or(gauges.loading_workers, |value| u64_cast(value.loading_workers)),
        "connected_workers": connected_workers,
        "leases": snapshot.map_or(gauges.leases, |value| u64_cast(value.leases)),
        "queue_depth": snapshot.map_or(gauges.queue_depth, |value| u64_cast(value.queue_depth)),
        "mailbox_pending": mailbox_pending,
    })
}

/// One worker row, from [`WorkerView`] (built by `PoolActor::pool_snapshot`).
///
/// `engine` is the **engine** state (`ready|loading|degraded`) because that is the
/// question the dashboard asks; socket liveness is the separate `link` field, so an
/// orphaned-but-resumable worker is not silently counted as ready (`model.rs:98`, and
/// `WorkerRecord::advertised`'s rule). `in_flight` is the **list of job ids**, not a
/// count — the contract's whole point is that a monitor can see which job sits on
/// which worker without joining `jobs[]` by hand.
///
/// `free_slots` is the worker's own last heartbeat and `available_slots` is the
/// actor's reconciled number (`capacity - in_flight`, capped by the heartbeat): both
/// are reported because they answer different questions, and the actor's value is
/// what dispatch actually obeys (`model.rs:204`).
fn worker_row(worker: &WorkerView, clock: &Clock) -> Value {
    json!({
        "id": worker.id.as_ref(),
        "pools": worker
            .pools
            .iter()
            .map(|pool| pool.as_ref())
            .collect::<Vec<&str>>(),
        "link": worker.link,
        "engine": worker.engine,
        "epoch": worker.epoch,
        "capacity_slots": worker.capacity,
        "free_slots": worker.free_slots,
        // `null` when the worker reports no KV budget, per the contract.
        "free_kv_tokens": worker.free_kv_tokens,
        "in_flight": worker
            .in_flight
            .iter()
            .map(|job| job.as_ref())
            .collect::<Vec<&str>>(),
        "outstanding_claims": worker.outstanding_claims,
        "last_seen_ms": clock.at(worker.last_seen_ms),
        // Arbitrary JSON relayed from the engine's heartbeat, or `null`.
        "telemetry": worker.telemetry,
        // Additive: the resumability and the reconciled capacity described above.
        "resumable": worker.link == LinkState::Orphaned,
        "available_slots": worker.available_slots,
    })
}

/// One job row: the subset of a [`JobView`] the dashboard reads.
///
/// Deliberately not the whole `JobView` — `requires`, `usage`, `artifact`,
/// `payload_sha256` and the replay accounting are per-job debugging detail, and a
/// hub-side poll that copies all of them for every active job is a document
/// nobody can fetch on a laptop.
fn job_row(view: &JobView, connections: &[String], clock: &Clock) -> Value {
    json!({
        "id": view.id.as_ref(),
        "pool": view.pool.as_ref(),
        "kind": view.kind,
        "tenant": view.tenant.as_ref(),
        // Actor vocabulary, not the contract example's `"running"` — see the
        // module docs. `running` is the boolean a simple client can read.
        "phase": view.phase,
        "running": RUNNING_PHASES.contains(&view.phase),
        "terminal": view.terminal,
        "attempt": view.attempt,
        "priority": view.priority,
        "streaming": view.streaming,
        // The actor's consumer count: every face, HTTP/SSE included. The hub-side
        // socket ids are the separate `consumer_connections` field.
        "consumers": view.consumers,
        "consumer_connections": connections,
        "payload_bytes": view.payload_bytes,
        "delivered_bytes": view.delivered_bytes,
        "created_ms": clock.at_nonzero(view.timings.accept_ms),
        "first_frame_ms": clock.at_opt(view.timings.first_frame_ms),
        "last_frame_ms": clock.at_opt(view.timings.last_frame_ms),
        "lease": view.lease.as_ref().map(|lease| lease_row(lease, view.phase, clock)),
    })
}

/// One live lease.
///
/// `state` is derived from the job's phase rather than stored: a lease has no
/// state field of its own, and a second vocabulary for "who owns this job right
/// now" is how a dashboard ends up with two answers to one question.
/// `streamed_bytes` is an addition — it is the zero-vs-partial discriminator the
/// actor's own requeue policy turns on (`model.rs:322`), and the dashboard's
/// "will this job be retried?" cell reads it.
fn lease_row(lease: &Lease, phase: JobPhase, clock: &Clock) -> Value {
    json!({
        "job": lease.job.as_ref(),
        "worker": lease.worker.as_ref(),
        "epoch": lease.epoch,
        "attempt": lease.attempt,
        "state": match phase {
            JobPhase::Assigned => "leased",
            JobPhase::Streaming => "streaming",
            _ => "active",
        },
        "deadline_ms": clock.at(lease.deadline_ms),
        "streamed_bytes": lease.streamed_bytes,
    })
}

/// One live consumer socket.
///
/// `attached_jobs` comes from the hub-side registry written by `consumer_ws` (attach
/// in `spawn_pump`, detach when the pump ends, deregistration in `ConnGuard::drop`),
/// so an abnormal disconnect that never reaches the actor still disappears from
/// this document.
fn connection_row(conn: &ConsumerConnection, clock: &Clock) -> Value {
    let _ = clock; // `since_ms` is already epoch ms (the hub stamped it); the clock is taken for symmetry
    json!({
        "id": conn.id.as_ref(),
        "tenant": conn.tenant.as_ref(),
        "since_ms": conn.since_ms,
        "peer": conn.peer,
        "attached_jobs": conn
            .attached_jobs
            .iter()
            .map(|job| job.as_ref())
            .collect::<Vec<&str>>(),
    })
}

/// Whether `candidate` should replace `existing` for the same worker id.
///
/// Both rows were built by [`worker_row`], so `last_seen_ms` is always present;
/// `as_u64` is a guard against a future renderer changing that shape, not a
/// hedge, and it keeps the comparison from ever panicking.
fn keep_fresher(candidate: &Value, existing: &Value) -> bool {
    candidate["last_seen_ms"]
        .as_u64()
        .is_some_and(|seen| seen > existing["last_seen_ms"].as_u64().unwrap_or_default())
}

/// `usize` → `u64` without a truncation warning on 32-bit targets.
///
/// These are counts of workers, leases and queued jobs, all bounded by
/// `PoolLimits`, so the value always fits; `try_from` would only add a branch
/// whose fallback (`u64::MAX`) is a lie.
fn u64_cast(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use nexus_core::ids::ConsumerId;
    use nexus_core::model::{EngineState, LinkState};
    use nexus_core::model::{JobTimings, Priority, TerminalReason};
    use nexus_core::{JobId, Lease, PoolId, TenantId, WorkerId};
    use serde_json::json;

    use super::*;

    /// A clock with exact, round numbers, so every translated timestamp in these
    /// tests is an assertion on arithmetic rather than on wall-clock drift:
    /// actor `10_000` ≡ epoch `1_000_000`, i.e. epoch = actor + 990_000.
    fn clock() -> Clock {
        Clock {
            epoch_origin_ms: 1_000_000,
            actor_now_ms: 10_000,
        }
    }

    fn snapshot() -> PoolSnapshot {
        PoolSnapshot {
            pool: PoolId::new_unchecked("p"),
            queue_depth: 7,
            ready_workers: 3,
            loading_workers: 1,
            connected_workers: 4,
            leases: 2,
            workers: Vec::new(),
            jobs: Vec::new(),
        }
    }

    fn gauges() -> PoolGauges {
        PoolGauges {
            queue_depth: 99,
            ready_workers: 98,
            loading_workers: 97,
            leases: 96,
            warm_models: Default::default(),
            metrics: Default::default(),
        }
    }

    fn view() -> JobView {
        JobView {
            id: JobId::new_unchecked("job-1"),
            pool: PoolId::new_unchecked("p"),
            kind: "llm.completion".to_string(),
            tenant: TenantId::new_unchecked("t1"),
            phase: JobPhase::Streaming,
            attempt: 2,
            terminal: None,
            priority: Priority::Default,
            payload_bytes: 1_024,
            payload_sha256: "deadbeef".to_string(),
            requires: nexus_core::protocol::Requires::default(),
            timings: JobTimings {
                accept_ms: 9_000,
                dispatch_ms: Some(9_500),
                first_frame_ms: Some(9_800),
                last_frame_ms: None,
            },
            acked_seq: 5,
            gaps: 0,
            duplicates: 1,
            replay_frames: 3,
            consumers: 1,
            delivered_bytes: 640,
            lease: Some(Lease {
                job: JobId::new_unchecked("job-1"),
                worker: WorkerId::new_unchecked("w-1"),
                epoch: 4,
                attempt: 2,
                deadline_ms: 20_000,
                streamed_bytes: 640,
            }),
            artifact: None,
            usage: None,
            finish: None,
            worker_error: None,
            streaming: true,
            idempotent: true,
        }
    }

    #[test]
    fn clock_translates_past_and_future_actor_timestamps() {
        // The whole point of the signed arithmetic: a lease deadline in the future
        // must stay in the future. A saturating subtraction would clamp this to
        // `epoch_origin_ms` and every live lease would read "expires now".
        assert_eq!(clock().at(20_000), 1_010_000);
        assert_eq!(clock().at(9_000), 999_000);
        assert_eq!(clock().at(10_000), 1_000_000);
        // `0` is a legitimate actor timestamp (a hub that has just started), so it
        // translates rather than becoming `null`; only `at_nonzero` maps it away.
        assert_eq!(clock().at(0), 990_000);
        assert_eq!(clock().at_nonzero(0), None);
        assert_eq!(clock().at_opt(Some(9_800)), Some(999_800));
        assert_eq!(clock().at_opt(None), None);
    }

    #[test]
    fn pool_row_prefers_the_actor_snapshot_over_the_mirrored_gauges() {
        assert_eq!(
            pool_row("p", Some(&snapshot()), &gauges(), 4, 12),
            json!({
                // The contract key is `id` — the dashboard's pool table and its
                // graph both index on it.
                "id": "p",
                "ready_workers": 3,
                "loading_workers": 1,
                "connected_workers": 4,
                "leases": 2,
                "queue_depth": 7,
                "mailbox_pending": 12,
            })
        );
    }

    #[test]
    fn pool_row_falls_back_to_the_mirrored_gauges_when_the_actor_did_not_answer() {
        assert_eq!(
            pool_row("p", None, &gauges(), 6, 512),
            json!({
                "id": "p",
                "ready_workers": 98,
                "loading_workers": 97,
                "connected_workers": 6,
                "leases": 96,
                "queue_depth": 99,
                // Mailbox depth still comes from the handle: an actor cannot see
                // the depth of its own mailbox.
                "mailbox_pending": 512,
            })
        );
    }

    #[test]
    fn worker_row_separates_engine_state_from_socket_liveness() {
        let worker = WorkerView {
            id: WorkerId::new_unchecked("w-1"),
            pools: [PoolId::new_unchecked("p")].into_iter().collect(),
            link: LinkState::Orphaned,
            engine: EngineState::Ready,
            epoch: 7,
            capacity: 4,
            free_slots: 2,
            free_kv_tokens: Some(3),
            in_flight: [JobId::new_unchecked("job-1")].into_iter().collect(),
            outstanding_claims: 1,
            available_slots: 2,
            last_seen_ms: 9_000,
            telemetry: None,
        };
        assert_eq!(
            worker_row(&worker, &clock()),
            json!({
                "id": "w-1",
                "pools": ["p"],
                // Ready engine, dead socket: `engine` stays `ready` and `link` says
                // why it is not taking work.
                "engine": "ready",
                "link": "orphaned",
                "epoch": 7,
                "capacity_slots": 4,
                // The worker's own heartbeat value, not the actor's reconciled one.
                "free_slots": 2,
                "free_kv_tokens": 3,
                // Job ids, per the contract — a monitor must be able to see which
                // job sits on which worker without joining `jobs[]` itself.
                "in_flight": ["job-1"],
                "outstanding_claims": 1,
                "last_seen_ms": 999_000,
                "telemetry": null,
                // Additive fields: resumability and the dispatchable slot count.
                "resumable": true,
                "available_slots": 2,
            })
        );
    }

    #[test]
    fn worker_row_reports_a_kv_budget_of_null_rather_than_zero() {
        // A worker that never declared a KV budget must not look like one that has
        // spent it all: `0` and `null` answer different questions.
        let worker = WorkerView {
            id: WorkerId::new_unchecked("w-2"),
            pools: [PoolId::new_unchecked("p")].into_iter().collect(),
            link: LinkState::Live,
            engine: EngineState::Loading,
            epoch: 1,
            capacity: 1,
            free_slots: 0,
            free_kv_tokens: None,
            in_flight: Default::default(),
            outstanding_claims: 0,
            available_slots: 0,
            last_seen_ms: 0,
            telemetry: Some(json!({"tokens_per_s": 41.5})),
        };
        let row = worker_row(&worker, &clock());
        assert_eq!(row["free_kv_tokens"], json!(null));
        assert_eq!(row["in_flight"], json!([]));
        assert_eq!(row["link"], json!("live"));
        assert_eq!(row["resumable"], json!(false));
        // `telemetry` is passed through verbatim, whatever the engine reported.
        assert_eq!(row["telemetry"], json!({"tokens_per_s": 41.5}));
        // `0` on the actor's clock is a real instant (process start), not "never".
        assert_eq!(row["last_seen_ms"], json!(990_000));
    }

    #[test]
    fn job_row_keeps_the_actor_phase_and_adds_the_running_flag() {
        let row = job_row(&view(), &["con-9".to_string()], &clock());
        assert_eq!(
            row,
            json!({
                "id": "job-1",
                "pool": "p",
                "kind": "llm.completion",
                "tenant": "t1",
                "phase": "streaming",
                "running": true,
                "terminal": null,
                "attempt": 2,
                "priority": "default",
                "streaming": true,
                "consumers": 1,
                "consumer_connections": ["con-9"],
                "payload_bytes": 1024,
                "delivered_bytes": 640,
                "created_ms": 999_000,
                "first_frame_ms": 999_800,
                "last_frame_ms": null,
                "lease": {
                    "job": "job-1",
                    "worker": "w-1",
                    "epoch": 4,
                    "attempt": 2,
                    "state": "streaming",
                    "deadline_ms": 1_010_000,
                    "streamed_bytes": 640,
                },
            })
        );
    }

    #[test]
    fn job_row_marks_a_terminal_job_as_not_running() {
        // Named `finished`, not `view`: a local called `view` would shadow the
        // `view()` helper for the rest of the module and break the next test's
        // `view()` call at compile time.
        let mut finished = view();
        finished.phase = JobPhase::Finished;
        finished.terminal = Some(TerminalReason::Completed);
        finished.lease = None;
        let row = job_row(&finished, &[], &clock());
        assert_eq!(row["phase"], json!("finished"));
        assert_eq!(row["running"], json!(false));
        assert_eq!(row["terminal"], json!("completed"));
        assert_eq!(row["lease"], json!(null));
        assert_eq!(row["consumer_connections"], json!([]));

        // Queued work counts as running but has no lease, which is exactly the
        // state a dashboard needs to show as "waiting for a worker".
        let mut queued = view();
        queued.phase = JobPhase::Queued;
        queued.lease = None;
        let row = job_row(&queued, &[], &clock());
        assert_eq!(row["running"], json!(true));
        assert_eq!(row["lease"], json!(null));
    }

    #[test]
    fn lease_row_labels_state_from_the_job_phase() {
        let lease = Lease {
            job: JobId::new_unchecked("job-1"),
            worker: WorkerId::new_unchecked("w-1"),
            epoch: 4,
            attempt: 1,
            deadline_ms: 20_000,
            streamed_bytes: 0,
        };
        assert_eq!(
            lease_row(&lease, JobPhase::Assigned, &clock()),
            json!({
                "job": "job-1",
                "worker": "w-1",
                "epoch": 4,
                "attempt": 1,
                "state": "leased",
                "deadline_ms": 1_010_000,
                "streamed_bytes": 0,
            })
        );
        assert_eq!(
            lease_row(&lease, JobPhase::Pending, &clock())["state"],
            "active"
        );
    }

    #[test]
    fn keep_fresher_picks_the_newest_seen_time() {
        let older = json!({ "last_seen_ms": 999_000 });
        let newer = json!({ "last_seen_ms": 999_500 });
        assert!(keep_fresher(&newer, &older));
        assert!(!keep_fresher(&older, &newer));
        // Equal stays as-is: the first pool in sorted order wins, so two polls of
        // the same hub render identical bytes.
        assert!(!keep_fresher(&older, &older));
    }

    #[test]
    fn connection_row_lists_the_jobs_the_socket_is_streaming() {
        let conn = ConsumerConnection {
            id: ConsumerId::new_unchecked("c-9"),
            tenant: TenantId::new_unchecked("t1"),
            since_ms: 999_000,
            peer: None,
            attached_jobs: [JobId::new_unchecked("job-1")].into_iter().collect(),
        };
        assert_eq!(
            connection_row(&conn, &clock()),
            json!({
                "id": "c-9",
                "tenant": "t1",
                // Already epoch ms: the hub stamped it at accept, so it is not
                // translated like an actor timestamp.
                "since_ms": 999_000,
                // `axum::serve` installs no `ConnectInfo`, so the hub has no peer
                // address to report — `null`, never a guess.
                "peer": null,
                // The contract key is `attached_jobs`.
                "attached_jobs": ["job-1"],
            })
        );
    }
}
