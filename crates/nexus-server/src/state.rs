//! Shared runtime state: actor mailboxes, per-job relay fan-out, the worker
//! connection registry, and the effect dispatcher.
//!
//! # Shape of a pool (task §5, §13)
//!
//! ```text
//! HTTP/WS handler ──mpsc(bounded)──▶ [ actor task ] ──Vec<Effect>──▶ dispatcher
//!                                                              │
//!                       worker outbound mpsc ◀─────────────────┤ SendWorker
//!                       per-job broadcast   ◀──────────────────┤ Relay / Keepalive
//!                       gate / replay / snapshot waiters ◀─────┘ Accepted / Reject / …
//! ```
//!
//! The `DashMap<JobId, PoolHandle>` the task permits "only as a read-mostly index
//! so a consumer's relay task can locate its job" is [`Hub::job_index`]: it is written
//! at submit time and pruned whenever the actor reports `Effect::JobForgotten`, so it
//! holds only jobs the actor still knows about rather than one entry per submission for
//! the life of the process (NEX-7.2). It never holds job state, so the actor remains the
//! single authority.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::http::StatusCode;
use nexus_core::ids::ConsumerId;
use nexus_core::ids::JobId;
use nexus_core::ids::PayloadRefId;
use nexus_core::ids::PoolId;
use nexus_core::ids::WorkerId;
// `Hub::cfg` is typed by name in three places below, so the type itself is needed.
use nexus_core::protocol::ByeReason;
use nexus_core::protocol::HubFrame;
use nexus_core::protocol::WireCaps;
use nexus_core::ActorMsg;
use nexus_core::BrokerConfig;
use nexus_core::ConsumerEvent;
use nexus_core::Effect;
use nexus_core::JobPhase;
use nexus_core::JobView;
use nexus_core::PoolActor;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Mutex;

/// Actor mailbox depth. Bounded so a submission flood back-pressures the HTTP
/// layer instead of growing memory without end (task §8).
const MAILBOX_DEPTH: usize = 512;

/// How long a read-only probe (snapshot, payload fetch) may take before the
/// caller answers "hub busy" rather than holding the request open forever.
const PROBE_TIMEOUT_MS: u64 = 2_000;

/// One item on a worker socket's outbound queue.
#[derive(Debug, Clone)]
pub enum WorkerWrite {
    /// A JSON-lines frame.
    Frame(String),
    /// Echo of a ping payload.
    Pong(Vec<u8>),
    /// Close the socket after everything already queued has been written. Used
    /// for eviction (task §11): the hub must drop the stale socket rather than
    /// leave it open and double-dispatch to the same GPU.
    Close,
}

/// A payload fetch outcome, handed back to the HTTP handler.
#[derive(Debug, Clone)]
pub struct PayloadGrant {
    /// Body bytes.
    pub bytes: Vec<u8>,
    /// Declared content type.
    pub content_type: String,
    /// Digest of the body.
    pub sha256: String,
    /// Refusal reason when not authorized.
    pub reason: Option<String>,
}

/// Item on a job's relay channel.
#[derive(Debug, Clone, PartialEq)]
pub enum RelayItem {
    /// A semantic broker event.
    Event(ConsumerEvent),
    /// An SSE comment (`: ping`) while the job waits (task §7.2).
    Comment(&'static str),
    /// Nothing more will arrive: the handler may end its body.
    End,
}

/// What the first effect about a job says about its HTTP response.
#[derive(Debug, Clone, PartialEq)]
pub enum Gate {
    /// Accepted: headers may go out and the body may start.
    Accepted,
    /// Refuse **before** any body bytes (task §7.2, acceptance test 6).
    Reject {
        /// HTTP status.
        status: StatusCode,
        /// Stable machine code.
        code: String,
        /// Human detail.
        message: String,
        /// `Retry-After` in seconds.
        retry_after_secs: Option<u64>,
    },
    /// The actor stopped answering: a retryable failure, never a hang (task §11).
    Closed,
}

/// Everything the consumer side needs to follow one job.
#[derive(Debug)]
pub struct JobRelay {
    /// Fan-out to attached consumers. Bounded and lossy for a lagging consumer,
    /// which is the backpressure trade task §8 accepts.
    pub events: broadcast::Sender<RelayItem>,
    /// First-answer gate, resolved once by `Accepted` or `Reject`.
    pub gate: Mutex<Option<oneshot::Sender<Gate>>>,
    /// Replay delivery for the consumer that just attached, so a joiner does not
    /// make every other subscriber replay too.
    pub replay_target: Mutex<Option<mpsc::Sender<RelayItem>>>,
    /// Set once a terminal event has been broadcast, so `End` is sent once.
    pub finished: AtomicBool,
}

impl JobRelay {
    /// New relay with a bounded fan-out.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            events: broadcast::Sender::new(capacity.max(16)),
            gate: Mutex::new(None),
            replay_target: Mutex::new(None),
            finished: AtomicBool::new(false),
        }
    }

    /// Resolve the gate, if it is still open.
    pub async fn resolve(&self, gate: Gate) {
        if let Some(tx) = self.gate.lock().await.take() {
            let _ = tx.send(gate);
        }
    }

    /// Broadcast one event, marking the stream finished when it is terminal.
    pub fn push(&self, event: ConsumerEvent) {
        let terminal = matches!(
            event,
            ConsumerEvent::Finish { .. } | ConsumerEvent::Error { .. }
        );
        let _ = self.events.send(RelayItem::Event(event));
        if terminal && !self.finished.swap(true, Ordering::Relaxed) {
            let _ = self.events.send(RelayItem::End);
        }
    }
}

/// One connected worker socket, as seen by a pool.
#[derive(Debug)]
pub struct WorkerConn {
    /// Connection generation, echoed as `Welcome.epoch`.
    pub conn: u64,
    /// Outbound queue for this socket.
    pub tx: mpsc::Sender<WorkerWrite>,
}

/// Per-pool runtime handle.
#[derive(Debug)]
pub struct PoolHandle {
    /// The pool's id.
    pub id: PoolId,
    /// Actor mailbox.
    pub tx: mpsc::Sender<ActorMsg>,
    /// Worker id → live connection.
    pub workers: Mutex<HashMap<WorkerId, WorkerConn>>,
    /// Job id → relay fan-out.
    pub relays: Mutex<HashMap<JobId, Arc<JobRelay>>>,
    /// Read-mostly mirror of the actor's authoritative gauges, refreshed on every
    /// dispatch so `/healthz` and `/metrics` never disagree with the actor.
    pub gauges: Mutex<PoolGauges>,
}

/// Mirror of the actor's gauges and counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PoolGauges {
    /// Queued jobs.
    pub queue_depth: u64,
    /// Ready workers.
    pub ready_workers: u64,
    /// Connected but not-ready workers.
    pub loading_workers: u64,
    /// Live leases.
    pub leases: u64,
    /// Warm model names, for `/v1/models`.
    pub warm_models: BTreeSet<String>,
    /// Mirror of the actor's counters **and** gauges, so `/metrics` reports the
    /// exact numbers the acceptance tests assert. Gauges matter as much as
    /// counters here: `nexus_queue_depth`, `nexus_ready_workers` and
    /// `nexus_leases` are what make §13's "metrics that must exist because
    /// debugging is otherwise blind" true, and copying only `counters()` would
    /// drop them from the exposition entirely.
    pub metrics: nexus_core::Metrics,
}

impl PoolGauges {
    /// Pull the gauges from the actor itself: one source, no drift.
    #[must_use]
    pub fn from_actor(actor: &PoolActor) -> Self {
        Self {
            queue_depth: actor.queue_depth() as u64,
            ready_workers: actor.ready_workers() as u64,
            loading_workers: actor.loading_workers() as u64,
            leases: actor.leases_len() as u64,
            warm_models: actor.warm_models(),
            metrics: actor.metrics().clone(),
        }
    }
}

/// The hub: config, credentials, one handle per pool, and the routing index.
#[derive(Debug)]
pub struct Hub {
    /// Validated configuration.
    pub cfg: BrokerConfig,
    /// Pool id → handle.
    pub pools: HashMap<PoolId, Arc<PoolHandle>>,
    /// Job id → pool, populated at submit time (the §5 read-mostly index). Bounded by
    /// the actor's own bounds: the entry is removed when the actor drops the record
    /// (`Effect::JobForgotten`, handled in `dispatch`), so a long-lived hub does not
    /// accumulate one id per submission ever made.
    pub job_index: Mutex<HashMap<JobId, PoolId>>,
    /// Hub-side mirror of the actor's §5.2 idempotency table: key → (pool, job, expiry).
    ///
    /// It exists because the actor answers a duplicate on the **original** job's id
    /// (`Effect::Accepted { job: existing, attached: true, .. }`, `actor.rs:1045-1064`)
    /// while a face registers its relay, its verdict gate and its pump under the id it
    /// minted for the request. With no way to find the original *before* minting, the
    /// retry's gate was never resolved and the answer was `503 hub_unavailable` while the
    /// original's replay was dropped on a relay nobody was reading.
    ///
    /// Bounded exactly as the actor's own table is (`limits.max_idempotency_entries`,
    /// evicting whatever expires first — the rule of `actor.rs:1199-1215`) and pruned by
    /// `Effect::JobForgotten` through [`Self::key_of_job`], so an entry can never outlive
    /// the record it indexes: a task NEX-6.3 deleted has no mirror row left, and its retry
    /// runs fresh. Expiry uses the actor's own `limits.idempotency_ttl_ms` and the actor's
    /// own clock (`monotonic_ms`, the value `run_actor` feeds it), so the two tables lapse
    /// together; if the actor's copy lapses a millisecond first, the face answers from the
    /// still-retained record with the same id and the same bytes the actor's duplicate
    /// branch would have replayed.
    pub idempotency: Mutex<BTreeMap<String, (PoolId, JobId, u64)>>,
    /// Expiry-ordered index of [`Self::idempotency`]'s keys, `(expiry_ms, key)`, so lazy
    /// expiry and bounded eviction are O(log n) instead of a scan. Mirrors
    /// `actor.rs:482`'s `idempotency_timer`.
    pub idempotency_expiry: Mutex<BTreeSet<(u64, String)>>,
    /// Job → key, so `Effect::JobForgotten` prunes the row that belongs to it without
    /// scanning the table.
    pub key_of_job: Mutex<HashMap<JobId, String>>,
    /// Consumer API keys, as digests.
    pub api_keys: nexus_core::auth::ApiKeyRegistry,
    /// Pool tokens, as digests.
    pub pool_tokens: nexus_core::auth::PoolTokenRegistry,
    /// Monotonic id source.
    pub ids: Arc<IdSource>,
    /// Artifact storage root.
    pub artifact_root: String,
    /// Waiters for payload fetch answers, keyed by reference.
    pub payload_waiters: Mutex<HashMap<PayloadRefId, oneshot::Sender<PayloadGrant>>>,
    /// Waiters for `GET /v1/jobs/{id}` answers, keyed by job.
    pub snapshot_waiters: Mutex<HashMap<JobId, Vec<oneshot::Sender<Option<JobView>>>>>,
    /// Wall-clock origin, for OpenAI `created`.
    pub epoch_ms: u64,
}

/// Opaque, monotonic, process-unique ids.
#[derive(Debug)]
pub struct IdSource {
    /// Next job sequence.
    pub job: AtomicU64,
    /// Next connection generation.
    pub conn: AtomicU64,
    /// Next consumer id.
    pub consumer: AtomicU64,
    /// Per-process salt, so ids from two hubs cannot collide.
    pub salt: u64,
}

impl Default for IdSource {
    fn default() -> Self {
        Self::new()
    }
}

impl IdSource {
    /// New source, salted from the wall clock.
    #[must_use]
    pub fn new() -> Self {
        let salt = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.subsec_nanos() as u64);
        Self {
            job: AtomicU64::new(1),
            conn: AtomicU64::new(1),
            consumer: AtomicU64::new(1),
            salt,
        }
    }

    /// Next job id.
    #[must_use]
    pub fn job_id(&self) -> JobId {
        let n = self.job.fetch_add(1, Ordering::Relaxed);
        JobId::new_unchecked(format!("job-{:08x}-{n:08x}", self.salt & 0xffff_ffff))
    }

    /// Next connection generation.
    #[must_use]
    pub fn conn_id(&self) -> u64 {
        self.conn.fetch_add(1, Ordering::Relaxed)
    }

    /// Next consumer id.
    #[must_use]
    pub fn consumer_id(&self) -> ConsumerId {
        let n = self.consumer.fetch_add(1, Ordering::Relaxed);
        ConsumerId::new_unchecked(format!("con-{:08x}-{n:08x}", self.salt & 0xffff_ffff))
    }
}

impl Hub {
    /// Build the hub, then start one actor task per configured pool.
    #[must_use]
    pub fn new(cfg: BrokerConfig, artifact_root: &str) -> Arc<Self> {
        let mut pools = HashMap::new();
        // The receiver travels **with** the handle it belongs to. The spawn loop below
        // runs after `pools` has moved into the `Hub`, so a handle that is looked up
        // there instead of carried here would need an `expect` to prove an invariant the
        // compiler can already see: `starts` is built from exactly the pool ids inserted
        // above, one handle each.
        let mut starts: Vec<(PoolId, mpsc::Receiver<ActorMsg>, Arc<PoolHandle>)> = Vec::new();
        for id in cfg.pools.keys() {
            let pid = PoolId::new_unchecked(id.clone());
            let (tx, rx) = mpsc::channel::<ActorMsg>(MAILBOX_DEPTH);
            let handle = Arc::new(PoolHandle {
                id: pid.clone(),
                tx,
                workers: Mutex::new(HashMap::new()),
                relays: Mutex::new(HashMap::new()),
                gauges: Mutex::new(PoolGauges::default()),
            });
            starts.push((pid.clone(), rx, Arc::clone(&handle)));
            pools.insert(pid, handle);
        }
        let hub = Arc::new(Self {
            epoch_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as u64),
            api_keys: nexus_core::auth::ApiKeyRegistry::from_config(&cfg.auth),
            pool_tokens: nexus_core::auth::PoolTokenRegistry::from_config(&cfg.auth),
            job_index: Mutex::new(HashMap::new()),
            idempotency: Mutex::new(BTreeMap::new()),
            idempotency_expiry: Mutex::new(BTreeSet::new()),
            key_of_job: Mutex::new(HashMap::new()),
            payload_waiters: Mutex::new(HashMap::new()),
            snapshot_waiters: Mutex::new(HashMap::new()),
            artifact_root: artifact_root.to_string(),
            ids: Arc::new(IdSource::new()),
            cfg,
            pools,
        });
        for (pid, rx, handle) in starts {
            let actor =
                PoolActor::with_artifact_root(pid.clone(), hub.cfg.clone(), &hub.artifact_root);
            tokio::spawn(run_actor(pid, actor, rx, handle, Arc::clone(&hub)));
        }
        hub
    }

    /// Handle for a pool.
    #[must_use]
    pub fn pool(&self, pool: &PoolId) -> Option<&Arc<PoolHandle>> {
        self.pools.get(pool)
    }

    /// Pool ids in a stable order.
    #[must_use]
    pub fn pool_ids(&self) -> Vec<PoolId> {
        self.pools
            .keys()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .cloned()
            .collect()
    }

    /// Record which pool owns a job (called by the submit handler).
    ///
    /// Paired with [`Self::forget_job`]: the index only stays bounded if every entry a
    /// submit adds is removed when the actor stops knowing the job.
    pub async fn bind_job(&self, job: &JobId, pool: &PoolId) {
        self.job_index
            .lock()
            .await
            .insert(job.clone(), pool.clone());
    }

    /// Drop the routing entry for a job the actor has forgotten
    /// (`Effect::JobForgotten`). Idempotent: an unknown job prunes to nothing and
    /// answers `false`, so the dispatcher can call it from every drop site without
    /// tracking whether an earlier effect already did the work.
    pub async fn forget_job(&self, job: &JobId) -> bool {
        self.job_index.lock().await.remove(job).is_some()
    }

    /// Route a job to its pool from the read-mostly index.
    pub async fn route_job(&self, job: &JobId) -> Option<Arc<PoolHandle>> {
        let pid = self.job_index.lock().await.get(job).cloned()?;
        self.pool(&pid).map(Arc::clone)
    }

    /// Record which job an idempotency key owns, for the reason given on
    /// [`Self::idempotency`]. Called by the faces at submit time, in the same step that
    /// writes `job_index` (`consumer::submit`), because the two must name the same job.
    ///
    /// Lock order across the three mirrors is fixed — table, expiry index, reverse index —
    /// and no guard is held across a call that takes them again, so no path can acquire
    /// them in opposite orders.
    pub async fn remember_idempotency(&self, key: &str, pool: &PoolId, job: &JobId) {
        let mut table = self.idempotency.lock().await;
        let mut expiry_index = self.idempotency_expiry.lock().await;
        let mut reverse = self.key_of_job.lock().await;
        // The actor's clock, not the wall clock: `monotonic_ms` is the value `run_actor`
        // hands the actor as `now_ms`, so a TTL started here expires with the actor's own
        // entry for the same key.
        let expiry = monotonic_ms().saturating_add(self.cfg.limits.idempotency_ttl_ms);
        // A key reaching a second job replaces its first row, timer entry included;
        // `JobForgotten` normally did this first, and the reverse row only changes when
        // the job actually changed.
        if let Some((_old_pool, old_job, old_expiry)) = table.remove(key) {
            expiry_index.remove(&(old_expiry, key.to_string()));
            if old_job != *job {
                reverse.remove(&old_job);
            }
        }
        // Bound the table by evicting whatever expires first, so a long-lived key is
        // never dropped ahead of a stale one — the actor's rule (`actor.rs:1199-1215`),
        // copied rather than paraphrased so the two tables never disagree.
        while table.len() >= self.cfg.limits.max_idempotency_entries {
            let Some((oldest_expiry, oldest)) = expiry_index.iter().next().cloned() else {
                break;
            };
            expiry_index.remove(&(oldest_expiry, oldest.clone()));
            if let Some((_pool, evicted, _evicted_expiry)) = table.remove(&oldest) {
                reverse.remove(&evicted);
            }
        }
        table.insert(key.to_string(), (pool.clone(), job.clone(), expiry));
        expiry_index.insert((expiry, key.to_string()));
        reverse.insert(job.clone(), key.to_string());
    }

    /// The job an idempotency key already owns, when the hub still mirrors it.
    ///
    /// Expiry is lazy and O(log n): the timer index is ordered, so the loop pops rows that
    /// are already stale and stops at the first live one. `Effect::JobForgotten` normally
    /// removes the row long before its TTL, so reaching an expired entry here needs a
    /// record the actor kept for a full `idempotency_ttl_ms` with nobody following it.
    #[must_use]
    pub async fn idempotency_target(&self, key: &str) -> Option<(PoolId, JobId)> {
        let now = monotonic_ms();
        let mut table = self.idempotency.lock().await;
        let mut expiry_index = self.idempotency_expiry.lock().await;
        let mut reverse = self.key_of_job.lock().await;
        while let Some((expiry, stale)) = expiry_index.iter().next().cloned() {
            if expiry > now {
                break;
            }
            expiry_index.remove(&(expiry, stale.clone()));
            if let Some((_pool, job, _)) = table.remove(&stale) {
                reverse.remove(&job);
            }
        }
        let (pool, job, expiry) = table.get(key).cloned()?;
        // The loop above only pops rows the timer index still lists, and this one can be
        // the boundary case (`expiry == now`) that a coarse clock leaves behind, so the
        // hit itself is checked too rather than answered a millisecond stale.
        if expiry <= now {
            table.remove(key);
            expiry_index.remove(&(expiry, key.to_string()));
            reverse.remove(&job);
            return None;
        }
        Some((pool, job))
    }

    /// Drop one mirror entry by key: a face that found the mirrored record already gone,
    /// so the key belongs to whatever it submits next.
    pub async fn forget_idempotency(&self, key: &str) {
        let mut table = self.idempotency.lock().await;
        let mut expiry_index = self.idempotency_expiry.lock().await;
        let mut reverse = self.key_of_job.lock().await;
        if let Some((_pool, job, expiry)) = table.remove(key) {
            expiry_index.remove(&(expiry, key.to_string()));
            reverse.remove(&job);
        }
    }

    /// Drop the mirror entry for a job the actor has forgotten (`Effect::JobForgotten`).
    ///
    /// This is what keeps a deleted task's retry honest: without it a key would go on
    /// naming a record nobody holds, and NEX-6.3's fresh re-run would become a replay of a
    /// `404`. Idempotent, like `forget_job`.
    pub async fn forget_idempotency_for_job(&self, job: &JobId) {
        let mut table = self.idempotency.lock().await;
        let mut expiry_index = self.idempotency_expiry.lock().await;
        let mut reverse = self.key_of_job.lock().await;
        let Some(key) = reverse.remove(job) else {
            return;
        };
        if let Some((_pool, _job, expiry)) = table.remove(&key) {
            expiry_index.remove(&(expiry, key));
        }
    }

    /// Authoritative job view, read through the owning actor's mailbox.
    ///
    /// This is a serialized read, not a lock on shared job state: the answer
    /// cannot be a torn mix of two transitions because it comes back as an effect
    /// from the same single-owner loop.
    pub async fn job_snapshot(&self, job: &JobId) -> Option<JobView> {
        let handle = self.route_job(job).await?;
        let (reply, waiting) = oneshot::channel();
        self.snapshot_waiters
            .lock()
            .await
            .entry(job.clone())
            .or_default()
            .push(reply);
        if handle
            .tx
            .send(ActorMsg::Query { job: job.clone() })
            .await
            .is_err()
        {
            self.forget_snapshot_waiter(job).await;
            return None;
        }
        // `timeout(..)` yields `Result<Result<Option<JobView>, RecvError>, Elapsed>`:
        // `.ok()` drops the timeout arm, `and_then(ok())` drops the channel-closed
        // arm, and the final `.flatten()` unwraps the `Option<JobView>` the waiter
        // sent. (`Result` is not an iterator, so `.flatten()` alone cannot step over
        // the inner `Result`.)
        let view =
            tokio::time::timeout(std::time::Duration::from_millis(PROBE_TIMEOUT_MS), waiting)
                .await
                .ok()
                .and_then(|answered| answered.ok())
                .flatten();
        if view.is_none() {
            self.forget_snapshot_waiter(job).await;
        }
        view
    }

    async fn forget_snapshot_waiter(&self, job: &JobId) {
        let mut waiters = self.snapshot_waiters.lock().await;
        if let Some(slot) = waiters.get_mut(job) {
            slot.pop();
            if slot.is_empty() {
                waiters.remove(job);
            }
        }
    }

    /// Fetch a spilled payload on behalf of a worker, through the actor.
    pub async fn payload_fetch(
        &self,
        reference: &PayloadRefId,
        token: &str,
        worker: &WorkerId,
        attempt: u32,
    ) -> Option<PayloadGrant> {
        // A payload reference is the job id it was spilled for (see `PayloadBook`),
        // so routing the job routes the payload.
        let job = JobId::new_unchecked(reference.as_ref().to_string());
        let handle = self.route_job(&job).await?;
        let (reply, waiting) = oneshot::channel();
        self.payload_waiters
            .lock()
            .await
            .insert(reference.clone(), reply);
        let msg = ActorMsg::FetchPayload {
            reference: reference.clone(),
            token: token.to_string(),
            worker: worker.clone(),
            attempt,
        };
        if handle.tx.send(msg).await.is_err() {
            self.payload_waiters.lock().await.remove(reference);
            return None;
        }
        // See `job_snapshot` for the shape: `Result<Result<PayloadGrant, RecvError>,
        // Elapsed>`, so `.ok()` plus `and_then(ok())` is the whole answer.
        let grant =
            tokio::time::timeout(std::time::Duration::from_millis(PROBE_TIMEOUT_MS), waiting)
                .await
                .ok()
                .and_then(|answered| answered.ok());
        if grant.is_none() {
            self.payload_waiters.lock().await.remove(reference);
        }
        grant
    }

    /// Wall-clock milliseconds, for OpenAI's `created` field.
    #[must_use]
    pub fn wall_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(self.epoch_ms, |d| d.as_millis() as u64)
    }

    /// Aggregate warm capacity, read from the mirrored gauges.
    pub async fn readiness(&self) -> HubReadiness {
        let mut out = HubReadiness::default();
        for handle in self.pools.values() {
            let gauges = handle.gauges.lock().await;
            out.ready_workers += gauges.ready_workers;
            out.loading_workers += gauges.loading_workers;
            out.queue_depth += gauges.queue_depth;
            out.mailbox_pending += handle.mailbox_pending() as u64;
            out.models.extend(gauges.warm_models.iter().cloned());
        }
        out
    }

    /// Prometheus text exposition, assembled from the actors' own registries.
    ///
    /// Two things the previous shape got wrong, both visible to a scraper:
    ///   * the pool label is inserted **inside** an existing label set, so a
    ///     counter recorded as `nexus_frames_total{kind="delta"}` becomes
    ///     `nexus_frames_total{pool="…",kind="delta"}`. Appending a second brace
    ///     group is not valid exposition format;
    ///   * gauges are merged as well as counters, because §13's mandatory
    ///     `queue depth` / `ready workers` / `leases` series are gauges and were
    ///     absent from `/metrics` before.
    pub async fn render_metrics(&self) -> String {
        // base name → (type, labeled samples). `BTreeMap` keeps the exposition
        // byte-stable, which the node smoke test and any `diff` of scrapes rely on.
        let mut families: BTreeMap<String, (String, BTreeMap<String, u64>)> = BTreeMap::new();
        for handle in self.pools.values() {
            let gauges = handle.gauges.lock().await;
            for (kind, entries) in [
                ("gauge", gauges.metrics.gauges()),
                ("counter", gauges.metrics.counters()),
            ] {
                for (key, value) in entries {
                    let base = nexus_core::metrics::base_name(key);
                    let labeled = with_pool_label(key, handle.id.as_ref());
                    let slot = families
                        .entry(base)
                        .or_insert_with(|| (kind.to_string(), BTreeMap::new()));
                    *slot.1.entry(labeled).or_insert(0) += *value;
                }
            }
        }
        let mut out = String::new();
        for (base, (kind, samples)) in families {
            out.push_str(&format!("# TYPE {base} {kind}\n"));
            for (key, value) in samples {
                out.push_str(&format!("{key} {value}\n"));
            }
        }
        out
    }
}

/// Insert the `pool` label into a metric key, creating a label set only when the
/// key does not already have one.
#[must_use]
fn with_pool_label(key: &str, pool: &str) -> String {
    match key.split_once('{') {
        Some((base, rest)) => format!("{base}{{pool=\"{pool}\",{rest}"),
        None => format!("{key}{{pool=\"{pool}\"}}"),
    }
}

/// Warm-capacity summary across pools.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HubReadiness {
    /// Total ready workers.
    pub ready_workers: u64,
    /// Total connected-but-loading workers.
    pub loading_workers: u64,
    /// Total queued jobs.
    pub queue_depth: u64,
    /// Total mailbox depth.
    pub mailbox_pending: u64,
    /// Warm model names.
    pub models: BTreeSet<String>,
}

impl PoolHandle {
    /// Get or create a job's relay.
    pub async fn relay(&self, job: &JobId, capacity: usize) -> Arc<JobRelay> {
        let mut relays = self.relays.lock().await;
        if let Some(existing) = relays.get(job) {
            return Arc::clone(existing);
        }
        let created = Arc::new(JobRelay::new(capacity));
        relays.insert(job.clone(), Arc::clone(&created));
        created
    }

    /// Drop a job's relay once its consumers are gone.
    pub async fn forget_relay(&self, job: &JobId) {
        self.relays.lock().await.remove(job);
    }

    /// The live connection generation for a worker, if it has one.
    pub async fn live_conn(&self, worker: &WorkerId) -> Option<u64> {
        self.workers.lock().await.get(worker).map(|conn| conn.conn)
    }

    /// Jobs currently being followed (shutdown drain, metrics).
    pub async fn followed_jobs(&self) -> Vec<JobId> {
        self.relays.lock().await.keys().cloned().collect()
    }

    /// Pending messages in the mailbox, the backpressure signal.
    #[must_use]
    pub fn mailbox_pending(&self) -> usize {
        self.tx.max_capacity() - self.tx.capacity()
    }
}

/// Send one message to a pool actor. An error means the actor exited, which the
/// caller turns into a retryable failure rather than a hang.
pub async fn send(handle: &PoolHandle, msg: ActorMsg) -> anyhow::Result<()> {
    handle
        .tx
        .send(msg)
        .await
        .map_err(|_| anyhow::anyhow!("pool actor mailbox is closed"))
}

/// Push an already-encoded frame to a worker's live socket.
///
/// `caps` must be the hub's **configured** outbound caps
/// (`Limits::hub_caps()`), not `WireCaps::default()`: an `Assigned` carrying an
/// inline payload near `inline_payload_cap_bytes` is far larger than the 1 MiB
/// default line, and encoding it against the default would drop the dispatch
/// silently — the worker is never told and the lease only lapses at expiry.
pub async fn send_frame(
    handle: &Arc<PoolHandle>,
    worker: &WorkerId,
    frame: &HubFrame,
    caps: &WireCaps,
) {
    let line = match encode_line(frame, caps) {
        Ok(line) => line,
        Err(err) => {
            tracing::error!(%worker, %err, "outbound frame violated the wire cap");
            return;
        }
    };
    let mut workers = handle.workers.lock().await;
    match workers.get_mut(worker) {
        Some(conn) => {
            // The actor emits `Bye{Evicted}` for the *previous* generation of a
            // worker id. The registry (see `worker_ws::session`) is what
            // actually closes that old socket, and by the time this effect runs
            // the live binding is the **new** connection. Forwarding it here
            // would tell a freshly welcomed worker it had just been evicted, and
            // a client that honours `Bye` would drop the good connection and
            // reconnect forever. The WASM host sees the frame; it owns the
            // sockets and can tell its own generations apart.
            if matches!(
                frame,
                HubFrame::Bye {
                    reason: ByeReason::Evicted
                }
            ) {
                tracing::debug!(%worker, "eviction `Bye` handled by the connection registry, not the live socket");
                return;
            }
            // A `Welcome` re-binds the worker to its new generation. The
            // registry already stores the new connection id before `Hello` is
            // sent, so this is a no-op in the normal path; it is kept because a
            // host that registers lazily still ends up bound to the epoch the
            // actor believes in.
            if let HubFrame::Welcome { epoch, .. } = frame {
                conn.conn = *epoch;
            }
            // Bounded, never indefinite. The actor applies its effects inline (see
            // `run_actor`), so parking on a socket that has stopped draining would
            // head-of-line block every other job in this pool. The queue is
            // `worker_ws::OUTBOUND_QUEUE` frames deep; a frame that cannot enter it
            // within the grace period means the connection is dead in every way that
            // matters, and the **actor** decides what that does to the jobs — the
            // same message the socket handler sends when the peer hangs up
            // (`ActorMsg::WorkerGone`, `actor.rs:1772`), so the lease/resume-grace
            // rules run and an in-flight `Assigned`/`Cancel` is resolved rather than
            // silently swallowed.
            const SEND_GRACE_MS: u64 = 25;
            match tokio::time::timeout(
                std::time::Duration::from_millis(SEND_GRACE_MS),
                conn.tx.send(WorkerWrite::Frame(line)),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    tracing::warn!(%worker, "worker socket closed during send");
                    report_dead_connection(handle, worker, conn.conn);
                }
                Err(_elapsed) => {
                    tracing::warn!(%worker, grace_ms = SEND_GRACE_MS, "worker outbound queue is stalled; reporting the connection as gone");
                    report_dead_connection(handle, worker, conn.conn);
                }
            }
        }
        None => tracing::debug!(%worker, "frame dropped: no live connection"),
    }
}

/// Encode and send one frame to a worker.
fn encode_line(frame: &HubFrame, caps: &WireCaps) -> Result<String, nexus_core::CoreError> {
    nexus_core::protocol::encode_line(frame, caps)
}

/// Report a worker connection that can no longer accept frames back to the actor.
///
/// The actor is the only authority on what a dead connection means (`on_worker_gone`
/// → resume grace / lease expiry), and it filters reports about a superseded
/// generation itself via the `conn` id it believes is live (`actor.rs:1772`). This
/// layer therefore never decides on its own whether a job is requeued.
///
/// `try_send`, deliberately: this runs *inside* the actor's own effect loop, so
/// awaiting its mailbox would re-enter the loop that is calling it. If the mailbox
/// is saturated the report is dropped with a log, and the actor's lease timer still
/// resolves the job — a late answer, never a lost one.
fn report_dead_connection(handle: &Arc<PoolHandle>, worker: &WorkerId, conn: u64) {
    if let Err(err) = handle.tx.try_send(ActorMsg::WorkerGone {
        worker: worker.clone(),
        conn,
    }) {
        tracing::warn!(%worker, %err, "pool mailbox would block: the WorkerGone report is deferred to the lease timer");
    }
}

/// The actor task: take a message, mutate state, perform the effects. Nothing else
/// holds a mutable reference to the actor, which is what makes the claim guarantee
/// structural rather than "a mutex we hope is held correctly" (task §5).
async fn run_actor(
    pool: PoolId,
    mut actor: PoolActor,
    mut rx: mpsc::Receiver<ActorMsg>,
    handle: Arc<PoolHandle>,
    hub: Arc<Hub>,
) {
    // The actor's heartbeat period also sets how often the virtual clock advances, and
    // both ends are intentional: never faster than a scheduler tick, never so slow that a
    // dead worker looks alive for a whole lease period.
    let tick_ms = actor.config().heartbeat_ms_for(&pool).clamp(50, 1_000);
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(tick_ms));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tracing::info!(pool = %pool, tick_ms, "pool actor started");
    loop {
        tokio::select! {
            biased;
            maybe_msg = rx.recv() => match maybe_msg {
                Some(msg) => {
                    let now = monotonic_ms();
                    let effects = actor.handle(now, msg);
                    dispatch(&hub, &handle, &actor, effects).await;
                }
                None => break,
            },
            _ = ticker.tick() => {
                let now = monotonic_ms();
                let effects = actor.handle(now, ActorMsg::Tick { now_ms: now });
                dispatch(&hub, &handle, &actor, effects).await;
            }
        }
    }
    // The actor is gone: end every open stream with the retryable signal, so a
    // client sees a clean failure instead of a hang (task §11, §16.18).
    let relays: Vec<Arc<JobRelay>> = handle.relays.lock().await.values().cloned().collect();
    for relay in relays {
        let _ = relay.events.send(RelayItem::End);
        relay.resolve(Gate::Closed).await;
    }
    tracing::warn!(pool = %pool, "pool actor exited");
}

/// Monotonic millisecond clock for the actor, immune to wall-clock jumps.
fn monotonic_ms() -> u64 {
    use std::sync::OnceLock;
    static ORIGIN: OnceLock<std::time::Instant> = OnceLock::new();
    ORIGIN
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_millis() as u64
}

/// Perform the actor's effects, in order: worker writes first, so a consumer never
/// receives a terminal event before its owning worker was told.
async fn dispatch(
    hub: &Arc<Hub>,
    handle: &Arc<PoolHandle>,
    actor: &PoolActor,
    effects: Vec<Effect>,
) {
    for effect in effects {
        match effect {
            // Outbound caps come from the hub's own configuration so an
            // `Assigned` with a large inline payload is not dropped for
            // exceeding a default that is smaller than the inline cap.
            Effect::SendWorker { worker, ref frame } => {
                send_frame(handle, &worker, frame, &hub.cfg.limits.hub_caps()).await;
                // §7.2's second half. A job submitted while the pool had no warm
                // worker was `Accepted` but deliberately left unanswered (see the
                // `Accepted` arm below: the grace window can still end in a `503`
                // with no body bytes). Handing the job to a worker is the moment
                // the request became serviceable, so the body may open now.
                // `frame` is `&Box<HubFrame>` — the effect boxes the frame so the
                // effect enum stays small (`clippy::large_enum_variant`), and serde
                // unwraps the box, so the wire shape is unchanged.
                if let HubFrame::Assigned { job, .. } = frame.as_ref() {
                    let relay = handle
                        .relay(job, actor.limits().consumer_backpressure_frames)
                        .await;
                    relay.resolve(Gate::Accepted).await;
                }
            }
            Effect::Accepted {
                job,
                attached,
                replayed,
            } => {
                let relay = handle
                    .relay(&job, actor.limits().consumer_backpressure_frames)
                    .await;
                // `attached` joins an execution that is already running, so its
                // stream is open by definition. A fresh submission whose record is
                // still `Pending` is the §7.2 grace case: nothing may be written
                // yet, and the gate stays unresolved until either an `Assigned`
                // lands (arm above) or the grace expiry answers `503`.
                if attached
                    || actor
                        .job(&job)
                        .is_some_and(|record| record.phase != JobPhase::Pending)
                {
                    relay.resolve(Gate::Accepted).await;
                }
                // The private replay queue carries only this consumer's buffered
                // prefix. A fresh submission has none, so the sender must be
                // dropped: `pump_sse` otherwise waits in its replay loop forever
                // and never watches the fan-out, which would delay every delta
                // until the job terminates and swallow §6's consumer-disconnect
                // `Detach` (the worker would keep generating for nobody).
                let target = relay.replay_target.lock().await.take();
                if let (Some(tx), true) = (target, attached) {
                    for event in replayed {
                        let _ = tx.send(RelayItem::Event(event)).await;
                    }
                }
            }
            Effect::Reject {
                job,
                status,
                code,
                message,
                retry_after_ms,
            } => {
                let gate = Gate::Reject {
                    status: StatusCode::from_u16(status).unwrap_or(StatusCode::SERVICE_UNAVAILABLE),
                    code: code.clone(),
                    message: message.clone(),
                    retry_after_secs: retry_after_ms.map(|ms| ms.div_ceil(1_000)),
                };
                tracing::warn!(pool = %handle.id, ?job, status, %code, %message, "request rejected");
                if let Some(job) = job {
                    let relay = handle.relay(&job, 16).await;
                    // Nothing will replay into this submission's private queue,
                    // so close it: a waiting pump must fall through to its
                    // verdict rather than hold the slot open.
                    drop(relay.replay_target.lock().await.take());
                    relay.resolve(gate).await;
                }
            }
            Effect::Relay { job, events } => {
                let relay = handle
                    .relay(&job, actor.limits().consumer_backpressure_frames)
                    .await;
                for event in events {
                    relay.push(event);
                }
            }
            Effect::ReplayTo {
                job,
                consumer,
                events,
                lossy,
            } => {
                let relay = handle
                    .relay(&job, actor.limits().consumer_backpressure_frames)
                    .await;
                match relay.replay_target.lock().await.take() {
                    Some(tx) => {
                        if lossy {
                            // The ring already evicted part of what was asked for:
                            // say so, rather than pretending the text is complete.
                            let _ = tx
                                .send(RelayItem::Event(ConsumerEvent::Gap {
                                    expected: 0,
                                    got: 0,
                                }))
                                .await;
                        }
                        for event in events {
                            let _ = tx.send(RelayItem::Event(event)).await;
                        }
                    }
                    None => {
                        tracing::debug!(pool = %handle.id, %job, %consumer, "attach replay had no waiting consumer")
                    }
                };
                // The trailing `;` matters: without it the `match` is the block's
                // tail expression, so the `MutexGuard` temporary created by
                // `replay_target.lock()` would outlive `relay` and its drop would run
                // on a borrow of a value that is already gone (E0597).
            }
            Effect::Keepalive { job } => {
                if let Some(relay) = handle.relays.lock().await.get(&job) {
                    let _ = relay.events.send(RelayItem::Comment("ping"));
                }
            }
            Effect::ReleasePayload { job, reference } => {
                tracing::debug!(pool = %handle.id, %job, ?reference, "prompt released from memory");
            }
            Effect::DeleteArtifact { artifact, path } => {
                tracing::info!(pool = %handle.id, %artifact, %path, "artifact TTL expired");
                crate::io::delete_artifact(&path).await;
            }
            Effect::Terminal {
                job,
                reason,
                retryable,
            } => {
                tracing::info!(pool = %handle.id, %job, reason = reason.as_str(), retryable, "job terminal");
                // A terminal job's fan-out has carried its last event, so the map
                // entry goes: without this, `relays` grows for the hub's lifetime
                // while the actor has already bounded its own job map by retention.
                // Live pumps hold their own `Arc<JobRelay>`, and a later `/events`
                // attach creates a fresh one, because the replay ring itself lives
                // in the actor and is unaffected.
                handle.forget_relay(&job).await;
            }
            Effect::JobForgotten { job } => {
                // The actor has dropped the record, the replay ring and the idempotency
                // key, so the job id is unknown from here on: a later submission with the
                // old key starts a fresh execution (NEX-6.3). Every hub-side structure
                // keyed by job id has to agree, or the two read-mostly maps outlive the
                // thing they index — `job_index` was otherwise one entry per submission
                // for the lifetime of the process (NEX-7.2).
                //
                // Removal is keyed by this job alone and idempotent, so it cannot disturb
                // a live relay belonging to another job. A pump that is still attached
                // holds its own `Arc<JobRelay>` and ends on the verdict it already has;
                // a later `/events` attach answers `404 unknown_job`, which is the truth.
                let routed = hub.forget_job(&job).await;
                handle.forget_relay(&job).await;
                // The key goes with the record: an entry left behind would answer a
                // later retry with a job nobody has, where NEX-6.3 says it runs fresh.
                hub.forget_idempotency_for_job(&job).await;
                tracing::debug!(pool = %handle.id, %job, routed, "job forgotten by the actor; index, relay and idempotency mirror pruned");
            }
            Effect::Snapshot { job, view } => {
                let mut waiters = hub.snapshot_waiters.lock().await;
                if let Some(slot) = waiters.get_mut(&job) {
                    if let Some(waiter) = slot.pop() {
                        // `view` is `Box<Option<JobView>>` (see `Effect::Snapshot`): the
                        // box is an allocation detail, so the waiter receives exactly the
                        // derived view it received before, cloned out from behind it.
                        let _ = waiter.send(view.as_ref().clone());
                    }
                    if slot.is_empty() {
                        waiters.remove(&job);
                    }
                }
            }
            Effect::PayloadBytes {
                reference,
                granted,
                bytes,
                content_type,
                sha256,
                reason,
            } => {
                if let Some(waiter) = hub.payload_waiters.lock().await.remove(&reference) {
                    let _ = waiter.send(PayloadGrant {
                        bytes,
                        content_type,
                        sha256,
                        reason,
                    });
                } else {
                    tracing::debug!(%reference, granted, "payload answer had no waiter");
                }
            }
            Effect::Log { level, message } => match level {
                nexus_core::LogLevel::Info => tracing::info!(pool = %handle.id, %message),
                nexus_core::LogLevel::Warn => tracing::warn!(pool = %handle.id, %message),
                nexus_core::LogLevel::Error => tracing::error!(pool = %handle.id, %message),
            },
        }
    }
    *handle.gauges.lock().await = PoolGauges::from_actor(actor);
}

/// Cancel every leased job on shutdown, so GPUs stop generating (task §6, §16.18).
pub async fn drain_pools(hub: &Arc<Hub>) {
    for handle in hub.pools.values() {
        let _ = handle
            .tx
            .send(ActorMsg::Shutdown {
                now_ms: monotonic_ms(),
            })
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::dispatch;
    use super::send_frame;
    use super::with_pool_label;
    use super::Hub;
    use super::PoolGauges;
    use super::PoolHandle;
    use super::WorkerConn;
    use super::WorkerWrite;
    use nexus_core::ids::JobId;
    use nexus_core::ids::PoolId;
    use nexus_core::ids::WorkerId;
    use nexus_core::protocol::CancelReason;
    use nexus_core::protocol::HubFrame;
    use nexus_core::protocol::WireCaps;
    use nexus_core::ActorMsg;
    use nexus_core::BrokerConfig;
    use nexus_core::Effect;
    use nexus_core::PoolActor;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio::sync::Mutex;

    #[test]
    fn pool_label_goes_inside_the_existing_label_set() {
        // Exact expected output: a second brace group is not valid Prometheus.
        assert_eq!(
            with_pool_label(r#"nexus_frames_total{kind="delta"}"#, "llm.chat/qwen38"),
            r#"nexus_frames_total{pool="llm.chat/qwen38",kind="delta"}"#
        );
        assert_eq!(
            with_pool_label("nexus_queue_depth", "p"),
            r#"nexus_queue_depth{pool="p"}"#
        );
    }

    /// The actor applies its effects inline, so a worker whose outbound queue is
    /// full and unread (a wedged socket) must cost the pool a bounded grace, not an
    /// indefinite park — and the frame must be **reported**, not swallowed. The
    /// actor stays the only authority: it gets `WorkerGone` with the generation it
    /// believes in and decides whether that means resume grace or lease expiry.
    #[tokio::test]
    async fn a_stalled_worker_queue_is_reported_instead_of_parking_the_actor_loop() {
        // A handle with no actor task behind it: the mailbox is drained by this test,
        // so the assertion cannot race a running actor.
        let (actor_tx, mut actor_rx) = mpsc::channel::<ActorMsg>(16);
        let handle = Arc::new(PoolHandle {
            id: PoolId::new_unchecked("llm.chat/qwen38"),
            tx: actor_tx,
            workers: Mutex::new(HashMap::new()),
            relays: Mutex::new(HashMap::new()),
            gauges: Mutex::new(PoolGauges::default()),
        });

        let (worker_tx, worker_rx) = mpsc::channel::<WorkerWrite>(1);
        worker_tx
            .send(WorkerWrite::Frame("queued".to_string()))
            .await
            .expect("the first frame fits");
        handle.workers.lock().await.insert(
            WorkerId::new_unchecked("w-stalled"),
            WorkerConn {
                conn: 7,
                tx: worker_tx,
            },
        );

        let started = std::time::Instant::now();
        send_frame(
            &handle,
            &WorkerId::new_unchecked("w-stalled"),
            &HubFrame::Cancel {
                job: JobId::new_unchecked("job-1"),
                reason: CancelReason::ConsumerGone,
            },
            &WireCaps::default(),
        )
        .await;
        let elapsed = started.elapsed();

        assert!(
            elapsed >= std::time::Duration::from_millis(20),
            "the queue gets its grace period first: {elapsed:?}"
        );
        assert!(
            elapsed < std::time::Duration::from_millis(500),
            "the send is bounded, never indefinite: {elapsed:?}"
        );

        let report = actor_rx
            .recv()
            .await
            .expect("the failed send must reach the actor");
        // Matched on a reference: binding `worker`/`conn` out of the owned value
        // would partially move `report`, and the message below still formats it.
        assert!(
            matches!(&report, ActorMsg::WorkerGone { worker, conn } if worker.as_ref() == "w-stalled" && *conn == 7),
            "the report carries the connection the actor believes is live: {report:?}"
        );
        assert!(
            actor_rx.try_recv().is_err(),
            "exactly one report per failed send"
        );
        drop(worker_rx);
    }

    /// NEX-7.2 / NEX-6.3: the two hub-side maps keyed by job id must shrink when the
    /// actor stops knowing the job. Without the `Effect::JobForgotten` arm, `job_index`
    /// grows by one entry per submission for the lifetime of the process — the actor
    /// bounds its own job map, but the index and the relay fan-out do not.
    #[tokio::test]
    async fn job_index_and_relays_are_pruned_when_the_actor_forgets_a_job() {
        // A real `Hub`, so the dispatcher prunes the same maps the HTTP surface reads.
        // The actor tasks it spawns stay parked on empty mailboxes: this test drives
        // `dispatch` directly, which is the half that owns the pruning.
        let hub = Hub::new(BrokerConfig::default(), "var/artifacts");
        let pid = PoolId::new_unchecked("llm.chat/qwen38");
        let handle = Arc::clone(
            hub.pools
                .get(&pid)
                .expect("the sample topology owns the llm pool"),
        );
        let actor = PoolActor::new(pid.clone(), hub.cfg.clone());

        let kept = JobId::new_unchecked("job-kept");
        let gone = JobId::new_unchecked("job-gone");
        hub.bind_job(&gone, &pid).await;
        hub.bind_job(&kept, &pid).await;
        // Two live fan-outs, because pruning has to be per-job and never "clear the map".
        handle.relay(&gone, 16).await;
        handle.relay(&kept, 16).await;
        // And two idempotency rows, for the same reason: `JobForgotten` has to take the
        // deleted task's key with it while its neighbour's stays (NEX-6.3's retry runs
        // fresh, §5.2's duplicate still replays).
        hub.remember_idempotency("gone-key", &pid, &gone).await;
        hub.remember_idempotency("kept-key", &pid, &kept).await;
        assert_eq!(hub.job_index.lock().await.len(), 2);
        assert_eq!(handle.relays.lock().await.len(), 2);
        assert_eq!(hub.idempotency.lock().await.len(), 2);

        dispatch(
            &hub,
            &handle,
            &actor,
            vec![Effect::JobForgotten { job: gone.clone() }],
        )
        .await;

        // The routing entry is gone, so a request for that job is a `404`, not a route to
        // a pool whose actor has already forgotten it.
        assert!(!hub.forget_job(&gone).await, "the entry was already pruned");
        assert!(hub.route_job(&gone).await.is_none());
        // The key goes with it: a retained row would answer the next retry with a task that
        // no longer exists, where NEX-6.3 says it runs fresh.
        assert!(hub.idempotency_target("gone-key").await.is_none());
        assert!(!hub.key_of_job.lock().await.contains_key(&gone));
        // The neighbour is untouched: a live relay for another job must survive.
        assert!(handle.relays.lock().await.contains_key(&kept));
        assert!(hub.job_index.lock().await.contains_key(&kept));
        assert_eq!(
            hub.idempotency_target("kept-key").await.map(|(_, job)| job),
            Some(kept.clone()),
            "a pruned job must not take another task's key with it"
        );

        // Idempotent, because two drop sites can report the same job: the NEX-6.3 orphan
        // path deletes the record and the retention sweep may have armed a timer first.
        dispatch(
            &hub,
            &handle,
            &actor,
            vec![Effect::JobForgotten { job: gone }],
        )
        .await;
        assert_eq!(handle.relays.lock().await.len(), 1);
        assert_eq!(hub.job_index.lock().await.len(), 1);
        assert_eq!(hub.idempotency.lock().await.len(), 1);
    }

    /// NEX-7.2 on the new map: the idempotency mirror is bounded by the actor's own entry
    /// cap, evicting whatever expires first rather than the newest key, and its rows expire
    /// on the actor's own TTL so a long-lived hub does not route retries to jobs nobody has.
    #[tokio::test]
    async fn the_idempotency_mirror_is_bounded_and_expires() {
        let mut cfg = BrokerConfig::default();
        // Three rows is the whole table, and a TTL the test can outwait. `Hub::new` does
        // not run `validate`, so `1 ms` is usable here rather than refused at boot.
        cfg.limits.max_idempotency_entries = 3;
        cfg.limits.idempotency_ttl_ms = 1;
        let hub = Hub::new(cfg, "var/artifacts");
        let pid = PoolId::new_unchecked("llm.chat/qwen38");

        hub.remember_idempotency("k-1", &pid, &JobId::new_unchecked("job-1"))
            .await;
        hub.remember_idempotency("k-2", &pid, &JobId::new_unchecked("job-2"))
            .await;
        hub.remember_idempotency("k-3", &pid, &JobId::new_unchecked("job-3"))
            .await;
        assert_eq!(hub.idempotency.lock().await.len(), 3);
        assert_eq!(hub.idempotency_expiry.lock().await.len(), 3);
        assert_eq!(hub.key_of_job.lock().await.len(), 3);

        // A fourth row is the cap: the entry that expires first leaves, and every reverse
        // index loses exactly that one row.
        hub.remember_idempotency("k-4", &pid, &JobId::new_unchecked("job-4"))
            .await;
        assert_eq!(
            hub.idempotency
                .lock()
                .await
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec!["k-2".to_string(), "k-3".to_string(), "k-4".to_string()],
            "the oldest-expiring key is the one that goes"
        );
        assert_eq!(hub.key_of_job.lock().await.len(), 3);
        assert!(hub.idempotency_target("k-1").await.is_none());

        // Expiry: every row carries the same 1 ms TTL, so after a short wait the table is
        // empty on the next read rather than on a timer the hub would have to run.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(hub.idempotency_target("k-4").await.is_none());
        assert!(hub.idempotency.lock().await.is_empty());
        assert!(hub.idempotency_expiry.lock().await.is_empty());
        assert!(
            hub.key_of_job.lock().await.is_empty(),
            "lazy expiry clears the reverse index too, or it grows on its own"
        );
    }

    /// The bounded-shape claim, stated exactly: N submissions leave the index holding
    /// only the jobs the actor still has records for.
    #[tokio::test]
    async fn the_routing_index_holds_only_the_jobs_the_actor_still_knows() {
        let hub = Hub::new(BrokerConfig::default(), "var/artifacts");
        let pid = PoolId::new_unchecked("llm.chat/qwen38");
        let handle = Arc::clone(
            hub.pools
                .get(&pid)
                .expect("the sample topology owns the llm pool"),
        );
        let actor = PoolActor::new(pid.clone(), hub.cfg.clone());

        // Twelve submissions: twelve index entries and twelve fan-outs.
        let ids: Vec<JobId> = (1..=12)
            .map(|n| JobId::new_unchecked(format!("job-{n}")))
            .collect();
        for id in &ids {
            hub.bind_job(id, &pid).await;
            handle.relay(id, 16).await;
        }
        assert_eq!(hub.job_index.lock().await.len(), 12);

        // One dispatcher pass carrying the actor's drop reports for the first nine.
        let forgotten: Vec<Effect> = ids[..9]
            .iter()
            .map(|job| Effect::JobForgotten { job: job.clone() })
            .collect();
        dispatch(&hub, &handle, &actor, forgotten).await;

        let expected: Vec<String> = ids[9..].iter().map(|j| j.as_ref().to_string()).collect();
        let mut indexed: Vec<String> = hub
            .job_index
            .lock()
            .await
            .keys()
            .map(|j| j.as_ref().to_string())
            .collect();
        indexed.sort();
        assert_eq!(indexed, expected);
        let mut relays: Vec<String> = handle
            .relays
            .lock()
            .await
            .keys()
            .map(|j| j.as_ref().to_string())
            .collect();
        relays.sort();
        assert_eq!(relays, expected);
    }
}
