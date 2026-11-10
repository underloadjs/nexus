//! `pool_actor` — the switch (task §5). One actor owns one pool's queue, worker
//! set, leases and jobs, and it is the **only** place job state mutates.
//!
//! # Deterministic claim, defined precisely
//!
//! A `Claim` grants *authority to receive work*. Authority is recorded in
//! [`PoolActor::claims`] as a FIFO of `(worker, slots)` in **mailbox order**, and
//! fulfilment always consumes the oldest outstanding authority first. A job
//! leaves the queue the instant it is leased, so it is either queued or leased —
//! never both. Consequently:
//!
//! - two `Claim`s in the same tick yield exactly two authority entries, ordered
//!   by arrival, and only the first receives the queued job (acceptance test 2);
//! - a second dispatch of the same job is not "unlikely" but unrepresentable
//!   (test 1);
//! - the winner depends only on message order, never on hashing or map ordering.
//!
//! # The socket-close vs `Resume` contradiction, resolved
//!
//! §5.1 says "socket closed → expire immediately"; §4 says a reconnecting worker
//! must `Resume` a still-leased job. Both cannot be literal, so the implemented
//! semantics are:
//!
//! 1. **Dispatchability expires immediately.** The worker leaves the ready set,
//!    counts as zero capacity, gets no new work, and cannot renew its lease.
//! 2. **Ownership is retained for `resume_grace_ms`** (default 1500 ms) so
//!    `Resume` can re-attach. During the grace the job is not dispatchable to
//!    anyone else, so there is never a second owner.
//! 3. At grace expiry the §5.1 table is applied: zero bytes delivered +
//!    idempotent → requeue with `attempt++` preferring a different worker; zero
//!    bytes + non-idempotent → fail loudly (never requeued); bytes already
//!    delivered → in-band terminal, or continuation when enabled (§7.4).
//! 4. `resume_grace_ms = 0` reproduces the strict "expire immediately" reading,
//!    trading `Resume` reachability for the literal wording — an operator choice.
//! 5. A `Resume` whose grace elapsed, whose attempt is stale, or whose lease
//!    moved is refused with `ResumeNack` and never applied twice.
//!
//! Frames tagged with a connection id that is not the worker's live connection
//! are dropped as stale. That is what makes the "re-ran the notebook cell" race
//! (task §11) safe: the evicted socket cannot resurrect itself, renew a lease,
//! or close the new socket's state.
//!
//! A `Hello` for a worker id that already has a record is a **socket swap, not a
//! death**, and is applied by updating that record in place: `in_flight` (the
//! actor's own slot accounting), the held leases' `epoch`, and — when the previous
//! socket was still live — the engine state all survive, while the replaced
//! socket's unfulfilled `Claim` authority is voided. A reconnect after the socket
//! died keeps the `Degraded` demotion `orphan_worker` applied, so readiness must be
//! re-declared by a `Ready` heartbeat before anything is dispatched to it (§8).

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;

// `Effect`/`LogLevel` derive `Serialize` for the WASM effect boundary.
use serde::Serialize;
use serde_json::Value;

use crate::auth::AttestationAllowlist;
use crate::config::BrokerConfig;
use crate::config::KindConfig;
use crate::errors::CoreError;
use crate::ids::ArtifactId;
use crate::ids::ConsumerId;
use crate::ids::JobId;
use crate::ids::PayloadRefId;
use crate::ids::PoolId;
use crate::ids::TenantId;
use crate::ids::WorkerId;
use crate::limits::Limits;
use crate::metrics::names;
use crate::metrics::Metrics;
use crate::model::BufferedFrame;
use crate::model::EngineState;
use crate::model::JobPhase;
use crate::model::JobRecord;
use crate::model::JobView;
use crate::model::Lease;
use crate::model::LinkState;
use crate::model::PoolSnapshot;
use crate::model::Submission;
use crate::model::TerminalReason;
use crate::model::WorkerRecord;
use crate::model::WorkerTier;
use crate::model::WorkerView;
use crate::protocol::Attestation;
use crate::protocol::ByeReason;
use crate::protocol::CancelReason;
use crate::protocol::FinishKind;
use crate::protocol::FrameKind;
use crate::protocol::HubFrame;
use crate::protocol::JobPayload;
use crate::protocol::Requires;
use crate::protocol::ResumedJob;
use crate::protocol::WorkerFrame;
use crate::queue::InMemoryQueue;
use crate::queue::QueueBackend;
use crate::queue::QueueError;
use crate::relay::ConsumerEvent;
// Module-level import (not the function) so the call site reads `scheduler::..`;
// the actor is the only caller of the ranking helpers.
use crate::scheduler;
use crate::seq::SeqVerdict;
use crate::store::sha256_hex;
use crate::store::ArtifactBook;
use crate::store::PayloadBook;
use crate::time::Timestamp;

/// Minimum spacing between SSE keepalive comments while a job waits (§7.2).
pub const KEEPALIVE_INTERVAL_MS: u64 = 1_000;

/// Clamp on a worker's self-declared capacity: `capacity: 1000000` must not
/// become unbounded over-dispatch.
pub const MAX_DECLARED_CAPACITY: u32 = 64;

/// How many queued jobs one dispatch pass inspects per claiming worker.
const QUEUE_SCAN_LIMIT: usize = 64;

/// Access-log severity for [`Effect::Log`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum LogLevel {
    /// Routine state change.
    Info,
    /// Degraded but handled.
    Warn,
    /// Rejected input or a lost job.
    Error,
}

/// The `Hello` fields the actor needs. The transport layer has already checked
/// the pool token; the actor checks version, attestation and pool membership.
///
/// `Default` is derived because a transport that has already matched on
/// `WorkerFrame::Hello` (the native WS session) builds this with
/// `HelloInput::from_frame(..).unwrap_or_default()`; every field has a sane
/// zero value, and a default-constructed input is still refused by the actor
/// (protocol negotiation runs before anything else, and an empty attestation
/// fails the allowlist while attestation is required).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HelloInput {
    /// Requested protocol version.
    pub protocol: u32,
    /// Pools requested.
    pub pools: Vec<String>,
    /// Declared slot count.
    pub capacity: u32,
    /// What the worker can serve.
    pub requires: Requires,
    /// Digests it claims to have loaded.
    pub attestation: Attestation,
    /// Requested tier; config may refuse it.
    pub tier: Option<WorkerTier>,
    /// Engine label, informational.
    pub engine: Option<String>,
}

impl HelloInput {
    /// Extract from a decoded `Hello` frame, or `None` for any other frame.
    #[must_use]
    pub fn from_frame(frame: &WorkerFrame) -> Option<Self> {
        let WorkerFrame::Hello {
            protocol,
            pools,
            capacity,
            requires,
            attestation,
            tier,
            engine,
            ..
        } = frame
        else {
            return None;
        };
        Some(Self {
            protocol: *protocol,
            pools: pools.clone(),
            capacity: *capacity,
            requires: requires.clone(),
            attestation: attestation.clone(),
            tier: *tier,
            engine: engine.clone(),
        })
    }
}

/// A worker's claim about an artifact, checked against what the hub stored.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeclaredArtifact {
    /// Declared sha256 of the whole artifact.
    pub sha256: Option<String>,
    /// Declared total size.
    pub size_bytes: Option<u64>,
}

/// Input to the actor: one variant per thing that can happen to the pool.
#[derive(Debug, Clone)]
pub enum ActorMsg {
    /// A consumer submitted work; `job` is the id clients will see.
    Submit {
        /// Pre-assigned job id.
        job: JobId,
        /// Validated submission from an edge adapter.
        submission: Submission,
        /// Submitting consumer, attached immediately.
        consumer: ConsumerId,
    },
    /// Attach a consumer to an existing job, replaying from `from_seq`.
    Attach {
        /// Job to follow.
        job: JobId,
        /// Consumer identity.
        consumer: ConsumerId,
        /// Replay floor; `0` means "everything still buffered".
        from_seq: u64,
    },
    /// A consumer's socket went away.
    Detach {
        /// Job being followed.
        job: JobId,
        /// Consumer that left.
        consumer: ConsumerId,
    },
    /// Explicit cancellation from the consumer surface.
    Cancel {
        /// Job to cancel.
        job: JobId,
        /// Why.
        reason: CancelReason,
    },
    /// A worker socket completed its handshake and sent `Hello`.
    Hello {
        /// Stable worker id from the frame.
        worker: WorkerId,
        /// Server-assigned connection id for this socket.
        conn: u64,
        /// The decoded `Hello`.
        hello: HelloInput,
    },
    /// Any other worker frame, tagged with the socket that produced it.
    WorkerFrame {
        /// Worker id bound to the connection.
        worker: WorkerId,
        /// Connection the frame arrived on.
        conn: u64,
        /// Decoded frame.
        frame: WorkerFrame,
    },
    /// A worker socket closed.
    WorkerGone {
        /// Worker id.
        worker: WorkerId,
        /// Connection that closed; a mismatch means a stale socket.
        conn: u64,
    },
    /// Artifact bytes landed in the store, uploaded over HTTP. This is the
    /// authority for artifact accounting: the socket's `artifact_chunk` frames
    /// only announce, they never carry the bulk bytes (task §7.3, §10).
    ArtifactStored {
        /// Job that produced it.
        job: JobId,
        /// Artifact handle.
        artifact: ArtifactId,
        /// Worker id bound to the upload token.
        worker: WorkerId,
        /// Connection id of that worker's live socket.
        conn: u64,
        /// Attempt the upload belongs to.
        attempt: u32,
        /// Chunk index.
        chunk_index: u64,
        /// Bytes in this chunk.
        chunk_bytes: u64,
        /// True when this PUT completed the artifact.
        complete: bool,
        /// Digest the hub computed over what it actually stored.
        stored_sha256: Option<String>,
        /// What the worker declared.
        declared: DeclaredArtifact,
        /// MIME type, when declared.
        content_type: Option<String>,
    },
    /// A worker asks for a spilled payload it was pointed at by `Assigned`.
    /// Answered with [`Effect::PayloadBytes`]; the fetch is *counted*, so prompt
    /// re-reads by an old attempt are visible (task §10).
    FetchPayload {
        /// Reference being fetched.
        reference: PayloadRefId,
        /// Bearer token presented.
        token: String,
        /// Worker presenting it.
        worker: WorkerId,
        /// Attempt the worker claims.
        attempt: u32,
    },
    /// Read-only snapshot request: the actor answers with [`Effect::Snapshot`].
    ///
    /// This is how `GET /v1/jobs/{id}` reads authoritative state without a shared
    /// mutable job map: the answer comes back through the same single-owner
    /// mailbox that mutates it, so it can never be a torn read.
    Query {
        /// Job to read.
        job: JobId,
    },
    /// Read-only whole-pool snapshot for the monitoring surface: the actor answers
    /// with [`Effect::Observed`], built by [`PoolActor::pool_snapshot`].
    ///
    /// Same serialized-read contract as [`ActorMsg::Query`] — the answer arrives
    /// from the same single-owner loop that mutates the state, so it cannot be a
    /// torn mix of two transitions. It carries no reply channel because **this
    /// crate has no tokio** (the hard rule at the top of `nexus-core/Cargo.toml`:
    /// `wasm32-unknown-unknown` has no IO or time driver), so the reply travels as
    /// an effect and the embedder owns the waiting side. The native hub does that
    /// in `state::Hub::pool_snapshot`, keyed by a per-request token so two
    /// concurrent monitors of one pool cannot be handed each other's snapshot.
    Observe {
        /// Correlation id, echoed verbatim into [`Effect::Observed`] so the
        /// embedder can route the answer to whoever asked. The actor has no use
        /// for it and does not interpret it.
        token: u64,
    },
    /// Virtual clock advance: leases, dispatch deadlines, TTLs, keepalives.
    Tick {
        /// Current time.
        now_ms: Timestamp,
    },
    /// Hub is draining: cancel everything, fail consumers retryably.
    Shutdown {
        /// Current time.
        now_ms: Timestamp,
    },
}

/// Output of the actor: everything the embedder must do, in order.
///
/// `PartialEq` (not `Eq`) because payloads and usage are `serde_json::Value`,
/// which is only partially equal — floats. Tests rely on this comparison.
///
/// `Serialize` is load-bearing for the WASM boundary: `nexus-wasm` hands a JS host
/// the effect list as JSON (`to_json` in `crates/nexus-wasm/src/lib.rs`), so the
/// JS host and the native dispatcher consume one contract. Encoding is serde's
/// default **external** tagging — an accept is `{"Accepted": {"job": …}}` — which
/// is what `examples/node-smoke.mjs` reads.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum Effect {
    /// Submission accepted, or attached to an existing execution (§5.2).
    Accepted {
        /// Job accepted or attached.
        job: JobId,
        /// True when this joined an existing execution instead of queueing a
        /// second one.
        attached: bool,
        /// Events to hand this consumer before live relay begins.
        replayed: Vec<ConsumerEvent>,
    },
    /// Refuse before any body bytes.
    Reject {
        /// Job id when one was allocated.
        job: Option<JobId>,
        /// HTTP status to answer with.
        status: u16,
        /// Stable machine code.
        code: String,
        /// Human detail.
        message: String,
        /// `Retry-After` in milliseconds.
        retry_after_ms: Option<u64>,
    },
    /// Write this frame to that worker's socket — the mpsc send of task §5.
    SendWorker {
        /// Destination worker.
        worker: WorkerId,
        /// Frame to send.
        ///
        /// Boxed, not inline: `HubFrame::Assigned` carries the payload, the
        /// `Requires` set and the artifact grant, so a `HubFrame` is ~430 bytes
        /// and the effect list the actor returns per message would be dominated
        /// by frame copies (`clippy::large_enum_variant`). serde unwraps `Box`
        /// transparently, so the JSON effect contract the WASM host and
        /// `examples/node-smoke.mjs` read is unchanged.
        frame: Box<HubFrame>,
    },
    /// Fan these events out to every consumer attached to the job.
    Relay {
        /// Job the events belong to.
        job: JobId,
        /// Events, already flush-batched.
        events: Vec<ConsumerEvent>,
    },
    /// Send these buffered events to exactly one newly attached consumer.
    ReplayTo {
        /// Job being followed.
        job: JobId,
        /// Consumer to serve.
        consumer: ConsumerId,
        /// Buffered events.
        events: Vec<ConsumerEvent>,
        /// True when the ring had already evicted part of what was requested,
        /// so the adapter must surface a gap rather than fake continuity.
        lossy: bool,
    },
    /// Emit one SSE comment (`: ping`) for a job whose headers are already out.
    Keepalive {
        /// Job to keep warm.
        job: JobId,
    },
    /// The hub may drop the payload retained for a job.
    ReleasePayload {
        /// Job whose payload may go.
        job: JobId,
        /// Reference that was spilled, when there was one.
        reference: Option<PayloadRefId>,
    },
    /// Delete an artifact whose TTL lapsed.
    DeleteArtifact {
        /// Artifact handle.
        artifact: ArtifactId,
        /// Path or storage key.
        path: String,
    },
    /// Job reached a terminal state.
    Terminal {
        /// Terminated job.
        job: JobId,
        /// Why.
        reason: TerminalReason,
        /// Whether the consumer may retry.
        retryable: bool,
    },
    /// The actor has **dropped the job record entirely**: no queue entry, no lease, no
    /// replay ring, and no idempotency entry that would hand the same answer to a later
    /// stranger. The job id is now unknown, so a later submission with its key executes
    /// afresh rather than attaching to it.
    ///
    /// Emitted at every place a record leaves `PoolActor::jobs`:
    ///   * the NEX-6.3 consumer-gone orphan deletion (`on_detach`, same mailbox step as
    ///     the `Cancel` and the `Terminal`),
    ///   * the NEX-6.6 unclaimed-detached sweep (`on_tick` step 9),
    ///   * the `job_retention_ms` sweep (`on_tick` step 10).
    ///
    /// The embedder must drop every per-job side structure it keys by job id — the
    /// server's routing index and its relay fan-out (`nexus-server/src/state.rs`) — or
    /// those maps grow for the process lifetime while the actor has bounded its own.
    JobForgotten {
        /// Job whose record, replay buffer and idempotency entry are gone.
        job: JobId,
    },
    /// Answer to [`ActorMsg::FetchPayload`]: the payload bytes, or the refusal.
    PayloadBytes {
        /// Reference fetched.
        reference: PayloadRefId,
        /// True when the fetch was authorized.
        granted: bool,
        /// Body bytes; empty when refused.
        bytes: Vec<u8>,
        /// Declared content type.
        content_type: String,
        /// Digest of the body.
        sha256: String,
        /// Why a refusal happened.
        reason: Option<String>,
    },
    /// Answer to [`ActorMsg::Query`]: the derived view of one job.
    Snapshot {
        /// Job read.
        job: JobId,
        /// `None` when the job is unknown or has left retention.
        ///
        /// Boxed on purpose: `JobView` is by far the largest payload in this enum
        /// (~700 B), and `Effect` values live in `Vec<Effect>` on every mailbox step.
        /// A query is the rarest effect by orders of magnitude, so paying one
        /// indirection there is what keeps the relay path's `Effect` at the size of its
        /// next-largest variant (a boxed `SendWorker` frame) instead of copying a whole
        /// view around for every delta.
        view: Box<Option<JobView>>,
    },
    /// Answer to [`ActorMsg::Observe`]: one pool's whole state, as its own actor
    /// sees it.
    ///
    /// Boxed for the reason `Effect::Snapshot` states — a snapshot carries every
    /// worker and every active job of the pool, which is by far the largest thing
    /// in this enum, and `Effect` values live in `Vec<Effect>` on every mailbox
    /// step. `Serialize` (the `Effect` derive) is the WASM half of the same
    /// contract: a JS host that sends `observe` gets the identical JSON shape the
    /// native hub renders.
    Observed {
        /// Correlation id from [`ActorMsg::Observe`], echoed unchanged. A pool has
        /// one actor but may be watched by several monitors at once, and the actor
        /// answers in mailbox order — routing by token is what pairs each answer
        /// with the request that caused it instead of with whoever happens to be
        /// waiting.
        token: u64,
        /// Whole-pool snapshot.
        snapshot: Box<PoolSnapshot>,
    },
    /// Structured log line for the embedder.
    Log {
        /// Severity.
        level: LogLevel,
        /// Message.
        message: String,
    },
}

/// One pool's entire mutable state.
pub struct PoolActor {
    pool: PoolId,
    cfg: BrokerConfig,
    allowlist: AttestationAllowlist,
    queue: Box<dyn QueueBackend>,
    workers: BTreeMap<WorkerId, WorkerRecord>,
    live_conn: BTreeMap<WorkerId, u64>,
    next_conn: u64,
    arrival_seq: u64,
    orphan_timer: BTreeSet<(Timestamp, WorkerId)>,
    jobs: BTreeMap<JobId, JobRecord>,
    leases: BTreeMap<JobId, Lease>,
    lease_timer: BTreeSet<(Timestamp, JobId)>,
    claims: VecDeque<(WorkerId, u32)>,
    idempotency: BTreeMap<String, (JobId, Timestamp)>,
    idempotency_timer: BTreeSet<(Timestamp, String)>,
    tenant_active: BTreeMap<TenantId, usize>,
    retention_timer: BTreeSet<(Timestamp, JobId)>,
    payloads: PayloadBook,
    artifacts: ArtifactBook,
    metrics: Metrics,
    now_ms: Timestamp,
    closed: bool,
}

impl PoolActor {
    /// Actor for `pool`, with the default artifact root.
    #[must_use]
    pub fn new(pool: PoolId, cfg: BrokerConfig) -> Self {
        Self::with_artifact_root(pool, cfg, "artifacts")
    }

    /// Actor for `pool` whose artifacts live under `artifact_root` (server-side
    /// directory or object-storage prefix).
    ///
    /// An **empty** root is not "the current directory": it selects NEX-7.1's
    /// memory-only mode, in which artifact-declaring kinds are refused at admission
    /// (`on_submit`'s `artifact_unavailable`) instead of being written anywhere. The
    /// native runtime passes `NEXUS_ARTIFACT_ROOT` through verbatim, so the empty string
    /// is the operator's opt-out and an unset variable still means a real root.
    #[must_use]
    pub fn with_artifact_root(pool: PoolId, cfg: BrokerConfig, artifact_root: &str) -> Self {
        let limits = cfg.limits.clone();
        let depth = cfg.queue_depth_for(&pool);
        Self {
            artifacts: ArtifactBook::new(
                artifact_root,
                limits.max_artifact_bytes,
                limits.artifact_chunk_bytes,
                limits.artifact_ttl_ms,
            ),
            allowlist: AttestationAllowlist::from_config(&cfg.auth),
            queue: Box::new(InMemoryQueue::new(depth)),
            payloads: PayloadBook::new(
                limits.max_spilled_payloads,
                limits.max_spill_bytes,
                limits.payload_ttl_ms,
            ),
            pool,
            cfg,
            now_ms: 0,
            next_conn: 1,
            arrival_seq: 0,
            claims: VecDeque::new(),
            workers: BTreeMap::new(),
            live_conn: BTreeMap::new(),
            orphan_timer: BTreeSet::new(),
            jobs: BTreeMap::new(),
            leases: BTreeMap::new(),
            lease_timer: BTreeSet::new(),
            idempotency: BTreeMap::new(),
            idempotency_timer: BTreeSet::new(),
            tenant_active: BTreeMap::new(),
            retention_timer: BTreeSet::new(),
            metrics: Metrics::new(),
            closed: false,
        }
    }

    // ------------------------------------------------------------------ accessors

    /// Pool this actor owns.
    #[must_use]
    pub fn pool(&self) -> &PoolId {
        &self.pool
    }

    /// Config in force.
    #[must_use]
    pub fn config(&self) -> &BrokerConfig {
        &self.cfg
    }

    /// Limits shorthand.
    #[must_use]
    pub fn limits(&self) -> &Limits {
        &self.cfg.limits
    }

    /// Metrics registry, for rendering `/metrics`.
    #[must_use]
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// Payload book, read by the ref-fetch endpoint.
    #[must_use]
    pub fn payloads(&self) -> &PayloadBook {
        &self.payloads
    }

    /// Artifact book, read by the result endpoint.
    #[must_use]
    pub fn artifacts(&self) -> &ArtifactBook {
        &self.artifacts
    }

    /// Workers known to this pool.
    #[must_use]
    pub fn workers(&self) -> &BTreeMap<WorkerId, WorkerRecord> {
        &self.workers
    }

    /// Live lease count (the invariant the acceptance tests assert).
    #[must_use]
    pub fn leases_len(&self) -> usize {
        self.leases.len()
    }

    /// Pending terminal-job retention entries (bounds the jobs map).
    #[must_use]
    pub fn retention_len(&self) -> usize {
        self.retention_timer.len()
    }

    /// Mutable limits, for hosts that retune a running pool. The actor re-reads
    /// these on every decision, so a change takes effect on the next message.
    pub fn limits_mut(&mut self) -> &mut Limits {
        &mut self.cfg.limits
    }

    /// Mutable artifact book, so the native runtime can attach a real storage
    /// root and record measured digests.
    pub fn artifacts_mut(&mut self) -> &mut ArtifactBook {
        &mut self.artifacts
    }

    /// The authoritative lease for a job.
    #[must_use]
    pub fn lease_of(&self, job: &JobId) -> Option<&Lease> {
        self.leases.get(job)
    }

    /// Read-only job record.
    #[must_use]
    pub fn job(&self, job: &JobId) -> Option<&JobRecord> {
        self.jobs.get(job)
    }

    /// Whether a job id is known at all (drives 404 vs 200).
    #[must_use]
    pub fn has_job(&self, job: &JobId) -> bool {
        self.jobs.contains_key(job)
    }

    /// Queued job count.
    #[must_use]
    pub fn queue_depth(&self) -> usize {
        self.queue.depth()
    }

    /// Queue snapshot for `/healthz`.
    #[must_use]
    pub fn queue_snapshot(&self) -> crate::queue::QueueSnapshot {
        self.queue.snapshot(self.now_ms)
    }

    /// Ready worker count.
    #[must_use]
    pub fn ready_workers(&self) -> usize {
        self.workers
            .values()
            .filter(|w| {
                w.engine == EngineState::Ready
                    && w.link == LinkState::Live
                    && w.pools.contains(&self.pool)
            })
            .count()
    }

    /// Connected workers that are not ready yet (degraded-capacity signal, §8).
    #[must_use]
    pub fn loading_workers(&self) -> usize {
        self.workers
            .values()
            .filter(|w| {
                w.link == LinkState::Live
                    && w.engine != EngineState::Ready
                    && w.pools.contains(&self.pool)
            })
            .count()
    }

    /// Live model names from **ready** workers only, so `/v1/models` reports what
    /// is actually warm (§7.1).
    #[must_use]
    pub fn warm_models(&self) -> BTreeSet<String> {
        self.workers
            .values()
            .filter(|w| {
                w.engine == EngineState::Ready
                    && w.link == LinkState::Live
                    && w.pools.contains(&self.pool)
            })
            .filter_map(|w| w.requires.model.clone())
            .collect()
    }

    /// Whether any connected worker could ever satisfy `requires` (the
    /// capability pre-check of §8; drives the immediate `400`).
    #[must_use]
    pub fn can_satisfy(&self, requires: &Requires) -> bool {
        self.workers.values().any(|w| {
            w.link == LinkState::Live
                && w.pools.contains(&self.pool)
                && requires.satisfied_by(&w.requires)
        })
    }

    /// Derived consumer-facing view of one job.
    #[must_use]
    pub fn job_view(&self, job: &JobId) -> Option<JobView> {
        let r = self.jobs.get(job)?;
        Some(JobView {
            id: r.id.clone(),
            pool: r.pool.clone(),
            kind: r.kind.clone(),
            tenant: r.tenant.clone(),
            phase: r.phase,
            attempt: r.attempt,
            terminal: r.terminal,
            priority: r.priority,
            payload_bytes: r.payload_bytes,
            payload_sha256: r.payload_sha256.clone(),
            requires: r.requires.clone(),
            timings: r.timings,
            acked_seq: r.seq.acked_through(),
            gaps: r.seq.gaps(),
            duplicates: r.seq.duplicates(),
            replay_frames: r.replay.len(),
            consumers: r.consumers.len(),
            delivered_bytes: r.delivered_bytes,
            lease: self.leases.get(job).cloned(),
            artifact: r.artifact.clone(),
            usage: r.usage.clone(),
            finish: r.finish,
            worker_error: r.worker_error.clone(),
            streaming: r.streaming,
            idempotent: r.idempotent,
        })
    }

    /// Whole-pool monitoring snapshot: every worker record and every **active**
    /// job view, plus the pool's own gauges. Answers [`ActorMsg::Observe`].
    ///
    /// Every number here is read from the one value that owns it, which is the
    /// same reason [`PoolActor::job_view`] exists: a monitor that read a hub-side
    /// mirror would be looking at a copy that can lag the truth. The worker filter
    /// is the pool-membership test the readiness counters already use
    /// (`w.pools.contains(&self.pool)`), so a snapshot's worker list can never
    /// disagree with its `ready_workers` / `loading_workers` counts — including for
    /// an **orphaned** record, which the counters deliberately exclude: an orphaned
    /// worker owns a lease but cannot take work, so `in_flight` stays populated and
    /// `connected_workers` does not count it.
    ///
    /// `jobs` is built through [`PoolActor::job_view`] for exactly one reason: the
    /// lease is a second map (`self.leases`), and deriving the view is what joins
    /// job state to its lease without a second copy of either.
    #[must_use]
    pub fn pool_snapshot(&self) -> PoolSnapshot {
        // The three counters are method calls rather than one pass over `workers`,
        // because they *are* the readiness definition (`ready_workers`/
        // `loading_workers`): reusing them keeps `/healthz`, `/metrics` and
        // `/v1/observe` answering from one rule instead of three lookalikes.
        let connected_workers = self
            .workers
            .values()
            .filter(|w| w.link == LinkState::Live && w.pools.contains(&self.pool))
            .count();
        PoolSnapshot {
            pool: self.pool.clone(),
            queue_depth: self.queue.depth(),
            ready_workers: self.ready_workers(),
            loading_workers: self.loading_workers(),
            connected_workers,
            leases: self.leases.len(),
            workers: self
                .workers
                .values()
                .filter(|w| w.pools.contains(&self.pool))
                .map(WorkerView::from_record)
                .collect(),
            // Active records only; see `PoolSnapshot`'s note on retention.
            // `retain` + a second pass rather than `filter_map(job_view)`: the view
            // is derived from `self.jobs` and `self.leases` together, so building it
            // inside the filter would re-look-up the record it just matched.
            jobs: self
                .jobs
                .values()
                .filter(|record| !record.phase.is_terminal())
                .filter_map(|record| self.job_view(&record.id))
                .collect(),
        }
    }

    /// Reserve the next connection id; the transport tags every inbound frame
    /// with it, which is how stale sockets are detected.
    pub fn reserve_conn(&mut self) -> u64 {
        let conn = self.next_conn;
        self.next_conn = self.next_conn.wrapping_add(1);
        conn
    }

    /// True once shutdown has drained the pool.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    // ---------------------------------------------------------------------- entry

    /// The single mutation point. Effects come back in the order they must be
    /// performed: worker writes before consumer relays where it matters, and
    /// deltas before the terminal event that closes them.
    pub fn handle(&mut self, now_ms: Timestamp, msg: ActorMsg) -> Vec<Effect> {
        self.now_ms = now_ms;
        match msg {
            ActorMsg::Submit {
                job,
                submission,
                consumer,
            } => self.on_submit(job, submission, consumer, now_ms),
            ActorMsg::Attach {
                job,
                consumer,
                from_seq,
            } => self.on_attach(job, consumer, from_seq),
            ActorMsg::Detach { job, consumer } => self.on_detach(job, consumer, now_ms),
            ActorMsg::Cancel { job, reason } => self.on_cancel(job, reason, now_ms),
            ActorMsg::Hello {
                worker,
                conn,
                hello,
            } => self.on_hello(worker, conn, hello, now_ms),
            ActorMsg::WorkerGone { worker, conn } => self.on_worker_gone(worker, conn, now_ms),
            ActorMsg::ArtifactStored {
                job,
                artifact,
                worker,
                conn,
                attempt,
                chunk_index,
                chunk_bytes,
                complete,
                stored_sha256,
                declared,
                content_type,
            } => self.on_artifact_stored(
                job,
                artifact,
                worker,
                conn,
                attempt,
                chunk_index,
                chunk_bytes,
                complete,
                stored_sha256,
                declared,
                content_type,
                now_ms,
            ),
            ActorMsg::WorkerFrame {
                worker,
                conn,
                frame,
            } => self.on_worker_frame(worker, conn, frame, now_ms),
            ActorMsg::FetchPayload {
                reference,
                token,
                worker,
                attempt,
            } => {
                // Authorization and the fetch count both happen here, inside the
                // single owner: no torn read, and no second place that decides
                // whether a token is still good.
                match self.payloads.authorize_fetch(
                    &reference,
                    &token,
                    &worker,
                    attempt,
                    self.now_ms,
                ) {
                    Ok(entry) => {
                        let entry = entry.clone();
                        self.payloads.record_fetch(&reference);
                        vec![Effect::PayloadBytes {
                            reference,
                            granted: true,
                            bytes: entry.bytes,
                            content_type: entry.content_type,
                            sha256: entry.sha256,
                            reason: None,
                        }]
                    }
                    Err(err) => vec![Effect::PayloadBytes {
                        reference,
                        granted: false,
                        bytes: Vec::new(),
                        content_type: String::new(),
                        sha256: String::new(),
                        reason: Some(err.to_string()),
                    }],
                }
            }
            ActorMsg::Query { job } => vec![Effect::Snapshot {
                // Boxed payload: see `Effect::Snapshot`. The derivation itself is
                // unchanged, so a snapshot is still exactly what the single owner sees.
                view: Box::new(self.job_view(&job)),
                job,
            }],
            ActorMsg::Observe { token } => vec![Effect::Observed {
                // Boxed payload: see `Effect::Observed`. The token is carried
                // through untouched, so a reply can never be paired with the wrong
                // request when two monitors poll the same pool at once.
                token,
                snapshot: Box::new(self.pool_snapshot()),
            }],
            ActorMsg::Tick { .. } => self.on_tick(now_ms),
            ActorMsg::Shutdown { .. } => self.on_shutdown(now_ms),
        }
    }

    fn on_worker_frame(
        &mut self,
        worker: WorkerId,
        conn: u64,
        frame: WorkerFrame,
        now_ms: Timestamp,
    ) -> Vec<Effect> {
        match frame {
            WorkerFrame::Ready {
                free_slots,
                free_kv_tokens,
                engine_ready,
                telemetry,
            } => self.on_ready(
                worker,
                conn,
                free_slots,
                free_kv_tokens,
                engine_ready,
                telemetry,
                now_ms,
            ),
            WorkerFrame::Claim { jobs } => self.on_claim(worker, conn, jobs, now_ms),
            WorkerFrame::Frame {
                job,
                seq,
                kind,
                content_type,
                text,
                artifact,
            } => self.on_frame(
                worker,
                conn,
                job,
                seq,
                kind,
                content_type,
                text,
                artifact,
                now_ms,
            ),
            WorkerFrame::Done {
                job,
                seq,
                finish,
                usage,
                error,
            } => self.on_done(worker, conn, job, seq, finish, usage, error, now_ms),
            WorkerFrame::Resume {
                job,
                attempt,
                acked_seq,
            } => self.on_resume(worker, conn, job, attempt, acked_seq, now_ms),
            WorkerFrame::CancelAck { job } => self.on_cancel_ack(worker, conn, job, now_ms),
            WorkerFrame::Hello { .. } => {
                // A second `Hello` on a live socket is a re-handshake (a cell
                // re-run reusing the connection): close the old generation and
                // open the new one so no lease points at the previous epoch.
                //
                // `expect` is infallible by invariant and stays that way on purpose: this
                // arm just matched `WorkerFrame::Hello`, and `HelloInput::from_frame` only
                // returns `None` for a frame that is *not* a `Hello`. Silently returning no
                // effects here would swallow a handshake instead of failing loudly, which
                // is the worse failure for a socket whose state the actor already trusts.
                #[allow(clippy::expect_used)]
                let hello = HelloInput::from_frame(&frame).expect("matched on Hello");
                let mut effects = self.on_worker_gone(worker.clone(), conn, now_ms);
                effects.extend(self.on_hello(worker, conn, hello, now_ms));
                effects
            }
        }
    }

    // --------------------------------------------------------------------- submit

    fn on_submit(
        &mut self,
        job: JobId,
        sub: Submission,
        consumer: ConsumerId,
        now_ms: Timestamp,
    ) -> Vec<Effect> {
        if self.closed {
            return vec![reject(
                None,
                503,
                "hub_shutdown",
                "hub is draining, retry",
                Some(1_000),
            )];
        }
        let Some(kind) = self.cfg.kind(&sub.kind).cloned() else {
            self.metrics
                .incr_label(names::JOBS_REJECTED, "reason", "unknown_kind", 1);
            return vec![reject(
                Some(job),
                400,
                "unknown_kind",
                &format!("kind {:?} is not registered", sub.kind),
                None,
            )];
        };
        if sub.pool != self.pool {
            return vec![reject(
                Some(job),
                400,
                "wrong_pool",
                &format!(
                    "kind {:?} belongs to pool {sub_pool}, this actor owns {pool}",
                    sub.kind,
                    sub_pool = sub.pool,
                    pool = self.pool
                ),
                None,
            )];
        }
        if !self.cfg.tenant_allows_pool(&sub.tenant, &self.pool) {
            self.metrics
                .incr_label(names::JOBS_REJECTED, "reason", "tenant_pool_forbidden", 1);
            return vec![reject(
                Some(job),
                403,
                "tenant_pool_forbidden",
                "this tenant may not submit to this pool",
                None,
            )];
        }

        // NEX-7.1 / §9.1 item 5 — **in-memory-only mode**. An empty artifact root is the
        // operator's explicit "this deployment has no scratch disk" opt-out
        // (`NEXUS_ARTIFACT_ROOT=""`; the built-in default still applies when the variable
        // is absent), and bulk artifact egress is the only thing NEX-7.4 lets this hub
        // write anywhere.
        //
        // A non-streaming kind has no other way to deliver a result: dispatch hands the
        // worker a pre-signed upload slot (`PoolActor::dispatch`, the
        // `if record.streaming { None } else { … }` branch), so accepting the job here
        // would only move the failure to the worker's first `PUT` — after the response
        // body is already open, where a clean 4xx is no longer legal (§7.2). Refusing at
        // admission, before any body byte, is the answer the client can act on.
        //
        // "Declares artifact output" is `!streaming`: the same test dispatch uses to
        // decide whether to allocate a slot, so admission and dispatch can never
        // disagree about which kinds need disk.
        if !kind.streaming && self.artifacts.root().is_empty() {
            self.metrics
                .incr_label(names::JOBS_REJECTED, "reason", "artifact_unavailable", 1);
            return vec![reject(
                Some(job),
                400,
                "artifact_unavailable",
                &format!(
                    "kind {:?} returns an artifact, but this hub runs in memory-only mode (empty artifact root): submit a streaming kind, or point NEXUS_ARTIFACT_ROOT at scratch storage",
                    sub.kind
                ),
                None,
            )];
        }

        // Payload budget: the cheap prompt-size mitigation of task §8 plus the
        // §10 body cap.
        let body = serde_json::to_vec(&sub.payload).unwrap_or_default();
        let body_len = body.len() as u64;
        let payload_sha = sha256_hex(&body);
        let max_body = self.cfg.tenant_payload_bytes_for(&sub.tenant);
        if body_len > max_body {
            self.metrics
                .incr_label(names::JOBS_REJECTED, "reason", "payload_too_large", 1);
            return vec![reject(
                Some(job),
                413,
                "payload_too_large",
                &format!("body is {body_len} bytes, the limit is {max_body}"),
                None,
            )];
        }
        let max_tokens = self.limits().tenant_max_prompt_tokens;
        if sub
            .requires
            .kv_estimate_tokens
            .is_some_and(|estimate| estimate > max_tokens)
        {
            self.metrics
                .incr_label(names::JOBS_REJECTED, "reason", "prompt_too_large", 1);
            return vec![reject(
                Some(job),
                413,
                "prompt_too_large",
                &format!("prompt estimate exceeds the {max_tokens} token limit"),
                None,
            )];
        }

        // Idempotency attach runs **before** the capability pre-check and the
        // concurrency cap: a duplicate submission is not new work (§5.2), so it
        // must not be refused for reasons that only apply to new work.
        if let Some((existing, _)) = self.idempotency.get(&sub.idempotency_key).cloned() {
            if self.jobs.contains_key(&existing) {
                let (replayed, _) = self.collect_replay(&existing, 0).unwrap_or_default();
                // Defensive re-lookup: `contains_key` was checked a moment ago in the same
                // mailbox step, so this cannot miss, and a miss must stay as silent as it was
                // before this branch grew a second statement.
                if let Some(record) = self.jobs.get_mut(&existing) {
                    record.consumers.insert(consumer);
                    // The duplicate *is* a consumer arriving, so the NEX-6.6 unwatched window
                    // closes: while somebody is following a detached job, its bytes have the
                    // stated reason NEX-7.2 asks for and the subscriber-less clock must not run.
                    record.subscriberless_since = None;
                }
                self.metrics.incr(names::IDEMPOTENT_ATTACH, 1);
                return vec![Effect::Accepted {
                    job: existing,
                    attached: true,
                    replayed,
                }];
            }
            self.forget_idempotency(&sub.idempotency_key);
        }

        // Tenant concurrency cap (task §8 fairness).
        let cap = self.cfg.tenant_concurrency_for(&sub.tenant);
        if self.tenant_active.get(&sub.tenant).copied().unwrap_or(0) >= cap {
            self.metrics
                .incr_label(names::JOBS_REJECTED, "reason", "tenant_limit", 1);
            return vec![reject(
                Some(job),
                429,
                "tenant_limit",
                &format!("tenant already has {cap} jobs in flight"),
                Some(1_000),
            )];
        }

        // Capability pre-check → immediate 400, never a 60 s timeout behind a
        // model that had to load 50 GB (task §8, acceptance test 10).
        if !self.can_satisfy(&sub.requires) {
            self.metrics
                .incr_label(names::JOBS_REJECTED, "reason", "capability_unsatisfied", 1);
            return vec![reject(
                Some(job),
                400,
                "capability_unsatisfied",
                &unsatisfiable_reason(&sub.requires),
                None,
            )];
        }

        // Nothing warm yet: hold the response open for the §7.2 grace window,
        // during which no body bytes are written, so a 503 is still legal.
        let ready_now = self.ready_workers() > 0;

        let limits = self.limits().clone();
        let no_worker_grace = kind.no_worker_grace_ms.unwrap_or(limits.no_worker_grace_ms);
        let claim_deadline = kind.claim_deadline_ms.unwrap_or(limits.claim_deadline_ms);
        let hard_cap = kind.job_hard_cap_ms.unwrap_or(limits.job_hard_cap_ms);
        let dispatch_deadline = if ready_now {
            now_ms.saturating_add(claim_deadline)
        } else {
            now_ms.saturating_add(no_worker_grace)
        };
        let arrival = self.next_arrival_seq();
        let mut record = JobRecord::new(
            job.clone(),
            &sub,
            body_len,
            payload_sha.clone(),
            now_ms,
            dispatch_deadline,
            now_ms.saturating_add(hard_cap),
            limits.replay_frames,
            limits.replay_bytes,
        );
        record.queue_seq = Some(arrival);
        record.consumers.insert(consumer);

        // Over the inline cap: keep the bytes in memory (never on disk, §9) and
        // let the worker fetch them by reference after the claim (§10, test 16).
        if PayloadBook::needs_spill(body.len(), limits.inline_payload_cap_bytes) {
            let token = format!("ptok-{}", job.as_ref());
            match self.payloads.register(
                &job,
                &token,
                payload_sha,
                body,
                sub.content_type.clone(),
                now_ms,
            ) {
                Ok(descriptor) => {
                    record.payload_ref = Some(descriptor.id);
                    record.payload_inline = None;
                    self.metrics.incr(names::SPILL_BYTES, descriptor.size_bytes);
                }
                Err(err) => {
                    self.metrics
                        .incr_label(names::JOBS_REJECTED, "reason", "spill_budget", 1);
                    return vec![reject(
                        Some(job),
                        429,
                        "spill_budget",
                        &err.to_string(),
                        Some(1_000),
                    )];
                }
            }
        }

        record.phase = if ready_now {
            JobPhase::Queued
        } else {
            JobPhase::Pending
        };
        let seq = record.queue_seq.unwrap_or(arrival);
        if let Err(QueueError::Full { limit }) =
            self.queue
                .enqueue(&job, record.priority, seq, record.dispatch_deadline_ms)
        {
            self.metrics
                .incr_label(names::JOBS_REJECTED, "reason", "queue_full", 1);
            if let Some(id) = &record.payload_ref {
                self.payloads.release(id);
            }
            return vec![reject(
                Some(job),
                429,
                "queue_full",
                &format!("pool queue is saturated at {limit} jobs"),
                Some(1_000),
            )];
        }

        self.jobs.insert(job.clone(), record);
        *self.tenant_active.entry(sub.tenant.clone()).or_insert(0) += 1;
        self.remember_idempotency(&sub.idempotency_key, &job, now_ms);
        self.metrics.incr(names::JOBS_ACCEPTED, 1);
        self.refresh_gauges();

        let mut effects = vec![Effect::Accepted {
            job,
            attached: false,
            replayed: Vec::new(),
        }];
        effects.extend(self.fulfil_claims(now_ms));
        effects
    }

    fn next_arrival_seq(&mut self) -> u64 {
        self.arrival_seq = self.arrival_seq.wrapping_add(1);
        self.arrival_seq
    }

    fn remember_idempotency(&mut self, key: &str, job: &JobId, now_ms: Timestamp) {
        // Bound the map by evicting the entry that expires first, so a long-lived
        // key is never dropped ahead of a stale one.
        if self.idempotency.len() >= self.limits().max_idempotency_entries
            && !self.idempotency.contains_key(key)
        {
            // `(expiry, key)` is not `Copy` (the key is a String), so the timer entry
            // has to be cloned out of the BTreeSet before it can be removed.
            if let Some((expiry, oldest)) = self.idempotency_timer.iter().next().cloned() {
                self.idempotency_timer.remove(&(expiry, oldest.clone()));
                self.idempotency.remove(&oldest);
            }
        }
        if let Some((_, old_expiry)) = self.idempotency.get(key).cloned() {
            self.idempotency_timer
                .remove(&(old_expiry, key.to_string()));
        }
        let expires = now_ms.saturating_add(self.limits().idempotency_ttl_ms);
        self.idempotency
            .insert(key.to_string(), (job.clone(), expires));
        self.idempotency_timer.insert((expires, key.to_string()));
    }

    fn forget_idempotency(&mut self, key: &str) {
        if let Some((_, expiry)) = self.idempotency.remove(key) {
            self.idempotency_timer.remove(&(expiry, key.to_string()));
        }
    }

    fn on_attach(&mut self, job: JobId, consumer: ConsumerId, from_seq: u64) -> Vec<Effect> {
        let Some(record) = self.jobs.get_mut(&job) else {
            return vec![reject(
                Some(job),
                404,
                "unknown_job",
                "no such job, or its retention window has passed",
                None,
            )];
        };
        record.consumers.insert(consumer.clone());
        // A re-attached consumer restarts the keepalive cadence immediately.
        record.last_ping_ms = None;
        // An attached consumer is the stated reason a job's bytes still exist
        // (NEX-7.2), so the NEX-6.6 subscriber-less window stops running for as long as
        // anybody is watching — including a watcher that only opened `/events` to poll.
        record.subscriberless_since = None;
        let attached_terminal = record.phase.is_terminal();
        let (mut events, lossy) = self.collect_replay(&job, from_seq).unwrap_or_default();
        if attached_terminal && events.is_empty() {
            events.push(ConsumerEvent::Error {
                code: "retention_expired".to_string(),
                message: "the job finished but its replay buffer is gone".to_string(),
                retryable: false,
                through_seq: None,
            });
        }
        vec![Effect::ReplayTo {
            job,
            consumer,
            events,
            lossy,
        }]
    }

    /// Replay a job's buffered events, flagging when the ring already evicted
    /// part of what was asked for (§7.5).
    fn collect_replay(&self, job: &JobId, from_seq: u64) -> Option<(Vec<ConsumerEvent>, bool)> {
        let record = self.jobs.get(job)?;
        let lossy = record.replay.replay_is_lossy(from_seq);
        // `since` yields `Vec<&BufferedFrame>`, so `into_iter` (not `iter`) is what
        // hands `frame_event` the `&BufferedFrame` its signature asks for.
        let mut events: Vec<ConsumerEvent> = record
            .replay
            .since(from_seq)
            .into_iter()
            .map(frame_event)
            .collect();
        if let Some(finish) = record.finish {
            events.push(ConsumerEvent::Finish {
                seq: record.seq.acked_through(),
                finish,
                usage: record.usage.clone(),
            });
        }
        if let Some(reason) = record.terminal {
            if reason != TerminalReason::Completed {
                events.push(ConsumerEvent::Error {
                    code: reason.as_str().to_string(),
                    message: format!("job ended: {}", reason.as_str()),
                    retryable: reason.retryable(),
                    through_seq: Some(record.seq.acked_through()),
                });
            }
        }
        Some((events, lossy))
    }

    fn on_detach(&mut self, job: JobId, consumer: ConsumerId, now_ms: Timestamp) -> Vec<Effect> {
        let (empty, terminal, cancel_on_gone) = {
            let Some(record) = self.jobs.get_mut(&job) else {
                return Vec::new();
            };
            record.consumers.remove(&consumer);
            (
                record.consumers.is_empty(),
                record.phase.is_terminal(),
                record.cancel_on_consumer_gone,
            )
        };
        if !empty || terminal {
            // Two "nothing to do" cases, and the second one is a deliberate decision:
            //
            //   * somebody is still attached, so this socket was not the subscription;
            //   * the record is **already** terminal. Here the answer exists and finished
            //     before the socket closed, which is exactly the reason NEX-7.2 allows
            //     retaining bytes ("an answer a retry must not re-run"), and retention
            //     already bounds the record by `job_retention_ms`. Deleting a completed
            //     job because its consumer lingered for one more second would take away
            //     the poll/replay the job API exists for.
            //
            // The case NEX-6.3 targets is the one below: a record that was **still
            // running** when the last consumer left. Its only reason to exist was that
            // consumer.
            return Vec::new();
        }
        // Task §6: the *subscription* decides, not the wire format. An SSE chat
        // request is its own subscription (`cancel_on_consumer_gone: true`), so the
        // last consumer leaving cancels the job. A `POST /v1/jobs` submission is
        // detached on purpose and keeps running so the client can poll later —
        // including when its kind happens to be streaming and the only thing that
        // just hung up is a transient `GET /v1/jobs/{id}/events` watcher.
        if cancel_on_gone {
            // NEX-6.3: "the task is then **deleted**: no queue entry, no lease, no
            // record, no retained stream, no idempotency entry that will hand the same
            // answer to a later stranger. A task with no attached consumer has no reason
            // to exist." So the orphan is dropped in **this same mailbox step**, not
            // after `job_retention_ms`:
            //
            //   * `on_cancel` tells the lease holder first (NEX-6.4), pulls the task out
            //     of the queue (NEX-6.2) and makes it terminal, releasing the prompt;
            //   * `forget_job` then removes the record (the replay ring dies with it —
            //     NEX-6.3's "no retained stream"), un-arms the retention entry
            //     `finish_job` armed, frees a still-held payload reference and a still
            //     recorded lease defensively, and deletes the idempotency entry, so the
            //     next submission with the same key executes **fresh** instead of
            //     attaching to a dead execution and replaying `job ended: cancelled`.
            //
            // This holds whatever `bytes_delivered` says: a consumer-gone task has no
            // reason to exist whether or not it streamed (NEX-6.2/6.3), and the only
            // reader of those bytes was the socket that just closed.
            //
            // The supplier side is untouched: the `Cancel` is the same one the worker
            // already got, its slot is still released on `CancelAck` or lease expiry, and
            // any frame that arrives afterwards finds no lease and takes the existing
            // "refused, stop producing" path (`refuse_foreign_frame`) rather than
            // relaying into nobody.
            let mut effects = self.on_cancel(job.clone(), CancelReason::ConsumerGone, now_ms);
            effects.extend(self.forget_job(&job));
            effects
        } else {
            // NEX-6.6's exception, with §9.1 item 3's cap: the job keeps running for the
            // client that said "pick the result up later", but it is now watched by
            // nobody, so the subscriber-less clock starts (or restarts) here and
            // `on_tick` step 9 deletes it if it is *still* unclaimed when that window
            // lapses. A claimed job is deliberately not swept by this clock — see the
            // note in that step.
            if let Some(record) = self.jobs.get_mut(&job) {
                record.subscriberless_since = Some(now_ms);
            }
            Vec::new()
        }
    }

    /// Drop a job record and **everything keyed to it**, in one step: this is NEX-6.3's
    /// "deleted", and it is the only place a record leaves [`PoolActor::jobs`].
    ///
    /// Concurrency accounting is handled here: a record deleted while it was still
    /// non-terminal still occupied a tenant slot, so its slot is released on the way out;
    /// a terminal record already released it in `finish_job` and is not counted twice.
    ///
    /// Idempotent by construction: a job that is already gone yields no effects, so the
    /// same call is safe from the detach path, the subscriber-less sweep and the retention
    /// sweep.
    fn forget_job(&mut self, job: &JobId) -> Vec<Effect> {
        let Some(record) = self.jobs.remove(job) else {
            return Vec::new();
        };
        // Un-arm retention first: `finish_job` armed a timer for a record that is about
        // to vanish, and a sweep that finds no record is pure noise (`retention_len`
        // also feeds `/metrics`, so a dangling entry would misreport the bound).
        self.retention_timer.retain(|(_, j)| j != job);
        // A queued job must leave the queue too — NEX-6.3's "no queue entry". Removing a
        // job that is not queued (the common terminal case) is a no-op.
        self.queue.remove(job);

        let mut effects = Vec::new();
        // NEX-7.1's concurrency slot. Only a live record still holds one: `finish_job`
        // released it for anything terminal, so decrementing there too would push the
        // tenant's count below zero forever and slowly raise its effective cap.
        if !record.phase.is_terminal() {
            if let Some(count) = self.tenant_active.get_mut(&record.tenant) {
                *count = count.saturating_sub(1);
            }
        }
        // A prompt is normally released by `finish_job`; a record dropped *before* it
        // terminated still holds the bytes, and releasing them here is what stops the
        // spill budget leaking on every deleted detached job.
        if let Some(reference) = &record.payload_ref {
            self.payloads.release(reference);
            effects.push(Effect::ReleasePayload {
                job: job.clone(),
                reference: Some(reference.clone()),
            });
        }
        // Same defensive shape for the lease: every terminal path drops it first, so this
        // only fires for a record deleted while it was still leased.
        if let Some(lease) = self.leases.remove(job) {
            self.lease_timer.remove(&(lease.deadline_ms, job.clone()));
            if let Some(worker) = self.workers.get_mut(&lease.worker) {
                worker.in_flight.remove(job);
            }
        }
        // NEX-6.3's last clause: "no idempotency entry that will hand the same answer to a
        // later stranger". Keyed by the record's own key — which is exactly the map key,
        // because the tenant namespacing happens in the edge adapter before the
        // submission reaches the actor.
        self.forget_idempotency_for(&record.idempotency_key, job);
        effects.push(Effect::JobForgotten { job: job.clone() });
        // A deletion moves every gauge this actor publishes: queue depth, live leases,
        // held prompt bytes. Refreshing here keeps `/metrics` honest on the very next
        // scrape instead of waiting for the next terminal transition.
        self.refresh_gauges();
        effects
    }

    /// Remove an idempotency entry **only while it still points at `job`**.
    ///
    /// The guard matters: the same key may already have been re-pointed at a newer
    /// execution (`remember_idempotency` overwrites), and deleting the key then would
    /// forget a mapping that belongs to a different record.
    fn forget_idempotency_for(&mut self, key: &str, job: &JobId) {
        if self
            .idempotency
            .get(key)
            .is_some_and(|(owner, _)| owner == job)
        {
            self.forget_idempotency(key);
        }
    }

    fn on_cancel(&mut self, job: JobId, reason: CancelReason, now_ms: Timestamp) -> Vec<Effect> {
        if !self.jobs.contains_key(&job) {
            return vec![reject(Some(job), 404, "unknown_job", "no such job", None)];
        }
        self.metrics.incr(names::CANCELS_SENT, 1);
        let mut effects = Vec::new();
        // Tell the owner first: GPU time is the scarce resource (§6).
        if let Some(lease) = self.leases.get(&job).cloned() {
            effects.push(Effect::SendWorker {
                worker: lease.worker,
                frame: Box::new(HubFrame::Cancel {
                    job: job.clone(),
                    reason,
                }),
            });
        }
        if self.queue.remove(&job) {
            // Cancelled before anyone claimed it: no worker was ever involved, so
            // the GPU cost of this job is exactly zero.
            effects.push(Effect::Log {
                level: LogLevel::Info,
                message: format!("{job} was cancelled while queued"),
            });
        }
        // The consumer's view ends now. The worker keeps its in-flight slot until
        // it acks (or the lease lapses), which bounds GPU waste without pretending
        // the slot is free while the engine is still decoding.
        effects.extend(self.finish_job(&job, TerminalReason::Cancelled, now_ms));
        effects
    }

    fn on_cancel_ack(
        &mut self,
        worker: WorkerId,
        conn: u64,
        job: JobId,
        now_ms: Timestamp,
    ) -> Vec<Effect> {
        if self.is_stale(&worker, conn) {
            self.metrics.incr(names::STALE_FRAMES, 1);
            return Vec::new();
        }
        self.metrics.incr(names::CANCEL_ACKS, 1);
        self.drop_lease(&job, &worker);
        let mut effects = Vec::new();
        effects.extend(self.fulfil_claims(now_ms));
        effects
    }

    // ---------------------------------------------------------------------- hello

    fn on_hello(
        &mut self,
        worker: WorkerId,
        conn: u64,
        hello: HelloInput,
        now_ms: Timestamp,
    ) -> Vec<Effect> {
        // Protocol negotiation before anything else: mismatch is a clean reject.
        if let Err(err) = crate::protocol::negotiate(hello.protocol) {
            self.metrics
                .incr_label(names::JOBS_REJECTED, "reason", "protocol_mismatch", 1);
            return vec![
                Effect::SendWorker {
                    worker: worker.clone(),
                    frame: Box::new(HubFrame::Reject {
                        code: err.code().to_string(),
                        message: err.to_string(),
                    }),
                },
                Effect::SendWorker {
                    worker,
                    frame: Box::new(HubFrame::Bye {
                        reason: ByeReason::VersionMismatch,
                    }),
                },
            ];
        }

        // A self-declared model name is not identity: the digests must be on the
        // allowlist (task §9, acceptance test 13).
        if let Err(err) = self.allowlist.verify(&hello.attestation) {
            return vec![
                Effect::SendWorker {
                    worker: worker.clone(),
                    frame: Box::new(HubFrame::Reject {
                        code: "attestation_rejected".to_string(),
                        message: err.to_string(),
                    }),
                },
                Effect::SendWorker {
                    worker,
                    frame: Box::new(HubFrame::Bye {
                        reason: ByeReason::AttestationRejected,
                    }),
                },
            ];
        }

        // Resolve which pools this worker may actually join.
        let joined = self.resolve_pools(&hello);
        let (joined, why_empty) = match joined {
            Ok(pools) => (pools, None),
            Err(reason) => (BTreeSet::new(), Some(reason)),
        };
        if joined.is_empty() {
            let message =
                why_empty.unwrap_or_else(|| "worker joined no pool this hub serves".to_string());
            return vec![
                Effect::SendWorker {
                    worker: worker.clone(),
                    frame: Box::new(HubFrame::Reject {
                        code: "pool_unavailable".to_string(),
                        message: message.clone(),
                    }),
                },
                Effect::SendWorker {
                    worker,
                    frame: Box::new(HubFrame::Bye {
                        reason: ByeReason::AuthRejected,
                    }),
                },
                Effect::Log {
                    level: LogLevel::Warn,
                    message,
                },
            ];
        }

        // Capacity is declared, never assumed, and clamped against nonsense
        // (`capacity: 1000000` must not become unbounded over-dispatch, and `0` would
        // make a connected worker undispatchable forever). `clamp` cannot panic here:
        // both bounds are constants with `1 <= MAX_DECLARED_CAPACITY`.
        let capacity = hello.capacity.clamp(1, MAX_DECLARED_CAPACITY);
        if !self.workers.contains_key(&worker)
            && self.workers.len() >= self.limits().max_workers_per_pool
        {
            let limit = self.limits().max_workers_per_pool;
            return vec![
                Effect::SendWorker {
                    worker: worker.clone(),
                    frame: Box::new(HubFrame::Reject {
                        code: "pool_full".to_string(),
                        message: format!("this hub holds {limit} workers for this pool"),
                    }),
                },
                Effect::SendWorker {
                    worker,
                    frame: Box::new(HubFrame::Bye {
                        reason: ByeReason::Evicted,
                    }),
                },
            ];
        }

        let mut effects = Vec::new();
        // Which socket (if any) this `Hello` replaces: drives the reconnect log line.
        let mut evicted_previous: Option<u64> = None;
        // Dedupe by stable id: evict the stale socket instead of double-dispatching
        // to the same GPU (task §11).
        if let Some(previous) = self.live_conn.get(&worker).copied() {
            if previous != conn {
                self.metrics.incr(names::WORKER_EVCTIONS, 1);
                effects.push(Effect::SendWorker {
                    worker: worker.clone(),
                    frame: Box::new(HubFrame::Bye {
                        reason: ByeReason::Evicted,
                    }),
                });
                // Only the *authority* dies with the old socket. This is a socket
                // swap, not a death: `orphan_worker` here used to demote the engine,
                // zero the slots and schedule a resume-grace expiry that then deleted
                // a perfectly live worker (§11's cell re-run lost its leases that way).
                self.void_claim_authority(&worker);
                evicted_previous = Some(previous);
            }
        }

        // A worker id that already has a record is **re-attaching**, not joining for
        // the first time. Updating the record in place is what keeps the held leases
        // true: `in_flight` is the actor's own slot accounting (clearing it lets a
        // reconnecting 4-slot worker be handed 4 *more* jobs, §4/test 12) and it is
        // the set `Ready` heartbeats renew, so wiping it silently stops lease
        // renewal for a worker that is still streaming.
        let re_attach = self.workers.contains_key(&worker);
        let was_orphaned = self
            .workers
            .get(&worker)
            .is_some_and(|w| w.link == LinkState::Orphaned);
        if let Some(w) = self.workers.get_mut(&worker) {
            w.epoch = conn;
            w.link = LinkState::Live;
            w.resume_deadline_ms = None;
            w.last_seen_ms = now_ms;
            // The declaration is re-read: a re-run cell may report a different
            // capacity, capability floor, attestation or tier.
            w.capacity = capacity;
            w.requires = hello.requires.clone();
            w.attestation = hello.attestation.clone();
            w.pools = joined.clone();
            w.tier = hello.tier.unwrap_or(WorkerTier::Ephemeral);
            // Engine state deliberately differs by how the previous socket ended.
            // Eviction leaves the hub with continuous liveness information, so
            // readiness carries over. After a death, `orphan_worker` demoted the
            // engine and only a fresh `Ready` heartbeat may make this worker
            // dispatchable again (§8's "never dispatch to a non-`ready` worker").
        } else {
            self.workers.insert(
                worker.clone(),
                WorkerRecord::new(
                    worker.clone(),
                    conn,
                    capacity,
                    hello.requires.clone(),
                    hello.attestation.clone(),
                    joined.clone(),
                    hello.tier.unwrap_or(WorkerTier::Ephemeral),
                    now_ms,
                ),
            );
        }
        if re_attach {
            // A resume-grace timer left over from the dead socket must not fire for a
            // worker that is live again: it would expire the leases and delete the
            // record of a healthy pool member.
            self.orphan_timer.retain(|(_, w)| w != &worker);
            // The leases this worker still holds move to the new generation, which is
            // what `Lease::epoch` exists to identify (stale generation, §11).
            let held: Vec<JobId> = self
                .leases
                .values()
                .filter(|lease| lease.worker == worker)
                .map(|lease| lease.job.clone())
                .collect();
            for job in held {
                if let Some(lease) = self.leases.get_mut(&job) {
                    lease.epoch = conn;
                }
            }
            effects.push(Effect::Log {
                level: LogLevel::Info,
                message: format!(
                    "worker {worker} re-attached on conn {conn} ({}); {} lease(s) now point at the new generation",
                    match (evicted_previous, was_orphaned) {
                        (Some(previous), _) =>
                            format!("conn {previous} was evicted, engine state carried over"),
                        (None, true) =>
                            "the previous socket died, so a fresh Ready heartbeat is required before dispatch".to_string(),
                        (None, false) => "record re-declared".to_string(),
                    },
                    self.jobs_for_worker(&worker).len()
                ),
            });
        }
        self.live_conn.insert(worker.clone(), conn);

        // Anything still leased and resumable is offered back in `Welcome`, so a
        // reconnecting client needs no per-job round trip.
        let resumed: Vec<ResumedJob> = self
            .leases
            .values()
            .filter(|lease| lease.worker == worker)
            .map(|lease| ResumedJob {
                job: lease.job.clone(),
                attempt: lease.attempt,
                resume_from_seq: self
                    .jobs
                    .get(&lease.job)
                    .map_or(0, |j| j.seq.acked_through()),
                lease_expires_ms: lease.deadline_ms,
            })
            .collect();

        effects.push(Effect::SendWorker {
            worker: worker.clone(),
            frame: Box::new(HubFrame::Welcome {
                protocol: crate::PROTOCOL_VERSION,
                worker_id: worker,
                epoch: conn,
                pools: joined.iter().cloned().collect(),
                heartbeat_ms: self.cfg.heartbeat_ms_for(&self.pool),
                lease_ms: self.cfg.lease_ms_for(&self.pool),
                frame_cap_bytes: self.limits().worker_frame_text_cap_bytes,
                resumed,
            }),
        });
        self.refresh_gauges();
        // A newly warm worker may absorb queued work at once: §4's `Claim` drives
        // the stream, but a pool with work already waiting must not pay a round
        // trip for it.
        effects.extend(self.fulfil_claims(now_ms));
        effects
    }

    /// Validate the pools a worker asked for against config.
    fn resolve_pools(&self, hello: &HelloInput) -> Result<BTreeSet<PoolId>, String> {
        let mut joined = BTreeSet::new();
        let mut reason = String::new();
        for name in &hello.pools {
            let Ok(id) = PoolId::parse(name) else {
                reason = format!("pool name {name:?} is not a valid id");
                continue;
            };
            let Some(pool_cfg) = self.cfg.pool(&id) else {
                reason = format!("unknown pool {name:?}");
                continue;
            };
            if id != self.pool {
                // Owned by another actor; skipped so one socket can serve several
                // pools.
                continue;
            }
            let tier = hello.tier.unwrap_or(WorkerTier::Ephemeral);
            if !self.cfg.tier_allowed(&id, tier) {
                reason = format!("{tier:?} workers are not allowed in pool {name:?}");
                continue;
            }
            if !pool_cfg.requires.satisfied_by(&hello.requires) {
                reason = format!("worker does not meet the capability floor of pool {name:?}");
                continue;
            }
            joined.insert(id);
        }
        if joined.is_empty() {
            Err(if reason.is_empty() {
                "worker asked for no pool".to_string()
            } else {
                reason
            })
        } else {
            Ok(joined)
        }
    }

    // ---------------------------------------------------------------------- ready

    #[allow(clippy::too_many_arguments)]
    fn on_ready(
        &mut self,
        worker: WorkerId,
        conn: u64,
        free_slots: u32,
        free_kv_tokens: Option<u64>,
        engine_ready: bool,
        telemetry: Option<Value>,
        now_ms: Timestamp,
    ) -> Vec<Effect> {
        if self.is_stale(&worker, conn) {
            self.metrics.incr(names::STALE_FRAMES, 1);
            return Vec::new();
        }
        let Some(w) = self.workers.get_mut(&worker) else {
            return vec![Effect::Log {
                level: LogLevel::Warn,
                message: format!("Ready from unknown worker {worker}"),
            }];
        };
        let was_ready = w.engine == EngineState::Ready;
        let was_orphaned = w.link == LinkState::Orphaned;
        w.last_seen_ms = now_ms;
        // Clamp to declared capacity so an inflated heartbeat cannot over-dispatch
        // (§4), and reconcile against the actor's own in-flight count.
        w.free_slots = free_slots.min(w.capacity);
        w.free_kv_tokens = free_kv_tokens;
        w.telemetry = telemetry;
        if engine_ready {
            w.engine = EngineState::Ready;
        } else if w.engine == EngineState::Ready {
            // A ready engine that reports not-ready again has regressed; stop
            // feeding it work but keep its leases (§5.1).
            w.engine = EngineState::Degraded;
        }
        if was_orphaned {
            w.link = LinkState::Live;
            w.resume_deadline_ms = None;
        }

        // Heartbeats renew every lease this worker holds (§5.1).
        let lease_ms = self.cfg.lease_ms_for(&self.pool);
        let deadline = now_ms.saturating_add(lease_ms);
        let held: Vec<JobId> = w.in_flight.iter().cloned().collect();
        for job in &held {
            if let Some(lease) = self.leases.get_mut(job) {
                self.lease_timer.remove(&(lease.deadline_ms, job.clone()));
                lease.deadline_ms = deadline;
                lease.epoch = conn;
                self.lease_timer.insert((deadline, job.clone()));
            }
        }
        let _ = w; // ends the &mut borrow of self.workers (NLL; `drop(w)` was a no-op)
        if was_orphaned {
            self.orphan_timer.retain(|(_, wid)| wid != &worker);
        }
        self.refresh_gauges();

        let mut effects = Vec::new();
        if !was_ready && engine_ready {
            effects.push(Effect::Log {
                level: LogLevel::Info,
                message: format!(
                    "worker {worker} became ready: capacity {}, model {:?}",
                    self.workers[&worker].capacity, self.workers[&worker].requires.model
                ),
            });
            effects.extend(self.fulfil_claims(now_ms));
        }
        effects
    }

    // ---------------------------------------------------------------------- claim

    fn on_claim(
        &mut self,
        worker: WorkerId,
        conn: u64,
        jobs: Option<u32>,
        now_ms: Timestamp,
    ) -> Vec<Effect> {
        if self.is_stale(&worker, conn) {
            self.metrics.incr(names::STALE_FRAMES, 1);
            return Vec::new();
        }
        let Some(w) = self.workers.get_mut(&worker) else {
            return vec![Effect::Log {
                level: LogLevel::Warn,
                message: format!("Claim from unknown worker {worker}"),
            }];
        };
        w.last_seen_ms = now_ms;
        // Field access, not `self.limits()`: the method borrows all of `self` while
        // `w` already holds a mutable borrow of `self.workers`. Reading
        // `self.cfg.limits` is a disjoint field borrow, so both can be live.
        let asked = jobs.unwrap_or(1).min(self.cfg.limits.max_claim_batch);
        // Authority is capped by declared capacity, so a buggy client cannot
        // hoard claims and starve its peers (task §4).
        let grant = asked.min(
            w.capacity
                .saturating_sub(w.in_flight.len() as u32 + w.outstanding_claims),
        );
        if grant == 0 {
            return Vec::new();
        }
        w.outstanding_claims += grant;
        self.claims.push_back((worker, grant));
        self.fulfil_claims(now_ms)
    }

    /// Drain claim authority in mailbox order, one job per granted slot.
    ///
    /// Determinism and fairness, deliberately separated:
    /// * the *order* authority is honoured is mailbox order (test 2);
    /// * a worker that cannot take anything **right now** (loading, degraded, no
    ///   free slot) keeps its authority in a deferred list, so it is handed the
    ///   next matching job without another round trip;
    /// * a worker that is gone or whose socket was replaced loses its authority,
    ///   which is what stops an evicted notebook cell from hoarding work;
    /// * leftover slots of a multi-slot claim go to the **back** of the list, so a
    ///   worker that claimed 4 cannot take four jobs while a peer that claimed 1
    ///   waits.
    fn fulfil_claims(&mut self, now_ms: Timestamp) -> Vec<Effect> {
        let mut effects = Vec::new();
        let mut deferred: VecDeque<(WorkerId, u32)> = VecDeque::new();
        // Hard bound: no more dispatches than there is work plus authority.
        let budget = self.queue.depth() + self.claims.len() + 1;
        for _ in 0..budget {
            let Some((worker, slots)) = self.claims.pop_front() else {
                break;
            };
            let Some(w) = self.workers.get(&worker) else {
                continue; // unknown worker: authority is void
            };
            let live = self.live_conn.contains_key(&worker);
            if !live || w.link != LinkState::Live {
                // Gone or evicted: void the authority (module docs, stale sockets).
                let _ = w; // ends the immutable borrow before the get_mut below
                if let Some(w) = self.workers.get_mut(&worker) {
                    w.outstanding_claims = w.outstanding_claims.saturating_sub(slots);
                }
                continue;
            }
            // Readiness and capacity are hard gates: §8 "never dispatch to a
            // non-ready worker" (test 11) and §4 "cap in-flight to declared
            // capacity" (test 12).
            if !w.dispatchable() {
                let _ = w; // borrow ends here; the row goes back on the deferred queue
                deferred.push_back((worker, slots));
                continue;
            }
            match self.pick_job_for(&worker) {
                Some(job) => {
                    let _ = w; // borrow of self.workers ends before self.dispatch
                    match self.dispatch(job.clone(), worker.clone(), now_ms) {
                        Ok(mut made) => effects.append(&mut made),
                        Err(err) => {
                            self.metrics.incr_label(
                                names::JOBS_REJECTED,
                                "reason",
                                "dispatch_failed",
                                1,
                            );
                            effects.push(Effect::Log {
                                level: LogLevel::Error,
                                message: format!("dispatch of {job} to {worker} failed: {err}"),
                            });
                            self.queue.remove(&job);
                            // The job was already `Accepted`, so its consumer's
                            // body is open: end it in band rather than leaving
                            // the stream hanging (§7.2).
                            let in_band = self.jobs.get(&job).map(|record| {
                                let message = if record.streaming {
                                    "the hub could no longer hold your payload; re-submit with the same idempotency key"
                                } else {
                                    "the hub could not hold the payload for this job"
                                };
                                ConsumerEvent::Error {
                                    code: TerminalReason::QueueTimeout.as_str().to_string(),
                                    message: message.to_string(),
                                    // `QueueTimeout` is retryable, which is the
                                    // right advice for "the hub could no longer
                                    // hold your body".
                                    retryable: TerminalReason::QueueTimeout.retryable(),
                                    through_seq: Some(record.seq.acked_through()),
                                }
                            });
                            if let Some(event) = in_band {
                                effects.push(Effect::Relay {
                                    job: job.clone(),
                                    events: vec![event],
                                });
                            }
                            effects.extend(self.finish_job(
                                &job,
                                TerminalReason::QueueTimeout,
                                now_ms,
                            ));
                        }
                    }
                    if let Some(w) = self.workers.get_mut(&worker) {
                        w.outstanding_claims = w.outstanding_claims.saturating_sub(1);
                        // Keep the remaining slots, but behind everyone else. This
                        // re-queues into the *live* mailbox rather than the deferred
                        // side list: a row parked until after the loop is what made a
                        // multi-slot `Claim` dispatch exactly one job per pass (the
                        // next `pop_front` saw an empty mailbox and broke), so a 4-slot
                        // worker with four queued jobs stayed at one lease (§4's
                        // declared capacity, acceptance test 12).
                        if slots > 1 {
                            self.claims.push_back((worker.clone(), slots - 1));
                        }
                    }
                }
                None => {
                    let _ = w; // borrow ends; this worker stays deferred with its authority
                    deferred.push_back((worker, slots));
                }
            }
        }
        // Anything left in `claims` when the budget ran out stays in front of the
        // deferred list, preserving mailbox order across passes.
        self.claims.append(&mut deferred);
        effects
    }

    /// First queued job this worker may take, in class+FIFO order.
    ///
    /// Session affinity is applied **here** rather than by ranking workers per
    /// job: authority is per-worker (the pull model of §4) while affinity is
    /// per-job (§8), and the only way to honour both without a second scheduler
    /// is to let a worker claim only the jobs that are meant for it. A job with no
    /// session hint, or whose affinity owner is not dispatchable, is up for grabs —
    /// that is §8's "fall back to least-loaded".
    fn pick_job_for(&self, worker: &WorkerId) -> Option<JobId> {
        let w = self.workers.get(worker)?;
        self.queue
            .ordered(QUEUE_SCAN_LIMIT)
            .into_iter()
            .find(|job| {
                self.jobs.get(job).is_some_and(|record| {
                    (record.phase == JobPhase::Queued || record.phase == JobPhase::Pending)
                        && record.requires.satisfied_by(&w.requires)
                        && self.affinity_allows(worker, record)
                        && match (record.requires.kv_estimate_tokens, w.free_kv_tokens) {
                            (Some(need), Some(free)) => free >= need,
                            _ => true,
                        }
                })
            })
    }

    /// Whether `worker` may take a job whose session hashes elsewhere.
    fn affinity_allows(&self, worker: &WorkerId, record: &JobRecord) -> bool {
        let Some(session) = record.session_hint.as_deref() else {
            return true;
        };
        match scheduler::affinity_target(session, &self.workers) {
            // The affinity owner is connected and able: only it may take this job.
            Some(target) => &target == worker,
            // Nobody dispatchable: the caller's other gates decide.
            None => true,
        }
    }

    /// Lease a job to a worker and build its `Assigned` frame.
    fn dispatch(
        &mut self,
        job: JobId,
        worker: WorkerId,
        now_ms: Timestamp,
    ) -> Result<Vec<Effect>, CoreError> {
        let limits = self.limits().clone();
        let record = self
            .jobs
            .get_mut(&job)
            .ok_or_else(|| CoreError::Queue(format!("{job} vanished")))?;
        if record.phase.is_terminal() {
            return Err(CoreError::Protocol {
                reason: format!("{job} is already terminal"),
            });
        }
        record.attempt += 1;
        let attempt = record.attempt;
        // Seq floor for this attempt (§7.4). A continuation is dispatched to a
        // *different* worker, which would otherwise restart numbering at 1 and
        // have every frame dropped as a duplicate by the per-job seq tracker
        // (silently: the consumer would see nothing after the seam). The hub
        // tells the new owner where to start, exactly as `Resume` does for the
        // same worker. A first attempt has nothing acked, so this is 0.
        let start_seq = record.seq.acked_through();
        let lease_ms = self.cfg.lease_ms_for(&self.pool);
        let lease_deadline = now_ms.saturating_add(lease_ms);

        // Payload: inline when it fits the cap, otherwise re-issue a ref with a
        // fresh token bound to *this* attempt, so an evicted attempt cannot
        // refetch after ownership moved (task §10).
        let payload = if let Some(reference) = record.payload_ref.clone() {
            // The spilled prompt must still be in memory. Handing a worker a
            // reference to bytes that already left would be a silent truncation,
            // so the attempt is refused and the client is told to retry (the
            // prompt itself is the thing that expired).
            let bytes = self
                .payloads
                .payload_bytes(&reference)
                .map(<[u8]>::to_vec)
                .ok_or_else(|| {
                    CoreError::Queue(format!(
                        "{job}'s prompt left memory before dispatch (payload TTL); re-submit"
                    ))
                })?;
            let token = format!("ptok-{job}-{attempt}");
            let descriptor = self.payloads.register(
                &job,
                &token,
                record.payload_sha256.clone(),
                bytes,
                record.content_type.clone(),
                now_ms,
            )?;
            self.payloads.assign_owner(&descriptor.id, &worker, attempt);
            JobPayload::ByRef {
                path: descriptor.path,
                token: descriptor.token,
                sha256: descriptor.sha256,
                size_bytes: descriptor.size_bytes,
                expires_ms: descriptor.expires_ms,
                content_type: record.content_type.clone(),
            }
        } else {
            JobPayload::Inline {
                content_type: record.content_type.clone(),
                body: record.payload_inline.clone().unwrap_or(Value::Null),
            }
        };

        // Non-streaming kinds get a pre-signed upload slot: bulk bytes travel over
        // HTTP so a 40 MB render cannot starve other jobs' deltas on one socket
        // (task §4, §7.3).
        let artifact_put = if record.streaming {
            None
        } else {
            let artifact = ArtifactId::new_unchecked(format!("art-{}", job.as_ref()));
            // Allocation only: the actor needs the slot to exist and be owned, and
            // the returned entry would duplicate what `assign_owner`/`upload_hint`
            // read back out of the book.
            let _ = self.artifacts.register(&job, artifact.clone(), now_ms);
            self.artifacts.assign_owner(&artifact, &worker, attempt);
            let token = format!("atok-{job}-{attempt}");
            self.artifacts.upload_hint(
                &artifact,
                &token,
                now_ms.saturating_add(limits.job_hard_cap_ms),
                limits.artifact_chunk_bytes,
            )
        };

        // Resume-by-continuation: hand the partial text to the next attempt so the
        // consumer sees a seam instead of a dropped response (task §7.4).
        let continuation_text = if record.continuations > 0 && !record.partial_text.is_empty() {
            Some(record.partial_text.clone())
        } else {
            None
        };

        record.phase = JobPhase::Assigned;
        record.timings.dispatch_ms = Some(now_ms);
        let latency = now_ms.saturating_sub(record.timings.accept_ms);
        if let Some(w) = self.workers.get_mut(&worker) {
            w.in_flight.insert(job.clone());
        }
        let conn = self.live_conn.get(&worker).copied().unwrap_or(0);
        self.leases.insert(
            job.clone(),
            Lease {
                job: job.clone(),
                worker: worker.clone(),
                epoch: conn,
                attempt,
                deadline_ms: lease_deadline,
                streamed_bytes: 0,
            },
        );
        self.lease_timer.insert((lease_deadline, job.clone()));
        self.queue.remove(&job);
        self.metrics.observe(names::DISPATCH_LATENCY, latency);

        Ok(vec![Effect::SendWorker {
            worker,
            frame: Box::new(HubFrame::Assigned {
                job,
                attempt,
                kind: record.kind.clone(),
                tenant: record.tenant.clone(),
                requires: record.requires.clone(),
                payload,
                lease_expires_ms: lease_deadline,
                job_deadline_ms: record.deadline_ms,
                start_seq,
                continuation_text,
                artifact_put,
            }),
        }])
    }

    // --------------------------------------------------------------------- frames

    #[allow(clippy::too_many_arguments)]
    fn on_frame(
        &mut self,
        worker: WorkerId,
        conn: u64,
        job: JobId,
        seq: u64,
        kind: FrameKind,
        content_type: Option<String>,
        text: String,
        artifact: Option<crate::protocol::ArtifactManifest>,
        now_ms: Timestamp,
    ) -> Vec<Effect> {
        if self.is_stale(&worker, conn) {
            self.metrics.incr(names::STALE_FRAMES, 1);
            if let Some(w) = self.workers.get_mut(&worker) {
                w.stale_frames += 1;
            }
            return Vec::new();
        }
        // Ownership check. A frame for a job we no longer own (or no longer own on
        // this attempt) is refused and the sender is told to stop: two owners
        // streaming into one consumer is the failure mode §5 exists to prevent.
        let Some(lease) = self.leases.get(&job).cloned() else {
            return self.refuse_foreign_frame(&worker, &job, "no lease for that job");
        };
        let Some(record) = self.jobs.get_mut(&job) else {
            return self.refuse_foreign_frame(&worker, &job, "unknown job");
        };
        if lease.worker != worker || lease.attempt != record.attempt {
            return self.refuse_foreign_frame(
                &worker,
                &job,
                "lease belongs to another worker or attempt",
            );
        }
        if record.phase.is_terminal() || record.cancel_sent_ms.is_some() {
            // The worker kept producing briefly after Cancel: relay nothing, but
            // make it visible in metrics.
            record.post_cancel_frames += 1;
            return Vec::new();
        }

        let verdict = record.seq.accept(seq);
        if verdict == SeqVerdict::Duplicate {
            // Acceptance test 8: a reconnecting worker replaying its tail must
            // not produce duplicate deltas.
            self.metrics.incr(names::FRAMES_DUPLICATE, 1);
            return vec![Effect::SendWorker {
                worker,
                frame: Box::new(HubFrame::Ack {
                    job,
                    seq: record.seq.acked_through(),
                }),
            }];
        }

        let mut events = Vec::new();
        if let SeqVerdict::Gap { expected, got } = verdict {
            self.metrics.incr(names::SEQ_GAPS, 1);
            events.push(ConsumerEvent::Gap { expected, got });
        }
        // `&self.cfg.limits` rather than `self.limits()`: `record` holds a mutable
        // borrow of `self.jobs`, and a disjoint field borrow can coexist with it
        // while a whole-`self` method borrow cannot.
        let relayed = relay_into(
            record,
            &self.cfg.limits,
            FrameInput {
                seq,
                kind,
                text: &text,
                artifact: artifact.as_ref(),
                content_type: content_type.as_deref(),
                now_ms,
            },
        );
        self.metrics
            .incr_label(names::FRAMES, "kind", frame_kind_name(kind), 1);
        let bytes = relayed.iter().map(ConsumerEvent::size_bytes).sum::<usize>();
        self.metrics.incr(names::RELAY_BYTES, bytes as u64);
        if bytes > 0 {
            if let Some(lease) = self.leases.get_mut(&job) {
                lease.streamed_bytes += bytes as u64;
            }
            if let Some(w) = self.workers.get_mut(&worker) {
                w.last_seen_ms = now_ms;
            }
        }
        events.extend(relayed);

        let mut effects = Vec::new();
        if !events.is_empty() {
            effects.push(Effect::Relay {
                job: job.clone(),
                events,
            });
        }
        // Flow control: ack the contiguous prefix, which is also the number a
        // reconnecting worker resumes from.
        effects.push(Effect::SendWorker {
            worker,
            frame: Box::new(HubFrame::Ack {
                job: job.clone(),
                seq: record.seq.acked_through(),
            }),
        });
        effects
    }

    /// Emit everything the flush gate is still holding. Used before a terminal
    /// event so no delta is stranded behind a finish (task §7.2).
    fn pending_batch(&mut self, job: &JobId) -> Vec<ConsumerEvent> {
        let Some(record) = self.jobs.get_mut(job) else {
            return Vec::new();
        };
        if record.flush.pending_frames == 0 {
            return Vec::new();
        }
        let through = record.seq.acked_through();
        let events: Vec<ConsumerEvent> = record
            .replay
            .since(record.flush.last_flushed_seq)
            .into_iter()
            .map(frame_event)
            .collect();
        record.flush.commit_flush();
        record.flush.seal_through(through);
        record.bytes_delivered = true;
        events
    }

    fn refuse_foreign_frame(&mut self, worker: &WorkerId, job: &JobId, why: &str) -> Vec<Effect> {
        self.metrics.incr(names::INBAND_ERRORS, 1);
        vec![
            Effect::Log {
                level: LogLevel::Warn,
                message: format!("worker {worker} sent a frame for {job} that was refused: {why}"),
            },
            Effect::SendWorker {
                worker: worker.clone(),
                frame: Box::new(HubFrame::Cancel {
                    job: job.clone(),
                    reason: CancelReason::LeaseExpired,
                }),
            },
        ]
    }

    #[allow(clippy::too_many_arguments)]
    fn on_done(
        &mut self,
        worker: WorkerId,
        conn: u64,
        job: JobId,
        seq: u64,
        finish: FinishKind,
        usage: Option<Value>,
        error: Option<crate::protocol::WorkerError>,
        now_ms: Timestamp,
    ) -> Vec<Effect> {
        if self.is_stale(&worker, conn) {
            self.metrics.incr(names::STALE_FRAMES, 1);
            return Vec::new();
        }
        let Some(lease) = self.leases.get(&job).cloned() else {
            return self.refuse_foreign_frame(
                &worker,
                &job,
                "Done for a job this worker does not own",
            );
        };
        let outcome = {
            let Some(record) = self.jobs.get_mut(&job) else {
                return self.refuse_foreign_frame(&worker, &job, "Done for an unknown job");
            };
            if record.phase.is_terminal() {
                // Acceptance test 4: never two finish_reasons.
                self.metrics.incr_label(
                    names::JOBS_TERMINAL,
                    "reason",
                    TerminalReason::DuplicateDone.as_str(),
                    1,
                );
                return Vec::new();
            }
            if lease.worker != worker || lease.attempt != record.attempt {
                return self.refuse_foreign_frame(
                    &worker,
                    &job,
                    "Done from a stale owner or attempt",
                );
            }
            if !record.seq.done_is_current(seq) {
                return self.refuse_foreign_frame(
                    &worker,
                    &job,
                    "Done is older than the last accepted frame",
                );
            }
            record.finish = Some(finish);
            record.usage = usage.clone();
            record.worker_error = error.clone();
            record.seq.accept(seq);
            (usage, error)
        };
        let (usage, error) = outcome;
        if let Some(tokens) = usage_tokens(usage.as_ref()) {
            self.metrics.incr(names::TOKENS, tokens);
        }

        // A worker's `Done{error}` is a *definitive* answer, so it is terminal
        // even for an idempotent kind: §5.3 reserves auto-requeue for ambiguity
        // (a lost lease or missing ack), not for a refusal.
        let mut events = self.pending_batch(&job);
        if finish == FinishKind::Error {
            self.metrics.incr(names::INBAND_ERRORS, 1);
            let message = error.map_or_else(
                || "worker reported an error".to_string(),
                |e| format!("{}: {}", e.code, e.message),
            );
            events.push(ConsumerEvent::Error {
                code: "worker_error".to_string(),
                message,
                retryable: false,
                through_seq: Some(seq),
            });
        } else {
            events.push(ConsumerEvent::Finish { seq, finish, usage });
        }

        let mut effects = vec![Effect::Relay {
            job: job.clone(),
            events,
        }];
        effects.extend(self.finish_job(&job, TerminalReason::Completed, now_ms));
        effects.extend(self.fulfil_claims(now_ms));
        effects
    }

    // --------------------------------------------------------------------- resume

    fn on_resume(
        &mut self,
        worker: WorkerId,
        conn: u64,
        job: JobId,
        attempt: u32,
        acked_seq: u64,
        now_ms: Timestamp,
    ) -> Vec<Effect> {
        if self.is_stale(&worker, conn) {
            self.metrics.incr(names::STALE_FRAMES, 1);
            return Vec::new();
        }
        let nack = |reason: String, metrics: &mut Metrics| -> Vec<Effect> {
            metrics.incr(names::RESUME_REJECTED, 1);
            vec![Effect::SendWorker {
                worker: worker.clone(),
                frame: Box::new(HubFrame::ResumeNack {
                    job: job.clone(),
                    attempt,
                    reason,
                }),
            }]
        };
        let Some(lease) = self.leases.get(&job).cloned() else {
            return nack(
                "the job is no longer leased: it was requeued or finished while you were away"
                    .to_string(),
                &mut self.metrics,
            );
        };
        if lease.worker != worker {
            return nack(
                "another worker owns that job now".to_string(),
                &mut self.metrics,
            );
        }
        if lease.attempt != attempt {
            return nack(
                format!(
                    "attempt {attempt} is stale; the current attempt is {}",
                    lease.attempt
                ),
                &mut self.metrics,
            );
        }
        let Some(record) = self.jobs.get_mut(&job) else {
            return nack("the job no longer exists".to_string(), &mut self.metrics);
        };
        if record.phase.is_terminal() {
            return nack("the job already finished".to_string(), &mut self.metrics);
        }

        // Accept: the socket is live again, the lease is renewed, and the worker
        // continues from the seq the hub actually received (test 8).
        self.metrics.incr(names::RESUME_ACCEPTED, 1);
        if let Some(w) = self.workers.get_mut(&worker) {
            w.link = LinkState::Live;
            w.resume_deadline_ms = None;
            w.last_seen_ms = now_ms;
        }
        self.orphan_timer.retain(|(_, wid)| wid != &worker);
        self.live_conn.insert(worker.clone(), conn);
        self.lease_timer.remove(&(lease.deadline_ms, job.clone()));
        let deadline = now_ms.saturating_add(self.cfg.lease_ms_for(&self.pool));
        if let Some(current) = self.leases.get_mut(&job) {
            current.deadline_ms = deadline;
            current.epoch = conn;
        }
        self.lease_timer.insert((deadline, job.clone()));
        record.last_ping_ms = None;
        let through = record.seq.acked_through();
        self.refresh_gauges();

        let resume_from = through.max(acked_seq);
        // Build the log line first: `worker` moves into the `Ack` effect below, and
        // the same vec! element list is evaluated in order.
        let message = format!(
            "worker {worker} resumed job attempt {attempt} from seq {}",
            resume_from + 1
        );
        vec![
            Effect::SendWorker {
                worker,
                frame: Box::new(HubFrame::Ack {
                    job,
                    seq: resume_from,
                }),
            },
            Effect::Log {
                level: LogLevel::Info,
                message,
            },
        ]
    }

    // ---------------------------------------------------------- artifact ingest

    #[allow(clippy::too_many_arguments)]
    fn on_artifact_stored(
        &mut self,
        job: JobId,
        artifact: ArtifactId,
        worker: WorkerId,
        conn: u64,
        attempt: u32,
        chunk_index: u64,
        chunk_bytes: u64,
        complete: bool,
        stored_sha256: Option<String>,
        declared: DeclaredArtifact,
        content_type: Option<String>,
        now_ms: Timestamp,
    ) -> Vec<Effect> {
        if self.is_stale(&worker, conn) {
            self.metrics.incr(names::STALE_FRAMES, 1);
            return vec![reject(
                Some(job),
                401,
                "stale_connection",
                "this worker's connection was replaced",
                None,
            )];
        }
        let owns_it = self
            .leases
            .get(&job)
            .is_some_and(|l| l.worker == worker && l.attempt == attempt);
        if !owns_it {
            return vec![reject(
                Some(job),
                409,
                "stale_attempt",
                "this upload does not belong to the current attempt",
                None,
            )];
        }
        if let Err(err) = self.artifacts.record_chunk(
            &artifact,
            &worker,
            attempt,
            chunk_index,
            chunk_bytes,
            now_ms,
        ) {
            return vec![reject(Some(job), 413, err.code(), &err.to_string(), None)];
        }
        self.metrics.incr(names::ARTIFACT_BYTES, chunk_bytes);
        if !complete {
            return Vec::new();
        }

        let recorded = self
            .artifacts
            .get(&artifact)
            .map_or(0, |r| r.recorded_bytes());
        let stored = stored_sha256.unwrap_or_default();
        let outcome = self.artifacts.complete(
            &artifact,
            declared.sha256.clone(),
            declared.size_bytes,
            stored,
            recorded,
            content_type.clone(),
            now_ms,
        );
        match outcome {
            Ok(record) => {
                if let Some(r) = self.jobs.get_mut(&job) {
                    r.artifact = Some(record.clone());
                }
                let seq = self.jobs.get(&job).map_or(0, |r| r.seq.acked_through());
                vec![Effect::Relay {
                    job,
                    events: vec![ConsumerEvent::Artifact {
                        seq,
                        manifest: crate::protocol::ArtifactManifest {
                            artifact: artifact.to_string(),
                            content_type: Some(record.content_type.clone()),
                            size_bytes: Some(record.size_bytes),
                            sha256: record.stored_sha256.clone(),
                            chunk_index: None,
                            chunk_bytes: Some(record.recorded_bytes()),
                        },
                    }],
                }]
            }
            Err(err) => {
                // The worker's declared digest/size did not match what the hub
                // stored: trust the hub, surface it in band (§9).
                self.metrics.incr(names::INBAND_ERRORS, 1);
                vec![Effect::Relay {
                    job,
                    events: vec![ConsumerEvent::Error {
                        code: "artifact_mismatch".to_string(),
                        message: err.to_string(),
                        retryable: false,
                        through_seq: None,
                    }],
                }]
            }
        }
    }

    // ----------------------------------------------------------------- gone, tick

    /// True when `conn` is not this worker's live connection (or the worker is
    /// unknown), i.e. the frame arrived on a dead socket.
    #[must_use]
    pub fn is_stale(&self, worker: &WorkerId, conn: u64) -> bool {
        match self.live_conn.get(worker) {
            Some(live) => *live != conn,
            None => true,
        }
    }

    fn on_worker_gone(&mut self, worker: WorkerId, conn: u64, now_ms: Timestamp) -> Vec<Effect> {
        // A stale socket closing must not disturb the live one (task §11).
        if self.live_conn.get(&worker).copied() != Some(conn) {
            self.metrics.incr(names::STALE_FRAMES, 1);
            return Vec::new();
        }
        self.orphan_worker(&worker, now_ms)
    }

    /// Drops every unfulfilled `Claim` held by `worker` and returns the slot count
    /// that was cleared, keeping `WorkerRecord::outstanding_claims` in step with the
    /// mailbox. Both a dead socket and a replaced socket void authority (§4: a client
    /// that reconnects must ask for work again), but only the dead one orphans leases.
    fn void_claim_authority(&mut self, worker: &WorkerId) -> u32 {
        let mut freed = 0u32;
        self.claims.retain(|(w, slots)| {
            if w == worker {
                freed += *slots;
                false
            } else {
                true
            }
        });
        if let Some(w) = self.workers.get_mut(worker) {
            w.outstanding_claims = w.outstanding_claims.saturating_sub(freed);
        }
        freed
    }

    /// Mark a worker's connection dead, then either start the resume grace or,
    /// with `resume_grace_ms == 0`, apply the expiry table immediately.
    fn orphan_worker(&mut self, worker: &WorkerId, now_ms: Timestamp) -> Vec<Effect> {
        self.live_conn.remove(worker);
        let grace = self.limits().resume_grace_ms;
        let mut effects = Vec::new();
        if let Some(w) = self.workers.get_mut(worker) {
            w.link = LinkState::Orphaned;
            // Immediate loss of capacity accounting (module rule 1).
            w.free_slots = 0;
            w.engine = if w.engine == EngineState::Ready {
                EngineState::Degraded
            } else {
                w.engine
            };
            w.resume_deadline_ms = Some(now_ms.saturating_add(grace));
        }
        // Claims held by a dead socket are void. `in_flight` is deliberately **not**
        // cleared: the leases still name this worker, and a reconnecting client that
        // resumes must not be able to take a second set of slots while the
        // mid-stream attempt is still owned (task §4's capacity cap, §7.5's resume).
        self.void_claim_authority(worker);

        let held = self.jobs_for_worker(worker).len();
        if grace == 0 {
            effects.extend(self.expire_worker_leases(worker, now_ms));
            self.workers.remove(worker);
        } else {
            self.orphan_timer
                .insert((now_ms.saturating_add(grace), worker.clone()));
            effects.push(Effect::Log {
                level: LogLevel::Warn,
                message: format!("worker {worker} disconnected; holding {held} lease(s) for {grace} ms of resume grace"),
            });
        }
        self.refresh_gauges();
        effects
    }

    fn jobs_for_worker(&self, worker: &WorkerId) -> Vec<JobId> {
        self.leases
            .values()
            .filter(|l| &l.worker == worker)
            .map(|l| l.job.clone())
            .collect()
    }

    fn expire_worker_leases(&mut self, worker: &WorkerId, now_ms: Timestamp) -> Vec<Effect> {
        let jobs = self.jobs_for_worker(worker);
        let mut effects = Vec::new();
        for job in jobs {
            effects.extend(self.resolve_lost_job(&job, worker, now_ms));
        }
        effects
    }

    /// The §5.1/§5.3 decision table for a job whose owner died or lapsed.
    fn resolve_lost_job(
        &mut self,
        job: &JobId,
        worker: &WorkerId,
        now_ms: Timestamp,
    ) -> Vec<Effect> {
        let Some(record) = self.jobs.get_mut(job) else {
            self.leases.remove(job);
            return Vec::new();
        };
        if record.phase.is_terminal() {
            let _ = record; // ends the &mut borrow of self.jobs before drop_lease
            self.drop_lease(job, worker);
            return Vec::new();
        }
        // Snapshot the budget once: `record` is a mutable borrow out of `self.jobs`,
        // and every read below happens while that borrow is live. Field access
        // (`&self.cfg.limits`) keeps it a disjoint borrow instead of a whole-`self`
        // method borrow, and avoids cloning the budget on a hot path.
        let limits = &self.cfg.limits;
        let delivered = record.bytes_delivered;
        let idempotent = record.idempotent;
        let opaque = record.output_opaque;
        let attempts_left = record.attempt < limits.max_attempts;
        let continuation_possible = limits.max_continuations > 0
            && record.continuations < limits.max_continuations
            && idempotent
            && !record.partial_text.is_empty()
            // Opaque output (tool-call chunks, or a stream interleaved with
            // event/log/artifact frames) is **not** auto-continued even when the
            // operator turned continuation on: the hub would re-run a half-written
            // tool argument. `continuation_allows_opaque_output` is the explicit
            // opt-out for a pool whose kinds always restart cleanly (§7.4).
            && (!opaque || limits.continuation_allows_opaque_output);
        // Distinguishes "continuation was never on" from "continuation is on but
        // this attempt's output was opaque", which is otherwise invisible.
        let continuation_refused_opaque = !continuation_possible
            && limits.max_continuations > 0
            && idempotent
            && !record.partial_text.is_empty()
            && opaque
            && !limits.continuation_allows_opaque_output;
        let acked = record.seq.acked_through();
        let _ = record; // last read of the job record; the borrow ends here

        self.drop_lease(job, worker);
        let mut effects = Vec::new();

        if !delivered {
            if idempotent && attempts_left {
                self.metrics
                    .incr_label(names::LEASE_EXPIRIES, "kind", "no_output", 1);
                effects.extend(self.requeue(job, worker, now_ms));
                effects.extend(self.fulfil_claims(now_ms));
                return effects;
            }
            // Non-idempotent: never auto-requeue once assigned (§5.3).
            let reason = if idempotent {
                TerminalReason::WorkerGoneNoOutput
            } else {
                TerminalReason::NonIdempotentAmbiguous
            };
            self.metrics.incr_label(
                names::LEASE_EXPIRIES,
                "kind",
                if idempotent {
                    "no_output"
                } else {
                    "non_idempotent"
                },
                1,
            );
            effects.push(Effect::Relay {
                job: job.clone(),
                events: vec![ConsumerEvent::Error {
                    code: reason.as_str().to_string(),
                    message: if idempotent {
                        "the worker died before producing output; the job was retried".to_string()
                    } else {
                        "the worker died before producing output; this kind is not idempotent, so it will not be retried".to_string()
                    },
                    retryable: reason.retryable(),
                    through_seq: Some(acked),
                }],
            });
            effects.extend(self.finish_job(job, reason, now_ms));
            effects.extend(self.fulfil_claims(now_ms));
            return effects;
        }

        // Bytes are on the wire: the status is spent, so this ends in band, or is
        // re-run as a continuation when that is safe (acceptance test 4).
        if continuation_possible {
            if let Some(r) = self.jobs.get_mut(job) {
                r.continuations += 1;
            }
            self.metrics
                .incr_label(names::LEASE_EXPIRIES, "kind", "continuation", 1);
            effects.extend(self.requeue(job, worker, now_ms));
            effects.push(Effect::Log {
                level: LogLevel::Warn,
                message: format!(
                    "{job} is being re-run as a continuation after {worker} died mid-stream"
                ),
            });
            effects.extend(self.fulfil_claims(now_ms));
            return effects;
        }

        if continuation_refused_opaque {
            effects.push(Effect::Log {
                level: LogLevel::Warn,
                message: format!("{job} ended in band instead of continuing: the relayed output was opaque (tool-call chunks or non-delta frames), so `prompt + partial text` could not be re-run. Set NEXUS_CONTINUATION_ALLOW_OPAQUE=1 to continue anyway (task §7.4)."),
            });
        }

        self.metrics
            .incr_label(names::LEASE_EXPIRIES, "kind", "partial", 1);
        self.metrics.incr(names::INBAND_ERRORS, 1);
        let mut events = self.pending_batch(job);
        events.push(ConsumerEvent::Error {
            code: TerminalReason::WorkerGonePartial.as_str().to_string(),
            message: "the worker died mid-stream; the response is incomplete".to_string(),
            retryable: false,
            through_seq: Some(acked),
        });
        effects.push(Effect::Relay {
            job: job.clone(),
            events,
        });
        effects.extend(self.finish_job(job, TerminalReason::WorkerGonePartial, now_ms));
        effects.extend(self.fulfil_claims(now_ms));
        effects
    }

    /// Put a job back in the queue: `attempt` already counts the failed try, and
    /// `last_worker` makes the next dispatch prefer a different worker (§5.1).
    fn requeue(&mut self, job: &JobId, failed_worker: &WorkerId, now_ms: Timestamp) -> Vec<Effect> {
        let deadline_len = kind_claim_deadline(&self.cfg, &self.jobs[job].kind);
        let arrival = self.next_arrival_seq();
        let Some(record) = self.jobs.get_mut(job) else {
            return Vec::new();
        };
        record.last_worker = Some(failed_worker.clone());
        // Always `Queued`, never `Pending`, however thin the pool looks right now.
        // `Pending` is the pre-answer phase: it is what makes §7.2's `503 +
        // Retry-After` still legal, and `PoolActor::on_tick` only emits that
        // response for a `Pending` job. A requeue has already been dispatched once,
        // so the actor already emitted `Accepted` and the consumer's body is open:
        // a `503` arriving there would be resolved against a gate that is spent and
        // the stream would end with no explanation, so §7.2's in-band error is the
        // only honest answer (and `on_tick`'s non-`Pending` arm is exactly that).
        record.phase = JobPhase::Queued;
        record.queue_seq = Some(arrival);
        record.dispatch_deadline_ms = now_ms.saturating_add(deadline_len);
        record.cancel_sent_ms = None;
        let priority = record.priority;
        let deadline = record.dispatch_deadline_ms;
        if let Err(err) = self.queue.enqueue(job, priority, arrival, deadline) {
            // The queue saturated while this job was in flight: it cannot wait
            // again, so surface the admission failure in band.
            let mut effects = vec![Effect::Relay {
                job: job.clone(),
                events: vec![ConsumerEvent::Error {
                    code: TerminalReason::QueueFull.as_str().to_string(),
                    message: format!("the job could not be requeued: {err:?}"),
                    retryable: true,
                    through_seq: Some(record.seq.acked_through()),
                }],
            }];
            effects.extend(self.finish_job(job, TerminalReason::QueueFull, now_ms));
            return effects;
        }
        vec![Effect::Log {
            level: LogLevel::Info,
            message: format!("{job} requeued for attempt {}", record.attempt + 1),
        }]
    }

    fn drop_lease(&mut self, job: &JobId, worker: &WorkerId) {
        if let Some(lease) = self.leases.remove(job) {
            self.lease_timer.remove(&(lease.deadline_ms, job.clone()));
        }
        if let Some(w) = self.workers.get_mut(worker) {
            w.in_flight.remove(job);
        }
    }

    /// Move a job to a terminal phase, free its resources, and report it.
    fn finish_job(
        &mut self,
        job: &JobId,
        reason: TerminalReason,
        now_ms: Timestamp,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();
        if let Some(record) = self.jobs.get_mut(job) {
            if record.phase.is_terminal() {
                return effects;
            }
            let applied = record.terminate(reason, now_ms);
            if let Some(dispatch) = record.timings.dispatch_ms {
                self.metrics
                    .observe(names::JOB_DURATION, now_ms.saturating_sub(dispatch));
            }
            self.metrics
                .incr_label(names::JOBS_TERMINAL, "reason", applied.as_str(), 1);
            // Prompts are released the moment a job is terminal (§9).
            let reference = record.payload_ref.take();
            if let Some(id) = &reference {
                self.payloads.release(id);
            }
            effects.push(Effect::ReleasePayload {
                job: job.clone(),
                reference,
            });
            if let Some(count) = self.tenant_active.get_mut(&record.tenant) {
                *count = count.saturating_sub(1);
            }
            // The idempotency entry outlives the job so a late retry replays the
            // finished stream instead of re-running it (§5.2). The one exception is the
            // NEX-6.3 consumer-gone orphan: `on_detach` calls `forget_job` right after
            // this returns, which deletes the record, this timer entry and the key
            // together, because nobody is left who could replay the answer.
            self.retention_timer.insert((
                now_ms.saturating_add(self.limits().job_retention_ms),
                job.clone(),
            ));
            effects.push(Effect::Terminal {
                job: job.clone(),
                reason: applied,
                retryable: applied.retryable(),
            });
        }
        // Any lease still pointing at the finished job must go, whoever held it.
        if let Some(lease) = self.leases.remove(job) {
            self.lease_timer.remove(&(lease.deadline_ms, job.clone()));
            if let Some(w) = self.workers.get_mut(&lease.worker) {
                w.in_flight.remove(job);
            }
        }
        self.refresh_gauges();
        effects
    }

    // ----------------------------------------------------------------------- tick

    fn on_tick(&mut self, now_ms: Timestamp) -> Vec<Effect> {
        if self.closed {
            return Vec::new();
        }
        let mut effects = Vec::new();

        // 1. Orphan-grace expiry: the socket-close decisions deferred by
        //    `resume_grace_ms` land here (module rule 3).
        let due_orphans: Vec<WorkerId> = self
            .orphan_timer
            .iter()
            .take_while(|(deadline, _)| *deadline <= now_ms)
            .map(|(_, worker)| worker.clone())
            .collect();
        for worker in due_orphans {
            self.orphan_timer.retain(|(_, w)| w != &worker);
            effects.extend(self.expire_worker_leases(&worker, now_ms));
            self.workers.remove(&worker);
        }

        // 2. Lease deadlines for sockets that are still up but stopped renewing
        //    (§5.1: unhealthy-but-leased keeps waiting until the lease expires).
        let due_leases: Vec<JobId> = self
            .lease_timer
            .iter()
            .take_while(|(deadline, _)| *deadline <= now_ms)
            .map(|(_, job)| job.clone())
            .collect();
        for job in due_leases {
            let Some(lease) = self.leases.get(&job).cloned() else {
                continue;
            };
            if lease.deadline_ms > now_ms {
                continue; // renewed between collection and application
            }
            effects.extend(self.resolve_lost_job(&job, &lease.worker, now_ms));
        }

        // 3. Jobs that waited too long for a worker.
        let stale_queue = self.queue.purge_expired(now_ms);
        let pending_too: Vec<JobId> = self
            .jobs
            .iter()
            .filter(|(_, r)| r.phase == JobPhase::Pending && r.dispatch_deadline_ms <= now_ms)
            .map(|(id, _)| id.clone())
            .collect();
        let mut waiting = stale_queue;
        for job in pending_too {
            if !waiting.contains(&job) {
                waiting.push(job);
            }
        }
        for job in waiting {
            let (streaming, phase) = match self.jobs.get(&job) {
                Some(record) => (record.streaming, record.phase),
                None => continue,
            };
            let reason = if phase == JobPhase::Pending {
                TerminalReason::NoReadyWorker
            } else {
                TerminalReason::QueueTimeout
            };
            self.metrics
                .incr_label(names::LEASE_EXPIRIES, "kind", "no_dispatch", 1);
            // Two different contracts, decided by whether the job has already
            // been answered:
            //   * `Pending`  → nothing was written, so `503 + Retry-After` is
            //     still legal (§7.2, acceptance test 6);
            //   * any other phase → the actor already emitted `Accepted`, which
            //     is what opens the consumer's SSE body. The status is spent, so
            //     the failure must go out in band (§7.2) — a `Reject` at this
            //     point lands on a gate that is already closed and the consumer's
            //     stream would hang instead of ending.
            if phase == JobPhase::Pending {
                effects.push(Effect::Reject {
                    // `Effect::Reject.job` is optional by design (a rejection can
                    // precede job creation); this one names the job it refused.
                    job: Some(job.clone()),
                    status: 503,
                    code: reason.as_str().to_string(),
                    message: "no capable worker became ready in time".to_string(),
                    retry_after_ms: Some(2_000),
                });
            } else {
                let through = self.jobs.get(&job).map_or(0, |r| r.seq.acked_through());
                effects.push(Effect::Relay {
                    job: job.clone(),
                    events: vec![ConsumerEvent::Error {
                        code: reason.as_str().to_string(),
                        message: if streaming {
                            "no capable worker claimed this job in time; the response is complete as far as it goes".to_string()
                        } else {
                            "no capable worker claimed this job in time; retry it".to_string()
                        },
                        // §5.3: only a job with no bytes on the wire is retryable.
                        retryable: reason.retryable(),
                        through_seq: Some(through),
                    }],
                });
            }
            effects.extend(self.finish_job(&job, reason, now_ms));
        }

        // 4. Hard per-job deadlines.
        let overdue: Vec<JobId> = self
            .leases
            .values()
            .filter(|lease| {
                self.jobs
                    .get(&lease.job)
                    .is_some_and(|r| r.deadline_ms <= now_ms)
            })
            .map(|lease| lease.job.clone())
            .collect();
        for job in overdue {
            let worker = self.leases.get(&job).map(|l| l.worker.clone());
            if let Some(worker) = worker {
                effects.push(Effect::SendWorker {
                    worker,
                    frame: Box::new(HubFrame::Cancel {
                        job: job.clone(),
                        reason: CancelReason::Deadline,
                    }),
                });
            }
            let through = self.jobs.get(&job).map(|r| r.seq.acked_through());
            let mut events = self.pending_batch(&job);
            events.push(ConsumerEvent::Error {
                code: TerminalReason::DeadlineExceeded.as_str().to_string(),
                message: "the job exceeded its hard deadline".to_string(),
                retryable: false,
                through_seq: through,
            });
            effects.push(Effect::Relay {
                job: job.clone(),
                events,
            });
            effects.extend(self.finish_job(&job, TerminalReason::DeadlineExceeded, now_ms));
        }

        // 5. Missed heartbeats: stop feeding the worker, keep its leases (§5.1).
        let heartbeat_timeout = self.cfg.heartbeat_ms_for(&self.pool).saturating_mul(3);
        let degraded: Vec<WorkerId> = self
            .workers
            .iter()
            .filter(|(_, w)| {
                w.link == LinkState::Live
                    && w.engine == EngineState::Ready
                    && now_ms.saturating_sub(w.last_seen_ms) >= heartbeat_timeout
            })
            .map(|(id, _)| id.clone())
            .collect();
        for worker in degraded {
            if let Some(w) = self.workers.get_mut(&worker) {
                w.engine = EngineState::Degraded;
            }
            effects.push(Effect::Log {
                level: LogLevel::Warn,
                message: format!(
                    "worker {worker} missed {heartbeat_timeout} ms of heartbeats; pausing dispatch"
                ),
            });
        }

        // 6. Flush windows that aged out with no new frame arriving: §7.2's "flush
        //    every ~50–100 ms" is a timer, not a frame-driven heuristic, or small
        //    deltas would wait for the next token forever.
        let limits = self.limits().clone();
        let window_due: Vec<JobId> = self
            .jobs
            .iter()
            .filter(|(_, r)| {
                r.flush.pending_frames > 0
                    && !r.consumers.is_empty()
                    && r.flush.oldest_ms.is_some_and(|first| {
                        now_ms.saturating_sub(first) >= limits.flush_interval_ms
                    })
            })
            .map(|(id, _)| id.clone())
            .collect();
        for job in window_due {
            let events = self.pending_batch(&job);
            if !events.is_empty() {
                effects.push(Effect::Relay { job, events });
            }
        }

        // 7. Keepalives for jobs whose SSE body is already open.
        let ping_due: Vec<JobId> = self
            .jobs
            .iter()
            .filter(|(_, r)| {
                r.streaming
                    && !r.consumers.is_empty()
                    && matches!(r.phase, JobPhase::Pending | JobPhase::Queued)
                    && r.last_ping_ms
                        .is_none_or(|last| now_ms.saturating_sub(last) >= KEEPALIVE_INTERVAL_MS)
            })
            .map(|(id, _)| id.clone())
            .collect();
        for job in ping_due {
            if let Some(r) = self.jobs.get_mut(&job) {
                r.last_ping_ms = Some(now_ms);
            }
            effects.push(Effect::Keepalive { job });
        }

        // 8. TTL sweeps. Prompts go first: they are the sensitive bytes.
        for id in self.payloads.purge_expired(now_ms) {
            effects.push(Effect::ReleasePayload {
                job: JobId::new_unchecked(id.as_ref()),
                reference: Some(id),
            });
        }
        for (artifact, path) in self.artifacts.purge_expired(now_ms) {
            effects.push(Effect::DeleteArtifact { artifact, path });
        }
        let expired_keys: Vec<String> = self
            .idempotency_timer
            .iter()
            .take_while(|(deadline, _)| *deadline <= now_ms)
            .map(|(_, key)| key.clone())
            .collect();
        for key in expired_keys {
            self.forget_idempotency(&key);
        }

        // 9. NEX-6.6's cap on a detached task with no subscriber (§9.1 item 3: "cap its
        //    lifetime while it has no subscriber"). A detached submission is the one
        //    permitted exception to "no consumer, no task", and until now it was the only
        //    state in the actor with **no** upper bound while it stayed unclaimed: the
        //    queue/claim deadlines answer the consumer that is not there, and
        //    `job_retention_ms` only starts at a terminal state that may never be
        //    reached. Delete the record and its idempotency entry, releasing the tenant
        //    slot and the queued prompt with it.
        //
        //    **Only while unclaimed** (`Pending`/`Queued`), and that restriction is the
        //    deliberate half: once a worker holds the lease, GPU work has been committed,
        //    and killing a job that is 3 minutes into a render because a poller hung up
        //    would throw away the expensive part to save the cheap part. An attached
        //    worker is bounded by `job_hard_cap_ms` (step 4) and its terminal record by
        //    `job_retention_ms` (step 10), so the state is still bounded — just by the
        //    clock that matches what is actually being spent.
        //
        //    `finish_job` is *not* called: nobody is attached, so there is no stream to
        //    end in band and no `Accepted`-opened body to answer; the record simply stops
        //    existing, and the `JobForgotten` effect tells the embedder to drop its
        //    routing entry.
        let detached_orphan_ttl = self.limits().detached_orphan_ttl_ms;
        let expired_detached: Vec<JobId> = self
            .jobs
            .iter()
            .filter(|(_, r)| {
                !r.cancel_on_consumer_gone
                    && r.consumers.is_empty()
                    && matches!(r.phase, JobPhase::Pending | JobPhase::Queued)
                    && r.subscriberless_since
                        .is_some_and(|since| now_ms.saturating_sub(since) >= detached_orphan_ttl)
            })
            .map(|(id, _)| id.clone())
            .collect();
        for job in expired_detached {
            // The tenant slot, the queued prompt and the idempotency key are all released
            // by `forget_job`, which releases the slot exactly for a record that was still
            // non-terminal (`finish_job` has already released a terminal one).
            //
            // No new metric here on purpose: every existing counter means something else
            // (`jobs_rejected` is pre-acceptance, `jobs_terminal` is a state transition,
            // `lease_expiries` involves a lease), and this deletion is none of those. It is
            // observable as `Effect::JobForgotten`, which the server logs, plus the drop in
            // `nexus_queue_depth` that `refresh_gauges` reports.
            effects.extend(self.forget_job(&job));
        }

        // 10. Terminal-job retention sweep, which bounds the jobs map (and with it
        //     the replay rings) on a long-lived hub. `forget_job` is what actually drops
        //     the record, so this sweep and the NEX-6.3 orphan path cannot drift apart on
        //     what "deleted" means: queue entry, payload, lease, idempotency key and the
        //     `JobForgotten` effect are the same list in both.
        let expired_jobs: Vec<JobId> = self
            .retention_timer
            .iter()
            .take_while(|(deadline, _)| *deadline <= now_ms)
            .map(|(_, job)| job.clone())
            .collect();
        for job in expired_jobs {
            effects.extend(self.forget_job(&job));
        }

        self.refresh_gauges();
        effects
    }

    fn on_shutdown(&mut self, now_ms: Timestamp) -> Vec<Effect> {
        self.closed = true;
        let mut effects = Vec::new();
        for (job, lease) in self.leases.iter() {
            effects.push(Effect::SendWorker {
                worker: lease.worker.clone(),
                frame: Box::new(HubFrame::Cancel {
                    job: job.clone(),
                    reason: CancelReason::HubShutdown,
                }),
            });
        }
        for worker in self.workers.keys() {
            effects.push(Effect::SendWorker {
                worker: worker.clone(),
                frame: Box::new(HubFrame::Bye {
                    reason: ByeReason::Shutdown,
                }),
            });
        }
        let mut draining = self.queue.drain();
        draining.extend(self.leases.keys().cloned());
        for job in draining {
            // §16.18: an in-flight consumer must be told, in band, that the hub
            // is going away. `Effect::Terminal` is bookkeeping for the embedder;
            // the SSE pumps end on a terminal *event*, so without this relay a
            // consumer of a shutting-down hub hangs until the socket dies.
            let (open, through) = match self.jobs.get(&job) {
                Some(record) => (!record.phase.is_terminal(), record.seq.acked_through()),
                None => (false, 0),
            };
            if open {
                effects.push(Effect::Relay {
                    job: job.clone(),
                    events: vec![ConsumerEvent::Error {
                        code: TerminalReason::HubShutdown.as_str().to_string(),
                        message: "the hub is shutting down; retry the request".to_string(),
                        retryable: true,
                        through_seq: Some(through),
                    }],
                });
            }
            effects.extend(self.finish_job(&job, TerminalReason::HubShutdown, now_ms));
        }
        effects
    }

    // --------------------------------------------------------------------- shared

    /// Recompute the gauges `/metrics` and `/healthz` read.
    fn refresh_gauges(&mut self) {
        self.metrics
            .set(names::QUEUE_DEPTH, self.queue.depth() as u64);
        self.metrics
            .set(names::READY_WORKERS, self.ready_workers() as u64);
        self.metrics
            .set(names::LOADING_WORKERS, self.loading_workers() as u64);
        self.metrics.set(
            names::CONNECTED_WORKERS,
            self.workers
                .values()
                .filter(|w| w.link == LinkState::Live)
                .count() as u64,
        );
        self.metrics.set(names::LEASES, self.leases.len() as u64);
        self.metrics
            .set("nexus_payload_bytes", self.payloads.bytes());
        self.metrics
            .set("nexus_artifact_bytes_on_disk", self.artifacts.bytes());
    }
}

// ------------------------------------------------------------------ free helpers

/// One accepted worker frame, exactly as [`relay_into`] needs it.
///
/// A struct rather than seven positional arguments: these fields are the decoded frame,
/// they are read-only, and they always travel together. Grouping them leaves the relay
/// helper's signature describing the two things that genuinely vary per call — the job's
/// state, and the frame being appended to it.
struct FrameInput<'a> {
    /// Worker-assigned sequence number for the job.
    seq: u64,
    /// Frame class.
    kind: FrameKind,
    /// Verbatim text, copied into the ring untouched (NEX-5.2).
    text: &'a str,
    /// Manifest carried by an `artifact_chunk` frame.
    artifact: Option<&'a crate::protocol::ArtifactManifest>,
    /// Declared content type, recorded but never interpreted (only the adapters know
    /// what it means).
    content_type: Option<&'a str>,
    /// Hub-side arrival time.
    now_ms: Timestamp,
}

/// Append a frame to the job's replay ring, accumulate continuation text, and
/// decide whether the flush gate is open (§7.2). Returns the events to relay
/// **now**: empty when the batch is still accumulating.
///
/// A free function, not a method, deliberately: it must not reach through
/// `&mut self`, which would let per-job state and actor state borrow each other.
fn relay_into(
    record: &mut JobRecord,
    limits: &Limits,
    frame: FrameInput<'_>,
) -> Vec<ConsumerEvent> {
    // Destructured once so the body below reads as it always did: every name here is a
    // field of the frame the caller just handed over.
    let FrameInput {
        seq,
        kind,
        text,
        artifact,
        content_type,
        now_ms,
    } = frame;

    if record.phase == JobPhase::Assigned {
        record.phase = JobPhase::Streaming;
        if record.timings.first_frame_ms.is_none() {
            record.timings.first_frame_ms = Some(now_ms);
        }
    }

    // Continuation needs the partial assistant text, capped and memory-only.
    if kind == FrameKind::Delta && record.partial_text.len() < limits.partial_text_cap_bytes {
        record.partial_text.push_str(text);
        record.partial_text.truncate(limits.partial_text_cap_bytes);
    }
    // Continuation is only honest when what was relayed can be re-played to the
    // next worker as assistant text. Two things make the accumulated prefix
    // unreconstructible, and both are recorded here, at the one place that sees
    // every frame:
    //   * a non-`delta` kind (an `event`/`log`/`artifact_chunk` interleaved with
    //     the deltas is not part of the assistant message), and
    //   * an OpenAI chunk carrying tool-call deltas: those arrive as `delta` JSON
    //     fragments (`{"function":{"arguments":"{\"ci"}`) that concatenate into a
    //     half-written tool argument, never into prose, so re-running on
    //     `prompt + partial text` would duplicate or corrupt the call (task §7.4).
    // `content_type` is deliberately **not** consulted: the reference worker labels
    // every chunk `application/json` (`acceptance.rs:117` `delta_msg`), so the
    // label carries no information about reconstructibility.
    if kind != FrameKind::Delta
        || text.contains("\"tool_calls\"")
        || text.contains("\"function_call\"")
    {
        record.output_opaque = true;
    }
    // A content type on a delta frame is recorded but never interpreted: only
    // the adapters know what it means.
    if let Some(ct) = content_type {
        record.content_type = ct.to_string();
    }

    record.replay.push(BufferedFrame {
        seq,
        kind,
        content: text.to_string(),
        artifact: artifact.cloned(),
        received_ms: now_ms,
    });

    // Account the frame in the gate first, then ask whether the window is full:
    // a frame that crosses the byte threshold must flush immediately, including
    // the frames that were waiting.
    record.flush.note(now_ms, text.len());
    let batch: Vec<ConsumerEvent> =
        if record
            .flush
            .should_flush(now_ms, limits.flush_interval_ms, limits.flush_bytes)
        {
            let events = record
                .replay
                .since(record.flush.last_flushed_seq)
                .into_iter()
                .map(frame_event)
                .collect();
            record.flush.commit_flush();
            record.flush.seal_through(seq);
            events
        } else {
            Vec::new()
        };

    // `bytes_delivered` is the "HTTP status is spent" flag (§7.2).
    record.bytes_delivered = true;
    record.delivered_bytes = record
        .delivered_bytes
        .saturating_add(batch.iter().map(ConsumerEvent::size_bytes).sum::<usize>() as u64);
    batch
}

/// Convert one buffered frame into its consumer event.
///
/// `#[allow(clippy::expect_used)]` for the `ArtifactChunk` arm only, and it is
/// infallible by invariant rather than by hope: `validate_worker_frame` rejects an
/// `artifact_chunk` frame whose manifest is missing (`protocol.rs`, "artifact_chunk
/// requires an artifact manifest"), so every frame the actor buffered under that kind
/// carries one. Falling back to a synthetic event instead would invent consumer-visible
/// content for a case the wire format cannot produce.
#[allow(clippy::expect_used)]
fn frame_event(frame: &BufferedFrame) -> ConsumerEvent {
    match frame.kind {
        FrameKind::Delta => ConsumerEvent::Delta {
            seq: frame.seq,
            text: frame.content.clone(),
        },
        FrameKind::Log => ConsumerEvent::Log {
            seq: frame.seq,
            text: frame.content.clone(),
        },
        FrameKind::Event => ConsumerEvent::Event {
            seq: frame.seq,
            payload: serde_json::from_str(&frame.content)
                .unwrap_or_else(|_| Value::String(frame.content.clone())),
        },
        FrameKind::ArtifactChunk => ConsumerEvent::Artifact {
            seq: frame.seq,
            manifest: frame
                .artifact
                .clone()
                .expect("the protocol layer requires a manifest"),
        },
    }
}

/// Build a `Reject` effect.
fn reject(
    job: Option<JobId>,
    status: u16,
    code: &str,
    message: &str,
    retry_after_ms: Option<u64>,
) -> Effect {
    Effect::Reject {
        job,
        status,
        code: code.to_string(),
        message: message.to_string(),
        retry_after_ms,
    }
}

/// Wire name of a frame kind, for metric labels.
fn frame_kind_name(kind: FrameKind) -> &'static str {
    match kind {
        FrameKind::Delta => "delta",
        FrameKind::Event => "event",
        FrameKind::Log => "log",
        FrameKind::ArtifactChunk => "artifact_chunk",
    }
}

/// Explain an unsatisfiable capability request precisely enough that a client can
/// fix it (the payload of §8's immediate 400).
fn unsatisfiable_reason(requires: &Requires) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(model) = &requires.model {
        parts.push(format!("model {model:?}"));
    }
    if let Some(min) = requires.min_context_tokens {
        parts.push(format!("{min} tokens of context"));
    }
    if !requires.capabilities.is_empty() {
        parts.push(format!(
            "capabilities {:?}",
            requires.capabilities.iter().collect::<Vec<_>>()
        ));
    }
    if parts.is_empty() {
        "no worker is connected to this pool".to_string()
    } else {
        format!("no connected worker satisfies {}", parts.join(", "))
    }
}

/// Read `usage.completion_tokens` for the tokens/s metric only. Read-only: the
/// usage object itself is relayed untouched (§7.2).
fn usage_tokens(usage: Option<&Value>) -> Option<u64> {
    usage
        .and_then(|v| v.get("completion_tokens"))
        .and_then(Value::as_u64)
        .or_else(|| {
            usage
                .and_then(|v| v.get("usage"))
                .and_then(|u| u.get("completion_tokens"))
                .and_then(Value::as_u64)
        })
}

/// Per-kind claim deadline, used when requeueing.
fn kind_claim_deadline(cfg: &BrokerConfig, kind: &str) -> u64 {
    cfg.kind(kind)
        .map(|k: &KindConfig| k.claim_deadline_ms.unwrap_or(cfg.limits.claim_deadline_ms))
        .unwrap_or(cfg.limits.claim_deadline_ms)
}
