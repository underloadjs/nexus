//! Job/worker/lease state. These are *data* types: nothing here decides
//! anything. All transitions are performed by [`crate::actor::PoolActor`], the
//! single mutator required by task §5.
//!
//! `BTreeMap`/`BTreeSet` are used wherever iteration order is observable, so a
//! dispatch decision between two equally-scored workers is reproducible.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

// The id newtypes are re-exported so callers can import the whole vocabulary
// from one place (`crate::model::*`); protocol.rs and adapters.rs depend on it.
pub use crate::ids::{ArtifactId, ConsumerId, JobId, PayloadRefId, PoolId, TenantId, WorkerId};

use crate::protocol::ArtifactManifest;
use crate::protocol::Attestation;
use crate::protocol::FinishKind;
use crate::protocol::FrameKind;
use crate::protocol::Requires;
use crate::protocol::WorkerError;
use crate::relay::FlushGate;
use crate::seq::ReplayBuffer;
use crate::seq::SeqTracker;
use crate::time::Timestamp;

/// Fairness class inside a pool queue. Lower numeric value = served first.
/// Realtime exists so a latency-sensitive consumer is not stuck behind bulk
/// renders (task §8 "a priority class").
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    /// Latency-sensitive interactive traffic.
    Realtime = 0,
    /// Default class for ordinary submissions.
    Default = 1,
    /// Bulk/offline work that may wait.
    Bulk = 2,
}

impl Priority {
    /// Parse a caller-supplied priority name; anything unknown becomes
    /// [`Priority::Default`] so a typo cannot starve a tenant.
    #[must_use]
    pub fn from_name(name: &str) -> Self {
        match name {
            "realtime" | "high" => Self::Realtime,
            "bulk" | "low" => Self::Bulk,
            _ => Self::Default,
        }
    }

    /// Wire name, used by the generic job API and the metrics text format.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Realtime => "realtime",
            Self::Default => "default",
            Self::Bulk => "bulk",
        }
    }
}

/// Reliability tier of the host a worker runs on (task §11). Not trust: a
/// dedicated box is still an unvalidated peer (§9), only *more available*.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum WorkerTier {
    /// Always-on hardware the operator controls; carries the floor.
    Dedicated,
    /// molab/Kaggle-class host with session and quota limits; burst only.
    #[default]
    Ephemeral,
}

/// Liveness of a worker's *engine*, orthogonal to whether its socket is up.
///
/// `Loading` workers are counted as advertised capacity (so `/v1/models` and the
/// degraded-capacity signal are honest) but are never dispatched to — task §8
/// "never dispatch to a non-ready worker", acceptance test 11.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EngineState {
    /// Connected and handshaked, engine still loading or warming up.
    Loading,
    /// Engine reported loaded + slot count + passed warmup/admission probes.
    Ready,
    /// Engine reported not-ready after having been ready (probe regression).
    Degraded,
}

/// Connection lifecycle of the worker's outbound socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LinkState {
    /// Socket live and handshaked.
    Live,
    /// Socket closed; leases are orphaned but resumable until `resume_deadline`.
    Orphaned,
}

/// Everything the hub knows about one connected worker. Owned exclusively by the
/// actor of each pool the worker joined.
#[derive(Debug, Clone)]
pub struct WorkerRecord {
    /// Stable worker identity (dedupe key).
    pub id: WorkerId,
    /// Connection generation. Bumped on every accepted `Hello` for this id, so
    /// frames from an evicted socket are recognisably stale (task §11).
    pub epoch: u64,
    /// Current link state.
    pub link: LinkState,
    /// Engine liveness.
    pub engine: EngineState,
    /// Declared concurrent slot count; dispatch is capped at this (test 12).
    pub capacity: u32,
    /// Slots the worker last reported free. Reconciled against `in_flight`.
    pub free_slots: u32,
    /// KV/context budget the worker reports free, in tokens (task §8).
    pub free_kv_tokens: Option<u64>,
    /// Context window this worker can serve.
    pub context_tokens: u64,
    /// Capability set announced at `Hello`, plus the model names.
    pub requires: Requires,
    /// Model digests attested at `Hello` (task §9).
    pub attestation: Attestation,
    /// Hosts the worker joined.
    pub pools: BTreeSet<PoolId>,
    /// Availability tier.
    pub tier: WorkerTier,
    /// Jobs currently leased to this worker.
    pub in_flight: BTreeSet<JobId>,
    /// Jobs this worker has an unfulfilled `Claim` for. Dispatch is *pull*
    /// driven, so a claim is authority to receive up to N assignments.
    pub outstanding_claims: u32,
    /// Last inbound frame time; a missed heartbeat is visible here.
    pub last_seen_ms: Timestamp,
    /// When the socket died and orphaned leases stop being resumable. `Some`
    /// only while [`LinkState::Orphaned`].
    pub resume_deadline_ms: Option<Timestamp>,
    /// Engine telemetry from the last heartbeat (relay of the engine's own
    /// status endpoint, e.g. Strata `/v1/status`).
    pub telemetry: Option<Value>,
    /// Count of frames dropped because their epoch was stale.
    pub stale_frames: u64,
}

impl WorkerRecord {
    /// Fresh record for a worker that has just passed handshake; starts
    /// `Loading` because readiness must be *reported*, never assumed.
    #[must_use]
    pub fn new(
        id: WorkerId,
        epoch: u64,
        capacity: u32,
        requires: Requires,
        attestation: Attestation,
        pools: BTreeSet<PoolId>,
        tier: WorkerTier,
        now_ms: Timestamp,
    ) -> Self {
        Self {
            free_slots: capacity,
            context_tokens: requires.min_context_tokens.unwrap_or(0),
            free_kv_tokens: None,
            engine: EngineState::Loading,
            link: LinkState::Live,
            last_seen_ms: now_ms,
            telemetry: None,
            stale_frames: 0,
            in_flight: BTreeSet::new(),
            outstanding_claims: 0,
            resume_deadline_ms: None,
            id,
            epoch,
            capacity,
            requires,
            attestation,
            pools,
            tier,
        }
    }

    /// Slots this worker may still accept right now: declared capacity minus
    /// what is already leased, clamped by what it reports free.
    ///
    /// Reconciling both is deliberate. A buggy or stale heartbeat that reports
    /// `free_slots == capacity` while jobs are in flight must not cause
    /// over-dispatch, because over-dispatch "only queues inside the engine and
    /// degrades everyone's TTFT" (task §4).
    #[must_use]
    pub fn available_slots(&self) -> u32 {
        let own = self.capacity.saturating_sub(self.in_flight.len() as u32);
        own.min(self.free_slots)
    }

    /// True when the actor may hand this worker a new job.
    #[must_use]
    pub fn dispatchable(&self) -> bool {
        self.link == LinkState::Live
            && self.engine == EngineState::Ready
            && self.available_slots() > 0
    }

    /// True when the worker is counted in advertised capacity (`/v1/models`,
    /// degraded-capacity metrics) even if it cannot take work this instant.
    #[must_use]
    pub fn advertised(&self) -> bool {
        self.link == LinkState::Live
    }
}

/// Why a job left the running set. Distinguishing these matters because the
/// requeue policy differs per task §5.1/§5.3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalReason {
    /// Worker reported `Done{finish}` and the consumer saw a finish_reason.
    Completed,
    /// Consumer cancelled / disconnected; worker received `Cancel`.
    Cancelled,
    /// Worker died, zero output had reached the consumer, idempotent kind →
    /// requeued, so this reason only appears when requeue was impossible.
    WorkerGoneNoOutput,
    /// Worker died after bytes were already on the wire → in-band terminal.
    WorkerGonePartial,
    /// Non-idempotent kind hit an ambiguous ACK or expired lease: fail loudly,
    /// never re-run (task §5.3).
    NonIdempotentAmbiguous,
    /// Lease deadline passed with no heartbeat renewal, zero output → requeued.
    LeaseExpiredNoOutput,
    /// Lease deadline passed after output started → in-band terminal.
    LeaseExpiredPartial,
    /// Per-job hard cap elapsed.
    DeadlineExceeded,
    /// Waited for a ready worker past `no_worker_grace_ms` → 503.
    NoReadyWorker,
    /// Waited in queue past `queue_timeout_ms` → 503.
    QueueTimeout,
    /// Capability pre-check failed at submit time → 400.
    CapabilityUnsatisfied,
    /// Queue depth bound hit at submit time → 429.
    QueueFull,
    /// Per-tenant concurrency cap hit at submit time → 429.
    TenantLimit,
    /// Hub is shutting down; in-flight clients get a clean retryable failure
    /// (acceptance test 18).
    HubShutdown,
    /// A `Done` arrived for a job that had already terminated (duplicate ACK).
    DuplicateDone,
}

impl TerminalReason {
    /// Whether the consumer's HTTP/SSE response may be retried safely. Only
    /// terminal states with **zero bytes delivered** qualify: once bytes are on
    /// the wire the status is spent (task §7.2).
    #[must_use]
    pub fn retryable(&self) -> bool {
        matches!(
            self,
            Self::NoReadyWorker
                | Self::QueueTimeout
                | Self::QueueFull
                | Self::TenantLimit
                | Self::HubShutdown
                | Self::WorkerGoneNoOutput
                | Self::LeaseExpiredNoOutput
        )
    }

    /// Wire name for error bodies and metrics labels.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::WorkerGoneNoOutput => "worker_gone_no_output",
            Self::WorkerGonePartial => "worker_gone_partial",
            Self::NonIdempotentAmbiguous => "non_idempotent_ambiguous",
            Self::LeaseExpiredNoOutput => "lease_expired_no_output",
            Self::LeaseExpiredPartial => "lease_expired_partial",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::NoReadyWorker => "no_ready_worker",
            Self::QueueTimeout => "queue_timeout",
            Self::CapabilityUnsatisfied => "capability_unsatisfied",
            Self::QueueFull => "queue_full",
            Self::TenantLimit => "tenant_limit",
            Self::HubShutdown => "hub_shutdown",
            Self::DuplicateDone => "duplicate_done",
        }
    }
}

/// The lease held by one attempt of one job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Lease {
    /// Job being leased.
    pub job: JobId,
    /// Current owner.
    pub worker: WorkerId,
    /// Connection generation of the owner socket.
    pub epoch: u64,
    /// Attempt number, 1-based. Incremented on every requeue.
    pub attempt: u32,
    /// Absolute deadline; heartbeats renew it, expiry triggers the §5.1 rules.
    pub deadline_ms: Timestamp,
    /// Bytes streamed to consumers under this lease — the zero-vs-partial
    /// discriminator for expiry handling.
    pub streamed_bytes: u64,
}

impl Lease {
    /// True when `now_ms` is past the deadline.
    #[must_use]
    pub fn is_expired(&self, now_ms: Timestamp) -> bool {
        crate::time::expired(now_ms, self.deadline_ms)
    }
}

/// One relayed frame, kept for replay (task §7.5).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BufferedFrame {
    /// Worker-assigned sequence number for the job.
    pub seq: u64,
    /// Frame class.
    pub kind: FrameKind,
    /// Verbatim bytes for delta/log frames; JSON-encoded payload for events.
    pub content: String,
    /// Artifact descriptor for `artifact_chunk` frames.
    #[serde(default)]
    pub artifact: Option<ArtifactManifest>,
    /// Hub-side arrival time, used for replay TTL decisions.
    pub received_ms: Timestamp,
}

impl BufferedFrame {
    /// Approximate bytes retained in RSS for this entry (ring buffer accounting).
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        self.content.len() + 32
    }
}

/// Complete hub-side view of one job. The only authority for this job's state.
#[derive(Debug, Clone)]
pub struct JobRecord {
    /// Identity.
    pub id: JobId,
    /// Pool the job is routed to.
    pub pool: PoolId,
    /// Registry kind, carrying idempotency/streaming/timeout semantics.
    pub kind: String,
    /// Whether this kind may be auto-requeued (task §5.3).
    pub idempotent: bool,
    /// Streaming kind (SSE contract) or artifact kind (job API contract).
    pub streaming: bool,
    /// Copy of [`Submission::cancel_on_consumer_gone`]: whether this job dies with
    /// its last consumer. `on_detach` asks **this**, not `streaming`, so an async
    /// (poll-later) submission survives a `GET /v1/jobs/{id}/events` watcher that
    /// connects and disconnects, even when its kind happens to be a streaming one
    /// (task §6).
    pub cancel_on_consumer_gone: bool,
    /// Owner account.
    pub tenant: TenantId,
    /// Session hint used for affinity (task §8, §17.4 resolution).
    pub session_hint: Option<String>,
    /// Fairness class.
    pub priority: Priority,
    /// Capability requirements resolved at submit time.
    pub requires: Requires,
    /// Inline payload bytes, or `None` once spilled to a payload ref.
    pub payload_inline: Option<Value>,
    /// Set when the payload travels by reference (task §10, test 16).
    pub payload_ref: Option<PayloadRefId>,
    /// Content type of the payload.
    pub content_type: String,
    /// Payload size in bytes, measured before any spill.
    pub payload_bytes: u64,
    /// sha256 of the payload as received, hex.
    pub payload_sha256: String,
    /// Idempotency key (explicit or content hash) for dedupe (task §5.2).
    pub idempotency_key: String,
    /// Number of dispatch attempts so far (1 after the first `Assigned`).
    /// The lease itself lives in [`crate::actor::PoolActor::leases`] — §5's
    /// `leased` map — because holding a copy here would put one fact in two
    /// places and re-create the "second authority" the task forbids.
    pub attempt: u32,
    /// Worker we must avoid on the next requeue ("prefer a different worker").
    pub last_worker: Option<WorkerId>,
    /// Where the job is in its lifecycle.
    pub phase: JobPhase,
    /// Why it terminated, if it did.
    pub terminal: Option<TerminalReason>,
    /// Per-job seq accounting: dedupe + gap detection.
    pub seq: SeqTracker,
    /// Replay ring for consumer re-attach.
    pub replay: ReplayBuffer,
    /// Partial assistant text accumulated for resume-by-continuation.
    pub partial_text: String,
    /// True once the relayed output stops being reconstructible assistant text:
    /// a non-`delta` frame kind, or a chunk carrying OpenAI `tool_calls` /
    /// `function_call` deltas. Re-running such an attempt on `prompt + partial
    /// text` would hand the next worker a half-written tool argument, so
    /// resume-by-continuation refuses it unless the operator opts in (§7.4, and
    /// the reason continuation is off by default in the first place).
    pub output_opaque: bool,
    /// Outbound SSE batching state for this job (task §7.2). Part of the job's
    /// state, so it lives with the job and only the actor mutates it.
    pub flush: FlushGate,
    /// Last keepalive emitted while queued; bounds the `: ping` rate.
    pub last_ping_ms: Option<Timestamp>,
    /// Consumers currently attached (drives cancel-on-gone).
    pub consumers: BTreeSet<ConsumerId>,
    /// True once at least one delta/event frame has been relayed to a consumer.
    /// This is the "HTTP status is spent" flag.
    pub bytes_delivered: bool,
    /// Cancel has been sent to the owner and not yet acked.
    pub cancel_sent_ms: Option<Timestamp>,
    /// Artifact produced by this job, if any.
    pub artifact: Option<ArtifactRecord>,
    /// Timing marks for the mandatory metrics (task §13).
    pub timings: JobTimings,
    /// Absolute per-job deadline (hard cap from the kind config).
    pub deadline_ms: Timestamp,
    /// Deadline for "must be dispatched by" — drives 503 on queue/no-worker.
    pub dispatch_deadline_ms: Timestamp,
    /// Queue sequence assigned when enqueued (FIFO tie-break).
    pub queue_seq: Option<u64>,
    /// Number of continuation re-dispatches already performed (task §7.4).
    pub continuations: u32,
    /// Frames dropped after cancellation (worker kept generating briefly).
    pub post_cancel_frames: u64,
    /// Usage reported by the worker, relayed verbatim (opaque OpenAI object).
    pub usage: Option<Value>,
    /// Final finish kind reported by the worker.
    pub finish: Option<FinishKind>,
    /// Worker-side error attached to a `Done{finish: error}`.
    pub worker_error: Option<WorkerError>,
    /// Bytes streamed to consumers, total across attempts.
    pub delivered_bytes: u64,
}

/// Lifecycle phases. Phase order is enforced only by the actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobPhase {
    /// Accepted, waiting for a ready worker inside the pre-body grace window.
    Pending,
    /// Enqueued in the pool queue.
    Queued,
    /// Leased to a worker, waiting for the first frame.
    Assigned,
    /// At least one frame relayed.
    Streaming,
    /// Terminal, consumer saw finish.
    Finished,
    /// Terminal, consumer saw an in-band error.
    Failed,
    /// Terminal, consumer cancelled.
    Cancelled,
}

impl JobPhase {
    /// True for the three absorbing phases.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Finished | Self::Failed | Self::Cancelled)
    }
}

/// Timing marks; every one is set by the actor from injected time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct JobTimings {
    /// When the hub accepted the submission.
    pub accept_ms: Timestamp,
    /// When the first `Assigned` was emitted.
    pub dispatch_ms: Option<Timestamp>,
    /// First worker frame received.
    pub first_frame_ms: Option<Timestamp>,
    /// Terminal frame received.
    pub last_frame_ms: Option<Timestamp>,
}

/// Derived consumer-facing view of one job. Built by
/// [`crate::actor::PoolActor::job_view`]; it is derived state, never a second
/// authority.
#[derive(Debug, Clone, Serialize)]
pub struct JobView {
    /// Job identity.
    pub id: JobId,
    /// Pool.
    pub pool: PoolId,
    /// Kind.
    pub kind: String,
    /// Tenant.
    pub tenant: TenantId,
    /// Lifecycle phase.
    pub phase: JobPhase,
    /// Dispatch attempts so far.
    pub attempt: u32,
    /// Terminal reason, when terminal.
    pub terminal: Option<TerminalReason>,
    /// Fairness class.
    pub priority: Priority,
    /// Payload size in bytes.
    pub payload_bytes: u64,
    /// Payload digest.
    pub payload_sha256: String,
    /// Resolved requirements.
    pub requires: Requires,
    /// Timing marks.
    pub timings: JobTimings,
    /// Highest contiguous seq received.
    pub acked_seq: u64,
    /// Gaps observed.
    pub gaps: u64,
    /// Duplicates dropped.
    pub duplicates: u64,
    /// Frames still replayable.
    pub replay_frames: usize,
    /// Consumers attached.
    pub consumers: usize,
    /// Bytes relayed.
    pub delivered_bytes: u64,
    /// Authoritative lease, if any.
    pub lease: Option<Lease>,
    /// Produced artifact, if any.
    pub artifact: Option<ArtifactRecord>,
    /// Verbatim usage object.
    pub usage: Option<Value>,
    /// Final finish reason.
    pub finish: Option<FinishKind>,
    /// Worker-side error.
    pub worker_error: Option<WorkerError>,
    /// Whether the open-early SSE contract applies.
    pub streaming: bool,
    /// Whether this kind may be requeued.
    pub idempotent: bool,
}

/// Metadata for a produced artifact. Bytes live on disk/object storage (task
/// §7.3), never in this record beyond the descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArtifactRecord {
    /// Handle.
    pub id: ArtifactId,
    /// Job that produced it.
    pub job: JobId,
    /// Declared MIME type.
    pub content_type: String,
    /// Total bytes, from the worker manifest.
    pub size_bytes: u64,
    /// sha256 from the worker manifest; hub re-verifies what it actually stored.
    pub declared_sha256: Option<String>,
    /// sha256 the hub computed over stored bytes.
    pub stored_sha256: Option<String>,
    /// Filesystem path (server-side) or storage key.
    pub path: String,
    /// Chunks recorded so far, keyed by chunk index.
    pub chunks: BTreeMap<u64, u64>,
    /// Absolute retention deadline.
    pub expires_ms: Timestamp,
    /// True once the artifact is complete (all bytes stored, size matches).
    pub complete: bool,
}

impl ArtifactRecord {
    /// Bytes recorded so far, summed from the chunk index.
    #[must_use]
    pub fn recorded_bytes(&self) -> u64 {
        self.chunks.values().sum()
    }
}

/// Which reference a fetch/put token unlocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefScope {
    /// A spilled request payload the hub serves to the owning worker.
    PayloadFetch,
    /// An artifact upload slot the owning worker writes into.
    ArtifactPut,
    /// An artifact download slot for the consumer that owns the job.
    ArtifactGet,
}

/// Everything an edge adapter hands the actor when a consumer submits work
/// (task §7.1: the adapters speak the dialect, the spine does not).
///
/// `Serialize`/`Deserialize` so the WASM host can build a submission in JS with
/// the identical schema the native adapters produce.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Submission {
    /// Registry kind.
    pub kind: String,
    /// Pool resolved from the kind.
    pub pool: PoolId,
    /// Consumer account.
    pub tenant: TenantId,
    /// Resolved capability requirements.
    pub requires: Requires,
    /// Opaque request body, forwarded to the worker mostly verbatim.
    pub payload: Value,
    /// Declared content type.
    pub content_type: String,
    /// Explicit or content-hash idempotency key (task §5.2).
    pub idempotency_key: String,
    /// Session hint header, used only for affinity (task §17.4).
    pub session_hint: Option<String>,
    /// Fairness class.
    pub priority: Priority,
    /// `idempotent` copied from the kind registry, never from the client.
    pub idempotent: bool,
    /// Streaming contract (`true` = open-early SSE).
    pub streaming: bool,
    /// Cancel the job when its last consumer detaches. `true` for an SSE chat
    /// request (the socket *is* the subscription); `false` for `POST /v1/jobs`,
    /// which is explicitly a detached job the client may poll later.
    pub cancel_on_consumer_gone: bool,
}

impl JobRecord {
    /// Build a fresh, non-terminal record from a validated submission.
    #[must_use]
    pub fn new(
        id: JobId,
        sub: &Submission,
        payload_bytes: u64,
        payload_sha256: String,
        now_ms: Timestamp,
        dispatch_deadline_ms: Timestamp,
        hard_deadline_ms: Timestamp,
        replay_frames: usize,
        replay_bytes: usize,
    ) -> Self {
        Self {
            payload_inline: Some(sub.payload.clone()),
            id,
            pool: sub.pool.clone(),
            kind: sub.kind.clone(),
            idempotent: sub.idempotent,
            streaming: sub.streaming,
            cancel_on_consumer_gone: sub.cancel_on_consumer_gone,
            tenant: sub.tenant.clone(),
            session_hint: sub.session_hint.clone(),
            priority: sub.priority,
            requires: sub.requires.clone(),
            payload_ref: None,
            content_type: sub.content_type.clone(),
            payload_bytes,
            payload_sha256,
            idempotency_key: sub.idempotency_key.clone(),
            attempt: 0,
            last_worker: None,
            phase: JobPhase::Pending,
            terminal: None,
            seq: SeqTracker::new(),
            replay: ReplayBuffer::new(replay_frames, replay_bytes),
            partial_text: String::new(),
            output_opaque: false,
            flush: FlushGate::default(),
            last_ping_ms: None,
            consumers: BTreeSet::new(),
            bytes_delivered: false,
            cancel_sent_ms: None,
            artifact: None,
            timings: JobTimings { accept_ms: now_ms, ..JobTimings::default() },
            deadline_ms: hard_deadline_ms,
            dispatch_deadline_ms,
            queue_seq: None,
            continuations: 0,
            post_cancel_frames: 0,
            usage: None,
            finish: None,
            worker_error: None,
            delivered_bytes: 0,
        }
    }

    /// Mark terminal and return the reason for metrics/relay bookkeeping.
    pub fn terminate(&mut self, reason: TerminalReason, now_ms: Timestamp) -> TerminalReason {
        self.terminal = Some(reason);
        self.phase = match reason {
            TerminalReason::Cancelled => JobPhase::Cancelled,
            TerminalReason::Completed => JobPhase::Finished,
            _ => JobPhase::Failed,
        };
        self.timings.last_frame_ms = Some(now_ms);
        reason
    }

    /// True when the job still occupies a tenant's concurrency budget.
    #[must_use]
    pub fn occupies_tenant_slot(&self) -> bool {
        !self.phase.is_terminal()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Attestation;
    use crate::protocol::Requires;

    fn worker(capacity: u32, free_slots: u32) -> WorkerRecord {
        let mut w = WorkerRecord::new(
            WorkerId::new_unchecked("wkr-a"),
            1,
            capacity,
            Requires::default(),
            Attestation::default(),
            BTreeSet::from([PoolId::new_unchecked("llm.chat")]),
            WorkerTier::Dedicated,
            1_000,
        );
        w.free_slots = free_slots;
        w.engine = EngineState::Ready;
        w
    }

    #[test]
    fn available_slots_reconciles_heartbeat_against_own_bookkeeping() {
        // Heartbeat claims all 4 slots free while 2 are leased → the actor's own
        // count wins, so a stale heartbeat cannot over-dispatch.
        let mut w = worker(4, 4);
        w.in_flight = BTreeSet::from([JobId::new_unchecked("j1"), JobId::new_unchecked("j2")]);
        assert_eq!(w.available_slots(), 2);

        // Heartbeat reporting fewer free slots than we know about must also win:
        // the engine, not the hub, knows when a slot is busy.
        let mut w2 = worker(4, 1);
        w2.in_flight = BTreeSet::from([JobId::new_unchecked("j1")]);
        assert_eq!(w2.available_slots(), 1);
    }

    #[test]
    fn loading_worker_is_advertised_but_never_dispatchable() {
        let mut w = worker(4, 4);
        w.engine = EngineState::Loading;
        assert!(w.advertised());
        assert!(!w.dispatchable());
        w.engine = EngineState::Ready;
        assert!(w.dispatchable());
        // Orphaned sockets are out of the dispatch set even if still "Ready".
        w.link = LinkState::Orphaned;
        assert!(!w.dispatchable());
    }

    #[test]
    fn priority_and_reason_mapping_is_exact() {
        assert_eq!(Priority::from_name("realtime"), Priority::Realtime);
        assert_eq!(Priority::from_name("typo"), Priority::Default);
        assert_eq!(Priority::Bulk.as_str(), "bulk");
        assert!(TerminalReason::NoReadyWorker.retryable());
        assert!(TerminalReason::WorkerGonePartial.retryable() == false);
        assert_eq!(TerminalReason::LeaseExpiredPartial.as_str(), "lease_expired_partial");
    }
}
