//! Wire protocols (task §4, spec §7): frame vocabulary, version negotiation,
//! size caps and the JSON-lines codec, for **both** faces of the hub.
//!
//! M0 wire encoding is **JSON lines** — one frame per line, `{"type": ...}`
//! discriminator — because it is debuggable from a notebook cell and the
//! protocol is not stable yet (task §17.1, resolved: JSON until stable, a
//! length-prefixed binary encoding later). The codec below is target-agnostic:
//! the native runtime and the WASM host use the exact same functions.
//!
//! # The two dialects
//!
//! * [`WorkerFrame`] / [`HubFrame`] — the supplier face (`/v1/worker`,
//!   `nexus-server/src/worker_ws.rs`): `Hello`/`Ready`/`Claim`/`Frame`/`Done`…
//! * [`ConsumerFrame`] / [`HubConsumerFrame`] — the consumer face
//!   (`GET /v1/consumer`, NEX-3, `nexus-server/src/consumer_ws.rs`):
//!   `submit`/`attach`/`detach`/`cancel` in, `accepted`/`rejected` + the
//!   [`crate::relay::ConsumerEvent`] vocabulary tagged with the job id, and an
//!   explicit `end` out (NEX-3.2/3.3/3.4). One JSON object per WebSocket text
//!   message; the frame *is* the message, so no line splitting is involved.
//!
//! The two vocabularies are deliberately disjoint (`submit` vs `frame`,
//! `accepted` vs `assigned`): a consumer frame can never be mistaken for a
//! supplier frame, which is what NEX-2.2 requires of a hub that fans one task
//! out to several subscribers.

use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::errors::CoreError;
use crate::ids::{JobId, PoolId, TenantId, WorkerId};
use crate::model::{JobView, WorkerTier};
use crate::relay::ConsumerEvent;
use crate::time::Timestamp;
use std::collections::BTreeSet;

/// Protocol version carried in `Hello.protocol`. Kept as a single integer
/// because the only incompatibility we negotiate today is the frame vocabulary.
pub const WIRE_VERSION: u32 = crate::PROTOCOL_VERSION;

/// The wildcard model: a consumer requesting `auto` takes whichever model in
/// the pool is free - the scheduler's own ranking picks one.
pub const AUTO_MODEL: &str = "auto";

/// Size caps for one direction of a worker connection (task §9 "frame/payload
/// size limits on the worker connection").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireCaps {
    /// Largest single JSON line accepted (or produced) on the socket.
    pub max_line_bytes: usize,
    /// Largest `Frame.text` body accepted inside one frame.
    pub max_frame_text_bytes: usize,
    /// Largest inline job payload the hub will push to a worker (task §10
    /// "honest inline frame cap", ~8 MB).
    pub max_inline_payload_bytes: usize,
}

impl Default for WireCaps {
    fn default() -> Self {
        Self {
            max_line_bytes: 1 << 20,           // 1 MiB control line
            max_frame_text_bytes: 256 << 10,   // 256 KiB delta/event body
            max_inline_payload_bytes: 8 << 20, // 8 MiB inline cap
        }
    }
}

/// Capability requirements. Used both by a submission ("what I need") and by a
/// worker's `Hello` ("what I offer"), which is why matching is a subset test.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Requires {
    /// Model name the job must run on. Not trusted from workers — see
    /// [`Attestation`]; this is the *advertised* label.
    #[serde(default)]
    pub model: Option<String>,
    /// Minimum context window, in tokens.
    #[serde(default)]
    pub min_context_tokens: Option<u64>,
    /// Named capabilities: `vision`, `tools`, `logprobs`, `audio`, ...
    #[serde(default)]
    pub capabilities: BTreeSet<String>,
    /// Expected KV footprint of this prompt, in tokens. Drives KV-aware
    /// scheduling (task §8) and per-tenant prompt limits.
    #[serde(default)]
    pub kv_estimate_tokens: Option<u64>,
}

impl Requires {
    /// Capability pre-check: can a worker offering `offered` serve a job that
    /// requires `self`? Unknown-required capabilities are unsatisfiable, which
    /// is what makes the immediate `400` of task §8/§16.10 correct.
    #[must_use]
    pub fn satisfied_by(&self, offered: &Requires) -> bool {
        if let Some(model) = &self.model {
            if offered.model.as_deref() != Some(model.as_str()) {
                return false;
            }
        }
        if let Some(min) = self.min_context_tokens {
            if offered.min_context_tokens.unwrap_or(0) < min {
                return false;
            }
        }
        if let Some(estimate) = self.kv_estimate_tokens {
            // A worker that cannot report its context budget cannot promise it.
            if offered.min_context_tokens.unwrap_or(0) < estimate {
                return false;
            }
        }
        self.capabilities.is_subset(&offered.capabilities)
    }

    /// Model-only matching - the routing contract. A supplier registers its
    /// model, a consumer requests that model, and the payload travels as-is:
    /// what the engine cannot serve it answers itself, and the hub relays that
    /// error to the consumer. Capabilities and context floors stay as
    /// advertisement metadata but gate nothing.
    #[must_use]
    pub fn serves_model(&self, offered: &Requires) -> bool {
        match &self.model {
            // `auto` is the wildcard: whichever model in the pool is free.
            Some(model) if model == AUTO_MODEL => true,
            Some(model) => offered.model.as_deref() == Some(model.as_str()),
            None => true,
        }
    }

    /// Union used when a worker joins several pools with different offers.
    /// Mutates `self`; no `#[must_use]` because the return value is `()` and the
    /// effect *is* the mutation.
    pub fn merge(&mut self, other: &Self) {
        if self.model.is_none() {
            self.model.clone_from(&other.model);
        }
        self.min_context_tokens = self.min_context_tokens.max(other.min_context_tokens);
        self.capabilities.extend(other.capabilities.iter().cloned());
    }
}

/// Model identity a worker proves, not asserts (task §9). The reference runtime
/// already carries these digests as its download trust anchor
/// (`services/molab/molab-strata-runtime.py:94` `SHARD_TABLE`), so the hub's
/// allowlist reuses those SHA-256s instead of inventing an identifier.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attestation {
    /// Lowercase hex SHA-256 digests of the weights actually loaded.
    #[serde(default)]
    pub model_shas: BTreeSet<String>,
    /// Total weight bytes loaded, informational.
    #[serde(default)]
    pub weight_bytes: Option<u64>,
}

/// Frame classes a worker may emit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FrameKind {
    /// A token/chunk delta, relayed verbatim.
    Delta,
    /// A structured progress event (ComfyUI-style `executing`/`progress`).
    Event,
    /// Operator-visible log line.
    Log,
    /// Bulk output; bytes travel over HTTP, the frame carries a descriptor.
    ArtifactChunk,
}

/// Why a generation stopped. Mirrors the OpenAI vocabulary because the relay
/// must pass it through untouched (task §7.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinishKind {
    /// Natural stop.
    Stop,
    /// Hit the token limit.
    Length,
    /// The model asked for tool calls.
    ToolCalls,
    /// Content filter stopped the generation.
    ContentFilter,
    /// Worker-side failure; `Done.error` explains.
    Error,
}

impl FinishKind {
    /// OpenAI `finish_reason` string, used only when re-encoding an assembled
    /// (non-streamed) response.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Stop => "stop",
            Self::Length => "length",
            Self::ToolCalls => "tool_calls",
            Self::ContentFilter => "content_filter",
            Self::Error => "error",
        }
    }
}

/// Worker-reported failure attached to `Done{finish: error}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerError {
    /// Machine code.
    pub code: String,
    /// Human message.
    pub message: String,
    /// Worker's own retryability hint; the hub still applies §5.3's rules.
    #[serde(default)]
    pub retryable: bool,
}

/// Descriptor for bulk output. `Frame{artifact_chunk}` carries this so tens of
/// MB never enter the hub's RSS over a JSON socket (task §7.3/§10).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactManifest {
    /// Artifact handle the worker PUT the bytes under.
    pub artifact: String,
    /// Declared MIME type.
    #[serde(default)]
    pub content_type: Option<String>,
    /// Total artifact size in bytes.
    #[serde(default)]
    pub size_bytes: Option<u64>,
    /// sha256 of the whole artifact as computed by the worker.
    #[serde(default)]
    pub sha256: Option<String>,
    /// Index of the chunk this frame announces.
    #[serde(default)]
    pub chunk_index: Option<u64>,
    /// Bytes in that chunk.
    #[serde(default)]
    pub chunk_bytes: Option<u64>,
}

/// Where the job payload lives from the worker's point of view (task §10).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum JobPayload {
    /// Small enough to ride the socket inside `Assigned`.
    Inline {
        /// Declared content type.
        content_type: String,
        /// Opaque body. The hub never interprets it; only adapters do.
        body: Value,
    },
    /// Too big: the worker `GET`s it after the claim.
    #[serde(rename = "ref")]
    ByRef {
        /// Path to fetch, e.g. `/v1/payloads/job-1a2b/3`.
        path: String,
        /// Bearer token scoped to this fetch (single worker, single job).
        token: String,
        /// sha256 of the bytes at the other end, so a worker can verify.
        sha256: String,
        /// Size in bytes, so a worker can pre-flight its own memory.
        size_bytes: u64,
        /// Fetch must start before this instant.
        expires_ms: Timestamp,
        /// Declared content type.
        content_type: String,
    },
}

/// Why the hub asked a worker to stop producing (task §6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelReason {
    /// Consumer disconnected or explicitly cancelled.
    ConsumerGone,
    /// Per-job hard cap elapsed.
    Deadline,
    /// Hub is draining for shutdown.
    HubShutdown,
    /// Worker was evicted and its jobs are being handed elsewhere.
    Evicted,
    /// Lease expired and the output is already terminal for the consumer.
    LeaseExpired,
}

/// Why the hub is closing a worker socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ByeReason {
    /// `Hello.protocol` is not supported.
    VersionMismatch,
    /// Attested digests are not on the allowlist.
    AttestationRejected,
    /// Pool token missing/unknown (also sent on HTTP 401 upgrades).
    AuthRejected,
    /// A newer socket with the same `worker_id` took over (task §11).
    Evicted,
    /// No heartbeat before the lease deadline.
    HeartbeatTimeout,
    /// Hub shutdown.
    Shutdown,
}

impl ByeReason {
    /// WebSocket close code to pair with the reason, so a client can branch.
    #[must_use]
    pub fn close_code(&self) -> u16 {
        match self {
            Self::VersionMismatch => 1002, // protocol error
            Self::AttestationRejected | Self::AuthRejected => 1008, // policy violation
            Self::Evicted | Self::HeartbeatTimeout => 1000, // normal, reconnect expected
            Self::Shutdown => 1001,        // going away
        }
    }
}

/// A frame sent by a worker. `#[serde(tag = "type")]` gives the JSON-lines
/// discriminator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkerFrame {
    /// First frame on a connection: identity, pools, capacity, version, proof.
    Hello {
        /// Stable worker identity.
        worker_id: String,
        /// Pools this worker serves.
        pools: Vec<String>,
        /// Declared concurrent slots (reference worker: `PARALLEL = 4`).
        capacity: u32,
        /// What it can serve.
        #[serde(default)]
        requires: Requires,
        /// Proof of what it loaded.
        #[serde(default)]
        attestation: Attestation,
        /// Protocol version requested.
        protocol: u32,
        /// Engine label, informational.
        #[serde(default)]
        engine: Option<String>,
        /// Availability tier request; the hub may downgrade it in config.
        #[serde(default)]
        tier: Option<WorkerTier>,
    },
    /// Heartbeat + readiness + capacity report (task §4, §8).
    Ready {
        /// Free engine slots right now.
        free_slots: u32,
        /// Free KV budget in tokens, when the engine can report it.
        #[serde(default)]
        free_kv_tokens: Option<u64>,
        /// Engine genuinely ready: loaded, slot count confirmed, warmup and
        /// admission probes passed (reference `start()` semantics).
        engine_ready: bool,
        /// Raw engine status payload, relayed for scheduling/telemetry.
        #[serde(default)]
        telemetry: Option<Value>,
    },
    /// Pull authority: "give me up to N jobs".
    Claim {
        /// How many jobs the worker can accept; `None` means 1.
        #[serde(default)]
        jobs: Option<u32>,
    },
    /// One output frame for one job, seq-numbered (task §4).
    Frame {
        /// Job this frame belongs to.
        job: JobId,
        /// Monotonic per-job sequence number.
        seq: u64,
        /// Frame class.
        kind: FrameKind,
        /// MIME type of `text`.
        #[serde(default)]
        content_type: Option<String>,
        /// Verbatim delta/log text or JSON-encoded event body.
        #[serde(default)]
        text: String,
        /// Artifact descriptor; required when `kind == artifact_chunk`.
        #[serde(default)]
        artifact: Option<ArtifactManifest>,
    },
    /// Terminal for a job: `finish` + final `seq` + `usage` (+ optional error).
    Done {
        /// Job that finished.
        job: JobId,
        /// Final sequence number.
        seq: u64,
        /// Why generation stopped.
        finish: FinishKind,
        /// OpenAI-shaped usage object, relayed untouched.
        #[serde(default)]
        usage: Option<Value>,
        /// Present when `finish == error`.
        #[serde(default)]
        error: Option<WorkerError>,
    },
    /// Post-reconnect re-attach to a still-leased job (task §4 Resume).
    Resume {
        /// Job the worker believes it still owns.
        job: JobId,
        /// Attempt the worker was assigned, so a stale Resume is detectable.
        attempt: u32,
        /// Highest seq the worker knows the hub received.
        acked_seq: u64,
    },
    /// Acknowledgement that generation stopped for a `Cancel`.
    CancelAck {
        /// Job that was cancelled.
        job: JobId,
    },
}

/// A frame sent by the hub.
///
/// `large_enum_variant` is allowed here rather than fixed by boxing, and the reason
/// is a size that no hot path pays: the only `HubFrame` in flight at any moment is
/// the one inside `Effect::SendWorker { frame: Box<HubFrame> }`
/// (`actor.rs:349-361`), so `Assigned`'s ~404-byte inline payload costs one heap
/// allocation per dispatched job — exactly the allocation the box already forces.
/// Boxing `payload` (clippy's suggestion) would also shrink the enum, but its single
/// construction site is `actor.rs:2175`, a file this work package does not own, so
/// the shrink is a handoff rather than an edit here.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HubFrame {
    /// Handshake accepted; carries the operating parameters for this socket.
    Welcome {
        /// Negotiated protocol version.
        protocol: u32,
        /// Worker id the hub bound this socket to.
        worker_id: WorkerId,
        /// Connection generation the worker should treat as current.
        epoch: u64,
        /// Pools actually joined after allowlist checks.
        pools: Vec<PoolId>,
        /// How often the worker must send `Ready`.
        heartbeat_ms: u64,
        /// Lease lifetime granted per dispatch; `Ready` renews it.
        lease_ms: u64,
        /// Largest frame body the hub will accept from this worker.
        frame_cap_bytes: usize,
        /// Jobs handed back after a successful `Resume`.
        #[serde(default)]
        resumed: Vec<ResumedJob>,
    },
    /// You own this job now (task §4 Assigned).
    Assigned {
        /// Job identity.
        job: JobId,
        /// Attempt number; 1 on first dispatch, incremented after a requeue.
        attempt: u32,
        /// Registry kind string the worker-side adapter switches on.
        kind: String,
        /// Consumer account, for the worker's own logging/quota attribution.
        tenant: TenantId,
        /// Resolved requirements.
        requires: Requires,
        /// Inline payload or a ref to fetch.
        payload: JobPayload,
        /// When the lease must be renewed by.
        lease_expires_ms: Timestamp,
        /// Hard per-job deadline.
        job_deadline_ms: Timestamp,
        /// Lowest `seq` this attempt may emit: the highest seq the hub has already
        /// relayed for this job. A first dispatch is `0`; a requeue or a
        /// resume-by-continuation (§7.4) hands the new owner a floor, so its
        /// frames are not mistaken for duplicates of the previous attempt's.
        #[serde(default)]
        start_seq: u64,
        /// Resume-by-continuation hint: text already streamed to the consumer
        /// that the worker should continue after (task §7.4). Opaque to the hub.
        #[serde(default)]
        continuation_text: Option<String>,
        /// Upload slot for a non-streaming kind's artifact (task §7.3/§10): the
        /// worker PUTs bulk bytes here instead of pushing them over this socket.
        #[serde(default)]
        artifact_put: Option<ArtifactPut>,
    },
    /// Stop producing / discard (task §6 — mandatory).
    Cancel {
        /// Job to stop.
        job: JobId,
        /// Why.
        reason: CancelReason,
    },
    /// Flow-control ack: everything up to `seq` is durably relayed or buffered,
    /// so a reconnecting worker may resume from `seq + 1`.
    Ack {
        /// Job the ack covers.
        job: JobId,
        /// Highest contiguous seq received.
        seq: u64,
    },
    /// Pre-`Welcome` rejection with a machine-readable reason.
    Reject {
        /// Stable code, e.g. `protocol_mismatch`.
        code: String,
        /// Human-readable detail.
        message: String,
    },
    /// A `Resume` the hub could not honour; the worker must stop re-sending.
    ResumeNack {
        /// Job the worker asked about.
        job: JobId,
        /// Attempt the worker was on.
        attempt: u32,
        /// Why it was refused (grace elapsed, owner moved, already terminal).
        reason: String,
    },
    /// Closing the socket.
    Bye {
        /// Why.
        reason: ByeReason,
    },
}

/// A job the hub re-attached to a reconnecting worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumedJob {
    /// Job still owned by this worker.
    pub job: JobId,
    /// Attempt that remains valid.
    pub attempt: u32,
    /// Seq the worker should continue from.
    pub resume_from_seq: u64,
    /// New lease deadline after renewal.
    pub lease_expires_ms: Timestamp,
}

/// Pre-signed upload slot handed to a worker with its assignment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactPut {
    /// Artifact handle the bytes belong to.
    pub artifact: String,
    /// Path to `PUT` to (chunked with a `Content-Range` header).
    pub path: String,
    /// Bearer token scoped to this upload (one worker, one attempt).
    pub token: String,
    /// Upload must finish before this instant.
    pub expires_ms: Timestamp,
    /// Largest chunk the hub will accept.
    pub chunk_bytes: usize,
}

/// Why a task's result stream ended on a consumer connection (NEX-3.4: the end is
/// always explicit, so a client can tell "answered", "failed" and "stopped watching"
/// apart instead of inferring them from silence).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndReason {
    /// A `finish` event was delivered: the answer is complete.
    Finished,
    /// An in-band `error` event was delivered (worker failure, cancellation, hub
    /// shutdown, an expired replay window).
    Errored,
    /// This connection could not keep up, so frames were dropped and the stream was
    /// ended in band rather than allowed to stall the pool (NEX-5.3).
    Lagged,
    /// This connection asked to stop watching (`detach`); the task itself lives on
    /// under the NEX-6.6 detached contract.
    Detached,
    /// The connection is going away while the task is still live. Attached tasks are
    /// orphaned and deleted by that same event (NEX-6.1/6.3).
    Closed,
}

/// A frame a consumer sends on the hub's consumer WebSocket (`GET /v1/consumer`,
/// NEX-3). The vocabulary is disjoint from [`WorkerFrame`] on purpose: a hub that
/// fans one task out to several subscribers must never confuse a consumer frame
/// with a supplier frame (NEX-2.2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ConsumerFrame {
    /// Post a task on this connection and stream its answer back on the same one
    /// (NEX-3.1). The verdict (`accepted` / `rejected`) always precedes any event
    /// bytes for the task (NEX-3.3).
    Submit {
        /// Registry kind, e.g. `image.workflow`. Decides the pool unless `pool` says
        /// otherwise, exactly as `POST /v1/jobs` does.
        kind: String,
        /// Opaque payload, forwarded to the supplier verbatim.
        payload: Value,
        /// Optional pool override; the kind normally decides.
        #[serde(default)]
        pool: Option<String>,
        /// Optional capability requirements the consumer declares. Absent means the
        /// kind's registered floor, never a blank offer.
        #[serde(default)]
        requires: Option<Requires>,
        /// Optional priority class name.
        #[serde(default)]
        priority: Option<String>,
        /// NEX-6.6's explicit opt-in: "post it now, I will pick the result up later".
        /// `false` (the default) keeps NEX-6.1 in force — this connection **is** the
        /// subscription, so hanging up orphans and deletes the task.
        #[serde(default)]
        detach: bool,
        /// Explicit idempotency key, namespaced by the authenticated tenant. Absent
        /// means a fresh per-submit key: a consumer that wants "don't run this twice"
        /// asks for it, rather than having identical payloads silently merged.
        #[serde(default)]
        idempotency_key: Option<String>,
    },
    /// Follow a task this hub already knows, replaying from `from_seq` (`0` means
    /// "everything still buffered"). Same replay machinery and cursor semantics as
    /// `GET /v1/jobs/{id}/events` (task §7.5).
    Attach {
        /// Task to follow.
        job: JobId,
        /// Replay floor.
        #[serde(default)]
        from_seq: u64,
    },
    /// Stop watching one task **without** cancelling it. Only ever honoured for a task
    /// this connection was watching, and the task's own contract decides what losing
    /// this subscription means: an attached task is orphaned and deleted (NEX-6.3), a
    /// detached one keeps running (NEX-6.6).
    Detach {
        /// Task to stop watching.
        job: JobId,
    },
    /// Cancel one task explicitly. This is a consumer-initiated cancellation, so the
    /// record keeps its normal terminal retention — unlike connection loss, which
    /// deletes the task outright (NEX-6.3).
    Cancel {
        /// Task to cancel.
        job: JobId,
    },
}

/// A frame the hub sends on a consumer connection.
///
/// The event variants carry the **same field names** as [`ConsumerEvent`] plus the
/// `job` they belong to: the supplier's stream is the consumer's stream
/// (NEX-5.2), and the job id is what makes one connection able to carry several
/// tasks at once (NEX-3.2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HubConsumerFrame {
    /// Verdict: this task is accepted and its stream is open (NEX-3.3).
    Accepted {
        /// Task identity the consumer should use from now on.
        job: JobId,
        /// Echo of the registry kind.
        kind: String,
        /// Pool the task landed in.
        pool: PoolId,
        /// Echo of the NEX-6.6 opt-in, so a client can tell which contract it got.
        detached: bool,
    },
    /// Verdict: this task was refused, with the reason the HTTP surface would give
    /// (status + machine code), plus `retry_after_secs` when the hub means "not now"
    /// rather than "no" (NEX-3.3).
    Rejected {
        /// Task this verdict answers.
        job: JobId,
        /// HTTP status the equivalent HTTP call would have carried.
        status: u16,
        /// Stable machine code, e.g. `queue_full`.
        code: String,
        /// Human detail.
        message: String,
        /// Seconds to wait before retrying; absent means "do not retry this request".
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retry_after_secs: Option<u64>,
    },
    /// Hub-issued snapshot that opens a stream (§5.4 item 5: "the first frame after
    /// attach is meta"). Based on [`ConsumerEvent::Meta`], with the two fields the pool
    /// actor does not carry and a consumer needs to place the stream: the job's current
    /// phase, and **when this stream was attached**.
    ///
    /// The extras are optional rather than zero-filled because the frame has two
    /// producers: this face fills them from the job record it just read, while a
    /// `meta` event that arrives through the relay (the actor emitting one is legal
    /// vocabulary) can only fill the mirror fields.
    Meta {
        /// Task these bytes belong to.
        job: JobId,
        /// Attempt that produced them.
        attempt: u32,
        /// Pool the task runs in.
        pool: String,
        /// Kind string.
        kind: String,
        /// `Pending`/`Queued`/`Assigned`/`Finished`…, as the actor's record has it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        state: Option<String>,
        /// Wall-clock ms at which this stream was attached, so a client that reconnects
        /// can tell a resumed stream from a fresh one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attached_ms: Option<u64>,
    },
    /// Verbatim delta text from the supplier, mirroring [`ConsumerEvent::Delta`].
    Delta {
        /// Task this delta belongs to.
        job: JobId,
        /// Supplier seq.
        seq: u64,
        /// Raw text exactly as received.
        text: String,
    },
    /// Structured progress event, mirroring [`ConsumerEvent::Event`].
    Event {
        /// Task this event belongs to.
        job: JobId,
        /// Supplier seq.
        seq: u64,
        /// Verbatim JSON body.
        payload: Value,
    },
    /// Operator log line, mirroring [`ConsumerEvent::Log`].
    Log {
        /// Task this line belongs to.
        job: JobId,
        /// Supplier seq.
        seq: u64,
        /// Log text.
        text: String,
    },
    /// Artifact availability, mirroring [`ConsumerEvent::Artifact`]. The bytes
    /// themselves travel over HTTP (NEX-7.4).
    Artifact {
        /// Task that produced it.
        job: JobId,
        /// Supplier seq.
        seq: u64,
        /// Descriptor with digest and size.
        manifest: ArtifactManifest,
    },
    /// Terminal success, mirroring [`ConsumerEvent::Finish`].
    Finish {
        /// Task that finished.
        job: JobId,
        /// Final seq.
        seq: u64,
        /// Why generation stopped.
        finish: FinishKind,
        /// Verbatim usage object.
        #[serde(default)]
        usage: Option<Value>,
    },
    /// Terminal in-band failure, mirroring [`ConsumerEvent::Error`]. `job` is absent
    /// only for a connection-level error (an undecodable frame, a stalled socket),
    /// where no single task owns the problem.
    Error {
        /// Task this error ends, when the error belongs to one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        job: Option<JobId>,
        /// Stable machine code.
        code: String,
        /// Human detail.
        message: String,
        /// Whether the consumer may submit the whole task again.
        #[serde(default)]
        retryable: bool,
        /// Seq range the hub knows is complete.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        through_seq: Option<u64>,
    },
    /// A hole in the seq stream, mirroring [`ConsumerEvent::Gap`] (NEX-5.2: a gap is
    /// reported in band rather than papered over).
    Gap {
        /// Task the gap is in.
        job: JobId,
        /// First missing seq.
        expected: u64,
        /// Seq that arrived instead.
        got: u64,
    },
    /// Replay marker: everything after this point came from the ring buffer,
    /// mirroring [`ConsumerEvent::Replay`].
    Replay {
        /// Task being replayed.
        job: JobId,
        /// Seq the replay resumed from.
        from_seq: u64,
    },
    /// Explicit end-of-stream marker for one task (NEX-3.4). Exactly one `end`
    /// follows every task this connection started, whatever happened in between.
    End {
        /// Task whose stream is over on this connection.
        job: JobId,
        /// Why it ended, so a client can branch instead of guessing.
        reason: EndReason,
        /// Highest seq delivered on this connection, usable as `attach{from_seq}`
        /// when the consumer reconnects.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        through_seq: Option<u64>,
    },
}

impl HubConsumerFrame {
    /// Tag one relay event with the task it belongs to. Field-for-field this is the
    /// `ConsumerEvent` shape plus `job`, which is the whole NEX-3.2 requirement
    /// ("the stream for each task is tagged with that task's id").
    #[must_use]
    pub fn relayed(job: &JobId, event: ConsumerEvent) -> Self {
        match event {
            ConsumerEvent::Meta {
                job: _,
                attempt,
                pool,
                kind,
            } => Self::Meta {
                // The relay's meta carries only what the actor stores; the snapshot
                // fields belong to whoever opened the stream (see [`Self::meta`]), so
                // they stay absent here rather than being guessed at.
                job: job.clone(),
                attempt,
                pool,
                kind,
                state: None,
                attached_ms: None,
            },
            ConsumerEvent::Delta { seq, text } => Self::Delta {
                job: job.clone(),
                seq,
                text,
            },
            ConsumerEvent::Event { seq, payload } => Self::Event {
                job: job.clone(),
                seq,
                payload,
            },
            ConsumerEvent::Log { seq, text } => Self::Log {
                job: job.clone(),
                seq,
                text,
            },
            ConsumerEvent::Artifact { seq, manifest } => Self::Artifact {
                job: job.clone(),
                seq,
                manifest,
            },
            ConsumerEvent::Finish { seq, finish, usage } => Self::Finish {
                job: job.clone(),
                seq,
                finish,
                usage,
            },
            ConsumerEvent::Error {
                code,
                message,
                retryable,
                through_seq,
            } => Self::Error {
                job: Some(job.clone()),
                code,
                message,
                retryable,
                through_seq,
            },
            ConsumerEvent::Gap { expected, got } => Self::Gap {
                job: job.clone(),
                expected,
                got,
            },
            ConsumerEvent::Replay { from_seq } => Self::Replay {
                job: job.clone(),
                from_seq,
            },
        }
    }

    /// A connection-level error frame (no owning task): the socket itself is the
    /// thing that failed.
    #[must_use]
    pub fn connection_error(code: &str, message: &str) -> Self {
        Self::Error {
            job: None,
            code: code.to_string(),
            message: message.to_string(),
            retryable: true,
            through_seq: None,
        }
    }

    /// The `meta` frame that opens a stream, filled from the job record the face just
    /// read (§5.4 item 5).
    ///
    /// `attached_ms` is when THIS stream was established, which is what the spec asks
    /// `meta` to carry. The pool actor's own `meta` vocabulary has no such notion
    /// because its subscription is implicit; on this face the attach is an explicit
    /// event, and a client reconciling replay against live needs the point the stream
    /// started, not the point the task was queued.
    #[must_use]
    pub fn meta(view: &JobView, attached_ms: u64) -> Self {
        Self::Meta {
            job: view.id.clone(),
            attempt: view.attempt,
            pool: view.pool.as_ref().to_string(),
            kind: view.kind.clone(),
            state: Some(format!("{:?}", view.phase)),
            attached_ms: Some(attached_ms),
        }
    }

    /// The `end` frame that closes a subscription, §5.4 item 5.
    ///
    /// `through_seq` is the last sequence number this stream actually sent, so a client that
    /// reconnects knows exactly where to resume; `None` means the stream never carried a
    /// sequenced event.
    #[must_use]
    pub fn end_for(job: &JobId, reason: EndReason, through_seq: Option<u64>) -> Self {
        Self::End {
            job: job.clone(),
            reason,
            through_seq,
        }
    }
}

/// Negotiate the wire version. Anything but the supported version is a clean
/// reject, never undefined behavior (task §4).
///
/// # Errors
/// [`CoreError::VersionMismatch`] when the worker asks for something else.
pub fn negotiate(worker_version: u32) -> Result<(), CoreError> {
    if worker_version == WIRE_VERSION {
        Ok(())
    } else {
        Err(CoreError::VersionMismatch {
            requested: worker_version,
            supported: WIRE_VERSION,
        })
    }
}

/// Encode one frame as a single JSON line (no trailing newline).
///
/// # Errors
/// [`CoreError::TooLarge`] when the encoded frame would exceed `caps.max_line_bytes`,
/// [`CoreError::Json`] on a serialization failure.
pub fn encode_line<T: Serialize>(frame: &T, caps: &WireCaps) -> Result<String, CoreError> {
    let line = serde_json::to_string(frame)?;
    if line.len() > caps.max_line_bytes {
        return Err(CoreError::TooLarge {
            what: "line",
            limit: caps.max_line_bytes,
            actual: line.len(),
        });
    }
    Ok(line)
}

/// Decode one worker→hub line, enforcing the caps for that direction.
///
/// # Errors
/// [`CoreError::TooLarge`] / [`CoreError::Protocol`] / [`CoreError::Json`].
pub fn decode_worker_frame(line: &str, caps: &WireCaps) -> Result<WorkerFrame, CoreError> {
    guard_line(line, caps)?;
    let frame: WorkerFrame = serde_json::from_str(line)?;
    validate_worker_frame(&frame, caps)?;
    Ok(frame)
}

/// Decode one hub→worker line (used by the worker example, the WASM host and
/// protocol tests).
///
/// # Errors
/// [`CoreError::TooLarge`] / [`CoreError::Protocol`] / [`CoreError::Json`].
pub fn decode_hub_frame(line: &str, caps: &WireCaps) -> Result<HubFrame, CoreError> {
    guard_line(line, caps)?;
    let frame: HubFrame = serde_json::from_str(line)?;
    validate_hub_frame(&frame, caps)?;
    Ok(frame)
}

/// Decode one consumer→hub line (the `/v1/consumer` WebSocket dialect, NEX-3).
///
/// The consumer face sends one JSON object per WebSocket text message, so `line`
/// is that message with any trailing newline trimmed; the caps still apply because
/// the message is untrusted input of exactly the same kind a worker frame is.
///
/// # Errors
/// [`CoreError::TooLarge`] / [`CoreError::Protocol`] / [`CoreError::Json`].
pub fn decode_consumer_frame(line: &str, caps: &WireCaps) -> Result<ConsumerFrame, CoreError> {
    guard_line(line, caps)?;
    let frame: ConsumerFrame = serde_json::from_str(line)?;
    validate_consumer_frame(&frame, caps)?;
    Ok(frame)
}

/// Decode one hub→consumer line (used by consumer tests and by any host that wants
/// a typed reader instead of raw JSON).
///
/// # Errors
/// [`CoreError::TooLarge`] / [`CoreError::Protocol`] / [`CoreError::Json`].
pub fn decode_hub_consumer_frame(
    line: &str,
    caps: &WireCaps,
) -> Result<HubConsumerFrame, CoreError> {
    guard_line(line, caps)?;
    let frame: HubConsumerFrame = serde_json::from_str(line)?;
    Ok(frame)
}

fn guard_line(line: &str, caps: &WireCaps) -> Result<(), CoreError> {
    if line.len() > caps.max_line_bytes {
        return Err(CoreError::TooLarge {
            what: "line",
            limit: caps.max_line_bytes,
            actual: line.len(),
        });
    }
    Ok(())
}

/// Semantic validation of untrusted worker input (task §9: "a worker on hardware
/// someone else administers is still a worker, not a trusted peer"). Structural
/// serde success is not enough: ids, seq ordering and size limits are checked
/// here, before anything reaches the actor.
fn validate_worker_frame(frame: &WorkerFrame, caps: &WireCaps) -> Result<(), CoreError> {
    match frame {
        WorkerFrame::Hello {
            worker_id,
            capacity,
            attestation,
            ..
        } => {
            WorkerId::parse(worker_id)?;
            if *capacity == 0 {
                return Err(CoreError::Protocol {
                    reason: "capacity must be >= 1".to_string(),
                });
            }
            // Shape only here: an out-of-allowlist digest is a policy decision
            // (auth::AttestationAllowlist), not a protocol error.
            validate_attestation_shape(attestation)?;
            Ok(())
        }
        WorkerFrame::Frame {
            job,
            kind,
            text,
            artifact,
            ..
        } => {
            JobId::parse(job.as_ref())?;
            if text.len() > caps.max_frame_text_bytes {
                return Err(CoreError::TooLarge {
                    what: "frame text",
                    limit: caps.max_frame_text_bytes,
                    actual: text.len(),
                });
            }
            match kind {
                // Artifact frames carry a descriptor, never bulk bytes.
                FrameKind::ArtifactChunk => {
                    artifact.as_ref().ok_or_else(|| CoreError::Protocol {
                        reason: "artifact_chunk requires an artifact manifest".to_string(),
                    })?;
                    Ok(())
                }
                // Delta/event/log frames must carry content.
                _ if text.is_empty() => Err(CoreError::Protocol {
                    reason: format!("{kind:?} frame requires non-empty text"),
                }),
                _ => Ok(()),
            }
        }
        WorkerFrame::Done { job, .. } => JobId::parse(job.as_ref()).map(|_| ()),
        WorkerFrame::Resume { job, .. } | WorkerFrame::CancelAck { job } => {
            JobId::parse(job.as_ref()).map(|_| ())
        }
        WorkerFrame::Ready { .. } | WorkerFrame::Claim { .. } => Ok(()),
    }
}

fn validate_hub_frame(frame: &HubFrame, caps: &WireCaps) -> Result<(), CoreError> {
    match frame {
        HubFrame::Assigned { payload, .. } => match payload {
            JobPayload::Inline { body, .. } => {
                let len = serde_json::to_string(body)?.len();
                if len > caps.max_inline_payload_bytes {
                    return Err(CoreError::TooLarge {
                        what: "inline payload",
                        limit: caps.max_inline_payload_bytes,
                        actual: len,
                    });
                }
                Ok(())
            }
            JobPayload::ByRef { .. } => Ok(()),
        },
        HubFrame::Welcome { worker_id, .. } => {
            WorkerId::parse(worker_id.as_ref())?;
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Semantic validation of untrusted consumer input, the same discipline
/// [`validate_worker_frame`] applies to the supplier face: serde success is not
/// enough, because ids, kinds and payload sizes are policy the actor must not be
/// asked to police after the fact.
fn validate_consumer_frame(frame: &ConsumerFrame, caps: &WireCaps) -> Result<(), CoreError> {
    match frame {
        ConsumerFrame::Submit {
            kind,
            payload,
            idempotency_key,
            ..
        } => {
            if kind.trim().is_empty() {
                return Err(CoreError::Protocol {
                    reason: "submit requires a kind".to_string(),
                });
            }
            // The same cap `Assigned` is held to in the other direction: a payload
            // this large cannot ride a hub frame either, and refusing it here keeps
            // a consumer from discovering it only after the actor has done the work
            // of admitting the job.
            let len = serde_json::to_string(payload)?.len();
            if len > caps.max_inline_payload_bytes {
                return Err(CoreError::TooLarge {
                    what: "submit payload",
                    limit: caps.max_inline_payload_bytes,
                    actual: len,
                });
            }
            // A key that is present but blank would be shared by every such request,
            // which is the opposite of what an idempotency key means.
            if idempotency_key
                .as_deref()
                .is_some_and(|key| key.trim().is_empty() || key.len() > 512)
            {
                return Err(CoreError::Protocol {
                    reason: "idempotency_key must be non-empty and at most 512 chars".to_string(),
                });
            }
            Ok(())
        }
        ConsumerFrame::Attach { job, .. }
        | ConsumerFrame::Detach { job }
        | ConsumerFrame::Cancel { job } => JobId::parse(job.as_ref()).map(|_| ()),
    }
}

/// Sanity check that attested digests are the shape we expect; the *trust*
/// decision is `auth::AttestationAllowlist::verify`.
fn validate_attestation_shape(attestation: &Attestation) -> Result<(), CoreError> {
    for sha in &attestation.model_shas {
        if !crate::util::is_sha256_hex(sha) {
            return Err(CoreError::Attestation(format!(
                "{sha:?} is not 64 hex chars"
            )));
        }
    }
    Ok(())
}

/// Incremental JSON-lines splitter for a byte stream.
///
/// The native runtime feeds it socket chunks; the WASM host feeds it whole
/// strings. Splitting and cap enforcement therefore cannot drift between the
/// two runtimes.
#[derive(Debug, Clone)]
pub struct JsonLines {
    buf: Vec<u8>,
    caps: WireCaps,
    overflow: bool,
}

impl JsonLines {
    /// New splitter using `caps` for the per-line bound.
    #[must_use]
    pub fn new(caps: WireCaps) -> Self {
        Self {
            buf: Vec::new(),
            caps,
            overflow: false,
        }
    }

    /// Append a chunk and return every complete line, in arrival order.
    ///
    /// # Errors
    /// [`CoreError::TooLarge`] when a single line grows past the cap, which the
    /// caller must treat as a fatal protocol violation (the stream position is
    /// no longer knowable).
    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<String>, CoreError> {
        if self.overflow {
            return Err(CoreError::TooLarge {
                what: "line",
                limit: self.caps.max_line_bytes,
                actual: self.buf.len(),
            });
        }
        self.buf.extend_from_slice(chunk);
        if self.buf.len() > self.caps.max_line_bytes {
            self.overflow = true;
            return Err(CoreError::TooLarge {
                what: "line",
                limit: self.caps.max_line_bytes,
                actual: self.buf.len(),
            });
        }
        let mut lines = Vec::new();
        // Split on b'\n' then drop CR so CRLF-terminated hosts also work.
        while let Some(pos) = self.buf.iter().position(|b| *b == b'\n') {
            let raw: Vec<u8> = self.buf.drain(..=pos).collect();
            // Two sequential suffix trims, written as rebinds rather than a nested
            // `match`: `split_last` binds the remainder by reference, so nested arms
            // disagree between `[u8]` and `&[u8]`. Drop the `\n` that was split on
            // first, then the `\r` in front of it when a CRLF host sent one.
            let without_lf: &[u8] = raw
                .split_last()
                .filter(|(last, _)| **last == b'\n')
                .map_or(raw.as_slice(), |(_, rest)| rest);
            let trimmed: &[u8] = without_lf
                .split_last()
                .filter(|(last, _)| **last == b'\r')
                .map_or(without_lf, |(_, rest)| rest);
            if !trimmed.is_empty() {
                // Invalid UTF-8 on a JSON socket is a protocol error, but the
                // lossy conversion keeps the message readable in logs.
                let line =
                    String::from_utf8(trimmed.to_vec()).map_err(|e| CoreError::Protocol {
                        reason: format!("line is not utf-8: {e}"),
                    })?;
                lines.push(line);
            }
        }
        Ok(lines)
    }

    /// Flush a trailing unterminated line at end of stream.
    #[must_use]
    pub fn finish(&mut self) -> Option<String> {
        if self.buf.is_empty() {
            None
        } else {
            let bytes = std::mem::take(&mut self.buf);
            String::from_utf8(bytes).ok()
        }
    }

    /// Bytes currently buffered without a newline.
    #[must_use]
    pub fn pending_bytes(&self) -> usize {
        self.buf.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const SHA_A: &str = "40f369eca301ec6ea318dbb671b3132c26a2f0b89aa5458a7051b33215ee296c";

    #[test]
    fn negotiation_rejects_mismatch_exactly() {
        assert!(negotiate(1).is_ok());
        assert!(
            matches!(
                negotiate(2),
                Err(CoreError::VersionMismatch {
                    requested: 2,
                    supported: 1
                })
            ),
            "a version the hub does not speak must be a clean reject"
        );
    }

    #[test]
    fn hello_round_trips_with_real_reference_digest() {
        // SHA from services/molab/molab-strata-runtime.py:95-96 (SHARD_TABLE).
        let frame = WorkerFrame::Hello {
            worker_id: "wkr-molab-1".to_string(),
            pools: vec!["llm.chat/qwen38".to_string()],
            capacity: 4,
            requires: Requires {
                model: Some("qwen38".to_string()),
                min_context_tokens: Some(524288),
                capabilities: BTreeSet::from(["tools".to_string()]),
                kv_estimate_tokens: None,
            },
            attestation: Attestation {
                model_shas: BTreeSet::from([SHA_A.to_string()]),
                weight_bytes: Some(83863586496),
            },
            protocol: 1,
            engine: Some("strata".to_string()),
            tier: Some(WorkerTier::Ephemeral),
        };
        let caps = WireCaps::default();
        let line = encode_line(&frame, &caps).expect("encode");
        assert!(line.starts_with(r#"{"type":"hello""#), "{line}");
        let back = decode_worker_frame(&line, &caps).expect("decode");
        assert_eq!(back, frame);
    }

    #[test]
    fn artifact_chunk_without_manifest_is_rejected() {
        let line = r#"{"type":"frame","job":"j1","seq":3,"kind":"artifact_chunk","text":""}"#;
        let err = decode_worker_frame(line, &WireCaps::default()).expect_err("must reject");
        assert!(matches!(err, CoreError::Protocol { .. }), "{err}");
    }

    #[test]
    fn delta_with_empty_text_is_rejected() {
        let line = r#"{"type":"frame","job":"j1","seq":1,"kind":"delta","text":""}"#;
        let err = decode_worker_frame(line, &WireCaps::default()).expect_err("must reject");
        assert!(matches!(err, CoreError::Protocol { .. }), "{err}");
    }

    #[test]
    fn oversize_delta_is_capped() {
        let caps = WireCaps {
            max_frame_text_bytes: 8,
            ..WireCaps::default()
        };
        let line =
            r#"{"type":"frame","job":"j1","seq":1,"kind":"delta","text":"0123456789abcdef"}"#;
        let err = decode_worker_frame(line, &caps).expect_err("must reject");
        assert!(
            matches!(
                err,
                CoreError::TooLarge {
                    what: "frame text",
                    limit: 8,
                    actual: 16
                }
            ),
            "{err:?}"
        );
    }

    #[test]
    fn json_lines_splits_and_caps() {
        let mut split = JsonLines::new(WireCaps::default());
        let got = split.push(b"{\"a\":1}\n{\"b\":2}\n{\"c\"").expect("split");
        assert_eq!(got, vec!["{\"a\":1}".to_string(), "{\"b\":2}".to_string()]);
        assert_eq!(split.pending_bytes(), 4);

        // CRLF tolerance and end-of-stream flush.
        let got = split.push(b":3}\r\n").expect("split");
        assert_eq!(got, vec!["{\"c\":3}".to_string()]);
        assert_eq!(split.finish(), None);

        // A line larger than the cap is fatal, and stays fatal.
        let mut tight = JsonLines::new(WireCaps {
            max_line_bytes: 4,
            ..WireCaps::default()
        });
        let err = tight.push(b"123456").expect_err("must reject");
        assert!(
            matches!(
                err,
                CoreError::TooLarge {
                    limit: 4,
                    actual: 6,
                    ..
                }
            ),
            "{err:?}"
        );
        assert!(tight.push(b"x").is_err(), "overflow must be sticky");
    }

    // ------------------------------------------------------- consumer face (NEX-3)

    /// The whole submit frame, as a client writes it.
    #[must_use]
    fn submit_line() -> String {
        r#"{"type":"submit","kind":"image.workflow","payload":{"prompt":"a red cube"},"requires":{"capabilities":["vision"]},"priority":"high","detach":true,"idempotency_key":"k-1"}"#
            .to_string()
    }

    #[test]
    fn consumer_submit_round_trips_through_the_shared_codec() {
        let caps = WireCaps::default();
        let frame = decode_consumer_frame(&submit_line(), &caps).expect("submit decodes");
        assert_eq!(
            frame,
            ConsumerFrame::Submit {
                kind: "image.workflow".to_string(),
                payload: json!({"prompt": "a red cube"}),
                pool: None,
                requires: Some(Requires {
                    model: None,
                    min_context_tokens: None,
                    capabilities: BTreeSet::from(["vision".to_string()]),
                    kv_estimate_tokens: None,
                }),
                priority: Some("high".to_string()),
                // NEX-6.6's opt-in is carried explicitly; nothing here infers it.
                detach: true,
                idempotency_key: Some("k-1".to_string()),
            },
            "{frame:?}"
        );
        // Round-trip is byte-stable for the fields a client wrote.
        let back = encode_line(&frame, &caps).expect("encode");
        assert_eq!(
            decode_consumer_frame(&back, &caps).expect("re-decode"),
            frame
        );
    }

    #[test]
    fn detach_defaults_to_false_because_the_socket_is_the_subscription() {
        // NEX-6.1 is the rule, so an absent `detach` must mean "attached", never
        // "survive the hangup": an omitted field can never widen the exception.
        let line = r#"{"type":"submit","kind":"llm.chat","payload":{"a":1}}"#;
        let frame = decode_consumer_frame(line, &WireCaps::default()).expect("minimal submit");
        match frame {
            ConsumerFrame::Submit {
                detach,
                pool,
                requires,
                priority,
                idempotency_key,
                ..
            } => {
                assert!(!detach, "absent detach means attached");
                assert_eq!(pool, None);
                assert_eq!(requires, None);
                assert_eq!(priority, None);
                assert_eq!(idempotency_key, None);
            }
            other => panic!("expected a submit, got {other:?}"),
        }
    }

    #[test]
    fn the_two_vocabularies_are_disjoint_so_a_consumer_cannot_pose_as_a_supplier() {
        // NEX-2.2: the hub fans one task out to several subscribers, so a frame from
        // one face must never be readable as a frame from the other.
        let caps = WireCaps::default();
        let err =
            decode_worker_frame(&submit_line(), &caps).expect_err("submit is not a worker frame");
        assert!(matches!(err, CoreError::Json { .. }), "{err:?}");
        let consumer_line = r#"{"type":"frame","job":"j1","seq":1,"kind":"delta","text":"hi"}"#;
        let err = decode_consumer_frame(consumer_line, &caps)
            .expect_err("a supplier frame is not a consumer frame");
        assert!(matches!(err, CoreError::Json { .. }), "{err:?}");
    }

    #[test]
    fn consumer_frames_are_capped_and_id_validated() {
        let caps = WireCaps {
            max_inline_payload_bytes: 32,
            ..WireCaps::default()
        };
        let line = r#"{"type":"submit","kind":"llm.chat","payload":{"prompt":"0123456789012345678901234567890123456789"}}"#;
        let err = decode_consumer_frame(line, &caps).expect_err("payload must be capped");
        assert!(
            matches!(
                err,
                CoreError::TooLarge {
                    what: "submit payload",
                    limit: 32,
                    ..
                }
            ),
            "{err:?}"
        );

        // A blank kind and an unparsable job id are refused at the boundary, which is
        // what keeps the actor's mailbox free of garbage.
        let err = decode_consumer_frame(
            r#"{"type":"submit","kind":"  ","payload":{}}"#,
            &WireCaps::default(),
        )
        .expect_err("kind is required");
        assert!(matches!(err, CoreError::Protocol { .. }), "{err:?}");
        let err = decode_consumer_frame(
            r#"{"type":"attach","job":"../../etc/passwd","from_seq":4}"#,
            &WireCaps::default(),
        )
        .expect_err("job id must be traversal-proof");
        assert!(matches!(err, CoreError::InvalidId { .. }), "{err:?}");
        // A well-formed cursor frame still decodes.
        let frame = decode_consumer_frame(r#"{"type":"attach","job":"job-1","from_seq":4}"#, &caps)
            .expect("attach");
        assert_eq!(
            frame,
            ConsumerFrame::Attach {
                job: JobId::new_unchecked("job-1"),
                from_seq: 4,
            }
        );
    }

    #[test]
    fn relayed_consumer_frames_keep_the_consumer_event_fields_plus_the_job_id() {
        // NEX-3.2 (tag every event) and NEX-5.2 (verbatim shapes): the WS dialect is
        // `ConsumerEvent` plus `job`, with no renamed or dropped field.
        let job = JobId::new_unchecked("job-7");
        let caps = WireCaps::default();
        let cases = [
            (
                ConsumerEvent::Delta {
                    seq: 3,
                    text: "{\"partial\":1".to_string(),
                },
                json!({"type": "delta", "job": "job-7", "seq": 3, "text": "{\"partial\":1"}),
            ),
            (
                ConsumerEvent::Finish {
                    seq: 9,
                    finish: FinishKind::ToolCalls,
                    usage: Some(json!({"completion_tokens": 3})),
                },
                json!({"type": "finish", "job": "job-7", "seq": 9, "finish": "tool_calls", "usage": {"completion_tokens": 3}}),
            ),
            (
                ConsumerEvent::Error {
                    code: "worker_died".to_string(),
                    message: "the supplier stopped mid-stream".to_string(),
                    retryable: true,
                    through_seq: Some(8),
                },
                json!({"type": "error", "job": "job-7", "code": "worker_died", "message": "the supplier stopped mid-stream", "retryable": true, "through_seq": 8}),
            ),
            (
                ConsumerEvent::Gap {
                    expected: 4,
                    got: 6,
                },
                json!({"type": "gap", "job": "job-7", "expected": 4, "got": 6}),
            ),
            (
                ConsumerEvent::Replay { from_seq: 2 },
                json!({"type": "replay", "job": "job-7", "from_seq": 2}),
            ),
            (
                ConsumerEvent::Meta {
                    job: "job-7".to_string(),
                    attempt: 1,
                    pool: "llm.chat/qwen38".to_string(),
                    kind: "llm.chat".to_string(),
                },
                json!({"type": "meta", "job": "job-7", "attempt": 1, "pool": "llm.chat/qwen38", "kind": "llm.chat"}),
            ),
            (
                ConsumerEvent::Event {
                    seq: 2,
                    payload: json!({"progress": 0.5}),
                },
                json!({"type": "event", "job": "job-7", "seq": 2, "payload": {"progress": 0.5}}),
            ),
            (
                ConsumerEvent::Log {
                    seq: 1,
                    text: "loading".to_string(),
                },
                json!({"type": "log", "job": "job-7", "seq": 1, "text": "loading"}),
            ),
        ];
        for (event, expected) in cases {
            let name = event.event_name();
            let line = encode_line(&HubConsumerFrame::relayed(&job, event), &caps).expect("encode");
            assert!(line.starts_with(&format!(r#"{{"type":"{name}""#)), "{line}");
            let value: Value = serde_json::from_str(&line).expect("valid json");
            assert_eq!(value, expected, "{line}");
            // And it decodes back into the typed frame the tests want to match on.
            let back = decode_hub_consumer_frame(&line, &caps).expect("decode");
            assert_eq!(
                back,
                HubConsumerFrame::relayed(&job, value_to_event(&value))
            );
        }
    }

    #[test]
    fn verdict_and_end_frames_are_exact_and_retry_after_is_optional() {
        // NEX-3.3: a "not now" refusal carries `retry_after_secs`; a "no" does not,
        // so a client can tell a retry from a dead end without parsing prose.
        let caps = WireCaps::default();
        let job = JobId::new_unchecked("job-7");
        let not_now = encode_line(
            &HubConsumerFrame::Rejected {
                job: job.clone(),
                status: 429,
                code: "queue_full".to_string(),
                message: "pool queue is saturated at 1 jobs".to_string(),
                retry_after_secs: Some(1),
            },
            &caps,
        )
        .expect("encode");
        assert_eq!(
            serde_json::from_str::<Value>(&not_now).expect("json"),
            json!({
                "type": "rejected",
                "job": "job-7",
                "status": 429,
                "code": "queue_full",
                "message": "pool queue is saturated at 1 jobs",
                "retry_after_secs": 1,
            })
        );
        let never = encode_line(
            &HubConsumerFrame::Rejected {
                job: job.clone(),
                status: 400,
                code: "unknown_kind".to_string(),
                message: "kind is not registered".to_string(),
                retry_after_secs: None,
            },
            &caps,
        )
        .expect("encode");
        assert!(
            !never.contains("retry_after_secs"),
            "a refusal must not pretend to be retryable: {never}"
        );

        // NEX-3.4: the end marker names the reason, and the cursor it leaves behind.
        let end = encode_line(
            &HubConsumerFrame::End {
                job: job.clone(),
                reason: EndReason::Lagged,
                through_seq: Some(12),
            },
            &caps,
        )
        .expect("encode");
        assert_eq!(
            serde_json::from_str::<Value>(&end).expect("json"),
            json!({"type": "end", "job": "job-7", "reason": "lagged", "through_seq": 12})
        );

        // A connection-level error has no owning task, and says so by omitting `job`.
        let notice = encode_line(
            &HubConsumerFrame::connection_error(
                "malformed_frame",
                "the message is not a known frame type",
            ),
            &caps,
        )
        .expect("encode");
        assert_eq!(
            serde_json::from_str::<Value>(&notice).expect("json"),
            json!({
                "type": "error",
                "code": "malformed_frame",
                "message": "the message is not a known frame type",
                "retryable": true,
            })
        );

        let accepted = encode_line(
            &HubConsumerFrame::Accepted {
                job: job.clone(),
                kind: "llm.chat".to_string(),
                pool: PoolId::new_unchecked("llm.chat/qwen38"),
                detached: false,
            },
            &caps,
        )
        .expect("encode");
        assert_eq!(
            serde_json::from_str::<Value>(&accepted).expect("json"),
            json!({
                "type": "accepted",
                "job": "job-7",
                "kind": "llm.chat",
                "pool": "llm.chat/qwen38",
                "detached": false,
            })
        );
    }

    /// Reads back the event half of a tagged frame, so the round-trip assertion above
    /// compares against what the wire actually said rather than the input value.
    fn value_to_event(value: &Value) -> ConsumerEvent {
        serde_json::from_value::<ConsumerEvent>(
            // The WS frame is the event plus `job`; `ConsumerEvent` ignores the extra
            // key, except for `Meta`, which carries its own copy.
            value.clone(),
        )
        .unwrap_or_else(|err| panic!("{} is a ConsumerEvent: {err}", value["type"]))
    }

    #[test]
    fn auto_is_the_whichever_model_is_free_wildcard() {
        let auto = Requires {
            model: Some(AUTO_MODEL.to_string()),
            ..Default::default()
        };
        let basalt = Requires {
            model: Some("local/basalt".to_string()),
            ..Default::default()
        };
        let strata = Requires {
            model: Some("local/strata".to_string()),
            ..Default::default()
        };
        assert!(auto.serves_model(&basalt));
        assert!(auto.serves_model(&strata));
        // A concrete model keeps matching only its own name.
        assert!(basalt.serves_model(&basalt));
        assert!(!basalt.serves_model(&strata));
    }

    #[test]
    fn capability_precheck_is_subset_semantics() {
        let job = Requires {
            model: Some("qwen38".to_string()),
            min_context_tokens: Some(200_000),
            capabilities: BTreeSet::from(["tools".to_string(), "vision".to_string()]),
            kv_estimate_tokens: None,
        };
        let warm_4 = Requires {
            model: Some("qwen38".to_string()),
            min_context_tokens: Some(524288),
            capabilities: BTreeSet::from(["tools".to_string()]),
            kv_estimate_tokens: None,
        };
        assert!(
            !job.satisfied_by(&warm_4),
            "vision missing → immediate 400 path"
        );
        let full = Requires {
            capabilities: ["tools", "vision"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            ..warm_4.clone()
        };
        assert!(job.satisfied_by(&full));
        // kv estimate above the offered context is also unsatisfiable.
        let kv = Requires {
            kv_estimate_tokens: Some(600_000),
            ..job.clone()
        };
        assert!(!kv.satisfied_by(&full));
    }
}
