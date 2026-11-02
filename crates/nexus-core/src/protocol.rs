//! Worker-facing wire protocol (task §4): frame vocabulary, version
//! negotiation, size caps and the JSON-lines codec.
//!
//! M0 wire encoding is **JSON lines** — one frame per line, `{"type": ...}`
//! discriminator — because it is debuggable from a notebook cell and the
//! protocol is not stable yet (task §17.1, resolved: JSON until stable, a
//! length-prefixed binary encoding later). The codec below is target-agnostic:
//! the native runtime and the WASM host use the exact same functions.

use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::errors::CoreError;
use crate::ids::{JobId, PoolId, TenantId, WorkerId};
use crate::model::WorkerTier;
use crate::time::Timestamp;
use std::collections::BTreeSet;

/// Protocol version carried in `Hello.protocol`. Kept as a single integer
/// because the only incompatibility we negotiate today is the frame vocabulary.
pub const WIRE_VERSION: u32 = crate::PROTOCOL_VERSION;

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
            max_line_bytes: 1 << 20,              // 1 MiB control line
            max_frame_text_bytes: 256 << 10,      // 256 KiB delta/event body
            max_inline_payload_bytes: 8 << 20,    // 8 MiB inline cap
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

    /// Union used when a worker joins several pools with different offers.
    #[must_use]
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
            Self::Shutdown => 1001, // going away
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
        WorkerFrame::Hello { worker_id, capacity, attestation, .. } => {
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
        WorkerFrame::Frame { job, kind, text, artifact, .. } => {
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
        Self { buf: Vec::new(), caps, overflow: false }
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
            let trimmed = match raw.split_last() {
                Some((b'\n', head)) => match head.split_last() {
                    Some((b'\r', head2)) => *head2,
                    _ => head,
                },
                _ => raw.as_slice(),
            };
            if !trimmed.is_empty() {
                // Invalid UTF-8 on a JSON socket is a protocol error, but the
                // lossy conversion keeps the message readable in logs.
                let line = String::from_utf8(trimmed.to_vec()).map_err(|e| CoreError::Protocol {
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

    const SHA_A: &str = "40f369eca301ec6ea318dbb671b3132c26a2f0b89aa5458a7051b33215ee296c";

    #[test]
    fn negotiation_rejects_mismatch_exactly() {
        assert!(negotiate(1).is_ok());
        assert!(
            matches!(
                negotiate(2),
                Err(CoreError::VersionMismatch { requested: 2, supported: 1 })
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
        let caps = WireCaps { max_frame_text_bytes: 8, ..WireCaps::default() };
        let line = r#"{"type":"frame","job":"j1","seq":1,"kind":"delta","text":"0123456789abcdef"}"#;
        let err = decode_worker_frame(line, &caps).expect_err("must reject");
        assert!(
            matches!(err, CoreError::TooLarge { what: "frame text", limit: 8, actual: 16 }),
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
        let mut tight = JsonLines::new(WireCaps { max_line_bytes: 4, ..WireCaps::default() });
        let err = tight.push(b"123456").expect_err("must reject");
        assert!(matches!(err, CoreError::TooLarge { limit: 4, actual: 6, .. }), "{err:?}");
        assert!(tight.push(b"x").is_err(), "overflow must be sticky");
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
        assert!(!job.satisfied_by(&warm_4), "vision missing → immediate 400 path");
        let full = Requires { capabilities: ["tools", "vision"].into_iter().collect(), ..warm_4.clone() };
        assert!(job.satisfied_by(&full));
        // kv estimate above the offered context is also unsatisfiable.
        let kv = Requires { kv_estimate_tokens: Some(600_000), ..job.clone() };
        assert!(!kv.satisfied_by(&full));
    }
}
