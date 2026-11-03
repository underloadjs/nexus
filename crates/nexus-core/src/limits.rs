//! Every bound the broker enforces, in one place, with the task's numbers as
//! defaults. "Explicit limits/TTLs/deadlines" is a requirement (task §4, §8,
//! §10, §17.3): a broker with unbounded queues, leases or buffers is just a
//! slower way to run out of memory.

use crate::protocol::WireCaps;
// Derives below are serde ones: the import is what makes `#[derive(Serialize,
// Deserialize)]` resolve (it is not in the std prelude).
use serde::Deserialize;
use serde::Serialize;

/// All numeric policy for one pool's actor plus the shared wire/store bounds.
///
/// `#[serde(default)]` means a config file may override *only* the numbers an
/// operator cares about; `deny_unknown_fields` means a typo in a limit name is a
/// startup error instead of a silently retained default.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    // ---- wire / framing (task §9, §10) ----
    /// Max bytes of one JSON line on a worker socket.
    pub worker_line_cap_bytes: usize,
    /// Max bytes of one `Frame.text` body.
    pub worker_frame_text_cap_bytes: usize,
    /// Inline payload cap; above it the payload travels by `payload_ref`.
    pub inline_payload_cap_bytes: usize,
    /// Largest `jobs` a worker may ask for in one `Claim`.
    pub max_claim_batch: u32,
    /// Admission control on worker fan-in; a runaway host cannot DoS the actor.
    pub max_workers_per_pool: usize,

    // ---- admission (task §8, §17.3) ----
    /// Hard queue depth. Beyond it: `429`, never an unbounded wait.
    pub queue_depth: usize,
    /// Concurrent non-terminal jobs per tenant across all its pools.
    pub tenant_max_concurrent: usize,
    /// Prompt body cap per submission, the cheap KV-mitigation of task §8.
    pub tenant_max_payload_bytes: u64,
    /// Prompt token estimate cap per submission.
    pub tenant_max_prompt_tokens: u64,
    /// LRU-ish bound on remembered idempotency keys.
    pub max_idempotency_entries: usize,
    /// How long an idempotency key keeps attaching to its job.
    pub idempotency_ttl_ms: u64,

    // ---- scheduling, leases, retries (task §5.1, §5.3, §17.5) ----
    /// Expected gap between `Ready` heartbeats (reference: 15–30 s).
    pub heartbeat_ms: u64,
    /// Lease lifetime granted per dispatch; renewed by heartbeats.
    pub lease_ms: u64,
    /// Window after socket close in which a `Resume` may re-take a lease.
    /// `0` reproduces the strict "expire immediately" reading of §5.1.
    pub resume_grace_ms: u64,
    /// Pre-body grace for "no ready worker" → then `503 + Retry-After`.
    pub no_worker_grace_ms: u64,
    /// Max time a job may sit undispatched (Pending or Queued) → `503`.
    pub claim_deadline_ms: u64,
    /// Hard per-job cap from dispatch to terminal.
    pub job_hard_cap_ms: u64,
    /// How long a worker may take to answer `Cancel` before its slot is freed.
    pub cancel_ack_ms: u64,
    /// Max attempts (1-based) for an idempotent kind before failing.
    pub max_attempts: u32,
    /// Max resume-by-continuation re-dispatches per job (task §7.4). `0` — the
    /// default — leaves a mid-stream death terminal; `1` re-runs `prompt +
    /// partial text` on another worker, which costs a prefill and may show the
    /// consumer a seam.
    pub max_continuations: u32,
    /// Whether a continuation may be attempted when the relayed output was **not**
    /// reconstructible assistant text — tool-call chunks, or a stream interleaved
    /// with `event`/`log`/`artifact_chunk` frames. Default `false`: with
    /// continuation enabled, an opaque attempt still ends in band rather than being
    /// re-run on a half-written tool argument (task §7.4). Only set this on a pool
    /// whose workers restart cleanly from arbitrary prefixes.
    pub continuation_allows_opaque_output: bool,

    // ---- relay (task §7.2, §7.5, §8) ----
    /// Flush an outbound SSE batch after this long even if under-sized.
    pub flush_interval_ms: u64,
    /// Flush an outbound SSE batch once this many bytes accumulate.
    pub flush_bytes: usize,
    /// Replay ring: frames retained per job.
    pub replay_frames: usize,
    /// Replay ring: bytes retained per job (the RSS bound).
    pub replay_bytes: usize,
    /// Cap on retained partial text for continuation (memory-only, task §9).
    pub partial_text_cap_bytes: usize,
    /// Per-job consumer backlog before the worker is back-pressured.
    pub consumer_backpressure_frames: usize,

    // ---- store (task §7.3, §9, §10) ----
    /// TTL for a spilled request payload. Prompts are memory-only: the TTL *is*
    /// the retention policy, and nothing here ever writes prompt text to disk.
    pub payload_ttl_ms: u64,
    /// Max concurrently spilled payloads.
    pub max_spilled_payloads: usize,
    /// Total spilled-bytes budget; over it, admission fails with `429`.
    pub max_spill_bytes: u64,
    /// TTL for produced artifacts on disk.
    pub artifact_ttl_ms: u64,
    /// Max artifact size the hub will accept.
    pub max_artifact_bytes: u64,
    /// Max bytes in one artifact `PUT` chunk.
    pub artifact_chunk_bytes: usize,
    /// How long a terminal job record (and its replay ring) stays queryable.
    pub job_retention_ms: u64,
}

impl Default for Limits {
    fn default() -> Self {
        // Reference anchors: services/molab/molab-strata-runtime.py:111-120
        // (PARALLEL=4, CONTEXT=524288, loopback :8090) and task §10 (~8 MB
        // inline cap), §7.2 (50–100 ms / 1–2 KiB flush), §4 (15–30 s heartbeat).
        Self {
            worker_line_cap_bytes: 1 << 20,
            worker_frame_text_cap_bytes: 256 << 10,
            inline_payload_cap_bytes: 8 << 20,
            max_claim_batch: 8,
            max_workers_per_pool: 32,

            queue_depth: 128,
            tenant_max_concurrent: 4,
            tenant_max_payload_bytes: 32 << 20,
            tenant_max_prompt_tokens: 524_288,
            max_idempotency_entries: 4_096,
            idempotency_ttl_ms: 600_000,

            heartbeat_ms: 20_000,
            lease_ms: 60_000,
            resume_grace_ms: 1_500,
            no_worker_grace_ms: 2_000,
            claim_deadline_ms: 60_000,
            job_hard_cap_ms: 1_800_000,
            cancel_ack_ms: 10_000,
            max_attempts: 3,
            // Resume-by-continuation (task §7.4) is **opt-in**: a mid-stream
            // death is terminal unless the operator asks for a re-run. It costs
            // a full prefill on the second worker (quota on a free-tier host),
            // the hub cannot tell whether that worker's prompt cache is cold as
            // §7.4 asks it to gate on, and the consumer may see a seam. Set
            // `NEXUS_MAX_CONTINUATIONS=1` to turn exactly one attempt on.
            max_continuations: 0,
            // Tool-call chunks are the exact case §7.4 warns about: the hub cannot
            // turn `{"function":{"arguments":"{\"ci"` into a prompt prefix, so an
            // opaque attempt stays terminal even when continuation is on.
            continuation_allows_opaque_output: false,

            flush_interval_ms: 75,
            flush_bytes: 1_536,
            replay_frames: 256,
            replay_bytes: 1 << 20,
            partial_text_cap_bytes: 1 << 20,
            consumer_backpressure_frames: 256,

            payload_ttl_ms: 120_000,
            max_spilled_payloads: 64,
            max_spill_bytes: 512 << 20,
            artifact_ttl_ms: 600_000,
            max_artifact_bytes: 512 << 20,
            artifact_chunk_bytes: 4 << 20,
            job_retention_ms: 300_000,
        }
    }
}

impl Limits {
    /// Wire caps for the worker→hub direction.
    #[must_use]
    pub fn worker_caps(&self) -> WireCaps {
        WireCaps {
            max_line_bytes: self.worker_line_cap_bytes,
            max_frame_text_bytes: self.worker_frame_text_cap_bytes,
            max_inline_payload_bytes: self.inline_payload_cap_bytes,
        }
    }

    /// Wire caps for the hub→worker direction: the line cap must accommodate a
    /// full inline payload, otherwise `Assigned` could never carry one.
    #[must_use]
    pub fn hub_caps(&self) -> WireCaps {
        WireCaps {
            max_line_bytes: self.inline_payload_cap_bytes + (self.worker_line_cap_bytes / 2),
            max_frame_text_bytes: self.worker_frame_text_cap_bytes,
            max_inline_payload_bytes: self.inline_payload_cap_bytes,
        }
    }

    /// Reject nonsense combinations before they can misbehave at runtime.
    ///
    /// Returns the list of violations so an operator fixing a config sees every
    /// problem at once, not one per restart.
    #[must_use]
    pub fn validate(&self) -> Vec<String> {
        let mut problems = Vec::new();
        if self.lease_ms < self.heartbeat_ms * 2 {
            problems.push(format!(
                "lease_ms ({}) must be at least 2x heartbeat_ms ({}) or leases expire between heartbeats",
                self.lease_ms, self.heartbeat_ms
            ));
        }
        if self.flush_bytes == 0 {
            problems.push("flush_bytes must be > 0".to_string());
        }
        if self.flush_interval_ms == 0 {
            problems.push("flush_interval_ms must be > 0".to_string());
        }
        if self.replay_frames == 0 || self.replay_bytes < 1024 {
            problems.push("replay ring must hold at least 1 frame and 1 KiB".to_string());
        }
        if self.inline_payload_cap_bytes > self.worker_line_cap_bytes * 32 {
            problems.push(format!(
                "inline_payload_cap_bytes ({}) is absurdly large next to worker_line_cap_bytes ({}); use payload_ref or raise the line cap deliberately",
                self.inline_payload_cap_bytes, self.worker_line_cap_bytes
            ));
        }
        if self.max_attempts == 0 {
            problems.push("max_attempts must be >= 1".to_string());
        }
        if self.queue_depth == 0 {
            problems
                .push("queue_depth must be > 0 (a 0-depth pool can never accept work)".to_string());
        }
        if self.resume_grace_ms > self.lease_ms {
            problems.push("resume_grace_ms must not exceed lease_ms".to_string());
        }
        if self.claim_deadline_ms <= self.no_worker_grace_ms {
            problems.push("claim_deadline_ms must exceed no_worker_grace_ms".to_string());
        }
        problems
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_task_numbers() {
        let l = Limits::default();
        assert_eq!(l.inline_payload_cap_bytes, 8 << 20);
        assert_eq!(l.queue_depth, 128);
        assert_eq!(l.tenant_max_prompt_tokens, 524_288);
        assert_eq!((l.heartbeat_ms, l.lease_ms), (20_000, 60_000));
        assert_eq!((l.flush_interval_ms, l.flush_bytes), (75, 1_536));
        assert_eq!(
            l.max_continuations, 0,
            "resume-by-continuation is opt-in (§7.4)"
        );
        assert!(
            !l.continuation_allows_opaque_output,
            "opaque (tool-call / non-delta) output is never auto-continued by default (§7.4)"
        );
    }

    #[test]
    fn validate_catches_the_lease_heartbeat_trap() {
        let mut l = Limits::default();
        l.heartbeat_ms = 30_000;
        l.lease_ms = 45_000; // < 2x heartbeat
        assert_eq!(
            l.validate(),
            vec![format!(
                "lease_ms (45000) must be at least 2x heartbeat_ms (30000) or leases expire between heartbeats"
            )]
        );
        assert!(Limits::default().validate().is_empty());
    }

    #[test]
    fn hub_line_cap_fits_a_full_inline_assigned() {
        let l = Limits::default();
        assert!(l.hub_caps().max_line_bytes > l.inline_payload_cap_bytes);
        assert_eq!(l.worker_caps().max_frame_text_bytes, 256 << 10);
    }
}
