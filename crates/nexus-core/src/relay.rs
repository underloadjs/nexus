//! Relay (task §13): the worker frame stream becomes consumer events.
//!
//! This module owns the *neutral* event vocabulary, the SSE framing primitives
//! and the time/size flush policy of task §7.2. Dialect-specific bodies (OpenAI
//! chunks vs generic job events) are produced by [`crate::adapters`] on top of
//! these primitives, which is what keeps "the hub must not parse job payloads"
//! true: only the adapters know what a chunk looks like.
//!
//! # Verbatim relay (acceptance test 14)
//!
//! A `delta` frame's text is carried through untouched — `tool_calls` fragments,
//! `reasoning_content` deltas, `finish_reason`, `usage`. The hub never
//! re-tokenizes, re-parses or "improves" mid-stream; the only transformation
//! allowed is the SSE line splitting mandated by the transport.

use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::protocol::ArtifactManifest;
use crate::protocol::FinishKind;
use crate::time::Timestamp;

/// One event destined for a consumer stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ConsumerEvent {
    /// Hub-issued metadata block: job id, attempt, pool. Emitted once, first.
    Meta {
        /// Job identity (stringified for transport neutrality).
        job: String,
        /// Attempt that produced these bytes.
        attempt: u32,
        /// Pool the job runs in.
        pool: String,
        /// Kind string.
        kind: String,
    },
    /// Verbatim delta text from the worker.
    Delta {
        /// Worker seq.
        seq: u64,
        /// Raw bytes (UTF-8 text) exactly as received.
        text: String,
    },
    /// Structured progress event, passed through as JSON.
    Event {
        /// Worker seq.
        seq: u64,
        /// Parsed-to-verbatim JSON value.
        payload: Value,
    },
    /// Operator log line.
    Log {
        /// Worker seq.
        seq: u64,
        /// Log text.
        text: String,
    },
    /// Artifact availability (bulk bytes travel over HTTP).
    Artifact {
        /// Worker seq.
        seq: u64,
        /// Descriptor with digest and size.
        manifest: ArtifactManifest,
    },
    /// Terminal success: finish reason plus verbatim usage.
    Finish {
        /// Final seq.
        seq: u64,
        /// Finish reason.
        finish: FinishKind,
        /// Verbatim usage object.
        #[serde(default)]
        usage: Option<Value>,
    },
    /// Terminal in-band failure (task §7.2: the status is already spent).
    Error {
        /// Stable code.
        code: String,
        /// Human message.
        message: String,
        /// Whether the client may retry the whole request.
        retryable: bool,
        /// Seq range the hub knows is complete, for replay/rejoin decisions.
        #[serde(default)]
        through_seq: Option<u64>,
    },
    /// A hole in the seq stream: bytes between `expected` and `got` were lost.
    Gap {
        /// First missing seq.
        expected: u64,
        /// Seq that arrived instead.
        got: u64,
    },
    /// Replay marker: frames after this point were served from the ring buffer.
    Replay {
        /// Seq the replay resumed from.
        from_seq: u64,
    },
}

impl ConsumerEvent {
    /// SSE `event:` name for the generic job API.
    #[must_use]
    pub fn event_name(&self) -> &'static str {
        match self {
            Self::Meta { .. } => "meta",
            Self::Delta { .. } => "delta",
            Self::Event { .. } => "event",
            Self::Log { .. } => "log",
            Self::Artifact { .. } => "artifact",
            Self::Finish { .. } => "finish",
            Self::Error { .. } => "error",
            Self::Gap { .. } => "gap",
            Self::Replay { .. } => "replay",
        }
    }

    /// Seq carried by this event, used as the SSE `id:` for replay (§7.5).
    #[must_use]
    pub fn seq(&self) -> Option<u64> {
        match self {
            Self::Delta { seq } | Self::Event { seq } | Self::Log { seq } | Self::Artifact { seq } | Self::Finish { seq } => Some(*seq),
            Self::Error { through_seq, .. } => *through_seq,
            Self::Gap { expected, .. } => Some(*expected - 1),
            Self::Replay { from_seq } => Some(*from_seq),
            Self::Meta { .. } => None,
        }
    }

    /// Bytes this event will cost the replay ring.
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        match self {
            Self::Delta { text, .. } | Self::Log { text, .. } => text.len(),
            Self::Event { payload, .. } => serde_json::to_string(payload).map_or(0, |s| s.len()),
            other => serde_json::to_string(other).map_or(0, |s| s.len()),
        }
    }
}

/// Outbound SSE batching state for one job (task §7.2: "batch outgoing chunks
/// in time, not per token").
///
/// Held per job inside [`crate::model::JobRecord`] so it is part of the actor's
/// single authority rather than a side table.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlushGate {
    /// Bytes accumulated but not yet flushed.
    pub pending_bytes: usize,
    /// Frames accumulated but not yet flushed.
    pub pending_frames: usize,
    /// When the first unflushed frame arrived; `None` when nothing is pending.
    pub oldest_ms: Option<Timestamp>,
    /// Highest seq already handed to a consumer. The next flush emits everything
    /// the ring holds above this number, which is what makes batching safe: no
    /// frame is emitted twice and none is stranded.
    pub last_flushed_seq: u64,
    /// Frames flushed in total (metrics).
    pub flushed_frames: u64,
    /// Batches emitted in total (metrics: syscalls saved).
    pub flushes: u64,
}

impl FlushGate {
    /// Record that `bytes` of content for a frame arrived at `now_ms`.
    pub fn note(&mut self, now_ms: Timestamp, bytes: usize) {
        if self.oldest_ms.is_none() {
            self.oldest_ms = Some(now_ms);
        }
        self.pending_bytes += bytes;
        self.pending_frames += 1;
    }

    /// Whether the accumulated batch must go out now: size threshold first
    /// (throughput), then age threshold (latency floor).
    #[must_use]
    pub fn should_flush(&self, now_ms: Timestamp, interval_ms: u64, byte_threshold: usize) -> bool {
        if self.pending_frames == 0 {
            return false;
        }
        if self.pending_bytes >= byte_threshold {
            return true;
        }
        self.oldest_ms.is_some_and(|first| now_ms.saturating_sub(first) >= interval_ms)
    }

    /// Clear the pending counters after a flush, recording the batch.
    pub fn commit_flush(&mut self) -> (usize, usize) {
        let out = (self.pending_bytes, self.pending_frames);
        self.pending_bytes = 0;
        self.pending_frames = 0;
        self.oldest_ms = None;
        self.flushed_frames += out.1 as u64;
        self.flushes += 1;
        out
    }

    /// Record that everything up to and including `seq` reached consumers, so
    /// the next flush resumes after it.
    pub fn seal_through(&mut self, seq: u64) {
        if seq > self.last_flushed_seq {
            self.last_flushed_seq = seq;
        }
    }

    /// Force the next check to flush (cancel/finish must never hold bytes back).
    pub fn force(&mut self, now_ms: Timestamp) {
        self.oldest_ms = Some(now_ms);
    }
}

/// Encode one SSE block. Multi-line data is split into repeated `data:` lines,
/// as the spec requires, and a missing event name omits the `event:` line so
/// browsers default to `message`.
#[must_use]
pub fn sse(id: Option<u64>, event: Option<&str>, data: &str) -> String {
    let mut out = String::with_capacity(data.len() + 32);
    if let Some(id) = id {
        out.push_str("id: ");
        out.push_str(itoa(id).as_str());
        out.push('\n');
    }
    if let Some(event) = event {
        out.push_str("event: ");
        out.push_str(event);
        out.push('\n');
    }
    for line in data.split('\n') {
        out.push_str("data: ");
        out.push_str(line);
        out.push('\n');
    }
    out.push('\n');
    out
}

/// A comment line, used for the queued-state keepalive (`: ping`) that every
/// mainstream SDK ignores (task §7.2).
#[must_use]
pub fn sse_comment(text: &str) -> String {
    format!(": {text}\n\n")
}

/// Render a consumer event as a generic job-API SSE block.
#[must_use]
pub fn sse_event(event: &ConsumerEvent) -> String {
    let data = serde_json::to_string(event).unwrap_or_else(|_| "{\"type\":\"error\"}".to_string());
    sse(event.seq(), Some(event.event_name()), &data)
}

/// `Last-Event-ID` / `?from_seq=N` parsing. Anything unparsable means "start
/// from the beginning", which is safe: replay from 0 is a superset, and the hub
/// de-duplicates against the seq tracker.
#[must_use]
pub fn parse_resume_seq(value: Option<&str>) -> u64 {
    value.and_then(|v| v.trim().parse::<u64>().ok()).unwrap_or(0)
}

/// Tiny integer formatter (avoids pulling `itoa`/`ryu` into the core).
fn itoa(value: u64) -> String {
    value.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::FrameKind;
    use serde_json::json;

    #[test]
    fn sse_block_framing_is_exact() {
        assert_eq!(
            sse(Some(7), Some("delta"), "{\"a\":1}"),
            "id: 7\nevent: delta\ndata: {\"a\":1}\n\n"
        );
        // No event name → browsers treat it as `message`.
        assert_eq!(sse(None, None, "hello"), "data: hello\n\n");
        // Embedded newlines must each get their own `data:` prefix.
        assert_eq!(sse(None, Some("log"), "one\ntwo\n"), "event: log\ndata: one\ndata: two\ndata: \n\n");
        assert_eq!(sse_comment("ping"), ": ping\n\n");
    }

    #[test]
    fn consumer_event_names_ids_and_sizes_are_exact() {
        let delta = ConsumerEvent::Delta { seq: 3, text: "{\"choices\":[]}".to_string() };
        assert_eq!(delta.event_name(), "delta");
        assert_eq!(delta.seq(), Some(3));
        assert_eq!(delta.size_bytes(), 13);

        let finish = ConsumerEvent::Finish { seq: 9, finish: FinishKind::ToolCalls, usage: Some(json!({"completion_tokens": 4})) };
        assert_eq!(finish.event_name(), "finish");
        assert_eq!(finish.seq(), Some(9));

        let err = ConsumerEvent::Error { code: "worker_gone_partial".into(), message: "x".into(), retryable: false, through_seq: Some(5) };
        assert_eq!(err.seq(), Some(5));

        let meta = ConsumerEvent::Meta { job: "job-1".into(), attempt: 1, pool: "p".into(), kind: "llm.chat".into() };
        assert_eq!(meta.seq(), None);
        assert_eq!(meta.event_name(), "meta");
    }

    #[test]
    fn sse_event_round_trips_through_the_generic_encoder() {
        let event = ConsumerEvent::Event { seq: 4, payload: json!({"progress": 0.5}) };
        let block = sse_event(&event);
        assert!(block.starts_with("id: 4\nevent: event\ndata: "), "{block}");
        // The data line is the serialized event, so a client can parse it back.
        let data = block.trim_end().trim_start_matches("id: 4\nevent: event\ndata: ");
        let back: ConsumerEvent = serde_json::from_str(data).expect("parse back");
        assert_eq!(back, event);
        let _ = FrameKind::Delta;
    }

    #[test]
    fn flush_gate_batches_by_size_then_age() {
        let mut gate = FlushGate::default();
        assert!(!gate.should_flush(0, 75, 1_536), "nothing pending");

        gate.note(100, 512);
        gate.note(110, 512);
        assert!(!gate.should_flush(120, 75, 1_536), "1024 bytes is under the byte threshold and under the age");
        gate.note(120, 512);
        assert!(gate.should_flush(125, 75, 1_536), "1536 bytes hits the byte threshold immediately");
        assert_eq!(gate.commit_flush(), (1_536, 3));
        assert_eq!((gate.flushes, gate.flushed_frames), (1, 3));
        assert!(!gate.should_flush(5_000, 75, 1_536));

        // Age threshold: one small frame waits, then flushes at the interval.
        gate.note(1_000, 8);
        assert!(!gate.should_flush(1_074, 75, 1_536));
        assert!(gate.should_flush(1_075, 75, 1_536));
        // force() makes the next check flush immediately (used by finish/cancel).
        let mut g2 = FlushGate::default();
        g2.note(0, 1);
        assert!(!g2.should_flush(1, 75, 1_536));
        g2.force(1);
        assert!(g2.should_flush(1, 75, 1_536));
    }

    #[test]
    fn resume_seq_parsing_is_permissive_and_exact() {
        assert_eq!(parse_resume_seq(Some("12")), 12);
        assert_eq!(parse_resume_seq(Some(" 13 ")), 13);
        assert_eq!(parse_resume_seq(Some("abc")), 0);
        assert_eq!(parse_resume_seq(None), 0);
        assert_eq!(parse_resume_seq(Some("-1")), 0);
    }
}
