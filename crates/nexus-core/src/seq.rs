//! Per-job sequence accounting and the consumer replay ring (task §4
//! "sequence numbers per job", §7.5 "re-attach and replay").
//!
//! De-duplication and gap detection live here because they are pure functions of
//! the seq stream — keeping them separate from the actor makes the acceptance
//! tests (no duplicate deltas, gap reported rather than silently swallowed)
//! directly assertable.

use std::collections::VecDeque;

use crate::model::BufferedFrame;

/// Outcome of offering a seq to the tracker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeqVerdict {
    /// First time this seq is seen, immediately after the previous one.
    Accepted,
    /// A hole: the worker skipped seqs (lost frames, or a resumed socket that
    /// re-numbered). The frame is relayed, but consumers get a gap event.
    Gap {
        /// Seq the hub expected next.
        expected: u64,
        /// Seq the worker actually sent.
        got: u64,
    },
    /// Already relayed. A reconnecting worker replaying its tail must not cause
    /// duplicate deltas (acceptance test 8), so the hub drops these.
    Duplicate,
}

/// Strictly-increasing seq accounting for one job.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SeqTracker {
    last_accepted: u64,
    total_frames: u64,
    duplicates: u64,
    gaps: u64,
}

impl SeqTracker {
    /// Empty tracker for a fresh job.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Classify and record `seq`. Seqs are 1-based on the wire; 0 is reserved
    /// for "nothing received yet" so `acked_seq: 0` in `Resume` means "from the
    /// beginning".
    pub fn accept(&mut self, seq: u64) -> SeqVerdict {
        self.total_frames += 1;
        if seq == 0 || seq <= self.last_accepted {
            self.duplicates += 1;
            return SeqVerdict::Duplicate;
        }
        let verdict = if seq == self.last_accepted + 1 {
            SeqVerdict::Accepted
        } else {
            self.gaps += 1;
            SeqVerdict::Gap { expected: self.last_accepted + 1, got: seq }
        };
        self.last_accepted = seq;
        verdict
    }

    /// Highest seq accepted so far — exactly what the hub puts in
    /// `HubFrame::Ack`, and what a reconnecting worker echoes in `Resume`.
    #[must_use]
    pub fn acked_through(&self) -> u64 {
        self.last_accepted
    }

    /// True when a `Done{seq}` is consistent with what has been received. A
    /// terminal frame older than the last accepted delta must not end the job.
    #[must_use]
    pub fn done_is_current(&self, seq: u64) -> bool {
        seq >= self.last_accepted
    }

    /// Frames offered, including duplicates (metrics: frames/s).
    #[must_use]
    pub fn total_frames(&self) -> u64 {
        self.total_frames
    }

    /// Duplicates dropped (metrics: dedupe pressure).
    #[must_use]
    pub fn duplicates(&self) -> u64 {
        self.duplicates
    }

    /// Gaps observed (metrics: in-band error precursor).
    #[must_use]
    pub fn gaps(&self) -> u64 {
        self.gaps
    }
}

/// Bounded ring of relayed frames per job, used to replay to a consumer that
/// reconnects with `Last-Event-ID` / `?from_seq=N` (task §7.5).
///
/// Bounded twice — by frame count and by byte count — because one delta and one
/// 64 KiB event are both "one frame". The byte bound is what protects RSS.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplayBuffer {
    frames: VecDeque<BufferedFrame>,
    bytes: usize,
    max_frames: usize,
    max_bytes: usize,
    dropped_frames: u64,
}

impl ReplayBuffer {
    /// Ring sized from the configured limits.
    #[must_use]
    pub fn new(max_frames: usize, max_bytes: usize) -> Self {
        Self {
            frames: VecDeque::new(),
            bytes: 0,
            max_frames: max_frames.max(1),
            max_bytes: max_bytes.max(1024),
            dropped_frames: 0,
        }
    }

    /// Retain a frame, evicting the oldest until both bounds hold. Returns the
    /// frames evicted by this push (normally none).
    pub fn push(&mut self, frame: BufferedFrame) -> Vec<BufferedFrame> {
        let size = frame.size_bytes();
        let mut evicted = Vec::new();
        self.frames.push_back(frame);
        self.bytes += size;
        while self.frames.len() > self.max_frames || (self.bytes > self.max_bytes && self.frames.len() > 1) {
            if let Some(old) = self.frames.pop_front() {
                self.bytes = self.bytes.saturating_sub(old.size_bytes());
                self.dropped_frames += 1;
                evicted.push(old);
            } else {
                break;
            }
        }
        evicted
    }

    /// Frames with `seq > since_seq`, oldest first.
    #[must_use]
    pub fn since(&self, since_seq: u64) -> Vec<&BufferedFrame> {
        self.frames.iter().filter(|f| f.seq > since_seq).collect()
    }

    /// Oldest seq still held. `None` when the ring is empty.
    #[must_use]
    pub fn earliest_seq(&self) -> Option<u64> {
        self.frames.front().map(|f| f.seq)
    }

    /// True when a consumer asks for a seq the ring has already evicted, i.e.
    /// replay cannot be gap-free. Callers must surface that as an in-band error
    /// instead of silently sending a partial stream.
    #[must_use]
    pub fn replay_is_lossy(&self, requested_seq: u64) -> bool {
        match self.earliest_seq() {
            // A request for seq 0 / "everything" is satisfied if we still hold
            // the first frame or the job has produced nothing since eviction.
            Some(earliest) => requested_seq + 1 < earliest,
            None => false,
        }
    }

    /// Frames currently retained.
    #[must_use]
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    /// True when nothing is retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    /// Bytes retained.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Frames evicted because the ring was full (metrics).
    #[must_use]
    pub fn dropped_frames(&self) -> u64 {
        self.dropped_frames
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::FrameKind;

    fn frame(seq: u64, content: &str) -> BufferedFrame {
        BufferedFrame { seq, kind: FrameKind::Delta, content: content.to_string(), artifact: None, received_ms: 0 }
    }

    #[test]
    fn seq_tracker_classifies_accept_gap_and_duplicate() {
        let mut t = SeqTracker::new();
        assert_eq!(t.accept(1), SeqVerdict::Accepted);
        assert_eq!(t.accept(2), SeqVerdict::Accepted);
        assert_eq!(t.accept(5), SeqVerdict::Gap { expected: 3, got: 5 });
        assert_eq!(t.accept(3), SeqVerdict::Duplicate, "an older seq is a duplicate");
        assert_eq!(t.accept(0), SeqVerdict::Duplicate, "seq 0 means 'nothing yet'");
        assert_eq!(t.acked_through(), 5);
        assert_eq!((t.duplicates(), t.gaps(), t.total_frames()), (2, 1, 5));
        assert!(t.done_is_current(5));
        assert!(!t.done_is_current(4), "a stale Done must not end the job");
    }

    #[test]
    fn replay_evicts_on_byte_bound_and_reports_loss() {
        // 3 frames of ~50 bytes each into a 120-byte ring → oldest two evicted.
        let mut ring = ReplayBuffer::new(64, 120);
        ring.push(frame(1, &"x".repeat(60)));
        ring.push(frame(2, &"y".repeat(60)));
        ring.push(frame(3, &"z".repeat(60)));
        assert_eq!(ring.len(), 1);
        assert_eq!(ring.earliest_seq(), Some(3));
        assert_eq!(ring.dropped_frames(), 2);
        assert!(ring.replay_is_lossy(1), "asking for seq 1 can no longer be gap-free");
        assert!(!ring.replay_is_lossy(2), "seq 2 is the boundary: only seq 3 remains, which is contiguous after 2");
        assert_eq!(ring.since(2).iter().map(|f| f.seq).collect::<Vec<_>>(), vec![3]);
    }

    #[test]
    fn replay_respects_frame_bound_and_never_evicts_the_newest() {
        let mut ring = ReplayBuffer::new(3, 1024);
        for seq in 1..=5 {
            ring.push(frame(seq, "a"));
        }
        assert_eq!(ring.len(), 3);
        assert_eq!(ring.since(0).iter().map(|f| f.seq).collect::<Vec<_>>(), vec![3, 4, 5]);
        // Byte bound can never drop the only frame, however large it is.
        let mut tight = ReplayBuffer::new(8, 1024);
        tight.push(frame(1, &"b".repeat(4096)));
        assert_eq!(tight.len(), 1);
        assert_eq!(tight.frames.front().expect("one").seq, 1);
    }
}
