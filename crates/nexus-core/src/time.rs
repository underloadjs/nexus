//! Injected time. The core never reads a wall clock: every mutation receives
//! `now_ms` from the embedder, which makes lease/TTL behaviour deterministic and
//! unit-testable (no sleeps in tests).

/// Milliseconds on a monotonic-ish clock owned by the embedder.
pub type Timestamp = u64;

/// Saturating `now + delta` — a lease deadline must never wrap to the past.
#[must_use]
pub fn add_ms(now_ms: Timestamp, delta_ms: u64) -> Timestamp {
    now_ms.saturating_add(delta_ms)
}

/// `true` when `deadline` has been reached at `now_ms` (inclusive, so a tick
/// scheduled exactly at the deadline fires).
#[must_use]
pub fn expired(now_ms: Timestamp, deadline_ms: Timestamp) -> bool {
    now_ms >= deadline_ms
}

/// Elapsed milliseconds, clamped at zero (defensive against an embedder whose
/// clock jumped backwards).
#[must_use]
pub fn elapsed_ms(now_ms: Timestamp, since_ms: Timestamp) -> u64 {
    now_ms.saturating_sub(since_ms)
}

/// A clock the native runtime implements; kept as a trait (not a generic bound on
/// the actor) so the actor's public API stays object-safe and easy to box.
pub trait ClockSource {
    /// Current monotonic millisecond value.
    fn now_ms(&mut self) -> Timestamp;
}

/// Fixed clock — used by tests and by the WASM host when it drives the actor
/// with explicit virtual time.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConstClock {
    /// The instant this clock reports.
    pub now_ms: Timestamp,
}

impl ConstClock {
    /// Build a clock starting at `now_ms`.
    #[must_use]
    pub fn new(now_ms: Timestamp) -> Self {
        Self { now_ms }
    }
}

impl ClockSource for ConstClock {
    fn now_ms(&mut self) -> Timestamp {
        self.now_ms
    }
}
