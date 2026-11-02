//! nexus-core — the portable spine of the `rust-hub` inference broker.
//!
//! # What this crate is
//!
//! Every *decision* the broker makes lives here: frame vocabulary, capability
//! pre-check, admission, queueing, the atomic claim, lease accounting,
//! requeue/terminal rules, seq de-duplication, replay, relay flushing, payload
//! spill decisions and artifact TTL bookkeeping. The crate is deliberately
//! **synchronous and IO-free**: `PoolActor::handle(&mut self, msg) -> Vec<Effect>`
//! mutates the one and only copy of scheduling state and *returns* the side
//! effects an embedder must perform (write a frame to a socket, open an SSE
//! body, spill bytes, reject an HTTP request).
//!
//! # Why that shape
//!
//! 1. **Mutual exclusion is structural** (task §5). All job-state transitions
//!    happen inside one `PoolActor` value. There is no shared mutable job state
//!    to race on, so "no two workers process the same job" is a property of the
//!    data model, not of a lock being held correctly.
//! 2. **Determinism is testable.** Claim order is exactly the actor's mailbox
//!    order, and time is injected (`now_ms` on every message). Double-claim,
//!    lease-expiry-with/without-partial-output and flush-batching tests are
//!    therefore exact-value assertions with no wall-clock flake.
//! 3. **The same logic runs in WASM.** tokio's IO and time drivers do not exist
//!    on `wasm32-unknown-unknown`, so anything that embeds tokio is stuck on the
//!    native side. Because this crate needs neither, `nexus-wasm` can re-export
//!    the *real* actor + relay to Node with zero logic duplication. The runtime
//!    boundary is documented in the package README: axum's sockets stay native;
//!    the scheduling/ownership brain is shared.
//!
//! # Module map (task §13 "module boundaries that must exist from M0")
//!
//! - [`protocol`]  frame types + version negotiation + wire caps
//! - [`actor`]     `pool_actor`: queue, claims, leases, dispatch — only mutator
//! - [`queue`]     queue trait + bounded in-memory backend (Redis/PG/NATS slot)
//! - [`relay`]     worker frame stream → consumer events, flush policy, assembly
//! - [`adapters`]  OpenAI-shaped and generic-job consumer surfaces (pure shaping)
//! - [`store`]     payload spill decisions, artifact bookkeeping, TTL, path safety
//! - [`scheduler`] capability/ready/capacity/tier/affinity worker selection
//! - [`auth`]      pool tokens, consumer API keys, model-hash attestation
//! - [`model`] [`seq`] [`config`] [`limits`] [`metrics`] [`ids`] [`errors`]

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![allow(clippy::module_name_repetitions)]

pub mod adapters;
pub mod actor;
pub mod auth;
pub mod config;
pub mod errors;
pub mod ids;
pub mod limits;
pub mod metrics;
pub mod model;
pub mod protocol;
pub mod queue;
pub mod relay;
pub mod scheduler;
pub mod seq;
pub mod store;
pub mod time;

/// Shared deterministic fixtures for the crate's unit tests (cfg(test) only).
#[cfg(test)]
pub mod fixture;

// A small, curated prelude: the vocabulary a host (native runtime, WASM module,
// or an integration test) needs without importing every internal path.
pub use actor::ActorMsg;
pub use actor::Effect;
pub use actor::HelloInput;
pub use actor::LogLevel;
pub use actor::PoolActor;
pub use adapters::ChatRequest;
pub use adapters::KIND_CHAT;
pub use adapters::KIND_COMPLETION;
pub use config::BrokerConfig;
pub use config::KindConfig;
pub use config::PoolConfig;
pub use config::TenantConfig;
pub use errors::CoreError;
pub use ids::ArtifactId;
pub use ids::ConsumerId;
pub use ids::JobId;
pub use ids::PayloadRefId;
pub use ids::PoolId;
pub use ids::TenantId;
pub use ids::WorkerId;
pub use limits::Limits;
pub use metrics::Metrics;
pub use model::EngineState;
pub use model::JobPhase;
pub use model::JobRecord;
pub use model::JobView;
pub use model::Lease;
pub use model::LinkState;
pub use model::Priority;
pub use model::Submission;
pub use model::TerminalReason;
pub use model::WorkerRecord;
pub use model::WorkerTier;
pub use protocol::HubFrame;
pub use protocol::WorkerFrame;
pub use queue::InMemoryQueue;
pub use queue::QueueBackend;
pub use relay::ConsumerEvent;
pub use store::ArtifactBook;
pub use store::PayloadBook;
pub use time::ClockSource;
pub use time::Timestamp;

/// Version of the on-the-wire worker protocol this build speaks (task §4
/// "protocol version negotiation"). M0/M3 use a single integer: JSON lines,
/// one object per line, `{"type": "..."}` discriminator. A length-prefixed
/// binary encoding is a later optimization, so a `major` bump is the only
/// negotiation axis we expose today (task §17.1 resolution).
pub const PROTOCOL_VERSION: u32 = 1;

/// crate-internal shared small helpers kept out of the public API.
pub(crate) mod util {
    /// Lowercase hex encoding, hand-rolled so the core needs no `hex` crate
    /// (the wasm build must stay dependency-light).
    pub fn to_hex(bytes: &[u8]) -> String {
        const TABLE: &[u8; 16] = b"0123456789abcdef";
        let mut out: Vec<u8> = Vec::with_capacity(bytes.len() * 2);
        for b in bytes {
            out.push(TABLE[usize::from(b >> 4)]);
            out.push(TABLE[usize::from(b & 0x0f)]);
        }
        // The table above is ASCII by construction, so a UTF-8 failure here is a
        // bug in this function, not a runtime condition.
        String::from_utf8(out).expect("hex table is ascii")
    }

    /// FNV-1a 64-bit. Used for rendezvous (highest-random-score) hashing in
    /// [`crate::scheduler`] and for content-hash idempotency fallbacks. It is
    /// deterministic across builds, which is what session affinity needs.
    pub fn fnv1a(bytes: &[u8]) -> u64 {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for b in bytes {
            hash ^= u64::from(*b);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash
    }

    /// Case-insensitive, non-allocating hex validation for SHA-256 digests
    /// coming from workers (untrusted input — task §9).
    pub fn is_sha256_hex(value: &str) -> bool {
        value.len() == 64
            && value
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
    }

    /// Worker-supplied identifiers end up in filesystem paths and URLs, so they
    /// are restricted to a traversal-proof alphabet instead of "any string".
    pub fn is_safe_id(value: &str) -> bool {
        !value.is_empty()
            && value.len() <= 64
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
            // A leading '.' or a value that is only dots ("..") could still
            // escape a join; reject any run of dots at the edges.
            && !value.starts_with('.')
            && !value.ends_with('.')
    }

    /// Length-checked, byte-wise equality without early exit on the content
    /// bytes. Used for credential and token digests so a remote peer cannot
    /// probe matching prefixes with response timing.
    #[must_use]
    pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        let mut acc: u8 = 0;
        for (x, y) in a.iter().zip(b.iter()) {
            acc |= x ^ y;
        }
        acc == 0
    }

    /// Percent-decode-free, ASCII-only lowercase of a header value, so
    /// comparisons against digests/hex allowlists are not case sensitive.
    #[must_use]
    pub fn normalize_hex(value: &str) -> String {
        value.trim().chars().map(|c| c.to_ascii_lowercase()).collect()
    }
}
