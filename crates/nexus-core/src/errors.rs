//! Boundary error type for the core. `thiserror` because the native runtime maps
//! these onto HTTP statuses / WS close codes at exactly one place.

use crate::model::JobId;

/// Every way a core operation can refuse to do something.
#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    /// Wire bytes could not be decoded as a frame of the expected direction.
    #[error("protocol: bad frame: {reason}")]
    Protocol {
        /// Human-readable decode failure (never echoed to an untrusted peer verbatim beyond logging).
        reason: String,
    },

    /// A decoded frame or line exceeded a configured cap.
    #[error("protocol: {what} of {actual} bytes exceeds the {limit} byte cap")]
    TooLarge {
        /// Which quantity was capped (line, text field, payload...).
        what: &'static str,
        /// Cap that was exceeded.
        limit: usize,
        /// Observed size.
        actual: usize,
    },

    /// A worker spoke a protocol version this hub does not implement.
    #[error("protocol: worker requested protocol {requested}, hub supports {supported}")]
    VersionMismatch {
        /// Version the worker asked for.
        requested: u32,
        /// Version the hub speaks.
        supported: u32,
    },

    /// A model digest was not on the configured allowlist (task §9).
    #[error("attestation rejected: {0}")]
    Attestation(String),

    /// Credentials were missing or unknown. There is no anonymous default.
    #[error("unauthorized: {0}")]
    Unauthorized(String),

    /// A caller supplied an id outside the traversal-proof alphabet.
    #[error("invalid {kind}: {value:?}")]
    InvalidId {
        /// Newtype that was rejected.
        kind: &'static str,
        /// Offending value.
        value: String,
    },

    /// The kind is not in the configured registry, so its idempotency/streaming
    /// semantics are unknown and dispatching it would be a guess.
    #[error("unknown kind: {0}")]
    UnknownKind(String),

    /// Queue backend refused the job (depth bound).
    #[error("queue: {0}")]
    Queue(String),

    /// The referenced job is not present in this pool (already evicted by TTL,
    /// or the id belongs to another pool).
    #[error("no such job: {0}")]
    UnknownJob(JobId),

    /// A serde failure while (de)serializing an opaque payload.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

impl CoreError {
    /// Machine-readable code the HTTP/WS layers put in error bodies, so a client
    /// can branch without string matching on prose.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Protocol { .. } => "bad_frame",
            Self::TooLarge { .. } => "frame_too_large",
            Self::VersionMismatch { .. } => "protocol_mismatch",
            Self::Attestation(_) => "attestation_rejected",
            Self::Unauthorized(_) => "unauthorized",
            Self::InvalidId { .. } => "invalid_id",
            Self::UnknownKind(_) => "unknown_kind",
            Self::Queue(_) => "queue_full",
            Self::UnknownJob(_) => "unknown_job",
            Self::Json(_) => "bad_json",
        }
    }
}
