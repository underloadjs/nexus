//! Identifier newtypes + a dependency-free unique id source.
//!
//! Ids cross three trust boundaries (HTTP path segment, WS frame field, file
//! path component), so every constructor funnels through [`is_safe_id`] and the
//! generator only ever emits a traversal-proof alphabet.

use serde::{Deserialize, Serialize};

use crate::errors::CoreError;
use crate::util::is_safe_id;

/// Generates opaque, monotonic, collision-resistant ids with no external deps.
///
/// Layout: `<prefix>-<seed>-<counter>`, all lowercase hex. `seed` comes from the
/// wall clock at process start (single-replica broker, so a per-process unique
/// salt is enough); `counter` is atomic so ids never repeat inside a process.
#[derive(Debug)]
pub struct IdGenerator {
    seed: u64,
    counter: std::sync::atomic::AtomicU64,
}

impl Default for IdGenerator {
    fn default() -> Self {
        // SystemTime is allowed here (and only here): ids are not part of any
        // scheduling decision, so a non-deterministic salt cannot make the
        // actor's behaviour depend on the wall clock.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64);
        Self {
            seed: nanos,
            counter: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

impl IdGenerator {
    /// Next id with the given short prefix (`job`, `wkr`, `ref`, `art`, ...).
    #[must_use]
    pub fn next(&self, prefix: &str) -> String {
        let n = self.counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("{prefix}-{:08x}-{n:x}", (self.seed & 0xffff_ffff) as u32)
    }

    /// Opaque capability token (payload refs, artifact PUT urls): 32 hex chars,
    // 128 bits of (seed,counter) entropy — enough that brute force over a
    // rate-limited loopback endpoint is not a real risk.
    #[must_use]
    pub fn next_token(&self) -> String {
        let a = self.counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let b = a
            .wrapping_mul(0x9e37_79b9_7f4a_7c15)
            .wrapping_add(self.seed);
        format!("{:016x}{:016x}", b, a ^ (b >> 17))
    }
}

/// Declares a string newtype for an opaque identifier.
macro_rules! id_newtype {
    ($name:ident, $doc:expr) => {
        #[doc = $doc]
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl $name {
            #[doc = concat!("Wrap an already-vetted value as a [`", stringify!($name), "`].")]
            #[must_use]
            pub fn new_unchecked(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            #[doc = concat!("Validate then wrap. Rejects charset violations, traversal and over-length values.")]
            pub fn parse(value: &str) -> Result<Self, CoreError> {
                if is_safe_id(value) {
                    Ok(Self(value.to_string()))
                } else {
                    Err(CoreError::InvalidId {
                        kind: stringify!($name),
                        value: value.to_string(),
                    })
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }
    };
}

id_newtype!(JobId, "A submitted unit of work, unique across the hub's lifetime.");
id_newtype!(WorkerId, "Stable worker identity — dedupe key for re-run notebook cells (task §11).");
id_newtype!(PoolId, "A capability pool: kind+requires routing target (task §8).");
id_newtype!(TenantId, "A consumer account, the unit of concurrency caps and rate limits (task §9).");
id_newtype!(ConsumerId, "One attached consumer stream on one job (fan-out / re-attach, task §17.7).");
id_newtype!(PayloadRefId, "Handle for a spilled request payload fetched by reference (task §10).");
id_newtype!(ArtifactId, "Handle for a produced artifact on disk with a TTL (task §7.3).");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_safe_and_rejects_traversal() {
        // Exact expected values, not "an error occurred".
        assert_eq!(
            JobId::parse("job-1a2b").map(|id| id.0),
            Ok("job-1a2b".to_string())
        );
        assert!(JobId::parse("../etc/passwd").is_err());
        assert!(JobId::parse("").is_err());
        assert!(WorkerId::parse("wkr 1").is_err(), "space is not in the alphabet");
        assert!(TenantId::parse(&"a".repeat(65)).is_err(), "65 > 64 limit");
    }

    #[test]
    fn ids_are_unique_and_safe() {
        let gen = IdGenerator::default();
        let a = gen.next("job");
        let b = gen.next("job");
        assert_ne!(a, b);
        assert!(is_safe_id(&a), "{a} must survive path/URL embedding");
        assert_eq!(gen.next_token().len(), 32);
    }
}
