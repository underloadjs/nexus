//! Identity and trust (task §9): distinct credentials per direction, model
//! attestation against an allowlist, and no fail-open defaults.
//!
//! Secrets are compared by **digest**, never in the clear: registries hold only
//! SHA-256 hex, so a crash dump, a metrics debug endpoint or a log line cannot
//! leak a usable credential.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::config::AuthConfig;
use crate::errors::CoreError;
use crate::ids::TenantId;
use crate::store::sha256_hex;
use crate::util::constant_time_eq;
use crate::util::is_sha256_hex;
use crate::util::normalize_hex;

/// Tenant granted when anonymous mode is explicitly enabled.
pub const ANONYMOUS_TENANT: &str = "anonymous";

/// Strip an `Authorization` scheme prefix, case-insensitively.
#[must_use]
pub fn bearer(header: Option<&str>) -> Option<&str> {
    let value = header?.trim();
    if value.is_empty() {
        return None;
    }
    let (scheme, rest) = value
        .split_once(' ')
        .map_or((None, value), |(s, r)| (Some(s), r));
    match scheme {
        // `Bearer` is the documented scheme; a bare value is accepted only
        // because some notebook clients cannot set a scheme.
        None | Some("bearer") | Some("Bearer") => Some(rest.trim()),
        Some(_) => None,
    }
}

/// Registry of accepted pool tokens (worker→hub direction).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PoolTokenRegistry {
    digests: BTreeMap<String, String>,
    allow_anonymous: bool,
}

impl PoolTokenRegistry {
    /// Build from config, hashing every token immediately.
    #[must_use]
    pub fn from_config(config: &AuthConfig) -> Self {
        Self::new(
            config.worker_tokens.iter().map(String::as_str),
            config.allow_anonymous,
        )
    }

    /// Build from `(token, label)` pairs.
    pub fn new<'a>(tokens: impl Iterator<Item = &'a str>, allow_anonymous: bool) -> Self {
        let mut digests = BTreeMap::new();
        for (index, token) in tokens.enumerate() {
            let token = token.trim();
            if token.is_empty() {
                continue;
            }
            digests.insert(sha256_hex(token.as_bytes()), format!("pool-token-{index}"));
        }
        Self {
            digests,
            allow_anonymous,
        }
    }

    /// Accepted token count, for a startup log line (never logs the secrets).
    #[must_use]
    pub fn len(&self) -> usize {
        self.digests.len()
    }

    /// True when no token is configured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.digests.is_empty()
    }

    /// Resolve a candidate token to its label.
    ///
    /// A missing token is accepted **only** in explicit anonymous mode; the
    /// default denies, which is the "no fail-open auth" requirement.
    ///
    /// # Errors
    /// [`CoreError::Unauthorized`] when absent-but-required or unknown.
    pub fn verify(&self, token: Option<&str>) -> Result<String, CoreError> {
        let Some(token) = token.map(str::trim) else {
            return if self.allow_anonymous {
                Ok("anonymous-token".to_string())
            } else {
                Err(CoreError::Unauthorized(
                    "missing pool token (Authorization: Bearer <pool token>)".to_string(),
                ))
            };
        };
        if token.is_empty() {
            return Err(CoreError::Unauthorized("empty pool token".to_string()));
        }
        let digest = sha256_hex(token.as_bytes());
        for (candidate, label) in &self.digests {
            if constant_time_eq(candidate.as_bytes(), digest.as_bytes()) {
                return Ok(label.clone());
            }
        }
        Err(CoreError::Unauthorized("unknown pool token".to_string()))
    }
}

/// Registry of consumer API keys (consumer→hub direction), mapped to tenants.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApiKeyRegistry {
    digests: BTreeMap<String, TenantId>,
    allow_anonymous: bool,
}

impl ApiKeyRegistry {
    /// Build from config.
    #[must_use]
    pub fn from_config(config: &AuthConfig) -> Self {
        let mut digests = BTreeMap::new();
        for entry in &config.api_keys {
            let key = entry.key.trim();
            if key.is_empty() {
                continue;
            }
            digests.insert(
                sha256_hex(key.as_bytes()),
                TenantId::new_unchecked(entry.tenant.clone()),
            );
        }
        Self {
            digests,
            allow_anonymous: config.allow_anonymous,
        }
    }

    /// Accepted key count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.digests.len()
    }

    /// True when no key is configured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.digests.is_empty()
    }

    /// Resolve a candidate key to its tenant.
    ///
    /// # Errors
    /// [`CoreError::Unauthorized`] when absent-but-required or unknown.
    pub fn verify(&self, key: Option<&str>) -> Result<TenantId, CoreError> {
        let Some(key) = key.map(str::trim) else {
            return if self.allow_anonymous {
                Ok(TenantId::new_unchecked(ANONYMOUS_TENANT))
            } else {
                Err(CoreError::Unauthorized(
                    "missing api key (Authorization: Bearer <api key>)".to_string(),
                ))
            };
        };
        if key.is_empty() {
            return Err(CoreError::Unauthorized("empty api key".to_string()));
        }
        let digest = sha256_hex(key.as_bytes());
        for (candidate, tenant) in &self.digests {
            if constant_time_eq(candidate.as_bytes(), digest.as_bytes()) {
                return Ok(tenant.clone());
            }
        }
        Err(CoreError::Unauthorized("unknown api key".to_string()))
    }
}

/// Allowlist of model digests a worker may attest (task §9). The reference
/// runtime's `SHARD_TABLE` (services/molab/molab-strata-runtime.py:94-99) is the
/// intended source, so the hub reuses an identifier that already anchors the
/// worker's own download instead of inventing a second one.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AttestationAllowlist {
    shas: BTreeSet<String>,
    required: bool,
    /// Config entries that were not 64 hex chars, i.e. silently unusable.
    rejected_config: usize,
}

impl AttestationAllowlist {
    /// Build from config.
    #[must_use]
    pub fn from_config(config: &AuthConfig) -> Self {
        Self::new(
            config.model_shas.iter().map(String::as_str),
            config.attestation_required(),
        )
    }

    /// Build from raw digests; entries that are not 64 hex chars are dropped
    /// with a count available via [`Self::rejected_config_entries`].
    pub fn new<'a>(shas: impl Iterator<Item = &'a str>, required: bool) -> Self {
        let mut allow = BTreeSet::new();
        let mut bad = 0usize;
        for sha in shas {
            let normalized = normalize_hex(sha.trim());
            if is_sha256_hex(&normalized) {
                allow.insert(normalized);
            } else {
                bad += 1;
            }
        }
        Self {
            shas: allow,
            required,
            rejected_config: bad,
        }
    }

    /// How many config entries were malformed (a silent drop here would let an
    /// operator believe a sha was allowlisted when it was not).
    #[must_use]
    pub fn rejected_config_entries(&self) -> usize {
        self.rejected_config
    }

    /// Allowlisted digest count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.shas.len()
    }

    /// True when the allowlist is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.shas.is_empty()
    }

    /// Size of the allowlist, i.e. how many shards are trusted.
    #[must_use]
    pub fn is_required(&self) -> bool {
        self.required
    }

    /// Decide whether a worker may join the pool.
    ///
    /// Every attested digest must be allowlisted (a worker shipping one extra
    /// untrusted shard is not trusted for the rest), and at least one digest is
    /// required when attestation is on.
    ///
    /// # Errors
    /// [`CoreError::Attestation`] on an empty or non-allowlisted set.
    pub fn verify(&self, attestation: &crate::protocol::Attestation) -> Result<(), CoreError> {
        if attestation.model_shas.is_empty() {
            return if self.required {
                Err(CoreError::Attestation(
                    "worker attested no model digest".to_string(),
                ))
            } else {
                Ok(())
            };
        }
        let mut matched = 0usize;
        for sha in &attestation.model_shas {
            let normalized = normalize_hex(sha);
            if !is_sha256_hex(&normalized) {
                return Err(CoreError::Attestation(format!(
                    "{sha:?} is not 64 hex chars"
                )));
            }
            if !self.shas.contains(&normalized) {
                return Err(CoreError::Attestation(format!(
                    "{normalized} is not on the model allowlist"
                )));
            }
            matched += 1;
        }
        if matched == 0 && self.required {
            return Err(CoreError::Attestation(
                "no attested digest is allowlisted".to_string(),
            ));
        }
        Ok(())
    }

    /// Whether a digest is on the list, for `/v1/models` labelling.
    #[must_use]
    pub fn contains(&self, sha: &str) -> bool {
        self.shas.contains(&normalize_hex(sha))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Attestation;
    use std::collections::BTreeSet;

    /// Reference digests, copied from SHARD_TABLE (molab-strata-runtime.py:95-98).
    const SHARD_1: &str = "40f369eca301ec6ea318dbb671b3132c26a2f0b89aa5458a7051b33215ee296c";
    const SHARD_2: &str = "316b46f3a2dbd68c900f43136ab9449f9dcc3725dfd8c794847c204bc161e113";

    #[test]
    fn bearer_strips_only_supported_schemes() {
        assert_eq!(bearer(Some("Bearer tok-1")), Some("tok-1"));
        assert_eq!(bearer(Some("bearer tok-1")), Some("tok-1"));
        assert_eq!(bearer(Some("tok-1")), Some("tok-1"));
        assert_eq!(bearer(Some("Basic Zm9v")), None);
        assert_eq!(bearer(Some("   ")), None);
        assert_eq!(bearer(None), None);
    }

    #[test]
    fn pool_tokens_map_to_labels_and_reject_unknown() {
        // Struct-update rather than `default()` then reassignment: the field list
        // is what the test is about, and `field_reassign_with_default` is a lint
        // the workspace runs with `-D warnings`.
        let auth = AuthConfig {
            worker_tokens: vec!["tok-a".to_string(), "tok-b".to_string()],
            ..AuthConfig::default()
        };
        let reg = PoolTokenRegistry::from_config(&auth);
        assert_eq!(reg.len(), 2);
        assert_eq!(reg.verify(Some("tok-a")).expect("ok"), "pool-token-0");
        assert_eq!(reg.verify(Some("tok-b")).expect("ok"), "pool-token-1");
        assert!(matches!(
            reg.verify(Some("nope")),
            Err(CoreError::Unauthorized(_))
        ));
        // Fail closed: no header at all is a denial unless configured otherwise.
        assert!(matches!(reg.verify(None), Err(CoreError::Unauthorized(_))));

        let mut anon = auth.clone();
        anon.allow_anonymous = true;
        assert_eq!(
            PoolTokenRegistry::from_config(&anon)
                .verify(None)
                .expect("anonymous mode accepts a missing token"),
            "anonymous-token"
        );
    }

    #[test]
    fn api_keys_resolve_to_their_tenants_exactly() {
        use crate::config::ApiKeyConfig;
        // Struct-update, not `default()` plus a reassignment (see the pool-token
        // test above for why the lint exists).
        let auth = AuthConfig {
            api_keys: vec![
                ApiKeyConfig {
                    key: "sk-1".to_string(),
                    tenant: "acme".to_string(),
                },
                ApiKeyConfig {
                    key: "sk-2".to_string(),
                    tenant: "default".to_string(),
                },
            ],
            ..AuthConfig::default()
        };
        let reg = ApiKeyRegistry::from_config(&auth);
        assert_eq!(
            reg.verify(Some("sk-1")).expect("ok"),
            TenantId::new_unchecked("acme")
        );
        assert_eq!(
            reg.verify(Some("sk-2")).expect("ok"),
            TenantId::new_unchecked("default")
        );
        assert!(matches!(
            reg.verify(Some("sk-3")),
            Err(CoreError::Unauthorized(_))
        ));
        assert!(matches!(
            reg.verify(Some("")),
            Err(CoreError::Unauthorized(_))
        ));
        let missing = reg.verify(None).expect_err("fail closed without a key");
        assert_eq!(missing.code(), "unauthorized", "{missing}");
        let mut anon = auth;
        anon.allow_anonymous = true;
        assert_eq!(
            ApiKeyRegistry::from_config(&anon)
                .verify(None)
                .expect("anonymous mode accepts a missing key"),
            TenantId::new_unchecked(ANONYMOUS_TENANT)
        );
    }

    #[test]
    fn attestation_requires_every_digest_to_be_allowlisted() {
        let allow = AttestationAllowlist::new([SHARD_1, SHARD_2].into_iter(), true);
        assert_eq!(allow.len(), 2);

        let good = Attestation {
            model_shas: BTreeSet::from([SHARD_1.to_string()]),
            weight_bytes: None,
        };
        assert!(allow.verify(&good).is_ok());

        // A self-declared model name is not identity: an off-allowlist digest
        // cannot join the pool (acceptance test 13).
        let rogue = Attestation {
            model_shas: BTreeSet::from([
                "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
            ]),
            weight_bytes: None,
        };
        let err = allow.verify(&rogue).expect_err("must reject");
        assert!(matches!(err, CoreError::Attestation(_)), "{err}");

        // One trusted shard plus one untrusted shard is still a rejection.
        let mixed = Attestation {
            model_shas: [
                SHARD_1.to_string(),
                rogue.model_shas.iter().next().expect("one").clone(),
            ]
            .into_iter()
            .collect(),
            weight_bytes: None,
        };
        assert!(allow.verify(&mixed).is_err());

        // Empty attestation is a rejection while attestation is required.
        assert!(allow.verify(&Attestation::default()).is_err());
        let lenient = AttestationAllowlist::new([SHARD_1].into_iter(), false);
        assert!(lenient.verify(&Attestation::default()).is_ok());
        // Case-insensitive match, because hex arrives in both cases.
        assert!(allow
            .verify(&Attestation {
                model_shas: [SHARD_1.to_uppercase()].into_iter().collect(),
                weight_bytes: None
            })
            .is_ok());
    }

    #[test]
    fn malformed_allowlist_entries_are_counted_not_silently_dropped() {
        let allow = AttestationAllowlist::new(["not-a-sha", SHARD_1].into_iter(), true);
        assert_eq!(allow.len(), 1);
        assert_eq!(allow.rejected_config_entries(), 1);
        assert!(allow.contains(SHARD_1));
        assert!(!allow.contains("nope"));
    }
}
