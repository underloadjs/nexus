//! Declarative configuration: pools, kinds, tenants and credentials, loadable
//! from a JSON document or from environment-derived values.
//!
//! Why a registry instead of inference: task §5.3 requires each `kind` to carry
//! `idempotent: bool`, and §7.1 requires `kind`+`requires` to select a pool. A
//! hub that *guessed* either would be guessing about correctness, so both are
//! declared. Unknown kinds are rejected rather than dispatched blindly.
//!
//! The core never calls `std::env`: the native runtime collects a
//! `BTreeMap<String, String>` and passes it to [`BrokerConfig::apply_values`],
//! which keeps configuration reproducible in tests and in the WASM host.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;

use crate::ids::{PoolId, TenantId};
use crate::limits::Limits;
use crate::model::WorkerTier;
use crate::protocol::Requires;

/// One kind of work. The registry the hub consults instead of trusting a
/// client's claim about its own job.
///
/// `Default` is derived rather than hand-written because every field's zero value
/// *is* the conservative answer: an empty pool (which `validate()` then refuses),
/// `idempotent = false` — re-running a side-effecting job is worse than failing it,
/// so a new kind is non-idempotent until the operator says otherwise (task §5.3
/// "fail loudly instead") — and no timeout override, which leaves the pool limits in
/// charge.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct KindConfig {
    /// Pool this kind routes to.
    pub pool: String,
    /// Auto-requeue after an ambiguous ACK or expired lease (task §5.3).
    pub idempotent: bool,
    /// Token-stream kind (open-early SSE, §7.2) vs artifact kind (job API, §7.3).
    pub streaming: bool,
    /// Capability floor added to every submission of this kind.
    #[serde(default)]
    pub requires: Requires,
    /// Per-kind override of `Limits::claim_deadline_ms`.
    #[serde(default)]
    pub claim_deadline_ms: Option<u64>,
    /// Per-kind override of `Limits::job_hard_cap_ms`.
    #[serde(default)]
    pub job_hard_cap_ms: Option<u64>,
    /// Per-kind override of `Limits::no_worker_grace_ms`.
    #[serde(default)]
    pub no_worker_grace_ms: Option<u64>,
}

/// One capability pool: a queue plus the actor that owns it (task §5).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PoolConfig {
    /// Pool-specific queue depth; `None` uses `Limits::queue_depth`.
    #[serde(default)]
    pub queue_depth: Option<usize>,
    /// Pool-specific lease lifetime; `None` uses `Limits::lease_ms`.
    #[serde(default)]
    pub lease_ms: Option<u64>,
    /// Pool-specific heartbeat expectation.
    #[serde(default)]
    pub heartbeat_ms: Option<u64>,
    /// Floor a worker must meet to join this pool at all (e.g. `tools`).
    #[serde(default)]
    pub requires: Requires,
    /// Tiers allowed to join; empty means both. Lets an operator keep a
    /// latency-critical pool free of ephemeral hosts (task §11).
    #[serde(default)]
    pub allowed_tiers: BTreeSet<WorkerTier>,
    /// Artifact TTL override for artifact kinds in this pool.
    #[serde(default)]
    pub artifact_ttl_ms: Option<u64>,
}

/// A consumer account: the unit of concurrency caps and rate limits (task §8,
/// §9).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TenantConfig {
    /// Concurrent non-terminal jobs; `None` uses `Limits::tenant_max_concurrent`.
    #[serde(default)]
    pub max_concurrent: Option<usize>,
    /// Prompt body cap; `None` uses `Limits::tenant_max_payload_bytes`.
    #[serde(default)]
    pub max_payload_bytes: Option<u64>,
    /// Pools this tenant may submit to; empty means all.
    #[serde(default)]
    pub allowed_pools: BTreeSet<String>,
    /// Priority class granted to this tenant's submissions.
    #[serde(default)]
    pub priority: Option<String>,
}

/// One consumer credential.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiKeyConfig {
    /// The secret. Lives here only at load time; `auth` keeps digests.
    pub key: String,
    /// Tenant it maps to.
    pub tenant: String,
}

/// Secrets and trust anchors, grouped so "what must not be hardcoded" is
/// greppable in one place.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthConfig {
    /// Pool tokens accepted on worker upgrades (task §9 "pool token per worker").
    #[serde(default)]
    pub worker_tokens: Vec<String>,
    /// Consumer API keys, distinct from pool tokens (§9 "never share one secret
    /// across both directions").
    #[serde(default)]
    pub api_keys: Vec<ApiKeyConfig>,
    /// SHA-256 digests a worker may attest. Seed from the reference
    /// `SHARD_TABLE` (services/molab/molab-strata-runtime.py:94-99).
    #[serde(default)]
    pub model_shas: BTreeSet<String>,
    /// Explicit opt-in to run with no credentials, for local smoke only. Off by
    /// default, and `validate()` refuses an empty credential set without it —
    /// the "no fail-open auth" requirement.
    #[serde(default)]
    pub allow_anonymous: bool,
    /// Whether attestation must be present and allowlisted. Default `true`.
    #[serde(default)]
    pub require_attestation: Option<bool>,
}

impl AuthConfig {
    /// Whether a worker's digests must be non-empty and allowlisted.
    #[must_use]
    pub fn attestation_required(&self) -> bool {
        self.require_attestation.unwrap_or(true)
    }
}

/// The whole broker configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BrokerConfig {
    /// Numeric bounds.
    #[serde(default)]
    pub limits: Limits,
    /// Pools by id.
    #[serde(default)]
    pub pools: BTreeMap<String, PoolConfig>,
    /// Kinds by id.
    #[serde(default)]
    pub kinds: BTreeMap<String, KindConfig>,
    /// Tenants by id.
    #[serde(default)]
    pub tenants: BTreeMap<String, TenantConfig>,
    /// Credentials and trust anchors.
    #[serde(default)]
    pub auth: AuthConfig,
}

/// A configuration problem. Accumulated as a list so an operator fixing a
/// config sees every issue in one startup log line.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid configuration: {}", .problems.join("; "))]
pub struct ConfigError {
    /// Human-readable violations.
    pub problems: Vec<String>,
}

/// Sample kind entry helper, keeping `Default` readable.
fn streaming_kind(pool: &str) -> KindConfig {
    KindConfig {
        pool: pool.to_string(),
        idempotent: true,
        streaming: true,
        ..KindConfig::default()
    }
}

impl Default for BrokerConfig {
    /// A working sample topology mirroring the reference environment: a
    /// 4-slot Strata LLM pool (loopback :8090, 512K context) and a 1-slot
    /// ComfyUI render pool.
    fn default() -> Self {
        let llm_pool = "llm.chat/qwen38";
        let image_pool = "image.render/comfyui";

        let mut cfg = Self {
            limits: Limits::default(),
            pools: BTreeMap::new(),
            kinds: BTreeMap::new(),
            tenants: BTreeMap::new(),
            auth: AuthConfig::default(),
        };

        cfg.pools.insert(
            llm_pool.to_string(),
            PoolConfig {
                // Reference engine shape: qwen38 at 524288 context.
                requires: Requires {
                    model: Some("qwen38".to_string()),
                    min_context_tokens: Some(524_288),
                    capabilities: BTreeSet::new(),
                    kv_estimate_tokens: None,
                },
                ..PoolConfig::default()
            },
        );
        cfg.pools
            .insert(image_pool.to_string(), PoolConfig::default());

        // Written as one literal, matching the pool's `requires` above: the capability
        // floor every `llm.chat` submission inherits is the reference engine's shape
        // (qwen38 at 524288 context), and listing all four fields keeps the model name
        // and the context floor from ever drifting apart.
        let llm_requires = Requires {
            model: Some("qwen38".to_string()),
            min_context_tokens: Some(524_288),
            capabilities: BTreeSet::new(),
            kv_estimate_tokens: None,
        };

        cfg.kinds.insert(
            "llm.chat".to_string(),
            KindConfig {
                requires: llm_requires.clone(),
                ..streaming_kind(llm_pool)
            },
        );
        cfg.kinds.insert(
            "llm.completion".to_string(),
            KindConfig {
                requires: llm_requires,
                ..streaming_kind(llm_pool)
            },
        );
        // Non-streaming, artifact-producing: the ComfyUI shape of §7.3.
        cfg.kinds.insert(
            "image.workflow".to_string(),
            KindConfig {
                pool: image_pool.to_string(),
                idempotent: true,
                streaming: false,
                ..KindConfig::default()
            },
        );
        // Side-effecting kind, so §5.3's "never auto-requeue" is reachable.
        cfg.kinds.insert(
            "notify.webhook".to_string(),
            KindConfig {
                pool: llm_pool.to_string(),
                idempotent: false,
                streaming: false,
                ..KindConfig::default()
            },
        );

        cfg.tenants
            .insert("default".to_string(), TenantConfig::default());
        cfg
    }
}

impl BrokerConfig {
    /// Parse a JSON config document and validate it.
    ///
    /// # Errors
    /// [`ConfigError`] listing every violation, including JSON shape problems.
    pub fn from_json_str(text: &str) -> Result<Self, ConfigError> {
        let cfg: Self = serde_json::from_str(text).map_err(|e| ConfigError {
            problems: vec![format!("config json: {e}")],
        })?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Defaults mutated by a `NAME=value` map (see [`Self::apply_values`]).
    ///
    /// Best-effort accessor: a malformed `NEXUS_CONFIG_JSON` leaves the defaults
    /// in place here. The native runtime calls [`Self::from_values_checked`]
    /// instead, because "my config file was ignored" must never be silent.
    #[must_use]
    pub fn from_values(values: &BTreeMap<String, String>) -> Self {
        let mut cfg = Self::default();
        let _ = cfg.apply_values(values);
        cfg
    }

    /// Defaults mutated by a `NAME=value` map, refusing a malformed
    /// `NEXUS_CONFIG_JSON` rather than silently keeping the defaults.
    ///
    /// # Errors
    /// [`ConfigError`] when `NEXUS_CONFIG_JSON` is present but does not parse
    /// into a valid document (including an unknown field name, which
    /// `deny_unknown_fields` turns into an error at exactly this point).
    pub fn from_values_checked(values: &BTreeMap<String, String>) -> Result<Self, ConfigError> {
        let mut cfg = Self::default();
        cfg.apply_values(values)?;
        Ok(cfg)
    }

    /// Apply environment-style overrides. Unknown names are ignored (the shell
    /// holds plenty of unrelated vars); only `NEXUS_CONFIG_JSON` replaces the
    /// whole document.
    ///
    /// # Errors
    /// [`ConfigError`] when `NEXUS_CONFIG_JSON` is present and unparsable.
    pub fn apply_values(&mut self, values: &BTreeMap<String, String>) -> Result<(), ConfigError> {
        if let Some(json) = values.get("NEXUS_CONFIG_JSON") {
            // Replacing *everything* is the documented behaviour, so a document
            // that fails to parse cannot be partially applied: the hub would
            // otherwise boot on sample pools and no credentials and call that
            // "configured".
            let parsed: Self = serde_json::from_str(json).map_err(|e| ConfigError {
                problems: vec![format!(
                    "NEXUS_CONFIG_JSON is not a valid broker config: {e}"
                )],
            })?;
            *self = parsed;
            return Ok(());
        }

        // Helper closures: typed getters over the string map.
        let text = |key: &str| values.get(key).map(String::as_str);
        let num = |key: &str| text(key).and_then(|v| v.trim().parse::<u64>().ok());
        let set_num = |key: &str, slot: &mut u64| {
            if let Some(v) = num(key) {
                *slot = v;
            }
        };
        let set_usize = |key: &str, slot: &mut usize| {
            if let Some(v) = num(key).map(|v| v as usize) {
                *slot = v;
            }
        };
        let set_u32 = |key: &str, slot: &mut u32| {
            if let Some(v) = num(key).map(|v| v as u32) {
                *slot = v;
            }
        };
        let flag = |key: &str| {
            text(key).map(|v| {
                matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
        };

        set_num("NEXUS_HEARTBEAT_MS", &mut self.limits.heartbeat_ms);
        set_num("NEXUS_LEASE_MS", &mut self.limits.lease_ms);
        set_num("NEXUS_RESUME_GRACE_MS", &mut self.limits.resume_grace_ms);
        set_num(
            "NEXUS_NO_WORKER_GRACE_MS",
            &mut self.limits.no_worker_grace_ms,
        );
        set_num(
            "NEXUS_CLAIM_DEADLINE_MS",
            &mut self.limits.claim_deadline_ms,
        );
        set_num("NEXUS_JOB_HARD_CAP_MS", &mut self.limits.job_hard_cap_ms);
        set_num("NEXUS_CANCEL_ACK_MS", &mut self.limits.cancel_ack_ms);
        set_num(
            "NEXUS_FLUSH_INTERVAL_MS",
            &mut self.limits.flush_interval_ms,
        );
        set_usize("NEXUS_FLUSH_BYTES", &mut self.limits.flush_bytes);
        set_usize("NEXUS_REPLAY_BYTES", &mut self.limits.replay_bytes);
        set_usize(
            "NEXUS_PARTIAL_TEXT_CAP_BYTES",
            &mut self.limits.partial_text_cap_bytes,
        );
        set_num("NEXUS_PAYLOAD_TTL_MS", &mut self.limits.payload_ttl_ms);
        set_num("NEXUS_ARTIFACT_TTL_MS", &mut self.limits.artifact_ttl_ms);
        set_num("NEXUS_JOB_RETENTION_MS", &mut self.limits.job_retention_ms);
        // NEX-6.6 / §9.1 item 3: the cap on an unclaimed detached job. Same
        // `NEXUS_*_MS` shape as the other lifetimes, so an operator retunes every
        // retention bound from one place.
        set_num(
            "NEXUS_DETACHED_ORPHAN_TTL_MS",
            &mut self.limits.detached_orphan_ttl_ms,
        );
        set_num(
            "NEXUS_IDEMPOTENCY_TTL_MS",
            &mut self.limits.idempotency_ttl_ms,
        );
        set_usize(
            "NEXUS_IDEMPOTENCY_MAX_ENTRIES",
            &mut self.limits.max_idempotency_entries,
        );
        set_num(
            "NEXUS_TENANT_MAX_PAYLOAD_BYTES",
            &mut self.limits.tenant_max_payload_bytes,
        );
        set_num(
            "NEXUS_TENANT_MAX_PROMPT_TOKENS",
            &mut self.limits.tenant_max_prompt_tokens,
        );
        set_num("NEXUS_MAX_SPILL_BYTES", &mut self.limits.max_spill_bytes);
        set_num(
            "NEXUS_MAX_ARTIFACT_BYTES",
            &mut self.limits.max_artifact_bytes,
        );
        set_usize(
            "NEXUS_ARTIFACT_CHUNK_BYTES",
            &mut self.limits.artifact_chunk_bytes,
        );
        set_usize(
            "NEXUS_INLINE_PAYLOAD_CAP_BYTES",
            &mut self.limits.inline_payload_cap_bytes,
        );
        set_usize(
            "NEXUS_WORKER_LINE_CAP_BYTES",
            &mut self.limits.worker_line_cap_bytes,
        );
        set_usize(
            "NEXUS_WORKER_FRAME_TEXT_CAP_BYTES",
            &mut self.limits.worker_frame_text_cap_bytes,
        );
        set_usize("NEXUS_QUEUE_DEPTH", &mut self.limits.queue_depth);
        set_usize(
            "NEXUS_TENANT_MAX_CONCURRENT",
            &mut self.limits.tenant_max_concurrent,
        );
        set_usize(
            "NEXUS_MAX_WORKERS_PER_POOL",
            &mut self.limits.max_workers_per_pool,
        );
        set_usize("NEXUS_REPLAY_FRAMES", &mut self.limits.replay_frames);
        set_usize(
            "NEXUS_MAX_SPILLED_PAYLOADS",
            &mut self.limits.max_spilled_payloads,
        );
        set_u32("NEXUS_MAX_ATTEMPTS", &mut self.limits.max_attempts);
        set_u32(
            "NEXUS_MAX_CONTINUATIONS",
            &mut self.limits.max_continuations,
        );
        // Booleans use the `flag` helper (1/true/yes/on); the JSON config reaches
        // the same field by name through `Limits`' serde derive.
        if let Some(on) = flag("NEXUS_CONTINUATION_ALLOW_OPAQUE") {
            self.limits.continuation_allows_opaque_output = on;
        }
        set_u32("NEXUS_MAX_CLAIM_BATCH", &mut self.limits.max_claim_batch);

        if let Some(list) = text("NEXUS_POOL_TOKENS") {
            self.auth.worker_tokens = split_list(list);
        }
        if let Some(list) = text("NEXUS_MODEL_SHAS") {
            self.auth.model_shas = split_list(list).into_iter().collect();
        }
        if let Some(list) = text("NEXUS_API_KEYS") {
            // `secret=tenant` pairs; a bare secret maps to the "default" tenant.
            self.auth.api_keys = split_list(list)
                .into_iter()
                .map(|entry| match entry.split_once('=') {
                    Some((key, tenant)) => ApiKeyConfig {
                        key: key.trim().to_string(),
                        tenant: tenant.trim().to_string(),
                    },
                    None => ApiKeyConfig {
                        key: entry.trim().to_string(),
                        tenant: "default".to_string(),
                    },
                })
                .collect();
        }
        if let Some(on) = flag("NEXUS_ALLOW_ANONYMOUS") {
            self.auth.allow_anonymous = on;
        }
        if let Some(on) = flag("NEXUS_REQUIRE_ATTESTATION") {
            self.auth.require_attestation = Some(on);
        }
        Ok(())
    }

    /// Validate numeric bounds, kind→pool references and the credential policy.
    ///
    /// # Errors
    /// [`ConfigError`] with every problem found.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let mut problems = self.limits.validate();

        if self.pools.is_empty() {
            problems.push("no pools configured".to_string());
        }
        for (kind, k) in &self.kinds {
            if k.pool.is_empty() {
                problems.push(format!("kind {kind:?} has no pool"));
            } else if !self.pools.contains_key(&k.pool) {
                problems.push(format!(
                    "kind {kind:?} references unknown pool {:?}",
                    k.pool
                ));
            }
        }
        if !self.kinds.contains_key("llm.chat") {
            problems
                .push("kind \"llm.chat\" must exist: the OpenAI adapter depends on it".to_string());
        }

        if !self.auth.allow_anonymous {
            if self.auth.worker_tokens.is_empty() {
                problems.push(
                    "auth.worker_tokens is empty: workers could join unauthenticated (set NEXUS_POOL_TOKENS or NEXUS_ALLOW_ANONYMOUS=1)"
                        .to_string(),
                );
            }
            if self.auth.api_keys.is_empty() {
                problems.push(
                    "auth.api_keys is empty: consumers could submit unauthenticated (set NEXUS_API_KEYS or NEXUS_ALLOW_ANONYMOUS=1)"
                        .to_string(),
                );
            }
        }
        if self.auth.attestation_required()
            && self.auth.model_shas.is_empty()
            && !self.auth.allow_anonymous
        {
            problems.push(
                "auth.model_shas is empty: no worker could pass attestation (set NEXUS_MODEL_SHAS or NEXUS_ALLOW_ANONYMOUS=1)"
                    .to_string(),
            );
        }
        for key in &self.auth.api_keys {
            if key.key.trim().is_empty() {
                problems.push("an api key is blank".to_string());
            }
            if !self.tenants.contains_key(&key.tenant) {
                problems.push(format!("api key maps to unknown tenant {:?}", key.tenant));
            }
        }
        for (tenant, t) in &self.tenants {
            for p in &t.allowed_pools {
                if !self.pools.contains_key(p) {
                    problems.push(format!("tenant {tenant:?} allows unknown pool {p:?}"));
                }
            }
            if let Some(priority) = &t.priority {
                if !matches!(priority.as_str(), "realtime" | "default" | "bulk") {
                    problems.push(format!(
                        "tenant {tenant:?} priority {priority:?} must be realtime|default|bulk"
                    ));
                }
            }
        }

        if problems.is_empty() {
            Ok(())
        } else {
            Err(ConfigError { problems })
        }
    }

    /// Kind lookup.
    #[must_use]
    pub fn kind(&self, kind: &str) -> Option<&KindConfig> {
        self.kinds.get(kind)
    }

    /// Pool lookup by parsed id.
    #[must_use]
    pub fn pool(&self, pool: &PoolId) -> Option<&PoolConfig> {
        self.pools.get(pool.as_ref())
    }

    /// Tenant lookup by parsed id.
    #[must_use]
    pub fn tenant(&self, tenant: &TenantId) -> Option<&TenantConfig> {
        self.tenants.get(tenant.as_ref())
    }

    /// Effective queue depth for a pool.
    #[must_use]
    pub fn queue_depth_for(&self, pool: &PoolId) -> usize {
        self.pool(pool)
            .and_then(|p| p.queue_depth)
            .unwrap_or(self.limits.queue_depth)
    }

    /// Effective lease lifetime for a pool.
    #[must_use]
    pub fn lease_ms_for(&self, pool: &PoolId) -> u64 {
        self.pool(pool)
            .and_then(|p| p.lease_ms)
            .unwrap_or(self.limits.lease_ms)
    }

    /// Effective heartbeat expectation for a pool.
    #[must_use]
    pub fn heartbeat_ms_for(&self, pool: &PoolId) -> u64 {
        self.pool(pool)
            .and_then(|p| p.heartbeat_ms)
            .unwrap_or(self.limits.heartbeat_ms)
    }

    /// Effective artifact TTL for a pool.
    #[must_use]
    pub fn artifact_ttl_for(&self, pool: &PoolId) -> u64 {
        self.pool(pool)
            .and_then(|p| p.artifact_ttl_ms)
            .unwrap_or(self.limits.artifact_ttl_ms)
    }

    /// Whether a tier may join a pool; an empty `allowed_tiers` admits both.
    #[must_use]
    pub fn tier_allowed(&self, pool: &PoolId, tier: WorkerTier) -> bool {
        match self.pool(pool) {
            Some(p) if !p.allowed_tiers.is_empty() => p.allowed_tiers.contains(&tier),
            _ => true,
        }
    }

    /// Effective per-tenant concurrency cap.
    #[must_use]
    pub fn tenant_concurrency_for(&self, tenant: &TenantId) -> usize {
        self.tenant(tenant)
            .and_then(|t| t.max_concurrent)
            .unwrap_or(self.limits.tenant_max_concurrent)
    }

    /// Effective per-tenant payload cap.
    #[must_use]
    pub fn tenant_payload_bytes_for(&self, tenant: &TenantId) -> u64 {
        self.tenant(tenant)
            .and_then(|t| t.max_payload_bytes)
            .unwrap_or(self.limits.tenant_max_payload_bytes)
    }

    /// Whether a tenant may submit to a pool.
    #[must_use]
    pub fn tenant_allows_pool(&self, tenant: &TenantId, pool: &PoolId) -> bool {
        match self.tenant(tenant) {
            Some(t) if !t.allowed_pools.is_empty() => t.allowed_pools.contains(pool.as_ref()),
            _ => true,
        }
    }

    /// Priority class for a tenant's submissions.
    #[must_use]
    pub fn tenant_priority(&self, tenant: &TenantId) -> crate::model::Priority {
        let name = self
            .tenant(tenant)
            .and_then(|t| t.priority.as_deref())
            .unwrap_or("default");
        crate::model::Priority::from_name(name)
    }
}

/// Split a comma/whitespace separated env list, trimming and dropping empties.
fn split_list(list: &str) -> Vec<String> {
    list.split([',', ' ', '\n', '\t'])
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn anonymous() -> BrokerConfig {
        let mut cfg = BrokerConfig::default();
        cfg.auth.allow_anonymous = true;
        cfg
    }

    #[test]
    fn default_config_is_valid_and_sampled() {
        let cfg = anonymous();
        assert_eq!(cfg.validate(), Ok(()));
        let chat = cfg.kind("llm.chat").expect("kind");
        assert_eq!(chat.pool, "llm.chat/qwen38");
        assert!(chat.idempotent && chat.streaming);
        let hook = cfg.kind("notify.webhook").expect("kind");
        assert!(!hook.idempotent && !hook.streaming);
        assert_eq!(
            cfg.queue_depth_for(&PoolId::new_unchecked("llm.chat/qwen38")),
            128
        );
        assert_eq!(
            cfg.tenant_concurrency_for(&TenantId::new_unchecked("default")),
            4
        );
    }

    #[test]
    fn empty_credentials_fail_closed() {
        // With allow_anonymous = false and no secrets, boot must be refused
        // rather than quietly accepting anyone.
        let cfg = BrokerConfig::default();
        let err = cfg.validate().expect_err("must fail");
        assert!(err
            .problems
            .iter()
            .any(|p| p.contains("auth.worker_tokens is empty")));
        assert!(err
            .problems
            .iter()
            .any(|p| p.contains("auth.api_keys is empty")));
        assert!(err
            .problems
            .iter()
            .any(|p| p.contains("auth.model_shas is empty")));
    }

    #[test]
    fn kind_referencing_unknown_pool_is_caught() {
        let mut cfg = anonymous();
        cfg.kinds.insert(
            "bad.kind".to_string(),
            KindConfig {
                pool: "nope".to_string(),
                ..Default::default()
            },
        );
        let err = cfg.validate().expect_err("must fail");
        assert_eq!(
            err.problems,
            vec![r#"kind "bad.kind" references unknown pool "nope""#.to_string()]
        );
    }

    #[test]
    fn env_overrides_are_exact_and_lists_parse() {
        let mut values = BTreeMap::new();
        values.insert("NEXUS_QUEUE_DEPTH".to_string(), "7".to_string());
        values.insert("NEXUS_LEASE_MS".to_string(), "90000".to_string());
        values.insert(
            "NEXUS_POOL_TOKENS".to_string(),
            " tok-a ,tok-b ".to_string(),
        );
        values.insert("NEXUS_API_KEYS".to_string(), "sk-1=acme,sk-2".to_string());
        values.insert("NEXUS_MODEL_SHAS".to_string(), "aa\nbb".to_string());
        values.insert("NEXUS_ALLOW_ANONYMOUS".to_string(), "0".to_string());
        let mut cfg = BrokerConfig::from_values(&values);
        assert_eq!(cfg.limits.queue_depth, 7);
        assert_eq!(cfg.limits.lease_ms, 90_000);
        assert_eq!(
            cfg.auth.worker_tokens,
            vec!["tok-a".to_string(), "tok-b".to_string()]
        );
        assert_eq!(
            cfg.auth.api_keys,
            vec![
                ApiKeyConfig {
                    key: "sk-1".to_string(),
                    tenant: "acme".to_string()
                },
                ApiKeyConfig {
                    key: "sk-2".to_string(),
                    tenant: "default".to_string()
                },
            ]
        );
        assert_eq!(cfg.auth.model_shas.len(), 2);
        assert!(!cfg.auth.allow_anonymous);
        // `validate()` refuses an api key that names an undeclared tenant: §9's
        // tenant registry is declared, never inferred from a credential. The env
        // path starts from `BrokerConfig::default()`, which declares only
        // "default", so this fixture declares `acme` before validating.
        cfg.tenants
            .insert("acme".to_string(), TenantConfig::default());
        assert!(cfg.validate().is_ok(), "{:?}", cfg.validate().err());
    }

    #[test]
    fn malformed_nexus_config_json_stops_the_hub() {
        let mut values = BTreeMap::new();
        // `queue_dpeth` is caught by `deny_unknown_fields`, and a document that
        // replaces everything cannot be silently skipped: the hub would otherwise
        // boot on the sample pools with no credentials and call that configured.
        values.insert(
            "NEXUS_CONFIG_JSON".to_string(),
            r#"{"limits":{"queue_dpeth":4}}"#.to_string(),
        );
        let err = BrokerConfig::from_values_checked(&values).expect_err("a typo must stop startup");
        assert!(
            err.problems[0].contains("NEXUS_CONFIG_JSON"),
            "{:?}",
            err.problems
        );
        // The best-effort accessor still yields the defaults, and says so by
        // keeping the documented default depth rather than a partial merge.
        assert_eq!(BrokerConfig::from_values(&values).limits.queue_depth, 128);
        // A valid document replaces everything.
        values.insert(
            "NEXUS_CONFIG_JSON".to_string(),
            r#"{"limits":{"queue_depth":3}}"#.to_string(),
        );
        assert_eq!(
            BrokerConfig::from_values_checked(&values)
                .expect("valid")
                .limits
                .queue_depth,
            3
        );
    }

    #[test]
    fn bad_numeric_env_is_ignored_not_zeroed() {
        let mut values = BTreeMap::new();
        values.insert("NEXUS_QUEUE_DEPTH".to_string(), "lots".to_string());
        let cfg = BrokerConfig::from_values(&values);
        assert_eq!(
            cfg.limits.queue_depth, 128,
            "an unparsable value keeps the default"
        );
    }

    /// The NEX-6.6 detached-job cap has to be reachable the same way every other
    /// lifetime is: exact value through `NEXUS_*`, default when the var is absent,
    /// default when the value is junk.
    #[test]
    fn detached_orphan_ttl_env_override_is_exact() {
        let mut values = BTreeMap::new();
        values.insert(
            "NEXUS_DETACHED_ORPHAN_TTL_MS".to_string(),
            "45000".to_string(),
        );
        assert_eq!(
            BrokerConfig::from_values(&values)
                .limits
                .detached_orphan_ttl_ms,
            45_000
        );

        // Absent: the documented 5-minute default, which `validate()` accepts.
        let mut unset = BrokerConfig::from_values(&BTreeMap::new());
        assert_eq!(unset.limits.detached_orphan_ttl_ms, 300_000);
        // Credentials are unrelated to this bound, so opt out of them the way every
        // other fixture here does; the assertion is about the TTL, not §9's policy.
        unset.auth.allow_anonymous = true;
        assert_eq!(unset.validate(), Ok(()));

        // Junk: keeps the default rather than zeroing the bound (`set_num` only ever
        // assigns a value that parsed).
        let mut junk = BTreeMap::new();
        junk.insert("NEXUS_DETACHED_ORPHAN_TTL_MS".to_string(), "5m".to_string());
        assert_eq!(
            BrokerConfig::from_values(&junk)
                .limits
                .detached_orphan_ttl_ms,
            300_000
        );
    }

    #[test]
    fn json_config_round_trips_and_rejects_typos() {
        let cfg = anonymous();
        let text = serde_json::to_string(&cfg).expect("serialize");
        let back = BrokerConfig::from_json_str(&text).expect("parse");
        assert_eq!(back, cfg);

        let err = BrokerConfig::from_json_str(r#"{"limits":{"queue_dpeth":4}}"#)
            .expect_err("a limit-name typo must be a hard error");
        assert!(
            err.problems[0].contains("queue_dpeth"),
            "{}",
            err.problems[0]
        );
    }

    #[test]
    fn pool_scoped_overrides_win() {
        let mut cfg = anonymous();
        cfg.pools.insert(
            "llm.chat/qwen38".to_string(),
            PoolConfig {
                queue_depth: Some(3),
                lease_ms: Some(90_000),
                allowed_tiers: BTreeSet::from([WorkerTier::Dedicated]),
                ..PoolConfig::default()
            },
        );
        assert_eq!(
            cfg.queue_depth_for(&PoolId::new_unchecked("llm.chat/qwen38")),
            3
        );
        assert_eq!(
            cfg.lease_ms_for(&PoolId::new_unchecked("llm.chat/qwen38")),
            90_000
        );
        assert!(!cfg.tier_allowed(
            &PoolId::new_unchecked("llm.chat/qwen38"),
            WorkerTier::Ephemeral
        ));
        // A pool with no override still reports the global lease.
        assert_eq!(
            cfg.lease_ms_for(&PoolId::new_unchecked("image.render/comfyui")),
            60_000
        );
    }
}
