//! Shared deterministic test fixtures. `cfg(test)` so it never ships, and shared
//! across modules so a `JobRecord` gained field cannot silently break one test
//! module's hand-written literal.

use std::collections::BTreeSet;

use serde_json::json;

use crate::config::BrokerConfig;
use crate::ids::{JobId, PoolId, TenantId, WorkerId};
use crate::model::{EngineState, JobRecord, LinkState, Priority, Submission, WorkerRecord, WorkerTier};
use crate::protocol::{Attestation, Requires};
use crate::time::Timestamp;

/// A validated, credential-free config for logic tests.
#[must_use]
pub fn config() -> BrokerConfig {
    let mut cfg = BrokerConfig::default();
    cfg.auth.allow_anonymous = true;
    // Small windows so tests can drive deadlines without large loops.
    cfg.limits.no_worker_grace_ms = 2_000;
    cfg
}

/// Default pool used by fixtures.
#[must_use]
pub fn pool_id() -> PoolId {
    PoolId::new_unchecked("llm.chat/qwen38")
}

/// A plain streaming submission.
#[must_use]
pub fn submission(kind: &str) -> Submission {
    Submission {
        kind: kind.to_string(),
        pool: pool_id(),
        tenant: TenantId::new_unchecked("acme"),
        requires: Requires::default(),
        payload: json!({"model": "qwen38", "messages": [{"role": "user", "content": "hi"}]}),
        content_type: "application/json".to_string(),
        idempotency_key: format!("key-{kind}"),
        session_hint: None,
        priority: Priority::Default,
        idempotent: true,
        streaming: true,
        cancel_on_consumer_gone: true,
    }
}

/// A ready job record with sane bounds, used by scheduler/relay/actor tests.
#[must_use]
pub fn job_record(id: &str) -> JobRecord {
    JobRecord::new(
        JobId::new_unchecked(id),
        &submission("llm.chat"),
        64,
        "0".repeat(64),
        0,
        60_000,
        120_000,
        8,
        4_096,
    )
}

/// A worker record in a chosen state.
#[must_use]
pub fn worker(
    id: &str,
    tier: WorkerTier,
    capacity: u32,
    free_slots: u32,
    free_kv: Option<u64>,
    ready: bool,
) -> WorkerRecord {
    let mut rec = WorkerRecord::new(
        WorkerId::new_unchecked(id),
        1,
        capacity,
        Requires {
            model: Some("qwen38".to_string()),
            min_context_tokens: Some(524_288),
            capabilities: BTreeSet::from(["tools".to_string()]),
            kv_estimate_tokens: None,
        },
        Attestation::default(),
        BTreeSet::from([pool_id()]),
        tier,
        0,
    );
    rec.free_slots = free_slots;
    rec.free_kv_tokens = free_kv;
    rec.engine = if ready { EngineState::Ready } else { EngineState::Loading };
    rec.link = LinkState::Live;
    rec
}

/// The reference SHARD_TABLE digests (molab-strata-runtime.py:95-98).
pub const SHARD_1: &str = "40f369eca301ec6ea318dbb671b3132c26a2f0b89aa5458a7051b33215ee296c";
/// Second shard digest.
pub const SHARD_2: &str = "316b46f3a2dbd68c900f43136ab9449f9dcc3725dfd8c794847c204bc161e113";

/// A ready dedicated worker with full capacity.
#[must_use]
pub fn ready_worker(id: &str, capacity: u32) -> WorkerRecord {
    worker(id, WorkerTier::Dedicated, capacity, capacity, Some(524_288), true)
}

/// An attestation carrying the reference shard digests.
#[must_use]
pub fn attestation(shas: &[&str]) -> Attestation {
    Attestation {
        model_shas: shas.iter().map(|s| (*s).to_string()).collect(),
        weight_bytes: None,
    }
}

/// Advance a timestamp, for readability in deadline tests.
#[must_use]
pub fn at(ms: Timestamp) -> Timestamp {
    ms
}
