//! `nexus-wasm` — the broker core across the wasm boundary (task requirement
//! "a real Node-compatible WASM broker core").
//!
//! # What crosses the boundary
//!
//! Every **decision** crosses: admission, capability pre-check, the atomic claim,
//! lease accounting, requeue vs terminal, seq de-duplication, replay, flush
//! batching, payload spill and artifact TTL bookkeeping. This module is a thin
//! JSON shim over [`nexus_core::PoolActor`]; it contains no scheduling logic of
//! its own, so there is no second implementation to drift.
//!
//! # What does not, and why (the honest runtime boundary)
//!
//! `axum` + `tokio::net` need an I/O driver and a multi-threaded runtime that
//! `wasm32-unknown-unknown` does not have, and a WASM module has no listener to
//! accept a socket into. So this build is **not** "the axum server running in
//! WASM". It is the ownership/scheduling spine, driven by a JS host that owns the
//! sockets:
//!
//! ```text
//! native : sockets, HTTP/SSE, timers, disk artifacts   ->  ActorMsg
//! wasm   : ActorMsg in, Effect[] out                  ->  JS performs the I/O
//! ```
//!
//! The host therefore supplies the clock (`now_ms` on every call — the module has
//! no clock of its own) and performs every side effect the actor asks for: write
//! a WS frame, start an SSE body, answer 503, delete a file. `examples/node-smoke.mjs`
//! shows that loop end to end and asserts the same claims the Rust tests make.
//!
//! # Wire format
//!
//! Calls take JSON strings and return JSON strings (`Effect[]`), so no
//! `serde-wasm-bindgen` conversion layer is needed and the payloads are exactly
//! the shapes the native runtime emits — one contract, two hosts.

// Test builds may be blunt (see `nexus-core`'s crate attributes and the workspace
// lint policy in Cargo.toml): an `expect` in a test is the assertion.
#![cfg_attr(test, allow(clippy::expect_used, clippy::unwrap_used))]

use wasm_bindgen::prelude::*;

use nexus_core::adapters;
use nexus_core::ids::ArtifactId;
use nexus_core::ids::ConsumerId;
use nexus_core::ids::JobId;
use nexus_core::ids::PayloadRefId;
use nexus_core::ids::PoolId;
use nexus_core::ids::WorkerId;
use nexus_core::ActorMsg;
use nexus_core::BrokerConfig;
use nexus_core::Effect;
use nexus_core::HelloInput;
use nexus_core::PoolActor;
use nexus_core::Submission;
use serde_json::json;
use serde_json::Value;

/// One broker instance: one pool's actor plus the host-supplied clock.
#[wasm_bindgen]
#[derive(Default)]
pub struct NexusBroker {
    /// The actor; `None` until `NexusBroker::new` succeeds, because
    /// wasm-bindgen constructors cannot return `Result` on every target.
    actor: Option<PoolActor>,
    /// Pool served by this instance.
    pool: String,
    /// Connection ids per worker id, assigned here so stale-socket detection
    /// works identically to the native runtime.
    conns: std::collections::BTreeMap<String, u64>,
    /// Last clock value, used as the default when a call omits `now_ms`.
    now_ms: u64,
}

#[wasm_bindgen]
impl NexusBroker {
    /// Build a broker for the first configured pool.
    ///
    /// `config_json` is the same document the native server reads
    /// (`NEXUS_CONFIG_JSON`). Errors are returned as JS exceptions.
    #[wasm_bindgen(constructor)]
    pub fn new(config_json: &str) -> Result<NexusBroker, JsError> {
        let cfg = BrokerConfig::from_json_str(config_json)
            .map_err(|e| JsError::new(&format!("config: {e}")))?;
        let pool = cfg
            .pools
            .keys()
            .next()
            .cloned()
            .ok_or_else(|| JsError::new("config has no pools"))?;
        let actor = PoolActor::new(PoolId::new_unchecked(pool.clone()), cfg);
        Ok(Self {
            actor: Some(actor),
            pool,
            conns: std::collections::BTreeMap::new(),
            now_ms: 0,
        })
    }

    /// Reserve this worker's connection id and send its `Hello`.
    ///
    /// `hello_json` is the `WorkerFrame::Hello` object (the same bytes a real
    /// worker puts on the socket).
    #[wasm_bindgen(js_name = hello)]
    pub fn hello(
        &mut self,
        worker_id: &str,
        hello_json: &str,
        now_ms: f64,
    ) -> Result<String, JsError> {
        let now = self.clock(now_ms);
        let frame: nexus_core::WorkerFrame = serde_json::from_str(hello_json)
            .map_err(|e| JsError::new(&format!("hello json: {e}")))?;
        let hello =
            HelloInput::from_frame(&frame).ok_or_else(|| JsError::new("expected a hello frame"))?;
        // Reserve the connection id before touching the actor: `next_conn` needs
        // `&mut self`, and the actor's mutable borrow has to outlive it. Taking
        // the actor borrow first is what made the previous ordering a
        // double-borrow of `self`.
        let conn = self.next_conn(worker_id);
        let actor = self.actor_mut()?;
        let effects = actor.handle(
            now,
            ActorMsg::Hello {
                worker: WorkerId::new_unchecked(worker_id),
                conn,
                hello,
            },
        );
        Ok(to_json(&effects))
    }

    /// Deliver one worker frame. The connection id recorded by `hello` is
    /// attached automatically, so a host that drops and re-runs a connection gets
    /// the same stale-frame rejection the native runtime applies.
    #[wasm_bindgen(js_name = workerFrame)]
    pub fn worker_frame(
        &mut self,
        worker_id: &str,
        frame_json: &str,
        now_ms: f64,
    ) -> Result<String, JsError> {
        let now = self.clock(now_ms);
        let conn = *self
            .conns
            .get(worker_id)
            .ok_or_else(|| JsError::new("call hello() for this worker first"))?;
        let frame: nexus_core::WorkerFrame = serde_json::from_str(frame_json)
            .map_err(|e| JsError::new(&format!("frame json: {e}")))?;
        let effects = self.actor_mut()?.handle(
            now,
            ActorMsg::WorkerFrame {
                worker: WorkerId::new_unchecked(worker_id),
                conn,
                frame,
            },
        );
        Ok(to_json(&effects))
    }

    /// A worker socket closed.
    #[wasm_bindgen(js_name = workerGone)]
    pub fn worker_gone(&mut self, worker_id: &str, now_ms: f64) -> Result<String, JsError> {
        let now = self.clock(now_ms);
        let conn = self.conns.remove(worker_id).unwrap_or(0);
        let effects = self.actor_mut()?.handle(
            now,
            ActorMsg::WorkerGone {
                worker: WorkerId::new_unchecked(worker_id),
                conn,
            },
        );
        Ok(to_json(&effects))
    }

    /// Submit a job. `submission_json` is a [`Submission`]; `job_id` is chosen by
    /// the host (the native runtime uses an id generator; a notebook host can use
    /// `crypto.randomUUID()`).
    #[wasm_bindgen(js_name = submit)]
    pub fn submit(
        &mut self,
        job_id: &str,
        submission_json: &str,
        now_ms: f64,
    ) -> Result<String, JsError> {
        let now = self.clock(now_ms);
        let submission: Submission = serde_json::from_str(submission_json)
            .map_err(|e| JsError::new(&format!("submission json: {e}")))?;
        let effects = self.actor_mut()?.handle(
            now,
            ActorMsg::Submit {
                job: JobId::new_unchecked(job_id),
                consumer: ConsumerId::new_unchecked(format!("con-{job_id}")),
                submission,
            },
        );
        Ok(to_json(&effects))
    }

    /// Attach a consumer for replay (`Last-Event-ID` / `?from_seq=`).
    #[wasm_bindgen(js_name = attach)]
    pub fn attach(
        &mut self,
        job_id: &str,
        consumer_id: &str,
        from_seq: f64,
        now_ms: f64,
    ) -> Result<String, JsError> {
        let now = self.clock(now_ms);
        let effects = self.actor_mut()?.handle(
            now,
            ActorMsg::Attach {
                job: JobId::new_unchecked(job_id),
                consumer: ConsumerId::new_unchecked(consumer_id),
                from_seq: from_seq as u64,
            },
        );
        Ok(to_json(&effects))
    }

    /// Detach a consumer; a streaming job's last detach fans out `Cancel` (§6).
    #[wasm_bindgen(js_name = detach)]
    pub fn detach(
        &mut self,
        job_id: &str,
        consumer_id: &str,
        now_ms: f64,
    ) -> Result<String, JsError> {
        let now = self.clock(now_ms);
        let effects = self.actor_mut()?.handle(
            now,
            ActorMsg::Detach {
                job: JobId::new_unchecked(job_id),
                consumer: ConsumerId::new_unchecked(consumer_id),
            },
        );
        Ok(to_json(&effects))
    }

    /// Explicit cancel.
    #[wasm_bindgen(js_name = cancel)]
    pub fn cancel(&mut self, job_id: &str, now_ms: f64) -> Result<String, JsError> {
        let now = self.clock(now_ms);
        let effects = self.actor_mut()?.handle(
            now,
            ActorMsg::Cancel {
                job: JobId::new_unchecked(job_id),
                reason: nexus_core::protocol::CancelReason::ConsumerGone,
            },
        );
        Ok(to_json(&effects))
    }

    /// Record artifact bytes the host wrote to disk/object storage.
    #[wasm_bindgen(js_name = artifactStored)]
    #[allow(clippy::too_many_arguments)]
    pub fn artifact_stored(
        &mut self,
        job_id: &str,
        artifact_id: &str,
        worker_id: &str,
        attempt: f64,
        chunk_index: f64,
        chunk_bytes: f64,
        complete: bool,
        stored_sha256: Option<String>,
        declared_sha256: Option<String>,
        declared_size: Option<f64>,
        content_type: Option<String>,
        now_ms: f64,
    ) -> Result<String, JsError> {
        let now = self.clock(now_ms);
        let conn = self.conns.get(worker_id).copied().unwrap_or(0);
        let effects = self.actor_mut()?.handle(
            now,
            ActorMsg::ArtifactStored {
                job: JobId::new_unchecked(job_id),
                artifact: ArtifactId::new_unchecked(artifact_id),
                worker: WorkerId::new_unchecked(worker_id),
                conn,
                attempt: attempt as u32,
                chunk_index: chunk_index as u64,
                chunk_bytes: chunk_bytes as u64,
                complete,
                stored_sha256,
                declared: nexus_core::actor::DeclaredArtifact {
                    sha256: declared_sha256,
                    size_bytes: declared_size.map(|v| v as u64),
                },
                content_type,
            },
        );
        Ok(to_json(&effects))
    }

    /// Advance the broker: leases, dispatch deadlines, TTL sweeps, flush windows,
    /// keepalives. The host should call this on its own timer (the native runtime
    /// uses a 100 ms interval).
    #[wasm_bindgen(js_name = tick)]
    pub fn tick(&mut self, now_ms: f64) -> Result<String, JsError> {
        let now = self.clock(now_ms);
        Ok(to_json(
            &self
                .actor_mut()?
                .handle(now, ActorMsg::Tick { now_ms: now }),
        ))
    }

    /// Drain the pool: cancel leased jobs, fail queued ones retryably.
    #[wasm_bindgen(js_name = shutdown)]
    pub fn shutdown(&mut self, now_ms: f64) -> Result<String, JsError> {
        let now = self.clock(now_ms);
        Ok(to_json(
            &self
                .actor_mut()?
                .handle(now, ActorMsg::Shutdown { now_ms: now }),
        ))
    }

    /// Derived job view as JSON, or `null` when unknown.
    #[wasm_bindgen(js_name = jobView)]
    pub fn job_view(&self, job_id: &str) -> Option<String> {
        let actor = self.actor.as_ref()?;
        actor
            .job_view(&JobId::new_unchecked(job_id))
            .map(|view| serde_json::to_string(&view).unwrap_or_default())
    }

    /// The spilled prompt bytes for a worker fetch, as JSON metadata with the
    /// body as a UTF-8 string; `null` when unauthorized or gone. The native
    /// runtime serves the same grant as raw bytes.
    #[wasm_bindgen(js_name = payloadFetch)]
    pub fn payload_fetch(
        &self,
        ref_id: &str,
        token: &str,
        worker_id: &str,
        attempt: f64,
        now_ms: f64,
    ) -> Option<String> {
        let actor = self.actor.as_ref()?;
        let entry = actor
            .payloads()
            .authorize_fetch(
                &PayloadRefId::parse(ref_id).ok()?,
                token,
                &WorkerId::new_unchecked(worker_id),
                attempt as u32,
                now_ms as u64,
            )
            .ok()?;
        let body = String::from_utf8(entry.bytes.clone()).ok()?;
        // `to_string(..).ok()` already yields the `Option<String>` this returns; the
        // extra `Some(..)` wrapped an Option in an Option.
        serde_json::to_string(&json!({
            "sha256": entry.sha256,
            "size_bytes": entry.size_bytes,
            "content_type": entry.content_type,
            "body": body,
        }))
        .ok()
    }

    /// `GET /v1/models` body, derived from warm workers only.
    #[wasm_bindgen(js_name = modelsDocument)]
    pub fn models_document(&self, now_ms: f64) -> String {
        match self.actor.as_ref() {
            Some(actor) => serde_json::to_string(&adapters::models_document(
                &actor.warm_models(),
                now_ms as u64,
            ))
            .unwrap_or_default(),
            None => "{\"object\":\"list\",\"data\":[]}".to_string(),
        }
    }

    /// `GET /healthz` body, including the degraded-capacity signal.
    #[wasm_bindgen(js_name = healthBody)]
    pub fn health_body(&self) -> String {
        match self.actor.as_ref() {
            Some(actor) => serde_json::to_string(&adapters::health_body(
                &self.pool,
                actor.ready_workers(),
                actor.loading_workers(),
                actor.queue_depth(),
                actor.leases_len(),
            ))
            .unwrap_or_default(),
            None => "{\"status\":\"uninitialized\"}".to_string(),
        }
    }

    /// `GET /metrics` body in Prometheus text format.
    #[wasm_bindgen(js_name = metricsText)]
    pub fn metrics_text(&self) -> String {
        self.actor
            .as_ref()
            .map(|a| a.metrics().render())
            .unwrap_or_default()
    }

    /// A counter value, so a JS test can assert metrics without parsing text.
    #[wasm_bindgen(js_name = metricValue)]
    pub fn metric_value(&self, name: &str) -> f64 {
        self.actor
            .as_ref()
            .map(|a| a.metrics().get(name) as f64)
            .unwrap_or(0.0)
    }

    /// Decode one worker→hub line with the configured caps. Exposed so the JS
    /// host and the Rust runtime share one codec (framing bugs cannot differ).
    #[wasm_bindgen(js_name = decodeWorkerLine)]
    pub fn decode_worker_line(&self, line: &str) -> Result<String, JsError> {
        let caps = self.actor()?.limits().worker_caps();
        let frame = nexus_core::protocol::decode_worker_frame(line, &caps)
            .map_err(|e| JsError::new(&e.to_string()))?;
        // Returned directly: the `?` inside an enclosing `Ok(..)` was the whole
        // conversion, so the wrapper only made clippy (`needless_question_mark`)
        // and the reader do the same work twice. Behavior is identical — the
        // `JsError` still carries the codec message.
        serde_json::to_string(&frame).map_err(|e| JsError::new(&e.to_string()))
    }

    /// Encode a hub→worker frame, enforcing the outbound line cap.
    #[wasm_bindgen(js_name = encodeHubFrame)]
    pub fn encode_hub_frame(&self, frame_json: &str) -> Result<String, JsError> {
        let caps = self.actor()?.limits().hub_caps();
        let frame: nexus_core::HubFrame =
            serde_json::from_str(frame_json).map_err(|e| JsError::new(&e.to_string()))?;
        nexus_core::protocol::encode_line(&frame, &caps).map_err(|e| JsError::new(&e.to_string()))
    }

    /// Parse an OpenAI chat body and produce the matching `Submission` JSON, so a
    /// JS edge (or the smoke test) builds submissions the same way the Rust
    /// adapters do.
    #[wasm_bindgen(js_name = submissionFromChat)]
    pub fn submission_from_chat(
        &self,
        body_json: &str,
        tenant: &str,
        idempotency_key: Option<String>,
        session_hint: Option<String>,
    ) -> Result<String, JsError> {
        let actor = self.actor()?;
        let body: Value = serde_json::from_str(body_json)
            .map_err(|e| JsError::new(&format!("body json: {e}")))?;
        let req = adapters::ChatRequest::parse(&body, adapters::KIND_CHAT)
            .map_err(|e| JsError::new(&e.to_string()))?;
        let cfg = actor.config();
        let sub = req.to_submission(
            cfg,
            &nexus_core::TenantId::new_unchecked(tenant),
            PoolId::new_unchecked(self.pool.clone()),
            idempotency_key.as_deref(),
            session_hint,
            cfg.tenant_priority(&nexus_core::TenantId::new_unchecked(tenant)),
        );
        // Same shape as `decode_worker_line`: one `map_err`, no `Ok(..?)` wrapper.
        serde_json::to_string(&sub).map_err(|e| JsError::new(&e.to_string()))
    }

    // ------------------------------------------------------------------ plumbing

    /// Resolve the caller-supplied clock; `NaN`/`<0` reuse the last value.
    fn clock(&mut self, now_ms: f64) -> u64 {
        if now_ms.is_finite() && now_ms >= 0.0 {
            self.now_ms = now_ms as u64;
        }
        self.now_ms
    }

    fn next_conn(&mut self, worker_id: &str) -> u64 {
        let conn = self.conns.values().max().copied().unwrap_or(0) + 1;
        self.conns.insert(worker_id.to_string(), conn);
        conn
    }

    fn actor(&self) -> Result<&PoolActor, JsError> {
        self.actor
            .as_ref()
            .ok_or_else(|| JsError::new("broker is not initialised"))
    }

    fn actor_mut(&mut self) -> Result<&mut PoolActor, JsError> {
        self.actor
            .as_mut()
            .ok_or_else(|| JsError::new("broker is not initialised"))
    }
}

/// Serialize an effect list. Every effect variant is `Serialize`, so the JS host
/// sees exactly what the native runtime's dispatcher sees.
fn to_json(effects: &[Effect]) -> String {
    serde_json::to_string(effects).unwrap_or_else(|_| "[]".to_string())
}

/// `Submission` needs `Deserialize` for the WASM entry point; it is derived in
/// `nexus-core`, and this compile-time assertion keeps that contract from being
/// dropped by accident.
const _: fn() = || {
    fn assert<'a, T: serde::Deserialize<'a>>() {}
    assert::<Submission>();
};
