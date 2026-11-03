//! Consumer-facing surfaces (task §7.1): "the hub must **not** parse job
//! payloads. Only edge adapters speak a dialect."
//!
//! Everything dialect-shaped lives here: OpenAI request parsing, the capability
//! requirements inferred from a request, the SSE chunk shapes, and the assembly
//! used for `stream: false`. Adding Whisper, sd.cpp or "run this and stream rows"
//! means adding a `kind` string plus a worker-side adapter — no hub change, and
//! nothing in [`crate::actor`] needs to know.
//!
//! The functions are pure (values in, values out, injected `created_ms`), so the
//! WASM host can produce the identical shapes and the tests assert exact JSON.

use std::collections::BTreeSet;

use serde_json::json;
use serde_json::Map;
use serde_json::Value;

use crate::config::BrokerConfig;
use crate::ids::JobId;
use crate::ids::TenantId;
use crate::model::JobView;
use crate::model::Priority;
use crate::model::Submission;
use crate::protocol::FinishKind;
use crate::protocol::Requires;
use crate::relay::ConsumerEvent;
use crate::store::content_idempotency_key;
use crate::time::Timestamp;

/// The two kinds the OpenAI adapter maps onto.
pub const KIND_CHAT: &str = "llm.chat";
/// `/v1/completions` kind.
pub const KIND_COMPLETION: &str = "llm.completion";

/// Characters-per-token heuristic for the prompt-size guard (task §8's "cheap
/// mitigation"). Deliberately crude: it gates admission, it does not schedule.
pub const CHARS_PER_TOKEN: u64 = 4;

/// A problem with a request that an adapter must catch before the actor sees it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{code}: {message}")]
pub struct AdapterError {
    /// HTTP status to answer with.
    pub status: u16,
    /// Stable machine code.
    pub code: &'static str,
    /// Human detail.
    pub message: String,
}

impl AdapterError {
    /// Build a 400 with a message.
    #[must_use]
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: 400,
            code: "invalid_request",
            message: message.into(),
        }
    }

    /// Build a 404.
    #[must_use]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: 404,
            code: "not_found",
            message: message.into(),
        }
    }
}

/// A parsed OpenAI request. Unknown fields are preserved verbatim so the worker
/// receives the request "mostly verbatim" (task §7.1) — the hub adds nothing and
/// removes nothing it does not have to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatRequest {
    /// Model name, from `model`.
    pub model: String,
    /// Whether the client asked for a stream.
    pub stream: bool,
    /// Which `kind` this maps to.
    pub kind: String,
    /// The original body, minus nothing.
    pub body: Value,
    /// Capabilities inferred from the body.
    pub requires: Requires,
    /// Estimated prompt tokens.
    pub prompt_tokens_estimate: u64,
}

impl ChatRequest {
    /// Parse a chat-completions or completions body.
    ///
    /// # Errors
    /// [`AdapterError`] when the body is not an object, has no `model`, or (for
    /// chat) has no `messages` / `input`.
    pub fn parse(body: &Value, kind: &str) -> Result<Self, AdapterError> {
        if !body.is_object() {
            return Err(AdapterError::bad_request("body must be a JSON object"));
        }
        let model = body
            .get("model")
            .and_then(Value::as_str)
            .filter(|m| !m.trim().is_empty())
            .ok_or_else(|| {
                AdapterError::bad_request("`model` is required and must be a non-empty string")
            })?
            .to_string();

        // `/v1/completions` carries `prompt`; chat carries `messages`. Accept
        // either so a client mixing the two is not silently dropped.
        let has_input = body
            .get("messages")
            .is_some_and(|m| m.is_array() && !m.as_array().expect("array").is_empty())
            || body.get("prompt").is_some_and(|p| !p.is_null());
        if !has_input {
            return Err(AdapterError::bad_request(
                "`messages` (or `prompt`) is required",
            ));
        }

        let stream = body.get("stream").and_then(Value::as_bool).unwrap_or(false);
        let requires = infer_requires(&model, body);
        let prompt_tokens_estimate = estimate_tokens(body);
        Ok(Self {
            model,
            stream,
            kind: kind.to_string(),
            body: body.clone(),
            requires,
            prompt_tokens_estimate,
        })
    }

    /// Turn the request into an actor submission.
    ///
    /// `idempotency_key_override` is the `Idempotency-Key` header; without it the
    /// content hash is used (task §5.2).
    #[must_use]
    pub fn to_submission(
        &self,
        cfg: &BrokerConfig,
        tenant: &TenantId,
        pool: crate::ids::PoolId,
        idempotency_key_override: Option<&str>,
        session_hint: Option<String>,
        priority: Priority,
    ) -> Submission {
        let kind_cfg = cfg.kind(&self.kind);
        let mut requires = self.requires.clone();
        if let Some(floor) = kind_cfg.map(|k| k.requires.clone()) {
            requires.merge(&floor);
        }
        let body_bytes = serde_json::to_vec(&self.body).unwrap_or_default();
        let idempotency_key = idempotency_key_override
            .map(str::to_string)
            .unwrap_or_else(|| content_idempotency_key(tenant.as_ref(), &self.kind, &body_bytes));
        Submission {
            kind: self.kind.clone(),
            pool,
            tenant: tenant.clone(),
            requires,
            payload: self.body.clone(),
            content_type: "application/json".to_string(),
            idempotency_key,
            session_hint,
            priority,
            // Declared by the registry, never by the client (task §5.3).
            idempotent: kind_cfg.map_or(false, |k| k.idempotent),
            streaming: kind_cfg.map_or(true, |k| k.streaming),
            // An SSE chat request *is* its subscription (§6): losing the socket
            // means nobody wants the tokens any more.
            cancel_on_consumer_gone: true,
        }
    }
}

/// Infer capability requirements from the request body: tools, vision, logprobs.
/// Each is a named capability a worker must advertise, so a request that needs
/// vision is refused immediately on a text-only pool (task §8).
#[must_use]
pub fn infer_requires(model: &str, body: &Value) -> Requires {
    let mut capabilities: BTreeSet<String> = BTreeSet::new();
    if body
        .get("tools")
        .is_some_and(|t| t.is_array() && !t.as_array().expect("array").is_empty())
    {
        capabilities.insert("tools".to_string());
    }
    if body
        .get("messages")
        .and_then(Value::as_array)
        .is_some_and(|messages| messages.iter().any(message_has_image_part))
    {
        capabilities.insert("vision".to_string());
    }
    if body
        .get("logprobs")
        .is_some_and(|l| !l.is_null() && l != &json!(false))
    {
        capabilities.insert("logprobs".to_string());
    }
    if body.get("response_format").is_some() || body.get("response_type").is_some() {
        capabilities.insert("structured_output".to_string());
    }
    let min_context_tokens = body
        .get("max_completion_tokens")
        .or(body.get("max_tokens"))
        .and_then(Value::as_u64);

    Requires {
        model: Some(model.to_string()),
        min_context_tokens,
        capabilities,
        kv_estimate_tokens: Some(estimate_tokens(body)),
    }
}

/// True when a message carries an image content part (either OpenAI's
/// `{type:"image_url"}` or a `{image: ...}` shorthand).
fn message_has_image_part(message: &Value) -> bool {
    let Some(parts) = message.get("content").and_then(Value::as_array) else {
        return false;
    };
    parts.iter().any(|part| {
        part.get("type").and_then(Value::as_str) == Some("image_url") || part.get("image").is_some()
    })
}

/// Rough prompt size in tokens: every string in the body, divided by
/// [`CHARS_PER_TOKEN`]. Walked generically so a `prompt` string, a `messages`
/// array and any dialect-specific text field are all counted.
#[must_use]
pub fn estimate_tokens(body: &Value) -> u64 {
    fn walk(value: &Value, acc: &mut u64) {
        match value {
            Value::String(s) => *acc += s.len() as u64,
            Value::Array(items) => items.iter().for_each(|i| walk(i, acc)),
            Value::Object(map) => map.iter().for_each(|(_, v)| walk(v, acc)),
            _ => {}
        }
    }
    let mut chars = 0u64;
    walk(body, &mut chars);
    chars / CHARS_PER_TOKEN
}

/// OpenAI chunk object id for a job: `chatcmpl-<job>` mirrors upstream's shape so
/// clients that log it stay useful.
#[must_use]
pub fn completion_id(job: &JobId) -> String {
    format!("chatcmpl-{}", job.as_ref())
}

/// The role delta sent **before** the job is claimed (task §7.2), so the client's
/// SDK unblocks immediately and sees a coherent assistant message even if the
/// stream later dies in band.
#[must_use]
pub fn role_chunk(job: &JobId, model: &str, created_ms: Timestamp) -> Value {
    json!({
        "id": completion_id(job),
        "object": "chat.completion.chunk",
        "created": created_ms / 1000,
        "model": model,
        "choices": [{ "index": 0, "delta": { "role": "assistant", "content": "" }, "finish_reason": null }],
    })
}

/// Encode one relayed delta as the JSON payload of an OpenAI SSE `data:` line.
///
/// A worker that already speaks OpenAI (the reference client relays engine chunks
/// verbatim) passes through **byte for byte** — no re-tokenization, so
/// `tool_calls` fragments, `reasoning_content` deltas and `finish_reason` survive
/// intact (acceptance test 14). A worker that sends a bare token is wrapped in a
/// minimal chunk, which is dialect work by the adapter, not by the spine. The
/// caller owns the SSE framing via [`crate::relay::sse`].
#[must_use]
pub fn openai_delta_payload(text: &str, job: &JobId, model: &str, created_ms: Timestamp) -> String {
    if let Ok(value) = serde_json::from_str::<Value>(text) {
        if value.is_object() && value.get("choices").is_some() {
            return text.to_string();
        }
    }
    serde_json::to_string(&json!({
        "id": completion_id(job),
        "object": "chat.completion.chunk",
        "created": created_ms / 1000,
        "model": model,
        "choices": [{ "index": 0, "delta": { "content": text }, "finish_reason": null }],
    }))
    .unwrap_or_else(|_| "null".to_string())
}

/// The terminal chunk of an OpenAI stream: `finish_reason` plus `usage`.
#[must_use]
pub fn finish_chunk(
    job: &JobId,
    model: &str,
    created_ms: Timestamp,
    finish: FinishKind,
    usage: Option<&Value>,
) -> Value {
    let mut chunk = json!({
        "id": completion_id(job),
        "object": "chat.completion.chunk",
        "created": created_ms / 1000,
        "model": model,
        "choices": [{ "index": 0, "delta": {}, "finish_reason": finish.as_str() }],
    });
    if let Some(usage) = usage {
        chunk["usage"] = usage.clone();
    }
    chunk
}

/// SSE error body for a stream that already started (task §7.2's in-band error).
#[must_use]
pub fn in_band_error(code: &str, message: &str, retryable: bool) -> Value {
    json!({ "error": { "type": code, "message": message, "retryable": retryable, "nexus_retryable": retryable } })
}

/// The JSON a consumer's `400`/`503`/`429` body carries, OpenAI-shaped so SDKs
/// render it sensibly.
#[must_use]
pub fn http_error(code: &str, message: &str, retryable: bool) -> Value {
    json!({ "error": { "type": code, "code": code, "message": message, "param": null, "retryable": retryable } })
}

/// Merge a relayed delta chunk into the accumulating message of a non-streaming
/// response: content concatenates, `tool_calls` merge per index, reasoning text
/// concatenates, `finish_reason` keeps the last non-null one.
/// Mutates `message` in place; the return value is `()`, so no `#[must_use]`.
pub fn merge_delta(message: &mut Value, delta: &Value) {
    if let Some(content) = delta.get("content").and_then(Value::as_str) {
        let existing = message
            .get("content")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        message["content"] = Value::String(format!("{existing}{content}"));
    }
    if let Some(reasoning) = delta.get("reasoning_content").and_then(Value::as_str) {
        let existing = message
            .get("reasoning_content")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        message["reasoning_content"] = Value::String(format!("{existing}{reasoning}"));
    }
    if let Some(calls) = delta.get("tool_calls").and_then(Value::as_array) {
        let mut merged: Vec<Value> = message
            .get("tool_calls")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for call in calls {
            let index = call.get("index").and_then(Value::as_u64).unwrap_or(0);
            match merged
                .iter_mut()
                .find(|slot| slot.get("index").and_then(Value::as_u64) == Some(index))
            {
                Some(slot) => {
                    // Arguments arrive as fragments that concatenate; name and id
                    // arrive once.
                    if let Some(name) = call
                        .get("function")
                        .and_then(|f| f.get("name"))
                        .and_then(Value::as_str)
                    {
                        slot["function"]["name"] = Value::String(name.to_string());
                    }
                    if let Some(args) = call
                        .get("function")
                        .and_then(|f| f.get("arguments"))
                        .and_then(Value::as_str)
                    {
                        let existing = slot
                            .get("function")
                            .and_then(|f| f.get("arguments"))
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string();
                        slot["function"]["arguments"] = Value::String(format!("{existing}{args}"));
                    }
                    if call.get("id").and_then(Value::as_str).is_some() {
                        slot["id"] = call["id"].clone();
                    }
                }
                None => {
                    let mut slot = call.clone();
                    slot["type"] = json!("function");
                    merged.push(slot);
                }
            }
        }
        message["tool_calls"] = Value::Array(merged);
    }
    if let Some(role) = delta.get("role").and_then(Value::as_str) {
        message["role"] = Value::String(role.to_string());
    } else if message.get("role").is_none() {
        message["role"] = json!("assistant");
    }
}

/// Assemble a full `chat.completion` from the relayed events, for `stream: false`
/// (acceptance test 17: the same content as the streamed response).
///
/// The events' delta payloads are parsed here — that is dialect work inside the
/// adapter, and it is why the spine never has to understand a chunk.
#[must_use]
pub fn assemble_completion(
    job: &JobId,
    model: &str,
    created_ms: Timestamp,
    events: &[ConsumerEvent],
) -> Value {
    let mut message = json!({ "role": "assistant" });
    let mut finish: Option<String> = None;
    let mut usage: Option<Value> = None;
    let mut logprobs: Option<Value> = None;

    for event in events {
        match event {
            ConsumerEvent::Delta { text, .. } => {
                if let Ok(chunk) = serde_json::from_str::<Value>(text) {
                    if let Some(choice) = chunk
                        .get("choices")
                        .and_then(Value::as_array)
                        .and_then(|c| c.first())
                    {
                        if let Some(delta) = choice.get("delta") {
                            merge_delta(&mut message, delta);
                        }
                        if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
                            finish = Some(reason.to_string());
                        }
                        if choice.get("logprobs").is_some() {
                            logprobs = choice.get("logprobs").cloned();
                        }
                    }
                } else {
                    // A bare token: still concatenate.
                    let existing = message
                        .get("content")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    message["content"] = Value::String(format!("{existing}{text}"));
                }
            }
            ConsumerEvent::Finish {
                finish: f,
                usage: u,
                ..
            } => {
                finish = Some(f.as_str().to_string());
                usage = u.clone();
            }
            ConsumerEvent::Error { code, message, .. } => {
                finish = Some("error".to_string());
                message_extra(&mut usage, code, message);
            }
            _ => {}
        }
    }
    if message.get("content").is_none() && message.get("tool_calls").is_none() {
        message["content"] = Value::String(String::new());
    }

    json!({
        "id": completion_id(job),
        "object": "chat.completion",
        "created": created_ms / 1000,
        "model": model,
        "choices": [{ "index": 0, "message": message, "finish_reason": finish.unwrap_or_else(|| "stop".to_string()) }],
        "usage": usage.unwrap_or(Value::Null),
    })
    .with_optional_logprobs(logprobs)
}

/// Attach `logprobs` to the assembled choice when the model produced them.
trait WithOptionalLogprobs {
    fn with_optional_logprobs(self, logprobs: Option<Value>) -> Value;
}

impl WithOptionalLogprobs for Value {
    fn with_optional_logprobs(mut self, logprobs: Option<Value>) -> Value {
        if let (Some(lp), Some(choice)) = (
            logprobs,
            self.get_mut("choices").and_then(Value::as_array_mut),
        ) {
            if let Some(choice) = choice.first_mut() {
                choice["logprobs"] = lp;
            }
        }
        self
    }
}

/// Record a terminal in-band error inside the `usage` slot of an assembled
/// response, so a non-streaming client can still tell success from failure.
fn message_extra(usage: &mut Option<Value>, code: &str, message: &str) {
    let mut map = Map::new();
    map.insert(
        "error".to_string(),
        json!({ "type": code, "message": message }),
    );
    *usage = Some(Value::Object(map));
}

/// Body of `POST /v1/jobs`'s `202 Accepted`.
#[must_use]
pub fn accepted_body(view: &JobView, artifact_path: Option<String>) -> Value {
    json!({
        "job_id": view.id.to_string(),
        "kind": view.kind,
        "pool": view.pool.to_string(),
        "state": format!("{:?}", view.phase),
        "attempt": view.attempt,
        "streaming": view.streaming,
        "idempotent": view.idempotent,
        "payload_sha256": view.payload_sha256,
        "payload_bytes": view.payload_bytes,
        "links": {
            "events": format!("/v1/jobs/{}/events", view.id),
            "result": format!("/v1/jobs/{}/result", view.id),
            "cancel": format!("/v1/jobs/{}", view.id),
            "artifact_upload": artifact_path,
        },
    })
}

/// Body of `GET /v1/jobs/{id}`: the derived job view plus timings.
#[must_use]
pub fn job_status_body(view: &JobView) -> Value {
    json!({
        "job_id": view.id.to_string(),
        "kind": view.kind,
        "pool": view.pool.to_string(),
        "state": format!("{:?}", view.phase),
        "terminal_reason": view.terminal.map(|t| t.as_str()),
        "retryable": view.terminal.map(|t| t.retryable()),
        "attempt": view.attempt,
        "priority": view.priority.as_str(),
        "payload_bytes": view.payload_bytes,
        "payload_sha256": view.payload_sha256,
        "requires": view.requires,
        "acked_seq": view.acked_seq,
        "gaps": view.gaps,
        "duplicate_frames": view.duplicates,
        "replay_frames": view.replay_frames,
        "consumers": view.consumers,
        "delivered_bytes": view.delivered_bytes,
        "lease": view.lease,
        "artifact": view.artifact,
        "usage": view.usage,
        "finish_reason": view.finish.map(|f| f.as_str()),
        "worker_error": view.worker_error,
        "timings": view.timings,
    })
}

/// A worker's `artifact_chunk` manifest, re-reading the JSON the worker put in
/// `Frame.meta`. Kept permissive: the manifest is the worker's description of an
/// upload the hub has already verified by measurement.
#[must_use]
pub fn declared_artifact(
    manifest: Option<&crate::protocol::ArtifactManifest>,
) -> (Option<String>, Option<u64>) {
    manifest
        .map(|m| (m.sha256.clone(), m.size_bytes))
        .unwrap_or((None, None))
}

/// The `GET /v1/models` document, derived from warm workers only (task §7.1).
#[must_use]
pub fn models_document(models: &BTreeSet<String>, created_ms: Timestamp) -> Value {
    let data: Vec<Value> = models
        .iter()
        .map(|model| {
            json!({
                "id": model,
                "object": "model",
                "created": created_ms / 1000,
                "owned_by": "nexus",
                // Not a claim about the weights: it is the hub's own readiness
                // signal, so a client can tell warm from loading capacity.
                "nexus": { "warm": true },
            })
        })
        .collect();
    json!({ "object": "list", "data": data })
}

/// `/healthz` body: degraded capacity is reported, never hidden (task §8, §11).
#[must_use]
pub fn health_body(
    pool: &str,
    ready: usize,
    loading: usize,
    queue_depth: usize,
    lease_count: usize,
) -> Value {
    json!({
        "status": if ready == 0 { "degraded" } else { "ok" },
        "pool": pool,
        "ready_workers": ready,
        "loading_workers": loading,
        "queue_depth": queue_depth,
        "leases": lease_count,
        "capacity_signal": if ready == 0 && loading > 0 { "cold_start" } else if ready == 0 { "no_workers" } else { "warm" },
    })
}

/// Convenience: the flush window bounds a relay uses, resolved from config.
#[must_use]
pub fn flush_thresholds(cfg: &BrokerConfig) -> (u64, usize) {
    (cfg.limits.flush_interval_ms, cfg.limits.flush_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::PoolId;
    use serde_json::json;

    fn cfg() -> BrokerConfig {
        let mut cfg = BrokerConfig::default();
        cfg.auth.allow_anonymous = true;
        cfg
    }

    #[test]
    fn parse_requires_the_model_and_an_input() {
        let err = ChatRequest::parse(
            &json!({"messages": [{"role": "user", "content": "hi"}]}),
            KIND_CHAT,
        )
        .expect_err("no model");
        assert_eq!(err.status, 400);
        assert_eq!(err.code, "invalid_request");

        let err = ChatRequest::parse(&json!({"model": "qwen38"}), KIND_CHAT).expect_err("no input");
        assert_eq!(err.code, "invalid_request");

        let err = ChatRequest::parse(&json!("a string"), KIND_CHAT).expect_err("not an object");
        assert_eq!(err.message, "body must be a JSON object");

        let req = ChatRequest::parse(
            &json!({"model": "qwen38", "prompt": "write a haiku", "max_tokens": 64}),
            KIND_COMPLETION,
        )
        .expect("completions body parses");
        assert_eq!(req.kind, KIND_COMPLETION);
        assert_eq!(req.model, "qwen38");
        assert_eq!(req.requires.min_context_tokens, Some(64));
    }

    #[test]
    fn capability_inference_is_exact() {
        let plain = infer_requires(
            "qwen38",
            &json!({"model":"qwen38","messages":[{"role":"user","content":"hi"}]}),
        );
        assert_eq!(plain.capabilities, BTreeSet::new());

        let tools = infer_requires(
            "qwen38",
            &json!({"model":"qwen38","messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function"}]}),
        );
        assert_eq!(tools.capabilities, BTreeSet::from(["tools".to_string()]));

        let vision = infer_requires(
            "qwen38",
            &json!({
                "model":"qwen38",
                "messages":[{"role":"user","content":[{"type":"text","text":"what is this"},{"type":"image_url","image_url":{"url":"http://x/y.png"}}]}]
            }),
        );
        assert_eq!(vision.capabilities, BTreeSet::from(["vision".to_string()]));

        let both = infer_requires(
            "qwen38",
            &json!({
                "model":"qwen38",
                "messages":[{"role":"user","content":[{"image":"base64..."}]}],
                "logprobs": true,
                "tools": [{"type":"function"}],
                "response_format": {"type":"json_object"}
            }),
        );
        assert_eq!(
            both.capabilities,
            BTreeSet::from([
                "tools".to_string(),
                "vision".to_string(),
                "logprobs".to_string(),
                "structured_output".to_string()
            ])
        );
        // `logprobs: false` must not claim the capability.
        let no_lp = infer_requires(
            "qwen38",
            &json!({"model":"qwen38","messages":[{"role":"user","content":"hi"}],"logprobs": false}),
        );
        assert!(!no_lp.capabilities.contains("logprobs"));
    }

    #[test]
    fn token_estimate_counts_every_string() {
        // Every string in the body is counted: "abcd" + "user" + "abcd" = 12
        // chars, which is 3 tokens at 4 chars/token.
        assert_eq!(
            estimate_tokens(&json!({"model":"abcd","messages":[{"role":"user","content":"abcd"}]})),
            3
        );
        assert_eq!(estimate_tokens(&json!({})), 0);
    }

    #[test]
    fn submission_takes_idempotency_from_the_header_or_the_content_hash() {
        let c = cfg();
        let req = ChatRequest::parse(
            &json!({"model":"qwen38","messages":[{"role":"user","content":"hi"}],"stream":true}),
            KIND_CHAT,
        )
        .expect("parse");
        let sub = req.to_submission(
            &c,
            &TenantId::new_unchecked("acme"),
            PoolId::new_unchecked("llm.chat/qwen38"),
            Some("key-123"),
            Some("sess-9".to_string()),
            Priority::Realtime,
        );
        assert_eq!(sub.idempotency_key, "key-123");
        assert_eq!(sub.session_hint.as_deref(), Some("sess-9"));
        assert_eq!(sub.priority, Priority::Realtime);
        assert!(sub.streaming && sub.idempotent && sub.cancel_on_consumer_gone);
        assert_eq!(sub.pool, PoolId::new_unchecked("llm.chat/qwen38"));
        // The registry's floor is merged in, so a client cannot under-declare.
        assert_eq!(sub.requires.model.as_deref(), Some("qwen38"));
        assert_eq!(sub.requires.min_context_tokens, Some(524_288));

        let hashed = req.to_submission(
            &c,
            &TenantId::new_unchecked("acme"),
            PoolId::new_unchecked("llm.chat/qwen38"),
            None,
            None,
            Priority::Default,
        );
        assert_eq!(
            hashed.idempotency_key.len(),
            64,
            "a content hash is a sha256"
        );
        let other_tenant = req.to_submission(
            &c,
            &TenantId::new_unchecked("other"),
            PoolId::new_unchecked("llm.chat/qwen38"),
            None,
            None,
            Priority::Default,
        );
        assert_ne!(
            hashed.idempotency_key, other_tenant.idempotency_key,
            "tenants must not collide"
        );
    }

    #[test]
    fn role_chunk_matches_the_openai_shape_exactly() {
        assert_eq!(
            role_chunk(&JobId::new_unchecked("job-1"), "qwen38", 1_700_000_000_000),
            json!({
                "id": "chatcmpl-job-1",
                "object": "chat.completion.chunk",
                "created": 1_700_000_000,
                "model": "qwen38",
                "choices": [{"index": 0, "delta": {"role": "assistant", "content": ""}, "finish_reason": null}]
            })
        );
    }

    #[test]
    fn a_worker_chunk_that_is_already_openai_passes_through_verbatim() {
        let chunk = r#"{"id":"chatcmpl-x","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"c1","function":{"name":"get_weather","arguments":"{\"ci"}}],"reasoning_content":"thinking"},"finish_reason":null}]}"#;
        assert_eq!(
            openai_delta_payload(chunk, &JobId::new_unchecked("job-1"), "qwen38", 0),
            chunk
        );
    }

    #[test]
    fn a_bare_token_is_wrapped_without_altering_its_text() {
        let wrapped = openai_delta_payload(
            "hello",
            &JobId::new_unchecked("job-1"),
            "qwen38",
            1_700_000_000_000,
        );
        let value: Value = serde_json::from_str(&wrapped).expect("valid json");
        assert_eq!(value["choices"][0]["delta"]["content"], json!("hello"));
        assert_eq!(value["id"], json!("chatcmpl-job-1"));
    }

    #[test]
    fn finish_chunk_carries_usage_untouched() {
        let usage =
            json!({"prompt_tokens": 10, "completion_tokens": 4, "extra_engine_field": "kept"});
        let chunk = finish_chunk(
            &JobId::new_unchecked("job-1"),
            "qwen38",
            1_700_000_000_000,
            FinishKind::ToolCalls,
            Some(&usage),
        );
        assert_eq!(chunk["choices"][0]["finish_reason"], json!("tool_calls"));
        assert_eq!(chunk["usage"], usage);
        assert!(finish_chunk(
            &JobId::new_unchecked("job-1"),
            "qwen38",
            0,
            FinishKind::Stop,
            None
        )
        .get("usage")
        .is_none());
    }

    #[test]
    fn assembly_reconstructs_content_tools_and_usage_identically_to_the_stream() {
        // Acceptance test 17: the non-streaming response carries the same content.
        let events = vec![
            ConsumerEvent::Delta { seq: 1, text: r#"{"choices":[{"index":0,"delta":{"role":"assistant","content":"Weath"},"finish_reason":null}]}"#.to_string() },
            ConsumerEvent::Delta { seq: 2, text: r#"{"choices":[{"index":0,"delta":{"content":"er: sunny"},"finish_reason":null}]}"#.to_string() },
            ConsumerEvent::Delta { seq: 3, text: r#"{"choices":[{"index":0,"delta":{"reasoning_content":"because"},"finish_reason":null}]}"#.to_string() },
            ConsumerEvent::Delta { seq: 4, text: r#"{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"get_weather","arguments":"{\"city\":"}}]},"finish_reason":null}]}"#.to_string() },
            ConsumerEvent::Delta { seq: 5, text: r#"{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"Paris\"}"}}]},"finish_reason":null}]}"#.to_string() },
            ConsumerEvent::Finish { seq: 6, finish: FinishKind::ToolCalls, usage: Some(json!({"completion_tokens": 12})) },
        ];
        let assembled = assemble_completion(
            &JobId::new_unchecked("job-1"),
            "qwen38",
            1_700_000_000_000,
            &events,
        );
        assert_eq!(assembled["object"], json!("chat.completion"));
        assert_eq!(
            assembled["choices"][0]["message"]["content"],
            json!("Weather: sunny")
        );
        assert_eq!(
            assembled["choices"][0]["message"]["reasoning_content"],
            json!("because")
        );
        assert_eq!(
            assembled["choices"][0]["message"]["tool_calls"][0]["id"],
            json!("call_1")
        );
        assert_eq!(
            assembled["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
            json!("get_weather")
        );
        assert_eq!(
            assembled["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"],
            json!("{\"city\":\"Paris\"}")
        );
        assert_eq!(
            assembled["choices"][0]["message"]["tool_calls"]
                .as_array()
                .expect("one call")
                .len(),
            1,
            "fragments must merge by index"
        );
        assert_eq!(
            assembled["choices"][0]["finish_reason"],
            json!("tool_calls")
        );
        assert_eq!(assembled["usage"]["completion_tokens"], json!(12));
    }

    #[test]
    fn assembly_of_an_error_stream_is_marked_not_silent() {
        let events = vec![
            ConsumerEvent::Delta {
                seq: 1,
                text: "partial".to_string(),
            },
            ConsumerEvent::Error {
                code: "worker_gone_partial".into(),
                message: "died mid-stream".into(),
                retryable: false,
                through_seq: Some(1),
            },
        ];
        let assembled = assemble_completion(&JobId::new_unchecked("job-2"), "qwen38", 0, &events);
        assert_eq!(
            assembled["choices"][0]["message"]["content"],
            json!("partial")
        );
        assert_eq!(assembled["choices"][0]["finish_reason"], json!("error"));
        assert_eq!(
            assembled["usage"]["error"]["type"],
            json!("worker_gone_partial")
        );
    }

    #[test]
    fn models_health_and_status_bodies_are_exact() {
        assert_eq!(
            models_document(&BTreeSet::from(["qwen38".to_string()]), 1_700_000_000_000),
            json!({"object":"list","data":[{"id":"qwen38","object":"model","created":1_700_000_000,"owned_by":"nexus","nexus":{"warm":true}}]})
        );
        assert_eq!(
            health_body("llm.chat/qwen38", 0, 1, 2, 0),
            json!({"status":"degraded","pool":"llm.chat/qwen38","ready_workers":0,"loading_workers":1,"queue_depth":2,"leases":0,"capacity_signal":"cold_start"})
        );
        assert_eq!(health_body("p", 1, 0, 0, 0)["status"], json!("ok"));
        assert_eq!(
            http_error("capability_unsatisfied", "no vision worker", false)["error"]["type"],
            json!("capability_unsatisfied")
        );
        assert_eq!(
            in_band_error("worker_gone_partial", "x", false)["error"]["retryable"],
            json!(false)
        );
        assert_eq!(declared_artifact(None), (None, None));
    }
}
