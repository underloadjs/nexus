//! Consumer-facing HTTP surface (task §7): the OpenAI-shaped adapters, the
//! generic job API, and the operational routes.
//!
//! # The open-early contract, and where the 503 still fits (task §7.2)
//!
//! The handler sends `Submit` and then **awaits the first verdict** before it
//! writes a single body byte:
//!
//! * `Accepted` → headers + the role delta go out immediately (before any worker
//!   has claimed the job, since claiming is pull-driven), then `: ping` comments
//!   while it waits, then verbatim deltas.
//! * `Reject`   → a plain JSON error with the actor's status, including
//!   `503 + Retry-After` when no worker became ready in the grace window, and
//!   `400` for an unsatisfiable capability request.
//!
//! That ordering is what makes acceptance test 6 true: no bytes are spent until
//! the actor has decided the request is serviceable.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::http::header;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::response::sse::Event;
use axum::response::sse::KeepAlive;
use axum::response::sse::Sse;
use axum::response::IntoResponse;
use axum::response::Response;
use nexus_core::adapters;
use nexus_core::auth::bearer;
use nexus_core::ids::ConsumerId;
use nexus_core::ids::JobId;
use nexus_core::ids::PoolId;
use nexus_core::ids::TenantId;
use nexus_core::model::Priority;
use nexus_core::protocol::CancelReason;
use nexus_core::relay::parse_resume_seq;
use nexus_core::ActorMsg;
use nexus_core::ConsumerEvent;
use nexus_core::Submission;
use serde::Deserialize;
use serde_json::json;
use serde_json::Value;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;

use crate::state;
use crate::state::Gate;
use crate::state::Hub;
use crate::state::JobRelay;
use crate::state::RelayItem;

/// Header carrying a conversation id for affinity (task §17.4 resolution: the hub
/// trusts a client-supplied `messages` array plus this hint; it stores no history).
const SESSION_HEADER: &str = "x-session-id";
/// Explicit idempotency key (task §5.2).
const IDEMPOTENCY_HEADER: &str = "idempotency-key";
// The `?pool=` override has no constant here on purpose: its wire name is the
// `ConsumerQuery::pool` field name, so a second spelling could only drift.
/// The SSE replay cursor (`task §7.5`). `http::header` ships no constant for it,
/// so the name is declared once here instead of being re-typed at every read.
const LAST_EVENT_ID: axum::http::HeaderName = axum::http::HeaderName::from_static("last-event-id");

/// How long a non-streaming request waits for its answer before giving up.
const AGGREGATE_TIMEOUT_SECS: u64 = 300;

/// Query parameters shared by the consumer endpoints.
#[derive(Debug, Deserialize, Default)]
pub struct ConsumerQuery {
    /// Pool override; the kind normally decides.
    #[serde(default)]
    pub pool: Option<String>,
    /// Replay floor for `/events` (`Last-Event-ID` wins).
    #[serde(default)]
    pub from_seq: Option<String>,
    /// `Last-Event-ID`, forwarded by proxies that cannot set headers.
    #[serde(default)]
    pub last_event_id: Option<String>,
}

/// Body of `POST /v1/jobs`.
#[derive(Debug, Deserialize)]
pub struct JobRequest {
    /// Registry kind, e.g. `image.workflow`.
    pub kind: String,
    /// Optional pool override.
    #[serde(default)]
    pub pool: Option<String>,
    /// Opaque payload, forwarded to the worker verbatim.
    pub payload: Value,
    /// Optional capability requirements the client declares.
    #[serde(default)]
    pub requires: Option<nexus_core::protocol::Requires>,
    /// Optional priority class name.
    #[serde(default)]
    pub priority: Option<String>,
}

/// `POST /v1/chat/completions`.
pub async fn chat_completions(
    State(hub): State<Arc<Hub>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    openai_endpoint(hub, &headers, &body, adapters::KIND_CHAT).await
}

/// `POST /v1/completions`.
pub async fn completions(State(hub): State<Arc<Hub>>, headers: HeaderMap, body: Bytes) -> Response {
    openai_endpoint(hub, &headers, &body, adapters::KIND_COMPLETION).await
}

/// Shared implementation of the two OpenAI-shaped routes.
async fn openai_endpoint(hub: Arc<Hub>, headers: &HeaderMap, raw: &[u8], kind: &str) -> Response {
    let tenant = match tenant_of(&hub, headers) {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    let Ok(body) = serde_json::from_slice::<Value>(raw) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "bad_json",
            "the request body is not valid JSON",
            false,
        );
    };
    let Ok(request) = adapters::ChatRequest::parse(&body, kind) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "the body is not a usable chat or completions request",
            false,
        );
    };
    let Some(pool_id) = pool_for_kind(&hub, kind) else {
        return json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "pool_unconfigured",
            "this kind has no pool in the configuration",
            true,
        );
    };
    let session = header_value(headers, SESSION_HEADER).map(str::to_string);
    // Tenant-namespaced, for the reason given in `create_job`.
    let idempotency =
        header_value(headers, IDEMPOTENCY_HEADER).map(|key| format!("{}:{key}", tenant.as_ref()));
    let submission = request.to_submission(
        &hub.cfg,
        &tenant,
        pool_id.clone(),
        idempotency.as_deref(),
        session,
        hub.cfg.tenant_priority(&tenant),
    );
    let stream = request.stream;
    let model = request.model.clone();
    let (job, consumer, handle, _relay, gate, live, replay) =
        submit(&hub, &pool_id, submission).await;

    // Await the actor's first verdict before writing anything (task §7.2).
    let verdict = tokio::time::timeout(
        Duration::from_millis(hub.cfg.limits.no_worker_grace_ms + 500),
        gate,
    )
    .await;
    match verdict {
        Ok(Ok(Gate::Accepted)) => {}
        Ok(Ok(Gate::Reject {
            status,
            code,
            message,
            retry_after_secs,
        })) => {
            let mut response = json_error(status, &code, &message, retry_after_secs.is_some());
            if let Some(secs) = retry_after_secs {
                response.headers_mut().insert(
                    header::RETRY_AFTER,
                    secs.to_string().parse().expect("seconds parse"),
                );
            }
            response.headers_mut().insert(
                "x-nexus-job",
                job.as_ref().parse().expect("job id is a header value"),
            );
            return response;
        }
        // A timeout here is not "the worker is slow": it is the hub failing to
        // answer, and the honest answer is a retryable 503, not a hung stream.
        Ok(Ok(Gate::Closed)) | Ok(Err(_)) | Err(_) => {
            let mut response = json_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "hub_unavailable",
                "the hub did not answer in time; retry",
                true,
            );
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("2"));
            return response;
        }
    }
    if !stream {
        return aggregate_response(&hub, job, consumer, handle, live, replay, &model).await;
    }

    let created = hub.wall_ms();
    let (out_tx, out_rx) =
        mpsc::channel::<Result<Event, Infallible>>(hub.cfg.limits.consumer_backpressure_frames);
    // Headers first, then the role delta, then a keepalive while queued (§7.2).
    let role = Event::default()
        .json_data(adapters::role_chunk(&job, &model, created))
        .expect("chunk serializes");
    let _ = out_tx.send(Ok(role)).await;
    tokio::spawn(pump_sse(
        handle,
        consumer,
        job.clone(),
        model,
        created,
        live,
        replay,
        out_tx,
    ));
    let mut response = Sse::new(ReceiverStream::new(out_rx))
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(1))
                .text("ping"),
        )
        .into_response();
    add_stream_headers(&mut response);
    response_headers(&job, &mut response);
    response
}

/// Tag a response with the job it belongs to, so a client can poll or replay it
/// after the socket closes (§7.5).
fn response_headers(job: &JobId, response: &mut Response) {
    if let Ok(value) = HeaderValue::from_str(job.as_ref()) {
        response.headers_mut().insert("x-nexus-job", value);
    }
}

/// Collect the whole stream and answer once, for `stream: false` (test 17).
async fn aggregate_response(
    hub: &Arc<Hub>,
    job: JobId,
    consumer: ConsumerId,
    handle: Arc<state::PoolHandle>,
    live: broadcast::Receiver<RelayItem>,
    replay: mpsc::Receiver<RelayItem>,
    model: &str,
) -> Response {
    let mut events: Vec<ConsumerEvent> = Vec::new();
    let mut chunks: Vec<String> = Vec::new();
    // A joiner's buffered prefix arrives on its private queue, so the assembled
    // answer contains every delta rather than only the ones after the join.
    let mut live = live;
    let mut replay = replay;
    while let Some(item) = replay.recv().await {
        if let RelayItem::Event(event) = item {
            if let ConsumerEvent::Delta { text, .. } = &event {
                chunks.push(text.clone());
            }
            events.push(event);
        }
    }
    let mut outcome = "end";
    let deadline = tokio::time::sleep(Duration::from_secs(AGGREGATE_TIMEOUT_SECS));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            biased;
            item = live.recv() => match item {
                Ok(RelayItem::Event(event)) => {
                    let terminal = matches!(event, ConsumerEvent::Finish { .. } | ConsumerEvent::Error { .. });
                    if let ConsumerEvent::Delta { text, .. } = &event {
                        chunks.push(text.clone());
                    }
                    events.push(event);
                    if terminal {
                        outcome = "terminal";
                        break;
                    }
                }
                Ok(RelayItem::End) => break,
                Ok(RelayItem::Comment(_)) => {}
                Err(_) => break,
            },
            _ = &mut deadline => {
                outcome = "timeout";
                break;
            }
        }
    }
    // §6: the request socket was the subscription. Whether or not an answer
    // arrived, the actor has to learn it closed, or a `stream: false` client
    // that gave up leaves the job occupying a worker slot.
    let _ = state::send(
        handle.as_ref(),
        ActorMsg::Detach {
            job: job.clone(),
            consumer,
        },
    )
    .await;
    handle.forget_relay(&job).await;
    if events
        .iter()
        .any(|e| matches!(e, ConsumerEvent::Error { .. }))
    {
        return json_error(
            StatusCode::BAD_GATEWAY,
            "stream_incomplete",
            "the worker stopped before finishing",
            false,
        );
    }
    if outcome != "terminal" {
        return json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "no_ready_worker",
            "the hub produced no answer in time; retry",
            true,
        );
    }
    let assembled = adapters::assemble_completion(&job, model, hub.wall_ms(), &events);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-nexus-chunks", chunks.len().to_string())
        .body(axum::body::Body::from(
            serde_json::to_string(&assembled).expect("assembled object serializes"),
        ))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// `POST /v1/jobs` — the generic async job API (task §7.1, §7.3).
pub async fn create_job(
    State(hub): State<Arc<Hub>>,
    headers: HeaderMap,
    Query(query): Query<ConsumerQuery>,
    body: Bytes,
) -> Response {
    let tenant = match tenant_of(&hub, &headers) {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    let Ok(request) = serde_json::from_slice::<JobRequest>(&body) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "bad_json",
            "expected {kind, payload}",
            false,
        );
    };
    let Some(kind_cfg) = hub.cfg.kind(&request.kind).cloned() else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "unknown_kind",
            &format!("kind {:?} is not registered", request.kind),
            false,
        );
    };
    let pool_name = request
        .pool
        .as_deref()
        .or(query.pool.as_deref())
        .unwrap_or(&kind_cfg.pool);
    if pool_name != kind_cfg.pool {
        return json_error(
            StatusCode::BAD_REQUEST,
            "wrong_pool",
            "the pool does not serve this kind",
            false,
        );
    }
    let Ok(pool_id) = PoolId::parse(pool_name) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_pool",
            "the pool name is not in the safe alphabet",
            false,
        );
    };
    let payload_bytes = serde_json::to_vec(&request.payload).unwrap_or_default();
    let mut requires = request
        .requires
        .clone()
        .unwrap_or_else(|| kind_cfg.requires.clone());
    if requires.kv_estimate_tokens.is_none() {
        requires.kv_estimate_tokens = Some(adapters::estimate_tokens(&request.payload));
    }
    // The idempotency map is hub-wide, so an explicit key is namespaced by
    // tenant: guessing another tenant's `Idempotency-Key` must not attach to
    // their job and stream their prompt's answer back (§9 tenancy).
    let idempotency = match header_value(&headers, IDEMPOTENCY_HEADER) {
        Some(key) => format!("{}:{key}", tenant.as_ref()),
        None => nexus_core::store::content_idempotency_key(
            tenant.as_ref(),
            &request.kind,
            &payload_bytes,
        ),
    };
    let submission = Submission {
        kind: request.kind.clone(),
        pool: pool_id.clone(),
        tenant: tenant.clone(),
        requires,
        payload: request.payload.clone(),
        content_type: "application/json".to_string(),
        idempotency_key: idempotency,
        session_hint: header_value(&headers, SESSION_HEADER).map(str::to_string),
        priority: request
            .priority
            .as_deref()
            .map(Priority::from_name)
            .unwrap_or_else(|| hub.cfg.tenant_priority(&tenant)),
        idempotent: kind_cfg.idempotent,
        streaming: kind_cfg.streaming,
        // A job API submission is detached on purpose: the client may poll later
        // (task §7.3), so losing the submitting socket is not a cancellation.
        cancel_on_consumer_gone: false,
    };
    let (job, _consumer, _handle, relay, gate, _live, _replay) =
        submit(&hub, &pool_id, submission).await;
    match tokio::time::timeout(
        Duration::from_millis(hub.cfg.limits.no_worker_grace_ms + 500),
        gate,
    )
    .await
    {
        Ok(Ok(Gate::Accepted)) => {}
        Ok(Ok(Gate::Reject {
            status,
            code,
            message,
            retry_after_secs,
        })) => {
            let mut response = json_error(status, &code, &message, retry_after_secs.is_some());
            if let Some(secs) = retry_after_secs {
                response.headers_mut().insert(
                    header::RETRY_AFTER,
                    secs.to_string().parse().expect("seconds parse"),
                );
            }
            return response;
        }
        _ => {
            let mut response = json_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "hub_unavailable",
                "the hub did not answer in time; retry",
                true,
            );
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("2"));
            return response;
        }
    }
    let _ = relay;
    let view = hub.job_snapshot(&job).await;
    let artifact_path = view.as_ref().map(|_| format!("/v1/artifacts/art-{}", job));
    let body = adapters::accepted_body(
        &view.unwrap_or_else(|| {
            placeholder_view(job.clone(), pool_id.clone(), request.kind.clone())
        }),
        artifact_path,
    );
    // `handle` stays alive through the hub; the client polls `/events`.
    json_response(StatusCode::ACCEPTED, &body)
}

/// `GET /v1/jobs` — a coarse listing, bounded by the hub's own retention.
pub async fn list_jobs(State(hub): State<Arc<Hub>>, headers: HeaderMap) -> Response {
    let tenant = match tenant_of(&hub, &headers) {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    let mut jobs = Vec::new();
    for handle in hub.pools.values() {
        for job in handle.followed_jobs().await {
            if let Some(view) = hub.job_snapshot(&job).await {
                // A listing is still a read of somebody's prompt metadata, so it
                // is filtered to the calling tenant (§9).
                if view.tenant == tenant {
                    jobs.push(adapters::job_status_body(&view));
                }
            }
        }
    }
    json_response(StatusCode::OK, &json!({ "object": "list", "data": jobs }))
}

/// `GET /v1/jobs/{id}` — authoritative state, read through the actor.
pub async fn job_status(
    State(hub): State<Arc<Hub>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let tenant = match tenant_of(&hub, &headers) {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    let Ok(job) = JobId::parse(&id) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_id",
            "job id is not in the safe alphabet",
            false,
        );
    };
    match hub.job_snapshot(&job).await {
        Some(view) if view.tenant == tenant => {
            json_response(StatusCode::OK, &adapters::job_status_body(&view))
        }
        // A foreign job reads as `404`, not `403`: job ids are otherwise
        // enumerable across tenants by probing statuses.
        Some(_) => json_error(
            StatusCode::NOT_FOUND,
            "unknown_job",
            "no such job for this tenant",
            false,
        ),
        None => json_error(
            StatusCode::NOT_FOUND,
            "unknown_job",
            "no such job, or its retention window passed",
            false,
        ),
    }
}

/// `DELETE /v1/jobs/{id}` — cancel (task §6).
pub async fn cancel_job(
    State(hub): State<Arc<Hub>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let tenant = match tenant_of(&hub, &headers) {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    let Ok(job) = JobId::parse(&id) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_id",
            "job id is not in the safe alphabet",
            false,
        );
    };
    let Some(handle) = hub.route_job(&job).await else {
        return json_error(StatusCode::NOT_FOUND, "unknown_job", "no such job", false);
    };
    // Any valid API key used to be able to cancel anyone's job: the tenant was
    // authenticated and then discarded. The owning tenant is authoritative
    // because the view comes back through the actor that owns the job.
    match hub.job_snapshot(&job).await {
        Some(view) if view.tenant == tenant => {}
        Some(_) => {
            return json_error(
                StatusCode::NOT_FOUND,
                "unknown_job",
                "no such job for this tenant",
                false,
            )
        }
        None => return json_error(StatusCode::NOT_FOUND, "unknown_job", "no such job", false),
    }
    if state::send(
        handle.as_ref(),
        ActorMsg::Cancel {
            job: job.clone(),
            reason: CancelReason::ConsumerGone,
        },
    )
    .await
    .is_err()
    {
        return json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "hub_unavailable",
            "the pool actor is draining",
            true,
        );
    }
    json_response(
        StatusCode::OK,
        &json!({ "job_id": job.to_string(), "cancelling": true }),
    )
}

/// `GET /v1/jobs/{id}/events` — SSE progress for any kind (task §7.1).
pub async fn job_events(
    State(hub): State<Arc<Hub>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<ConsumerQuery>,
) -> Response {
    let tenant = match tenant_of(&hub, &headers) {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    let Ok(job) = JobId::parse(&id) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_id",
            "job id is not in the safe alphabet",
            false,
        );
    };
    let Some(handle) = hub.route_job(&job).await else {
        return json_error(StatusCode::NOT_FOUND, "unknown_job", "no such job", false);
    };
    match hub.job_snapshot(&job).await {
        Some(view) if view.tenant == tenant => {}
        Some(_) => {
            return json_error(
                StatusCode::NOT_FOUND,
                "unknown_job",
                "no such job for this tenant",
                false,
            )
        }
        None => return json_error(StatusCode::NOT_FOUND, "unknown_job", "no such job", false),
    }
    let from_seq = parse_resume_seq(
        headers
            .get(&LAST_EVENT_ID)
            .and_then(|v| v.to_str().ok())
            .or(query.from_seq.as_deref())
            .or(query.last_event_id.as_deref()),
    );
    let relay = handle
        .relay(&job, hub.cfg.limits.consumer_backpressure_frames)
        .await;
    // The replay lands in this consumer's private queue, then the live fan-out
    // takes over: a joiner's buffer must not be broadcast to everyone.
    let (replay_tx, replay_rx) = mpsc::channel::<RelayItem>(hub.cfg.limits.replay_frames.max(16));
    *relay.replay_target.lock().await = Some(replay_tx);
    let consumer = hub.ids.consumer_id();
    if state::send(
        handle.as_ref(),
        ActorMsg::Attach {
            job: job.clone(),
            consumer: consumer.clone(),
            from_seq,
        },
    )
    .await
    .is_err()
    {
        return json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "hub_unavailable",
            "the pool actor is draining",
            true,
        );
    }
    let (out_tx, out_rx) =
        mpsc::channel::<Result<Event, Infallible>>(hub.cfg.limits.consumer_backpressure_frames);
    tokio::spawn(pump_relay(
        handle,
        consumer,
        job.clone(),
        relay.events.subscribe(),
        replay_rx,
        out_tx,
    ));
    let mut response = Sse::new(ReceiverStream::new(out_rx))
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(1))
                .text("ping"),
        )
        .into_response();
    add_stream_headers(&mut response);
    response_headers(&job, &mut response);
    response
}

/// `GET /v1/jobs/{id}/result` — the produced artifact bytes, resumable (test 15).
pub async fn job_result(
    State(hub): State<Arc<Hub>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(range): Query<crate::io::RangeQuery>,
) -> Response {
    let tenant = match tenant_of(&hub, &headers) {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    let Ok(job) = JobId::parse(&id) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_id",
            "job id is not in the safe alphabet",
            false,
        );
    };
    let Some(view) = hub.job_snapshot(&job).await else {
        return json_error(StatusCode::NOT_FOUND, "unknown_job", "no such job", false);
    };
    if view.tenant != tenant {
        return json_error(
            StatusCode::NOT_FOUND,
            "unknown_job",
            "no such job for this tenant",
            false,
        );
    }
    let Some(artifact) = view.artifact.clone() else {
        return json_error(
            if view.terminal.map(|t| t.retryable()).unwrap_or(true) {
                StatusCode::CONFLICT
            } else {
                StatusCode::NOT_FOUND
            },
            "artifact_unavailable",
            "this job has not produced a complete artifact",
            true,
        );
    };
    let from = range.from.unwrap_or(0);
    let to = range.to.unwrap_or(artifact.size_bytes);
    crate::io::serve_artifact(
        &artifact.path,
        artifact.size_bytes,
        &artifact.content_type,
        artifact.stored_sha256.as_deref(),
        from,
        to,
    )
    .await
}

/// `GET /v1/models` — live, from ready workers only (task §7.1).
pub async fn models(State(hub): State<Arc<Hub>>) -> Response {
    let readiness = hub.readiness().await;
    let models: std::collections::BTreeSet<String> = readiness.models.iter().cloned().collect();
    json_response(
        StatusCode::OK,
        &adapters::models_document(&models, hub.wall_ms()),
    )
}

/// `GET /healthz` — degraded capacity is reported, never hidden (task §8, §11).
pub async fn healthz(State(hub): State<Arc<Hub>>) -> Response {
    let readiness = hub.readiness().await;
    let body = json!({
        "status": if readiness.ready_workers == 0 { "degraded" } else { "ok" },
        "ready_workers": readiness.ready_workers,
        "loading_workers": readiness.loading_workers,
        "queue_depth": readiness.queue_depth,
        "mailbox_pending": readiness.mailbox_pending,
        "warm_models": readiness.models,
        "capacity_signal": if readiness.ready_workers == 0 && readiness.loading_workers > 0 { "cold_start" } else if readiness.ready_workers == 0 { "no_workers" } else { "warm" },
        "protocol": nexus_core::PROTOCOL_VERSION,
    });
    json_response(
        if readiness.ready_workers == 0 {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::OK
        },
        &body,
    )
}

/// `GET /metrics` — Prometheus text (task §13's mandatory set).
pub async fn metrics(State(hub): State<Arc<Hub>>) -> Response {
    let text = hub.render_metrics().await;
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; version=0.0.4")
        .body(axum::body::Body::from(text))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

// ---------------------------------------------------------------------- plumbing

/// Resolve the caller's tenant from the API key, failing closed (task §9: an
/// API key per consumer, never one secret shared across both directions).
/// Returns the response to send when the key is missing or unknown.
pub(crate) fn tenant_of(hub: &Arc<Hub>, headers: &HeaderMap) -> Result<TenantId, Response> {
    let key = bearer(
        headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok()),
    )
    .or_else(|| header_value(headers, "x-api-key"));
    hub.api_keys.verify(key).map_err(|err| {
        json_error(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            &err.to_string(),
            false,
        )
    })
}

/// Pool that serves a kind.
fn pool_for_kind(hub: &Arc<Hub>, kind: &str) -> Option<PoolId> {
    hub.cfg.kind(kind).and_then(|k| PoolId::parse(&k.pool).ok())
}

/// A header value as `&str`.
fn header_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|v| !v.trim().is_empty())
}

/// Send a submission and register everything its answer needs **before** the
/// actor is told about the job.
///
/// Two orderings were previously wrong and are the reason this returns more than
/// a gate:
///   * the broadcast subscription must exist before `Submit` is sent, because
///     `tokio::sync::broadcast` delivers only what is published afterwards — a
///     worker frame relayed in the same tick that answered `Accepted` used to
///     land in that gap and be lost (§16.14's "same content" claim);
///   * this consumer's private replay queue must be installed before `Submit`,
///     because an idempotent attach delivers its buffered frames through it
///     (`Accepted { attached: true, replayed }`). With no receiver installed the
///     dispatcher dropped them, so the second client saw only whatever came
///     after it joined (§16.7).
///
/// The actor's `Accepted` / `ReplayTo` handler drops the replay sender once it
/// has written the buffered prefix, which is what closes this channel and lets
/// the pumps fall through to live events.
#[allow(clippy::type_complexity)]
async fn submit(
    hub: &Arc<Hub>,
    pool: &PoolId,
    submission: Submission,
) -> (
    JobId,
    ConsumerId,
    Arc<state::PoolHandle>,
    Arc<JobRelay>,
    oneshot::Receiver<Gate>,
    broadcast::Receiver<RelayItem>,
    mpsc::Receiver<RelayItem>,
) {
    let job = hub.ids.job_id();
    let consumer = hub.ids.consumer_id();
    let handle = hub
        .pool(pool)
        .expect("pool handle exists for a configured pool");
    let relay = handle
        .relay(&job, hub.cfg.limits.consumer_backpressure_frames)
        .await;
    let (gate_tx, gate_rx) = oneshot::channel();
    *relay.gate.lock().await = Some(gate_tx);
    let live = relay.events.subscribe();
    let (replay_tx, replay_rx) = mpsc::channel::<RelayItem>(hub.cfg.limits.replay_frames.max(16));
    *relay.replay_target.lock().await = Some(replay_tx);
    // The §5 read-mostly index is written here, once, so a later `GET
    // /v1/jobs/{id}` can be routed without a second source of job state.
    hub.bind_job(&job, pool).await;
    if state::send(
        handle.as_ref(),
        ActorMsg::Submit {
            job: job.clone(),
            submission,
            consumer: consumer.clone(),
        },
    )
    .await
    .is_err()
    {
        // The actor is gone: resolve the gate ourselves so the caller answers 503
        // instead of waiting for the timeout.
        relay.resolve(Gate::Closed).await;
    }
    (
        job,
        consumer,
        Arc::clone(handle),
        relay,
        gate_rx,
        live,
        replay_rx,
    )
}

/// Translate relay items into SSE blocks for the OpenAI surface.
///
/// Order matters twice over: this consumer's replay prefix (the
/// `Accepted { attached: true }` path, §5.2) goes out before the live events,
/// and seqs already delivered are skipped so a replay that overlaps the fan-out
/// cannot print the same delta twice (§16.4/§16.8). When the pump ends — because
/// the stream finished or because the consumer vanished — it sends `Detach`:
/// for a streaming kind that is what makes the last consumer leaving fan out a
/// `Cancel` to the worker (§6). Without it, a client that closes its socket
/// leaves the GPU decoding for nobody.
async fn pump_sse(
    handle: Arc<state::PoolHandle>,
    consumer: ConsumerId,
    job: JobId,
    model: String,
    created: u64,
    mut live: broadcast::Receiver<RelayItem>,
    mut replay: mpsc::Receiver<RelayItem>,
    tx: mpsc::Sender<Result<Event, Infallible>>,
) {
    let mut through: u64 = 0;
    let mut finished = false;
    while let Some(item) = replay.recv().await {
        match item {
            RelayItem::End => {
                finished = true;
                break;
            }
            RelayItem::Comment(_) => {}
            RelayItem::Event(event) => {
                let terminal = matches!(
                    event,
                    ConsumerEvent::Finish { .. } | ConsumerEvent::Error { .. }
                );
                if !send_block(&job, &model, created, event, &mut through, &tx).await {
                    finished = true;
                    break;
                }
                if terminal {
                    let _ = tx.send(Ok(Event::default().data("[DONE]"))).await;
                    finished = true;
                    break;
                }
            }
        }
    }
    drop(replay);
    if !finished {
        loop {
            let item = match live.recv().await {
                Ok(item) => item,
                // A lagging consumer: end the stream in band rather than pretend
                // the text continued (task §7.5).
                Err(broadcast::error::RecvError::Lagged(missed)) => {
                    let payload = adapters::in_band_error(
                        "consumer_lagged",
                        &format!("{missed} frames were dropped by the fan-out"),
                        false,
                    );
                    let _ = tx
                        .send(Ok(Event::default()
                            .event("error")
                            .json_data(payload)
                            .expect("error object serializes")))
                        .await;
                    break;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            };
            match item {
                RelayItem::Comment(_text) => {
                    // axum's `KeepAlive` already emits the `: ping` comments on
                    // this response, so an actor keepalive is not duplicated.
                }
                RelayItem::End => break,
                RelayItem::Event(event) => {
                    let terminal = matches!(
                        event,
                        ConsumerEvent::Finish { .. } | ConsumerEvent::Error { .. }
                    );
                    if !send_block(&job, &model, created, event, &mut through, &tx).await {
                        break;
                    }
                    if terminal {
                        // The OpenAI contract ends with the `[DONE]` sentinel.
                        let _ = tx.send(Ok(Event::default().data("[DONE]"))).await;
                        break;
                    }
                }
            }
        }
    }
    let _ = state::send(
        handle.as_ref(),
        ActorMsg::Detach {
            job: job.clone(),
            consumer,
        },
    )
    .await;
}

/// Write one consumer event as an OpenAI chunk. Returns false once the client is
/// gone. `through` tracks the highest seq already written so an overlapping
/// replay is skipped; `Gap` and `Error` carry no content seq and always go out.
async fn send_block(
    job: &JobId,
    model: &str,
    created: u64,
    event: ConsumerEvent,
    through: &mut u64,
    tx: &mpsc::Sender<Result<Event, Infallible>>,
) -> bool {
    let seq = match &event {
        ConsumerEvent::Delta { seq, .. }
        | ConsumerEvent::Event { seq, .. }
        | ConsumerEvent::Log { seq, .. }
        | ConsumerEvent::Artifact { seq, .. }
        | ConsumerEvent::Finish { seq, .. } => Some(*seq),
        _ => None,
    };
    if let Some(seq) = seq {
        if seq <= *through {
            return true;
        }
        *through = seq;
    }
    let block = match &event {
        ConsumerEvent::Delta { text, seq } => Event::default()
            .id(seq.to_string())
            .data(adapters::openai_delta_payload(text, job, model, created)),
        ConsumerEvent::Event { seq, payload } => Event::default()
            .id(seq.to_string())
            .event("event")
            .json_data(payload.clone())
            .expect("event data serializes"),
        ConsumerEvent::Log { seq, text } => Event::default()
            .id(seq.to_string())
            .event("log")
            .data(text.clone()),
        ConsumerEvent::Artifact { seq, manifest } => Event::default()
            .id(seq.to_string())
            .event("artifact")
            .json_data(manifest.clone())
            .expect("manifest serializes"),
        ConsumerEvent::Gap { expected, got } => Event::default()
            .event("gap")
            .json_data(json!({ "expected": expected, "got": got }))
            .expect("gap object serializes"),
        ConsumerEvent::Finish { seq, finish, usage } => Event::default()
            .id(seq.to_string())
            .json_data(adapters::finish_chunk(
                job,
                model,
                created,
                *finish,
                usage.as_ref(),
            ))
            .expect("finish chunk serializes"),
        ConsumerEvent::Error {
            code,
            message,
            retryable,
            ..
        } => Event::default()
            .event("error")
            .json_data(adapters::in_band_error(code, message, *retryable))
            .expect("error object serializes"),
        other => Event::default()
            .json_data(other.clone())
            .expect("event serializes"),
    };
    tx.send(Ok(block)).await.is_ok()
}

/// §7.2: intermediaries must not buffer the stream. `text/event-stream` comes
/// from axum's `Sse`; without these two headers a proxy (or nginx's default)
/// holds the deltas back and the "streaming" API is not streaming.
fn add_stream_headers(response: &mut Response) {
    response
        .headers_mut()
        .insert("x-accel-buffering", HeaderValue::from_static("no"));
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
}

/// Translate relay items for the generic job API: one `event:` name per kind, and
/// `id:` set to the worker seq so `Last-Event-ID` resumes exactly (task §7.5).
///
/// The private replay queue is drained first (the dispatcher closes it once the
/// buffered prefix has been written), then the live fan-out takes over, skipping
/// any seq the replay already delivered. Like the OpenAI pump, it sends `Detach`
/// when the consumer goes away, so a `/events` watcher that disconnects is not
/// counted as an attached consumer forever.
async fn pump_relay(
    handle: Arc<state::PoolHandle>,
    consumer: ConsumerId,
    job: JobId,
    mut live: broadcast::Receiver<RelayItem>,
    mut replay: mpsc::Receiver<RelayItem>,
    tx: mpsc::Sender<Result<Event, Infallible>>,
) {
    let mut through: u64 = 0;
    while let Some(item) = replay.recv().await {
        if !forward(item, &tx, &mut through).await {
            let _ = state::send(handle.as_ref(), ActorMsg::Detach { job, consumer }).await;
            return;
        }
    }
    drop(replay);
    loop {
        let item = match live.recv().await {
            Ok(item) => item,
            Err(broadcast::error::RecvError::Lagged(missed)) => {
                let _ = tx
                    .send(Ok(Event::default()
                        .event("gap")
                        .json_data(json!({ "dropped": missed }))
                        .expect("gap serializes")))
                    .await;
                break;
            }
            Err(broadcast::error::RecvError::Closed) => break,
        };
        if !forward(item, &tx, &mut through).await {
            break;
        }
    }
    let _ = state::send(handle.as_ref(), ActorMsg::Detach { job, consumer }).await;
}

/// Forward one relay item; false means the consumer went away or the stream
/// reached a terminal event. `through` de-duplicates the live fan-out against a
/// replay that overlapped it; `Gap` and `Error` carry no content seq, so they are
/// never suppressed.
async fn forward(
    item: RelayItem,
    tx: &mpsc::Sender<Result<Event, Infallible>>,
    through: &mut u64,
) -> bool {
    match item {
        RelayItem::End => false,
        RelayItem::Comment(_text) => {
            // Comments are produced by axum's `KeepAlive`; forwarding the actor's
            // keepalive as a named event would confuse a generic client.
            true
        }
        RelayItem::Event(event) => {
            let seq = match &event {
                ConsumerEvent::Delta { seq, .. }
                | ConsumerEvent::Event { seq, .. }
                | ConsumerEvent::Log { seq, .. }
                | ConsumerEvent::Artifact { seq, .. }
                | ConsumerEvent::Finish { seq, .. } => Some(*seq),
                _ => None,
            };
            if let Some(seq) = seq {
                if seq <= *through {
                    return true;
                }
                *through = seq;
            }
            let name = event.event_name().to_string();
            let terminal = matches!(
                event,
                ConsumerEvent::Finish { .. } | ConsumerEvent::Error { .. }
            );
            let mut builder = Event::default()
                .event(&name)
                .json_data(event)
                .expect("event serializes");
            if let Some(seq) = seq {
                builder = builder.id(seq.to_string());
            }
            let sent = tx.send(Ok(builder)).await.is_ok();
            if terminal {
                return false;
            }
            sent
        }
    }
}

/// A JSON response with the right content type.
fn json_response(status: StatusCode, body: &Value) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(
            serde_json::to_string(body).expect("value serializes"),
        ))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// An OpenAI-shaped error body (task §7.1: clients should be able to read it).
fn json_error(status: StatusCode, code: &str, message: &str, retryable: bool) -> Response {
    json_response(status, &adapters::http_error(code, message, retryable))
}

/// Placeholder view when a snapshot probe timed out: enough for a `202` body.
fn placeholder_view(job: JobId, pool: PoolId, kind: String) -> nexus_core::JobView {
    nexus_core::JobView {
        id: job,
        pool,
        kind,
        tenant: TenantId::new_unchecked("unknown"),
        phase: nexus_core::JobPhase::Queued,
        attempt: 1,
        terminal: None,
        priority: Priority::Default,
        payload_bytes: 0,
        payload_sha256: String::new(),
        requires: nexus_core::protocol::Requires::default(),
        timings: nexus_core::model::JobTimings::default(),
        acked_seq: 0,
        gaps: 0,
        duplicates: 0,
        replay_frames: 0,
        consumers: 0,
        delivered_bytes: 0,
        lease: None,
        artifact: None,
        usage: None,
        finish: None,
        worker_error: None,
        streaming: false,
        idempotent: true,
    }
}
