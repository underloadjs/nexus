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
    /// NEX-6.6's explicit opt-in, **required** on this route (spec §9.1 item 3:
    /// detachment "must be stated as such on the request; it never applies to a
    /// plain 'stream me the answer' submission").
    ///
    /// Only `true` is accepted here, and the two refusals are separate facts:
    ///   * absent — nothing was stated. An omitted field is not a declaration, so
    ///     defaulting it would hand every plain `POST /v1/jobs` detached-lifetime
    ///     semantics as a route artifact, which is precisely what NEX-6.6 forbids.
    ///     Refused with `400 detach_required`.
    ///   * `true` — stated: "post it now, I will pick the result up later". The task
    ///     keeps running when nothing is attached, bounded by
    ///     `limits.detached_orphan_ttl_ms`.
    ///   * `false` — the plain "stream me the answer" contract of NEX-6.1, which
    ///     this route cannot honour: it answers `202` and hangs up, so a coupled
    ///     submission would be orphaned and deleted before the client could poll it.
    ///     Refused with the same code rather than silently picking one reading.
    ///
    /// `GET /v1/consumer` is where `detach: false` is meaningful, because there the
    /// connection really is the subscription; on that face the field stays optional
    /// and defaults to attached (`consumer_ws.rs:375-382`).
    ///
    /// `Option<bool>` rather than `bool` with `#[serde(default)]`: a plain `bool`
    /// would make an absent field a serde failure, and the route would answer
    /// `400 bad_json` for a body that is perfectly valid JSON. The refusal belongs
    /// in the handler, where the code and the message can name the contract.
    #[serde(default)]
    pub detach: Option<bool>,
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

    // §5.2 at the runtime level, on the surface that actually carries the answer: a key
    // this hub has already routed is **followed**, not re-run. See `create_job` for the
    // full reason this is decided on the face rather than left to the actor — the
    // actor's duplicate branch replays on the original id, which is not the id this
    // request would have registered its relay and gate under, and the retry used to
    // time out as `503 hub_unavailable` while the original's replay was dropped.
    //
    // One caveat worth stating where it happens: the actor's own
    // `nexus_idempotent_attach_total` no longer moves for these retries, because the
    // actor is never told about a duplicate this hub can answer. The hub cannot
    // compensate — `PoolGauges::from_actor` (state.rs:213-222) overwrites the mirrored
    // registry from the actor on every dispatch, so a hub-side increment would be
    // erased a tick later.
    if let Some((_pool, existing)) = hub.idempotency_target(&submission.idempotency_key).await {
        if let Some((consumer, handle, live, replay)) = join_existing_job(&hub, &existing).await {
            // No verdict wait, unlike a fresh submission (§7.2): an attach emits only
            // `ReplayTo` (`actor.rs:1228-1260`), and the record it follows is already
            // past `Pending`, so its stream is open by definition — the same reading
            // `GET /v1/jobs/{id}/events` takes.
            let follow = Follow {
                handle,
                consumer,
                job: existing.clone(),
                live,
                replay,
            };
            if !stream {
                // `false`: a joiner does not own the relay it borrowed (see the
                // parameter's documentation).
                return aggregate_response(&hub, follow, &model, false).await;
            }
            return stream_completion(&hub, follow, model).await;
        }
        // Declining means the mirrored record is gone (NEX-6.3 deleted it, or its
        // retention lapsed). Drop the straggler and let `submit` start the fresh
        // execution NEX-6.3 asks for, re-mirroring the key on the way.
        hub.forget_idempotency(&submission.idempotency_key).await;
    }
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
            abandon_submission(&hub, &handle, &job).await;
            let mut response = json_error(status, &code, &message, retry_after_secs.is_some());
            if let Some(secs) = retry_after_secs {
                response
                    .headers_mut()
                    .insert(header::RETRY_AFTER, retry_after(secs));
            }
            response
                .headers_mut()
                .insert("x-nexus-job", header_of(job.as_ref()));
            return response;
        }
        // A timeout here is not "the worker is slow": it is the hub failing to
        // answer, and the honest answer is a retryable 503, not a hung stream.
        Ok(Ok(Gate::Closed)) | Ok(Err(_)) | Err(_) => {
            abandon_submission(&hub, &handle, &job).await;
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
    let follow = Follow {
        handle,
        consumer,
        job: job.clone(),
        live,
        replay,
    };
    if !stream {
        // The request owns this job's relay: nothing else will ever follow an id that
        // was minted for this one call.
        return aggregate_response(&hub, follow, &model, true).await;
    }
    stream_completion(&hub, follow, model).await
}

/// Open the OpenAI SSE body for one followed job: headers, the role delta, then the
/// pump that carries the rest (§7.2's ordering, §16.14's verbatim relay).
///
/// Split out of `openai_endpoint` because the idempotent-join path needs the identical
/// sequence for a job this request did not create; two copies of "role chunk before the
/// first keepalive" is how the two surfaces would drift.
async fn stream_completion(hub: &Arc<Hub>, follow: Follow, model: String) -> Response {
    let job = follow.job.clone();
    let created = hub.wall_ms();
    let (out_tx, out_rx) =
        mpsc::channel::<Result<Event, Infallible>>(hub.cfg.limits.consumer_backpressure_frames);
    // Headers first, then the role delta, then a keepalive while queued (§7.2).
    let role = json_data_event(adapters::role_chunk(&job, &model, created));
    let _ = out_tx.send(Ok(role)).await;
    tokio::spawn(pump_sse(follow, model, created, out_tx));
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

/// The frame number an event carries, or `None` for the ones that have none (`Meta`,
/// `Gap`, `Error`).
///
/// Extracted from `send_block` so the aggregating path can apply the **same** overlap rule
/// the streaming pumps do: a joiner receives its buffered prefix on its private queue *and*
/// the same seqs on the fan-out, and printing them twice is §16.4's duplicate-delta bug.
/// Before the idempotent join path existed this could not show up — a fresh submission has
/// no buffered prefix — which is why only the pumps guarded it.
fn event_seq(event: &ConsumerEvent) -> Option<u64> {
    match event {
        ConsumerEvent::Delta { seq, .. }
        | ConsumerEvent::Event { seq, .. }
        | ConsumerEvent::Log { seq, .. }
        | ConsumerEvent::Artifact { seq, .. }
        | ConsumerEvent::Finish { seq, .. } => Some(*seq),
        _ => None,
    }
}

/// Fold one relayed event into an aggregated answer, skipping any seq this consumer has
/// already contributed (see [`event_seq`]). Returns whether the event was terminal, which
/// is what ends `aggregate_response`'s live loop.
fn collect_event(
    event: ConsumerEvent,
    events: &mut Vec<ConsumerEvent>,
    chunks: &mut Vec<String>,
    through: &mut u64,
) -> bool {
    if let Some(seq) = event_seq(&event) {
        if seq <= *through {
            return false;
        }
        *through = seq;
    }
    if let ConsumerEvent::Delta { text, .. } = &event {
        chunks.push(text.clone());
    }
    let terminal = matches!(
        &event,
        ConsumerEvent::Finish { .. } | ConsumerEvent::Error { .. }
    );
    events.push(event);
    terminal
}

/// Collect the whole stream and answer once, for `stream: false` (test 17).
///
/// `prune_relay` is the difference between a request that **owns** the relay it is
/// reading and one that borrowed it. A fresh submission mints a job id that nothing
/// else will ever follow, so its entry goes with the request. An idempotent join does
/// **not** own it: the relay belongs to the job, and removing the map entry while the
/// original submission's pump is still attached moves the fan-out to a fresh relay
/// under a live stream (`PoolHandle::relay` creates on demand), which would silence the
/// original consumer's deltas.
async fn aggregate_response(
    hub: &Arc<Hub>,
    follow: Follow,
    model: &str,
    prune_relay: bool,
) -> Response {
    let Follow {
        handle,
        consumer,
        job,
        mut live,
        mut replay,
    } = follow;
    let mut events: Vec<ConsumerEvent> = Vec::new();
    let mut chunks: Vec<String> = Vec::new();
    // The joiner's overlap watermark, same rule as the streaming pumps: the buffered
    // prefix arrives on the private queue and the fan-out repeats those seqs.
    let mut through: u64 = 0;
    // A joiner's buffered prefix arrives on its private queue, so the assembled
    // answer contains every delta rather than only the ones after the join.
    let mut outcome = "end";
    // The buffered prefix can already carry the terminal event: a retry of a job that
    // finished while the hub still keeps its ring. Stopping there is what `pump_sse` does
    // for the streaming surface, and for the same reason — waiting for a second terminal on
    // the fan-out would answer `503 no_ready_worker` for an answer that is already complete.
    let mut replayed_terminal = false;
    while let Some(item) = replay.recv().await {
        if let RelayItem::Event(event) = item {
            if collect_event(event, &mut events, &mut chunks, &mut through) {
                replayed_terminal = true;
                outcome = "terminal";
                break;
            }
        }
    }
    if !replayed_terminal {
        let deadline = tokio::time::sleep(Duration::from_secs(AGGREGATE_TIMEOUT_SECS));
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                biased;
                item = live.recv() => match item {
                    Ok(RelayItem::Event(event)) => {
                        if collect_event(event, &mut events, &mut chunks, &mut through) {
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
    if prune_relay {
        handle.forget_relay(&job).await;
    }
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
        // An assembled completion is a `Value`, so serialization cannot fail here;
        // the `"{}"` fallback is the house style for that case (`io.rs:968`), not a
        // silent downgrade of a real error path.
        .body(axum::body::Body::from(
            serde_json::to_string(&assembled).unwrap_or_else(|_| "{}".to_string()),
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
    // NEX-6.6's letter: on this route the field is required, and only `true` is a
    // valid answer. The two refusals are separate facts that share one code, because
    // they are the same lesson: this route answers `202` and hangs up, so it can
    // neither *declare* detachment on a silent client's behalf (absent) nor honour the
    // coupled contract (`false`) — a coupled task posted here would be orphaned and
    // deleted before the client could poll it. Surfaces that do hold the connection
    // open are named in the message, because "then how do I stream the answer?" is the
    // next question a client asks.
    if !matches!(request.detach, Some(true)) {
        return json_error(
            StatusCode::BAD_REQUEST,
            "detach_required",
            "this route keeps no subscription; post with \"detach\": true, or use a surface that holds the connection open (the streaming completion routes, or GET /v1/consumer)",
            false,
        );
    }
    // Admission — kind, pool, requirements, priority, idempotency — is shared with
    // the consumer WebSocket so both faces refuse the same task the same way
    // (§5.4). `?pool=` is HTTP-only, hence the override argument.
    let payload_bytes = serde_json::to_vec(&request.payload).unwrap_or_default();
    let fallback_key =
        nexus_core::store::content_idempotency_key(tenant.as_ref(), &request.kind, &payload_bytes);
    let (mut submission, pool_id) = match build_submission(
        &hub,
        &tenant,
        &request,
        query.pool.as_deref(),
        header_value(&headers, IDEMPOTENCY_HEADER),
        fallback_key,
    ) {
        Ok(built) => built,
        Err((status, code, message)) => return json_error(status, code, &message, false),
    };
    submission.session_hint = header_value(&headers, SESSION_HEADER).map(str::to_string);
    // §5.2 at the runtime level: a key this hub has already routed is not new work, and
    // the honest answer to a retry is the record that already exists — its id, its
    // state, its links. Answering it here rather than letting the actor do it matters
    // because the actor's duplicate branch replays on the **original** id
    // (`actor.rs:1045-1064`, `Effect::Accepted { job: existing, attached: true, .. }`)
    // while this face has to register its relay and its verdict gate under the id it
    // mints (`submit`): a verdict that names a different job never resolves that gate,
    // and the retry used to time out as `503 hub_unavailable`.
    //
    // NEX-6.3's delete-on-orphan is not weakened: the mirror is pruned by
    // `Effect::JobForgotten` (`state.rs`'s arm for it), expires on the actor's own
    // `limits.idempotency_ttl_ms`, and the record is re-read through the actor below
    // before it is trusted. A task that was orphaned and deleted has neither a mirror
    // entry nor a snapshot, so its retry runs fresh.
    if let Some((_pool, existing)) = hub.idempotency_target(&submission.idempotency_key).await {
        match hub.job_snapshot(&existing).await {
            // Same tenant by construction — mirrored keys are namespaced
            // `{tenant}:{key}` in `build_submission` — but the view is the proof, and a
            // mismatch means the entry is not this caller's to reuse.
            Some(view) if view.tenant == tenant => {
                // Same shape as the fresh `202` below, built from the actor's view of
                // the job that is answering: the client's links point at the task that
                // exists, not at the id this request would have minted.
                let artifact_path = Some(format!("/v1/artifacts/art-{existing}"));
                return json_response(
                    StatusCode::ACCEPTED,
                    &adapters::accepted_body(&view, artifact_path),
                );
            }
            Some(_) | None => {
                // The record is gone (NEX-6.3 deleted it, or its retention lapsed) or it
                // belongs to somebody else: drop the straggler and submit as new work.
                hub.forget_idempotency(&submission.idempotency_key).await;
            }
        }
    }
    let (job, consumer, handle, _relay, gate, _live, _replay) =
        submit(&hub, &pool_id, submission).await;
    match tokio::time::timeout(
        Duration::from_millis(hub.cfg.limits.no_worker_grace_ms + 500),
        gate,
    )
    .await
    {
        Ok(Ok(Gate::Accepted)) => {
            // The phantom consumer, removed. `ActorMsg::Submit` always registers
            // the submitting consumer, but on this route nobody pumps that
            // subscription — the request ends as soon as the `202` is written. Left
            // in `record.consumers` it made the task look watched forever, which is
            // what kept NEX-6.3's deletion and NEX-6.6's subscriber-less window
            // from ever applying to an async job (`actor.rs:1296` for the detach,
            // and the `consumers.is_empty()` test of the NEX-6.6 sweep at
            // `actor.rs:3232-3246`). Detaching here is what makes `detach: true`
            // mean what it says: watched by nobody, bounded by
            // `limits.detached_orphan_ttl_ms`, and a later `/events` attach
            // restarts the window. The OpenAI streaming routes do **not** do this:
            // there the pump is the subscription and its own `Detach` is the event.
            let _ = state::send(
                handle.as_ref(),
                ActorMsg::Detach {
                    job: job.clone(),
                    consumer,
                },
            )
            .await;
        }
        Ok(Ok(Gate::Reject {
            status,
            code,
            message,
            retry_after_secs,
        })) => {
            abandon_submission(&hub, &handle, &job).await;
            let mut response = json_error(status, &code, &message, retry_after_secs.is_some());
            if let Some(secs) = retry_after_secs {
                response
                    .headers_mut()
                    .insert(header::RETRY_AFTER, retry_after(secs));
            }
            return response;
        }
        _ => {
            abandon_submission(&hub, &handle, &job).await;
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
    // The task is accepted and detached; the client polls `/events` or the
    // consumer WebSocket to watch it, and each of those subscriptions has its own
    // lifetime (`state.rs`'s relay fan-out is per job, not per request).
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
        Follow {
            handle,
            consumer,
            job: job.clone(),
            live: relay.events.subscribe(),
            replay: replay_rx,
        },
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
//
// `result_large_err` measures the cost of moving a big `Err` out of a return
// value. `Response` is an opaque ~128-byte axum type, but every caller here
// returns the error verbatim (`Err(response) => return response`) instead of
// matching on it, so the only alternative — `Box<Response>` — adds an allocation
// on the unauthenticated path and an `into_response()` hop in nine places for a
// move the compiler already elides. Scoped to this signature for that reason.
#[allow(clippy::result_large_err)]
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

// ------------------------------------------------------------------ shared admission

/// Resolve a generic job posting into the `Submission` the actor sees, and report
/// the same refusals the route would have given: `unknown_kind`, `wrong_pool`,
/// `invalid_pool`.
///
/// Shared by `POST /v1/jobs` and the consumer WebSocket (`consumer_ws.rs`) because
/// §5.4 requires the two faces to refuse the same way: one task, one admission
/// rule, whatever the transport. The OpenAI-shaped adapters do **not** call this —
/// their kind comes from the route and their payload is translated, so their
/// resolution genuinely differs (`adapters::openai_submission`).
///
/// `fallback_key` is what deduplication uses when the caller supplied no explicit
/// key: the HTTP route passes a content hash ("same prompt, same answer"), the
/// WebSocket passes a fresh per-submit id, because a socket that posts the same
/// payload twice means two tasks unless it asked otherwise. An explicit key is
/// namespaced by tenant either way: guessing another tenant's `Idempotency-Key`
/// must not attach to their job and stream their prompt's answer back (§9).
pub(crate) fn build_submission(
    hub: &Arc<Hub>,
    tenant: &TenantId,
    request: &JobRequest,
    pool_override: Option<&str>,
    idempotency_key: Option<&str>,
    fallback_key: String,
) -> Result<(Submission, PoolId), (StatusCode, &'static str, String)> {
    let Some(kind_cfg) = hub.cfg.kind(&request.kind).cloned() else {
        return Err((
            StatusCode::BAD_REQUEST,
            "unknown_kind",
            format!("kind {:?} is not registered", request.kind),
        ));
    };
    let pool_name = request
        .pool
        .as_deref()
        .or(pool_override)
        .unwrap_or(&kind_cfg.pool);
    if pool_name != kind_cfg.pool {
        return Err((
            StatusCode::BAD_REQUEST,
            "wrong_pool",
            "the pool does not serve this kind".to_string(),
        ));
    }
    let Ok(pool_id) = PoolId::parse(pool_name) else {
        return Err((
            StatusCode::BAD_REQUEST,
            "invalid_pool",
            "the pool name is not in the safe alphabet".to_string(),
        ));
    };
    let mut requires = request
        .requires
        .clone()
        .unwrap_or_else(|| kind_cfg.requires.clone());
    if requires.kv_estimate_tokens.is_none() {
        requires.kv_estimate_tokens = Some(adapters::estimate_tokens(&request.payload));
    }
    let idempotency = match idempotency_key {
        Some(key) => format!("{}:{key}", tenant.as_ref()),
        None => fallback_key,
    };
    Ok((
        Submission {
            kind: request.kind.clone(),
            pool: pool_id.clone(),
            tenant: tenant.clone(),
            requires,
            payload: request.payload.clone(),
            content_type: "application/json".to_string(),
            idempotency_key: idempotency,
            // Session affinity is a header concept; the WebSocket carries no
            // session hint, so the actor's affinity simply does not apply there.
            session_hint: None,
            priority: request
                .priority
                .as_deref()
                .map(Priority::from_name)
                .unwrap_or_else(|| hub.cfg.tenant_priority(tenant)),
            idempotent: kind_cfg.idempotent,
            streaming: kind_cfg.streaming,
            // NEX-6.6, read off the request rather than off the route: a declared
            // detached task survives every consumer leaving, and a plain
            // submission does not. `create_job` has already refused the
            // combination it cannot honour, so on the HTTP route this is only
            // ever reached with the detached reading.
            cancel_on_consumer_gone: matches!(request.detach, Some(false)),
        },
        pool_id,
    ))
}

/// One consumer's view of one job: everything a pump needs in order to follow it
/// and to say so when the consumer goes away.
///
/// All three pumps — the OpenAI SSE body, the `/events` SSE body, and the
/// consumer WebSocket's per-task stream — take this instead of a fistful of loose
/// arguments, which is what keeps the three surfaces from drifting on what "the
/// consumer left" means: same fields, same `Detach` at the end.
pub(crate) struct Follow {
    /// The pool actor that owns the job.
    pub(crate) handle: Arc<state::PoolHandle>,
    /// The consumer identity registered for this socket.
    pub(crate) consumer: ConsumerId,
    /// Job being followed.
    pub(crate) job: JobId,
    /// The job's live fan-out.
    pub(crate) live: broadcast::Receiver<RelayItem>,
    /// This consumer's private replay queue, closed by the dispatcher once the
    /// buffered prefix has been written.
    pub(crate) replay: mpsc::Receiver<RelayItem>,
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
pub(crate) async fn submit(
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
    // Infallible by invariant: `pool` came out of `build_submission`, which only
    // ever returns a pool the validated config registers a kind for, and
    // `Hub::new` builds one handle per configured pool. A missing handle here is a
    // broken config/registry pairing, not an input a caller can recover from, so
    // the fail-stop stays and only the lint is scoped (same shape as `io.rs:285`).
    #[allow(clippy::expect_used)]
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
    // The hub-side mirror of the actor's §5.2 key table (`state.rs`'s `idempotency`),
    // written with the id this face registered everything under. That mirror is what
    // lets a later retry find this job instead of minting a second id the actor's
    // answer would never name; `idempotency_target` reads it, and
    // `Effect::JobForgotten` prunes it with the record.
    hub.remember_idempotency(&submission.idempotency_key, pool, &job)
        .await;
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

/// Undo everything `submit` installed for a submission the actor refused or never
/// answered: the routing entry, the relay, and the idempotency row (NEX-7.2 — no per-task
/// hub state that outlives the task).
///
/// All three are written *before* the actor is told, because the verdict's replay needs
/// somewhere to land (see `submit`'s ordering comment). A submission that never became a
/// job therefore owns entries the actor will never prune itself: it has no record, so
/// neither `Effect::Terminal` nor `Effect::JobForgotten` can ever name its id. Without
/// this, every refused `POST /v1/jobs` and every refused completion leaves a relay in the
/// pool's map plus a row in each of the two hub-side indexes for the life of the process —
/// the consumer WebSocket already prunes its own refusals (`consumer_ws.rs:487-496`), and
/// this is that same step on the HTTP faces.
///
/// The gate-timeout arm is the one judgement call: an actor that answered nothing may
/// still hold the record, and dropping the *route* makes it unpollable for the rest of its
/// retention. That is the trade the WebSocket already makes, and the honest direction — the
/// client has been told the hub did not answer, and a route to a task the hub has disowned
/// is worse than the `404` that admits it.
async fn abandon_submission(hub: &Arc<Hub>, handle: &Arc<state::PoolHandle>, job: &JobId) {
    hub.forget_job(job).await;
    handle.forget_relay(job).await;
    // The third map `submit` writes. Pruning by job (rather than by the submission's key)
    // is deliberate: the reverse index only ever points at the row for this job, so a row
    // a newer submission has meanwhile taken over is left alone.
    hub.forget_idempotency_for_job(job).await;
}

/// Follow the execution an idempotency key already owns instead of submitting a second
/// one: the same three registrations `GET /v1/jobs/{id}/events` makes (relay, this
/// consumer's private replay queue, `ActorMsg::Attach` from seq 0) and nothing else.
///
/// `submit`'s shape is deliberately **not** reproduced: there is no gate, because an attach
/// emits only `Effect::ReplayTo` (`actor.rs:1228-1260`) and never `Accepted`, so a face
/// that waited for a verdict here would wait out the grace window for a job that is already
/// running. The record being past `Pending` is what makes its stream open by definition —
/// the reading `/events` has always taken, and the reason a queued original answers with
/// keepalives rather than a `503` here as well.
///
/// `None` means the mirrored record is no longer there: `route_job` lost it when the actor
/// emitted `Effect::JobForgotten`, or the snapshot probe found nothing between the mirror
/// lookup and now. The caller then submits normally, which is exactly NEX-6.3's "a deleted
/// task re-runs" — and it closes the one gap a blind attach would leave, because attaching
/// to a record the actor has dropped closes the replay queue with nothing in it and leaves
/// the pump parked on a fan-out that can no longer produce events.
///
/// Two joins racing on one job inherit `/events`' pre-existing behaviour: the second
/// overwrite of `replay_target` can strand the first joiner's buffered prefix. Only the
/// ring's contents are at stake, and every consumer still gets the live tail.
#[allow(clippy::type_complexity)]
pub(crate) async fn join_existing_job(
    hub: &Arc<Hub>,
    job: &JobId,
) -> Option<(
    ConsumerId,
    Arc<state::PoolHandle>,
    broadcast::Receiver<RelayItem>,
    mpsc::Receiver<RelayItem>,
)> {
    // The mirrored pool is only a hint; `job_index` is what owns the routing, and a job
    // the actor has forgotten has no entry left to follow.
    let handle = hub.route_job(job).await?;
    // Guarded against the actor's retention rather than trusting the mirror's TTL: the
    // two expiry clocks (`limits.idempotency_ttl_ms` here and in the actor) agree to the
    // millisecond, but a record deleted by NEX-6.3 and a key that has not yet expired is
    // a real combination.
    hub.job_snapshot(job).await?;
    let relay = handle
        .relay(job, hub.cfg.limits.consumer_backpressure_frames)
        .await;
    let live = relay.events.subscribe();
    let (replay_tx, replay_rx) = mpsc::channel::<RelayItem>(hub.cfg.limits.replay_frames.max(16));
    *relay.replay_target.lock().await = Some(replay_tx);
    let consumer = hub.ids.consumer_id();
    if state::send(
        handle.as_ref(),
        ActorMsg::Attach {
            job: job.clone(),
            consumer: consumer.clone(),
            from_seq: 0,
        },
    )
    .await
    .is_err()
    {
        // The pool actor is draining: undo what was just registered, so a caller that
        // falls back to a fresh submission is not shadowed by a relay nobody will replay.
        handle.forget_relay(job).await;
        return None;
    }
    Some((consumer, handle, live, replay_rx))
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
///
/// NEX-6.1 is why the live loop also waits on `tx.closed()`: hyper drops a parked
/// response body as soon as the client hangs up (measured, not assumed —
/// `runtime_e2e::an_idle_sse_hangup_cancels_the_worker_without_a_failed_write`
/// pins the behaviour end to end), and dropping the body drops this channel's
/// receiver. Before that arm the loss surfaced only when a push into the body
/// failed, which on an idle stream is never: the task sat on a worker slot until
/// the TCP stack or the hard cap gave up.
async fn pump_sse(
    follow: Follow,
    model: String,
    created: u64,
    tx: mpsc::Sender<Result<Event, Infallible>>,
) {
    let Follow {
        handle,
        consumer,
        job,
        mut live,
        mut replay,
    } = follow;
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
        // NEX-6.1's independent notice, raced against the event source: as soon as
        // the response body is gone this future resolves and the loop exits, and
        // the `Detach` below is what turns it into the supplier `Cancel`.
        let mut gone = std::pin::pin!(tx.closed());
        loop {
            let item = tokio::select! {
                biased;
                () = &mut gone => break,
                result = live.recv() => match result {
                    Ok(item) => item,
                    // A lagging consumer: end the stream in band rather than pretend
                    // the text continued (task §7.5).
                    Err(broadcast::error::RecvError::Lagged(missed)) => {
                        let payload = adapters::in_band_error(
                            "consumer_lagged",
                            &format!("{missed} frames were dropped by the fan-out"),
                            false,
                        );
                        let _ = tx.send(Ok(json_data_event(payload).event("error"))).await;
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                },
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
    // The same watermark the aggregating path applies, through the same helper (§16.4).
    let seq = event_seq(&event);
    if let Some(seq) = seq {
        if seq <= *through {
            return true;
        }
        *through = seq;
    }
    // Every payload below is a `Value` built from types that always serialize, so
    // `json_data_event`'s fallback branch is never taken in practice — the house
    // style for that case is `unwrap_or_else`, not an `expect` (`io.rs:968`).
    let block =
        match &event {
            ConsumerEvent::Delta { text, seq } => Event::default()
                .id(seq.to_string())
                .data(adapters::openai_delta_payload(text, job, model, created)),
            ConsumerEvent::Event { seq, payload } => json_data_event(payload.clone())
                .id(seq.to_string())
                .event("event"),
            ConsumerEvent::Log { seq, text } => Event::default()
                .id(seq.to_string())
                .event("log")
                .data(text.clone()),
            ConsumerEvent::Artifact { seq, manifest } => json_data_event(manifest.clone())
                .id(seq.to_string())
                .event("artifact"),
            ConsumerEvent::Gap { expected, got } => {
                json_data_event(json!({ "expected": expected, "got": got })).event("gap")
            }
            ConsumerEvent::Finish { seq, finish, usage } => json_data_event(
                adapters::finish_chunk(job, model, created, *finish, usage.as_ref()),
            )
            .id(seq.to_string()),
            ConsumerEvent::Error {
                code,
                message,
                retryable,
                ..
            } => json_data_event(adapters::in_band_error(code, message, *retryable)).event("error"),
            other => json_data_event(other.clone()),
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
///
/// NEX-6.1 applies to a watcher exactly as it does to a streaming answer: a
/// `/events` client that hangs up while nothing is arriving must stop counting as
/// an attached consumer, so the live loop also waits on `tx.closed()`. The body
/// being dropped *is* the connection event; it needs no failed write first.
async fn pump_relay(follow: Follow, tx: mpsc::Sender<Result<Event, Infallible>>) {
    let Follow {
        handle,
        consumer,
        job,
        mut live,
        mut replay,
    } = follow;
    let mut through: u64 = 0;
    while let Some(item) = replay.recv().await {
        if !forward(item, &tx, &mut through).await {
            // `forward` returns false when a write failed, which already means the
            // body is gone; the `Detach` below is the only thing left to do.
            break;
        }
    }
    drop(replay);
    // The replay prefix can also have ended the stream; `forward` reports that the
    // same way, so the live loop is skipped rather than re-entered.
    if !tx.is_closed() {
        let mut gone = std::pin::pin!(tx.closed());
        loop {
            let item = tokio::select! {
                biased;
                () = &mut gone => break,
                result = live.recv() => match result {
                    Ok(item) => item,
                    Err(broadcast::error::RecvError::Lagged(missed)) => {
                        let _ = tx
                            .send(Ok(json_data_event(json!({ "dropped": missed })).event("gap")))
                            .await;
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                },
            };
            if !forward(item, &tx, &mut through).await {
                break;
            }
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
            let mut builder = json_data_event(event).event(&name);
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

/// One SSE event carrying a JSON payload.
///
/// `Event::json_data` is fallible only because it is generic over `Serialize`;
/// every caller here passes something that always serializes (a `Value`, or a
/// `ConsumerEvent` whose `Serialize` impl is derived), so the empty fallback is a
/// defensive branch rather than a real case. Production code may not `expect`
/// (workspace lint policy), and the house style for an infallible serialization is
/// `unwrap_or_else` (`io.rs:968`).
fn json_data_event<T: serde::Serialize>(payload: T) -> Event {
    Event::default()
        .json_data(payload)
        .unwrap_or_else(|_| Event::default())
}

/// A `Retry-After` value. A decimal integer always encodes, so the fallback is
/// defensive; `1` keeps the header meaningful if it is ever reached.
fn retry_after(secs: u64) -> HeaderValue {
    HeaderValue::from_str(&secs.to_string()).unwrap_or_else(|_| HeaderValue::from_static("1"))
}

/// A response header built from text the hub generated itself (a job id). Ids come
/// from the safe alphabet, so the empty fallback cannot be reached in practice.
fn header_of(text: &str) -> HeaderValue {
    HeaderValue::from_str(text).unwrap_or_else(|_| HeaderValue::from_static(""))
}

/// A JSON response with the right content type.
fn json_response(status: StatusCode, body: &Value) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(
            serde_json::to_string(body).unwrap_or_else(|_| "{}".to_string()),
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
