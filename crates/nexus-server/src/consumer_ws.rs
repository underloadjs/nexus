//! The consumer WebSocket face (`GET /v1/consumer`, spec §5.4 / NEX-3): post,
//! watch, and stop watching several tasks over one authenticated socket.
//!
//! # Why a second face, and why it owns nothing
//!
//! Each HTTP surface carries exactly one subscription per request —
//! `POST /v1/chat/completions` streams the answer to the request that posted it,
//! `/v1/jobs/{id}/events` follows exactly one job — so a consumer that wants
//! several answers at once, or a verdict before any event bytes, has no way to
//! ask. The pool actor already speaks `Submit`/`Attach`/`Detach`/`Cancel` per
//! consumer and already fans one task out to N subscribers
//! (`nexus-core/src/actor.rs:195-312`), so this module is only a translation
//! layer: socket message → `ActorMsg`, `Effect::Relay` → frame. It holds no job
//! state, which is what keeps the HTTP and WebSocket surfaces from drifting on
//! what a task's stream contains.
//!
//! # The two ordering rules the design follows from
//!
//!   * **Verdict before bytes, in submission order** (NEX-3.3). A `submit` is
//!     handled *inline*: the reader does not take another message until this
//!     task's `accepted`/`rejected` has been handed to the writer task, and the
//!     event pump for that task is spawned only afterwards. Since the writer
//!     drains one FIFO queue, the verdict necessarily precedes every event frame
//!     for that task, and N submits in one TCP segment get N verdicts in order.
//!     That is also what lets a client correlate a *pre-admission* refusal to the
//!     request it belongs to: the verdict names a task id minted here, before the
//!     actor ever saw it.
//!   * **Exactly one `end` per task** (NEX-3.4). The pump task and the session
//!     teardown share one `AtomicBool` per task; whoever wins the CAS writes the
//!     `end` frame. A task that was never accepted needs no `end` — its `rejected`
//!     verdict *is* the terminal marker, and `retry_after_secs` rides on that
//!     frame rather than on an `end`.
//!
//! # Lifetime coupling (NEX-6)
//!
//! One `ConsumerId` per (connection, task), minted here — the actor keys
//! `consumers` by it, so closing one subscription never disturbs another
//! connection watching the same task (NEX-3.4/NEX-6.5). A pump task and a
//! subscription live and die together, which is the invariant the detach
//! bookkeeping rests on:
//!
//!   * a pump that ends on its own (terminal event, lag, or a socket that stopped
//!     accepting writes) sends `Detach` itself;
//!   * a pump the *session* stops (client `detach`, or connection close) has its
//!     `Detach` sent by the session, for exactly the tasks still marked attached.
//!
//! `on_detach` is idempotent (`actor.rs:1296`, an unknown job answers
//! `Vec::new()`), so the rare overlap costs nothing; double-detaching is
//! preferred to leaving a phantom consumer behind, because a non-empty `consumers`
//! set is what stops the NEX-6.6 subscriber-less window from ever running.
//!
//! `detach:true` (NEX-6.6) is the declared-detached opt-in and flips
//! `cancel_on_consumer_gone`. A detached task that outlives this connection keeps
//! running, bounded by `limits.detached_orphan_ttl_ms`; an attached one is
//! orphaned and deleted by the close itself (NEX-6.1/NEX-6.3).

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::Message;
use axum::extract::ws::WebSocket;
use axum::extract::ws::WebSocketUpgrade;
use axum::extract::State;
use axum::http::header;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::response::Response;
use futures_util::SinkExt;
use futures_util::StreamExt;
use nexus_core::ids::ConsumerId;
use nexus_core::ids::JobId;
use nexus_core::ids::TenantId;
use nexus_core::protocol::decode_consumer_frame;
use nexus_core::protocol::encode_line;
use nexus_core::protocol::CancelReason;
use nexus_core::protocol::ConsumerFrame;
use nexus_core::protocol::EndReason;
use nexus_core::protocol::HubConsumerFrame;
use nexus_core::protocol::WireCaps;
use nexus_core::ActorMsg;
use nexus_core::ConsumerEvent;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::consumer;
use crate::consumer::Follow;
use crate::state;
use crate::state::Gate;
use crate::state::Hub;

/// Frames the hub may have queued in front of one consumer socket before a pump
/// parks. This is the hub-side half of NEX-5.3: a consumer that stops reading
/// feels it as a paused stream, and the hub's memory stays bounded by
/// `tasks × WRITER_QUEUE` frames rather than growing with the answer.
const WRITER_QUEUE: usize = 256;

/// How long a pump gets to write its final `end` frame once the socket is gone.
/// Pumps are parked on a `recv`, so this only has to cover scheduling.
const PUMP_DRAIN: Duration = Duration::from_millis(200);

/// The retry hint for "the hub itself did not answer", matching the `503 +
/// Retry-After: 2` the HTTP surfaces send (`consumer.rs:621-633`).
const HUB_UNAVAILABLE_RETRY_SECS: u64 = 2;

/// `GET /v1/consumer` — the upgrade entry point.
///
/// Spec §7 makes credentials per direction and never shared — the consumer
/// direction presents the API key, never the pool token — and that check happens
/// **before** the upgrade. A missing or unknown API key is therefore an HTTP
/// `401` a plain client can act on, never a socket that opens and then closes
/// for a reason only visible in a frame.
pub async fn upgrade(
    ws: WebSocketUpgrade,
    State(hub): State<Arc<Hub>>,
    headers: HeaderMap,
) -> Response {
    match consumer::tenant_of(&hub, &headers) {
        Ok(tenant) => ws.on_upgrade(move |socket| session(socket, hub, tenant)),
        Err(mut response) => {
            // `tenant_of` builds the OpenAI-shaped body (§7.1); the challenge
            // header belongs to the transport, so it is added on this side.
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                HeaderValue::from_static("Bearer realm=\"nexus\""),
            );
            response
        }
    }
}

/// One thing the writer task may put on the socket.
///
/// One task owns every write (frames, pongs, the close), so the order on the wire
/// is the order in the queue — the property NEX-3.3's verdict-before-bytes rule
/// depends on.
enum ConsumerWrite {
    /// An already-encoded consumer frame. Encoding happens where the frame is
    /// built, so a serialization failure cannot take the writer down.
    Frame(String),
    /// Answer to a client ping.
    Pong(Vec<u8>),
    /// Flush what is queued, then close. The final `end` frames are in front of
    /// this in the same queue, so a clean close cannot truncate them.
    Close,
}

/// One task this connection is watching.
struct Watch {
    /// The consumer identity this connection is registered under for this task.
    consumer: ConsumerId,
    /// Whether this connection currently counts as an attached consumer, shared with
    /// the pump so the pump can clear it when the stream ends on its own. Two things
    /// read it: the teardown detaches exactly the tasks still marked attached
    /// (NEX-6.6: a task the client already detached has to survive the close), and
    /// `attach` refuses a second live stream for one task. Clearing it at
    /// end-of-stream is what lets a client re-attach to a task whose answer already
    /// ran out — the resume NEX-3.4's `through_seq` exists for.
    attached: Arc<AtomicBool>,
    /// End-of-stream once-flag, shared with the pump (NEX-3.4).
    ended: Arc<AtomicBool>,
    /// Asks the pump to stop and name the reason. Taken out once signalled, so a
    /// second `detach` cannot signal a dying pump again.
    stop: Option<oneshot::Sender<EndReason>>,
    /// The pump itself, joined during teardown so its `end` frame can flush.
    pump: Option<JoinHandle<()>>,
}

impl Watch {
    /// Tells the pump to close this stream with `reason`. Idempotent: the sender
    /// is taken, so only the first caller reaches the pump.
    fn signal(&mut self, reason: EndReason) {
        if let Some(tx) = self.stop.take() {
            if tx.send(reason).is_err() {
                // The pump already finished on its own and wrote its `end`; claim
                // the flag so nothing tries to write a second one.
                self.ended.store(true, Ordering::SeqCst);
            }
        }
    }

    /// Waits for the pump to finish writing, then abandons it: it cannot write to
    /// a socket that is going away, and it owes the actor nothing the session has
    /// not already said.
    async fn drain(self) {
        if let Some(task) = self.pump {
            if tokio::time::timeout(PUMP_DRAIN, task).await.is_err() {
                tracing::debug!("consumer pump still parked at teardown; abandoning it");
            }
        }
    }
}

/// Everything a frame handler needs that belongs to the connection rather than to
/// one task. Grouped so handlers stay under the argument limit and share one view
/// of the mutable per-task map.
struct Session<'a> {
    /// Shared hub state: config, ids, pool handles, api keys.
    hub: &'a Arc<Hub>,
    /// The authenticated tenant, fixed for the connection by the pre-upgrade key.
    tenant: TenantId,
    /// Caps on what we send back — the same pair the worker face uses (§9). The
    /// inbound half stays with the read loop, which is the only reader of what the
    /// peer sends.
    caps_out: WireCaps,
    /// The single FIFO in front of the socket.
    sink: mpsc::Sender<ConsumerWrite>,
    /// Tasks this connection is watching.
    watches: HashMap<JobId, Watch>,
}

async fn session(socket: WebSocket, hub: Arc<Hub>, tenant: TenantId) {
    let caps_in = hub.cfg.limits.worker_caps();
    let caps_out = hub.cfg.limits.hub_caps();
    let (mut sink, mut stream) = socket.split();

    // 1. Outbound pump. Dropping its `stall` clone is how the reader learns the
    //    socket can no longer be written: every pump would otherwise park behind
    //    a queue nobody drains, which NEX-5.3 wants reported, not absorbed.
    let (write_tx, mut write_rx) = mpsc::channel::<ConsumerWrite>(WRITER_QUEUE);
    let (stall_tx, mut stall_rx) = mpsc::channel::<()>(1);
    let writer = tokio::spawn(async move {
        while let Some(item) = write_rx.recv().await {
            let message = match item {
                ConsumerWrite::Frame(line) => Message::Text(line.into()),
                ConsumerWrite::Pong(payload) => Message::Pong(payload.into()),
                ConsumerWrite::Close => {
                    let _ = sink.send(Message::Close(None)).await;
                    break;
                }
            };
            if sink.send(message).await.is_err() {
                break;
            }
        }
        let _ = sink.close().await;
        drop(stall_tx);
    });

    let mut session = Session {
        hub: &hub,
        tenant,
        caps_out,
        sink: write_tx.clone(),
        watches: HashMap::new(),
    };

    // 2. Inbound loop: one frame at a time, handled to completion. That is what
    //    keeps a submit's verdict ahead of the next task's bytes.
    'read: loop {
        let message = tokio::select! {
            biased;
            // The writer is gone, so nothing this connection could be told would
            // arrive; stop and let the teardown detach.
            _ = stall_rx.recv() => break 'read,
            got = stream.next() => match got {
                Some(Ok(message)) => message,
                // A client FIN or a broken frame: either way there is nothing
                // left to read, and this is NEX-6.1's independent notice of the
                // consumer leaving — no failed write has to happen first.
                Some(Err(_)) | None => break 'read,
            },
        };
        match message {
            Message::Text(text) => {
                let frame = match decode_consumer_frame(text.trim(), &caps_in) {
                    Ok(frame) => frame,
                    Err(err) => {
                        // A consumer has no handshake to lose, so a bad frame is
                        // answered in band with the shared machine code and the
                        // connection stays up: one typo must not cost the other
                        // tasks this socket carries.
                        let note = HubConsumerFrame::connection_error(err.code(), &err.to_string());
                        push(&session.sink, session.caps_out, note).await;
                        continue;
                    }
                };
                match frame {
                    // Passed whole: `handle_submit` reads the task's fields off the
                    // frame, which is what keeps NEX-3.3's ordering in one place.
                    ConsumerFrame::Submit { .. } => handle_submit(&mut session, frame).await,
                    ConsumerFrame::Attach { job, from_seq } => {
                        // `false`: the attach verb changes nothing about a task's
                        // contract, so the verdict echoes the attached reading.
                        handle_attach(&mut session, job, from_seq, false).await;
                    }
                    ConsumerFrame::Detach { job } => handle_detach(&mut session, job).await,
                    ConsumerFrame::Cancel { job } => handle_cancel(&mut session, job).await,
                }
            }
            // The consumer dialect is JSON in text messages; binary means the
            // peer misunderstood the protocol, not that one task failed.
            Message::Binary(_) => {
                let note = HubConsumerFrame::connection_error(
                    "bad_frame",
                    "consumer frames are JSON text messages",
                );
                push(&session.sink, session.caps_out, note).await;
            }
            Message::Ping(payload) => {
                if session
                    .sink
                    .send(ConsumerWrite::Pong(payload.to_vec()))
                    .await
                    .is_err()
                {
                    break 'read;
                }
            }
            Message::Pong(_) => {}
            Message::Close(_) => break 'read,
        }
    }

    // 3. Teardown, in the order that keeps both invariants: first tell every pump
    //    to close its stream (each writes exactly one `end`, NEX-3.4) and wait for
    //    the frames to reach the queue; then tell the actor about the consumers
    //    this connection still held (NEX-6.1/NEX-6.6); then close the socket.
    let Session {
        hub, sink, watches, ..
    } = session;
    let mut leaving = Vec::with_capacity(watches.len());
    for (job, mut watch) in watches {
        // Held tasks get `closed`; a task the client already detached got its
        // `detached` end from `handle_detach` and has no pump left to signal.
        watch.signal(EndReason::Closed);
        // Swapped, not read, and swapped before the drain: see `handle_detach`. A pump
        // that exits while this waits clears the flag too, so the worst case is the
        // actor hearing `Detach` twice for one consumer, which `on_detach` absorbs.
        let attached = watch.attached.swap(false, Ordering::SeqCst);
        let consumer = watch.consumer.clone();
        watch.drain().await;
        if attached {
            leaving.push((job, consumer));
        }
    }
    for (job, consumer) in leaving {
        if let Some(handle) = hub.route_job(&job).await {
            let _ = state::send(handle.as_ref(), ActorMsg::Detach { job, consumer }).await;
        }
    }
    let _ = sink.send(ConsumerWrite::Close).await;
    drop(sink);
    let _ = writer.await;
}

/// Hand one task's outcome to the actor, answer with the verdict, then start the
/// pump that streams it. Returns once the verdict is queued — that is the NEX-3.3
/// contract, and the reason the next message is not read before this happens.
async fn handle_submit(session: &mut Session<'_>, submit: ConsumerFrame) {
    // The read loop routes only a `Submit` here. Naming the pattern (rather than
    // destructuring eight parameters) keeps the frame's own field names next to the
    // submission they build, and the `else` arm means a new inbound verb cannot
    // silently fall through into a submission.
    let ConsumerFrame::Submit {
        kind,
        payload,
        pool,
        requires,
        priority,
        detach,
        idempotency_key,
    } = submit
    else {
        return;
    };
    // NEX-6.6 vs NEX-6.1, decided by the frame rather than by the route: `false`
    // (the default on this face) means this socket *is* the subscription, so
    // hanging up orphans and deletes the task.
    let request = consumer::JobRequest {
        kind,
        pool,
        payload,
        requires,
        priority,
        detach: Some(detach),
    };
    // A consumer that names an idempotency key wants deduplication. One that does
    // not must not get the content-hash key the HTTP surface derives: the hub
    // cannot report *which* task a duplicate attached to on this face, so the
    // verdict would name a job the client never posted. A fresh key per submit is
    // the honest reading of "post me a task", and it is minted once so the
    // refusal below can name the same id it was derived from.
    let refusal_job = session.hub.ids.job_id();
    let fallback_key = format!("ws:{}", refusal_job.as_ref());
    let (submission, pool_id) = match consumer::build_submission(
        session.hub,
        &session.tenant,
        &request,
        // No route-level override on this face: the frame's own `pool` field is
        // already in `request.pool`, whereas HTTP has a second `?pool=` spelling.
        None,
        idempotency_key.as_deref(),
        fallback_key,
    ) {
        Ok(built) => built,
        Err((status, code, message)) => {
            // Refused before the actor ever saw a task. The id named here was
            // minted for the verdict alone: NEX-3.3 wants the refusal tied to the
            // request, and in-order verdicts are what makes that tie usable.
            let frame = HubConsumerFrame::Rejected {
                job: refusal_job,
                status: status.as_u16(),
                code: code.to_string(),
                message,
                retry_after_secs: None,
            };
            push(&session.sink, session.caps_out, frame).await;
            return;
        }
    };

    // §5.2 at the runtime level: a key this hub has already routed is **followed**, not
    // re-submitted. Doing it here rather than letting the actor's duplicate branch answer
    // is the whole of the fix — that branch replays on the *original* id
    // (`actor.rs:1045-1064`), while this face registers its relay, its verdict and its
    // pump under the id it mints, so a verdict naming a different job would never resolve
    // and the client would read `hub_unavailable` for a task that is answering fine.
    //
    // `handle_attach` is the same follow the `attach` verb performs: verdict, `meta`, then
    // the replay of the original's ring. If the record went away in the meantime it
    // answers `unknown_job`, which is the truth about a task this hub can no longer
    // replay, and the client's next attempt starts a fresh execution (NEX-6.3).
    //
    // One consequence to keep honest: the actor's `nexus_idempotent_attach_total` no
    // longer moves for these retries, because the actor is never told about a duplicate
    // the hub can answer itself. The hub cannot compensate — `PoolGauges::from_actor`
    // (state.rs) rebuilds the mirrored registry from the actor on every dispatch.
    if let Some((_pool, existing)) = session
        .hub
        .idempotency_target(&submission.idempotency_key)
        .await
    {
        // `from_seq: 0` — a resubmission asks for the whole answer, which is the only
        // thing it can mean when the client never named a resume point. `detach` is
        // echoed rather than forced to `false`: this frame *is* a submission, so it
        // declares its own contract (NEX-6.6), unlike a bare attach.
        handle_attach(session, existing, 0, detach).await;
        return;
    }

    // Register the relay, the gate and this consumer's private replay queue
    // *before* the actor hears about the job — the ordering `consumer::submit`
    // documents, shared rather than reimplemented here. The `JobRelay` itself is
    // not held: the pool's relay map owns it, and if another surface prunes it the
    // fan-out closes and this stream ends in band with `closed`, which is the
    // truth about what the hub can still deliver.
    let (job, consumer, handle, _relay, gate, live, replay) =
        consumer::submit(session.hub, &pool_id, submission).await;
    let accepted = match verdict_of(session.hub, gate).await {
        Gate::Accepted => HubConsumerFrame::Accepted {
            job: job.clone(),
            kind: request.kind.clone(),
            pool: pool_id.clone(),
            // Echo of the contract this connection is under, so a client can tell
            // which lifetime rules it bought (NEX-6.6).
            detached: detach,
        },
        Gate::Reject {
            status,
            code,
            message,
            retry_after_secs,
        } => HubConsumerFrame::Rejected {
            job: job.clone(),
            status: status.as_u16(),
            code,
            message,
            retry_after_secs,
        },
        Gate::Closed => HubConsumerFrame::Rejected {
            job: job.clone(),
            status: StatusCode::SERVICE_UNAVAILABLE.as_u16(),
            code: "hub_unavailable".to_string(),
            message: "the hub did not answer in time; retry".to_string(),
            retry_after_secs: Some(HUB_UNAVAILABLE_RETRY_SECS),
        },
    };
    let refused = matches!(accepted, HubConsumerFrame::Rejected { .. });
    push(&session.sink, session.caps_out, accepted).await;

    if refused {
        // A refused submission must leave nothing behind on this connection. The
        // actor either never created the record (admission refusals) or drops it
        // on its own grace path, but the routing entry, the relay and the
        // idempotency row installed above are ours to prune.
        let _ = session.hub.forget_job(&job).await;
        handle.forget_relay(&job).await;
        session.hub.forget_idempotency_for_job(&job).await;
        return;
    }

    // §5.4 item 5: `meta` opens the stream, ahead of every event. Queued here rather
    // than from the pump, because this is the only point that still provably precedes
    // the first relay item. The record is read from the actor — the hub keeps no job
    // state of its own — so the phase is the actor's, not a guess. If the task has
    // already been forgotten (a worker can finish a zero-frame task this fast) there
    // is no snapshot to report, and the events that follow are the whole truth.
    if let Some(view) = session.hub.job_snapshot(&job).await {
        let meta = HubConsumerFrame::meta(&view, session.hub.wall_ms());
        push(&session.sink, session.caps_out, meta).await;
    }

    spawn_pump(session, job, consumer, handle, live, replay);
}

/// Refusal used by both follow paths, so an unknown task is answered identically
/// whichever verb reached it.
fn attach_refused(job: &JobId, status: StatusCode, message: &str) -> HubConsumerFrame {
    HubConsumerFrame::Rejected {
        job: job.clone(),
        status: status.as_u16(),
        // The HTTP surface's own code for this case, so both faces answer the same
        // question the same way (§5.1 vs §5.4).
        code: "unknown_job".to_string(),
        message: message.to_string(),
        retry_after_secs: None,
    }
}

/// Follow a task this hub already knows: verdict first, like `submit` — `accepted` is
/// queued before the pump exists, so no event byte can precede it.
///
/// Two callers, one flow: the `attach` verb, and a `submit` whose idempotency key this
/// hub has already routed (see `handle_submit`, which delegates here rather than
/// inventing a third follow path). `detached` is the contract the verdict echoes — `false`
/// for a bare attach, because following a task changes nothing about the lifetime its
/// submission declared (NEX-6.6), and the frame's own declaration for a resubmission,
/// which really is a submission.
async fn handle_attach(session: &mut Session<'_>, job: JobId, from_seq: u64, detached: bool) {
    let Some(handle) = session.hub.route_job(&job).await else {
        let frame = attach_refused(&job, StatusCode::NOT_FOUND, "no such job on this hub");
        push(&session.sink, session.caps_out, frame).await;
        return;
    };
    // Tenancy: `on_attach` keys nothing by tenant (the actor trusts its caller), so
    // this face has to check it — a guessed job id must not stream somebody else's
    // prompt. Same reading as `GET /v1/jobs/{id}/events` (consumer.rs:791-802).
    let view = match session.hub.job_snapshot(&job).await {
        Some(view) if view.tenant == session.tenant => view,
        Some(_) | None => {
            let frame = attach_refused(&job, StatusCode::NOT_FOUND, "no such job for this tenant");
            push(&session.sink, session.caps_out, frame).await;
            return;
        }
    };
    if let Some(watch) = session.watches.get(&job) {
        // Only a *live* stream is a duplicate. A task whose answer already ran out has
        // no pump and no consumer left — the pump cleared the flag on its way out — so
        // re-attaching it is the resume NEX-3.4's `through_seq` exists for, and the new
        // entry simply replaces the finished one.
        if watch.attached.load(Ordering::SeqCst) {
            // A second attach on the connection that already watches the task
            // would add a second consumer and a second pump for one stream:
            // refuse rather than duplicate the answer on one socket.
            let frame = HubConsumerFrame::Rejected {
                job,
                status: StatusCode::CONFLICT.as_u16(),
                code: "already_attached".to_string(),
                message: "this connection already watches that job".to_string(),
                retry_after_secs: None,
            };
            push(&session.sink, session.caps_out, frame).await;
            return;
        }
    }

    let relay = handle
        .relay(&job, session.hub.cfg.limits.consumer_backpressure_frames)
        .await;
    let live = relay.events.subscribe();
    let (replay_tx, replay_rx) =
        mpsc::channel::<state::RelayItem>(session.hub.cfg.limits.replay_frames.max(16));
    *relay.replay_target.lock().await = Some(replay_tx);
    let consumer = session.hub.ids.consumer_id();
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
        let frame = HubConsumerFrame::Rejected {
            job,
            status: StatusCode::SERVICE_UNAVAILABLE.as_u16(),
            code: "hub_unavailable".to_string(),
            message: "the pool actor is draining".to_string(),
            retry_after_secs: Some(HUB_UNAVAILABLE_RETRY_SECS),
        };
        push(&session.sink, session.caps_out, frame).await;
        return;
    }
    // `meta` first (§5.4 item 5), built while the record is still in hand: the
    // accepted verdict below moves the fields it echoes. Queued before the pump
    // exists, so `accepted` then `meta` then events is the order on the wire.
    let meta = HubConsumerFrame::meta(&view, session.hub.wall_ms());
    push(
        &session.sink,
        session.caps_out,
        HubConsumerFrame::Accepted {
            job: job.clone(),
            kind: view.kind,
            pool: view.pool,
            // The contract this follow is under, echoed so a client knows which lifetime
            // rules it bought (NEX-6.6). `false` for the attach verb — following a task
            // never changes them; the frame's own declaration for a resubmission.
            detached,
        },
    )
    .await;
    push(&session.sink, session.caps_out, meta).await;

    spawn_pump(session, job, consumer, handle, live, replay_rx);
}

/// Stop watching one task without cancelling it (NEX-6.6).
async fn handle_detach(session: &mut Session<'_>, job: JobId) {
    let Some(mut watch) = session.watches.remove(&job) else {
        // Not a task this connection was watching. `error` rather than `end`:
        // there is no stream here to end, and an `end` for a task the client is
        // not following would misreport what this connection carried.
        let frame = HubConsumerFrame::Error {
            job: Some(job),
            code: "not_attached".to_string(),
            message: "this connection does not watch that job".to_string(),
            retryable: false,
            through_seq: None,
        };
        push(&session.sink, session.caps_out, frame).await;
        return;
    };
    let consumer = watch.consumer.clone();
    // Swapped rather than read: the pump may clear it while this teardown drains the
    // stream, and whichever side sees `true` is the one that owes the actor the
    // `Detach`. Read before the drain on purpose, so a pump finishing underneath this
    // loop can only ever produce a second (idempotent) detach, never zero.
    let was_attached = watch.attached.swap(false, Ordering::SeqCst);
    // The pump closes the stream with `detached`; the session does the actor-side
    // detach here so the two cannot race over who told the hub.
    watch.signal(EndReason::Detached);
    let pump = watch.pump.take();
    drop(watch);
    if was_attached {
        if let Some(handle) = session.hub.route_job(&job).await {
            let _ = state::send(
                handle.as_ref(),
                ActorMsg::Detach {
                    job: job.clone(),
                    consumer,
                },
            )
            .await;
        }
    }
    if let Some(task) = pump {
        let _ = tokio::time::timeout(PUMP_DRAIN, task).await;
    }
    // The relay is the task's shared fan-out, not this connection's property:
    // another connection may be watching the same task, and the actor prunes it
    // when it forgets the job (`state.rs:1065-1083`). Deliberately left alone.
}

/// Cancel one task explicitly. Retention stays (this is a consumer's decision,
/// not a lost connection) and the stream is closed here, because `on_cancel`
/// relays no terminal event of its own (`actor.rs:1449-1477`).
async fn handle_cancel(session: &mut Session<'_>, job: JobId) {
    let unknown = HubConsumerFrame::Error {
        job: Some(job.clone()),
        code: "unknown_job".to_string(),
        message: "no such job for this tenant".to_string(),
        retryable: false,
        through_seq: None,
    };
    let Some(handle) = session.hub.route_job(&job).await else {
        push(&session.sink, session.caps_out, unknown).await;
        return;
    };
    match session.hub.job_snapshot(&job).await {
        Some(view) if view.tenant == session.tenant => {}
        Some(_) | None => {
            push(&session.sink, session.caps_out, unknown).await;
            return;
        }
    }
    let _ = state::send(
        handle.as_ref(),
        ActorMsg::Cancel {
            job: job.clone(),
            // A consumer-initiated cancellation: the same reason
            // `DELETE /v1/jobs/{id}` sends, so no new vocabulary is needed and the
            // supplier sees one cause for both surfaces.
            reason: CancelReason::ConsumerGone,
        },
    )
    .await;
    if let Some(watch) = session.watches.get_mut(&job) {
        // Close this connection's stream now rather than waiting for a supplier
        // that may never have claimed the task. A supplier `error` that arrives
        // first already wrote the end frame, and the once-flag makes this a no-op.
        watch.signal(EndReason::Closed);
    }
}

/// Start the stream for one task and register the subscription that outlives it.
///
/// The task starts out attached on purpose: both entry points (a submit that was
/// accepted, and an attach) leave this connection as an attached consumer of the task,
/// which is what the teardown's detach list is built from. The pump clears that flag
/// when the stream ends, so the list never holds a task this connection already stopped
/// watching.
fn spawn_pump(
    session: &mut Session<'_>,
    job: JobId,
    consumer: ConsumerId,
    handle: Arc<state::PoolHandle>,
    live: broadcast::Receiver<state::RelayItem>,
    replay: mpsc::Receiver<state::RelayItem>,
) {
    let (stop_tx, stop_rx) = oneshot::channel();
    let ended = Arc::new(AtomicBool::new(false));
    // Shared with the pump: end-of-stream ends the subscription, and the pump is the
    // one that knows when it happened.
    let attached = Arc::new(AtomicBool::new(true));
    let task = tokio::spawn(pump(Pump {
        follow: Follow {
            handle,
            consumer: consumer.clone(),
            job: job.clone(),
            live,
            replay,
        },
        caps: session.caps_out,
        sink: session.sink.clone(),
        stop: stop_rx,
        ended: Arc::clone(&ended),
        attached: Arc::clone(&attached),
    }));
    session.watches.insert(
        job,
        Watch {
            consumer,
            attached,
            ended,
            stop: Some(stop_tx),
            pump: Some(task),
        },
    );
}

/// Await the actor's first verdict with the same budget the HTTP surfaces use, so
/// a submit cannot park the connection when nothing will ever claim it
/// (`limits.no_worker_grace_ms` plus the actor's own tick slack).
async fn verdict_of(hub: &Arc<Hub>, gate: oneshot::Receiver<Gate>) -> Gate {
    let budget = Duration::from_millis(hub.cfg.limits.no_worker_grace_ms + 500);
    match tokio::time::timeout(budget, gate).await {
        Ok(Ok(gate)) => gate,
        // `Ok(Err(_))` means the relay — and with it the gate sender — is gone, so
        // the hub has already moved on. Both that and the timeout are the retryable
        // "the hub did not answer" answer, never a hang.
        Ok(Err(_)) | Err(_) => Gate::Closed,
    }
}

/// One `Pump` is one task's stream on this connection, until it ends with exactly
/// one `end`.
struct Pump {
    /// The subscription: pool handle, consumer id, job, replay + live sources.
    follow: Follow,
    caps: WireCaps,
    sink: mpsc::Sender<ConsumerWrite>,
    stop: oneshot::Receiver<EndReason>,
    /// End-of-stream once-flag, shared with the session (NEX-3.4).
    ended: Arc<AtomicBool>,
    /// This connection's attachment, shared with the session: cleared here when the
    /// stream ends, because a finished stream is a gone subscription and the session's
    /// teardown must not claim it a second time.
    attached: Arc<AtomicBool>,
}

/// What one relay item means for this connection's stream.
enum Translated {
    /// Write this frame and keep going.
    Send(HubConsumerFrame),
    /// Nothing to write: a keepalive comment, or a seq the replay already
    /// delivered (so an overlapping replay cannot print one delta twice).
    Skip,
    /// Write the frame if there is one, then close the stream. `reason` is the
    /// `end` reason this item implies, and `frame` is the last event itself —
    /// `None` for the relay's own `End` marker, which has no payload.
    Last {
        frame: Option<HubConsumerFrame>,
        reason: EndReason,
    },
}

async fn pump(pump: Pump) {
    let Pump {
        follow,
        caps,
        sink,
        mut stop,
        ended,
        attached,
    } = pump;
    let Follow {
        handle,
        consumer,
        job,
        mut live,
        mut replay,
    } = follow;

    let mut through: u64 = 0;
    let mut alive = true;
    let mut reason = EndReason::Closed;
    // Two-phase pump, exactly as the SSE surfaces do it: drain this consumer's
    // private replay queue first (the dispatcher closes it once the buffered
    // prefix is written, and that close is what hands the stream to the live
    // fan-out), then follow the fan-out.
    let mut replay_open = true;
    // A `oneshot` that resolved `Err` stays ready forever, which in a biased select
    // would starve the streams; the flag takes the branch out instead.
    let mut stop_open = true;

    while alive {
        tokio::select! {
            biased;
            // A `detach`/`cancel`/close from the session.
            asked = &mut stop, if stop_open => match asked {
                Ok(asked) => {
                    reason = asked;
                    alive = false;
                }
                Err(_) => stop_open = false,
            },
            item = replay.recv(), if replay_open => match item {
                Some(item) => match relayed(&job, item, &mut through) {
                    Translated::Send(frame) => alive = push(&sink, caps, frame).await,
                    Translated::Skip => {}
                    Translated::Last { frame, reason: why } => {
                        reason = why;
                        alive = last(&sink, caps, frame).await;
                    }
                },
                None => replay_open = false,
            },
            item = live.recv() => match item {
                Ok(item) => match relayed(&job, item, &mut through) {
                    Translated::Send(frame) => alive = push(&sink, caps, frame).await,
                    Translated::Skip => {}
                    Translated::Last { frame, reason: why } => {
                        reason = why;
                        alive = last(&sink, caps, frame).await;
                    }
                },
                Err(broadcast::error::RecvError::Lagged(missed)) => {
                    // NEX-5.3: this consumer could not keep up, so the fan-out
                    // dropped frames. The `gap` goes out first — it is ahead of
                    // the `end` in the same queue — rather than being swallowed by
                    // the backlog that caused it (NEX-5.2's reported hole).
                    let gap = HubConsumerFrame::Gap {
                        job: job.clone(),
                        expected: through + 1,
                        got: through + 1 + missed,
                    };
                    push(&sink, caps, gap).await;
                    reason = EndReason::Lagged;
                    alive = false;
                }
                // The fan-out is gone: the actor forgot the job, or the hub is
                // draining. Nothing more can arrive for this task.
                Err(broadcast::error::RecvError::Closed) => {
                    reason = EndReason::Closed;
                    alive = false;
                }
            },
            // Unreachable in practice: the live branch has no guard, so the select
            // always has something to poll. Stated so a future guard cannot turn a
            // disabled-all-branches select into a panic.
            else => alive = false,
        }
    }

    // Exactly one `end` per task (NEX-3.4): the CAS is what makes "whoever closes
    // the stream first owns the frame" true when session and pump race.
    if ended
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        let frame = HubConsumerFrame::end_for(
            &job,
            reason,
            // The cursor a consumer needs to resume elsewhere (NEX-3.4).
            (through > 0).then_some(through),
        );
        push(&sink, caps, frame).await;
    }
    // The subscription is over, whatever ended it, so this connection no longer
    // counts as a consumer of this task: the teardown must not report it as one, and a
    // fresh `attach` for the same task (the resume NEX-3.4's cursor enables) is no
    // longer a duplicate.
    attached.store(false, Ordering::SeqCst);
    // A pump that ended on its own is the subscription going away (NEX-6.1). A
    // pump the session stopped has already been detached by the session, and
    // `on_detach` is idempotent, so saying it twice is cheaper than risking a
    // phantom consumer — a non-empty `consumers` set is what keeps NEX-6.3's
    // deletion and NEX-6.6's window from ever firing.
    let _ = state::send(handle.as_ref(), ActorMsg::Detach { job, consumer }).await;
}

/// Translate one relay item into this connection's frame.
fn relayed(job: &JobId, item: state::RelayItem, through: &mut u64) -> Translated {
    let event = match item {
        // The actor's keepalive comment: this transport has its own ping/pong, so
        // there is nothing to write. The SSE pumps skip it for the same reason.
        state::RelayItem::Comment(_) => return Translated::Skip,
        state::RelayItem::End => {
            // The fan-out's own marker: the task reached a terminal state on the hub.
            // Whether that was a `finish` or an `error` is not knowable here — this
            // consumer may have attached after the terminal event, or the event may
            // have been suppressed below as a replay overlap — so the stream closes
            // with `closed`, the one reason that is true either way.
            return Translated::Last {
                frame: None,
                reason: EndReason::Closed,
            };
        }
        state::RelayItem::Event(event) => event,
    };
    // Seqs already delivered are skipped. `Gap`, `Error`, `Meta` and `Replay`
    // carry no content seq and always go out.
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
            return Translated::Skip;
        }
        *through = seq;
    }
    let frame = HubConsumerFrame::relayed(job, event.clone());
    match &event {
        // A terminal event: the frame goes out, then the stream ends. `Finish` is
        // a complete answer; every `error` — supplier failure, cancellation, hub
        // shutdown — ends the stream as failed (NEX-5.2's in-band failure).
        ConsumerEvent::Finish { .. } => Translated::Last {
            frame: Some(frame),
            reason: EndReason::Finished,
        },
        ConsumerEvent::Error { .. } => Translated::Last {
            frame: Some(frame),
            reason: EndReason::Errored,
        },
        _ => Translated::Send(frame),
    }
}

/// Queue one frame, encoded under the outbound caps.
///
/// Returns whether the socket may still be written: `false` means the writer task
/// has stopped, which ends every pump rather than parking it on a queue nobody
/// drains. A frame that cannot fit the caps is logged and skipped — reachable only
/// by something that passed the ingress caps and grew in translation, never by a
/// delta (the worker face caps `Frame.text` at 256 KiB, far under the outbound
/// line cap).
async fn push(sink: &mpsc::Sender<ConsumerWrite>, caps: WireCaps, frame: HubConsumerFrame) -> bool {
    let line = match encode_line(&frame, &caps) {
        Ok(line) => line,
        Err(err) => {
            tracing::warn!(%err, "consumer frame exceeded the wire caps and was dropped");
            return true;
        }
    };
    sink.send(ConsumerWrite::Frame(line)).await.is_ok()
}

/// Writes the last event of a stream, if the item carried one, and reports that
/// the pump is done (`false` is the loop's new `alive`).
///
/// The result of the write is deliberately ignored: the `end` frame goes out after
/// the loop whatever the socket did with this event, so a consumer that vanished
/// mid-answer still sees the terminal frame in front of it or nothing at all —
/// never an open stream with nobody on it.
async fn last(
    sink: &mpsc::Sender<ConsumerWrite>,
    caps: WireCaps,
    frame: Option<HubConsumerFrame>,
) -> bool {
    if let Some(frame) = frame {
        let _ = push(sink, caps, frame).await;
    }
    false
}
