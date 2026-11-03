//! The worker-facing WebSocket endpoint (task §4): one persistent outbound
//! connection per worker process, JSON-lines frames, multiplexed per `job_id`.
//!
//! Runtime boundary: this is the **native** half. The WASM build cannot host it
//! (no I/O driver, no listener), so `nexus-wasm` exposes the actor and lets a JS
//! host perform exactly what this module does: read a frame, hand it to the
//! actor, write the returned frames.

use std::sync::Arc;

use axum::extract::ws::Message;
use axum::extract::ws::WebSocketUpgrade;
use axum::extract::State;
use axum::http::header;
use axum::http::StatusCode;
use axum::http::Uri;
use axum::response::IntoResponse;
use axum::response::Response;
use futures_util::SinkExt;
use futures_util::StreamExt;
use nexus_core::auth::bearer;
use nexus_core::ids::WorkerId;
use nexus_core::protocol::decode_worker_frame;
use nexus_core::protocol::encode_line;
use nexus_core::protocol::ByeReason;
use nexus_core::protocol::WorkerFrame;
use nexus_core::ActorMsg;
use nexus_core::HubFrame;
use tokio::sync::mpsc;

use crate::state;
use crate::state::Hub;
use crate::state::WorkerConn;
use crate::state::WorkerWrite;

/// Outbound queue depth per worker. Bounded deliberately: a worker that cannot
/// keep up is a worker whose engine is already behind, and buffering without
/// bound would just move the RSS problem.
const OUTBOUND_QUEUE: usize = 256;

/// How long a socket may take to send its `Hello`. Unauthenticated sockets are a
/// cheap DoS, so this is short.
const HANDSHAKE_TIMEOUT_SECS: u64 = 20;

/// Upgrade handler: the pool token is checked **before** the upgrade, so an
/// unauthenticated host never becomes a connection the hub has to track.
pub async fn upgrade(
    State(hub): State<Arc<Hub>>,
    ws: WebSocketUpgrade,
    headers: axum::http::HeaderMap,
    uri: Uri,
) -> Response {
    let token = bearer(
        headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
    )
    // A query-string token is accepted for notebook clients that cannot set a
    // header; it is never logged, because it is a secret.
    .or_else(|| {
        uri.query().unwrap_or_default().split('&').find_map(|pair| {
            let (key, value) = pair.split_once('=')?;
            (key == "token" && !value.is_empty()).then_some(value)
        })
    });

    match hub.pool_tokens.verify(token) {
        Ok(label) => ws.on_upgrade(move |socket| session(socket, hub, label)),
        Err(err) => {
            tracing::warn!(%err, "worker upgrade refused");
            (
                StatusCode::UNAUTHORIZED,
                [(header::WWW_AUTHENTICATE, "Bearer")],
                format!("{err}\n"),
            )
                .into_response()
        }
    }
}

/// One worker session, for the whole lifetime of its socket.
async fn session(socket: axum::extract::ws::WebSocket, hub: Arc<Hub>, token_label: String) {
    let (mut sink, mut stream) = socket.split();

    // 1. Handshake: the first frame must be a valid `Hello`.
    let first = match tokio::time::timeout(
        std::time::Duration::from_secs(HANDSHAKE_TIMEOUT_SECS),
        stream.next(),
    )
    .await
    {
        Ok(Some(Ok(Message::Text(text)))) => text.to_string(),
        Ok(Some(Ok(Message::Binary(bytes)))) => String::from_utf8_lossy(&bytes).into_owned(),
        Ok(Some(Ok(Message::Close(_)))) | Ok(Some(Err(_))) | Ok(None) => return,
        Ok(Some(Ok(Message::Ping(_)))) | Ok(Some(Ok(Message::Pong(_)))) => {
            tracing::debug!(%token_label, "control frame before hello");
            return;
        }
        Err(_) => {
            tracing::warn!(%token_label, "worker handshake timed out");
            return;
        }
    };

    let caps = hub.cfg.limits.worker_caps();
    let frame = match decode_worker_frame(first.trim(), &caps) {
        Ok(frame) => frame,
        Err(err) => {
            let reject = HubFrame::Reject {
                code: err.code().to_string(),
                message: err.to_string(),
            };
            if let Ok(text) = encode_line(&reject, &hub.cfg.limits.hub_caps()) {
                let _ = sink.send(Message::Text(text.into())).await;
            }
            tracing::warn!(%err, %token_label, "worker sent an unusable hello");
            return;
        }
    };
    let WorkerFrame::Hello {
        worker_id, pools, ..
    } = &frame
    else {
        tracing::warn!(%token_label, "first worker frame was not a hello");
        return;
    };
    let Ok(worker) = WorkerId::parse(worker_id) else {
        tracing::warn!(%worker_id, %token_label, "worker id rejected by the alphabet check");
        return;
    };
    let handles: Vec<Arc<state::PoolHandle>> = pools
        .iter()
        .filter_map(|name| nexus_core::PoolId::parse(name).ok())
        .filter_map(|pid| hub.pool(&pid).map(Arc::clone))
        .collect();
    if handles.is_empty() {
        // The actor would reject this too; refusing here avoids registering a
        // connection that can never do anything.
        tracing::warn!(%worker, %token_label, "worker asked for pools this hub does not serve");
        return;
    }

    // 2. Outbound pump. Everything the actor wants to say goes through this
    //    single task, so frame order on the socket equals effect order.
    let conn = hub.ids.conn_id();
    let (out_tx, mut out_rx) = mpsc::channel::<WorkerWrite>(OUTBOUND_QUEUE);
    let writer = tokio::spawn(async move {
        while let Some(item) = out_rx.recv().await {
            let message = match item {
                WorkerWrite::Frame(line) => Message::Text(line.into()),
                WorkerWrite::Pong(payload) => Message::Pong(payload.into()),
                // Eviction: close after the `Bye` above has been flushed, then
                // stop the pump so this socket's queue is finished.
                WorkerWrite::Close => {
                    let _ = sink.send(Message::Close(None)).await;
                    break;
                }
            };
            if sink.send(message).await.is_err() {
                break;
            }
        }
        let _ = sink.close().await;
    });

    for handle in &handles {
        // Replace the binding first, then evict whatever used to hold it. The
        // actor addresses a worker by id, so `send_frame` can only ever reach
        // the *live* connection; the stale socket (task §11's re-run cell) has
        // to be closed here, from the registry entry that pointed at it.
        let previous = {
            let mut workers = handle.workers.lock().await;
            workers.insert(
                worker.clone(),
                WorkerConn {
                    conn,
                    tx: out_tx.clone(),
                },
            )
        };
        if let Some(stale) = previous.filter(|previous| previous.conn != conn) {
            if let Ok(line) = encode_line(
                &HubFrame::Bye {
                    reason: ByeReason::Evicted,
                },
                &hub.cfg.limits.hub_caps(),
            ) {
                let _ = stale.tx.send(WorkerWrite::Frame(line)).await;
            }
            let _ = stale.tx.send(WorkerWrite::Close).await;
            tracing::info!(%worker, stale_conn = stale.conn, conn, "a newer socket took over this worker id");
        }
        let _ = state::send(
            handle.as_ref(),
            ActorMsg::Hello {
                worker: worker.clone(),
                conn,
                hello: nexus_core::HelloInput::from_frame(&frame).unwrap_or_default(),
            },
        )
        .await;
    }
    tracing::info!(%worker, %token_label, conn, pools = handles.len(), "worker joined");

    // 3. Read loop. Each inbound frame is tagged with this socket's generation,
    //    so an evicted socket can never mutate the live one's state (§11).
    'read: loop {
        let next = match stream.next().await {
            Some(Ok(Message::Text(text))) => text.to_string(),
            Some(Ok(Message::Binary(bytes))) => String::from_utf8_lossy(&bytes).into_owned(),
            Some(Ok(Message::Ping(payload))) => {
                let _ = out_tx.send(WorkerWrite::Pong(payload.to_vec())).await;
                continue;
            }
            Some(Ok(Message::Pong(_))) => continue,
            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
        };
        // A message may hold several lines; the codec enforces the caps either way.
        let lines: Vec<String> = next
            .split('\n')
            .map(str::to_string)
            .filter(|l| !l.trim().is_empty())
            .collect();
        for line in lines {
            // A frame this hub cannot decode means the peer and the hub are out
            // of step (or someone is fuzzing the socket). Dropping the rest of
            // the message and continuing would let a client that never sent a
            // valid frame hold the connection open forever, so the documented
            // behaviour is to close: `break 'read`, not `break` out of the line
            // loop.
            let Ok(frame) = decode_worker_frame(line.trim(), &hub.cfg.limits.worker_caps()) else {
                tracing::warn!(%worker, "undecodable worker frame; closing the socket");
                break 'read;
            };
            for handle in &handles {
                if state::send(
                    handle.as_ref(),
                    ActorMsg::WorkerFrame {
                        worker: worker.clone(),
                        conn,
                        frame: frame.clone(),
                    },
                )
                .await
                .is_err()
                {
                    tracing::error!(%worker, "pool actor mailbox closed");
                    break 'read;
                }
            }
        }
    }

    // 4. Departure. Only remove the binding if it is still ours: a newer socket
    //    may already have replaced it (eviction).
    for handle in &handles {
        let mut workers = handle.workers.lock().await;
        if workers
            .get(&worker)
            .is_some_and(|existing| existing.conn == conn)
        {
            workers.remove(&worker);
            drop(workers);
            let _ = state::send(
                handle.as_ref(),
                ActorMsg::WorkerGone {
                    worker: worker.clone(),
                    conn,
                },
            )
            .await;
        } else {
            tracing::debug!(%worker, "stale socket closing; leaving the live binding alone");
        }
    }
    drop(out_tx);
    let _ = writer.await;
    tracing::info!(%worker, conn, "worker left");
}
