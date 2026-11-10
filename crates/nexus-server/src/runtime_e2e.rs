//! Runtime end-to-end tests for the native hub: a real listener, a real worker
//! WebSocket, and a real consumer HTTP request whose SSE bytes are read off the
//! wire. The core acceptance tests (`crates/nexus-core/tests/acceptance.rs`) drive
//! the actor with injected time and prove the *decisions*; only this file can prove
//! the runtime performs them — that effect bytes reach a socket, that a 503 is
//! answered before a body opens, that an unauthenticated read is refused.
//!
//! ## Status: executed
//!
//! These run on every `cargo test --workspace` (the `--bins` target), and they are
//! the only place the **runtime** is observed. The acceptance suite drives the actor
//! with injected time, so anything that lives in the wire layer is invisible there:
//! an SSE body that never ends, a detach that never reaches the actor, a verdict
//! that arrives after the first event byte, a 503 that opens a body anyway. When one
//! of these fails, suspect the runtime wiring before the actor's decisions — the
//! decision half has its own green suite.
//!
//! ## Why the assertions wait rather than sleep
//!
//! The hub batches outgoing deltas (`flush_interval_ms`), the actor ticks on an
//! interval, and the no-worker grace is a wall-clock deadline. Tests therefore wait
//! for the next wire frame or read the response body to completion, and the config
//! shrinks the windows so a broken expectation fails fast instead of hanging.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use futures_util::SinkExt;
use futures_util::StreamExt;
use nexus_core::protocol::decode_hub_frame;
use nexus_core::protocol::encode_line;
use nexus_core::protocol::Attestation;
use nexus_core::protocol::FinishKind;
use nexus_core::protocol::FrameKind;
use nexus_core::protocol::HubFrame;
use nexus_core::protocol::Requires;
use nexus_core::protocol::WireCaps;
use nexus_core::protocol::WorkerFrame;
use nexus_core::BrokerConfig;
use nexus_core::JobId;
use tokio::net::TcpListener;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

/// Reference digest from `SHARD_TABLE` (services/molab/molab-strata-runtime.py:95).
const SHARD_1: &str = "40f369eca301ec6ea318dbb671b3132c26a2f0b89aa5458a7051b33215ee296c";
/// The pool the reference worker joins, matching `BrokerConfig::default()`.
const POOL: &str = "llm.chat/qwen38";
/// Consumer key and worker token for the test hub (secrets in both directions,
/// exactly as §9 requires — they are deliberately different strings).
const API_KEY: &str = "sk-e2e-1";
const POOL_TOKEN: &str = "pool-token-e2e";

/// A hub config with the task's shape and the wait windows shrunk to test scale.
fn test_config() -> BrokerConfig {
    let mut cfg = BrokerConfig::default();
    // Anonymous is deliberately **off**: the hub must answer an unauthenticated
    // consumer read with `401` (`job_reads_require_an_api_key_and_operational_routes_do_not`),
    // and every test request carries `API_KEY` / `?token=POOL_TOKEN` anyway.
    cfg.auth.allow_anonymous = false;
    cfg.auth.worker_tokens = vec![POOL_TOKEN.to_string()];
    cfg.auth.api_keys = vec![nexus_core::config::ApiKeyConfig {
        key: API_KEY.to_string(),
        tenant: "default".to_string(),
    }];
    cfg.auth.model_shas = BTreeSet::from([SHARD_1.to_string()]);
    // Short enough to keep the suite quick, long enough that the actor's 1 s tick
    // is guaranteed to see the expiry before the handler's own deadline does.
    cfg.limits.no_worker_grace_ms = 800;
    cfg.limits.flush_interval_ms = 20;
    cfg.limits.flush_bytes = 4_096;
    cfg.validate().expect("test config must validate");
    cfg
}

/// A test hub: its address, the `Hub` behind it (kept alive by the caller so the
/// actor tasks are not dropped mid-test), and the spill directory to clean up.
struct TestHub {
    addr: std::net::SocketAddr,
    /// The `Hub` behind the address. Holding a second `Arc` is what keeps the hub (and
    /// every pool actor it owns) alive for the lifetime of the test, and the tests that
    /// assert on hub-side bookkeeping (the routing index, the relay map, the idempotency
    /// mirror) read it directly rather than inferring it from a response.
    hub: Arc<crate::state::Hub>,
    root: String,
}

impl Drop for TestHub {
    /// Best-effort teardown: these tests write real artifact parts to disk, and a
    /// green run must not leave untracked spill in the working tree.
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// Serve the **production router** on an ephemeral loopback port.
///
/// `name` isolates the artifact spill: one directory per test, because `cargo test`
/// runs these in parallel and two hubs sharing a spill directory would race on the
/// per-attempt staging file (`io::write_chunk` appends to
/// `{object}.parts/a{attempt:04}/staging.bin`, `io.rs:759`). The directory is the
/// repo-root scratch location (`io::test_scratch`, i.e. `<repo>/Temp/nexus-tests/<name>`)
/// rather than the package's `var/`, per the root `AGENTS.md` rule: this is
/// throwaway test state, not a hub's configured artifact root. The runtime default
/// (`Temp/nexus/artifacts`, `NEXUS_ARTIFACT_ROOT`) is untouched.
async fn spawn_hub(name: &str, cfg: BrokerConfig) -> anyhow::Result<TestHub> {
    let root = crate::io::test_scratch(&format!("e2e-{name}"))
        .display()
        .to_string();
    // Start from an empty tree so a leftover part from an aborted run can never
    // satisfy a digest assertion in this one.
    let _ = std::fs::remove_dir_all(&root);
    crate::io::ensure_root(&root).await?;
    let hub = crate::state::Hub::new(cfg, &root);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    // The serving task owns its own handle: cloning before the `async move` keeps
    // `hub` alive for the caller (the tests assert against it after `spawn`).
    let serving = Arc::clone(&hub);
    tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, crate::app(serving, 1 << 20)).await {
            eprintln!("[e2e] test hub stopped: {err}");
        }
    });
    Ok(TestHub { addr, hub, root })
}

/// A worker that dials out and handshakes. `ready` additionally sends
/// `Ready{engine_ready:true}` and a `Claim`, i.e. a worker the hub may dispatch to.
async fn dial_worker(
    addr: std::net::SocketAddr,
    worker_id: &str,
    ready: bool,
) -> anyhow::Result<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>> {
    let url = format!("ws://{addr}/v1/worker?token={POOL_TOKEN}");
    let (mut ws, _response) = connect_async(url).await?;
    let caps = WireCaps::default();
    let hello = WorkerFrame::Hello {
        worker_id: worker_id.to_string(),
        pools: vec![POOL.to_string()],
        capacity: 4,
        requires: Requires {
            model: Some("qwen38".to_string()),
            min_context_tokens: Some(524_288),
            capabilities: BTreeSet::from(["tools".to_string()]),
            kv_estimate_tokens: None,
        },
        attestation: Attestation {
            model_shas: BTreeSet::from([SHARD_1.to_string()]),
            weight_bytes: Some(83_863_586_496),
        },
        protocol: nexus_core::PROTOCOL_VERSION,
        engine: Some("strata".to_string()),
        tier: Some(nexus_core::WorkerTier::Dedicated),
    };
    ws.send(Message::Text(encode_line(&hello, &caps)?.into()))
        .await?;
    if ready {
        ws.send(Message::Text(encode_line(&ready_frame(), &caps)?.into()))
            .await?;
        ws.send(Message::Text(
            encode_line(&WorkerFrame::Claim { jobs: Some(4) }, &caps)?.into(),
        ))
        .await?;
    }
    Ok(ws)
}

/// The heartbeat that makes a worker dispatchable: engine ready, four slots, enough
/// KV for the pool's declared context.
#[must_use]
fn ready_frame() -> WorkerFrame {
    WorkerFrame::Ready {
        free_slots: 4,
        free_kv_tokens: Some(524_288),
        engine_ready: true,
        telemetry: None,
    }
}

/// A worker the hub may admit a task for but never picks one up for: `hello` and
/// `Ready`, no `Claim`. The distinction the caller needs is the job's phase — with a
/// ready worker the record reaches `Queued`, which NEX-6.6's sweep may delete, while a
/// `Claim` would move it to `Assigned`, which the sweep deliberately leaves alone
/// because a lease means GPU work has been committed (`actor.rs:3221-3227`).
async fn dial_ready_but_idle_worker(
    addr: std::net::SocketAddr,
    worker_id: &str,
) -> anyhow::Result<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>> {
    let mut ws = dial_worker(addr, worker_id, false).await?;
    ws.send(Message::Text(
        encode_line(&ready_frame(), &WireCaps::default())?.into(),
    ))
    .await?;
    Ok(ws)
}

/// The next hub frame of the named kind (`welcome`, `assigned`, `cancel`, …).
///
/// Anything else on the socket is *read and skipped* rather than ignored silently:
/// a hub that sends the wrong thing surfaces as "timed out waiting for X", with the
/// frames it did send visible in the failure of the calling assertion.
async fn next_frame_of(
    ws: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    kind: &str,
) -> anyhow::Result<HubFrame> {
    let until = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(until);
    loop {
        // The select only *produces* the message: replying to a ping needs another
        // mutable borrow of the socket, which must not overlap the `Next` future.
        let message = tokio::select! {
            _ = &mut until => return Err(anyhow::anyhow!("timed out waiting for a `{kind}` frame")),
            message = ws.next() => message,
        };
        let text = match message {
            Some(Ok(Message::Text(text))) => text.to_string(),
            Some(Ok(Message::Binary(bytes))) => String::from_utf8_lossy(&bytes).into_owned(),
            Some(Ok(Message::Ping(payload))) => {
                ws.send(Message::Pong(payload)).await?;
                continue;
            }
            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                return Err(anyhow::anyhow!("the hub closed the worker socket"))
            }
            Some(Ok(_)) => continue,
        };
        // One message may carry several lines; the hub writes one frame per line.
        for line in text.split('\n').filter(|line| !line.trim().is_empty()) {
            let frame = decode_hub_frame(line.trim(), &WireCaps::default())?;
            let name = match &frame {
                HubFrame::Welcome { .. } => "welcome",
                HubFrame::Assigned { .. } => "assigned",
                HubFrame::Cancel { .. } => "cancel",
                HubFrame::Ack { .. } => "ack",
                HubFrame::Reject { .. } => "reject",
                HubFrame::ResumeNack { .. } => "resume_nack",
                HubFrame::Bye { .. } => "bye",
            };
            if name == kind {
                return Ok(frame);
            }
        }
    }
}

/// The consumer body every test posts: an OpenAI chat request with streaming on.
fn chat_body(content: &str) -> serde_json::Value {
    serde_json::json!({"model": "qwen38", "messages": [{"role": "user", "content": content}], "stream": true})
}

/// §16.6 at runtime: a connected-but-loading worker makes the request serviceable
/// in principle, so it is not a `400`; when nobody becomes ready inside the grace
/// window the consumer must get `503 + Retry-After` **and no SSE body at all**,
/// because at that point the status is still free (task §7.2, §11).
#[tokio::test]
async fn loading_worker_answers_503_with_retry_after_before_any_body() -> anyhow::Result<()> {
    let hub = spawn_hub("loading_503", test_config()).await?;
    let mut ws = dial_worker(hub.addr, "wkr-loading", false).await?;
    // The handshake must be accepted before the consumer arrives, otherwise this
    // test would be measuring "no workers at all" (a 400) instead.
    assert!(matches!(
        next_frame_of(&mut ws, "welcome").await?,
        HubFrame::Welcome { .. }
    ));

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{}/v1/chat/completions", hub.addr))
        .bearer_auth(API_KEY)
        .json(&chat_body("hi"))
        .send()
        .await?;

    assert_eq!(
        response.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "grace expiry must answer 503"
    );
    let retry_after = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    assert!(retry_after.is_some(), "a 503 must carry Retry-After");
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert_eq!(
        content_type, "application/json",
        "the answer is a plain JSON error, not an opened SSE body"
    );
    let text = response.text().await?;
    assert!(
        text.contains("no_ready_worker"),
        "expected the retryable no-capacity code, got {text}"
    );
    Ok(())
}

/// The M0/M1 pipe end to end: the worker claims, the hub dispatches with a seq
/// floor, the worker relays an OpenAI-shaped `tool_calls` fragment, and the
/// consumer's SSE body carries that fragment **verbatim**, one finish reason, and
/// exactly one `[DONE]` (§16.14; §16.4's no-double-finish half; §7.2's headers).
#[tokio::test]
async fn worker_relay_reaches_the_consumer_verbatim_over_sse() -> anyhow::Result<()> {
    let hub = spawn_hub("relay_sse", test_config()).await?;
    let mut ws = dial_worker(hub.addr, "wkr-relay", true).await?;
    assert!(matches!(
        next_frame_of(&mut ws, "welcome").await?,
        HubFrame::Welcome { .. }
    ));

    let chunk = serde_json::json!({
        "id": "chatcmpl-x",
        "object": "chat.completion.chunk",
        "choices": [{"index": 0, "delta": {"tool_calls": [{"index": 0, "id": "c1", "function": {"name": "get_weather", "arguments": "{\"ci"}}]}, "finish_reason": null}],
    });
    let client = reqwest::Client::new();
    let mut body = chat_body("weather?");
    body["tools"] = serde_json::json!([{"type": "function"}]);
    let response = client
        .post(format!("http://{}/v1/chat/completions", hub.addr))
        .bearer_auth(API_KEY)
        .json(&body)
        .send()
        .await?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "a ready worker means the stream opens"
    );
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        content_type.starts_with("text/event-stream"),
        "expected SSE, got {content_type}"
    );
    assert_eq!(
        response
            .headers()
            .get("x-accel-buffering")
            .and_then(|v| v.to_str().ok()),
        Some("no"),
        "§7.2: intermediaries must not buffer"
    );

    let assigned = next_frame_of(&mut ws, "assigned").await?;
    let (job, start_seq) = match assigned {
        HubFrame::Assigned { job, start_seq, .. } => (job, start_seq),
        other => return Err(anyhow::anyhow!("expected an Assigned, got {other:?}")),
    };
    assert_eq!(start_seq, 0, "a first attempt numbers from 1");

    let caps = WireCaps::default();
    let delta = WorkerFrame::Frame {
        job: job.clone(),
        seq: start_seq + 1,
        kind: FrameKind::Delta,
        content_type: Some("application/json".to_string()),
        text: chunk.to_string(),
        artifact: None,
    };
    ws.send(Message::Text(encode_line(&delta, &caps)?.into()))
        .await?;
    let done = WorkerFrame::Done {
        job: job.clone(),
        seq: start_seq + 2,
        finish: FinishKind::Stop,
        usage: Some(serde_json::json!({"completion_tokens": 3})),
        error: None,
    };
    ws.send(Message::Text(encode_line(&done, &caps)?.into()))
        .await?;

    // `text()` resolves once the handler ends its body, which is itself the
    // assertion that the stream terminated instead of hanging.
    let text = tokio::time::timeout(Duration::from_secs(10), response.text()).await??;

    // The OpenAI chunk stream is exactly three chunks plus the sentinel: the hub's
    // own role delta (headers-before-dispatch, §7.2), the worker's frame relayed as
    // its own event, and the terminal chunk that carries `finish_reason` + `usage`.
    // Counting the `object` label is the "no duplicates" half of §16.4/§16.14: a
    // relay that emitted the same delta twice, or a finish chunk emitted twice, lands
    // on 4 here.
    assert_eq!(
        text.matches("chat.completion.chunk").count(),
        3,
        "role delta + the relayed chunk + the finish chunk, no duplicates: {text}"
    );
    assert_eq!(
        text.matches("get_weather").count(),
        1,
        "the tool-call fragment is relayed exactly once: {text}"
    );
    let payloads: Vec<serde_json::Value> = text
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter(|line| *line != "[DONE]")
        .map(|line| serde_json::from_str(line).unwrap_or_else(|_| panic!("chunk is JSON: {line}")))
        .collect();
    assert_eq!(payloads.len(), 3, "one SSE data payload per chunk: {text}");
    assert!(
        payloads
            .iter()
            .all(|chunk| chunk["object"] == serde_json::json!("chat.completion.chunk")),
        "every event is an OpenAI chunk object: {payloads:?}"
    );
    assert_eq!(
        payloads[0]["choices"][0]["delta"]["role"].as_str(),
        Some("assistant"),
        "§7.2: the role delta opens the stream"
    );
    assert_eq!(
        payloads[1]["choices"][0]["delta"]["tool_calls"][0]["function"]["name"].as_str(),
        Some("get_weather"),
        "§16.14: the tool-call fragment survives the relay"
    );
    assert_eq!(
        payloads[1]["choices"][0]["delta"]["tool_calls"][0]["function"]["arguments"].as_str(),
        Some("{\"ci"),
        "a half-written tool argument is not re-tokenized: {text}"
    );
    assert_eq!(
        payloads[1]["id"].as_str(),
        Some("chatcmpl-x"),
        "the worker's own chunk fields travel untouched"
    );
    assert_eq!(
        payloads[2]["choices"][0]["finish_reason"].as_str(),
        Some("stop"),
        "§16.4: exactly one finish_reason, on the last chunk"
    );
    assert_eq!(
        payloads[2]["usage"],
        serde_json::json!({"completion_tokens": 3}),
        "usage is relayed verbatim"
    );
    assert!(
        text.contains("id: 1\n") && text.contains("id: 2\n"),
        "the two relayed frames keep their seqs as `Last-Event-ID` cursors (§7.5): {text}"
    );
    assert_eq!(
        text.matches("[DONE]").count(),
        1,
        "exactly one [DONE] sentinel: {text}"
    );
    Ok(())
}

/// §6 at runtime: when the consumer's socket goes away the hub must tell the
/// worker. The core proves the decision (`t05_...`); this proves the native
/// runtime sends the `Detach` that triggers it — before the review pass it never
/// did, so a client that hung up left the GPU decoding for nobody.
#[tokio::test]
async fn consumer_disconnect_reaches_the_worker_as_cancel() -> anyhow::Result<()> {
    let hub = spawn_hub("disconnect_cancel", test_config()).await?;
    let mut ws = dial_worker(hub.addr, "wkr-cancel", true).await?;
    assert!(matches!(
        next_frame_of(&mut ws, "welcome").await?,
        HubFrame::Welcome { .. }
    ));

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{}/v1/chat/completions", hub.addr))
        .bearer_auth(API_KEY)
        .json(&chat_body("long one"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let assigned = next_frame_of(&mut ws, "assigned").await?;
    let job = match assigned {
        HubFrame::Assigned { job, .. } => job,
        other => return Err(anyhow::anyhow!("expected an Assigned, got {other:?}")),
    };

    // Dropping the response without draining the body is an SSE client hanging up.
    drop(response);
    let caps = WireCaps::default();
    // Any inbound frame gives the handler's pump a chance to notice the closed
    // body; the hub's own write is what the assertion waits for.
    let still_working = WorkerFrame::Frame {
        job: job.clone(),
        seq: 1,
        kind: FrameKind::Log,
        content_type: None,
        text: "still generating".to_string(),
        artifact: None,
    };
    ws.send(Message::Text(encode_line(&still_working, &caps)?.into()))
        .await?;

    let frame =
        tokio::time::timeout(Duration::from_secs(5), next_frame_of(&mut ws, "cancel")).await??;
    match frame {
        HubFrame::Cancel { job: cancelled, .. } => {
            assert_eq!(cancelled, JobId::new_unchecked(job.as_ref()))
        }
        other => return Err(anyhow::anyhow!("expected a Cancel, got {other:?}")),
    }
    Ok(())
}

/// §9 tenancy at runtime: job-scoped reads are consumer reads, and the operational
/// endpoints stay open for a load balancer. The `/metrics` assertion also pins the
/// exposition shape the review pass fixed (one label set per key, gauges present).
#[tokio::test]
async fn job_reads_require_an_api_key_and_operational_routes_do_not() -> anyhow::Result<()> {
    let hub = spawn_hub("tenancy", test_config()).await?;
    let client = reqwest::Client::new();

    let anonymous = client
        .get(format!("http://{}/v1/jobs", hub.addr))
        .send()
        .await?;
    assert_eq!(
        anonymous.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "a job listing is a consumer read"
    );

    let scoped = client
        .get(format!("http://{}/v1/jobs", hub.addr))
        .bearer_auth(API_KEY)
        .send()
        .await?;
    assert_eq!(scoped.status(), reqwest::StatusCode::OK);

    let unknown = client
        .get(format!("http://{}/v1/jobs/job-does-not-exist", hub.addr))
        .bearer_auth(API_KEY)
        .send()
        .await?;
    assert_eq!(unknown.status(), reqwest::StatusCode::NOT_FOUND);

    let health = client
        .get(format!("http://{}/healthz", hub.addr))
        .send()
        .await?;
    assert!(
        health.status() == reqwest::StatusCode::OK
            || health.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "healthz answers without a key, degraded or not"
    );

    let metrics = client
        .get(format!("http://{}/metrics", hub.addr))
        .send()
        .await?;
    assert_eq!(metrics.status(), reqwest::StatusCode::OK);
    let text = metrics.text().await?;
    assert!(
        !text.contains("}{"),
        "each metric key carries exactly one label set: {text}"
    );
    assert!(
        text.contains("nexus_queue_depth"),
        "gauges must be exported, not only counters: {text}"
    );
    Ok(())
}

// -------------------------------------------------- the consumer WebSocket face (NEX-3)

/// The kind the consumer socket posts: a streaming LLM kind on the pool the test
/// worker offers, so the capability pre-check passes and the deltas are real.
const WS_KIND: &str = "llm.completion";

/// The `Sec-WebSocket-Key` every consumer dial in this file sends: the RFC 6455
/// sample value, fixed so the handshake (and the `accept` the client verifies) is
/// the same on every run.
const WS_HANDSHAKE_KEY: &str = "dGhlIHNhbXBsZSBub25jZQ==";

/// A consumer on `GET /v1/consumer`, authenticated with the test API key.
///
/// `connect_async` needs a request, not a URL, because the consumer face is keyed
/// on the `Authorization` header exactly as the HTTP surfaces are (spec §7: the
/// consumer direction presents the API key, never the pool token).
async fn dial_consumer(
    addr: std::net::SocketAddr,
) -> anyhow::Result<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>> {
    // `connect_async` takes a *request* here rather than a URL because the face is
    // keyed on the `Authorization` header exactly as the HTTP surfaces are (spec
    // §7: the consumer direction presents the API key, never the pool token).
    // It also means the handshake headers are ours to supply — tungstenite validates
    // that a caller-built request carries `Host`/`Connection`/`Upgrade`/
    // `Sec-WebSocket-Version`/`Sec-WebSocket-Key` and derives the expected `accept`
    // from the key, so a missing one fails client-side before the hub is reached.
    // The key is the RFC 6455 sample value, which keeps the handshake deterministic.
    let request = axum::http::Request::builder()
        .method("GET")
        .header("host", addr.to_string())
        .header("connection", "upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", WS_HANDSHAKE_KEY)
        .header("authorization", format!("Bearer {API_KEY}"))
        .uri(format!("ws://{addr}/v1/consumer"))
        .body(())?;
    let (ws, response) = connect_async(request).await?;
    assert_eq!(
        response.status(),
        axum::http::StatusCode::SWITCHING_PROTOCOLS,
        "a valid API key gets the upgrade, not a closed socket"
    );
    // Nothing is read here on purpose: the consumer dialect has no handshake frame
    // (NEX-2.2 keeps the two vocabularies disjoint — a consumer has no version or
    // attestation to offer), so the first message on the socket is always the
    // client's own, and the first reply is always a verdict.
    Ok(ws)
}

/// Sends one consumer frame (a JSON object per text message, NEX-2.2).
async fn send_consumer(
    ws: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    frame: &serde_json::Value,
) -> anyhow::Result<()> {
    ws.send(Message::Text(frame.to_string().into())).await?;
    Ok(())
}

/// Posts one task on the consumer socket. `prompt` doubles as the task's identity:
/// it travels to the worker verbatim, which is how the assertions below can tell
/// one task's `Assigned` from another's.
async fn consumer_submit(
    ws: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    prompt: &str,
    detach: bool,
) -> anyhow::Result<()> {
    send_consumer(
        ws,
        &serde_json::json!({
            "type": "submit",
            "kind": WS_KIND,
            "payload": {"prompt": prompt},
            "detach": detach,
        }),
    )
    .await
}

/// The next frame on the consumer socket, decoded into the hub→consumer dialect.
///
/// The five-second budget matches `next_frame_of`'s: a broken expectation should
/// fail with the frames it *did* get, not hang until the test harness gives up.
async fn next_consumer_frame(
    ws: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
) -> anyhow::Result<nexus_core::protocol::HubConsumerFrame> {
    let until = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(until);
    loop {
        let message = tokio::select! {
            _ = &mut until => return Err(anyhow::anyhow!("timed out waiting for a consumer frame")),
            message = ws.next() => message,
        };
        let text = match message {
            Some(Ok(Message::Text(text))) => text.to_string(),
            Some(Ok(Message::Binary(bytes))) => String::from_utf8_lossy(&bytes).into_owned(),
            Some(Ok(Message::Ping(payload))) => {
                ws.send(Message::Pong(payload)).await?;
                continue;
            }
            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                return Err(anyhow::anyhow!("the hub closed the consumer socket"))
            }
            Some(Ok(_)) => continue,
        };
        // One JSON object per text message, so the first non-empty line *is* the frame.
        // A message with nothing in it (a bare pong payload would be one) is not a
        // frame, and the read just goes around again.
        let Some(line) = text.split('\n').find(|line| !line.trim().is_empty()) else {
            continue;
        };
        return Ok(nexus_core::protocol::decode_hub_consumer_frame(
            line.trim(),
            &WireCaps::default(),
        )?);
    }
}

/// NEX-3.1/3.2/3.3/3.4 at runtime: two tasks on one socket, each verdict followed
/// by the snapshot that opens its stream, each task's stream tagged with its own id,
/// and exactly one `end` per task.
#[tokio::test]
async fn one_consumer_socket_carries_two_tasks_and_answers_both_in_order() -> anyhow::Result<()> {
    let hub = spawn_hub("ws_two_tasks", test_config()).await?;
    let mut worker = dial_worker(hub.addr, "wkr-two-tasks", true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));
    let mut consumer = dial_consumer(hub.addr).await?;

    // Both submits are written before a single verdict is read: NEX-3.3 promises
    // verdicts in submission order for a client that does not wait, which is the
    // only ordering guarantee a pipelining client can rely on.
    consumer_submit(&mut consumer, "first", false).await?;
    consumer_submit(&mut consumer, "second", false).await?;

    let mut jobs = Vec::new();
    for expected in 0..2 {
        match next_consumer_frame(&mut consumer).await? {
            nexus_core::protocol::HubConsumerFrame::Accepted {
                job,
                kind,
                pool,
                detached,
            } => {
                assert_eq!(kind, WS_KIND, "the verdict echoes the kind it accepted");
                assert_eq!(pool.as_ref(), POOL);
                assert!(!detached, "a plain submit keeps the NEX-6.1 contract");
                jobs.push(job);
            }
            other => {
                return Err(anyhow::anyhow!(
                    "expected verdict #{expected}, got {other:?}"
                ))
            }
        }
        // The pair the hub owes for each accepted task is `accepted` then `meta`
        // (§5.4 item 5: the snapshot opens the stream). Reading it here rather than
        // skipping it is what proves `meta` sits ahead of every event byte of its own
        // task, and that the next task's verdict cannot jump the queue.
        match next_consumer_frame(&mut consumer).await? {
            nexus_core::protocol::HubConsumerFrame::Meta {
                job,
                kind,
                pool,
                state,
                attached_ms,
                ..
            } => {
                assert_eq!(job, jobs[expected], "the snapshot names the task it opens");
                assert_eq!(kind, WS_KIND);
                assert_eq!(pool, POOL);
                // The phase is whatever the actor's record held at attach time, which
                // races with the dispatch below, so this asserts presence, not value.
                assert!(state.is_some(), "the snapshot carries the task's phase");
                assert!(
                    attached_ms.is_some(),
                    "the snapshot carries the attach time"
                );
            }
            other => {
                return Err(anyhow::anyhow!(
                    "expected the snapshot that opens task #{expected}, got {other:?}"
                ));
            }
        }
    }
    assert_ne!(jobs[0], jobs[1], "two submissions are two tasks");

    // The worker sees the tasks in arrival order and answers the first one to
    // completion before touching the second, so the per-task grouping the consumer
    // is about to observe is forced by the wire, not incidental.
    let mut slots = Vec::new();
    for index in 0..2 {
        let assigned = next_frame_of(&mut worker, "assigned").await?;
        let HubFrame::Assigned { job, .. } = assigned.clone() else {
            return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
        };
        let marker = if index == 0 { "first" } else { "second" };
        assert!(
            serde_json::to_string(&assigned)?.contains(marker),
            "task {} must carry its own payload: {assigned:?}",
            index + 1
        );
        slots.push(job);
    }
    assert_eq!(slots, jobs, "the hub dispatches in the order it accepted");

    let caps = WireCaps::default();
    for (slot, (a, b)) in slots.iter().zip([("one", "two"), ("three", "four")]) {
        for (seq, text) in [(1_u64, a), (2, b)] {
            let delta = WorkerFrame::Frame {
                job: slot.clone(),
                seq,
                kind: FrameKind::Delta,
                content_type: None,
                text: text.to_string(),
                artifact: None,
            };
            worker
                .send(Message::Text(encode_line(&delta, &caps)?.into()))
                .await?;
        }
        let done = WorkerFrame::Done {
            job: slot.clone(),
            seq: 3,
            finish: FinishKind::Stop,
            usage: Some(serde_json::json!({"completion_tokens": 2})),
            error: None,
        };
        worker
            .send(Message::Text(encode_line(&done, &caps)?.into()))
            .await?;
    }

    // The exact stream the consumer is owed: per task, two deltas, the finish, and
    // one `end` naming the highest seq delivered. Nothing else on the socket, so a
    // cross-streamed delta (NEX-6.5) or a duplicated terminal (NEX-3.4) shows up
    // here as the first mismatching element.
    let mut seen = Vec::new();
    for _ in 0..8 {
        seen.push(next_consumer_frame(&mut consumer).await?);
    }
    let slot_of = |job: &JobId| -> &'static str {
        if job == &slots[0] {
            "a"
        } else {
            "b"
        }
    };
    let mut actual = Vec::new();
    for frame in &seen {
        actual.push(match frame {
            nexus_core::protocol::HubConsumerFrame::Delta { job, seq, text } => {
                format!("{} delta {seq} {text}", slot_of(job))
            }
            nexus_core::protocol::HubConsumerFrame::Finish {
                job,
                seq,
                finish,
                usage,
            } => {
                assert_eq!(*finish, FinishKind::Stop);
                assert_eq!(
                    usage.as_ref(),
                    Some(&serde_json::json!({"completion_tokens": 2}))
                );
                format!("{} finish {seq}", slot_of(job))
            }
            nexus_core::protocol::HubConsumerFrame::End {
                job,
                reason,
                through_seq,
            } => {
                assert_eq!(*reason, nexus_core::protocol::EndReason::Finished);
                assert_eq!(
                    *through_seq,
                    Some(3),
                    "the cursor a reconnecting consumer needs"
                );
                format!("{} end", slot_of(job))
            }
            other => return Err(anyhow::anyhow!("unexpected frame on the socket: {other:?}")),
        });
    }
    assert_eq!(
        actual,
        [
            "a delta 1 one",
            "a delta 2 two",
            "a finish 3",
            "a end",
            "b delta 1 three",
            "b delta 2 four",
            "b finish 3",
            "b end",
        ],
        "one socket, two tagged streams, one end each"
    );
    Ok(())
}

/// NEX-3.3 at runtime, the refusal half: a task refused under the tenant concurrency
/// cap is reported with the HTTP surface's code and its retry hint. Nothing that
/// streams precedes it — the only frame ahead of this verdict is the snapshot that
/// opened the accepted task's stream, never an event byte.
#[tokio::test]
async fn a_refusal_reaches_the_consumer_before_any_event_bytes() -> anyhow::Result<()> {
    let mut cfg = test_config();
    // One job in flight for the tenant: the second submission is the refused one.
    // Deterministic because the cap is checked in `on_submit` before the queue and
    // before the capability pre-check (`actor.rs:1068-1080`).
    cfg.tenants
        .get_mut("default")
        .expect("the sample topology defines the default tenant")
        .max_concurrent = Some(1);
    let hub = spawn_hub("ws_refusal", cfg).await?;
    let mut worker = dial_worker(hub.addr, "wkr-refusal", true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));
    let mut consumer = dial_consumer(hub.addr).await?;

    consumer_submit(&mut consumer, "allowed", false).await?;
    consumer_submit(&mut consumer, "refused", false).await?;
    // The worker has not been given anything to answer yet, so no event frame can
    // exist on this socket at all: whatever arrives in the next two reads is a
    // verdict, and the assertion below is what NEX-3.3 is about.
    let first = next_consumer_frame(&mut consumer).await?;
    let accepted = match &first {
        nexus_core::protocol::HubConsumerFrame::Accepted { job, .. } => job.clone(),
        other => return Err(anyhow::anyhow!("the first task is accepted, got {other:?}")),
    };
    // The snapshot that opens the first task's stream comes next, still ahead of the
    // second task's verdict: §5.4 item 5 puts `meta` first in the stream it opens, and
    // a submit's verdict is the first frame of its own stream.
    match next_consumer_frame(&mut consumer).await? {
        nexus_core::protocol::HubConsumerFrame::Meta { job, .. } => {
            assert_eq!(job, accepted, "the snapshot belongs to the task it opens");
        }
        other => return Err(anyhow::anyhow!("expected a snapshot, got {other:?}")),
    }
    let second = next_consumer_frame(&mut consumer).await?;

    // The wire shape, exactly, apart from the hub-minted job id (a refusal names a
    // task the client never chose an id for). Same code, message and retry hint the
    // HTTP surface would have sent with `429`.
    let value = serde_json::to_value(&second)?;
    let mut object = value.as_object().expect("a frame is a JSON object").clone();
    let job = object
        .remove("job")
        .expect("a refusal names the task it refuses");
    assert!(
        job.as_str().is_some_and(|id| !id.is_empty()),
        "the refusal is tied to a task id: {job}"
    );
    assert_eq!(
        // Re-wrapped as a `Value` so the comparison is against the whole expected
        // frame, field for field, rather than a per-key check.
        serde_json::Value::Object(object),
        serde_json::json!({
            "type": "rejected",
            "status": 429,
            "code": "tenant_limit",
            "message": "tenant already has 1 jobs in flight",
            "retry_after_secs": 1,
        }),
        "§5.4: the refusal carries the HTTP surface's code and retry hint"
    );

    // Nothing else has been written on the socket: the refused task produced no
    // stream, and the accepted one has not been answered yet.
    drop(consumer);
    Ok(())
}

/// NEX-6.1/NEX-6.3 at runtime on the consumer socket: dropping the socket must
/// reach the worker as a `Cancel` and delete the task, with no goodbye frame sent.
#[tokio::test]
async fn dropping_a_consumer_socket_cancels_and_deletes_its_task() -> anyhow::Result<()> {
    let hub = spawn_hub("ws_hangup", test_config()).await?;
    let mut worker = dial_worker(hub.addr, "wkr-hangup", true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));

    let job = {
        let mut consumer = dial_consumer(hub.addr).await?;
        consumer_submit(&mut consumer, "orphan me", false).await?;
        let nexus_core::protocol::HubConsumerFrame::Accepted { job, .. } =
            next_consumer_frame(&mut consumer).await?
        else {
            return Err(anyhow::anyhow!("expected an accepted verdict"));
        };
        let assigned = next_frame_of(&mut worker, "assigned").await?;
        let HubFrame::Assigned {
            job: assigned_job, ..
        } = assigned
        else {
            return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
        };
        assert_eq!(assigned_job, job);
        // The hangup itself: no `detach`, no `cancel`, no close frame — the socket
        // is the subscription, and dropping it is the whole event (NEX-6.4).
        drop(consumer);
        job
    };

    let cancel = tokio::time::timeout(Duration::from_secs(5), next_frame_of(&mut worker, "cancel"))
        .await
        .map_err(|_| anyhow::anyhow!("the worker never heard about the consumer leaving"))??;
    match cancel {
        HubFrame::Cancel { job: cancelled, .. } => assert_eq!(cancelled, job),
        other => return Err(anyhow::anyhow!("expected a Cancel, got {other:?}")),
    }

    // NEX-6.3's "deleted", not "retained": the record is gone, so a later read is a
    // `404` rather than an orphan's answer.
    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://{}/v1/jobs/{job}", hub.addr))
        .bearer_auth(API_KEY)
        .send()
        .await?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::NOT_FOUND,
        "an orphaned task is deleted, not kept for a stranger"
    );
    Ok(())
}

/// NEX-6.6 at runtime: `detach:true` is the declared-detached opt-in. The stream
/// closes on request with its own reason, the socket may then go away, and the task
/// keeps running for a consumer that polls later.
#[tokio::test]
async fn an_explicit_detach_ends_the_stream_and_keeps_the_task_alive() -> anyhow::Result<()> {
    let hub = spawn_hub("ws_detach", test_config()).await?;
    let mut worker = dial_worker(hub.addr, "wkr-detach", true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));
    let mut consumer = dial_consumer(hub.addr).await?;

    consumer_submit(&mut consumer, "keep running", true).await?;
    let nexus_core::protocol::HubConsumerFrame::Accepted { job, detached, .. } =
        next_consumer_frame(&mut consumer).await?
    else {
        return Err(anyhow::anyhow!("expected an accepted verdict"));
    };
    assert!(
        detached,
        "the accepted verdict echoes the contract it granted"
    );
    // The snapshot that opens this stream (§5.4 item 5), ahead of the first delta.
    match next_consumer_frame(&mut consumer).await? {
        nexus_core::protocol::HubConsumerFrame::Meta { job: snapshot, .. } => {
            assert_eq!(snapshot, job, "the snapshot names the task it opens");
        }
        other => return Err(anyhow::anyhow!("expected a snapshot, got {other:?}")),
    }

    let assigned = next_frame_of(&mut worker, "assigned").await?;
    let HubFrame::Assigned {
        job: assigned_job, ..
    } = assigned
    else {
        return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
    };
    assert_eq!(assigned_job, job);

    // One delta, so the `end` frame has a cursor to report.
    let delta = WorkerFrame::Frame {
        job: job.clone(),
        seq: 1,
        kind: FrameKind::Delta,
        content_type: None,
        text: "still working".to_string(),
        artifact: None,
    };
    worker
        .send(Message::Text(
            encode_line(&delta, &WireCaps::default())?.into(),
        ))
        .await?;
    assert_eq!(
        next_consumer_frame(&mut consumer).await?,
        nexus_core::protocol::HubConsumerFrame::Delta {
            job: job.clone(),
            seq: 1,
            text: "still working".to_string(),
        },
        "the delta arrives before the detach is even sent"
    );

    send_consumer(
        &mut consumer,
        &serde_json::json!({"type": "detach", "job": job.to_string()}),
    )
    .await?;
    assert_eq!(
        next_consumer_frame(&mut consumer).await?,
        nexus_core::protocol::HubConsumerFrame::End {
            job: job.clone(),
            reason: nexus_core::protocol::EndReason::Detached,
            through_seq: Some(1),
        },
        "§5.4: an explicit detach closes the stream and names its own reason"
    );

    // The socket goes away while the task is still live and now has no consumer.
    drop(consumer);
    let quiet = tokio::time::timeout(
        // Longer than `no_worker_grace_ms` plus the actor's 1 s tick: a hub that
        // treated this as an orphan would have cancelled by now.
        Duration::from_millis(1_400),
        next_frame_of(&mut worker, "cancel"),
    )
    .await;
    // `as_ref` so the value survives into the failure message: what arrived instead of
    // a `Cancel` is the evidence.
    assert!(
        quiet.as_ref().is_err(),
        "a detached task keeps running (NEX-6.6), got {quiet:?}"
    );

    // And the task is still there to be polled — which is the whole point of the
    // opt-in, and what NEX-6.1 would have destroyed.
    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://{}/v1/jobs/{job}", hub.addr))
        .bearer_auth(API_KEY)
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let view = response.json::<serde_json::Value>().await?;
    // The job-status body names the task `job_id` (`adapters.rs:550`), which is the
    // same key `POST /v1/jobs` answers with.
    assert_eq!(view["job_id"].as_str(), Some(job.as_ref()));
    assert_eq!(
        view["consumers"].as_u64(),
        Some(0),
        "the detached task is watched by nobody, which is what bounds its lifetime"
    );
    Ok(())
}

/// NEX-3's `cancel` at runtime, and the retention half of NEX-6.3: cancelling on
/// purpose tells the worker, closes the stream, and **keeps** the record as a terminal
/// one — the opposite of hanging up, which deletes it outright.
#[tokio::test]
async fn an_explicit_cancel_closes_the_stream_and_keeps_the_record() -> anyhow::Result<()> {
    let hub = spawn_hub("ws_cancel", test_config()).await?;
    let mut worker = dial_worker(hub.addr, "wkr-cancel-explicit", true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));
    let mut consumer = dial_consumer(hub.addr).await?;

    consumer_submit(&mut consumer, "cancel me", false).await?;
    let nexus_core::protocol::HubConsumerFrame::Accepted { job, .. } =
        next_consumer_frame(&mut consumer).await?
    else {
        return Err(anyhow::anyhow!("expected an accepted verdict"));
    };
    expect_consumer_meta(&mut consumer, &job).await?;

    let assigned = next_frame_of(&mut worker, "assigned").await?;
    let HubFrame::Assigned {
        job: assigned_job, ..
    } = assigned
    else {
        return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
    };
    assert_eq!(assigned_job, job);

    // One delta first, so the stream has a cursor for its `end` to report, and so the
    // task is past the zero-bytes state that would make a retry legal.
    let delta = WorkerFrame::Frame {
        job: job.clone(),
        seq: 1,
        kind: FrameKind::Delta,
        content_type: None,
        text: "as far as it goes".to_string(),
        artifact: None,
    };
    worker
        .send(Message::Text(
            encode_line(&delta, &WireCaps::default())?.into(),
        ))
        .await?;
    expect_consumer_frame(
        &mut consumer,
        nexus_core::protocol::HubConsumerFrame::Delta {
            job: job.clone(),
            seq: 1,
            text: "as far as it goes".to_string(),
        },
    )
    .await?;

    send_consumer(
        &mut consumer,
        &serde_json::json!({"type": "cancel", "job": job.to_string()}),
    )
    .await?;

    // The supplier is told, with the reason the HTTP `DELETE /v1/jobs/{id}` sends: one
    // cause for both surfaces, so a worker cannot tell a socket loss from a decision.
    match next_frame_of(&mut worker, "cancel").await? {
        HubFrame::Cancel {
            job: cancelled,
            reason,
        } => {
            assert_eq!(cancelled, job);
            assert_eq!(
                reason,
                nexus_core::protocol::CancelReason::ConsumerGone,
                "a consumer cancel is the same cause from either face"
            );
        }
        other => return Err(anyhow::anyhow!("expected a Cancel, got {other:?}")),
    }

    // The stream, closed in band. The actor's `on_cancel` relays no terminal event of
    // its own (`actor.rs:1449-1480`), so without this frame the consumer would sit on an
    // open stream behind a task that already stopped existing.
    expect_consumer_frame(
        &mut consumer,
        nexus_core::protocol::HubConsumerFrame::end_for(
            &job,
            nexus_core::protocol::EndReason::Closed,
            Some(1),
        ),
    )
    .await?;

    // And the record is still there to be read: a deliberate cancellation is a consumer's
    // decision about a task, not a lost connection, so it keeps normal terminal retention
    // (NEX-6.3) instead of being deleted like an orphan.
    let client = reqwest::Client::new();
    let status = client
        .get(format!("http://{}/v1/jobs/{job}", hub.addr))
        .bearer_auth(API_KEY)
        .send()
        .await?
        .status();
    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "a cancelled task is retained, not deleted"
    );
    Ok(())
}

/// Reads the next consumer frame and requires it to be exactly `want`.
///
/// Whole-frame equality, not a per-field probe: a stream that is one frame short or one
/// frame long is the bug these tests exist to catch.
async fn expect_consumer_frame(
    ws: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    want: nexus_core::protocol::HubConsumerFrame,
) -> anyhow::Result<()> {
    let got = next_consumer_frame(ws).await?;
    assert_eq!(got, want, "the socket owes this exact frame");
    Ok(())
}

/// Reads the next consumer frame and requires it to be this task's opening snapshot,
/// returning nothing: the tests that are about the snapshot's contents assert its
/// fields, the others only need it out of the way.
async fn expect_consumer_meta(
    ws: &mut WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    job: &JobId,
) -> anyhow::Result<()> {
    match next_consumer_frame(ws).await? {
        nexus_core::protocol::HubConsumerFrame::Meta { job: meta, .. } => {
            assert_eq!(meta, *job, "the snapshot names the task it opens");
            Ok(())
        }
        other => Err(anyhow::anyhow!("expected a snapshot, got {other:?}")),
    }
}

/// NEX-3.2/3.4's join half at runtime: a task whose answer already ran out can be
/// followed again on the same socket, and the replay is a complete stream of its own —
/// verdict, snapshot, the buffered answer, one `end`.
///
/// This is what a consumer that dropped and re-attached at `end.through_seq` needs. The
/// task is still inside the actor's retention, so the replay ring is the source and the
/// hub adds nothing to it; the second stream being byte-for-byte the first one's answer
/// is the claim.
#[tokio::test]
async fn a_finished_task_can_be_followed_again_on_the_same_socket() -> anyhow::Result<()> {
    let hub = spawn_hub("ws_reattach", test_config()).await?;
    let mut worker = dial_worker(hub.addr, "wkr-reattach", true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));
    let mut consumer = dial_consumer(hub.addr).await?;

    consumer_submit(&mut consumer, "read me twice", false).await?;
    let nexus_core::protocol::HubConsumerFrame::Accepted { job, .. } =
        next_consumer_frame(&mut consumer).await?
    else {
        return Err(anyhow::anyhow!("expected an accepted verdict"));
    };
    expect_consumer_meta(&mut consumer, &job).await?;

    let assigned = next_frame_of(&mut worker, "assigned").await?;
    let HubFrame::Assigned {
        job: assigned_job, ..
    } = assigned
    else {
        return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
    };
    assert_eq!(assigned_job, job);

    let caps = WireCaps::default();
    let delta = WorkerFrame::Frame {
        job: job.clone(),
        seq: 1,
        kind: FrameKind::Delta,
        content_type: None,
        text: "twice".to_string(),
        artifact: None,
    };
    worker
        .send(Message::Text(encode_line(&delta, &caps)?.into()))
        .await?;
    let usage = serde_json::json!({"completion_tokens": 1});
    let done = WorkerFrame::Done {
        job: job.clone(),
        seq: 2,
        finish: FinishKind::Stop,
        usage: Some(usage.clone()),
        error: None,
    };
    worker
        .send(Message::Text(encode_line(&done, &caps)?.into()))
        .await?;

    let answer = [
        nexus_core::protocol::HubConsumerFrame::Delta {
            job: job.clone(),
            seq: 1,
            text: "twice".to_string(),
        },
        nexus_core::protocol::HubConsumerFrame::Finish {
            job: job.clone(),
            seq: 2,
            finish: FinishKind::Stop,
            usage: Some(usage),
        },
        nexus_core::protocol::HubConsumerFrame::end_for(
            &job,
            nexus_core::protocol::EndReason::Finished,
            Some(2),
        ),
    ];
    for want in answer.clone() {
        expect_consumer_frame(&mut consumer, want).await?;
    }

    // The join: same socket, same task, from the beginning of the ring.
    send_consumer(
        &mut consumer,
        &serde_json::json!({"type": "attach", "job": job.to_string(), "from_seq": 0}),
    )
    .await?;
    expect_consumer_frame(
        &mut consumer,
        nexus_core::protocol::HubConsumerFrame::Accepted {
            job: job.clone(),
            kind: WS_KIND.to_string(),
            pool: nexus_core::ids::PoolId::new_unchecked(POOL),
            // An attach cannot change the contract the task was submitted under.
            detached: false,
        },
    )
    .await?;
    // The snapshot says what the task is now, which is the only thing a joiner can
    // learn that the replay itself does not already say.
    match next_consumer_frame(&mut consumer).await? {
        nexus_core::protocol::HubConsumerFrame::Meta {
            job: meta,
            state,
            attached_ms,
            ..
        } => {
            assert_eq!(meta, job);
            assert_eq!(
                state.as_deref(),
                Some("Finished"),
                "the phase is the actor's, not a guess"
            );
            assert!(attached_ms.is_some(), "and it carries the attach time");
        }
        other => return Err(anyhow::anyhow!("expected a snapshot, got {other:?}")),
    }
    // Then the answer, identical to the first time, and its own single `end`.
    for want in answer {
        expect_consumer_frame(&mut consumer, want).await?;
    }
    Ok(())
}

/// NEX-6.6's other half at runtime: a task posted on the async job API is watched by
/// nobody from the moment the `202` is written, so its subscriber-less window runs and
/// the record stops existing.
///
/// That is only true because `create_job` hands the actor a `Detach` for the consumer
/// `ActorMsg::Submit` registers on the caller's behalf (`consumer.rs:585-604`). Left in
/// `record.consumers`, that phantom consumer made an async task look watched forever,
/// and neither NEX-6.6's window nor NEX-6.3's deletion could ever fire for it. The test
/// ends by posting the **same idempotency key** again: because the actor's
/// `Effect::JobForgotten` prunes the hub's mirror along with the record, that retry is new
/// work rather than a replay of a task that no longer exists.
#[tokio::test]
async fn a_task_posted_without_a_consumer_is_deleted_when_its_window_closes() -> anyhow::Result<()>
{
    let mut cfg = test_config();
    // The window itself, shortened for the test. Deleting is a tick step
    // (`actor.rs:3232-3257`), so the sweep lands on the actor's 1 s tick rather than
    // at the exact millisecond the TTL lapses.
    cfg.limits.detached_orphan_ttl_ms = 300;
    let hub = spawn_hub("orphan_window", cfg).await?;
    let client = reqwest::Client::new();

    // Ready, so the task is admitted into `Queued` and the `202` is immediate; never
    // claiming, so it stays there. Held open for the whole test (hence `_`): the
    // capability pre-check reads live workers, so hanging up here would make the task
    // unserviceable rather than merely unwatched.
    let _idle_worker = dial_ready_but_idle_worker(hub.addr, "wkr-orphan-window").await?;
    let response = client
        .post(format!("http://{}/v1/jobs", hub.addr))
        .bearer_auth(API_KEY)
        // Named so the retry at the end of the test is provably the *same* key: the
        // content-hash fallback would make it a different one by construction.
        .header("Idempotency-Key", "orphan-window-key")
        .json(&serde_json::json!({
            "kind": WS_KIND,
            "payload": {"prompt": "nobody is watching"},
            // NEX-6.6: the lifetime has to be stated, and this route only accepts the
            // detached reading (`the_async_job_route_requires_detach_to_be_stated`).
            "detach": true,
        }))
        .send()
        .await?;
    let status = response.status();
    let text = response.text().await?;
    assert_eq!(
        status,
        reqwest::StatusCode::ACCEPTED,
        "a detached post is accepted: {text}"
    );
    let body: serde_json::Value = serde_json::from_str(&text)?;
    let job = body["job_id"]
        .as_str()
        .expect("a 202 names the task it accepted")
        .to_string();

    // The phantom, proved: the record reports nobody watching it, which is the
    // precondition for the window existing at all. A hub that left the submitting
    // consumer registered answers `1` here — and then never deletes anything.
    let view = client
        .get(format!("http://{}/v1/jobs/{job}", hub.addr))
        .bearer_auth(API_KEY)
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;
    assert_eq!(
        view["consumers"].as_u64(),
        Some(0),
        "an async job has no consumer, so its window is already running: {view}"
    );

    // The window, closing: the record goes away, and a poll after that is a `404`
    // rather than an orphan kept for `job_retention_ms` that nobody will ever read.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let status = client
            .get(format!("http://{}/v1/jobs/{job}", hub.addr))
            .bearer_auth(API_KEY)
            .send()
            .await?
            .status();
        if status == reqwest::StatusCode::NOT_FOUND {
            break;
        }
        if std::time::Instant::now() >= deadline {
            return Err(anyhow::anyhow!(
                "the subscriber-less window never deleted {job}; last status {status}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // NEX-6.3's second half, and the guard on §5.2's hub-side mirror: a key whose task was
    // deleted must not keep answering with it. The mirror is pruned by the actor's
    // `Effect::JobForgotten` (`state.rs`'s arm for it), so the retry re-runs — the same key
    // now mints a record rather than pointing at one that no longer exists.
    assert!(
        !hub.hub
            .idempotency
            .lock()
            .await
            .contains_key("default:orphan-window-key"),
        "forgetting the task has to forget its key, or a deleted task would come back as a replay"
    );
    let response = client
        .post(format!("http://{}/v1/jobs", hub.addr))
        .bearer_auth(API_KEY)
        .header("Idempotency-Key", "orphan-window-key")
        .json(&serde_json::json!({
            "kind": WS_KIND,
            "payload": {"prompt": "nobody is watching"},
            "detach": true,
        }))
        .send()
        .await?;
    let status = response.status();
    let retry: serde_json::Value = response.json().await?;
    assert_eq!(
        status,
        reqwest::StatusCode::ACCEPTED,
        "the retry is accepted as new work: {retry}"
    );
    let retry_job = retry["job_id"].as_str().unwrap_or_default();
    assert!(
        !retry_job.is_empty(),
        "the retry names the task it accepted: {retry}"
    );
    assert_ne!(
        retry_job,
        job.as_str(),
        "a deleted task's key runs fresh, it does not resurrect the record: {retry}"
    );
    Ok(())
}
/// Spec §7's credential rule at runtime: consumer credentials are the API key,
/// never the pool token, and they are checked before the upgrade — so a bad key
/// is an HTTP `401` with a challenge. Asserted off the raw socket, because that
/// is the claim: what a plain HTTP client sees, not what a WebSocket library
/// reports.
#[tokio::test]
async fn the_consumer_socket_refuses_an_unauthenticated_upgrade() -> anyhow::Result<()> {
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    let hub = spawn_hub("ws_auth", test_config()).await?;

    for label in ["absent", "wrong"] {
        let auth = match label {
            "absent" => None,
            _ => Some("sk-not-a-key"),
        };
        // The upgrade headers are what make this a *consumer socket* request: axum's
        // `WebSocketUpgrade` extractor runs before the handler, so a request without
        // them is rejected as a malformed upgrade (`400`) and never reaches the key
        // check. With them, the extractor accepts and the handler's answer is the
        // claim under test — spec §7's consumer credential rule enforced with a
        // `401` before the upgrade.
        //
        // `Connection` is sent twice on purpose: `upgrade` is what the extractor
        // requires, `close` is what makes hyper hang up after the response so the
        // read below terminates without an upgrade that never comes.
        let request = format!(
            "GET /v1/consumer HTTP/1.1\r\nHost: {}\r\nUpgrade: websocket\r\n\
             Connection: upgrade\r\nConnection: close\r\n\
             Sec-WebSocket-Key: {WS_HANDSHAKE_KEY}\r\nSec-WebSocket-Version: 13\r\n{}\r\n",
            hub.addr,
            auth.map(|key| format!("Authorization: Bearer {key}\r\n"))
                .unwrap_or_default(),
        );
        let mut socket = tokio::net::TcpStream::connect(hub.addr).await?;
        socket.write_all(request.as_bytes()).await?;
        let mut bytes = Vec::new();
        let read = tokio::time::timeout(Duration::from_secs(5), socket.read_to_end(&mut bytes))
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "the hub never answered an unauthenticated upgrade; got: {}",
                    String::from_utf8_lossy(&bytes)
                )
            })??;
        assert!(read > 0, "the hub answered with an empty response");
        let text = String::from_utf8_lossy(&bytes).into_owned();
        let (status_line, headers) = text.split_once("\r\n").unwrap_or((text.as_str(), ""));
        assert_eq!(
            status_line, "HTTP/1.1 401 Unauthorized",
            "{label} key: {text}"
        );
        let challenge: Vec<&str> = headers
            .lines()
            .filter(|line| line.to_ascii_lowercase().starts_with("www-authenticate:"))
            .collect();
        assert_eq!(
            challenge,
            ["www-authenticate: Bearer realm=\"nexus\""],
            "the 401 says how to authenticate ({label} key)"
        );
    }

    // The good key, for contrast: the upgrade happens (asserted inside
    // `dial_consumer`) and the socket speaks the dialect — a warm worker is needed for
    // the verdict to be an acceptance rather than the no-worker refusal, so the claim
    // under test is the key, not the scheduling.
    let mut worker = dial_worker(hub.addr, "wkr-auth", true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));
    let mut consumer = dial_consumer(hub.addr).await?;
    consumer_submit(&mut consumer, "authenticated", true).await?;
    match next_consumer_frame(&mut consumer).await? {
        nexus_core::protocol::HubConsumerFrame::Accepted { detached, .. } => {
            assert!(detached, "the frame's own `detach` is echoed back");
        }
        other => {
            return Err(anyhow::anyhow!(
                "an authenticated submit is accepted, got {other:?}"
            ))
        }
    }
    Ok(())
}

/// NEX-6.1's mechanism, measured: the hub must notice a consumer hanging up from
/// the connection, not from the first write that fails.
///
/// The counterfactual is concrete on this surface — the OpenAI SSE body carries a
/// 1 s keepalive (`consumer.rs:308-311`), so a hub that only learned on a failed write
/// would take at least that long, and a body with a longer interval would wait
/// forever. The read-side `select` on the body channel ends the stream in
/// milliseconds whatever the keepalive cadence is.
#[tokio::test]
async fn an_idle_sse_hangup_cancels_the_worker_without_a_failed_write() -> anyhow::Result<()> {
    let hub = spawn_hub("idle_hangup", test_config()).await?;
    let mut worker = dial_worker(hub.addr, "wkr-idle-hangup", true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{}/v1/chat/completions", hub.addr))
        .bearer_auth(API_KEY)
        .json(&chat_body("nothing to stream"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let assigned = next_frame_of(&mut worker, "assigned").await?;
    let HubFrame::Assigned { job, .. } = assigned else {
        return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
    };

    // Nothing has been written since the hub's own role delta: the pump is parked
    // on the fan-out, which is the state NEX-6.1 says must not be invisible.
    let started = std::time::Instant::now();
    drop(response);
    let cancel = tokio::time::timeout(Duration::from_secs(5), next_frame_of(&mut worker, "cancel"))
        .await
        .map_err(|_| anyhow::anyhow!("the worker never learned the consumer left"))??;
    let elapsed = started.elapsed();
    match cancel {
        HubFrame::Cancel { job: cancelled, .. } => assert_eq!(cancelled, job),
        other => return Err(anyhow::anyhow!("expected a Cancel, got {other:?}")),
    }
    assert!(
        elapsed < Duration::from_millis(700),
        "the notice must come from the connection, not from the 1 s keepalive: {elapsed:?}"
    );
    Ok(())
}

/// One counter out of `GET /metrics`, read as the hub exposed it.
///
/// Exact value, not a range: the counters here are the cheapest way to pin "this
/// submission did not create a second task", which no response body can show on its own.
/// The exposition labels every series by pool (`nexus_jobs_accepted_total{pool=…} 1`), so
/// the answer is the sum over the hub's pools — a hub-wide total, which is what these
/// assertions are about.
async fn metric_value(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    name: &str,
) -> anyhow::Result<u64> {
    let text = client
        .get(format!("http://{addr}/metrics"))
        .send()
        .await?
        .text()
        .await?;
    let mut total: u64 = 0;
    let mut series = 0_usize;
    for line in text.lines() {
        if line.starts_with('#') {
            continue;
        }
        let Some((metric, value)) = line.split_once(' ') else {
            continue;
        };
        // The label set is part of the metric token, not a separate field.
        if metric.split('{').next() != Some(name) {
            continue;
        }
        total += value.trim().parse::<u64>()?;
        series += 1;
    }
    if series == 0 {
        return Err(anyhow::anyhow!(
            "`{name}` is missing from the exposition:\n{text}"
        ));
    }
    Ok(total)
}

/// NEX-6.6's letter at the HTTP boundary: detachment has to be **stated** on the
/// request, so neither an omitted field nor `false` is accepted by the async job route.
///
/// The omitted case is the one this test exists for. Treating absence as "detached" was
/// a route artifact — every plain `POST /v1/jobs` would have bought detached-lifetime
/// semantics without anybody asking, which is exactly what the spec's "it never applies to
/// a plain 'stream me the answer' submission" forbids.
#[tokio::test]
async fn the_async_job_route_requires_detach_to_be_stated() -> anyhow::Result<()> {
    let hub = spawn_hub("detach_required", test_config()).await?;
    // Ready, so the accepted case below is an immediate `202` rather than a grace-window
    // race; it never claims, which is irrelevant here because nothing is dispatched in
    // the two refusals.
    let _idle_worker = dial_ready_but_idle_worker(hub.addr, "wkr-detach-required").await?;
    let client = reqwest::Client::new();
    let post = |body: serde_json::Value| {
        let client = client.clone();
        async move {
            let response = client
                .post(format!("http://{}/v1/jobs", hub.addr))
                .bearer_auth(API_KEY)
                .json(&body)
                .send()
                .await?;
            let status = response.status();
            let value = response.json::<serde_json::Value>().await?;
            Ok::<_, anyhow::Error>((status, value))
        }
    };
    // The whole refusal, field for field, so the contract's wording is pinned as well as
    // its code. `retryable: false`: nothing about the hub is wrong here.
    let expected_refusal = serde_json::json!({
        "error": {
            "type": "detach_required",
            "code": "detach_required",
            "message": "this route keeps no subscription; post with \"detach\": true, or use a surface that holds the connection open (the streaming completion routes, or GET /v1/consumer)",
            "param": null,
            "retryable": false,
        }
    });

    // Absent: nothing was stated, so nothing is inferred.
    let (status, body) = post(serde_json::json!({
        "kind": WS_KIND,
        "payload": {"prompt": "silent about its lifetime"},
    }))
    .await?;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "got: {body}");
    assert_eq!(body, expected_refusal);

    // `false`: the plain streaming contract, which this route cannot honour. Same
    // answer, because the lesson is the same.
    let (status, body) = post(serde_json::json!({
        "kind": WS_KIND,
        "payload": {"prompt": "stream me the answer"},
        "detach": false,
    }))
    .await?;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "got: {body}");
    assert_eq!(body, expected_refusal);

    // Stated: accepted. Also the proof the refusals above are not a blanket rejection of
    // the kind or the tenant.
    let (status, body) = post(serde_json::json!({
        "kind": WS_KIND,
        "payload": {"prompt": "post it now, I will poll later"},
        "detach": true,
    }))
    .await?;
    assert_eq!(status, reqwest::StatusCode::ACCEPTED, "got: {body}");
    assert!(
        body["job_id"].as_str().is_some_and(|id| !id.is_empty()),
        "the `202` names the task it accepted: {body}"
    );
    Ok(())
}

/// §5.2 at the runtime level, on the surface that carries the answer: a second request
/// with the same `Idempotency-Key` gets the **first** task's completion replayed, not a
/// second execution and not a `503`.
///
/// Until the hub could find the original job before minting an id, this path answered
/// `503 hub_unavailable`: the actor attached the duplicate to the original record and
/// replayed it there (`actor.rs:1045-1064`), while the face waited for a verdict on a gate
/// registered under an id the actor never named. README §16.7 row 7 claims this works; the
/// claim was about the replay queue being installed, which was true and not sufficient.
#[tokio::test]
async fn a_retry_on_an_idempotency_key_replays_the_original_completion() -> anyhow::Result<()> {
    let hub = spawn_hub("idempotent_completion", test_config()).await?;
    let mut worker = dial_worker(hub.addr, "wkr-idempotent-completion", true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));

    let client = reqwest::Client::new();
    let chat = || {
        let client = client.clone();
        async move {
            Ok::<_, anyhow::Error>(
                client
                    .post(format!("http://{}/v1/chat/completions", hub.addr))
                    .bearer_auth(API_KEY)
                    .header("Idempotency-Key", "dup-chat-1")
                    .json(&chat_body("count once"))
                    .send()
                    .await?,
            )
        }
    };

    // First request: one task, one dispatch, one answer.
    let first = chat().await?;
    assert_eq!(first.status(), reqwest::StatusCode::OK);
    let first_job = first
        .headers()
        .get("x-nexus-job")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let assigned = next_frame_of(&mut worker, "assigned").await?;
    let HubFrame::Assigned { job, .. } = assigned else {
        return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
    };
    assert_eq!(job.as_ref(), first_job, "the header names the task");

    let caps = WireCaps::default();
    let delta = WorkerFrame::Frame {
        job: job.clone(),
        seq: 1,
        kind: FrameKind::Delta,
        content_type: None,
        text: serde_json::json!({
            "id": "chatcmpl-dup",
            "object": "chat.completion.chunk",
            "choices": [{"index": 0, "delta": {"content": "forty-two"}, "finish_reason": null}],
        })
        .to_string(),
        artifact: None,
    };
    worker
        .send(Message::Text(encode_line(&delta, &caps)?.into()))
        .await?;
    let done = WorkerFrame::Done {
        job: job.clone(),
        seq: 2,
        finish: FinishKind::Stop,
        usage: Some(serde_json::json!({"completion_tokens": 1})),
        error: None,
    };
    worker
        .send(Message::Text(encode_line(&done, &caps)?.into()))
        .await?;
    let first_body = tokio::time::timeout(Duration::from_secs(10), first.text()).await??;
    assert_eq!(
        first_body.matches("chat.completion.chunk").count(),
        3,
        "role delta + the relayed chunk + the finish chunk: {first_body}"
    );

    // Second request, same key, after the first answer completed but while the record is
    // still retained: the pre-fix answer was a `503` with a 1.3 s wait.
    let second = chat().await?;
    assert_eq!(
        second.status(),
        reqwest::StatusCode::OK,
        "a duplicate key follows the task that exists; it is not a hub failure"
    );
    assert_eq!(
        second
            .headers()
            .get("x-nexus-job")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default(),
        first_job,
        "the retry answers with the original task's id, not a second one"
    );
    let second_body = tokio::time::timeout(Duration::from_secs(10), second.text()).await??;
    // The replay, byte for byte: the same three chunks, the worker's text exactly once.
    assert_eq!(
        second_body.matches("chat.completion.chunk").count(),
        3,
        "the retry replays the whole answer: {second_body}"
    );
    assert_eq!(
        second_body.matches("forty-two").count(),
        1,
        "the buffered answer is replayed once, not re-executed: {second_body}"
    );

    // The proof that nothing ran twice, independent of the bytes: the actor accepted one
    // job for the two requests.
    assert_eq!(
        metric_value(&client, hub.addr, "nexus_jobs_accepted_total").await?,
        1,
        "two requests, one execution"
    );
    Ok(())
}

/// §5.2 on the async job API: the second `POST /v1/jobs` with the same key is answered
/// with the record that already exists — same id, same state — and the hub keeps one
/// idempotency row rather than two.
///
/// The task is deliberately never dispatched (`dial_ready_but_idle_worker`), so its state
/// cannot move underneath the assertions and the retry's answer is fully determined.
#[tokio::test]
async fn a_retry_on_an_idempotency_key_gets_the_task_that_already_exists() -> anyhow::Result<()> {
    let hub = spawn_hub("idempotent_job", test_config()).await?;
    let _idle_worker = dial_ready_but_idle_worker(hub.addr, "wkr-idempotent-job").await?;
    let client = reqwest::Client::new();
    let post = || {
        let client = client.clone();
        async move {
            let response = client
                .post(format!("http://{}/v1/jobs", hub.addr))
                .bearer_auth(API_KEY)
                .header("Idempotency-Key", "dup-job-1")
                .json(&serde_json::json!({
                    "kind": WS_KIND,
                    "payload": {"prompt": "post me twice"},
                    "detach": true,
                }))
                .send()
                .await?;
            let status = response.status();
            let value = response.json::<serde_json::Value>().await?;
            Ok::<_, anyhow::Error>((status, value))
        }
    };

    let (status, first) = post().await?;
    assert_eq!(status, reqwest::StatusCode::ACCEPTED, "got: {first}");
    let first_job = first["job_id"]
        .as_str()
        .expect("a 202 names the task it accepted")
        .to_string();

    let (status, second) = post().await?;
    assert_eq!(
        status,
        reqwest::StatusCode::ACCEPTED,
        "a retry on a retained task is accepted, not a 503: {second}"
    );
    assert_eq!(
        second["job_id"].as_str(),
        Some(first_job.as_str()),
        "the retry names the task that is already running"
    );
    assert_eq!(
        second["state"], first["state"],
        "and reports the same state, read from the actor"
    );
    // The retry registered nothing: no consumer, so the task's NEX-6.6 window is still
    // running exactly as it was after the first post.
    let view = client
        .get(format!("http://{}/v1/jobs/{first_job}", hub.addr))
        .bearer_auth(API_KEY)
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;
    assert_eq!(
        view["consumers"].as_u64(),
        Some(0),
        "following a detached task's key does not invent a consumer: {view}"
    );

    // Hub-side bookkeeping, exact: the key maps to the one job, and the retry added no
    // second row (the same mirror entry the retry answered from).
    let mirrored = hub.hub.idempotency.lock().await;
    assert_eq!(
        mirrored
            .get("default:dup-job-1")
            .map(|(_, job, _)| job.as_ref().to_string()),
        Some(first_job.clone()),
        "one key, one mirrored job"
    );
    assert_eq!(
        mirrored.len(),
        1,
        "the retry wrote no second idempotency row"
    );
    drop(mirrored);
    assert_eq!(
        metric_value(&client, hub.addr, "nexus_jobs_accepted_total").await?,
        1,
        "two posts, one accepted task"
    );
    Ok(())
}

/// §5.2 on the consumer socket: a `submit` whose idempotency key this hub has already
/// routed is answered by following the task that exists — verdict, snapshot, the whole
/// replayed answer, one `end` — rather than `hub_unavailable`.
///
/// This is the reconnect path the socket exists for: a client that dropped and resends the
/// same submission must not pay for a second execution, and must not be told the hub did
/// not answer.
#[tokio::test]
async fn a_resubmitted_key_on_the_consumer_socket_follows_the_original_task() -> anyhow::Result<()>
{
    let hub = spawn_hub("ws_idempotent_submit", test_config()).await?;
    let mut worker = dial_worker(hub.addr, "wkr-ws-idempotent", true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));
    let mut consumer = dial_consumer(hub.addr).await?;

    // Same payload a client would resend, with the key that makes it a duplicate.
    let submission = serde_json::json!({
        "type": "submit",
        "kind": WS_KIND,
        "payload": {"prompt": "answer me once"},
        "detach": false,
        "idempotency_key": "dup-ws-1",
    });
    send_consumer(&mut consumer, &submission).await?;
    let nexus_core::protocol::HubConsumerFrame::Accepted { job, .. } =
        next_consumer_frame(&mut consumer).await?
    else {
        return Err(anyhow::anyhow!("expected an accepted verdict"));
    };
    expect_consumer_meta(&mut consumer, &job).await?;

    let assigned = next_frame_of(&mut worker, "assigned").await?;
    let HubFrame::Assigned {
        job: assigned_job, ..
    } = assigned
    else {
        return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
    };
    assert_eq!(assigned_job, job);

    let caps = WireCaps::default();
    let delta = WorkerFrame::Frame {
        job: job.clone(),
        seq: 1,
        kind: FrameKind::Delta,
        content_type: None,
        text: "once".to_string(),
        artifact: None,
    };
    worker
        .send(Message::Text(encode_line(&delta, &caps)?.into()))
        .await?;
    let usage = serde_json::json!({"completion_tokens": 1});
    let done = WorkerFrame::Done {
        job: job.clone(),
        seq: 2,
        finish: FinishKind::Stop,
        usage: Some(usage.clone()),
        error: None,
    };
    worker
        .send(Message::Text(encode_line(&done, &caps)?.into()))
        .await?;

    let answer = [
        nexus_core::protocol::HubConsumerFrame::Delta {
            job: job.clone(),
            seq: 1,
            text: "once".to_string(),
        },
        nexus_core::protocol::HubConsumerFrame::Finish {
            job: job.clone(),
            seq: 2,
            finish: FinishKind::Stop,
            usage: Some(usage),
        },
        nexus_core::protocol::HubConsumerFrame::end_for(
            &job,
            nexus_core::protocol::EndReason::Finished,
            Some(2),
        ),
    ];
    for want in answer.clone() {
        expect_consumer_frame(&mut consumer, want).await?;
    }

    // The resubmission: same socket, same frame, same key. The verdict names the original
    // task, and the answer that follows is the ring's, not a second run.
    send_consumer(&mut consumer, &submission).await?;
    expect_consumer_frame(
        &mut consumer,
        nexus_core::protocol::HubConsumerFrame::Accepted {
            job: job.clone(),
            kind: WS_KIND.to_string(),
            pool: nexus_core::ids::PoolId::new_unchecked(POOL),
            // A resubmission declares its own contract, unlike a bare attach.
            detached: false,
        },
    )
    .await?;
    match next_consumer_frame(&mut consumer).await? {
        nexus_core::protocol::HubConsumerFrame::Meta {
            job: meta, state, ..
        } => {
            assert_eq!(meta, job, "the snapshot is of the original task");
            assert_eq!(
                state.as_deref(),
                Some("Finished"),
                "the phase is the actor's, not a guess"
            );
        }
        other => return Err(anyhow::anyhow!("expected a snapshot, got {other:?}")),
    }
    for want in answer {
        expect_consumer_frame(&mut consumer, want).await?;
    }
    Ok(())
}

/// §5.2 on the non-streaming chat surface: a duplicate key assembles its answer from the
/// task that exists, while that task is still running, and the original consumer's stream
/// keeps running alongside it.
///
/// `stream: false` reaches `aggregate_response`, which is the one follow path that used to
/// drop the relay when its request ended. A joiner borrows that relay — the original
/// submission's pump is reading the same fan-out — so the entry has to survive the joiner
/// (see the `prune_relay` parameter). The last assertion is what pins that: if the joiner
/// had torn the relay down, the live stream would stop delivering here.
#[tokio::test]
async fn a_non_streaming_retry_on_a_key_assembles_the_running_answer() -> anyhow::Result<()> {
    let hub = spawn_hub("idempotent_aggregate", test_config()).await?;
    let mut worker = dial_worker(hub.addr, "wkr-idempotent-aggregate", true).await?;
    assert!(matches!(
        next_frame_of(&mut worker, "welcome").await?,
        HubFrame::Welcome { .. }
    ));

    let client = reqwest::Client::new();
    // The original: streaming, and deliberately left open with one delta already in the
    // ring, so the retry joins a task that is running rather than finished.
    let live = client
        .post(format!("http://{}/v1/chat/completions", hub.addr))
        .bearer_auth(API_KEY)
        .header("Idempotency-Key", "dup-aggregate-1")
        .json(&chat_body("answer slowly"))
        .send()
        .await?;
    assert_eq!(live.status(), reqwest::StatusCode::OK);
    let live_job = live
        .headers()
        .get("x-nexus-job")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let assigned = next_frame_of(&mut worker, "assigned").await?;
    let HubFrame::Assigned { job, .. } = assigned else {
        return Err(anyhow::anyhow!("expected an Assigned, got {assigned:?}"));
    };
    assert_eq!(job.as_ref(), live_job);

    let caps = WireCaps::default();
    let delta = WorkerFrame::Frame {
        job: job.clone(),
        seq: 1,
        kind: FrameKind::Delta,
        content_type: None,
        text: serde_json::json!({
            "id": "chatcmpl-dup-aggregate",
            "object": "chat.completion.chunk",
            "choices": [{"index": 0, "delta": {"content": "partly"}, "finish_reason": null}],
        })
        .to_string(),
        artifact: None,
    };
    worker
        .send(Message::Text(encode_line(&delta, &caps)?.into()))
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The retry, `stream: false`, in its own task: an aggregated answer arrives as one
    // response, so its headers only come when the handler has collected everything — which
    // needs the `Done` below. Its `Attach` is in the actor's mailbox as soon as the handler
    // has run `join_existing_job`, and the pause below is what makes that happen first.
    let retry_client = client.clone();
    // Copied out: the task cannot borrow the hub, and `SocketAddr` is `Copy`.
    let addr = hub.addr;
    let retry = tokio::spawn(async move {
        let response = retry_client
            .post(format!("http://{addr}/v1/chat/completions"))
            .bearer_auth(API_KEY)
            .header("Idempotency-Key", "dup-aggregate-1")
            .json(&{
                let mut body = chat_body("answer slowly");
                body["stream"] = serde_json::json!(false);
                body
            })
            .send()
            .await?;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::OK,
            "a retry on a running task is an answer, not a hub failure"
        );
        let value = response.json::<serde_json::Value>().await?;
        Ok::<_, anyhow::Error>(value)
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    // The task finishes while the retry is still collecting, and the retry's answer is the
    // whole message: the replayed prefix plus whatever arrived after it joined. (If the
    // `Done` were to land first, the ring carries it instead, and the assembled content
    // below is the same — the assertion does not depend on which of the two orders runs.)
    let done = WorkerFrame::Done {
        job: job.clone(),
        seq: 2,
        finish: FinishKind::Stop,
        usage: Some(serde_json::json!({"completion_tokens": 1})),
        error: None,
    };
    worker
        .send(Message::Text(encode_line(&done, &caps)?.into()))
        .await?;
    let retry_body = tokio::time::timeout(Duration::from_secs(10), retry)
        .await
        .expect("the retry answers within 10 s")??;
    assert_eq!(
        retry_body["choices"][0]["message"]["content"].as_str(),
        Some("partly"),
        "the assembled answer is the replayed prefix: {retry_body}"
    );
    assert_eq!(
        metric_value(&client, hub.addr, "nexus_jobs_accepted_total").await?,
        1,
        "the retry ran nothing"
    );

    // The original stream is still a live stream: its own finish and `[DONE]` arrive after
    // the retry has come and gone.
    let live_body = tokio::time::timeout(Duration::from_secs(10), live.text()).await??;
    assert_eq!(
        live_body.matches("partly").count(),
        1,
        "the consumer that was here first still gets its answer: {live_body}"
    );
    assert!(
        live_body.contains("[DONE]"),
        "and its stream ends normally: {live_body}"
    );

    // Third request, same key, now that the task is finished: the whole answer is in the
    // replayed prefix, so the aggregating path must stop there rather than wait for a second
    // terminal event on the fan-out. The 10 s budget is the assertion — the alternative is
    // the aggregate deadline (300 s) followed by a `503 no_ready_worker` for a completion
    // that was already in hand.
    let finished_retry = tokio::time::timeout(
        Duration::from_secs(10),
        client
            .post(format!("http://{}/v1/chat/completions", hub.addr))
            .bearer_auth(API_KEY)
            .header("Idempotency-Key", "dup-aggregate-1")
            .json(&{
                let mut body = chat_body("answer slowly");
                body["stream"] = serde_json::json!(false);
                body
            })
            .send(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("a retry on a finished task never answered"))??;
    assert_eq!(finished_retry.status(), reqwest::StatusCode::OK);
    let finished_body = finished_retry.json::<serde_json::Value>().await?;
    assert_eq!(
        finished_body["choices"][0]["message"]["content"].as_str(),
        Some("partly"),
        "the finished task's replay is the whole answer: {finished_body}"
    );
    assert_eq!(
        metric_value(&client, hub.addr, "nexus_jobs_accepted_total").await?,
        1,
        "three requests, one execution"
    );
    Ok(())
}

/// NEX-7.2 on the HTTP faces: a submission the actor refused leaves no per-task state
/// behind — not a routing entry, not a relay, not an idempotency row.
///
/// `submit` registers all three before the actor is told, so the refusal has to undo them:
/// the actor holds no record, which means no `Effect::Terminal` and no `Effect::JobForgotten`
/// will ever name the id to prune it. The consumer WebSocket already did this for its own
/// refusals; the HTTP surfaces previously did not, so every `429` and every `503` leaked one
/// entry per attempt for the life of the process.
#[tokio::test]
async fn a_refused_submission_leaves_no_route_or_relay_behind() -> anyhow::Result<()> {
    let mut cfg = test_config();
    // Zero jobs in flight allowed: every submission is refused, in `on_submit`, before
    // the queue and before the capability pre-check (`actor.rs:1068-1080`) — which is
    // what makes the count below an exact number rather than a race.
    cfg.tenants
        .get_mut("default")
        .expect("the sample topology defines the default tenant")
        .max_concurrent = Some(0);
    let hub = spawn_hub("refusal_prune", cfg).await?;
    let _idle_worker = dial_ready_but_idle_worker(hub.addr, "wkr-refusal-prune").await?;
    let client = reqwest::Client::new();

    // Refusal one: the async job API.
    let response = client
        .post(format!("http://{}/v1/jobs", hub.addr))
        .bearer_auth(API_KEY)
        .json(&serde_json::json!({
            "kind": WS_KIND,
            "payload": {"prompt": "no room"},
            "detach": true,
        }))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
    let body = response.json::<serde_json::Value>().await?;
    assert_eq!(body["error"]["code"], "tenant_limit", "got: {body}");

    // Refusal two: the OpenAI surface, whose reject arm used to leak the same way.
    let response = client
        .post(format!("http://{}/v1/chat/completions", hub.addr))
        .bearer_auth(API_KEY)
        .json(&chat_body("still no room"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
    let body = response.json::<serde_json::Value>().await?;
    assert_eq!(body["error"]["code"], "tenant_limit", "got: {body}");

    // Nothing left in any of the three job-keyed maps.
    assert!(
        hub.hub.job_index.lock().await.is_empty(),
        "a refused submission owns no route"
    );
    assert!(
        hub.hub.idempotency.lock().await.is_empty(),
        "a refused submission owns no idempotency row"
    );
    for handle in hub.hub.pools.values() {
        assert!(
            handle.followed_jobs().await.is_empty(),
            "a refused submission owns no relay"
        );
    }
    Ok(())
}
