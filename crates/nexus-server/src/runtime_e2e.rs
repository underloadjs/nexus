//! Runtime end-to-end tests for the native hub: a real listener, a real worker
//! WebSocket, and a real consumer HTTP request whose SSE bytes are read off the
//! wire. The core acceptance tests (`crates/nexus-core/tests/acceptance.rs`) drive
//! the actor with injected time and prove the *decisions*; only this file can prove
//! the runtime performs them — that effect bytes reach a socket, that a 503 is
//! answered before a body opens, that an unauthenticated read is refused.
//!
//! ## Status: written, never compiled
//!
//! There is no Rust toolchain on the machine this was authored on (no `cargo`,
//! `rustc`, `rustup` — see the package README's *Verification status*), so these
//! tests have executed exactly zero times. If `cargo check --workspace --all-targets`
//! fails, look here first: this is the newest and least-scrutinised code in the
//! package, and a failure here says nothing about the actor's correctness.
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
    /// Not read by the assertions, but holding a second `Arc<Hub>` is what keeps the
    /// hub (and every pool actor it owns) alive for the lifetime of the test.
    #[allow(dead_code)]
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
/// runs these in parallel and two hubs sharing a parts directory would race on the
/// per-attempt part files (`io::write_chunk`). The directory is the repo-root scratch
/// location (`io::test_scratch`, i.e. `<repo>/Temp/nexus-tests/<name>`) rather than the
/// package's `var/`, per the root `AGENTS.md` rule: this is throwaway test state, not a
/// hub's configured artifact root. The runtime default (`Temp/nexus/artifacts`,
/// `NEXUS_ARTIFACT_ROOT`) is untouched.
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
        let heartbeat = WorkerFrame::Ready {
            free_slots: 4,
            free_kv_tokens: Some(524_288),
            engine_ready: true,
            telemetry: None,
        };
        ws.send(Message::Text(encode_line(&heartbeat, &caps)?.into()))
            .await?;
        ws.send(Message::Text(
            encode_line(&WorkerFrame::Claim { jobs: Some(4) }, &caps)?.into(),
        ))
        .await?;
    }
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
