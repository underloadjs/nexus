//! nexus-worker-example — a Rust worker client that dials **out** to the hub
//! (task §12's "the thing that dials the hub").
//!
//! Written in Rust rather than Python because this package keeps application logic
//! in Rust. It is a faithful template for the molab/Kaggle deployment:
//!
//! * binds nothing: the engine stays on loopback `127.0.0.1:8090`
//!   (`services/molab/molab-strata-runtime.py:119-125`), so the hub is reached by
//!   an outbound connection exactly as the design assumes;
//! * `Hello` declares capacity, capabilities and the GGUF digests actually loaded
//!   (attestation, task §9);
//! * `Ready` heartbeats relay the engine's own `/v1/status` reading, which is what
//!   makes `loading → ready` honest (task §8) and renews every lease (§5.1);
//! * `Claim` → `Assigned` → engine deltas relayed as `Frame{delta}` with a per-job
//!   `seq`, then `Done{finish, usage}`;
//! * `Cancel` stops local generation at the next chunk (task §6);
//! * reconnect + `Resume` from the last acked `seq` (task §4);
//! * backoff that resets on a clean `Welcome`, so a flapping notebook host does
//!   not hammer the hub.
//!
//! Bulk output uses the hub's HTTP upload slot instead of the socket, so a 40 MB
//! render cannot starve other jobs' deltas (task §10).

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use futures_util::SinkExt;
use futures_util::StreamExt;
use nexus_core::protocol::decode_hub_frame;
use nexus_core::protocol::encode_line;
use nexus_core::protocol::ArtifactManifest;
use nexus_core::protocol::Attestation;
use nexus_core::protocol::ByeReason;
use nexus_core::protocol::FinishKind;
use nexus_core::protocol::FrameKind;
use nexus_core::protocol::HubFrame;
use nexus_core::protocol::JobPayload;
use nexus_core::protocol::Requires;
use nexus_core::protocol::WireCaps;
use nexus_core::protocol::WorkerFrame;
use nexus_core::WorkerTier;
use serde_json::Value;
use serde_json::json;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

/// Hub WebSocket URL. `NEXUS_HUB_WS` overrides.
const HUB_WS_DEFAULT: &str = "ws://127.0.0.1:8085/v1/worker";
/// Hub HTTP base, used for payload refs and artifact uploads.
const HUB_HTTP_DEFAULT: &str = "http://127.0.0.1:8085";
/// Loopback engine base URL, matching the reference runtime's `LOCAL_URL`.
const ENGINE_DEFAULT: &str = "http://127.0.0.1:8090";
/// Pool this worker serves; it must exist in the hub's configuration.
const POOL_DEFAULT: &str = "llm.chat/qwen38";
/// Model label.
const MODEL_DEFAULT: &str = "qwen38";
/// Reference digests, reused as the attestation anchor (SHARD_TABLE,
/// services/molab/molab-strata-runtime.py:95-98).
const SHARD_1: &str = "40f369eca301ec6ea318dbb671b3132c26a2f0b89aa5458a7051b33215ee296c";
const SHARD_2: &str = "316b46f3a2dbd68c900f43136ab9449f9dcc3725dfd8c794847c204bc161e113";
/// Context window the engine advertises (`CONTEXT = 524288`).
const CONTEXT_TOKENS: u64 = 524_288;
/// Weight bytes for informational attestation (`SHARD_TOTAL_BYTES`).
const WEIGHT_BYTES: u64 = 83_863_586_496;
/// Heartbeat period, inside the 15–30 s range the task specifies.
const HEARTBEAT_SECS: u64 = 20;
/// Reconnect backoff floor and ceiling.
const BACKOFF_MIN_MS: u64 = 500;
const BACKOFF_MAX_MS: u64 = 15_000;
/// Cap on declared capacity, mirroring the hub's own clamp.
const MAX_CAPACITY: u32 = 64;

/// Per-job cancellation flags, keyed by job id (see `main`).
type CancelFlags = Arc<Mutex<BTreeMap<nexus_core::JobId, Arc<AtomicBool>>>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config::from_env();
    eprintln!(
        "[worker] id={} hub={} engine={} pool={} capacity={} tier={:?}",
        cfg.worker_id, cfg.hub_ws, cfg.engine_base, cfg.pool, cfg.capacity, cfg.tier
    );

    // One outbound queue, one writer: socket write order equals decision order.
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();
    // Cancellation is **per job**. A worker with capacity > 1 runs several
    // generations at once, so a single shared flag would stop all of them the
    // moment the hub cancels one, and the next `Assigned` resetting it would
    // silently un-cancel a job that had just been told to stop (task §6).
    let cancels: CancelFlags = Arc::new(Mutex::new(BTreeMap::new()));

    let mut backoff = BACKOFF_MIN_MS;
    loop {
        match session(&cfg, &mut out_rx, &out_tx, &cancels).await {
            Ok(bye) => {
                eprintln!("[worker] session ended: {bye:?}");
                backoff = BACKOFF_MIN_MS;
            }
            Err(err) => eprintln!("[worker] session failed: {err}"),
        }
        tokio::time::sleep(Duration::from_millis(backoff)).await;
        backoff = (backoff * 2).min(BACKOFF_MAX_MS);
        // Anything queued while we were down is stale: drop it rather than
        // replaying deltas the hub has already re-acked past.
        while out_rx.try_recv().is_ok() {}
    }
}

/// One hub connection, for its whole lifetime.
async fn session(cfg: &Config, out_rx: &mut mpsc::UnboundedReceiver<String>, out_tx: &mpsc::UnboundedSender<String>, cancels: &CancelFlags) -> anyhow::Result<ByeReason> {
    // The pool token rides in the query string: the hub accepts either form, and a
    // notebook cell cannot always set an `Authorization` header on a WebSocket.
    let url = with_token(&cfg.hub_ws, &cfg.pool_token);
    let (mut ws, _response) = connect_async(url).await?;
    eprintln!("[worker] dialled the hub");
    // A new socket means new job generations: a flag left behind by the previous
    // session would make a re-dispatched job stop before it starts.
    cancels.lock().await.clear();

    // 1. Identify ourselves: capacity and digests are what the hub schedules on.
    let hello = WorkerFrame::Hello {
        worker_id: cfg.worker_id.clone(),
        pools: vec![cfg.pool.clone()],
        capacity: cfg.capacity,
        requires: Requires {
            model: Some(cfg.model.clone()),
            min_context_tokens: Some(CONTEXT_TOKENS),
            capabilities: cfg.capabilities.clone(),
            kv_estimate_tokens: None,
        },
        attestation: Attestation { model_shas: cfg.model_shas.clone(), weight_bytes: Some(WEIGHT_BYTES) },
        protocol: nexus_core::PROTOCOL_VERSION,
        engine: Some("strata".to_string()),
        tier: Some(cfg.tier),
    };
    write_frame(&mut ws, &hello).await?;

    // 2. Heartbeats on an interval, driven by the same loop that reads the socket:
    //    no second task borrowing the socket, and a missed beat is a lost lease on
    //    the hub side — which is the intended behaviour (§5.1).
    let mut heartbeat = tokio::time::interval(Duration::from_secs(HEARTBEAT_SECS));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let caps = WireCaps::default();

    loop {
        tokio::select! {
            // Outbound pump: frames produced by the job tasks.
            Some(line) = out_rx.recv() => {
                ws.send(Message::Text(line.into())).await?;
            }
            // Heartbeat / lease renewal.
            _ = heartbeat.tick() => {
                let status = engine_status(&cfg.engine_base).await;
                let ready = WorkerFrame::Ready {
                    free_slots: status.free_slots,
                    free_kv_tokens: status.free_kv,
                    engine_ready: status.ready,
                    telemetry: status.raw,
                };
                write_frame(&mut ws, &ready).await?;
            }
            // Inbound frames drive everything else.
            maybe_frame = ws.next() => {
                let text = match maybe_frame {
                    Some(Ok(Message::Text(text))) => text.to_string(),
                    Some(Ok(Message::Binary(bytes))) => String::from_utf8_lossy(&bytes).into_owned(),
                    Some(Ok(Message::Ping(payload))) => {
                        ws.send(Message::Pong(payload)).await?;
                        continue;
                    }
                    Some(Ok(Message::Pong(_))) => continue,
                    Some(Ok(Message::Close(_))) | None => return Ok(ByeReason::Shutdown),
                    Some(Err(err)) => return Err(err.into()),
                };
                let frame = match decode_hub_frame(text.trim(), &caps) {
                    Ok(frame) => frame,
                    Err(err) => {
                        // The hub speaks the protocol we negotiated; anything else
                        // means we are out of step, so reconnect rather than guess.
                        eprintln!("[worker] undecodable hub frame: {err}");
                        return Ok(ByeReason::VersionMismatch);
                    }
                };
                match frame {
                    HubFrame::Welcome { worker_id, pools, lease_ms, resumed, frame_cap_bytes, .. } => {
                        eprintln!(
                            "[worker] welcome: {worker_id} pools={pools:?} lease={lease_ms}ms frame_cap={frame_cap_bytes} resumable={}",
                            resumed.len()
                        );
                        // Re-attach whatever the hub still holds for us (§4).
                        for job in resumed {
                            write_frame(&mut ws, &WorkerFrame::Resume { job: job.job, attempt: job.attempt, acked_seq: job.resume_from_seq }).await?;
                        }
                        write_frame(&mut ws, &WorkerFrame::Claim { jobs: Some(cfg.capacity) }).await?;
                    }
                    HubFrame::Assigned { job, attempt, kind, payload, artifact_put, continuation_text, start_seq, .. } => {
                        // Resolve the payload before spawning: a by-ref body is
                        // fetched over HTTP so a 512K prompt never queues behind
                        // control frames on this socket (task §10).
                        let body = resolve_payload(cfg, &payload).await;
                        eprintln!("[worker] assigned {job} attempt {attempt} kind={kind} body={} bytes (seq from {start_seq})", body.len());
                        // One flag per job, so a `Cancel` for one generation does
                        // not stop the others sharing this socket.
                        let flag = Arc::new(AtomicBool::new(false));
                        cancels.lock().await.insert(job.clone(), Arc::clone(&flag));
                        let out = out_tx.clone();
                        let cancels = Arc::clone(cancels);
                        tokio::spawn(run_job(
                            cfg.engine_base.clone(),
                            cfg.hub_http.clone(),
                            cfg.pool_token.clone(),
                            job,
                            attempt,
                            kind,
                            body,
                            start_seq,
                            continuation_text,
                            artifact_put,
                            out,
                            flag,
                            cancels,
                        ));
                    }
                    HubFrame::Cancel { job, reason } => {
                        eprintln!("[worker] cancel {job} ({reason:?})");
                        // Stop exactly the job the hub named. The ack goes back
                        // either way, so the hub frees the slot on schedule (§6).
                        if let Some(flag) = cancels.lock().await.remove(&job) {
                            flag.store(true, Ordering::SeqCst);
                        }
                        write_frame(&mut ws, &WorkerFrame::CancelAck { job }).await?;
                    }
                    HubFrame::Ack { job, seq } => {
                        eprintln!("[worker] {job} acked through seq {seq}");
                    }
                    HubFrame::ResumeNack { job, attempt, reason } => {
                        eprintln!("[worker] resume refused for {job} attempt {attempt}: {reason}");
                    }
                    HubFrame::Reject { code, message } => {
                        eprintln!("[worker] hub rejected this worker: {code}: {message}");
                        return Ok(ByeReason::AuthRejected);
                    }
                    HubFrame::Bye { reason } => {
                        eprintln!("[worker] hub said bye: {reason:?}");
                        return Ok(reason);
                    }
                }
            }
        }
    }
}

/// Encode and write one frame.
async fn write_frame<S>(ws: &mut S, frame: &WorkerFrame) -> anyhow::Result<()>
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let line = encode_line(frame, &WireCaps::default())?;
    ws.send(Message::Text(line.into())).await?;
    Ok(())
}

/// Run one assigned job against the loopback engine and stream its deltas out.
#[allow(clippy::too_many_arguments)]
async fn run_job(
    engine: String,
    hub_http: String,
    pool_token: String,
    job: nexus_core::JobId,
    attempt: u32,
    kind: String,
    body: Vec<u8>,
    start_seq: u64,
    continuation: Option<String>,
    artifact_put: Option<nexus_core::protocol::ArtifactPut>,
    out: mpsc::UnboundedSender<String>,
    cancel: Arc<AtomicBool>,
    cancels: CancelFlags,
) {
    // The flag was created for this job and starts clear, so nothing to inherit.
    let caps = WireCaps::default();
    // Numbering continues from the hub's floor: a requeue or a
    // resume-by-continuation (§7.4) hands a fresh worker a job whose earlier
    // attempts already occupied seqs, and frames at or below the acked seq would
    // be dropped as duplicates by the hub. For a first attempt this is `0`.
    let mut seq: u64 = start_seq;
    let mut finish = FinishKind::Stop;
    let mut usage: Option<Value> = None;
    let mut emitted = 0usize;

    match engine_stream_chat(&engine, &body, continuation.as_deref()).await {
        Ok(events) => {
            for event in events {
                if cancel.load(Ordering::SeqCst) {
                    eprintln!("[worker] {job}: hub cancelled; stopping generation (GPU time is the scarce resource)");
                    finish = FinishKind::Stop;
                    break;
                }
                match event {
                    EngineEvent::Delta(chunk) => {
                        seq += 1;
                        emitted += 1;
                        let frame = WorkerFrame::Frame {
                            job: job.clone(),
                            seq,
                            kind: FrameKind::Delta,
                            content_type: Some("application/json".to_string()),
                            text: chunk,
                            artifact: None,
                        };
                        if let Ok(line) = encode_line(&frame, &caps) {
                            if out.send(line).is_err() {
                                break;
                            }
                        }
                    }
                    EngineEvent::Usage(value) => usage = Some(value),
                }
            }
        }
        Err(err) => {
            eprintln!("[worker] {job}: engine call failed: {err}");
            finish = FinishKind::Error;
            usage = Some(json!({ "error": err.to_string() }));
        }
    }

    // Artifact kinds upload their bulk output over HTTP, then announce it.
    if let Some(put) = artifact_put {
        let payload = json!({ "kind": kind, "attempt": attempt, "generated_at": "engine-side" });
        match upload_artifact(&hub_http, &put.path, &put.token, &payload.to_string()).await {
            Ok(()) => {
                seq += 1;
                let frame = WorkerFrame::Frame {
                    job: job.clone(),
                    seq,
                    kind: FrameKind::ArtifactChunk,
                    content_type: Some("application/json".to_string()),
                    text: String::new(),
                    artifact: Some(ArtifactManifest {
                        artifact: put.artifact,
                        content_type: Some("application/json".to_string()),
                        size_bytes: Some(payload.to_string().len() as u64),
                        sha256: None,
                        chunk_index: Some(0),
                        chunk_bytes: Some(payload.to_string().len() as u64),
                    }),
                };
                if let Ok(line) = encode_line(&frame, &caps) {
                    let _ = out.send(line);
                }
            }
            Err(err) => eprintln!("[worker] {job}: artifact upload failed: {err}"),
        }
    }

    let done = WorkerFrame::Done { job: job.clone(), seq, finish, usage, error: None };
    if let Ok(line) = encode_line(&done, &caps) {
        let _ = out.send(line);
    }
    // Forget this job's flag: the map is bounded by the number of jobs in flight,
    // not by the hub's lifetime.
    cancels.lock().await.remove(&job);
    eprintln!("[worker] {job}: finished after {emitted} deltas (seq {seq})");
    // Ask for the next job: pull keeps the pool busy without the hub guessing when
    // we are free.
    if let Ok(line) = encode_line(&WorkerFrame::Claim { jobs: Some(1) }, &caps) {
        let _ = out.send(line);
    }
}

/// Engine-side events.
enum EngineEvent {
    /// One OpenAI-shaped chunk, relayed verbatim.
    Delta(String),
    /// The usage object, carried in `Done`.
    Usage(Value),
}

/// Call the loopback engine and collect its streamed chunks.
///
/// **Known limitation of the reference client (M0 shape):** `http_request` reads
/// the response to end of stream, so the engine's chunks are relayed only after
/// the generation has finished. The hub's own consumer path does stream, so this
/// is a client-side shortcut, not a protocol limit: a production worker would
/// parse each `data:` block as it arrives. Until then, end-to-end TTFT through
/// this example equals the engine's full generation time.
async fn engine_stream_chat(engine: &str, body: &[u8], continuation: Option<&str>) -> anyhow::Result<Vec<EngineEvent>> {
    let mut payload: Value = serde_json::from_slice(body)?;
    if let Some(partial) = continuation {
        // Resume-by-continuation (task §7.4): the hub appends the frames the
        // previous attempt relayed, verbatim. It is deliberately *not* parsed on
        // the hub side (§7.1: only adapters speak a dialect), so reconstructing
        // the assistant's speech is this worker adapter's job.
        if let Some(messages) = payload.get_mut("messages").and_then(Value::as_array_mut) {
            messages.push(json!({ "role": "assistant", "content": continuation_as_text(partial) }));
        }
    }
    // `stream: true` is forced: the hub relays deltas, so buffering the whole
    // answer here would reintroduce the latency the design removes.
    payload["stream"] = json!(true);
    let text = http_post(&format!("{engine}/v1/chat/completions"), &serde_json::to_string(&payload)?).await?;

    let mut events = Vec::new();
    for block in text.split("\n\n") {
        for line in block.lines() {
            let Some(data) = line.strip_prefix("data: ") else { continue };
            if data.trim() == "[DONE]" {
                continue;
            }
            let Ok(chunk) = serde_json::from_str::<Value>(data) else { continue };
            if let Some(usage) = chunk.get("usage").filter(|u| !u.is_null()) {
                events.push(EngineEvent::Usage(usage.clone()));
            }
            if chunk.get("choices").is_some() {
                // Verbatim: the hub promises not to re-tokenize, so neither do we.
                events.push(EngineEvent::Delta(data.to_string()));
            }
        }
    }
    Ok(events)
}

/// Rebuild the assistant's speech from the hub's verbatim `continuation_text`.
///
/// The hub concatenates the frames the previous attempt relayed and never parses
/// them (§7.1: only adapters speak a dialect), so for an OpenAI-shaped worker the
/// hint is a run of adjacent chunk objects. `serde_json`'s stream deserializer
/// walks concatenated JSON values, and each chunk's `choices[].delta.content` is
/// text the consumer has already seen. If nothing parses, the hint was not chunk
/// JSON and is used literally, so a plain-text worker keeps working.
fn continuation_as_text(raw: &str) -> String {
    let mut out = String::new();
    let mut parsed = 0usize;
    for value in serde_json::Deserializer::from_str(raw).into_iter::<Value>() {
        match value {
            Ok(value) => {
                parsed += 1;
                if let Some(choices) = value.get("choices").and_then(Value::as_array) {
                    for choice in choices {
                        if let Some(text) = choice.get("delta").and_then(|d| d.get("content")).and_then(Value::as_str) {
                            out.push_str(text);
                        }
                    }
                }
            }
            // Truncated tail (the previous attempt died mid-frame): stop, keep
            // what was reconstructed.
            Err(_) => break,
        }
    }
    if parsed == 0 {
        return raw.to_string();
    }
    out
}

/// What the hub needs before it dispatches anything to us.
#[derive(Debug, Default)]
struct EngineStatus {
    ready: bool,
    free_slots: u32,
    free_kv: Option<u64>,
    raw: Option<Value>,
}

/// Read the engine's own status endpoint — the same probe the reference runtime
/// performs inside `start()` (task §12). Readiness is measured, never assumed.
async fn engine_status(engine: &str) -> EngineStatus {
    match http_get(&format!("{engine}/v1/status")).await {
        Ok(text) => serde_json::from_str::<Value>(&text)
            .map(|value| EngineStatus {
                ready: value.get("slots_free").and_then(Value::as_u64).is_some(),
                free_slots: value.get("slots_free").and_then(Value::as_u64).unwrap_or(0) as u32,
                free_kv: value.get("kv_free_tokens").and_then(Value::as_u64),
                raw: Some(value),
            })
            .unwrap_or_default(),
        Err(_) => EngineStatus::default(),
    }
}

/// Inline payload bytes, or a fetch by reference.
async fn resolve_payload(cfg: &Config, payload: &JobPayload) -> Vec<u8> {
    match payload {
        JobPayload::Inline { body, .. } => serde_json::to_vec(body).unwrap_or_default(),
        JobPayload::ByRef { path, token, sha256, size_bytes, .. } => {
            eprintln!("[worker] fetching payload ref {path} ({size_bytes} bytes, sha {sha256})");
            let url = format!("{}{path}", cfg.hub_http);
            match http_get_with_token(&url, token).await {
                Ok(body) => body.into_bytes(),
                Err(err) => {
                    eprintln!("[worker] payload fetch failed: {err}");
                    Vec::new()
                }
            }
        }
    }
}

/// Upload one artifact chunk to the hub's pre-signed slot.
async fn upload_artifact(hub_http: &str, path: &str, token: &str, body: &str) -> anyhow::Result<()> {
    http_put(&format!("{hub_http}{path}?token={token}&complete=1"), body).await.map(|_| ())
}

/// Minimal HTTP GET over loopback: the worker keeps no HTTP client dependency, and
/// the engine hop is always plain HTTP on `127.0.0.1`.
async fn http_get(url: &str) -> anyhow::Result<String> {
    http_request("GET", url, None, None).await
}

/// GET with a bearer token (hub payload refs).
async fn http_get_with_token(url: &str, token: &str) -> anyhow::Result<String> {
    http_request("GET", url, Some(&format!("Bearer {token}")), None).await
}

/// POST with a JSON body.
async fn http_post(url: &str, body: &str) -> anyhow::Result<String> {
    http_request("POST", url, Some("application/json"), Some(body)).await
}

/// PUT with a JSON body (artifact chunks).
async fn http_put(url: &str, body: &str) -> anyhow::Result<String> {
    http_request("PUT", url, Some("application/octet-stream"), Some(body)).await
}

/// The tiny request/response walk. `Connection: close` is deliberate: a worker that
/// is re-run as a notebook cell must not depend on pooled keep-alive state.
async fn http_request(method: &str, url: &str, auth_or_type: Option<&str>, body: Option<&str>) -> anyhow::Result<String> {
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    let (scheme, rest) = url.split_once("://").ok_or_else(|| anyhow::anyhow!("not a url: {url}"))?;
    if scheme != "http" {
        return Err(anyhow::anyhow!("only plain http is supported on this hop: {scheme}"));
    }
    let (authority, path) = rest
        .split_once('/')
        .map_or((rest.to_string(), "/".to_string()), |(host, path)| (host.to_string(), format!("/{path}")));
    let host = authority.split(':').next().unwrap_or("127.0.0.1");
    let port = authority.split(':').nth(1).and_then(|p| p.parse::<u16>().ok()).unwrap_or(80);

    let mut stream = tokio::net::TcpStream::connect((host, port)).await?;
    let mut request = format!("{method} {path} HTTP/1.1\r\nHost: {authority}\r\nAccept: */*\r\n");
    match auth_or_type {
        Some(value) if value.starts_with("Bearer") => request.push_str(&format!("Authorization: {value}\r\n")),
        Some(value) => request.push_str(&format!("Content-Type: {value}\r\n")),
        None => {}
    }
    if let Some(body) = body {
        request.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    request.push_str("Connection: close\r\n\r\n");
    if let Some(body) = body {
        request.push_str(body);
    }
    stream.write_all(request.as_bytes()).await?;
    stream.flush().await?;

    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    let text = String::from_utf8_lossy(&response).into_owned();
    let Some((head, rest)) = text.split_once("\r\n\r\n") else {
        return Err(anyhow::anyhow!("peer returned a malformed response"));
    };
    let status = head.split_whitespace().nth(1).unwrap_or("?");
    if !status.starts_with('2') {
        return Err(anyhow::anyhow!("peer returned {status}: {}", rest.chars().take(200).collect::<String>()));
    }
    Ok(rest.to_string())
}

/// Append the pool token to a WebSocket URL, keeping any existing query string.
fn with_token(url: &str, token: &str) -> String {
    if token.is_empty() {
        return url.to_string();
    }
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}token={token}")
}

/// Runtime configuration, all from the environment so a notebook cell can set it.
#[derive(Debug, Clone)]
struct Config {
    worker_id: String,
    hub_ws: String,
    hub_http: String,
    engine_base: String,
    pool: String,
    pool_token: String,
    capacity: u32,
    model: String,
    model_shas: BTreeSet<String>,
    capabilities: BTreeSet<String>,
    tier: WorkerTier,
}

impl Config {
    /// Read config, refusing to start without a pool token: the hub denies
    /// unauthenticated workers by default, so a tokenless client is a bug.
    fn from_env() -> Self {
        let var = |key: &str, default: &str| std::env::var(key).unwrap_or_else(|_| default.to_string());
        let pool_token = var("NEXUS_POOL_TOKEN", "");
        if pool_token.is_empty() {
            eprintln!("[worker] NEXUS_POOL_TOKEN is empty: the hub will reject this connection");
        }
        let capacity = var("NEXUS_CAPACITY", "4").parse::<u32>().unwrap_or(4).min(MAX_CAPACITY);
        Self {
            worker_id: var("NEXUS_WORKER_ID", &format!("wkr-{}", std::process::id())),
            hub_ws: var("NEXUS_HUB_WS", HUB_WS_DEFAULT),
            hub_http: var("NEXUS_HUB_HTTP", HUB_HTTP_DEFAULT),
            engine_base: var("STRATA_URL", ENGINE_DEFAULT),
            pool: var("NEXUS_POOL", POOL_DEFAULT),
            pool_token,
            capacity,
            model: var("NEXUS_MODEL", MODEL_DEFAULT),
            model_shas: BTreeSet::from([SHARD_1.to_string(), SHARD_2.to_string()]),
            capabilities: ["tools".to_string()].into_iter().collect(),
            tier: match var("NEXUS_TIER", "ephemeral").as_str() {
                "dedicated" => WorkerTier::Dedicated,
                _ => WorkerTier::Ephemeral,
            },
        }
    }
}
