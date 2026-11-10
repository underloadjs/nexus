// Test builds may be blunt (workspace lint policy, see Cargo.toml): `io::tests`
// and `runtime_e2e` assert with `expect`, which is what an assertion is.
#![cfg_attr(test, allow(clippy::expect_used, clippy::unwrap_used))]

mod consumer;
mod consumer_ws;
mod io;
mod observe;
#[cfg(test)]
mod runtime_e2e;
mod state;
mod worker_ws;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::http::Method;
use axum::routing::get;
use axum::routing::post;
use axum::Router;
use nexus_core::BrokerConfig;
use tower_http::cors::{AllowHeaders, AllowMethods, AllowOrigin, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;

use crate::state::Hub;

/// Environment variable naming is documented in the package README; every value
/// here is opt-in with a conservative default, and `validate()` refuses an
/// unauthenticated hub unless `NEXUS_ALLOW_ANONYMOUS=1` says otherwise.
const LISTEN_ENV: &str = "NEXUS_LISTEN";
const ARTIFACT_ROOT_ENV: &str = "NEXUS_ARTIFACT_ROOT";
const BODY_LIMIT_ENV: &str = "NEXUS_MAX_BODY_BYTES";
const DEFAULT_LISTEN: &str = "127.0.0.1:8085";

/// Default consumer body cap. Larger than the 8 MiB inline cap because a spilled
/// prompt must be uploadable in one request.
const DEFAULT_BODY_LIMIT: usize = 64 << 20;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Structured logs; RUST_LOG wins, default keeps the hub quiet but useful.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new("info,nexus_server=info,nexus_core=warn")
            }),
        )
        .with_target(false)
        .init();

    let values = collect_env();
    // `from_values_checked`, not the best-effort variant: a `NEXUS_CONFIG_JSON`
    // that does not parse (a typo in a field name, a truncated document) must
    // stop the hub here rather than boot it on the built-in sample pools with no
    // credentials, which is what silently keeping the defaults used to do.
    let cfg = BrokerConfig::from_values_checked(&values)
        .map_err(|e| anyhow::anyhow!("configuration refused startup: {e}"))?;
    cfg.validate()
        .map_err(|e| anyhow::anyhow!("configuration refused startup: {e}"))?;
    let listen: SocketAddr = values
        .get(LISTEN_ENV)
        .and_then(|v| v.parse().ok())
        // Infallible by construction: the fallback is a literal address, so a
        // parse failure here is a typo in `DEFAULT_LISTEN` itself. Failing loudly
        // beats substituting some other address, which would put the hub on a port
        // nobody configured and make the health check pass in the wrong place.
        .unwrap_or_else(|| {
            #[allow(clippy::expect_used)]
            let fallback: SocketAddr = DEFAULT_LISTEN
                .parse()
                .expect("built-in listen address parses");
            fallback
        });
    let artifact_root = values
        .get(ARTIFACT_ROOT_ENV)
        .cloned()
        .unwrap_or_else(|| "Temp/nexus/artifacts".to_string());
    let body_limit = values
        .get(BODY_LIMIT_ENV)
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_BODY_LIMIT);

    let hub = Hub::new(cfg, &artifact_root);
    io::ensure_root(&artifact_root).await?;

    let app = app(Arc::clone(&hub), body_limit);

    let listener = tokio::net::TcpListener::bind(listen).await?;
    let local = listener.local_addr()?;
    tracing::info!(%local, pools = hub.pool_ids().len(), artifact_root, "nexus hub listening");

    // Graceful shutdown: stop accepting, then cancel in-flight work so GPUs stop
    // burning (task §6, §16.18's "clean, retryable failure").
    let shutdown_hub = Arc::clone(&hub);
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            match tokio::signal::ctrl_c().await {
                Ok(()) => tracing::info!("SIGINT received, draining"),
                Err(err) => tracing::warn!("signal handler unavailable: {err}"),
            }
            state::drain_pools(&shutdown_hub).await;
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        })
        .await?;
    Ok(())
}

/// The whole consumer + worker surface, factored out of `main` so `runtime_e2e`
/// serves the same route table, layers and state type on an ephemeral port instead
/// of a copy that could drift.
///
/// `body_limit` is the `tower-http` request cap; above it a body is refused before
/// any handler sees it (task §9's size limits, §10's honest inline cap).
pub fn app(hub: Arc<Hub>, body_limit: usize) -> Router {
    Router::new()
        // ---- worker surface (task §4) ----
        .route("/v1/worker", get(worker_ws::upgrade))
        // ---- consumer surface (task §7.1) ----
        .route("/v1/chat/completions", post(consumer::chat_completions))
        .route("/v1/completions", post(consumer::completions))
        .route(
            "/v1/jobs",
            post(consumer::create_job).get(consumer::list_jobs),
        )
        .route(
            "/v1/jobs/{id}",
            get(consumer::job_status).delete(consumer::cancel_job),
        )
        .route("/v1/jobs/{id}/events", get(consumer::job_events))
        .route("/v1/jobs/{id}/result", get(consumer::job_result))
        .route("/v1/models", get(consumer::models))
        // NEX-3: the same consumer surface over one socket, so several tasks can be
        // posted and watched at once and a verdict can arrive before any event
        // byte. It sits with the HTTP routes on purpose — same auth, same actor,
        // same admission rules; only the transport differs.
        .route("/v1/consumer", get(consumer_ws::upgrade))
        // ---- payload and artifact bulk transfer (task §10) ----
        .route("/v1/payloads/{id}", get(io::fetch_payload))
        .route(
            "/v1/artifacts/{id}",
            get(io::get_artifact).put(io::put_artifact),
        )
        // ---- operations: open on purpose, a load balancer has no API key ----
        .route("/healthz", get(consumer::healthz))
        .route("/metrics", get(consumer::metrics))
        // ---- observation plane for the GitHub-Pages dashboard ----
        // Open by the same argument as `/healthz` (no secret, no identity), and
        // additionally CORS-enabled because a browser on another origin calls it.
        // Documented in README ("Routes" + "`GET /v1/observe`") and in
        // `documents/specifications/hub-specification.md` §7's route list.
        .route("/v1/observe", get(observe::observe))
        // `observe_cors` is added last, so it is the OUTERMOST layer: an `OPTIONS`
        // preflight is answered there without reaching the router, the body limit,
        // or any handler. Order matters — see `observe_cors`.
        .layer(observe_cors())
        .layer(RequestBodyLimitLayer::new(body_limit))
        .layer(TraceLayer::new_for_http())
        .with_state(hub)
}

/// CORS policy for the observation plane: any origin, `GET`/`HEAD`/`OPTIONS`,
/// any request headers, and NO credentials.
///
/// Why it is safe to be permissive: `/v1/observe` carries no secret (the payload
/// is aggregated pool state and job metadata) and never acts on the caller's
/// identity, so a cross-site read cannot be forged into a mutation. Keeping
/// credentials off is what makes `Allow-Origin: *` legal in the first place, and
/// it means a browser never sends a cookie or `Authorization` header that this
/// handler would then have to interpret.
///
/// The layer wraps every route, but only `GET` is a cross-origin read; `POST
/// /v1/jobs` stays protected by the browser's same-origin policy plus its API key
/// header, and the WS endpoints are not CORS-gated at all (a WS handshake ignores
/// CORS). Because this layer is outermost, an `OPTIONS` preflight is answered by
/// the layer itself, before the router or the body-limit layer sees it.
fn observe_cors() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(AllowOrigin::any())
        .allow_methods(AllowMethods::list(vec![
            Method::GET,
            Method::HEAD,
            Method::OPTIONS,
        ]))
        .allow_headers(AllowHeaders::any())
        // Explicit, because `Allow-Origin: *` and credentials are mutually
        // exclusive: if a cookie were sent, the browser would reject `*`.
        .allow_credentials(false)
        // Cache the preflight, so a dashboard polling every second does not pay a
        // round trip per poll.
        .max_age(Duration::from_secs(600))
}

/// Snapshot of the process environment as a sorted map, so `nexus-core` can parse
/// configuration without ever touching `std::env` itself (keeps the core wasm-safe
/// and unit-testable).
fn collect_env() -> std::collections::BTreeMap<String, String> {
    std::env::vars().collect()
}
