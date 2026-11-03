//! Bulk transfer (task §10) and artifact persistence (task §7.3).
//!
//! * `GET  /v1/payloads/{id}` — a worker fetches a spilled prompt. The bytes live
//!   in the hub's **memory** for `payload_ttl_ms` and are never written to disk
//!   (task §9: free-tier hosts are shared infrastructure, so prompt text must not
//!   end up in a scratch directory).
//! * `PUT  /v1/artifacts/{id}` — a worker uploads one chunk. Bytes are streamed to
//!   disk and the hub measures the digest, so a worker's declared sha256 is
//!   verified rather than trusted (task §9).
//! * `GET  /v1/artifacts/{id}` / `GET /v1/jobs/{id}/result` — a consumer downloads
//!   an artifact, with range support and the digest in a header (test 15).
//!
//! Every state question (is this token still valid? does this attempt still own
//! the upload?) is answered by the actor, not by a second copy of the state: the
//! handlers only move bytes and forward messages.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::http::header;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use nexus_core::actor::DeclaredArtifact;
use nexus_core::auth::bearer;
use nexus_core::ids::ArtifactId;
use nexus_core::ids::JobId;
use nexus_core::ids::PayloadRefId;
use nexus_core::ids::WorkerId;
use nexus_core::store::artifact_path_checked;
use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;
use tokio_stream::wrappers::ReceiverStream;

use crate::state;
use crate::state::Hub;

/// Query parameters of a worker's payload fetch. The token is also accepted as a
/// bearer header, for notebook clients that cannot set one.
#[derive(Debug, Deserialize)]
pub struct PayloadQuery {
    /// Bearer or query token from `Assigned.payload{mode:"ref"}`.
    #[serde(default)]
    pub token: Option<String>,
    /// Worker performing the fetch.
    pub worker: String,
    /// Attempt the fetch belongs to (task §10: an old attempt must not refetch).
    #[serde(default)]
    pub attempt: Option<u64>,
}

/// Query parameters of an artifact upload.
#[derive(Debug, Deserialize)]
pub struct ArtifactQuery {
    /// Bearer token from `Assigned.artifact_put`.
    #[serde(default)]
    pub token: Option<String>,
    /// Job the artifact belongs to.
    pub job: String,
    /// Worker claiming the upload.
    pub worker: String,
    /// Attempt claiming the upload.
    #[serde(default)]
    pub attempt: Option<u64>,
    /// Chunk index, for a resumable multi-part upload.
    #[serde(default)]
    pub chunk: Option<u64>,
    /// `1` when this PUT completes the artifact.
    #[serde(default)]
    pub complete: Option<u64>,
    /// Digest the worker declares for the whole artifact.
    #[serde(default)]
    pub sha256: Option<String>,
    /// Size the worker declares for the whole artifact.
    #[serde(default)]
    pub size: Option<u64>,
    /// Declared MIME type.
    #[serde(default)]
    pub content_type: Option<String>,
}

/// Byte range for an artifact download.
#[derive(Debug, Deserialize, Default)]
pub struct RangeQuery {
    /// Inclusive start byte.
    #[serde(default)]
    pub from: Option<u64>,
    /// Exclusive end byte.
    #[serde(default)]
    pub to: Option<u64>,
}

/// Serve one spilled prompt to the worker that owns it.
pub async fn fetch_payload(
    State(hub): State<Arc<Hub>>,
    Path(id): Path<String>,
    Query(query): Query<PayloadQuery>,
    headers: axum::http::HeaderMap,
) -> Response {
    let Some(reference) = PayloadRefId::parse(&id).ok() else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_id",
            "payload id is not in the safe alphabet",
        );
    };
    let Some(token) = bearer(
        headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok()),
    )
    .or(query.token.as_deref()) else {
        return json_error(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "a payload reference needs its bearer token",
        );
    };
    let Ok(worker) = WorkerId::parse(&query.worker) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_id",
            "worker id is not in the safe alphabet",
        );
    };
    let attempt = query.attempt.unwrap_or(1) as u32;
    // The actor answers: token digest, owning worker, owning attempt and the TTL
    // are all checked in one serialized step, and the fetch is counted.
    let grant = hub.payload_fetch(&reference, token, &worker, attempt).await;
    match grant {
        Some(grant) if grant.reason.is_none() => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, grant.content_type)
            .header(header::CONTENT_LENGTH, grant.bytes.len())
            .header("x-nexus-sha256", grant.sha256)
            .body(axum::body::Body::from(grant.bytes))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response()),
        Some(grant) => json_error(
            StatusCode::FORBIDDEN,
            "payload_denied",
            grant.reason.as_deref().unwrap_or("denied"),
        ),
        None => json_error(
            StatusCode::NOT_FOUND,
            "payload_unavailable",
            "the payload is gone, expired, or its pool is unknown",
        ),
    }
}

/// Accept one artifact chunk from its owning worker.
pub async fn put_artifact(
    State(hub): State<Arc<Hub>>,
    Path(id): Path<String>,
    Query(query): Query<ArtifactQuery>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    let Some(token) = bearer(
        headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok()),
    )
    .or(query.token.as_deref()) else {
        return json_error(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "an artifact upload needs its bearer token",
        );
    };
    let Ok(artifact) = ArtifactId::parse(&id) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_id",
            "artifact id is not in the safe alphabet",
        );
    };
    let Ok(job) = JobId::parse(&query.job) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_id",
            "job id is not in the safe alphabet",
        );
    };
    let Ok(worker) = WorkerId::parse(&query.worker) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_id",
            "worker id is not in the safe alphabet",
        );
    };
    let chunk = query.chunk.unwrap_or(0);
    let complete = query.complete.unwrap_or(0) == 1;
    let attempt = query.attempt.unwrap_or(1) as u32;
    // Enforce the chunk cap **before** writing. The actor also enforces it, but
    // only hears about the bytes after they are on disk, so without this a
    // worker could fill the artifact root with chunks the hub has refused.
    // `Limits::artifact_chunk_bytes` is the value published to the worker in
    // `Assigned.artifact_put.chunk_bytes`.
    let chunk_cap = hub.cfg.limits.artifact_chunk_bytes;
    if body.len() > chunk_cap {
        return json_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "chunk_too_large",
            &format!(
                "{} bytes exceeds the {chunk_cap} byte chunk cap",
                body.len()
            ),
        );
    }

    // Bytes first: the hub measures what it actually stored, which is what makes
    // the declared digest verifiable instead of taken on trust (task §9).
    let (path, written, digest) = match write_chunk(
        &hub.artifact_root,
        &job,
        &artifact,
        attempt,
        chunk,
        &body,
        complete,
    )
    .await
    {
        Ok(value) => value,
        Err(err) => {
            tracing::warn!(%job, %artifact, %err, "artifact write failed");
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "artifact_write_failed",
                &err.to_string(),
            );
        }
    };
    tracing::debug!(%job, %artifact, chunk, written, %path, "artifact chunk stored");

    let Some(handle) = hub.route_job(&job).await else {
        return json_error(
            StatusCode::NOT_FOUND,
            "unknown_job",
            "no pool owns that job",
        );
    };
    // The upload carries no socket, so the hub resolves the worker's live
    // generation; an absent one means the worker is already gone.
    let conn = handle.live_conn(&worker).await.unwrap_or(u64::MAX);
    let msg = nexus_core::ActorMsg::ArtifactStored {
        job: job.clone(),
        artifact: artifact.clone(),
        worker,
        conn,
        attempt,
        chunk_index: chunk,
        chunk_bytes: written,
        complete,
        stored_sha256: complete.then_some(digest.clone()),
        declared: DeclaredArtifact {
            sha256: query.sha256.clone(),
            size_bytes: query.size,
        },
        content_type: query.content_type.clone(),
    };
    if state::send(handle.as_ref(), msg).await.is_err() {
        return json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "hub_shutdown",
            "the pool actor is draining; retry",
        );
    }
    let body = serde_json::to_string(&serde_json::json!({
        "accepted_bytes": written,
        "complete": complete,
        "hub_sha256": digest,
        "chunk": chunk,
        "token_hint": format!("{}…", &token[..token.len().min(8)]),
    }))
    .expect("object serializes");
    (
        StatusCode::ACCEPTED,
        [(header::CONTENT_TYPE, "application/json")],
        body,
    )
        .into_response()
}

/// `GET /v1/artifacts/{id}` — direct artifact download. The artifact id is derived
/// from the job id (`art-<job>`, see `PoolActor::dispatch`), so the job is the
/// routing key.
pub async fn get_artifact(
    State(hub): State<Arc<Hub>>,
    headers: axum::http::HeaderMap,
    Path(id): Path<String>,
    Query(range): Query<RangeQuery>,
) -> Response {
    let tenant = match crate::consumer::tenant_of(&hub, &headers) {
        Ok(tenant) => tenant,
        Err(response) => return response,
    };
    let Some(job_name) = id.strip_prefix("art-") else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_artifact_id",
            "artifact ids are derived from their job id",
        );
    };
    let Ok(job) = JobId::parse(job_name) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_id",
            "the embedded job id is not safe",
        );
    };
    let Some(view) = hub.job_snapshot(&job).await else {
        return json_error(StatusCode::NOT_FOUND, "unknown_job", "no such job");
    };
    // A produced artifact is the submitting tenant's data: reading it by guessed
    // id used to be unauthenticated (§9 tenancy), and a foreign job answers `404`
    // so ids are not enumerable.
    if view.tenant != tenant {
        return json_error(
            StatusCode::NOT_FOUND,
            "unknown_job",
            "no such job for this tenant",
        );
    }
    let Some(artifact) = view.artifact else {
        return json_error(
            StatusCode::NOT_FOUND,
            "artifact_unavailable",
            "this job produced no artifact",
        );
    };
    serve_artifact(
        &artifact.path,
        artifact.size_bytes,
        &artifact.content_type,
        artifact.stored_sha256.as_deref(),
        range.from.unwrap_or(0),
        range.to.unwrap_or(artifact.size_bytes),
    )
    .await
}

/// Bytes copied per poll while streaming an artifact range.
///
/// This is the hub's memory bound for a download: at most
/// `STREAM_INFLIGHT_CHUNKS + 1` of these are alive per request, whatever the size
/// of the object or of the requested range (task §7.3, §9 "bounded memory").
const STREAM_CHUNK_BYTES: u64 = 64 * 1024;

/// How many chunks may sit between the file and the socket before the read task
/// parks. A bounded channel is what turns a slow consumer into backpressure on the
/// reader instead of an unbounded buffer in hub RSS.
const STREAM_INFLIGHT_CHUNKS: usize = 4;

/// Stream `[from, to)` of an artifact out of storage.
///
/// The range is **streamed**, never buffered whole: the file is opened, seeked to
/// `from`, and read in [`STREAM_CHUNK_BYTES`] pieces into a bounded channel that
/// feeds the response body (task §7.3; a 40 MB range of a 40 MB artifact costs
/// ~256 KiB of hub RSS, not 40 MB).
///
/// Checks performed before a single byte is committed:
/// * `to` is clamped to the recorded size (RFC 9110 §14.21.1: an open-ended or
///   over-long range is satisfiable), an inverted or wholly-out-of-bounds range is
///   `416` with `Content-Range: bytes */size`;
/// * the object on disk must be at least as long as the recorded size — a shorter
///   file means the assembly was truncated, and streaming a range past EOF would
///   silently answer a short body with `Content-Length` of the requested length;
/// * for a **whole-object** read the streamed bytes are hashed and compared with
///   the stored digest; a mismatch aborts the body. A *partial* read cannot be
///   digest-checked without reading the whole object (which is exactly what this
///   function must not do), so `x-nexus-sha256` is documented as the digest of the
///   whole artifact, never of the range.
pub async fn serve_artifact(
    path: &str,
    size: u64,
    content_type: &str,
    sha256: Option<&str>,
    from: u64,
    to: u64,
) -> Response {
    let to = to.min(size);
    // `from >= size` is unsatisfiable for a non-empty object; `from > to` can only
    // happen when the client asked for a backwards range (`?from=10&to=4`).
    if from > to || (size > 0 && from >= size) {
        return range_unsatisfiable(size);
    }
    let length = to - from;
    let partial = from != 0 || to != size;

    // Open and stat before answering: a missing or short object is a status code,
    // not a body that dies halfway.
    let mut file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(err) => {
            tracing::warn!(%path, %err, "artifact read failed");
            return json_error(
                StatusCode::NOT_FOUND,
                "artifact_unavailable",
                "the artifact file is missing",
            );
        }
    };
    match file.metadata().await {
        Ok(meta) if meta.len() >= to => {}
        Ok(meta) => {
            tracing::error!(%path, recorded = size, on_disk = meta.len(), "artifact is shorter than the recorded size");
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "artifact_incomplete",
                "the stored artifact is shorter than its recorded size",
            );
        }
        Err(err) => {
            tracing::warn!(%path, %err, "artifact stat failed");
            return json_error(
                StatusCode::NOT_FOUND,
                "artifact_unavailable",
                "the artifact file is missing",
            );
        }
    }
    // Seek before the status is committed: a failed seek is a `404`, not a body
    // that starts and stops. Everything after this point reads forward only.
    if let Err(err) = file.seek(std::io::SeekFrom::Start(from)).await {
        tracing::warn!(%path, %err, from, "artifact seek failed");
        return json_error(
            StatusCode::NOT_FOUND,
            "artifact_unavailable",
            "the artifact file is missing",
        );
    }

    // A whole-object read is the one case where the digest is verifiable while
    // streaming, so it is verified; partial reads keep the digest as an
    // out-of-band value the consumer can check after assembling ranges.
    let verify = sha256
        .filter(|_| from == 0 && to == size && size > 0)
        .map(str::to_string);
    let declared = sha256.map(str::to_string);
    let path_owned = path.to_string();
    let (tx, rx) =
        tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(STREAM_INFLIGHT_CHUNKS);
    tokio::spawn(async move {
        let mut reader = file.take(length);
        let mut hasher = verify
            .as_ref()
            .map(|_| <sha2::Sha256 as sha2::Digest>::new());
        let mut buffer = vec![0u8; STREAM_CHUNK_BYTES as usize];
        let mut streamed = 0u64;
        loop {
            match reader.read(&mut buffer).await {
                Ok(0) => break,
                Ok(read) => {
                    streamed += read as u64;
                    if let Some(h) = hasher.as_mut() {
                        sha2::Digest::update(h, &buffer[..read]);
                    }
                    // The peer hung up: stop reading, do not spin on a closed body.
                    if tx
                        .send(Ok(Bytes::copy_from_slice(&buffer[..read])))
                        .await
                        .is_err()
                    {
                        tracing::debug!(path = %path_owned, streamed, "artifact download consumer hung up mid-range");
                        return;
                    }
                }
                Err(err) => {
                    tracing::warn!(path = %path_owned, %err, "artifact range read failed mid-body");
                    let _ = tx.send(Err(err)).await;
                    return;
                }
            }
        }
        if streamed != length {
            // EOF before the declared length: the body is short, so the status
            // (200/206) was a lie. Abort rather than answer a silent truncation.
            tracing::error!(path = %path_owned, streamed, expected = length, "artifact range ended early");
            let _ = tx
                .send(Err(std::io::Error::other(
                    "the artifact ended before the requested range",
                )))
                .await;
            return;
        }
        if let (Some(expected), Some(h)) = (verify, hasher) {
            // `sha256_hex` digests *its input*, so feeding it the already-final
            // digest would hex a hash of a hash and abort every correct download.
            // The streaming path must hex the 32 digest bytes instead (test 15:
            // the recorded digest is the object's own sha256).
            let digest = sha2::Digest::finalize(h);
            let actual: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
            if actual != *expected {
                tracing::error!(path = %path_owned, expected, actual, "stored artifact digest does not match its bytes");
                let _ = tx
                    .send(Err(std::io::Error::other(
                        "the artifact does not match its recorded digest",
                    )))
                    .await;
            }
        }
    });

    let mut builder = Response::builder()
        .status(if partial {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        })
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_LENGTH, length)
        .header(header::ACCEPT_RANGES, "bytes");
    if let Some(sha) = declared {
        // Whole-object digest, deliberately not a range digest (see above).
        builder = builder.header("x-nexus-sha256", sha);
    }
    if partial {
        builder = builder.header(
            header::CONTENT_RANGE,
            format!("bytes {from}-{}", to.saturating_sub(1)),
        );
    }
    builder
        .body(axum::body::Body::from_stream(ReceiverStream::new(rx)))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// `416 Range Not Satisfiable` with the only header a client can act on.
fn range_unsatisfiable(size: u64) -> Response {
    (
        StatusCode::RANGE_NOT_SATISFIABLE,
        [
            (header::CONTENT_RANGE, format!("bytes */{size}")),
            (header::CONTENT_TYPE, "application/json".to_string()),
        ],
        serde_json::to_string(&nexus_core::adapters::http_error(
            "range_unsatisfiable",
            "the requested range is outside the artifact",
            false,
        ))
        .unwrap_or_else(|_| "{}".to_string()),
    )
        .into_response()
}

/// Write one chunk under a traversal-checked path.
///
/// Returns `(path, bytes_written, sha256 of the whole assembled artifact so far)`.
/// When `complete` is set the parts are concatenated into the final object, so a
/// consumer download reads one file.
async fn write_chunk(
    root: &str,
    job: &JobId,
    artifact: &ArtifactId,
    attempt: u32,
    chunk: u64,
    body: &[u8],
    complete: bool,
) -> anyhow::Result<(String, u64, String)> {
    // Reject traversal before touching the filesystem (task §9 "secure paths").
    let base = artifact_path_checked(root, job.as_ref(), artifact.as_ref())?;
    // Parts are scoped **per attempt**. `ArtifactBook::register` re-opens the same
    // artifact slot when a job is requeued, and a digest measured over the dead
    // attempt's leftover parts would disagree with the bytes this attempt is
    // accounted for — an honest worker's declared `sha256` would then fail against
    // bytes it never sent.
    let parts = format!("{base}.parts/a{attempt:04}");
    if chunk == 0 {
        // Chunk 0 starts this attempt's object: clear the directory so a re-PUT
        // restarts the upload rather than appending to a half-finished one.
        match tokio::fs::remove_dir_all(&parts).await {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
    }
    tokio::fs::create_dir_all(&parts).await?;
    let path = format!("{parts}/chunk-{chunk:06}.bin");
    let mut file = tokio::fs::File::create(&path).await?;
    file.write_all(body).await?;
    file.flush().await?;
    drop(file);

    // Digest over the parts in index order: measures only bytes that are really on
    // disk, and stays correct when a chunk is re-PUT after a partial upload.
    let mut entries: Vec<_> = std::fs::read_dir(&parts)?
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .collect();
    entries.sort();
    let mut assembled = Vec::new();
    for entry in &entries {
        assembled.extend_from_slice(&std::fs::read(entry)?);
    }
    let digest = nexus_core::store::sha256_hex(&assembled);
    if complete {
        tokio::fs::write(&base, &assembled).await?;
        // Keep the parts: a resumed upload must still be able to re-PUT a chunk.
    }
    Ok((path, body.len() as u64, digest))
}

/// Delete an expired artifact and its part directory.
pub async fn delete_artifact(path: &str) {
    let _ = tokio::fs::remove_file(path).await;
    match tokio::fs::remove_dir_all(format!("{path}.parts")).await {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => tracing::warn!(%path, %err, "artifact part cleanup failed"),
    }
}

/// Create the artifact root at startup, so a first upload cannot fail on a missing
/// directory.
pub async fn ensure_root(root: &str) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(root)
        .await
        .map_err(|err| anyhow::anyhow!("cannot create artifact root {root}: {err}"))
}

/// An OpenAI-shaped JSON error body.
fn json_error(status: StatusCode, code: &str, message: &str) -> Response {
    let body = nexus_core::adapters::http_error(
        code,
        message,
        status == StatusCode::SERVICE_UNAVAILABLE || status == StatusCode::TOO_MANY_REQUESTS,
    );
    let text = serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string());
    (status, [(header::CONTENT_TYPE, "application/json")], text).into_response()
}

/// Test-only scratch directory: `<repo-root>/Temp/nexus-tests/<tag>`.
///
/// The root `AGENTS.md` rule puts disposable agent and test work in the repository's own
/// `Temp/`, never OS temp (permission prompts) and never inside the package — the
/// package's `var/` is where a *running hub* spills artifacts, which is a different
/// thing from a test fixture. The root is located by walking up from
/// `env!("CARGO_MANIFEST_DIR")`, which cargo bakes in at compile time, so the answer
/// does not move with the working directory `cargo test` happens to use, and stops at
/// the ancestor carrying `AGENTS.md`.
///
/// Falls back to `<package>/Temp/nexus-tests` if no such ancestor exists (e.g. the
/// package checked out on its own); that path is git-ignored by this package too.
#[cfg(test)]
pub(crate) fn test_scratch(tag: &str) -> std::path::PathBuf {
    let mut dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf();
    while !dir.join("AGENTS.md").is_file() {
        match dir.parent() {
            Some(parent) => dir = parent.to_path_buf(),
            None => break,
        }
    }
    if !dir.join("AGENTS.md").is_file() {
        dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf();
    }
    dir.join("Temp").join("nexus-tests").join(tag)
}

#[cfg(test)]
mod tests {
    //! Artifact range serving: the acceptance criterion is *"bounded streaming of
    //! the requested byte range"*. These tests therefore measure the response
    //! **data frame by data frame** — a whole-object buffer arrives as one giant
    //! chunk, a streaming body arrives as several chunks no larger than
    //! [`super::STREAM_CHUNK_BYTES`] — alongside the range/length/digest decisions
    //! that must be taken *before* any byte is committed.
    //!
    //! Bodies are drained through `BodyExt::into_data_stream`, whose `Stream` item is
    //! documented as `Result<B::Data, B::Error>`; no `Frame` accessor is used, so the
    //! test does not depend on `http-body`'s frame-API naming.
    //!
    //! Object files land under the **repo-root** `Temp/nexus-tests/<test>/` named by the
    //! root `AGENTS.md` scratch rule (see [`super::test_scratch`]), one directory per
    //! test, so the suite stays parallel-safe and nothing disposable is written into the
    //! package or into OS temp.

    use super::*;
    use futures_util::stream::StreamExt;
    use http_body_util::BodyExt;

    /// A deterministic, incompressible-enough payload of `len` bytes.
    fn payload(len: usize) -> Vec<u8> {
        (0..len).map(|i| ((i * 31 + 7) % 251) as u8).collect()
    }

    /// Write one artifact object into this test's own scratch directory.
    async fn object(name: &str, bytes: &[u8]) -> String {
        let dir = test_scratch(name);
        // Own the directory outright: a run starts from an empty tree and cannot
        // inherit bytes from an aborted one.
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch directory");
        let path = dir.join("object.bin");
        tokio::fs::write(&path, bytes)
            .await
            .expect("scratch object");
        path.display().to_string()
    }

    /// Everything a caller can observe about a download, including the chunk sizes
    /// the body actually arrived in.
    struct Served {
        status: StatusCode,
        content_length: u64,
        content_range: String,
        accept_ranges: String,
        digest: String,
        chunks: Vec<usize>,
        bytes: Vec<u8>,
        error: Option<String>,
        text: String,
    }

    async fn drain(response: Response) -> Served {
        // Header names are `HeaderName` values here: `HeaderMap::get` is generic over
        // `AsHeaderName`, and `&HeaderName` is an implementor that needs no conversion.
        // The map is cloned so the header reads do not keep `response` borrowed past
        // `into_body()` — consuming the body is what takes ownership of the response.
        let headers = response.headers().clone();
        let read = |name: &axum::http::HeaderName| {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string()
        };
        let status = response.status();
        let content_length = read(&header::CONTENT_LENGTH).parse().unwrap_or(usize::MAX) as u64;
        let body = response.into_body();
        let mut served = Served {
            status,
            content_length,
            content_range: read(&header::CONTENT_RANGE),
            accept_ranges: read(&header::ACCEPT_RANGES),
            digest: read(&axum::http::HeaderName::from_static("x-nexus-sha256")),
            chunks: Vec::new(),
            bytes: Vec::new(),
            error: None,
            text: String::new(),
        };
        // One read path for every response, streamed or buffered: `into_data_stream`
        // yields exactly the data frames the client would see, so the chunk sizes in
        // `served.chunks` are the hub's own write pattern, not hyper's re-chunking.
        let mut stream = BodyExt::into_data_stream(body);
        while let Some(item) = stream.next().await {
            match item {
                Ok(chunk) => {
                    served.chunks.push(chunk.len());
                    served.bytes.extend_from_slice(&chunk);
                }
                Err(err) => {
                    served.error = Some(err.to_string());
                    break;
                }
            }
        }
        served.text = String::from_utf8_lossy(&served.bytes).into_owned();
        served
    }

    /// A full-object read is streamed in bounded chunks, its length header is the
    /// requested length, and the recorded digest is verified against the bytes.
    #[tokio::test]
    async fn full_object_read_streams_bounded_chunks_and_verifies_the_digest() {
        let bytes = payload(200_000);
        let digest = nexus_core::store::sha256_hex(&bytes);
        let path = object("full_stream", &bytes).await;

        let served = drain(
            serve_artifact(
                &path,
                bytes.len() as u64,
                "application/octet-stream",
                Some(&digest),
                0,
                bytes.len() as u64,
            )
            .await,
        )
        .await;

        assert_eq!(served.status, StatusCode::OK);
        assert_eq!(
            served.content_length, 200_000,
            "the length header is the object length"
        );
        assert_eq!(served.bytes, bytes, "every byte arrives, in order");
        assert!(
            served.chunks.len() >= 4,
            "the body arrives as {} chunks, not one whole-object buffer",
            served.chunks.len()
        );
        assert!(
            served
                .chunks
                .iter()
                .all(|chunk| *chunk <= STREAM_CHUNK_BYTES as usize),
            "no chunk exceeds the stream bound: {:?}",
            served.chunks
        );
        assert_eq!(served.accept_ranges, "bytes");
        assert_eq!(
            served.digest, digest,
            "the whole-object digest is published"
        );
        assert_eq!(
            served.content_range, "",
            "a full read is not a partial response"
        );
        assert_eq!(
            served.error, None,
            "a matching digest must not abort the body"
        );
    }

    /// A partial read carries **only** the slice: the response body length is the
    /// range length, and the bytes are the exact window.
    #[tokio::test]
    async fn partial_range_streams_only_the_requested_slice() {
        let bytes = payload(200_000);
        let digest = nexus_core::store::sha256_hex(&bytes);
        let path = object("partial_slice", &bytes).await;

        let served = drain(
            serve_artifact(
                &path,
                bytes.len() as u64,
                "application/octet-stream",
                Some(&digest),
                100_000,
                150_000,
            )
            .await,
        )
        .await;

        assert_eq!(served.status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(served.content_length, 50_000);
        assert_eq!(
            served.bytes,
            &bytes[100_000..150_000],
            "the window is exact, neither shifted nor padded"
        );
        assert_eq!(served.content_range, "bytes 100000-149999");
        assert_eq!(
            served.digest, digest,
            "the header is the object digest, never a range digest"
        );
        assert_eq!(served.error, None);
        assert_ne!(
            served.bytes.len(),
            bytes.len(),
            "the object itself was never read whole"
        );
    }

    /// An inverted range and a range that starts past the end are both refused with
    /// `416` plus the only header a client can act on.
    #[tokio::test]
    async fn unsatisfiable_ranges_answer_416_before_any_byte_is_committed() {
        let bytes = payload(1_000);
        let path = object("unsatisfiable", &bytes).await;

        let backwards = drain(
            serve_artifact(
                &path,
                bytes.len() as u64,
                "application/octet-stream",
                None,
                10,
                4,
            )
            .await,
        )
        .await;
        assert_eq!(backwards.status, StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(backwards.content_range, "bytes */1000");
        assert!(
            backwards.text.contains("range_unsatisfiable"),
            "the error body names the code: {}",
            backwards.text
        );
        assert!(
            backwards.bytes.len() < 200,
            "the answer is the error object, not the artifact: {} bytes",
            backwards.bytes.len()
        );

        let past_end = drain(
            serve_artifact(
                &path,
                bytes.len() as u64,
                "application/octet-stream",
                None,
                1_000,
                1_010,
            )
            .await,
        )
        .await;
        assert_eq!(
            past_end.status,
            StatusCode::RANGE_NOT_SATISFIABLE,
            "a range starting at or past EOF is unsatisfiable"
        );

        // An *open-ended* range is satisfiable and clamps, per RFC 9110 §14.21.1.
        let clamped = drain(
            serve_artifact(
                &path,
                bytes.len() as u64,
                "application/octet-stream",
                None,
                900,
                5_000,
            )
            .await,
        )
        .await;
        assert_eq!(clamped.status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(clamped.content_length, 100);
        assert_eq!(clamped.bytes, &bytes[900..1_000]);

        // A whole 64 KiB range of a 256 KiB object stays inside the stream bound.
        let big = payload(262_144);
        let big_path = object("unsatisfiable_big", &big).await;
        let window = drain(
            serve_artifact(
                &big_path,
                big.len() as u64,
                "application/octet-stream",
                None,
                131_072,
                196_608,
            )
            .await,
        )
        .await;
        assert_eq!(window.status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(window.bytes, &big[131_072..196_608]);
        assert!(
            window
                .chunks
                .iter()
                .all(|chunk| *chunk <= STREAM_CHUNK_BYTES as usize),
            "{:?}",
            window.chunks
        );
    }

    /// The recorded size outrunning the bytes on disk (an interrupted assembly) is
    /// a status code, not a short body wearing a long `Content-Length`.
    #[tokio::test]
    async fn a_recorded_size_beyond_the_stored_object_is_refused() {
        let bytes = payload(1_000);
        let path = object("short_object", &bytes).await;

        let served =
            drain(serve_artifact(&path, 4_000, "application/octet-stream", None, 0, 4_000).await)
                .await;

        assert_eq!(served.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            served.text.contains("artifact_incomplete"),
            "the answer names the corruption: {}",
            served.text
        );
        assert!(
            served.bytes.len() < 200,
            "the object was not streamed once its length was found wrong: {} bytes",
            served.bytes.len()
        );
    }

    /// A stored object whose bytes disagree with its digest aborts the body after
    /// the (unverifiable-in-advance) status has been sent: a silent truncation is
    /// worse than a broken stream.
    #[tokio::test]
    async fn a_digest_mismatch_aborts_the_body() {
        let bytes = payload(150_000);
        let path = object("digest_mismatch", &bytes).await;

        let served = drain(
            serve_artifact(
                &path,
                bytes.len() as u64,
                "application/octet-stream",
                Some(&"0".repeat(64)),
                0,
                bytes.len() as u64,
            )
            .await,
        )
        .await;

        assert_eq!(
            served.status,
            StatusCode::OK,
            "the digest is only knowable once the bytes have been read"
        );
        assert_eq!(served.bytes, bytes, "the bytes are still delivered");
        let error = served
            .error
            .expect("a mismatched digest must end the body with an error");
        assert!(
            error.contains("digest"),
            "the abort reason is the digest: {error}"
        );
    }

    /// A missing file is a status, and a zero-length object is a valid empty answer.
    #[tokio::test]
    async fn missing_objects_answer_404_and_empty_ones_answer_200() {
        // A path under the scratch root that no test ever writes to.
        let missing_path = test_scratch("nope")
            .join("object.bin")
            .display()
            .to_string();
        let missing =
            drain(serve_artifact(&missing_path, 10, "application/octet-stream", None, 0, 10).await)
                .await;
        assert_eq!(missing.status, StatusCode::NOT_FOUND);
        assert!(missing.text.contains("artifact_unavailable"));

        let path = object("empty_object", b"").await;
        let empty =
            drain(serve_artifact(&path, 0, "application/octet-stream", None, 0, 0).await).await;
        assert_eq!(empty.status, StatusCode::OK);
        assert_eq!(empty.content_length, 0);
        assert!(empty.bytes.is_empty());
        assert_eq!(empty.error, None);
    }
}
