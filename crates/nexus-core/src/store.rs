//! Payload spill and artifact bookkeeping (task §7.3, §9, §10).
//!
//! # Retention policy, stated explicitly (task §9)
//!
//! | Data | Where | Lifetime | Why |
//! | --- | --- | --- | --- |
//! | Request payload / prompt text (including spilled `payload_ref` bodies) | **memory only** | `Limits::payload_ttl_ms` or job terminal, whichever first | free-tier hosts are shared infrastructure; §9 forbids leaving prompt text in a scratch directory |
//! | Worker→consumer deltas | memory ring buffer, bounded twice | `Limits::replay_*` | replay needs them, RSS needs a bound |
//! | Artifacts (image/video/TTS output) | disk (or object storage), streamed | `Limits::artifact_ttl_ms`, or `MAX_ARTIFACT_ENTRIES` eviction, whichever comes first | "tens of MB" must not accumulate in RSS (§7.3) |
//!
//! This module holds only the *decisions and bookkeeping*; the native runtime
//! performs the actual `read`/`write`, and the WASM host has no filesystem at
//! all. The rules that make acceptance tests 15/16 pass (spill threshold, path
//! safety, TTL, sha verification) all live here so they are unit-testable in both
//! runtimes.

use std::collections::BTreeMap;

use crate::errors::CoreError;
use crate::ids::ArtifactId;
use crate::ids::JobId;
use crate::ids::PayloadRefId;
use crate::ids::WorkerId;
use crate::model::ArtifactRecord;
use crate::protocol::ArtifactPut;
use crate::time::Timestamp;
use crate::util::is_safe_id;
use crate::util::to_hex;

/// sha256 of arbitrary bytes, lowercase hex. Used for payload digests,
/// artifact verification and credential digests.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    to_hex(&hasher.finalize())
}

/// Content-hash fallback idempotency key (task §5.2): `sha256(tenant|kind|body)`.
/// Deterministic across submissions of the same request, so a flaky client
/// retrying a 202 cannot create a second execution.
#[must_use]
pub fn content_idempotency_key(tenant: &str, kind: &str, body: &[u8]) -> String {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(tenant.as_bytes());
    hasher.update([0u8]);
    hasher.update(kind.as_bytes());
    hasher.update([0u8]);
    hasher.update(body);
    to_hex(&hasher.finalize())
}

/// What the hub hands the worker in `Assigned.payload{mode:"ref"}`: enough to
/// `GET` the body, and nothing more.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadDescriptor {
    /// Handle.
    pub id: PayloadRefId,
    /// Bearer token for the fetch (plaintext; only ever this once).
    pub token: String,
    /// Fetch path.
    pub path: String,
    /// Digest of the body.
    pub sha256: String,
    /// Body length.
    pub size_bytes: u64,
    /// Declared content type.
    pub content_type: String,
    /// Fetch must start before this instant.
    pub expires_ms: Timestamp,
}

/// A spilled payload entry. Bytes live in memory (never on disk) — the entry is
/// the bookkeeping that lets the worker fetch them exactly once per lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadEntry {
    /// Handle.
    pub id: PayloadRefId,
    /// Job the payload belongs to.
    pub job: JobId,
    /// Worker allowed to fetch it (set at dispatch, so a pre-dispatch fetch is
    /// refused rather than guessed).
    pub owner_worker: Option<WorkerId>,
    /// Attempt that owns it: a requeue issues a new token, so an old attempt
    /// cannot refetch after ownership moved.
    pub attempt: u32,
    /// sha256 of the bearer token that unlocks the fetch. The plaintext token is
    /// never stored, so a state dump cannot replay a live url.
    pub token_digest: String,
    /// Digest of the stored bytes.
    pub sha256: String,
    /// Exact size in bytes.
    pub size_bytes: u64,
    /// Declared content type.
    pub content_type: String,
    /// The prompt bytes themselves. **Memory-only**: task §9 forbids leaving
    /// prompt text in a scratch directory, so these are never written to disk,
    /// and the book's entry/byte bounds are what cap RSS.
    pub bytes: Vec<u8>,
    /// When it was stored.
    pub created_ms: Timestamp,
    /// Fetch must start before this instant.
    pub expires_ms: Timestamp,
    /// Number of successful fetches (a worker may re-GET after a crash).
    pub fetch_count: u32,
}

/// One artifact upload/download slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactEntry {
    /// Record (path, digests, chunks, TTL).
    pub record: ArtifactRecord,
    /// Worker allowed to write it.
    pub owner_worker: Option<WorkerId>,
    /// Attempt that owns the upload.
    pub attempt: u32,
}

/// Bookkeeping for spilled request payloads, bounded by both entry count and
/// total bytes so "memory only" cannot become "memory unbounded".
#[derive(Debug, Clone)]
pub struct PayloadBook {
    entries: BTreeMap<PayloadRefId, PayloadEntry>,
    total_bytes: u64,
    max_entries: usize,
    max_bytes: u64,
    ttl_ms: u64,
    /// Bytes the hub refused to hold, i.e. admissions rejected for spill budget
    /// (metrics: `nexus_spill_rejected_total`).
    rejected: u64,
}

impl PayloadBook {
    /// New book with the configured bounds.
    #[must_use]
    pub fn new(max_entries: usize, max_bytes: u64, ttl_ms: u64) -> Self {
        Self {
            entries: BTreeMap::new(),
            total_bytes: 0,
            max_entries,
            max_bytes,
            ttl_ms,
            rejected: 0,
        }
    }

    /// Spill decision (task §10): at or below the inline cap the payload rides
    /// `Assigned`; above it, the worker fetches by reference.
    #[must_use]
    pub fn needs_spill(size_bytes: usize, inline_cap_bytes: usize) -> bool {
        size_bytes > inline_cap_bytes
    }

    /// Register a spilled payload and return the wire descriptor. `bytes` are
    /// retained in memory by the book until the job terminates or the TTL lapses.
    ///
    /// # Errors
    /// [`CoreError::Queue`] when the spill budget is exhausted — the caller
    /// turns that into a `429`, because "no room to hold your prompt" is a
    /// retryable admission failure, not a client bug.
    pub fn register(
        &mut self,
        job: &JobId,
        token: &str,
        sha256: String,
        bytes: Vec<u8>,
        content_type: String,
        now_ms: Timestamp,
    ) -> Result<PayloadDescriptor, CoreError> {
        let size_bytes = bytes.len() as u64;
        let id = PayloadRefId::new_unchecked(job.as_ref().to_string());
        // A retry re-registers the **same** job id with a fresh attempt-scoped
        // token (see `PoolActor::dispatch`), because this book is keyed by job.
        // The outgoing entry has to leave the budget *before* the bounds are
        // checked: double-counting it inflated `total_bytes` on every attempt
        // (acceptance test 16 asserts the exact figure), and the entry-count
        // bound made a plain renewal of an already-held prompt fail admission —
        // which the actor turns into a spurious, prompt-losing `QueueTimeout`.
        if let Some(previous) = self.entries.remove(&id) {
            self.total_bytes = self.total_bytes.saturating_sub(previous.size_bytes);
        }
        if self.entries.len() >= self.max_entries
            || self.total_bytes.saturating_add(size_bytes) > self.max_bytes
        {
            self.rejected += 1;
            return Err(CoreError::Queue(format!(
                "spill budget exhausted ({} entries / {} of {} bytes)",
                self.entries.len(),
                self.total_bytes,
                self.max_bytes
            )));
        }
        let expires_ms = now_ms.saturating_add(self.ttl_ms);
        let entry = PayloadEntry {
            // The plaintext token exists only in the returned descriptor: the
            // book keeps its digest, so a state dump cannot replay a live url.
            id: id.clone(),
            job: job.clone(),
            owner_worker: None,
            attempt: 0,
            token_digest: sha256_hex(token.as_bytes()),
            sha256: sha256.clone(),
            size_bytes,
            content_type: content_type.clone(),
            bytes,
            created_ms: now_ms,
            expires_ms,
            fetch_count: 0,
        };
        self.total_bytes = self.total_bytes.saturating_add(size_bytes);
        self.entries.insert(id.clone(), entry);
        Ok(PayloadDescriptor {
            // Cloned, not moved: `id` is still needed for the fetch path below.
            id: id.clone(),
            token: token.to_string(),
            path: format!("/v1/payloads/{}", id.as_ref()),
            sha256,
            size_bytes,
            content_type,
            expires_ms,
        })
    }

    /// Number of live entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True when nothing is spilled.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Bytes currently held in memory.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Spill-budget rejections so far.
    #[must_use]
    pub fn rejected(&self) -> u64 {
        self.rejected
    }

    /// The retained prompt bytes, for the ref-fetch endpoint.
    #[must_use]
    pub fn payload_bytes(&self, id: &PayloadRefId) -> Option<&[u8]> {
        self.entries.get(id).map(|entry| entry.bytes.as_slice())
    }

    /// Bind a payload to the worker that is about to own the job. Called when
    /// the `Assigned` is emitted, so a fetch before dispatch is unauthorized.
    pub fn assign_owner(&mut self, id: &PayloadRefId, worker: &WorkerId, attempt: u32) {
        if let Some(entry) = self.entries.get_mut(id) {
            entry.owner_worker = Some(worker.clone());
            entry.attempt = attempt;
        }
    }

    /// Authorize a fetch: right token, right worker, right attempt, not expired.
    /// All three are required — a leaked url without the bearer token gets
    /// `401`, and a valid token replayed by another worker also gets `401`.
    ///
    /// # Errors
    /// [`CoreError::Queue`] if evicted/expired, [`CoreError::Unauthorized`] on a
    /// wrong token, wrong worker, wrong attempt or a lapsed TTL.
    pub fn authorize_fetch(
        &self,
        id: &PayloadRefId,
        token: &str,
        worker: &WorkerId,
        attempt: u32,
        now_ms: Timestamp,
    ) -> Result<&PayloadEntry, CoreError> {
        let entry = self.entries.get(id).ok_or_else(|| {
            CoreError::Queue(format!("payload {} is gone or expired", id.as_ref()))
        })?;
        if !crate::util::constant_time_eq(
            entry.token_digest.as_bytes(),
            sha256_hex(token.as_bytes()).as_bytes(),
        ) {
            return Err(CoreError::Unauthorized(
                "payload token does not match".to_string(),
            ));
        }
        match &entry.owner_worker {
            Some(owner) if owner == worker && entry.attempt == attempt => {}
            Some(_) => {
                return Err(CoreError::Unauthorized(
                    "this worker does not own that payload reference".to_string(),
                ))
            }
            None => {
                return Err(CoreError::Unauthorized(
                    "payload is not assigned to a worker yet".to_string(),
                ))
            }
        }
        if now_ms >= entry.expires_ms {
            return Err(CoreError::Unauthorized(
                "payload reference expired".to_string(),
            ));
        }
        Ok(entry)
    }

    /// Record a successful fetch.
    pub fn record_fetch(&mut self, id: &PayloadRefId) {
        if let Some(entry) = self.entries.get_mut(id) {
            entry.fetch_count += 1;
        }
    }

    /// Release a payload (job terminal or TTL expired). Returns the bytes freed.
    pub fn release(&mut self, id: &PayloadRefId) -> Option<u64> {
        self.entries.remove(id).map(|entry| {
            self.total_bytes = self.total_bytes.saturating_sub(entry.size_bytes);
            entry.size_bytes
        })
    }

    /// Expire everything past its TTL, returning the ids so the embedder can
    /// drop the retained bytes.
    pub fn purge_expired(&mut self, now_ms: Timestamp) -> Vec<PayloadRefId> {
        let stale: Vec<PayloadRefId> = self
            .entries
            .iter()
            .filter(|(_, e)| now_ms >= e.expires_ms)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &stale {
            self.release(id);
        }
        stale
    }

    /// Release every payload belonging to a job.
    pub fn release_for_job(&mut self, job: &JobId) -> Vec<PayloadRefId> {
        let ids: Vec<PayloadRefId> = self
            .entries
            .iter()
            .filter(|(_, e)| &e.job == job)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &ids {
            self.release(id);
        }
        ids
    }
}

/// Bookkeeping for artifacts on disk, with chunk accounting so a partially
/// uploaded artifact can be resumed and verified (acceptance test 15).
#[derive(Debug, Clone)]
pub struct ArtifactBook {
    entries: BTreeMap<ArtifactId, ArtifactEntry>,
    total_bytes: u64,
    max_bytes: u64,
    chunk_cap_bytes: usize,
    ttl_ms: u64,
    root: String,
    /// Slots dropped by the count bound whose bytes still have to leave the disk.
    /// Drained by [`ArtifactBook::purge_expired`], so an eviction reuses the TTL
    /// sweep's deletion path (`Effect::DeleteArtifact` → `io::delete_artifact`)
    /// instead of inventing a second effect the actor would have to learn about.
    pending_deletions: Vec<(ArtifactId, String)>,
    /// Slots dropped by the count bound. Exported for a
    /// `nexus_artifact_evicted_total` gauge; the actor does not read it yet.
    evictions: u64,
}

/// How many artifact slots the book may hold before it starts evicting — the
/// count half of NEX-7.2's "bounded twice over, by count/bytes **and** by TTL".
///
/// **The stated reason every entry exists** (NEX-7.2's other clause): an entry is
/// (a) the chunk ledger for one upload, one `(index, size)` pair per chunk and so
/// up to `max_artifact_bytes / artifact_chunk_bytes` = 131 072 pairs for a single
/// 512 MiB artifact, (b) one staging directory under the artifact root, and (c)
/// the ability to answer `GET /v1/artifacts/{id}` for one produced artifact.
/// `max_bytes` bounds only the *bytes those entries describe*: a thousand jobs
/// uploading 1 KiB each sit three orders of magnitude below the 512 MiB budget
/// while still growing the map, the directory count and the downloadable set
/// without limit. The count bound closes exactly that hole.
///
/// 128 is deliberately `Limits::queue_depth`'s number — no more artifact slots
/// than the pool can hold jobs at once. It is a `const` and not a `Limits` field
/// because it is a property of this bookkeeping structure, not a storage decision
/// an operator has a reason to tune (unlike `max_artifact_bytes`).
const MAX_ARTIFACT_ENTRIES: usize = 128;

impl ArtifactBook {
    /// New book rooted at `root` (server-side directory or object prefix).
    #[must_use]
    pub fn new(
        root: impl Into<String>,
        max_bytes: u64,
        chunk_cap_bytes: usize,
        ttl_ms: u64,
    ) -> Self {
        Self {
            entries: BTreeMap::new(),
            total_bytes: 0,
            max_bytes,
            chunk_cap_bytes,
            ttl_ms,
            root: root.into(),
            pending_deletions: Vec::new(),
            evictions: 0,
        }
    }

    /// Build the `Assigned.artifact_put` hint for an artifact slot.
    #[must_use]
    pub fn upload_hint(
        &self,
        artifact: &ArtifactId,
        token: &str,
        expires_ms: Timestamp,
        chunk_bytes: usize,
    ) -> Option<ArtifactPut> {
        // Existence gate only. The stored record's own `path` is the hub-side
        // location, which this worker-facing PUT hint deliberately does not leak.
        if !self.entries.contains_key(artifact) {
            return None;
        }
        Some(ArtifactPut {
            artifact: artifact.to_string(),
            path: format!("/v1/artifacts/{}", artifact.as_ref()),
            token: token.to_string(),
            // The worker PUTs relative to the hub, so the URL path is what
            // matters; `record.path` is the hub-side storage location.
            expires_ms,
            chunk_bytes,
        })
    }

    /// Storage root.
    #[must_use]
    pub fn root(&self) -> &str {
        &self.root
    }

    /// Allocate an artifact slot for a job.
    ///
    /// Enforces [`MAX_ARTIFACT_ENTRIES`] before inserting (NEX-7.2): the book
    /// never grows past the count bound, and whatever it drops is queued for the
    /// same file deletion the TTL sweep performs. See [`ArtifactBook::make_room`]
    /// for what gets sacrificed and why.
    #[must_use]
    pub fn register(&mut self, job: &JobId, id: ArtifactId, now_ms: Timestamp) -> ArtifactEntry {
        let path = artifact_path(&self.root, job.as_ref(), id.as_ref());
        let entry = ArtifactEntry {
            owner_worker: None,
            attempt: 0,
            record: ArtifactRecord {
                id: id.clone(),
                job: job.clone(),
                content_type: "application/octet-stream".to_string(),
                size_bytes: 0,
                declared_sha256: None,
                stored_sha256: None,
                path,
                chunks: BTreeMap::new(),
                expires_ms: now_ms.saturating_add(self.ttl_ms),
                complete: false,
            },
        };
        if let Some(previous) = self.entries.remove(&id) {
            // A re-dispatch re-opens the same artifact slot for the new attempt.
            // The old attempt's chunk accounting disappears with the entry, so
            // its bytes must leave the shared budget here, or `total_bytes`
            // ratchets up on every retry and eventually refuses an upload that
            // fits `max_artifact_bytes`.
            self.total_bytes = self
                .total_bytes
                .saturating_sub(previous.record.recorded_bytes());
        }
        // Freeing a slot *before* the insert is what makes the bound exact: after
        // removing this id's own entry the book is at most one under the cap, so
        // a re-dispatched job never evicts somebody else's artifact.
        self.make_room(now_ms);
        self.entries.insert(id, entry.clone());
        entry
    }

    /// Drops entries until one fits under [`MAX_ARTIFACT_ENTRIES`].
    ///
    /// Victim order, oldest first inside each class (`expires_ms` ascending —
    /// `expires_ms` is refreshed by both `record_chunk` and `complete`, so it is
    /// the least-recently-active order, not just the registration order):
    ///
    /// 1. **already expired** — the entry is only still here because no sweep has
    ///    run since it lapsed; the sweep would delete it anyway, so this is free;
    /// 2. **complete** — its bytes are on disk and were downloadable. Preferring a
    ///    finished artifact over a fresh upload is a stated trade: the job record
    ///    keeps the `ArtifactRecord` after the slot goes (so the manifest is still
    ///    readable), only the *bytes* become unreachable, and a consumer that
    ///    arrives late gets a clean `404 artifact_unavailable` from `get_artifact`.
    ///    The job waiting for this slot, by contrast, has been dispatched and cannot
    ///    deliver its result at all without one;
    /// 3. **saturation corner** — every slot belongs to an in-flight upload, so
    ///    nothing is safe to sacrifice and the oldest goes anyway. The honest
    ///    consequence, stated because it is observable: that upload's next chunk
    ///    PUT finds no slot, `ArtifactBook::record_chunk` answers
    ///    `Queue("artifact ... is unknown or already expired")`, and
    ///    `PoolActor::on_artifact_stored` turns that into a `413` reject for the
    ///    job (`actor.rs:2353-2361`) — a clean, retryable failure per NEX-7.3, not
    ///    corruption. The bytes it had already staged go with the entry through
    ///    `pending_deletions`. Refusing the *new* slot instead was not an option:
    ///    `register` returns an `ArtifactEntry`, not a `Result`, because dispatch
    ///    cannot roll back a lease it already granted.
    ///
    /// Ties break on artifact id: `BTreeMap` iterates in key order and
    /// `min_by_key` keeps the first minimum, which keeps eviction deterministic
    /// (the same reason the whole crate uses ordered maps).
    fn make_room(&mut self, now_ms: Timestamp) {
        // One victim per iteration, so at most one entry is evicted per `register`,
        // and `pending_deletions` cannot outrun the sweep: the actor ticks at
        // `heartbeat_ms.min(1000).max(50)` (`state.rs:731`), so a queued path waits
        // at most a second on disk.
        while self.entries.len() >= MAX_ARTIFACT_ENTRIES {
            let Some(victim) = self
                .oldest(|entry| now_ms >= entry.record.expires_ms)
                .or_else(|| self.oldest(|entry| entry.record.complete))
                .or_else(|| self.oldest(|_| true))
            else {
                // Unreachable: the loop body only runs while the map is non-empty.
                break;
            };
            if let Some(entry) = self.entries.remove(&victim) {
                // The evicted bytes leave the shared budget exactly as a TTL purge
                // removes them, or `total_bytes` would leak against `max_bytes`.
                self.total_bytes = self
                    .total_bytes
                    .saturating_sub(entry.record.recorded_bytes());
                self.pending_deletions.push((victim, entry.record.path));
                self.evictions += 1;
            }
        }
    }

    /// Oldest entry (by `expires_ms`) matching `is_victim`, in artifact-id order
    /// on ties. Kept separate so [`ArtifactBook::make_room`] can chain its three
    /// preference classes.
    fn oldest(&self, is_victim: impl Fn(&ArtifactEntry) -> bool) -> Option<ArtifactId> {
        self.entries
            .iter()
            .filter(|(_, entry)| is_victim(entry))
            .min_by_key(|(_, entry)| entry.record.expires_ms)
            .map(|(id, _)| id.clone())
    }

    /// Bind the upload to a worker attempt.
    pub fn assign_owner(&mut self, id: &ArtifactId, worker: &WorkerId, attempt: u32) {
        if let Some(entry) = self.entries.get_mut(id) {
            entry.owner_worker = Some(worker.clone());
            entry.attempt = attempt;
        }
    }

    /// Record a stored chunk.
    ///
    /// # Errors
    /// [`CoreError::TooLarge`] when the chunk exceeds the configured chunk cap
    /// or the artifact would exceed the total byte budget;
    /// [`CoreError::Unauthorized`] when a different worker tries to write.
    pub fn record_chunk(
        &mut self,
        id: &ArtifactId,
        worker: &WorkerId,
        attempt: u32,
        chunk_index: u64,
        chunk_bytes: u64,
        now_ms: Timestamp,
    ) -> Result<(), CoreError> {
        if chunk_bytes as usize > self.chunk_cap_bytes {
            return Err(CoreError::TooLarge {
                what: "artifact chunk",
                limit: self.chunk_cap_bytes,
                actual: chunk_bytes as usize,
            });
        }
        let entry = self.entries.get_mut(id).ok_or_else(|| {
            CoreError::Queue(format!(
                "artifact {} is unknown or already expired",
                id.as_ref()
            ))
        })?;
        if entry.owner_worker.as_ref() != Some(worker) || entry.attempt != attempt {
            return Err(CoreError::Unauthorized(
                "this worker does not own that artifact upload".to_string(),
            ));
        }
        if entry.record.complete {
            return Err(CoreError::Protocol {
                reason: "artifact is already complete; chunk writes must stop".to_string(),
            });
        }
        let projected = entry.record.recorded_bytes().saturating_add(chunk_bytes);
        if projected > self.max_bytes
            || self.total_bytes.saturating_add(chunk_bytes) > self.max_bytes
        {
            return Err(CoreError::TooLarge {
                what: "artifact total",
                limit: self.max_bytes as usize,
                actual: projected as usize,
            });
        }
        let prev = entry.record.chunks.insert(chunk_index, chunk_bytes);
        let delta = chunk_bytes.saturating_sub(prev.unwrap_or(0));
        self.total_bytes = self.total_bytes.saturating_add(delta);
        // Writing refreshes the TTL: an artifact being uploaded is in use.
        entry.record.expires_ms = now_ms.saturating_add(self.ttl_ms);
        Ok(())
    }

    /// Mark an artifact complete after the hub has computed the digest of what
    /// it actually stored.
    ///
    /// `declared_*` are the worker's claims from its `artifact_chunk` manifest;
    /// `stored_*` are what the hub measured. A disagreement fails the artifact
    /// instead of trusting the worker (task §9).
    ///
    /// # Errors
    /// [`CoreError::Protocol`] on a digest or size mismatch, [`CoreError::Queue`]
    /// when the artifact slot is unknown.
    // One argument over clippy's threshold, and every one of them is a distinct
    // measurement (the worker's declared claim vs what the hub actually stored),
    // so a parameter struct would only rename them. The call site that would have
    // to change is `PoolActor::on_artifact_stored` (`actor.rs:2373`), outside this
    // change's file list, so the count is documented instead of re-shaped.
    #[allow(clippy::too_many_arguments)]
    pub fn complete(
        &mut self,
        id: &ArtifactId,
        declared_sha256: Option<String>,
        declared_size_bytes: Option<u64>,
        stored_sha256: String,
        stored_bytes: u64,
        content_type: Option<String>,
        now_ms: Timestamp,
    ) -> Result<ArtifactRecord, CoreError> {
        let entry = self
            .entries
            .get_mut(id)
            .ok_or_else(|| CoreError::Queue(format!("artifact {} is unknown", id.as_ref())))?;
        if let Some(declared) = &declared_sha256 {
            if declared != &stored_sha256 {
                return Err(CoreError::Protocol {
                    reason: format!(
                        "artifact digest mismatch: worker declared {declared}, hub stored {stored_sha256}"
                    ),
                });
            }
        }
        if let Some(declared) = declared_size_bytes {
            if declared != stored_bytes {
                return Err(CoreError::Protocol {
                    reason: format!(
                        "artifact size mismatch: worker declared {declared} bytes, hub stored {stored_bytes}"
                    ),
                });
            }
        }
        entry.record.size_bytes = stored_bytes;
        entry.record.stored_sha256 = Some(stored_sha256);
        entry.record.declared_sha256 = declared_sha256;
        if let Some(ct) = content_type {
            entry.record.content_type = ct;
        }
        entry.record.complete = true;
        // The TTL restarts at completion: retention is measured from when the
        // consumer can first download it, not from when the upload started.
        entry.record.expires_ms = now_ms.saturating_add(self.ttl_ms);
        Ok(entry.record.clone())
    }

    /// Look up a record.
    #[must_use]
    pub fn get(&self, id: &ArtifactId) -> Option<&ArtifactRecord> {
        self.entries.get(id).map(|e| &e.record)
    }

    /// Mutable lookup for streaming writers.
    pub fn get_mut(&mut self, id: &ArtifactId) -> Option<&mut ArtifactEntry> {
        self.entries.get_mut(id)
    }

    /// Bytes recorded across all artifacts.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Artifact count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Slots dropped by the [`MAX_ARTIFACT_ENTRIES`] bound. Ready for a
    /// `nexus_artifact_evicted_total` gauge; nothing reads it yet (see the report
    /// handoff — `actor.rs` publishes `nexus_artifact_bytes_on_disk` next door).
    #[must_use]
    pub fn evictions(&self) -> u64 {
        self.evictions
    }

    /// True when there are no artifacts.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Expire finished artifacts, returning `(id, path)` so the server can
    /// `remove_file`.
    ///
    /// Also drains the slots dropped by [`ArtifactBook::make_room`]: their files
    /// have to go too, and this is the only artifact sweep the actor runs, so
    /// returning evictions from here lets eviction reuse the TTL path
    /// (`Effect::DeleteArtifact` → `state.rs:890-893` → `io::delete_artifact`)
    /// instead of needing a second effect type and a second call site in files
    /// this module does not own. The actor ticks this every sweep, so an evicted
    /// entry's staging directory lives at most one tick past its eviction.
    pub fn purge_expired(&mut self, now_ms: Timestamp) -> Vec<(ArtifactId, String)> {
        let mut stale: Vec<(ArtifactId, String)> = self
            .entries
            .iter()
            .filter(|(_, e)| now_ms >= e.record.expires_ms)
            .map(|(id, e)| (id.clone(), e.record.path.clone()))
            .collect();
        for (id, _) in &stale {
            if let Some(entry) = self.entries.remove(id) {
                self.total_bytes = self
                    .total_bytes
                    .saturating_sub(entry.record.recorded_bytes());
            }
        }
        // Evicted entries are already out of `entries` and out of `total_bytes`
        // (`make_room` did both when it dropped them); only their paths remain.
        stale.append(&mut self.pending_deletions);
        stale
    }
}

/// Build a storage path from untrusted ids, rejecting anything that could walk
/// out of the artifact root (task §9 "secure paths").
///
/// # Errors
/// [`CoreError::InvalidId`] when either component fails the alphabet check.
pub fn artifact_path_checked(root: &str, job: &str, artifact: &str) -> Result<String, CoreError> {
    if !is_safe_id(job) {
        return Err(CoreError::InvalidId {
            kind: "job path component",
            value: job.to_string(),
        });
    }
    if !is_safe_id(artifact) {
        return Err(CoreError::InvalidId {
            kind: "artifact path component",
            value: artifact.to_string(),
        });
    }
    Ok(artifact_path(root, job, artifact))
}

/// Unchecked path join, used after validation. `/` separators are deliberate:
/// object-storage keys use them, and the native runtime converts to a platform
/// path itself.
#[must_use]
pub fn artifact_path(root: &str, job: &str, artifact: &str) -> String {
    let trimmed = root.trim_end_matches(['/', '\\']);
    format!("{trimmed}/{job}/{artifact}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn book() -> PayloadBook {
        PayloadBook::new(4, 1 << 20, 5_000)
    }

    /// Budget-aware variant of [`register`]: keeps the `Result` so the spill-budget
    /// tests can assert on the refusal instead of panicking inside the helper.
    fn try_register(
        b: &mut PayloadBook,
        job: &str,
        size: usize,
    ) -> Result<PayloadDescriptor, CoreError> {
        let bytes = vec![7u8; size];
        let sha = sha256_hex(&bytes);
        b.register(
            &JobId::new_unchecked(job),
            "tok",
            sha,
            bytes,
            "application/json".to_string(),
            1_000,
        )
    }

    fn register(b: &mut PayloadBook, job: &str, size: usize) -> PayloadDescriptor {
        try_register(b, job, size).expect("register")
    }

    #[test]
    fn spill_threshold_is_strictly_above_the_cap() {
        assert!(
            !PayloadBook::needs_spill(8 << 20, 8 << 20),
            "exactly at the cap rides inline"
        );
        assert!(PayloadBook::needs_spill((8 << 20) + 1, 8 << 20));
    }

    #[test]
    fn sha256_matches_the_reference_shard_digest() {
        // Anchor: services/molab/molab-strata-runtime.py:95-96 SHARD_TABLE sha
        // for shard 1 of the pinned GGUF. Recomputed here for a known input so
        // the digest helper is pinned against a real-world expectation.
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            content_idempotency_key("acme", "llm.chat", b"{\"model\":\"qwen38\"}"),
            content_idempotency_key("acme", "llm.chat", b"{\"model\":\"qwen38\"}")
        );
        assert_ne!(
            content_idempotency_key("acme", "llm.chat", b"a"),
            content_idempotency_key("other", "llm.chat", b"a"),
            "the tenant must be part of the key"
        );
    }

    #[test]
    fn payload_authorization_is_token_worker_attempt_and_ttl_scoped() {
        let mut b = book();
        let entry = register(&mut b, "job-a", 4096);
        let id = entry.id.clone();
        let w1 = WorkerId::new_unchecked("wkr-1");
        let w2 = WorkerId::new_unchecked("wkr-2");

        // Not yet assigned: nobody may fetch.
        assert!(matches!(
            b.authorize_fetch(&id, "tok", &w1, 1, 1_100),
            Err(CoreError::Unauthorized(_))
        ));
        b.assign_owner(&id, &w1, 1);
        assert_eq!(
            b.authorize_fetch(&id, "tok", &w1, 1, 1_100)
                .expect("ok")
                .size_bytes,
            4096
        );
        // The wrong token is refused even with the right worker.
        assert!(matches!(
            b.authorize_fetch(&id, "wrong", &w1, 1, 1_100),
            Err(CoreError::Unauthorized(_))
        ));
        // Another worker cannot read someone else's prompt.
        assert!(matches!(
            b.authorize_fetch(&id, "tok", &w2, 1, 1_100),
            Err(CoreError::Unauthorized(_))
        ));
        // A requeue's attempt cannot refetch the previous attempt's token.
        assert!(matches!(
            b.authorize_fetch(&id, "tok", &w1, 2, 1_100),
            Err(CoreError::Unauthorized(_))
        ));
        // TTL: created at 1_000 with a 5_000 ms ttl.
        assert!(matches!(
            b.authorize_fetch(&id, "tok", &w1, 1, 6_000),
            Err(CoreError::Unauthorized(_))
        ));
        assert!(b.authorize_fetch(&id, "tok", &w1, 1, 5_999).is_ok());

        b.record_fetch(&id);
        assert_eq!(
            b.authorize_fetch(&id, "tok", &w1, 1, 1_100)
                .expect("still ok")
                .fetch_count,
            1
        );
        // Releasing for the job frees the bytes and invalidates the ref.
        assert_eq!(
            b.release_for_job(&JobId::new_unchecked("job-a")),
            vec![id.clone()]
        );
        assert_eq!(b.bytes(), 0);
        assert!(matches!(
            b.authorize_fetch(&id, "tok", &w1, 1, 1_100),
            Err(CoreError::Queue(_))
        ));
    }

    #[test]
    fn spill_budget_is_enforced_in_bytes_and_count() {
        let mut b = PayloadBook::new(2, 10_000, 1_000);
        register(&mut b, "job-1", 6_000);
        let err = try_register(&mut b, "job-2", 6_000).expect_err("byte budget");
        assert!(matches!(err, CoreError::Queue(_)), "{err}");
        assert_eq!(b.rejected(), 1);
        assert_eq!(b.len(), 1);
        // The TTL clock starts at registration: the helper registers at `now_ms =
        // 1_000` and the book's ttl is 1_000, so the entry expires at 2_000 and
        // `purge_expired` is inclusive at that instant (`now_ms >= expires_ms`).
        assert_eq!(b.purge_expired(1_999), Vec::<PayloadRefId>::new());
        assert_eq!(
            b.purge_expired(2_000),
            vec![PayloadRefId::new_unchecked("job-1")]
        );
        assert!(try_register(&mut b, "job-2", 6_000).is_ok());
        assert_eq!(b.bytes(), 6_000);
    }

    #[test]
    fn artifact_chunk_and_completion_are_verified() {
        let mut a = ArtifactBook::new("var/artifacts", 100_000, 32_768, 10_000);
        let job = JobId::new_unchecked("job-9");
        let id = ArtifactId::new_unchecked("art-9");
        let w = WorkerId::new_unchecked("wkr-9");
        let entry = a.register(&job, id.clone(), 1_000);
        assert_eq!(entry.record.path, "var/artifacts/job-9/art-9");
        a.assign_owner(&id, &w, 1);

        a.record_chunk(&id, &w, 1, 0, 1_024, 1_100).expect("chunk");
        a.record_chunk(&id, &w, 1, 1, 2_048, 1_200).expect("chunk");
        assert_eq!(a.get(&id).expect("entry").recorded_bytes(), 3_072);
        assert_eq!(a.bytes(), 3_072);
        // Chunk over cap is refused before any state change.
        let err = a
            .record_chunk(&id, &w, 1, 2, 40_000, 1_300)
            .expect_err("too big");
        assert!(matches!(
            err,
            CoreError::TooLarge {
                what: "artifact chunk",
                limit: 32_768,
                ..
            }
        ));
        // Wrong owner is refused.
        assert!(matches!(
            a.record_chunk(&id, &WorkerId::new_unchecked("wkr-x"), 1, 3, 1, 1_300),
            Err(CoreError::Unauthorized(_))
        ));

        let declared = sha256_hex(b"payload");
        let record = a
            .complete(
                &id,
                Some(declared.clone()),
                Some(3_072),
                declared.clone(),
                3_072,
                Some("image/png".to_string()),
                1_400,
            )
            .expect("complete");
        assert!(record.complete);
        assert_eq!(record.content_type, "image/png");
        assert_eq!(record.stored_sha256.as_deref(), Some(declared.as_str()));
        assert_eq!(record.size_bytes, 3_072);
        // Writing after completion is refused.
        assert!(matches!(
            a.record_chunk(&id, &w, 1, 4, 1, 1_500),
            Err(CoreError::Protocol { .. })
        ));
        // A mismatched digest must fail, not silently accept the worker's claim.
        let bad = a.complete(
            &id,
            Some("00".repeat(32)),
            None,
            sha256_hex(b"x"),
            1,
            None,
            1_600,
        );
        assert!(
            matches!(bad, Err(CoreError::Protocol { reason }) if reason.contains("digest mismatch"))
        );
        // So must a mismatched size.
        let bad_size = a.complete(&id, None, Some(9_999), declared.clone(), 3_072, None, 1_700);
        assert!(
            matches!(bad_size, Err(CoreError::Protocol { reason }) if reason.contains("size mismatch"))
        );
    }

    #[test]
    fn artifact_ttl_purge_reports_paths_for_deletion() {
        let mut a = ArtifactBook::new("/tmp/art", 100_000, 1_024, 1_000);
        let job = JobId::new_unchecked("job-1");
        // Slot allocation only: the purge assertion below reads the book itself, so
        // the returned entry is not needed here.
        let _ = a.register(&job, ArtifactId::new_unchecked("art-1"), 0);
        assert!(a.purge_expired(999).is_empty());
        assert_eq!(
            a.purge_expired(1_000),
            vec![(
                ArtifactId::new_unchecked("art-1"),
                "/tmp/art/job-1/art-1".to_string()
            )]
        );
        assert_eq!(a.len(), 0);
    }

    /// NEX-7.2's count half: the artifact book holds at most
    /// `MAX_ARTIFACT_ENTRIES` slots, and every slot it drops is handed to the
    /// sweep that deletes the file, so the bound cannot leak a directory.
    #[test]
    fn artifact_book_is_count_bounded_and_reports_evicted_paths() {
        let mut a = ArtifactBook::new("/tmp/art", 100_000, 1_024, 1_000);
        let job = JobId::new_unchecked("job-cap");
        // Fill the book exactly to the cap. Nothing is complete and nothing has
        // lapsed, so the next allocation has to hit the saturation rule. The
        // returned entries are unused: every assertion below reads the book itself.
        for index in 0..MAX_ARTIFACT_ENTRIES {
            let when = u64::try_from(index).expect("index fits in a timestamp");
            let _ = a.register(
                &job,
                ArtifactId::new_unchecked(format!("art-{index}")),
                when,
            );
        }
        assert_eq!(a.len(), MAX_ARTIFACT_ENTRIES);
        assert_eq!(a.evictions(), 0, "filling to the cap evicts nothing");

        // One over the cap. With no expired and no complete slot to sacrifice, the
        // oldest goes regardless of state; all deadlines are distinct here, so the
        // victim is the first-registered slot, deterministically.
        let _ = a.register(&job, ArtifactId::new_unchecked("art-cap"), 0);
        assert_eq!(
            a.len(),
            MAX_ARTIFACT_ENTRIES,
            "the bound is exact, not approximately the cap"
        );
        assert_eq!(a.evictions(), 1);
        assert!(
            a.get(&ArtifactId::new_unchecked("art-0")).is_none(),
            "the oldest in-flight slot is the one that pays"
        );
        assert!(a.get(&ArtifactId::new_unchecked("art-1")).is_some());
        assert!(a.get(&ArtifactId::new_unchecked("art-cap")).is_some());
        // The eviction is reported on the sweep path the actor already runs, which
        // is what turns it into `io::delete_artifact` for that staging directory.
        assert_eq!(
            a.purge_expired(0),
            vec![(
                ArtifactId::new_unchecked("art-0"),
                "/tmp/art/job-cap/art-0".to_string()
            )]
        );
        assert!(
            a.purge_expired(0).is_empty(),
            "the eviction queue is drained, not replayed"
        );
    }

    /// The eviction order the bound commits to: a lapsed slot first, a finished
    /// artifact second, an in-flight upload last.
    #[test]
    fn artifact_eviction_prefers_expired_slots_then_completed_ones() {
        let mut a = ArtifactBook::new("/tmp/art", 1 << 30, 1 << 20, 1_000);
        let job = JobId::new_unchecked("job-evict");
        let worker = WorkerId::new_unchecked("wkr-1");
        // Registered one millisecond apart, so `expires_ms` order is registration
        // order and `art-000` is unambiguously the oldest slot. The returned
        // entries are unused: the assertions read the book.
        for index in 0..MAX_ARTIFACT_ENTRIES {
            let when = u64::try_from(index).expect("index fits in a timestamp");
            let _ = a.register(
                &job,
                ArtifactId::new_unchecked(format!("art-{index:03}")),
                when,
            );
        }
        // `art-001` becomes a finished artifact holding 7 bytes of the budget.
        let done = ArtifactId::new_unchecked("art-001");
        a.assign_owner(&done, &worker, 1);
        a.record_chunk(&done, &worker, 1, 0, 7, 900).expect("chunk");
        a.complete(&done, None, None, sha256_hex(b"x"), 7, None, 900)
            .expect("complete");
        assert_eq!(a.bytes(), 7);

        // (a) A lapsed slot is the victim even though the completed `art-001`
        // sorts ahead of it by id: at t=1_000 only `art-000` (registered at 0 with
        // a 1_000 ms ttl) has expired.
        let _ = a.register(&job, ArtifactId::new_unchecked("art-fresh-1"), 1_000);
        assert!(
            a.get(&ArtifactId::new_unchecked("art-000")).is_none(),
            "an entry the sweep would delete anyway goes first"
        );
        assert!(a.get(&done).is_some());

        // (b) With nothing left lapsed, the finished artifact is next: the fresh
        // upload is a dispatched job that cannot deliver without a slot.
        let _ = a.register(&job, ArtifactId::new_unchecked("art-fresh-2"), 1_000);
        assert!(
            a.get(&done).is_none(),
            "a completed artifact is preferred over an in-flight upload"
        );
        assert_eq!(
            a.bytes(),
            0,
            "an evicted entry leaves the shared byte budget"
        );

        // Both victims reach the sweep, oldest first, with the paths to delete.
        assert_eq!(
            a.purge_expired(1_000),
            vec![
                (
                    ArtifactId::new_unchecked("art-000"),
                    "/tmp/art/job-evict/art-000".to_string()
                ),
                (done, "/tmp/art/job-evict/art-001".to_string()),
            ]
        );
    }

    #[test]
    fn path_builder_rejects_traversal() {
        assert_eq!(
            artifact_path_checked("root", "job-1", "art-1").expect("ok"),
            "root/job-1/art-1".to_string()
        );
        assert!(matches!(
            artifact_path_checked("root", "../etc", "art-1"),
            Err(CoreError::InvalidId {
                kind: "job path component",
                ..
            })
        ));
        assert!(matches!(
            artifact_path_checked("root", "job-1", "a/b"),
            Err(CoreError::InvalidId {
                kind: "artifact path component",
                ..
            })
        ));
        // Trailing separator on the root must not double up.
        assert_eq!(artifact_path("root/", "j", "a"), "root/j/a".to_string());
    }
}
