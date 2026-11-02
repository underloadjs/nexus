# @underload/nexus — outbound-dial inference broker

Rust implementation of [`tasks/task-rust-hub-inference-broker.md`](../../../tasks/task-rust-hub-inference-broker.md):
a hub that inference hosts reach by **dialing out**, so a worker behind NAT, inside a
marimo/molab container, or in a Kaggle notebook can serve consumer requests without
a public URL. The hub is the only public surface; it owns the queue, claims each job
to exactly one worker, relays the worker's stream to the consumer, and propagates
cancellation back.

This package is **standalone** (`[workspace]` declared here on purpose) and contains
no JS/TS application logic — the only JavaScript is generated `wasm-bindgen` glue.

```text
consumer ──POST /v1/chat/completions (SSE)──▶┌───────────────┐◀──one persistent WS──▶ worker A (Strata, 4 slots)
consumer ──POST /v1/jobs (artifact)      ──▶│  nexus hub    │◀──one persistent WS──▶ worker B (ComfyUI, 1 slot)
                                             │ axum + tokio  │
                                             └───────────────┘
                          one actor per pool owns the queue, leases and jobs
```

## Layout

| Path | What it is |
| --- | --- |
| `crates/nexus-core` | The spine: `protocol`, `actor` (pool_actor), `queue` (trait + in-memory), `relay`, `adapters`, `store`, `scheduler`, `auth`, `seq`, `metrics`, `limits`, `config`. Synchronous, IO-free, compiles for native **and** `wasm32-unknown-unknown`. |
| `crates/nexus-server` | Native runtime: axum + tokio, worker WebSocket, consumer SSE, artifact spill to disk, metrics, graceful drain. Not wasm-buildable (tokio IO/time drivers do not exist there). |
| `crates/nexus-server/src/runtime_e2e.rs` | Runtime tests: a real listener, a real worker WebSocket, real SSE/JSON responses (`§16.5`, `§16.6`, `§16.14`, `§9` tenancy, `/metrics` shape). A unit-test module of the bin, because this crate has no library target. Unexecuted, like everything else here. |
| `crates/nexus-wasm` | The **same** `PoolActor` re-exported to Node/browser via `wasm-bindgen`. |
| `crates/nexus-worker-example` | Rust worker client that dials out and fronts a loopback engine (task §12). |
| `examples/node-smoke.mjs` | Node smoke test that drives the WASM actor and asserts broker behaviour end-to-end. |
| `scripts/build-wasm.mjs` | Two-step wasm build (cargo artifact + `wasm-bindgen` glue). |

## Verification status — read this first

**No Rust toolchain exists on the machine this was written on, and none was
installed here.** Re-checked in the review pass: `cargo`, `rustc`, `rustup` and
`wasm-pack` are all absent from `PATH`, and the root `rust-toolchain.toml` pins
`1.99.1`, which is therefore not installed. Consequences, stated plainly:

* **nothing in this package has ever been compiled or executed.** No `cargo check`,
  `test`, `clippy`, `fmt --check`, release build or wasm build has run, and
  `examples/node-smoke.mjs` could not run because it needs the wasm artifact;
* what *was* run is JS-only: `node --check` on `scripts/build-wasm.mjs` and
  `examples/node-smoke.mjs` (both parse), and `package.json` parses;
* the review pass fixed defects found by reading (see *Review pass* and *Second pass*
  below), but a static review cannot prove compilation. Expect the first `cargo check
  --workspace --all-targets` to surface further ordinary type errors, most likely in
  `nexus-server`;
* "covered" in the §16 matrix means *a test asserts it in Rust source*, not *it
  passed*. Core tests exercise the actor with injected time; they are **not**
  runtime tests, and the runtime tests added in the review pass
  (`crates/nexus-server/src/runtime_e2e.rs`, a unit-test module of the bin because
  `nexus-server` has no library target) are likewise unexecuted.

The test inventory, as written (all of it unexecuted): 42 tests in
`crates/nexus-core/tests/acceptance.rs`, 73 inline unit tests across `nexus-core/src`,
and 12 in `nexus-server` — 6 artifact-download tests in `io.rs`, 4 runtime tests in
`runtime_e2e.rs`, 2 in `state.rs`. `crates/nexus-wasm` and
`crates/nexus-worker-example` contain no Rust tests: the wasm crate is exercised through
`examples/node-smoke.mjs`, which cannot run without a wasm build.

To verify for real: `rustup toolchain install`, `rustup target add
wasm32-unknown-unknown`, `cargo install wasm-bindgen-cli --locked`, then
`yarn verify`. `yarn fmt:check` inside `verify` may itself fail: the sources were
hand-written and never passed through `rustfmt`.

No performance claim is made anywhere in this README that has not been measured by
a human running the binary; §14's 30–60 ms is the task's own budget, restated.

## Running it

```bash
# native hub (loopback by default)
export NEXUS_ALLOW_ANONYMOUS=1        # local smoke only; unset = credentials required
yarn start                            # cargo run -p nexus-server --bin nexus-hub

# a worker fronting the reference Strata engine on 127.0.0.1:8090
export NEXUS_POOL_TOKEN=pool-token-a
export NEXUS_WORKER_ID=wkr-molab-1     # stable id: cell re-runs evict the stale socket
yarn worker

# consumer
curl -N localhost:8085/v1/chat/completions \
  -H 'Authorization: Bearer sk-1' -H 'Idempotency-Key: demo-1' \
  -d '{"model":"qwen38","messages":[{"role":"user","content":"hi"}],"stream":true}'
```

### Configuration

Load order: built-in defaults → `NEXUS_CONFIG_JSON` (replaces everything) → individual
`NEXUS_*` overrides. Secrets are never hardcoded: `validate()` **refuses to boot** with
an empty pool-token set, an empty API-key set, or an empty attestation allowlist unless
`NEXUS_ALLOW_ANONYMOUS=1` says otherwise — the "no fail-open auth" requirement.

| Variable | Default | Notes |
| --- | --- | --- |
| `NEXUS_LISTEN` | `127.0.0.1:8085` | Consumer + worker surface. Keep it loopback unless you terminate TLS elsewhere. |
| `NEXUS_POOL_TOKENS` | — | Comma-separated worker tokens; stored as SHA-256 digests. |
| `NEXUS_API_KEYS` | — | `secret=tenant` pairs; a bare secret maps to `default`. |
| `NEXUS_MODEL_SHAS` | — | Attestation allowlist. Seed it from `SHARD_TABLE` in `services/molab/molab-strata-runtime.py:94-99`. |
| `NEXUS_ALLOW_ANONYMOUS` | `0` | Local smoke only. |
| `NEXUS_ARTIFACT_ROOT` | `Temp/nexus/artifacts` | Artifact spill directory (repo-root `Temp/`, per root `AGENTS.md`). |
| `NEXUS_QUEUE_DEPTH` | `128` | Beyond it: `429`, never an unbounded wait. |
| `NEXUS_INLINE_PAYLOAD_CAP_BYTES` | `8 MiB` | Above it the prompt travels by `payload_ref`. |
| `NEXUS_LEASE_MS` / `NEXUS_HEARTBEAT_MS` | `60000` / `20000` | `validate()` requires lease ≥ 2× heartbeat. |
| `NEXUS_RESUME_GRACE_MS` | `1500` | `0` = strict "expire immediately" (see *Open decisions*). |
| `NEXUS_NO_WORKER_GRACE_MS` | `2000` | Pre-body grace; then `503 + Retry-After`. |
| `NEXUS_JOB_HARD_CAP_MS` | `1800000` | Per-job deadline. |
| `NEXUS_MAX_CONTINUATIONS` | `0` | Resume-by-continuation (§7.4) is **opt-in**: `0` leaves a mid-stream death terminal, `1` re-runs it once on another worker. |
| `NEXUS_CONTINUATION_ALLOW_OPAQUE` | `0` | With continuation on, an attempt whose relayed output was **not** reconstructible assistant text (tool-call chunks, or frames interleaved with `event`/`log`/`artifact_chunk`) is still terminal. Set `1` only for a pool whose workers restart cleanly from an arbitrary prefix. |
| `NEXUS_FLUSH_INTERVAL_MS` / `NEXUS_FLUSH_BYTES` | `75` / `1536` | Outbound SSE batching window (task §7.2). |
| `NEXUS_MAX_BODY_BYTES` | `64 MiB` | HTTP request body cap (tower-http `RequestBodyLimitLayer`). |

Every name is handled in `nexus-core/src/config.rs`, and a typo inside
`NEXUS_CONFIG_JSON` is a hard startup error (`deny_unknown_fields`), not a silently
retained default.

## WASM use, and the honest runtime boundary

```bash
yarn build:wasm     # cargo build --release --target wasm32-unknown-unknown -p nexus-wasm
                    # + node scripts/build-wasm.mjs  (wasm-bindgen --target nodejs)
yarn smoke:wasm     # node examples/node-smoke.mjs
```

**What this is not:** a claim that axum's sockets run on `wasm32-unknown-unknown`.
A WASM module has no listener to accept into, and tokio's IO/time drivers do not exist
on that target. So the boundary is explicit:

```text
native : sockets, HTTP/SSE, timers, disk artifacts   →  ActorMsg
wasm   : ActorMsg in, Effect[] out                  →  the JS host performs the I/O
```

**What this is:** the scheduling/ownership brain is shared, not reimplemented. The JS
host supplies the clock (`now_ms` on every call — the module has no clock) and performs
each returned effect: write a WS frame, open an SSE body, answer 503, delete a file.
Admission, the atomic claim, leases, requeue-vs-terminal, seq de-duplication, replay,
flush batching, spill decisions and artifact TTLs are the same compiled Rust the native
server runs, which is why `examples/node-smoke.mjs` asserts lease defaults,
`400 capability_unsatisfied`, `503 no_ready_worker`, capacity limits, verbatim relay and
the shared frame codec.

## Module boundaries (task §13) and how promises are kept

* **`protocol`** — frame vocabulary (`Hello/Ready/Claim/Frame/Done/Cancel/Resume`,
  `Welcome/Assigned/Ack/Reject/ResumeNack/Bye`), integer version negotiation
  (mismatch = clean reject + close), the JSON-lines codec with per-direction size caps,
  and semantic validation of untrusted input (id alphabet, capacity ≥ 1, artifact
  frames must carry a manifest).
* **`actor` (`pool_actor`)** — the only mutator. Claim semantics are defined as
  **authority granted in mailbox order**: `Claim` appends `(worker, slots)` to a FIFO,
  fulfilment consumes the oldest authority first, and a job leaves the queue the moment
  it is leased. Two claims in the same tick therefore have exactly one winner, and the
  winner is a function of message order alone.
* **`queue`** — a trait, with `enqueue/claim/requeue/remove/ordered/purge_expired/drain`
  mapped in the module docs to Redis Streams (`XADD`/`XREADGROUP`/`XAUTOCLAIM`),
  Postgres (`SELECT … FOR UPDATE SKIP LOCKED`) and NATS JetStream (pull, `AckExplicit`,
  ack-wait) without touching the actor. The trait is synchronous and non-blocking on
  purpose: an async backend satisfies it with one round trip per call off the actor
  thread.
* **`relay`** — the neutral consumer event vocabulary, SSE framing primitives, and the
  flush gate. Dialect bodies belong to `adapters`.
* **`adapters`** — OpenAI request parsing (tools/vision/logprobs/structured-output
  inference, prompt token estimate), chunk shapes, delta merge and the non-streaming
  assembly; the generic job API bodies; `/v1/models` and `/healthz`.
* **`store`** — spill decision (`size > inline cap`), in-memory payload book bounded by
  entry count **and** total bytes, artifact book with chunk accounting, TTLs, traversal-
  checked paths and declared-vs-measured digest/size verification.

## Open decisions (task §17), resolved

1. **Wire encoding for M0** — JSON lines, one frame per line, `{"type": …}`
   discriminator; a length-prefixed binary encoding is a later optimization. Chosen for
   debuggability from a notebook cell while the protocol is unstable.
2. **Non-LLM consumer contract** — job API only (`202` + `/events` + `/result` +
   `DELETE`). No synchronous "block until artifact" endpoint: a 400/503 for "no capable
   worker" is worth more than a request that dies at 80 % progress, and blocking would
   hold a socket open for minutes.
3. **Queue depth / admission** — bounded queue, `429` with `Retry-After` when saturated
   (`NEXUS_QUEUE_DEPTH`, per-pool override). No deadline-based waiting beyond
   `claim_deadline_ms`, after which the job is answered `503` (streaming, before body
   bytes) or marked retryable-failed (job API). Chosen because free-tier capacity is
   bursty and a fast "no capacity" beats a silent hang (task §11).
4. **Conversation history** — the hub stores **no** history. It trusts a
   client-supplied `messages` array plus a session hint header (`X-Session-Id`), and uses
   the hint only for rendezvous-hash affinity. Cheaper, and it keeps prompt text out of
   long-lived hub state.
5. **Per-kind timeouts** — `claim_deadline_ms` (wait for dispatch), `job_hard_cap_ms`
   (hard cap), `no_worker_grace_ms` (pre-body grace), all per-kind overrides of global
   defaults. `lease_ms` is renewed only by heartbeats.
6. **Single replica** — yes for the first deployment, so the queue stays in-process.
   The `QueueBackend` trait is the upgrade path; nothing else changes.
7. **Multi-consumer fan-out** — in scope, read-only: several consumers may attach to the
   same job (each gets the live stream, joiners get a replay), but `n>1` sampling (one
   submission executed N times) is **deferred**: it would need a fan-out/selection policy
   the task does not specify.
8. **Worker client location** — in-repo reference is `crates/nexus-worker-example` (Rust,
   because this package keeps application logic in Rust). A per-host client beside the
   engine it fronts (`services/molab/`-style, one Python cell) is the deployment shape for
   notebook hosts; the protocol is identical, and `decode_worker_line`/`encode_hub_frame`
   in the WASM build exist so a JS/Python client can share the Rust caps.

Two documented deviations from a literal reading of the task:

* **Socket close vs `Resume`** (§5.1 vs §4). Both cannot be literal, so: dispatchability
  expires **immediately** (the worker leaves the ready set, counts as zero capacity, gets
  no new work, cannot renew), while **ownership** is retained for `resume_grace_ms`
  (1500 ms) so a `Resume` may re-attach — during which no second owner can appear. At
  grace expiry the §5.1 table is applied (zero-output+idempotent → requeue preferring a
  different worker; zero-output+non-idempotent → fail loudly; bytes delivered → in-band
  terminal, or continuation). `resume_grace_ms = 0` reproduces the strict wording at the
  cost of making `Resume` unreachable. A `Resume` after grace, on a stale attempt, or for
  a moved lease is refused with `ResumeNack` and never applied twice. Stale-connection
  frames are dropped by connection generation, and an evicted socket's close is a no-op.
* **`payload_ref` byte ownership.** Task §10 says the *worker* `GET`s the payload; the hub
  must therefore hold the prompt bytes somewhere. They are held in **memory only**
  (`payload_ttl_ms`, bounded by entry count and total bytes), never on disk, per §9's
  retention rule. If the TTL lapses before dispatch, the attempt is refused with a
  retryable `503`-class terminal (`queue_timeout`) rather than handing the worker a
  reference to bytes that are gone.
* **Resume-by-continuation is opt-in, and it refuses opaque output** (§7.4).
  `max_continuations = 0` by default: a mid-stream death is terminal, because a
  continuation costs a full prefill on a second worker, may show the consumer a seam, and
  §7.4 asks the hub to gate on prompt-cache warmth the hub cannot observe. With
  `NEXUS_MAX_CONTINUATIONS=1` the attempt is re-run elsewhere carrying `start_seq` plus
  the partial text — but only when that text is reconstructible assistant prose. A stream
  that carried tool-call chunks (`"tool_calls"` / `"function_call"`) or interleaved
  `event`/`log`/`artifact_chunk` frames is marked `output_opaque` at relay time
  (`relay_into`) and stays in-band-terminal, with a `Warn` log naming the reason;
  `NEXUS_CONTINUATION_ALLOW_OPAQUE=1` is the explicit opt-in for a pool whose workers
  restart cleanly from an arbitrary prefix. Neither half is automatic: the budget **and**
  the content class both have to be turned on.

## Performance bounds, stated rather than hidden

* Added proxy overhead ≈ **30–60 ms** (dispatch over a live WS 5–20 ms, loopback hop
  ~0.1 ms, delta → consumer 10–30 ms plus the flush window). TTFT is dominated by queue
  wait and prefill, not by the middleman (task §14).
* Outbound SSE is batched per job: flush at **1536 bytes or 75 ms**, whichever first, and
  a timer flush so a small tail never waits for the next token. Per-token frames would be
  ~4× the syscalls and JSON parsing for no perceptible gain.
* Worker in-flight is capped at **declared** capacity (reference Strata `PARALLEL = 4` →
  4; ComfyUI → 1), clamped at 64 against typos, and reconciled against heartbeats so a
  stale `free_slots` cannot over-dispatch.
* Bounded everywhere: mailbox 512, per-worker outbound 256, per-job fan-out
  `consumer_backpressure_frames` (256), replay ring (256 frames / 1 MiB), queue depth
  128, spilled payloads 64 entries / 512 MiB, idempotency map 4096 entries, terminal-job
  retention 300 s, worker fan-in 32 per pool. Two bounds were added in the second pass:
  an artifact download holds at most 5 × 64 KiB between the file and the socket
  (`io::STREAM_CHUNK_BYTES` × `io::STREAM_INFLIGHT_CHUNKS`), and a frame pushed to a
  worker gets 25 ms (`state::SEND_GRACE_MS`) before the connection is declared dead
  rather than the actor parking on it.
* Release profile: `opt-level = 3`, `lto = "fat"`, `codegen-units = 1`,
  `panic = "abort"` (fail-stop for a single-replica broker whose parse paths all return
  `Result`; tests keep `unwind`), `strip = "symbols"`, `debug = "line-tables-only"`.
* Metrics that exist because debugging is otherwise blind (task §13): `nexus_queue_depth`,
  `nexus_ready_workers`, `nexus_loading_workers`, `nexus_leases`, `nexus_ttft_ms`,
  `nexus_dispatch_latency_ms`, `nexus_job_duration_ms`, `nexus_frames_total{kind}`,
  `nexus_frames_duplicate_total`, `nexus_seq_gaps_total`, `nexus_lease_expiries_total{kind}`,
  `nexus_inband_errors_total`, `nexus_artifact_bytes_total`, `nexus_spill_bytes_total`,
  `nexus_tokens_total`, `nexus_cancels_sent_total`, `nexus_resume_accepted_total`.

## Security and retention

* **Distinct credentials per direction**: pool token for workers, API key for consumers,
  never one secret for both. Both are compared by SHA-256 digest with constant-time
  equality; the plaintext is never retained in the registry.
* **Attestation**: a self-declared model name is not identity. A worker must report the
  GGUF blob SHA-256 it loaded and every digest must be on the allowlist; the reference
  `SHARD_TABLE` digests are the intended values, so the hub reuses the identifier that
  already anchors the worker's own download.
* **Scoped, expiring capability tokens** for payload fetch and artifact upload, bound to
  `(job, attempt, worker)`; the book stores only the token digest, and an old attempt
  cannot refetch after a requeue.
* **Size limits**: 1 MiB worker line, 256 KiB frame body, 8 MiB inline payload, 64 MiB
  HTTP body, 4 MiB artifact chunk, 512 MiB per artifact.
* **Retention, explicitly**: prompt text (including spilled `payload_ref` bodies) is
  memory-only, released at job terminal or `payload_ttl_ms` (120 s), whichever first, and
  never written to disk. Deltas live in a bounded ring per job. Artifacts go to disk
  streamed, TTL `artifact_ttl_ms` (600 s), digest and size measured by the hub — a worker's
  declared digest that disagrees with the measurement is surfaced in band, not trusted.
* **Path safety**: job/artifact ids are restricted to `[A-Za-z0-9._-]{1,64}` with no
  leading/trailing dot, and artifact paths are built through `artifact_path_checked`, so a
  `../` cannot escape the root.
* A worker on hardware someone else administers is a worker, not a trusted peer: unknown
  fields are tolerated (forward compatibility) but every value is validated, and a bad
  frame closes the socket rather than desynchronising it.

## §16 acceptance matrix

Two words carry precise meaning in this table:

* **core** — `crates/nexus-core/tests/acceptance.rs` asserts the actor's decision
  with injected time. That file has never been *run*, so "core" means "a test
  exists and states the exact expected value", not "it passed";
* **runtime** — the behaviour needs the native handlers (`crates/nexus-server`)
  and a live socket. Since the review pass these are asserted by
  `crates/nexus-server/src/runtime_e2e.rs` (real listener, real worker WebSocket,
  real SSE bytes) — **also never executed**.

| # | Requirement | Status | Where |
| --- | --- | --- | --- |
| 1 | Two workers, one job: exactly one `Assigned`, the other gets nothing | **core** | `t01_two_workers_one_job_yields_exactly_one_assigned` |
| 2 | Concurrent claims: one winner, deterministically | **core** (mailbox order; both arrival orders asserted) | `t02_concurrent_claims_resolve_deterministically_in_mailbox_order`, `the_actor_never_reports_two_owners_for_one_job` |
| 3 | Worker killed before any byte: requeued, completes elsewhere, no error to the consumer | **core**, with a caveat: a delta that is still inside the flush window counts as "delivered", so a kill in that window takes the in-band-terminal branch instead of the transparent requeue | `t03_worker_killed_before_any_output_is_requeued_and_completes_elsewhere` |
| 4 | Worker killed after bytes: in-band error, one finish, no duplicate deltas | **core** | `t04_worker_killed_after_output_terminates_in_band_with_one_finish` |
| 5 | Consumer disconnect → worker receives `Cancel`, slot freed on ack | **core**; at runtime the decision existed but nothing ever sent `Detach`, so the chat surface could not satisfy this at all. Fixed: both pumps send `Detach` when the body ends, and `runtime_e2e::consumer_disconnect_reaches_the_worker_as_cancel` drives it over a real socket. Ownership is now the **subscription's** flag, not the wire format: `JobRecord::cancel_on_consumer_gone` (copied from the submission) decides in `on_detach`, so a poll-later job survives its `GET /v1/jobs/{id}/events` watcher hanging up | `t05_consumer_disconnect_cancels_the_worker_immediately`, `an_async_job_survives_its_events_watcher_disconnecting` + runtime e2e |
| 6 | Zero ready workers at submit: `503 + Retry-After` before any body bytes | **core**; **runtime asserted** (a connected-but-loading worker, real 503, JSON content type, `Retry-After` present) | `t06_no_ready_worker_answers_503_with_retry_after_before_any_body`, `t06b`, `t06c` + `runtime_e2e::loading_worker_answers_503_with_retry_after_before_any_body` |
| 7 | Duplicate idempotency key while in flight: one execution, second client joins/replays | **core**; the runtime's join path previously **dropped** the replay (the dispatcher's private replay queue was only wired to `/v1/jobs/{id}/events`, never to the OpenAI surface), so the second chat client saw only later deltas. Fixed in `submit`, which installs the queue before the actor is told | `t07_duplicate_idempotency_key_attaches_and_replays_never_reruns` |
| 8 | Reconnecting worker resumes from the last acked `seq`, no re-run, no duplicate deltas | **core** | `t08_reconnecting_worker_resumes_from_the_acked_seq_without_duplicates`, `resume_for_a_moved_or_finished_lease_is_nacked` |
| 9 | Consumer reconnect with `Last-Event-ID`: replay with no gap | **core**; runtime: the job API had the plumbing, the chat surface had none (a chat reconnect always allocates a new job, so `Last-Event-ID` was ignored) — the join case is now covered by row 7's fix, an in-place chat resume is still not implemented | `t09_consumer_reconnect_with_last_event_id_replays_without_a_gap` |
| 10 | Unsatisfiable capability → immediate `400` | **core** | `t10_unsatisfiable_capability_is_an_immediate_400`, `t06c` |
| 11 | Only `ready` workers are dispatched to | **core**, **runtime asserted** (a loading worker's consumer gets the 503 rather than a dispatch) | `t11_a_loading_worker_never_receives_assigned` + runtime e2e |
| 12 | Declared capacity respected (4 and 1 slot) | **core**, **WASM-asserted** (same actor, Node host) | `t12_declared_capacity_is_respected_per_worker`, `t12b_a_one_slot_worker_takes_exactly_one_job` |
| 13 | Off-allowlist SHA cannot join | **core**, **WASM-asserted** (+ version and capability-floor rejects) | `t13_attestation_outside_the_allowlist_cannot_join_the_pool`, `t13b`, `t13c` |
| 14 | Tool-call/reasoning deltas and `usage` survive verbatim | **core**; **runtime asserted** for byte-for-byte relay over real SSE (one chunk in, the same chunk out, one `[DONE]`) | `t14_tool_call_reasoning_and_usage_survive_the_relay_verbatim` + `runtime_e2e::worker_relay_reaches_the_consumer_verbatim_over_sse` |
| 15 | Tens-of-MB artifact: RSS flat, resumable download with valid sha256 | **partly core**: chunk accounting, measured-digest verification, size mismatch and foreign-writer refusal. **Runtime**: the download is a **streamed** range — seek + bounded 64 KiB reads through a 4-deep channel, never a whole-object buffer — with `416` for unsatisfiable ranges, an on-disk length check before the status is committed, and a whole-object digest verification that aborts the body on mismatch. Chunk-cap enforcement and attempt-scoped parts came from the review pass. Asserted by `io::tests` (6 tests, real files under the repo-root `Temp/nexus-tests/` scratch directory, body drained chunk by chunk); **RSS itself has never been measured** | `t15_artifact_upload_is_measured_not_trusted_and_survives_a_resume` + `io::tests::full_object_read_streams_bounded_chunks_and_verifies_the_digest`, `partial_range_streams_only_the_requested_slice`, `unsatisfiable_ranges_answer_416_before_any_byte_is_committed`, `a_recorded_size_beyond_the_stored_object_is_refused`, `a_digest_mismatch_aborts_the_body` |
| 16 | Above-cap payload travels by `payload_ref` and is fetched | **core** (ref descriptor, in-memory bytes, owner/attempt/token/TTL authorization, release on terminal). The review pass fixed the byte accounting: `PayloadBook::register` double-counted every re-dispatch, which both inflated the spill budget and made the entry cap reject an ordinary renewal | `t16_large_payload_travels_by_reference_and_is_fetchable_by_its_owner`, `prompt_retention_lapses_by_ttl_and_terminal_retention_bounds_the_job_map` |
| 17 | `stream: false` returns the same content as streamed | **core** (assembly equals concatenation); runtime: the aggregate path used to subscribe *after* the verdict, so early deltas could be missed — fixed by subscribing before `Submit` | `t17_non_streaming_response_carries_the_same_content_as_the_stream` |
| 18 | Hub restart: clean retryable failure; work resumes with workers reconnecting | **core** for the actor contract. The review pass added the in-band retryable `Error` relay on `Shutdown` and on queue/dispatch timeout: a `Terminal` effect is invisible to the SSE pumps, so before that a draining hub left every open stream hanging | `t18_hub_shutdown_fails_clients_retryably_and_a_fresh_hub_resumes` |

Extra coverage beyond §16: flush batching by size and by age, seq gap reporting, queue
depth `429`, tenant concurrency `429`, payload/prompt `413`, unknown-kind `400`,
stale-connection frames and closes, refused foreign-owner frames, non-idempotent
never-requeue, lease renewal and missed-heartbeat degradation, hard deadlines, warm-model
reporting, keepalive pings, session affinity (and its invalidation on worker death),
resume-by-continuation on/off **and its refusal of opaque tool-call output**, detached
jobs surviving an events-watcher disconnect, and TTL/retention sweeps.

The review passes also added the runtime and I/O coverage the package shipped without:
`nexus-server`'s `runtime_e2e` module (4 tests: real listener, real worker socket, real
SSE), `io.rs`'s download module (6 tests: chunked streaming, exact slices, `416`,
length/digest refusal), and `state.rs`'s two unit tests (label merging, the stalled-
connection report). Every one of them is written against the real types and **none has
been executed**.

## Commands

| Command | What it runs |
| --- | --- |
| `yarn fmt` / `yarn fmt:check` | `cargo fmt --all [-- --check]` |
| `yarn check` | `cargo check --workspace --all-targets` |
| `yarn clippy` | `cargo clippy --workspace --all-targets -- -D warnings` |
| `yarn test` / `yarn test:accept` | core + server tests / the §16 matrix only |
| `yarn build` | release native server (LTO, `panic=abort`) |
| `yarn wasm` / `yarn wasm:glue` / `yarn build:wasm` | wasm artifact, then the Node glue |
| `yarn smoke:wasm` | Node smoke test against the WASM actor |
| `yarn verify` | fmt-check + clippy + tests + release + wasm + smoke |

The repo-root `.cargo/config.toml` still governs this workspace on purpose:
`target-dir = .rust/target`, and `TMP`/`TEMP`/`TMPDIR` point at `Temp/rust` for every
child process cargo spawns, which is the root `AGENTS.md` scratch rule.

## Known gaps — what this build does *not* do

**Blocking, and not fixable from here:**

* **nothing has been compiled** (see *Verification status*). Expect the first
  `cargo check --workspace --all-targets` to surface ordinary type errors, most
  likely in `nexus-server` and in the new `runtime_e2e` module.
* the dependency graph has never been resolved on this machine: no `.rust/target`,
  no vendored crates, no `Cargo.lock`. Offline builds will fail until a registry
  cache exists, and the pinned versions in `Cargo.toml` are stated floors that have
  not been resolved against a real index. The second pass added one dev-dependency,
  `http-body-util` (test-only, for draining a streamed body frame by frame); it has
  the same never-resolved caveat as everything else.

**Real limitations of the current implementation:**

* `scheduler::{eligible, rank, pick, Candidate}` is a complete, unit-tested ranking
  function that **the actor does not call**. Dispatch is pull-driven (§5): a worker
  claims, and `pick_job_for` scans the queue for the first job that worker may take
  (capability, KV, affinity). Consequence: §8's "prefer dedicated workers for
  long-context and latency-sensitive traffic" and "fall back to least-loaded" are
  only partially live — tier preference and ephemeral-as-burst-spill are not applied
  at dispatch, so two claiming workers are served in mailbox order. `affinity_target`
  is the one scheduler function the actor uses.
* the reference worker (`nexus-worker-example`) reads the loopback engine's response
  to end before relaying, so **the example client does not stream**: end-to-end TTFT
  through it equals the engine's full generation time. The hub's own consumer path
  does stream. A production worker parses `data:` blocks incrementally.
* §13's "tokens/s and frames/s **per job**" are not exported: frames and tokens are
  hub-wide counters, and the latency summaries (`ttft/dispatch/duration`) are
  `_sum`/`_count` pairs without per-job series.
* §9's "rate limits per consumer" is a **concurrency cap**, not a request rate: a
  tenant may issue unlimited `POST`s as long as it has fewer than
  `tenant_max_concurrent` jobs in flight, and `queue_depth`/`max_idempotency_entries`
  are the only global backstops.
* §16.3's "worker killed before any byte reaches the consumer" has a window: a delta
  that is accepted but still inside the flush gate sets `bytes_delivered`, so a kill
  in that window answers in-band-terminal rather than requeueing transparently.
  Narrow (≤ `flush_interval_ms`), and the consumer still gets the buffered text plus
  an error, but it is not the literal acceptance.
* the actor loop performs effect delivery inline, so a worker whose socket stops
  draining used to park **its whole pool** indefinitely. The push is now bounded: the
  frame gets 25 ms (`SEND_GRACE_MS`) to enter the worker's 256-deep outbound queue, and
  on expiry the connection is reported to the actor as `ActorMsg::WorkerGone`, which runs
  the ordinary resume-grace / lease-expiry rules. Stated plainly, the cost is: that one
  frame (`Assigned`, `Cancel`) is not delivered, and a worker whose queue has been full
  for 25 ms straight — merely slow, not dead — is treated as gone and its jobs take the
  resume path. Task §8 asks for per-job backpressure; this is still coarser.
* artifact downloads are **streamed** ranges (seek + 64 KiB reads behind a 4-deep
  channel), so a `GET /v1/artifacts/{id}` of a 40 MB range costs the hub a few hundred
  KiB, not 40 MB. Two honest limits: a **partial** read cannot be digest-checked without
  reading the whole object, so `x-nexus-sha256` is the *object* digest and the consumer
  verifies the reassembled bytes itself; and the bound is by construction — RSS during a
  download has still never been measured. Writes are chunked with the cap enforced
  before the disk is touched.
* **ranges are query parameters, not the `Range:` header.** `GET /v1/artifacts/{id}` and
  `GET /v1/jobs/{id}/result` read `?from=&to=` (exclusive end) while *answering* with
  `Accept-Ranges` / `Content-Range` as a proper ranged endpoint would; a client that sends
  a standard `Range: bytes=1000-2000` header still gets the whole object. The worker's
  resumable upload is query-parameter driven too (`?chunk=&complete=`). Adding header
  parsing is a handler-level step (`io::get_artifact`), not a storage change.
* the `/metrics` exposition carries `# TYPE` but no `# HELP` lines, and the actor
  registers no metric help strings.
* **an output frame's `content_type` overwrites the job's payload `content_type`**
  (`relay_into`, `actor.rs:2341`): `JobRecord::content_type` is documented as the
  *payload's* type and is sent to the next attempt as such, so a worker that labels its
  deltas changes what a re-queued attempt is told the prompt was. Left as found — the
  fix needs a separate output-type field, which no test in the suite pins today.
* the eviction asymmetry between the two hosts is deliberate but real: the native
  registry closes the stale socket itself and suppresses `Bye{Evicted}` on the live one
  (`state::send_frame`), while a WASM host **does** receive that frame, because it owns
  its sockets and cannot be reached from inside the actor. A JS host that reconnects on
  every `Bye` will therefore loop; it must compare `Welcome.epoch` first.
* the native runtime does not relay the engine's *prompt-cache* telemetry into
  scheduling; affinity uses the session hint alone.
* multi-replica durability is deliberately **not** implemented (task §18): the `queue`
  trait is the boundary, and a hub restart relies on client retry.
* `n>1` sampling fan-out, KV-replica/cache sharing, autoscaling, a web UI, billing and
  sandboxing claims are out of scope (task §18).

## Review pass — corrected defects

A second, independent read of the whole package against the task found the
following. Each was a defect in code that had already been written; none of them had
been observed by a compiler or a test run, and each fix is local to `packages/underload/nexus`.

Compile-shape (these three would have stopped a build):

* `nexus-worker-example/src/main.rs`: `struct EngineStatus` was defined twice
  (E0428);
* `nexus-wasm/src/lib.rs`: `NexusBroker::hello` held a `&mut` borrow of `self`
  (`actor_mut()`) across another `&mut self` call (`next_conn`) (E0499);
* `nexus-server/src/state.rs`: `status.as_u16()` on `Effect::Reject.status`, which is
  already a `u16`; plus `HelloInput` needed `Default` for `worker_ws`'s
  `unwrap_or_default()`, and two imports (`relay::sse`, `protocol::CancelReason`)
  were dead under `clippy -- -D warnings`.

Correctness against the task:

* **cancellation was not wired at runtime** (`§6`, the task's own "wire this before
  anything optional"): no handler ever sent `ActorMsg::Detach`, so a consumer that
  hung up left the job leased and the worker generating. Both SSE pumps now send
  `Detach` when their body ends, and the aggregate (`stream: false`) path does too;
* **an idempotent join lost its replay** on the OpenAI surface: the actor returns
  `Accepted { attached: true, replayed }` and the dispatcher pushed it into a private
  queue that only `/v1/jobs/{id}/events` had subscribed to (§16.7's second client
  silently got a truncated stream). `submit` now installs the queue and subscribes to
  the fan-out **before** the actor is told, which also closes a lost-first-frames race
  in the same place (§16.14/§17);
* **terminal transitions could hang a stream**: `Shutdown`, queue-timeout and
  dispatch-failure emitted `Terminal`/`Reject` only, and the pumps end on a terminal
  *event*, so a draining or timing-out hub left consumers attached to a stream that
  never closed. All three now relay an in-band `Error` (§7.2's "once bytes are on the
  wire, failures are in band"), and the pre-body `Pending` case still answers a clean
  `503`;
* **continuation could not work**: a re-dispatched attempt restarts `seq` at 1, and
  every frame at or below the acked seq is dropped as a duplicate, so the consumer
  would have received nothing after the seam. `Assigned` now carries `start_seq`, the
  worker example honours it, and §7.4 is **opt-in** (`max_continuations` default `0`,
  was `1`);
* **`X-Accel-Buffering: no` was never set** anywhere, which is §7.2's explicit
  requirement for a stream that intermediaries do not buffer;
* **tenant isolation was absent on job reads**: `GET /v1/jobs/{id}`, `/events`,
  `/result`, `GET /v1/artifacts/{id}` and `DELETE /v1/jobs/{id}` did not check the
  caller's tenant (two of them did not authenticate at all), so any anonymous client
  could read, stream or cancel anyone's job. They now require a key and treat a
  foreign job as `404`; explicit `Idempotency-Key` headers are namespaced per tenant
  so a guessed key cannot attach to another tenant's execution;
* **memory-accounting leaks**: `PayloadBook::register` re-added the same prompt's
  bytes on every dispatch (acceptance test 16 asserts the exact figure and would have
  failed), and `ArtifactBook::register` left the dead attempt's bytes in the shared
  budget;
* **artifact uploads were not attempt-isolated**: the parts directory was shared
  across attempts, so a requeue's digest was measured over the previous attempt's
  leftover chunks. Parts are now per attempt, and an oversized chunk is refused
  before it reaches the disk;
* **`/metrics` was not scrapeable**: labelled counters got a second brace group
  (`nexus_frames_total{kind="delta"}{pool="…"}`) and every gauge
  (`nexus_queue_depth`, `nexus_ready_workers`, `nexus_leases`) was missing from the
  response. Labels are now merged into one set and gauges are exported;
* **eviction did not evict** (task §11): the actor's `Bye{Evicted}` is addressed by
  worker id, which by then pointed at the *new* socket, so the stale socket stayed
  open and the new one was told it had been evicted (a client honouring `Bye`
  reconnects forever). The registry now closes the previous generation explicitly,
  and `send_frame` no longer forwards that frame to the live socket;
* **`send_frame` encoded outbound frames against `WireCaps::default()`** (1 MiB line)
  instead of the configured hub caps, so an inline payload above ~1 MiB failed to
  encode and the dispatch was dropped without a word;
* **a bad `NEXUS_CONFIG_JSON` was silently ignored** (the warning hook was an inert
  no-op) while the README promised a hard startup error; `apply_values` now returns
  the error and `main` refuses to boot on it — the hub will not come up on sample
  pools with no credentials and look configured;
* the worker example's single shared cancellation flag stopped *every* in-flight job
  and was cleared by the next assignment; flags are now per job;
* `examples/node-smoke.mjs` referenced `chunk` outside its block (a
  `ReferenceError` before the first assertion), read frames with the wrong tagging
  (`frame.Assigned` instead of `frame.type === "assigned"`), and asserted a flush
  behaviour the gate does not implement. It has been rewritten against the real
  effect/frame shapes.

## Second pass — the six bounded corrections

A follow-up pass took the six correctness items that the first pass had found but left
open. Same rules: only this package, nothing compiled, every claim below is from reading
code and tests that have never been executed.

1. **The contradictory phase assertion was fixed in the test, not in the actor**
   (`tests/acceptance.rs:1095`, in `heartbeats_renew_the_lease_and_missing_heartbeats_pause_dispatch_only`).
   `JobPhase::Streaming` is set in exactly one place — `PoolActor::relay_into`
   (`actor.rs:2312`), on the first relayed frame — and `JobPhase::Assigned`'s own doc
   says "leased to a worker, waiting for the first frame". Two sibling tests
   (`:393`, `:1239`) already assert `Assigned` for a dispatched, frame-less job, so the
   lone `Streaming` expectation was the defect. Nothing in `nexus-core` was changed to
   chase it.
2. **Artifact result serving is now bounded streaming** (`io::serve_artifact`).
   The file is opened, `stat`ed, seeked to `from` and read in `STREAM_CHUNK_BYTES`
   (64 KiB) pieces through a 4-deep `mpsc` into `Body::from_stream` — the old
   `read_range` helper allocated `to - from` up front, i.e. 40 MB for a 40 MB range.
   Checks before any byte is committed: over-long `to` clamps (RFC 9110 §14.21.1),
   `from > to` and `from >= size` answer `416` with `Content-Range: bytes */size`, a
   stored object shorter than its recorded size answers `500 artifact_incomplete`, and a
   missing file answers `404`. A whole-object read hashes the bytes as they stream and
   aborts the body on a digest mismatch; a partial read cannot be digest-checked without
   reading the whole object, so `x-nexus-sha256` stays the *object* digest — documented
   in the function. Tenant checks were already on both readers (`io::get_artifact`,
   `consumer::job_result`). Six tests in `io::tests` drive real files and read the body
   frame by frame, so "streamed" is asserted by chunk sizes, not by trust.
3. **Opaque output can no longer be auto-continued** (`model::JobRecord::output_opaque`,
   set in `relay_into`; consulted in `resolve_lost_job`). `max_continuations` was already
   `0` by default, but with an operator who turned it on, a tool-call stream would have
   been re-run on `prompt + a half-written JSON argument`. Now a non-`delta` frame or a
   chunk carrying `"tool_calls"` / `"function_call"` marks the attempt un-reconstructible,
   the death stays in-band-terminal, and a `Warn` log says why. The escape hatch is
   `NEXUS_CONTINUATION_ALLOW_OPAQUE` / `Limits::continuation_allows_opaque_output`
   (default `false`). Tests: `opaque_tool_output_is_not_continued_even_when_continuation_is_on`,
   plus the new default in `limits::tests::defaults_match_the_task_numbers`.
4. **`cancel_on_consumer_gone` stopped being a dead field**
   (`JobRecord::cancel_on_consumer_gone`, honoured in `on_detach`). The actor used to key
   last-watcher cancellation off `streaming`, i.e. off the wire format: an async job
   submitted through `POST /v1/jobs` with a streaming kind was cancelled the moment a
   transient `GET /v1/jobs/{id}/events` watcher hung up, which is the opposite of
   "poll-later". The flag now travels from the submission onto the record and is the only
   thing `on_detach` consults. Idempotency fan-out is untouched: joins still attach
   consumers and replay. Test: `an_async_job_survives_its_events_watcher_disconnecting`.
5. **The actor loop can no longer park on a stalled worker**
   (`state::send_frame`, `state::report_dead_connection`). The push to a worker's
   outbound queue gets `SEND_GRACE_MS` (25 ms); on expiry — or on a closed queue — the
   connection is reported to the actor as `ActorMsg::WorkerGone{worker, conn}` with the
   generation the actor believes is live, so `on_worker_gone` runs the existing resume
   grace / lease rules. The frame for that one effect is still lost (honest: the peer is
   not draining), but it is never *silently* lost, and the pool stops paying for it.
   Test: `state::tests::a_stalled_worker_queue_is_reported_instead_of_parking_the_actor_loop`
   (no actor task, so the mailbox read cannot race).
6. **The runtime tests are actually in the build, and the spill they write is not
   litter.** `main.rs:3-4` holds `#[cfg(test)] mod runtime_e2e;`, which is the only way a
   binary-only crate gets `cargo test -p nexus-server` coverage; the module is 4 tests
   over a real listener. Each test now gets its own artifact root under the **repo-root**
   scratch directory (`Temp/nexus-tests/e2e-<test>`, via `io::test_scratch`, resolved from
   `CARGO_MANIFEST_DIR` so it does not depend on cargo's working directory), cleared before
   the run and after it (`TestHub::drop`), so parallel tests cannot collide on per-attempt
   part files, nothing disposable lands in the package or in OS temp (root `AGENTS.md`
   rule), and the hub's own configured artifact paths are unchanged.
   `.gitignore` also gained `Temp/`, because the hub's built-in default artifact root is
   CWD-relative and a `yarn start` from inside this package would otherwise dirty the
   nested repository.
