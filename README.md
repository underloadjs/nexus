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
consumer ──GET /v1/consumer (WS)          ──▶│  nexus hub    │◀──one persistent WS──▶ worker B (ComfyUI, 1 slot)
consumer ──POST /v1/jobs (artifact)       ──▶│ axum + tokio  │
                                             └───────────────┘
                          one actor per pool owns the queue, leases and jobs
```

## Layout

| Path | What it is |
| --- | --- |
| `crates/nexus-core` | The spine: `protocol`, `actor` (pool_actor), `queue` (trait + in-memory), `relay`, `adapters`, `store`, `scheduler`, `auth`, `seq`, `metrics`, `limits`, `config`. Synchronous, IO-free, compiles for native **and** `wasm32-unknown-unknown`. |
| `crates/nexus-server` | Native runtime: axum + tokio, worker WebSocket, **consumer WebSocket (`/v1/consumer`)** and consumer SSE, artifact spill to disk, metrics, graceful drain. Not wasm-buildable (tokio IO/time drivers do not exist there). |
| `crates/nexus-server/src/observe.rs` | `GET /v1/observe`: the one read-only document the monitoring UI is built against — readiness, pools, workers, active jobs with leases, live consumer sockets, unresponsive pools. Open by the same rule as `/healthz`, always `200`, and the only route that *relies* on CORS — the layer itself wraps every route. `crates/nexus-server/src/main.rs` (`observe_cors`) is the policy. |
| `crates/nexus-server/src/runtime_e2e.rs` | Runtime tests: a real listener, a real worker WebSocket, real SSE/JSON responses and real consumer-WebSocket frames (`§16.5`, `§16.6`, `§16.7`, `§16.14`, `§9` tenancy, `/metrics` shape), plus the observation plane end to end (the `/v1/observe` contract, and a cross-origin preflight + read). A unit-test module of the bin, because this crate has no library target. These **run and pass** — all 21, against real loopback sockets, real WebSocket handshakes and real SSE bytes; see *Verification status*. |
| `crates/nexus-wasm` | The **same** `PoolActor` re-exported to Node/browser via `wasm-bindgen`. |
| `crates/nexus-worker-example` | Rust worker client that dials out and fronts a loopback engine (task §12). |
| `examples/node-smoke.mjs` | Node smoke test that drives the WASM actor and asserts broker behaviour end-to-end. |
| `scripts/build-wasm.mjs` | Two-step wasm build (cargo artifact + `wasm-bindgen` glue). |

## Verification status — read this first

**Compiled and executed on this host, on the pinned toolchain, 2026-11-10.** What is
actually installed: active toolchain `1.99.1` (the pin in the root `rust-toolchain.toml`),
`rustc 1.99.1`, `cargo 1.99.1`, with `rustfmt` and `clippy` as declared components and
`wasm32-unknown-unknown` as a declared target. **`wasm-bindgen` / `wasm-bindgen-cli` /
`wasm-pack` / `wasm-opt` remain absent from `PATH`**, and that is still the only toolchain
gap — it is why the last two rows below are blocked rather than green. (The 2026-11-03
row set, run on Windows/MSVC with the same pinned toolchain, is superseded by this one;
the environment notes under the table describe that earlier pass.)

Everything below was run from `packages/underload/nexus` with the repo-root
`.cargo/config.toml` in force (`target-dir = .rust/target`). Exit codes are the
command's own. The whole set also reproduces from a **pristine copy with a fresh target
directory and `--locked`**, so nothing here depends on an incremental build.

| Command | Result |
| --- | --- |
| `cargo fmt --all -- --check` | **clean** (exit 0) |
| `cargo check --workspace --all-targets` | **ok**, 0 errors, 0 warnings |
| `cargo test --workspace` | **exit 0 — 177 passed / 0 failed**: `nexus-core` lib **86/86**, `crates/nexus-core/tests/acceptance.rs` **48/48**, `nexus-server` bin **43/43** (21 `runtime_e2e` + 10 `observe::tests` + 7 `io::tests` + 5 `state::tests`), `nexus-wasm` 0 tests, `nexus-worker-example` 0 tests, doc-tests 0 |
| `cargo clippy --workspace --all-targets` | **exit 0**, every crate linted |
| `cargo clippy --workspace --all-targets -- -D warnings` (what `yarn clippy` runs) | **exit 0 — zero findings**. The 17 pre-existing `nexus-core` lints this row used to record are gone, so `yarn verify` no longer aborts before `yarn test` |
| `cargo build --release -p nexus-server` | **ok** → `.rust/target/release/nexus-hub`, 3 403 096 bytes (3.25 MiB), a stripped ELF executable (fat LTO, `panic = abort`, symbols stripped) |
| `cargo check -p nexus-wasm --target wasm32-unknown-unknown` | **ok** — the spine still compiles for the wasm target |
| `node --check scripts/build-wasm.mjs`, `node --check examples/node-smoke.mjs` | **ok** — syntax only; the two scripts are unchanged in this pass |
| `node scripts/build-wasm.mjs` | **blocked**: exits 1 — `wasm-bindgen is not on PATH … cargo install wasm-bindgen-cli --locked --version 0.2.130` (the version is read from `Cargo.lock`) |
| `node examples/node-smoke.mjs` | **blocked**: exits 2 — `[smoke] missing …\pkg\nexus_wasm.js`, because the glue step above cannot run, so its assertion blocks have still never executed |

Two environment notes, because they will bite the next person:

* **cargo cannot open a direct connection to `index.crates.io` on this host** — every
  fetch dies with `curl error 28, "Connection timed out"` (or "transfer too slow:
  transferred 0 bytes"), while `curl.exe`, PowerShell and Node reach the same URL in
  ~200 ms. Builds in this pass ran with `HTTPS_PROXY=http://127.0.0.1:18080` pointing
  at a throwaway loopback CONNECT proxy (`Temp/opencode/proxy.mjs`, IPv4-pinned
  upstream). Nothing about the package requires it; the per-session env is the fix.
* the registry cache was empty, so `Cargo.lock` is **new in this pass** (the repo-root
  `.gitignore` says Cargo.lock is committed on purpose). Nothing else was added to the
  dependency graph.

The suite is green, so "covered" means *a test asserts it and that test passed on this
host*. What is still unproven is listed in *Known gaps* rather than averaged away: the
wasm glue and the Node smoke test that consumes it (a missing tool, not a code gap), and
every §14 latency number (nothing has been benchmarked).

The compile pass itself fixed roughly forty defects that a read-only review cannot
catch. The classes, so the shape of the work is visible without re-reading the diff:

* **vocabulary.** `PoolId::parse` rejected `llm.chat/qwen38` because the id alphabet
  had no `/`. Pool ids are validated per segment now (`util::is_safe_pool_id` +
  `ids.rs`); with the strict rule in force every `Hello` came back as
  `pool_unavailable` and nothing anywhere dispatched.
* **traits that were never derived.** `Effect` and `LogLevel` gained `Serialize` (the
  WASM host is handed `Effect[]`), `JobView` gained `PartialEq` (`Effect::Snapshot`
  compares), `TerminalReason` gained `Ord` (the §16 tests sort tuples of it), and
  `limits.rs` was missing its `serde` import, which took `BrokerConfig` down with it.
* **object safety across the async boundary.** `QueueBackend: Send + Sync` —
  `Box<dyn QueueBackend>` inside `PoolActor` made `run_actor`'s future `!Send`, and
  `tokio::spawn` refused the only actor loop the hub has.
* **one real scheduling defect.** `PoolActor::fulfil_claims` parked a claim's
  remaining slots in a side list that was merged back only *after* the loop, so the
  next `pop_front` saw an empty mailbox and broke: a 4-slot worker with four queued
  jobs took exactly one lease (§16 test 12). The remainder is re-queued into the live
  mailbox, still behind the other claimants.
* **dependency API drift.** `axum 0.8` (`Message::Text`/`Utf8Bytes`, no
  `header::LAST_EVENT_ID`), `tungstenite 0.26` (`Message::Frame` in the match),
  `Result` is not an iterator (`timeout(..).ok().ok().flatten()`), a `MutexGuard`
  temporary outliving the `JobRelay` it borrowed (a tail-position `match` without `;`),
  an orphan doc comment at the end of `worker_ws.rs`, and `JsonLines::push`'s nested
  `split_last` whose arms disagreed on `&[u8]` vs `[u8]`.
* **tests that encoded arithmetic the code never had.** The metrics `# HELP` line, a
  13-vs-14-byte delta length, a replay ring sized below its own 1 KiB floor, a payload
  TTL boundary read from the wrong origin, an api key naming an undeclared tenant,
  `Claim`s stamped with another socket's connection id, ten `drain(..)` calls that were
  never awaited, a tenant-concurrency cap that contradicted the test it lived in, and a
  WASM smoke config with no pools, kinds or tenants — which
  `BrokerConfig::from_json_str` refuses, so the smoke would have died in the
  constructor.
* **`FlushGate::force` was a no-op**: it moved a timestamp that `should_flush` never
  read. A `forced` flag now carries the "a terminal or cancelled job never holds bytes
  behind a batching window" rule the tests document.
* **the runtime test hub now runs with `allow_anonymous = false`**, which is what makes
  `job_reads_require_an_api_key_and_operational_routes_do_not` mean anything; every
  request in that file already carries `API_KEY` or `?token=`.

### Fixed in this pass — all fourteen failures, with their real causes

The previous draft listed fourteen failing tests and left several of them as open
semantic questions. Every one is now resolved, and the diagnosis in a few rows
corrects what this README previously guessed. Read it as: *which side was wrong*, and
*what the rule now is*.

| Test | Which side was wrong | Resolution |
| --- | --- | --- |
| `t02_concurrent_claims_resolve_deterministically_in_mailbox_order` | test | Each `Claim` now carries the connection it arrived on. The `("w-b","w-a")` permutation had stamped `w-b`'s claim with `w-a`'s generation, so the actor correctly dropped it as a stale frame and the "lost dispatch" was the §11 rule working. |
| `t03_worker_killed_before_any_output_is_requeued_and_completes_elsewhere` | test | The expected length was `2 400`; the two attempt-2 deltas are 1 800 + 1 200 chars. The test now asserts the delta **texts** verbatim as well as the 3 000-char total, so "one coherent stream" is pinned by content and not only by length. |
| `t09_consumer_reconnect_with_last_event_id_replays_without_a_gap` | test | §7.2's age flush is a timer, so the third delta leaves on the next `Tick` (the same rule `deltas_are_batched_by_size_and_age_not_per_token` pins). The test now advances the clock one flush interval before asserting the three live deltas, and the attach/replay assertions keep their original values — the ring held all three frames the whole time. |
| `t12_declared_capacity_is_respected_per_worker` | test fixture | Five jobs have to be admitted for the fifth to stay queued; the shared fixture's 4-job tenant cap answered the fifth `Submit` with `429`. The test raises its own cap (the tenant-limit test keeps the shared number). |
| `t15_artifact_upload_is_measured_not_trusted_and_survives_a_resume` | test fixture | The ComfyUI worker's `Hello` named `llm.chat/qwen38`, a pool this actor does not own, so it joined nothing, the pool had zero capacity, and the image job was refused `400 capability_unsatisfied` before anything was dispatched. It now declares `image.render/comfyui`. |
| `t15` (digest-mismatch half) | test sequencing | The mismatched declaration was uploaded **after** the artifact had completed, and `ArtifactBook::record_chunk` refuses chunk writes to a completed artifact, so the mismatch path was never reached. It is now the first completion attempt: the refusal is surfaced in band, the slot stays open (`complete` is still false), and the honest retry completes normally. |
| `t15` (foreign-writer half) | test | `w-other` had no live socket at all, so the upload was refused `401 stale_connection` by the §11 generation rule one step before the ownership rule under test. `w-other` now connects, and the refusal is the `409 stale_attempt` the row is about. |
| `session_affinity_keeps_a_conversation_on_one_worker` | test, twice | Only three of the six turns were ever dispatched, and two of the six were refused outright: every turn stayed in flight, so the fourth filled the rendezvous target's 4 slots and turns five and six hit the shared fixture's 4-job tenant cap (`429 tenant_limit`) — neither of which is what the row tests. A conversation is also sequential, so each turn is now answered with `Done` before the next is submitted. And `seen` only collected the `Submit` effects, which missed the turn the first `Claim` dispatched (no authority exists at that instant); all three messages are counted now, and each turn asserts exactly one dispatch of its own job at attempt 1. |
| `stale_connection_frames_and_closes_are_ignored` | **implementation** | An evicting `Hello` replaced the `WorkerRecord` wholesale: `in_flight` (the actor's own slot accounting) and the engine state vanished, the held leases kept the retired `epoch`, heartbeat-driven lease renewal silently stopped renewing them, and the resume-grace timer `orphan_worker` had armed then deleted a **live** worker at `now + resume_grace_ms`. `on_hello` re-attaches in place now: only the replaced socket's claim authority is voided, the leases move to the new generation, the orphan timer is cancelled, and the engine state carries over only when the previous socket was still live (after a death, readiness must be re-declared — §8). |
| `resume_for_a_moved_or_finished_lease_is_nacked` | test | The stale `Resume` was sent on the connection the hub had already retired, and stale frames are dropped without a reply. The worker now reconnects first and the nack lands on the live generation, which is the race the row is about. |
| `heartbeats_renew_the_lease_and_missing_heartbeats_pause_dispatch_only` | test, twice | The lease deadline is granted at dispatch (`10 + lease_ms` = 60 010); the expected 60 001 anchored it on the worker's last heartbeat, which would shorten a lease by however long the worker had been idle — and the same test's renewal assertion already agreed with the dispatch anchor. Separately, 3 × `heartbeat_ms` equals `lease_ms` at the reference 20 s cadence, so "dispatch paused, still leased" and "lease expired" land on the same tick and the state is unobservable: the test now runs a 5 s cadence, which `Limits::validate`'s `lease_ms >= 2 × heartbeat_ms` rule accepts. §5.1's state is now asserted rather than deferred. |
| `hard_deadline_terminates_a_stalled_job_and_cancels_the_worker` | test | `job_hard_cap_ms` is measured from accept (10 + 5 000 = 5 010), the same origin `Limits` documents for the queue and grace windows. The deadline `Cancel` + `DeadlineExceeded` terminal fire on the tick at 5 010 exactly as asserted, and the 60 s lease still does not double-fire. |
| `prompt_retention_lapses_by_ttl_and_terminal_retention_bounds_the_job_map` | **implementation** + test timing | A requeued job with no *ready* worker was parked as `Pending`, and `Pending` is the phase §7.2 still allows a `503 + Retry-After` for — so a job whose SSE body had already been opened came out of the queue-timeout path with an HTTP status nobody could deliver, ending the stream with no explanation. A requeue is now always `Queued`. Two timing expectations also moved onto the events that actually carry them: the dispatch failure lands on the `Ready` heartbeat that drains the worker's retained claim authority (exactly what `t11` asserts), and retention is measured from the terminal (20 + 30 000), so the sweep is asserted at the 30 020 boundary. The test now also pins the in-band retryable `Error` and that the later `Claim` does not re-fail the job. |
| `io::tests::full_object_read_streams_bounded_chunks_and_verifies_the_digest` | **implementation** — and the earlier diagnosis here was wrong | This README blamed "`sha2 0.10.9` incremental hashing" and advised re-testing on another host. The streaming task was feeding the *already-final* digest into `store::sha256_hex`, which digests its input, so it compared `sha256(sha256(body))` against the recorded `sha256(body)` and aborted **every** correct whole-object download (the mismatch test passed only because any disagreement aborts). It now hex-encodes the 32 digest bytes; `Get-FileHash`, `store::sha256_hex` and the streaming chain now all agree, and the mismatch-aborts test still proves the check bites. No dependency fault, no host caveat. |
| `runtime_e2e::loading_worker_answers_503_with_retry_after_before_any_body` | **implementation** (runtime) | The actor emits `Accepted` for a job that is still `Pending` inside the no-worker grace, and the dispatcher resolved the first-answer gate on it, so the handler wrote SSE headers and answered `200` before the grace could expire. The gate now stays unresolved while the job is `Pending`, and the `Assigned` frame is what opens the body — which is §7.2's "the 503 is still free until the first byte", with the core's `t06` unchanged. |
| `runtime_e2e::worker_relay_reaches_the_consumer_verbatim_over_sse` | test | An OpenAI stream is **three** chunks plus the sentinel: the role delta, the relayed frame, and the finish chunk carrying `usage`. The expected 2 had forgotten the terminal chunk. The count is kept and every chunk is now also asserted field by field (role, the half-written tool argument, `chatcmpl-x`, `finish_reason`, `usage`, the `id:` replay cursors). |
| `runtime_e2e::consumer_disconnect_reaches_the_worker_as_cancel` | **implementation** (runtime) | `pump_sse` drains the submission's private replay queue before it starts watching the fan-out, and for a fresh (non-attaching) submission nothing ever closed that queue: the pump sat in a receive loop that would not end until the job reached a terminal, so deltas were withheld until then **and** a hung-up consumer was never noticed, so §6's `Detach` → `Cancel` never fired. A non-attached `Accepted` now closes the queue, the same way the `Reject` path already did. |

Outstanding work, in order: install `wasm-bindgen-cli` at the locked version
(`cargo install wasm-bindgen-cli --locked --version 0.2.130`) so `yarn wasm:glue`,
`yarn build:wasm` and `yarn smoke:wasm` can run — the wasm crate compiles for the target
and the artifact itself builds; only the glue step and the Node smoke on top of it are
missing a tool; and take §14's latency numbers by running the hub, which nobody has done
yet. The `clippy -D warnings` step that used to sit between those two is green and no
longer blocks `yarn verify`.

No performance claim is made anywhere in this README that has not been measured by
a human running the binary; §14's 30–60 ms is the task's own budget, restated, and
**no measurement of it has been taken** (the hub binary exists now, so `yarn start` +
`yarn worker` can produce one).

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

### Routes

Everything the hub listens for, as registered in `crates/nexus-server/src/main.rs:117-160`.
The frame vocabulary and the body shapes are the specification's §7
(`documents/specifications/hub-specification.md`).

| Route | Who | Notes |
| --- | --- | --- |
| `GET /v1/worker` (WS) | supplier | The one dial-out socket: `Hello`/`Ready`/`Claim`/`Frame`/`Done`/`Resume` in, `Welcome`/`Assigned`/`Ack`/`Cancel`/`Reject`/`ResumeNack`/`Bye` out. Pool token. |
| **`GET /v1/consumer` (WS)** | consumer | **New.** NEX-3's face: `submit`/`attach`/`detach`/`cancel` as JSON text messages, and `accepted`/`rejected` then tagged `meta`/`delta`/`event`/`log`/`artifact`/`finish`/`error`/`gap`/`replay` plus exactly one `end {job, reason}` per task out — several tasks multiplexed on one socket. The API key is checked **before** the upgrade: a bad key is a plain `401` with `www-authenticate: Bearer realm="nexus"`, never a socket that opens and then closes. |
| `POST /v1/chat/completions`, `POST /v1/completions` | consumer | OpenAI-shaped; the SSE response body **is** the stream, on the connection it was posted on. Unchanged, and no `detach` field: the response body is the subscription. |
| `POST /v1/jobs` | consumer | **Changed, breaking.** `202` + `Location`. The body must now carry `"detach": true`; absent or `false` is `400 detach_required` (`consumer.rs:514-522`), because this route answers and hangs up, so it cannot honour the coupled contract. |
| **`GET /v1/jobs`** | consumer | **New.** Lists the jobs this hub currently holds a live relay for, filtered to the calling tenant. |
| **`GET /v1/jobs/{id}`** | consumer | **New.** One job's status, tenant-checked (a foreign job is `404`). |
| `GET /v1/jobs/{id}/events` (SSE), `GET /v1/jobs/{id}/result` | consumer | Follow a detached job's stream with `Last-Event-ID` replay; fetch its result (`?from=&to=` ranges). |
| `DELETE /v1/jobs/{id}` | consumer | Explicit cancel; the record keeps its normal terminal retention, unlike a lost connection. |
| `GET /v1/models`, `GET /healthz`, `GET /metrics` | consumer / ops | Model list, health, Prometheus exposition. The last two are open on purpose: a load balancer has no API key. |
| **`GET /v1/observe`** | consumer / ops / **browser** | **New.** One read-only document for the monitoring UI: readiness, one row per pool, worker liveness and load, the active jobs with their leases, and the live consumer sockets with the jobs each is attached to. Open like `/healthz`, and **always `200`** while the hub is serving. The only route that *relies* on CORS (the layer wraps every route). See the section below. |
| `GET /v1/payloads/{id}`, `GET\|PUT /v1/artifacts/{id}` | supplier / both | Bulk payload fetch, and artifact upload/download with query-parameter ranges and a measured digest. |

### `GET /v1/observe` — the monitoring plane

`crates/nexus-server/src/observe.rs`. One request answers every question a dashboard
asks, so a static bundle hosted on GitHub Pages does not have to combine `/healthz`
(counts only), `/metrics` (a text exposition the browser must parse) and one
`GET /v1/jobs/{id}` probe per job.

```jsonc
GET /v1/observe            // no Authorization header
{
  "protocol": "nexus-hub-observe/v1",
  "now_ms": 1762760000000,                 // epoch ms, hub clock
  "readiness": { "status": "ok", "ready_workers": 1, "loading_workers": 0,
                 "queue_depth": 0, "mailbox_pending": 0, "warm_models": ["qwen38"],
                 "capacity_signal": "warm", "connected_workers": 1, "active_jobs": 1,
                 "pool_mailbox_depth": [ … ] },   // field names mirror `/healthz`
  "pools":  [ { "id": "llm.chat/qwen38", "queue_depth": 0, "ready_workers": 1,
                "loading_workers": 0, "connected_workers": 1, "leases": 1,
                "mailbox_pending": 0 } ],
  "workers":[ { "id": "w-…", "pools": ["llm.chat/qwen38"], "link": "live",
                "engine": "ready", "epoch": 1, "capacity_slots": 4, "free_slots": 2,
                "free_kv_tokens": 8192, "in_flight": ["job-…"], "outstanding_claims": 3,
                "last_seen_ms": 1762759999000, "telemetry": null,
                "resumable": false, "available_slots": 3 } ],
  "jobs":   [ { "id": "job-…", "pool": "llm.chat/qwen38", "tenant": "default",
                "phase": "assigned", "running": true, "created_ms": 1762759990000,
                "consumers": 2,
                "lease": { "worker": "w-…", "epoch": 1, "attempt": 1,
                           "deadline_ms": 1762760005000 } } ],   // null while queued
  "leases": [ { "job": "job-…", "worker": "w-…", "state": "leased", … } ],
  "consumer_connections": [ { "id": "c-…", "tenant": "default",
                              "since_ms": 1762759980000, "peer": null,
                              "attached_jobs": ["job-…"] } ],
  "unresponsive_pools": []
}
```

Rules worth stating, because a dashboard is written against them:

* **Open, and always `200`.** No key, same argument as `/healthz` (`main.rs`'s
  "operations: open on purpose" group). Unlike `/healthz`, zero ready workers is a
  field (`readiness.status: "degraded"`), never a `503`: a monitor should treat only a
  transport failure as downtime.
* **The actor is asked, not the mirror.** Workers, leases and job phases have exactly
  one owner (`nexus-core/src/actor.rs`), so `Hub::pool_snapshot` sends one
  `ActorMsg::Observe` per pool and reads the answer off the same mailbox that mutates
  the state — the identical serialized read `GET /v1/jobs/{id}` already relies on. The
  mirrored `PoolGauges` appear only where the actor could not answer (per-pool mailbox
  depth) or where the actor is missing.
* **A wedged pool cannot hang the dashboard.** The fan-out is concurrent and each
  probe is bounded by the same 2 s budget every read-through probe uses
  (`state.rs`, `PROBE_TIMEOUT_MS`). A pool that does not answer keeps its mirrored
  gauges in `pools[]` and is named in `unresponsive_pools`; the response is still
  `200`. `None` never means "empty pool".
* **Clocks are translated, not mixed.** The actor's clock is process-relative
  (`std::time::Instant`), so `last_seen_ms`, `created_ms` and a lease `deadline_ms`
  are translated to epoch ms by one function (`state::actor_to_wall_ms`), with signed
  arithmetic so a deadline in the future stays in the future.
* **`leases` is null while a job is queued**, `telemetry` is arbitrary JSON or `null`,
  and `jobs[].phase` is the actor's own vocabulary (`pending|queued|assigned|streaming`
  for active work) rather than a single `"running"` — the queued-vs-leased distinction
  is the whole point of the view. `jobs[].running` is the boolean for a client that
  only wants "in flight". Extra fields are additive; renaming or dropping a documented
  one is breaking.
* **CORS** (`main.rs`, `observe_cors`): any origin, `GET`/`HEAD`/`OPTIONS`, any request
  headers, **no credentials**, preflight cached 600 s. `Allow-Origin: *` is legal only
  because nothing is sent that could identify the caller — a cross-site read cannot be
  forged into a mutation. The layer is outermost, so a preflight is answered without
  reaching the router. It wraps every route; see the privacy note below.
* **Consumer sockets are hub state, not actor state.** The actor keys its consumer sets
  by a per-(connection, task) id and cannot answer "which sockets are up", so
  `consumer_ws` registers one record per accepted socket in the hub (`Hub::
  consumer_connections`) and updates it on attach, on end-of-stream, and on close via
  a `Drop` guard — which is what makes an abnormal disconnect disappear from this
  document instead of lingering as a live connection.

### Configuration

Load order: built-in defaults → `NEXUS_CONFIG_JSON` (replaces everything) → individual
`NEXUS_*` overrides. Secrets are never hardcoded: `validate()` **refuses to boot** with
an empty pool-token set, an empty API-key set, or an empty attestation allowlist unless
`NEXUS_ALLOW_ANONYMOUS=1` says otherwise — the "no fail-open auth" requirement.

| Variable | Default | Notes |
| --- | --- | --- |
| `NEXUS_LISTEN` | `127.0.0.1:8085` | Consumer + worker surface. Keep it loopback unless you terminate TLS elsewhere. |
| `NEXUS_POOL_TOKENS` | — | Comma-separated worker tokens; stored as SHA-256 digests. |
| `NEXUS_API_KEYS` | — | `secret=tenant` pairs; a bare secret maps to `default`. The tenant must also exist in the config's `tenants` map (only `default` ships in the built-in defaults): `validate()` refuses a key that names an undeclared tenant, so a multi-tenant deployment supplies `NEXUS_CONFIG_JSON`. |
| `NEXUS_MODEL_SHAS` | — | Attestation allowlist. Seed it from `SHARD_TABLE` in `services/molab/molab-strata-runtime.py:94-99`. |
| `NEXUS_ALLOW_ANONYMOUS` | `0` | Local smoke only. |
| `NEXUS_ARTIFACT_ROOT` | `Temp/nexus/artifacts` | Artifact spill directory (repo-root `Temp/`, per root `AGENTS.md`). **Set it to the empty string for memory-only mode**: the hub starts and creates no directory, and a kind whose result must be an artifact is refused at admission with `400 artifact_unavailable` instead of half-running (spec §8.2). |
| `NEXUS_QUEUE_DEPTH` | `128` | Beyond it: `429`, never an unbounded wait. |
| `NEXUS_INLINE_PAYLOAD_CAP_BYTES` | `8 MiB` | Above it the prompt travels by `payload_ref`. |
| `NEXUS_LEASE_MS` / `NEXUS_HEARTBEAT_MS` | `60000` / `20000` | `validate()` requires lease ≥ 2× heartbeat. |
| `NEXUS_RESUME_GRACE_MS` | `1500` | `0` = strict "expire immediately" (see *Open decisions*). |
| `NEXUS_NO_WORKER_GRACE_MS` | `2000` | Pre-body grace; then `503 + Retry-After`. |
| `NEXUS_JOB_HARD_CAP_MS` | `1800000` | Per-job deadline. |
| `NEXUS_DETACHED_ORPHAN_TTL_MS` | `300000` | **New.** How long a detached (`"detach": true`) job that **nobody has ever watched and nobody has claimed** is kept before it is forgotten with its queue entry, tenant slot and idempotency key. Must be `> 0` — a zero would delete a detached job the instant it was submitted. A job already running is not covered by it; `job_hard_cap_ms` and the lease rules are. |
| `NEXUS_IDEMPOTENCY_MAX_ENTRIES` / `NEXUS_IDEMPOTENCY_TTL_MS` | `4096` / `600000` | Bounds of the idempotency mirror (both hub-side and actor-side); the TTL must be `> 0` or a duplicate submission would re-run work that is already in flight. Entries also go the moment the job they name is forgotten. |
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

* **`protocol`** — frame vocabulary, both directions: supplier
  (`Hello/Ready/Claim/Frame/Done/Cancel/Resume` in,
  `Welcome/Assigned/Ack/Reject/ResumeNack/Bye` out) and consumer
  (`ConsumerFrame::Submit/Attach/Detach/Cancel` in, `HubConsumerFrame`'s
  `Accepted/Rejected/meta/delta/event/log/artifact/finish/error/gap/replay/End` out, with
  `EndReason` naming why a stream closed), integer version negotiation
  (mismatch = clean reject + close), the wire codec — JSON lines for the worker direction,
  one JSON object per WebSocket text message for the consumer direction — with
  per-direction size caps (`decode_consumer_frame`, `protocol.rs:954`),
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
   the task does not specify. Two joins no longer race each other: a joiner **borrows** the
   relay that belongs to the job rather than owning one, so its request ending must not
   prune the map entry — removing it while the original submission's pump is still attached
   would move the fan-out to a fresh relay under a live stream and silence the first
   consumer's deltas (`prune_relay`, `consumer.rs:374-385`, honoured at `:451`; pinned by
   `a_non_streaming_retry_on_a_key_assembles_the_running_answer`).
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
  frames are dropped by connection generation, and an evicted socket's close is a no-op —
  which is also why a nack can only reach a worker that has a **live** socket: a `Resume`
  stamped with the retired generation is dropped like any other stale frame, so a
  reconnecting client is re-attached first and refused second.
  A re-`Hello` for a known worker id is a socket swap, not a death: the record is updated
  in place, so `in_flight` (which is what §4's capacity cap and the heartbeat-driven lease
  renewal both read), the held leases' `epoch` and — when the previous socket was still
  live — the engine state survive, while the replaced socket's unfulfilled `Claim`
  authority is voided and any pending resume-grace timer is cancelled. After a death the
  `Degraded` demotion stays, so a reconnecting worker must report `Ready` again before it
  is dispatched to (§8).
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
  A deployment with no scratch disk sets `NEXUS_ARTIFACT_ROOT=""` and runs memory-only: no
  directory is created and a kind that would have to write is refused at admission with
  `400 artifact_unavailable` (spec §8.2). The idempotency tables are the only other
  keyed state on the hub — 4 096 entries / 600 s, and dropped the moment the job they name
  is forgotten, so an entry is a dedupe pointer rather than a cached answer.
* **Path safety**: job/artifact ids are restricted to `[A-Za-z0-9._-]{1,64}` with no
  leading/trailing dot, and artifact paths are built through `artifact_path_checked`, so a
  `../` cannot escape the root.
* **What `GET /v1/observe` publishes, to anyone.** The snapshot is open by the same
  rule as `/healthz` and `/metrics`, and from any origin, so whoever can reach the
  port can read **worker ids, pool and kind names, job ids, tenant ids, connection ids,
  byte counters, lease deadlines and per-worker telemetry**. That is a deliberate
  disclosure of topology and activity — which tenant is running what, and how loaded a
  worker is. It is not a disclosure of content: no prompt text, no streamed answer, no
  artifact bytes, no credentials, and no peer addresses (the transport does not expose
  them). A deployment that must hide tenant names or job ids from the network should
  not expose this route — front it with a proxy, or keep the dashboard on the same
  origin — since the route itself carries no authentication to revoke.
* A worker on hardware someone else administers is a worker, not a trusted peer: unknown
  fields are tolerated (forward compatibility) but every value is validated, and a bad
  frame closes the socket rather than desynchronising it.

## §16 acceptance matrix

Two words carry precise meaning in this table:

* **core** — `crates/nexus-core/tests/acceptance.rs` asserts the actor's decision
  with injected time. That file has been **run and is green** (48/48), so "core" now
  means "a test states the exact expected value *and that value was observed*";
* **runtime** — the behaviour needs the native handlers (`crates/nexus-server`)
  and a live socket. These are asserted by
  `crates/nexus-server/src/runtime_e2e.rs` (real listener, real worker WebSocket, real
  SSE bytes and real consumer-WebSocket frames) — **also run and green**, as is the rest
  of the bin's **31**: 19 `runtime_e2e`, 7 `io::tests` against real files, 5
  `state::tests`.

| # | Requirement | Status | Where |
| --- | --- | --- | --- |
| 1 | Two workers, one job: exactly one `Assigned`, the other gets nothing | **core** | `t01_two_workers_one_job_yields_exactly_one_assigned` |
| 2 | Concurrent claims: one winner, deterministically | **core** (mailbox order; both arrival orders asserted) | `t02_concurrent_claims_resolve_deterministically_in_mailbox_order`, `the_actor_never_reports_two_owners_for_one_job` |
| 3 | Worker killed before any byte: requeued, completes elsewhere, no error to the consumer | **core**, with a caveat: a delta that is still inside the flush window counts as "delivered", so a kill in that window takes the in-band-terminal branch instead of the transparent requeue | `t03_worker_killed_before_any_output_is_requeued_and_completes_elsewhere` |
| 4 | Worker killed after bytes: in-band error, one finish, no duplicate deltas | **core** | `t04_worker_killed_after_output_terminates_in_band_with_one_finish` |
| 5 | Consumer disconnect → worker receives `Cancel`, slot freed on ack | **core**; at runtime the decision existed but nothing ever sent `Detach`, so the chat surface could not satisfy this at all. Fixed: both pumps send `Detach` when the body ends, and `runtime_e2e::consumer_disconnect_reaches_the_worker_as_cancel` drives it over a real socket. Ownership is now the **subscription's** flag, not the wire format: `JobRecord::cancel_on_consumer_gone` (copied from the submission) decides in `on_detach`, so a poll-later job survives its `GET /v1/jobs/{id}/events` watcher hanging up | `t05_consumer_disconnect_cancels_the_worker_immediately`, `an_async_job_survives_its_events_watcher_disconnecting` + runtime e2e |
| 6 | Zero ready workers at submit: `503 + Retry-After` before any body bytes | **core**; **runtime asserted** (a connected-but-loading worker, real 503, JSON content type, `Retry-After` present) | `t06_no_ready_worker_answers_503_with_retry_after_before_any_body`, `t06b`, `t06c` + `runtime_e2e::loading_worker_answers_503_with_retry_after_before_any_body` |
| 7 | Duplicate idempotency key while in flight: one execution, second client joins/replays | **core** and **runtime**. The key is now resolved **before** any id is minted: both consumer faces look it up in the hub's own mirror (`Hub::idempotency`, `state.rs:237-259`, read by `idempotency_target` `:473`) and, on a hit, **follow the job that already exists** — `join_existing_job` (`consumer.rs:1251`) on HTTP, `handle_attach` (`consumer_ws.rs:533`) on the socket — rather than submitting a second execution and hoping the actor's duplicate branch reaches them. What this row used to claim ("fixed in `submit`, which installs the queue before the actor is told") described a mechanism that did not hold at runtime: the actor replays under the **original** job id, which is not the id a fresh submission would have registered its own relay and verdict gate under, so the second chat client saw only the later deltas and its retry could time out as `503 hub_unavailable` while the original's replay was being dropped. Deciding on the face removes the id mismatch instead of racing it; the mirror is bounded and TTL'd, and is pruned the moment the actor forgets the job (`state.rs:1065-1082`), so it is a dedupe table and not a result cache (spec §8.1.6) | `t07_duplicate_idempotency_key_attaches_and_replays_never_reruns`, plus `a_retry_on_an_idempotency_key_replays_the_original_completion` (`runtime_e2e.rs:1783`), `a_retry_on_an_idempotency_key_gets_the_task_that_already_exists` (`:1903`), `a_resubmitted_key_on_the_consumer_socket_follows_the_original_task` (`:1996`) and `a_non_streaming_retry_on_a_key_assembles_the_running_answer` (`:2120`) |
| 8 | Reconnecting worker resumes from the last acked `seq`, no re-run, no duplicate deltas | **core** | `t08_reconnecting_worker_resumes_from_the_acked_seq_without_duplicates`, `resume_for_a_moved_or_finished_lease_is_nacked` |
| 9 | Consumer reconnect with `Last-Event-ID`: replay with no gap | **core**; runtime: the job API had the plumbing, the chat surface had none (a chat reconnect always allocates a new job, so `Last-Event-ID` was ignored) — the join case is now covered by row 7's fix, an in-place chat resume is still not implemented | `t09_consumer_reconnect_with_last_event_id_replays_without_a_gap` |
| 10 | Unsatisfiable capability → immediate `400` | **core** | `t10_unsatisfiable_capability_is_an_immediate_400`, `t06c` |
| 11 | Only `ready` workers are dispatched to | **core**, **runtime asserted** (a loading worker's consumer gets the 503 rather than a dispatch) | `t11_a_loading_worker_never_receives_assigned` + runtime e2e |
| 12 | Declared capacity respected (4 and 1 slot) | **core**, **WASM-asserted** (same actor, Node host) | `t12_declared_capacity_is_respected_per_worker`, `t12b_a_one_slot_worker_takes_exactly_one_job` |
| 13 | Off-allowlist SHA cannot join | **core**, **WASM-asserted** (+ version and capability-floor rejects) | `t13_attestation_outside_the_allowlist_cannot_join_the_pool`, `t13b`, `t13c` |
| 14 | Tool-call/reasoning deltas and `usage` survive verbatim | **core**; **runtime asserted** for byte-for-byte relay over real SSE (one chunk in, the same chunk out, one `[DONE]`) | `t14_tool_call_reasoning_and_usage_survive_the_relay_verbatim` + `runtime_e2e::worker_relay_reaches_the_consumer_verbatim_over_sse` |
| 15 | Tens-of-MB artifact: RSS flat, resumable download with valid sha256 | **partly core**: chunk accounting, measured-digest verification, size mismatch and foreign-writer refusal. **Runtime**: the download is a **streamed** range — seek + bounded 64 KiB reads through a 4-deep channel, never a whole-object buffer — with `416` for unsatisfiable ranges, an on-disk length check before the status is committed, and a whole-object digest verification that aborts the body on mismatch. Chunk-cap enforcement and attempt-scoped parts came from the review pass. Asserted by `io::tests` (7 tests, real files under the repo-root `Temp/nexus-tests/` scratch directory, body drained chunk by chunk; the ingest half pins the append-only staging file, the streaming digest and the publish-by-rename); **RSS itself has never been measured** | `t15_artifact_upload_is_measured_not_trusted_and_survives_a_resume` + `io::tests::full_object_read_streams_bounded_chunks_and_verifies_the_digest`, `partial_range_streams_only_the_requested_slice`, `unsatisfiable_ranges_answer_416_before_any_byte_is_committed`, `a_recorded_size_beyond_the_stored_object_is_refused`, `a_digest_mismatch_aborts_the_body` |
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

The review passes also added the runtime and I/O coverage the package shipped without, and
all of it has been executed since: `nexus-server`'s `runtime_e2e` module (**21** tests —
real listener, real worker socket, real SSE, and now real consumer-WebSocket traffic for
the NEX-3 face and §16.7), `io.rs`'s download-and-ingest module (**7** tests: chunked
streaming, exact slices, `416`, length/digest refusal, and staged ingest finalising by
rename), and `state.rs`'s **5** unit tests (label merging, the stalled-connection report,
the `JobForgotten` pruning of `job_index` and the relays, the idempotency mirror's bound
and expiry, and the routing index holding only the jobs the actor still knows).
Every one of them is written against the real types, and **177/177 pass** on this host
(the 163 that existed before the observation plane are unchanged and still green; the 14
more are the `/v1/observe` actor snapshot, its unit tests, and the two runtime tests for
the route and its CORS).

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

* **the Node glue cannot be generated on this host.** `wasm-bindgen`/`wasm-bindgen-cli`
  are still not installed, so `yarn wasm:glue` — and with it `yarn smoke:wasm` — cannot
  run, `pkg/` has never existed here, and the smoke test's assertion blocks remain
  unexecuted. `scripts/build-wasm.mjs` fails with the exact version `Cargo.lock` pins:
  `cargo install wasm-bindgen-cli --locked --version 0.2.130`. What this host does verify
  is that the crate compiles for the target (`cargo check -p nexus-wasm --target
  wasm32-unknown-unknown` → ok). The `.wasm` artifact itself, its 984 056-byte size and
  its `WebAssembly.compile` validation are the **2026-11-03** result: the release wasm was
  not rebuilt here during the remediation pass, and nothing in this README claims it was.
  Installing the CLI is a host change, which this pass was scoped not to make.
* **the suite is green on this host, and on a clean copy of it.** **163/163** with the
  pinned `1.99.1` toolchain at the time of that reproduction, reproduced from a pristine
  copy with a fresh target directory and `--locked` — so it is no longer a single-host,
  single-incremental-build result. The observation plane has since taken that to
  **177/177** on the same pinned toolchain (`cargo test --workspace`, 2026-11-10), with
  the 14 additions listed in the table above; the pristine-copy `--locked` reproduction
  has not been re-run since. The
  wasm smoke test and §14's latency budgets remain unmeasured, so nothing in this README
  claims them.

**Resolved since the first draft of this package:**

* the dependency graph is resolved and locked: `Cargo.lock` is committed in this
  package (the repo-root `.gitignore` says Cargo.lock is committed on purpose), and
  every version stated in `Cargo.toml` resolved — `axum 0.8.9`, `tokio 1.54.1`,
  `tower-http 0.6.11`, `tokio-tungstenite 0.26.2`, `reqwest 0.12.28`,
  `wasm-bindgen 0.2.130`, `sha2 0.10.9`, `serde 1.0.229`.
* `cargo check --workspace --all-targets` and a release build now pass; the sources
  have been through `rustfmt`.
* **`yarn verify` no longer stops before the tests.** It used to abort on `yarn clippy`,
  because 17 pre-existing `nexus-core` findings were denied by `-D warnings`. All 17 are
  settled: `cargo clippy --workspace --all-targets -- -D warnings` is exit 0 with **zero**
  findings, so the chain reaches `yarn test`.

**Resolved by the conformance pass (2026-11-10), item by item against the specification's
§9.1** (`documents/specifications/hub-specification.md`):

* **NEX-3 — the consumer WebSocket face exists.** `GET /v1/consumer`
  (`crates/nexus-server/src/consumer_ws.rs`, 1 005 lines) carries post *and* stream on one
  socket: `submit`/`attach`/`detach`/`cancel` in, `accepted`/`rejected` first, then
  `job`-tagged `meta`/`delta`/`event`/`log`/`artifact`/`finish`/`error`/`gap`/`replay`, and
  exactly one `end {job, reason}` per task with `EndReason {finished, errored, lagged,
  detached, closed}`. Several tasks multiplex on one connection, the verdict can never be
  preceded by an event byte (one writer owns every write), and the API key is checked
  **before** the upgrade (`401` + `www-authenticate: Bearer realm="nexus"`). The SSE
  surface is unchanged for OpenAI clients, and both faces share one admission path, so the
  same task is refused the same way (`a_refusal_reaches_the_consumer_before_any_event_bytes`,
  `one_consumer_socket_carries_two_tasks_and_answers_both_in_order`,
  `the_consumer_socket_refuses_an_unauthenticated_upgrade`).
* **orphan ⇒ delete, not orphan ⇒ retain** (NEX-6.3). A record that was still *running*
  when its last consumer left is cancelled **and dropped in the same mailbox step**:
  `forget_job` (`actor.rs:1382`) removes the record, the replay ring, the lease, the
  payload reference and the idempotency entry, so a later same-key request executes fresh
  instead of attaching to a dead stranger's answer. Only an already-**terminal** record is
  still retained, which is exactly the reason NEX-7.2 allows (`dropping_a_consumer_socket_cancels_and_deletes_its_task`).
* **hub-side mirror pruning.** `Effect::JobForgotten` now removes the routing entry, the
  relay and the idempotency row (`state.rs:1065-1082`). That also closed a slow leak this
  README did not used to mention: `job_index` was otherwise one entry per submission for
  the life of the process (`job_index_and_relays_are_pruned_when_the_actor_forgets_a_job`,
  `the_routing_index_holds_only_the_jobs_the_actor_still_knows`).
* **detached is now an explicit request field** (NEX-6.6), not a side effect of which route
  was used: `submit{detach}` defaults to `false` on the socket, `POST /v1/jobs` requires
  `"detach": true` (`400 detach_required` otherwise), and a detached job nobody ever
  watched and nobody claimed is forgotten after `NEXUS_DETACHED_ORPHAN_TTL_MS`
  (`a_task_posted_without_a_consumer_is_deleted_when_its_window_closes`,
  `the_async_job_route_requires_detach_to_be_stated`).
* **disconnection is noticed by the connection, not by the first failed write** (NEX-6.1):
  the socket face breaks on FIN/broken frame, and the SSE pump races the event source
  against `tx.closed()` (`consumer.rs:1358`) — `an_idle_sse_hangup_cancels_the_worker_without_a_failed_write`
  is the case that used to sit on a GPU slot until the TCP stack gave up.
* **`ArtifactBook` is bounded by count as well as bytes**: `MAX_ARTIFACT_ENTRIES = 128`
  (`store.rs:394`), deliberately `queue_depth`'s number, with evictions drained through the
  TTL sweep's deletion path rather than a second effect. Bytes alone were the wrong bound:
  a thousand 1 KiB uploads sit far below the byte budget while still growing the map, the
  directory count and the downloadable set without limit.
* **artifact ingest is bounded and atomic**: one append-only staging file per attempt,
  O(chunk) memory, a digest the hub measures as it goes, and a **rename** to publish
  (`io.rs:759`, `:809`, `:918`) — no second copy at completion, no half-written object for
  a downloader to find. `staged_ingest_appends_then_finalizes_by_rename` pins it at the
  file level.
* **flush/replay coherence and zero-TTL config are startup errors, not runtime surprises**:
  `Limits::validate` (`limits.rs:213`) refuses `flush_bytes > replay_bytes` (a full flush
  batch would otherwise be evictable from the ring before it was relayed) and refuses a
  zero `payload_ttl_ms`, `job_retention_ms`, `idempotency_ttl_ms`, `artifact_ttl_ms` or
  `detached_orphan_ttl_ms`, each with the consequence in the message
  (`limits.rs:351`, `:377`).
* **memory-only mode exists** for deployments with no scratch disk: an empty
  `NEXUS_ARTIFACT_ROOT` makes a kind that must write an artifact a clean
  `400 artifact_unavailable` at admission instead of a failure after the response body has
  opened (`memory_only_mode_refuses_a_kind_that_writes_an_artifact_and_admits_the_rest`).

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
* **`nexus_idempotent_attach_total` counts less than it used to, deliberately.** A
  same-key retry the hub can answer from its own mirror never reaches the actor, so the
  actor's counter does not move for it; what it counts now is the duplicates **the actor
  itself replays** — a key the hub's mirror has already dropped or expired on, or a race
  the mirror lost. The hub cannot compensate: `PoolGauges::from_actor` (`state.rs:213`)
  rebuilds the mirrored registry from the actor on every dispatch, so a hub-side increment
  would be erased a tick later (`consumer.rs:200-206`, `consumer_ws.rs:427-432`). Read the
  series as "actor-side joins", not "duplicate submissions observed"; the hub-answered
  retries are visible as the absence of a second `nexus_jobs_submitted_total` instead.
* **the artifact ingest layout changed, and the README's old wording did not.** The
  per-attempt *part files* are gone. One attempt now appends to a single staging file,
  `{object}.parts/a{attempt:04}/staging.bin` (`io.rs:759`), while a running SHA-256 covers
  exactly the bytes it has appended (`io::stage_chunk`, `io.rs:809`), and completion
  publishes by **rename** (`io.rs:918`) — no second copy at completion, and no half-written
  object for a downloader to meet. Chunk accounting still lives in the actor's
  `ArtifactBook`; what changed is that an abandoned upload leaves one disposable file
  instead of a directory of part files, and the TTL sweep's single `io::delete_artifact`
  (`io.rs:944-951`) removes the finished object **and** the `{path}.parts` directory under
  it in one call — which is why staging lives *inside* that directory.
* **ranges are query parameters, not the `Range:` header.** `GET /v1/artifacts/{id}` and
  `GET /v1/jobs/{id}/result` read `?from=&to=` (exclusive end) while *answering* with
  `Accept-Ranges` / `Content-Range` as a proper ranged endpoint would; a client that sends
  a standard `Range: bytes=1000-2000` header still gets the whole object. The worker's
  resumable upload is query-parameter driven too (`?chunk=&complete=`). Adding header
  parsing is a handler-level step (`io::get_artifact`), not a storage change.
* the `/metrics` exposition carries `# TYPE` but no `# HELP` lines, and the actor
  registers no metric help strings.
* **an output frame's `content_type` overwrites the job's payload `content_type`**
  (`relay_into`, the assignment at `actor.rs:3429`): `JobRecord::content_type` is documented as the
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
  silently got a truncated stream). The fix that actually holds is **upstream of the
  queue**: the consumer faces resolve the idempotency key against the hub's own mirror
  (`Hub::idempotency`, `state.rs:237-259`) **before minting an id**, and on a hit follow
  the job the hub already routed — `join_existing_job` (`consumer.rs:1251`) on HTTP,
  `handle_attach` (`consumer_ws.rs:533`) on the socket — so the replay is delivered on the
  relay that joiner registered under, and there is no second execution to reconcile. (What
  this bullet used to credit — `submit` installing the queue before the actor is told — was
  the wrong mechanism: the actor replays under the *original* id, which is not the id a
  fresh submission registers its relay and verdict gate under, so the retry could still
  time out as `503 hub_unavailable` while the original's replay was dropped.) Subscribing
  to the fan-out before the actor is told still stands, and still closes the
  lost-first-frames race in the same place (§16.14/§17). Pinned by the four idempotency
  retries in `runtime_e2e.rs` named in §16 row 7;
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
open. Same rules: only this package, nothing compiled, and at the time every claim below
was from reading code and tests that had never been executed. The verification pass above
has since executed them: the claims held (the suite stood at 128/128 in that pass's
own count, and at **163/163** when the verification pass executed it — **177/177**
today, with the observation plane), with the fourteen exceptions itemised in *Fixed
in this pass* — three of which were this document being confidently wrong, notably
the `sha2` diagnosis for the streamed artifact digest.

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
   `consumer::job_result`). Seven tests in `io::tests` drive real files and read the body
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
   litter.** `main.rs:8-9` hold `#[cfg(test)] mod runtime_e2e;`, which is the only way a
   binary-only crate gets `cargo test -p nexus-server` coverage; the module is 19 tests
   over a real listener (4 at that pass; the conformance pass added the consumer-WebSocket,
   orphan-deletion, detach, idempotency-retry and route-contract cases). Each test gets its own artifact root under the **repo-root**
   scratch directory (`Temp/nexus-tests/e2e-<test>`, via `io::test_scratch`, resolved from
   `CARGO_MANIFEST_DIR` so it does not depend on cargo's working directory), cleared before
   the run and after it (`TestHub::drop`), so parallel tests cannot collide on per-attempt
   part files, nothing disposable lands in the package or in OS temp (root `AGENTS.md`
   rule), and the hub's own configured artifact paths are unchanged.
   `.gitignore` also gained `Temp/`, because the hub's built-in default artifact root is
   CWD-relative and a `yarn start` from inside this package would otherwise dirty the
   nested repository.
