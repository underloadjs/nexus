# Specification — the nexus hub

**What this document is.** The statement of what `@underload/nexus` **is**, written as
requirements a build can be checked against. It is the normative answer to "what is
this package?" for anyone reading the code, and it is the contract the package is
measured against in §9 (*Conformance*) below.

**Where it sits relative to the other documents.** [`tasks/task-rust-hub-inference-broker.md`](../../../../../tasks/task-rust-hub-inference-broker.md)
is the original design brief (its `§`-numbers are what the source comments cite).
[`README.md`](../../README.md) is the build's own report card: verification status,
resolved open decisions, known gaps. Neither one is a specification — the task is a
wish list written before the code existed, and the README describes one particular
build. This file states the target, independently of whether the current build hits it.

**Reading the requirements.** Each rule is `NEX-<n>` and is normative: *must* means the
package is wrong if it does not hold. §9 records the current build's status per rule
with file-and-line evidence. Where this document deliberately narrows or extends the
task brief, §8 says so.

---

## 1. Position

Nexus is a **hub that streams data between suppliers and consumers**. A consumer posts
a *task*; the hub holds it until a supplier able to do it asks for work; the supplier
streams its result into the hub and the hub streams the same bytes out to the consumer
**while the supplier is still producing them**. Nothing is stored durably, nothing is
queued for later, nothing is written down as a matter of course: the hub is a live
conduit between two sockets.

The mental model is **Kafka-style eventing between two parties — but over real
WebSockets, not a broker log**:

| Kafka notion | Nexus equivalent |
| --- | --- |
| topic | **pool** — an in-memory queue of tasks of one kind, identified by `PoolId` |
| producer / publisher | **consumer** (the party that posts tasks; it also consumes the answer stream) |
| consumer group | **supplier pool membership** — suppliers pull work; the hub gives each task to exactly one of them |
| pull-based consume + offset | supplier `Claim` → `Assigned` → seq-numbered `Frame`s → `Ack` |
| partition log | **absent, on purpose** — there is no log. See §6. |

The differences are the point. Kafka is a durable, ordered, replayable log with a disk
behind it; nexus is an **in-memory switch with a bounded, best-effort replay buffer** in
front of live sockets. A task that nobody can serve now is refused with a retryable
error rather than parked for hours. If the hub restarts, its state does not come back.
"Streams" here means *live byte relay*, not *retained stream*.

## 2. Vocabulary

| Term | Meaning in this package |
| --- | --- |
| **hub** | The nexus server process. The only public surface. Owns the queues, the leases, and the relay. |
| **task** | One unit of work a consumer posts. Called a *job* in the code (`JobId`), with numbered *attempts* after a requeue. |
| **consumer** | The party that posts a task and receives its result stream. Authenticates with an API key; identified as `ConsumerId` for subscription accounting. |
| **supplier** | The party that does the work. Called a *worker* in the code (`WorkerId`). Authenticates with a pool token, **dials out** over a WebSocket, so it needs no public address. |
| **pool** | One queue + one set of suppliers serving one task kind. The hub runs one actor per pool, and the actor is the only mutator of that pool's state. |
| **lease** | Timed, exclusive ownership of a task by one supplier attempt. Renewed only by heartbeats. |
| **relay** | Copying supplier output frames to consumer subscribers, verbatim, with per-job sequencing and bounded replay. |
| **orphan** | A task whose consumer's connection has gone away while the task is still live (§5.4). |

## 3. NEX-1 — Implementation

> The hub is a server **written in Rust and compiled from Rust source into a native
> executable** by cargo. It ships as one binary.

- The runtime is Rust end to end: `crates/nexus-server` (axum + tokio) is the hub
  process; `crates/nexus-core` holds the queue/lease/relay logic and contains no I/O;
  `crates/nexus-worker-example` is the reference supplier client. `yarn build` produces
  the shippable binary (`cargo build --release -p nexus-server`).
- No JavaScript or TypeScript application logic participates in a request path. The only
  JS in the package is generated `wasm-bindgen` glue and two dev scripts.
- The core crate additionally compiles to `wasm32-unknown-unknown` (`crates/nexus-wasm`)
  so the *same* compiled scheduling rules can be embedded in a JS host. This is a
  portability affordance, not a second deployment shape, and it must never become a
  reason to move policy out of Rust: the JS host may perform I/O only (README, *WASM use,
  and the honest runtime boundary*).

*Note on the source wording.* The brief for this document says "a server build on rust,
compiled to rust". Read literally that is a tautology, so this specification records the
intended reading: **authored in Rust, compiled from Rust to a native Rust binary** — not
transpiled to another language, and not a JS server with a Rust addon. The wasm build is
listed above because it exists, not because the brief asks for it.

## 4. NEX-2 — The hub's job

> The hub is a nexus between suppliers and consumers: it accepts tasks, hands each one
> to exactly one supplier, and **streams** bytes between them like eventing — over real
> WebSockets.

- **NEX-2.1** The hub is the only component with a listening socket. Suppliers do not
  accept connections; they dial out and keep one persistent connection each.
- **NEX-2.2** One task is executed by exactly one supplier attempt at a time. Two
  suppliers streaming into one consumer stream is the failure this rule exists to
  prevent; the hub refuses frames from anyone but the current lease holder.
- **NEX-2.3** The hub treats task **content** as opaque. It validates envelope fields
  (kind, capability requirements, size, ids) and forwards the body; it does not parse,
  re-tokenize or rewrite a result stream mid-flight.
- **NEX-2.4** Delivery semantics are **at-most-once to the consumer, at-least-once
  across retries by explicit request**. A task is dispatched once; a lost supplier may
  cause a requeue only under the rules in §5.5, and the consumer is told in band when
  the answer is not trustworthy.
- **NEX-2.5** Cancellation is bidirectional and mandatory: a consumer that goes away
  stops the supplier (§5.4), and a supplier that dies is reported to the consumer
  (§5.5). GPU time and socket slots are both scarce, so "stream into the void" is a bug.

## 5. NEX-3 … NEX-7 — The two faces and the lifecycle

### 5.1 NEX-3 — The consumer's face

> A consumer opens a **WebSocket** to the hub, **posts tasks** on it, and **receives the
> result stream for those tasks on that same connection**.

- **NEX-3.1** One connection carries both directions: task in, result stream out. A
  consumer must not need a second connection to receive what the first one posted.
- **NEX-3.2** A connection may carry more than one task, and the stream for each task is
  tagged with that task's id so a consumer can multiplex.
- **NEX-3.3** Posting a task is acknowledged (accepted / refused) before any result
  bytes, and a refusal carries a status, a machine-readable code and, where the hub
  means "not now", a `Retry-After`.
- **NEX-3.4** The result stream is terminated by an explicit end-of-stream marker or an
  in-band error, never by silence.

### 5.2 NEX-4 — The supplier's face

> A supplier opens a **WebSocket** to the hub, **looks up tasks it can do**, **reads the
> task**, then **streams the result** back over that same connection.

- **NEX-4.1** A supplier declares what it can do on joining: pools, task kinds /
  capability requirements, concurrency capacity, and proof of what it actually loaded.
  The hub is not allowed to trust a self-declared model name as identity.
- **NEX-4.2** Work selection is **pull-driven**: the supplier asks for work, and the hub
  answers only with tasks that supplier's declared requirements can actually satisfy.
  A supplier never receives a task it cannot do.
- **NEX-4.3** `Assigned` (or equivalent) carries the whole readable task: kind, resolved
  requirements, the payload, and the deadlines the supplier must respect.
- **NEX-4.4** The supplier streams output as ordered, per-task numbered frames, and ends
  with a terminal frame. Out-of-order and duplicate frames are detectable by the hub.
- **NEX-4.5** The supplier must keep heartbeating while it owns a task, or the hub takes
  the task away.
- **NEX-4.6** A supplier that is not genuinely ready (model still loading, engine
  degraded) is not dispatched to, however idle it looks.

### 5.3 NEX-5 — Live relay

> While a supplier is still streaming, the hub is already streaming to the consumer.

- **NEX-5.1** No "wait for completion then deliver". Bytes cross the hub as they arrive,
  subject only to an explicit, bounded batching window that exists for syscall economy
  and must never exceed a stated latency budget.
- **NEX-5.2** The consumer's stream is the supplier's stream: verbatim, in order, without
  gaps. A detected gap is reported in band rather than papered over.
- **NEX-5.3** The hub keeps no per-task buffering that grows without bound. Replay
  buffers are bounded in frames and bytes; a consumer that cannot keep up is ended in
  band with a stated reason, not allowed to stall the pool.

### 5.4 NEX-6 — Lifetime coupling: no consumer, no task

> To prevent cross-streaming, a consumer that posts a task **keeps its connection open**
> for as long as it wants the answer. If it does not, the task is **orphaned and
> deleted**, and any supplier stream feeding it is **terminated automatically**.

- **NEX-6.1** The consumer's connection **is** the subscription. Its loss is an event the
  hub notices, not something discovered when a write finally fails.
- **NEX-6.2** On that event the hub stops feeding nobody: the task stops being queued,
  the supplier is told to stop producing, and further supplier frames for that task are
  not relayed to anyone.
- **NEX-6.3** The task is then **deleted**: no queue entry, no lease, no record, no
  retained stream, no idempotency entry that will hand the same answer to a later
  stranger. A task with no attached consumer has no reason to exist.
- **NEX-6.4** Termination is automatic — the supplier does not have to be asked politely
  by a third party, and the consumer does not have to send a goodbye.
- **NEX-6.5** Cross-streaming means: an answer arriving for a task nobody is listening
  to, or an answer arriving on a connection that does not own the task. Both are
  prevented by NEX-2.2 and this rule together.
- **NEX-6.6 (permitted exception).** A consumer may *explicitly* declare a task detached
  — "post it now, I will pick the result up later". That is a different, opt-in contract
  from NEX-6.1 and must be stated as such on the request; it never applies to a plain
  "stream me the answer" submission.

### 5.5 Loss and recovery (supporting rules)

- **LOSS-1** A supplier that dies having produced **no** bytes may be requeued to
  another supplier, transparently, when the task is idempotent. A non-idempotent task
  fails loudly instead of silently running twice.
- **LOSS-2** A supplier that dies **after** bytes reached the consumer cannot be made
  invisible: the stream is ended in band with a retryable error.
- **LOSS-3** A reconnected supplier may resume a lease it still holds and must resume
  from the last frame the hub acknowledged — no re-run, no duplicated deltas.

### 5.6 NEX-7 — Nothing is kept

> The server keeps nothing on the server. Everything is in memory, and it streams
> accordingly.

- **NEX-7.1** Task payloads and result bytes are **memory-only**. Nothing is written to a
  filesystem, database or object store on the ordinary path.
- **NEX-7.2** In-memory state is bounded twice over — by count/bytes **and** by TTL — and
  every retained byte has a stated reason for still existing (replay for a live
  consumer, a lease that must be honoured, an answer a retry must not re-run).
- **NEX-7.3** Hub restart is allowed to lose everything. Clients must see a clean,
  retryable failure, not corruption; no recovery procedure may depend on state the hub
  kept.
- **NEX-7.4** The one permitted exception is **bulk artifact egress**: a result large
  enough that buffering it in RSS would be the wrong engineering choice may leave the
  socket and be handed off as a bounded stream to a store. That exception must be
  explicit, must be bounded, must not apply to task payloads, and must be reported as a
  deviation (§8.2) rather than treated as the default.

## 6. Explicit non-goals

These are *not* part of what nexus is, and a reviewer should not mark their absence as
a defect:

1. **Durability / replay log.** No Kafka. A hub restart discards queues, tasks, replay
   buffers and leases. Nothing is re-read from disk on boot.
2. **Conversation memory.** The hub stores no chat history. Context arrives with the
   request; the hub may use a session hint only to keep a conversation on one supplier.
3. **Multi-replica scaling.** One process, in-process queues. A shared-backend trait is
   the upgrade path, not a current capability.
4. **Result caching.** Two different consumers asking the same question get two
   executions unless they share an idempotency key while one is in flight.
5. **Sampling fan-out** (`n > 1` completions per submission) and any per-task billing,
   accounting or audit trail beyond operational metrics.
6. **TLS termination.** The hub listens in the clear by default and expects to sit behind
   a terminator.
7. **Supplier scheduling sophistication.** Capability match, readiness, capacity and
   session affinity are required; tier preference, warm-cache-aware placement and
   latency-based ranking are not.

## 7. Protocol expectations

The specification names the shape, not the spelling — but the spelling below is what the
package uses today, and a change to it is a change to this section.

**Supplier ↔ hub (WebSocket, JSON lines, one frame per line).**

| Direction | Frames |
| --- | --- |
| supplier → hub | `Hello` (identity, pools, capacity, requirements, attestation, version) · `Ready` (heartbeat, free slots, engine readiness) · `Claim` ("give me up to N") · `Frame` (one seq-numbered output frame for one job) · `Done` (terminal) · `Resume` (re-claim after a socket loss) · `CancelAck` |
| hub → supplier | `Welcome` (negotiated version, joined pools, heartbeat/lease parameters) · `Assigned` (the readable task: kind, requirements, payload, deadlines, `start_seq`, artifact upload slot) · `Ack` (flow control + resume floor) · `Cancel` (stop producing) · `Reject` · `ResumeNack` · `Bye` |

**Consumer → hub.** The specification requires a WebSocket carrying both a *post* and
the *stream* on one connection (§5.1). Today the consumer surface is HTTP:

- `POST /v1/chat/completions`, `POST /v1/completions` → an SSE response body (the stream)
- `POST /v1/jobs` → `202` + `GET /v1/jobs/{id}/events` (SSE) + `GET /v1/jobs/{id}/result`
  + `DELETE /v1/jobs/{id}`
- `GET /v1/models`, `GET /healthz`, `GET /metrics`
- bulk: `GET /v1/payloads/{id}` (supplier fetch), `GET|PUT /v1/artifacts/{id}` (§8.2)

Credentials are per direction and never shared: pool token for suppliers, API key for
consumers, plus an attestation allowlist for the model a supplier claims to have loaded.

## 8. Deviations and interpretations recorded here

### 8.1 Interpretations of the brief

1. *"streams data between supplier and consumer like kafka eventing, but with real
   websockets"* — read as a **shape** statement (topic-like queues, pull-based worker
   groups, live relay), not a durability statement, because the same brief also requires
   that nothing be stored (§6.1, NEX-7). Where the two readings conflict, **NEX-7 wins**:
   nexus is an in-memory conduit, not a log.
2. *"consumer … can post tasks, waiting to receive the stream"* — read as one connection
   doing both (NEX-3.1), because NEX-6 makes that connection the subscription; a two-
   connection contract would make "keep your socket open" ambiguous.
3. *"supplier … look up any tasks that they can do, read the task"* — read as
   **capability-filtered pull** (NEX-4.2/4.3). Whether the supplier may *browse* a task
   before committing to it is **not** specified; a lookup that immediately leases is
   conforming, a peek-without-lease is permitted but not required.
4. *"the server can stream the result to the consumer"* while the supplier is still
   streaming — read as NEX-5.1, with an explicit allowance for a bounded batching window
   (a per-token flush would cost 4× the syscalls for no perceptible gain).

### 8.2 Deliberate extensions of the brief

- **Bulk artifact egress** (NEX-7.4). The package supports non-text results — images,
  video, TTS — of "tens of megabytes". Holding those in RSS while a supplier uploads and
  a consumer downloads at unpredictable speeds is a worse engineering choice than
  streaming them to disk with a TTL. This is the **only** on-host persistence in the
  design, it is opt-out in spirit (artifact kinds only, never task payloads), and it is
  recorded as a deviation from the literal "keeps nothing" of NEX-7 rather than folded
  into it silently.
- **Operational surface** (`/healthz`, `/metrics`, `Idempotency-Key`, `Last-Event-ID`,
  tenancy caps, size caps, backpressure) is specified elsewhere (the task brief §8, §9)
  and is not restated as a requirement here. Where an operational bound exists to protect
  NEX-5.3 or NEX-7.2, it is named in §9's evidence.

## 9. Conformance status of the current build

Measured against `packages/underload/nexus` at the working tree of this commit. Every
row cites the code that decides the answer; nothing here is inferred from the README.

**Summary: 4 met, 2 partially met, 1 not met.** The single unmet requirement is
**NEX-3** — the consumer face is not a WebSocket.

| Rule | Status | Evidence and gaps |
| --- | --- | --- |
| **NEX-1** Rust server, compiled from Rust | **Met** | `Cargo.toml` declares a standalone workspace; `crates/nexus-server/src/main.rs:36-98` is the binary (`yarn build` → `cargo build --release -p nexus-server`). No JS in any request path; `crates/nexus-wasm` is the same `PoolActor` for a JS host, `crates/nexus-core/src/lib.rs` is I/O-free and compiles for native **and** wasm32. |
| **NEX-2** hub streams between supplier and consumer, exactly-one-owner, opaque content | **Met** | One actor per pool owns queue/leases/jobs (`state.rs:304-342`, "start one actor task per configured pool"; `PoolActor` in `actor.rs`); claim authority is fulfilled in mailbox order (`on_claim` `actor.rs:1643-1677`, `fulfil_claims` `:1691`); a frame from a non-owner or wrong attempt is refused (`on_frame` `:1997-2010`); relay copies `delta` text untouched (`relay.rs` module docs, `relay_into` `actor.rs:3087`); consumer-facing dialects live only in `adapters.rs`. |
| **NEX-3** consumer posts and streams over a **WebSocket on one connection** | **Not met** | The hub has exactly one WebSocket endpoint and it is the supplier's: `main.rs:109` → `/v1/worker` (`worker_ws.rs:49`). Every consumer route is HTTP: `main.rs:111-123`, answered by `Sse::new` (`consumer.rs:233`, `:680`). There is **no** consumer WebSocket anywhere in the tree — `ConsumerFrame`, `/v1/ws` and `/v1/consumer` match nothing in `crates/`, `examples/` or `scripts/`. Mitigating: `POST /v1/chat/completions` does answer on the **same** TCP connection it was posted on (NEX-3.1 partially satisfied), but one-way and half-duplex, so a consumer cannot post a second task on a live stream (**NEX-3.2 not met**), and `POST /v1/jobs` splits post and stream across two connections (202 + `GET /v1/jobs/{id}/events`). NEX-3.3 (accept/refuse before body bytes) and NEX-3.4 (explicit end or in-band error) **are** met on the HTTP surface: `consumer.rs` answers `503 + Retry-After` before committing an SSE body, and every pump ends on `Finish`/`Error`/`[DONE]`. |
| **NEX-4** supplier opens a WebSocket, looks up work it can do, reads the task, streams the result | **Met** | `/v1/worker` upgrade (`worker_ws.rs:49`); `Hello` declares pools/capacity/`requires`/attestation (`protocol.rs:281-303`) and is checked against the pool floor and the allowlist before joining (`on_hello` `actor.rs:1286+`); `Claim` is a pull (`protocol.rs:318-323`) and `pick_job_for` (`actor.rs:1803-1819`) filters strictly — `requires.satisfied_by(worker.requires)`, phase must be `Queued`/`Pending`, KV headroom, affinity — so **NEX-4.2** holds; `Assigned` carries kind, requirements, payload (inline or ref), deadlines and `start_seq` (`protocol.rs:397-429`) = **NEX-4.3**; `Frame{job,seq,kind,text}` + `Done` = **NEX-4.4**; heartbeat-only lease renewal and `Ready`-gated dispatch = **NEX-4.5/4.6** (asserted by `t11_a_loading_worker_never_receives_assigned`, `t12_*`, `heartbeats_renew_the_lease_*`). Caveat, per §8.1.3: lookup and lease are fused — a supplier cannot enumerate or peek available tasks without claiming them, which the specification permits but the brief's wording "look up … read" might not. |
| **NEX-5** the consumer receives bytes while the supplier is still producing | **Met, with a stated window** | Each accepted `Frame` becomes a `ConsumerEvent` relayed immediately in the same tick (`on_frame` → `Effect::Relay` `actor.rs:2065-2069`), fanned out to subscribers (`state.rs:840`); outbound SSE is gated, not per-token: flush at `flush_bytes = 1536` or `flush_interval_ms = 75`, whichever first, plus an age-based timer flush and a `forced` bypass for terminals (`limits.rs:148-149`, `relay.rs:153` `FlushGate`, `:191` `should_flush`) = **NEX-5.1**. Verbatim + gap detection = **NEX-5.2** (`Gap{expected,got}` on a seq jump, `actor.rs:2034-2038`). Replay ring is bounded twice (256 frames / 1 MiB) and a lagging subscriber is ended in band with `consumer_lagged` (`consumer.rs:952-960`, and `:1113` on the job-events pump) = **NEX-5.3**. Honest limit, already in the README: a delta inside the flush window counts as delivered, which shifts task brief §16.3's "killed before any byte" into the in-band-terminal branch. |
| **NEX-6** consumer gone ⇒ task orphaned, deleted, supplier stream terminated | **Partially met** | The mechanism exists on the chat path: the SSE pump sends `Detach` when the body ends for any reason (`pump_sse` `consumer.rs:909`, `Detach` at `:991-997`; events-watcher pump `:1105`, `:1128`), `on_detach` cancels when the last consumer leaves (`actor.rs:1207-1233`), `on_cancel` tells the owner first and pulls the task out of the queue (`:1235-1264`, `queue.remove` at `:1251`), and **any later supplier frame is dropped rather than relayed** (`on_frame` `:2012-2018`, `post_cancel_frames`) — so **NEX-6.2/6.4** hold. Four gaps: **(a) NEX-6.3's "deleted" is delayed** — the job is made terminal and its prompt is released immediately (`finish_job` `:2725-2733`), but the record and its replay ring are retained until the sweep at `now + job_retention_ms` (default **300 s**, `limits.rs:161`; armed `actor.rs:2737-2740`, applied `:2993-3001`), and the idempotency entry outlives the job for 600 s (`:2741-2742`), so a later request with the same key still attaches to an orphan's answer. **(b) NEX-6.1's "notices" is write-then-learn** — the loss surfaces when a push into the response body fails (`send_block` returns false, `consumer.rs:1004`) or blocks on backpressure, not as an independent connection event; `send` is awaited with no timeout, so a stalled-but-open consumer holds the task until its TCP stack gives up. **(c) NEX-6.6's exception is not opt-in, it is a different surface** — `POST /v1/jobs` hardcodes `cancel_on_consumer_gone: false` (`consumer.rs:432`) and deliberately survives every consumer leaving (`t15`/`an_async_job_survives_its_events_watcher_disconnecting`), which contradicts NEX-6.1 unless the request is understood as the declared-detached case; the OpenAI adapters hardcode `true` (`adapters.rs:175`). The hub also lets several consumers attach to one task and replay (task brief §17.7), so "the consumer's socket is the subscription" is per-subscription rather than per-task. **(d)** supplier-side termination is cooperative: the hub stops relaying and sends `Cancel`, and frees the slot on `CancelAck` or lease expiry (`on_cancel_ack` `actor.rs:1266-1282`) — a supplier that ignores `Cancel` keeps burning its slot, though its bytes reach nobody. |
| **NEX-7** nothing kept; everything in memory | **Partially met** | Task payloads are memory-only and bounded twice: `store.rs` module table ("Request payload / prompt text … **memory only** … never on disk"), `PayloadBook` bounded by entry count **and** total bytes, released at terminal or `payload_ttl_ms = 120 s` (`limits.rs:155`, `finish_job` `actor.rs:2725-2733`); deltas live in the bounded ring; terminal records are TTL-swept; so **NEX-7.2/7.3** hold. The gap is **NEX-7.1 vs artifacts**: every artifact chunk is written to disk — `io::write_chunk` does `create_dir_all` + `File::create` + `write_all` per chunk (`io.rs:578-589`), assembles and `write`s the object on completion (`:605`), under `NEXUS_ARTIFACT_ROOT` (default `Temp/nexus/artifacts`, `main.rs:65-68`, `io::ensure_root` at `:76`), TTL `artifact_ttl_ms = 600 s` (`limits.rs:158`), deleted by the sweep (`Effect::DeleteArtifact` → `state.rs:890-893`); the per-attempt part files survive completion (`io.rs:606`) and only go when the object is deleted (`io.rs:610-620`). Under the NEX-7.4 exception this is *permitted* rather than conforming-by-default, and it is reported as a deviation in the README — but under a literal NEX-7.1 it fails. Read charitably, the design is still "nothing is *retained*": the artifact is deleted when `artifact_ttl_ms` lapses (`io::delete_artifact`, `io.rs:610-620`), so a hub write-down is a bounded staging area for a live transfer, not a store a client can come back to indefinitely. |

### 9.1 What would close the open items

Ordered by how much of the specification they buy:

1. **NEX-3** — a consumer WebSocket: one connection, `{"type":"submit",…}` in,
   `accepted` + tagged `delta`/`end` frames out, multiplexed by task id. This is a new
   endpoint plus a consumer-side dialect of the existing frame codec; `PoolActor` already
   speaks `Submit/Attach/Detach/Cancel` per consumer (`ActorMsg` `actor.rs:195-312`) and
   already fans one task out to N subscribers, so the actor needs no change — only a
   second face that turns socket messages into `ActorMsg` and `Effect::Relay` back into
   frames. The SSE surface can stay for OpenAI-compatible clients.
2. **NEX-6.3** — orphan ⇒ delete, not orphan ⇒ retain: on `Detach` of a
   cancel-on-gone task with no other subscriber, drop the record and its idempotency
   entry instead of arming `job_retention_ms`. Retention is worth paying for only when a
   consumer might come back for an answer that exists.
3. **NEX-6.6** — make "detached task" an explicit request field rather than an artifact of
   which route was used, and cap its lifetime while it has no subscriber.
4. **NEX-6.1** — notice disconnection from the connection, not the first failed write
   (a read-side `select` on socket close next to the send loop in `pump_sse`).
5. **NEX-7.4** — keep the artifact exception honest: state the root, the TTL and the byte
   bound in one place, and give an operator an in-memory-only mode for deployments that
   have no scratch disk (`artifact_root = ""` ⇒ reject artifact kinds at admission rather
   than write them).

### 9.2 Verification of this audit

- Read against the working tree, not against `README.md`'s claims: every row above cites
  the deciding code path.
- **Executed on this host, Linux, 2026-11-10:** `cargo test --workspace --no-fail-fast`
  → **128 passed / 0 failed** — `nexus-core` lib 74/74,
  `crates/nexus-core/tests/acceptance.rs` 42/42, `nexus-server` bin 12/12 (the 4
  `runtime_e2e` tests over a real listener + real worker WebSocket + real SSE bytes, and
  the 6 `io::tests` + 2 `state::tests`), `nexus-wasm` and `nexus-worker-example` 0 tests.
  Toolchain: `cargo 1.99.1`, the repo's pinned root `.cargo/config.toml`
  (`target-dir = .rust/target`). This matches the Windows/MSVC result the README records,
  so the green suite is no longer single-host.
- Which of these support the rows above: `consumer_disconnect_reaches_the_worker_as_cancel`
  is the executed proof of NEX-6.2/6.4 on the HTTP surface; `runtime_e2e::worker_relay_reaches_the_consumer_verbatim_over_sse`
  is the executed proof of NEX-5.2 (one chunk in, the same chunk out, one `[DONE]`);
  `t03`/`t04` are LOSS-1/LOSS-2; `t08` is LOSS-3; `t11`/`t12`/`t13*` are NEX-4.2/4.5/4.6.
- **No executed test covers NEX-3, because no consumer WebSocket exists to test**: the
  only WebSocket traffic in the suite is the worker's (`runtime_e2e.rs:154-198`).
- Not re-benchmarked here: the task brief's §14 30–60 ms proxy budget and RSS-during-download are
  unmeasured, so NEX-5.1's "bounded window" is verified by constant and by test, not by
  a latency histogram. The wasm artifacts were not rebuilt on this host during this audit
  (`wasm-bindgen` CLI remains absent, so the Node glue and `examples/node-smoke.mjs` are
  still unexecuted — see the README's *Known gaps*).
