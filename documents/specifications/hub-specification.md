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

**Consumer ↔ hub (WebSocket, JSON, one frame per text message).** This is the face §5.1
requires, and the spelling is what `crates/nexus-server/src/consumer_ws.rs` speaks today;
the shapes are `ConsumerFrame` (`crates/nexus-core/src/protocol.rs:550`) and
`HubConsumerFrame` (`:616`).

- **Route** `GET /v1/consumer` (`main.rs:135` → `consumer_ws::upgrade`). Credentials are
  the consumer's API key on the `Authorization` header, checked **before** the upgrade:
  a missing or unknown key is an HTTP `401` carrying
  `www-authenticate: Bearer realm="nexus"` (`consumer_ws.rs:121-131`), never a socket
  that opens and then closes for a reason only a frame could have said. The source
  comments call this **NEX-3.5**; §5.1 stops at NEX-3.4, so that id is a code label
  rather than a rule of this document — the behaviour is specified here, in this section
  that owns spelling.
- **Dialect.** JSON, one frame per WebSocket **text** message, tagged by a snake_case
  `type` field. The vocabulary is deliberately disjoint from the supplier's (NEX-2.2):
  one hub speaks both, and must never read a consumer frame as a supplier frame. A frame
  that fails to decode is answered **in band**, with the decoder's own machine code
  (`bad_json`, `bad_frame`, `frame_too_large`, `invalid_id`, `unknown_kind`, … —
  `CoreError::code`, `errors.rs:76-89`), and a **binary** message — the
  peer having misunderstood the dialect rather than one task having failed — is answered
  `error{code:"bad_frame"}` (`consumer_ws.rs:296-302`). Either way the connection stays
  up: one typo must not cost the other tasks that socket carries.
- **Consumer → hub.** `submit {kind, payload, pool?, requires?, priority?, detach?,
  idempotency_key?}` · `attach {job, from_seq?}` · `detach {job}` · `cancel {job}`.
  `detach` defaults to `false`, which keeps NEX-6.1 in force: this connection **is** the
  subscription. `true` is NEX-6.6's opt-in, stated on the request as that rule requires.
  An absent `idempotency_key` means a fresh key per submit — a consumer that wants "do
  not run this twice" asks for it, rather than having identical payloads silently merged.
- **Hub → consumer.** The verdict first, always ahead of any event byte for that task
  (NEX-3.3): `accepted {job, kind, pool, detached}` or `rejected {job, status, code,
  message, retry_after_secs?}` — the same status and machine code the HTTP surface would
  have given. Then the task's stream, every frame tagged with the `job` it belongs to so
  one connection can carry several tasks (NEX-3.2): `meta {job, attempt, pool, kind,
  state?, attached_ms?}` · `delta {job, seq, text}` · `event {job, seq, payload}` ·
  `log {job, seq, text}` · `artifact {job, seq, manifest}` · `finish {job, seq, finish,
  usage?}` · `error {job?, code, message, retryable, through_seq?}` ·
  `gap {job, expected, got}` · `replay {job, from_seq}`. Only `error` may omit `job`,
  for a connection-level failure no single task owns. `meta`'s two extras (`state`,
  `attached_ms`) are filled by the face that opened the stream, from the job record it
  just read; a `meta` that arrives through the relay leaves them absent rather than
  guessing at them (`protocol.rs:644-668`).
- **One `end` per task.** Exactly one `end {job, reason, through_seq?}` follows every
  task this connection started, whatever happened in between (NEX-3.4). `reason` is
  `EndReason` (`protocol.rs:527-542`), spelled `finished` (a `finish` was delivered),
  `errored` (an in-band `error` was), `lagged` (this connection could not keep up,
  NEX-5.3), `detached` (this connection asked to stop watching; the task lives on under
  NEX-6.6) or `closed` (the connection is going away while the task is still live).
  `through_seq` is the highest seq delivered here, which is the cursor for a later
  `attach {from_seq}`.

**Consumer → hub (HTTP/SSE).** The OpenAI-compatible surface remains for clients that
cannot hold a WebSocket. An SSE response answers on the connection it was posted on —
§8.1.2's one-connection reading — and that response body **is** the subscription NEX-6.1
means, which is why the two completion routes carry no `detach` field at all:

- `POST /v1/chat/completions`, `POST /v1/completions` → an SSE response body (the stream)
- `POST /v1/jobs` → `202` + `GET /v1/jobs/{id}/events` (SSE) + `GET /v1/jobs/{id}/result`
  + `DELETE /v1/jobs/{id}`
- `GET /v1/jobs` (the jobs this hub currently holds a live relay for, filtered to the
  calling tenant) and `GET /v1/jobs/{id}` (one job's status)
- `GET /v1/models`, `GET /healthz`, `GET /metrics`
- bulk: `GET /v1/payloads/{id}` (supplier fetch), `GET|PUT /v1/artifacts/{id}` (§8.2)

On `POST /v1/jobs` the NEX-6.6 contract is **required, not inferred**: the body must
carry `"detach": true`. An absent field or `"detach": false` is refused with
`400 detach_required` (`consumer.rs:514-522`). A route that answers `202` and hangs up
holds no subscription, so it can neither honour NEX-6.1's coupled contract — a coupled
task posted there would be orphaned and deleted before the client could poll it — nor
declare detachment on a silent client's behalf. Surfaces that do keep the connection
open are named in the refusal, because "then how do I stream the answer?" is the next
question a client asks.

Credentials are per direction and never shared: pool token for suppliers, API key for
consumers (including the pre-upgrade check on `GET /v1/consumer`), plus an attestation
allowlist for the model a supplier claims to have loaded.

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
5. **LOSS-2's "ended in band with a retryable error"** (§5.5) — read as the *end* being
   the requirement, not the retryability. The operative clause is **"the stream is ended
   in band"**: the consumer is never left in silence over a stream the hub knows is
   broken. "A retryable error" describes the shape of that terminal (a typed `error`
   event with a code, a message and a `through_seq`), not a promise that re-sending the
   task reproduces the answer. Once bytes have reached the consumer, an automatic blind
   retry would run the task a second time and hand back a duplicated answer, which is
   exactly what NEX-2.4 refuses: delivery is **at-most-once to the consumer**, and
   at-least-once only **across retries by explicit request** — so the retry the hub owes
   here is the one the consumer asks for, by re-posting the task. Accordingly the
   in-band terminal for a mid-stream supplier death carries `retryable: false`
   (`ConsumerEvent::Error` in `actor.rs:2855-2862`;
   `TerminalReason::WorkerGonePartial` is deliberately outside
   `TerminalReason::retryable()`'s set, `model.rs:273-284`), because "retryable" on this
   surface means "the answer can be reproduced without duplicating output", and after
   partial delivery it cannot. With `max_continuations = 0` — the default, and the
   reading this document's §5.5 assumes — **every** mid-stream death takes this in-band
   path (`limits.rs:68`, `:156`); resume-by-continuation (task brief §7.4) is an operator
   opt-in that turns one specific case into a seam instead of a truncation, and it is
   not a change to LOSS-2.
6. **Idempotency is a dedupe table, not a result cache.** The task brief's §5.2 ("a
   duplicate submission must attach to the existing job … never enqueue a second
   execution") is the requirement; §6 item 4 keeps result *caching* a non-goal, and
   NEX-7.2 requires every retained byte to have a stated reason. The hub therefore keeps
   a **bounded idempotency mirror** — key → (pool, job, expiry) on the hub
   (`state.rs:237-259`) plus the actor's own table and expiry index (`actor.rs:481-482`) —
   bounded by
   `max_idempotency_entries` (4 096) and `idempotency_ttl_ms` (600 s, must be > 0),
   pruned eagerly whenever the actor forgets a job (`Effect::JobForgotten` →
   `forget_idempotency_for_job`, `state.rs:1065-1082`), so a same-key retry **within**
   TTL is answered from the job the hub already routed rather than by re-executing it.
   That is a keyed *join*, not a §6.4 cache: the entry names one live-or-just-finished
   execution, it is deleted with that execution (NEX-6.3's "no idempotency entry that
   will hand the same answer to a later stranger"), it expires on its own, and two
   different consumers asking the same question without a shared key still get two
   executions.

### 8.2 Deliberate extensions of the brief

- **Bulk artifact egress** (NEX-7.4). The package supports non-text results — images,
  video, TTS — of "tens of megabytes". Holding those in RSS while a supplier uploads and
  a consumer downloads at unpredictable speeds is a worse engineering choice than
  streaming them to disk with a TTL. This is the **only** on-host persistence in the
  design, it is opt-out (artifact kinds only, never task payloads), and it is recorded as
  a deviation from the literal "keeps nothing" of NEX-7 rather than folded into it
  silently. The opt-out is a switch, not a sentiment: `artifact_root = ""` is the
  operator's explicit "no scratch disk here" — startup's `io::ensure_root` (`main.rs:83`)
  creates nothing for an empty path, so the hub comes up with no directory at all — and a
  kind that would have written an artifact is refused **at admission** with
  `400 artifact_unavailable` before any body byte (`actor.rs:993-1005`, asserted by
  `memory_only_mode_refuses_a_kind_that_writes_an_artifact_and_admits_the_rest`,
  `acceptance.rs:2818`) rather than failing on the supplier's first upload. What the
  default path does write, it writes bounded and transiently: one append-only staging
  file per attempt, `{object}.parts/a{attempt:04}/staging.bin` (`io.rs:759`), fed with
  O(chunk) memory into a running SHA-256 the hub measures rather than trusts
  (`io::stage_chunk`, `io.rs:809`), published by an atomic **rename** into the object
  path (`io.rs:918`) so a download never sees a half-written file and completion never
  copies the bytes twice; the TTL sweep's single `io::delete_artifact` removes the object
  **and** that parts directory (`io.rs:937-951`).
- **Operational surface** (`/healthz`, `/metrics`, `Idempotency-Key`, `Last-Event-ID`,
  tenancy caps, size caps, backpressure) is specified elsewhere (the task brief §8, §9)
  and is not restated as a requirement here. Where an operational bound exists to protect
  NEX-5.3 or NEX-7.2, it is named in §9's evidence.

## 9. Conformance status of the current build

Measured against `packages/underload/nexus` at the working tree of this commit. Every
row cites the code that decides the answer; nothing here is inferred from the README, and
every line number was re-walked in this tree rather than carried over from the last audit.

**Summary: all ten rows met** — nine outright, and **NEX-7** under the NEX-7.4 artifact
exception, which §8.2 records and which an operator can now switch off completely. The
five open items the previous version of this section listed are closed (§9.1). What
remains is stated as limits, not averaged away: the cooperative supplier `Cancel`, the
flush-window caveat that moves task §16.3's "killed before any byte" into the LOSS-2
branch, and the latency and RSS numbers nobody has measured.

| Rule | Status | Evidence and gaps |
| --- | --- | --- |
| **NEX-1** Rust server, compiled from Rust | **Met** | `Cargo.toml` at the package root declares the `[workspace]` on purpose, so cargo builds this package alone; `crates/nexus-server/src/main.rs:38-104` is the one binary, and `yarn build` runs `cargo build --release -p nexus-server` — §9.2 records the native ELF that produced, not the script that promises it. `crates/nexus-core/src/lib.rs:9` states the spine's contract ("**synchronous and IO-free**: `PoolActor::handle(&mut self, msg) -> Vec<Effect>`") and the crate keeps it, which is why the same scheduling rules also compile for `wasm32-unknown-unknown` (`crates/nexus-wasm`; §9.2's `cargo check -p nexus-wasm --target wasm32-unknown-unknown` row). No JavaScript participates in any request path: the only JS here is generated `wasm-bindgen` glue and two dev scripts, and the wasm host performs I/O only (§3). |
| **NEX-2** hub streams between supplier and consumer, exactly-one-owner, opaque content | **Met** | One actor per pool owns that pool's queue, leases and jobs and is its only mutator (`state.rs:335` documents the rule, `:378` spawns `run_actor` per pool). Exactly-one-owner-at-a-time: claim authority is granted in mailbox order (`on_claim` `actor.rs:1860`, `fulfil_claims` `:1908`); a frame from a retired connection is dropped and counted (`on_frame` `:2194`, stale check `:2205`); a frame from a worker that no longer owns the job — or no longer owns it **on this attempt** — is refused and the sender is told to stop (`:2213`). Asserted by `t01`, `t02` and `the_actor_never_reports_two_owners_for_one_job`. **NEX-2.3** content is opaque: the relay copies `delta` text field-for-field (`relay_into` `:3379`), and every consumer-facing dialect lives in `adapters.rs` or in the two consumer faces, never in the actor (`t14` `acceptance.rs:1066`). **NEX-2.4** is the reading §8.1.5 spells out: dispatch once, requeue only under §5.5, tell the consumer in band when the answer is not trustworthy. **NEX-2.5** both directions are live — consumer loss ⇒ `Detach` ⇒ `Cancel` (`on_detach` `:1296` → `on_cancel` `:1449`, executed over real sockets by `runtime_e2e.rs:465`), supplier loss ⇒ an in-band `error` the consumer actually receives (`resolve_lost_job` `:2740+`). |
| **NEX-3** consumer posts and streams over a **WebSocket on one connection** | **Met** — via the consumer WebSocket; the SSE surface stays for OpenAI-compatible clients | `GET /v1/consumer` (`main.rs:135` → `consumer_ws::upgrade` `consumer_ws.rs:116`) is the face §5.1 describes, spelled in §7. **NEX-3.1** one socket, both directions: the read loop (`session` `:215`) takes `submit` (`handle_submit` `:353`) and the same connection's single writer (`:226`) streams that task's pump (`pump` `:803`) — driven end to end by `runtime_e2e::dial_consumer` (`:589`). **NEX-3.2** several tasks per connection, each frame tagged: `HubConsumerFrame::relayed` (`protocol.rs:774`) stamps every relayed event with its `job`, and `one_consumer_socket_carries_two_tasks_and_answers_both_in_order` (`runtime_e2e.rs:694`) asserts two interleaved streams on one socket, each resolvable by id. **NEX-3.3** verdict before bytes: one writer task owns every write, so wire order is queue order (`consumer_ws.rs:135-143`), `handle_submit` does not return until the verdict is queued (`:350-353`), and the next message is not read before that happens — `a_refusal_reaches_the_consumer_before_any_event_bytes` (`runtime_e2e.rs:880`) pins it. **NEX-3.4** never silence: exactly one `end {job, reason}` per task this connection started, with `reason` from `EndReason` (`protocol.rs:522-542`). Authentication precedes the upgrade — `401` + `www-authenticate: Bearer realm="nexus"` (`consumer_ws.rs:121-131`), pinned by `the_consumer_socket_refuses_an_unauthenticated_upgrade` (`runtime_e2e.rs:1523`); §7 records that rule, which the code labels NEX-3.5. Stated honestly: the SSE face is still one-way and half-duplex, so on its own it satisfies NEX-3.1 only partially and NEX-3.2 not at all — it exists for clients that cannot hold a socket, and the socket face is what the rule is measured against. |
| **NEX-4** supplier opens a WebSocket, looks up work it can do, reads the task, streams the result | **Met** | `/v1/worker` upgrade (`worker_ws.rs:47`). **NEX-4.1** `Hello` (`protocol.rs:298-303`) declares pools, capacity, `requires` and attestation, and `on_hello` (`actor.rs:1500`) checks it against the pool floor and the model allowlist before the worker joins; a self-declared model name is never identity (`t13*`, attestation). **NEX-4.2** work selection is a pull: `Claim` (`protocol.rs:335`) asks for up to N, and `pick_job_for` (`actor.rs:2020`) answers only with tasks that worker can actually do — `requires.satisfied_by(worker.requires)`, phase `Queued`/`Pending`, KV headroom, affinity. **NEX-4.3** `Assigned` (`protocol.rs:424`) carries the whole readable task: kind, resolved requirements, payload inline or by `payload_ref`, the deadlines, `start_seq` (`:446`) and the artifact upload slot (`:455`). **NEX-4.4** `Frame{job, seq, kind, text}` + `Done` (`protocol.rs:341`, `:359`) with duplicates and reordering detected by the seq machine, reported in band as a `gap` rather than patched (§5.2). **NEX-4.5/4.6** leases renew on heartbeats only and dispatch is `Ready`-gated: `t11_a_loading_worker_never_receives_assigned` (`acceptance.rs:941`), `t12_*` (`:962`) and the runtime reading of the same rule (`runtime_e2e.rs:259`). Caveat, unchanged and still permitted by §8.1.3: lookup and lease are fused — a supplier cannot enumerate or peek available tasks without claiming one. |
| **NEX-5** the consumer receives bytes while the supplier is still producing | **Met, with a stated window** | Every accepted `Frame` is relayed in the same mailbox step (`on_frame` → `Effect::Relay` `actor.rs:2286`) and fanned out to that job's subscribers, on the socket face and on SSE alike. Outbound is gated, not per-token (`FlushGate` `relay.rs:153`, `should_flush` `:191`): `flush_bytes = 1_536` or `flush_interval_ms = 75`, whichever first, plus an age-based flush and a `forced` bypass so a terminal never queues behind a window (`limits.rs:162-163`) = **NEX-5.1**. **NEX-5.2** verbatim, with honest gap reporting: a seq jump raises `SeqVerdict::Gap` and a `gap{expected, got}` event (`actor.rs:2251-2253`; `HubConsumerFrame::Gap` `protocol.rs:739-746`), and `worker_relay_reaches_the_consumer_verbatim_over_sse` (`runtime_e2e.rs:311`) is the executed byte-for-byte proof — one chunk in, the same chunk out. **NEX-5.3** nothing grows without bound: replay rings are bounded twice (256 frames / 1 MiB, `limits.rs:164-165`), a consumer that cannot keep up is ended **in band with a reason** — `consumer_lagged` on SSE (`consumer.rs:1367`), `end{reason:"lagged"}` on the socket (`consumer_ws.rs:874`) — and each connection's outbound queue is capped by `WRITER_QUEUE` (`consumer_ws.rs:97-101`), so a stalled consumer feels it as a paused stream instead of stalling the pool. Honest limit, restated rather than hidden: a delta accepted inside the flush window already counts as delivered, which shifts task §16.3's "killed before any byte" into the in-band-terminal branch (see LOSS-1). |
| **NEX-6** consumer gone ⇒ task orphaned, deleted, supplier stream terminated | **Met, with one documented boundary** | **NEX-6.1** the loss is noticed **by the connection**: the socket face breaks its read loop on `None`/`Err` — a client FIN or a broken frame — and then detaches every task it was watching (`consumer_ws.rs:253-262`, teardown at `:329` and `:342`); the SSE face races the event source against `tx.closed()`, which hyper resolves the moment the client hangs up even while nothing is streaming (`consumer.rs:1358`, `an_idle_sse_hangup_cancels_the_worker_without_a_failed_write` `runtime_e2e.rs:1613`). **NEX-6.2/6.4** termination is automatic and needs no goodbye: `on_detach` (`actor.rs:1296`) acts the moment the last consumer leaves, `on_cancel` (`:1449`) tells the lease holder first and pulls the task out of the queue, and a frame that arrives afterwards is dropped rather than relayed (`post_cancel_frames` `:2232`) — executed over a real socket and a real worker by `consumer_disconnect_reaches_the_worker_as_cancel` (`runtime_e2e.rs:465`). **NEX-6.3** the orphan is **deleted**, not retained: for a record that was still running, `forget_job` (`:1382`) drops the record, the replay ring, the lease, the payload reference **and the idempotency entry** in that same mailbox step, and `Effect::JobForgotten` makes the embedder's routing, relay and idempotency mirrors agree (`state.rs:1065-1082`, which also closed an otherwise unbounded `job_index`) — so a later request with the same key executes fresh instead of attaching to a dead stranger's answer (`dropping_a_consumer_socket_cancels_and_deletes_its_task` `runtime_e2e.rs:953`). The one record retention still applies to is an **already terminal** job, and NEX-7.2 sanctions exactly that: the answer exists and a retry must not re-run it. **NEX-6.6** the exception is now opt-in on both faces: `submit{detach}` defaults to `false` on the socket (`protocol.rs:570-574`), and the connectionless route refuses to guess — `POST /v1/jobs` requires `"detach": true` and answers `400 detach_required` otherwise (`consumer.rs:514-522`, `the_async_job_route_requires_detach_to_be_stated` `runtime_e2e.rs:1704`); a detached job nobody ever watches is capped by `detached_orphan_ttl_ms` (300 s, validated `> 0`, `limits.rs:181`, `:412-417`) and forgotten by the sweep (`actor.rs:3232-3260`; `a_task_posted_without_a_consumer_is_deleted_when_its_window_closes` `runtime_e2e.rs:1397`). **The boundary:** that sweep matches only `Pending \| Queued`, so a detached job that is **already running** is not killed by it — it runs to its terminal, its lease or `job_hard_cap_ms`. That is "I will pick the result up later" read literally: the contract pays for the GPU and keeps the answer, rather than silently truncating it. Remaining stated limit, unchanged from before: supplier-side termination is **cooperative** — the hub sends `Cancel` and frees the slot on `CancelAck` (`on_cancel_ack` `:1480`) or lease expiry, so a supplier that ignores `Cancel` keeps burning its slot while its bytes reach nobody. |
| **NEX-7** nothing kept; everything in memory | **Met, under the §8.2 / NEX-7.4 exception** | **NEX-7.1** task payloads and result bytes are memory-only: `store.rs`'s module table states "Request payload / prompt text … **memory only** … never on disk", and `PayloadBook` is bounded by entry count **and** total bytes (`store.rs:127-142`), released at terminal or at `payload_ttl_ms = 120 s` (`limits.rs:169`). Deltas live in the bounded ring; terminal records are swept at `job_retention_ms = 300 s` (`limits.rs:175`); the idempotency tables are bounded by count and TTL (4 096 / 600 s, `limits.rs:139-140`) **and** pruned the moment the actor forgets the job (`state.rs:1081`); every TTL has a stated reason in `Limits` and a validator that refuses a zero with the consequence spelled out (`limits.rs:376-417`) — that is **NEX-7.2**. **NEX-7.3** a restart loses everything and says so retryably rather than corrupting (`t18_hub_shutdown_fails_clients_retryably_and_a_fresh_hub_resumes` `acceptance.rs:1437`). The one exception is artifact egress, and it is now bounded and switchable rather than a fait accompli: the book is capped by **count** as well as bytes (`MAX_ARTIFACT_ENTRIES = 128`, `store.rs:394`, evictions drained through the same TTL deletion path rather than a second effect), ingest is O(chunk) staging + streaming digest + atomic rename (§8.2), the object and its parts directory are deleted at `artifact_ttl_ms = 600 s` (`limits.rs:172` → `Effect::DeleteArtifact` `actor.rs:3199` → `state.rs:1047-1050` → `io::delete_artifact` `io.rs:944-951`), and `artifact_root = ""` refuses every kind that would write at **admission** instead (`actor.rs:993-1005`, `memory_only_mode_refuses_a_kind_that_writes_an_artifact_and_admits_the_rest` `acceptance.rs:2818`). Under a literal NEX-7.1 the default deployment still writes to disk: that is the deviation §8.2 records, not a claim that it does not happen. |
| **LOSS-1** supplier dies with **no** bytes ⇒ transparent requeue when idempotent, loud failure when not | **Met** | `resolve_lost_job` (`actor.rs:2740+`) branches on what the consumer actually received. Nothing delivered + idempotent + attempts left ⇒ requeued, and the consumer sees no error at all (`:2784-2790`) — `t03_worker_killed_before_any_output_is_requeued_and_completes_elsewhere` (`acceptance.rs:415`) asserts the two attempt-2 delta **texts**, not merely their lengths. A non-idempotent kind is never auto-requeued once assigned; it fails loudly as `non_idempotent_ambiguous` (`:2797`, `model.rs:294`), asserted by `non_idempotent_kinds_are_never_requeued_after_an_ambiguous_loss` (`acceptance.rs:1822`). The flush-window caveat still holds and is stated rather than smoothed: a delta accepted but still inside `FlushGate` sets `bytes_delivered`, so a kill within ≤ 75 ms of a flush takes the LOSS-2 branch instead. |
| **LOSS-2** supplier dies **after** bytes reach the consumer ⇒ ended in band | **Met, as interpreted in §8.1.5** | The pending batch is flushed, one `ConsumerEvent::Error{code: "worker_gone_partial", through_seq}` is relayed, and the job is finished (`actor.rs:2853-2868`): the consumer keeps the partial text, gets an explanation, and sees exactly one terminal with no duplicated deltas — `t04_worker_killed_after_output_terminates_in_band_with_one_finish` (`acceptance.rs:495`). `retryable` is `false` here on purpose and honestly (`:2860`; `TerminalReason::WorkerGonePartial` is deliberately outside `TerminalReason::retryable()`'s set, `model.rs:273-284`), because after partial delivery a blind automatic retry would duplicate output, which NEX-2.4 refuses; the retry this rule means is the consumer's explicit re-post. This is always the path taken by default, since `max_continuations` defaults to `0` (`limits.rs:68`, `:156`) — resume-by-continuation is an operator opt-in, not the conformance path. |
| **LOSS-3** a reconnected supplier resumes from the last acked frame — no re-run, no duplicate deltas | **Met** | `on_resume` (`actor.rs:2435`) honours a lease the worker still holds and resumes from the seq the hub acknowledged (`Seq::acked_through`, `seq.rs:72`); `Assigned` carries `start_seq` (`protocol.rs:446`) so the attempt continues the numbering instead of restarting it, and every frame at or below the acked seq is dropped as a duplicate. Asserted by `t08_reconnecting_worker_resumes_from_the_acked_seq_without_duplicates` (`acceptance.rs:737`) and by the resume half of `t15_artifact_upload_is_measured_not_trusted_and_survives_a_resume` (`:1126`); `resume_for_a_moved_or_finished_lease_is_nacked` pins the negative case. |

### 9.1 What closed the previous audit's open items

The five items this section listed as outstanding, and where each one stands:

1. **NEX-3** — closed by `crates/nexus-server/src/consumer_ws.rs` (1 005 lines): one
   connection, `submit` in, `accepted`/`rejected` then tagged `delta`/`event`/`log`/
   `artifact`/`finish`/`error`/`gap`/`replay` out, and exactly one `end` per task (§7).
   It is exactly the shape this section predicted: the socket turns messages into
   `ActorMsg` (`Submit`/`Attach`/`Detach`/`Cancel`, `actor.rs:197-222`) and turns
   `Effect::Relay` back into frames, so `PoolActor` did not change shape at all. The SSE
   surface stayed for OpenAI-compatible clients, and both faces share one admission path
   so the same task is refused the same way.
2. **NEX-6.3** — closed: `forget_job` (`actor.rs:1382`) drops the record, the replay ring
   and the idempotency entry in the same mailbox step as the cancel, instead of arming
   `job_retention_ms`, and `state.rs:1065-1082` makes the hub-side mirrors agree with the
   actor. That second half also closed a slow leak this section had not flagged:
   `job_index` was otherwise one entry per submission for the life of the process.
3. **NEX-6.6** — closed: detachment is a stated request field on both faces (default
   `false` on the socket, required `true` on the route that holds no subscription), and a
   detached job with no subscriber is capped by `detached_orphan_ttl_ms` rather than
   living forever.
4. **NEX-6.1** — closed: the notice comes from the connection — the socket read loop, and
   `tx.closed()` on the SSE body — rather than from the first write that happens to fail.
5. **NEX-7.4** — closed on its own terms: root, TTL, byte bound and count bound are
   stated in one place (§8.2) and enforced in `store.rs` / `limits.rs` / `io.rs`, and
   `artifact_root = ""` is the in-memory-only mode this section asked an operator be
   given.

What is genuinely left, none of it a requirement of this document: an in-place resume for
`Last-Event-ID` on the chat routes (replay is available to the job API and to
`attach{from_seq}`, while a chat reconnect still allocates a new job);
`scheduler::{eligible, rank, pick}` is complete and unit-tested but uncalled at dispatch —
`affinity_target` is the one scheduler entry point the actor uses; RSS during an artifact
download and task §14's 30–60 ms proxy budget are still unmeasured; and a supplier that
ignores `Cancel` keeps its slot until its lease lapses.

### 9.2 Verification of this audit

- Read against the working tree, not against `README.md`'s claims: every row above cites
  the deciding code path, and each line number was re-walked in this tree.
- **Executed on this host, Linux, 2026-11-10.** Toolchain `rustc 1.99.1` / `cargo 1.99.1`,
  the `1.99.1` pin in the repo-root `rust-toolchain.toml`, built with the repo-root
  `.cargo/config.toml` in force (`target-dir = .rust/target`).
  - `cargo test --workspace` → **163 passed / 0 failed**: `nexus-core` lib **86**,
    `crates/nexus-core/tests/acceptance.rs` **46**, `nexus-server` bin **31** (19
    `runtime_e2e` over a real listener + a real worker WebSocket + real consumer SSE and
    consumer WebSocket bytes, 7 `io::tests` against real files, 5 `state::tests`),
    `nexus-wasm` and `nexus-worker-example` 0 tests, doc-tests 0.
  - `cargo fmt --check` → **clean**.
  - `cargo clippy --workspace --all-targets -- -D warnings` → **exit 0, zero findings**.
    The 17 pre-existing `nexus-core` denials this section used to record — and the
    `yarn verify` chain abort that followed from them — are gone.
  - `cargo build --release -p nexus-server` → `.rust/target/release/nexus-hub`, a
    **3.25 MiB stripped ELF** (3 403 096 bytes): NEX-1's native-executable clause,
    observed rather than asserted.
  - `cargo check -p nexus-wasm --target wasm32-unknown-unknown` → **ok**, which is what
    keeps NEX-1's portability clause true rather than merely intended.
- **Cold-cache re-verification, also 2026-11-10:** the same tree, copied to a pristine
  location with a fresh target directory and built `--locked`, reproduced every result
  above — so the green suite is not an artifact of an incremental target directory, and
  the dependency graph is the one `Cargo.lock` pins.
- Which runs support which rows: `one_consumer_socket_carries_two_tasks_and_answers_both_in_order`,
  `a_refusal_reaches_the_consumer_before_any_event_bytes`,
  `a_finished_task_can_be_followed_again_on_the_same_socket` and
  `the_consumer_socket_refuses_an_unauthenticated_upgrade` are NEX-3.1/.2/.3 and §7's
  pre-upgrade auth rule; `dropping_a_consumer_socket_cancels_and_deletes_its_task`,
  `an_explicit_detach_ends_the_stream_and_keeps_the_task_alive`,
  `an_explicit_cancel_closes_the_stream_and_keeps_the_record`,
  `a_task_posted_without_a_consumer_is_deleted_when_its_window_closes` and
  `an_idle_sse_hangup_cancels_the_worker_without_a_failed_write` are NEX-6.1/.3/.6;
  `the_async_job_route_requires_detach_to_be_stated` is §7's `detach_required` rule;
  `a_retry_on_an_idempotency_key_replays_the_original_completion`,
  `a_retry_on_an_idempotency_key_gets_the_task_that_already_exists`,
  `a_resubmitted_key_on_the_consumer_socket_follows_the_original_task` and
  `a_non_streaming_retry_on_a_key_assembles_the_running_answer`
  (`runtime_e2e.rs:1783`, `:1903`, `:1996`, `:2120`) are §8.1.6;
  `a_refused_submission_leaves_no_route_or_relay_behind` is NEX-7.2's mirror-hygiene
  half; `worker_relay_reaches_the_consumer_verbatim_over_sse` is NEX-5.2; `t03`/`t04`/`t08`
  are LOSS-1/.2/.3; `t11`/`t12*`/`t13*` are NEX-4.2/.5/.6;
  `memory_only_mode_refuses_a_kind_that_writes_an_artifact_and_admits_the_rest` is §8.2.
- **Still not executable on this host, said plainly rather than implied:** `wasm-bindgen`
  / `wasm-bindgen-cli` are absent from the toolchain, so the Node glue is never generated
  (`pkg/` does not exist). `scripts/build-wasm.mjs` and `examples/node-smoke.mjs` have
  **still never run**, and nothing in this section claims they did: the wasm *check*
  passing is a compile-level fact, not a runtime one.
- Not re-benchmarked: task §14's 30–60 ms proxy budget and RSS-during-artifact-download
  remain unmeasured, so NEX-5.1's bounded window is verified by constant and by test, not
  by a latency histogram.
