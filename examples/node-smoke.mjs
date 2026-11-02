// Node smoke test for the WASM build of the broker core.
//
// This is *glue*, not application logic: it drives the real Rust actor through its
// wasm-bindgen surface and asserts the same claims the Rust acceptance tests make
// (§16.1/2/6/10/12 plus the WASM-boundary claim that Node and Rust agree). It
// requires `yarn build:wasm` first, because the .wasm module and the glue JS are
// build artifacts, not sources.
//
// Wire shapes matter here, so the two tagging rules are written down once:
//   * `Effect` is **externally** tagged (no `#[serde(tag)]`): an accept is
//     `{"Accepted": {"job": …, "attached": …, "replayed": […]}}`;
//   * `HubFrame`, `WorkerFrame` and `ConsumerEvent` are **internally** tagged with
//     `{"type": "…"}`: a welcome is `{"type":"welcome","protocol":1,…}`, a relayed
//     delta is `{"type":"delta","seq":1,"text":"…"}`.
// Everything below reads frames through those discriminators.
//
// Run: yarn smoke:wasm
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";
import assert from "node:assert/strict";

const here = dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);
const gluePath = resolve(here, "..", "pkg", "nexus_wasm.js");

// Reference digests from services/molab/molab-strata-runtime.py:95-98 (SHARD_TABLE),
// so the WASM host attests exactly what the native runtime attests.
const SHARD_1 = "40f369eca301ec6ea318dbb671b3132c26a2f0b89aa5458a7051b33215ee296c";
const SHARD_2 = "316b46f3a2dbd68c900f43136ab9449f9dcc3725dfd8c794847c204bc161e113";

/** The config the module is built with; identical to the Rust test fixture. */
const CONFIG_JSON = JSON.stringify({
    limits: {
        no_worker_grace_ms: 2000,
        resume_grace_ms: 1500,
        flush_interval_ms: 75,
        flush_bytes: 1536,
        inline_payload_cap_bytes: 64,
        // §7.4 is opt-in: a mid-stream death is terminal unless the operator asks
        // for a re-run, so this test asserts the disabled-by-default behaviour.
        max_continuations: 0,
    },
    auth: {
        allow_anonymous: true,
        worker_tokens: ["pool-token-a"],
        api_keys: [{ key: "sk-1", tenant: "acme" }],
        model_shas: [SHARD_1, SHARD_2],
    },
});

let broker;
try {
    const { NexusBroker } = require(gluePath);
    broker = new NexusBroker(CONFIG_JSON);
} catch (err) {
    if (String(err).includes("Cannot find module")) {
        console.error(`[smoke] missing ${gluePath}\n[smoke] run: yarn wasm && yarn wasm:glue`);
        process.exit(2);
    }
    throw err;
}

// ------------------------------------------------------------------ helpers

const helloFrame = (capacity = 4) => ({
    type: "hello",
    worker_id: "wkr-1",
    pools: ["llm.chat/qwen38"],
    capacity,
    requires: { model: "qwen38", min_context_tokens: 524288, capabilities: ["tools"] },
    attestation: { model_shas: [SHARD_1, SHARD_2], weight_bytes: 83863586496 },
    protocol: 1,
    engine: "strata",
    tier: "dedicated",
});

const readyFrame = (slots = 4, engineReady = true) => ({
    type: "ready",
    free_slots: slots,
    free_kv_tokens: 524288,
    engine_ready: engineReady,
});
const claimFrame = (jobs = 4) => ({ type: "claim", jobs });
const deltaFrame = (job, seq, text) => ({ type: "frame", job, seq, kind: "delta", content_type: "application/json", text });
const doneFrame = (job, seq) => ({ type: "done", job, seq, finish: "stop", usage: { completion_tokens: 3 } });

/** A `tool_calls` fragment, exactly as an OpenAI-speaking worker would relay it. */
const TOOL_CALL_CHUNK = JSON.stringify({
    choices: [{ index: 0, delta: { tool_calls: [{ index: 0, id: "c1", function: { name: "get_weather", arguments: '{"ci' } }] }, finish_reason: null }],
});

/** Build a Submission through the module's own adapter, so JS and Rust agree. */
function submission(text = "hi", key = "k1", session = null) {
    return JSON.parse(broker.submissionFromChat(JSON.stringify({ model: "qwen38", messages: [{ role: "user", content: text }], stream: true }), "acme", key, session));
}

const effects = (json) => JSON.parse(json);
/** Hub frames sent to one worker, with their internal `type` discriminator. */
const sentTo = (list, worker) => list.filter((e) => e.SendWorker && e.SendWorker.worker === worker).map((e) => e.SendWorker.frame);
const framesOfType = (list, worker, type) => sentTo(list, worker).filter((f) => f.type === type);
/** Every `Assigned` as `[worker, job, attempt]`. */
const assigned = (list) =>
    list
        .filter((e) => e.SendWorker?.frame?.type === "assigned")
        .map((e) => [e.SendWorker.worker, e.SendWorker.frame.job, e.SendWorker.frame.attempt]);
const rejects = (list) => list.filter((e) => e.Reject).map((e) => [e.Reject.status, e.Reject.code]);
const relayed = (list) => list.flatMap((e) => (e.Relay ? e.Relay.events : []));
const deltas = (events) => events.filter((e) => e.type === "delta").map((e) => e.text);
const health = () => JSON.parse(broker.healthBody());

// --------------------------------------------------------------------- tests

// 1. Handshake is accepted on a real attestation, and `Welcome` carries the
//    operating parameters — the same values the Rust `Welcome` effect produces.
{
    const out = effects(broker.hello("wkr-1", JSON.stringify(helloFrame()), 0));
    const [welcome] = framesOfType(out, "wkr-1", "welcome");
    assert.ok(welcome, `expected a Welcome, got ${JSON.stringify(out)}`);
    assert.equal(welcome.protocol, 1);
    assert.equal(welcome.lease_ms, 60000, "lease default matches the Rust config");
    assert.deepEqual(welcome.pools, ["llm.chat/qwen38"]);
}

// 2. Readiness must be reported, never assumed: a worker that has not sent
//    `Ready{engine_ready:true}` is loading and never receives `Assigned`.
{
    assert.equal(health().ready_workers, 0);
    const queued = effects(broker.submit("job-a", JSON.stringify(submission("hi", "kA")), 10));
    assert.deepEqual(rejects(queued), [], "capability is satisfiable, so no 400");
    assert.equal(queued[0].Accepted.attached, false);
    const claimed = effects(broker.workerFrame("wkr-1", JSON.stringify(claimFrame(4)), 20));
    assert.deepEqual(assigned(claimed), [], "a loading worker is never dispatched to (§16.11)");

    effects(broker.workerFrame("wkr-1", JSON.stringify(readyFrame()), 30));
    assert.equal(health().ready_workers, 1);
    const retried = effects(broker.workerFrame("wkr-1", JSON.stringify(claimFrame(4)), 31));
    assert.deepEqual(assigned(retried), [["wkr-1", "job-a", 1]], "the retained claim authority pays off (§16.11)");
}

// 3. Declared capacity is respected: one 4-slot worker never gets a 5th job.
{
    for (const n of [2, 3, 4, 5]) {
        effects(broker.submit(`job-${n}`, JSON.stringify(submission(`t${n}`, `k${n}`)), 40 + n));
    }
    const got = assigned(effects(broker.workerFrame("wkr-1", JSON.stringify(claimFrame(8)), 50)));
    assert.equal(got.length, 3, `expected the three remaining slots, got ${JSON.stringify(got)}`);
    assert.equal(health().queue_depth, 1, "the fifth job stays queued (§16.12)");
}

// 4. Deltas relay verbatim, batched by the flush window rather than per token
//    (§7.2): a small delta is held, the age timer releases it, and `usage` is
//    relayed untouched.
{
    const out = effects(broker.workerFrame("wkr-1", JSON.stringify(deltaFrame("job-a", 1, TOOL_CALL_CHUNK)), 60));
    assert.equal(sentTo(out, "wkr-1").some((f) => f.type === "ack" && f.seq === 1), true, "the hub acks the contiguous prefix");
    assert.equal(relayed(out).length, 0, "150 bytes is inside the 1536-byte / 75 ms window, so nothing is flushed yet");

    const aged = effects(broker.tick(140));
    const events = relayed(aged);
    assert.equal(events.length, 1, `the age timer must release the batch, got ${JSON.stringify(events)}`);
    assert.equal(events[0].text, TOOL_CALL_CHUNK, "verbatim relay, no re-tokenization (§16.14)");

    const usage = effects(broker.workerFrame("wkr-1", JSON.stringify(doneFrame("job-a", 2)), 160));
    const finish = relayed(usage).find((e) => e.type === "finish");
    assert.ok(finish, `expected a finish event, got ${JSON.stringify(relayed(usage))}`);
    assert.deepEqual(finish.usage, { completion_tokens: 3 }, "usage is relayed untouched");
}

// 5. A consumer that re-attaches with Last-Event-ID gets the missing frames from
//    the ring buffer (§16.9), and only that consumer sees the replay.
{
    const out = effects(broker.attach("job-a", "late-consumer", 0, 200));
    const replay = out.find((e) => e.ReplayTo);
    assert.ok(replay, "expected a replay");
    assert.equal(replay.ReplayTo.lossy, false, "the ring still holds both frames");
    assert.deepEqual(deltas(replay.ReplayTo.events), [TOOL_CALL_CHUNK], "replay carries the buffered delta once, then the terminal event");
    assert.equal(replay.ReplayTo.events.filter((e) => e.type === "finish").length, 1, "a joiner to a finished job sees its one finish");
}

// 6. Capability pre-check: an unsatisfiable request is an immediate 400, not a
//    timeout behind a model that had to load 50 GB (§16.10).
{
    const requiresVision = submission("hi", "kVision");
    requiresVision.requires.capabilities = ["vision"];
    const out = effects(broker.submit("job-vision", JSON.stringify(requiresVision), 210));
    assert.deepEqual(rejects(out), [[400, "capability_unsatisfied"]]);
}

// 7. A digests-outside-the-allowlist worker cannot join the pool (§16.13), and a
//    wrong protocol version is a clean reject rather than undefined behaviour.
{
    const rogue = { ...helloFrame(1), worker_id: "wkr-rogue", attestation: { model_shas: ["0".repeat(64)] } };
    const out = effects(broker.hello("wkr-rogue", JSON.stringify(rogue), 220));
    assert.deepEqual(sentTo(out, "wkr-rogue").map((f) => f.type), ["reject", "bye"]);
    assert.equal(sentTo(out, "wkr-rogue")[0].code, "attestation_rejected");

    const old = { ...helloFrame(1), worker_id: "wkr-old", protocol: 2 };
    const outOld = effects(broker.hello("wkr-old", JSON.stringify(old), 221));
    assert.equal(sentTo(outOld, "wkr-old")[1].reason, "version_mismatch");
}

// 8. Cancellation fans out when the last consumer detaches from a live job
//    (§16.5). `job-a` is already terminal, so the still-assigned `job-2` is the
//    one that must be cancelled.
{
    const out = effects(broker.detach("job-2", "con-job-2", 230));
    assert.ok(sentTo(out, "wkr-1").some((f) => f.type === "cancel" && f.reason === "consumer_gone"), "cancel must reach the worker");
    assert.ok(out.some((e) => e.Terminal?.reason === "cancelled"));
    assert.equal(broker.metricValue("nexus_cancels_sent_total"), 1);
}

// 9. No body bytes before the verdict (§16.6). A connected-but-loading worker
//    satisfies the capability pre-check (so this is not a `400`) but is never
//    dispatched to, so the tick after the grace window must answer
//    `503 + Retry-After`.
{
    effects(broker.workerGone("wkr-1", 240));
    const joined = effects(broker.hello("wkr-slow", JSON.stringify({ ...helloFrame(2), worker_id: "wkr-slow" }), 240));
    assert.ok(framesOfType(joined, "wkr-slow", "welcome").length === 1, "a cold worker may join the pool; it just cannot be dispatched to");
    assert.equal(health().ready_workers, 0, "connected is not ready (§16.11)");

    const out = effects(broker.submit("job-cold", JSON.stringify(submission("hi", "kCold")), 241));
    assert.deepEqual(rejects(out), [], "inside the grace window the answer is still open");
    assert.equal(relayed(out).length, 0, "no body bytes may be spent before the verdict");
    // The grace expires at 241 + 2000; the engine never finishes loading.
    const late = effects(broker.tick(4_000));
    const rejected = late.filter((e) => e.Reject);
    assert.ok(rejected.some((e) => e.Reject.status === 503 && e.Reject.code === "no_ready_worker"), `expected a 503, got ${JSON.stringify(rejected)}`);
    assert.ok(rejected.every((e) => e.Reject.retry_after_ms > 0), "a 503 must carry Retry-After");
    assert.equal(relayed(late).length, 0, "a pre-body rejection never leaks an in-band event");
}

// 10. Metrics and health come from the same actor that decided everything, so the
//     numbers here are the numbers the Rust tests assert.
{
    const accepted = broker.metricValue("nexus_jobs_accepted_total");
    assert.ok(accepted >= 6, `expected the accepted jobs, saw ${accepted}`);
    assert.equal(broker.metricValue("nexus_frames_duplicate_total"), 0);
    const text = broker.metricsText();
    assert.match(text, /nexus_frames_total\{kind="delta"\} \d+/);
    assert.match(text, /nexus_queue_depth \d+/);
    // The worker left before this tick, and its orphan grace had already expired,
    // so the pool reports "no workers" rather than a cold start.
    assert.equal(health().capacity_signal, "no_workers", "a pool with nobody connected is not a cold start");
    assert.deepEqual(JSON.parse(broker.modelsDocument(1700000000000)).data, [], "no warm worker, no advertised model (§7.1)");
}

// 11. The codec is shared: a JS-built frame round-trips through the Rust caps, and
//     an oversize frame is refused with the same error the native runtime raises.
{
    const decoded = JSON.parse(broker.decodeWorkerLine(JSON.stringify(claimFrame(2))));
    assert.deepEqual(decoded, { type: "claim", jobs: 2 });
    assert.throws(() => broker.decodeWorkerLine(JSON.stringify(deltaFrame("job-a", 1, "x".repeat(300 * 1024)))), /exceeds/);
    const encoded = broker.encodeHubFrame(JSON.stringify({ type: "bye", reason: "shutdown" }));
    assert.deepEqual(JSON.parse(encoded), { type: "bye", reason: "shutdown" });
}

console.log("[smoke] 11 WASM-boundary assertion blocks passed against the real pool actor");
