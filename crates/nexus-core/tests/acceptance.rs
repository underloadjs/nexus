//! Task §16 acceptance matrix, driven against the **real** pool actor — the
//! thing under test is never mocked. Everything is deterministic: time is
//! injected and claim order is mailbox order, so each test asserts exact effects.
//!
//! The native runtime's real WS/SSE round trip lives in `crates/nexus-server`
//! (`src/runtime_e2e.rs`), which dials a real listener with a real WebSocket
//! client; both this file and that one have now been executed (`cargo test
//! --workspace`, see README "Verification status"). What this file proves is the
//! load-bearing part — mutual exclusion, leases, cancellation, de-duplication,
//! replay, admission, payload spill and relay fidelity.

// The workspace lints (`unwrap_used`/`expect_used`, Cargo.toml) exist for the
// shipped paths, and the policy comment says tests "are allowed to be blunt".
// They are applied to test targets too, so the exception has to be spelled out
// here: an `expect` in an acceptance test *is* the assertion.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;

use nexus_core::adapters::assemble_completion;
use nexus_core::adapters::KIND_CHAT;
use nexus_core::config::BrokerConfig;
use nexus_core::ids::ArtifactId;
use nexus_core::ids::ConsumerId;
use nexus_core::ids::JobId;
use nexus_core::ids::PoolId;
use nexus_core::ids::TenantId;
use nexus_core::ids::WorkerId;
use nexus_core::model::EngineState;
use nexus_core::model::JobPhase;
use nexus_core::model::Priority;
use nexus_core::model::Submission;
use nexus_core::model::TerminalReason;
use nexus_core::model::WorkerTier;
use nexus_core::protocol::Attestation;
use nexus_core::protocol::ByeReason;
use nexus_core::protocol::CancelReason;
use nexus_core::protocol::FinishKind;
use nexus_core::protocol::FrameKind;
use nexus_core::protocol::HubFrame;
use nexus_core::protocol::JobPayload;
use nexus_core::protocol::Requires;
use nexus_core::protocol::WorkerFrame;
use nexus_core::ActorMsg;
use nexus_core::ConsumerEvent;
use nexus_core::Effect;
use nexus_core::HelloInput;
use nexus_core::PoolActor;
use serde_json::json;
use serde_json::Value;

/// Reference digests from SHARD_TABLE (services/molab/molab-strata-runtime.py:95-98).
const SHARD_1: &str = "40f369eca301ec6ea318dbb671b3132c26a2f0b89aa5458a7051b33215ee296c";
const SHARD_2: &str = "316b46f3a2dbd68c900f43136ab9449f9dcc3725dfd8c794847c204bc161e113";

fn pool() -> PoolId {
    PoolId::new_unchecked("llm.chat/qwen38")
}

/// Task defaults, with windows shrunk where a test needs to cross a deadline.
fn config() -> BrokerConfig {
    let mut cfg = BrokerConfig::default();
    cfg.auth.allow_anonymous = true;
    cfg.auth.model_shas = BTreeSet::from([SHARD_1.to_string(), SHARD_2.to_string()]);
    cfg.limits.no_worker_grace_ms = 2_000;
    cfg.limits.resume_grace_ms = 1_500;
    cfg.limits.heartbeat_ms = 20_000;
    cfg.limits.lease_ms = 60_000;
    cfg.limits.flush_interval_ms = 75;
    cfg.limits.flush_bytes = 1_536;
    cfg.limits.inline_payload_cap_bytes = 64;
    cfg.limits.max_spilled_payloads = 8;
    cfg.limits.max_spill_bytes = 4_096;
    cfg.limits.replay_frames = 64;
    cfg.limits.replay_bytes = 65_536;
    cfg.limits.job_retention_ms = 30_000;
    cfg.limits.tenant_max_payload_bytes = 4_096;
    cfg.limits.tenant_max_concurrent = 4;
    cfg.limits.max_continuations = 0;
    cfg.validate().expect("config must validate");
    cfg
}

fn actor() -> PoolActor {
    PoolActor::new(pool(), config())
}

fn hello_msg(worker: &str, conn: u64, capacity: u32) -> ActorMsg {
    ActorMsg::Hello {
        worker: WorkerId::new_unchecked(worker),
        conn,
        hello: HelloInput {
            protocol: 1,
            pools: vec![pool().to_string()],
            capacity,
            requires: Requires {
                model: Some("qwen38".to_string()),
                min_context_tokens: Some(524_288),
                capabilities: BTreeSet::from(["tools".to_string()]),
                kv_estimate_tokens: None,
            },
            attestation: Attestation {
                model_shas: BTreeSet::from([SHARD_1.to_string(), SHARD_2.to_string()]),
                weight_bytes: Some(83_863_586_496),
            },
            tier: Some(WorkerTier::Dedicated),
            engine: Some("strata".to_string()),
        },
    }
}

fn frame(worker: &str, conn: u64, frame: WorkerFrame) -> ActorMsg {
    ActorMsg::WorkerFrame {
        worker: WorkerId::new_unchecked(worker),
        conn,
        frame,
    }
}

fn ready_msg(worker: &str, conn: u64, free_slots: u32) -> ActorMsg {
    frame(
        worker,
        conn,
        WorkerFrame::Ready {
            free_slots,
            free_kv_tokens: Some(524_288),
            engine_ready: true,
            telemetry: None,
        },
    )
}

fn claim_msg(worker: &str, conn: u64, jobs: u32) -> ActorMsg {
    frame(worker, conn, WorkerFrame::Claim { jobs: Some(jobs) })
}

fn delta_msg(worker: &str, conn: u64, job: &str, seq: u64, text: &str) -> ActorMsg {
    frame(
        worker,
        conn,
        WorkerFrame::Frame {
            job: JobId::new_unchecked(job),
            seq,
            kind: FrameKind::Delta,
            content_type: Some("application/json".to_string()),
            text: text.to_string(),
            artifact: None,
        },
    )
}

fn done_msg(worker: &str, conn: u64, job: &str, seq: u64, usage: Option<Value>) -> ActorMsg {
    frame(
        worker,
        conn,
        WorkerFrame::Done {
            job: JobId::new_unchecked(job),
            seq,
            finish: FinishKind::Stop,
            usage,
            error: None,
        },
    )
}

fn gone_msg(worker: &str, conn: u64) -> ActorMsg {
    ActorMsg::WorkerGone {
        worker: WorkerId::new_unchecked(worker),
        conn,
    }
}

fn chat_body(text: &str) -> Value {
    json!({ "model": "qwen38", "messages": [{ "role": "user", "content": text }], "stream": true })
}

/// A streaming submission for `job`. `kv_estimate_tokens` is 1024 so the KV
/// scheduling gate has a real number to compare against.
fn submit_msg(job: &str, key: &str, session: Option<&str>) -> ActorMsg {
    submit_msg_with(job, key, session, chat_body("hi"), None)
}

fn submit_msg_with(
    job: &str,
    key: &str,
    session: Option<&str>,
    body: Value,
    tenant: Option<&str>,
) -> ActorMsg {
    ActorMsg::Submit {
        job: JobId::new_unchecked(job),
        consumer: ConsumerId::new_unchecked(format!("con-{job}")),
        submission: Submission {
            kind: KIND_CHAT.to_string(),
            pool: pool(),
            tenant: TenantId::new_unchecked(tenant.unwrap_or("acme")),
            requires: Requires {
                model: Some("qwen38".to_string()),
                min_context_tokens: Some(524_288),
                capabilities: BTreeSet::new(),
                kv_estimate_tokens: Some(1_024),
            },
            payload: body,
            content_type: "application/json".to_string(),
            idempotency_key: key.to_string(),
            session_hint: session.map(str::to_string),
            priority: Priority::Default,
            idempotent: true,
            streaming: true,
            cancel_on_consumer_gone: true,
        },
    }
}

/// Connect a worker and bring it to `ready`, then let it claim `slots` jobs.
fn warm(a: &mut PoolActor, worker: &str, conn: u64, capacity: u32, now: u64) {
    a.handle(now, hello_msg(worker, conn, capacity));
    a.handle(now + 1, ready_msg(worker, conn, capacity));
}

/// `warm` for a pool the actor owns that is *not* the reference llm pool.
///
/// `Hello.pools` is the join request, and `PoolActor::resolve_pools` skips every
/// name that belongs to another actor, so a worker that only names
/// `llm.chat/qwen38` joins nothing here and the pool has zero capacity (t15's
/// ComfyUI host runs on `image.render/comfyui`).
fn warm_pool(a: &mut PoolActor, worker: &str, conn: u64, capacity: u32, pool: &str, now: u64) {
    let mut msg = hello_msg(worker, conn, capacity);
    if let ActorMsg::Hello { hello, .. } = &mut msg {
        hello.pools = vec![pool.to_string()];
    }
    a.handle(now, msg);
    a.handle(now + 1, ready_msg(worker, conn, capacity));
}

/// All `Assigned` frames as `(worker, job, attempt)`.
///
/// `Effect::SendWorker::frame` is a `Box<HubFrame>` (the effect enum boxes it so a
/// relay step does not copy ~430 bytes per frame), so every helper matches through
/// `frame.as_ref()` rather than matching the field directly.
fn assigned(effects: &[Effect]) -> Vec<(String, String, u32)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::SendWorker { worker, frame } => match frame.as_ref() {
                HubFrame::Assigned { job, attempt, .. } => {
                    Some((worker.to_string(), job.to_string(), *attempt))
                }
                _ => None,
            },
            _ => None,
        })
        .collect()
}

/// Events relayed to consumers (live relay only).
fn relay_events(effects: &[Effect]) -> Vec<ConsumerEvent> {
    effects
        .iter()
        .flat_map(|e| match e {
            Effect::Relay { events, .. } => events.clone(),
            _ => Vec::new(),
        })
        .collect()
}

/// Frames sent to one worker.
///
/// The output lifetime is spelled out because there are two input lifetimes
/// (`effects` and `worker`) and rustc cannot infer which one the frames borrow.
fn sent_to<'a>(effects: &'a [Effect], worker: &str) -> Vec<&'a HubFrame> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::SendWorker { worker: w, frame } if w.as_ref() == worker => Some(frame.as_ref()),
            _ => None,
        })
        .collect()
}

fn rejects(effects: &[Effect]) -> Vec<(u16, String, Option<u64>)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Reject {
                status,
                code,
                retry_after_ms,
                ..
            } => Some((*status, code.clone(), *retry_after_ms)),
            _ => None,
        })
        .collect()
}

fn terminals(effects: &[Effect]) -> Vec<(String, TerminalReason, bool)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Terminal {
                job,
                reason,
                retryable,
            } => Some((job.to_string(), *reason, *retryable)),
            _ => None,
        })
        .collect()
}

/// Job ids the actor reported as **deleted** (`Effect::JobForgotten`).
///
/// NEX-6.3's "the task is then deleted" is only observable through this effect: the
/// hub's `job_index` and fan-out maps are keyed by job id and the actor is the only
/// thing that knows a record is gone.
fn forgotten(effects: &[Effect]) -> Vec<String> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::JobForgotten { job } => Some(job.to_string()),
            _ => None,
        })
        .collect()
}

/// `Effect::Accepted` as `(job, attached, replayed)`.
///
/// `attached=false` with an empty replay is the whole difference between "a fresh
/// execution" and "joined an execution that already has an answer" (§5.2, §9.1).
fn accepted(effects: &[Effect]) -> Vec<(String, bool, Vec<ConsumerEvent>)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Accepted {
                job,
                attached,
                replayed,
            } => Some((job.to_string(), *attached, replayed.clone())),
            _ => None,
        })
        .collect()
}

fn deltas(events: &[ConsumerEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match e {
            ConsumerEvent::Delta { text, .. } => Some(text.clone()),
            _ => None,
        })
        .collect()
}

// --------------------------------------------------------------------------- §16

#[test]
fn t01_two_workers_one_job_yields_exactly_one_assigned() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    warm(&mut a, "w-b", 2, 4, 0);
    a.handle(10, submit_msg("job-1", "k1", None));
    let effects_a = a.handle(20, claim_msg("w-a", 1, 1));
    let effects_b = a.handle(20, claim_msg("w-b", 2, 1));

    assert_eq!(
        assigned(&effects_a),
        vec![("w-a".to_string(), "job-1".to_string(), 1u32)]
    );
    assert_eq!(
        assigned(&effects_b),
        Vec::new(),
        "the second worker receives nothing"
    );
    assert_eq!(
        a.lease_of(&JobId::new_unchecked("job-1"))
            .expect("leased")
            .worker,
        WorkerId::new_unchecked("w-a")
    );
    assert_eq!(a.queue_depth(), 0);
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-b")].in_flight.len(),
        0
    );
}

#[test]
fn t02_concurrent_claims_resolve_deterministically_in_mailbox_order() {
    // Same world, opposite arrival order: the winner follows the mailbox, so the
    // outcome is reproducible rather than "one of them, somehow".
    //
    // Each worker's own connection id travels with its claim: `w-a` is live on
    // conn 1 and `w-b` on conn 2 (both `warm` above), and a claim stamped with the
    // other socket's generation is a **stale** frame that the actor correctly drops
    // (`§11`, `is_stale`). Hardcoding conn 1 for whichever worker arrived first was
    // what made the `("w-b", "w-a")` permutation look like a lost dispatch.
    for (first, first_conn, second, second_conn) in
        [("w-a", 1u64, "w-b", 2u64), ("w-b", 2, "w-a", 1)]
    {
        let mut a = actor();
        warm(&mut a, "w-a", 1, 4, 0);
        warm(&mut a, "w-b", 2, 4, 0);
        a.handle(10, submit_msg("job-1", "k1", None));
        let e1 = a.handle(20, claim_msg(first, first_conn, 1));
        let e2 = a.handle(20, claim_msg(second, second_conn, 1));
        assert_eq!(
            assigned(&e1),
            vec![(first.to_string(), "job-1".to_string(), 1u32)]
        );
        assert_eq!(assigned(&e2), Vec::new());
        assert_eq!(a.leases_len(), 1, "no double dispatch for {first}/{second}");
    }
}

#[test]
fn t03_worker_killed_before_any_output_is_requeued_and_completes_elsewhere() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    warm(&mut a, "w-b", 2, 4, 0);
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(20, claim_msg("w-a", 1, 1));
    a.handle(21, claim_msg("w-b", 2, 1));
    assert_eq!(
        a.lease_of(&JobId::new_unchecked("job-1"))
            .expect("leased")
            .attempt,
        1
    );

    // Socket closes: the resume grace holds ownership, so nobody else can take it.
    let gone = a.handle(30, gone_msg("w-a", 1));
    assert_eq!(
        a.lease_of(&JobId::new_unchecked("job-1"))
            .expect("still owned")
            .worker,
        WorkerId::new_unchecked("w-a")
    );
    assert_eq!(a.ready_workers(), 1);
    assert!(assigned(&gone).is_empty());

    // Grace expires with zero bytes delivered → requeue, preferring the other worker.
    let after = a.handle(31_501, ActorMsg::Tick { now_ms: 31_501 });
    assert_eq!(
        assigned(&after),
        vec![("w-b".to_string(), "job-1".to_string(), 2u32)]
    );
    assert_eq!(
        a.lease_of(&JobId::new_unchecked("job-1"))
            .expect("leased again")
            .attempt,
        2
    );

    let mut seen = after;
    seen.extend(a.handle(32_000, delta_msg("w-b", 2, "job-1", 1, &"tok".repeat(600))));
    seen.extend(a.handle(32_100, delta_msg("w-b", 2, "job-1", 2, &"en".repeat(600))));
    seen.extend(a.handle(
        32_200,
        done_msg("w-b", 2, "job-1", 3, Some(json!({"completion_tokens": 6}))),
    ));
    let events = relay_events(&seen);
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, ConsumerEvent::Error { .. })),
        "{events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, ConsumerEvent::Finish { .. }))
            .count(),
        1
    );
    assert_eq!(
        a.metrics()
            .get("nexus_lease_expiries_total{kind=\"no_output\"}"),
        1
    );
    // The consumer's stream is coherent: exactly the two deltas from attempt 2, in
    // order, and nothing from the killed attempt. The length is 3 000, not 2 400:
    // `"tok".repeat(600)` is 1 800 chars and `"en".repeat(600)` is 1 200, so the two
    // halves are unequal (the 600 is the repetition count, not the byte count).
    assert_eq!(
        deltas(&events),
        vec!["tok".repeat(600), "en".repeat(600)],
        "the two attempt-2 deltas, verbatim, with no attempt-1 residue"
    );
    assert_eq!(
        deltas(&events).iter().map(String::len).sum::<usize>(),
        3_000
    );
}

#[test]
fn t04_worker_killed_after_output_terminates_in_band_with_one_finish() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    warm(&mut a, "w-b", 2, 4, 0);
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(20, claim_msg("w-a", 1, 1));
    let first = a.handle(30, delta_msg("w-a", 1, "job-1", 1, &"a".repeat(2_000)));
    assert_eq!(
        deltas(&relay_events(&first)).len(),
        1,
        "2 kB crosses the byte threshold and flushes"
    );

    let gone = a.handle(40, gone_msg("w-a", 1));
    let after = a.handle(41_501, ActorMsg::Tick { now_ms: 41_501 });
    let all: Vec<Effect> = first.into_iter().chain(gone).chain(after.clone()).collect();
    let events = relay_events(&all);

    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, ConsumerEvent::Error { .. }))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, ConsumerEvent::Finish { .. }))
            .count(),
        0,
        "no finish_reason after a partial stream"
    );
    assert_eq!(deltas(&events).len(), 1, "deltas are never re-emitted");
    assert_eq!(
        terminals(&after),
        vec![(
            "job-1".to_string(),
            TerminalReason::WorkerGonePartial,
            false
        )]
    );
    assert_eq!(a.metrics().get("nexus_inband_errors_total"), 1);
    assert!(
        assigned(&after).is_empty(),
        "continuation is disabled here: the job is not re-owned"
    );
    assert_eq!(a.leases_len(), 0);
}

#[test]
fn t05_consumer_disconnect_cancels_the_worker_immediately() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(20, claim_msg("w-a", 1, 1));
    let effects = a.handle(
        30,
        ActorMsg::Detach {
            job: JobId::new_unchecked("job-1"),
            consumer: ConsumerId::new_unchecked("con-job-1"),
        },
    );

    assert!(
        sent_to(&effects, "w-a").iter().any(|f| matches!(
            f,
            HubFrame::Cancel {
                reason: CancelReason::ConsumerGone,
                ..
            }
        )),
        "cancel must fan out on disconnect"
    );
    assert_eq!(a.metrics().get("nexus_cancels_sent_total"), 1);
    assert_eq!(
        terminals(&effects),
        vec![("job-1".to_string(), TerminalReason::Cancelled, false)]
    );

    // The ack releases the slot, so the engine is idle again (the §16.5 assertion
    // "engine goes idle" is exactly this state).
    a.handle(
        31,
        frame(
            "w-a",
            1,
            WorkerFrame::CancelAck {
                job: JobId::new_unchecked("job-1"),
            },
        ),
    );
    assert_eq!(a.metrics().get("nexus_cancel_acks_total"), 1);
    assert!(a.workers()[&WorkerId::new_unchecked("w-a")]
        .in_flight
        .is_empty());
    assert!(a.workers()[&WorkerId::new_unchecked("w-a")].dispatchable());
    assert_eq!(a.leases_len(), 0);

    // Frames that arrive after the cancel are counted, never relayed.
    let late = a.handle(32, delta_msg("w-a", 1, "job-1", 1, "too late"));
    assert!(relay_events(&late).is_empty());
}

#[test]
fn t06_no_ready_worker_answers_503_with_retry_after_before_any_body() {
    let mut a = actor();
    a.handle(0, hello_msg("w-a", 1, 4));
    a.handle(
        1,
        frame(
            "w-a",
            1,
            WorkerFrame::Ready {
                free_slots: 4,
                free_kv_tokens: None,
                engine_ready: false,
                telemetry: None,
            },
        ),
    );
    assert_eq!(a.ready_workers(), 0);
    assert_eq!(
        a.loading_workers(),
        1,
        "cold start shows as degraded capacity, not as zero capacity"
    );

    let effects = a.handle(10, submit_msg("job-1", "k1", None));
    assert!(
        rejects(&effects).is_empty(),
        "still inside the grace window"
    );
    assert!(
        relay_events(&effects).is_empty(),
        "no body bytes before the answer"
    );
    assert_eq!(
        a.job(&JobId::new_unchecked("job-1")).expect("record").phase,
        JobPhase::Pending
    );

    // Grace expires with nothing warm: 503 + Retry-After, and no Relay at all.
    let late = a.handle(2_010, ActorMsg::Tick { now_ms: 2_010 });
    assert_eq!(
        rejects(&late),
        vec![(503, "no_ready_worker".to_string(), Some(2_000))]
    );
    assert!(relay_events(&late).is_empty());
    assert_eq!(
        terminals(&late),
        vec![("job-1".to_string(), TerminalReason::NoReadyWorker, true)]
    );
    assert_eq!(a.queue_depth(), 0);
}

#[test]
fn t06b_a_warm_worker_arriving_inside_the_grace_avoids_the_503() {
    let mut a = actor();
    a.handle(0, hello_msg("w-a", 1, 4));
    a.handle(10, submit_msg("job-1", "k1", None));
    // The engine finishes loading at 1.5 s, inside the 2 s window.
    a.handle(1_500, ready_msg("w-a", 1, 4));
    a.handle(1_501, claim_msg("w-a", 1, 1));
    assert_eq!(
        a.job(&JobId::new_unchecked("job-1")).expect("r").phase,
        JobPhase::Assigned
    );
    let late = a.handle(2_010, ActorMsg::Tick { now_ms: 2_010 });
    assert!(rejects(&late).is_empty(), "the job was already dispatched");
}

#[test]
fn t06c_zero_connected_workers_is_a_capability_reject_not_a_503() {
    let mut a = actor();
    let effects = a.handle(10, submit_msg("job-1", "k1", None));
    assert_eq!(
        rejects(&effects),
        vec![(400, "capability_unsatisfied".to_string(), None)]
    );
}

#[test]
fn t07_duplicate_idempotency_key_attaches_and_replays_never_reruns() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(10, submit_msg("job-1", "same-key", None));
    a.handle(20, claim_msg("w-a", 1, 1));
    let streamed = a.handle(
        30,
        delta_msg("w-a", 1, "job-1", 1, &"delta-text".repeat(200)),
    );
    assert_eq!(deltas(&relay_events(&streamed)).len(), 1);

    let dup = a.handle(
        40,
        ActorMsg::Submit {
            job: JobId::new_unchecked("job-2"),
            consumer: ConsumerId::new_unchecked("con-2"),
            submission: match submit_msg("job-ignored", "same-key", None) {
                ActorMsg::Submit { submission, .. } => submission,
                _ => unreachable!(),
            },
        },
    );
    match &dup[..] {
        [Effect::Accepted {
            attached,
            job,
            replayed,
        }] => {
            assert!(attached);
            assert_eq!(
                job.as_ref(),
                "job-1",
                "the second client joins the existing execution"
            );
            assert_eq!(
                replayed.len(),
                1,
                "and is handed what the buffer already holds"
            );
        }
        other => panic!("expected exactly one attach effect, got {other:?}"),
    }
    assert!(assigned(&dup).is_empty(), "no second execution");
    assert_eq!(a.metrics().get("nexus_idempotent_attach_total"), 1);
    assert_eq!(
        a.job(&JobId::new_unchecked("job-1"))
            .expect("record")
            .consumers
            .len(),
        2
    );
    assert_eq!(
        a.job(&JobId::new_unchecked("job-2")).map(|r| r.phase),
        None,
        "job-2 was never allocated"
    );
}

#[test]
fn t08_reconnecting_worker_resumes_from_the_acked_seq_without_duplicates() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(20, claim_msg("w-a", 1, 1));
    a.handle(30, delta_msg("w-a", 1, "job-1", 1, &"one".repeat(600)));
    a.handle(31, delta_msg("w-a", 1, "job-1", 2, &"two".repeat(600)));
    a.handle(120, delta_msg("w-a", 1, "job-1", 3, &"three".repeat(600)));
    assert_eq!(
        a.job(&JobId::new_unchecked("job-1"))
            .expect("r")
            .seq
            .acked_through(),
        3
    );

    // The socket drops and comes back inside the grace window.
    a.handle(130, gone_msg("w-a", 1));
    let welcome = a.handle(135, hello_msg("w-a", 5, 4));
    let resumed = welcome
        .iter()
        .find_map(|e| match e {
            // `frame` is a `Box<HubFrame>`; match through it.
            Effect::SendWorker { frame, .. } => match frame.as_ref() {
                HubFrame::Welcome { resumed, .. } => Some(resumed.clone()),
                _ => None,
            },
            _ => None,
        })
        .expect("Welcome lists the resumable jobs");
    assert_eq!(resumed.len(), 1);
    assert_eq!(
        resumed[0].resume_from_seq, 3,
        "the hub tells the worker where to continue"
    );

    let resume = a.handle(
        136,
        frame(
            "w-a",
            5,
            WorkerFrame::Resume {
                job: JobId::new_unchecked("job-1"),
                attempt: 1,
                acked_seq: 3,
            },
        ),
    );
    assert!(sent_to(&resume, "w-a")
        .iter()
        .any(|f| matches!(f, HubFrame::Ack { seq: 3, .. })));
    assert_eq!(a.metrics().get("nexus_resume_accepted_total"), 1);
    assert_eq!(
        a.lease_of(&JobId::new_unchecked("job-1"))
            .expect("still leased")
            .attempt,
        1,
        "the job was never re-dispatched"
    );

    // The worker re-sends its tail: both frames are dropped as duplicates.
    let replay_2 = a.handle(140, delta_msg("w-a", 5, "job-1", 2, &"two".repeat(600)));
    let replay_3 = a.handle(141, delta_msg("w-a", 5, "job-1", 3, &"three".repeat(600)));
    for effects in [&replay_2, &replay_3] {
        assert!(
            deltas(&relay_events(effects)).is_empty(),
            "no duplicate deltas on the wire"
        );
        assert!(
            sent_to(effects, "w-a")
                .iter()
                .any(|f| matches!(f, HubFrame::Ack { seq: 3, .. })),
            "the worker is re-acked to where it belongs"
        );
    }
    assert_eq!(a.metrics().get("nexus_frames_duplicate_total"), 2);
    assert_eq!(deltas(&relay_events(&welcome)).len(), 0);

    // It then continues from seq 4 and finishes normally.
    let tail = a.handle(150, delta_msg("w-a", 5, "job-1", 4, &"four".repeat(600)));
    assert_eq!(deltas(&relay_events(&tail)).len(), 1);
    let done = a.handle(260, done_msg("w-a", 5, "job-1", 4, None));
    assert!(relay_events(&done)
        .iter()
        .any(|e| matches!(e, ConsumerEvent::Finish { seq: 4, .. })));
}

#[test]
fn t09_consumer_reconnect_with_last_event_id_replays_without_a_gap() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(20, claim_msg("w-a", 1, 1));
    let mut texts = Vec::new();
    for (seq, word) in [(1u64, "alpha"), (2, "beta"), (3, "gamma")] {
        let effects = a.handle(seq * 100, delta_msg("w-a", 1, "job-1", seq, word));
        texts.extend(deltas(&relay_events(&effects)));
    }
    // The third delta is still inside its flush window when its frame is handled:
    // §7.2's age rule is a timer, not a frame-driven heuristic, so it leaves on the
    // next tick (`limits.flush_interval_ms = 75`, and
    // `deltas_are_batched_by_size_and_age_not_per_token` pins the same behaviour for
    // a small tail). The replay ring already holds all three frames, which is the
    // point of the attach below.
    let aged = a.handle(375, ActorMsg::Tick { now_ms: 375 });
    texts.extend(deltas(&relay_events(&aged)));
    assert_eq!(
        texts,
        vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()]
    );

    // A second consumer drops and returns with Last-Event-ID: 1.
    let effects = a.handle(
        380,
        ActorMsg::Attach {
            job: JobId::new_unchecked("job-1"),
            consumer: ConsumerId::new_unchecked("late"),
            from_seq: 1,
        },
    );
    let mut lossy = None;
    let mut replayed = Vec::new();
    for e in &effects {
        if let Effect::ReplayTo {
            events, lossy: l, ..
        } = e
        {
            lossy = Some(*l);
            replayed.extend(deltas(events));
        }
    }
    assert_eq!(
        lossy,
        Some(false),
        "the ring still holds everything after seq 1"
    );
    assert_eq!(
        replayed,
        vec!["beta".to_string(), "gamma".to_string()],
        "no gap in the text"
    );

    // Asking for something already evicted is reported, not silently truncated.
    let beyond = a.handle(
        390,
        ActorMsg::Attach {
            job: JobId::new_unchecked("job-1"),
            consumer: ConsumerId::new_unchecked("older"),
            from_seq: 0,
        },
    );
    for e in &beyond {
        if let Effect::ReplayTo { events, .. } = e {
            assert_eq!(
                deltas(events).len(),
                3,
                "the whole buffer is still available at this size"
            );
        }
    }
}

#[test]
fn t10_unsatisfiable_capability_is_an_immediate_400() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    let body = json!({"model":"qwen38","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"x"}}]}],"stream":true});
    let effects = a.handle(
        10,
        ActorMsg::Submit {
            job: JobId::new_unchecked("job-vis"),
            consumer: ConsumerId::new_unchecked("c"),
            submission: Submission {
                kind: KIND_CHAT.to_string(),
                pool: pool(),
                tenant: TenantId::new_unchecked("acme"),
                requires: nexus_core::adapters::infer_requires("qwen38", &body),
                payload: body,
                content_type: "application/json".to_string(),
                idempotency_key: "vision-key".to_string(),
                session_hint: None,
                priority: Priority::Default,
                idempotent: true,
                streaming: true,
                cancel_on_consumer_gone: true,
            },
        },
    );
    assert_eq!(
        rejects(&effects),
        vec![(400, "capability_unsatisfied".to_string(), None)]
    );
    assert!(
        !a.has_job(&JobId::new_unchecked("job-vis")),
        "a refused request must not occupy a slot"
    );
    assert_eq!(
        a.metrics()
            .get("nexus_jobs_rejected_total{reason=\"capability_unsatisfied\"}"),
        1
    );
}

#[test]
fn t11_a_loading_worker_never_receives_assigned() {
    let mut a = actor();
    a.handle(0, hello_msg("w-load", 1, 4));
    a.handle(10, submit_msg("job-1", "k1", None));
    // It claims while still loading: the answer is nothing at all.
    let effects = a.handle(20, claim_msg("w-load", 1, 4));
    assert!(assigned(&effects).is_empty());
    assert_eq!(
        a.job(&JobId::new_unchecked("job-1")).expect("r").phase,
        JobPhase::Pending
    );

    // When it reports ready, the authority it already holds pays off immediately.
    let ready = a.handle(30, ready_msg("w-load", 1, 4));
    assert_eq!(
        assigned(&ready),
        vec![("w-load".to_string(), "job-1".to_string(), 1u32)]
    );
}

#[test]
fn t12_declared_capacity_is_respected_per_worker() {
    // Five jobs have to be **admitted** for the fifth to still sit queued behind a
    // 4-slot worker. The shared fixture caps a tenant at 4 concurrent jobs (the
    // tenant-limit test depends on that number), which would answer the fifth
    // `Submit` with `429` instead of queueing it, so this test raises its own cap.
    let mut cfg = config();
    cfg.limits.tenant_max_concurrent = 8;
    let mut a = PoolActor::new(pool(), cfg);
    warm(&mut a, "w-4", 1, 4, 0);
    for n in 1..=5 {
        a.handle(
            10 + n,
            submit_msg(&format!("job-{n}"), &format!("k{n}"), None),
        );
    }
    let effects = a.handle(20, claim_msg("w-4", 1, 8));
    let got = assigned(&effects);
    assert_eq!(got.len(), 4, "a 4-slot worker gets exactly 4 jobs, never 5");
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-4")].in_flight.len(),
        4
    );
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-4")].available_slots(),
        0
    );
    assert_eq!(a.queue_depth(), 1, "the fifth job stays queued");
    // A further claim while full grants no authority, so nothing over-dispatches.
    assert!(assigned(&a.handle(30, claim_msg("w-4", 1, 4))).is_empty());
}

#[test]
fn t12b_a_one_slot_worker_takes_exactly_one_job() {
    let mut a = actor();
    warm(&mut a, "w-1", 1, 1, 0);
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(11, submit_msg("job-2", "k2", None));
    let effects = a.handle(20, claim_msg("w-1", 1, 4));
    assert_eq!(assigned(&effects).len(), 1, "ComfyUI-style 1-slot engine");
    assert_eq!(a.queue_depth(), 1);
}

#[test]
fn t13_attestation_outside_the_allowlist_cannot_join_the_pool() {
    let mut a = actor();
    let mut msg = hello_msg("w-rogue", 1, 4);
    if let ActorMsg::Hello { hello, .. } = &mut msg {
        hello.attestation = Attestation {
            model_shas: BTreeSet::from([
                "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
            ]),
            weight_bytes: None,
        };
    }
    let effects = a.handle(0, msg);
    assert!(sent_to(&effects, "w-rogue")
        .iter()
        .any(|f| matches!(f, HubFrame::Reject { code, .. } if code == "attestation_rejected")));
    assert!(sent_to(&effects, "w-rogue").iter().any(|f| matches!(
        f,
        HubFrame::Bye {
            reason: ByeReason::AttestationRejected
        }
    )));
    assert!(
        a.workers().is_empty(),
        "an unattested worker is not capacity"
    );
    assert_eq!(a.ready_workers(), 0);
}

#[test]
fn t13b_protocol_mismatch_is_a_clean_reject() {
    let mut a = actor();
    let mut msg = hello_msg("w-old", 1, 4);
    if let ActorMsg::Hello { hello, .. } = &mut msg {
        hello.protocol = 2;
    }
    let effects = a.handle(0, msg);
    assert!(sent_to(&effects, "w-old").iter().any(|f| matches!(
        f,
        HubFrame::Bye {
            reason: ByeReason::VersionMismatch
        }
    )));
    assert!(a.workers().is_empty());
}

#[test]
fn t13c_a_worker_may_not_join_a_pool_it_cannot_serve() {
    let mut a = actor();
    let mut msg = hello_msg("w-text", 1, 4);
    if let ActorMsg::Hello { hello, .. } = &mut msg {
        // The pool floor requires a 512K qwen38; this worker offers 8K.
        hello.requires.min_context_tokens = Some(8_192);
    }
    let effects = a.handle(0, msg);
    assert!(sent_to(&effects, "w-text")
        .iter()
        .any(|f| matches!(f, HubFrame::Reject { code, .. } if code == "pool_unavailable")));
    assert!(a.workers().is_empty());
}

#[test]
fn t14_tool_call_reasoning_and_usage_survive_the_relay_verbatim() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(20, claim_msg("w-a", 1, 1));
    let chunks = [
        r#"{"choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}"#,
        r#"{"choices":[{"index":0,"delta":{"reasoning_content":"thin"},"finish_reason":null}]}"#,
        r#"{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"c1","function":{"name":"get_weather","arguments":"{\"ci"}}]},"finish_reason":null}]}"#,
        r#"{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"ty\"}"}}]},"finish_reason":"tool_calls"}]}"#,
    ];
    let mut events = Vec::new();
    for (i, chunk) in chunks.iter().enumerate() {
        // Each frame is handled a full flush window apart, so the ordering
        // assertion below is about relay fidelity, not about batching.
        events.extend(relay_events(&a.handle(
            30 + i as u64 * 100,
            delta_msg("w-a", 1, "job-1", i as u64 + 1, chunk),
        )));
    }
    assert_eq!(
        deltas(&events),
        chunks.iter().map(|s| (*s).to_string()).collect::<Vec<_>>(),
        "byte-for-byte, no re-tokenization artifacts"
    );

    let usage = json!({"prompt_tokens": 11, "completion_tokens": 7, "tool_calls": 1, "engine_specific": "kept"});
    let done = a.handle(
        430,
        frame(
            "w-a",
            1,
            WorkerFrame::Done {
                job: JobId::new_unchecked("job-1"),
                seq: 5,
                finish: FinishKind::ToolCalls,
                usage: Some(usage.clone()),
                error: None,
            },
        ),
    );
    match relay_events(&done).last().expect("a finish event") {
        ConsumerEvent::Finish {
            finish,
            usage: relayed_usage,
            ..
        } => {
            assert_eq!(*finish, FinishKind::ToolCalls);
            assert_eq!(
                relayed_usage.as_ref(),
                Some(&usage),
                "usage is relayed untouched"
            );
        }
        other => panic!("expected a finish event, got {other:?}"),
    }
    assert_eq!(a.metrics().get("nexus_tokens_total"), 7);
}

#[test]
fn t15_artifact_upload_is_measured_not_trusted_and_survives_a_resume() {
    let image_pool = PoolId::new_unchecked("image.render/comfyui");
    let mut hub = PoolActor::with_artifact_root(image_pool.clone(), config(), "var/artifacts");
    // The worker has to *say* it is joining this pool: the reference `warm` names
    // the llm pool, which this actor does not own, and the join would be refused.
    warm_pool(&mut hub, "w-comfy", 1, 1, image_pool.as_ref(), 0);
    hub.handle(5, claim_msg("w-comfy", 1, 1));

    let effects = hub.handle(
        10,
        ActorMsg::Submit {
            job: JobId::new_unchecked("job-img"),
            consumer: ConsumerId::new_unchecked("c"),
            submission: Submission {
                kind: "image.workflow".to_string(),
                pool: image_pool.clone(),
                tenant: TenantId::new_unchecked("acme"),
                requires: Requires::default(),
                payload: json!({"graph": "workflow-json"}),
                content_type: "application/json".to_string(),
                idempotency_key: "img-1".to_string(),
                session_hint: None,
                priority: Priority::Bulk,
                idempotent: true,
                streaming: false,
                // A job-API submission survives its submitting socket (task §7.1).
                cancel_on_consumer_gone: false,
            },
        },
    );
    assert_eq!(
        assigned(&effects),
        vec![("w-comfy".to_string(), "job-img".to_string(), 1u32)]
    );
    let HubFrame::Assigned { artifact_put, .. } = sent_to(&effects, "w-comfy")[0] else {
        panic!("expected an Assigned frame carrying an upload slot");
    };
    let put = artifact_put.clone().expect("an upload slot");
    assert_eq!(put.path, "/v1/artifacts/art-job-img");
    assert_eq!(put.token, "atok-job-img-1");
    assert_eq!(put.chunk_bytes, 4 * 1024 * 1024);

    let stored = nexus_core::store::sha256_hex(b"half-one-half-two");
    hub.handle(20, stored_chunk(8, 0, false, None, None, None));

    // A digest the hub cannot confirm is surfaced in band, not swallowed (§9: the
    // declaration is measured, never trusted). It is the **first** completion attempt
    // for this artifact, because a disagreement leaves the slot open: `complete()`
    // refuses to mark an artifact whose declared bytes disagree with the hub's own
    // measurement, and an already-complete artifact takes a chunk-write refusal
    // instead. Sequencing it here is what makes the retry below the interesting part:
    // the honest declaration still completes cleanly afterwards.
    let bad = hub.handle(
        21,
        stored_chunk(
            9,
            1,
            true,
            Some(stored.clone()),
            Some(&"f".repeat(64)),
            Some(17),
        ),
    );
    assert!(relay_events(&bad)
        .iter()
        .any(|e| matches!(e, ConsumerEvent::Error { code, .. } if code == "artifact_mismatch")));
    assert!(
        !hub.artifacts()
            .get(&nexus_core::ArtifactId::new_unchecked("art-job-img"))
            .expect("the slot survives its own refusal")
            .complete,
        "a refused declaration must not mark the artifact complete"
    );

    let done = hub.handle(
        22,
        stored_chunk(9, 1, true, Some(stored.clone()), Some(&stored), Some(17)),
    );
    let view = hub
        .job_view(&JobId::new_unchecked("job-img"))
        .expect("view");
    let artifact = view.artifact.clone().expect("artifact recorded");
    assert_eq!(artifact.size_bytes, 17);
    assert_eq!(artifact.stored_sha256.as_deref(), Some(stored.as_str()));
    assert_eq!(artifact.content_type, "image/png");
    assert!(artifact.complete);
    assert_eq!(artifact.path, "var/artifacts/job-img/art-job-img");
    // 8 + 9 from the refused attempt's chunk and 9 again from the honest retry: the
    // counter is what the worker wrote to the upload endpoint, while the book
    // deduplicates the chunk itself (the recorded total is the 17 asserted above).
    assert_eq!(hub.metrics().get("nexus_artifact_bytes_total"), 26);
    match relay_events(&done).as_slice() {
        [ConsumerEvent::Artifact { manifest, .. }] => {
            assert_eq!(manifest.sha256.as_deref(), Some(stored.as_str()));
            assert_eq!(manifest.size_bytes, Some(17));
        }
        other => panic!("expected one artifact announcement, got {other:?}"),
    }
    // An upload from a worker that no longer owns the job is refused outright.
    //
    // `w-other` has to be **connected** for this to be the ownership refusal: a
    // stranger with no live socket at all is refused one step earlier with
    // `401 stale_connection` (the §11 generation rule), which is a different answer
    // to a different question. With a live socket of its own, the only thing wrong
    // with its upload is that the lease belongs to `w-comfy`.
    warm_pool(&mut hub, "w-other", 9, 1, image_pool.as_ref(), 22);
    let stranger = hub.handle(
        23,
        ActorMsg::ArtifactStored {
            job: JobId::new_unchecked("job-img"),
            artifact: ArtifactId::new_unchecked("art-job-img"),
            worker: WorkerId::new_unchecked("w-other"),
            conn: 9,
            attempt: 1,
            chunk_index: 3,
            chunk_bytes: 1,
            complete: false,
            stored_sha256: None,
            declared: Default::default(),
            content_type: None,
        },
    );
    assert_eq!(
        rejects(&stranger),
        vec![(409, "stale_attempt".to_string(), None)],
        "a foreign worker's upload is refused, not absorbed"
    );

    // Detaching the submitting consumer must not cancel a detached job (§7.3).
    let detach = hub.handle(
        24,
        ActorMsg::Detach {
            job: JobId::new_unchecked("job-img"),
            consumer: ConsumerId::new_unchecked("c"),
        },
    );
    assert!(
        terminals(&detach).is_empty(),
        "the job keeps running so the client can poll for the artifact"
    );
}

/// One `ArtifactStored` message, with the chunk shape the runtime emits.
fn stored_chunk(
    bytes: u64,
    index: u64,
    complete: bool,
    stored: Option<String>,
    declared_sha: Option<&str>,
    declared_size: Option<u64>,
) -> ActorMsg {
    ActorMsg::ArtifactStored {
        job: JobId::new_unchecked("job-img"),
        artifact: ArtifactId::new_unchecked("art-job-img"),
        worker: WorkerId::new_unchecked("w-comfy"),
        conn: 1,
        attempt: 1,
        chunk_index: index,
        chunk_bytes: bytes,
        complete,
        stored_sha256: stored,
        declared: nexus_core::actor::DeclaredArtifact {
            sha256: declared_sha.map(str::to_string),
            size_bytes: declared_size,
        },
        content_type: if complete {
            Some("image/png".to_string())
        } else {
            None
        },
    }
}

#[test]
fn t16_large_payload_travels_by_reference_and_is_fetchable_by_its_owner() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));
    // The config caps inline payloads at 64 bytes.
    let big =
        json!({"model": "qwen38", "messages": [{"role": "user", "content": "x".repeat(512)}]});
    let effects = a.handle(
        10,
        submit_msg_with("job-big", "k-big", None, big.clone(), None),
    );
    let HubFrame::Assigned { payload, .. } = sent_to(&effects, "w-a")[0] else {
        panic!("expected an Assigned frame");
    };
    match payload {
        JobPayload::ByRef {
            path,
            token,
            size_bytes,
            sha256,
            content_type,
            ..
        } => {
            assert_eq!(path, "/v1/payloads/job-big");
            assert_eq!(token, "ptok-job-big-1");
            assert_eq!(
                *size_bytes, 572,
                "serialized length: 512 content chars plus JSON framing"
            );
            assert_eq!(
                sha256,
                &nexus_core::store::sha256_hex(&serde_json::to_vec(&big).expect("serialize"))
            );
            assert_eq!(content_type, "application/json");
        }
        JobPayload::Inline { .. } => panic!("a 512-char prompt must spill at a 64-byte cap"),
    }
    // The bytes are held in memory (never written to disk) and are readable only
    // by the owning worker, on the current attempt, with the right token.
    let id = nexus_core::PayloadRefId::new_unchecked("job-big");
    let entry = a
        .payloads()
        .authorize_fetch(
            &id,
            "ptok-job-big-1",
            &WorkerId::new_unchecked("w-a"),
            1,
            20,
        )
        .expect("authorized");
    let fetched: Value =
        serde_json::from_slice(&entry.bytes).expect("the spilled bytes are the payload");
    assert_eq!(fetched, big);
    assert!(a
        .payloads()
        .authorize_fetch(&id, "wrong-token", &WorkerId::new_unchecked("w-a"), 1, 20)
        .is_err());
    assert!(a
        .payloads()
        .authorize_fetch(
            &id,
            "ptok-job-big-1",
            &WorkerId::new_unchecked("w-b"),
            1,
            20
        )
        .is_err());
    assert_eq!(a.metrics().get("nexus_spill_bytes_total"), 572);
    assert_eq!(a.payloads().bytes(), 572);

    // Terminal releases the prompt immediately (the §9 retention policy).
    let done = a.handle(30, done_msg("w-a", 1, "job-big", 1, None));
    assert!(done.iter().any(|e| matches!(
        e,
        Effect::ReleasePayload {
            reference: Some(_),
            ..
        }
    )));
    assert!(a
        .payloads()
        .authorize_fetch(
            &id,
            "ptok-job-big-1",
            &WorkerId::new_unchecked("w-a"),
            1,
            31
        )
        .is_err());
    assert_eq!(a.payloads().bytes(), 0);
}

#[test]
fn t17_non_streaming_response_carries_the_same_content_as_the_stream() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(20, claim_msg("w-a", 1, 1));
    let mut events = Vec::new();
    for (i, part) in ["Hel", "lo ", "wor", "ld"].iter().enumerate() {
        let chunk =
            json!({"choices": [{"index": 0, "delta": {"content": part}, "finish_reason": null}]})
                .to_string();
        events.extend(relay_events(&a.handle(
            30 + i as u64 * 100,
            delta_msg("w-a", 1, "job-1", i as u64 + 1, &chunk),
        )));
    }
    let streamed: String = deltas(&events)
        .iter()
        .filter_map(|text| serde_json::from_str::<Value>(text).ok())
        .filter_map(|v| {
            v["choices"][0]["delta"]["content"]
                .as_str()
                .map(str::to_string)
        })
        .collect();
    assert_eq!(streamed, "Hello world");

    let usage = json!({"completion_tokens": 12});
    events.extend(relay_events(
        &a.handle(430, done_msg("w-a", 1, "job-1", 5, Some(usage.clone()))),
    ));
    let assembled = assemble_completion(&JobId::new_unchecked("job-1"), "qwen38", 430_000, &events);
    assert_eq!(
        assembled["choices"][0]["message"]["content"].as_str(),
        Some("Hello world")
    );
    assert_eq!(
        assembled["choices"][0]["finish_reason"].as_str(),
        Some("stop")
    );
    assert_eq!(assembled["usage"], usage);
    assert_eq!(assembled["object"].as_str(), Some("chat.completion"));
}

#[test]
fn t18_hub_shutdown_fails_clients_retryably_and_a_fresh_hub_resumes() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(11, submit_msg("job-2", "k2", None));

    let effects = a.handle(30, ActorMsg::Shutdown { now_ms: 30 });
    assert!(sent_to(&effects, "w-a").iter().any(|f| matches!(
        f,
        HubFrame::Cancel {
            reason: CancelReason::HubShutdown,
            ..
        }
    )));
    assert!(sent_to(&effects, "w-a").iter().any(|f| matches!(
        f,
        HubFrame::Bye {
            reason: ByeReason::Shutdown
        }
    )));
    let mut reasons = terminals(&effects);
    reasons.sort();
    assert_eq!(
        reasons,
        vec![
            ("job-1".to_string(), TerminalReason::HubShutdown, true),
            ("job-2".to_string(), TerminalReason::HubShutdown, true),
        ]
    );
    assert!(
        reasons.iter().all(|(_, _, retryable)| *retryable),
        "in-flight clients must get a retryable failure"
    );
    assert_eq!(a.queue_depth(), 0);
    assert!(a.is_closed());
    assert_eq!(
        rejects(&a.handle(31, submit_msg("job-3", "k3", None)))[0].0,
        503
    );
    assert_eq!(
        relay_events(&effects)
            .iter()
            .filter(|e| matches!(
                e,
                ConsumerEvent::Error {
                    retryable: true,
                    ..
                }
            ))
            .count(),
        2
    );

    // A fresh hub accepts the reconnecting worker and the retried submissions,
    // with the worker dialing in on its own (task §11, §16.18).
    let mut b = actor();
    warm(&mut b, "w-a", 9, 4, 40);
    b.handle(45, claim_msg("w-a", 9, 4));
    let first = b.handle(50, submit_msg("job-1", "k1-retry", None));
    assert_eq!(
        assigned(&first),
        vec![("w-a".to_string(), "job-1".to_string(), 1u32)]
    );
    let second = b.handle(51, submit_msg("job-2", "k2-retry", None));
    assert_eq!(
        assigned(&second),
        vec![("w-a".to_string(), "job-2".to_string(), 1u32)]
    );
}

// ----------------------------------------------------------------- beyond §16

#[test]
fn deltas_are_batched_by_size_and_age_not_per_token() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(20, claim_msg("w-a", 1, 1));
    // Nine 100-byte tokens inside one 75 ms window: nothing may go out yet.
    let mut relayed = 0;
    for seq in 1..=9 {
        relayed += deltas(&relay_events(&a.handle(
            30 + seq,
            delta_msg("w-a", 1, "job-1", seq, &"t".repeat(100)),
        )))
        .len();
    }
    assert_eq!(
        relayed, 0,
        "900 bytes is under the 1536-byte window, so zero syscalls"
    );
    // Crossing the byte threshold flushes the whole window as one batch.
    let flush = a.handle(31, delta_msg("w-a", 1, "job-1", 10, &"u".repeat(700)));
    assert_eq!(
        deltas(&relay_events(&flush)).len(),
        10,
        "one flush carries all ten deltas"
    );
    // A small tail is flushed by the age timer, not by the next token.
    let small = a.handle(40, delta_msg("w-a", 1, "job-1", 11, "tiny"));
    assert!(deltas(&relay_events(&small)).is_empty());
    let aged = a.handle(120, ActorMsg::Tick { now_ms: 120 });
    assert_eq!(deltas(&relay_events(&aged)), vec!["tiny".to_string()]);
}

#[test]
fn seq_gaps_are_reported_to_the_consumer_instead_of_swallowed() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(20, claim_msg("w-a", 1, 1));
    a.handle(30, delta_msg("w-a", 1, "job-1", 1, &"a".repeat(2_000)));
    let effects = a.handle(140, delta_msg("w-a", 1, "job-1", 4, &"b".repeat(2_000)));
    let events = relay_events(&effects);
    assert!(
        matches!(
            events.first(),
            Some(ConsumerEvent::Gap {
                expected: 2,
                got: 4
            })
        ),
        "{events:?}"
    );
    assert_eq!(a.metrics().get("nexus_seq_gaps_total"), 1);
    assert_eq!(
        a.job(&JobId::new_unchecked("job-1"))
            .expect("r")
            .seq
            .duplicates(),
        0
    );
}

#[test]
fn queue_depth_bound_answers_429_never_an_unbounded_wait() {
    let mut cfg = config();
    cfg.pools
        .get_mut("llm.chat/qwen38")
        .expect("pool")
        .queue_depth = Some(2);
    cfg.tenants.insert(
        "acme".to_string(),
        nexus_core::TenantConfig {
            max_concurrent: Some(64),
            ..Default::default()
        },
    );
    let mut a = PoolActor::new(pool(), cfg);
    // Connected but loading: nothing drains the queue, so depth is what binds.
    a.handle(0, hello_msg("w-load", 1, 4));
    for n in 1..=4 {
        let effects = a.handle(
            10 + n,
            submit_msg(&format!("job-{n}"), &format!("k{n}"), None),
        );
        if n <= 2 {
            assert!(
                rejects(&effects).is_empty(),
                "job-{n} should have been accepted"
            );
        } else {
            assert_eq!(
                rejects(&effects),
                vec![(429, "queue_full".to_string(), Some(1_000))]
            );
        }
    }
    assert_eq!(a.queue_depth(), 2);
    assert_eq!(
        a.metrics()
            .get("nexus_jobs_rejected_total{reason=\"queue_full\"}"),
        2
    );
}

#[test]
fn tenant_concurrency_cap_answers_429_and_releases_on_terminal() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));
    for n in 1..=5 {
        let effects = a.handle(
            10 + n,
            submit_msg(&format!("job-{n}"), &format!("k{n}"), None),
        );
        if n == 5 {
            assert_eq!(
                rejects(&effects),
                vec![(429, "tenant_limit".to_string(), Some(1_000))],
                "one client cannot take every slot"
            );
        }
    }
    let done = a.handle(50, done_msg("w-a", 1, "job-1", 0, None));
    assert_eq!(terminals(&done).first().expect("terminal").0, "job-1");
    // One slot freed, so the retry now fits.
    assert!(rejects(&a.handle(60, submit_msg("job-5", "k5-retry", None))).is_empty());
}

#[test]
fn oversized_prompt_is_413_and_an_unregistered_kind_is_400() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));
    let huge = json!({"model":"qwen38","messages":[{"role":"user","content":"z".repeat(5_000)}]});
    assert_eq!(
        rejects(&a.handle(10, submit_msg_with("job-x", "kx", None, huge, None)))[0],
        (413, "payload_too_large".to_string(), None)
    );

    // Prompt-token cap, the cheap KV mitigation of §8.
    let mut cfg = config();
    cfg.limits.tenant_max_prompt_tokens = 10;
    let mut b = PoolActor::new(pool(), cfg);
    warm(&mut b, "w-a", 1, 4, 0);
    let effects = b.handle(
        10,
        submit_msg_with("job-y", "ky", None, chat_body("hi"), None),
    );
    assert_eq!(
        rejects(&effects),
        vec![(413, "prompt_too_large".to_string(), None)]
    );

    // Unknown kind: the hub refuses rather than guessing idempotency semantics.
    let mut c = actor();
    warm(&mut c, "w-a", 1, 4, 0);
    let unknown = c.handle(
        10,
        ActorMsg::Submit {
            job: JobId::new_unchecked("job-z"),
            consumer: ConsumerId::new_unchecked("c"),
            submission: match submit_msg("job-z", "kz", None) {
                ActorMsg::Submit { submission, .. } => Submission {
                    kind: "not.registered".to_string(),
                    ..submission
                },
                _ => unreachable!(),
            },
        },
    );
    assert_eq!(
        rejects(&unknown),
        vec![(400, "unknown_kind".to_string(), None)]
    );
}

#[test]
fn stale_connection_frames_and_closes_are_ignored() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));
    a.handle(10, submit_msg("job-1", "k1", None));
    assert_eq!(a.leases_len(), 1);

    // The same worker id reconnects: the old socket is evicted, not doubled up.
    let welcome = a.handle(30, hello_msg("w-a", 7, 4));
    assert!(sent_to(&welcome, "w-a").iter().any(|f| matches!(
        f,
        HubFrame::Bye {
            reason: ByeReason::Evicted
        }
    )));
    assert_eq!(a.metrics().get("nexus_worker_evictions_total"), 1);
    assert_eq!(a.workers().len(), 1, "one record per stable worker id");

    // A frame already in flight on the dead socket is dropped.
    let stale = a.handle(31, delta_msg("w-a", 1, "job-1", 1, &"x".repeat(2_000)));
    assert!(relay_events(&stale).is_empty());
    assert_eq!(a.metrics().get("nexus_stale_frames_total"), 1);
    assert_eq!(a.workers()[&WorkerId::new_unchecked("w-a")].stale_frames, 1);

    // And the dead socket closing must not disturb the live one.
    a.handle(32, gone_msg("w-a", 1));
    assert!(a.workers()[&WorkerId::new_unchecked("w-a")].dispatchable());
    assert_eq!(
        a.lease_of(&JobId::new_unchecked("job-1"))
            .expect("intact")
            .epoch,
        7
    );
}

#[test]
fn frames_for_a_job_this_worker_no_longer_owns_are_refused_with_a_cancel() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 1, 0);
    a.handle(5, claim_msg("w-a", 1, 1));
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(20, delta_msg("w-a", 1, "job-1", 1, &"a".repeat(2_000)));
    // Consumer cancels: the lease goes away, but the engine may still be decoding.
    a.handle(
        21,
        ActorMsg::Cancel {
            job: JobId::new_unchecked("job-1"),
            reason: CancelReason::ConsumerGone,
        },
    );
    let late = a.handle(22, delta_msg("w-a", 1, "job-1", 2, "still working"));
    assert!(
        sent_to(&late, "w-a")
            .iter()
            .any(|f| matches!(f, HubFrame::Cancel { .. })),
        "the worker is told to stop again"
    );
    assert!(
        relay_events(&late).is_empty(),
        "nothing reaches the gone consumer"
    );
    assert_eq!(a.leases_len(), 0);
}

#[test]
fn resume_for_a_moved_or_finished_lease_is_nacked() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(30, done_msg("w-a", 1, "job-1", 1, None));
    let effects = a.handle(
        40,
        frame(
            "w-a",
            1,
            WorkerFrame::Resume {
                job: JobId::new_unchecked("job-1"),
                attempt: 1,
                acked_seq: 1,
            },
        ),
    );
    assert!(sent_to(&effects, "w-a")
        .iter()
        .any(|f| matches!(f, HubFrame::ResumeNack { reason, .. } if !reason.is_empty())));
    assert_eq!(a.metrics().get("nexus_resume_rejected_total"), 1);
    assert_eq!(a.metrics().get("nexus_resume_accepted_total"), 0);

    // A stale attempt number is refused too, so an old generation cannot hijack
    // a retried job.
    let mut b = actor();
    warm(&mut b, "w-a", 1, 1, 0);
    warm(&mut b, "w-b", 2, 1, 0);
    b.handle(5, claim_msg("w-a", 1, 1));
    b.handle(6, claim_msg("w-b", 2, 1));
    b.handle(10, submit_msg("job-9", "k9", None));
    b.handle(20, gone_msg("w-a", 1));
    let moved = b.handle(41_501, ActorMsg::Tick { now_ms: 41_501 });
    assert_eq!(
        assigned(&moved),
        vec![("w-b".to_string(), "job-9".to_string(), 2u32)]
    );
    // w-a dials back in on a new socket before trying to resume: a `Resume` stamped
    // with the generation the hub already retired is a **stale frame** and is dropped
    // without a reply (`stale_connection_frames_and_closes_are_ignored` pins that
    // rule), so the "old generation cannot hijack a retried job" race is the one this
    // assertion is about — the client reconnects and asks for the attempt it still
    // believes it owns.
    b.handle(41_550, hello_msg("w-a", 5, 1));
    let stale_resume = b.handle(
        41_600,
        frame(
            "w-a",
            5,
            WorkerFrame::Resume {
                job: JobId::new_unchecked("job-9"),
                attempt: 1,
                acked_seq: 0,
            },
        ),
    );
    assert!(sent_to(&stale_resume, "w-a")
        .iter()
        .any(|f| matches!(f, HubFrame::ResumeNack { .. })));
    assert_eq!(
        b.lease_of(&JobId::new_unchecked("job-9"))
            .expect("owner")
            .worker,
        WorkerId::new_unchecked("w-b"),
        "one owner survives the race"
    );
}

#[test]
fn non_idempotent_kinds_are_never_requeued_after_an_ambiguous_loss() {
    let mut cfg = config();
    cfg.kinds.insert(
        "notify.webhook".to_string(),
        nexus_core::KindConfig {
            pool: pool().to_string(),
            idempotent: false,
            streaming: true,
            ..Default::default()
        },
    );
    let mut a = PoolActor::new(pool(), cfg);
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));
    a.handle(
        10,
        ActorMsg::Submit {
            job: JobId::new_unchecked("job-hook"),
            consumer: ConsumerId::new_unchecked("c"),
            submission: Submission {
                kind: "notify.webhook".to_string(),
                pool: pool(),
                tenant: TenantId::new_unchecked("acme"),
                requires: Requires::default(),
                payload: json!({"url": "https://example.invalid/hook"}),
                content_type: "application/json".to_string(),
                idempotency_key: "hook-1".to_string(),
                session_hint: None,
                priority: Priority::Default,
                idempotent: false,
                streaming: true,
                cancel_on_consumer_gone: true,
            },
        },
    );
    assert_eq!(
        a.lease_of(&JobId::new_unchecked("job-hook"))
            .expect("leased")
            .attempt,
        1
    );
    a.handle(30, gone_msg("w-a", 1));
    let effects = a.handle(31_501, ActorMsg::Tick { now_ms: 31_501 });
    assert!(
        assigned(&effects).is_empty(),
        "a side-effecting job is failed loudly, never re-run"
    );
    assert_eq!(
        terminals(&effects),
        vec![(
            "job-hook".to_string(),
            TerminalReason::NonIdempotentAmbiguous,
            false
        )]
    );
    assert_eq!(
        a.metrics()
            .get("nexus_lease_expiries_total{kind=\"non_idempotent\"}"),
        1
    );
    assert_eq!(a.leases_len(), 0);
}

#[test]
fn heartbeats_renew_the_lease_and_missing_heartbeats_pause_dispatch_only() {
    // `heartbeat_ms = 5 s` is what makes §5.1's two states separable on the
    // injected clock: dispatch pauses after three missed heartbeats (15 s of
    // silence), while a lease renewed by the last heartbeat stays alive for
    // `lease_ms` (60 s). At the reference 20 s cadence the two deadlines coincide
    // (3 x 20 s = 60 s = the lease), so "unhealthy but still leased" would be
    // unobservable rather than untrue. `Limits::validate` already insists
    // `lease_ms >= 2 x heartbeat_ms`, which is the same invariant from the other side.
    let mut cfg = config();
    cfg.limits.heartbeat_ms = 5_000;
    let mut a = PoolActor::new(pool(), cfg);
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));
    a.handle(10, submit_msg("job-1", "k1", None));
    // The lease is stamped where it is granted: `dispatch(now_ms) +
    // lease_ms_for(pool)`, i.e. 10 + 60 000. Anchoring it at the worker's last
    // heartbeat instead would shorten a lease by however long the worker had been
    // idle before the job arrived, which is not what `Welcome.lease_ms` promises.
    assert_eq!(
        a.lease_of(&JobId::new_unchecked("job-1"))
            .expect("l")
            .deadline_ms,
        60_010
    );

    // A heartbeat at 40 s pushes the deadline out; the job is untouched.
    a.handle(40_000, ready_msg("w-a", 1, 3));
    assert_eq!(
        a.lease_of(&JobId::new_unchecked("job-1"))
            .expect("l")
            .deadline_ms,
        100_000
    );
    assert_eq!(a.job(&JobId::new_unchecked("job-1")).expect("r").attempt, 1);

    // Silence: after three heartbeat intervals dispatch pauses, but the lease is
    // kept until it actually expires (§5.1's "unhealthy but leased").
    let degraded = a.handle(60_001, ActorMsg::Tick { now_ms: 60_001 });
    assert_eq!(
        a.workers()[&WorkerId::new_unchecked("w-a")].engine,
        EngineState::Degraded
    );
    assert!(a.lease_of(&JobId::new_unchecked("job-1")).is_some());
    assert!(!degraded
        .iter()
        .any(|e| matches!(e, Effect::Terminal { .. })));
    // `Assigned`, not `Streaming`: the phase advances to `Streaming` only when a
    // frame is relayed (`PoolActor::relay_into`, and `JobPhase::Assigned`'s own doc
    // says "leased to a worker, waiting for the first frame"). Nothing has been
    // relayed here — that is the point of the test — and `acceptance.rs:393` and
    // `:1234` assert the same `Assigned` phase for a dispatched, frame-less job.
    assert_eq!(
        a.job(&JobId::new_unchecked("job-1")).expect("r").phase,
        JobPhase::Assigned
    );

    // The lease lapses at 100 s: zero bytes delivered → requeued, and the
    // degraded worker is not handed the retry either.
    let expired = a.handle(100_000, ActorMsg::Tick { now_ms: 100_000 });
    assert!(
        a.lease_of(&JobId::new_unchecked("job-1")).is_none(),
        "the lease is gone, and no new owner yet"
    );
    assert_eq!(
        a.job(&JobId::new_unchecked("job-1")).expect("r").phase,
        JobPhase::Queued
    );
    assert_eq!(
        a.job(&JobId::new_unchecked("job-1"))
            .expect("r")
            .last_worker,
        Some(WorkerId::new_unchecked("w-a"))
    );
    assert!(assigned(&expired).is_empty());
    assert_eq!(
        a.metrics()
            .get("nexus_lease_expiries_total{kind=\"no_output\"}"),
        1
    );

    // The worker recovers, claims again, and retries it (attempt 2).
    let back = a.handle(100_100, ready_msg("w-a", 1, 4));
    let retried = a.handle(100_200, claim_msg("w-a", 1, 4));
    assert_eq!(assigned(&back).len() + assigned(&retried).len(), 1);
    assert_eq!(
        a.lease_of(&JobId::new_unchecked("job-1"))
            .expect("re-leased")
            .attempt,
        2
    );
}

#[test]
fn hard_deadline_terminates_a_stalled_job_and_cancels_the_worker() {
    let mut cfg = config();
    cfg.kinds.get_mut(KIND_CHAT).expect("kind").job_hard_cap_ms = Some(5_000);
    let mut a = PoolActor::new(pool(), cfg);
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));
    a.handle(
        10,
        submit_msg_with("job-1", "k1", None, chat_body("hi"), None),
    );
    assert_eq!(
        a.job(&JobId::new_unchecked("job-1"))
            .expect("r")
            .deadline_ms,
        // `kind.job_hard_cap_ms` is measured from accept: the submit landed at
        // t = 10, so the hard cap is 10 + 5 000. The consumer's clock starts when
        // the request was accepted, not when the pool came into being.
        5_010
    );
    let effects = a.handle(5_010, ActorMsg::Tick { now_ms: 5_010 });
    assert!(sent_to(&effects, "w-a").iter().any(|f| matches!(
        f,
        HubFrame::Cancel {
            reason: CancelReason::Deadline,
            ..
        }
    )));
    assert_eq!(
        terminals(&effects),
        vec![("job-1".to_string(), TerminalReason::DeadlineExceeded, false)]
    );
    // The lease deadline (60 s) is later, so the tick must not double-fire it.
    assert_eq!(a.leases_len(), 0);
    assert_eq!(
        a.metrics()
            .get("nexus_jobs_terminal_total{reason=\"deadline_exceeded\"}"),
        1
    );
}

#[test]
fn models_and_health_reflect_only_warm_capacity() {
    let mut a = actor();
    a.handle(0, hello_msg("w-load", 1, 4));
    assert!(
        a.warm_models().is_empty(),
        "a loading worker is not warm capacity"
    );
    assert_eq!(a.metrics().get("nexus_loading_workers"), 1);
    let cold = nexus_core::adapters::health_body(
        "llm.chat/qwen38",
        a.ready_workers(),
        a.loading_workers(),
        a.queue_depth(),
        a.leases_len(),
    );
    assert_eq!(cold["status"], json!("degraded"));
    assert_eq!(cold["capacity_signal"], json!("cold_start"));

    a.handle(1, ready_msg("w-load", 1, 4));
    assert_eq!(a.warm_models(), BTreeSet::from(["qwen38".to_string()]));
    assert_eq!(a.metrics().get("nexus_ready_workers"), 1);
    let warm = nexus_core::adapters::health_body(
        "llm.chat/qwen38",
        a.ready_workers(),
        a.loading_workers(),
        a.queue_depth(),
        a.leases_len(),
    );
    assert_eq!(warm["status"], json!("ok"));
    assert_eq!(
        nexus_core::adapters::models_document(&a.warm_models(), 1_700_000_000_000)["data"][0]["id"],
        json!("qwen38")
    );
}

#[test]
fn queued_streaming_jobs_receive_keepalive_pings() {
    let mut cfg = config();
    // Long enough that the grace deadline does not pre-empt the ping assertions.
    cfg.limits.no_worker_grace_ms = 60_000;
    let mut a = PoolActor::new(pool(), cfg);
    a.handle(0, hello_msg("w-load", 1, 4));
    a.handle(10, submit_msg("job-1", "k1", None));

    let first = a.handle(10_010, ActorMsg::Tick { now_ms: 10_010 });
    assert!(first
        .iter()
        .any(|e| matches!(e, Effect::Keepalive { job } if job.as_ref() == "job-1")));
    // Not twice inside one second.
    let second = a.handle(10_400, ActorMsg::Tick { now_ms: 10_400 });
    assert!(!second.iter().any(|e| matches!(e, Effect::Keepalive { .. })));
    let third = a.handle(11_100, ActorMsg::Tick { now_ms: 11_100 });
    assert!(third.iter().any(|e| matches!(e, Effect::Keepalive { .. })));
    // Once it is assigned and streaming, the pings stop: real bytes are flowing.
    a.handle(12_000, ready_msg("w-load", 1, 4));
    a.handle(12_100, claim_msg("w-load", 1, 4));
    let streaming = a.handle(13_000, ActorMsg::Tick { now_ms: 13_000 });
    assert!(!streaming
        .iter()
        .any(|e| matches!(e, Effect::Keepalive { .. })));
}

#[test]
fn session_affinity_keeps_a_conversation_on_one_worker() {
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    warm(&mut a, "w-b", 2, 4, 0);
    let mut seen = Vec::new();
    for turn in 1..=6u64 {
        let effects = a.handle(
            turn * 10,
            submit_msg(&format!("job-{turn}"), &format!("k{turn}"), Some("sess-42")),
        );
        let claim_a = a.handle(turn * 10 + 1, claim_msg("w-a", 1, 4));
        let claim_b = a.handle(turn * 10 + 2, claim_msg("w-b", 2, 4));
        // Every dispatch counts, whichever message triggered it: the first turn can
        // only be handed out by the `Claim` that carries the authority, and after
        // that the retained authority lets the next `Submit` dispatch on arrival.
        let mut made = assigned(&effects);
        made.extend(assigned(&claim_a));
        made.extend(assigned(&claim_b));
        assert_eq!(made.len(), 1, "turn {turn} is dispatched exactly once");
        let (owner, job, attempt) = made[0].clone();
        assert_eq!(job, format!("job-{turn}"), "the right turn was dispatched");
        assert_eq!(attempt, 1u32, "no turn of a conversation is re-run");
        // A conversation is sequential: the answer completes before the next turn is
        // asked, which frees the slot and the tenant's concurrency budget. Leaving
        // all six turns in flight instead would bind on §8's per-worker capacity
        // (4 slots) and on `tenant_max_concurrent` before affinity had anything to
        // say about it.
        let conn = if owner == "w-a" { 1 } else { 2 };
        a.handle(
            turn * 10 + 3,
            done_msg(
                &owner,
                conn,
                &job,
                turn,
                Some(json!({"completion_tokens": turn})),
            ),
        );
        seen.extend(made);
    }
    let workers: BTreeSet<String> = seen.iter().map(|(w, _, _)| w.clone()).collect();
    assert_eq!(
        workers.len(),
        1,
        "one conversation must not hop workers, or turn 5 re-prefills 100K tokens: {seen:?}"
    );
    assert_eq!(seen.len(), 6);

    // Death of the affinity owner invalidates only that mapping (§8): the next
    // turn lands on the survivor.
    let dead = seen[0].0.clone();
    let survivor = if dead == "w-a" { "w-b" } else { "w-a" };
    let conn = if dead == "w-a" { 1 } else { 2 };
    a.handle(100, gone_msg(&dead, conn));
    let effects = a.handle(110, submit_msg("job-7", "k7", Some("sess-42")));
    a.handle(
        111,
        claim_msg(survivor, if survivor == "w-a" { 1 } else { 2 }, 4),
    );
    assert_eq!(
        assigned(&effects),
        vec![(survivor.to_string(), "job-7".to_string(), 1u32)]
    );

    // Jobs with no session hint remain open to either worker.
    let plain = a.handle(120, submit_msg("job-8", "k8", None));
    a.handle(
        121,
        claim_msg(survivor, if survivor == "w-a" { 1 } else { 2 }, 4),
    );
    assert_eq!(assigned(&plain).len(), 1);
}

#[test]
fn resume_by_continuation_is_off_by_default_and_opt_in() {
    // Default (budget 0): a mid-stream death is terminal.
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    warm(&mut a, "w-b", 2, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));
    a.handle(6, claim_msg("w-b", 2, 4));
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(30, delta_msg("w-a", 1, "job-1", 1, &"a".repeat(2_000)));
    a.handle(40, gone_msg("w-a", 1));
    let effects = a.handle(41_501, ActorMsg::Tick { now_ms: 41_501 });
    assert_eq!(
        terminals(&effects).first().expect("terminal").1,
        TerminalReason::WorkerGonePartial
    );

    // Opt-in: the partial text is handed to the next owner and the consumer's
    // stream is not ended (§7.4).
    let mut b = actor();
    b.limits_mut().max_continuations = 1;
    warm(&mut b, "w-a", 1, 4, 0);
    warm(&mut b, "w-b", 2, 4, 0);
    b.handle(5, claim_msg("w-a", 1, 4));
    b.handle(6, claim_msg("w-b", 2, 4));
    b.handle(10, submit_msg("job-1", "k1", None));
    b.handle(30, delta_msg("w-a", 1, "job-1", 1, &"a".repeat(2_000)));
    b.handle(40, gone_msg("w-a", 1));
    let after = b.handle(41_501, ActorMsg::Tick { now_ms: 41_501 });
    assert!(
        relay_events(&after)
            .iter()
            .filter(|e| matches!(e, ConsumerEvent::Error { .. }))
            .count()
            == 0,
        "the stream stays open"
    );
    assert_eq!(
        assigned(&after),
        vec![("w-b".to_string(), "job-1".to_string(), 2u32)]
    );
    let frame = sent_to(&after, "w-b")
        .into_iter()
        .find(|f| matches!(f, HubFrame::Assigned { .. }))
        .expect("an assignment");
    match frame {
        HubFrame::Assigned {
            continuation_text, ..
        } => {
            assert_eq!(
                continuation_text.as_deref().map(str::len),
                Some(2_000),
                "the partial text travels to the new owner"
            );
        }
        _ => unreachable!(),
    }
    assert_eq!(
        b.metrics()
            .get("nexus_lease_expiries_total{kind=\"continuation\"}"),
        1
    );
    assert_eq!(
        b.job(&JobId::new_unchecked("job-1")).expect("r").phase,
        JobPhase::Assigned
    );
}

/// §7.4's safety half: switching continuation on must not resurrect a stream whose
/// partial output is a **tool-call fragment**. The hub cannot rebuild a prompt
/// prefix out of `{"function":{"arguments":"{\"ci"}`, so the attempt stays terminal
/// even with budget available — and says why, in a log the operator can act on.
/// `continuation_allows_opaque_output` is the explicit opt-in for a pool whose
/// workers restart cleanly from an arbitrary prefix.
#[test]
fn opaque_tool_output_is_not_continued_even_when_continuation_is_on() {
    let tool_chunk = r#"{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"get_weather","arguments":"{\"ci"}}]}}]}"#;

    let mut a = actor();
    a.limits_mut().max_continuations = 1;
    warm(&mut a, "w-a", 1, 4, 0);
    warm(&mut a, "w-b", 2, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));
    a.handle(6, claim_msg("w-b", 2, 4));
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(30, delta_msg("w-a", 1, "job-1", 1, tool_chunk));
    a.handle(40, gone_msg("w-a", 1));
    let after = a.handle(41_501, ActorMsg::Tick { now_ms: 41_501 });

    assert!(assigned(&after).is_empty(), "an opaque attempt is not re-owned: the next worker would inherit a half-written tool argument");
    assert_eq!(
        terminals(&after),
        vec![(
            "job-1".to_string(),
            TerminalReason::WorkerGonePartial,
            false
        )],
        "the refusal is the ordinary in-band terminal"
    );
    assert_eq!(
        relay_events(&after)
            .iter()
            .filter(|e| matches!(e, ConsumerEvent::Error { .. }))
            .count(),
        1,
        "the consumer is told on the same stream"
    );
    assert_eq!(
        a.metrics()
            .get("nexus_lease_expiries_total{kind=\"partial\"}"),
        1
    );
    assert!(
        after.iter().any(|e| matches!(e, Effect::Log { message, .. } if message.contains("opaque"))),
        "the refusal must be explainable: continuation is on, so a silent terminal looks like a bug"
    );

    // The same death with the escape hatch open: seam and all, exactly one seam.
    let mut b = actor();
    b.limits_mut().max_continuations = 1;
    b.limits_mut().continuation_allows_opaque_output = true;
    warm(&mut b, "w-a", 1, 4, 0);
    warm(&mut b, "w-b", 2, 4, 0);
    b.handle(5, claim_msg("w-a", 1, 4));
    b.handle(6, claim_msg("w-b", 2, 4));
    b.handle(10, submit_msg("job-1", "k1", None));
    b.handle(30, delta_msg("w-a", 1, "job-1", 1, tool_chunk));
    b.handle(40, gone_msg("w-a", 1));
    let opted = b.handle(41_501, ActorMsg::Tick { now_ms: 41_501 });

    assert_eq!(
        assigned(&opted),
        vec![("w-b".to_string(), "job-1".to_string(), 2u32)],
        "the operator asked for the seam"
    );
    assert_eq!(
        b.metrics()
            .get("nexus_lease_expiries_total{kind=\"continuation\"}"),
        1
    );
    let HubFrame::Assigned {
        continuation_text, ..
    } = sent_to(&opted, "w-b")
        .into_iter()
        .find(|f| matches!(f, HubFrame::Assigned { .. }))
        .expect("an assignment")
    else {
        unreachable!();
    };
    assert_eq!(
        continuation_text.as_deref(),
        Some(tool_chunk),
        "the opaque prefix travels verbatim when the operator insists on it"
    );
}

/// §6's ownership rule is the **subscription**, not the wire format: an async job
/// (one the client submits through `POST /v1/jobs` and polls later) survives every
/// watcher hanging up, even when its kind happens to be a streaming one. `t05_...`
/// covers the mirror image, where the flag is `true` and the last consumer leaving
/// does cancel.
#[test]
fn an_async_job_survives_its_events_watcher_disconnecting() {
    let mut submit = submit_msg("job-async", "kasync", None);
    if let ActorMsg::Submit { submission, .. } = &mut submit {
        // A streaming kind submitted as a detached, poll-later job: the wire format
        // is SSE, the ownership contract is "this socket is not the owner".
        submission.streaming = true;
        submission.cancel_on_consumer_gone = false;
    }

    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(10, submit);
    a.handle(20, claim_msg("w-a", 1, 1));
    a.handle(
        25,
        ActorMsg::Attach {
            job: JobId::new_unchecked("job-async"),
            consumer: ConsumerId::new_unchecked("watcher"),
            from_seq: 0,
        },
    );

    let watcher_leaves = a.handle(
        28,
        ActorMsg::Detach {
            job: JobId::new_unchecked("job-async"),
            consumer: ConsumerId::new_unchecked("watcher"),
        },
    );
    assert!(
        terminals(&watcher_leaves).is_empty(),
        "one watcher leaving must not cancel anything"
    );

    // The submitting socket goes away too, and it still is not an owner: this is
    // the case the wire format alone got wrong.
    let submitter_leaves = a.handle(
        30,
        ActorMsg::Detach {
            job: JobId::new_unchecked("job-async"),
            consumer: ConsumerId::new_unchecked("con-job-async"),
        },
    );
    assert!(
        terminals(&submitter_leaves).is_empty(),
        "a detached submission keeps running so the client can poll for the result"
    );
    assert!(
        sent_to(&submitter_leaves, "w-a")
            .iter()
            .all(|f| !matches!(f, HubFrame::Cancel { .. })),
        "no Cancel reaches the GPU"
    );
    assert_eq!(a.metrics().get("nexus_cancels_sent_total"), 0);
    assert_eq!(
        a.job(&JobId::new_unchecked("job-async")).expect("r").phase,
        JobPhase::Assigned,
        "the job is still owned by w-a"
    );
    assert_eq!(a.leases_len(), 1, "and the worker keeps its slot");
}

#[test]
fn prompt_retention_lapses_by_ttl_and_terminal_retention_bounds_the_job_map() {
    // Two separate bounds, both deliberate (task §9's retention policy):
    //   payload TTL drops a queued prompt out of memory,
    //   job retention drops a terminal record (and with it its replay ring).
    let mut cfg = config();
    // Long enough that neither the no-worker grace nor the queue deadline fires.
    cfg.limits.no_worker_grace_ms = 600_000;
    cfg.limits.claim_deadline_ms = 600_000;
    let mut a = PoolActor::new(pool(), cfg);
    // Connected but loading: the job stays queued and nothing dispatches it.
    a.handle(0, hello_msg("w-load", 1, 4));
    a.handle(5, claim_msg("w-load", 1, 4));
    let big =
        json!({"model": "qwen38", "messages": [{"role": "user", "content": "y".repeat(300)}]});
    a.handle(10, submit_msg_with("job-p", "kp", None, big, None));
    assert_eq!(
        a.payloads().bytes(),
        360,
        "the prompt is held in memory, not on disk"
    );

    // Payload TTL is 120 s. Sweeping it must free the bytes and say so.
    let swept = a.handle(120_010, ActorMsg::Tick { now_ms: 120_010 });
    assert!(swept.iter().any(|e| matches!(
        e,
        Effect::ReleasePayload {
            reference: Some(_),
            ..
        }
    )));
    assert_eq!(a.payloads().bytes(), 0);
    assert!(
        a.has_job(&JobId::new_unchecked("job-p")),
        "the job record survives its prompt; only the prompt expired"
    );

    // A dispatch after the prompt lapsed is refused rather than handed a body of
    // nothing, and the refusal is retryable so the client re-sends the text.
    //
    // The refusal lands on the heartbeat, not on the later `Claim`: w-load still
    // holds the authority it granted at t = 5, and `PoolActor::on_ready` drains that
    // authority the moment the engine reports ready — which is exactly what
    // `t11_a_loading_worker_never_receives_assigned` asserts for a worker with a
    // healthy prompt. The trailing `Claim` then has nothing left to dispatch.
    let refused = a.handle(120_100, ready_msg("w-load", 1, 4));
    let late_claim = a.handle(120_200, claim_msg("w-load", 1, 4));
    assert!(assigned(&refused).is_empty());
    assert!(assigned(&late_claim).is_empty());
    assert_eq!(
        terminals(&refused).first().expect("terminal").1,
        TerminalReason::QueueTimeout
    );
    assert!(
        terminals(&late_claim).is_empty(),
        "one terminal per attempt: the claim must not re-fail the job"
    );
    assert_eq!(
        relay_events(&refused)
            .iter()
            .filter(|e| matches!(
                e,
                ConsumerEvent::Error {
                    retryable: true,
                    ..
                }
            ))
            .count(),
        1,
        "the body was already open, so the failure goes out in band and says retry"
    );
    assert_eq!(
        a.metrics()
            .get("nexus_jobs_rejected_total{reason=\"dispatch_failed\"}"),
        1
    );

    // Retention: a terminal record is dropped after `job_retention_ms`.
    let mut b = actor();
    warm(&mut b, "w-a", 1, 4, 0);
    b.handle(5, claim_msg("w-a", 1, 4));
    b.handle(10, submit_msg("job-r", "kr", None));
    let done = b.handle(20, done_msg("w-a", 1, "job-r", 1, None));
    assert_eq!(
        terminals(&done).first().expect("terminal").1,
        TerminalReason::Completed
    );
    assert!(
        b.has_job(&JobId::new_unchecked("job-r")),
        "the record is kept for replay first"
    );
    assert_eq!(b.retention_len(), 1);
    // Retention is measured from the terminal moment (`finish_job` stamps
    // `terminal_ms + job_retention_ms`), so the record is swept at 20 + 30 000 — the
    // boundary itself, since the sweep takes everything whose deadline is `<= now`.
    let later = b.handle(30_020, ActorMsg::Tick { now_ms: 30_020 });
    // NEX-6.3's deletion is **reported**, not silent: the hub's `job_index` and relay
    // maps are keyed by job id, and `Effect::JobForgotten` is the only signal that they
    // may be pruned. It stays silent to *consumers* — the terminal event went out at
    // t = 20, and a job that is merely forgotten must not invent a second verdict.
    assert_eq!(
        forgotten(&later),
        vec!["job-r".to_string()],
        "the retention sweep deletes the record and names it"
    );
    assert!(
        relay_events(&later).is_empty() && terminals(&later).is_empty(),
        "no consumer-visible event accompanies the deletion"
    );
    assert!(!b.has_job(&JobId::new_unchecked("job-r")));
    assert_eq!(b.retention_len(), 0);
}

// ------------------------------------------------ NEX-6.3 / NEX-6.6: who may keep a task

#[test]
fn a_chat_task_that_loses_its_last_consumer_is_deleted_with_its_cancel() {
    // NEX-6.3's decisive trace, resolved in code: "the task is then **deleted**: no
    // queue entry, no lease, no record, no retained stream, no idempotency entry that
    // will hand the same answer to a later stranger. A task with no attached consumer
    // has no reason to exist." The question §9.1 item 2 asks — "does the next submission
    // with the same idempotency key execute fresh, or attach to the dead execution and
    // replay `job ended: cancelled`?" — is answered **fresh**, in the same mailbox step
    // as the cancel, because the hub is single-threaded by construction.
    let mut a = actor();
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(10, submit_msg("job-1", "k1", None));
    a.handle(20, claim_msg("w-a", 1, 1));
    assert_eq!(
        a.leases_len(),
        1,
        "w-a holds the job before the socket dies"
    );

    let gone = a.handle(
        30,
        ActorMsg::Detach {
            job: JobId::new_unchecked("job-1"),
            consumer: ConsumerId::new_unchecked("con-job-1"),
        },
    );
    // The consumer-visible half is exactly what it was: NEX-6.4's order (worker first),
    // the terminal event in band.
    assert!(
        sent_to(&gone, "w-a").iter().any(|f| matches!(
            f,
            HubFrame::Cancel {
                reason: CancelReason::ConsumerGone,
                ..
            }
        )),
        "cancel must fan out on disconnect"
    );
    assert_eq!(
        terminals(&gone),
        vec![("job-1".to_string(), TerminalReason::Cancelled, false)],
        "the consumer's stream ends in band"
    );
    // The deletion half — NEX-6.3's list, in this same step, reported so the embedder's
    // own job-id maps can go with it.
    assert_eq!(
        forgotten(&gone),
        vec!["job-1".to_string()],
        "the actor reports the deletion so the hub prunes its routing entry"
    );
    assert!(!a.has_job(&JobId::new_unchecked("job-1")), "no record");
    assert_eq!(a.leases_len(), 0, "no lease");
    assert_eq!(a.queue_depth(), 0, "no queue entry");
    assert_eq!(
        a.retention_len(),
        0,
        "no retention entry either: the job is deleted, not retired"
    );
    assert!(
        a.workers()[&WorkerId::new_unchecked("w-a")]
            .in_flight
            .is_empty(),
        "the worker holds no slot for a job that no longer exists"
    );

    // A frame still in flight from the cancelled worker is refused, never relayed into a
    // stream nobody watches, and the worker is told to stop producing.
    let late = a.handle(31, delta_msg("w-a", 1, "job-1", 2, "still producing"));
    assert!(
        relay_events(&late).is_empty(),
        "bytes that arrive after the deletion are refused, not relayed"
    );
    assert!(
        sent_to(&late, "w-a").iter().any(|f| matches!(
            f,
            HubFrame::Cancel {
                reason: CancelReason::LeaseExpired,
                ..
            }
        )),
        "the refusal tells the worker to stop"
    );
    assert_eq!(a.metrics().get("nexus_inband_errors_total"), 1);

    // The decisive half: same idempotency key, new job id, and a **fresh** execution.
    let retry = a.handle(40, submit_msg("job-2", "k1", None));
    assert_eq!(
        accepted(&retry),
        vec![("job-2".to_string(), false, Vec::new())],
        "not attached and nothing replayed: the dead execution is not resurrected"
    );
    assert_eq!(
        a.metrics().get("nexus_idempotent_attach_total"),
        0,
        "the idempotency entry was deleted with the task, so there was nothing to attach to"
    );
    assert_eq!(
        a.job(&JobId::new_unchecked("job-2"))
            .expect("the retry exists")
            .attempt,
        0,
        "a brand new execution, not a continuation of the cancelled one"
    );
}

#[test]
fn a_detached_queue_without_a_subscriber_is_capped_by_the_orphan_ttl() {
    // §9.1 item 3: "a detached job is the one permitted exception [to *no consumer, no
    // task*]; cap its lifetime while it has no subscriber." The cap is
    // `detached_orphan_ttl_ms`, and because the actor runs on a virtual clock it is a
    // `Tick` statement, never a wall-clock sleep.
    let mut cfg = config();
    cfg.limits.detached_orphan_ttl_ms = 20_000;
    // Neither the no-worker grace nor the queue deadline may answer first: the deletion
    // under test has to be attributable to this cap and to nothing else.
    cfg.limits.no_worker_grace_ms = 600_000;
    cfg.limits.claim_deadline_ms = 600_000;
    let mut a = PoolActor::new(pool(), cfg);
    // Connected but loading: the job stays queued and nothing dispatches it.
    a.handle(0, hello_msg("w-load", 1, 4));
    a.handle(5, claim_msg("w-load", 1, 4));
    let mut detached = submit_msg_with("job-d", "kd", None, chat_body(&"x".repeat(300)), None);
    if let ActorMsg::Submit { submission, .. } = &mut detached {
        // `POST /v1/jobs` semantics (NEX-6.1): this socket is not the owner, so the task
        // goes on without one.
        submission.cancel_on_consumer_gone = false;
    }
    a.handle(10, detached);
    assert_eq!(
        a.queue_depth(),
        1,
        "queued, waiting for a worker that is still loading"
    );
    assert!(a.has_job(&JobId::new_unchecked("job-d")));
    // 300 characters of prompt is above the 64 B inline cap, so the queued body is held
    // by reference and charged to the spill budget: 374 bytes is the serialized
    // `chat_body` (56 + 300 + 18), and the deletion has to give every byte of it back.
    assert_eq!(
        a.payloads().bytes(),
        374,
        "the queued prompt occupies spill budget"
    );

    // The only socket that could ever follow this job closes at t = 20. The task survives
    // that — §8's "keep it running" — so nothing is deleted yet.
    let left = a.handle(
        20,
        ActorMsg::Detach {
            job: JobId::new_unchecked("job-d"),
            consumer: ConsumerId::new_unchecked("con-job-d"),
        },
    );
    assert!(
        forgotten(&left).is_empty(),
        "the cap runs from the detach, and 20 - 20 is inside it"
    );
    assert_eq!(a.queue_depth(), 1, "NEX-6.6 keeps the queued work alive");

    // One millisecond short of the window and the state is still there: the bound is the
    // number, not an approximation of it.
    let before = a.handle(20_019, ActorMsg::Tick { now_ms: 20_019 });
    assert!(
        forgotten(&before).is_empty(),
        "19 999 ms of a 20 000 ms window"
    );
    assert!(a.has_job(&JobId::new_unchecked("job-d")));

    // On the boundary the whole state stops existing.
    let swept = a.handle(20_020, ActorMsg::Tick { now_ms: 20_020 });
    assert_eq!(forgotten(&swept), vec!["job-d".to_string()]);
    assert!(!a.has_job(&JobId::new_unchecked("job-d")), "no record");
    assert_eq!(a.queue_depth(), 0, "no queue entry");
    assert_eq!(
        a.retention_len(),
        0,
        "and it is not parked in retention either"
    );
    assert_eq!(
        a.payloads().bytes(),
        0,
        "the queued prompt's bytes go back to the budget"
    );
    assert_eq!(
        a.metrics().get("nexus_queue_depth"),
        0,
        "the published gauge moves with the deletion"
    );

    // Its idempotency key went with it, so the same key executes fresh: §9.1 item 2's
    // decisive question, asked of the detached shape this cap exists for. It goes first
    // because the fresh execution is itself a **live** job: once the capacity probe below
    // fills the tenant's budget, a new execution would answer 429 and prove nothing about
    // the key.
    let retry = a.handle(20_100, submit_msg("job-e", "kd", None));
    assert_eq!(
        accepted(&retry),
        vec![("job-e".to_string(), false, Vec::new())],
        "the key is free, so a new execution starts instead of an attach"
    );

    // NEX-7.1's tenant cap is what the deletion gave back. The cap is **4** concurrent
    // jobs for `acme` and admission is `active < cap`
    // (`tenant_concurrency_cap_answers_429_and_releases_on_terminal` admits 1..=4 and
    // 429s on the 5th), so the freed slot is exactly the room `job-e` plus `job-n1..=n3`
    // now occupy: four live jobs, and the fifth is the cap speaking. Had the deleted
    // record kept its slot, `job-n3` would already be the 429; had the slot been released
    // twice, `job-full` would have been admitted as well.
    for n in 1..=3 {
        let id = format!("job-n{n}");
        let effects = a.handle(20_110 + n, submit_msg(&id, &format!("kn{n}"), None));
        assert_eq!(
            accepted(&effects),
            vec![(id.clone(), false, Vec::new())],
            "{id} must be admitted into the freed capacity"
        );
    }
    assert_eq!(
        rejects(&a.handle(20_200, submit_msg("job-full", "kfull", None))),
        vec![(429, "tenant_limit".to_string(), Some(1_000))],
        "the deleted job gave back exactly one slot, no more"
    );
}

#[test]
fn a_detached_job_with_a_live_lease_outlives_the_orphan_ttl_and_stays_replayable() {
    // The restriction that makes the cap safe. Once a worker holds the lease, GPU work
    // has been committed: killing a job 25 s into a render because a poller hung up would
    // throw away the expensive part to save the bookkeeping. Claimed work is bounded by
    // `job_hard_cap_ms`, and once terminal by `job_retention_ms` — never by this clock.
    let mut cfg = config();
    cfg.limits.detached_orphan_ttl_ms = 20_000;
    let mut a = PoolActor::new(pool(), cfg);
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));
    let mut detached = submit_msg("job-c", "kc", None);
    if let ActorMsg::Submit { submission, .. } = &mut detached {
        submission.cancel_on_consumer_gone = false;
    }
    a.handle(10, detached);
    assert_eq!(a.leases_len(), 1, "a worker took it straight away");

    let left = a.handle(
        30,
        ActorMsg::Detach {
            job: JobId::new_unchecked("job-c"),
            consumer: ConsumerId::new_unchecked("con-job-c"),
        },
    );
    assert!(
        forgotten(&left).is_empty(),
        "the lease holder is still producing: this is the case the cap must not kill"
    );
    assert!(
        sent_to(&left, "w-a")
            .iter()
            .all(|f| !matches!(f, HubFrame::Cancel { .. })),
        "no Cancel reaches the GPU"
    );
    assert_eq!(a.leases_len(), 1, "and it keeps its slot");

    // t = 25 000: past the detached cap, inside the lease (`lease_ms` 60 000) and the
    // hard cap, so the subscriber-less sweep must leave it alone.
    let held = a.handle(25_000, ActorMsg::Tick { now_ms: 25_000 });
    assert!(
        forgotten(&held).is_empty(),
        "a claimed job is bounded by `job_hard_cap_ms`, not by the subscriber-less clock"
    );
    assert_eq!(
        a.job(&JobId::new_unchecked("job-c"))
            .expect("the job is still known")
            .phase,
        JobPhase::Assigned,
        "still owned by w-a"
    );
    assert_eq!(a.leases_len(), 1, "the worker keeps its slot");

    // The answer arrives, and the two legitimate clocks take over: the record is retained
    // for `job_retention_ms`, and a retry with the same key attaches and replays it
    // instead of re-running the GPU — NEX-7.2's "an answer a retry must not re-run".
    a.handle(26_000, delta_msg("w-a", 1, "job-c", 1, "a frame"));
    let done = a.handle(27_000, done_msg("w-a", 1, "job-c", 2, None));
    assert_eq!(
        terminals(&done).first().expect("terminal").1,
        TerminalReason::Completed
    );
    assert!(
        forgotten(&done).is_empty(),
        "a terminal answer is retained, not deleted"
    );
    assert_eq!(
        a.retention_len(),
        1,
        "bounded by retention from the terminal moment"
    );

    let retry = a.handle(28_000, submit_msg("job-c2", "kc", None));
    let (job, attached, replayed) = accepted(&retry)
        .first()
        .expect("an Accepted effect")
        .clone();
    assert_eq!(
        job, "job-c",
        "the retry joins the execution that has the answer"
    );
    assert!(attached, "and does not start a second execution");
    assert!(
        matches!(replayed.last(), Some(ConsumerEvent::Finish { .. })),
        "the finished stream is replayed to it"
    );
    assert_eq!(a.metrics().get("nexus_idempotent_attach_total"), 1);

    // Terminal at 27 000 plus the harness's 30 000 ms retention: 57 000 is the boundary,
    // and the orphan TTL had nothing to do with any of it.
    let before = a.handle(56_999, ActorMsg::Tick { now_ms: 56_999 });
    assert!(forgotten(&before).is_empty(), "one ms short of retention");
    assert!(a.has_job(&JobId::new_unchecked("job-c")));
    let later = a.handle(57_000, ActorMsg::Tick { now_ms: 57_000 });
    assert_eq!(forgotten(&later), vec!["job-c".to_string()]);
    assert!(!a.has_job(&JobId::new_unchecked("job-c")));
    assert_eq!(a.retention_len(), 0);
}

#[test]
fn memory_only_mode_refuses_a_kind_that_writes_an_artifact_and_admits_the_rest() {
    // NEX-7.1 / §9.1 item 5: an empty artifact root is the operator's explicit "this
    // deployment has no scratch disk" opt-out, and bulk artifact egress is the only thing
    // this hub writes anywhere. A kind whose answer is a file can never be served, so the
    // refusal happens at **admission**: once the response body is open a clean 4xx is no
    // longer legal (§7.2), so waiting for the worker's first `PUT` would be too late.
    let mut a = PoolActor::with_artifact_root(pool(), config(), "");
    assert!(
        a.artifacts().root().is_empty(),
        "an empty root is the mode flag"
    );
    warm(&mut a, "w-a", 1, 4, 0);
    a.handle(5, claim_msg("w-a", 1, 4));

    // `notify.webhook` is the reference config's non-streaming kind on this pool: its
    // answer is an artifact, which is exactly the pair the mode cannot serve.
    let mut webhook = submit_msg("job-w", "kw", None);
    if let ActorMsg::Submit { submission, .. } = &mut webhook {
        submission.kind = "notify.webhook".to_string();
        submission.streaming = false;
        submission.idempotent = false;
        submission.requires = Requires::default();
    }
    let refused = a.handle(10, webhook);
    assert_eq!(
        rejects(&refused),
        vec![(400, "artifact_unavailable".to_string(), None)],
        "refused before any body byte, with the reason the operator can act on"
    );
    assert!(
        !a.has_job(&JobId::new_unchecked("job-w")),
        "refused before the record existed"
    );
    assert_eq!(a.queue_depth(), 0, "and before the queue was touched");
    assert_eq!(
        a.metrics()
            .get("nexus_jobs_rejected_total{reason=\"artifact_unavailable\"}"),
        1,
        "the refusal is counted under its own reason"
    );

    // A streaming kind answers on the response body, so the same mode still serves it:
    // the rule is about artifact output, not about disk in general.
    let ok = a.handle(11, submit_msg("job-s", "ks", None));
    assert_eq!(
        accepted(&ok),
        vec![("job-s".to_string(), false, Vec::new())],
        "a streaming kind needs no scratch disk"
    );
    assert!(a.has_job(&JobId::new_unchecked("job-s")));

    // And a hub with the default root admits the artifact kind normally, which is the
    // half that keeps `PoolActor::new` deployments untouched.
    let mut d = actor();
    warm(&mut d, "w-a", 1, 4, 0);
    d.handle(5, claim_msg("w-a", 1, 4));
    let mut second = submit_msg("job-w2", "kw2", None);
    if let ActorMsg::Submit { submission, .. } = &mut second {
        submission.kind = "notify.webhook".to_string();
        submission.streaming = false;
        submission.idempotent = false;
        submission.requires = Requires::default();
    }
    assert_eq!(
        accepted(&d.handle(10, second)),
        vec![("job-w2".to_string(), false, Vec::new())],
        "with a root configured, nothing about admission changes"
    );
}

#[test]
fn the_actor_never_reports_two_owners_for_one_job() {
    // The invariant every other test leans on: exactly one live lease per job,
    // and the queue never holds a leased job.
    let mut a = actor();
    warm(&mut a, "w-a", 1, 2, 0);
    warm(&mut a, "w-b", 2, 2, 0);
    for n in 1..=6 {
        a.handle(
            10 + n,
            submit_msg(&format!("job-{n}"), &format!("k{n}"), None),
        );
    }
    for t in 0..40u64 {
        let now = 100 + t * 13;
        a.handle(now, claim_msg("w-a", 1, 2));
        a.handle(now + 1, claim_msg("w-b", 2, 2));
        for worker in ["w-a", "w-b"] {
            let conn = if worker == "w-a" { 1 } else { 2 };
            for job in 1..=6 {
                let id = JobId::new_unchecked(format!("job-{job}"));
                if a.lease_of(&id).is_some_and(|l| l.worker.as_ref() == worker) {
                    let _ = a.handle(
                        now + 2,
                        delta_msg(worker, conn, &format!("job-{job}"), t, &"z".repeat(400)),
                    );
                    if t % 7 == 0 {
                        let _ = a.handle(
                            now + 3,
                            done_msg(worker, conn, &format!("job-{job}"), t, None),
                        );
                    }
                }
            }
        }
        let _ = a.handle(now + 5, ActorMsg::Tick { now_ms: now + 5 });
        for job in 1..=6 {
            let id = JobId::new_unchecked(format!("job-{job}"));
            let owners = a
                .workers()
                .values()
                .filter(|w| w.in_flight.contains(&id))
                .count();
            let leased = a.lease_of(&id).map(|l| l.worker.clone());
            assert!(owners <= 1, "job-{job} has {owners} owners at round {t}");
            if let Some(owner) = leased {
                assert_eq!(owners, 1, "a leased job occupies exactly one worker");
                let record = a.job(&id).expect("record");
                assert_ne!(
                    record.phase,
                    JobPhase::Queued,
                    "a leased job is not also queued"
                );
                assert_eq!(
                    record.attempt,
                    a.lease_of(&id).expect("l").attempt,
                    "the lease attempt matches the job"
                );
                assert_eq!(owner, a.lease_of(&id).expect("l").worker);
            }
        }
    }
    let in_flight: usize = a.workers().values().map(|w| w.in_flight.len()).sum();
    assert_eq!(
        a.leases_len(),
        in_flight,
        "the lease table and the worker slots agree"
    );
}
