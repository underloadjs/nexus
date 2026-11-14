// Overview panel: the "is the hub healthy right now" strip.
//
// Two data sources meet here on purpose: readiness/jobs/pools come from
// /v1/observe (instantaneous state) while accepted/rejected/terminal job
// counters come from /metrics (cumulative). They are labelled accordingly —
// mixing an instantaneous queue depth with a cumulative counter in one
// number is how dashboards lie.
import { counterTotal, type MetricFamily } from '../api/prometheus';
import type { CapacitySignal, NormalizedSnapshot } from '../api/nexus-client';
import { formatAge, formatClockTime, formatCounter } from '../utils/format';
import { EmptyState, Panel, StatCard, type Tone } from './ui';

// Hub capacity signal → chip tone (an unknown signal stays neutral).
const SIGNAL_TONES: Record<CapacitySignal, Tone> = {
    warm: 'ok',
    cold_start: 'warn',
    no_workers: 'bad'
};

export interface OverviewPanelProps {
    snapshot: NormalizedSnapshot;
    // null until /metrics answers (the hub may not expose it at all).
    metrics: MetricFamily[] | null;
    nowMs: number;
}

export function OverviewPanel({ snapshot, metrics, nowMs }: OverviewPanelProps) {
    const { readiness } = snapshot;
    const signalTone: Tone = SIGNAL_TONES[readiness.capacitySignal] ?? 'neutral';

    // Only the queue-depth card needs a job split now (the active-jobs card
    // that also used `leased` was removed): a queued job is one the scheduler
    // has not leased to a worker yet.
    const queued = snapshot.jobs.filter((job) => job.lease === null).length;

    // Cumulative counters; null = the hub does not expose that metric, which
    // the card renders as "n/a" rather than a misleading 0.
    const accepted = metrics ? counterTotal(metrics, 'nexus_jobs_accepted_total') : null;
    const rejected = metrics ? counterTotal(metrics, 'nexus_jobs_rejected_total') : null;
    const terminal = metrics ? counterTotal(metrics, 'nexus_jobs_terminal_total') : null;

    return (
        <Panel
            title="Overview"
            count={`${snapshot.pools.length} pools`}
            hint={`Snapshot state from /v1/observe, counters from /metrics · polled ${formatAge(snapshot.fetchedAtMs, nowMs)}`}
        >
            <div className="stat-grid">
                <StatCard
                    label="Capacity signal"
                    value={readiness.capacitySignal}
                    tone={signalTone}
                    sub={readiness.warmModels.length > 0 ? `${readiness.warmModels.length} warm model(s)` : 'no warm models'}
                />
                <StatCard label="Queue depth" value={readiness.queueDepth} tone={readiness.queueDepth > 0 ? 'warn' : 'ok'} sub={`${queued} queued job(s) shown`} />
                {/*
                    Mailbox pending is NOT jobs and NOT consumer messages: it is
                    the depth of each pool actor's bounded (512-slot) tokio mpsc
                    inbox, summed across pools — the backpressure / wedged-actor
                    signal, read from outside the actor.
                    Server side: services/nexus/crates/nexus-server/src/state.rs:1041
                    (`mailbox_pending`, "Pending messages in the mailbox, the
                    backpressure signal"), aggregated in `Hub::readiness`
                    (state.rs:934-946); rationale in observe.rs:165-167.
                    Wire field: readiness.mailbox_pending → NormalizedReadiness.mailboxPending
                    (api/nexus-client.ts:170). Sustained non-zero means the pool
                    actor is behind its incoming message stream; the hard cap is
                    512 per pool.
                */}
                <StatCard label="Mailbox pending" value={readiness.mailboxPending} tone={readiness.mailboxPending > 0 ? 'warn' : 'neutral'} sub="messages queued to pool actors" />
                <StatCard
                    label="Consumer connections"
                    value={snapshot.consumerConnections.length}
                    // 0 with jobs present usually means the hub does not
                    // report live connections (older build) — say so.
                    sub={
                        snapshot.consumerConnections.length === 0 && snapshot.jobs.length > 0
                            ? 'not reported — see Consumers'
                            : 'live streams'
                    }
                />
                <StatCard label="Unresponsive pools" value={snapshot.unresponsivePools.length} tone={snapshot.unresponsivePools.length > 0 ? 'bad' : 'ok'} />
                <StatCard
                    label="Snapshot age"
                    value={formatAge(snapshot.fetchedAtMs, nowMs)}
                    // Hub clock (`now_ms`) next to our own makes browser/hub
                    // drift visible instead of silently skewing every age.
                    sub={snapshot.serverNowMs > 0 ? `hub clock ${formatClockTime(snapshot.serverNowMs)}` : 'hub clock not reported'}
                />
                <StatCard label="Jobs accepted" value={accepted === null ? 'n/a' : formatCounter(accepted)} sub="cumulative · /metrics" />
                <StatCard
                    label="Jobs rejected"
                    value={rejected === null ? 'n/a' : formatCounter(rejected)}
                    tone={rejected && rejected > 0 ? 'warn' : 'neutral'}
                    sub="cumulative · /metrics"
                />
                <StatCard label="Jobs terminal" value={terminal === null ? 'n/a' : formatCounter(terminal)} sub="cumulative · /metrics" />
            </div>

            {readiness.warmModels.length > 0 ? (
                // Warm models are the difference between an instant dispatch
                // and a cold start — listing them beats a bare count.
                <p className="model-list">
                    <span className="muted">Warm models:</span> {readiness.warmModels.join(', ')}
                </p>
            ) : null}

            {snapshot.pools.length === 0 ? <EmptyState message="No pools registered yet." hint="Pools appear once a supplier worker connects to one." /> : null}
        </Panel>
    );
}
