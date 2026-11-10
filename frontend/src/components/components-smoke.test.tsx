// Component smoke tests via react-dom/server (no jsdom): each check pins an
// exact user-facing string, so a panel that silently stops rendering the
// state it claims to render fails here.
import { describe, expect, it } from 'vitest';
import { renderToString } from 'react-dom/server';
import { normalizeObserveSnapshot } from '../api/nexus-client';
import { parsePrometheusText } from '../api/prometheus';
import { ConnectBar } from './ConnectBar';
import { ConnectScreen } from './ConnectScreen';
import { ConsumersPanel } from './ConsumersPanel';
import { JobsPanel } from './JobsPanel';
import { MetricsPanel } from './MetricsPanel';
import { OverviewPanel } from './OverviewPanel';
import { PoolsPanel } from './PoolsPanel';
import { SuppliersPanel } from './SuppliersPanel';
import { DEFAULT_HUB_URL } from '../utils/hub-url';

const NOW = 1_762_760_000_000;
const snapshotOf = (payload: unknown) => normalizeObserveSnapshot(payload, NOW);

const EMPTY_SNAPSHOT = snapshotOf({ protocol: 'nexus-hub-observe/v1', now_ms: NOW });
// No now_ms at all → serverNowMs 0 → the panel must say "not reported".
const MINIMAL_SNAPSHOT = snapshotOf({ protocol: 'nexus-hub-observe/v1' });

const RICH_SNAPSHOT = snapshotOf({
    protocol: 'nexus-hub-observe/v1',
    now_ms: NOW,
    readiness: { ready_workers: 2, loading_workers: 0, queue_depth: 4, mailbox_pending: 1, warm_models: ['qwen38'], capacity_signal: 'no_workers' },
    pools: [
        { id: 'llm.chat/qwen38', queue_depth: 4, ready_workers: 2, connected_workers: 2, leases: 2, mailbox_pending: 1 },
        { id: 'llm.embed/small', queue_depth: 0 }
    ],
    unresponsive_pools: ['llm.embed/small'],
    workers: [{ id: 'w-1', pools: ['llm.chat/qwen38'], link: 'live', engine: 'ready', epoch: 3, capacity_slots: 4, free_slots: 1, free_kv_tokens: 4096, in_flight: ['job-1'], outstanding_claims: 2, last_seen_ms: NOW - 2000, telemetry: { temp_c: 61 } }],
    jobs: [
        { id: 'job-1', pool: 'llm.chat/qwen38', tenant: 'default', phase: 'assigned', created_ms: NOW - 3000, consumers: 1, lease: { worker: 'w-1', epoch: 3, attempt: 1, deadline_ms: NOW + 4000 } },
        { id: 'job-2', pool: 'llm.chat/qwen38', tenant: 'tenant-b', phase: 'queued', created_ms: NOW - 1000, consumers: 3, lease: null }
    ],
    consumer_connections: [{ id: 'c-1', tenant: 'default', since_ms: NOW - 9000, attached_jobs: ['job-1'] }]
});

const METRICS = parsePrometheusText(
    [
        '# TYPE nexus_jobs_rejected_total counter',
        'nexus_jobs_rejected_total{reason="no_capacity"} 2',
        '# TYPE nexus_dispatch_latency_ms summary',
        'nexus_dispatch_latency_ms_count 4',
        'nexus_dispatch_latency_ms_sum 120'
    ].join('\n')
);

describe('ConnectScreen', () => {
    it('offers the hub default as the placeholder and prefills the stored URL', () => {
        const blank = renderToString(<ConnectScreen initialUrl="" onConnect={() => undefined} />);
        expect(blank).toContain('Nexus Monitor');
        expect(blank).toContain(`placeholder="${DEFAULT_HUB_URL}"`);
        expect(blank).toContain('value=""');

        const prefilled = renderToString(<ConnectScreen initialUrl="https://hub.example" onConnect={() => undefined} />);
        expect(prefilled).toContain('value="https://hub.example"');
    });
});

describe('ConnectBar', () => {
    const base = {
        baseUrl: 'http://127.0.0.1:8085',
        onReconnect: () => undefined,
        onDisconnect: () => undefined,
        onRefresh: () => undefined,
        onTogglePaused: () => undefined,
        nowMs: NOW,
        lastSuccessMs: NOW - 3000,
        failureCount: 0,
        nextAttemptMs: null
    };

    it('labels a healthy poll with its age', () => {
        const html = renderToString(<ConnectBar {...base} status="live" paused={false} />);
        expect(html).toContain('live · 3s ago');
        expect(html).toContain('>Pause<');
        // Refresh is available while running.
        expect(html).not.toContain('disabled');
    });

    it('labels a paused poll and disables manual refresh', () => {
        const html = renderToString(<ConnectBar {...base} status="paused" paused={true} />);
        expect(html).toContain('paused · 3s ago');
        expect(html).toContain('>Resume<');
        expect(html).toContain('disabled=""');
    });

    it('counts failures and announces the backoff retry', () => {
        const html = renderToString(<ConnectBar {...base} status="error" paused={false} failureCount={2} nextAttemptMs={NOW + 2500} />);
        expect(html).toContain('unreachable (2×) — retrying in 3s');
    });

    it('says "connecting…" before the first snapshot', () => {
        expect(renderToString(<ConnectBar {...base} status="pending" paused={false} />)).toContain('connecting…');
    });
});

describe('OverviewPanel', () => {
    it('renders readiness, the lease split and the cumulative counters', () => {
        const html = renderToString(<OverviewPanel snapshot={RICH_SNAPSHOT} metrics={METRICS} nowMs={NOW} />);
        expect(html).toContain('>no_workers<');
        expect(html).toContain('1 warm model(s)');
        expect(html).toContain('1 leased · 1 unleased');
        // accepted/terminal are not in this exposition → exactly two n/a
        // cards, while rejected (2) is shown.
        expect(html.match(/n\/a/g)?.length).toBe(2);
    });

    it('marks every counter as n/a when /metrics has not landed', () => {
        const html = renderToString(<OverviewPanel snapshot={MINIMAL_SNAPSHOT} metrics={null} nowMs={NOW} />);
        // Three counters, none of them a fabricated zero.
        expect(html.match(/n\/a/g)?.length).toBe(3);
        // A hub that does not report now_ms says so instead of showing 1970.
        expect(html).toContain('hub clock not reported');
    });

    it('lists warm models instead of hiding them behind a count', () => {
        // `<!-- -->` is React's SSR text-node separator between the label and
        // the interpolated list — pinned exactly rather than loosely matched.
        expect(renderToString(<OverviewPanel snapshot={RICH_SNAPSHOT} metrics={METRICS} nowMs={NOW} />)).toContain(
            '<span class="muted">Warm models:</span> <!-- -->qwen38</p>'
        );
    });
});

describe('PoolsPanel', () => {
    it('badges the pools the hub flagged as unresponsive', () => {
        const html = renderToString(<PoolsPanel snapshot={RICH_SNAPSHOT} />);
        expect(html).toContain('llm.embed/small');
        expect(html).toContain('unresponsive');
        expect(html).toContain('pool-flagged');
    });

    it('explains an empty pool list', () => {
        expect(renderToString(<PoolsPanel snapshot={EMPTY_SNAPSHOT} />)).toContain('No pools on this hub.');
    });

    it('lists unresponsive ids that have no pool card, and only those', () => {
        // llm.embed/small has a card (badged, not listed); llm.gone/x does
        // not, so it must appear in the note — otherwise the flag is only a
        // bare count on the Overview panel.
        const html = renderToString(
            <PoolsPanel
                snapshot={snapshotOf({
                    pools: [{ id: 'llm.chat/qwen38' }, { id: 'llm.embed/small' }],
                    unresponsive_pools: ['llm.embed/small', 'llm.gone/x']
                })}
            />
        );
        expect(html).toContain('pool-flagged');
        expect(html).toContain('Unresponsive pools with no reported stats:</span> <span class="mono">llm.gone/x</span>');
    });

    it('omits the note when every flagged pool has a card', () => {
        expect(renderToString(<PoolsPanel snapshot={RICH_SNAPSHOT} />)).not.toContain('no reported stats');
    });
});

describe('SuppliersPanel', () => {
    it('renders worker state, slots and the telemetry affordance', () => {
        const html = renderToString(<SuppliersPanel snapshot={RICH_SNAPSHOT} nowMs={NOW} />);
        expect(html).toContain('>live<');
        expect(html).toContain('>ready<');
        expect(html).toContain('<td>3</td>'); // epoch
        // React splits the interpolated slot ratio into separate text nodes.
        expect(html).toContain('1<!-- -->/<!-- -->4');
        // 3 of 4 slots used → the usage bar fills 75%.
        expect(html).toContain('width:75%');
        expect(html).toContain('>telemetry<');
    });

    it('renders a missing KV budget as an em dash and a spent one as 0', () => {
        // The hub keeps null and 0 apart on purpose: null = the worker never
        // declared a KV budget, 0 = it declared one and has spent it all
        // (crates/nexus-server/src/observe.rs,
        // `worker_row_reports_a_kv_budget_of_null_rather_than_zero`). A `|| 0`
        // coercion here would report every unreported worker as exhausted.
        const budgetRow = (id: string, freeKvTokens: unknown) => ({
            id,
            pools: ['llm.chat/qwen38'],
            link: 'live',
            engine: 'ready',
            epoch: 1,
            capacity_slots: 1,
            free_slots: 1,
            free_kv_tokens: freeKvTokens,
            in_flight: ['job-1'],
            outstanding_claims: 1,
            last_seen_ms: NOW - 2000
        });
        const snapshot = snapshotOf({ workers: [budgetRow('w-null', null), budgetRow('w-zero', 0)] });
        // Normalization keeps the distinction (no coercion to 0).
        expect(snapshot.workers.map((worker) => worker.freeKvTokens)).toEqual([null, 0]);
        // Exactly one em-dash cell across both rows: w-null's kv cell. Every
        // other cell in these rows has a value, so the count also proves
        // w-zero's budget printed as a number rather than as a dash.
        expect(renderToString(<SuppliersPanel snapshot={snapshot} nowMs={NOW} />).match(/<td>—<\/td>/g)).toHaveLength(1);
        // And 0 prints literally (epoch/in-flight/claims are all 1 in these rows).
        expect(renderToString(<SuppliersPanel snapshot={snapshot} nowMs={NOW} />)).toContain('<td>0</td>');
    });

    it('tells the operator how to get a supplier', () => {
        expect(renderToString(<SuppliersPanel snapshot={EMPTY_SNAPSHOT} nowMs={NOW} />)).toContain('No suppliers connected.');
    });
});

describe('JobsPanel', () => {
    it('names the leasing worker and marks queued jobs explicitly', () => {
        const html = renderToString(<JobsPanel snapshot={RICH_SNAPSHOT} nowMs={NOW} />);
        expect(html).toContain('w-1');
        expect(html).toContain('queued'); // job-2 has no lease
        expect(html).toContain('4s'); // remaining lease deadline
        // job-1 is `assigned`, a phase the tone map does not know, so it must
        // still print its real label on the neutral chip (no fake state).
        expect(html).toContain('>assigned<');
    });

    it('says the queue is empty rather than "no data"', () => {
        expect(renderToString(<JobsPanel snapshot={EMPTY_SNAPSHOT} nowMs={NOW} />)).toContain('No active jobs.');
    });
});

describe('ConsumersPanel', () => {
    it('lists live connections and their attached jobs', () => {
        const html = renderToString(<ConsumersPanel snapshot={RICH_SNAPSHOT} nowMs={NOW} />);
        expect(html).toContain('c-1');
        expect(html).toContain('9s ago');
        expect(html).toContain('job-1');
    });

    it('falls back to per-job counts and explains why', () => {
        const noConnections = snapshotOf({
            jobs: [{ id: 'job-9', pool: 'p', tenant: 'default', phase: 'streaming', consumers: 4 }]
        });
        const html = renderToString(<ConsumersPanel snapshot={noConnections} nowMs={NOW} />);
        expect(html).toContain('No live consumer connections reported.');
        expect(html).toContain('those streams are not individually observable in this hub build');
        expect(html).toContain('job-9');
        expect(html).toContain('<td>4</td>');
    });
});

describe('MetricsPanel', () => {
    it('computes the latency average and keeps label breakdowns', () => {
        const html = renderToString(<MetricsPanel metrics={METRICS} metricsError={null} />);
        // 120 ms / 4 samples = 30.0 ms
        expect(html).toContain('30.0 ms');
        expect(html).toContain('nexus_jobs_rejected_total');
        // Quotes are HTML-escaped in the markup.
        expect(html).toContain('reason=&quot;no_capacity&quot;');
        expect(html).toContain('>counter<');
    });

    it('shows n/a for a summary that has never been observed', () => {
        const html = renderToString(<MetricsPanel metrics={parsePrometheusText('nexus_ttft_ms_count 0\nnexus_ttft_ms_sum 0')} metricsError={null} />);
        // dispatch/latency families absent → both n/a; ttft count 0 → n/a.
        expect(html.match(/n\/a/g)?.length).toBe(3);
    });

    it('reports a /metrics failure without hiding the table', () => {
        const html = renderToString(<MetricsPanel metrics={null} metricsError="HTTP 404 — no /metrics on that origin" />);
        expect(html).toContain('/metrics unavailable.');
        expect(html).toContain('HTTP 404 — no /metrics on that origin');
        expect(html).toContain('No metrics yet.');
    });
});
