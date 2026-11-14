// Component smoke tests via react-dom/server (no jsdom): each check pins an
// exact user-facing string, so a panel that silently stops rendering the
// state it claims to render fails here.
import { describe, expect, it, vi } from 'vitest';
import { renderToString } from 'react-dom/server';
import { normalizeObserveSnapshot } from '../api/nexus-client';
import { parsePrometheusText } from '../api/prometheus';
import { ConnectScreen } from './ConnectScreen';
import { ConsumersPanel } from './ConsumersPanel';
import { MetricsPanel } from './MetricsPanel';
import { OverviewPanel } from './OverviewPanel';
import { StatusBar } from './StatusBar';
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

describe('StatusBar', () => {
    // Fixed clock for every case: `formatAge` (utils/format.ts:23) and the
    // backoff countdown (StatusBar.tsx:33) are pure functions of these numbers,
    // so each chip label below is a literal string in the assertion.
    const base = {
        nowMs: NOW,
        lastSuccessMs: NOW - 3000,
        failureCount: 0,
        nextAttemptMs: null as number | null
    };

    it('labels a healthy poll with the exact age of the last snapshot', () => {
        // 3000 ms old → "3s ago"; the bar is the wrapper div plus one chip and
        // nothing else, so the whole markup is assertable.
        expect(renderToString(<StatusBar {...base} status="live" />)).toBe(
            '<div class="status-bar"><span class="chip tone-ok" title="Last successful poll">live · 3s ago</span></div>'
        );
    });

    it('counts failures and announces the backoff retry', () => {
        // ceil(2500 / 1000) = 3s; the em dash and the raw `3s` are part of the
        // copy, so they are pinned rather than loosely matched.
        expect(renderToString(<StatusBar {...base} status="error" failureCount={2} nextAttemptMs={NOW + 2500} />)).toBe(
            '<div class="status-bar"><span class="chip tone-bad" title="The hub is not answering /v1/observe">unreachable (2×) — retrying in 3s</span></div>'
        );
    });

    it('drops the retry suffix when no next attempt is scheduled', () => {
        // nextAttemptMs === null means the loop is not scheduling another poll;
        // claiming a retry ETA anyway would be a lie on the chip.
        const html = renderToString(<StatusBar {...base} status="error" failureCount={4} nextAttemptMs={null} />);
        expect(html).toBe(
            '<div class="status-bar"><span class="chip tone-bad" title="The hub is not answering /v1/observe">unreachable (4×)</span></div>'
        );
        expect(html).not.toContain('retrying');
        expect(html).not.toContain('—');
    });

    it('says "connecting…" before the first snapshot', () => {
        // Neutral tone on purpose: the first poll on a wrong URL has not failed
        // yet, so it must not look like an error (StatusBar.tsx:36-38).
        expect(renderToString(<StatusBar {...base} status="pending" />)).toBe(
            '<div class="status-bar"><span class="chip tone-neutral" title="Waiting for the first response from the hub">connecting…</span></div>'
        );
    });

    it('carries no controls where the old connect bar had them', () => {
        // All four variants are checked: Pause / Resume / Refresh now used to
        // render on the live and paused chips, Reconnect / Disconnect and the
        // editable hub URL on every one of them.
        const htmlByStatus = [
            renderToString(<StatusBar {...base} status="live" />),
            renderToString(<StatusBar {...base} status="error" failureCount={2} nextAttemptMs={NOW + 2500} />),
            renderToString(<StatusBar {...base} status="error" failureCount={2} nextAttemptMs={null} />),
            renderToString(<StatusBar {...base} status="pending" />)
        ];
        for (const html of htmlByStatus) {
            expect(html).not.toContain('<button');
            expect(html).not.toContain('<input');
            expect(html).not.toContain('<form');
            expect(html).not.toContain('Reconnect');
            expect(html).not.toContain('Disconnect');
            expect(html).not.toContain('Pause');
            expect(html).not.toContain('Resume');
            expect(html).not.toContain('Refresh now');
            expect(html).not.toContain('paused');
            // The hub URL moved to the connect screen and the header brand line.
            expect(html).not.toContain('http://127.0.0.1:8085');
        }
    });
});

describe('OverviewPanel', () => {
    it('renders readiness, the queue depth and the cumulative counters', () => {
        const html = renderToString(<OverviewPanel snapshot={RICH_SNAPSHOT} metrics={METRICS} nowMs={NOW} />);
        expect(html).toContain('>no_workers<');
        expect(html).toContain('1 warm model(s)');
        // Queue depth is the only job-split card left: readiness.queue_depth is
        // the hub-side number (4), while the sub line counts the unleased jobs in
        // THIS snapshot (job-2 has lease: null → 1).
        expect(html).toContain('<span class="stat-value">4</span>');
        expect(html).toContain('<span class="stat-sub">1 queued job(s) shown</span>');
        // The ready/loading workers and active-jobs cards were removed, so their
        // labels must not survive anywhere in the strip.
        expect(html).not.toContain('Ready workers');
        expect(html).not.toContain('Loading workers');
        expect(html).not.toContain('Active jobs');
        // The mailbox card carries its own definition: these are messages queued
        // to pool actors, not jobs (OverviewPanel.tsx:70, StatCard `sub` renders
        // ui.tsx:67 `<span class="stat-sub">…</span>`).
        expect(html).toContain('<span class="stat-sub">messages queued to pool actors</span>');
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

describe('SuppliersPanel', () => {
    // Row fixture shared by the tone/budget/detail tests. One pool, seen 2s
    // ago, and slot/claim numbers chosen so every cell of the expected markup
    // below is a literal: 1 free of 4 → width:75%, claims 2, age "2s ago".
    const workerRow = (id: string, link: string, engine: string, extra: Record<string, unknown> = {}) => ({
        id,
        pools: ['llm.chat/qwen38'],
        link,
        engine,
        epoch: 1,
        capacity_slots: 4,
        free_slots: 1,
        free_kv_tokens: 4096,
        in_flight: ['job-1'],
        outstanding_claims: 2,
        last_seen_ms: NOW - 2000,
        telemetry: { temp_c: 61 },
        ...extra
    });

    // Expected worker-row markup, spelled out rather than derived from the
    // component: the tone/title pair SuppliersPanel.tsx:109-110 emits, then the
    // six surviving cells in order (worker, pools, slots, claims, last seen,
    // telemetry button). A change to the row has to be mirrored here by hand.
    const rowMarkup = (row: { tone: string; title: string; id: string; poolsTitle: string; poolsText: string; barTitle: string; width: string; slots: string; claims: string; button: string }) =>
        `<tr class="row-tone-${row.tone}" title="${row.title}">` +
        `<td class="mono" title="${row.id}">${row.id}</td>` +
        `<td class="cell-pools" title="${row.poolsTitle}">${row.poolsText}</td>` +
        `<td class="cell-slots"><span class="slot-bar" title="${row.barTitle}"><i style="width:${row.width}"></i></span><span class="mono">${row.slots}</span></td>` +
        `<td>${row.claims}</td>` +
        `<td>2s ago</td>` +
        // The label and aria-expanded move together: 'hide' is what an open row
        // renders (SuppliersPanel.tsx:136-138), 'telemetry' a closed one.
        `<td><button class="btn btn-mini" type="button" aria-expanded="${row.button === 'hide' ? 'true' : 'false'}">${row.button}</button></td></tr>`;

    const TABLE_HEAD = '<thead><tr><th>worker</th><th>pools</th><th title="free / capacity execution slots">slots</th><th>claims</th><th>last seen</th><th aria-label="telemetry"></th></tr></thead>';
    const tbodyOf = (html: string) => html.match(/<tbody>[\s\S]*?<\/tbody>/)?.[0];

    it('renders the six surviving columns with worker state folded into the row edge', () => {
        const html = renderToString(<SuppliersPanel snapshot={RICH_SNAPSHOT} nowMs={NOW} />);
        // The head is exactly these six headers — the equality is what proves
        // `link`, `engine`, `epoch`, `kv free` and `in flight` are gone (a
        // leftover <th> would change the string).
        expect(html.match(/<thead>[\s\S]*?<\/thead>/)?.[0]).toBe(TABLE_HEAD);
        expect(html).not.toContain('<th>link</th>');
        expect(html).not.toContain('<th>engine</th>');
        expect(html).not.toContain('<th>epoch</th>');
        expect(html).not.toContain('<th>kv free</th>');
        expect(html).not.toContain('<th>in flight</th>');
        // The two StateChips are gone from the rows as well (the component no
        // longer imports it, ui.tsx:73), the epoch cell (`<td>3</td>`) is gone,
        // the KV cell (`<td>4096</td>`) is gone, and the in-flight count is
        // gone so `job-1` cannot appear while the row is collapsed.
        expect(html).not.toContain('<span class="chip');
        expect(html).not.toContain('<td>3</td>');
        expect(html).not.toContain('<td>4096</td>');
        expect(html).not.toContain('job-1');
        // link + engine live on as the row edge and the row title. The `·` is
        // the literal U+00B7 from SuppliersPanel.tsx:110, not an entity.
        expect(html).toContain('<tr class="row-tone-ok" title="live · ready">');
        // React splits the interpolated slot ratio into separate text nodes.
        expect(html).toContain('1<!-- -->/<!-- -->4');
        // 3 of 4 slots used → the usage bar fills 75%.
        expect(html).toContain('width:75%');
        // Telemetry is an affordance, not a column: collapsed button, and no
        // detail row until it is opened.
        expect(html).toContain('<button class="btn btn-mini" type="button" aria-expanded="false">telemetry</button>');
        expect(html).not.toContain('row-detail');
        // The whole row, end to end (spelled literally once, as the shape the
        // `rowMarkup` helper above mirrors).
        expect(tbodyOf(html)).toBe(
            '<tbody><tr class="row-tone-ok" title="live · ready"><td class="mono" title="w-1">w-1</td>' +
                '<td class="cell-pools" title="llm.chat/qwen38">llm.chat/qwen38</td>' +
                '<td class="cell-slots"><span class="slot-bar" title="1 free of 4"><i style="width:75%"></i></span><span class="mono">1<!-- -->/<!-- -->4</span></td>' +
                '<td>2</td><td>2s ago</td><td><button class="btn btn-mini" type="button" aria-expanded="false">telemetry</button></td></tr></tbody>'
        );
    });

    it('maps link + engine onto one row tone per worker', () => {
        // Every documented rowTone outcome (SuppliersPanel.tsx:23-36): the
        // dispatch gate minus slot count. Both `bad` paths are pinned, and
        // `future_state` is a hub enum member this build does not know — asEnum
        // keeps its text verbatim (nexus-client.ts:150-154).
        const snapshot = snapshotOf({
            workers: [
                workerRow('w-ok', 'live', 'ready'),
                workerRow('w-warn', 'live', 'loading'),
                workerRow('w-bad-live', 'live', 'degraded'),
                workerRow('w-bad', 'orphaned', 'degraded'),
                workerRow('w-neutral', 'future_state', 'ready')
            ]
        });
        // Unknown hub text survives normalization, so it really is the renderer
        // that decides 'neutral' — not a normalization fallback.
        expect(snapshot.workers.map((worker) => [worker.link, worker.engine])).toEqual([
            ['live', 'ready'],
            ['live', 'loading'],
            ['live', 'degraded'],
            ['orphaned', 'degraded'],
            ['future_state', 'ready']
        ]);
        const tone = (toneName: string, title: string, id: string) => rowMarkup({ tone: toneName, title, id, poolsTitle: 'llm.chat/qwen38', poolsText: 'llm.chat/qwen38', barTitle: '1 free of 4', width: '75%', slots: '1<!-- -->/<!-- -->4', claims: '2', button: 'telemetry' });
        expect(renderToString(<SuppliersPanel snapshot={snapshot} nowMs={NOW} />).match(/<tr class="row-tone-[^"]*"[^>]*>[\s\S]*?<\/tr>/g)).toEqual([
            // live + ready → ok (green edge, taking work)
            tone('ok', 'live · ready', 'w-ok'),
            // live + loading → warn (socket up, engine warming)
            tone('warn', 'live · loading', 'w-warn'),
            // live + degraded → bad (socket up, engine regressed)
            tone('bad', 'live · degraded', 'w-bad-live'),
            // orphaned + degraded → bad (socket gone)
            tone('bad', 'orphaned · degraded', 'w-bad'),
            // unknown link → neutral, never a masquerading known-bad tone
            tone('neutral', 'future_state · ready', 'w-neutral')
        ]);
    });

    it('keeps the KV null/0 split in the client while rendering no KV column at all', () => {
        // The hub keeps null and 0 apart on purpose: null = the worker never
        // declared a KV budget, 0 = it declared one and has spent it all
        // (@services/nexus: crates/nexus-server/src/observe.rs,
        // `worker_row_reports_a_kv_budget_of_null_rather_than_zero`). The
        // `kv free` column is gone, so the split now lives only in the client
        // (asNullableNumber, nexus-client.ts:199) — a `|| 0` coercion there
        // would still silently report every unreported worker as exhausted.
        const budgetRow = (id: string, freeKvTokens: unknown) => workerRow(id, 'live', 'ready', { free_kv_tokens: freeKvTokens, capacity_slots: 1, free_slots: 1, outstanding_claims: 1, telemetry: null });
        const snapshot = snapshotOf({ workers: [budgetRow('w-null', null), budgetRow('w-zero', 0)] });
        // Normalization keeps the distinction (no coercion to 0).
        expect(snapshot.workers.map((worker) => worker.freeKvTokens)).toEqual([null, 0]);
        // Byte-identical rows apart from the id: null and 0 no longer reach the
        // DOM at all — neither a `—` KV cell nor a `0` one. (`<td>1</td>` here
        // is outstanding_claims, 1 in both rows.)
        const budget = (id: string) => rowMarkup({ tone: 'ok', title: 'live · ready', id, poolsTitle: 'llm.chat/qwen38', poolsText: 'llm.chat/qwen38', barTitle: '1 free of 1', width: '0%', slots: '1<!-- -->/<!-- -->1', claims: '1', button: 'telemetry' });
        expect(tbodyOf(renderToString(<SuppliersPanel snapshot={snapshot} nowMs={NOW} />))).toBe(`<tbody>${budget('w-null')}${budget('w-zero')}</tbody>`);
        // The em dash now belongs to one thing only: the pools fallback when a
        // worker reports no pools (SuppliersPanel.tsx:116). One row, one dash.
        const poolless = snapshotOf({ workers: [{ ...budgetRow('w-no-pools', 4096), pools: [] }] });
        expect(tbodyOf(renderToString(<SuppliersPanel snapshot={poolless} nowMs={NOW} />))).toBe(
            '<tbody><tr class="row-tone-ok" title="live · ready"><td class="mono" title="w-no-pools">w-no-pools</td>' +
                '<td class="cell-pools" title="">—</td>' +
                '<td class="cell-slots"><span class="slot-bar" title="1 free of 1"><i style="width:0%"></i></span><span class="mono">1<!-- -->/<!-- -->1</span></td>' +
                '<td>1</td><td>2s ago</td><td><button class="btn btn-mini" type="button" aria-expanded="false">telemetry</button></td></tr></tbody>'
        );
    });

    it('spans the telemetry detail row across all six columns when open', async () => {
        const snapshot = snapshotOf({ workers: [workerRow('w-1', 'live', 'ready', { in_flight: ['job-1', 'job-7'] })] });
        // `expanded` is internal useState([]), which react-dom/server always
        // seeds from the initial value, so the detail row is unreachable
        // through a plain render (no jsdom in this package). Re-import the
        // panel in a throwaway module instance whose `react` reports one open
        // row: vi.doMock only affects imports made after it, so the components
        // the other describes import statically keep the real hooks.
        vi.resetModules();
        vi.doMock('react', async (importOriginal) => {
            const actual = await importOriginal<typeof import('react')>();
            // SuppliersPanel's only useState call is `useState<string[]>([])`
            // (SuppliersPanel.tsx:61); pass any other initial value through so
            // a future hook is not silently clobbered.
            const useState = (init: unknown) => (Array.isArray(init) && init.length === 0 ? [['w-1'], () => undefined] : [init, () => undefined]);
            return { ...actual, useState } as unknown as typeof actual;
        });
        try {
            const { SuppliersPanel: PanelWithOpenRow } = await import('./SuppliersPanel');
            const html = renderToString(<PanelWithOpenRow snapshot={snapshot} nowMs={NOW} />);
            // Opening a row re-classes it (`row-tone-ok row-expanded`,
            // SuppliersPanel.tsx:109) and flips the button to hide/true.
            expect(tbodyOf(html)).toBe(
                '<tbody><tr class="row-tone-ok row-expanded" title="live · ready"><td class="mono" title="w-1">w-1</td>' +
                    '<td class="cell-pools" title="llm.chat/qwen38">llm.chat/qwen38</td>' +
                    '<td class="cell-slots"><span class="slot-bar" title="1 free of 4"><i style="width:75%"></i></span><span class="mono">1<!-- -->/<!-- -->4</span></td>' +
                    '<td>2</td><td>2s ago</td><td><button class="btn btn-mini" type="button" aria-expanded="true">hide</button></td></tr>' +
                    // colSpan={6} now matches the six surviving columns. React
                    // writes the React prop name verbatim in SSR — `colSpan="6"`,
                    // not the lowercased `colspan="6"` an HTML parser would give;
                    // attribute names are case-insensitive in HTML, so CSS/JS
                    // selectors on [colspan] still match. The in-flight job ids
                    // moved out of the row into this list.
                    '<tr class="row-detail"><td colSpan="6"><div class="detail-grid"><div><h4>in-flight jobs</h4><ul class="mono"><li>job-1</li><li>job-7</li></ul></div>' +
                    '<div><h4>telemetry</h4><pre class="telemetry">{\n  &quot;temp_c&quot;: 61\n}</pre></div></div></td></tr></tbody>'
            );
        } finally {
            // Leave the module registry as the other describes expect it.
            vi.doUnmock('react');
            vi.resetModules();
        }
    });

    it('tells the operator how to get a supplier', () => {
        expect(renderToString(<SuppliersPanel snapshot={EMPTY_SNAPSHOT} nowMs={NOW} />)).toContain('No suppliers connected.');
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
