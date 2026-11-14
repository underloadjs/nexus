// Unit tests for the Connections graph model (pure) plus a renderToString
// smoke check of the SVG panel — no jsdom: react-dom/server is enough to
// prove the renderer produces the expected DOM string for a snapshot.
import { describe, expect, it } from 'vitest';
import { renderToString } from 'react-dom/server';
import { normalizeObserveSnapshot } from '../api/nexus-client';
import { MAX_NODES_PER_COLUMN, buildGraphModel, type GraphEdge } from './build-graph';
import { ConnectionsPanel } from '../components/ConnectionsPanel';

const NOW = 1_762_760_000_000;

// Raw observe-shaped payloads → the same normalizer the app uses, so the
// graph tests exercise the real snapshot shape.
function snapshotOf(payload: unknown) {
    return normalizeObserveSnapshot(payload, NOW);
}

const LIVE_TOPOLOGY = snapshotOf({
    protocol: 'nexus-hub-observe/v1',
    now_ms: NOW,
    pools: [{ id: 'llm.chat/qwen38' }],
    workers: [
        { id: 'w-b', pools: ['llm.chat/qwen38'], link: 'orphaned', engine: 'degraded', epoch: 2, capacity_slots: 2, free_slots: 2 },
        {
            id: 'w-a',
            pools: ['llm.chat/qwen38'],
            link: 'live',
            engine: 'ready',
            epoch: 1,
            capacity_slots: 4,
            free_slots: 2,
            in_flight: ['job-1'],
            last_seen_ms: NOW - 1000
        }
    ],
    jobs: [
        {
            id: 'job-1',
            pool: 'llm.chat/qwen38',
            tenant: 'default',
            phase: 'streaming',
            created_ms: NOW - 5000,
            consumers: 1,
            lease: { worker: 'w-a', epoch: 1, attempt: 1, deadline_ms: NOW + 5000 }
        },
        // Queued job: no lease, so no supplier edge.
        { id: 'job-2', pool: 'llm.chat/qwen38', tenant: 'default', phase: 'queued', consumers: 2 }
    ],
    consumer_connections: [
        // attached to job-1 (and to a job the hub already dropped — the
        // dangling id must not create an edge).
        { id: 'c-1', tenant: 'default', since_ms: NOW - 20_000, attached_jobs: ['job-1', 'job-gone'] },
        { id: 'c-2', tenant: 'tenant-b', since_ms: NOW - 10_000, attached_jobs: ['job-2'] }
    ]
});

describe('buildGraphModel (live connections)', () => {
    it('produces the three columns in stable id order', () => {
        const model = buildGraphModel(LIVE_TOPOLOGY);
        // Sorted by id regardless of hub emission order (w-b came first).
        expect(model.workers.map((node) => node.id)).toEqual(['w-a', 'w-b']);
        expect(model.jobs.map((node) => node.id)).toEqual(['job-1', 'job-2']);
        expect(model.consumers.map((node) => node.id)).toEqual(['c-1', 'c-2']);
        expect(model.aggregatedConsumers).toBe(false);
    });

    it('encodes the states the renderer colours by', () => {
        const model = buildGraphModel(LIVE_TOPOLOGY);
        expect(model.workers.map((node) => node.state)).toEqual(['live', 'orphaned']);
        expect(model.jobs.map((node) => node.state)).toEqual(['leased', 'queued']);
        expect(model.consumers.map((node) => node.state)).toEqual(['default', 'tenant-b']);
    });

    // Edges are stored in DRAWING direction (build-graph.ts:6-8): the left
    // column is always `from`, so worker → job → consumer.
    it('draws job→consumer and worker→job edges in drawing order, dropping dangling ids', () => {
        const model = buildGraphModel(LIVE_TOPOLOGY);
        // Deterministic order: from, then to, then kind — 'job-1' < 'job-2' < 'w-a'.
        expect(model.edges).toEqual([
            { kind: 'attached', from: 'job-1', to: 'c-1' },
            { kind: 'attached', from: 'job-2', to: 'c-2' },
            { kind: 'lease', from: 'w-a', to: 'job-1' }
        ]);
        // GraphEdge is exactly { kind, from, to } now: the ×N fan-out labels
        // were removed from the UI, so no edge may smuggle a count back in.
        expect(model.edges.every((edge) => !('count' in edge))).toBe(true);
    });

    it('carries full ids in the tooltip titles and abbreviates labels', () => {
        const model = buildGraphModel(
            snapshotOf({
                workers: [{ id: 'w-9f3c11aa22bb33cc', link: 'live', engine: 'ready', capacity_slots: 4, free_slots: 4 }],
                jobs: [{ id: 'job-71ab00c0ffee99dd', pool: 'llm.chat', tenant: 'default', phase: 'assigned', lease: { worker: 'w-9f3c11aa22bb33cc' } }]
            })
        );
        const [worker] = model.workers;
        const [job] = model.jobs;
        expect(worker?.label).toBe('w-9f3c11…3cc');
        expect(worker?.title).toBe('w-9f3c11aa22bb33cc · live · ready · epoch 0');
        expect(job?.label).toBe('job-71ab…9dd');
    });
});

describe('buildGraphModel (tenant aggregation fallback)', () => {
    it('groups consumer edges by tenant when no connection objects exist', () => {
        const model = buildGraphModel(
            snapshotOf({
                workers: [{ id: 'w-a', link: 'live', engine: 'ready' }],
                jobs: [
                    { id: 'job-1', pool: 'p', tenant: 'default', phase: 'assigned', consumers: 2, lease: { worker: 'w-a' } },
                    { id: 'job-2', pool: 'p', tenant: 'default', phase: 'pending', consumers: 3 },
                    { id: 'job-3', pool: 'p', tenant: 'tenant-b', phase: 'queued', consumers: 1 }
                ]
            })
        );
        expect(model.aggregatedConsumers).toBe(true);
        // Column order is by tenant name, and the ids are namespaced so they
        // can never collide with a real connection id.
        expect(model.consumers.map((node) => node.id)).toEqual(['tenant:default', 'tenant:tenant-b']);
        expect(model.consumers.map((node) => node.title)).toEqual(['5 consumer(s) across 2 job(s)', '1 consumer(s) across 1 job(s)']);
        // One line per job, in drawing direction (job → tenant node); the
        // fan-out total lives in the tenant node's title, not on the edges.
        expect(model.edges).toEqual([
            { kind: 'tenant', from: 'job-1', to: 'tenant:default' },
            { kind: 'tenant', from: 'job-2', to: 'tenant:default' },
            { kind: 'tenant', from: 'job-3', to: 'tenant:tenant-b' },
            { kind: 'lease', from: 'w-a', to: 'job-1' }
        ]);
    });

    it('treats an entirely empty hub as no aggregation', () => {
        const model = buildGraphModel(snapshotOf({ protocol: 'nexus-hub-observe/v1' }));
        expect(model).toEqual({ consumers: [], jobs: [], workers: [], edges: [], aggregatedConsumers: false });
    });
});

describe('buildGraphModel (caps)', () => {
    it('caps every column and drops edges to uncapped endpoints', () => {
        // Zero-padded ids keep the sort order identical to the creation
        // order, so exactly the first MAX_NODES_PER_COLUMN of each column
        // survive the cap and the overflow rows lose their edges.
        const suffix = (index: number) => String(index).padStart(3, '0');
        const total = MAX_NODES_PER_COLUMN + 5;
        const many = {
            workers: Array.from({ length: total }, (_unused, index) => ({
                id: `w-${suffix(index)}`,
                link: 'live',
                engine: 'ready'
            })),
            jobs: Array.from({ length: total }, (_unused, index) => ({
                id: `job-${suffix(index)}`,
                pool: 'p',
                tenant: 'default',
                phase: 'assigned',
                consumers: 1,
                // Each job leases the worker with the same index.
                lease: { worker: `w-${suffix(index)}` }
            })),
            consumer_connections: Array.from({ length: total }, (_unused, index) => ({
                id: `c-${suffix(index)}`,
                tenant: 'default',
                attached_jobs: [`job-${suffix(index)}`]
            }))
        };
        const model = buildGraphModel(snapshotOf(many));
        expect(model.workers).toHaveLength(MAX_NODES_PER_COLUMN);
        expect(model.jobs).toHaveLength(MAX_NODES_PER_COLUMN);
        expect(model.consumers).toHaveLength(MAX_NODES_PER_COLUMN);

        const drawnWorkers = new Set(model.workers.map((node) => node.id));
        const drawnJobs = new Set(model.jobs.map((node) => node.id));
        const drawnConsumers = new Set(model.consumers.map((node) => node.id));
        // 30 attached + 30 lease; the five highest indexes lost both edges.
        // Exact expected list: attached edges sort first because every `from`
        // is `job-*` (lease `from` is `w-*`, and 'j' < 'w'), and each of the
        // two groups has unique `from` ids in ascending index order.
        const expectedEdges: GraphEdge[] = [
            ...Array.from({ length: MAX_NODES_PER_COLUMN }, (_unused, index) => ({ kind: 'attached', from: `job-${suffix(index)}`, to: `c-${suffix(index)}` }) as GraphEdge),
            ...Array.from({ length: MAX_NODES_PER_COLUMN }, (_unused, index) => ({ kind: 'lease', from: `w-${suffix(index)}`, to: `job-${suffix(index)}` }) as GraphEdge)
        ];
        expect(model.edges).toEqual(expectedEdges);
        expect(model.edges).toHaveLength(MAX_NODES_PER_COLUMN * 2);
        // Every surviving edge points left→right: attached ends at a DRAWN
        // consumer, lease starts at a drawn worker and ends at a drawn job.
        expect(model.edges.filter((edge) => edge.kind === 'attached').every((edge) => drawnJobs.has(edge.from) && drawnConsumers.has(edge.to))).toBe(true);
        expect(model.edges.filter((edge) => edge.kind === 'lease').every((edge) => drawnWorkers.has(edge.from) && drawnJobs.has(edge.to))).toBe(true);
        // Capped-out rows must not leak in either direction: w-034 can only be
        // a lease `from`, c-034 only an attached `to`, and neither is drawn.
        expect(model.edges.some((edge) => edge.from === 'w-034')).toBe(false);
        expect(model.edges.some((edge) => edge.to === 'c-034')).toBe(false);
    });
});

describe('ConnectionsPanel (renderToString smoke)', () => {
    it('renders one edge path per visible edge and the legend', () => {
        const html = renderToString(<ConnectionsPanel snapshot={LIVE_TOPOLOGY} />);
        expect(html).toContain('class="graph"');
        // The non-aggregated hint states the drawing direction the model now
        // uses (build-graph.ts: GraphEdge.from is always the left column).
        expect(html).toContain('<p class="panel-hint">Edges: supplier → leased job, job → attached consumer.</p>');
        // The model above has exactly three edges (the dangling job-gone is
        // dropped), so exactly three paths may be drawn.
        expect(html.match(/<path/g)?.length).toBe(3);
        expect(html).toContain('node-live');
        expect(html).toContain('node-orphaned');
        expect(html).toContain('node-leased');
        expect(html).toContain('node-queued');
        expect(html).toContain('consumers (2)');
        expect(html).toContain('jobs (2)');
        expect(html).toContain('suppliers (2)');
        // Caption order is the READING order of the columns, left to right:
        // suppliers (x 12 + 84), jobs (x 236 + 84), consumers (x 460 + 84).
        // Regex extraction preserves document order, so this pins the exact
        // three-element sequence — fragments, positions and order at once.
        expect(html.match(/<text class="graph-caption"[^>]*>[^<]*<\/text>/g)).toEqual([
            '<text class="graph-caption" x="96" y="14">suppliers (2)</text>',
            '<text class="graph-caption" x="320" y="14">jobs (2)</text>',
            '<text class="graph-caption" x="544" y="14">consumers (2)</text>'
        ]);
    });

    it('explains the aggregated fallback in the panel hint', () => {
        const html = renderToString(
            <ConnectionsPanel
                snapshot={snapshotOf({
                    jobs: [{ id: 'job-1', pool: 'p', tenant: 'default', phase: 'queued', consumers: 2 }]
                })}
            />
        );
        expect(html).toContain('<p class="panel-hint">Consumer side aggregated per tenant from jobs[].consumers (this hub reports no live connection objects).</p>');
        // The ×N fan-out labels are gone: the per-tenant total is only in the
        // tenant node's title, and no edge-count element is rendered at all.
        expect(html).not.toContain('graph-edge-count');
        expect(html).not.toContain('×');
    });

    it('shows the friendly empty state for a bare hub', () => {
        const html = renderToString(<ConnectionsPanel snapshot={snapshotOf({ protocol: 'nexus-hub-observe/v1' })} />);
        expect(html).toContain('Nothing to connect yet.');
        expect(html).not.toContain('<svg');
    });
});
