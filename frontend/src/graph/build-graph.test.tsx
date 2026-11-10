// Unit tests for the Connections graph model (pure) plus a renderToString
// smoke check of the SVG panel — no jsdom: react-dom/server is enough to
// prove the renderer produces the expected DOM string for a snapshot.
import { describe, expect, it } from 'vitest';
import { renderToString } from 'react-dom/server';
import { normalizeObserveSnapshot } from '../api/nexus-client';
import { MAX_NODES_PER_COLUMN, buildGraphModel } from './build-graph';
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

    it('draws consumer→job and job→worker edges, dropping dangling ids', () => {
        const model = buildGraphModel(LIVE_TOPOLOGY);
        // Deterministic order: from, then to, then kind.
        expect(model.edges).toEqual([
            { kind: 'attached', from: 'c-1', to: 'job-1' },
            { kind: 'attached', from: 'c-2', to: 'job-2' },
            { kind: 'lease', from: 'job-1', to: 'w-a' }
        ]);
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
        // The fan-out count rides on every edge of that tenant.
        expect(model.edges).toEqual([
            { kind: 'lease', from: 'job-1', to: 'w-a' },
            { kind: 'tenant', from: 'tenant:default', to: 'job-1', count: 5 },
            { kind: 'tenant', from: 'tenant:default', to: 'job-2', count: 5 },
            { kind: 'tenant', from: 'tenant:tenant-b', to: 'job-3', count: 1 }
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
        // 30 attached + 30 lease; the five highest indexes lost both edges.
        expect(model.edges).toHaveLength(MAX_NODES_PER_COLUMN * 2);
        expect(model.edges.every((edge) => drawnWorkers.has(edge.to) || drawnJobs.has(edge.to))).toBe(true);
        expect(model.edges.some((edge) => edge.from === 'c-034' || edge.to === 'w-034')).toBe(false);
    });
});

describe('ConnectionsPanel (renderToString smoke)', () => {
    it('renders one edge path per visible edge and the legend', () => {
        const html = renderToString(<ConnectionsPanel snapshot={LIVE_TOPOLOGY} />);
        expect(html).toContain('class="graph"');
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
    });

    it('explains the aggregated fallback in the panel hint', () => {
        const html = renderToString(
            <ConnectionsPanel
                snapshot={snapshotOf({
                    jobs: [{ id: 'job-1', pool: 'p', tenant: 'default', phase: 'queued', consumers: 2 }]
                })}
            />
        );
        expect(html).toContain('Consumer side aggregated per tenant');
        expect(html).toContain('×2');
    });

    it('shows the friendly empty state for a bare hub', () => {
        const html = renderToString(<ConnectionsPanel snapshot={snapshotOf({ protocol: 'nexus-hub-observe/v1' })} />);
        expect(html).toContain('Nothing to connect yet.');
        expect(html).not.toContain('<svg');
    });
});
