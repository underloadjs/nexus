// Graph model for the Connections panel: a three-column supplier → job →
// consumer topology derived from ONE observe snapshot.
//
// Column order is the operator's reading order, left to right: who serves
// (supplier workers), what is being served (jobs), who is waiting on it
// (consumers). Edges are therefore always stored in DRAWING direction — from
// the left column to the right column — so the SVG renderer can assume
// `from.x < to.x` for every edge.
//
// Pure + dependency-free (node-environment unit-testable) — the SVG
// component only draws what this returns.
import type { NormalizedJob, NormalizedSnapshot, NormalizedWorker } from '../api/nexus-client';
import { shortId } from '../utils/format';

export type GraphColumn = 'worker' | 'job' | 'consumer';

export interface GraphNode {
    id: string;
    column: GraphColumn;
    // Primary label (abbreviated id or tenant name).
    label: string;
    // Secondary line (pool / phase / tenant / consumer count).
    sub: string;
    // Drives the colour: phase for jobs, link state for workers, tenant for
    // consumers. Free-form so an unknown hub value still renders (neutral).
    state: string;
    // Full id for the hover title (labels are abbreviated).
    title: string;
}

export interface GraphEdge {
    // `attached`: job → consumer watching it. `lease`: worker → job it holds.
    // `tenant`: job → aggregated per-tenant consumer node (fallback when the
    // hub reports no live connections).
    kind: 'attached' | 'lease' | 'tenant';
    // Both endpoints, in drawing direction: `from` is always the column further
    // left (worker → job → consumer). No fan-out count is carried any more —
    // the fan-out is drawn as one line per connection, and per-node totals are
    // already in each node's sub/title text.
    from: string;
    to: string;
}

export interface GraphModel {
    consumers: GraphNode[];
    jobs: GraphNode[];
    workers: GraphNode[];
    edges: GraphEdge[];
    // True when live consumer connections were absent and the consumer side
    // was aggregated per tenant from jobs[].consumers — the UI must say so,
    // otherwise a hub without connection reporting looks like it has no
    // consumers at all.
    aggregatedConsumers: boolean;
}

// Cap per column: SVG labels are absolutely positioned text, so a hub with
// hundreds of jobs would produce an unreadable (and huge) DOM. Truncation is
// reported by the panel via the counts it renders alongside the graph.
export const MAX_NODES_PER_COLUMN = 30;

// Deterministic display order: ids first, so two snapshots of the same state
// produce the same layout (no flicker between polls).
function byId(a: { id: string }, b: { id: string }): number {
    return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
}

function consumerNode(id: string, tenant: string, label: string, title: string): GraphNode {
    return { id, column: 'consumer', label, sub: tenant || 'no tenant', state: tenant, title };
}

function jobNode(job: NormalizedJob): GraphNode {
    return {
        id: job.id,
        column: 'job',
        label: shortId(job.id, 8, 3),
        // The pool + phase are the interesting dimensions; the leased/queued
        // distinction rides on the node colour (state) below.
        sub: `${job.pool || 'no pool'} · ${job.phase || 'unknown'}`,
        // Design requirement: leased jobs must look different from queued
        // ones — a queued job has no outgoing edge, which is the point.
        state: job.lease ? 'leased' : 'queued',
        title: `${job.id} · ${job.phase} · pool ${job.pool || '—'} · ${job.consumers} consumer(s)${
            job.lease ? ` · leased by ${job.lease.worker || '(unknown worker)'}` : ' · no lease yet'
        }`
    };
}

function workerNode(worker: NormalizedWorker): GraphNode {
    return {
        id: worker.id,
        column: 'worker',
        label: shortId(worker.id, 8, 3),
        // Slot usage is the capacity signal an operator scans for.
        sub: `${worker.freeSlots}/${worker.capacitySlots} slots · ${worker.engine}`,
        state: worker.link,
        title: `${worker.id} · ${worker.link} · ${worker.engine} · epoch ${worker.epoch}`
    };
}

// Build the whole model from a snapshot. Every cross-reference is validated
// against the ids actually present in THIS snapshot: the hub may have dropped
// a job between the observe read and the connection list, and a dangling edge
// would crash the renderer.
export function buildGraphModel(snapshot: NormalizedSnapshot): GraphModel {
    // Cap each column before drawing, keeping the stable id order.
    const take = <T extends { id: string }>(rows: T[]): T[] => [...rows].sort(byId).slice(0, MAX_NODES_PER_COLUMN);

    // Raw rows → drawable nodes (labels abbreviated, state encoded).
    const jobNodes = take(snapshot.jobs).map(jobNode);
    const workerNodes = take(snapshot.workers).map(workerNode);

    // Membership sets come from the FULL lists: an edge may legitimately
    // reference a real id that was cut from the drawing by the cap, and the
    // final filter below drops it only because it is not drawn.
    const jobIds = new Set(snapshot.jobs.map((job) => job.id));
    const workerIds = new Set(snapshot.workers.map((worker) => worker.id));

    const edges: GraphEdge[] = [];
    let consumers: GraphNode[];
    let aggregatedConsumers = false;

    const connections = snapshot.consumerConnections;
    if (connections.length > 0) {
        // Live consumer connections: one node each, edges from the attached
        // job (left/middle column) to the consumer (right column).
        consumers = take(connections).map((connection) =>
            consumerNode(connection.id, connection.tenant, shortId(connection.id, 8, 3), `${connection.id} · ${connection.tenant}`)
        );
        for (const connection of connections) {
            for (const jobId of connection.attachedJobs) {
                if (jobIds.has(jobId)) edges.push({ kind: 'attached', from: jobId, to: connection.id });
            }
        }
    } else {
        // Fallback aggregation: the hub reported no connection objects (older
        // build, or consumers that never opened the stream), but each job
        // carries `tenant` + `consumers`. Group by tenant so the shape of the
        // traffic is still visible; the tenant node's title keeps the fan-out
        // count, one line per job is drawn rather than a ×N label.
        aggregatedConsumers = snapshot.jobs.length > 0;
        const byTenant = new Map<string, { count: number; jobs: string[] }>();
        for (const job of snapshot.jobs) {
            const tenant = job.tenant || 'no tenant';
            const entry = byTenant.get(tenant) ?? { count: 0, jobs: [] };
            // jobs[].consumers is the authoritative per-job consumer count.
            entry.count += Math.max(0, Math.trunc(job.consumers));
            entry.jobs.push(job.id);
            byTenant.set(tenant, entry);
        }
        consumers = [...byTenant.entries()]
            .sort((a, b) => (a[0] < b[0] ? -1 : 1))
            .slice(0, MAX_NODES_PER_COLUMN)
            .map(([tenant, entry]) =>
                consumerNode(`tenant:${tenant}`, tenant, tenant, `${entry.count} consumer(s) across ${entry.jobs.length} job(s)`)
            );
        for (const [tenant, entry] of byTenant) {
            for (const jobId of entry.jobs) {
                if (jobIds.has(jobId)) edges.push({ kind: 'tenant', from: jobId, to: `tenant:${tenant}` });
            }
        }
    }

    // Lease edges: the worker holding the lease → the job it runs, i.e. left
    // column to middle column. Queued jobs simply have no incoming edge, which
    // is exactly the "waiting for capacity" signal in the picture.
    for (const job of snapshot.jobs) {
        const workerId = job.lease?.worker;
        if (workerId && workerIds.has(workerId)) edges.push({ kind: 'lease', from: workerId, to: job.id });
    }

    // Final safety net: a column may have been truncated by
    // MAX_NODES_PER_COLUMN, and the hub's ids come from an async snapshot —
    // drop any edge whose endpoints are not actually drawn.
    const drawn = new Set<string>([...consumers, ...jobNodes, ...workerNodes].map((node) => node.id));
    const visibleEdges = edges
        .filter((edge) => drawn.has(edge.from) && drawn.has(edge.to))
        // Deterministic order regardless of hub emission order: the SVG is
        // rebuilt every poll, and a stable order means no edge flicker.
        .sort((a, b) => (a.from === b.from ? (a.to === b.to ? a.kind.localeCompare(b.kind) : a.to < b.to ? -1 : 1) : a.from < b.from ? -1 : 1));

    return { consumers, jobs: jobNodes, workers: workerNodes, edges: visibleEdges, aggregatedConsumers };
}
