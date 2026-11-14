// Connections panel: the three-column topology graph
// (supplier workers → jobs → consumers), drawn as plain SVG.
//
// No graphing library: the picture is exactly three columns of boxes with
// cubic edges, and a static bundle benefits from shipping zero chart bytes.
// Layout math lives here (positions), while the graph CONTENT lives in
// ../../graph/build-graph.ts (pure, unit-tested).
import { useMemo } from 'react';
import type { NormalizedSnapshot } from '../api/nexus-client';
import { buildGraphModel, MAX_NODES_PER_COLUMN, type GraphColumn, type GraphEdge, type GraphNode } from '../graph/build-graph';
import { EmptyState, Panel, StateChip } from './ui';

// Fixed geometry, read left to right in the order an operator scans the hub:
// supply (workers) → work (jobs) → demand (consumers). The 56 px gutters
// between columns are where the bezier edges live. `buildGraphModel` emits
// every edge in this same direction, so `from.x` is always left of `to.x`.
const COLUMN_X: Record<GraphColumn, number> = { worker: 12, job: 236, consumer: 460 };
const NODE_WIDTH = 168;
const NODE_HEIGHT = 34;
const ROW_GAP = 46;
// Vertical room for the column captions above the first node row.
const TOP_PADDING = 30;
// Total drawing width: last column + node width + the same 12 px margin as
// the left edge.
const GRAPH_WIDTH = COLUMN_X.consumer + NODE_WIDTH + 12;

export interface ConnectionsPanelProps {
    snapshot: NormalizedSnapshot;
}

// CSS class for a node's fill: workers are coloured by link state (live vs
// orphaned is the single most important distinction on the supply side),
// jobs by lease state, consumers uniformly.
function nodeClass(node: GraphNode): string {
    if (node.column === 'worker') return node.state === 'orphaned' ? 'node node-orphaned' : 'node node-live';
    if (node.column === 'job') return node.state === 'queued' ? 'node node-queued' : 'node node-leased';
    return 'node node-consumer';
}

// Edge stroke class: attached (a live consumer watches the job), tenant
// (aggregated fallback) or lease (the job runs on that worker).
function edgeClass(edge: GraphEdge): string {
    if (edge.kind === 'lease') return 'edge edge-lease';
    if (edge.kind === 'tenant') return 'edge edge-tenant';
    return 'edge edge-attached';
}

export function ConnectionsPanel({ snapshot }: ConnectionsPanelProps) {
    const model = useMemo(() => buildGraphModel(snapshot), [snapshot]);

    // y position per node id, computed per column so rows never overlap even
    // when the columns have different sizes. Column order matches COLUMN_X:
    // suppliers, jobs, consumers.
    const positions = useMemo(() => {
        const map = new Map<string, { x: number; y: number }>();
        const columns: GraphNode[][] = [model.workers, model.jobs, model.consumers];
        for (const column of columns) {
            column.forEach((node, index) => {
                map.set(node.id, { x: COLUMN_X[node.column], y: TOP_PADDING + index * ROW_GAP });
            });
        }
        return map;
    }, [model]);

    const height = TOP_PADDING + Math.max(1, model.consumers.length, model.jobs.length, model.workers.length) * ROW_GAP;
    const isEmpty = model.consumers.length === 0 && model.jobs.length === 0 && model.workers.length === 0;

    // Column captions: static text centered over each column, so the columns
    // remain identifiable even when one of them is empty (an idle hub has
    // jobs but maybe no consumers, and a bare gap would be ambiguous).
    const captions: Array<{ x: number; text: string; count: number; total: number }> = [
        { x: COLUMN_X.worker + NODE_WIDTH / 2, text: 'suppliers', count: model.workers.length, total: snapshot.workers.length },
        { x: COLUMN_X.job + NODE_WIDTH / 2, text: 'jobs', count: model.jobs.length, total: snapshot.jobs.length },
        { x: COLUMN_X.consumer + NODE_WIDTH / 2, text: 'consumers', count: model.consumers.length, total: snapshot.consumerConnections.length }
    ];

    return (
        <Panel
            title="Connections"
            count={`${model.edges.length} edge(s)`}
            hint={
                model.aggregatedConsumers
                    ? 'Consumer side aggregated per tenant from jobs[].consumers (this hub reports no live connection objects).'
                    : 'Edges: supplier → leased job, job → attached consumer.'
            }
            actions={
                <span className="legend">
                    <StateChip label="live" tone="ok" />
                    <StateChip label="orphaned" tone="bad" />
                    <StateChip label="leased" tone="info" />
                    <StateChip label="queued" tone="warn" />
                </span>
            }
        >
            {isEmpty ? (
                <EmptyState
                    message="Nothing to connect yet."
                    hint="As soon as a worker dials in and a consumer submits a job, the graph fills up."
                />
            ) : (
                <div className="graph-scroll">
                    {/* viewBox is fixed-width; the SVG scales down on narrow
                        screens instead of clipping, which is why there is no
                        responsive JS here. */}
                    <svg className="graph" viewBox={`0 0 ${GRAPH_WIDTH} ${height}`} width={GRAPH_WIDTH} height={height} role="img">
                        <title>nexus topology: supplier workers, jobs and consumers</title>
                        {captions.map((caption) => (
                            <text key={caption.text} className="graph-caption" x={caption.x} y={14}>
                                {`${caption.text} (${caption.count}${caption.count < caption.total ? ` of ${caption.total}` : ''})`}
                            </text>
                        ))}

                        {/* Edges first so nodes paint on top of them. */}
                        {model.edges.map((edge, index) => {
                            const from = positions.get(edge.from);
                            const to = positions.get(edge.to);
                            if (!from || !to) return null; // defensive: endpoints always exist
                            const startX = from.x + NODE_WIDTH;
                            const endX = to.x;
                            const midY1 = from.y + NODE_HEIGHT / 2;
                            const midY2 = to.y + NODE_HEIGHT / 2;
                            // Horizontal-tangent cubic curve: reads as a flow,
                            // and never crosses a node box.
                            const path = `M ${startX} ${midY1} C ${startX + 40} ${midY1}, ${endX - 40} ${midY2}, ${endX} ${midY2}`;
                            return (
                                // Index-based key: edges are recomputed from a
                                // snapshot every poll, and a from→to pair can
                                // legitimately repeat (fan-out).
                                <path key={`${edge.from}->${edge.to}#${index}`} className={edgeClass(edge)} d={path} />
                            );
                        })}

                        {[...model.workers, ...model.jobs, ...model.consumers].map((node) => {
                            const position = positions.get(node.id);
                            if (!position) return null;
                            return (
                                <g key={node.id} className={nodeClass(node)} transform={`translate(${position.x} ${position.y})`}>
                                    <rect className="node-box" width={NODE_WIDTH} height={NODE_HEIGHT} rx={6} />
                                    {/* SVG text needs explicit clipping-ish
                                        truncation: labels are already
                                        abbreviated upstream (shortId), and the
                                        full id is in <title> for the tooltip. */}
                                    <title>{node.title}</title>
                                    <text className="node-label" x={10} y={14}>
                                        {node.label}
                                    </text>
                                    <text className="node-sub" x={10} y={27}>
                                        {node.sub}
                                    </text>
                                </g>
                            );
                        })}
                    </svg>
                    {Math.max(model.consumers.length, model.jobs.length, model.workers.length) >= MAX_NODES_PER_COLUMN ? (
                        <p className="graph-note">
                            Columns are capped at {MAX_NODES_PER_COLUMN} nodes for readability — the supplier/consumer tables beside this graph list every row.
                        </p>
                    ) : null}
                </div>
            )}
        </Panel>
    );
}
