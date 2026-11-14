// Suppliers panel: every worker that has dialed into the hub.
//
// "Suppliers" is the domain word for workers: they supply capacity. The table
// is sorted by the hub's own order (no client-side re-sort) so rows stay
// stable between polls; ids are sorted upstream by the hub.
//
// The `link` and `engine` columns are deliberately gone: both are two axes of
// one worker (socket lifecycle vs engine liveness, see `model.rs:79-103` in
// @services/nexus) and reading two chips per row cost a third of the table
// width. They are folded into the row's status edge instead — the tone below
// is the dispatchability question, and the raw words stay in the row's title.
import { Fragment, useState } from 'react';
import type { NormalizedSnapshot } from '../api/nexus-client';
import { formatAge, shortId } from '../utils/format';
import { EmptyState, Panel, type Tone } from './ui';

// Status tone of one supplier row, i.e. "would the hub dispatch here?".
//
// The mapping is the dispatch gate from @services/nexus (`model.rs:209-222`:
// `dispatchable()` = link live && engine ready && free slots > 0; `advertised()`
// = link live only), minus the slot count, which the `slots` column already
// shows:
//   live + ready    -> ok    green edge, taking work
//   live + loading  -> warn  amber edge, socket up, engine still warming
//   live + degraded -> bad   red edge, socket up, engine regressed
//   orphaned + any  -> bad   red edge, socket gone (resumable within grace)
// A value this build does not know (a future hub enum member) falls back to
// 'neutral' rather than masquerading as a known-bad state — the client's
// `asEnum` deliberately keeps unknown text verbatim (`nexus-client.ts:150-154`).
function rowTone(link: string, engine: string): Tone {
    if (link === 'live' && engine === 'ready') return 'ok';
    if (link === 'live' && engine === 'loading') return 'warn';
    const knownLink = link === 'live' || link === 'orphaned';
    const knownEngine = engine === 'ready' || engine === 'loading' || engine === 'degraded';
    return knownLink && knownEngine ? 'bad' : 'neutral';
}

export interface SuppliersPanelProps {
    snapshot: NormalizedSnapshot;
    nowMs: number;
}

// Render an arbitrary telemetry value defensively: an object/array becomes
// pretty JSON, and a scalar (hub may emit a plain string or number) is shown
// as-is rather than "[object Object]".
function renderTelemetry(telemetry: unknown): string {
    if (telemetry === null || telemetry === undefined) return 'not reported by this worker';
    if (typeof telemetry === 'string') return telemetry;
    try {
        return JSON.stringify(telemetry, null, 2);
    } catch {
        // A circular structure would come from a custom serializer on the
        // hub side; never let it blank the panel.
        return String(telemetry);
    }
}

export function SuppliersPanel({ snapshot, nowMs }: SuppliersPanelProps) {
    // Expanded telemetry rows are keyed by worker id (multiple may be open at
    // once when comparing two workers).
    const [expanded, setExpanded] = useState<string[]>([]);

    const toggle = (id: string) => setExpanded((prev) => (prev.includes(id) ? prev.filter((row) => row !== id) : [...prev, id]));

    return (
        <Panel
            title="Suppliers (workers)"
            count={snapshot.workers.length}
            hint="Capacity on the supply side: free slots and outstanding claims. The row edge is the worker's state — green ready, amber loading, red orphaned or degraded."
        >
            {snapshot.workers.length === 0 ? (
                <EmptyState
                    message="No suppliers connected."
                    hint="Start a worker (cargo run -p nexus-worker-example, in @services/nexus) — it dials the hub and shows up here."
                />
            ) : (
                <div className="table-scroll">
                    <table className="table suppliers-table">
                        <thead>
                            <tr>
                                <th>worker</th>
                                <th>pools</th>
                                <th title="free / capacity execution slots">slots</th>
                                <th>claims</th>
                                <th>last seen</th>
                                <th aria-label="telemetry" />
                            </tr>
                        </thead>
                        <tbody>
                            {snapshot.workers.map((worker) => {
                                const open = expanded.includes(worker.id);
                                // One tone per row, derived from both axes —
                                // see `rowTone` above for the mapping.
                                const tone: Tone = rowTone(worker.link, worker.engine);
                                const usedSlots = Math.max(0, worker.capacitySlots - worker.freeSlots);
                                const slotRatio = worker.capacitySlots > 0 ? usedSlots / worker.capacitySlots : 0;
                                return (
                                    // Keyed Fragment: a worker row plus its
                                    // optional telemetry detail row are one
                                    // list item for React.
                                    <Fragment key={worker.id}>
                                        <tr
                                            key={worker.id}
                                            // The edge colour is the removed
                                            // link/engine columns; the title
                                            // keeps the raw words reachable
                                            // (same convention as the graph
                                            // node title, build-graph.ts:94-96).
                                            className={`row-tone-${tone}${open ? ' row-expanded' : ''}`}
                                            title={`${worker.link || 'unknown'} · ${worker.engine || 'unknown'}`}
                                        >
                                            <td className="mono" title={worker.id}>
                                                {shortId(worker.id, 10, 4)}
                                            </td>
                                            <td className="cell-pools" title={worker.pools.join(', ')}>
                                                {worker.pools.length > 0 ? worker.pools.join(', ') : '—'}
                                            </td>
                                            <td className="cell-slots">
                                                {/* Slot usage bar — the one genuinely
                                                    dynamic width in the app, so it is an
                                                    inline style on a styled span. */}
                                                <span className="slot-bar" title={`${worker.freeSlots} free of ${worker.capacitySlots}`}>
                                                    <i style={{ width: `${Math.min(1, slotRatio) * 100}%` }} />
                                                </span>
                                                <span className="mono">
                                                    {worker.freeSlots}/{worker.capacitySlots}
                                                </span>
                                            </td>
                                            <td>{worker.outstandingClaims}</td>
                                            <td>{formatAge(worker.lastSeenMs, nowMs)}</td>
                                            <td>
                                                <button
                                                    className="btn btn-mini"
                                                    type="button"
                                                    onClick={() => toggle(worker.id)}
                                                    aria-expanded={open}
                                                >
                                                    {open ? 'hide' : 'telemetry'}
                                                </button>
                                            </td>
                                        </tr>
                                        {open ? (
                                            <tr key={`${worker.id}:telemetry`} className="row-detail">
                                                <td colSpan={6}>
                                                    <div className="detail-grid">
                                                        <div>
                                                            <h4>in-flight jobs</h4>
                                                            {worker.inFlight.length === 0 ? (
                                                                <p className="muted">none</p>
                                                            ) : (
                                                                <ul className="mono">
                                                                    {worker.inFlight.map((jobId) => (
                                                                        <li key={jobId}>{jobId}</li>
                                                                    ))}
                                                                </ul>
                                                            )}
                                                        </div>
                                                        <div>
                                                            <h4>telemetry</h4>
                                                            <pre className="telemetry">{renderTelemetry(worker.telemetry)}</pre>
                                                        </div>
                                                    </div>
                                                </td>
                                            </tr>
                                        ) : null}
                                    </Fragment>
                                );
                            })}
                        </tbody>
                    </table>
                </div>
            )}
        </Panel>
    );
}
