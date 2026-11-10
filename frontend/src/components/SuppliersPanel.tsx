// Suppliers panel: every worker that has dialed into the hub.
//
// "Suppliers" is the domain word for workers: they supply capacity. The table
// is sorted by the hub's own order (no client-side re-sort) so rows stay
// stable between polls; ids are sorted upstream by the hub.
//
// The `kv free` column keeps the hub's null/0 split: `—` means the worker
// never declared a KV budget, `0` means it reported one and has spent it all
// (see `crates/nexus-server/src/observe.rs`,
// `worker_row_reports_a_kv_budget_of_null_rather_than_zero`).
import { Fragment, useState } from 'react';
import type { EngineState, NormalizedSnapshot, WorkerLink } from '../api/nexus-client';
import { formatAge, formatNumber, shortId } from '../utils/format';
import { EmptyState, Panel, StateChip, type Tone } from './ui';

const LINK_TONES: Record<WorkerLink, Tone> = { live: 'ok', orphaned: 'bad' };
const ENGINE_TONES: Record<EngineState, Tone> = { ready: 'ok', loading: 'warn', degraded: 'bad' };

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
            hint="Capacity on the supply side: link state, engine state, free slots and outstanding claims."
        >
            {snapshot.workers.length === 0 ? (
                <EmptyState
                    message="No suppliers connected."
                    hint="Start a worker (cargo run -p nexus-worker-example) — it dials the hub and shows up here."
                />
            ) : (
                <div className="table-scroll">
                    <table className="table">
                        <thead>
                            <tr>
                                <th>worker</th>
                                <th>pools</th>
                                <th>link</th>
                                <th>engine</th>
                                <th>epoch</th>
                                <th title="free / capacity execution slots">slots</th>
                                <th title="free KV cache tokens; — means this worker reports no KV budget">kv free</th>
                                <th>in flight</th>
                                <th>claims</th>
                                <th>last seen</th>
                                <th aria-label="telemetry" />
                            </tr>
                        </thead>
                        <tbody>
                            {snapshot.workers.map((worker) => {
                                const open = expanded.includes(worker.id);
                                // Unknown enum values fall back to neutral
                                // rather than pretending to be a known state.
                                const linkTone: Tone = LINK_TONES[worker.link] ?? 'neutral';
                                const engineTone: Tone = ENGINE_TONES[worker.engine] ?? 'neutral';
                                const usedSlots = Math.max(0, worker.capacitySlots - worker.freeSlots);
                                const slotRatio = worker.capacitySlots > 0 ? usedSlots / worker.capacitySlots : 0;
                                return (
                                    // Keyed Fragment: a worker row plus its
                                    // optional telemetry detail row are one
                                    // list item for React.
                                    <Fragment key={worker.id}>
                                        <tr key={worker.id} className={open ? 'row-expanded' : undefined}>
                                            <td className="mono" title={worker.id}>
                                                {shortId(worker.id, 10, 4)}
                                            </td>
                                            <td className="cell-pools" title={worker.pools.join(', ')}>
                                                {worker.pools.length > 0 ? worker.pools.join(', ') : '—'}
                                            </td>
                                            <td>
                                                <StateChip label={worker.link || 'unknown'} tone={linkTone} />
                                            </td>
                                            <td>
                                                <StateChip label={worker.engine || 'unknown'} tone={engineTone} />
                                            </td>
                                            <td>{worker.epoch}</td>
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
                                            <td>{formatNumber(worker.freeKvTokens)}</td>
                                            <td title={worker.inFlight.join('\n')}>{worker.inFlight.length}</td>
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
                                                <td colSpan={11}>
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
