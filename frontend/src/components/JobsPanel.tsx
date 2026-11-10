// Jobs panel: the active job list with its lease (the concrete
// "who is connected to whom" record on the demand side).
//
// The hub reports ONLY active jobs here, so "no rows" genuinely means an
// idle hub — the empty state says that instead of implying a problem.
import type { NormalizedSnapshot } from '../api/nexus-client';
import { formatAge, formatRemaining, shortId } from '../utils/format';
import { EmptyState, Panel, StateChip, type Tone } from './ui';

// Phase → tone. Anything the hub adds later renders neutral (a new phase is
// not an error), which is why this map is a lookup with a fallback.
const PHASE_TONES: Record<string, Tone> = {
    queued: 'warn',
    pending: 'warn',
    dispatched: 'info',
    running: 'ok',
    streaming: 'ok',
    cancelling: 'warn',
    failed: 'bad',
    error: 'bad',
    completed: 'neutral',
    succeeded: 'neutral',
    cancelled: 'neutral'
};

export interface JobsPanelProps {
    snapshot: NormalizedSnapshot;
    nowMs: number;
}

export function JobsPanel({ snapshot, nowMs }: JobsPanelProps) {
    return (
        <Panel
            title="Jobs (active)"
            count={snapshot.jobs.length}
            hint="Active jobs only, with the worker currently holding each lease and its deadline."
        >
            {snapshot.jobs.length === 0 ? (
                <EmptyState message="No active jobs." hint="The queue is empty: every accepted job has reached a terminal state." />
            ) : (
                <div className="table-scroll">
                    <table className="table">
                        <thead>
                            <tr>
                                <th>job</th>
                                <th>pool</th>
                                <th>tenant</th>
                                <th>phase</th>
                                <th title="consumers watching this job">consumers</th>
                                <th>leased to</th>
                                <th title="lease epoch / attempt">epoch · try</th>
                                <th title="remaining time on the lease">deadline</th>
                                <th>created</th>
                            </tr>
                        </thead>
                        <tbody>
                            {snapshot.jobs.map((job) => {
                                const tone: Tone = PHASE_TONES[job.phase] ?? 'neutral';
                                return (
                                    <tr key={job.id}>
                                        <td className="mono" title={job.id}>
                                            {shortId(job.id, 10, 4)}
                                        </td>
                                        <td className="mono" title={job.pool}>
                                            {job.pool || '—'}
                                        </td>
                                        <td title={job.tenant}>{job.tenant || '—'}</td>
                                        <td>
                                            <StateChip label={job.phase || 'unknown'} tone={tone} />
                                        </td>
                                        <td>{job.consumers}</td>
                                        <td className="mono" title={job.lease?.worker ?? ''}>
                                            {/* A job without a lease is queued
                                                for capacity — spelled out, not
                                                left as an ambiguous dash. */}
                                            {job.lease ? shortId(job.lease.worker || '(unknown worker)', 10, 4) : <span className="muted">queued</span>}
                                        </td>
                                        <td>{job.lease ? `${job.lease.epoch} · ${job.lease.attempt}` : '—'}</td>
                                        <td title="negative means the lease has overrun its deadline">
                                            {job.lease ? formatRemaining(job.lease.deadlineMs, nowMs) : '—'}
                                        </td>
                                        <td>{formatAge(job.createdMs, nowMs)}</td>
                                    </tr>
                                );
                            })}
                        </tbody>
                    </table>
                </div>
            )}
        </Panel>
    );
}
