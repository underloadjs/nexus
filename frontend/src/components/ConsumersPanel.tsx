// Consumers panel: the demand side.
//
// Two views because the hub gives two different facts:
//  1. `consumer_connections` — live streams (who is attached to which jobs);
//  2. `jobs[].consumers` — how many consumers watch each job, which exists
//     even when the hub reports no live connection objects at all.
// Showing only (1) would render a misleading "no consumers" on hubs that
// report (2), so the fallback table is always kept.
import type { NormalizedSnapshot } from '../api/nexus-client';
import { formatAge, shortId } from '../utils/format';
import { EmptyState, Panel, StateChip } from './ui';

export interface ConsumersPanelProps {
    snapshot: NormalizedSnapshot;
    nowMs: number;
}

export function ConsumersPanel({ snapshot, nowMs }: ConsumersPanelProps) {
    const connections = snapshot.consumerConnections;
    // Jobs that have consumers attached according to the job records, even
    // without a live connection object — the fallback view.
    const jobsWithConsumers = snapshot.jobs.filter((job) => job.consumers > 0);

    return (
        <Panel
            title="Consumers"
            count={connections.length > 0 ? `${connections.length} live` : `${jobsWithConsumers.length} job ref(s)`}
            hint="Live consumer streams, and per-job consumer counts as reported by the scheduler."
        >
            {connections.length === 0 ? (
                <EmptyState
                    message="No live consumer connections reported."
                    hint={
                        jobsWithConsumers.length > 0
                            ? 'The hub still reports consumers attached to jobs (below): those streams are not individually observable in this hub build.'
                            : 'Nothing is watching any job right now.'
                    }
                />
            ) : (
                <div className="table-scroll">
                    <table className="table">
                        <thead>
                            <tr>
                                <th>connection</th>
                                <th>tenant</th>
                                <th>attached jobs</th>
                                <th>connected</th>
                            </tr>
                        </thead>
                        <tbody>
                            {connections.map((connection) => (
                                <tr key={connection.id}>
                                    <td className="mono" title={connection.id}>
                                        {shortId(connection.id, 10, 4)}
                                    </td>
                                    <td title={connection.tenant}>{connection.tenant || '—'}</td>
                                    <td>
                                        {/* Chips rather than a raw count: the
                                            attached-job ids are exactly the
                                            edges the graph draws. */}
                                        {connection.attachedJobs.length === 0 ? (
                                            <span className="muted">idle stream</span>
                                        ) : (
                                            connection.attachedJobs.map((jobId) => (
                                                <StateChip key={jobId} label={shortId(jobId, 8, 3)} tone="info" title={jobId} />
                                            ))
                                        )}
                                    </td>
                                    <td>{formatAge(connection.sinceMs, nowMs)}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            )}

            {jobsWithConsumers.length > 0 ? (
                <div className="table-scroll">
                    <table className="table table-compact">
                        <thead>
                            <tr>
                                <th title="per-job consumer count from jobs[].consumers">job</th>
                                <th>tenant</th>
                                <th>consumers</th>
                            </tr>
                        </thead>
                        <tbody>
                            {jobsWithConsumers.map((job) => (
                                <tr key={job.id}>
                                    <td className="mono" title={job.id}>
                                        {shortId(job.id, 10, 4)}
                                    </td>
                                    <td>{job.tenant || '—'}</td>
                                    <td>{job.consumers}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            ) : null}
        </Panel>
    );
}
