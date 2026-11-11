// Pools panel: one card per pool (the queue + worker group a job targets).
//
// A pool is the unit an operator reasons about ("why is llm.chat slow?"), so
// it gets cards rather than table rows: each card shows the pressure signals
// together — queue depth next to ready workers next to leases.
import type { NormalizedPool, NormalizedSnapshot } from '../api/nexus-client';
import { EmptyState, Panel, StateChip } from './ui';

export interface PoolsPanelProps {
    snapshot: NormalizedSnapshot;
}

// A pool the hub explicitly flagged as unresponsive (engine wedged, workers
// lost) — listed in `unresponsive_pools`, so its card is badged as such.
function isUnresponsive(pool: NormalizedPool, unresponsive: string[]): boolean {
    return unresponsive.includes(pool.id);
}

export function PoolsPanel({ snapshot }: PoolsPanelProps) {
    // Ids the hub flagged as unresponsive that have NO `pools[]` entry: a pool
    // is typically flagged the moment its last worker disappears, and the hub
    // may drop the stats entry in the same breath. Listing them here is the
    // only place that information becomes readable — Overview shows a count.
    const cardlessUnresponsive = snapshot.unresponsivePools.filter((id) => !snapshot.pools.some((pool) => pool.id === id));

    return (
        <Panel title="Pools" count={snapshot.pools.length} hint="Queue depth vs. capacity per pool, straight from the hub scheduler.">
            {snapshot.pools.length === 0 ? (
                <EmptyState message="No pools on this hub." hint="A pool materialises as soon as a supplier worker dials in for it." />
            ) : (
                <div className="pool-grid">
                    {snapshot.pools.map((pool) => {
                        const flagged = isUnresponsive(pool, snapshot.unresponsivePools);
                        return (
                            <div key={pool.id || '(no id)'} className={`pool-card ${flagged ? 'pool-flagged' : ''}`}>
                                <div className="pool-head">
                                    <span className="mono pool-id" title={pool.id}>
                                        {pool.id || '(no id)'}
                                    </span>
                                    {flagged ? <StateChip label="unresponsive" tone="bad" title="The hub reports this pool as unresponsive" /> : null}
                                </div>
                                <dl className="pool-stats">
                                    {/* Paired deliberately: depth without capacity is
                                        alarming, depth WITH idle workers is not. */}
                                    <div>
                                        <dt>queue</dt>
                                        <dd className={pool.queueDepth > 0 ? 'tone-warn-text' : ''}>{pool.queueDepth}</dd>
                                    </div>
                                    <div>
                                        <dt>ready</dt>
                                        <dd>{pool.readyWorkers}</dd>
                                    </div>
                                    <div>
                                        <dt>loading</dt>
                                        <dd>{pool.loadingWorkers}</dd>
                                    </div>
                                    <div>
                                        <dt>connected</dt>
                                        <dd>{pool.connectedWorkers}</dd>
                                    </div>
                                    <div>
                                        <dt>leases</dt>
                                        <dd>{pool.leases}</dd>
                                    </div>
                                    <div>
                                        <dt>mailbox</dt>
                                        <dd>{pool.mailboxPending}</dd>
                                    </div>
                                </dl>
                            </div>
                        );
                    })}
                </div>
            )}
            {cardlessUnresponsive.length > 0 ? (
                // Reuses the Overview note styling (`.model-list`) — it is the
                // same kind of one-line, secondary listing.
                <p className="model-list">
                    <span className="muted">Unresponsive pools with no reported stats:</span>{' '}
                    <span className="mono">{cardlessUnresponsive.join(', ')}</span>
                </p>
            ) : null}
        </Panel>
    );
}
