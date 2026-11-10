// App root: owns the hub URL, the poll loop and the shared clock, and lays
// out the panels. No panel fetches anything by itself — every number below
// comes from one snapshot, so the dashboard is internally consistent (two
// separate pollers would show a job leased to a worker that the other panel
// says is gone).
import { useCallback, useState } from 'react';
import { Banners } from './components/Banners';
import { ConnectBar } from './components/ConnectBar';
import { ConnectScreen } from './components/ConnectScreen';
import { ConnectionsPanel } from './components/ConnectionsPanel';
import { ConsumersPanel } from './components/ConsumersPanel';
import { JobsPanel } from './components/JobsPanel';
import { MetricsPanel } from './components/MetricsPanel';
import { OverviewPanel } from './components/OverviewPanel';
import { PoolsPanel } from './components/PoolsPanel';
import { SuppliersPanel } from './components/SuppliersPanel';
import { useNexusPoll } from './hooks/use-nexus-poll';
import { useNowTick } from './hooks/use-now-tick';
import { getPageProtocol, isMixedContentBlocked, loadStoredUrl, saveStoredUrl } from './utils/hub-url';

export function App() {
    // null = the connect screen. Kept in state (not derived from storage) so
    // "Disconnect" is immediate and storage remains only a convenience.
    const [baseUrl, setBaseUrl] = useState<string | null>(null);
    // Seed for the connect input, read once: localStorage access can throw,
    // which loadStoredUrl already swallows.
    const [storedUrl] = useState<string>(() => loadStoredUrl() ?? '');

    const poll = useNexusPoll(baseUrl);
    const nowMs = useNowTick();

    const connect = useCallback((url: string) => {
        saveStoredUrl(url);
        setBaseUrl(url);
    }, []);

    const disconnect = useCallback(() => setBaseUrl(null), []);

    // Pausing is a view control: the snapshot stays on screen, frozen.
    const togglePaused = useCallback(() => poll.setPaused(!poll.paused), [poll]);

    // Chip/banner status. Error wins over paused: if the hub died while a
    // poll was in flight, "paused · 3s ago" would hide the real problem.
    const status = baseUrl === null ? 'pending' : poll.error !== null ? 'error' : poll.paused ? 'paused' : poll.snapshot !== null ? 'live' : 'pending';

    const mixedContent = baseUrl !== null && isMixedContentBlocked(baseUrl, getPageProtocol());

    // Not connected: the connect screen is the entire page.
    if (baseUrl === null) {
        return (
            <div className="app">
                <ConnectScreen initialUrl={storedUrl} onConnect={connect} />
                <footer className="app-footer">
                    Nexus Monitor · read-only · polls <code>/v1/observe</code> and <code>/metrics</code> directly from the browser
                </footer>
            </div>
        );
    }

    // Connected but the first response has not landed (or is failing): say
    // what is happening instead of showing nine empty panels.
    const booting = poll.snapshot === null && poll.error === null;

    return (
        <div className="app">
            <header className="app-header">
                <div className="brand">
                    <h1>Nexus Monitor</h1>
                    <span className="brand-meta">
                        polling <span className="mono">{baseUrl}</span> · read-only
                    </span>
                </div>
                {/* `key` remounts the bar on every URL change so its local
                    draft input re-seeds from the connected hub (no
                    props-state sync effect needed). */}
                <ConnectBar
                    key={baseUrl}
                    baseUrl={baseUrl}
                    onReconnect={connect}
                    onDisconnect={disconnect}
                    paused={poll.paused}
                    onTogglePaused={togglePaused}
                    onRefresh={poll.refresh}
                    status={status}
                    lastSuccessMs={poll.lastSuccessMs}
                    nowMs={nowMs}
                    failureCount={poll.failureCount}
                    nextAttemptMs={poll.nextAttemptMs}
                />
            </header>

            <Banners
                mixedContent={mixedContent}
                protocolWarning={poll.protocolWarning}
                error={poll.error}
                failureCount={poll.failureCount}
                nextAttemptMs={poll.nextAttemptMs}
                lastSuccessMs={poll.lastSuccessMs}
                nowMs={nowMs}
            />

            {booting ? (
                <div className="boot">
                    <span className="boot-spinner" aria-hidden />
                    <span>
                        Connecting to <span className="mono">{baseUrl}</span> …
                    </span>
                </div>
            ) : poll.snapshot === null ? (
                // Failed before ever succeeding: one honest panel beats the
                // whole grid of zeros a naive render would produce.
                <div className="boot boot-error">
                    No data yet from <span className="mono">{baseUrl}</span> — check that the hub is running and that its
                    CORS allows this origin.
                </div>
            ) : (
                <main className="grid">
                    <div className="span-all">
                        <OverviewPanel snapshot={poll.snapshot} metrics={poll.metrics} nowMs={nowMs} />
                    </div>
                    <PoolsPanel snapshot={poll.snapshot} />
                    <JobsPanel snapshot={poll.snapshot} nowMs={nowMs} />
                    <div className="span-all">
                        <ConnectionsPanel snapshot={poll.snapshot} />
                    </div>
                    <SuppliersPanel snapshot={poll.snapshot} nowMs={nowMs} />
                    <ConsumersPanel snapshot={poll.snapshot} nowMs={nowMs} />
                    <div className="span-all">
                        <MetricsPanel metrics={poll.metrics} metricsError={poll.metricsError} />
                    </div>
                </main>
            )}

            <footer className="app-footer">
                Nexus Monitor · state from <code>/v1/observe</code> (polled, not streamed) · counters from{' '}
                <code>/metrics</code> · the hub is never mutated by this page
            </footer>
        </div>
    );
}
