// Header bar while connected: hub URL, connection status, poll controls.
//
// The URL stays editable here (not just on the connect screen) — pointing the
// monitor at a second hub is a matter of editing the field and pressing
// "Reconnect", which is how the hub is usually run (multiple local instances).
import { useState, type FormEvent } from 'react';
import { formatAge } from '../utils/format';
import { normalizeUserUrl, DEFAULT_HUB_URL } from '../utils/hub-url';
import { StateChip, type Tone } from './ui';

export interface ConnectBarProps {
    // The hub currently being polled. The component is remounted (via `key`)
    // whenever this changes, so the local draft always starts from it.
    baseUrl: string;
    // Apply a new/edited URL (re-arms the poller and persists to storage).
    onReconnect: (url: string) => void;
    onDisconnect: () => void;
    paused: boolean;
    onTogglePaused: () => void;
    onRefresh: () => void;
    // Poll status, derived in App from the hook state.
    status: 'pending' | 'live' | 'paused' | 'error';
    lastSuccessMs: number | null;
    // Shared ticker from App: ages/countdowns re-render every second.
    nowMs: number;
    failureCount: number;
    // Absolute time of the next scheduled attempt (backoff ETA for the chip).
    nextAttemptMs: number | null;
}

export function ConnectBar({
    baseUrl,
    onReconnect,
    onDisconnect,
    paused,
    onTogglePaused,
    onRefresh,
    status,
    lastSuccessMs,
    nowMs,
    failureCount,
    nextAttemptMs
}: ConnectBarProps) {
    const [draft, setDraft] = useState(baseUrl);
    const [error, setError] = useState<string | null>(null);

    const submit = (event: FormEvent<HTMLFormElement>) => {
        event.preventDefault();
        const result = normalizeUserUrl(draft);
        if (!result.ok) {
            // Inline hint instead of an alert; the previous hub keeps polling
            // until a valid URL is applied.
            setError(result.error);
            return;
        }
        setError(null);
        onReconnect(result.url);
    };

    // Status chip copy. `pending` (no data yet) is visually distinct from
    // `error`: the first poll on a wrong URL takes a moment to fail, and a
    // grey "connecting…" is honest there.
    const chip: { label: string; tone: Tone; title: string } = (() => {
        if (status === 'error') {
            const retry = nextAttemptMs === null ? '' : ` — retrying in ${Math.max(0, Math.ceil((nextAttemptMs - nowMs) / 1000))}s`;
            return { label: `unreachable (${failureCount}×)${retry}`, tone: 'bad', title: 'The hub is not answering /v1/observe' };
        }
        if (status === 'paused') {
            return { label: `paused · ${formatAge(lastSuccessMs, nowMs)}`, tone: 'warn', title: 'Polling is paused' };
        }
        if (status === 'pending') {
            // The tooltip names the hub actually being polled (not the
            // built-in default), because that is the URL the user just typed.
            return { label: 'connecting…', tone: 'neutral', title: `Waiting for the first response from ${baseUrl}` };
        }
        return { label: `live · ${formatAge(lastSuccessMs, nowMs)}`, tone: 'ok', title: 'Last successful poll' };
    })();

    return (
        <div className="connect-bar">
            <form className="connect-form" onSubmit={submit} noValidate>
                <label className="field-label field-label-inline" htmlFor="hub-url-connected">
                    Hub
                </label>
                <input
                    id="hub-url-connected"
                    className="url-input url-input-compact"
                    type="text"
                    autoComplete="off"
                    spellCheck={false}
                    placeholder={DEFAULT_HUB_URL}
                    value={draft}
                    onChange={(event) => setDraft(event.target.value)}
                />
                <button className="btn" type="submit">
                    Reconnect
                </button>
                <button className="btn btn-ghost" type="button" onClick={onDisconnect}>
                    Disconnect
                </button>
            </form>
            {error ? <p className="field-error field-error-inline">{error}</p> : null}

            <div className="bar-controls">
                <StateChip label={chip.label} tone={chip.tone} title={chip.title} />
                {/* Pause keeps the last snapshot on screen — the point is to
                    freeze a state you are looking at, not to blank it. */}
                <button className="btn" type="button" onClick={onTogglePaused}>
                    {paused ? 'Resume' : 'Pause'}
                </button>
                <button className="btn btn-ghost" type="button" onClick={onRefresh} disabled={paused}>
                    Refresh now
                </button>
            </div>
        </div>
    );
}
