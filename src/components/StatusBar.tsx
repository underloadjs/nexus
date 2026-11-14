// Header status bar while connected: one chip that says live / connecting / unreachable.
//
// This replaced the old connect bar (hub URL input, Reconnect, Disconnect, Pause,
// Refresh now). Those controls were removed on purpose:
//   - the hub URL is chosen once on the connect screen and remembered in
//     localStorage — pointing at another hub means reloading the page, so there
//     is no in-app reconnect/disconnect;
//   - the poll loop runs on a fixed 1 s schedule with no pause or manual refresh.
// What is left is a pure read-only indicator, so this bar can never mutate the
// hub — the same rule every panel follows.
import { formatAge } from '../utils/format';
import { StateChip, type Tone } from './ui';

export interface StatusBarProps {
    // Poll status, derived in App from the hook state (`paused` no longer
    // exists: the loop has no pause control).
    status: 'pending' | 'live' | 'error';
    lastSuccessMs: number | null;
    // Shared ticker from App: ages/countdowns re-render every second.
    nowMs: number;
    failureCount: number;
    // Absolute time of the next scheduled attempt (backoff ETA for the chip).
    nextAttemptMs: number | null;
}

export function StatusBar({ status, lastSuccessMs, nowMs, failureCount, nextAttemptMs }: StatusBarProps) {
    // Chip copy. `pending` (no data yet) is visually distinct from `error`:
    // the first poll on a wrong URL takes a moment to fail, and a grey
    // "connecting…" is honest there. The hub actually being polled is named by
    // the header's `brand-meta` line, so the tooltips stay generic.
    const chip: { label: string; tone: Tone; title: string } = (() => {
        if (status === 'error') {
            const retry = nextAttemptMs === null ? '' : ` — retrying in ${Math.max(0, Math.ceil((nextAttemptMs - nowMs) / 1000))}s`;
            return { label: `unreachable (${failureCount}×)${retry}`, tone: 'bad', title: 'The hub is not answering /v1/observe' };
        }
        if (status === 'pending') {
            return { label: 'connecting…', tone: 'neutral', title: 'Waiting for the first response from the hub' };
        }
        return { label: `live · ${formatAge(lastSuccessMs, nowMs)}`, tone: 'ok', title: 'Last successful poll' };
    })();

    return (
        <div className="status-bar">
            <StateChip label={chip.label} tone={chip.tone} title={chip.title} />
        </div>
    );
}
