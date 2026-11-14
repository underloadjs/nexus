// HTTP polling hook for the nexus hub.
//
// Why polling and not WebSockets: the hub exposes a plain, open HTTP
// snapshot (`GET /v1/observe`) plus a Prometheus text endpoint, and the
// monitor is a static GitHub Pages bundle — no signalling, no reconnect
// protocol to get wrong from a browser tab that may be backgrounded.
//
// The hook owns three concerns that are easy to get wrong together, so they
// live in one place instead of in the components:
//   1. one request in flight at a time (a stale response is aborted, never
//      rendered — otherwise a slow poll could overwrite newer data),
//   2. exponential backoff while the hub is unreachable (a dead hub must not
//      become a request-per-second hammer),
//   3. full teardown when the hub URL goes away (abort + clear timer, no
//      orphans).
//
// There is deliberately no pause and no manual refresh: the monitor has no
// controls that stop or accelerate the loop, so the schedule is the only
// driver (see StatusBar.tsx for the read-only indicator it feeds).
import { useEffect, useState } from 'react';
import { fetchMetricsText, fetchObserve, type NormalizedSnapshot } from '../api/nexus-client';
import { parsePrometheusText, type MetricFamily } from '../api/prometheus';

// Default cadence: the hub snapshot is cheap (a read of in-memory state),
// and 1 s is fast enough to watch a job dispatch without flooding the log.
export const DEFAULT_POLL_INTERVAL_MS = 1000;

// Backoff ceiling — after a few failures the monitor checks the hub at most
// every 15 s and keeps retrying forever (hubs restart; operators walk away).
export const MAX_BACKOFF_MS = 15_000;

export interface PollState {
    // Last good snapshot (kept on transient failure so the dashboard does
    // not blank out while the hub hiccups).
    snapshot: NormalizedSnapshot | null;
    protocolWarning?: string;
    // Metrics are best-effort: an older hub may serve /v1/observe but not
    // /metrics, which must not disable the rest of the dashboard.
    metrics: MetricFamily[] | null;
    metricsError: string | null;
    // Fatal-ish failure of /v1/observe (network/CORS/HTTP/bad payload).
    error: string | null;
    // Consecutive failures — drives the backoff and the banner copy.
    failureCount: number;
    // Browser clock of the last successful observe (drives "last updated").
    lastSuccessMs: number | null;
    // Absolute time of the scheduled next attempt (banner: "retrying in 3s").
    nextAttemptMs: number | null;
    hasCompletedFirstAttempt: boolean;
}

// Exponential backoff: interval, 2×, 4× … clamped. Attempt 1 (the first
// failure) already waits `interval`, so a momentary blip is invisible.
export function backoffDelayMs(failureCount: number, intervalMs: number): number {
    const growth = 2 ** Math.max(0, failureCount - 1);
    return Math.min(intervalMs * growth, MAX_BACKOFF_MS);
}

// Hook state machine. `baseUrl` null means "no hub attached": the effect tears
// everything down and the state resets to the connect screen's expectations.
export function useNexusPoll(baseUrl: string | null, intervalMs: number = DEFAULT_POLL_INTERVAL_MS): PollState {
    const [state, setState] = useState<PollState>({
        snapshot: null,
        protocolWarning: undefined,
        metrics: null,
        metricsError: null,
        error: null,
        failureCount: 0,
        lastSuccessMs: null,
        nextAttemptMs: null,
        hasCompletedFirstAttempt: false
    });

    useEffect(() => {
        if (!baseUrl) {
            // No hub attached: wipe the slate so the next connect starts from
            // a clean "connecting…" state.
            setState({
                snapshot: null,
                protocolWarning: undefined,
                metrics: null,
                metricsError: null,
                error: null,
                failureCount: 0,
                lastSuccessMs: null,
                nextAttemptMs: null,
                hasCompletedFirstAttempt: false
            });
            return;
        }

        let cancelled = false;
        let timer: ReturnType<typeof setTimeout> | undefined;
        // One AbortController per attempt (not per effect): each new attempt
        // supersedes anything still in flight.
        let inFlight: AbortController | null = null;
        // Consecutive failures live in a closure counter rather than in React
        // state: the schedule must know them synchronously, and they are only
        // mirrored into state for the banner copy.
        let failures = 0;

        const schedule = (delayMs: number) => {
            if (cancelled) return;
            clearTimeout(timer);
            setState((prev) => ({ ...prev, nextAttemptMs: Date.now() + delayMs }));
            timer = setTimeout(poll, delayMs);
        };

        async function poll() {
            inFlight?.abort();
            const controller = new AbortController();
            inFlight = controller;
            try {
                // Observe is authoritative. Metrics is wrapped so its failure
                // cannot take the whole dashboard down (an older hub may
                // serve /v1/observe without /metrics).
                const metricsPromise: Promise<{ text: string | null; error: string | null }> = fetchMetricsText(
                    baseUrl as string,
                    controller.signal
                ).then(
                    (text) => ({ text, error: null }),
                    // Rejection handler (not catch) so a metrics failure never
                    // turns into an observe-level error.
                    (error: Error) => ({ text: null, error: error?.message ?? String(error) })
                );
                const [observe, metrics] = await Promise.all([
                    fetchObserve(baseUrl as string, controller.signal),
                    metricsPromise
                ]);
                if (cancelled || controller.signal.aborted) return; // stale response

                failures = 0;
                setState((prev) => ({
                    ...prev,
                    snapshot: observe.snapshot,
                    protocolWarning: observe.protocolWarning,
                    // Keep the previous families when metrics failed: the
                    // Metrics panel then shows the last known numbers with the
                    // error note, instead of an empty table.
                    metrics: metrics.text === null ? prev.metrics : parsePrometheusText(metrics.text),
                    metricsError: metrics.error,
                    error: null,
                    failureCount: 0,
                    lastSuccessMs: observe.snapshot.fetchedAtMs,
                    hasCompletedFirstAttempt: true
                }));
                schedule(intervalMs);
            } catch (error) {
                if (cancelled || controller.signal.aborted) return;
                // An explicit abort (teardown/replace) is not an error.
                if ((error as Error | undefined)?.name === 'AbortError') return;
                failures += 1;
                setState((prev) => ({
                    ...prev,
                    // Keep the last snapshot: stale data with an error banner
                    // is more useful than an empty dashboard.
                    error: (error as Error | undefined)?.message ?? String(error),
                    failureCount: failures,
                    hasCompletedFirstAttempt: true
                }));
                schedule(backoffDelayMs(failures, intervalMs));
            }
        }

        // First attempt fires immediately on connect, then on the schedule.
        void poll();

        return () => {
            cancelled = true;
            clearTimeout(timer);
            inFlight?.abort();
        };
        // The effect deliberately does NOT depend on poll state (that would
        // restart the loop every second); only the hub URL and cadence rebuild
        // the schedule.
    }, [baseUrl, intervalMs]);

    return state;
}
