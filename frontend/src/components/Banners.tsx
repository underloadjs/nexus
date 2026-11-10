// Stacked status banners: everything that must be seen without scrolling and
// must never disappear silently.
//
// Order is severity: mixed-content (the page can never connect) → poll error
// (it stopped working) → protocol mismatch (data may be misread).
import { formatAge } from '../utils/format';

export interface BannersProps {
    // https page + http hub on a non-loopback host: the browser blocks every
    // request, so this is a permanent banner, not a toast.
    mixedContent: boolean;
    // Observe payload came back but with an unexpected `protocol`.
    protocolWarning?: string;
    // Last /v1/observe failure (null while healthy).
    error: string | null;
    failureCount: number;
    nextAttemptMs: number | null;
    // Last good snapshot — while erroring we keep showing it, marked stale.
    lastSuccessMs: number | null;
    nowMs: number;
}

export function Banners({ mixedContent, protocolWarning, error, failureCount, nextAttemptMs, lastSuccessMs, nowMs }: BannersProps) {
    const retryIn = nextAttemptMs === null ? null : Math.max(0, Math.ceil((nextAttemptMs - nowMs) / 1000));

    return (
        <>
            {mixedContent ? (
                <div className="banner banner-warn" role="status">
                    <strong>Mixed content blocked.</strong> This page is https, but the hub is http on a non-localhost
                    host — browsers refuse that combination. Put the hub behind https (reverse proxy or tunnel), or run
                    this monitor locally with <code>yarn dev</code>.
                </div>
            ) : null}

            {error ? (
                <div className="banner banner-error" role="alert">
                    <strong>Cannot poll the hub.</strong> <span className="mono">{error}</span>
                    <span className="banner-meta">
                        {`failures: ${failureCount}`}
                        {retryIn === null ? null : ` · retrying in ~${retryIn}s`}
                        {lastSuccessMs === null ? ' · no data received yet' : ` · showing data from ${formatAge(lastSuccessMs, nowMs)}`}
                    </span>
                </div>
            ) : null}

            {protocolWarning ? (
                <div className="banner banner-info" role="status">
                    <strong>Protocol mismatch.</strong> {protocolWarning}
                </div>
            ) : null}
        </>
    );
}
