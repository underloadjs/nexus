// Shared display helpers for the Nexus Monitor.
//
// Kept free of React imports on purpose: the unit tests run in the plain
// node vitest environment (no jsdom), and components stay render-only.

// "just now" threshold: below one second there is nothing to show.
const JUST_NOW_MS = 1000;
const MINUTE_MS = 60_000;
const HOUR_MS = 60 * MINUTE_MS;

// Zero-pad the sub-unit so the text width does not jitter mid-sentence while
// the ticker re-renders every second.
function pad(value: number): string {
    return value.toString().padStart(2, '0');
}

// Human age for poll freshness ("last updated 3s ago") and per-row
// last-seen/since columns. Guards:
// - null/undefined/NaN timestamps render as '—' (a missing optional from a
//   hub that does not report it yet must never render "49d ago");
// - future timestamps (browser clock behind the hub) clamp to "just now";
// - sub-second ages collapse to "just now".
export function formatAge(fromMs: number | null | undefined, nowMs: number): string {
    if (typeof fromMs !== 'number' || !Number.isFinite(fromMs)) return '—';
    const age = nowMs - fromMs;
    if (age < JUST_NOW_MS) return 'just now';
    if (age < MINUTE_MS) return `${Math.floor(age / 1000)}s ago`;
    if (age < HOUR_MS) return `${Math.floor(age / MINUTE_MS)}m ${pad(Math.floor((age % MINUTE_MS) / 1000))}s ago`;
    return `${Math.floor(age / HOUR_MS)}h ${pad(Math.floor((age % HOUR_MS) / MINUTE_MS))}m ago`;
}

// Compact labels for the graph and tables: long ids (w-9f3c…, job-71ab…)
// stay recognisable when abbreviated as `w-9f3c…7d2`.
export function shortId(id: string, prefix = 6, suffix = 3): string {
    if (id.length <= prefix + suffix + 1) return id;
    return `${id.slice(0, prefix)}…${id.slice(-suffix)}`;
}

// Thousands-separated counter values for the metrics tables. Non-finite
// values (NaN / ±Inf survive the Prometheus text format) fall through to
// their literal string so the table never shows a blank cell.
export function formatCounter(value: number): string {
    return Number.isFinite(value) ? value.toLocaleString('en-US') : String(value);
}

// Absolute clock time (Overview's "hub clock" sub line next to the browser's
// own age), so browser/hub drift is visible at a glance.
export function formatClockTime(ms: number): string {
    return new Date(ms).toLocaleTimeString('en-US', { hour12: false });
}

// Plain integer-ish text for values that must not gain separators (ids,
// protocol strings, raw JSON dumps already handled elsewhere).
export function formatNumber(value: number | null): string {
    return value === null || !Number.isFinite(value) ? '—' : String(value);
}
