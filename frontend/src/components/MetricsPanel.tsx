// Metrics panel: the hub's Prometheus exposition (/metrics) rendered as
// human-readable tables instead of a text dump.
//
// Two views, because the two kinds of Prometheus series answer different
// questions:
//   - summaries (`…_sum` / `…_count`) → an average latency, which is what a
//     human compares over time;
//   - counters/gauges → current totals, with label breakdowns (reject
//     reasons, frame kinds) rendered as separate rows.
import { useMemo, useState } from 'react';
import { summaryAverage, type MetricFamily } from '../api/prometheus';
import { formatCounter } from '../utils/format';
import { EmptyState, Panel, StateChip } from './ui';

// Latency summaries the hub exposes; listed explicitly so the panel has a
// stable, meaningful headline even when the hub adds new metrics.
export const LATENCY_METRICS: string[] = ['nexus_dispatch_latency_ms', 'nexus_ttft_ms', 'nexus_job_duration_ms'];

// Safety cap on the raw table: a debug build exporting thousands of series
// should not render thousands of DOM rows.
const MAX_RAW_ROWS = 300;

export interface MetricsPanelProps {
    // null while the first /metrics fetch has not landed (or never will).
    metrics: MetricFamily[] | null;
    metricsError: string | null;
}

// `le="…"` (histogram buckets) and `_bucket` families are noise for an
// operator; the summary averages above cover the same information.
function isRawRowInteresting(family: MetricFamily): boolean {
    return !family.name.endsWith('_bucket');
}

// Label map → compact `k="v"` text for a row's dimension column.
function formatLabels(labels: Record<string, string>): string {
    const entries = Object.entries(labels);
    if (entries.length === 0) return '—';
    return entries.map(([key, value]) => `${key}="${value}"`).join(' ');
}

export function MetricsPanel({ metrics, metricsError }: MetricsPanelProps) {
    const [filter, setFilter] = useState('');

    const summaries = useMemo(
        () => LATENCY_METRICS.map((name) => ({ name, average: metrics ? summaryAverage(metrics, name) : null })),
        [metrics]
    );

    const rows = useMemo(() => {
        if (!metrics) return [];
        const needle = filter.trim().toLowerCase();
        const all = metrics
            .filter(isRawRowInteresting)
            .flatMap((family) =>
                family.series.map((series) => ({
                    key: `${family.name}|${formatLabels(series.labels)}`,
                    name: family.name,
                    type: family.type ?? '',
                    labels: formatLabels(series.labels),
                    value: series.value
                }))
            );
        if (needle.length === 0) return all;
        return all.filter((row) => row.name.toLowerCase().includes(needle) || row.labels.toLowerCase().includes(needle));
    }, [metrics, filter]);

    return (
        <Panel
            title="Metrics"
            count={metrics ? `${rows.length} series` : '—'}
            actions={
                <input
                    className="filter-input"
                    type="search"
                    placeholder="filter name or labels"
                    value={filter}
                    onChange={(event) => setFilter(event.target.value)}
                    aria-label="filter metrics"
                />
            }
            hint="Prometheus exposition parsed in the browser: latency averages from _sum/_count, counters with their label breakdown."
        >
            {metricsError ? (
                // /metrics failing must not invalidate the rest of the
                // dashboard — /v1/observe is the authoritative feed.
                <div className="banner banner-warn">
                    <strong>/metrics unavailable.</strong> <span className="mono">{metricsError}</span> The other panels keep working from /v1/observe.
                </div>
            ) : null}

            <div className="latency-grid">
                {summaries.map((summary) => {
                    // null family → nothing reported; average null → the
                    // summary exists but has never been observed (count 0).
                    // Both render "n/a" rather than a fabricated 0 ms.
                    const average = summary.average ? summary.average.average : null;
                    return (
                        <div key={summary.name} className="latency-card">
                            <span className="stat-label">{summary.name.replace(/^nexus_/, '').replace(/_ms$/, '')}</span>
                            <span className="stat-value">{average === null ? 'n/a' : `${average.toFixed(1)} ms`}</span>
                            <span className="stat-sub">
                                {summary.average === null
                                    ? 'no _sum/_count family'
                                    : `${formatCounter(summary.average.count)} samples · Σ ${formatCounter(summary.average.sum)} ms`}
                            </span>
                        </div>
                    );
                })}
            </div>

            {!metrics ? (
                <EmptyState message="No metrics yet." hint="The first /metrics fetch has not succeeded — check the banner above if it persists." />
            ) : rows.length === 0 ? (
                <EmptyState message="No series match the filter." hint="Clear the filter to see everything the hub exposes." />
            ) : (
                <div className="table-scroll">
                    <table className="table table-compact">
                        <thead>
                            <tr>
                                <th>metric</th>
                                <th>type</th>
                                <th>labels</th>
                                <th>value</th>
                            </tr>
                        </thead>
                        <tbody>
                            {rows.slice(0, MAX_RAW_ROWS).map((row) => (
                                <tr key={row.key}>
                                    <td className="mono">{row.name}</td>
                                    <td>{row.type ? <StateChip label={row.type} tone="info" /> : '—'}</td>
                                    <td className="mono cell-labels" title={row.labels}>
                                        {row.labels}
                                    </td>
                                    <td className="mono">{formatCounter(row.value)}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                    {rows.length > MAX_RAW_ROWS ? (
                        <p className="graph-note">
                            Showing the first {MAX_RAW_ROWS} of {rows.length} series — narrow the filter to see the rest.
                        </p>
                    ) : null}
                </div>
            )}
        </Panel>
    );
}
