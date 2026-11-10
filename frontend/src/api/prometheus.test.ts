// Unit tests for the Prometheus text-exposition parser: the hub's /metrics
// is plain text, and the monitor must survive both the documented shapes
// (labels, summaries, comments, NaN/Inf) and sloppy lines.
import { describe, expect, it } from 'vitest';
import { counterTotal, parsePrometheusText, summaryAverage } from './prometheus';

// A realistic hub body: HELP/TYPE pairs, labelled counters, an unlabelled
// gauge, a summary pair, non-finite values, a label value containing an
// escaped quote and a closing brace, plus junk that must be skipped.
const EXPOSITION = [
    '# HELP nexus_queue_depth Jobs waiting for a worker',
    '# TYPE nexus_queue_depth gauge',
    'nexus_queue_depth{pool="llm.chat/qwen38"} 3',
    'nexus_queue_depth{pool="llm.embed"} 1',
    '# UNIT seconds',
    '',
    '# TYPE nexus_jobs_rejected_total counter',
    'nexus_jobs_rejected_total{reason="no_capacity"} 2',
    'nexus_jobs_rejected_total{reason="pool_unresponsive"} 1',
    'nexus_jobs_accepted_total 42 1762760000000',
    '# TYPE nexus_dispatch_latency_ms summary',
    'nexus_dispatch_latency_ms_count 4',
    'nexus_dispatch_latency_ms_sum 120',
    'nexus_ttft_ms_count 0',
    'nexus_ttft_ms_sum 0',
    'nexus_frames_total{kind="result"} 12',
    'nexus_metric_with_nan NaN',
    'nexus_metric_with_pos_inf +Inf',
    'nexus_metric_with_neg_inf -Inf',
    'nexus_escaped{path="a\\"b}c"} 5',
    'nexus_trailing_comma{a="1",b="2",} 7',
    'this line is not a sample',
    'nexus_missing_value',
    'nexus_unterminated{broken 7',
    '# EOF'
].join('\n');

describe('parsePrometheusText', () => {
    it('parses the documented shapes into exact families', () => {
        // The whole result, in emission order — nothing invented, nothing
        // dropped except the three deliberately malformed lines.
        expect(parsePrometheusText(EXPOSITION)).toEqual([
            {
                name: 'nexus_queue_depth',
                help: 'Jobs waiting for a worker',
                type: 'gauge',
                series: [
                    { labels: { pool: 'llm.chat/qwen38' }, value: 3 },
                    { labels: { pool: 'llm.embed' }, value: 1 }
                ]
            },
            {
                name: 'nexus_jobs_rejected_total',
                type: 'counter',
                series: [
                    { labels: { reason: 'no_capacity' }, value: 2 },
                    { labels: { reason: 'pool_unresponsive' }, value: 1 }
                ]
            },
            { name: 'nexus_jobs_accepted_total', series: [{ labels: {}, value: 42 }] },
            {
                name: 'nexus_dispatch_latency_ms',
                type: 'summary',
                series: []
            },
            { name: 'nexus_dispatch_latency_ms_count', series: [{ labels: {}, value: 4 }] },
            { name: 'nexus_dispatch_latency_ms_sum', series: [{ labels: {}, value: 120 }] },
            { name: 'nexus_ttft_ms_count', series: [{ labels: {}, value: 0 }] },
            { name: 'nexus_ttft_ms_sum', series: [{ labels: {}, value: 0 }] },
            { name: 'nexus_frames_total', series: [{ labels: { kind: 'result' }, value: 12 }] },
            { name: 'nexus_metric_with_nan', series: [{ labels: {}, value: Number.NaN }] },
            { name: 'nexus_metric_with_pos_inf', series: [{ labels: {}, value: Number.POSITIVE_INFINITY }] },
            { name: 'nexus_metric_with_neg_inf', series: [{ labels: {}, value: Number.NEGATIVE_INFINITY }] },
            // Label value with an escaped quote AND a closing brace inside.
            { name: 'nexus_escaped', series: [{ labels: { path: 'a"b}c' }, value: 5 }] },
            { name: 'nexus_trailing_comma', series: [{ labels: { a: '1', b: '2' }, value: 7 }] }
        ]);
    });

    it('handles CRLF line endings and a trailing newline', () => {
        expect(parsePrometheusText('nexus_a 1\r\nnexus_b{t="x"} 2\r\n')).toEqual([
            { name: 'nexus_a', series: [{ labels: {}, value: 1 }] },
            { name: 'nexus_b', series: [{ labels: { t: 'x' }, value: 2 }] }
        ]);
    });

    it('returns nothing for an empty body', () => {
        expect(parsePrometheusText('')).toEqual([]);
        expect(parsePrometheusText('\n\n# only a comment\n')).toEqual([]);
    });

    it('keeps a metadata-only family (TYPE before any sample)', () => {
        // Deliberate: a hub that has declared a metric but never observed it
        // should still appear as an empty family, not vanish.
        expect(parsePrometheusText('# TYPE nexus_never_observed counter\n')).toEqual([
            { name: 'nexus_never_observed', type: 'counter', series: [] }
        ]);
    });

    it('supports the histogram bucket shape', () => {
        expect(
            parsePrometheusText(
                [
                    '# TYPE nexus_job_duration_ms_bucket histogram',
                    'nexus_job_duration_ms_bucket{le="10"} 3',
                    'nexus_job_duration_ms_bucket{le="+Inf"} 5'
                ].join('\n')
            )
        ).toEqual([
            {
                name: 'nexus_job_duration_ms_bucket',
                type: 'histogram',
                series: [
                    { labels: { le: '10' }, value: 3 },
                    { labels: { le: '+Inf' }, value: 5 }
                ]
            }
        ]);
    });
});

describe('summaryAverage', () => {
    const families = parsePrometheusText(EXPOSITION);

    it('divides _sum by _count for a summary pair', () => {
        expect(summaryAverage(families, 'nexus_dispatch_latency_ms')).toEqual({ sum: 120, count: 4, average: 30 });
    });

    it('reports null (not 0) when nothing has been observed', () => {
        // count === 0 → an "average of 0 ms" would be a lie.
        expect(summaryAverage(families, 'nexus_ttft_ms')).toEqual({ sum: 0, count: 0, average: null });
    });

    it('returns null when the family is absent', () => {
        expect(summaryAverage(families, 'nexus_job_duration_ms')).toBeNull();
    });

    it('sums across label series when a summary is split', () => {
        const split = parsePrometheusText(['split_sum{pool="a"} 10', 'split_sum{pool="b"} 20', 'split_count{pool="a"} 1', 'split_count{pool="b"} 4'].join('\n'));
        expect(summaryAverage(split, 'split')).toEqual({ sum: 30, count: 5, average: 6 });
    });
});

describe('counterTotal', () => {
    const families = parsePrometheusText(EXPOSITION);

    it('sums a labelled counter across its reasons', () => {
        expect(counterTotal(families, 'nexus_jobs_rejected_total')).toBe(3);
    });

    it('returns the plain value for an unlabelled counter', () => {
        expect(counterTotal(families, 'nexus_jobs_accepted_total')).toBe(42);
    });

    it('returns null for a metric the hub does not expose', () => {
        // null lets the UI print "n/a" instead of a confident 0.
        expect(counterTotal(families, 'nexus_jobs_accepted')).toBeNull();
    });
});
