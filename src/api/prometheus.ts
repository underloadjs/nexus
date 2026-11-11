// Minimal Prometheus text-exposition parser (format v0.0.4 subset) for the
// hub's `GET /metrics`. Pure + dependency-free so the node-environment unit
// tests can feed it raw exposition text.
//
// Deliberately lenient: monitoring must keep working when the hub adds
// metric families this UI has never seen. Unknown/malformed sample lines are
// skipped, and nothing here throws — a parse failure would blank the whole
// panel, whereas a skipped line only loses one series.

export interface MetricSeries {
    // Label dimensions ({} when the sample is unlabelled).
    labels: Record<string, string>;
    // NaN / ±Inf are valid exposition values and are preserved as numbers.
    value: number;
}

export interface MetricFamily {
    name: string;
    // Captured from `# HELP` / `# TYPE` comment lines when present.
    help?: string;
    type?: string;
    series: MetricSeries[];
}

// Metric names per the exposition spec (also what the hub emits: nexus_*).
const NAME_PATTERN = /^[a-zA-Z_:][a-zA-Z0-9_:]*$/;

// Prometheus spells non-finite values `NaN`, `+Inf`, `-Inf` (bare `Inf`
// shows up in some clients' output too). Everything else goes through
// Number(); non-numeric becomes null so the caller can skip the line.
function parseSampleValue(token: string): number | null {
    if (token === 'NaN') return Number.NaN;
    if (token === '+Inf' || token === 'Inf') return Number.POSITIVE_INFINITY;
    if (token === '-Inf') return Number.NEGATIVE_INFINITY;
    const value = Number(token);
    return Number.isNaN(value) ? null : value;
}

// Scan forward from an opening `{` to its matching `}`, honouring quoted
// label values (which may contain `}`, quotes and backslash escapes).
// Returns the closing brace index, or -1 for an unterminated label block.
function findClosingBrace(text: string, openIndex: number): number {
    let inQuotes = false;
    let escaped = false;
    for (let index = openIndex + 1; index < text.length; index += 1) {
        const char = text[index];
        if (inQuotes) {
            // Inside a quoted value, `\` escapes the next char (e.g. `\"`).
            if (escaped) escaped = false;
            else if (char === '\\') escaped = true;
            else if (char === '"') inQuotes = false;
            continue;
        }
        if (char === '"') inQuotes = true;
        else if (char === '}') return index;
    }
    return -1;
}

// Parse the inside of a label block: `a="1",b="two"}`-ish. Tolerates a
// trailing comma and skips individual malformed pairs.
function parseLabels(inner: string): Record<string, string> {
    const labels: Record<string, string> = {};
    let index = 0;
    while (index < inner.length) {
        // Skip separators/whitespace between pairs.
        while (index < inner.length && /[\s,]/.test(inner[index] ?? '')) index += 1;
        const nameStart = index;
        while (index < inner.length && /[a-zA-Z0-9_]/.test(inner[index] ?? '')) index += 1;
        const name = inner.slice(nameStart, index);
        if (name.length === 0) break; // anything else here is unparseable — stop
        while (index < inner.length && inner[index] === ' ') index += 1;
        if (inner[index] !== '=') break; // malformed pair (no `=`) — stop
        index += 1;
        while (index < inner.length && inner[index] === ' ') index += 1;
        if (inner[index] !== '"') break; // unquoted values are out of spec — stop
        index += 1;
        let escaped = false;
        let value = '';
        for (; index < inner.length; index += 1) {
            const char = inner[index] ?? '';
            if (escaped) {
                // Only \n becomes a real newline; \" and \\ collapse to the
                // literal character (spec-sanctioned escapes).
                value += char === 'n' ? '\n' : char;
                escaped = false;
                continue;
            }
            if (char === '\\') {
                escaped = true;
                continue;
            }
            if (char === '"') break;
            value += char;
        }
        index += 1; // step over the closing quote
        // The loop above already resolved \\ \" \n, so no second unescape.
        labels[name] = value;
    }
    return labels;
}

// Parse one sample line into its parts, or null when the line is not a
// well-formed sample.
function parseSampleLine(line: string): { name: string; labels: Record<string, string>; value: number } | null {
    // Metric name runs until `{` or whitespace.
    let index = 0;
    while (index < line.length && line[index] !== '{' && line[index] !== ' ' && line[index] !== '\t') index += 1;
    const name = line.slice(0, index);
    if (!NAME_PATTERN.test(name)) return null;

    let labels: Record<string, string> = {};
    if (line[index] === '{') {
        const close = findClosingBrace(line, index);
        if (close < 0) return null; // unterminated labels — skip the line
        labels = parseLabels(line.slice(index + 1, close));
        index = close + 1;
    }

    // Remainder is `value [timestamp] [exemplar]` — only the value matters.
    const rest = line.slice(index).trim().split(/\s+/);
    if (rest.length === 0 || rest[0] === '') return null;
    const value = parseSampleValue(rest[0] ?? '');
    if (value === null) return null;
    return { name, labels, value };
}

// Parse a whole exposition body into families, preserving the hub's emission
// order. CRLF and trailing blank lines are handled (the hub serves \n, but
// a proxy could rewrite it).
export function parsePrometheusText(text: string): MetricFamily[] {
    const families = new Map<string, MetricFamily>();

    // get-or-create, so HELP/TYPE lines can attach metadata before samples.
    const familyOf = (name: string): MetricFamily => {
        let family = families.get(name);
        if (!family) {
            family = { name, series: [] };
            families.set(name, family);
        }
        return family;
    };

    for (const rawLine of text.split('\n')) {
        const line = rawLine.replace(/\r$/, '');
        if (line.length === 0) continue;

        if (line.startsWith('#')) {
            // Comment metadata: `# HELP <name> <free text>` /
            // `# TYPE <name> <type>`. Anything else (examples, unit hints) is
            // ignored rather than guessed at.
            const comment = line.slice(1).trimStart();
            const parts = comment.split(/\s+/);
            const directive = parts[0];
            const name = parts[1];
            if (name && !NAME_PATTERN.test(name)) continue;
            if (directive === 'HELP' && name) {
                // The help text is free-form (may contain any whitespace), so
                // only the `<directive> <name>` prefix is stripped.
                familyOf(name).help = comment.slice(`HELP ${name}`.length).trim();
            } else if (directive === 'TYPE' && name) {
                familyOf(name).type = parts[2] ?? '';
            }
            continue;
        }

        const sample = parseSampleLine(line);
        if (!sample) continue;
        familyOf(sample.name).series.push({ labels: sample.labels, value: sample.value });
    }

    return [...families.values()];
}

export interface SummaryAverage {
    sum: number;
    count: number;
    // null when the summary has never been observed (count === 0) — the UI
    // renders "n/a" instead of a misleading 0 ms average.
    average: number | null;
}

// Average of a `<base>_sum` / `<base>_count` summary pair, aggregating every
// series of both families (the hub emits unlabelled summaries today, but a
// labelled breakdown still sums correctly). Missing families yield null so
// callers can hide the row instead of showing zeros.
export function summaryAverage(families: MetricFamily[], base: string): SummaryAverage | null {
    const suffixSum = families.find((family) => family.name === `${base}_sum`);
    const suffixCount = families.find((family) => family.name === `${base}_count`);
    if (!suffixSum || !suffixCount) return null;

    // Only finite values contribute: a NaN sample would poison the mean.
    const sum = suffixSum.series.reduce((total, series) => (Number.isFinite(series.value) ? total + series.value : total), 0);
    const count = suffixCount.series.reduce((total, series) => (Number.isFinite(series.value) ? total + series.value : total), 0);
    return { sum, count, average: count > 0 ? sum / count : null };
}

// Total of a counter family across all its label series (accepted/rejected
// counters are `nexus_jobs_accepted_total` style single series, while
// rejected/terminal carry a `reason` label — summing gives the headline).
export function counterTotal(families: MetricFamily[], name: string): number | null {
    const family = families.find((candidate) => candidate.name === name);
    if (!family) return null;
    return family.series.reduce((total, series) => (Number.isFinite(series.value) ? total + series.value : total), 0);
}
