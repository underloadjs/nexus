// Nexus hub observation client: typed fetch + defensive normalization of
// `GET {base}/v1/observe`.
//
// CONTRACT (as implemented server-side, nexus-hub-observe/v1):
// - open endpoint (no auth), HTTP 200 while the hub is up, application/json;
// - every field below is what the hub sends TODAY; the hub may add fields
//   (ignored here) and may omit optionals (null-normalized, never thrown).
// The UI must never crash on a newer/older hub — that is the entire point of
// `normalizeObserveSnapshot`, which is a pure function so the node-environment
// unit tests can feed it hand-written payloads.
import { DEFAULT_HUB_URL, trimTrailingSlashes } from '../utils/hub-url';

// Protocol id the hub reports in `protocol`; a mismatch only warns (the
// payload may still parse fine — the hub is allowed to evolve).
export const NEXUS_OBSERVE_PROTOCOL = 'nexus-hub-observe/v1';

// Endpoint paths appended to the base URL the user pastes.
export const NEXUS_OBSERVE_PATH = '/v1/observe';
export const NEXUS_METRICS_PATH = '/metrics';

// Hub default listen address (env NEXUS_LISTEN) — shown as the placeholder.
// Re-exported from the URL utility so the literal lives in exactly one place.
export const NEXUS_DEFAULT_BASE_URL = DEFAULT_HUB_URL;

// ── Normalized snapshot types (camelCase; every field always present) ──

export type WorkerLink = 'live' | 'orphaned';
export type EngineState = 'loading' | 'ready' | 'degraded';
export type CapacitySignal = 'warm' | 'cold_start' | 'no_workers';

export interface NormalizedReadiness {
    readyWorkers: number;
    loadingWorkers: number;
    queueDepth: number;
    mailboxPending: number;
    warmModels: string[];
    capacitySignal: CapacitySignal;
}

export interface NormalizedPool {
    id: string;
    queueDepth: number;
    readyWorkers: number;
    loadingWorkers: number;
    connectedWorkers: number;
    leases: number;
    mailboxPending: number;
}

export interface NormalizedWorker {
    id: string;
    pools: string[];
    link: WorkerLink;
    engine: EngineState;
    epoch: number;
    capacitySlots: number;
    freeSlots: number;
    // null when the hub reports no KV budget at all — deliberately NOT 0, which
    // means "budget reported and fully spent" (observe.rs
    // `worker_row_reports_a_kv_budget_of_null_rather_than_zero`). Rendered '—'.
    freeKvTokens: number | null;
    inFlight: string[];
    outstandingClaims: number;
    // null when the hub omits it — rendered as "—", never as an age.
    lastSeenMs: number | null;
    // Arbitrary JSON per worker, or null. Rendered read-only (expandable).
    telemetry: unknown;
}

export interface NormalizedLease {
    worker: string;
    epoch: number;
    attempt: number;
    deadlineMs: number | null;
}

export interface NormalizedJob {
    id: string;
    pool: string;
    tenant: string;
    // Free-form: an older/newer hub may add phases; the UI styles known
    // phases and renders everything else as a neutral chip.
    phase: string;
    createdMs: number | null;
    consumers: number;
    // null while the job is queued (no worker holds it).
    lease: NormalizedLease | null;
}

export interface NormalizedConsumerConnection {
    id: string;
    tenant: string;
    sinceMs: number | null;
    attachedJobs: string[];
}

export interface NormalizedSnapshot {
    // Browser clock at parse time — the reference for every "Ns ago" cell,
    // so ages stay stable within one render pass.
    fetchedAtMs: number;
    protocol: string;
    // Hub clock (`now_ms`); 0 when omitted. Displayed next to our own clock
    // so browser/hub drift is visible instead of silently skewing ages.
    serverNowMs: number;
    readiness: NormalizedReadiness;
    pools: NormalizedPool[];
    workers: NormalizedWorker[];
    jobs: NormalizedJob[];
    consumerConnections: NormalizedConsumerConnection[];
    unresponsivePools: string[];
}

// Result of one successful observe poll.
export interface ObserveResult {
    snapshot: NormalizedSnapshot;
    // Non-fatal hint (e.g. protocol mismatch) surfaced as a banner.
    protocolWarning?: string;
}

// ── Unknown-value coercions ───────────────────────────────────────────────
// All are deliberately total: any shape the hub sends maps to something the
// renderer can print, never to a crash.

function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function asArray(value: unknown): unknown[] {
    return Array.isArray(value) ? value : [];
}

// Numeric fields default to 0 on garbage (a counter that says nothing is 0);
// nullableTimestamp prefers null so the UI can tell "not reported" apart
// from "epoch". `free_kv_tokens` joins the nullable group for the same reason:
// the hub sends null when a worker never declared a KV budget, and 0 when it
// has spent it all.
function asNumber(value: unknown): number {
    return typeof value === 'number' && Number.isFinite(value) ? value : 0;
}

function asNullableNumber(value: unknown): number | null {
    return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function asString(value: unknown): string {
    return typeof value === 'string' ? value : '';
}

// Unknown enum members keep their text (so a future hub state is still
// visible in the UI) but satisfy the union type via the cast fallback.
function asEnum<T extends string>(value: unknown, fallback: T): T {
    return typeof value === 'string' && value.length > 0 ? (value as T) : fallback;
}

function asStringList(value: unknown): string[] {
    return asArray(value).filter((item): item is string => typeof item === 'string');
}

// ── Section normalizers ───────────────────────────────────────────────────

function normalizeReadiness(raw: unknown): NormalizedReadiness {
    // A hub without readiness reporting (or a literal null) still yields a
    // zeroed card rather than a missing panel.
    const data = isRecord(raw) ? raw : {};
    return {
        readyWorkers: asNumber(data['ready_workers']),
        loadingWorkers: asNumber(data['loading_workers']),
        queueDepth: asNumber(data['queue_depth']),
        mailboxPending: asNumber(data['mailbox_pending']),
        warmModels: asStringList(data['warm_models']),
        capacitySignal: asEnum<CapacitySignal>(data['capacity_signal'], 'warm')
    };
}

function normalizePool(raw: unknown): NormalizedPool {
    const data = isRecord(raw) ? raw : {};
    return {
        id: asString(data['id']),
        queueDepth: asNumber(data['queue_depth']),
        readyWorkers: asNumber(data['ready_workers']),
        loadingWorkers: asNumber(data['loading_workers']),
        connectedWorkers: asNumber(data['connected_workers']),
        leases: asNumber(data['leases']),
        mailboxPending: asNumber(data['mailbox_pending'])
    };
}

function normalizeWorker(raw: unknown): NormalizedWorker {
    const data = isRecord(raw) ? raw : {};
    return {
        id: asString(data['id']),
        pools: asStringList(data['pools']),
        link: asEnum<WorkerLink>(data['link'], 'orphaned'),
        engine: asEnum<EngineState>(data['engine'], 'loading'),
        epoch: asNumber(data['epoch']),
        capacitySlots: asNumber(data['capacity_slots']),
        freeSlots: asNumber(data['free_slots']),
        freeKvTokens: asNullableNumber(data['free_kv_tokens']),
        inFlight: asStringList(data['in_flight']),
        outstandingClaims: asNumber(data['outstanding_claims']),
        lastSeenMs: asNullableNumber(data['last_seen_ms']),
        // Telemetry stays `unknown` on purpose: any JSON is valid here and
        // the Suppliers table renders it with JSON.stringify in a <pre>.
        telemetry: data['telemetry'] ?? null
    };
}

function normalizeLease(raw: unknown): NormalizedLease | null {
    // `lease` is explicitly nullable while a job is queued.
    if (!isRecord(raw)) return null;
    return {
        worker: asString(raw['worker']),
        epoch: asNumber(raw['epoch']),
        attempt: asNumber(raw['attempt']),
        deadlineMs: asNullableNumber(raw['deadline_ms'])
    };
}

function normalizeJob(raw: unknown): NormalizedJob {
    const data = isRecord(raw) ? raw : {};
    return {
        id: asString(data['id']),
        pool: asString(data['pool']),
        tenant: asString(data['tenant']),
        phase: asString(data['phase']),
        createdMs: asNullableNumber(data['created_ms']),
        consumers: asNumber(data['consumers']),
        lease: normalizeLease(data['lease'])
    };
}

function normalizeConsumerConnection(raw: unknown): NormalizedConsumerConnection {
    const data = isRecord(raw) ? raw : {};
    return {
        id: asString(data['id']),
        tenant: asString(data['tenant']),
        sinceMs: asNullableNumber(data['since_ms']),
        attachedJobs: asStringList(data['attached_jobs'])
    };
}

// ── Entry point ───────────────────────────────────────────────────────────

// Thrown for payloads that are not an observe document at all (HTML error
// pages, proxies, a non-hub origin answering on the port).
export class ObserveParseError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'ObserveParseError';
    }
}

// Turn the raw parsed JSON into a NormalizedSnapshot. Only the top-level
// "is this even an object" check is fatal — everything else degrades.
export function normalizeObserveSnapshot(payload: unknown, nowMs: number = Date.now()): NormalizedSnapshot {
    if (!isRecord(payload)) {
        throw new ObserveParseError(
            'observe response is not an object — is this URL the nexus hub root (e.g. ' +
                `${NEXUS_DEFAULT_BASE_URL}), not a proxy or an old hub without ${NEXUS_OBSERVE_PATH}?`
        );
    }
    return {
        fetchedAtMs: nowMs,
        protocol: asString(payload['protocol']),
        serverNowMs: asNumber(payload['now_ms']),
        readiness: normalizeReadiness(payload['readiness']),
        pools: asArray(payload['pools']).map(normalizePool),
        workers: asArray(payload['workers']).map(normalizeWorker),
        jobs: asArray(payload['jobs']).map(normalizeJob),
        consumerConnections: asArray(payload['consumer_connections']).map(normalizeConsumerConnection),
        unresponsivePools: asStringList(payload['unresponsive_pools'])
    };
}

// Protocol check kept separate from normalize so the parse never fails on a
// version bump that still produces a compatible document.
export function checkObserveProtocol(snapshot: NormalizedSnapshot): string | undefined {
    if (snapshot.protocol === NEXUS_OBSERVE_PROTOCOL) return undefined;
    return (
        `Hub reports protocol "${snapshot.protocol || '(none)'}" — this monitor speaks ` +
        `${NEXUS_OBSERVE_PROTOCOL}. Numbers below are whatever the hub sent; treat unknown fields with care.`
    );
}

// ── Fetch layer ───────────────────────────────────────────────────────────

// Absolute endpoint URL for a user-pasted base (trailing slashes removed).
export function observeUrl(base: string): string {
    return `${trimTrailingSlashes(base)}${NEXUS_OBSERVE_PATH}`;
}

export function metricsUrl(base: string): string {
    return `${trimTrailingSlashes(base)}${NEXUS_METRICS_PATH}`;
}

// HTTP failure with an operator-actionable message (kept separate from
// ObserveParseError so tests/UI can distinguish transport vs. shape issues).
export class ObserveHttpError extends Error {
    readonly status: number;
    constructor(status: number, message: string) {
        super(message);
        this.name = 'ObserveHttpError';
        this.status = status;
    }
}

// Map transport-level failures to hints. A 404 on /v1/observe almost always
// means "older hub" or "wrong path"; 401/403 never happen on the open hub
// endpoints but a reverse proxy in front of it can add them.
function httpError(status: number, path: string): ObserveHttpError {
    const hints: Record<number, string> = {
        404: `no ${path} on that origin — an older nexus build without observe support, or the URL points somewhere else?`,
        401: `${path} is not open — front the hub directly, or a proxy is demanding auth`,
        403: `${path} rejected the browser origin — CORS/allow-list on the hub or an intermediate proxy`,
        502: 'bad gateway — is the hub actually running behind that address?',
        503: 'hub not ready yet — keep the page open, polling continues'
    };
    const hint = hints[status];
    return new ObserveHttpError(status, `HTTP ${status}${hint ? ` — ${hint}` : ` — ${path} failed`}`);
}

// One abortable observe poll: fetch → JSON (guarded) → normalize.
// `signal` lets the polling hook cancel a stale in-flight request on
// disconnect/pause; aborts reject with the DOMException 'AbortError', which
// the hook treats as "not an error".
export async function fetchObserve(base: string, signal?: AbortSignal): Promise<ObserveResult> {
    let response: Response;
    try {
        response = await fetch(observeUrl(base), { signal });
    } catch (cause) {
        // fetch() itself rejects on both network failure and CORS block; the
        // browser cannot tell them apart, so the message lists both.
        if ((cause as Error | undefined)?.name === 'AbortError') throw cause;
        throw new ObserveHttpError(
            0,
            `Cannot reach ${observeUrl(base)} — network, or CORS blocked the browser origin. ` +
                'The nexus hub serves CORS for browser origins; an https page also cannot reach an http hub. ' +
                `(fetch: ${(cause as Error | undefined)?.message ?? 'unknown error'})`
        );
    }
    if (!response.ok) throw httpError(response.status, NEXUS_OBSERVE_PATH);
    let payload: unknown;
    try {
        payload = await response.json();
    } catch {
        // A hub returning HTML (dev proxy, typo'd host landing page) fails
        // here with "Unexpected token '<'" — translate it into a diagnosis.
        throw new ObserveParseError(
            `${observeUrl(base)} did not return JSON — double-check the hub URL ` +
                '(an HTML error page or a non-nexus server on that port looks exactly like this)'
        );
    }
    const snapshot = normalizeObserveSnapshot(payload);
    return { snapshot, protocolWarning: checkObserveProtocol(snapshot) };
}

// Raw Prometheus text exposition (parsed client-side by prometheus.ts).
export async function fetchMetricsText(base: string, signal?: AbortSignal): Promise<string> {
    let response: Response;
    try {
        response = await fetch(metricsUrl(base), { signal });
    } catch (cause) {
        if ((cause as Error | undefined)?.name === 'AbortError') throw cause;
        throw new ObserveHttpError(0, `Cannot reach ${metricsUrl(base)} — network or CORS`);
    }
    if (!response.ok) throw httpError(response.status, NEXUS_METRICS_PATH);
    return await response.text();
}
