// Unit tests for the observe client: the defensive normalizer is the piece
// that decides whether a newer/older hub crashes the dashboard or not, so it
// is pinned against the exact nexus-hub-observe/v1 contract.
//
// Node environment (no jsdom): normalizeObserveSnapshot / checkObserveProtocol
// are pure, and the fetch wrappers are exercised with a stubbed global fetch
// (undici `Response` exists in Node 18+, which vitest runs on).
import { afterEach, describe, expect, it } from 'vitest';
import {
    ObserveHttpError,
    ObserveParseError,
    checkObserveProtocol,
    fetchMetricsText,
    fetchObserve,
    metricsUrl,
    normalizeObserveSnapshot,
    observeUrl
} from './nexus-client';

// Fixed clock: every age-derived assertion must be reproducible.
const NOW = 1_762_760_000_000;

// The contract sample, PLUS unknown fields at every nesting level — those
// must be dropped rather than forwarded to the renderer.
const CONTRACT_PAYLOAD = {
    protocol: 'nexus-hub-observe/v1',
    now_ms: NOW,
    readiness: {
        ready_workers: 2,
        loading_workers: 1,
        queue_depth: 3,
        mailbox_pending: 4,
        warm_models: ['qwen38'],
        capacity_signal: 'cold_start',
        future_readiness: true
    },
    pools: [
        {
            id: 'llm.chat/qwen38',
            queue_depth: 3,
            ready_workers: 2,
            loading_workers: 1,
            connected_workers: 3,
            leases: 1,
            mailbox_pending: 4,
            future_pool: 'ignored'
        }
    ],
    workers: [
        {
            id: 'w-1',
            pools: ['llm.chat/qwen38'],
            link: 'live',
            engine: 'ready',
            epoch: 1,
            capacity_slots: 4,
            free_slots: 2,
            free_kv_tokens: 8192,
            in_flight: ['job-1'],
            outstanding_claims: 0,
            last_seen_ms: NOW - 1000,
            telemetry: { gpu: 'cuda0' },
            future_worker: 1
        }
    ],
    jobs: [
        {
            id: 'job-1',
            pool: 'llm.chat/qwen38',
            tenant: 'default',
            phase: 'running',
            created_ms: NOW - 10_000,
            consumers: 2,
            lease: { worker: 'w-1', epoch: 1, attempt: 2, deadline_ms: NOW + 5000 },
            future_job: []
        }
    ],
    consumer_connections: [{ id: 'c-1', tenant: 'default', since_ms: NOW - 20_000, attached_jobs: ['job-1'], future_conn: {} }],
    unresponsive_pools: ['llm.embed/small'],
    future_top_level: 'ignored'
};

// What the renderer is allowed to see: camelCase, every key present, and
// nothing the hub invented.
const CONTRACT_SNAPSHOT = {
    fetchedAtMs: NOW,
    protocol: 'nexus-hub-observe/v1',
    serverNowMs: NOW,
    readiness: {
        readyWorkers: 2,
        loadingWorkers: 1,
        queueDepth: 3,
        mailboxPending: 4,
        warmModels: ['qwen38'],
        capacitySignal: 'cold_start'
    },
    pools: [
        {
            id: 'llm.chat/qwen38',
            queueDepth: 3,
            readyWorkers: 2,
            loadingWorkers: 1,
            connectedWorkers: 3,
            leases: 1,
            mailboxPending: 4
        }
    ],
    workers: [
        {
            id: 'w-1',
            pools: ['llm.chat/qwen38'],
            link: 'live',
            engine: 'ready',
            epoch: 1,
            capacitySlots: 4,
            freeSlots: 2,
            freeKvTokens: 8192,
            inFlight: ['job-1'],
            outstandingClaims: 0,
            lastSeenMs: NOW - 1000,
            telemetry: { gpu: 'cuda0' }
        }
    ],
    jobs: [
        {
            id: 'job-1',
            pool: 'llm.chat/qwen38',
            tenant: 'default',
            phase: 'running',
            createdMs: NOW - 10_000,
            consumers: 2,
            lease: { worker: 'w-1', epoch: 1, attempt: 2, deadlineMs: NOW + 5000 }
        }
    ],
    consumerConnections: [{ id: 'c-1', tenant: 'default', sinceMs: NOW - 20_000, attachedJobs: ['job-1'] }],
    unresponsivePools: ['llm.embed/small']
};

describe('normalizeObserveSnapshot', () => {
    it('maps the v1 contract exactly and drops unknown fields', () => {
        // toEqual (not toMatchObject): a leaked `future_*` key would fail.
        expect(normalizeObserveSnapshot(CONTRACT_PAYLOAD, NOW)).toEqual(CONTRACT_SNAPSHOT);
    });

    it('fills every missing section with neutral defaults', () => {
        expect(normalizeObserveSnapshot({}, NOW)).toEqual({
            fetchedAtMs: NOW,
            protocol: '',
            serverNowMs: 0,
            readiness: { readyWorkers: 0, loadingWorkers: 0, queueDepth: 0, mailboxPending: 0, warmModels: [], capacitySignal: 'warm' },
            pools: [],
            workers: [],
            jobs: [],
            consumerConnections: [],
            unresponsivePools: []
        });
    });

    it('tolerates null sections and non-array lists', () => {
        // A hub may send explicit nulls (serde defaults) — same neutral
        // result as omitting the keys entirely.
        const snapshot = normalizeObserveSnapshot({ readiness: null, pools: null, workers: 'nope', unresponsive_pools: null }, NOW);
        expect(snapshot.readiness.readyWorkers).toBe(0);
        expect(snapshot.pools).toEqual([]);
        expect(snapshot.workers).toEqual([]);
        expect(snapshot.unresponsivePools).toEqual([]);
    });

    it('nulls missing optional timestamps instead of inventing an age', () => {
        const [worker] = normalizeObserveSnapshot({ workers: [{ id: 'w-1' }] }, NOW).workers;
        expect(worker.lastSeenMs).toBeNull();
        expect(worker.telemetry).toBeNull();
        expect(worker.pools).toEqual([]);
        expect(worker.inFlight).toEqual([]);

        const [job] = normalizeObserveSnapshot({ jobs: [{ id: 'job-1' }] }, NOW).jobs;
        expect(job.createdMs).toBeNull();
        expect(job.lease).toBeNull();
        expect(job.consumers).toBe(0);

        const [connection] = normalizeObserveSnapshot({ consumer_connections: [{ id: 'c-1' }] }, NOW).consumerConnections;
        expect(connection.sinceMs).toBeNull();
        expect(connection.tenant).toBe('');
    });

    it('keeps unknown enum members visible instead of rewriting them', () => {
        // A hub that adds link="draining" must still say "draining" in the
        // UI; silently coercing it to a known state would lie.
        const snapshot = normalizeObserveSnapshot(
            { workers: [{ id: 'w-1', link: 'draining', engine: 'restarting' }], readiness: { capacity_signal: 'throttled' } },
            NOW
        );
        expect(snapshot.workers[0]?.link).toBe('draining');
        expect(snapshot.workers[0]?.engine).toBe('restarting');
        expect(snapshot.readiness.capacitySignal).toBe('throttled');
    });

    it('drops non-string entries from id lists', () => {
        // Defensive: a half-migrated hub could emit numbers here.
        const snapshot = normalizeObserveSnapshot({ workers: [{ id: 'w-1', pools: ['ok', 7, null, 'also-ok'] }] }, NOW);
        expect(snapshot.workers[0]?.pools).toEqual(['ok', 'also-ok']);
    });

    it('treats a nullable lease that is not an object as no lease', () => {
        const snapshot = normalizeObserveSnapshot({ jobs: [{ id: 'job-1', lease: 'none' }] }, NOW);
        expect(snapshot.jobs[0]?.lease).toBeNull();
    });

    it('rejects payloads that are not an observe document', () => {
        // The exact operator-facing message is part of the contract (it is
        // the only diagnosis the UI can offer for a wrong URL).
        const expected =
            'observe response is not an object — is this URL the nexus hub root (e.g. http://127.0.0.1:8085), ' +
            'not a proxy or an old hub without /v1/observe?';
        for (const payload of ['boom', [], null, undefined, 42]) {
            let caught: unknown;
            try {
                normalizeObserveSnapshot(payload, NOW);
            } catch (error) {
                caught = error;
            }
            expect(caught).toBeInstanceOf(ObserveParseError);
            expect((caught as Error).message).toBe(expected);
        }
    });
});

describe('checkObserveProtocol', () => {
    it('stays silent for the supported protocol', () => {
        expect(checkObserveProtocol(normalizeObserveSnapshot(CONTRACT_PAYLOAD, NOW))).toBeUndefined();
    });

    it('names both protocols on a mismatch', () => {
        const snapshot = normalizeObserveSnapshot({ protocol: 'nexus-hub-observe/v2' }, NOW);
        expect(checkObserveProtocol(snapshot)).toBe(
            'Hub reports protocol "nexus-hub-observe/v2" — this monitor speaks nexus-hub-observe/v1. ' +
                'Numbers below are whatever the hub sent; treat unknown fields with care.'
        );
    });

    it('says so when the hub reports no protocol at all', () => {
        expect(checkObserveProtocol(normalizeObserveSnapshot({ now_ms: NOW }, NOW))).toBe(
            'Hub reports protocol "(none)" — this monitor speaks nexus-hub-observe/v1. ' +
                'Numbers below are whatever the hub sent; treat unknown fields with care.'
        );
    });
});

describe('url builders', () => {
    it('appends the endpoint paths and trims trailing slashes', () => {
        expect(observeUrl('http://127.0.0.1:8085')).toBe('http://127.0.0.1:8085/v1/observe');
        expect(observeUrl('http://127.0.0.1:8085/')).toBe('http://127.0.0.1:8085/v1/observe');
        // Repeated slashes are what the browser address bar produces.
        expect(observeUrl('https://hub.example/nexus///')).toBe('https://hub.example/nexus/v1/observe');
        expect(metricsUrl('http://127.0.0.1:8085//')).toBe('http://127.0.0.1:8085/metrics');
    });
});

// Transport tests: replace global fetch with a stub. The point is the exact
// error copy the operator sees, and that the abort signal is forwarded.
const realFetch = globalThis.fetch;

afterEach(() => {
    globalThis.fetch = realFetch;
});

describe('fetchObserve', () => {
    it('requests /v1/observe with the caller signal and normalizes the body', async () => {
        let requested = '';
        let sawSignal = false;
        globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
            requested = String(input);
            sawSignal = init?.signal instanceof AbortSignal;
            return new Response(JSON.stringify(CONTRACT_PAYLOAD), {
                status: 200,
                headers: { 'content-type': 'application/json' }
            });
        }) as unknown as typeof fetch;

        const controller = new AbortController();
        const result = await fetchObserve('http://127.0.0.1:8085/', controller.signal);

        expect(requested).toBe('http://127.0.0.1:8085/v1/observe');
        expect(sawSignal).toBe(true);
        expect(result.protocolWarning).toBeUndefined();
        // fetchedAtMs is Date.now() here, so it is excluded from the exact
        // comparison rather than faked with a range check.
        const { fetchedAtMs, ...snapshot } = result.snapshot;
        expect(typeof fetchedAtMs).toBe('number');
        expect(snapshot).toEqual({ ...CONTRACT_SNAPSHOT, fetchedAtMs: undefined });
    });

    it('turns a 404 into the "older hub" diagnosis', async () => {
        globalThis.fetch = (async () => new Response('not found', { status: 404 })) as unknown as typeof fetch;
        await expect(fetchObserve('http://127.0.0.1:8085')).rejects.toThrowError(
            new ObserveHttpError(
                404,
                'HTTP 404 — no /v1/observe on that origin — an older nexus build without observe support, ' +
                    'or the URL points somewhere else?'
            )
        );
    });

    it('explains an HTML answer instead of a JSON syntax error', async () => {
        globalThis.fetch = (async () => new Response('<html>gateway</html>', { status: 200 })) as unknown as typeof fetch;
        await expect(fetchObserve('http://127.0.0.1:8085')).rejects.toThrowError(
            'http://127.0.0.1:8085/v1/observe did not return JSON — double-check the hub URL ' +
                '(an HTML error page or a non-nexus server on that port looks exactly like this)'
        );
    });

    it('names network and CORS as the two causes of a fetch rejection', async () => {
        globalThis.fetch = (async () => {
            throw new TypeError('Failed to fetch');
        }) as unknown as typeof fetch;
        await expect(fetchObserve('http://127.0.0.1:8085')).rejects.toThrowError(
            'Cannot reach http://127.0.0.1:8085/v1/observe — network, or CORS blocked the browser origin. ' +
                'The nexus hub serves CORS for browser origins; an https page also cannot reach an http hub. ' +
                '(fetch: Failed to fetch)'
        );
    });

    it('re-throws aborts unchanged so the poller can ignore them', async () => {
        globalThis.fetch = (async () => {
            const error = new Error('The operation was aborted.');
            error.name = 'AbortError';
            throw error;
        }) as unknown as typeof fetch;
        const error = await fetchObserve('http://127.0.0.1:8085').catch((caught: unknown) => caught);
        expect((error as Error).name).toBe('AbortError');
    });
});

describe('fetchMetricsText', () => {
    it('returns the exposition body verbatim', async () => {
        globalThis.fetch = (async () => new Response('# TYPE x gauge\nx 1\n', { status: 200 })) as unknown as typeof fetch;
        expect(await fetchMetricsText('http://127.0.0.1:8085')).toBe('# TYPE x gauge\nx 1\n');
    });

    it('maps a 503 to the "hub not ready" hint', async () => {
        globalThis.fetch = (async () => new Response('nope', { status: 503 })) as unknown as typeof fetch;
        await expect(fetchMetricsText('http://127.0.0.1:8085')).rejects.toThrowError(
            'HTTP 503 — hub not ready yet — keep the page open, polling continues'
        );
    });
});
