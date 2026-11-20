// Unit tests for the pure helpers: URL validation/persistence, the
// mixed-content rule, and the relative-time formatting that every panel
// renders. All node-environment (no jsdom), so the storage helpers are
// exercised in their "no window" form.
import { describe, expect, it } from 'vitest';
import { formatAge, formatCounter, formatNumber, shortId } from './format';
import {
    DEFAULT_APP_PATH,
    DEFAULT_HUB_URL,
    STORAGE_KEY_URL,
    getPageProtocol,
    isMixedContentBlocked,
    loadStoredUrl,
    normalizeUserUrl,
    saveStoredUrl,
    trimTrailingSlashes
} from './hub-url';

const NOW = 1_762_760_000_000;

describe('normalizeUserUrl', () => {
    it('accepts a full URL unchanged (minus trailing slashes)', () => {
        expect(normalizeUserUrl('http://127.0.0.1:8085')).toEqual({ ok: true, url: 'http://127.0.0.1:8085' });
        expect(normalizeUserUrl('  https://hub.example/nexus/  ')).toEqual({ ok: true, url: 'https://hub.example/nexus' });
        expect(normalizeUserUrl('https://hub.example///')).toEqual({ ok: true, url: 'https://hub.example' });
    });

    it('treats a bare host[:port] as http, which is how the hub runs', () => {
        // The URL constructor adds the root slash; the trailing-slash trim
        // takes it back off so `/v1/observe` is appended cleanly.
        expect(normalizeUserUrl('localhost:8085')).toEqual({ ok: true, url: 'http://localhost:8085' });
        expect(normalizeUserUrl('192.168.0.10:8085')).toEqual({ ok: true, url: 'http://192.168.0.10:8085' });
    });

    it('rejects empty input, unparseable URLs and other schemes', () => {
        expect(normalizeUserUrl('')).toEqual({
            ok: false,
            error: `Enter the hub URL, e.g. ${DEFAULT_HUB_URL}`
        });
        expect(normalizeUserUrl('http://')).toEqual({ ok: false, error: '"http://" is not a valid URL or host:port' });
        expect(normalizeUserUrl('ftp://hub.example')).toEqual({
            ok: false,
            error: 'Only http:// and https:// hubs are supported (got "ftp:")'
        });
    });
});

describe('isMixedContentBlocked', () => {
    it('blocks an http hub from an https page, except on loopback hosts', () => {
        expect(isMixedContentBlocked('http://hub.example:8085', 'https:')).toBe(true);
        expect(isMixedContentBlocked('http://localhost:8085', 'https:')).toBe(false);
        expect(isMixedContentBlocked('http://127.0.0.1:8085', 'https:')).toBe(false);
        expect(isMixedContentBlocked('http://[::1]:8085', 'https:')).toBe(false);
    });

    it('never blocks when the page itself is http, or the hub is https', () => {
        expect(isMixedContentBlocked('http://hub.example', 'http:')).toBe(false);
        expect(isMixedContentBlocked('https://hub.example', 'https:')).toBe(false);
    });

    it('is false for garbage (nothing to block in a non-browser run)', () => {
        expect(isMixedContentBlocked('not a url', 'https:')).toBe(false);
        expect(getPageProtocol()).toBe('http:');
    });
});

describe('storage helpers', () => {
    it('degrade to null / no-ops without a window (node + private browsing)', () => {
        expect(loadStoredUrl()).toBeNull();
        expect(() => saveStoredUrl('http://x')).not.toThrow();
        // The key is part of the contract: bumping it invalidates old values.
        expect(STORAGE_KEY_URL).toBe('nexus-monitor.url.v1');
    });
});

describe('trimTrailingSlashes', () => {
    it('removes one or more trailing slashes and nothing else', () => {
        expect(trimTrailingSlashes('http://h:1//')).toBe('http://h:1');
        expect(trimTrailingSlashes('http://h:1/path/')).toBe('http://h:1/path');
        expect(trimTrailingSlashes('http://h:1')).toBe('http://h:1');
    });
});

// v2 sandbox deployment (`services/daytona/sandbox-specs.md` §4.1-4.2): one
// public preview origin for the runner, every app under `/app/<name>/`, and
// nexus under `/app/nexus/`. The monitor's only legal input is the MOUNTED
// base; the tests below pin both halves of that contract — the mount survives
// normalization, and a bare origin is never guessed into an app path.
describe('sandbox mount normalization', () => {
    const SANDBOX_ORIGIN = 'https://7777-abc123.daytonaproxy01.net';
    const SANDBOX_BASE = `${SANDBOX_ORIGIN}/app/nexus/`;

    it('keeps the /app/nexus/ mount and trims only the trailing slashes', () => {
        expect(normalizeUserUrl(SANDBOX_BASE)).toEqual({ ok: true, url: `${SANDBOX_ORIGIN}/app/nexus` });
        // R-4.12's 308 only matters for a bare app root; appending `/v1/observe`
        // to the no-slash form is already a valid app route, so both pastes
        // normalize to the same base.
        expect(normalizeUserUrl(`${SANDBOX_ORIGIN}/app/nexus`)).toEqual({ ok: true, url: `${SANDBOX_ORIGIN}/app/nexus` });
        // Address-bar paste: repeated slashes collapse to the mount, not away
        // from it.
        expect(normalizeUserUrl(`${SANDBOX_ORIGIN}/app/nexus///`)).toEqual({ ok: true, url: `${SANDBOX_ORIGIN}/app/nexus` });
        // Surrounding whitespace is ignored; the mount is not.
        expect(normalizeUserUrl(`  ${SANDBOX_BASE}  `)).toEqual({ ok: true, url: `${SANDBOX_ORIGIN}/app/nexus` });
    });

    it('treats a scheme-less sandbox host as http, which the UI warns about', () => {
        // No https inference for preview origins: the bare host/path becomes
        // http:// and the mixed-content rule then blocks it on an https page.
        // The connect screen therefore shows the `https://` form explicitly.
        expect(normalizeUserUrl('7777-abc123.daytonaproxy01.net/app/nexus/')).toEqual({
            ok: true,
            url: 'http://7777-abc123.daytonaproxy01.net/app/nexus'
        });
        expect(isMixedContentBlocked('http://7777-abc123.daytonaproxy01.net/app/nexus', 'https:')).toBe(true);
    });

    it('never turns a bare preview origin into an app path', () => {
        // A bare preview origin is the sandbox CONTROL PLANE, not the hub
        // (§4.2/§4.4). Guessing `/app/nexus/` would be wrong for every other
        // app name and would hide the operator's mistake, so the normalizer
        // leaves the input alone and the connect-screen copy explains it.
        expect(normalizeUserUrl(SANDBOX_ORIGIN)).toEqual({ ok: true, url: SANDBOX_ORIGIN });
        // The shared copy literal the ConnectScreen interpolates.
        expect(DEFAULT_APP_PATH).toBe('/app/nexus/');
    });
});

describe('formatAge', () => {
    it('renders the exact bucketed age strings', () => {
        expect(formatAge(NOW - 400, NOW)).toBe('just now');
        expect(formatAge(NOW - 3000, NOW)).toBe('3s ago');
        expect(formatAge(NOW - 59_000, NOW)).toBe('59s ago');
        expect(formatAge(NOW - 65_000, NOW)).toBe('1m 05s ago');
        expect(formatAge(NOW - 3_725_000, NOW)).toBe('1h 02m ago');
    });

    it('never invents an age for a missing or future timestamp', () => {
        expect(formatAge(null, NOW)).toBe('—');
        expect(formatAge(undefined, NOW)).toBe('—');
        expect(formatAge(Number.NaN, NOW)).toBe('—');
        // Browser clock behind the hub: clamp, do not show "-5s ago".
        expect(formatAge(NOW + 5000, NOW)).toBe('just now');
    });
});

describe('shortId / counters', () => {
    it('abbreviates long ids and leaves short ones alone', () => {
        expect(shortId('w-1')).toBe('w-1');
        // The threshold is prefix + suffix + 1 characters; at that length the
        // abbreviated form would not be shorter, so the id passes through.
        expect(shortId('0123456789')).toBe('0123456789');
        expect(shortId('01234567890')).toBe('012345…890');
        expect(shortId('w-9f3c11aa22bb33cc')).toBe('w-9f3c…3cc');
    });

    it('formats counter values without mangling non-finite ones', () => {
        expect(formatCounter(0)).toBe('0');
        expect(formatCounter(1234567)).toBe('1,234,567');
        expect(formatCounter(Number.NaN)).toBe('NaN');
        expect(formatCounter(Number.POSITIVE_INFINITY)).toBe('Infinity');
        expect(formatNumber(null)).toBe('—');
    });
});
