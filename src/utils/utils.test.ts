// Unit tests for the pure helpers: URL validation/persistence, the
// mixed-content rule, and the relative-time formatting that every panel
// renders. All node-environment (no jsdom), so the storage helpers are
// exercised in their "no window" form.
import { describe, expect, it } from 'vitest';
import { formatAge, formatCounter, formatNumber, shortId } from './format';
import {
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
