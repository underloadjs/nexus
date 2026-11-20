// Hub-URL handling: what the user pastes, how it is validated/persisted,
// and when the browser will refuse to talk to it at all.
// Kept React-free so the node-environment unit tests can exercise it, and
// dependency-free (the api layer imports THIS module, never the reverse —
// a cycle here would TDZ-crash at module init depending on entry order).

// localStorage key for the hub URL. The `.v1` suffix is a cheap cache-bust:
// change it if the stored value's meaning ever changes.
export const STORAGE_KEY_URL = 'nexus-monitor.url.v1';

// The hub's default listen address (env NEXUS_LISTEN): shown as the input
// placeholder and in validation copy. `nexus-client.ts` re-exposes it as the
// contract constant next to the endpoint paths.
export const DEFAULT_HUB_URL = 'http://127.0.0.1:8085';

// v2 sandbox deployments (the noobscript monorepo's `services/daytona` runner)
// publish the hub on the runner's single public origin under `/app/<name>/`;
// for the nexus app that is
// `https://7777-<sandbox-id>.daytonaproxy01.net/app/nexus/`
// (`services/daytona/sandbox-specs.md` §4.1-4.2, R-8.6 for NEXUS_LISTEN).
//
// This is a MOUNT PATH, not something this monitor appends: a bare preview
// origin is the sandbox CONTROL PLANE, and `/v1/observe` at its root is a
// control 404 (R-4.4), so the operator must paste the mounted base. Exported
// so the ConnectScreen copy and the focused tests share one literal; the
// endpoint builders (api/nexus-client.ts) only trim trailing slashes, which is
// what preserves the mount exactly once.
export const DEFAULT_APP_PATH = '/app/nexus/';

// Trim repeated trailing slashes: users paste `http://127.0.0.1:8085/` from
// the address bar and every request builder appends `/v1/observe` directly.
export function trimTrailingSlashes(url: string): string {
    return url.replace(/\/+$/, '');
}

// Accept what users actually paste: a full URL, or a bare host[:port] which
// implicitly means http:// (the hub default is http://127.0.0.1:8085).
// Discriminated on `ok` so callers cannot accidentally read the wrong half
// of the result. Returns the normalized absolute URL (trailing slashes
// trimmed) or a human-readable validation error shown verbatim by the UI.
export type NormalizedUrl = { ok: true; url: string } | { ok: false; error: string };

export function normalizeUserUrl(raw: string): NormalizedUrl {
    const trimmed = raw.trim();
    if (trimmed.length === 0) return { ok: false, error: `Enter the hub URL, e.g. ${DEFAULT_HUB_URL}` };
    // A scheme prefix (anything matching `^[a-z][a-z0-9+.-]*://`) is kept as
    // typed; anything else is treated as host[:port] on http.
    const candidate = /^[a-zA-Z][a-zA-Z0-9+.-]*:\/\//.test(trimmed) ? trimmed : `http://${trimmed}`;
    let parsed: URL;
    try {
        parsed = new URL(candidate);
    } catch {
        return { ok: false, error: `"${trimmed}" is not a valid URL or host:port` };
    }
    if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
        return { ok: false, error: `Only http:// and https:// hubs are supported (got "${parsed.protocol}")` };
    }
    return { ok: true, url: trimTrailingSlashes(parsed.toString()) };
}

// localhost/loopback hostnames — http hubs on these are exempt from the
// mixed-content block because browsers treat them as potentially trusted.
const LOCAL_HOSTS = new Set(['localhost', '127.0.0.1', '0.0.0.0', '::1', '[::1]']);

// Persistent mixed-content warning: the GitHub Pages deployment is always
// https, and browsers hard-block https pages from fetching http resources —
// EXCEPT on localhost/loopback. When this fires the only fixes are an https
// front for the hub (reverse proxy / tunnel) or running this UI via dev.
// Unknown protocols (non-browser environments) are treated as safe.
export function isMixedContentBlocked(baseUrl: string, pageProtocol: string): boolean {
    if (pageProtocol !== 'https:') return false;
    try {
        const parsed = new URL(baseUrl);
        return parsed.protocol === 'http:' && !LOCAL_HOSTS.has(parsed.hostname);
    } catch {
        return false;
    }
}

// The page's own protocol, for the mixed-content check. SSR-safe (node
// renderToString smoke tests have no `window`, and "not a browser" is never
// a mixed-content situation).
export function getPageProtocol(): string {
    return typeof window === 'undefined' ? 'http:' : window.location.protocol;
}

// Safe localStorage access: private browsing and sandboxed embeds throw on
// both read and write, and a monitoring dashboard must never white-screen
// because storage is unavailable. Node (renderToString smoke tests) has
// neither `window` nor `localStorage`.
export function loadStoredUrl(): string | null {
    try {
        if (typeof window === 'undefined' || !window.localStorage) return null;
        const raw = window.localStorage.getItem(STORAGE_KEY_URL);
        return typeof raw === 'string' && raw.length > 0 ? raw : null;
    } catch {
        return null;
    }
}

export function saveStoredUrl(url: string): void {
    try {
        if (typeof window === 'undefined' || !window.localStorage) return;
        window.localStorage.setItem(STORAGE_KEY_URL, url);
    } catch {
        // ignore — the URL simply won't persist across reloads
    }
}
