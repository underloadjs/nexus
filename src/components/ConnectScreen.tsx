// The connect screen shown before a hub is attached (and after disconnect).
//
// This is the whole onboarding story of a static Pages bundle: paste the hub
// URL, press connect. Everything it must get right lives here — URL
// validation, the mixed-content trap, and honest requirements text so a
// failed connect is diagnosable without reading the console.
import { useState, type FormEvent } from 'react';
import {
    DEFAULT_APP_PATH,
    DEFAULT_HUB_URL,
    isMixedContentBlocked,
    getPageProtocol,
    normalizeUserUrl
} from '../utils/hub-url';

export interface ConnectScreenProps {
    // Value seeded into the input: the last URL used (localStorage) or ''.
    initialUrl: string;
    // Called with an already-normalized absolute URL (no trailing slash).
    onConnect: (url: string) => void;
}

export function ConnectScreen({ initialUrl, onConnect }: ConnectScreenProps) {
    const [draft, setDraft] = useState(initialUrl);
    const [error, setError] = useState<string | null>(null);

    // Live mixed-content preview: an https page + an http hub on a non-
    // loopback host will fail the fetch in every modern browser, so warn
    // BEFORE the user presses connect (and keep warning after).
    const normalizedDraft = normalizeUserUrl(draft);
    const mixedBlocked = normalizedDraft.ok && isMixedContentBlocked(normalizedDraft.url, getPageProtocol());

    const submit = (event: FormEvent<HTMLFormElement>) => {
        event.preventDefault();
        const result = normalizeUserUrl(draft);
        if (!result.ok) {
            setError(result.error);
            return;
        }
        setError(null);
        onConnect(result.url);
    };

    return (
        <main className="connect-screen">
            <form className="connect-card" onSubmit={submit} noValidate>
                <h1>Nexus Monitor</h1>
                <p className="connect-lede">
                    Read-only view of a <strong>nexus-hub</strong>: pools, supplier workers, consumer connections, job
                    leases and Prometheus counters. This page is static — it talks to the hub straight from your browser,
                    so nothing is proxied and nothing is stored server-side.
                </p>

                <label className="field-label" htmlFor="hub-url">
                    Hub URL
                </label>
                <input
                    id="hub-url"
                    className="url-input"
                    type="text"
                    inputMode="url"
                    autoComplete="off"
                    spellCheck={false}
                    placeholder={DEFAULT_HUB_URL}
                    value={draft}
                    onChange={(event) => setDraft(event.target.value)}
                />
                {/* Both deployment forms sit next to the field. The sandbox
                    form must show the /app/nexus/ mount: a bare preview origin
                    is the Daytona control plane (404 for /v1/observe), and
                    this page never appends the mount (see DEFAULT_APP_PATH in
                    utils/hub-url.ts). The https:// hint matters because a
                    scheme-less entry is treated as http:// and then trips the
                    mixed-content banner on an https page. */}
                <p className="connect-lede">
                    Examples — local hub: <code>{DEFAULT_HUB_URL}</code>; Daytona sandbox:{' '}
                    <code>{`https://7777-<sandbox-id>.daytonaproxy01.net${DEFAULT_APP_PATH}`}</code> (paste the{' '}
                    <code>https://</code> form — a scheme-less entry is treated as <code>http://</code>).
                </p>
                {/* Validation copy is user-facing (not a stack trace) because
                    the most common mistake is pasting the wrong root. */}
                {error ? <p className="field-error">{error}</p> : null}
                {mixedBlocked ? (
                    <p className="field-warn">
                        This page is https and that hub is http: the browser will refuse the request. Front the hub with
                        https (reverse proxy / tunnel), or run this UI locally with <code>yarn dev</code>.
                    </p>
                ) : null}

                <button className="btn btn-primary" type="submit">
                    Connect
                </button>

                <ul className="connect-notes">
                    <li>
                        Local hub: listens on <code>{DEFAULT_HUB_URL}</code> by default (env <code>NEXUS_LISTEN</code>)
                        and serves <code>GET /v1/observe</code> + <code>GET /metrics</code> for browser origins
                        (CORS).
                    </li>
                    <li>
                        Daytona sandbox (v2): the browser base is the runner's public preview origin plus the app
                        mount, e.g. <code>{`https://7777-<sandbox-id>.daytonaproxy01.net${DEFAULT_APP_PATH}`}</code>.
                        The monitor then requests <code>/app/nexus/v1/observe</code> and <code>/app/nexus/metrics</code>.
                    </li>
                    <li>
                        Inside the sandbox the hub itself still only binds{' '}
                        <code>{`NEXUS_LISTEN=127.0.0.1:<internal-port>`}</code> — that loopback address is internal
                        and not publicly previewed, so never paste it into this field from outside the sandbox. Use
                        the mounted public base above.
                    </li>
                    <li>
                        A bare preview origin is the sandbox <em>control plane</em>, not the hub:{' '}
                        <code>/v1/observe</code> at its root is a control 404, and this page does not guess or append{' '}
                        <code>{DEFAULT_APP_PATH}</code> to what you paste.
                    </li>
                    <li>The hub must be a build with observe support — an older hub answers 404 and this page says so.</li>
                    <li>The last URL you connected to is remembered in this browser.</li>
                </ul>
            </form>
        </main>
    );
}
