// The connect screen shown before a hub is attached (and after disconnect).
//
// This is the whole onboarding story of a static Pages bundle: paste the hub
// URL, press connect. Everything it must get right lives here — URL
// validation, the mixed-content trap, and honest requirements text so a
// failed connect is diagnosable without reading the console.
import { useState, type FormEvent } from 'react';
import { DEFAULT_HUB_URL, isMixedContentBlocked, getPageProtocol, normalizeUserUrl } from '../utils/hub-url';

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
                        The hub listens on <code>{DEFAULT_HUB_URL}</code> by default (env <code>NEXUS_LISTEN</code>) and
                        serves <code>GET /v1/observe</code> + <code>GET /metrics</code> for browser origins (CORS).
                    </li>
                    <li>The hub must be a build with observe support — an older hub answers 404 and this page says so.</li>
                    <li>The last URL you connected to is remembered in this browser.</li>
                </ul>
            </form>
        </main>
    );
}
