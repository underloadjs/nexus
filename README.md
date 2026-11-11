# @underload/nexus — Nexus Monitor

Read-only observability console for a running **nexus-hub**. It is a static
Vite + React app: no backend, no account, no websocket. Open the page, paste the
hub URL, and the browser talks to the hub itself — this package holds no server
code and never mutates one.

The hub is the Rust (axum + tokio) broker that used to live in this package. It
is no longer developed here: it moved to **[`@services/nexus`](../../../services/nexus)**
(`services/nexus` in the noobscript monorepo), which owns the two open APIs this
UI is built entirely on:

| API | What the monitor takes from it |
| --- | --- |
| `GET /v1/observe` | the JSON snapshot of live hub state (protocol `nexus-hub-observe/v1`): readiness, pools, workers, active jobs and leases, consumer connections, unresponsive pools |
| `GET /metrics` | the Prometheus exposition: latency summaries and every counter/gauge family, parsed in the browser |

Both are implemented and tested on the server side in
`services/nexus/crates/nexus-server/src/observe.rs`, with the CORS policy in
`services/nexus/crates/nexus-server/src/main.rs` (`observe_cors`) — the only
routes on the hub that depend on CORS, because this page is served from a
different origin.

## What it shows

| Panel | Source | Content |
| --- | --- | --- |
| Overview | `/v1/observe` + `/metrics` | readiness (capacity signal, ready/loading workers, queue depth, mailbox pending, warm models), active jobs split by leased/queued, consumer connections, unresponsive pools, snapshot age (with the hub's own clock), and the cumulative accepted/rejected/terminal job counters |
| Pools | `/v1/observe` | one card per pool: queue depth next to ready/loading/connected workers, leases and mailbox pending; pools the hub flags as unresponsive are badged, and flagged ids with no stats entry are listed under the cards |
| Jobs (active) | `/v1/observe` | every active job: pool, tenant, phase, consumer count, the worker holding the lease, lease epoch/attempt, remaining deadline, age |
| Connections | `/v1/observe` | three-column SVG graph: consumers → jobs → suppliers. Edges come from `consumer_connections[].attached_jobs` and `jobs[].lease.worker`; live vs. orphaned suppliers and leased vs. queued jobs are drawn differently |
| Suppliers (workers) | `/v1/observe` | per worker: pools, link, engine, epoch, free/capacity slots, free KV tokens, in-flight jobs, outstanding claims, last-seen age, and expandable raw telemetry JSON |
| Consumers | `/v1/observe` | live consumer connections (tenant, attached jobs, connected-for) plus the per-job consumer counts the scheduler reports |
| Metrics | `/metrics` | Prometheus text parsed in the browser: latency averages from the `_sum`/`_count` summary pairs (`nexus_dispatch_latency_ms`, `nexus_ttft_ms`, `nexus_job_duration_ms`), then every series with its labels |

The page never writes to the hub — there is no button that can mutate it.

## Run it locally

```bash
cd packages/underload/nexus
yarn install    # writes/uses yarn.lock + node_modules
yarn dev        # http://localhost:8300
```

`yarn test` (vitest, node environment), `yarn typecheck` (`tsc --noEmit`),
`yarn build` (into `dist/`), `yarn preview` (serve the built `dist/`).

**Two install contexts, one manifest.** Inside the noobscript monorepo this
folder is a workspace member (`packages/underload/*` in the root `package.json`),
so a root `yarn install` resolves and hoists these dependencies into the root
`node_modules`, and `yarn workspace @underload/nexus dev` from the repo root
works. Checked out on its own — the `underloadjs/nexus` repo, or the GitHub Pages
CI box — the same manifest is a standalone Yarn 1 project: the repo root has no
`workspaces` field, so `yarn install` resolves into this directory's own
`yarn.lock` + `node_modules`. That lockfile is committed and CI installs with
`--frozen-lockfile`, so whenever `package.json` changes, run `yarn install` here
and commit the refreshed lockfile with it.

One consequence worth knowing before you touch `tsconfig.json`: `@types/node` is
a devDependency here, not decoration. `tsconfig.json` lists `"node"` in
`compilerOptions.types`, and `tsc` resolves type libraries through every ancestor
`node_modules/@types`. Inside the monorepo a missing `@types/node` hides itself by
resolving into the hoisted root install; in a standalone checkout (CI, or a plain
`git clone` of this repo) it fails with
`TS2688: Cannot find type definition file for 'node'`.

## Connect to a hub

1. Start the hub (default listen address `http://127.0.0.1:8085`, overridable
   with `NEXUS_LISTEN`) from the server package:
   ```bash
   # from the noobscript monorepo root
   yarn workspace @services/nexus start    # cargo run -p nexus-server --bin nexus-hub
   ```
2. Paste that URL into the monitor. A bare `host:port` is accepted and treated
   as `http://`; trailing slashes are trimmed. The last URL is remembered in
   your browser (localStorage).
3. Polling starts immediately (1 s interval, exponential backoff while the hub
   is unreachable). **Pause** freezes the current snapshot, **Refresh now**
   forces one poll, **Disconnect** returns to the connect screen.

### Requirements on the hub side

- The hub must be a build that serves `GET /v1/observe` (protocol
  `nexus-hub-observe/v1`) and `GET /metrics`. An older hub answers **404**, and
  the monitor says so instead of showing empty panels.
- Both endpoints are open (no auth) and must carry CORS headers for the origin
  this page is served from; `@services/nexus` does that. A CORS failure and a
  network failure look identical to the browser, so the error banner lists both.
- Unknown fields in the payload are ignored and missing optional fields are
  treated as unknown (`—`), so a newer hub keeps rendering; a payload whose
  `protocol` is not the one this UI speaks shows a banner rather than failing.
- Mixed content: an **https** page cannot poll an **http** hub on a non-localhost
  host — browsers block it. The monitor shows a permanent banner when you try.
  Either front the hub with https (reverse proxy / tunnel), or run this UI
  locally with `yarn dev` (http page → http hub is allowed).

## Deployment

`.github/workflows/deploy-pages.yml` — **in this repository**, not in the
noobscript monorepo — builds this directory and publishes it to this repo's
GitHub Pages site on pushes to `main` that touch the app (plus manual
dispatch).

It lives here because `packages/underload/nexus` is its own git repository
(`underloadjs/nexus`): inside the monorepo it is an embedded repo, so a
root-level workflow never sees these files and could not upload an artifact
built from them. Every other standalone mirror in the monorepo ships its own
workflow for the same reason (`packages/underload/bench/.github/workflows/deploy-pages.yml`,
`packages/underload/dashboard`, `packages/underload/harness`, `distribution/*`).
All paths in the workflow are relative to this repo — the site source is the
repository root (`index.html`, `src/`, `vite.config.ts`), and the artifact is
`dist/`.

What it does, in order:

1. `actions/checkout@v5`, `actions/setup-node@v5` (Node 24), then
   `npm install --global yarn@1.22.22` — pinned Yarn 1 instead of corepack or
   npm; the reasons (npm 11 arborist crash on this dependency tree, corepack
   deprecation) are written out in the workflow header.
2. `yarn install --frozen-lockfile` — the standalone install described under
   [Run it locally](#run-it-locally), so CI and local development resolve the
   identical tree from `yarn.lock`.
3. `yarn typecheck` and `yarn test` — the site is gated before it is published.
4. `yarn build --base=/${{ github.event.repository.name }}/` — Pages serves the
   site under `https://<owner>.github.io/<repo-name>/`, i.e. `/nexus/`, so Vite
   has to rewrite the asset URLs to that subpath. The repo name comes from the
   event payload so a rename or fork keeps working; `vite.config.ts`
   deliberately carries no `base`.
5. `actions/configure-pages@v5` → `actions/upload-pages-artifact@v3`
   (`path: dist`, in the build job that produced it) →
   `actions/deploy-pages@v4` in the deploy job. That artifact flow replaces the
   old `gh-pages` branch push, which silently kept serving a stale bundle after
   Pages' "Deploy from a branch" stopped triggering — see the header of
   `distribution/story-generator/.github/workflows/deploy.yml`.

One-time repository setting (required once, not per deploy): **Settings →
Pages → Build and deployment → Source: "GitHub Actions"**.

To see the exact artifact locally, build and serve it with the Pages base path:

```bash
yarn build --base=/nexus/
yarn preview --base=/nexus/     # open http://localhost:4173/nexus/
```

The `--base` has to be repeated on `preview`: `vite.config.ts` has no `base`, so
a plain `yarn preview` serves that `dist/` from `/` and every `/nexus/...` asset
request falls through to `index.html`.

After deployment, open the Pages URL, paste the hub URL, and the browser talks
to the hub directly — the deployed bundle never proxies through GitHub. The
CORS and mixed-content requirements in
[Requirements on the hub side](#requirements-on-the-hub-side) apply to the
deployed page exactly as they do to `yarn dev`.

## Where the Rust server went

Until 2026-11 this package was the hub: `crates/nexus-core` (the actor, queue,
protocol, relay, store), `crates/nexus-server` (axum + tokio runtime, worker and
consumer sockets, SSE, artifacts, `/v1/observe`, `/metrics`), `crates/nexus-wasm`,
`crates/nexus-worker-example`, the wasm build scripts, the Daytona deployment
scripts and `documents/specifications/hub-specification.md`. All of that is now
**[`@services/nexus`](../../../services/nexus)**, and nothing here compiles Rust.
The observation contract this UI reads is `nexus-hub-observe/v1`, defined and
accepted-tested in that package (`crates/nexus-server/src/observe.rs`); change it
there and the monitor keeps rendering unknown fields, but a protocol-id change
shows the banner in `Banners.tsx` instead.
