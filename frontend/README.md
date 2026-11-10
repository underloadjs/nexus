# Nexus Monitor

Read-only web dashboard for a running **nexus-hub** (the Rust server in
[`../crates`](../crates)). It is a static Vite + React app: no backend, no
account, no websocket. Open the page, paste the hub URL, and the browser polls
the hub directly.

It lives **outside** `crates/` on purpose — it is not part of the cargo package
and is not published to crates.io. It ships to GitHub Pages instead (see
[Deployment](#deployment)).

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
# everything happens in THIS directory — the package installs standalone
cd packages/underload/nexus/frontend
yarn install    # writes/uses frontend/yarn.lock + frontend/node_modules
yarn dev        # http://localhost:8300
```

`yarn test` (vitest, node environment), `yarn typecheck` (`tsc --noEmit`),
`yarn build` (into `dist/`), `yarn preview` (serve the built `dist/`).

**Why the install is per-directory, not per-monorepo.** This package is a
standalone Yarn 1 project even while it sits inside the noobscript monorepo:
the root `package.json` lists `"packages/underload/*"` as a workspace glob,
which matches `packages/underload/nexus` but **not**
`packages/underload/nexus/frontend` (one `*` = one path segment). Yarn 1 walks
up from the working directory to the nearest manifest that has a `workspaces`
field and, when the relative path matches none of its patterns, treats the
current folder as its own project — so `yarn install` in here resolves into
`frontend/yarn.lock` + `frontend/node_modules` instead of hoisting to the root,
and `yarn workspace @underload/nexus-frontend dev` from the monorepo root does
**not** work (the root does not know this package). Verified on this host:
`yarn workspaces info` run from `frontend/` answers
"Cannot find the root of your workspace", and a root `yarn install` never
resolves `@underload/nexus-frontend`.

That is also the CI context: the Pages workflow below checks out *this* repo,
whose root manifest has no `workspaces` field, and runs the same commands in
`frontend/`. So local development and CI install identically — one
`frontend/yarn.lock`, no hoisting on either side.

One consequence worth knowing before you touch `tsconfig.json`: `@types/node`
is a devDependency here, not decoration. `tsconfig.json` lists `"node"` in
`compilerOptions.types`, and `tsc` resolves type libraries through every
ancestor `node_modules/@types`. Inside the monorepo a missing `@types/node`
hides itself by resolving into the hoisted root install; in a standalone
checkout (CI, or a plain `git clone` of this repo) it fails with
`TS2688: Cannot find type definition file for 'node'`.

`frontend/yarn.lock` is committed, and CI installs with `--frozen-lockfile`:
whenever `package.json` changes, run `yarn install` here and commit the
refreshed lockfile with it.

## Connect to a hub

1. Start the hub (default listen address `http://127.0.0.1:8085`, overridable
   with `NEXUS_LISTEN`):
   ```bash
   cd ..                 # the nexus package root (crates/ + Cargo.toml live here)
   yarn start            # cargo run -p nexus-server --bin nexus-hub
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
  this page is served from; the hub does that. A CORS failure and a network
  failure look identical to the browser, so the error banner lists both.
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
GitHub Pages site on pushes to `main` that touch `frontend/**` (plus manual
dispatch).

It lives here because `packages/underload/nexus` is its own git repository
(`underloadjs/nexus`): inside the monorepo it is an embedded repo, so a
root-level workflow never sees these files and could not upload an artifact
built from them. Every other standalone mirror in the monorepo ships its own
workflow for the same reason (`packages/underload/bench/.github/workflows/deploy-pages.yml`,
`packages/underload/dashboard`, `packages/underload/harness`, `distribution/*`).
All paths in the workflow are relative to this repo — the site source is
`frontend/`.

What it does, in order:

1. `actions/checkout@v5`, `actions/setup-node@v5` (Node 24), then
   `npm install --global yarn@1.22.22` — pinned Yarn 1 instead of corepack or
   npm; the reasons (npm 11 arborist crash on this dependency tree, corepack
   deprecation) are written out in the workflow header.
2. `yarn install --frozen-lockfile` in `frontend/` — this is the same standalone
   install described under [Run it locally](#run-it-locally), so CI and local
   development resolve the identical tree from `frontend/yarn.lock`.
3. `yarn typecheck` and `yarn test` — the site is gated before it is published.
4. `yarn build --base=/${{ github.event.repository.name }}/` — Pages serves the
   site under `https://<owner>.github.io/<repo-name>/`, i.e. `/nexus/`, so Vite
   has to rewrite the asset URLs to that subpath. The repo name comes from the
   event payload so a rename or fork keeps working; `vite.config.ts`
   deliberately carries no `base`.
5. `actions/configure-pages@v5` → `actions/upload-pages-artifact@v3`
   (`path: frontend/dist`, in the build job that produced it) →
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
