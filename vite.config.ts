// Vite config for the Nexus Monitor frontend.
//
// Kept deliberately minimal (react plugin + a fixed dev port). Two decisions
// worth recording:
//
// 1. NO `base` here. The GitHub Pages deploy (see the workflow that ships with
//    THIS repo: .github/workflows/deploy-pages.yml) passes
//    `--base=/<repo-name>/` on the build command line — the repo name is
//    only known at deploy time, so it stays out of the source. Omitting
//    `base` also keeps `yarn preview` working when the dist/ folder is
//    served from a domain root.
// 2. NO vitest section here either. `vitest run` (the package `test`
//    script) falls back to this config file and defaults to the node
//    environment with the default include pattern
//    (`**/*.test.ts(x)` outside node_modules). Every test in this package
//    is pure-logic or react-dom/server (no DOM), so node is correct and
//    no jsdom dependency is needed.
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
    plugins: [react()],
    server: {
        // Fixed dev port so `yarn dev` — run here in a standalone checkout, or
        // as `yarn workspace @underload/nexus dev` from the noobscript
        // monorepo root, the two install contexts this package has (see README
        // "Two install contexts, one manifest") — always lands in one place
        // (8200 is bench, 8000 the distribution apps; 8300 is unclaimed among
        // the dashboard frontends).
        port: 8300,
        host: true
    },
    build: {
        // Default outDir/dist; the Pages workflow uploads this exact path
        // (`dist`, relative to the repo root) as the artifact.
        outDir: 'dist',
        emptyOutDir: true
    }
});
