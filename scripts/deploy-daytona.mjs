// Compile @underload/nexus and boot it on a Daytona sandbox runtime endpoint.
//
// WHAT THIS DOES (R3 of the daytona runtime work):
//   cargo build → ONE single-file bundle (`nexus-hub`, a stripped static-ish ELF)
//   → POST it to the sandbox runner → the runner stages/boots it → we poll
//   `/status` until `ready`. The sandbox owns the lifecycle; this script only
//   ships bytes and waits.
//
// THE ENDPOINT CONTRACT (v1, frozen — the sandbox implements exactly this):
//   POST   /        body = raw bundle bytes (application/octet-stream) or JSON
//                   { profile, config?, bundleBase64, filename? }
//                   202 while accepted into the single runtime slot,
//                   409 { error: "runtime_active", slot } when the slot is busy
//   GET    /status  { slot, runtime?: { profile, port, pid, ready, … } }
//   GET    /logs    text/plain tail of the build+run log
//   DELETE /        idempotent teardown → { tornDown: true }
//   AUTH: a private Daytona preview URL authenticates ONLY through the request
//   header `x-daytona-preview-token` (no query param, no URL-embedded secret),
//   so --token / NEXUS_DAYTONA_PREVIEW_TOKEN is sent on POST, GET /status,
//   GET /logs and DELETE alike. Without it the proxy answers 401/403 before the
//   runner ever sees the profile headers.
//   The slot is single-runtime: empty → staging → building → booting →
//   ready | failed, and even `failed` holds the slot until DELETE. Hence
//   a 409 here means "someone (maybe a previous run of this script) already
//   owns the sandbox" — we tell the user to pass --teardown, we never
//   auto-teardown, because silently killing a running hub is worse than an
//   extra flag.
//
// TWO MODES:
//   --mode local         compile HERE, ship the binary. Zero sandbox build
//                        time, and it honours "nexus always builds correctly
//                        for a cargo/rust script that can execute anywhere".
//                        Risk: this dev host may not reach index.crates.io
//                        without an HTTPS proxy (README "Verification status")
//                        — that is a real failure we report, not hide.
//   --mode remote-build  ship the SOURCE as a tar.gz and let the sandbox run
//                        cargo (manifest profile). Slower, but independent of
//                        host registry access.
//
// GENERICITY: the bundle + profile + config are the only nexus-specific
// things here; the profile/config blocks below are what a future script type
// would swap out. Everything else (args, POST, 409 handling, polling) is
// copy-paste generic — deliberately NOT built into a framework, since nexus is
// the only wired profile today.
//
// RUN VIA: `yarn deploy:daytona --endpoint https://…` (package.json).
// Zero dependencies: node:fs, node:path, node:child_process, built-in fetch.
// Scratch output (the source tarball) goes in the repo-root `Temp/` folder per
// AGENTS.md — never OS temp, never inside the nexus package (nested git repo).
import { execFileSync, spawnSync } from "node:child_process";
// openSync/readSync/closeSync back the C3 format guard: the first 4 bytes of the
// bundle are probed without slurping a multi-MiB binary.
import { closeSync, existsSync, mkdirSync, openSync, readFileSync, readSync, statSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
// packages/underload/nexus — cargo runs from here so the repo-root
// .cargo/config.toml (target-dir = .rust/target) is discovered by walking up.
const pkgRoot = resolve(here, "..");
// packages/underload/nexus → packages/underload → packages → monorepo root.
// Used only to locate the repo-root `Temp/` scratch dir and the monorepo
// cargo target dir; a standalone nexus checkout has neither, which the
// candidate list below accounts for.
const workspaceRoot = resolve(pkgRoot, "..", "..", "..");

const BIN_NAME = "nexus-hub";
const PROFILE_NEXUS = "nexus";
const PROFILE_MANIFEST = "manifest";
const DEFAULT_PORT = 8085;
// Cap waits: local is just a proxy upload + process boot; remote-build pays a
// full in-sandbox cargo build (fat LTO), which is minutes-to-decades slow.
const WAIT_CAP_MS = { [PROFILE_NEXUS]: 6 * 60_000, [PROFILE_MANIFEST]: 40 * 60_000 };
const POLL_INTERVAL_MS = 2_000;
// C4: consecutive /status TRANSPORT misses tolerated before giving up. A
// Daytona preview proxy blips (TLS resets, 502 during deploys) while the
// runtime keeps booting, so one reset must not abort a 40-minute build wait;
// 5 consecutive misses (~10 s) is a dead endpoint and stays fatal.
const POLL_TRANSIENT_MAX = 5;
// Scratch tarball location (repo-root Temp/, git-ignored, disposable).
const tempDir = join(workspaceRoot, "Temp", "nexus-deploy");
const srcTarball = join(tempDir, "nexus-src.tar.gz");
// Directories that must never ride along in the source tarball: VCS state,
// host-built artifacts (Windows ELF/DLL/PDB here), wasm glue, deps, scratch.
const tarExcludes = [".git", "frontend", "target", "pkg", "node_modules", "Temp"];

const log = (message) => console.log(`>>> ${message}`);
const ok = (message) => console.log(`✓ ${message}`);
// Same `[nexus:…]` prefix the sibling build-wasm.mjs uses for failures, so
// script output is greppable across the package.
const die = (message, hint) => {
    console.error(`\n[nexus:deploy] ${message}`);
    if (hint) console.error(`[nexus:deploy] ${hint}`);
    process.exit(1);
};

const usage = `usage: node scripts/deploy-daytona.mjs [flags]

  --endpoint <url>   sandbox runner endpoint (env NEXUS_DAYTONA_ENDPOINT)
  --token <t>        Daytona preview token, sent as the x-daytona-preview-token
                     header on EVERY request (env NEXUS_DAYTONA_PREVIEW_TOKEN)
  --mode <mode>      local (default: compile here, ship the nexus-hub binary)
                     remote-build (ship a source tarball, cargo runs in-sandbox)
  --port <n>         port the hub listens on inside the sandbox (default ${DEFAULT_PORT})
  --env K=V          extra env var for the runtime; repeatable
  --no-build         local mode only: skip cargo and ship an existing artifact
                     from the candidate locations (dies if none exists)
  --artifact <path>  local mode only: ship exactly this file, skip cargo
                     (implies --no-build); must be a Linux ELF binary
  --no-wait          return after the 202 accepted instead of polling /status
  --teardown         DELETE / instead of posting (stop the active runtime)
  --dry-run          print the exact plan/request; no cargo, no network, no writes
  -h, --help         this text

examples:
  yarn deploy:daytona --endpoint https://proxy.example/abc --token <preview-token> --env NEXUS_TOKEN=xyz
  yarn deploy:daytona --mode remote-build --endpoint https://proxy.example/abc --token <preview-token>
  yarn deploy:daytona --no-build --token <preview-token>            # ship the already-built nexus-hub
  yarn deploy:daytona --artifact .rust/target/release/nexus-hub --token <preview-token>
  yarn deploy:daytona --teardown --endpoint https://proxy.example/abc --token <preview-token>`;

// ── args ────────────────────────────────────────────────────────────────────
/**
 * Parses the flat flag list. `--env` is repeatable and accumulates into a
 * plain object; K=V splits on the FIRST "=" so values may contain "=" (e.g.
 * base64 or `KEY=a=b`). Unknown/missing-value flags are fatal with usage.
 * The secret-bearing flags (--token) fall back to env vars so a CI wrapper can
 * inject them without putting the secret in the visible command line.
 */
function parseArgs(argv) {
    const out = {
        endpoint: process.env.NEXUS_DAYTONA_ENDPOINT ?? "",
        // C1: preview token; empty string means "send no auth header" (public
        // endpoint / loopback stub), which is a supported configuration.
        token: process.env.NEXUS_DAYTONA_PREVIEW_TOKEN ?? "",
        mode: "local",
        port: DEFAULT_PORT,
        env: {},
        wait: true,
        // C2: build-skip switches, local mode only (validated after --mode).
        noBuild: false,
        artifact: "",
        teardown: false,
        dryRun: false,
        help: false,
    };
    for (let i = 0; i < argv.length; i++) {
        const flag = argv[i];
        // Flags that all take a following value share this branch so the
        // "missing value" failure stays worded identically for each.
        if (flag === "--endpoint" || flag === "--mode" || flag === "--port" || flag === "--env" || flag === "--token" || flag === "--artifact") {
            const value = argv[++i];
            if (value === undefined || value.startsWith("--")) die(`${flag} needs a value`, usage);
            if (flag === "--endpoint") out.endpoint = value;
            else if (flag === "--token") out.token = value;
            else if (flag === "--artifact") out.artifact = value;
            else if (flag === "--mode") out.mode = value;
            else if (flag === "--port") out.port = Number(value);
            else {
                // K=V: reject an empty key; allow empty value and "=" in value.
                const eq = value.indexOf("=");
                if (eq <= 0) die(`--env expects KEY=VALUE, got "${value}"`, usage);
                out.env[value.slice(0, eq)] = value.slice(eq + 1);
            }
            continue;
        }
        if (flag === "--wait") out.wait = true;
        else if (flag === "--no-wait") out.wait = false;
        else if (flag === "--no-build") out.noBuild = true;
        else if (flag === "--teardown") out.teardown = true;
        else if (flag === "--dry-run") out.dryRun = true;
        else if (flag === "-h" || flag === "--help") out.help = true;
        else die(`unknown flag "${flag}"`, usage);
    }
    if (out.help) return out;
    if (out.mode !== "local" && out.mode !== "remote-build") {
        die(`--mode must be "local" or "remote-build", got "${out.mode}"`, usage);
    }
    // --no-build/--artifact pick which LOCAL bundle to ship; remote-build ships
    // a source tarball, so accepting them there would be a silent no-op and an
    // operator would believe a build (or an artifact) was skipped/used.
    if (out.mode !== "local" && (out.noBuild || out.artifact)) {
        die(`--no-build and --artifact apply to --mode local only (remote-build ships a source tarball)`, usage);
    }
    // Integer port in range; the sandbox binds 0.0.0.0:<port> verbatim.
    if (!Number.isInteger(out.port) || out.port < 1 || out.port > 65535) {
        die(`--port must be an integer 1-65535, got "${out.port}"`, usage);
    }
    return out;
}

/**
 * Validates the endpoint for the paths that actually need it (post/teardown).
 * Preview proxies hand out absolute URLs with paths, so trailing-slash joining
 * is done by joinUrl rather than assuming the endpoint is host-rooted.
 */
function requireEndpoint(endpoint) {
    if (!endpoint) {
        die("no endpoint — pass --endpoint <url> or set NEXUS_DAYTONA_ENDPOINT", usage);
    }
    let url;
    try {
        url = new URL(endpoint);
    } catch {
        die(`--endpoint is not a valid absolute URL: "${endpoint}"`);
    }
    if (url.protocol !== "http:" && url.protocol !== "https:") {
        die(`--endpoint must be http(s), got "${url.protocol}"`);
    }
    return endpoint;
}

// ── small helpers ───────────────────────────────────────────────────────────
const bytes = (n) => `${n.toLocaleString("en-US")} bytes (${(n / 1024 / 1024).toFixed(2)} MiB)`;
const pretty = (value) => JSON.stringify(value, null, 2);
/** base64url (RFC 4648 §5) — the header form the contract expects. */
const b64url = (value) => Buffer.from(JSON.stringify(value), "utf8").toString("base64url");
/** Promise wrapper for the poll cadence (no dependency on node:timers). */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
/**
 * C1: Daytona private preview URLs authenticate ONLY via this header (query
 * params and URL-embedded secrets are ignored by the proxy). Empty token →
 * no header at all, which keeps public endpoints and the loopback stub working.
 */
const authHeaders = (token) => (token ? { "x-daytona-preview-token": token } : {});
/** C1: dry-run/plan rendering — presence and length are shown, the value is not. */
const maskToken = (token) => (token ? `*** (length ${token.length})` : "(not sent — no --token / NEXUS_DAYTONA_PREVIEW_TOKEN)");
const authPlanLine = (token) =>
    `  x-daytona-preview-token: ${maskToken(token)}${token ? "  ← sent on POST, GET /status, GET /logs and DELETE" : ""}`;
/** Base-relative join that keeps a proxy's path prefix (no root-path clobbering). */
function joinUrl(base, suffix) {
    const url = new URL(base);
    url.pathname = `${url.pathname.endsWith("/") ? url.pathname : `${url.pathname}/`}${suffix}`;
    return url.toString();
}
/** size/existence probe used by both the plan print and the real build path. */
function probe(path) {
    if (!existsSync(path)) return { path, exists: false, size: 0 };
    const st = statSync(path);
    return { path, exists: st.isFile(), size: st.isFile() ? st.size : 0 };
}

// ── artifact location ───────────────────────────────────────────────────────
/**
 * C3 format guard. The sandbox execs the bundle verbatim, so it must be a
 * native Linux ELF. A host-built Windows PE POSTs fine (202) and then fails at
 * exec time with an opaque ENOEXEC buried in the sandbox log, so the check
 * happens here, before any bytes or sandbox time are spent.
 */
const ELF_MAGIC = Buffer.from([0x7f, 0x45, 0x4c, 0x46]); // 0x7f "E" "L" "F"
/** Reads the first 4 bytes without slurping a multi-MiB binary. */
function readMagic(path, n = 4) {
    const buf = Buffer.alloc(n);
    let fd;
    try {
        fd = openSync(path, "r");
        return buf.subarray(0, readSync(fd, buf, 0, n, 0));
    } catch {
        // Unreadable (permissions, race with a build): report as "no magic" so
        // the caller dies with the format message instead of a stack.
        return Buffer.alloc(0);
    } finally {
        if (fd !== undefined) closeSync(fd);
    }
}
/** Human label for whatever the first bytes actually were. */
function describeMagic(magic) {
    if (magic.length < 4) return `short read (${magic.length} bytes)`;
    const hex = [...magic].map((b) => b.toString(16).padStart(2, "0")).join(" ");
    if (magic[0] === 0x4d && magic[1] === 0x5a) return `Windows PE/DOS ("MZ", ${hex})`;
    if (magic[0] === 0xca && magic[1] === 0xfe) return `macOS Mach-O ("CAFÉ", ${hex})`;
    return `unknown (${hex})`;
}
/** Dies unless the bundle starts with the ELF magic; returns it otherwise. */
function assertElf(found) {
    const magic = readMagic(found.path);
    if (magic.equals(ELF_MAGIC)) return found;
    const label = describeMagic(magic);
    const isWindows = magic[0] === 0x4d && magic[1] === 0x5a;
    die(
        `${found.path} is not a Linux ELF binary — ${label}`,
        `the sandbox execs the bundle as-is, so a Windows build here would only fail in-sandbox:\n` +
            (isWindows
                ? `  - --mode remote-build  (recommended: the sandbox compiles a Linux ELF from source)\n`
                : `  - --mode remote-build  (the sandbox compiles a Linux ELF from source)\n`) +
            `  - --artifact <path> pointing at a Linux ${BIN_NAME} build\n` +
            `  - --no-build with a Linux artifact at ${artifactCandidates()[0]}`,
    );
}
/**
 * Two candidate locations, because cargo's target dir depends on where the
 * package is checked out:
 *   - monorepo: the repo-root .cargo/config.toml sets `target-dir = .rust/target`
 *     and cargo finds it by walking UP past the package boundary →
 *     <monorepoRoot>/.rust/target/release/nexus-hub
 *   - standalone nexus checkout: no such config → ./target/release/nexus-hub
 * The build path prefers cargo's own JSON output over guessing either one.
 */
const artifactCandidates = () => [
    join(workspaceRoot, ".rust", "target", "release", BIN_NAME),
    join(pkgRoot, "target", "release", BIN_NAME),
];

/**
 * Presence check WITHOUT invoking cargo — the dry run must not exec anything.
 * Scans PATH for an executable entry literally named `cargo`.
 */
function cargoOnPath() {
    const sep = process.platform === "win32" ? ";" : ":";
    // On Windows both an extensionless `cargo` (git-bash shim) and the
    // PATHEXT-suffixed binaries count; on POSIX the bare name is the only form.
    const names = ["", ...(process.platform === "win32" ? (process.env.PATHEXT ?? ".EXE").split(";") : [])];
    for (const dir of (process.env.PATH ?? "").split(sep)) {
        if (!dir) continue;
        for (const ext of names) {
            try {
                if (existsSync(join(dir, `cargo${ext}`))) return true;
            } catch {
                /* a malformed PATH entry is a miss, not a crash */
            }
        }
    }
    return false;
}

/**
 * One cargo attempt with the pinned recipe; returns the raw spawnSync result.
 * `extraArgs` carries the `--offline` retry flag (see buildLocally).
 */
function runCargoBuild(extraArgs = []) {
    const args = ["build", "--release", "--locked", "-p", "nexus-server", "--message-format=json", ...extraArgs];
    log(`cargo ${args.join(" ")}  (cwd ${pkgRoot})`);
    const run = spawnSync("cargo", args, {
        cwd: pkgRoot,
        encoding: "utf8",
        // JSON lines plus compiler diagnostics can exceed the 1 MiB default.
        maxBuffer: 64 * 1024 * 1024,
    });
    // cargo itself is missing/uncallable: no retry could help, so stop here
    // with the toolchain message rather than the registry-fetch one.
    if (run.error) die(`cargo could not be started: ${run.error.message}`, "cargo is not on PATH (or the pinned toolchain is missing)");
    return run;
}

/** stderr tail used for the failure report; 12 lines keeps it readable. */
const stderrTail = (run) => (run.stderr ?? "").trim().split("\n").slice(-12).join("\n");

/**
 * Runs the pinned compile recipe and returns the artifact path.
 *
 * C2: the FIRST failure is retried exactly once with `--offline`, because the
 * documented host failure is the `index.crates.io` fetch (nexus README,
 * "Verification status") while the lockfile's deps are usually already vendored
 * in ~/.cargo. A second failure is fatal and lists the three escape routes —
 * nothing is ever shipped implicitly, so a stale artifact needs an explicit
 * --no-build / --artifact from the operator.
 * Returns the successful spawnSync result; resolveArtifact reads the artifact
 * path out of it (a no-op build may emit no artifact line, which is why the
 * caller still keeps the candidate list as a fallback).
 */
function buildLocally() {
    const first = runCargoBuild();
    if (first.status === 0) return first;
    const firstTail = stderrTail(first);
    log(`cargo build failed (exit ${first.status ?? "?"}) — retrying once with --offline (the crates.io index fetch is the known host failure)`);
    const retry = runCargoBuild(["--offline"]);
    if (retry.status !== 0) {
        const retryTail = stderrTail(retry);
        die(
            `cargo build failed (exit ${retry.status ?? "?"}; the --offline retry failed too)`,
            `--- first attempt ---\n${firstTail}\n--- --offline retry ---\n${retryTail}\n` +
                `options, all explicit operator actions:\n` +
                `  1. --mode remote-build      ship the source tarball; cargo runs in the sandbox (no host registry access)\n` +
                `  2. export HTTPS_PROXY=http://<proxy>:<port>  then re-run — this host needs a proxy for index.crates.io (nexus README, "Verification status")\n` +
                `  3. --no-build [--artifact <path>]  ship an existing Linux ${BIN_NAME} ELF (e.g. ${artifactCandidates()[0]})`,
        );
    }
    ok("cargo build succeeded on the --offline retry");
    return retry;
}

/**
 * Pulls the compiler-artifact executable path out of cargo's NDJSON stream
 * (`--message-format=json`). This is the only reliable locator: the target dir
 * depends on whether a repo-root .cargo/config.toml is discovered (see
 * artifactCandidates above), so guessing is the fallback, not the primary.
 * Returns null for a no-op build that emitted no artifact line.
 */
function artifactFromCargoJson(run) {
    let executable = null;
    for (const line of (run.stdout ?? "").split("\n")) {
        if (!line.startsWith("{")) continue;
        let msg;
        try {
            msg = JSON.parse(line);
        } catch {
            continue;
        }
        // Later matches win: the final artifact line is the finished binary.
        if (msg.reason === "compiler-artifact" && msg.target?.name === BIN_NAME && msg.executable) executable = msg.executable;
    }
    if (executable) ok(`cargo reported the artifact: ${executable}`);
    return executable;
}

/**
 * Picks the bundle to ship in local mode (C2/C3), in operator-intent order:
 *   --artifact <path> → that exact file; cargo never runs, so an explicitly
 *                       named artifact can never be silently shadowed by a
 *                       build that writes somewhere else (implies --no-build)
 *   --no-build        → cargo never runs; first non-empty candidate wins
 *   otherwise         → compile, trust cargo's reported path, then candidates
 * Every returned path has passed the ELF format guard (C3).
 */
function resolveArtifact(args) {
    if (args.artifact) {
        const path = resolve(args.artifact);
        const found = probe(path);
        if (!found.exists || found.size === 0) {
            die(
                `--artifact "${args.artifact}" is not a non-empty file`,
                `resolved to ${path} (${found.exists ? "not a regular file" : "absent"}) — cargo is skipped by --artifact, so there is nothing to ship`,
            );
        }
        ok(`shipping --artifact ${path} — ${bytes(found.size)} (cargo skipped)`);
        return assertElf(found);
    }
    // cargo's reported path first, then the two guessed locations as fallbacks.
    let fromCargo = null;
    if (args.noBuild) {
        log("--no-build: cargo skipped; looking for an existing artifact instead");
    } else {
        fromCargo = artifactFromCargoJson(buildLocally());
    }
    const candidates = [fromCargo, ...artifactCandidates()].filter(Boolean);
    for (const c of candidates) {
        const found = probe(c);
        if (found.exists && found.size > 0) {
            ok(`${BIN_NAME} ready — ${bytes(found.size)}`);
            return assertElf(found);
        }
    }
    die(
        args.noBuild
            ? `--no-build: no existing non-empty ${BIN_NAME} found to ship`
            : `build reported success but no non-empty ${BIN_NAME} was found`,
        `${candidates.map((c) => `  - ${c}`).join("\n")}\n` +
            (args.noBuild ? `drop --no-build to compile it, or point --artifact at an existing Linux ${BIN_NAME}` : ""),
    );
}

/**
 * Packs the package source for the manifest profile using system tar.
 * Excludes keep host-built and VCS bytes out of the bundle; the sandbox
 * unpacks to a fresh dir, so its cargo writes to ./target (no repo-root
 * .cargo/config.toml exists inside the tarball).
 */
function buildSourceTarball() {
    mkdirSync(tempDir, { recursive: true });
    const args = ["-czf", srcTarball];
    for (const pattern of tarExcludes) args.push(`--exclude=${pattern}`);
    args.push("-C", pkgRoot, ".");
    log(`tar ${tarExcludes.map((e) => `--exclude=${e}`).join(" ")} ${pkgRoot} → ${srcTarball}`);
    try {
        execFileSync("tar", args, { encoding: "utf8", maxBuffer: 16 * 1024 * 1024 });
    } catch (error) {
        // tar exit 1 means "some files changed/vanished while reading"; the
        // archive is still usable. Exit ≥2 is fatal. Surface stderr either way.
        const stderr = (error.stderr ?? "").trim();
        if (error.status === undefined || error.status >= 2) {
            die(`tar failed (exit ${error.status ?? error.message})`, stderr);
        }
        console.warn(`[nexus:deploy] tar warning: ${stderr.split("\n")[0] ?? ""}`);
    }
    const found = probe(srcTarball);
    if (!found.exists || found.size === 0) die(`tar produced no archive at ${srcTarball}`);
    ok(`source bundle ready — ${bytes(found.size)}`);
    return found;
}

// ── HTTP ────────────────────────────────────────────────────────────────────
/**
 * fetch + JSON parse with a body dump on non-2xx: a preview proxy returning
 * HTML (502/404) is otherwise an opaque "Unexpected token <" JSON error.
 *
 * `options.soft` (C4, poll loop only) turns a TRANSPORT failure into a
 * `{ networkError }` result instead of exiting, so the caller can tolerate a
 * proxy blip. It deliberately does NOT cover HTTP statuses — those are the
 * runner answering, which is a real answer.
 */
async function request(method, url, options = {}) {
    // `timeoutMs` and `soft` are ours, never part of fetch's init.
    const { timeoutMs, soft, ...init } = options;
    let response;
    try {
        // `method` must be in the init: fetch defaults to GET, and a GET with a
        // body throws "Request with GET/HEAD method cannot have body".
        response = await fetch(url, { ...init, method, signal: AbortSignal.timeout(timeoutMs ?? 15_000) });
    } catch (error) {
        const message = error?.message ?? String(error);
        if (soft) return { status: 0, json: null, text: "", ok: false, networkError: message };
        // A POST that timed out here may already have been ACCEPTED, so callers
        // must not retry it — the single slot would answer 409 to the retry.
        die(`${method} ${url} failed: ${message}`, "is the endpoint reachable and the Daytona preview proxy up?");
    }
    // Reading the body is inside the same timeout window, so a proxy that
    // headers-then-hangs surfaces as a clear message rather than a stack.
    let text;
    try {
        text = await response.text();
    } catch (error) {
        const message = `response body read failed: ${error?.message ?? error}`;
        if (soft) return { status: 0, json: null, text: "", ok: false, networkError: message };
        die(`${method} ${url}: ${message}`);
    }
    let json = null;
    try {
        json = JSON.parse(text);
    } catch {
        /* non-JSON (proxy error page) — handled below */
    }
    if (!response.ok) {
        return { status: response.status, json, text, ok: false };
    }
    return { status: response.status, json, text, ok: true };
}

/** POSTs a raw bundle with the profile/config/filename headers (+ auth, C1). */
function postBundle(endpoint, { profile, config, filename, body, token }) {
    return request("POST", joinUrl(endpoint, ""), {
        headers: {
            "content-type": "application/octet-stream",
            "x-runtime-profile": profile,
            // base64url so the header stays ASCII/JSON-safe.
            "x-runtime-config": b64url(config),
            "x-runtime-filename": filename,
            // C1: the preview proxy rejects unauthenticated traffic before the
            // runner sees anything, so the bundle upload needs the token too.
            ...authHeaders(token),
        },
        body,
        // A 3+ MiB binary through a preview proxy is slower than a GET.
        timeoutMs: 120_000,
    });
}

/**
 * Polls /status every 2 s until ready/failed or the profile-specific cap.
 * Prints only slot transitions (plus a heartbeat every 10 polls) so a 40-minute
 * in-sandbox cargo build does not drown the terminal.
 *
 * C1: every poll carries the preview token — the proxy guards /status exactly
 * like POST /.
 * C4: TRANSPORT failures are tolerated up to POLL_TRANSIENT_MAX consecutive
 * misses (the run keeps going, the overall cap still bounds it); an HTTP error
 * from the runner is a real answer and stays fatal, and the persistent-miss
 * path returns false so main() exits non-zero.
 */
async function pollUntilReady(endpoint, capMs, token) {
    const deadline = Date.now() + capMs;
    let lastSlot = "";
    let tick = 0;
    let misses = 0;
    while (Date.now() < deadline) {
        const res = await request("GET", joinUrl(endpoint, "status"), {
            headers: authHeaders(token),
            soft: true,
            timeoutMs: 15_000,
        });
        if (res.networkError) {
            // `>` not `>=`: 5 misses are tolerated, the 6th in a row ends it.
            if (++misses > POLL_TRANSIENT_MAX) {
                console.error(`✗ /status failed ${misses} times in a row — giving up: ${res.networkError}`);
                return false;
            }
            log(`/status fetch failed (${res.networkError}) — transient miss ${misses}/${POLL_TRANSIENT_MAX}, the runtime may still be booting`);
            await sleep(POLL_INTERVAL_MS);
            continue;
        }
        // A successful round trip (any status) proves the endpoint is alive.
        misses = 0;
        if (!res.ok) die(`/status returned ${res.status}`, (res.text ?? "").slice(0, 400));
        const slot = res.json?.slot ?? "unknown";
        if (slot !== lastSlot) {
            log(`slot: ${slot}${res.json?.runtime ? ` (port ${res.json.runtime.port}, ready=${!!res.json.runtime.ready})` : ""}`);
            lastSlot = slot;
        }
        if (slot === "ready" && res.json?.runtime?.ready) {
            ok(`runtime ready — profile ${res.json.runtime.profile}, port ${res.json.runtime.port}`);
            console.log(pretty(res.json));
            return true;
        }
        if (slot === "failed") {
            console.error(`✗ runtime failed in slot "${slot}"`);
            console.log(pretty(res.json));
            // Best-effort log tail: the sandbox's build/run log is the only
            // way to see WHY (cargo error, missing env, bind failure). The
            // token is required here too — /logs is behind the same proxy.
            const logs = await request("GET", `${joinUrl(endpoint, "logs")}?lines=40`, { headers: authHeaders(token) });
            if (logs.ok) console.error(`--- last log lines ---\n${logs.text}`);
            return false;
        }
        if (++tick % 10 === 0) log(`still ${slot}… (${Math.round((deadline - Date.now()) / 1000)} s of budget left)`);
        await sleep(POLL_INTERVAL_MS);
    }
    console.error(`✗ timed out after ${Math.round(capMs / 60_000)} min waiting for "ready" (last slot: ${lastSlot || "never seen"})`);
    return false;
}

/** Handles the 409 the single-slot runtime returns when it is already busy. */
function handleBusy(status, json) {
    if (status !== 409) return false;
    console.error(`\n✗ runtime already active — run with --teardown first`);
    console.error(`  the sandbox holds one runtime slot; it is in "${json?.slot ?? "non-empty"}" state (even "failed" occupies it).`);
    return true;
}

// ── dry run ─────────────────────────────────────────────────────────────────
/**
 * Prints the full plan and the exact request that WOULD be sent, with no cargo
 * exec, no network, and no file writes. Used to sanity-check an endpoint/config
 * before spending sandbox time, and to show where the artifact would be.
 */
function printDryRunPlan(args, config) {
    const isNexus = args.mode === "local";
    const profile = isNexus ? PROFILE_NEXUS : PROFILE_MANIFEST;
    log(`dry run — mode ${args.mode}, profile ${profile}, no cargo / no network / no writes`);
    if (isNexus) {
        // C2: the plan mirrors resolveArtifact's precedence exactly, so a dry
        // run tells the truth about which bundle a real run would ship.
        if (args.artifact) log(`cargo: skipped (--artifact ships an explicit file)`);
        else if (args.noBuild) log("cargo: skipped (--no-build); an existing artifact from the candidates below must exist");
        else if (!cargoOnPath()) log("cargo is NOT on PATH here — local mode would fail; --mode remote-build avoids it");
        const paths = args.artifact ? [resolve(args.artifact)] : artifactCandidates();
        log(args.artifact ? "artifact (explicit --artifact, used verbatim):" : "artifact candidates (first non-empty one wins):");
        for (const c of paths) {
            const found = probe(c);
            if (!found.exists || found.size === 0) {
                console.log(`    · ${found.path}  (absent${args.artifact ? " — a real run dies here" : ""})`);
                continue;
            }
            // C3 preview: the ELF magic is read (read-only, no writes/exec) so a
            // Windows build is caught in the plan, not after a 3 MiB upload.
            const magic = readMagic(found.path);
            const format = magic.equals(ELF_MAGIC) ? "ELF ✓" : `NOT ELF — ${describeMagic(magic)}, a real run refuses it`;
            console.log(`    ✓ ${found.path}  ${bytes(found.size)}  [${format}]`);
        }
        if (!args.artifact && !args.noBuild) {
            log("(a real run compiles with `cargo build --release --locked -p nexus-server --message-format=json`, retries once with --offline on failure, and trusts cargo's reported path)");
        }
    } else {
        const found = probe(srcTarball);
        log(`source bundle: ${srcTarball} ${found.exists ? `✓ ${bytes(found.size)}` : "(absent — a real run creates it via system tar)"}`);
        log(`tar excludes: ${tarExcludes.join(", ")}`);
    }
    // Validating the URL here (not just before the POST) is the point of a dry
    // run: a malformed endpoint should fail in the plan, not after the build.
    if (args.endpoint) requireEndpoint(args.endpoint);
    const endpointLabel = args.endpoint ? args.endpoint : "(none — --endpoint not set)";
    const target = args.endpoint ? joinUrl(args.endpoint, "") : "<endpoint>/";
    log(`endpoint: ${endpointLabel}${args.endpoint ? `  →  POST target ${target}` : ""}`);
    const filename = isNexus ? BIN_NAME : "nexus-src.tar.gz";
    log(
        `request that would be sent:\n  POST ${target}\n  content-type: application/octet-stream\n  x-runtime-profile: ${profile}\n` +
            `  x-runtime-filename: ${filename}\n  x-runtime-config: ${b64url(config).slice(0, 24)}… (base64url, masked; decoded below)\n` +
            // C1: shown masked — the plan is pasted into logs/terminals, the
            // preview token must never be echoed in full.
            authPlanLine(args.token),
    );
    console.log(`  config:\n${pretty(config).split("\n").map((l) => `    ${l}`).join("\n")}`);
    const bodyNote = isNexus
        ? args.artifact
            ? `${args.artifact} as it sits on disk (cargo skipped)`
            : args.noBuild
              ? `the existing ${BIN_NAME} ELF from the candidates above (cargo skipped)`
              : `the ${BIN_NAME} ELF once compiled here (a few MiB)`
        : "nexus-src.tar.gz source tree (a few hundred KiB)";
    console.log(`  body: raw application/octet-stream — ${bodyNote}`);
    log(`after 202: ${args.wait ? `poll GET ${args.endpoint ? joinUrl(args.endpoint, "status") : "<endpoint>/status"} every ${POLL_INTERVAL_MS / 1000}s (auth header on every poll; up to ${POLL_TRANSIENT_MAX} consecutive fetch blips tolerated), cap ${WAIT_CAP_MS[profile] / 60_000} min` : "no wait (--no-wait)"}; 409 → "run with --teardown first", exit 1`);
    ok("dry run complete (exit 0)");
}

/**
 * C5: `--dry-run --teardown` must not touch the network, so the DELETE is
 * printed instead of sent (a real teardown against an unreachable endpoint is
 * still a valid plan check). The endpoint is only validated, never contacted,
 * and may be omitted entirely in a dry run.
 */
function printTeardownPlan(args) {
    log("dry run — teardown, no cargo / no network / no writes");
    if (args.endpoint) requireEndpoint(args.endpoint);
    const target = args.endpoint ? joinUrl(args.endpoint, "") : "<endpoint>/";
    log(`request that would be sent:\n  DELETE ${target}\n  (idempotent: an empty slot still answers { tornDown: true })`);
    console.log(authPlanLine(args.token));
    ok("dry run complete (exit 0)");
}

// ── main ────────────────────────────────────────────────────────────────────
async function main() {
    const args = parseArgs(process.argv.slice(2));
    if (args.help) {
        console.log(usage);
        process.exit(0);
    }

    // Teardown is its own verb: DELETE / is idempotent, so it needs neither a
    // mode nor an artifact.
    if (args.teardown) {
        // C5: a dry run prints the DELETE rather than sending it — no network,
        // no endpoint requirement, exit 0 even against an unreachable host.
        if (args.dryRun) {
            printTeardownPlan(args);
            return;
        }
        const endpoint = requireEndpoint(args.endpoint);
        log(`DELETE ${joinUrl(endpoint, "")} — teardown`);
        // C1: / (DELETE) is guarded by the same preview proxy as everything else.
        const res = await request("DELETE", joinUrl(endpoint, ""), { headers: authHeaders(args.token), timeoutMs: 60_000 });
        console.log(pretty(res.json ?? { status: res.status, text: res.text.slice(0, 400) }));
        if (!res.ok) die(`teardown failed with HTTP ${res.status}`);
        ok("runtime torn down — slot is empty, POST again");
        return;
    }

    // Config sent in x-runtime-config; only overrides, the sandbox owns the
    // rest of the profile defaults.
    const profile = args.mode === "local" ? PROFILE_NEXUS : PROFILE_MANIFEST;
    const config =
        args.mode === "local"
            ? // nexus profile: port + extra env. anonymous:true is a sandbox
              // default, so it is only worth overriding if a caller wants it.
              { port: args.port, env: args.env }
            : // manifest profile: build inside the sandbox, then run the artifact
              // it produced. artifactPath/runCmd are extraction-dir relative, and
              // `./target` is right there because the tarball has no
              // repo-root .cargo/config.toml (so no .rust/target remap).
              {
                  buildCmd: "cargo build --release --locked -p nexus-server",
                  buildTimeoutSec: 1800,
                  artifactPath: `target/release/${BIN_NAME}`,
                  runCmd: `./target/release/${BIN_NAME}`,
                  env: args.env,
                  port: args.port,
                  probePath: "/v1/observe",
                  probeTimeoutSec: 60,
                  stopSignal: "SIGINT",
              };

    if (args.dryRun) {
        printDryRunPlan(args, config);
        return;
    }

    const endpoint = requireEndpoint(args.endpoint);
    let bundle;
    if (args.mode === "local") {
        // C2/C3: honours --no-build/--artifact and rejects a non-ELF bundle
        // before a single byte is uploaded.
        bundle = resolveArtifact(args);
    } else {
        bundle = buildSourceTarball();
    }

    log(`POST ${joinUrl(endpoint, "")} — profile ${profile}, ${bytes(bundle.size)}`);
    // Read the bytes here (not at build time) so the body matches what is on
    // disk right before sending; the size printed above is the same stat.
    const payload = readFileSync(bundle.path);
    const res = await postBundle(endpoint, {
        profile,
        config,
        filename: args.mode === "local" ? BIN_NAME : "nexus-src.tar.gz",
        body: payload,
        // C1: the upload itself is behind the preview proxy too.
        token: args.token,
    });
    // Pretty-print whatever the runner said (202 runtime info, 409 slot info,
    // or the raw proxy error page when the body is not JSON).
    console.log(pretty(res.json ?? { status: res.status, text: (res.text ?? "").slice(0, 400) }));
    if (handleBusy(res.status, res.json)) process.exit(1);
    if (res.status !== 202 && !res.ok) die(`POST failed with HTTP ${res.status}`, (res.text ?? "").slice(0, 400));
    ok(`accepted (HTTP ${res.status}) — sandbox is staging/building/booting`);

    if (!args.wait) {
        log("not waiting (--no-wait); poll GET /status when you want the port");
        return;
    }
    const ready = await pollUntilReady(endpoint, WAIT_CAP_MS[profile], args.token);
    if (!ready) process.exit(1);
}

main().catch((error) => die(error?.stack ?? String(error)));
