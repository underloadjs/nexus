// ══════════════════════════════════════════════════════════════════════════════
//  start-daytona.ts — ONE COMMAND THAT BOOTS A NEXUS HUB INSIDE A DAYTONA SANDBOX
//  ("spawn a sandbox, then POST the nexus code into it once it is ready")
// ══════════════════════════════════════════════════════════════════════════════
//
//  WHAT THIS DOES (the orchestrator the deploy script was missing):
//    deploy-daytona.mjs already speaks the sandbox runtime contract (POST bytes,
//    poll /status), but it needs an `--endpoint` that already exists. This script
//    supplies one:
//
//        createSandbox()  ──►  spawn deploy-daytona.mjs  ──►  getPreviewLink(hubPort)
//        (fresh sandbox,      (cargo build + POST bytes      (the URL clients use
//         runner on :7777      + poll until ready)            to reach the hub)
//         already health-checked)
//
//    It never talks to Daytona or to the sandbox itself: sandbox bring-up goes
//    through the @services/daytona/sandbox LIBRARY (createSandbox, which already
//    waits for `GET /health`), and every byte of POST/poll/teardown is delegated
//    to the sibling script as a CHILD PROCESS (that script runs main() at import,
//    so importing it is not an option — spawning it is).
//
//  TWO DIFFERENT URLS — the distinction this script exists to make obvious:
//    · MANAGEMENT endpoint = createSandbox().endpoint = the runner's preview URL
//      (port 7777 inside the sandbox). It is what `--endpoint` is pointed at, what
//      deploy POSTs to, and what GET /status + GET /logs + DELETE / answer on.
//      Auth, when the sandbox is private, is the `x-daytona-preview-token` header
//      (deploy-daytona.mjs:17-21) — the orchestrator just forwards `--token`.
//    · HUB URL = sandbox.getPreviewLink(<hub port>) = a DIFFERENT preview link,
//      one per port, for the nexus hub process the runner started on `--port`
//      (default 8085). It can carry its OWN token even when the management URL
//      does not, which is why the summary prints both, separately.
//
//  RUN VIA (package.json scripts; `vite-node` is root-hoisted, house convention):
//      yarn workspace @underload/nexus start:daytona                       # fresh sandbox + deploy, waits for ready
//      yarn workspace @underload/nexus start:daytona --ttl 60 --env NEXUS_TOKEN=xyz
//      yarn workspace @underload/nexus start:daytona --private             # token-authed preview URLs
//      yarn workspace @underload/nexus start:daytona --endpoint https://… --token …   # REUSE an endpoint you already own
//      yarn workspace @underload/nexus start:daytona --teardown --endpoint https://… # DELETE / (frees the slot, NOT the sandbox)
//      yarn workspace @underload/nexus start:daytona --dry-run             # print the plan, touch nothing
//    Extra args after the yarn script name are appended to the command, so every
//    flag below works through yarn too. `--help` is the usage screen.
//
//  MODES (mutually exclusive, derived from the flags — see resolveMode):
//    fresh   (default)      createSandbox → deploy → hub URL. Owns the sandbox.
//    reuse   (--endpoint)   deploy only, against someone else's endpoint.
//    teardown (--teardown)  deploy --teardown only; requires --endpoint.
//
//  LIFECYCLE / BILLING — read this before walking away from a running command:
//    · A fresh sandbox is created PER CALL (createSandbox never reuses anything,
//      setup-sandbox.ts:1411-1415) and KEEPS BILLING until it is deleted or its
//      TTL fires. Nothing here deletes a sandbox on SUCCESS — that is the point
//      (the hub you just paid for is meant to keep serving).
//    · `--ttl <minutes>` is the only wall-clock fence; it defaults to UNSET, i.e.
//      "runs forever" (createSandbox's own docs, setup-sandbox.ts:1557-1563).
//    · `public: true` is the default (tokenless preview URLs). `--private` makes
//      both the management URL and the hub URL require their tokens.
//    · On a FAILED deploy the orchestrator deletes the sandbox it just created
//      (a half-deployed sandbox is pure burn); `--keep-on-fail` opts out so the
//      sandbox can be poked at, and then the sandbox IS still billing.
//    · Reuse/teardown NEVER touch sandbox lifecycle: that endpoint was not ours.
//    · createSandbox itself already deletes only on a FAILED bring-up
//      (setup-sandbox.ts:1560-1563) — no double-delete here.
//    · Ctrl-C while a FRESH run owns the sandbox is NOT a free exit: from the moment
//      createSandbox returns until the run ends, SIGINT/SIGTERM are trapped, the
//      sandbox is deleted best-effort (bounded ~10s wait so a hung SDK call can
//      never turn Ctrl-C into a hang), the outcome is printed, and the process exits
//      130 (SIGINT) / 143 (SIGTERM). Without that fence the freshly-created sandbox
//      bills forever. decideInterruptCleanup() is the pure decision; reuse, teardown
//      and dry-run install NO handlers because they never own a sandbox.
//
//  MACHINE-READABLE OUTPUT: after a successful fresh/reuse start, the LAST line
//  of stdout is exactly ONE single-line JSON object (nothing else is on that line;
//  the example below is wrapped only to fit):
//      {"mode":"fresh","endpoint":"https://…","token":"…","hubUrl":"https://…",
//       "hubToken":"…","sandboxId":"…","name":"…","public":true,"hubPort":8085}
//  `token` (management preview token) and `hubToken` (hub preview token) are
//  omitted when empty — public sandbox, no header needed. In REUSE mode the same
//  line carries `"hubUrl":null` (the hub preview link needs the sandbox's SDK
//  handle, which only a fresh create has) and no `sandboxId`/`name`/`public`.
//  Automation chains off `hubUrl` (client-facing) and, for a later
//  `--teardown`/`--endpoint` run, off `endpoint` + `token`. Teardown and dry-run
//  print no JSON line.
//
//  IMPORT IS A NO-OP: everything meaningful is an exported pure function over an
//  explicit input (parseArgs / resolveMode / buildSandboxOptions /
//  buildDeployArgs / runStartDaytona), and the only I/O lives in main()'s default
//  deps (real createSandbox + real child_process.spawn). The colocated
//  start-daytona.test.ts drives the whole flow with fakes, with zero network.
//  The auto-run guard at the bottom mirrors the `ENTRY POINT` block in
//  services/daytona/sandbox/setup-sandbox.ts (the same three entry rules, because
//  plain `vite-node <file>` DROPS the file from process.argv — see the rule comments).
// ══════════════════════════════════════════════════════════════════════════════

import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
// The curated barrel (index.ts) is the ONLY public surface of the service; it has
// no import side effects, which is what keeps this module importable in tests.
import { createSandbox } from '@services/daytona/sandbox';
import type { CreateSandboxOptions, SandboxLog } from '@services/daytona/sandbox';

// Re-exported so a consumer (or the test suite) can NAME the shapes this module's
// plan types carry, without reaching past the daytona barrel on its own.
export type { CreateSandboxOptions, SandboxLog };

// ══════════════════════════════════════════════════════════════════════════════
//  PINS — where the sibling script lives, resolved from THIS module (never cwd)
// ══════════════════════════════════════════════════════════════════════════════

/** The zero-dependency sibling this script spawns; it owns the POST/poll contract. */
const DEPLOY_SCRIPT_NAME = 'deploy-daytona.mjs';
/**
 * Same candidate-list discipline the daytona library uses for module-relative
 * paths (setup-sandbox.ts:161-195): under this repo's vitest env `import.meta.url`
 * is not always a file:// URL, while vite-node (the real runner) always gives one.
 * Each candidate must actually CONTAIN the deploy script, which is what makes the
 * cwd fallbacks safe from the three possible launch cwds (pkg root / repo root /
 * scripts dir).
 */
const SCRIPTS_DIR_CANDIDATES = (): string[] => {
    const fromImportMeta = (() => {
        try {
            const url = import.meta.url;
            if (typeof url === 'string' && url.startsWith('file://')) return dirname(fileURLToPath(url));
        } catch {
            /* non-file module url under vitest → fall through to the cwd candidates */
        }
        return undefined;
    })();
    return [
        fromImportMeta, //                                   vite-node / node / tsx
        resolve(process.cwd(), 'scripts'), //                cwd = the nexus package root (yarn lifecycle)
        resolve(process.cwd(), 'packages/underload/nexus/scripts'), // cwd = repo root
        resolve(process.cwd()), //                           cwd = the scripts dir itself
    ].filter((candidate): candidate is string => Boolean(candidate));
};
/** A candidate is only usable when the sibling script really is sitting next to it. */
const isScriptsDir = (dir: string): boolean => existsSync(join(dir, DEPLOY_SCRIPT_NAME));
/**
 * Resolved ONCE at module load. A non-null deploy script is guaranteed in every
 * real launch context; under vitest the import.meta.url candidate resolves too,
 * so the plan/invocation paths tests assert are the real ones.
 */
const SCRIPTS_DIR = SCRIPTS_DIR_CANDIDATES().find(isScriptsDir) ?? resolve(process.cwd(), 'scripts');
/** Absolute path of the child script — spawn does NOT resolve it against cwd. */
export const DEPLOY_SCRIPT_PATH = join(SCRIPTS_DIR, DEPLOY_SCRIPT_NAME);
/** cargo runs from the package root so the repo-root .cargo/config.toml is found (deploy-daytona.mjs:57-60). */
export const PACKAGE_ROOT = resolve(SCRIPTS_DIR, '..');

/** Hub port default — MUST equal DEFAULT_PORT in deploy-daytona.mjs:70 (8085). */
export const DEFAULT_HUB_PORT = 8085;
/** Every line this script itself writes is prefixed, so it is greppable next to the child's `>>>`/`✓` lines. */
const PREFIX = '[nexus:start]';
/**
 * dry-run placeholder for an endpoint that only exists after createSandbox. It is
 * deliberately NOT a URL: dry-run never spawns, so it can never reach a fetch call.
 */
const PLACEHOLDER_ENDPOINT = '<from createSandbox>';

// ══════════════════════════════════════════════════════════════════════════════
//  USAGE
// ══════════════════════════════════════════════════════════════════════════════

export const USAGE = `usage: yarn workspace @underload/nexus start:daytona [flags]
     (or: npx vite-node scripts/start-daytona.ts [flags])

  spawns a Daytona sandbox, hands it to scripts/deploy-daytona.mjs, and prints the
  hub URL. Three modes, derived from the flags (never combined):
    fresh     default            createSandbox → deploy → hub URL
    reuse     --endpoint <url>   deploy into an endpoint you already own
    teardown  --teardown         deploy --teardown (needs --endpoint)

  sandbox creation (fresh mode only — illegal together with --endpoint):
    --api-key <key>   Daytona API key (else DAYTONA_API_KEY, else @config/vault)
    --ttl <minutes>   server-side kill fence; DEFAULT = unset = bills until delete
    --public          tokenless preview URLs (default)
    --private         token-authenticated preview URLs (last of --public/--private wins)
    --snapshot <n>    base snapshot to boot from (default: the service's own)
    --cpu <n>         vCPU, >= 1 (default 4)
    --mem <gb>        GiB RAM, >= 1 (default 8)
    --disk <gb>       GiB disk, >= 1 (default 10)

  targeting (reuse / teardown modes ONLY — a fresh run has no endpoint to target):
    --endpoint <url>  the sandbox RUNNER preview URL (management port 7777)
    --token <t>       that endpoint's preview token, if it is private; the child
                      sends it as the x-daytona-preview-token header. A FRESH run
                      takes its token from createSandbox instead, so --token without
                      --endpoint is an error.

  deploy passthrough — forwarded verbatim to scripts/deploy-daytona.mjs:
    --mode <m>        local (default) | remote-build
    --port <n>        port the HUB listens on inside the sandbox (default ${DEFAULT_HUB_PORT});
                      also the port the hub URL is minted for (getPreviewLink)
    --env K=V         extra runtime env var; repeatable
    --no-build        ship an already-built artifact instead of running cargo
    --artifact <path> ship exactly this file (implies --no-build)
    --no-wait         return after the deploy is accepted, without polling /status

  behavior:
    --keep-on-fail    fresh mode: leave the sandbox running when the deploy fails
                      (it keeps BILLING — remember to delete it)
    --dry-run         print the plan; no createSandbox, no spawn, no network
    -h, --help        this text

  signals (fresh mode only):
    Ctrl-C / SIGTERM from the moment the sandbox exists until the run ends DELETES
    that sandbox (best effort, capped at 10s) and exits 130 for SIGINT, 143 for
    SIGTERM. Reuse, teardown and dry-run install no handlers — that sandbox was
    never ours to delete.

  billing: a fresh sandbox is created per call and never deleted on success; only
  --ttl or an explicit delete stops the meter. On deploy failure the sandbox IS
  deleted unless --keep-on-fail. Reuse/teardown never touch a sandbox's lifecycle.`;

// ══════════════════════════════════════════════════════════════════════════════
//  FLAGS — pure parser: it returns errors instead of printing or exiting
// ══════════════════════════════════════════════════════════════════════════════

/** Which mode a flag set means. Derived in ONE place so the rules are testable. */
export type StartMode = 'fresh' | 'reuse' | 'teardown';

/** Every flag value after parsing; defaults are the values the underlying tools already use. */
export type StartDaytonaFlags = {
    help: boolean;
    dryRun: boolean;
    teardown: boolean;
    keepOnFail: boolean;
    // ── sandbox creation (fresh) ──
    /** '' = not given → the service falls back to DAYTONA_API_KEY, then @config/vault. */
    apiKey: string;
    /** undefined = NO wall-clock fence (the billing caveat in the header). */
    ttl?: number;
    /** undefined = the service's own default snapshot. */
    snapshot?: string;
    cpu?: number;
    mem?: number;
    disk?: number;
    /** true = tokenless preview URLs (createSandbox's default); --private flips it. */
    public: boolean;
    // ── targeting (reuse / teardown) ──
    /** '' = not given → fresh mode. */
    endpoint: string;
    token: string;
    // ── deploy passthrough ──
    mode: 'local' | 'remote-build';
    /** The HUB's port inside the sandbox — not the runner's 7777. See buildSandboxOptions. */
    port: number;
    env: Record<string, string>;
    noBuild: boolean;
    artifact: string;
    wait: boolean;
};

/** Parsed flags plus the problems found — main() decides how to report them. */
export type ParsedStartArgs = { flags: StartDaytonaFlags; errors: string[] };

/** Numeric flags and the range each accepts — same bounds/wording as the service CLI (setup-sandbox.ts:553-562). */
const NUMERIC_FLAGS: Record<string, { min: number; max?: number; unit: string }> = {
    // 0 is meaningful for ttl ("no fence"), unlike the resource knobs.
    '--ttl': { min: 0, unit: 'minutes, >= 0 (0 = disabled)' },
    '--cpu': { min: 1, unit: 'vCPU, >= 1' },
    '--mem': { min: 1, unit: 'GiB, >= 1' },
    '--disk': { min: 1, unit: 'GiB, >= 1' },
    // The hub port: validated HERE so a bad value is a usage error, not a
    // deploy-script death after a multi-minute cargo build.
    '--port': { min: 1, max: 65535, unit: 'TCP port 1-65535' },
};

/** Range check with the service's exact message shape (setup-sandbox.ts:575-579). */
const numericError = (label: string, value: unknown, spec: { min: number; max?: number; unit: string }): string | undefined => {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || !Number.isInteger(parsed) || parsed < spec.min || (spec.max !== undefined && parsed > spec.max))
        return `${label} ${JSON.stringify(value)} is out of range (${spec.unit})`;
    return undefined;
};

/**
 * Strict flat-flag parser in the deploy script's shape (deploy-daytona.mjs:130-190):
 * `--env` repeats and accumulates, a `--`-leading token after a value flag counts
 * as a missing value, unknown flags are errors, and LAST flag wins for the
 * --public/--private pair (the service's own rule, setup-sandbox.ts:581-592).
 * It never throws and never prints — the errors it returns are reported by main().
 */
export const parseArgs = (argv: readonly string[]): ParsedStartArgs => {
    const flags: StartDaytonaFlags = {
        help: false,
        dryRun: false,
        teardown: false,
        keepOnFail: false,
        apiKey: '',
        public: true,
        endpoint: '',
        token: '',
        mode: 'local',
        port: DEFAULT_HUB_PORT,
        env: {},
        noBuild: false,
        artifact: '',
        wait: true,
    };
    const errors: string[] = [];
    // Which creation flags were ACTUALLY typed — needed to name the offenders in the
    // "--endpoint + creation flag" conflict message instead of listing all eight. The
    // set is exactly: --api-key --ttl --public --private --snapshot --cpu --mem --disk,
    // and each of those call sites registers itself here (targeting/deploy flags do not).
    const creationGiven: string[] = [];
    const given = (flag: string): void => {
        if (!creationGiven.includes(flag)) creationGiven.push(flag);
    };
    for (let i = 0; i < argv.length; i++) {
        const flag = argv[i]!;
        // Value flags share one branch so the "needs a value" wording stays identical.
        if (flag === '--api-key' || flag === '--ttl' || flag === '--snapshot' || flag === '--cpu' || flag === '--mem' || flag === '--disk' || flag === '--endpoint' || flag === '--token' || flag === '--mode' || flag === '--port' || flag === '--env' || flag === '--artifact') {
            const value = argv[++i];
            if (value === undefined || value.startsWith('--')) {
                errors.push(`${flag} needs a value`);
                continue;
            }
            if (flag === '--endpoint') flags.endpoint = value;
            else if (flag === '--token') flags.token = value;
            else if (flag === '--api-key') {
                flags.apiKey = value;
                given(flag);
            } else if (flag === '--snapshot') {
                flags.snapshot = value;
                given(flag);
            } else if (flag === '--artifact') flags.artifact = value;
            else if (flag === '--mode') flags.mode = value as StartDaytonaFlags['mode'];
            else if (flag === '--env') {
                // Split on the FIRST '=' so values may contain '=' (base64, K=a=b).
                const eq = value.indexOf('=');
                if (eq <= 0) errors.push(`--env expects KEY=VALUE, got "${value}"`);
                else flags.env[value.slice(0, eq)] = value.slice(eq + 1);
            } else {
                // --ttl / --cpu / --mem / --disk / --port
                const spec = NUMERIC_FLAGS[flag]!;
                const message = numericError(flag, value, spec);
                if (message !== undefined) errors.push(message);
                else if (flag === '--ttl') {
                    flags.ttl = Number(value);
                    given(flag);
                } else if (flag === '--cpu') {
                    flags.cpu = Number(value);
                    given(flag);
                } else if (flag === '--mem') {
                    flags.mem = Number(value);
                    given(flag);
                } else if (flag === '--disk') {
                    flags.disk = Number(value);
                    given(flag);
                } else flags.port = Number(value);
            }
            continue;
        }
        if (flag === '--public') {
            flags.public = true;
            given(flag);
        } else if (flag === '--private') {
            flags.public = false;
            given(flag);
        } else if (flag === '--no-build') flags.noBuild = true;
        else if (flag === '--no-wait') flags.wait = false;
        else if (flag === '--teardown') flags.teardown = true;
        else if (flag === '--dry-run') flags.dryRun = true;
        else if (flag === '--keep-on-fail') flags.keepOnFail = true;
        else if (flag === '-h' || flag === '--help') flags.help = true;
        else errors.push(`unknown flag "${flag}"`);
    }
    // --help wins outright: a broken command line still gets the usage screen.
    if (flags.help) return { flags, errors };
    if (flags.mode !== 'local' && flags.mode !== 'remote-build') {
        errors.push(`--mode must be "local" or "remote-build", got "${flags.mode}"`);
    }
    // Mirrors the deploy script's own rule (deploy-daytona.mjs:179-184) so the
    // contradiction dies BEFORE a sandbox is paid for.
    if (flags.mode !== 'local' && (flags.noBuild || flags.artifact)) {
        errors.push('--no-build and --artifact apply to --mode local only (remote-build ships a source tarball)');
    }
    // teardown needs an endpoint: a FRESH sandbox has an empty slot, so creating one
    // only to tear it down would burn minutes and delete nothing.
    if (flags.teardown && !flags.endpoint) {
        errors.push('--teardown needs --endpoint <url> — a fresh sandbox has an empty runtime slot, and this script never tears down a sandbox it did not create');
    }
    // Sandbox-creation knobs + --endpoint contradict each other: an endpoint already
    // names a sandbox, so there is nothing left to configure.
    if (flags.endpoint && creationGiven.length) {
        errors.push(`${creationGiven.join(', ')} only apply when this script creates the sandbox — drop --endpoint (fresh) or drop ${creationGiven.length > 1 ? 'them' : 'it'} (reusing ${flags.endpoint})`);
    }
    // The mirror case (D2): --token is TARGETING, and a fresh run has nothing to
    // target. The real fresh run always authenticates with the token createSandbox
    // returns, so a typed --token would be silently ignored by the run while
    // buildPlan would happily print a child command that can never happen. Making it
    // a parse error keeps --dry-run honest. Gated on resolveMode so `--teardown
    // --token t` (no endpoint) reports only the missing --endpoint.
    if (resolveMode(flags) === 'fresh' && flags.token) {
        errors.push('--token only applies when you reuse an endpoint — a fresh run authenticates with the token createSandbox returns; drop --token or add --endpoint <url>');
    }
    return { flags, errors };
};

/** Mode resolution, the single source of truth for "whose sandbox is this". */
export const resolveMode = (flags: Pick<StartDaytonaFlags, 'teardown' | 'endpoint'>): StartMode => (flags.teardown ? 'teardown' : flags.endpoint ? 'reuse' : 'fresh');

// ══════════════════════════════════════════════════════════════════════════════
//  PLAN BUILDERS — pure data in, data out (asserted exactly by the test suite)
// ══════════════════════════════════════════════════════════════════════════════

/**
 * Maps orchestrator flags onto createSandbox's option shape (sandbox/index.ts:8-12).
 * Only PRESENT knobs are set, so every default stays the service's own — and
 * `port` is deliberately NEVER set: createSandbox's `port` is the RUNNER's port
 * (7777, the management endpoint), while our `--port` is the HUB's port. Passing it
 * through would repoint the management endpoint at the hub and break the deploy.
 */
export const buildSandboxOptions = (flags: StartDaytonaFlags): CreateSandboxOptions => ({
    // '' means "not given" (the parser's absent marker) → env/vault fallback.
    apiKey: flags.apiKey || undefined,
    ttlMinutes: flags.ttl,
    public: flags.public,
    snapshot: flags.snapshot,
    cpu: flags.cpu,
    mem: flags.mem,
    disk: flags.disk,
});

/** The endpoint + token a deploy run is pointed at (fresh fills these in after createSandbox). */
export type DeployTarget = { endpoint: string; token?: string };

/**
 * The exact argv handed to the child. Ordering is fixed and documented so the
 * tests assert the whole array:
 *   --endpoint, --token?, --mode, --port, --env…, --no-build?, --artifact?, --no-wait?, --teardown?
 * `--token` is OMITTED when empty — the deploy script sends no auth header without
 * it (deploy-daytona.mjs:133-135, 225-229), which is what keeps public endpoints working.
 * Teardown carries ONLY endpoint/token: mode, port and env are irrelevant to a
 * DELETE /, and forwarding them would imply they do something.
 */
export const buildDeployArgs = (flags: StartDaytonaFlags, target: DeployTarget, mode: StartMode): string[] => {
    const args = ['--endpoint', target.endpoint];
    if (target.token) args.push('--token', target.token);
    if (mode === 'teardown') {
        args.push('--teardown');
        return args;
    }
    args.push('--mode', flags.mode, '--port', String(flags.port));
    // Object insertion order == the order --env was typed (JS string-key guarantee).
    for (const [key, value] of Object.entries(flags.env)) args.push('--env', `${key}=${value}`);
    if (flags.noBuild) args.push('--no-build');
    if (flags.artifact) args.push('--artifact', flags.artifact);
    // The child defaults to waiting; only the deviation has to be spelled out.
    if (!flags.wait) args.push('--no-wait');
    return args;
};

/** A fully-resolved child-process call: what to run, with what, from where. */
export type DeployInvocation = {
    /** Plain `node` — the script is dependency-free, so the ambient runtime is fine. */
    command: string;
    /** Full child argv: args[0] is DEPLOY_SCRIPT_PATH, then the deploy flags. */
    args: string[];
    /** The nexus package root: cargo's target-dir config is found by walking up from here. */
    cwd: string;
    /** Passed through UNTOUCHED — cargo needs HTTPS_PROXY etc. from the caller's shell. */
    env: Record<string, string | undefined>;
};

/** Wraps buildDeployArgs with the command, the script path, the cwd and the env that make it runnable. */
export const buildDeployInvocation = (flags: StartDaytonaFlags, target: DeployTarget, mode: StartMode, env: Record<string, string | undefined>): DeployInvocation => ({
    command: 'node',
    // args[0] is the script itself: `spawn('node', args)` then runs exactly what the
    // dry run prints, and the path is resolved from THIS module (never from cwd), so
    // cargo/yarn can be invoked from anywhere.
    args: [DEPLOY_SCRIPT_PATH, ...buildDeployArgs(flags, target, mode)],
    cwd: PACKAGE_ROOT,
    env,
});

/** Everything a dry run needs to show, and what the real run will do with it. */
export type StartPlan = {
    mode: StartMode;
    /** Absent for reuse/teardown — no sandbox is created, so nothing is configured. */
    sandboxOptions?: CreateSandboxOptions;
    invocation: DeployInvocation;
    /** How the hub URL will be obtained (teardown has none). */
    hub?: { port: number; via: string };
};

/**
 * The plan for a flag set. `target` is optional: for a fresh run the endpoint does
 * not exist yet, so the placeholder is printed instead (and never spawned).
 */
export const buildPlan = (flags: StartDaytonaFlags, env: Record<string, string | undefined>, target?: DeployTarget): StartPlan => {
    const mode = resolveMode(flags);
    const resolved: DeployTarget = target ?? { endpoint: mode === 'fresh' ? PLACEHOLDER_ENDPOINT : flags.endpoint, token: flags.token || undefined };
    const plan: StartPlan = {
        mode,
        sandboxOptions: mode === 'fresh' ? buildSandboxOptions(flags) : undefined,
        invocation: buildDeployInvocation(flags, resolved, mode, env),
        hub: mode === 'teardown' ? undefined : { port: flags.port, via: `sandbox.getPreviewLink(${flags.port})` },
    };
    return plan;
};

/**
 * The machine-readable line. Key order is insertion order, and empty tokens are
 * dropped so a consumer's `token`/`hubToken` check means "needs a header" rather
 * than "present but empty". In reuse mode `sandboxId`/`name`/`public` are absent
 * (that sandbox was never ours) and `hubUrl` is explicitly `null` (no handle to
 * mint a per-port preview link with) — a consumer must handle `null`, which is
 * why the key is kept instead of omitted.
 */
export const buildSummary = (input: {
    mode: StartMode;
    endpoint: string;
    token?: string;
    /** null = this run could not mint it (reuse mode); fresh always has one. */
    hubUrl: string | null;
    hubToken?: string;
    sandboxId?: string;
    name?: string;
    public?: boolean;
    hubPort: number;
}): string =>
    JSON.stringify({
        mode: input.mode,
        endpoint: input.endpoint,
        ...(input.token ? { token: input.token } : {}),
        hubUrl: input.hubUrl,
        ...(input.hubToken ? { hubToken: input.hubToken } : {}),
        ...(input.sandboxId ? { sandboxId: input.sandboxId } : {}),
        ...(input.name ? { name: input.name } : {}),
        ...(input.public === undefined ? {} : { public: input.public }),
        hubPort: input.hubPort,
    });

// ══════════════════════════════════════════════════════════════════════════════
//  INTERRUPT POLICY — pure decision first, then the thin signal wiring that uses it
// ══════════════════════════════════════════════════════════════════════════════

/** The signals trapped while this process owns a BILLING sandbox. */
export type StartSignal = 'SIGINT' | 'SIGTERM';

/** How far a fresh run got — decides whether an interrupt owes a sandbox delete. */
export type InterruptPhase = 'creating' | 'deploying' | 'minting';

/** What an interrupt is worth: the exit code to use, and whether a delete is owed. */
export type InterruptDecision = {
    /** true = this process holds the only handle to a live sandbox, so it must try to delete it. */
    shouldDelete: boolean;
    /** Shell convention for signal deaths: 128 + signo, so a wrapper can tell them apart. */
    exitCode: number;
};

/** 128 + signal number: SIGINT(2) → 130, SIGTERM(15) → 143. */
const SIGNAL_EXIT_CODES: Record<StartSignal, number> = { SIGINT: 130, SIGTERM: 143 };

/** Hard cap on the interrupt-time delete: Ctrl-C must never trade a sandbox for a hung terminal. */
export const INTERRUPT_DELETE_TIMEOUT_MS = 10_000;

/**
 * THE decision, with no I/O, so it can be pinned exactly. Three ways an interrupt
 * is NOT allowed to delete:
 *   · not fresh mode — reuse/teardown were handed someone else's endpoint;
 *   · no `created` handle — there is nothing to send the DELETE to;
 *   · phase 'creating' — createSandbox has not returned yet, and its own failure
 *     path already deletes on a failed bring-up, so deleting here would double-fire.
 * Anything else (deploying/minting with a handle in hand) is a sandbox this command
 * created seconds ago that would otherwise bill forever.
 */
export const decideInterruptCleanup = (input: {
    mode: StartMode;
    created?: { sandboxId: string };
    phase: InterruptPhase;
    signal: StartSignal;
}): InterruptDecision => ({
    shouldDelete: input.mode === 'fresh' && input.created !== undefined && input.phase !== 'creating',
    exitCode: SIGNAL_EXIT_CODES[input.signal],
});

/** Real bounded wait. The timer is unref'd so it can never be the thing keeping the process alive. */
const timeoutAfter = (ms: number): Promise<'timeout'> =>
    new Promise((resolveTimeout) => {
        const timer = setTimeout(() => resolveTimeout('timeout'), ms);
        if (typeof timer === 'object' && typeof timer.unref === 'function') timer.unref();
    });

/** Real signal wiring; hands back the dispose so the happy path un-traps everything. */
const onProcessSignal = (signal: StartSignal, listener: () => void): (() => void) => {
    process.on(signal, listener);
    return () => process.off(signal, listener);
};

/** A trapped-signal window: a promise that ONLY settles on a signal, plus its teardown. */
type InterruptGuard = {
    /** Resolves with 130/143 when a signal lands; otherwise it just never settles. */
    promise: Promise<number>;
    /** true from the first signal on — the flow stops doing lifecycle work once set. */
    interrupted: () => boolean;
    /** The exit code the signal dictates, or undefined while no signal has landed. */
    code: () => number | undefined;
    /** Removes the listeners (called in a finally, so a successful run leaves the process's default Ctrl-C behavior intact). */
    dispose: () => void;
};

/**
 * The whole signal wiring, deliberately thin: it registers the two handlers, and on
 * a signal it asks decideInterruptCleanup what is owed, pays the delete best effort
 * within INTERRUPT_DELETE_TIMEOUT_MS, prints the outcome, and settles the race with
 * the signal's exit code. Nothing here decides policy by itself.
 */
const armInterruptGuard = (input: {
    mode: StartMode;
    created: SandboxHandle;
    /** Live read of where the flow is, so a signal during create is classified correctly. */
    phase: () => InterruptPhase;
    deps: StartDaytonaDeps;
    log: (line: string) => void;
    error: (line: string) => void;
}): InterruptGuard => {
    const { mode, created, phase, deps, log, error } = input;
    const onSignal = deps.onSignal ?? onProcessSignal;
    const deleteTimeout = deps.deleteTimeout ?? timeoutAfter;
    let interrupted = false;
    let signalCode: number | undefined;
    let settle!: (code: number) => void;
    const promise = new Promise<number>((resolveSignal) => {
        settle = resolveSignal;
    });
    const handle = (signal: StartSignal): void => {
        void (async () => {
            // FIRST signal wins. A second Ctrl-C while the delete is still in flight
            // must not fire a second delete nor change the exit code being reported.
            if (interrupted) return;
            interrupted = true;
            const decision = decideInterruptCleanup({ mode, created, phase: phase(), signal });
            // Recorded BEFORE any await: the flow reads this to make the exit code
            // deterministic even when it finishes the race first.
            signalCode = decision.exitCode;
            log(`${PREFIX} ${signal} received — ${decision.shouldDelete ? `deleting sandbox ${created.sandboxId} before exit` : 'no sandbox of ours to clean up'}`);
            if (decision.shouldDelete) {
                // Racing the delete against a timer covers BOTH failure modes at once:
                // a throwing SDK call and one that simply never returns.
                const deleted = await Promise.race([
                    Promise.resolve()
                        .then(() => created.sandbox.delete())
                        .then(
                            () => true,
                            () => false,
                        ),
                    deleteTimeout(INTERRUPT_DELETE_TIMEOUT_MS).then(() => false),
                ]);
                if (deleted) log(`${PREFIX} interrupted — sandbox ${created.sandboxId} deleted`);
                else error(`${PREFIX} interrupted — sandbox ${created.sandboxId} DELETE FAILED (bills until manual cleanup)`);
            }
            settle(decision.exitCode);
        })();
    };
    const disposers = (['SIGINT', 'SIGTERM'] as const).map((signal) => onSignal(signal, () => handle(signal)));
    return {
        promise,
        interrupted: () => interrupted,
        code: () => signalCode,
        dispose: () => disposers.forEach((dispose) => dispose()),
    };
};

// ══════════════════════════════════════════════════════════════════════════════
//  ORCHESTRATION CORE — all side effects come from the injected deps
// ══════════════════════════════════════════════════════════════════════════════

/**
 * The slice of CreateSandboxResult this script touches, written as a structural
 * type so a test fake can satisfy it without implementing the Daytona SDK class.
 * The real result (setup-sandbox.ts:1456-1483) is assignable to it.
 */
export type SandboxHandle = {
    endpoint: string;
    token?: string;
    sandboxId: string;
    name?: string;
    public?: boolean;
    sandbox: {
        delete: () => Promise<unknown>;
        getPreviewLink: (port: number) => Promise<{ url: string; token?: string }>;
    };
};

/** Injectable side effects — the seams the test suite drives. */
export type StartDaytonaDeps = {
    /** real: @services/daytona/sandbox's createSandbox. */
    createSandbox: (options: CreateSandboxOptions) => Promise<SandboxHandle>;
    /** real: child_process.spawn with stdio inherit; resolves the child's exit code. */
    spawn: (invocation: DeployInvocation) => Promise<number>;
    /** stdout sink. Default console.log. */
    log?: (line: string) => void;
    /** stderr sink (usage, failures, billing warnings). Default console.error. */
    error?: (line: string) => void;
    /** env handed to the child. Default process.env, untouched. */
    env?: Record<string, string | undefined>;
    /**
     * Signal wiring for the fresh-mode interrupt fence. Real: process.on/off, and the
     * returned dispose un-traps. Injected so a test can fire SIGINT mid-deploy.
     */
    onSignal?: (signal: StartSignal, listener: () => void) => () => void;
    /**
     * Bounded wait used by the interrupt delete. Real: an unref'd 10s timer. Injected
     * so the "delete never comes back" branch is testable without waiting or fake clocks.
     */
    deleteTimeout?: (ms: number) => Promise<'timeout'>;
};

/**
 * Runs the whole flow and returns the process exit code (0 = hub is up / plan
 * printed). Never calls process.exit itself, so a caller can wrap it.
 */
export const runStartDaytona = async (flags: StartDaytonaFlags, deps: StartDaytonaDeps): Promise<number> => {
    const log = deps.log ?? ((line: string) => console.log(line));
    const error = deps.error ?? ((line: string) => console.error(line));
    const env = deps.env ?? process.env;
    const mode = resolveMode(flags);

    // ── dry run: plan only, and provably nothing else (no createSandbox, no spawn) ──
    if (flags.dryRun) {
        const plan = buildPlan(flags, env);
        log(`${PREFIX} DRY RUN — no sandbox is created, no script is spawned, no network call`);
        log(`${PREFIX} mode: ${plan.mode}`);
        if (plan.sandboxOptions) {
            log(`${PREFIX} sandbox options that WOULD be used:`);
            // JSON.stringify drops the undefined knobs, so the print is the effective config.
            log(`  ${JSON.stringify(plan.sandboxOptions, null, 2).split('\n').join('\n  ')}`);
            log(`${PREFIX} (unset knobs keep the @services/daytona defaults; --ttl unset means NO kill fence)`);
        }
        log(`${PREFIX} deploy command that WOULD run (cwd ${plan.invocation.cwd}):`);
        log(`  ${plan.invocation.command} ${plan.invocation.args.join(' ')}`);
        if (plan.hub) log(`${PREFIX} hub URL after success: ${plan.hub.via}`);
        return 0;
    }

    // ── teardown: free the runtime slot; the sandbox itself keeps billing ──
    if (mode === 'teardown') {
        log(`${PREFIX} tearing down the runtime slot at ${flags.endpoint} (DELETE / — the SANDBOX is not deleted)`);
        const code = await deps.spawn(buildDeployInvocation(flags, { endpoint: flags.endpoint, token: flags.token || undefined }, mode, env));
        if (code !== 0) error(`${PREFIX} ✗ teardown failed (exit ${code}) — see the deploy output above`);
        return code;
    }

    // ── reuse: someone else's sandbox, so no lifecycle work ever ──
    if (mode === 'reuse') {
        log(`${PREFIX} deploying into the existing endpoint ${flags.endpoint} (not created here — no sandbox lifecycle actions)`);
        const code = await deps.spawn(buildDeployInvocation(flags, { endpoint: flags.endpoint, token: flags.token || undefined }, mode, env));
        if (code === 0) {
            // NO hub URL here, deliberately: the hub listens on its own port inside
            // that sandbox, and the only way to mint its preview link is the SDK
            // handle (getPreviewLink) — which createSandbox owns and reuse mode
            // never had. Guessing the URL from the endpoint (port substitution)
            // would be worse than printing null, because the per-port preview token
            // cannot be guessed either.
            log(`${PREFIX} ✓ deploy ready on ${flags.endpoint}`);
            log(`${PREFIX}   hub URL (clients connect here): not minted — reuse mode holds no sandbox handle; run getPreviewLink(${flags.port}) on the owning sandbox`);
            log(`${PREFIX}   management endpoint (POST/status/logs): ${flags.endpoint}`);
            // LAST line, single-line JSON; `hubUrl: null` = "not minted here" (header).
            log(buildSummary({ mode, endpoint: flags.endpoint, token: flags.token || undefined, hubUrl: null, hubPort: flags.port }));
        } else error(`${PREFIX} ✗ deploy failed (exit ${code}) — the deploy output above is the diagnosis; nothing was created, so nothing to clean up`);
        return code;
    }

    // ── fresh: create → deploy → mint the hub URL ──
    // createSandbox is QUIET by default, and a bring-up takes minutes, so progress is
    // forwarded through the prefixed sink (multi-line chunks are indented, matching
    // the CLI sink's treatment of raw Daytona build output, setup-sandbox.ts:420-421).
    log(`${PREFIX} creating a Daytona sandbox (minutes; --ttl unset = it bills until deleted)…`);
    // The (message, level) level is deliberately dropped: createSandbox routes the raw
    // Daytona build output through the same callback, so level-based filtering would
    // hide exactly the progress an operator wants while a create takes minutes.
    const sandboxLog: SandboxLog = (message) => log(`${PREFIX}   ${String(message).replace(/\n+/g, '\n  ')}`);
    const created = await deps.createSandbox({ ...buildSandboxOptions(flags), log: sandboxLog });
    log(`${PREFIX} ✓ sandbox ${created.sandboxId}${created.name ? ` (${created.name})` : ''} ready — management endpoint ${created.endpoint}`);

    // ── R1 — the interrupt fence (fresh mode ONLY, and only for the window in which
    // this process really owns a BILLING sandbox: created → flow end). Before this,
    // Ctrl-C killed parent+child and left the sandbox running forever with no delete,
    // no summary and no TTL. Handlers come off in the finally below, so a completed
    // run leaves Node's default Ctrl-C behavior intact for whatever runs next.
    const phase: { current: InterruptPhase } = { current: 'deploying' };
    const guard = armInterruptGuard({ mode, created, phase: () => phase.current, deps, log, error });

    // Everything after the create, as ONE unit the guard can abandon mid-flight.
    const finish = async (): Promise<number> => {
        const code = await deps.spawn(buildDeployInvocation(flags, { endpoint: created.endpoint, token: created.token }, mode, env));
        // A signal during the deploy means the guard already owns the cleanup; running
        // the deploy-failure delete here too would delete twice and print a lie.
        if (guard.interrupted()) return code;
        if (code !== 0) {
            // The child already printed everything it knows (including /logs on a failed
            // slot), so no re-diagnosis here — just the lifecycle decision.
            error(`${PREFIX} ✗ deploy failed (exit ${code}) — the deploy output above is the diagnosis`);
            if (flags.keepOnFail) {
                error(`${PREFIX} --keep-on-fail: sandbox ${created.sandboxId} is LEFT RUNNING and BILLING — delete it when you are done`);
            } else {
                try {
                    await created.sandbox.delete();
                    log(`${PREFIX} ✓ deleted sandbox ${created.sandboxId} (a failed deploy does not get to keep burning)`);
                } catch (deleteError) {
                    // The one failure mode where money keeps leaking — say it loudly.
                    error(`${PREFIX} ✗ could NOT delete sandbox ${created.sandboxId}: ${describe(deleteError)}`);
                    error(`${PREFIX}   it keeps BILLING until deleted: sandbox.delete() or the @services/daytona \`down\` verb`);
                }
            }
            return code;
        }

        // The hub listens on its OWN port inside the sandbox, so its URL is a separate
        // per-port preview link — possibly with a token of its own when private.
        phase.current = 'minting';
        let hubUrl: string;
        let hubToken: string | undefined;
        try {
            const link = await created.sandbox.getPreviewLink(flags.port);
            hubUrl = link.url;
            hubToken = link.token || undefined;
        } catch (mintError) {
            // The deploy SUCCEEDED: the hub is running and the sandbox is paid for, so
            // deleting it here would destroy the very thing the command was for.
            error(`${PREFIX} ✗ deploy is ready but the hub preview URL could not be minted: ${describe(mintError)}`);
            error(`${PREFIX}   sandbox ${created.sandboxId} LEFT RUNNING (billing) — mint getPreviewLink(${flags.port}) yourself`);
            error(`${PREFIX}   management endpoint, still usable: ${created.endpoint}`);
            return 1;
        }

        // An interrupt that landed while the URL was being minted still wins: the run
        // is about to delete the sandbox and exit 130/143, so printing a success line
        // and the machine-readable contract would send a consumer to a dead sandbox.
        if (guard.interrupted()) return 0;

        log(`${PREFIX} ✓ nexus hub is live`);
        // The hub's preview link is authenticated with the SAME header Daytona uses for
        // every preview URL (setup-sandbox.ts `PREVIEW_TOKEN_HEADER`, deploy-daytona.mjs:225)
        // — naming a header that does not exist sends operators on a wild goose chase.
        log(`${PREFIX}   hub URL (clients connect here)${hubToken ? ' [x-daytona-preview-token required]' : ' [public, no token]'}: ${hubUrl}`);
        log(`${PREFIX}   management endpoint (POST/status/logs)${created.token ? ' [x-daytona-preview-token]' : ' [no token]'}: ${created.endpoint}`);
        log(`${PREFIX}   sandbox ${created.sandboxId} — bills until sandbox.delete()${flags.ttl === undefined ? ' (no --ttl fence was set)' : ` or its ${flags.ttl}-minute TTL fires`}`);
        if (!flags.wait) log(`${PREFIX}   (--no-wait: the deploy was accepted, the hub may still be booting — probe the hub URL)`);
        // LAST line, single-line JSON — the machine-readable contract in the header.
        log(
            buildSummary({
                mode,
                endpoint: created.endpoint,
                token: created.token,
                hubUrl,
                hubToken,
                sandboxId: created.sandboxId,
                name: created.name,
                public: created.public,
                hubPort: flags.port,
            }),
        );
        return 0;
    };

    try {
        // The guard only settles on a signal, so the deploy normally wins the race.
        // The signal's code is then read from the guard rather than taken from the
        // race, because a signal that lands while finish() is unwinding would
        // otherwise lose a coin-flip on microtask ordering — the operator interrupted
        // this run, so 130/143 must win regardless.
        const raced = await Promise.race([finish(), guard.promise]);
        // A signal in flight means the cleanup (and its outcome line) is still
        // running; waiting for it is bounded by the delete timeout and keeps the
        // billing warning from being truncated by the exit.
        if (guard.interrupted()) await guard.promise;
        return guard.code() ?? raced;
    } finally {
        guard.dispose();
    }
};

/** Error → one-line text, so a failure never prints a bare [object Object]. */
const describe = (error: unknown): string => (error instanceof Error ? error.message : String(error));

// ══════════════════════════════════════════════════════════════════════════════
//  REAL DEPS + main()
// ══════════════════════════════════════════════════════════════════════════════

/**
 * The slice of child_process.spawn this module needs, as an injectable seam.
 * WHY A SEAM INSTEAD OF `vi.mock('node:child_process')`: in this repo's vitest setup
 * a builtin mock rebinds the binding for the TEST FILE but leaves the module under
 * test calling the real builtin, so a mocked spawn would never be seen by the code
 * being pinned (measured: the real deploy child really did start, doing a DELETE
 * against the fake endpoint). An explicit default-parameter seam is the only way to
 * exercise the child wiring — argv, cwd, stdio, env identity, exit-code mapping —
 * without starting a cargo build.
 */
export type SpawnChild = {
    on: (event: 'error' | 'close', listener: (arg?: unknown) => void) => unknown;
};

/** Injected spawner: same shape as `child_process.spawn` for the arguments we pass. */
export type SpawnImpl = (command: string, args: readonly string[], options: { cwd: string; env: Record<string, string | undefined>; stdio: 'inherit' }) => SpawnChild;

/**
 * The real child-process dep: stdio INHERIT so the deploy script's `>>>`/`✓`
 * progress and its final /status JSON reach the terminal untouched, and the child's
 * exit code becomes this command's exit code. A spawn-level failure (no node on
 * PATH) resolves 1 rather than rejecting, so the caller still exits 1 with a line.
 */
export const spawnDeploy = (invocation: DeployInvocation, spawnImpl: SpawnImpl = spawn): Promise<number> =>
    new Promise<number>((resolveRun) => {
        const child = spawnImpl(invocation.command, invocation.args, { cwd: invocation.cwd, env: invocation.env, stdio: 'inherit' });
        child.on('error', (cause) => {
            console.error(`${PREFIX} ✗ could not start ${DEPLOY_SCRIPT_NAME}: ${describe(cause)}`);
            resolveRun(1);
        });
        // Signal-killed children report code null (and a shim may report undefined);
        // anything that is not a number becomes 1 so "non-zero = failed" always holds.
        child.on('close', (code) => resolveRun(typeof code === 'number' ? code : 1));
    });

/** The default dep set: the real library call and the real child process. */
export const createDefaultDeps = (): StartDaytonaDeps => ({
    createSandbox: (options) => createSandbox(options),
    spawn: spawnDeploy,
});

/**
 * Parses, validates, runs, and maps the result onto an exit code. Returns it
 * instead of calling process.exit so the flow stays testable (the entry guard at
 * the bottom is the only place that touches process state).
 */
export const main = async (argv: readonly string[] = process.argv.slice(2), deps: StartDaytonaDeps = createDefaultDeps()): Promise<number> => {
    const log = deps.log ?? ((line: string) => console.log(line));
    const error = deps.error ?? ((line: string) => console.error(line));
    const { flags, errors } = parseArgs(argv);
    if (flags.help) {
        log(USAGE);
        return 0;
    }
    if (errors.length) {
        for (const problem of errors) error(`${PREFIX} ✗ ${problem}`);
        error('');
        error(USAGE);
        return 1;
    }
    try {
        return await runStartDaytona(flags, deps);
    } catch (failure) {
        // createSandbox throws DaytonaCredentialError / CreateSandboxOptionsError by
        // name; both are "your command line / your credentials", so say what to fix.
        error(`${PREFIX} ✗ ${describe(failure)}`);
        return 1;
    }
};

// ══════════════════════════════════════════════════════════════════════════════
//  ENTRY POINT — importing this module must never start anything
// ══════════════════════════════════════════════════════════════════════════════
//
//  Copied in shape from the `ENTRY POINT` block of services/daytona/sandbox/
//  setup-sandbox.ts, because the same constraint applies here: `yarn start:daytona`
//  must auto-run, `vitest` importing this file must not. Rule 3 exists because a
//  PLAIN `vite-node <file>` rewrites process.argv and DROPS the file (measured in
//  that file's own entry-point comment), so neither argv nor the lifecycle line can
//  see the entry. A stack probe is only evidence inside vite-node's module runner,
//  which is exactly where that gap is.

/** Extension-bearing token that identifies THIS file as the runnable script. */
const ENTRY_SCRIPT_PATTERN = /(?:^|[/\\])start-daytona\.[cm]?[jt]sx?$/;
/** vite/vite-node append module ids and query strings; strip them before matching. */
const stripModuleQuery = (token: string): string => String(token).split('?')[0].split('#')[0];
const isScriptToken = (token: unknown): boolean => ENTRY_SCRIPT_PATTERN.test(stripModuleQuery(String(token)));

/** Signals the entry detector reads — every field is process state, gathered once at import. */
export type EntryPointSignals = {
    argv?: readonly string[];
    env?: Readonly<Record<string, string | undefined>>;
    modulePath?: string;
    /** Only meaningful while this module is still being evaluated (rule 3). */
    stack?: string;
    /** true under vitest, where nothing may run. */
    vitest?: boolean;
};

/** Rule 1: argv's SCRIPT POSITIONAL is this file (`node|tsx … start-daytona.ts`, `--script` form). */
const namesScriptPositional = (argv: readonly string[] | undefined): boolean => {
    const tokens = argv ?? [];
    if (isScriptToken(tokens[1])) return true;
    const flag = tokens.indexOf('--script');
    return flag !== -1 && isScriptToken(tokens[flag + 1]);
};

/** Rule 2: the npm/yarn lifecycle line names it (`start:daytona` = "vite-node scripts/start-daytona.ts"). */
const namesLifecycleEntryPoint = (env: Readonly<Record<string, string | undefined>> | undefined): boolean =>
    String(env?.npm_lifecycle_script ?? '')
        .split(/\s+/)
        .some(isScriptToken);

/** Rule 3: vite-node evaluated THIS file as its entry — only runner frames sit above it. */
const isRunnerEntryPoint = (stack: string | undefined, modulePath: string | undefined): boolean => {
    if (!stack || !modulePath) return false;
    const frames = stack.split('\n').map((frame) => frame.trim().replace('file://', ''));
    if (!frames.some((frame) => frame.includes('ViteNodeRunner'))) return false;
    const own = frames.reduce((found, frame, index) => (frame.includes(modulePath) ? index : found), -1);
    if (own < 0) return false;
    const callers = frames.slice(own + 1);
    const executeFile = callers.findIndex((frame) => frame.includes('ViteNodeRunner.executeFile'));
    const upTo = executeFile === -1 ? callers.length : executeFile;
    return callers.slice(0, upTo).every((frame) => frame.includes('ViteNodeRunner'));
};

/** True when — and only when — this module is the process entry point. */
export const isCliEntryPoint = (signals: EntryPointSignals): boolean => {
    if (signals.vitest) return false;
    if (namesScriptPositional(signals.argv)) return true;
    if (namesLifecycleEntryPoint(signals.env)) return true;
    return isRunnerEntryPoint(signals.stack, signals.modulePath);
};

/** This file's own path (rule 3 needs to identify its own stack frames). */
const MODULE_FILE_PATH = (() => {
    try {
        return fileURLToPath(import.meta.url);
    } catch {
        // vitest may hand a non-file import.meta.url (see SCRIPTS_DIR_CANDIDATES).
        return join(SCRIPTS_DIR, 'start-daytona.ts');
    }
})();

// The stack must be captured during module evaluation; inside main() the evidence
// for rule 3 is already gone.
if (isCliEntryPoint({ argv: process.argv, env: process.env, modulePath: MODULE_FILE_PATH, stack: new Error().stack, vitest: Boolean(process.env.VITEST) })) {
    main().then(
        (code) => {
            process.exitCode = code;
        },
        (failure) => {
            console.error(`${PREFIX} ✗ unexpected failure: ${failure instanceof Error ? (failure.stack ?? failure.message) : String(failure)}`);
            process.exitCode = 1;
        },
    );
}
