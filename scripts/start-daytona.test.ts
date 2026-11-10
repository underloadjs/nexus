// ─────────────────────────────────────────────────────────────────────────────
//  start-daytona.test.ts — the orchestrator, end to end, with ZERO network.
//
//  Everything side-effectful in start-daytona.ts sits behind an injected dep
//  (createSandbox / spawn / log / error / env), so these tests drive the REAL
//  control flow with fakes and assert EXACT values:
//    · parser     — defaults, every flag, --env repeats, out-of-range words,
//                   and each illegal combination
//    · plan / argv— the whole deploy argv array per mode, plus command/cwd/env
//    · flow       — fresh success (create once → spawn once → hub URL → the
//                   machine-readable last line), deploy failure (delete, and
//                   --keep-on-fail not deleting, and a delete that itself fails),
//                   reuse (no lifecycle at all), teardown (endpoint/token/--teardown
//                   only), dry-run (no create, no spawn, no fetch), exit codes
//    · interrupt  — decideInterruptCleanup's policy table, and the fresh-mode fence
//                   fired through an injected onSignal: SIGINT/SIGTERM mid-deploy
//                   delete the sandbox and exit 130/143, a hung delete still exits
//                   after the bounded wait, and reuse/teardown/dry-run trap nothing
//    · spawnDeploy— the one real side effect, driven through the injectable spawner
//                   seam (see SpawnImpl in the source for why vi.mock cannot rebind a
//                   builtin here): argv/cwd/env/stdio and the exit-code mapping
//    · import     — isCliEntryPoint's entry rules, i.e. the guarantee that
//                   importing the module starts nothing
//
//  @services/daytona/sandbox is vi.mock'd so the real SDK / @config/vault modules
//  are never loaded here; the only other global touched is `fetch`, stubbed as a
//  tripwire in the dry-run test (root vitest env is jsdom, where fetch is flaky).
//  No cargo build is ever started: the real child process is only ever reached
//  through the injected SpawnImpl seam.
// ─────────────────────────────────────────────────────────────────────────────

import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it, vi } from 'vitest';

// Hoisted above the imports by vitest: the barrel resolves to a spy, not to the real
// SDK, so nothing in this file can reach Daytona even by accident.
vi.mock('@services/daytona/sandbox', () => ({ createSandbox: vi.fn() }));

import { createSandbox } from '@services/daytona/sandbox';
import {
    buildDeployArgs,
    buildPlan,
    buildSandboxOptions,
    buildSummary,
    createDefaultDeps,
    decideInterruptCleanup,
    DEFAULT_HUB_PORT,
    DEPLOY_SCRIPT_PATH,
    INTERRUPT_DELETE_TIMEOUT_MS,
    isCliEntryPoint,
    main,
    PACKAGE_ROOT,
    parseArgs,
    resolveMode,
    runStartDaytona,
    spawnDeploy,
    USAGE,
    type CreateSandboxOptions,
    type DeployInvocation,
    type InterruptPhase,
    type SandboxHandle,
    type SpawnImpl,
    type StartDaytonaDeps,
    type StartDaytonaFlags,
    type StartSignal,
} from './start-daytona';

/** The library spy behind the mocked barrel (see createDefaultDeps below). */
const createSandboxMock = createSandbox as unknown as ReturnType<typeof vi.fn>;

/** Flags exactly as the CLI produces them — one source of truth for the defaults. */
const flagsOf = (argv: readonly string[]): StartDaytonaFlags => parseArgs(argv).flags;

/** The env object handed to every fake; identity is asserted to prove it is untouched. */
const ENV = { PATH: '/usr/bin', HTTPS_PROXY: 'http://proxy:3128' };

/** What a fake createSandbox resolves: a structural SandboxHandle with recorded calls. */
const makeHandle = (overrides: Partial<SandboxHandle> = {}): SandboxHandle & { sandbox: { delete: ReturnType<typeof vi.fn>; getPreviewLink: ReturnType<typeof vi.fn> } } =>
    ({
        endpoint: 'https://proxy.example/runner-7777',
        token: 'dtn_preview_token',
        sandboxId: 'sbx-1234',
        name: 'nexus-1',
        public: false,
        sandbox: {
            delete: vi.fn(async () => undefined),
            getPreviewLink: vi.fn(async (port: number) => ({ url: `https://proxy.example/hub-${port}`, token: 'hub_token' })),
        },
        ...overrides,
    }) as SandboxHandle & { sandbox: { delete: ReturnType<typeof vi.fn>; getPreviewLink: ReturnType<typeof vi.fn> } };

/** A dep set whose every side effect is a spy, plus the captured sinks for assertions. */
const makeDeps = (handle: SandboxHandle | undefined, exitCode = 0) => {
    const lines: string[] = [];
    const problems: string[] = [];
    const invocations: DeployInvocation[] = [];
    const createSpy = vi.fn(async (_options: CreateSandboxOptions) => {
        if (!handle) throw new Error('createSandbox must not run in this mode');
        return handle;
    });
    const spawnSpy = vi.fn(async (invocation: DeployInvocation) => {
        invocations.push(invocation);
        return exitCode;
    });
    const deps: StartDaytonaDeps = { createSandbox: createSpy, spawn: spawnSpy, log: (line) => lines.push(line), error: (line) => problems.push(line), env: ENV };
    return { deps, createSpy, spawnSpy, lines, problems, invocations };
};

/**
 * Deps whose signal wiring is CAPTURED rather than installed, so a test can fire a
 * signal at an exact instant of the flow (the same instant a real Ctrl-C lands) and
 * so `trapped` doubles as the per-mode evidence that reuse/teardown/dry-run arm nothing.
 */
const makeInterruptDeps = (
    handle: SandboxHandle,
    options: {
        /** When the signal is fired: while the deploy child is in flight, or while the hub URL is minted. */
        fireDuring?: 'deploy' | 'mint';
        signal?: StartSignal;
        /** Override sandbox.delete to model a failing or a never-returning delete. */
        deleteImpl?: () => Promise<unknown>;
        /** Override the bounded wait so the timeout branch needs no real 10s. */
        deleteTimeout?: (ms: number) => Promise<'timeout'>;
    } = {},
) => {
    const lines: string[] = [];
    const problems: string[] = [];
    /** signal → live listener. Empty after a run means nothing is left trapped. */
    const trapped = new Map<StartSignal, () => void>();
    const signal = options.signal ?? 'SIGINT';
    /** Fires at most once, at the point the flow reaches the requested phase. */
    let fired = false;
    const fire = (): void => {
        if (fired) return;
        fired = true;
        trapped.get(signal)?.();
    };
    if (options.deleteImpl) handle.sandbox.delete = vi.fn(options.deleteImpl) as unknown as typeof handle.sandbox.delete;
    if (options.fireDuring === 'mint') {
        const realMint = handle.sandbox.getPreviewLink;
        handle.sandbox.getPreviewLink = vi.fn(async (port: number) => {
            fire();
            return realMint(port);
        }) as typeof handle.sandbox.getPreviewLink;
    }
    const deps: StartDaytonaDeps = {
        createSandbox: vi.fn(async () => handle),
        spawn: vi.fn(async () => {
            if (options.fireDuring === 'deploy') fire();
            return 0;
        }),
        log: (line) => lines.push(line),
        error: (line) => problems.push(line),
        env: ENV,
        onSignal: (name, listener) => {
            trapped.set(name, listener);
            return () => {
                trapped.delete(name);
            };
        },
        ...(options.deleteTimeout ? { deleteTimeout: options.deleteTimeout } : {}),
    };
    return { deps, lines, problems, trapped };
};

/**
 * A fake ChildProcess: every `on()` is recorded so a test can emit exactly one
 * lifecycle event, and `mock.calls` on the spawn spy shows what was requested.
 */
const makeChild = () => {
    const listeners: Record<string, ((arg?: unknown) => void)[]> = {};
    const child = {
        on: vi.fn((event: string, listener: (arg?: unknown) => void) => {
            (listeners[event] ??= []).push(listener);
            return child;
        }),
        /** Fires every listener registered for `event` (mirrors EventEmitter ordering). */
        emit: (event: string, arg?: unknown): void => {
            for (const listener of listeners[event] ?? []) listener(arg);
        },
    };
    return { child, listeners };
};

describe('start-daytona: module pins (paths resolved from import.meta.url, never cwd)', () => {
    it('resolves the sibling script inside the package root, and it exists', () => {
        expect(PACKAGE_ROOT.endsWith(join('packages', 'underload', 'nexus'))).toBe(true);
        expect(DEPLOY_SCRIPT_PATH).toBe(join(PACKAGE_ROOT, 'scripts', 'deploy-daytona.mjs'));
        expect(existsSync(DEPLOY_SCRIPT_PATH)).toBe(true);
    });

    it('keeps the hub-port default in lockstep with deploy-daytona.mjs DEFAULT_PORT', () => {
        // Drift between the two defaults is a silent mis-deploy, so it is pinned.
        expect(DEFAULT_HUB_PORT).toBe(8085);
        expect(flagsOf([]).port).toBe(8085);
    });
});

describe('start-daytona: argument parsing', () => {
    it('defaults to a fresh local deploy on the hub port, with no errors', () => {
        const { flags, errors } = parseArgs([]);
        expect(errors).toEqual([]);
        expect(flags).toEqual({
            help: false,
            dryRun: false,
            teardown: false,
            keepOnFail: false,
            apiKey: '',
            public: true,
            endpoint: '',
            token: '',
            mode: 'local',
            port: 8085,
            env: {},
            noBuild: false,
            artifact: '',
            wait: true,
        });
    });

    it('parses every flag into the exact flag record', () => {
        const { flags, errors } = parseArgs(['--api-key', 'dtn_key', '--ttl', '30', '--private', '--snapshot', 'toolchain-v1', '--cpu', '2', '--mem', '4', '--disk', '20', '--mode', 'remote-build', '--port', '9000', '--env', 'A=1', '--no-wait', '--keep-on-fail']);
        expect(errors).toEqual([]);
        expect(flags).toEqual({
            help: false,
            dryRun: false,
            teardown: false,
            keepOnFail: true,
            apiKey: 'dtn_key',
            ttl: 30,
            snapshot: 'toolchain-v1',
            cpu: 2,
            mem: 4,
            disk: 20,
            public: false,
            endpoint: '',
            token: '',
            mode: 'remote-build',
            port: 9000,
            env: { A: '1' },
            noBuild: false,
            artifact: '',
            wait: false,
        });
    });

    it('accumulates repeated --env in order and keeps "=" inside values', () => {
        const { flags, errors } = parseArgs(['--env', 'B=2', '--env', 'A=1', '--env', 'TOKEN=a=b']);
        expect(errors).toEqual([]);
        // Key order IS the typed order (JS string-key guarantee) — the deploy argv relies on it.
        expect(Object.keys(flags.env)).toEqual(['B', 'A', 'TOKEN']);
        expect(flags.env).toEqual({ B: '2', A: '1', TOKEN: 'a=b' });
    });

    it('lets the last of --public/--private win (the service CLI rule)', () => {
        expect(flagsOf(['--private', '--public']).public).toBe(true);
        expect(flagsOf(['--public', '--private']).public).toBe(false);
    });

    it('reports unknown flags and missing values with the deploy script wording', () => {
        expect(parseArgs(['--wat']).errors).toEqual(['unknown flag "--wat"']);
        expect(parseArgs(['--endpoint']).errors).toEqual(['--endpoint needs a value']);
        // A `--`-leading token after a value flag is a missing value, not a value.
        expect(parseArgs(['--token', '--dry-run']).errors).toEqual(['--token needs a value']);
    });

    it('rejects --env without a key', () => {
        expect(parseArgs(['--env', 'NOPE']).errors).toEqual(['--env expects KEY=VALUE, got "NOPE"']);
    });

    it('range-checks every numeric flag with the service\'s exact words', () => {
        expect(parseArgs(['--port', '0']).errors).toEqual(['--port "0" is out of range (TCP port 1-65535)']);
        expect(parseArgs(['--port', '65536']).errors).toEqual(['--port "65536" is out of range (TCP port 1-65535)']);
        expect(parseArgs(['--cpu', 'zero']).errors).toEqual(['--cpu "zero" is out of range (vCPU, >= 1)']);
        expect(parseArgs(['--ttl', '-1']).errors).toEqual(['--ttl "-1" is out of range (minutes, >= 0 (0 = disabled))']);
        // ttl is the ONE flag where 0 is meaningful ("no fence").
        expect(parseArgs(['--ttl', '0']).errors).toEqual([]);
        expect(flagsOf(['--ttl', '0']).ttl).toBe(0);
    });

    it('rejects an unknown --mode and the remote-build build-skip contradiction', () => {
        expect(parseArgs(['--mode', 'weird']).errors).toEqual(['--mode must be "local" or "remote-build", got "weird"']);
        expect(parseArgs(['--mode', 'remote-build', '--no-build']).errors).toEqual(['--no-build and --artifact apply to --mode local only (remote-build ships a source tarball)']);
        expect(parseArgs(['--mode', 'remote-build', '--artifact', 'x']).errors).toEqual(['--no-build and --artifact apply to --mode local only (remote-build ships a source tarball)']);
    });

    it('rejects --teardown without --endpoint (a fresh slot has nothing to tear down)', () => {
        expect(parseArgs(['--teardown']).errors).toEqual(['--teardown needs --endpoint <url> — a fresh sandbox has an empty runtime slot, and this script never tears down a sandbox it did not create']);
        expect(parseArgs(['--teardown']).flags.teardown).toBe(true);
        expect(parseArgs(['--teardown', '--endpoint', 'https://proxy.example/runner-7777']).errors).toEqual([]);
    });

    it('rejects sandbox-creation flags combined with --endpoint, naming only the offenders', () => {
        expect(parseArgs(['--endpoint', 'https://e.example', '--cpu', '2']).errors).toEqual(['--cpu only apply when this script creates the sandbox — drop --endpoint (fresh) or drop it (reusing https://e.example)']);
        expect(parseArgs(['--endpoint', 'https://e.example', '--private', '--ttl', '5']).errors).toEqual(['--private, --ttl only apply when this script creates the sandbox — drop --endpoint (fresh) or drop them (reusing https://e.example)']);
        // --token alone is targeting, not creation, so it stays legal with --endpoint.
        expect(parseArgs(['--endpoint', 'https://e.example', '--token', 't', '--keep-on-fail']).errors).toEqual([]);
    });

    it('rejects --token without --endpoint: a fresh run takes its token from createSandbox', () => {
        const TOKEN_IS_TARGETING = '--token only applies when you reuse an endpoint — a fresh run authenticates with the token createSandbox returns; drop --token or add --endpoint <url>';
        // The real fresh run IGNORES a typed --token (the created token wins) while
        // buildPlan would print it, so it is a parse error, not a silent surprise.
        expect(parseArgs(['--token', 't']).errors).toEqual([TOKEN_IS_TARGETING]);
        expect(parseArgs(['--dry-run', '--token', 't']).errors).toEqual([TOKEN_IS_TARGETING]);
        expect(resolveMode(parseArgs(['--token', 't']).flags)).toBe('fresh');
        // Reuse and teardown still accept it: those are the modes that target an endpoint.
        expect(parseArgs(['--endpoint', 'https://e.example', '--token', 't']).errors).toEqual([]);
        expect(parseArgs(['--teardown', '--endpoint', 'https://e.example', '--token', 't']).errors).toEqual([]);
        // teardown WITHOUT an endpoint reports only the missing endpoint, never both.
        expect(parseArgs(['--teardown', '--token', 't']).errors).toEqual(['--teardown needs --endpoint <url> — a fresh sandbox has an empty runtime slot, and this script never tears down a sandbox it did not create']);
    });

    it('allows --dry-run with --endpoint (the reuse plan must be printable)', () => {
        const { flags, errors } = parseArgs(['--dry-run', '--endpoint', 'https://proxy.example/runner-7777', '--token', 't']);
        expect(errors).toEqual([]);
        expect(resolveMode(flags)).toBe('reuse');
        expect(flags.dryRun).toBe(true);
    });
});

describe('start-daytona: mode resolution', () => {
    it('derives the mode from teardown/endpoint only', () => {
        expect(resolveMode({ teardown: false, endpoint: '' })).toBe('fresh');
        expect(resolveMode({ teardown: false, endpoint: 'https://e' })).toBe('reuse');
        expect(resolveMode({ teardown: true, endpoint: 'https://e' })).toBe('teardown');
        // teardown outranks reuse; fresh+teardown is impossible (the parser refuses it).
        expect(resolveMode({ teardown: true, endpoint: '' })).toBe('teardown');
    });
});

describe('start-daytona: sandbox option mapping', () => {
    it('maps only the knobs that were given, and never passes --port as the RUNNER port', () => {
        const options = buildSandboxOptions(flagsOf(['--api-key', 'dtn_key', '--ttl', '30', '--private', '--snapshot', 'snap', '--cpu', '2', '--mem', '4', '--disk', '20', '--port', '9000']));
        expect(options).toEqual({ apiKey: 'dtn_key', ttlMinutes: 30, public: false, snapshot: 'snap', cpu: 2, mem: 4, disk: 20 });
        // createSandbox's `port` is the RUNNER port (7777 = the management endpoint) while
        // --port is the HUB port; forwarding it would repoint the deploy at the hub.
        expect('port' in options).toBe(false);
    });

    it('keeps every service default when no creation flag is given', () => {
        const options = buildSandboxOptions(flagsOf([]));
        expect(options).toEqual({ public: true }); // undefined-valued keys are the service's defaults
        expect(options.apiKey).toBeUndefined(); // '' → absent, so DAYTONA_API_KEY/vault still apply
        expect(options.ttlMinutes).toBeUndefined(); // unset = NO wall-clock kill fence
        expect(options.snapshot).toBeUndefined(); // the service's own default snapshot
    });
});

describe('start-daytona: deploy argv per mode', () => {
    it('fresh with a private endpoint carries the preview token, in the documented order', () => {
        expect(buildDeployArgs(flagsOf(['--env', 'NEXUS_TOKEN=xyz']), { endpoint: 'https://proxy.example/runner-7777', token: 'dtn_preview_token' }, 'fresh')).toEqual([
            '--endpoint', 'https://proxy.example/runner-7777',
            '--token', 'dtn_preview_token',
            '--mode', 'local',
            '--port', '8085',
            '--env', 'NEXUS_TOKEN=xyz',
        ]);
    });

    it('omits --token entirely for a public endpoint (the child then sends no auth header)', () => {
        const expected = ['--endpoint', 'https://proxy.example/runner-7777', '--mode', 'local', '--port', '8085'];
        expect(buildDeployArgs(flagsOf([]), { endpoint: 'https://proxy.example/runner-7777', token: '' }, 'fresh')).toEqual(expected);
        expect(buildDeployArgs(flagsOf([]), { endpoint: 'https://proxy.example/runner-7777', token: undefined }, 'fresh')).toEqual(expected);
    });

    it('forwards every deploy passthrough flag verbatim', () => {
        expect(buildDeployArgs(flagsOf(['--mode', 'remote-build', '--port', '9000', '--env', 'B=2', '--env', 'A=1', '--no-wait']), { endpoint: 'https://e', token: 't' }, 'reuse')).toEqual([
            '--endpoint', 'https://e',
            '--token', 't',
            '--mode', 'remote-build',
            '--port', '9000',
            '--env', 'B=2',
            '--env', 'A=1',
            '--no-wait',
        ]);
        expect(buildDeployArgs(flagsOf(['--no-build', '--artifact', '/tmp/nexus-hub']), { endpoint: 'https://e' }, 'fresh')).toEqual([
            '--endpoint', 'https://e',
            '--mode', 'local',
            '--port', '8085',
            '--no-build',
            '--artifact', '/tmp/nexus-hub',
        ]);
    });

    it('teardown carries ONLY endpoint/token/--teardown (mode/port/env would imply meaning)', () => {
        const flags = flagsOf(['--teardown', '--endpoint', 'https://e', '--token', 't', '--port', '9000', '--env', 'A=1']);
        expect(buildDeployArgs(flags, { endpoint: 'https://e', token: 't' }, 'teardown')).toEqual(['--endpoint', 'https://e', '--token', 't', '--teardown']);
        expect(buildDeployArgs(flagsOf(['--teardown', '--endpoint', 'https://e']), { endpoint: 'https://e' }, 'teardown')).toEqual(['--endpoint', 'https://e', '--teardown']);
    });

    it('wraps the argv with the node command, the package-root cwd and the untouched env', () => {
        const invocation = buildPlan(flagsOf([]), ENV, { endpoint: 'https://e' }).invocation;
        expect(invocation).toEqual({ command: 'node', args: [DEPLOY_SCRIPT_PATH, '--endpoint', 'https://e', '--mode', 'local', '--port', '8085'], cwd: PACKAGE_ROOT, env: ENV });
        // Identity, not equality: the child must see the caller's env object UNTOUCHED.
        expect(invocation.env).toBe(ENV);
        // args[0] is the script path itself — spawn('node', args) runs the child as-is.
        expect(invocation.args[0]).toBe(DEPLOY_SCRIPT_PATH);
    });
});

describe('start-daytona: plans', () => {
    it('fresh plan shows the createSandbox options and a placeholder endpoint (it does not exist yet)', () => {
        const plan = buildPlan(flagsOf(['--ttl', '30', '--private', '--api-key', 'dtn_key']), ENV);
        expect(plan.mode).toBe('fresh');
        expect(plan.sandboxOptions).toEqual({ apiKey: 'dtn_key', ttlMinutes: 30, public: false });
        expect(plan.invocation.args).toEqual([DEPLOY_SCRIPT_PATH, '--endpoint', '<from createSandbox>', '--mode', 'local', '--port', '8085']);
        expect(plan.invocation.cwd).toBe(PACKAGE_ROOT);
        expect(plan.hub).toEqual({ port: 8085, via: 'sandbox.getPreviewLink(8085)' });
    });

    it('reuse plan prints the given endpoint and has no sandbox-options block', () => {
        const plan = buildPlan(flagsOf(['--endpoint', 'https://e', '--token', 't']), ENV);
        expect(plan.mode).toBe('reuse');
        expect(plan.sandboxOptions).toBeUndefined();
        expect(plan.invocation.args).toEqual([DEPLOY_SCRIPT_PATH, '--endpoint', 'https://e', '--token', 't', '--mode', 'local', '--port', '8085']);
        expect(plan.hub).toEqual({ port: 8085, via: 'sandbox.getPreviewLink(8085)' });
    });

    it('teardown plan mints no hub URL', () => {
        const plan = buildPlan(flagsOf(['--teardown', '--endpoint', 'https://e']), ENV);
        expect(plan.mode).toBe('teardown');
        expect(plan.sandboxOptions).toBeUndefined();
        expect(plan.invocation.args).toEqual([DEPLOY_SCRIPT_PATH, '--endpoint', 'https://e', '--teardown']);
        expect(plan.hub).toBeUndefined();
    });
});

describe('start-daytona: the machine-readable line', () => {
    it('is one single line carrying every fresh-mode field, in the documented order', () => {
        const line = buildSummary({ mode: 'fresh', endpoint: 'https://e', token: 't', hubUrl: 'https://hub', hubToken: 'ht', sandboxId: 'sbx-1234', name: 'nexus-1', public: false, hubPort: 8085 });
        expect(line.includes('\n')).toBe(false);
        expect(line).toBe('{"mode":"fresh","endpoint":"https://e","token":"t","hubUrl":"https://hub","hubToken":"ht","sandboxId":"sbx-1234","name":"nexus-1","public":false,"hubPort":8085}');
    });

    it('omits empty tokens instead of sending ""', () => {
        expect(buildSummary({ mode: 'fresh', endpoint: 'https://e', hubUrl: 'https://hub', hubToken: '', sandboxId: 'sbx-1234', hubPort: 9000 })).toBe('{"mode":"fresh","endpoint":"https://e","hubUrl":"https://hub","sandboxId":"sbx-1234","hubPort":9000}');
    });

    it('keeps hubUrl present but null in reuse mode (a consumer must not read it as a URL)', () => {
        expect(buildSummary({ mode: 'reuse', endpoint: 'https://e', token: 't', hubUrl: null, hubPort: 8085 })).toBe('{"mode":"reuse","endpoint":"https://e","token":"t","hubUrl":null,"hubPort":8085}');
    });
});

describe('start-daytona: fresh mode', () => {
    it('creates once, spawns once with the CREATED endpoint, mints the hub URL, ends on the JSON line', async () => {
        const handle = makeHandle();
        const { createSpy, spawnSpy, lines, problems, invocations, deps } = makeDeps(handle, 0);
        expect(await runStartDaytona(flagsOf(['--ttl', '30', '--private', '--api-key', 'dtn_key', '--env', 'NEXUS_TOKEN=xyz']), deps)).toBe(0);
        expect(problems).toEqual([]);

        // ONE create, with the mapped options plus the progress sink.
        expect(createSpy).toHaveBeenCalledTimes(1);
        expect(createSpy.mock.calls[0]![0]).toEqual({ apiKey: 'dtn_key', ttlMinutes: 30, public: false, snapshot: undefined, cpu: undefined, mem: undefined, disk: undefined, log: expect.any(Function) });

        // ONE child call: the created endpoint, the forwarded token, env untouched.
        expect(spawnSpy).toHaveBeenCalledTimes(1);
        expect(invocations).toEqual([{ command: 'node', args: [DEPLOY_SCRIPT_PATH, '--endpoint', 'https://proxy.example/runner-7777', '--token', 'dtn_preview_token', '--mode', 'local', '--port', '8085', '--env', 'NEXUS_TOKEN=xyz'], cwd: PACKAGE_ROOT, env: ENV }]);
        expect(invocations[0]!.env).toBe(ENV);

        // The hub URL is a per-port preview link, never a rewrite of the endpoint string.
        expect(handle.sandbox.getPreviewLink).toHaveBeenCalledWith(8085);
        expect(handle.sandbox.delete).not.toHaveBeenCalled();

        // Human lines carry BOTH URLs and the billing note; the LAST line is the contract.
        expect(lines).toEqual([
            '[nexus:start] creating a Daytona sandbox (minutes; --ttl unset = it bills until deleted)…',
            '[nexus:start] ✓ sandbox sbx-1234 (nexus-1) ready — management endpoint https://proxy.example/runner-7777',
            '[nexus:start] ✓ nexus hub is live',
            '[nexus:start]   hub URL (clients connect here) [x-daytona-preview-token required]: https://proxy.example/hub-8085',
            '[nexus:start]   management endpoint (POST/status/logs) [x-daytona-preview-token]: https://proxy.example/runner-7777',
            '[nexus:start]   sandbox sbx-1234 — bills until sandbox.delete() or its 30-minute TTL fires',
            '{"mode":"fresh","endpoint":"https://proxy.example/runner-7777","token":"dtn_preview_token","hubUrl":"https://proxy.example/hub-8085","hubToken":"hub_token","sandboxId":"sbx-1234","name":"nexus-1","public":false,"hubPort":8085}',
        ]);
    });

    it('a public sandbox prints an unauthenticated hub URL and drops the token fields', async () => {
        const handle = makeHandle({ token: '', public: true });
        handle.sandbox.getPreviewLink = vi.fn(async (port: number) => ({ url: `https://proxy.example/hub-${port}`, token: '' }));
        const { deps, lines } = makeDeps(handle, 0);
        expect(await runStartDaytona(flagsOf([]), deps)).toBe(0);
        expect(lines[3]).toBe('[nexus:start]   hub URL (clients connect here) [public, no token]: https://proxy.example/hub-8085');
        expect(lines[5]).toBe('[nexus:start]   sandbox sbx-1234 — bills until sandbox.delete() (no --ttl fence was set)');
        expect(lines[6]).toBe('{"mode":"fresh","endpoint":"https://proxy.example/runner-7777","hubUrl":"https://proxy.example/hub-8085","sandboxId":"sbx-1234","name":"nexus-1","public":true,"hubPort":8085}');
    });

    it('forwards createSandbox progress through the prefixed sink (a quiet library needs a loud host)', async () => {
        const handle = makeHandle();
        const { deps, lines } = makeDeps(handle, 0);
        let sandboxLog: ((message: string) => void) | undefined;
        deps.createSandbox = vi.fn(async (options) => {
            sandboxLog = options.log as (message: string) => void;
            return handle;
        });
        await runStartDaytona(flagsOf([]), deps);
        sandboxLog!('creating sandbox\nstep 2');
        expect(lines[lines.length - 1]).toBe('[nexus:start]   creating sandbox\n  step 2');
    });

    it('deletes the sandbox it created when the deploy fails, and propagates the exit code', async () => {
        const handle = makeHandle();
        const { deps, lines, problems, invocations } = makeDeps(handle, 3);
        expect(await runStartDaytona(flagsOf([]), deps)).toBe(3);
        expect(invocations).toHaveLength(1);
        expect(handle.sandbox.delete).toHaveBeenCalledTimes(1);
        expect(handle.sandbox.getPreviewLink).not.toHaveBeenCalled();
        expect(problems).toEqual(['[nexus:start] ✗ deploy failed (exit 3) — the deploy output above is the diagnosis']);
        expect(lines[lines.length - 1]).toBe('[nexus:start] ✓ deleted sandbox sbx-1234 (a failed deploy does not get to keep burning)');
    });

    it('--keep-on-fail leaves the sandbox running and says it is still BILLING', async () => {
        const handle = makeHandle();
        const { deps, problems } = makeDeps(handle, 1);
        expect(await runStartDaytona(flagsOf(['--keep-on-fail']), deps)).toBe(1);
        expect(handle.sandbox.delete).not.toHaveBeenCalled();
        expect(problems).toEqual([
            '[nexus:start] ✗ deploy failed (exit 1) — the deploy output above is the diagnosis',
            '[nexus:start] --keep-on-fail: sandbox sbx-1234 is LEFT RUNNING and BILLING — delete it when you are done',
        ]);
    });

    it('a failed delete is reported as a billing leak, and still exits with the deploy code', async () => {
        const handle = makeHandle();
        handle.sandbox.delete = vi.fn(async () => {
            throw new Error('sdk offline');
        });
        const { deps, problems } = makeDeps(handle, 2);
        expect(await runStartDaytona(flagsOf([]), deps)).toBe(2);
        expect(problems).toEqual([
            '[nexus:start] ✗ deploy failed (exit 2) — the deploy output above is the diagnosis',
            '[nexus:start] ✗ could NOT delete sandbox sbx-1234: sdk offline',
            '[nexus:start]   it keeps BILLING until deleted: sandbox.delete() or the @services/daytona `down` verb',
        ]);
    });

    it('keeps the sandbox when only the hub URL fails to mint (the deploy itself succeeded)', async () => {
        const handle = makeHandle();
        handle.sandbox.getPreviewLink = vi.fn(async () => {
            throw new Error('preview boom');
        });
        const { deps, lines, problems } = makeDeps(handle, 0);
        expect(await runStartDaytona(flagsOf([]), deps)).toBe(1);
        expect(handle.sandbox.delete).not.toHaveBeenCalled();
        expect(problems).toEqual([
            '[nexus:start] ✗ deploy is ready but the hub preview URL could not be minted: preview boom',
            '[nexus:start]   sandbox sbx-1234 LEFT RUNNING (billing) — mint getPreviewLink(8085) yourself',
            '[nexus:start]   management endpoint, still usable: https://proxy.example/runner-7777',
        ]);
        // No hub URL, so no machine-readable line to chain off.
        expect(lines[lines.length - 1]).not.toMatch(/^\{/);
    });

    it('--no-wait forwards the flag and warns that the hub may still be booting', async () => {
        const handle = makeHandle();
        const { deps, lines, invocations } = makeDeps(handle, 0);
        expect(await runStartDaytona(flagsOf(['--no-wait']), deps)).toBe(0);
        expect(invocations[0]!.args).toEqual([DEPLOY_SCRIPT_PATH, '--endpoint', 'https://proxy.example/runner-7777', '--token', 'dtn_preview_token', '--mode', 'local', '--port', '8085', '--no-wait']);
        expect(lines[6]).toBe('[nexus:start]   (--no-wait: the deploy was accepted, the hub may still be booting — probe the hub URL)');
        expect(lines[7]).toBe('{"mode":"fresh","endpoint":"https://proxy.example/runner-7777","token":"dtn_preview_token","hubUrl":"https://proxy.example/hub-8085","hubToken":"hub_token","sandboxId":"sbx-1234","name":"nexus-1","public":false,"hubPort":8085}');
    });
});

describe('start-daytona: reuse mode (never touches sandbox lifecycle)', () => {
    it('creates nothing, spawns against the given endpoint, and prints hubUrl null', async () => {
        const { deps, createSpy, lines, invocations } = makeDeps(undefined, 0);
        expect(await runStartDaytona(flagsOf(['--endpoint', 'https://proxy.example/runner-7777', '--token', 'dtn_preview_token']), deps)).toBe(0);
        expect(createSpy).not.toHaveBeenCalled();
        expect(invocations).toEqual([{ command: 'node', args: [DEPLOY_SCRIPT_PATH, '--endpoint', 'https://proxy.example/runner-7777', '--token', 'dtn_preview_token', '--mode', 'local', '--port', '8085'], cwd: PACKAGE_ROOT, env: ENV }]);
        expect(lines).toEqual([
            '[nexus:start] deploying into the existing endpoint https://proxy.example/runner-7777 (not created here — no sandbox lifecycle actions)',
            '[nexus:start] ✓ deploy ready on https://proxy.example/runner-7777',
            '[nexus:start]   hub URL (clients connect here): not minted — reuse mode holds no sandbox handle; run getPreviewLink(8085) on the owning sandbox',
            '[nexus:start]   management endpoint (POST/status/logs): https://proxy.example/runner-7777',
            '{"mode":"reuse","endpoint":"https://proxy.example/runner-7777","token":"dtn_preview_token","hubUrl":null,"hubPort":8085}',
        ]);
    });

    it('propagates a failing deploy without deleting anything', async () => {
        const handle = makeHandle();
        const { deps, problems } = makeDeps(handle, 4);
        // A handle is lying around on purpose: reuse must still never reach for delete.
        expect(await runStartDaytona(flagsOf(['--endpoint', 'https://e.example', '--keep-on-fail']), deps)).toBe(4);
        expect(handle.sandbox.delete).not.toHaveBeenCalled();
        expect(problems).toEqual(['[nexus:start] ✗ deploy failed (exit 4) — the deploy output above is the diagnosis; nothing was created, so nothing to clean up']);
    });
});

describe('start-daytona: teardown mode', () => {
    it('spawns endpoint/token/--teardown only, mints nothing, deletes nothing', async () => {
        const { deps, createSpy, lines, problems, invocations } = makeDeps(undefined, 0);
        expect(await runStartDaytona(flagsOf(['--teardown', '--endpoint', 'https://proxy.example/runner-7777', '--token', 'dtn_preview_token']), deps)).toBe(0);
        expect(createSpy).not.toHaveBeenCalled();
        expect(invocations).toEqual([{ command: 'node', args: [DEPLOY_SCRIPT_PATH, '--endpoint', 'https://proxy.example/runner-7777', '--token', 'dtn_preview_token', '--teardown'], cwd: PACKAGE_ROOT, env: ENV }]);
        expect(lines).toEqual(['[nexus:start] tearing down the runtime slot at https://proxy.example/runner-7777 (DELETE / — the SANDBOX is not deleted)']);
        // No hub URL exists in this mode, so the JSON contract does not apply.
        expect(lines.filter((line) => line.startsWith('{'))).toEqual([]);
        expect(problems).toEqual([]);
    });

    it('reports a failing teardown with the child\'s exit code', async () => {
        const { deps, problems } = makeDeps(undefined, 1);
        expect(await runStartDaytona(flagsOf(['--teardown', '--endpoint', 'https://e.example']), deps)).toBe(1);
        expect(problems).toEqual(['[nexus:start] ✗ teardown failed (exit 1) — see the deploy output above']);
    });
});

describe('start-daytona: dry run touches nothing', () => {
    it('prints the fresh plan and calls neither createSandbox, spawn, nor fetch', async () => {
        // Tripwire: any network use in a dry run would throw AND be counted.
        const fetchSpy = vi.fn(() => {
            throw new Error('dry run must not fetch');
        });
        vi.stubGlobal('fetch', fetchSpy);
        try {
            const { deps, createSpy, lines, invocations } = makeDeps(makeHandle(), 0);
            expect(await runStartDaytona(flagsOf(['--dry-run', '--ttl', '30', '--private', '--api-key', 'dtn_key']), deps)).toBe(0);
            expect(createSpy).not.toHaveBeenCalled();
            expect(invocations).toEqual([]);
            expect(fetchSpy).not.toHaveBeenCalled();
            expect(lines).toEqual([
                '[nexus:start] DRY RUN — no sandbox is created, no script is spawned, no network call',
                '[nexus:start] mode: fresh',
                '[nexus:start] sandbox options that WOULD be used:',
                '  {\n    "apiKey": "dtn_key",\n    "ttlMinutes": 30,\n    "public": false\n  }',
                '[nexus:start] (unset knobs keep the @services/daytona defaults; --ttl unset means NO kill fence)',
                `[nexus:start] deploy command that WOULD run (cwd ${PACKAGE_ROOT}):`,
                `  node ${DEPLOY_SCRIPT_PATH} --endpoint <from createSandbox> --mode local --port 8085`,
                '[nexus:start] hub URL after success: sandbox.getPreviewLink(8085)',
            ]);
        } finally {
            vi.unstubAllGlobals();
        }
    });

    it('prints the reuse and teardown plans with the given endpoint and no create', async () => {
        const reuse = makeDeps(undefined, 0);
        // reuse keeps --token (D2): it is the ONLY mode where the token means anything.
        expect(await runStartDaytona(flagsOf(['--dry-run', '--endpoint', 'https://proxy.example/runner-7777', '--token', 't']), reuse.deps)).toBe(0);
        expect(reuse.lines).toEqual([
            '[nexus:start] DRY RUN — no sandbox is created, no script is spawned, no network call',
            '[nexus:start] mode: reuse',
            `[nexus:start] deploy command that WOULD run (cwd ${PACKAGE_ROOT}):`,
            `  node ${DEPLOY_SCRIPT_PATH} --endpoint https://proxy.example/runner-7777 --token t --mode local --port 8085`,
            '[nexus:start] hub URL after success: sandbox.getPreviewLink(8085)',
        ]);
        expect(reuse.createSpy).not.toHaveBeenCalled();

        const teardown = makeDeps(undefined, 0);
        expect(await runStartDaytona(flagsOf(['--dry-run', '--teardown', '--endpoint', 'https://e.example']), teardown.deps)).toBe(0);
        expect(teardown.lines).toEqual([
            '[nexus:start] DRY RUN — no sandbox is created, no script is spawned, no network call',
            '[nexus:start] mode: teardown',
            `[nexus:start] deploy command that WOULD run (cwd ${PACKAGE_ROOT}):`,
            `  node ${DEPLOY_SCRIPT_PATH} --endpoint https://e.example --teardown`,
        ]);
        expect(teardown.createSpy).not.toHaveBeenCalled();
        expect(teardown.invocations).toEqual([]);
    });
});

describe('start-daytona: main() maps results onto exit codes', () => {
    it('--help prints the usage screen and exits 0 even alongside a bad flag', async () => {
        const { deps, lines } = makeDeps(undefined, 0);
        expect(await main(['--help', '--wat'], deps)).toBe(0);
        expect(lines).toEqual([USAGE]);
        expect(USAGE).toContain('start:daytona');
        expect(USAGE).toContain('--keep-on-fail');
        expect(USAGE).toContain('BILLING');
        // D1: the header name operators are told to send must be the real one.
        expect(USAGE).toContain('x-daytona-preview-token');
        expect(USAGE).not.toContain('x-preview-token required');
        // R1: the interrupt contract is operator-facing, so it lives in --help.
        expect(USAGE).toContain('exits 130 for SIGINT, 143 for');
        expect(USAGE).toContain('Reuse, teardown and dry-run install no handlers');
    });

    it('a fresh --token (even with --dry-run) is a parse error naming --token, before any work', async () => {
        const { deps, problems, createSpy, spawnSpy } = makeDeps(makeHandle(), 0);
        expect(await main(['--dry-run', '--token', 't'], deps)).toBe(1);
        expect(problems[0]).toBe('[nexus:start] ✗ --token only applies when you reuse an endpoint — a fresh run authenticates with the token createSandbox returns; drop --token or add --endpoint <url>');
        expect(problems[1]).toBe('');
        expect(problems[2]).toBe(USAGE);
        // Rejected at parse time: a dry run that would have lied never happens.
        expect(createSpy).not.toHaveBeenCalled();
        expect(spawnSpy).not.toHaveBeenCalled();
    });

    it('a bad command line prints every error plus the usage and exits 1', async () => {
        const { deps, problems } = makeDeps(undefined, 0);
        expect(await main(['--wat', '--port', '0'], deps)).toBe(1);
        expect(problems.slice(0, 3)).toEqual(['[nexus:start] ✗ unknown flag "--wat"', '[nexus:start] ✗ --port "0" is out of range (TCP port 1-65535)', '']);
        expect(problems[3]).toBe(USAGE);
    });

    it('a thrown createSandbox error (credentials, bad knobs) becomes exit 1 with its message', async () => {
        const problems: string[] = [];
        const deps: StartDaytonaDeps = {
            createSandbox: vi.fn(async () => {
                throw new Error('DaytonaCredentialError: no Daytona API key found');
            }),
            spawn: vi.fn(async () => 0),
            log: () => undefined,
            error: (line) => problems.push(line),
            env: ENV,
        };
        expect(await main([], deps)).toBe(1);
        expect(problems).toEqual(['[nexus:start] ✗ DaytonaCredentialError: no Daytona API key found']);
    });

    it('--dry-run short-circuits before credentials even exist', async () => {
        const deps: StartDaytonaDeps = {
            createSandbox: vi.fn(async () => {
                throw new Error('must not be reached');
            }),
            spawn: vi.fn(async () => 0),
            log: () => undefined,
            error: () => undefined,
            env: ENV,
        };
        expect(await main(['--dry-run'], deps)).toBe(0);
        expect(deps.createSandbox).not.toHaveBeenCalled();
        expect(deps.spawn).not.toHaveBeenCalled();
    });
});

describe('start-daytona: spawnDeploy — the one real side effect (child driven through the SpawnImpl seam)', () => {
    /** A child invocation identical to what buildDeployInvocation produces for teardown. */
    const invocationOf = (): DeployInvocation => ({
        command: 'node',
        args: [DEPLOY_SCRIPT_PATH, '--endpoint', 'https://proxy.example/runner-7777', '--token', 'dtn_preview_token', '--teardown'],
        cwd: PACKAGE_ROOT,
        env: ENV,
    });

    it('argvs the resolved script path from the package root with inherited stdio and the caller\'s env', async () => {
        const invocation = invocationOf();
        const { child } = makeChild();
        const impl = vi.fn(() => child) as unknown as SpawnImpl;
        const run = spawnDeploy(invocation, impl);
        expect(impl).toHaveBeenCalledTimes(1);
        // The whole call is the contract: plain node, args[0] = the resolved script,
        // cwd = the package root (cargo's target-dir config), env untouched, output
        // inherited so the child's progress reaches the terminal as-is.
        expect((impl as unknown as ReturnType<typeof vi.fn>).mock.calls[0]).toEqual(['node', invocation.args, { cwd: PACKAGE_ROOT, env: ENV, stdio: 'inherit' }]);
        const [command, args, options] = (impl as unknown as ReturnType<typeof vi.fn>).mock.calls[0] as [string, string[], { cwd: string; env: Record<string, string | undefined>; stdio: string }];
        // Identity, not equality: the argv array and the env object are handed over
        // UNTOUCHED, so the child runs exactly what the dry run printed.
        expect(command).toBe('node');
        expect(args).toBe(invocation.args);
        expect(args[0]).toBe(DEPLOY_SCRIPT_PATH);
        expect(options.cwd).toBe(PACKAGE_ROOT);
        expect(options.env).toBe(ENV);
        expect(options.stdio).toBe('inherit');
        child.emit('close', 0);
        expect(await run).toBe(0);
    });

    it('resolves the child\'s exit code, mapping a signal-killed child (code null) to 1', async () => {
        // Every non-zero child outcome must reach the caller as non-zero, including
        // `code === null`, which is what a signal-killed child reports.
        for (const [childCode, expected] of [[0, 0], [3, 3], [127, 127], [null, 1]] as const) {
            const { child } = makeChild();
            const impl = vi.fn(() => child) as unknown as SpawnImpl;
            const run = spawnDeploy(invocationOf(), impl);
            child.emit('close', childCode);
            expect(await run).toBe(expected);
        }
    });

    it('a spawn-level failure (no node on PATH) resolves 1 and says which script failed to start', async () => {
        const { child } = makeChild();
        const impl = vi.fn(() => child) as unknown as SpawnImpl;
        const errors = vi.spyOn(console, 'error').mockImplementation(() => undefined);
        try {
            const run = spawnDeploy(invocationOf(), impl);
            child.emit('error', new Error('spawn node ENOENT'));
            expect(await run).toBe(1);
            expect(errors.mock.calls.map((call) => call[0])).toEqual(['[nexus:start] ✗ could not start deploy-daytona.mjs: spawn node ENOENT']);
        } finally {
            errors.mockRestore();
        }
    });

    it('registers exactly the error and close listeners it reads the outcome from', async () => {
        const { child, listeners } = makeChild();
        const impl = vi.fn(() => child) as unknown as SpawnImpl;
        const run = spawnDeploy(invocationOf(), impl);
        expect(Object.keys(listeners).sort()).toEqual(['close', 'error']);
        // The promise settles ONCE: whichever event arrives first is the answer, and
        // a late error must not overwrite an exit code the child already reported.
        child.emit('close', 7);
        child.emit('error', new Error('late'));
        expect(await run).toBe(7);
    });
});

describe('start-daytona: interrupt policy (decideInterruptCleanup, pure)', () => {
    const created = { sandboxId: 'sbx-1234' };
    it('a fresh run holding a sandbox owes a delete, at 130 for SIGINT', () => {
        expect(decideInterruptCleanup({ mode: 'fresh', created, phase: 'deploying' as InterruptPhase, signal: 'SIGINT' })).toEqual({ shouldDelete: true, exitCode: 130 });
        // Minting the hub URL is still inside the window: the hub is paid for, unserved.
        expect(decideInterruptCleanup({ mode: 'fresh', created, phase: 'minting' as InterruptPhase, signal: 'SIGTERM' })).toEqual({ shouldDelete: true, exitCode: 143 });
    });

    it('SIGINT is 130 and SIGTERM is 143 in every branch', () => {
        expect(decideInterruptCleanup({ mode: 'fresh', created, phase: 'creating' as InterruptPhase, signal: 'SIGINT' })).toEqual({ shouldDelete: false, exitCode: 130 });
        expect(decideInterruptCleanup({ mode: 'reuse', created, phase: 'deploying' as InterruptPhase, signal: 'SIGTERM' })).toEqual({ shouldDelete: false, exitCode: 143 });
    });

    it('owes nothing while createSandbox has not returned (its own failure path deletes)', () => {
        expect(decideInterruptCleanup({ mode: 'fresh', created, phase: 'creating' as InterruptPhase, signal: 'SIGTERM' })).toEqual({ shouldDelete: false, exitCode: 143 });
    });

    it('owes nothing without a handle, and never in reuse/teardown (not our sandbox)', () => {
        expect(decideInterruptCleanup({ mode: 'fresh', phase: 'deploying' as InterruptPhase, signal: 'SIGINT' })).toEqual({ shouldDelete: false, exitCode: 130 });
        expect(decideInterruptCleanup({ mode: 'reuse', created, phase: 'deploying' as InterruptPhase, signal: 'SIGINT' })).toEqual({ shouldDelete: false, exitCode: 130 });
        expect(decideInterruptCleanup({ mode: 'teardown', created, phase: 'minting' as InterruptPhase, signal: 'SIGTERM' })).toEqual({ shouldDelete: false, exitCode: 143 });
    });
});

describe('start-daytona: the fresh-mode interrupt fence (Ctrl-C must not leave a billing sandbox)', () => {
    it('SIGINT during the deploy deletes the sandbox, prints the outcome, and exits 130', async () => {
        const handle = makeHandle();
        const { deps, lines, problems, trapped } = makeInterruptDeps(handle, { fireDuring: 'deploy', signal: 'SIGINT' });
        expect(await runStartDaytona(flagsOf([]), deps)).toBe(130);
        // The delete is the whole point of the fence.
        expect(handle.sandbox.delete).toHaveBeenCalledTimes(1);
        // The deploy succeeded, but the run was interrupted before the mint, so no
        // hub URL, no JSON line, and no second delete from the failure path.
        expect(handle.sandbox.getPreviewLink).not.toHaveBeenCalled();
        expect(problems).toEqual([]);
        expect(lines).toEqual([
            '[nexus:start] creating a Daytona sandbox (minutes; --ttl unset = it bills until deleted)…',
            '[nexus:start] ✓ sandbox sbx-1234 (nexus-1) ready — management endpoint https://proxy.example/runner-7777',
            '[nexus:start] SIGINT received — deleting sandbox sbx-1234 before exit',
            '[nexus:start] interrupted — sandbox sbx-1234 deleted',
        ]);
        // Handlers come off with the run, so a finished command keeps Node's defaults.
        expect(trapped.size).toBe(0);
    });

    it('SIGTERM during the hub-URL mint deletes it too and exits 143', async () => {
        const handle = makeHandle();
        const { deps, lines, problems, trapped } = makeInterruptDeps(handle, { fireDuring: 'mint', signal: 'SIGTERM' });
        expect(await runStartDaytona(flagsOf([]), deps)).toBe(143);
        expect(handle.sandbox.delete).toHaveBeenCalledTimes(1);
        // The mint itself finished, but an interrupted run prints NO success block and
        // no JSON contract: the sandbox is on its way out, so a consumer chaining off
        // hubUrl would dial a dead box.
        expect(problems).toEqual([]);
        expect(lines).toEqual([
            '[nexus:start] creating a Daytona sandbox (minutes; --ttl unset = it bills until deleted)…',
            '[nexus:start] ✓ sandbox sbx-1234 (nexus-1) ready — management endpoint https://proxy.example/runner-7777',
            '[nexus:start] SIGTERM received — deleting sandbox sbx-1234 before exit',
            '[nexus:start] interrupted — sandbox sbx-1234 deleted',
        ]);
        expect(trapped.size).toBe(0);
    });

    it('a delete that never comes back is cut off by the bounded wait and reported as a leak', async () => {
        const handle = makeHandle();
        const timeouts: number[] = [];
        const { deps, lines, problems } = makeInterruptDeps(handle, {
            fireDuring: 'deploy',
            signal: 'SIGINT',
            // A delete that never settles: only the bounded wait can end this run.
            deleteImpl: () => new Promise<never>(() => undefined),
            deleteTimeout: (ms) => {
                timeouts.push(ms);
                return Promise.resolve('timeout');
            },
        });
        expect(await runStartDaytona(flagsOf([]), deps)).toBe(130);
        expect(handle.sandbox.delete).toHaveBeenCalledTimes(1);
        // The cap the operator is promised in --help, asserted rather than assumed.
        expect(timeouts).toEqual([INTERRUPT_DELETE_TIMEOUT_MS]);
        expect(INTERRUPT_DELETE_TIMEOUT_MS).toBe(10_000);
        expect(lines[lines.length - 1]).toBe('[nexus:start] SIGINT received — deleting sandbox sbx-1234 before exit');
        expect(problems).toEqual(['[nexus:start] interrupted — sandbox sbx-1234 DELETE FAILED (bills until manual cleanup)']);
    });

    it('a delete that throws is reported the same way instead of escaping the handler', async () => {
        const handle = makeHandle();
        const { deps, problems } = makeInterruptDeps(handle, {
            fireDuring: 'deploy',
            signal: 'SIGINT',
            deleteImpl: async () => {
                throw new Error('sdk offline');
            },
        });
        expect(await runStartDaytona(flagsOf([]), deps)).toBe(130);
        expect(problems).toEqual(['[nexus:start] interrupted — sandbox sbx-1234 DELETE FAILED (bills until manual cleanup)']);
    });

    it('reuse, teardown and dry-run arm no signal handlers at all', async () => {
        // Reuse/teardown: that endpoint was never ours, so trapping Ctrl-C there would
        // delete someone else\'s sandbox.
        const reuse = makeInterruptDeps(makeHandle());
        expect(await runStartDaytona(flagsOf(['--endpoint', 'https://proxy.example/runner-7777', '--token', 't']), reuse.deps)).toBe(0);
        expect(reuse.trapped.size).toBe(0);

        const teardown = makeInterruptDeps(makeHandle());
        expect(await runStartDaytona(flagsOf(['--teardown', '--endpoint', 'https://proxy.example/runner-7777']), teardown.deps)).toBe(0);
        expect(teardown.trapped.size).toBe(0);

        const dryRun = makeInterruptDeps(makeHandle());
        expect(await runStartDaytona(flagsOf(['--dry-run']), dryRun.deps)).toBe(0);
        expect(dryRun.trapped.size).toBe(0);
    });

    it('fresh mode arms exactly SIGINT and SIGTERM, and un-arms them when the run ends', async () => {
        const handle = makeHandle();
        const { deps, trapped } = makeInterruptDeps(handle);
        const seen: StartSignal[] = [];
        const original = deps.onSignal!;
        deps.onSignal = (signal, listener) => {
            seen.push(signal);
            return original(signal, listener);
        };
        expect(await runStartDaytona(flagsOf([]), deps)).toBe(0);
        // Exactly the two documented signals, in that order, and nothing else.
        expect(seen).toEqual(['SIGINT', 'SIGTERM']);
        expect(trapped.size).toBe(0);
        expect(handle.sandbox.delete).not.toHaveBeenCalled();
    });
});

describe('start-daytona: wiring and the import-is-a-no-op guard', () => {
    it('the default deps delegate to the real library call and the real spawner', async () => {
        const deps = createDefaultDeps();
        expect(deps.spawn).toBe(spawnDeploy);
        await deps.createSandbox({ public: true });
        expect(createSandboxMock).toHaveBeenCalledTimes(1);
        expect(createSandboxMock).toHaveBeenCalledWith({ public: true });
    });

    it('isCliEntryPoint: vitest never runs, and only the three entry rules count', () => {
        const module = '/repo/packages/underload/nexus/scripts/start-daytona.ts';
        // The vitest guard comes first, whatever else the signals say.
        expect(isCliEntryPoint({ vitest: true, argv: ['/usr/bin/node', module], modulePath: module })).toBe(false);
        // Rule 1: the script positional (node/tsx), and vite-node's --script form.
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', module], modulePath: module })).toBe(true);
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node', '--script', 'scripts/start-daytona.ts'], modulePath: module })).toBe(true);
        // Rule 2: the yarn lifecycle line ("vite-node scripts/start-daytona.ts").
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node'], env: { npm_lifecycle_script: 'vite-node scripts/start-daytona.ts' }, modulePath: module })).toBe(true);
        // Rule 3: vite-node evaluated THIS file as its entry — only runner frames above it.
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node'], modulePath: module, stack: `Error\n    at Object.<anonymous> (${module}:1:1)\n    at ViteNodeRunner.executeFile (runner.mjs:1:1)` })).toBe(true);
        // NOT an entry: an imported module has the importer's frame in that window.
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node'], modulePath: module, stack: `Error\n    at Object.<anonymous> (${module}:1:1)\n    at consumer (/repo/packages/agentic/provider/src/consumer.ts:2:2)\n    at ViteNodeRunner.executeFile (runner.mjs:1:1)` })).toBe(false);
        // NOT an entry: a bare specifier, this test file itself, or no signals at all.
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node', '@services/daytona/sandbox'], modulePath: module })).toBe(false);
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'start-daytona.test.ts'], modulePath: module })).toBe(false);
        expect(isCliEntryPoint({})).toBe(false);
    });
});
