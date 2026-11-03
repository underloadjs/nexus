// Build the wasm-bindgen JS glue around the Node-compatible WASM build.
//
// `cargo build --target wasm32-unknown-unknown` produces only the .wasm module;
// the host bindings come from the `wasm-bindgen-cli` binary, which is not a cargo
// subcommand. This script keeps that two-step build reproducible from yarn, and
// fails loudly with install guidance instead of silently leaving a stale pkg/.
//
// Layout produced in pkg/:
//   nexus_wasm.js / nexus_wasm.d.ts / nexus_wasm_bg.wasm   (--target nodejs)
// which is exactly what `examples/node-smoke.mjs` requires()s.
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, "..");
const pkg = join(root, "pkg");
const wasmOut = resolve(
    process.env.NEXUS_WASM_DIR || join(root, "..", "..", "..", ".rust", "target", "wasm32-unknown-unknown", "release"),
    "nexus_wasm.wasm",
);

// The repo-root .cargo/config.toml sets target-dir = .rust/target, so that is the
// default place the artifact appears. NEXUS_WASM_DIR overrides for CI.
function fail(message, hint) {
    console.error(`\n[nexus:wasm] ${message}`);
    if (hint) console.error(`[nexus:wasm] ${hint}\n`);
    process.exit(1);
}

function have(binary, args = ["--version"]) {
    try {
        return execFileSync(binary, args, { encoding: "utf8" }).trim();
    } catch {
        return null;
    }
}

// The `wasm-bindgen` **library** version in `Cargo.lock` and the `wasm-bindgen-cli`
// **binary** version have to match exactly: the generated glue embeds a version
// check and panics on any drift. Reading the pin out of the lock keeps this
// instruction correct instead of hardcoding a number that goes stale.
function lockedBindgenVersion() {
    const lock = join(root, "Cargo.lock");
    if (!existsSync(lock)) {
        return null;
    }
    const entry = /name = "wasm-bindgen"\r?\nversion = "([^"]+)"/.exec(readFileSync(lock, "utf8"));
    return entry ? entry[1] : null;
}

if (!existsSync(wasmOut)) {
    fail(
        `no wasm module at ${wasmOut}`,
        "run `yarn wasm` first (cargo build --release --target wasm32-unknown-unknown -p nexus-wasm)",
    );
}

// --target nodejs keeps the smoke test a plain require() with no bundler and no
// top-level await; the web build (--target web) is a separate, opt-in step.
const binary = have("wasm-bindgen") === null ? (have("wasm-bindgen-cli") === null ? null : "wasm-bindgen-cli") : "wasm-bindgen";
const pinned = lockedBindgenVersion();
if (binary === null) {
    // There is no cargo subcommand for this: `cargo run -p wasm-bindgen-cli`
    // only resolves a workspace member, and this crate is not one, so the
    // honest failure is a named install step rather than a second error.
    fail(
        "wasm-bindgen is not on PATH",
        `install the CLI at the version Cargo.lock pins the library at${pinned ? ` (${pinned})` : ""}: ` +
            `\`cargo install wasm-bindgen-cli --locked${pinned ? ` --version ${pinned}` : ""}\``,
    );
}
// A CLI that is present but a different version fails later, inside the loader,
// with a message nobody connects back to this script. Say it here instead.
const found = ((have(binary) ?? "").match(/\d+\.\d+\.\d+/) ?? [null])[0];
if (pinned && found && found !== pinned) {
    fail(
        `${binary} ${found} is on PATH, but Cargo.lock pins the wasm-bindgen library at ${pinned}`,
        `the two must match exactly: \`cargo install wasm-bindgen-cli --locked --version ${pinned}\``,
    );
}
const args = ["--target", "nodejs", "--out-dir", pkg, "--out-name", "nexus_wasm", wasmOut];
execFileSync(binary, args, { stdio: "inherit" });

if (!existsSync(join(pkg, "nexus_wasm.js")) || !existsSync(join(pkg, "nexus_wasm_bg.wasm"))) {
    const produced = readdirSync(pkg).join(", ");
    fail(`wasm-bindgen did not produce the expected files in pkg/ (found: ${produced || "nothing"})`);
}

// The generated glue is CommonJS (`module.exports`), while this package is
// `"type": "module"`. Without a nested manifest Node parses `pkg/*.js` as ESM
// and every `require()` of the glue fails with "module is not defined".
if (!existsSync(join(pkg, "package.json"))) {
    writeFileSync(join(pkg, "package.json"), `${JSON.stringify({ type: "commonjs" }, null, 4)}\n`);
}
console.log(`[nexus:wasm] glue written to ${pkg}`);
