// @effect-diagnostics nodeBuiltinImport:off - the generated remote shell regression test drives a real child HTTP server and POSIX shell.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NetService from "@t3tools/shared/Net";
import { HostProcessArchitecture, HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { SshPasswordPrompt } from "./auth.ts";
import { SshCommandError } from "./errors.ts";
import {
  buildRemoteLaunchScript,
  buildRemotePairingScript,
  buildRemoteStopScript,
  buildRemoteT3RunnerIdentity,
  buildRemoteT3RunnerScript,
  SshInvalidArchiveVersionError,
  SshMissingRunnerError,
  describeReadinessCause,
  issueRemotePairingToken,
  launchOrReuseRemoteServer,
  REMOTE_PICK_PORT_SCRIPT,
  SshEnvironmentManager,
  waitForHttpReady,
} from "./tunnel.ts";

const TEST_NODE_ENGINE_RANGE = "^22.16 || ^23.11 || >=24.10";

function readProcessStart(pid: number): string {
  const procStatPath = `/proc/${pid}/stat`;
  if (NodeFS.existsSync(procStatPath)) {
    const stat = NodeFS.readFileSync(procStatPath, "utf8");
    const fields = stat
      .slice(stat.lastIndexOf(") ") + 2)
      .trim()
      .split(/\s+/u);
    return `pid:${pid}:proc:${fields[19] ?? ""}`;
  }
  const result = NodeChildProcess.spawnSync("ps", ["-o", "lstart=", "-p", `${pid}`], {
    encoding: "utf8",
  });
  return `pid:${pid}:ps:${result.stdout.trim().replace(/\s+/gu, " ")}`;
}

function buildFastRemoteLaunchScript(input?: Parameters<typeof buildRemoteLaunchScript>[0]) {
  return buildRemoteLaunchScript(input)
    .replaceAll('wait_ready "15000"', 'wait_ready "300"')
    .replaceAll('wait_ready "60000"', 'wait_ready "300"')
    .replace('node - "$REMOTE_PORT" "$1" "1000"', 'node - "$REMOTE_PORT" "$1" "100"')
    .replaceAll(
      'while kill -0 "$PID_TO_WAIT" 2>/dev/null && [ "$WAIT_COUNT" -lt 20 ]; do',
      'while kill -0 "$PID_TO_WAIT" 2>/dev/null && [ "$WAIT_COUNT" -lt 2 ]; do',
    );
}

function withDefaultRemotePort(script: string, port: number): string {
  return script.replace('node - "$PORT_FILE" "3773" "200"', `node - "$PORT_FILE" "${port}" "200"`);
}

async function reserveLoopbackPort(): Promise<number> {
  const reservation = NodeNet.createServer();
  await new Promise<void>((resolve, reject) => {
    reservation.once("error", reject);
    reservation.listen(0, "127.0.0.1", resolve);
  });
  const address = reservation.address();
  assert.isObject(address);
  const port = typeof address === "object" && address !== null ? address.port : 0;
  await new Promise<void>((resolve, reject) =>
    reservation.close((error) => (error ? reject(error) : resolve())),
  );
  return port;
}

async function spawnPersistentManagedProcess(signalMarker: string) {
  const child = NodeChildProcess.spawn(
    process.execPath,
    [
      "-e",
      'const fs = require("node:fs"); const marker = process.argv[1]; process.on("SIGTERM", () => fs.writeFileSync(marker, "signaled\\n")); process.stdout.write("ready\\n"); setInterval(() => {}, 1000);',
      signalMarker,
    ],
    { stdio: ["ignore", "pipe", "inherit"] },
  );
  await new Promise<void>((resolve, reject) => {
    const handleExit = (code: number | null, signal: NodeJS.Signals | null) => {
      reject(
        new Error(
          "managed test process exited before ready: " + String(code === null ? signal : code),
        ),
      );
    };
    child.once("error", reject);
    child.once("exit", handleExit);
    child.stdout?.once("data", () => {
      child.off("error", reject);
      child.off("exit", handleExit);
      resolve();
    });
  });
  assert.isNumber(child.pid);
  return child;
}

async function forceStopTestProcess(child: NodeChildProcess.ChildProcess) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  await new Promise<void>((resolve) => {
    child.once("exit", () => resolve());
    child.kill("SIGKILL");
  });
}

function spawnShellScript(
  input: string,
  env: NodeJS.ProcessEnv,
  args: ReadonlyArray<string> = ["-s"],
) {
  const child = NodeChildProcess.spawn("sh", args, {
    env,
    stdio: ["pipe", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (chunk: string) => {
    stdout += chunk;
  });
  child.stderr.on("data", (chunk: string) => {
    stderr += chunk;
  });
  child.stdin.end(input);
  const result = new Promise<{ status: number | null; stdout: string; stderr: string }>(
    (resolve, reject) => {
      child.once("error", reject);
      child.once("close", (status) => resolve({ status, stdout, stderr }));
    },
  );
  return { child, result };
}

async function runShellScript(
  input: string,
  env: NodeJS.ProcessEnv,
  args: ReadonlyArray<string> = ["-s"],
) {
  return spawnShellScript(input, env, args).result;
}

async function waitForShellStderr(
  child: NodeChildProcess.ChildProcessWithoutNullStreams,
  marker: string,
) {
  await new Promise<void>((resolve, reject) => {
    const onData = (chunk: string) => {
      if (chunk.includes(marker)) {
        child.stderr.off("data", onData);
        resolve();
      }
    };
    child.stderr.on("data", onData);
    child.once("exit", (code, signal) => {
      reject(new Error(`shell exited before ${marker}: ${String(code ?? signal)}`));
    });
  });
}

function writeTrackedManagedState(input: {
  readonly home: string;
  readonly stateKey: string;
  readonly pid: number;
  readonly port: number;
  readonly runnerIdentity: string;
}) {
  const stateDir = NodePath.join(input.home, ".t3", "ssh-launch", input.stateKey);
  const userdataDir = NodePath.join(input.home, ".t3", "userdata");
  NodeFS.mkdirSync(stateDir, { recursive: true });
  NodeFS.mkdirSync(userdataDir, { recursive: true });
  NodeFS.writeFileSync(NodePath.join(stateDir, "pid"), String(input.pid) + "\n");
  NodeFS.writeFileSync(NodePath.join(stateDir, "port"), String(input.port) + "\n");
  NodeFS.writeFileSync(NodePath.join(stateDir, "managed"), "managed\n");
  NodeFS.writeFileSync(
    NodePath.join(stateDir, "process-start"),
    readProcessStart(input.pid) + "\n",
  );
  NodeFS.writeFileSync(NodePath.join(stateDir, "runner-id"), input.runnerIdentity + "\n");
  NodeFS.writeFileSync(
    NodePath.join(userdataDir, "server-runtime.json"),
    JSON.stringify({
      pid: input.pid,
      port: input.port,
      origin: "http://127.0.0.1:" + String(input.port),
    }),
  );
  return stateDir;
}

const makeSuccessfulProcess = (stdout: string) => {
  const stdoutStream = Stream.make(new TextEncoder().encode(stdout));
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(123),
    stdout: stdoutStream,
    stderr: Stream.empty,
    all: stdoutStream,
    exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
    isRunning: Effect.succeed(false),
    kill: () => Effect.void,
    stdin: Sink.drain,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
    unref: Effect.succeed(Effect.void),
  });
};

const makeDelayedSuccessfulProcess = (stdout: string, delayMs: number) => {
  const process = makeSuccessfulProcess(stdout);
  return {
    ...process,
    exitCode: Effect.sleep(Duration.millis(delayMs)).pipe(
      Effect.as(ChildProcessSpawner.ExitCode(0)),
    ),
  };
};

const makeRunningProcess = (onKill: () => void) => {
  let finish: ((exitCode: ChildProcessSpawner.ExitCode) => void) | null = null;
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(123),
    stdout: Stream.empty,
    stderr: Stream.empty,
    all: Stream.empty,
    exitCode: Effect.callback<ChildProcessSpawner.ExitCode>((resume) => {
      finish = (exitCode) => resume(Effect.succeed(exitCode));
      return Effect.sync(() => {
        finish = null;
      });
    }),
    isRunning: Effect.succeed(true),
    kill: () =>
      Effect.sync(() => {
        onKill();
        finish?.(ChildProcessSpawner.ExitCode(143));
      }),
    stdin: Sink.drain,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
    unref: Effect.succeed(Effect.void),
  });
};

const testHttpClient = HttpClient.make((request) =>
  Effect.succeed(HttpClientResponse.fromWeb(request, new Response("", { status: 200 }))),
);

const hangingHttpClient = HttpClient.make(() => Effect.never);

const testNetService = NetService.NetService.of({
  canListenOnHost: () => Effect.succeed(true),
  isPortAvailableOnLoopback: () => Effect.succeed(true),
  hasListenerOnHost: () => Effect.succeed(false),
  reserveLoopbackPort: () => Effect.succeed(41_773),
  findAvailablePort: (preferred) => Effect.succeed(preferred),
});

function commandArgs(command: ChildProcess.Command): ReadonlyArray<string> {
  return command._tag === "StandardCommand" ? command.args : [];
}

const ARCHIVE = { archiveVersion: "1.2.3-preview.20260911.4" } as const;
const NODE_SCRIPT = {
  nodeScriptPath: "/Users/julius/Development/Work/codething-mvp/apps/server/dist/bin.mjs",
} as const;

describe("ssh tunnel scripts", () => {
  it("installs and runs the release archive without Node, npm, or npx", () => {
    const script = buildRemoteT3RunnerScript(ARCHIVE);

    assert.include(script, "T3_ARCHIVE_VERSION='1.2.3-preview.20260911.4'");
    assert.include(script, "T3_NODE_SCRIPT_PATH=''");
    assert.include(
      script,
      "T3_RELEASE_BASE_URL='https://github.com/pingdotgg/t3code/releases/download'",
    );
    assert.include(script, 'T3_RUNTIME_DIR="$HOME/.t3/runtime/versions/$T3_ARCHIVE_VERSION"');
    assert.include(script, 'T3_ARCHIVE="t3-$T3_ARCHIVE_VERSION-$T3_PLATFORM-$T3_ARCH.tar.gz"');
    assert.include(script, "SHA256SUMS");
    assert.include(script, 'exec "$T3_RUNTIME_DIR/t3" "$@"');
    assert.notInclude(script, "npx");
    assert.notInclude(script, "npm exec");
    assert.notInclude(script, "t3@latest");
    assert.notInclude(script, 'exec t3 "$@"');
    // Concurrent launches serialize on a per-version mkdir lock and recheck
    // the completion marker after acquiring it.
    assert.include(
      script,
      'T3_LOCK="$HOME/.t3/runtime/versions/.$T3_ARCHIVE_VERSION.install.lock"',
    );
    // mkdir is the exclusive create; the pid follows atomically. A dead owner
    // is reclaimed at once, a never-published owner after a short grace.
    assert.include(script, 'while ! mkdir "$T3_LOCK" 2>/dev/null; do');
    assert.include(script, 'mv "$T3_LOCK/pid.tmp" "$T3_LOCK/pid"');
    assert.include(script, 'if ! kill -0 "$T3_LOCK_OWNER" 2>/dev/null; then');
    assert.include(script, 'if [ "$T3_LOCK_UNOWNED" -ge 5 ]; then');
    assert.include(script, 'if [ "$T3_LOCK_WAITED" -ge 360 ]; then');
    assert.include(script, '"$T3_STAGING/SHA256SUMS" 30');
    assert.include(script, '"$T3_STAGING/$T3_ARCHIVE" 240');
    assert.notInclude(script, "T3_LOCK_CANDIDATE");
    assert.notInclude(script, "-mmin");
    assert.equal(script.split("if ! t3_runtime_ready; then").length - 1, 2);
    assert.isBelow(
      script.indexOf('"$T3_STAGING/t3" --version'),
      script.indexOf('> "$T3_STAGING/.install-complete"'),
    );
    // Node discovery is defined for the dev path but only ever invoked inside
    // the node-script branch, which the archive path skips entirely.
    assert.equal(script.split("ensure_remote_node_path || true").length - 1, 1);
    assert.isBelow(
      script.indexOf("ensure_remote_node_path || true"),
      script.indexOf('exec node "$T3_NODE_SCRIPT_PATH" "$@"'),
    );
    assert.isBelow(
      script.indexOf('exec node "$T3_NODE_SCRIPT_PATH" "$@"'),
      script.indexOf("T3_ARCHIVE_VERSION="),
    );

    const launch = buildRemoteLaunchScript({
      ...ARCHIVE,
      releaseBaseUrl: "https://mirror.example/t3/",
    });
    assert.include(launch, "T3_ARCHIVE_MODE=1");
    assert.include(launch, "T3_RELEASE_BASE_URL='https://mirror.example/t3'");
    assert.include(launch, '"$RUNNER_FILE" __ssh-helper pick-port "$PORT_FILE"');
    assert.include(launch, '"$RUNNER_FILE" __ssh-helper wait-ready "$REMOTE_PORT"');
    assert.include(launch, '"$RUNNER_FILE" __ssh-helper runtime-port "$DEFAULT_RUNTIME_FILE"');
    assert.include(buildRemoteLaunchScript(NODE_SCRIPT), "T3_ARCHIVE_MODE=0");
  });

  it("rejects archive versions that are not a single exact version segment", () => {
    for (const archiveVersion of [
      "../other",
      "1.2.3/evil",
      "1.2.3\\evil",
      "1.2.3-preview.1 x",
      "1.2.3-preview.1\nrm -rf /",
      "v1.2.3",
    ]) {
      assert.throws(
        () => buildRemoteT3RunnerScript({ archiveVersion }),
        SshInvalidArchiveVersionError,
        undefined,
        archiveVersion,
      );
    }
    assert.include(
      buildRemoteT3RunnerScript(ARCHIVE),
      "T3_ARCHIVE_VERSION='1.2.3-preview.20260911.4'",
    );
  });

  it("refuses to build a runner with neither an archive version nor a node script", () => {
    for (const input of [undefined, {}, { archiveVersion: "  " }, { nodeScriptPath: null }]) {
      assert.throws(() => buildRemoteT3RunnerScript(input), SshMissingRunnerError);
    }
    assert.throws(() => buildRemoteLaunchScript(), SshMissingRunnerError);
  });

  it("does not hard-code a remote node engine range", () => {
    const script = buildRemoteT3RunnerScript(NODE_SCRIPT);

    assert.include(script, "T3_NODE_ENGINE_RANGE=''");
    assert.notInclude(script, TEST_NODE_ENGINE_RANGE);
  });

  it("builds the remote t3 runner with a node script override", () => {
    const script = buildRemoteT3RunnerScript({
      ...NODE_SCRIPT,
      nodeEngineRange: TEST_NODE_ENGINE_RANGE,
    });

    assert.include(
      script,
      "T3_NODE_SCRIPT_PATH='/Users/julius/Development/Work/codething-mvp/apps/server/dist/bin.mjs'",
    );
    assert.include(script, 'exec node "$T3_NODE_SCRIPT_PATH" "$@"');
    assert.include(script, "T3_ARCHIVE_VERSION=''");
    assert.include(script, 'prepend_path_if_dir "$HOME/.local/bin"');
    assert.include(script, `T3_NODE_ENGINE_RANGE='${TEST_NODE_ENGINE_RANGE}'`);
    assert.include(script, "remote_node_satisfies_engine()");
    assert.include(script, "function satisfiesSemverRange");
    assert.include(script, "satisfiesSemverRange(rawVersion, range)");
    assert.include(script, 'prepend_path_if_dir "$VOLTA_HOME/bin"');
    assert.include(script, 'prepend_path_if_dir "$HOME/.asdf/shims"');
    assert.include(script, 'prepend_path_if_dir "$HOME/.local/share/mise/shims"');
    assert.include(script, 'eval "$(fnm env --shell bash)"');
    assert.include(script, "fnm use --silent-if-unchanged");
    assert.include(script, "fnm use default");
    assert.include(script, 'prepend_path_if_dir "$HOME/.nodenv/shims"');
    assert.include(script, 'NVM_DIR="$HOME/.nvm"');
    assert.include(script, "nvm use --silent default");
    assert.include(script, 'for T3_NODE_BIN in "$NVM_DIR"/versions/node/*/bin');
    assert.notInclude(script, "ensure $NVM_DIR/nvm.sh is available");
    assert.notInclude(script, "npx");
  });

  it("builds the remote t3 runner with a node script override", () => {
    const runner = {
      nodeScriptPath: "/Users/julius/Development/Work/codething-mvp/apps/server/dist/bin.mjs",
    } as const;
    const script = buildRemoteT3RunnerScript(runner);
    const launchScript = buildRemoteLaunchScript(runner);

    assert.include(
      script,
      "T3_NODE_SCRIPT_PATH='/Users/julius/Development/Work/codething-mvp/apps/server/dist/bin.mjs'",
    );
    assert.include(script, 'exec node "$T3_NODE_SCRIPT_PATH" "$@"');
    assert.include(script, "REMOTE_PID=$$");
    assert.notInclude(script, "REMOTE_PID=$\n");
    assert.include(launchScript, "REMOTE_PID=$$");
    assert.notInclude(launchScript, "REMOTE_PID=$\n");
  });

  it("distinguishes node-script builds at the same path without changing package identities", () => {
    const nodeScriptPath = "/home/user/t3/apps/server/dist/bin.mjs";
    const previousBuild = buildRemoteT3RunnerIdentity({
      nodeScriptPath,
      nodeScriptBuildIdentity: "commit-a",
    });
    const nextBuild = buildRemoteT3RunnerIdentity({
      nodeScriptPath,
      nodeScriptBuildIdentity: "commit-b",
    });

    assert.notEqual(previousBuild, nextBuild);
    assert.equal(
      buildRemoteT3RunnerIdentity({ archiveVersion: "0.0.36" }),
      buildRemoteT3RunnerIdentity({
        archiveVersion: "0.0.36",
        nodeScriptBuildIdentity: "ignored-for-package-runners",
      }),
    );
  });

  it("uses the remote t3 runner for launch and pairing scripts", () => {
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;
    const launch = buildRemoteLaunchScript(ARCHIVE);
    const devLaunch = buildRemoteLaunchScript({
      ...NODE_SCRIPT,
      nodeEngineRange: TEST_NODE_ENGINE_RANGE,
    });

    assert.include(
      launch,
      'REMOTE_PROCESS_START="$(cat "$PROCESS_START_FILE" 2>/dev/null || true)"',
    );
    assert.include(launch, "RUNNER_CHANGED=1");
    assert.include(launch, "ensure_remote_node_path()");
    assert.include(launch, "if ! ensure_remote_node_path; then");
    assert.include(devLaunch, `T3_NODE_ENGINE_RANGE='${TEST_NODE_ENGINE_RANGE}'`);
    assert.include(devLaunch, "does not satisfy required range ");
    assert.include(launch, 'kill "$REMOTE_PID" 2>/dev/null || true');
    assert.include(launch, "wait_ready");
    assert.include(launch, '"$RUNNER_FILE" serve --host 127.0.0.1');
    assert.include(launch, '--base-dir "$DEFAULT_SERVER_HOME"');
    assert.notInclude(launch, "server-home");
    assert.include(launch, "Remote T3 server did not become ready");
    assert.include(launch, 'wait_ready "60000"');
    assert.include(launch, 'if [ -s "$LOG_FILE" ]; then');
    assert.include(launch, "It wrote nothing to %s");
    assert.include(launch, "T3_ARCHIVE_VERSION='1.2.3-preview.20260911.4'");
    assert.include(
      buildRemotePairingScript(target, ARCHIVE),
      '"$RUNNER_FILE" auth pairing create --base-dir "$PAIRING_BASE_DIR" --json',
    );
    assert.include(
      buildRemotePairingScript(target, ARCHIVE),
      'PAIRING_BASE_DIR="$DEFAULT_SERVER_HOME"',
    );
    assert.notInclude(buildRemotePairingScript(target, ARCHIVE), "server-home");
    assert.include(
      buildRemotePairingScript(target, ARCHIVE),
      "T3_ARCHIVE_VERSION='1.2.3-preview.20260911.4'",
    );
    assert.include(
      buildRemoteStopScript(target),
      'if [ "$REMOTE_MANAGED" != "external" ] && managed_pid_is_owned',
    );
    assert.include(buildRemoteStopScript(target), 'kill "$REMOTE_PID" 2>/dev/null || true');
    assert.include(buildRemoteStopScript(target), 'PROCESS_START_FILE="$STATE_DIR/process-start"');
    assert.include(buildRemoteStopScript(target), "managed_pid_is_owned");
    assert.include(buildRemoteLaunchScript(ARCHIVE), "acquire_state_lifecycle_lock");
    assert.include(buildRemotePairingScript(target, ARCHIVE), "acquire_state_lifecycle_lock");
    assert.include(buildRemoteStopScript(target), "acquire_state_lifecycle_lock");
    assert.include(
      launch,
      'DEFAULT_RUNTIME_FILE="$DEFAULT_SERVER_HOME/userdata/server-runtime.json"',
    );
    assert.include(buildRemoteLaunchScript(ARCHIVE), "resolve_default_runtime_port()");
    assert.include(
      buildRemoteLaunchScript(ARCHIVE),
      'DEFAULT_RUNTIME_INFO="$(resolve_default_runtime_port',
    );
    assert.include(
      buildRemoteLaunchScript(ARCHIVE),
      "if (!Number.isInteger(pid) || pid <= 0 || !Number.isInteger(port))",
    );
    assert.include(buildRemoteLaunchScript(ARCHIVE), "DEFAULT_RUNTIME_IS_TRACKED_MANAGED=0");
    assert.include(buildRemoteLaunchScript(ARCHIVE), '[ "$REMOTE_PID" = "$DEFAULT_RUNTIME_PID" ]');
    assert.include(
      buildRemoteLaunchScript(ARCHIVE),
      'PROCESS_START_FILE="$STATE_DIR/process-start"',
    );
    assert.include(buildRemoteLaunchScript(ARCHIVE), "managed_pid_is_owned");
    assert.include(buildRemoteLaunchScript(ARCHIVE), 'REMOTE_PORT="$DEFAULT_REMOTE_PORT"');
    assert.include(buildRemoteLaunchScript(ARCHIVE), 'rm -f "$PID_FILE"');
    assert.include(buildRemoteLaunchScript(ARCHIVE), "printf 'external\\n' >\"$MANAGED_FILE\"");
    assert.include(buildRemoteLaunchScript(ARCHIVE), 'if [ -z "$REMOTE_PORT" ]; then');
    assert.isBelow(
      launch.indexOf('if [ "$REMOTE_MANAGED" = "managed" ]'),
      launch.indexOf("printf 'external\\n' >\"$MANAGED_FILE\""),
    );
    assert.isBelow(
      buildRemoteLaunchScript(ARCHIVE).indexOf(
        'DEFAULT_RUNTIME_INFO="$(resolve_default_runtime_port',
      ),
      buildRemoteLaunchScript(ARCHIVE).indexOf(
        'elif [ -n "$REMOTE_PORT" ] && managed_pid_is_owned',
      ),
    );
  });

  it("reuses a healthy managed runtime when equivalent runner bytes differ", async () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-ssh-managed-runtime-"));
    const home = NodePath.join(root, "home");
    const stateKey = "managed-runtime-regression";
    const stateDir = NodePath.join(home, ".t3", "ssh-launch", stateKey);
    const userdataDir = NodePath.join(home, ".t3", "userdata");
    const runner = {
      nodeScriptPath: "/unused/t3-server.mjs",
    } as const;
    const reservation = NodeNet.createServer();
    await new Promise<void>((resolve, reject) => {
      reservation.once("error", reject);
      reservation.listen(0, "127.0.0.1", resolve);
    });
    const address = reservation.address();
    assert.isObject(address);
    const port = typeof address === "object" && address !== null ? address.port : 0;
    await new Promise<void>((resolve, reject) =>
      reservation.close((error) => (error ? reject(error) : resolve())),
    );
    const server = NodeChildProcess.spawn(
      process.execPath,
      [
        "-e",
        'const http = require("node:http"); const port = Number(process.argv[1]); const server = http.createServer((_request, response) => { response.writeHead(200); response.end("ok"); }); setTimeout(() => server.listen(port, "127.0.0.1"), 3000);',
        `${port}`,
      ],
      { stdio: ["ignore", "ignore", "inherit"] },
    );

    try {
      assert.isNumber(server.pid);
      NodeFS.mkdirSync(stateDir, { recursive: true });
      NodeFS.mkdirSync(userdataDir, { recursive: true });
      NodeFS.writeFileSync(NodePath.join(stateDir, "pid"), `${server.pid}\n`);
      NodeFS.writeFileSync(NodePath.join(stateDir, "port"), `${port}\n`);
      NodeFS.writeFileSync(NodePath.join(stateDir, "managed"), "managed\n");
      NodeFS.writeFileSync(
        NodePath.join(stateDir, "process-start"),
        `${readProcessStart(server.pid!)}\n`,
      );
      NodeFS.writeFileSync(NodePath.join(stateDir, "run-t3.sh"), "previously bundled formatting\n");
      NodeFS.writeFileSync(
        NodePath.join(stateDir, "runner-id"),
        `${buildRemoteT3RunnerIdentity(runner)}\n`,
      );
      NodeFS.writeFileSync(
        NodePath.join(userdataDir, "server-runtime.json"),
        JSON.stringify({ pid: server.pid, port, origin: `http://127.0.0.1:${port}` }),
      );

      const result = NodeChildProcess.spawnSync("sh", ["-s", "--", stateKey], {
        encoding: "utf8",
        env: { ...process.env, HOME: home },
        input: buildRemoteLaunchScript(runner),
        timeout: 10_000,
      });

      if ((result.error as NodeJS.ErrnoException | undefined)?.code === "ENOENT") {
        return;
      }

      assert.equal(result.status, 0, result.stderr || result.error?.message);
      const output = result.stdout.trim();
      assert.isNotEmpty(output, JSON.stringify({ stderr: result.stderr, error: result.error }));
      assert.deepEqual(JSON.parse(output), {
        remotePort: port,
        serverKind: "managed",
      });
      assert.isNull(server.exitCode, "the reconnect script must not stop the tracked runtime");
      assert.equal(
        NodeFS.readFileSync(NodePath.join(stateDir, "pid"), "utf8").trim(),
        `${server.pid}`,
      );
      assert.equal(
        NodeFS.readFileSync(NodePath.join(stateDir, "managed"), "utf8").trim(),
        "managed",
      );
      assert.equal(
        NodeFS.readFileSync(NodePath.join(stateDir, "run-t3.sh"), "utf8"),
        `${buildRemoteT3RunnerScript(runner)}\n`,
      );

      NodeFS.writeFileSync(
        NodePath.join(stateDir, "process-start"),
        `pid:${server.pid}:ps:stale-process-start\n`,
      );
      const staleIdentityResult = NodeChildProcess.spawnSync("sh", ["-s", "--", stateKey], {
        encoding: "utf8",
        env: { ...process.env, HOME: home },
        input: buildRemoteLaunchScript(runner),
        timeout: 10_000,
      });

      assert.equal(
        staleIdentityResult.status,
        0,
        staleIdentityResult.stderr || staleIdentityResult.error?.message,
      );
      assert.deepEqual(JSON.parse(staleIdentityResult.stdout.trim()), {
        remotePort: port,
        serverKind: "external",
      });
      assert.isNull(server.exitCode, "a stale PID identity must never be signaled");
      assert.isFalse(NodeFS.existsSync(NodePath.join(stateDir, "pid")));
      assert.isFalse(NodeFS.existsSync(NodePath.join(stateDir, "process-start")));
    } finally {
      if (server.exitCode === null) {
        server.kill();
      }
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });

  it("keeps an unready exact managed process alive and tracked for retry", async () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-ssh-unready-managed-"));
    const home = NodePath.join(root, "home");
    const stateKey = "unready-managed-regression";
    const signalMarker = NodePath.join(root, "signaled");
    const runner = { nodeScriptPath: "/unused/t3-server.mjs" } as const;
    const port = await reserveLoopbackPort();
    const server = await spawnPersistentManagedProcess(signalMarker);

    try {
      const pid = server.pid;
      assert.isNumber(pid);
      const stateDir = writeTrackedManagedState({
        home,
        stateKey,
        pid: pid!,
        port,
        runnerIdentity: buildRemoteT3RunnerIdentity(runner),
      });

      const result = NodeChildProcess.spawnSync("sh", ["-s", "--", stateKey], {
        encoding: "utf8",
        env: { ...process.env, HOME: home },
        input: buildFastRemoteLaunchScript(runner),
        timeout: 5_000,
      });

      if ((result.error as NodeJS.ErrnoException | undefined)?.code === "ENOENT") {
        return;
      }

      assert.notEqual(result.status, 0, result.stderr || result.error?.message);
      assert.include(result.stderr, "did not answer readiness checks");
      assert.include(result.stderr, "leaving it running and tracked");
      assert.isFalse(
        NodeFS.existsSync(signalMarker),
        "a readiness timeout must not signal the exact managed process",
      );
      assert.isNull(server.exitCode, "the exact managed process must remain alive");
      assert.equal(NodeFS.readFileSync(NodePath.join(stateDir, "pid"), "utf8").trim(), String(pid));
      assert.equal(
        NodeFS.readFileSync(NodePath.join(stateDir, "process-start"), "utf8").trim(),
        readProcessStart(pid!),
      );
      assert.equal(
        NodeFS.readFileSync(NodePath.join(stateDir, "runner-id"), "utf8").trim(),
        buildRemoteT3RunnerIdentity(runner),
      );
    } finally {
      await forceStopTestProcess(server);
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });

  it("keeps a newly launched unready process alive and exactly tracked", async () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-ssh-unready-launch-"));
    const home = NodePath.join(root, "home");
    const stateKey = "unready-launch-regression";
    const stateDir = NodePath.join(home, ".t3", "ssh-launch", stateKey);
    const serverEntry = NodePath.join(root, "unready-server.cjs");
    const launchMarker = NodePath.join(root, "launches");
    const signalMarker = NodePath.join(root, "signaled");
    const port = await reserveLoopbackPort();
    NodeFS.mkdirSync(stateDir, { recursive: true });
    NodeFS.writeFileSync(NodePath.join(stateDir, "port"), String(port) + "\n");
    NodeFS.writeFileSync(
      serverEntry,
      [
        'const fs = require("node:fs");',
        'const http = require("node:http");',
        "const launchMarker = " + JSON.stringify(launchMarker) + ";",
        "const signalMarker = " + JSON.stringify(signalMarker) + ";",
        'const args = process.argv.slice(2); const port = Number(args[args.indexOf("--port") + 1]);',
        'fs.appendFileSync(launchMarker, String(process.pid) + "\\n");',
        'process.on("SIGTERM", () => fs.appendFileSync(signalMarker, "signaled\\n"));',
        'http.createServer((_request, response) => { response.writeHead(503); response.end("not ready"); }).listen(port, "127.0.0.1");',
      ].join("\n"),
    );
    const runner = { nodeScriptPath: serverEntry } as const;
    let launchedPid: number | null = null;

    try {
      const result = NodeChildProcess.spawnSync("sh", ["-s", "--", stateKey], {
        encoding: "utf8",
        env: { ...process.env, HOME: home },
        input: buildFastRemoteLaunchScript(runner).replaceAll(
          'wait_ready "300"',
          'wait_ready "1000"',
        ),
        timeout: 5_000,
      });

      if ((result.error as NodeJS.ErrnoException | undefined)?.code === "ENOENT") {
        return;
      }

      assert.notEqual(
        result.status,
        0,
        JSON.stringify({
          stdout: result.stdout,
          stderr: result.stderr,
          error: result.error?.message,
        }),
      );
      assert.include(result.stderr, "did not become ready");
      assert.include(result.stderr, "remains tracked for a later readiness retry");
      launchedPid = Number(NodeFS.readFileSync(NodePath.join(stateDir, "pid"), "utf8").trim());
      assert.isTrue(Number.isInteger(launchedPid) && launchedPid > 0);
      assert.equal(
        NodeFS.readFileSync(NodePath.join(stateDir, "process-start"), "utf8").trim(),
        readProcessStart(launchedPid),
      );
      assert.equal(
        NodeFS.readFileSync(NodePath.join(stateDir, "managed"), "utf8").trim(),
        "managed",
      );
      assert.equal(
        Number(NodeFS.readFileSync(NodePath.join(stateDir, "port"), "utf8").trim()),
        port,
      );
      assert.equal(
        NodeFS.readFileSync(NodePath.join(stateDir, "runner-id"), "utf8").trim(),
        buildRemoteT3RunnerIdentity(runner),
      );
      assert.deepEqual(
        NodeFS.readFileSync(launchMarker, "utf8").trim().split(/\s+/u),
        [String(launchedPid)],
        "a readiness miss must not launch a replacement",
      );
      assert.isFalse(
        NodeFS.existsSync(signalMarker),
        "a readiness miss must not signal the newly tracked process",
      );
      assert.doesNotThrow(() => process.kill(launchedPid!, 0));
    } finally {
      if (launchedPid !== null) {
        try {
          process.kill(launchedPid, "SIGKILL");
        } catch {
          // The test process may already have exited after a failed assertion.
        }
      }
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });

  it("keeps a same-path build restart pending until the exact managed process exits", async () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-ssh-pending-restart-"));
    const home = NodePath.join(root, "home");
    const stateKey = "pending-runner-restart-regression";
    const signalMarker = NodePath.join(root, "signaled");
    const nodeScriptPath = "/deployed/t3-server.mjs";
    const previousRunner = {
      nodeScriptPath,
      nodeScriptBuildIdentity: "commit-a",
    } as const;
    const nextRunner = {
      nodeScriptPath,
      nodeScriptBuildIdentity: "commit-b",
    } as const;
    const port = await reserveLoopbackPort();
    const server = await spawnPersistentManagedProcess(signalMarker);

    try {
      const pid = server.pid;
      assert.isNumber(pid);
      const previousRunnerIdentity = buildRemoteT3RunnerIdentity(previousRunner);
      const stateDir = writeTrackedManagedState({
        home,
        stateKey,
        pid: pid!,
        port,
        runnerIdentity: previousRunnerIdentity,
      });

      const result = NodeChildProcess.spawnSync("sh", ["-s", "--", stateKey], {
        encoding: "utf8",
        env: { ...process.env, HOME: home },
        input: buildFastRemoteLaunchScript(nextRunner),
        timeout: 5_000,
      });

      if ((result.error as NodeJS.ErrnoException | undefined)?.code === "ENOENT") {
        return;
      }

      assert.notEqual(result.status, 0, result.stderr || result.error?.message);
      assert.include(result.stderr, "runner update requested a restart");
      assert.include(result.stderr, "runner update pending");
      assert.isTrue(
        NodeFS.existsSync(signalMarker),
        "a runner update may request an exact managed process restart",
      );
      assert.isNull(server.exitCode, "a replacement must not start while the prior PID is alive");
      assert.equal(NodeFS.readFileSync(NodePath.join(stateDir, "pid"), "utf8").trim(), String(pid));
      assert.equal(
        NodeFS.readFileSync(NodePath.join(stateDir, "process-start"), "utf8").trim(),
        readProcessStart(pid!),
      );
      assert.equal(
        NodeFS.readFileSync(NodePath.join(stateDir, "runner-id"), "utf8").trim(),
        previousRunnerIdentity,
        "the old identity keeps the requested runner restart pending on retry",
      );
    } finally {
      await forceStopTestProcess(server);
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });

  it("migrates and stops a legacy package entrypoint through an explicit node runner", async () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-ssh-package-migration-"));
    const home = NodePath.join(root, "home");
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;
    const stopScript = buildRemoteStopScript(target);
    const stateKey = /ssh-launch\/([^"/]+)"/u.exec(stopScript)?.[1];
    assert.isString(stateKey);
    if (!stateKey) throw new Error("stop script did not contain its SSH state key");
    const stateDir = NodePath.join(home, ".t3", "ssh-launch", stateKey);
    const userdataDir = NodePath.join(home, ".t3", "userdata");
    const serverEntry = NodePath.join(root, "node_modules", "t3", "dist", "bin.mjs");
    const runner = { nodeScriptPath: serverEntry } as const;
    NodeFS.mkdirSync(NodePath.dirname(serverEntry), { recursive: true });
    NodeFS.writeFileSync(
      serverEntry,
      'import http from "node:http"; const args = process.argv.slice(2); const port = Number(args[args.indexOf("--port") + 1]); const server = http.createServer((_request, response) => { response.writeHead(200); response.end("ok"); }); server.listen(port, "127.0.0.1");',
    );
    const reservation = NodeNet.createServer();
    await new Promise<void>((resolve, reject) => {
      reservation.once("error", reject);
      reservation.listen(0, "127.0.0.1", resolve);
    });
    const address = reservation.address();
    assert.isObject(address);
    const port = typeof address === "object" && address !== null ? address.port : 0;
    await new Promise<void>((resolve, reject) =>
      reservation.close((error) => (error ? reject(error) : resolve())),
    );
    const server = NodeChildProcess.spawn(
      process.execPath,
      [
        serverEntry,
        "serve",
        "--host",
        "127.0.0.1",
        "--port",
        `${port}`,
        "--base-dir",
        NodePath.join(home, ".t3"),
      ],
      { stdio: ["ignore", "ignore", "inherit"] },
    );

    try {
      assert.isNumber(server.pid);
      NodeFS.mkdirSync(stateDir, { recursive: true });
      NodeFS.mkdirSync(userdataDir, { recursive: true });
      NodeFS.writeFileSync(NodePath.join(stateDir, "pid"), `${server.pid}\n`);
      NodeFS.writeFileSync(NodePath.join(stateDir, "port"), `${port}\n`);
      NodeFS.writeFileSync(NodePath.join(stateDir, "managed"), "managed\n");
      NodeFS.writeFileSync(NodePath.join(stateDir, "run-t3.sh"), "legacy package runner\n");
      NodeFS.writeFileSync(
        NodePath.join(stateDir, "runner-id"),
        `${buildRemoteT3RunnerIdentity(runner)}\n`,
      );
      NodeFS.writeFileSync(
        NodePath.join(userdataDir, "server-runtime.json"),
        JSON.stringify({ pid: server.pid, port, origin: `http://127.0.0.1:${port}` }),
      );

      const launchResult = NodeChildProcess.spawnSync("sh", ["-s", "--", stateKey], {
        encoding: "utf8",
        env: { ...process.env, HOME: home },
        input: buildRemoteLaunchScript(runner),
        timeout: 10_000,
      });

      assert.equal(launchResult.status, 0, launchResult.stderr || launchResult.error?.message);
      const launchOutput = launchResult.stdout.trim();
      assert.isNotEmpty(
        launchOutput,
        JSON.stringify({ stderr: launchResult.stderr, error: launchResult.error }),
      );
      assert.deepEqual(JSON.parse(launchOutput), {
        remotePort: port,
        serverKind: "managed",
      });
      assert.equal(
        NodeFS.readFileSync(NodePath.join(stateDir, "process-start"), "utf8").trim(),
        readProcessStart(server.pid!),
      );

      const stopResult = await runShellScript(stopScript, { ...process.env, HOME: home });
      assert.equal(stopResult.status, 0, stopResult.stderr);
      assert.deepEqual(JSON.parse(stopResult.stdout.trim()), { stopped: true });
      if (server.exitCode === null && server.signalCode === null) {
        await new Promise<void>((resolve) => server.once("exit", () => resolve()));
      }
      assert.equal(server.signalCode, "SIGTERM");
    } finally {
      if (server.exitCode === null) {
        server.kill();
      }
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });

  it("keeps managed state when an explicit stop cannot terminate the exact process", async () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-ssh-stubborn-stop-"));
    const home = NodePath.join(root, "home");
    const signalMarker = NodePath.join(root, "signaled");
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;
    const stopScript = buildRemoteStopScript(target);
    const stateKey = /ssh-launch\/([^"/]+)"/u.exec(stopScript)?.[1];
    assert.isString(stateKey);
    if (!stateKey) throw new Error("stop script did not contain its SSH state key");
    const server = await spawnPersistentManagedProcess(signalMarker);

    try {
      assert.isNumber(server.pid);
      const stateDir = writeTrackedManagedState({
        home,
        stateKey,
        pid: server.pid!,
        port: 3773,
        runnerIdentity: buildRemoteT3RunnerIdentity({
          nodeScriptPath: "/unused/t3-server.mjs",
        }),
      });
      const stopResult = await runShellScript(stopScript, { ...process.env, HOME: home });

      assert.notEqual(stopResult.status, 0, stopResult.stderr);
      assert.include(stopResult.stderr, "did not exit after explicit disconnect");
      assert.isTrue(NodeFS.existsSync(signalMarker));
      assert.isNull(server.exitCode, "the stubborn managed process remains the tracked owner");
      for (const fileName of ["pid", "port", "managed", "process-start"]) {
        assert.isTrue(
          NodeFS.existsSync(NodePath.join(stateDir, fileName)),
          `${fileName} must remain for a later stop retry`,
        );
      }
    } finally {
      await forceStopTestProcess(server);
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });

  it("serializes concurrent remote launches for the same SSH state", async () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-ssh-concurrent-launch-"));
    const home = NodePath.join(root, "home");
    const launchMarker = NodePath.join(root, "launched-pids");
    const serverEntry = NodePath.join(root, "server.mjs");
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;
    const runner = {
      nodeScriptPath: serverEntry,
      nodeScriptBuildIdentity: "concurrent-launch-test",
    } as const;
    const port = await reserveLoopbackPort();
    const launchScript = withDefaultRemotePort(buildRemoteLaunchScript(runner), port);
    const stateKey = /ssh-launch\/([^"/]+)"/u.exec(buildRemoteStopScript(target))?.[1];
    assert.isString(stateKey);
    if (!stateKey) throw new Error("launch script did not contain its SSH state key");
    NodeFS.writeFileSync(
      serverEntry,
      `import fs from "node:fs"; import http from "node:http"; fs.appendFileSync(${JSON.stringify(
        launchMarker,
      )}, String(process.pid) + "\\n"); const args = process.argv.slice(2); const port = Number(args[args.indexOf("--port") + 1]); const server = http.createServer((_request, response) => { response.writeHead(200); response.end("ok"); }); server.listen(port, "127.0.0.1");`,
    );
    const env = { ...process.env, HOME: home };
    let launchedPid: number | undefined;

    try {
      const [first, second] = await Promise.all([
        runShellScript(launchScript, env, ["-s", "--", stateKey]),
        runShellScript(launchScript, env, ["-s", "--", stateKey]),
      ]);
      assert.equal(first.status, 0, first.stderr);
      assert.equal(second.status, 0, second.stderr);
      const firstResult = JSON.parse(first.stdout.trim()) as { remotePort: number };
      const secondResult = JSON.parse(second.stdout.trim()) as { remotePort: number };
      assert.equal(firstResult.remotePort, secondResult.remotePort);

      const launchedPids = NodeFS.readFileSync(launchMarker, "utf8").trim().split(/\s+/u);
      assert.lengthOf(launchedPids, 1, "only one managed server may be launched");
      launchedPid = Number(launchedPids[0]);
      assert.isTrue(Number.isInteger(launchedPid) && launchedPid > 0);
      const stateDir = NodePath.join(home, ".t3", "ssh-launch", stateKey);
      assert.equal(
        NodeFS.readFileSync(NodePath.join(stateDir, "pid"), "utf8").trim(),
        String(launchedPid),
      );
      assert.equal(
        NodeFS.readFileSync(NodePath.join(stateDir, "process-start"), "utf8").trim(),
        readProcessStart(launchedPid),
      );

      const stopResult = await runShellScript(buildRemoteStopScript(target), env);
      assert.equal(stopResult.status, 0, stopResult.stderr);
      assert.deepEqual(JSON.parse(stopResult.stdout.trim()), { stopped: true });
      assert.throws(() => process.kill(launchedPid!, 0));
      launchedPid = undefined;
    } finally {
      if (launchedPid !== undefined) {
        try {
          process.kill(launchedPid, "SIGKILL");
        } catch {
          // The exact test process already exited.
        }
      }
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });

  it("serializes explicit stop behind an in-flight remote launch", async () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-ssh-launch-stop-race-"));
    const home = NodePath.join(root, "home");
    const launchMarker = NodePath.join(root, "launched-pids");
    const releaseGate = NodePath.join(root, "release-launch");
    const serverEntry = NodePath.join(root, "server.mjs");
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;
    const runner = {
      nodeScriptPath: serverEntry,
      nodeScriptBuildIdentity: "launch-stop-race-test",
    } as const;
    const port = await reserveLoopbackPort();
    const stateKey = /ssh-launch\/([^"/]+)"/u.exec(buildRemoteStopScript(target))?.[1];
    assert.isString(stateKey);
    if (!stateKey) throw new Error("stop script did not contain its SSH state key");
    NodeFS.writeFileSync(
      serverEntry,
      `import fs from "node:fs"; import http from "node:http"; fs.appendFileSync(${JSON.stringify(
        launchMarker,
      )}, String(process.pid) + "\\n"); const args = process.argv.slice(2); const port = Number(args[args.indexOf("--port") + 1]); const server = http.createServer((_request, response) => { response.writeHead(200); response.end("ok"); }); server.listen(port, "127.0.0.1");`,
    );
    const launchScript = withDefaultRemotePort(buildRemoteLaunchScript(runner), port).replace(
      "acquire_state_lifecycle_lock\ncat >",
      `acquire_state_lifecycle_lock\nprintf 'lifecycle-lock-acquired\\n' >&2\nwhile [ ! -f ${JSON.stringify(
        releaseGate,
      )} ]; do sleep 0.1; done\ncat >`,
    );
    const env = { ...process.env, HOME: home };
    let launchedPid: number | undefined;

    try {
      const launch = spawnShellScript(launchScript, env, ["-s", "--", stateKey]);
      await waitForShellStderr(launch.child, "lifecycle-lock-acquired");
      const stopScript = buildRemoteStopScript(target).replace(
        "acquire_state_lifecycle_lock\nREMOTE_MANAGED=",
        "printf 'stop-started\\n' >&2\nacquire_state_lifecycle_lock\nREMOTE_MANAGED=",
      );
      const stop = spawnShellScript(stopScript, env);
      await waitForShellStderr(stop.child, "stop-started");
      NodeFS.writeFileSync(releaseGate, "release\n");

      const launchResult = await launch.result;
      const stopResult = await stop.result;
      assert.equal(launchResult.status, 0, launchResult.stderr);
      assert.equal(stopResult.status, 0, stopResult.stderr);
      assert.deepEqual(JSON.parse(stopResult.stdout.trim()), { stopped: true });
      assert.isTrue(NodeFS.existsSync(launchMarker), JSON.stringify({ launchResult, stopResult }));
      const launchedPids = NodeFS.readFileSync(launchMarker, "utf8").trim().split(/\s+/u);
      assert.lengthOf(launchedPids, 1);
      launchedPid = Number(launchedPids[0]);
      assert.isTrue(Number.isInteger(launchedPid) && launchedPid > 0);
      const stateDir = NodePath.join(home, ".t3", "ssh-launch", stateKey);
      assert.isFalse(NodeFS.existsSync(NodePath.join(stateDir, "pid")));
      assert.isFalse(NodeFS.existsSync(NodePath.join(stateDir, "process-start")));
      assert.throws(() => process.kill(launchedPid!, 0));
      launchedPid = undefined;
    } finally {
      if (launchedPid !== undefined) {
        try {
          process.kill(launchedPid, "SIGKILL");
        } catch {
          // The exact test process already exited.
        }
      }
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });

  it.effect("accepts launch JSON after remote shell startup noise", () => {
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;
    const spawnedCommands: Array<ReadonlyArray<string>> = [];
    const spawner = ChildProcessSpawner.make((command) =>
      Effect.sync(() => {
        spawnedCommands.push(commandArgs(command));
        return makeSuccessfulProcess('loaded nvm default\n{"remotePort":3774}\n');
      }),
    );
    const spawnerLayer = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner);
    const processLayer = Layer.merge(NodeServices.layer, spawnerLayer);

    return Effect.gen(function* () {
      const result = yield* launchOrReuseRemoteServer(target, undefined, ARCHIVE);
      assert.equal(result.remotePort, 3774);
      assert.deepEqual(spawnedCommands[0]?.slice(-5, -1), ["sh", "-l", "-s", "--"]);
    }).pipe(Effect.provide(processLayer));
  });

  it.effect("allows cold remote launches to exceed the default SSH command timeout", () => {
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;
    const spawner = ChildProcessSpawner.make(() =>
      Effect.succeed(makeDelayedSuccessfulProcess('{"remotePort":3774}\n', 75_000)),
    );
    const spawnerLayer = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner);
    const processLayer = Layer.mergeAll(NodeServices.layer, spawnerLayer, TestClock.layer());

    return Effect.gen(function* () {
      const fiber = yield* Effect.forkChild(
        launchOrReuseRemoteServer(target, undefined, NODE_SCRIPT),
      );
      yield* Effect.yieldNow;
      yield* TestClock.adjust(Duration.seconds(75));

      const result = yield* Fiber.join(fiber);
      assert.equal(result.remotePort, 3774);
    }).pipe(Effect.provide(processLayer));
  });

  it.effect("gives cold archive launches a larger budget than node-script launches", () => {
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;
    const spawner = ChildProcessSpawner.make(() =>
      Effect.succeed(makeDelayedSuccessfulProcess('{"remotePort":3774}\n', 800_000)),
    );
    const spawnerLayer = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner);
    const processLayer = Layer.mergeAll(NodeServices.layer, spawnerLayer, TestClock.layer());

    return Effect.gen(function* () {
      const fiber = yield* Effect.forkChild(launchOrReuseRemoteServer(target, undefined, ARCHIVE));
      yield* Effect.yieldNow;
      yield* TestClock.adjust(Duration.seconds(800));

      const result = yield* Fiber.join(fiber);
      assert.equal(result.remotePort, 3774);
    }).pipe(Effect.provide(processLayer));
  });

  it("allows the remote port picker to run without a state file path", () => {
    assert.include(REMOTE_PICK_PORT_SCRIPT, 'const filePath = process.argv[2] ?? "";');
  });

  it.effect("bounds each HTTP readiness probe so retries cannot hang on one request", () =>
    Effect.gen(function* () {
      const fiber = yield* Effect.forkChild(
        Effect.result(
          waitForHttpReady({
            baseUrl: "http://127.0.0.1:41773/",
            timeoutMs: 1_000,
            intervalMs: 100,
            probeTimeoutMs: 250,
          }),
        ),
      );
      yield* Effect.yieldNow;
      yield* TestClock.adjust(Duration.millis(1_000));

      const result = yield* Fiber.join(fiber);

      assert.isTrue(Result.isFailure(result));
      if (Result.isFailure(result)) {
        assert.include(result.failure.message, "Timed out waiting 1000ms");
      }
    }).pipe(
      Effect.provide(
        Layer.merge(TestClock.layer(), Layer.succeed(HttpClient.HttpClient, hangingHttpClient)),
      ),
    ),
  );

  it("preserves primitive readiness reason values in diagnostic output", () => {
    assert.deepEqual(
      describeReadinessCause({
        _tag: "HttpClientError",
        message: "Backend readiness probe failed.",
        reason: "authentication failed",
        cause: "upstream closed",
      }),
      {
        _tag: "HttpClientError",
        message: "Backend readiness probe failed.",
        reason: "authentication failed",
        cause: "upstream closed",
      },
    );
  });

  it.effect("accepts pretty-printed pairing JSON from the remote CLI", () => {
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;
    const spawner = ChildProcessSpawner.make(() =>
      Effect.succeed(
        makeSuccessfulProcess(`{
  "id": "88941235-6ed5-4184-a2ff-5339e2075958",
  "credential": "LCL4R2TPHDKQ",
  "scopes": ["orchestration:read"],
  "expiresAt": "2026-04-29T01:01:20.994Z"
}

`),
      ),
    );
    const spawnerLayer = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner);
    const processLayer = Layer.merge(NodeServices.layer, spawnerLayer);
    return Effect.gen(function* () {
      const result = yield* issueRemotePairingToken(target, undefined, ARCHIVE);
      assert.equal(result.credential, "LCL4R2TPHDKQ");
    }).pipe(Effect.provide(processLayer));
  });

  it.effect("accepts pretty-printed pairing JSON after remote shell startup noise", () => {
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;
    const spawner = ChildProcessSpawner.make(() =>
      Effect.succeed(
        makeSuccessfulProcess(`loaded nvm default
{
  "id": "88941235-6ed5-4184-a2ff-5339e2075958",
  "credential": "LCL4R2TPHDKQ",
  "scopes": ["orchestration:read"],
  "expiresAt": "2026-04-29T01:01:20.994Z"
}

`),
      ),
    );
    const spawnerLayer = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner);
    const processLayer = Layer.merge(NodeServices.layer, spawnerLayer);
    return Effect.gen(function* () {
      const result = yield* issueRemotePairingToken(target, undefined, ARCHIVE);
      assert.equal(result.credential, "LCL4R2TPHDKQ");
    }).pipe(Effect.provide(processLayer));
  });

  it.effect.each(["successful stop", "failed stop"] as const)(
    "closes the tunnel scope and starts fresh after a %s",
    (mode) => {
      const spawnedCommands: Array<ReadonlyArray<string>> = [];
      let tunnelKillCount = 0;
      let stopCommandCount = 0;
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.sync(() => {
          const args = commandArgs(command);
          spawnedCommands.push(args);
          if (args.includes("-N")) {
            return makeRunningProcess(() => {
              tunnelKillCount += 1;
            });
          }
          if (args.includes("sh") && args.includes("--")) {
            return makeSuccessfulProcess('{"remotePort":3773}\n');
          }
          if (args.includes("sh")) {
            stopCommandCount += 1;
            if (mode === "failed stop" && stopCommandCount === 1) {
              return {
                ...makeSuccessfulProcess(""),
                exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
                stderr: Stream.make(
                  new TextEncoder().encode("Remote T3 server did not stop within 2 seconds.\n"),
                ),
              };
            }
            return makeSuccessfulProcess('{"stopped":true}\n');
          }
          return makeSuccessfulProcess("\n");
        }),
      );
      const layer = Layer.mergeAll(
        NodeServices.layer,
        Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Layer.succeed(HttpClient.HttpClient, testHttpClient),
        Layer.succeed(NetService.NetService, testNetService),
        SshPasswordPrompt.disabledLayer,
        SshEnvironmentManager.layer({ resolveCliRunner: Effect.succeed(ARCHIVE) }),
      );
      const target = {
        alias: "devbox",
        hostname: "devbox.example.com",
        username: "julius",
        port: 2222,
      } as const;

      return Effect.gen(function* () {
        const manager = yield* SshEnvironmentManager;

        const first = yield* manager.ensureEnvironment(target);
        assert.equal(first.httpBaseUrl, "http://127.0.0.1:41773/");
        const firstTunnelArgs = spawnedCommands.find((args) => args.includes("-N"));
        assert.isDefined(firstTunnelArgs);
        assert.include(firstTunnelArgs, "ControlMaster=no");
        assert.include(firstTunnelArgs, "ControlPath=none");
        assert.include(firstTunnelArgs, "ControlPersist=no");

        const disconnected = yield* Effect.result(manager.disconnectEnvironment(target));
        if (mode === "failed stop") {
          assert.isTrue(Result.isFailure(disconnected));
          if (Result.isFailure(disconnected)) {
            assert.instanceOf(disconnected.failure, SshCommandError);
            assert.equal(
              disconnected.failure.message,
              "Remote T3 server did not stop within 2 seconds.",
            );
          }
        } else {
          assert.isTrue(Result.isSuccess(disconnected));
        }
        assert.equal(tunnelKillCount, 1);
        assert.equal(stopCommandCount, 1);

        if (mode === "failed stop") {
          yield* manager.disconnectEnvironment(target);
          assert.equal(tunnelKillCount, 1);
          assert.equal(stopCommandCount, 2);
        }

        yield* manager.ensureEnvironment(target);

        assert.equal(spawnedCommands.filter((args) => args.includes("-N")).length, 2);
        assert.equal(tunnelKillCount, 1);
      }).pipe(
        Effect.provide(layer),
        Effect.scoped,
        Effect.andThen(
          Effect.sync(() => {
            assert.equal(tunnelKillCount, 2);
            assert.equal(stopCommandCount, mode === "failed stop" ? 2 : 1);
          }),
        ),
      );
    },
  );

  it.effect.each(["local tunnel", "remote server"] as const)(
    "waits for %s shutdown before reconnecting the same target",
    (stalledStep) =>
      Effect.gen(function* () {
        const shutdownStarted = yield* Deferred.make<void>();
        const finishShutdown = yield* Deferred.make<void>();
        const reconnectsStarted = yield* Deferred.make<void>();
        const pauseShutdown = Deferred.succeed(shutdownStarted, undefined).pipe(
          Effect.andThen(Deferred.await(finishShutdown)),
        );
        let resolutions = 0;
        let launches = 0;
        let tunnels = 0;
        let stops = 0;
        let remoteRunning = false;
        const target = { alias: "devbox", hostname: "devbox", username: null, port: null };
        const spawner = ChildProcessSpawner.make((command) =>
          Effect.gen(function* () {
            const args = commandArgs(command);
            const isTarget = args.includes(target.alias);
            if (args.includes("-G")) {
              if (isTarget && ++resolutions === 4) {
                yield* Deferred.succeed(reconnectsStarted, undefined);
              }
              return makeSuccessfulProcess("");
            }
            if (args.includes("-N")) {
              const tunnel = makeRunningProcess(() => undefined);
              if (isTarget && ++tunnels === 1 && stalledStep === "local tunnel") {
                return {
                  ...tunnel,
                  kill: (options?: ChildProcess.KillOptions) =>
                    pauseShutdown.pipe(Effect.andThen(tunnel.kill(options))),
                };
              }
              return tunnel;
            }
            if (args.includes("--")) {
              if (isTarget) {
                launches += 1;
                remoteRunning = true;
              }
              return makeSuccessfulProcess('{"remotePort":3773}\n');
            }
            const stop = makeSuccessfulProcess('{"stopped":true}\n');
            if (!isTarget) return stop;
            const pause = ++stops === 1 && stalledStep === "remote server";
            return {
              ...stop,
              exitCode: (pause ? pauseShutdown : Effect.void).pipe(
                Effect.andThen(
                  Effect.sync(() => {
                    remoteRunning = false;
                    return ChildProcessSpawner.ExitCode(0);
                  }),
                ),
              ),
            };
          }),
        );
        const layer = Layer.mergeAll(
          NodeServices.layer,
          Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Layer.succeed(HttpClient.HttpClient, testHttpClient),
          Layer.succeed(NetService.NetService, testNetService),
          SshPasswordPrompt.disabledLayer,
          SshEnvironmentManager.layer({ resolveCliRunner: Effect.succeed(ARCHIVE) }),
        );
        yield* Effect.gen(function* () {
          const manager = yield* SshEnvironmentManager;
          yield* manager.ensureEnvironment(target);
          const disconnect = yield* Effect.forkChild(manager.disconnectEnvironment(target));
          yield* Deferred.await(shutdownStarted);
          const firstReconnect = yield* Effect.forkChild(manager.ensureEnvironment(target));
          const secondReconnect = yield* Effect.forkChild(manager.ensureEnvironment(target));
          yield* Deferred.await(reconnectsStarted);

          yield* manager.ensureEnvironment({
            alias: "other",
            hostname: "other",
            username: null,
            port: null,
          });
          yield* TestClock.adjust(Duration.zero);
          const launchesBeforeShutdown = launches;
          yield* Deferred.succeed(finishShutdown, undefined);
          yield* Fiber.join(disconnect);
          const first = yield* Fiber.join(firstReconnect);
          const second = yield* Fiber.join(secondReconnect);

          assert.equal(launchesBeforeShutdown, 1);
          assert.equal(launches, 2);
          assert.equal(tunnels, 2);
          assert.isTrue(remoteRunning);
          assert.equal(first.httpBaseUrl, second.httpBaseUrl);
        }).pipe(
          Effect.ensuring(Deferred.succeed(finishShutdown, undefined)),
          Effect.provide(layer),
          Effect.scoped,
        );
      }),
  );

  it.effect("serializes explicit disconnect behind a pending connection", () => {
    const launchStarted = Deferred.makeUnsafe<void>();
    const releaseLaunch = Deferred.makeUnsafe<void>();
    let tunnelKillCount = 0;
    let stopCommandCount = 0;
    const spawner = ChildProcessSpawner.make((command) =>
      Effect.sync(() => {
        const args = commandArgs(command);
        if (args.includes("-N")) {
          return makeRunningProcess(() => {
            tunnelKillCount += 1;
          });
        }
        if (args.includes("sh") && args.includes("--")) {
          const process = makeSuccessfulProcess('{"remotePort":3773,"serverKind":"managed"}\n');
          return {
            ...process,
            exitCode: Deferred.succeed(launchStarted, undefined).pipe(
              Effect.andThen(Deferred.await(releaseLaunch)),
              Effect.as(ChildProcessSpawner.ExitCode(0)),
            ),
          };
        }
        if (args.includes("sh")) {
          stopCommandCount += 1;
          return makeSuccessfulProcess('{"stopped":true}\n');
        }
        return makeSuccessfulProcess("\n");
      }),
    );
    const layer = Layer.mergeAll(
      NodeServices.layer,
      Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Layer.succeed(HttpClient.HttpClient, testHttpClient),
      Layer.succeed(NetService.NetService, testNetService),
      SshPasswordPrompt.disabledLayer,
      SshEnvironmentManager.layer({ resolveCliRunner: Effect.succeed(ARCHIVE) }),
    );
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;

    return Effect.gen(function* () {
      const manager = yield* SshEnvironmentManager;
      const ensureFiber = yield* Effect.forkChild(manager.ensureEnvironment(target));
      yield* Deferred.await(launchStarted);
      const disconnectFiber = yield* Effect.forkChild(manager.disconnectEnvironment(target));

      yield* Deferred.succeed(releaseLaunch, undefined);
      yield* Fiber.join(ensureFiber);
      yield* Fiber.join(disconnectFiber);

      assert.equal(tunnelKillCount, 1);
      assert.equal(stopCommandCount, 1);
    }).pipe(Effect.provide(layer), Effect.scoped);
  });

  it.effect("leaves the managed server running when the manager scope closes", () => {
    let tunnelKillCount = 0;
    let stopCommandCount = 0;
    const spawner = ChildProcessSpawner.make((command) =>
      Effect.sync(() => {
        const args = commandArgs(command);
        if (args.includes("-N")) {
          return makeRunningProcess(() => {
            tunnelKillCount += 1;
          });
        }
        if (args.includes("sh") && args.includes("--")) {
          return makeSuccessfulProcess('{"remotePort":3773,"serverKind":"managed"}\n');
        }
        if (args.includes("sh")) {
          stopCommandCount += 1;
          return makeSuccessfulProcess('{"stopped":true}\n');
        }
        return makeSuccessfulProcess("\n");
      }),
    );
    const layer = Layer.mergeAll(
      NodeServices.layer,
      Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Layer.succeed(HttpClient.HttpClient, testHttpClient),
      Layer.succeed(NetService.NetService, testNetService),
      SshPasswordPrompt.disabledLayer,
      SshEnvironmentManager.layer({ resolveCliRunner: Effect.succeed(ARCHIVE) }),
    );
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;

    return Effect.gen(function* () {
      yield* Effect.scoped(
        Effect.gen(function* () {
          const manager = yield* SshEnvironmentManager;
          yield* manager.ensureEnvironment(target);
        }).pipe(Effect.provide(layer)),
      );

      assert.equal(tunnelKillCount, 1);
      assert.equal(stopCommandCount, 0);
    });
  });
});

// The archive runner is generated shell; string assertions cannot prove the
// lock excludes concurrent installers. Run the real script against a tiny
// fake archive served from a file:// mirror.
describe("archive runner script", () => {
  const hostPlatform = HostProcessPlatform.defaultValue();
  const hostArch = HostProcessArchitecture.defaultValue();
  const windowsHost = hostPlatform === "win32";
  const archiveVersion = "1.2.3-preview.20260911.4";

  const runRunner = (home: string, runner: string) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const child = yield* spawner.spawn(
        ChildProcess.make("sh", [runner, "--version"], {
          env: { PATH: process.env.PATH ?? "", HOME: home },
          extendEnv: false,
        }),
      );
      const [stdout, stderr, exitCode] = yield* Effect.all(
        [
          child.stdout.pipe(
            Stream.decodeText(),
            Stream.runFold(
              () => "",
              (acc, chunk) => acc + chunk,
            ),
          ),
          child.stderr.pipe(
            Stream.decodeText(),
            Stream.runFold(
              () => "",
              (acc, chunk) => acc + chunk,
            ),
          ),
          child.exitCode.pipe(Effect.map(Number)),
        ],
        { concurrency: "unbounded" },
      );
      return { stdout, stderr, exitCode };
    });

  // A fake "executable" that answers --version, packed the way the release
  // workflow packs the real archive: one top-level directory named after the
  // stem, checksummed in SHA256SUMS.
  const makeMirror = Effect.fn("makeMirror")(function* (root: string) {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const platform = hostPlatform === "darwin" ? "darwin" : "linux";
    const arch = hostArch === "arm64" ? "arm64" : "x64";
    const stem = `t3-${archiveVersion}-${platform}-${arch}`;
    const stage = `${root}/stage/${stem}`;
    const release = `${root}/mirror/v${archiveVersion}`;
    const script = [
      "set -eu",
      `mkdir -p '${stage}' '${release}'`,
      `printf '#!/bin/sh\\necho t3 v${archiveVersion}\\n' > '${stage}/t3'`,
      `chmod +x '${stage}/t3'`,
      `tar -czf '${release}/${stem}.tar.gz' -C '${root}/stage' '${stem}'`,
      `cd '${release}' && (sha256sum '${stem}.tar.gz' 2>/dev/null || shasum -a 256 '${stem}.tar.gz') > SHA256SUMS`,
    ].join("\n");
    const child = yield* spawner.spawn(ChildProcess.make("sh", ["-c", script]));
    assert.equal(Number(yield* child.exitCode), 0);
    return `file://${root}/mirror`;
  });

  it.effect.skipIf(windowsHost)(
    "installs once when several launches race, and reclaims stale locks",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-archive-runner-" });
        const releaseBaseUrl = yield* makeMirror(root);
        const runner = `${root}/run-t3.sh`;
        yield* fs.writeFileString(
          runner,
          buildRemoteT3RunnerScript({ archiveVersion, releaseBaseUrl }),
        );
        const home = `${root}/home`;
        yield* fs.makeDirectory(home, { recursive: true });

        const results = yield* Effect.all(
          [runRunner(home, runner), runRunner(home, runner), runRunner(home, runner)],
          { concurrency: "unbounded" },
        );
        for (const result of results) {
          assert.equal(result.exitCode, 0, result.stderr);
          assert.include(result.stdout, `t3 v${archiveVersion}`);
        }
        const versionsDir = `${home}/.t3/runtime/versions`;
        assert.deepEqual(yield* fs.readDirectory(versionsDir), [archiveVersion]);
        assert.equal(
          (yield* fs.readFileString(`${versionsDir}/${archiveVersion}/.install-complete`)).trim(),
          archiveVersion,
        );

        // A lock left by a crashed installer (dead pid) must not block the
        // next launch, and neither must one that never published a pid.
        const lock = `${versionsDir}/.${archiveVersion}.install.lock`;
        yield* fs.remove(`${versionsDir}/${archiveVersion}`, { recursive: true });
        yield* fs.makeDirectory(lock);
        yield* fs.writeFileString(`${lock}/pid`, "999999\n");
        const afterDead = yield* runRunner(home, runner);
        assert.equal(afterDead.exitCode, 0, afterDead.stderr);

        yield* fs.remove(`${versionsDir}/${archiveVersion}`, { recursive: true });
        yield* fs.makeDirectory(lock);
        const afterUnowned = yield* runRunner(home, runner);
        assert.equal(afterUnowned.exitCode, 0, afterUnowned.stderr);
        assert.isFalse(yield* fs.exists(lock));
      }).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );
});
