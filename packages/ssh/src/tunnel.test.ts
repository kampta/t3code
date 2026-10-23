// @effect-diagnostics nodeBuiltinImport:off - the generated remote shell regression test drives a real child HTTP server and POSIX shell.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NetService from "@t3tools/shared/Net";
import * as Schema from "effect/Schema";
import * as Path from "effect/Path";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as PlatformError from "effect/PlatformError";
import * as Result from "effect/Result";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";
import { HttpClient, HttpClientResponse } from "effect/http";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import * as SshAuth from "./auth.ts";
import { SshCommandError } from "./errors.ts";
import * as SshTunnel from "./tunnel.ts";

// Remote scripts take a precomputed state key; any stable key works inside a temp HOME.
const OWNERSHIP_STATE_KEY = "ownership-regression";

const Started = Schema.Struct({
  pid: Schema.Number,
  port: Schema.Number,
  args: Schema.Array(Schema.String),
});
const decodeStarted = Schema.decodeUnknownSync(Schema.fromJsonString(Started));

function readProcessStart(pid: number): string {
  const procStatPath = `/proc/${pid}/stat`;
  if (NodeFS.existsSync(procStatPath)) {
    const stat = NodeFS.readFileSync(procStatPath, "utf8");
    const fields = stat
      .slice(stat.lastIndexOf(") ") + 2)
      .trim()
      .split(/\s+/u);
    const bootId = NodeFS.readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim();
    return `pid:${pid}:proc:${bootId}:${fields[19] ?? ""}`;
  }
  const result = NodeChildProcess.spawnSync("ps", ["-o", "lstart=", "-p", `${pid}`], {
    encoding: "utf8",
    env: { ...process.env, LC_ALL: "C" },
  });
  return `pid:${pid}:ps:${result.stdout.trim().replace(/\s+/gu, " ")}`;
}

function buildFastRemoteLaunchScript(
  input?: Parameters<typeof SshTunnel.buildRemoteLaunchScript>[0],
) {
  return SshTunnel.buildRemoteLaunchScript(input)
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

async function spawnPersistentManagedProcess(signalMarker: string, exitOnTerm = false) {
  const child = NodeChildProcess.spawn(
    process.execPath,
    [
      "-e",
      'const fs = require("node:fs"); const marker = process.argv[1]; process.on("SIGTERM", () => { fs.writeFileSync(marker, "signaled\\n"); if (process.argv[2] === "true") process.exit(0); }); process.stdout.write("ready\\n"); setInterval(() => {}, 1000);',
      signalMarker,
      String(exitOnTerm),
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

describe("ssh tunnel scripts", () => {
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
        `${SshTunnel.buildRemoteT3RunnerIdentity(runner)}\n`,
      );
      NodeFS.writeFileSync(
        NodePath.join(userdataDir, "server-runtime.json"),
        JSON.stringify({ pid: server.pid, port, origin: `http://127.0.0.1:${port}` }),
      );

      const result = NodeChildProcess.spawnSync("sh", ["-s", "--", stateKey], {
        encoding: "utf8",
        env: { ...process.env, HOME: home },
        input: SshTunnel.buildRemoteLaunchScript(runner),
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
        `${SshTunnel.buildRemoteT3RunnerScript(runner)}\n`,
      );

      NodeFS.writeFileSync(
        NodePath.join(stateDir, "process-start"),
        `pid:${server.pid}:ps:stale-process-start\n`,
      );
      const staleIdentityResult = NodeChildProcess.spawnSync("sh", ["-s", "--", stateKey], {
        encoding: "utf8",
        env: { ...process.env, HOME: home },
        input: SshTunnel.buildRemoteLaunchScript(runner),
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
        runnerIdentity: SshTunnel.buildRemoteT3RunnerIdentity(runner),
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
        SshTunnel.buildRemoteT3RunnerIdentity(runner),
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
        SshTunnel.buildRemoteT3RunnerIdentity(runner),
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
      const previousRunnerIdentity = SshTunnel.buildRemoteT3RunnerIdentity(previousRunner);
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
    const stopScript = SshTunnel.buildRemoteStopScript(OWNERSHIP_STATE_KEY);
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
        `${SshTunnel.buildRemoteT3RunnerIdentity(runner)}\n`,
      );
      NodeFS.writeFileSync(
        NodePath.join(userdataDir, "server-runtime.json"),
        JSON.stringify({ pid: server.pid, port, origin: `http://127.0.0.1:${port}` }),
      );

      const launchResult = NodeChildProcess.spawnSync("sh", ["-s", "--", stateKey], {
        encoding: "utf8",
        env: { ...process.env, HOME: home },
        input: SshTunnel.buildRemoteLaunchScript(runner),
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

  it.each(["empty", "dead-pending"] as const)(
    "recovers an abandoned %s SSH lifecycle lock",
    async (unpublished) => {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-ssh-unpublished-"));
      const home = NodePath.join(root, "home");
      const script = SshTunnel.buildRemoteStopScript(OWNERSHIP_STATE_KEY);
      const key = /ssh-launch\/([^"/]+)"/u.exec(script)?.[1];
      if (!key) throw new Error("missing state key");
      const stateDir = NodePath.join(home, ".t3", "ssh-launch", key);
      const lock = NodePath.join(stateDir, "lifecycle.lock");
      NodeFS.mkdirSync(lock, { recursive: true });
      try {
        if (unpublished === "dead-pending") {
          const exited = NodeChildProcess.spawn(process.execPath, ["-e", "process.exit(0)"], {
            stdio: "ignore",
          });
          await new Promise<void>((resolve) => exited.once("exit", () => resolve()));
          NodeFS.writeFileSync(NodePath.join(lock, `pending.${exited.pid}`), "abandoned\n");
        }
        NodeFS.utimesSync(lock, 0, 0);
        const result = await runShellScript(script, { ...process.env, HOME: home });
        assert.equal(result.status, 0, result.stderr);
        assert.deepEqual(JSON.parse(result.stdout.trim()), { stopped: true });
        assert.isFalse(NodeFS.existsSync(lock));
        assert.isFalse(
          NodeFS.readdirSync(stateDir).some((name) => name.startsWith("lifecycle.pending.")),
        );
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    },
  );

  it("does not signal a managed PID whose start ticks match a different Linux boot", async () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-ssh-reboot-"));
    const home = NodePath.join(root, "home");
    const marker = NodePath.join(root, "signaled");
    const script = SshTunnel.buildRemoteStopScript(OWNERSHIP_STATE_KEY).replace(
      /managed_process_start\(\) \{[\s\S]*?\n\}/u,
      'managed_process_start() { printf "pid:%s:proc:current-boot:1234\\n" "$1"; }',
    );
    const key = /ssh-launch\/([^"/]+)"/u.exec(script)?.[1];
    if (!key) throw new Error("missing state key");
    const server = await spawnPersistentManagedProcess(marker);
    try {
      const stateDir = writeTrackedManagedState({
        home,
        stateKey: key,
        pid: server.pid!,
        port: 3773,
        runnerIdentity: SshTunnel.buildRemoteT3RunnerIdentity({
          nodeScriptPath: "/unused/t3-server.mjs",
        }),
      });
      NodeFS.writeFileSync(
        NodePath.join(stateDir, "process-start"),
        `pid:${server.pid}:proc:previous-boot:1234\n`,
      );
      const result = await runShellScript(script, { ...process.env, HOME: home });
      assert.equal(result.status, 0, result.stderr);
      assert.isFalse(NodeFS.existsSync(marker));
      assert.isNull(server.exitCode);
    } finally {
      await forceStopTestProcess(server);
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });

  it.each([
    ["launch", undefined],
    ["launch", 1],
    ["stop", undefined],
    ["stop", 1],
  ] as const)(
    "migrates legacy Linux SSH state during %s without Node (lease %s)",
    async (action, leaseVersion) => {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-ssh-legacy-"));
      const home = NodePath.join(root, "home");
      const marker = NodePath.join(root, "signaled");
      const bin = NodePath.join(root, "bin");
      NodeFS.mkdirSync(bin);
      for (const command of [
        "sh",
        "cat",
        "grep",
        "rm",
        "mv",
        "chmod",
        "mkdir",
        "rmdir",
        "ps",
        "sed",
        "awk",
        "date",
        "stat",
        "sleep",
      ]) {
        const source = NodeFS.existsSync("/usr/bin/" + command)
          ? "/usr/bin/" + command
          : "/bin/" + command;
        NodeFS.symlinkSync(source, NodePath.join(bin, command));
      }
      const stopScript = SshTunnel.buildRemoteStopScript(OWNERSHIP_STATE_KEY);
      const key = /ssh-launch\/([^"/]+)"/u.exec(stopScript)?.[1];
      if (!key) throw new Error("missing state key");
      const server = await spawnPersistentManagedProcess(marker, true);
      try {
        const stateDir = writeTrackedManagedState({
          home,
          stateKey: key,
          pid: server.pid!,
          port: 3773,
          runnerIdentity: SshTunnel.buildRemoteT3RunnerIdentity(ARCHIVE),
        });
        NodeFS.writeFileSync(
          NodePath.join(stateDir, "process-start"),
          `pid:${server.pid}:proc:1234\n`,
        );
        NodeFS.writeFileSync(
          NodePath.join(home, ".t3", "userdata", "server-owner.lock"),
          JSON.stringify({
            version: 1,
            ...(leaseVersion === undefined ? {} : { leaseVersion }),
            token: "a025f918-6bfc-4d88-bbae-9313412d20a9",
            pid: server.pid,
            processStartIdentity: "linux:current-boot:1234",
            startedAt: "2026-09-30T00:00:00.000Z",
          }) + "\n",
        );
        const runtimeDir = NodePath.join(
          home,
          ".t3",
          "runtime",
          "versions",
          ARCHIVE.archiveVersion,
        );
        NodeFS.mkdirSync(runtimeDir, { recursive: true });
        NodeFS.writeFileSync(
          NodePath.join(runtimeDir, ".install-complete"),
          ARCHIVE.archiveVersion,
        );
        NodeFS.writeFileSync(
          NodePath.join(runtimeDir, "t3"),
          `#!/bin/sh
case "$1:\${2:-}" in
  --version:) printf '%s\\n' '${ARCHIVE.archiveVersion}' ;;
  __ssh-helper:runtime-port) printf '%s %s' '${server.pid}' '3773' ;;
  __ssh-helper:wait-ready) exit 0 ;;
  *) printf 'Unexpected archive command\\n' >&2; exit 1 ;;
esac
`,
          { mode: 0o700 },
        );
        const script = (
          action === "launch" ? SshTunnel.buildRemoteLaunchScript(ARCHIVE) : stopScript
        ).replaceAll(
          /managed_process_start\(\) \{[\s\S]*?\n\}/gu,
          'managed_process_start() { printf "pid:%s:proc:current-boot:1234\\n" "$1"; }',
        );
        const result = await runShellScript(
          script,
          { ...process.env, HOME: home, PATH: bin },
          action === "launch" ? ["-s", "--", key] : ["-s"],
        );
        assert.equal(result.status, 0, result.stderr);
        if (action === "launch") {
          assert.deepEqual(JSON.parse(result.stdout.trim()), {
            remotePort: 3773,
            serverKind: "managed",
          });
          assert.equal(
            NodeFS.readFileSync(NodePath.join(stateDir, "process-start"), "utf8").trim(),
            `pid:${server.pid}:proc:current-boot:1234`,
          );
          assert.isFalse(NodeFS.existsSync(marker));
          assert.isNull(server.exitCode);
        } else {
          assert.deepEqual(JSON.parse(result.stdout.trim()), { stopped: true });
          assert.equal(NodeFS.readFileSync(marker, "utf8"), "signaled\n");
          assert.isFalse(NodeFS.existsSync(NodePath.join(stateDir, "pid")));
          assert.isFalse(NodeFS.existsSync(NodePath.join(stateDir, "process-start")));
        }
      } finally {
        await forceStopTestProcess(server);
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    },
  );

  it.each(["absent", "previous-boot", "wrong-pid", "malformed"] as const)(
    "preserves unverified legacy SSH state with %s ownership proof",
    async (proof) => {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-ssh-legacy-unowned-"));
      const home = NodePath.join(root, "home");
      const marker = NodePath.join(root, "signaled");
      const script = SshTunnel.buildRemoteStopScript(OWNERSHIP_STATE_KEY).replace(
        /managed_process_start\(\) \{[\s\S]*?\n\}/u,
        'managed_process_start() { printf "pid:%s:proc:current-boot:1234\\n" "$1"; }',
      );
      const key = /ssh-launch\/([^"/]+)"/u.exec(script)?.[1];
      if (!key) throw new Error("missing state key");
      const server = await spawnPersistentManagedProcess(marker);
      try {
        const stateDir = writeTrackedManagedState({
          home,
          stateKey: key,
          pid: server.pid!,
          port: 3773,
          runnerIdentity: SshTunnel.buildRemoteT3RunnerIdentity(ARCHIVE),
        });
        const legacyIdentity = `pid:${server.pid}:proc:1234\n`;
        NodeFS.writeFileSync(NodePath.join(stateDir, "process-start"), legacyIdentity);
        if (proof !== "absent") {
          NodeFS.writeFileSync(
            NodePath.join(home, ".t3", "userdata", "server-owner.lock"),
            proof === "malformed"
              ? "incomplete"
              : JSON.stringify({
                  version: 1,
                  token: "a025f918-6bfc-4d88-bbae-9313412d20a9",
                  pid: proof === "wrong-pid" ? server.pid! + 1 : server.pid,
                  processStartIdentity: `linux:${proof === "previous-boot" ? "previous-boot" : "current-boot"}:1234`,
                  startedAt: "2026-09-30T00:00:00.000Z",
                }),
          );
        }
        const result = await runShellScript(script, { ...process.env, HOME: home });
        assert.equal(result.status, 1);
        assert.include(result.stderr, "cannot be verified against its server ownership record");
        assert.equal(
          NodeFS.readFileSync(NodePath.join(stateDir, "process-start"), "utf8"),
          legacyIdentity,
        );
        assert.equal(
          NodeFS.readFileSync(NodePath.join(stateDir, "pid"), "utf8").trim(),
          String(server.pid),
        );
        assert.isFalse(NodeFS.existsSync(marker));
        assert.isNull(server.exitCode);
      } finally {
        await forceStopTestProcess(server);
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    },
  );

  it("reclaims a pending lifecycle record when its PID belongs to a different process identity", async () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-ssh-pending-reused-"));
    const home = NodePath.join(root, "home");
    const marker = NodePath.join(root, "signaled");
    const script = SshTunnel.buildRemoteStopScript(OWNERSHIP_STATE_KEY);
    const key = /ssh-launch\/([^"/]+)"/u.exec(script)?.[1];
    if (!key) throw new Error("missing state key");
    const server = await spawnPersistentManagedProcess(marker);
    try {
      const stateDir = NodePath.join(home, ".t3", "ssh-launch", key);
      const lock = NodePath.join(stateDir, "lifecycle.lock");
      NodeFS.mkdirSync(lock, { recursive: true });
      const pending = NodePath.join(stateDir, `lifecycle.pending.${server.pid}`);
      NodeFS.writeFileSync(pending, `pid:${server.pid}:proc:previous-boot:0\n`);
      NodeFS.utimesSync(lock, 0, 0);
      const result = await runShellScript(script, { ...process.env, HOME: home });
      assert.equal(result.status, 0, result.stderr);
      assert.isFalse(NodeFS.existsSync(pending));
      assert.isFalse(NodeFS.existsSync(lock));
      assert.isFalse(NodeFS.existsSync(marker));
      assert.isNull(server.exitCode);
    } finally {
      await forceStopTestProcess(server);
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });

  it("keeps a live unpublished lifecycle owner protected while publishing is paused", async () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-ssh-publishing-"));
    const home = NodePath.join(root, "home");
    const gate = NodePath.join(root, "publish");
    const script = SshTunnel.buildRemoteStopScript(OWNERSHIP_STATE_KEY);
    const key = /ssh-launch\/([^"/]+)"/u.exec(script)?.[1];
    if (!key) throw new Error("missing state key");
    const ownerScript = script.replace(
      "STATE_LIFECYCLE_LOCK_HELD=1\n",
      'STATE_LIFECYCLE_LOCK_HELD=1\nprintf "publisher-paused\\n" >&2\nwhile [ ! -f "$T3_TEST_PUBLISH_GATE" ]; do sleep 0.1; done\n',
    );
    const contenderScript = script.replaceAll(
      "UNPUBLISHED_OWNER_ACTIVE=1",
      'UNPUBLISHED_OWNER_ACTIVE=1\nprintf "publisher-protected\\n" >&2',
    );
    const env = { ...process.env, HOME: home, T3_TEST_PUBLISH_GATE: gate };
    const owner = spawnShellScript(ownerScript, env);
    let contender: ReturnType<typeof spawnShellScript> | undefined;
    try {
      await waitForShellStderr(owner.child, "publisher-paused");
      const lock = NodePath.join(home, ".t3", "ssh-launch", key, "lifecycle.lock");
      NodeFS.utimesSync(lock, 0, 0);
      contender = spawnShellScript(contenderScript, env);
      await waitForShellStderr(contender.child, "publisher-protected");
      NodeFS.writeFileSync(gate, "publish\n");
      const results = await Promise.all([owner.result, contender.result]);
      for (const result of results) assert.equal(result.status, 0, result.stderr);
      assert.isFalse(NodeFS.existsSync(lock));
    } finally {
      NodeFS.writeFileSync(gate, "publish\n");
      await Promise.all([owner.result, contender?.result]);
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });

  it("preserves a live legacy managed server until its process identity is migrated", async () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-ssh-legacy-stop-"));
    const home = NodePath.join(root, "home");
    const signalMarker = NodePath.join(root, "signaled");
    const stopScript = SshTunnel.buildRemoteStopScript(OWNERSHIP_STATE_KEY);
    const stateKey = /ssh-launch\/([^"/]+)"/u.exec(stopScript)?.[1];
    if (!stateKey) throw new Error("stop script did not contain its SSH state key");
    const server = await spawnPersistentManagedProcess(signalMarker);
    try {
      const stateDir = writeTrackedManagedState({
        home,
        stateKey,
        pid: server.pid!,
        port: 3773,
        runnerIdentity: SshTunnel.buildRemoteT3RunnerIdentity({
          nodeScriptPath: "/unused/t3-server.mjs",
        }),
      });
      NodeFS.rmSync(NodePath.join(stateDir, "process-start"));
      const result = await runShellScript(stopScript, { ...process.env, HOME: home });
      assert.notEqual(result.status, 0, result.stderr);
      assert.include(result.stderr, "no recorded process identity");
      assert.isFalse(NodeFS.existsSync(signalMarker));
      assert.isNull(server.exitCode);
      for (const name of ["pid", "port", "managed"]) {
        assert.isTrue(NodeFS.existsSync(NodePath.join(stateDir, name)));
      }
    } finally {
      await forceStopTestProcess(server);
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });

  it("keeps managed state when an explicit stop cannot terminate the exact process", async () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-ssh-stubborn-stop-"));
    const home = NodePath.join(root, "home");
    const signalMarker = NodePath.join(root, "signaled");
    const stopScript = SshTunnel.buildRemoteStopScript(OWNERSHIP_STATE_KEY);
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
        runnerIdentity: SshTunnel.buildRemoteT3RunnerIdentity({
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
    const runner = {
      nodeScriptPath: serverEntry,
      nodeScriptBuildIdentity: "concurrent-launch-test",
    } as const;
    const port = await reserveLoopbackPort();
    const launchScript = withDefaultRemotePort(SshTunnel.buildRemoteLaunchScript(runner), port);
    const stateKey = /ssh-launch\/([^"/]+)"/u.exec(
      SshTunnel.buildRemoteStopScript(OWNERSHIP_STATE_KEY),
    )?.[1];
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

      const stopResult = await runShellScript(
        SshTunnel.buildRemoteStopScript(OWNERSHIP_STATE_KEY),
        env,
      );
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
    const runner = {
      nodeScriptPath: serverEntry,
      nodeScriptBuildIdentity: "launch-stop-race-test",
    } as const;
    const port = await reserveLoopbackPort();
    const stateKey = /ssh-launch\/([^"/]+)"/u.exec(
      SshTunnel.buildRemoteStopScript(OWNERSHIP_STATE_KEY),
    )?.[1];
    assert.isString(stateKey);
    if (!stateKey) throw new Error("stop script did not contain its SSH state key");
    NodeFS.writeFileSync(
      serverEntry,
      `import fs from "node:fs"; import http from "node:http"; fs.appendFileSync(${JSON.stringify(
        launchMarker,
      )}, String(process.pid) + "\\n"); const args = process.argv.slice(2); const port = Number(args[args.indexOf("--port") + 1]); const server = http.createServer((_request, response) => { response.writeHead(200); response.end("ok"); }); server.listen(port, "127.0.0.1");`,
    );
    const launchScript = withDefaultRemotePort(
      SshTunnel.buildRemoteLaunchScript(runner),
      port,
    ).replace(
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
      const stopScript = SshTunnel.buildRemoteStopScript(OWNERSHIP_STATE_KEY).replace(
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
        if (args.some((arg, index) => arg === "-s" && args[index + 1] === "--")) {
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
      SshAuth.SshPasswordPrompt.disabledLayer,
      SshTunnel.SshEnvironmentManager.layer({ resolveCliRunner: Effect.succeed(ARCHIVE) }),
    );
    const target = {
      alias: "devbox",
      hostname: "devbox.example.com",
      username: "julius",
      port: 2222,
    } as const;

    return Effect.gen(function* () {
      const manager = yield* SshTunnel.SshEnvironmentManager;
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
        if (args.some((arg, index) => arg === "-s" && args[index + 1] === "--")) {
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
      SshAuth.SshPasswordPrompt.disabledLayer,
      SshTunnel.SshEnvironmentManager.layer({ resolveCliRunner: Effect.succeed(ARCHIVE) }),
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
          const manager = yield* SshTunnel.SshEnvironmentManager;
          yield* manager.ensureEnvironment(target);
        }).pipe(Effect.provide(layer)),
      );

      assert.equal(tunnelKillCount, 1);
      assert.equal(stopCommandCount, 0);
    });
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
        () => SshTunnel.buildRemoteT3RunnerScript({ archiveVersion }),
        SshTunnel.SshInvalidArchiveVersionError,
        undefined,
        archiveVersion,
      );
    }
    assert.include(
      SshTunnel.buildRemoteT3RunnerScript(ARCHIVE),
      "T3_ARCHIVE_VERSION='1.2.3-preview.20260911.4'",
    );
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
    const layerSpawner = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner);
    const layerProcess = Layer.merge(NodeServices.layer, layerSpawner);

    return Effect.gen(function* () {
      const result = yield* SshTunnel.launchOrReuseRemoteServer(target, undefined, ARCHIVE);
      assert.equal(result.remotePort, 3774);
      assert.deepEqual(spawnedCommands[0]?.slice(-5, -1), ["sh", "-l", "-s", "--"]);
    }).pipe(Effect.provide(layerProcess));
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
    const layerSpawner = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner);
    const layerProcess = Layer.merge(NodeServices.layer, layerSpawner);
    return Effect.gen(function* () {
      const result = yield* SshTunnel.issueRemotePairingToken(target, undefined, ARCHIVE);
      assert.equal(result.credential, "LCL4R2TPHDKQ");
    }).pipe(Effect.provide(layerProcess));
  });

  it.effect("retains a bounded diagnostic tail when the tunnel exits before readiness", () =>
    Effect.gen(function* () {
      const diagnostic = "Connection refused: café";
      const output = new TextEncoder().encode(
        "old diagnostics\n" + "x".repeat(32_768) + "\n" + diagnostic,
      );
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.succeed(
          commandArgs(command).includes("-N")
            ? {
                ...makeSuccessfulProcess(""),
                exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(1)),
                stderr: Stream.make(output.subarray(0, output.length - 1), output.subarray(-1)),
              }
            : makeSuccessfulProcess('{"remotePort":3773}\n'),
        ),
      );
      const layer = Layer.mergeAll(
        NodeServices.layer,
        Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Layer.succeed(HttpClient.HttpClient, hangingHttpClient),
        Layer.succeed(NetService.NetService, testNetService),
        SshAuth.SshPasswordPrompt.disabledLayer,
        SshTunnel.SshEnvironmentManager.layer({ resolveCliRunner: Effect.succeed(ARCHIVE) }),
      );
      yield* Effect.gen(function* () {
        const manager = yield* SshTunnel.SshEnvironmentManager;
        const result = yield* Effect.result(
          manager.ensureEnvironment({
            alias: "devbox",
            hostname: "devbox",
            username: null,
            port: null,
          }),
        );
        assert.isTrue(Result.isFailure(result));
        if (Result.isFailure(result)) {
          assert.instanceOf(result.failure, SshCommandError);
          if (result.failure instanceof SshCommandError) {
            assert.equal(result.failure.exitCode, 1);
            assert.equal(result.failure.stderr.length, 16_384);
            assert.isTrue(result.failure.stderr.endsWith(diagnostic));
            assert.equal(result.failure.message, result.failure.stderr.trim());
          }
        }
      }).pipe(Effect.provide(layer), Effect.scoped);
    }),
  );

  it.effect("reports the real exit when reading tunnel diagnostics fails", () =>
    Effect.gen(function* () {
      const diagnostic = "Permission denied (publickey)";
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.succeed(
          commandArgs(command).includes("-N")
            ? {
                ...makeSuccessfulProcess(""),
                exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(255)),
                stderr: Stream.make(new TextEncoder().encode(diagnostic)).pipe(
                  Stream.concat(
                    Stream.fail(
                      PlatformError.systemError({
                        _tag: "Unknown",
                        module: "Stream",
                        method: "read",
                        description: "stderr read failed",
                      }),
                    ),
                  ),
                ),
              }
            : makeSuccessfulProcess('{"remotePort":3773}\n'),
        ),
      );
      const layer = Layer.mergeAll(
        NodeServices.layer,
        Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Layer.succeed(HttpClient.HttpClient, hangingHttpClient),
        Layer.succeed(NetService.NetService, testNetService),
        SshAuth.SshPasswordPrompt.disabledLayer,
        SshTunnel.SshEnvironmentManager.layer({ resolveCliRunner: Effect.succeed(ARCHIVE) }),
      );
      yield* Effect.gen(function* () {
        const manager = yield* SshTunnel.SshEnvironmentManager;
        const result = yield* Effect.result(
          manager.ensureEnvironment({
            alias: "devbox",
            hostname: "devbox",
            username: null,
            port: null,
          }),
        );
        assert.isTrue(Result.isFailure(result));
        if (Result.isFailure(result)) {
          assert.instanceOf(result.failure, SshCommandError);
          if (result.failure instanceof SshCommandError) {
            assert.equal(result.failure.exitCode, 255);
            assert.equal(result.failure.stderr, diagnostic);
          }
        }
      }).pipe(Effect.provide(layer), Effect.scoped);
    }),
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
            if (args.some((arg, index) => arg === "-s" && args[index + 1] === "--")) {
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
          SshAuth.SshPasswordPrompt.disabledLayer,
          SshTunnel.SshEnvironmentManager.layer({ resolveCliRunner: Effect.succeed(ARCHIVE) }),
        );
        yield* Effect.gen(function* () {
          const manager = yield* SshTunnel.SshEnvironmentManager;
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
});

// On macOS, Node can bind 127.0.0.1:<port> while another process (such as the
// desktop app with network access on) listens on *:<port>. Linux refuses that
// bind unless both sockets set SO_REUSEPORT, so on Linux the preload adds it to
// the picker's probe and the holder sets it too.

describe.skipIf(HostProcess.Platform.defaultValue() === "win32")(
  "remote runner process ownership",
  () => {
    it.live("keeps the server PID and graceful shutdown through the node-script runner", () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const fixture = yield* fs.makeTempDirectoryScoped({ prefix: "t3-runner-" });
        const bin = path.join(fixture, "bin");
        const cliPath = path.join(fixture, "installed cli.mjs");
        yield* fs.makeDirectory(bin);
        yield* fs.symlink(process.execPath, path.join(bin, "node"));
        yield* fs.writeFileString(
          cliPath,
          `#!/usr/bin/env node
import * as net from "node:net";
const server = net.createServer((socket) => {
  socket.end();
  server.close();
});
process.on("SIGTERM", () => server.close(() => {
  process.stdout.write("graceful shutdown\\n");
}));
server.listen(Number(process.env.T3_TEST_PORT ?? 0), "127.0.0.1", () => {
  process.stdout.write(JSON.stringify({
    pid: process.pid,
    port: server.address().port,
    args: process.argv.slice(2),
  }) + "\\n");
});
`,
        );
        yield* fs.chmod(cliPath, 0o700);
        const runnerPath = path.join(fixture, "run-t3.sh");
        yield* fs.writeFileString(
          runnerPath,
          SshTunnel.buildRemoteT3RunnerScript({ nodeScriptPath: cliPath }),
        );

        const runServer = (port = 0) =>
          Effect.gen(function* () {
            const child = yield* spawner.spawn(
              ChildProcess.make("/bin/sh", [runnerPath, "serve", "a path with spaces"], {
                cwd: fixture,
                env: {
                  PATH: `${bin}:/usr/bin:/bin`,
                  T3_TEST_PORT: String(port),
                },
                detached: false,
              }),
            );
            const ready = yield* Deferred.make<typeof Started.Type>();
            const stdout: string[] = [];
            const output = yield* child.stdout.pipe(
              Stream.decodeText(),
              Stream.splitLines,
              Stream.runForEach((line) =>
                Effect.gen(function* () {
                  stdout.push(line);
                  if (stdout.length === 1) {
                    yield* Deferred.succeed(ready, decodeStarted(line));
                  }
                }),
              ),
              Effect.forkScoped,
            );
            const stderr = yield* child.stderr.pipe(
              Stream.decodeText(),
              Stream.mkString,
              Effect.forkScoped,
            );
            const receipt = yield* Effect.raceFirst(
              Deferred.await(ready),
              Fiber.join(output).pipe(
                Effect.flatMap(() => Fiber.join(stderr)),
                Effect.flatMap((message) =>
                  Effect.die(new Error(`Runner exited before listening: ${message}`)),
                ),
              ),
            );
            // A failed PID assertion must still close the owned fixture server.
            yield* Effect.addFinalizer(() =>
              Effect.gen(function* () {
                if (yield* child.isRunning) {
                  yield* Effect.callback<void>((resume) => {
                    const connection = NodeNet.connect(receipt.port, "127.0.0.1");
                    connection.on("error", () => undefined);
                    connection.once("close", () => resume(Effect.void));
                    return Effect.sync(() => connection.destroy());
                  });
                  yield* child.exitCode;
                }
              }).pipe(Effect.orDie),
            );
            assert.equal(receipt.pid, child.pid);
            assert.deepEqual(receipt.args, ["serve", "a path with spaces"]);
            yield* child.kill({ killSignal: "SIGTERM" });
            assert.equal(yield* child.exitCode, 0);
            yield* Fiber.join(output);
            assert.include(stdout, "graceful shutdown");
            return receipt.port;
          }).pipe(Effect.scoped);

        const port = yield* runServer();
        assert.equal(yield* runServer(port), port);
      }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
    );
  },
);

describe.skipIf(HostProcess.Platform.defaultValue() === "win32")(
  "remote stop process ownership",
  () => {
    it.live.each(["graceful", "timeout", "external"] as const)(
      "confirms the stop result for a %s server",
      (mode) =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
          const fixture = yield* fs.makeTempDirectoryScoped({ prefix: "t3-stop-" });
          const signalPath = path.join(fixture, "signals");
          const child = yield* spawner.spawn(
            ChildProcess.make(
              process.execPath,
              [
                "--input-type=module",
                "-e",
                `import * as fs from "node:fs";
import * as net from "node:net";
const server = net.createServer((socket) => socket.end());
let signals = 0;
process.on("SIGTERM", () => {
  fs.writeFileSync(process.argv[2], String(++signals));
  if (process.argv[1] !== "timeout" || signals > 1) server.close();
});
server.listen(0, "127.0.0.1", () => {
  process.stdout.write(JSON.stringify({ pid: process.pid, port: server.address().port, args: [] }) + "\\n");
});
`,
                mode,
                signalPath,
              ],
              { cwd: fixture, detached: false },
            ),
          );
          // A failed assertion must still stop this captured fixture process.
          yield* Effect.addFinalizer(() =>
            child.kill({ killSignal: "SIGKILL" }).pipe(Effect.ignore),
          );
          const started = decodeStarted(
            yield* child.stdout.pipe(
              Stream.decodeText(),
              Stream.splitLines,
              Stream.take(1),
              Stream.mkString,
            ),
          );
          assert.equal(started.pid, child.pid);
          const savedState = {
            pid: `${child.pid}\n`,
            port: `${started.port}\n`,
            managed: mode === "external" ? "external\n" : "managed\n",
            "process-start": `${readProcessStart(child.pid)}\n`,
          };
          for (const [name, contents] of Object.entries(savedState)) {
            yield* fs.writeFileString(path.join(fixture, name), contents);
          }
          const script = SshTunnel.buildRemoteStopScript(OWNERSHIP_STATE_KEY);
          // Redirect only the state directory. Never use the developer's SSH state.
          const isolatedScript = script.replace(
            /^STATE_DIR=.*$/mu,
            'STATE_DIR="$T3_TEST_STATE_DIR"',
          );
          assert.notEqual(isolatedScript, script);
          const runStop = Effect.fn("test.remoteStop")(function* () {
            const stop = yield* spawner.spawn(
              ChildProcess.make("/bin/sh", ["-s"], {
                cwd: fixture,
                env: { T3_TEST_STATE_DIR: fixture },
                stdin: Stream.make(new TextEncoder().encode(isolatedScript)),
              }),
            );
            return yield* Effect.all(
              {
                stdout: stop.stdout.pipe(Stream.decodeText(), Stream.mkString),
                stderr: stop.stderr.pipe(Stream.decodeText(), Stream.mkString),
                exitCode: stop.exitCode,
              },
              { concurrency: "unbounded" },
            );
          }, Effect.scoped);
          let result = yield* runStop();
          if (mode !== "graceful") {
            assert.isTrue(yield* child.isRunning);
            yield* Effect.callback<void, Error>((resume) => {
              const connection = NodeNet.connect(started.port, "127.0.0.1");
              connection.once("error", (error) => resume(Effect.fail(error)));
              connection.once("close", () => resume(Effect.void));
              return Effect.sync(() => connection.destroy());
            });
          }
          if (mode === "timeout") {
            assert.equal(result.exitCode, 1);
            assert.equal(result.stdout, "");
            assert.include(result.stderr, "did not exit after explicit disconnect");
            assert.include(result.stderr, "keeping its managed state for a later retry");
            assert.equal(yield* fs.readFileString(signalPath), "1");
            for (const [name, contents] of Object.entries(savedState)) {
              assert.equal(yield* fs.readFileString(path.join(fixture, name)), contents);
            }
            result = yield* runStop();
          }
          assert.equal(result.exitCode, 0);
          assert.equal(result.stdout, '{"stopped":true}\n');
          assert.equal(result.stderr, "");
          for (const name of Object.keys(savedState)) {
            assert.isFalse(yield* fs.exists(path.join(fixture, name)));
          }
          if (mode === "external") {
            assert.isFalse(yield* fs.exists(signalPath));
          } else {
            assert.equal(yield* child.exitCode, 0);
            assert.equal(yield* fs.readFileString(signalPath), mode === "timeout" ? "2" : "1");
          }
        }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
    );
  },
);
