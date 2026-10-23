// @effect-diagnostics nodeBuiltinImport:off - the generated remote shell regression test drives a real child HTTP server and POSIX shell.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { assert, describe, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NetService from "@t3tools/shared/Net";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";
import * as Result from "effect/Result";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";
import { HttpClient, HttpClientError, HttpClientResponse } from "effect/http";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import {
  captureLauncher,
  captureLauncherFor,
  readProcessStart,
  runLauncher,
} from "./launcherTestHarness.ts";
import * as SshAuth from "./auth.ts";
import { remoteStateKey } from "./command.ts";
import { SshCommandError, SshPasswordPromptError, SshReadinessError } from "./errors.ts";
import * as SshTunnel from "./tunnel.ts";

const isSshCommandError = Schema.is(SshCommandError);

// Remote scripts take a precomputed state key; any stable key works inside a temp HOME.
const OWNERSHIP_STATE_KEY = "ownership-regression";

const Started = Schema.Struct({
  pid: Schema.Number,
  port: Schema.Number,
  args: Schema.Array(Schema.String),
});
const decodeStarted = Schema.decodeUnknownSync(Schema.fromJsonString(Started));

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
  hasListenerOnHost: () => Effect.succeed(true),
  reserveLoopbackPort: () => Effect.succeed(41_773),
  findAvailablePort: (preferred) => Effect.succeed(preferred),
});

function commandArgs(command: ChildProcess.Command): ReadonlyArray<string> {
  return command._tag === "StandardCommand" ? command.args.map(String) : [];
}

const ARCHIVE = { archiveVersion: "1.2.3-preview.20260911.4" } as const;

// Emulates `t3 serve`: it takes the home owner lock at startup, then binds
// once a second server has tried the same home (or after 2s), so a concurrent
// launch always sees the home still unclaimed. Like the real server, it
// publishes server-runtime.json after it starts answering HTTP, if at all.
const fakeT3Serve = (publishDelayMs: number | null) => `const fs = require("node:fs");
const http = require("node:http");
const path = require("node:path");
const args = process.argv.slice(2);
const port = Number(args[args.indexOf("--port") + 1]);
const userdata = path.join(args[args.indexOf("--base-dir") + 1], "userdata");
const servers = path.join(process.env.HOME, "servers");
fs.mkdirSync(userdata, { recursive: true });
fs.appendFileSync(servers, process.pid + "\\n");
try {
  fs.writeFileSync(path.join(userdata, "server.lock"), String(process.pid), { flag: "wx" });
} catch {
  console.error("ServerAlreadyRunningError: another server owns " + userdata);
  process.exit(1);
}
setTimeout(() => process.exit(0), 60_000);
const serve = (tries) => {
  if (tries > 0 && fs.readFileSync(servers, "utf8").trim().split("\\n").length < 2) {
    setTimeout(() => serve(tries - 1), 50);
    return;
  }
  http.createServer((_, response) => response.end()).listen(port, "127.0.0.1", () => {
    const origin = "http://127.0.0.1:" + port;
    ${publishDelayMs === null ? "return;" : ""}
    setTimeout(() => {
      fs.writeFileSync(path.join(userdata, "server-runtime.json"), JSON.stringify({ pid: process.pid, port, origin }));
    }, ${publishDelayMs});
  });
};
serve(40);
`;

// The launch's POSIX bootstrap without the account login shell around it, so the
// test machine's login profile cannot move HOME.
const launchStateKey = (command: ChildProcess.Command) =>
  /exec sh -s -- ([^"]+)"/u.exec(commandArgs(command).join(" "))?.[1];

// Runs the remote half of an SSH launch as a local shell
// whose HOME is the given directory. Every launch after the first starts once
// a server is running (or after 10s), which lines up the race deterministically.
const localRemoteShellLayer = (home: string) =>
  Layer.effect(
    ChildProcessSpawner.ChildProcessSpawner,
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      let launches = 0;
      return ChildProcessSpawner.make((command) => {
        if (command._tag !== "StandardCommand") return spawner.spawn(command);
        const gate =
          launches++ === 0
            ? ""
            : 'i=0; while [ ! -s "$HOME/servers" ] && [ "$i" -lt 200 ]; do sleep 0.05; i=$((i+1)); done; ';
        return spawner.spawn(
          ChildProcess.make(
            "sh",
            ["-c", `${gate}exec sh -s -- "$1"`, "sh", launchStateKey(command) ?? ""],
            {
              ...command.options,
              env: { ...command.options.env, HOME: home },
              // Like an SSH session ending, the shell's exit leaves its nohup'd server running.
              detached: false,
            },
          ),
        );
      });
    }),
  );

describe.skipIf(HostProcess.Platform.defaultValue() === "win32")(
  "ssh POSIX process scripts",
  () => {
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
        NodeFS.writeFileSync(
          NodePath.join(stateDir, "run-t3.sh"),
          "previously bundled formatting\n",
        );
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
        assert.equal(
          NodeFS.readFileSync(NodePath.join(stateDir, "pid"), "utf8").trim(),
          String(pid),
        );
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
      const started = Promise.withResolvers<number>();
      const startupServer = NodeNet.createServer((socket) => {
        let pid = "";
        socket.setEncoding("utf8");
        socket.on("data", (chunk: string) => {
          pid += chunk;
        });
        socket.on("error", started.reject);
        socket.on("end", () => {
          started.resolve(Number(pid.trim()));
          socket.end();
        });
      });
      let launchedPid: number | null = null;

      try {
        await new Promise<void>((resolve, reject) => {
          startupServer.once("error", reject);
          startupServer.listen(0, "127.0.0.1", resolve);
        });
        const startupAddress = startupServer.address();
        if (startupAddress === null || typeof startupAddress === "string") {
          throw new Error("startup receipt listener has no TCP address");
        }
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
            'const started = require("node:net").connect(' +
              String(startupAddress.port) +
              ', "127.0.0.1", () => started.end(String(process.pid)));',
            'http.createServer((_request, response) => { response.writeHead(503); response.end("not ready"); }).listen(port, "127.0.0.1");',
          ].join("\n"),
        );
        const runner = { nodeScriptPath: serverEntry } as const;
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
        assert.equal(await started.promise, launchedPid);
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
        if (startupServer.listening) {
          await new Promise<void>((resolve, reject) =>
            startupServer.close((error) => (error ? reject(error) : resolve())),
          );
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
        assert.equal(
          NodeFS.readFileSync(NodePath.join(stateDir, "pid"), "utf8").trim(),
          String(pid),
        );
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

    it.each(["empty", "dead-pending", "dead-owner"] as const)(
      "handles concurrent waiters for an abandoned %s SSH lifecycle lock safely",
      async (unpublished) => {
        const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-ssh-unpublished-"));
        const home = NodePath.join(root, "home");
        const script = SshTunnel.buildRemoteStopScript(OWNERSHIP_STATE_KEY).replaceAll(
          "-ge 1200",
          "-ge 10",
        );
        const key = /ssh-launch\/([^"/]+)"/u.exec(script)?.[1];
        if (!key) throw new Error("missing state key");
        const stateDir = NodePath.join(home, ".t3", "ssh-launch", key);
        const lock = NodePath.join(stateDir, "lifecycle.lock");
        NodeFS.mkdirSync(lock, { recursive: true });
        try {
          if (unpublished !== "empty") {
            const exited = NodeChildProcess.spawn(process.execPath, ["-e", "process.exit(0)"], {
              stdio: "ignore",
            });
            await new Promise<void>((resolve) => exited.once("exit", () => resolve()));
            const record =
              unpublished === "dead-owner" ? `owner.${exited.pid}.0` : `pending.${exited.pid}`;
            NodeFS.writeFileSync(NodePath.join(lock, record), "abandoned\n");
          }
          NodeFS.utimesSync(lock, 0, 0);
          const results = await Promise.all([
            runShellScript(script, { ...process.env, HOME: home }),
            runShellScript(script, { ...process.env, HOME: home }),
          ]);
          for (const result of results) {
            if (unpublished === "dead-owner") {
              assert.equal(result.status, 0, result.stderr);
              assert.deepEqual(JSON.parse(result.stdout.trim()), { stopped: true });
            } else {
              assert.equal(result.status, 1, result.stderr);
              assert.include(
                result.stderr,
                "remove it only after confirming no operation is active",
              );
            }
          }
          assert.equal(NodeFS.existsSync(lock), unpublished !== "dead-owner");
          assert.isFalse(
            NodeFS.readdirSync(stateDir).some((name) => name.startsWith("lifecycle.pending.")),
          );
        } finally {
          NodeFS.rmSync(root, { recursive: true, force: true });
        }
      },
    );

    it("does not let a losing stale-lock reclaimer remove a replacement publisher", async () => {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-ssh-reclaim-race-"));
      const home = NodePath.join(root, "home");
      const reclaimGate = NodePath.join(root, "reclaim");
      const publishGate = NodePath.join(root, "publish");
      const script = SshTunnel.buildRemoteStopScript(OWNERSHIP_STATE_KEY);
      const key = /ssh-launch\/([^"/]+)"/u.exec(script)?.[1];
      if (!key) throw new Error("missing state key");
      const lock = NodePath.join(home, ".t3", "ssh-launch", key, "lifecycle.lock");
      NodeFS.mkdirSync(lock, { recursive: true });
      const exited = NodeChildProcess.spawn(process.execPath, ["-e", "process.exit(0)"], {
        stdio: "ignore",
      });
      await new Promise<void>((resolve) => exited.once("exit", () => resolve()));
      const staleOwner = NodePath.join(lock, `owner.${exited.pid}.0`);
      NodeFS.writeFileSync(staleOwner, "abandoned\n");
      const env = {
        ...process.env,
        HOME: home,
        T3_TEST_STALE_OWNER: staleOwner,
        T3_TEST_RECLAIM_GATE: reclaimGate,
        T3_TEST_PUBLISH_GATE: publishGate,
      };
      const lateScript = script.replaceAll("-ge 1200", "-ge 10").replace(
        'STATE_LIFECYCLE_LOCK_DIR="$STATE_DIR/lifecycle.lock"',
        `exec 3>&2
rm() {
  for path in "$@"; do
    if [ "$path" = "$T3_TEST_STALE_OWNER" ]; then
      printf 'stale-owner-observed\\n' >&3
      while [ ! -f "$T3_TEST_RECLAIM_GATE" ]; do sleep 0.1; done
    fi
  done
  command rm "$@"
}
STATE_LIFECYCLE_LOCK_DIR="$STATE_DIR/lifecycle.lock"`,
      );
      const publisherScript = script.replace(
        "STATE_LIFECYCLE_LOCK_HELD=1\n",
        'STATE_LIFECYCLE_LOCK_HELD=1\nprintf "publisher-paused\\n" >&2\nwhile [ ! -f "$T3_TEST_PUBLISH_GATE" ]; do sleep 0.1; done\n',
      );
      const late = spawnShellScript(lateScript, env);
      let publisher: ReturnType<typeof spawnShellScript> | undefined;
      try {
        await waitForShellStderr(late.child, "stale-owner-observed");
        publisher = spawnShellScript(publisherScript, env);
        await waitForShellStderr(publisher.child, "publisher-paused");
        const replacementInode = NodeFS.statSync(lock).ino;
        NodeFS.writeFileSync(reclaimGate, "reclaim\n");
        const lateResult = await late.result;
        assert.equal(lateResult.status, 1, lateResult.stderr);
        assert.equal(NodeFS.statSync(lock).ino, replacementInode);
        NodeFS.writeFileSync(publishGate, "publish\n");
        const publisherResult = await publisher.result;
        assert.equal(publisherResult.status, 0, publisherResult.stderr);
        assert.isFalse(NodeFS.existsSync(lock));
      } finally {
        NodeFS.writeFileSync(reclaimGate, "reclaim\n");
        NodeFS.writeFileSync(publishGate, "publish\n");
        await Promise.all([late.result, publisher?.result]);
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    });

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

    it.each([false, true])(
      "keeps an unpublished lifecycle owner protected with mkdir EEXIST success=%s",
      async (mkdirReportsExisting) => {
        const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-ssh-publishing-"));
        const home = NodePath.join(root, "home");
        const gate = NodePath.join(root, "publish");
        const script = `${mkdirReportsExisting ? 'mkdir() { command mkdir "$@" 2>/dev/null || return 0; }\n' : ""}${SshTunnel.buildRemoteStopScript(OWNERSHIP_STATE_KEY)}`;
        const key = /ssh-launch\/([^"/]+)"/u.exec(script)?.[1];
        if (!key) throw new Error("missing state key");
        const ownerScript = script.replace(
          "STATE_LIFECYCLE_LOCK_HELD=1\n",
          'STATE_LIFECYCLE_LOCK_HELD=1\nprintf "publisher-paused\\n" >&2\nwhile [ ! -f "$T3_TEST_PUBLISH_GATE" ]; do sleep 0.1; done\n',
        );
        const contenderScript = script.replaceAll("-ge 1200", "-ge 10");
        const env = { ...process.env, HOME: home, T3_TEST_PUBLISH_GATE: gate };
        const owner = spawnShellScript(ownerScript, env);
        let contender: ReturnType<typeof spawnShellScript> | undefined;
        try {
          await waitForShellStderr(owner.child, "publisher-paused");
          const lock = NodePath.join(home, ".t3", "ssh-launch", key, "lifecycle.lock");
          NodeFS.utimesSync(lock, 0, 0);
          contender = spawnShellScript(contenderScript, env);
          const contenderResult = await contender.result;
          assert.equal(contenderResult.status, 1, contenderResult.stderr);
          assert.include(
            contenderResult.stderr,
            "remove it only after confirming no operation is active",
          );
          assert.isTrue(NodeFS.existsSync(lock));
          NodeFS.writeFileSync(gate, "publish\n");
          const ownerResult = await owner.result;
          assert.equal(ownerResult.status, 0, ownerResult.stderr);
          assert.isFalse(NodeFS.existsSync(lock));
        } finally {
          NodeFS.writeFileSync(gate, "publish\n");
          await Promise.all([owner.result, contender?.result]);
          NodeFS.rmSync(root, { recursive: true, force: true });
        }
      },
    );

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
        assert.isTrue(
          NodeFS.existsSync(launchMarker),
          JSON.stringify({ launchResult, stopResult }),
        );
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
  },
);

describe("ssh tunnel scripts", () => {
  it.live("distinguishes an incompatible remote Node from a missing executable", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const home = yield* fs.makeTempDirectoryScoped({ prefix: "t3-ssh-node-" });
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      for (const missing of [false, true]) {
        const nodeFixture = missing
          ? "command() { return 1; }; "
          : 'node() { cat >/dev/null; printf "Remote node 18.20.8 does not satisfy required range >=999.0.0.\\n" >&2; return 1; }; ';
        const remoteShell = ChildProcessSpawner.make((command) => {
          if (command._tag !== "StandardCommand") return spawner.spawn(command);
          return spawner.spawn(
            ChildProcess.make(
              "sh",
              ["-c", `${nodeFixture}eval "$(cat)"`, "sh", launchStateKey(command) ?? ""],
              {
                ...command.options,
                env: {
                  ...command.options.env,
                  HOME: home,
                  NVM_DIR: NodePath.join(home, ".nvm"),
                  FNM_DIR: NodePath.join(home, ".fnm"),
                  VOLTA_HOME: NodePath.join(home, ".volta"),
                },
              },
            ),
          );
        });
        const result = yield* SshTunnel.launchOrReuseRemoteServer(
          { alias: "devbox", hostname: "devbox", username: null, port: null },
          undefined,
          { nodeScriptPath: "/unused/server.ts", nodeEngineRange: ">=999.0.0" },
        ).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, remoteShell),
          Effect.result,
        );
        assert.isTrue(Result.isFailure(result));
        if (!Result.isFailure(result)) return;
        assert.isTrue(isSshCommandError(result.failure));
        if (!isSshCommandError(result.failure)) return;
        assert.equal(result.failure.exitCode, 1);
        assert.equal(result.failure.stderr.includes("missing node on PATH"), missing);
        assert.equal(
          result.failure.stderr.includes("does not satisfy required range >=999.0.0"),
          !missing,
        );
      }
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  it.live("reports only the failed launch's own output, not an earlier launch's log", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const home = yield* fs.makeTempDirectoryScoped({ prefix: "t3-ssh-launch-log-" });
      const target = { alias: "devbox", hostname: "devbox", username: null, port: null };
      const stateDir = NodePath.join(home, ".t3", "ssh-launch", yield* remoteStateKey(target));
      const bin = NodePath.join(home, "bin");
      yield* fs.makeDirectory(stateDir, { recursive: true });
      yield* fs.makeDirectory(bin);
      yield* fs.writeFileString(NodePath.join(stateDir, "server.log"), "EARLIER_LAUNCH\n");
      // The new server exits without output. Readiness then fails after a process from the
      // earlier launch writes through the log descriptor it still holds (fd 3).
      yield* fs.writeFileString(
        NodePath.join(bin, "node"),
        '#!/bin/sh\n[ "$1" = - ] || exit 0\ncat >/dev/null\ncase "$2" in\n  */port) echo 37731 ;;\n  37731) { printf "EARLIER_PROCESS\\n" >&3; } 2>/dev/null; exit 1 ;;\n  *) exit 1 ;;\nesac\n',
        { mode: 0o755 },
      );
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const remoteShell = ChildProcessSpawner.make((command) => {
        if (command._tag !== "StandardCommand") return spawner.spawn(command);
        return spawner.spawn(
          ChildProcess.make(
            "sh",
            [
              "-c",
              'exec 3>>"$1/server.log"; exec sh -s -- "$2"',
              "sh",
              stateDir,
              launchStateKey(command) ?? "",
            ],
            {
              ...command.options,
              env: { HOME: home, PATH: `${bin}:${process.env.PATH ?? ""}` },
              extendEnv: false,
            },
          ),
        );
      });
      const result = yield* SshTunnel.launchOrReuseRemoteServer(target, undefined, {
        nodeScriptPath: "/unused/server.ts",
      }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, remoteShell),
        Effect.result,
      );
      assert.isTrue(Result.isFailure(result));
      if (!Result.isFailure(result)) return;
      assert.isTrue(isSshCommandError(result.failure));
      if (!isSshCommandError(result.failure)) return;
      assert.include(result.failure.stderr, "It wrote nothing to");
      assert.notInclude(result.failure.stderr, "EARLIER_LAUNCH");
      assert.notInclude(result.failure.stderr, "EARLIER_PROCESS");
      for (const [path, mode] of [
        [stateDir, 0o700],
        [NodePath.join(stateDir, "server.log"), 0o600],
      ] as const) {
        assert.equal((yield* fs.stat(path)).mode & 0o777, mode, path);
      }
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
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
        if (launchStateKey(command) !== undefined) {
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
      NodeCrypto.layer,
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

  it.effect.each([false, true])(
    "leaves the managed server running after local cleanup with stale=%s",
    (stale) => {
      let tunnelKillCount = 0;
      let tunnelCount = 0;
      let rejectFirstTunnel = false;
      const staleProbe = Deferred.makeUnsafe<void>();
      let stopCommandCount = 0;
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.sync(() => {
          const args = commandArgs(command);
          if (args.includes("-N")) {
            tunnelCount += 1;
            return makeRunningProcess(() => {
              tunnelKillCount += 1;
            });
          }
          if (launchStateKey(command) !== undefined) {
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
        NodeCrypto.layer,
        Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Layer.succeed(
          HttpClient.HttpClient,
          HttpClient.make((request) =>
            Effect.gen(function* () {
              const failed = rejectFirstTunnel && tunnelCount === 1;
              if (failed) yield* Deferred.succeed(staleProbe, undefined);
              return HttpClientResponse.fromWeb(
                request,
                new Response("", { status: failed ? 503 : 200 }),
              );
            }),
          ),
        ),
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
            if (stale) {
              rejectFirstTunnel = true;
              const reconnect = yield* Effect.forkChild(manager.ensureEnvironment(target));
              yield* Deferred.await(staleProbe);
              yield* TestClock.adjust(Duration.seconds(5));
              yield* Fiber.join(reconnect);
              assert.equal(tunnelKillCount, 1);
            }
          }).pipe(Effect.provide(layer)),
        );

        assert.equal(tunnelKillCount, stale ? 2 : 1);
        assert.equal(stopCommandCount, 0);
      });
    },
  );

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

  it.each([
    { host: "Darwin x86_64", system: "Darwin", machine: "x86_64", downloads: false },
    { host: "Darwin arm64", system: "Darwin", machine: "arm64", downloads: true },
  ])(
    "only downloads a release archive the host can run: $host",
    ({ system, machine, downloads }) => {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-ssh-runner-"));
      try {
        const bin = NodePath.join(root, "bin");
        NodeFS.mkdirSync(bin);
        NodeFS.mkdirSync(NodePath.join(root, "home"));
        NodeFS.writeFileSync(
          NodePath.join(bin, "uname"),
          `#!/bin/sh\ncase "$1" in -s) echo ${system} ;; -m) echo ${machine} ;; esac\n`,
          { mode: 0o755 },
        );
        NodeFS.writeFileSync(
          NodePath.join(bin, "curl"),
          "#!/bin/sh\necho 'download attempt' >&2\necho 'curl: (6) Could not resolve host: github.com' >&2\nexit 6\n",
          { mode: 0o755 },
        );
        const runner = NodePath.join(root, "run-t3.sh");
        NodeFS.writeFileSync(runner, SshTunnel.buildRemoteT3RunnerScript(ARCHIVE));
        const result = NodeChildProcess.spawnSync("sh", [runner, "--version"], {
          encoding: "utf8",
          env: { HOME: NodePath.join(root, "home"), PATH: `${bin}:${process.env.PATH ?? ""}` },
        });
        assert.notEqual(result.status, 0);
        if (downloads) {
          assert.include(result.stderr, "download attempt");
          // A bare curl error does not tell the user that t3 itself could not be downloaded.
          assert.include(result.stderr, ARCHIVE.archiveVersion);
          assert.include(result.stderr, "https://github.com/pingdotgg/t3code/releases/download");
        } else {
          assert.notInclude(result.stderr, "download attempt");
          assert.include(result.stderr, "has no t3 release archive.");
        }
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    },
  );

  it.each([
    { installed: ARCHIVE.archiveVersion, status: 0, reuses: true },
    { installed: ARCHIVE.archiveVersion, status: 1, reuses: false },
    { installed: "9.9.9", status: 0, reuses: false },
  ])(
    "reuses a host t3 only at exactly the requested version: installed $installed, status $status",
    ({ installed, status, reuses }) => {
      const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-ssh-runner-"));
      try {
        const bin = NodePath.join(root, "bin");
        NodeFS.mkdirSync(bin);
        NodeFS.mkdirSync(NodePath.join(root, "home"));
        // A host the release archive cannot run on, so only reuse can succeed.
        NodeFS.writeFileSync(
          NodePath.join(bin, "uname"),
          `#!/bin/sh\ncase "$1" in -s) echo Darwin ;; -m) echo x86_64 ;; esac\n`,
          { mode: 0o755 },
        );
        NodeFS.writeFileSync(
          NodePath.join(bin, "t3"),
          `#!/bin/sh\ncase "$1" in --version) echo "t3 v${installed}"; exit ${status} ;; *) echo "host t3 $*" ;; esac\n`,
          { mode: 0o755 },
        );
        const runner = NodePath.join(root, "run-t3.sh");
        NodeFS.writeFileSync(runner, SshTunnel.buildRemoteT3RunnerScript(ARCHIVE));
        const result = NodeChildProcess.spawnSync("sh", [runner, "serve"], {
          encoding: "utf8",
          env: { HOME: NodePath.join(root, "home"), PATH: `${bin}:${process.env.PATH ?? ""}` },
        });
        if (reuses) {
          assert.equal(result.status, 0);
          assert.equal(result.stdout, "host t3 serve\n");
        } else {
          assert.notEqual(result.status, 0);
          assert.include(result.stderr, "has no t3 release archive.");
        }
      } finally {
        NodeFS.rmSync(root, { recursive: true, force: true });
      }
    },
  );

  // sha256sum and shasum escape a newline in a file name they print, so the home path has one.
  const checksumTools =
    NodeChildProcess.spawnSync("sh", ["-c", 'command -v "$1"', "sh", "shasum"]).status === 0
      ? ["sha256sum", "shasum"]
      : ["sha256sum"];
  it.each(checksumTools)("names missing gzip after verifying with %s", (checksum) => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-ssh-runner-"));
    try {
      const bin = NodePath.join(root, "bin");
      const release = NodePath.join(root, "release");
      const home = NodePath.join(root, "home\ncache");
      const archive = `t3-${ARCHIVE.archiveVersion}-linux-x64.tar.gz`;
      NodeFS.mkdirSync(bin);
      NodeFS.mkdirSync(home);
      NodeFS.mkdirSync(NodePath.join(release, "t3"), { recursive: true });
      NodeFS.writeFileSync(NodePath.join(release, "t3", "t3"), "#!/bin/sh\necho ok\n", {
        mode: 0o755,
      });
      NodeChildProcess.execFileSync("tar", ["-czf", archive, "t3"], { cwd: release });
      NodeChildProcess.execFileSync("sh", ["-c", `sha256sum ${archive} > SHA256SUMS`], {
        cwd: release,
      });
      for (const tool of ["sh", "mkdir", "mktemp", "cat", "rm", "grep", "cut", checksum, "mv"]) {
        NodeFS.symlinkSync(
          NodeChildProcess.execFileSync("sh", ["-c", `command -v ${tool}`], {
            encoding: "utf8",
          }).trim(),
          NodePath.join(bin, tool),
        );
      }
      // GNU tar needs external gzip; BSD tar decompresses internally.
      NodeFS.writeFileSync(NodePath.join(bin, "tar"), '#!/bin/sh\nexec gzip -dc "$2"\n', {
        mode: 0o755,
      });
      NodeFS.writeFileSync(
        NodePath.join(bin, "uname"),
        '#!/bin/sh\ncase "$1" in -s) echo Linux ;; -m) echo x86_64 ;; esac\n',
        { mode: 0o755 },
      );
      NodeFS.writeFileSync(
        NodePath.join(bin, "curl"),
        `#!/bin/sh\nfor last; do :; done\nfor a; do case "$a" in http*) src="${release}/\${a##*/}" ;; esac; done\nexec cat "$src" > "$last"\n`,
        { mode: 0o755 },
      );
      const runner = NodePath.join(root, "run-t3.sh");
      NodeFS.writeFileSync(runner, SshTunnel.buildRemoteT3RunnerScript(ARCHIVE));
      const result = NodeChildProcess.spawnSync(NodePath.join(bin, "sh"), [runner, "--version"], {
        encoding: "utf8",
        env: { HOME: home, PATH: bin },
      });
      assert.notEqual(result.status, 0);
      assert.match(result.stderr, /(missing|needs) gzip/);
      assert.notInclude(result.stderr, "Cannot exec");
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });

  it("names missing tar before downloading the release archive", () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-ssh-runner-"));
    try {
      const bin = NodePath.join(root, "bin");
      const home = NodePath.join(root, "home");
      NodeFS.mkdirSync(bin);
      NodeFS.mkdirSync(home);
      for (const tool of ["sh", "mkdir", "mktemp", "cat", "rm"]) {
        NodeFS.symlinkSync(
          NodeChildProcess.execFileSync("sh", ["-c", `command -v ${tool}`], {
            encoding: "utf8",
          }).trim(),
          NodePath.join(bin, tool),
        );
      }
      NodeFS.writeFileSync(
        NodePath.join(bin, "uname"),
        '#!/bin/sh\ncase "$1" in -s) echo Linux ;; -m) echo x86_64 ;; esac\n',
        { mode: 0o755 },
      );
      NodeFS.writeFileSync(
        NodePath.join(bin, "curl"),
        '#!/bin/sh\necho "download attempt" >&2\nexit 1\n',
        { mode: 0o755 },
      );
      const runner = NodePath.join(root, "run-t3.sh");
      NodeFS.writeFileSync(runner, SshTunnel.buildRemoteT3RunnerScript(ARCHIVE));
      const result = NodeChildProcess.spawnSync(NodePath.join(bin, "sh"), [runner, "--version"], {
        encoding: "utf8",
        env: { HOME: home, PATH: bin },
      });
      assert.notEqual(result.status, 0);
      assert.include(result.stderr, "needs tar");
      assert.notInclude(result.stderr, "download attempt");
    } finally {
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
    const spawner = ChildProcessSpawner.make(() =>
      Effect.succeed(makeSuccessfulProcess('loaded nvm default\n{"remotePort":3774}\n')),
    );
    const layerSpawner = Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner);
    const layerProcess = Layer.merge(
      Layer.mergeAll(NodeServices.layer, NodeCrypto.layer),
      layerSpawner,
    );

    return Effect.gen(function* () {
      const result = yield* SshTunnel.launchOrReuseRemoteServer(target, undefined, ARCHIVE);
      assert.equal(result.remotePort, 3774);
    }).pipe(Effect.provide(layerProcess));
  });

  it.effect.skipIf(HostProcess.Platform.defaultValue() === "win32")(
    "loads login settings without changing the POSIX bootstrap or its state key",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-ssh-login-" });
        const startupFile = `${directory}/bash-env`;
        const bash = `${directory}/bash`;
        // System profiles can reset HOME. Suppress profile reads while BASH_ENV
        // checks that the launcher still requests Bash login mode.
        yield* fs.writeFileString(bash, '#!/bin/sh\nexec /bin/bash --noprofile "$@"\n');
        yield* fs.chmod(bash, 0o755);
        yield* fs.writeFileString(
          startupFile,
          "if shopt -q login_shell; then export T3_SSH_LOGIN_ENV=loaded; fi\n",
        );
        const target = { alias: "login-test", hostname: "login-test", username: null, port: null };
        const remoteCommands: string[] = [];
        const capture = ChildProcessSpawner.make((command) =>
          Effect.sync(() => {
            const args = commandArgs(command);
            remoteCommands.push(args.slice(args.indexOf(target.alias) + 1).join(" "));
            return makeSuccessfulProcess('{"remotePort":3774,"credential":"test"}\n');
          }),
        );
        yield* SshTunnel.launchOrReuseRemoteServer(target, undefined, ARCHIVE).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, capture),
        );
        yield* SshTunnel.issueRemotePairingToken(target, undefined, ARCHIVE).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, capture),
        );
        yield* fs.writeFileString(
          `${directory}/.zprofile`,
          "if [[ -o login ]]; then export T3_SSH_LOGIN_ENV=loaded; fi\n",
        );
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const cases = [
          { shell: bash, outerShell: "/bin/sh", expected: "loaded" },
          { shell: "/bin/sh", outerShell: "/bin/sh", expected: "missing" },
          { shell: "", outerShell: "/bin/sh", expected: "missing" },
          { shell: "/unavailable/csh", outerShell: "/bin/sh", expected: "missing" },
          { shell: "/unavailable/tcsh", outerShell: "/bin/sh", expected: "missing" },
          { shell: "/unavailable/pwsh", outerShell: "/bin/sh", expected: "missing" },
        ];
        if (yield* fs.exists("/bin/zsh")) {
          cases.push({ shell: "/bin/zsh", outerShell: "/bin/zsh", expected: "loaded" });
        }
        if (yield* fs.exists("/bin/tcsh")) {
          cases.push({ shell: "/bin/tcsh", outerShell: "/bin/tcsh", expected: "missing" });
        }
        const environment = yield* HostProcess.Environment;
        for (const folder of (environment.PATH ?? "").split(":")) {
          const nu = `${folder}/nu`;
          if (folder && (yield* fs.exists(nu))) {
            cases.push({ shell: nu, outerShell: nu, expected: "missing" });
            break;
          }
        }
        for (const { shell, outerShell, expected, index, remoteCommand } of cases.flatMap((entry) =>
          remoteCommands.map((remoteCommand, index) => ({ ...entry, remoteCommand, index })),
        )) {
          const child = yield* spawner.spawn(
            ChildProcess.make(outerShell, ["-c", remoteCommand], {
              env: {
                PATH: environment.PATH ?? "/usr/bin:/bin",
                HOME: directory,
                XDG_CONFIG_HOME: directory,
                SHELL: shell,
                BASH_ENV: startupFile,
                ZDOTDIR: directory,
                T3_SSH_LOGIN_ENV: "",
              },
              stdin: {
                stream: Stream.make(
                  new TextEncoder().encode('printf "%s:%s" "${T3_SSH_LOGIN_ENV:-missing}" "$1"'),
                ),
                endOnDone: true,
              },
            }),
          );
          const [stdout, stderr, exitCode] = yield* Effect.all(
            [
              child.stdout.pipe(
                Stream.decodeText(),
                Stream.runFold(
                  () => "",
                  (a, b) => a + b,
                ),
              ),
              child.stderr.pipe(
                Stream.decodeText(),
                Stream.runFold(
                  () => "",
                  (a, b) => a + b,
                ),
              ),
              child.exitCode,
            ],
            { concurrency: "unbounded" },
          );
          assert.equal(Number(exitCode), 0, stderr);
          // First-run shell messages can precede the bootstrap response.
          const response = stdout.trim().split(/\r?\n/u).at(-1) ?? "";
          assert.match(
            response,
            new RegExp(index === 0 ? `^${expected}:[a-f0-9]{16}$` : `^${expected}:$`),
            shell || "unset SHELL",
          );
        }
      }).pipe(Effect.provide(Layer.merge(NodeServices.layer, NodeCrypto.layer))),
  );

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
    const layerProcess = Layer.merge(
      Layer.mergeAll(NodeServices.layer, NodeCrypto.layer),
      layerSpawner,
    );
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
        NodeCrypto.layer,
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
          if (isSshCommandError(result.failure)) {
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
        NodeCrypto.layer,
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
          if (isSshCommandError(result.failure)) {
            assert.equal(result.failure.exitCode, 255);
            assert.equal(result.failure.stderr, diagnostic);
          }
        }
      }).pipe(Effect.provide(layer), Effect.scoped);
    }),
  );

  it.effect("waits for a tunnel whose ssh connection outlasts the readiness timeout", () =>
    Effect.gen(function* () {
      const LOCAL_PORT = 41_773;
      // ssh spends ConnectTimeout on each unreachable address before the forward listens.
      const SSH_CONNECT_MS = 25_000;
      let forwardListensAt = Number.POSITIVE_INFINITY;
      const tunnelSpawned = yield* Deferred.make<void>();
      const forwardListening = (port: number) =>
        Clock.currentTimeMillis.pipe(
          Effect.map((now) => port === LOCAL_PORT && now >= forwardListensAt),
        );
      const spawner = ChildProcessSpawner.make((command) =>
        commandArgs(command).includes("-N")
          ? Effect.gen(function* () {
              forwardListensAt = (yield* Clock.currentTimeMillis) + SSH_CONNECT_MS;
              yield* Deferred.succeed(tunnelSpawned, undefined);
              return makeRunningProcess(() => undefined);
            })
          : Effect.succeed(makeSuccessfulProcess('{"remotePort":3773}\n')),
      );
      const httpClient = HttpClient.make((request) =>
        forwardListening(LOCAL_PORT).pipe(
          Effect.flatMap((listening) =>
            listening
              ? Effect.succeed(
                  HttpClientResponse.fromWeb(request, new Response("", { status: 200 })),
                )
              : Effect.fail(
                  new HttpClientError.HttpClientError({
                    reason: new HttpClientError.TransportError({
                      request,
                      description: `connect ECONNREFUSED 127.0.0.1:${LOCAL_PORT}`,
                    }),
                  }),
                ),
          ),
        ),
      );
      const net = NetService.NetService.of({
        ...testNetService,
        reserveLoopbackPort: () => Effect.succeed(LOCAL_PORT),
        canListenOnHost: (port) =>
          forwardListening(port).pipe(Effect.map((listening) => !listening)),
        isPortAvailableOnLoopback: (port) =>
          forwardListening(port).pipe(Effect.map((listening) => !listening)),
        hasListenerOnHost: (port) => forwardListening(port),
      });
      const layer = Layer.mergeAll(
        NodeServices.layer,
        NodeCrypto.layer,
        Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Layer.succeed(HttpClient.HttpClient, httpClient),
        Layer.succeed(NetService.NetService, net),
        SshAuth.SshPasswordPrompt.disabledLayer,
        SshTunnel.SshEnvironmentManager.layer({ resolveCliRunner: Effect.succeed(ARCHIVE) }),
      );
      yield* Effect.gen(function* () {
        const manager = yield* SshTunnel.SshEnvironmentManager;
        const ensure = yield* Effect.forkChild(
          manager.ensureEnvironment({
            alias: "devbox",
            hostname: "devbox",
            username: null,
            port: null,
          }),
        );
        yield* Deferred.await(tunnelSpawned);
        yield* TestClock.adjust(Duration.seconds(60));
        const environment = yield* Fiber.join(ensure);
        assert.equal(environment.httpBaseUrl, `http://127.0.0.1:${LOCAL_PORT}/`);
      }).pipe(Effect.provide(layer), Effect.scoped);
    }),
  );

  it.effect.each([
    // Over a ~1s RTT link each request through the forward takes a few round trips.
    { condition: "responses take over a second", reconnectResponseMs: 2_500 },
    // An alive server whose event loop stalls (e.g. on a blocked fsync) answers late.
    { condition: "server stalls for 20 seconds", reconnectResponseMs: 20_000 },
    // ssh closes the forwarded connection when nothing listens on the remote port.
    { condition: "server is gone", reconnectResponseMs: 0, serverGone: true },
  ])("connects and reuses a tunnel whose $condition", ({ reconnectResponseMs, serverGone }) =>
    Effect.gen(function* () {
      const RESPONSE_MS = 2_500;
      const tunnelSpawned = yield* Deferred.make<void>();
      const reconnectProbed = yield* Deferred.make<void>();
      const forwardReplaced = yield* Deferred.make<void>();
      let forwards = 0;
      let connected = false;
      const spawner = ChildProcessSpawner.make((command) =>
        commandArgs(command).includes("-N")
          ? Effect.sync(() => ++forwards).pipe(
              Effect.flatMap((count) =>
                Deferred.succeed(count === 1 ? tunnelSpawned : forwardReplaced, undefined),
              ),
              Effect.as(makeRunningProcess(() => undefined)),
            )
          : Effect.succeed(makeSuccessfulProcess('{"remotePort":3773}\n')),
      );
      const httpClient = HttpClient.make((request) =>
        Effect.gen(function* () {
          if (connected) yield* Deferred.succeed(reconnectProbed, undefined);
          yield* Effect.sleep(Duration.millis(connected ? reconnectResponseMs : RESPONSE_MS));
          if (connected && serverGone) {
            return yield* new HttpClientError.HttpClientError({
              reason: new HttpClientError.TransportError({
                request,
                description: "socket hang up",
              }),
            });
          }
          return HttpClientResponse.fromWeb(request, new Response("", { status: 200 }));
        }),
      );
      const layer = Layer.mergeAll(
        NodeServices.layer,
        NodeCrypto.layer,
        Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Layer.succeed(HttpClient.HttpClient, httpClient),
        Layer.succeed(NetService.NetService, testNetService),
        SshAuth.SshPasswordPrompt.disabledLayer,
        SshTunnel.SshEnvironmentManager.layer({ resolveCliRunner: Effect.succeed(ARCHIVE) }),
      );
      yield* Effect.gen(function* () {
        const manager = yield* SshTunnel.SshEnvironmentManager;
        const ensure = yield* Effect.forkChild(
          Effect.result(
            manager.ensureEnvironment({
              alias: "devbox",
              hostname: "devbox",
              username: null,
              port: null,
            }),
          ),
        );
        yield* Deferred.await(tunnelSpawned);
        yield* TestClock.adjust(Duration.seconds(60));
        const result = yield* Fiber.join(ensure);
        assert.isTrue(
          Result.isSuccess(result),
          Result.isFailure(result) ? result.failure.message : undefined,
        );
        if (Result.isFailure(result)) return;

        connected = true;
        const reconnect = yield* Effect.forkChild(manager.ensureEnvironment(result.success.target));
        yield* Deferred.await(reconnectProbed);
        yield* TestClock.adjust(Duration.seconds(60));
        const outcome = yield* Effect.raceFirst(
          Fiber.join(reconnect).pipe(Effect.map((environment) => environment.httpBaseUrl)),
          Deferred.await(forwardReplaced).pipe(Effect.as("replaced the forward")),
        );
        assert.equal(outcome, serverGone ? "replaced the forward" : result.success.httpBaseUrl);
      }).pipe(Effect.provide(layer), Effect.scoped);
    }),
  );

  it.effect("reconnects a saved alias through the port its ssh config names now", () =>
    Effect.gen(function* () {
      let configPort = 53_791;
      const tunnelPorts: Array<string | undefined> = [];
      const spawner = ChildProcessSpawner.make((command) => {
        const args = commandArgs(command);
        if (args.includes("-G")) {
          return Effect.succeed(
            makeSuccessfulProcess(`hostname 127.0.0.1\nuser developer\nport ${configPort}\n`),
          );
        }
        if (args.includes("-N")) {
          tunnelPorts.push(args[args.indexOf("-p") + 1]);
          return Effect.succeed(makeRunningProcess(() => undefined));
        }
        return Effect.succeed(makeSuccessfulProcess('{"remotePort":3773}\n'));
      });
      const layer = Layer.mergeAll(
        NodeServices.layer,
        NodeCrypto.layer,
        Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Layer.succeed(HttpClient.HttpClient, testHttpClient),
        Layer.succeed(NetService.NetService, testNetService),
        SshAuth.SshPasswordPrompt.disabledLayer,
        SshTunnel.SshEnvironmentManager.layer({ resolveCliRunner: Effect.succeed(ARCHIVE) }),
      );
      yield* Effect.gen(function* () {
        const manager = yield* SshTunnel.SshEnvironmentManager;
        const added = yield* manager.ensureEnvironment({
          alias: "lima-sandbox",
          hostname: "lima-sandbox",
          username: null,
          port: null,
        });
        // The VM restarts on a new port; the client reconnects with the target it saved.
        yield* manager.disconnectEnvironment(added.target);
        configPort = 53_812;
        yield* manager.ensureEnvironment(added.target);
        assert.deepStrictEqual(tunnelPorts, ["53791", "53812"]);
      }).pipe(Effect.provide(layer), Effect.scoped);
    }),
  );

  it.effect("fails with a readiness error when ssh never connects", () =>
    Effect.gen(function* () {
      const tunnelSpawned = yield* Deferred.make<void>();
      let tunnelKilled = false;
      const spawner = ChildProcessSpawner.make((command) =>
        commandArgs(command).includes("-N")
          ? Deferred.succeed(tunnelSpawned, undefined).pipe(
              Effect.as(
                makeRunningProcess(() => {
                  tunnelKilled = true;
                }),
              ),
            )
          : Effect.succeed(makeSuccessfulProcess('{"remotePort":3773}\n')),
      );
      const layer = Layer.mergeAll(
        NodeServices.layer,
        NodeCrypto.layer,
        Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Layer.succeed(HttpClient.HttpClient, hangingHttpClient),
        Layer.succeed(
          NetService.NetService,
          NetService.NetService.of({
            ...testNetService,
            hasListenerOnHost: () => Effect.succeed(false),
          }),
        ),
        SshAuth.SshPasswordPrompt.disabledLayer,
        SshTunnel.SshEnvironmentManager.layer({ resolveCliRunner: Effect.succeed(ARCHIVE) }),
      );
      yield* Effect.gen(function* () {
        const manager = yield* SshTunnel.SshEnvironmentManager;
        const ensure = yield* Effect.forkChild(
          Effect.result(
            manager.ensureEnvironment({
              alias: "devbox",
              hostname: "devbox",
              username: null,
              port: null,
            }),
          ),
        );
        yield* Deferred.await(tunnelSpawned);
        yield* TestClock.adjust(Duration.minutes(2));
        const result = yield* Fiber.join(ensure);
        assert.isTrue(Result.isFailure(result));
        if (Result.isFailure(result)) {
          assert.instanceOf(result.failure, SshReadinessError);
          assert.include(result.failure.message, "SSH connection to devbox did not come up");
        }
        assert.isTrue(tunnelKilled);
      }).pipe(Effect.provide(layer), Effect.scoped);
    }),
  );

  it.effect.each([
    {
      name: "does not ask for a password when the agent cannot sign an accepted key",
      stderr:
        'sign_and_send_pubkey: signing failed for ED25519 "/home/julius/.ssh/id_ed25519" from agent: communication with agent failed\r\n' +
        "julius@devbox: Permission denied (publickey,password).\r\n",
      expectedPromptAttempts: [],
    },
    {
      name: "asks for a password when signing failure has no agent provenance",
      stderr:
        'sign_and_send_pubkey: signing failed for ED25519-CERT "key-cert.pub": agent refused operation\n' +
        "julius@devbox: Permission denied (publickey,password).\n",
      expectedPromptAttempts: [1],
    },
    {
      name: "does not ask for a password when the host key cannot be verified",
      stderr: "Host key verification failed.\r\n",
      expectedPromptAttempts: [],
    },
    {
      name: "asks for a password when the server rejects every key",
      stderr: "julius@devbox: Permission denied (publickey,password).\r\n",
      expectedPromptAttempts: [1],
    },
  ])("$name", ({ stderr, expectedPromptAttempts }) =>
    Effect.gen(function* () {
      const promptAttempts: Array<number> = [];
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.succeed(
          commandArgs(command).includes("-G")
            ? makeSuccessfulProcess("")
            : {
                ...makeSuccessfulProcess(""),
                exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(255)),
                stderr: Stream.make(new TextEncoder().encode(stderr)),
              },
        ),
      );
      const layer = Layer.mergeAll(
        NodeServices.layer,
        NodeCrypto.layer,
        Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Layer.succeed(HttpClient.HttpClient, testHttpClient),
        Layer.succeed(NetService.NetService, testNetService),
        Layer.succeed(
          SshAuth.SshPasswordPrompt,
          SshAuth.SshPasswordPrompt.of({
            isAvailable: true,
            request: (request) =>
              Effect.sync(() => {
                promptAttempts.push(request.attempt);
                return null;
              }),
          }),
        ),
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
        assert.deepEqual(promptAttempts, expectedPromptAttempts);
        if (Result.isFailure(result)) {
          if (expectedPromptAttempts.length === 0) {
            assert.instanceOf(result.failure, SshCommandError);
            if (isSshCommandError(result.failure)) {
              assert.equal(result.failure.stderr, stderr);
            }
          } else {
            assert.instanceOf(result.failure, SshPasswordPromptError);
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
            if (args.includes("-c")) {
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
          NodeCrypto.layer,
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

  it.effect.each([
    { name: "rides a live ControlMaster", masterLive: true },
    { name: "keeps the auth failure without a live ControlMaster", masterLive: false },
  ])("when the tunnel cannot sign in again, $name", ({ masterLive }) =>
    Effect.gen(function* () {
      const denied = "julius@devbox: Permission denied (publickey,keyboard-interactive).\n";
      const openedForwards: Array<string | undefined> = [];
      const cancelledForwards: Array<string | undefined> = [];
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.sync(() => {
          const args = commandArgs(command);
          const forward = args.includes("-L") ? args[args.indexOf("-L") + 1] : undefined;
          const muxCommand = args.includes("-O") ? args[args.indexOf("-O") + 1] : undefined;
          const failed = (stderr: string) => ({
            ...makeSuccessfulProcess(""),
            exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(255)),
            stderr: Stream.make(new TextEncoder().encode(stderr)),
          });
          if (args.includes("-G")) {
            return makeSuccessfulProcess("hostname devbox\ncontrolpath /tmp/t3-cm\n");
          }
          if (muxCommand === "check") {
            return masterLive
              ? makeSuccessfulProcess("")
              : failed("Control socket connect: ENOENT\n");
          }
          if (muxCommand === "cancel") {
            cancelledForwards.push(forward);
            return makeSuccessfulProcess("");
          }
          if (forward !== undefined) {
            // Only the signed-in master can pass the host's one-time-code prompt.
            if (!masterLive || args.includes("ControlPath=none")) return failed(denied);
            openedForwards.push(forward);
            return muxCommand === "forward"
              ? makeSuccessfulProcess("")
              : makeRunningProcess(() => undefined);
          }
          return makeSuccessfulProcess('{"remotePort":3773}\n');
        }),
      );
      const layer = Layer.mergeAll(
        NodeServices.layer,
        NodeCrypto.layer,
        Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Layer.succeed(
          HttpClient.HttpClient,
          HttpClient.make((request) =>
            openedForwards.length > 0 ? testHttpClient.execute(request) : Effect.never,
          ),
        ),
        Layer.succeed(NetService.NetService, testNetService),
        SshAuth.SshPasswordPrompt.disabledLayer,
        SshTunnel.SshEnvironmentManager.layer({ resolveCliRunner: Effect.succeed(ARCHIVE) }),
      );
      const target = { alias: "devbox", hostname: "devbox", username: null, port: null };
      yield* Effect.gen(function* () {
        const manager = yield* SshTunnel.SshEnvironmentManager;
        const result = yield* Effect.result(manager.ensureEnvironment(target));
        if (!masterLive) {
          assert.isTrue(Result.isFailure(result));
          if (Result.isFailure(result)) {
            assert.instanceOf(result.failure, SshCommandError);
            assert.include(result.failure.message, "Permission denied");
          }
          assert.deepEqual(openedForwards, []);
          return;
        }
        assert.equal(Result.isFailure(result) ? result.failure.message : "connected", "connected");
        assert.lengthOf(openedForwards, 1);
        yield* manager.disconnectEnvironment(target);
        // The master keeps a forward after the client that asked for it exits.
        assert.deepEqual(cancelledForwards, openedForwards);
      }).pipe(Effect.provide(layer), Effect.scoped);
    }),
  );

  it.live("concurrent launches on one remote home record the server that owns it", () =>
    // The second row's server never publishes server-runtime.json.
    Effect.forEach([1_500, null, "wrapper"] as const, (variant) =>
      Effect.gen(function* () {
        const publishDelayMs = variant === "wrapper" ? 0 : variant;
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const home = yield* fs.makeTempDirectoryScoped({ prefix: "t3-ssh-launch-" });
        const serversFile = path.join(home, "servers");
        yield* Effect.addFinalizer(() =>
          fs.readFileString(serversFile).pipe(
            Effect.map((pids) => {
              for (const pid of pids.split("\n").filter(Boolean)) {
                try {
                  process.kill(Number(pid), "SIGKILL");
                } catch {}
              }
            }),
            Effect.ignore,
          ),
        );
        const serverScript = path.join(home, "fake-t3.cjs");
        yield* fs.writeFileString(serverScript, fakeT3Serve(publishDelayMs));
        const wrapperScript = path.join(home, "wrapper.cjs");
        yield* fs.writeFileString(
          wrapperScript,
          `const child = require("node:child_process").spawn(process.execPath, [${JSON.stringify(serverScript)}, ...process.argv.slice(2)], { stdio: "inherit" });
child.on("exit", (code) => process.exit(code ?? 1));`,
        );
        // A launch that died holding the lock must not block or double-admit later ones.
        const launchLock = path.join(home, ".t3", "ssh-launch", ".launch.lock");
        yield* fs.makeDirectory(path.dirname(launchLock), { recursive: true });
        yield* fs.writeFileString(launchLock, "999999999\n");
        const targets = [
          { alias: "devbox", hostname: "devbox", username: null, port: null },
          { alias: "devbox-tailnet", hostname: "devbox-tailnet", username: null, port: null },
        ];

        const launches = yield* Effect.forEach(
          targets,
          (target) =>
            SshTunnel.launchOrReuseRemoteServer(target, undefined, {
              nodeScriptPath: variant === "wrapper" ? wrapperScript : serverScript,
            }),
          { concurrency: "unbounded" },
        ).pipe(Effect.provide(localRemoteShellLayer(home)));

        const ownerPid = Number(
          yield* fs.readFileString(path.join(home, ".t3", "userdata", "server.lock")),
        );
        assert.sameMembers(
          launches.map((launch) => launch.remoteServerKind),
          ["managed", "external"],
        );
        assert.equal(launches[0]!.remotePort, launches[1]!.remotePort);
        assert.deepEqual((yield* fs.readFileString(serversFile)).trim().split("\n"), [
          String(ownerPid),
        ]);
        for (const [index, launch] of launches.entries()) {
          if (launch.remoteServerKind !== "managed") continue;
          const stateKey = yield* remoteStateKey(targets[index]!);
          const recordedPid = yield* fs.readFileString(
            path.join(home, ".t3", "ssh-launch", stateKey, "pid"),
          );
          assert.equal(
            Number(recordedPid.trim()),
            ownerPid,
            `${targets[index]!.alias} reported a managed server, but its recorded PID is not the server that owns the remote home`,
          );
        }
        assert.isFalse(yield* fs.exists(launchLock));
      }).pipe(Effect.scoped),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );
});

// On Linux kill -0 accepts a thread ID and kill <tid> signals its whole process,
// but ps lists only thread-group leaders. After a reboot the managed pid file can
// name a thread or a whole process that is not our server.
describe.runIf(HostProcess.Platform.defaultValue() === "linux")("stale managed pid file", () => {
  const FAKE_SERVER = `const fs = require("node:fs");
const http = require("node:http");
const path = require("node:path");
const args = process.argv.slice(2);
const flag = (name) => args[args.indexOf(name) + 1];
const server = http.createServer((request, response) => {
  // Closes the listener but stays up, like a server stuck in its teardown.
  if (request.url === "/stop") {
    server.close();
    setInterval(() => {}, 1 << 30);
    response.setHeader("connection", "close");
  }
  response.end("ok");
});
server.listen(Number(flag("--port") ?? 0), "127.0.0.1", () => {
  const port = server.address().port;
  const base = flag("--base-dir");
  if (base) {
    fs.mkdirSync(path.join(base, "userdata"), { recursive: true });
    fs.writeFileSync(
      path.join(base, "userdata", "server-runtime.json"),
      JSON.stringify({ version: 1, pid: process.pid, port, origin: "http://127.0.0.1:" + port }),
    );
  }
  console.log(port);
});
`;
  const target = { alias: "devbox", hostname: "devbox", username: null, port: null } as const;
  const staleIds = ["a thread of an unrelated process", "an unrelated process"] as const;

  const isRunning = (pid: number) =>
    Effect.flatMap(FileSystem.FileSystem, (fs) => fs.readFileString(`/proc/${pid}/stat`)).pipe(
      Effect.map((stat) => !/^\d+ \(.*\) Z/u.test(stat)),
      Effect.orElseSucceed(() => false),
    );

  const prepare = (staleId: (typeof staleIds)[number]) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const realSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-stale-pid-" });
      const home = `${root}/home`;
      const fakeServer = `${root}/fake-t3.cjs`;
      yield* fs.makeDirectory(home, { recursive: true });
      yield* fs.writeFileString(fakeServer, FAKE_SERVER);
      // Runs the remote command locally under the temp HOME. setsid keeps a launched server alive
      // when the command's process group goes away, as sshd would.
      const sshAsLocalShell = ChildProcessSpawner.make((command) => {
        if (command._tag !== "StandardCommand" || command.command !== "ssh") {
          return realSpawner.spawn(command);
        }
        if (command.args.includes("-G")) return Effect.succeed(makeSuccessfulProcess(""));
        const stateKey = launchStateKey(command);
        const remoteArgs =
          stateKey === undefined
            ? command.args.slice(command.args.indexOf("--") + 2)
            : ["sh", "-s", "--", ...stateKey.split(" ")];
        return realSpawner.spawn(
          ChildProcess.make("setsid", ["-w", ...remoteArgs], {
            ...command.options,
            env: { PATH: process.env.PATH ?? "", HOME: home },
            extendEnv: false,
          }),
        );
      });
      const holder = yield* realSpawner.spawn(ChildProcess.make("node", [fakeServer]));
      const holderPort = Number(
        Option.getOrThrow(
          yield* holder.stdout.pipe(Stream.decodeText(), Stream.splitLines, Stream.runHead),
        ),
      );
      const tid = Number(
        (yield* fs.readDirectory(`/proc/${holder.pid}/task`)).find(
          (task) => task !== `${holder.pid}`,
        ),
      );
      assert.isTrue(Number.isInteger(tid));
      return {
        fs,
        home,
        fakeServer,
        holder,
        holderPort,
        sshAsLocalShell,
        staleId: staleId === "an unrelated process" ? holder.pid : tid,
        stateDir: `${home}/.t3/ssh-launch/${yield* remoteStateKey(target)}`,
      };
    });

  it.live.each(staleIds)("launches a fresh server when the managed pid file names %s", (kind) =>
    Effect.gen(function* () {
      const { fs, home, fakeServer, holderPort, sshAsLocalShell, staleId, stateDir } =
        yield* prepare(kind);
      const launch = SshTunnel.launchOrReuseRemoteServer(target, undefined, {
        nodeScriptPath: fakeServer,
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, sshAsLocalShell));
      const readPid = fs
        .readFileString(`${stateDir}/pid`)
        .pipe(Effect.map((pid) => Number(pid.trim())));
      const launchedPids: Array<number> = [];
      yield* Effect.addFinalizer(() =>
        Effect.forEach(launchedPids, (pid) =>
          Effect.sync(() => {
            try {
              process.kill(pid, "SIGKILL");
            } catch {}
          }),
        ),
      );

      const first = yield* launch;
      const firstPid = yield* readPid;
      launchedPids.push(firstPid);
      assert.deepEqual(yield* launch, first);
      assert.equal(yield* readPid, firstPid);

      // Reboot: our server is gone and its recorded PID now names something that
      // answers on the recorded port.
      process.kill(firstPid, "SIGKILL");
      yield* fs.remove(`${home}/.t3/userdata/server-runtime.json`);
      yield* fs.writeFileString(`${stateDir}/pid`, `${staleId}\n`);
      yield* fs.writeFileString(`${stateDir}/port`, `${holderPort}\n`);

      const relaunched = yield* launch;
      const pid = yield* readPid;
      if (pid !== staleId) launchedPids.push(pid);
      assert.notEqual(
        relaunched.remotePort,
        holderPort,
        `the launcher kept ${kind} as its running server`,
      );
      assert.isTrue(yield* isRunning(pid));
    }).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  it.live("a reconnect names the previous server while it is still shutting down", () =>
    Effect.gen(function* () {
      const { fs, fakeServer, sshAsLocalShell, stateDir } = yield* prepare("an unrelated process");
      const launch = SshTunnel.launchOrReuseRemoteServer(target, undefined, {
        nodeScriptPath: fakeServer,
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, sshAsLocalShell));

      const first = yield* launch;
      const pid = Number((yield* fs.readFileString(`${stateDir}/pid`)).trim());
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          try {
            process.kill(pid, "SIGKILL");
          } catch {}
        }),
      );
      yield* HttpClient.get(`http://127.0.0.1:${first.remotePort}/stop`).pipe(
        Effect.flatMap((response) => response.text),
        Effect.provide(FetchHttpClient.layer),
      );

      const error = yield* Effect.flip(launch);
      assert.include(
        error.message,
        `The previous remote T3 server (PID ${pid}) is still shutting down.`,
      );
      assert.isTrue(yield* isRunning(pid), "the launcher signalled the stopping server");
    }).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  it.live("an update replaces the managed server after its connection is renamed", () =>
    Effect.gen(function* () {
      const { fs, home, fakeServer, sshAsLocalShell, stateDir } =
        yield* prepare("an unrelated process");
      const updatedServer = `${home}/fake-t3-next.cjs`;
      yield* fs.writeFileString(updatedServer, FAKE_SERVER);
      const renamed = { ...target, alias: "devbox-renamed" };
      const launch = (
        launchTarget: typeof renamed,
        nodeScriptPath: string,
        launcherId: string | null = "a".repeat(32),
      ) =>
        SshTunnel.launchOrReuseRemoteServer(
          launchTarget,
          undefined,
          { nodeScriptPath },
          launcherId,
        ).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, sshAsLocalShell));
      const runtimePid = fs
        .readFileString(`${home}/.t3/userdata/server-runtime.json`)
        .pipe(Effect.map((runtime) => Number(JSON.parse(runtime).pid)));
      const launchedPids: Array<number> = [];
      yield* Effect.addFinalizer(() =>
        Effect.forEach(launchedPids, (pid) =>
          Effect.sync(() => {
            try {
              process.kill(pid, "SIGKILL");
            } catch {}
          }),
        ),
      );

      assert.equal((yield* launch(target, fakeServer)).remoteServerKind, "managed");
      const originalPid = yield* runtimePid;
      launchedPids.push(originalPid);

      // Another desktop on another version shares the server; it must not take it over.
      const otherDesktop = yield* launch(
        { ...target, alias: "devbox-other" },
        updatedServer,
        "b".repeat(32),
      );
      assert.equal(otherDesktop.remoteServerKind, "external");
      assert.isTrue(yield* isRunning(originalPid), "another desktop replaced the server");

      const afterRename = yield* launch(renamed, fakeServer);
      assert.equal(yield* runtimePid, originalPid, "renaming alone restarted the server");
      assert.equal(
        yield* fs.readFileString(`${stateDir}/managed`),
        "external\n",
        "the old alias still owns the server, so its disconnect would stop it",
      );
      const afterUpdate = yield* launch(renamed, updatedServer);
      launchedPids.push(yield* runtimePid);
      assert.deepEqual(
        [afterRename.remoteServerKind, afterUpdate.remoteServerKind],
        ["managed", "managed"],
      );
      assert.isFalse(
        yield* isRunning(originalPid),
        "the update left the server launched under the old alias running",
      );

      // A rename and an update in one reconnect still replace the server.
      const updatedPid = yield* runtimePid;
      const renamedAndUpdated = yield* launch({ ...target, alias: "devbox-again" }, fakeServer);
      launchedPids.push(yield* runtimePid);
      assert.equal(renamedAndUpdated.remoteServerKind, "managed");
      assert.isFalse(yield* isRunning(updatedPid), "a rename with an update kept the old server");

      // A server that a launch without an ID started under a reused key is not this desktop's.
      const again = { ...target, alias: "devbox-again" };
      process.kill(yield* runtimePid, "SIGKILL");
      assert.equal((yield* launch(again, fakeServer, null)).remoteServerKind, "managed");
      const unownedPid = yield* runtimePid;
      launchedPids.push(unownedPid);
      const final = yield* launch({ ...target, alias: "devbox-final" }, updatedServer);
      assert.equal(final.remoteServerKind, "external");
      assert.isTrue(yield* isRunning(unownedPid), "a stale launcher ID claimed another's server");
    }).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  it.live(
    "another desktop with the same connection details neither owns nor stops the server",
    () =>
      Effect.gen(function* () {
        const { fs, home, fakeServer, sshAsLocalShell, stateDir } =
          yield* prepare("an unrelated process");
        const launch = (launcherId: string) =>
          SshTunnel.launchOrReuseRemoteServer(
            target,
            undefined,
            { nodeScriptPath: fakeServer },
            launcherId,
          ).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, sshAsLocalShell));
        const runtimePid = fs
          .readFileString(`${home}/.t3/userdata/server-runtime.json`)
          .pipe(Effect.map((runtime) => Number(JSON.parse(runtime).pid)));

        assert.equal((yield* launch("a".repeat(32))).remoteServerKind, "managed");
        const serverPid = yield* runtimePid;
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            try {
              process.kill(serverPid, "SIGKILL");
            } catch {}
          }),
        );

        const otherDesktop = yield* launch("b".repeat(32));
        assert.equal(
          yield* fs.readFileString(`${stateDir}/launcher`),
          `${"a".repeat(32)}\n`,
          "the second desktop recorded itself as the server's launcher",
        );
        assert.equal(otherDesktop.remoteServerKind, "external");

        const layer = Layer.mergeAll(
          NodeServices.layer,
          Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, sshAsLocalShell),
          Layer.succeed(HttpClient.HttpClient, testHttpClient),
          Layer.succeed(NetService.NetService, testNetService),
          SshAuth.SshPasswordPrompt.disabledLayer,
          SshTunnel.SshEnvironmentManager.layer({
            resolveCliRunner: Effect.succeed(ARCHIVE),
            resolveLauncherId: Effect.succeed("b".repeat(32)),
          }),
        );
        yield* Effect.flatMap(SshTunnel.SshEnvironmentManager, (manager) =>
          manager.disconnectEnvironment(target),
        ).pipe(Effect.provide(layer));
        assert.isTrue(
          yield* isRunning(serverPid),
          "the second desktop's disconnect stopped the server the first desktop launched",
        );
      }).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  it.live.each(staleIds)("disconnecting does not signal %s named by the managed pid file", (kind) =>
    Effect.gen(function* () {
      const { fs, holder, sshAsLocalShell, staleId, stateDir } = yield* prepare(kind);
      yield* fs.makeDirectory(stateDir, { recursive: true });
      yield* fs.writeFileString(`${stateDir}/pid`, `${staleId}\n`);
      yield* fs.writeFileString(`${stateDir}/managed`, "managed\n");
      yield* fs.writeFileString(`${stateDir}/process-start`, "stale-process-identity\n");
      const layer = Layer.mergeAll(
        NodeServices.layer,
        Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, sshAsLocalShell),
        Layer.succeed(HttpClient.HttpClient, testHttpClient),
        Layer.succeed(NetService.NetService, testNetService),
        SshAuth.SshPasswordPrompt.disabledLayer,
        SshTunnel.SshEnvironmentManager.layer({ resolveCliRunner: Effect.succeed(ARCHIVE) }),
      );

      yield* Effect.flatMap(SshTunnel.SshEnvironmentManager, (manager) =>
        manager.disconnectEnvironment(target),
      ).pipe(Effect.provide(layer));
      assert.isTrue(yield* isRunning(holder.pid), `disconnecting signalled ${kind}`);
    }).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );
});

// On macOS, Node can bind 127.0.0.1:<port> while another process (such as the
// desktop app with network access on) listens on *:<port>. Linux refuses that
// bind unless both sockets set SO_REUSEPORT, so on Linux the preload adds it to
// the picker's probe and the holder sets it too.

// Reuse decisions live in generated shell, so drive the real launch script in
// a temporary HOME. Each archive version gets a pre-installed fake runtime
// whose server writes the default runtime file the way `t3 serve` does.
describe("archive launch script", () => {
  const windowsHost = HostProcess.Platform.defaultValue() === "win32";
  const FAKE_T3 = `const fs = require("node:fs");
const http = require("node:http");
const net = require("node:net");
const path = require("node:path");
const [version, ...args] = process.argv.slice(2);
const flag = (name) => args[args.indexOf(name) + 1];
const probe = (port) =>
  new Promise((resolve) => {
    http
      .get({ hostname: "127.0.0.1", port, path: "/", timeout: 1000 }, (response) => {
        response.resume();
        resolve(response.statusCode === 200);
      })
      .once("error", () => resolve(false));
  });
const canBind = (port) =>
  new Promise((resolve) => {
    const server = net.createServer();
    server.once("error", () => resolve(false));
    server.listen(port, "127.0.0.1", () => server.close(() => resolve(true)));
  });
(async () => {
  if (args[0] === "--version") return console.log(version);
  if (args[0] === "serve") {
    const server = http.createServer((_request, response) => response.end("ok"));
    server.listen(Number(flag("--port")), "127.0.0.1", () => {
      const port = server.address().port;
      const userdata = path.join(flag("--base-dir"), "userdata");
      fs.mkdirSync(userdata, { recursive: true });
      fs.writeFileSync(
        path.join(userdata, "server-runtime.json"),
        JSON.stringify({
          version: 1,
          pid: process.pid,
          port,
          origin: "http://127.0.0.1:" + port,
          startedAt: new Date().toISOString(),
          serverVersion: version,
        }),
      );
    });
    return;
  }
  const [helper, a, b, c] = args.slice(1);
  if (helper === "pick-port") {
    const start = Number(fs.existsSync(a) ? fs.readFileSync(a, "utf8") : b);
    for (let port = start; port < start + Number(c); port += 1) {
      if (await canBind(port)) return process.stdout.write(String(port));
    }
  } else if (helper === "wait-ready") {
    const deadline = Date.now() + Number(b);
    while (Date.now() < deadline) {
      if (await probe(Number(a))) return;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  } else if (helper === "runtime-port") {
    try {
      const runtime = JSON.parse(fs.readFileSync(a, "utf8"));
      process.kill(runtime.pid, 0);
      return process.stdout.write(runtime.pid + " " + runtime.port);
    } catch {}
  } else if (helper === "keep-server") {
    try {
      const runtime = JSON.parse(fs.readFileSync(a, "utf8"));
      const order = runtime.serverVersion.localeCompare(version, "en", { numeric: true });
      if (String(runtime.pid) === b && order >= 0) return;
    } catch {}
  }
  process.exitCode = 1;
})();
`;

  const isAlive = (pid: number) => {
    try {
      process.kill(pid, 0);
      return true;
    } catch {
      return false;
    }
  };

  const freeLoopbackPort = Effect.callback<number>((resume) => {
    const server = NodeNet.createServer();
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      server.close(() =>
        resume(Effect.succeed(typeof address === "object" && address ? address.port : 0)),
      );
    });
  });

  const runScript = (home: string, args: ReadonlyArray<string>) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const child = yield* spawner.spawn(
        ChildProcess.make("sh", args, { env: { HOME: home } }),
      );
      const collect = (stream: Stream.Stream<Uint8Array, PlatformError.PlatformError>) =>
        stream.pipe(Stream.decodeText(), Stream.mkString);
      const [stdout, stderr, exitCode] = yield* Effect.all(
        [collect(child.stdout), collect(child.stderr), child.exitCode.pipe(Effect.map(Number))],
        { concurrency: "unbounded" },
      );
      return { stdout, stderr, exitCode };
    });

  // Installs a fake runtime per version so the runner never downloads. The
  // seeded remembered port keeps the test off the default 3773, which a live
  // T3 on this machine may own.
  const prepareLaunch = (versions: ReadonlyArray<string>) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-archive-launch-" });
      const home = `${root}/home`;
      const stateDir = `${home}/.t3/ssh-launch/test-state`;
      const fakeT3 = `${root}/fake-t3.cjs`;
      yield* fs.makeDirectory(stateDir, { recursive: true });
      yield* fs.writeFileString(`${stateDir}/port`, `${yield* freeLoopbackPort}\n`);
      yield* fs.writeFileString(fakeT3, FAKE_T3);
      for (const version of versions) {
        const runtimeDir = `${home}/.t3/runtime/versions/${version}`;
        yield* fs.makeDirectory(runtimeDir, { recursive: true });
        // The runner gets only HOME, so `node` would resolve on sh's default PATH, which may not have it.
        yield* fs.writeFileString(
          `${runtimeDir}/t3`,
          `#!/bin/sh\nexec '${process.execPath}' '${fakeT3}' '${version}' "$@"\n`,
        );
        yield* fs.chmod(`${runtimeDir}/t3`, 0o755);
        yield* fs.writeFileString(`${runtimeDir}/.install-complete`, `${version}\n`);
      }
      const readManagedPid = fs
        .readFileString(`${stateDir}/pid`)
        .pipe(Effect.map((pid) => Number(pid.trim())));
      const readServerVersion = fs
        .readFileString(`${home}/.t3/userdata/server-runtime.json`)
        .pipe(Effect.map((raw) => (JSON.parse(raw) as { serverVersion: string }).serverVersion));
      // Launched servers outlive the script, so stop each PID it recorded.
      const launchedPids = new Set<number>();
      yield* Effect.addFinalizer(() =>
        readManagedPid.pipe(
          Effect.map((pid) => [...launchedPids, pid]),
          Effect.orElseSucceed(() => [...launchedPids]),
          Effect.map((pids) => {
            for (const pid of pids) {
              if (isAlive(pid)) process.kill(pid, "SIGKILL");
            }
          }),
        ),
      );
      const connect = (archiveVersion: string) =>
        Effect.gen(function* () {
          const launch = `${root}/launch-${archiveVersion}.sh`;
          yield* fs.writeFileString(launch, SshTunnel.buildRemoteLaunchScript({ archiveVersion }));
          const result = yield* runScript(home, [launch, "test-state"]);
          assert.equal(result.exitCode, 0, result.stderr);
          const pid = yield* readManagedPid;
          launchedPids.add(pid);
          return pid;
        });
      return { connect, readManagedPid, readServerVersion };
    });

  it.live.skipIf(windowsHost)(
    "a reconnect from an older client keeps a newer managed server running",
    () =>
      Effect.gen(function* () {
        const { connect, readServerVersion } = yield* prepareLaunch(["0.1.5", "0.1.6"]);
        const newerPid = yield* connect("0.1.6");
        assert.equal(yield* readServerVersion, "0.1.6");

        const pidAfterOlderClient = yield* connect("0.1.5");
        assert.isTrue(
          isAlive(newerPid),
          "the 0.1.6 managed server was killed by a 0.1.5 reconnect",
        );
        assert.equal(pidAfterOlderClient, newerPid);
        assert.equal(yield* readServerVersion, "0.1.6");
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  // An NFS home can carry a ~/.t3 symlink whose target exists only on another host.
  it.live.skipIf(windowsHost)("launches through a ~/.t3 symlink whose target is missing", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-dangling-home-" });
      const home = `${root}/home`;
      const target = `${root}/shared/t3`;
      yield* fs.makeDirectory(home, { recursive: true });
      yield* fs.symlink(target, `${home}/.t3`);
      const launch = `${root}/launch.sh`;
      yield* fs.writeFileString(
        launch,
        SshTunnel.buildRemoteLaunchScript({
          archiveVersion: "0.1.6",
          releaseBaseUrl: "http://127.0.0.1:9",
        }),
      );
      const result = yield* runScript(home, [launch, "test-state"]);
      assert.notInclude(result.stderr, "File exists");
      assert.isTrue(yield* fs.exists(`${target}/ssh-launch/test-state`), result.stderr);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live.skipIf(windowsHost)("a newer client still replaces an older managed server", () =>
    Effect.gen(function* () {
      const { connect, readServerVersion } = yield* prepareLaunch(["0.1.5", "0.1.6"]);
      const olderPid = yield* connect("0.1.5");

      const newerPid = yield* connect("0.1.6");
      assert.notEqual(newerPid, olderPid);
      assert.isFalse(isAlive(olderPid));
      assert.equal(yield* readServerVersion, "0.1.6");
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});

describe("SSH launch during desktop update", () => {
  it.effect("archive launcher reuses the replacement without host Node", () =>
    Effect.gen(function* () {
      const script = yield* captureLauncherFor({ archiveVersion: "1.2.3" });
      const result = runLauncher(script, {
        archive: true,
        marker: { startedAt: 0, expiresAt: 60_000 },
        readyAt: 2_000,
      });
      assert.deepEqual(result.output, { remotePort: 4888, serverKind: "external" });
      assert.equal(result.now, 2_000);
      assert.isFalse(result.events.some((event) => event.startsWith("start ")));
    }),
  );

  it.effect.each(["managed", "external"] as const)(
    "reuses recorded %s server without runtime metadata or an update wait",
    (recordedKind) =>
      Effect.gen(function* () {
        const result = runLauncher(yield* captureLauncher, {
          marker: { startedAt: 0, expiresAt: 60_000 },
          recordedKind,
        });
        assert.deepEqual(result.output, { remotePort: 5999, serverKind: recordedKind });
        assert.equal(result.now, 0);
        assert.isFalse(result.events.some((event) => event.startsWith("kill ")));
        assert.isFalse(result.events.some((event) => event.startsWith("start ")));
      }),
  );

  it.effect.each([
    ["malformed", "broken"],
    ["future", { startedAt: 1_000, expiresAt: 61_000 }],
    ["expired", { startedAt: -60_000, expiresAt: 0 }],
  ] as const)("preserves %s markers without waiting", ([, marker]) =>
    Effect.gen(function* () {
      const result = runLauncher(yield* captureLauncher, { marker });
      assert.equal(result.now, 0);
      assert.isTrue(result.markerExists);
      assert.deepEqual(result.output, { remotePort: 5999, serverKind: "managed" });
    }),
  );

  it.effect("bounds the wait when the host clock jumps back", () =>
    Effect.gen(function* () {
      const result = runLauncher(yield* captureLauncher, {
        marker: { startedAt: 0, expiresAt: 60_000 },
        clockJump: -30_000,
      });
      assert.deepEqual(result.output, { remotePort: 5999, serverKind: "managed" });
      assert.isBelow(result.events.filter((event) => event.startsWith("runtime ")).length, 62);
    }),
  );

  it.effect("refreshes runtime after readiness removes the marker between polls", () =>
    Effect.gen(function* () {
      const result = runLauncher(yield* captureLauncher, {
        marker: { startedAt: 0, expiresAt: 60_000 },
        readyAt: 8_000,
        clearMarkerOnReady: true,
      });
      assert.deepEqual(result.output, { remotePort: 4888, serverKind: "external" });
      assert.equal(result.now, 8_000);
      assert.isFalse(result.markerExists);
      assert.isFalse(result.events.some((event) => event.startsWith("start ")));
      assert.isFalse(result.events.some((event) => event.startsWith("kill ")));
    }),
  );

  it.effect("reuses a runtime published before the first marker read", () =>
    Effect.gen(function* () {
      const result = runLauncher(yield* captureLauncher, {
        marker: { startedAt: 0, expiresAt: 60_000 },
        publishAfterFirstRuntimeRead: true,
      });
      assert.deepEqual(result.output, { remotePort: 4888, serverKind: "external" });
      assert.equal(result.now, 0);
      assert.isFalse(result.events.some((event) => event.startsWith("start ")));
    }),
  );

  it.effect("waits across the eight-second installer gap and reuses the replacement server", () =>
    Effect.gen(function* () {
      const result = runLauncher(yield* captureLauncher, {
        marker: { startedAt: 0, expiresAt: 60_000 },
        readyAt: 8_000,
      });
      assert.deepEqual(result.output, { remotePort: 4888, serverKind: "external" });
      assert.equal(result.now, 8_000);
      assert.isFalse(result.events.some((event) => event.startsWith("start ")));
      assert.isAtLeast(result.events.filter((event) => event.startsWith("runtime ")).length, 2);
    }),
  );

  it.effect("keeps waiting while a stale runtime record points at a dead port", () =>
    Effect.gen(function* () {
      const result = runLauncher(yield* captureLauncher, {
        marker: { startedAt: 0, expiresAt: 60_000 },
        initialPort: 3773,
        readyAt: 3_000,
      });
      assert.deepEqual(result.output, { remotePort: 4888, serverKind: "external" });
      assert.equal(result.now, 3_000);
      assert.isFalse(result.events.some((event) => event.startsWith("start ")));
    }),
  );

  it.effect("starts a server when the marker expires without a replacement becoming ready", () =>
    Effect.gen(function* () {
      const result = runLauncher(yield* captureLauncher, {
        marker: { startedAt: 0, expiresAt: 60_000 },
      });
      assert.deepEqual(result.output, { remotePort: 5999, serverKind: "managed" });
      assert.isAtLeast(result.now, 60_000);
      assert.isAtMost(result.now, 61_000);
      assert.isTrue(result.markerExists, "an expired marker is ignored, not deleted");
      assert.equal(result.events.filter((event) => event.startsWith("start ")).length, 1);
    }),
  );

  it.effect.each([false, true])(
    "waits for a boot service between two servers (archive: %s)",
    (archive) =>
      Effect.gen(function* () {
        const script = yield* archive
          ? captureLauncherFor({ archiveVersion: "1.2.3" })
          : captureLauncher;
        const result = runLauncher(script, { archive, serviceServing: true, readyAt: 5_000 });
        assert.deepEqual(result.output, { remotePort: 4888, serverKind: "external" });
        assert.equal(result.now, 5_000);
        assert.isFalse(result.events.some((event) => event.startsWith("start ")));
      }),
  );

  it.effect("fails without starting a second server when the home's owner never answers", () =>
    Effect.gen(function* () {
      const result = runLauncher(yield* captureLauncher, {
        serviceServing: true,
        allowFailure: true,
      });
      assert.equal(result.status, 1);
      assert.include(result.stderr, "no second server was started");
      assert.equal(result.now, 60_000);
      assert.isFalse(result.events.some((event) => event.startsWith("start ")));
    }),
  );

  it.effect("starts immediately without a marker", () =>
    Effect.gen(function* () {
      const result = runLauncher(yield* captureLauncher, {});
      assert.deepEqual(result.output, { remotePort: 5999, serverKind: "managed" });
      assert.equal(result.now, 0);
      assert.equal(result.events.filter((event) => event.startsWith("start ")).length, 1);
      assert.equal(result.events.filter((event) => event.startsWith("marker ")).length, 1);
      assert.equal(result.events.filter((event) => event.startsWith("runtime ")).length, 1);
    }),
  );
});

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
