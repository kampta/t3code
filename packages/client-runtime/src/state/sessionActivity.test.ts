import {
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type OrchestrationSessionStatus,
  type OrchestrationThreadShell,
} from "@t3tools/contracts";
import { afterEach, describe, expect, it } from "@effect/vitest";
import * as Option from "effect/Option";
import { Atom, AtomRegistry } from "effect/unstable/reactivity";

import { PrimaryConnectionTarget } from "../connection/model.ts";
import type { EnvironmentCatalogState } from "./connections.ts";
import { createActiveSessionCountAtom, resolveSessionReaction } from "./sessionActivity.ts";
import type { EnvironmentShellState } from "./shell.ts";

const MAC = EnvironmentId.make("mac");
const SPARK1 = EnvironmentId.make("spark1");
const SPARK2 = EnvironmentId.make("spark2");
const NOW = "2026-09-07T00:00:00.000Z";

function thread(
  status: OrchestrationSessionStatus | null,
  overrides: Partial<OrchestrationThreadShell> = {},
): OrchestrationThreadShell {
  return {
    id: ThreadId.make("thread"),
    projectId: ProjectId.make("project"),
    title: "Session",
    modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6-astra" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    latestTurn: null,
    createdAt: NOW,
    updatedAt: NOW,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    session:
      status === null
        ? null
        : {
            threadId: ThreadId.make("thread"),
            status,
            providerName: "codex",
            runtimeMode: "full-access",
            activeTurnId: status === "running" ? TurnId.make("turn") : null,
            lastError: null,
            updatedAt: NOW,
          },
    latestUserMessageAt: null,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    hasActionableProposedPlan: false,
    ...overrides,
  };
}

function shell(
  threads: readonly OrchestrationThreadShell[],
  status: EnvironmentShellState["status"] = "live",
): EnvironmentShellState {
  return {
    status,
    snapshot:
      status === "empty"
        ? Option.none()
        : Option.some({ snapshotSequence: 1, updatedAt: NOW, projects: [], threads }),
    error: Option.none(),
  };
}

function catalog(environmentIds: readonly EnvironmentId[]): EnvironmentCatalogState {
  return {
    isReady: true,
    entries: new Map(
      environmentIds.map((environmentId) => [
        environmentId,
        {
          target: new PrimaryConnectionTarget({
            environmentId,
            label: environmentId,
            httpBaseUrl: `https://${environmentId}.example.test`,
            wsBaseUrl: `wss://${environmentId}.example.test`,
          }),
          profile: Option.none(),
        },
      ]),
    ),
  };
}

const registries: AtomRegistry.AtomRegistry[] = [];
afterEach(() => {
  for (const registry of registries.splice(0)) registry.dispose();
});

function makeHarness(environmentIds: readonly EnvironmentId[] = [MAC]) {
  const catalogValueAtom = Atom.make(catalog(environmentIds));
  const shellStateValueAtom = Atom.family((_environmentId: EnvironmentId) =>
    Atom.make(shell([], "empty")),
  );
  const count = createActiveSessionCountAtom({ catalogValueAtom, shellStateValueAtom });
  const registry = AtomRegistry.make();
  registries.push(registry);
  return { catalogValueAtom, shellStateValueAtom, count, registry };
}

describe("active session count", () => {
  it("follows session start, activity, and completion", () => {
    const { registry, count, shellStateValueAtom } = makeHarness();
    expect(registry.get(count)).toBe(0);
    for (const status of [
      null,
      "idle",
      "starting",
      "running",
      "ready",
      "interrupted",
      "stopped",
      "error",
    ] as const) {
      registry.set(shellStateValueAtom(MAC), shell([thread(status)]));
      expect(registry.get(count)).toBe(status === "starting" || status === "running" ? 1 : 0);
    }
  });

  it("sums Mac and Sparks independently even when their thread IDs coincide", () => {
    const { registry, count, shellStateValueAtom, catalogValueAtom } = makeHarness([
      MAC,
      SPARK1,
      SPARK2,
    ]);
    registry.set(shellStateValueAtom(MAC), shell([thread("running")]));
    registry.set(shellStateValueAtom(SPARK1), shell([thread("starting")]));
    registry.set(shellStateValueAtom(SPARK2), shell([thread("running")]));
    expect(registry.get(count)).toBe(3);

    registry.set(catalogValueAtom, catalog([MAC, SPARK2]));
    expect(registry.get(count)).toBe(2);
    registry.set(catalogValueAtom, catalog([]));
    expect(registry.get(count)).toBe(0);
  });

  it("drops disconnected and synchronizing snapshots until live data returns", () => {
    const { registry, count, shellStateValueAtom } = makeHarness([MAC, SPARK1]);
    registry.set(shellStateValueAtom(MAC), shell([thread("running")]));
    registry.set(shellStateValueAtom(SPARK1), shell([thread("running")]));
    expect(registry.get(count)).toBe(2);

    for (const status of ["cached", "synchronizing", "empty"] as const) {
      registry.set(shellStateValueAtom(SPARK1), shell([thread("running")], status));
      expect(registry.get(count)).toBe(1);
    }
    registry.set(shellStateValueAtom(SPARK1), shell([thread("ready")]));
    expect(registry.get(count)).toBe(1);
    registry.set(shellStateValueAtom(SPARK1), shell([thread("running")]));
    expect(registry.get(count)).toBe(2);
  });

  it("excludes approvals, user input, and archived sessions until they resume", () => {
    const { registry, count, shellStateValueAtom } = makeHarness();
    for (const overrides of [
      { hasPendingApprovals: true },
      { hasPendingUserInput: true },
      { archivedAt: NOW },
    ]) {
      registry.set(shellStateValueAtom(MAC), shell([thread("running", overrides)]));
      expect(registry.get(count)).toBe(0);
      registry.set(shellStateValueAtom(MAC), shell([thread("running")]));
      expect(registry.get(count)).toBe(1);
    }
  });

  it("counts live background work once per parent and ignores monitoring or failures", () => {
    const { registry, count, shellStateValueAtom } = makeHarness();
    for (const status of [null, "ready", "running"] as const) {
      registry.set(
        shellStateValueAtom(MAC),
        shell([thread(status, { backgroundLiveness: "working" })]),
      );
      expect(registry.get(count)).toBe(1);
    }
    for (const idleThread of [
      thread("ready", { backgroundLiveness: "monitoring" }),
      thread("error", { backgroundLiveness: "working" }),
      thread("ready", { backgroundLiveness: "working", hasPendingApprovals: true }),
      thread("ready", { backgroundLiveness: "working", hasPendingUserInput: true }),
    ]) {
      registry.set(shellStateValueAtom(MAC), shell([idleThread]));
      expect(registry.get(count)).toBe(0);
    }
  });

  it("still counts a running session hidden by snooze or settled metadata", () => {
    const { registry, count, shellStateValueAtom } = makeHarness();
    registry.set(
      shellStateValueAtom(MAC),
      shell([
        thread("running", {
          snoozedUntil: "2026-09-08T00:00:00.000Z",
          settledAt: NOW,
          settledOverride: "settled",
        }),
      ]),
    );
    expect(registry.get(count)).toBe(1);
  });
});

describe("session reactions", () => {
  it("appears at two sessions and escalates through four reactions", () => {
    expect(resolveSessionReaction(0)).toBeNull();
    expect(resolveSessionReaction(1)).toBeNull();
    expect(resolveSessionReaction(2)).toMatchObject({ stage: 0, label: "Getting interesting" });
    expect(resolveSessionReaction(3)).toMatchObject({ stage: 1, label: "Now we’re cooking" });
    expect(resolveSessionReaction(4)).toMatchObject({ stage: 2, label: "Maximum productivity" });
    expect(resolveSessionReaction(5)).toMatchObject({ stage: 3, label: "UNLIMITED POWER" });
  });

  it("caps the reaction at five sessions and returns stable values", () => {
    expect(resolveSessionReaction(2)).toBe(resolveSessionReaction(2));
    expect(resolveSessionReaction(6)).toBe(resolveSessionReaction(5));
    expect(resolveSessionReaction(100)).toBe(resolveSessionReaction(5));
    expect(resolveSessionReaction(3)).toBe(resolveSessionReaction(3));
  });
});
