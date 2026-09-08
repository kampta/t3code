import type { EnvironmentId } from "@t3tools/contracts";
import * as Option from "effect/Option";
import { Atom } from "effect/unstable/reactivity";

import type { EnvironmentCatalogState } from "./connections.ts";
import type { EnvironmentShellState } from "./shell.ts";

export function createActiveSessionCountAtom(input: {
  readonly catalogValueAtom: Atom.Atom<EnvironmentCatalogState>;
  readonly shellStateValueAtom: (environmentId: EnvironmentId) => Atom.Atom<EnvironmentShellState>;
}) {
  return Atom.make((get) => {
    let count = 0;
    for (const environmentId of get(input.catalogValueAtom).entries.keys()) {
      const shell = get(input.shellStateValueAtom(environmentId));
      // Cached running states cannot establish that a disconnected agent is still working.
      if (shell.status !== "live" || Option.isNone(shell.snapshot)) continue;
      for (const thread of shell.snapshot.value.threads) {
        if (
          thread.archivedAt !== null ||
          thread.hasPendingApprovals ||
          thread.hasPendingUserInput ||
          thread.session?.status === "error"
        ) {
          continue;
        }
        // Background agents contribute one working parent session, never one per agent.
        if (
          thread.session?.status === "starting" ||
          thread.session?.status === "running" ||
          thread.backgroundLiveness === "working"
        ) {
          count += 1;
        }
      }
    }
    return count;
  }).pipe(Atom.withLabel("active-session-count"));
}

export const REACTION_IMAGE = {
  width: 1_000,
  height: 1_500,
  cropLeft: 500,
  cropWidth: 500,
  cropHeight: 330,
} as const;

const SESSION_REACTIONS = [
  { stage: 0, label: "Getting interesting", cropTop: 0 },
  { stage: 1, label: "Now we’re cooking", cropTop: 350 },
  { stage: 2, label: "Maximum productivity", cropTop: 735 },
  { stage: 3, label: "UNLIMITED POWER", cropTop: 1_125 },
] as const;

export function resolveSessionReaction(count: number) {
  if (count < 2) return null;
  return SESSION_REACTIONS[Math.min(Math.floor(count) - 2, SESSION_REACTIONS.length - 1)] ?? null;
}
