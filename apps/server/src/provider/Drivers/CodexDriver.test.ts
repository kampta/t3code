import { expect, it } from "@effect/vitest";

import { CODEX_PROVIDER_MAINTENANCE } from "./CodexDriver.ts";

it("updates standalone Codex through its native updater", () => {
  expect(
    CODEX_PROVIDER_MAINTENANCE.resolve({
      binaryPath: "codex",
      resolvedCommandPath: "/home/user/.local/bin/codex",
      realCommandPath:
        "/home/user/.codex/packages/standalone/releases/0.148.0-aarch64-unknown-linux-musl/bin/codex",
    }),
  ).toEqual({
    provider: "codex",
    packageName: "@openai/codex",
    update: {
      command: "codex update",
      executable: "codex",
      args: ["update"],
      lockKey: "codex-native",
    },
  });
});

it("keeps npm updates for npm-managed Codex", () => {
  expect(
    CODEX_PROVIDER_MAINTENANCE.resolve({
      binaryPath: "codex",
      resolvedCommandPath: "/opt/homebrew/bin/codex",
      realCommandPath: "/opt/homebrew/lib/node_modules/@openai/codex/bin/codex.js",
    }).update,
  ).toEqual({
    command: "npm install -g --allow-scripts=@openai/codex @openai/codex@latest",
    executable: "npm",
    args: ["install", "-g", "--allow-scripts=@openai/codex", "@openai/codex@latest"],
    lockKey: "npm-global",
  });
});
