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
      command: "/home/user/.local/bin/codex update",
      executable: "/home/user/.local/bin/codex",
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

it("does not offer an in-place update for a pinned standalone release", () => {
  expect(
    CODEX_PROVIDER_MAINTENANCE.resolve({
      binaryPath:
        "/home/user/.codex/packages/standalone/releases/0.148.0-aarch64-unknown-linux-musl/bin/codex",
      resolvedCommandPath:
        "/home/user/.codex/packages/standalone/releases/0.148.0-aarch64-unknown-linux-musl/bin/codex",
      realCommandPath:
        "/home/user/.codex/packages/standalone/releases/0.148.0-aarch64-unknown-linux-musl/bin/codex",
    }).update,
  ).toBeNull();
});

it("quotes standalone launchers with shell-sensitive paths", () => {
  expect(
    CODEX_PROVIDER_MAINTENANCE.resolve({
      binaryPath: "codex",
      resolvedCommandPath: "/home/First Last/.local/bin/codex",
      realCommandPath:
        "/home/First Last/.codex/packages/standalone/releases/0.148.0-aarch64-unknown-linux-musl/bin/codex",
    }).update,
  ).toMatchObject({
    command: "'/home/First Last/.local/bin/codex' update",
    executable: "/home/First Last/.local/bin/codex",
  });
});
