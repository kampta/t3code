import { ProviderDriverKind, ProviderInstanceId, type ServerProvider } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { resolveSelectableProvider } from "./providerModels";

function provider(input: {
  readonly driver: string;
  readonly instanceId: string;
  readonly status: ServerProvider["status"];
}): ServerProvider {
  return {
    instanceId: ProviderInstanceId.make(input.instanceId),
    driver: ProviderDriverKind.make(input.driver),
    enabled: true,
    installed: true,
    version: null,
    status: input.status,
    availability: "available",
    auth: { status: "authenticated" },
    checkedAt: "2026-01-01T00:00:00.000Z",
    models: [],
    slashCommands: [],
    skills: [],
  };
}

describe("resolveSelectableProvider", () => {
  it("prefers a ready Claude provider over an errored Codex provider", () => {
    const providers = [
      provider({ driver: "codex", instanceId: "codex", status: "error" }),
      provider({ driver: "claudeAgent", instanceId: "claudeAgent", status: "ready" }),
    ];

    expect(resolveSelectableProvider(providers, null)).toBe("claudeAgent");
  });

  it("does not invent a warning provider as an implicit default", () => {
    const providers = [provider({ driver: "codex", instanceId: "codex", status: "warning" })];

    expect(resolveSelectableProvider(providers, null)).toBe("unconfigured");
  });

  it("preserves an explicitly selected warning provider", () => {
    const providers = [provider({ driver: "codex", instanceId: "codex", status: "warning" })];

    expect(resolveSelectableProvider(providers, ProviderInstanceId.make("codex"))).toBe("codex");
  });
});
