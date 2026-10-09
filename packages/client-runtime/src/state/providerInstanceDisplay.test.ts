import { ProviderDriverKind, ProviderInstanceId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  normalizeProviderAccentColor,
  providerInstanceInitials,
  resolveProviderInstanceDisplayName,
  shouldShowInstanceBadge,
} from "./providerInstanceDisplay.ts";

const codex = ProviderDriverKind.make("codex");
const claude = ProviderDriverKind.make("claudeAgent");

describe("resolveProviderInstanceDisplayName", () => {
  it("keeps a snapshot name that differs from the brand label", () => {
    expect(
      resolveProviderInstanceDisplayName({
        instanceId: ProviderInstanceId.make("codex"),
        driver: codex,
        displayName: "Work",
      }),
    ).toBe("Work");
  });

  it("humanizes a custom instance id when the snapshot only carries the brand label", () => {
    expect(
      resolveProviderInstanceDisplayName({
        instanceId: ProviderInstanceId.make("codex_personal"),
        driver: codex,
        displayName: "Codex",
      }),
    ).toBe("Codex Personal");
  });

  it("uses the brand label for the default instance", () => {
    expect(
      resolveProviderInstanceDisplayName({
        instanceId: ProviderInstanceId.make("codex"),
        driver: codex,
      }),
    ).toBe("Codex");
  });
});

describe("providerInstanceInitials", () => {
  it("takes the first two characters of a single word", () => {
    expect(providerInstanceInitials("Codex")).toBe("CO");
  });

  it("takes the first character of each of the first two words", () => {
    expect(providerInstanceInitials("Codex Personal")).toBe("CP");
  });

  it("ignores words past the first two", () => {
    expect(providerInstanceInitials("Codex Personal Backup Account")).toBe("CP");
  });

  it("returns an empty string for an empty label", () => {
    expect(providerInstanceInitials("")).toBe("");
  });

  it("keeps an emoji whole instead of splitting its surrogate pair", () => {
    expect(providerInstanceInitials("😀 Work")).toBe("😀W");
    expect(providerInstanceInitials("😀")).toBe("😀");
  });
});

describe("normalizeProviderAccentColor", () => {
  it("accepts a lowercase hex color", () => {
    expect(normalizeProviderAccentColor("#ff8800")).toBe("#ff8800");
  });

  it("accepts an uppercase hex color", () => {
    expect(normalizeProviderAccentColor("#FF8800")).toBe("#FF8800");
  });

  it("rejects a non-hex value", () => {
    expect(normalizeProviderAccentColor("blue")).toBeUndefined();
  });

  it("rejects a short hex value", () => {
    expect(normalizeProviderAccentColor("#fff")).toBeUndefined();
  });

  it("treats undefined and blank as unset", () => {
    expect(normalizeProviderAccentColor(undefined)).toBeUndefined();
    expect(normalizeProviderAccentColor("   ")).toBeUndefined();
  });
});

describe("shouldShowInstanceBadge", () => {
  const codexId = ProviderInstanceId.make("codex");
  const codexWorkId = ProviderInstanceId.make("codex_work");

  it("shows the badge when the entry has an accent color", () => {
    const entry = { instanceId: codexId, driverKind: codex, enabled: true, accentColor: "#ff8800" };
    expect(shouldShowInstanceBadge(entry, [entry])).toBe(true);
  });

  it("shows the badge when two entries share a driver, even without an accent", () => {
    const first = { instanceId: codexId, driverKind: codex, enabled: true, accentColor: undefined };
    const second = { instanceId: codexWorkId, driverKind: codex, enabled: true };
    expect(shouldShowInstanceBadge(first, [first, second])).toBe(true);
  });

  it("hides the badge for a single instance of a driver with no accent", () => {
    const entry = { instanceId: codexId, driverKind: codex, enabled: true, accentColor: undefined };
    const other = {
      instanceId: ProviderInstanceId.make("claudeAgent"),
      driverKind: claude,
      enabled: true,
    };
    expect(shouldShowInstanceBadge(entry, [entry, other])).toBe(false);
  });

  it("ignores a turned-off instance of the same driver", () => {
    const personal = { instanceId: codexId, driverKind: codex, enabled: false };
    const work = { instanceId: codexWorkId, driverKind: codex, enabled: true };
    expect(shouldShowInstanceBadge(work, [personal, work])).toBe(false);
    // Threads still on the turned-off instance stay distinguishable.
    expect(shouldShowInstanceBadge(personal, [personal, work])).toBe(true);
  });
});
