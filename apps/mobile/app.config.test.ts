import { afterEach, describe, expect, it, vi } from "vite-plus/test";

// Keep config parsing independent of local env files and build credentials.
vi.mock("../../scripts/lib/public-config.ts", () => ({
  loadRepoEnv: () => ({
    APP_VARIANT: process.env.APP_VARIANT,
    T3CODE_IOS_PERSONAL_TEAM: process.env.T3CODE_IOS_PERSONAL_TEAM,
    T3CODE_IOS_PERSONAL_TEAM_ID: process.env.T3CODE_IOS_PERSONAL_TEAM_ID,
    T3CODE_IOS_PERSONAL_TEAM_BUNDLE_ID: process.env.T3CODE_IOS_PERSONAL_TEAM_BUNDLE_ID,
  }),
}));

const PERSONAL_TEAM_ENV_NAMES = [
  "T3CODE_IOS_PERSONAL_TEAM",
  "T3CODE_IOS_PERSONAL_TEAM_ID",
  "T3CODE_IOS_PERSONAL_TEAM_BUNDLE_ID",
] as const;

afterEach(() => {
  vi.unstubAllEnvs();
  vi.resetModules();
});

async function loadConfig(env: Readonly<Record<string, string | undefined>> = {}) {
  vi.stubEnv("APP_VARIANT", undefined);
  for (const name of PERSONAL_TEAM_ENV_NAMES) {
    vi.stubEnv(name, "");
  }
  for (const [name, value] of Object.entries(env)) {
    vi.stubEnv(name, value);
  }
  vi.resetModules();
  return (await import("./app.config")).default;
}

function pluginNames(config: Awaited<ReturnType<typeof loadConfig>>) {
  return (config.plugins ?? []).map((plugin) => (typeof plugin === "string" ? plugin : plugin[0]));
}

describe("mobile app config", () => {
  it("preserves the official production signing capabilities", async () => {
    const config = await loadConfig({ APP_VARIANT: "production" });

    expect(config.ios?.appleTeamId).toBe("ARK85ZXQ4Z");
    expect(config.ios?.associatedDomains).toEqual([
      "applinks:clerk.t3.codes",
      "webcredentials:clerk.t3.codes",
    ]);
    expect(pluginNames(config)).toContain("expo-notifications");
    expect(pluginNames(config)).not.toContain("./plugins/withoutIosPersonalTeamCapabilities.cjs");
  });

  it("uses Personal Team signing without unsupported domains", async () => {
    const config = await loadConfig({
      APP_VARIANT: "production",
      T3CODE_IOS_PERSONAL_TEAM: "1",
      T3CODE_IOS_PERSONAL_TEAM_ID: "ABC1234567",
      T3CODE_IOS_PERSONAL_TEAM_BUNDLE_ID: "com.example.t3code",
    });
    const plugins = pluginNames(config);

    expect(config.ios?.appleTeamId).toBe("ABC1234567");
    expect(config.ios?.bundleIdentifier).toBe("com.example.t3code");
    expect(config.ios).not.toHaveProperty("associatedDomains");
    expect(config.ios).not.toHaveProperty("entitlements");
    expect(config.extra?.iosPersonalTeamBuild).toBe(true);
    expect(plugins.indexOf("./plugins/withoutIosPersonalTeamCapabilities.cjs")).toBeLessThan(
      plugins.indexOf("expo-notifications"),
    );
  });

  it("rejects a malformed Personal Team ID", async () => {
    await expect(
      loadConfig({
        T3CODE_IOS_PERSONAL_TEAM: "1",
        T3CODE_IOS_PERSONAL_TEAM_ID: "not-a-team-id",
        T3CODE_IOS_PERSONAL_TEAM_BUNDLE_ID: "com.example.t3code",
      }),
    ).rejects.toThrow(/10-character uppercase Apple Team ID/u);
  });
});

it.each(["prod", ""])("rejects APP_VARIANT=%j before selecting production IDs", async (value) => {
  await expect(loadConfig({ APP_VARIANT: value })).rejects.toThrow(
    "APP_VARIANT must be development, preview, or production.",
  );
});

it.each([
  [undefined, "com.t3tools.t3code"],
  ["development", "com.t3tools.t3code.dev"],
  ["preview", "com.t3tools.t3code.preview"],
  ["production", "com.t3tools.t3code"],
])("keeps the iOS and Android identifiers for APP_VARIANT=%j", async (value, identifier) => {
  const config = await loadConfig({ APP_VARIANT: value });
  expect(config.ios?.bundleIdentifier).toBe(identifier);
  expect(config.android?.package).toBe(identifier);
});
