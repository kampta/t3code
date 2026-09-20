import * as NodeModule from "node:module";
import { describe, expect, it } from "vite-plus/test";

const require = NodeModule.createRequire(import.meta.url);
const { stripUnsupportedEntitlements } = require("./withoutIosPersonalTeamCapabilities.cjs") as {
  readonly stripUnsupportedEntitlements: (
    entitlements: Record<string, unknown>,
  ) => Record<string, unknown>;
};

describe("Personal Team iOS entitlements", () => {
  it("removes unsupported capabilities and preserves unrelated entitlements", () => {
    const entitlements = {
      "aps-environment": "production",
      "com.apple.developer.applesignin": ["Default"],
      "com.apple.developer.associated-domains": ["applinks:clerk.t3.codes"],
      "com.apple.security.application-groups": ["group.com.example.t3code"],
      "com.apple.developer.networking.wifi-info": true,
    };

    expect(stripUnsupportedEntitlements(entitlements)).toEqual({
      "com.apple.developer.networking.wifi-info": true,
    });
  });
});
