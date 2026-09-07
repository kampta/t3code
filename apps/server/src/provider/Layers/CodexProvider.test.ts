import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { CodexSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";

import { writeFakeCli } from "../../testUtils/fakeCli.ts";
import {
  applyPreferredCodexDefaultModel,
  checkCodexProviderStatus,
  mapCodexModelCapabilities,
} from "./CodexProvider.ts";

const decodeCodexSettings = Schema.decodeSync(CodexSettings);

it.live("degrades stalled model pagination but surfaces protocol failures", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-codex-inventory-" });
    const binaryPath = writeFakeCli({
      directory,
      name: "codex-inventory",
      source: [
        'import { createInterface } from "node:readline";',
        'createInterface({ input: process.stdin }).on("line", (line) => {',
        "  const request = JSON.parse(line);",
        "  if (request.id === undefined) return;",
        '  if (request.method === "model/list" && request.params?.cursor === "next" && !process.env.T3_TEST_MODEL_ERROR) return;',
        "  const responses = {",
        '    initialize: { userAgent: "codex/1.0.0", codexHome: process.cwd(), platformFamily: "unix", platformOs: "darwin" },',
        '    "account/read": { account: { type: "apiKey" }, requiresOpenaiAuth: false },',
        '    "skills/list": { data: [{ cwd: process.cwd(), errors: [], skills: [] }] },',
        '    "model/list": { data: [{ id: "partial", model: "partial", displayName: "Partial", description: "Partial page", hidden: false, isDefault: true, defaultReasoningEffort: "medium", supportedReasoningEfforts: [], defaultServiceTier: null, serviceTiers: [], additionalSpeedTiers: [] }], nextCursor: "next" },',
        "  };",
        '  const result = request.method === "model/list" && request.params?.cursor === "next" ? undefined : responses[request.method];',
        '  const response = result === undefined ? { id: request.id, error: { code: -32601, message: "Not found" } } : { id: request.id, result };',
        '  process.stdout.write(JSON.stringify(response) + "\\n");',
        "});",
      ].join("\n"),
    });

    const settings = decodeCodexSettings({ enabled: true, binaryPath });
    const snapshot = yield* checkCodexProviderStatus(settings, undefined, {
      ...process.env,
      T3_TEST_MODEL_ERROR: "",
    });

    assert.strictEqual(snapshot.status, "warning");
    assert.strictEqual(snapshot.auth.status, "authenticated");
    assert.strictEqual(snapshot.inventory?.models, "stale");
    assert.strictEqual(snapshot.inventory?.skills, "authoritative");
    assert.deepStrictEqual(snapshot.models, []);
    assert.match(snapshot.message ?? "", /model discovery did not complete/);

    const incompatible = yield* checkCodexProviderStatus(settings, undefined, {
      ...process.env,
      T3_TEST_MODEL_ERROR: "1",
    });
    assert.strictEqual(incompatible.status, "error");
    assert.strictEqual(incompatible.installed, true);
    assert.match(incompatible.message ?? "", /Not found/);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it("maps current Codex model capability fields", () => {
  const capabilities = mapCodexModelCapabilities({
    additionalSpeedTiers: [],
    defaultReasoningEffort: "super-high",
    description: "Test model",
    displayName: "GPT Test",
    hidden: false,
    id: "gpt-test",
    isDefault: true,
    model: "gpt-test",
    defaultServiceTier: "flex",
    serviceTiers: [
      {
        id: "priority",
        name: "Fast",
        description: "Lower latency responses.",
      },
      {
        id: "flex",
        name: "Flex",
        description: "Lower-cost asynchronous routing.",
      },
    ],
    supportedReasoningEfforts: [
      {
        description: "Maximum reasoning",
        reasoningEffort: "super-high",
      },
    ],
  });

  assert.deepStrictEqual(capabilities.optionDescriptors, [
    {
      id: "reasoningEffort",
      label: "Reasoning",
      type: "select",
      options: [{ id: "super-high", label: "super-high", isDefault: true }],
      currentValue: "super-high",
    },
    {
      id: "serviceTier",
      label: "Service Tier",
      type: "select",
      options: [
        { id: "default", label: "Standard" },
        {
          id: "priority",
          label: "Fast",
          description: "Lower latency responses.",
        },
        {
          id: "flex",
          label: "Flex",
          description: "Lower-cost asynchronous routing.",
          isDefault: true,
        },
      ],
      currentValue: "flex",
    },
  ]);
});

it("uses standard routing when the catalog has no default service tier", () => {
  const capabilities = mapCodexModelCapabilities({
    additionalSpeedTiers: ["fast"],
    defaultReasoningEffort: "medium",
    defaultServiceTier: null,
    description: "Test model",
    displayName: "GPT Test",
    hidden: false,
    id: "gpt-test",
    isDefault: true,
    model: "gpt-test",
    serviceTiers: [
      {
        id: "priority",
        name: "Fast",
        description: "1.5x speed, increased usage",
      },
    ],
    supportedReasoningEfforts: [],
  });

  assert.deepStrictEqual(capabilities.optionDescriptors, [
    {
      id: "serviceTier",
      label: "Service Tier",
      type: "select",
      options: [
        { id: "default", label: "Standard", isDefault: true },
        {
          id: "priority",
          label: "Fast",
          description: "1.5x speed, increased usage",
        },
      ],
      currentValue: "default",
    },
  ]);
});

it("marks the most preferred available model as default", () => {
  const models = applyPreferredCodexDefaultModel([
    { slug: "gpt-5.6-terra", name: "GPT-5.6-Terra", isCustom: false, capabilities: null },
    { slug: "gpt-5.4", name: "GPT-5.4", isCustom: false, isDefault: true, capabilities: null },
  ]);

  assert.deepStrictEqual(
    models.map((model) => ({ slug: model.slug, isDefault: model.isDefault })),
    [
      { slug: "gpt-5.6-terra", isDefault: true },
      { slug: "gpt-5.4", isDefault: undefined },
    ],
  );
});

it("prefers sol over terra when both are available", () => {
  const models = applyPreferredCodexDefaultModel([
    { slug: "gpt-5.6-terra", name: "GPT-5.6-Terra", isCustom: false, capabilities: null },
    { slug: "gpt-5.6-sol", name: "GPT-5.6-Sol", isCustom: false, capabilities: null },
  ]);

  assert.deepStrictEqual(models.find((model) => model.isDefault)?.slug, "gpt-5.6-sol");
});

it("keeps Codex's own default when no preferred model is available", () => {
  const models = applyPreferredCodexDefaultModel([
    { slug: "gpt-5.5", name: "GPT-5.5", isCustom: false, capabilities: null },
    { slug: "gpt-5.4", name: "GPT-5.4", isCustom: false, isDefault: true, capabilities: null },
  ]);

  assert.deepStrictEqual(models.find((model) => model.isDefault)?.slug, "gpt-5.4");
});

it("ignores custom models that shadow a preferred slug", () => {
  const models = applyPreferredCodexDefaultModel([
    { slug: "gpt-5.6-sol", name: "gpt-5.6-sol", isCustom: true, capabilities: null },
    { slug: "gpt-5.4", name: "GPT-5.4", isCustom: false, isDefault: true, capabilities: null },
  ]);

  assert.deepStrictEqual(models.find((model) => model.isDefault)?.slug, "gpt-5.4");
});
