import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import {
  acquireServerOwnership,
  ServerOwnershipConflictError,
  ServerOwnershipRecord,
  serverOwnershipLockPath,
} from "./serverOwnership.ts";

const isServerOwnershipConflictError = Schema.is(ServerOwnershipConflictError);
const decodeRecord = Schema.decodeUnknownEffect(Schema.fromJsonString(ServerOwnershipRecord));
const encodeRecord = Schema.encodeEffect(Schema.fromJsonString(ServerOwnershipRecord));

describe("server ownership", () => {
  it.effect("fails fast while a live process owns the canonical state directory", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const stateDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-server-owner-live-" });

      yield* Effect.scoped(
        Effect.gen(function* () {
          const owner = yield* acquireServerOwnership(stateDir);
          const failure = yield* acquireServerOwnership(stateDir).pipe(Effect.flip);

          assert.isTrue(isServerOwnershipConflictError(failure));
          if (isServerOwnershipConflictError(failure)) {
            assert.equal(failure.ownerPid, process.pid);
            assert.equal(failure.stateDir, owner.stateDir);
            assert.equal(failure.lockPath, owner.lockPath);
          }
        }),
      );

      yield* Effect.scoped(
        Effect.gen(function* () {
          const next = yield* acquireServerOwnership(stateDir);
          assert.equal(next.record.pid, process.pid);
        }),
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("uses the real state directory as the ownership key", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-server-owner-realpath-" });
      const stateDir = path.join(root, "state");
      const linkedStateDir = path.join(root, "linked-state");
      yield* fs.makeDirectory(stateDir);
      yield* fs.symlink(stateDir, linkedStateDir);

      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* acquireServerOwnership(stateDir);
          const failure = yield* acquireServerOwnership(linkedStateDir).pipe(Effect.flip);
          assert.isTrue(isServerOwnershipConflictError(failure));
        }),
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("reclaims a lock whose recorded process is no longer alive", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const stateDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-server-owner-stale-" });
      const canonicalStateDir = yield* fs.realPath(stateDir);
      const lockPath = yield* serverOwnershipLockPath(canonicalStateDir);
      yield* encodeRecord({
        version: 1,
        token: "stale-token",
        pid: 12345,
        startedAt: "2026-01-01T00:00:00.000Z",
      }).pipe(Effect.flatMap((record) => fs.writeFileString(lockPath, `${record}\n`)));

      yield* Effect.scoped(
        Effect.gen(function* () {
          const owner = yield* acquireServerOwnership(stateDir, {
            isProcessAlive: () => Effect.succeed(false),
          });
          assert.notEqual(owner.record.token, "stale-token");
          const persisted = yield* fs.readFileString(lockPath).pipe(Effect.flatMap(decodeRecord));
          assert.equal(persisted.token, owner.record.token);
        }),
      );

      assert.isFalse(yield* fs.exists(lockPath));
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("does not remove a lock record that no longer carries its token", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const stateDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-server-owner-token-" });
      let lockPath = "";

      yield* Effect.scoped(
        Effect.gen(function* () {
          const owner = yield* acquireServerOwnership(stateDir);
          lockPath = owner.lockPath;
          yield* encodeRecord({
            ...owner.record,
            token: "replacement-token",
          }).pipe(Effect.flatMap((record) => fs.writeFileString(lockPath, `${record}\n`)));
        }),
      );

      assert.isTrue(yield* fs.exists(lockPath));
      const persisted = yield* fs.readFileString(lockPath).pipe(Effect.flatMap(decodeRecord));
      assert.equal(persisted.token, "replacement-token");
      yield* fs.remove(lockPath);
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
