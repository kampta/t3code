import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
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

  it.effect("reclaims a truncated lock after acquiring the ownership lease", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const stateDir = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-server-owner-truncated-",
      });
      const canonicalStateDir = yield* fs.realPath(stateDir);
      const lockPath = yield* serverOwnershipLockPath(canonicalStateDir);
      yield* fs.writeFileString(
        lockPath,
        '{"version":1,"leaseVersion":1,"token":"truncated-owner"',
      );

      yield* Effect.scoped(
        Effect.gen(function* () {
          const owner = yield* acquireServerOwnership(stateDir);
          const persisted = yield* fs.readFileString(lockPath).pipe(Effect.flatMap(decodeRecord));
          assert.equal(persisted.token, owner.record.token);
          assert.equal(persisted.leaseVersion, 1);
        }),
      );

      assert.isFalse(yield* fs.exists(lockPath));
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps a truncated lock conservative while the ownership lease is busy", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const stateDir = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-server-owner-busy-truncated-",
      });
      const canonicalStateDir = yield* fs.realPath(stateDir);
      const lockPath = yield* serverOwnershipLockPath(canonicalStateDir);
      const truncated = '{"version":1,"leaseVersion":1,"token":"truncated-owner"';

      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* acquireServerOwnership(stateDir);
          yield* fs.writeFileString(lockPath, truncated);

          const failure = yield* Effect.scoped(acquireServerOwnership(stateDir)).pipe(Effect.flip);
          assert.isTrue(isServerOwnershipConflictError(failure));
          if (isServerOwnershipConflictError(failure)) {
            assert.isUndefined(failure.ownerPid);
          }
          assert.equal(yield* fs.readFileString(lockPath), truncated);
        }),
      );

      assert.equal(yield* fs.readFileString(lockPath), truncated);
      yield* Effect.scoped(acquireServerOwnership(stateDir));
      assert.isFalse(yield* fs.exists(lockPath));
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("serializes three concurrent reclaimers behind one ownership lease", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const stateDir = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-server-owner-concurrent-reclaim-",
      });
      const canonicalStateDir = yield* fs.realPath(stateDir);
      const lockPath = yield* serverOwnershipLockPath(canonicalStateDir);
      yield* encodeRecord({
        version: 1,
        token: "stale-token",
        pid: 12345,
        processStartIdentity: "old-process",
        startedAt: "2026-01-01T00:00:00.000Z",
      }).pipe(Effect.flatMap((record) => fs.writeFileString(lockPath, `${record}\n`)));

      const ready = yield* Ref.make(0);
      const start = yield* Deferred.make<void>();
      const failed = yield* Ref.make(0);
      const contendersFinished = yield* Deferred.make<void>();
      const releaseOwner = yield* Deferred.make<void>();
      const attempt = Effect.gen(function* () {
        const readyCount = yield* Ref.updateAndGet(ready, (count) => count + 1);
        if (readyCount === 3) {
          yield* Deferred.succeed(start, undefined);
        }
        yield* Deferred.await(start);
        return yield* Effect.scoped(
          Effect.gen(function* () {
            const owner = yield* acquireServerOwnership(stateDir, {
              isProcessAlive: () => Effect.succeed(false),
              getProcessStartIdentity: () => Effect.succeed("current-process"),
            });
            yield* Deferred.await(releaseOwner);
            return { _tag: "Owner", token: owner.record.token } as const;
          }),
        );
      }).pipe(
        Effect.catch((error) =>
          Effect.gen(function* () {
            const failureCount = yield* Ref.updateAndGet(failed, (count) => count + 1);
            if (failureCount === 2) {
              yield* Deferred.succeed(contendersFinished, undefined);
            }
            return { _tag: "Failure", error } as const;
          }),
        ),
      );

      const fibers = yield* Effect.all(
        Array.from({ length: 3 }, () => attempt.pipe(Effect.forkChild)),
      );
      yield* Deferred.await(contendersFinished);
      yield* Deferred.succeed(releaseOwner, undefined);
      const attempts = yield* Effect.all(fibers.map(Fiber.join));

      assert.equal(attempts.filter((attempt) => attempt._tag === "Owner").length, 1);
      assert.equal(
        attempts.filter(
          (attempt) => attempt._tag === "Failure" && isServerOwnershipConflictError(attempt.error),
        ).length,
        2,
      );
      assert.isFalse(yield* fs.exists(lockPath));

      yield* Effect.scoped(acquireServerOwnership(stateDir));
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("reclaims a live pid when its process-start identity was reused", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const stateDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-server-owner-reused-pid-" });
      const canonicalStateDir = yield* fs.realPath(stateDir);
      const lockPath = yield* serverOwnershipLockPath(canonicalStateDir);
      yield* encodeRecord({
        version: 1,
        token: "reused-pid-token",
        pid: process.pid,
        processStartIdentity: "previous-process-start",
        startedAt: "2026-01-01T00:00:00.000Z",
      }).pipe(Effect.flatMap((record) => fs.writeFileString(lockPath, `${record}\n`)));

      yield* Effect.scoped(
        Effect.gen(function* () {
          const owner = yield* acquireServerOwnership(stateDir, {
            isProcessAlive: () => Effect.succeed(true),
            getProcessStartIdentity: () => Effect.succeed("current-process-start"),
          });
          assert.notEqual(owner.record.token, "reused-pid-token");
          assert.equal(owner.record.processStartIdentity, "current-process-start");
          const persisted = yield* fs.readFileString(lockPath).pipe(Effect.flatMap(decodeRecord));
          assert.equal(persisted.token, owner.record.token);
          assert.equal(persisted.processStartIdentity, "current-process-start");
        }),
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("reclaims a lease-backed record when process identity is unavailable on Windows", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const stateDir = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-server-owner-lease-backed-reclaim-",
      });
      const canonicalStateDir = yield* fs.realPath(stateDir);
      const lockPath = yield* serverOwnershipLockPath(canonicalStateDir);
      yield* encodeRecord({
        version: 1,
        leaseVersion: 1,
        token: "crashed-lease-owner",
        pid: process.pid,
        startedAt: "2026-01-01T00:00:00.000Z",
      }).pipe(Effect.flatMap((record) => fs.writeFileString(lockPath, `${record}\n`)));

      yield* Effect.scoped(
        Effect.gen(function* () {
          const owner = yield* acquireServerOwnership(stateDir, {
            isProcessAlive: () => Effect.succeed(true),
            getProcessStartIdentity: () => Effect.sync((): string | undefined => undefined),
          });
          assert.notEqual(owner.record.token, "crashed-lease-owner");
          assert.equal(owner.record.leaseVersion, 1);
        }),
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("preserves a live legacy JSON-only owner", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const stateDir = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-server-owner-live-legacy-",
      });
      const canonicalStateDir = yield* fs.realPath(stateDir);
      const lockPath = yield* serverOwnershipLockPath(canonicalStateDir);
      yield* encodeRecord({
        version: 1,
        token: "live-legacy-owner",
        pid: process.pid,
        startedAt: "2026-01-01T00:00:00.000Z",
      }).pipe(Effect.flatMap((record) => fs.writeFileString(lockPath, `${record}\n`)));

      const failure = yield* Effect.scoped(
        acquireServerOwnership(stateDir, {
          isProcessAlive: () => Effect.succeed(true),
          getProcessStartIdentity: () => Effect.sync((): string | undefined => undefined),
        }),
      ).pipe(Effect.flip);
      assert.isTrue(isServerOwnershipConflictError(failure));

      const persisted = yield* fs.readFileString(lockPath).pipe(Effect.flatMap(decodeRecord));
      assert.equal(persisted.token, "live-legacy-owner");
      assert.isUndefined(persisted.leaseVersion);
      yield* fs.remove(lockPath);
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
