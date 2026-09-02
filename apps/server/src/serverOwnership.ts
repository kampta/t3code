import * as NodeCrypto from "node:crypto";

import * as DateTime from "effect/DateTime";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import type { PlatformError } from "effect/PlatformError";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";

const LOCK_FILE_NAME = "server-owner.lock";

export const ServerOwnershipRecord = Schema.Struct({
  version: Schema.Literal(1),
  token: Schema.String,
  pid: Schema.Int.check(Schema.isGreaterThan(0)),
  startedAt: Schema.String,
});
export type ServerOwnershipRecord = typeof ServerOwnershipRecord.Type;

const decodeServerOwnershipRecord = Schema.decodeUnknownEffect(
  Schema.fromJsonString(ServerOwnershipRecord),
);
const encodeServerOwnershipRecord = Schema.encodeEffect(
  Schema.fromJsonString(ServerOwnershipRecord),
);

export class ServerOwnershipConflictError extends Schema.TaggedErrorClass<ServerOwnershipConflictError>()(
  "ServerOwnershipConflictError",
  {
    stateDir: Schema.String,
    lockPath: Schema.String,
    ownerPid: Schema.Int.check(Schema.isGreaterThan(0)),
    ownerStartedAt: Schema.String,
  },
) {
  override get message(): string {
    return `T3 Code is already running for '${this.stateDir}' (PID ${this.ownerPid}, started ${this.ownerStartedAt}).`;
  }
}

export class ServerOwnershipStateError extends Schema.TaggedErrorClass<ServerOwnershipStateError>()(
  "ServerOwnershipStateError",
  {
    operation: Schema.Literals(["canonicalize", "acquire", "inspect", "reclaim", "release"]),
    stateDir: Schema.String,
    lockPath: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to ${this.operation} T3 Code server ownership for '${this.stateDir}'.`;
  }
}

export interface ServerOwnership {
  readonly stateDir: string;
  readonly lockPath: string;
  readonly record: ServerOwnershipRecord;
}

export interface ServerOwnershipOptions {
  readonly isProcessAlive?: (pid: number) => Effect.Effect<boolean>;
}

export class ServerOwnershipToken extends Context.Reference<string | undefined>(
  "t3/serverOwnershipToken",
  { defaultValue: () => undefined },
) {}

export const serverOwnershipLockPath = (
  stateDir: string,
): Effect.Effect<string, never, Path.Path> =>
  Effect.map(Path.Path, (path) => path.join(stateDir, LOCK_FILE_NAME));

const defaultIsProcessAlive = (pid: number): Effect.Effect<boolean> =>
  Effect.sync(() => {
    try {
      process.kill(pid, 0);
      return true;
    } catch (error) {
      return !(
        Predicate.hasProperty(error, "code") &&
        (error.code === "ESRCH" || error.code === "EINVAL")
      );
    }
  });

const isAlreadyExists = (error: PlatformError): boolean => error.reason._tag === "AlreadyExists";

const isNotFound = (error: PlatformError): boolean => error.reason._tag === "NotFound";

const readOwnershipRecord = Effect.fn("readServerOwnershipRecord")(function* (input: {
  readonly stateDir: string;
  readonly lockPath: string;
}) {
  const fs = yield* FileSystem.FileSystem;
  const raw = yield* fs.readFileString(input.lockPath).pipe(
    Effect.map(Option.some),
    Effect.catchIf(isNotFound, () => Effect.succeed(Option.none<string>())),
    Effect.mapError(
      (cause) =>
        new ServerOwnershipStateError({
          operation: "inspect",
          stateDir: input.stateDir,
          lockPath: input.lockPath,
          cause,
        }),
    ),
  );
  if (Option.isNone(raw)) {
    return Option.none<ServerOwnershipRecord>();
  }
  return yield* decodeServerOwnershipRecord(raw.value).pipe(
    Effect.map(Option.some),
    Effect.mapError(
      (cause) =>
        new ServerOwnershipStateError({
          operation: "inspect",
          stateDir: input.stateDir,
          lockPath: input.lockPath,
          cause,
        }),
    ),
  );
});

const createOwnershipRecord = Effect.fn("createServerOwnershipRecord")(function* (input: {
  readonly stateDir: string;
  readonly lockPath: string;
  readonly record: ServerOwnershipRecord;
}) {
  const fs = yield* FileSystem.FileSystem;
  const encoded = yield* encodeServerOwnershipRecord(input.record);
  const contents = new TextEncoder().encode(`${encoded}\n`);
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const file = yield* fs.open(input.lockPath, { flag: "wx", mode: 0o600 });
      yield* file.writeAll(contents);
      yield* file.sync;
      return true;
    }),
  ).pipe(
    Effect.catchIf(isAlreadyExists, () => Effect.succeed(false)),
    Effect.tapError(() => fs.remove(input.lockPath, { force: true }).pipe(Effect.ignore)),
    Effect.mapError(
      (cause) =>
        new ServerOwnershipStateError({
          operation: "acquire",
          stateDir: input.stateDir,
          lockPath: input.lockPath,
          cause,
        }),
    ),
  );
});

const reclaimStaleOwnership = Effect.fn("reclaimStaleServerOwnership")(function* (input: {
  readonly stateDir: string;
  readonly lockPath: string;
  readonly expected: ServerOwnershipRecord;
  readonly token: string;
}) {
  const fs = yield* FileSystem.FileSystem;
  const stalePath = `${input.lockPath}.stale-${input.token}`;
  const moved = yield* fs.rename(input.lockPath, stalePath).pipe(
    Effect.as(true),
    Effect.catchIf(isNotFound, () => Effect.succeed(false)),
    Effect.mapError(
      (cause) =>
        new ServerOwnershipStateError({
          operation: "reclaim",
          stateDir: input.stateDir,
          lockPath: input.lockPath,
          cause,
        }),
    ),
  );
  if (!moved) {
    return false;
  }

  const movedRecord = yield* fs.readFileString(stalePath).pipe(
    Effect.flatMap(decodeServerOwnershipRecord),
    Effect.mapError(
      (cause) =>
        new ServerOwnershipStateError({
          operation: "reclaim",
          stateDir: input.stateDir,
          lockPath: input.lockPath,
          cause,
        }),
    ),
  );
  if (movedRecord.token !== input.expected.token) {
    const restored = yield* fs.link(stalePath, input.lockPath).pipe(
      Effect.as(true),
      Effect.catchIf(isAlreadyExists, () => Effect.succeed(false)),
      Effect.mapError(
        (cause) =>
          new ServerOwnershipStateError({
            operation: "reclaim",
            stateDir: input.stateDir,
            lockPath: input.lockPath,
            cause,
          }),
      ),
    );
    if (!restored) {
      return yield* new ServerOwnershipStateError({
        operation: "reclaim",
        stateDir: input.stateDir,
        lockPath: input.lockPath,
        cause: new Error("Server ownership changed while a stale record was being reclaimed."),
      });
    }
    yield* fs.remove(stalePath, { force: true }).pipe(
      Effect.mapError(
        (cause) =>
          new ServerOwnershipStateError({
            operation: "reclaim",
            stateDir: input.stateDir,
            lockPath: input.lockPath,
            cause,
          }),
      ),
    );
    return false;
  }

  yield* fs.remove(stalePath, { force: true }).pipe(
    Effect.mapError(
      (cause) =>
        new ServerOwnershipStateError({
          operation: "reclaim",
          stateDir: input.stateDir,
          lockPath: input.lockPath,
          cause,
        }),
    ),
  );
  return true;
});

const acquire = Effect.fn("acquireServerOwnership")(function* (
  stateDir: string,
  options?: ServerOwnershipOptions,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const canonicalStateDir = yield* fs.realPath(path.resolve(stateDir)).pipe(
    Effect.mapError(
      (cause) =>
        new ServerOwnershipStateError({
          operation: "canonicalize",
          stateDir,
          lockPath: path.join(stateDir, LOCK_FILE_NAME),
          cause,
        }),
    ),
  );
  const lockPath = path.join(canonicalStateDir, LOCK_FILE_NAME);
  const record: ServerOwnershipRecord = {
    version: 1,
    token: NodeCrypto.randomUUID(),
    pid: process.pid,
    startedAt: DateTime.formatIso(yield* DateTime.now),
  };
  const isProcessAlive = options?.isProcessAlive ?? defaultIsProcessAlive;

  while (true) {
    if (yield* createOwnershipRecord({ stateDir: canonicalStateDir, lockPath, record })) {
      return { stateDir: canonicalStateDir, lockPath, record } satisfies ServerOwnership;
    }

    const existingOption = yield* readOwnershipRecord({
      stateDir: canonicalStateDir,
      lockPath,
    });
    if (Option.isNone(existingOption)) {
      continue;
    }
    const existing = existingOption.value;
    if (yield* isProcessAlive(existing.pid)) {
      return yield* new ServerOwnershipConflictError({
        stateDir: canonicalStateDir,
        lockPath,
        ownerPid: existing.pid,
        ownerStartedAt: existing.startedAt,
      });
    }

    yield* reclaimStaleOwnership({
      stateDir: canonicalStateDir,
      lockPath,
      expected: existing,
      token: record.token,
    });
  }
});

const release = Effect.fn("releaseServerOwnership")(function* (ownership: ServerOwnership) {
  const fs = yield* FileSystem.FileSystem;
  const existingOption = yield* readOwnershipRecord({
    stateDir: ownership.stateDir,
    lockPath: ownership.lockPath,
  });
  if (Option.isNone(existingOption) || existingOption.value.token !== ownership.record.token) {
    return;
  }

  const releasePath = `${ownership.lockPath}.release-${ownership.record.token}`;
  yield* fs.rename(ownership.lockPath, releasePath).pipe(
    Effect.catchIf(isNotFound, () => Effect.void),
    Effect.mapError(
      (cause) =>
        new ServerOwnershipStateError({
          operation: "release",
          stateDir: ownership.stateDir,
          lockPath: ownership.lockPath,
          cause,
        }),
    ),
  );
  yield* fs.remove(releasePath, { force: true }).pipe(
    Effect.mapError(
      (cause) =>
        new ServerOwnershipStateError({
          operation: "release",
          stateDir: ownership.stateDir,
          lockPath: ownership.lockPath,
          cause,
        }),
    ),
  );
});

export const acquireServerOwnership = (stateDir: string, options?: ServerOwnershipOptions) =>
  Effect.acquireRelease(acquire(stateDir, options), (ownership) =>
    release(ownership).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("Failed to release T3 Code server ownership", {
          stateDir: ownership.stateDir,
          lockPath: ownership.lockPath,
          cause,
        }),
      ),
    ),
  );
