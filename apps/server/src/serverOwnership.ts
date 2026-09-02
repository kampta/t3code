import * as NodeCrypto from "node:crypto";

import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as DateTime from "effect/DateTime";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import type { PlatformError } from "effect/PlatformError";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";

import { getProcessStartIdentity, isProcessAlive } from "./serverRuntimeState.ts";

const LOCK_FILE_NAME = "server-owner.lock";
const LEASE_FILE_NAME = "server-owner.lease.sqlite";
const ownershipLease = Symbol("t3/serverOwnershipLease");

interface OwnershipLeaseDatabase {
  readonly exec: (sql: string) => unknown;
  readonly close: () => void;
}

export const ServerOwnershipRecord = Schema.Struct({
  version: Schema.Literal(1),
  // Present on records whose owner also holds the SQLite lifetime lease.
  // Older JSON-only records omit it and require conservative pid inspection.
  leaseVersion: Schema.optional(Schema.Literal(1)),
  token: Schema.String,
  pid: Schema.Int.check(Schema.isGreaterThan(0)),
  processStartIdentity: Schema.optional(Schema.String),
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
    ownerPid: Schema.optional(Schema.Int.check(Schema.isGreaterThan(0))),
    ownerStartedAt: Schema.optional(Schema.String),
  },
) {
  override get message(): string {
    return this.ownerPid === undefined
      ? `T3 Code is already running for '${this.stateDir}'.`
      : `T3 Code is already running for '${this.stateDir}' (PID ${this.ownerPid}, started ${this.ownerStartedAt ?? "unknown"}).`;
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
  readonly [ownershipLease]: OwnershipLeaseDatabase;
}

export interface ServerOwnershipOptions {
  readonly isProcessAlive?: (pid: number) => Effect.Effect<boolean>;
  readonly getProcessStartIdentity?: (pid: number) => Effect.Effect<string | undefined>;
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
  Effect.sync(() => isProcessAlive(pid));

const defaultGetProcessStartIdentity = Effect.fn("getServerProcessStartIdentity")(function* (
  pid: number,
) {
  const platform = yield* HostProcessPlatform;
  return getProcessStartIdentity(pid, platform);
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

const isSqliteBusy = (cause: unknown): boolean =>
  (Predicate.hasProperty(cause, "errcode") && (cause.errcode === 5 || cause.errcode === 6)) ||
  (Predicate.hasProperty(cause, "errno") && (cause.errno === 5 || cause.errno === 6)) ||
  (Predicate.hasProperty(cause, "code") &&
    (cause.code === "SQLITE_BUSY" || cause.code === "SQLITE_LOCKED"));

type OwnershipLeaseAttempt =
  | { readonly _tag: "Acquired"; readonly database: OwnershipLeaseDatabase }
  | { readonly _tag: "Busy" };

/**
 * SQLite owns the actual exclusion guarantee. An exclusive transaction is
 * released by the OS when a process exits, so stale recovery needs no unlink
 * race and cannot recursively require another reclaimable filesystem lock.
 */
const tryAcquireOwnershipLease = (input: {
  readonly stateDir: string;
  readonly lockPath: string;
  readonly leasePath: string;
}): Effect.Effect<OwnershipLeaseAttempt, ServerOwnershipStateError> =>
  Effect.tryPromise({
    try: async () => {
      let database: OwnershipLeaseDatabase | undefined;
      try {
        database =
          typeof Bun === "undefined"
            ? new (await import("node:sqlite")).DatabaseSync(input.leasePath)
            : new (await import("bun:sqlite")).Database(input.leasePath, { create: true });
        database.exec("PRAGMA busy_timeout = 0; BEGIN EXCLUSIVE");
        return { _tag: "Acquired", database } as const;
      } catch (cause) {
        try {
          database?.close();
        } catch {
          // Preserve the error that explains why acquisition failed.
        }
        if (isSqliteBusy(cause)) {
          return { _tag: "Busy" } as const;
        }
        throw cause;
      }
    },
    catch: (cause) =>
      new ServerOwnershipStateError({
        operation: "acquire",
        stateDir: input.stateDir,
        lockPath: input.lockPath,
        cause,
      }),
  });

const closeOwnershipLease = (input: {
  readonly stateDir: string;
  readonly lockPath: string;
  readonly database: OwnershipLeaseDatabase;
}): Effect.Effect<void, ServerOwnershipStateError> =>
  Effect.try({
    try: () => {
      try {
        input.database.exec("ROLLBACK");
      } finally {
        input.database.close();
      }
    },
    catch: (cause) =>
      new ServerOwnershipStateError({
        operation: "release",
        stateDir: input.stateDir,
        lockPath: input.lockPath,
        cause,
      }),
  });

const isOwnershipRecordAlive = Effect.fn("isServerOwnershipRecordAlive")(function* (
  record: ServerOwnershipRecord,
  options?: ServerOwnershipOptions,
) {
  const checkAlive = options?.isProcessAlive ?? defaultIsProcessAlive;
  if (!(yield* checkAlive(record.pid))) {
    return false;
  }
  if (record.processStartIdentity === undefined) {
    // Version-1 records written before process identities existed remain safe:
    // prefer a false conflict over starting beside a potentially live server.
    return true;
  }
  const getIdentity = options?.getProcessStartIdentity ?? defaultGetProcessStartIdentity;
  const currentIdentity = yield* getIdentity(record.pid);
  return currentIdentity === undefined || currentIdentity === record.processStartIdentity;
});

const conflictForRecord = (
  stateDir: string,
  lockPath: string,
  record?: ServerOwnershipRecord,
): ServerOwnershipConflictError =>
  new ServerOwnershipConflictError({
    stateDir,
    lockPath,
    ...(record === undefined ? {} : { ownerPid: record.pid, ownerStartedAt: record.startedAt }),
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
  const leasePath = path.join(canonicalStateDir, LEASE_FILE_NAME);
  const getIdentity = options?.getProcessStartIdentity ?? defaultGetProcessStartIdentity;
  const processStartIdentity = yield* getIdentity(process.pid);
  const record: ServerOwnershipRecord = {
    version: 1,
    leaseVersion: 1,
    token: NodeCrypto.randomUUID(),
    pid: process.pid,
    ...(processStartIdentity === undefined ? {} : { processStartIdentity }),
    startedAt: DateTime.formatIso(yield* DateTime.now),
  };
  const leaseAttempt = yield* tryAcquireOwnershipLease({
    stateDir: canonicalStateDir,
    lockPath,
    leasePath,
  });
  if (leaseAttempt._tag === "Busy") {
    const existing = yield* readOwnershipRecord({
      stateDir: canonicalStateDir,
      lockPath,
    }).pipe(Effect.catchTag("ServerOwnershipStateError", () => Effect.succeed(Option.none())));
    return yield* conflictForRecord(canonicalStateDir, lockPath, Option.getOrUndefined(existing));
  }

  const database = leaseAttempt.database;
  return yield* Effect.gen(function* () {
    const existing = yield* readOwnershipRecord({
      stateDir: canonicalStateDir,
      lockPath,
    });
    if (
      Option.isSome(existing) &&
      existing.value.leaseVersion === undefined &&
      (yield* isOwnershipRecordAlive(existing.value, options))
    ) {
      // A live version deployed before the SQLite lease was introduced still
      // owns this state directory. Preserve that migration boundary.
      return yield* conflictForRecord(canonicalStateDir, lockPath, existing.value);
    }

    if (Option.isSome(existing)) {
      yield* fs.remove(lockPath, { force: true }).pipe(
        Effect.mapError(
          (cause) =>
            new ServerOwnershipStateError({
              operation: "reclaim",
              stateDir: canonicalStateDir,
              lockPath,
              cause,
            }),
        ),
      );
    }

    if (!(yield* createOwnershipRecord({ stateDir: canonicalStateDir, lockPath, record }))) {
      const replacement = yield* readOwnershipRecord({ stateDir: canonicalStateDir, lockPath });
      return yield* conflictForRecord(
        canonicalStateDir,
        lockPath,
        Option.getOrUndefined(replacement),
      );
    }

    return {
      stateDir: canonicalStateDir,
      lockPath,
      record,
      [ownershipLease]: database,
    } satisfies ServerOwnership;
  }).pipe(
    Effect.onExit((exit) =>
      Exit.isFailure(exit)
        ? closeOwnershipLease({ stateDir: canonicalStateDir, lockPath, database }).pipe(
            Effect.ignore,
          )
        : Effect.void,
    ),
  );
});

const release = Effect.fn("releaseServerOwnership")(function* (ownership: ServerOwnership) {
  yield* Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const existingOption = yield* readOwnershipRecord({
      stateDir: ownership.stateDir,
      lockPath: ownership.lockPath,
    });
    if (Option.isNone(existingOption) || existingOption.value.token !== ownership.record.token) {
      return;
    }

    yield* fs.remove(ownership.lockPath, { force: true }).pipe(
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
  }).pipe(
    Effect.ensuring(
      closeOwnershipLease({
        stateDir: ownership.stateDir,
        lockPath: ownership.lockPath,
        database: ownership[ownershipLease],
      }).pipe(Effect.orDie),
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
