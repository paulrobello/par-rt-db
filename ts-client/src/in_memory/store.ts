/**
 * In-memory implementation of the par-rt-db client for unit tests — the client
 * core (mirrors `rust-client/src/in_memory/mod.rs`): stored rows, the
 * transaction executor, storage, and the public client/admin classes. The
 * query engine lives in `./query.ts`, the migration engine in
 * `./migrate.ts`, filter/value predicates in `./validate.ts`, doc
 * validation/stamping in `./doc_stamp.ts`, the schedule/workflow machinery in
 * `./workflow.ts`, presence in `./presence.ts`, and the admin surface in
 * `./admin_view.ts`; the public surface of the former `in_memory.ts` monolith
 * is re-exported from `./index.ts` (this module re-exports the names that
 * moved so existing `./store.js` imports keep resolving).
 *
 * The server (`server/src/{txn,query,schema,protocol}.rs`) is the source of
 * truth for the declarative DSL, step-result shapes, system fields, and query
 * semantics; this client mirrors them so app code can exercise query/txn/schema
 * behavior with no network and no live Postgres. It exposes the same data
 * surface as the live clients — `pushSchema`, `query` (one-shot, like
 * {@link RtDbHttpClient}), `mutate`/transactions (like {@link RtDbClient}), and
 * `subscribe` (reactive `queryUpdate`s) — so a test can swap it in behind a
 * shared interface.
 *
 * Parity is not aspirational: this engine executes every case in
 * `wire-corpus/semantics/` as one of the corpus runners, alongside the server
 * and the Rust, Python, and Swift engines, so a behavior difference fails the
 * corpus test rather than surfacing later as a silent divergence.
 */

import { canonical } from "../canonical.js";
import { RtDbError } from "../errors.js";
import type { FileMetadata, UploadInput, UploadResult } from "../http.js";
import { parseStepResults, type StepResult } from "../mutation.js";
import type {
  AuthedUser,
  DirectiveReportJson,
  FilterExpr,
  MigrateRequestJson,
  MigrateResultJson,
  PresenceMember,
  QueryJson,
  ScheduleInfo,
  ScheduleWhen,
  SchemaJson,
  TableJson,
  TransactionJson,
  WorkflowInfo,
  WorkflowInfoFull,
  WorkflowSpec,
  WorkflowStatus,
} from "../protocol.js";
import type { RtQuery } from "../query.js";
import type { AnySchema } from "../schema.js";
import { InMemoryAdminClient } from "./admin_view.js";
import {
  applyDefaults,
  applyPatch,
  stampComputed,
  stampTtlDefault,
  stampUpdatedAt,
  stripUnsetOptionals,
  validateComputed,
  validateConstraints,
  validateDoc,
} from "./doc_stamp.js";
import { checkFieldExpectations, validateFieldGuardShapes } from "./field-guard.js";
import {
  applyMigrationDirective,
  detectDestructiveChanges,
  grandfatherTrgm,
  onDeleteRef,
  validateOnDelete,
  validateSchema,
} from "./migrate.js";
import { PresenceMembership, PresenceRooms } from "./presence.js";
import { executeQuery, requireIndex, stripVersionForDiff } from "./query.js";
import {
  clone,
  coerceIndexValue,
  evalFilterExpr,
  isPlainObject,
  validateFilter,
} from "./validate.js";
import {
  claimDueWorkflows,
  countSteps,
  CRON_STEP_MS,
  dueAtFor,
  MAX_EVERY_MS,
  MAX_SIGNAL_PAYLOAD_BYTES,
  MAX_STEPS,
  type ScheduledJob,
  toScheduleInfo,
  toWorkflowInfo,
  validateWorkflowSpec,
  type WorkflowRun,
} from "./workflow.js";

// Re-exported so imports from "./store.js" (the ./index.ts façade above all)
// keep resolving after the QA-003b split moved these to their own modules.
export { InMemoryAdminClient } from "./admin_view.js";
export { PresenceRooms } from "./presence.js";
export { MAX_EVERY_MS, MAX_STEPS } from "./workflow.js";

/** Normalizes any {@link UploadInput} to `Uint8Array` for hashing/storage.
 *  The in-memory harness is not a real transport, so streaming inputs are
 *  read into memory in full (ENH-021). `Blob.arrayBuffer` handles `File` too
 *  (a `Blob` subtype). Throws `BAD_REQUEST` for any other shape — matching the
 *  http client's runtime guard. */
async function toUint8Array(body: UploadInput): Promise<Uint8Array> {
  if (body instanceof Uint8Array) {
    return body;
  }
  if (typeof Blob !== "undefined" && body instanceof Blob) {
    return new Uint8Array(await body.arrayBuffer());
  }
  if (body instanceof ReadableStream) {
    const reader = body.getReader();
    const chunks: Uint8Array[] = [];
    let total = 0;
    for (;;) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      if (value) {
        chunks.push(value);
        total += value.byteLength;
      }
    }
    const out = new Uint8Array(total);
    let offset = 0;
    for (const c of chunks) {
      out.set(c, offset);
      offset += c.byteLength;
    }
    return out;
  }
  if (body instanceof ArrayBuffer) {
    return new Uint8Array(body);
  }
  if (typeof body === "string") {
    return new TextEncoder().encode(body);
  }
  throw new RtDbError(
    "BAD_REQUEST",
    "upload body must be Uint8Array, Blob, ReadableStream, ArrayBuffer, or string",
  );
}

/** Hard cap on rows a single `patchByQuery`/`deleteByQuery` step may touch.
 * Mirrors server `txn::MAX_BY_QUERY_ROWS`; a larger match set is truncated. */
/** Step ops an `insertOnly` table rejects with SCHEMA_VIOLATION (everything that
 *  modifies or removes an existing row; `insert`/`insertMany` stay allowed). */
const INSERT_ONLY_BLOCKED_OPS: ReadonlySet<string> = new Set([
  "patch",
  "adjustCounter",
  "replace",
  "delete",
  "undelete",
  "upsert",
  "upsertMany",
  "patchByQuery",
  "deleteByQuery",
]);

const MAX_BY_QUERY_ROWS = 1000;

function checkBulkLen(op: string, len: number): void {
  if (len > MAX_BY_QUERY_ROWS) {
    throw new RtDbError(
      "BAD_REQUEST",
      `${op} has ${len} docs, exceeding the limit of ${MAX_BY_QUERY_ROWS}`,
    );
  }
}
/** SEC-104: hard cap on the count of `patchByQuery`/`deleteByQuery` steps in
 * one txn. Mirrors server `txn::MAX_BY_QUERY_STEPS_PER_TXN`. Without it, 1024
 * by-query steps × 1000 rows each could stall the (server's) single-writer on
 * roughly 1M rows; the cap bounds the worst case at 16 × 1000 = 16,000. */
export const MAX_BY_QUERY_STEPS_PER_TXN = 16;
/** SEC-104: hard ceiling on the worst-case total documents a single txn may
 * touch. Mirrors server `txn::MAX_AFFECTED_ROWS_PER_TXN`. Per-id steps count 1
 * each; each by-query step counts up to its `limit` (default `MAX_BY_QUERY_ROWS`). */
export const MAX_AFFECTED_ROWS_PER_TXN = 10_000;
/** FM-33: hard cap on the total rows one initiating delete (per-id `delete` or
 * one `deleteByQuery` step) may cascade through via `onDelete`. Mirrors server
 * `txn::MAX_CASCADE_ROWS`; a cascade past the budget surfaces as Conflict and
 * the whole txn rolls back. */
const MAX_CASCADE_ROWS = 10_000;

/** SEC-104: total documents a txn could touch in the worst case. Per-id steps
 * count 1 each; control-flow steps (`schedule`/`cancelSchedule` FM-28,
 * `startWorkflow`/`cancelWorkflow` FM-29) count 0 — they touch no documents;
 * each `patchByQuery`/`deleteByQuery` step counts up to its `limit` (default
 * and cap `MAX_BY_QUERY_ROWS`). Mirrors server `txn::worst_case_affected`. */
export function worstCaseAffected(txn: TransactionJson): number {
  let total = 0;
  for (const step of txn.steps) {
    if (
      step.op === "expectBefore" ||
      step.op === "expectNoMatch" ||
      step.op === "schedule" ||
      step.op === "cancelSchedule" ||
      step.op === "startWorkflow" ||
      step.op === "cancelWorkflow"
    ) {
      continue; // control-flow/precondition: touches no documents (server counts 0)
    }
    if (step.op === "insertMany" || step.op === "upsertMany") {
      total += step.docs.length;
    } else if (step.op === "patchByQuery" || step.op === "deleteByQuery") {
      total += Math.min(step.limit ?? MAX_BY_QUERY_ROWS, MAX_BY_QUERY_ROWS);
    } else {
      total += 1;
    }
  }
  return total;
}

/** A stored row: the user doc plus its identity/history, kept separate so the
 * system fields (`_id`/`_creationTime`/`_version`) are merged in only at read
 * time — exactly as the server stores `doc` jsonb alongside `id`/`created_at`/
 * `version` columns. `deletedAt` is the FM-33 soft-delete stamp: present only
 * on a softDelete table's deleted rows (the server's `deleted_at` column),
 * invisible to every read terminal and eq-lookup. */
export interface StoredRow {
  id: string;
  doc: Record<string, unknown>;
  createdAt: number;
  version: number;
  deletedAt?: number;
}

export interface Subscription {
  query: QueryJson;
  table: string;
  listeners: Set<(value: unknown) => void>;
  last: unknown;
  hasValue: boolean;
}

/**
 * Options for configuring the `InMemoryRtDbClient`.
 */
export interface InMemoryRtDbClientOptions {
  /** Injectable clock (epoch ms) for deterministic `_creationTime` and id minting. */
  now?: () => number;
  /** Injectable RNG in [0, 1) for deterministic id minting. */
  random?: () => number;
  /** Stable identity for this client in presence rooms. Auto-generated as a
   *  counter-prefixed token (distinct from document ids) when omitted. */
  connectionId?: string;
  /** Display identity stamped on this client's presence entries. Defaults to a
   *  bare `{ kind: "user" }` (no email/name) — tests that assert on member
   *  identity can override. */
  presenceUser?: AuthedUser;
  /** Optional shared presence backing. Two clients that pass the same
   *  `PresenceRooms` instance see each other's joins/updates/leaves; a client
   *  with no `presenceRooms` gets a private instance and sees only itself. */
  presenceRooms?: PresenceRooms;
}

function toSchemaJson(schema: AnySchema | SchemaJson): SchemaJson {
  return "toJSON" in schema && typeof schema.toJSON === "function"
    ? schema.toJSON()
    : (schema as SchemaJson);
}

/** Per-initiating-delete cascade context (FM-33), mirroring the `visited` set
 * and `cascade_rows` counter server `txn::delete_row_cascade` threads through
 * one step. `visited` guards cycles AND lets a `deleteByQuery` skip rows an
 * earlier matched row's cascade already removed; `rows` is the shared
 * `MAX_CASCADE_ROWS` budget; `touched` collects every table the cascade wrote
 * so the txn's subscription fan-out covers child tables, not just the step's. */
interface CascadeCtx {
  visited: Set<string>;
  rows: number;
  touched: Set<string>;
}

/** The canonical a subscription diffs against for push decisions (a port of
 *  server `diff_canonical`). For an UNPROJECTED query this is the plain
 *  `canonical` — byte-identical push semantics to pre-projection behavior. For
 *  a PROJECTED query (`fields` set) the volatile `_version` is stripped from
 *  every doc before comparing: `_version` bumps on every write, so an
 *  unstripped canonical would push on any member write even when no projected
 *  field changed — defeating the payload-width point of a projected
 *  subscription. Pushed payloads still carry `_version`; only the
 *  change-detection comparison ignores it. */
function diffCanonical(value: unknown, q: QueryJson): string {
  return canonical(q.fields === undefined ? value : stripVersionForDiff(value, q));
}

/**
 * In-memory par-rt-db client for unit tests. No network, no Postgres; mirrors
 * server DSL/step-result/system-field semantics. See the file-level doc for the
 * parity scope and deferred gaps.
 */
export class InMemoryRtDbClient {
  private readonly now: () => number;
  private readonly random: () => number;
  private schema: SchemaJson | null = null;
  private readonly tables = new Map<string, Map<string, StoredRow>>();
  /** Next value of each table's auto-increment counter (FM-37) — the
   * in-memory stand-in for the server's per-table Postgres sequence.
   * Monotonic from 1; absent until the first stamp (a table that declares
   * no `autoIncrementField` never touches it). */
  private readonly autoCounters = new Map<string, bigint>();
  private readonly idempotency = new Map<string, unknown[]>();
  private readonly subs: Subscription[] = [];
  private readonly schedules = new Map<string, ScheduledJob>();
  private readonly workflows = new Map<string, WorkflowRun>();
  private readonly files = new Map<
    string,
    { bytes: Uint8Array; contentType?: string; createdAt: number }
  >();
  /** Join/update/leave mechanics for this client's presence rooms, driving
   * the shared (or private) `PresenceRooms` backing. */
  private readonly presenceMembership: PresenceMembership;
  /** This client's stable identity in presence rooms. Generated as a
   * counter-prefixed token distinct from document ids so tests can tell them
   * apart at a glance. */
  readonly connectionId: string;
  private idCounter = 0;
  private _admin?: InMemoryAdminClient;
  /** Per-database read-only freeze (server `PATCH /admin/db/{db}/readonly`):
   * while set, `mutate` rejects transactions with `READ_ONLY` after the
   * idempotency-replay lookup, mirroring the server committer's Mutate arm.
   * Queries and subscriptions are unaffected. */
  private readOnly = false;

  constructor(options: InMemoryRtDbClientOptions = {}) {
    this.now = options.now ?? (() => Date.now());
    this.random = options.random ?? Math.random;
    this.connectionId = options.connectionId ?? `c${(++this.idCounter).toString(36)}`;
    this.presenceMembership = new PresenceMembership(
      options.presenceRooms ?? new PresenceRooms(),
      this.connectionId,
      options.presenceUser ?? { kind: "user" },
      this.now,
    );
  }

  /** In-memory admin surface mirroring `RtDbAdminClient`: a seedable audit log
   *  (`getAudit`) and the live subscription inspector (`listSubscriptions`).
   *  Bound to this client's state (shares the subscription registry + clock) so
   *  admin-keyed consumers can be unit-tested with no network. */
  get admin(): InMemoryAdminClient {
    if (!this._admin) {
      this._admin = new InMemoryAdminClient({ now: this.now, subs: this.subs });
    }
    return this._admin;
  }

  /** Toggles the per-database read-only freeze (corpus `dbReadOnly` cases).
   * Mirrors the server's admin toggle; writes fail with `READ_ONLY` while
   * set, after the idempotency-replay lookup. */
  setReadOnly(readOnly: boolean): void {
    this.readOnly = readOnly;
  }

  /** Installs `schema` as this client's sole in-memory database schema. The
   * first push seeds an empty doc store per table. A subsequent push must be
   * additive (server `ddl::detect_destructive_changes`): it throws BAD_REQUEST
   * on a removed/retyped table, field, or index, and otherwise merges — keeping
   * every existing table's rows and the idempotency cache intact, and seeding
   * empty doc stores only for brand-new tables. Every push validates TTL and
   * index-field rules (`validateSchema`) and `onDelete` declarations (FM-33),
   * both SCHEMA_VIOLATION, like the server does. */
  pushSchema(schema: AnySchema | SchemaJson): void {
    // Cloned because `grandfatherTrgm` mutates in place and `toJSON()` hands
    // back the builder's own index objects — a shared `defineSchema(...)`
    // fixture must not pick up one client's grandfathered flag.
    const next = clone(toSchemaJson(schema));
    validateSchema(next);
    validateOnDelete(next);
    validateComputed(next);
    validateConstraints(next);
    // FM-30: mirrors server `ddl::push_schema` — grandfather `trgm` on a
    // search index that already existed before this push declared it,
    // before the (deliberately trgm-blind) destructive-change check.
    grandfatherTrgm(this.schema ?? undefined, next);
    if (this.schema) {
      detectDestructiveChanges(this.schema, next);
      // Additive: keep existing tables' rows and the idempotency cache; only
      // seed empty doc stores for brand-new tables.
      for (const tableName of Object.keys(next.tables)) {
        if (!this.tables.has(tableName)) {
          this.tables.set(tableName, new Map());
        }
      }
    } else {
      for (const tableName of Object.keys(next.tables)) {
        this.tables.set(tableName, new Map());
      }
    }
    this.schema = next;
  }

  /** Applies (or previews) a declarative schema migration — a port of server
   * `migrate::plan_migration` (validation + structural schema fold) and
   * `migrate::apply_migration` (data effects). Structural directives update the
   * installed schema; data directives rewrite the in-memory doc map to match so
   * subsequent reads stay consistent with the new schema.
   *
   * A failed directive is atomic: every structural and data effect from earlier
   * directives rolls back (snapshot/restore, like `executeTransaction`). With
   * `req.dryRun`, the full plan is validated and `affectedRows` reported against
   * the derived schema, but nothing is committed (`applied: false`).
   *
   * `evalExpr` has no in-memory SQL engine and throws `BAD_REQUEST` — same
   * convention as the search/vector stubs. Both branches of the dual-accept
   * `expr`/`where` union (the typed `ValueExprJson`/`FilterExpr` path from
   * ENH-020 and the legacy raw-SQL string path) are unsupported here for the
   * same reason: no SQL engine to compile either against. Affected-rows counts
   * mirror the server: `renameField`/`setDefault`/`changeType`/`dropField`
   * count the rows whose docs actually changed; `dropTable` counts every row
   * (all deleted); `renameTable`/`dropIndex` report zero. */
  migrate(req: MigrateRequestJson): MigrateResultJson {
    const old = this.requireSchema();
    const planned: SchemaJson = clone(old);
    const touched = new Set<string>();
    const tableSnap = this.snapshotTables();
    const reports: DirectiveReportJson[] = [];
    try {
      for (const d of req.directives) {
        const { report, table } = applyMigrationDirective(planned, d, {
          tables: this.tables,
          rowsFor: (t) => this.rowsFor(t),
        });
        reports.push(report);
        if (table) touched.add(table);
      }
      // ENH-028: directive folding must not be able to invalidate a computed
      // entry — e.g. `changeType` retyping a computed field so its
      // expression's static kind no longer fits. Re-validate the derived maps
      // (pure) before anything commits, mirroring `plan_migration`'s trailing
      // `validate_computed` call; a failure rolls back like any failed
      // directive (also on `dryRun`, which validates the full plan).
      validateComputed(planned);
    } catch (err) {
      // Atomicity: a failed directive rolls back every earlier structural+data effect.
      this.restoreTables(tableSnap);
      throw err;
    }
    if (req.dryRun) {
      this.restoreTables(tableSnap);
      return { applied: false, schema: planned, directives: reports };
    }
    this.schema = planned;
    this.notifySubs(touched);
    return { applied: true, schema: planned, directives: reports };
  }

  /** One-shot query — same shape as {@link RtDbHttpClient.query}. */
  async query<R>(query: RtQuery<R>): Promise<R> {
    return this.executeQuery(query.json) as R;
  }

  /** Executes a transaction and returns one result per step, in order. Same
   * shape (and `idempotencyKey` semantics) as the live clients; `mutId` is a
   * deprecated alias for `idempotencyKey`. */
  async mutate(
    txn: TransactionJson,
    opts?: {
      idempotencyKey?: string;
      /** @deprecated use `idempotencyKey`. */
      mutId?: string;
    },
  ): Promise<StepResult[]> {
    const idempotencyKey = opts?.idempotencyKey ?? opts?.mutId;
    if (idempotencyKey) {
      const cached = this.idempotency.get(idempotencyKey);
      if (cached) {
        return parseStepResults(clone(cached));
      }
    }
    if (this.readOnly) {
      throw new RtDbError(
        "READ_ONLY",
        "database is frozen read-only; an operator can unfreeze it via PATCH /admin/db/{db}/readonly",
      );
    }
    const results = this.executeTransaction(txn);
    if (idempotencyKey) {
      this.idempotency.set(idempotencyKey, clone(results));
    }
    return parseStepResults(results);
  }

  /** Synchronous atomic core shared by `mutate` and `tick`'s scheduled fires:
   * enforces the step cap, snapshots, applies every step (rolling back the
   * whole txn on any error), then notifies subscriptions. */
  private executeTransaction(txn: TransactionJson): unknown[] {
    if (countSteps(txn) > MAX_STEPS) {
      throw new RtDbError("BAD_REQUEST", `transaction exceeds maximum of ${MAX_STEPS} steps`);
    }
    validateFieldGuardShapes(txn);
    // SEC-104: bound the worst-case row count before any step applies so an
    // over-budget txn rolls back nothing. Mirrors server `execute_txn`.
    let byQuerySteps = 0;
    let deadline: number | undefined;
    for (const step of txn.steps) {
      if (step.op === "expectBefore") {
        if (
          Object.keys(step).sort().join(",") !== "deadlineMs,op" ||
          !Number.isSafeInteger(step.deadlineMs) ||
          step.deadlineMs < 0
        )
          throw new RtDbError("BAD_REQUEST", "deadlineMs must be a nonnegative safe integer");
        deadline = deadline === undefined ? step.deadlineMs : Math.min(deadline, step.deadlineMs);
      }
      if (step.op === "patchByQuery" || step.op === "deleteByQuery" || step.op === "expectNoMatch")
        byQuerySteps += 1;
    }
    if (byQuerySteps > MAX_BY_QUERY_STEPS_PER_TXN) {
      throw new RtDbError(
        "BAD_REQUEST",
        `transaction has ${byQuerySteps} by-query steps, exceeding the limit of ${MAX_BY_QUERY_STEPS_PER_TXN}`,
      );
    }
    const worst = worstCaseAffected(txn);
    if (worst > MAX_AFFECTED_ROWS_PER_TXN) {
      throw new RtDbError(
        "BAD_REQUEST",
        `transaction could affect up to ${worst} documents, exceeding the limit of ${MAX_AFFECTED_ROWS_PER_TXN}`,
      );
    }
    if (deadline !== undefined && this.now() >= deadline)
      throw new RtDbError("PRECONDITION_FAILED", "transaction deadline expired");
    const snapshot = this.snapshotTables();
    // FM-28: a schedule/cancelSchedule step mutates the schedule store, so a
    // failed txn must roll it back with the docs (the server inserts/deletes
    // the scheduled_txns row on the open sqlx tx, which the rollback aborts).
    // FM-29: same for startWorkflow/cancelWorkflow and the workflow store.
    const schedulesSnapshot = new Map(this.schedules);
    const workflowsSnapshot = new Map(this.workflows);
    const workflowStates = structuredClone(this.workflows);
    const results: unknown[] = [];
    const writeSet = new Set<string>();
    try {
      for (const step of txn.steps) {
        const { result, table, extraTables } = this.executeStep(step);
        results.push(result);
        if (table) {
          writeSet.add(table);
        }
        // FM-33: an onDelete cascade writes child tables beyond the step's own.
        for (const extra of extraTables ?? []) {
          writeSet.add(extra);
        }
      }
      if (deadline !== undefined && this.now() >= deadline)
        throw new RtDbError("PRECONDITION_FAILED", "transaction deadline expired");
    } catch (error) {
      // Atomicity: any step's error rolls back everything already applied.
      this.restoreTables(snapshot);
      this.schedules.clear();
      for (const [id, job] of schedulesSnapshot) {
        this.schedules.set(id, job);
      }
      this.workflows.clear();
      for (const [id, run] of workflowsSnapshot) {
        // tick keeps the run reference while executing its step transaction.
        for (const key of Object.keys(run)) Reflect.deleteProperty(run, key);
        Object.assign(run, workflowStates.get(id));
        this.workflows.set(id, run);
      }
      throw error;
    }
    this.notifySubs(writeSet);
    return results;
  }

  /** Reactive subscription — recomputes and fires `onUpdate` on the initial
   * value and again whenever a mutation changes the result. Returns an
   * unsubscribe handle, like {@link RtDbClient.subscribe}. */
  subscribe<R>(query: RtQuery<R>, onUpdate: (value: R) => void): () => void {
    const sub: Subscription = {
      query: query.json,
      table: query.json.table,
      listeners: new Set([(value) => onUpdate(value as R)]),
      last: undefined,
      hasValue: false,
    };
    this.subs.push(sub);
    // Initial value is delivered synchronously, like the server's first
    // `queryUpdate` arriving right after `subscribe`.
    const initial = this.executeQuery(sub.query);
    sub.last = initial;
    sub.hasValue = true;
    onUpdate(initial as R);
    return () => {
      sub.listeners.clear();
      const index = this.subs.indexOf(sub);
      if (index >= 0) {
        this.subs.splice(index, 1);
      }
    };
  }

  // ---- presence -------------------------------------------------------------

  /**
   * Joins presence room `room` with optional initial `state`, mirroring
   * `RtDbClient.presence`. When `onUpdate` is supplied, it fires with the
   * current member list on join and again on every local mutation (a peer's
   * join/update/leave on a shared `PresenceRooms`).
   *
   * Returns an unsubscribe that stops listening but does NOT leave the room —
   * call `leavePresence(room)` for that, mirroring the reactive client.
   */
  presence(
    room: string,
    state?: unknown,
    onUpdate?: (members: PresenceMember[]) => void,
  ): () => void {
    return this.presenceMembership.join(room, state, onUpdate);
  }

  /** Broadcasts updated `state` for this connection in `room`. No-op if this
   * client has not joined `room` (mirrors the live server, which would not
   * relay an update from a non-member). When `ttlMs` is set, the harness
   * schedules an expiry that nulls this member's `state` at `now + ttlMs`
   * (the member stays listed) — mirroring the live server. */
  updatePresence(room: string, state: unknown, ttlMs?: number): void {
    this.presenceMembership.update(room, state, ttlMs);
  }

  /** Leaves `room`: removes this connection from the member list, drops every
   * local subscriber this client registered for that room, and fans out a
   * fresh snapshot to remaining subscribers. */
  leavePresence(room: string): void {
    this.presenceMembership.leave(room);
  }

  // ---- schedules ------------------------------------------------------------

  /** Stores `txn` scheduled for `when` and returns its id. Cron validation is
   * deferred to the live server; the harness accepts any expression. An
   * interval's `everyMs` IS validated here (mirroring server
   * `scheduler::resolve_when`) — the shared core of the public `schedule` and
   * the `Schedule` txn step. Pass `external` to create an external job: one
   * never fired by the harness's `tick` (the server's internal scheduler
   * excludes them too) — an application worker claims it via
   * `POST /api/schedule/claim` and finalizes with the returned
   * `leaseGeneration` fencing token. */
  async schedule(
    txn: TransactionJson,
    when: ScheduleWhen,
    external?: boolean,
  ): Promise<{ id: string }> {
    return this.scheduleJob(txn, when, external);
  }

  /** Sync core of {@link schedule} — the body is synchronous, and the
   * `Step::Schedule` transaction step (FM-28) reuses it from the sync
   * `executeStep` path. */
  private scheduleJob(txn: TransactionJson, when: ScheduleWhen, external = false): { id: string } {
    if (when.type === "interval") {
      if (when.everyMs <= 0) {
        throw new RtDbError("BAD_REQUEST", "everyMs must be positive");
      }
      if (when.everyMs > MAX_EVERY_MS) {
        throw new RtDbError("BAD_REQUEST", `everyMs must be at most ${MAX_EVERY_MS}`);
      }
    }
    const id = this.newId();
    const now = this.now();
    const job: ScheduledJob = {
      id,
      kind: when.type === "cron" ? "cron" : when.type === "interval" ? "interval" : "oneshot",
      txn,
      dueAt: dueAtFor(when, now),
      status: "pending",
      createdAt: now,
      firedCount: 0,
      external,
      missedCount: 0,
    };
    if (when.type === "cron") {
      job.cron = when.expr;
      if (when.tz !== undefined) job.tz = when.tz;
    }
    if (when.type === "interval") {
      job.everyMs = when.everyMs;
    }
    this.schedules.set(id, job);
    return { id };
  }

  /** Cancels a scheduled job. Resolves `true` when a row was removed;
   * `false` for an unknown id (a no-op, not an error) — the server's
   * `scheduler::cancel` contract. */
  async cancelSchedule(id: string): Promise<boolean> {
    return this.schedules.delete(id);
  }

  /** Pauses a pending job. Resolves `false` when the job is missing or not
   * pending — the server's `scheduler::set_paused(true)` contract. */
  async pauseSchedule(id: string): Promise<boolean> {
    const job = this.schedules.get(id);
    if (job?.status !== "pending") {
      return false;
    }
    job.status = "paused";
    return true;
  }

  /** Resumes a paused job. Resolves `false` when the job is missing or not
   * paused — the server's `scheduler::set_paused(false)` contract. An interval
   * job's next fire shifts to `now + everyMs` (windows elapsed while paused are
   * skipped, never backfilled); a cron's `dueAt` is left alone — the harness
   * cannot recompute a real cron fire, unlike the server. */
  async resumeSchedule(id: string): Promise<boolean> {
    const job = this.schedules.get(id);
    if (job?.status !== "paused") {
      return false;
    }
    job.status = "pending";
    if (job.kind === "interval" && job.everyMs !== undefined) {
      job.dueAt = this.now() + job.everyMs;
    }
    return true;
  }

  /**
   * Lists all workflows/tasks scheduled in this in-memory instance.
   */
  async listSchedules(): Promise<ScheduleInfo[]> {
    return [...this.schedules.values()].map((job) => toScheduleInfo(job));
  }

  // ---- workflows (FM-29) ------------------------------------------------------

  /** Starts a durable workflow run from `spec`, validating it like the server's
   * `workflows::validate_spec`. Resolves the new run's info row (pending,
   * gated at `now + steps[0].sleepBeforeMs`); `tick` advances it. */
  async startWorkflow(spec: WorkflowSpec): Promise<WorkflowInfo> {
    validateWorkflowSpec(spec);
    return toWorkflowInfo(this.startWorkflowJob(spec));
  }

  /** Cancels a pending/running/waiting run: flips it to `cancelled` +
   * `finishedAt` and clears the awaitSignal wait columns (a parked run's only
   * other escape). Resolves `false` (a no-op, not an error) for an unknown or
   * terminal run — the server's `workflows::cancel` contract. */
  async cancelWorkflow(id: string): Promise<boolean> {
    const run = this.workflows.get(id);
    if (
      !run ||
      (run.status !== "pending" && run.status !== "running" && run.status !== "waiting")
    ) {
      return false;
    }
    run.status = "cancelled";
    run.finishedAt = this.now();
    run.updatedAt = this.now();
    delete run.waitName;
    delete run.waitedSince;
    delete run.signalPayload;
    return true;
  }

  /** Delivers a named signal to a waiting run's `awaitSignal` step
   * (latest-wins: the payload overwrites any earlier unconsumed delivery and
   * the run wakes on the next `tick`). Typed errors mirror the server's
   * `SignalDelivery` classification: unknown id → NOT_FOUND; a parked run
   * waiting on a different name → CONFLICT naming both; any other non-waiting
   * state → CONFLICT. A payload over 64 KiB serialized rejects BAD_REQUEST.
   * An omitted payload is delivered as JSON null — the slot is always set on
   * a delivery (undefined stays exclusively the no-signal state). */
  async signalWorkflow(id: string, name: string, payload?: unknown): Promise<boolean> {
    if (payload !== undefined) {
      const size = new TextEncoder().encode(JSON.stringify(payload)).length;
      if (size > MAX_SIGNAL_PAYLOAD_BYTES) {
        throw new RtDbError(
          "BAD_REQUEST",
          `signal payload exceeds ${MAX_SIGNAL_PAYLOAD_BYTES} bytes`,
        );
      }
    }
    const run = this.workflows.get(id);
    if (!run) {
      throw new RtDbError("NOT_FOUND", "unknown workflow");
    }
    // Matches a waiting row AND a pending row whose waitName survived an
    // earlier delivery (latest-wins) — the server's conditional UPDATE.
    if ((run.status === "waiting" || run.status === "pending") && run.waitName === name) {
      const now = this.now();
      run.status = "pending";
      run.sleepUntil = now;
      run.signalPayload = payload ?? null;
      run.updatedAt = now;
      return true;
    }
    if (run.status === "waiting") {
      throw new RtDbError("CONFLICT", `workflow waiting on '${run.waitName}', got '${name}'`);
    }
    throw new RtDbError("CONFLICT", "workflow is not waiting for a signal");
  }

  /** Lists runs, newest first (createdAt DESC), optionally filtered by
   * `status` — the server `workflows::list` ordering. */
  async listWorkflows(status?: WorkflowStatus): Promise<WorkflowInfo[]> {
    return [...this.workflows.values()]
      .filter((run) => status === undefined || run.status === status)
      .sort((a, b) => b.createdAt - a.createdAt)
      .map((run) => toWorkflowInfo(run));
  }

  /** Fetches one full run row — info plus the per-step outcome trail
   * (mirrors `GET /admin/db/{db}/workflows/{id}`). NOT_FOUND on unknown id. */
  async getWorkflow(id: string): Promise<WorkflowInfoFull> {
    const run = this.workflows.get(id);
    if (!run) {
      throw new RtDbError("NOT_FOUND", `workflow '${id}' not found`);
    }
    return { ...toWorkflowInfo(run), stepOutcomes: [...run.stepOutcomes] };
  }

  /** Sync core of {@link startWorkflow} — the `Step::StartWorkflow` transaction
   * step reuses it from the sync `executeStep` path (the server's
   * `workflows::insert_on`). */
  private startWorkflowJob(spec: WorkflowSpec): WorkflowRun {
    const now = this.now();
    const run: WorkflowRun = {
      id: this.newId(),
      spec,
      status: "pending",
      currentStep: 0,
      attempts: 0,
      sleepUntil: now + (spec.steps[0].sleepBeforeMs ?? 0),
      createdAt: now,
      updatedAt: now,
      stepOutcomes: [],
    };
    this.workflows.set(run.id, run);
    return run;
  }

  // ---- file storage ----------------------------------------------------------
  //
  // Storage is HTTP-only on the live server; the in-memory harness mirrors the
  // surface (upload/delete/getFileMetadata/getUrl) so unit tests can exercise
  // app storage flows with no network. `getUrl` returns a synthetic
  // `memory://` handle — there is no real byte stream to serve.

  /** Stores `body` and returns a server-shaped UploadResult. The id is a
   * short counter-prefixed token (distinct in shape from document ids).
   * The in-memory harness is not a real transport, so streaming inputs
   * (`Blob`/`ReadableStream`/`ArrayBuffer`/`string`) are read into memory in
   * full for hashing/storage (ENH-021); the live server streams them. */
  async upload(body: UploadInput, contentType?: string): Promise<UploadResult> {
    const bytes = await toUint8Array(body);
    const id = `f${(++this.idCounter).toString(36)}`;
    const digest = await crypto.subtle.digest("SHA-256", bytes as BufferSource);
    const sha256 = [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
    // ARC-133: contentType is optional on the wire (omitted when unknown);
    // exactOptionalPropertyTypes forbids passing literal `undefined`, so the
    // key is included only when the caller supplied a value.
    const meta = {
      bytes,
      createdAt: this.now(),
      ...(contentType === undefined ? {} : { contentType }),
    };
    this.files.set(id, meta);
    return {
      id,
      sha256,
      size: bytes.length,
      ...(contentType === undefined ? {} : { contentType }),
    };
  }

  /**
   * Deletes a stored file by its unique identifier from the in-memory store.
   */
  async deleteFile(id: string): Promise<void> {
    if (!this.files.delete(id)) {
      throw new RtDbError("NOT_FOUND", "unknown file");
    }
  }

  /**
   * Retrieves metadata for a stored file in the in-memory store.
   */
  async getFileMetadata(id: string): Promise<FileMetadata> {
    const f = this.files.get(id);
    if (!f) {
      throw new RtDbError("NOT_FOUND", "unknown file");
    }
    return {
      id,
      sha256: "", // not tracked in-memory; only the http client computes it
      size: f.bytes.length,
      // ARC-133: include contentType only when present (omitted on the wire
      // when unknown); exactOptionalPropertyTypes forbids literal undefined.
      ...(f.contentType === undefined ? {} : { contentType: f.contentType }),
      creationTime: f.createdAt,
    };
  }

  /** Synthetic handle — no real byte stream. */
  getUrl(id: string): string {
    return `memory://${id}`;
  }

  /** Advances time for the harness: fires due non-paused scheduled jobs,
   * claims due workflows for execution, then reaps expired documents.
   * Mirrors the server's scheduler/workflow/TTL background tasks; returns the
   * number of TTL documents reaped. */
  tick(nowMs?: number): number {
    const now = nowMs ?? this.now();
    this.fireDueJobs(now);
    claimDueWorkflows(this.workflows, now, (txn) => this.executeTransaction(txn));
    return this.reapTtl(now);
  }

  /** Fire every due non-paused scheduled job (the first `tick` phase): apply
   * the job's txn through the same atomic path as `mutate`, then remove
   * one-shots or re-arm recurring kinds (a cron on the fixed CRON_STEP_MS
   * approximation; an interval from each actual fire time — missed windows
   * are skipped, never backfilled). A failing fire marks the job `error`
   * with the message but leaves it in place (recurring kinds re-arm too, so
   * the next tick retries). External jobs are NEVER fired — the worker that
   * claims them owns their execution. */
  private fireDueJobs(now: number): void {
    for (const job of this.schedules.values()) {
      // External jobs are never internally executed (server `claim_due`/
      // `next_due`/`reset_running` all exclude `external` rows): they sit
      // pending until an application worker claims them through the external
      // claim surface.
      if (job.status === "paused" || job.external || job.dueAt > now) {
        continue;
      }
      try {
        const prevDueAt = job.dueAt;
        this.executeTransaction(job.txn);
        job.firedCount++;
        if (job.kind === "oneshot") {
          this.schedules.delete(job.id);
        } else {
          // Interval re-arms from each actual fire time (cron parity: windows
          // missed during the fire's latency are skipped, not backfilled).
          // The elapsed-window count is exact for interval jobs (unlike
          // cron, which this harness approximates on a fixed CRON_STEP_MS).
          const stepMs =
            job.kind === "interval" && job.everyMs !== undefined ? job.everyMs : CRON_STEP_MS;
          if (job.kind === "interval" && job.everyMs !== undefined) {
            const delta = now - prevDueAt;
            if (delta > 0) {
              const missed = Math.floor((delta - 1) / job.everyMs);
              if (missed > 0) {
                job.missedCount += missed;
                job.lastMissedAt = now;
              }
            }
          }
          job.dueAt = now + stepMs;
          job.status = "pending";
        }
      } catch (e) {
        job.status = "error";
        job.lastError = e instanceof Error ? e.message : String(e);
        if (job.kind === "cron") {
          job.dueAt = now + CRON_STEP_MS;
        } else if (job.kind === "interval" && job.everyMs !== undefined) {
          job.dueAt = now + job.everyMs;
        }
      }
    }
  }

  /** Removes documents whose TTL field value is a number strictly less than
   * `now`, for every table that declares a `ttl`. Fires subscription fan-out
   * for touched tables (mirroring `executeTransaction`). Returns the count
   * removed. FM-33: the reaper always HARD-deletes — even rows on a softDelete
   * table — expanding onDelete cascades (`forceHard`) with one shared visited
   * set and budget across the sweep, like the server's reaper batch. */
  private reapTtl(now: number): number {
    const tables = this.schema?.tables;
    if (!tables) {
      return 0;
    }
    let removed = 0;
    const ctx: CascadeCtx = { visited: new Set(), rows: 0, touched: new Set() };
    for (const [tableName, rows] of this.tables) {
      const ttl = tables[tableName]?.ttl;
      if (!ttl) {
        continue;
      }
      for (const row of [...rows.values()]) {
        const v = row.doc[ttl.field];
        if (typeof v === "number" && v < now) {
          // An earlier expiry's cascade may already have removed this row.
          if (!rows.has(row.id)) {
            continue;
          }
          this.deleteRowCascade(tableName, row.id, ctx, true);
          removed++;
        }
      }
    }
    if (ctx.touched.size > 0) {
      this.notifySubs(ctx.touched);
    }
    return removed;
  }

  // ---- transaction execution -------------------------------------------------

  private executeStep(step: TransactionJson["steps"][number]): {
    result: unknown;
    table?: string;
    extraTables?: string[];
  } {
    if (step.op === "expectBefore") return { result: null };
    // The schedule control-flow steps (FM-28) target the scheduler, not a
    // table. Cancel mirrors the server's standalone op: `cancelled: false`
    // (not an error) when the id is missing or already fired/cancelled.
    if (step.op === "schedule") {
      const { id } = this.scheduleJob(step.txn, step.when, step.external === true);
      return { result: { scheduleId: id } };
    }
    if (step.op === "cancelSchedule") {
      return { result: { cancelled: this.schedules.delete(step.id) } };
    }
    // The workflow control-flow steps (FM-29) target the workflow store the
    // same way: start validates + inserts the run row, cancel mirrors the
    // standalone op (`cancelled: false` for unknown/terminal runs).
    if (step.op === "startWorkflow") {
      validateWorkflowSpec(step.spec);
      const run = this.startWorkflowJob(step.spec);
      return { result: { workflowId: run.id } };
    }
    if (step.op === "cancelWorkflow") {
      const run = this.workflows.get(step.id);
      const cancelled =
        !!run && (run.status === "pending" || run.status === "running" || run.status === "waiting");
      if (cancelled) {
        run.status = "cancelled";
        run.finishedAt = this.now();
        run.updatedAt = this.now();
        delete run.waitName;
        delete run.waitedSince;
        delete run.signalPayload;
      }
      return { result: { cancelled } };
    }
    const table = step.table;
    // `insertOnly` gate (mirrors server `enforce_insert_only`): runs first for
    // the step, per top-level step only, so schema-driven removals (cascade,
    // TTL reaping) that never pass through here stay sanctioned.
    if (INSERT_ONLY_BLOCKED_OPS.has(step.op) && this.schema?.tables[table]?.insertOnly) {
      throw new RtDbError(
        "SCHEMA_VIOLATION",
        `table '${table}' is insertOnly: only insert and insertMany are allowed`,
      );
    }
    switch (step.op) {
      case "insert": {
        const tableDef = this.requireTable(table);
        const id = this.doInsert(table, tableDef, step.doc);
        return { result: { id }, table };
      }
      case "patch": {
        const tableDef = this.requireTable(table);
        this.doPatch(tableDef, table, step.id, step.fields);
        return { result: null, table };
      }
      case "adjustCounter": {
        const tableDef = this.requireTable(table);
        this.doAdjustCounter(tableDef, step);
        return { result: null, table };
      }
      case "replace": {
        const tableDef = this.requireTable(table);
        this.doReplace(tableDef, table, step.id, step.doc);
        return { result: null, table };
      }
      case "delete": {
        const tableDef = this.requireTable(table);
        const extraTables = this.doDelete(tableDef, table, step.id);
        return { result: null, table, extraTables };
      }
      case "undelete": {
        const tableDef = this.requireTable(table);
        this.doUndelete(tableDef, table, step.id);
        return { result: null, table };
      }
      case "expectVersion": {
        this.requireTable(table);
        this.doExpectVersion(table, step.id, step.version);
        return { result: null };
      }
      case "expectFields": {
        const tableDef = this.requireTable(table);
        const row = this.requireRow(table, step.id);
        checkFieldExpectations(tableDef, row.doc, step.expected);
        return { result: null };
      }
      case "expectAbsent": {
        const tableDef = this.requireTable(table);
        const rows = this.eqLookup(tableDef, table, step.index, step.eq);
        if (rows.length > 0) {
          throw new RtDbError(
            "PRECONDITION_FAILED",
            `index '${step.index}' already has a matching document`,
          );
        }
        return { result: null };
      }
      case "expectNoMatch": {
        const tableDef = this.requireTable(table);
        const { rows } = this.scanByQuery(tableDef, table, step.filter, 1);
        if (rows.length > 0) {
          throw new RtDbError("PRECONDITION_FAILED", "filter matched an existing document");
        }
        return { result: null };
      }
      case "insertMany": {
        checkBulkLen("insertMany", step.docs.length);
        const tableDef = this.requireTable(table);
        const ids = step.docs.map((doc) => this.doInsert(table, tableDef, doc));
        return { result: { ids }, table };
      }
      case "upsertMany": {
        checkBulkLen("upsertMany", step.docs.length);
        const tableDef = this.requireTable(table);
        const index = requireIndex(tableDef, step.index);
        const ids: string[] = [];
        const inserted: boolean[] = [];
        step.docs.forEach((doc, n) => {
          const eq = index.fields.map((field) => {
            if (!(field in doc)) {
              throw new RtDbError(
                "BAD_REQUEST",
                `upsertMany docs[${n}] is missing indexed field '${field}' of index '${step.index}'`,
              );
            }
            return doc[field];
          });
          const rows = this.eqLookup(tableDef, table, step.index, eq);
          if (rows.length > 1) {
            throw new RtDbError("PRECONDITION_FAILED", "upsert matched multiple documents");
          }
          if (rows.length === 0) {
            ids.push(this.doInsert(table, tableDef, doc));
            inserted.push(true);
            return;
          }
          const row = rows[0];
          const merged = applyPatch(
            tableDef,
            row.doc,
            stampUpdatedAt(tableDef, doc, this.now()),
            this.now(),
          );
          this.doUpdate(table, tableDef, row, merged);
          ids.push(row.id);
          inserted.push(false);
        });
        return { result: { ids, inserted }, table };
      }
      case "upsert": {
        const tableDef = this.requireTable(table);
        const rows = this.eqLookup(tableDef, table, step.index, step.eq);
        if (rows.length > 1) {
          throw new RtDbError("PRECONDITION_FAILED", "upsert matched multiple documents");
        }
        if (rows.length === 0) {
          const id = this.doInsert(table, tableDef, step.insert);
          return { result: { id, inserted: true }, table };
        }
        const row = rows[0];
        // FM-36: upsert-update stamps the patch payload like `patch` does.
        const merged = applyPatch(
          tableDef,
          row.doc,
          stampUpdatedAt(tableDef, step.patch, this.now()),
          this.now(),
        );
        this.doUpdate(table, tableDef, row, merged);
        return { result: { id: row.id, inserted: false }, table };
      }
      case "patchByQuery": {
        const tableDef = this.requireTable(table);
        const { rows, truncated } = this.scanByQuery(tableDef, table, step.filter, step.limit);
        for (const row of rows) {
          // FM-36: stamp per matched row with a fresh clock read, like the
          // server's per-row `now_ms()` in `step_patch_by_query`.
          const merged = applyPatch(
            tableDef,
            row.doc,
            stampUpdatedAt(tableDef, step.patch, this.now()),
            this.now(),
          );
          this.doUpdate(table, tableDef, row, merged);
        }
        return { result: { patched: rows.length, truncated }, table };
      }
      case "deleteByQuery": {
        const tableDef = this.requireTable(table);
        const { rows, truncated } = this.scanByQuery(tableDef, table, step.filter, step.limit);
        // FM-33: every matched row deletes through the same onDelete-aware path
        // as a per-id delete — stamped on a softDelete table, else cascaded —
        // with ONE shared visited set and row budget across the step, so a row
        // an earlier matched row's cascade already removed is skipped, not a
        // NotFound abort.
        if (tableDef.softDelete) {
          for (const row of rows) {
            row.deletedAt = this.now();
            row.version += 1;
          }
          return { result: { deleted: rows.length, truncated }, table };
        }
        const ctx: CascadeCtx = { visited: new Set(), rows: 0, touched: new Set() };
        for (const row of rows) {
          this.deleteRowCascade(table, row.id, ctx, false);
        }
        return {
          result: { deleted: rows.length, truncated },
          table,
          extraTables: [...ctx.touched],
        };
      }
    }
  }

  private doInsert(tableName: string, tableDef: TableJson, doc: Record<string, unknown>): string {
    // Server `step_insert` order: ttl default → updatedAt stamp → defaults →
    // autoIncrement stamp (the counter wins over a defaults entry) → computed
    // stamp (ENH-028: overwrites any client-supplied computed value before
    // validation ever sees it).
    const stamped = stampComputed(
      tableDef,
      this.stampAutoIncrement(
        tableName,
        tableDef,
        applyDefaults(
          tableDef,
          stampUpdatedAt(tableDef, stampTtlDefault(tableDef, doc, this.now()), this.now()),
        ),
      ),
      this.now(),
    );
    validateDoc(tableDef, stamped);
    const stored = stripUnsetOptionals(tableDef, stamped);
    this.checkUniqueIndexes(tableName, tableDef, stored);
    const id = this.newId();
    this.rowsFor(tableName).set(id, { id, doc: stored, createdAt: this.now(), version: 1 });
    return id;
  }

  private doAdjustCounter(
    tableDef: TableJson,
    step: Extract<TransactionJson["steps"][number], { op: "adjustCounter" }>,
  ): void {
    for (const value of [step.delta, step.min, step.max]) {
      if (value !== undefined && !Number.isSafeInteger(value))
        throw new RtDbError("BAD_REQUEST", "counter values must be safe integers");
    }
    if (
      !Number.isSafeInteger(step.delta) ||
      (step.min !== undefined && step.max !== undefined && step.min > step.max)
    )
      throw new RtDbError("BAD_REQUEST", "invalid counter delta or bounds");
    if (step.expected !== undefined && !isPlainObject(step.expected))
      throw new RtDbError("BAD_REQUEST", "expected counter fields must be an object");
    if (!Object.hasOwn(tableDef.fields, step.field))
      throw new RtDbError("SCHEMA_VIOLATION", `unknown field '${step.field}'`);
    if (
      Object.hasOwn(tableDef.computed ?? {}, step.field) ||
      tableDef.autoIncrementField === step.field ||
      tableDef.updatedAtField === step.field
    )
      throw new RtDbError("BAD_REQUEST", "server-controlled fields cannot be counters");
    const fieldType = tableDef.fields[step.field];
    const numericType = fieldType.type === "optional" ? fieldType.inner : fieldType;
    if (numericType.type !== "number")
      throw new RtDbError("SCHEMA_VIOLATION", "counter field must have numeric type");
    const row = this.requireRow(step.table, step.id);
    for (const [field, expected] of Object.entries(step.expected ?? {})) {
      if (!Object.hasOwn(tableDef.fields, field))
        throw new RtDbError("SCHEMA_VIOLATION", `unknown expected field '${field}'`);
      if (!Object.hasOwn(row.doc, field) || canonical(row.doc[field]) !== canonical(expected))
        throw new RtDbError("PRECONDITION_FAILED", "counter expected fields do not match");
    }
    const current = row.doc[step.field];
    if (typeof current !== "number" || !Number.isSafeInteger(current))
      throw new RtDbError("BAD_REQUEST", "stored counter must be a safe integer");
    const next = current + step.delta;
    if (!Number.isSafeInteger(next))
      throw new RtDbError("BAD_REQUEST", "counter result must be a safe integer");
    if ((step.min !== undefined && next < step.min) || (step.max !== undefined && next > step.max))
      throw new RtDbError("PRECONDITION_FAILED", "counter result is outside its bounds");
    this.doPatch(tableDef, step.table, step.id, { [step.field]: next });
  }

  private doPatch(
    tableDef: TableJson,
    tableName: string,
    id: string,
    fields: Record<string, unknown>,
  ): void {
    const row = this.requireRow(tableName, id);
    // FM-36: the updatedAt stamp rides inside the patch payload (server
    // `step_patch`), overwriting any client-supplied value pre-merge.
    const merged = applyPatch(
      tableDef,
      row.doc,
      stampUpdatedAt(tableDef, fields, this.now()),
      this.now(),
    );
    this.doUpdate(tableName, tableDef, row, merged);
  }

  /** Stamps the table's `autoIncrementField` (FM-37) with the next value of
   * the per-table counter, overwriting any client-supplied value — the same
   * authority model as `stampUpdatedAt`, one step later (the counter wins
   * over a `defaults` entry on the same field). Runs on the two insert paths
   * only (`insert` and upsert's insert branch — both funnel through
   * `doInsert`); after insert the field is immutable (`applyPatch` /
   * `doReplace` reject changes). The value is a decimal string, matching the
   * int64 wire convention. Mirrors server `txn::stamp_auto_increment`
   * (minus gap semantics: the in-memory counter only advances on stamps, so
   * it is gap-free where a rolled-back server txn still consumes its
   * number). */
  private stampAutoIncrement(
    tableName: string,
    tableDef: TableJson,
    doc: Record<string, unknown>,
  ): Record<string, unknown> {
    const field = tableDef.autoIncrementField;
    if (field === undefined) {
      return doc;
    }
    const next = (this.autoCounters.get(tableName) ?? 0n) + 1n;
    this.autoCounters.set(tableName, next);
    const out: Record<string, unknown> = { ...doc };
    out[field] = String(next);
    return out;
  }

  private doReplace(
    tableDef: TableJson,
    tableName: string,
    id: string,
    doc: Record<string, unknown>,
  ): void {
    const row = this.requireRow(tableName, id);
    // Server `step_replace` order: defaults → updatedAt stamp (the stamp wins
    // over a defaults entry on the same field).
    let stamped = stampUpdatedAt(tableDef, applyDefaults(tableDef, doc), this.now());
    // FM-37: a replace validates as a complete document, so the server-stamped
    // auto-increment value must be present: an omitted (or null) field is
    // filled from the stored row (preserved, never re-assigned), and a
    // supplied value must equal the stored one — round-trip replace works,
    // changing the counter does not. A stored doc that PREDATES the
    // declaration (no stored value) has no value to preserve, so a replace
    // may set one — first-set, like an insert. Mirrors the preserve-or-reject
    // block in server `txn::do_replace`.
    const auto = tableDef.autoIncrementField;
    if (auto !== undefined && row.doc[auto] !== undefined) {
      const stored = row.doc[auto];
      if (stamped[auto] == null) {
        stamped = { ...stamped, [auto]: stored };
      } else if (stamped[auto] !== stored) {
        throw new RtDbError("BAD_REQUEST", `autoIncrementField '${auto}' cannot be changed`);
      }
    }
    // ENH-028: the computed stamp runs last, overwriting any client-supplied
    // computed value before validation (server `txn::do_replace`).
    stamped = stampComputed(tableDef, stamped, this.now());
    validateDoc(tableDef, stamped);
    const stored = stripUnsetOptionals(tableDef, stamped);
    this.checkUniqueIndexes(tableName, tableDef, stored, row.id);
    row.doc = stored;
    row.version += 1;
  }

  /** Per-id delete — FM-33-aware. On a softDelete table this stamps `deletedAt`
   * (+version bump) and never cascades; otherwise the row deletes through
   * `deleteRowCascade` (onDelete expansion). Returns the cascade-touched tables
   * (empty for a soft stamp) so the txn's subscription fan-out covers them. */
  private doDelete(tableDef: TableJson, tableName: string, id: string): string[] {
    if (tableDef.softDelete) {
      const row = this.rowsFor(tableName).get(id);
      if (!row || row.deletedAt !== undefined) {
        throw new RtDbError("NOT_FOUND", `document '${id}' not found`);
      }
      row.deletedAt = this.now();
      row.version += 1;
      return [];
    }
    const ctx: CascadeCtx = { visited: new Set(), rows: 0, touched: new Set() };
    this.deleteRowCascade(tableName, id, ctx, false);
    return [...ctx.touched];
  }

  /** Restores a soft-deleted row (FM-33) — port of server `txn::step_undelete`.
   * BadRequest on a table without `softDelete`; NotFound on an absent id;
   * idempotent (no version bump) on a row that is already live. Restoring must
   * not violate a unique index another live row now holds — checked BEFORE the
   * stamp clears, surfacing as Conflict. */
  private doUndelete(tableDef: TableJson, tableName: string, id: string): void {
    if (!tableDef.softDelete) {
      throw new RtDbError("BAD_REQUEST", `table '${tableName}' does not declare softDelete`);
    }
    const row = this.rowsFor(tableName).get(id);
    if (!row) {
      throw new RtDbError("NOT_FOUND", `document '${id}' not found`);
    }
    if (row.deletedAt === undefined) {
      return;
    }
    this.checkUniqueIndexes(tableName, tableDef, row.doc, row.id);
    delete row.deletedAt;
    row.version += 1;
  }

  /** Hard delete with `onDelete` expansion — a port of server
   * `txn::delete_row_cascade` (FM-33). Children-first-parent-last: for each
   * child-table field declaring an action against `tableName`, restrict throws
   * a Conflict naming `table.field` while a LIVE child references the row,
   * cascade recurses (stamping instead of deleting when the CHILD table is
   * softDelete — recursion stops there), and setNull removes the child's field
   * key (+version bump, patch-shaped). A softDelete PARENT row stamps instead
   * of deleting unless `forceHard` (the TTL reaper always hard-deletes). The
   * `ctx` visited set guards self-reference cycles and lets a `deleteByQuery`
   * step skip rows an earlier row's cascade already removed; its shared budget
   * guards runaway cascades (over-budget → Conflict, the txn rolls back). */
  private deleteRowCascade(
    tableName: string,
    id: string,
    ctx: CascadeCtx,
    forceHard: boolean,
  ): void {
    const key = `${tableName} ${id}`;
    if (ctx.visited.has(key)) {
      return;
    }
    ctx.visited.add(key);
    if (ctx.rows >= MAX_CASCADE_ROWS) {
      throw new RtDbError(
        "CONFLICT",
        `onDelete cascade exceeds the limit of ${MAX_CASCADE_ROWS} rows`,
      );
    }
    ctx.rows++;
    ctx.touched.add(tableName);

    const schema = this.requireSchema();
    const tableDef = this.requireTable(tableName);
    const row = this.rowsFor(tableName).get(id);
    if (!row || (tableDef.softDelete && row.deletedAt !== undefined)) {
      throw new RtDbError("NOT_FOUND", `document '${id}' not found`);
    }

    // A softDelete parent stamps and stops — a stamped row is never a cascade
    // trigger, so its own children are untouched.
    if (tableDef.softDelete && !forceHard) {
      row.deletedAt = this.now();
      row.version += 1;
      return;
    }

    for (const [childTableName, childTableDef] of Object.entries(schema.tables)) {
      for (const [fieldName, fieldTy] of Object.entries(childTableDef.fields)) {
        const action = onDeleteRef(fieldTy, tableName);
        if (!action) continue;
        const childIds = this.visibleChildIds(childTableName, fieldName, id);
        if (action === "restrict") {
          if (childIds.length > 0) {
            throw new RtDbError(
              "CONFLICT",
              `cannot delete '${tableName}': '${childTableName}.${fieldName}' is referenced by document '${childIds[0]}'`,
            );
          }
        } else if (action === "cascade") {
          for (const childId of childIds) {
            this.deleteRowCascade(childTableName, childId, ctx, forceHard);
          }
        } else {
          // setNull: remove the child's field key (a null-on-optional patch)
          // and bump its version — one budget slot per child, like the server.
          for (const childId of childIds) {
            if (ctx.rows >= MAX_CASCADE_ROWS) {
              throw new RtDbError(
                "CONFLICT",
                `onDelete cascade exceeds the limit of ${MAX_CASCADE_ROWS} rows`,
              );
            }
            ctx.rows++;
            const childRow = this.rowsFor(childTableName).get(childId);
            if (!childRow) continue; // visibleChildIds returns only live rows
            // FM-36: the setNull patch carries the CHILD table's updatedAt
            // stamp (server `delete_row_cascade`).
            const merged = applyPatch(
              childTableDef,
              childRow.doc,
              stampUpdatedAt(childTableDef, { [fieldName]: null }, this.now()),
              this.now(),
            );
            this.doUpdate(childTableName, childTableDef, childRow, merged);
            ctx.touched.add(childTableName);
          }
        }
      }
    }

    // Parent last.
    this.rowsFor(tableName).delete(id);
  }

  /** LIVE child rows whose `fieldName` references `parentId` — port of server
   * `txn::visible_child_ids` (FM-33): a soft-deleted child is invisible to
   * every action, so a stamped row neither blocks (restrict) nor receives
   * (cascade/setNull) its parent's delete. */
  private visibleChildIds(childTableName: string, fieldName: string, parentId: string): string[] {
    const ids: string[] = [];
    for (const row of this.rowsFor(childTableName).values()) {
      if (row.deletedAt !== undefined) continue;
      if (row.doc[fieldName] === parentId) {
        ids.push(row.id);
      }
    }
    return ids;
  }

  private doExpectVersion(tableName: string, id: string, expected: number): void {
    const row = this.requireRow(tableName, id);
    if (row.version !== expected) {
      throw new RtDbError(
        "PRECONDITION_FAILED",
        `version mismatch: expected ${expected}, actual ${row.version}`,
      );
    }
  }

  /** Scans `table` for rows matching `filter` (the same `FilterExpr` the read
   * path uses — plus the by-query-only `olderThan` op), ordered by `createdAt`
   * then `id` (server `ORDER BY "created_at", "id"`), and applies the by-query
   * `limit` (default and cap `MAX_BY_QUERY_ROWS`). Returns the selected rows
   * and whether the match set exceeded the limit (`truncated`). Mirrors server
   * `txn::step_{patch,delete}_by_query`'s scan + `query::compile_scan_where`:
   * `allowRelativeTime = true` is what makes this the one filter surface that
   * accepts `olderThan`, and the clock is read ONCE per step execution (the
   * server compiles the `now − ms` cutoff at that point), so a scheduled txn
   * re-derives it on every fire. */
  private scanByQuery(
    tableDef: TableJson,
    tableName: string,
    filter: FilterExpr,
    limitOpt: number | undefined,
  ): { rows: StoredRow[]; truncated: boolean } {
    validateFilter(filter, tableDef, true);
    const limit = Math.min(limitOpt ?? MAX_BY_QUERY_ROWS, MAX_BY_QUERY_ROWS);
    const now = this.now();
    const matched: StoredRow[] = [];
    for (const row of this.rowsFor(tableName).values()) {
      if (row.deletedAt !== undefined) continue; // FM-33: stamped rows are absent
      if (evalFilterExpr(filter, row.doc, tableDef.fields, now)) {
        matched.push(row);
      }
    }
    matched.sort((a, b) => a.createdAt - b.createdAt || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
    const truncated = matched.length > limit;
    return { rows: truncated ? matched.slice(0, limit) : matched, truncated };
  }

  /** Shared by `patch`, `replace`, and `upsert`'s patch path: writes the merged
   * doc and bumps `version` (server `apply_update`). */
  private doUpdate(
    tableName: string,
    tableDef: TableJson,
    row: StoredRow,
    merged: Record<string, unknown>,
  ): void {
    this.checkUniqueIndexes(tableName, tableDef, merged, row.id);
    row.doc = merged;
    row.version += 1;
  }

  /** Enforce `unique` indexes on a candidate write (mirrors server
   * `CREATE UNIQUE INDEX`): for each unique index on `tableName`, no OTHER row
   * (excluding `excludeId` when given) that satisfies the index's `where`
   * predicate may share the candidate's key values on the index's declared
   * `fields`. NULL/absent key fields disable the constraint for that row
   * (Postgres UNIQUE treats NULLs as distinct). Throws `CONFLICT` on collision;
   * `executeTransaction` then rolls back the whole txn via the same
   * snapshot/restore path as the `PRECONDITION_FAILED` checks. Uniqueness is on
   * `fields` only — never `id` or `created_at` (a trailing tiebreaker column
   * would defeat uniqueness, as it does on the server). */
  private checkUniqueIndexes(
    tableName: string,
    tableDef: TableJson,
    candidateDoc: Record<string, unknown>,
    excludeId?: string,
  ): void {
    const indexes = tableDef.indexes;
    if (!indexes || indexes.length === 0) return;
    for (const index of indexes) {
      if (!index.unique) continue;
      const pred = index.where;
      // A partial unique index constrains only rows matching its predicate.
      if (pred && !evalFilterExpr(pred, candidateDoc, tableDef.fields)) continue;
      const candidateKey = index.fields.map((f) => candidateDoc[f]);
      // NULLs are distinct under Postgres UNIQUE — skip when any key field is null/absent.
      if (candidateKey.some((v) => v === null || v === undefined)) continue;
      for (const row of this.rowsFor(tableName).values()) {
        if (row.deletedAt !== undefined) continue; // FM-33: stamped rows are outside unique indexes
        if (excludeId !== undefined && row.id === excludeId) continue;
        if (pred && !evalFilterExpr(pred, row.doc, tableDef.fields)) continue;
        let collision = true;
        for (let i = 0; i < index.fields.length; i++) {
          const rowVal = row.doc[index.fields[i]];
          if (rowVal === null || rowVal === undefined || rowVal !== candidateKey[i]) {
            collision = false;
            break;
          }
        }
        if (collision) {
          throw new RtDbError("CONFLICT", `unique index '${index.name}' violated`);
        }
      }
    }
  }

  /** Full-arity index lookup — a port of server `txn::eq_lookup` (shared by
   * `expectAbsent` and `upsert`). Returns every matching stored row. */
  private eqLookup(
    tableDef: TableJson,
    tableName: string,
    indexName: string,
    eq: unknown[],
  ): StoredRow[] {
    const index = requireIndex(tableDef, indexName);
    if (eq.length !== index.fields.length) {
      throw new RtDbError(
        "BAD_REQUEST",
        `index '${indexName}' expects ${index.fields.length} eq value(s), got ${eq.length}`,
      );
    }
    const typed = eq.map((value, i) => coerceIndexValue(tableDef, index.fields[i], value));
    const matches: StoredRow[] = [];
    for (const row of this.rowsFor(tableName).values()) {
      // FM-33: a soft-deleted row is absent to eq-lookup (expectAbsent/upsert).
      if (row.deletedAt !== undefined) continue;
      if (index.fields.every((field, i) => row.doc[field] != null && row.doc[field] === typed[i])) {
        matches.push(row);
      }
    }
    return matches;
  }

  // ---- query execution -------------------------------------------------------

  private executeQuery(q: QueryJson): unknown {
    return executeQuery(
      q,
      this.requireTable(q.table),
      (t) => this.rowsFor(t),
      (t) => this.requireTable(t),
    );
  }

  // ---- subscriptions ---------------------------------------------------------

  private notifySubs(writeSet: Set<string>): void {
    for (const sub of this.subs) {
      if (!writeSet.has(sub.table) || sub.listeners.size === 0) {
        continue;
      }
      // Skip a failing subscriber query (mirrors the Rust engine and keeps a
      // bad subscription from aborting the write) and keep notifying the rest.
      let next: unknown;
      try {
        next = this.executeQuery(sub.query);
      } catch {
        continue;
      }
      if (sub.hasValue && diffCanonical(next, sub.query) === diffCanonical(sub.last, sub.query)) {
        continue;
      }
      sub.last = next;
      sub.hasValue = true;
      for (const listener of sub.listeners) {
        listener(next);
      }
    }
  }

  // ---- helpers ---------------------------------------------------------------

  private requireSchema(): SchemaJson {
    if (!this.schema) {
      throw new RtDbError("INTERNAL", "no schema pushed; call pushSchema first");
    }
    return this.schema;
  }

  private requireTable(name: string): TableJson {
    const def = this.requireSchema().tables[name];
    if (!def) {
      throw new RtDbError("NOT_FOUND", `table '${name}' not found`);
    }
    return def;
  }

  private rowsFor(tableName: string): Map<string, StoredRow> {
    let rows = this.tables.get(tableName);
    if (!rows) {
      rows = new Map();
      this.tables.set(tableName, rows);
    }
    return rows;
  }

  private requireRow(tableName: string, id: string): StoredRow {
    const row = this.rowsFor(tableName).get(id);
    // FM-33: a soft-deleted row is absent to every per-id write lookup (patch,
    // replace, expectVersion — the server's live-only WHERE clauses).
    if (!row || row.deletedAt !== undefined) {
      throw new RtDbError("NOT_FOUND", `document '${id}' not found`);
    }
    return row;
  }

  /** UUIDv7-shaped id (timestamp-prefixed for sort stability), 32 hex chars. */
  private newId(): string {
    const ts = this.now().toString(16).padStart(12, "0").slice(-12);
    // The counter suffix guarantees uniqueness even under a deterministic
    // `random: () => 0` — two ids minted in the same pinned instant (e.g. two
    // workflow steps firing in one tick) must never collide.
    const rand = this.randomHex(13) + (this.idCounter++ % 0x1000000).toString(16).padStart(6, "0");
    return `${ts}7${rand}`;
  }

  private randomHex(count: number): string {
    let out = "";
    for (let i = 0; i < count; i++) {
      out += Math.floor(this.random() * 16).toString(16);
    }
    return out;
  }

  private snapshotTables(): Map<string, Map<string, StoredRow>> {
    const out = new Map<string, Map<string, StoredRow>>();
    for (const [tableName, rows] of this.tables) {
      const copy = new Map<string, StoredRow>();
      for (const [id, row] of rows) {
        copy.set(id, {
          id: row.id,
          doc: clone(row.doc),
          createdAt: row.createdAt,
          version: row.version,
          ...(row.deletedAt !== undefined ? { deletedAt: row.deletedAt } : {}),
        });
      }
      out.set(tableName, copy);
    }
    return out;
  }

  private restoreTables(snapshot: Map<string, Map<string, StoredRow>>): void {
    this.tables.clear();
    for (const [tableName, rows] of snapshot) {
      this.tables.set(tableName, rows);
    }
  }
}
