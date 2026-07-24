import { RtDbError } from "./errors.js";
import type { FileMetadata, UploadResult } from "./http.js";
import { decodeCursor, encodeCursor } from "./pagination.js";
import type {
  FieldTypeJson,
  IndexJson,
  Order,
  Paginate,
  PaginatedResultJson,
  QueryJson,
  ScheduleInfo,
  ScheduleWhen,
  SchemaJson,
  TableJson,
  TransactionJson,
} from "./protocol.js";
import type { RtQuery } from "./query.js";
import type { SchemaDefinition } from "./schema.js";

/**
 * In-memory implementation of the par-rt-db client for unit tests.
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
 * Parity is deliberately scoped to the documented core (schema push, insert /
 * patch / replace / delete / expectVersion / expectAbsent / upsert, point reads,
 * index eq + range queries with order/take/unique/first/count, and reactive
 * subscriptions). Gaps are marked with `TODO` and throw a clear `INTERNAL`
 * error rather than silently misbehaving.
 */

const MAX_STEPS = 256;
const MAX_TAKE = 4096;

/** Indexed-column storage type, mirroring server `indexed_column_type`. */
type PgType = "text" | "number" | "boolean";

interface IndexedType {
  pg: PgType;
  nullable: boolean;
}

/** A stored row: the user doc plus its identity/history, kept separate so the
 * system fields (`_id`/`_creationTime`/`_version`) are merged in only at read
 * time — exactly as the server stores `doc` jsonb alongside `id`/`created_at`/
 * `version` columns. */
interface StoredRow {
  id: string;
  doc: Record<string, unknown>;
  createdAt: number;
  version: number;
}

interface Subscription {
  query: QueryJson;
  table: string;
  listeners: Set<(value: unknown) => void>;
  last: unknown;
  hasValue: boolean;
}

/** Mirrors server schedule status values. */
type ScheduleStatus = "pending" | "running" | "paused" | "error";

/** A stored scheduled job in the in-memory harness. `tick` fires due non-paused
 * jobs by applying `txn` through the same atomic path as `mutate`. */
interface ScheduledJob {
  id: string;
  kind: "oneshot" | "cron";
  txn: TransactionJson;
  dueAt: number;
  cron?: string;
  status: ScheduleStatus;
  createdAt: number;
  firedCount: number;
  lastError?: string;
}

/** Approximate cron re-fire interval for the in-memory stub. Real 5-field cron
 * parsing is deferred to the server; the harness only needs crons to re-arm. */
const CRON_STEP_MS = 60_000;

export interface InMemoryRtDbClientOptions {
  /** Injectable clock (epoch ms) for deterministic `_creationTime` and id minting. */
  now?: () => number;
  /** Injectable RNG in [0, 1) for deterministic id minting. */
  random?: () => number;
}

function toSchemaJson(schema: SchemaDefinition<any> | SchemaJson): SchemaJson {
  return "toJSON" in schema && typeof schema.toJSON === "function"
    ? schema.toJSON()
    : (schema as SchemaJson);
}

/** Deep clone of a JSON doc (docs are pure JSON — safe to round-trip). */
function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

/** Canonical string form for change detection, independent of key order. */
function canonical(value: unknown): string {
  return JSON.stringify(value, (_k, v) => {
    if (v && typeof v === "object" && !Array.isArray(v)) {
      const sorted: Record<string, unknown> = {};
      for (const key of Object.keys(v as Record<string, unknown>).sort()) {
        sorted[key] = (v as Record<string, unknown>)[key];
      }
      return sorted;
    }
    return v;
  });
}

function isHexId(value: unknown): value is string {
  return typeof value === "string" && value.length === 32 && /^[0-9a-f]+$/.test(value);
}

function isInt64String(value: unknown): boolean {
  if (typeof value !== "string" || !/^-?\d+$/.test(value)) {
    return false;
  }
  try {
    const n = BigInt(value);
    return n >= -(2n ** 63n) && n <= 2n ** 63n - 1n;
  } catch {
    return false;
  }
}

function isBase64String(value: unknown): boolean {
  return (
    typeof value === "string" && /^[A-Za-z0-9+/]*={0,2}$/.test(value) && value.length % 4 === 0
  );
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Recursive value validator — a port of server `schema::validate_value`. */
function validateValue(ty: FieldTypeJson, value: unknown): boolean {
  switch (ty.type) {
    case "string":
      return typeof value === "string";
    case "number":
      return typeof value === "number";
    case "boolean":
      return typeof value === "boolean";
    case "null":
      return value === null;
    case "id":
      return isHexId(value);
    case "literal":
      return value === ty.value;
    case "optional":
      return value === null || validateValue(ty.inner, value);
    case "union":
      return ty.variants.some((variant) => validateValue(variant, value));
    case "array":
      return Array.isArray(value) && value.every((item) => validateValue(ty.element, item));
    case "object":
      if (!isPlainObject(value)) {
        return false;
      }
      if (Object.keys(value).some((key) => !(key in ty.fields))) {
        return false;
      }
      return Object.entries(ty.fields).every(([field, fieldTy]) => {
        if (field in value) {
          return validateValue(fieldTy, value[field]);
        }
        return fieldTy.type === "optional";
      });
    case "int64":
      return isInt64String(value);
    case "bytes":
      return isBase64String(value);
    case "any":
      return true;
    case "record":
      return isPlainObject(value) && Object.values(value).every((v) => validateValue(ty.value, v));
  }
}

/** Full-document validation — a port of server `schema::validate_doc`. */
function validateDoc(table: TableJson, doc: Record<string, unknown>): void {
  for (const key of Object.keys(doc)) {
    if (key.startsWith("_")) {
      throw new RtDbError("SCHEMA_VIOLATION", `field '${key}' is reserved`);
    }
    if (!(key in table.fields)) {
      throw new RtDbError("SCHEMA_VIOLATION", `unknown field '${key}'`);
    }
  }
  for (const [field, fieldTy] of Object.entries(table.fields)) {
    if (field in doc) {
      if (!validateValue(fieldTy, doc[field])) {
        throw new RtDbError("SCHEMA_VIOLATION", `field '${field}' has an invalid value`);
      }
    } else if (fieldTy.type !== "optional") {
      throw new RtDbError("SCHEMA_VIOLATION", `field '${field}' is required`);
    }
  }
}

/** Removes keys whose value is `null` for an `Optional` field whose inner type
 * does not itself accept `null` — a port of server `strip_unset_optionals`, so
 * an inserted/patched-then-nulled optional lands as "key absent", matching the
 * server's single representation of an unset optional. */
function stripUnsetOptionals(
  table: TableJson,
  doc: Record<string, unknown>,
): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(doc)) {
    if (value === null) {
      const fieldTy = table.fields[key];
      if (fieldTy?.type === "optional" && !validateValue(fieldTy.inner, null)) {
        continue;
      }
    }
    out[key] = value;
  }
  return out;
}

/** Applies a patch's fields onto `doc` — a port of server `txn::apply_patch`. */
function applyPatch(
  table: TableJson,
  doc: Record<string, unknown>,
  fields: Record<string, unknown>,
): Record<string, unknown> {
  const merged: Record<string, unknown> = { ...doc };
  for (const [field, value] of Object.entries(fields)) {
    const fieldTy = table.fields[field];
    if (!fieldTy) {
      throw new RtDbError("SCHEMA_VIOLATION", `unknown field '${field}'`);
    }
    if (value === null && fieldTy.type === "optional" && !validateValue(fieldTy.inner, null)) {
      delete merged[field];
      continue;
    }
    if (!validateValue(fieldTy, value)) {
      throw new RtDbError("SCHEMA_VIOLATION", `field '${field}' has an invalid value`);
    }
    merged[field] = value;
  }
  validateDoc(table, merged);
  return merged;
}

function typeTag(ty: FieldTypeJson): string {
  return ty.type;
}

/** Indexable column type — a port of server `schema::indexed_column_type`. */
function indexColumnType(ty: FieldTypeJson): IndexedType {
  switch (ty.type) {
    case "string":
    case "id":
      return { pg: "text", nullable: false };
    case "number":
      return { pg: "number", nullable: false };
    case "boolean":
      return { pg: "boolean", nullable: false };
    case "literal":
      if (typeof ty.value === "string") {
        return { pg: "text", nullable: false };
      }
      throw new RtDbError("SCHEMA_VIOLATION", `field type '${typeTag(ty)}' is not indexable`);
    case "union":
      if (ty.variants.every((v) => v.type === "literal" && typeof v.value === "string")) {
        return { pg: "text", nullable: false };
      }
      throw new RtDbError("SCHEMA_VIOLATION", `field type '${typeTag(ty)}' is not indexable`);
    case "optional": {
      const inner = indexColumnType(ty.inner);
      return { pg: inner.pg, nullable: true };
    }
    default:
      throw new RtDbError("SCHEMA_VIOLATION", `field type '${typeTag(ty)}' is not indexable`);
  }
}

/** Type-checks an eq/range bind value, mirroring server `eq_bind_for`. */
function coerceIndexValue(table: TableJson, fieldName: string, value: unknown): unknown {
  const fieldTy = table.fields[fieldName];
  if (!fieldTy) {
    throw new RtDbError("INTERNAL", `index references unknown field '${fieldName}'`);
  }
  const { pg } = indexColumnType(fieldTy);
  switch (pg) {
    case "text":
      if (typeof value !== "string") {
        throw new RtDbError("BAD_REQUEST", "eq value must be a string");
      }
      return value;
    case "number":
      if (typeof value !== "number") {
        throw new RtDbError("BAD_REQUEST", "eq value must be a number");
      }
      return value;
    case "boolean":
      if (typeof value !== "boolean") {
        throw new RtDbError("BAD_REQUEST", "eq value must be a boolean");
      }
      return value;
  }
}

/** `null`-sorts-last comparison for one sort key. JS relational ops order
 * numbers and strings; booleans coerce too. Nulls sort last (asc) / first
 * (desc, via the caller negating the result) — Postgres's default. */
function compareIndexValues(a: unknown, b: unknown): number {
  const aNull = a === null || a === undefined;
  const bNull = b === null || b === undefined;
  if (aNull && bNull) {
    return 0;
  }
  if (aNull) {
    return 1;
  }
  if (bNull) {
    return -1;
  }
  const av = a as number | string;
  const bv = b as number | string;
  if (av < bv) {
    return -1;
  }
  if (av > bv) {
    return 1;
  }
  return 0;
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
  private readonly idempotency = new Map<string, unknown[]>();
  private readonly subs: Subscription[] = [];
  private readonly schedules = new Map<string, ScheduledJob>();
  private readonly files = new Map<
    string,
    { bytes: Uint8Array; contentType?: string; createdAt: number }
  >();
  private idCounter = 0;

  constructor(options: InMemoryRtDbClientOptions = {}) {
    this.now = options.now ?? (() => Date.now());
    this.random = options.random ?? Math.random;
  }

  /** Installs `schema` as this client's sole in-memory database schema. Clears
   * any previously-stored documents so each push starts from a clean slate.
   * (The live server is additive-only; full additive evolution is deferred.) */
  pushSchema(schema: SchemaDefinition<any> | SchemaJson): void {
    this.schema = toSchemaJson(schema);
    this.tables.clear();
    this.idempotency.clear();
    for (const tableName of Object.keys(this.schema.tables)) {
      this.tables.set(tableName, new Map());
    }
  }

  /** One-shot query — same shape as {@link RtDbHttpClient.query}. */
  async query<R>(query: RtQuery<R>): Promise<R> {
    return this.executeQuery(query.json) as R;
  }

  /** Executes a transaction and returns one result per step, in order. Same
   * shape (and `mutId` idempotency-key semantics) as the live clients. */
  async mutate(txn: TransactionJson, opts?: { mutId?: string }): Promise<unknown[]> {
    if (opts?.mutId) {
      const cached = this.idempotency.get(opts.mutId);
      if (cached) {
        return clone(cached);
      }
    }
    const results = this.executeTransaction(txn);
    if (opts?.mutId) {
      this.idempotency.set(opts.mutId, clone(results));
    }
    return results;
  }

  /** Synchronous atomic core shared by `mutate` and `tick`'s scheduled fires:
   * enforces the step cap, snapshots, applies every step (rolling back the
   * whole txn on any error), then notifies subscriptions. */
  private executeTransaction(txn: TransactionJson): unknown[] {
    if (txn.steps.length > MAX_STEPS) {
      throw new RtDbError("BAD_REQUEST", `transaction exceeds maximum of ${MAX_STEPS} steps`);
    }
    const snapshot = this.snapshotTables();
    const results: unknown[] = [];
    const writeSet = new Set<string>();
    try {
      for (const step of txn.steps) {
        const { result, table } = this.executeStep(step);
        results.push(result);
        if (table) {
          writeSet.add(table);
        }
      }
    } catch (error) {
      // Atomicity: any step's error rolls back everything already applied.
      this.restoreTables(snapshot);
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

  // ---- schedules ------------------------------------------------------------

  /** Stores `txn` scheduled for `when` and returns its id. Cron validation is
   * deferred to the live server; the harness accepts any expression. */
  async schedule(txn: TransactionJson, when: ScheduleWhen): Promise<{ id: string }> {
    const id = this.newId();
    const now = this.now();
    const job: ScheduledJob = {
      id,
      kind: when.type === "cron" ? "cron" : "oneshot",
      txn,
      dueAt: this.dueAtFor(when, now),
      status: "pending",
      createdAt: now,
      firedCount: 0,
    };
    if (when.type === "cron") {
      job.cron = when.expr;
    }
    this.schedules.set(id, job);
    return { id };
  }

  async cancelSchedule(id: string): Promise<void> {
    if (!this.schedules.delete(id)) {
      throw new RtDbError("NOT_FOUND", `schedule '${id}' not found`);
    }
  }

  async pauseSchedule(id: string): Promise<void> {
    this.requireSchedule(id).status = "paused";
  }

  async resumeSchedule(id: string): Promise<void> {
    this.requireSchedule(id).status = "pending";
  }

  async listSchedules(): Promise<ScheduleInfo[]> {
    return [...this.schedules.values()].map((job) => this.toScheduleInfo(job));
  }

  // ---- file storage ----------------------------------------------------------
  //
  // Storage is HTTP-only on the live server; the in-memory harness mirrors the
  // surface (upload/delete/getFileMetadata/getUrl) so unit tests can exercise
  // app storage flows with no network. `getUrl` returns a synthetic
  // `memory://` handle — there is no real byte stream to serve.

  /** Stores `bytes` and returns a server-shaped UploadResult. The id is a
   * short counter-prefixed token (distinct in shape from document ids). */
  async upload(bytes: Uint8Array, contentType?: string): Promise<UploadResult> {
    const id = `f${(++this.idCounter).toString(36)}`;
    const digest = await crypto.subtle.digest("SHA-256", bytes as BufferSource);
    const sha256 = [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
    this.files.set(id, { bytes, contentType, createdAt: this.now() });
    return { id, sha256, size: bytes.length, contentType };
  }

  async deleteFile(id: string): Promise<void> {
    if (!this.files.delete(id)) {
      throw new RtDbError("NOT_FOUND", "unknown file");
    }
  }

  async getFileMetadata(id: string): Promise<FileMetadata> {
    const f = this.files.get(id);
    if (!f) {
      throw new RtDbError("NOT_FOUND", "unknown file");
    }
    return {
      id,
      sha256: "", // not tracked in-memory; only the http client computes it
      size: f.bytes.length,
      contentType: f.contentType,
      creationTime: f.createdAt,
    };
  }

  /** Synthetic handle — no real byte stream. */
  getUrl(id: string): string {
    return `memory://${id}`;
  }

  /** Fires every due non-paused job by applying its txn through the same atomic
   * path as `mutate` (so reactive subscriptions see the write). One-shots are
   * removed after a successful fire; crons are re-armed. Pass `nowMs` to drive
   * the clock deterministically; omit it to use the client's injected clock. */
  tick(nowMs?: number): void {
    const now = nowMs ?? this.now();
    for (const job of this.schedules.values()) {
      if (job.status === "paused" || job.dueAt > now) {
        continue;
      }
      try {
        this.executeTransaction(job.txn);
        job.firedCount++;
        if (job.kind === "oneshot") {
          this.schedules.delete(job.id);
        } else {
          job.dueAt = now + CRON_STEP_MS;
          job.status = "pending";
        }
      } catch (e) {
        job.status = "error";
        job.lastError = e instanceof Error ? e.message : String(e);
        if (job.kind === "cron") {
          job.dueAt = now + CRON_STEP_MS;
        }
      }
    }
  }

  private dueAtFor(when: ScheduleWhen, now: number): number {
    switch (when.type) {
      case "afterMs":
        return now + when.ms;
      case "runAt":
        return when.ms;
      case "cron":
        return now + CRON_STEP_MS;
    }
  }

  private requireSchedule(id: string): ScheduledJob {
    const job = this.schedules.get(id);
    if (!job) {
      throw new RtDbError("NOT_FOUND", `schedule '${id}' not found`);
    }
    return job;
  }

  private toScheduleInfo(job: ScheduledJob): ScheduleInfo {
    const info: ScheduleInfo = {
      id: job.id,
      kind: job.kind,
      dueAt: job.dueAt,
      status: job.status,
      createdAt: job.createdAt,
      firedCount: job.firedCount,
    };
    if (job.cron !== undefined) {
      info.cron = job.cron;
    }
    if (job.lastError !== undefined) {
      info.lastError = job.lastError;
    }
    return info;
  }

  // ---- transaction execution -------------------------------------------------

  private executeStep(step: TransactionJson["steps"][number]): {
    result: unknown;
    table?: string;
  } {
    const table = step.table;
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
      case "replace": {
        const tableDef = this.requireTable(table);
        this.doReplace(tableDef, table, step.id, step.doc);
        return { result: null, table };
      }
      case "delete": {
        this.requireTable(table);
        this.doDelete(table, step.id);
        return { result: null, table };
      }
      case "expectVersion": {
        this.requireTable(table);
        this.doExpectVersion(table, step.id, step.version);
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
        const merged = applyPatch(tableDef, row.doc, step.patch);
        this.doUpdate(table, row, merged);
        return { result: { id: row.id, inserted: false }, table };
      }
    }
  }

  private doInsert(tableName: string, tableDef: TableJson, doc: Record<string, unknown>): string {
    validateDoc(tableDef, doc);
    const stored = stripUnsetOptionals(tableDef, doc);
    const id = this.newId();
    this.rowsFor(tableName).set(id, { id, doc: stored, createdAt: this.now(), version: 1 });
    return id;
  }

  private doPatch(
    tableDef: TableJson,
    tableName: string,
    id: string,
    fields: Record<string, unknown>,
  ): void {
    const row = this.requireRow(tableName, id);
    const merged = applyPatch(tableDef, row.doc, fields);
    this.doUpdate(tableName, row, merged);
  }

  private doReplace(
    tableDef: TableJson,
    tableName: string,
    id: string,
    doc: Record<string, unknown>,
  ): void {
    const row = this.requireRow(tableName, id);
    validateDoc(tableDef, doc);
    row.doc = stripUnsetOptionals(tableDef, doc);
    row.version += 1;
  }

  private doDelete(tableName: string, id: string): void {
    if (!this.rowsFor(tableName).delete(id)) {
      throw new RtDbError("NOT_FOUND", `document '${id}' not found`);
    }
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

  /** Shared by `patch`, `replace`, and `upsert`'s patch path: writes the merged
   * doc and bumps `version` (server `apply_update`). */
  private doUpdate(tableName: string, row: StoredRow, merged: Record<string, unknown>): void {
    void tableName;
    row.doc = merged;
    row.version += 1;
  }

  /** Full-arity index lookup — a port of server `txn::eq_lookup` (shared by
   * `expectAbsent` and `upsert`). Returns every matching stored row. */
  private eqLookup(
    tableDef: TableJson,
    tableName: string,
    indexName: string,
    eq: unknown[],
  ): StoredRow[] {
    const index = this.requireIndex(tableDef, indexName);
    if (eq.length !== index.fields.length) {
      throw new RtDbError(
        "BAD_REQUEST",
        `index '${indexName}' expects ${index.fields.length} eq value(s), got ${eq.length}`,
      );
    }
    const typed = eq.map((value, i) => coerceIndexValue(tableDef, index.fields[i], value));
    const matches: StoredRow[] = [];
    for (const row of this.rowsFor(tableName).values()) {
      if (index.fields.every((field, i) => row.doc[field] != null && row.doc[field] === typed[i])) {
        matches.push(row);
      }
    }
    return matches;
  }

  // ---- query execution -------------------------------------------------------

  private executeQuery(q: QueryJson): unknown {
    const tableDef = this.requireTable(q.table);
    const eq = q.eq ?? [];
    const hasRange =
      q.gt !== undefined || q.gte !== undefined || q.lt !== undefined || q.lte !== undefined;

    if (q.get !== undefined) {
      if (
        q.index !== undefined ||
        eq.length > 0 ||
        hasRange ||
        q.order !== undefined ||
        q.take !== undefined ||
        q.unique ||
        q.first ||
        q.count ||
        q.paginate !== undefined
      ) {
        throw new RtDbError(
          "BAD_REQUEST",
          "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, or paginate",
        );
      }
      const row = this.rowsFor(q.table).get(q.get);
      return row ? this.mergeDoc(row) : null;
    }

    if (q.unique && (q.take !== undefined || q.order !== undefined)) {
      throw new RtDbError("BAD_REQUEST", "unique cannot be combined with take or order");
    }
    if (q.first && q.unique) {
      throw new RtDbError("BAD_REQUEST", "first cannot be combined with unique");
    }
    if (q.first && q.take !== undefined) {
      throw new RtDbError("BAD_REQUEST", "first cannot be combined with take");
    }
    if (q.count && q.unique) {
      throw new RtDbError("BAD_REQUEST", "count cannot be combined with unique");
    }
    if (q.count && q.take !== undefined) {
      throw new RtDbError("BAD_REQUEST", "count cannot be combined with take");
    }
    if (q.count && q.first) {
      throw new RtDbError("BAD_REQUEST", "count cannot be combined with first");
    }
    if (q.count && q.order !== undefined) {
      throw new RtDbError("BAD_REQUEST", "count cannot be combined with order");
    }
    if (q.paginate !== undefined) {
      // Combination guards mirror server `validate_query`: paginate is one-shot
      // paging, so it can't also narrow to count/unique/first/take. (`get` is
      // rejected above; `order`, index, eq, and range bounds are allowed.)
      if (q.count) {
        throw new RtDbError("BAD_REQUEST", "paginate cannot be combined with count");
      }
      if (q.unique) {
        throw new RtDbError("BAD_REQUEST", "paginate cannot be combined with unique");
      }
      if (q.first) {
        throw new RtDbError("BAD_REQUEST", "paginate cannot be combined with first");
      }
      if (q.take !== undefined) {
        throw new RtDbError("BAD_REQUEST", "paginate cannot be combined with take");
      }
    }
    if (q.gt !== undefined && q.gte !== undefined) {
      throw new RtDbError("BAD_REQUEST", "gt and gte cannot both be set");
    }
    if (q.lt !== undefined && q.lte !== undefined) {
      throw new RtDbError("BAD_REQUEST", "lt and lte cannot both be set");
    }
    if (q.take !== undefined && q.take > MAX_TAKE) {
      throw new RtDbError("BAD_REQUEST", `take exceeds maximum of ${MAX_TAKE}`);
    }

    const indexDef: IndexJson | null = q.index
      ? this.requireIndex(tableDef, q.index)
      : eq.length > 0
        ? (() => {
            throw new RtDbError("BAD_REQUEST", "eq requires an index");
          })()
        : null;

    const eqLen = eq.length;
    if (indexDef && eqLen > indexDef.fields.length) {
      throw new RtDbError(
        "BAD_REQUEST",
        `index '${indexDef.name}' expects at most ${indexDef.fields.length} eq value(s), got ${eqLen}`,
      );
    }
    // Type-check each eq prefix bind (server `eq_binds`).
    const typedEq = indexDef
      ? eq.map((value, i) => coerceIndexValue(tableDef, indexDef.fields[i], value))
      : [];

    let rangeField: string | null = null;
    if (hasRange) {
      if (!indexDef) {
        throw new RtDbError("BAD_REQUEST", "range bound requires an index");
      }
      if (eqLen >= indexDef.fields.length) {
        throw new RtDbError("BAD_REQUEST", "range bound requires a remaining index field after eq");
      }
      rangeField = indexDef.fields[eqLen];
    }

    const gt =
      q.gt !== undefined && rangeField ? coerceIndexValue(tableDef, rangeField, q.gt) : null;
    const gte =
      q.gte !== undefined && rangeField ? coerceIndexValue(tableDef, rangeField, q.gte) : null;
    const lt =
      q.lt !== undefined && rangeField ? coerceIndexValue(tableDef, rangeField, q.lt) : null;
    const lte =
      q.lte !== undefined && rangeField ? coerceIndexValue(tableDef, rangeField, q.lte) : null;

    const filtered: StoredRow[] = [];
    for (const row of this.rowsFor(q.table).values()) {
      if (indexDef) {
        let ok = true;
        for (let i = 0; i < eqLen; i++) {
          const v = row.doc[indexDef.fields[i]];
          if (v === null || v === undefined || v !== typedEq[i]) {
            ok = false;
            break;
          }
        }
        if (!ok) {
          continue;
        }
      }
      if (rangeField) {
        const v = row.doc[rangeField];
        if (v === null || v === undefined) {
          continue;
        }
        if (gt !== null && compareIndexValues(v, gt) <= 0) {
          continue;
        }
        if (gte !== null && compareIndexValues(v, gte) < 0) {
          continue;
        }
        if (lt !== null && compareIndexValues(v, lt) >= 0) {
          continue;
        }
        if (lte !== null && compareIndexValues(v, lte) > 0) {
          continue;
        }
      }
      filtered.push(row);
    }

    if (q.count) {
      return filtered.length;
    }

    // Sort keys: unbound index fields (after the eq prefix), then createdAt, id.
    const sortKeys: string[] = [];
    if (indexDef) {
      for (const field of indexDef.fields.slice(eqLen)) {
        sortKeys.push(field);
      }
    }
    sortKeys.push("__createdAt");
    sortKeys.push("__id");

    const dir: Order = q.order ?? "asc";
    filtered.sort((a, b) => {
      for (const field of sortKeys) {
        const av = field === "__createdAt" ? a.createdAt : field === "__id" ? a.id : a.doc[field];
        const bv = field === "__createdAt" ? b.createdAt : field === "__id" ? b.id : b.doc[field];
        const cmp = compareIndexValues(av, bv);
        if (cmp !== 0) {
          return dir === "desc" ? -cmp : cmp;
        }
      }
      return 0;
    });

    if (q.paginate !== undefined) {
      return this.paginateResult(q.paginate, tableDef, filtered, sortKeys, dir);
    }

    if (q.unique) {
      if (filtered.length > 1) {
        throw new RtDbError("PRECONDITION_FAILED", "unique query matched multiple documents");
      }
      return filtered[0] ? this.mergeDoc(filtered[0]) : null;
    }
    if (q.first) {
      return filtered[0] ? this.mergeDoc(filtered[0]) : null;
    }

    const limit = q.take ?? MAX_TAKE;
    return filtered.slice(0, limit).map((row) => this.mergeDoc(row));
  }

  /** Merges a stored row with its system fields — a port of server `merge_doc`. */
  private mergeDoc(row: StoredRow): Record<string, unknown> {
    return { ...row.doc, _id: row.id, _creationTime: row.createdAt, _version: row.version };
  }

  /** Cursor keyset pagination — a port of server `query.rs`'s paginate branch.
   * `sorted` is already filtered (eq/range) and sorted over `sortKeys` (unbound
   * index fields, then `__createdAt`, then `__id`) in direction `dir`. The
   * cursor stores one value per sort column; the resume predicate is the
   * standard OR-of-AND row-value comparison, so paging is stable (the unique
   * `id` tiebreaker means no row is skipped or duplicated across pages). */
  private paginateResult(
    paginate: Paginate,
    tableDef: TableJson,
    sorted: StoredRow[],
    sortKeys: string[],
    dir: Order,
  ): PaginatedResultJson {
    const { numItems: requested, cursor } = paginate;
    const numItems = Math.min(requested, MAX_TAKE);

    let rows = sorted;
    if (cursor) {
      const cursorValues = this.decodePaginateCursor(cursor);
      if (cursorValues.length !== sortKeys.length) {
        throw new RtDbError(
          "BAD_REQUEST",
          `cursor has ${cursorValues.length} value(s) but this query sorts over ${sortKeys.length} column(s)`,
        );
      }
      this.validateCursorValues(cursorValues, sortKeys, tableDef);
      rows = sorted.filter((row) => this.isAfterCursor(row, cursorValues, sortKeys, dir));
    }

    // Fetch one past the page size so a next page is detectable without a second
    // pass; the extra is discarded after the has-next check (server `LIMIT n+1`).
    const fetched = rows.slice(0, numItems + 1);
    const hasNext = fetched.length > numItems;
    if (hasNext) {
      fetched.pop();
    }
    const docs = fetched.map((row) => this.mergeDoc(row));
    // The next cursor is built from the page's last row; absent when the page is
    // empty or this was the final page.
    const nextCursor =
      hasNext && fetched.length > 0
        ? encodeCursor(sortKeys.map((key) => this.sortValue(fetched[fetched.length - 1], key)))
        : undefined;
    return { docs, nextCursor };
  }

  /** Decodes a paginate cursor, rethrowing the live client's generic parse error
   * as a server-shaped `BAD_REQUEST` (server `decode_cursor` → bad_request). */
  private decodePaginateCursor(cursor: string): unknown[] {
    let values: unknown;
    try {
      values = decodeCursor(cursor);
    } catch (e) {
      throw new RtDbError("BAD_REQUEST", `invalid cursor: ${(e as Error).message}`);
    }
    if (!Array.isArray(values)) {
      throw new RtDbError("BAD_REQUEST", "invalid cursor: expected an array");
    }
    return values;
  }

  /** Type-checks decoded cursor values positionally against the sort columns —
   * a port of server `SortCol::cursor_bind` (index fields via `eq_bind_for`,
   * `created_at` as number, `id` as string). The final two columns are always
   * `__createdAt` / `__id`; the rest are unbound indexed fields. */
  private validateCursorValues(
    cursorValues: unknown[],
    sortKeys: string[],
    tableDef: TableJson,
  ): void {
    for (let i = 0; i < sortKeys.length - 2; i++) {
      const value = cursorValues[i];
      // Null sorts (nulls-last) and is a legitimate value for an optional index
      // field; only type-check present values, mirroring the server's typed bind.
      if (value !== null) {
        coerceIndexValue(tableDef, sortKeys[i], value);
      }
    }
    const createdAt = cursorValues[sortKeys.length - 2];
    if (typeof createdAt !== "number") {
      throw new RtDbError("BAD_REQUEST", "cursor value for created_at must be a number");
    }
    const id = cursorValues[sortKeys.length - 1];
    if (typeof id !== "string") {
      throw new RtDbError("BAD_REQUEST", "cursor value for id must be a string");
    }
  }

  /** The keyset resume predicate: true when `row` sorts strictly after the cursor
   * row. This is the lexicographic "greater than" expanded to OR-of-AND —
   *
   *   (c0 OP v0) OR (c0 = v0 AND c1 OP v1) OR ... —
   *
   * where OP is `>` (asc) / `<` (desc). Evaluated with the same `null`-sorts-last
   * comparator as the sort, so it agrees with the ordering that produced `sorted`. */
  private isAfterCursor(
    row: StoredRow,
    cursorValues: unknown[],
    sortKeys: string[],
    dir: Order,
  ): boolean {
    for (let i = 0; i < sortKeys.length; i++) {
      let prefixEqual = true;
      for (let j = 0; j < i; j++) {
        if (compareIndexValues(this.sortValue(row, sortKeys[j]), cursorValues[j]) !== 0) {
          prefixEqual = false;
          break;
        }
      }
      if (!prefixEqual) {
        continue;
      }
      const cmp = compareIndexValues(this.sortValue(row, sortKeys[i]), cursorValues[i]);
      if (dir === "desc" ? cmp < 0 : cmp > 0) {
        return true;
      }
    }
    return false;
  }

  /** Sort value for a synthetic sort key, normalizing an absent optional index
   * field to `null` so cursor encoding and the resume predicate stay consistent
   * with the `null`-sorts-last comparator. */
  private sortValue(row: StoredRow, key: string): unknown {
    if (key === "__createdAt") {
      return row.createdAt;
    }
    if (key === "__id") {
      return row.id;
    }
    const v = row.doc[key];
    return v === undefined ? null : v;
  }

  // ---- subscriptions ---------------------------------------------------------

  private notifySubs(writeSet: Set<string>): void {
    for (const sub of this.subs) {
      if (!writeSet.has(sub.table) || sub.listeners.size === 0) {
        continue;
      }
      const next = this.executeQuery(sub.query);
      if (sub.hasValue && canonical(next) === canonical(sub.last)) {
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

  private requireIndex(tableDef: TableJson, name: string): IndexJson {
    const index = tableDef.indexes?.find((idx) => idx.name === name);
    if (!index) {
      throw new RtDbError("BAD_REQUEST", `index '${name}' not found`);
    }
    return index;
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
    if (!row) {
      throw new RtDbError("NOT_FOUND", `document '${id}' not found`);
    }
    return row;
  }

  /** UUIDv7-shaped id (timestamp-prefixed for sort stability), 32 hex chars. */
  private newId(): string {
    const ts = this.now().toString(16).padStart(12, "0").slice(-12);
    const rand = this.randomHex(19);
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
