import { RtDbError } from "./errors.js";
import type { FileMetadata, UploadResult } from "./http.js";
import { parseStepResults, type StepResult } from "./mutation.js";
import { decodeCursor, encodeCursor } from "./pagination.js";
import type {
  AggregateOp,
  AuthedUser,
  Cast,
  DirectiveJson,
  DirectiveReportJson,
  FieldTypeJson,
  FilterExpr,
  IndexJson,
  MigrateRequestJson,
  MigrateResultJson,
  Order,
  Paginate,
  PaginatedResultJson,
  PresenceMember,
  QueryJson,
  ScheduleInfo,
  ScheduleWhen,
  SchemaJson,
  TableJson,
  TransactionJson,
} from "./protocol.js";
import type { RtQuery } from "./query.js";
import type { SchemaDefinition } from "./schema.js";
import type {
  AuditEntry,
  DbSubCounters,
  GetAuditOptions,
  SessionInfo,
  SubscriptionInfo,
  SubscriptionsResponse,
} from "./admin.js";

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

export const MAX_STEPS = 1024;
const MAX_TAKE = 4096;
/** Hard cap on rows a single `patchByQuery`/`deleteByQuery` step may touch.
 * Mirrors server `txn::MAX_BY_QUERY_ROWS`; a larger match set is truncated. */
const MAX_BY_QUERY_ROWS = 1000;
/** SEC-104: hard cap on the count of `patchByQuery`/`deleteByQuery` steps in
 * one txn. Mirrors server `txn::MAX_BY_QUERY_STEPS_PER_TXN`. Without it, 1024
 * by-query steps × 1000 rows each could stall the (server's) single-writer on
 * roughly 1M rows; the cap bounds the worst case at 16 × 1000 = 16,000. */
export const MAX_BY_QUERY_STEPS_PER_TXN = 16;
/** SEC-104: hard ceiling on the worst-case total documents a single txn may
 * touch. Mirrors server `txn::MAX_AFFECTED_ROWS_PER_TXN`. Per-id steps count 1
 * each; each by-query step counts up to its `limit` (default `MAX_BY_QUERY_ROWS`). */
export const MAX_AFFECTED_ROWS_PER_TXN = 10_000;

/** SEC-104: total documents a txn could touch in the worst case. Per-id steps
 * count 1 each; each `patchByQuery`/`deleteByQuery` step counts up to its
 * `limit` (default and cap `MAX_BY_QUERY_ROWS`). Mirrors server
 * `txn::worst_case_affected`. */
export function worstCaseAffected(txn: TransactionJson): number {
  let total = 0;
  for (const step of txn.steps) {
    if (step.op === "patchByQuery" || step.op === "deleteByQuery") {
      total += Math.min(step.limit ?? MAX_BY_QUERY_ROWS, MAX_BY_QUERY_ROWS);
    } else {
      total += 1;
    }
  }
  return total;
}

/** Lowercase a value to FTS-indexable text. Mirrors the text the server feeds
 * into a search index's generated tsvector for a declared field. */
function ftsStringify(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "string") return v;
  if (typeof v === "number" || typeof v === "boolean") return String(v);
  return JSON.stringify(v);
}

/** Split text into lowercase word tokens — an approximation of the lexemes
 * `plainto_tsquery` produces, close enough for match/no-match test parity.
 * Stemming/stopwords are deliberately not replicated (a deterministic stand-in
 * is sufficient; exact `ts_rank` ordering is out of scope for the harness). */
function ftsTokens(s: string): string[] {
  return s.toLowerCase().match(/[a-z0-9]+/g) ?? [];
}

/** Indexed-column storage type, mirroring server `indexed_column_type`. */
type PgType = "text" | "number" | "boolean" | "int64";

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

/**
 * Shared in-memory presence backing: a `room -> connectionId -> member` map with
 * a per-room subscriber set. Two `InMemoryRtDbClient`s that share a
 * `PresenceRooms` instance see each other's joins/updates/leaves fan out,
 * approximating the server's per-db presence registry for tests (one client,
 * one connection — exactly like the live server's per-ConnId keying). A client
 * with no `presenceRooms` option gets a private instance and only ever sees
 * itself in its rooms.
 *
 * Mirrors the existing harness pattern of `notifySubs(writeSet)` fanning a
 * recomputed snapshot to every local subscriber after a write.
 */
export class PresenceRooms {
  private readonly members = new Map<string, Map<string, PresenceMember>>();
  private readonly subs = new Map<string, Set<(members: PresenceMember[]) => void>>();
  private readonly expiry = new Map<string, Map<string, number>>();

  /** Returns a stable-order snapshot of `room`'s current members. */
  snapshot(room: string): PresenceMember[] {
    const map = this.members.get(room);
    return map ? [...map.values()] : [];
  }

  /** Adds or replaces `member` in `room` and fans out a fresh snapshot. */
  join(room: string, member: PresenceMember): void {
    let map = this.members.get(room);
    if (!map) {
      map = new Map();
      this.members.set(room, map);
    }
    map.set(member.connectionId, member);
    this.fanOut(room);
  }

  /** Updates `connectionId`'s state in `room` and fans out. No-op if the
   * connection is not in the room (matches the live server, which would not
   * relay an update for a non-member). When `ttlMs` > 0, schedules an expiry
   * sweep that nulls this member's `state` at `now + ttlMs` (the member stays
   * listed); a refresh with `ttlMs` absent/0/falsy clears any pending expiry (a
   * permissive offline approximation — the LIVE SERVER rejects ttlMs <= 0 with
   * BAD_REQUEST), mirroring the live server's "ttlMs after the last refresh"
   * semantics for the > 0 case. */
  update(
    room: string,
    connectionId: string,
    state: unknown,
    ttlMs?: number,
    now: number = Date.now(),
  ): void {
    const map = this.members.get(room);
    const member = map?.get(connectionId);
    if (!member) {
      return;
    }
    member.state = state ?? null;
    let exp = this.expiry.get(room);
    if (!exp) {
      exp = new Map();
      this.expiry.set(room, exp);
    }
    if (ttlMs && ttlMs > 0) {
      exp.set(connectionId, now + ttlMs);
    } else {
      exp.delete(connectionId);
    }
    this.fanOut(room);
  }

  /** Removes `connectionId` from `room` and fans out. No-op if absent. Also
   * clears any pending expiry so a re-join doesn't inherit a stale ttl. */
  leave(room: string, connectionId: string): void {
    const map = this.members.get(room);
    if (!map) {
      return;
    }
    map.delete(connectionId);
    this.expiry.get(room)?.delete(connectionId);
    if (map.size === 0) {
      this.members.delete(room);
    }
    this.fanOut(room);
  }

  /** Clears expired members' `state` to `null` (the member stays listed) and
   * fans out each touched room once. Returns true if anything expired. Mirrors
   * the live server's per-connection ttl clearing. */
  expire(now: number = Date.now()): boolean {
    let any = false;
    const touched: string[] = [];
    for (const [room, exp] of this.expiry) {
      const roomMap = this.members.get(room);
      if (!roomMap) {
        this.expiry.delete(room);
        continue;
      }
      let roomTouched = false;
      for (const [connId, at] of exp) {
        if (at <= now) {
          const m = roomMap.get(connId);
          if (m) {
            m.state = null;
            any = true;
            roomTouched = true;
          }
          exp.delete(connId);
        }
      }
      if (roomTouched) {
        touched.push(room);
      }
    }
    for (const room of touched) {
      this.fanOut(room);
    }
    return any;
  }

  /** Registers `fn` for `room` snapshots and immediately fires it with the
   * current snapshot (mirroring the server's first `presenceSnapshot` on join).
   * Returns an unsubscribe. */
  subscribe(room: string, fn: (members: PresenceMember[]) => void): () => void {
    let set = this.subs.get(room);
    if (!set) {
      set = new Set();
      this.subs.set(room, set);
    }
    set.add(fn);
    fn(this.snapshot(room));
    return () => {
      const current = this.subs.get(room);
      if (!current) {
        return;
      }
      current.delete(fn);
      if (current.size === 0) {
        this.subs.delete(room);
      }
    };
  }

  private fanOut(room: string): void {
    const set = this.subs.get(room);
    if (!set) {
      return;
    }
    const snap = this.snapshot(room);
    for (const fn of set) {
      fn(snap);
    }
  }
}

export interface InMemoryRtDbClientOptions {
  /** Injectable clock (epoch ms) for deterministic `_creationTime` and id minting. */
  now?: () => number;
  /** Injectable RNG in [0, 1) for deterministic id minting. */
  random?: () => number;
  /** Stable identity for this client in presence rooms. Auto-generated as a
   * counter-prefixed token (distinct from document ids) when omitted. */
  connectionId?: string;
  /** Display identity stamped on this client's presence entries. Defaults to a
   * bare `{ kind: "user" }` (no email/name) — tests that assert on member
   * identity can override. */
  presenceUser?: AuthedUser;
  /** Optional shared presence backing. Two clients that pass the same
   * `PresenceRooms` instance see each other's joins/updates/leaves; a client
   * with no `presenceRooms` gets a private instance and sees only itself. */
  presenceRooms?: PresenceRooms;
}

function toSchemaJson(schema: SchemaDefinition<any> | SchemaJson): SchemaJson {
  return "toJSON" in schema && typeof schema.toJSON === "function"
    ? schema.toJSON()
    : (schema as SchemaJson);
}

/** Returns the values a finite literal-union (or lone literal) accepts, mirroring
 * server `schema::literal_set`: a lone `literal` yields its single value, and a
 * `union` yields its variants' literal values only when EVERY variant is a
 * `literal` (and the union is non-empty). Returns `null` for any other type
 * (scalar, optional, object, array, mixed/open union, empty union) — those are
 * not finite sets and cannot widen. */
function literalSet(ty: FieldTypeJson): unknown[] | null {
  switch (ty.type) {
    case "literal":
      return [ty.value];
    case "union": {
      if (ty.variants.length === 0) {
        return null;
      }
      const vals: unknown[] = [];
      for (const variant of ty.variants) {
        if (variant.type !== "literal") {
          return null;
        }
        vals.push(variant.value);
      }
      return vals;
    }
    default:
      return null;
  }
}

/** True iff every value accepted by `old` is also accepted by `next` — a port
 * of server `schema::is_widening_of`. Both sides must be finite literal sets
 * (per {@link literalSet}); membership is compared by `===` since literal values
 * are primitives (`string | number | boolean`). Linear scan, matching the
 * Rust `Vec::contains` semantics over the new set. */
function isWideningOf(old: FieldTypeJson, next: FieldTypeJson): boolean {
  const oldVals = literalSet(old);
  const newVals = literalSet(next);
  if (oldVals === null || newVals === null) {
    return false;
  }
  return oldVals.every((o) => newVals.some((n) => n === o));
}

/** Rejects destructive schema changes — a port of server
 * `ddl::detect_destructive_changes`. A second `pushSchema` may only ADD tables,
 * fields, and indexes; removing or retyping any existing table/field/index is a
 * `BAD_REQUEST` with the same message the live server returns. Additive changes
 * (new tables, new fields, new indexes) pass through. Field types and index
 * `fields`/`vector` are compared by `JSON.stringify` deep equality; index kind
 * (btree vs search) by the presence/absence of `search`. A field-type change is
 * accepted when it is a safe widening (server `schema::is_widening_of`): a
 * finite literal-union that grows, or a single literal that becomes a union. */
function detectDestructiveChanges(oldSchema: SchemaJson, newSchema: SchemaJson): void {
  for (const [tableName, oldTable] of Object.entries(oldSchema.tables)) {
    const newTable = newSchema.tables[tableName];
    if (!newTable) {
      throw new RtDbError("BAD_REQUEST", `removed table '${tableName}'`);
    }
    for (const [fieldName, oldFieldType] of Object.entries(oldTable.fields)) {
      const newFieldType = newTable.fields[fieldName];
      if (!newFieldType) {
        throw new RtDbError("BAD_REQUEST", `removed field '${tableName}.${fieldName}'`);
      }
      if (
        JSON.stringify(newFieldType) !== JSON.stringify(oldFieldType) &&
        !isWideningOf(oldFieldType, newFieldType)
      ) {
        throw new RtDbError("BAD_REQUEST", `changed type of field '${tableName}.${fieldName}'`);
      }
    }
    for (const oldIndex of oldTable.indexes ?? []) {
      const newIndex = (newTable.indexes ?? []).find((i) => i.name === oldIndex.name);
      if (!newIndex) {
        throw new RtDbError("BAD_REQUEST", `removed index '${oldIndex.name}'`);
      }
      if (JSON.stringify(newIndex.fields) !== JSON.stringify(oldIndex.fields)) {
        throw new RtDbError("BAD_REQUEST", `changed fields of index '${oldIndex.name}'`);
      }
      if (!!newIndex.search !== !!oldIndex.search) {
        throw new RtDbError(
          "BAD_REQUEST",
          `changed kind of index '${oldIndex.name}' (btree <-> search)`,
        );
      }
      if (JSON.stringify(newIndex.vector ?? null) !== JSON.stringify(oldIndex.vector ?? null)) {
        throw new RtDbError("BAD_REQUEST", `changed vector spec of index '${oldIndex.name}'`);
      }
    }
  }
}

/** True iff `cast` can coerce from `old` — a port of server `migrate::cast_valid_for`.
 * Mirrors the spec's coercion matrix: only the listed scalar source types are
 * accepted; an `optional`/`object`/`array`/etc. source has no sound coercion. */
function castValidFor(cast: Cast, old: FieldTypeJson): boolean {
  const t = old.type;
  switch (cast) {
    case "toString":
      return t === "string" || t === "number" || t === "boolean" || t === "int64";
    case "toNumber":
      return t === "string" || t === "boolean" || t === "int64";
    case "toInt64":
      return t === "string" || t === "number";
    case "toBoolean":
      return t === "string" || t === "number";
  }
}

/** Pure TS coercion mirroring server `migrate::coerce_value`. Returns the
 * coerced JSON value, or `undefined` when the value cannot be coerced under
 * `cast` — the caller then substitutes a (coerced) default or raises a
 * row-named `BAD_REQUEST`, matching the server's per-row decision.
 *
 * `toInt64` emits a decimal-string JSON value (int64 travels as a canonical
 * decimal string on this wire — see `schema::is_valid_int64` and
 * `FEATURE_MATRIX.md` #13); `toNumber` emits a JSON number. The other casts
 * produce the natural JSON representation. */
function coerceValue(cast: Cast, v: unknown): unknown {
  switch (cast) {
    case "toString":
      if (typeof v === "string") return v;
      if (typeof v === "number") return String(v);
      if (typeof v === "boolean") return v ? "true" : "false";
      return undefined;
    case "toNumber": {
      if (typeof v === "string") {
        const n = Number(v);
        return Number.isFinite(n) ? n : undefined;
      }
      if (typeof v === "number") return v;
      if (typeof v === "boolean") return v ? 1 : 0;
      return undefined;
    }
    case "toInt64": {
      if (typeof v === "string") {
        // `isInt64String` validates the canonical decimal-string form and the
        // i64 range; the value passes through unchanged.
        return isInt64String(v) ? v : undefined;
      }
      if (typeof v === "number") {
        if (!Number.isInteger(v)) return undefined;
        const bi = BigInt(v);
        if (bi < -(2n ** 63n) || bi > 2n ** 63n - 1n) return undefined;
        return String(v);
      }
      return undefined;
    }
    case "toBoolean": {
      if (typeof v === "string") {
        if (v === "true" || v === "1") return true;
        if (v === "false" || v === "0") return false;
        return undefined;
      }
      if (typeof v === "number") return v !== 0;
      return undefined;
    }
  }
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
    case "vector":
      return (
        Array.isArray(value) &&
        value.length === ty.dimensions &&
        value.every((item) => typeof item === "number" && Number.isFinite(item))
      );
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

/** Stamps the TTL field at insert when the table declares a
 * `defaultDurationMs` and the document omits the field. After this, the TTL
 * field is ordinary (patch/replace manipulate it normally). Mirrors server
 * `txn::stamp_ttl_default`; runs BEFORE validation so the stamped value
 * satisfies a required numeric field. Returns the same `doc` reference when no
 * stamp is needed, otherwise a shallow copy with the field set. */
function stampTtlDefault(
  table: TableJson,
  doc: Record<string, unknown>,
  now: number,
): Record<string, unknown> {
  const ttl = table.ttl;
  if (ttl?.defaultDurationMs != null && !(ttl.field in doc)) {
    const out: Record<string, unknown> = { ...doc };
    out[ttl.field] = now + ttl.defaultDurationMs;
    return out;
  }
  return doc;
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
    case "int64":
      return { pg: "int64", nullable: false };
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
    case "int64":
      // Canonical decimal string, validated exactly as on insert: `isInt64String`
      // mirrors the server's `i64::from_str` (rejects a leading `+` and
      // out-of-range values). eq is string === string, so the value is returned
      // as-is; only the comparator parses to BigInt for ordering.
      if (!isInt64String(value)) {
        throw new RtDbError("BAD_REQUEST", "eq value must be an int64 string");
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
 * (desc, via the caller negating the result) — Postgres's default. When `pg`
 * is `"int64"`, operands are parsed as `BigInt` so decimal-string values sort
 * and range numerically (no 2^53 limit) instead of lexicographically. */
function compareIndexValues(a: unknown, b: unknown, pg?: PgType): number {
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
  if (pg === "int64") {
    // Both operands are decimal-string int64 values (validated by
    // `coerceIndexValue` or stored as the canonical form on insert), so the
    // `BigInt()` parse is total — no try/catch needed.
    const an = BigInt(a as string);
    const bn = BigInt(b as string);
    if (an < bn) {
      return -1;
    }
    if (an > bn) {
      return 1;
    }
    return 0;
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

/** Applies one aggregate op over a non-empty `values` array. Mirrors the SQL
 * semantics: SUM/AVG require all entries numeric; MIN/MAX pick the smallest/
 * largest per `compareIndexValues` so a string field's MIN/MAX matches Postgres
 * lexicographic ordering, unless `pg === "int64"` in which case both ordering
 * and numeric reduction parse the decimal strings (server `SUM(bigint)`/
 * `AVG(bigint)` return Postgres `numeric` → JSON number, so `Number()` is the
 * correct projection — accepted precision loss past 2^53). AVG returns the
 * arithmetic mean (no rounding). */
function applyAggregate(op: AggregateOp, values: unknown[], pg?: PgType): unknown {
  switch (op) {
    case "count":
      // COUNT(*) over the matching set — consumes no aggregate field, so `pg`
      // is irrelevant and `values` is one entry per counted row.
      return values.length;
    case "sum":
      if (pg === "int64") {
        return values.reduce<number>((acc, v) => acc + Number(v), 0);
      }
      return values.reduce<number>((acc, v) => acc + (v as number), 0);
    case "avg":
      if (pg === "int64") {
        return values.reduce<number>((acc, v) => acc + Number(v), 0) / values.length;
      }
      return values.reduce<number>((acc, v) => acc + (v as number), 0) / values.length;
    case "min":
      return values.reduce((best, v) => (compareIndexValues(best, v, pg) <= 0 ? best : v));
    case "max":
      return values.reduce((best, v) => (compareIndexValues(best, v, pg) >= 0 ? best : v));
  }
}

type FilterLeafOp = "eq" | "neq" | "gt" | "gte" | "lt" | "lte";

/**
 * Structural validation of a `FilterExpr` against a table's declared fields,
 * mirroring server `query::compile_filter_node` / `field_lhs_and_bind`
 * (`query.rs`). Throws `BAD_REQUEST` for an unknown field, an empty `and`/`or`,
 * an empty `in`, or a non-string/number/boolean leaf value. Call once before
 * evaluating per row.
 */
export function validateFilter(node: FilterExpr, fields: ReadonlySet<string>): void {
  switch (node.op) {
    case "and":
    case "or":
      if (node.exprs.length === 0) {
        throw new RtDbError("BAD_REQUEST", `${node.op} filter requires at least one expr`);
      }
      for (const e of node.exprs) validateFilter(e, fields);
      return;
    case "in": {
      if (node.values.length === 0) {
        throw new RtDbError("BAD_REQUEST", "in filter requires at least one value");
      }
      for (const v of node.values) checkLeafValue(node.field, v, fields);
      const firstKind = inValueKind(node.values[0]);
      for (const v of node.values.slice(1)) {
        if (inValueKind(v) !== firstKind) {
          throw new RtDbError("BAD_REQUEST", "in filter values must all be the same type");
        }
      }
      return;
    }
    case "not":
      validateFilter(node.expr, fields);
      return;
    case "contains":
      checkLeafValue(node.field, node.value, fields);
      return;
    case "exists":
      if (!fields.has(node.field)) {
        throw new RtDbError("BAD_REQUEST", `filter references unknown field '${node.field}'`);
      }
      return;
    default:
      checkLeafValue(node.field, node.value, fields);
  }
}

function checkLeafValue(field: string, value: unknown, fields: ReadonlySet<string>): void {
  if (!fields.has(field)) {
    throw new RtDbError("BAD_REQUEST", `filter references unknown field '${field}'`);
  }
  if (typeof value !== "string" && typeof value !== "number" && typeof value !== "boolean") {
    throw new RtDbError("BAD_REQUEST", "filter value must be a string, number, or boolean");
  }
}

function inValueKind(value: unknown): "string" | "number" | "boolean" {
  if (typeof value === "string") return "string";
  if (typeof value === "number") return "number";
  return "boolean";
}

/**
 * Evaluate a `FilterExpr` predicate against a stored doc, mirroring server
 * `query::jsonb_lhs_and_bind` (`query.rs`): the filter value's kind picks the
 * comparison domain — string compares the doc field's `->>` text, number
 * compares it as `float8`, boolean as `boolean`. A null/absent field never
 * matches (SQL NULL exclusion). Assumes `validateFilter` already passed.
 */
export function evalFilterExpr(node: FilterExpr, doc: Record<string, unknown>): boolean {
  switch (node.op) {
    case "and":
      return node.exprs.every((e) => evalFilterExpr(e, doc));
    case "or":
      return node.exprs.some((e) => evalFilterExpr(e, doc));
    case "in":
      return node.values.some((v) => compareLeaf("eq", node.field, v, doc));
    case "not":
      return !evalFilterExpr(node.expr, doc);
    case "contains": {
      const arr = doc[node.field];
      const want = JSON.stringify(node.value);
      return Array.isArray(arr) && arr.some((v) => JSON.stringify(v) === want);
    }
    case "exists": {
      const v = doc[node.field];
      return v !== undefined && v !== null;
    }
    default:
      return compareLeaf(node.op, node.field, node.value, doc);
  }
}

function compareLeaf(
  op: FilterLeafOp,
  field: string,
  filterValue: unknown,
  doc: Record<string, unknown>,
): boolean {
  const docVal = doc[field];
  if (docVal === null || docVal === undefined) {
    return false;
  }
  if (typeof filterValue === "string") {
    return compareValues(op, docToText(docVal), filterValue);
  }
  if (typeof filterValue === "number") {
    const lhs = docToNumber(docVal);
    return lhs === null ? false : compareValues(op, lhs, filterValue);
  }
  if (typeof docVal === "boolean") {
    return compareValues(op, docVal, filterValue as boolean);
  }
  return false;
}

/** Mirrors Postgres `doc->>'field'`: the JSON text of the value. */
function docToText(docVal: unknown): string {
  if (typeof docVal === "string") return docVal;
  if (typeof docVal === "number") return JSON.stringify(docVal);
  if (typeof docVal === "boolean") return docVal ? "true" : "false";
  return JSON.stringify(docVal);
}

/** Mirrors Postgres `(doc->>'field')::float8`: a number, or a numeric string. */
function docToNumber(docVal: unknown): number | null {
  if (typeof docVal === "number") return Number.isFinite(docVal) ? docVal : null;
  if (typeof docVal === "string" && docVal.trim() !== "") {
    const n = Number(docVal);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function compareValues(
  op: FilterLeafOp,
  lhs: string | number | boolean,
  rhs: string | number | boolean,
): boolean {
  switch (op) {
    case "eq":
      return lhs === rhs;
    case "neq":
      return lhs !== rhs;
    case "gt":
      return lhs > rhs;
    case "gte":
      return lhs >= rhs;
    case "lt":
      return lhs < rhs;
    case "lte":
      return lhs <= rhs;
  }
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
  private readonly presenceRooms: PresenceRooms;
  private readonly presenceUser: AuthedUser;
  private readonly joinedRooms = new Set<string>();
  /** Unsubscribe handles for the per-room callbacks this client registered on
   * `PresenceRooms`. Tracked so `leavePresence(room)` can drop every local
   * subscriber for that room, mirroring the reactive client and the live
   * server (which stops delivering snapshots once a connection has left). */
  private readonly presenceUnsubs = new Map<string, Array<() => void>>();
  /** This client's stable identity in presence rooms. Generated as a
   * counter-prefixed token distinct from document ids so tests can tell them
   * apart at a glance. */
  readonly connectionId: string;
  private idCounter = 0;
  private _admin?: InMemoryAdminClient;

  constructor(options: InMemoryRtDbClientOptions = {}) {
    this.now = options.now ?? (() => Date.now());
    this.random = options.random ?? Math.random;
    this.presenceRooms = options.presenceRooms ?? new PresenceRooms();
    this.presenceUser = options.presenceUser ?? { kind: "user" };
    this.connectionId = options.connectionId ?? `c${(++this.idCounter).toString(36)}`;
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

  /** Installs `schema` as this client's sole in-memory database schema. The
   * first push seeds an empty doc store per table. A subsequent push must be
   * additive (server `ddl::detect_destructive_changes`): it throws BAD_REQUEST
   * on a removed/retyped table, field, or index, and otherwise merges — keeping
   * every existing table's rows and the idempotency cache intact, and seeding
   * empty doc stores only for brand-new tables. */
  pushSchema(schema: SchemaDefinition<any> | SchemaJson): void {
    const next = toSchemaJson(schema);
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
   * convention as the search/vector stubs. Affected-rows counts mirror the
   * server: `renameField`/`setDefault`/`changeType`/`dropField` count the rows
   * whose docs actually changed; `dropTable` counts every row (all deleted);
   * `renameTable`/`dropIndex` report zero. */
  migrate(req: MigrateRequestJson): MigrateResultJson {
    const old = this.requireSchema();
    const planned: SchemaJson = clone(old);
    const touched = new Set<string>();
    const tableSnap = this.snapshotTables();
    const reports: DirectiveReportJson[] = [];
    try {
      for (const d of req.directives) {
        const { report, table } = this.applyMigrationDirective(planned, d);
        reports.push(report);
        if (table) touched.add(table);
      }
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

  /** Validates and applies one directive: folds the structural effect into
   * `planned` (the working schema copy) and rewrites the in-memory doc map. */
  private applyMigrationDirective(
    planned: SchemaJson,
    d: DirectiveJson,
  ): { report: DirectiveReportJson; table?: string } {
    switch (d.op) {
      case "renameField": {
        const t = this.migrateTable(planned, d.table);
        if (d.to in t.fields) {
          throw new RtDbError("BAD_REQUEST", `rename target '${d.table}.${d.to}' already exists`);
        }
        const ftype = t.fields[d.from];
        if (!ftype) {
          throw new RtDbError("BAD_REQUEST", `renamed field '${d.table}.${d.from}' does not exist`);
        }
        delete t.fields[d.from];
        t.fields[d.to] = ftype;
        for (const ix of t.indexes ?? []) {
          for (let i = 0; i < ix.fields.length; i++) {
            if (ix.fields[i] === d.from) ix.fields[i] = d.to;
          }
        }
        if (t.ownerField === d.from) t.ownerField = d.to;
        if (t.collaboratorsField === d.from) t.collaboratorsField = d.to;
        let affected = 0;
        for (const row of this.rowsFor(d.table).values()) {
          if (d.from in row.doc) {
            row.doc[d.to] = row.doc[d.from];
            delete row.doc[d.from];
            affected++;
          }
        }
        return { report: { op: "renameField", affectedRows: affected }, table: d.table };
      }
      case "renameTable": {
        if (d.to in planned.tables) {
          throw new RtDbError("BAD_REQUEST", `rename target table '${d.to}' already exists`);
        }
        const def = planned.tables[d.from];
        if (!def) {
          throw new RtDbError("BAD_REQUEST", `renamed table '${d.from}' does not exist`);
        }
        delete planned.tables[d.from];
        // Id references to `from` in other tables follow the rename.
        for (const other of Object.values(planned.tables)) {
          for (const [fname, ftype] of Object.entries(other.fields)) {
            if (ftype.type === "id" && ftype.table === d.from) {
              other.fields[fname] = { type: "id", table: d.to };
            }
          }
        }
        planned.tables[d.to] = def;
        const rows = this.tables.get(d.from);
        if (rows) {
          this.tables.delete(d.from);
          this.tables.set(d.to, rows);
        }
        return { report: { op: "renameTable", affectedRows: 0 }, table: d.to };
      }
      case "changeType": {
        const t = this.migrateTable(planned, d.table);
        const oldTy = t.fields[d.field];
        if (!oldTy) {
          throw new RtDbError(
            "BAD_REQUEST",
            `changed field '${d.table}.${d.field}' does not exist`,
          );
        }
        if (!castValidFor(d.cast, oldTy)) {
          throw new RtDbError(
            "BAD_REQUEST",
            `cast ${d.cast} is not valid for ${d.table}.${d.field}`,
          );
        }
        const rows = [...this.rowsFor(d.table).values()];
        let affected = 0;
        for (const row of rows) {
          if (!(d.field in row.doc)) continue;
          affected++;
          const coerced = coerceValue(d.cast, row.doc[d.field]);
          if (coerced !== undefined) {
            row.doc[d.field] = coerced;
            continue;
          }
          if (d.default !== undefined) {
            const dv = coerceValue(d.cast, d.default);
            row.doc[d.field] = dv ?? d.default;
            continue;
          }
          throw new RtDbError(
            "BAD_REQUEST",
            `changeType cannot coerce value in ${d.table}.${row.id} (${row.doc[d.field]}) and no default given`,
          );
        }
        t.fields[d.field] = d.to;
        return { report: { op: "changeType", affectedRows: affected }, table: d.table };
      }
      case "dropField": {
        const t = this.migrateTable(planned, d.table);
        if (!(d.field in t.fields)) {
          throw new RtDbError(
            "BAD_REQUEST",
            `dropped field '${d.table}.${d.field}' does not exist`,
          );
        }
        delete t.fields[d.field];
        for (const ix of t.indexes ?? []) {
          ix.fields = ix.fields.filter((f) => f !== d.field);
        }
        if (t.ownerField === d.field) t.ownerField = undefined;
        if (t.collaboratorsField === d.field) t.collaboratorsField = undefined;
        const rows = this.rowsFor(d.table);
        let affected = 0;
        for (const row of rows.values()) {
          if (!(d.field in row.doc)) continue;
          delete row.doc[d.field];
          affected++;
        }
        return { report: { op: "dropField", affectedRows: affected }, table: d.table };
      }
      case "dropTable": {
        const def = planned.tables[d.name];
        if (!def) {
          throw new RtDbError("BAD_REQUEST", `dropped table '${d.name}' does not exist`);
        }
        const count = this.rowsFor(d.name).size;
        delete planned.tables[d.name];
        this.tables.delete(d.name);
        return { report: { op: "dropTable", affectedRows: count }, table: d.name };
      }
      case "dropIndex": {
        const t = this.migrateTable(planned, d.table);
        const ix = (t.indexes ?? []).find((i) => i.name === d.name);
        if (!ix) {
          throw new RtDbError("BAD_REQUEST", `dropped index '${d.table}.${d.name}' does not exist`);
        }
        t.indexes = (t.indexes ?? []).filter((i) => i.name !== d.name);
        return { report: { op: "dropIndex", affectedRows: 0 }, table: d.table };
      }
      case "setDefault": {
        const t = this.migrateTable(planned, d.table);
        if (!(d.field in t.fields)) {
          throw new RtDbError(
            "BAD_REQUEST",
            `setDefault target '${d.table}.${d.field}' does not exist`,
          );
        }
        let affected = 0;
        for (const row of this.rowsFor(d.table).values()) {
          if (!(d.field in row.doc)) {
            row.doc[d.field] = clone(d.value);
            affected++;
          }
        }
        return { report: { op: "setDefault", affectedRows: affected }, table: d.table };
      }
      case "evalExpr": {
        // No SQL engine in the harness — throw rather than silently misbehave.
        throw new RtDbError("BAD_REQUEST", "evalExpr unsupported in-memory");
      }
    }
  }

  /** Resolves a mutable table definition from the working schema, throwing the
   * server-shaped `BAD_REQUEST` when the table is absent. */
  private migrateTable(schema: SchemaJson, name: string): TableJson {
    const t = schema.tables[name];
    if (!t) {
      throw new RtDbError("BAD_REQUEST", `table '${name}' does not exist`);
    }
    return t;
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
    if (txn.steps.length > MAX_STEPS) {
      throw new RtDbError("BAD_REQUEST", `transaction exceeds maximum of ${MAX_STEPS} steps`);
    }
    // SEC-104: bound the worst-case row count before any step applies so an
    // over-budget txn rolls back nothing. Mirrors server `execute_txn`.
    let byQuerySteps = 0;
    for (const step of txn.steps) {
      if (step.op === "patchByQuery" || step.op === "deleteByQuery") byQuerySteps += 1;
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
    this.joinedRooms.add(room);
    this.presenceRooms.join(room, {
      connectionId: this.connectionId,
      user: this.presenceUser,
      state: state ?? null,
    });
    let off: (() => void) | undefined;
    if (onUpdate) {
      off = this.presenceRooms.subscribe(room, onUpdate);
      let arr = this.presenceUnsubs.get(room);
      if (!arr) {
        arr = [];
        this.presenceUnsubs.set(room, arr);
      }
      arr.push(off);
    }
    return () => {
      off?.();
      if (off) {
        const arr = this.presenceUnsubs.get(room);
        if (arr) {
          const i = arr.indexOf(off);
          if (i >= 0) arr.splice(i, 1);
          if (arr.length === 0) this.presenceUnsubs.delete(room);
        }
      }
    };
  }

  /** Broadcasts updated `state` for this connection in `room`. No-op if this
   * client has not joined `room` (mirrors the live server, which would not
   * relay an update from a non-member). When `ttlMs` is set, the harness
   * schedules an expiry that nulls this member's `state` at `now + ttlMs`
   * (the member stays listed) — mirroring the live server. */
  updatePresence(room: string, state: unknown, ttlMs?: number): void {
    if (!this.joinedRooms.has(room)) {
      return;
    }
    this.presenceRooms.update(room, this.connectionId, state, ttlMs, this.now());
  }

  /** Leaves `room`: removes this connection from the member list, drops every
   * local subscriber this client registered for that room, and fans out a
   * fresh snapshot to remaining subscribers. */
  leavePresence(room: string): void {
    if (!this.joinedRooms.delete(room)) {
      return;
    }
    const arr = this.presenceUnsubs.get(room);
    if (arr) {
      for (const off of arr) off();
      this.presenceUnsubs.delete(room);
    }
    this.presenceRooms.leave(room, this.connectionId);
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
   * the clock deterministically; omit it to use the client's injected clock.
   *
   * Also reaps expired documents: any table that declares a `ttl` has rows
   * removed whose TTL field value is a number strictly less than `now` (a no-op
   * for tables without TTL). Returns the count of documents reaped. The live
   * server's per-db reaper is the real expiry; this is best-effort, for
   * tests/local workflows. */
  tick(nowMs?: number): number {
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
    return this.reapTtl(now);
  }

  /** Removes documents whose TTL field value is a number strictly less than
   * `now`, for every table that declares a `ttl`. Fires subscription fan-out
   * for touched tables (mirroring `executeTransaction`). Returns the count
   * removed. */
  private reapTtl(now: number): number {
    const tables = this.schema?.tables;
    if (!tables) {
      return 0;
    }
    let removed = 0;
    const touched = new Set<string>();
    for (const [tableName, rows] of this.tables) {
      const ttl = tables[tableName]?.ttl;
      if (!ttl) {
        continue;
      }
      for (const [id, row] of rows) {
        const v = row.doc[ttl.field];
        if (typeof v === "number" && v < now) {
          rows.delete(id);
          removed++;
          touched.add(tableName);
        }
      }
    }
    if (touched.size > 0) {
      this.notifySubs(touched);
    }
    return removed;
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
        this.doUpdate(table, tableDef, row, merged);
        return { result: { id: row.id, inserted: false }, table };
      }
      case "patchByQuery": {
        const tableDef = this.requireTable(table);
        const { rows, truncated } = this.scanByQuery(tableDef, table, step.filter, step.limit);
        for (const row of rows) {
          const merged = applyPatch(tableDef, row.doc, step.patch);
          this.doUpdate(table, tableDef, row, merged);
        }
        return { result: { patched: rows.length, truncated }, table };
      }
      case "deleteByQuery": {
        const tableDef = this.requireTable(table);
        const { rows, truncated } = this.scanByQuery(tableDef, table, step.filter, step.limit);
        for (const row of rows) {
          this.rowsFor(table).delete(row.id);
        }
        return { result: { deleted: rows.length, truncated }, table };
      }
    }
  }

  private doInsert(tableName: string, tableDef: TableJson, doc: Record<string, unknown>): string {
    const stamped = stampTtlDefault(tableDef, doc, this.now());
    validateDoc(tableDef, stamped);
    const stored = stripUnsetOptionals(tableDef, stamped);
    this.checkUniqueIndexes(tableName, tableDef, stored);
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
    this.doUpdate(tableName, tableDef, row, merged);
  }

  private doReplace(
    tableDef: TableJson,
    tableName: string,
    id: string,
    doc: Record<string, unknown>,
  ): void {
    const row = this.requireRow(tableName, id);
    validateDoc(tableDef, doc);
    const stored = stripUnsetOptionals(tableDef, doc);
    this.checkUniqueIndexes(tableName, tableDef, stored, row.id);
    row.doc = stored;
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

  /** Scans `table` for rows matching `filter` (the same `FilterExpr` the read
   * path uses), ordered by `createdAt` then `id` (server
   * `ORDER BY "created_at", "id"`), and applies the by-query `limit` (default
   * and cap `MAX_BY_QUERY_ROWS`). Returns the selected rows and whether the
   * match set exceeded the limit (`truncated`). Mirrors server
   * `txn::step_{patch,delete}_by_query`'s scan + `query::compile_scan_where`. */
  private scanByQuery(
    tableDef: TableJson,
    tableName: string,
    filter: FilterExpr,
    limitOpt: number | undefined,
  ): { rows: StoredRow[]; truncated: boolean } {
    validateFilter(filter, new Set(Object.keys(tableDef.fields)));
    const limit = Math.min(limitOpt ?? MAX_BY_QUERY_ROWS, MAX_BY_QUERY_ROWS);
    const matched: StoredRow[] = [];
    for (const row of this.rowsFor(tableName).values()) {
      if (evalFilterExpr(filter, row.doc)) {
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
      if (pred && !evalFilterExpr(pred, candidateDoc)) continue;
      const candidateKey = index.fields.map((f) => candidateDoc[f]);
      // NULLs are distinct under Postgres UNIQUE — skip when any key field is null/absent.
      if (candidateKey.some((v) => v === null || v === undefined)) continue;
      for (const row of this.rowsFor(tableName).values()) {
        if (excludeId !== undefined && row.id === excludeId) continue;
        if (pred && !evalFilterExpr(pred, row.doc)) continue;
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
        q.distinct ||
        q.aggregate !== undefined ||
        q.paginate !== undefined ||
        q.filter !== undefined ||
        q.search !== undefined ||
        q.vectorSearch !== undefined ||
        q.hybridSearch !== undefined
      ) {
        throw new RtDbError(
          "BAD_REQUEST",
          "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, distinct, aggregate, paginate, filter, search, or vector search",
        );
      }
      const row = this.rowsFor(q.table).get(q.get);
      return row ? this.mergeDoc(row) : null;
    }

    if (
      q.unique &&
      (q.take !== undefined || q.order !== undefined || q.distinct || q.aggregate !== undefined)
    ) {
      throw new RtDbError(
        "BAD_REQUEST",
        "unique cannot be combined with take, order, distinct, or aggregate",
      );
    }
    if (q.first && q.unique) {
      throw new RtDbError("BAD_REQUEST", "first cannot be combined with unique");
    }
    if (q.first && q.take !== undefined) {
      throw new RtDbError("BAD_REQUEST", "first cannot be combined with take");
    }
    if (q.first && q.distinct) {
      throw new RtDbError("BAD_REQUEST", "first cannot be combined with distinct");
    }
    if (q.first && q.aggregate !== undefined) {
      throw new RtDbError("BAD_REQUEST", "first cannot be combined with aggregate");
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
    if (q.count && q.distinct) {
      throw new RtDbError("BAD_REQUEST", "count cannot be combined with distinct");
    }
    if (q.count && q.aggregate !== undefined) {
      throw new RtDbError("BAD_REQUEST", "count cannot be combined with aggregate");
    }
    // `distinct` is a standalone terminal like `count`: it rejects every other
    // terminal except `index`/`eq`/range bounds/`filter` (which compose by
    // narrowing the matching set). The `get`/`unique`/`first`/`count` peers
    // above already throw on `+distinct`; this branch covers the rest.
    if (q.distinct) {
      if (q.take !== undefined) {
        throw new RtDbError("BAD_REQUEST", "distinct cannot be combined with take");
      }
      if (q.order !== undefined) {
        throw new RtDbError("BAD_REQUEST", "distinct cannot be combined with order");
      }
      if (q.paginate !== undefined) {
        throw new RtDbError("BAD_REQUEST", "distinct cannot be combined with paginate");
      }
      if (q.search !== undefined) {
        throw new RtDbError("BAD_REQUEST", "distinct cannot be combined with search");
      }
      if (q.vectorSearch !== undefined) {
        throw new RtDbError("BAD_REQUEST", "distinct cannot be combined with vector search");
      }
      if (q.hybridSearch !== undefined) {
        throw new RtDbError("BAD_REQUEST", "distinct cannot be combined with hybrid search");
      }
      if (q.aggregate !== undefined) {
        throw new RtDbError("BAD_REQUEST", "distinct cannot be combined with aggregate");
      }
    }
    // `aggregate` is a standalone terminal like `distinct`: it rejects every
    // other terminal except `index`/`eq`/range bounds/`filter`. The
    // `get`/`unique`/`first`/`count`/`distinct` peers above already throw on
    // `+aggregate`; this branch covers the rest.
    if (q.aggregate !== undefined) {
      if (q.take !== undefined) {
        throw new RtDbError("BAD_REQUEST", "aggregate cannot be combined with take");
      }
      if (q.order !== undefined) {
        throw new RtDbError("BAD_REQUEST", "aggregate cannot be combined with order");
      }
      if (q.paginate !== undefined) {
        throw new RtDbError("BAD_REQUEST", "aggregate cannot be combined with paginate");
      }
      if (q.search !== undefined) {
        throw new RtDbError("BAD_REQUEST", "aggregate cannot be combined with search");
      }
      if (q.vectorSearch !== undefined) {
        throw new RtDbError("BAD_REQUEST", "aggregate cannot be combined with vector search");
      }
      if (q.hybridSearch !== undefined) {
        throw new RtDbError("BAD_REQUEST", "aggregate cannot be combined with hybrid search");
      }
    }
    if (q.paginate !== undefined) {
      // Combination guards mirror server `validate_query`: paginate is one-shot
      // paging, so it can't also narrow to count/unique/first/take. (`get` is
      // rejected above; `order`, index, eq, and range bounds are allowed.)
      if (q.count) {
        throw new RtDbError("BAD_REQUEST", "paginate cannot be combined with count");
      }
      if (q.distinct) {
        throw new RtDbError("BAD_REQUEST", "paginate cannot be combined with distinct");
      }
      if (q.aggregate !== undefined) {
        throw new RtDbError("BAD_REQUEST", "paginate cannot be combined with aggregate");
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

    // Vector-similarity terminal. Cascade mirror of server `execute_query`:
    // `vectorSearch` carries its own `limit` and does not compose with any other
    // terminal (including `take`). The in-memory replica does not rank by vector
    // distance, but the guard exists so the cascade agrees with the server —
    // callers learn about invalid combinations here instead of silently getting
    // the wrong (unranked) result and then a 400 from the real server.
    if (q.vectorSearch !== undefined) {
      if (
        q.index !== undefined ||
        eq.length > 0 ||
        hasRange ||
        q.order !== undefined ||
        q.unique ||
        q.first ||
        q.count ||
        q.distinct ||
        q.aggregate !== undefined ||
        q.paginate !== undefined ||
        q.filter !== undefined ||
        q.search !== undefined ||
        q.take !== undefined ||
        q.hybridSearch !== undefined
      ) {
        throw new RtDbError(
          "BAD_REQUEST",
          "vectorSearch cannot be combined with any other terminal",
        );
      }
      const vs = q.vectorSearch;
      const vectorDef = tableDef.indexes?.find((i) => i.name === vs.index && i.vector);
      if (!vectorDef) {
        throw new RtDbError("BAD_REQUEST", `vector index '${vs.index}' not found`);
      }
      // Validate the vector-search-level filter against declared fields once
      // (mirrors server `compile_filter` composed into the vector WHERE) via the
      // SAME evaluator `search` uses. The in-memory replica does not rank by
      // vector distance, so it returns filter-narrowed candidates in insertion
      // order (a deterministic stand-in that exercises the filter path); the
      // real server ranks by the index's distance metric.
      if (vs.filter) {
        validateFilter(vs.filter, new Set(Object.keys(tableDef.fields)));
      }
      const out: unknown[] = [];
      for (const row of this.rowsFor(q.table).values()) {
        if (vs.filter && !evalFilterExpr(vs.filter, row.doc)) {
          continue;
        }
        out.push(row.doc);
        if (out.length >= vs.limit) {
          break;
        }
      }
      return out;
    }

    // Hybrid search terminal. Cascade mirror of server `execute_query`:
    // `hybridSearch` carries its own `limit` and does not compose with any other
    // terminal (it IS the search+vector combination). The in-memory replica does
    // not rank by ts_rank or vector distance, but the guard exists so the cascade
    // agrees with the server.
    if (q.hybridSearch !== undefined) {
      if (
        q.index !== undefined ||
        eq.length > 0 ||
        hasRange ||
        q.order !== undefined ||
        q.unique ||
        q.first ||
        q.count ||
        q.distinct ||
        q.aggregate !== undefined ||
        q.paginate !== undefined ||
        q.filter !== undefined ||
        q.search !== undefined ||
        q.vectorSearch !== undefined ||
        q.take !== undefined
      ) {
        throw new RtDbError(
          "BAD_REQUEST",
          "hybridSearch cannot be combined with any other terminal",
        );
      }
      // No in-memory hybrid ranking; return an empty result rather than silently
      // misranking by falling through to the collect path.
      return [];
    }

    // Full-text search terminal. Cascade mirror of server `execute_query`:
    // `search` composes only with `take`; every other terminal is rejected. The
    // in-memory replica matches by `plainto_tsquery` token-AND (a deterministic
    // relevance stand-in, not `ts_rank`) so match/no-match agrees with the
    // server while the combination guards agree too.
    if (q.search !== undefined) {
      if (
        q.index !== undefined ||
        eq.length > 0 ||
        hasRange ||
        q.order !== undefined ||
        q.unique ||
        q.first ||
        q.count ||
        q.distinct ||
        q.aggregate !== undefined ||
        q.paginate !== undefined ||
        q.filter !== undefined ||
        q.vectorSearch !== undefined ||
        q.hybridSearch !== undefined
      ) {
        throw new RtDbError(
          "BAD_REQUEST",
          "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, aggregate, paginate, filter, or vector search",
        );
      }
      // Full-text matching (not ts_rank ordering): mirror `plainto_tsquery`
      // token-AND semantics closely enough that a unit test can assert
      // match/no-match. The query is tokenized into lowercase word lexemes and a
      // doc matches when every query lexeme appears in the concatenated text of
      // the search index's declared fields. Ranking is a deterministic stand-in
      // (query-lexeme frequency desc, then `created_at` desc, then `id` desc) —
      // exact `ts_rank` order is intentionally not replicated. `take` (already
      // capped to MAX_TAKE above) limits the result.
      const search = q.search;
      if (search.query.trim().length === 0) {
        throw new RtDbError("BAD_REQUEST", "search query text must not be empty");
      }
      const searchDef = tableDef.indexes?.find((i) => i.name === search.index && i.search);
      if (!searchDef) {
        throw new RtDbError("BAD_REQUEST", `search index '${search.index}' not found`);
      }
      // Validate the search-level filter against declared fields once (mirrors
      // server `compile_filter` composed into the search WHERE).
      if (search.filter) {
        validateFilter(search.filter, new Set(Object.keys(tableDef.fields)));
      }
      const queryTokens = [...new Set(ftsTokens(search.query))];
      const limit = q.take ?? MAX_TAKE;
      if (queryTokens.length === 0) {
        return [];
      }
      const scored: Array<{ row: StoredRow; score: number }> = [];
      for (const row of this.rowsFor(q.table).values()) {
        if (search.filter && !evalFilterExpr(search.filter, row.doc)) {
          continue;
        }
        const docTokens = ftsTokens(
          searchDef.fields.map((f) => ftsStringify(row.doc[f])).join(" "),
        );
        let score = 0;
        let allPresent = true;
        for (const qt of queryTokens) {
          let occurrences = 0;
          for (const dt of docTokens) {
            if (dt === qt) occurrences++;
          }
          if (occurrences === 0) {
            allPresent = false;
            break;
          }
          score += occurrences;
        }
        if (allPresent) scored.push({ row, score });
      }
      scored.sort((a, b) =>
        a.score !== b.score
          ? b.score - a.score
          : a.row.createdAt !== b.row.createdAt
            ? b.row.createdAt - a.row.createdAt
            : a.row.id > b.row.id
              ? -1
              : a.row.id < b.row.id
                ? 1
                : 0,
      );
      return scored.slice(0, limit).map((s) => this.mergeDoc(s.row));
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
    let rangeFieldPg: PgType | null = null;
    if (hasRange) {
      if (!indexDef) {
        throw new RtDbError("BAD_REQUEST", "range bound requires an index");
      }
      if (eqLen >= indexDef.fields.length) {
        throw new RtDbError("BAD_REQUEST", "range bound requires a remaining index field after eq");
      }
      rangeField = indexDef.fields[eqLen];
      rangeFieldPg = indexColumnType(tableDef.fields[rangeField]).pg;
    }

    const gt =
      q.gt !== undefined && rangeField ? coerceIndexValue(tableDef, rangeField, q.gt) : null;
    const gte =
      q.gte !== undefined && rangeField ? coerceIndexValue(tableDef, rangeField, q.gte) : null;
    const lt =
      q.lt !== undefined && rangeField ? coerceIndexValue(tableDef, rangeField, q.lt) : null;
    const lte =
      q.lte !== undefined && rangeField ? coerceIndexValue(tableDef, rangeField, q.lte) : null;

    // Validate the filter against declared fields once (mirrors server compile_filter).
    const fieldSet = new Set(Object.keys(tableDef.fields));
    if (q.filter) {
      validateFilter(q.filter, fieldSet);
    }
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
        if (gt !== null && compareIndexValues(v, gt, rangeFieldPg ?? undefined) <= 0) {
          continue;
        }
        if (gte !== null && compareIndexValues(v, gte, rangeFieldPg ?? undefined) < 0) {
          continue;
        }
        if (lt !== null && compareIndexValues(v, lt, rangeFieldPg ?? undefined) >= 0) {
          continue;
        }
        if (lte !== null && compareIndexValues(v, lte, rangeFieldPg ?? undefined) > 0) {
          continue;
        }
      }
      if (q.filter && !evalFilterExpr(q.filter, row.doc)) {
        continue;
      }
      filtered.push(row);
    }

    if (q.count) {
      return filtered.length;
    }

    // Distinct terminal: return the unique values of the index field immediately
    // after the eq prefix over the matching set. Server-side preconditions
    // (index set AND eqLen < fields.length) are mirrored here as BAD_REQUEST so
    // the in-memory cascade agrees with the server's accept/reject decision.
    if (q.distinct) {
      if (!indexDef) {
        throw new RtDbError("BAD_REQUEST", "distinct requires an index field beyond the eq prefix");
      }
      if (eqLen >= indexDef.fields.length) {
        throw new RtDbError("BAD_REQUEST", "distinct requires an index field beyond the eq prefix");
      }
      const field = indexDef.fields[eqLen];
      const fieldPg = indexColumnType(tableDef.fields[field]).pg;
      const seen = new Set<unknown>();
      const values: unknown[] = [];
      for (const row of filtered) {
        const v = row.doc[field];
        // Skip null/undefined index values — they cannot match a typed column
        // on the server (NULL filtering mirrors `WHERE "<col>" IS NOT NULL`).
        if (v === null || v === undefined) continue;
        const key =
          typeof v === "number" || typeof v === "string" || typeof v === "boolean"
            ? v
            : JSON.stringify(v);
        if (!seen.has(key)) {
          seen.add(key);
          values.push(v);
        }
      }
      values.sort((a, b) => compareIndexValues(a, b, fieldPg));
      return values.slice(0, MAX_TAKE);
    }

    // Aggregate terminal: run <OP> over the index field after the eq prefix
    // over the matching set. With `groupBy: true`, groups by that field and
    // aggregates the next one. `count` aggregates rows (COUNT(*)) and consumes
    // no aggregate field — a scalar `count` needs no index, a grouped `count`
    // needs one index field (the group key). Server-side preconditions (index
    // set, fields beyond prefix, sum/avg numeric) are mirrored here as
    // BAD_REQUEST so the in-memory cascade agrees with the server. Group count
    // is capped by MAX_TAKE. Empty matching set yields null for the scalar
    // sum/avg/min/max form (server `to_jsonb(SUM(empty))` → SQL NULL → JSON
    // null); a scalar `count` over zero rows yields 0 (COUNT(*)).
    if (q.aggregate !== undefined) {
      const isNumeric = (fieldName: string): boolean => {
        const ft = tableDef.fields[fieldName];
        // `number` and `int64` are the numeric indexable types; an optional
        // wrapper unwraps to its inner type. Mirrors server `is_numeric_index_field`.
        if (!ft) return false;
        const tag = (ft as { type: string }).type;
        if (tag === "number" || tag === "int64") return true;
        if (tag === "optional") {
          const inner = (ft as { inner: { type: string } }).inner;
          return inner?.type === "number" || inner?.type === "int64";
        }
        return false;
      };
      const { op, groupBy = false } = q.aggregate;
      // `count` aggregates rows, not a field — it consumes no aggregate index
      // field (mirrors server `AggregateOp::needs_field`).
      const needsField = op !== "count";
      if (groupBy) {
        if (!indexDef || eqLen >= indexDef.fields.length) {
          throw new RtDbError(
            "BAD_REQUEST",
            "aggregate groupBy requires an index field beyond the eq prefix",
          );
        }
        const groupField = indexDef.fields[eqLen];
        const groupFieldPg = indexColumnType(tableDef.fields[groupField]).pg;
        let aggField: string | undefined;
        let aggFieldPg: PgType | undefined;
        if (needsField) {
          if (eqLen + 1 >= indexDef.fields.length) {
            throw new RtDbError(
              "BAD_REQUEST",
              "aggregate groupBy requires two index fields beyond the eq prefix",
            );
          }
          aggField = indexDef.fields[eqLen + 1];
          aggFieldPg = indexColumnType(tableDef.fields[aggField]).pg;
          if ((op === "sum" || op === "avg") && !isNumeric(aggField)) {
            throw new RtDbError("BAD_REQUEST", `aggregate op ${op} requires a numeric index field`);
          }
        }
        // Group rows by `groupField` value (skip null/undefined group keys —
        // the server's typed column excludes NULL), preserving first-seen order
        // and then sorting by key ascending for parity with the server's ORDER BY k.
        // `count` counts rows (one entry per row); else aggregate the field value.
        const groups = new Map<unknown, unknown[]>();
        for (const row of filtered) {
          const k = row.doc[groupField];
          if (k === null || k === undefined) continue;
          const entry = aggField !== undefined ? row.doc[aggField] : row;
          const existing = groups.get(k);
          if (existing) {
            existing.push(entry);
          } else {
            groups.set(k, [entry]);
          }
        }
        const out = Array.from(groups.entries())
          .map(([k, values]) => ({ key: k, value: applyAggregate(op, values, aggFieldPg) }))
          .sort((a, b) => compareIndexValues(a.key, b.key, groupFieldPg))
          .slice(0, MAX_TAKE);
        return out;
      }
      // Scalar: `count` needs no index/field (COUNT(*) over the matching set);
      // sum/avg/min/max require an aggregate field beyond the eq prefix.
      if (needsField) {
        if (!indexDef) {
          throw new RtDbError(
            "BAD_REQUEST",
            "aggregate requires an index field beyond the eq prefix",
          );
        }
        if (eqLen >= indexDef.fields.length) {
          throw new RtDbError(
            "BAD_REQUEST",
            "aggregate requires an index field beyond the eq prefix",
          );
        }
        const aggField = indexDef.fields[eqLen];
        const aggFieldPg = indexColumnType(tableDef.fields[aggField]).pg;
        if ((op === "sum" || op === "avg") && !isNumeric(aggField)) {
          throw new RtDbError("BAD_REQUEST", `aggregate op ${op} requires a numeric index field`);
        }
        const values = filtered
          .map((row) => row.doc[aggField])
          .filter((v) => v !== null && v !== undefined);
        // Empty set → null (matches server SUM/AVG/MIN/MAX over zero rows).
        return values.length === 0 ? null : applyAggregate(op, values, aggFieldPg);
      }
      // Scalar count: COUNT(*) over the matching set (0 when empty).
      return filtered.length;
    }

    // Sort keys: unbound index fields (after the eq prefix), then createdAt, id.
    // `sortPgs[i]` is the storage type of `sortKeys[i]` so the comparator can
    // pick the int64 numeric path for decimal-string fields. `__createdAt` is a
    // number column; `__id` is a text column on the server.
    const sortKeys: string[] = [];
    const sortPgs: PgType[] = [];
    if (indexDef) {
      for (const field of indexDef.fields.slice(eqLen)) {
        sortKeys.push(field);
        sortPgs.push(indexColumnType(tableDef.fields[field]).pg);
      }
    }
    sortKeys.push("__createdAt");
    sortPgs.push("number");
    sortKeys.push("__id");
    sortPgs.push("text");

    const dir: Order = q.order ?? "asc";
    filtered.sort((a, b) => {
      for (let i = 0; i < sortKeys.length; i++) {
        const field = sortKeys[i];
        const av = field === "__createdAt" ? a.createdAt : field === "__id" ? a.id : a.doc[field];
        const bv = field === "__createdAt" ? b.createdAt : field === "__id" ? b.id : b.doc[field];
        const cmp = compareIndexValues(av, bv, sortPgs[i]);
        if (cmp !== 0) {
          return dir === "desc" ? -cmp : cmp;
        }
      }
      return 0;
    });

    if (q.paginate !== undefined) {
      return this.paginateResult(q.paginate, tableDef, filtered, sortKeys, sortPgs, dir);
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
   * `id` tiebreaker means no row is skipped or duplicated across pages).
   * `sortPgs[i]` is the storage type of `sortKeys[i]` so the resume predicate
   * uses the same int64-aware comparator as the producing sort. */
  private paginateResult(
    paginate: Paginate,
    tableDef: TableJson,
    sorted: StoredRow[],
    sortKeys: string[],
    sortPgs: PgType[],
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
      rows = sorted.filter((row) => this.isAfterCursor(row, cursorValues, sortKeys, sortPgs, dir));
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
    sortPgs: PgType[],
    dir: Order,
  ): boolean {
    for (let i = 0; i < sortKeys.length; i++) {
      let prefixEqual = true;
      for (let j = 0; j < i; j++) {
        if (
          compareIndexValues(this.sortValue(row, sortKeys[j]), cursorValues[j], sortPgs[j]) !== 0
        ) {
          prefixEqual = false;
          break;
        }
      }
      if (!prefixEqual) {
        continue;
      }
      const cmp = compareIndexValues(this.sortValue(row, sortKeys[i]), cursorValues[i], sortPgs[i]);
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

/** The single database name the in-memory harness models (it is single-db).
 *  Surfaced on audit rows and the subscription inspector for shape parity with
 *  `RtDbAdminClient`, which reads per-db data on a real multi-db server. */
const IN_MEMORY_DB = "db";

/** Terminal step kind a query resolves to (the `terminal` field the server
 *  reports on `GET /admin/subscriptions`). Best-effort derivation from the
 *  query JSON — sufficient for shape parity in the harness. */
function queryTerminal(q: QueryJson): string {
  if (q.get !== undefined) return "get";
  if (q.count) return "count";
  if (q.first) return "first";
  if (q.unique) return "unique";
  if (q.distinct) return "distinct";
  if (q.aggregate !== undefined) return "aggregate";
  if (q.paginate !== undefined) return "paginate";
  if (q.search !== undefined) return "search";
  if (q.vectorSearch !== undefined) return "vectorSearch";
  if (q.hybridSearch !== undefined) return "hybridSearch";
  return "collect";
}

/** Invalidation class the committer would assign (the `readSetClass` field the
 *  server reports): point reads, indexed (eq/range) reads, ordered (take/first)
 *  reads, else table-level. Best-effort — the harness does not track the real
 *  skip logic, only the shape. */
function queryReadSetClass(q: QueryJson): string {
  if (q.get !== undefined) return "point";
  const hasIndex =
    q.index !== undefined ||
    (q.eq?.length ?? 0) > 0 ||
    q.gt !== undefined ||
    q.gte !== undefined ||
    q.lt !== undefined ||
    q.lte !== undefined;
  if (q.order !== undefined || q.first || q.take !== undefined) return "ordered";
  if (hasIndex) return "indexed";
  return "table";
}

/** In-memory admin surface mirroring `RtDbAdminClient`: a seedable durable
 *  audit log (`getAudit`) and the live subscription inspector
 *  (`listSubscriptions`). Bound to an `InMemoryRtDbClient`'s clock and
 *  subscription registry so admin-keyed consumers (audit backstops, quota
 *  inspection) are unit-testable with no network. The harness does not track
 *  invalidation counters, so those totals read zero; the audit log is seedable
 *  rather than auto-recorded (tests assert the read shape, not DocOp
 *  provenance). */
export class InMemoryAdminClient {
  private readonly auditLog: AuditEntry[] = [];
  private auditSeq = 0;

  constructor(private readonly backplane: { now: () => number; subs: readonly Subscription[] }) {}

  /** Seed audit rows directly (test affordance). `id` auto-increments when
   *  omitted; `tsMs` defaults to the host clock; `op`/`principal` default to
   *  null and the rest to empty strings, matching the `AuditEntry` shape. */
  seedAudit(rows: Array<Partial<AuditEntry>>): void {
    for (const row of rows) {
      const id = row.id ?? ++this.auditSeq;
      if (row.id !== undefined && row.id > this.auditSeq) this.auditSeq = row.id;
      this.auditLog.push({
        id,
        tsMs: row.tsMs ?? this.backplane.now(),
        db: row.db ?? IN_MEMORY_DB,
        table: row.table ?? "",
        op: row.op ?? null,
        docId: row.docId ?? "",
        principal: row.principal ?? null,
        source: row.source ?? "client",
      });
    }
  }

  /** Durable audit-log entries, newest-first, mirroring `GET /admin/audit`.
   *  Each of `db`/`table`/`op`/`principal`/`source` is an optional equality
   *  filter (combined AND); `limit`/`offset` page (defaults 100/0). */
  getAudit(opts?: GetAuditOptions): Promise<AuditEntry[]> {
    let rows = this.auditLog;
    if (opts?.db) rows = rows.filter((r) => r.db === opts.db);
    if (opts?.table) rows = rows.filter((r) => r.table === opts.table);
    if (opts?.op) rows = rows.filter((r) => r.op === opts.op);
    if (opts?.principal) rows = rows.filter((r) => r.principal === opts.principal);
    if (opts?.source) rows = rows.filter((r) => r.source === opts.source);
    const sorted = [...rows].sort((a, b) => (a.tsMs !== b.tsMs ? b.tsMs - a.tsMs : b.id - a.id));
    const offset = opts?.offset ?? 0;
    const limit = opts?.limit ?? 100;
    return Promise.resolve(sorted.slice(offset, offset + limit));
  }

  /** Live subscription inspector, mirroring `GET /admin/subscriptions`. Returns
   *  the currently-registered queries as `SubscriptionInfo` rows plus per-db
   *  rollup. Invalidation totals read zero (the harness does not track them). */
  listSubscriptions(opts?: { db?: string }): Promise<SubscriptionsResponse> {
    if (opts?.db && opts.db !== IN_MEMORY_DB) {
      return Promise.resolve(this.emptySubscriptions());
    }
    const subscriptions: SubscriptionInfo[] = this.backplane.subs.map((s) => ({
      db: IN_MEMORY_DB,
      table: s.table,
      terminal: queryTerminal(s.query),
      readSetClass: queryReadSetClass(s.query),
      principal: null,
    }));
    const perDb: DbSubCounters[] = subscriptions.length
      ? [
          {
            db: IN_MEMORY_DB,
            reruns: 0,
            skipsPoint: 0,
            skipsIndexed: 0,
            skipsOrdered: 0,
            missed: 0,
          },
        ]
      : [];
    return Promise.resolve({
      subscriptions,
      subsRerunsTotal: 0,
      subsSkipsPointTotal: 0,
      subsSkipsIndexedTotal: 0,
      subsSkipsOrderedTotal: 0,
      subsMissedPushesTotal: 0,
      perDb,
    });
  }

  /** Active interactive sessions. The harness mints no sessions, so this always
   *  reads empty — mirroring `RtDbAdminClient.listSessions` for admin consumers
   *  unit-tested with no network. */
  listSessions(_filter?: { user?: string; limit?: number }): Promise<SessionInfo[]> {
    return Promise.resolve([]);
  }

  /** Revoke one session by token hash. No-op in-memory; resolves void. */
  revokeSession(_tokenHash: string): Promise<void> {
    return Promise.resolve();
  }

  /** Revoke every session for a user. No-op in-memory; reports zero revoked. */
  revokeUserSessions(_userId: string): Promise<{ ok: boolean; revoked: number }> {
    return Promise.resolve({ ok: true, revoked: 0 });
  }

  private emptySubscriptions(): SubscriptionsResponse {
    return {
      subscriptions: [],
      subsRerunsTotal: 0,
      subsSkipsPointTotal: 0,
      subsSkipsIndexedTotal: 0,
      subsSkipsOrderedTotal: 0,
      subsMissedPushesTotal: 0,
      perDb: [],
    };
  }
}
