/**
 * In-memory implementation of the par-rt-db client for unit tests — the client
 * core (mirrors `rust-client/src/in_memory/mod.rs`): stored rows, the
 * transaction executor, schedules/workflows/storage/presence, and the admin
 * surface. The query engine lives in `./query.ts`, the migration engine in
 * `./migrate.ts`, and value/filter validation in `./validate.ts`; the public
 * surface of the former `in_memory.ts` monolith is re-exported from
 * `./index.ts`.
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

import type {
  AuditEntry,
  DbSubCounters,
  GetAuditOptions,
  MergeReport,
  SessionInfo,
  SubscriptionInfo,
  SubscriptionsResponse,
} from "../admin.js";
import { RtDbError } from "../errors.js";
import type { FileMetadata, UploadInput, UploadResult } from "../http.js";
import { parseStepResults, type StepResult } from "../mutation.js";
import type {
  AuthedUser,
  DirectiveReportJson,
  FieldTypeJson,
  FilterExpr,
  MigrateRequestJson,
  MigrateResultJson,
  PresenceMember,
  QueryJson,
  ScheduleInfo,
  ScheduleWhen,
  SchemaJson,
  StepOutcome,
  StepRetry,
  TableJson,
  TransactionJson,
  WorkflowInfo,
  WorkflowInfoFull,
  WorkflowSpec,
  WorkflowStatus,
} from "../protocol.js";
import type { RtQuery } from "../query.js";
import type { SchemaDefinition } from "../schema.js";
import {
  applyMigrationDirective,
  detectDestructiveChanges,
  onDeleteRef,
  validateOnDelete,
} from "./migrate.js";
import { executeQuery, requireIndex } from "./query.js";
import {
  clone,
  coerceIndexValue,
  evalFilterExpr,
  isBase64String,
  isHexId,
  isInt64String,
  isPlainObject,
  validateFilter,
} from "./validate.js";

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

export const MAX_STEPS = 1024;
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
      step.op === "schedule" ||
      step.op === "cancelSchedule" ||
      step.op === "startWorkflow" ||
      step.op === "cancelWorkflow"
    ) {
      continue; // control-flow: touches no documents (server counts 0)
    }
    if (step.op === "patchByQuery" || step.op === "deleteByQuery") {
      total += Math.min(step.limit ?? MAX_BY_QUERY_ROWS, MAX_BY_QUERY_ROWS);
    } else {
      total += 1;
    }
  }
  return total;
}

/** FM-28/FM-29: recursive step count — a `schedule` step counts as itself plus
 * every step in its nested txn; a `startWorkflow` step counts as itself plus
 * every step of every txn in its spec. Mirrors the server's recursive gate
 * against `MAX_STEPS` (a nested tree can't smuggle past the flat cap). */
function countSteps(txn: TransactionJson): number {
  let total = txn.steps.length;
  for (const step of txn.steps) {
    if (step.op === "schedule") total += countSteps(step.txn);
    if (step.op === "startWorkflow") {
      for (const s of step.spec.steps) total += countSteps(s.txn);
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

/** FM-29: hard cap on steps per workflow spec. Mirrors server
 * `workflows::MAX_WORKFLOW_STEPS`. */
const MAX_WORKFLOW_STEPS = 64;

/** FM-29: retry policy applied when a step spec omits `retry`. Mirrors server
 * `protocol::StepRetry::default`. */
const DEFAULT_STEP_RETRY: Required<StepRetry> = {
  maxAttempts: 3,
  initialRetryMs: 1_000,
  maxRetryMs: 60_000,
};

/** FM-29: exponential backoff after the `attempts`-th failure of a step —
 * `initialRetryMs * 2^(attempts-1)` (shift capped at 32), clamped to
 * `maxRetryMs`. Mirrors server `workflows::backoff_ms`. */
function backoffMs(retry: Required<StepRetry>, attempts: number): number {
  const shift = Math.min(attempts - 1, 32);
  return Math.min(retry.initialRetryMs * 2 ** shift, retry.maxRetryMs);
}

/** FM-29: submit-time spec validation. Mirrors server `workflows::validate_spec`
 * (same checks, same BAD_REQUEST messages, including the recursive
 * `MAX_STEPS` gate over the spec's step txns). */
function validateWorkflowSpec(spec: WorkflowSpec): void {
  if (spec.steps.length === 0) {
    throw new RtDbError("BAD_REQUEST", "workflow must have at least one step");
  }
  if (spec.steps.length > MAX_WORKFLOW_STEPS) {
    throw new RtDbError("BAD_REQUEST", `workflow exceeds ${MAX_WORKFLOW_STEPS} steps`);
  }
  for (const [i, step] of spec.steps.entries()) {
    if (step.retry) {
      if (step.retry.maxAttempts === 0) {
        throw new RtDbError("BAD_REQUEST", `steps[${i}].retry.maxAttempts must be >= 1`);
      }
      const initial = step.retry.initialRetryMs ?? DEFAULT_STEP_RETRY.initialRetryMs;
      const max = step.retry.maxRetryMs ?? DEFAULT_STEP_RETRY.maxRetryMs;
      if (initial === 0 || max < initial) {
        throw new RtDbError(
          "BAD_REQUEST",
          `steps[${i}].retry requires initialRetryMs > 0 and maxRetryMs >= initialRetryMs`,
        );
      }
    }
  }
  const total = spec.steps.reduce((sum, s) => sum + countSteps(s.txn), 0);
  if (total > MAX_STEPS) {
    throw new RtDbError(
      "BAD_REQUEST",
      `workflow recursive step count ${total} exceeds MAX_STEPS ${MAX_STEPS}`,
    );
  }
}

/** FM-29: a stored workflow run in the in-memory harness. Field names mirror
 * the server's `workflows` table columns; `sleepUntil` is always set (the
 * column is NOT NULL server-side — insert computes the initial gate, later
 * transitions overwrite it, terminal states leave it). */
interface WorkflowRun {
  id: string;
  spec: WorkflowSpec;
  status: WorkflowStatus;
  currentStep: number;
  attempts: number;
  sleepUntil: number;
  lastError?: string;
  createdAt: number;
  updatedAt: number;
  startedAt?: number;
  finishedAt?: number;
  stepOutcomes: StepOutcome[];
}

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

function toSchemaJson(schema: SchemaDefinition<any> | SchemaJson): SchemaJson {
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

/** Applies the table's push-time-validated `defaults` (FM-32) to a NEW
 * document: every key the doc omits is stamped from the schema. Runs after
 * `stampTtlDefault` (a ttl default on the same field wins — it stamped the key
 * first) and before any principal stamps (server-stamped owner/authorize
 * values win). Callers are exactly the new-document paths — insert, replace,
 * upsert-insert; `patch` (and upsert-update / patchByQuery) never re-apply, so
 * clearing an optional field stays cleared. Mirrors server `txn::apply_defaults`;
 * returns the same `doc` reference when no stamp is needed, otherwise a
 * shallow copy with the fields set. */
function applyDefaults(table: TableJson, doc: Record<string, unknown>): Record<string, unknown> {
  const defaults = table.defaults;
  if (!defaults) return doc;
  const missing = Object.keys(defaults).filter((field) => !(field in doc));
  if (missing.length === 0) return doc;
  const out: Record<string, unknown> = { ...doc };
  for (const field of missing) {
    // Clone so array/object defaults are never aliased into a doc (the server
    // clones the serde value too).
    out[field] = clone(defaults[field]);
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
  private readonly workflows = new Map<string, WorkflowRun>();
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
   * empty doc stores only for brand-new tables. Every push validates `onDelete`
   * declarations (FM-33, SCHEMA_VIOLATION) like the server does. */
  pushSchema(schema: SchemaDefinition<any> | SchemaJson): void {
    const next = toSchemaJson(schema);
    validateOnDelete(next);
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
    // FM-28: a schedule/cancelSchedule step mutates the schedule store, so a
    // failed txn must roll it back with the docs (the server inserts/deletes
    // the scheduled_txns row on the open sqlx tx, which the rollback aborts).
    // FM-29: same for startWorkflow/cancelWorkflow and the workflow store.
    const schedulesSnapshot = new Map(this.schedules);
    const workflowsSnapshot = new Map(this.workflows);
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
    } catch (error) {
      // Atomicity: any step's error rolls back everything already applied.
      this.restoreTables(snapshot);
      this.schedules.clear();
      for (const [id, job] of schedulesSnapshot) {
        this.schedules.set(id, job);
      }
      this.workflows.clear();
      for (const [id, run] of workflowsSnapshot) {
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
    return this.scheduleJob(txn, when);
  }

  /** Sync core of {@link schedule} — the body is synchronous, and the
   * `Step::Schedule` transaction step (FM-28) reuses it from the sync
   * `executeStep` path. */
  private scheduleJob(txn: TransactionJson, when: ScheduleWhen): { id: string } {
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
    if (!job || job.status !== "pending") {
      return false;
    }
    job.status = "paused";
    return true;
  }

  /** Resumes a paused job. Resolves `false` when the job is missing or not
   * paused — the server's `scheduler::set_paused(false)` contract. */
  async resumeSchedule(id: string): Promise<boolean> {
    const job = this.schedules.get(id);
    if (!job || job.status !== "paused") {
      return false;
    }
    job.status = "pending";
    return true;
  }

  async listSchedules(): Promise<ScheduleInfo[]> {
    return [...this.schedules.values()].map((job) => this.toScheduleInfo(job));
  }

  // ---- workflows (FM-29) ------------------------------------------------------

  /** Starts a durable workflow run from `spec`, validating it like the server's
   * `workflows::validate_spec`. Resolves the new run's info row (pending,
   * gated at `now + steps[0].sleepBeforeMs`); `tick` advances it. */
  async startWorkflow(spec: WorkflowSpec): Promise<WorkflowInfo> {
    validateWorkflowSpec(spec);
    return this.toWorkflowInfo(this.startWorkflowJob(spec));
  }

  /** Cancels a pending/running run: flips it to `cancelled` + `finishedAt`.
   * Resolves `false` (a no-op, not an error) for an unknown or terminal run —
   * the server's `workflows::cancel` contract. */
  async cancelWorkflow(id: string): Promise<boolean> {
    const run = this.workflows.get(id);
    if (!run || (run.status !== "pending" && run.status !== "running")) {
      return false;
    }
    run.status = "cancelled";
    run.finishedAt = this.now();
    run.updatedAt = this.now();
    return true;
  }

  /** Lists runs, newest first (createdAt DESC), optionally filtered by
   * `status` — the server `workflows::list` ordering. */
  async listWorkflows(status?: WorkflowStatus): Promise<WorkflowInfo[]> {
    return [...this.workflows.values()]
      .filter((run) => status === undefined || run.status === status)
      .sort((a, b) => b.createdAt - a.createdAt)
      .map((run) => this.toWorkflowInfo(run));
  }

  /** Fetches one full run row — info plus the per-step outcome trail
   * (mirrors `GET /admin/db/{db}/workflows/{id}`). NOT_FOUND on unknown id. */
  async getWorkflow(id: string): Promise<WorkflowInfoFull> {
    const run = this.workflows.get(id);
    if (!run) {
      throw new RtDbError("NOT_FOUND", `workflow '${id}' not found`);
    }
    return { ...this.toWorkflowInfo(run), stepOutcomes: [...run.stepOutcomes] };
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

  private toWorkflowInfo(run: WorkflowRun): WorkflowInfo {
    return {
      id: run.id,
      name: run.spec.name,
      status: run.status,
      currentStep: run.currentStep,
      stepCount: run.spec.steps.length,
      attempts: run.attempts,
      sleepUntil: run.sleepUntil,
      createdAt: run.createdAt,
      updatedAt: run.updatedAt,
      ...(run.lastError === undefined ? {} : { lastError: run.lastError }),
      ...(run.startedAt === undefined ? {} : { startedAt: run.startedAt }),
      ...(run.finishedAt === undefined ? {} : { finishedAt: run.finishedAt }),
    };
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

  /** Fires every due non-paused job by applying its txn through the same atomic
   * path as `mutate` (so reactive subscriptions see the write). One-shots are
   * removed after a successful fire; crons are re-armed. Pass `nowMs` to drive
   * the clock deterministically; omit it to use the client's injected clock.
   *
   * FM-29: also advances due workflow runs (pending + `sleepUntil <= now`)
   * through the server's `handle_workflow_advance` semantics — claim to
   * running, execute the current step txn atomically, then success/retry/
   * exhaust transitions (see `advanceWorkflow`).
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
    // FM-29: claim due pending runs (server `claim_due`), then advance each.
    const due = [...this.workflows.values()].filter(
      (run) => run.status === "pending" && run.sleepUntil <= now,
    );
    for (const run of due) {
      run.status = "running";
      if (run.startedAt === undefined) {
        run.startedAt = now;
      }
      run.updatedAt = now;
      this.advanceWorkflow(run, now);
    }
    return this.reapTtl(now);
  }

  /** FM-29: drives one claimed run across step boundaries, mirroring the
   * server committer's advance loop. Success on the last step finalizes;
   * success earlier moves to the next step and applies its `sleepBeforeMs`
   * gate (a future gate re-pends the run and ends this pass — a `now` gate
   * continues in the same tick); failure re-pends with exponential backoff
   * (no outcome recorded for retried attempts) or, once attempts are
   * exhausted, marks the run failed with a terminal outcome. */
  private advanceWorkflow(run: WorkflowRun, now: number): void {
    for (;;) {
      // Per-boundary liveness check: a cancel (or terminal transition) between
      // steps ends the pass — the server re-checks the row each boundary.
      const live = this.workflows.get(run.id);
      if (live !== run || run.status !== "running") {
        return;
      }
      const step = run.spec.steps[run.currentStep];
      if (!step) {
        return;
      }
      let execError: string | null = null;
      try {
        this.executeTransaction(step.txn);
      } catch (e) {
        execError = e instanceof Error ? e.message : String(e);
      }
      if (execError === null) {
        const outcome: StepOutcome = {
          stepIndex: run.currentStep,
          status: "success",
          attempts: run.attempts + 1,
          at: now,
        };
        const isLast = run.currentStep + 1 >= run.spec.steps.length;
        run.stepOutcomes.push(outcome);
        run.updatedAt = now;
        if (isLast) {
          run.status = "success";
          run.attempts = 0;
          delete run.lastError;
          run.finishedAt = now;
          return;
        }
        run.currentStep += 1;
        run.attempts = 0;
        const next = run.spec.steps[run.currentStep];
        const gate = now + (next.sleepBeforeMs ?? 0);
        if (gate > now) {
          run.status = "pending";
          run.sleepUntil = gate;
          run.updatedAt = now;
          return;
        }
        continue;
      }
      const retry: Required<StepRetry> = step.retry
        ? {
            maxAttempts: step.retry.maxAttempts,
            initialRetryMs: step.retry.initialRetryMs ?? DEFAULT_STEP_RETRY.initialRetryMs,
            maxRetryMs: step.retry.maxRetryMs ?? DEFAULT_STEP_RETRY.maxRetryMs,
          }
        : DEFAULT_STEP_RETRY;
      run.attempts += 1;
      if (run.attempts < retry.maxAttempts) {
        run.status = "pending";
        run.sleepUntil = now + backoffMs(retry, run.attempts);
        run.updatedAt = now;
        return;
      }
      run.stepOutcomes.push({
        stepIndex: run.currentStep,
        status: "failed",
        attempts: run.attempts,
        at: now,
        error: execError,
      });
      run.status = "failed";
      run.lastError = execError;
      run.finishedAt = now;
      run.updatedAt = now;
      return;
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
    extraTables?: string[];
  } {
    // The schedule control-flow steps (FM-28) target the scheduler, not a
    // table. Cancel mirrors the server's standalone op: `cancelled: false`
    // (not an error) when the id is missing or already fired/cancelled.
    if (step.op === "schedule") {
      const { id } = this.scheduleJob(step.txn, step.when);
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
      const cancelled = !!run && (run.status === "pending" || run.status === "running");
      if (cancelled) {
        run.status = "cancelled";
        run.finishedAt = this.now();
        run.updatedAt = this.now();
      }
      return { result: { cancelled } };
    }
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
    const stamped = applyDefaults(tableDef, stampTtlDefault(tableDef, doc, this.now()));
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
    const stamped = applyDefaults(tableDef, doc);
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
            const merged = applyPatch(childTableDef, childRow.doc, { [fieldName]: null });
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
    validateFilter(filter, tableDef);
    const limit = Math.min(limitOpt ?? MAX_BY_QUERY_ROWS, MAX_BY_QUERY_ROWS);
    const matched: StoredRow[] = [];
    for (const row of this.rowsFor(tableName).values()) {
      if (row.deletedAt !== undefined) continue; // FM-33: stamped rows are absent
      if (evalFilterExpr(filter, row.doc, tableDef.fields)) {
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
    return executeQuery(q, this.requireTable(q.table), (t) => this.rowsFor(t));
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
            skips: 0,
            rerunRatio: 0,
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

  /** Anon→real account merge. No-op in-memory; resolves an empty report
   *  (nothing re-stamped, nothing repointed, no anon row deleted) — mirroring
   *  `RtDbAdminClient.mergeUsers` for admin consumers unit-tested with no
   *  network. */
  mergeUsers(_anonUserId: string, _realUserId: string): Promise<MergeReport> {
    return Promise.resolve({
      dbs: {},
      storageRepointed: 0,
      sessionsRepointed: 0,
      anonDeleted: false,
    });
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
