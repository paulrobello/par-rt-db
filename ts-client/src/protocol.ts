import type { RtDbErrorEnvelope } from "./errors.js";

export type Order = "asc" | "desc";

/** Mirrors server `query::Query` (serde `deny_unknown_fields`). */
export interface QueryJson {
  table: string;
  get?: string;
  index?: string;
  eq?: unknown[];
  gt?: unknown;
  gte?: unknown;
  lt?: unknown;
  lte?: unknown;
  order?: Order;
  take?: number;
  unique?: boolean;
  first?: boolean;
  count?: boolean;
  paginate?: Paginate;
  filter?: FilterExpr;
  search?: SearchQuery;
  vectorSearch?: VectorQuery;
}

export interface Paginate {
  cursor?: string;
  numItems: number;
}

/**
 * Mirrors server `query::FilterExpr` byte-for-byte: internally tagged by `op`
 * (lowercase variant names), `deny_unknown_fields`. Leaves compare one declared
 * field to a value (`in` to a non-empty list); `and`/`or` nest arbitrarily.
 */
export type FilterExpr =
  | { op: "eq"; field: string; value: unknown }
  | { op: "neq"; field: string; value: unknown }
  | { op: "gt"; field: string; value: unknown }
  | { op: "gte"; field: string; value: unknown }
  | { op: "lt"; field: string; value: unknown }
  | { op: "lte"; field: string; value: unknown }
  | { op: "in"; field: string; values: unknown[] }
  | { op: "and"; exprs: FilterExpr[] }
  | { op: "or"; exprs: FilterExpr[] };

/** Mirrors server `query::SearchQuery` byte-for-byte (camelCase, deny_unknown_fields). */
export interface SearchQuery {
  index: string;
  query: string;
}

/** Mirrors server `query::VectorSearchQuery` byte-for-byte (camelCase, deny_unknown_fields).
 * `filter` is an eq-map over the index's declared `filterFields`; omitted on the wire when empty. */
export interface VectorQuery {
  index: string;
  vector: number[];
  limit: number;
  filter?: Record<string, unknown>;
}

/** Mirrors server `protocol::ScheduleWhen` byte-for-byte (tag `type`, camelCase). */
export type ScheduleWhen =
  | { type: "afterMs"; ms: number }
  | { type: "runAt"; ms: number }
  | { type: "cron"; expr: string };

/** Mirrors server `protocol::ScheduleInfo` (camelCase; `cron`/`lastError` omitted when absent). */
export interface ScheduleInfo {
  id: string;
  kind: "oneshot" | "cron";
  dueAt: number;
  cron?: string;
  status: "pending" | "running" | "paused" | "error";
  lastError?: string;
  createdAt: number;
  firedCount: number;
}

/** Mirrors server `PaginatedResult` (cursor-based pagination). */
export interface PaginatedResultJson {
  docs: unknown[];
  nextCursor?: string;
}

export type QueryResultJson =
  | { type: "doc"; value: unknown | null }
  | { type: "docs"; value: unknown[] }
  | { type: "count"; value: number }
  | { type: "paginated"; value: PaginatedResultJson };

/** Mirrors server `txn::Step` (tag `op`, every step carries `table`). */
export type StepJson =
  | { op: "insert"; table: string; doc: Record<string, unknown> }
  | { op: "patch"; table: string; id: string; fields: Record<string, unknown> }
  | { op: "replace"; table: string; id: string; doc: Record<string, unknown> }
  | { op: "delete"; table: string; id: string }
  | { op: "expectVersion"; table: string; id: string; version: number }
  | { op: "expectAbsent"; table: string; index: string; eq: unknown[] }
  | {
      op: "upsert";
      table: string;
      index: string;
      eq: unknown[];
      insert: Record<string, unknown>;
      patch: Record<string, unknown>;
    };

export interface TransactionJson {
  steps: StepJson[];
}

export interface AuthedUser {
  kind: string;
  email?: string | null;
  name?: string | null;
  /** GitHub login. Absent on the wire for machine tokens / non-GitHub users. */
  githubLogin?: string | null;
  /** GitHub numeric id, paired with `githubLogin`. */
  githubId?: number | null;
}

/** Client -> server WS vocabulary. Tags/fields match server `protocol::ClientMessage`. */
export type ClientMessage =
  | { type: "auth"; token: string; db: string }
  | { type: "subscribe"; queryId: string; query: QueryJson }
  | { type: "unsubscribe"; queryId: string }
  | { type: "mutate"; mutId: string; idempotencyKey?: string; txn: TransactionJson }
  | { type: "schedule"; scheduleId: string; when: ScheduleWhen; txn: TransactionJson }
  | { type: "cancelSchedule"; scheduleId: string; id: string }
  | { type: "pauseSchedule"; scheduleId: string; id: string }
  | { type: "resumeSchedule"; scheduleId: string; id: string }
  | { type: "listSchedules"; scheduleId: string }
  | { type: "ping" };

/** Server -> client WS vocabulary. Tags/fields match server `protocol::ServerMessage`. */
export type ServerMessage =
  | { type: "authOk"; user: AuthedUser }
  | { type: "authErr"; error: RtDbErrorEnvelope }
  | { type: "queryUpdate"; queryId: string; result: unknown }
  | { type: "mutateOk"; mutId: string; results: unknown[] }
  | { type: "mutateErr"; mutId: string; error: RtDbErrorEnvelope }
  | { type: "subscribeErr"; queryId: string; error: RtDbErrorEnvelope }
  | { type: "scheduleOk"; scheduleId: string; id: string }
  | { type: "scheduleErr"; scheduleId: string; error: RtDbErrorEnvelope }
  | { type: "scheduleAck"; scheduleId: string; ok: boolean; error?: RtDbErrorEnvelope }
  | { type: "listSchedulesOk"; scheduleId: string; schedules: ScheduleInfo[] }
  | { type: "pong" };

/** Mirrors server `schema::FieldType` (tag `type`). */
export type FieldTypeJson =
  | { type: "string" }
  | { type: "number" }
  | { type: "boolean" }
  | { type: "null" }
  | { type: "id"; table: string }
  | { type: "literal"; value: string | number | boolean }
  | { type: "optional"; inner: FieldTypeJson }
  | { type: "union"; variants: FieldTypeJson[] }
  | { type: "array"; element: FieldTypeJson }
  | { type: "object"; fields: Record<string, FieldTypeJson> }
  | { type: "int64" }
  | { type: "bytes" }
  | { type: "any" }
  | { type: "record"; value: FieldTypeJson }
  | { type: "vector"; dimensions: number };

/** Mirrors server `schema::VectorIndexSpec` byte-for-byte (camelCase). `filterFields`
 * is omitted on the wire when the index declares none. */
export interface VectorIndexSpec {
  dimensions: number;
  filterFields?: string[];
}

export interface IndexJson {
  name: string;
  fields: string[];
  /** `true` marks a full-text search index; omitted on the wire for ordinary btree indexes. */
  search?: boolean;
  /** Present marks a vector index; omitted otherwise. */
  vector?: VectorIndexSpec;
}

export interface TableJson {
  fields: Record<string, FieldTypeJson>;
  indexes?: IndexJson[];
}

export interface SchemaJson {
  tables: Record<string, TableJson>;
}
