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
  distinct?: boolean;
  aggregate?: AggregateSpec;
  paginate?: Paginate;
  filter?: FilterExpr;
  search?: SearchQuery;
  vectorSearch?: VectorQuery;
  hybridSearch?: HybridSearchQuery;
}

export interface Paginate {
  cursor?: string;
  numItems: number;
}

/**
 * Mirrors server `query::FilterExpr` byte-for-byte: internally tagged by `op`
 * (lowercase variant names), `deny_unknown_fields`. Leaves compare one declared
 * field to a value (`in` to a non-empty list); `and`/`or` nest arbitrarily;
 * `not` wraps a nested expr; `contains` tests membership of `value` in
 * `doc.field[]` (reverse of `in`); `exists` tests the field is present and
 * non-null.
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
  | { op: "or"; exprs: FilterExpr[] }
  | { op: "not"; expr: FilterExpr }
  | { op: "contains"; field: string; value: unknown }
  | { op: "exists"; field: string };

/** Mirrors server `query::SearchMode` byte-for-byte (lowercase variants).
 * `tsquery` is the default (word/stem full-text); `trgm` is case-insensitive
 * substring matching ranked by trigram similarity. An omitted `mode` means
 * `tsquery`; the server and clients never emit an explicit `"tsquery"`. */
export type SearchMode = "tsquery" | "trgm";

/** Mirrors server `query::SearchQuery` byte-for-byte (camelCase, deny_unknown_fields).
 * `filter` narrows search results server-side via the db-side `FilterExpr` DSL
 * (the full type — not vector search's eq-only map); `mode` selects the match
 * strategy (`tsquery` default | `trgm` substring); `snippet` (FM-31) opts each
 * hit into a `_searchSnippet` ts_headline fragment (`<mark>`-wrapped,
 * server-bounded; tsquery mode only) — all omitted on the wire when absent so
 * existing search requests stay byte-identical. */
export interface SearchQuery {
  index: string;
  query: string;
  filter?: FilterExpr;
  mode?: SearchMode;
  snippet?: boolean;
}

/** Mirrors server `query::AggregateOp` byte-for-byte (lowercase variants).
 * `count` aggregates rows (consumes no aggregate field): a scalar `count` is the
 * number of matching rows; a grouped `count` (`groupBy: true`) is the count per
 * group. */
export type AggregateOp = "sum" | "avg" | "min" | "max" | "count";

/** Mirrors server `query::AggregateSpec` byte-for-byte (camelCase, deny_unknown_fields).
 * `groupBy` defaults false — the server emits `false` even when unset (it uses
 * `#[serde(default)]`, not a skip predicate), but we omit it on the wire when
 * false to match the SDK's omit-when-default convention; the server accepts
 * either form (the field is `#[serde(default)]`). */
export interface AggregateSpec {
  op: AggregateOp;
  groupBy?: boolean;
}

/** Mirrors server `query::AggregateGroup` byte-for-byte (camelCase). One row
 * from a grouped `aggregate` (`groupBy: true`) terminal. */
export interface AggregateGroup {
  key: unknown;
  value: unknown;
}

/** Mirrors server `query::VectorSearchQuery` byte-for-byte (camelCase, deny_unknown_fields).
 * `filter` narrows vector-search results server-side via the db-side `FilterExpr` DSL
 * (the full type — the same one `search` and the query-level `.filter()` use, not an
 * eq-only map over the index's `filterFields`); omitted on the wire when absent so
 * existing requests stay byte-identical. */
export interface VectorQuery {
  index: string;
  vector: number[];
  limit: number;
  filter?: FilterExpr;
}

/** Mirrors server `query::HybridSearchQuery` byte-for-byte (camelCase,
 * deny_unknown_fields). Fuses full-text (`search`) and vector (`vectorSearch`)
 * ranking via Reciprocal Rank Fusion; the table must declare BOTH a search
 * index and a vector index. `searchIndex`/`vectorIndex` optionally name the
 * indexes (auto-selected when omitted); `k` is the RRF constant (default 60,
 * omitted on the wire when absent). */
export interface HybridSearchQuery {
  query: string;
  vector: number[];
  limit: number;
  searchIndex?: string;
  vectorIndex?: string;
  k?: number;
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

// ---- Workflow wire types (FM-29) --------------------------------------------
//
// Mirror server `protocol`'s workflow family byte-for-byte (camelCase,
// deny_unknown_fields; optional fields omitted on the wire when absent).
// See `server/src/protocol.rs` for the authoritative shapes.

/** Per-step retry policy. `maxAttempts` counts TOTAL attempts — the first try
 * included. Defaults when a step omits `retry` (server `StepRetry::default`):
 * 3 attempts, 1s initial backoff doubling to a 60s cap. */
export interface StepRetry {
  maxAttempts: number;
  initialRetryMs?: number;
  maxRetryMs?: number;
}

/** One workflow step: an ordinary `Transaction` plus policy. The txn may
 * itself carry `schedule`/`cancelSchedule` steps (FM-28 rules apply). */
export interface WorkflowStepSpec {
  txn: TransactionJson;
  retry?: StepRetry;
  sleepBeforeMs?: number;
}

/** A submitted workflow definition. Stored verbatim per run — a run snapshots
 * its spec, so template edits never drift a live run. */
export interface WorkflowSpec {
  name: string;
  steps: WorkflowStepSpec[];
}

/** Run lifecycle, snake_case on the wire (mirrors server `WorkflowStatus`). */
export type WorkflowStatus = "pending" | "running" | "success" | "failed" | "cancelled";

/** Terminal record for one step: completed successfully, or exhausted its
 * retries (`status: "failed"`). Individual retried attempts are NOT recorded —
 * the `attempts` count carries them. `error` omitted unless failed. */
export interface StepOutcome {
  stepIndex: number;
  status: "success" | "failed";
  attempts: number;
  at: number;
  error?: string;
}

/** List/get projection of one run. `sleepUntil`/`lastError`/`startedAt`/
 * `finishedAt` omitted on the wire when absent. */
export interface WorkflowInfo {
  id: string;
  name: string;
  status: WorkflowStatus;
  currentStep: number;
  stepCount: number;
  attempts: number;
  sleepUntil?: number;
  lastError?: string;
  createdAt: number;
  updatedAt: number;
  startedAt?: number;
  finishedAt?: number;
}

/** `GET /admin/db/{db}/workflows/{id}` shape: the info row flattened (server
 * `#[serde(flatten)]`) plus the per-step outcome trail. */
export interface WorkflowInfoFull extends WorkflowInfo {
  stepOutcomes: StepOutcome[];
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
  | { type: "distinct"; value: unknown[] }
  | { type: "aggregate"; value: unknown }
  | { type: "aggregateGroups"; value: AggregateGroup[] }
  | { type: "paginated"; value: PaginatedResultJson };

/**
 * Mirrors server `http_api::BatchQueryOutcome`. `result` is the raw untagged
 * `QueryResult` value (the server serializes `QueryResult` with
 * `#[serde(untagged)]`, so the on-wire form is the bare value — `null`, a doc,
 * an array of docs, a count number, a `{docs,nextCursor}`, etc. — matching how
 * `RtDbHttpClient.query<R>` types its return as a caller-chosen `R`). A batch
 * spans terminals, so the caller narrows each slot. `error` reuses the standard
 * `{code, message}` envelope.
 */
export type BatchQueryOutcomeJson =
  | { ok: true; result: unknown }
  | { ok: false; error: { code: string; message: string } };

/** Mirrors server `txn::Step` (tag `op`, camelCase; document steps carry
 * `table`, the control-flow steps — `schedule`/`cancelSchedule` (FM-28) and
 * `startWorkflow`/`cancelWorkflow` (FM-29) — do not). */
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
    }
  | {
      op: "patchByQuery";
      table: string;
      filter: FilterExpr;
      patch: Record<string, unknown>;
      limit?: number;
    }
  | {
      op: "deleteByQuery";
      table: string;
      filter: FilterExpr;
      limit?: number;
    }
  | { op: "schedule"; when: ScheduleWhen; txn: TransactionJson }
  | { op: "cancelSchedule"; id: string }
  | { op: "startWorkflow"; spec: WorkflowSpec }
  | { op: "cancelWorkflow"; id: string };

export interface TransactionJson {
  steps: StepJson[];
}

// ---- Migration wire types ---------------------------------------------------
//
// Mirror server `migrate::*` byte-for-byte: the `Directive` enum (tag `op`,
// camelCase, `deny_unknown_fields` — the same shape contract as `StepJson`),
// `Cast`, `MigrateRequest`, `MigrateResult`, `DirectiveReport`, `CastFailure`,
// `SampleChange`. See `server/src/migrate.rs` for the authoritative shapes.

/** Mirrors server `migrate::Cast` (camelCase): the closed set of sound coercions
 * accepted by `Directive::ChangeType`. */
export type Cast = "toString" | "toNumber" | "toInt64" | "toBoolean";

// ---- ValueExpr (ENH-020 / SEC-107) ------------------------------------------
//
// Mirror server `migrate::ValueExpr` byte-for-byte: a closed, typed expression
// grammar for `Directive::EvalExpr`'s backfill expression. Internally tagged by
// `op` (camelCase variant names), `deny_unknown_fields` — the same shape
// contract as `FilterExpr`. There is deliberately no subquery node, no
// function-call-by-name node, and no raw-SQL escape; the only way to reach raw
// SQL is the deprecated `Legacy(String)` arm of `EvalExpr.expr`, gated to the
// root admin_key. See `server/src/migrate.rs` for the authoritative shapes.

/** One branch of `ValueExprJson.case`. Mirrors server `migrate::CaseWhen`
 * (camelCase, deny_unknown_fields): `{ when: FilterExpr, then: ValueExpr }`. */
export interface CaseWhenJson {
  when: FilterExpr;
  then: ValueExprJson;
}

/** Mirrors server `migrate::ValueExpr` byte-for-byte (tag `op`, camelCase,
 * `deny_unknown_fields`). The 14 variants: field/literal (leaves), concat/add/
 * sub/mul/div/coalesce (n-ary and binary composites), lower/upper/trim (text),
 * cast (scalar coercion reusing {@link Cast}), now (current timestamp), and
 * case (conditional, whose `when` arms are {@link FilterExpr} predicates). */
export type ValueExprJson =
  | { op: "field"; field: string }
  | { op: "literal"; value: unknown }
  | { op: "concat"; parts: ValueExprJson[] }
  | { op: "add"; left: ValueExprJson; right: ValueExprJson }
  | { op: "sub"; left: ValueExprJson; right: ValueExprJson }
  | { op: "mul"; left: ValueExprJson; right: ValueExprJson }
  | { op: "div"; left: ValueExprJson; right: ValueExprJson }
  | { op: "coalesce"; parts: ValueExprJson[] }
  | { op: "lower"; value: ValueExprJson }
  | { op: "upper"; value: ValueExprJson }
  | { op: "trim"; value: ValueExprJson }
  | { op: "cast"; value: ValueExprJson; to: Cast }
  | { op: "now" }
  | { op: "case"; whens: CaseWhenJson[]; otherwise: ValueExprJson };

/** Mirrors server `migrate::Directive` byte-for-byte (tag `op`, camelCase,
 * `deny_unknown_fields`). `evalExpr.where` is the wire alias for the server's
 * `where_clause` field (serde `rename = "where"`).
 *
 * `evalExpr` is dual-accept (ENH-020, structurally closing SEC-107): `expr` is
 * either a typed {@link ValueExprJson} (the safe, all-literals-bound path) or a
 * legacy raw-SQL string (deprecated, gated to the root admin_key on the server);
 * `where` is either a typed {@link FilterExpr} or a legacy raw-SQL predicate
 * string. The two sources may not mix — typed `expr` requires typed `where`,
 * legacy `expr` requires legacy `where`. The legacy string form is accepted for
 * one deprecation cycle, then removed. */
export type DirectiveJson =
  | { op: "renameField"; table: string; from: string; to: string }
  | { op: "renameTable"; from: string; to: string }
  | {
      op: "changeType";
      table: string;
      field: string;
      to: FieldTypeJson;
      cast: Cast;
      default?: unknown;
    }
  | { op: "dropField"; table: string; field: string }
  | { op: "dropTable"; name: string }
  | { op: "dropIndex"; table: string; name: string }
  | { op: "setDefault"; table: string; field: string; value: unknown }
  | {
      op: "evalExpr";
      table: string;
      set: string;
      expr: ValueExprJson | string;
      where?: FilterExpr | string;
    };

/** Mirrors server `migrate::MigrateRequest` (camelCase; `dryRun` is
 * `#[serde(default)]` false). */
export interface MigrateRequestJson {
  directives: DirectiveJson[];
  dryRun?: boolean;
}

/** Mirrors server `migrate::CastFailure` (camelCase). */
export interface CastFailureJson {
  id: string;
  value: unknown;
}

/** Mirrors server `migrate::SampleChange` (camelCase). */
export interface SampleChangeJson {
  id: string;
  before: unknown;
  after: unknown;
}

/** Mirrors server `migrate::DirectiveReport` (camelCase). `castFailures` and
 * `sampleChanges` are `skip_serializing_if = "Vec::is_empty"` on the server, so
 * they surface as optional on the wire (absent when empty). */
export interface DirectiveReportJson {
  op: string;
  affectedRows: number;
  castFailures?: CastFailureJson[];
  sampleChanges?: SampleChangeJson[];
}

/** Mirrors server `migrate::MigrateResult` (camelCase). `schema` is the
 * post-migration derived schema — returned even on `dryRun` (with
 * `applied: false`), so a caller can preview the resulting shape. */
export interface MigrateResultJson {
  applied: boolean;
  schema: SchemaJson;
  directives: DirectiveReportJson[];
}

/** Mirrors server `schema_history::HistorySummary` (camelCase). One row in
 * `GET /admin/db/{db}/schema/history` — no `schema` blob, just the metadata
 * needed to pick a version to inspect or restore. `source` is the event that
 * captured the snapshot: `push` (push-schema), `migrate`, or `restore`. */
export interface SchemaHistoryEntrySummary {
  version: number;
  capturedAt: number;
  source: "push" | "migrate" | "restore";
  principal: string | null;
}

/** Mirrors server `schema_history::HistoryEntry` (camelCase). Adds the full
 * `schema` blob — returned by `GET /admin/db/{db}/schema/history/{version}`. */
export interface SchemaHistoryEntry extends SchemaHistoryEntrySummary {
  schema: SchemaJson;
}

/**
 * Whether an `AuthedUser` resolved from an interactive OAuth session or a
 * per-database machine token. Mirrors server `protocol::UserKind` (ARC-009):
 * narrows the prior unbounded `string` so TS consumers get exhaustiveness.
 */
export type AuthedUserKind = "user" | "machine";

export interface AuthedUser {
  kind: AuthedUserKind;
  email?: string | null;
  name?: string | null;
  /** GitHub login. Absent on the wire for machine tokens / non-GitHub users. */
  githubLogin?: string | null;
  /** GitHub numeric id, paired with `githubLogin`. */
  githubId?: number | null;
}

/** One entry in a presence room's member list. Mirrors server
 * `protocol::PresenceMember` byte-for-byte (camelCase). `connectionId` is the
 * opaque, unique-per-session key; `user` carries display identity; `state` is
 * an opaque client-supplied blob (always present on the wire — the server
 * serializes `serde_json::Value` even when it is JSON `null`). */
export interface PresenceMember {
  connectionId: string;
  user: AuthedUser;
  state: unknown;
}

/** Client -> server WS vocabulary. Tags/fields match server `protocol::ClientMessage`. */
export type ClientMessage =
  | { type: "auth"; token?: string; db: string }
  | { type: "subscribe"; queryId: string; query: QueryJson }
  | { type: "unsubscribe"; queryId: string }
  | { type: "mutate"; mutId: string; idempotencyKey?: string; txn: TransactionJson }
  | { type: "schedule"; scheduleId: string; when: ScheduleWhen; txn: TransactionJson }
  | { type: "cancelSchedule"; scheduleId: string; id: string }
  | { type: "pauseSchedule"; scheduleId: string; id: string }
  | { type: "resumeSchedule"; scheduleId: string; id: string }
  | { type: "listSchedules"; scheduleId: string }
  | { type: "startWorkflow"; workflowId: string; spec: WorkflowSpec }
  | { type: "cancelWorkflow"; workflowId: string; id: string }
  | { type: "listWorkflows"; workflowId: string; status?: WorkflowStatus }
  | { type: "presence"; room: string; state?: unknown }
  | { type: "presenceState"; room: string; state: unknown; ttlMs?: number }
  | { type: "leavePresence"; room: string }
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
  | { type: "startWorkflowOk"; workflowId: string; info: WorkflowInfo }
  | { type: "startWorkflowErr"; workflowId: string; error: RtDbErrorEnvelope }
  | { type: "workflowAck"; workflowId: string; ok: boolean; error?: RtDbErrorEnvelope }
  | { type: "listWorkflowsOk"; workflowId: string; workflows: WorkflowInfo[] }
  | { type: "presenceSnapshot"; room: string; members: PresenceMember[] }
  | { type: "presenceErr"; room: string; error: RtDbErrorEnvelope }
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

/** Distance metric for a vector index. Lives on the index spec, not the query.
 * `cosine` is the default and is omitted on the wire so existing schemas serialize
 * unchanged. */
export type DistanceMetric = "cosine" | "l2" | "ip";

/** Mirrors server `schema::VectorIndexSpec` byte-for-byte (camelCase). `filterFields`
 * is omitted on the wire when the index declares none. `metric` is omitted when it
 * equals the default `cosine`. */
export interface VectorIndexSpec {
  dimensions: number;
  filterFields?: string[];
  metric?: DistanceMetric;
}

export interface IndexJson {
  name: string;
  fields: string[];
  /** `true` marks a full-text search index; omitted on the wire for ordinary btree indexes. */
  search?: boolean;
  /** Present marks a vector index; omitted otherwise. */
  vector?: VectorIndexSpec;
  /** `true` compiles to `CREATE UNIQUE INDEX` over the declared `fields` (no
   * trailing tiebreaker column — uniqueness is on `fields` only). Omitted when
   * false so existing schemas deserialize unchanged. Btree-only (rejected
   * alongside `search`/`vector` by the server). */
  unique?: boolean;
  /** Optional partial-index predicate (`CREATE INDEX … WHERE`). Same `FilterExpr`
   * type as the query-time `filter()` terminal. Omitted when absent. Wire key is
   * `where` (Rust keyword ⇒ raw identifier on the server). */
  where?: FilterExpr;
  /** Optional Postgres `regconfig` name (e.g. `"english"`, `"simple"`,
   * `"spanish"`) selecting the FTS dictionary the server uses to tsvectorize
   * this index's `fields`. Omitted on the wire when absent (the server default
   * behaves as `english`). Search-only. */
  language?: string;
}

/** Declarative document TTL (auto-expiry). `field` names a declared numeric
 * field whose value is each document's absolute epoch-ms expiry;
 * `defaultDurationMs` stamps the field at insert time when the document omits
 * it. Mirrors the server `TtlDef` (camelCase wire keys). Omitted on the wire
 * when the table has no TTL. */
export interface TtlDef {
  field: string;
  defaultDurationMs?: number;
}

export interface TableJson {
  fields: Record<string, FieldTypeJson>;
  indexes?: IndexJson[];
  /** Opt-in per-row authorization: names a declared string-compatible field
   * whose value is the owning user's id. Server-enforced; clients only declare
   * it. Omitted on the wire when unset. */
  ownerField?: string;
  /** Opt-in extension of `ownerField`: names a declared array-of-strings (or
   * array-of-id) field whose values are additional user ids that may
   * read/mutate the row (owner OR collaborator). May be declared alone.
   * Omitted on the wire when unset. */
  collaboratorsField?: string;
  /** Opt-in document TTL: names a declared numeric field whose value is each
   * document's absolute epoch-ms expiry. `defaultDurationMs` stamps the field
   * at insert time when the document omits it. Server-enforced; clients only
   * declare it. Omitted on the wire when unset. */
  ttl?: TtlDef;
  /** Opt-in per-row authorization predicate (Model C). A general `FilterExpr`
   * over this table's declared doc fields and the principal's markers
   * (`{"$user":true}` / `{"$email":true}`). Enforced on the same
   * read/write/subscription seams as `ownerField`; additive to it. Marker
   * values are valid only here — client `.filter()` queries reject them.
   * Server-enforced; clients only declare it. Omitted on the wire when unset. */
  authorize?: FilterExpr;
  /** Field-level default values (FM-32): a map of declared field name →
   * literal JSON value stamped onto a NEW document that omits the key (insert,
   * replace, and upsert's insert branch — never patch, upsert-update, or
   * patchByQuery). Push-time-validated server-side (each key must be a declared
   * field of the table; values non-null and matching the field's type);
   * clients only declare it. Omitted on the wire when the table declares
   * none. */
  defaults?: Record<string, unknown>;
}

export interface SchemaJson {
  tables: Record<string, TableJson>;
}
