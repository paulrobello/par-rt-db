// Admin API data contract — mirrors server/src/admin.rs, op_feed.rs, metrics.rs.
// Keep field names in sync with the server's serde output (camelCase).

import type { AuthedUser, QueryJson, TransactionJson } from "@par-rt-db/client";

export type { AuthedUser, QueryJson, TransactionJson };

export type OpKind = "insert" | "patch" | "replace" | "delete";

export interface OpEvent {
  db: string;
  table: string;
  docId: string;
  kind: OpKind;
  ts: number;
  owner?: string | null;
}

export interface LatencyStats {
  p50: number;
  p95: number;
  p99: number;
}
export interface MetricsSnapshot {
  queriesTotal: number;
  mutationsTotal: number;
  uploadsTotal: number;
  wsConnections: number;
  activeSubscriptions: number;
  poolSize: number;
  poolIdle: number;
  uptimeSeconds: number;
  queryLatency: LatencyStats;
  mutateLatency: LatencyStats;
  subscribeLatency: LatencyStats;
  /**
   * Subscription-invalidation effectiveness. Counted per subscription whose
   * table was written, so `reruns + skips` is the number of read-set decisions
   * made; a skip means the read set proved every written document irrelevant.
   * Skips are split by the class that proved it: `point` = `get(id)` reads,
   * `indexed` = count/collect/unique over an eq-prefix window, `ordered` =
   * take/first/paginate bounded by a top-N sort boundary.
   */
  subsRerunsTotal: number;
  subsSkipsPointTotal: number;
  subsSkipsIndexedTotal: number;
  subsSkipsOrderedTotal: number;
  /**
   * Sampled shadow verifications of skips (server-side `RTDB_SUBS_VERIFY_SKIP_EVERY`),
   * and how many found the skip was WRONG. `subsMissedPushesTotal > 0` means
   * invalidation under-approximated and a realtime update would have been
   * dropped — a correctness defect, not a tuning signal.
   */
  subsSkipVerificationsTotal: number;
  subsMissedPushesTotal: number;
}

export interface TableStat {
  name: string;
  rowCount: number;
  sizeBytes: number;
}

export interface DbStats {
  tables: TableStat[];
  totalSizeBytes: number;
}

export interface AdminMember {
  email: string;
  githubId?: number;
}

export interface HotConfig {
  allowedOrigins: string[];
  sessionTtlDays: number;
  maxFileSize: number;
  idempotencyTtlMs: number;
}

export interface ConfigResponse {
  port: number;
  publicUrl: string;
  githubBaseUrl: string;
  githubApiUrl: string;
  databaseUrlConfigured: boolean;
  adminKeyConfigured: boolean;
  githubConfigured: boolean;
  googleConfigured: boolean;
  gitlabConfigured: boolean;
  oidcConfigured: boolean;
  hot: HotConfig;
  version: string;
  gitCommit: string;
  admins: AdminMember[];
}

export interface TokenRow {
  id: string;
  name: string;
  createdAt: number;
  revoked: boolean;
  /** Server always emits these three; `null` means "no limit" (full access). */
  expiresAt: number | null;
  readOnly: boolean;
  tables: string[] | null;
}

// File storage — mirrors server/src/storage.rs `FileMeta`. Field names are
// camelCase on the wire (serde `rename_all = "camelCase"`). `contentType` is
// omitted by the server when the upload supplied no Content-Type header.
export interface FileMeta {
  id: string;
  sha256: string;
  size: number;
  contentType?: string;
  creationTime: number;
}

export interface RtDbErrorEnvelope {
  code: string;
  message: string;
}

export interface AdminQueryResult {
  result: unknown;
}
export interface AdminMutateResult {
  results: unknown[];
}

// Schema preview diff — mirrors server/src/schema_diff.rs (camelCase wire).
// `previewSchema` returns this; `added` lists new tables/columns/indexes the
// additive-only push will create, `rejected` lists drops and type changes the
// DDL layer will refuse.
export interface ColumnAdd {
  name: string;
  fieldType: string;
}
export interface IndexAdd {
  name: string;
  fields: string[];
}
export interface TableAdd {
  table: string;
  columns: ColumnAdd[];
  indexes: IndexAdd[];
}
export interface Rejection {
  table: string;
  item: string;
  reason: string;
}
export interface SchemaDiff {
  added: TableAdd[];
  rejected: Rejection[];
}

// Scheduled jobs — mirrors server/src/protocol.rs (ScheduleInfo, ScheduleWhen,
// ScheduleKind, ScheduleStatus). Field names are camelCase on the wire.
export type ScheduleKind = "oneshot" | "cron";
export type ScheduleStatus = "pending" | "running" | "paused" | "error";

export interface ScheduleInfo {
  id: string;
  kind: ScheduleKind;
  dueAt: number;
  cron?: string;
  status: ScheduleStatus;
  lastError?: string;
  createdAt: number;
  firedCount: number;
}

/** Wire shape for the `when` field of a create-schedule request. Mirrors the
 *  server's `ScheduleWhen` (tagged union, `type` discriminator). */
export type ScheduleWhen =
  | { type: "afterMs"; ms: number }
  | { type: "runAt"; ms: number }
  | { type: "cron"; expr: string };

export interface HotConfigPatch {
  allowedOrigins?: string[];
  sessionTtlDays?: number;
  maxFileSize?: number;
  idempotencyTtlMs?: number;
}

// Webhooks — mirrors server/src/webhook.rs (Webhook, DeliveryRow) and the
// ts-client's admin.ts shapes. Field names are camelCase on the wire.
// `table: null` means "all tables"; `events` carries op names
// (`insert`/`patch`/`replace`/`delete`/`upsert`) or `["*"]` for all.
export interface Webhook {
  id: number;
  db: string;
  table: string | null;
  url: string;
  events: string[];
  createdAt: number;
  enabled: boolean;
}

// `payload` is the raw JSON body the worker POSTs — typed `unknown` because the
// server passes it through verbatim. `status` is one of
// `pending|retrying|delivered|failed` (free-form here to tolerate future states).
export interface WebhookDelivery {
  id: number;
  attempts: number;
  status: string;
  nextAttempt: number;
  lastError: string | null;
  payload: unknown;
}

export interface CreateWebhookOptions {
  url: string;
  table?: string | null;
  events?: string[];
  enabled?: boolean;
}

// Tri-state `table`: omitted = unchanged, `null` = clear to all-tables,
// string = set. Other fields are plain optional — present sets, absent keeps.
export interface EditWebhookOptions {
  url?: string;
  table?: string | null;
  events?: string[];
  enabled?: boolean;
}

export interface ListDeliveriesOptions {
  status?: string;
  limit?: number;
  offset?: number;
}

// Audit log — mirrors server/src/audit.rs (AuditEntry, camelCase wire) and the
// ts-client's admin.ts shape. `op` is null for rows the server could not label
// with a kind; `principal` is null for system-initiated writes (TTL reaper,
// scheduled jobs) where there is no interactive user.
export interface AuditEntry {
  id: number;
  tsMs: number;
  db: string;
  table: string;
  op: string | null;
  docId: string;
  principal: string | null;
  source: string;
}

/** Options for `getAudit`. All optional; omitted filters match all rows, and
 *  omitted paging falls back to server defaults (limit 100, offset 0). */
export interface GetAuditOptions {
  db?: string;
  table?: string;
  op?: string;
  principal?: string;
  source?: string;
  limit?: number;
  offset?: number;
}
