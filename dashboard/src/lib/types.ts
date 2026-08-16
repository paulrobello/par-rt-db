// Dashboard type hub.
//
// Wire-contract types are re-exported from `@par-rt-db/client` so the dashboard
// cannot drift from the SDK — the fifth-copy bug this file used to be (ARC-107).
// Only genuinely dashboard-local view shapes are declared here. The SDK
// (`ts-client/src/index.ts`) is the single source of truth for the wire contract.

import type { AdminStreamFrame, DbSubCounters, ScheduleInfo } from "@par-rt-db/client";

// ---------------------------------------------------------------------------
// Wire-contract types: re-exported from the SDK (single source of truth).
// ---------------------------------------------------------------------------

// Aliased re-exports — the dashboard's historical names for SDK types whose
// canonical name differs. `FileMeta` is the SDK's `FileMetadata`; `SessionRow`
// is the SDK's `SessionInfo` (same shape, dashboard-local name).
export type {
  AuditEntry,
  AuthedUser,
  CreateWebhookOptions,
  DbSubCounters,
  EditWebhookOptions,
  FileMetadata as FileMeta,
  GetAuditOptions,
  ListDeliveriesOptions,
  QueryJson,
  RtDbErrorEnvelope,
  ScheduleInfo,
  ScheduleWhen,
  SessionInfo as SessionRow,
  StepOutcome,
  SubscriptionInfo,
  SubscriptionsPrincipal,
  SubscriptionsResponse,
  TransactionJson,
  Webhook,
  WebhookDelivery,
  WorkflowInfo,
  WorkflowInfoFull,
  WorkflowSpec,
  WorkflowStatus,
  WorkflowStepSpec,
} from "@par-rt-db/client";

// ---------------------------------------------------------------------------
// Op feed — derived from the SDK's exported `AdminStreamFrame`.
//
// `OpEvent`/`OpEventKind` live in `admin.ts` but are not re-exported through the
// package entry point. `AdminStreamFrame` IS exported and carries `OpEvent` on
// its `"op"` arm, so deriving here means a new op variant (e.g. `upsert`)
// propagates automatically instead of silently drifting. `Record<OpKind, string>`
// in AppShell becomes exhaustive by construction — the compiler flags any missing
// variant.
// ---------------------------------------------------------------------------

export type OpEvent = Extract<AdminStreamFrame, { kind: "op" }>["event"];
export type OpKind = OpEvent["kind"];

// ---------------------------------------------------------------------------
// Schedule enums — derived from the exported `ScheduleInfo` (the SDK inlines
// these unions rather than naming them `ScheduleKind`/`ScheduleStatus`).
// ---------------------------------------------------------------------------

export type ScheduleKind = ScheduleInfo["kind"];
export type ScheduleStatus = ScheduleInfo["status"];

// ---------------------------------------------------------------------------
// Dashboard-local view types (not part of the wire contract).
// ---------------------------------------------------------------------------

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
  /** Per-db breakdown of the skip/re-run/missed counters above (ENH-010),
   *  sorted by db; absent/empty until a fan_out records a decision. */
  perDbSubs?: DbSubCounters[];
}

export interface TableStat {
  name: string;
  rowCount: number;
  sizeBytes: number;
}

// Richer than the SDK's `DbStats` (admin.ts): the dashboard surface carries the
// per-db ENH-011 quota/usage fields (`tablesQuota`/`storageQuotaBytes`/`subsQuota`
// and their `*Used` counters) that the server includes on `/admin/dbs/{db}/stats`.
export interface DbStats {
  tables: TableStat[];
  totalSizeBytes: number;
  tablesQuota: number;
  tablesUsed: number;
  storageQuotaBytes: number;
  storageUsedBytes: number;
  subsQuota: number;
  subsUsed: number;
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
  maxTablesPerDb: number;
  maxStorageBytesPerDb: number;
  maxSubsPerDb: number;
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

export interface HotConfigPatch {
  allowedOrigins?: string[];
  sessionTtlDays?: number;
  maxFileSize?: number;
  idempotencyTtlMs?: number;
  maxTablesPerDb?: number;
  maxStorageBytesPerDb?: number;
  maxSubsPerDb?: number;
}
