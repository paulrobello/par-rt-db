import type { WebSocketLike } from "../client.js";
import type { FilterExpr } from "../protocol.js";

/**
 * Options for configuring the `RtDbAdminClient`.
 */
export interface RtDbAdminClientOptions {
  /**
   * The base URL of the Realtime Database server admin API.
   */
  url: string;
  /** Instance admin key. When set, every request carries
   *  `Authorization: Bearer <adminKey>` (the CLI/automation path). When omitted,
   *  the client authenticates via the HttpOnly `rtdb_session` cookie: each
   *  request is sent with `credentials: "include"` and the readable
   *  `rtdb-admin-csrf` nonce is echoed as `X-Rtdb-Csrf` (the header the server's
   *  admin-CSRF guard requires on cookie-authenticated mutating `/admin/*`
   *  verbs). Cookie mode is how the operator dashboard — a same-origin SPA with
   *  no JS-readable admin key — consumes this client. */
  adminKey?: string;
  /**
   * Optional custom fetch implementation to override the global fetch.
   */
  fetch?: typeof fetch;
  /** Injectable WebSocket constructor (browser/Node/bun). Defaults to the global
   *  `WebSocket`. The second arg carries the WS subprotocol(s) — `/admin/stream`
   *  authenticates via the `rtdb-admin.<token>` subprotocol in bearer mode; in
   *  cookie mode the subprotocol is omitted (the browser attaches the session
   *  cookie to the same-origin upgrade). */
  webSocketFactory?: (url: string, protocols?: string | string[]) => WebSocketLike;
}

/**
 * Represents an administrative user member.
 */
export interface AdminMember {
  /**
   * The member's email address.
   */
  email: string;
  /**
   * The member's optional GitHub user ID.
   */
  githubId?: number;
}

/**
 * Represents statistics for a single database table.
 */
export interface TableStat {
  /**
   * The name of the table.
   */
  name: string;
  /**
   * The total number of rows in the table.
   */
  rowCount: number;
  /**
   * The size of the table in bytes.
   */
  sizeBytes: number;
}

/**
 * Combined statistics for a database.
 */
export interface DbStats {
  /**
   * An array of stats per table.
   */
  tables: TableStat[];
  /**
   * The total size of all tables in bytes.
   */
  totalSizeBytes: number;
  /** ENH-011 per-db quota/usage fields (0 = unlimited). Server always emits all six. */
  tablesQuota: number;
  tablesUsed: number;
  storageQuotaBytes: number;
  storageUsedBytes: number;
  subsQuota: number;
  subsUsed: number;
  /** Stable creation identity of the database (changes if the name is deleted and recreated). */
  creationId: string;
}
/**
 * A versioned, host-scoped capability attached to a machine token, tagged with
 * a camelCase `"type"` — currently `{"type":"parMeshV1","host":"macdaddy"}`.
 * `host` is lowercase `[a-z0-9][a-z0-9-_]{0,31}`. The server rejects unknown
 * capability types and extra fields; pass the shape through verbatim.
 */
export interface HostCapability {
  type: "parMeshV1";
  host: string;
}

/**
 * Metadata representing information about an administrative token.
 */
export interface TokenInfo {
  /**
   * Unique identifier of the token.
   */
  id: string;
  /**
   * User-supplied descriptive name for the token.
   */
  name: string;
  /**
   * Epoch timestamp when the token was created.
   */
  createdAt: number;
  /**
   * Flag indicating whether the token has been revoked.
   */
  revoked: boolean;
  /** Server always sends these three; `null` means "no limit" (full access). */
  expiresAt: number | null;
  readOnly: boolean;
  tables: string[] | null;
  /** Host machine authorization: the token's capability, when it carries one.
   *  The server omits the key entirely (`skip_serializing_if`) for legacy
   *  tokens — absent, never `null`. */
  hostCapability?: HostCapability;
}
/** One active interactive session from `GET /admin/sessions`. `tokenHash` is a
 *  non-reversible sha256 digest (the plaintext token is never stored), safe to
 *  surface to an admin and used to target a row for revoke. `email`/`name`/`login`
 *  are `null` when the user has none (e.g. an anonymous session). */
export interface SessionInfo {
  tokenHash: string;
  userId: string;
  email: string | null;
  /** The OAuth provider's display name, when one was supplied. Unlike `login`
   *  this is never a handle or an email fallback. */
  name: string | null;
  login: string | null;
  anonymous: boolean;
  createdAt: number;
  expiresAt: number;
}
/** A row skipped by the anon→real merge because the re-stamp would collide
 *  with an existing doc under a unique index (server `merge::MergeConflict`). */
export interface MergeConflict {
  table: string;
  id: string;
}
/** Per-database outcome of an anon→real merge: re-stamped-doc counts per
 *  table plus the rows skipped over unique-index conflicts. */
export interface MergeDbResult {
  tables: Record<string, number>;
  conflicts: MergeConflict[];
}
/** Full-instance anon→real merge outcome from `POST /admin/merge-users`:
 *  per-db doc re-stamps, storage blobs repointed, sessions repointed (an
 *  open WS or stored SDK token promotes to the real principal on its next
 *  op), and whether the anon user row was deleted. */
export interface MergeReport {
  dbs: Record<string, MergeDbResult>;
  storageRepointed: number;
  sessionsRepointed: number;
  anonDeleted: boolean;
}
/** Optional capabilities for `mintToken`. Omitted fields fall back to server
 *  defaults (full access: no expiry, read-write, all tables). */
export interface MintTokenOptions {
  expiresAt?: number;
  readOnly?: boolean;
  tables?: string[];
  /** Host machine authorization: attach a host capability to the minted
   *  token. Absence mints legacy semantics unchanged. */
  hostCapability?: HostCapability;
}

/** Request body for `createEnrollmentGrant` (POST /admin/enrollment-grants).
 *  `db`, `name`, `capability`, and `expiresAt` are required; `tokenExpiresAt`
 *  (ms epoch) is optional. */
export interface CreateEnrollmentGrantOptions {
  db: string;
  name: string;
  capability: HostCapability;
  expiresAt: number;
  tokenExpiresAt?: number;
}

/** Response from `createEnrollmentGrant`. `grant` is the one-time plaintext —
 *  shown once, never persisted in plaintext server-side. */
export interface EnrollmentGrant {
  grantId: string;
  grant: string;
  db: string;
  capability: HostCapability;
  expiresAt: number;
}

/** Response from `enroll` (POST /auth/enroll) after redeeming a grant. */
export interface EnrollResult {
  tokenId: string;
  token: string;
  db: string;
  capability: HostCapability;
}
/**
 * Percentile latency measurements for server operations.
 */
export interface LatencyStats {
  /**
   * The 50th percentile (median) latency in milliseconds.
   */
  p50: number;
  /**
   * The 95th percentile latency in milliseconds.
   */
  p95: number;
  /**
   * The 99th percentile latency in milliseconds.
   */
  p99: number;
}

/**
 * A snapshot of server-wide telemetry metrics.
 */
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
  /** Presence gauges and counters (ENH-015): distinct rooms, total sessions,
   *  `presence/update` frames processed, broadcasts fanned out, and sessions
   *  whose per-state TTL expired. Absent on older server builds. */
  presenceRooms?: number;
  presenceSessions?: number;
  /** Aggregate quota rejections by kind (ENH-011). Absent on older server builds. */
  quotaRejectionsTablesTotal?: number;
  quotaRejectionsStorageTotal?: number;
  quotaRejectionsSubsTotal?: number;
  /** SEC-109: total admin-key login failures (brute-force signal). Absent on
   *  older server builds. */
  adminAuthFailuresTotal?: number;
  /** Workflow step attempts by outcome (FM-29). Absent on older server builds. */
  workflowStepsSuccessTotal?: number;
  workflowStepsRetryTotal?: number;
  workflowStepsFailTotal?: number;
  /** Per-db rollup of the subscription counters above, when the server
   *  includes it on `/admin/metrics`. Absent on older server builds. */
  perDbSubs?: DbSubCounters[];
  /** Per-room presence inspector rows, when the server includes them on
   *  `/admin/metrics`. Absent on older server builds. */
  presenceDetail?: PresenceRoomInspect[];
  /** Per-db quota-rejection counter rows (ENH-011), when the server includes
   *  them on `/admin/metrics`. Absent on older server builds. */
  perDbQuota?: DbQuotaCounters[];
  /** Per-db workflow-run counts by status (FM-29), when the server includes
   *  them on `/admin/metrics`. Absent on older server builds. */
  perDbWorkflows?: DbWorkflowStatusCounts[];
}
/**
 * One room's live footprint — the rows of `GET /admin/presence` and
 * `presenceDetail` on `/admin/metrics`. In multi-instance mode
 * `memberCount`/`stateBytes` are merged with gossiped peer membership;
 * `localMemberCount` is always this replica's own count and
 * `mergedWithPeers` says whether the merge happened (eventually-consistent,
 * best-effort — never an authoritative total).
 */
export interface PresenceRoomInspect {
  room: string;
  /** Merged count: local plus deduped gossiped peers in multi-instance mode. */
  memberCount: number;
  /** This replica's own local member count, always (never merged). */
  localMemberCount: number;
  stateBytes: number;
  /** Local only — 0 for a room this replica has no local members in. */
  oldestMemberAgeMs: number;
  /** True when `memberCount`/`stateBytes` include gossiped peer membership. */
  mergedWithPeers: boolean;
}
/** One db's quota-rejection counters (`perDbQuota[]` on `GET /admin/metrics`). */
export interface DbQuotaCounters {
  db: string;
  tables: number;
  storage: number;
  subs: number;
}
/** One db's workflow-run counts by status (`perDbWorkflows[]` on `GET /admin/metrics`). */
export interface DbWorkflowStatusCounts {
  db: string;
  pending: number;
  running: number;
  waiting: number;
  success: number;
  failed: number;
  cancelled: number;
}
/**
 * `GET /admin/presence` response — per-replica live room inspector.
 */
export interface PresenceRoomsResponse {
  rooms: PresenceRoomInspect[];
}
/**
 * Represents the runtime configuration variables that can be hot-patched on the server.
 */
export interface HotConfig {
  allowedOrigins: string[];
  sessionTtlDays: number;
  maxFileSize: number;
  idempotencyTtlMs: number;
  /** Per-db resource quotas (ENH-011). Zero means "no limit" on the server. */
  maxTablesPerDb: number;
  maxStorageBytesPerDb: number;
  maxSubsPerDb: number;
  /** Change-feed retention: the newest N `changes` rows kept per db (server
   * default 100000; a patch value `> 0` clamps to `>= 1000`, `0` restores
   * the default). */
  changeLogMaxRows: number;
  /** Per-token rate-limit ceiling (requests/minute, fixed window); 0 =
   * unlimited. Hot-mutable via patchConfig. */
  perTokenRpm: number;
  /** Per-db aggregate rate-limit ceiling (requests/minute); 0 = unlimited. */
  perDbRpm: number;
}
/**
 * Read-only view of the server's `RTDB_STORAGE_REQUIRE_SIGNED_URLS` scope:
 * which databases require signed storage URLs (SEC-113).
 */
export interface StorageSignedUrls {
  /** `off` = none, `all` = every database, `databases` = only the listed ones. */
  mode: "off" | "all" | "databases";
  /** The listed databases when mode is `databases`; empty otherwise. */
  databases: string[];
}

/**
 * The full configuration profile response returned by the server.
 */
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
  /** Signed-URL scope (SEC-113), read-only. */
  storageSignedUrls: StorageSignedUrls;
}
/**
 * Patch object for updating server runtime configuration variables.
 */
export interface HotConfigPatch {
  allowedOrigins?: string[];
  sessionTtlDays?: number;
  maxFileSize?: number;
  idempotencyTtlMs?: number;
  /** Per-db resource quotas (ENH-011). Omit to leave unchanged. */
  maxTablesPerDb?: number;
  maxStorageBytesPerDb?: number;
  maxSubsPerDb?: number;
  /** Change-feed retention rows; `0` restores the server default, `> 0`
   * clamps to `>= 1000`. Omit to leave unchanged. */
  changeLogMaxRows?: number;
  /** Per-token rate-limit ceiling (requests/minute); 0 = unlimited. */
  perTokenRpm?: number;
  /** Per-db rate-limit ceiling (requests/minute); 0 = unlimited. */
  perDbRpm?: number;
}
/**
 * Union of valid operation event kinds recorded by the mutation/change log.
 */
export type OpEventKind = "insert" | "patch" | "replace" | "delete" | "upsert";

/**
 * An entry in the mutation event log representing a database change event.
 */
export interface OpEvent {
  db: string;
  table: string;
  docId: string;
  kind: OpEventKind;
  ts: number;
  owner?: string | null;
  /** 1-based monotonic sequence within the feed instance. The ring replays on
   *  every (re)connect — dedup replays by tracking the max `seq` seen per
   *  `feedEpoch`; a gap means evicted/dropped events, never a reordering. */
  seq: number;
  /** The feed instance's boot-time UUID identity. An epoch change means the
   *  counter reset (server restart), not dropped events. Dedup key:
   *  `(feedEpoch, seq)`. */
  feedEpoch: string;
}
/** A frame on the `/admin/stream` op-feed: a document op event (replay then live),
 *  or a ~1s server metrics snapshot. */
export type AdminStreamFrame =
  | { kind: "op"; event: OpEvent }
  | { kind: "gauges"; gauges: MetricsSnapshot };

/** One durable row from the audit log (`GET /admin/audit`). `op` is `null` for
 *  rows the server could not label with a kind; `principal` is `null` for
 *  system-initiated writes (TTL reaper, scheduled jobs) where there is no
 *  interactive user. */
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

/** Identity of a subscriber, when it has an interactive principal. `null` for
 *  machine tokens, scheduled jobs, and admin-bypass subscriptions — anything
 *  with no user identity. */
export interface SubscriptionsPrincipal {
  userId: string | null;
  email: string | null;
}

/** One live subscription from `GET /admin/subscriptions`. `terminal` is the
 *  query-DSL terminal step kind; `readSetClass` is the invalidation class the
 *  committer uses to decide skip-vs-rerun (point/indexed/ordered/table). */
export interface SubscriptionInfo {
  db: string;
  table: string;
  terminal: string;
  readSetClass: string;
  principal: SubscriptionsPrincipal | null;
}

/** Per-db rollup of subscription-invalidation counters (the same fields the
 *  server totals across all dbs on `SubscriptionsResponse`). `skips` is the
 *  total across the three classes and `rerunRatio` is
 *  `reruns / max(1, reruns + skips)` — in [0, 1]; sustained above 0.5 means
 *  re-runs dominate this db's fan-out (ENH-024). */
export interface DbSubCounters {
  db: string;
  reruns: number;
  skipsPoint: number;
  skipsIndexed: number;
  skipsOrdered: number;
  missed: number;
  skips: number;
  rerunRatio: number;
}

/** Response from `GET /admin/subscriptions`: the live subscription list plus
 *  server-wide invalidation totals and a per-db rollup. */
export interface SubscriptionsResponse {
  subscriptions: SubscriptionInfo[];
  subsRerunsTotal: number;
  subsSkipsPointTotal: number;
  subsSkipsIndexedTotal: number;
  subsSkipsOrderedTotal: number;
  subsMissedPushesTotal: number;
  perDb: DbSubCounters[];
}

/** One managed pg_dump file on disk (GET /admin/backups). */
export interface BackupFile {
  name: string;
  sizeBytes: number;
  createdMs: number;
}
/** Response from GET /admin/backups: existing dumps and whether one is running. */
export interface BackupsListResponse {
  running: boolean;
  backups: BackupFile[];
}
/** Response from POST /admin/restore: the fresh rtdb_restored_<stamp> DB and follow-up. */
export interface RestoreResult {
  target: string;
  instructions: string;
}

/** One registered webhook. `table: null` means "all tables"; `events` carries op
 *  names (`insert`/`patch`/`replace`/`delete`/`upsert`) or the single element
 *  `["*"]` to match every event. `secret` is the per-webhook HMAC key the
 *  server generates (SEC-115); the receiver uses it to verify each delivery's
 *  `X-Rtdb-Signature` header. Surfaced here so an operator can copy it to the
 *  receiver; `null` only before the boot backfill has run. */
export interface Webhook {
  id: number;
  db: string;
  table: string | null;
  url: string;
  events: string[];
  createdAt: number;
  enabled: boolean;
  secret: string | null;
}

/** One delivery row from a webhook's outbox (`GET .../webhooks/{id}/deliveries`).
 *  `payload` is the raw JSON body the worker POSTed (or will POST) — typed
 *  `unknown` because the server passes it through verbatim. */
export interface WebhookDelivery {
  id: number;
  attempts: number;
  status: string;
  nextAttempt: number;
  lastError: string | null;
  payload: unknown;
}

/** Options for `createWebhook`. `url` is required; the rest fall back to server
 *  defaults (all tables, `["*"]` events, enabled). */
export interface CreateWebhookOptions {
  url: string;
  table?: string | null;
  events?: string[];
  enabled?: boolean;
}

/** Options for `editWebhook`. Every field is optional: omitted = unchanged.
 *  `table` is a tri-state — omit to leave the filter alone, `null` to clear it
 *  to all-tables, or a string to set it. `rotateSecret: true` generates a fresh
 *  server-side signing secret (SEC-115); the secret value itself is never
 *  accepted from the client. */
export interface EditWebhookOptions {
  url?: string;
  table?: string | null;
  events?: string[];
  enabled?: boolean;
  rotateSecret?: boolean;
}

/** Options for `listDeliveries`. All optional; omitted filters/limits mean: no
 *  status filter, server default limit, offset 0. */
export interface ListDeliveriesOptions {
  status?: string;
  limit?: number;
  offset?: number;
}

/** One new column reported by `previewSchema`. `fieldType` is the
 *  human-readable field type (e.g. `string`, `id<projects>`, `string?`). */
export interface SchemaPreviewColumnAdd {
  name: string;
  fieldType: string;
}
/** One new index reported by `previewSchema`. */
export interface SchemaPreviewIndexAdd {
  name: string;
  fields: string[];
}
/** One new table reported by `previewSchema`: its name plus the columns and
 *  indexes the additive-only push would add. */
export interface SchemaPreviewTableAdd {
  table: string;
  columns: SchemaPreviewColumnAdd[];
  indexes: SchemaPreviewIndexAdd[];
}
/** One rejection reported by `previewSchema`: a drop or type change the DDL
 *  layer will refuse. `item` is the bare column/index name. */
export interface SchemaPreviewRejection {
  table: string;
  item: string;
  reason: string;
}
/** Result of `previewSchema`: what an additive-only push would ADD and what it
 *  would REJECT (drops, type changes). Mirrors the server's `schema_diff`. */
export interface SchemaPreviewDiff {
  added: SchemaPreviewTableAdd[];
  rejected: SchemaPreviewRejection[];
}

/** Result of POST /admin/db/{db}/explain — the compiled SQL for a query, with
 *  bind values formatted as strings (numbers/booleans via Display) and any
 *  compile-time warnings (e.g. unindexed-filter). `terminal` is the query
 *  terminal kind (get|collect|count|unique|first|distinct|aggregate|paginate|
 *  search|vectorSearch|hybridSearch). */
export interface ExplainResult {
  sql: string;
  params: string[];
  terminal: string;
  warnings: string[];
}

/** One entry from GET /admin/slow-queries. `params` is omitted from the server
 *  JSON when redacted (the default) and present as string[] when
 *  `RTDB_SLOW_QUERY_LOG_PARAMS=true`. */
export interface SlowQueryEntry {
  startedAtMs: number;
  durationMs: number;
  db: string;
  table: string;
  terminal: string;
  sql: string;
  params?: string[];
}

/** Response envelope for GET /admin/slow-queries. `thresholdMs` is 0 when slow
 *  query logging is disabled. */
export interface SlowQueriesResponse {
  queries: SlowQueryEntry[];
  thresholdMs: number;
  capacity: number;
}

/** Options for `purgePreview`/`purge` (POST /admin/db/{db}/purge). `filter` is
 *  the same `FilterExpr` DSL a query's `where` uses — omit to purge every row
 *  of the table. `limit` caps rows deleted per call; the server clamps it to
 *  [1, 10000] and defaults to 10000. */
export interface PurgeOptions {
  filter?: FilterExpr;
  limit?: number;
}

/** Preview reply from `purgePreview`: how many rows the filter currently
 *  matches, the `confirmToken` to echo back to `purge` (stale — rows changed
 *  since the preview — makes the confirm refuse with 409; re-preview), and the
 *  per-call row cap the confirm will apply. */
export interface PurgePreview {
  matching: number;
  confirmToken: string;
  limit: number;
}

/** Confirm reply from `purge`. `truncated: true` means rows still match: the
 *  token bound the preview-time count, so that count has now changed and the
 *  next confirm needs a fresh token — re-run `purgePreview` first, then
 *  confirm again, until a pass reports `truncated: false`. */
export interface PurgeResult {
  deleted: number;
  truncated: boolean;
}
