//! HTTP request/response bodies for `/admin/*`. These mirror the server's
//! `admin.rs` handler structs (not the WS `protocol.rs`) field-for-field; the
//! casing is load-bearing — `tokenId` is camelCase on the wire.
//!
//! Extracted from `wire.rs` (QA-008) so the WS protocol types stay at parity
//! with `server/src/protocol.rs` (~640 LOC) instead of carrying the admin
//! control-plane shapes that are HTTP-only. The `#[cfg(feature = "admin")]`
//! gate is on the module declaration in `wire.rs`, not here, so this file
//! compiles cleanly under `--all-features`.

use std::collections::BTreeMap;

use crate::schema::SchemaDef;
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
pub(crate) struct CreateDbRequest<'a> {
    pub(crate) name: &'a str,
}

#[derive(Serialize)]
pub(crate) struct DeleteDbRequest<'a> {
    pub(crate) name: &'a str,
    pub(crate) confirm: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MergeUsersRequest<'a> {
    pub(crate) anon_user_id: &'a str,
    pub(crate) real_user_id: &'a str,
    pub(crate) confirm: &'a str,
}

#[derive(Serialize)]
pub(crate) struct PushSchemaRequest<'a> {
    pub(crate) db: &'a str,
    pub(crate) schema: &'a SchemaDef,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MintTokenRequest<'a> {
    pub(crate) db: &'a str,
    pub(crate) name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) expires_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) read_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) tables: Option<&'a [String]>,
}

/// Optional capabilities for [`crate::http::RtDbHttpClient::mint_token_with_options`].
/// Every field is optional; `MintTokenOptions::default()` is a full-access
/// mint (no expiry, read-write, all tables) — the server applies those
/// defaults to any field left `None`.
#[derive(Debug, Clone, Default)]
pub struct MintTokenOptions {
    /// Unix-millis expiry (`expiresAt` on the wire). `None` = no expiry.
    pub expires_at: Option<i64>,
    /// `readOnly` on the wire. `None` = read-write (server default).
    pub read_only: Option<bool>,
    /// `tables` allowlist on the wire. `None` = all tables (server default).
    pub tables: Option<Vec<String>>,
}

#[derive(Serialize)]
pub(crate) struct RevokeTokenRequest<'a> {
    #[serde(rename = "tokenId")]
    pub(crate) token_id: &'a str,
}

#[derive(Serialize)]
pub(crate) struct AllowlistWriteRequest<'a> {
    pub(crate) db: &'a str,
    pub(crate) action: &'a str,
    pub(crate) email: &'a str,
}

#[derive(Deserialize)]
pub(crate) struct OkResponse {
    pub(crate) ok: bool,
}

#[derive(Deserialize)]
pub(crate) struct DatabasesResponse {
    pub(crate) databases: Vec<String>,
}

/// Returned by `mint_token`: the server's `{tokenId, token}` shape, with the
/// wire `tokenId` exposed as `token_id`. ARC-130 response-shaped.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct MintedToken {
    #[serde(rename = "tokenId")]
    /// The minted token's stable id (for revoke/list).
    pub token_id: String,
    /// The plaintext bearer token — shown once, never stored server-side.
    pub token: String,
}

#[derive(Deserialize)]
pub(crate) struct AllowlistListResponse {
    pub(crate) emails: Vec<String>,
}

/// One row of the admin allowlist returned by `GET /admin/admins`. ARC-130 response-shaped.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct AdminMember {
    /// The admin's email (the allowlist key).
    pub email: String,
    /// GitHub numeric id when linked.
    pub github_id: Option<i64>,
}

/// One row of `DbStats.tables` (`GET /admin/dbs/{db}/stats`). ARC-130 response-shaped.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct TableStat {
    /// Table name.
    pub name: String,
    /// Live row count.
    pub row_count: i64,
    /// On-disk size in bytes.
    pub size_bytes: i64,
}

/// `GET /admin/dbs/{db}/stats` response.
///
/// The six quota/usage fields (ENH-011) are always emitted by the server; `i64`
/// matches the existing `HotConfig` quota typing (the server's `usize`/`u64` both
/// serialize as JSON numbers and never approach `i64` range).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct DbStats {
    /// Per-table stats.
    pub tables: Vec<TableStat>,
    /// Whole-db size in bytes.
    pub total_size_bytes: i64,
    /// Per-db resource quotas (ENH-011); 0 = unlimited.
    pub tables_quota: i64,
    /// Tables currently pushed.
    pub tables_used: i64,
    /// Storage cap in bytes; 0 = unlimited.
    pub storage_quota_bytes: i64,
    /// Storage currently used.
    pub storage_used_bytes: i64,
    /// Subscription cap; 0 = unlimited.
    pub subs_quota: i64,
    /// Live subscriptions.
    pub subs_used: i64,
}

/// One row of `TokenInfo` returned by `GET /admin/tokens?db=...`. ARC-130 response-shaped.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct TokenInfo {
    /// Stable token id (for revoke).
    pub id: String,
    /// Operator-assigned label.
    pub name: String,
    /// Mint time, epoch ms.
    pub created_at: i64,
    /// Whether the token is revoked.
    pub revoked: bool,
    /// `null` = no expiry. Defaults to `None` for older servers that
    /// omit the field.
    #[serde(default)]
    pub expires_at: Option<i64>,
    /// Server always emits `readOnly`. Defaults to `false` for older
    /// servers that omit the field.
    #[serde(default)]
    pub read_only: bool,
    /// `null` = all tables. Defaults to `None` for older servers that
    /// omit the field.
    #[serde(default)]
    pub tables: Option<Vec<String>>,
}

// ---- interactive sessions (GET/DELETE /admin/sessions) ------------------
//
// Mirror `ts-client`'s `SessionInfo` + `listSessions`/`revokeSession`/
// `revokeUserSessions` byte-for-byte (camelCase). `tokenHash` is a
// non-reversible sha256 digest (the plaintext token is never stored), safe
// to surface to an admin and used to target a row for revoke. `email`/`login`
// are `None` when the user has none (e.g. an anonymous session).

/// One active interactive session as returned by `GET /admin/sessions`.
/// Mirrors `ts-client`'s `SessionInfo` (camelCase).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SessionInfo {
    /// Non-reversible sha256 of the session token (revoke target).
    pub token_hash: String,
    /// The authed user's id.
    pub user_id: String,
    /// `None` when the user has no email (e.g. an anonymous session).
    #[serde(default)]
    pub email: Option<String>,
    /// `None` when the user has no login handle.
    #[serde(default)]
    pub login: Option<String>,
    /// Whether this is an anonymous session.
    pub anonymous: bool,
    /// Login time, epoch ms.
    pub created_at: i64,
    /// Expiry time, epoch ms.
    pub expires_at: i64,
}

/// Optional filter for `list_sessions` (`GET /admin/sessions?user=&limit=`).
/// Both fields optional: `user` filters by user id or email; `limit` pages the
/// result (server default 200, clamped to `[1, 1000]`). Mirrors the
/// `{user?, limit?}` shape in `ts-client`'s `listSessions`.
#[derive(Debug, Clone, Default)]
pub struct SessionListOptions {
    /// Filter by user id or email.
    pub user: Option<String>,
    /// Page size (server default 200, clamped to 1..=1000).
    pub limit: Option<i64>,
}

/// Response wrapper for `GET /admin/sessions` → `{sessions:[...]}`.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct SessionsResponse {
    pub(crate) sessions: Vec<SessionInfo>,
}

/// Optional filter for `list_workflows`
/// (`GET /admin/db/{db}/workflows?status=&limit=`). Both fields optional:
/// `status` filters by run state; `limit` pages the result (server default
/// 100, capped at 500). Mirrors ts-client's `listWorkflows` opts.
#[derive(Debug, Clone, Default)]
pub struct WorkflowListOptions {
    /// Filter by run lifecycle state.
    pub status: Option<crate::wire::WorkflowStatus>,
    /// Page size (server default 100, capped 500).
    pub limit: Option<u32>,
}

/// Response wrapper for `GET /admin/db/{db}/workflows` → `{workflows:[...]}`.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct WorkflowsResponse {
    pub(crate) workflows: Vec<crate::wire::WorkflowInfo>,
}

/// Response for `DELETE /admin/sessions?user={userId}` → `{ok, revoked}` where
/// `revoked` is the count of sessions dropped. Mirrors `ts-client`'s
/// `revokeUserSessions` return shape.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct RevokeUserSessionsResponse {
    /// Always true on success.
    pub ok: bool,
    /// How many sessions were dropped.
    pub revoked: i64,
}

// ---- anon→real account merge (POST /admin/merge-users) -------------------
//
// Mirror `server::merge::MergeReport` and `ts-client`'s `MergeReport`
// byte-for-byte (camelCase). Both serde directions are derived: the client
// deserializes the server's report, and the CLI re-serializes it for output.

/// A row skipped by the anon→real merge because the re-stamp would collide
/// with an existing doc under a unique index. Mirrors `ts-client`'s
/// `MergeConflict`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MergeConflict {
    /// The conflicting row's table.
    pub table: String,
    /// The conflicting row's id.
    pub id: String,
}

/// Per-database outcome of an anon→real merge: re-stamped-doc counts per
/// table plus the rows skipped over unique-index conflicts. Mirrors
/// `ts-client`'s `MergeDbResult`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MergeDbResult {
    /// Re-stamped-doc counts per table.
    pub tables: BTreeMap<String, usize>,
    /// Rows skipped over unique-index collisions.
    pub conflicts: Vec<MergeConflict>,
}

/// Full-instance anon→real merge outcome from `POST /admin/merge-users`:
/// per-db doc re-stamps, storage blobs repointed, sessions repointed (an
/// open WS or stored SDK token promotes to the real principal on its next
/// op), and whether the anon user row was deleted. Mirrors `ts-client`'s
/// `MergeReport`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MergeReport {
    /// Per-db doc re-stamp outcomes.
    pub dbs: BTreeMap<String, MergeDbResult>,
    /// Storage blobs moved to the real user.
    pub storage_repointed: u64,
    /// Sessions re-pointed to the real user.
    pub sessions_repointed: u64,
    /// Whether the anon user row was removed.
    pub anon_deleted: bool,
}

/// p50/p95/p99 latency percentile triple (microseconds). Mirrors
/// `server::metrics::LatencyStats`. Field names are already lowercase, so
/// `rename_all = "camelCase"` leaves them as `p50`/`p95`/`p99` on the wire.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct LatencyStats {
    /// Median, microseconds.
    pub p50: i64,
    /// 95th percentile, microseconds.
    pub p95: i64,
    /// 99th percentile, microseconds.
    pub p99: i64,
}

/// `GET /admin/metrics` snapshot — server counters and gauges.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct MetricsSnapshot {
    /// Queries served since boot.
    pub queries_total: i64,
    /// Transactions applied since boot.
    pub mutations_total: i64,
    /// File uploads since boot.
    pub uploads_total: i64,
    /// Open `/sync` sockets.
    pub ws_connections: i64,
    /// Live query subscriptions.
    pub active_subscriptions: i64,
    /// Postgres pool connections.
    pub pool_size: i64,
    /// Idle pool connections.
    pub pool_idle: i64,
    /// Process uptime.
    pub uptime_seconds: i64,
    /// Query latency percentiles.
    pub query_latency: LatencyStats,
    /// Mutation latency percentiles.
    pub mutate_latency: LatencyStats,
    /// Subscribe latency percentiles.
    pub subscribe_latency: LatencyStats,
    /// Subscription-invalidation effectiveness: read-set decisions that
    /// ended in a re-run vs. a proven skip, split by the class that proved
    /// it (`point` = `get(id)`, `indexed` = count/collect/unique over an
    /// eq-prefix window, `ordered` = take/first/paginate bounded by a top-N
    /// sort boundary).
    ///
    /// `#[serde(default)]` on this group so a client built against a newer
    /// server still deserializes an OLDER server's response (these counters
    /// landed 2026-07-29); 0 is the correct "not reported" value for a
    /// monotonic counter. The rest of the struct stays strict.
    #[serde(default)]
    pub subs_reruns_total: i64,
    #[serde(default)]
    /// Skips proven by a `get(id)` point read.
    pub subs_skips_point_total: i64,
    #[serde(default)]
    /// Skips proven by an eq-prefix window.
    pub subs_skips_indexed_total: i64,
    #[serde(default)]
    /// Skips proven by a top-N sort boundary.
    pub subs_skips_ordered_total: i64,
    /// Sampled shadow verifications of skips and the ones that found the
    /// skip WRONG. `subs_missed_pushes_total > 0` means invalidation
    /// under-approximated — a dropped realtime update.
    #[serde(default)]
    pub subs_skip_verifications_total: i64,
    #[serde(default)]
    /// Verifications that found a skip WRONG — alert on any increase.
    pub subs_missed_pushes_total: i64,
    /// ENH-010 per-db breakdown of the subscription counters above
    /// (`perDbSubs` on the wire). `#[serde(default)]` so an older server
    /// that omits it still deserializes to an empty vec.
    #[serde(default)]
    pub per_db_subs: Vec<DbSubCounters>,
}

/// Subscriber identity for [`SubscriptionInfo`]. The server emits `null`
/// (→ `None`) when the subscriber has no interactive identity — a machine
/// token, a scheduled job, or admin bypass.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SubscriptionsPrincipal {
    /// User id when interactive.
    pub user_id: Option<String>,
    /// Email when known.
    pub email: Option<String>,
}

/// One row of [`SubscriptionsResponse::subscriptions`]: a live subscription
/// and the read-set class that governs its skip/re-run invalidation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SubscriptionInfo {
    /// Which database the subscription reads.
    pub db: String,
    /// The queried table.
    pub table: String,
    /// The query terminal.
    pub terminal: String,
    /// `point` / `indexed` / `ordered` / `table`.
    pub read_set_class: String,
    /// Subscriber identity (`None` for machine/bypass).
    pub principal: Option<SubscriptionsPrincipal>,
}

/// Per-db subscription-invalidation counters — one row of
/// [`SubscriptionsResponse::per_db`] and [`MetricsSnapshot::per_db_subs`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct DbSubCounters {
    /// Which database.
    pub db: String,
    /// Fan-out decisions that re-ran.
    pub reruns: u64,
    /// Skips proven by a point read.
    pub skips_point: u64,
    /// Skips proven by an eq-prefix window.
    pub skips_indexed: u64,
    /// Skips proven by a top-N boundary.
    pub skips_ordered: u64,
    /// Verifications that found a skip wrong.
    pub missed: u64,
}

/// `GET /admin/subscriptions?db=<optional>` response (ENH-010): the live
/// subscription inspector. `subscriptions` enumerates every active
/// subscription; the counter totals mirror `MetricsSnapshot`'s invalidation
/// totals server-wide, and `per_db` breaks them down per database.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SubscriptionsResponse {
    /// Every live subscription.
    pub subscriptions: Vec<SubscriptionInfo>,
    /// Server-wide re-runs.
    pub subs_reruns_total: u64,
    /// Server-wide point skips.
    pub subs_skips_point_total: u64,
    /// Server-wide indexed skips.
    pub subs_skips_indexed_total: u64,
    /// Server-wide ordered skips.
    pub subs_skips_ordered_total: u64,
    /// Server-wide missed pushes.
    pub subs_missed_pushes_total: u64,
    /// The same counters per database.
    pub per_db: Vec<DbSubCounters>,
}

/// Runtime-mutable hot-config subset of `ConfigResponse`. Mirrors
/// `server/src/config::HotConfig` field-for-field. ARC-130 response-shaped.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct HotConfig {
    /// CORS allowlist (hot-reloaded per request).
    pub allowed_origins: Vec<String>,
    /// Session cookie lifetime in days.
    pub session_ttl_days: i64,
    /// Upload size cap in bytes.
    pub max_file_size: i64,
    /// Idempotency-key retention window.
    pub idempotency_ttl_ms: i64,
    /// Per-db resource quotas (ENH-011); 0 = unlimited. Mirrors server.
    pub max_tables_per_db: i64,
    /// Storage cap per db in bytes; 0 = unlimited.
    pub max_storage_bytes_per_db: i64,
    /// Subscription cap per db; 0 = unlimited.
    pub max_subs_per_db: i64,
}

/// `GET /admin/config` response — redacted boot config + hot config + build
/// identity + admin allowlist. Mirrors `server/src/admin::ConfigResponse`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ConfigResponse {
    /// HTTP listen port.
    pub port: i64,
    /// Configured public origin.
    pub public_url: String,
    /// GitHub OAuth base (overrideable for GitHub Enterprise).
    pub github_base_url: String,
    /// GitHub API base.
    pub github_api_url: String,
    /// Boot redaction: whether the DB URL is set.
    pub database_url_configured: bool,
    /// Boot redaction: whether the admin key is set.
    pub admin_key_configured: bool,
    /// Whether GitHub OAuth is configured.
    pub github_configured: bool,
    /// Whether Google OAuth is configured.
    pub google_configured: bool,
    /// Whether GitLab OAuth is configured.
    pub gitlab_configured: bool,
    /// Whether generic OIDC is configured.
    pub oidc_configured: bool,
    /// The runtime-mutable subset.
    pub hot: HotConfig,
    /// Crate version.
    pub version: String,
    /// Build commit label.
    pub git_commit: String,
    /// The server-wide admin allowlist.
    pub admins: Vec<AdminMember>,
}

/// `PATCH /admin/config` body — every field optional, omitted fields left
/// unchanged. Unknown fields rejected server-side (`deny_unknown_fields`).
/// `skip_serializing_if = "Option::is_none"` keeps the wire body minimal.
#[derive(Debug, Clone, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HotConfigPatch {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// New value; `None` leaves it unchanged.
    pub allowed_origins: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// New value; `None` leaves it unchanged.
    pub session_ttl_days: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// New value; `None` leaves it unchanged.
    pub max_file_size: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// New value; `None` leaves it unchanged.
    pub idempotency_ttl_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// New value; `None` leaves it unchanged.
    pub max_tables_per_db: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// New value; `None` leaves it unchanged.
    pub max_storage_bytes_per_db: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// New value; `None` leaves it unchanged.
    pub max_subs_per_db: Option<i64>,
}

/// One row of `OpEvent` returned by `GET /admin/ops/recent`. `kind` is a
/// `String` — the admin client passes it through; consumers match on it.
/// `owner` is `Option<String>` for the `string | null` wire.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct OpEvent {
    /// Which database.
    pub db: String,
    /// Which table.
    pub table: String,
    /// The document's id.
    pub doc_id: String,
    /// The op kind (`insert`/`patch`/…).
    pub kind: String,
    /// Commit time, epoch ms.
    pub ts: i64,
    /// Per-row owner principal, when one applies.
    pub owner: Option<String>,
}

// ---- schema migration (POST /admin/db/{db}/migrate) -------------------
//
// Mirror server `migrate::*` byte-for-byte: the `Directive` enum (tag `op`,
// camelCase, `deny_unknown_fields` — the same shape contract as
// `mutation::Step`), `Cast`, `MigrateRequest`, `MigrateResult`,
// `DirectiveReport`, `CastFailure`, `SampleChange`. See
// `server/src/migrate.rs` for the authoritative shapes; `ts-client`'s
// `protocol.ts` carries the parity-checked TS view.

/// One schema-migration step. Wire shape mirrors server `migrate::Directive`:
/// `tag = "op"`, `rename_all = "camelCase"`, `deny_unknown_fields` (the same
/// shape contract as [`crate::mutation::Step`]). `evalExpr.where_clause` is
/// renamed to the wire alias `where`. ENH-020 (closing SEC-107) made
/// `evalExpr` dual-accept: `expr` is either a typed [`ValueExpr`] (safe,
/// all-literals-bound) or a legacy raw-SQL string (deprecated, gated to the
/// root admin_key); `where` is either a typed [`crate::wire::FilterExpr`] or a
/// legacy raw-SQL predicate string. The two sources may not mix. See
/// [`ExprSource`] / [`CondSource`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", deny_unknown_fields)]
pub enum Directive {
    /// Rename a field (re-keys indexes/defaults, keeps values).
    RenameField {
        /// Which table.
        table: String,
        /// Old field name.
        from: String,
        /// New field name.
        to: String,
    },
    /// Rename a table.
    RenameTable {
        /// Old table name.
        from: String,
        /// New table name.
        to: String,
    },
    /// Coerce a field to a new type via a closed cast.
    ChangeType {
        /// Which table.
        table: String,
        /// Which field.
        field: String,
        /// The new declared type.
        to: crate::schema::FieldType,
        /// How to coerce existing values.
        cast: Cast,
        #[serde(default)]
        /// Substitute for un-coercible rows (`None` = roll back on any).
        default: Option<serde_json::Value>,
    },
    /// Remove a field (destructive).
    DropField {
        /// Which table.
        table: String,
        /// Which field.
        field: String,
    },
    /// Remove a whole table (destructive).
    DropTable {
        /// Which table.
        name: String,
    },
    /// Remove an index.
    DropIndex {
        /// Which table.
        table: String,
        /// Which index.
        name: String,
    },
    /// Backfill a default onto rows missing the field.
    SetDefault {
        /// Which table.
        table: String,
        /// Which field.
        field: String,
        /// The literal to stamp.
        value: serde_json::Value,
    },
    /// Compute and set a field from a typed expression.
    EvalExpr {
        /// Which table.
        table: String,
        /// The write-target field.
        set: String,
        /// ENH-020: dual-accept. A typed [`ValueExpr`] (safe, all-literals-bound
        /// path) or a legacy raw-SQL string (deprecated, gated to the root
        /// admin_key — the SEC-107 boundary until the string form is removed).
        expr: ExprSource,
        /// Dual-accept `where`: a typed [`crate::wire::FilterExpr`] (safe) or a
        /// legacy raw-SQL predicate string (deprecated, same root-admin gate).
        #[serde(default, skip_serializing_if = "Option::is_none", rename = "where")]
        where_clause: Option<CondSource>,
    },
}

/// Closed set of sound coercions for [`Directive::ChangeType`] and
/// [`ValueExpr::Cast`]. Mirrors server `migrate::Cast` (camelCase on the wire).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Cast {
    /// Coerce to string.
    ToString,
    /// Coerce to JSON number.
    ToNumber,
    /// Coerce to 64-bit integer.
    ToInt64,
    /// Coerce to boolean.
    ToBoolean,
}

/// A closed, typed expression grammar for [`Directive::EvalExpr`]'s backfill
/// expression (ENH-020 Stage 1, closing SEC-107). Mirrors server
/// `migrate::ValueExpr` byte-for-byte: `tag = "op"`, camelCase,
/// `deny_unknown_fields` (the same serde conventions as
/// [`crate::wire::FilterExpr`]). Every `Literal` compiles to a bound `$n`
/// placeholder (as jsonb); every `Field` resolves through the table's
/// `TableDef` and reads `doc->'field'`. There is deliberately **no** subquery
/// node, no function-call-by-name node, and no raw-SQL escape — the grammar is
/// closed, so the SEC-107 injection concern cannot arise from a `ValueExpr`
/// payload. The only way to reach raw SQL is the deprecated
/// [`ExprSource::Legacy`] source, which remains gated to the root admin_key.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", deny_unknown_fields)]
pub enum ValueExpr {
    /// A declared field on this table (validated against `TableDef`). Reads
    /// `doc->'field'` (jsonb). The field must be declared; the write target
    /// (`EvalExpr.set`) need not be.
    Field {
        /// The declared field to read.
        field: String,
    },
    /// Any JSON literal. Bound as `$n::jsonb`, so objects/arrays/null round-trip.
    Literal {
        /// The literal value.
        value: serde_json::Value,
    },
    /// String concatenation. Postgres `concat(...)`, which ignores NULL args
    /// (treats them as empty) — wrap operands in `Coalesce` for explicit control.
    Concat {
        /// The concatenation operands.
        parts: Vec<ValueExpr>,
    },
    /// Numeric arithmetic. Operands are cast to `::numeric`; the result is a
    /// JSON number via the surrounding `to_jsonb`. Division by zero errors at
    /// runtime — guard with `Case`/`Coalesce` when the divisor may be zero.
    Add {
        /// Left operand (+).
        left: Box<ValueExpr>,
        /// Right operand (+).
        right: Box<ValueExpr>,
    },
    /// Subtraction (`left - right`).
    Sub {
        /// Left operand (-).
        left: Box<ValueExpr>,
        /// Right operand (-).
        right: Box<ValueExpr>,
    },
    /// Multiplication (`left * right`).
    Mul {
        /// Left operand (*).
        left: Box<ValueExpr>,
        /// Right operand (*).
        right: Box<ValueExpr>,
    },
    /// Division (`left / right`); by-zero errors at runtime.
    Div {
        /// Left operand (/).
        left: Box<ValueExpr>,
        /// Right operand (/).
        right: Box<ValueExpr>,
    },
    /// `COALESCE(parts...)` — first non-null, or NULL.
    Coalesce {
        /// First-non-null candidates.
        parts: Vec<ValueExpr>,
    },
    /// Text casing / trim. Operand cast to `::text`.
    Lower {
        /// Operand to lowercase.
        value: Box<ValueExpr>,
    },
    /// Uppercase.
    Upper {
        /// Operand to uppercase.
        value: Box<ValueExpr>,
    },
    /// Trim surrounding whitespace.
    Trim {
        /// Operand to trim.
        value: Box<ValueExpr>,
    },
    /// A closed scalar coercion. Reuses [`Directive::ChangeType`]'s [`Cast`].
    Cast {
        /// Operand to coerce.
        value: Box<ValueExpr>,
        /// Target scalar type.
        to: Cast,
    },
    /// Current timestamp (`now()`), as jsonb.
    Now,
    /// Conditional: first matching `when`'s `then`, else `otherwise`. Each
    /// `when` is a [`crate::wire::FilterExpr`] (field references schema-
    /// validated, values bound).
    Case {
        /// Branch conditions, in order.
        whens: Vec<CaseWhen>,
        /// Fallback when no `when` matches.
        otherwise: Box<ValueExpr>,
    },
}

/// One branch of [`ValueExpr::Case`]. Wire shape `{ when, then }`. Mirrors
/// server `migrate::CaseWhen` (camelCase, `deny_unknown_fields`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CaseWhen {
    /// The branch condition.
    pub when: crate::wire::FilterExpr,
    /// The value when it matches.
    pub then: ValueExpr,
}

/// Dual-accept source for [`Directive::EvalExpr`]'s `expr`: a typed
/// [`ValueExpr`] (the safe path) or a legacy raw-SQL string (the deprecated
/// path, gated to root admin_key). `#[serde(untagged)]` tries `Typed` first; a
/// string fails `ValueExpr` (an internally-tagged object) and falls through to
/// `Legacy`. A hostile object that is not a valid `ValueExpr` fails both arms
/// and is rejected — it does NOT silently become legacy. Mirrors server
/// `migrate::ExprSource`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ExprSource {
    /// The safe typed expression.
    Typed(ValueExpr),
    /// Deprecated raw SQL (root admin_key only).
    Legacy(String),
}

/// Dual-accept source for [`Directive::EvalExpr`]'s `where`: a typed
/// [`crate::wire::FilterExpr`] or a legacy raw-SQL predicate string. Same
/// untagged discipline as [`ExprSource`]. Mirrors server `migrate::CondSource`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CondSource {
    /// The safe typed predicate.
    Typed(crate::wire::FilterExpr),
    /// Deprecated raw SQL (root admin_key only).
    Legacy(String),
}

/// Borrowed HTTP body for `POST /admin/db/{db}/migrate`. Mirrors server
/// `migrate::MigrateRequest` (camelCase; `dryRun` is `#[serde(default)]`
/// false). The borrowed lifetime lets [`crate::http::RtDbHttpClient`]
/// serialize a caller-owned `&[Directive]` without copying.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrateRequest<'a> {
    /// The migration steps, applied in order.
    pub directives: &'a [Directive],
    #[serde(default)]
    /// Preview only: validate, derive, commit nothing.
    pub dry_run: bool,
}

/// Owned counterpart of [`MigrateRequest`] — needed by the CLI and any
/// caller that builds a request from a [`crate::migration::Migration`] and
/// holds it past the borrow. Mirrors the same wire shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrateRequestOwned {
    /// The migration steps, applied in order.
    pub directives: Vec<Directive>,
    #[serde(default)]
    /// Preview only: validate, derive, commit nothing.
    pub dry_run: bool,
}

/// `POST /admin/db/{db}/migrate` response. `schema` is the post-migration
/// derived schema — returned even on `dryRun` (with `applied: false`), so a
/// caller can preview the resulting shape. Mirrors server
/// `migrate::MigrateResult`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct MigrateResult {
    /// Whether the directives committed (`false` on dry-run).
    pub applied: bool,
    /// The post-migration derived schema.
    pub schema: crate::schema::SchemaDef,
    /// Per-directive outcome reports.
    pub directives: Vec<DirectiveReport>,
}

/// One row of `GET /admin/db/{db}/schema/history` (newest-first). Mirrors
/// server `schema_history::HistorySummary` (camelCase). `source` is the
/// event that captured the snapshot: `"push"` | `"migrate"` | `"restore"`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SchemaHistorySummary {
    /// Snapshot version (monotonic).
    pub version: i64,
    /// Capture time, epoch ms.
    pub captured_at: i64,
    /// `"push"` / `"migrate"` / `"restore"`.
    pub source: String,
    #[serde(default)]
    /// Who captured it, when known.
    pub principal: Option<String>,
}

/// One full snapshot from `GET /admin/db/{db}/schema/history/{version}`,
/// adding the `schema` blob. Mirrors server `schema_history::HistoryEntry`.
/// `schema` is the raw captured JSON (a serialized `SchemaDef`), kept as a
/// `Value` so an older snapshot never fails to deserialize.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SchemaHistoryEntry {
    /// Snapshot version.
    pub version: i64,
    /// Capture time, epoch ms.
    pub captured_at: i64,
    /// `"push"` / `"migrate"` / `"restore"`.
    pub source: String,
    #[serde(default)]
    /// Who captured it, when known.
    pub principal: Option<String>,
    /// The captured schema JSON, verbatim.
    pub schema: serde_json::Value,
}

// ---- schema preview (POST /admin/db/{db}/schema/preview) -----------------
//
// Mirror server `schema_diff::{SchemaDiff, TableAdd, ColumnAdd, IndexAdd,
// Rejection}` byte-for-byte (camelCase) and `ts-client`'s `SchemaPreview*`
// views. Both serde directions are derived: the client deserializes the
// server's diff, and the CLI re-serializes it for output.

/// Borrowed HTTP body for `POST /admin/db/{db}/schema/preview` — the same
/// shape as [`PushSchemaRequest`] minus `db` (it rides the URL path).
/// Serialize-only like every borrowed request wrapper (a `&SchemaDef` field
/// cannot derive Deserialize).
#[derive(Serialize)]
pub(crate) struct PreviewSchemaRequest<'a> {
    pub(crate) schema: &'a SchemaDef,
}

/// One new column reported by `preview_schema`. `field_type` is the
/// human-readable field type (e.g. `string`, `id<projects>`, `string?`).
/// Mirrors server `schema_diff::ColumnAdd`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SchemaPreviewColumnAdd {
    /// Column name.
    pub name: String,
    /// Human-readable type (`string`, `id<projects>`, …).
    pub field_type: String,
}

/// One new index reported by `preview_schema`. Mirrors server
/// `schema_diff::IndexAdd`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SchemaPreviewIndexAdd {
    /// Index name.
    pub name: String,
    /// The indexed fields.
    pub fields: Vec<String>,
}

/// One new table reported by `preview_schema`: its name plus the columns and
/// indexes the additive-only push would add. Mirrors server
/// `schema_diff::TableAdd`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SchemaPreviewTableAdd {
    /// New table name.
    pub table: String,
    /// Columns the push would add.
    pub columns: Vec<SchemaPreviewColumnAdd>,
    /// Indexes the push would add.
    pub indexes: Vec<SchemaPreviewIndexAdd>,
}

/// One rejection reported by `preview_schema`: a drop or type change the DDL
/// layer will refuse. `item` is the bare column/index name. Mirrors server
/// `schema_diff::Rejection`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SchemaPreviewRejection {
    /// Table holding the rejected item.
    pub table: String,
    /// The column/index name.
    pub item: String,
    /// Why the push would refuse it.
    pub reason: String,
}

/// Result of `preview_schema` (`POST /admin/db/{db}/schema/preview`): what an
/// additive-only push would ADD and what it would REJECT (drops, type
/// changes). Pure/advisory — the preview does not apply anything. Mirrors
/// server `schema_diff::SchemaDiff`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SchemaPreviewDiff {
    /// Additive changes a push would make.
    pub added: Vec<SchemaPreviewTableAdd>,
    /// Drops/type changes a push would refuse.
    pub rejected: Vec<SchemaPreviewRejection>,
}

/// Per-directive outcome. `castFailures` and `sampleChanges` are
/// `skip_serializing_if = "Vec::is_empty"` on the server, so they surface as
/// optional on the wire (absent when empty). Mirrors server
/// `migrate::DirectiveReport`. `Default` is derived so the in-memory harness
/// and later tasks can build it incrementally with `..Default::default()`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct DirectiveReport {
    /// Which directive ran.
    pub op: String,
    /// Rows touched.
    pub affected_rows: i64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    /// Rows that failed coercion (with their values).
    pub cast_failures: Vec<CastFailure>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    /// Before/after samples.
    pub sample_changes: Vec<SampleChange>,
}

/// One row of [`DirectiveReport::cast_failures`]. Mirrors server
/// `migrate::CastFailure`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct CastFailure {
    /// The row's id.
    pub id: String,
    /// The value that failed to coerce.
    pub value: serde_json::Value,
}

/// One row of [`DirectiveReport::sample_changes`]. Mirrors server
/// `migrate::SampleChange`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SampleChange {
    /// The row's id.
    pub id: String,
    /// The row before the directive.
    pub before: serde_json::Value,
    /// The row after.
    pub after: serde_json::Value,
}

/// One managed-backup file as returned by `GET /admin/backups`. Mirrors
/// server `backup::BackupFile` (camelCase on the wire).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BackupFile {
    /// Dump file name.
    pub name: String,
    /// Dump size in bytes.
    pub size_bytes: u64,
    /// Dump time, epoch ms.
    pub created_ms: i64,
}

/// `GET /admin/backups` response: the in-progress flag plus the on-disk
/// dump list, newest-first.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BackupsListResponse {
    /// Whether a dump is in progress.
    pub running: bool,
    /// On-disk dumps, newest-first.
    pub backups: Vec<BackupFile>,
}

/// `POST /admin/restore` body. `confirm` must equal `name` — the typed
/// confirmation guard mirrors `delete_db`.
#[derive(Serialize)]
pub(crate) struct RestoreRequest<'a> {
    pub(crate) name: &'a str,
    pub(crate) confirm: &'a str,
}

/// `POST /admin/restore` response: the freshly-created target DB name and
/// cutover instructions.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct RestoreResult {
    /// The freshly-created restore DB name.
    pub target: String,
    /// Operator cutover instructions.
    pub instructions: String,
}

// ---- webhook management (GET/POST/PUT/DELETE /admin/db/{db}/webhooks[...]) -
//
// Mirror server `webhook::{Webhook, DeliveryRow}` and the admin handler
// request/response structs byte-for-byte (camelCase). The `enabled` field
// landed after launch (ENH-003), so it and the optional `table` carry
// `#[serde(default)]` for back-compat with an older server's responses.

/// One registered webhook returned by `GET /admin/db/{db}/webhooks`. Mirrors
/// server `webhook::Webhook`. `table = None` means "all tables"; `events`
/// carries op names (`insert`/`patch`/`replace`/`delete`/`upsert`) or the
/// single-element `["*"]` to match every event.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Webhook {
    /// Webhook id.
    pub id: i64,
    /// Owning database.
    pub db: String,
    #[serde(default)]
    /// Scoped table, or `None` for all tables.
    pub table: Option<String>,
    /// Delivery target.
    pub url: String,
    /// Op names or `["*"]`.
    pub events: Vec<String>,
    /// Registration time, epoch ms.
    pub created_at: i64,
    /// Added in ENH-003. `#[serde(default)]` so an older server that omits
    /// the field still parses (defaulting to `false`); a current server
    /// always emits it.
    #[serde(default)]
    pub enabled: bool,
    /// Per-webhook HMAC signing key (SEC-115); the receiver uses it to verify
    /// each delivery's `X-Rtdb-Signature` header. Server-generated; surfaced
    /// here so an operator can copy it to the receiver. `#[serde(default)]`
    /// parses an older server that omits the field as `None`.
    #[serde(default)]
    pub secret: Option<String>,
}

/// One delivery row from a webhook's outbox
/// (`GET .../webhooks/{id}/deliveries`). Mirrors server
/// `webhook::DeliveryRow`. `payload` is the raw JSON body queued at enqueue
/// time, passed through verbatim so an operator can inspect the exact event
/// the worker will/did POST.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct WebhookDelivery {
    /// Delivery row id.
    pub id: i64,
    /// Delivery attempts so far.
    pub attempts: i64,
    /// `pending` / `retrying` / `delivered` / `failed`.
    pub status: String,
    /// Scheduled retry time, epoch ms.
    pub next_attempt: i64,
    /// The last failure, if any.
    pub last_error: Option<String>,
    /// The exact JSON body queued for POST.
    pub payload: serde_json::Value,
}

/// Options for [`crate::http::RtDbHttpClient::create_webhook`]. `url` is
/// required; the rest fall back to server defaults when `None` (all-tables,
/// `["*"]` events, enabled). Mirrors `CreateWebhookOptions` in `ts-client`.
/// `skip_serializing_if = "Option::is_none"` keeps each `None` field off the
/// wire so the server default applies.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateWebhookOptions {
    /// Delivery target (required).
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Scope to one table (all tables when `None`).
    pub table: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Op names to match ( `["*"]` when `None`).
    pub events: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Start enabled/disabled (enabled when `None`).
    pub enabled: Option<bool>,
}

/// Options for [`crate::http::RtDbHttpClient::edit_webhook`]. Every field is
/// optional — absent means "leave unchanged". `table` is a tri-state: outer
/// `None` leaves the filter alone, `Some(None)` (serialized as JSON `null`)
/// clears it to all-tables, and `Some(Some(t))` sets it to `t`. Mirrors
/// `EditWebhookOptions` in `ts-client` (where `undefined`/`null`/`string` is
/// the same tri-state) and pairs with the server's `deserialize_some` on the
/// PUT body. `rotate_secret = Some(true)` generates a fresh server-side
/// signing secret (SEC-115); the secret value itself is never accepted from
/// the client, so this is a flag, not a value.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WebhookEditOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// New target URL.
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Tri-state: skip / clear to all-tables / set.
    pub table: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// New event set.
    pub events: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Enable/disable.
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// `Some(true)` generates a fresh signing secret.
    pub rotate_secret: Option<bool>,
}

/// Optional filters for [`crate::http::RtDbHttpClient::list_deliveries`]. All
/// fields optional: `status` filters by `pending|retrying|delivered|failed`;
/// `limit`/`offset` page (server defaults: limit=50 clamped to `[1,1000]`,
/// offset=0). Mirrors `ListDeliveriesOptions` in `ts-client`.
#[derive(Debug, Clone, Default)]
pub struct ListDeliveriesOptions {
    /// Filter by delivery status.
    pub status: Option<String>,
    /// Page size (default 50, clamped to 1..=1000).
    pub limit: Option<i64>,
    /// Page offset.
    pub offset: Option<i64>,
}

// ---- audit log (GET /admin/audit) --------------------------------------
//
// Mirror server `audit::AuditEntry` byte-for-byte (camelCase). `op` and
// `principal` are `Option<String>` on the wire (the server emits JSON `null`
// for system-initiated rows such as TTL reaps and scheduled jobs, which
// carry no per-doc op or user principal); both carry `#[serde(default)]` so
// an older server that omits either still parses. `table` is the wire alias
// of the server's `tbl` field — named `table` directly here so the
// camelCase rename leaves it untouched, matching the wire key.

/// One durable-audit row as returned by `GET /admin/audit`. Mirrors server
/// `audit::AuditEntry` (camelCase). `op`/`principal` are `None` for
/// system-initiated writes (TTL reaps, scheduled jobs).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct AuditEntry {
    /// Audit row id.
    pub id: i64,
    /// Write time, epoch ms.
    pub ts_ms: i64,
    /// Which database.
    pub db: String,
    /// Which table.
    pub table: String,
    /// Wire `op`. `null` for system-initiated rows. Defaults to `None`
    /// for older servers that omit the field.
    #[serde(default)]
    pub op: Option<String>,
    /// Which document.
    pub doc_id: String,
    /// Wire `principal` (the per-row owner when an interactive user wrote
    /// the doc, `null` for machine tokens / system sources). Defaults to
    /// `None` for older servers that omit the field.
    #[serde(default)]
    pub principal: Option<String>,
    /// Tap arm (`mutate`/`ttl`/`merge`/…).
    pub source: String,
}

/// Optional filters for [`crate::http::RtDbHttpClient::get_audit`]. Every
/// field is optional: `table`/`op`/`principal`/`source` are equality filters
/// combined with AND (an absent field matches all rows); `limit`/`offset`
/// page (server defaults: limit=100 clamped to `[1,1000]`, offset=0). Mirrors
/// `AuditQuery` in `ts-client`.
#[derive(Debug, Clone, Default)]
pub struct AuditQuery {
    /// Equality filter on table.
    pub table: Option<String>,
    /// Equality filter on op.
    pub op: Option<String>,
    /// Equality filter on principal.
    pub principal: Option<String>,
    /// Equality filter on source.
    pub source: Option<String>,
    /// Page size (default 100, clamped to 1..=1000).
    pub limit: Option<i64>,
    /// Page offset.
    pub offset: Option<i64>,
}

// Internal response wrappers (one per admin webhook endpoint that returns a
// collection or a scalar the SDK unwraps). Field names match the server's
// JSON keys verbatim (no camelCase mapping needed — `webhooks`/`id`/
// `deliveries`/`entries` are already lowercase).
#[derive(Deserialize)]
pub(crate) struct WebhooksResponse {
    pub(crate) webhooks: Vec<Webhook>,
}

#[derive(Deserialize)]
pub(crate) struct CreateWebhookResponse {
    pub(crate) id: i64,
}

#[derive(Deserialize)]
pub(crate) struct DeliveriesResponse {
    pub(crate) deliveries: Vec<WebhookDelivery>,
}

#[derive(Deserialize)]
pub(crate) struct AuditResponse {
    pub(crate) entries: Vec<AuditEntry>,
}

/// `POST /admin/db/{db}/explain` → the compiled SQL + ordered bind params for
/// a Query DSL body (ENH-019). Mirrors server `admin::observability::ExplainResponse`.
/// `sql` is byte-identical to what the read path executes; `params` carries
/// the same `$1..$n` binds formatted as strings; `warnings` surfaces
/// compile-time concerns (e.g. a filter on a declared-but-unindexed field).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ExplainResult {
    /// The compiled SQL (byte-identical to execution).
    pub sql: String,
    /// The `$1..$n` binds, formatted as strings.
    pub params: Vec<String>,
    /// Which terminal compiled.
    pub terminal: String,
    /// Compile-time concerns (e.g. unindexed filter field).
    pub warnings: Vec<String>,
}

/// One row of [`SlowQueriesResponse::queries`]: a recorded slow-query event
/// (ENH-019). Mirrors server `metrics::SlowQueryRecord`. `params` is included
/// only when the server has `RTDB_SLOW_QUERY_LOG_PARAMS=true` — otherwise it
/// is omitted on the wire and deserializes to `None` to keep document content
/// out of the log by default.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SlowQueryEntry {
    /// When the query started, as epoch milliseconds.
    pub started_at_ms: i64,
    /// Wall-clock duration in milliseconds.
    pub duration_ms: u64,
    /// Which database.
    pub db: String,
    /// Which table.
    pub table: String,
    /// Which terminal.
    pub terminal: String,
    /// The executed SQL.
    pub sql: String,
    /// Bound parameters; `None` when the server redacts them (the default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Vec<String>>,
}

/// `GET /admin/slow-queries?db=<optional>&limit=<n>` response (ENH-019): the
/// bounded in-memory ring newest-first. Mirrors server
/// `admin::observability::SlowQueriesResponse`. `threshold_ms` is the
/// configured `RTDB_SLOW_QUERY_MS` (0 = logging disabled → `queries` is
/// empty); `capacity` is the configured ring-buffer cap.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SlowQueriesResponse {
    /// Recorded events, newest-first.
    pub queries: Vec<SlowQueryEntry>,
    /// Configured `RTDB_SLOW_QUERY_MS` (0 = disabled).
    pub threshold_ms: u64,
    /// Ring-buffer cap.
    pub capacity: usize,
}
