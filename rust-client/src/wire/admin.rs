//! HTTP request/response bodies for `/admin/*`. These mirror the server's
//! `admin.rs` handler structs (not the WS `protocol.rs`) field-for-field; the
//! casing is load-bearing — `tokenId` is camelCase on the wire.
//!
//! Extracted from `wire.rs` (QA-008) so the WS protocol types stay at parity
//! with `server/src/protocol.rs` (~640 LOC) instead of carrying the admin
//! control-plane shapes that are HTTP-only. The `#[cfg(feature = "admin")]`
//! gate is on the module declaration in `wire.rs`, not here, so this file
//! compiles cleanly under `--all-features`.

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
    pub token_id: String,
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
    pub email: String,
    pub github_id: Option<i64>,
}

/// One row of `DbStats.tables` (`GET /admin/dbs/{db}/stats`). ARC-130 response-shaped.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct TableStat {
    pub name: String,
    pub row_count: i64,
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
    pub tables: Vec<TableStat>,
    pub total_size_bytes: i64,
    /// Per-db resource quotas (ENH-011); 0 = unlimited.
    pub tables_quota: i64,
    pub tables_used: i64,
    pub storage_quota_bytes: i64,
    pub storage_used_bytes: i64,
    pub subs_quota: i64,
    pub subs_used: i64,
}

/// One row of `TokenInfo` returned by `GET /admin/tokens?db=...`. ARC-130 response-shaped.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct TokenInfo {
    pub id: String,
    pub name: String,
    pub created_at: i64,
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
    pub token_hash: String,
    pub user_id: String,
    /// `None` when the user has no email (e.g. an anonymous session).
    #[serde(default)]
    pub email: Option<String>,
    /// `None` when the user has no login handle.
    #[serde(default)]
    pub login: Option<String>,
    pub anonymous: bool,
    pub created_at: i64,
    pub expires_at: i64,
}

/// Optional filter for `list_sessions` (`GET /admin/sessions?user=&limit=`).
/// Both fields optional: `user` filters by user id or email; `limit` pages the
/// result (server default 200, clamped to `[1, 1000]`). Mirrors the
/// `{user?, limit?}` shape in `ts-client`'s `listSessions`.
#[derive(Debug, Clone, Default)]
pub struct SessionListOptions {
    pub user: Option<String>,
    pub limit: Option<i64>,
}

/// Response wrapper for `GET /admin/sessions` → `{sessions:[...]}`.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct SessionsResponse {
    pub(crate) sessions: Vec<SessionInfo>,
}

/// Response for `DELETE /admin/sessions?user={userId}` → `{ok, revoked}` where
/// `revoked` is the count of sessions dropped. Mirrors `ts-client`'s
/// `revokeUserSessions` return shape.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct RevokeUserSessionsResponse {
    pub ok: bool,
    pub revoked: i64,
}

/// p50/p95/p99 latency percentile triple (microseconds). Mirrors
/// `server::metrics::LatencyStats`. Field names are already lowercase, so
/// `rename_all = "camelCase"` leaves them as `p50`/`p95`/`p99` on the wire.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct LatencyStats {
    pub p50: i64,
    pub p95: i64,
    pub p99: i64,
}

/// `GET /admin/metrics` snapshot — server counters and gauges.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct MetricsSnapshot {
    pub queries_total: i64,
    pub mutations_total: i64,
    pub uploads_total: i64,
    pub ws_connections: i64,
    pub active_subscriptions: i64,
    pub pool_size: i64,
    pub pool_idle: i64,
    pub uptime_seconds: i64,
    pub query_latency: LatencyStats,
    pub mutate_latency: LatencyStats,
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
    pub subs_skips_point_total: i64,
    #[serde(default)]
    pub subs_skips_indexed_total: i64,
    #[serde(default)]
    pub subs_skips_ordered_total: i64,
    /// Sampled shadow verifications of skips and the ones that found the
    /// skip WRONG. `subs_missed_pushes_total > 0` means invalidation
    /// under-approximated — a dropped realtime update.
    #[serde(default)]
    pub subs_skip_verifications_total: i64,
    #[serde(default)]
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
    pub user_id: Option<String>,
    pub email: Option<String>,
}

/// One row of [`SubscriptionsResponse::subscriptions`]: a live subscription
/// and the read-set class that governs its skip/re-run invalidation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SubscriptionInfo {
    pub db: String,
    pub table: String,
    pub terminal: String,
    pub read_set_class: String,
    pub principal: Option<SubscriptionsPrincipal>,
}

/// Per-db subscription-invalidation counters — one row of
/// [`SubscriptionsResponse::per_db`] and [`MetricsSnapshot::per_db_subs`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct DbSubCounters {
    pub db: String,
    pub reruns: u64,
    pub skips_point: u64,
    pub skips_indexed: u64,
    pub skips_ordered: u64,
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
    pub subscriptions: Vec<SubscriptionInfo>,
    pub subs_reruns_total: u64,
    pub subs_skips_point_total: u64,
    pub subs_skips_indexed_total: u64,
    pub subs_skips_ordered_total: u64,
    pub subs_missed_pushes_total: u64,
    pub per_db: Vec<DbSubCounters>,
}

/// Runtime-mutable hot-config subset of `ConfigResponse`. Mirrors
/// `server/src/config::HotConfig` field-for-field. ARC-130 response-shaped.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct HotConfig {
    pub allowed_origins: Vec<String>,
    pub session_ttl_days: i64,
    pub max_file_size: i64,
    pub idempotency_ttl_ms: i64,
    /// Per-db resource quotas (ENH-011); 0 = unlimited. Mirrors server.
    pub max_tables_per_db: i64,
    pub max_storage_bytes_per_db: i64,
    pub max_subs_per_db: i64,
}

/// `GET /admin/config` response — redacted boot config + hot config + build
/// identity + admin allowlist. Mirrors `server/src/admin::ConfigResponse`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ConfigResponse {
    pub port: i64,
    pub public_url: String,
    pub github_base_url: String,
    pub github_api_url: String,
    pub database_url_configured: bool,
    pub admin_key_configured: bool,
    pub github_configured: bool,
    pub google_configured: bool,
    pub gitlab_configured: bool,
    pub oidc_configured: bool,
    pub hot: HotConfig,
    pub version: String,
    pub git_commit: String,
    pub admins: Vec<AdminMember>,
}

/// `PATCH /admin/config` body — every field optional, omitted fields left
/// unchanged. Unknown fields rejected server-side (`deny_unknown_fields`).
/// `skip_serializing_if = "Option::is_none"` keeps the wire body minimal.
#[derive(Debug, Clone, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HotConfigPatch {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_origins: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_ttl_days: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_file_size: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_ttl_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tables_per_db: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_storage_bytes_per_db: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_subs_per_db: Option<i64>,
}

/// One row of `OpEvent` returned by `GET /admin/ops/recent`. `kind` is a
/// `String` — the admin client passes it through; consumers match on it.
/// `owner` is `Option<String>` for the `string | null` wire.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct OpEvent {
    pub db: String,
    pub table: String,
    pub doc_id: String,
    pub kind: String,
    pub ts: i64,
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
/// renamed to the wire alias `where`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", deny_unknown_fields)]
pub enum Directive {
    RenameField {
        table: String,
        from: String,
        to: String,
    },
    RenameTable {
        from: String,
        to: String,
    },
    ChangeType {
        table: String,
        field: String,
        to: crate::schema::FieldType,
        cast: Cast,
        #[serde(default)]
        default: Option<serde_json::Value>,
    },
    DropField {
        table: String,
        field: String,
    },
    DropTable {
        name: String,
    },
    DropIndex {
        table: String,
        name: String,
    },
    SetDefault {
        table: String,
        field: String,
        value: serde_json::Value,
    },
    EvalExpr {
        table: String,
        set: String,
        expr: String,
        #[serde(default, rename = "where")]
        where_clause: Option<String>,
    },
}

/// Closed set of sound coercions for [`Directive::ChangeType`]. Mirrors
/// server `migrate::Cast` (camelCase on the wire).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Cast {
    ToString,
    ToNumber,
    ToInt64,
    ToBoolean,
}

/// Borrowed HTTP body for `POST /admin/db/{db}/migrate`. Mirrors server
/// `migrate::MigrateRequest` (camelCase; `dryRun` is `#[serde(default)]`
/// false). The borrowed lifetime lets [`crate::http::RtDbHttpClient`]
/// serialize a caller-owned `&[Directive]` without copying.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrateRequest<'a> {
    pub directives: &'a [Directive],
    #[serde(default)]
    pub dry_run: bool,
}

/// Owned counterpart of [`MigrateRequest`] — needed by the CLI and any
/// caller that builds a request from a [`crate::migration::Migration`] and
/// holds it past the borrow. Mirrors the same wire shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrateRequestOwned {
    pub directives: Vec<Directive>,
    #[serde(default)]
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
    pub applied: bool,
    pub schema: crate::schema::SchemaDef,
    pub directives: Vec<DirectiveReport>,
}

/// One row of `GET /admin/db/{db}/schema/history` (newest-first). Mirrors
/// server `schema_history::HistorySummary` (camelCase). `source` is the
/// event that captured the snapshot: `"push"` | `"migrate"` | `"restore"`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SchemaHistorySummary {
    pub version: i64,
    pub captured_at: i64,
    pub source: String,
    #[serde(default)]
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
    pub version: i64,
    pub captured_at: i64,
    pub source: String,
    #[serde(default)]
    pub principal: Option<String>,
    pub schema: serde_json::Value,
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
    pub op: String,
    pub affected_rows: i64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cast_failures: Vec<CastFailure>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sample_changes: Vec<SampleChange>,
}

/// One row of [`DirectiveReport::cast_failures`]. Mirrors server
/// `migrate::CastFailure`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct CastFailure {
    pub id: String,
    pub value: serde_json::Value,
}

/// One row of [`DirectiveReport::sample_changes`]. Mirrors server
/// `migrate::SampleChange`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SampleChange {
    pub id: String,
    pub before: serde_json::Value,
    pub after: serde_json::Value,
}

/// One managed-backup file as returned by `GET /admin/backups`. Mirrors
/// server `backup::BackupFile` (camelCase on the wire).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BackupFile {
    pub name: String,
    pub size_bytes: u64,
    pub created_ms: i64,
}

/// `GET /admin/backups` response: the in-progress flag plus the on-disk
/// dump list, newest-first.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BackupsListResponse {
    pub running: bool,
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
    pub target: String,
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
    pub id: i64,
    pub db: String,
    #[serde(default)]
    pub table: Option<String>,
    pub url: String,
    pub events: Vec<String>,
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
    pub id: i64,
    pub attempts: i64,
    pub status: String,
    pub next_attempt: i64,
    pub last_error: Option<String>,
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
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub events: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub events: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rotate_secret: Option<bool>,
}

/// Optional filters for [`crate::http::RtDbHttpClient::list_deliveries`]. All
/// fields optional: `status` filters by `pending|retrying|delivered|failed`;
/// `limit`/`offset` page (server defaults: limit=50 clamped to `[1,1000]`,
/// offset=0). Mirrors `ListDeliveriesOptions` in `ts-client`.
#[derive(Debug, Clone, Default)]
pub struct ListDeliveriesOptions {
    pub status: Option<String>,
    pub limit: Option<i64>,
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
    pub id: i64,
    pub ts_ms: i64,
    pub db: String,
    pub table: String,
    /// Wire `op`. `null` for system-initiated rows. Defaults to `None`
    /// for older servers that omit the field.
    #[serde(default)]
    pub op: Option<String>,
    pub doc_id: String,
    /// Wire `principal` (the per-row owner when an interactive user wrote
    /// the doc, `null` for machine tokens / system sources). Defaults to
    /// `None` for older servers that omit the field.
    #[serde(default)]
    pub principal: Option<String>,
    pub source: String,
}

/// Optional filters for [`crate::http::RtDbHttpClient::get_audit`]. Every
/// field is optional: `table`/`op`/`principal`/`source` are equality filters
/// combined with AND (an absent field matches all rows); `limit`/`offset`
/// page (server defaults: limit=100 clamped to `[1,1000]`, offset=0). Mirrors
/// `AuditQuery` in `ts-client`.
#[derive(Debug, Clone, Default)]
pub struct AuditQuery {
    pub table: Option<String>,
    pub op: Option<String>,
    pub principal: Option<String>,
    pub source: Option<String>,
    pub limit: Option<i64>,
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
