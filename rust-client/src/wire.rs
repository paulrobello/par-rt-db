//! Wire vocabulary — the third implementation of the protocol contract
//! (server `protocol.rs` first, TS `protocol.ts` second). Tags/fields are load-bearing.

use crate::error::{ErrorEnvelope, RtDbError};
use crate::mutation::Transaction;
use crate::query::Query;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type QueryRef = Query;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ClientMessage {
    Auth {
        // SEC-001 phase 2: optional — a browser dashboard authenticates over
        // `/sync` from the HttpOnly cookie, sending only `db`. CLI/SDK/machine
        // tokens still send `token` (the prior wire form); backward-compatible.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        token: Option<String>,
        db: String,
    },
    Subscribe {
        query_id: String,
        query: Box<Query>,
    },
    Unsubscribe {
        query_id: String,
    },
    Mutate {
        mut_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        idempotency_key: Option<String>,
        txn: Transaction,
    },
    Schedule {
        schedule_id: String,
        when: ScheduleWhen,
        txn: Transaction,
    },
    CancelSchedule {
        schedule_id: String,
        id: String,
    },
    PauseSchedule {
        schedule_id: String,
        id: String,
    },
    ResumeSchedule {
        schedule_id: String,
        id: String,
    },
    ListSchedules {
        schedule_id: String,
    },
    Ping,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ServerMessage {
    AuthOk {
        user: AuthedUser,
    },
    AuthErr {
        error: RtDbError,
    },
    QueryUpdate {
        query_id: String,
        result: serde_json::Value,
    },
    MutateOk {
        mut_id: String,
        results: Vec<serde_json::Value>,
    },
    MutateErr {
        mut_id: String,
        error: RtDbError,
    },
    SubscribeErr {
        query_id: String,
        error: RtDbError,
    },
    ScheduleOk {
        schedule_id: String,
        id: String,
    },
    ScheduleErr {
        schedule_id: String,
        error: RtDbError,
    },
    /// Reply to cancel/pause/resume. `error` is omitted on the wire when `ok`.
    ScheduleAck {
        schedule_id: String,
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<RtDbError>,
    },
    ListSchedulesOk {
        schedule_id: String,
        schedules: Vec<ScheduleInfo>,
    },
    Pong,
}

/// Whether an `AuthedUser` resolved from an OAuth session or a machine token.
/// Mirrors `server/src/protocol.rs::UserKind` (ARC-004/QA-008): serializes as
/// `"user"` / `"machine"`, byte-identical to the prior `String` form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UserKind {
    User,
    Machine,
}

impl UserKind {
    pub fn as_wire_str(&self) -> &'static str {
        match self {
            UserKind::User => "user",
            UserKind::Machine => "machine",
        }
    }
}

impl From<UserKind> for &'static str {
    fn from(k: UserKind) -> &'static str {
        k.as_wire_str()
    }
}

impl std::str::FromStr for UserKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "user" => Ok(UserKind::User),
            "machine" => Ok(UserKind::Machine),
            other => Err(format!("unknown UserKind: {other}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthedUser {
    pub kind: UserKind,
    pub email: Option<String>,
    pub name: Option<String>,
    /// GitHub login. Absent on the wire for machine tokens / non-GitHub
    /// users; serde defaults a missing field to `None` so this stays
    /// backward-compatible with older servers that omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub github_login: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub github_id: Option<i64>,
}

/// How a caller wants a transaction scheduled. Mirrored byte-for-byte in
/// `server/src/protocol.rs` and `ts-client/src/protocol.ts`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum ScheduleWhen {
    /// Fire `ms` milliseconds from now.
    AfterMs { ms: i64 },
    /// Fire at this UTC epoch-ms instant (in the past = fire immediately).
    RunAt { ms: i64 },
    /// Fire on this 5-field cron schedule (UTC, min-first). The server validates
    /// the expression; the client does no cron parsing.
    Cron { expr: String },
}

/// Whether a scheduled job fires once or repeats on cron. Mirrors
/// `server/src/protocol.rs::ScheduleKind` (ARC-004/QA-008): serializes as
/// `"oneshot"` / `"cron"`, byte-identical to the prior `String` form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleKind {
    Oneshot,
    Cron,
}

impl ScheduleKind {
    pub fn as_wire_str(&self) -> &'static str {
        match self {
            ScheduleKind::Oneshot => "oneshot",
            ScheduleKind::Cron => "cron",
        }
    }
}

impl From<ScheduleKind> for &'static str {
    fn from(k: ScheduleKind) -> &'static str {
        k.as_wire_str()
    }
}

impl std::str::FromStr for ScheduleKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "oneshot" => Ok(ScheduleKind::Oneshot),
            "cron" => Ok(ScheduleKind::Cron),
            other => Err(format!("unknown ScheduleKind: {other}")),
        }
    }
}

/// Lifecycle state of a scheduled job. Mirrors
/// `server/src/protocol.rs::ScheduleStatus` (ARC-004/QA-008): serializes as
/// `"pending"` / `"running"` / `"paused"` / `"error"`, byte-identical to the
/// prior `String` form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleStatus {
    Pending,
    Running,
    Paused,
    Error,
}

impl ScheduleStatus {
    pub fn as_wire_str(&self) -> &'static str {
        match self {
            ScheduleStatus::Pending => "pending",
            ScheduleStatus::Running => "running",
            ScheduleStatus::Paused => "paused",
            ScheduleStatus::Error => "error",
        }
    }
}

impl From<ScheduleStatus> for &'static str {
    fn from(s: ScheduleStatus) -> &'static str {
        s.as_wire_str()
    }
}

impl std::str::FromStr for ScheduleStatus {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(ScheduleStatus::Pending),
            "running" => Ok(ScheduleStatus::Running),
            "paused" => Ok(ScheduleStatus::Paused),
            "error" => Ok(ScheduleStatus::Error),
            other => Err(format!("unknown ScheduleStatus: {other}")),
        }
    }
}

/// A scheduled job's public view (returned by `listSchedules`). `cron` and
/// `last_error` are omitted on the wire when absent. Mirrors
/// `server/src/protocol.rs::ScheduleInfo`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleInfo {
    pub id: String,
    pub kind: ScheduleKind,
    pub due_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cron: Option<String>,
    pub status: ScheduleStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub created_at: i64,
    pub fired_count: i64,
}

/// A full-text search terminal over a declared search index. `index` names a
/// search index on the query's table; `query` is free-form user text. Mirrors
/// `server/src/query.rs::SearchQuery` byte-for-byte (camelCase, deny_unknown_fields).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SearchQuery {
    pub index: String,
    pub query: String,
}

/// A vector-similarity terminal over a declared vector index. `vector` is the
/// caller-supplied query embedding (length must equal the index dimensions);
/// ranked by cosine distance ascending. `filter` is an optional eq-map over
/// the index's declared `filterFields`. Mirrors
/// `server/src/query.rs::VectorSearchQuery` byte-for-byte (camelCase,
/// deny_unknown_fields).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VectorSearchQuery {
    pub index: String,
    // ARC-008(a): f64 (not f32) — the server, TS, and Python clients all carry
    // full JSON-number precision, so narrowing to f32 here was the lone path
    // that silently dropped precision on a round-trip. f64 matches the wire.
    pub vector: Vec<f64>,
    pub limit: u32,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub filter: BTreeMap<String, serde_json::Value>,
}

/// A hybrid search terminal that fuses full-text (`search`) and vector
/// (`vectorSearch`) ranking via Reciprocal Rank Fusion (RRF). Mirrors
/// `server/src/query.rs::HybridSearchQuery` byte-for-byte (camelCase,
/// deny_unknown_fields). `search_index`/`vector_index` optionally name the
/// indexes (auto-selected server-side when omitted); `k` is the RRF constant
/// (default 60, omitted on the wire when `None`). The vector is f64 for
/// wire-precision parity with the other clients (ARC-008(a)).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HybridSearchQuery {
    pub query: String,
    pub vector: Vec<f64>,
    pub limit: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search_index: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector_index: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub k: Option<u32>,
}

/// Aggregate operator for the `aggregate` terminal. Mirrors
/// `server/src/query.rs::AggregateOp` byte-for-byte (lowercase variants).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AggregateOp {
    Sum,
    Avg,
    Min,
    Max,
}

/// `aggregate` terminal spec. `op` selects the SQL aggregate run over the index
/// field after the eq prefix; `group_by` shifts the terminal to a grouped
/// aggregate (groups by the index field after the eq prefix, aggregates the one
/// after that). Mirrors `server/src/query.rs::AggregateSpec` byte-for-byte
/// (camelCase, deny_unknown_fields). The server uses `#[serde(default)]` on
/// `group_by` (always emits it); this client mirrors the rest of the SDK's
/// bool convention and omits it on the wire when false, which the server
/// accepts (the field is `#[serde(default)]`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AggregateSpec {
    pub op: AggregateOp,
    #[serde(default, skip_serializing_if = "is_false")]
    pub group_by: bool,
}

/// Serde skip predicate for `bool` fields whose default is `false`. Lets the
/// rust-client omit `groupBy` on the wire when false, matching the TS client.
fn is_false(b: &bool) -> bool {
    !*b
}

/// One `{key, value}` row from a grouped `aggregate` (`groupBy: true`) terminal.
/// Mirrors `server/src/query.rs::AggregateGroup` byte-for-byte (camelCase).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregateGroup {
    pub key: serde_json::Value,
    pub value: serde_json::Value,
}

/// A db-side predicate appended to a query's WHERE clause. Mirrors
/// `server/src/query.rs::FilterExpr` byte-for-byte: internally tagged by `op`
/// (lowercase), `deny_unknown_fields`. Leaves compare one declared field to a
/// value (`In` to a non-empty list); `And`/`Or` nest arbitrarily; `Not` wraps
/// a nested expr; `Contains` tests membership of `value` in `doc.field[]`
/// (reverse of `In`); `Exists` tests the field is present and non-null.
///
/// Construct variants directly (`FilterExpr::Eq { field, value }`) — inherent
/// constructors named `eq`/`gt`/`lt` are avoided because they shadow
/// `PartialEq`/`PartialOrd` trait methods (`clippy::should_implement_trait`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase", deny_unknown_fields)]
pub enum FilterExpr {
    Eq {
        field: String,
        value: serde_json::Value,
    },
    Neq {
        field: String,
        value: serde_json::Value,
    },
    Gt {
        field: String,
        value: serde_json::Value,
    },
    Gte {
        field: String,
        value: serde_json::Value,
    },
    Lt {
        field: String,
        value: serde_json::Value,
    },
    Lte {
        field: String,
        value: serde_json::Value,
    },
    In {
        field: String,
        values: Vec<serde_json::Value>,
    },
    And {
        exprs: Vec<FilterExpr>,
    },
    Or {
        exprs: Vec<FilterExpr>,
    },
    Not {
        expr: Box<FilterExpr>,
    },
    Contains {
        field: String,
        value: serde_json::Value,
    },
    Exists {
        field: String,
    },
}

/// One slot of a `POST /api/query-batch` response. Mirrors server
/// `http_api::BatchQueryOutcome` byte-for-byte (camelCase, omit-when-None). The
/// `result` field is the raw untagged `QueryResult` value (the server serializes
/// `QueryResult` with `#[serde(untagged)]`, so the on-wire form is the bare
/// value — `null`, a doc, an array of docs, a count, a `{docs,nextCursor}`,
/// etc. — matching how [`RtDbHttpClient::run`](crate::http::RtDbHttpClient::run)
/// types its return as a caller-chosen `T`). A batch spans terminals, so the
/// caller narrows each slot via [`serde_json::Value`] rather than a typed
/// result. `error` reuses the standard `{code, message}` envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchQueryOutcome {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorEnvelope>,
}

/// HTTP request/response bodies for `/admin/*`. These mirror the server's
/// `admin.rs` handler structs (not the WS `protocol.rs`) field-for-field; the
/// casing is load-bearing — `tokenId` is camelCase on the wire.
#[cfg(feature = "admin")]
pub mod admin {
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
    /// wire `tokenId` exposed as `token_id`.
    #[derive(Debug, Clone, Deserialize)]
    pub struct MintedToken {
        #[serde(rename = "tokenId")]
        pub token_id: String,
        pub token: String,
    }

    #[derive(Deserialize)]
    pub(crate) struct AllowlistListResponse {
        pub(crate) emails: Vec<String>,
    }

    /// One row of the admin allowlist returned by `GET /admin/admins`.
    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct AdminMember {
        pub email: String,
        pub github_id: Option<i64>,
    }

    /// One row of `DbStats.tables` (`GET /admin/dbs/{db}/stats`).
    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct TableStat {
        pub name: String,
        pub row_count: i64,
        pub size_bytes: i64,
    }

    /// `GET /admin/dbs/{db}/stats` response.
    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct DbStats {
        pub tables: Vec<TableStat>,
        pub total_size_bytes: i64,
    }

    /// One row of `TokenInfo` returned by `GET /admin/tokens?db=...`.
    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
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

    /// p50/p95/p99 latency percentile triple (microseconds). Mirrors
    /// `server::metrics::LatencyStats`. Field names are already lowercase, so
    /// `rename_all = "camelCase"` leaves them as `p50`/`p95`/`p99` on the wire.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct LatencyStats {
        pub p50: i64,
        pub p95: i64,
        pub p99: i64,
    }

    /// `GET /admin/metrics` snapshot — server counters and gauges.
    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
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
    pub struct SubscriptionsPrincipal {
        pub user_id: Option<String>,
        pub email: Option<String>,
    }

    /// One row of [`SubscriptionsResponse::subscriptions`]: a live subscription
    /// and the read-set class that governs its skip/re-run invalidation.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
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
    /// `server/src/config::HotConfig` field-for-field.
    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct HotConfig {
        pub allowed_origins: Vec<String>,
        pub session_ttl_days: i64,
        pub max_file_size: i64,
        pub idempotency_ttl_ms: i64,
    }

    /// `GET /admin/config` response — redacted boot config + hot config + build
    /// identity + admin allowlist. Mirrors `server/src/admin::ConfigResponse`.
    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
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
    }

    /// One row of `OpEvent` returned by `GET /admin/ops/recent`. `kind` is a
    /// `String` — the admin client passes it through; consumers match on it.
    /// `owner` is `Option<String>` for the `string | null` wire.
    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
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
    pub struct CastFailure {
        pub id: String,
        pub value: serde_json::Value,
    }

    /// One row of [`DirectiveReport::sample_changes`]. Mirrors server
    /// `migrate::SampleChange`.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct SampleChange {
        pub id: String,
        pub before: serde_json::Value,
        pub after: serde_json::Value,
    }

    /// One managed-backup file as returned by `GET /admin/backups`. Mirrors
    /// server `backup::BackupFile` (camelCase on the wire).
    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct BackupFile {
        pub name: String,
        pub size_bytes: u64,
        pub created_ms: i64,
    }

    /// `GET /admin/backups` response: the in-progress flag plus the on-disk
    /// dump list, newest-first.
    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
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
    }

    /// One delivery row from a webhook's outbox
    /// (`GET .../webhooks/{id}/deliveries`). Mirrors server
    /// `webhook::DeliveryRow`. `payload` is the raw JSON body queued at enqueue
    /// time, passed through verbatim so an operator can inspect the exact event
    /// the worker will/did POST.
    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
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
    /// PUT body.
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutation::Transaction;
    use crate::query::TableQuery;
    use serde_json::json;

    fn sample_query() -> Query {
        TableQuery::new("workItems").collect()
    }
    fn empty_txn() -> Transaction {
        Transaction { steps: vec![] }
    }

    #[test]
    fn client_message_tags_and_fields() {
        assert_eq!(
            serde_json::to_value(ClientMessage::Auth {
                token: Some("t".into()),
                db: "d".into()
            })
            .unwrap(),
            json!({"type":"auth","token":"t","db":"d"})
        );
        let sub = serde_json::to_value(ClientMessage::Subscribe {
            query_id: "q1".into(),
            query: Box::new(sample_query()),
        })
        .unwrap();
        assert_eq!(sub["type"], json!("subscribe"));
        assert_eq!(sub["query"], json!({"table":"workItems"}));
        assert_eq!(
            serde_json::to_value(ClientMessage::Unsubscribe {
                query_id: "q1".into()
            })
            .unwrap(),
            json!({"type":"unsubscribe","queryId":"q1"})
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::Mutate {
                mut_id: "m1".into(),
                idempotency_key: None,
                txn: empty_txn(),
            })
            .unwrap(),
            json!({"type":"mutate","mutId":"m1","txn":{"steps":[]}})
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::Mutate {
                mut_id: "m1".into(),
                idempotency_key: Some("key1".into()),
                txn: empty_txn(),
            })
            .unwrap(),
            json!({"type":"mutate","mutId":"m1","idempotencyKey":"key1","txn":{"steps":[]}})
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::Ping).unwrap(),
            json!({"type":"ping"})
        );
    }

    #[test]
    fn client_message_rejects_unknown_fields() {
        let raw = json!({"type":"auth","token":"t","db":"d","bogus":true});
        assert!(serde_json::from_value::<ClientMessage>(raw).is_err());
    }

    #[test]
    fn server_message_tags_and_fields() {
        let ok = serde_json::to_value(ServerMessage::AuthOk {
            user: AuthedUser {
                kind: UserKind::User,
                email: Some("a@b.com".into()),
                name: None,
                github_login: None,
                github_id: None,
            },
        })
        .unwrap();
        assert_eq!(ok["type"], json!("authOk"));
        assert_eq!(
            serde_json::to_value(ServerMessage::QueryUpdate {
                query_id: "q1".into(),
                result: json!([]),
            })
            .unwrap(),
            json!({"type":"queryUpdate","queryId":"q1","result":[]})
        );
        assert_eq!(
            serde_json::to_value(ServerMessage::MutateOk {
                mut_id: "m1".into(),
                results: vec![]
            })
            .unwrap(),
            json!({"type":"mutateOk","mutId":"m1","results":[]})
        );
        let err = serde_json::to_value(ServerMessage::MutateErr {
            mut_id: "m1".into(),
            error: crate::error::RtDbError::new(crate::error::ErrorCode::NotFound, "x"),
        })
        .unwrap();
        assert_eq!(err["type"], json!("mutateErr"));
        let serr = serde_json::to_value(ServerMessage::SubscribeErr {
            query_id: "q1".into(),
            error: crate::error::RtDbError::new(crate::error::ErrorCode::BadRequest, "bad index"),
        })
        .unwrap();
        assert_eq!(
            serr,
            json!({"type":"subscribeErr","queryId":"q1","error":{"code":"BAD_REQUEST","message":"bad index"}})
        );
        assert_eq!(
            serde_json::to_value(ServerMessage::Pong).unwrap(),
            json!({"type":"pong"})
        );
    }

    #[test]
    fn client_message_round_trips_through_json() {
        let msg = ClientMessage::Subscribe {
            query_id: "q1".into(),
            query: Box::new(sample_query()),
        };
        let value = serde_json::to_value(&msg).unwrap();
        let restored: ClientMessage = serde_json::from_value(value).unwrap();
        assert!(matches!(restored, ClientMessage::Subscribe { query_id, .. } if query_id == "q1"));
    }

    // FilterExpr/SearchQuery wire shapes are byte-identical to server query.rs.
    #[test]
    fn search_query_wire_shape() {
        let q = SearchQuery {
            index: "search_content".into(),
            query: "hello world".into(),
        };
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"index":"search_content","query":"hello world"})
        );
        let back: SearchQuery =
            serde_json::from_value(json!({"index":"search_content","query":"hello world"}))
                .unwrap();
        assert_eq!(back.index, "search_content");
    }

    #[test]
    fn vector_search_query_wire_shape() {
        let q = VectorSearchQuery {
            index: "by_embedding".into(),
            vector: vec![1.0, 0.0, 0.0],
            limit: 5,
            filter: BTreeMap::new(),
        };
        // Empty `filter` is omitted on the wire (skip_serializing_if).
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"index":"by_embedding","vector":[1.0,0.0,0.0],"limit":5})
        );
        // Round-trips; absent filter deserializes to empty.
        let back: VectorSearchQuery = serde_json::from_value(json!({
            "index": "by_embedding",
            "vector": [1.0, 0.0, 0.0],
            "limit": 5
        }))
        .unwrap();
        assert_eq!(back.index, "by_embedding");
        assert_eq!(back.limit, 5);
        assert!(back.filter.is_empty());
        // deny_unknown_fields: extra key rejected.
        assert!(
            serde_json::from_value::<VectorSearchQuery>(json!({
                "index": "by_embedding",
                "vector": [1.0],
                "limit": 5,
                "bogus": true
            }))
            .is_err()
        );
        // A non-empty filter round-trips through the wire.
        let mut filter = BTreeMap::new();
        filter.insert("userId".into(), json!("u1"));
        let with_filter = VectorSearchQuery {
            index: "by_embedding".into(),
            vector: vec![1.0],
            limit: 3,
            filter,
        };
        assert_eq!(
            serde_json::to_value(&with_filter).unwrap(),
            json!({"index":"by_embedding","vector":[1.0],"limit":3,"filter":{"userId":"u1"}})
        );
    }

    #[test]
    fn hybrid_search_query_wire_shape() {
        // Required fields only — optional searchIndex/vectorIndex/k are omitted.
        let q = HybridSearchQuery {
            query: "hello world".into(),
            vector: vec![1.0, 0.0, 0.0],
            limit: 5,
            search_index: None,
            vector_index: None,
            k: None,
        };
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"query":"hello world","vector":[1.0,0.0,0.0],"limit":5})
        );
        // Round-trips; absent optionals deserialize to None.
        let back: HybridSearchQuery = serde_json::from_value(json!({
            "query": "hello world",
            "vector": [1.0, 0.0, 0.0],
            "limit": 5
        }))
        .unwrap();
        assert_eq!(back.query, "hello world");
        assert_eq!(back.limit, 5);
        assert!(back.search_index.is_none());
        assert!(back.vector_index.is_none());
        assert!(back.k.is_none());
        // deny_unknown_fields: extra key rejected.
        assert!(
            serde_json::from_value::<HybridSearchQuery>(json!({
                "query": "x", "vector": [1.0], "limit": 1, "bogus": true
            }))
            .is_err()
        );
        // Explicit optionals round-trip through the wire (camelCase keys).
        let full = HybridSearchQuery {
            query: "x".into(),
            vector: vec![1.0],
            limit: 1,
            search_index: Some("search_body".into()),
            vector_index: Some("by_embedding".into()),
            k: Some(42),
        };
        assert_eq!(
            serde_json::to_value(&full).unwrap(),
            json!({
                "query": "x",
                "vector": [1.0],
                "limit": 1,
                "searchIndex": "search_body",
                "vectorIndex": "by_embedding",
                "k": 42
            })
        );
    }

    #[test]
    fn filter_expr_leaf_tags_and_fields() {
        assert_eq!(
            serde_json::to_value(FilterExpr::Eq {
                field: "status".into(),
                value: json!("done")
            })
            .unwrap(),
            json!({"op":"eq","field":"status","value":"done"})
        );
        assert_eq!(
            serde_json::to_value(FilterExpr::Neq {
                field: "archived".into(),
                value: json!(true)
            })
            .unwrap(),
            json!({"op":"neq","field":"archived","value":true})
        );
        assert_eq!(
            serde_json::to_value(FilterExpr::Gt {
                field: "order".into(),
                value: json!(5)
            })
            .unwrap(),
            json!({"op":"gt","field":"order","value":5})
        );
        assert_eq!(
            serde_json::to_value(FilterExpr::Gte {
                field: "order".into(),
                value: json!(5)
            })
            .unwrap(),
            json!({"op":"gte","field":"order","value":5})
        );
        assert_eq!(
            serde_json::to_value(FilterExpr::Lt {
                field: "order".into(),
                value: json!(5)
            })
            .unwrap(),
            json!({"op":"lt","field":"order","value":5})
        );
        assert_eq!(
            serde_json::to_value(FilterExpr::Lte {
                field: "order".into(),
                value: json!(5)
            })
            .unwrap(),
            json!({"op":"lte","field":"order","value":5})
        );
        assert_eq!(
            serde_json::to_value(FilterExpr::In {
                field: "status".into(),
                values: vec![json!("a"), json!("b")]
            })
            .unwrap(),
            json!({"op":"in","field":"status","values":["a","b"]})
        );
    }

    #[test]
    fn filter_expr_combinators_nest() {
        let and = FilterExpr::And {
            exprs: vec![
                FilterExpr::Eq {
                    field: "status".into(),
                    value: json!("done"),
                },
                FilterExpr::Gt {
                    field: "order".into(),
                    value: json!(0),
                },
            ],
        };
        assert_eq!(
            serde_json::to_value(&and).unwrap(),
            json!({"op":"and","exprs":[
                {"op":"eq","field":"status","value":"done"},
                {"op":"gt","field":"order","value":0}
            ]})
        );
        let or = FilterExpr::Or {
            exprs: vec![
                FilterExpr::Eq {
                    field: "status".into(),
                    value: json!("backlog"),
                },
                FilterExpr::In {
                    field: "status".into(),
                    values: vec![json!("blocked")],
                },
            ],
        };
        assert_eq!(serde_json::to_value(&or).unwrap()["op"], json!("or"));
    }

    #[test]
    fn filter_expr_round_trips_and_rejects_unknown_fields() {
        let expr = FilterExpr::Or {
            exprs: vec![FilterExpr::Eq {
                field: "x".into(),
                value: json!(1),
            }],
        };
        let v = serde_json::to_value(&expr).unwrap();
        let back: FilterExpr = serde_json::from_value(v).unwrap();
        assert!(matches!(back, FilterExpr::Or { exprs } if exprs.len() == 1));
        // deny_unknown_fields: an extra key is rejected.
        let bad = json!({"op":"eq","field":"x","value":1,"bogus":true});
        assert!(serde_json::from_value::<FilterExpr>(bad).is_err());
        // Unknown op tag is rejected.
        assert!(
            serde_json::from_value::<FilterExpr>(json!({"op":"between","field":"x","value":1}))
                .is_err()
        );
    }

    // New variants mirroring server FilterExpr (Task 1, commit b6b6c2a):
    // `not` / `contains` / `exists` — wire shapes must match the server byte-for-byte.
    #[test]
    fn filter_expr_not_contains_exists_variants() {
        // `Not` wraps a nested FilterExpr (Box) — {"op":"not","expr":{...}}
        let not = FilterExpr::Not {
            expr: Box::new(FilterExpr::Eq {
                field: "status".into(),
                value: json!("done"),
            }),
        };
        assert_eq!(
            serde_json::to_value(&not).unwrap(),
            json!({"op":"not","expr":{"op":"eq","field":"status","value":"done"}})
        );
        let back: FilterExpr = serde_json::from_value(serde_json::to_value(&not).unwrap()).unwrap();
        assert!(matches!(back, FilterExpr::Not { .. }));

        // `Contains`: value ∈ doc.field[] — {"op":"contains","field","value"}
        let contains = FilterExpr::Contains {
            field: "tags".into(),
            value: json!("red"),
        };
        assert_eq!(
            serde_json::to_value(&contains).unwrap(),
            json!({"op":"contains","field":"tags","value":"red"})
        );

        // `Exists`: field present and non-null — {"op":"exists","field"}
        let exists = FilterExpr::Exists {
            field: "dueAt".into(),
        };
        assert_eq!(
            serde_json::to_value(&exists).unwrap(),
            json!({"op":"exists","field":"dueAt"})
        );

        // deny_unknown_fields applies to the new variants too.
        assert!(
            serde_json::from_value::<FilterExpr>(
                json!({"op":"not","expr":{"op":"eq","field":"x","value":1},"bogus":true})
            )
            .is_err()
        );
        assert!(
            serde_json::from_value::<FilterExpr>(
                json!({"op":"contains","field":"x","value":1,"bogus":true})
            )
            .is_err()
        );
        assert!(
            serde_json::from_value::<FilterExpr>(json!({"op":"exists","field":"x","bogus":true}))
                .is_err()
        );
    }

    // Schedule wire shapes are byte-identical to server protocol.rs.
    #[test]
    fn schedule_when_wire_tags() {
        assert_eq!(
            serde_json::to_value(ScheduleWhen::AfterMs { ms: 5 }).unwrap(),
            json!({"type": "afterMs", "ms": 5})
        );
        assert_eq!(
            serde_json::to_value(ScheduleWhen::RunAt { ms: 9 }).unwrap(),
            json!({"type": "runAt", "ms": 9})
        );
        assert_eq!(
            serde_json::to_value(ScheduleWhen::Cron {
                expr: "*/5 * * * *".into()
            })
            .unwrap(),
            json!({"type": "cron", "expr": "*/5 * * * *"})
        );
        // deny_unknown_fields.
        assert!(
            serde_json::from_value::<ScheduleWhen>(json!({"type": "afterMs", "ms": 1, "x": 9}))
                .is_err()
        );
    }

    #[test]
    fn schedule_client_message_variants() {
        let s = serde_json::to_value(ClientMessage::Schedule {
            schedule_id: "s1".into(),
            when: ScheduleWhen::AfterMs { ms: 100 },
            txn: empty_txn(),
        })
        .unwrap();
        assert_eq!(
            s,
            json!({
                "type": "schedule",
                "scheduleId": "s1",
                "when": {"type": "afterMs", "ms": 100},
                "txn": {"steps": []}
            })
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::CancelSchedule {
                schedule_id: "s1".into(),
                id: "job-1".into(),
            })
            .unwrap(),
            json!({"type": "cancelSchedule", "scheduleId": "s1", "id": "job-1"})
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::PauseSchedule {
                schedule_id: "s1".into(),
                id: "job-1".into(),
            })
            .unwrap(),
            json!({"type": "pauseSchedule", "scheduleId": "s1", "id": "job-1"})
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::ResumeSchedule {
                schedule_id: "s1".into(),
                id: "job-1".into(),
            })
            .unwrap(),
            json!({"type": "resumeSchedule", "scheduleId": "s1", "id": "job-1"})
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::ListSchedules {
                schedule_id: "s1".into()
            })
            .unwrap(),
            json!({"type": "listSchedules", "scheduleId": "s1"})
        );
    }

    #[test]
    fn schedule_server_message_variants() {
        assert_eq!(
            serde_json::to_value(ServerMessage::ScheduleOk {
                schedule_id: "s1".into(),
                id: "job-9".into(),
            })
            .unwrap(),
            json!({"type": "scheduleOk", "scheduleId": "s1", "id": "job-9"})
        );
        let err = serde_json::to_value(ServerMessage::ScheduleErr {
            schedule_id: "s1".into(),
            error: crate::error::RtDbError::new(crate::error::ErrorCode::BadRequest, "bad cron"),
        })
        .unwrap();
        assert_eq!(
            err,
            json!({
                "type": "scheduleErr",
                "scheduleId": "s1",
                "error": {"code": "BAD_REQUEST", "message": "bad cron"}
            })
        );
        // `error` is skipped on the wire when `None`.
        let ack_ok = serde_json::to_value(ServerMessage::ScheduleAck {
            schedule_id: "s1".into(),
            ok: true,
            error: None,
        })
        .unwrap();
        assert_eq!(
            ack_ok,
            json!({"type": "scheduleAck", "scheduleId": "s1", "ok": true})
        );
        // An `ok:false` ack carries its error envelope.
        let ack_err = serde_json::to_value(ServerMessage::ScheduleAck {
            schedule_id: "s1".into(),
            ok: false,
            error: Some(crate::error::RtDbError::new(
                crate::error::ErrorCode::NotFound,
                "missing job",
            )),
        })
        .unwrap();
        assert_eq!(
            ack_err,
            json!({
                "type": "scheduleAck",
                "scheduleId": "s1",
                "ok": false,
                "error": {"code": "NOT_FOUND", "message": "missing job"}
            })
        );
        assert_eq!(
            serde_json::to_value(ServerMessage::ListSchedulesOk {
                schedule_id: "s1".into(),
                schedules: vec![],
            })
            .unwrap(),
            json!({"type": "listSchedulesOk", "scheduleId": "s1", "schedules": []})
        );
    }

    #[test]
    fn schedule_info_round_trip_omits_absent_optionals() {
        let oneshot = ScheduleInfo {
            id: "j1".into(),
            kind: ScheduleKind::Oneshot,
            due_at: 1000,
            cron: None,
            status: ScheduleStatus::Pending,
            last_error: None,
            created_at: 500,
            fired_count: 0,
        };
        let v = serde_json::to_value(&oneshot).unwrap();
        assert_eq!(
            v,
            json!({
                "id": "j1",
                "kind": "oneshot",
                "dueAt": 1000,
                "status": "pending",
                "createdAt": 500,
                "firedCount": 0
            })
        );
        let cron = ScheduleInfo {
            id: "j2".into(),
            kind: ScheduleKind::Cron,
            due_at: 2000,
            cron: Some("*/5 * * * *".into()),
            status: ScheduleStatus::Error,
            last_error: Some("boom".into()),
            created_at: 500,
            fired_count: 3,
        };
        let v = serde_json::to_value(&cron).unwrap();
        assert_eq!(
            v,
            json!({
                "id": "j2",
                "kind": "cron",
                "dueAt": 2000,
                "cron": "*/5 * * * *",
                "status": "error",
                "lastError": "boom",
                "createdAt": 500,
                "firedCount": 3
            })
        );
        // Round-trips back.
        let back: ScheduleInfo = serde_json::from_value(v).unwrap();
        assert_eq!(back.cron.as_deref(), Some("*/5 * * * *"));
        assert_eq!(back.last_error.as_deref(), Some("boom"));
    }

    // ---- admin migrate wire (tag `op`, camelCase, `where` alias) -----------
    #[cfg(feature = "admin")]
    #[test]
    fn migrate_directive_round_trip() {
        use crate::schema::FieldType;
        use crate::wire::admin::{Cast, Directive, MigrateRequest, MigrateResult};

        let req = MigrateRequest {
            directives: &[
                Directive::RenameField {
                    table: "users".into(),
                    from: "name".into(),
                    to: "fullName".into(),
                },
                Directive::ChangeType {
                    table: "users".into(),
                    field: "age".into(),
                    to: FieldType::String,
                    cast: Cast::ToString,
                    default: None,
                },
                Directive::EvalExpr {
                    table: "users".into(),
                    set: "upper".into(),
                    expr: "upper(doc->>'fullName')".into(),
                    where_clause: Some("doc ? 'fullName'".into()),
                },
            ],
            dry_run: true,
        };
        let json = serde_json::to_value(&req).unwrap();
        // tag is "op", camelCase keys, `where` alias.
        assert_eq!(json["directives"][0]["op"], "renameField");
        assert_eq!(json["directives"][1]["op"], "changeType");
        assert_eq!(json["directives"][1]["cast"], "toString");
        assert_eq!(json["directives"][2]["op"], "evalExpr");
        assert_eq!(json["directives"][2]["where"], "doc ? 'fullName'");
        // `where_clause` must not appear under its snake-case name.
        assert!(json["directives"][2].get("where_clause").is_none());
        assert_eq!(json["dryRun"], true);

        // Borrowed request round-trips into the owned variants; `MigrateRequest`
        // itself is Serialize-only (borrowed slice), so deserialize via
        // `MigrateRequestOwned`'s shape by re-serializing each directive.
        let ops_json = json["directives"].as_array().unwrap().clone();
        for (i, d) in [
            Directive::RenameField {
                table: "users".into(),
                from: "name".into(),
                to: "fullName".into(),
            },
            Directive::ChangeType {
                table: "users".into(),
                field: "age".into(),
                to: FieldType::String,
                cast: Cast::ToString,
                default: None,
            },
            Directive::EvalExpr {
                table: "users".into(),
                set: "upper".into(),
                expr: "upper(doc->>'fullName')".into(),
                where_clause: Some("doc ? 'fullName'".into()),
            },
        ]
        .iter()
        .enumerate()
        {
            let dumped = serde_json::to_value(d).unwrap();
            assert_eq!(dumped, ops_json[i], "directive {i} drifted");
            // Each directive round-trips through Deserialize.
            let _: &Directive = &serde_json::from_value::<Directive>(dumped).unwrap();
        }

        // MigrateResult deserializes the server shape (camelCase, nested
        // reports carry `affectedRows`).
        let resp = json!({
            "applied": true,
            "schema": {"tables": {"users": {"fields": {"fullName": {"type": "string"}}}}},
            "directives": [
                {"op": "renameField", "affectedRows": 3},
                {"op": "changeType", "affectedRows": 3, "castFailures": [{"id": "u1", "value": null}]}
            ]
        });
        let parsed: MigrateResult = serde_json::from_value(resp).unwrap();
        assert!(parsed.applied);
        assert_eq!(parsed.directives.len(), 2);
        assert_eq!(parsed.directives[0].op, "renameField");
        assert_eq!(parsed.directives[0].affected_rows, 3);
        assert_eq!(parsed.directives[1].cast_failures.len(), 1);
        assert_eq!(parsed.directives[1].cast_failures[0].id, "u1");
        // Re-serialize drops empty `cast_failures`/`sampleChanges` (skip_if_empty)
        // but keeps the populated one.
        let back = serde_json::to_value(&parsed).unwrap();
        assert!(back["directives"][0].get("castFailures").is_none());
        assert_eq!(back["directives"][1]["castFailures"][0]["id"], "u1");
    }
}
