//! Wire vocabulary — the third implementation of the protocol contract
//! (server `protocol.rs` first, TS `protocol.ts` second). Tags/fields are load-bearing.

use crate::error::{ErrorEnvelope, RtDbError};
use crate::mutation::Transaction;
use crate::query::Query;
use serde::{Deserialize, Serialize};

/// Alias kept for naming continuity with the server's protocol module.
pub type QueryRef = Query;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
/// A client -> server `/sync` frame (tagged `{"type": "..."}`, camelCase).
pub enum ClientMessage {
    /// Authenticate the socket (first frame).
    Auth {
        // SEC-001 phase 2: optional — a browser dashboard authenticates over
        // `/sync` from the HttpOnly cookie, sending only `db`. CLI/SDK/machine
        // tokens still send `token` (the prior wire form); backward-compatible.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// Bearer token; `None` when authenticating from the session cookie.
        token: Option<String>,
        /// The database to authorize against.
        db: String,
    },
    /// Start a live query subscription.
    Subscribe {
        /// Caller-chosen correlation id for subsequent updates.
        query_id: String,
        /// The built query.
        query: Box<Query>,
    },
    /// Stop a subscription.
    Unsubscribe {
        /// The subscription to stop.
        query_id: String,
    },
    /// Run a transaction.
    Mutate {
        /// Caller-chosen correlation id for the reply.
        mut_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// Optional idempotency key (same key replays the cached result).
        idempotency_key: Option<String>,
        /// The transaction to apply.
        txn: Transaction,
    },
    /// Schedule a transaction for later.
    Schedule {
        /// Caller-chosen correlation id for the reply.
        schedule_id: String,
        /// One-shot delay/absolute time, or cron.
        when: ScheduleWhen,
        /// The transaction to fire when due.
        txn: Transaction,
    },
    /// Cancel a scheduled job.
    CancelSchedule {
        /// Correlation id for the reply.
        schedule_id: String,
        /// The job to cancel.
        id: String,
    },
    /// Pause a cron job.
    PauseSchedule {
        /// Correlation id for the reply.
        schedule_id: String,
        /// The job to pause.
        id: String,
    },
    /// Resume a paused cron job.
    ResumeSchedule {
        /// Correlation id for the reply.
        schedule_id: String,
        /// The job to resume.
        id: String,
    },
    /// List scheduled jobs.
    ListSchedules {
        /// Correlation id for the reply.
        schedule_id: String,
    },
    /// Start a durable workflow run.
    StartWorkflow {
        /// Caller-chosen correlation id for the reply.
        workflow_id: String,
        /// The run's spec (snapshotted server-side).
        spec: WorkflowSpec,
    },
    /// Cancel a workflow run.
    CancelWorkflow {
        /// Correlation id for the reply.
        workflow_id: String,
        /// The run to cancel.
        id: String,
    },
    /// List workflow runs.
    ListWorkflows {
        /// Correlation id for the reply.
        workflow_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// Optional lifecycle filter.
        status: Option<WorkflowStatus>,
    },
    /// Join a presence room (idempotent re-join refreshes `state`).
    Presence {
        /// Room name.
        room: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// Opaque client-supplied presence blob.
        state: Option<serde_json::Value>,
    },
    /// Update presence state without (re)joining.
    PresenceState {
        /// Room name.
        room: String,
        /// The new opaque blob (replaces the old).
        state: serde_json::Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// Per-state expiry override.
        ttl_ms: Option<u64>,
    },
    /// Leave a presence room.
    LeavePresence {
        /// Room to leave.
        room: String,
    },
    /// Keepalive; the server replies `Pong`.
    Ping,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
/// A server -> client `/sync` frame (tagged `{"type": "..."}`, camelCase).
pub enum ServerMessage {
    /// Authentication succeeded.
    AuthOk {
        /// The authed principal.
        user: AuthedUser,
    },
    /// Authentication failed; the socket closes.
    AuthErr {
        /// Why.
        error: RtDbError,
    },
    /// A live query's new result (sent only on change).
    QueryUpdate {
        /// Which subscription.
        query_id: String,
        /// The query's new full result value.
        result: serde_json::Value,
    },
    /// Transaction applied; one entry per step.
    MutateOk {
        /// Correlation id from the request.
        mut_id: String,
        /// Positionally aligned step results.
        results: Vec<serde_json::Value>,
    },
    /// Transaction failed and rolled back.
    MutateErr {
        /// Correlation id from the request.
        mut_id: String,
        /// Why.
        error: RtDbError,
    },
    /// Subscription rejected (bad query, authz).
    SubscribeErr {
        /// Which subscription.
        query_id: String,
        /// Why.
        error: RtDbError,
    },
    /// Job scheduled.
    ScheduleOk {
        /// Correlation id from the request.
        schedule_id: String,
        /// The created job's id.
        id: String,
    },
    /// Scheduling failed.
    ScheduleErr {
        /// Correlation id from the request.
        schedule_id: String,
        /// Why.
        error: RtDbError,
    },
    /// Reply to cancel/pause/resume. `error` is omitted on the wire when `ok`.
    ScheduleAck {
        /// Correlation id from the request.
        schedule_id: String,
        /// Whether the action applied.
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        /// Why it failed (absent when ok).
        error: Option<RtDbError>,
    },
    /// Reply to `listSchedules`.
    ListSchedulesOk {
        /// Correlation id from the request.
        schedule_id: String,
        /// All jobs for the db.
        schedules: Vec<ScheduleInfo>,
    },
    /// Run started.
    StartWorkflowOk {
        /// Correlation id from the request.
        workflow_id: String,
        /// The run's initial info row.
        info: WorkflowInfo,
    },
    /// Run rejected (spec validation, authz).
    StartWorkflowErr {
        /// Correlation id from the request.
        workflow_id: String,
        /// Why.
        error: RtDbError,
    },
    /// Reply to cancelWorkflow — and, per the server's single-error-frame
    /// design, to a failed `listWorkflows` too (no `listWorkflowsErr` frame
    /// exists; the list's correlation id rides `StartWorkflowErr`). `error`
    /// is omitted on the wire when `ok`.
    WorkflowAck {
        /// Correlation id from the request.
        workflow_id: String,
        /// Whether the action applied.
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        /// Why it failed (absent when ok).
        error: Option<RtDbError>,
    },
    /// Reply to `listWorkflows`.
    ListWorkflowsOk {
        /// Correlation id from the request.
        workflow_id: String,
        /// Matching runs, newest first.
        workflows: Vec<WorkflowInfo>,
    },
    /// Full room membership (on join and on every change).
    PresenceSnapshot {
        /// Which room.
        room: String,
        /// Everyone currently in the room.
        members: Vec<PresenceMember>,
    },
    /// Presence operation failed.
    PresenceErr {
        /// Which room.
        room: String,
        /// Why.
        error: RtDbError,
    },
    /// Reply to `Ping`.
    Pong,
}

/// Whether an `AuthedUser` resolved from an OAuth session or a machine token.
/// Mirrors `server/src/protocol.rs::UserKind` (ARC-004/QA-008): serializes as
/// `"user"` / `"machine"`, byte-identical to the prior `String` form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UserKind {
    /// An OAuth/anonymous session principal.
    User,
    /// A machine token principal.
    Machine,
}

impl UserKind {
    /// The wire string (`"user"` / `"machine"`).
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
/// The authenticated principal returned by `authOk` / `GET /auth/me`.
pub struct AuthedUser {
    /// Session user or machine token.
    pub kind: UserKind,
    /// Email when known (always `None` for anonymous/machine).
    pub email: Option<String>,
    /// Display name when known.
    pub name: Option<String>,
    /// GitHub login. Absent on the wire for machine tokens / non-GitHub
    /// users; serde defaults a missing field to `None` so this stays
    /// backward-compatible with older servers that omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub github_login: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// GitHub numeric id when the login was via GitHub.
    pub github_id: Option<i64>,
}

/// One entry in a presence room's member list. Mirrors
/// `server/src/protocol.rs::PresenceMember` byte-for-byte (camelCase):
/// `connectionId` is the opaque per-session key, `user` carries display
/// identity, `state` is an opaque client-supplied blob.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PresenceMember {
    /// Opaque per-session key (the liveness unit).
    pub connection_id: String,
    /// Who is connected.
    pub user: AuthedUser,
    /// Their opaque presence blob.
    pub state: serde_json::Value,
}

/// How a caller wants a transaction scheduled. Mirrored byte-for-byte in
/// `server/src/protocol.rs` and `ts-client/src/protocol.ts`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum ScheduleWhen {
    /// Fire `ms` milliseconds from now.
    AfterMs {
        /// Milliseconds from now.
        ms: i64,
    },
    /// Fire at this UTC epoch-ms instant (in the past = fire immediately).
    RunAt {
        /// Absolute UTC epoch-ms instant.
        ms: i64,
    },
    /// Fire on this 5-field cron schedule (UTC, min-first). The server validates
    /// the expression; the client does no cron parsing.
    Cron {
        /// 5-field cron expression (UTC, min-first).
        expr: String,
    },
}

/// Whether a scheduled job fires once or repeats on cron. Mirrors
/// `server/src/protocol.rs::ScheduleKind` (ARC-004/QA-008): serializes as
/// `"oneshot"` / `"cron"`, byte-identical to the prior `String` form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleKind {
    /// Fires once.
    Oneshot,
    /// Repeats on a cron schedule.
    Cron,
}

impl ScheduleKind {
    /// The wire string (`"oneshot"` / `"cron"`).
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
    /// Waiting for its due time.
    Pending,
    /// Currently executing (crash-recovered at startup).
    Running,
    /// Held by pause; resume re-arms it.
    Paused,
    /// Failed; `last_error` carries why.
    Error,
}

impl ScheduleStatus {
    /// The wire string (`"pending"` etc.).
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
    /// Opaque job id.
    pub id: String,
    /// One-shot or cron.
    pub kind: ScheduleKind,
    /// Next due time, epoch ms.
    pub due_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// The cron expression, for cron jobs.
    pub cron: Option<String>,
    /// Lifecycle state.
    pub status: ScheduleStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// The last firing error, if any.
    pub last_error: Option<String>,
    /// Creation time, epoch ms.
    pub created_at: i64,
    /// Times fired.
    pub fired_count: i64,
}

/// Per-step retry policy (FM-29). `maxAttempts` counts TOTAL attempts — the
/// first try included. Defaults when a step omits `retry`: 3 attempts, 1s
/// initial backoff doubling to a 60s cap. Mirrors
/// `server/src/protocol.rs::StepRetry` byte-for-byte.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StepRetry {
    /// TOTAL attempts including the first.
    pub max_attempts: u32,
    #[serde(default = "default_initial_retry_ms")]
    /// First backoff delay; doubles per retry.
    pub initial_retry_ms: u64,
    #[serde(default = "default_max_retry_ms")]
    /// Backoff cap.
    pub max_retry_ms: u64,
}

fn default_initial_retry_ms() -> u64 {
    1_000
}

fn default_max_retry_ms() -> u64 {
    60_000
}

impl Default for StepRetry {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_retry_ms: 1_000,
            max_retry_ms: 60_000,
        }
    }
}

/// One workflow step: an ordinary [`Transaction`] plus policy. The txn may
/// itself carry `Schedule`/`CancelSchedule` steps (FM-28 rules apply).
/// Mirrors `server/src/protocol.rs::WorkflowStepSpec` byte-for-byte.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkflowStepSpec {
    /// The step's transaction.
    pub txn: Transaction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Per-step policy (server default 3/1s/60s when `None`).
    pub retry: Option<StepRetry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Gate the step until this many ms have passed.
    pub sleep_before_ms: Option<u64>,
}

/// A submitted workflow definition. Stored verbatim per run — a run
/// snapshots its spec, so template edits never drift a live run. Mirrors
/// `server/src/protocol.rs::WorkflowSpec` byte-for-byte.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkflowSpec {
    /// Run name (operator-facing).
    pub name: String,
    /// Ordered steps; snapshot per run.
    pub steps: Vec<WorkflowStepSpec>,
}

/// Run lifecycle (FM-29). Closed domain (ARC-004/QA-008 pattern). Snake-case
/// wire: pending|running|success|failed|cancelled. Mirrors
/// `server/src/protocol.rs::WorkflowStatus` byte-for-byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus {
    /// Not yet advanced.
    Pending,
    /// Mid-run (or crashed mid-run, pre-recovery).
    Running,
    /// All steps completed.
    Success,
    /// A step exhausted its retries (terminal).
    Failed,
    /// Cancelled by request (terminal).
    Cancelled,
}

impl WorkflowStatus {
    /// The wire string (`"pending"` etc.).
    pub fn as_wire_str(&self) -> &'static str {
        match self {
            WorkflowStatus::Pending => "pending",
            WorkflowStatus::Running => "running",
            WorkflowStatus::Success => "success",
            WorkflowStatus::Failed => "failed",
            WorkflowStatus::Cancelled => "cancelled",
        }
    }
}

impl std::str::FromStr for WorkflowStatus {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(WorkflowStatus::Pending),
            "running" => Ok(WorkflowStatus::Running),
            "success" => Ok(WorkflowStatus::Success),
            "failed" => Ok(WorkflowStatus::Failed),
            "cancelled" => Ok(WorkflowStatus::Cancelled),
            other => Err(format!("unknown WorkflowStatus: {other}")),
        }
    }
}

/// Terminal record for one step: completed successfully, or exhausted its
/// retries (`status: failed`). Individual retried attempts are NOT recorded —
/// the `attempts` count on the entry (and on the row) carries them. Mirrors
/// `server/src/protocol.rs::StepOutcome` byte-for-byte.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StepOutcome {
    /// Which step (0-based).
    pub step_index: u32,
    /// Success or failed.
    pub status: OutcomeStatus,
    /// Total attempts (retries included).
    pub attempts: u32,
    /// Completion time, epoch ms.
    pub at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// The final error on failure.
    pub error: Option<String>,
}

/// Mirrors `server/src/protocol.rs::OutcomeStatus` byte-for-byte (lowercase).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutcomeStatus {
    /// The step completed.
    Success,
    /// The step exhausted its retries.
    Failed,
}

/// List/get projection of one run (FM-29). Optional fields are omitted on the
/// wire when absent. Mirrors `server/src/protocol.rs::WorkflowInfo`
/// byte-for-byte.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkflowInfo {
    /// Opaque run id.
    pub id: String,
    /// The spec's name.
    pub name: String,
    /// Lifecycle state.
    pub status: WorkflowStatus,
    /// 0-based index of the next/current step.
    pub current_step: u32,
    /// Total steps in the spec.
    pub step_count: u32,
    /// Current step's total attempts so far.
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Epoch-ms gate before the next advance.
    pub sleep_until: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// The failure that ended the run.
    pub last_error: Option<String>,
    /// Submission time, epoch ms.
    pub created_at: i64,
    /// Last advance time, epoch ms.
    pub updated_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// First advance time.
    pub started_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Terminal transition time.
    pub finished_at: Option<i64>,
}

/// `GET .../{id}` shape: the info row plus the per-step outcome trail.
/// Mirrors `server/src/protocol.rs::WorkflowInfoFull` byte-for-byte (the info
/// fields flatten alongside `stepOutcomes`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkflowInfoFull {
    #[serde(flatten)]
    /// The run's info projection.
    pub info: WorkflowInfo,
    /// Terminal record per completed step.
    pub step_outcomes: Vec<StepOutcome>,
}

/// A full-text search terminal over a declared search index. `index` names a
/// search index on the query's table; `query` is free-form user text parsed
/// with web-search operator syntax (FM-31) — quoted phrases require
/// adjacency, the bare word `or` unions, `-term` excludes, plain terms stay
/// ANDed; `filter` is an optional `FilterExpr` (the db-side `filter()` DSL)
/// that narrows the search `WHERE` server-side; `mode` selects the match
/// strategy (FM-30) — `None`/`"tsquery"` is today's full-text behavior,
/// `"trgm"` is substring/autocomplete matching over the index's text fields
/// (see [`SearchMode`]). `snippet` (FM-31) opts each hit into a
/// `_searchSnippet` field — a server-rendered `<mark>`-highlighted fragment;
/// tsquery mode only. Mirrors `server/src/query.rs::SearchQuery`
/// byte-for-byte (camelCase, deny_unknown_fields). The nested `filter`,
/// `mode`, and `snippet` are additive — omitted when `None`, so existing
/// search requests round-trip unchanged — and `filter` is distinct from the
/// Query-level top-level `filter` builder.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SearchQuery {
    /// The declared search index.
    pub index: String,
    /// Free-form user text (web-search operator syntax).
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional `FilterExpr` narrowing the `WHERE`.
    pub filter: Option<FilterExpr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// `None` = tsquery (default); `Trgm` = substring matching.
    pub mode: Option<SearchMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// `Some(true)` adds a `_searchSnippet` per hit (tsquery only).
    pub snippet: Option<bool>,
}

/// Match mode for the `search` terminal. `Tsquery` (the default, and the
/// behavior when `mode` is omitted) matches stemmed words via
/// `tsvector @@ websearch_to_tsquery`, ranked by `ts_rank`. `Trgm` matches
/// substrings case-insensitively (`ILIKE '%query%'`) over the search index's
/// text fields — prefix/infix/autocomplete lookups FTS can't serve — ranked
/// by `GREATEST(similarity(field, query))` (the doc's best-matching field),
/// with `created_at`/`id` tiebreaks for determinism. Wire form is lowercase
/// (`"tsquery"` | `"trgm"`); serialized only when the caller opts in, so
/// existing traffic stays byte-identical. Mirrors
/// `server/src/query.rs::SearchMode` byte-for-byte (lowercase variants).
/// docs/superpowers/specs/2026-08-15-trgm-search-design.md.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SearchMode {
    #[default]
    /// Stemmed full-text via `websearch_to_tsquery` (default).
    Tsquery,
    /// Case-insensitive substring/autocomplete via `pg_trgm`.
    Trgm,
}

/// A vector-similarity terminal over a declared vector index. `vector` is the
/// caller-supplied query embedding (length must equal the index dimensions);
/// ranked by cosine distance ascending. `filter` is an optional `FilterExpr`
/// (the db-side `filter()` DSL) that narrows the vector search `WHERE`
/// server-side. Mirrors `server/src/query.rs::VectorSearchQuery` byte-for-byte
/// (camelCase, deny_unknown_fields). The nested `filter` is additive — omitted
/// when `None`, so existing vector-search requests round-trip unchanged — and,
/// like the standalone `search` terminal, may reference any field (not just
/// declared filterFields).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VectorSearchQuery {
    /// The declared vector index.
    pub index: String,
    // ARC-008(a): f64 (not f32) — the server, TS, and Python clients all carry
    // full JSON-number precision, so narrowing to f32 here was the lone path
    // that silently dropped precision on a round-trip. f64 matches the wire.
    /// Query embedding (length = index dimensions).
    pub vector: Vec<f64>,
    /// Max neighbors to return.
    pub limit: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional `FilterExpr` narrowing the `WHERE`.
    pub filter: Option<FilterExpr>,
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
    /// Full-text query text.
    pub query: String,
    /// Query embedding.
    pub vector: Vec<f64>,
    /// Fused result size.
    pub limit: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Named search index (auto-selected when `None`).
    pub search_index: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Named vector index (auto-selected when `None`).
    pub vector_index: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// RRF constant (server default 60).
    pub k: Option<u32>,
}

/// Aggregate operator for the `aggregate` terminal. Mirrors
/// `server/src/query.rs::AggregateOp` byte-for-byte (lowercase variants). `Count`
/// aggregates rows and consumes no aggregate field (a grouped `count` is the
/// count per group).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AggregateOp {
    /// Sum (numeric field required).
    Sum,
    /// Average (numeric field required).
    Avg,
    /// Minimum.
    Min,
    /// Maximum.
    Max,
    /// Row count (no aggregate field consumed).
    Count,
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
    /// Which aggregate.
    pub op: AggregateOp,
    #[serde(default, skip_serializing_if = "is_false")]
    /// Group by the index field after the eq prefix.
    pub group_by: bool,
}

/// Serde skip predicate for `bool` fields whose default is `false`. Lets the
/// rust-client omit `groupBy` on the wire when false, matching the TS client.
fn is_false(b: &bool) -> bool {
    !*b
}

/// One `{key, value}` row from a grouped `aggregate` (`groupBy: true`) terminal.
/// Mirrors `server/src/query.rs::AggregateGroup` byte-for-byte (camelCase).
/// ARC-130: response-shaped — `#[non_exhaustive]` lets the wire shape gain
/// fields later without breaking exhaustive destructures.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct AggregateGroup {
    /// The group's key value.
    pub key: serde_json::Value,
    /// The group's aggregate.
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
    /// `field == value`.
    Eq {
        /// The declared field to compare.
        field: String,
        /// The value to compare against.
        value: serde_json::Value,
    },
    /// `field != value`.
    Neq {
        /// The declared field to compare.
        field: String,
        /// The value to compare against.
        value: serde_json::Value,
    },
    /// `field > value`.
    Gt {
        /// The declared field to compare.
        field: String,
        /// The value to compare against.
        value: serde_json::Value,
    },
    /// `field >= value`.
    Gte {
        /// The declared field to compare.
        field: String,
        /// The value to compare against.
        value: serde_json::Value,
    },
    /// `field < value`.
    Lt {
        /// The declared field to compare.
        field: String,
        /// The value to compare against.
        value: serde_json::Value,
    },
    /// `field <= value`.
    Lte {
        /// The declared field to compare.
        field: String,
        /// The value to compare against.
        value: serde_json::Value,
    },
    /// `field` equals any of `values` (non-empty).
    In {
        /// The declared field to compare.
        field: String,
        /// The accepted values.
        values: Vec<serde_json::Value>,
    },
    /// Every sub-expression matches.
    And {
        /// The conjuncts.
        exprs: Vec<FilterExpr>,
    },
    /// Any sub-expression matches.
    Or {
        /// The disjuncts.
        exprs: Vec<FilterExpr>,
    },
    /// Negation.
    Not {
        /// The negated expression.
        expr: Box<FilterExpr>,
    },
    /// `value` is a member of `doc.field[]`.
    Contains {
        /// The array field to test.
        field: String,
        /// The candidate member.
        value: serde_json::Value,
    },
    /// The field is present and non-null.
    Exists {
        /// The field to test.
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
    /// Whether the query executed.
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// The raw untagged query result (present when ok).
    pub result: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// The `{code, message}` envelope (present when not ok).
    pub error: Option<ErrorEnvelope>,
}

/// HTTP request/response bodies for `/admin/*`. Extracted to `wire/admin.rs` (QA-008)
/// so the WS protocol types stay at parity with `server/src/protocol.rs` (~640 LOC)
/// instead of carrying the admin control-plane shapes that are HTTP-only. The
/// `#[cfg(feature = "admin")]` gate stays on the module declaration so this file
/// compiles cleanly under `--all-features` and `--no-default-features` alike.
#[cfg(feature = "admin")]
pub mod admin;

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

    // Presence wire tags — port of `server/src/protocol.rs`'s Task-1 presence
    // tests. The four clients must serialize byte-identically (camelCase).
    #[test]
    fn presence_client_message_wire_tags() {
        // presence: optional state omitted when None
        assert_eq!(
            serde_json::to_value(ClientMessage::Presence {
                room: "doc:1".to_string(),
                state: None,
            })
            .unwrap(),
            json!({"type": "presence", "room": "doc:1"})
        );
        // presence: state present when Some
        assert_eq!(
            serde_json::to_value(ClientMessage::Presence {
                room: "doc:1".to_string(),
                state: Some(json!({"x": 3, "y": 4})),
            })
            .unwrap(),
            json!({"type": "presence", "room": "doc:1", "state": {"x": 3, "y": 4}})
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::PresenceState {
                room: "doc:1".to_string(),
                state: json!({"typing": true}),
                ttl_ms: None,
            })
            .unwrap(),
            json!({"type": "presenceState", "room": "doc:1", "state": {"typing": true}})
        );
        // ttl_ms: Some emits "ttlMs" on the wire (ENH-015 presence-ttl).
        assert_eq!(
            serde_json::to_value(ClientMessage::PresenceState {
                room: "doc:1".to_string(),
                state: json!({"typing": true}),
                ttl_ms: Some(3000),
            })
            .unwrap(),
            json!({"type": "presenceState", "room": "doc:1", "state": {"typing": true}, "ttlMs": 3000})
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::LeavePresence {
                room: "doc:1".to_string(),
            })
            .unwrap(),
            json!({"type": "leavePresence", "room": "doc:1"})
        );
    }

    // ENH-015 presence-ttl: PresenceState carries an optional `ttlMs` that the
    // server uses to clear this connection's `state` to null `ttlMs` after the
    // last refresh (member stays). Some emits the field; None omits it; the
    // deserialize path accepts both forms and round-trips.
    #[test]
    fn presence_state_ttl_ms_round_trip() {
        // ttlMs present → deserializes into Some(_).
        let with_ttl = json!({
            "type": "presenceState",
            "room": "doc:1",
            "state": {"typing": true},
            "ttlMs": 3000
        });
        let back: ClientMessage = serde_json::from_value(with_ttl.clone()).unwrap();
        match back {
            ClientMessage::PresenceState {
                room,
                state,
                ttl_ms,
            } => {
                assert_eq!(room, "doc:1");
                assert_eq!(state, json!({"typing": true}));
                assert_eq!(ttl_ms, Some(3000));
            }
            other => panic!("expected PresenceState, got {other:?}"),
        }
        // Re-serialize preserves ttlMs.
        assert_eq!(
            serde_json::to_value(ClientMessage::PresenceState {
                room: "doc:1".to_string(),
                state: json!({"typing": true}),
                ttl_ms: Some(3000),
            })
            .unwrap(),
            with_ttl
        );

        // Absent ttlMs → deserializes into None; re-serialization omits the key.
        let without_ttl = json!({
            "type": "presenceState",
            "room": "doc:1",
            "state": {"typing": true}
        });
        let back: ClientMessage = serde_json::from_value(without_ttl.clone()).unwrap();
        match back {
            ClientMessage::PresenceState { ttl_ms, .. } => assert_eq!(ttl_ms, None),
            other => panic!("expected PresenceState, got {other:?}"),
        }
        assert_eq!(
            serde_json::to_value(ClientMessage::PresenceState {
                room: "doc:1".to_string(),
                state: json!({"typing": true}),
                ttl_ms: None,
            })
            .unwrap(),
            without_ttl
        );

        // ttlMs: 0 is a real value on the wire TYPE, not omitted
        // (skip_serializing_if checks Option::is_none, not falsiness). The live
        // server REJECTS ttl_ms <= 0 with BAD_REQUEST at the logic layer — the
        // wire type faithfully carries it (serialization != validation), and the
        // SDK forwards ttl as-is so the server stays the authoritative validator.
        assert_eq!(
            serde_json::to_value(ClientMessage::PresenceState {
                room: "doc:1".to_string(),
                state: json!({"typing": true}),
                ttl_ms: Some(0),
            })
            .unwrap(),
            json!({"type": "presenceState", "room": "doc:1", "state": {"typing": true}, "ttlMs": 0})
        );
    }

    #[test]
    fn presence_server_message_wire_tags() {
        let member = PresenceMember {
            connection_id: "42".to_string(),
            user: AuthedUser {
                kind: UserKind::User,
                email: Some("a@b.com".to_string()),
                name: None,
                github_login: None,
                github_id: None,
            },
            state: json!({"x": 1}),
        };
        assert_eq!(
            serde_json::to_value(ServerMessage::PresenceSnapshot {
                room: "doc:1".to_string(),
                members: vec![member],
            })
            .unwrap(),
            json!({
                "type": "presenceSnapshot",
                "room": "doc:1",
                "members": [{
                    "connectionId": "42",
                    // AuthedUser has no `skip_serializing_if` on `name`, so
                    // `None` serializes as `null` (pre-existing wire shape
                    // mirrored across all four clients).
                    "user": {"kind": "user", "email": "a@b.com", "name": null},
                    "state": {"x": 1}
                }]
            })
        );
        // `presenceErr` round-trips with the error envelope; just assert the
        // tag and payload shape (the RtDbError wire shape is covered elsewhere).
        let err = serde_json::to_value(ServerMessage::PresenceErr {
            room: "doc:1".to_string(),
            error: crate::error::RtDbError::new(
                crate::error::ErrorCode::Forbidden,
                "presence not enabled",
            ),
        })
        .unwrap();
        assert_eq!(err["type"], json!("presenceErr"));
        assert_eq!(err["room"], json!("doc:1"));
        assert_eq!(
            err["error"],
            json!({"code": "FORBIDDEN", "message": "presence not enabled"})
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
        // `filter` omitted (None) → additive: same wire shape as before the field.
        let q = SearchQuery {
            index: "search_content".into(),
            query: "hello world".into(),
            filter: None,
            mode: None,
            snippet: None,
        };
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"index":"search_content","query":"hello world"})
        );
        let back: SearchQuery =
            serde_json::from_value(json!({"index":"search_content","query":"hello world"}))
                .unwrap();
        assert_eq!(back.index, "search_content");
        assert!(back.filter.is_none());
        assert!(back.mode.is_none());
        assert!(back.snippet.is_none());

        // `filter` present → emitted on the wire and round-trips through the
        // `FilterExpr` tag (`op`, lowercase). Mirrors the server's camelCase
        // `filter` nesting on the search terminal.
        let with_filter = SearchQuery {
            index: "search_content".into(),
            query: "hello world".into(),
            filter: Some(FilterExpr::And {
                exprs: vec![
                    FilterExpr::Eq {
                        field: "channel".into(),
                        value: "#general".into(),
                    },
                    FilterExpr::Gt {
                        field: "createdAt".into(),
                        value: 1_780_000_000_000_i64.into(),
                    },
                ],
            }),
            mode: None,
            snippet: None,
        };
        assert_eq!(
            serde_json::to_value(&with_filter).unwrap(),
            json!({
                "index":"search_content",
                "query":"hello world",
                "filter":{
                    "op":"and",
                    "exprs":[
                        {"op":"eq","field":"channel","value":"#general"},
                        {"op":"gt","field":"createdAt","value":1780000000000_i64}
                    ]
                }
            })
        );
    }

    #[test]
    fn search_query_mode_wire_shape() {
        // `mode` set → emitted on the wire as the lowercase variant and
        // round-trips; `Tsquery` is accepted even though clients never emit it
        // by default. Mirrors the FM-30 corpus entries ("conv" + trgm/tsquery).
        let trgm = SearchQuery {
            index: "search_body".into(),
            query: "conv".into(),
            filter: None,
            mode: Some(SearchMode::Trgm),
            snippet: None,
        };
        assert_eq!(
            serde_json::to_value(&trgm).unwrap(),
            json!({"index":"search_body","query":"conv","mode":"trgm"})
        );
        let back: SearchQuery =
            serde_json::from_value(json!({"index":"search_body","query":"conv","mode":"trgm"}))
                .unwrap();
        assert_eq!(back.mode, Some(SearchMode::Trgm));

        let tsquery = SearchQuery {
            mode: Some(SearchMode::Tsquery),
            filter: Some(FilterExpr::Eq {
                field: "status".into(),
                value: "open".into(),
            }),
            index: "search_body".into(),
            query: "conv".into(),
            snippet: None,
        };
        assert_eq!(
            serde_json::to_value(&tsquery).unwrap(),
            json!({
                "index":"search_body",
                "query":"conv",
                "filter":{"op":"eq","field":"status","value":"open"},
                "mode":"tsquery"
            })
        );

        // Omitted `mode` deserializes to `None` (tsquery default) and never
        // re-serializes — existing traffic stays byte-identical.
        let omitted: SearchQuery =
            serde_json::from_value(json!({"index":"search_body","query":"hello world"})).unwrap();
        assert_eq!(omitted.mode, None);

        // Unknown mode strings are rejected (serde enum, mirroring the
        // server's BadRequest on a bad mode).
        assert!(
            serde_json::from_value::<SearchQuery>(json!({
                "index":"search_body","query":"conv","mode":"bogus"
            }))
            .is_err()
        );
    }

    #[test]
    fn search_query_snippet_wire_shape() {
        // `snippet: Some(true)` → emitted on the wire and round-trips;
        // `Some(false)` is honored when a caller names it (the server treats
        // it exactly like omission). Mirrors the FM-31 corpus entry
        // ("hello world" + snippet:true); operator-syntax query text is plain
        // `query` string bytes — no new wire fields (covered by the other
        // FM-31 corpus entry through tests/wire_corpus.rs).
        let snippet = SearchQuery {
            index: "search_body".into(),
            query: "hello world".into(),
            filter: None,
            mode: None,
            snippet: Some(true),
        };
        assert_eq!(
            serde_json::to_value(&snippet).unwrap(),
            json!({"index":"search_body","query":"hello world","snippet":true})
        );
        let back: SearchQuery = serde_json::from_value(json!({
            "index":"search_body","query":"hello world","snippet":true
        }))
        .unwrap();
        assert_eq!(back.snippet, Some(true));

        // Omitted `snippet` deserializes to `None` and never re-serializes —
        // existing traffic stays byte-identical.
        let omitted: SearchQuery =
            serde_json::from_value(json!({"index":"search_body","query":"hello world"})).unwrap();
        assert_eq!(omitted.snippet, None);

        // Explicit `Some(false)` serializes as `false` (additive opt-in the
        // server reads as off — same behavior as omission).
        let off = SearchQuery {
            snippet: Some(false),
            ..omitted.clone()
        };
        assert_eq!(
            serde_json::to_value(&off).unwrap(),
            json!({"index":"search_body","query":"hello world","snippet":false})
        );
    }

    #[test]
    fn vector_search_query_wire_shape() {
        // `filter` omitted (None) → additive: same wire shape as before the field
        // changed to Option<FilterExpr>.
        let q = VectorSearchQuery {
            index: "by_embedding".into(),
            vector: vec![1.0, 0.0, 0.0],
            limit: 5,
            filter: None,
        };
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"index":"by_embedding","vector":[1.0,0.0,0.0],"limit":5})
        );
        // Round-trips; absent filter deserializes to None.
        let back: VectorSearchQuery = serde_json::from_value(json!({
            "index": "by_embedding",
            "vector": [1.0, 0.0, 0.0],
            "limit": 5
        }))
        .unwrap();
        assert_eq!(back.index, "by_embedding");
        assert_eq!(back.limit, 5);
        assert!(back.filter.is_none());
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
        // `filter` present → emitted on the wire and round-trips through the
        // `FilterExpr` tag (`op`, lowercase). Mirrors the search terminal's
        // camelCase `filter` nesting.
        let with_filter = VectorSearchQuery {
            index: "by_embedding".into(),
            vector: vec![1.0],
            limit: 3,
            filter: Some(FilterExpr::And {
                exprs: vec![
                    FilterExpr::Eq {
                        field: "userId".into(),
                        value: "u1".into(),
                    },
                    FilterExpr::Gte {
                        field: "createdAt".into(),
                        value: 1_780_000_000_000_i64.into(),
                    },
                ],
            }),
        };
        assert_eq!(
            serde_json::to_value(&with_filter).unwrap(),
            json!({
                "index":"by_embedding",
                "vector":[1.0],
                "limit":3,
                "filter":{
                    "op":"and",
                    "exprs":[
                        {"op":"eq","field":"userId","value":"u1"},
                        {"op":"gte","field":"createdAt","value":1780000000000_i64}
                    ]
                }
            })
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

    // ---- FM-29 workflow wire (fixtures mirror server protocol.rs tests) ----

    #[test]
    fn workflow_spec_wire_shape() {
        let spec = serde_json::from_value::<WorkflowSpec>(serde_json::json!({
            "name": "drip",
            "steps": [
                { "txn": { "steps": [ { "op": "insert", "table": "t", "doc": {} } ] } },
                { "txn": { "steps": [] },
                  "retry": { "maxAttempts": 5, "initialRetryMs": 500, "maxRetryMs": 2000 },
                  "sleepBeforeMs": 86400000 }
            ]
        }))
        .unwrap();
        assert_eq!(spec.steps.len(), 2);
        assert_eq!(spec.steps[1].sleep_before_ms, Some(86_400_000));
        let retry = spec.steps[1].retry.unwrap();
        assert_eq!(
            (
                retry.max_attempts,
                retry.initial_retry_ms,
                retry.max_retry_ms
            ),
            (5, 500, 2000)
        );
        // Omitted retry defaults on deserialize:
        assert!(
            serde_json::from_value::<StepRetry>(serde_json::json!({"maxAttempts": 2}))
                .unwrap()
                .initial_retry_ms
                == 1_000
        );
        // Round-trip: absent optionals are SKIPPED on serialize (corpus parity).
        let v = serde_json::to_value(&spec).unwrap();
        assert!(v["steps"][0].get("retry").is_none());
        assert!(v["steps"][0].get("sleepBeforeMs").is_none());
    }

    #[test]
    fn workflow_status_wire_is_snake_case() {
        assert_eq!(
            serde_json::to_value(WorkflowStatus::Pending).unwrap(),
            serde_json::json!("pending")
        );
        assert_eq!(
            "failed".parse::<WorkflowStatus>().unwrap(),
            WorkflowStatus::Failed
        );
        assert!("bogus".parse::<WorkflowStatus>().is_err());
    }

    #[test]
    fn workflow_info_wire_shape() {
        let info = serde_json::from_value::<WorkflowInfo>(serde_json::json!({
            "id": "wf1", "name": "drip", "status": "pending",
            "currentStep": 0, "stepCount": 3, "attempts": 0,
            "sleepUntil": 123, "createdAt": 1, "updatedAt": 2
        }))
        .unwrap();
        assert_eq!(info.step_count, 3);
        assert!(info.last_error.is_none());
        let v = serde_json::to_value(&info).unwrap();
        assert!(v.get("lastError").is_none() && v.get("finishedAt").is_none());
    }

    #[test]
    fn workflow_info_full_flattens_info_plus_outcomes() {
        let full = serde_json::from_value::<WorkflowInfoFull>(serde_json::json!({
            "id": "wf1", "name": "drip", "status": "success",
            "currentStep": 2, "stepCount": 2, "attempts": 1,
            "createdAt": 1, "updatedAt": 9, "startedAt": 2, "finishedAt": 9,
            "stepOutcomes": [
                { "stepIndex": 0, "status": "success", "attempts": 1, "at": 5 },
                { "stepIndex": 1, "status": "failed", "attempts": 3, "at": 8,
                  "error": "version mismatch" }
            ]
        }))
        .unwrap();
        assert_eq!(full.info.id, "wf1");
        assert_eq!(full.step_outcomes.len(), 2);
        assert_eq!(full.step_outcomes[0].status, OutcomeStatus::Success);
        assert_eq!(full.step_outcomes[1].status, OutcomeStatus::Failed);
        assert_eq!(
            full.step_outcomes[1].error.as_deref(),
            Some("version mismatch")
        );
        // Flatten round-trip: info fields sit at the top level beside
        // stepOutcomes, byte-identical to the server's GET shape.
        let v = serde_json::to_value(&full).unwrap();
        assert_eq!(v["id"], serde_json::json!("wf1"));
        assert_eq!(v["stepOutcomes"][1]["stepIndex"], serde_json::json!(1));
        assert!(v["stepOutcomes"][0].get("error").is_none());
    }

    #[test]
    fn workflow_client_message_variants() {
        let spec = sample_workflow_spec();
        let m = serde_json::to_value(ClientMessage::StartWorkflow {
            workflow_id: "wf-1".into(),
            spec: spec.clone(),
        })
        .unwrap();
        assert_eq!(m["type"], serde_json::json!("startWorkflow"));
        assert_eq!(m["workflowId"], serde_json::json!("wf-1"));
        assert_eq!(m["spec"]["name"], serde_json::json!("drip"));

        assert_eq!(
            serde_json::to_value(ClientMessage::CancelWorkflow {
                workflow_id: "wf-2".into(),
                id: "wf9".into(),
            })
            .unwrap(),
            serde_json::json!({"type": "cancelWorkflow", "workflowId": "wf-2", "id": "wf9"})
        );

        // status omitted when None, snake_case string when Some, and the
        // filtered frame parses back.
        assert_eq!(
            serde_json::to_value(ClientMessage::ListWorkflows {
                workflow_id: "wf-3".into(),
                status: None,
            })
            .unwrap(),
            serde_json::json!({"type": "listWorkflows", "workflowId": "wf-3"})
        );
        let m = serde_json::to_value(ClientMessage::ListWorkflows {
            workflow_id: "wf-3".into(),
            status: Some(WorkflowStatus::Failed),
        })
        .unwrap();
        assert_eq!(m["status"], serde_json::json!("failed"));
        match serde_json::from_value::<ClientMessage>(serde_json::json!({
            "type": "listWorkflows", "workflowId": "wf-3", "status": "failed"
        }))
        .unwrap()
        {
            ClientMessage::ListWorkflows {
                workflow_id,
                status,
            } => {
                assert_eq!(workflow_id, "wf-3");
                assert_eq!(status, Some(WorkflowStatus::Failed));
            }
            other => panic!("unexpected variant: {other:?}"),
        }
        // The start frame parses back too (spec round-trips through the frame).
        match serde_json::from_value::<ClientMessage>(m_start_frame(&spec)).unwrap() {
            ClientMessage::StartWorkflow { workflow_id, spec } => {
                assert_eq!(workflow_id, "wf-1");
                assert_eq!(spec.name, "drip");
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    fn m_start_frame(spec: &WorkflowSpec) -> serde_json::Value {
        serde_json::json!({
            "type": "startWorkflow", "workflowId": "wf-1",
            "spec": serde_json::to_value(spec).unwrap()
        })
    }

    fn sample_workflow_spec() -> WorkflowSpec {
        serde_json::from_value(serde_json::json!({
            "name": "drip",
            "steps": [ { "txn": { "steps": [] } } ]
        }))
        .expect("sample workflow spec")
    }

    fn sample_workflow_info() -> WorkflowInfo {
        WorkflowInfo {
            id: "wf1".to_string(),
            name: "drip".to_string(),
            status: WorkflowStatus::Pending,
            current_step: 0,
            step_count: 2,
            attempts: 0,
            sleep_until: Some(123),
            last_error: None,
            created_at: 1,
            updated_at: 2,
            started_at: None,
            finished_at: None,
        }
    }

    #[test]
    fn workflow_server_message_variants() {
        let info = sample_workflow_info();
        let m = serde_json::to_value(ServerMessage::StartWorkflowOk {
            workflow_id: "wf-1".into(),
            info: info.clone(),
        })
        .unwrap();
        assert_eq!(m["type"], serde_json::json!("startWorkflowOk"));
        assert_eq!(m["workflowId"], serde_json::json!("wf-1"));
        assert_eq!(m["info"]["id"], serde_json::json!("wf1"));

        let m = serde_json::to_value(ServerMessage::StartWorkflowErr {
            workflow_id: "wf-1".into(),
            error: RtDbError::new(crate::error::ErrorCode::BadRequest, "bad spec"),
        })
        .unwrap();
        assert_eq!(m["type"], serde_json::json!("startWorkflowErr"));
        assert_eq!(m["error"]["code"], serde_json::json!("BAD_REQUEST"));

        let m = serde_json::to_value(ServerMessage::WorkflowAck {
            workflow_id: "wf-1".into(),
            ok: true,
            error: None,
        })
        .unwrap();
        assert_eq!(m["type"], serde_json::json!("workflowAck"));
        // `error` is skipped on the wire when the ack is clean.
        assert!(m.get("error").is_none());
        let m = serde_json::to_value(ServerMessage::WorkflowAck {
            workflow_id: "wf-1".into(),
            ok: false,
            error: Some(RtDbError::new(
                crate::error::ErrorCode::NotFound,
                "no such run",
            )),
        })
        .unwrap();
        assert_eq!(m["ok"], serde_json::json!(false));
        assert_eq!(m["error"]["code"], serde_json::json!("NOT_FOUND"));

        let m = serde_json::to_value(ServerMessage::ListWorkflowsOk {
            workflow_id: "wf-4".into(),
            workflows: vec![info],
        })
        .unwrap();
        assert_eq!(m["type"], serde_json::json!("listWorkflowsOk"));
        assert_eq!(m["workflows"][0]["id"], serde_json::json!("wf1"));
    }

    // ---- admin migrate wire (tag `op`, camelCase, `where` alias) -----------
    #[cfg(feature = "admin")]
    #[test]
    fn migrate_directive_round_trip() {
        use crate::schema::FieldType;
        use crate::wire::admin::{
            Cast, CondSource, Directive, ExprSource, MigrateRequest, MigrateResult, ValueExpr,
        };

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
                    expr: ExprSource::Legacy("upper(doc->>'fullName')".into()),
                    where_clause: Some(CondSource::Legacy("doc ? 'fullName'".into())),
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
                expr: ExprSource::Legacy("upper(doc->>'fullName')".into()),
                where_clause: Some(CondSource::Legacy("doc ? 'fullName'".into())),
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

        // ENH-020: the typed `ValueExpr` path round-trips through the same
        // `ExprSource::Typed` arm and serializes to the closed grammar's wire
        // shape (tag `op`, camelCase variant names). `Case` exercises the
        // nested `CaseWhen { when, then }` + `FilterExpr` binding.
        let typed = Directive::EvalExpr {
            table: "users".into(),
            set: "greeting".into(),
            expr: ExprSource::Typed(ValueExpr::Concat {
                parts: vec![
                    ValueExpr::Literal {
                        value: json!("Hello, "),
                    },
                    ValueExpr::Upper {
                        value: Box::new(ValueExpr::Field {
                            field: "name".into(),
                        }),
                    },
                ],
            }),
            where_clause: Some(CondSource::Typed(crate::wire::FilterExpr::Exists {
                field: "name".into(),
            })),
        };
        let typed_json = serde_json::to_value(&typed).unwrap();
        assert_eq!(typed_json["op"], "evalExpr");
        assert_eq!(typed_json["expr"]["op"], "concat");
        assert_eq!(typed_json["expr"]["parts"][0]["op"], "literal");
        assert_eq!(typed_json["expr"]["parts"][0]["value"], "Hello, ");
        assert_eq!(typed_json["expr"]["parts"][1]["op"], "upper");
        assert_eq!(typed_json["expr"]["parts"][1]["value"]["op"], "field");
        assert_eq!(typed_json["expr"]["parts"][1]["value"]["field"], "name");
        assert_eq!(typed_json["where"]["op"], "exists");
        assert_eq!(typed_json["where"]["field"], "name");
        // Round-trips back, and the typed payload survives (not rewritten to legacy).
        let typed_back: Directive = serde_json::from_value(typed_json.clone()).unwrap();
        match typed_back {
            Directive::EvalExpr {
                expr: ExprSource::Typed(ValueExpr::Concat { parts }),
                where_clause: Some(CondSource::Typed(crate::wire::FilterExpr::Exists { .. })),
                ..
            } => {
                assert_eq!(parts.len(), 2);
            }
            other => panic!("typed EvalExpr did not round-trip: {other:?}"),
        }
        // A hostile object that is not a valid `ValueExpr` is rejected — it
        // does NOT silently fall through to `Legacy(String)`.
        let hostile = json!({
            "op": "evalExpr",
            "table": "users",
            "set": "greeting",
            "expr": {"op": "bogusOp", "field": "name"}
        });
        assert!(serde_json::from_value::<Directive>(hostile).is_err());

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
