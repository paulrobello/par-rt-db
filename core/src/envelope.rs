//! The par-rt-db WS envelope vocabulary shared by the server and the Rust client.
//!
//! `ClientMessage`/`ServerMessage` — plus the auth, presence, and
//! workflow-projection types they carry — are ONE vocabulary with two
//! consumers, not "the server's types that the client mirrors". Before ARC-008
//! each crate carried its own copy, kept byte-identical by review, the golden
//! vector, and the wire corpus; a drift showed up as a corpus failure rather
//! than a compile error. Defining them once here makes the two crates
//! structurally incapable of disagreeing (the same reasoning as
//! [`crate::wire`], the ARC-004 txn vocabulary).
//!
//! [`ServerMessage`] is generic over exactly the two payloads that are
//! genuinely per-crate — the error envelope `E` and the change-feed op `C`.
//! The server instantiates it with its `RtDbError` and
//! `change_log::ChangeRow`, the Rust client with its own `RtDbError` and
//! `wire::ChangeOp`; each re-exports the instantiated alias at its historical
//! path. The five non-Rust clients keep their hand-written mirrors, enforced
//! byte-identical by the wire corpus and `golden-vector.json`.
//!
//! Both crates re-export everything at their historical paths
//! (`rtdb_server::protocol::ClientMessage`,
//! `par_rt_db_client::wire::ServerMessage`, …), so no call site moved. Serde
//! tags and field casing here are load-bearing and deliberately non-uniform:
//! any change is a wire change and must be mirrored in the other five clients.

use serde::{Deserialize, Serialize};

use crate::mutation::{ScheduleInfo, ScheduleWhen, Transaction, WorkflowSpec, WorkflowStatus};
use crate::query::Query;

/// The wire protocol version this vocabulary speaks (ARC-013). A client that
/// omits `protocolVersion` on its `Auth` frame (or the `X-Rtdb-Protocol`
/// header for the one-shot HTTP API) is treated as version 1 for backward
/// compatibility. A requested version GREATER than this is rejected with
/// `ErrorCode::UnsupportedProtocol` rather than a generic 400/
/// `deny_unknown_fields` failure.
///
/// Version 2 (2026-09-19 wire-v2 bundle) adds: the aggregate terminal's
/// `aggregates` alias→op map, the `groupBy` field-list form, the optional
/// `tz` on cron `ScheduleWhen`, and `POST /api/mutate-batch`.
///
/// Version 3 (2026-09-22 presence deltas) adds: the `presenceDelta` server
/// frame, emitted by the presence flush loop ONLY to connections whose
/// negotiated `protocolVersion` is at least [`PRESENCE_DELTA_MIN_VERSION`];
/// every other connection keeps receiving full `presenceSnapshot` frames, so
/// the frame's rollout is not lockstep with SDK upgrades.
///
/// There is no per-message enforcement — a v3 build parses the full
/// vocabulary from any authenticated peer; the number exists so an OLD
/// server fails a newer client cleanly at auth instead of on the first
/// unknown field.
pub const PROTOCOL_VERSION: u32 = 3;

/// The protocol version at which the server may emit `PresenceDelta`
/// (ARC-013 gate for the v3 frame). A connection that negotiated a version
/// below this — or omitted `protocolVersion` entirely (v1 semantics) — keeps
/// receiving full `PresenceSnapshot` frames instead. Pinned as its own
/// constant (not read off [`PROTOCOL_VERSION`]) because the gate must stay at
/// 3 even after `PROTOCOL_VERSION` moves to 4+.
pub const PRESENCE_DELTA_MIN_VERSION: u32 = 3;

/// Full WS client vocabulary. Consumed by the server's WS handler and sent by
/// every client SDK — wire tags and field names are load-bearing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ClientMessage {
    /// Authenticate the socket (first frame).
    Auth {
        // SEC-001 phase 2: `token` is optional so a browser dashboard can
        // authenticate over `/sync` from the HttpOnly `rtdb_session` cookie the
        // browser sends on the WS upgrade — the Auth message then carries only
        // `db`. CLI/SDK/machine tokens still send `token` (the prior wire form),
        // so this is backward-compatible.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// Bearer token; `None` when authenticating from the session cookie.
        token: Option<String>,
        /// The database to authorize against.
        db: String,
        // ARC-013: optional protocol version the client speaks. Absent ⇒
        // version 1. A value greater than `PROTOCOL_VERSION` is rejected with
        // `UNSUPPORTED_PROTOCOL`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// The protocol version this client speaks; absent means version 1.
        protocol_version: Option<u32>,
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
        // External-claim job mode (2026-09-09): when set, the job is never
        // executed by the internal scheduler — an application worker claims
        // it via `POST /api/schedule/claim` and finalizes with the returned
        // `leaseGeneration` fencing token. Omitted = ordinary internal job.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// External-claim job mode: the job is claimed via
        /// `POST /api/schedule/claim` instead of executed internally.
        external: Option<bool>,
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
    /// Deliver a named signal to a waiting run (`awaitSignal` steps). The
    /// reply reuses [`ServerMessage::WorkflowAck`].
    SignalWorkflow {
        /// Correlation id for the reply.
        workflow_id: String,
        /// The waiting run to signal.
        id: String,
        /// The signal name (must match the step's `awaitSignal.name`).
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// Latest-wins payload; the step's outcome records it as `signal`.
        payload: Option<serde_json::Value>,
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
    /// Opens a push channel over the durable change feed. `since` is the last
    /// seq the consumer processed (the server streams strictly after it);
    /// omitted = start at the current head. `feedEpoch` is the `feedEpoch` a
    /// previous session saw: a mismatch (database dropped and recreated)
    /// answers `CURSOR_EXPIRED`. `tables` filters server-side (empty = all).
    FeedSubscribe {
        /// Client-chosen feed id echoed on every reply frame.
        feed_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// Resume cursor.
        since: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// Epoch of the log the cursor belongs to.
        feed_epoch: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        /// Server-side table filter.
        tables: Vec<String>,
    },
    /// Flow-control acknowledgement: every op with `seq <= seq` is processed.
    /// The server keeps at most a few unacked batches in flight.
    FeedAck {
        /// Which feed.
        feed_id: String,
        /// Highest processed seq.
        seq: i64,
    },
    /// Close a feed.
    FeedUnsubscribe {
        /// Which feed.
        feed_id: String,
    },
    /// Keepalive; the server replies [`ServerMessage::Pong`].
    Ping,
}

/// Full WS server vocabulary. The server sends these frames; every client SDK
/// parses them. `E` is the per-crate error-envelope type and `C` the
/// per-crate change-feed op type (see the module docs) — on the wire both
/// carry identical shapes for every consumer.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ServerMessage<E, C> {
    /// Authentication succeeded.
    AuthOk {
        /// The authed principal.
        user: AuthedUser,
        // ARC-013: the server's `PROTOCOL_VERSION`, echoed back only when the
        // client's `Auth` frame carried a `protocolVersion` (signaling it
        // understands the field) — so a pre-ARC-013 client's `AuthOk` bytes
        // are unchanged and a `deny_unknown_fields`/`extra="forbid"` decoder
        // on the older side never sees the new key.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// The server's [`PROTOCOL_VERSION`], echoed only when the client's
        /// `Auth` frame carried a `protocolVersion`.
        protocol_version: Option<u32>,
    },
    /// Authentication failed; the socket closes.
    AuthErr {
        /// Why.
        error: E,
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
        error: E,
    },
    /// Subscription rejected (bad query, authz).
    SubscribeErr {
        /// Which subscription.
        query_id: String,
        /// Why.
        error: E,
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
        error: E,
    },
    /// Reply to cancel/pause/resume. `error` is omitted on the wire when `ok`.
    ScheduleAck {
        /// Correlation id from the request.
        schedule_id: String,
        /// Whether the action applied.
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        /// Why it failed (absent when ok).
        error: Option<E>,
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
        error: E,
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
        error: Option<E>,
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
    /// Incremental alternative to [`ServerMessage::PresenceSnapshot`], sent
    /// instead of a full snapshot to a recipient with a known prior broadcast
    /// to diff against. `seq` is a per-ROOM monotonic counter, shared by every
    /// caught-up recipient of that room (NOT per-connection) — it increments
    /// once per flush tick that has a prior baseline to diff against, so a
    /// connection's first `PresenceDelta` can arrive at any `seq` (whatever
    /// the room's counter had reached when that connection caught up), not
    /// necessarily 1 or 2. `PresenceSnapshot` carries no `seq`, so the
    /// snapshot→first-delta transition is not itself verifiable; `seq` only
    /// proves CONTIGUITY delta-to-delta. The server's outbound path either
    /// delivers a message or tears the whole connection down — there is no
    /// path that silently drops one queued frame while leaving the connection
    /// open — so a gap in `seq` cannot reach a live, connected client; a
    /// fresh connection always starts from `PresenceSnapshot`, never
    /// mid-delta-stream. `seq` gap-detection is therefore a defensive
    /// invariant against a future regression in that guarantee, not
    /// protection against an observed failure mode today. `left` carries
    /// departed connection ids (a TTL expiry that nulls `state` is
    /// `stateChanged`, not `left` — membership is untouched). Additive: a
    /// client that only knows `PresenceSnapshot`/`PresenceErr` silently
    /// ignores this frame's `type` and keeps working off snapshots (the
    /// server always sends at least one full snapshot per join, so an
    /// unaware client never observes a gap). Empty buckets are omitted on
    /// the wire; the server emits this frame only to connections that
    /// negotiated [`PRESENCE_DELTA_MIN_VERSION`]+ (ARC-013 gate).
    PresenceDelta {
        /// Which room.
        room: String,
        /// Per-room broadcast counter (see above).
        seq: u64,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        /// Members that joined since the previous broadcast.
        joined: Vec<PresenceMember>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        /// Connection ids that left since the previous broadcast.
        left: Vec<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        /// Members whose `state` changed since the previous broadcast.
        state_changed: Vec<PresenceMember>,
    },
    /// Presence operation failed.
    PresenceErr {
        /// Which room.
        room: String,
        /// Why.
        error: E,
    },
    /// One batch of durable change ops for a feed (oldest first,
    /// at-least-once). `feedEpoch` is the change log's identity (`logId`):
    /// dedupe on `(feedEpoch, op.seq)`. The first frame after `feedSubscribe`
    /// always arrives (possibly with no ops) to confirm the subscription and
    /// carry the epoch and head.
    FeedEvents {
        /// Which feed.
        feed_id: String,
        /// The change log's identity (`logId`).
        feed_epoch: String,
        /// The ops, oldest first.
        ops: Vec<C>,
        /// The log's end when the batch was read.
        head: i64,
    },
    /// Terminal feed failure; `CURSOR_EXPIRED` means resync over HTTP.
    FeedErr {
        /// Which feed.
        feed_id: String,
        /// Why.
        error: E,
    },
    /// Reply to `Ping`.
    Pong,
}

/// Whether an [`AuthedUser`] resolved from an interactive OAuth session or a
/// per-database machine token. Closed domain — the field used to be a free
/// `String`, which silently accepted typos on the wire (ARC-004/QA-008).
/// Serializes as `"user"` / `"machine"` (snake_case), matching the prior
/// stringly-typed bytes byte-for-byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UserKind {
    /// An interactive OAuth/anonymous session principal.
    User,
    /// A per-database machine token principal.
    Machine,
}

impl UserKind {
    /// Snake-case wire string for this variant (e.g. `"user"`). Useful for
    /// call sites that bind the value into a SQL text column or log field.
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

/// The authenticated principal returned by `authOk` / `GET /auth/me`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthedUser {
    /// Session user or machine token.
    pub kind: UserKind,
    /// Email when known (always `None` for anonymous/machine).
    pub email: Option<String>,
    /// Display name when known.
    pub name: Option<String>,
    // GitHub login. Omitted entirely on the wire when absent (machine token
    // or a non-GitHub user) so existing clients keep parsing unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// GitHub login; absent on the wire for machine tokens / non-GitHub users.
    pub github_login: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// GitHub numeric id, paired with `github_login`.
    pub github_id: Option<i64>,
    // Stable opaque identity of a validated user principal — the
    // `rtdb_auth.users.id` that `resolve_user` keys on the per-provider
    // subject, so it survives provider-side email/name changes. Machine
    // tokens (and any absent id) omit the key entirely. Never the raw
    // provider subject (`google_sub` stays server-side).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Stable opaque identity of a validated user principal; omitted on the
    /// wire for machine tokens. Never the raw provider subject.
    pub user_id: Option<String>,
}

/// One entry in a presence room's member list. `connectionId` is the opaque,
/// unique-per-session key; `user` carries display identity; `state` is an
/// opaque client-supplied blob.
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

/// List/get projection of one run (FM-29). Optional fields are omitted on the
/// wire when absent.
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// The signal name a `waiting` run is parked on.
    pub waiting_for: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// When the run parked (epoch ms), while `waiting`.
    pub waited_since: Option<i64>,
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
