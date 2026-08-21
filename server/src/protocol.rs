//! Wire protocol — the source of truth for the client/server vocabulary.
//!
//! Defines the WS client/server message envelopes (`ClientMessage`/
//! `ServerMessage`) plus the `Query`, `Transaction`, and schedule shapes consumed
//! by the WS handler (`ws`) and the one-shot HTTP handler (`http_api`). Mirrored
//! verbatim by the three client SDKs (`ts-client`, `rust-client`,
//! `python-client`): the serde tags and field casing are load-bearing and
//! deliberately non-uniform, so any change here must be reflected in all four
//! implementations.

use crate::dsl::{Query, Transaction};
use crate::error::RtDbError;

/// Full WS client vocabulary. Consumed by the WS handler (Task 9) and mirrored
/// by the TS client — wire tags and field names are load-bearing.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ClientMessage {
    Auth {
        // SEC-001 phase 2: `token` is optional so a browser dashboard can
        // authenticate over `/sync` from the HttpOnly `rtdb_session` cookie the
        // browser sends on the WS upgrade — the Auth message then carries only
        // `db`. CLI/SDK/machine tokens still send `token` (the prior wire form),
        // so this is backward-compatible.
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
    StartWorkflow {
        workflow_id: String,
        spec: WorkflowSpec,
    },
    CancelWorkflow {
        workflow_id: String,
        id: String,
    },
    ListWorkflows {
        workflow_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<WorkflowStatus>,
    },
    Presence {
        room: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        state: Option<serde_json::Value>,
    },
    PresenceState {
        room: String,
        state: serde_json::Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ttl_ms: Option<u64>,
    },
    LeavePresence {
        room: String,
    },
    Ping,
}

/// Full WS server vocabulary.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
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
    StartWorkflowOk {
        workflow_id: String,
        info: WorkflowInfo,
    },
    StartWorkflowErr {
        workflow_id: String,
        error: RtDbError,
    },
    /// Reply to cancelWorkflow. `error` is omitted on the wire when `ok`.
    WorkflowAck {
        workflow_id: String,
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<RtDbError>,
    },
    ListWorkflowsOk {
        workflow_id: String,
        workflows: Vec<WorkflowInfo>,
    },
    PresenceSnapshot {
        room: String,
        members: Vec<PresenceMember>,
    },
    PresenceErr {
        room: String,
        error: RtDbError,
    },
    Pong,
}

/// Whether an `AuthedUser` resolved from an interactive OAuth session or a
/// per-database machine token. Closed domain — the field used to be a free
/// `String`, which silently accepted typos on the wire (ARC-004/QA-008).
/// Serializes as `"user"` / `"machine"` (snake_case), matching the prior
/// stringly-typed bytes byte-for-byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UserKind {
    User,
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthedUser {
    pub kind: UserKind,
    pub email: Option<String>,
    pub name: Option<String>,
    /// GitHub login. Omitted entirely on the wire when absent (machine token
    /// or a non-GitHub user) so existing clients keep parsing unchanged.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub github_login: Option<String>,
    /// GitHub numeric id, paired with `github_login`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub github_id: Option<i64>,
}

/// One entry in a presence room's member list. `connectionId` is the opaque,
/// unique-per-session key (the `ConnId` stringified); `user` carries display
/// identity; `state` is an opaque client-supplied blob.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PresenceMember {
    pub connection_id: String,
    pub user: AuthedUser,
    pub state: serde_json::Value,
}

/// How a caller wants a transaction scheduled. Mirrored byte-for-byte in
/// `ts-client/src/protocol.ts` and `rust-client/src/wire.rs`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum ScheduleWhen {
    /// Fire `ms` milliseconds from now.
    AfterMs { ms: i64 },
    /// Fire at this UTC epoch-ms instant (in the past = fire immediately).
    RunAt { ms: i64 },
    /// Fire on this 5-field cron schedule (UTC, min-first).
    Cron { expr: String },
    /// Fire every `every_ms` milliseconds, starting one interval from now.
    /// Missed windows (downtime, pause) are skipped, never backfilled —
    /// each fire re-arms from its actual fire time, like cron recompute.
    Interval {
        #[serde(rename = "everyMs")]
        every_ms: i64,
    },
}

/// Whether a scheduled job fires once (`ScheduleWhen::AfterMs`/`RunAt`) or
/// repeats (`Cron` on an expression, `Interval` every N ms). Closed domain —
/// was a free `String` (ARC-004/QA-008). Serializes as `"oneshot"` / `"cron"`
/// / `"interval"`, byte-identical to the prior stringly-typed bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleKind {
    Oneshot,
    Cron,
    Interval,
}

impl ScheduleKind {
    pub fn as_wire_str(&self) -> &'static str {
        match self {
            ScheduleKind::Oneshot => "oneshot",
            ScheduleKind::Cron => "cron",
            ScheduleKind::Interval => "interval",
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
            "interval" => Ok(ScheduleKind::Interval),
            other => Err(format!("unknown ScheduleKind: {other}")),
        }
    }
}

/// Lifecycle state of a scheduled job. Closed domain — was a free `String`
/// (ARC-004/QA-008). Serializes as `"pending"` / `"running"` / `"paused"` /
/// `"error"`, byte-identical to the prior stringly-typed bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
/// `last_error` are omitted on the wire when absent. The canonical home is
/// `protocol::ScheduleInfo`; `scheduler` re-exports it for its `list` return
/// type and existing call sites.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleInfo {
    pub id: String,
    pub kind: ScheduleKind,
    pub due_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cron: Option<String>,
    /// Interval jobs only: the fixed recurrence in ms (`kind: "interval"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub every_ms: Option<i64>,
    pub status: ScheduleStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub created_at: i64,
    pub fired_count: i64,
}

/// Per-step retry policy (FM-29). `maxAttempts` counts TOTAL attempts — the
/// first try included. Defaults when a step omits `retry`: 3 attempts, 1s
/// initial backoff doubling to a 60s cap.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StepRetry {
    pub max_attempts: u32,
    #[serde(default = "default_initial_retry_ms")]
    pub initial_retry_ms: u64,
    #[serde(default = "default_max_retry_ms")]
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

/// One workflow step: an ordinary `Transaction` plus policy. The txn may
/// itself carry `Schedule`/`CancelSchedule` steps (FM-28 rules apply).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkflowStepSpec {
    pub txn: Transaction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<StepRetry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_before_ms: Option<u64>,
}

/// A submitted workflow definition. Stored verbatim per run — a run
/// snapshots its spec, so template edits never drift a live run.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkflowSpec {
    pub name: String,
    pub steps: Vec<WorkflowStepSpec>,
}

/// Run lifecycle. Closed domain (ARC-004/QA-008 pattern — was never a free
/// string). Snake-case wire: pending|running|success|failed|cancelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus {
    Pending,
    Running,
    Success,
    Failed,
    Cancelled,
}

impl WorkflowStatus {
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
/// the `attempts` count on the entry (and on the row) carries them.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StepOutcome {
    pub step_index: u32,
    pub status: OutcomeStatus,
    pub attempts: u32,
    pub at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutcomeStatus {
    Success,
    Failed,
}

/// List/get projection of one run (FM-29).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkflowInfo {
    pub id: String,
    pub name: String,
    pub status: WorkflowStatus,
    pub current_step: u32,
    pub step_count: u32,
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_until: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<i64>,
}

/// `GET .../{id}` shape: the info row plus the per-step outcome trail.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkflowInfoFull {
    #[serde(flatten)]
    pub info: WorkflowInfo,
    pub step_outcomes: Vec<StepOutcome>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_query() -> crate::query::Query {
        serde_json::from_value(serde_json::json!({"table": "workItems"})).expect("query")
    }

    fn sample_txn() -> Transaction {
        Transaction { steps: vec![] }
    }

    // Client tags/fields exactly match the wire vocabulary consumed by
    // Task 9's WS handler and the TS client.
    #[test]
    fn client_message_wire_tags_and_fields() {
        assert_eq!(
            serde_json::to_value(ClientMessage::Auth {
                token: Some("t".to_string()),
                db: "d".to_string()
            })
            .unwrap(),
            serde_json::json!({"type": "auth", "token": "t", "db": "d"})
        );
        // SEC-001 phase 2: a tokenless Auth (cookie-mode) serializes without the
        // `token` field and round-trips back to `None`.
        assert_eq!(
            serde_json::to_value(ClientMessage::Auth {
                token: None,
                db: "d".to_string()
            })
            .unwrap(),
            serde_json::json!({"type": "auth", "db": "d"})
        );
        let parsed: ClientMessage =
            serde_json::from_value(serde_json::json!({"type": "auth", "db": "d"})).unwrap();
        match parsed {
            ClientMessage::Auth { token, db } => {
                assert!(token.is_none());
                assert_eq!(db, "d");
            }
            other => panic!("unexpected variant: {other:?}"),
        }
        assert_eq!(
            serde_json::to_value(ClientMessage::Subscribe {
                query_id: "q1".to_string(),
                query: Box::new(sample_query())
            })
            .unwrap()["type"],
            serde_json::json!("subscribe")
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::Unsubscribe {
                query_id: "q1".to_string()
            })
            .unwrap(),
            serde_json::json!({"type": "unsubscribe", "queryId": "q1"})
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::Mutate {
                mut_id: "m1".to_string(),
                idempotency_key: None,
                txn: sample_txn()
            })
            .unwrap(),
            serde_json::json!({"type": "mutate", "mutId": "m1", "txn": {"steps": []}})
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::Mutate {
                mut_id: "m1".to_string(),
                idempotency_key: Some("key1".to_string()),
                txn: sample_txn()
            })
            .unwrap(),
            serde_json::json!({
                "type": "mutate",
                "mutId": "m1",
                "idempotencyKey": "key1",
                "txn": {"steps": []}
            })
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::Ping).unwrap(),
            serde_json::json!({"type": "ping"})
        );
    }

    #[test]
    fn client_message_rejects_unknown_fields() {
        let raw = serde_json::json!({"type": "auth", "token": "t", "db": "d", "bogus": true});
        assert!(serde_json::from_value::<ClientMessage>(raw).is_err());
    }

    #[test]
    fn server_message_wire_tags_and_fields() {
        assert_eq!(
            serde_json::to_value(ServerMessage::AuthOk {
                user: AuthedUser {
                    kind: UserKind::User,
                    email: Some("a@b.com".to_string()),
                    name: None,
                    github_login: None,
                    github_id: None
                }
            })
            .unwrap()["type"],
            serde_json::json!("authOk")
        );
        assert_eq!(
            serde_json::to_value(ServerMessage::QueryUpdate {
                query_id: "q1".to_string(),
                result: serde_json::json!([])
            })
            .unwrap(),
            serde_json::json!({"type": "queryUpdate", "queryId": "q1", "result": []})
        );
        assert_eq!(
            serde_json::to_value(ServerMessage::MutateOk {
                mut_id: "m1".to_string(),
                results: vec![]
            })
            .unwrap(),
            serde_json::json!({"type": "mutateOk", "mutId": "m1", "results": []})
        );
        assert_eq!(
            serde_json::to_value(ServerMessage::MutateErr {
                mut_id: "m1".to_string(),
                error: RtDbError::not_found("x")
            })
            .unwrap()["type"],
            serde_json::json!("mutateErr")
        );
        assert_eq!(
            serde_json::to_value(ServerMessage::SubscribeErr {
                query_id: "q1".to_string(),
                error: RtDbError::bad_request("bad index")
            })
            .unwrap(),
            serde_json::json!({
                "type": "subscribeErr",
                "queryId": "q1",
                "error": {"code": "BAD_REQUEST", "message": "bad index"}
            })
        );
        assert_eq!(
            serde_json::to_value(ServerMessage::Pong).unwrap(),
            serde_json::json!({"type": "pong"})
        );
    }

    #[test]
    fn schedule_when_wire_tags() {
        assert_eq!(
            serde_json::to_value(ScheduleWhen::AfterMs { ms: 5 }).unwrap(),
            serde_json::json!({"type": "afterMs", "ms": 5})
        );
        assert_eq!(
            serde_json::to_value(ScheduleWhen::RunAt { ms: 9 }).unwrap(),
            serde_json::json!({"type": "runAt", "ms": 9})
        );
        assert_eq!(
            serde_json::to_value(ScheduleWhen::Cron {
                expr: "*/5 * * * *".to_string()
            })
            .unwrap(),
            serde_json::json!({"type": "cron", "expr": "*/5 * * * *"})
        );
        assert_eq!(
            serde_json::to_value(ScheduleWhen::Interval { every_ms: 5_000 }).unwrap(),
            serde_json::json!({"type": "interval", "everyMs": 5000})
        );
    }

    #[test]
    fn schedule_message_wire_tags() {
        let s = serde_json::to_value(ClientMessage::Schedule {
            schedule_id: "s1".to_string(),
            when: ScheduleWhen::AfterMs { ms: 100 },
            txn: sample_txn(),
        })
        .unwrap();
        assert_eq!(s["type"], serde_json::json!("schedule"));
        assert_eq!(s["scheduleId"], serde_json::json!("s1"));
        assert_eq!(s["when"], serde_json::json!({"type": "afterMs", "ms": 100}));

        let ok = serde_json::to_value(ServerMessage::ScheduleOk {
            schedule_id: "s1".to_string(),
            id: "job-9".to_string(),
        })
        .unwrap();
        assert_eq!(
            ok,
            serde_json::json!({"type": "scheduleOk", "scheduleId": "s1", "id": "job-9"})
        );

        let ack = serde_json::to_value(ServerMessage::ScheduleAck {
            schedule_id: "s1".to_string(),
            ok: true,
            error: None,
        })
        .unwrap();
        assert_eq!(ack["type"], serde_json::json!("scheduleAck"));
        // `error` is skipped on the wire when `None`.
        assert!(ack.get("error").is_none());
    }

    #[test]
    fn client_message_round_trips_through_json() {
        let msg = ClientMessage::Subscribe {
            query_id: "q1".to_string(),
            query: Box::new(sample_query()),
        };
        let value = serde_json::to_value(&msg).unwrap();
        let restored: ClientMessage = serde_json::from_value(value).unwrap();
        assert!(matches!(restored, ClientMessage::Subscribe { query_id, .. } if query_id == "q1"));
    }

    #[test]
    fn presence_client_message_wire_tags() {
        // presence: optional state omitted when None
        assert_eq!(
            serde_json::to_value(ClientMessage::Presence {
                room: "doc:1".to_string(),
                state: None,
            })
            .unwrap(),
            serde_json::json!({"type": "presence", "room": "doc:1"})
        );
        // presence: state present when Some
        assert_eq!(
            serde_json::to_value(ClientMessage::Presence {
                room: "doc:1".to_string(),
                state: Some(serde_json::json!({"x": 3, "y": 4})),
            })
            .unwrap(),
            serde_json::json!({"type": "presence", "room": "doc:1", "state": {"x": 3, "y": 4}})
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::PresenceState {
                room: "doc:1".to_string(),
                state: serde_json::json!({"typing": true}),
                ttl_ms: None,
            })
            .unwrap(),
            serde_json::json!({"type": "presenceState", "room": "doc:1", "state": {"typing": true}})
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::LeavePresence {
                room: "doc:1".to_string(),
            })
            .unwrap(),
            serde_json::json!({"type": "leavePresence", "room": "doc:1"})
        );
    }

    #[test]
    fn presence_state_ttl_ms_wire_tag() {
        // ttlMs omitted when None (backward compatible — unchanged shape)
        assert_eq!(
            serde_json::to_value(ClientMessage::PresenceState {
                room: "doc:1".to_string(),
                state: serde_json::json!({"typing": true}),
                ttl_ms: None,
            })
            .unwrap(),
            serde_json::json!({"type": "presenceState", "room": "doc:1", "state": {"typing": true}})
        );
        // ttlMs present when Some
        assert_eq!(
            serde_json::to_value(ClientMessage::PresenceState {
                room: "doc:1".to_string(),
                state: serde_json::json!({"typing": true}),
                ttl_ms: Some(3000),
            })
            .unwrap(),
            serde_json::json!({"type": "presenceState", "room": "doc:1", "state": {"typing": true}, "ttlMs": 3000})
        );
        // and it deserializes back
        let parsed: ClientMessage = serde_json::from_str(
            r#"{"type":"presenceState","room":"doc:1","state":{},"ttlMs":500}"#,
        )
        .unwrap();
        match parsed {
            ClientMessage::PresenceState { ttl_ms, .. } => assert_eq!(ttl_ms, Some(500)),
            _ => panic!("expected PresenceState"),
        }
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
            state: serde_json::json!({"x": 1}),
        };
        assert_eq!(
            serde_json::to_value(ServerMessage::PresenceSnapshot {
                room: "doc:1".to_string(),
                members: vec![member.clone()],
            })
            .unwrap(),
            serde_json::json!({
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
        assert_eq!(
            serde_json::to_value(ServerMessage::PresenceErr {
                room: "doc:1".to_string(),
                error: RtDbError::forbidden("presence not enabled"),
            })
            .unwrap()["type"],
            serde_json::json!("presenceErr")
        );
    }

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

    // FM-29 WS frame vocabulary: tags/fields exactly match the wire shapes the
    // WS handlers and the client SDKs speak (spec §Wire protocol, WS frames).
    #[test]
    fn workflow_frame_wire_shapes() {
        let m = serde_json::to_value(ClientMessage::StartWorkflow {
            workflow_id: "c1".to_string(),
            spec: sample_workflow_spec(),
        })
        .unwrap();
        assert_eq!(m["type"], serde_json::json!("startWorkflow"));
        assert_eq!(m["workflowId"], serde_json::json!("c1"));
        assert_eq!(m["spec"]["name"], serde_json::json!("drip"));

        assert_eq!(
            serde_json::to_value(ClientMessage::CancelWorkflow {
                workflow_id: "c2".to_string(),
                id: "wf9".to_string(),
            })
            .unwrap(),
            serde_json::json!({"type": "cancelWorkflow", "workflowId": "c2", "id": "wf9"})
        );

        // status omitted when None, snake_case string when Some, and the
        // filtered frame parses back.
        assert_eq!(
            serde_json::to_value(ClientMessage::ListWorkflows {
                workflow_id: "c3".to_string(),
                status: None,
            })
            .unwrap(),
            serde_json::json!({"type": "listWorkflows", "workflowId": "c3"})
        );
        let m = serde_json::to_value(ClientMessage::ListWorkflows {
            workflow_id: "c3".to_string(),
            status: Some(WorkflowStatus::Failed),
        })
        .unwrap();
        assert_eq!(m["status"], serde_json::json!("failed"));
        match serde_json::from_value::<ClientMessage>(serde_json::json!({
            "type": "listWorkflows", "workflowId": "c3", "status": "failed"
        }))
        .unwrap()
        {
            ClientMessage::ListWorkflows {
                workflow_id,
                status,
            } => {
                assert_eq!(workflow_id, "c3");
                assert_eq!(status, Some(WorkflowStatus::Failed));
            }
            other => panic!("unexpected variant: {other:?}"),
        }

        let m = serde_json::to_value(ServerMessage::StartWorkflowOk {
            workflow_id: "c1".to_string(),
            info: sample_workflow_info(),
        })
        .unwrap();
        assert_eq!(m["type"], serde_json::json!("startWorkflowOk"));
        assert_eq!(m["workflowId"], serde_json::json!("c1"));
        assert_eq!(m["info"]["id"], serde_json::json!("wf1"));

        let m = serde_json::to_value(ServerMessage::StartWorkflowErr {
            workflow_id: "c1".to_string(),
            error: RtDbError::bad_request("bad spec"),
        })
        .unwrap();
        assert_eq!(m["type"], serde_json::json!("startWorkflowErr"));
        assert_eq!(m["error"]["code"], serde_json::json!("BAD_REQUEST"));

        let m = serde_json::to_value(ServerMessage::WorkflowAck {
            workflow_id: "c1".to_string(),
            ok: true,
            error: None,
        })
        .unwrap();
        assert_eq!(m["type"], serde_json::json!("workflowAck"));
        // `error` is skipped on the wire when the ack is clean.
        assert!(m.get("error").is_none());
        let m = serde_json::to_value(ServerMessage::WorkflowAck {
            workflow_id: "c1".to_string(),
            ok: false,
            error: Some(RtDbError::not_found("no such run")),
        })
        .unwrap();
        assert_eq!(m["ok"], serde_json::json!(false));
        assert_eq!(m["error"]["code"], serde_json::json!("NOT_FOUND"));

        let m = serde_json::to_value(ServerMessage::ListWorkflowsOk {
            workflow_id: "c4".to_string(),
            workflows: vec![sample_workflow_info()],
        })
        .unwrap();
        assert_eq!(m["type"], serde_json::json!("listWorkflowsOk"));
        assert_eq!(m["workflows"][0]["id"], serde_json::json!("wf1"));
    }

    #[test]
    fn workflow_status_round_trips_all_variants() {
        let all = [
            (WorkflowStatus::Pending, "pending"),
            (WorkflowStatus::Running, "running"),
            (WorkflowStatus::Success, "success"),
            (WorkflowStatus::Failed, "failed"),
            (WorkflowStatus::Cancelled, "cancelled"),
        ];
        for (variant, wire) in all {
            assert_eq!(
                serde_json::to_value(variant).unwrap(),
                serde_json::json!(wire)
            );
            assert_eq!(wire.parse::<WorkflowStatus>().unwrap(), variant);
            assert_eq!(variant.as_wire_str(), wire);
        }
    }

    #[test]
    fn workflow_info_full_flatten_round_trip() {
        let full = WorkflowInfoFull {
            info: sample_workflow_info(),
            step_outcomes: vec![StepOutcome {
                step_index: 0,
                status: OutcomeStatus::Success,
                attempts: 1,
                at: 99,
                error: None,
            }],
        };
        let v = serde_json::to_value(&full).unwrap();
        // The flattened info keys land at the TOP level (no "info" wrapper),
        // alongside stepOutcomes.
        assert!(v.get("info").is_none());
        assert_eq!(v["id"], serde_json::json!("wf1"));
        assert_eq!(v["status"], serde_json::json!("pending"));
        assert_eq!(v["stepCount"], serde_json::json!(2));
        assert_eq!(v["stepOutcomes"][0]["stepIndex"], serde_json::json!(0));
        let restored: WorkflowInfoFull = serde_json::from_value(v).unwrap();
        assert_eq!(restored.info.id, "wf1");
        assert_eq!(restored.info.step_count, 2);
        assert_eq!(restored.step_outcomes.len(), 1);

        // deny_unknown_fields: a bogus key is rejected on the plain info
        // projection AND on the flattened full shape.
        let bad_info = serde_json::json!({
            "id": "wf1", "name": "drip", "status": "pending",
            "currentStep": 0, "stepCount": 1, "attempts": 0,
            "createdAt": 1, "updatedAt": 2, "bogus": true
        });
        assert!(serde_json::from_value::<WorkflowInfo>(bad_info).is_err());
        let bad_full = serde_json::json!({
            "id": "wf1", "name": "drip", "status": "pending",
            "currentStep": 0, "stepCount": 1, "attempts": 0,
            "createdAt": 1, "updatedAt": 2, "stepOutcomes": [], "bogus": true
        });
        assert!(serde_json::from_value::<WorkflowInfoFull>(bad_full).is_err());
    }

    // `maxAttempts` is the one required StepRetry field — omitting it is a
    // deserialize error, while initialRetryMs/maxRetryMs default (3.2 §retry).
    #[test]
    fn step_retry_requires_max_attempts() {
        assert!(
            serde_json::from_value::<StepRetry>(serde_json::json!({
                "initialRetryMs": 100, "maxRetryMs": 200
            }))
            .is_err(),
            "a retry object without maxAttempts must not deserialize"
        );
        let r: StepRetry = serde_json::from_value(serde_json::json!({ "maxAttempts": 4 })).unwrap();
        assert_eq!(
            (r.max_attempts, r.initial_retry_ms, r.max_retry_ms),
            (4, 1_000, 60_000)
        );
    }
}
