//! Wire vocabulary — the third implementation of the protocol contract
//! (server `protocol.rs` first, TS `protocol.ts` second). Tags/fields are load-bearing.

use crate::error::{ErrorEnvelope, RtDbError};
use crate::mutation::Transaction;
use crate::query::Query;
use serde::{Deserialize, Serialize};

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

/// One entry in a presence room's member list. Mirrors
/// `server/src/protocol.rs::PresenceMember` byte-for-byte (camelCase):
/// `connectionId` is the opaque per-session key, `user` carries display
/// identity, `state` is an opaque client-supplied blob.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PresenceMember {
    pub connection_id: String,
    pub user: AuthedUser,
    pub state: serde_json::Value,
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
/// search index on the query's table; `query` is free-form user text; `filter`
/// is an optional `FilterExpr` (the db-side `filter()` DSL) that narrows the
/// search `WHERE` server-side. Mirrors `server/src/query.rs::SearchQuery`
/// byte-for-byte (camelCase, deny_unknown_fields). The nested `filter` is
/// additive — omitted when `None`, so existing search requests round-trip
/// unchanged — and distinct from the Query-level top-level `filter` builder.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SearchQuery {
    pub index: String,
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<FilterExpr>,
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
    pub index: String,
    // ARC-008(a): f64 (not f32) — the server, TS, and Python clients all carry
    // full JSON-number precision, so narrowing to f32 here was the lone path
    // that silently dropped precision on a round-trip. f64 matches the wire.
    pub vector: Vec<f64>,
    pub limit: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
/// `server/src/query.rs::AggregateOp` byte-for-byte (lowercase variants). `Count`
/// aggregates rows and consumes no aggregate field (a grouped `count` is the
/// count per group).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AggregateOp {
    Sum,
    Avg,
    Min,
    Max,
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
/// ARC-130: response-shaped — `#[non_exhaustive]` lets the wire shape gain
/// fields later without breaking exhaustive destructures.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
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
