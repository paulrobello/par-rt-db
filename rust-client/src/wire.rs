//! Wire vocabulary — the third implementation of the protocol contract
//! (server `protocol.rs` first, TS `protocol.ts` second). Tags/fields are load-bearing.

use crate::error::RtDbError;
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

/// A db-side predicate appended to a query's WHERE clause. Mirrors
/// `server/src/query.rs::FilterExpr` byte-for-byte: internally tagged by `op`
/// (lowercase), `deny_unknown_fields`. Leaves compare one declared field to a
/// value (`In` to a non-empty list); `And`/`Or` nest arbitrarily.
///
/// Construct variants directly (`FilterExpr::Eq { field, value }`) — inherent
/// constructors named `eq`/`gt`/`lt` are avoided because they shadow
/// `PartialEq`/`PartialOrd` trait methods (`clippy::should_implement_trait`).
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub(crate) struct PushSchemaRequest<'a> {
        pub(crate) db: &'a str,
        pub(crate) schema: &'a SchemaDef,
    }

    #[derive(Serialize)]
    pub(crate) struct MintTokenRequest<'a> {
        pub(crate) db: &'a str,
        pub(crate) name: &'a str,
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
}
