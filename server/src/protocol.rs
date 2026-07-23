use crate::error::RtDbError;
use crate::query::Query;
use crate::txn::Transaction;

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
        token: String,
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

/// Full WS server vocabulary.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthedUser {
    pub kind: String, // "user" | "machine"
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
}

/// A scheduled job's public view (returned by `listSchedules`). `cron` and
/// `last_error` are omitted on the wire when absent. The canonical home is
/// `protocol::ScheduleInfo`; `scheduler` re-exports it for its `list` return
/// type and existing call sites.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleInfo {
    pub id: String,
    pub kind: String, // "oneshot" | "cron"
    pub due_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cron: Option<String>,
    pub status: String, // "pending" | "running" | "paused" | "error"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub created_at: i64,
    pub fired_count: i64,
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
                token: "t".to_string(),
                db: "d".to_string()
            })
            .unwrap(),
            serde_json::json!({"type": "auth", "token": "t", "db": "d"})
        );
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
                    kind: "user".to_string(),
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
}
