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
    Auth { token: String, db: String },
    Subscribe { query_id: String, query: Query },
    Unsubscribe { query_id: String },
    Mutate { mut_id: String, txn: Transaction },
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
    Pong,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AuthedUser {
    pub kind: String, // "user" | "machine"
    pub email: Option<String>,
    pub name: Option<String>,
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
                query: sample_query()
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
                txn: sample_txn()
            })
            .unwrap()["type"],
            serde_json::json!("mutate")
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
                    name: None
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
    fn client_message_round_trips_through_json() {
        let msg = ClientMessage::Subscribe {
            query_id: "q1".to_string(),
            query: sample_query(),
        };
        let value = serde_json::to_value(&msg).unwrap();
        let restored: ClientMessage = serde_json::from_value(value).unwrap();
        assert!(matches!(restored, ClientMessage::Subscribe { query_id, .. } if query_id == "q1"));
    }
}
