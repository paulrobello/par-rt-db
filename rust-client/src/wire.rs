//! Wire vocabulary — the third implementation of the protocol contract
//! (server `protocol.rs` first, TS `protocol.ts` second). Tags/fields are load-bearing.

use crate::error::RtDbError;
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
    Pong,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthedUser {
    pub kind: String,
    pub email: Option<String>,
    pub name: Option<String>,
    /// GitHub login. Absent on the wire for machine tokens / non-GitHub
    /// users; serde defaults a missing field to `None` so this stays
    /// backward-compatible with older servers that omit it.
    #[serde(default)]
    pub github_login: Option<String>,
    #[serde(default)]
    pub github_id: Option<i64>,
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
                token: "t".into(),
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
                kind: "user".into(),
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
}
