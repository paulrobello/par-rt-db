//! One-shot HTTP client for par-rt-db. `Authorization: Bearer <token>` on every call.

use crate::error::{ErrorEnvelope, RtDbError};
use crate::mutation::{StepResult, Transaction};
use crate::query::{TableQuery, parse_result};
use crate::wire::AuthedUser;
use serde::Serialize;
use serde::de::DeserializeOwned;

pub struct RtDbHttpClient {
    url: String,
    db: String,
    token: String,
    client: reqwest::Client,
}

impl RtDbHttpClient {
    pub fn new(url: &str, db: &str, token: &str) -> Self {
        let url = url.trim_end_matches('/').to_string();
        Self {
            url,
            db: db.to_string(),
            token: token.to_string(),
            client: reqwest::Client::new(),
        }
    }

    /// Run any built query; deserialize `{result}` into `T`.
    /// Use the terminal that matches `T` (`collect`→`Vec<T>`, `first/unique/get`→`Option<T>`,
    /// `count`→`i64`, `paginate`→`Paginated<T>`).
    pub async fn run<T: DeserializeOwned>(
        &self,
        query: impl Into<crate::query::Query>,
    ) -> Result<T, RtDbError> {
        #[derive(Serialize)]
        struct Body<'a> {
            db: &'a str,
            query: &'a crate::query::Query,
        }
        let query = query.into();
        let body = Body {
            db: &self.db,
            query: &query,
        };
        let resp = self
            .client
            .post(format!("{}/api/query", self.url))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("query request failed: {e}")))?;
        self.json_result::<T>(resp).await
    }

    /// Point read: `{"table","get:id"}` → `Option<T>`.
    pub async fn get<T: DeserializeOwned>(
        &self,
        table: &str,
        id: &str,
    ) -> Result<Option<T>, RtDbError> {
        self.run(TableQuery::get(table, id)).await
    }

    /// Run a transaction; returns one `StepResult` per step.
    pub async fn mutate(
        &self,
        txn: &Transaction,
        idempotency_key: Option<&str>,
    ) -> Result<Vec<StepResult>, RtDbError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body<'a> {
            db: &'a str,
            txn: &'a Transaction,
            #[serde(skip_serializing_if = "Option::is_none")]
            idempotency_key: Option<&'a str>,
        }
        let body = Body {
            db: &self.db,
            txn,
            idempotency_key,
        };
        let resp = self
            .client
            .post(format!("{}/api/mutate", self.url))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("mutate request failed: {e}")))?;
        #[derive(serde::Deserialize)]
        struct MutateResponse {
            results: Vec<serde_json::Value>,
        }
        let parsed = self.deserialize::<MutateResponse>(resp).await?;
        parsed
            .results
            .into_iter()
            .map(|v| {
                serde_json::from_value::<StepResult>(v)
                    .map_err(|e| RtDbError::internal(format!("invalid step result: {e}")))
            })
            .collect()
    }

    /// Validate the bearer (session) token via `GET /auth/me`. Machine tokens get 401.
    pub async fn auth_me(&self) -> Result<AuthedUser, RtDbError> {
        let resp = self
            .client
            .get(format!("{}/auth/me", self.url))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("auth_me request failed: {e}")))?;
        #[derive(serde::Deserialize)]
        struct MeResponse {
            user: AuthedUser,
        }
        let parsed = self.deserialize::<MeResponse>(resp).await?;
        Ok(parsed.user)
    }

    async fn json_result<T: DeserializeOwned>(
        &self,
        resp: reqwest::Response,
    ) -> Result<T, RtDbError> {
        #[derive(serde::Deserialize)]
        struct QueryResponse {
            result: serde_json::Value,
        }
        let parsed = self.deserialize::<QueryResponse>(resp).await?;
        parse_result::<T>(parsed.result)
    }

    async fn deserialize<T: DeserializeOwned>(
        &self,
        resp: reqwest::Response,
    ) -> Result<T, RtDbError> {
        let status = resp.status();
        if status.is_success() {
            return resp
                .json::<T>()
                .await
                .map_err(|e| RtDbError::internal(format!("invalid response body: {e}")));
        }
        // Error path: try to parse {code,message}, else INTERNAL.
        match resp.json::<ErrorEnvelope>().await {
            Ok(env) => Err(RtDbError::from_envelope(env)),
            Err(_) => Err(RtDbError::internal(format!(
                "request failed with status {}",
                status.as_u16()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutation::Mutation;
    use crate::query::TableQuery;
    use serde_json::{Value, json};
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn doc(id: &str) -> Value {
        json!({"_id": id, "name": format!("n-{id}")})
    }

    async fn setup() -> (MockServer, RtDbHttpClient) {
        let server = MockServer::start().await;
        let client = RtDbHttpClient::new(server.uri().as_str(), "t<uuid>", "machine-token");
        (server, client)
    }

    #[tokio::test]
    async fn run_collect_posts_query_and_parses_result() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/api/query"))
            .and(header("authorization", "Bearer machine-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": [doc("a"), doc("b")]
            })))
            .mount(&server)
            .await;
        let q = TableQuery::new("items")
            .with_index("by_status", &[json!("active")])
            .take(2);
        let got: Vec<Value> = client.run(q).await.unwrap();
        assert_eq!(got.len(), 2);
    }

    #[tokio::test]
    async fn run_count_parses_number() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/api/query"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result": 5})))
            .mount(&server)
            .await;
        let n: i64 = client.run(TableQuery::new("items").count()).await.unwrap();
        assert_eq!(n, 5);
    }

    #[tokio::test]
    async fn get_returns_optional_doc() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/api/query"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result": doc("a")})))
            .mount(&server)
            .await;
        let some: Option<Value> = client.get("items", "a").await.unwrap();
        assert!(some.is_some());
    }

    #[tokio::test]
    async fn mutate_posts_and_parses_results() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/api/mutate"))
            .and(header("authorization", "Bearer machine-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "results": [{"id":"new1"}, null]
            })))
            .mount(&server)
            .await;
        let txn = Mutation::new()
            .insert("items", json!({"name":"x"}))
            .patch("items", "i1", json!({"y":1}))
            .build();
        let res = client.mutate(&txn, None).await.unwrap();
        assert_eq!(res.len(), 2);
        assert!(matches!(res[0], crate::mutation::StepResult::Insert { ref id } if id == "new1"));
    }

    #[tokio::test]
    async fn mutate_sends_idempotency_key() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/api/mutate"))
            .and(wiremock::matchers::body_partial_json(
                json!({"idempotencyKey":"k1"}),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"results":[]})))
            .mount(&server)
            .await;
        let txn = Mutation::new().delete("items", "i1").build();
        client.mutate(&txn, Some("k1")).await.unwrap();
    }

    #[tokio::test]
    async fn error_envelope_becomes_rtdb_error() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/api/query"))
            .respond_with(
                ResponseTemplate::new(409).set_body_json(
                    json!({"code":"PRECONDITION_FAILED","message":"version mismatch"}),
                ),
            )
            .mount(&server)
            .await;
        let err = client
            .run::<i64>(TableQuery::new("items").count())
            .await
            .unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::PreconditionFailed);
        assert_eq!(err.message, "version mismatch");
    }

    #[tokio::test]
    async fn non_envelope_error_is_internal() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/api/query"))
            .respond_with(ResponseTemplate::new(500).set_body_string("gateway down"))
            .mount(&server)
            .await;
        let err = client
            .run::<i64>(TableQuery::new("items").count())
            .await
            .unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::Internal);
    }

    #[tokio::test]
    async fn auth_me_returns_user() {
        let (server, client) = setup().await;
        Mock::given(method("GET"))
            .and(path("/auth/me"))
            .and(header("authorization", "Bearer machine-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "user": {"kind":"user","email":"a@b.com","name":null}
            })))
            .mount(&server)
            .await;
        let user = client.auth_me().await.unwrap();
        assert_eq!(user.kind, "user");
        assert_eq!(user.email.as_deref(), Some("a@b.com"));
    }
}
