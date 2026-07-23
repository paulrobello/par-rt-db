//! One-shot HTTP client for par-rt-db. `Authorization: Bearer <token>` on every call.

use crate::error::{ErrorEnvelope, RtDbError, retry_on_precondition};
use crate::mutation::{StepResult, Transaction};
use crate::query::{TableQuery, parse_result};
use crate::wire::AuthedUser;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::future::Future;

/// Maximum total attempts for [`RtDbHttpClient::mutate_with_retry`] when a
/// caller wants the SDK's default bound. Matches `ts-client`'s
/// `retryOnPrecondition` default of 4 retries (5 total attempts).
pub const DEFAULT_MUTATE_MAX_ATTEMPTS: u32 = 5;

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

    /// Read-modify-write a single document with optimistic-concurrency retry.
    ///
    /// Fetches the doc at `(table, id)` (as `Option<T>`, `None` if absent),
    /// hands it to `rebuild`, and submits the returned [`Transaction`] through
    /// [`mutate`](Self::mutate). If the server rejects with `PRECONDITION_FAILED`
    /// (an `expectVersion` conflict from a concurrent write), it re-fetches and
    /// retries — up to `max_attempts` total attempts — surfacing the final error
    /// if it never succeeds. Pass [`DEFAULT_MUTATE_MAX_ATTEMPTS`] for the
    /// recommended bound.
    ///
    /// Fold the fetched `_version` into a
    /// [`Mutation::expect_version`](crate::mutation::Mutation::expect_version)
    /// step so a concurrent write triggers a retry rather than silently
    /// overwriting.
    ///
    /// This composes [`retry_on_precondition`] — it does not re-implement
    /// conflict detection.
    pub async fn mutate_with_retry<T, F, Fut>(
        &self,
        table: &str,
        id: &str,
        max_attempts: u32,
        rebuild: F,
    ) -> Result<Vec<StepResult>, RtDbError>
    where
        T: DeserializeOwned,
        F: Fn(Option<T>) -> Fut,
        Fut: Future<Output = Result<Transaction, RtDbError>>,
    {
        // Bind `rebuild` by shared ref so the per-attempt closure captures only
        // `Copy` handles and can move them into each fresh future — required for
        // `retry_on_precondition`'s `FnMut() -> Fut` bound (a single `Fut` type
        // can't borrow from the closure's state across calls).
        let rebuild = &rebuild;
        retry_on_precondition(
            move || async move {
                let current = self.get::<T>(table, id).await?;
                let txn = rebuild(current).await?;
                self.mutate(&txn, None).await
            },
            max_attempts.saturating_sub(1),
        )
        .await
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

/// Admin control-plane methods (`/admin/*`). The client's bearer token must be
/// the instance admin key for these to authorize (constant-time compared server
/// side, same as every other bearer call). Gated on the `admin` feature, which
/// implies `http`. Mirrors `ts-client`'s `RtDbAdminClient` one-to-one — paths,
/// bodies, and return shapes are identical; only the method names are snake_cased.
#[cfg(feature = "admin")]
impl RtDbHttpClient {
    /// `POST /admin/create-db` `{name}` → `{ok:true}`.
    pub async fn create_db(&self, name: &str) -> Result<(), RtDbError> {
        let resp = self
            .post_json(
                "/admin/create-db",
                &crate::wire::admin::CreateDbRequest { name },
            )
            .await?;
        self.expect_ok(resp).await
    }

    /// `POST /admin/push-schema` `{db, schema}` → `{ok:true}`.
    pub async fn push_schema(
        &self,
        db: &str,
        schema: &crate::schema::SchemaDef,
    ) -> Result<(), RtDbError> {
        let resp = self
            .post_json(
                "/admin/push-schema",
                &crate::wire::admin::PushSchemaRequest { db, schema },
            )
            .await?;
        self.expect_ok(resp).await
    }

    /// `GET /admin/dbs` → `{databases:[...]}`.
    pub async fn list_dbs(&self) -> Result<Vec<String>, RtDbError> {
        let parsed: crate::wire::admin::DatabasesResponse =
            self.get_json("/admin/dbs", &[]).await?;
        Ok(parsed.databases)
    }

    /// `POST /admin/mint-token` `{db, name}` → `{tokenId, token}`.
    pub async fn mint_token(
        &self,
        db: &str,
        name: &str,
    ) -> Result<crate::wire::admin::MintedToken, RtDbError> {
        let resp = self
            .post_json(
                "/admin/mint-token",
                &crate::wire::admin::MintTokenRequest { db, name },
            )
            .await?;
        self.deserialize::<crate::wire::admin::MintedToken>(resp)
            .await
    }

    /// `POST /admin/revoke-token` `{tokenId}` → `{ok:true}`.
    pub async fn revoke_token(&self, token_id: &str) -> Result<(), RtDbError> {
        let resp = self
            .post_json(
                "/admin/revoke-token",
                &crate::wire::admin::RevokeTokenRequest { token_id },
            )
            .await?;
        self.expect_ok(resp).await
    }

    /// `POST /admin/allowlist` `{db, action:"add", email}` → `{ok:true}`.
    pub async fn allowlist_add(&self, db: &str, email: &str) -> Result<(), RtDbError> {
        let resp = self
            .post_json(
                "/admin/allowlist",
                &crate::wire::admin::AllowlistWriteRequest {
                    db,
                    action: "add",
                    email,
                },
            )
            .await?;
        self.expect_ok(resp).await
    }

    /// `POST /admin/allowlist` `{db, action:"remove", email}` → `{ok:true}`.
    pub async fn allowlist_remove(&self, db: &str, email: &str) -> Result<(), RtDbError> {
        let resp = self
            .post_json(
                "/admin/allowlist",
                &crate::wire::admin::AllowlistWriteRequest {
                    db,
                    action: "remove",
                    email,
                },
            )
            .await?;
        self.expect_ok(resp).await
    }

    /// `GET /admin/allowlist?db=<db>` → `{emails:[...]}`.
    pub async fn allowlist_list(&self, db: &str) -> Result<Vec<String>, RtDbError> {
        let parsed: crate::wire::admin::AllowlistListResponse =
            self.get_json("/admin/allowlist", &[("db", db)]).await?;
        Ok(parsed.emails)
    }

    /// `GET /admin/export-db?db=<db>` → the database's schema + every document as
    /// JSONL text (see server `snapshot::export_database`).
    pub async fn export_db(&self, db: &str) -> Result<String, RtDbError> {
        let resp = self
            .client
            .get(format!("{}/admin/export-db", self.url))
            .bearer_auth(&self.token)
            .query(&[("db", db)])
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("export_db request failed: {e}")))?;
        let status = resp.status();
        if status.is_success() {
            return resp
                .text()
                .await
                .map_err(|e| RtDbError::internal(format!("invalid export body: {e}")));
        }
        Err(self.error_response(resp).await)
    }

    /// `POST /admin/import-db?db=<db>` with an `application/x-ndjson` body of a
    /// snapshot produced by [`export_db`](Self::export_db) (see server
    /// `snapshot::import_database`).
    pub async fn import_db(&self, db: &str, jsonl: &str) -> Result<(), RtDbError> {
        let resp = self
            .client
            .post(format!("{}/admin/import-db", self.url))
            .bearer_auth(&self.token)
            .query(&[("db", db)])
            .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
            .body(jsonl.to_string())
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("import_db request failed: {e}")))?;
        self.expect_ok(resp).await
    }

    async fn post_json<Req: Serialize>(
        &self,
        path: &str,
        body: &Req,
    ) -> Result<reqwest::Response, RtDbError> {
        self.client
            .post(format!("{}{}", self.url, path))
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("admin request failed: {e}")))
    }

    async fn get_json<Resp: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<Resp, RtDbError> {
        let resp = self
            .client
            .get(format!("{}{}", self.url, path))
            .bearer_auth(&self.token)
            .query(query)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("admin request failed: {e}")))?;
        self.deserialize::<Resp>(resp).await
    }

    async fn expect_ok(&self, resp: reqwest::Response) -> Result<(), RtDbError> {
        let parsed: crate::wire::admin::OkResponse = self.deserialize(resp).await?;
        if !parsed.ok {
            return Err(RtDbError::internal("admin request returned ok=false"));
        }
        Ok(())
    }

    async fn error_response(&self, resp: reqwest::Response) -> RtDbError {
        let status = resp.status();
        match resp.json::<ErrorEnvelope>().await {
            Ok(env) => RtDbError::from_envelope(env),
            Err(_) => {
                RtDbError::internal(format!("request failed with status {}", status.as_u16()))
            }
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

    // `mutate_with_retry` reuses `retry_on_precondition`, so these tests focus on
    // the read-modify-write composition: a conflict re-fetches and retries, and
    // `max_attempts` bounds the loop. Rebuild folds the fetched `_version` into an
    // `expectVersion` step, mirroring the common write-through pattern.
    fn current_doc() -> Value {
        json!({"_id": "i1", "_version": 3, "n": 1})
    }

    // Fetches the doc's `_version` and guards a patch with `expectVersion`.
    fn write_through(current: Option<Value>) -> Result<Transaction, RtDbError> {
        let version = current
            .as_ref()
            .and_then(|c| c.get("_version"))
            .and_then(Value::as_i64)
            .unwrap_or(0);
        Ok(Mutation::new()
            .expect_version("items", "i1", version)
            .patch("items", "i1", json!({"n": 2}))
            .build())
    }

    #[tokio::test]
    async fn mutate_with_retry_succeeds_on_first_attempt() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/api/query"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"result": current_doc()})),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/mutate"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"results":[{"id":"i1"}]})),
            )
            .mount(&server)
            .await;

        let res = client
            .mutate_with_retry::<Value, _, _>(
                "items",
                "i1",
                DEFAULT_MUTATE_MAX_ATTEMPTS,
                |current| async move { write_through(current) },
            )
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert!(matches!(res[0], crate::mutation::StepResult::Insert { ref id } if id == "i1"));
    }

    #[tokio::test]
    async fn mutate_with_retry_retries_once_then_succeeds() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU32, Ordering};
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/api/query"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"result": current_doc()})),
            )
            .mount(&server)
            .await;
        let mutate_calls = Arc::new(AtomicU32::new(0));
        let calls = mutate_calls.clone();
        Mock::given(method("POST"))
            .and(path("/api/mutate"))
            .respond_with(move |_: &wiremock::Request| {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    ResponseTemplate::new(409).set_body_json(json!({
                        "code": "PRECONDITION_FAILED",
                        "message": "version mismatch: expected 3, actual 4"
                    }))
                } else {
                    ResponseTemplate::new(200).set_body_json(json!({"results":[{"id":"i1"}]}))
                }
            })
            .mount(&server)
            .await;

        let res = client
            .mutate_with_retry::<Value, _, _>(
                "items",
                "i1",
                DEFAULT_MUTATE_MAX_ATTEMPTS,
                |current| async move { write_through(current) },
            )
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        // One initial attempt plus exactly one retry.
        assert_eq!(mutate_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn mutate_with_retry_exhausts_attempts_and_surfaces_error() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU32, Ordering};
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/api/query"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"result": current_doc()})),
            )
            .mount(&server)
            .await;
        let mutate_calls = Arc::new(AtomicU32::new(0));
        let calls = mutate_calls.clone();
        Mock::given(method("POST"))
            .and(path("/api/mutate"))
            .respond_with(move |_: &wiremock::Request| {
                calls.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(409).set_body_json(
                    json!({"code": "PRECONDITION_FAILED", "message": "version mismatch"}),
                )
            })
            .mount(&server)
            .await;

        let err = client
            .mutate_with_retry::<Value, _, _>("items", "i1", 3, |current| async move {
                write_through(current)
            })
            .await
            .unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::PreconditionFailed);
        assert_eq!(err.message, "version mismatch");
        // `max_attempts = 3` → initial attempt plus 2 retries, then give up.
        assert_eq!(mutate_calls.load(Ordering::SeqCst), 3);
    }
}

/// Mirrors `ts-client/tests/admin.test.ts`: each method posts/gets the right
/// path with the admin-key bearer, the right body shape, and returns the right
/// type. `wiremock` matchers assert the on-the-wire request; `#[ignore]`-free
/// because they hit a mock, not a real server.
#[cfg(all(test, feature = "admin"))]
mod admin_tests {
    use super::RtDbHttpClient;
    use crate::error::ErrorCode;
    use crate::schema::{FieldType, SchemaDef, Table};
    use serde_json::json;
    use wiremock::matchers::{
        body_partial_json, body_string_contains, header, method, path, query_param,
    };
    use wiremock::{Mock, MockServer, ResponseTemplate};

    // Admin methods authorize via the bearer, so the token passed to the client
    // is the instance admin key here — same field as every other call.
    const BEARER: &str = "Bearer admin-key";

    async fn setup() -> (MockServer, RtDbHttpClient) {
        let server = MockServer::start().await;
        let client = RtDbHttpClient::new(server.uri().as_str(), "kanban", "admin-key");
        (server, client)
    }

    #[tokio::test]
    async fn create_db_posts_name() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/admin/create-db"))
            .and(header("authorization", BEARER))
            .and(body_partial_json(json!({"name": "kanban"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
            .mount(&server)
            .await;
        client.create_db("kanban").await.unwrap();
    }

    #[tokio::test]
    async fn push_schema_serializes_schema_json() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/admin/push-schema"))
            .and(header("authorization", BEARER))
            .and(body_partial_json(json!({
                "db": "kanban",
                "schema": {"tables": {"notes": {"fields": {"body": {"type": "string"}}}}}
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
            .mount(&server)
            .await;
        let schema = SchemaDef::builder()
            .table("notes", Table::new().field("body", FieldType::String))
            .build();
        client.push_schema("kanban", &schema).await.unwrap();
    }

    #[tokio::test]
    async fn list_dbs_returns_databases() {
        let (server, client) = setup().await;
        Mock::given(method("GET"))
            .and(path("/admin/dbs"))
            .and(header("authorization", BEARER))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"databases": ["kanban", "demo"]})),
            )
            .mount(&server)
            .await;
        let dbs = client.list_dbs().await.unwrap();
        assert_eq!(dbs, vec!["kanban".to_string(), "demo".to_string()]);
    }

    #[tokio::test]
    async fn mint_token_returns_token_id_and_token() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/admin/mint-token"))
            .and(body_partial_json(json!({"db": "kanban", "name": "cli"})))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"tokenId": "id1", "token": "secret"})),
            )
            .mount(&server)
            .await;
        let minted = client.mint_token("kanban", "cli").await.unwrap();
        assert_eq!(minted.token_id, "id1");
        assert_eq!(minted.token, "secret");
    }

    #[tokio::test]
    async fn revoke_token_posts_token_id() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/admin/revoke-token"))
            .and(body_partial_json(json!({"tokenId": "tid"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
            .mount(&server)
            .await;
        client.revoke_token("tid").await.unwrap();
    }

    #[tokio::test]
    async fn allowlist_add_posts_action() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/admin/allowlist"))
            .and(body_partial_json(
                json!({"db": "kanban", "action": "add", "email": "a@b.com"}),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
            .mount(&server)
            .await;
        client.allowlist_add("kanban", "a@b.com").await.unwrap();
    }

    #[tokio::test]
    async fn allowlist_remove_posts_action() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/admin/allowlist"))
            .and(body_partial_json(
                json!({"db": "kanban", "action": "remove", "email": "a@b.com"}),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
            .mount(&server)
            .await;
        client.allowlist_remove("kanban", "a@b.com").await.unwrap();
    }

    #[tokio::test]
    async fn allowlist_list_uses_query_string() {
        let (server, client) = setup().await;
        Mock::given(method("GET"))
            .and(path("/admin/allowlist"))
            .and(query_param("db", "kanban"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"emails": ["a@b.com"]})))
            .mount(&server)
            .await;
        let emails = client.allowlist_list("kanban").await.unwrap();
        assert_eq!(emails, vec!["a@b.com".to_string()]);
    }

    #[tokio::test]
    async fn export_db_returns_jsonl_text() {
        let (server, client) = setup().await;
        let jsonl = "{\"kind\":\"schema\",\"schema\":{\"tables\":{}}}\n";
        Mock::given(method("GET"))
            .and(path("/admin/export-db"))
            .and(query_param("db", "kanban"))
            .and(header("authorization", BEARER))
            .respond_with(ResponseTemplate::new(200).set_body_string(jsonl))
            .mount(&server)
            .await;
        let got = client.export_db("kanban").await.unwrap();
        assert_eq!(got, jsonl);
    }

    #[tokio::test]
    async fn export_db_surfaces_error_envelope() {
        let (server, client) = setup().await;
        Mock::given(method("GET"))
            .and(path("/admin/export-db"))
            .respond_with(
                ResponseTemplate::new(404)
                    .set_body_json(json!({"code": "NOT_FOUND", "message": "unknown database"})),
            )
            .mount(&server)
            .await;
        let err = client.export_db("missing").await.unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
        assert_eq!(err.message, "unknown database");
    }

    #[tokio::test]
    async fn import_db_posts_ndjson_body() {
        let (server, client) = setup().await;
        let jsonl = "{\"kind\":\"schema\",\"schema\":{\"tables\":{}}}\n";
        Mock::given(method("POST"))
            .and(path("/admin/import-db"))
            .and(query_param("db", "kanban"))
            .and(header("content-type", "application/x-ndjson"))
            .and(body_string_contains(jsonl))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
            .mount(&server)
            .await;
        client.import_db("kanban", jsonl).await.unwrap();
    }
}
