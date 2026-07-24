//! One-shot HTTP client for par-rt-db. `Authorization: Bearer <token>` on every call.

use crate::error::{ErrorEnvelope, RtDbError, retry_on_precondition};
use crate::mutation::{Mutation, StepResult, Transaction};
use crate::query::{TableQuery, parse_result};
use crate::wire::{AuthedUser, ScheduleInfo, ScheduleWhen};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::future::Future;

/// Maximum total attempts for [`RtDbHttpClient::mutate_with_retry`] when a
/// caller wants the SDK's default bound. Matches `ts-client`'s
/// `retryOnPrecondition` default of 4 retries (5 total attempts).
pub const DEFAULT_MUTATE_MAX_ATTEMPTS: u32 = 5;

/// Percent-encode `s` the way the browser's `encodeURIComponent` does, so a
/// schedule id interpolated into a URL path segment is byte-identical to what
/// the TS client sends (`encodeURIComponent(id)`). Today ids are server-
/// generated uuid v7 (`[0-9a-f-]`, all unescaped), so this is a no-op in
/// practice — it exists for cross-client consistency if that ever changes.
fn encode_uri_component(s: &str) -> String {
    const UNESCAPED: &str = "-_.!~*'()";
    let mut out = String::with_capacity(s.len());
    // Byte-wise iteration reproduces `encodeURIComponent`: ASCII chars are
    // encoded one-for-one, and every byte of a multi-byte UTF-8 sequence is
    // >= 0x80 (non-alphanumeric) so it is percent-encoded individually — which
    // is exactly the UTF-8 percent-encoding the browser emits.
    for &b in s.as_bytes() {
        let c = b as char;
        if c.is_ascii_alphanumeric() || UNESCAPED.contains(c) {
            out.push(c);
        } else {
            out.push('%');
            // uppercase hex, matching encodeURIComponent
            out.push_str(&format!("{:02X}", b));
        }
    }
    out
}

pub struct RtDbHttpClient {
    url: String,
    db: String,
    token: String,
    client: reqwest::Client,
}

/// Result of [`RtDbHttpClient::upload`] — the server-computed file identity,
/// content hash, size in bytes, and (if the upload carried one) the stored
/// `contentType`. `contentType` is `#[serde(default)]` so an older server
/// omitting the field deserializes to `None`.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadResult {
    pub id: String,
    pub sha256: String,
    pub size: i64,
    #[serde(default)]
    pub content_type: Option<String>,
}

/// File metadata returned by [`RtDbHttpClient::get_file_metadata`]. Mirrors
/// `UploadResult` plus the server-recorded `creationTime`.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileMetadata {
    pub id: String,
    pub sha256: String,
    pub size: i64,
    #[serde(default)]
    pub content_type: Option<String>,
    pub creation_time: i64,
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

    /// Find the single doc matching `value` on `index`, or `None` if no doc
    /// matches. A thin wrapper over the indexed `eq` query path using the
    /// `first` terminal (server runs `LIMIT 1`), so it returns at most one doc
    /// and never errors on duplicate matches. On a unique index this is
    /// exactly-one semantics; on a non-unique index it returns one of the
    /// matches deterministically (by index order, then creation time, then id).
    pub async fn find_one_by_index<T: DeserializeOwned>(
        &self,
        table: &str,
        index: &str,
        value: impl Into<serde_json::Value>,
    ) -> Result<Option<T>, RtDbError> {
        let query = TableQuery::new(table)
            .with_index(index, &[value.into()])
            .first();
        self.run::<Option<T>>(query).await
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

    /// Upsert by index-field value: builds a one-step transaction that matches
    /// `value` on `index` — match → patch the provided fields, no match → insert
    /// — and runs it via [`mutate`](Self::mutate). Returns the resulting doc id
    /// and whether it was inserted, as a [`StepResult::Upsert`].
    ///
    /// The server's upsert step requires the `eq` value to cover the index's
    /// full arity (a single value fits a single-field index). If more than one
    /// doc matches it rejects with `PRECONDITION_FAILED`; the helper surfaces
    /// that error rather than retrying, since it is not a transient conflict.
    pub async fn upsert_by_index(
        &self,
        table: &str,
        index: &str,
        value: impl Into<serde_json::Value>,
        insert: serde_json::Value,
        patch: serde_json::Value,
    ) -> Result<StepResult, RtDbError> {
        let txn = Mutation::new()
            .upsert(table, index, &[value.into()], insert, patch)
            .build();
        let mut results = self.mutate(&txn, None).await?;
        results
            .pop()
            .ok_or_else(|| RtDbError::internal("upsert returned no result"))
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

    /// Schedule `txn` to fire at `when`. The server validates cron expressions
    /// and resolves the due time; the client does no schedule arithmetic. Returns
    /// the new schedule's id. Mirrors `ts-client`'s `schedule`.
    pub async fn schedule(
        &self,
        txn: &Transaction,
        when: ScheduleWhen,
    ) -> Result<String, RtDbError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body<'a> {
            db: &'a str,
            when: ScheduleWhen,
            txn: &'a Transaction,
        }
        let body = Body {
            db: &self.db,
            when,
            txn,
        };
        let resp = self
            .client
            .post(format!("{}/api/schedule", self.url))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("schedule request failed: {e}")))?;
        #[derive(serde::Deserialize)]
        struct ScheduleResponse {
            id: String,
        }
        let parsed = self.deserialize::<ScheduleResponse>(resp).await?;
        Ok(parsed.id)
    }

    /// Cancel a scheduled job (`POST /api/schedule/{id}/cancel`). The server
    /// returns `{ok:true}` on success.
    pub async fn cancel_schedule(&self, id: &str) -> Result<(), RtDbError> {
        self.manage_schedule(id, "cancel").await
    }

    /// Pause a scheduled job until [`resume_schedule`](Self::resume_schedule) is
    /// called (`POST /api/schedule/{id}/pause`).
    pub async fn pause_schedule(&self, id: &str) -> Result<(), RtDbError> {
        self.manage_schedule(id, "pause").await
    }

    /// Resume a paused scheduled job (`POST /api/schedule/{id}/resume`).
    pub async fn resume_schedule(&self, id: &str) -> Result<(), RtDbError> {
        self.manage_schedule(id, "resume").await
    }

    /// Shared authorize-then-op body for the three boolean manage handlers. `op`
    /// is always a hardcoded literal ("cancel" | "pause" | "resume"), never
    /// caller-supplied, so interpolating it into the path is safe.
    async fn manage_schedule(&self, id: &str, op: &str) -> Result<(), RtDbError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body<'a> {
            db: &'a str,
        }
        let body = Body { db: &self.db };
        let resp = self
            .client
            // `id` is caller-supplied, so percent-encode the path segment;
            // `op` is always a hardcoded literal, never interpolated raw from input.
            .post(format!(
                "{}/api/schedule/{}/{op}",
                self.url,
                encode_uri_component(id)
            ))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("schedule {op} request failed: {e}")))?;
        #[derive(serde::Deserialize)]
        struct ManageResponse {
            ok: bool,
        }
        let parsed = self.deserialize::<ManageResponse>(resp).await?;
        if !parsed.ok {
            return Err(RtDbError::internal("schedule operation returned ok=false"));
        }
        Ok(())
    }

    /// List scheduled jobs for this client's database (`POST /api/schedules`).
    pub async fn list_schedules(&self) -> Result<Vec<ScheduleInfo>, RtDbError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body<'a> {
            db: &'a str,
        }
        #[derive(serde::Deserialize)]
        struct ListResponse {
            schedules: Vec<ScheduleInfo>,
        }
        let body = Body { db: &self.db };
        let resp = self
            .client
            .post(format!("{}/api/schedules", self.url))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("list schedules request failed: {e}")))?;
        let parsed = self.deserialize::<ListResponse>(resp).await?;
        Ok(parsed.schedules)
    }

    /// Upload raw bytes; `content_type` sets the Content-Type header and is
    /// stored as the file's type. Returns the server-computed metadata.
    pub async fn upload(
        &self,
        bytes: &[u8],
        content_type: Option<&str>,
    ) -> Result<UploadResult, RtDbError> {
        let mut req = self
            .client
            .post(format!("{}/api/storage/{}", self.url, self.db))
            .bearer_auth(&self.token)
            .body(bytes.to_vec());
        if let Some(ct) = content_type {
            req = req.header(reqwest::header::CONTENT_TYPE, ct);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("upload request failed: {e}")))?;
        self.deserialize::<UploadResult>(resp).await
    }

    pub async fn delete_file(&self, id: &str) -> Result<(), RtDbError> {
        let resp = self
            .client
            .delete(format!("{}/api/storage/{}/{id}", self.url, self.db))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("delete file request failed: {e}")))?;
        #[derive(serde::Deserialize)]
        struct OkResp {
            ok: bool,
        }
        let parsed = self.deserialize::<OkResp>(resp).await?;
        if !parsed.ok {
            return Err(RtDbError::internal("delete file returned ok=false"));
        }
        Ok(())
    }

    pub async fn get_file_metadata(&self, id: &str) -> Result<FileMetadata, RtDbError> {
        let resp = self
            .client
            .get(format!(
                "{}/api/storage/{}/{id}/metadata",
                self.url, self.db
            ))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("file metadata request failed: {e}")))?;
        self.deserialize::<FileMetadata>(resp).await
    }

    /// The public serve URL — no request is made.
    pub fn get_url(&self, id: &str) -> String {
        format!("{}/storage/{id}", self.url)
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

    /// Validate an arbitrary player-supplied session/machine token via
    /// `GET /auth/validate`, returning the authed user. Unlike `auth_me`
    /// (which validates this client's own token), this takes the token to
    /// validate as an argument and accepts both session and machine tokens —
    /// for a trusted backend validating a player's token. An invalid/expired
    /// token surfaces as the standard `RtDbError` auth envelope.
    pub async fn validate_session_token(&self, token: &str) -> Result<AuthedUser, RtDbError> {
        let resp = self
            .client
            .get(format!("{}/auth/validate", self.url))
            .bearer_auth(token)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("validate request failed: {e}")))?;
        #[derive(serde::Deserialize)]
        struct ValidateResponse {
            user: AuthedUser,
        }
        let parsed = self.deserialize::<ValidateResponse>(resp).await?;
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
    use wiremock::matchers::{body_bytes, body_partial_json, header, method, path};
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
    async fn schedule_posts_when_and_txn_and_returns_id() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/api/schedule"))
            .and(header("authorization", "Bearer machine-token"))
            .and(body_partial_json(json!({
                "db": "t<uuid>",
                "when": {"type": "afterMs", "ms": 5000},
                "txn": {"steps": []}
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "job-7"})))
            .mount(&server)
            .await;
        let txn = Mutation::new().build();
        let id = client
            .schedule(&txn, crate::wire::ScheduleWhen::AfterMs { ms: 5000 })
            .await
            .unwrap();
        assert_eq!(id, "job-7");
    }

    #[tokio::test]
    async fn schedule_manage_ops_post_their_path_and_db_body() {
        let (server, client) = setup().await;
        for op in ["cancel", "pause", "resume"] {
            Mock::given(method("POST"))
                .and(path(format!("/api/schedule/job-1/{op}")))
                .and(body_partial_json(json!({"db": "t<uuid>"})))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
                .mount(&server)
                .await;
        }
        client.cancel_schedule("job-1").await.unwrap();
        client.pause_schedule("job-1").await.unwrap();
        client.resume_schedule("job-1").await.unwrap();
    }

    #[test]
    fn encode_uri_component_matches_browser() {
        // The encodeURIComponent unescaped set: A-Za-z0-9 - _ . ! ~ * ' ( )
        assert_eq!(encode_uri_component("aB3-_.!~*'()"), "aB3-_.!~*'()");
        // Reserved chars get uppercase-hex percent-encoding.
        assert_eq!(encode_uri_component("a/b"), "a%2Fb");
        assert_eq!(encode_uri_component("a b"), "a%20b");
        assert_eq!(encode_uri_component("a+b"), "a%2Bb");
        // Multi-byte UTF-8 percent-encodes each byte (é = C3 A9), matching the browser.
        assert_eq!(encode_uri_component("é"), "%C3%A9");
        // Real schedule ids (uuid v7) are untouched.
        assert_eq!(
            encode_uri_component("019f9111-acf2-7803-9074-fc50505582d4"),
            "019f9111-acf2-7803-9074-fc50505582d4"
        );
    }

    #[tokio::test]
    async fn schedule_manage_encodes_the_id_path_segment() {
        let (server, client) = setup().await;
        // An id with a reserved path char (`/`) and a space must be
        // percent-encoded like encodeURIComponent, matching the TS client:
        // `a b/c` -> `a%20b%2Fc`.
        Mock::given(method("POST"))
            .and(path("/api/schedule/a%20b%2Fc/cancel"))
            .and(body_partial_json(json!({"db": "t<uuid>"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
            .mount(&server)
            .await;
        client.cancel_schedule("a b/c").await.unwrap();
    }

    #[tokio::test]
    async fn list_schedules_returns_schedule_info_vec() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/api/schedules"))
            .and(header("authorization", "Bearer machine-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "schedules": [{
                    "id": "job-1",
                    "kind": "cron",
                    "dueAt": 9000,
                    "cron": "*/5 * * * *",
                    "status": "pending",
                    "createdAt": 1000,
                    "firedCount": 0
                }]
            })))
            .mount(&server)
            .await;
        let list = client.list_schedules().await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].id, "job-1");
        assert_eq!(list[0].cron.as_deref(), Some("*/5 * * * *"));
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

    #[tokio::test]
    async fn validate_session_token_returns_user_and_github_identity() {
        let (server, client) = setup().await;
        // The validated token is the argument, not the client's own token —
        // the mock asserts the request carries exactly the player's bearer.
        Mock::given(method("GET"))
            .and(path("/auth/validate"))
            .and(header("authorization", "Bearer player-session-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "user": {
                    "kind": "user",
                    "email": "player@example.com",
                    "name": null,
                    "githubLogin": "player",
                    "githubId": 42
                }
            })))
            .mount(&server)
            .await;
        let user = client
            .validate_session_token("player-session-token")
            .await
            .unwrap();
        assert_eq!(user.kind, "user");
        assert_eq!(user.email.as_deref(), Some("player@example.com"));
        assert_eq!(user.github_login.as_deref(), Some("player"));
        assert_eq!(user.github_id, Some(42));
    }

    #[tokio::test]
    async fn validate_session_token_surfaces_auth_error_for_invalid_token() {
        let (server, client) = setup().await;
        Mock::given(method("GET"))
            .and(path("/auth/validate"))
            .respond_with(ResponseTemplate::new(401).set_body_json(json!({
                "code": "UNAUTHORIZED",
                "message": "invalid token"
            })))
            .mount(&server)
            .await;
        let err = client
            .validate_session_token("not-a-real-token")
            .await
            .unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::Unauthorized);
    }

    #[tokio::test]
    async fn validate_session_token_tolerates_response_without_github_fields() {
        let (server, client) = setup().await;
        // An older server omitting githubLogin/githubId must still parse,
        // defaulting both to None (backward-compatible additive fields).
        Mock::given(method("GET"))
            .and(path("/auth/validate"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "user": {"kind": "machine", "email": null, "name": null}
            })))
            .mount(&server)
            .await;
        let user = client.validate_session_token("mach-tok").await.unwrap();
        assert_eq!(user.kind, "machine");
        assert_eq!(user.github_login, None);
        assert_eq!(user.github_id, None);
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

    // `find_one_by_index` and `upsert_by_index` are thin wrappers over the
    // query/mutate paths, so these tests assert both the on-the-wire request
    // (the `first` terminal / the one-step upsert) and the parsed result.

    #[tokio::test]
    async fn find_one_by_index_hit_returns_doc() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/api/query"))
            .and(body_partial_json(json!({
                "query": {
                    "table": "users",
                    "index": "by_email",
                    "eq": ["a@b.com"],
                    "first": true
                }
            })))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"result": {"_id": "u1", "email": "a@b.com", "n": 1}})),
            )
            .mount(&server)
            .await;
        let got: Option<Value> = client
            .find_one_by_index("users", "by_email", "a@b.com")
            .await
            .unwrap();
        let doc = got.expect("expected a matching doc");
        assert_eq!(doc.get("email").and_then(Value::as_str), Some("a@b.com"));
    }

    #[tokio::test]
    async fn find_one_by_index_miss_returns_none() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/api/query"))
            .and(body_partial_json(json!({
                "query": {
                    "table": "users",
                    "index": "by_email",
                    "eq": ["none@x.com"],
                    "first": true
                }
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result": null})))
            .mount(&server)
            .await;
        let got: Option<Value> = client
            .find_one_by_index("users", "by_email", "none@x.com")
            .await
            .unwrap();
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn upsert_by_index_inserts_when_no_match() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/api/mutate"))
            .and(body_partial_json(json!({
                "txn": {
                    "steps": [{
                        "op": "upsert",
                        "table": "users",
                        "index": "by_email",
                        "eq": ["a@b.com"],
                        "insert": {"email": "a@b.com"},
                        "patch": {"n": 1}
                    }]
                }
            })))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"results": [{"id": "new1", "inserted": true}]})),
            )
            .mount(&server)
            .await;
        let res = client
            .upsert_by_index(
                "users",
                "by_email",
                "a@b.com",
                json!({"email": "a@b.com"}),
                json!({"n": 1}),
            )
            .await
            .unwrap();
        assert!(matches!(
            res,
            crate::mutation::StepResult::Upsert { ref id, inserted: true } if id == "new1"
        ));
    }

    #[tokio::test]
    async fn upsert_by_index_patches_when_match() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/api/mutate"))
            .and(body_partial_json(json!({
                "txn": {
                    "steps": [{
                        "op": "upsert",
                        "table": "users",
                        "index": "by_email",
                        "eq": ["a@b.com"],
                        "insert": {"email": "a@b.com"},
                        "patch": {"n": 2}
                    }]
                }
            })))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"results": [{"id": "u1", "inserted": false}]})),
            )
            .mount(&server)
            .await;
        let res = client
            .upsert_by_index(
                "users",
                "by_email",
                "a@b.com",
                json!({"email": "a@b.com"}),
                json!({"n": 2}),
            )
            .await
            .unwrap();
        assert!(matches!(
            res,
            crate::mutation::StepResult::Upsert { ref id, inserted: false } if id == "u1"
        ));
    }

    // Storage surface (`POST /api/storage/{db}` raw-body upload, `DELETE`, `GET
    // .../metadata`, and the no-request `get_url`). `body_bytes` (a function, not
    // a macro, in wiremock 0.6) matches the exact raw request bytes.
    //
    // Unlike the schedule routes (where `db` rides in the JSON body), the storage
    // routes put `db` in the URL path. The `setup()` fixture uses `db = "t<uuid>"`
    // as a recognizable placeholder, and reqwest parses the URL via the `url`
    // crate — which percent-encodes `<` and `>` (members of the URL path encode
    // set) — so the actual path the server receives is `/api/storage/t%3Cuuid%3E`.
    // Production db names never contain `<>`, so the matchers use the encoded
    // form to reflect what the client really sends.

    #[tokio::test]
    async fn upload_posts_raw_bytes_and_returns_metadata() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/api/storage/t%3Cuuid%3E"))
            .and(header("authorization", "Bearer machine-token"))
            .and(header("content-type", "image/png"))
            .and(body_bytes("raw-bytes"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "f1", "sha256": "abc", "size": 9, "contentType": "image/png"
            })))
            .mount(&server)
            .await;
        let up = client
            .upload(b"raw-bytes", Some("image/png"))
            .await
            .unwrap();
        assert_eq!(up.id, "f1");
        assert_eq!(up.size, 9);
        assert_eq!(up.content_type.as_deref(), Some("image/png"));
    }

    #[tokio::test]
    async fn delete_file_and_metadata_and_get_url() {
        let (server, client) = setup().await;
        Mock::given(method("DELETE"))
            .and(path("/api/storage/t%3Cuuid%3E/f1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/storage/t%3Cuuid%3E/f1/metadata"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "f1", "sha256": "abc", "size": 9, "creationTime": 5
            })))
            .mount(&server)
            .await;
        client.delete_file("f1").await.unwrap();
        let meta = client.get_file_metadata("f1").await.unwrap();
        assert_eq!(meta.size, 9);
        assert_eq!(meta.creation_time, 5);
        assert_eq!(meta.content_type, None);
        // `mod tests` is a child of the `http` module, so it can read the private
        // `url` field of `RtDbHttpClient`.
        assert_eq!(client.get_url("f1"), format!("{}/storage/f1", client.url));
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
