//! One-shot HTTP client for par-rt-db. `Authorization: Bearer <token>` on every call.

use crate::error::{ErrorEnvelope, RtDbError, retry_on_precondition};
use crate::mutation::{Mutation, StepResult, Transaction};
use crate::query::{Query, TableQuery, parse_result};
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

/// Image-transform `fit` mode for [`RtDbHttpClient::transform_url`] (ENH-014).
/// The serde rename is `kebab-case` so `ScaleDown` serializes as `scale-down`
/// on the wire; `as_str` mirrors that for the hand-built query string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Fit {
    #[default]
    Contain,
    Cover,
    ScaleDown,
}

impl Fit {
    fn as_str(self) -> &'static str {
        match self {
            Fit::Cover => "cover",
            Fit::Contain => "contain",
            Fit::ScaleDown => "scale-down",
        }
    }
}

/// Output format for [`RtDbHttpClient::transform_url`] (ENH-014).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutFormat {
    #[default]
    Auto,
    Jpeg,
    Png,
}

/// Optional image-transform parameters for [`RtDbHttpClient::transform_url`]
/// (ENH-014). All fields are optional; only `Some` fields appear in the query.
#[derive(Debug, Clone, Default)]
pub struct TransformOpts {
    pub w: Option<u32>,
    pub h: Option<u32>,
    pub fit: Option<Fit>,
    pub q: Option<u8>,
    pub format: Option<OutFormat>,
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

    /// Run a batch of independent queries against this client's db in one round
    /// trip (`POST /api/query-batch`). Auth and owner resolution run once for
    /// the whole request (same db, same principal); each query's outcome lands
    /// in its own aligned slot. A per-query execution error becomes that slot's
    /// `{ok:false,error}` (standard envelope) and never fails the call — only a
    /// db-level auth failure, an empty `queries` array, or a transport error
    /// surfaces as [`RtDbError`]. The returned vector is length-aligned with the
    /// input order; the caller narrows each [`BatchQueryOutcome::result`](crate::wire::BatchQueryOutcome) based
    /// on the terminal it used for that query (the raw value is `serde_json::Value`
    /// because a batch spans terminals, unlike the per-call `T` of [`run`]).
    ///
    /// [`run`]: Self::run
    pub async fn batch_query(
        &self,
        queries: &[Query],
    ) -> Result<Vec<crate::wire::BatchQueryOutcome>, RtDbError> {
        #[derive(Serialize)]
        struct Body<'a> {
            db: &'a str,
            queries: &'a [Query],
        }
        let body = Body {
            db: &self.db,
            queries,
        };
        let resp = self
            .client
            .post(format!("{}/api/query-batch", self.url))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("batch query request failed: {e}")))?;
        #[derive(serde::Deserialize)]
        struct BatchResponse {
            results: Vec<crate::wire::BatchQueryOutcome>,
        }
        let parsed = self.deserialize::<BatchResponse>(resp).await?;
        Ok(parsed.results)
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

    /// The public serve URL for `id` with image-transform query params appended
    /// (ENH-014). Params are emitted in fixed order `w, h, fit, q, format` and
    /// only when `Some`. No request is made.
    pub fn transform_url(&self, id: &str, opts: &TransformOpts) -> String {
        let base = format!("{}/storage/{id}", self.url);
        let mut parts: Vec<String> = Vec::new();
        if let Some(w) = opts.w {
            parts.push(format!("w={w}"));
        }
        if let Some(h) = opts.h {
            parts.push(format!("h={h}"));
        }
        if let Some(fit) = opts.fit {
            parts.push(format!("fit={}", fit.as_str()));
        }
        if let Some(q) = opts.q {
            parts.push(format!("q={q}"));
        }
        if let Some(f) = opts.format {
            // `Auto` is the server default; omit it so the URL stays minimal.
            let s = match f {
                OutFormat::Auto => "",
                OutFormat::Jpeg => "jpeg",
                OutFormat::Png => "png",
            };
            if !s.is_empty() {
                parts.push(format!("format={s}"));
            }
        }
        if parts.is_empty() {
            base
        } else {
            format!("{base}?{}", parts.join("&"))
        }
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

    /// `POST /admin/delete-db` `{name, confirm}` → `{ok:true}`. The server
    /// rejects with `BAD_REQUEST` unless `confirm == name` exactly — the typed
    /// confirmation guard against accidental deletion. Drops the db's Postgres
    /// schema (CASCADE) and every per-db row (registry, tokens, allowlist,
    /// storage_index).
    pub async fn delete_db(&self, name: &str, confirm: &str) -> Result<(), RtDbError> {
        let resp = self
            .post_json(
                "/admin/delete-db",
                &crate::wire::admin::DeleteDbRequest { name, confirm },
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

    /// `POST /admin/db/{db}/migrate` `{directives, dryRun}` → `MigrateResult`.
    /// Apply (when `dry_run` is false) or preview (when `dry_run` is true) a
    /// declarative schema migration. The server validates and folds the
    /// directives transactionally; on `dry_run` nothing is committed and the
    /// returned `schema` is the derived preview. Mirrors `ts-client`'s
    /// `RtDbAdminClient.migrate` one-to-one.
    pub async fn migrate_schema(
        &self,
        db: &str,
        directives: &[crate::wire::admin::Directive],
        dry_run: bool,
    ) -> Result<crate::wire::admin::MigrateResult, RtDbError> {
        let resp = self
            .post_json(
                &format!("/admin/db/{}/migrate", db),
                &crate::wire::admin::MigrateRequest {
                    directives,
                    dry_run,
                },
            )
            .await?;
        self.deserialize::<crate::wire::admin::MigrateResult>(resp)
            .await
    }

    /// `GET /admin/dbs` → `{databases:[...]}`.
    pub async fn list_dbs(&self) -> Result<Vec<String>, RtDbError> {
        let parsed: crate::wire::admin::DatabasesResponse =
            self.get_json("/admin/dbs", &[]).await?;
        Ok(parsed.databases)
    }

    /// `POST /admin/mint-token` `{db, name}` → `{tokenId, token}`.
    ///
    /// Mints a full-access token (no expiry, read-write, all tables) — the
    /// server defaults. For scoped capabilities use
    /// [`mint_token_with_options`](Self::mint_token_with_options).
    pub async fn mint_token(
        &self,
        db: &str,
        name: &str,
    ) -> Result<crate::wire::admin::MintedToken, RtDbError> {
        self.mint_token_with_options(db, name, &crate::wire::admin::MintTokenOptions::default())
            .await
    }

    /// `POST /admin/mint-token` `{db, name, expiresAt?, readOnly?, tables?}` →
    /// `{tokenId, token}`. Fields left `None` on [`MintTokenOptions`] are omitted
    /// from the body so the server applies its defaults (no expiry, read-write,
    /// all tables).
    pub async fn mint_token_with_options(
        &self,
        db: &str,
        name: &str,
        opts: &crate::wire::admin::MintTokenOptions,
    ) -> Result<crate::wire::admin::MintedToken, RtDbError> {
        let resp = self
            .post_json(
                "/admin/mint-token",
                &crate::wire::admin::MintTokenRequest {
                    db,
                    name,
                    expires_at: opts.expires_at,
                    read_only: opts.read_only,
                    tables: opts.tables.as_deref(),
                },
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

    /// `GET /admin/admins` → `{admins:[{email, githubId?}]}`.
    pub async fn admins_list(&self) -> Result<Vec<crate::wire::admin::AdminMember>, RtDbError> {
        #[derive(serde::Deserialize)]
        struct Resp {
            admins: Vec<crate::wire::admin::AdminMember>,
        }
        Ok(self.get_json::<Resp>("/admin/admins", &[]).await?.admins)
    }

    /// `POST /admin/admins` `{email, githubId?}` → `{ok:true}`.
    pub async fn admins_add(&self, email: &str, github_id: Option<i64>) -> Result<(), RtDbError> {
        #[derive(serde::Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body {
            email: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            github_id: Option<i64>,
        }
        let resp = self
            .post_json(
                "/admin/admins",
                &Body {
                    email: email.to_string(),
                    github_id,
                },
            )
            .await?;
        self.expect_ok(resp).await
    }

    /// `DELETE /admin/admins` `{email}` → `{ok:true}`.
    pub async fn admins_remove(&self, email: &str) -> Result<(), RtDbError> {
        #[derive(serde::Serialize)]
        struct Body {
            email: String,
        }
        let resp = self
            .delete_json(
                "/admin/admins",
                &Body {
                    email: email.to_string(),
                },
            )
            .await?;
        self.expect_ok(resp).await
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

    /// `GET /admin/dbs/{db}/schema` → the database's pushed `SchemaDef`.
    pub async fn get_schema(&self, db: &str) -> Result<crate::schema::SchemaDef, RtDbError> {
        self.get_json(&format!("/admin/dbs/{db}/schema"), &[]).await
    }

    /// `GET /admin/db/{db}/schema/history?limit=&offset=` → newest-first list of
    /// captured schema snapshots (summaries, no `schema` blob). Mirrors server
    /// `schema_history::list`. `limit`/`offset` are optional paging params
    /// (server defaults: limit 100 clamped to 1000, offset 0).
    pub async fn schema_history(
        &self,
        db: &str,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<Vec<crate::wire::admin::SchemaHistorySummary>, RtDbError> {
        #[derive(serde::Deserialize)]
        struct Resp {
            entries: Vec<crate::wire::admin::SchemaHistorySummary>,
        }
        let mut params: Vec<(&str, String)> = Vec::new();
        if let Some(l) = limit {
            params.push(("limit", l.to_string()));
        }
        if let Some(o) = offset {
            params.push(("offset", o.to_string()));
        }
        let q: Vec<(&str, &str)> = params.iter().map(|(k, v)| (*k, v.as_str())).collect();
        Ok(self
            .get_json::<Resp>(&format!("/admin/db/{db}/schema/history"), &q)
            .await?
            .entries)
    }

    /// `GET /admin/db/{db}/schema/history/{version}` → one full snapshot,
    /// including the `schema` blob. `not_found` if the database or version does
    /// not exist. Mirrors server `schema_history::get`.
    pub async fn schema_history_get(
        &self,
        db: &str,
        version: i64,
    ) -> Result<crate::wire::admin::SchemaHistoryEntry, RtDbError> {
        self.get_json(&format!("/admin/db/{db}/schema/history/{version}"), &[])
            .await
    }

    /// `POST /admin/db/{db}/schema/restore` `{version, confirm}` → restore the
    /// live schema shape to a prior captured snapshot; returns the restored
    /// version. `confirm` must equal the db name (typed guard, mirrors
    /// delete-db). Mirrors server `admin::restore_schema`; the redundant `ok`
    /// flag collapses into the `Result` (errors surface as `RtDbError`), the
    /// same way `delete_db`/`push_schema` collapse `{ok:true}` bodies.
    pub async fn restore_schema(
        &self,
        db: &str,
        version: i64,
        confirm: &str,
    ) -> Result<i64, RtDbError> {
        #[derive(Serialize)]
        struct Body<'a> {
            version: i64,
            confirm: &'a str,
        }
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Resp {
            restored_to: i64,
        }
        let resp = self
            .post_json(
                &format!("/admin/db/{db}/schema/restore"),
                &Body { version, confirm },
            )
            .await?;
        Ok(self.deserialize::<Resp>(resp).await?.restored_to)
    }

    /// `GET /admin/dbs/{db}/stats` → per-table row counts + sizes.
    pub async fn db_stats(&self, db: &str) -> Result<crate::wire::admin::DbStats, RtDbError> {
        self.get_json(&format!("/admin/dbs/{db}/stats"), &[]).await
    }

    /// `GET /admin/tokens?db=<db>` → machine tokens minted for this database.
    pub async fn list_tokens(
        &self,
        db: &str,
    ) -> Result<Vec<crate::wire::admin::TokenInfo>, RtDbError> {
        #[derive(serde::Deserialize)]
        struct Resp {
            tokens: Vec<crate::wire::admin::TokenInfo>,
        }
        Ok(self
            .get_json::<Resp>("/admin/tokens", &[("db", db)])
            .await?
            .tokens)
    }

    /// `GET /admin/metrics` → server-wide counters and gauges.
    pub async fn metrics(&self) -> Result<crate::wire::admin::MetricsSnapshot, RtDbError> {
        self.get_json("/admin/metrics", &[]).await
    }

    /// `GET /admin/subscriptions?db=<optional>` → live subscription inspector
    /// (ENH-010): every active subscription's db/table/terminal/read-set
    /// class/principal, plus invalidation-effectiveness counters (re-runs vs.
    /// proven skips by class, sampled missed pushes) both server-wide and
    /// per-db. Pass `Some(db)` to scope to one database; `None` for every
    /// database on the instance.
    pub async fn list_subscriptions(
        &self,
        db: Option<&str>,
    ) -> Result<crate::wire::admin::SubscriptionsResponse, RtDbError> {
        let params: &[(&str, &str)] = match db {
            Some(d) => &[("db", d)],
            None => &[],
        };
        self.get_json("/admin/subscriptions", params).await
    }

    /// `GET /admin/config` → redacted running config + build identity + admins.
    pub async fn get_config(&self) -> Result<crate::wire::admin::ConfigResponse, RtDbError> {
        self.get_json("/admin/config", &[]).await
    }

    /// `PATCH /admin/config` with a partial hot-config body → updated config.
    pub async fn patch_config(
        &self,
        patch: &crate::wire::admin::HotConfigPatch,
    ) -> Result<crate::wire::admin::ConfigResponse, RtDbError> {
        self.deserialize(self.patch_json("/admin/config", patch).await?)
            .await
    }

    /// `GET /admin/ops/recent?db=<db>&table=<t>&n=<n>` → recent document-op
    /// events from the in-memory ring, newest-first. `table` and `n` optional.
    pub async fn ops_recent(
        &self,
        db: &str,
        table: Option<&str>,
        n: Option<u32>,
    ) -> Result<Vec<crate::wire::admin::OpEvent>, RtDbError> {
        #[derive(serde::Deserialize)]
        struct Resp {
            ops: Vec<crate::wire::admin::OpEvent>,
        }
        let db_s = db.to_string();
        let table_s = table.map(|t| t.to_string());
        let n_s = n.map(|n| n.to_string());
        let mut q: Vec<(&str, &str)> = Vec::with_capacity(3);
        q.push(("db", db_s.as_str()));
        if let Some(ref t) = table_s {
            q.push(("table", t.as_str()));
        }
        if let Some(ref n_str) = n_s {
            q.push(("n", n_str.as_str()));
        }
        Ok(self.get_json::<Resp>("/admin/ops/recent", &q).await?.ops)
    }

    /// `POST /admin/db/{db}/query` `{query}` → `{result}`. Owner-bypass: an
    /// admin reads documents across every database regardless of `ownerField`.
    /// Mirrors [`run`](Self::run) but routes through the admin path with `db`
    /// in the URL (singular `db`, not the plural `dbs` of `get_schema`), so the
    /// body omits `db`. Deserialize `{result}` into `T` the same way `run` does.
    pub async fn admin_query<T: DeserializeOwned>(
        &self,
        db: &str,
        query: &crate::query::Query,
    ) -> Result<T, RtDbError> {
        #[derive(Serialize)]
        struct Body<'a> {
            query: &'a crate::query::Query,
        }
        let resp = self
            .post_json(&format!("/admin/db/{}/query", db), &Body { query })
            .await?;
        self.json_result::<T>(resp).await
    }

    /// `POST /admin/db/{db}/mutate` `{txn, idempotencyKey?}` → `{results}`.
    /// Owner-bypass: an admin writes documents across every database regardless
    /// of `ownerField`. Mirrors [`mutate`](Self::mutate) but routes through the
    /// admin path with `db` in the URL, so the body omits `db`. Returns one
    /// [`StepResult`] per step.
    pub async fn admin_mutate(
        &self,
        db: &str,
        txn: &Transaction,
        idempotency_key: Option<&str>,
    ) -> Result<Vec<StepResult>, RtDbError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body<'a> {
            txn: &'a Transaction,
            #[serde(skip_serializing_if = "Option::is_none")]
            idempotency_key: Option<&'a str>,
        }
        let resp = self
            .post_json(
                &format!("/admin/db/{}/mutate", db),
                &Body {
                    txn,
                    idempotency_key,
                },
            )
            .await?;
        #[derive(serde::Deserialize)]
        struct Resp {
            results: Vec<serde_json::Value>,
        }
        let parsed = self.deserialize::<Resp>(resp).await?;
        parsed
            .results
            .into_iter()
            .map(|v| {
                serde_json::from_value::<StepResult>(v)
                    .map_err(|e| RtDbError::internal(format!("invalid step result: {e}")))
            })
            .collect()
    }

    /// `POST /admin/backup` (empty body) → 202 `{ok:true}`. Triggers one
    /// `pg_dump` immediately; the dump runs detached and the in-progress flag
    /// is observable via [`list_backups`](Self::list_backups). A second call
    /// while one is running → 409 `CONFLICT`. Runs outside the committer.
    pub async fn backup_now(&self) -> Result<(), RtDbError> {
        let resp = self
            .post_json("/admin/backup", &serde_json::json!({}))
            .await?;
        self.expect_ok(resp).await
    }

    /// `GET /admin/backups` → `{running, backups:[{name, sizeBytes, createdMs}]}`.
    /// A missing backup dir returns an empty list (the endpoint describes what
    /// is on disk, not what is configured).
    pub async fn list_backups(&self) -> Result<crate::wire::admin::BackupsListResponse, RtDbError> {
        self.get_json("/admin/backups", &[]).await
    }

    /// `GET /admin/backups/{name}` → the raw dump bytes
    /// (`application/octet-stream`). The response is NOT JSON-decoded — binary
    /// pg_dump output is returned verbatim as `Vec<u8>`.
    pub async fn download_backup(&self, name: &str) -> Result<Vec<u8>, RtDbError> {
        let resp = self
            .client
            .get(format!("{}/admin/backups/{name}", self.url))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("download_backup request failed: {e}")))?;
        let status = resp.status();
        if status.is_success() {
            return resp
                .bytes()
                .await
                .map(|b| b.to_vec())
                .map_err(|e| RtDbError::internal(format!("invalid backup body: {e}")));
        }
        Err(self.error_response(resp).await)
    }

    /// `DELETE /admin/backups/{name}` → 204. Returns 404 if the file is
    /// already gone. Same `validate_dump_name` short-circuit as download runs
    /// server-side first.
    pub async fn delete_backup(&self, name: &str) -> Result<(), RtDbError> {
        let resp = self
            .client
            .delete(format!("{}/admin/backups/{name}", self.url))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("delete_backup request failed: {e}")))?;
        let status = resp.status();
        if status.is_success() {
            // 204 No Content (or any other 2xx the server returns) — nothing
            // to parse.
            return Ok(());
        }
        Err(self.error_response(resp).await)
    }

    /// `POST /admin/restore` `{name, confirm}` → `{target, instructions}`.
    /// The SDK sends `confirm == name` (the typed confirmation guard mirrors
    /// [`delete_db`](Self::delete_db)). Restores into a fresh
    /// `rtdb_restored_<stamp>` DB; the live DB is never touched.
    pub async fn restore_backup(
        &self,
        name: &str,
    ) -> Result<crate::wire::admin::RestoreResult, RtDbError> {
        let resp = self
            .post_json(
                "/admin/restore",
                &crate::wire::admin::RestoreRequest {
                    name,
                    confirm: name,
                },
            )
            .await?;
        self.deserialize::<crate::wire::admin::RestoreResult>(resp)
            .await
    }

    // ── Webhook management (GET/POST/PUT/DELETE /admin/db/{db}/webhooks[...]) ──
    //
    // Mirror `ts-client`'s `listWebhooks`/`createWebhook`/`editWebhook`/
    // `deleteWebhook`/`listDeliveries` one-to-one — paths, bodies, and return
    // shapes are identical; only the method names are snake_cased. Each call
    // carries the admin-key bearer like every other admin method.

    /// `GET /admin/db/{db}/webhooks` → `{webhooks:[...]}`. Returns an empty
    /// list when webhooks are disabled at boot (the table may not exist).
    pub async fn list_webhooks(
        &self,
        db: &str,
    ) -> Result<Vec<crate::wire::admin::Webhook>, RtDbError> {
        let parsed: crate::wire::admin::WebhooksResponse = self
            .get_json(&format!("/admin/db/{db}/webhooks"), &[])
            .await?;
        Ok(parsed.webhooks)
    }

    /// `POST /admin/db/{db}/webhooks` `{url, table?, events?, enabled?}` →
    /// `{id}`. Only the provided option keys are sent; the server defaults
    /// `table` to all-tables, `events` to `["*"]`, and `enabled` to `true` when
    /// their keys are absent. Returns the new webhook's server-assigned id.
    pub async fn create_webhook(
        &self,
        db: &str,
        opts: &crate::wire::admin::CreateWebhookOptions,
    ) -> Result<i64, RtDbError> {
        let resp = self
            .post_json(&format!("/admin/db/{db}/webhooks"), opts)
            .await?;
        let parsed: crate::wire::admin::CreateWebhookResponse = self.deserialize(resp).await?;
        Ok(parsed.id)
    }

    /// `PUT /admin/db/{db}/webhooks/{id}` `{url?, table?, events?, enabled?}` →
    /// the updated [`Webhook`](crate::wire::admin::Webhook). Each present field
    /// overwrites the stored value; absent fields are unchanged. The `table`
    /// field is a tri-state on the wire: omitted (`opts.table = None`) leaves
    /// the filter alone, JSON `null` (`opts.table = Some(None)`) clears it to
    /// all-tables, and a string (`opts.table = Some(Some("x"))`) sets it.
    pub async fn edit_webhook(
        &self,
        db: &str,
        id: i64,
        opts: &crate::wire::admin::WebhookEditOptions,
    ) -> Result<crate::wire::admin::Webhook, RtDbError> {
        let resp = self
            .put_json(&format!("/admin/db/{db}/webhooks/{id}"), opts)
            .await?;
        self.deserialize::<crate::wire::admin::Webhook>(resp).await
    }

    /// `DELETE /admin/db/{db}/webhooks/{id}` → `{ok:true}`. Cascades the
    /// webhook's pending deliveries via the foreign key.
    pub async fn delete_webhook(&self, db: &str, id: i64) -> Result<(), RtDbError> {
        let resp = self
            .client
            .delete(format!("{}/admin/db/{db}/webhooks/{id}", self.url))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("delete_webhook request failed: {e}")))?;
        self.expect_ok(resp).await
    }

    /// `GET /admin/db/{db}/webhooks/{id}/deliveries?status=&limit=&offset=` →
    /// `{deliveries:[...]}`, newest `next_attempt` first. `opts` may be `None`
    /// for the server-default first page (limit=50, no status filter, offset=0).
    pub async fn list_deliveries(
        &self,
        db: &str,
        id: i64,
        opts: Option<&crate::wire::admin::ListDeliveriesOptions>,
    ) -> Result<Vec<crate::wire::admin::WebhookDelivery>, RtDbError> {
        // Borrowed query-string assembly mirrors `ops_recent`: own the strings
        // on this stack, then hand `get_json` a slice of `(&str, &str)` refs.
        let status_s = opts.and_then(|o| o.status.clone());
        let limit_s = opts.and_then(|o| o.limit).map(|n| n.to_string());
        let offset_s = opts.and_then(|o| o.offset).map(|n| n.to_string());
        let mut q: Vec<(&str, &str)> = Vec::with_capacity(3);
        if let Some(ref s) = status_s {
            q.push(("status", s.as_str()));
        }
        if let Some(ref n) = limit_s {
            q.push(("limit", n.as_str()));
        }
        if let Some(ref n) = offset_s {
            q.push(("offset", n.as_str()));
        }
        let parsed: crate::wire::admin::DeliveriesResponse = self
            .get_json(&format!("/admin/db/{db}/webhooks/{id}/deliveries"), &q)
            .await?;
        Ok(parsed.deliveries)
    }

    /// `GET /admin/audit?db=&table=&op=&principal=&source=&limit=&offset=` →
    /// `{entries:[...]}`, newest `ts_ms` first. `db` is always sent; every other
    /// filter is omitted from the query when `None` (matches all rows).
    /// `opts = None` sends just `db` (server defaults: limit=100, offset=0, no
    /// table/op/principal/source filter). When audit is disabled at boot the
    /// server short-circuits to an empty list. Mirrors `list_deliveries`'
    /// borrowed query-string assembly: own the strings on this stack, then hand
    /// `get_json` a slice of `(&str, &str)` refs.
    pub async fn get_audit(
        &self,
        db: &str,
        opts: Option<&crate::wire::admin::AuditQuery>,
    ) -> Result<Vec<crate::wire::admin::AuditEntry>, RtDbError> {
        let table_s = opts.and_then(|o| o.table.clone());
        let op_s = opts.and_then(|o| o.op.clone());
        let principal_s = opts.and_then(|o| o.principal.clone());
        let source_s = opts.and_then(|o| o.source.clone());
        let limit_s = opts.and_then(|o| o.limit).map(|n| n.to_string());
        let offset_s = opts.and_then(|o| o.offset).map(|n| n.to_string());
        let mut q: Vec<(&str, &str)> = Vec::with_capacity(7);
        q.push(("db", db));
        if let Some(ref v) = table_s {
            q.push(("table", v.as_str()));
        }
        if let Some(ref v) = op_s {
            q.push(("op", v.as_str()));
        }
        if let Some(ref v) = principal_s {
            q.push(("principal", v.as_str()));
        }
        if let Some(ref v) = source_s {
            q.push(("source", v.as_str()));
        }
        if let Some(ref v) = limit_s {
            q.push(("limit", v.as_str()));
        }
        if let Some(ref v) = offset_s {
            q.push(("offset", v.as_str()));
        }
        let parsed: crate::wire::admin::AuditResponse = self.get_json("/admin/audit", &q).await?;
        Ok(parsed.entries)
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

    // PUT helper for `edit_webhook` (the one admin method that PUTs rather than
    // POSTs/PATCHes). Same shape as `post_json`/`patch_json`.
    async fn put_json<Req: Serialize>(
        &self,
        path: &str,
        body: &Req,
    ) -> Result<reqwest::Response, RtDbError> {
        self.client
            .put(format!("{}{}", self.url, path))
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("admin request failed: {e}")))
    }

    async fn delete_json<Req: Serialize>(
        &self,
        path: &str,
        body: &Req,
    ) -> Result<reqwest::Response, RtDbError> {
        self.client
            .delete(format!("{}{}", self.url, path))
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("admin request failed: {e}")))
    }

    // PATCH helper for the admin sweep; consumed by `patch_config`.
    async fn patch_json<Req: Serialize>(
        &self,
        path: &str,
        body: &Req,
    ) -> Result<reqwest::Response, RtDbError> {
        self.client
            .patch(format!("{}{}", self.url, path))
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
    use crate::wire::UserKind;
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
    async fn batch_query_posts_queries_and_returns_aligned_outcomes() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/api/query-batch"))
            .and(header("authorization", "Bearer machine-token"))
            .and(body_partial_json(json!({
                "db": "t<uuid>",
                "queries": [{"table": "items"}]
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "results": [
                    {"ok": true, "result": [{"_id": "a"}]},
                    {"ok": false, "error": {"code": "NOT_FOUND", "message": "no such table"}}
                ]
            })))
            .mount(&server)
            .await;
        let q = TableQuery::new("items").take(5);
        let got = client.batch_query(&[q]).await.expect("batch query");
        assert_eq!(got.len(), 2);
        assert!(got[0].ok);
        assert_eq!(
            got[0]
                .result
                .as_ref()
                .and_then(serde_json::Value::as_array)
                .map(Vec::len),
            Some(1)
        );
        assert!(got[0].error.is_none());
        assert!(!got[1].ok);
        let err = got[1].error.as_ref().expect("error envelope");
        assert_eq!(err.code, crate::error::ErrorCode::NotFound);
        assert_eq!(err.message, "no such table");
        assert!(got[1].result.is_none());
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
        assert_eq!(user.kind, UserKind::User);
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
        assert_eq!(user.kind, UserKind::User);
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
        assert_eq!(user.kind, UserKind::Machine);
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

    #[test]
    fn transform_url_appends_query_params() {
        let client = RtDbHttpClient::new("https://rtdb.example", "db", "tok");
        let url = client.transform_url(
            "f1",
            &TransformOpts {
                w: Some(100),
                h: Some(50),
                fit: Some(Fit::Cover),
                q: Some(80),
                format: Some(OutFormat::Auto),
            },
        );
        assert_eq!(
            url,
            "https://rtdb.example/storage/f1?w=100&h=50&fit=cover&q=80"
        );
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
    use crate::mutation::Mutation;
    use crate::query::TableQuery;
    use crate::schema::{FieldType, SchemaDef, Table};
    use serde_json::{Value, json};
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
    async fn delete_db_posts_name_and_confirm() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/admin/delete-db"))
            .and(header("authorization", BEARER))
            .and(body_partial_json(
                json!({"name": "kanban", "confirm": "kanban"}),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
            .mount(&server)
            .await;
        client.delete_db("kanban", "kanban").await.unwrap();
    }

    #[tokio::test]
    async fn delete_db_surfaces_confirmation_mismatch_envelope() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/admin/delete-db"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "code": "BAD_REQUEST",
                "message": "confirmation does not match database name"
            })))
            .mount(&server)
            .await;
        let err = client.delete_db("kanban", "wrong").await.unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert_eq!(err.message, "confirmation does not match database name");
    }

    #[tokio::test]
    async fn delete_db_surfaces_unknown_database_envelope() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/admin/delete-db"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({
                "code": "NOT_FOUND",
                "message": "database not found"
            })))
            .mount(&server)
            .await;
        let err = client.delete_db("missing", "missing").await.unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
        assert_eq!(err.message, "database not found");
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
    async fn admins_list_unwraps_admins_array() {
        let (server, client) = setup().await;
        Mock::given(method("GET"))
            .and(path("/admin/admins"))
            .and(header("authorization", BEARER))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                json!({"admins":[{"email":"a@x.com","githubId":1},{"email":"b@x.com"}]}),
            ))
            .mount(&server)
            .await;
        let rows = client.admins_list().await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].email, "a@x.com");
        assert_eq!(rows[0].github_id, Some(1));
        assert_eq!(rows[1].github_id, None);
    }

    #[tokio::test]
    async fn admins_add_posts_email_and_optional_github_id() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/admin/admins"))
            .and(header("authorization", BEARER))
            .and(body_partial_json(json!({"email":"a@x.com","githubId":7})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
            .mount(&server)
            .await;
        client.admins_add("a@x.com", Some(7)).await.unwrap();
    }

    #[tokio::test]
    async fn admins_remove_uses_delete_with_body() {
        let (server, client) = setup().await;
        Mock::given(method("DELETE"))
            .and(path("/admin/admins"))
            .and(header("authorization", BEARER))
            .and(body_partial_json(json!({"email":"a@x.com"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
            .mount(&server)
            .await;
        client.admins_remove("a@x.com").await.unwrap();
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

    #[tokio::test]
    async fn get_schema_returns_schema_def() {
        let (server, client) = setup().await;
        Mock::given(method("GET"))
            .and(path("/admin/dbs/kanban/schema"))
            .and(header("authorization", BEARER))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "tables": {"notes": {"fields": {"body": {"type": "string"}}}}
            })))
            .mount(&server)
            .await;
        let schema = client.get_schema("kanban").await.unwrap();
        assert_eq!(schema.tables.len(), 1);
        assert!(schema.tables.contains_key("notes"));
    }

    #[tokio::test]
    async fn schema_history_lists_summaries() {
        let (server, client) = setup().await;
        Mock::given(method("GET"))
            .and(path("/admin/db/kanban/schema/history"))
            .and(header("authorization", BEARER))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "entries": [
                    {"version": 3, "capturedAt": 30, "source": "migrate", "principal": "u@x"},
                    {"version": 2, "capturedAt": 20, "source": "push", "principal": null}
                ]
            })))
            .mount(&server)
            .await;
        let entries = client
            .schema_history("kanban", Some(5), None)
            .await
            .unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].version, 3);
        assert_eq!(entries[0].source, "migrate");
        assert_eq!(entries[0].principal.as_deref(), Some("u@x"));
        assert!(entries[1].principal.is_none());
    }

    #[tokio::test]
    async fn schema_history_get_returns_entry_with_schema_blob() {
        let (server, client) = setup().await;
        Mock::given(method("GET"))
            .and(path("/admin/db/kanban/schema/history/3"))
            .and(header("authorization", BEARER))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "version": 3,
                "capturedAt": 30,
                "source": "restore",
                "principal": null,
                "schema": {"tables": {"notes": {"fields": {"body": {"type": "string"}}}}}
            })))
            .mount(&server)
            .await;
        let entry = client.schema_history_get("kanban", 3).await.unwrap();
        assert_eq!(entry.version, 3);
        assert_eq!(entry.source, "restore");
        assert_eq!(
            entry.schema["tables"]["notes"]["fields"]["body"]["type"],
            "string"
        );
    }

    #[tokio::test]
    async fn restore_schema_posts_version_and_confirm() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/admin/db/kanban/schema/restore"))
            .and(header("authorization", BEARER))
            .and(body_partial_json(
                json!({"version": 2, "confirm": "kanban"}),
            ))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"ok": true, "restoredTo": 2})),
            )
            .mount(&server)
            .await;
        let restored_to = client.restore_schema("kanban", 2, "kanban").await.unwrap();
        assert_eq!(restored_to, 2);
    }

    #[tokio::test]
    async fn db_stats_returns_table_stats() {
        let (server, client) = setup().await;
        Mock::given(method("GET"))
            .and(path("/admin/dbs/kanban/stats"))
            .and(header("authorization", BEARER))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "tables": [{"name":"notes","rowCount":5,"sizeBytes":100}],
                "totalSizeBytes": 100
            })))
            .mount(&server)
            .await;
        let stats = client.db_stats("kanban").await.unwrap();
        assert_eq!(stats.total_size_bytes, 100);
        assert_eq!(stats.tables.len(), 1);
        assert_eq!(stats.tables[0].name, "notes");
        assert_eq!(stats.tables[0].row_count, 5);
        assert_eq!(stats.tables[0].size_bytes, 100);
    }

    #[tokio::test]
    async fn list_tokens_returns_token_info() {
        let (server, client) = setup().await;
        Mock::given(method("GET"))
            .and(path("/admin/tokens"))
            .and(query_param("db", "kanban"))
            .and(header("authorization", BEARER))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "tokens": [
                    {"id":"id1","name":"cli","createdAt":123,"revoked":false},
                    {"id":"id2","name":"ci","createdAt":999,"revoked":true}
                ]
            })))
            .mount(&server)
            .await;
        let tokens = client.list_tokens("kanban").await.unwrap();
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].id, "id1");
        assert_eq!(tokens[0].name, "cli");
        assert_eq!(tokens[0].created_at, 123);
        assert!(!tokens[0].revoked);
        assert!(tokens[1].revoked);
    }

    #[tokio::test]
    async fn mint_token_with_options_posts_capabilities() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/admin/mint-token"))
            .and(body_partial_json(json!({
                "db": "kanban",
                "name": "scraper",
                "expiresAt": 1700000000000_i64,
                "readOnly": true,
                "tables": ["users"],
            })))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"tokenId": "id2", "token": "s2"})),
            )
            .mount(&server)
            .await;
        let minted = client
            .mint_token_with_options(
                "kanban",
                "scraper",
                &crate::wire::admin::MintTokenOptions {
                    expires_at: Some(1700000000000),
                    read_only: Some(true),
                    tables: Some(vec!["users".to_string()]),
                },
            )
            .await
            .unwrap();
        assert_eq!(minted.token_id, "id2");
        assert_eq!(minted.token, "s2");
    }

    #[tokio::test]
    async fn mint_token_omits_unset_capabilities() {
        // Backward compat: an unset MintTokenOptions (Default) must serialize
        // to exactly {db, name} — no capability keys leak, so server defaults
        // (full access) apply. Verifies `skip_serializing_if = "Option::is_none"`.
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
        let minted = client
            .mint_token_with_options(
                "kanban",
                "cli",
                &crate::wire::admin::MintTokenOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(minted.token_id, "id1");
        assert_eq!(minted.token, "secret");
    }

    #[tokio::test]
    async fn list_tokens_returns_capability_fields() {
        let (server, client) = setup().await;
        Mock::given(method("GET"))
            .and(path("/admin/tokens"))
            .and(query_param("db", "kanban"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "tokens": [
                    {
                        "id": "t1",
                        "name": "ci",
                        "createdAt": 1,
                        "revoked": false,
                        "expiresAt": null,
                        "readOnly": false,
                        "tables": null
                    },
                    {
                        "id": "t2",
                        "name": "scraper",
                        "createdAt": 2,
                        "revoked": false,
                        "expiresAt": 1700000000000_i64,
                        "readOnly": true,
                        "tables": ["users"]
                    }
                ]
            })))
            .mount(&server)
            .await;
        let tokens = client.list_tokens("kanban").await.unwrap();
        assert_eq!(tokens.len(), 2);
        // Full-access token: null/None/false means "no limit".
        assert_eq!(tokens[0].expires_at, None);
        assert!(!tokens[0].read_only);
        assert_eq!(tokens[0].tables, None);
        // Restricted token: every capability field populated.
        assert_eq!(tokens[1].expires_at, Some(1700000000000));
        assert!(tokens[1].read_only);
        assert_eq!(tokens[1].tables, Some(vec!["users".to_string()]));
    }

    #[tokio::test]
    async fn list_tokens_deserializes_legacy_fixture() {
        // Older servers that don't send the capability fields must still
        // deserialize (defaults: expires_at=None, read_only=false, tables=None).
        let (server, client) = setup().await;
        Mock::given(method("GET"))
            .and(path("/admin/tokens"))
            .and(query_param("db", "kanban"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "tokens": [
                    {"id":"old","name":"legacy","createdAt":42,"revoked":false}
                ]
            })))
            .mount(&server)
            .await;
        let tokens = client.list_tokens("kanban").await.unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].id, "old");
        assert_eq!(tokens[0].expires_at, None);
        assert!(!tokens[0].read_only);
        assert_eq!(tokens[0].tables, None);
    }

    /// Wire-parity for `MintTokenRequest`: camelCase keys, and `None`
    /// capability fields are omitted entirely (not `null`) so server defaults
    /// apply. Mirrors the ts-client `mintToken` body-shape assertion.
    #[test]
    fn mint_token_request_serializes_wire_shape() {
        use crate::wire::admin::MintTokenRequest;

        // Full-access: every capability None → body is exactly {db, name}.
        let full = MintTokenRequest {
            db: "kanban",
            name: "cli",
            expires_at: None,
            read_only: None,
            tables: None,
        };
        assert_eq!(
            serde_json::to_value(&full).unwrap(),
            json!({"db": "kanban", "name": "cli"}),
        );

        // Restricted: every capability set → camelCase keys present.
        let tables = vec!["users".to_string(), "notes".to_string()];
        let scoped = MintTokenRequest {
            db: "dbx",
            name: "scraper",
            expires_at: Some(1700000000000),
            read_only: Some(true),
            tables: Some(&tables),
        };
        assert_eq!(
            serde_json::to_value(&scoped).unwrap(),
            json!({
                "db": "dbx",
                "name": "scraper",
                "expiresAt": 1700000000000_i64,
                "readOnly": true,
                "tables": ["users", "notes"],
            }),
        );

        // Partial: only one capability set → only that key appears.
        let partial = MintTokenRequest {
            db: "dbx",
            name: "reader",
            expires_at: None,
            read_only: Some(true),
            tables: None,
        };
        assert_eq!(
            serde_json::to_value(&partial).unwrap(),
            json!({"db": "dbx", "name": "reader", "readOnly": true}),
        );
    }

    /// Wire-parity for `TokenInfo`: deserializes both the restricted fixture
    /// (`expiresAt:number, readOnly:true, tables:[...]`) and the full-access
    /// fixture (`expiresAt:null, readOnly:false, tables:null`), plus the
    /// legacy fixture (capability fields absent → defaults).
    #[test]
    fn token_info_deserializes_wire_shapes() {
        use crate::wire::admin::TokenInfo;

        let restricted: TokenInfo = serde_json::from_value(json!({
            "id": "t2",
            "name": "scraper",
            "createdAt": 2,
            "revoked": false,
            "expiresAt": 1700000000000_i64,
            "readOnly": true,
            "tables": ["users"],
        }))
        .unwrap();
        assert_eq!(restricted.expires_at, Some(1700000000000));
        assert!(restricted.read_only);
        assert_eq!(restricted.tables, Some(vec!["users".to_string()]));

        let full: TokenInfo = serde_json::from_value(json!({
            "id": "t1",
            "name": "ci",
            "createdAt": 1,
            "revoked": false,
            "expiresAt": null,
            "readOnly": false,
            "tables": null,
        }))
        .unwrap();
        assert_eq!(full.expires_at, None);
        assert!(!full.read_only);
        assert_eq!(full.tables, None);

        let legacy: TokenInfo = serde_json::from_value(json!({
            "id": "old",
            "name": "legacy",
            "createdAt": 42,
            "revoked": false,
        }))
        .unwrap();
        assert_eq!(legacy.expires_at, None);
        assert!(!legacy.read_only);
        assert_eq!(legacy.tables, None);
    }

    #[tokio::test]
    async fn metrics_returns_snapshot() {
        let (server, client) = setup().await;
        Mock::given(method("GET"))
            .and(path("/admin/metrics"))
            .and(header("authorization", BEARER))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "queriesTotal": 10,
                "mutationsTotal": 20,
                "uploadsTotal": 30,
                "wsConnections": 40,
                "activeSubscriptions": 50,
                "poolSize": 60,
                "poolIdle": 70,
                "uptimeSeconds": 80,
                "queryLatency": {"p50": 110, "p95": 220, "p99": 330},
                "mutateLatency": {"p50": 440, "p95": 550, "p99": 660},
                "subscribeLatency": {"p50": 770, "p95": 880, "p99": 990}
            })))
            .mount(&server)
            .await;
        let snap = client.metrics().await.unwrap();
        assert_eq!(snap.queries_total, 10);
        assert_eq!(snap.mutations_total, 20);
        assert_eq!(snap.uploads_total, 30);
        assert_eq!(snap.ws_connections, 40);
        assert_eq!(snap.active_subscriptions, 50);
        assert_eq!(snap.pool_size, 60);
        assert_eq!(snap.pool_idle, 70);
        assert_eq!(snap.uptime_seconds, 80);
        assert_eq!(snap.query_latency.p50, 110);
        assert_eq!(snap.query_latency.p95, 220);
        assert_eq!(snap.query_latency.p99, 330);
        assert_eq!(snap.mutate_latency.p99, 660);
        assert_eq!(snap.subscribe_latency.p50, 770);
        // This mock body omits the invalidation counters entirely — i.e. it is
        // an OLDER server's response. `#[serde(default)]` on that field group
        // must keep the whole snapshot parseable rather than failing the call.
        assert_eq!(snap.subs_reruns_total, 0);
        assert_eq!(snap.subs_skips_ordered_total, 0);
        assert_eq!(snap.subs_missed_pushes_total, 0);
    }

    #[tokio::test]
    async fn get_metrics_parses_invalidation_counters() {
        let (server, client) = setup().await;
        Mock::given(method("GET"))
            .and(path("/admin/metrics"))
            .and(header("authorization", BEARER))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "queriesTotal": 0,
                "mutationsTotal": 0,
                "uploadsTotal": 0,
                "wsConnections": 0,
                "activeSubscriptions": 0,
                "poolSize": 0,
                "poolIdle": 0,
                "uptimeSeconds": 0,
                "queryLatency": {"p50": 0, "p95": 0, "p99": 0},
                "mutateLatency": {"p50": 0, "p95": 0, "p99": 0},
                "subscribeLatency": {"p50": 0, "p95": 0, "p99": 0},
                "subsRerunsTotal": 4,
                "subsSkipsPointTotal": 1,
                "subsSkipsIndexedTotal": 2,
                "subsSkipsOrderedTotal": 3,
                "subsSkipVerificationsTotal": 6,
                "subsMissedPushesTotal": 0
            })))
            .mount(&server)
            .await;
        let snap = client.metrics().await.unwrap();
        assert_eq!(snap.subs_reruns_total, 4);
        assert_eq!(snap.subs_skips_point_total, 1);
        assert_eq!(snap.subs_skips_indexed_total, 2);
        assert_eq!(snap.subs_skips_ordered_total, 3);
        assert_eq!(snap.subs_skip_verifications_total, 6);
        assert_eq!(snap.subs_missed_pushes_total, 0);
    }

    #[tokio::test]
    async fn get_config_returns_response() {
        let (server, client) = setup().await;
        Mock::given(method("GET"))
            .and(path("/admin/config"))
            .and(header("authorization", BEARER))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "port": 8080,
                "publicUrl": "https://rtdb.example",
                "githubBaseUrl": "https://github.com",
                "githubApiUrl": "https://api.github.com",
                "databaseUrlConfigured": true,
                "adminKeyConfigured": true,
                "githubConfigured": false,
                "googleConfigured": false,
                "gitlabConfigured": false,
                "oidcConfigured": false,
                "hot": {
                    "allowedOrigins": ["https://app.example"],
                    "sessionTtlDays": 30,
                    "maxFileSize": 5242880,
                    "idempotencyTtlMs": 300000
                },
                "version": "0.1.0",
                "gitCommit": "abc1234",
                "admins": [{"email":"a@x.com","githubId":1}]
            })))
            .mount(&server)
            .await;
        let cfg = client.get_config().await.unwrap();
        assert_eq!(cfg.port, 8080);
        assert_eq!(cfg.public_url, "https://rtdb.example");
        assert!(cfg.database_url_configured);
        assert!(cfg.admin_key_configured);
        assert!(!cfg.github_configured);
        assert_eq!(
            cfg.hot.allowed_origins,
            vec!["https://app.example".to_string()]
        );
        assert_eq!(cfg.hot.session_ttl_days, 30);
        assert_eq!(cfg.hot.max_file_size, 5242880);
        assert_eq!(cfg.hot.idempotency_ttl_ms, 300000);
        assert_eq!(cfg.version, "0.1.0");
        assert_eq!(cfg.git_commit, "abc1234");
        assert_eq!(cfg.admins.len(), 1);
        assert_eq!(cfg.admins[0].email, "a@x.com");
        assert_eq!(cfg.admins[0].github_id, Some(1));
    }

    #[tokio::test]
    async fn patch_config_patches_and_returns_config() {
        let (server, client) = setup().await;
        Mock::given(method("PATCH"))
            .and(path("/admin/config"))
            .and(header("authorization", BEARER))
            .and(body_partial_json(json!({
                "allowedOrigins": ["https://x.example"],
                "sessionTtlDays": 60
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "port": 8080,
                "publicUrl": "",
                "githubBaseUrl": "",
                "githubApiUrl": "",
                "databaseUrlConfigured": false,
                "adminKeyConfigured": false,
                "githubConfigured": false,
                "googleConfigured": false,
                "gitlabConfigured": false,
                "oidcConfigured": false,
                "hot": {
                    "allowedOrigins": ["https://x.example"],
                    "sessionTtlDays": 60,
                    "maxFileSize": 5242880,
                    "idempotencyTtlMs": 300000
                },
                "version": "",
                "gitCommit": "",
                "admins": []
            })))
            .mount(&server)
            .await;
        let patch = crate::wire::admin::HotConfigPatch {
            allowed_origins: Some(vec!["https://x.example".to_string()]),
            session_ttl_days: Some(60),
            max_file_size: None,
            idempotency_ttl_ms: None,
        };
        let cfg = client.patch_config(&patch).await.unwrap();
        assert_eq!(
            cfg.hot.allowed_origins,
            vec!["https://x.example".to_string()]
        );
        assert_eq!(cfg.hot.session_ttl_days, 60);
    }

    #[tokio::test]
    async fn patch_config_surfaces_400_envelope() {
        let (server, client) = setup().await;
        Mock::given(method("PATCH"))
            .and(path("/admin/config"))
            .respond_with(ResponseTemplate::new(400).set_body_json(
                json!({"code": "BAD_REQUEST", "message": "sessionTtlDays must be >= 1"}),
            ))
            .mount(&server)
            .await;
        let err = client
            .patch_config(&crate::wire::admin::HotConfigPatch::default())
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert_eq!(err.message, "sessionTtlDays must be >= 1");
    }

    #[tokio::test]
    async fn ops_recent_returns_events_and_query_params() {
        let (server, client) = setup().await;
        Mock::given(method("GET"))
            .and(path("/admin/ops/recent"))
            .and(query_param("db", "kanban"))
            .and(query_param("n", "50"))
            .and(header("authorization", BEARER))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "ops": [
                    {"db":"kanban","table":"notes","docId":"n1","kind":"insert","ts":1000,"owner":null},
                    {"db":"kanban","table":"notes","docId":"n2","kind":"patch","ts":2000,"owner":"u1"}
                ]
            })))
            .mount(&server)
            .await;
        let ops = client.ops_recent("kanban", None, Some(50)).await.unwrap();
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0].db, "kanban");
        assert_eq!(ops[0].table, "notes");
        assert_eq!(ops[0].doc_id, "n1");
        assert_eq!(ops[0].kind, "insert");
        assert_eq!(ops[0].ts, 1000);
        assert_eq!(ops[0].owner, None);
        assert_eq!(ops[1].owner, Some("u1".to_string()));
    }

    // Owner-bypass document endpoints (`POST /admin/db/{db}/query|mutate`).
    // Unlike the non-admin `run`/`mutate`, `db` rides in the path (singular
    // `db`, not the plural `dbs` used by `get_schema`/`db_stats`), so the body
    // omits it. `idempotencyKey` is omitted when `None`.

    #[tokio::test]
    async fn admin_query_posts_to_admin_db_singular_path_and_unwraps_result() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/admin/db/kanban/query"))
            .and(header("authorization", BEARER))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": [{"_id": "a"}, {"_id": "b"}]
            })))
            .mount(&server)
            .await;
        let q = TableQuery::new("items").take(2);
        let got: Vec<Value> = client.admin_query("kanban", &q).await.unwrap();
        assert_eq!(got.len(), 2);
        // `db` rides in the path, not the body
        let body: Value =
            serde_json::from_slice(&server.received_requests().await.unwrap()[0].body).unwrap();
        assert!(
            body.get("db").is_none(),
            "admin_query body must not carry db: {body}"
        );
        assert!(
            body.get("query").is_some(),
            "admin_query body must carry query: {body}"
        );
    }

    #[tokio::test]
    async fn admin_mutate_posts_to_admin_db_singular_path_and_unwraps_results() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/admin/db/kanban/mutate"))
            .and(header("authorization", BEARER))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "results": [{"id": "new1"}, null]
            })))
            .mount(&server)
            .await;
        let txn = Mutation::new()
            .insert("items", json!({"name": "x"}))
            .patch("items", "i1", json!({"y": 1}))
            .build();
        let res = client.admin_mutate("kanban", &txn, None).await.unwrap();
        assert_eq!(res.len(), 2);
        assert!(matches!(
            res[0],
            crate::mutation::StepResult::Insert { ref id } if id == "new1"
        ));
        // omit-when-no-key: `idempotencyKey` is absent and `db` rides in the path
        let body: Value =
            serde_json::from_slice(&server.received_requests().await.unwrap()[0].body).unwrap();
        assert!(
            body.get("db").is_none(),
            "admin_mutate body must not carry db: {body}"
        );
        assert!(
            body.get("idempotencyKey").is_none(),
            "admin_mutate must omit idempotencyKey when None: {body}"
        );
        assert!(
            body.get("txn").is_some(),
            "admin_mutate body must carry txn: {body}"
        );
    }

    #[tokio::test]
    async fn admin_mutate_includes_idempotency_key_when_some() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/admin/db/kanban/mutate"))
            .and(header("authorization", BEARER))
            .and(body_partial_json(json!({"idempotencyKey": "k1"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"results": []})))
            .mount(&server)
            .await;
        let txn = Mutation::new().delete("items", "i1").build();
        client
            .admin_mutate("kanban", &txn, Some("k1"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn migrate_schema_posts_directives_and_dry_run() {
        // Mirrors `push_schema_serializes_schema_json`: the body carries the
        // `directives` array (tagged by `op`, camelCase) plus `dryRun`, and the
        // parsed `MigrateResult` preserves `applied` / derived `schema` /
        // per-directive `affectedRows`.
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/admin/db/kanban/migrate"))
            .and(header("authorization", BEARER))
            .and(body_partial_json(json!({
                "directives": [
                    {"op": "renameField", "table": "items", "from": "name", "to": "title"},
                    {"op": "dropIndex", "table": "items", "name": "by_name"}
                ],
                "dryRun": true
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "applied": false,
                "schema": {"tables": {"items": {"fields": {"title": {"type": "string"}}}}},
                "directives": [
                    {"op": "renameField", "affectedRows": 2},
                    {"op": "dropIndex", "affectedRows": 0}
                ]
            })))
            .mount(&server)
            .await;
        let directives = crate::migration::Migration::new()
            .rename_field("items", "name", "title")
            .drop_index("items", "by_name")
            .build();
        let result = client
            .migrate_schema("kanban", &directives, true)
            .await
            .unwrap();
        assert!(!result.applied);
        assert_eq!(result.directives.len(), 2);
        assert_eq!(result.directives[0].op, "renameField");
        assert_eq!(result.directives[0].affected_rows, 2);
        assert_eq!(result.directives[1].op, "dropIndex");
        assert_eq!(result.directives[1].affected_rows, 0);
        assert!(result.schema.tables.contains_key("items"));
        assert!(result.schema.tables["items"].fields.contains_key("title"));
    }

    // ── Admin backup methods (trigger / list / download / delete / restore) ──

    #[tokio::test]
    async fn backup_now_posts_empty_body_to_admin_backup() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/admin/backup"))
            .and(header("authorization", BEARER))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({"ok": true})))
            .mount(&server)
            .await;
        client.backup_now().await.unwrap();
    }

    #[tokio::test]
    async fn list_backups_parses_running_and_backup_entries() {
        let (server, client) = setup().await;
        Mock::given(method("GET"))
            .and(path("/admin/backups"))
            .and(header("authorization", BEARER))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "running": true,
                "backups": [
                    {"name": "rtdb-20260728T143045Z.dump", "sizeBytes": 12345, "createdMs": 1753713045000_i64},
                    {"name": "rtdb-20260727T010000Z.dump", "sizeBytes": 999,   "createdMs": 1753574400000_i64}
                ]
            })))
            .mount(&server)
            .await;
        let res = client.list_backups().await.unwrap();
        assert!(res.running);
        assert_eq!(res.backups.len(), 2);
        assert_eq!(res.backups[0].name, "rtdb-20260728T143045Z.dump");
        assert_eq!(res.backups[0].size_bytes, 12345);
        assert_eq!(res.backups[0].created_ms, 1753713045000_i64);
        assert_eq!(res.backups[1].size_bytes, 999);
    }

    #[tokio::test]
    async fn download_backup_returns_raw_bytes_without_json_decoding() {
        let (server, client) = setup().await;
        let payload = b"PG_DUMP binary payload \x00\x01\x02 here";
        Mock::given(method("GET"))
            .and(path("/admin/backups/rtdb-20260728T143045Z.dump"))
            .and(header("authorization", BEARER))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "application/octet-stream")
                    .set_body_bytes(payload.to_vec()),
            )
            .mount(&server)
            .await;
        let bytes = client
            .download_backup("rtdb-20260728T143045Z.dump")
            .await
            .unwrap();
        assert_eq!(bytes, payload);
    }

    #[tokio::test]
    async fn download_backup_surfaces_not_found_envelope() {
        let (server, client) = setup().await;
        Mock::given(method("GET"))
            .and(path("/admin/backups/missing.dump"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({
                "code": "NOT_FOUND",
                "message": "backup file not found"
            })))
            .mount(&server)
            .await;
        let err = client.download_backup("missing.dump").await.unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
        assert_eq!(err.message, "backup file not found");
    }

    #[tokio::test]
    async fn delete_backup_returns_no_content_on_success() {
        let (server, client) = setup().await;
        Mock::given(method("DELETE"))
            .and(path("/admin/backups/rtdb-20260728T143045Z.dump"))
            .and(header("authorization", BEARER))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;
        client
            .delete_backup("rtdb-20260728T143045Z.dump")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn restore_backup_sends_confirm_equal_to_name_and_parses_target() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/admin/restore"))
            .and(header("authorization", BEARER))
            .and(body_partial_json(json!({
                "name": "rtdb-20260728T143045Z.dump",
                "confirm": "rtdb-20260728T143045Z.dump"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "target": "rtdb_restored_20260728T143045Z",
                "instructions": "Restore complete into database 'rtdb_restored_20260728T143045Z'."
            })))
            .mount(&server)
            .await;
        let r = client
            .restore_backup("rtdb-20260728T143045Z.dump")
            .await
            .unwrap();
        assert_eq!(r.target, "rtdb_restored_20260728T143045Z");
        assert!(r.instructions.starts_with("Restore complete"));
    }

    // ── Webhook management (mirror ts-client admin.test.ts webhook suite) ─────

    #[tokio::test]
    async fn list_webhooks_returns_rows() {
        let (server, client) = setup().await;
        Mock::given(method("GET"))
            .and(path("/admin/db/kanban/webhooks"))
            .and(header("authorization", BEARER))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "webhooks": [
                    {"id":1,"db":"kanban","table":null,"url":"https://a.example/hook","events":["*"],"createdAt":1000,"enabled":true},
                    {"id":2,"db":"kanban","table":"notes","url":"https://b.example/hook","events":["insert","patch"],"createdAt":2000,"enabled":false}
                ]
            })))
            .mount(&server)
            .await;
        let rows = client.list_webhooks("kanban").await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].id, 1);
        assert_eq!(rows[0].db, "kanban");
        assert_eq!(rows[0].table, None);
        assert_eq!(rows[0].url, "https://a.example/hook");
        assert_eq!(rows[0].events, vec!["*".to_string()]);
        assert_eq!(rows[0].created_at, 1000);
        assert!(rows[0].enabled);
        assert_eq!(rows[1].table.as_deref(), Some("notes"));
        assert_eq!(
            rows[1].events,
            vec!["insert".to_string(), "patch".to_string()]
        );
        assert!(!rows[1].enabled);
    }

    #[tokio::test]
    async fn list_webhooks_deserializes_legacy_fixture_omitting_enabled() {
        // Older server (pre-ENH-003 `enabled` flag) must still parse via
        // `#[serde(default)]` on Webhook.enabled — defaulting to false.
        let (server, client) = setup().await;
        Mock::given(method("GET"))
            .and(path("/admin/db/kanban/webhooks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "webhooks": [
                    {"id":1,"db":"kanban","table":null,"url":"https://a.example/hook","events":["*"],"createdAt":1000}
                ]
            })))
            .mount(&server)
            .await;
        let rows = client.list_webhooks("kanban").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, 1);
        assert_eq!(rows[0].table, None);
        assert!(!rows[0].enabled);
    }

    #[tokio::test]
    async fn create_webhook_posts_options_and_returns_id() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/admin/db/kanban/webhooks"))
            .and(header("authorization", BEARER))
            .and(body_partial_json(json!({
                "url": "https://hook.example/cb",
                "table": "notes",
                "events": ["insert", "patch"],
                "enabled": false
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": 7})))
            .mount(&server)
            .await;
        let id = client
            .create_webhook(
                "kanban",
                &crate::wire::admin::CreateWebhookOptions {
                    url: "https://hook.example/cb".to_string(),
                    table: Some("notes".to_string()),
                    events: Some(vec!["insert".to_string(), "patch".to_string()]),
                    enabled: Some(false),
                },
            )
            .await
            .unwrap();
        assert_eq!(id, 7);
    }

    #[tokio::test]
    async fn create_webhook_omits_unset_options() {
        // Backward compat: only `url` is required — an unset `table`/`events`/
        // `enabled` must each stay off the wire (skip_serializing_if), so the
        // server applies its defaults (all-tables, ["*"], enabled=true).
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/admin/db/kanban/webhooks"))
            .and(body_partial_json(json!({"url": "https://hook.example/cb"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": 1})))
            .mount(&server)
            .await;
        let id = client
            .create_webhook(
                "kanban",
                &crate::wire::admin::CreateWebhookOptions {
                    url: "https://hook.example/cb".to_string(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(id, 1);
        // Assert nothing else leaked onto the wire.
        let body: Value =
            serde_json::from_slice(&server.received_requests().await.unwrap()[0].body).unwrap();
        assert!(body.get("table").is_none(), "table leaked: {body}");
        assert!(body.get("events").is_none(), "events leaked: {body}");
        assert!(body.get("enabled").is_none(), "enabled leaked: {body}");
    }

    #[tokio::test]
    async fn edit_webhook_puts_options_and_returns_webhook() {
        let (server, client) = setup().await;
        Mock::given(method("PUT"))
            .and(path("/admin/db/kanban/webhooks/3"))
            .and(header("authorization", BEARER))
            .and(body_partial_json(json!({
                "url": "https://new.example/cb",
                "enabled": false
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id":3,"db":"kanban","table":null,"url":"https://new.example/cb","events":["*"],"createdAt":1000,"enabled":false
            })))
            .mount(&server)
            .await;
        let updated = client
            .edit_webhook(
                "kanban",
                3,
                &crate::wire::admin::WebhookEditOptions {
                    url: Some("https://new.example/cb".to_string()),
                    enabled: Some(false),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(updated.id, 3);
        assert_eq!(updated.url, "https://new.example/cb");
        assert!(!updated.enabled);
        // `opts.table = None` → field must be ABSENT from the body (leave alone).
        let body: Value =
            serde_json::from_slice(&server.received_requests().await.unwrap()[0].body).unwrap();
        assert!(
            body.get("table").is_none(),
            "edit_webhook must omit table when None: {body}"
        );
    }

    #[tokio::test]
    async fn edit_webhook_clears_table_with_some_none() {
        // `opts.table = Some(None)` → serialized as JSON `null` → server clears
        // to all-tables. The body must contain `"table": null` (NOT omit it).
        let (server, client) = setup().await;
        Mock::given(method("PUT"))
            .and(path("/admin/db/kanban/webhooks/3"))
            .and(body_partial_json(json!({"table": null})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id":3,"db":"kanban","table":null,"url":"https://x/cb","events":["*"],"createdAt":1,"enabled":true
            })))
            .mount(&server)
            .await;
        client
            .edit_webhook(
                "kanban",
                3,
                &crate::wire::admin::WebhookEditOptions {
                    table: Some(None),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn edit_webhook_sets_table_with_some_some() {
        // `opts.table = Some(Some("notes"))` → serialized as `"notes"` → server
        // sets the filter.
        let (server, client) = setup().await;
        Mock::given(method("PUT"))
            .and(path("/admin/db/kanban/webhooks/3"))
            .and(body_partial_json(json!({"table": "notes"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id":3,"db":"kanban","table":"notes","url":"https://x/cb","events":["*"],"createdAt":1,"enabled":true
            })))
            .mount(&server)
            .await;
        client
            .edit_webhook(
                "kanban",
                3,
                &crate::wire::admin::WebhookEditOptions {
                    table: Some(Some("notes".to_string())),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn delete_webhook_deletes_and_returns_ok() {
        let (server, client) = setup().await;
        Mock::given(method("DELETE"))
            .and(path("/admin/db/kanban/webhooks/4"))
            .and(header("authorization", BEARER))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
            .mount(&server)
            .await;
        client.delete_webhook("kanban", 4).await.unwrap();
    }

    #[tokio::test]
    async fn delete_webhook_surfaces_not_found_envelope() {
        let (server, client) = setup().await;
        Mock::given(method("DELETE"))
            .and(path("/admin/db/kanban/webhooks/99"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({
                "code": "NOT_FOUND",
                "message": "webhook not found for this database"
            })))
            .mount(&server)
            .await;
        let err = client.delete_webhook("kanban", 99).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
    }

    #[tokio::test]
    async fn list_deliveries_returns_rows_with_query_params() {
        let (server, client) = setup().await;
        Mock::given(method("GET"))
            .and(path("/admin/db/kanban/webhooks/3/deliveries"))
            .and(query_param("status", "retrying"))
            .and(query_param("limit", "10"))
            .and(query_param("offset", "20"))
            .and(header("authorization", BEARER))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "deliveries": [
                    {"id":1,"attempts":2,"status":"retrying","nextAttempt":5000,"lastError":"boom","payload":{"db":"kanban","table":"notes","docId":"n1","kind":"insert","ts":1000,"owner":null,"source":"mutate"}},
                    {"id":2,"attempts":0,"status":"retrying","nextAttempt":6000,"lastError":null,"payload":{"db":"kanban","table":"notes","docId":"n2","kind":"patch","ts":2000,"owner":"u1","source":"scheduled"}}
                ]
            })))
            .mount(&server)
            .await;
        let rows = client
            .list_deliveries(
                "kanban",
                3,
                Some(&crate::wire::admin::ListDeliveriesOptions {
                    status: Some("retrying".to_string()),
                    limit: Some(10),
                    offset: Some(20),
                }),
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].id, 1);
        assert_eq!(rows[0].attempts, 2);
        assert_eq!(rows[0].status, "retrying");
        assert_eq!(rows[0].next_attempt, 5000);
        assert_eq!(rows[0].last_error.as_deref(), Some("boom"));
        assert_eq!(
            rows[0].payload.get("docId").and_then(Value::as_str),
            Some("n1")
        );
        assert_eq!(rows[1].last_error, None);
        assert_eq!(
            rows[1].payload.get("owner").and_then(Value::as_str),
            Some("u1")
        );
    }

    #[tokio::test]
    async fn list_deliveries_none_opts_sends_no_query_params() {
        // `opts = None` ⇒ first page, no filter — no `status`/`limit`/`offset`
        // query params may appear on the wire.
        let (server, client) = setup().await;
        Mock::given(method("GET"))
            .and(path("/admin/db/kanban/webhooks/3/deliveries"))
            .and(header("authorization", BEARER))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"deliveries": []})))
            .mount(&server)
            .await;
        let rows = client.list_deliveries("kanban", 3, None).await.unwrap();
        assert!(rows.is_empty());
        let reqs = server.received_requests().await.unwrap();
        assert_eq!(reqs.len(), 1);
        assert!(
            reqs[0].url.query().is_none(),
            "query leaked: {}",
            reqs[0].url
        );
    }

    /// Wire-shape parity for `Webhook`: every field camelCase on the wire,
    /// `null` table deserializes to `None`, and a missing `enabled`/`table`
    /// (legacy fixture) still parses via `#[serde(default)]`.
    #[test]
    fn webhook_deserializes_wire_shapes() {
        use crate::wire::admin::Webhook;

        let full: Webhook = serde_json::from_value(json!({
            "id": 1,
            "db": "kanban",
            "table": null,
            "url": "https://a.example/hook",
            "events": ["*"],
            "createdAt": 1000,
            "enabled": true
        }))
        .unwrap();
        assert_eq!(full.id, 1);
        assert_eq!(full.table, None);
        assert!(full.enabled);

        let scoped: Webhook = serde_json::from_value(json!({
            "id": 2,
            "db": "kanban",
            "table": "notes",
            "url": "https://b.example/hook",
            "events": ["insert", "patch"],
            "createdAt": 2000,
            "enabled": false
        }))
        .unwrap();
        assert_eq!(scoped.table.as_deref(), Some("notes"));
        assert_eq!(
            scoped.events,
            vec!["insert".to_string(), "patch".to_string()]
        );
        assert!(!scoped.enabled);

        // Legacy server omitting `enabled` (and, defensively, `table`) must
        // still deserialize. `enabled` defaults to false; `table` to None.
        let legacy: Webhook = serde_json::from_value(json!({
            "id": 3,
            "db": "kanban",
            "url": "https://c.example/hook",
            "events": ["*"],
            "createdAt": 3000
        }))
        .unwrap();
        assert_eq!(legacy.table, None);
        assert!(!legacy.enabled);
    }

    /// Wire-shape parity for `WebhookDelivery`: camelCase keys, optional
    /// `lastError` (`null` and absent both deserialize to `None`), and `payload`
    /// is the verbatim JSON body the worker will/did POST.
    #[test]
    fn webhook_delivery_deserializes_wire_shapes() {
        use crate::wire::admin::WebhookDelivery;

        let with_err: WebhookDelivery = serde_json::from_value(json!({
            "id": 1,
            "attempts": 2,
            "status": "retrying",
            "nextAttempt": 5000,
            "lastError": "connection refused",
            "payload": {"db":"kanban","table":"notes","docId":"n1","kind":"insert","ts":1000,"owner":null,"source":"mutate"}
        }))
        .unwrap();
        assert_eq!(with_err.attempts, 2);
        assert_eq!(with_err.next_attempt, 5000);
        assert_eq!(with_err.last_error.as_deref(), Some("connection refused"));
        assert_eq!(
            with_err.payload.get("kind").and_then(Value::as_str),
            Some("insert")
        );

        // No error yet (queued, not yet attempted): `null` lastError.
        let no_err: WebhookDelivery = serde_json::from_value(json!({
            "id": 2,
            "attempts": 0,
            "status": "pending",
            "nextAttempt": 1000,
            "lastError": null,
            "payload": {"db":"kanban","table":"notes","docId":"n2","kind":"patch","ts":2000,"owner":"u1","source":"scheduled"}
        }))
        .unwrap();
        assert_eq!(no_err.last_error, None);
        assert_eq!(
            no_err.payload.get("source").and_then(Value::as_str),
            Some("scheduled")
        );

        // Defensive legacy: a server omitting `lastError` entirely must still
        // parse (defaults to None via Option's implicit default).
        let missing_err: WebhookDelivery = serde_json::from_value(json!({
            "id": 3,
            "attempts": 0,
            "status": "pending",
            "nextAttempt": 1000,
            "payload": {}
        }))
        .unwrap();
        assert_eq!(missing_err.last_error, None);
    }

    /// The load-bearing tri-state on `WebhookEditOptions.table`. Outer `None`
    /// omits the field entirely (leave alone); `Some(None)` emits JSON `null`
    /// (clear to all-tables); `Some(Some("x"))` emits `"x"` (set). The other
    /// fields follow the standard `Option::is_none` skip pattern. Mirrors
    /// `mint_token_request_serializes_wire_shape`'s body-shape assertion.
    #[test]
    fn webhook_edit_options_table_tri_state() {
        use crate::wire::admin::WebhookEditOptions;

        // 1) Every field None → empty body (nothing on the wire → server
        //    changes nothing).
        let all_none = WebhookEditOptions::default();
        assert_eq!(serde_json::to_value(&all_none).unwrap(), json!({}),);

        // 2) `table = None` (outer) → field ABSENT from body, even though the
        //    inner is also None. This is the "leave the filter alone" path.
        let table_omitted = WebhookEditOptions {
            enabled: Some(false),
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_value(&table_omitted).unwrap(),
            json!({"enabled": false}),
        );

        // 3) `table = Some(None)` → serialized as JSON `null` → server clears
        //    to all-tables. This is the case that distinguishes the double
        //    Option from a flat Option.
        let table_cleared = WebhookEditOptions {
            table: Some(None),
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_value(&table_cleared).unwrap(),
            json!({"table": null}),
        );

        // 4) `table = Some(Some("notes"))` → serialized as the string → server
        //    sets the filter.
        let table_set = WebhookEditOptions {
            table: Some(Some("notes".to_string())),
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_value(&table_set).unwrap(),
            json!({"table": "notes"}),
        );

        // 5) Full body: every field set exercises the camelCase wire names.
        let events = vec!["insert".to_string(), "patch".to_string()];
        let full = WebhookEditOptions {
            url: Some("https://new.example/cb".to_string()),
            table: Some(Some("notes".to_string())),
            events: Some(events.clone()),
            enabled: Some(false),
        };
        assert_eq!(
            serde_json::to_value(&full).unwrap(),
            json!({
                "url": "https://new.example/cb",
                "table": "notes",
                "events": ["insert", "patch"],
                "enabled": false
            }),
        );
    }

    /// Wire-shape parity for `CreateWebhookOptions`: camelCase keys, and `None`
    /// fields are omitted entirely so server defaults apply (the same contract
    /// as `MintTokenOptions`).
    #[test]
    fn create_webhook_options_serializes_wire_shape() {
        use crate::wire::admin::CreateWebhookOptions;

        // Bare minimum: only `url` is required → body is exactly `{url}`.
        let bare = CreateWebhookOptions {
            url: "https://hook.example/cb".to_string(),
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_value(&bare).unwrap(),
            json!({"url": "https://hook.example/cb"}),
        );

        // Fully-specified → camelCase keys for every option.
        let events = vec!["insert".to_string(), "patch".to_string()];
        let full = CreateWebhookOptions {
            url: "https://hook.example/cb".to_string(),
            table: Some("notes".to_string()),
            events: Some(events.clone()),
            enabled: Some(false),
        };
        assert_eq!(
            serde_json::to_value(&full).unwrap(),
            json!({
                "url": "https://hook.example/cb",
                "table": "notes",
                "events": ["insert", "patch"],
                "enabled": false
            }),
        );
    }

    // ── Audit log (GET /admin/audit) ─────────────────────────────────────────
    //
    // Mirror `list_deliveries`-style assertions: provided opts build the right
    // `?db=&table=&op=&principal=&source=&limit=&offset=` query, `opts = None`
    // sends only `db`, and the parsed `AuditEntry` rows match the wire shape
    // (camelCase, `null` op/principal for system-initiated rows).

    #[tokio::test]
    async fn get_audit_builds_query_params_from_opts() {
        let (server, client) = setup().await;
        Mock::given(method("GET"))
            .and(path("/admin/audit"))
            .and(query_param("db", "kanban"))
            .and(query_param("table", "notes"))
            .and(query_param("op", "insert"))
            .and(query_param("principal", "u1"))
            .and(query_param("source", "mutate"))
            .and(query_param("limit", "50"))
            .and(query_param("offset", "100"))
            .and(header("authorization", BEARER))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "entries": [
                    {"id":1,"tsMs":1000,"db":"kanban","table":"notes","op":"insert","docId":"n1","principal":"u1","source":"mutate"}
                ]
            })))
            .mount(&server)
            .await;
        let rows = client
            .get_audit(
                "kanban",
                Some(&crate::wire::admin::AuditQuery {
                    table: Some("notes".to_string()),
                    op: Some("insert".to_string()),
                    principal: Some("u1".to_string()),
                    source: Some("mutate".to_string()),
                    limit: Some(50),
                    offset: Some(100),
                }),
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, 1);
        assert_eq!(rows[0].ts_ms, 1000);
        assert_eq!(rows[0].db, "kanban");
        assert_eq!(rows[0].table, "notes");
        assert_eq!(rows[0].op.as_deref(), Some("insert"));
        assert_eq!(rows[0].doc_id, "n1");
        assert_eq!(rows[0].principal.as_deref(), Some("u1"));
        assert_eq!(rows[0].source, "mutate");
    }

    #[tokio::test]
    async fn get_audit_none_opts_sends_only_db() {
        // `opts = None` ⇒ only `db` rides on the query string; no
        // `table`/`op`/`principal`/`source`/`limit`/`offset` may appear.
        let (server, client) = setup().await;
        Mock::given(method("GET"))
            .and(path("/admin/audit"))
            .and(query_param("db", "kanban"))
            .and(header("authorization", BEARER))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"entries": []})))
            .mount(&server)
            .await;
        let rows = client.get_audit("kanban", None).await.unwrap();
        assert!(rows.is_empty());
        let reqs = server.received_requests().await.unwrap();
        assert_eq!(reqs.len(), 1);
        let query = reqs[0].url.query().unwrap_or("");
        for key in ["table", "op", "principal", "source", "limit", "offset"] {
            assert!(
                !query.contains(&format!("{key}=")),
                "unexpected {key} in query: {query}"
            );
        }
    }

    /// Wire-shape parity for `AuditEntry`: camelCase keys, `null` op/principal
    /// deserializes to `None`, and a legacy server omitting the optional fields
    /// still parses via `#[serde(default)]`.
    #[test]
    fn audit_entry_deserializes_wire_shapes() {
        use crate::wire::admin::AuditEntry;

        // Fully-specified interactive row: every field present, op/principal
        // carrying real values.
        let interactive: AuditEntry = serde_json::from_value(json!({
            "id": 1,
            "tsMs": 1700000000000_i64,
            "db": "kanban",
            "table": "notes",
            "op": "insert",
            "docId": "n1",
            "principal": "u1",
            "source": "mutate"
        }))
        .unwrap();
        assert_eq!(interactive.id, 1);
        assert_eq!(interactive.ts_ms, 1700000000000_i64);
        assert_eq!(interactive.db, "kanban");
        assert_eq!(interactive.table, "notes");
        assert_eq!(interactive.op.as_deref(), Some("insert"));
        assert_eq!(interactive.doc_id, "n1");
        assert_eq!(interactive.principal.as_deref(), Some("u1"));
        assert_eq!(interactive.source, "mutate");

        // System-initiated row (TTL reap / scheduled job): op and principal
        // are JSON `null` on the wire.
        let system: AuditEntry = serde_json::from_value(json!({
            "id": 2,
            "tsMs": 1700000000001_i64,
            "db": "kanban",
            "table": "notes",
            "op": null,
            "docId": "n2",
            "principal": null,
            "source": "ttl"
        }))
        .unwrap();
        assert_eq!(system.op, None);
        assert_eq!(system.principal, None);
        assert_eq!(system.source, "ttl");

        // Legacy fixture: an older server that omits `op`/`principal` entirely
        // must still deserialize via `#[serde(default)]` (both default to None).
        let legacy: AuditEntry = serde_json::from_value(json!({
            "id": 3,
            "tsMs": 42,
            "db": "legacy",
            "table": "things",
            "docId": "t3",
            "source": "mutate"
        }))
        .unwrap();
        assert_eq!(legacy.id, 3);
        assert_eq!(legacy.op, None);
        assert_eq!(legacy.principal, None);
    }
}
