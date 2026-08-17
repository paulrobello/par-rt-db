//! One-shot HTTP client for par-rt-db. `Authorization: Bearer <token>` on every call.

use crate::error::{ErrorEnvelope, RtDbError, retry_on_precondition};
use crate::mutation::{Mutation, StepResult, Transaction};
use crate::query::{Query, TableQuery, parse_result};
use crate::wire::{
    AuthedUser, ScheduleInfo, ScheduleWhen, WorkflowInfo, WorkflowSpec, WorkflowStatus,
};
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

/// One-shot HTTP client for one par-rt-db database: typed queries, atomic
/// transactions, scheduling/cron, durable workflows, and file storage, all
/// bearer-token authorized. For live query subscriptions use the reactive
/// `RtDbClient` (`ws` feature); for `/admin/*` work obtain an admin client via
/// `admin_client()` (the `admin` feature).
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
    /// Server-assigned opaque file id (usable in `get_url`/serve routes).
    pub id: String,
    /// SHA-256 hex digest of the stored bytes.
    pub sha256: String,
    /// Size in bytes.
    pub size: i64,
    #[serde(default)]
    /// The upload's `Content-Type`, when the server recorded one.
    pub content_type: Option<String>,
}

/// File metadata returned by [`RtDbHttpClient::get_file_metadata`]. Mirrors
/// `UploadResult` plus the server-recorded `creationTime`.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileMetadata {
    /// Server-assigned opaque file id.
    pub id: String,
    /// SHA-256 hex digest of the stored bytes.
    pub sha256: String,
    /// Size in bytes.
    pub size: i64,
    #[serde(default)]
    /// The stored `Content-Type`, when the server recorded one.
    pub content_type: Option<String>,
    /// Upload timestamp, epoch milliseconds.
    pub creation_time: i64,
}

/// A signed, time-limited storage URL minted by the server
/// (`GET /api/storage/{db}/{id}/signed-url`). `expires_at` is epoch
/// milliseconds; the URL stops working after it.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignedUrl {
    /// The signed public URL.
    pub url: String,
    /// Absolute expiry, epoch milliseconds — the URL stops working after it.
    pub expires_at: i64,
}

/// Image-transform `fit` mode for [`RtDbHttpClient::transform_url`] (ENH-014).
/// The serde rename is `kebab-case` so `ScaleDown` serializes as `scale-down`
/// on the wire; `as_str` mirrors that for the hand-built query string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Fit {
    #[default]
    /// Fit entirely inside the target box, preserving aspect ratio (default).
    Contain,
    /// Fill the box, cropping the overflow.
    Cover,
    /// Never upscale; downscale as `Contain` would.
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
    /// Let the server pick (default).
    Auto,
    /// JPEG output.
    Jpeg,
    /// PNG output.
    Png,
}

/// Optional image-transform parameters for [`RtDbHttpClient::transform_url`]
/// (ENH-014). All fields are optional; only `Some` fields appear in the query.
#[derive(Debug, Clone, Default)]
pub struct TransformOpts {
    /// Target width in pixels.
    pub w: Option<u32>,
    /// Target height in pixels.
    pub h: Option<u32>,
    /// Resize fit mode.
    pub fit: Option<Fit>,
    /// JPEG quality (1-100).
    pub q: Option<u8>,
    /// Output format.
    pub format: Option<OutFormat>,
}

impl RtDbHttpClient {
    /// Create a client for `db` at `url` authenticated with `token` (a machine
    /// or session bearer token). A trailing `/` on `url` is trimmed.
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
        crate::mutation::parse_step_results(parsed.results)
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

    /// Cancel a scheduled job (`POST /api/schedule/{id}/cancel`). Resolves
    /// `true` on `{ok:true}`; resolves `false` on `{ok:false}` (unknown or
    /// already-terminal id — a no-op, not a failure, matching the WS ack
    /// contract of [`RtDbClient::cancel_schedule`](crate::ws::RtDbClient::cancel_schedule));
    /// rejects with [`RtDbError`] on a non-2xx error envelope.
    pub async fn cancel_schedule(&self, id: &str) -> Result<bool, RtDbError> {
        self.manage_schedule(id, "cancel").await
    }

    /// Pause a scheduled job until [`resume_schedule`](Self::resume_schedule) is
    /// called (`POST /api/schedule/{id}/pause`). Same ack contract as
    /// [`cancel_schedule`](Self::cancel_schedule).
    pub async fn pause_schedule(&self, id: &str) -> Result<bool, RtDbError> {
        self.manage_schedule(id, "pause").await
    }

    /// Resume a paused scheduled job (`POST /api/schedule/{id}/resume`). Same
    /// ack contract as [`cancel_schedule`](Self::cancel_schedule).
    pub async fn resume_schedule(&self, id: &str) -> Result<bool, RtDbError> {
        self.manage_schedule(id, "resume").await
    }

    /// Shared authorize-then-op body for the three boolean manage handlers. `op`
    /// is always a hardcoded literal ("cancel" | "pause" | "resume"), never
    /// caller-supplied, so interpolating it into the path is safe. A 200
    /// `{ok:false}` is the no-op signal — resolves `false`, not an error.
    async fn manage_schedule(&self, id: &str, op: &str) -> Result<bool, RtDbError> {
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
        Ok(parsed.ok)
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

    /// Start a durable workflow run (`POST /api/workflows`, FM-29). Returns
    /// the new run's id. Mirrors `ts-client`'s `startWorkflow`.
    pub async fn start_workflow(&self, spec: &WorkflowSpec) -> Result<String, RtDbError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body<'a> {
            db: &'a str,
            spec: &'a WorkflowSpec,
        }
        let body = Body { db: &self.db, spec };
        let resp = self
            .client
            .post(format!("{}/api/workflows", self.url))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("start workflow request failed: {e}")))?;
        #[derive(serde::Deserialize)]
        struct StartResponse {
            id: String,
        }
        let parsed = self.deserialize::<StartResponse>(resp).await?;
        Ok(parsed.id)
    }

    /// Cancel a workflow run (`POST /api/workflows/{id}/cancel`, FM-29).
    /// Returns `false` for a missing or already-terminal run — a no-op, not
    /// an error.
    pub async fn cancel_workflow(&self, id: &str) -> Result<bool, RtDbError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body<'a> {
            db: &'a str,
        }
        let body = Body { db: &self.db };
        let resp = self
            .client
            // `id` is caller-supplied, so percent-encode the path segment
            // (same guard as `manage_schedule`).
            .post(format!(
                "{}/api/workflows/{}/cancel",
                self.url,
                encode_uri_component(id)
            ))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("cancel workflow request failed: {e}")))?;
        #[derive(serde::Deserialize)]
        struct CancelResponse {
            cancelled: bool,
        }
        let parsed = self.deserialize::<CancelResponse>(resp).await?;
        Ok(parsed.cancelled)
    }

    /// List this database's workflow runs, newest first
    /// (`POST /api/workflows/list`, FM-29). `status` optionally filters by
    /// run state. Mirrors `ts-client`'s `listWorkflows`.
    pub async fn list_workflows(
        &self,
        status: Option<WorkflowStatus>,
    ) -> Result<Vec<WorkflowInfo>, RtDbError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body<'a> {
            db: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            status: Option<WorkflowStatus>,
        }
        #[derive(serde::Deserialize)]
        struct ListResponse {
            workflows: Vec<WorkflowInfo>,
        }
        let body = Body {
            db: &self.db,
            status,
        };
        let resp = self
            .client
            .post(format!("{}/api/workflows/list", self.url))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("list workflows request failed: {e}")))?;
        let parsed = self.deserialize::<ListResponse>(resp).await?;
        Ok(parsed.workflows)
    }

    /// Upload raw bytes; `content_type` sets the Content-Type header and is
    /// stored as the file's type. Returns the server-computed metadata.
    ///
    /// For large files prefer [`Self::upload_stream`], which streams the body
    /// through reqwest instead of buffering it whole in memory.
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

    /// Upload a streaming body without buffering it whole in memory. The server
    /// (ENH-021) streams the request through to storage, so a file of any size
    /// only occupies one chunk in flight at a time.
    ///
    /// `stream` is any `TryStream` whose `Ok` item converts into `bytes::Bytes`
    /// (e.g. `Bytes`, `Vec<u8>`, `&'static [u8]`); chunks are forwarded to
    /// reqwest's streaming body as they arrive. Pass `content_type` to set the
    /// Content-Type header and the file's stored type, exactly like
    /// [`Self::upload`].
    pub async fn upload_stream<S>(
        &self,
        stream: S,
        content_type: Option<&str>,
    ) -> Result<UploadResult, RtDbError>
    where
        S: futures_util::stream::TryStream + Send + 'static,
        bytes::Bytes: From<S::Ok>,
        S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let body = reqwest::Body::wrap_stream(stream);
        let mut req = self
            .client
            .post(format!("{}/api/storage/{}", self.url, self.db))
            .bearer_auth(&self.token)
            .body(body);
        if let Some(ct) = content_type {
            req = req.header(reqwest::header::CONTENT_TYPE, ct);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("upload request failed: {e}")))?;
        self.deserialize::<UploadResult>(resp).await
    }

    /// Delete the file `id` (`DELETE /api/storage/{db}/{id}`) — also revokes
    /// its public serve URL. Idempotent: deleting an unknown id still returns
    /// `Ok(())`.
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

    /// Fetch stored metadata for `id` via `GET /api/storage/{db}/{id}/metadata`.
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

    /// Mint an HMAC-signed, time-limited public URL for `id` via
    /// `GET /api/storage/{db}/{id}/signed-url`. Pass `ttl_seconds` to override
    /// the server default lifetime; `None` omits the query parameter. Unlike
    /// [`get_url`](Self::get_url), this makes a network request.
    pub async fn get_signed_url(
        &self,
        id: &str,
        ttl_seconds: Option<u64>,
    ) -> Result<SignedUrl, RtDbError> {
        let mut req = self
            .client
            .get(format!(
                "{}/api/storage/{}/{id}/signed-url",
                self.url, self.db
            ))
            .bearer_auth(&self.token);
        if let Some(ttl) = ttl_seconds {
            req = req.query(&[("ttlSeconds", ttl)]);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("signed url request failed: {e}")))?;
        self.deserialize::<SignedUrl>(resp).await
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

/// Deprecated admin control-plane methods on [`RtDbHttpClient`]. Each delegates
/// to the canonical [`RtDbAdminClient`](crate::admin::RtDbAdminClient) (ARC-121).
/// Migrate by obtaining an admin client via [`admin_client`](Self::admin_client)
/// and calling the same-named method on it. Gated on the `admin` feature.
#[cfg(feature = "admin")]
impl RtDbHttpClient {
    /// Return a dedicated [`RtDbAdminClient`](crate::admin::RtDbAdminClient) sharing this client's connection
    /// pool. The admin key is the same bearer this client carries. This is the
    /// non-deprecated entry point for admin work; the per-method shims below
    /// exist only for backward compatibility.
    pub fn admin_client(&self) -> crate::admin::RtDbAdminClient {
        crate::admin::RtDbAdminClient::from_parts(&self.url, &self.token, self.client.clone())
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::create_db`](crate::admin::RtDbAdminClient::create_db).
    pub async fn create_db(&self, name: &str) -> Result<(), RtDbError> {
        self.admin_client().create_db(name).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::delete_db`](crate::admin::RtDbAdminClient::delete_db).
    pub async fn delete_db(&self, name: &str, confirm: &str) -> Result<(), RtDbError> {
        self.admin_client().delete_db(name, confirm).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::push_schema`](crate::admin::RtDbAdminClient::push_schema).
    pub async fn push_schema(
        &self,
        db: &str,
        schema: &crate::schema::SchemaDef,
    ) -> Result<(), RtDbError> {
        self.admin_client().push_schema(db, schema).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::migrate_schema`](crate::admin::RtDbAdminClient::migrate_schema).
    pub async fn migrate_schema(
        &self,
        db: &str,
        directives: &[crate::wire::admin::Directive],
        dry_run: bool,
    ) -> Result<crate::wire::admin::MigrateResult, RtDbError> {
        self.admin_client()
            .migrate_schema(db, directives, dry_run)
            .await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::list_dbs`](crate::admin::RtDbAdminClient::list_dbs).
    pub async fn list_dbs(&self) -> Result<Vec<String>, RtDbError> {
        self.admin_client().list_dbs().await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::mint_token`](crate::admin::RtDbAdminClient::mint_token).
    pub async fn mint_token(
        &self,
        db: &str,
        name: &str,
    ) -> Result<crate::wire::admin::MintedToken, RtDbError> {
        self.admin_client().mint_token(db, name).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::mint_token_with_options`](crate::admin::RtDbAdminClient::mint_token_with_options).
    pub async fn mint_token_with_options(
        &self,
        db: &str,
        name: &str,
        opts: &crate::wire::admin::MintTokenOptions,
    ) -> Result<crate::wire::admin::MintedToken, RtDbError> {
        self.admin_client()
            .mint_token_with_options(db, name, opts)
            .await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::revoke_token`](crate::admin::RtDbAdminClient::revoke_token).
    pub async fn revoke_token(&self, token_id: &str) -> Result<(), RtDbError> {
        self.admin_client().revoke_token(token_id).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::allowlist_add`](crate::admin::RtDbAdminClient::allowlist_add).
    pub async fn allowlist_add(&self, db: &str, email: &str) -> Result<(), RtDbError> {
        self.admin_client().allowlist_add(db, email).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::allowlist_remove`](crate::admin::RtDbAdminClient::allowlist_remove).
    pub async fn allowlist_remove(&self, db: &str, email: &str) -> Result<(), RtDbError> {
        self.admin_client().allowlist_remove(db, email).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::allowlist_list`](crate::admin::RtDbAdminClient::allowlist_list).
    pub async fn allowlist_list(&self, db: &str) -> Result<Vec<String>, RtDbError> {
        self.admin_client().allowlist_list(db).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::admins_list`](crate::admin::RtDbAdminClient::admins_list).
    pub async fn admins_list(&self) -> Result<Vec<crate::wire::admin::AdminMember>, RtDbError> {
        self.admin_client().admins_list().await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::admins_add`](crate::admin::RtDbAdminClient::admins_add).
    pub async fn admins_add(&self, email: &str, github_id: Option<i64>) -> Result<(), RtDbError> {
        self.admin_client().admins_add(email, github_id).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::admins_remove`](crate::admin::RtDbAdminClient::admins_remove).
    pub async fn admins_remove(&self, email: &str) -> Result<(), RtDbError> {
        self.admin_client().admins_remove(email).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::export_db`](crate::admin::RtDbAdminClient::export_db).
    pub async fn export_db(&self, db: &str) -> Result<String, RtDbError> {
        self.admin_client().export_db(db).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::import_db`](crate::admin::RtDbAdminClient::import_db).
    pub async fn import_db(&self, db: &str, jsonl: &str) -> Result<(), RtDbError> {
        self.admin_client().import_db(db, jsonl).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::clone_db`](crate::admin::RtDbAdminClient::clone_db).
    pub async fn clone_db(&self, from: &str, to: &str) -> Result<(), RtDbError> {
        self.admin_client().clone_db(from, to).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::get_schema`](crate::admin::RtDbAdminClient::get_schema).
    pub async fn get_schema(&self, db: &str) -> Result<crate::schema::SchemaDef, RtDbError> {
        self.admin_client().get_schema(db).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::schema_history`](crate::admin::RtDbAdminClient::schema_history).
    pub async fn schema_history(
        &self,
        db: &str,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<Vec<crate::wire::admin::SchemaHistorySummary>, RtDbError> {
        self.admin_client().schema_history(db, limit, offset).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::schema_history_get`](crate::admin::RtDbAdminClient::schema_history_get).
    pub async fn schema_history_get(
        &self,
        db: &str,
        version: i64,
    ) -> Result<crate::wire::admin::SchemaHistoryEntry, RtDbError> {
        self.admin_client().schema_history_get(db, version).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::restore_schema`](crate::admin::RtDbAdminClient::restore_schema).
    pub async fn restore_schema(
        &self,
        db: &str,
        version: i64,
        confirm: &str,
    ) -> Result<i64, RtDbError> {
        self.admin_client()
            .restore_schema(db, version, confirm)
            .await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::db_stats`](crate::admin::RtDbAdminClient::db_stats).
    pub async fn db_stats(&self, db: &str) -> Result<crate::wire::admin::DbStats, RtDbError> {
        self.admin_client().db_stats(db).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::list_tokens`](crate::admin::RtDbAdminClient::list_tokens).
    pub async fn list_tokens(
        &self,
        db: &str,
    ) -> Result<Vec<crate::wire::admin::TokenInfo>, RtDbError> {
        self.admin_client().list_tokens(db).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::metrics`](crate::admin::RtDbAdminClient::metrics).
    pub async fn metrics(&self) -> Result<crate::wire::admin::MetricsSnapshot, RtDbError> {
        self.admin_client().metrics().await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::list_subscriptions`](crate::admin::RtDbAdminClient::list_subscriptions).
    pub async fn list_subscriptions(
        &self,
        db: Option<&str>,
    ) -> Result<crate::wire::admin::SubscriptionsResponse, RtDbError> {
        self.admin_client().list_subscriptions(db).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::get_config`](crate::admin::RtDbAdminClient::get_config).
    pub async fn get_config(&self) -> Result<crate::wire::admin::ConfigResponse, RtDbError> {
        self.admin_client().get_config().await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::patch_config`](crate::admin::RtDbAdminClient::patch_config).
    pub async fn patch_config(
        &self,
        patch: &crate::wire::admin::HotConfigPatch,
    ) -> Result<crate::wire::admin::ConfigResponse, RtDbError> {
        self.admin_client().patch_config(patch).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::ops_recent`](crate::admin::RtDbAdminClient::ops_recent).
    pub async fn ops_recent(
        &self,
        db: &str,
        table: Option<&str>,
        n: Option<u32>,
    ) -> Result<Vec<crate::wire::admin::OpEvent>, RtDbError> {
        self.admin_client().ops_recent(db, table, n).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::admin_query`](crate::admin::RtDbAdminClient::admin_query).
    pub async fn admin_query<T: DeserializeOwned>(
        &self,
        db: &str,
        query: &crate::query::Query,
    ) -> Result<T, RtDbError> {
        // Keeps its pre-include_deleted 2-arg signature; the new flag defaults
        // to None (key omitted on the wire).
        self.admin_client().admin_query::<T>(db, query, None).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::admin_mutate`](crate::admin::RtDbAdminClient::admin_mutate).
    pub async fn admin_mutate(
        &self,
        db: &str,
        txn: &Transaction,
        idempotency_key: Option<&str>,
    ) -> Result<Vec<StepResult>, RtDbError> {
        self.admin_client()
            .admin_mutate(db, txn, idempotency_key)
            .await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::backup_now`](crate::admin::RtDbAdminClient::backup_now).
    pub async fn backup_now(&self) -> Result<(), RtDbError> {
        self.admin_client().backup_now().await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::list_backups`](crate::admin::RtDbAdminClient::list_backups).
    pub async fn list_backups(&self) -> Result<crate::wire::admin::BackupsListResponse, RtDbError> {
        self.admin_client().list_backups().await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::download_backup`](crate::admin::RtDbAdminClient::download_backup).
    pub async fn download_backup(&self, name: &str) -> Result<Vec<u8>, RtDbError> {
        self.admin_client().download_backup(name).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::delete_backup`](crate::admin::RtDbAdminClient::delete_backup).
    pub async fn delete_backup(&self, name: &str) -> Result<(), RtDbError> {
        self.admin_client().delete_backup(name).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::restore_backup`](crate::admin::RtDbAdminClient::restore_backup).
    pub async fn restore_backup(
        &self,
        name: &str,
    ) -> Result<crate::wire::admin::RestoreResult, RtDbError> {
        self.admin_client().restore_backup(name).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::list_webhooks`](crate::admin::RtDbAdminClient::list_webhooks).
    pub async fn list_webhooks(
        &self,
        db: &str,
    ) -> Result<Vec<crate::wire::admin::Webhook>, RtDbError> {
        self.admin_client().list_webhooks(db).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::create_webhook`](crate::admin::RtDbAdminClient::create_webhook).
    pub async fn create_webhook(
        &self,
        db: &str,
        opts: &crate::wire::admin::CreateWebhookOptions,
    ) -> Result<i64, RtDbError> {
        self.admin_client().create_webhook(db, opts).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::edit_webhook`](crate::admin::RtDbAdminClient::edit_webhook).
    pub async fn edit_webhook(
        &self,
        db: &str,
        id: i64,
        opts: &crate::wire::admin::WebhookEditOptions,
    ) -> Result<crate::wire::admin::Webhook, RtDbError> {
        self.admin_client().edit_webhook(db, id, opts).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::delete_webhook`](crate::admin::RtDbAdminClient::delete_webhook).
    pub async fn delete_webhook(&self, db: &str, id: i64) -> Result<(), RtDbError> {
        self.admin_client().delete_webhook(db, id).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::list_deliveries`](crate::admin::RtDbAdminClient::list_deliveries).
    pub async fn list_deliveries(
        &self,
        db: &str,
        id: i64,
        opts: Option<&crate::wire::admin::ListDeliveriesOptions>,
    ) -> Result<Vec<crate::wire::admin::WebhookDelivery>, RtDbError> {
        self.admin_client().list_deliveries(db, id, opts).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::get_audit`](crate::admin::RtDbAdminClient::get_audit).
    pub async fn get_audit(
        &self,
        db: &str,
        opts: Option<&crate::wire::admin::AuditQuery>,
    ) -> Result<Vec<crate::wire::admin::AuditEntry>, RtDbError> {
        self.admin_client().get_audit(db, opts).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::list_sessions`](crate::admin::RtDbAdminClient::list_sessions).
    pub async fn list_sessions(
        &self,
        opts: Option<&crate::wire::admin::SessionListOptions>,
    ) -> Result<Vec<crate::wire::admin::SessionInfo>, RtDbError> {
        self.admin_client().list_sessions(opts).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::revoke_session`](crate::admin::RtDbAdminClient::revoke_session).
    pub async fn revoke_session(&self, token_hash: &str) -> Result<(), RtDbError> {
        self.admin_client().revoke_session(token_hash).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::revoke_user_sessions`](crate::admin::RtDbAdminClient::revoke_user_sessions).
    pub async fn revoke_user_sessions(
        &self,
        user_id: &str,
    ) -> Result<crate::wire::admin::RevokeUserSessionsResponse, RtDbError> {
        self.admin_client().revoke_user_sessions(user_id).await
    }

    #[deprecated(note = "use RtDbAdminClient")]
    /// Deprecated ARC-121 shim — delegates to
    /// [`RtDbAdminClient::merge_users`](crate::admin::RtDbAdminClient::merge_users).
    pub async fn merge_users(
        &self,
        anon_user_id: &str,
        real_user_id: &str,
    ) -> Result<crate::wire::admin::MergeReport, RtDbError> {
        self.admin_client()
            .merge_users(anon_user_id, real_user_id)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutation::Mutation;
    use crate::query::TableQuery;
    use crate::wire::UserKind;
    use serde_json::{Value, json};
    use wiremock::matchers::{body_bytes, body_partial_json, header, method, path, query_param};
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
        assert!(client.cancel_schedule("job-1").await.unwrap());
        assert!(client.pause_schedule("job-1").await.unwrap());
        assert!(client.resume_schedule("job-1").await.unwrap());
    }

    #[tokio::test]
    async fn schedule_manage_ok_false_resolves_false_and_errors_reject() {
        let (server, client) = setup().await;
        // 200 {ok:false} = unknown/terminal id: the no-op signal, resolves
        // Ok(false) (same contract as the WS bare ack).
        Mock::given(method("POST"))
            .and(path("/api/schedule/job-9/cancel"))
            .and(body_partial_json(json!({"db": "t<uuid>"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": false})))
            .mount(&server)
            .await;
        // A real failure arrives as a non-2xx error envelope and still rejects.
        Mock::given(method("POST"))
            .and(path("/api/schedule/job-8/cancel"))
            .and(body_partial_json(json!({"db": "t<uuid>"})))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({
                "code": "NOT_FOUND", "message": "no such job"
            })))
            .mount(&server)
            .await;
        assert!(!client.cancel_schedule("job-9").await.unwrap());
        let err = client.cancel_schedule("job-8").await.unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::NotFound);
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
    async fn start_workflow_posts_db_and_spec_and_returns_id() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/api/workflows"))
            .and(header("authorization", "Bearer machine-token"))
            .and(body_partial_json(json!({
                "db": "t<uuid>",
                "spec": {
                    "name": "drip",
                    "steps": [ { "txn": { "steps": [] } } ]
                }
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "wf-7"})))
            .mount(&server)
            .await;
        let spec = crate::wire::WorkflowSpec {
            name: "drip".into(),
            steps: vec![crate::wire::WorkflowStepSpec {
                txn: Mutation::new().build(),
                retry: None,
                sleep_before_ms: None,
            }],
        };
        let id = client.start_workflow(&spec).await.unwrap();
        assert_eq!(id, "wf-7");
    }

    #[tokio::test]
    async fn cancel_workflow_posts_db_body_and_returns_cancelled() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/api/workflows/wf-1/cancel"))
            .and(body_partial_json(json!({"db": "t<uuid>"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"cancelled": false})))
            .mount(&server)
            .await;
        // `false` (missing or already-terminal run) is a no-op, not an error.
        assert!(!client.cancel_workflow("wf-1").await.unwrap());
    }

    #[tokio::test]
    async fn list_workflows_posts_optional_status_filter() {
        let (server, client) = setup().await;
        // Some(status) serializes as a snake_case filter key in the body.
        Mock::given(method("POST"))
            .and(path("/api/workflows/list"))
            .and(header("authorization", "Bearer machine-token"))
            .and(body_partial_json(
                json!({"db": "t<uuid>", "status": "failed"}),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"workflows": []})))
            .mount(&server)
            .await;
        // None omits the key entirely — exact raw body, not a partial match.
        Mock::given(method("POST"))
            .and(path("/api/workflows/list"))
            .and(body_bytes(
                serde_json::to_string(&json!({"db": "t<uuid>"}))
                    .unwrap()
                    .into_bytes(),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "workflows": [{
                    "id": "wf1", "name": "drip", "status": "success",
                    "currentStep": 2, "stepCount": 2, "attempts": 1,
                    "createdAt": 1, "updatedAt": 9, "finishedAt": 9
                }]
            })))
            .mount(&server)
            .await;
        let failed = client
            .list_workflows(Some(crate::wire::WorkflowStatus::Failed))
            .await
            .unwrap();
        assert!(failed.is_empty());
        let all = client.list_workflows(None).await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].id, "wf1");
        assert_eq!(all[0].status, crate::wire::WorkflowStatus::Success);
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

    // ENH-021 mirror: `upload_stream` posts the concatenated chunk bytes as the
    // raw request body (same wire contract as `upload`) without buffering the
    // whole file in memory. Two chunks exercise the multi-item path; wiremock
    // concatenates them into the body the matcher sees, and the byte-for-byte
    // match proves the streaming path lands on the identical route / headers /
    // body the server expects.
    #[tokio::test]
    async fn upload_stream_posts_chunked_body_and_returns_metadata() {
        use bytes::Bytes;
        use futures_util::stream;
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/api/storage/t%3Cuuid%3E"))
            .and(header("authorization", "Bearer machine-token"))
            .and(header("content-type", "image/png"))
            .and(body_bytes("chunked-bytes"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "s1", "sha256": "def", "size": 13, "contentType": "image/png"
            })))
            .mount(&server)
            .await;
        let chunks: Vec<Result<Bytes, std::io::Error>> = vec![
            Ok(Bytes::from_static(b"chunked-")),
            Ok(Bytes::from_static(b"bytes")),
        ];
        let up = client
            .upload_stream(stream::iter(chunks), Some("image/png"))
            .await
            .unwrap();
        assert_eq!(up.id, "s1");
        assert_eq!(up.size, 13);
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

    #[tokio::test]
    async fn get_signed_url_builds_get_with_optional_ttl() {
        let (server, client) = setup().await;
        // With ttl — asserts the query param is sent.
        Mock::given(method("GET"))
            .and(path("/api/storage/t%3Cuuid%3E/f1/signed-url"))
            .and(query_param("ttlSeconds", "120"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "url": "http://x/storage/f1?exp=9&sig=ab", "expiresAt": 9
            })))
            .mount(&server)
            .await;
        let r = client.get_signed_url("f1", Some(120)).await.unwrap();
        assert_eq!(r.url, "http://x/storage/f1?exp=9&sig=ab");
        assert_eq!(r.expires_at, 9);

        // Without ttl — no query param matcher, still matches by path.
        Mock::given(method("GET"))
            .and(path("/api/storage/t%3Cuuid%3E/f1/signed-url"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "url": "u", "expiresAt": 7
            })))
            .mount(&server)
            .await;
        let r = client.get_signed_url("f1", None).await.unwrap();
        assert_eq!(r.expires_at, 7);
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

    #[test]
    fn transform_url_scale_down_and_jpeg() {
        let client = RtDbHttpClient::new("https://rtdb.example", "db", "tok");
        let url = client.transform_url(
            "f1",
            &TransformOpts {
                w: None,
                h: Some(200),
                fit: Some(Fit::ScaleDown),
                q: None,
                format: Some(OutFormat::Jpeg),
            },
        );
        assert_eq!(
            url,
            "https://rtdb.example/storage/f1?h=200&fit=scale-down&format=jpeg"
        );
    }

    #[test]
    fn transform_url_png_omits_unset_params() {
        let client = RtDbHttpClient::new("https://rtdb.example", "db", "tok");
        let url = client.transform_url(
            "f1",
            &TransformOpts {
                w: None,
                h: None,
                fit: None,
                q: None,
                format: Some(OutFormat::Png),
            },
        );
        assert_eq!(url, "https://rtdb.example/storage/f1?format=png");
    }
}
