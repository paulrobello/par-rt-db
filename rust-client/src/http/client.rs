use super::RtDbHttpClient;
use crate::error::{RtDbError, retry_on_precondition};
use crate::http_common::AuthedRequest;
use crate::mutation::{Mutation, StepResult, Transaction};
use crate::query::{Query, TableQuery};
use crate::wire::AuthedUser;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::future::Future;

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
            .authed(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("query request failed: {e}")))?;
        crate::http_common::json_result::<T>(resp).await
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
            .authed(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("batch query request failed: {e}")))?;
        #[derive(serde::Deserialize)]
        struct BatchResponse {
            results: Vec<crate::wire::BatchQueryOutcome>,
        }
        let parsed = crate::http_common::deserialize::<BatchResponse>(resp).await?;
        Ok(parsed.results)
    }

    /// Run independent transactions sequentially through `/api/mutate-batch`.
    /// Each response slot is aligned with the input transaction and may fail
    /// without rolling back other slots.
    pub async fn batch_mutate(
        &self,
        txns: &[(&Transaction, Option<&str>)],
    ) -> Result<Vec<crate::wire::BatchMutateOutcome>, RtDbError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body<'a> {
            db: &'a str,
            txns: Vec<crate::wire::BatchMutateRequest<'a>>,
        }
        let body = Body {
            db: &self.db,
            txns: txns
                .iter()
                .map(|(txn, key)| crate::wire::BatchMutateRequest {
                    txn,
                    idempotency_key: *key,
                })
                .collect(),
        };
        let resp = self
            .client
            .post(format!("{}/api/mutate-batch", self.url))
            .authed(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("batch mutate request failed: {e}")))?;
        #[derive(serde::Deserialize)]
        struct BatchResponse {
            results: Vec<crate::wire::BatchMutateOutcome>,
        }
        Ok(crate::http_common::deserialize::<BatchResponse>(resp)
            .await?
            .results)
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
            .authed(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("mutate request failed: {e}")))?;
        #[derive(serde::Deserialize)]
        struct MutateResponse {
            results: Vec<serde_json::Value>,
        }
        let parsed = crate::http_common::deserialize::<MutateResponse>(resp).await?;
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
    /// if it never succeeds. Pass [`DEFAULT_MUTATE_MAX_ATTEMPTS`](super::DEFAULT_MUTATE_MAX_ATTEMPTS) for the
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
            .authed(&self.token)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("auth_me request failed: {e}")))?;
        #[derive(serde::Deserialize)]
        struct MeResponse {
            user: AuthedUser,
        }
        let parsed = crate::http_common::deserialize::<MeResponse>(resp).await?;
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
            .authed(token)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("validate request failed: {e}")))?;
        #[derive(serde::Deserialize)]
        struct ValidateResponse {
            user: AuthedUser,
        }
        let parsed = crate::http_common::deserialize::<ValidateResponse>(resp).await?;
        Ok(parsed.user)
    }

    /// Redeem a one-time enrollment grant via `POST /auth/enroll` — the
    /// public, unauthenticated redemption side of host machine
    /// authorization. Takes ONLY the grant string: the server denies unknown
    /// fields, so the database and capability are fixed at grant-creation
    /// time and cannot be overridden here. Returns the minted machine
    /// token (`tokenId`/`token` — shown once), its database, and the
    /// capability the grant fixed. An invalid/expired/revoked/used grant
    /// surfaces as the generic `UNAUTHORIZED` envelope.
    pub async fn enroll(
        &self,
        grant: &str,
    ) -> Result<crate::wire::enrollment::RedeemedGrant, RtDbError> {
        let resp = self
            .client
            .post(format!("{}/auth/enroll", self.url))
            .json(&crate::wire::enrollment::EnrollRequest {
                grant: grant.to_string(),
            })
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("enroll request failed: {e}")))?;
        crate::http_common::deserialize::<crate::wire::enrollment::RedeemedGrant>(resp).await
    }

    /// Fetch the database's pushed `SchemaDef` via `GET /api/db/{db}/schema` —
    /// the machine-token twin of the admin client's
    /// [`get_schema`](crate::admin::RtDbAdminClient::get_schema). Serves
    /// tooling that holds only a data-plane credential (the CLI's
    /// `import --dry-run` validates lines against it). Named `fetch_schema`
    /// because the deprecated [`get_schema`](Self::get_schema) shim already
    /// occupies that name on this type (admin route, admin key).
    pub async fn fetch_schema(&self) -> Result<crate::schema::SchemaDef, RtDbError> {
        let resp = self
            .client
            .get(format!("{}/api/db/{}/schema", self.url, self.db))
            .authed(&self.token)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("fetch_schema request failed: {e}")))?;
        crate::http_common::deserialize::<crate::schema::SchemaDef>(resp).await
    }
}
