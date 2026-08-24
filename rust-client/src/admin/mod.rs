//! Admin control-plane client for par-rt-db (`/admin/*`). The bearer token
//! must be the instance admin key for these to authorize (constant-time compared
//! server side, same as every other bearer call). Gated on the `admin` feature,
//! which implies `http`. Mirrors `ts-client`'s `RtDbAdminClient` one-to-one —
//! paths, bodies, and return shapes are identical; only the method names are
//! snake_cased.
//!
//! Separated from [`RtDbHttpClient`](crate::RtDbHttpClient) (the data-plane
//! client) at ARC-121 so the control plane has its own type, matching ts-client
//! and python. The deprecated admin methods on `RtDbHttpClient` delegate here.

use crate::error::{ErrorEnvelope, RtDbError};
use crate::mutation::{StepResult, Transaction};
use crate::query::parse_result;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Admin control-plane client (`/admin/*`). Owns its own `reqwest::Client` (or
/// one shared from [`RtDbHttpClient::admin_client`](crate::RtDbHttpClient::admin_client))
/// plus the instance URL and admin-key bearer. Every call sends
/// `Authorization: Bearer <admin key>`.
///
/// Construct directly with [`RtDbAdminClient::new`], or from an existing
/// [`RtDbHttpClient`](crate::RtDbHttpClient) via
/// [`RtDbHttpClient::admin_client`](crate::RtDbHttpClient::admin_client) (which
/// shares the underlying connection pool).
pub struct RtDbAdminClient {
    url: String,
    token: String,
    client: reqwest::Client,
}

impl RtDbAdminClient {
    /// Create a standalone admin client. `admin_key` is the instance admin key
    /// (the same value `RtDbHttpClient::new` would carry as its `token` for an
    /// admin caller).
    pub fn new(url: &str, admin_key: &str) -> Self {
        Self {
            url: url.trim_end_matches('/').to_string(),
            token: admin_key.to_string(),
            client: reqwest::Client::new(),
        }
    }

    /// Create an admin client that reuses an existing `reqwest::Client` (the
    /// connection pool is shared, so this is cheap). Used by
    /// [`RtDbHttpClient::admin_client`](crate::RtDbHttpClient::admin_client).
    pub fn from_parts(url: &str, admin_key: &str, client: reqwest::Client) -> Self {
        Self {
            url: url.trim_end_matches('/').to_string(),
            token: admin_key.to_string(),
            client,
        }
    }
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

    /// `POST /admin/db/{db}/schema/preview` `{schema}` → `SchemaPreviewDiff`.
    /// Pure/advisory — validates the pending schema and diffs it against the
    /// currently-applied one WITHOUT applying anything: `added` lists every new
    /// table/column/index an additive-only push would create, `rejected` lists
    /// every drop or type change the DDL layer would refuse (`push_schema`
    /// remains the authoritative gate). Same body shape as
    /// [`push_schema`](Self::push_schema) minus the `db` key (it rides the
    /// path). Mirrors `ts-client`'s `previewSchema` one-to-one.
    pub async fn preview_schema(
        &self,
        db: &str,
        schema: &crate::schema::SchemaDef,
    ) -> Result<crate::wire::admin::SchemaPreviewDiff, RtDbError> {
        let resp = self
            .post_json(
                &format!("/admin/db/{db}/schema/preview"),
                &crate::wire::admin::PreviewSchemaRequest { schema },
            )
            .await?;
        self.deserialize::<crate::wire::admin::SchemaPreviewDiff>(resp)
            .await
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
    /// `{tokenId, token}`. Fields left `None` on
    /// [`MintTokenOptions`](crate::wire::admin::MintTokenOptions) are omitted
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

    /// `POST /admin/clone-db?from=<from>&to=<to>` → `{ok:true}`. Clones `from`
    /// (schema + documents) into a freshly created `to` in one server-side step
    /// (see server `admin::dbs::clone_db`, ENH-009). `to` must not already exist;
    /// scope matches [`export_db`](Self::export_db)/[`import_db`](Self::import_db)
    /// — storage blobs and scheduled transactions are not copied.
    pub async fn clone_db(&self, from: &str, to: &str) -> Result<(), RtDbError> {
        let resp = self
            .client
            .post(format!("{}/admin/clone-db", self.url))
            .bearer_auth(&self.token)
            .query(&[("from", from), ("to", to)])
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("clone_db request failed: {e}")))?;
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

    /// `POST /admin/db/{db}/query` `{query, includeDeleted?}` → `{result}`.
    /// Owner-bypass: an admin reads documents across every database regardless
    /// of `ownerField`. Mirrors [`run`](crate::http::RtDbHttpClient::run) but routes through the admin
    /// path with `db` in the URL (singular `db`, not the plural `dbs` of
    /// `get_schema`), so the body omits `db`. Deserialize `{result}` into `T`
    /// the same way `run` does.
    ///
    /// `include_deleted` is an internal admin-route parameter, NOT a wire
    /// `Query` field: `Some(true)` surfaces soft-deleted (FM-33 `deleted_at`)
    /// rows so an operator can see them; `None` (the default) omits the key
    /// entirely so the server's live-rows-only default applies.
    pub async fn admin_query<T: DeserializeOwned>(
        &self,
        db: &str,
        query: &crate::query::Query,
        include_deleted: Option<bool>,
    ) -> Result<T, RtDbError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body<'a> {
            query: &'a crate::query::Query,
            #[serde(skip_serializing_if = "Option::is_none")]
            include_deleted: Option<bool>,
        }
        let resp = self
            .post_json(
                &format!("/admin/db/{}/query", db),
                &Body {
                    query,
                    include_deleted,
                },
            )
            .await?;
        self.json_result::<T>(resp).await
    }

    /// `POST /admin/db/{db}/explain` `{query}` → `{sql, params, terminal,
    /// warnings}` (ENH-019). Compiles a Query DSL body for inspection without
    /// executing it; the returned `sql` is byte-identical to what the read
    /// path would run. Mirrors server `admin::observability::explain_query`.
    pub async fn explain_query(
        &self,
        db: &str,
        query: &crate::query::Query,
    ) -> Result<crate::wire::admin::ExplainResult, RtDbError> {
        #[derive(Serialize)]
        struct Body<'a> {
            query: &'a crate::query::Query,
        }
        let resp = self
            .post_json(&format!("/admin/db/{}/explain", db), &Body { query })
            .await?;
        self.deserialize(resp).await
    }

    /// `GET /admin/slow-queries?db=<optional>&limit=<n>` → the slow-query log
    /// (ENH-019). Returns the bounded in-memory ring newest-first, optionally
    /// filtered by database. Mirrors server
    /// `admin::observability::list_slow_queries`. Pass `None` for both args
    /// for the unfiltered instance-wide ring.
    pub async fn get_slow_queries(
        &self,
        db: Option<&str>,
        limit: Option<u32>,
    ) -> Result<crate::wire::admin::SlowQueriesResponse, RtDbError> {
        let db_s = db.map(|d| d.to_string());
        let limit_s = limit.map(|n| n.to_string());
        let mut q: Vec<(&str, &str)> = Vec::with_capacity(2);
        if let Some(ref d) = db_s {
            q.push(("db", d.as_str()));
        }
        if let Some(ref n) = limit_s {
            q.push(("limit", n.as_str()));
        }
        self.get_json("/admin/slow-queries", &q).await
    }

    /// `POST /admin/db/{db}/mutate` `{txn, idempotencyKey?}` → `{results}`.
    /// Owner-bypass: an admin writes documents across every database regardless
    /// of `ownerField`. Mirrors [`mutate`](crate::http::RtDbHttpClient::mutate) but routes through the
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
        crate::mutation::parse_step_results(parsed.results)
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

    // ── Interactive-session management (GET/DELETE /admin/sessions) ──
    //
    // Mirror `ts-client`'s `listSessions`/`revokeSession`/`revokeUserSessions`
    // one-to-one — paths, query params, and return shapes are identical; only
    // the method names are snake_cased. Each call carries the admin-key bearer
    // like every other admin method.

    /// `GET /admin/sessions?user=&limit=` → `{sessions:[...]}`, newest-first.
    /// `opts = None` lists every session server-wide (server default limit 200,
    /// clamped to `[1, 1000]`). `opts.user` filters by user id or email.
    pub async fn list_sessions(
        &self,
        opts: Option<&crate::wire::admin::SessionListOptions>,
    ) -> Result<Vec<crate::wire::admin::SessionInfo>, RtDbError> {
        let user_s = opts.and_then(|o| o.user.clone());
        let limit_s = opts.and_then(|o| o.limit).map(|n| n.to_string());
        let mut q: Vec<(&str, &str)> = Vec::with_capacity(2);
        if let Some(ref v) = user_s {
            q.push(("user", v.as_str()));
        }
        if let Some(ref v) = limit_s {
            q.push(("limit", v.as_str()));
        }
        let parsed: crate::wire::admin::SessionsResponse =
            self.get_json("/admin/sessions", &q).await?;
        Ok(parsed.sessions)
    }

    /// `DELETE /admin/sessions/{tokenHash}` → `{ok:true}`. Revokes a single
    /// session by its non-reversible sha256 digest.
    pub async fn revoke_session(&self, token_hash: &str) -> Result<(), RtDbError> {
        let resp = self
            .client
            .delete(format!("{}/admin/sessions/{token_hash}", self.url))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("revoke_session request failed: {e}")))?;
        self.expect_ok(resp).await
    }

    /// `DELETE /admin/sessions?user={userId}` → `{ok, revoked}`. Revokes every
    /// session for a user; `revoked` is the count of sessions dropped.
    pub async fn revoke_user_sessions(
        &self,
        user_id: &str,
    ) -> Result<crate::wire::admin::RevokeUserSessionsResponse, RtDbError> {
        let resp = self
            .client
            .delete(format!("{}/admin/sessions", self.url))
            .bearer_auth(&self.token)
            .query(&[("user", user_id)])
            .send()
            .await
            .map_err(|e| {
                RtDbError::internal(format!("revoke_user_sessions request failed: {e}"))
            })?;
        let status = resp.status();
        if status.is_success() {
            return self
                .deserialize::<crate::wire::admin::RevokeUserSessionsResponse>(resp)
                .await;
        }
        Err(self.error_response(resp).await)
    }

    /// `DELETE /admin/sessions?expired=true` → `{ok, revoked}`. Revokes every
    /// EXPIRED session instance-wide (OAuth/anonymous and admin-key login rows
    /// alike); `revoked` is the count of sessions dropped.
    pub async fn revoke_expired_sessions(
        &self,
    ) -> Result<crate::wire::admin::RevokeUserSessionsResponse, RtDbError> {
        let resp = self
            .client
            .delete(format!("{}/admin/sessions", self.url))
            .bearer_auth(&self.token)
            .query(&[("expired", "true")])
            .send()
            .await
            .map_err(|e| {
                RtDbError::internal(format!("revoke_expired_sessions request failed: {e}"))
            })?;
        let status = resp.status();
        if status.is_success() {
            return self
                .deserialize::<crate::wire::admin::RevokeUserSessionsResponse>(resp)
                .await;
        }
        Err(self.error_response(resp).await)
    }

    // ── Anon→real account merge (POST /admin/merge-users) ────────────────────
    //
    // Mirror `ts-client`'s `mergeUsers` one-to-one — path, body, and return
    // shape are identical; only the method name is snake_cased.

    /// `POST /admin/merge-users` `{anonUserId, realUserId, confirm}` →
    /// `MergeReport`. Runs the anon→real account merge synchronously (FM-27's
    /// admin escape hatch). The server's typed guard is applied for you:
    /// `confirm` is sent as `realUserId` (same pattern as `delete_db`). A 404
    /// means the anon user row does not exist (nothing to merge).
    pub async fn merge_users(
        &self,
        anon_user_id: &str,
        real_user_id: &str,
    ) -> Result<crate::wire::admin::MergeReport, RtDbError> {
        let resp = self
            .post_json(
                "/admin/merge-users",
                &crate::wire::admin::MergeUsersRequest {
                    anon_user_id,
                    real_user_id,
                    confirm: real_user_id,
                },
            )
            .await?;
        self.deserialize::<crate::wire::admin::MergeReport>(resp)
            .await
    }

    // ── Workflow-run management (FM-29:
    //     GET|POST /admin/db/{db}/workflows, GET|DELETE /admin/db/{db}/workflows/{id},
    //     POST /admin/db/{db}/workflows/{id}/cancel) ────────────────────────────
    //
    // Mirror `ts-client`'s `adminListWorkflows`/`adminGetWorkflow`/
    // `adminStartWorkflow`/`adminCancelWorkflow`/`adminDeleteWorkflow`
    // one-to-one — paths, bodies, and return shapes are identical; only the
    // method names are snake_cased.

    /// `GET /admin/db/{db}/workflows?status=&limit=` → `{workflows:[...]}`,
    /// newest first. `opts = None` for the server-default first page (limit
    /// default 100, capped at 500, no status filter).
    pub async fn list_workflows(
        &self,
        db: &str,
        opts: Option<&crate::wire::admin::WorkflowListOptions>,
    ) -> Result<Vec<crate::wire::WorkflowInfo>, RtDbError> {
        let status_s = opts
            .and_then(|o| o.status)
            .map(|s| s.as_wire_str().to_string());
        let limit_s = opts.and_then(|o| o.limit).map(|n| n.to_string());
        let mut q: Vec<(&str, &str)> = Vec::with_capacity(2);
        if let Some(ref v) = status_s {
            q.push(("status", v.as_str()));
        }
        if let Some(ref v) = limit_s {
            q.push(("limit", v.as_str()));
        }
        let parsed: crate::wire::admin::WorkflowsResponse = self
            .get_json(&format!("/admin/db/{db}/workflows"), &q)
            .await?;
        Ok(parsed.workflows)
    }

    /// `GET /admin/db/{db}/workflows/{id}` → one full run row: the info
    /// projection plus the per-step outcome trail (`WorkflowInfoFull`).
    pub async fn get_workflow(
        &self,
        db: &str,
        id: &str,
    ) -> Result<crate::wire::WorkflowInfoFull, RtDbError> {
        self.get_json(&format!("/admin/db/{db}/workflows/{id}"), &[])
            .await
    }

    /// `POST /admin/db/{db}/workflows` with the bare `WorkflowSpec` body (no
    /// wrapper) → `{id}`. Returns the new run's id.
    pub async fn start_workflow(
        &self,
        db: &str,
        spec: &crate::wire::WorkflowSpec,
    ) -> Result<String, RtDbError> {
        let resp = self
            .post_json(&format!("/admin/db/{db}/workflows"), spec)
            .await?;
        #[derive(serde::Deserialize)]
        struct Resp {
            id: String,
        }
        Ok(self.deserialize::<Resp>(resp).await?.id)
    }

    /// `POST /admin/db/{db}/workflows/{id}/cancel` → `{ok}`. `Ok(false)` = an
    /// unknown or already-terminal run (a no-op, not an error).
    pub async fn cancel_workflow(&self, db: &str, id: &str) -> Result<bool, RtDbError> {
        let resp = self
            .client
            .post(format!("{}/admin/db/{db}/workflows/{id}/cancel", self.url))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("cancel_workflow request failed: {e}")))?;
        let parsed: crate::wire::admin::OkResponse = self.deserialize(resp).await?;
        Ok(parsed.ok)
    }

    /// `POST /admin/db/{db}/workflows/{id}/signal` with body `{name, payload?}`
    /// → `{ok}` — deliver a named signal to a waiting run (`awaitSignal`
    /// steps). `Ok(true)` = delivered; typed 404/409 rejections (unknown run,
    /// not waiting, name mismatch) surface as [`RtDbError`]. `payload` is
    /// latest-wins and rides the step's outcome as `signal`.
    pub async fn signal_workflow(
        &self,
        db: &str,
        id: &str,
        name: &str,
        payload: Option<&serde_json::Value>,
    ) -> Result<bool, RtDbError> {
        // Transport errors are already mapped inside `post_json`; the typed
        // 404/409 envelopes surface through `deserialize`.
        let resp = self
            .post_json(
                &format!("/admin/db/{db}/workflows/{id}/signal"),
                &crate::wire::admin::WorkflowSignalRequest { name, payload },
            )
            .await?;
        let parsed: crate::wire::admin::OkResponse = self.deserialize(resp).await?;
        Ok(parsed.ok)
    }

    /// `DELETE /admin/db/{db}/workflows/{id}` → `{ok}`. Hard-deletes the run
    /// row — unlike cancel, the outcome trail does not survive. `Ok(false)`
    /// when already gone.
    pub async fn delete_workflow(&self, db: &str, id: &str) -> Result<bool, RtDbError> {
        let resp = self
            .client
            .delete(format!("{}/admin/db/{db}/workflows/{id}", self.url))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("delete_workflow request failed: {e}")))?;
        let parsed: crate::wire::admin::OkResponse = self.deserialize(resp).await?;
        Ok(parsed.ok)
    }

    // ── Admin schedule management
    //     (GET|POST /admin/db/{db}/schedules,
    //      POST /admin/db/{db}/schedules/{id}/cancel|pause|resume) ───────────
    //
    // Mirror `ts-client`'s `adminListSchedules`/`adminCreateSchedule`/
    // `adminCancelSchedule`/`adminPauseSchedule`/`adminResumeSchedule`
    // one-to-one — paths, bodies, and return shapes are identical; only the
    // method names are snake_cased. Reuses the wire `ScheduleInfo`/
    // `ScheduleWhen` and the DSL `Transaction` types the client already
    // carries.

    /// `GET /admin/db/{db}/schedules` → `{schedules:[...]}`. Lists every
    /// pending and in-flight scheduled job for the database (the admin view
    /// spans all principals).
    pub async fn list_schedules(
        &self,
        db: &str,
    ) -> Result<Vec<crate::wire::ScheduleInfo>, RtDbError> {
        #[derive(serde::Deserialize)]
        struct Resp {
            schedules: Vec<crate::wire::ScheduleInfo>,
        }
        Ok(self
            .get_json::<Resp>(&format!("/admin/db/{db}/schedules"), &[])
            .await?
            .schedules)
    }

    /// `POST /admin/db/{db}/schedules` `{when, txn}` → `{id}`. Registers a
    /// scheduled job through the admin surface (the same enqueue the
    /// `Schedule` mutation step and the WS `schedule` frame use). Returns the
    /// new job's server-assigned id.
    pub async fn create_schedule(
        &self,
        db: &str,
        when: crate::wire::ScheduleWhen,
        txn: &Transaction,
    ) -> Result<String, RtDbError> {
        #[derive(Serialize)]
        struct Body<'a> {
            when: crate::wire::ScheduleWhen,
            txn: &'a Transaction,
        }
        #[derive(serde::Deserialize)]
        struct Resp {
            id: String,
        }
        let resp = self
            .post_json(&format!("/admin/db/{db}/schedules"), &Body { when, txn })
            .await?;
        Ok(self.deserialize::<Resp>(resp).await?.id)
    }

    /// `POST /admin/db/{db}/schedules/{id}/cancel` → `{ok}`. `Ok(false)` = an
    /// unknown or already-fired id (a no-op, not an error).
    pub async fn cancel_schedule(&self, db: &str, id: &str) -> Result<bool, RtDbError> {
        self.manage_schedule(db, id, "cancel").await
    }

    /// `POST /admin/db/{db}/schedules/{id}/pause` → `{ok}`. `Ok(false)` = an
    /// unknown or non-pausable id (a no-op, not an error).
    pub async fn pause_schedule(&self, db: &str, id: &str) -> Result<bool, RtDbError> {
        self.manage_schedule(db, id, "pause").await
    }

    /// `POST /admin/db/{db}/schedules/{id}/resume` → `{ok}`. `Ok(false)` = an
    /// unknown or non-paused id (a no-op, not an error).
    pub async fn resume_schedule(&self, db: &str, id: &str) -> Result<bool, RtDbError> {
        self.manage_schedule(db, id, "resume").await
    }

    // Shared bodyless-POST helper for the three manage ops (`cancel`/`pause`/
    // `resume`) — the server's manage endpoints take the id + op from the
    // path and no body, and ack `{ok: bool}` where `ok=false` means "unknown
    // or terminal id" (a no-op). `op` is only ever one of the three literals
    // above, each a path segment the server routes on.
    async fn manage_schedule(
        &self,
        db: &str,
        id: &str,
        op: &'static str,
    ) -> Result<bool, RtDbError> {
        let resp = self
            .client
            .post(format!("{}/admin/db/{db}/schedules/{id}/{op}", self.url))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("manage_schedule request failed: {e}")))?;
        let parsed: crate::wire::admin::OkResponse = self.deserialize(resp).await?;
        Ok(parsed.ok)
    }

    // ── Admin file storage
    //     (GET|POST /admin/db/{db}/storage, DELETE /admin/db/{db}/storage/{id}) ──
    //
    // Mirror `ts-client`'s `adminListFiles`/`adminUploadFile`/`adminDeleteFile`
    // one-to-one — same paths, same raw-byte upload body, same `{files}`/`{id}`/
    // `{ok}` response shapes; only the method names are snake_cased. Reuses
    // `FileMetadata` from `http` (admin implies http).

    /// `GET /admin/db/{db}/storage` → `{files:[...]}`. Lists every blob the
    /// database owns (the admin view spans all principals).
    pub async fn list_files(&self, db: &str) -> Result<Vec<crate::http::FileMetadata>, RtDbError> {
        #[derive(serde::Deserialize)]
        struct Resp {
            files: Vec<crate::http::FileMetadata>,
        }
        Ok(self
            .get_json::<Resp>(&format!("/admin/db/{db}/storage"), &[])
            .await?
            .files)
    }

    /// `POST /admin/db/{db}/storage` with the RAW bytes as the body (not
    /// JSON) → `{id}`. `content_type` sets the `Content-Type` header; when
    /// `None` no header is sent and the server stores the blob untyped.
    /// Returns the new blob's server-assigned id.
    pub async fn upload_file(
        &self,
        db: &str,
        bytes: &[u8],
        content_type: Option<&str>,
    ) -> Result<String, RtDbError> {
        #[derive(serde::Deserialize)]
        struct Resp {
            id: String,
        }
        let mut req = self
            .client
            .post(format!("{}/admin/db/{db}/storage", self.url))
            .bearer_auth(&self.token)
            .body(bytes.to_vec());
        if let Some(ct) = content_type {
            req = req.header(reqwest::header::CONTENT_TYPE, ct);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("upload_file request failed: {e}")))?;
        Ok(self.deserialize::<Resp>(resp).await?.id)
    }

    /// `DELETE /admin/db/{db}/storage/{id}` → `{ok:true}`. Idempotent — the
    /// server acks ok even when the blob is already gone.
    pub async fn delete_file(&self, db: &str, id: &str) -> Result<(), RtDbError> {
        let resp = self
            .client
            .delete(format!("{}/admin/db/{db}/storage/{id}", self.url))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("delete_file request failed: {e}")))?;
        self.expect_ok(resp).await
    }

    // ── Anonymous-access toggle (SEC-103:
    //     GET|PATCH /admin/db/{db}/anonymous-access) ───────────────────────────
    //
    // Mirror `ts-client`'s `getAnonymousAccess`/`setAnonymousAccess`
    // one-to-one — same paths and `{enabled}` shapes; only the method names
    // are snake_cased.

    /// `GET /admin/db/{db}/anonymous-access` → `{enabled: bool}`. This is the
    /// per-database flag only — the instance-wide
    /// `RTDB_AUTH_ANONYMOUS_ENABLED` boot gate is separate and always applies
    /// on top (both must allow for an anonymous sign-in to succeed).
    pub async fn get_anonymous_access(&self, db: &str) -> Result<bool, RtDbError> {
        #[derive(serde::Deserialize)]
        struct Resp {
            enabled: bool,
        }
        Ok(self
            .get_json::<Resp>(&format!("/admin/db/{db}/anonymous-access"), &[])
            .await?
            .enabled)
    }

    /// `PATCH /admin/db/{db}/anonymous-access` `{enabled}` → `{ok:true}`.
    /// Flips the per-database anonymous-access flag; the instance-wide
    /// `RTDB_AUTH_ANONYMOUS_ENABLED` boot gate is separate (see
    /// [`get_anonymous_access`](Self::get_anonymous_access)). A `not_found`
    /// error means the database is not registered.
    pub async fn set_anonymous_access(&self, db: &str, enabled: bool) -> Result<(), RtDbError> {
        #[derive(Serialize)]
        struct Body {
            enabled: bool,
        }
        let resp = self
            .patch_json(
                &format!("/admin/db/{db}/anonymous-access"),
                &Body { enabled },
            )
            .await?;
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

/// Mirrors `ts-client/tests/admin.test.ts`: each method posts/gets the right
/// path with the admin-key bearer, the right body shape, and returns the right
/// type. `wiremock` matchers assert the on-the-wire request; `#[ignore]`-free
/// because they hit a mock, not a real server.
#[cfg(all(test, feature = "admin"))]
mod tests;
