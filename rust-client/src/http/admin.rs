use super::RtDbHttpClient;
use crate::error::RtDbError;
use crate::mutation::{StepResult, Transaction};
use serde::de::DeserializeOwned;

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
        self.admin_client().create_db(name).await.map(|_| ())
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
