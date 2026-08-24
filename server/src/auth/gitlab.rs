//! GitLab OAuth provider (`/auth/gitlab/*`), configured via
//! `RTDB_GITLAB_CLIENT_ID` / `RTDB_GITLAB_CLIENT_SECRET`.

use std::sync::Arc;

use async_trait::async_trait;

use crate::AppState;
use crate::auth::provider::OAuthProvider;
use crate::auth::{self, ConflictStyle, ProviderIdentity, session};
use crate::config::Config;
use crate::error::RtDbError;

/// GitLab OAuth provider using the standard authorization-code flow:
/// authorize -> callback -> token exchange -> `/api/v4/user`. The instance-wide
/// OAuth application's `client_id`/`client_secret` come from config; `base_url`
/// is overridable so a self-hosted GitLab can serve the same flow.
pub struct GitlabProvider {
    client_id: String,
    client_secret: String,
    base_url: String,
}

impl GitlabProvider {
    fn redirect_uri(&self, public_url: &str) -> String {
        format!("{public_url}/auth/gitlab/callback")
    }
}

/// Normalized identity extracted from GitLab's `/api/v4/user` response.
struct GitlabIdentity {
    email: String,
    login: String,
}

#[async_trait]
impl OAuthProvider for GitlabProvider {
    fn name() -> &'static str {
        "gitlab"
    }

    fn from_config(config: &Config) -> Option<Self> {
        let client_id = config.oauth.gitlab.client_id.clone()?;
        let client_secret = config.oauth.gitlab.client_secret.clone()?;
        Some(Self {
            client_id,
            client_secret,
            base_url: config.oauth.gitlab.base_url.clone(),
        })
    }

    fn callback_path(&self) -> &'static str {
        "/auth/gitlab/callback"
    }

    fn authorize_url(&self, redirect_uri: &str, state: &str) -> String {
        format!(
            "{}/oauth/authorize?client_id={}&redirect_uri={redirect_uri}&response_type=code&scope=read_user%20email&state={state}",
            self.base_url, self.client_id,
        )
    }

    async fn complete_login(&self, state: &Arc<AppState>, code: &str) -> Result<String, RtDbError> {
        let http = state.auth.http.clone();
        let redirect_uri = self.redirect_uri(&state.config.public_url);
        let token_url = format!("{}/oauth/token", self.base_url);
        let userinfo_url = format!("{}/api/v4/user", self.base_url);

        let user = crate::auth::provider::oidc_exchange_and_fetch_userinfo(
            crate::auth::provider::OidcExchange {
                http: &http,
                slug: Self::name(),
                token_url: &token_url,
                userinfo_url: &userinfo_url,
                client_id: &self.client_id,
                client_secret: &self.client_secret,
                code,
                redirect_uri: &redirect_uri,
            },
        )
        .await?;

        let identity = parse_user(user)?;
        let email = identity.email.to_lowercase();

        // Identity is email-keyed, mirroring the Google provider: `email` is
        // UNIQUE and is the key the allowlist uses, so a GitLab login reuses an
        // existing row if the same person previously signed in with GitHub or
        // Google (matching on email). There is no `gitlab_id` column — GitLab's
        // numeric id is not persisted, which avoids a schema change and keeps
        // identity aligned with the email-based authorization model. QA-004:
        // resolved through the shared `auth::resolve_user` email-keyed path.
        let user_id = auth::resolve_user(
            &state.pool,
            ProviderIdentity {
                provider_id_column: auth::PROVIDER_COL_EMAIL,
                provider_id: &email,
                login: &identity.login,
                email: &email,
                allow_email_link: true,
                conflict_style: ConflictStyle::Conflict,
            },
        )
        .await?;

        session::create_session(
            &state.pool,
            &user_id,
            state.runtime.hot.load().session_ttl_days,
        )
        .await
    }
}

/// Parses `/api/v4/user` into a normalized identity. Requires a present,
/// confirmed email — GitLab's `confirmed_at` is a timestamp when the address is
/// confirmed and `null` when it is not, so an unconfirmed email is rejected
/// (`forbidden`) rather than trusted, mirroring GitHub's verified-email and
/// Google's `email_verified` stance. The display `login` prefers the full name,
/// then the `@username` handle, then the email.
fn parse_user(value: serde_json::Value) -> Result<GitlabIdentity, RtDbError> {
    let email = value
        .get("email")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| RtDbError::forbidden("no verified email"))?;

    let confirmed = matches!(
        value.get("confirmed_at"),
        Some(serde_json::Value::String(s)) if !s.is_empty()
    );
    if !confirmed {
        return Err(RtDbError::forbidden("no verified email"));
    }

    let name = value.get("name").and_then(|v| v.as_str());
    let username = value.get("username").and_then(|v| v.as_str());
    let login = name.or(username).unwrap_or(email).to_string();

    Ok(GitlabIdentity {
        email: email.to_string(),
        login,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::OAuthConfig;
    use serde_json::json;

    #[test]
    fn parse_user_accepts_confirmed_email() {
        let id = parse_user(json!({
            "id": 42,
            "username": "alice",
            "name": "Alice",
            "email": "Alice@Example.com",
            "confirmed_at": "2024-01-02T03:04:05Z"
        }))
        .unwrap();
        assert_eq!(id.email, "Alice@Example.com");
        assert_eq!(id.login, "Alice");
    }

    #[test]
    fn parse_user_falls_back_to_username_then_email_for_login() {
        let no_name = parse_user(json!({
            "username": "bob",
            "email": "bob@example.com",
            "confirmed_at": "2024-01-02T03:04:05Z"
        }))
        .unwrap();
        assert_eq!(no_name.login, "bob");

        let neither = parse_user(json!({
            "email": "carol@example.com",
            "confirmed_at": "2024-01-02T03:04:05Z"
        }))
        .unwrap();
        assert_eq!(neither.login, "carol@example.com");
    }

    #[test]
    fn parse_user_rejects_unconfirmed_email() {
        let err = parse_user(json!({
            "email": "unconfirmed@example.com",
            "confirmed_at": null
        }));
        assert!(err.is_err());
    }

    #[test]
    fn parse_user_rejects_missing_email() {
        let err = parse_user(json!({
            "username": "dave",
            "confirmed_at": "2024-01-02T03:04:05Z"
        }));
        assert!(err.is_err());
    }

    #[test]
    fn parse_user_rejects_empty_confirmed_at() {
        // An empty-string confirmed_at is treated as unconfirmed, not verified.
        let err = parse_user(json!({"email": "e@example.com", "confirmed_at": ""}));
        assert!(err.is_err());
    }

    #[test]
    fn authorize_url_contains_response_type_scope_and_state() {
        let provider = GitlabProvider {
            client_id: "gl-client".into(),
            client_secret: "gl-secret".into(),
            base_url: "https://gitlab.example.com".into(),
        };
        let url = provider.authorize_url("https://app.example/auth/gitlab/callback", "st");
        assert!(url.starts_with("https://gitlab.example.com/oauth/authorize?"));
        assert!(url.contains("client_id=gl-client"));
        assert!(url.contains("response_type=code"));
        assert!(url.contains("scope=read_user%20email"));
        assert!(url.contains("state=st"));
    }

    #[test]
    fn from_config_returns_none_without_credentials() {
        let mut cfg = Config {
            port: 0,
            database_url: "x".into(),
            admin_key: "k".into(),
            public_url: "http://localhost:0".into(),
            oauth: OAuthConfig::default(),
            max_affected_docs: 100,
            auth_anonymous_enabled: false,
            anonymous_session_ttl_days: 1,
            static_dir: None,
            pool_max_connections: 75,
            schema_cache_max_entries: 1024,
            slow_query_ms: 0,
            slow_query_capacity: 200,
            slow_query_log_params: false,
            audit_log_enabled: false,
            oauth_login_csrf: true,
            webhooks_enabled: false,
            webhook_allow_http: false,
            subs_verify_skip_every: 0,
            ttl_sweep_interval_secs: 60,
            ttl_batch: 5000,
            presence_enabled: false,
            presence_max_state_bytes: 1024,
            presence_max_room_size: 100,
            presence_max_rooms_per_conn: 32,
            presence_max_room_bytes: 256,
            presence_broadcast_interval_ms: 50,
            presence_update_limit_per_sec: 20,
            presence_max_ttl_ms: 300_000,
            presence_beat_interval_ms: 5000,
            presence_beat_timeout_ms: 15000,
            quota_cache_ttl_secs: 60,
            db_idle_reclaim_secs: 0,
            cookie_secure: false,
            trusted_proxy: false,
            otel_enabled: false,
            otel_endpoint: String::new(),
            otel_service_name: String::new(),
            otel_sample_ratio: 0.0,
            limits: crate::config::LimitsConfig {
                per_token_rpm: 0,
                per_db_rpm: 0,
                exact: false,
                sync_ms: 1000,
                storage_per_ip_rpm: 0,
                anonymous_per_ip_rpm: 0,
                admin_per_ip_rpm: 0,
            },
            storage: crate::config::StorageConfig {
                require_signed_urls: false,
                image: crate::config::ImageTransformConfig {
                    enabled: true,
                    max_dim: 2048,
                    max_pixels: 25_000_000,
                    cache_bytes: 256 * 1024 * 1024,
                    concurrency: 4,
                    default_quality: 80,
                },
            },
            backup: crate::config::BackupConfig {
                enabled: false,
                cron: "0 3 * * *".into(),
                dir: "./backups".into(),
                retention: 7,
            },
            multi_instance: crate::config::MultiInstanceConfig {
                enabled: false,
                instance_id: None,
                forward_timeout_ms: 5000,
                forward_concurrency: 64,
            },
        };
        assert!(GitlabProvider::from_config(&cfg).is_none());
        cfg.oauth.gitlab.client_id = Some("id".into());
        assert!(GitlabProvider::from_config(&cfg).is_none());
        cfg.oauth.gitlab.client_secret = Some("secret".into());
        assert!(GitlabProvider::from_config(&cfg).is_some());
    }

    // --- QA-004: returning user via the shared email-keyed resolver --------

    async fn users_pool() -> sqlx::PgPool {
        let url = std::env::var("RTDB_TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://rtdb:rtdb@127.0.0.1:55434/rtdb".into());
        let pool = sqlx::PgPool::connect(&url)
            .await
            .expect("connect to dev postgres");
        crate::db::bootstrap(&pool)
            .await
            .expect("bootstrap rtdb_auth");
        pool
    }

    /// GitLab has no persisted per-provider id, so its durable key IS the
    /// email: a returning login with the same email (but a changed display
    /// `login`) reuses the row instead of forking a new one.
    #[tokio::test]
    async fn returning_user_with_the_same_email_reuses_the_row() {
        let pool = users_pool().await;
        let email = format!(
            "{}@gitlab-resolve-test.example",
            uuid::Uuid::now_v7().simple()
        );

        let first_id = auth::resolve_user(
            &pool,
            ProviderIdentity {
                provider_id_column: auth::PROVIDER_COL_EMAIL,
                provider_id: &email,
                login: "Bob",
                email: &email,
                allow_email_link: true,
                conflict_style: ConflictStyle::Conflict,
            },
        )
        .await
        .expect("initial insert");

        let second_id = auth::resolve_user(
            &pool,
            ProviderIdentity {
                provider_id_column: auth::PROVIDER_COL_EMAIL,
                provider_id: &email,
                login: "Bob Renamed",
                email: &email,
                allow_email_link: true,
                conflict_style: ConflictStyle::Conflict,
            },
        )
        .await
        .expect("returning-user resolve");

        assert_eq!(second_id, first_id, "same email reuses the row");
        let (login,): (String,) = sqlx::query_as("SELECT login FROM rtdb_auth.users WHERE id = $1")
            .bind(&first_id)
            .fetch_one(&pool)
            .await
            .expect("user row exists");
        assert_eq!(
            login, "Bob Renamed",
            "login follows the provider-side change"
        );
    }
}
