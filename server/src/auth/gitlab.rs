use std::sync::Arc;

use async_trait::async_trait;

use crate::AppState;
use crate::auth::provider::OAuthProvider;
use crate::auth::session;
use crate::config::Config;
use crate::db::{new_id, now_ms};
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
        let client_id = config.gitlab_client_id.clone()?;
        let client_secret = config.gitlab_client_secret.clone()?;
        Some(Self {
            client_id,
            client_secret,
            base_url: config.gitlab_base_url.clone(),
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
            &http,
            Self::name(),
            &token_url,
            &userinfo_url,
            &self.client_id,
            &self.client_secret,
            code,
            &redirect_uri,
        )
        .await?;

        let identity = parse_user(user)?;
        let email = identity.email.to_lowercase();

        // Identity is email-keyed, mirroring the Google provider: `email` is
        // UNIQUE and is the key the allowlist uses, so a GitLab login reuses an
        // existing row if the same person previously signed in with GitHub or
        // Google (matching on email). There is no `gitlab_id` column — GitLab's
        // numeric id is not persisted, which avoids a schema change and keeps
        // identity aligned with the email-based authorization model.
        let id = new_id();
        let now = now_ms();
        let (user_id,): (String,) = sqlx::query_as(
            "INSERT INTO rtdb_auth.users (id, login, email, created_at) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (email) DO UPDATE SET login = EXCLUDED.login \
             RETURNING id",
        )
        .bind(&id)
        .bind(&identity.login)
        .bind(&email)
        .bind(now)
        .fetch_one(&state.pool)
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
            github_client_id: None,
            github_client_secret: None,
            github_base_url: "https://github.com".into(),
            github_api_url: "https://api.github.com".into(),
            google_client_id: None,
            google_client_secret: None,
            gitlab_client_id: None,
            gitlab_client_secret: None,
            gitlab_base_url: "https://gitlab.com".into(),
            oidc_client_id: None,
            oidc_client_secret: None,
            oidc_authorize_url: None,
            oidc_token_url: None,
            oidc_userinfo_url: None,
            microsoft_client_id: None,
            microsoft_client_secret: None,
            microsoft_tenant: "common".into(),
            apple_client_id: None,
            apple_team_id: None,
            apple_key_id: None,
            apple_private_key: None,
            max_affected_docs: 100,
            auth_anonymous_enabled: false,
            anonymous_session_ttl_days: 1,
            anonymous_rate_limit_per_ip_rpm: 0,
            static_dir: None,
            pool_max_connections: 75,
            schema_cache_max_entries: 1024,
            slow_query_ms: 0,
            slow_query_capacity: 200,
            slow_query_log_params: false,
            rate_limit_per_token_rpm: 0,
            rate_limit_per_db_rpm: 0,
            audit_log_enabled: false,
            oauth_login_csrf: true,
            webhooks_enabled: false,
            webhook_allow_http: false,
            storage_rate_limit_per_ip_rpm: 0,
            storage_require_signed_urls: false,
            backup_enabled: false,
            backup_cron: "0 3 * * *".into(),
            backup_dir: "./backups".into(),
            backup_retention: 7,
            subs_verify_skip_every: 0,
            ttl_sweep_interval_secs: 60,
            ttl_batch: 5000,
            image_transforms_enabled: true,
            image_max_dim: 2048,
            image_max_pixels: 25_000_000,
            image_cache_bytes: 256 * 1024 * 1024,
            image_concurrency: 4,
            image_default_quality: 80,
            presence_enabled: false,
            presence_max_state_bytes: 1024,
            presence_max_room_size: 100,
            presence_max_rooms_per_conn: 32,
            presence_max_room_bytes: 256,
            presence_broadcast_interval_ms: 50,
            presence_update_limit_per_sec: 20,
            presence_max_ttl_ms: 300_000,
            quota_cache_ttl_secs: 60,
            db_idle_reclaim_secs: 0,
            admin_rate_limit_per_ip_rpm: 0,
            cookie_secure: false,
            otel_enabled: false,
            otel_endpoint: String::new(),
            otel_service_name: String::new(),
            otel_sample_ratio: 0.0,
            multi_instance: false,
            instance_id: None,
        };
        assert!(GitlabProvider::from_config(&cfg).is_none());
        cfg.gitlab_client_id = Some("id".into());
        assert!(GitlabProvider::from_config(&cfg).is_none());
        cfg.gitlab_client_secret = Some("secret".into());
        assert!(GitlabProvider::from_config(&cfg).is_some());
    }
}
