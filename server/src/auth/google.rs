//! Google OAuth provider (`/auth/google/*`). Optional — leave
//! `RTDB_GOOGLE_CLIENT_ID` / `RTDB_GOOGLE_CLIENT_SECRET` unset to disable
//! Google login.

use std::sync::Arc;

use async_trait::async_trait;

use crate::AppState;
use crate::auth::provider::OAuthProvider;
use crate::auth::session;
use crate::config::Config;
use crate::db::{new_id, now_ms};
use crate::error::RtDbError;

/// Google's OAuth endpoints are fixed (no on-prem variant), so they are
/// constants rather than config — only `client_id`/`client_secret` vary.
const AUTHORIZE_URL: &str = "https://accounts.google.com/o/oauth2/v2/auth";
const TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const USERINFO_URL: &str = "https://openidconnect.googleapis.com/v1/userinfo";

/// Google OAuth provider using the standard authorization-code (OIDC) flow:
/// authorize -> callback -> token exchange -> `/userinfo`.
pub struct GoogleProvider {
    client_id: String,
    client_secret: String,
}

impl GoogleProvider {
    fn redirect_uri(&self, public_url: &str) -> String {
        format!("{public_url}/auth/google/callback")
    }
}

/// Normalized identity extracted from Google's `/userinfo` response.
struct GoogleIdentity {
    email: String,
    name: Option<String>,
}

#[async_trait]
impl OAuthProvider for GoogleProvider {
    fn name() -> &'static str {
        "google"
    }

    fn from_config(config: &Config) -> Option<Self> {
        let client_id = config.oauth.google.client_id.clone()?;
        let client_secret = config.oauth.google.client_secret.clone()?;
        Some(Self {
            client_id,
            client_secret,
        })
    }

    fn callback_path(&self) -> &'static str {
        "/auth/google/callback"
    }

    fn authorize_url(&self, redirect_uri: &str, state: &str) -> String {
        format!(
            "{AUTHORIZE_URL}?client_id={}&redirect_uri={redirect_uri}&response_type=code&scope=openid%20email%20profile&state={state}",
            self.client_id,
        )
    }

    async fn complete_login(&self, state: &Arc<AppState>, code: &str) -> Result<String, RtDbError> {
        let http = state.auth.http.clone();
        let redirect_uri = self.redirect_uri(&state.config.public_url);

        let userinfo = crate::auth::provider::oidc_exchange_and_fetch_userinfo(
            &http,
            Self::name(),
            TOKEN_URL,
            USERINFO_URL,
            &self.client_id,
            &self.client_secret,
            code,
            &redirect_uri,
        )
        .await?;

        let identity = parse_userinfo(userinfo)?;
        let email = identity.email.to_lowercase();

        // Identity is email-keyed: `email` is UNIQUE and is the key the
        // allowlist uses, so a Google login reuses an existing row if the same
        // person previously signed in with GitHub (matching on email). There
        // is no `google_id` column — Google's `sub` is not persisted, which
        // avoids a schema change and keeps identity aligned with the
        // email-based authorization model.
        let login = identity.name.clone().unwrap_or_else(|| email.clone());
        let id = new_id();
        let now = now_ms();
        let (user_id,): (String,) = sqlx::query_as(
            "INSERT INTO rtdb_auth.users (id, login, email, created_at) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (email) DO UPDATE SET login = EXCLUDED.login \
             RETURNING id",
        )
        .bind(&id)
        .bind(&login)
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

/// Google's `/userinfo` returns `email_verified` as a JSON boolean; some
/// Google-issued tokens serialize it as the string `"true"`. Accept both so a
/// format quirk can't silently reject a verified user.
fn is_email_verified(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Bool(b) => *b,
        serde_json::Value::String(s) => s.eq_ignore_ascii_case("true"),
        _ => false,
    }
}

/// Parses `/userinfo` into a normalized identity. Requires a present,
/// verified email — Google only guarantees the email belongs to the account
/// when `email_verified` is true, so an unverified email is rejected
/// (`forbidden`) rather than trusted. `name` is optional.
fn parse_userinfo(value: serde_json::Value) -> Result<GoogleIdentity, RtDbError> {
    let email = value
        .get("email")
        .and_then(|v| v.as_str())
        .ok_or_else(|| RtDbError::forbidden("no verified email"))?
        .to_string();

    if !value
        .get("email_verified")
        .map(is_email_verified)
        .unwrap_or(false)
    {
        return Err(RtDbError::forbidden("no verified email"));
    }

    let name = value.get("name").and_then(|v| v.as_str()).map(String::from);

    Ok(GoogleIdentity { email, name })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::OAuthConfig;
    use serde_json::json;

    #[test]
    fn parse_userinfo_accepts_verified_boolean_email() {
        let id = parse_userinfo(json!({
            "sub": "123",
            "email": "Alice@Example.com",
            "email_verified": true,
            "name": "Alice"
        }))
        .unwrap();
        assert_eq!(id.email, "Alice@Example.com");
        assert_eq!(id.name.as_deref(), Some("Alice"));
    }

    #[test]
    fn parse_userinfo_accepts_verified_string_email() {
        let id = parse_userinfo(json!({
            "email": "bob@example.com",
            "email_verified": "true"
        }))
        .unwrap();
        assert_eq!(id.email, "bob@example.com");
        assert!(id.name.is_none());
    }

    #[test]
    fn parse_userinfo_rejects_unverified_email() {
        let err = parse_userinfo(json!({"email": "c@x.com", "email_verified": false}));
        assert!(err.is_err());
    }

    #[test]
    fn parse_userinfo_rejects_missing_email() {
        let err = parse_userinfo(json!({"sub": "1", "email_verified": true}));
        assert!(err.is_err());
    }

    #[test]
    fn parse_userinfo_rejects_missing_verified_flag() {
        let err = parse_userinfo(json!({"email": "d@x.com"}));
        assert!(err.is_err());
    }

    #[test]
    fn authorize_url_contains_response_type_scope_and_state() {
        let provider = GoogleProvider {
            client_id: "g-client".into(),
            client_secret: "g-secret".into(),
        };
        let url = provider.authorize_url("https://app.example/auth/google/callback", "st");
        assert!(url.starts_with("https://accounts.google.com/o/oauth2/v2/auth?"));
        assert!(url.contains("client_id=g-client"));
        assert!(url.contains("response_type=code"));
        assert!(url.contains("scope=openid%20email%20profile"));
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
        assert!(GoogleProvider::from_config(&cfg).is_none());
        cfg.oauth.google.client_id = Some("id".into());
        assert!(GoogleProvider::from_config(&cfg).is_none());
        cfg.oauth.google.client_secret = Some("secret".into());
        assert!(GoogleProvider::from_config(&cfg).is_some());
    }
}
