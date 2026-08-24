//! Google OAuth provider (`/auth/google/*`). Optional — leave
//! `RTDB_GOOGLE_CLIENT_ID` / `RTDB_GOOGLE_CLIENT_SECRET` unset to disable
//! Google login.

use std::sync::Arc;

use async_trait::async_trait;

use crate::AppState;
use crate::auth::provider::OAuthProvider;
use crate::auth::{self, ConflictStyle, ProviderIdentity, session};
use crate::config::Config;
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
    /// Google's `sub` claim — the stable, per-account identifier written to
    /// `users.google_sub`. Immutable across a Google-side email change, which
    /// `email` is not.
    sub: String,
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
            crate::auth::provider::OidcExchange {
                http: &http,
                slug: Self::name(),
                token_url: TOKEN_URL,
                userinfo_url: USERINFO_URL,
                client_id: &self.client_id,
                client_secret: &self.client_secret,
                code,
                redirect_uri: &redirect_uri,
            },
        )
        .await?;

        let identity = parse_userinfo(userinfo)?;
        let email = identity.email.to_lowercase();

        // Identity keys on Google's stable `sub` (`users.google_sub`), not on
        // email: a Google-side email change follows the account instead of
        // forking a second one. Cross-provider linking still works — step (2)
        // of `auth::resolve_user` adopts an existing row that carries the same
        // verified email and no `google_sub` yet, which is also what links a
        // row created before this column existed.
        let login = identity.name.clone().unwrap_or_else(|| email.clone());
        let user_id = auth::resolve_user(
            &state.pool,
            ProviderIdentity {
                provider_id_column: auth::PROVIDER_COL_GOOGLE_SUB,
                provider_id: &identity.sub,
                login: &login,
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
    // `sub` is mandatory in an OIDC userinfo response and is the durable
    // identity key, so a response without it is rejected rather than silently
    // falling back to email-keyed identity.
    let sub = value
        .get("sub")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| RtDbError::forbidden("userinfo missing sub"))?
        .to_string();

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

    Ok(GoogleIdentity { sub, email, name })
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
        assert_eq!(id.sub, "123", "sub is the durable identity key");
    }

    #[test]
    fn parse_userinfo_accepts_verified_string_email() {
        let id = parse_userinfo(json!({
            "sub": "456",
            "email": "bob@example.com",
            "email_verified": "true"
        }))
        .unwrap();
        assert_eq!(id.email, "bob@example.com");
        assert!(id.name.is_none());
    }

    /// `sub` is what keeps identity stable across a Google-side email change,
    /// so userinfo without it is rejected rather than silently degrading to
    /// email-keyed identity.
    #[test]
    fn parse_userinfo_rejects_missing_sub() {
        let err = parse_userinfo(json!({"email": "e@x.com", "email_verified": true}));
        assert!(err.is_err());
    }

    #[test]
    fn parse_userinfo_rejects_unverified_email() {
        let err = parse_userinfo(json!({"sub": "2", "email": "c@x.com", "email_verified": false}));
        assert!(err.is_err());
    }

    #[test]
    fn parse_userinfo_rejects_missing_email() {
        let err = parse_userinfo(json!({"sub": "1", "email_verified": true}));
        assert!(err.is_err());
    }

    #[test]
    fn parse_userinfo_rejects_missing_verified_flag() {
        let err = parse_userinfo(json!({"sub": "3", "email": "d@x.com"}));
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

    /// Google's durable key is its stable `sub` (`users.google_sub`), not the
    /// email: a returning login with the same `sub` but a Google-side email
    /// change reuses the row instead of forking a new account.
    #[tokio::test]
    async fn returning_user_with_the_same_sub_reuses_the_row_across_an_email_change() {
        let pool = users_pool().await;
        let sub = uuid::Uuid::now_v7().simple().to_string();
        let email = format!(
            "{}@google-resolve-test.example",
            uuid::Uuid::now_v7().simple()
        );
        let new_email = format!(
            "{}@google-resolve-test.example",
            uuid::Uuid::now_v7().simple()
        );

        let first_id = auth::resolve_user(
            &pool,
            ProviderIdentity {
                provider_id_column: auth::PROVIDER_COL_GOOGLE_SUB,
                provider_id: &sub,
                login: "Alice",
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
                provider_id_column: auth::PROVIDER_COL_GOOGLE_SUB,
                provider_id: &sub,
                login: "Alice Renamed",
                email: &new_email,
                allow_email_link: true,
                conflict_style: ConflictStyle::Conflict,
            },
        )
        .await
        .expect("returning-user resolve");

        assert_eq!(
            second_id, first_id,
            "the same google_sub reuses the row across an email change"
        );
        let (login, row_email): (String, String) =
            sqlx::query_as("SELECT login, email FROM rtdb_auth.users WHERE id = $1")
                .bind(&first_id)
                .fetch_one(&pool)
                .await
                .expect("user row exists");
        assert_eq!(
            login, "Alice Renamed",
            "login follows the provider-side change"
        );
        assert_eq!(
            row_email, new_email,
            "email follows the provider-side change"
        );
    }
}
