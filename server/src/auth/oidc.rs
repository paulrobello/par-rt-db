//! Generic OIDC provider (`/auth/oidc/*`): discovery-driven issuer, so any
//! compliant IdP works via `RTDB_OIDC_*` env. See `docs/OAUTH_SETUP.md`.

use std::sync::Arc;

use async_trait::async_trait;

use crate::AppState;
use crate::auth::provider::OAuthProvider;
use crate::auth::session;
use crate::config::Config;
use crate::db::{new_id, now_ms};
use crate::error::RtDbError;

/// Generic OpenID Connect provider — one implementation serving any
/// standards-compliant IdP (Azure AD, Keycloak, Auth0, Okta, self-hosted).
/// Unlike the per-IdP modules, the authorize/token/userinfo endpoints are not
/// constants: the operator supplies them from their IdP's
/// `/.well-known/openid-configuration`. The trait's `authorize_url` is sync, so
/// it cannot perform live OIDC discovery at request time — endpoints are
/// configuration rather than discovered per login. The provider is active only
/// when all five required fields are set; otherwise the routes return 503, like
/// an unconfigured google/gitlab.
pub struct OidcProvider {
    client_id: String,
    client_secret: String,
    authorize_url: String,
    token_url: String,
    userinfo_url: String,
}

impl OidcProvider {
    fn redirect_uri(&self, public_url: &str) -> String {
        format!("{public_url}/auth/oidc/callback")
    }
}

/// Normalized identity extracted from the IdP's userinfo response.
struct OidcIdentity {
    email: String,
    name: Option<String>,
}

#[async_trait]
impl OAuthProvider for OidcProvider {
    fn name() -> &'static str {
        "oidc"
    }

    fn from_config(config: &Config) -> Option<Self> {
        let client_id = config.oidc_client_id.clone()?;
        let client_secret = config.oidc_client_secret.clone()?;
        let authorize_url = config.oidc_authorize_url.clone()?;
        let token_url = config.oidc_token_url.clone()?;
        let userinfo_url = config.oidc_userinfo_url.clone()?;
        Some(Self {
            client_id,
            client_secret,
            authorize_url,
            token_url,
            userinfo_url,
        })
    }

    fn callback_path(&self) -> &'static str {
        "/auth/oidc/callback"
    }

    fn authorize_url(&self, redirect_uri: &str, state: &str) -> String {
        format!(
            "{}?client_id={}&redirect_uri={redirect_uri}&response_type=code&scope=openid%20email%20profile&state={state}",
            self.authorize_url, self.client_id,
        )
    }

    async fn complete_login(&self, state: &Arc<AppState>, code: &str) -> Result<String, RtDbError> {
        let http = state.auth.http.clone();
        let redirect_uri = self.redirect_uri(&state.config.public_url);

        let userinfo = crate::auth::provider::oidc_exchange_and_fetch_userinfo(
            &http,
            Self::name(),
            &self.token_url,
            &self.userinfo_url,
            &self.client_id,
            &self.client_secret,
            code,
            &redirect_uri,
        )
        .await?;

        let identity = parse_userinfo(userinfo)?;
        let email = identity.email.to_lowercase();

        // Identity is email-keyed (UNIQUE; the key the allowlist uses), so an
        // OIDC login reuses an existing row if the same person previously signed
        // in with another provider matching on email. The IdP's `sub` is not
        // persisted, mirroring the google provider — no schema change, identity
        // aligned with the email-based authorization model.
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

/// `email_verified` may arrive as a JSON boolean or the string `"true"` (some
/// IdPs serialize it both ways); anything else is treated as unverified.
fn is_email_verified(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Bool(b) => *b,
        serde_json::Value::String(s) => s.eq_ignore_ascii_case("true"),
        _ => false,
    }
}

/// Parses userinfo into a normalized identity. A present, **verified** email
/// is required.
///
/// SEC-122: the default is now unverified. A generic OIDC IdP's verification
/// posture cannot be assumed, so an absent `email_verified` no longer trusts
/// the address — the IdP must positively assert `email_verified: true` (as a
/// boolean or the string `"true"`). Operators whose IdP genuinely verifies
/// mail but omits the claim should patch their IdP's userinfo to emit it
/// rather than relax this gate.
fn parse_userinfo(value: serde_json::Value) -> Result<OidcIdentity, RtDbError> {
    let email = value
        .get("email")
        .and_then(|v| v.as_str())
        .ok_or_else(|| RtDbError::forbidden("no email"))?
        .to_string();

    let verified = value
        .get("email_verified")
        .map(is_email_verified)
        .unwrap_or(false);
    if !verified {
        return Err(RtDbError::forbidden("email is not verified"));
    }

    let name = value.get("name").and_then(|v| v.as_str()).map(String::from);

    Ok(OidcIdentity { email, name })
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let id =
            parse_userinfo(json!({"email": "bob@example.com", "email_verified": "true"})).unwrap();
        assert_eq!(id.email, "bob@example.com");
        assert!(id.name.is_none());
    }

    #[test]
    fn parse_userinfo_rejects_absent_verified_flag() {
        // SEC-122: an absent email_verified is now treated as unverified, not
        // verified. The IdP must positively assert email_verified.
        let err = parse_userinfo(json!({"sub": "9", "email": "c@x.com", "name": "C"}));
        assert!(err.is_err());
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
    fn authorize_url_uses_configured_endpoint_and_scope() {
        let provider = OidcProvider {
            client_id: "oidc-client".into(),
            client_secret: "oidc-secret".into(),
            authorize_url: "https://idp.example.com/oauth2/authorize".into(),
            token_url: "https://idp.example.com/oauth2/token".into(),
            userinfo_url: "https://idp.example.com/userinfo".into(),
        };
        let url = provider.authorize_url("https://app.example/auth/oidc/callback", "st");
        assert!(url.starts_with("https://idp.example.com/oauth2/authorize?"));
        assert!(url.contains("client_id=oidc-client"));
        assert!(url.contains("response_type=code"));
        assert!(url.contains("scope=openid%20email%20profile"));
        assert!(url.contains("state=st"));
    }

    #[test]
    fn from_config_requires_all_five_fields() {
        let mut cfg = base_cfg();
        assert!(OidcProvider::from_config(&cfg).is_none());
        cfg.oidc_client_id = Some("id".into());
        assert!(
            OidcProvider::from_config(&cfg).is_none(),
            "still missing secret+urls"
        );
        cfg.oidc_client_secret = Some("secret".into());
        assert!(
            OidcProvider::from_config(&cfg).is_none(),
            "still missing urls"
        );
        cfg.oidc_authorize_url = Some("https://idp/authorize".into());
        cfg.oidc_token_url = Some("https://idp/token".into());
        assert!(
            OidcProvider::from_config(&cfg).is_none(),
            "still missing userinfo url"
        );
        cfg.oidc_userinfo_url = Some("https://idp/userinfo".into());
        assert!(OidcProvider::from_config(&cfg).is_some());
    }

    /// Minimal `Config` with every OAuth provider unconfigured — shared by the
    /// `from_config` test. Mirrors the literal `Config { ... }` the google
    /// provider's test constructs.
    fn base_cfg() -> Config {
        Config {
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
            presence_beat_interval_ms: 5000,
            presence_beat_timeout_ms: 15000,
            quota_cache_ttl_secs: 60,
            db_idle_reclaim_secs: 0,
            admin_rate_limit_per_ip_rpm: 0,
            cookie_secure: false,
            trusted_proxy: false,
            otel_enabled: false,
            otel_endpoint: String::new(),
            otel_service_name: String::new(),
            otel_sample_ratio: 0.0,
            multi_instance: false,
            instance_id: None,
        }
    }
}
