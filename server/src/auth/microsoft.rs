use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;

use crate::AppState;
use crate::auth::provider::OAuthProvider;
use crate::auth::session;
use crate::config::Config;
use crate::db::{new_id, now_ms};
use crate::error::RtDbError;

/// Microsoft (Entra ID / Azure AD v2.0) OAuth provider. This is OIDC with
/// well-known endpoint URLs derived from `tenant`, so — unlike the generic
/// `oidc` provider — the operator supplies credentials + tenant only and never
/// pastes four discovery URLs. `tenant` defaults to "common" (any Microsoft
/// account, work/school or personal); a specific tenant GUID/name restricts
/// the audience to one organization.
///
/// Identity is email-keyed (the account email, read from the id_token and
/// userinfo — Entra ID omits the email from userinfo, so the id_token's
/// `preferred_username`/`email` is the primary source), mirroring the generic
/// OIDC and Google providers: `email` is UNIQUE and the allowlist key, so a
/// Microsoft login reuses an existing row if the same person previously signed
/// in with another provider matching on email. Microsoft's `sub` is not
/// persisted — consistent with the google/oidc providers and the email-based
/// authorization model.
pub struct MicrosoftProvider {
    client_id: String,
    client_secret: String,
    tenant: String,
}

impl MicrosoftProvider {
    fn redirect_uri(&self, public_url: &str) -> String {
        format!("{public_url}/auth/microsoft/callback")
    }

    fn authorize_endpoint(&self) -> String {
        format!(
            "https://login.microsoftonline.com/{}/oauth2/v2.0/authorize",
            self.tenant
        )
    }

    fn token_endpoint(&self) -> String {
        format!(
            "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
            self.tenant
        )
    }

    // Microsoft's OIDC userinfo endpoint. It returns a sparse `{sub, name,
    // given_name, family_name, picture}` and omits `email`/`preferred_username`
    // for work/school (Entra ID) accounts, so the email is sourced from the
    // id_token claims returned by the token exchange — see `complete_login` and
    // `parse_userinfo`.
    const USERINFO_ENDPOINT: &'static str = "https://graph.microsoft.com/oidc/userinfo";
}

/// Normalized identity extracted from Graph's userinfo response.
struct MicrosoftIdentity {
    email: String,
    name: Option<String>,
}

#[async_trait]
impl OAuthProvider for MicrosoftProvider {
    fn name() -> &'static str {
        "microsoft"
    }

    fn from_config(config: &Config) -> Option<Self> {
        let client_id = config.microsoft_client_id.clone()?;
        let client_secret = config.microsoft_client_secret.clone()?;
        Some(Self {
            client_id,
            client_secret,
            tenant: config.microsoft_tenant.clone(),
        })
    }

    fn callback_path(&self) -> &'static str {
        "/auth/microsoft/callback"
    }

    fn authorize_url(&self, redirect_uri: &str, state: &str) -> String {
        // `response_mode=query` is a supported Microsoft mode, so the existing
        // GET `provider_callback` generic handles the redirect (no form_post).
        format!(
            "{}?client_id={}&redirect_uri={redirect_uri}&response_type=code&response_mode=query&scope=openid%20email%20profile&state={state}",
            self.authorize_endpoint(),
            self.client_id,
        )
    }

    async fn complete_login(&self, state: &Arc<AppState>, code: &str) -> Result<String, RtDbError> {
        let client = reqwest::Client::new();
        let redirect_uri = self.redirect_uri(&state.config.public_url);

        let token_resp: serde_json::Value = client
            .post(self.token_endpoint())
            .form(&TokenExchangeRequest {
                client_id: &self.client_id,
                client_secret: &self.client_secret,
                code,
                redirect_uri: &redirect_uri,
                grant_type: "authorization_code",
                scope: "openid email profile",
            })
            .send()
            .await
            .map_err(|err| {
                tracing::warn!(error = %err, "microsoft token exchange request failed");
                RtDbError::internal("microsoft token exchange failed")
            })?
            .json()
            .await
            .map_err(|err| {
                tracing::warn!(error = %err, "microsoft token exchange response decode failed");
                RtDbError::internal("microsoft token exchange failed")
            })?;

        let access_token = parse_token_response(&token_resp)?;
        // The userinfo endpoint omits the email for Entra ID accounts; the
        // id_token returned by the token exchange carries it (MSA `email`, AAD
        // `preferred_username` = UPN), so decode its claims as the primary source.
        let id_claims = token_resp
            .get("id_token")
            .and_then(|v| v.as_str())
            .and_then(decode_jwt_claims);

        let userinfo: serde_json::Value = client
            .get(Self::USERINFO_ENDPOINT)
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {access_token}"),
            )
            .send()
            .await
            .map_err(|err| {
                tracing::warn!(error = %err, "microsoft userinfo fetch request failed");
                RtDbError::internal("microsoft userinfo fetch failed")
            })?
            .json()
            .await
            .map_err(|err| {
                tracing::warn!(error = %err, "microsoft userinfo fetch response decode failed");
                RtDbError::internal("microsoft userinfo fetch failed")
            })?;

        let identity = parse_userinfo(userinfo, id_claims.as_ref())?;
        let email = identity.email.to_lowercase();

        // Email-keyed (UNIQUE; the allowlist key), so a Microsoft login reuses
        // an existing row if the same person previously signed in with another
        // provider matching on email. Microsoft's `sub` is not persisted —
        // mirrors the google/oidc providers and the email-based authz model.
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

#[derive(Serialize)]
struct TokenExchangeRequest<'a> {
    client_id: &'a str,
    client_secret: &'a str,
    code: &'a str,
    redirect_uri: &'a str,
    grant_type: &'a str,
    scope: &'a str,
}

/// Extracts the access token from Microsoft's token-exchange response. The v2.0
/// token endpoint returns `{"access_token": "...", "id_token": "...", ...}` on
/// success and an `{"error": "...", "error_description": "..."}` body on
/// failure — the latter is surfaced as a generic internal error so the OAuth
/// error text never reaches the response body.
fn parse_token_response(value: &serde_json::Value) -> Result<String, RtDbError> {
    match value.get("access_token").and_then(|v| v.as_str()) {
        Some(token) => Ok(token.to_string()),
        None => {
            tracing::warn!(response = ?value, "microsoft token exchange returned no access_token");
            Err(RtDbError::internal("microsoft token exchange failed"))
        }
    }
}

/// Decodes (without verifying) the payload claims of a JWT `id_token`. The
/// token arrived from Microsoft's token endpoint over TLS, so trusting its
/// contents matches the userinfo posture; verifying against Microsoft's JWKS
/// would add a discovery fetch for no security gain on this trusted channel.
/// Microsoft's userinfo endpoint omits the email for Entra ID accounts, so the
/// id_token (`email` for MSA, `preferred_username` = UPN for Entra ID) is the
/// reliable source.
fn decode_jwt_claims(id_token: &str) -> Option<serde_json::Value> {
    use base64::Engine;
    let payload = id_token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    serde_json::from_slice(&bytes).ok()
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

/// Parses Graph userinfo into a normalized identity. A present email is
/// required. Microsoft work/school emails are administrator-verified and MSA
/// emails are Microsoft-verified, but Graph does not always emit
/// `email_verified`; matching the generic OIDC provider's posture, the email is
/// trusted unless it is explicitly marked unverified (`email_verified: false`
/// rejects).
fn parse_userinfo(
    value: serde_json::Value,
    id_claims: Option<&serde_json::Value>,
) -> Result<MicrosoftIdentity, RtDbError> {
    // Resolve the email from the userinfo fields, then the id_token claims —
    // the reliable source for Entra ID accounts, where the userinfo endpoint
    // omits the email and `preferred_username` is the UPN. If none are present
    // both payloads are logged so a field-shape change is diagnosable rather
    // than a silent FORBIDDEN.
    let email = value
        .get("email")
        .and_then(|v| v.as_str())
        .or_else(|| value.get("preferred_username").and_then(|v| v.as_str()))
        .or_else(|| value.get("userPrincipalName").and_then(|v| v.as_str()))
        .or_else(|| id_claims.and_then(|c| c.get("email").and_then(|v| v.as_str())))
        .or_else(|| id_claims.and_then(|c| c.get("preferred_username").and_then(|v| v.as_str())))
        .or_else(|| id_claims.and_then(|c| c.get("upn").and_then(|v| v.as_str())))
        .ok_or_else(|| {
            tracing::warn!(
                userinfo = ?value,
                id_claims = ?id_claims,
                "microsoft userinfo and id_token had no email"
            );
            RtDbError::forbidden("no email")
        })?
        .to_string();

    if let Some(verified) = value.get("email_verified")
        && !is_email_verified(verified)
    {
        return Err(RtDbError::forbidden("email is not verified"));
    }

    let name = value.get("name").and_then(|v| v.as_str()).map(String::from);

    Ok(MicrosoftIdentity { email, name })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_token_response_returns_access_token_on_success() {
        let resp = json!({
            "access_token": "EwAoA-abc",
            "token_type": "Bearer",
            "expires_in": 3600,
            "id_token": "eyJ..."
        });
        assert_eq!(parse_token_response(&resp).unwrap(), "EwAoA-abc");
    }

    #[test]
    fn parse_token_response_fails_on_error_body() {
        let resp = json!({"error": "invalid_grant", "error_description": "Bad code"});
        assert!(parse_token_response(&resp).is_err());
    }

    #[test]
    fn decode_jwt_claims_reads_payload_segment() {
        use base64::Engine;
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"preferred_username":"a@b.com","sub":"x"}"#);
        let token = format!("header.{payload}.sig");
        let claims = decode_jwt_claims(&token).unwrap();
        assert_eq!(
            claims.get("preferred_username").and_then(|v| v.as_str()),
            Some("a@b.com")
        );
    }

    #[test]
    fn parse_userinfo_accepts_verified_boolean_email() {
        let id = parse_userinfo(
            json!({
                "sub": "AAAA",
                "email": "Alice@Example.com",
                "email_verified": true,
                "name": "Alice"
            }),
            None,
        )
        .unwrap();
        assert_eq!(id.email, "Alice@Example.com");
        assert_eq!(id.name.as_deref(), Some("Alice"));
    }

    #[test]
    fn parse_userinfo_accepts_absent_verified_flag() {
        // Graph often omits email_verified — trust Microsoft's assertion.
        let id =
            parse_userinfo(json!({"sub": "9", "email": "c@x.com", "name": "C"}), None).unwrap();
        assert_eq!(id.email, "c@x.com");
    }

    #[test]
    fn parse_userinfo_rejects_unverified_email() {
        let err = parse_userinfo(json!({"email": "c@x.com", "email_verified": false}), None);
        assert!(err.is_err());
    }

    #[test]
    fn parse_userinfo_rejects_missing_email() {
        // Neither userinfo nor id_token carries an address.
        let err = parse_userinfo(json!({"sub": "1", "email_verified": true}), None);
        assert!(err.is_err());
    }

    #[test]
    fn parse_userinfo_uses_userinfo_preferred_username_when_present() {
        // Some accounts put the UPN on userinfo `preferred_username`.
        let id = parse_userinfo(
            json!({
                "sub": "AAAA",
                "preferred_username": "Alice@contoso.com",
                "name": "Alice"
            }),
            None,
        )
        .unwrap();
        // parse_userinfo preserves case; lowercasing happens in complete_login.
        assert_eq!(id.email, "Alice@contoso.com");
        assert_eq!(id.name.as_deref(), Some("Alice"));
    }

    #[test]
    fn parse_userinfo_uses_id_token_preferred_username_when_userinfo_is_sparse() {
        // Real Entra ID userinfo is {sub, name, ...} with no email/UPN; the
        // id_token `preferred_username` (the UPN) is the reliable source.
        let id_claims = json!({
            "sub": "AAAA",
            "preferred_username": "Paul@contoso.com",
            "name": "Paul Robello"
        });
        let id = parse_userinfo(
            json!({"sub": "AAAA", "name": "Paul Robello"}),
            Some(&id_claims),
        )
        .unwrap();
        assert_eq!(id.email, "Paul@contoso.com");
    }

    #[test]
    fn parse_userinfo_uses_id_token_email_when_userinfo_is_sparse() {
        // MSA userinfo can omit the email; the id_token `email` claim covers it.
        let id_claims = json!({"sub": "M", "email": "real@outlook.com"});
        let id = parse_userinfo(json!({"sub": "M", "name": "M"}), Some(&id_claims)).unwrap();
        assert_eq!(id.email, "real@outlook.com");
    }

    #[test]
    fn parse_userinfo_prefers_userinfo_email_over_id_token() {
        // When both carry an address, the userinfo `email` wins.
        let id_claims = json!({"email": "idtoken@outlook.com"});
        let id = parse_userinfo(
            json!({"email": "userinfo@outlook.com", "preferred_username": "alias@outlook.com"}),
            Some(&id_claims),
        )
        .unwrap();
        assert_eq!(id.email, "userinfo@outlook.com");
    }

    #[test]
    fn authorize_url_uses_tenant_endpoint_query_mode_and_scope() {
        let provider = MicrosoftProvider {
            client_id: "ms-client".into(),
            client_secret: "ms-secret".into(),
            tenant: "common".into(),
        };
        let url = provider.authorize_url("https://app.example/auth/microsoft/callback", "st");
        assert!(url.starts_with("https://login.microsoftonline.com/common/oauth2/v2.0/authorize?"));
        assert!(url.contains("client_id=ms-client"));
        assert!(url.contains("response_type=code"));
        assert!(url.contains("response_mode=query"));
        assert!(url.contains("scope=openid%20email%20profile"));
        assert!(url.contains("state=st"));
    }

    #[test]
    fn token_and_authorize_endpoints_reflect_a_specific_tenant() {
        let provider = MicrosoftProvider {
            client_id: "x".into(),
            client_secret: "y".into(),
            tenant: "11111111-2222-3333-4444-555555555555".into(),
        };
        assert_eq!(
            provider.authorize_endpoint(),
            "https://login.microsoftonline.com/11111111-2222-3333-4444-555555555555/oauth2/v2.0/authorize"
        );
        assert_eq!(
            provider.token_endpoint(),
            "https://login.microsoftonline.com/11111111-2222-3333-4444-555555555555/oauth2/v2.0/token"
        );
    }

    #[test]
    fn from_config_requires_client_id_and_secret() {
        let mut cfg = base_cfg();
        assert!(MicrosoftProvider::from_config(&cfg).is_none());
        cfg.microsoft_client_id = Some("id".into());
        assert!(
            MicrosoftProvider::from_config(&cfg).is_none(),
            "still missing secret"
        );
        cfg.microsoft_client_secret = Some("secret".into());
        let provider = MicrosoftProvider::from_config(&cfg).expect("configured");
        // tenant defaults to "common" when RTDB_MICROSOFT_TENANT is unset.
        assert_eq!(provider.tenant, "common");
    }

    /// Minimal `Config` with every OAuth provider unconfigured — shared by the
    /// `from_config` test. Mirrors the literal `Config { ... }` the other
    /// provider tests construct.
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
            static_dir: None,
            pool_max_connections: 75,
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
        }
    }
}
