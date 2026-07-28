use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;

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
        let client_id = config.google_client_id.clone()?;
        let client_secret = config.google_client_secret.clone()?;
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
        let client = reqwest::Client::new();
        let redirect_uri = self.redirect_uri(&state.config.public_url);

        let token_resp: serde_json::Value = client
            .post(TOKEN_URL)
            .form(&TokenExchangeRequest {
                client_id: &self.client_id,
                client_secret: &self.client_secret,
                code,
                redirect_uri: &redirect_uri,
                grant_type: "authorization_code",
            })
            .send()
            .await
            .map_err(|err| {
                tracing::warn!(error = %err, "google token exchange request failed");
                RtDbError::internal("google token exchange failed")
            })?
            .json()
            .await
            .map_err(|err| {
                tracing::warn!(error = %err, "google token exchange response decode failed");
                RtDbError::internal("google token exchange failed")
            })?;

        let access_token = parse_token_response(token_resp)?;

        let userinfo: serde_json::Value = client
            .get(USERINFO_URL)
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {access_token}"),
            )
            .send()
            .await
            .map_err(|err| {
                tracing::warn!(error = %err, "google userinfo fetch request failed");
                RtDbError::internal("google userinfo fetch failed")
            })?
            .json()
            .await
            .map_err(|err| {
                tracing::warn!(error = %err, "google userinfo fetch response decode failed");
                RtDbError::internal("google userinfo fetch failed")
            })?;

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

#[derive(Serialize)]
struct TokenExchangeRequest<'a> {
    client_id: &'a str,
    client_secret: &'a str,
    code: &'a str,
    redirect_uri: &'a str,
    grant_type: &'a str,
}

/// Extracts the access token from Google's token-exchange response. Google
/// returns `{"access_token": "...", ...}` on success and
/// `{"error": "invalid_grant", "error_description": "..."}` on failure — the
/// latter is surfaced as a generic internal error.
fn parse_token_response(value: serde_json::Value) -> Result<String, RtDbError> {
    match value.get("access_token").and_then(|v| v.as_str()) {
        Some(token) => Ok(token.to_string()),
        None => {
            tracing::warn!(response = ?value, "google token exchange returned no access_token");
            Err(RtDbError::internal("google token exchange failed"))
        }
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
    use serde_json::json;

    #[test]
    fn parse_token_response_returns_access_token_on_success() {
        let resp = json!({"access_token": "ya29.abc", "token_type": "Bearer", "expires_in": 3599});
        assert_eq!(parse_token_response(resp).unwrap(), "ya29.abc");
    }

    #[test]
    fn parse_token_response_fails_on_error_body() {
        let resp = json!({"error": "invalid_grant", "error_description": "Bad code"});
        assert!(parse_token_response(resp).is_err());
    }

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
            github_client_id: None,
            github_client_secret: None,
            github_base_url: "https://github.com".into(),
            github_api_url: "https://api.github.com".into(),
            google_client_id: None,
            google_client_secret: None,
            max_affected_docs: 100,
            static_dir: None,
            pool_max_connections: 75,
            rate_limit_per_token_rpm: 0,
            rate_limit_per_db_rpm: 0,
        };
        assert!(GoogleProvider::from_config(&cfg).is_none());
        cfg.google_client_id = Some("id".into());
        assert!(GoogleProvider::from_config(&cfg).is_none());
        cfg.google_client_secret = Some("secret".into());
        assert!(GoogleProvider::from_config(&cfg).is_some());
    }
}
