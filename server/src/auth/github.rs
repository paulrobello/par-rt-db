use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::auth::provider::OAuthProvider;
use crate::auth::session;
use crate::config::Config;
use crate::db::{new_id, now_ms};
use crate::error::RtDbError;

/// GitHub OAuth provider. One GitHub OAuth App serves the whole instance
/// (see spec §"Users"); `client_id`/`client_secret` come from config, the
/// base/API URLs are overridable for GitHub Enterprise.
pub struct GithubProvider {
    client_id: String,
    client_secret: String,
    base_url: String,
    api_url: String,
}

impl GithubProvider {
    fn redirect_uri(&self, public_url: &str) -> String {
        format!("{public_url}/auth/callback")
    }
}

#[async_trait]
impl OAuthProvider for GithubProvider {
    fn name() -> &'static str {
        "github"
    }

    fn from_config(config: &Config) -> Option<Self> {
        let client_id = config.github_client_id.clone()?;
        let client_secret = config.github_client_secret.clone()?;
        Some(Self {
            client_id,
            client_secret,
            base_url: config.github_base_url.clone(),
            api_url: config.github_api_url.clone(),
        })
    }

    fn callback_path(&self) -> &'static str {
        "/auth/callback"
    }

    fn authorize_url(&self, redirect_uri: &str, state: &str) -> String {
        format!(
            "{}/login/oauth/authorize?client_id={}&redirect_uri={redirect_uri}&scope=read:user%20user:email&state={state}",
            self.base_url, self.client_id,
        )
    }

    async fn complete_login(&self, state: &Arc<AppState>, code: &str) -> Result<String, RtDbError> {
        let client = reqwest::Client::new();
        let redirect_uri = self.redirect_uri(&state.config.public_url);

        let token_resp: serde_json::Value = client
            .post(format!("{}/login/oauth/access_token", self.base_url))
            .header(reqwest::header::ACCEPT, "application/json")
            .form(&TokenExchangeRequest {
                client_id: &self.client_id,
                client_secret: &self.client_secret,
                code,
                redirect_uri: &redirect_uri,
            })
            .send()
            .await
            .map_err(|err| {
                tracing::warn!(error = %err, "github token exchange request failed");
                RtDbError::internal("github token exchange failed")
            })?
            .json()
            .await
            .map_err(|err| {
                tracing::warn!(error = %err, "github token exchange response decode failed");
                RtDbError::internal("github token exchange failed")
            })?;

        let access_token = parse_token_response(token_resp)?;

        let user: GithubUser = client
            .get(format!("{}/user", self.api_url))
            .header(reqwest::header::USER_AGENT, "par-rt-db")
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {access_token}"),
            )
            .send()
            .await
            .map_err(|err| {
                tracing::warn!(error = %err, "github user fetch request failed");
                RtDbError::internal("github user fetch failed")
            })?
            .json()
            .await
            .map_err(|err| {
                tracing::warn!(error = %err, "github user fetch response decode failed");
                RtDbError::internal("github user fetch failed")
            })?;

        let emails: Vec<GithubEmail> = client
            .get(format!("{}/user/emails", self.api_url))
            .header(reqwest::header::USER_AGENT, "par-rt-db")
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {access_token}"),
            )
            .send()
            .await
            .map_err(|err| {
                tracing::warn!(error = %err, "github email fetch request failed");
                RtDbError::internal("github email fetch failed")
            })?
            .json()
            .await
            .map_err(|err| {
                tracing::warn!(error = %err, "github email fetch response decode failed");
                RtDbError::internal("github email fetch failed")
            })?;

        let email = select_email(&user.email, &emails)
            .ok_or_else(|| RtDbError::forbidden("no verified email"))?
            .to_lowercase();

        let id = new_id();
        let now = now_ms();
        let (user_id,): (String,) = sqlx::query_as(
            "INSERT INTO rtdb_auth.users (id, github_id, login, email, created_at) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (github_id) DO UPDATE SET login = EXCLUDED.login, email = EXCLUDED.email \
             RETURNING id",
        )
        .bind(&id)
        .bind(user.id)
        .bind(&user.login)
        .bind(&email)
        .bind(now)
        .fetch_one(&state.pool)
        .await?;

        session::create_session(&state.pool, &user_id, state.config.session_ttl_days).await
    }
}

#[derive(Serialize)]
struct TokenExchangeRequest<'a> {
    client_id: &'a str,
    client_secret: &'a str,
    code: &'a str,
    redirect_uri: &'a str,
}

#[derive(Deserialize)]
struct GithubUser {
    id: i64,
    login: String,
    email: Option<String>,
}

#[derive(Deserialize)]
struct GithubEmail {
    email: String,
    primary: bool,
    verified: bool,
}

/// Extracts the access token from GitHub's token-exchange response. Happy
/// path: `{"access_token": "...", ...}`. Error path: GitHub returns
/// `{"error": "...", "error_description": "..."}` with no `access_token` —
/// surfaced as a generic internal error so the OAuth error text never reaches
/// the response body.
fn parse_token_response(value: serde_json::Value) -> Result<String, RtDbError> {
    match value.get("access_token").and_then(|v| v.as_str()) {
        Some(token) => Ok(token.to_string()),
        None => {
            tracing::warn!(response = ?value, "github token exchange returned no access_token");
            Err(RtDbError::internal("github token exchange failed"))
        }
    }
}

/// Picks the best email from a GitHub profile: the primary verified address,
/// falling back to any verified address, then to the profile-level email.
/// Mirrors GitHub's own recommended selection order. Returns the address
/// before lowercasing; the caller normalizes case.
fn select_email(profile_email: &Option<String>, emails: &[GithubEmail]) -> Option<String> {
    emails
        .iter()
        .find(|e| e.primary && e.verified)
        .or_else(|| emails.iter().find(|e| e.verified))
        .map(|e| e.email.clone())
        .or_else(|| profile_email.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_token_response_returns_access_token_on_success() {
        let resp = json!({"access_token": "gho_abc", "token_type": "bearer", "scope": "user"});
        assert_eq!(parse_token_response(resp).unwrap(), "gho_abc");
    }

    #[test]
    fn parse_token_response_fails_on_error_body_without_access_token() {
        let resp = json!({"error": "bad_verification_code", "error_description": "expired"});
        assert!(parse_token_response(resp).is_err());
    }

    #[test]
    fn select_email_prefers_primary_verified() {
        let emails = vec![
            GithubEmail {
                email: "secondary@example.com".into(),
                primary: false,
                verified: true,
            },
            GithubEmail {
                email: "primary@example.com".into(),
                primary: true,
                verified: true,
            },
        ];
        assert_eq!(
            select_email(&None, &emails),
            Some("primary@example.com".to_string())
        );
    }

    #[test]
    fn select_email_falls_back_to_any_verified() {
        let emails = vec![GithubEmail {
            email: "only@example.com".into(),
            primary: false,
            verified: true,
        }];
        assert_eq!(
            select_email(&None, &emails),
            Some("only@example.com".to_string())
        );
    }

    #[test]
    fn select_email_falls_back_to_profile_email_when_none_verified() {
        let emails = vec![GithubEmail {
            email: "unverified@example.com".into(),
            primary: true,
            verified: false,
        }];
        assert_eq!(
            select_email(&Some("profile@example.com".into()), &emails),
            Some("profile@example.com".to_string())
        );
    }

    #[test]
    fn select_email_returns_none_when_no_email_anywhere() {
        assert_eq!(select_email(&None, &[]), None);
    }

    #[test]
    fn authorize_url_contains_scope_and_state() {
        let provider = GithubProvider {
            client_id: "cid".into(),
            client_secret: "secret".into(),
            base_url: "https://github.example".into(),
            api_url: "https://api.github.example".into(),
        };
        let url = provider.authorize_url("https://app.example/auth/callback", "xyz");
        assert!(url.starts_with("https://github.example/login/oauth/authorize?"));
        assert!(url.contains("client_id=cid"));
        assert!(url.contains("scope=read:user%20user:email"));
        assert!(url.contains("state=xyz"));
        // redirect_uri is interpolated raw (not percent-encoded), matching the
        // original GitHub handler — browsers and GitHub accept it as-is.
        assert!(url.contains("redirect_uri=https://app.example/auth/callback"));
    }

    #[test]
    fn from_config_returns_none_without_credentials() {
        let mut cfg = Config {
            port: 0,
            database_url: "x".into(),
            admin_key: "k".into(),
            public_url: "http://localhost:0".into(),
            allowed_origins: vec![],
            github_client_id: None,
            github_client_secret: None,
            github_base_url: "https://github.com".into(),
            github_api_url: "https://api.github.com".into(),
            google_client_id: None,
            google_client_secret: None,
            session_ttl_days: 30,
        };
        assert!(GithubProvider::from_config(&cfg).is_none());
        cfg.github_client_id = Some("id".into());
        // still none: secret missing
        assert!(GithubProvider::from_config(&cfg).is_none());
        cfg.github_client_secret = Some("secret".into());
        assert!(GithubProvider::from_config(&cfg).is_some());
    }
}
