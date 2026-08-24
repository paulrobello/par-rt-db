//! GitHub OAuth provider (`/auth/github/*`): standard web flow; the user's
//! email resolves against the GitHub API when the primary email is private.
//! Enabled by `RTDB_GITHUB_CLIENT_ID` / `RTDB_GITHUB_CLIENT_SECRET`.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::auth::provider::OAuthProvider;
use crate::auth::{self, ConflictStyle, ProviderIdentity, session};
use crate::config::Config;
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
        let client_id = config.oauth.github.client_id.clone()?;
        let client_secret = config.oauth.github.client_secret.clone()?;
        Some(Self {
            client_id,
            client_secret,
            base_url: config.oauth.github.base_url.clone(),
            api_url: config.oauth.github.api_url.clone(),
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
        let client = state.auth.http.clone();
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

        let email = select_email(&emails)
            .ok_or_else(|| RtDbError::forbidden("no verified email"))?
            .to_lowercase();

        // QA-004: resolution (returning-user-by-github_id, then link an
        // unlinked email-keyed row, then insert) now lives in
        // `auth::resolve_user`, shared with every other provider. GitHub's
        // wire-facing conflict code (`PRECONDITION_FAILED`, asserted by
        // `oauth_test.rs`) is preserved via `ConflictStyle::Precondition`.
        let github_id_str = user.id.to_string();
        let user_id = auth::resolve_user(
            &state.pool,
            ProviderIdentity {
                provider_id_column: auth::PROVIDER_COL_GITHUB_ID,
                provider_id: &github_id_str,
                login: &user.login,
                email: &email,
                allow_email_link: true,
                conflict_style: ConflictStyle::Precondition,
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
            // SEC-205: log only the response SHAPE (which keys are present),
            // never the body — a response missing access_token can still carry
            // id_token/refresh_token fragments (same pattern as apple.rs).
            let keys: Vec<&str> = value
                .as_object()
                .map(|m| m.keys().map(String::as_str).collect())
                .unwrap_or_default();
            tracing::warn!(present_keys = ?keys, "github token exchange returned no access_token");
            Err(RtDbError::internal("github token exchange failed"))
        }
    }
}

/// Picks the best verified email from a GitHub profile: the primary verified
/// address if any, otherwise any verified address. The unverified profile-level
/// `email` field on the user object is deliberately NOT consulted — GitHub's
/// `emails` endpoint is the verified source of truth and always returns one
/// verified entry for a real account; falling back to the unverified profile
/// email would let an attacker set a victim's address as their public profile
/// email and be admitted if GitHub ever returned an empty `emails` array.
/// Returns the address before lowercasing; the caller normalizes case.
fn select_email(emails: &[GithubEmail]) -> Option<String> {
    emails
        .iter()
        .find(|e| e.primary && e.verified)
        .or_else(|| emails.iter().find(|e| e.verified))
        .map(|e| e.email.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::OAuthConfig;
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
            select_email(&emails),
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
        assert_eq!(select_email(&emails), Some("only@example.com".to_string()));
    }

    #[test]
    fn select_email_returns_none_when_none_verified() {
        // Security: unverified entries must never be admitted, even if the only
        // candidate is the primary. Regression guard for the dropped
        // profile-email fallback (SEC-006).
        let emails = vec![GithubEmail {
            email: "unverified@example.com".into(),
            primary: true,
            verified: false,
        }];
        assert_eq!(select_email(&emails), None);
    }

    #[test]
    fn select_email_returns_none_when_no_email_anywhere() {
        assert_eq!(select_email(&[]), None);
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
        assert!(GithubProvider::from_config(&cfg).is_none());
        cfg.oauth.github.client_id = Some("id".into());
        // still none: secret missing
        assert!(GithubProvider::from_config(&cfg).is_none());
        cfg.oauth.github.client_secret = Some("secret".into());
        assert!(GithubProvider::from_config(&cfg).is_some());
    }
}
