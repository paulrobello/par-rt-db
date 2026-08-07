use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

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

        let email = select_email(&emails)
            .ok_or_else(|| RtDbError::forbidden("no verified email"))?
            .to_lowercase();

        let user_id = upsert_user(&state.pool, user.id, &user.login, &email).await?;
        session::create_session(
            &state.pool,
            &user_id,
            state.runtime.hot.load().session_ttl_days,
        )
        .await
    }
}

/// Upserts the GitHub user into `rtdb_auth.users`, linking across providers by
/// verified email so a cross-provider same-email login resolves deliberately
/// instead of surfacing the email UNIQUE constraint as a 500.
///
/// Resolution order:
/// 1. An existing user with this `github_id` (a returning GitHub user) is
///    reused, with `login`/`email` refreshed — so a GitHub email change follows
///    the account rather than forking it.
/// 2. Otherwise, if the verified email already belongs to an account that is
///    not yet GitHub-linked (`github_id` IS NULL — e.g. one created by a Google
///    login), that account is linked by setting its `github_id`, reusing the
///    row. Both providers verified the email, so this is the same person; the
///    GitHub flow's per-account stability is preserved because step 1 still
///    keys every returning GitHub user to their own `github_id`.
/// 3. Otherwise a new row is inserted. A UNIQUE violation here — an email
///    already linked to a *different* GitHub account, or a concurrent login
///    racing past the checks — is mapped to a deliberate 409 conflict rather
///    than leaked as a 500.
async fn upsert_user(
    pool: &PgPool,
    github_id: i64,
    login: &str,
    email: &str,
) -> Result<String, RtDbError> {
    let mut tx = pool.begin().await?;

    // (1) returning GitHub user: reuse the account, refresh login/email.
    if let Some((id,)) =
        sqlx::query_as::<_, (String,)>("SELECT id FROM rtdb_auth.users WHERE github_id = $1")
            .bind(github_id)
            .fetch_optional(&mut *tx)
            .await?
    {
        sqlx::query("UPDATE rtdb_auth.users SET login = $1, email = $2 WHERE id = $3")
            .bind(login)
            .bind(email)
            .bind(&id)
            .execute(&mut *tx)
            .await
            .map_err(map_email_conflict)?;
        tx.commit().await?;
        return Ok(id);
    }

    // (2) link an email-keyed account that is not yet GitHub-linked.
    if let Some((id,)) = sqlx::query_as::<_, (String,)>(
        "UPDATE rtdb_auth.users \
         SET github_id = $1, login = $2 \
         WHERE email = $3 AND github_id IS NULL \
         RETURNING id",
    )
    .bind(github_id)
    .bind(login)
    .bind(email)
    .fetch_optional(&mut *tx)
    .await?
    {
        tx.commit().await?;
        return Ok(id);
    }

    // (3) brand-new user.
    let id = new_id();
    let now = now_ms();
    sqlx::query(
        "INSERT INTO rtdb_auth.users (id, github_id, login, email, created_at) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(&id)
    .bind(github_id)
    .bind(login)
    .bind(email)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(map_email_conflict)?;
    tx.commit().await?;
    Ok(id)
}

/// Maps a Postgres unique-violation (`23505`) from a `users` upsert to a
/// deliberate 409 conflict — the email is already linked to another sign-in
/// method (or a concurrent login just claimed it). Any other database error
/// passes through as the usual internal-error mapping (logged, never leaked).
fn map_email_conflict(err: sqlx::Error) -> RtDbError {
    let is_unique_violation = matches!(
        &err,
        sqlx::Error::Database(db_err) if db_err.code().as_deref() == Some("23505")
    );
    if is_unique_violation {
        RtDbError::precondition("email already linked to another sign-in method")
    } else {
        RtDbError::from(err)
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
            tracing::warn!(response = ?value, "github token exchange returned no access_token");
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
            static_dir: None,
            pool_max_connections: 75,
            rate_limit_per_token_rpm: 0,
            rate_limit_per_db_rpm: 0,
            audit_log_enabled: false,
            oauth_login_csrf: true,
            webhooks_enabled: false,
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
        };
        assert!(GithubProvider::from_config(&cfg).is_none());
        cfg.github_client_id = Some("id".into());
        // still none: secret missing
        assert!(GithubProvider::from_config(&cfg).is_none());
        cfg.github_client_secret = Some("secret".into());
        assert!(GithubProvider::from_config(&cfg).is_some());
    }
}
