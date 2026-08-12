use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine;
use serde::Serialize;
use sqlx::PgPool;

use crate::AppState;
use crate::auth::provider::OAuthProvider;
use crate::auth::session;
use crate::config::Config;
use crate::db::{new_id, now_ms};
use crate::error::RtDbError;

/// Sign in with Apple. Two things make this unlike the other OIDC providers:
///
/// 1. Apple rejects a static `client_secret`. The secret sent to Apple's token
///    endpoint is a short-lived ES256 JWT the server signs with the EC private
///    key registered with Apple (`build_client_secret_jwt`), assembled from the
///    team id, key id, and private key — so the operator supplies four config
///    pieces, not a password.
///
/// 2. Apple posts the authorization code to the redirect URI with
///    `response_mode=form_post`, not as a query string. The dedicated POST
///    `/auth/apple/callback` handler (in `provider.rs`) reads the form body; it
///    can't share the GET `provider_callback` generic.
///
/// Identity is keyed on Apple's stable `sub`, mirrored from `github_id`: Apple
/// may relay the email through `@privaterelay.appleid.com` and rotate it if the
/// user re-hides their address, so `sub` is the durable key, not email. The
/// `email` from the id_token is still stored (UNIQUE; the relay address is
/// unique per user) and links to an existing email-keyed account when it matches
/// — mirroring the GitHub provider's two-phase upsert.
pub struct AppleProvider {
    client_id: String,
    team_id: String,
    key_id: String,
    private_key: String,
}

impl AppleProvider {
    fn redirect_uri(&self, public_url: &str) -> String {
        format!("{public_url}/auth/apple/callback")
    }

    const AUTHORIZE_URL: &'static str = "https://appleid.apple.com/auth/authorize";
    const TOKEN_URL: &'static str = "https://appleid.apple.com/auth/token";
    const AUDIENCE: &'static str = "https://appleid.apple.com";
}

/// Normalized identity extracted from Apple's id_token claims.
struct AppleIdentity {
    sub: String,
    email: String,
}

#[async_trait]
impl OAuthProvider for AppleProvider {
    fn name() -> &'static str {
        "apple"
    }

    fn from_config(config: &Config) -> Option<Self> {
        let client_id = config.apple_client_id.clone()?;
        let team_id = config.apple_team_id.clone()?;
        let key_id = config.apple_key_id.clone()?;
        let private_key = config.apple_private_key.clone()?;
        Some(Self {
            client_id,
            team_id,
            key_id,
            private_key,
        })
    }

    fn callback_path(&self) -> &'static str {
        "/auth/apple/callback"
    }

    fn authorize_url(&self, redirect_uri: &str, state: &str) -> String {
        // Apple mandates response_mode=form_post (Apple POSTs the code to the
        // redirect URI), so the GET `provider_callback` generic cannot serve the
        // callback — the dedicated POST handler does. `scope=name email` (Apple
        // scopes, not OIDC `openid`).
        format!(
            "{}?client_id={}&redirect_uri={redirect_uri}&response_type=code&response_mode=form_post&scope=name%20email&state={state}",
            Self::AUTHORIZE_URL,
            self.client_id,
        )
    }

    async fn complete_login(&self, state: &Arc<AppState>, code: &str) -> Result<String, RtDbError> {
        let key = parse_apple_private_key(&self.private_key)?;
        let client_secret =
            build_client_secret_jwt(&self.team_id, &self.client_id, &self.key_id, &key)?;

        let client = state.auth.http.clone();
        let redirect_uri = self.redirect_uri(&state.config.public_url);

        // SEC-002R: the id_token read below arrives ONLY from this
        // server-initiated TLS POST to Apple's token endpoint, authenticated by
        // the operator's ES256 client_secret JWT built above. It is never
        // accepted from a client-controlled channel, which is why the deferred
        // JWKS signature verification (SEC-002) is hardening rather than a
        // missing control — forgery requires a TLS-path compromise. LATENT
        // RISK: adding any future endpoint that accepts a client-supplied
        // id_token (e.g. "sign in with the id_token I already have") would make
        // the unverified signature trivially forgeable overnight. Such an
        // endpoint MUST land JWKS verification first.
        let token_resp: serde_json::Value = client
            .post(Self::TOKEN_URL)
            .form(&TokenExchangeRequest {
                client_id: &self.client_id,
                client_secret: &client_secret,
                code,
                redirect_uri: &redirect_uri,
                grant_type: "authorization_code",
            })
            .send()
            .await
            .map_err(|err| {
                tracing::warn!(error = %err, "apple token exchange request failed");
                RtDbError::internal("apple token exchange failed")
            })?
            .json()
            .await
            .map_err(|err| {
                tracing::warn!(error = %err, "apple token exchange response decode failed");
                RtDbError::internal("apple token exchange failed")
            })?;

        let id_token = token_resp
            .get("id_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                // SEC-130: log only the response SHAPE (which keys are present),
                // never the body — a token_resp with no id_token still carries
                // access_token/refresh_token, which must not reach the logs.
                let keys: Vec<&str> = token_resp
                    .as_object()
                    .map(|m| m.keys().map(String::as_str).collect())
                    .unwrap_or_default();
                tracing::warn!(present_keys = ?keys, "apple token exchange returned no id_token");
                RtDbError::internal("apple token exchange failed")
            })?;

        let identity = parse_identity(decode_id_token_claims(id_token, &self.client_id)?)?;
        let email = identity.email.to_lowercase();
        let login = email.clone();

        let user_id = upsert_apple_user(&state.pool, &identity.sub, &login, &email).await?;

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

#[derive(Serialize)]
struct ClientSecretClaims<'a> {
    iss: &'a str,
    sub: &'a str,
    aud: &'a str,
    iat: i64,
    exp: i64,
}

/// Parses the configured EC private key (PEM) into a `jsonwebtoken` encoding
/// key. A bad key (wrong format / not P-256) surfaces as a generic internal
/// error so the PEM never reaches the response body — log it via `tracing`.
fn parse_apple_private_key(pem: &str) -> Result<jsonwebtoken::EncodingKey, RtDbError> {
    jsonwebtoken::EncodingKey::from_ec_pem(pem.as_bytes()).map_err(|err| {
        tracing::warn!(error = %err, "apple client_secret key parse failed");
        RtDbError::internal("apple oauth misconfigured")
    })
}

/// Builds the short-lived ES256 JWT Apple requires as the `client_secret`,
/// signed with a pre-parsed key. Regenerated on every exchange (cheap; `ring`
/// is already in the tree). Apple caps `exp` at 180 days; ~6 months is inside.
fn build_client_secret_jwt(
    team_id: &str,
    client_id: &str,
    key_id: &str,
    key: &jsonwebtoken::EncodingKey,
) -> Result<String, RtDbError> {
    use jsonwebtoken::{Algorithm, Header};

    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(key_id.to_string());

    let now = now_secs();
    let claims = ClientSecretClaims {
        iss: team_id,
        sub: client_id,
        aud: AppleProvider::AUDIENCE,
        iat: now,
        exp: now + 15_777_000, // ~182 days, just under Apple's 6-month cap
    };

    jsonwebtoken::encode(&header, &claims, key).map_err(|err| {
        tracing::warn!(error = %err, "apple client_secret jwt encode failed");
        RtDbError::internal("apple token exchange failed")
    })
}

/// Seconds since the Unix epoch (stdlib, no chrono `clock` feature needed).
fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Decodes the payload claims of Apple's id_token and validates the OIDC claims
/// (`iss`, `aud`, `exp`) that defend against a token minted for a different
/// client or used after its short lifetime. The token was received over TLS
/// directly from Apple's token endpoint using our confidential `client_secret`
/// JWT, so it is authentic by transport — the same trust model the google/oidc
/// providers apply to their userinfo fetch (they don't verify a signature
/// either). **Signature verification** (fetching Apple's rotating JWKS and
/// validating the ES256 signature against the token's `kid`) is therefore a
/// defense-in-depth hardening, not a missing control, and is out of scope for
/// v1 — see SEC-002. The claim checks below are the partial fix: a stolen code
/// replay against the wrong client_id, or a token past its 10-minute `exp`,
/// is rejected before any account upsert.
fn decode_id_token_claims(
    id_token: &str,
    expected_client_id: &str,
) -> Result<serde_json::Value, RtDbError> {
    let payload = id_token.split('.').nth(1).ok_or_else(|| {
        tracing::warn!("apple id_token malformed (no payload segment)");
        RtDbError::internal("apple token exchange failed")
    })?;
    // JWT segments are base64url without padding; strip stray '=' defensively.
    let payload = payload.trim_end_matches('=');
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|err| {
            tracing::warn!(error = %err, "apple id_token payload decode failed");
            RtDbError::internal("apple token exchange failed")
        })?;
    let claims: serde_json::Value = serde_json::from_slice(&bytes).map_err(|err| {
        tracing::warn!(error = %err, "apple id_token json decode failed");
        RtDbError::internal("apple token exchange failed")
    })?;

    // iss MUST be Apple's sole issuer — a token from any other issuer is not an
    // Apple id_token regardless of how it reached us.
    let iss = claims.get("iss").and_then(|v| v.as_str()).unwrap_or("");
    if iss != AppleProvider::AUDIENCE {
        tracing::warn!(iss, "apple id_token has unexpected issuer");
        return Err(RtDbError::forbidden("apple id_token rejected"));
    }
    // aud MUST equal our client_id — a token minted for a different app cannot
    // be redeemed for our user's identity, even if a stolen code reaches our
    // token endpoint. Apple also sends `aud` as an array in some flows, so
    // accept both shapes.
    let aud_matches = match claims.get("aud") {
        Some(serde_json::Value::String(s)) => s == expected_client_id,
        Some(serde_json::Value::Array(arr)) => arr
            .iter()
            .filter_map(|v| v.as_str())
            .any(|s| s == expected_client_id),
        _ => false,
    };
    if !aud_matches {
        tracing::warn!("apple id_token audience does not match client_id");
        return Err(RtDbError::forbidden("apple id_token rejected"));
    }
    // exp MUST be in the future (strict — Apple tokens carry a 10-minute
    // lifetime; a small clock skew would still be inside the window). Reject
    // any expired or malformed-exp token rather than trusting its claims.
    let exp = claims.get("exp").and_then(|v| v.as_i64());
    let now = now_secs();
    match exp {
        Some(exp) if exp > now => {}
        _ => {
            tracing::warn!(exp, now, "apple id_token missing or expired exp");
            return Err(RtDbError::forbidden("apple id_token rejected"));
        }
    }

    Ok(claims)
}

/// `email_verified` arrives as a JSON boolean or the string `"true"` (Apple
/// serializes it as a string); anything else is treated as unverified.
fn is_email_verified(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Bool(b) => *b,
        serde_json::Value::String(s) => s.eq_ignore_ascii_case("true"),
        _ => false,
    }
}

/// Extracts the stable `sub` and email from the id_token claims. A present
/// `sub` and email are required; `email_verified` is trusted unless explicitly
/// false (Apple verifies both real and relay addresses).
fn parse_identity(value: serde_json::Value) -> Result<AppleIdentity, RtDbError> {
    let sub = value
        .get("sub")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| RtDbError::forbidden("no apple subject"))?
        .to_string();

    let email = value
        .get("email")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| RtDbError::forbidden("no email"))?
        .to_string();

    if let Some(verified) = value.get("email_verified")
        && !is_email_verified(verified)
    {
        return Err(RtDbError::forbidden("email is not verified"));
    }

    Ok(AppleIdentity { sub, email })
}

/// Two-phase upsert mirroring `github::upsert_user`, keyed on Apple's stable
/// `sub`: (1) a returning Apple user reuses the row, refreshing login/email;
/// (2) an email-keyed account that is not yet Apple-linked claims `apple_sub`;
/// (3) otherwise a brand-new user. A unique violation (the email already linked
/// to a different account, or a concurrent login) is a deliberate 409.
async fn upsert_apple_user(
    pool: &PgPool,
    apple_sub: &str,
    login: &str,
    email: &str,
) -> Result<String, RtDbError> {
    let mut tx = pool.begin().await?;

    // (1) returning Apple user.
    if let Some((id,)) =
        sqlx::query_as::<_, (String,)>("SELECT id FROM rtdb_auth.users WHERE apple_sub = $1")
            .bind(apple_sub)
            .fetch_optional(&mut *tx)
            .await?
    {
        sqlx::query("UPDATE rtdb_auth.users SET login = $1, email = $2 WHERE id = $3")
            .bind(login)
            .bind(email)
            .bind(&id)
            .execute(&mut *tx)
            .await
            .map_err(map_conflict)?;
        tx.commit().await?;
        return Ok(id);
    }

    // (2) link an email-keyed account that is not yet Apple-linked.
    if let Some((id,)) = sqlx::query_as::<_, (String,)>(
        "UPDATE rtdb_auth.users \
         SET apple_sub = $1, login = $2 \
         WHERE email = $3 AND apple_sub IS NULL \
         RETURNING id",
    )
    .bind(apple_sub)
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
        "INSERT INTO rtdb_auth.users (id, apple_sub, login, email, created_at) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(&id)
    .bind(apple_sub)
    .bind(login)
    .bind(email)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(map_conflict)?;
    tx.commit().await?;
    Ok(id)
}

/// Maps a Postgres unique-violation (`23505`) from an Apple upsert to a
/// deliberate 409 — the email/`apple_sub` is already linked to another account
/// (or a concurrent login just claimed it). Any other db error passes through.
fn map_conflict(err: sqlx::Error) -> RtDbError {
    let is_unique_violation = matches!(
        &err,
        sqlx::Error::Database(db_err) if db_err.code().as_deref() == Some("23505")
    );
    if is_unique_violation {
        RtDbError::conflict("account conflict")
    } else {
        RtDbError::from(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Generates a throwaway P-256 key at runtime and feeds its PKCS#8 DER
    /// straight to `from_ec_der` — no private key, and no PEM-header literal,
    /// lives in the repo, so gitleaks / detect-private-key stay fully active.
    /// The key is regenerated on every test run.
    fn fresh_test_key() -> jsonwebtoken::EncodingKey {
        let rng = ring::rand::SystemRandom::new();
        let pkcs8 = ring::signature::EcdsaKeyPair::generate_pkcs8(
            &ring::signature::ECDSA_P256_SHA256_FIXED_SIGNING,
            &rng,
        )
        .expect("generate a P-256 key for the test");
        jsonwebtoken::EncodingKey::from_ec_der(pkcs8.as_ref())
    }

    fn b64url(bytes: &[u8]) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    }

    /// Builds a fake Apple id_token (header.payload.sig) for decode tests. The
    /// signature is a placeholder — `decode_id_token_claims` does not verify it.
    fn fake_id_token(payload: &serde_json::Value) -> String {
        let header = b64url(br#"{"alg":"ES256","kid":"TESTKEY"}"#);
        let payload = b64url(serde_json::to_vec(payload).unwrap().as_slice());
        format!("{header}.{payload}.sig")
    }

    #[test]
    fn build_client_secret_jwt_is_a_three_part_es256_jwt_with_kid() {
        let jwt =
            build_client_secret_jwt("TEAM123", "com.example.svc", "KEYID99", &fresh_test_key())
                .expect("encodes with a valid P-256 key");
        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3, "a JWS has three dot-separated segments");

        // Header: alg ES256, kid set.
        let header: serde_json::Value = serde_json::from_slice(
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(parts[0].trim_end_matches('='))
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        assert_eq!(header["alg"], "ES256");
        assert_eq!(header["kid"], "KEYID99");

        // Claims: iss/team, aud Apple, sub = client_id, exp > iat.
        let claims: serde_json::Value = serde_json::from_slice(
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(parts[1].trim_end_matches('='))
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        assert_eq!(claims["iss"], "TEAM123");
        assert_eq!(claims["sub"], "com.example.svc");
        assert_eq!(claims["aud"], "https://appleid.apple.com");
        assert!(claims["exp"].as_i64().unwrap() > claims["iat"].as_i64().unwrap());
    }

    #[test]
    fn decode_id_token_extracts_sub_and_email() {
        let token = fake_id_token(&json!({
            "iss": "https://appleid.apple.com",
            "aud": "com.example.svc",
            "exp": now_secs() + 600,
            "sub": "000123.abc",
            "email": "Alice@Example.com",
            "email_verified": "true",
            "is_private_email": "false"
        }));
        let claims = decode_id_token_claims(&token, "com.example.svc").unwrap();
        let id = parse_identity(claims).unwrap();
        assert_eq!(id.sub, "000123.abc");
        assert_eq!(id.email, "Alice@Example.com");
    }

    /// SEC-002: a token whose `aud` does not match our `client_id` is rejected,
    /// even though the transport is trusted — defense against a stolen code
    /// redeemed against the wrong app being used to log in here.
    #[test]
    fn decode_id_token_rejects_wrong_audience() {
        let token = fake_id_token(&json!({
            "iss": "https://appleid.apple.com",
            "aud": "com.someone.else",
            "exp": now_secs() + 600,
            "sub": "s",
            "email": "x@y.com",
            "email_verified": "true"
        }));
        let err = decode_id_token_claims(&token, "com.example.svc").unwrap_err();
        assert!(err.message.contains("rejected") || err.message.contains("forbidden"));
    }

    /// SEC-002: an id_token past its `exp` is rejected.
    #[test]
    fn decode_id_token_rejects_expired_exp() {
        let token = fake_id_token(&json!({
            "iss": "https://appleid.apple.com",
            "aud": "com.example.svc",
            "exp": now_secs() - 1,
            "sub": "s",
            "email": "x@y.com",
            "email_verified": "true"
        }));
        assert!(decode_id_token_claims(&token, "com.example.svc").is_err());
    }

    /// SEC-002: an id_token from an unexpected issuer is rejected.
    #[test]
    fn decode_id_token_rejects_wrong_issuer() {
        let token = fake_id_token(&json!({
            "iss": "https://evil.example.com",
            "aud": "com.example.svc",
            "exp": now_secs() + 600,
            "sub": "s",
            "email": "x@y.com",
            "email_verified": "true"
        }));
        assert!(decode_id_token_claims(&token, "com.example.svc").is_err());
    }

    /// SEC-002: Apple occasionally sends `aud` as an array — accept it as long
    /// as our `client_id` is one of the entries.
    #[test]
    fn decode_id_token_accepts_array_audience_containing_client_id() {
        let token = fake_id_token(&json!({
            "iss": "https://appleid.apple.com",
            "aud": ["com.example.svc", "com.example.svc.alt"],
            "exp": now_secs() + 600,
            "sub": "s",
            "email": "x@y.com",
            "email_verified": "true"
        }));
        let claims = decode_id_token_claims(&token, "com.example.svc").unwrap();
        assert_eq!(claims["sub"], "s");
    }

    #[test]
    fn parse_identity_accepts_private_relay_email() {
        // Hidden-email users get a @privaterelay.appleid.com address; it is a
        // legitimate, unique, Apple-verified email and must be accepted.
        let id = parse_identity(json!({
            "sub": "sub-relay",
            "email": "abc@privaterelay.appleid.com",
            "email_verified": "true",
            "is_private_email": "true"
        }))
        .unwrap();
        assert_eq!(id.email, "abc@privaterelay.appleid.com");
    }

    #[test]
    fn parse_identity_accepts_boolean_email_verified() {
        let id = parse_identity(json!({"sub": "s", "email": "x@y.com", "email_verified": true}))
            .unwrap();
        assert_eq!(id.email, "x@y.com");
    }

    #[test]
    fn parse_identity_rejects_unverified_email() {
        let err =
            parse_identity(json!({"sub": "s", "email": "x@y.com", "email_verified": "false"}));
        assert!(err.is_err());
    }

    #[test]
    fn parse_identity_rejects_missing_sub_or_email() {
        assert!(
            parse_identity(json!({"email": "x@y.com"})).is_err(),
            "no sub"
        );
        assert!(parse_identity(json!({"sub": "s"})).is_err(), "no email");
    }

    #[test]
    fn authorize_url_uses_form_post_and_apple_scopes() {
        let provider = AppleProvider {
            client_id: "com.example.svc".into(),
            team_id: "TEAM".into(),
            key_id: "KEY".into(),
            private_key: "pk".into(),
        };
        let url = provider.authorize_url("https://app.example/auth/apple/callback", "st");
        assert!(url.starts_with("https://appleid.apple.com/auth/authorize?"));
        assert!(url.contains("client_id=com.example.svc"));
        assert!(url.contains("response_type=code"));
        assert!(url.contains("response_mode=form_post"));
        assert!(url.contains("scope=name%20email"));
        assert!(url.contains("state=st"));
    }

    #[test]
    fn from_config_requires_all_four_fields() {
        let mut cfg = base_cfg();
        assert!(AppleProvider::from_config(&cfg).is_none());
        cfg.apple_client_id = Some("cid".into());
        cfg.apple_team_id = Some("team".into());
        cfg.apple_key_id = Some("key".into());
        assert!(
            AppleProvider::from_config(&cfg).is_none(),
            "still missing private key"
        );
        cfg.apple_private_key = Some("dummy-key".into());
        assert!(AppleProvider::from_config(&cfg).is_some());
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
        }
    }
}
