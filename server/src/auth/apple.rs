//! Apple OAuth provider (`/auth/apple/*`): an ES256 JWT `client_secret` minted
//! from the private key, `response_mode=form_post` callbacks, and user/email
//! derived from the first `id_token` (name only on first authorization).
//! Stable identity keys on Apple's `sub`. The `id_token` signature is verified
//! against Apple's JWKS (SEC-004), sharing the cache in `auth::jwks`.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::Serialize;
use sqlx::PgPool;

use crate::AppState;
use crate::auth::jwks;
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
    /// Apple's rotating public key set, used to verify the `id_token`
    /// signature (SEC-004). Keys rotate, so this is fetched and cached, never
    /// pinned.
    const JWKS_URL: &'static str = "https://appleid.apple.com/auth/keys";
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

        // SEC-004: the id_token read below is verified against Apple's JWKS by
        // `verify_id_token`, so its authenticity no longer rests on the
        // transport. A future endpoint that accepts a client-supplied id_token
        // would therefore be safe to add on this path.
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

        let claims = verify_id_token(&client, id_token, &self.client_id).await?;
        let identity = parse_identity(claims)?;
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

/// Rejection text for every id_token failure. One message for all of them:
/// which check failed (bad signature, wrong `aud`, expired) is a detail the
/// caller must not be able to enumerate. The specific cause is logged.
const ID_TOKEN_REJECTED: &str = "apple id_token rejected";

/// Verifies Apple's `id_token` (SEC-004) and returns its claims.
///
/// The token's `kid` — read from the **unverified** header purely as a lookup
/// key — selects a public key from Apple's rotating JWKS, and a single
/// `jsonwebtoken::decode` then checks the signature together with `iss`, `aud`,
/// and `exp`. Nothing in the token is trusted until that call returns, so a
/// forged token cannot select its own verification algorithm.
///
/// Apple publishes RS256 keys today. `jwks::select_key` derives the algorithm
/// from the published key material rather than the token header, so an EC
/// (ES256) rotation verifies with no code change, while an unexpected key type
/// is rejected instead of guessed at.
async fn verify_id_token(
    http: &reqwest::Client,
    id_token: &str,
    expected_client_id: &str,
) -> Result<serde_json::Value, RtDbError> {
    let kid = jwks::unverified_kid(id_token, ID_TOKEN_REJECTED)?;
    let key_set = jwks::fetch(
        http,
        AppleProvider::JWKS_URL,
        "apple id_token verification failed",
    )
    .await?;
    let key = jwks::select_key(&key_set, &kid, ID_TOKEN_REJECTED)?;
    // `iss` must be Apple's sole issuer and `aud` our own client_id: a token
    // minted for a different app cannot be redeemed for our user's identity
    // even if a stolen code reaches our token endpoint. Apple sends `aud` as a
    // string or a one-element array; `jsonwebtoken` accepts both shapes.
    jwks::decode_verified(
        id_token,
        &key,
        AppleProvider::AUDIENCE,
        expected_client_id,
        ID_TOKEN_REJECTED,
    )
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
    // Only the test helpers base64-encode now that `verify_id_token` delegates
    // decoding to `jwks`.
    use base64::Engine;
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

    // --- SEC-004: id_token signature verification --------------------------
    // Apple's JWKS URL is a single constant, so every test in this module
    // shares one cache entry. `apple_test_keys` seeds it exactly once with both
    // a P-256 (ES256) and an RSA (RS256) key under distinct kids; a test that
    // needs a rejection signs with a kid or a key that is not in that set.
    // Keys are generated at runtime — no PEM or key literal lives in the repo,
    // so gitleaks / detect-private-key stay meaningful.

    const CLIENT_ID: &str = "com.example.svc";
    const EC_KID: &str = "apple-ec-kid";
    const RSA_KID: &str = "apple-rsa-kid";

    struct TestKeys {
        ec_pkcs8: Vec<u8>,
        rsa: rsa::RsaPrivateKey,
    }

    /// Generates the key pair set once per test binary and seeds Apple's JWKS
    /// URL in the shared cache with both public halves.
    async fn apple_test_keys() -> &'static TestKeys {
        static KEYS: tokio::sync::OnceCell<TestKeys> = tokio::sync::OnceCell::const_new();
        KEYS.get_or_init(|| async {
            use ring::signature::KeyPair;
            use rsa::traits::PublicKeyParts;

            let rng = ring::rand::SystemRandom::new();
            let ec_pkcs8 = ring::signature::EcdsaKeyPair::generate_pkcs8(
                &ring::signature::ECDSA_P256_SHA256_FIXED_SIGNING,
                &rng,
            )
            .expect("generate a P-256 key for the test")
            .as_ref()
            .to_vec();
            let ec_pair = ring::signature::EcdsaKeyPair::from_pkcs8(
                &ring::signature::ECDSA_P256_SHA256_FIXED_SIGNING,
                &ec_pkcs8,
                &rng,
            )
            .expect("parse the generated P-256 key");
            // ring hands out the uncompressed SEC1 point: 0x04 || x(32) || y(32).
            let point = ec_pair.public_key().as_ref().to_vec();
            let (x, y) = point[1..].split_at(32);

            let rsa = rsa::RsaPrivateKey::new(&mut rand::rngs::OsRng, 2048)
                .expect("generate RSA-2048 key");

            jwks::seed_for_test(
                AppleProvider::JWKS_URL,
                json!({"keys": [
                    {"kty": "EC", "use": "sig", "kid": EC_KID, "crv": "P-256",
                     "x": b64url(x), "y": b64url(y)},
                    {"kty": "RSA", "use": "sig", "kid": RSA_KID, "alg": "RS256",
                     "n": b64url(&rsa.n().to_bytes_be()), "e": b64url(&rsa.e().to_bytes_be())},
                ]}),
            )
            .await;

            TestKeys { ec_pkcs8, rsa }
        })
        .await
    }

    /// Signs `claims` as a real ES256 JWT under `kid`.
    fn sign_es256(pkcs8: &[u8], kid: &str, claims: &serde_json::Value) -> String {
        let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::ES256);
        header.kid = Some(kid.to_string());
        jsonwebtoken::encode(
            &header,
            claims,
            &jsonwebtoken::EncodingKey::from_ec_der(pkcs8),
        )
        .expect("sign the test id_token")
    }

    /// Signs `claims` as a real RS256 JWT under `kid` — Apple's actual
    /// algorithm today.
    fn sign_rs256(key: &rsa::RsaPrivateKey, kid: &str, claims: &serde_json::Value) -> String {
        use rsa::pkcs1v15::SigningKey;
        use rsa::signature::{SignatureEncoding, Signer};
        use sha2::Sha256;

        let header = json!({"alg": "RS256", "typ": "JWT", "kid": kid});
        let signing_input = format!(
            "{}.{}",
            b64url(&serde_json::to_vec(&header).unwrap()),
            b64url(&serde_json::to_vec(claims).unwrap()),
        );
        let signature = SigningKey::<Sha256>::new(key.clone()).sign(signing_input.as_bytes());
        format!("{signing_input}.{}", b64url(&signature.to_vec()))
    }

    /// Claims of a well-formed Apple id_token.
    fn apple_claims() -> serde_json::Value {
        json!({
            "iss": "https://appleid.apple.com",
            "aud": CLIENT_ID,
            "exp": now_secs() + 600,
            "iat": now_secs(),
            "sub": "000123.abc",
            "email": "Alice@Example.com",
            "email_verified": "true",
            "is_private_email": "false"
        })
    }

    async fn verify(token: &str) -> Result<serde_json::Value, RtDbError> {
        verify_id_token(&reqwest::Client::new(), token, CLIENT_ID).await
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

    /// SEC-004: a genuinely RS256-signed token — Apple's algorithm today —
    /// verifies against the published JWKS and yields its claims.
    #[tokio::test]
    async fn verify_id_token_accepts_a_valid_rs256_token() {
        let keys = apple_test_keys().await;
        let token = sign_rs256(&keys.rsa, RSA_KID, &apple_claims());
        let id = parse_identity(verify(&token).await.unwrap()).unwrap();
        assert_eq!(id.sub, "000123.abc");
        assert_eq!(id.email, "Alice@Example.com");
    }

    /// SEC-004: the EC path. `select_key` reads ES256 off the `crv` of the JWKS
    /// entry, so an Apple key rotation to P-256 verifies with no code change.
    #[tokio::test]
    async fn verify_id_token_accepts_a_valid_es256_token() {
        let keys = apple_test_keys().await;
        let token = sign_es256(&keys.ec_pkcs8, EC_KID, &apple_claims());
        let id = parse_identity(verify(&token).await.unwrap()).unwrap();
        assert_eq!(id.sub, "000123.abc");
    }

    /// SEC-004, the control this whole change exists for: a token whose claims
    /// are perfect but whose signature was made by a key Apple never published
    /// is rejected. Before JWKS verification this token was accepted.
    #[tokio::test]
    async fn verify_id_token_rejects_a_signature_from_a_foreign_key() {
        apple_test_keys().await;
        let rng = ring::rand::SystemRandom::new();
        let attacker = ring::signature::EcdsaKeyPair::generate_pkcs8(
            &ring::signature::ECDSA_P256_SHA256_FIXED_SIGNING,
            &rng,
        )
        .expect("generate the attacker key")
        .as_ref()
        .to_vec();

        // Signed with the attacker's key but claiming Apple's published kid.
        let forged = sign_es256(&attacker, EC_KID, &apple_claims());
        let err = verify(&forged).await.unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::Forbidden);
        assert_eq!(err.message, ID_TOKEN_REJECTED);
    }

    /// SEC-004: tampering with the payload of an otherwise valid token breaks
    /// the signature over `header.payload`.
    #[tokio::test]
    async fn verify_id_token_rejects_a_tampered_payload() {
        let keys = apple_test_keys().await;
        let token = sign_rs256(&keys.rsa, RSA_KID, &apple_claims());
        let mut parts: Vec<&str> = token.split('.').collect();

        let mut escalated = apple_claims();
        escalated["sub"] = json!("999999.attacker");
        let swapped = b64url(&serde_json::to_vec(&escalated).unwrap());
        parts[1] = &swapped;

        let err = verify(&parts.join(".")).await.unwrap_err();
        assert_eq!(err.message, ID_TOKEN_REJECTED);
    }

    /// SEC-004: a `kid` Apple has not published cannot select a key.
    #[tokio::test]
    async fn verify_id_token_rejects_an_unknown_kid() {
        let keys = apple_test_keys().await;
        let token = sign_rs256(&keys.rsa, "not-a-published-kid", &apple_claims());
        assert!(verify(&token).await.is_err());
    }

    /// A token whose `aud` does not match our `client_id` is rejected — defense
    /// against a stolen code redeemed against a different app.
    #[tokio::test]
    async fn verify_id_token_rejects_wrong_audience() {
        let keys = apple_test_keys().await;
        let mut claims = apple_claims();
        claims["aud"] = json!("com.someone.else");
        let err = verify(&sign_rs256(&keys.rsa, RSA_KID, &claims))
            .await
            .unwrap_err();
        assert_eq!(err.message, ID_TOKEN_REJECTED);
    }

    /// An id_token past its `exp` is rejected.
    #[tokio::test]
    async fn verify_id_token_rejects_expired_exp() {
        let keys = apple_test_keys().await;
        let mut claims = apple_claims();
        // An hour in the past, well beyond the default validation leeway.
        claims["exp"] = json!(now_secs() - 3600);
        assert!(
            verify(&sign_rs256(&keys.rsa, RSA_KID, &claims))
                .await
                .is_err()
        );
    }

    /// An id_token from an unexpected issuer is rejected.
    #[tokio::test]
    async fn verify_id_token_rejects_wrong_issuer() {
        let keys = apple_test_keys().await;
        let mut claims = apple_claims();
        claims["iss"] = json!("https://evil.example.com");
        assert!(
            verify(&sign_rs256(&keys.rsa, RSA_KID, &claims))
                .await
                .is_err()
        );
    }

    /// Apple sends `aud` as an array in some flows — accept it as long as our
    /// `client_id` is one of the entries.
    #[tokio::test]
    async fn verify_id_token_accepts_array_audience_containing_client_id() {
        let keys = apple_test_keys().await;
        let mut claims = apple_claims();
        claims["aud"] = json!([CLIENT_ID, "com.example.svc.alt"]);
        let verified = verify(&sign_rs256(&keys.rsa, RSA_KID, &claims))
            .await
            .unwrap();
        assert_eq!(verified["sub"], "000123.abc");
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
            forward_timeout_ms: 5000,
            instance_id: None,
        }
    }
}
