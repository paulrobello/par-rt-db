use std::sync::{Arc, LazyLock};
use std::time::Duration;

use async_trait::async_trait;
use moka::future::Cache;
use serde::Serialize;
use sqlx::PgPool;

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
/// the audience to one organization. **A specific tenant GUID is strongly
/// recommended**: `common`/`organizations` accept any Entra tenant, and a
/// tenant admin can set a user's `mail` attribute to any address — see
/// `RTDB_MICROSOFT_TENANT` in `.env.example` / `docs/OAUTH_SETUP.md`.
///
/// Identity is keyed on `microsoft_sub` (the composite `{tid}.{sub}`, both
/// extracted from a signature-verified id_token), NOT on the spoofable `email`
/// claim. This is the nOAuth defense (SEC-102): a tenant admin can set a
/// victim's address as their `mail` attribute, and Microsoft faithfully emits
/// it as the `email` claim — but the immutable `sub`+`tid` pair stays theirs.
/// The `email` claim is used for account-linking-by-email only when
/// `xms_edov == true` (Microsoft's "email domain owner verified" signal, set
/// only when the domain was validated via a DNS TXT record); otherwise the
/// tenant-constrained UPN (`preferred_username`) is used as the contact
/// address and the email is never matched against allowlist/admin entries.
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
    // id_token claims returned by the token exchange — see `complete_login`
    // and `parse_identity`.
    const USERINFO_ENDPOINT: &'static str = "https://graph.microsoft.com/oidc/userinfo";
}

/// Normalized identity extracted from a verified id_token (and optionally
/// Graph userinfo).
///
/// `microsoft_sub` is the `{tid}.{sub}` composite — the immutable, per-user
/// identity key written to the `users.microsoft_sub` column. `contact_email`
/// is the address used for display and the allowlist/admin match. When
/// `email_domain_verified == true` it is the id_token `email` claim (Microsoft
/// confirmed the domain via DNS); when `false` it is the tenant-constrained
/// UPN (`preferred_username`), which is safe because it belongs to the
/// tenant's own verified namespace.
struct MicrosoftIdentity {
    microsoft_sub: String,
    contact_email: String,
    name: Option<String>,
    /// `true` only when Microsoft's `xms_edov` claim was `true` — the email
    /// domain is DNS-verified, so the `email` claim is trusted for
    /// allowlist/admin matching and cross-provider linking.
    email_domain_verified: bool,
}

/// Cached Microsoft JWKS (JSON Web Key Set), keyed by tenant id. Providers are
/// constructed per request (`from_config`), so a struct field could not hold
/// the cache across requests — a module-level static does. TTL bounds the
/// stale-key window; a refresh falls out naturally when an entry expires. The
/// `reqwest::Client` is shared for the same reason.
static JWKS_CACHE: LazyLock<Cache<String, Arc<serde_json::Value>>> = LazyLock::new(|| {
    Cache::builder()
        .time_to_live(Duration::from_secs(3600))
        .max_capacity(64)
        .build()
});

static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
});

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
        let redirect_uri = self.redirect_uri(&state.config.public_url);

        let token_resp: serde_json::Value = HTTP_CLIENT
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
        let id_token = token_resp
            .get("id_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                tracing::warn!("microsoft token exchange returned no id_token");
                RtDbError::internal("microsoft token exchange failed")
            })?;

        // SEC-102: verify the id_token signature against Microsoft's published
        // JWKS for the issuing tenant, and validate iss/aud/exp/tid. The
        // pre-fix code base64-decoded the payload without any verification,
        // which let a spoofed/relayed token dictate the identity. JWKS fetch
        // failures are fail-closed — a login is never admitted on the strength
        // of an unverified token.
        let claims = verify_id_token(id_token, &self.client_id, &self.tenant).await?;

        let userinfo: serde_json::Value = HTTP_CLIENT
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

        let identity = parse_identity(&claims, &userinfo)?;
        let email = identity.contact_email.to_lowercase();
        let login = identity.name.clone().unwrap_or_else(|| email.clone());

        let user_id = upsert_user(
            &state.pool,
            &identity.microsoft_sub,
            &login,
            &email,
            identity.email_domain_verified,
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

/// Upserts the Microsoft user into `rtdb_auth.users`. Identity is keyed on
/// `microsoft_sub` (the immutable `{tid}.{sub}` composite) — NOT on `email`,
/// which is the nOAuth attack surface.
///
/// Resolution order:
/// 1. An existing user with this `microsoft_sub` (a returning Microsoft user)
///    is reused, with `login`/`email` refreshed — so a tenant-side email change
///    follows the account rather than forking it.
/// 2. Otherwise, **only when the email is domain-verified (`xms_edov == true`)**
///    and an account with that email exists but is not yet Microsoft-linked
///    (`microsoft_sub IS NULL`), that account is linked by setting its
///    `microsoft_sub`. Both Microsoft (DNS-verified domain) and the other
///    provider verified the email, so this is the same person. When the email
///    is NOT domain-verified (xms_edov absent) this step is skipped entirely —
///    a spoofed email can never adopt an existing row.
/// 3. Otherwise a new row is inserted with `microsoft_sub` set.
async fn upsert_user(
    pool: &PgPool,
    microsoft_sub: &str,
    login: &str,
    email: &str,
    email_domain_verified: bool,
) -> Result<String, RtDbError> {
    let mut tx = pool.begin().await?;

    // (1) returning Microsoft user: reuse the account, refresh login/email.
    if let Some((id,)) =
        sqlx::query_as::<_, (String,)>("SELECT id FROM rtdb_auth.users WHERE microsoft_sub = $1")
            .bind(microsoft_sub)
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

    // (2) link an email-keyed account that is not yet Microsoft-linked — but
    // only when the email is domain-verified. xms_edov absent => skip entirely
    // (a fresh account is created in step 3 instead). This is the nOAuth
    // defense: a spoofed email cannot adopt an existing row.
    if email_domain_verified
        && let Some((id,)) = sqlx::query_as::<_, (String,)>(
            "UPDATE rtdb_auth.users \
             SET microsoft_sub = $1, login = $2 \
             WHERE email = $3 AND microsoft_sub IS NULL \
             RETURNING id",
        )
        .bind(microsoft_sub)
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
        "INSERT INTO rtdb_auth.users (id, microsoft_sub, login, email, created_at) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(&id)
    .bind(microsoft_sub)
    .bind(login)
    .bind(email)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(map_conflict)?;
    tx.commit().await?;
    Ok(id)
}

/// Maps a Postgres unique-violation (`23505`) from a `users` upsert to a
/// deliberate 409 conflict — the `microsoft_sub` or email is already linked to
/// another sign-in method (or a concurrent login just claimed it). Any other
/// database error passes through as the usual internal-error mapping (logged,
/// never leaked).
fn map_conflict(err: sqlx::Error) -> RtDbError {
    let is_unique_violation = matches!(
        &err,
        sqlx::Error::Database(db_err) if db_err.code().as_deref() == Some("23505")
    );
    if is_unique_violation {
        RtDbError::precondition("microsoft identity already linked to another sign-in method")
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

/// Verifies the Microsoft id_token: signature against the issuing tenant's
/// JWKS, plus `iss`/`aud`/`exp` validation and an optional `tid` check
/// against the configured tenant. Returns the trusted claims.
///
/// The `kid` (from the untrusted header) and `tid` (from the untrusted
/// payload) are read first only to locate the JWKS and build the expected
/// issuer — they are not trusted until `jsonwebtoken::decode` verifies the
/// signature. JWKS fetch failures are fail-closed: a generic internal error
/// rejects the login rather than admitting an unverified token.
async fn verify_id_token(
    id_token: &str,
    client_id: &str,
    configured_tenant: &str,
) -> Result<serde_json::Value, RtDbError> {
    use base64::Engine;

    // Untrusted header/payload reads — only for kid/tid/iss lookup. The values
    // are re-validated by jsonwebtoken::decode below.
    let mut parts = id_token.split('.');
    let header_b64 = parts
        .next()
        .ok_or_else(|| forbidden("malformed id_token"))?;
    let payload_b64 = parts
        .next()
        .ok_or_else(|| forbidden("malformed id_token"))?;
    let header: serde_json::Value = serde_json::from_slice(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(header_b64)
            .map_err(|e| {
                tracing::warn!(error = %e, "microsoft id_token header decode failed");
                forbidden("malformed id_token")
            })?,
    )
    .map_err(|e| {
        tracing::warn!(error = %e, "microsoft id_token header parse failed");
        forbidden("malformed id_token")
    })?;
    let untrusted_claims: serde_json::Value = serde_json::from_slice(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(payload_b64)
            .map_err(|e| {
                tracing::warn!(error = %e, "microsoft id_token payload decode failed");
                forbidden("malformed id_token")
            })?,
    )
    .map_err(|e| {
        tracing::warn!(error = %e, "microsoft id_token payload parse failed");
        forbidden("malformed id_token")
    })?;

    let kid = header
        .get("kid")
        .and_then(|v| v.as_str())
        .ok_or_else(|| forbidden("id_token missing kid"))?;
    let tid = untrusted_claims
        .get("tid")
        .and_then(|v| v.as_str())
        .ok_or_else(|| forbidden("id_token missing tid"))?;

    // If the operator pinned a specific tenant, the token's tid must match.
    // `common`/`organizations`/`consumers` are multi-tenant selectors and do
    // not constrain tid.
    if !matches!(configured_tenant, "common" | "organizations" | "consumers")
        && configured_tenant != tid
    {
        tracing::warn!(
            configured_tenant = %configured_tenant,
            token_tid = %tid,
            "microsoft id_token tenant mismatch"
        );
        return Err(forbidden("id_token tenant mismatch"));
    }

    let jwks = fetch_jwks(tid).await?;
    let key_obj = jwks
        .get("keys")
        .and_then(|v| v.as_array())
        .and_then(|arr| {
            arr.iter().find(|k| {
                k.get("kid").and_then(|v| v.as_str()) == Some(kid)
                    && k.get("kty").and_then(|v| v.as_str()) == Some("RSA")
            })
        })
        .ok_or_else(|| {
            tracing::warn!(kid = %kid, tid = %tid, "microsoft id_token kid not in JWKS");
            forbidden("id_token signature verification failed")
        })?;
    let n = key_obj
        .get("n")
        .and_then(|v| v.as_str())
        .ok_or_else(|| forbidden("JWKS key missing n"))?;
    let e = key_obj
        .get("e")
        .and_then(|v| v.as_str())
        .ok_or_else(|| forbidden("JWKS key missing e"))?;
    let decoding_key = jsonwebtoken::DecodingKey::from_rsa_components(n, e).map_err(|err| {
        tracing::warn!(error = %err, "microsoft JWKS key decode failed");
        forbidden("id_token signature verification failed")
    })?;

    // Microsoft v2.0 issuer is per-tenant; v1.0 uses sts.windows.net. Accept
    // the v2.0 form (the only form the v2.0 endpoints emit) — a v1.0 token on
    // a v2.0 flow is itself suspicious and rejecting it is the safe posture.
    let expected_iss = format!("https://login.microsoftonline.com/{tid}/v2.0");

    let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::RS256);
    validation.set_issuer(&[&expected_iss]);
    validation.set_audience(&[client_id]);
    // Require iss + aud + exp to be present and valid (exp is required by
    // default; iss/aud validate-but-not-required by default).
    validation.required_spec_claims.insert("iss".to_string());
    validation.required_spec_claims.insert("aud".to_string());

    let token_data =
        jsonwebtoken::decode::<serde_json::Value>(id_token, &decoding_key, &validation).map_err(
            |err| {
                tracing::warn!(error = %err, "microsoft id_token verification failed");
                forbidden("id_token signature verification failed")
            },
        )?;

    Ok(token_data.claims)
}

/// Fetches the JWKS for a tenant, caching the parsed key set for one hour. The
/// cache is keyed by tenant id so multiple tenants (a `common` deployment) do
/// not collide. Fail-closed: any network/decode error rejects the login.
async fn fetch_jwks(tid: &str) -> Result<Arc<serde_json::Value>, RtDbError> {
    if let Some(cached) = JWKS_CACHE.get(tid).await {
        return Ok(cached);
    }
    let url = format!("https://login.microsoftonline.com/{tid}/discovery/v2.0/keys");
    let jwks: serde_json::Value = HTTP_CLIENT
        .get(&url)
        .send()
        .await
        .map_err(|err| {
            tracing::warn!(error = %err, "microsoft JWKS fetch request failed");
            RtDbError::internal("microsoft id_token verification failed")
        })?
        .json()
        .await
        .map_err(|err| {
            tracing::warn!(error = %err, "microsoft JWKS decode failed");
            RtDbError::internal("microsoft id_token verification failed")
        })?;
    let jwks = Arc::new(jwks);
    JWKS_CACHE.insert(tid.to_string(), Arc::clone(&jwks)).await;
    Ok(jwks)
}

fn forbidden(msg: &'static str) -> RtDbError {
    RtDbError::forbidden(msg)
}

/// Extracts a normalized identity from the **verified** id_token claims and
/// (optionally) Graph userinfo. The contact email is selected per SEC-102:
///
/// - When `xms_edov == true` (Microsoft's "email domain owner verified"
///   signal — DNS TXT record validation) the `email` claim is trusted and
///   used for both the contact address and cross-provider account linking.
///   An explicitly-false `email_verified` (SEC-122) still rejects.
/// - Otherwise the email is treated as unverified; the tenant-constrained UPN
///   (`preferred_username`) is used as the contact address instead. The UPN
///   is safe because it lives in the tenant's own verified domain namespace.
/// - If neither is present the login is rejected.
fn parse_identity(
    claims: &serde_json::Value,
    userinfo: &serde_json::Value,
) -> Result<MicrosoftIdentity, RtDbError> {
    let sub = claims.get("sub").and_then(|v| v.as_str()).ok_or_else(|| {
        tracing::warn!(claims = ?claims, "microsoft id_token missing sub");
        forbidden("no microsoft subject")
    })?;
    let tid = claims.get("tid").and_then(|v| v.as_str()).ok_or_else(|| {
        tracing::warn!(claims = ?claims, "microsoft id_token missing tid");
        forbidden("no microsoft tenant")
    })?;

    // xms_edov: Microsoft's "email domain owner verified" claim — set only
    // when the email domain was validated via a DNS TXT record. Absent/false
    // means the email may be tenant-admin-settable and is NOT trusted for
    // allowlist/admin matching or cross-provider linking.
    let email_domain_verified = claims
        .get("xms_edov")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let contact_email = if email_domain_verified {
        // Domain verified — trust the email claim. SEC-122: an explicit
        // `email_verified: false` still rejects (absent is acceptable here
        // because xms_edov is the stronger signal).
        if let Some(ev) = claims
            .get("email_verified")
            .or_else(|| userinfo.get("email_verified"))
            && !is_email_verified(ev)
        {
            return Err(forbidden("email is not verified"));
        }
        let email = claims
            .get("email")
            .or_else(|| userinfo.get("email"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                tracing::warn!(claims = ?claims, userinfo = ?userinfo, "xms_edov set but no email claim");
                forbidden("no email")
            })?;
        email.to_string()
    } else {
        // No DNS-verified email — fall back to the tenant-constrained UPN,
        // which is safe because it lives in the tenant's own namespace. The
        // spoofable `email` claim is deliberately ignored here.
        let upn = claims
            .get("preferred_username")
            .or_else(|| claims.get("upn"))
            .or_else(|| userinfo.get("preferred_username"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                tracing::warn!(
                    claims = ?claims,
                    userinfo = ?userinfo,
                    "no xms_edov and no preferred_username/upn — no usable microsoft identity"
                );
                forbidden("no verified email")
            })?;
        upn.to_string()
    };

    let name = userinfo
        .get("name")
        .or_else(|| claims.get("name"))
        .and_then(|v| v.as_str())
        .map(String::from);

    Ok(MicrosoftIdentity {
        microsoft_sub: format!("{tid}.{sub}"),
        contact_email,
        name,
        email_domain_verified,
    })
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

    // --- SEC-102: identity is keyed on {tid}.{sub}, not email ---

    #[test]
    fn parse_identity_composes_microsoft_sub_from_tid_and_sub() {
        // sub + tid => microsoft_sub = "tid.sub" — the durable identity key.
        let id = parse_identity(
            &json!({
                "sub": "AAAAAA",
                "tid": "11111111-2222-3333-4444-555555555555",
                "xms_edov": true,
                "email": "alice@contoso.com"
            }),
            &json!({}),
        )
        .unwrap();
        assert_eq!(
            id.microsoft_sub,
            "11111111-2222-3333-4444-555555555555.AAAAAA"
        );
    }

    #[test]
    fn parse_identity_trusts_email_when_xms_edov_true() {
        // xms_edov == true => the email claim is the contact address.
        let id = parse_identity(
            &json!({
                "sub": "AAAA",
                "tid": "tid-1",
                "xms_edov": true,
                "email": "Alice@Example.com"
            }),
            &json!({}),
        )
        .unwrap();
        assert!(id.email_domain_verified);
        assert_eq!(id.contact_email, "Alice@Example.com");
    }

    #[test]
    fn parse_identity_falls_back_to_upn_when_xms_edov_absent() {
        // No xms_edov => email is untrusted; the UPN (preferred_username) is
        // the contact address instead, and email_domain_verified is false.
        let id = parse_identity(
            &json!({
                "sub": "AAAA",
                "tid": "tid-1",
                "preferred_username": "alice@contoso.com",
                "email": "spoofable@victim.com"
            }),
            &json!({}),
        )
        .unwrap();
        assert!(!id.email_domain_verified);
        // The spoofable email claim is ignored; the UPN wins.
        assert_eq!(id.contact_email, "alice@contoso.com");
    }

    #[test]
    fn parse_identity_falls_back_to_userinfo_preferred_username() {
        // id_token has no preferred_username, userinfo does.
        let id = parse_identity(
            &json!({"sub": "AAAA", "tid": "tid-1"}),
            &json!({"preferred_username": "bob@contoso.com"}),
        )
        .unwrap();
        assert!(!id.email_domain_verified);
        assert_eq!(id.contact_email, "bob@contoso.com");
    }

    #[test]
    fn parse_identity_rejects_when_no_xms_edov_and_no_upn() {
        // Neither a domain-verified email nor a UPN — no usable identity.
        let err = parse_identity(
            &json!({"sub": "A", "tid": "t", "email": "only@spoofable.com"}),
            &json!({}),
        );
        assert!(err.is_err());
    }

    #[test]
    fn parse_identity_rejects_explicitly_unverified_email_even_with_xms_edov() {
        // SEC-122: explicit email_verified=false rejects even when xms_edov is
        // true — the two signals contradict, treat the address as bad.
        let err = parse_identity(
            &json!({
                "sub": "A",
                "tid": "t",
                "xms_edov": true,
                "email": "a@b.com",
                "email_verified": false
            }),
            &json!({}),
        );
        assert!(err.is_err());
    }

    #[test]
    fn parse_identity_accepts_absent_email_verified_when_xms_edov_true() {
        // xms_edov is the stronger DNS-verification signal; an absent
        // email_verified does not override it (SEC-122 is satisfied by the
        // DNS check itself).
        let id = parse_identity(
            &json!({
                "sub": "A",
                "tid": "t",
                "xms_edov": true,
                "email": "a@verified-domain.com"
            }),
            &json!({}),
        )
        .unwrap();
        assert_eq!(id.contact_email, "a@verified-domain.com");
    }

    #[test]
    fn parse_identity_rejects_missing_sub() {
        let err = parse_identity(&json!({"tid": "t"}), &json!({}));
        assert!(err.is_err());
    }

    #[test]
    fn parse_identity_rejects_missing_tid() {
        let err = parse_identity(&json!({"sub": "s"}), &json!({}));
        assert!(err.is_err());
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
            anonymous_session_ttl_days: 1,
            anonymous_rate_limit_per_ip_rpm: 0,
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
