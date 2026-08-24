//! Microsoft / Entra ID v2 OAuth provider (`/auth/microsoft/*`). id_tokens are
//! verified with `jsonwebtoken` (RS256 against the tenant JWKS); the
//! client_secret is app-registered. See `docs/OAUTH_SETUP.md`.

use std::sync::Arc;

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

#[async_trait]
impl OAuthProvider for MicrosoftProvider {
    fn name() -> &'static str {
        "microsoft"
    }

    fn from_config(config: &Config) -> Option<Self> {
        let client_id = config.oauth.microsoft.client_id.clone()?;
        let client_secret = config.oauth.microsoft.client_secret.clone()?;
        Some(Self {
            client_id,
            client_secret,
            tenant: config.oauth.microsoft.tenant.clone(),
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
        let http = state.auth.http.clone();
        let redirect_uri = self.redirect_uri(&state.config.public_url);

        let token_resp: serde_json::Value = http
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
        let claims = verify_id_token(&http, id_token, &self.client_id, &self.tenant).await?;

        let userinfo: serde_json::Value = http
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
            // SEC-130: log only the response SHAPE (which keys are present),
            // never the body — a response missing access_token may still carry
            // id_token/refresh_token, which must not reach the logs.
            let keys: Vec<&str> = value
                .as_object()
                .map(|m| m.keys().map(String::as_str).collect())
                .unwrap_or_default();
            tracing::warn!(present_keys = ?keys, "microsoft token exchange returned no access_token");
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
    http: &reqwest::Client,
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

    let jwks = jwks::fetch(
        http,
        &jwks_url(tid),
        "microsoft id_token verification failed",
    )
    .await?;
    let key = jwks::select_key(&jwks, kid, "id_token signature verification failed")?;

    // Microsoft v2.0 issuer is per-tenant; v1.0 uses sts.windows.net. Accept
    // the v2.0 form (the only form the v2.0 endpoints emit) — a v1.0 token on
    // a v2.0 flow is itself suspicious and rejecting it is the safe posture.
    let expected_iss = format!("https://login.microsoftonline.com/{tid}/v2.0");

    jwks::decode_verified(
        id_token,
        &key,
        &expected_iss,
        client_id,
        "id_token signature verification failed",
    )
}

/// Microsoft's per-tenant JWKS endpoint. The shared cache is keyed by this URL,
/// so two tenants (a `common` deployment) cannot collide on one entry.
fn jwks_url(tid: &str) -> String {
    format!("https://login.microsoftonline.com/{tid}/discovery/v2.0/keys")
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
        // SEC-130: log only which claim keys are present, never the full claims
        // object — verified claims still carry PII (email, name) that must not
        // reach the logs.
        let keys: Vec<&str> = claims
            .as_object()
            .map(|m| m.keys().map(String::as_str).collect())
            .unwrap_or_default();
        tracing::warn!(present_keys = ?keys, "microsoft id_token missing sub");
        forbidden("no microsoft subject")
    })?;
    let tid = claims.get("tid").and_then(|v| v.as_str()).ok_or_else(|| {
        let keys: Vec<&str> = claims
            .as_object()
            .map(|m| m.keys().map(String::as_str).collect())
            .unwrap_or_default();
        tracing::warn!(present_keys = ?keys, "microsoft id_token missing tid");
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
                // SEC-130: log only the present claim/userinfo keys, never the
                // bodies (PII: email, name).
                tracing::warn!(
                    claim_keys = ?claims.as_object().map(|m| m.keys().collect::<Vec<_>>()).unwrap_or_default(),
                    userinfo_keys = ?userinfo.as_object().map(|m| m.keys().collect::<Vec<_>>()).unwrap_or_default(),
                    "xms_edov set but no email claim"
                );
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
                    claim_keys = ?claims.as_object().map(|m| m.keys().collect::<Vec<_>>()).unwrap_or_default(),
                    userinfo_keys = ?userinfo.as_object().map(|m| m.keys().collect::<Vec<_>>()).unwrap_or_default(),
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
    use crate::config::OAuthConfig;
    use crate::error::ErrorCode;
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
        cfg.oauth.microsoft.client_id = Some("id".into());
        assert!(
            MicrosoftProvider::from_config(&cfg).is_none(),
            "still missing secret"
        );
        cfg.oauth.microsoft.client_secret = Some("secret".into());
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
        }
    }

    // --- parse_token_response / verify_id_token / fetch_jwks ----------------
    // JWT fixtures below are hand-built RS256 tokens signed by throwaway
    // runtime-generated keys — no PEM or key literal ever lives in the repo
    // (gitleaks/detect-private-key stay meaningful), same pattern as the
    // Apple ES256 tests.

    #[test]
    fn parse_token_response_failures_are_generic_internal_errors() {
        // SEC-130: the OAuth error text never reaches the caller, and a
        // non-object body (an HTML error page, a bare string) has no
        // access_token either — both are generic internal errors.
        let err = parse_token_response(&json!({"error": "invalid_grant"})).unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        assert_eq!(err.message, "microsoft token exchange failed");

        let err = parse_token_response(&json!("<html>bad gateway</html>")).unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        assert_eq!(err.message, "microsoft token exchange failed");
    }

    /// Throwaway RSA-2048 key generated at runtime for signing test id_tokens.
    fn fresh_rsa_key() -> rsa::RsaPrivateKey {
        rsa::RsaPrivateKey::new(&mut rand::rngs::OsRng, 2048).expect("generate RSA-2048 key")
    }

    fn b64url(bytes: &[u8]) -> String {
        use base64::Engine;
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    }

    /// JWKS entry for a key's public half — base64url-no-pad of the minimal
    /// big-endian n/e bytes, the encoding `DecodingKey::from_rsa_components`
    /// parses.
    fn jwks_entry(key: &rsa::RsaPrivateKey, kid: &str) -> serde_json::Value {
        use rsa::traits::PublicKeyParts;
        json!({
            "kty": "RSA",
            "use": "sig",
            "kid": kid,
            "n": b64url(&key.n().to_bytes_be()),
            "e": b64url(&key.e().to_bytes_be()),
        })
    }

    /// Hand-built RS256 JWT with a real PKCS#1v1.5 signature over the
    /// `header.payload` signing input.
    fn sign_rs256_jwt(key: &rsa::RsaPrivateKey, kid: &str, claims: &serde_json::Value) -> String {
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

    /// uuid-bearing tenant id — tests run in parallel against the shared
    /// module-level JWKS_CACHE, so every seeded tid must be unique.
    fn unique_tid() -> String {
        format!("tid-{}", uuid::Uuid::now_v7().simple())
    }

    /// Seeds the JWKS cache for a fresh unique tid with `key` under `kid` and
    /// returns the tid, so `verify_id_token` resolves the key with no network.
    async fn seed_jwks_for(key: &rsa::RsaPrivateKey, kid: &str) -> String {
        let tid = unique_tid();
        jwks::seed_for_test(&jwks_url(&tid), json!({"keys": [jwks_entry(key, kid)]})).await;
        tid
    }

    const TEST_CLIENT_ID: &str = "test-client-id";

    /// Claims of a well-formed Microsoft v2.0 id_token for `tid`, with `exp`
    /// offset `exp_offset_secs` from now.
    fn ms_claims(tid: &str, exp_offset_secs: i64) -> serde_json::Value {
        let now = chrono::Utc::now().timestamp();
        json!({
            "iss": format!("https://login.microsoftonline.com/{tid}/v2.0"),
            "aud": TEST_CLIENT_ID,
            "exp": now + exp_offset_secs,
            "iat": now,
            "tid": tid,
            "sub": "AAAAAAAAAAAAAAAAAAAAAA",
            "preferred_username": "alice@contoso.example",
            "name": "Alice Example",
        })
    }

    #[tokio::test]
    async fn verify_id_token_accepts_a_valid_rs256_token_from_the_cached_jwks() {
        let key = fresh_rsa_key();
        let tid = seed_jwks_for(&key, "kid-1").await;
        let token = sign_rs256_jwt(&key, "kid-1", &ms_claims(&tid, 3600));

        let claims = verify_id_token(&reqwest::Client::new(), &token, TEST_CLIENT_ID, "common")
            .await
            .expect("valid id_token verifies");

        assert_eq!(claims["tid"].as_str(), Some(tid.as_str()));
        assert_eq!(claims["preferred_username"], "alice@contoso.example");
    }

    #[tokio::test]
    async fn verify_id_token_accepts_a_token_from_the_pinned_tenant() {
        let key = fresh_rsa_key();
        let tid = seed_jwks_for(&key, "kid-1").await;
        let token = sign_rs256_jwt(&key, "kid-1", &ms_claims(&tid, 3600));

        // configured tenant == token tid, and not a multi-tenant selector.
        verify_id_token(&reqwest::Client::new(), &token, TEST_CLIENT_ID, &tid)
            .await
            .expect("pinned-tenant token verifies");
    }

    #[tokio::test]
    async fn verify_id_token_rejects_malformed_tokens() {
        let header = b64url(br#"{"alg":"RS256","kid":"k"}"#);
        let non_json_header = format!("{}.{}.sig", b64url(b"not-json"), b64url(br#"{"tid":"t"}"#));
        let non_json_payload = format!("{header}.{}.sig", b64url(b"not-json"));
        let non_b64_payload = format!("{header}.!!.sig");
        for bad in [
            "no-dots-at-all",
            "!!!.e30.sig",
            non_json_header.as_str(),
            non_json_payload.as_str(),
            non_b64_payload.as_str(),
        ] {
            let err = verify_id_token(&reqwest::Client::new(), bad, TEST_CLIENT_ID, "common")
                .await
                .unwrap_err();
            assert_eq!(err.code, ErrorCode::Forbidden, "input: {bad}");
            assert_eq!(err.message, "malformed id_token", "input: {bad}");
        }
    }

    #[tokio::test]
    async fn verify_id_token_rejects_a_header_without_kid() {
        let token = format!(
            "{}.{}.sig",
            b64url(br#"{"alg":"RS256","typ":"JWT"}"#),
            b64url(br#"{"tid":"t"}"#)
        );
        let err = verify_id_token(&reqwest::Client::new(), &token, TEST_CLIENT_ID, "common")
            .await
            .unwrap_err();
        assert_eq!(err.message, "id_token missing kid");
    }

    #[tokio::test]
    async fn verify_id_token_rejects_a_payload_without_tid() {
        let token = format!(
            "{}.{}.sig",
            b64url(br#"{"alg":"RS256","typ":"JWT","kid":"k"}"#),
            b64url(br#"{"sub":"s"}"#)
        );
        let err = verify_id_token(&reqwest::Client::new(), &token, TEST_CLIENT_ID, "common")
            .await
            .unwrap_err();
        assert_eq!(err.message, "id_token missing tid");
    }

    #[tokio::test]
    async fn verify_id_token_rejects_a_tid_other_than_the_pinned_tenant() {
        let key = fresh_rsa_key();
        // JWKS seeded so a regression in check order degrades to a cache hit,
        // never a live network fetch from a unit test.
        let tid = seed_jwks_for(&key, "kid-1").await;
        let token = sign_rs256_jwt(&key, "kid-1", &ms_claims(&tid, 3600));

        let err = verify_id_token(
            &reqwest::Client::new(),
            &token,
            TEST_CLIENT_ID,
            "contoso.example",
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::Forbidden);
        assert_eq!(err.message, "id_token tenant mismatch");
    }

    #[tokio::test]
    async fn verify_id_token_rejects_a_kid_absent_from_the_jwks() {
        let key = fresh_rsa_key();
        let tid = seed_jwks_for(&key, "some-other-kid").await;
        let token = sign_rs256_jwt(&key, "kid-1", &ms_claims(&tid, 3600));

        let err = verify_id_token(&reqwest::Client::new(), &token, TEST_CLIENT_ID, "common")
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::Forbidden);
        assert_eq!(err.message, "id_token signature verification failed");
    }

    #[tokio::test]
    async fn verify_id_token_rejects_incomplete_jwks_entries() {
        use rsa::traits::PublicKeyParts;
        let key = fresh_rsa_key();
        let http = reqwest::Client::new();

        // SEC-004: every malformed-JWKS shape now yields the same generic
        // message. The old per-field text ("JWKS key missing n"/"…e") told a
        // caller which component was absent; the shared `jwks::select_key`
        // logs that detail and returns one fixed string instead.
        let malformed = [
            // No "n" component.
            json!({"keys": [{"kty": "RSA", "kid": "k", "e": "AQAB"}]}),
            // No "e" component.
            json!({"keys": [{"kty": "RSA", "kid": "k", "n": b64url(&key.n().to_bytes_be())}]}),
            // "n" present but not base64 — the decoding key cannot be built.
            json!({"keys": [{"kty": "RSA", "kid": "k", "n": "!!not-base64!!", "e": "AQAB"}]}),
        ];
        for entry in malformed {
            let tid = unique_tid();
            jwks::seed_for_test(&jwks_url(&tid), entry).await;
            let err = verify_id_token(
                &http,
                &sign_rs256_jwt(&key, "k", &ms_claims(&tid, 3600)),
                TEST_CLIENT_ID,
                "common",
            )
            .await
            .unwrap_err();
            assert_eq!(err.code, ErrorCode::Forbidden);
            assert_eq!(err.message, "id_token signature verification failed");
        }
    }

    #[tokio::test]
    async fn verify_id_token_rejects_a_signature_made_by_a_different_key() {
        let signing_key = fresh_rsa_key();
        let jwks_key = fresh_rsa_key();
        let tid = seed_jwks_for(&jwks_key, "kid-1").await;
        let token = sign_rs256_jwt(&signing_key, "kid-1", &ms_claims(&tid, 3600));

        let err = verify_id_token(&reqwest::Client::new(), &token, TEST_CLIENT_ID, "common")
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::Forbidden);
        assert_eq!(err.message, "id_token signature verification failed");
    }

    #[tokio::test]
    async fn verify_id_token_rejects_wrong_audience_issuer_or_expired_tokens() {
        let key = fresh_rsa_key();
        let tid = seed_jwks_for(&key, "kid-1").await;
        let http = reqwest::Client::new();

        let mut wrong_aud = ms_claims(&tid, 3600);
        wrong_aud["aud"] = json!("some-other-app");
        let err = verify_id_token(
            &http,
            &sign_rs256_jwt(&key, "kid-1", &wrong_aud),
            TEST_CLIENT_ID,
            "common",
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::Forbidden);

        let mut wrong_iss = ms_claims(&tid, 3600);
        wrong_iss["iss"] = json!("https://login.microsoftonline.com/someone-else/v2.0");
        let err = verify_id_token(
            &http,
            &sign_rs256_jwt(&key, "kid-1", &wrong_iss),
            TEST_CLIENT_ID,
            "common",
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::Forbidden);

        // exp an hour in the past — well beyond the default validation leeway.
        let err = verify_id_token(
            &http,
            &sign_rs256_jwt(&key, "kid-1", &ms_claims(&tid, -3600)),
            TEST_CLIENT_ID,
            "common",
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::Forbidden);
        assert_eq!(err.message, "id_token signature verification failed");
    }

    #[tokio::test]
    async fn fetch_jwks_returns_a_cached_key_set_without_a_network_round_trip() {
        let tid = unique_tid();
        let url = jwks_url(&tid);
        jwks::seed_for_test(&url, json!({"keys": [{"kty": "RSA", "kid": "k"}]})).await;

        // A cache hit must not touch the network: this client cannot reach
        // login.microsoftonline.com at all, so a miss would be an error.
        let fetched = jwks::fetch(&reqwest::Client::new(), &url, "unused")
            .await
            .expect("cache hit");

        assert_eq!(fetched["keys"][0]["kid"], "k");
    }

    // --- upsert_user (shared dev Postgres; every value uuid-unique) ---------

    /// Connects to the shared dev Postgres (RTDB_TEST_DATABASE_URL override,
    /// default 127.0.0.1:55434/rtdb — the DB is shared, never created/dropped)
    /// and bootstraps rtdb_auth.
    async fn users_pool() -> PgPool {
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

    fn uniq(prefix: &str) -> String {
        format!("{prefix}-{}", uuid::Uuid::now_v7().simple())
    }

    async fn user_row(pool: &PgPool, id: &str) -> (String, String, Option<String>) {
        sqlx::query_as::<_, (String, String, Option<String>)>(
            "SELECT login, email, microsoft_sub FROM rtdb_auth.users WHERE id = $1",
        )
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("user row exists")
    }

    /// Direct-inserts a non-Microsoft user (microsoft_sub NULL) keyed on a
    /// unique email — the pre-existing account the link/nOAuth/conflict tests
    /// target.
    async fn insert_email_user(pool: &PgPool, login: &str, email: &str) -> String {
        let id = new_id();
        sqlx::query(
            "INSERT INTO rtdb_auth.users (id, login, email, created_at) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(&id)
        .bind(login)
        .bind(email)
        .bind(now_ms())
        .execute(pool)
        .await
        .expect("pre-insert email-keyed user");
        id
    }

    #[tokio::test]
    async fn upsert_user_inserts_a_brand_new_microsoft_user() {
        let pool = users_pool().await;
        let sub = uniq("tid.sub");
        let login = uniq("ms-login");
        let email = format!("{}@ms-test.example", uniq("alice"));

        let id = upsert_user(&pool, &sub, &login, &email, false)
            .await
            .expect("insert brand-new user");

        let (row_login, row_email, row_sub) = user_row(&pool, &id).await;
        assert_eq!(row_login, login);
        assert_eq!(row_email, email);
        assert_eq!(row_sub.as_deref(), Some(sub.as_str()));
    }

    #[tokio::test]
    async fn upsert_user_reuses_the_account_and_refreshes_login_and_email() {
        let pool = users_pool().await;
        let sub = uniq("tid.sub");
        let first_id = upsert_user(
            &pool,
            &sub,
            &uniq("ms-login-a"),
            &format!("{}@ms-test.example", uniq("a")),
            false,
        )
        .await
        .expect("initial insert");

        let new_login = uniq("ms-login-b");
        let new_email = format!("{}@ms-test.example", uniq("b"));
        let second_id = upsert_user(&pool, &sub, &new_login, &new_email, false)
            .await
            .expect("returning-user upsert");

        assert_eq!(
            second_id, first_id,
            "a returning microsoft_sub reuses the row"
        );
        let (row_login, row_email, row_sub) = user_row(&pool, &first_id).await;
        assert_eq!(row_login, new_login, "login follows the tenant-side change");
        assert_eq!(row_email, new_email, "email follows the tenant-side change");
        assert_eq!(row_sub.as_deref(), Some(sub.as_str()));
    }

    #[tokio::test]
    async fn upsert_user_links_an_email_keyed_account_when_domain_verified() {
        let pool = users_pool().await;
        let email = format!("{}@ms-test.example", uniq("carol"));
        let existing_id = insert_email_user(&pool, &uniq("gh-carol"), &email).await;

        let sub = uniq("tid.sub");
        let id = upsert_user(&pool, &sub, &uniq("ms-login-carol"), &email, true)
            .await
            .expect("link by DNS-verified email");

        assert_eq!(
            id, existing_id,
            "the existing account is adopted, not forked"
        );
        let (_, _, row_sub) = user_row(&pool, &existing_id).await;
        assert_eq!(row_sub.as_deref(), Some(sub.as_str()));
    }

    #[tokio::test]
    async fn upsert_user_never_adopts_an_email_keyed_account_without_domain_verification() {
        let pool = users_pool().await;
        // nOAuth shape: the token asserts an email a tenant admin can set;
        // xms_edov absent => the contact address is the caller's own (UPN)
        // address and the victim row is never touched.
        let victim_email = format!("{}@ms-test.example", uniq("victim"));
        let victim_id = insert_email_user(&pool, &uniq("gh-victim"), &victim_email).await;

        let sub = uniq("tid.sub");
        let attacker_email = format!("{}@ms-test.example", uniq("attacker"));
        let id = upsert_user(
            &pool,
            &sub,
            &uniq("ms-login-attacker"),
            &attacker_email,
            false,
        )
        .await
        .expect("a fresh account is created instead");

        assert_ne!(id, victim_id, "the victim account is never adopted");
        let (_, row_email, row_sub) = user_row(&pool, &victim_id).await;
        assert_eq!(row_email, victim_email);
        assert_eq!(row_sub, None, "the victim row stays unlinked");
    }

    /// The step-3 INSERT hits the `users_email_key` UNIQUE index: the incoming
    /// (unverified) identity re-asserts an email another row already owns.
    #[tokio::test]
    async fn upsert_user_maps_a_unique_violation_on_insert_to_a_precondition_error() {
        let pool = users_pool().await;
        let taken_email = format!("{}@ms-test.example", uniq("taken"));
        let owner_id = insert_email_user(&pool, &uniq("gh-owner"), &taken_email).await;

        let err = upsert_user(
            &pool,
            &uniq("tid.sub"),
            &uniq("ms-login"),
            &taken_email,
            false,
        )
        .await
        .unwrap_err();

        // A deliberate 409 conflict, never a generic internal error.
        assert_eq!(err.code, ErrorCode::PreconditionFailed);
        assert_ne!(err.code, ErrorCode::Internal);
        assert!(err.message.contains("already linked"), "{}", err.message);

        let (_, row_email, row_sub) = user_row(&pool, &owner_id).await;
        assert_eq!(row_email, taken_email);
        assert_eq!(row_sub, None, "the failed tx rolled back — owner untouched");
    }

    /// The step-1 UPDATE refreshes to an email another row already owns — the
    /// other 23505 route through `map_conflict`.
    #[tokio::test]
    async fn upsert_user_maps_a_unique_violation_on_refresh_to_a_precondition_error() {
        let pool = users_pool().await;
        let sub = uniq("tid.sub");
        let original_email = format!("{}@ms-test.example", uniq("a"));
        let id = upsert_user(&pool, &sub, &uniq("ms-login-a"), &original_email, false)
            .await
            .expect("initial insert");
        let taken_email = format!("{}@ms-test.example", uniq("b"));
        insert_email_user(&pool, &uniq("gh-b"), &taken_email).await;

        let err = upsert_user(&pool, &sub, &uniq("ms-login-a2"), &taken_email, false)
            .await
            .unwrap_err();

        assert_eq!(err.code, ErrorCode::PreconditionFailed);
        assert!(err.message.contains("already linked"), "{}", err.message);
        let (_, row_email, _) = user_row(&pool, &id).await;
        assert_eq!(row_email, original_email, "the failed refresh rolled back");
    }

    // --- parse_identity: remaining email/name selection branches ------------

    #[test]
    fn parse_identity_reads_email_verified_from_userinfo_and_accepts_string_true() {
        // `email_verified` may live in userinfo (not the claims) and may be
        // the string "true" — both must count as verified.
        let id = parse_identity(
            &json!({"sub": "A", "tid": "t", "xms_edov": true, "email": "a@dns-verified.com"}),
            &json!({"email_verified": "true"}),
        )
        .unwrap();
        assert_eq!(id.contact_email, "a@dns-verified.com");
        assert!(id.email_domain_verified);
    }

    #[test]
    fn parse_identity_rejects_a_string_false_email_verified_in_userinfo() {
        let err = parse_identity(
            &json!({"sub": "A", "tid": "t", "xms_edov": true, "email": "a@b.com"}),
            &json!({"email_verified": "false"}),
        );
        assert!(err.is_err());
    }

    #[test]
    fn parse_identity_treats_a_non_bool_non_string_email_verified_as_unverified() {
        // A number is neither a bool nor the string "true" — unverified.
        let err = parse_identity(
            &json!({
                "sub": "A", "tid": "t", "xms_edov": true,
                "email": "a@b.com", "email_verified": 1
            }),
            &json!({}),
        );
        assert!(err.is_err());
    }

    #[test]
    fn parse_identity_falls_back_to_the_userinfo_email_when_claims_have_none() {
        let id = parse_identity(
            &json!({"sub": "A", "tid": "t", "xms_edov": true}),
            &json!({"email": "from-userinfo@dns-verified.com"}),
        )
        .unwrap();
        assert_eq!(id.contact_email, "from-userinfo@dns-verified.com");
    }

    #[test]
    fn parse_identity_rejects_xms_edov_true_when_no_email_exists_anywhere() {
        // xms_edov set but neither claims nor userinfo carry an email — the
        // verified-email path has nothing to use (a UPN is NOT substituted).
        let err = parse_identity(
            &json!({"sub": "A", "tid": "t", "xms_edov": true}),
            &json!({"preferred_username": "upn-only@contoso.example"}),
        );
        assert!(err.is_err());
    }

    #[test]
    fn parse_identity_falls_back_to_the_legacy_upn_claim() {
        // v1-era tokens carry `upn` instead of `preferred_username`; a
        // non-bool xms_edov counts as unverified and takes the UPN path.
        let id = parse_identity(
            &json!({"sub": "A", "tid": "t", "upn": "legacy@contoso.example", "xms_edov": "true"}),
            &json!({}),
        )
        .unwrap();
        assert!(!id.email_domain_verified);
        assert_eq!(id.contact_email, "legacy@contoso.example");
    }

    #[test]
    fn parse_identity_prefers_userinfo_name_and_falls_back_to_the_claim_name() {
        // A preferred_username is required for a usable identity (the contact
        // email); the assertions are about the `name` preference only.
        let from_userinfo = parse_identity(
            &json!({"sub": "A", "tid": "t", "preferred_username": "name-test@t.example", "name": "From Claims"}),
            &json!({"name": "From Userinfo"}),
        )
        .unwrap();
        assert_eq!(from_userinfo.name.as_deref(), Some("From Userinfo"));

        let from_claims = parse_identity(
            &json!({"sub": "A", "tid": "t", "preferred_username": "name-test@t.example", "name": "From Claims"}),
            &json!({}),
        )
        .unwrap();
        assert_eq!(from_claims.name.as_deref(), Some("From Claims"));
    }
}
