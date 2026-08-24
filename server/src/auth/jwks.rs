//! Shared JWKS (RFC 7517) fetch/cache, key selection, and verified decode for
//! the OIDC providers that check an `id_token` signature (SEC-004).
//!
//! Two properties are load-bearing:
//!
//! 1. **The algorithm comes from the key, never from the token header.** A JWKS
//!    entry's `kty`/`crv`/`alg` fix which `jsonwebtoken::Algorithm` may verify
//!    it. Trusting the token's own `alg` is the classic JWT confusion attack
//!    (an RSA public key replayed as an HMAC secret); `select_key` makes that
//!    unrepresentable because the caller never supplies an algorithm.
//! 2. **Fail-closed.** Every network, decode, and lookup failure returns an
//!    error. There is no path that yields "no key found, proceed unverified".
//!
//! The cache is module-level because providers are constructed per request
//! (`from_config`), so a struct field could not hold it across requests. It is
//! keyed by JWKS **URL**, which is what makes one cache safe to share across
//! providers and tenants: two issuers cannot collide on a single entry.

use std::sync::{Arc, LazyLock};
use std::time::Duration;

use base64::Engine;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use moka::future::Cache;

use crate::error::RtDbError;

static JWKS_CACHE: LazyLock<Cache<String, Arc<serde_json::Value>>> = LazyLock::new(|| {
    Cache::builder()
        .time_to_live(Duration::from_secs(3600))
        .max_capacity(64)
        .build()
});

/// A JWKS public key together with the algorithm its key material binds it to.
pub struct SigningKey {
    key: DecodingKey,
    alg: Algorithm,
}

/// Fetches the JWKS document at `url`, caching the parsed key set for one hour.
/// TTL bounds the stale-key window across an issuer's key rotation; a refresh
/// falls out naturally when the entry expires.
///
/// `generic_message` is the provider-facing text placed in the returned
/// envelope — the underlying error is logged, never returned (CWE-209).
pub async fn fetch(
    http: &reqwest::Client,
    url: &str,
    generic_message: &'static str,
) -> Result<Arc<serde_json::Value>, RtDbError> {
    if let Some(cached) = JWKS_CACHE.get(url).await {
        return Ok(cached);
    }
    let jwks: serde_json::Value = http
        .get(url)
        .send()
        .await
        .map_err(|err| {
            tracing::warn!(url = %url, error = %err, "JWKS fetch request failed");
            RtDbError::internal(generic_message)
        })?
        .json()
        .await
        .map_err(|err| {
            tracing::warn!(url = %url, error = %err, "JWKS decode failed");
            RtDbError::internal(generic_message)
        })?;
    let jwks = Arc::new(jwks);
    JWKS_CACHE.insert(url.to_string(), Arc::clone(&jwks)).await;
    Ok(jwks)
}

/// Reads `kid` from the token's **unverified** header. This is a lookup key
/// only: it selects which public key to try, and every claim in the token stays
/// untrusted until [`decode_verified`] returns.
pub fn unverified_kid(token: &str, generic_message: &'static str) -> Result<String, RtDbError> {
    let header_b64 = token.split('.').next().ok_or_else(|| {
        tracing::warn!("id_token malformed (no header segment)");
        RtDbError::forbidden(generic_message)
    })?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(header_b64.trim_end_matches('='))
        .map_err(|err| {
            tracing::warn!(error = %err, "id_token header decode failed");
            RtDbError::forbidden(generic_message)
        })?;
    let header: serde_json::Value = serde_json::from_slice(&bytes).map_err(|err| {
        tracing::warn!(error = %err, "id_token header parse failed");
        RtDbError::forbidden(generic_message)
    })?;
    header
        .get("kid")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .ok_or_else(|| {
            tracing::warn!("id_token header missing kid");
            RtDbError::forbidden(generic_message)
        })
}

/// Finds the JWKS entry for `kid` and turns it into a [`SigningKey`].
///
/// Supported key types are RSA (`n`/`e`, Microsoft) and EC over P-256/P-384
/// (`x`/`y`, Apple). Anything else — an unknown `kty`, a symmetric `oct` key, a
/// curve we do not expect — is rejected rather than guessed at.
pub fn select_key(
    jwks: &serde_json::Value,
    kid: &str,
    generic_message: &'static str,
) -> Result<SigningKey, RtDbError> {
    let key_obj = jwks
        .get("keys")
        .and_then(|v| v.as_array())
        .and_then(|arr| {
            arr.iter()
                .find(|k| k.get("kid").and_then(|v| v.as_str()) == Some(kid))
        })
        .ok_or_else(|| {
            tracing::warn!(kid = %kid, "id_token kid not in JWKS");
            RtDbError::forbidden(generic_message)
        })?;

    let field = |name: &str| key_obj.get(name).and_then(|v| v.as_str());
    let reject = |what: &'static str| {
        tracing::warn!(kid = %kid, reason = what, "JWKS key unusable");
        RtDbError::forbidden(generic_message)
    };

    match field("kty") {
        Some("RSA") => {
            let (Some(n), Some(e)) = (field("n"), field("e")) else {
                return Err(reject("RSA key missing n/e"));
            };
            // `alg` is advisory on a JWKS entry; default to RS256 (what both
            // Microsoft and Apple publish) and accept only the RSA family.
            let alg = match field("alg") {
                None | Some("RS256") => Algorithm::RS256,
                Some("RS384") => Algorithm::RS384,
                Some("RS512") => Algorithm::RS512,
                Some(_) => return Err(reject("unsupported RSA alg")),
            };
            let key = DecodingKey::from_rsa_components(n, e)
                .map_err(|err| reject_with(kid, generic_message, err))?;
            Ok(SigningKey { key, alg })
        }
        Some("EC") => {
            let (Some(x), Some(y)) = (field("x"), field("y")) else {
                return Err(reject("EC key missing x/y"));
            };
            // The curve fixes the algorithm outright: a P-256 key is an ES256
            // key and nothing else, so there is no `alg` to disagree with.
            let alg = match field("crv") {
                Some("P-256") => Algorithm::ES256,
                Some("P-384") => Algorithm::ES384,
                _ => return Err(reject("unsupported EC curve")),
            };
            let key = DecodingKey::from_ec_components(x, y)
                .map_err(|err| reject_with(kid, generic_message, err))?;
            Ok(SigningKey { key, alg })
        }
        _ => Err(reject("unsupported kty")),
    }
}

fn reject_with(
    kid: &str,
    generic_message: &'static str,
    err: jsonwebtoken::errors::Error,
) -> RtDbError {
    tracing::warn!(kid = %kid, error = %err, "JWKS key decode failed");
    RtDbError::forbidden(generic_message)
}

/// Verifies `token`'s signature against `key` and returns its claims.
///
/// `iss`, `aud`, and `exp` are all **required and validated**: `exp` is
/// required by `jsonwebtoken`'s default, while `iss`/`aud` only validate when
/// present unless explicitly required, so they are added to
/// `required_spec_claims`. A token missing any of the three is rejected.
pub fn decode_verified(
    token: &str,
    key: &SigningKey,
    issuer: &str,
    audience: &str,
    generic_message: &'static str,
) -> Result<serde_json::Value, RtDbError> {
    let mut validation = Validation::new(key.alg);
    validation.set_issuer(&[issuer]);
    validation.set_audience(&[audience]);
    validation.required_spec_claims.insert("iss".to_string());
    validation.required_spec_claims.insert("aud".to_string());

    jsonwebtoken::decode::<serde_json::Value>(token, &key.key, &validation)
        .map(|data| data.claims)
        .map_err(|err| {
            tracing::warn!(error = %err, "id_token verification failed");
            RtDbError::forbidden(generic_message)
        })
}

/// Seeds the shared cache so a provider's unit tests resolve a key with no
/// network. Test-only: production entries arrive solely through [`fetch`].
#[cfg(test)]
pub async fn seed_for_test(url: &str, jwks: serde_json::Value) {
    JWKS_CACHE.insert(url.to_string(), Arc::new(jwks)).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const MSG: &str = "id_token rejected";

    #[test]
    fn unverified_kid_reads_the_header_kid() {
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"alg":"RS256","kid":"abc"}"#);
        let token = format!("{header}.payload.sig");
        assert_eq!(unverified_kid(&token, MSG).unwrap(), "abc");
    }

    #[test]
    fn unverified_kid_rejects_a_header_without_kid() {
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(br#"{"alg":"RS256"}"#);
        let token = format!("{header}.payload.sig");
        assert!(unverified_kid(&token, MSG).is_err());
    }

    #[test]
    fn select_key_rejects_an_unknown_kid() {
        let jwks = json!({"keys": [{"kty": "RSA", "kid": "k1", "n": "AA", "e": "AQAB"}]});
        assert!(select_key(&jwks, "other", MSG).is_err());
    }

    /// SEC-004: a symmetric `oct` entry must never yield a usable key — that is
    /// the shape an alg-confusion attack needs.
    #[test]
    fn select_key_rejects_a_symmetric_key() {
        let jwks = json!({"keys": [{"kty": "oct", "kid": "k1", "k": "c2VjcmV0"}]});
        assert!(select_key(&jwks, "k1", MSG).is_err());
    }

    #[test]
    fn select_key_rejects_an_unexpected_curve() {
        let jwks =
            json!({"keys": [{"kty": "EC", "kid": "k1", "crv": "P-521", "x": "AA", "y": "AA"}]});
        assert!(select_key(&jwks, "k1", MSG).is_err());
    }

    #[test]
    fn select_key_rejects_an_rsa_entry_with_a_non_rsa_alg() {
        let jwks =
            json!({"keys": [{"kty": "RSA", "kid": "k1", "alg": "HS256", "n": "AA", "e": "AQAB"}]});
        assert!(select_key(&jwks, "k1", MSG).is_err());
    }
}
