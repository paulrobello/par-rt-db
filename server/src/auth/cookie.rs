//! SEC-001: HttpOnly session cookie for the operator dashboard.
//!
//! The dashboard's bearer credential (the configured admin key on admin-key
//! login, or the OAuth session token on OAuth login) is carried in an HttpOnly
//! cookie so client-side JS can never read it — XSS or a malicious browser
//! extension cannot lift it from `document.cookie`. The browser attaches the
//! cookie to same-origin requests automatically, including WebSocket upgrade
//! handshakes, so `/api/*`, `/admin/*`, `/admin/stream`, and `/sync` all
//! authenticate without JS ever holding the secret.
//!
//! The existing `Authorization`-header and WS-subprotocol auth paths are
//! untouched; the cookie is an additional source. CLI/automation and machine
//! tokens keep working exactly as before (they send a header / in-band token).

use axum::http::{HeaderMap, HeaderValue};

use crate::error::RtDbError;

/// Cookie name carrying the dashboard session credential.
pub(crate) const SESSION_COOKIE: &str = "rtdb_session";

/// Cookie `Max-Age` in seconds (30 days). The server re-validates the credential
/// on every request regardless, so this only bounds how long a stolen cookie is
/// replayable; it is independent of the OAuth session TTL stored server-side.
const COOKIE_MAX_AGE_SECS: u64 = 30 * 24 * 60 * 60;

/// Reads the `rtdb_session` cookie value from the `Cookie:` header, if present.
/// Borrows from `headers` — no allocation.
pub(crate) fn session_cookie(headers: &HeaderMap) -> Option<&str> {
    let raw = headers.get(axum::http::header::COOKIE)?.to_str().ok()?;
    raw.split(';').find_map(|pair| {
        let (name, value) = pair.split_once('=')?;
        (name.trim() == SESSION_COOKIE).then_some(value.trim())
    })
}

/// Builds the `Set-Cookie` header value for a freshly issued credential. `secure`
/// adds the `Secure` attribute (only when the request arrived over HTTPS — see
/// [`request_is_secure`]); local http dev omits it so the cookie is still set.
///
/// The value is validated against cookie-attribute-injection characters (`;`,
/// `,`, whitespace, control bytes); an admin key or session token containing any
/// of them is rejected (fails closed) rather than silently splitting the cookie.
pub(crate) fn set_session_cookie(value: &str, secure: bool) -> Result<HeaderValue, RtDbError> {
    if value.is_empty()
        || value
            .bytes()
            .any(|b| matches!(b, b';' | b',' | b' ' | b'\t' | b'\r' | b'\n') || b < 0x20)
    {
        return Err(RtDbError::internal(
            "session credential contains characters illegal in a cookie value",
        ));
    }
    let mut s = format!(
        "{SESSION_COOKIE}={value}; HttpOnly; SameSite=Lax; Path=/; Max-Age={COOKIE_MAX_AGE_SECS}"
    );
    if secure {
        s.push_str("; Secure");
    }
    HeaderValue::from_str(&s).map_err(|_| RtDbError::internal("invalid session cookie value"))
}

/// Builds the `Set-Cookie` header value that deletes the session cookie.
pub(crate) fn clear_session_cookie() -> HeaderValue {
    // `from_str` (not `from_static`): the name interpolates `SESSION_COOKIE`.
    HeaderValue::from_str(&format!(
        "{SESSION_COOKIE}=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0; Expires=Thu, 01 Jan 1970 00:00:00 GMT"
    ))
    .expect("static clear-cookie template is a valid header value")
}

/// Cookie name carrying the login-CSRF double-submit nonce. Its value is the
/// OAuth `state` token minted at `/begin`; `/callback` requires it to match.
pub(crate) const OAUTH_CSRF_COOKIE: &str = "rtdb-oauth-csrf";

/// `Max-Age` (seconds) for the CSRF nonce — matches `STATE_TTL_MS` (10 min).
const CSRF_MAX_AGE_SECS: u64 = 600;

/// Reads the `rtdb-oauth-csrf` cookie value from the `Cookie:` header, if present.
pub(crate) fn oauth_csrf_cookie(headers: &HeaderMap) -> Option<&str> {
    let raw = headers.get(axum::http::header::COOKIE)?.to_str().ok()?;
    raw.split(';').find_map(|pair| {
        let (name, value) = pair.split_once('=')?;
        (name.trim() == OAUTH_CSRF_COOKIE).then_some(value.trim())
    })
}

/// Builds the `Set-Cookie` for the CSRF nonce. `SameSite=None` is required: the
/// provider → callback redirect is a top-level cross-site navigation, so `Lax`
/// would not attach the cookie and the defense would never fire. `Secure` mirrors
/// the session cookie (omitted for local http dev). Same injection-char guard as
/// `set_session_cookie` (fails closed).
pub(crate) fn set_oauth_csrf_cookie(value: &str, secure: bool) -> Result<HeaderValue, RtDbError> {
    if value.is_empty()
        || value
            .bytes()
            .any(|b| matches!(b, b';' | b',' | b' ' | b'\t' | b'\r' | b'\n') || b < 0x20)
    {
        return Err(RtDbError::internal(
            "oauth csrf cookie value contains illegal characters",
        ));
    }
    let mut s = format!(
        "{OAUTH_CSRF_COOKIE}={value}; HttpOnly; SameSite=None; Path=/; Max-Age={CSRF_MAX_AGE_SECS}"
    );
    if secure {
        s.push_str("; Secure");
    }
    HeaderValue::from_str(&s).map_err(|_| RtDbError::internal("invalid oauth csrf cookie value"))
}

/// Builds the `Set-Cookie` that deletes the CSRF nonce (single-use hygiene on a
/// successful callback).
pub(crate) fn clear_oauth_csrf_cookie() -> HeaderValue {
    HeaderValue::from_str(&format!(
        "{OAUTH_CSRF_COOKIE}=; HttpOnly; SameSite=None; Path=/; Max-Age=0; Expires=Thu, 01 Jan 1970 00:00:00 GMT"
    ))
    .expect("static clear-cookie template is a valid header value")
}

/// True when the request arrived over HTTPS. The Cloudflare tunnel sets
/// `X-Forwarded-Proto: https`; a same-origin dashboard request carries it. Local
/// http dev has no such header → `false` → `Secure` is omitted so the cookie is
/// still accepted by the browser. (CSRF is bounded by `SameSite=Lax` plus the
/// server's CORS origin allowlist, which already gates cross-origin API access.)
pub(crate) fn request_is_secure(headers: &HeaderMap) -> bool {
    headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| s.eq_ignore_ascii_case("https"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_session_cookie_among_pairs() {
        let mut h = HeaderMap::new();
        h.insert(
            "cookie",
            "theme=dark; rtdb_session=abc123; lang=en".parse().unwrap(),
        );
        assert_eq!(session_cookie(&h), Some("abc123"));
    }

    #[test]
    fn missing_cookie_is_none() {
        let h = HeaderMap::new();
        assert_eq!(session_cookie(&h), None);
    }

    #[test]
    fn does_not_match_prefix_siblings() {
        // `x_rtdb_session=` must not match the `rtdb_session` name.
        let mut h = HeaderMap::new();
        h.insert("cookie", "x_rtdb_session=nope".parse().unwrap());
        assert_eq!(session_cookie(&h), None);
    }

    #[test]
    fn set_cookie_includes_attributes_and_optional_secure() {
        let plain = set_session_cookie("deadbeef", false)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(plain.contains("rtdb_session=deadbeef"));
        assert!(plain.contains("HttpOnly"));
        assert!(plain.contains("SameSite=Lax"));
        assert!(!plain.contains("Secure"));

        let secure = set_session_cookie("deadbeef", true)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(secure.contains("Secure"));
    }

    #[test]
    fn set_cookie_rejects_injection_chars() {
        // `;` / `,` would let the value masquerade as a cookie attribute;
        // whitespace and control bytes are likewise illegal in a cookie value.
        assert!(set_session_cookie("a;b", false).is_err());
        assert!(set_session_cookie("a,b", false).is_err());
        assert!(set_session_cookie("a b", false).is_err());
        assert!(set_session_cookie("a\tb", false).is_err());
        assert!(set_session_cookie("", false).is_err());
        // A sane admin key / hex session token is accepted.
        assert!(set_session_cookie("deadbeef-0123", false).is_ok());
    }

    #[test]
    fn oauth_csrf_cookie_reads_among_pairs() {
        let mut h = HeaderMap::new();
        h.insert(
            "cookie",
            "rtdb_session=abc; rtdb-oauth-csrf=xyz-state; lang=en"
                .parse()
                .unwrap(),
        );
        assert_eq!(oauth_csrf_cookie(&h), Some("xyz-state"));
    }

    #[test]
    fn oauth_csrf_cookie_missing_is_none() {
        let mut h = HeaderMap::new();
        h.insert("cookie", "rtdb_session=abc".parse().unwrap());
        assert_eq!(oauth_csrf_cookie(&h), None);
    }

    #[test]
    fn set_oauth_csrf_cookie_includes_attributes() {
        let plain = set_oauth_csrf_cookie("deadbeef", false)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(plain.contains("rtdb-oauth-csrf=deadbeef"));
        assert!(plain.contains("HttpOnly"));
        assert!(plain.contains("SameSite=None"));
        assert!(plain.contains("Max-Age=600"));
        assert!(!plain.contains("Secure"));

        let secure = set_oauth_csrf_cookie("deadbeef", true)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(secure.contains("Secure"));
    }

    #[test]
    fn set_oauth_csrf_cookie_rejects_injection_chars() {
        assert!(set_oauth_csrf_cookie("a;b", true).is_err());
        assert!(set_oauth_csrf_cookie("a,b", true).is_err());
        assert!(set_oauth_csrf_cookie("a b", true).is_err());
        assert!(set_oauth_csrf_cookie("", true).is_err());
        assert!(set_oauth_csrf_cookie("deadbeef-0123", true).is_ok());
    }
}
