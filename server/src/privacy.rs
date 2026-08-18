//! Unauthenticated `/privacy` &mdash; serves par-rt-db's own privacy policy as
//! static HTML, so the OAuth consent screen's required privacy URL can point at
//! the deployment itself (e.g. `https://rtdb.example.com/privacy`) instead of an
//! externally hosted page. Like `/healthz`, it is **public and stateless**: the
//! body is a compile-time-embedded HTML file, never `Config`, secrets, or user
//! data.
//!
//! The policy text lives in `static/privacy.html` (not a Rust string literal) so
//! it can be edited as a file; a deploy ships it inside the binary, with no
//! dependency on `RTDB_STATIC_DIR` &mdash; the route works in API-only mode too.

use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};

/// The privacy policy HTML, embedded at compile time from `static/privacy.html`.
const PRIVACY_HTML: &str = include_str!("static/privacy.html");

/// `GET /privacy`: `200` + the embedded policy as `text/html; charset=utf-8`.
/// `Cache-Control: no-cache` so a policy edit (which ships via redeploy) is live
/// without an intermediate cache serving a stale copy.
pub async fn handler() -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-store, must-revalidate"),
    );
    (StatusCode::OK, headers, PRIVACY_HTML).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    #[tokio::test]
    async fn privacy_returns_html_containing_the_policy() {
        let resp = handler().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/html; charset=utf-8"
        );
        assert_eq!(
            resp.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-cache, no-store, must-revalidate"
        );
        let bytes = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        assert!(body.contains("Privacy Policy"));
        assert!(body.contains("par-rt-db operator"));
    }
}
