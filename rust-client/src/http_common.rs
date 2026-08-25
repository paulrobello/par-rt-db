//! Shared reqwest response-parsing helpers for the HTTP data-plane
//! ([`crate::http::RtDbHttpClient`]) and admin control-plane
//! ([`crate::admin::RtDbAdminClient`]) clients. Both clients parsed a
//! success/error JSON envelope identically; hoisted here instead of
//! maintaining two copies (QA-013).

use crate::error::{ErrorEnvelope, RtDbError};
use crate::query::parse_result;
use serde::de::DeserializeOwned;

/// Parse `resp` as `T` on success, or as an [`ErrorEnvelope`] (falling back to
/// a generic INTERNAL error when the body doesn't parse) on failure.
pub(crate) async fn deserialize<T: DeserializeOwned>(
    resp: reqwest::Response,
) -> Result<T, RtDbError> {
    let status = resp.status();
    if status.is_success() {
        return resp
            .json::<T>()
            .await
            .map_err(|e| RtDbError::internal(format!("invalid response body: {e}")));
    }
    // Error path: try to parse {code,message}, else INTERNAL.
    match resp.json::<ErrorEnvelope>().await {
        Ok(env) => Err(RtDbError::from_envelope(env)),
        Err(_) => Err(RtDbError::internal(format!(
            "request failed with status {}",
            status.as_u16()
        ))),
    }
}

/// Parse a `{result: <value>}` query-response envelope and decode `result`
/// into `T`.
pub(crate) async fn json_result<T: DeserializeOwned>(
    resp: reqwest::Response,
) -> Result<T, RtDbError> {
    #[derive(serde::Deserialize)]
    struct QueryResponse {
        result: serde_json::Value,
    }
    let parsed = deserialize::<QueryResponse>(resp).await?;
    parse_result::<T>(parsed.result)
}

/// ARC-013: header name carrying the requesting client's protocol version on
/// the one-shot HTTP API — mirrors the WS `Auth` frame's `protocolVersion`.
pub(crate) const PROTOCOL_HEADER: &str = "X-Rtdb-Protocol";

/// The single seam every authorized request in this crate goes through:
/// `Authorization: Bearer <token>` plus the ARC-013 `X-Rtdb-Protocol` header.
///
/// Attaching the version per request rather than as a `reqwest::Client`
/// default keeps it on calls made through
/// [`RtDbAdminClient::from_parts`](crate::admin::RtDbAdminClient::from_parts),
/// which accepts a caller-supplied client. Every request site in `http.rs` and
/// `admin/mod.rs` calls `.authed(..)`; a bare `bearer_auth` outside this file
/// would silently drop the version header.
pub(crate) trait AuthedRequest {
    /// Attach the bearer token and the protocol-version header.
    fn authed(self, token: &str) -> Self;
}

impl AuthedRequest for reqwest::RequestBuilder {
    fn authed(self, token: &str) -> Self {
        self.bearer_auth(token)
            .header(PROTOCOL_HEADER, crate::wire::PROTOCOL_VERSION.to_string())
    }
}
