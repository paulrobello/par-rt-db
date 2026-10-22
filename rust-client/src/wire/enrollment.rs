//! HTTP request/response bodies for the public enrollment surface
//! (`POST /auth/enroll`) — the redemption side of host machine
//! authorization. The admin-side grant DTOs live in [`super::admin`]; these
//! are unconditional because redemption requires no admin key. Mirrors
//! server `auth::enrollment` field-for-field: camelCase on the wire, and
//! the strict `EnrollRequest` carries ONLY the grant string (the server
//! denies unknown fields — no db/host/capability override is accepted).

use crate::wire::HostCapability;
use serde::{Deserialize, Serialize};

/// `POST /auth/enroll` request. Public as part of the wire vocabulary (it
/// mirrors the server's strict `deny_unknown_fields` DTO), but callers
/// normally use [`enroll`](crate::http::RtDbHttpClient::enroll), which takes
/// the grant string directly — the shape is fixed at exactly `{grant}` so
/// no db/host/capability override can ever ride along.
#[derive(Debug, Clone, Serialize)]
pub struct EnrollRequest {
    /// The one-time plaintext grant secret.
    pub grant: String,
}

/// `POST /auth/enroll` response after a successful claim-and-mint. Mirrors
/// server `EnrollResponse` (camelCase, `tokenId` renamed).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct RedeemedGrant {
    /// The minted machine token's stable id (for revoke/list).
    pub token_id: String,
    /// The plaintext bearer token — shown once, never stored server-side.
    pub token: String,
    /// The database the token is scoped to.
    pub db: String,
    /// The capability the grant fixed at creation time.
    pub capability: HostCapability,
}
