//! `/admin/stream` op-feed consumer — the WebSocket half of the admin control
//! plane, gated on `ws` (on top of the module-wide `admin`).
//!
//! Mirrors ts-client's `streamAdmin()` (`ts-client/src/admin.ts`): the same
//! `db`/`table` query filter and the same [`AdminStreamFrame`] union, yielded
//! as the server sends it — the ≤200-event replay of the op ring, then live
//! events, interleaved with a ~1s gauge snapshot.
//!
//! Two deliberate differences from the browser client, both because this one
//! is a machine client:
//!
//! * The admin key rides the `Authorization: Bearer` header, not the
//!   `rtdb-admin.<token>` subprotocol. The subprotocol exists because browsers
//!   cannot set headers on a WS handshake; a CLI can, and the server prefers
//!   the header (`server/src/admin/observability.rs::admin_stream`). Offering
//!   no subprotocol also means the server negotiates none, which is what
//!   tungstenite expects.
//! * The connection is one-shot. Reconnect policy lives with the caller, since
//!   `RtDbAdminClient` is a one-shot HTTP client everywhere else and the
//!   retry-vs-surface decision depends on the error (an auth rejection must not
//!   be retried forever). [`AdminStream::next`] reports each terminal condition
//!   as a typed error so a caller can classify it by [`ErrorCode`].

use futures_util::stream::{BoxStream, StreamExt};
use tokio::time::{Duration, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::{AUTHORIZATION, HeaderValue};
use tokio_tungstenite::tungstenite::{Error as WsError, http::Response};

use super::RtDbAdminClient;
use crate::error::{ErrorCode, ErrorEnvelope, RtDbError};
use crate::wire::admin::AdminStreamFrame;

/// Handshake budget, matching the reactive ws client's `connect_async` timeout.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// Close code the server sends when an already-open admin stream's credential
/// stops validating (SEC-006 — revocation, expiry, or admin-list removal takes
/// effect on the gauge tick). Surfaced as [`ErrorCode::Unauthorized`] so a
/// caller does not treat a revoked key as a transient blip and reconnect
/// forever.
const CLOSE_CREDENTIAL_INVALID: u16 = 4401;

/// A live `/admin/stream` socket. Built by
/// [`RtDbAdminClient::stream_admin`]; drop it to close the socket.
///
/// The socket is boxed rather than named concretely: spelling out
/// `WebSocketStream<MaybeTlsStream<TcpStream>>` would pull `tokio::net`, a
/// feature this crate deliberately leaves off (see `ws::send_text`).
pub struct AdminStream {
    socket: BoxStream<'static, Result<WsMessage, WsError>>,
}

impl std::fmt::Debug for AdminStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminStream").finish_non_exhaustive()
    }
}

impl AdminStream {
    /// Next frame from the feed.
    ///
    /// * `Some(Ok(frame))` — an op event or a gauge snapshot.
    /// * `Some(Err(e))` — the stream ended for a reportable reason. `e.code` is
    ///   [`ErrorCode::Unauthorized`] when the server revoked the credential
    ///   mid-stream (close `4401`) and [`ErrorCode::Internal`] for a transport
    ///   failure. Nothing follows an error.
    /// * `None` — the peer closed cleanly (or went away); reconnecting is the
    ///   caller's call.
    ///
    /// A text frame this client cannot parse is skipped, not fatal: an unknown
    /// `kind` from a newer server must not kill an operator's tail. Ping/pong
    /// and binary frames are likewise skipped.
    pub async fn next(&mut self) -> Option<Result<AdminStreamFrame, RtDbError>> {
        loop {
            match self.socket.next().await {
                Some(Ok(WsMessage::Text(text))) => {
                    match serde_json::from_str::<AdminStreamFrame>(text.as_str()) {
                        Ok(frame) => return Some(Ok(frame)),
                        Err(_) => continue,
                    }
                }
                Some(Ok(WsMessage::Close(frame))) => {
                    let credential_revoked = frame
                        .as_ref()
                        .is_some_and(|f| u16::from(f.code) == CLOSE_CREDENTIAL_INVALID);
                    if !credential_revoked {
                        return None;
                    }
                    let reason = frame
                        .map(|f| f.reason.to_string())
                        .filter(|r| !r.is_empty())
                        .unwrap_or_else(|| "admin credential no longer valid".to_string());
                    return Some(Err(RtDbError::new(ErrorCode::Unauthorized, reason)));
                }
                Some(Ok(_)) => continue,
                Some(Err(e)) => {
                    return Some(Err(RtDbError::internal(format!(
                        "admin stream connection lost: {e}"
                    ))));
                }
                None => return None,
            }
        }
    }
}

impl RtDbAdminClient {
    /// Open `/admin/stream?db=<db>&table=<t>` and stream the op feed.
    ///
    /// `db`/`table` filter both the replay and the live broadcast, exactly as
    /// they do on `GET /admin/ops/recent`. Every connection replays up to 200
    /// ring events before going live, so a caller that reconnects will see the
    /// tail of the previous session again — `OpEvent` carries no monotonic
    /// sequence to dedup on, only `ts`.
    ///
    /// A rejected handshake (the server gates the admin bearer *before* WS
    /// negotiation, so a bad key is a plain 401/403, not a socket that opens
    /// and dies) comes back as the server's own `{code, message}` envelope.
    pub async fn stream_admin(
        &self,
        db: Option<&str>,
        table: Option<&str>,
    ) -> Result<AdminStream, RtDbError> {
        let request = self.admin_stream_request(db, table)?;
        match timeout(CONNECT_TIMEOUT, connect_async(request)).await {
            Ok(Ok((socket, _response))) => Ok(AdminStream {
                socket: socket.boxed(),
            }),
            Ok(Err(WsError::Http(response))) => Err(upgrade_error(response)),
            Ok(Err(e)) => Err(RtDbError::internal(format!(
                "admin stream connect failed: {e}"
            ))),
            Err(_) => Err(RtDbError::internal(
                "admin stream connect timed out after 15s",
            )),
        }
    }

    /// Build the upgrade request: `http(s)` → `ws(s)`, the filter as query
    /// pairs (percent-encoded by `Url`, never interpolated), and the admin key
    /// as the bearer.
    fn admin_stream_request(
        &self,
        db: Option<&str>,
        table: Option<&str>,
    ) -> Result<tokio_tungstenite::tungstenite::http::Request<()>, RtDbError> {
        let base = format!("{}/admin/stream", crate::ws::sync_url(&self.url));
        let mut url = reqwest::Url::parse(&base).map_err(|e| {
            RtDbError::new(ErrorCode::BadRequest, format!("invalid server url: {e}"))
        })?;
        // Guarded: `query_pairs_mut` leaves a bare `?` behind even when nothing
        // is appended, so an unfiltered stream would upgrade to
        // `/admin/stream?`.
        if db.is_some() || table.is_some() {
            let mut pairs = url.query_pairs_mut();
            if let Some(db) = db {
                pairs.append_pair("db", db);
            }
            if let Some(table) = table {
                pairs.append_pair("table", table);
            }
        }
        let mut request = url.as_str().into_client_request().map_err(|e| {
            RtDbError::new(ErrorCode::BadRequest, format!("invalid server url: {e}"))
        })?;
        let bearer = HeaderValue::from_str(&format!("Bearer {}", self.token)).map_err(|_| {
            RtDbError::new(
                ErrorCode::BadRequest,
                "admin key is not a valid header value",
            )
        })?;
        request.headers_mut().insert(AUTHORIZATION, bearer);
        Ok(request)
    }
}

/// Map a rejected upgrade to the client error type, preferring the server's own
/// `{code, message}` body so the operator reads the real cause instead of a
/// status number.
///
/// The body is best-effort by construction: tungstenite fills it with whatever
/// had already been buffered behind the headers when it gave up on the
/// handshake (`*e.body_mut() = Some(tail)`), never waiting on `Content-Length`,
/// so a response split across TCP segments arrives here with an empty body. The
/// status fallback is therefore a normal path, not a corner case — but the
/// `code` is derived from the status either way, so classification never
/// depends on the body arriving.
fn upgrade_error(response: Response<Option<Vec<u8>>>) -> RtDbError {
    let status = response.status();
    if let Some(body) = response.body().as_ref()
        && let Ok(envelope) = serde_json::from_slice::<ErrorEnvelope>(body)
    {
        return RtDbError::from_envelope(envelope);
    }
    let code = match status.as_u16() {
        401 => ErrorCode::Unauthorized,
        403 => ErrorCode::Forbidden,
        _ => ErrorCode::Internal,
    };
    RtDbError::new(
        code,
        format!(
            "admin stream upgrade rejected with status {}",
            status.as_u16()
        ),
    )
}
