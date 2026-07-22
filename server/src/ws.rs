use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocket, WebSocketUpgrade};
use axum::response::Response;
use axum::routing::get;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::time::{Instant, interval};

use crate::AppState;
use crate::auth::{Principal, authed_user, authorize, resolve_bearer};
use crate::error::{ErrorCode, RtDbError};
use crate::protocol::{ClientMessage, ServerMessage};
use crate::subs::{ConnId, next_conn_id};

const AUTH_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_FRAME_BYTES: usize = 64 * 1024;
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(10);
const RATE_LIMIT_MAX: u32 = 200;
const PING_INTERVAL: Duration = Duration::from_secs(30);
const LIVENESS_TIMEOUT: Duration = Duration::from_secs(75);
/// Outbound-queue high-water mark: once a connection's `out_rx` backlog of
/// unsent `ServerMessage`s (subscription pushes, etc.) exceeds this many, the
/// reader on the other end is hopelessly behind and the connection is
/// dropped rather than letting the backlog grow without bound.
const MAX_OUT_QUEUE: usize = 1024;
/// Timeout for writing one outbound frame to the socket. Pairs with
/// `MAX_OUT_QUEUE` above: the backlog check only runs after `socket.send`
/// returns, so a client that stalls TCP entirely (not just a slow reader)
/// blocks inside `send` while the channel keeps growing unchecked. On
/// timeout the connection is dropped, same as any other send failure.
const SEND_TIMEOUT: Duration = Duration::from_secs(30);
/// WS close code for an auth failure (bad/missing token, forbidden for this
/// database): distinct from `CLOSE_PROTOCOL_VIOLATION` so clients know not
/// to blind-retry with the same credentials. Both are in the 4000-4999
/// private-use range.
const CLOSE_AUTH_FAILED: u16 = 4401;
/// WS close code for any other protocol violation (oversized/malformed
/// frame, rate limit exceeded, out-of-order message).
const CLOSE_PROTOCOL_VIOLATION: u16 = 4400;

/// The realtime sync endpoint, speaking `protocol.rs` messages as JSON text
/// frames (see module-level docs in `protocol.rs` for the wire vocabulary).
pub fn ws_routes() -> Router<Arc<AppState>> {
    Router::new().route("/sync", get(ws_upgrade))
}

async fn ws_upgrade(State(state): State<Arc<AppState>>, ws: WebSocketUpgrade) -> Response {
    // Enforced at the protocol layer (not just the app-level length checks
    // below) so an unauthenticated `/sync` connection can't run axum's
    // default 64 MiB max message size as a memory-DoS vector.
    ws.max_message_size(MAX_FRAME_BYTES)
        .max_frame_size(MAX_FRAME_BYTES)
        .on_upgrade(move |socket| handle_socket(socket, state))
}

/// Tracks a tumbling 10s message-count window per connection: >200 messages
/// in a window closes the connection (see `handle_text_frame`). Tumbling
/// (not rolling) means a burst spanning a window boundary can briefly see up
/// to ~2x the nominal rate before the reset; acceptable at this
/// connection-level granularity.
struct RateLimiter {
    window_start: Instant,
    count: u32,
}

impl RateLimiter {
    fn new() -> Self {
        Self {
            window_start: Instant::now(),
            count: 0,
        }
    }

    /// Records one message and returns whether this message pushed the
    /// connection over the limit.
    fn hit(&mut self) -> bool {
        if self.window_start.elapsed() >= RATE_LIMIT_WINDOW {
            self.window_start = Instant::now();
            self.count = 0;
        }
        self.count += 1;
        self.count > RATE_LIMIT_MAX
    }
}

/// Drives one WebSocket connection end to end: auth handshake, then a single
/// `tokio::select!` loop serializing inbound frames, outbound channel
/// messages, and liveness pings through one socket handle (no read/write
/// split). The resolved `principal` is kept in scope for the whole loop (not
/// just the handshake) so every `Subscribe`/`Mutate` can re-check
/// authorization — see `handle_text_frame`. Whatever the exit path,
/// `subs.remove_conn` runs once auth has registered the connection (see the
/// `let ... else { return }` above the loop: before that point nothing has
/// been registered yet).
async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>) {
    let conn_id = next_conn_id();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<ServerMessage>();

    let Some((principal, db)) = authenticate(&mut socket, &state).await else {
        return;
    };

    let mut rate_limiter = RateLimiter::new();
    let mut last_activity = Instant::now();
    let mut ping_timer = interval(PING_INTERVAL);
    ping_timer.tick().await; // interval's first tick fires immediately; skip it

    loop {
        tokio::select! {
            incoming = socket.recv() => {
                let Some(incoming) = incoming else { break };
                let Ok(msg) = incoming else { break };
                last_activity = Instant::now();
                match msg {
                    Message::Close(_) => break,
                    Message::Ping(_) | Message::Pong(_) => {}
                    Message::Binary(_) => {
                        fail_and_close(
                            &mut socket,
                            RtDbError::bad_request("binary frames are not supported"),
                        )
                        .await;
                        break;
                    }
                    Message::Text(text) => {
                        let should_close = handle_text_frame(
                            &mut socket,
                            &state,
                            &principal,
                            &db,
                            conn_id,
                            &out_tx,
                            &mut rate_limiter,
                            &text,
                        )
                        .await;
                        if should_close {
                            break;
                        }
                    }
                }
            }
            Some(out_msg) = out_rx.recv() => {
                let sent = tokio::time::timeout(SEND_TIMEOUT, send_message(&mut socket, &out_msg)).await;
                if !matches!(sent, Ok(Ok(()))) {
                    break;
                }
                // A hopelessly-behind reader: drop it rather than let its
                // backlog grow without bound. `remove_conn` below still runs.
                if out_rx.len() > MAX_OUT_QUEUE {
                    break;
                }
            }
            _ = ping_timer.tick() => {
                if last_activity.elapsed() > LIVENESS_TIMEOUT {
                    break;
                }
                if socket.send(Message::Ping(Bytes::new())).await.is_err() {
                    break;
                }
            }
        }
    }

    state.subs.remove_conn(&db, conn_id).await;
}

/// Waits up to `AUTH_TIMEOUT` (in total, across any leading `Ping`/`Pong`
/// frames — a client that opens with a keepalive isn't penalized) for the
/// first data frame and requires it to be a valid `Auth` message; any other
/// outcome (timeout, wrong message, bad frame) sends `AuthErr` and closes.
/// Returns the resolved principal and authorized database name on success.
async fn authenticate(
    socket: &mut WebSocket,
    state: &Arc<AppState>,
) -> Option<(Principal, String)> {
    let deadline = Instant::now() + AUTH_TIMEOUT;
    let text = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, socket.recv()).await {
            Ok(Some(Ok(Message::Text(text)))) => break text,
            Ok(Some(Ok(Message::Close(_)))) | Ok(None) => return None,
            Ok(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => continue,
            Ok(Some(Ok(Message::Binary(_)))) => {
                fail_and_close(
                    socket,
                    RtDbError::bad_request("binary frames are not supported"),
                )
                .await;
                return None;
            }
            _ => {
                // Timeout elapsed or the socket errored before a first data
                // frame was received.
                fail_and_close(socket, RtDbError::unauthorized("authentication required")).await;
                return None;
            }
        }
    };

    if text.len() > MAX_FRAME_BYTES {
        fail_and_close(socket, RtDbError::bad_request("frame too large")).await;
        return None;
    }

    let (token, db) = match serde_json::from_str::<ClientMessage>(&text) {
        Ok(ClientMessage::Auth { token, db }) => (token, db),
        _ => {
            fail_and_close(
                socket,
                RtDbError::unauthorized("first message must be auth"),
            )
            .await;
            return None;
        }
    };

    let principal = match resolve_bearer(&state.pool, &token).await {
        Ok(principal) => principal,
        Err(err) => {
            fail_and_close(socket, err).await;
            return None;
        }
    };
    if let Err(err) = authorize(&state.pool, &principal, &db).await {
        fail_and_close(socket, err).await;
        return None;
    }

    let user = authed_user(&principal);
    if send_message(socket, &ServerMessage::AuthOk { user })
        .await
        .is_err()
    {
        return None;
    }

    Some((principal, db))
}

/// Validates and dispatches one post-auth text frame. Returns whether the
/// connection should now close (frame too large, rate limit exceeded,
/// malformed JSON, or an out-of-order `auth`); every other message is
/// handled and the connection stays open. `Subscribe` and `Mutate` each
/// re-check authorization for `principal` on `db` first — authorization can
/// be revoked mid-session (e.g. an allowlist removal) — and on failure the
/// operation errors (`SubscribeErr`/`MutateErr`) without closing the
/// connection.
// A3's `principal` param pushes this past clippy's default 7-argument
// threshold; every param is independently needed by a different message
// arm, so bundling them into a context struct would add indirection without
// reducing coupling.
#[allow(clippy::too_many_arguments)]
async fn handle_text_frame(
    socket: &mut WebSocket,
    state: &Arc<AppState>,
    principal: &Principal,
    db: &str,
    conn_id: ConnId,
    out_tx: &UnboundedSender<ServerMessage>,
    rate_limiter: &mut RateLimiter,
    text: &str,
) -> bool {
    if text.len() > MAX_FRAME_BYTES {
        fail_and_close(socket, RtDbError::bad_request("frame too large")).await;
        return true;
    }

    if rate_limiter.hit() {
        fail_and_close(socket, RtDbError::bad_request("rate limit exceeded")).await;
        return true;
    }

    let client_msg: ClientMessage = match serde_json::from_str(text) {
        Ok(msg) => msg,
        Err(_) => {
            fail_and_close(socket, RtDbError::bad_request("malformed message")).await;
            return true;
        }
    };

    match client_msg {
        ClientMessage::Auth { .. } => {
            fail_and_close(socket, RtDbError::bad_request("unexpected message type")).await;
            true
        }
        ClientMessage::Subscribe { query_id, query } => {
            match authorize(&state.pool, principal, db).await {
                Ok(()) => {
                    if let Err(error) = state
                        .committers
                        .subscribe(db, conn_id, query_id.clone(), *query, out_tx.clone())
                        .await
                    {
                        let _ = out_tx.send(ServerMessage::SubscribeErr { query_id, error });
                    }
                }
                Err(error) => {
                    let _ = out_tx.send(ServerMessage::SubscribeErr { query_id, error });
                }
            }
            false
        }
        ClientMessage::Unsubscribe { query_id } => {
            state.subs.remove(db, conn_id, &query_id).await;
            false
        }
        ClientMessage::Mutate { mut_id, txn } => {
            match authorize(&state.pool, principal, db).await {
                Ok(()) => match state.committers.mutate(db, Some(mut_id.clone()), txn).await {
                    Ok(outcome) => {
                        let _ = out_tx.send(ServerMessage::MutateOk {
                            mut_id,
                            results: outcome.results,
                        });
                    }
                    Err(error) => {
                        let _ = out_tx.send(ServerMessage::MutateErr { mut_id, error });
                    }
                },
                Err(error) => {
                    let _ = out_tx.send(ServerMessage::MutateErr { mut_id, error });
                }
            }
            false
        }
        ClientMessage::Ping => {
            let _ = out_tx.send(ServerMessage::Pong);
            false
        }
    }
}

/// Sends `AuthErr { error }` (best-effort) then closes the socket with a
/// close code derived from `error`'s `ErrorCode`: `Unauthorized`/`Forbidden`
/// (bad credentials, revoked authz) close with `CLOSE_AUTH_FAILED` so the
/// client knows not to blind-retry the same token; anything else closes with
/// `CLOSE_PROTOCOL_VIOLATION`. Used for every protocol violation, pre- and
/// post-auth alike.
async fn fail_and_close(socket: &mut WebSocket, error: RtDbError) {
    let code = match error.code {
        ErrorCode::Unauthorized | ErrorCode::Forbidden => CLOSE_AUTH_FAILED,
        _ => CLOSE_PROTOCOL_VIOLATION,
    };
    let _ = send_message(socket, &ServerMessage::AuthErr { error }).await;
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code,
            reason: Utf8Bytes::from_static(""),
        })))
        .await;
}

/// Serializes `msg` and sends it as a text frame. `ServerMessage` cannot
/// fail to serialize in practice (no non-string map keys, no NaN/Infinity
/// floats), so on the theoretical failure this logs and skips the send
/// (returning `Ok`, since the socket itself is fine) rather than emitting an
/// invalid `"{}"` frame with no `type` tag.
async fn send_message(socket: &mut WebSocket, msg: &ServerMessage) -> Result<(), axum::Error> {
    let text = match serde_json::to_string(msg) {
        Ok(text) => text,
        Err(err) => {
            tracing::error!(error = %err, "failed to serialize server message; skipping send");
            return Ok(());
        }
    };
    socket.send(Message::Text(text.into())).await
}
