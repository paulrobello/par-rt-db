use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::Response;
use axum::routing::get;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::time::{Instant, interval};

use crate::AppState;
use crate::auth::{authed_user, authorize, resolve_bearer};
use crate::error::RtDbError;
use crate::protocol::{ClientMessage, ServerMessage};
use crate::subs::{ConnId, next_conn_id};

const AUTH_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_FRAME_BYTES: usize = 64 * 1024;
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(10);
const RATE_LIMIT_MAX: u32 = 200;
const PING_INTERVAL: Duration = Duration::from_secs(30);
const LIVENESS_TIMEOUT: Duration = Duration::from_secs(75);

/// The realtime sync endpoint, speaking `protocol.rs` messages as JSON text
/// frames (see module-level docs in `protocol.rs` for the wire vocabulary).
pub fn ws_routes() -> Router<Arc<AppState>> {
    Router::new().route("/sync", get(ws_upgrade))
}

async fn ws_upgrade(State(state): State<Arc<AppState>>, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

/// Tracks a rolling 10s message-count window per connection: >200 messages
/// in a window closes the connection (see `handle_text_frame`).
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
/// split). Whatever the exit path, `subs.remove_conn` runs once auth has
/// registered the connection (see the `let ... else { return }` above the
/// loop: before that point nothing has been registered yet).
async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>) {
    let conn_id = next_conn_id();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<ServerMessage>();

    let Some(db) = authenticate(&mut socket, &state).await else {
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
                if send_message(&mut socket, &out_msg).await.is_err() {
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

/// Waits up to `AUTH_TIMEOUT` for the first frame and requires it to be a
/// valid `Auth` message; any other outcome (timeout, wrong message, bad
/// frame) sends `AuthErr` and closes. Returns the authorized database name on
/// success.
async fn authenticate(socket: &mut WebSocket, state: &Arc<AppState>) -> Option<String> {
    let text = match tokio::time::timeout(AUTH_TIMEOUT, socket.recv()).await {
        Ok(Some(Ok(Message::Text(text)))) => text,
        Ok(Some(Ok(Message::Close(_)))) | Ok(None) => return None,
        Ok(Some(Ok(Message::Binary(_)))) => {
            fail_and_close(
                socket,
                RtDbError::bad_request("binary frames are not supported"),
            )
            .await;
            return None;
        }
        _ => {
            // Timeout elapsed, a control frame arrived instead of data, or
            // the socket errored before a first data frame was received.
            fail_and_close(socket, RtDbError::unauthorized("authentication required")).await;
            return None;
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

    Some(db)
}

/// Validates and dispatches one post-auth text frame. Returns whether the
/// connection should now close (frame too large, rate limit exceeded,
/// malformed JSON, or an out-of-order `auth`); every other message is
/// handled and the connection stays open.
async fn handle_text_frame(
    socket: &mut WebSocket,
    state: &Arc<AppState>,
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
            if let Err(error) = state
                .committers
                .subscribe(db, conn_id, query_id.clone(), query, out_tx.clone())
                .await
            {
                let _ = out_tx.send(ServerMessage::SubscribeErr { query_id, error });
            }
            false
        }
        ClientMessage::Unsubscribe { query_id } => {
            state.subs.remove(db, conn_id, &query_id).await;
            false
        }
        ClientMessage::Mutate { mut_id, txn } => {
            match state.committers.mutate(db, txn).await {
                Ok(outcome) => {
                    let _ = out_tx.send(ServerMessage::MutateOk {
                        mut_id,
                        results: outcome.results,
                    });
                }
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

/// Sends `AuthErr { error }` (best-effort) then closes the socket. Used for
/// every protocol violation, pre- and post-auth alike.
async fn fail_and_close(socket: &mut WebSocket, error: RtDbError) {
    let _ = send_message(socket, &ServerMessage::AuthErr { error }).await;
    let _ = socket.send(Message::Close(None)).await;
}

async fn send_message(socket: &mut WebSocket, msg: &ServerMessage) -> Result<(), axum::Error> {
    let text = serde_json::to_string(msg).unwrap_or_else(|err| {
        tracing::error!(error = %err, "failed to serialize server message");
        "{}".to_string()
    });
    socket.send(Message::Text(text.into())).await
}
