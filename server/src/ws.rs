//! Reactive `/sync` WebSocket handler.
//!
//! NOTE: `permessage-deflate` (RFC 7692) WS compression is NOT available here.
//! axum 0.8's WebSocket is built on `tungstenite`, which does not implement
//! permessage-deflate (no feature flag). Cutting WS bandwidth therefore
//! requires a fronting proxy (e.g. nginx with WS compression) or swapping the
//! WS library (yawc/fastwebsockets). See ideas.md.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocket, WebSocketUpgrade};
use axum::http::HeaderMap;
use axum::response::Response;
use axum::routing::get;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::time::{Instant, interval};

use crate::AppState;
use crate::auth::{Principal, PrincipalCtx, authed_user, authorize, is_admin, resolve_bearer};
use crate::db::now_ms;
use crate::error::{ErrorCode, RtDbError};
use crate::protocol::{ClientMessage, ServerMessage};
use crate::rate_limit::{RateDecision, evaluate};
use crate::scheduler;
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

async fn ws_upgrade(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    // Enforced at the protocol layer (not just the app-level length checks
    // below) so an unauthenticated `/sync` connection can't run axum's
    // default 64 MiB max message size as a memory-DoS vector.
    ws.max_message_size(MAX_FRAME_BYTES)
        .max_frame_size(MAX_FRAME_BYTES)
        // `headers` are forwarded so SEC-001 phase 2 can authenticate a
        // tokenless Auth message from the `rtdb_session` cookie.
        .on_upgrade(move |socket| handle_socket(socket, state, headers))
}

/// Tracks a tumbling 10s message-count window per connection: >200 messages
/// in a window closes the connection (see `handle_text_frame`). Tumbling
/// (not rolling) means a burst spanning a window boundary can briefly see up
/// to ~2x the nominal rate before the reset; acceptable at this
/// connection-level granularity.
struct ConnRateLimiter {
    window_start: Instant,
    count: u32,
}

impl ConnRateLimiter {
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
async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>, headers: HeaderMap) {
    let conn_id = next_conn_id();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<ServerMessage>();

    let Some((principal, db)) = authenticate(&mut socket, &state, &headers).await else {
        return;
    };
    state.runtime.metrics.ws_connect();

    let mut rate_limiter = ConnRateLimiter::new();
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

    state.realtime.subs.remove_conn(&db, conn_id).await;
    state.runtime.metrics.ws_disconnect();
}

/// Waits up to `AUTH_TIMEOUT` (in total, across any leading `Ping`/`Pong`
/// frames — a client that opens with a keepalive isn't penalized) for the
/// first data frame and requires it to be a valid `Auth` message; any other
/// outcome (timeout, wrong message, bad frame) sends `AuthErr` and closes.
/// Returns the resolved principal and authorized database name. Whether the
/// principal is a server-wide dashboard admin is computed here ONLY to decide
/// whether the initial per-db `authorize` runs at the handshake; it is NOT
/// returned — `handle_text_frame` re-runs `is_admin` on each Subscribe/Mutate
/// so an admin removed from `rtdb_auth.admins` mid-connection loses the bypass
/// on the next op (SEC-004).
async fn authenticate(
    socket: &mut WebSocket,
    state: &Arc<AppState>,
    headers: &HeaderMap,
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

    // SEC-001 phase 2: a tokenless Auth authenticates from the HttpOnly
    // `rtdb_session` cookie the browser sent on the WS upgrade; an explicit
    // token (CLI/SDK/machine) resolves as before. Either credential runs
    // through the same `resolve_bearer` path.
    let credential: &str = match token.as_deref() {
        Some(t) => t,
        None => match crate::auth::cookie::session_cookie(headers) {
            Some(c) => c,
            None => {
                fail_and_close(socket, RtDbError::unauthorized("authentication required")).await;
                return None;
            }
        },
    };
    let principal = match resolve_bearer(&state.pool, credential).await {
        Ok(principal) => principal,
        Err(err) => {
            fail_and_close(socket, err).await;
            return None;
        }
    };
    // Admin OAuth sessions are authorized for every database (dashboard live
    // tables). `is_admin` is consulted here ONLY to gate the handshake's
    // per-db `authorize`; it is re-computed on each later Subscribe/Mutate
    // (SEC-004) so revoking admin takes effect on open connections. Machine
    // principals are never admin.
    let admin = is_admin(&state.pool, &principal).await;
    if !admin && let Err(err) = authorize(&state.pool, &principal, &db).await {
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
/// handled and the connection stays open. Every post-auth message arm —
/// `Subscribe`, `Mutate`, and the schedule family (`Schedule`,
/// `CancelSchedule`, `PauseSchedule`, `ResumeSchedule`, `ListSchedules`) —
/// re-checks authorization for `principal` on `db` first — authorization
/// can be revoked mid-session (e.g. an allowlist removal) — and on failure
/// the operation errors (e.g. `SubscribeErr`/`MutateErr`) without closing
/// the connection.
///
/// `Subscribe` and `Mutate` additionally re-run `is_admin` per op (SEC-004):
/// the admin bypass is re-justified against the live `rtdb_auth.admins` table
/// on each message, so an OAuth user removed from the admin allowlist while
/// holding an open `/sync` stops bypassing per-db `authorize` and stops
/// mutating with `owner=None` on the next op. (The schedule family never
/// carried the admin bypass, so it is unchanged.) `is_admin` returns false on
/// DB error (`auth::is_admin`), so a transient Postgres outage fails safe:
/// the bypass is withheld, not widened.
// A3's `principal` param pushes this past clippy's default 7-argument
// threshold; every param is independently needed by a different message arm,
// so bundling them into a context struct would add indirection without
// reducing coupling.
#[allow(clippy::too_many_arguments)]
async fn handle_text_frame(
    socket: &mut WebSocket,
    state: &Arc<AppState>,
    principal: &Principal,
    db: &str,
    conn_id: ConnId,
    out_tx: &UnboundedSender<ServerMessage>,
    rate_limiter: &mut ConnRateLimiter,
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
            // SEC-004: re-justify the admin bypass against the live admin
            // allowlist on every op — a principal removed from
            // `rtdb_auth.admins` mid-connection must not keep bypassing
            // per-db `authorize`. `is_admin` fails safe (returns false on DB
            // error).
            let admin = is_admin(&state.pool, principal).await;
            let authed = if admin {
                Ok(())
            } else {
                authorize(&state.pool, principal, db).await
            };
            match authed {
                Ok(()) => {
                    if let RateDecision::Denied { retry_after_secs } =
                        evaluate(state, principal, db).await
                    {
                        let _ = out_tx.send(ServerMessage::SubscribeErr {
                            query_id,
                            error: RtDbError::rate_limited(retry_after_secs),
                        });
                        return false;
                    }
                    // Admins subscribe with a bypass ctx (user_id None — they
                    // see every row); everyone else is scoped to their own
                    // identity via `principal.row_ctx()`.
                    let principal_ctx = if admin {
                        PrincipalCtx::bypass()
                    } else {
                        principal.row_ctx()
                    };
                    // Time the initial-query arm: committers.subscribe().await
                    // resolves after the initial query has run, its result been
                    // sent on `tx`, and the subscription registered (see
                    // handle_subscribe). That covers the full first-eval cost
                    // (channel + committer queue + execute_query + send +
                    // register); subsequent push-on-change re-runs are NOT
                    // included (they are fan_out work, not the initial query).
                    let t = Instant::now();
                    let sub_result = state
                        .realtime
                        .committers
                        .subscribe(
                            db,
                            conn_id,
                            query_id.clone(),
                            *query,
                            out_tx.clone(),
                            principal_ctx,
                        )
                        .await;
                    let elapsed_us = t.elapsed().as_micros() as u64;
                    match sub_result {
                        Ok(()) => {
                            state.runtime.metrics.record_subscribe_duration(elapsed_us);
                            state.runtime.metrics.record_query();
                        }
                        Err(error) => {
                            let _ = out_tx.send(ServerMessage::SubscribeErr { query_id, error });
                        }
                    }
                }
                Err(error) => {
                    let _ = out_tx.send(ServerMessage::SubscribeErr { query_id, error });
                }
            }
            false
        }
        ClientMessage::Unsubscribe { query_id } => {
            state.realtime.subs.remove(db, conn_id, &query_id).await;
            false
        }
        ClientMessage::Mutate {
            mut_id,
            idempotency_key,
            txn,
        } => {
            // SEC-004: re-justify the admin bypass on every op (see Subscribe).
            let admin = is_admin(&state.pool, principal).await;
            let authed = if admin {
                Ok(())
            } else {
                authorize(&state.pool, principal, db).await
            };
            match authed {
                Ok(()) => {
                    if let RateDecision::Denied { retry_after_secs } =
                        evaluate(state, principal, db).await
                    {
                        let _ = out_tx.send(ServerMessage::MutateErr {
                            mut_id,
                            error: RtDbError::rate_limited(retry_after_secs),
                        });
                        return false;
                    }
                    // Same admin guardrail as the HTTP data-browser path: reject
                    // an over-cap mutation before it reaches the committer.
                    let cap = state.config.max_affected_docs;
                    if admin && txn.steps.len() > cap {
                        let _ = out_tx.send(ServerMessage::MutateErr {
                            mut_id,
                            error: RtDbError::bad_request(format!(
                                "mutation has {} step(s), exceeding the limit of {cap}",
                                txn.steps.len()
                            )),
                        });
                        return false;
                    }
                    let principal_ctx = if admin {
                        PrincipalCtx::bypass()
                    } else {
                        principal.row_ctx()
                    };
                    match state
                        .realtime
                        .committers
                        .mutate(db, idempotency_key, txn, principal_ctx)
                        .await
                    {
                        Ok(outcome) => {
                            state.runtime.metrics.record_mutation();
                            let _ = out_tx.send(ServerMessage::MutateOk {
                                mut_id,
                                results: outcome.results,
                            });
                        }
                        Err(error) => {
                            let _ = out_tx.send(ServerMessage::MutateErr { mut_id, error });
                        }
                    }
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
        ClientMessage::Schedule {
            schedule_id,
            when,
            txn,
        } => {
            let reply = match authorize(&state.pool, principal, db).await {
                Ok(()) => match scheduler::resolve_when(when, now_ms()) {
                    Ok((kind, due_at, cron)) => {
                        match scheduler::insert(
                            &state.pool,
                            db,
                            kind,
                            due_at,
                            &txn,
                            cron.as_deref(),
                        )
                        .await
                        {
                            Ok(id) => ServerMessage::ScheduleOk { schedule_id, id },
                            Err(error) => ServerMessage::ScheduleErr { schedule_id, error },
                        }
                    }
                    Err(error) => ServerMessage::ScheduleErr { schedule_id, error },
                },
                Err(error) => ServerMessage::ScheduleErr { schedule_id, error },
            };
            let _ = out_tx.send(reply);
            false
        }
        ClientMessage::CancelSchedule { schedule_id, id } => {
            run_simple_schedule(
                state,
                principal,
                db,
                out_tx,
                schedule_id,
                scheduler::cancel(&state.pool, db, &id),
            )
            .await
        }
        ClientMessage::PauseSchedule { schedule_id, id } => {
            run_simple_schedule(
                state,
                principal,
                db,
                out_tx,
                schedule_id,
                scheduler::set_paused(&state.pool, db, &id, true),
            )
            .await
        }
        ClientMessage::ResumeSchedule { schedule_id, id } => {
            run_simple_schedule(
                state,
                principal,
                db,
                out_tx,
                schedule_id,
                scheduler::set_paused(&state.pool, db, &id, false),
            )
            .await
        }
        ClientMessage::ListSchedules { schedule_id } => {
            let reply = match authorize(&state.pool, principal, db).await {
                Ok(()) => match scheduler::list(&state.pool, db).await {
                    Ok(schedules) => ServerMessage::ListSchedulesOk {
                        schedule_id,
                        schedules,
                    },
                    Err(error) => ServerMessage::ScheduleErr { schedule_id, error },
                },
                Err(error) => ServerMessage::ScheduleErr { schedule_id, error },
            };
            let _ = out_tx.send(reply);
            false
        }
    }
}

/// Helper for the three structurally-identical WS schedule arms
/// (CancelSchedule / PauseSchedule / ResumeSchedule): `authorize → call
/// scheduler::{cancel|set_paused} → build ScheduleAck → send` (QA-005). The
/// caller passes the un-awaited scheduler future (constructed at the call
/// site); the helper awaits it only after `authorize` succeeds, matching the
/// original per-arm control flow exactly. The schedule family never carried
/// the admin bypass, so this helper preserves the existing `authorize`-only
/// gate (no `is_admin`); SEC-004's per-op `is_admin` re-run stays in the
/// Subscribe/Mutate arms above and is unaffected by this extraction.
async fn run_simple_schedule<'a>(
    state: &'a AppState,
    principal: &'a Principal,
    db: &'a str,
    out_tx: &'a UnboundedSender<ServerMessage>,
    schedule_id: String,
    action: impl std::future::Future<Output = Result<bool, RtDbError>> + Send + 'a,
) -> bool {
    let (ok, error) = match authorize(&state.pool, principal, db).await {
        Ok(()) => match action.await {
            Ok(ok) => (ok, None),
            Err(error) => (false, Some(error)),
        },
        Err(error) => (false, Some(error)),
    };
    let _ = out_tx.send(ServerMessage::ScheduleAck {
        schedule_id,
        ok,
        error,
    });
    false
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
