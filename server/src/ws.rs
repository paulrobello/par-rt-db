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
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::time::{Instant, interval};

use crate::AppState;
use crate::auth::{
    Principal, PrincipalCtx, authed_user, authorize, is_admin, resolve_bearer, session_still_valid,
};
use crate::db::now_ms;
use crate::error::{ErrorCode, RtDbError};
use crate::protocol::{ClientMessage, ScheduleWhen, ServerMessage, WorkflowSpec, WorkflowStatus};
use crate::rate_limit::{RateDecision, evaluate};
use crate::scheduler;
use crate::subs::{ConnId, next_conn_id};
use crate::txn::{authorize_spec_tables, authorize_txn_tables};
use crate::workflows;

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
    // SEC-105: CORS does not apply to a WS handshake, so a cookie-authenticated
    // upgrade from a disallowed (e.g. cross-origin-XSS) sibling host would
    // otherwise be admitted. Browsers always send `Origin`; a present value
    // must match the live allowlist or `public_url`. Absent `Origin` is a
    // non-browser client and the post-upgrade Auth frame still authenticates.
    if !crate::origin_allowed(&headers, &state.runtime.hot, &state.config.public_url) {
        return RtDbError::forbidden("websocket origin not allowed").into_response();
    }
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
                        let fctx = FrameCtx {
                            state: &state,
                            principal: &principal,
                            db: &db,
                            conn_id,
                            out_tx: &out_tx,
                        };
                        let should_close =
                            handle_text_frame(&mut socket, &fctx, &mut rate_limiter, &text).await;
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
    state.realtime.presence.remove_conn(&db, conn_id).await;
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

/// Shared, borrow-only context for the per-frame `handle_*` handlers that
/// [`handle_text_frame`] dispatches to (QA-107). Bundles the per-connection
/// reads every interactive frame shares — the app state, the authenticated
/// principal, the database name, the connection id, and the outbound channel
/// — so each handler signature stays flat. The socket and connection-level
/// rate limiter stay in `handle_text_frame`'s preamble (handlers reply over
/// the outbound channel, never via direct socket writes; the conn-level
/// limiter is a flood valve checked once per frame before dispatch). Mirrors
/// the `StepCtx`/`SearchCtx` pattern (QA-105) and the `CommitterCtx`
/// precedent (ARC-002); handlers take `&FrameCtx` and copy out the shared
/// reference fields at the top of the body.
struct FrameCtx<'a> {
    state: &'a Arc<AppState>,
    principal: &'a Principal,
    db: &'a str,
    conn_id: ConnId,
    out_tx: &'a UnboundedSender<ServerMessage>,
}

/// Validates and dispatches one post-auth text frame. Returns whether the
/// connection should now close (frame too large, connection-level rate limit
/// exceeded, malformed JSON, or an out-of-order `Auth`); every other message
/// is handled and the connection stays open.
///
/// The frame-size and per-connection rate-limit checks live here in the
/// preamble (no handler needs them); the per-arm work is delegated to one
/// `handle_*` per frame variant (the pattern `committer.rs` uses for its
/// committer-request arms), keeping the dispatcher a thin match. The
/// SEC-004 "re-run `authorize` (and `is_admin`) on every Subscribe and
/// Mutate" invariant is made structural: those arms — and `Presence`, which
/// carries the same admin-bypass re-justification — all funnel through
/// [`authorize_op`] (the single authorization seam) rather than each
/// copy-pasting the `is_admin → session_still_valid | authorize` block. The
/// schedule family (`Schedule`, `CancelSchedule`, `PauseSchedule`,
/// `ResumeSchedule`, `ListSchedules`) never carried the admin bypass and
/// keeps its `authorize`-only gate. `is_admin` fails safe (false on DB
/// error), so a transient Postgres outage withholds the bypass, never widens
/// it.
async fn handle_text_frame(
    socket: &mut WebSocket,
    fctx: &FrameCtx<'_>,
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
            handle_subscribe(fctx, query_id, query).await
        }
        ClientMessage::Unsubscribe { query_id } => {
            fctx.state
                .realtime
                .subs
                .remove(fctx.db, fctx.conn_id, &query_id)
                .await;
            false
        }
        ClientMessage::Mutate {
            mut_id,
            idempotency_key,
            txn,
        } => handle_mutate(fctx, mut_id, idempotency_key, txn).await,
        ClientMessage::Ping => {
            let _ = fctx.out_tx.send(ServerMessage::Pong);
            false
        }
        ClientMessage::Schedule {
            schedule_id,
            when,
            txn,
        } => handle_schedule(fctx, schedule_id, when, txn).await,
        ClientMessage::CancelSchedule { schedule_id, id } => {
            run_simple_schedule(
                fctx.state,
                fctx.principal,
                fctx.db,
                fctx.out_tx,
                schedule_id,
                scheduler::cancel(&fctx.state.pool, fctx.db, &id),
            )
            .await
        }
        ClientMessage::PauseSchedule { schedule_id, id } => {
            run_simple_schedule(
                fctx.state,
                fctx.principal,
                fctx.db,
                fctx.out_tx,
                schedule_id,
                scheduler::set_paused(&fctx.state.pool, fctx.db, &id, true),
            )
            .await
        }
        ClientMessage::ResumeSchedule { schedule_id, id } => {
            run_simple_schedule(
                fctx.state,
                fctx.principal,
                fctx.db,
                fctx.out_tx,
                schedule_id,
                scheduler::set_paused(&fctx.state.pool, fctx.db, &id, false),
            )
            .await
        }
        ClientMessage::ListSchedules { schedule_id } => {
            handle_list_schedules(fctx, schedule_id).await
        }
        ClientMessage::StartWorkflow { workflow_id, spec } => {
            handle_start_workflow(fctx, workflow_id, spec).await
        }
        ClientMessage::CancelWorkflow { workflow_id, id } => {
            handle_cancel_workflow(fctx, workflow_id, id).await
        }
        // Wire type landed ahead of the delivering handler (Task 3): ack
        // failure so an early client gets a clean reply, never silence.
        ClientMessage::SignalWorkflow { workflow_id, .. } => {
            let _ = fctx.out_tx.send(ServerMessage::WorkflowAck {
                workflow_id,
                ok: false,
                error: Some(RtDbError::bad_request("signalWorkflow not yet implemented")),
            });
            false
        }
        ClientMessage::ListWorkflows {
            workflow_id,
            status,
        } => handle_list_workflows(fctx, workflow_id, status).await,
        ClientMessage::Presence {
            room,
            state: presence_state,
        } => handle_presence(fctx, room, presence_state).await,
        ClientMessage::PresenceState {
            room,
            state: presence_state,
            ttl_ms,
        } => handle_presence_state(fctx, room, presence_state, ttl_ms).await,
        ClientMessage::LeavePresence { room } => {
            fctx.state
                .realtime
                .presence
                .leave(fctx.db, fctx.conn_id, &room)
                .await;
            false
        }
    }
}

/// SEC-004 per-op authorization guard — the single authorization seam for
/// interactive WS frames. Re-justifies the admin bypass against the live
/// `rtdb_auth.admins` allowlist (`is_admin`, fails safe to "not admin" on DB
/// error) and, depending on the branch, either checks the admin session is
/// still live (`session_still_valid`) or runs the per-db `authorize`.
/// Returns the per-row principal context to use for the op plus whether the
/// principal is currently a server-wide admin (the admin flag governs the
/// `max_affected_docs` guardrail in [`handle_mutate`]). Subscribe, Mutate,
/// and Presence all funnel through here so the "re-run `authorize` on every
/// Subscribe and Mutate" invariant is structural — there is one function to
/// audit, not three copy-pasted blocks. A principal removed from
/// `rtdb_auth.admins` mid-connection loses the bypass on the next op;
/// `is_admin` returns false on DB error so a transient Postgres outage
/// withholds the bypass rather than widening it.
async fn authorize_op(
    pool: &sqlx::PgPool,
    principal: &Principal,
    db: &str,
) -> Result<(PrincipalCtx, bool), RtDbError> {
    let admin = is_admin(pool, principal).await;
    if admin {
        session_still_valid(pool, principal).await?;
        Ok((PrincipalCtx::bypass(), true))
    } else {
        authorize(pool, principal, db).await?;
        Ok((principal.row_ctx(), false))
    }
}

/// Per-op token/db RPM check (SEC-003). Returns `Ok(())` or the
/// `retry_after_secs` on denial. [`handle_subscribe`] and [`handle_mutate`]
/// gate on this after [`authorize_op`]; the schedule and presence families
/// are not RPM-limited (matching the original per-arm behavior).
async fn check_rate_limit(state: &AppState, principal: &Principal, db: &str) -> Result<(), u32> {
    if let RateDecision::Denied { retry_after_secs } = evaluate(state, principal, db).await {
        Err(retry_after_secs)
    } else {
        Ok(())
    }
}

/// `Subscribe` arm: re-run [`authorize_op`] (SEC-004), check the RPM limiter,
/// then register the subscription via the committer. The initial-query
/// duration is timed end to end (channel enqueue → committer queue →
/// `execute_query` → send initial result → register); subsequent
/// push-on-change re-runs are `fan_out` work and not included.
async fn handle_subscribe(
    fctx: &FrameCtx<'_>,
    query_id: String,
    query: Box<crate::query::Query>,
) -> bool {
    let state = fctx.state;
    let principal = fctx.principal;
    let db = fctx.db;
    let conn_id = fctx.conn_id;
    let out_tx = fctx.out_tx;

    match authorize_op(&state.pool, principal, db).await {
        Ok((principal_ctx, _)) => {
            if let Err(retry_after_secs) = check_rate_limit(state, principal, db).await {
                let _ = out_tx.send(ServerMessage::SubscribeErr {
                    query_id,
                    error: RtDbError::rate_limited(retry_after_secs),
                });
                return false;
            }
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

/// `Mutate` arm: re-run [`authorize_op`] (SEC-004), reject read-only tokens,
/// check the RPM limiter, then enforce the admin `max_affected_docs`
/// guardrail before dispatching to the committer. The guardrail rejects an
/// over-budget mutation before it reaches the serialized committer turn —
/// the bound is worst-case affected documents, not raw step count (a
/// by-query step can touch many rows; SEC-104).
async fn handle_mutate(
    fctx: &FrameCtx<'_>,
    mut_id: String,
    idempotency_key: Option<String>,
    txn: crate::txn::Transaction,
) -> bool {
    let state = fctx.state;
    let principal = fctx.principal;
    let db = fctx.db;
    let out_tx = fctx.out_tx;

    match authorize_op(&state.pool, principal, db).await {
        Ok((principal_ctx, admin)) => {
            if principal.is_read_only() {
                let _ = out_tx.send(ServerMessage::MutateErr {
                    mut_id,
                    error: RtDbError::forbidden("read-only token cannot mutate"),
                });
                return false;
            }
            if let Err(retry_after_secs) = check_rate_limit(state, principal, db).await {
                let _ = out_tx.send(ServerMessage::MutateErr {
                    mut_id,
                    error: RtDbError::rate_limited(retry_after_secs),
                });
                return false;
            }
            let cap = state.config.max_affected_docs;
            let worst = crate::txn::worst_case_affected(&txn);
            if admin && worst > cap {
                let _ = out_tx.send(ServerMessage::MutateErr {
                    mut_id,
                    error: RtDbError::bad_request(format!(
                        "mutation could affect up to {worst} document(s), exceeding the limit of {cap}"
                    )),
                });
                return false;
            }
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

/// `Schedule` arm: `authorize`-only gate (the schedule family never carried
/// the admin bypass), reject read-only tokens, table-scope-check the txn
/// recursively at enqueue (FM-28 — fire time runs as bypass, so this is the
/// only scoped check), resolve the schedule timing, then insert the scheduled
/// transaction.
async fn handle_schedule(
    fctx: &FrameCtx<'_>,
    schedule_id: String,
    when: ScheduleWhen,
    txn: crate::txn::Transaction,
) -> bool {
    let state = fctx.state;
    let principal = fctx.principal;
    let db = fctx.db;
    let out_tx = fctx.out_tx;

    let reply = match authorize(&state.pool, principal, db).await {
        Ok(()) if principal.is_read_only() => ServerMessage::ScheduleErr {
            schedule_id,
            error: RtDbError::forbidden("read-only token cannot mutate"),
        },
        Ok(()) => {
            let prepared = authorize_txn_tables(&principal.row_ctx(), &txn)
                .and_then(|()| scheduler::resolve_when(when, now_ms()));
            match prepared {
                Ok((kind, due_at, cron, every_ms)) => {
                    // Fire time runs on the per-db scheduler, which only
                    // exists once the per-db tasks spawn — ensure that (and
                    // the table inline: the spawned scheduler's startup
                    // ensure is not ordered against this insert) or a job
                    // scheduled on a cold db sits pending forever.
                    let spawned = match state.realtime.committers.ensure_spawned(db).await {
                        Ok(()) => scheduler::ensure_table(&state.pool, db).await,
                        Err(error) => Err(error),
                    };
                    match spawned {
                        Ok(()) => {
                            match scheduler::insert(
                                &state.pool,
                                db,
                                kind,
                                due_at,
                                &txn,
                                cron.as_deref(),
                                every_ms,
                            )
                            .await
                            {
                                Ok(id) => ServerMessage::ScheduleOk { schedule_id, id },
                                Err(error) => ServerMessage::ScheduleErr { schedule_id, error },
                            }
                        }
                        Err(error) => ServerMessage::ScheduleErr { schedule_id, error },
                    }
                }
                Err(error) => ServerMessage::ScheduleErr { schedule_id, error },
            }
        }
        Err(error) => ServerMessage::ScheduleErr { schedule_id, error },
    };
    let _ = out_tx.send(reply);
    false
}

/// `ListSchedules` arm: `authorize`-only gate, then list the database's
/// scheduled transactions.
async fn handle_list_schedules(fctx: &FrameCtx<'_>, schedule_id: String) -> bool {
    let state = fctx.state;
    let principal = fctx.principal;
    let db = fctx.db;
    let out_tx = fctx.out_tx;

    let reply = match authorize(&state.pool, principal, db).await {
        Ok(()) => {
            // Cold-db guard: ensure the side table inline (it is ensured only
            // at scheduler startup / db creation) so a cold db lists empty
            // instead of erroring.
            let listed = match scheduler::ensure_table(&state.pool, db).await {
                Ok(()) => scheduler::list(&state.pool, db).await,
                Err(error) => Err(error),
            };
            match listed {
                Ok(schedules) => ServerMessage::ListSchedulesOk {
                    schedule_id,
                    schedules,
                },
                Err(error) => ServerMessage::ScheduleErr { schedule_id, error },
            }
        }
        Err(error) => ServerMessage::ScheduleErr { schedule_id, error },
    };
    let _ = out_tx.send(reply);
    false
}

/// `StartWorkflow` arm: `authorize`-only gate (the schedule family's
/// precedent — workflows never carried the admin bypass), reject read-only
/// tokens, then validate the spec and table-scope-check it recursively at
/// submit (FM-29 — steps fire later as bypass, so this is the only scoped
/// check), insert the run row, and re-read it for the `WorkflowInfo` reply.
async fn handle_start_workflow(
    fctx: &FrameCtx<'_>,
    workflow_id: String,
    spec: WorkflowSpec,
) -> bool {
    let state = fctx.state;
    let principal = fctx.principal;
    let db = fctx.db;
    let out_tx = fctx.out_tx;

    let reply = match authorize(&state.pool, principal, db).await {
        Ok(()) if principal.is_read_only() => ServerMessage::StartWorkflowErr {
            workflow_id,
            error: RtDbError::forbidden("read-only token cannot mutate"),
        },
        Ok(()) => {
            let prepared = workflows::validate_spec(&spec)
                .and_then(|()| authorize_spec_tables(&principal.row_ctx(), &spec));
            match prepared {
                // Steps fire from the per-db scheduler, which only exists once
                // the per-db tasks spawn — ensure that before insert or the
                // run sits `pending` forever on a cold db. The spawned
                // scheduler's own startup ensure is NOT ordered against this
                // insert, so ensure the table inline too or a cold-db insert
                // can lose the race and error.
                Ok(()) => match state.realtime.committers.ensure_spawned(db).await {
                    Ok(()) => match workflows::ensure_table(&state.pool, db).await {
                        Ok(()) => match workflows::insert(&state.pool, db, &spec).await {
                            Ok(id) => match workflows::get(&state.pool, db, &id).await {
                                Ok(Some(full)) => ServerMessage::StartWorkflowOk {
                                    workflow_id,
                                    info: full.info,
                                },
                                _ => ServerMessage::StartWorkflowErr {
                                    workflow_id,
                                    error: RtDbError::internal("workflow started but unreadable"),
                                },
                            },
                            Err(error) => ServerMessage::StartWorkflowErr { workflow_id, error },
                        },
                        Err(error) => ServerMessage::StartWorkflowErr { workflow_id, error },
                    },
                    Err(error) => ServerMessage::StartWorkflowErr { workflow_id, error },
                },
                Err(error) => ServerMessage::StartWorkflowErr { workflow_id, error },
            }
        }
        Err(error) => ServerMessage::StartWorkflowErr { workflow_id, error },
    };
    let _ = out_tx.send(reply);
    false
}

/// `CancelWorkflow` arm: `authorize`-only gate, reject read-only tokens, then
/// flip the run to `cancelled` (`run_simple_schedule`'s ack shape).
async fn handle_cancel_workflow(fctx: &FrameCtx<'_>, workflow_id: String, id: String) -> bool {
    let state = fctx.state;
    let principal = fctx.principal;
    let db = fctx.db;
    let out_tx = fctx.out_tx;

    let (ok, error) = match authorize(&state.pool, principal, db).await {
        Ok(()) if principal.is_read_only() => (
            false,
            Some(RtDbError::forbidden("read-only token cannot mutate")),
        ),
        // Cold-db guard (the table is ensured only at scheduler startup):
        // ensure inline so cancel on a db with no spawned tasks is a clean
        // `ok: false`, not an error.
        Ok(()) => match workflows::ensure_table(&state.pool, db).await {
            Ok(()) => match workflows::cancel(&state.pool, db, &id).await {
                Ok(ok) => (ok, None),
                Err(error) => (false, Some(error)),
            },
            Err(error) => (false, Some(error)),
        },
        Err(error) => (false, Some(error)),
    };
    let _ = out_tx.send(ServerMessage::WorkflowAck {
        workflow_id,
        ok,
        error,
    });
    false
}

/// `ListWorkflows` arm: `authorize`-only gate, then list the database's runs
/// (optional status filter, capped at 100).
async fn handle_list_workflows(
    fctx: &FrameCtx<'_>,
    workflow_id: String,
    status: Option<WorkflowStatus>,
) -> bool {
    let state = fctx.state;
    let principal = fctx.principal;
    let db = fctx.db;
    let out_tx = fctx.out_tx;

    let reply = match authorize(&state.pool, principal, db).await {
        // Cold-db guard (the table is ensured only at scheduler startup):
        // ensure inline so list on a db with no spawned tasks returns an
        // empty page, not an error.
        Ok(()) => match workflows::ensure_table(&state.pool, db).await {
            Ok(()) => match workflows::list(&state.pool, db, status.as_ref(), 100).await {
                Ok(workflows) => ServerMessage::ListWorkflowsOk {
                    workflow_id,
                    workflows,
                },
                Err(error) => ServerMessage::StartWorkflowErr { workflow_id, error },
            },
            Err(error) => ServerMessage::StartWorkflowErr { workflow_id, error },
        },
        Err(error) => ServerMessage::StartWorkflowErr { workflow_id, error },
    };
    let _ = out_tx.send(reply);
    false
}

/// `Presence` JOIN arm: re-runs [`authorize_op`] (SEC-004 parity — revocation
/// takes effect on open connections) and captures `authed_user(principal)`
/// — the load-bearing identity-capture point (the full `Principal` lives
/// only at the WS layer; downstream code sees `PrincipalCtx` with no display
/// identity). Presence is not routed through the committer and is not gated
/// by the token/db RPM limiter; the admin branch still live-checks the
/// session (admins must hold a live session — revocation applies to every
/// interactive principal).
async fn handle_presence(
    fctx: &FrameCtx<'_>,
    room: String,
    presence_state: Option<serde_json::Value>,
) -> bool {
    let state = fctx.state;
    let principal = fctx.principal;
    let db = fctx.db;
    let conn_id = fctx.conn_id;
    let out_tx = fctx.out_tx;

    if let Err(error) = authorize_op(&state.pool, principal, db).await {
        let _ = out_tx.send(ServerMessage::PresenceErr { room, error });
        return false;
    }
    let user = authed_user(principal);
    match state
        .realtime
        .presence
        .join(db, conn_id, &room, presence_state, user, out_tx.clone())
        .await
    {
        Ok(()) => state.runtime.metrics.record_presence_update(),
        Err(error) => {
            let _ = out_tx.send(ServerMessage::PresenceErr { room, error });
        }
    }
    false
}

/// `PresenceState` arm: update cursor/state in an already-joined room. Does
/// NOT re-run `authorize` — membership implies prior auth, and keeping
/// cursor updates off Postgres is a stated design rule (ENH-015).
async fn handle_presence_state(
    fctx: &FrameCtx<'_>,
    room: String,
    presence_state: serde_json::Value,
    ttl_ms: Option<u64>,
) -> bool {
    let state = fctx.state;
    let db = fctx.db;
    let conn_id = fctx.conn_id;
    let out_tx = fctx.out_tx;

    match state
        .realtime
        .presence
        .update_state(db, conn_id, &room, presence_state, ttl_ms)
        .await
    {
        Ok(()) => state.runtime.metrics.record_presence_update(),
        Err(error) => {
            let _ = out_tx.send(ServerMessage::PresenceErr { room, error });
        }
    }
    false
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
        Ok(()) if principal.is_read_only() => (
            false,
            Some(RtDbError::forbidden("read-only token cannot mutate")),
        ),
        // Cold-db guard: ensure the side table inline (it is ensured only at
        // scheduler startup / db creation) so a manage op on a cold db is a
        // clean `ok:false` no-op instead of an error.
        Ok(()) => match scheduler::ensure_table(&state.pool, db).await {
            Ok(()) => match action.await {
                Ok(ok) => (ok, None),
                Err(error) => (false, Some(error)),
            },
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
