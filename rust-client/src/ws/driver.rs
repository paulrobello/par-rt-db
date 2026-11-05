use super::server_msg::{
    apply_server_message, maybe_unsubscribe, reject_all, reject_all_schedules,
    reject_all_workflows, reject_inflight, reject_inflight_schedules, reject_inflight_workflows,
};
use super::state::{
    ClientInner, Cmd, ConnectionState, Liveness, MutReply, PendingQueues, QueuedMutate,
    QueuedSchedule, QueuedWorkflow, SchedReply, ScheduleMsg, WfReply, WorkflowMsg,
};
use crate::query::Query;
use crate::wire::{AuthedUser, ClientMessage, ServerMessage};
use futures_util::{Sink, SinkExt, StreamExt};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{interval, sleep, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message as WsMessage;

/// Server closes the socket if the auth frame does not arrive within this window.
const AUTH_DEADLINE: Duration = Duration::from_secs(15);

/// WS close code the server emits on an auth failure (bad/missing token, revoked
/// authz). Terminal from the client's perspective: do not reconnect with the same
/// credential.
const CLOSE_AUTH_FAILED: u16 = 4401;

// ── Driver task ──────────────────────────────────────────────────────────────

pub(super) struct Driver {
    pub(super) inner: Arc<ClientInner>,
    pub(super) cmd_rx: mpsc::UnboundedReceiver<Cmd>,
}

/// Why a session ended, and what the reconnect loop should do next.
enum SessionOutcome {
    /// Server rejected the credential (`authErr` / close `4401`): no auto-reconnect.
    AuthFailed,
    /// Transient disconnect: reconnect after backoff.
    Reconnect,
    /// `close()` was called or the handle dropped.
    Shutdown,
}

pub(super) async fn drive(mut driver: Driver) {
    let mut attempt: u32 = 0;
    let mut queues = PendingQueues::default();

    loop {
        if driver.inner.closed.load(Ordering::SeqCst) {
            break;
        }
        let epoch = driver.inner.generation.fetch_add(1, Ordering::SeqCst) + 1;

        let token = match resolve_token(&driver.inner, epoch).await {
            TokenResult::Some(t) => t,
            TokenResult::None => {
                // No credential: don't spin a dial loop. Wait for a poke.
                driver.set_state(ConnectionState::Idle);
                if !wait_for_poke(&mut driver).await {
                    break;
                }
                continue;
            }
            TokenResult::Shutdown => break,
        };

        driver.set_state(ConnectionState::Connecting);
        match run_session(&mut driver, epoch, token, &mut queues).await {
            SessionOutcome::AuthFailed => {
                driver.set_state(ConnectionState::Idle);
                reject_all(&driver.inner, &mut queues.mutate, "authentication failed");
                reject_all_schedules(&mut queues.schedule, "authentication failed");
                reject_all_workflows(&mut queues.workflow, "authentication failed");
                if !wait_for_poke(&mut driver).await {
                    break;
                }
            }
            SessionOutcome::Reconnect => {
                driver.set_state(ConnectionState::Reconnecting);
                reject_inflight(
                    &driver.inner,
                    &mut queues.mutate,
                    "connection closed before acknowledgment",
                );
                reject_inflight_schedules(
                    &mut queues.schedule,
                    "connection closed before acknowledgment",
                );
                reject_inflight_workflows(
                    &mut queues.workflow,
                    "connection closed before acknowledgment",
                );
                match backoff_wait(&mut driver, attempt).await {
                    WaitResult::Shutdown => break,
                    WaitResult::Retry => {
                        attempt += 1;
                        continue;
                    }
                }
            }
            SessionOutcome::Shutdown => break,
        }
    }

    reject_all(&driver.inner, &mut queues.mutate, "client is closed");
    reject_all_schedules(&mut queues.schedule, "client is closed");
    reject_all_workflows(&mut queues.workflow, "client is closed");
    driver.set_state(ConnectionState::Closed);
}

enum TokenResult {
    Some(String),
    None,
    Shutdown,
}

/// Resolve the token, aborting if `close()` advanced the generation meanwhile.
async fn resolve_token(inner: &ClientInner, epoch: u64) -> TokenResult {
    let token = (inner.get_token)().await;
    if inner.closed.load(Ordering::SeqCst) || inner.generation.load(Ordering::SeqCst) != epoch {
        return TokenResult::Shutdown;
    }
    match token {
        Some(t) => TokenResult::Some(t),
        None => TokenResult::None,
    }
}

/// Block while unauthenticated, returning `true` if a caller poked (re-attempt)
/// or `false` if the driver should exit.
async fn wait_for_poke(driver: &mut Driver) -> bool {
    // A single poke is enough: either a caller nudged us (re-attempt) or the
    // channel closed / shutdown fired (exit).
    match driver.cmd_rx.recv().await {
        Some(Cmd::Shutdown) | None => false,
        Some(_) => true,
    }
}

enum WaitResult {
    Retry,
    Shutdown,
}

/// Sleep for the jittered backoff, aborting on shutdown or a caller poke.
async fn backoff_wait(driver: &mut Driver, attempt: u32) -> WaitResult {
    let delay = backoff_delay(
        driver.inner.config.backoff_base,
        driver.inner.config.backoff_max,
        attempt,
    );
    tokio::select! {
        _ = sleep(delay) => WaitResult::Retry,
        cmd = driver.cmd_rx.recv() => match cmd {
            Some(Cmd::Shutdown) | None => WaitResult::Shutdown,
            Some(_) => WaitResult::Retry,
        },
    }
}

/// Open, authenticate, and run one session until it ends. Owns the heartbeat and
/// routes every inbound [`ServerMessage`] through [`apply_server_message`].
async fn run_session(
    driver: &mut Driver,
    epoch: u64,
    token: String,
    queues: &mut PendingQueues,
) -> SessionOutcome {
    if driver.inner.closed.load(Ordering::SeqCst)
        || driver.inner.generation.load(Ordering::SeqCst) != epoch
    {
        return SessionOutcome::Shutdown;
    }

    let url = format!("{}/sync", driver.inner.url);
    let (mut sink, mut stream) = match timeout(Duration::from_secs(15), connect_async(&url)).await {
        Ok(Ok((stream, _response))) => stream.split(),
        _ => return SessionOutcome::Reconnect,
    };

    // Auth must be the first frame.
    let auth = ClientMessage::Auth {
        token: Some(token),
        db: driver.inner.db.clone(),
        protocol_version: Some(crate::wire::PROTOCOL_VERSION),
    };
    let frame = serde_json::to_string(&auth).unwrap_or_default();
    if sink.send(WsMessage::Text(frame.into())).await.is_err() {
        return SessionOutcome::Reconnect;
    }

    // Await authOk (server closes within AUTH_DEADLINE on no-show).
    let handshake = timeout(AUTH_DEADLINE, async {
        loop {
            match stream.next().await {
                Some(Ok(WsMessage::Text(t))) => {
                    match serde_json::from_str::<ServerMessage>(t.as_str()) {
                        Ok(ServerMessage::AuthOk { user, .. }) => return Ok(user),
                        Ok(ServerMessage::AuthErr { .. }) => return Err(AuthFail::Failed),
                        Ok(_) => return Err(AuthFail::Reconnect),
                        Err(_) => continue,
                    }
                }
                Some(Ok(WsMessage::Ping(p))) => {
                    if sink.send(WsMessage::Pong(p)).await.is_err() {
                        return Err(AuthFail::Reconnect);
                    }
                }
                Some(Ok(WsMessage::Close(f))) => {
                    return Err(if f.map(|c| u16::from(c.code)) == Some(CLOSE_AUTH_FAILED) {
                        AuthFail::Failed
                    } else {
                        AuthFail::Reconnect
                    });
                }
                _ => return Err(AuthFail::Reconnect),
            }
        }
    })
    .await;
    let user = match handshake {
        Ok(Ok(user)) => user,
        Ok(Err(AuthFail::Failed)) => return SessionOutcome::AuthFailed,
        Ok(Err(AuthFail::Reconnect)) | Err(_) => return SessionOutcome::Reconnect,
    };

    driver.set_user(Some(user));
    driver.set_state(ConnectionState::Connected);

    // Re-establish active subscriptions. Snapshot under the lock, send outside it.
    let mut sent_subs: HashSet<String> = HashSet::new();
    let subs_snapshot: Vec<(String, Query)> = {
        let maps = driver.inner.subs.lock().unwrap_or_else(|p| p.into_inner());
        maps.by_key
            .values()
            .map(|s| (s.query_id.clone(), s.query.clone()))
            .collect()
    };
    for (query_id, query) in &subs_snapshot {
        sent_subs.insert(query_id.clone());
        let frame = ClientMessage::Subscribe {
            query_id: query_id.clone(),
            query: Box::new(query.clone()),
        };
        if send_text(&mut sink, &frame).await.is_err() {
            return SessionOutcome::Reconnect;
        }
    }

    // Replay joined presence rooms, mirroring subscription replay above. Each
    // (re)connect re-joins with the latest cached state — the server lost this
    // connection's presence on disconnect, so a fresh `presence` frame is
    // required (not `presenceState`).
    let mut sent_rooms: HashSet<String> = HashSet::new();
    let rooms_snapshot: Vec<(String, Option<serde_json::Value>)> = {
        let maps = driver
            .inner
            .presence
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        maps.by_room
            .values()
            .map(|s| {
                (
                    s.room.clone(),
                    s.state.lock().unwrap_or_else(|p| p.into_inner()).clone(),
                )
            })
            .collect()
    };
    for (room, state) in &rooms_snapshot {
        sent_rooms.insert(room.clone());
        // A fresh session re-joins every room, so the server will open with
        // a full snapshot; any delta sequence from the previous session is
        // void — reset the gap-detector cursor with it.
        {
            let maps = driver
                .inner
                .presence
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if let Some(st) = maps.by_room.get(room) {
                *st.last_seq.lock().unwrap_or_else(|p| p.into_inner()) = None;
            }
        }
        let frame = ClientMessage::Presence {
            room: room.clone(),
            state: state.clone(),
        };
        if send_text(&mut sink, &frame).await.is_err() {
            return SessionOutcome::Reconnect;
        }
    }

    // Replay open change feeds from their cursors: the server lost this
    // connection's feeds on disconnect, and the cursor makes the replay resume
    // exactly after the last delivered op.
    let feed_frames = driver
        .inner
        .feeds
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .replay_frames();
    for frame in &feed_frames {
        if send_text(&mut sink, frame).await.is_err() {
            return SessionOutcome::Reconnect;
        }
    }

    // Flush mutations queued while disconnected.
    while let Some(q) = queues.mutate.unsent.pop_front() {
        match deliver_mutate(&mut sink, q, &mut queues.mutate.by_id).await {
            Deliver::Sent => {}
            Deliver::Reconnect(q) => {
                queues.mutate.unsent.push_back(q);
                return SessionOutcome::Reconnect;
            }
        }
    }

    // Flush schedule/list/manage calls queued while disconnected.
    while let Some(q) = queues.schedule.unsent.pop_front() {
        match deliver_schedule(&mut sink, q, &mut queues.schedule.by_id).await {
            Ok(()) => {}
            Err(q) => {
                queues.schedule.unsent.push_back(q);
                return SessionOutcome::Reconnect;
            }
        }
    }

    // Flush workflow calls queued while disconnected.
    while let Some(q) = queues.workflow.unsent.pop_front() {
        match deliver_workflow(&mut sink, q, &mut queues.workflow.by_id).await {
            Ok(()) => {}
            Err(q) => {
                queues.workflow.unsent.push_back(q);
                return SessionOutcome::Reconnect;
            }
        }
    }

    let mut liveness = Liveness::new(driver.inner.config.heartbeat);
    let mut ticker = interval(driver.inner.config.heartbeat);
    ticker.tick().await; // skip the immediate tick

    loop {
        tokio::select! {
            biased;
            cmd = driver.cmd_rx.recv() => match cmd {
                Some(Cmd::Shutdown) | None => {
                    let _ = sink.close().await;
                    return SessionOutcome::Shutdown;
                }
                Some(Cmd::Wake) => {}
                Some(Cmd::Subscribe { query_id, query }) => {
                    if sent_subs.insert(query_id.clone()) {
                        let frame = ClientMessage::Subscribe { query_id, query };
                        if send_text(&mut sink, &frame).await.is_err() {
                            return SessionOutcome::Reconnect;
                        }
                    }
                }
                Some(Cmd::Unsubscribe { query_id }) => {
                    sent_subs.remove(&query_id);
                    maybe_unsubscribe(&driver.inner, &query_id);
                    let frame = ClientMessage::Unsubscribe { query_id };
                    if send_text(&mut sink, &frame).await.is_err() {
                        return SessionOutcome::Reconnect;
                    }
                }
                Some(Cmd::Mutate(q)) => {
                    match deliver_mutate(&mut sink, *q, &mut queues.mutate.by_id).await {
                        Deliver::Sent => {}
                        Deliver::Reconnect(q) => {
                            queues.mutate.unsent.push_back(q);
                            return SessionOutcome::Reconnect;
                        }
                    }
                }
                Some(Cmd::Schedule(q)) => {
                    match deliver_schedule(&mut sink, *q, &mut queues.schedule.by_id).await {
                        Ok(()) => {}
                        Err(q) => {
                            queues.schedule.unsent.push_back(q);
                            return SessionOutcome::Reconnect;
                        }
                    }
                }
                Some(Cmd::Workflow(q)) => {
                    match deliver_workflow(&mut sink, *q, &mut queues.workflow.by_id).await {
                        Ok(()) => {}
                        Err(q) => {
                            queues.workflow.unsent.push_back(q);
                            return SessionOutcome::Reconnect;
                        }
                    }
                }
                Some(Cmd::FeedOpen { feed_id }) => {
                    // A feed opened mid-session; the session-start replay
                    // covers feeds that already existed.
                    let frame = driver
                        .inner
                        .feeds
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .frame_for(&feed_id);
                    if let Some(frame) = frame
                        && !feed_frames.iter().any(|f| matches!(
                            (f, &frame),
                            (
                                ClientMessage::FeedSubscribe { feed_id: a, .. },
                                ClientMessage::FeedSubscribe { feed_id: b, .. },
                            ) if a == b
                        ))
                        && send_text(&mut sink, &frame).await.is_err()
                    {
                        return SessionOutcome::Reconnect;
                    }
                }
                Some(Cmd::FeedClose { feed_id }) => {
                    driver
                        .inner
                        .feeds
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .remove(&feed_id);
                    let frame = ClientMessage::FeedUnsubscribe { feed_id };
                    if send_text(&mut sink, &frame).await.is_err() {
                        return SessionOutcome::Reconnect;
                    }
                }
                Some(Cmd::FeedAck { feed_id, seq }) => {
                    let frame = ClientMessage::FeedAck { feed_id, seq };
                    if send_text(&mut sink, &frame).await.is_err() {
                        return SessionOutcome::Reconnect;
                    }
                }
                Some(Cmd::PresenceJoin { room, state }) => {
                    // Dedup within the session: the replay-at-session-start loop
                    // may have already sent this room's join.
                    if sent_rooms.insert(room.clone()) {
                        let frame = ClientMessage::Presence { room, state };
                        if send_text(&mut sink, &frame).await.is_err() {
                            return SessionOutcome::Reconnect;
                        }
                    }
                }
                Some(Cmd::PresenceUpdate { room, state, ttl_ms }) => {
                    // Only relay if the room is joined on this session (it may
                    // have been left while this cmd was queued, or the connection
                    // dropped and the room was not re-joined).
                    if sent_rooms.contains(&room) {
                        let frame = ClientMessage::PresenceState { room, state, ttl_ms };
                        if send_text(&mut sink, &frame).await.is_err() {
                            return SessionOutcome::Reconnect;
                        }
                    }
                }
                Some(Cmd::PresenceLeave { room }) => {
                    // Only send leavePresence if this room was joined on this
                    // session (via replay or a PresenceJoin cmd). A leave queued
                    // while disconnected fires on the next session, but the room
                    // is no longer in the map → no replay joined it →
                    // `sent_rooms` doesn't contain it → skip, matching the TS
                    // client's `if (authenticated) send(...)` gate.
                    if sent_rooms.remove(&room) {
                        let frame = ClientMessage::LeavePresence { room };
                        if send_text(&mut sink, &frame).await.is_err() {
                            return SessionOutcome::Reconnect;
                        }
                    }
                }
                Some(Cmd::PresenceResync { room }) => {
                    // Delta seq gap: unconditionally re-send the join so the
                    // server flips the room's caught-up flag and the next
                    // flush delivers a full snapshot. Deliberately NOT gated
                    // on `sent_rooms` — the room IS in the set; replacing the
                    // join is the resync.
                    let cached = {
                        let maps = driver
                            .inner
                            .presence
                            .lock()
                            .unwrap_or_else(|p| p.into_inner());
                        maps.by_room.get(&room).and_then(|s| {
                            s.state
                                .lock()
                                .unwrap_or_else(|p| p.into_inner())
                                .clone()
                        })
                    };
                    let frame = ClientMessage::Presence {
                        room,
                        state: cached,
                    };
                    if send_text(&mut sink, &frame).await.is_err() {
                        return SessionOutcome::Reconnect;
                    }
                }
            },
            incoming = stream.next() => match incoming {
                Some(Ok(WsMessage::Text(t))) => {
                    if let Ok(msg) = serde_json::from_str::<ServerMessage>(t.as_str()) {
                        match msg {
                            // The heartbeat is an app-level `{type:"ping"}` TEXT
                            // frame answered by `{type:"pong"}`; neither side
                            // sends protocol-level pings, so this is the only
                            // path that refreshes `Liveness` on a healthy link.
                            ServerMessage::Pong => liveness.note_pong(),
                            msg => apply_server_message(&driver.inner, msg, queues),
                        }
                    }
                }
                Some(Ok(WsMessage::Ping(p))) => {
                    if sink.send(WsMessage::Pong(p)).await.is_err() {
                        return SessionOutcome::Reconnect;
                    }
                }
                Some(Ok(WsMessage::Pong(_))) => liveness.note_pong(),
                Some(Ok(WsMessage::Close(f))) => {
                    return if f.map(|c| u16::from(c.code)) == Some(CLOSE_AUTH_FAILED) {
                        SessionOutcome::AuthFailed
                    } else {
                        SessionOutcome::Reconnect
                    };
                }
                Some(Ok(_)) => {}
                Some(Err(_)) | None => return SessionOutcome::Reconnect,
            },
            _ = ticker.tick() => {
                if liveness.timed_out() {
                    let _ = sink.send(WsMessage::Close(None)).await;
                    return SessionOutcome::Reconnect;
                }
                let _ = send_text(&mut sink, &ClientMessage::Ping).await;
            }
        }
    }
}

/// How an auth-handshake attempt failed.
enum AuthFail {
    /// Server rejected the credential (`authErr` / close `4401`): terminal.
    Failed,
    /// Transient: timeout, unexpected frame, or socket error.
    Reconnect,
}

enum Deliver {
    Sent,
    Reconnect(QueuedMutate),
}

/// Serialize + send a mutate frame, registering its reply on success.
async fn deliver_mutate<S>(
    sink: &mut S,
    q: QueuedMutate,
    pending: &mut HashMap<String, MutReply>,
) -> Deliver
where
    S: Sink<WsMessage> + Unpin,
{
    let frame = ClientMessage::Mutate {
        mut_id: q.mut_id.clone(),
        idempotency_key: q.idempotency_key.clone(),
        txn: q.txn.clone(),
    };
    if send_text(sink, &frame).await.is_err() {
        return Deliver::Reconnect(q);
    }
    pending.insert(q.mut_id, q.reply);
    Deliver::Sent
}

/// Serialize + send a schedule/list/manage frame, registering its reply on
/// success. `Err(q)` means the send failed mid-session: the caller re-queues
/// `q` so it fires on the next auth (same contract as [`deliver_mutate`]'s
/// `Deliver::Reconnect`).
async fn deliver_schedule<S>(
    sink: &mut S,
    q: QueuedSchedule,
    pending: &mut HashMap<String, SchedReply>,
) -> Result<(), QueuedSchedule>
where
    S: Sink<WsMessage> + Unpin,
{
    let frame = match &q.msg {
        ScheduleMsg::Schedule {
            when,
            txn,
            external,
        } => ClientMessage::Schedule {
            schedule_id: q.schedule_id.clone(),
            when: when.clone(),
            txn: txn.clone(),
            // `true` opts the job out of internal execution: an application
            // worker claims it via the HTTP claim route. Omitted on the wire
            // when false.
            external: if *external { Some(true) } else { None },
        },
        ScheduleMsg::Cancel { id } => ClientMessage::CancelSchedule {
            schedule_id: q.schedule_id.clone(),
            id: id.clone(),
        },
        ScheduleMsg::Pause { id } => ClientMessage::PauseSchedule {
            schedule_id: q.schedule_id.clone(),
            id: id.clone(),
        },
        ScheduleMsg::Resume { id } => ClientMessage::ResumeSchedule {
            schedule_id: q.schedule_id.clone(),
            id: id.clone(),
        },
        ScheduleMsg::List => ClientMessage::ListSchedules {
            schedule_id: q.schedule_id.clone(),
        },
    };
    if send_text(sink, &frame).await.is_err() {
        return Err(q);
    }
    pending.insert(q.schedule_id, q.reply);
    Ok(())
}

/// Serialize + send a workflow frame, registering its reply on success.
/// `Err(q)` means the send failed mid-session: the caller re-queues `q` so it
/// fires on the next auth (same contract as [`deliver_schedule`]).
async fn deliver_workflow<S>(
    sink: &mut S,
    q: QueuedWorkflow,
    pending: &mut HashMap<String, WfReply>,
) -> Result<(), QueuedWorkflow>
where
    S: Sink<WsMessage> + Unpin,
{
    let frame = match &q.msg {
        WorkflowMsg::Start { spec } => ClientMessage::StartWorkflow {
            workflow_id: q.workflow_id.clone(),
            spec: spec.clone(),
        },
        WorkflowMsg::Cancel { id } => ClientMessage::CancelWorkflow {
            workflow_id: q.workflow_id.clone(),
            id: id.clone(),
        },
        WorkflowMsg::Signal { id, name, payload } => ClientMessage::SignalWorkflow {
            workflow_id: q.workflow_id.clone(),
            id: id.clone(),
            name: name.clone(),
            payload: payload.clone(),
        },
        WorkflowMsg::List { status } => ClientMessage::ListWorkflows {
            workflow_id: q.workflow_id.clone(),
            status: *status,
        },
    };
    if send_text(sink, &frame).await.is_err() {
        return Err(q);
    }
    pending.insert(q.workflow_id, q.reply);
    Ok(())
}

/// Serialize a client message and send it as a text frame. Generic over the sink
/// so the concrete tungstenite type is never named (it would pull
/// `tokio::net::TcpStream`, needing the `net` feature).
async fn send_text<S>(sink: &mut S, msg: &ClientMessage) -> Result<(), ()>
where
    S: Sink<WsMessage> + Unpin,
{
    let text = serde_json::to_string(msg).map_err(|_| ())?;
    sink.send(WsMessage::Text(text.into()))
        .await
        .map_err(|_| ())
}

// ── helpers ──────────────────────────────────────────────────────────────────

impl Driver {
    fn set_state(&self, state: ConnectionState) {
        let mut status = self.inner.status_tx.borrow().clone();
        status.state = state;
        if matches!(state, ConnectionState::Idle | ConnectionState::Closed) {
            status.user = None;
        }
        let _ = self.inner.status_tx.send(status);
    }

    fn set_user(&self, user: Option<AuthedUser>) {
        let mut status = self.inner.status_tx.borrow().clone();
        status.user = user;
        let _ = self.inner.status_tx.send(status);
    }
}

/// Canonical dedup key for a query shape (stable field order via serde_json).
pub(super) fn canonical_key(query: &Query) -> String {
    serde_json::to_string(query).unwrap_or_default()
}

/// `http(s)://` → `ws(s)://`, trimming trailing slashes. Already-`ws(s)` URLs and
/// anything else pass through unchanged.
///
/// `pub(crate)` so the admin `/admin/stream` consumer derives its ws base the
/// same way `/sync` does instead of repeating the mapping.
pub(crate) fn sync_url(url: &str) -> String {
    let trimmed = url.trim_end_matches('/');
    if let Some(rest) = trimmed.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = trimmed.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        trimmed.to_string()
    }
}

/// Jittered exponential backoff: `min(max, base * 2^attempt) * (0.5 + rand*0.5)`,
/// matching `ts-client`.
pub(super) fn backoff_delay(base: Duration, max: Duration, attempt: u32) -> Duration {
    let exp = base.saturating_mul(2u32.saturating_pow(attempt.min(20)));
    let raw = exp.min(max);
    let frac = 0.5 + jitter_fraction() * 0.5;
    raw.mul_f64(frac)
}

/// Per-process pseudo-random fraction in `[0, 1)` sourced from the hasher of a
/// fresh `RandomState` — no external RNG dependency, sufficient for backoff jitter.
pub(super) fn jitter_fraction() -> f64 {
    use std::hash::{BuildHasher, Hasher};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(n);
    (h.finish() >> 11) as f64 / ((1u64 << 53) as f64)
}
