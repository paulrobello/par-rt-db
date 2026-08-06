//! Reactive WebSocket client for par-rt-db (`ws` feature).
//!
//! One long-lived connection to `/sync` runs in a background driver task. Callers
//! get an ergonomic, cloneable handle ([`RtDbClient`]) that exposes synchronous
//! [`subscribe`](RtDbClient::subscribe) (returning a [`Subscription`] backed by a
//! `tokio::sync::watch` channel) and async [`mutate`](RtDbClient::mutate) with
//! idempotency. The driver owns the socket; on disconnect it reconnects with
//! jittered exponential backoff, re-authenticates, and re-establishes every
//! active subscription.
//!
//! Wire vocabulary is shared with the server and the TS client via
//! [`crate::wire`]: tags and casing are load-bearing and are not redefined here.
//!
//! Design: `docs/superpowers/specs/2026-07-22-rust-client-design.md` (Reactive WS
//! client). The lifecycle mirrors `ts-client/src/client.ts` exactly.

#![cfg(feature = "ws")]

use crate::error::{ErrorCode, RtDbError};
use crate::mutation::{StepResult, Transaction};
use crate::optimistic::{OptimisticProjection, project_optimistic_update};
use crate::query::Query;
use crate::wire::{
    AuthedUser, ClientMessage, PresenceMember, ScheduleInfo, ScheduleWhen, ServerMessage,
};

#[cfg(test)]
use crate::wire::{ScheduleKind, ScheduleStatus};

use futures_util::{Sink, SinkExt, StreamExt};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Instant, interval, sleep, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message as WsMessage;

/// Async token provider called on every (re)open. Factored out so the field type
/// stays readable (clippy would otherwise flag it as overly complex).
type GetToken = Box<dyn Fn() -> Pin<Box<dyn Future<Output = Option<String>> + Send>> + Send + Sync>;

/// Server closes the socket if the auth frame does not arrive within this window.
const AUTH_DEADLINE: Duration = Duration::from_secs(15);

/// WS close code the server emits on an auth failure (bad/missing token, revoked
/// authz). Terminal from the client's perspective: do not reconnect with the same
/// credential.
const CLOSE_AUTH_FAILED: u16 = 4401;

// ── Public types ─────────────────────────────────────────────────────────────

/// Coarse connection state surfaced to the caller via [`RtDbClient::status`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConnectionState {
    /// Not dialing; either never connected or auth failed (terminal) — an explicit
    /// [`RtDbClient::connect`] (e.g. after re-login) revives it.
    #[default]
    Idle,
    /// A socket is being opened and authenticated.
    Connecting,
    /// Authenticated and usable; subscriptions and mutations flow.
    Connected,
    /// Disconnected mid-session; a reconnect is scheduled.
    Reconnecting,
    /// [`RtDbClient::close`] was called; the driver is winding down.
    Closed,
}

/// Snapshot of the client's connection + auth state.
#[derive(Debug, Clone, Default)]
pub struct ClientStatus {
    pub state: ConnectionState,
    /// The authed user once `authOk` has arrived; `None` otherwise.
    pub user: Option<AuthedUser>,
}

/// One observable value for a live query, delivered through a `watch` channel.
#[derive(Debug, Clone)]
pub enum Snapshot {
    /// No `queryUpdate` has arrived yet (the receiver starts here).
    Pending,
    /// The latest authoritative result.
    Value(Box<serde_json::Value>),
    /// The subscription failed (e.g. malformed query); it will not recover.
    Error(RtDbError),
}

/// One observable member-list for a presence room, delivered through a `watch`
/// channel. Mirrors [`Snapshot`] for subscriptions: [`Pending`](Self::Pending)
/// until the first `presenceSnapshot`, then the latest member list on each
/// fan-out, or [`Error`](Self::Error) if the server rejects the join.
#[derive(Debug, Clone)]
pub enum PresenceSnapshot {
    /// No `presenceSnapshot` has arrived yet (the receiver starts here).
    Pending,
    /// The latest member list from a `presenceSnapshot`.
    Members(Vec<PresenceMember>),
    /// The server rejected the join (e.g. presence not enabled).
    Error(RtDbError),
}

/// Tunables for [`RtDbClient::with_config`]. Defaults mirror `ts-client`.
#[derive(Debug, Clone)]
pub struct Config {
    /// Exponential-backoff base for reconnect delay.
    pub backoff_base: Duration,
    /// Ceiling for the exponential reconnect delay.
    pub backoff_max: Duration,
    /// How often to send a `{type:"ping"}` keepalive. Reconnect if no pong arrives
    /// within `2 × heartbeat`.
    pub heartbeat: Duration,
    /// When true, a `mutate` overlays [`project_optimistic_update`]'s projected
    /// effect on each matching subscription immediately (before the server
    /// round-trip); the overlay is reconciled to the authoritative `queryUpdate`
    /// and rolled back on `mutateErr`/reject/close. Off ⇒ byte-for-byte the
    /// pre-optimistic behavior.
    pub optimistic_updates: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            backoff_base: Duration::from_millis(500),
            backoff_max: Duration::from_secs(15),
            heartbeat: Duration::from_secs(20),
            optimistic_updates: false,
        }
    }
}

// ── Internal subscription / mutation state ───────────────────────────────────

/// Per-subscription optimistic-overlay state. `server_last` is the projection
/// base: the most recent authoritative result, updated on each `queryUpdate`.
/// `active` is true while an overlay is currently covering the receiver (set on
/// apply, cleared on reconcile/rollback). Guarded by its own mutex so it can be
/// mutated through the shared `Arc<SubState>`; lock order is `subs` → `optimistic`.
#[derive(Default)]
struct OptimisticState {
    active: bool,
    server_last: Option<serde_json::Value>,
}

/// One server subscription, shared by every caller subscribed to the same query
/// shape. The `watch::Sender` delivers [`Snapshot`]s; the `refcount` counts live
/// [`Subscription`] handles so the last drop can send `{type:"unsubscribe"}`.
struct SubState {
    query_id: String,
    query: Query,
    tx: watch::Sender<Snapshot>,
    /// Live [`Subscription`] handles for this shape. Atomic so it can be bumped
    /// through the shared `Arc` without taking the maps lock.
    refcount: AtomicU64,
    /// Optimistic-overlay bookkeeping; interior-mutable so the apply/reconcile/
    /// rollback hooks can mutate it through the `Arc<SubState>`.
    optimistic: Mutex<OptimisticState>,
}

/// Shared (caller ↔ driver) subscription tables, guarded by one mutex. `by_key`
/// is authoritative (keyed by the canonical query shape); `by_id` routes incoming
/// `queryUpdate`s back to the shape. `overlays` is the reverse index
/// (`mut_id → query_id`s) for rollback: when a `mutateErr`/reject drops a
/// mutation, every subscription it overlaid is reverted.
#[derive(Default)]
struct SubMaps {
    by_key: HashMap<String, Arc<SubState>>,
    by_id: HashMap<String, String>,
    overlays: HashMap<String, HashSet<String>>,
}

/// One presence room, shared by every caller that joined it. `state` is the
/// latest join/update state (so a reconnect replays with the freshest value);
/// `tx` delivers [`PresenceSnapshot`]s to every live [`Presence`] handle.
/// Guarded by its own mutex for interior mutability through the shared `Arc`.
struct PresenceRoomState {
    room: String,
    state: Mutex<Option<serde_json::Value>>,
    tx: watch::Sender<PresenceSnapshot>,
}

/// Per-client presence registry. One entry per joined room, keyed by room name
/// (rooms are unique per client — one connection joins a room once). Mirrors how
/// [`SubMaps`] holds one entry per query shape.
#[derive(Default)]
struct PresenceMaps {
    by_room: HashMap<String, Arc<PresenceRoomState>>,
}

/// Reply channel for a mutation the caller is awaiting.
type MutReply = oneshot::Sender<Result<Vec<serde_json::Value>, RtDbError>>;

/// A mutation awaiting its turn to be sent (queued while disconnected, or
/// re-queued when an in-flight send failed mid-session). Carries its reply so a
/// queued mutation survives a reconnect without dropping the caller's future.
struct QueuedMutate {
    mut_id: String,
    idempotency_key: Option<String>,
    txn: Transaction,
    reply: MutReply,
}

/// Reply channel for a schedule/list/manage call the caller is awaiting.
type SchedReply = oneshot::Sender<Result<ScheduleOutcome, RtDbError>>;

/// The typed success payload of a schedule-family reply. Each public method
/// extracts the arm it expects and treats any other arm as an internal error
/// (the server sends the reply matching the request kind).
#[derive(Debug)]
enum ScheduleOutcome {
    /// `scheduleOk { id }` — the newly created schedule's id.
    Id(String),
    /// `scheduleAck { ok: true }` — cancel/pause/resume succeeded (no payload).
    Ack,
    /// `listSchedulesOk { schedules }`.
    List(Vec<ScheduleInfo>),
}

/// The request kind a schedule call will send once authenticated, carried while
/// queued so the driver can build the right `ClientMessage` frame on flush.
/// Mirrors `ts-client`'s `ScheduleMsg`.
enum ScheduleMsg {
    Schedule {
        when: ScheduleWhen,
        txn: Transaction,
    },
    Cancel {
        id: String,
    },
    Pause {
        id: String,
    },
    Resume {
        id: String,
    },
    List,
}

/// A schedule/list/manage call awaiting its turn to be sent. Like
/// [`QueuedMutate`], it survives a reconnect: in-flight on a failed send it is
/// re-queued and fired on the next `authOk`.
struct QueuedSchedule {
    schedule_id: String,
    msg: ScheduleMsg,
    reply: SchedReply,
}

/// Commands callers send to the driver task.
enum Cmd {
    /// Nudge the driver to (re)connect / re-auth — `connect()` and token refresh.
    Wake,
    /// Register a subscribe intent for an existing shape (caller already did the
    /// bookkeeping); the driver dedups per session.
    Subscribe { query_id: String, query: Box<Query> },
    /// Last handle for a shape dropped; driver decrements and may unsubscribe.
    Unsubscribe { query_id: String },
    /// A caller-initiated mutation with its reply channel. Boxed so the small
    /// command variants don't inherit [`QueuedMutate`]'s size.
    Mutate(Box<QueuedMutate>),
    /// A caller-initiated schedule/list/manage call with its reply channel.
    /// Boxed for the same reason as [`Cmd::Mutate`].
    Schedule(Box<QueuedSchedule>),
    /// Join a presence room (bookkeeping already done; driver sends the wire
    /// frame only after auth, mirroring `Cmd::Subscribe`). Idempotent within a
    /// session — deduped via the `sent_rooms` set in the session loop.
    PresenceJoin {
        room: String,
        state: Option<serde_json::Value>,
    },
    /// Update this connection's state in a joined room. Sent only while
    /// authenticated; on a dropped session the cached state in
    /// [`PresenceRoomState`] replays with the freshest value on the next join.
    PresenceUpdate {
        room: String,
        state: serde_json::Value,
    },
    /// Leave a presence room (bookkeeping already cleared by the caller).
    PresenceLeave { room: String },
    /// Tear the driver down.
    Shutdown,
}

/// Heartbeat bookkeeping: every `heartbeat` send a ping; if no pong has arrived in
/// `2 × heartbeat`, the connection is presumed dead.
struct Liveness {
    heartbeat: Duration,
    last_pong: Instant,
}

impl Liveness {
    fn new(heartbeat: Duration) -> Self {
        Self {
            heartbeat,
            last_pong: Instant::now(),
        }
    }

    fn note_pong(&mut self) {
        self.last_pong = Instant::now();
    }

    /// True when the peer has not acknowledged a ping within the liveness window.
    fn timed_out(&self) -> bool {
        self.last_pong.elapsed() >= self.heartbeat * 2
    }
}

/// Shared client state referenced by the handle and the driver task.
struct ClientInner {
    url: String,
    db: String,
    config: Config,
    get_token: GetToken,
    cmd_tx: mpsc::UnboundedSender<Cmd>,
    status_tx: watch::Sender<ClientStatus>,
    subs: Mutex<SubMaps>,
    /// Joined presence rooms. The driver replays these on each (re)connect,
    /// mirroring how it re-establishes subscriptions from `subs`.
    presence: Mutex<PresenceMaps>,
    /// Bumped on every (re)open and on `close()`; async wakeups capture the value
    /// they were scheduled under and abort if it has advanced — the guard that
    /// keeps a stale token resolution from opening a duplicate socket.
    generation: Arc<AtomicU64>,
    closed: Arc<AtomicBool>,
    sub_counter: AtomicU64,
    mut_counter: AtomicU64,
    sched_counter: AtomicU64,
}

/// A cloneable handle to the reactive client. Cloning shares one driver; dropping
/// the last handle stops it. Call [`close`](RtDbClient::close) to stop explicitly.
#[derive(Clone)]
pub struct RtDbClient {
    inner: Arc<ClientInner>,
}

/// A live-query handle returned by [`RtDbClient::subscribe`]. Drop to unsubscribe:
/// the last handle for a query shape sends `{type:"unsubscribe"}`.
pub struct Subscription {
    rx: watch::Receiver<Snapshot>,
    cmd_tx: mpsc::UnboundedSender<Cmd>,
    query_id: String,
}

impl fmt::Debug for Subscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscription")
            .field("query_id", &self.query_id)
            .finish()
    }
}

impl Subscription {
    /// The current snapshot without waiting.
    pub fn snapshot(&self) -> Snapshot {
        self.rx.borrow().clone()
    }

    /// The latest result typed as `T`, or `Ok(None)` before the first `queryUpdate`.
    pub fn value<T: serde::de::DeserializeOwned>(&self) -> Result<Option<T>, RtDbError> {
        match self.rx.borrow().clone() {
            Snapshot::Pending => Ok(None),
            Snapshot::Value(v) => serde_json::from_value::<T>(*v)
                .map(Some)
                .map_err(|e| RtDbError::internal(format!("invalid query result: {e}"))),
            Snapshot::Error(e) => Err(e),
        }
    }

    /// The subscription error if it has failed (`subscribeErr`); `None` otherwise.
    pub fn error(&self) -> Option<RtDbError> {
        match self.rx.borrow().clone() {
            Snapshot::Error(e) => Some(e),
            _ => None,
        }
    }

    /// Wait for the next snapshot change. Resolves `Err` once the subscription (or
    /// the client) is gone so a poll loop can terminate.
    pub async fn changed(&mut self) -> Result<(), RtDbError> {
        if self.rx.changed().await.is_err() {
            return Err(RtDbError::internal("subscription closed"));
        }
        Ok(())
    }

    /// Consume the handle as a stream of authoritative results, terminating when the
    /// subscription errors or is dropped. The initial [`Snapshot::Pending`] is
    /// skipped so the stream yields only actual values.
    pub fn into_stream(self) -> impl futures_util::Stream<Item = serde_json::Value> {
        futures_util::stream::unfold(self, |mut handle| async move {
            loop {
                if handle.changed().await.is_err() {
                    return None;
                }
                // Bind the clone so the `watch::Ref` borrow drops before we move
                // `handle` into the next unfold state.
                let snap = handle.rx.borrow().clone();
                match snap {
                    Snapshot::Value(v) => return Some((*v, handle)),
                    Snapshot::Error(_) => return None,
                    Snapshot::Pending => continue,
                }
            }
        })
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        // Best-effort: the driver ignores unknown query_ids (already removed).
        let _ = self.cmd_tx.send(Cmd::Unsubscribe {
            query_id: self.query_id.clone(),
        });
    }
}

/// A presence-room handle returned by [`RtDbClient::presence`]. Each handle
/// observes the room's member list through its own `watch` receiver. Dropping a
/// handle stops listening but does NOT leave the room — call
/// [`RtDbClient::leave_presence`] for that, mirroring the TS client where the
/// returned unsubscribe only removes the listener and `leavePresence` is the
/// explicit leave.
pub struct Presence {
    rx: watch::Receiver<PresenceSnapshot>,
    room: String,
}

impl fmt::Debug for Presence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Presence")
            .field("room", &self.room)
            .finish()
    }
}

impl Presence {
    /// The current snapshot without waiting.
    pub fn snapshot(&self) -> PresenceSnapshot {
        self.rx.borrow().clone()
    }

    /// The latest member list, or `Ok(None)` before the first `presenceSnapshot`.
    pub fn members(&self) -> Result<Option<Vec<PresenceMember>>, RtDbError> {
        match self.rx.borrow().clone() {
            PresenceSnapshot::Pending => Ok(None),
            PresenceSnapshot::Members(m) => Ok(Some(m)),
            PresenceSnapshot::Error(e) => Err(e),
        }
    }

    /// The room error if the server rejected the join; `None` otherwise.
    pub fn error(&self) -> Option<RtDbError> {
        match self.rx.borrow().clone() {
            PresenceSnapshot::Error(e) => Some(e),
            _ => None,
        }
    }

    /// Wait for the next snapshot change. Resolves `Err` once the room (or the
    /// client) is gone so a poll loop can terminate.
    pub async fn changed(&mut self) -> Result<(), RtDbError> {
        if self.rx.changed().await.is_err() {
            return Err(RtDbError::internal("presence room closed"));
        }
        Ok(())
    }
}

impl Clone for Presence {
    /// Clone the handle — each clone gets its own watch receiver sharing the
    /// same room membership.
    fn clone(&self) -> Self {
        Self {
            rx: self.rx.clone(),
            room: self.room.clone(),
        }
    }
}

impl RtDbClient {
    /// Connect to `<url>/sync`, authenticating to `db` with the token returned by
    /// `get_token` (called on every open/reconnect so a refreshed credential can be
    /// fetched). `url` may be `http(s)://` or `ws(s)://`. Must be called from within
    /// a tokio runtime (the driver is spawned here).
    pub fn new<F, Fut>(url: &str, db: &str, get_token: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Option<String>> + Send + 'static,
    {
        Self::with_config(url, db, get_token, Config::default())
    }

    /// Like [`new`](Self::new) with custom [`Config`].
    pub fn with_config<F, Fut>(url: &str, db: &str, get_token: F, config: Config) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Option<String>> + Send + 'static,
    {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<Cmd>();
        let (status_tx, _status_rx) = watch::channel(ClientStatus::default());
        let get_token: GetToken = Box::new(move || Box::pin(get_token()));

        let inner = Arc::new(ClientInner {
            url: sync_url(url),
            db: db.to_string(),
            config,
            get_token,
            cmd_tx,
            status_tx,
            subs: Mutex::new(SubMaps::default()),
            presence: Mutex::new(PresenceMaps::default()),
            generation: Arc::new(AtomicU64::new(0)),
            closed: Arc::new(AtomicBool::new(false)),
            sub_counter: AtomicU64::new(1),
            mut_counter: AtomicU64::new(1),
            sched_counter: AtomicU64::new(1),
        });

        tokio::spawn(drive(Driver {
            inner: inner.clone(),
            cmd_rx,
        }));
        Self { inner }
    }

    /// Start (or resume) connecting. Idempotent: a no-op if already connecting or
    /// connected. Required after `new` and after a terminal auth failure / `close`.
    pub fn connect(&self) {
        if self.is_closed() {
            return;
        }
        self.set_state(ConnectionState::Connecting);
        let _ = self.inner.cmd_tx.send(Cmd::Wake);
    }

    /// Stop the driver, reject every in-flight/queued mutation, and drop the
    /// socket. Idempotent. The driver task winds down on its own (detached).
    pub fn close(&self) {
        if !self.inner.closed.swap(true, Ordering::SeqCst) {
            self.inner.generation.fetch_add(1, Ordering::SeqCst);
            let _ = self.inner.cmd_tx.send(Cmd::Shutdown);
            self.set_state(ConnectionState::Closed);
        }
    }

    /// Whether [`close`](Self::close) has been called.
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::SeqCst)
    }

    /// Current connection/auth snapshot.
    pub fn status(&self) -> ClientStatus {
        self.inner.status_tx.borrow().clone()
    }

    /// Live state for UIs: resolves on every transition. Use `changed().await`.
    pub fn status_receiver(&self) -> watch::Receiver<ClientStatus> {
        self.inner.status_tx.subscribe()
    }

    /// Subscribe to a live query. Multiple subscribes to the same query shape share
    /// one wire subscription (dedup by the canonical serialized query). The first
    /// `queryUpdate` resolves [`Snapshot::Pending`] → [`Snapshot::Value`].
    ///
    /// Synchronous: the [`watch`] receiver exists immediately, before the server
    /// replies. If the connection is down, the shape is registered and sent on the
    /// next successful auth.
    pub fn subscribe(&self, query: impl Into<Query>) -> Subscription {
        let query = query.into();
        let key = canonical_key(&query);
        // Bookkeeping under the lock; capture (receiver, query_id) and send the
        // wire intent after releasing it.
        let (rx, query_id, wire_query) = {
            let mut maps = self.inner.subs.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(sub) = maps.by_key.get(&key) {
                sub.refcount.fetch_add(1, Ordering::Relaxed);
                let query_id = sub.query_id.clone();
                let wire_query = sub.query.clone();
                let rx = sub.tx.subscribe();
                (rx, query_id, wire_query)
            } else {
                let query_id = format!(
                    "sub-{}",
                    self.inner.sub_counter.fetch_add(1, Ordering::Relaxed)
                );
                let (tx, rx) = watch::channel(Snapshot::Pending);
                let wire_query = query.clone();
                let sub = Arc::new(SubState {
                    query_id: query_id.clone(),
                    query: query.clone(),
                    tx,
                    refcount: AtomicU64::new(1),
                    optimistic: Mutex::new(OptimisticState::default()),
                });
                maps.by_key.insert(key.clone(), sub);
                maps.by_id.insert(query_id.clone(), key);
                (rx, query_id, wire_query)
            }
        };
        let _ = self.inner.cmd_tx.send(Cmd::Subscribe {
            query_id: query_id.clone(),
            query: Box::new(wire_query),
        });
        Subscription {
            rx,
            cmd_tx: self.inner.cmd_tx.clone(),
            query_id,
        }
    }

    /// Join presence room `room`, optionally with initial `state`. The first
    /// `presenceSnapshot` (the server sends one on join listing current members)
    /// resolves [`PresenceSnapshot::Pending`] → [`PresenceSnapshot::Members`].
    ///
    /// Synchronous: the [`watch`] receiver exists immediately, before the server
    /// replies. Multiple joins to the same room share one wire membership (dedup
    /// by room name); each call returns a fresh [`Presence`] handle. If the
    /// connection is down, the join is registered and sent on the next successful
    /// auth — same gate as [`subscribe`](Self::subscribe).
    ///
    /// Dropping the returned handle stops listening but does NOT leave the room;
    /// call [`leave_presence`](Self::leave_presence) for that, mirroring the TS
    /// client where the returned unsubscribe only removes the listener.
    pub fn presence(&self, room: &str, state: Option<serde_json::Value>) -> Presence {
        let (rx, is_new) = {
            let mut maps = self
                .inner
                .presence
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if let Some(existing) = maps.by_room.get(room) {
                // Refresh the cached join state so a reconnect replays with the
                // latest value (mirrors the TS client's joinedRooms.set on join).
                if let Some(s) = &state {
                    *existing.state.lock().unwrap_or_else(|p| p.into_inner()) = Some(s.clone());
                }
                let rx = existing.tx.subscribe();
                (rx, false)
            } else {
                let (tx, rx) = watch::channel(PresenceSnapshot::Pending);
                let st = Arc::new(PresenceRoomState {
                    room: room.to_string(),
                    state: Mutex::new(state.clone()),
                    tx,
                });
                maps.by_room.insert(room.to_string(), st);
                (rx, true)
            }
        };
        if is_new {
            let _ = self.inner.cmd_tx.send(Cmd::PresenceJoin {
                room: room.to_string(),
                state,
            });
        }
        Presence {
            rx,
            room: room.to_string(),
        }
    }

    /// Broadcast updated `state` for this connection in `room`. The server fans
    /// out a fresh `presenceSnapshot` to every member of the room. Also updates
    /// the cached join state so a reconnect re-joins with the latest value. No-op
    /// if this client has not joined `room` (mirrors the live server, which would
    /// not relay an update from a non-member).
    pub fn update_presence(&self, room: &str, state: serde_json::Value) {
        let joined = {
            let maps = self
                .inner
                .presence
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if let Some(existing) = maps.by_room.get(room) {
                *existing.state.lock().unwrap_or_else(|p| p.into_inner()) = Some(state.clone());
                true
            } else {
                false
            }
        };
        if joined {
            let _ = self.inner.cmd_tx.send(Cmd::PresenceUpdate {
                room: room.to_string(),
                state,
            });
        }
    }

    /// Leave presence room `room`: drops the local membership, so the driver
    /// sends `{type:"leavePresence"}` when the last handle for the room is
    /// dropped. Local state is cleared regardless of auth state so a buffered
    /// pre-auth join does not replay after the caller has already left.
    pub fn leave_presence(&self, room: &str) {
        // Removing from the map means the driver's replay-at-session-start loop
        // will skip this room, and a subsequent Cmd::PresenceLeave (from the
        // handle's Drop) becomes a no-op (room already absent).
        let _ = self
            .inner
            .presence
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .by_room
            .remove(room);
        let _ = self.inner.cmd_tx.send(Cmd::PresenceLeave {
            room: room.to_string(),
        });
    }

    /// Submit a transaction, resolving to one [`StepResult`] per step. Pass
    /// `idempotency_key` to safely retry a mutation whose reply was lost. While
    /// disconnected the mutation is queued and sent on the next auth; it is rejected
    /// only if the connection drops after it was sent but before acknowledgment
    /// (at-most-once, never auto-resent).
    pub async fn mutate(
        &self,
        txn: &Transaction,
        idempotency_key: Option<&str>,
    ) -> Result<Vec<StepResult>, RtDbError> {
        if self.is_closed() {
            return Err(RtDbError::internal("client is closed"));
        }
        let mut_id = format!(
            "mut-{}",
            self.inner.mut_counter.fetch_add(1, Ordering::Relaxed)
        );
        if self.inner.config.optimistic_updates {
            apply_optimistic(&self.inner, &mut_id, txn);
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = self.inner.cmd_tx.send(Cmd::Mutate(Box::new(QueuedMutate {
            mut_id,
            idempotency_key: idempotency_key.map(str::to_string),
            txn: txn.clone(),
            reply: reply_tx,
        })));
        let results = reply_rx
            .await
            .map_err(|_| RtDbError::internal("client is closed"))??;
        results
            .into_iter()
            .map(|v| {
                serde_json::from_value::<StepResult>(v)
                    .map_err(|e| RtDbError::internal(format!("invalid step result: {e}")))
            })
            .collect()
    }

    /// Schedule `txn` to fire at `when`. Resolves with the new schedule's id on
    /// `scheduleOk`; rejects with [`RtDbError`] on `scheduleErr` (e.g. a bad cron
    /// expression — the server validates cron). While unauthenticated, the
    /// request queues and fires on the next `authOk`, mirroring [`mutate`](Self::mutate).
    pub async fn schedule(
        &self,
        txn: &Transaction,
        when: ScheduleWhen,
    ) -> Result<String, RtDbError> {
        match self
            .queue_schedule(ScheduleMsg::Schedule {
                when,
                txn: txn.clone(),
            })
            .await?
        {
            ScheduleOutcome::Id(id) => Ok(id),
            _ => Err(RtDbError::internal("unexpected schedule reply")),
        }
    }

    /// Cancel a scheduled job. Resolves on `scheduleAck.ok:true`; rejects with
    /// [`RtDbError`] when the server returns `ok:false` (e.g. unknown id).
    pub async fn cancel_schedule(&self, id: &str) -> Result<(), RtDbError> {
        self.manage_schedule(ScheduleMsg::Cancel { id: id.to_string() })
            .await
    }

    /// Pause a scheduled job until [`resume_schedule`](Self::resume_schedule).
    /// Same ack contract as [`cancel_schedule`](Self::cancel_schedule).
    pub async fn pause_schedule(&self, id: &str) -> Result<(), RtDbError> {
        self.manage_schedule(ScheduleMsg::Pause { id: id.to_string() })
            .await
    }

    /// Resume a paused scheduled job. Same ack contract as
    /// [`cancel_schedule`](Self::cancel_schedule).
    pub async fn resume_schedule(&self, id: &str) -> Result<(), RtDbError> {
        self.manage_schedule(ScheduleMsg::Resume { id: id.to_string() })
            .await
    }

    /// List scheduled jobs. Resolves with the `schedules` array on
    /// `listSchedulesOk`.
    pub async fn list_schedules(&self) -> Result<Vec<ScheduleInfo>, RtDbError> {
        match self.queue_schedule(ScheduleMsg::List).await? {
            ScheduleOutcome::List(schedules) => Ok(schedules),
            _ => Err(RtDbError::internal("unexpected schedule reply")),
        }
    }

    /// Shared body for cancel/pause/resume: await the ack and surface `ok:false`
    /// as an error.
    async fn manage_schedule(&self, msg: ScheduleMsg) -> Result<(), RtDbError> {
        match self.queue_schedule(msg).await? {
            ScheduleOutcome::Ack => Ok(()),
            _ => Err(RtDbError::internal("unexpected schedule reply")),
        }
    }

    /// Mint a `sch-${n}` correlation id and either dispatch the request (when
    /// authenticated) or queue it for the next `authOk`, exactly like
    /// [`mutate`](Self::mutate). The driver routes the matching `ServerMessage`
    /// reply back through [`SchedReply`].
    async fn queue_schedule(&self, msg: ScheduleMsg) -> Result<ScheduleOutcome, RtDbError> {
        if self.is_closed() {
            return Err(RtDbError::internal("client is closed"));
        }
        let schedule_id = format!(
            "sch-{}",
            self.inner.sched_counter.fetch_add(1, Ordering::Relaxed)
        );
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = self
            .inner
            .cmd_tx
            .send(Cmd::Schedule(Box::new(QueuedSchedule {
                schedule_id,
                msg,
                reply: reply_tx,
            })));
        reply_rx
            .await
            .map_err(|_| RtDbError::internal("client is closed"))?
    }

    fn set_state(&self, state: ConnectionState) {
        let mut status = self.inner.status_tx.borrow().clone();
        status.state = state;
        if matches!(state, ConnectionState::Idle | ConnectionState::Closed) {
            status.user = None;
        }
        let _ = self.inner.status_tx.send(status);
    }
}

impl Drop for RtDbClient {
    fn drop(&mut self) {
        // Stop the driver only when the last handle goes away. `strong_count == 2`
        // means this `Arc` plus the driver's clone — no other handles exist.
        if Arc::strong_count(&self.inner) <= 2 {
            self.close();
        }
    }
}

// ── Driver task ──────────────────────────────────────────────────────────────

struct Driver {
    inner: Arc<ClientInner>,
    cmd_rx: mpsc::UnboundedReceiver<Cmd>,
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

async fn drive(mut driver: Driver) {
    let mut attempt: u32 = 0;
    let mut pending: HashMap<String, MutReply> = HashMap::new();
    let mut unsent: VecDeque<QueuedMutate> = VecDeque::new();
    let mut pending_schedules: HashMap<String, SchedReply> = HashMap::new();
    let mut unsent_schedules: VecDeque<QueuedSchedule> = VecDeque::new();

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
        match run_session(
            &mut driver,
            epoch,
            token,
            &mut pending,
            &mut unsent,
            &mut pending_schedules,
            &mut unsent_schedules,
        )
        .await
        {
            SessionOutcome::AuthFailed => {
                driver.set_state(ConnectionState::Idle);
                reject_all(
                    &driver.inner,
                    &mut pending,
                    &mut unsent,
                    "authentication failed",
                );
                reject_all_schedules(
                    &mut pending_schedules,
                    &mut unsent_schedules,
                    "authentication failed",
                );
                if !wait_for_poke(&mut driver).await {
                    break;
                }
            }
            SessionOutcome::Reconnect => {
                driver.set_state(ConnectionState::Reconnecting);
                reject_inflight(
                    &driver.inner,
                    &mut pending,
                    "connection closed before acknowledgment",
                );
                reject_inflight_schedules(
                    &mut pending_schedules,
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

    reject_all(&driver.inner, &mut pending, &mut unsent, "client is closed");
    reject_all_schedules(
        &mut pending_schedules,
        &mut unsent_schedules,
        "client is closed",
    );
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
    pending: &mut HashMap<String, MutReply>,
    unsent: &mut VecDeque<QueuedMutate>,
    pending_schedules: &mut HashMap<String, SchedReply>,
    unsent_schedules: &mut VecDeque<QueuedSchedule>,
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
                        Ok(ServerMessage::AuthOk { user }) => return Ok(user),
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
        let frame = ClientMessage::Presence {
            room: room.clone(),
            state: state.clone(),
        };
        if send_text(&mut sink, &frame).await.is_err() {
            return SessionOutcome::Reconnect;
        }
    }

    // Flush mutations queued while disconnected.
    while let Some(q) = unsent.pop_front() {
        match deliver_mutate(&mut sink, q, pending).await {
            Deliver::Sent => {}
            Deliver::Reconnect(q) => {
                unsent.push_back(q);
                return SessionOutcome::Reconnect;
            }
        }
    }

    // Flush schedule/list/manage calls queued while disconnected.
    while let Some(q) = unsent_schedules.pop_front() {
        match deliver_schedule(&mut sink, q, pending_schedules).await {
            Ok(()) => {}
            Err(q) => {
                unsent_schedules.push_back(q);
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
                    match deliver_mutate(&mut sink, *q, pending).await {
                        Deliver::Sent => {}
                        Deliver::Reconnect(q) => {
                            unsent.push_back(q);
                            return SessionOutcome::Reconnect;
                        }
                    }
                }
                Some(Cmd::Schedule(q)) => {
                    match deliver_schedule(&mut sink, *q, pending_schedules).await {
                        Ok(()) => {}
                        Err(q) => {
                            unsent_schedules.push_back(q);
                            return SessionOutcome::Reconnect;
                        }
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
                Some(Cmd::PresenceUpdate { room, state }) => {
                    // Only relay if the room is joined on this session (it may
                    // have been left while this cmd was queued, or the connection
                    // dropped and the room was not re-joined).
                    if sent_rooms.contains(&room) {
                        let frame = ClientMessage::PresenceState { room, state };
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
            },
            incoming = stream.next() => match incoming {
                Some(Ok(WsMessage::Text(t))) => {
                    if let Ok(msg) = serde_json::from_str::<ServerMessage>(t.as_str()) {
                        apply_server_message(&driver.inner, msg, pending, pending_schedules);
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
        ScheduleMsg::Schedule { when, txn } => ClientMessage::Schedule {
            schedule_id: q.schedule_id.clone(),
            when: when.clone(),
            txn: txn.clone(),
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

// ── pure routing ─────────────────────────────────────────────────────────────

/// Route one inbound server message to its subscription / pending mutation /
/// pending schedule call. Pure with respect to the socket (no I/O) so it is
/// unit-testable without a server. Pong freshness is tracked by the session's
/// select arm, not here.
fn apply_server_message(
    inner: &Arc<ClientInner>,
    msg: ServerMessage,
    pending: &mut HashMap<String, MutReply>,
    pending_schedules: &mut HashMap<String, SchedReply>,
) {
    match msg {
        ServerMessage::QueryUpdate { query_id, result } => {
            let maps = inner.subs.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(sub) = maps
                .by_id
                .get(&query_id)
                .and_then(|key| maps.by_key.get(key))
            {
                // Reconcile: the authoritative result supersedes any in-flight
                // overlay; record it as the new projection base.
                {
                    let mut opt = sub.optimistic.lock().unwrap_or_else(|p| p.into_inner());
                    opt.active = false;
                    opt.server_last = Some(result.clone());
                }
                let _ = sub.tx.send(Snapshot::Value(Box::new(result)));
            }
        }
        ServerMessage::SubscribeErr { query_id, error } => {
            let mut maps = inner.subs.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(key) = maps.by_id.remove(&query_id)
                && let Some(sub) = maps.by_key.remove(&key)
            {
                let _ = sub.tx.send(Snapshot::Error(error));
            }
        }
        ServerMessage::MutateOk { mut_id, results } => {
            if let Some(reply) = pending.remove(&mut_id) {
                let _ = reply.send(Ok(results));
            }
            // No revert: the reconciling `QueryUpdate`(s) arrive and supersede
            // any overlay. Just drop the reverse index — the overlays are no
            // longer rollback-eligible.
            let mut maps = inner.subs.lock().unwrap_or_else(|p| p.into_inner());
            maps.overlays.remove(&mut_id);
        }
        ServerMessage::MutateErr { mut_id, error } => {
            if let Some(reply) = pending.remove(&mut_id) {
                let _ = reply.send(Err(error));
            }
            let mut maps = inner.subs.lock().unwrap_or_else(|p| p.into_inner());
            revert_overlays_for(&mut maps, &mut_id);
        }
        ServerMessage::ScheduleOk { schedule_id, id } => {
            if let Some(reply) = pending_schedules.remove(&schedule_id) {
                let _ = reply.send(Ok(ScheduleOutcome::Id(id)));
            }
        }
        ServerMessage::ScheduleErr { schedule_id, error } => {
            if let Some(reply) = pending_schedules.remove(&schedule_id) {
                let _ = reply.send(Err(error));
            }
        }
        ServerMessage::ScheduleAck {
            schedule_id,
            ok,
            error,
        } => {
            if let Some(reply) = pending_schedules.remove(&schedule_id) {
                if ok {
                    let _ = reply.send(Ok(ScheduleOutcome::Ack));
                } else {
                    let err =
                        error.unwrap_or_else(|| RtDbError::internal("schedule operation failed"));
                    let _ = reply.send(Err(err));
                }
            }
        }
        ServerMessage::ListSchedulesOk {
            schedule_id,
            schedules,
        } => {
            if let Some(reply) = pending_schedules.remove(&schedule_id) {
                let _ = reply.send(Ok(ScheduleOutcome::List(schedules)));
            }
        }
        ServerMessage::PresenceSnapshot { room, members } => {
            // Per-room fan-out, mirroring how QueryUpdate routes to a per-id
            // handler through the subs map. Anyone holding a Presence handle for
            // this room observes the new member list via its watch receiver.
            let maps = inner.presence.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(state) = maps.by_room.get(&room) {
                let _ = state.tx.send(PresenceSnapshot::Members(members));
            }
        }
        ServerMessage::PresenceErr { room, error } => {
            // The server rejected the join (e.g. presence not enabled). Surface
            // the error on the room's watch channel so a stale room doesn't keep
            // accumulating snapshots the caller can no longer act on, mirroring
            // the TS client which drops listeners on presenceErr.
            let maps = inner.presence.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(state) = maps.by_room.get(&room) {
                let _ = state.tx.send(PresenceSnapshot::Error(error));
            }
        }
        // Pong is handled by the session loop; AuthOk/AuthErr arrive only at the
        // handshake, never mid-session.
        ServerMessage::Pong | ServerMessage::AuthOk { .. } | ServerMessage::AuthErr { .. } => {}
    }
}

/// Decrement a shape's refcount, removing it from both maps if it hit zero.
fn maybe_unsubscribe(inner: &Arc<ClientInner>, query_id: &str) {
    let mut maps = inner.subs.lock().unwrap_or_else(|p| p.into_inner());
    let Some(key) = maps.by_id.get(query_id).cloned() else {
        return;
    };
    let Some(sub) = maps.by_key.get(&key) else {
        return;
    };
    // fetch_sub returns the PREVIOUS value; reaching 1 → this is the last handle.
    let was_last = sub.refcount.fetch_sub(1, Ordering::SeqCst) == 1;
    if was_last {
        maps.by_key.remove(&key);
        maps.by_id.remove(query_id);
    }
}

/// Mirror ts's `applyOptimistic`: for each live subscription whose `server_last`
/// base value is known, project `txn` onto it; for each non-`Skip` projection,
/// push the overlaid value through the watch channel immediately (before the
/// server round-trip) and record its `query_id` under `mut_id` in `overlays`
/// so a later rollback can find it. Caller-side (in `RtDbClient::mutate`) so
/// subscribers in other tasks see the overlay before the caller awaits.
fn apply_optimistic(inner: &ClientInner, mut_id: &str, txn: &Transaction) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let mut maps = inner.subs.lock().unwrap_or_else(|p| p.into_inner());
    let mut touched: Vec<String> = Vec::new();
    for sub in maps.by_key.values() {
        let mut opt = sub.optimistic.lock().unwrap_or_else(|p| p.into_inner());
        let Some(base) = opt.server_last.clone() else {
            continue;
        };
        match project_optimistic_update(&sub.query, &base, txn, now) {
            OptimisticProjection::Overlaid(v) => {
                opt.active = true;
                let _ = sub.tx.send(Snapshot::Value(Box::new(v)));
                touched.push(sub.query_id.clone());
            }
            OptimisticProjection::Skip => {}
        }
    }
    if !touched.is_empty() {
        maps.overlays
            .insert(mut_id.to_string(), touched.into_iter().collect());
    }
}

/// Revert one subscription's overlay: if one is active and a `server_last` base
/// exists, push the base back through the watch channel and clear `active`. No
/// op when no overlay is active (e.g. a `QueryUpdate` already reconciled).
fn revert_overlay(maps: &SubMaps, query_id: &str) {
    if let Some(sub) = maps
        .by_id
        .get(query_id)
        .and_then(|key| maps.by_key.get(key))
    {
        let mut opt = sub.optimistic.lock().unwrap_or_else(|p| p.into_inner());
        if opt.active
            && let Some(base) = opt.server_last.clone()
        {
            opt.active = false;
            let _ = sub.tx.send(Snapshot::Value(Box::new(base)));
        }
    }
}

/// Reverse-index revert: drop `mut_id`'s entry from `overlays` and revert every
/// subscription it had overlaid. Called from `MutateErr` and the reject paths.
fn revert_overlays_for(maps: &mut SubMaps, mut_id: &str) {
    if let Some(qids) = maps.overlays.remove(mut_id) {
        for qid in qids {
            revert_overlay(maps, &qid);
        }
    }
}

/// Reject every in-flight (sent, unacked) mutation. Queued (never-sent) mutations
/// are left intact so they survive the reconnect — see [`reject_all`]. Each
/// rejected mutation's overlays are reverted (a sent-but-unacked mutate never
/// gets a `MutateOk`/`MutateErr`, so the rollback must happen here).
fn reject_inflight(
    inner: &Arc<ClientInner>,
    pending: &mut HashMap<String, MutReply>,
    reason: &str,
) {
    let err = RtDbError::new(ErrorCode::Internal, reason);
    let mut maps = inner.subs.lock().unwrap_or_else(|p| p.into_inner());
    for (mut_id, reply) in pending.drain() {
        revert_overlays_for(&mut maps, &mut_id);
        let _ = reply.send(Err(err.clone()));
    }
}

/// Reject every mutation, in-flight and queued. Used on terminal teardown.
fn reject_all(
    inner: &Arc<ClientInner>,
    pending: &mut HashMap<String, MutReply>,
    unsent: &mut VecDeque<QueuedMutate>,
    reason: &str,
) {
    reject_inflight(inner, pending, reason);
    let err = RtDbError::new(ErrorCode::Internal, reason);
    // Queued mutates also had overlays applied (the apply hook runs in
    // `RtDbClient::mutate` before the driver even receives the command), so
    // revert them too.
    let mut maps = inner.subs.lock().unwrap_or_else(|p| p.into_inner());
    for q in unsent.drain(..) {
        revert_overlays_for(&mut maps, &q.mut_id);
        let _ = q.reply.send(Err(err.clone()));
    }
}

/// Reject every in-flight (sent, unacked) schedule call. Queued (never-sent)
/// calls are left intact so they survive the reconnect — see
/// [`reject_all_schedules`].
fn reject_inflight_schedules(pending: &mut HashMap<String, SchedReply>, reason: &str) {
    let err = RtDbError::new(ErrorCode::Internal, reason);
    for (_, reply) in pending.drain() {
        let _ = reply.send(Err(err.clone()));
    }
}

/// Reject every schedule call, in-flight and queued. Used on terminal teardown.
fn reject_all_schedules(
    pending: &mut HashMap<String, SchedReply>,
    unsent: &mut VecDeque<QueuedSchedule>,
    reason: &str,
) {
    reject_inflight_schedules(pending, reason);
    let err = RtDbError::new(ErrorCode::Internal, reason);
    for q in unsent.drain(..) {
        let _ = q.reply.send(Err(err.clone()));
    }
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
fn canonical_key(query: &Query) -> String {
    serde_json::to_string(query).unwrap_or_default()
}

/// `http(s)://` → `ws(s)://`, trimming trailing slashes. Already-`ws(s)` URLs and
/// anything else pass through unchanged.
fn sync_url(url: &str) -> String {
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
fn backoff_delay(base: Duration, max: Duration, attempt: u32) -> Duration {
    let exp = base.saturating_mul(2u32.saturating_pow(attempt.min(20)));
    let raw = exp.min(max);
    let frac = 0.5 + jitter_fraction() * 0.5;
    raw.mul_f64(frac)
}

/// Per-process pseudo-random fraction in `[0, 1)` sourced from the hasher of a
/// fresh `RandomState` — no external RNG dependency, sufficient for backoff jitter.
fn jitter_fraction() -> f64 {
    use std::hash::{BuildHasher, Hasher};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(n);
    (h.finish() >> 11) as f64 / ((1u64 << 53) as f64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::TableQuery;
    use serde_json::json;

    fn q(table: &str) -> Query {
        TableQuery::new(table).collect()
    }

    #[test]
    fn sync_url_flips_scheme() {
        assert_eq!(sync_url("http://h:8000"), "ws://h:8000");
        assert_eq!(sync_url("https://h"), "wss://h");
        assert_eq!(sync_url("https://h/"), "wss://h");
        assert_eq!(sync_url("https://h//"), "wss://h");
        assert_eq!(sync_url("wss://h"), "wss://h");
        assert_eq!(sync_url("ws://h/sync"), "ws://h/sync");
    }

    #[test]
    fn canonical_key_dedups_same_shape() {
        let a = canonical_key(&q("items"));
        let b = canonical_key(&q("items"));
        let c = canonical_key(&TableQuery::new("items").take(5));
        assert_eq!(a, b, "same shape must share a key");
        assert_ne!(a, c, "different shape must differ");
    }

    #[test]
    fn backoff_is_bounded_and_jittered() {
        let base = Duration::from_millis(100);
        let max = Duration::from_secs(1);
        for attempt in 0..8 {
            let d = backoff_delay(base, max, attempt);
            assert!(d <= max, "attempt {attempt} exceeded ceiling: {d:?}");
            assert!(d >= base / 2, "attempt {attempt} below half-base: {d:?}");
        }
        let d0 = backoff_delay(base, max, 0);
        assert!(d0 >= base / 2 && d0 <= base);
    }

    #[test]
    fn jitter_fraction_in_unit_range() {
        for _ in 0..256 {
            let f = jitter_fraction();
            assert!((0.0..1.0).contains(&f), "out of range: {f}");
        }
    }

    // ── message (de)serialization round-trips ───────────────────────────────

    #[test]
    fn auth_frame_shape() {
        let v = serde_json::to_value(ClientMessage::Auth {
            token: Some("t".into()),
            db: "d".into(),
        })
        .unwrap();
        assert_eq!(v, json!({"type":"auth","token":"t","db":"d"}));
    }

    #[test]
    fn empty_mutate_frame_shape() {
        let v = serde_json::to_value(ClientMessage::Mutate {
            mut_id: "mut-1".into(),
            idempotency_key: Some("k".into()),
            txn: Transaction { steps: vec![] },
        })
        .unwrap();
        assert_eq!(
            v,
            json!({"type":"mutate","mutId":"mut-1","idempotencyKey":"k","txn":{"steps":[]}})
        );
    }

    #[test]
    fn schedule_client_frame_shapes() {
        let s = serde_json::to_value(ClientMessage::Schedule {
            schedule_id: "sch-1".into(),
            when: ScheduleWhen::Cron {
                expr: "*/5 * * * *".into(),
            },
            txn: Transaction { steps: vec![] },
        })
        .unwrap();
        assert_eq!(
            s,
            json!({
                "type":"schedule",
                "scheduleId":"sch-1",
                "when":{"type":"cron","expr":"*/5 * * * *"},
                "txn":{"steps":[]}
            })
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::CancelSchedule {
                schedule_id: "sch-1".into(),
                id: "job-1".into(),
            })
            .unwrap(),
            json!({"type":"cancelSchedule","scheduleId":"sch-1","id":"job-1"})
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::ListSchedules {
                schedule_id: "sch-1".into()
            })
            .unwrap(),
            json!({"type":"listSchedules","scheduleId":"sch-1"})
        );
    }

    #[test]
    fn server_messages_round_trip() {
        let cases = vec![
            json!({"type":"queryUpdate","queryId":"sub-1","result":[{"_id":"a"}]}),
            json!({"type":"mutateOk","mutId":"mut-1","results":[]}),
            json!({"type":"mutateErr","mutId":"mut-2","error":{"code":"NOT_FOUND","message":"x"}}),
            json!({"type":"subscribeErr","queryId":"sub-1","error":{"code":"BAD_REQUEST","message":"bad index"}}),
            json!({"type":"scheduleOk","scheduleId":"sch-1","id":"job-9"}),
            json!({"type":"scheduleErr","scheduleId":"sch-1","error":{"code":"BAD_REQUEST","message":"bad cron"}}),
            json!({"type":"scheduleAck","scheduleId":"sch-1","ok":true}),
            json!({"type":"scheduleAck","scheduleId":"sch-1","ok":false,"error":{"code":"NOT_FOUND","message":"missing job"}}),
            json!({"type":"listSchedulesOk","scheduleId":"sch-1","schedules":[]}),
            json!({"type":"pong"}),
        ];
        for raw in cases {
            let msg: ServerMessage = serde_json::from_value(raw.clone()).unwrap();
            let back = serde_json::to_value(&msg).unwrap();
            assert_eq!(back, raw);
        }
    }

    // ── subscription-state / routing logic (no live server) ─────────────────
    //
    // `apply_server_message` operates on the shared sub maps and a local pending
    // map; exercise it directly to cover QueryUpdate, SubscribeErr, MutateOk/Err.

    fn rig_with_sub() -> (Arc<ClientInner>, watch::Receiver<Snapshot>) {
        rig_with_sub_and_config(Config::default())
    }

    fn rig_with_sub_and_config(config: Config) -> (Arc<ClientInner>, watch::Receiver<Snapshot>) {
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel::<Cmd>();
        let (status_tx, _status_rx) = watch::channel(ClientStatus::default());
        let inner = Arc::new(ClientInner {
            url: "ws://h".into(),
            db: "d".into(),
            config,
            get_token: Box::new(|| Box::pin(async { None })),
            cmd_tx,
            status_tx,
            subs: Mutex::new(SubMaps::default()),
            presence: Mutex::new(PresenceMaps::default()),
            generation: Arc::new(AtomicU64::new(0)),
            closed: Arc::new(AtomicBool::new(false)),
            sub_counter: AtomicU64::new(1),
            mut_counter: AtomicU64::new(1),
            sched_counter: AtomicU64::new(1),
        });
        let query = q("items");
        let (tx, rx) = watch::channel(Snapshot::Pending);
        let sub = Arc::new(SubState {
            query_id: "sub-1".into(),
            query: query.clone(),
            tx,
            refcount: AtomicU64::new(1),
            optimistic: Mutex::new(OptimisticState::default()),
        });
        {
            let mut maps = inner.subs.lock().unwrap();
            maps.by_key.insert(canonical_key(&query), sub);
            maps.by_id.insert("sub-1".into(), canonical_key(&query));
        }
        (inner, rx)
    }

    /// Lock the maps and grab the lone seeded sub's `Arc`. Helper for tests that
    /// need to seed `optimistic` state before delivering a server message.
    fn only_sub(maps: &SubMaps) -> Arc<SubState> {
        maps.by_key
            .values()
            .next()
            .expect("rig seeds one sub")
            .clone()
    }

    #[test]
    fn query_update_delivers_value() {
        let (inner, rx) = rig_with_sub();
        let mut pending = HashMap::new();
        let mut pending_schedules: HashMap<String, SchedReply> = HashMap::new();
        apply_server_message(
            &inner,
            ServerMessage::QueryUpdate {
                query_id: "sub-1".into(),
                result: json!([{"_id":"a"}]),
            },
            &mut pending,
            &mut pending_schedules,
        );
        assert!(matches!(rx.borrow().clone(), Snapshot::Value(_)));
    }

    #[test]
    fn subscribe_err_routes_error_and_removes() {
        let (inner, rx) = rig_with_sub();
        let mut pending = HashMap::new();
        let mut pending_schedules: HashMap<String, SchedReply> = HashMap::new();
        apply_server_message(
            &inner,
            ServerMessage::SubscribeErr {
                query_id: "sub-1".into(),
                error: RtDbError::new(ErrorCode::BadRequest, "bad index"),
            },
            &mut pending,
            &mut pending_schedules,
        );
        assert!(matches!(rx.borrow().clone(), Snapshot::Error(_)));
        let maps = inner.subs.lock().unwrap();
        assert!(maps.by_id.is_empty() && maps.by_key.is_empty());
        assert!(maps.overlays.is_empty());
    }

    #[tokio::test]
    async fn mutate_ok_and_err_resolve_pending() {
        let (inner, _) = rig_with_sub();
        let mut pending: HashMap<String, MutReply> = HashMap::new();
        let mut pending_schedules: HashMap<String, SchedReply> = HashMap::new();
        let (tx_ok, rx_ok) = oneshot::channel();
        let (tx_err, rx_err) = oneshot::channel();
        pending.insert("mut-1".into(), tx_ok);
        pending.insert("mut-2".into(), tx_err);

        apply_server_message(
            &inner,
            ServerMessage::MutateOk {
                mut_id: "mut-1".into(),
                results: vec![json!({"id":"a"})],
            },
            &mut pending,
            &mut pending_schedules,
        );
        apply_server_message(
            &inner,
            ServerMessage::MutateErr {
                mut_id: "mut-2".into(),
                error: RtDbError::new(ErrorCode::NotFound, "x"),
            },
            &mut pending,
            &mut pending_schedules,
        );
        let ok = rx_ok.await.unwrap().unwrap();
        assert_eq!(ok.len(), 1);
        let err = rx_err.await.unwrap().unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
        assert!(pending.is_empty());
    }

    // ── presence routing ───────────────────────────────────────────────────
    //
    // Mirror how queryUpdate/SubscribeErr route to a per-id handler, but for
    // presence: a presenceSnapshot fans out to the room's watch sender, and a
    // presenceErr sets the Error state (mirrors the TS client dropping listeners).

    /// Helper: build an inner with one seeded presence room and return the receiver.
    fn rig_with_presence(room: &str) -> (Arc<ClientInner>, watch::Receiver<PresenceSnapshot>) {
        let (inner, _) = rig_with_sub();
        let (tx, rx) = watch::channel(PresenceSnapshot::Pending);
        let state = Arc::new(PresenceRoomState {
            room: room.to_string(),
            state: Mutex::new(None),
            tx,
        });
        inner
            .presence
            .lock()
            .unwrap()
            .by_room
            .insert(room.to_string(), state);
        (inner, rx)
    }

    fn presence_member(cid: &str) -> crate::wire::PresenceMember {
        crate::wire::PresenceMember {
            connection_id: cid.to_string(),
            user: crate::wire::AuthedUser {
                kind: crate::wire::UserKind::User,
                email: None,
                name: None,
                github_login: None,
                github_id: None,
            },
            state: json!({}),
        }
    }

    #[test]
    fn presence_snapshot_routes_to_room() {
        let (inner, rx) = rig_with_presence("doc:1");
        let mut pending = HashMap::new();
        let mut pending_schedules: HashMap<String, SchedReply> = HashMap::new();
        apply_server_message(
            &inner,
            ServerMessage::PresenceSnapshot {
                room: "doc:1".into(),
                members: vec![presence_member("c1"), presence_member("c2")],
            },
            &mut pending,
            &mut pending_schedules,
        );
        match rx.borrow().clone() {
            PresenceSnapshot::Members(m) => {
                assert_eq!(m.len(), 2);
                assert_eq!(m[0].connection_id, "c1");
                assert_eq!(m[1].connection_id, "c2");
            }
            other => panic!("expected Members, got {other:?}"),
        }
    }

    #[test]
    fn presence_snapshot_ignored_for_unknown_room() {
        let (inner, rx) = rig_with_presence("doc:1");
        let mut pending = HashMap::new();
        let mut pending_schedules: HashMap<String, SchedReply> = HashMap::new();
        // A snapshot for a room we haven't joined should not affect our receiver.
        apply_server_message(
            &inner,
            ServerMessage::PresenceSnapshot {
                room: "doc:other".into(),
                members: vec![presence_member("c9")],
            },
            &mut pending,
            &mut pending_schedules,
        );
        assert!(matches!(rx.borrow().clone(), PresenceSnapshot::Pending));
    }

    #[test]
    fn presence_err_routes_error_to_room() {
        let (inner, rx) = rig_with_presence("doc:1");
        let mut pending = HashMap::new();
        let mut pending_schedules: HashMap<String, SchedReply> = HashMap::new();
        apply_server_message(
            &inner,
            ServerMessage::PresenceErr {
                room: "doc:1".into(),
                error: RtDbError::new(ErrorCode::Forbidden, "presence not enabled"),
            },
            &mut pending,
            &mut pending_schedules,
        );
        match rx.borrow().clone() {
            PresenceSnapshot::Error(e) => {
                assert_eq!(e.code, ErrorCode::Forbidden);
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    // ── optimistic-update wiring ───────────────────────────────────────────
    //
    // With `optimistic_updates` off (the default) every preceding test is
    // unchanged. The three tests below exercise the apply/reconcile/rollback
    // hooks that light up only with the flag on.

    /// Apply hook: an insert transaction overlays the projected insert on the
    /// receiver immediately and records the reverse-index entry.
    #[test]
    fn apply_optimistic_pushes_overlay_when_enabled() {
        let (inner, rx) = rig_with_sub_and_config(Config {
            optimistic_updates: true,
            ..Config::default()
        });
        // Seed the projection base (normally set by the first QueryUpdate).
        {
            let maps = inner.subs.lock().unwrap();
            let sub = only_sub(&maps);
            let mut opt = sub.optimistic.lock().unwrap();
            opt.server_last = Some(json!([{"_id":"a","_creationTime":1,"_version":1,"n":1}]));
        }
        let txn = crate::mutation::Mutation::new()
            .insert("items", json!({ "n": 2 }))
            .build();
        apply_optimistic(&inner, "mut-7", &txn);

        match rx.borrow().clone() {
            Snapshot::Value(v) => {
                let arr = v.as_array().expect("overlaid is array");
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[1]["n"], 2);
                assert!(
                    arr[1]["_id"]
                        .as_str()
                        .unwrap()
                        .starts_with("__optimistic__")
                );
            }
            other => panic!("expected overlaid Value, got {other:?}"),
        }
        let maps = inner.subs.lock().unwrap();
        let qids = maps
            .overlays
            .get("mut-7")
            .expect("overlay reverse-index entry recorded");
        assert_eq!(qids.len(), 1);
        let sub = only_sub(&maps);
        let opt = sub.optimistic.lock().unwrap();
        assert!(opt.active, "overlay marked active");
    }

    /// Reconcile hook: a `QueryUpdate` clears an active overlay, pushes the
    /// authoritative value, and records it as the new projection base.
    #[test]
    fn query_update_reconcile_clears_active_and_records_base() {
        let (inner, rx) = rig_with_sub();
        // Seed an active overlay with a stale base.
        {
            let maps = inner.subs.lock().unwrap();
            let sub = only_sub(&maps);
            let mut opt = sub.optimistic.lock().unwrap();
            opt.active = true;
            opt.server_last = Some(json!([{"_id":"old"}]));
        }
        let mut pending = HashMap::new();
        let mut pending_schedules: HashMap<String, SchedReply> = HashMap::new();
        apply_server_message(
            &inner,
            ServerMessage::QueryUpdate {
                query_id: "sub-1".into(),
                result: json!([{"_id":"a"},{"_id":"b"}]),
            },
            &mut pending,
            &mut pending_schedules,
        );
        match rx.borrow().clone() {
            Snapshot::Value(v) => {
                assert_eq!(v.as_array().unwrap().len(), 2);
            }
            other => panic!("expected reconciled Value, got {other:?}"),
        }
        let maps = inner.subs.lock().unwrap();
        let sub = only_sub(&maps);
        let opt = sub.optimistic.lock().unwrap();
        assert!(!opt.active, "reconcile cleared active");
        assert_eq!(
            opt.server_last,
            Some(json!([{"_id":"a"},{"_id":"b"}])),
            "server_last updated to authoritative base"
        );
    }

    /// Rollback hook: a `MutateErr` reverts every overlay indexed under its
    /// `mut_id` back to `server_last` and drops the reverse-index entry.
    #[tokio::test]
    async fn mutate_err_rolls_back_overlay() {
        let (inner, rx) = rig_with_sub();
        let base = json!([{"_id":"a","_creationTime":1,"_version":1,"n":1}]);
        // Seed: an overlay is active, server_last holds the pre-overlay base,
        // and the overlay is reverse-indexed under mut-1.
        {
            let mut maps = inner.subs.lock().unwrap();
            let sub = only_sub(&maps);
            {
                let mut opt = sub.optimistic.lock().unwrap();
                opt.active = true;
                opt.server_last = Some(base.clone());
            }
            // Simulate the in-flight overlaid value the receiver currently sees.
            let _ = sub
                .tx
                .send(Snapshot::Value(Box::new(json!([{"_id":"a","n":2}]))));
            let mut set = HashSet::new();
            set.insert("sub-1".to_string());
            maps.overlays.insert("mut-1".into(), set);
        }
        match rx.borrow().clone() {
            Snapshot::Value(v) => assert_eq!(v[0]["n"], 2, "receiver shows overlaid value"),
            other => panic!("expected overlaid Value pre-rollback, got {other:?}"),
        }

        let mut pending: HashMap<String, MutReply> = HashMap::new();
        let mut pending_schedules: HashMap<String, SchedReply> = HashMap::new();
        apply_server_message(
            &inner,
            ServerMessage::MutateErr {
                mut_id: "mut-1".into(),
                error: RtDbError::new(ErrorCode::Internal, "boom"),
            },
            &mut pending,
            &mut pending_schedules,
        );

        match rx.borrow().clone() {
            Snapshot::Value(v) => assert_eq!(*v, base, "receiver reverted to base"),
            other => panic!("expected reverted base, got {other:?}"),
        }
        let maps = inner.subs.lock().unwrap();
        assert!(maps.overlays.is_empty(), "reverse-index entry dropped");
        let sub = only_sub(&maps);
        let opt = sub.optimistic.lock().unwrap();
        assert!(!opt.active, "overlay inactive after rollback");
        assert_eq!(
            opt.server_last,
            Some(base),
            "server_last preserved for next projection"
        );
    }

    /// `MutateOk` drops the reverse-index entry without reverting: the
    /// reconciling `QueryUpdate`(s) will supersede any overlay.
    #[tokio::test]
    async fn mutate_ok_drops_overlay_without_revert() {
        let (inner, rx) = rig_with_sub();
        let overlaid = json!([{"_id":"a","n":2}]);
        {
            let mut maps = inner.subs.lock().unwrap();
            let sub = only_sub(&maps);
            {
                let mut opt = sub.optimistic.lock().unwrap();
                opt.active = true;
                opt.server_last = Some(json!([{"_id":"a","n":1}]));
            }
            let _ = sub.tx.send(Snapshot::Value(Box::new(overlaid.clone())));
            let mut set = HashSet::new();
            set.insert("sub-1".to_string());
            maps.overlays.insert("mut-1".into(), set);
        }

        let mut pending: HashMap<String, MutReply> = HashMap::new();
        let mut pending_schedules: HashMap<String, SchedReply> = HashMap::new();
        let (reply_tx, _reply_rx) = oneshot::channel();
        pending.insert("mut-1".into(), reply_tx);
        apply_server_message(
            &inner,
            ServerMessage::MutateOk {
                mut_id: "mut-1".into(),
                results: vec![json!({"id":"a"})],
            },
            &mut pending,
            &mut pending_schedules,
        );

        // Receiver is unchanged — the overlaid value stays until the QueryUpdate
        // arrives to reconcile.
        match rx.borrow().clone() {
            Snapshot::Value(v) => assert_eq!(*v, overlaid, "MutateOk does not revert"),
            other => panic!("expected overlaid value to persist, got {other:?}"),
        }
        let maps = inner.subs.lock().unwrap();
        assert!(
            maps.overlays.is_empty(),
            "reverse-index dropped on MutateOk"
        );
        let sub = only_sub(&maps);
        let opt = sub.optimistic.lock().unwrap();
        assert!(
            opt.active,
            "overlay still active until QueryUpdate reconciles"
        );
    }

    // Mirror of `mutate_ok_and_err_resolve_pending` for the schedule track:
    // `scheduleOk`/`scheduleErr` resolve the pending reply, and a `scheduleAck`
    // with `ok:false` surfaces the server's error envelope.
    #[tokio::test]
    async fn schedule_replies_resolve_pending() {
        let (inner, _) = rig_with_sub();
        let mut pending: HashMap<String, MutReply> = HashMap::new();
        let mut pending_schedules: HashMap<String, SchedReply> = HashMap::new();
        let (tx_ok, rx_ok) = oneshot::channel();
        let (tx_err, rx_err) = oneshot::channel();
        let (tx_ack_ok, rx_ack_ok) = oneshot::channel();
        let (tx_ack_err, rx_ack_err) = oneshot::channel();
        let (tx_list, rx_list) = oneshot::channel();
        pending_schedules.insert("sch-1".into(), tx_ok);
        pending_schedules.insert("sch-2".into(), tx_err);
        pending_schedules.insert("sch-3".into(), tx_ack_ok);
        pending_schedules.insert("sch-4".into(), tx_ack_err);
        pending_schedules.insert("sch-5".into(), tx_list);

        apply_server_message(
            &inner,
            ServerMessage::ScheduleOk {
                schedule_id: "sch-1".into(),
                id: "job-9".into(),
            },
            &mut pending,
            &mut pending_schedules,
        );
        apply_server_message(
            &inner,
            ServerMessage::ScheduleErr {
                schedule_id: "sch-2".into(),
                error: RtDbError::new(ErrorCode::BadRequest, "bad cron"),
            },
            &mut pending,
            &mut pending_schedules,
        );
        apply_server_message(
            &inner,
            ServerMessage::ScheduleAck {
                schedule_id: "sch-3".into(),
                ok: true,
                error: None,
            },
            &mut pending,
            &mut pending_schedules,
        );
        apply_server_message(
            &inner,
            ServerMessage::ScheduleAck {
                schedule_id: "sch-4".into(),
                ok: false,
                error: Some(RtDbError::new(ErrorCode::NotFound, "missing job")),
            },
            &mut pending,
            &mut pending_schedules,
        );
        apply_server_message(
            &inner,
            ServerMessage::ListSchedulesOk {
                schedule_id: "sch-5".into(),
                schedules: vec![ScheduleInfo {
                    id: "job-1".into(),
                    kind: ScheduleKind::Cron,
                    due_at: 9000,
                    cron: Some("*/5 * * * *".into()),
                    status: ScheduleStatus::Pending,
                    last_error: None,
                    created_at: 1000,
                    fired_count: 0,
                }],
            },
            &mut pending,
            &mut pending_schedules,
        );

        match rx_ok.await.unwrap().unwrap() {
            ScheduleOutcome::Id(id) => assert_eq!(id, "job-9"),
            other => panic!("expected Id, got {other:?}"),
        }
        let err = rx_err.await.unwrap().unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(matches!(
            rx_ack_ok.await.unwrap().unwrap(),
            ScheduleOutcome::Ack
        ));
        let ack_err = rx_ack_err.await.unwrap().unwrap_err();
        assert_eq!(ack_err.code, ErrorCode::NotFound);
        match rx_list.await.unwrap().unwrap() {
            ScheduleOutcome::List(list) => {
                assert_eq!(list.len(), 1);
                assert_eq!(list[0].id, "job-1");
            }
            other => panic!("expected List, got {other:?}"),
        }
        assert!(pending_schedules.is_empty());
    }

    #[test]
    fn maybe_unsubscribe_removes_at_zero() {
        let (inner, _) = rig_with_sub();
        // refcount starts at 1; one decrement → zero → removed.
        maybe_unsubscribe(&inner, "sub-1");
        let maps = inner.subs.lock().unwrap();
        assert!(maps.by_id.is_empty() && maps.by_key.is_empty());
    }

    #[test]
    fn liveness_times_out_after_window_without_pong() {
        let mut l = Liveness::new(Duration::from_millis(10));
        assert!(!l.timed_out());
        l.last_pong = Instant::now() - Duration::from_millis(100);
        assert!(l.timed_out());
        l.note_pong();
        assert!(!l.timed_out());
    }
}
