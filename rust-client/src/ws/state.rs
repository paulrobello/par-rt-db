// Doc-link scope only: the prose below references these names; code does not.
#[allow(unused_imports)]
use super::client::{Presence, RtDbClient, Subscription};
use super::driver::backoff_delay;
use crate::error::RtDbError;
use crate::feed::FeedMaps;
use crate::mutation::Transaction;
#[allow(unused_imports)]
use crate::optimistic::project_optimistic_update;
use crate::query::Query;
use crate::wire::{
    AuthedUser, PresenceMember, ScheduleInfo, ScheduleWhen, WorkflowInfo, WorkflowSpec,
    WorkflowStatus,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;

/// Async token provider called on every (re)open. Factored out so the field type
/// stays readable (clippy would otherwise flag it as overly complex).
pub(super) type GetToken =
    Box<dyn Fn() -> Pin<Box<dyn Future<Output = Option<String>> + Send>> + Send + Sync>;

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
    /// Current connection state.
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

impl Config {
    /// How long to wait before reconnect attempt `attempt` (0-based): the
    /// jittered exponential schedule `/sync` itself backs off with, exposed so
    /// a caller driving its own socket — the `/admin/stream` tail — retries on
    /// the same curve instead of inventing a second one.
    pub fn backoff_for(&self, attempt: u32) -> Duration {
        backoff_delay(self.backoff_base, self.backoff_max, attempt)
    }
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
pub(super) struct OptimisticState {
    pub(super) active: bool,
    pub(super) server_last: Option<serde_json::Value>,
}

/// One server subscription, shared by every caller subscribed to the same query
/// shape. The `watch::Sender` delivers [`Snapshot`]s; the `refcount` counts live
/// [`Subscription`] handles so the last drop can send `{type:"unsubscribe"}`.
pub(super) struct SubState {
    pub(super) query_id: String,
    pub(super) query: Query,
    pub(super) tx: watch::Sender<Snapshot>,
    /// Live [`Subscription`] handles for this shape. Atomic so it can be bumped
    /// through the shared `Arc` without taking the maps lock.
    pub(super) refcount: AtomicU64,
    /// Optimistic-overlay bookkeeping; interior-mutable so the apply/reconcile/
    /// rollback hooks can mutate it through the `Arc<SubState>`.
    pub(super) optimistic: Mutex<OptimisticState>,
}

/// Shared (caller ↔ driver) subscription tables, guarded by one mutex. `by_key`
/// is authoritative (keyed by the canonical query shape); `by_id` routes incoming
/// `queryUpdate`s back to the shape. `overlays` is the reverse index
/// (`mut_id → query_id`s) for rollback: when a `mutateErr`/reject drops a
/// mutation, every subscription it overlaid is reverted.
#[derive(Default)]
pub(super) struct SubMaps {
    pub(super) by_key: HashMap<String, Arc<SubState>>,
    pub(super) by_id: HashMap<String, String>,
    pub(super) overlays: HashMap<String, HashSet<String>>,
}

/// One presence room, shared by every caller that joined it. `state` is the
/// latest join/update state (so a reconnect replays with the freshest value);
/// `tx` delivers [`PresenceSnapshot`]s to every live [`Presence`] handle.
/// Guarded by its own mutex for interior mutability through the shared `Arc`.
pub(super) struct PresenceRoomState {
    pub(super) room: String,
    pub(super) state: Mutex<Option<serde_json::Value>>,
    /// Last applied `presenceDelta` `seq` for this room: `None` until the
    /// first delta after the latest `PresenceSnapshot` (snapshots carry no
    /// `seq`, so any delta value may follow one). Drives the gap detector in
    /// `on_presence`: `seq <= last` is a stale duplicate, `seq > last + 1` is
    /// a missed delta (discard the baseline and re-join for a fresh
    /// snapshot). Reset to `None` on every snapshot and on each (re)connect's
    /// join replay.
    pub(super) last_seq: Mutex<Option<u64>>,
    pub(super) tx: watch::Sender<PresenceSnapshot>,
}

/// Per-client presence registry. One entry per joined room, keyed by room name
/// (rooms are unique per client — one connection joins a room once). Mirrors how
/// [`SubMaps`] holds one entry per query shape.
#[derive(Default)]
pub(super) struct PresenceMaps {
    pub(super) by_room: HashMap<String, Arc<PresenceRoomState>>,
}

/// Reply channel for a mutation the caller is awaiting.
pub(super) type MutReply = oneshot::Sender<Result<Vec<serde_json::Value>, RtDbError>>;

/// A mutation awaiting its turn to be sent (queued while disconnected, or
/// re-queued when an in-flight send failed mid-session). Carries its reply so a
/// queued mutation survives a reconnect without dropping the caller's future.
pub(super) struct QueuedMutate {
    pub(super) mut_id: String,
    pub(super) idempotency_key: Option<String>,
    pub(super) txn: Transaction,
    pub(super) reply: MutReply,
}

/// Reply channel for a schedule/list/manage call the caller is awaiting.
pub(super) type SchedReply = oneshot::Sender<Result<ScheduleOutcome, RtDbError>>;

/// The typed success payload of a schedule-family reply. Each public method
/// extracts the arm it expects and treats any other arm as an internal error
/// (the server sends the reply matching the request kind).
#[derive(Debug)]
pub(super) enum ScheduleOutcome {
    /// `scheduleOk { id }` — the newly created schedule's id.
    Id(String),
    /// `scheduleAck { ok }` — cancel/pause/resume outcome. `false` means the
    /// server found no such pending job (unknown or already-terminal id): a
    /// no-op, not a failure.
    Ack(bool),
    /// `listSchedulesOk { schedules }`.
    List(Vec<ScheduleInfo>),
}

/// The request kind a schedule call will send once authenticated, carried while
/// queued so the driver can build the right `ClientMessage` frame on flush.
/// Mirrors `ts-client`'s `ScheduleMsg`.
pub(super) enum ScheduleMsg {
    Schedule {
        when: ScheduleWhen,
        txn: Transaction,
        external: bool,
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
pub(super) struct QueuedSchedule {
    pub(super) schedule_id: String,
    pub(super) msg: ScheduleMsg,
    pub(super) reply: SchedReply,
}

/// Reply channel for a workflow call the caller is awaiting.
pub(super) type WfReply = oneshot::Sender<Result<WorkflowOutcome, RtDbError>>;

/// The typed success payload of a workflow-family reply. Each public method
/// extracts the arm it expects and treats any other arm as an internal error
/// (the server sends the reply matching the request kind).
#[derive(Debug)]
pub(super) enum WorkflowOutcome {
    /// `startWorkflowOk { info }` — the newly created run's projection.
    Info(WorkflowInfo),
    /// `workflowAck { ok }` — cancel resolved. `false` means the server
    /// found no active run (unknown or already terminal): a no-op, not a
    /// failure.
    Ack(bool),
    /// `listWorkflowsOk { workflows }`.
    List(Vec<WorkflowInfo>),
}

/// The request kind a workflow call will send once authenticated, carried
/// while queued so the driver can build the right `ClientMessage` frame on
/// flush. Mirrors `ts-client`'s `WorkflowMsg`.
pub(super) enum WorkflowMsg {
    Start {
        spec: WorkflowSpec,
    },
    Cancel {
        id: String,
    },
    Signal {
        id: String,
        name: String,
        payload: Option<serde_json::Value>,
    },
    List {
        status: Option<WorkflowStatus>,
    },
}

/// A workflow call awaiting its turn to be sent. Same reconnect contract as
/// [`QueuedSchedule`]: in-flight on a failed send it is re-queued and fired on
/// the next `authOk`.
pub(super) struct QueuedWorkflow {
    pub(super) workflow_id: String,
    pub(super) msg: WorkflowMsg,
    pub(super) reply: WfReply,
}

/// One reply-tracking family: replies awaiting a server ack, keyed by
/// correlation id (`by_id`), and calls still queued because no session was
/// open when they were made (`unsent`). `unsent` entries are replayed in
/// order on the next successful auth (or rejected on terminal teardown);
/// `by_id` entries were sent but not yet acknowledged, so a mid-session
/// disconnect rejects them (see `reject_inflight`) while `unsent` survives.
pub(super) struct Pending<Q, R> {
    pub(super) by_id: HashMap<String, R>,
    pub(super) unsent: VecDeque<Q>,
}

impl<Q, R> Default for Pending<Q, R> {
    fn default() -> Self {
        Self {
            by_id: HashMap::new(),
            unsent: VecDeque::new(),
        }
    }
}

/// The three structurally-parallel reply-tracking families threaded through
/// `drive`/`run_session`: mutate, schedule, and workflow calls each have their
/// own pending-ack map and queued-while-disconnected deque.
#[derive(Default)]
pub(super) struct PendingQueues {
    pub(super) mutate: Pending<QueuedMutate, MutReply>,
    pub(super) schedule: Pending<QueuedSchedule, SchedReply>,
    pub(super) workflow: Pending<QueuedWorkflow, WfReply>,
}

/// Commands callers send to the driver task.
pub(super) enum Cmd {
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
    /// A caller-initiated workflow call with its reply channel. Boxed for the
    /// same reason as [`Cmd::Mutate`].
    Workflow(Box<QueuedWorkflow>),
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
    /// `ttl_ms` (ENH-015 presence-ttl) tells the server to clear this
    /// connection's `state` to null `ttl_ms` after the last refresh (the member
    /// stays); `None` means no expiry. The join frame (`PresenceJoin`) never
    /// carries ttl — ttl is a refresh concept.
    PresenceUpdate {
        room: String,
        state: serde_json::Value,
        ttl_ms: Option<u64>,
    },
    /// Leave a presence room (bookkeeping already cleared by the caller).
    PresenceLeave { room: String },
    /// Seq-gap resync: re-send a joined room's `presence` frame (with its
    /// cached state) even though the session already sent it — the delta
    /// stream's `seq` skipped, so the receiver's baseline is stale and a
    /// re-join is the only way to force a full `presenceSnapshot` (the
    /// server resets the room's caught-up flag on every join). Distinct from
    /// [`Cmd::PresenceJoin`] precisely so the session's `sent_rooms` dedup
    /// cannot swallow it.
    PresenceResync { room: String },
    /// Open a change feed (bookkeeping already done; the driver sends
    /// `feedSubscribe` with the feed's current cursor).
    FeedOpen { feed_id: String },
    /// Close a change feed (handle dropped).
    FeedClose { feed_id: String },
    /// Acknowledge processed ops of a feed.
    FeedAck { feed_id: String, seq: i64 },
    /// Tear the driver down.
    Shutdown,
}

/// Heartbeat bookkeeping: every `heartbeat` send a ping; if no pong has arrived in
/// `2 × heartbeat`, the connection is presumed dead.
pub(super) struct Liveness {
    pub(super) heartbeat: Duration,
    pub(super) last_pong: Instant,
}

impl Liveness {
    pub(super) fn new(heartbeat: Duration) -> Self {
        Self {
            heartbeat,
            last_pong: Instant::now(),
        }
    }

    pub(super) fn note_pong(&mut self) {
        self.last_pong = Instant::now();
    }

    /// True when the peer has not acknowledged a ping within the liveness window.
    pub(super) fn timed_out(&self) -> bool {
        self.last_pong.elapsed() >= self.heartbeat * 2
    }
}

/// Shared client state referenced by the handle and the driver task.
pub(super) struct ClientInner {
    pub(super) url: String,
    pub(super) db: String,
    pub(super) config: Config,
    pub(super) get_token: GetToken,
    pub(super) cmd_tx: mpsc::UnboundedSender<Cmd>,
    pub(super) status_tx: watch::Sender<ClientStatus>,
    pub(super) subs: Mutex<SubMaps>,
    /// Joined presence rooms. The driver replays these on each (re)connect,
    /// mirroring how it re-establishes subscriptions from `subs`.
    pub(super) presence: Mutex<PresenceMaps>,
    /// Open change feeds; replayed on each (re)connect from their cursors.
    pub(super) feeds: Mutex<FeedMaps>,
    /// Bumped on every (re)open and on `close()`; async wakeups capture the value
    /// they were scheduled under and abort if it has advanced — the guard that
    /// keeps a stale token resolution from opening a duplicate socket.
    pub(super) generation: Arc<AtomicU64>,
    pub(super) closed: Arc<AtomicBool>,
    pub(super) sub_counter: AtomicU64,
    pub(super) feed_counter: AtomicU64,
    pub(super) mut_counter: AtomicU64,
    pub(super) sched_counter: AtomicU64,
    pub(super) wf_counter: AtomicU64,
}
