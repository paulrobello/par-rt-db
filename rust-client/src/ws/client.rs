use super::driver::{Driver, canonical_key, drive, sync_url};
use super::server_msg::apply_optimistic;
use super::state::{
    ClientInner, ClientStatus, Cmd, Config, ConnectionState, GetToken, OptimisticState,
    PresenceMaps, PresenceRoomState, PresenceSnapshot, QueuedMutate, QueuedSchedule,
    QueuedWorkflow, ScheduleMsg, ScheduleOutcome, Snapshot, SubMaps, SubState, WorkflowMsg,
    WorkflowOutcome,
};
use crate::error::RtDbError;
use crate::feed::{ChangeFeed, ChangeFeedOptions, FeedMaps, FeedState};
use crate::mutation::{StepResult, Transaction};
use crate::query::Query;
use crate::wire::{
    PresenceMember, ScheduleInfo, ScheduleWhen, WorkflowInfo, WorkflowSpec, WorkflowStatus,
};
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot, watch};

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
            feeds: Mutex::new(FeedMaps::default()),
            generation: Arc::new(AtomicU64::new(0)),
            closed: Arc::new(AtomicBool::new(false)),
            sub_counter: AtomicU64::new(1),
            feed_counter: AtomicU64::new(1),
            mut_counter: AtomicU64::new(1),
            sched_counter: AtomicU64::new(1),
            wf_counter: AtomicU64::new(1),
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
                    last_seq: Mutex::new(None),
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

    /// Open a durable change-feed push channel (`feedSubscribe`). Batches arrive
    /// oldest first, at-least-once across reconnects; the client dedupes by
    /// `(feedEpoch, seq)` and acknowledges each delivered batch. A terminal
    /// error (e.g. `CURSOR_EXPIRED`) is yielded once and never retried. Drop the
    /// returned handle to close the feed.
    pub fn watch_changes(&self, opts: ChangeFeedOptions) -> ChangeFeed {
        let feed_id = format!(
            "feed-{}",
            self.inner.feed_counter.fetch_add(1, Ordering::Relaxed)
        );
        let (state, rx) = FeedState::new(feed_id.clone(), opts);
        self.inner
            .feeds
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(state);
        let _ = self.inner.cmd_tx.send(Cmd::FeedOpen {
            feed_id: feed_id.clone(),
        });
        let cmd_tx = self.inner.cmd_tx.clone();
        let close_id = feed_id.clone();
        ChangeFeed::new(
            rx,
            feed_id,
            Box::new(move || {
                let _ = cmd_tx.send(Cmd::FeedClose {
                    feed_id: close_id.clone(),
                });
            }),
        )
    }

    /// Broadcast updated `state` for this connection in `room`. The server fans
    /// out a fresh `presenceSnapshot` to every member of the room. Also updates
    /// the cached join state so a reconnect re-joins with the latest value. No-op
    /// if this client has not joined `room` (mirrors the live server, which would
    /// not relay an update from a non-member).
    ///
    /// `ttl_ms` (ENH-015 presence-ttl) tells the server to clear this
    /// connection's `state` to null `ttl_ms` milliseconds after the last
    /// refresh (the member stays in the room). `None` clears any pending
    /// expiry, mirroring the live server's "ttlMs after the last refresh"
    /// semantics. Pass `Some(ms)` for a heartbeat-style refresh, `None` for a
    /// plain update.
    pub fn update_presence(&self, room: &str, state: serde_json::Value, ttl_ms: Option<u64>) {
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
                ttl_ms,
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
        crate::mutation::parse_step_results(results)
    }

    /// Schedule `txn` to fire at `when`. Resolves with the new schedule's id on
    /// `scheduleOk`; rejects with [`RtDbError`] on `scheduleErr` (e.g. a bad cron
    /// expression — the server validates cron). While unauthenticated, the
    /// request queues and fires on the next `authOk`, mirroring [`mutate`](Self::mutate).
    pub async fn schedule(
        &self,
        txn: &Transaction,
        when: ScheduleWhen,
        external: bool,
    ) -> Result<String, RtDbError> {
        match self
            .queue_schedule(ScheduleMsg::Schedule {
                when,
                txn: txn.clone(),
                external,
            })
            .await?
        {
            ScheduleOutcome::Id(id) => Ok(id),
            _ => Err(RtDbError::internal("unexpected schedule reply")),
        }
    }

    /// Cancel a scheduled job. Resolves `true` on `scheduleAck.ok:true`;
    /// resolves `false` for a bare `ok:false` (unknown or already-terminal id —
    /// a no-op, not a failure, matching the workflow-ack contract); rejects
    /// with [`RtDbError`] when the server returns an error envelope.
    pub async fn cancel_schedule(&self, id: &str) -> Result<bool, RtDbError> {
        self.manage_schedule(ScheduleMsg::Cancel { id: id.to_string() })
            .await
    }

    /// Pause a scheduled job until [`resume_schedule`](Self::resume_schedule).
    /// Same ack contract as [`cancel_schedule`](Self::cancel_schedule).
    pub async fn pause_schedule(&self, id: &str) -> Result<bool, RtDbError> {
        self.manage_schedule(ScheduleMsg::Pause { id: id.to_string() })
            .await
    }

    /// Resume a paused scheduled job. Same ack contract as
    /// [`cancel_schedule`](Self::cancel_schedule).
    pub async fn resume_schedule(&self, id: &str) -> Result<bool, RtDbError> {
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

    /// Shared body for cancel/pause/resume: await the ack. A bare `ok:false` is
    /// a no-op (resolves `false`), not an error.
    async fn manage_schedule(&self, msg: ScheduleMsg) -> Result<bool, RtDbError> {
        match self.queue_schedule(msg).await? {
            ScheduleOutcome::Ack(ok) => Ok(ok),
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

    /// Start a durable workflow run (FM-29). Resolves with the new run's
    /// [`WorkflowInfo`] on `startWorkflowOk`; rejects with [`RtDbError`] on
    /// `startWorkflowErr` (e.g. a spec validation failure). While
    /// unauthenticated, the request queues and fires on the next `authOk`,
    /// mirroring [`schedule`](Self::schedule).
    pub async fn start_workflow(&self, spec: &WorkflowSpec) -> Result<WorkflowInfo, RtDbError> {
        match self
            .queue_workflow(WorkflowMsg::Start { spec: spec.clone() })
            .await?
        {
            WorkflowOutcome::Info(info) => Ok(info),
            _ => Err(RtDbError::internal("unexpected workflow reply")),
        }
    }

    /// Cancel a workflow run (FM-29). Resolves `true` on `workflowAck.ok:true`;
    /// resolves `false` for a bare `ok:false` (unknown or already-terminal
    /// run — a no-op, not a failure, matching
    /// [`RtDbHttpClient::cancel_workflow`](crate::http::RtDbHttpClient::cancel_workflow));
    /// rejects with [`RtDbError`] when the server returns an error envelope.
    pub async fn cancel_workflow(&self, id: &str) -> Result<bool, RtDbError> {
        match self
            .queue_workflow(WorkflowMsg::Cancel { id: id.to_string() })
            .await?
        {
            WorkflowOutcome::Ack(ok) => Ok(ok),
            _ => Err(RtDbError::internal("unexpected workflow reply")),
        }
    }

    /// Deliver a named signal to a waiting run — an `awaitSignal` step's
    /// release. The reply reuses `workflowAck`: resolves `true` on
    /// `ok:true` (delivered); rejects with [`RtDbError`] on the typed
    /// error envelope (unknown run, not waiting, name mismatch — unlike
    /// cancel, the server never sends a bare `ok:false` here). `payload`
    /// is latest-wins and rides the step's outcome as `signal`. Mirrors
    /// [`RtDbHttpClient::signal_workflow`](crate::http::RtDbHttpClient::signal_workflow).
    pub async fn signal_workflow(
        &self,
        id: &str,
        name: &str,
        payload: Option<serde_json::Value>,
    ) -> Result<bool, RtDbError> {
        match self
            .queue_workflow(WorkflowMsg::Signal {
                id: id.to_string(),
                name: name.to_string(),
                payload,
            })
            .await?
        {
            WorkflowOutcome::Ack(ok) => Ok(ok),
            _ => Err(RtDbError::internal("unexpected workflow reply")),
        }
    }

    /// List workflow runs (FM-29). Resolves with the `workflows` array on
    /// `listWorkflowsOk`. A failed list arrives typed `startWorkflowErr`
    /// carrying this call's correlation id (the frame vocabulary has no
    /// distinct list-error frame) and rejects here.
    pub async fn list_workflows(
        &self,
        status: Option<WorkflowStatus>,
    ) -> Result<Vec<WorkflowInfo>, RtDbError> {
        match self.queue_workflow(WorkflowMsg::List { status }).await? {
            WorkflowOutcome::List(workflows) => Ok(workflows),
            _ => Err(RtDbError::internal("unexpected workflow reply")),
        }
    }

    /// Mint a `wf-${n}` correlation id and either dispatch the request (when
    /// authenticated) or queue it for the next `authOk`, exactly like
    /// [`queue_schedule`](Self::queue_schedule). The driver routes the matching
    /// `ServerMessage` reply back through [`WfReply`].
    async fn queue_workflow(&self, msg: WorkflowMsg) -> Result<WorkflowOutcome, RtDbError> {
        if self.is_closed() {
            return Err(RtDbError::internal("client is closed"));
        }
        let workflow_id = format!(
            "wf-{}",
            self.inner.wf_counter.fetch_add(1, Ordering::Relaxed)
        );
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = self
            .inner
            .cmd_tx
            .send(Cmd::Workflow(Box::new(QueuedWorkflow {
                workflow_id,
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
