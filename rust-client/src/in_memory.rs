//! In-memory par-rt-db client for unit tests. No network, no Postgres; mirrors
//! server DSL/step-result/system-field semantics. Ports
//! `ts-client/src/in_memory.ts`.
//!
//! The server (`server/src/{txn,query,schema,protocol}.rs`) is the source of
//! truth for the declarative DSL, step-result shapes, system fields, and query
//! semantics; this client mirrors them so app code can exercise query/txn/schema
//! behavior with no network and no live Postgres. It exposes the same data
//! surface as the live clients — `push_schema`, `query` (one-shot, like
//! [`crate::RtDbHttpClient`]), `mutate`/transactions (like
//! [`crate::RtDbClient`]), and `subscribe` (reactive `query_update`s) — so a
//! test can swap it in behind a shared interface.
//!
//! Parity is deliberately scoped to the documented core (schema push, insert /
//! patch / replace / delete / expect_version / expect_absent / upsert, point
//! reads, index eq + range queries with order/take/unique/first/count, and
//! reactive subscriptions). Gaps are marked with `TODO` and return an `INTERNAL`
//! error rather than silently misbehaving.
//!
//! This module currently houses the scaffold (Task 1: struct + options +
//! `push_schema` + the validation/id/format helpers), the mutate executor
//! (Task 2: insert/patch/replace/delete/expectVersion/expectAbsent/upsert with
//! idempotency-key caching, MAX_STEPS guard, and atomic rollback), and the
//! query executor (Task 3: `run_query` — index-eq + range filtering, sort over
//! unbound index fields with `_creationTime`/`_id` tiebreakers, and the
//! `get`/`first`/`unique`/`count`/`take`/`collect` terminals), the
//! `FilterExpr` evaluator (Task 4: `validate_filter` + `eval_filter_expr`,
//! ported from the C-corrected TS logic), the `paginate` terminal
//! (Task 5: cursor-keyset paging over the sorted set), and the reactive +
//! scheduling + storage surfaces (Task 6: `subscribe` (re-runs the query and
//! fires `on_update` on change), `schedule`/`cancel_schedule`/
//! `pause_schedule`/`resume_schedule`/`list_schedules`/`tick` (one-shot
//! catches up if past due; cron re-arms by `CRON_STEP_MS` and skips missed
//! windows), and the `upload`/`delete_file`/`get_file_metadata`/`get_url`
//! storage stubs). `search`/`vector_search` stub out — no in-memory ts_rank /
//! vector ranking, but the combination guards still reject conflicting
//! terminals so the cascade agrees with the server.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::error::{ErrorCode, RtDbError};
use crate::mutation::{Step, StepResult, Transaction};
use crate::query::{Order, Query};
use crate::schema::{FieldType, IndexDef, SchemaDef, TableDef, is_widening_of};
use crate::wire::{
    AggregateOp, AuthedUser, FilterExpr, PresenceMember, ScheduleInfo, ScheduleKind,
    ScheduleStatus, ScheduleWhen,
};

/// Maximum number of steps in a single transaction (mirrors the server cap).
pub const MAX_STEPS: usize = 1024;
/// Maximum rows returned from a single `take`/`collect` (mirrors the server cap).
pub const MAX_TAKE: usize = 4096;
/// Default/cap rows a single `patchByQuery`/`deleteByQuery` step may touch
/// (mirrors `server/src/txn.rs::MAX_BY_QUERY_ROWS`). A step whose match set
/// exceeds `limit` touches exactly `limit` and reports `truncated: true`.
pub const MAX_BY_QUERY_ROWS: u32 = 1000;
/// SEC-104: hard cap on the count of `patchByQuery`/`deleteByQuery` steps in
/// one txn (mirrors `server/src/txn.rs::MAX_BY_QUERY_STEPS_PER_TXN`). Bounds
/// the worst case at 16 × 1000 = 16,000 rows rather than 1024 × 1000 ≈ 1M.
pub const MAX_BY_QUERY_STEPS_PER_TXN: usize = 16;
/// SEC-104: hard ceiling on the worst-case total documents a single txn may
/// touch (mirrors `server/src/txn.rs::MAX_AFFECTED_ROWS_PER_TXN`). Per-id
/// steps count 1 each; each by-query step counts up to its `limit`.
pub const MAX_AFFECTED_ROWS_PER_TXN: usize = 10_000;

/// SEC-104: total documents a txn could touch in the worst case. Per-id steps
/// count 1 each; each `patchByQuery`/`deleteByQuery` step counts up to its
/// `limit` (default and cap `MAX_BY_QUERY_ROWS`). Mirrors server
/// `txn::worst_case_affected`. Used by [`Self::execute_transaction`]'s
/// [`MAX_AFFECTED_ROWS_PER_TXN`] budget check.
pub fn worst_case_affected(txn: &Transaction) -> usize {
    txn.steps
        .iter()
        .map(|step| match step {
            Step::PatchByQuery { limit, .. } | Step::DeleteByQuery { limit, .. } => {
                (*limit).unwrap_or(MAX_BY_QUERY_ROWS).min(MAX_BY_QUERY_ROWS) as usize
            }
            _ => 1,
        })
        .sum()
}
/// Approximate cron re-fire interval for the in-memory stub. Real 5-field cron
/// parsing is deferred to the server; the harness only needs crons to re-arm.
pub const CRON_STEP_MS: i64 = 60_000;

/// A stored row: the user doc plus its identity/history, kept separate so the
/// system fields (`_id`/`_creationTime`/`_version`) are merged in only at read
/// time — exactly as the server stores `doc` jsonb alongside `id`/`created_at`/
/// `version` columns.
#[derive(Debug, Clone)]
pub struct StoredRow {
    pub id: String,
    pub doc: Value,
    pub version: i64,
    pub created_at: i64,
}

/// A stored scheduled job in the in-memory harness. `tick` fires due non-paused
/// jobs by applying `txn` through the same atomic path as `mutate`. Ports the
/// `ScheduledJob` interface at `ts-client/src/in_memory.ts:75-85`.
#[derive(Debug, Clone)]
pub struct ScheduledJob {
    pub id: String,
    pub kind: ScheduleKind,
    pub txn: Transaction,
    pub due_at: i64,
    pub cron: Option<String>,
    pub status: ScheduleStatus,
    pub created_at: i64,
    pub fired_count: i64,
    pub last_error: Option<String>,
}

/// A stored file blob with its server-side metadata. Mirrors the TS
/// `{ bytes, contentType?, createdAt }` record (`ts-client/src/in_memory.ts:498-501`).
#[derive(Debug, Clone)]
pub struct StoredBlob {
    pub bytes: Vec<u8>,
    pub content_type: Option<String>,
    pub created_at: i64,
    pub sha256: String,
}

/// Result of [`InMemoryRtDbClient::upload`] — the server-computed file
/// identity, content hash, size in bytes, and (if the upload carried one) the
/// stored `contentType`. Mirrors [`crate::http::UploadResult`] (`http.rs:54`)
/// byte-for-byte but is defined locally so the `in_memory` feature does not
/// depend on the `http` feature.
#[derive(Debug, Clone)]
pub struct UploadResult {
    pub id: String,
    pub sha256: String,
    pub size: i64,
    pub content_type: Option<String>,
}

/// File metadata returned by [`InMemoryRtDbClient::get_file_metadata`]. Mirrors
/// [`crate::http::FileMetadata`] (`http.rs:66`) plus the server-recorded
/// `creation_time`; defined locally for the same feature-isolation reason as
/// [`UploadResult`].
#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub id: String,
    pub sha256: String,
    pub size: i64,
    pub content_type: Option<String>,
    pub creation_time: i64,
}

/// Inner state of one reactive subscription. The `callback` is wrapped in an
/// [`Arc`] so the returned [`SubscriptionHandle`] can clear it via the shared
/// `alive` flag without holding a borrow on the client. Ports the
/// `Subscription` interface at `ts-client/src/in_memory.ts:62-68`.
pub(crate) struct Subscription {
    pub query: Query,
    pub table: String,
    /// Cleared by the [`SubscriptionHandle`]'s Drop. notify_subs skips dead
    /// subscriptions (and lazily removes them from the vec).
    pub alive: Arc<AtomicBool>,
    pub callback: Arc<dyn Fn(Value) + Send + Sync>,
    /// Last delivered value (canonicalized) — only re-fires on a real change.
    /// Mutex is for interior mutability under `&self` notify; the harness is
    /// driven synchronously by tests so contention is not a concern.
    pub last: Mutex<Option<String>>,
}

/// Unsubscribe handle returned by [`InMemoryRtDbClient::subscribe`]. Dropping
/// it (or calling [`SubscriptionHandle::unsubscribe`]) clears the listener, so
/// no further updates fire — matching the TS `() => unsub()` contract.
pub struct SubscriptionHandle {
    alive: Arc<AtomicBool>,
}

/// Type alias for the subscriber callback — its raw form would otherwise trip
/// clippy's `type_complexity` lint where it's used in a tuple inside
/// [`InMemoryRtDbClient::notify_subs`].
type Listener = Arc<dyn Fn(Value) + Send + Sync>;

impl SubscriptionHandle {
    /// Detach the listener; equivalent to dropping the handle.
    pub fn unsubscribe(self) {
        // Drop runs the same clear; this method exists for explicit parity
        // with the TS `unsub()` call shape.
        self.alive.store(false, Ordering::SeqCst);
    }
}

impl Drop for SubscriptionHandle {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::SeqCst);
    }
}

/// Shared in-memory presence backing: a `room → connectionId → member` map with
/// a per-room subscriber list. Two [`InMemoryRtDbClient`]s that share a
/// `PresenceRooms` instance see each other's joins/updates/leaves fan out,
/// approximating the server's per-db presence registry for tests (one client,
/// one connection — exactly like the live server's per-ConnId keying). A client
/// with no `presence_rooms` option gets a private instance and only ever sees
/// itself in its rooms.
///
/// Ports `PresenceRooms` from `ts-client/src/in_memory.ts`. Mirrors the existing
/// harness pattern of `notify_subs(write_set)` fanning a recomputed snapshot to
/// every local subscriber after a write, including the `Arc<AtomicBool>` alive
/// flag that lets a dropped handle lazily unregister its callback.
#[derive(Default)]
pub struct PresenceRooms {
    /// `room → connectionId → member`. Inner vec preserves insertion order so
    /// `snapshot` returns members in join order (matching the TS `Map` iteration
    /// semantics).
    members: HashMap<String, Vec<(String, PresenceMember)>>,
    /// `room → list of (alive, callback)`. The alive flag is cleared by
    /// [`PresenceHandle`]'s Drop; fan_out skips and lazily compacts dead entries
    /// — the same pattern as [`InMemoryRtDbClient::notify_subs`].
    subs: HashMap<String, Vec<PresenceListener>>,
    /// `room → connectionId → expiresAt(ms)` for ENH-015 presence-ttl. A member
    /// with an entry here has its `state` cleared to null at the recorded
    /// instant by [`expire`](Self::expire) (the member stays listed). Mirrors
    /// the ts harness's parallel `expiry` map — kept separate from
    /// [`PresenceMember`] because that struct is the server→client snapshot
    /// shape and must stay byte-identical with the server.
    expiry: HashMap<String, HashMap<String, i64>>,
}

/// One registered presence callback + its alive flag.
struct PresenceListener {
    alive: Arc<AtomicBool>,
    callback: Arc<dyn Fn(Vec<PresenceMember>) + Send + Sync>,
}

/// Handle returned by [`PresenceRooms::subscribe`]. Dropping it (or calling
/// [`unsubscribe`](Self::unsubscribe)) clears the callback so no further fan-outs
/// fire — matching the TS `() => unsub()` contract. Mirrors [`SubscriptionHandle`].
#[derive(Clone)]
pub struct PresenceHandle {
    alive: Arc<AtomicBool>,
}

impl PresenceHandle {
    /// Detach the listener; equivalent to dropping the handle.
    pub fn unsubscribe(self) {
        self.alive.store(false, Ordering::SeqCst);
    }
}

impl Drop for PresenceHandle {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::SeqCst);
    }
}

impl PresenceRooms {
    /// Returns a stable-order snapshot of `room`'s current members (join order).
    pub fn snapshot(&self, room: &str) -> Vec<PresenceMember> {
        self.members
            .get(room)
            .map(|entries| entries.iter().map(|(_, m)| m.clone()).collect())
            .unwrap_or_default()
    }

    /// Adds or replaces `member` in `room` and fans out a fresh snapshot.
    pub fn join(&mut self, room: &str, member: PresenceMember) {
        let entries = self.members.entry(room.to_string()).or_default();
        if let Some((_, existing)) = entries
            .iter_mut()
            .find(|(id, _)| *id == member.connection_id)
        {
            *existing = member;
        } else {
            entries.push((member.connection_id.clone(), member));
        }
        self.fan_out(room);
    }

    /// Updates `connection_id`'s state in `room` and fans out. No-op if the
    /// connection is not in the room (matches the live server, which would not
    /// relay an update for a non-member). When `ttl_ms` is `Some(n)` with `n > 0`,
    /// schedules an expiry sweep that nulls this member's `state` at `now + n`
    /// (the member stays listed); `None` clears any pending expiry, mirroring the
    /// live server's "ttlMs after the last refresh" semantics. `Some(<=0)` is
    /// treated as `None` (no expiry) — a permissive offline approximation; the
    /// LIVE SERVER rejects ttl_ms <= 0 with BAD_REQUEST. Mirrors TS
    /// `PresenceRooms.update` (ENH-015 presence-ttl).
    pub fn update(
        &mut self,
        room: &str,
        connection_id: &str,
        state: Value,
        ttl_ms: Option<u64>,
        now: i64,
    ) {
        let Some(entries) = self.members.get_mut(room) else {
            return;
        };
        let Some((_, member)) = entries.iter_mut().find(|(id, _)| id == connection_id) else {
            return;
        };
        member.state = state;
        let exp = self.expiry.entry(room.to_string()).or_default();
        match ttl_ms {
            Some(n) if n > 0 => {
                exp.insert(connection_id.to_string(), now + n as i64);
            }
            _ => {
                exp.remove(connection_id);
            }
        }
        if exp.is_empty() {
            self.expiry.remove(room);
        }
        self.fan_out(room);
    }

    /// Removes `connection_id` from `room` and fans out. No-op if absent. Also
    /// clears any pending expiry entry so a re-join doesn't inherit a stale ttl.
    pub fn leave(&mut self, room: &str, connection_id: &str) {
        let Some(entries) = self.members.get_mut(room) else {
            return;
        };
        let before = entries.len();
        entries.retain(|(id, _)| id != connection_id);
        if entries.len() == before {
            return; // was not a member — no fan-out
        }
        if entries.is_empty() {
            self.members.remove(room);
        }
        if let Some(exp) = self.expiry.get_mut(room) {
            exp.remove(connection_id);
            if exp.is_empty() {
                self.expiry.remove(room);
            }
        }
        self.fan_out(room);
    }

    /// Clears expired members' `state` to `Value::Null` (the member stays
    /// listed) and fans out each touched room once. Returns `true` if anything
    /// expired. Mirrors the live server's per-connection ttl clearing
    /// (`server::presence::expire_once`) and the TS harness's `expire`. Idempotent:
    /// a second sweep with the same `now` is a no-op (the expiry entries were
    /// already drained).
    pub fn expire(&mut self, now: i64) -> bool {
        let mut any = false;
        let mut touched: Vec<String> = Vec::new();
        // Drain the rooms that currently have an expiry map. We can't mutate
        // `self.members` while iterating `self.expiry`, so collect the work
        // first, then apply.
        let rooms_with_expiry: Vec<String> = self.expiry.keys().cloned().collect();
        for room in rooms_with_expiry {
            let Some(exp) = self.expiry.get_mut(&room) else {
                continue;
            };
            let due: Vec<String> = exp
                .iter()
                .filter(|(_, at)| **at <= now)
                .map(|(id, _)| id.clone())
                .collect();
            if due.is_empty() {
                continue;
            }
            // Drop the expired entries from the expiry map.
            for id in &due {
                exp.remove(id);
            }
            if exp.is_empty() {
                self.expiry.remove(&room);
            }
            // Null the state of each due member in the room's member list.
            if let Some(entries) = self.members.get_mut(&room) {
                let mut room_touched = false;
                for id in &due {
                    if let Some((_, member)) = entries.iter_mut().find(|(cid, _)| cid == id) {
                        member.state = Value::Null;
                        any = true;
                        room_touched = true;
                    }
                }
                if room_touched {
                    touched.push(room);
                }
            }
        }
        for room in &touched {
            self.fan_out(room);
        }
        any
    }

    /// Registers `cb` for `room` snapshots and immediately fires it with the
    /// current snapshot (mirroring the server's first `presenceSnapshot` on
    /// join). Returns a [`PresenceHandle`] whose Drop clears the callback.
    pub fn subscribe<F>(&mut self, room: &str, cb: F) -> PresenceHandle
    where
        F: Fn(Vec<PresenceMember>) + Send + Sync + 'static,
    {
        let alive = Arc::new(AtomicBool::new(true));
        let callback: Arc<dyn Fn(Vec<PresenceMember>) + Send + Sync> = Arc::new(cb);
        let initial = self.snapshot(room);
        self.subs
            .entry(room.to_string())
            .or_default()
            .push(PresenceListener {
                alive: alive.clone(),
                callback: callback.clone(),
            });
        callback(initial);
        PresenceHandle { alive }
    }

    /// Re-snapshots `room` and fires every live callback. Lazily compacts dead
    /// listeners (handle dropped → alive=false), mirroring `notify_subs`.
    fn fan_out(&mut self, room: &str) {
        let snap = self.snapshot(room);
        let mut fires: Vec<Arc<dyn Fn(Vec<PresenceMember>) + Send + Sync>> = Vec::new();
        if let Some(listeners) = self.subs.get_mut(room) {
            // Collect live callbacks, then compact.
            for l in listeners.iter() {
                if l.alive.load(Ordering::SeqCst) {
                    fires.push(l.callback.clone());
                }
            }
            listeners.retain(|l| l.alive.load(Ordering::SeqCst));
            if listeners.is_empty() {
                self.subs.remove(room);
            }
        }
        for cb in fires {
            cb(snap.clone());
        }
    }
}

/// Injectable clock and RNG for deterministic id minting and `_creationTime`.
///
/// Mirrors `InMemoryRtDbClientOptions` in `ts-client/src/in_memory.ts:91-96`.
/// Both `now` and `random` are optional; `InMemoryRtDbClient::new` supplies
/// defaults (system clock for `now`, a constant `0.5` for `random` — tests that
/// need determinism should always inject both).
#[derive(Default)]
pub struct InMemoryRtDbClientOptions {
    now: Option<Arc<dyn Fn() -> i64 + Send + Sync>>,
    random: Option<Arc<dyn Fn() -> f64 + Send + Sync>>,
    /// Stable identity for this client in presence rooms. Auto-generated as a
    /// `c{N}` counter when not set (mirrors the TS harness default).
    connection_id: Option<String>,
    /// Display identity stamped on this client's presence entries. Defaults to
    /// `{ kind: User }` (a nameless user) when not set.
    presence_user: Option<AuthedUser>,
    /// Optional shared presence backing. Two clients that pass the same
    /// `PresenceRooms` instance see each other's joins/updates/leaves; a client
    /// with no `presence_rooms` gets a private instance and sees only itself.
    presence_rooms: Option<Arc<Mutex<PresenceRooms>>>,
}

impl InMemoryRtDbClientOptions {
    /// Inject a clock (epoch millis) for deterministic `_creationTime` and id
    /// minting.
    pub fn now(mut self, f: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        self.now = Some(Arc::new(f));
        self
    }
    /// Inject an RNG in `[0, 1)` for deterministic id minting.
    pub fn random(mut self, f: impl Fn() -> f64 + Send + Sync + 'static) -> Self {
        self.random = Some(Arc::new(f));
        self
    }
    /// Set this client's stable identity in presence rooms.
    pub fn connection_id(mut self, id: impl Into<String>) -> Self {
        self.connection_id = Some(id.into());
        self
    }
    /// Set the display identity stamped on this client's presence entries.
    pub fn presence_user(mut self, user: AuthedUser) -> Self {
        self.presence_user = Some(user);
        self
    }
    /// Share a presence backing so two clients see each other's joins/leaves.
    /// The caller mints one with [`PresenceRooms::default`] and passes a clone
    /// of `Arc::new(Mutex::new(rooms))` to each client.
    pub fn presence_rooms(mut self, rooms: Arc<Mutex<PresenceRooms>>) -> Self {
        self.presence_rooms = Some(rooms);
        self
    }
}

/// In-memory par-rt-db client for unit tests. See the
/// [module docs](crate::in_memory) for the parity scope and deferred gaps.
pub struct InMemoryRtDbClient {
    now: Arc<dyn Fn() -> i64 + Send + Sync>,
    random: Arc<dyn Fn() -> f64 + Send + Sync>,
    schema: Option<SchemaDef>,
    /// Per-table schema defs, keyed by table name. Separate from `schema` so
    /// Task 2+'s hot paths (validate-on-write, table lookups) don't re-walk the
    /// whole schema.
    tables: HashMap<String, TableDef>,
    /// Document store keyed by `(table_name, id)` — flat representation of the
    /// TS `Map<string, Map<string, StoredRow>>`.
    docs: HashMap<(String, String), StoredRow>,
    /// Counter for storage-upload id minting (`f{++counter}` per
    /// `ts-client/src/in_memory.ts:647`).
    id_counter: u64,
    /// `mut_id` → cached results. `mutate` reads/writes it for its idempotency
    /// short-circuit; `push_schema` merges additively and does not clear it.
    idempotency: HashMap<String, Vec<StepResult>>,
    /// Scheduled jobs (one-shot + cron). `tick` drains due non-paused entries
    /// by re-running `txn` through `execute_transaction`.
    schedules: Vec<ScheduledJob>,
    /// Reactive subscriptions. notify_subs (called from `execute_transaction`)
    /// re-runs each affected query and fires its callback on a real change.
    subscribers: Vec<Arc<Subscription>>,
    /// Storage stub: per-id blobs with their bytes, content-type, creation
    /// time, and SHA-256. Mirrors the TS `files: Map<...>`.
    storage: HashMap<String, StoredBlob>,
    /// Shared presence backing. Wrapped in `Arc<Mutex>` so two clients that
    /// pass the same instance see each other's joins/leaves.
    presence_rooms: Arc<Mutex<PresenceRooms>>,
    /// Display identity stamped on this client's presence entries.
    presence_user: AuthedUser,
    /// This client's stable identity in presence rooms. Generated as a `c{N}`
    /// counter when not injected.
    connection_id: String,
    /// Rooms this client has joined (for `update_presence`/`leave_presence`).
    joined_rooms: BTreeSet<String>,
    /// Unsubscribe handles for presence callbacks this client registered, keyed
    /// by room. Tracked so `leave_presence` can drop every local subscriber.
    presence_unsubs: HashMap<String, Vec<PresenceHandle>>,
}

impl InMemoryRtDbClient {
    /// Construct a new harness. `options.now` and `options.random` default to
    /// the system clock and a constant `0.5` respectively; tests that need
    /// deterministic ids/timestamps should always inject both.
    pub fn new(options: InMemoryRtDbClientOptions) -> Self {
        let mut id_counter = 0u64;
        let connection_id = options.connection_id.unwrap_or_else(|| {
            id_counter += 1;
            format!("c{id_counter}")
        });
        Self {
            now: options.now.unwrap_or_else(|| {
                Arc::new(|| {
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0)
                })
            }),
            random: options.random.unwrap_or_else(|| Arc::new(|| 0.5)),
            schema: None,
            tables: HashMap::new(),
            docs: HashMap::new(),
            id_counter,
            idempotency: HashMap::new(),
            schedules: Vec::new(),
            subscribers: Vec::new(),
            storage: HashMap::new(),
            presence_rooms: options
                .presence_rooms
                .unwrap_or_else(|| Arc::new(Mutex::new(PresenceRooms::default()))),
            presence_user: options.presence_user.unwrap_or(AuthedUser {
                kind: crate::wire::UserKind::User,
                email: None,
                name: None,
                github_login: None,
                github_id: None,
            }),
            connection_id,
            joined_rooms: BTreeSet::new(),
            presence_unsubs: HashMap::new(),
        }
    }

    /// Installs `schema` as this client's sole in-memory database schema,
    /// merging additively on subsequent pushes: existing docs and idempotency
    /// entries are preserved, and `self.tables` is repopulated from the new
    /// schema (folding in new fields/indexes/tables without touching rows).
    /// Destructive changes — a removed/changed table, field, or index — return
    /// [`ErrorCode::BadRequest`] with the same messages as the live server's
    /// `ddl.rs::detect_destructive_changes`.
    ///
    /// Ports `pushSchema` in `ts-client/src/in_memory.ts:512-519`. The Rust
    /// signature takes the typed [`SchemaDef`] directly (no `toSchemaJson`
    /// conversion needed since the builder already produces the wire shape).
    pub fn push_schema(&mut self, schema: &SchemaDef) -> Result<(), RtDbError> {
        if let Some(prev) = &self.schema {
            detect_destructive_changes(prev, schema)?;
        }
        self.schema = Some(schema.clone());
        for (name, def) in &schema.tables {
            self.tables.insert(name.clone(), def.clone());
        }
        Ok(())
    }

    /// Applies (or previews) a declarative schema migration — a port of server
    /// `migrate::plan_migration` (validation + structural schema fold) and
    /// `migrate::apply_migration` (data effects). Structural directives fold
    /// into a working copy of the schema; data directives rewrite the in-memory
    /// doc map to match so subsequent reads stay consistent.
    ///
    /// A failed directive is atomic: every earlier structural and data effect
    /// rolls back via snapshot/restore (the working schema copy was never
    /// installed, and the doc store is restored wholesale). With `dry_run`, the
    /// full plan is validated and `affected_rows` reported against the derived
    /// schema, but nothing is committed (`applied: false`).
    ///
    /// `evalExpr` has no in-memory SQL engine and returns
    /// `BAD_REQUEST` — same convention as the search/vector stubs.
    /// Affected-rows counts mirror the server and the ts-client harness:
    /// `renameField`/`setDefault`/`changeType`/`dropField` count the rows whose
    /// docs actually changed; `dropTable` counts every row (all deleted);
    /// `renameTable`/`dropIndex` report zero.
    #[cfg(feature = "admin")]
    pub fn migrate_schema(
        &mut self,
        directives: &[crate::wire::admin::Directive],
        dry_run: bool,
    ) -> Result<crate::wire::admin::MigrateResult, RtDbError> {
        use crate::wire::admin::{DirectiveReport, MigrateResult};

        let old = self.schema.clone().ok_or_else(|| {
            RtDbError::new(ErrorCode::BadRequest, "no schema pushed for migration")
        })?;
        let mut planned = old;
        let mut touched: BTreeSet<String> = BTreeSet::new();
        let mut reports: Vec<DirectiveReport> = Vec::with_capacity(directives.len());
        let snapshot = self.snapshot_docs();

        // Apply each directive against the working `planned` schema (structural)
        // and `self.docs` (data). On the first failure, restore the doc store
        // and surface the error — `self.schema`/`self.tables` were never touched
        // (the fold lived in `planned`), so nothing else needs rolling back.
        for d in directives {
            match self.apply_migration_directive(&mut planned, d) {
                Ok((report, table)) => {
                    reports.push(report);
                    if let Some(t) = table {
                        touched.insert(t);
                    }
                }
                Err(e) => {
                    self.restore_docs(snapshot);
                    return Err(e);
                }
            }
        }

        if dry_run {
            // Preview only: discard the data effects, return the derived schema.
            self.restore_docs(snapshot);
            return Ok(MigrateResult {
                applied: false,
                schema: planned,
                directives: reports,
            });
        }

        // Commit the folded schema and rebuild the denormalized table map so
        // subsequent reads see the new shape (mirrors `push_schema`).
        self.schema = Some(planned.clone());
        self.tables.clear();
        for (name, def) in &planned.tables {
            self.tables.insert(name.clone(), def.clone());
        }
        self.notify_subs(&touched);
        Ok(MigrateResult {
            applied: true,
            schema: planned,
            directives: reports,
        })
    }

    #[cfg(feature = "admin")]
    fn apply_migration_directive(
        &mut self,
        planned: &mut SchemaDef,
        d: &crate::wire::admin::Directive,
    ) -> Result<(crate::wire::admin::DirectiveReport, Option<String>), RtDbError> {
        use crate::wire::admin::{Directive, DirectiveReport};
        match d {
            Directive::RenameField { table, from, to } => {
                let t = migrate_table_mut(planned, table)?;
                if t.fields.contains_key(to) {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        format!("rename target '{table}.{to}' already exists"),
                    ));
                }
                let ft = t.fields.remove(from).ok_or_else(|| {
                    RtDbError::new(
                        ErrorCode::BadRequest,
                        format!("renamed field '{table}.{from}' does not exist"),
                    )
                })?;
                t.fields.insert(to.clone(), ft);
                if let Some(indexes) = t.indexes.as_mut() {
                    for ix in indexes.iter_mut() {
                        for f in ix.fields.iter_mut() {
                            if f == from {
                                *f = to.clone();
                            }
                        }
                    }
                }
                if t.owner_field.as_deref() == Some(from.as_str()) {
                    t.owner_field = Some(to.clone());
                }
                if t.collaborators_field.as_deref() == Some(from.as_str()) {
                    t.collaborators_field = Some(to.clone());
                }
                let mut affected = 0i64;
                for ((tname, _), row) in self.docs.iter_mut() {
                    if tname != table {
                        continue;
                    }
                    if let Some(obj) = row.doc.as_object_mut()
                        && let Some(v) = obj.remove(from)
                    {
                        obj.insert(to.clone(), v);
                        affected += 1;
                    }
                }
                Ok((
                    DirectiveReport {
                        op: "renameField".into(),
                        affected_rows: affected,
                        ..Default::default()
                    },
                    Some(table.clone()),
                ))
            }
            Directive::RenameTable { from, to } => {
                if planned.tables.contains_key(to) {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        format!("rename target table '{to}' already exists"),
                    ));
                }
                let def = planned.tables.remove(from).ok_or_else(|| {
                    RtDbError::new(
                        ErrorCode::BadRequest,
                        format!("renamed table '{from}' does not exist"),
                    )
                })?;
                // Id references to `from` in other tables follow the rename.
                for other in planned.tables.values_mut() {
                    for ft in other.fields.values_mut() {
                        if let FieldType::Id { table } = ft
                            && table == from
                        {
                            *table = to.clone();
                        }
                    }
                }
                planned.tables.insert(to.clone(), def);
                // Rename the live doc keys `(from, id)` → `(to, id)`.
                let ids: Vec<String> = self
                    .docs
                    .keys()
                    .filter_map(|(t, id)| if t == from { Some(id.clone()) } else { None })
                    .collect();
                for id in ids {
                    if let Some(row) = self.docs.remove(&(from.clone(), id.clone())) {
                        self.docs.insert((to.clone(), id), row);
                    }
                }
                Ok((
                    DirectiveReport {
                        op: "renameTable".into(),
                        affected_rows: 0,
                        ..Default::default()
                    },
                    Some(to.clone()),
                ))
            }
            Directive::ChangeType {
                table,
                field,
                to,
                cast,
                default,
            } => {
                let t = migrate_table_mut(planned, table)?;
                let old_ty = t.fields.get(field).ok_or_else(|| {
                    RtDbError::new(
                        ErrorCode::BadRequest,
                        format!("changed field '{table}.{field}' does not exist"),
                    )
                })?;
                if !cast_valid_for(*cast, old_ty) {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        format!("cast {cast:?} is not valid for {table}.{field}"),
                    ));
                }
                // Drop the immutable borrow of `old_ty` before mutating `self.docs`.
                let field_owned = field.clone();
                let mut affected = 0i64;
                for ((tname, _), row) in self.docs.iter_mut() {
                    if tname != table {
                        continue;
                    }
                    let Some(obj) = row.doc.as_object_mut() else {
                        continue;
                    };
                    let Some(val) = obj.get(&field_owned).cloned() else {
                        continue;
                    };
                    affected += 1;
                    if let Some(coerced) = coerce_value(*cast, &val) {
                        obj.insert(field_owned.clone(), coerced);
                    } else if let Some(d) = default {
                        let dv = coerce_value(*cast, d).unwrap_or_else(|| d.clone());
                        obj.insert(field_owned.clone(), dv);
                    } else {
                        return Err(RtDbError::new(
                            ErrorCode::BadRequest,
                            format!(
                                "changeType cannot coerce value in {table}.{} ({val}) and no default given",
                                row.id
                            ),
                        ));
                    }
                }
                // Fold the new type into the planned schema (field is guaranteed
                // present by the lookup above).
                t.fields.insert(field_owned, to.clone());
                Ok((
                    DirectiveReport {
                        op: "changeType".into(),
                        affected_rows: affected,
                        ..Default::default()
                    },
                    Some(table.clone()),
                ))
            }
            Directive::DropField { table, field } => {
                let t = migrate_table_mut(planned, table)?;
                if t.fields.remove(field).is_none() {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        format!("dropped field '{table}.{field}' does not exist"),
                    ));
                }
                if let Some(indexes) = t.indexes.as_mut() {
                    for ix in indexes.iter_mut() {
                        ix.fields.retain(|f| f != field);
                    }
                }
                if t.owner_field.as_deref() == Some(field.as_str()) {
                    t.owner_field = None;
                }
                if t.collaborators_field.as_deref() == Some(field.as_str()) {
                    t.collaborators_field = None;
                }
                // `affected_rows` counts only rows whose `doc` actually changes
                // (rows carrying the field) — server parity. `obj.remove` returns
                // the removed value, so count the row iff the key was present.
                let mut affected = 0i64;
                for ((tname, _), row) in self.docs.iter_mut() {
                    if tname != table {
                        continue;
                    }
                    if let Some(obj) = row.doc.as_object_mut()
                        && obj.remove(field).is_some()
                    {
                        affected += 1;
                    }
                }
                Ok((
                    DirectiveReport {
                        op: "dropField".into(),
                        affected_rows: affected,
                        ..Default::default()
                    },
                    Some(table.clone()),
                ))
            }
            Directive::DropTable { name } => {
                if planned.tables.remove(name).is_none() {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        format!("dropped table '{name}' does not exist"),
                    ));
                }
                let to_remove: Vec<String> = self
                    .docs
                    .keys()
                    .filter_map(|(t, id)| if t == name { Some(id.clone()) } else { None })
                    .collect();
                let affected = to_remove.len() as i64;
                for id in to_remove {
                    self.docs.remove(&(name.clone(), id));
                }
                Ok((
                    DirectiveReport {
                        op: "dropTable".into(),
                        affected_rows: affected,
                        ..Default::default()
                    },
                    Some(name.clone()),
                ))
            }
            Directive::DropIndex { table, name } => {
                let t = migrate_table_mut(planned, table)?;
                let indexes = t.indexes.as_mut().ok_or_else(|| {
                    RtDbError::new(
                        ErrorCode::BadRequest,
                        format!("dropped index '{table}.{name}' does not exist"),
                    )
                })?;
                if !indexes.iter().any(|ix| &ix.name == name) {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        format!("dropped index '{table}.{name}' does not exist"),
                    ));
                }
                indexes.retain(|ix| &ix.name != name);
                Ok((
                    DirectiveReport {
                        op: "dropIndex".into(),
                        affected_rows: 0,
                        ..Default::default()
                    },
                    Some(table.clone()),
                ))
            }
            Directive::SetDefault {
                table,
                field,
                value,
            } => {
                let t = migrate_table_mut(planned, table)?;
                if !t.fields.contains_key(field) {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        format!("setDefault target '{table}.{field}' does not exist"),
                    ));
                }
                let mut affected = 0i64;
                for ((tname, _), row) in self.docs.iter_mut() {
                    if tname != table {
                        continue;
                    }
                    if let Some(obj) = row.doc.as_object_mut()
                        && !obj.contains_key(field)
                    {
                        obj.insert(field.clone(), value.clone());
                        affected += 1;
                    }
                }
                Ok((
                    DirectiveReport {
                        op: "setDefault".into(),
                        affected_rows: affected,
                        ..Default::default()
                    },
                    Some(table.clone()),
                ))
            }
            Directive::EvalExpr { table, .. } => Err(RtDbError::new(
                ErrorCode::BadRequest,
                format!("evalExpr unsupported in-memory (table '{table}')"),
            )),
        }
    }

    /// Snapshot of the currently-installed schema (or `None` before
    /// `push_schema`). Returns a clone so callers can freely inspect/mutate.
    pub fn to_schema_json(&self) -> Option<SchemaDef> {
        self.schema.clone()
    }

    /// Minimal point read — returns the merged doc (system fields included) for
    /// `(table, id)`, or `None` if absent. Mirrors the server's `get(id)` read
    /// semantics. The full query DSL (`withIndex`, `order`, `take`, `filter`, …)
    /// lands in Task 3; tests that need a quick read use this until then.
    pub fn get(&self, table: &str, id: &str) -> Option<Value> {
        self.docs
            .get(&(table.to_string(), id.to_string()))
            .map(merge_doc)
    }

    /// Test/debug helper — every merged doc in `table`, in unspecified order.
    /// Not part of the query DSL; Task 3 replaces callers with proper queries.
    pub fn collect_all(&self, table: &str) -> Vec<Value> {
        self.docs
            .iter()
            .filter(|((t, _), _)| t == table)
            .map(|(_, row)| merge_doc(row))
            .collect()
    }

    /// One-shot query — ports `executeQuery` (`ts-client/src/in_memory.ts:889-1151`).
    /// Returns the terminal result as a [`Value`]:
    /// - `get(id)` / `first` → merged doc, or [`Value::Null`] when absent.
    /// - `unique` → merged doc, or `PRECONDITION_FAILED` when more than one row
    ///   matches (and [`Value::Null`] when zero match).
    /// - `count` → number of matching rows.
    /// - `take` / `collect` → array of merged docs.
    /// - `search` / `vector_search` → empty array (no in-memory ranking; the
    ///   guards still reject conflicting combinations so the cascade agrees with
    ///   the server).
    ///
    /// The harness is in-process — no `{result}` wire envelope; callers either
    /// match on the [`Value`] directly or use [`run`](Self::run) for typed
    /// deserialization.
    ///
    /// `filter` is structurally validated against the table's declared fields
    /// once up front (via [`validate_filter`], mirroring the server's
    /// compile-then-execute order), then evaluated per row via
    /// [`eval_filter_expr`]. `paginate` returns the wire `Paginated<T>` shape
    /// (`{docs, nextCursor?}`) via keyset-cursor paging over the sorted set;
    /// its combination guards reject `count`/`unique`/`first`/`take`.
    pub fn run_query(&self, q: &Query) -> Result<Value, RtDbError> {
        let table_def = self.require_table(&q.table)?.clone();
        let eq = &q.eq;
        let has_range = q.gt.is_some() || q.gte.is_some() || q.lt.is_some() || q.lte.is_some();

        // `get` terminal — exclusive of every other clause.
        if let Some(id) = &q.get {
            if q.index.is_some()
                || !eq.is_empty()
                || has_range
                || q.order.is_some()
                || q.take.is_some()
                || q.unique
                || q.first
                || q.count
                || q.distinct
                || q.aggregate.is_some()
                || q.paginate.is_some()
                || q.filter.is_some()
                || q.search.is_some()
                || q.vector_search.is_some()
            {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "get cannot be combined with index, eq, range bounds, order, take, \
                     unique, first, count, distinct, aggregate, paginate, filter, search, \
                     or vector search",
                ));
            }
            // The DSL `get` terminal reuses the point-read primitive so the
            // system-field merge path is shared with the Task 2 helper.
            return Ok(self.get(&q.table, id).unwrap_or(Value::Null));
        }

        // Conflicting-terminal guards (ports :919-939).
        if q.unique
            && (q.take.is_some() || q.order.is_some() || q.distinct || q.aggregate.is_some())
        {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "unique cannot be combined with take, order, distinct, or aggregate",
            ));
        }
        if q.first && q.unique {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "first cannot be combined with unique",
            ));
        }
        if q.first && q.take.is_some() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "first cannot be combined with take",
            ));
        }
        if q.first && q.distinct {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "first cannot be combined with distinct",
            ));
        }
        if q.first && q.aggregate.is_some() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "first cannot be combined with aggregate",
            ));
        }
        if q.count && q.unique {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "count cannot be combined with unique",
            ));
        }
        if q.count && q.take.is_some() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "count cannot be combined with take",
            ));
        }
        if q.count && q.first {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "count cannot be combined with first",
            ));
        }
        if q.count && q.order.is_some() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "count cannot be combined with order",
            ));
        }
        if q.count && q.distinct {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "count cannot be combined with distinct",
            ));
        }
        if q.count && q.aggregate.is_some() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "count cannot be combined with aggregate",
            ));
        }
        // Paginate combination guards (ports `:940-955`): paginate is one-shot
        // paging, so it cannot also narrow to count/unique/first/take. (`get`
        // is rejected above; `order`, index, eq, and range bounds are allowed.)
        if q.paginate.is_some() {
            if q.count {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "paginate cannot be combined with count",
                ));
            }
            if q.unique {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "paginate cannot be combined with unique",
                ));
            }
            if q.first {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "paginate cannot be combined with first",
                ));
            }
            if q.take.is_some() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "paginate cannot be combined with take",
                ));
            }
        }
        if q.gt.is_some() && q.gte.is_some() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "gt and gte cannot both be set",
            ));
        }
        if q.lt.is_some() && q.lte.is_some() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "lt and lte cannot both be set",
            ));
        }
        if q.take.is_some_and(|t| t as usize > MAX_TAKE) {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                format!("take exceeds maximum of {MAX_TAKE}"),
            ));
        }

        // `distinct`/`aggregate` are standalone terminals (like `count`): they
        // compose only with index/eq/range/filter. `get`/`unique`/`first`/`count`
        // rejected their own combinations above (validated first, matching the
        // server's check order in query.rs), so these blocks only reject the
        // remaining peers each terminal owns — mirroring the server's
        // DISTINCT/AGGREGATE_INCOMPATIBLES tables.
        if q.distinct {
            if q.take.is_some() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "distinct cannot be combined with take",
                ));
            }
            if q.order.is_some() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "distinct cannot be combined with order",
                ));
            }
            if q.aggregate.is_some() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "distinct cannot be combined with aggregate",
                ));
            }
            if q.paginate.is_some() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "distinct cannot be combined with paginate",
                ));
            }
            if q.search.is_some() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "distinct cannot be combined with search",
                ));
            }
            if q.vector_search.is_some() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "distinct cannot be combined with vector search",
                ));
            }
            if q.hybrid_search.is_some() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "distinct cannot be combined with hybrid search",
                ));
            }
        }
        if q.aggregate.is_some() {
            if q.take.is_some() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "aggregate cannot be combined with take",
                ));
            }
            if q.order.is_some() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "aggregate cannot be combined with order",
                ));
            }
            if q.paginate.is_some() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "aggregate cannot be combined with paginate",
                ));
            }
            if q.search.is_some() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "aggregate cannot be combined with search",
                ));
            }
            if q.vector_search.is_some() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "aggregate cannot be combined with vector search",
                ));
            }
            if q.hybrid_search.is_some() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "aggregate cannot be combined with hybrid search",
                ));
            }
        }

        // `vectorSearch` terminal — cascade mirror of server `execute_query`.
        // No in-memory ranking; return an empty array so the cascade agrees
        // with the server without silently misranking by falling through to
        // the collect path. A carried `filter` (the db-side `FilterExpr` DSL)
        // is validated against the table's declared fields and run through
        // `matches_filter` on the (empty) candidate set — same narrowing path
        // as the `search` terminal, exercised even though the stub result set
        // stays empty.
        if let Some(vector) = &q.vector_search {
            if q.index.is_some()
                || !eq.is_empty()
                || has_range
                || q.order.is_some()
                || q.unique
                || q.first
                || q.count
                || q.filter.is_some()
                || q.search.is_some()
                || q.take.is_some()
            {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "vectorSearch cannot be combined with any other terminal",
                ));
            }
            if let Some(filter) = &vector.filter {
                let fields: BTreeSet<String> = table_def.fields.keys().cloned().collect();
                validate_filter(filter, &fields)?;
            }
            let mut rows: Vec<Value> = Vec::new();
            if let Some(filter) = &vector.filter {
                rows.retain(|d| matches_filter(filter, d));
            }
            return Ok(Value::Array(rows));
        }

        // `search` terminal — same reasoning as `vectorSearch`: no in-memory
        // ts_rank, but the guard exists so invalid combinations fail here
        // instead of silently returning an unranked result. A carried `filter`
        // (the db-side `FilterExpr` DSL) is validated against the table's
        // declared fields and run through `matches_filter`, so the narrowing
        // path is exercised even though the stub result set stays empty.
        if let Some(search) = &q.search {
            if q.index.is_some()
                || !eq.is_empty()
                || has_range
                || q.order.is_some()
                || q.unique
                || q.first
                || q.count
                || q.filter.is_some()
                || q.vector_search.is_some()
            {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "search cannot be combined with index, eq, range bounds, order, \
                     unique, first, count, filter, or vector search",
                ));
            }
            if let Some(filter) = &search.filter {
                let fields: BTreeSet<String> = table_def.fields.keys().cloned().collect();
                validate_filter(filter, &fields)?;
            }
            let mut rows: Vec<Value> = Vec::new();
            if let Some(filter) = &search.filter {
                rows.retain(|d| matches_filter(filter, d));
            }
            return Ok(Value::Array(rows));
        }

        // Resolve index — required for `eq` and for any range bound.
        let index_def: Option<IndexDef> = match &q.index {
            Some(name) => Some(require_index(&table_def, name)?.clone()),
            None if !eq.is_empty() => {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "eq requires an index",
                ));
            }
            _ => None,
        };

        // eq-arity check (server `eq_binds` length guard at :1033-1038).
        if let Some(idx) = &index_def
            && eq.len() > idx.fields.len()
        {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                format!(
                    "index '{}' expects at most {} eq value(s), got {}",
                    idx.name,
                    idx.fields.len(),
                    eq.len()
                ),
            ));
        }

        // Type-check each eq prefix bind positionally.
        let typed_eq: Vec<Value> = match &index_def {
            Some(idx) => {
                let mut out = Vec::with_capacity(eq.len());
                for (i, value) in eq.iter().enumerate() {
                    out.push(coerce_index_value(&table_def, &idx.fields[i], value)?);
                }
                out
            }
            None => Vec::new(),
        };

        // Range bounds apply to the next index field after the eq prefix.
        let range_field: Option<&str> = if has_range {
            let idx = index_def.as_ref().ok_or_else(|| {
                RtDbError::new(ErrorCode::BadRequest, "range bound requires an index")
            })?;
            if eq.len() >= idx.fields.len() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "range bound requires a remaining index field after eq",
                ));
            }
            Some(idx.fields[eq.len()].as_str())
        } else {
            None
        };
        // The range field's storage type selects the comparison domain for the
        // bound checks below (int64 sorts numerically). `coerce_index_value`
        // already validated indexability when binding each bound, so the lookup
        // is guaranteed to succeed; the `Text` fallback is purely defensive.
        let range_field_pg: PgType = match range_field {
            Some(f) => table_def
                .fields
                .get(f)
                .and_then(|ty| index_column_type(ty).ok())
                .map(|it| it.pg)
                .unwrap_or(PgType::Text),
            None => PgType::Text,
        };
        let gt = match (&q.gt, range_field) {
            (Some(v), Some(f)) => Some(coerce_index_value(&table_def, f, v)?),
            _ => None,
        };
        let gte = match (&q.gte, range_field) {
            (Some(v), Some(f)) => Some(coerce_index_value(&table_def, f, v)?),
            _ => None,
        };
        let lt = match (&q.lt, range_field) {
            (Some(v), Some(f)) => Some(coerce_index_value(&table_def, f, v)?),
            _ => None,
        };
        let lte = match (&q.lte, range_field) {
            (Some(v), Some(f)) => Some(coerce_index_value(&table_def, f, v)?),
            _ => None,
        };

        // Compile the filter against the table's declared fields once up front,
        // mirroring the server's compile-then-execute order. Surfaces the
        // BAD_REQUEST cases (unknown field, empty and/or/in, mixed-type `in`
        // values, wrong value-kind) before any row is touched.
        if let Some(filter) = &q.filter {
            let fields: BTreeSet<String> = table_def.fields.keys().cloned().collect();
            validate_filter(filter, &fields)?;
        }

        // Row fetch + filter (eq prefix → range → filter hook).
        let mut filtered: Vec<StoredRow> = Vec::new();
        for ((t, _id), row) in &self.docs {
            if t != &q.table {
                continue;
            }
            if let Some(idx) = &index_def {
                let mut ok = true;
                for (i, tv) in typed_eq.iter().enumerate() {
                    match row.doc.get(&idx.fields[i]) {
                        Some(v) if !v.is_null() && v == tv => {}
                        _ => {
                            ok = false;
                            break;
                        }
                    }
                }
                if !ok {
                    continue;
                }
            }
            if let Some(field) = range_field {
                let v = match row.doc.get(field) {
                    Some(v) if !v.is_null() => v,
                    _ => continue,
                };
                if let Some(bound) = &gt
                    && compare_index_values(v, bound, range_field_pg) != std::cmp::Ordering::Greater
                {
                    continue;
                }
                if let Some(bound) = &gte
                    && compare_index_values(v, bound, range_field_pg) == std::cmp::Ordering::Less
                {
                    continue;
                }
                if let Some(bound) = &lt
                    && compare_index_values(v, bound, range_field_pg) != std::cmp::Ordering::Less
                {
                    continue;
                }
                if let Some(bound) = &lte
                    && compare_index_values(v, bound, range_field_pg) == std::cmp::Ordering::Greater
                {
                    continue;
                }
            }
            if let Some(expr) = &q.filter
                && !matches_filter(expr, &row.doc)
            {
                continue;
            }
            filtered.push(row.clone());
        }

        // `count` short-circuits before the sort (the count is the cardinality
        // of the filtered set, regardless of ordering).
        if q.count {
            return Ok(Value::Number(serde_json::Number::from(
                filtered.len() as i64
            )));
        }

        // `distinct` terminal: unique values of the index field immediately
        // after the eq prefix over the matching set, sorted ascending, capped by
        // MAX_TAKE. Ports ts `executeQuery` :1355-1382 and the server's distinct
        // arm. Null index values are skipped (mirror `WHERE "<col>" IS NOT NULL`).
        if q.distinct {
            let idx = index_def.as_ref().ok_or_else(|| {
                RtDbError::new(
                    ErrorCode::BadRequest,
                    "distinct requires an index field beyond the eq prefix",
                )
            })?;
            if typed_eq.len() >= idx.fields.len() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "distinct requires an index field beyond the eq prefix",
                ));
            }
            let field = idx.fields[typed_eq.len()].as_str();
            let field_pg = table_def
                .fields
                .get(field)
                .and_then(|ty| index_column_type(ty).ok())
                .map(|it| it.pg)
                .unwrap_or(PgType::Text);
            let mut seen: BTreeSet<String> = BTreeSet::new();
            let mut values: Vec<Value> = Vec::new();
            for row in &filtered {
                let Some(v) = row.doc.get(field) else {
                    continue;
                };
                if v.is_null() {
                    continue;
                }
                // Canonical JSON key so equal scalars dedupe.
                if seen.insert(v.to_string()) {
                    values.push(v.clone());
                }
            }
            values.sort_by(|a, b| compare_index_values(a, b, field_pg));
            let out: Vec<Value> = values.into_iter().take(MAX_TAKE).collect();
            return Ok(Value::Array(out));
        }

        // `aggregate` terminal: <OP> over the index field after the eq prefix
        // (groupBy: group by that field, aggregate the next). Ports ts
        // `executeQuery` :1391-1462 and the server's aggregate arm. Null agg
        // values are skipped (SQL SUM/AVG/MIN/MAX ignore NULL); an empty scalar
        // set yields null, an empty group yields a null `value`. Group count is
        // capped by MAX_TAKE.
        if let Some(agg) = &q.aggregate {
            let idx = index_def.as_ref().ok_or_else(|| {
                RtDbError::new(
                    ErrorCode::BadRequest,
                    "aggregate requires an index field beyond the eq prefix",
                )
            })?;
            let eq_len = typed_eq.len();
            // `count` aggregates matching rows and consumes no aggregate field
            // (mirrors `server/src/query.rs::AggregateOp::needs_field`). Scalar
            // count = number of matching rows (0 if none, never null); grouped
            // count = the size of each group.
            if matches!(agg.op, AggregateOp::Count) {
                if agg.group_by {
                    let group_field = idx.fields.get(eq_len).ok_or_else(|| {
                        RtDbError::new(
                            ErrorCode::BadRequest,
                            "aggregate groupBy requires an index field beyond the eq prefix",
                        )
                    })?;
                    let group_field_pg = table_def
                        .fields
                        .get(group_field.as_str())
                        .and_then(|ty| index_column_type(ty).ok())
                        .map(|it| it.pg)
                        .unwrap_or(PgType::Text);
                    let mut groups: Vec<(Value, u64)> = Vec::new();
                    let mut group_index: HashMap<String, usize> = HashMap::new();
                    for row in &filtered {
                        let Some(k) = row.doc.get(group_field.as_str()) else {
                            continue;
                        };
                        if k.is_null() {
                            continue;
                        }
                        let key = k.to_string();
                        let i = match group_index.get(&key).copied() {
                            Some(i) => i,
                            None => {
                                let i = groups.len();
                                group_index.insert(key, i);
                                groups.push((k.clone(), 0));
                                i
                            }
                        };
                        groups[i].1 += 1;
                    }
                    let mut out: Vec<Value> = groups
                        .into_iter()
                        .map(|(k, count)| {
                            let mut obj = Map::new();
                            obj.insert("key".to_string(), k);
                            obj.insert(
                                "value".to_string(),
                                Value::Number(serde_json::Number::from(count)),
                            );
                            Value::Object(obj)
                        })
                        .collect();
                    out.sort_by(|a, b| compare_index_values(&a["key"], &b["key"], group_field_pg));
                    let out: Vec<Value> = out.into_iter().take(MAX_TAKE).collect();
                    return Ok(Value::Array(out));
                }
                // Scalar count: number of matching rows (0 if none, never null).
                return Ok(Value::Number(serde_json::Number::from(
                    filtered.len() as i64
                )));
            }
            let (group_field, agg_field) = if agg.group_by {
                if eq_len + 1 >= idx.fields.len() {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        "aggregate groupBy requires two index fields beyond the eq prefix",
                    ));
                }
                (
                    Some(idx.fields[eq_len].as_str()),
                    idx.fields[eq_len + 1].as_str(),
                )
            } else {
                if eq_len >= idx.fields.len() {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        "aggregate requires an index field beyond the eq prefix",
                    ));
                }
                (None, idx.fields[eq_len].as_str())
            };
            let agg_field_pg = table_def
                .fields
                .get(agg_field)
                .and_then(|ty| index_column_type(ty).ok())
                .map(|it| it.pg)
                .unwrap_or(PgType::Text);
            let op_name = match agg.op {
                AggregateOp::Sum => "sum",
                AggregateOp::Avg => "avg",
                AggregateOp::Min => "min",
                AggregateOp::Max => "max",
                // Count returns early above; this arm is unreachable but keeps
                // the match exhaustive as the enum grows.
                AggregateOp::Count => "count",
            };
            if matches!(agg.op, AggregateOp::Sum | AggregateOp::Avg)
                && !matches!(agg_field_pg, PgType::Number | PgType::Int64)
            {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    format!("aggregate op {op_name} requires a numeric index field"),
                ));
            }
            if let Some(group_field) = group_field {
                let group_field_pg = table_def
                    .fields
                    .get(group_field)
                    .and_then(|ty| index_column_type(ty).ok())
                    .map(|it| it.pg)
                    .unwrap_or(PgType::Text);
                // Group rows by `group_field` (skip null keys), preserving
                // first-seen order; sort by key ascending after, for parity with
                // the server's `ORDER BY k`.
                let mut groups: Vec<(Value, Vec<Value>)> = Vec::new();
                let mut group_index: HashMap<String, usize> = HashMap::new();
                for row in &filtered {
                    let Some(k) = row.doc.get(group_field) else {
                        continue;
                    };
                    if k.is_null() {
                        continue;
                    }
                    let key = k.to_string();
                    let i = match group_index.get(&key).copied() {
                        Some(i) => i,
                        None => {
                            let i = groups.len();
                            group_index.insert(key, i);
                            groups.push((k.clone(), Vec::new()));
                            i
                        }
                    };
                    if let Some(v) = row.doc.get(agg_field)
                        && !v.is_null()
                    {
                        groups[i].1.push(v.clone());
                    }
                }
                let mut out: Vec<Value> = groups
                    .into_iter()
                    .map(|(k, vs)| {
                        let value = if vs.is_empty() {
                            Value::Null
                        } else {
                            apply_aggregate(agg.op, &vs, agg_field_pg)
                        };
                        let mut obj = Map::new();
                        obj.insert("key".to_string(), k);
                        obj.insert("value".to_string(), value);
                        Value::Object(obj)
                    })
                    .collect();
                out.sort_by(|a, b| compare_index_values(&a["key"], &b["key"], group_field_pg));
                let out: Vec<Value> = out.into_iter().take(MAX_TAKE).collect();
                return Ok(Value::Array(out));
            }
            // Scalar aggregate.
            let values: Vec<Value> = filtered
                .iter()
                .filter_map(|row| row.doc.get(agg_field))
                .filter(|v| !v.is_null())
                .cloned()
                .collect();
            if values.is_empty() {
                return Ok(Value::Null);
            }
            return Ok(apply_aggregate(agg.op, &values, agg_field_pg));
        }

        // Sort keys: unbound index fields (after the eq prefix), then
        // `_creationTime`, then `_id`. The unique `id` tiebreaker means the
        // order is total — no row is ambiguous relative to another.
        let dir = q.order.unwrap_or(Order::Asc);
        // Per-sort-column storage types — the comparator needs the domain to
        // pick numeric vs lexicographic ordering (int64 indexes store decimal
        // strings, which would otherwise sort lexicographically). The eq prefix
        // and range field have already been validated as indexable by
        // `coerce_index_value`; any remaining index field is schema-declared
        // indexable, so the lookup is total — the `Text` fallback is defensive.
        let sort_field_pgs: Vec<PgType> = match &index_def {
            Some(idx) => idx.fields[typed_eq.len()..]
                .iter()
                .map(|f| {
                    table_def
                        .fields
                        .get(f)
                        .and_then(|ty| index_column_type(ty).ok())
                        .map(|it| it.pg)
                        .unwrap_or(PgType::Text)
                })
                .collect(),
            None => Vec::new(),
        };
        filtered.sort_by(|a, b| {
            if let Some(idx) = &index_def {
                for (i, field) in idx.fields[typed_eq.len()..].iter().enumerate() {
                    let av = a.doc.get(field).unwrap_or(&Value::Null);
                    let bv = b.doc.get(field).unwrap_or(&Value::Null);
                    let cmp = compare_index_values(av, bv, sort_field_pgs[i]);
                    if cmp != std::cmp::Ordering::Equal {
                        return dir_order(cmp, dir);
                    }
                }
            }
            let cmp = a.created_at.cmp(&b.created_at);
            if cmp != std::cmp::Ordering::Equal {
                return dir_order(cmp, dir);
            }
            dir_order(a.id.cmp(&b.id), dir)
        });

        // `paginate` terminal: keyset-cursor paging over the sorted set. Ports
        // TS `executeQuery` :1135-1137 → `paginateResult` (`:1164-1202`). The
        // sort columns mirror the sort above (unbound index fields after the
        // eq prefix, then `_creationTime`, then `_id`); the cursor encodes one
        // value per column.
        if let Some(pag) = &q.paginate {
            let mut sort_cols: Vec<SortCol> = Vec::new();
            if let Some(idx) = &index_def {
                for field in idx.fields[typed_eq.len()..].iter() {
                    sort_cols.push(SortCol::Index(field.clone()));
                }
            }
            sort_cols.push(SortCol::CreatedAt);
            sort_cols.push(SortCol::Id);
            // Mirror the sort caller's per-column storage types so keyset
            // resume agrees with the ordering that produced `filtered`.
            let col_types: Vec<PgType> = sort_cols
                .iter()
                .map(|c| match c {
                    SortCol::Index(field) => table_def
                        .fields
                        .get(field)
                        .and_then(|ty| index_column_type(ty).ok())
                        .map(|it| it.pg)
                        .unwrap_or(PgType::Text),
                    SortCol::CreatedAt => PgType::Number,
                    SortCol::Id => PgType::Text,
                })
                .collect();
            return paginate_result(pag, &table_def, &filtered, &sort_cols, &col_types, dir);
        }

        if q.unique {
            if filtered.len() > 1 {
                return Err(RtDbError::new(
                    ErrorCode::PreconditionFailed,
                    "unique query matched multiple documents",
                ));
            }
            return Ok(filtered.first().map(merge_doc).unwrap_or(Value::Null));
        }
        if q.first {
            return Ok(filtered.first().map(merge_doc).unwrap_or(Value::Null));
        }

        let limit = q.take.map(|t| t as usize).unwrap_or(MAX_TAKE);
        let out: Vec<Value> = filtered
            .into_iter()
            .take(limit)
            .map(|row| merge_doc(&row))
            .collect();
        Ok(Value::Array(out))
    }

    /// Typed wrapper around [`run_query`](Self::run_query) that deserializes
    /// the result into `T` via [`crate::query::parse_result`]. Pick `T` to
    /// match the terminal: `Vec<T>` for `take`/`collect`, `Option<T>` for
    /// `get`/`first`/`unique`, `i64` for `count`, `Paginated<T>` for
    /// `paginate`.
    pub fn run<T: DeserializeOwned>(&self, q: &Query) -> Result<T, RtDbError> {
        let value = self.run_query(q)?;
        crate::query::parse_result(value)
    }

    /// Executes a transaction and returns one [`StepResult`] per step, in order.
    /// Same shape (and `mut_id` idempotency-key semantics) as the live clients.
    ///
    /// Ports `mutate` in `ts-client/src/in_memory.ts:528-540`: a `mut_id` that
    /// has been seen before short-circuits with the cached results; otherwise
    /// the txn runs through `execute_transaction`
    /// and, on success, the results are cached under `mut_id` for next time.
    pub async fn mutate(
        &mut self,
        txn: &Transaction,
        mut_id: Option<&str>,
    ) -> Result<Vec<StepResult>, RtDbError> {
        if let Some(mid) = mut_id
            && let Some(cached) = self.idempotency.get(mid)
        {
            return Ok(cached.clone());
        }
        let results = self.execute_transaction(txn)?;
        if let Some(mid) = mut_id {
            self.idempotency.insert(mid.to_string(), results.clone());
        }
        Ok(results)
    }

    /// Synchronous atomic core shared by [`mutate`](Self::mutate) and the
    /// scheduler's [`tick`](Self::tick): enforces the [`MAX_STEPS`] cap,
    /// snapshots the docs store, applies every step (rolling back the whole txn
    /// on any error), then — on success — fans out subscription notifications
    /// for the written tables. Ports `executeTransaction`
    /// (`ts-client/src/in_memory.ts:545-567`); the notify seam lives here so
    /// both mutate- and `tick`-driven writes fire subscription updates.
    fn execute_transaction(&mut self, txn: &Transaction) -> Result<Vec<StepResult>, RtDbError> {
        if txn.steps.len() > MAX_STEPS {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                format!("transaction exceeds maximum of {MAX_STEPS} steps"),
            ));
        }
        // SEC-104: bound the worst-case row count before any step applies so an
        // over-budget txn rolls back nothing. Mirrors server `execute_txn`.
        let by_query_steps = txn
            .steps
            .iter()
            .filter(|s| matches!(s, Step::PatchByQuery { .. } | Step::DeleteByQuery { .. }))
            .count();
        if by_query_steps > MAX_BY_QUERY_STEPS_PER_TXN {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                format!(
                    "transaction has {by_query_steps} by-query steps, exceeding the limit of {MAX_BY_QUERY_STEPS_PER_TXN}"
                ),
            ));
        }
        let worst = worst_case_affected(txn);
        if worst > MAX_AFFECTED_ROWS_PER_TXN {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                format!(
                    "transaction could affect up to {worst} documents, exceeding the limit of {MAX_AFFECTED_ROWS_PER_TXN}"
                ),
            ));
        }
        let snapshot = self.snapshot_docs();
        let mut results = Vec::with_capacity(txn.steps.len());
        let mut write_set: BTreeSet<String> = BTreeSet::new();
        for step in &txn.steps {
            match self.execute_step(step) {
                Ok((result, written_table)) => {
                    results.push(result);
                    if let Some(table) = written_table {
                        write_set.insert(table);
                    }
                }
                Err(error) => {
                    // Atomicity: any step's error rolls back everything already
                    // applied, mirroring the server's single-transaction semantics.
                    self.restore_docs(snapshot);
                    return Err(error);
                }
            }
        }
        self.notify_subs(&write_set);
        Ok(results)
    }

    /// Per-step executor — ports `executeStep` (`ts-client/src/in_memory.ts:747-805`).
    /// Each step validates against the live schema, mutates `self.docs` (or, for
    /// `Expect*`, just observes), and returns the [`StepResult`] plus the table
    /// that was written (so the Task 5 notify path can fan out by table).
    fn execute_step(&mut self, step: &Step) -> Result<(StepResult, Option<String>), RtDbError> {
        match step {
            Step::Insert { table, doc } => {
                let table_def = self.require_table(table)?.clone();
                let id = self.do_insert(table, &table_def, doc)?;
                Ok((StepResult::Insert { id }, Some(table.clone())))
            }
            Step::Patch { table, id, fields } => {
                let table_def = self.require_table(table)?.clone();
                self.do_patch(&table_def, table, id, fields)?;
                Ok((StepResult::Null, Some(table.clone())))
            }
            Step::Replace { table, id, doc } => {
                let table_def = self.require_table(table)?.clone();
                self.do_replace(&table_def, table, id, doc)?;
                Ok((StepResult::Null, Some(table.clone())))
            }
            Step::Delete { table, id } => {
                self.require_table(table)?;
                self.do_delete(table, id)?;
                Ok((StepResult::Null, Some(table.clone())))
            }
            Step::ExpectVersion { table, id, version } => {
                self.require_table(table)?;
                self.do_expect_version(table, id, *version)?;
                Ok((StepResult::Null, None))
            }
            Step::ExpectAbsent { table, index, eq } => {
                let table_def = self.require_table(table)?.clone();
                let rows = self.eq_lookup(&table_def, table, index, eq)?;
                if !rows.is_empty() {
                    return Err(RtDbError::new(
                        ErrorCode::PreconditionFailed,
                        format!("index '{index}' already has a matching document"),
                    ));
                }
                Ok((StepResult::Null, None))
            }
            Step::Upsert {
                table,
                index,
                eq,
                insert,
                patch,
            } => {
                let table_def = self.require_table(table)?.clone();
                let rows = self.eq_lookup(&table_def, table, index, eq)?;
                if rows.len() > 1 {
                    return Err(RtDbError::new(
                        ErrorCode::PreconditionFailed,
                        "upsert matched multiple documents",
                    ));
                }
                if let Some(row) = rows.into_iter().next() {
                    let merged = apply_patch(&table_def, &row.doc, patch)?;
                    self.do_update(&table_def, table, &row.id, merged)?;
                    Ok((
                        StepResult::Upsert {
                            id: row.id.clone(),
                            inserted: false,
                        },
                        Some(table.clone()),
                    ))
                } else {
                    let id = self.do_insert(table, &table_def, insert)?;
                    Ok((
                        StepResult::Upsert { id, inserted: true },
                        Some(table.clone()),
                    ))
                }
            }
            Step::PatchByQuery {
                table,
                filter,
                patch,
                limit,
            } => {
                let (patched, truncated) = self.patch_by_query(table, filter, patch, *limit)?;
                Ok((
                    StepResult::PatchByQuery { patched, truncated },
                    Some(table.clone()),
                ))
            }
            Step::DeleteByQuery {
                table,
                filter,
                limit,
            } => {
                let (deleted, truncated) = self.delete_by_query(table, filter, *limit)?;
                Ok((
                    StepResult::DeleteByQuery { deleted, truncated },
                    Some(table.clone()),
                ))
            }
        }
    }

    /// Inserts a new doc, minting the id and stamping `_creationTime` /
    /// `_version = 1`. Ports `doInsert` (`ts-client/src/in_memory.ts:807-813`)
    /// with the unique-index check threaded in before the write.
    fn do_insert(
        &mut self,
        table_name: &str,
        table_def: &TableDef,
        doc: &Map<String, Value>,
    ) -> Result<String, RtDbError> {
        let stamped = stamp_ttl_default(table_def, doc, (self.now)());
        let doc_value = Value::Object(stamped);
        validate_doc(table_def, &doc_value)?;
        let stored = strip_unset_optionals(table_def, &doc_value);
        self.check_unique_indexes(table_def, table_name, &stored, None)?;
        let id = self.new_id();
        self.docs.insert(
            (table_name.to_string(), id.clone()),
            StoredRow {
                id: id.clone(),
                doc: stored,
                version: 1,
                created_at: (self.now)(),
            },
        );
        Ok(id)
    }

    /// Patches an existing doc with `fields`, bumping `_version`. Ports
    /// `doPatch` (`ts-client/src/in_memory.ts:815-824`) — apply then update.
    fn do_patch(
        &mut self,
        table_def: &TableDef,
        table_name: &str,
        id: &str,
        fields: &Map<String, Value>,
    ) -> Result<(), RtDbError> {
        let key = (table_name.to_string(), id.to_string());
        let row = self.docs.get(&key).cloned().ok_or_else(|| {
            RtDbError::new(ErrorCode::NotFound, format!("document '{id}' not found"))
        })?;
        let merged = apply_patch(table_def, &row.doc, fields)?;
        self.do_update(table_def, table_name, id, merged)?;
        Ok(())
    }

    /// Replaces an existing doc whole, bumping `_version`. Ports `doReplace`
    /// (`ts-client/src/in_memory.ts:826-836`) with the unique-index check
    /// threaded in before the write (TS calls `checkUniqueIndexes` with the
    /// stored replacement doc and `excludeId = row.id`).
    fn do_replace(
        &mut self,
        table_def: &TableDef,
        table_name: &str,
        id: &str,
        doc: &Map<String, Value>,
    ) -> Result<(), RtDbError> {
        let key = (table_name.to_string(), id.to_string());
        if !self.docs.contains_key(&key) {
            return Err(RtDbError::new(
                ErrorCode::NotFound,
                format!("document '{id}' not found"),
            ));
        }
        let doc_value = Value::Object(doc.clone());
        validate_doc(table_def, &doc_value)?;
        let stored = strip_unset_optionals(table_def, &doc_value);
        self.check_unique_indexes(table_def, table_name, &stored, Some(id))?;
        if let Some(row) = self.docs.get_mut(&key) {
            row.doc = stored;
            row.version += 1;
        }
        Ok(())
    }

    /// Deletes a doc by id. Ports `doDelete` (`ts-client/src/in_memory.ts:838-842`).
    fn do_delete(&mut self, table_name: &str, id: &str) -> Result<(), RtDbError> {
        let key = (table_name.to_string(), id.to_string());
        self.docs.remove(&key).ok_or_else(|| {
            RtDbError::new(ErrorCode::NotFound, format!("document '{id}' not found"))
        })?;
        Ok(())
    }

    /// Scans `table` for rows matching `filter`, returning their ids ordered by
    /// `(created_at, id)` — the same order the server uses for
    /// `patchByQuery`/`deleteByQuery`. Fetches the full match set so a truncation
    /// is detectable; returns `(ids, truncated)` where `ids` is at most `limit`.
    /// `limit_opt` defaults to and is clamped by [`MAX_BY_QUERY_ROWS`].
    fn scan_ids_by_filter(
        &self,
        table_def: &TableDef,
        table: &str,
        filter: &FilterExpr,
        limit_opt: Option<u32>,
    ) -> Result<(Vec<String>, bool), RtDbError> {
        let fields: BTreeSet<String> = table_def.fields.keys().cloned().collect();
        validate_filter(filter, &fields)?;
        let limit = limit_opt
            .unwrap_or(MAX_BY_QUERY_ROWS)
            .min(MAX_BY_QUERY_ROWS);
        let mut matching: Vec<(i64, String)> = self
            .docs
            .iter()
            .filter(|((t, _), _)| t == table)
            .filter(|(_, row)| matches_filter(filter, &row.doc))
            .map(|(_, row)| (row.created_at, row.id.clone()))
            .collect();
        matching.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        let truncated = matching.len() as u32 > limit;
        let take = std::cmp::min(matching.len(), limit as usize);
        let ids: Vec<String> = matching.into_iter().take(take).map(|(_, id)| id).collect();
        Ok((ids, truncated))
    }

    /// `patchByQuery` executor: patches every matching row via the same
    /// `apply_patch` + `do_update` path as a per-id `Patch`. Mirrors
    /// `server/src/txn.rs::step_patch_by_query`.
    fn patch_by_query(
        &mut self,
        table: &str,
        filter: &FilterExpr,
        patch: &Map<String, Value>,
        limit: Option<u32>,
    ) -> Result<(u32, bool), RtDbError> {
        let table_def = self.require_table(table)?.clone();
        let (ids, truncated) = self.scan_ids_by_filter(&table_def, table, filter, limit)?;
        for id in &ids {
            self.do_patch(&table_def, table, id, patch)?;
        }
        Ok((ids.len() as u32, truncated))
    }

    /// `deleteByQuery` executor: deletes every matching row via the same
    /// `do_delete` path as a per-id `Delete`. Mirrors
    /// `server/src/txn.rs::step_delete_by_query`.
    fn delete_by_query(
        &mut self,
        table: &str,
        filter: &FilterExpr,
        limit: Option<u32>,
    ) -> Result<(u32, bool), RtDbError> {
        let table_def = self.require_table(table)?;
        let (ids, truncated) = self.scan_ids_by_filter(table_def, table, filter, limit)?;
        for id in &ids {
            self.do_delete(table, id)?;
        }
        Ok((ids.len() as u32, truncated))
    }

    /// Asserts a doc's current `_version` matches `expected`. Ports
    /// `doExpectVersion` (`ts-client/src/in_memory.ts:844-852`).
    fn do_expect_version(
        &self,
        table_name: &str,
        id: &str,
        expected: i64,
    ) -> Result<(), RtDbError> {
        let key = (table_name.to_string(), id.to_string());
        let row = self.docs.get(&key).ok_or_else(|| {
            RtDbError::new(ErrorCode::NotFound, format!("document '{id}' not found"))
        })?;
        if row.version != expected {
            return Err(RtDbError::new(
                ErrorCode::PreconditionFailed,
                format!(
                    "version mismatch: expected {expected}, actual {}",
                    row.version
                ),
            ));
        }
        Ok(())
    }

    /// Shared write-back helper for patch/replace/upsert-patch: enforces unique
    /// indexes on the merged doc, then writes it and bumps `_version`. Ports
    /// `doUpdate` (`ts-client/src/in_memory.ts:856-860`) with the unique check
    /// threaded in (TS `checkUniqueIndexes` is called from every write path).
    fn do_update(
        &mut self,
        table_def: &TableDef,
        table_name: &str,
        id: &str,
        merged: Value,
    ) -> Result<(), RtDbError> {
        self.check_unique_indexes(table_def, table_name, &merged, Some(id))?;
        let key = (table_name.to_string(), id.to_string());
        if let Some(row) = self.docs.get_mut(&key) {
            row.doc = merged;
            row.version += 1;
        }
        Ok(())
    }

    /// Enforce `unique` indexes on a candidate write (mirrors server
    /// `CREATE UNIQUE INDEX` and the TS `checkUniqueIndexes`): for each unique
    /// index on `table_name`, no OTHER row (excluding `exclude_id` when given)
    /// that satisfies the index's `where` predicate may share the candidate's
    /// key values on the index's declared `fields`. NULL/absent key fields
    /// disable the constraint for that row (Postgres UNIQUE treats NULLs as
    /// distinct). Returns `Err(Conflict)` on collision; `execute_transaction`
    /// then rolls back the whole txn via the same snapshot/restore path as the
    /// `PreconditionFailed` checks. Uniqueness is on `fields` only — never
    /// `id` or `created_at` (a trailing tiebreaker column would defeat
    /// uniqueness, as it does on the server).
    fn check_unique_indexes(
        &self,
        table_def: &TableDef,
        table_name: &str,
        candidate_doc: &Value,
        exclude_id: Option<&str>,
    ) -> Result<(), RtDbError> {
        let Some(indexes) = table_def.indexes.as_ref() else {
            return Ok(());
        };
        for index in indexes.iter().filter(|i| i.unique) {
            // A partial unique index constrains only rows matching its predicate.
            if let Some(pred) = &index.r#where
                && !eval_filter_expr(pred, candidate_doc)
            {
                continue;
            }
            // Build the candidate's collision key from declared `fields` only.
            // NULL/absent key fields disable the constraint for this row
            // (Postgres UNIQUE treats NULLs as distinct) — skip the index.
            let candidate_key: Vec<&Value> = match collect_index_key(&index.fields, candidate_doc) {
                Some(k) => k,
                None => continue,
            };
            for ((t, _row_id), row) in &self.docs {
                if t != table_name {
                    continue;
                }
                if matches!(exclude_id, Some(excl) if row.id == excl) {
                    continue;
                }
                if let Some(pred) = &index.r#where
                    && !eval_filter_expr(pred, &row.doc)
                {
                    continue;
                }
                let Some(row_key) = collect_index_key(&index.fields, &row.doc) else {
                    continue;
                };
                if row_key.len() == candidate_key.len()
                    && row_key
                        .iter()
                        .zip(candidate_key.iter())
                        .all(|(a, b)| *a == *b)
                {
                    return Err(RtDbError::new(
                        ErrorCode::Conflict,
                        format!("unique index '{}' violated", index.name),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Full-arity index eq lookup — ports `eqLookup`
    /// (`ts-client/src/in_memory.ts:864-885`), shared by `expectAbsent` and
    /// `upsert`. Returns every stored row whose indexed fields equal `eq`
    /// positionally (null/absent index fields never match, mirroring SQL NULL
    /// exclusion).
    fn eq_lookup(
        &self,
        table_def: &TableDef,
        table_name: &str,
        index_name: &str,
        eq: &[Value],
    ) -> Result<Vec<StoredRow>, RtDbError> {
        let index = require_index(table_def, index_name)?;
        if eq.len() != index.fields.len() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                format!(
                    "index '{}' expects {} eq value(s), got {}",
                    index_name,
                    index.fields.len(),
                    eq.len()
                ),
            ));
        }
        let typed: Vec<Value> = index
            .fields
            .iter()
            .zip(eq.iter())
            .map(|(field, value)| coerce_index_value(table_def, field, value))
            .collect::<Result<_, _>>()?;
        let mut matches = Vec::new();
        for ((t, _id), row) in &self.docs {
            if t != table_name {
                continue;
            }
            let all_match =
                index
                    .fields
                    .iter()
                    .zip(typed.iter())
                    .all(|(field, tv)| match row.doc.get(field) {
                        Some(v) => !v.is_null() && v == tv,
                        None => false,
                    });
            if all_match {
                matches.push(row.clone());
            }
        }
        Ok(matches)
    }

    /// Looks up a table def by name (NOT_FOUND if the schema has no such table).
    /// Ports `requireTable` (`ts-client/src/in_memory.ts:1320-1326`).
    fn require_table(&self, name: &str) -> Result<&TableDef, RtDbError> {
        self.tables
            .get(name)
            .ok_or_else(|| RtDbError::new(ErrorCode::NotFound, format!("table '{name}' not found")))
    }

    /// Snapshots the docs store for atomic rollback. Ports `snapshotTables`
    /// (`ts-client/src/in_memory.ts:1368-1383`).
    fn snapshot_docs(&self) -> HashMap<(String, String), StoredRow> {
        self.docs.clone()
    }

    /// Restores a previously-taken snapshot, discarding any partial writes.
    /// Ports `restoreTables` (`ts-client/src/in_memory.ts:1385-1390`).
    fn restore_docs(&mut self, snapshot: HashMap<(String, String), StoredRow>) {
        self.docs = snapshot;
    }

    /// UUIDv7-shaped id (timestamp-prefixed for sort stability), 32 hex chars.
    /// Ports `newId` (`ts-client/src/in_memory.ts:1354-1358`): low 48 bits of
    /// the epoch-millis timestamp (12 hex chars, the TS `.slice(-12)` of
    /// `toString(16)`), a constant `7` version nibble, then 19 random hex chars.
    fn new_id(&self) -> String {
        let ts = (self.now)() as u64 & 0xFFFF_FFFF_FFFF;
        let rand = self.random_hex(19);
        format!("{ts:012x}7{rand}")
    }

    /// `count` lowercase hex chars drawn from the injected RNG. Ports
    /// `randomHex` (`ts-client/src/in_memory.ts:1360-1366`).
    fn random_hex(&self, count: usize) -> String {
        let mut out = String::with_capacity(count);
        for _ in 0..count {
            // `random` is documented as `[0, 1)`; the `& 0xF` is a defensive
            // guard against a stray `1.0` overflowing the digit range.
            let digit = ((self.random)() * 16.0).floor() as u32 & 0xF;
            out.push(char::from_digit(digit, 16).unwrap_or('0'));
        }
        out
    }

    // ---- subscriptions -----------------------------------------------------
    //
    // Ports `subscribe` (`ts-client/src/in_memory.ts:572-594`) and `notifySubs`
    // (`:1294-1309`). On a successful `execute_transaction`, each subscriber
    // whose `table` is in the write-set re-runs its query and fires its
    // callback iff the (canonicalized) result changed. The initial value is
    // delivered synchronously inside `subscribe`, mirroring the server's first
    // `queryUpdate` arriving right after subscribe.

    /// Reactive subscription — fires `on_update` with the initial result
    /// synchronously, then again whenever a mutation changes the result.
    /// Dropping (or calling [`unsubscribe`](SubscriptionHandle::unsubscribe)
    /// on) the returned handle stops further notifications. The callback runs
    /// inline on the writing thread; never recursively mutate the same client
    /// from inside a callback (the harness is single-threaded and a recursive
    /// mutate would deadlock against the in-progress `execute_transaction`).
    pub fn subscribe<F>(&mut self, query: Query, on_update: F) -> SubscriptionHandle
    where
        F: Fn(Value) + Send + Sync + 'static,
    {
        let alive = Arc::new(AtomicBool::new(true));
        let callback: Listener = Arc::new(on_update);
        let table = query.table.clone();
        let sub = Arc::new(Subscription {
            query: query.clone(),
            table,
            alive: alive.clone(),
            callback: callback.clone(),
            last: Mutex::new(None),
        });
        self.subscribers.push(sub.clone());

        // Initial value, delivered synchronously (server's first queryUpdate).
        // DIVERGENCE from TS: the TS harness has no try/catch here or in
        // `notifySubs`, so a query error propagates out of `subscribe`/`mutate`/
        // `tick`. This port suppresses such errors (a failing query simply never
        // fires) to keep `subscribe`/`notify_subs` infallible — tests that need
        // to assert on a failing query should call `run_query` directly.
        if let Ok(initial) = self.run_query(&query) {
            let initial_canon = canonical(&initial);
            *sub.last.lock().unwrap_or_else(|p| p.into_inner()) = Some(initial_canon);
            callback(initial);
        }

        SubscriptionHandle { alive }
    }

    /// Re-runs each subscriber's query (only those whose `table` is in the
    /// write-set) and fires its callback iff the result changed. Ports
    /// `notifySubs` (`ts-client/src/in_memory.ts:1294-1309`). Dead subscriptions
    /// (whose handle was dropped) are lazily compacted away.
    fn notify_subs(&mut self, write_set: &BTreeSet<String>) {
        // Collect the work to do (callback + value pairs) before any mutation,
        // so callbacks run outside the iteration with no borrow on `self`.
        let mut fires: Vec<(Listener, Value)> = Vec::new();
        for sub in &self.subscribers {
            if !sub.alive.load(Ordering::SeqCst) {
                continue;
            }
            if !write_set.contains(&sub.table) {
                continue;
            }
            let next = match self.run_query(&sub.query) {
                Ok(v) => v,
                Err(_) => continue, // DIVERGENCE from TS (which propagates): suppress so a bad subscriber query can't abort the write
            };
            let next_canon = canonical(&next);
            let mut last_lock = sub.last.lock().unwrap_or_else(|p| p.into_inner());
            let changed = match &*last_lock {
                None => true,
                Some(prev) => prev != &next_canon,
            };
            if changed {
                *last_lock = Some(next_canon);
                drop(last_lock);
                fires.push((sub.callback.clone(), next));
            }
        }
        // Lazily compact dead subscriptions (handle dropped → alive=false).
        self.subscribers.retain(|s| s.alive.load(Ordering::SeqCst));
        // Fire outside the borrow — a callback that re-enters the client (e.g.
        // a query) does not deadlock against the iteration we just finished.
        for (callback, value) in fires {
            callback(value);
        }
    }

    // ---- presence ---------------------------------------------------------
    //
    // Ports `presence`/`updatePresence`/`leavePresence`
    // (`ts-client/src/in_memory.ts:1217-1285`). Backed by [`PresenceRooms`],
    // which approximates the server's per-db presence registry: one client =
    // one connection, keyed by `connectionId`. Two clients sharing the same
    // `PresenceRooms` instance see each other's joins/updates/leaves fan out.

    /// Joins presence room `room` with optional initial `state`, mirroring the
    /// reactive client's [`presence`](crate::ws::RtDbClient::presence). The
    /// `on_update` callback fires with the current member list on join and again
    /// on every local mutation (a peer's join/update/leave on a shared
    /// [`PresenceRooms`]).
    ///
    /// Returns a [`PresenceHandle`] whose Drop stops listening but does NOT leave
    /// the room — call [`leave_presence`](Self::leave_presence) for that,
    /// mirroring the TS harness and the reactive client.
    pub fn presence<F>(&mut self, room: &str, state: Option<Value>, on_update: F) -> PresenceHandle
    where
        F: Fn(Vec<PresenceMember>) + Send + Sync + 'static,
    {
        self.joined_rooms.insert(room.to_string());
        let member = PresenceMember {
            connection_id: self.connection_id.clone(),
            user: self.presence_user.clone(),
            state: state.unwrap_or(Value::Null),
        };
        // Join first, then subscribe — the TS harness calls `join` before
        // `subscribe` so the initial snapshot (fired synchronously inside
        // `subscribe`) already includes this connection.
        let handle = {
            let mut rooms = self
                .presence_rooms
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            rooms.join(room, member);
            rooms.subscribe(room, on_update)
        };
        // Track the handle so `leave_presence` can drop every local subscriber
        // for this room (the TS harness tracks an array of unsubs per room).
        self.presence_unsubs
            .entry(room.to_string())
            .or_default()
            .push(handle.clone());
        handle
    }

    /// Broadcasts updated `state` for this connection in `room`. No-op if this
    /// client has not joined `room` (mirrors the live server, which would not
    /// relay an update from a non-member). When `ttl_ms` is `Some(n)` with
    /// `n > 0`, the harness schedules an expiry that nulls this member's
    /// `state` at `now + n` (the member stays listed) — mirroring the live
    /// server. Call [`expire_presence`](Self::expire_presence) (or `tick`) to
    /// run the sweep.
    pub fn update_presence(&mut self, room: &str, state: Value, ttl_ms: Option<u64>) {
        if !self.joined_rooms.contains(room) {
            return;
        }
        let now = (self.now)();
        self.presence_rooms
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .update(room, &self.connection_id, state, ttl_ms, now);
    }

    /// Runs a presence-ttl expiry sweep: clears expired members' `state` to
    /// `Value::Null` (the member stays listed) and fans out each touched room
    /// once. Returns `true` if anything expired. Mirrors the live server's
    /// per-connection ttl clearing (`server::presence::expire_once`). Use this
    /// in tests that don't otherwise drive the clock via [`tick`](Self::tick).
    /// Pass an explicit `now` for determinism; `None` uses the client's injected
    /// clock.
    pub fn expire_presence(&mut self, now: Option<i64>) -> bool {
        let now = now.unwrap_or_else(|| (self.now)());
        self.presence_rooms
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .expire(now)
    }

    /// Leaves `room`: removes this connection from the member list, drops every
    /// local subscriber this client registered for that room, and fans out a
    /// fresh snapshot to remaining subscribers.
    pub fn leave_presence(&mut self, room: &str) {
        if !self.joined_rooms.remove(room) {
            return;
        }
        // Drop every local subscriber this client registered for this room.
        if let Some(handles) = self.presence_unsubs.remove(room) {
            drop(handles);
        }
        self.presence_rooms
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .leave(room, &self.connection_id);
    }

    // ---- schedules --------------------------------------------------------
    //
    // Ports `schedule`/`cancelSchedule`/`pauseSchedule`/`resumeSchedule`/
    // `listSchedules`/`tick` (`ts-client/src/in_memory.ts:600-706`). Cron
    // validation is deferred to the live server; the harness only needs the
    // `dueAt`-driven re-arm cadence (`CRON_STEP_MS`). One-shots catch up if
    // past due (fire once even if `due_at < now`); crons step by
    // `CRON_STEP_MS` and skip missed windows (re-arm to the next interval,
    // never fire N times for N missed windows).

    /// Stores `txn` scheduled for `when` and returns its id. Cron validation
    /// is deferred to the live server; the harness accepts any expression.
    /// Ports `schedule` (`ts-client/src/in_memory.ts:600-617`).
    pub fn schedule(&mut self, txn: Transaction, when: ScheduleWhen) -> Result<String, RtDbError> {
        let id = self.new_id();
        let now = (self.now)();
        let kind = match &when {
            ScheduleWhen::Cron { .. } => ScheduleKind::Cron,
            _ => ScheduleKind::Oneshot,
        };
        let cron = match &when {
            ScheduleWhen::Cron { expr } => Some(expr.clone()),
            _ => None,
        };
        let job = ScheduledJob {
            id: id.clone(),
            kind,
            txn,
            due_at: self.due_at_for(&when, now),
            cron,
            status: ScheduleStatus::Pending,
            created_at: now,
            fired_count: 0,
            last_error: None,
        };
        self.schedules.push(job);
        Ok(id)
    }

    /// Removes the scheduled job. NOT_FOUND if no such id. Ports
    /// `cancelSchedule` (`ts-client/src/in_memory.ts:619-623`).
    pub fn cancel_schedule(&mut self, id: &str) -> Result<(), RtDbError> {
        let before = self.schedules.len();
        self.schedules.retain(|j| j.id != id);
        if self.schedules.len() == before {
            return Err(RtDbError::new(
                ErrorCode::NotFound,
                format!("schedule '{id}' not found"),
            ));
        }
        Ok(())
    }

    /// Sets the schedule's status to `Paused`. NOT_FOUND if no such id. Ports
    /// `pauseSchedule` (`ts-client/src/in_memory.ts:625-627`).
    pub fn pause_schedule(&mut self, id: &str) -> Result<(), RtDbError> {
        let job = self
            .schedules
            .iter_mut()
            .find(|j| j.id == id)
            .ok_or_else(|| {
                RtDbError::new(ErrorCode::NotFound, format!("schedule '{id}' not found"))
            })?;
        job.status = ScheduleStatus::Paused;
        Ok(())
    }

    /// Sets a paused schedule's status back to `Pending`. NOT_FOUND if no such
    /// id. Ports `resumeSchedule` (`ts-client/src/in_memory.ts:629-631`).
    pub fn resume_schedule(&mut self, id: &str) -> Result<(), RtDbError> {
        let job = self
            .schedules
            .iter_mut()
            .find(|j| j.id == id)
            .ok_or_else(|| {
                RtDbError::new(ErrorCode::NotFound, format!("schedule '{id}' not found"))
            })?;
        job.status = ScheduleStatus::Pending;
        Ok(())
    }

    /// Snapshot of every scheduled job's public view. Ports `listSchedules`
    /// (`ts-client/src/in_memory.ts:633-635`).
    pub fn list_schedules(&self) -> Vec<ScheduleInfo> {
        self.schedules.iter().map(schedule_info).collect()
    }

    /// Fires every due non-paused job by applying its txn through the same
    /// atomic path as [`mutate`](Self::mutate) (so reactive subscriptions see
    /// the write). One-shots are removed after a successful fire; crons are
    /// re-armed by `CRON_STEP_MS`. A job whose txn fails is marked `Error` but
    /// left in place (still due), so a subsequent `tick` retries it — matching
    /// the TS harness, where only `Paused` jobs are skipped. Pass `now_ms` to
    /// drive the clock deterministically; omit it to use the client's injected
    /// clock. Ports `tick` (`ts-client/src/in_memory.ts:683-706`).
    ///
    /// Also reaps expired documents: any table that declares a `ttl` has rows
    /// removed whose TTL field value is a number strictly less than `now` (a
    /// no-op for tables without TTL). Returns the count of documents reaped.
    /// The live server's per-db reaper is the real expiry; this is best-effort,
    /// for tests/local workflows. Mirrors TS `tick` (`ts-client/src/in_memory.ts:1156-1190`).
    pub fn tick(&mut self, now_ms: Option<i64>) -> usize {
        let now = now_ms.unwrap_or_else(|| (self.now)());
        // Iterate by index so we can mutate (`pause`/error) and remove in place
        // without invalidating a borrow on `self.schedules`.
        let mut i = 0;
        while i < self.schedules.len() {
            let job = &mut self.schedules[i];
            if job.status == ScheduleStatus::Paused || job.due_at > now {
                i += 1;
                continue;
            }
            // Step out of the borrow so we can call execute_transaction (which
            // needs &mut self) without holding &mut self.schedules.
            let txn = job.txn.clone();
            let job_id = job.id.clone();
            let kind = job.kind;
            match self.execute_transaction(&txn) {
                Ok(_results) => {
                    // Re-borrow the job (it may have moved if execute_transaction
                    // triggered another tick path — it doesn't, but defensively
                    // look up by id rather than holding the &mut).
                    if let Some(j) = self.schedules.iter_mut().find(|j| j.id == job_id) {
                        j.fired_count += 1;
                        match kind {
                            ScheduleKind::Oneshot => {
                                // Remove after a successful fire.
                                self.schedules.retain(|x| x.id != job_id);
                                // Don't bump i — the next job shifted into this
                                // index; the loop re-examines the same i.
                                continue;
                            }
                            ScheduleKind::Cron => {
                                j.due_at = now + CRON_STEP_MS;
                                j.status = ScheduleStatus::Pending;
                            }
                        }
                    }
                }
                Err(error) => {
                    if let Some(j) = self.schedules.iter_mut().find(|j| j.id == job_id) {
                        j.status = ScheduleStatus::Error;
                        j.last_error = Some(error.message);
                        if kind == ScheduleKind::Cron {
                            j.due_at = now + CRON_STEP_MS;
                        }
                    }
                }
            }
            i += 1;
        }
        self.reap_ttl(now)
    }

    /// Removes documents whose TTL field value is a number strictly less than
    /// `now`, for every table that declares a `ttl`. Fires subscription fan-out
    /// for touched tables (mirroring `execute_transaction`). Returns the count
    /// removed. Ports TS `reapTtl` (`ts-client/src/in_memory.ts:1196-1221`).
    fn reap_ttl(&mut self, now: i64) -> usize {
        // Collect the (table, id) keys to remove — we can't mutate `self.docs`
        // while iterating it, so gather first then drain. A doc qualifies only
        // when its TTL field is a JSON number strictly less than `now`; a
        // missing or non-numeric TTL field is left alone (over-approximate
        // safely: never reap a doc that might still be live).
        let mut to_remove: Vec<(String, String)> = Vec::new();
        for (table_name, table_def) in &self.tables {
            let Some(ttl) = &table_def.ttl else {
                continue;
            };
            for ((t, id), row) in &self.docs {
                if t != table_name {
                    continue;
                }
                let reap = matches!(row.doc.get(&ttl.field),
                    Some(Value::Number(n)) if n.as_f64().is_some_and(|v| v < now as f64));
                if reap {
                    to_remove.push((table_name.clone(), id.clone()));
                }
            }
        }
        let mut removed = 0usize;
        let mut touched: BTreeSet<String> = BTreeSet::new();
        for key in &to_remove {
            if self.docs.remove(key).is_some() {
                removed += 1;
                touched.insert(key.0.clone());
            }
        }
        if !touched.is_empty() {
            self.notify_subs(&touched);
        }
        removed
    }

    /// Initial `due_at` for a schedule's `when`, mirroring `dueAtFor`
    /// (`ts-client/src/in_memory.ts:708-717`). `afterMs` is relative to `now`,
    /// `runAt` is absolute (in the past = fire on the next tick), and `cron`
    /// steps by `CRON_STEP_MS` from `now` (real cron parsing is server-side).
    fn due_at_for(&self, when: &ScheduleWhen, now: i64) -> i64 {
        match when {
            ScheduleWhen::AfterMs { ms } => now + ms,
            ScheduleWhen::RunAt { ms } => *ms,
            ScheduleWhen::Cron { .. } => now + CRON_STEP_MS,
        }
    }

    // ---- file storage ------------------------------------------------------
    //
    // Ports `upload`/`deleteFile`/`getFileMetadata`/`getUrl`
    // (`ts-client/src/in_memory.ts:644-677`). Storage is HTTP-only on the live
    // server; the in-memory harness mirrors the surface so unit tests can
    // exercise app storage flows with no network. `get_url` returns a
    // synthetic `memory://` handle — there is no real byte stream to serve.

    /// Stores `bytes` and returns a server-shaped [`UploadResult`]. The id is
    /// a short counter-prefixed token (distinct in shape from document ids).
    /// Ports `upload` (`ts-client/src/in_memory.ts:646-652`).
    pub fn upload(
        &mut self,
        bytes: Vec<u8>,
        content_type: Option<String>,
    ) -> Result<UploadResult, RtDbError> {
        self.id_counter += 1;
        let id = format!("f{}", base36(self.id_counter));
        let sha256 = sha256_hex(&bytes);
        let size = bytes.len() as i64;
        let created_at = (self.now)();
        self.storage.insert(
            id.clone(),
            StoredBlob {
                bytes,
                content_type: content_type.clone(),
                created_at,
                sha256: sha256.clone(),
            },
        );
        Ok(UploadResult {
            id,
            sha256,
            size,
            content_type,
        })
    }

    /// Deletes a stored blob. NOT_FOUND if unknown. Ports `deleteFile`
    /// (`ts-client/src/in_memory.ts:654-658`).
    pub fn delete_file(&mut self, id: &str) -> Result<(), RtDbError> {
        if self.storage.remove(id).is_none() {
            return Err(RtDbError::new(
                ErrorCode::NotFound,
                "unknown file".to_string(),
            ));
        }
        Ok(())
    }

    /// Reads back a stored blob's metadata. The returned `sha256` is the empty
    /// string — only the upload result carries the real digest (the live HTTP
    /// client does the same; tests that need the digest should keep the
    /// [`UploadResult`]). Ports `getFileMetadata`
    /// (`ts-client/src/in_memory.ts:660-672`).
    pub fn get_file_metadata(&self, id: &str) -> Result<FileMetadata, RtDbError> {
        let blob = self
            .storage
            .get(id)
            .ok_or_else(|| RtDbError::new(ErrorCode::NotFound, "unknown file".to_string()))?;
        Ok(FileMetadata {
            id: id.to_string(),
            sha256: String::new(),
            size: blob.bytes.len() as i64,
            content_type: blob.content_type.clone(),
            creation_time: blob.created_at,
        })
    }

    /// Synthetic handle — no real byte stream. Ports `getUrl`
    /// (`ts-client/src/in_memory.ts:675-677`).
    pub fn get_url(&self, id: &str) -> String {
        format!("memory://{id}")
    }
}

/// Builds the public [`ScheduleInfo`] view of an in-memory [`ScheduledJob`].
/// Mirrors `toScheduleInfo` (`ts-client/src/in_memory.ts:727-743`): `cron` and
/// `last_error` are present only when set.
fn schedule_info(job: &ScheduledJob) -> ScheduleInfo {
    ScheduleInfo {
        id: job.id.clone(),
        kind: job.kind,
        due_at: job.due_at,
        cron: job.cron.clone(),
        status: job.status,
        last_error: job.last_error.clone(),
        created_at: job.created_at,
        fired_count: job.fired_count,
    }
}

/// Lowercase base-36 encoding of `n`, matching `Number.prototype.toString(36)`
/// in the TS source (`ts-client/src/in_memory.ts:647`). Used for storage-id
/// minting; grows wider only after `z`, `10`, `11`, …
fn base36(n: u64) -> String {
    if n == 0 {
        return "0".to_string();
    }
    let mut out = Vec::new();
    let mut m = n;
    while m > 0 {
        let digit = (m % 36) as u32;
        let ch = char::from_digit(digit, 36).unwrap_or('0');
        out.push(ch);
        m /= 36;
    }
    out.iter().rev().collect()
}

/// SHA-256 hex digest of `bytes`. Mirrors `crypto.subtle.digest("SHA-256", …)`
/// in the TS source (`ts-client/src/in_memory.ts:648-649`).
fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut out = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write;
        // Unwrap is sound: hex formatting never errors on a single byte.
        let _ = write!(out, "{b:02x}");
    }
    out
}

// ---------------------------------------------------------------------------
// Free helpers — ports of the module-private functions in
// `ts-client/src/in_memory.ts`. Kept `pub` so task tests can exercise them
// directly (the TS source exports them via the module surface too).
// ---------------------------------------------------------------------------

/// Deep clone of a JSON doc. Docs are pure JSON — safe to round-trip — so
/// cloning is just [`Value::clone`]. Named to mirror the TS helper.
pub fn clone_value(value: &Value) -> Value {
    value.clone()
}

/// Canonical string form for change detection, independent of key order.
/// `serde_json` with default features uses a `BTreeMap`-backed `Map`, so
/// [`Value`] already serializes with sorted keys — `to_string` is canonical.
/// If `preserve_order` is ever enabled on the `serde_json` dep, replace this
/// with a key-sorting canonicalizer (same caveat as `optimistic.rs`).
pub fn canonical(value: &Value) -> String {
    value.to_string()
}

/// `true` iff `value` is a 32-char lowercase hex string (an `_id`). Mirrors
/// the TS `/^[0-9a-f]+$/` (lowercase only).
pub fn is_hex_id(value: &Value) -> bool {
    match value.as_str() {
        Some(s) if s.len() == 32 => s
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        _ => false,
    }
}

/// `true` iff `value` is a syntactically-valid integer string within `i64`
/// range (the wire form of an `int64` field). Mirrors the BigInt range check in
/// the TS source.
pub fn is_int64_string(value: &Value) -> bool {
    let s = match value.as_str() {
        Some(s) => s,
        None => return false,
    };
    // Strict `^-?\d+$`: an optional leading '-' then one or more ASCII digits.
    let digits = s.strip_prefix('-').unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    // i64 parse handles both sign and range.
    s.parse::<i64>().is_ok()
}

/// `true` iff `value` is a base64-shaped string: length a multiple of 4, body in
/// `[A-Za-z0-9+/]`, at most two trailing `=`. Mirrors the TS regex
/// `/^[A-Za-z0-9+/]*={0,2}$/`.
pub fn is_base64_string(value: &Value) -> bool {
    let s = match value.as_str() {
        Some(s) => s,
        None => return false,
    };
    if s.len() % 4 != 0 {
        return false;
    }
    let bytes = s.as_bytes();
    let eq_count = bytes.iter().rev().take_while(|&&b| b == b'=').count();
    eq_count <= 2
        && bytes[..bytes.len() - eq_count]
            .iter()
            .all(|&b| b.is_ascii_alphanumeric() || b == b'+' || b == b'/')
}

/// `true` iff `value` is a non-null, non-array JSON object. In `serde_json` the
/// only object kind is `Value::Object`, so this is `value.is_object()`.
pub fn is_plain_object(value: &Value) -> bool {
    value.is_object()
}

/// Recursive value validator — a port of server `schema::validate_value` and
/// the TS `validateValue` at `ts-client/src/in_memory.ts:150-198`. Switches on
/// the [`FieldType`] variant.
pub fn validate_value(ty: &FieldType, value: &Value) -> bool {
    match ty {
        FieldType::String => value.is_string(),
        FieldType::Number => value.is_number(),
        FieldType::Boolean => value.is_boolean(),
        FieldType::Null => value.is_null(),
        FieldType::Id { .. } => is_hex_id(value),
        FieldType::Literal { value: lit } => value == lit,
        FieldType::Optional { inner } => value.is_null() || validate_value(inner, value),
        FieldType::Union { variants } => variants.iter().any(|v| validate_value(v, value)),
        FieldType::Array { element } => value
            .as_array()
            .is_some_and(|arr| arr.iter().all(|item| validate_value(element, item))),
        FieldType::Object { fields } => {
            let map = match value.as_object() {
                Some(m) => m,
                None => return false,
            };
            // Reject unknown keys.
            for key in map.keys() {
                if !fields.contains_key(key) {
                    return false;
                }
            }
            // Declared fields: present-and-valid, or absent-and-optional.
            for (field, field_ty) in fields {
                match map.get(field) {
                    Some(v) => {
                        if !validate_value(field_ty, v) {
                            return false;
                        }
                    }
                    None if !matches!(field_ty, FieldType::Optional { .. }) => return false,
                    None => {}
                }
            }
            true
        }
        FieldType::Int64 => is_int64_string(value),
        FieldType::Bytes => is_base64_string(value),
        FieldType::Any => true,
        FieldType::Record { value: value_ty } => value
            .as_object()
            .is_some_and(|m| m.values().all(|v| validate_value(value_ty, v))),
        FieldType::Vector { dimensions } => {
            let arr = match value.as_array() {
                Some(a) => a,
                None => return false,
            };
            arr.len() == (*dimensions as usize)
                && arr
                    .iter()
                    .all(|v| v.as_f64().is_some_and(|f| f.is_finite()))
        }
    }
}

/// Full-document validator — a port of server `schema::validate_doc` and the TS
/// `validateDoc` at `ts-client/src/in_memory.ts:200-219`. Returns the first
/// violation as an [`RtDbError`] with code `SCHEMA_VIOLATION`.
///
/// Reserved (`_`-prefixed) and unknown fields are rejected, every declared
/// field is either present-and-valid or absent-and-optional.
pub fn validate_doc(table: &TableDef, doc: &Value) -> Result<(), RtDbError> {
    let map = doc.as_object();
    let map = match map {
        Some(m) => m,
        None => {
            return Err(RtDbError::new(
                ErrorCode::SchemaViolation,
                "document must be a JSON object",
            ));
        }
    };
    for key in map.keys() {
        if key.starts_with('_') {
            return Err(RtDbError::new(
                ErrorCode::SchemaViolation,
                format!("field '{key}' is reserved"),
            ));
        }
        if !table.fields.contains_key(key) {
            return Err(RtDbError::new(
                ErrorCode::SchemaViolation,
                format!("unknown field '{key}'"),
            ));
        }
    }
    for (field, field_ty) in &table.fields {
        match map.get(field) {
            Some(v) => {
                if !validate_value(field_ty, v) {
                    return Err(RtDbError::new(
                        ErrorCode::SchemaViolation,
                        format!("field '{field}' has an invalid value"),
                    ));
                }
            }
            None if !matches!(field_ty, FieldType::Optional { .. }) => {
                return Err(RtDbError::new(
                    ErrorCode::SchemaViolation,
                    format!("field '{field}' is required"),
                ));
            }
            None => {}
        }
    }
    Ok(())
}

/// Resolves a mutable table definition from a working schema, returning the
/// server-shaped `BAD_REQUEST` when the table is absent. Ports
/// `migrateTable` (`ts-client/src/in_memory.ts:965-971`) and mirrors server
/// `migrate::table_mut`.
#[cfg(feature = "admin")]
fn migrate_table_mut<'a>(
    schema: &'a mut SchemaDef,
    table: &str,
) -> Result<&'a mut TableDef, RtDbError> {
    schema.tables.get_mut(table).ok_or_else(|| {
        RtDbError::new(
            ErrorCode::BadRequest,
            format!("table '{table}' does not exist"),
        )
    })
}

/// True iff `cast` can coerce from `old` — a port of server
/// `migrate::cast_valid_for` and ts-client `castValidFor`. Locks the same
/// coercion matrix as the server (the closed set of sound source types per
/// cast).
#[cfg(feature = "admin")]
fn cast_valid_for(cast: crate::wire::admin::Cast, old: &FieldType) -> bool {
    use crate::wire::admin::Cast;
    use FieldType::*;
    matches!(
        (cast, old),
        (Cast::ToString, String | Number | Boolean | Int64)
            | (Cast::ToNumber, String | Boolean | Int64)
            | (Cast::ToInt64, String | Number)
            | (Cast::ToBoolean, String | Number)
    )
}

/// Pure Rust coercion mirroring server `migrate::coerce_value` and ts-client
/// `coerceValue`. Returns `None` if the value cannot be coerced under this cast;
/// the caller then substitutes `default` if supplied or raises a row-named
/// `BadRequest`. `ToInt64` emits a decimal-string JSON value (int64 travels as
/// a canonical decimal string on this wire — see `FEATURE_MATRIX.md` #13);
/// `ToNumber` emits a JSON number. The other casts produce the natural JSON
/// representation.
#[cfg(feature = "admin")]
fn coerce_value(cast: crate::wire::admin::Cast, v: &Value) -> Option<Value> {
    use crate::wire::admin::Cast;
    use serde_json::json;
    match (cast, v) {
        (Cast::ToString, Value::String(_)) => Some(v.clone()),
        (Cast::ToString, Value::Number(n)) => Some(Value::String(n.to_string())),
        (Cast::ToString, Value::Bool(b)) => Some(Value::String(b.to_string())),
        (Cast::ToString, _) => None,
        (Cast::ToNumber, Value::String(s)) => match s.parse::<f64>() {
            Ok(n) if n.is_finite() => Some(json!(n)),
            _ => None,
        },
        (Cast::ToNumber, Value::Number(_)) => Some(v.clone()),
        (Cast::ToNumber, Value::Bool(b)) => Some(json!(if *b { 1.0 } else { 0.0 })),
        (Cast::ToNumber, _) => None,
        (Cast::ToInt64, Value::String(s)) => s.parse::<i64>().ok().map(|i| json!(i.to_string())),
        (Cast::ToInt64, Value::Number(n)) => n.as_i64().map(|i| json!(i.to_string())),
        (Cast::ToInt64, _) => None,
        (Cast::ToBoolean, Value::String(s)) => match s.as_str() {
            "true" | "1" => Some(Value::Bool(true)),
            "false" | "0" => Some(Value::Bool(false)),
            _ => None,
        },
        (Cast::ToBoolean, Value::Number(n)) => {
            Some(Value::Bool(n.as_f64().map(|f| f != 0.0).unwrap_or(true)))
        }
        (Cast::ToBoolean, _) => None,
    }
}

/// Mirrors `server/src/ddl.rs::detect_destructive_changes`: walks `old` and
/// rejects any removed table, removed field, changed field type (except a safe
/// literal-union widening, which is additive and allowed — see
/// `schema::is_widening_of`), or removed/changed index with
/// [`ErrorCode::BadRequest`]. Additive changes (new tables, new fields, new
/// indexes, widening `Optional`/`Union`/`Any` inner types via a fresh push) pass
/// through — `push_schema` then folds the new schema into `self.tables` without
/// touching stored docs.
///
/// `FieldType`/`IndexDef`/`VectorIndexSpec` derive `PartialEq` (mirroring the
/// server), so structural equality is a direct `!=`.
fn detect_destructive_changes(old: &SchemaDef, new: &SchemaDef) -> Result<(), RtDbError> {
    for (table_name, old_table) in &old.tables {
        let new_table = new.tables.get(table_name).ok_or_else(|| {
            RtDbError::new(
                ErrorCode::BadRequest,
                format!("removed table '{table_name}'"),
            )
        })?;
        for (field_name, old_field_type) in &old_table.fields {
            match new_table.fields.get(field_name) {
                None => {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        format!("removed field '{table_name}.{field_name}'"),
                    ));
                }
                Some(new_field_type)
                    if old_field_type != new_field_type
                        && !is_widening_of(old_field_type, new_field_type) =>
                {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        format!("changed type of field '{table_name}.{field_name}'"),
                    ));
                }
                _ => {}
            }
        }
        for old_index in old_table.indexes.iter().flatten() {
            let new_index = new_table
                .indexes
                .iter()
                .flatten()
                .find(|i| i.name == old_index.name);
            let new_index = match new_index {
                None => {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        format!("removed index '{}'", old_index.name),
                    ));
                }
                Some(i) => i,
            };
            if new_index.fields != old_index.fields {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    format!("changed fields of index '{}'", old_index.name),
                ));
            }
            if new_index.search != old_index.search {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    format!(
                        "changed kind of index '{}' (btree <-> search)",
                        old_index.name
                    ),
                ));
            }
            if new_index.vector != old_index.vector {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    format!("changed vector spec of index '{}'", old_index.name),
                ));
            }
        }
    }
    Ok(())
}

/// Removes keys whose value is `null` for an `Optional` field whose inner type
/// does not itself accept `null` — a port of server `strip_unset_optionals` and
/// the TS helper at `ts-client/src/in_memory.ts:225-240`. An
/// inserted/patched-then-nulled optional lands as "key absent", matching the
/// server's single representation of an unset optional.
pub fn strip_unset_optionals(table: &TableDef, doc: &Value) -> Value {
    let map = match doc.as_object() {
        Some(m) => m,
        None => return doc.clone(),
    };
    let mut out = Map::new();
    for (key, value) in map {
        if value.is_null()
            && let Some(FieldType::Optional { inner }) = table.fields.get(key)
            && !validate_value(inner, value)
        {
            continue;
        }
        out.insert(key.clone(), value.clone());
    }
    Value::Object(out)
}

/// Stamps the TTL field at insert when the table declares a
/// `default_duration_ms` and the document omits the field. After this, the TTL
/// field is ordinary (patch/replace manipulate it normally). Mirrors server
/// `txn::stamp_ttl_default` and the TS `stampTtlDefault`
/// (`ts-client/src/in_memory.ts:407-425`); runs BEFORE validation so the
/// stamped value satisfies a required numeric field. Returns a cloned map with
/// the field set when a stamp is applied, otherwise the original doc cloned
/// unchanged.
fn stamp_ttl_default(
    table_def: &TableDef,
    doc: &Map<String, Value>,
    now: i64,
) -> Map<String, Value> {
    if let Some(ttl) = &table_def.ttl
        && let Some(d) = ttl.default_duration_ms
        && !doc.contains_key(&ttl.field)
    {
        let mut out = doc.clone();
        out.insert(ttl.field.clone(), Value::from(now + d));
        out
    } else {
        doc.clone()
    }
}

/// Applies a patch's `fields` onto `doc` — a port of server `txn::apply_patch`
/// and the TS `applyPatch` (`ts-client/src/in_memory.ts:243-265`). A `null`
/// onto an `Optional` field whose inner type doesn't itself accept `null`
/// deletes the key (mirroring `strip_unset_optionals`'s single representation
/// of an unset optional); the merged doc is then re-validated whole.
pub fn apply_patch(
    table: &TableDef,
    doc: &Value,
    fields: &Map<String, Value>,
) -> Result<Value, RtDbError> {
    let mut merged = match doc.as_object() {
        Some(m) => m.clone(),
        None => Map::new(),
    };
    for (field, value) in fields {
        let field_ty = match table.fields.get(field) {
            Some(t) => t,
            None => {
                return Err(RtDbError::new(
                    ErrorCode::SchemaViolation,
                    format!("unknown field '{field}'"),
                ));
            }
        };
        // null on an Optional<String> (or any Optional whose inner rejects null)
        // deletes the key — the server's strip_unset_optionals semantics.
        let strip = if let FieldType::Optional { inner } = field_ty {
            value.is_null() && !validate_value(inner, value)
        } else {
            false
        };
        if strip {
            merged.remove(field);
            continue;
        }
        if !validate_value(field_ty, value) {
            return Err(RtDbError::new(
                ErrorCode::SchemaViolation,
                format!("field '{field}' has an invalid value"),
            ));
        }
        merged.insert(field.clone(), value.clone());
    }
    let merged_value = Value::Object(merged);
    validate_doc(table, &merged_value)?;
    Ok(merged_value)
}

/// Lowercase camelCase type tag for a [`FieldType`] — used in error messages
/// (mirrors `typeTag` in `ts-client/src/in_memory.ts:267-269` and the serde tag
/// on [`FieldType`]).
pub fn type_tag(ty: &FieldType) -> &'static str {
    match ty {
        FieldType::String => "string",
        FieldType::Number => "number",
        FieldType::Boolean => "boolean",
        FieldType::Null => "null",
        FieldType::Id { .. } => "id",
        FieldType::Literal { .. } => "literal",
        FieldType::Optional { .. } => "optional",
        FieldType::Union { .. } => "union",
        FieldType::Array { .. } => "array",
        FieldType::Object { .. } => "object",
        FieldType::Int64 => "int64",
        FieldType::Bytes => "bytes",
        FieldType::Any => "any",
        FieldType::Record { .. } => "record",
        FieldType::Vector { .. } => "vector",
    }
}

/// Indexed-column storage type, mirroring server `indexed_column_type` and the
/// TS `IndexedType` (`ts-client/src/in_memory.ts:43-49`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgType {
    Text,
    Number,
    Boolean,
    Int64,
}

/// Shape returned by [`index_column_type`]: the storage type plus whether the
/// source field was wrapped in `Optional` (so callers can let null sort).
#[derive(Debug, Clone, Copy)]
pub struct IndexedType {
    pub pg: PgType,
    pub nullable: bool,
}

/// Indexable column type — a port of server `schema::indexed_column_type` and
/// the TS `indexColumnType` (`ts-client/src/in_memory.ts:271-298`). Returns
/// SCHEMA_VIOLATION for non-indexable types.
pub fn index_column_type(ty: &FieldType) -> Result<IndexedType, RtDbError> {
    let pg = match ty {
        FieldType::String | FieldType::Id { .. } => PgType::Text,
        FieldType::Number => PgType::Number,
        FieldType::Boolean => PgType::Boolean,
        FieldType::Int64 => PgType::Int64,
        FieldType::Literal {
            value: Value::String(_),
        } => PgType::Text,
        FieldType::Literal { .. } => {
            return Err(RtDbError::new(
                ErrorCode::SchemaViolation,
                format!("field type '{}' is not indexable", type_tag(ty)),
            ));
        }
        FieldType::Union { variants } => {
            if variants.iter().all(|v| {
                matches!(
                    v,
                    FieldType::Literal {
                        value: Value::String(_)
                    }
                )
            }) {
                PgType::Text
            } else {
                return Err(RtDbError::new(
                    ErrorCode::SchemaViolation,
                    format!("field type '{}' is not indexable", type_tag(ty)),
                ));
            }
        }
        FieldType::Optional { inner } => {
            let inner_ty = index_column_type(inner)?;
            return Ok(IndexedType {
                pg: inner_ty.pg,
                nullable: true,
            });
        }
        _ => {
            return Err(RtDbError::new(
                ErrorCode::SchemaViolation,
                format!("field type '{}' is not indexable", type_tag(ty)),
            ));
        }
    };
    Ok(IndexedType {
        pg,
        nullable: false,
    })
}

/// Type-checks an eq/range bind value, mirroring server `eq_bind_for` and the
/// TS `coerceIndexValue` (`ts-client/src/in_memory.ts:301-324`). Returns the
/// value unchanged on success.
pub fn coerce_index_value(
    table: &TableDef,
    field_name: &str,
    value: &Value,
) -> Result<Value, RtDbError> {
    let field_ty = table.fields.get(field_name).ok_or_else(|| {
        RtDbError::new(
            ErrorCode::Internal,
            format!("index references unknown field '{field_name}'"),
        )
    })?;
    let indexed = index_column_type(field_ty)?;
    match indexed.pg {
        PgType::Text => {
            if !value.is_string() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "eq value must be a string",
                ));
            }
        }
        PgType::Number => {
            if !value.is_number() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "eq value must be a number",
                ));
            }
        }
        PgType::Boolean => {
            if !value.is_boolean() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "eq value must be a boolean",
                ));
            }
        }
        PgType::Int64 => {
            // Int64 fields are stored as decimal strings; eq stays structural
            // equality on the string, so the value is returned unchanged. We
            // only validate that it parses as `i64` (mirrors `is_int64_string`).
            match value.as_str().and_then(|s| s.parse::<i64>().ok()) {
                Some(_) => {}
                None => {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        "eq value must be an int64 string",
                    ));
                }
            }
        }
    }
    Ok(value.clone())
}

/// Null-sorting comparison for one index sort key. Mirrors `compareIndexValues`
/// (`ts-client/src/in_memory.ts:329-350`): numbers compare numerically, strings
/// lexicographically, booleans as `false < true`; nulls sort last (asc) / first
/// (desc, via the caller flipping the result). Mixed types fall back to
/// [`Ordering::Equal`](std::cmp::Ordering) — indexed columns are single-type by schema, so this is
/// unreachable in practice.
///
/// `pg` selects the comparison domain. `PgType::Int64` parses the decimal
/// string to `i64` so int64 index values sort/range numerically (3 < 20 < 100)
/// rather than lexicographically (100 < 20 < 3); the on-the-wire representation
/// stays a string, so eq remains structural equality on the `Value`.
pub fn compare_index_values(a: &Value, b: &Value, pg: PgType) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let a_null = a.is_null();
    let b_null = b.is_null();
    if a_null && b_null {
        return Ordering::Equal;
    }
    if a_null {
        return Ordering::Greater;
    }
    if b_null {
        return Ordering::Less;
    }
    if pg == PgType::Int64 {
        let an = a
            .as_str()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(i64::MIN);
        let bn = b
            .as_str()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(i64::MIN);
        return an.cmp(&bn);
    }
    match (a, b) {
        (Value::Number(an), Value::Number(bn)) => {
            let av = an.as_f64().unwrap_or(f64::NAN);
            let bv = bn.as_f64().unwrap_or(f64::NAN);
            av.partial_cmp(&bv).unwrap_or(Ordering::Equal)
        }
        (Value::String(as_), Value::String(bs_)) => as_.cmp(bs_),
        (Value::Bool(ab), Value::Bool(bb)) => ab.cmp(bb),
        _ => Ordering::Equal,
    }
}

/// Applies one aggregate op over a non-empty slice of values, mirroring the
/// server's SQL semantics and ts `applyAggregate` (`in_memory.ts:432-449`).
/// SUM/AVG reduce numerically (`int64` values are decimal strings → parsed);
/// MIN/MAX pick the smallest/largest per [`compare_index_values`], so a string
/// field's extremes match Postgres lexicographic ordering. Only called on
/// non-empty input — the caller maps an empty set to JSON null.
pub fn apply_aggregate(op: AggregateOp, values: &[Value], pg: PgType) -> Value {
    match op {
        AggregateOp::Sum | AggregateOp::Avg => {
            let sum: f64 = values.iter().filter_map(|v| numeric_value(v, pg)).sum();
            let result = if matches!(op, AggregateOp::Avg) {
                sum / values.len() as f64
            } else {
                sum
            };
            serde_json::Number::from_f64(result)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        AggregateOp::Min | AggregateOp::Max => {
            let want_less = matches!(op, AggregateOp::Min);
            let mut best = &values[0];
            for v in &values[1..] {
                let cmp = compare_index_values(v, best, pg);
                if (want_less && cmp == std::cmp::Ordering::Less)
                    || (!want_less && cmp == std::cmp::Ordering::Greater)
                {
                    best = v;
                }
            }
            best.clone()
        }
        // Count counts rows and is handled by an early return in the aggregate
        // path (it consumes no field); this arm is for exhaustiveness when the
        // helper is called directly — it returns the count of provided values.
        AggregateOp::Count => Value::Number(serde_json::Number::from(values.len() as i64)),
    }
}

/// Parses an index value to `f64` for SUM/AVG. `Number` columns are JSON
/// numbers; `int64` columns are decimal strings on the wire and in this harness.
fn numeric_value(v: &Value, pg: PgType) -> Option<f64> {
    match pg {
        PgType::Int64 => v.as_str().and_then(|s| s.parse::<f64>().ok()),
        _ => v.as_f64(),
    }
}

/// Merges a stored row with its system fields — a port of server `merge_doc`
/// and the TS `mergeDoc` (`ts-client/src/in_memory.ts:1154-1156`). The stored
/// `doc` is the user-written payload; system fields (`_id`/`_creationTime`/
/// `_version`) are layered on top at read time so they always reflect the
/// current `StoredRow` identity/history.
pub fn merge_doc(row: &StoredRow) -> Value {
    let mut out = match row.doc.as_object() {
        Some(m) => m.clone(),
        None => Map::new(),
    };
    out.insert("_id".to_string(), Value::String(row.id.clone()));
    out.insert(
        "_creationTime".to_string(),
        Value::Number(serde_json::Number::from(row.created_at)),
    );
    out.insert(
        "_version".to_string(),
        Value::Number(serde_json::Number::from(row.version)),
    );
    Value::Object(out)
}

/// Collect a unique-index collision key from `doc` over the declared `fields`.
/// Returns `Some([&Value; n])` positionally, or `None` if ANY indexed field is
/// absent or null in `doc` — mirroring Postgres UNIQUE, which treats NULLs as
/// distinct (a row with a NULL key column never collides). Used by
/// [`InMemoryRtDbClient::check_unique_indexes`]; the returned key lives only as
/// long as `doc` (the caller compares positionally against another key built
/// from a doc of equal or longer lifetime).
fn collect_index_key<'a>(fields: &[String], doc: &'a Value) -> Option<Vec<&'a Value>> {
    let mut key = Vec::with_capacity(fields.len());
    for field in fields {
        match doc.get(field) {
            Some(v) if !v.is_null() => key.push(v),
            _ => return None,
        }
    }
    Some(key)
}

/// Flip an [`std::cmp::Ordering`] by the query's sort direction: identity for
/// `Asc`, reversed for `Desc`. Used by the sort comparator in
/// [`InMemoryRtDbClient::run_query`] so the same comparison serves either
/// direction. Inline in the TS source (`dir === "desc" ? -cmp : cmp`).
fn dir_order(o: std::cmp::Ordering, dir: Order) -> std::cmp::Ordering {
    match dir {
        Order::Asc => o,
        Order::Desc => o.reverse(),
    }
}

// ---------------------------------------------------------------------------
// Cursor-keyset pagination — a port of TS `paginateResult` and its helpers
// (`ts-client/src/in_memory.ts:1164-1290`). The cursor stores one value per
// sort column (unbound index fields, then `_creationTime`, then `_id`); the
// resume predicate is the standard OR-of-AND row-value comparison, so paging
// is stable — the unique `id` tiebreaker means no row is skipped or duplicated
// across pages.
// ---------------------------------------------------------------------------

/// A sort column for keyset pagination — either an indexed field or one of the
/// two synthetic tiebreakers. Mirrors the TS `sortKeys` sentinel strings
/// `__createdAt` / `__id` (`ts-client/src/in_memory.ts:1119-1120`) without
/// risking a collision with a real field name.
enum SortCol {
    Index(String),
    CreatedAt,
    Id,
}

/// Cursor keyset pagination. `sorted` is already filtered (eq/range) and
/// sorted over `sort_cols` in direction `dir`. Returns a `Value` shaped as
/// `{docs, nextCursor?}` — the wire `Paginated<T>` (camelCase field names match
/// [`crate::query::Paginated`] and the TS `PaginatedResultJson`).
///
/// Fetch one past the page size so a next page is detectable without a second
/// pass; the extra is discarded after the has-next check (server `LIMIT n+1`).
/// The next cursor is built from the page's last row; absent when the page is
/// empty or this was the final page.
fn paginate_result(
    paginate: &crate::query::Paginate,
    table_def: &TableDef,
    sorted: &[StoredRow],
    sort_cols: &[SortCol],
    col_types: &[PgType],
    dir: Order,
) -> Result<Value, RtDbError> {
    let num_items = std::cmp::min(paginate.num_items as usize, MAX_TAKE);

    // Decode + structurally validate the cursor (BAD_REQUEST on any failure —
    // the codec returns INTERNAL, so rewrap to match the live client's surface
    // and the TS `decodePaginateCursor` rethrow at `:1206-1217`).
    let cursor_values: Option<Vec<Value>> = match &paginate.cursor {
        None => None,
        Some(cursor) => {
            let decoded = crate::cursor::decode_cursor(cursor).map_err(|e| {
                RtDbError::new(
                    ErrorCode::BadRequest,
                    format!("invalid cursor: {}", e.message),
                )
            })?;
            if decoded.len() != sort_cols.len() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    format!(
                        "cursor has {} value(s) but this query sorts over {} column(s)",
                        decoded.len(),
                        sort_cols.len()
                    ),
                ));
            }
            validate_cursor_values(&decoded, sort_cols, table_def)?;
            Some(decoded)
        }
    };

    // Apply the keyset resume predicate (strictly-after in the sort direction).
    let rows: Vec<&StoredRow> = match &cursor_values {
        Some(cv) => sorted
            .iter()
            .filter(|row| is_after_cursor(row, cv, sort_cols, col_types, dir))
            .collect(),
        None => sorted.iter().collect(),
    };

    let has_next = rows.len() > num_items;
    let page: Vec<&StoredRow> = rows.into_iter().take(num_items).collect();
    let docs: Vec<Value> = page.iter().map(|row| merge_doc(row)).collect();

    let next_cursor = match (has_next, page.last()) {
        (true, Some(last)) => {
            let keyset: Vec<Value> = sort_cols.iter().map(|c| sort_value(last, c)).collect();
            Some(crate::cursor::encode_cursor(&keyset)?)
        }
        _ => None,
    };

    let mut out = Map::new();
    out.insert("docs".to_string(), Value::Array(docs));
    if let Some(nc) = next_cursor {
        out.insert("nextCursor".to_string(), Value::String(nc));
    }
    Ok(Value::Object(out))
}

/// Type-checks decoded cursor values positionally against the sort columns —
/// a port of TS `validateCursorValues`
/// (`ts-client/src/in_memory.ts:1223-1244`). Index columns use
/// [`coerce_index_value`] (null is a legitimate optional-field value, so only
/// present values are type-checked); the final two columns are always
/// `_creationTime` (number) and `_id` (string).
fn validate_cursor_values(
    cursor_values: &[Value],
    sort_cols: &[SortCol],
    table_def: &TableDef,
) -> Result<(), RtDbError> {
    for (i, col) in sort_cols.iter().enumerate() {
        let value = &cursor_values[i];
        match col {
            SortCol::Index(field) => {
                if !value.is_null() {
                    coerce_index_value(table_def, field, value)?;
                }
            }
            SortCol::CreatedAt => {
                if !value.is_number() {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        "cursor value for created_at must be a number",
                    ));
                }
            }
            SortCol::Id => {
                if !value.is_string() {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        "cursor value for id must be a string",
                    ));
                }
            }
        }
    }
    Ok(())
}

/// The keyset resume predicate: true when `row` sorts strictly after the cursor
/// row. This is the lexicographic "greater than" expanded to OR-of-AND —
///
///   (c0 OP v0) OR (c0 = v0 AND c1 OP v1) OR … —
///
/// where OP is `>` (asc) / `<` (desc). Evaluated with the same null-sorts-last
/// comparator as the sort, so it agrees with the ordering that produced
/// `sorted`. Ports `isAfterCursor` (`ts-client/src/in_memory.ts:1253-1276`).
///
/// `col_types` is the per-column storage type parallel to `sort_cols`, used to
/// select the comparison domain (int64 needs numeric parsing).
fn is_after_cursor(
    row: &StoredRow,
    cursor_values: &[Value],
    sort_cols: &[SortCol],
    col_types: &[PgType],
    dir: Order,
) -> bool {
    for i in 0..sort_cols.len() {
        let mut prefix_equal = true;
        for j in 0..i {
            let row_v = sort_value(row, &sort_cols[j]);
            if compare_index_values(&row_v, &cursor_values[j], col_types[j])
                != std::cmp::Ordering::Equal
            {
                prefix_equal = false;
                break;
            }
        }
        if !prefix_equal {
            continue;
        }
        let row_v = sort_value(row, &sort_cols[i]);
        let cmp = compare_index_values(&row_v, &cursor_values[i], col_types[i]);
        let ahead = match dir {
            Order::Asc => cmp == std::cmp::Ordering::Greater,
            Order::Desc => cmp == std::cmp::Ordering::Less,
        };
        if ahead {
            return true;
        }
    }
    false
}

/// Sort value for a column, normalizing an absent optional index field to
/// null. Ports TS `sortValue` (`ts-client/src/in_memory.ts:1281-1290`).
fn sort_value(row: &StoredRow, col: &SortCol) -> Value {
    match col {
        SortCol::CreatedAt => Value::Number(serde_json::Number::from(row.created_at)),
        SortCol::Id => Value::String(row.id.clone()),
        SortCol::Index(field) => row.doc.get(field).cloned().unwrap_or(Value::Null),
    }
}

// ---------------------------------------------------------------------------
// Filter evaluation — a port of `validateFilter`/`evalFilterExpr` and the leaf
// helpers in `ts-client/src/in_memory.ts:361-488`. The server compiles a
// `FilterExpr` once against the table's declared fields
// (`query::compile_filter`), then evaluates the compiled predicate per row
// (`query::jsonb_lhs_and_bind`). This harness mirrors that two-phase split:
// [`validate_filter`] runs once in `run_query` before the row loop,
// [`eval_filter_expr`] runs per row inside [`matches_filter`].
// ---------------------------------------------------------------------------

/// The six leaf comparison operators, mirroring `FilterLeafOp` in the TS
/// source. Used as the dispatch key for [`compare_leaf`]/[`compare_values`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
}

/// Value-kind domain that picks the comparison semantics for a leaf, mirroring
/// `inValueKind`'s three variants. Post-[`check_leaf_value`] the
/// `Boolean` fallthrough is unreachable — every value is one of the three.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ValueKind {
    String,
    Number,
    Boolean,
}

/// Structural validation of a [`FilterExpr`] against a table's declared fields,
/// mirroring server `query::compile_filter` and the TS `validateFilter`
/// (`ts-client/src/in_memory.ts:361-386`). Returns `BAD_REQUEST` for: an empty
/// `and`/`or`, an empty `in`, an unknown field, a non-string/number/boolean
/// leaf value, or mixed-type `in` values. Call once before evaluating per row.
pub fn validate_filter(expr: &FilterExpr, fields: &BTreeSet<String>) -> Result<(), RtDbError> {
    match expr {
        FilterExpr::And { exprs } => {
            if exprs.is_empty() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "and filter requires at least one expr",
                ));
            }
            for e in exprs {
                validate_filter(e, fields)?;
            }
            Ok(())
        }
        FilterExpr::Or { exprs } => {
            if exprs.is_empty() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "or filter requires at least one expr",
                ));
            }
            for e in exprs {
                validate_filter(e, fields)?;
            }
            Ok(())
        }
        FilterExpr::In { field, values } => {
            if values.is_empty() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "in filter requires at least one value",
                ));
            }
            for v in values {
                check_leaf_value(field, v, fields)?;
            }
            let first_kind = in_value_kind(&values[0]);
            for v in &values[1..] {
                if in_value_kind(v) != first_kind {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        "in filter values must all be the same type",
                    ));
                }
            }
            Ok(())
        }
        FilterExpr::Eq { field, value }
        | FilterExpr::Neq { field, value }
        | FilterExpr::Gt { field, value }
        | FilterExpr::Gte { field, value }
        | FilterExpr::Lt { field, value }
        | FilterExpr::Lte { field, value } => check_leaf_value(field, value, fields),
        FilterExpr::Not { expr } => validate_filter(expr, fields),
        FilterExpr::Contains { field, value } => check_leaf_value(field, value, fields),
        FilterExpr::Exists { field } => {
            if !fields.contains(field) {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    format!("filter references unknown field '{field}'"),
                ));
            }
            Ok(())
        }
    }
}

/// `BAD_REQUEST` if `field` is not in the table's declared fields or `value`
/// is not a string/number/boolean. Mirrors `checkLeafValue`
/// (`ts-client/src/in_memory.ts:388-395`).
fn check_leaf_value(
    field: &str,
    value: &Value,
    fields: &BTreeSet<String>,
) -> Result<(), RtDbError> {
    if !fields.contains(field) {
        return Err(RtDbError::new(
            ErrorCode::BadRequest,
            format!("filter references unknown field '{field}'"),
        ));
    }
    if !matches!(value, Value::String(_) | Value::Number(_) | Value::Bool(_)) {
        return Err(RtDbError::new(
            ErrorCode::BadRequest,
            "filter value must be a string, number, or boolean",
        ));
    }
    Ok(())
}

/// Value-kind domain for an `in` value, mirroring `inValueKind`
/// (`ts-client/src/in_memory.ts:397-401`).
fn in_value_kind(value: &Value) -> ValueKind {
    match value {
        Value::String(_) => ValueKind::String,
        Value::Number(_) => ValueKind::Number,
        _ => ValueKind::Boolean,
    }
}

/// Evaluate a [`FilterExpr`] predicate against a stored doc, mirroring server
/// `query::jsonb_lhs_and_bind` and the TS `evalFilterExpr`
/// (`ts-client/src/in_memory.ts:410-421`): the filter value's kind picks the
/// comparison domain — string compares the doc field's `->>` text, number
/// compares it as `float8`, boolean as `boolean`. A null/absent field never
/// matches (SQL NULL exclusion). Assumes [`validate_filter`] already passed.
pub fn eval_filter_expr(expr: &FilterExpr, doc: &Value) -> bool {
    match expr {
        FilterExpr::And { exprs } => exprs.iter().all(|e| eval_filter_expr(e, doc)),
        FilterExpr::Or { exprs } => exprs.iter().any(|e| eval_filter_expr(e, doc)),
        FilterExpr::In { field, values } => values
            .iter()
            .any(|v| compare_leaf(FilterOp::Eq, field, v, doc)),
        FilterExpr::Eq { field, value } => compare_leaf(FilterOp::Eq, field, value, doc),
        FilterExpr::Neq { field, value } => compare_leaf(FilterOp::Neq, field, value, doc),
        FilterExpr::Gt { field, value } => compare_leaf(FilterOp::Gt, field, value, doc),
        FilterExpr::Gte { field, value } => compare_leaf(FilterOp::Gte, field, value, doc),
        FilterExpr::Lt { field, value } => compare_leaf(FilterOp::Lt, field, value, doc),
        FilterExpr::Lte { field, value } => compare_leaf(FilterOp::Lte, field, value, doc),
        FilterExpr::Not { expr } => !eval_filter_expr(expr, doc),
        FilterExpr::Contains { field, value } => match doc.get(field) {
            Some(Value::Array(arr)) => arr.iter().any(|v| v == value),
            _ => false,
        },
        FilterExpr::Exists { field } => matches!(doc.get(field), Some(v) if !v.is_null()),
    }
}

/// Per-leaf comparison, mirroring `compareLeaf`
/// (`ts-client/src/in_memory.ts:423-444`). `doc[field]` null/absent → `false`
/// (SQL NULL exclusion); the filter value's kind picks the comparison domain.
fn compare_leaf(op: FilterOp, field: &str, filter_value: &Value, doc: &Value) -> bool {
    let doc_val = match doc.get(field) {
        Some(v) if !v.is_null() => v,
        _ => return false,
    };
    match filter_value {
        Value::String(s) => {
            let lhs = doc_to_text(doc_val);
            compare_values(op, &lhs, s)
        }
        Value::Number(_) => match doc_to_number(doc_val) {
            Some(lhs) => match filter_value.as_f64() {
                Some(rhs) => compare_values(op, &lhs, &rhs),
                None => false,
            },
            None => false,
        },
        Value::Bool(b) => match doc_val {
            Value::Bool(db) => compare_values(op, db, b),
            _ => false,
        },
        // Unreachable post-validate (`check_leaf_value` rejects non-string/
        // number/boolean values); defensively treat as no-match.
        _ => false,
    }
}

/// Mirrors Postgres `doc->>'field'`: the JSON text of the value. Ports
/// `docToText` (`ts-client/src/in_memory.ts:447-452`) — string→as-is,
/// number→`JSON.stringify(n)` (integer-valued numbers render without a decimal
/// point, matching JS), boolean→"true"/"false", else JSON text.
fn doc_to_text(doc_val: &Value) -> String {
    match doc_val {
        Value::String(s) => s.clone(),
        // JS `JSON.stringify` renders integer-valued numbers with no decimal
        // (`5`, not `5.0`); serde_json emits `"5.0"` for a float-backed
        // integer, so route integer-backed numbers to their exact form and
        // canonicalize integer-valued floats within JS's safe-integer range.
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.to_string()
            } else if let Some(u) = n.as_u64() {
                u.to_string()
            } else if let Some(f) = n.as_f64() {
                if f.is_finite() && f.fract() == 0.0 && f.abs() <= 9_007_199_254_740_992.0 {
                    (f as i64).to_string()
                } else {
                    n.to_string()
                }
            } else {
                n.to_string()
            }
        }
        Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

/// Mirrors Postgres `(doc->>'field')::float8`: a finite number, or a parsed
/// numeric string. Ports `docToNumber` (`ts-client/src/in_memory.ts:455-462`).
fn doc_to_number(doc_val: &Value) -> Option<f64> {
    match doc_val {
        Value::Number(n) => n.as_f64().filter(|f| f.is_finite()),
        Value::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                return None;
            }
            trimmed.parse::<f64>().ok().filter(|f| f.is_finite())
        }
        _ => None,
    }
}

/// Op dispatch over a same-typed pair (string/number/boolean — the filter
/// value's kind fixes the domain, so the operands never mix). Ports
/// `compareValues` (`ts-client/src/in_memory.ts:464-483`).
fn compare_values<T: PartialEq + PartialOrd>(op: FilterOp, lhs: &T, rhs: &T) -> bool {
    match op {
        FilterOp::Eq => lhs == rhs,
        FilterOp::Neq => lhs != rhs,
        FilterOp::Gt => lhs > rhs,
        FilterOp::Gte => lhs >= rhs,
        FilterOp::Lt => lhs < rhs,
        FilterOp::Lte => lhs <= rhs,
    }
}

/// Filter hook for [`InMemoryRtDbClient::run_query`]. Delegates to
/// [`eval_filter_expr`]; validation runs once in `run_query` before the row
/// loop, so by the time this runs the filter is structurally sound.
fn matches_filter(expr: &FilterExpr, doc: &Value) -> bool {
    eval_filter_expr(expr, doc)
}

/// Looks up an index by name (BAD_REQUEST if absent). Free function so it's
/// callable without `&self`. Ports `requireIndex`
/// (`ts-client/src/in_memory.ts:1328-1334`).
fn require_index<'a>(table_def: &'a TableDef, name: &str) -> Result<&'a IndexDef, RtDbError> {
    let indexes = table_def.indexes.as_ref().ok_or_else(|| {
        RtDbError::new(ErrorCode::BadRequest, format!("index '{name}' not found"))
    })?;
    indexes
        .iter()
        .find(|i| i.name == name)
        .ok_or_else(|| RtDbError::new(ErrorCode::BadRequest, format!("index '{name}' not found")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutation::Mutation;
    use crate::query::{Paginate, Paginated, SearchOpts, TableQuery, VectorSearchOpts};
    use crate::schema::{Schema, Table};
    use crate::wire::{AggregateOp, AggregateSpec, FilterExpr};
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    /// The test schema mirrored from `ts-client/tests/in_memory.test.ts:10-20`.
    fn test_schema() -> SchemaDef {
        Schema::builder()
            .table(
                "items",
                Table::new()
                    .field("name", FieldType::String)
                    .field("status", FieldType::String)
                    .field("order", FieldType::Number)
                    .field("note", FieldType::optional(FieldType::String))
                    .index("by_name", &["name"])
                    .index("by_status", &["status"])
                    .index("by_status_and_order", &["status", "order"]),
            )
            .build()
    }

    fn items_table(schema: &SchemaDef) -> &TableDef {
        schema.tables.get("items").expect("items table present")
    }

    // ---- schema push ---------------------------------------------------

    #[test]
    fn push_schema_stores_the_schema() {
        // Mirrors the TS "schema push" suite: after pushSchema, the schema is
        // installed and the table is known (the TS suite verifies this by
        // running `query().collect()` and getting `[]`; here we verify the
        // schema snapshot directly because query/collect land in task 3).
        let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
        let schema = test_schema();
        c.push_schema(&schema).unwrap();
        let stored = c.to_schema_json().expect("schema installed");
        assert!(stored.tables.contains_key("items"));
        assert!(c.tables.contains_key("items"));
    }

    #[test]
    fn push_schema_rejects_a_destructive_second_push() {
        // Server parity (ddl.rs::detect_destructive_changes): a second push
        // missing a previously-declared table is rejected with BadRequest and
        // the exact "removed table '<name>'" message; nothing is mutated.
        let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
        c.push_schema(&test_schema()).unwrap();
        let only_other = Schema::builder()
            .table("solo", Table::new().field("x", FieldType::Number))
            .build();
        let err = c.push_schema(&only_other).unwrap_err();
        assert!(
            matches!(err.code, ErrorCode::BadRequest),
            "got: {:?}",
            err.code
        );
        assert!(err.message.contains("removed table 'items'"), "got: {err}");
        // The rejected push left the prior schema in place.
        let stored = c.to_schema_json().expect("schema still installed");
        assert!(stored.tables.contains_key("items"));
        assert!(c.tables.contains_key("items"));
        assert!(!stored.tables.contains_key("solo"));
    }

    #[tokio::test]
    async fn push_schema_additively_preserves_docs() {
        // An additive second push (new optional field + new table) preserves
        // previously-inserted docs and the prior idempotency cache.
        let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
        c.push_schema(&test_schema()).unwrap();
        c.mutate(
            &Mutation::new()
                .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
                .build(),
            Some("m1"),
        )
        .await
        .unwrap();
        // Add a new optional field on `items` and an entirely new `users` table.
        let additive = Schema::builder()
            .table(
                "items",
                Table::new()
                    .field("name", FieldType::String)
                    .field("status", FieldType::String)
                    .field("order", FieldType::Number)
                    .field("note", FieldType::optional(FieldType::String))
                    .field("priority", FieldType::optional(FieldType::Number))
                    .index("by_name", &["name"])
                    .index("by_status", &["status"])
                    .index("by_status_and_order", &["status", "order"]),
            )
            .table("users", Table::new().field("email", FieldType::String))
            .build();
        c.push_schema(&additive).unwrap();
        // The new field/table are folded in…
        let stored = c.to_schema_json().expect("schema installed");
        assert!(stored.tables.contains_key("users"));
        assert!(stored.tables["items"].fields.contains_key("priority"));
        // …and the pre-existing row is still queryable.
        let r = c
            .run_query(&Query {
                table: "items".into(),
                ..Default::default()
            })
            .unwrap();
        let docs = r.as_array().expect("collect returns an array");
        assert_eq!(docs.len(), 1, "pre-existing row survived the additive push");
        assert_eq!(docs[0]["name"], json!("a"));
        // Idempotency cache is preserved across the additive push.
        c.mutate(
            &Mutation::new()
                .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
                .build(),
            Some("m1"),
        )
        .await
        .expect("idempotency cache hit short-circuits with the cached results");
    }

    #[test]
    fn push_schema_allows_widening_a_literal_union() {
        // Server parity (schema::is_widening_of): a second push that widens a
        // finite literal-union field — adding a variant — is additive and
        // accepted, mirroring the live server's `pushSchema` behavior.
        let union_field =
            || FieldType::union([FieldType::literal("backlog"), FieldType::literal("done")]);
        let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
        let first = Schema::builder()
            .table(
                "items",
                Table::new()
                    .field("title", FieldType::String)
                    .field("status", union_field()),
            )
            .build();
        c.push_schema(&first).unwrap();
        // Widen {backlog, done} -> {backlog, done, archived}.
        let widened = Schema::builder()
            .table(
                "items",
                Table::new().field("title", FieldType::String).field(
                    "status",
                    FieldType::union([
                        FieldType::literal("backlog"),
                        FieldType::literal("done"),
                        FieldType::literal("archived"),
                    ]),
                ),
            )
            .build();
        c.push_schema(&widened).expect("widening push succeeds");
        // The widened field type is folded into the stored schema.
        let stored = c.to_schema_json().expect("schema installed");
        let status = stored.tables["items"]
            .fields
            .get("status")
            .expect("status present");
        match status {
            FieldType::Union { variants } => assert_eq!(variants.len(), 3),
            other => panic!("expected Union, got {other:?}"),
        }
    }

    #[test]
    fn push_schema_rejects_narrowing_a_literal_union() {
        // Server parity: a second push that narrows a literal-union field —
        // dropping a variant some rows may hold — is destructive and rejected
        // with BadRequest and the "changed type of field" message.
        let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
        let first = Schema::builder()
            .table(
                "items",
                Table::new().field("title", FieldType::String).field(
                    "status",
                    FieldType::union([
                        FieldType::literal("backlog"),
                        FieldType::literal("done"),
                        FieldType::literal("archived"),
                    ]),
                ),
            )
            .build();
        c.push_schema(&first).unwrap();
        // Narrow {backlog, done, archived} -> {backlog, done}.
        let narrowed = Schema::builder()
            .table(
                "items",
                Table::new().field("title", FieldType::String).field(
                    "status",
                    FieldType::union([FieldType::literal("backlog"), FieldType::literal("done")]),
                ),
            )
            .build();
        let err = c.push_schema(&narrowed).unwrap_err();
        assert!(
            matches!(err.code, ErrorCode::BadRequest),
            "got: {:?}",
            err.code
        );
        assert!(
            err.message.contains("changed type of field 'items.status'"),
            "got: {err}"
        );
        // The rejected push left the prior (3-variant) schema in place.
        let stored = c.to_schema_json().expect("schema still installed");
        match stored.tables["items"]
            .fields
            .get("status")
            .expect("status present")
        {
            FieldType::Union { variants } => assert_eq!(variants.len(), 3),
            other => panic!("expected Union, got {other:?}"),
        }
    }

    // ---- migrate -------------------------------------------------------
    //
    // The harness `migrate_schema` ports the server's `plan_migration` (schema
    // fold) + `apply_migration` (data effects). Structural directives update
    // the installed schema; data directives rewrite the in-memory doc map;
    // `evalExpr` is unsupported (no SQL engine).

    #[cfg(feature = "admin")]
    async fn migrate_schema_with_rows() -> InMemoryRtDbClient {
        // Schema: items { name: string, status: string, order: number }, two rows.
        // Inject an incrementing clock + constant RNG (the `new_client` pattern)
        // so the two server-minted ids differ — the default RNG/now collide.
        let counter = Arc::new(Mutex::new(1_700_000_000_000_i64));
        let mut c = InMemoryRtDbClient::new(
            InMemoryRtDbClientOptions::default()
                .now(move || {
                    let mut g = counter.lock().expect("counter not poisoned");
                    let v = *g;
                    *g += 1;
                    v
                })
                .random(|| 0.0),
        );
        c.push_schema(&test_schema()).unwrap();
        c.mutate(
            &Mutation::new()
                .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
                .insert("items", json!({"name": "b", "status": "done", "order": 2}))
                .build(),
            Some("m1"),
        )
        .await
        .unwrap();
        c
    }

    #[cfg(feature = "admin")]
    #[tokio::test]
    async fn migrate_rename_field_moves_doc_key() {
        let mut c = migrate_schema_with_rows().await;
        let directives = vec![crate::wire::admin::Directive::RenameField {
            table: "items".into(),
            from: "name".into(),
            to: "title".into(),
        }];
        let result = c.migrate_schema(&directives, false).unwrap();
        assert!(result.applied);
        assert_eq!(result.directives.len(), 1);
        assert_eq!(result.directives[0].op, "renameField");
        assert_eq!(result.directives[0].affected_rows, 2);
        // The folded schema carries the renamed field.
        assert!(result.schema.tables["items"].fields.contains_key("title"));
        assert!(!result.schema.tables["items"].fields.contains_key("name"));
        // And the stored docs were rewritten to match.
        let docs = c.collect_all("items");
        assert_eq!(docs.len(), 2);
        assert!(
            docs.iter()
                .all(|d| d.get("title").is_some() && d.get("name").is_none())
        );
    }

    #[cfg(feature = "admin")]
    #[tokio::test]
    async fn migrate_drop_table_clears_rows() {
        let mut c = migrate_schema_with_rows().await;
        let directives = vec![crate::wire::admin::Directive::DropTable {
            name: "items".into(),
        }];
        let result = c.migrate_schema(&directives, false).unwrap();
        assert_eq!(result.directives[0].op, "dropTable");
        assert_eq!(result.directives[0].affected_rows, 2);
        assert!(result.schema.tables.is_empty());
        assert!(c.collect_all("items").is_empty());
    }

    #[cfg(feature = "admin")]
    #[tokio::test]
    async fn migrate_change_type_without_default_rolls_back() {
        let mut c = migrate_schema_with_rows().await;
        // String -> Int64 via ToInt64. "1"/"2" parse; the server coerces per row.
        let directives = vec![crate::wire::admin::Directive::ChangeType {
            table: "items".into(),
            field: "name".into(),
            to: FieldType::Int64,
            cast: crate::wire::admin::Cast::ToInt64,
            default: None,
        }];
        // All rows have non-numeric `name` values → coercion fails with no default.
        let err = c.migrate_schema(&directives, false).unwrap_err();
        assert!(matches!(err.code, ErrorCode::BadRequest));
        // Rollback: schema and docs unchanged.
        let stored = c.to_schema_json().unwrap();
        assert_eq!(
            stored.tables["items"].fields.get("name"),
            Some(&FieldType::String)
        );
        assert_eq!(c.collect_all("items").len(), 2);
    }

    #[cfg(feature = "admin")]
    #[tokio::test]
    async fn migrate_set_default_populates_missing_field() {
        let mut c = migrate_schema_with_rows().await;
        let directives = vec![crate::wire::admin::Directive::SetDefault {
            table: "items".into(),
            field: "note".into(),
            value: json!("untagged"),
        }];
        let result = c.migrate_schema(&directives, false).unwrap();
        assert_eq!(result.directives[0].op, "setDefault");
        assert_eq!(result.directives[0].affected_rows, 2);
        let docs = c.collect_all("items");
        assert!(
            docs.iter()
                .all(|d| d.get("note") == Some(&json!("untagged")))
        );
    }

    #[cfg(feature = "admin")]
    #[tokio::test]
    async fn migrate_dry_run_leaves_state_unchanged() {
        let mut c = migrate_schema_with_rows().await;
        let directives = vec![crate::wire::admin::Directive::DropTable {
            name: "items".into(),
        }];
        let result = c.migrate_schema(&directives, true).unwrap();
        assert!(!result.applied);
        // Preview reports the dropped table, but nothing was committed.
        assert!(result.schema.tables.is_empty());
        assert!(c.to_schema_json().unwrap().tables.contains_key("items"));
        assert_eq!(c.collect_all("items").len(), 2);
    }

    #[cfg(feature = "admin")]
    #[tokio::test]
    async fn migrate_eval_expr_unsupported() {
        let mut c = migrate_schema_with_rows().await;
        let directives = vec![crate::wire::admin::Directive::EvalExpr {
            table: "items".into(),
            set: "upper".into(),
            expr: "upper(doc->>'name')".into(),
            where_clause: None,
        }];
        let err = c.migrate_schema(&directives, false).unwrap_err();
        assert!(matches!(err.code, ErrorCode::BadRequest));
        assert!(err.message.contains("evalExpr unsupported in-memory"));
    }

    #[cfg(feature = "admin")]
    #[tokio::test]
    async fn migrate_failed_directive_is_atomic() {
        let mut c = migrate_schema_with_rows().await;
        // renameField succeeds (folds into planned + docs), then DropTable on a
        // missing table fails. The earlier rename must roll back.
        let directives = vec![
            crate::wire::admin::Directive::RenameField {
                table: "items".into(),
                from: "name".into(),
                to: "title".into(),
            },
            crate::wire::admin::Directive::DropTable {
                name: "nope".into(),
            },
        ];
        let err = c.migrate_schema(&directives, false).unwrap_err();
        assert!(matches!(err.code, ErrorCode::BadRequest));
        // Schema untouched: `name` still present, `title` absent.
        let stored = c.to_schema_json().unwrap();
        assert!(stored.tables["items"].fields.contains_key("name"));
        assert!(!stored.tables["items"].fields.contains_key("title"));
        // Docs untouched: `name` key still present on every row.
        assert!(
            c.collect_all("items")
                .iter()
                .all(|d| d.get("name").is_some())
        );
    }

    #[cfg(feature = "admin")]
    #[tokio::test]
    async fn migrate_drop_field_affected_rows_counts_only_carriers() {
        // dropField reports `affected_rows` as only the rows whose `doc`
        // actually changed — rows that carried the field — not every row in the
        // table (server parity). Build a table where most rows LACK the
        // optional `note` field, drop it, and assert the count is the CARRIER
        // count.
        let mut c = migrate_schema_with_rows().await;
        // Third row that DOES carry the optional `note` field (the fixture's
        // two rows omit it).
        c.mutate(
            &Mutation::new()
                .insert(
                    "items",
                    json!({"name": "c", "status": "todo", "order": 3, "note": "tagged"}),
                )
                .build(),
            Some("m-note"),
        )
        .await
        .unwrap();
        let before = c.collect_all("items");
        assert_eq!(before.len(), 3);
        assert_eq!(
            before.iter().filter(|d| d.get("note").is_some()).count(),
            1,
            "precondition: exactly one row carries `note`"
        );

        let directives = vec![crate::wire::admin::Directive::DropField {
            table: "items".into(),
            field: "note".into(),
        }];
        let result = c.migrate_schema(&directives, false).unwrap();
        assert!(result.applied);
        assert_eq!(result.directives[0].op, "dropField");
        // Counts only the single row that carried the field, not all 3 rows.
        assert_eq!(result.directives[0].affected_rows, 1);

        // The field is nonetheless removed from the row that carried it, and
        // the derived schema no longer declares it.
        let after = c.collect_all("items");
        assert_eq!(after.len(), 3);
        assert!(after.iter().all(|d| d.get("note").is_none()));
        assert!(!result.schema.tables["items"].fields.contains_key("note"));
    }

    // ---- validate_doc --------------------------------------------------

    #[test]
    fn validate_doc_rejects_unknown_field() {
        let schema = test_schema();
        let bad = json!({"name": "a", "status": "todo", "order": 1, "bogus": 9});
        let err = validate_doc(items_table(&schema), &bad).unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);
        assert!(err.message.contains("bogus"), "got: {}", err.message);
    }

    #[test]
    fn validate_doc_rejects_reserved_field() {
        let schema = test_schema();
        let bad = json!({"name": "a", "status": "todo", "order": 1, "_id": "x"});
        let err = validate_doc(items_table(&schema), &bad).unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);
        assert!(err.message.contains("_id"), "got: {}", err.message);
    }

    #[test]
    fn validate_doc_rejects_wrong_field_type() {
        // The "invalid field type on a doc is rejected" case from the brief.
        let schema = test_schema();
        let bad = json!({"name": 42, "status": "todo", "order": 1});
        let err = validate_doc(items_table(&schema), &bad).unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);
        assert!(err.message.contains("name"), "got: {}", err.message);
    }

    #[test]
    fn validate_doc_rejects_missing_required_field() {
        let schema = test_schema();
        let bad = json!({"name": "a", "order": 1}); // missing required "status"
        let err = validate_doc(items_table(&schema), &bad).unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);
        assert!(err.message.contains("status"), "got: {}", err.message);
    }

    #[test]
    fn validate_doc_accepts_a_valid_doc_with_optional_absent() {
        let schema = test_schema();
        let good = json!({"name": "a", "status": "todo", "order": 1});
        validate_doc(items_table(&schema), &good).expect("valid doc");
    }

    #[test]
    fn validate_doc_accepts_an_optional_field_set_to_null() {
        // `note` is `Optional<String>`; null is accepted at the doc level
        // because Optional accepts null. `strip_unset_optionals` is what
        // converts it to "absent" for storage.
        let schema = test_schema();
        let good = json!({"name": "a", "status": "todo", "order": 1, "note": null});
        validate_doc(items_table(&schema), &good).expect("valid doc");
    }

    // ---- strip_unset_optionals ----------------------------------------

    #[test]
    fn strip_unset_optionals_drops_null_optional_string() {
        // `note: Optional<String>` set to null → key is stripped (the inner
        // String doesn't accept null, so this is "unset").
        let schema = test_schema();
        let doc = json!({"name": "a", "status": "todo", "order": 1, "note": null});
        let stripped = strip_unset_optionals(items_table(&schema), &doc);
        assert_eq!(stripped, json!({"name": "a", "status": "todo", "order": 1}));
    }

    #[test]
    fn strip_unset_optionals_keeps_null_for_optional_that_accepts_null() {
        // `Optional<Null>` does accept null as its inner value, so the key is
        // preserved.
        let schema = Schema::builder()
            .table(
                "t",
                Table::new().field("x", FieldType::optional(FieldType::Null)),
            )
            .build();
        let table = schema.tables.get("t").expect("table present");
        let doc = json!({"x": null});
        let stripped = strip_unset_optionals(table, &doc);
        assert_eq!(stripped, json!({"x": null}));
    }

    // ---- id/format helpers --------------------------------------------

    #[test]
    fn is_hex_id_checks_32_lowercase_hex_chars() {
        assert!(is_hex_id(&json!("0123456789abcdef0123456789abcdef")));
        assert!(!is_hex_id(&json!("0123456789ABCDEF0123456789ABCDEF"))); // uppercase
        assert!(!is_hex_id(&json!("0123456789abcdef"))); // too short
        assert!(!is_hex_id(&json!(42)));
        assert!(!is_hex_id(&json!(null)));
    }

    #[test]
    fn is_int64_string_accepts_i64_range_only() {
        assert!(is_int64_string(&json!("0")));
        assert!(is_int64_string(&json!("-1")));
        assert!(is_int64_string(&json!("9223372036854775807"))); // i64::MAX
        assert!(is_int64_string(&json!("-9223372036854775808"))); // i64::MIN
        // Out of i64 range:
        assert!(!is_int64_string(&json!("9223372036854775808")));
        assert!(!is_int64_string(&json!("-9223372036854775809")));
        // Bad shape:
        assert!(!is_int64_string(&json!("1.5")));
        assert!(!is_int64_string(&json!("-")));
        assert!(!is_int64_string(&json!("")));
        assert!(!is_int64_string(&json!(42)));
    }

    #[test]
    fn is_base64_string_matches_the_ts_regex() {
        assert!(is_base64_string(&json!("")));
        assert!(is_base64_string(&json!("ABCD")));
        assert!(is_base64_string(&json!("ABC=")));
        assert!(is_base64_string(&json!("AB==")));
        assert!(is_base64_string(&json!("YWJjZA=="))); // "abcd"
        // Length not a multiple of 4:
        assert!(!is_base64_string(&json!("ABC")));
        // Too much padding:
        assert!(!is_base64_string(&json!("A===")));
        // Bad body char:
        assert!(!is_base64_string(&json!("ABC!")));
        assert!(!is_base64_string(&json!(42)));
    }

    #[test]
    fn validate_value_handles_each_field_type_variant() {
        // A sanity sweep over the variants; full per-variant coverage lives in
        // the schema tests. Here we just confirm routing works.
        assert!(validate_value(&FieldType::String, &json!("hi")));
        assert!(!validate_value(&FieldType::String, &json!(2)));
        assert!(validate_value(&FieldType::Number, &json!(2.5)));
        assert!(validate_value(&FieldType::Boolean, &json!(true)));
        assert!(validate_value(&FieldType::Null, &json!(null)));
        assert!(validate_value(&FieldType::Any, &json!(null)));
        assert!(validate_value(
            &FieldType::Id { table: "x".into() },
            &json!("0123456789abcdef0123456789abcdef")
        ));
        assert!(validate_value(
            &FieldType::Literal { value: json!("a") },
            &json!("a")
        ));
        assert!(validate_value(
            &FieldType::Optional {
                inner: Box::new(FieldType::String)
            },
            &json!(null)
        ));
        assert!(validate_value(
            &FieldType::Union {
                variants: vec![FieldType::String, FieldType::Number]
            },
            &json!(2)
        ));
        assert!(validate_value(
            &FieldType::Array {
                element: Box::new(FieldType::Number)
            },
            &json!([1, 2, 3])
        ));
        assert!(validate_value(&FieldType::Int64, &json!("42")));
        assert!(validate_value(&FieldType::Bytes, &json!("YWJjZA==")));
        assert!(validate_value(
            &FieldType::Vector { dimensions: 3 },
            &json!([1.0, 2.0, 3.0])
        ));
    }

    #[test]
    fn canonical_is_key_order_independent() {
        // serde_json's default BTreeMap-backed Map serializes with sorted keys,
        // so canonical(a) == canonical(b) even when the source maps had
        // different insertion order.
        let a = json!({"b": 1, "a": 2});
        let b = json!({"a": 2, "b": 1});
        assert_eq!(canonical(&a), canonical(&b));
    }

    // ---- mutate: insert + read ---------------------------------------

    /// Deterministic clock + RNG so ids, `_creationTime`, and `_version` are
    /// stable. Mirrors TS `newClient` (`ts-client/tests/in_memory.test.ts:25-30`):
    /// post-incrementing epoch-millis clock + a constant `0` RNG.
    fn new_client() -> InMemoryRtDbClient {
        let counter = Arc::new(Mutex::new(1_700_000_000_000_i64));
        let mut client = InMemoryRtDbClient::new(
            InMemoryRtDbClientOptions::default()
                .now(move || {
                    let mut g = counter.lock().expect("counter not poisoned");
                    let v = *g;
                    *g += 1;
                    v
                })
                .random(|| 0.0),
        );
        client.push_schema(&test_schema()).unwrap();
        client
    }

    #[tokio::test]
    async fn insert_merges_system_fields_at_read_time() {
        let mut c = new_client();
        let txn = Mutation::new()
            .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
            .build();
        let results = c.mutate(&txn, None).await.expect("mutate ok");
        assert_eq!(results.len(), 1);
        let id = match &results[0] {
            StepResult::Insert { id } => id.clone(),
            other => panic!("expected Insert, got {other:?}"),
        };
        assert!(is_hex_id(&json!(id)), "id should be 32 hex chars: {id}");

        let doc = c.get("items", &id).expect("doc present");
        // System fields merged at read time:
        assert_eq!(doc["_id"], json!(id));
        assert_eq!(doc["_version"], 1);
        assert!(doc["_creationTime"].is_number(), "creationTime is a number");
        // User fields preserved:
        assert_eq!(doc["name"], "a");
        assert_eq!(doc["status"], "todo");
        assert_eq!(doc["order"], 1);
    }

    #[tokio::test]
    async fn insert_strips_optional_field_set_to_null() {
        // Mirrors TS "strips an optional field set to null on insert".
        let mut c = new_client();
        let txn = Mutation::new()
            .insert(
                "items",
                json!({"name": "a", "status": "todo", "order": 1, "note": null}),
            )
            .build();
        let results = c.mutate(&txn, None).await.expect("mutate ok");
        let id = match &results[0] {
            StepResult::Insert { id } => id.clone(),
            _ => unreachable!(),
        };
        let doc = c.get("items", &id).expect("doc present");
        // `note: null` was stripped on insert — the server's single representation
        // of an unset Optional<String> is "key absent", never "key present with null".
        assert!(
            doc.get("note").is_none(),
            "optional-null should be stripped, got: {doc}"
        );
    }

    #[tokio::test]
    async fn insert_rejects_missing_required_field() {
        // Mirrors TS "rejects an insert missing a required field".
        let mut c = new_client();
        let txn = Mutation::new()
            .insert("items", json!({"status": "todo", "order": 1})) // missing required "name"
            .build();
        let err = c.mutate(&txn, None).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);
        assert!(err.message.contains("name"), "got: {}", err.message);
    }

    // ---- mutate: upsert by index --------------------------------------

    #[tokio::test]
    async fn upsert_inserts_on_no_match_and_patches_on_match() {
        // Mirrors TS "inserts on no match (inserted: true) and patches on match".
        let mut c = new_client();
        let upsert = |patch_order: i64| {
            Mutation::new()
                .upsert(
                    "items",
                    "by_name",
                    &[json!("a")],
                    json!({"name": "a", "status": "todo", "order": 1}),
                    json!({"order": patch_order}),
                )
                .build()
        };

        let r1 = c.mutate(&upsert(2), None).await.expect("first upsert ok");
        let (id, inserted) = match &r1[0] {
            StepResult::Upsert { id, inserted } => (id.clone(), *inserted),
            other => panic!("expected Upsert, got {other:?}"),
        };
        assert!(inserted, "first upsert should insert");
        assert!(is_hex_id(&json!(id)));

        let r2 = c.mutate(&upsert(3), None).await.expect("second upsert ok");
        match &r2[0] {
            StepResult::Upsert {
                id: id2,
                inserted: false,
            } => {
                assert_eq!(id2, &id, "second upsert patched the same doc");
            }
            other => panic!("expected Upsert inserted=false, got {other:?}"),
        }

        let doc = c.get("items", &id).expect("doc present");
        assert_eq!(doc["order"], 3, "patch applied");
        assert_eq!(doc["_version"], 2, "patch bumped version");
    }

    #[tokio::test]
    async fn upsert_patch_visible_in_later_index_lookup() {
        // Mirrors TS "patches a matched doc onto an index field and reflects it
        // in a later query" — now via the real query DSL (Task 3), not the
        // internal `eq_lookup` helper. The patched `order` value is observable
        // through a `unique()` query on `by_name`.
        let mut c = new_client();
        let upsert = |patch_order: i64| {
            Mutation::new()
                .upsert(
                    "items",
                    "by_name",
                    &[json!("a")],
                    json!({"name": "a", "status": "todo", "order": 1}),
                    json!({"order": patch_order}),
                )
                .build()
        };
        c.mutate(&upsert(2), None).await.unwrap();
        let r2 = c.mutate(&upsert(3), None).await.unwrap();
        let id = match &r2[0] {
            StepResult::Upsert { id, .. } => id.clone(),
            _ => unreachable!(),
        };

        let matched: Value = c
            .run_query(
                &TableQuery::new("items")
                    .with_index("by_name", &[json!("a")])
                    .unique(),
            )
            .expect("unique query ok");
        assert_eq!(matched["_id"], json!(id), "matched the patched doc");
        assert_eq!(matched["order"], 3, "patch value visible through the DSL");
    }

    #[tokio::test]
    async fn upsert_rejects_multiple_matches() {
        // The brief calls out the multi-match rejection explicitly. Seed two
        // docs with the same indexed value, then upsert by that index.
        let mut c = new_client();
        c.mutate(
            &Mutation::new()
                .insert(
                    "items",
                    json!({"name": "dup", "status": "todo", "order": 1}),
                )
                .build(),
            None,
        )
        .await
        .unwrap();
        c.mutate(
            &Mutation::new()
                .insert(
                    "items",
                    json!({"name": "dup", "status": "todo", "order": 2}),
                )
                .build(),
            None,
        )
        .await
        .unwrap();

        let txn = Mutation::new()
            .upsert(
                "items",
                "by_name",
                &[json!("dup")],
                json!({"name": "dup", "status": "todo", "order": 1}),
                json!({"order": 9}),
            )
            .build();
        let err = c.mutate(&txn, None).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::PreconditionFailed);
        assert!(err.message.contains("multiple"), "got: {}", err.message);
    }

    // ---- mutate: transactions ----------------------------------------

    #[tokio::test]
    async fn txn_runs_multi_steps_and_returns_one_result_per_step() {
        // Mirrors TS "runs a multi-step txn and returns one result per step".
        let mut c = new_client();
        let txn = Mutation::new()
            .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
            .insert("items", json!({"name": "b", "status": "todo", "order": 2}))
            .build();
        let results = c.mutate(&txn, None).await.expect("mutate ok");
        assert_eq!(results.len(), 2, "one result per step");
        for r in &results {
            match r {
                StepResult::Insert { id } => assert!(is_hex_id(&json!(id.clone()))),
                other => panic!("expected Insert, got {other:?}"),
            }
        }
        let docs = c.collect_all("items");
        assert_eq!(docs.len(), 2, "both inserts landed");
    }

    #[tokio::test]
    async fn txn_patch_inside_txn_bumps_version() {
        // Mirrors TS "patches a doc inside a txn and bumps its version".
        let mut c = new_client();
        let r = c
            .mutate(
                &Mutation::new()
                    .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
                    .build(),
                None,
            )
            .await
            .unwrap();
        let id = match &r[0] {
            StepResult::Insert { id } => id.clone(),
            _ => unreachable!(),
        };

        // patch then expectVersion=2 (the patch bumps to 2 inside the same txn).
        let patch_txn = Mutation::new()
            .patch("items", &id, json!({"order": 9}))
            .expect_version("items", &id, 2)
            .build();
        c.mutate(&patch_txn, None).await.expect("patch txn ok");

        let doc = c.get("items", &id).expect("doc present");
        assert_eq!(doc["order"], 9);
        assert_eq!(doc["_version"], 2);
    }

    #[tokio::test]
    async fn txn_rolls_back_on_later_step_failure() {
        // Mirrors TS "rolls back the whole txn when a later step fails".
        let mut c = new_client();
        let r = c
            .mutate(
                &Mutation::new()
                    .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
                    .build(),
                None,
            )
            .await
            .unwrap();
        let id = match &r[0] {
            StepResult::Insert { id } => id.clone(),
            _ => unreachable!(),
        };

        let bad_txn = Mutation::new()
            .insert("items", json!({"name": "b", "status": "todo", "order": 2}))
            .expect_version("items", &id, 999) // mismatch → aborts the whole txn
            .build();
        let err = c.mutate(&bad_txn, None).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::PreconditionFailed);

        // Atomicity: the second insert was rolled back; only the original "a"
        // remains.
        let docs = c.collect_all("items");
        assert_eq!(docs.len(), 1, "rollback removed the second insert");
        assert_eq!(docs[0]["name"], "a");
    }

    #[tokio::test]
    async fn txn_rejects_more_than_max_steps() {
        // MAX_STEPS guard (mirror `executeTransaction` :546-548).
        let mut c = new_client();
        let mut m = Mutation::new();
        for _ in 0..(MAX_STEPS + 1) {
            m = m.insert("items", json!({"name": "x", "status": "todo", "order": 1}));
        }
        let txn = m.build();
        let err = c.mutate(&txn, None).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(err.message.contains("maximum"), "got: {}", err.message);
    }

    #[tokio::test]
    async fn txn_accepts_more_than_256_steps() {
        // ARC-104: the server raised MAX_STEPS 256 -> 1024; the in-memory engine
        // must accept a 300-step txn (previously over the stale 256 cap).
        let mut c = new_client();
        let mut m = Mutation::new();
        for i in 0..300 {
            m = m.insert(
                "items",
                json!({"name": format!("n{i}"), "status": "todo", "order": i}),
            );
        }
        let txn = m.build();
        let results = c.mutate(&txn, None).await.expect("300-step txn accepted");
        assert_eq!(results.len(), 300);
    }

    #[tokio::test]
    async fn mut_id_caches_results_and_short_circuits() {
        // Brief: port the TS `mutId` idempotency-key semantics (mutate :40-47).
        let mut c = new_client();
        let txn = Mutation::new()
            .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
            .build();

        let r1 = c.mutate(&txn, Some("m1")).await.expect("first ok");
        let r2 = c.mutate(&txn, Some("m1")).await.expect("cached ok");
        assert_eq!(r1.len(), 1);
        assert_eq!(r2.len(), 1);
        // The cached result is byte-identical to the first call — same id.
        let id1 = match &r1[0] {
            StepResult::Insert { id } => id.clone(),
            _ => unreachable!(),
        };
        let id2 = match &r2[0] {
            StepResult::Insert { id } => id.clone(),
            _ => unreachable!(),
        };
        assert_eq!(id1, id2, "cached mut_id returned the same id");
        // The cache short-circuits execution, so only one doc was actually
        // stored — the second `mutate` did not run the txn again.
        assert_eq!(c.collect_all("items").len(), 1);
    }

    // ---- mutate: patchByQuery / deleteByQuery -----------------------

    #[tokio::test]
    async fn patch_by_query_patches_every_match_and_reports_count() {
        let mut c = new_client();
        seed_query_rows(&mut c).await; // three "todo" rows (orders 3,1,2)
        let results = c
            .mutate(
                &Mutation::new()
                    .patch_by_query(
                        "items",
                        FilterExpr::Eq {
                            field: "status".into(),
                            value: json!("todo"),
                        },
                        json!({"status": "done"}),
                        None,
                    )
                    .build(),
                None,
            )
            .await
            .expect("patchByQuery ok");
        assert_eq!(results.len(), 1);
        match &results[0] {
            StepResult::PatchByQuery { patched, truncated } => {
                assert_eq!(*patched, 3);
                assert!(!*truncated);
            }
            other => panic!("expected PatchByQuery, got {other:?}"),
        }
        // Every matching row was patched; no "todo" remains.
        let docs = c.collect_all("items");
        assert_eq!(docs.len(), 3);
        assert!(docs.iter().all(|d| d["status"] == "done"));
    }

    #[tokio::test]
    async fn delete_by_query_removes_matches_and_reports_count() {
        let mut c = new_client();
        seed_query_rows(&mut c).await; // three "todo" rows
        let results = c
            .mutate(
                &Mutation::new()
                    .delete_by_query(
                        "items",
                        FilterExpr::Eq {
                            field: "status".into(),
                            value: json!("todo"),
                        },
                        None,
                    )
                    .build(),
                None,
            )
            .await
            .expect("deleteByQuery ok");
        assert_eq!(results.len(), 1);
        match &results[0] {
            StepResult::DeleteByQuery { deleted, truncated } => {
                assert_eq!(*deleted, 3);
                assert!(!*truncated);
            }
            other => panic!("expected DeleteByQuery, got {other:?}"),
        }
        assert!(c.collect_all("items").is_empty());
    }

    #[tokio::test]
    async fn patch_by_query_truncates_at_limit() {
        let mut c = new_client();
        seed_query_rows(&mut c).await; // three "todo" rows
        // limit below the match set: patches exactly `limit` and reports
        // truncated.
        let results = c
            .mutate(
                &Mutation::new()
                    .patch_by_query(
                        "items",
                        FilterExpr::Eq {
                            field: "status".into(),
                            value: json!("todo"),
                        },
                        json!({"status": "done"}),
                        Some(2),
                    )
                    .build(),
                None,
            )
            .await
            .expect("patchByQuery ok");
        match &results[0] {
            StepResult::PatchByQuery { patched, truncated } => {
                assert_eq!(*patched, 2);
                assert!(*truncated, "match set (3) exceeded limit (2)");
            }
            other => panic!("expected PatchByQuery, got {other:?}"),
        }
        // Two patched, one still "todo".
        let docs = c.collect_all("items");
        let done = docs.iter().filter(|d| d["status"] == "done").count();
        let todo = docs.iter().filter(|d| d["status"] == "todo").count();
        assert_eq!(done, 2);
        assert_eq!(todo, 1);
    }

    #[tokio::test]
    async fn patch_by_query_zero_matches_reports_zero_not_truncated() {
        let mut c = new_client();
        seed_query_rows(&mut c).await;
        let results = c
            .mutate(
                &Mutation::new()
                    .patch_by_query(
                        "items",
                        FilterExpr::Eq {
                            field: "status".into(),
                            value: json!("missing"),
                        },
                        json!({"status": "done"}),
                        None,
                    )
                    .build(),
                None,
            )
            .await
            .expect("patchByQuery ok");
        match &results[0] {
            StepResult::PatchByQuery { patched, truncated } => {
                assert_eq!(*patched, 0);
                assert!(!*truncated);
            }
            other => panic!("expected PatchByQuery, got {other:?}"),
        }
        // Nothing changed.
        assert_eq!(c.collect_all("items").len(), 3);
    }

    #[tokio::test]
    async fn sec104_rejects_over_budget_by_query_step_count() {
        // Mirrors server `sec104_rejects_over_budget_by_query_step_count`. A
        // txn with MAX_BY_QUERY_STEPS_PER_TXN+1 patchByQuery steps is rejected
        // at the top of execute_transaction, before any step applies. The
        // original AUDIT finding was 1024 by-query steps (~1M-row single-writer
        // stall); the 16-step cap rejects it pre-execution.
        let mut c = new_client();
        c.mutate(
            &Mutation::new()
                .insert(
                    "items",
                    json!({"name": "seed", "status": "todo", "order": 0}),
                )
                .build(),
            None,
        )
        .await
        .unwrap();
        let mut m = Mutation::new();
        for i in 0..=(MAX_BY_QUERY_STEPS_PER_TXN as i32) {
            m = m.patch_by_query(
                "items",
                FilterExpr::Eq {
                    field: "status".into(),
                    value: json!("todo"),
                },
                json!({"order": i}),
                None,
            );
        }
        let err = c.mutate(&m.build(), None).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(
            err.message.contains("by-query steps"),
            "got: {}",
            err.message
        );
        // Pre-execution rejection commits nothing.
        let docs = c.collect_all("items");
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["order"], 0);
    }

    #[tokio::test]
    async fn sec104_rejects_over_budget_aggregate_affected() {
        // Mirrors server `sec104_rejects_over_budget_aggregate_affected`. A
        // txn with few by-query steps (under the step cap) but each at the
        // default 1000-row limit can still exceed MAX_AFFECTED_ROWS_PER_TXN;
        // reject it before any step applies.
        let over_steps = (MAX_AFFECTED_ROWS_PER_TXN / 1000) + 1;
        assert!(over_steps <= MAX_BY_QUERY_STEPS_PER_TXN);
        let mut c = new_client();
        c.mutate(
            &Mutation::new()
                .insert(
                    "items",
                    json!({"name": "seed", "status": "todo", "order": 0}),
                )
                .build(),
            None,
        )
        .await
        .unwrap();
        let mut m = Mutation::new();
        for _ in 0..over_steps {
            m = m.delete_by_query(
                "items",
                FilterExpr::Eq {
                    field: "status".into(),
                    value: json!("todo"),
                },
                None,
            );
        }
        let err = c.mutate(&m.build(), None).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(err.message.contains("affect up to"), "got: {}", err.message);
        // Pre-execution rejection commits nothing.
        assert_eq!(c.collect_all("items").len(), 1);
    }

    // ---- mutate: step helpers ----------------------------------------

    #[test]
    fn apply_patch_merges_fields_and_re_validates_whole_doc() {
        let schema = test_schema();
        let table = items_table(&schema);
        let doc = json!({"name": "a", "status": "todo", "order": 1});
        let fields = json!({"order": 9}).as_object().unwrap().clone();
        let merged = apply_patch(table, &doc, &fields).expect("patch ok");
        assert_eq!(merged["order"], 9);
        assert_eq!(merged["name"], "a", "non-patched fields preserved");
    }

    #[test]
    fn apply_patch_null_on_optional_inner_that_rejects_null_deletes_key() {
        // `note: Optional<String>` + null → key is removed (mirrors
        // strip_unset_optionals' single-representation rule).
        let schema = test_schema();
        let table = items_table(&schema);
        let doc = json!({"name": "a", "status": "todo", "order": 1, "note": "hi"});
        let fields = json!({"note": null}).as_object().unwrap().clone();
        let merged = apply_patch(table, &doc, &fields).expect("patch ok");
        assert!(merged.get("note").is_none(), "note key stripped: {merged}");
    }

    #[test]
    fn apply_patch_rejects_unknown_field() {
        let schema = test_schema();
        let table = items_table(&schema);
        let doc = json!({"name": "a", "status": "todo", "order": 1});
        let fields = json!({"bogus": 1}).as_object().unwrap().clone();
        let err = apply_patch(table, &doc, &fields).unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);
        assert!(err.message.contains("bogus"));
    }

    #[test]
    fn index_column_type_maps_each_indexable_field_and_rejects_others() {
        // Indexable shapes:
        assert_eq!(
            index_column_type(&FieldType::String).unwrap().pg,
            PgType::Text
        );
        assert_eq!(
            index_column_type(&FieldType::Number).unwrap().pg,
            PgType::Number
        );
        assert_eq!(
            index_column_type(&FieldType::Boolean).unwrap().pg,
            PgType::Boolean
        );
        assert_eq!(
            index_column_type(&FieldType::Int64).unwrap().pg,
            PgType::Int64
        );
        assert_eq!(
            index_column_type(&FieldType::id("t")).unwrap().pg,
            PgType::Text
        );
        assert_eq!(
            index_column_type(&FieldType::literal("a")).unwrap().pg,
            PgType::Text
        );
        assert_eq!(
            index_column_type(&FieldType::optional(FieldType::Number))
                .unwrap()
                .pg,
            PgType::Number
        );
        // Optional wraps and reports nullable=true.
        let it = index_column_type(&FieldType::optional(FieldType::Number)).unwrap();
        assert!(it.nullable);
        // Non-indexable shapes:
        let err = index_column_type(&FieldType::Array {
            element: Box::new(FieldType::Number),
        })
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);
        let err = index_column_type(&FieldType::literal(7)).unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);
    }

    #[test]
    fn coerce_index_value_type_checks_against_index_column() {
        let schema = test_schema();
        let table = items_table(&schema);
        // `name` is String → text column. Number is rejected.
        coerce_index_value(table, "name", &json!("a")).expect("string ok");
        let err = coerce_index_value(table, "name", &json!(7)).unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        // `order` is Number → number column. String is rejected.
        coerce_index_value(table, "order", &json!(7)).expect("number ok");
        let err = coerce_index_value(table, "order", &json!("7")).unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        // Unknown field is INTERNAL (schema-declared index references a missing
        // field — a server-side programming error, not a client one).
        let err = coerce_index_value(table, "bogus", &json!(7)).unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
    }

    #[test]
    fn compare_index_values_orders_nulls_last_and_compares_each_domain() {
        use std::cmp::Ordering;
        // Numbers:
        assert_eq!(
            compare_index_values(&json!(1), &json!(2), PgType::Number),
            Ordering::Less
        );
        assert_eq!(
            compare_index_values(&json!(2), &json!(2), PgType::Number),
            Ordering::Equal
        );
        // Strings (lexicographic):
        assert_eq!(
            compare_index_values(&json!("a"), &json!("b"), PgType::Text),
            Ordering::Less
        );
        // Booleans (false < true):
        assert_eq!(
            compare_index_values(&json!(false), &json!(true), PgType::Boolean),
            Ordering::Less
        );
        // Int64 decimal strings compare numerically, not lexicographically:
        assert_eq!(
            compare_index_values(&json!("3"), &json!("20"), PgType::Int64),
            Ordering::Less
        );
        assert_eq!(
            compare_index_values(&json!("100"), &json!("20"), PgType::Int64),
            Ordering::Greater
        );
        assert_eq!(
            compare_index_values(&json!("-1"), &json!("0"), PgType::Int64),
            Ordering::Less
        );
        // Nulls sort last under asc — `null > anything`. The `pg` domain is
        // irrelevant once either side is null.
        assert_eq!(
            compare_index_values(&json!(null), &json!(1), PgType::Number),
            Ordering::Greater
        );
        assert_eq!(
            compare_index_values(&json!(1), &json!(null), PgType::Number),
            Ordering::Less
        );
        assert_eq!(
            compare_index_values(&json!(null), &json!(null), PgType::Number),
            Ordering::Equal
        );
    }

    #[test]
    fn merge_doc_layers_system_fields_over_user_doc() {
        let row = StoredRow {
            id: "0018beacc10070000000000000000000".to_string(),
            doc: json!({"name": "a", "status": "todo", "order": 1}),
            version: 7,
            created_at: 1_700_000_000_000,
        };
        let merged = merge_doc(&row);
        assert_eq!(merged["_id"], json!("0018beacc10070000000000000000000"));
        assert_eq!(merged["_version"], 7);
        assert_eq!(merged["_creationTime"], 1_700_000_000_000_i64);
        // User fields preserved.
        assert_eq!(merged["name"], "a");
        assert_eq!(merged["order"], 1);
    }

    // ---- query: get / collect ----------------------------------------

    /// Mirrors TS `seed` (`ts-client/tests/in_memory.test.ts:134-142`): insert
    /// three rows in `order` = 3, 1, 2 so an ascending sort differs from
    /// insertion order (catches a fall-back-to-insertion-order bug).
    async fn seed_query_rows(c: &mut InMemoryRtDbClient) {
        for order in [3_i64, 1, 2] {
            c.mutate(
                &Mutation::new()
                    .insert(
                        "items",
                        json!({"name": format!("n{order}"), "status": "todo", "order": order}),
                    )
                    .build(),
                None,
            )
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn query_collect_returns_empty_for_empty_table() {
        // Mirrors TS "collects [] from an empty table after pushSchema".
        let c = new_client();
        let docs = c
            .run::<Vec<Value>>(&TableQuery::new("items").collect())
            .expect("collect ok");
        assert!(docs.is_empty());
    }

    #[tokio::test]
    async fn query_get_returns_merged_doc() {
        // Mirrors TS "inserts a doc and merges system fields at read time"
        // (the read is now via the DSL `get` terminal, not the bare helper).
        let mut c = new_client();
        let r = c
            .mutate(
                &Mutation::new()
                    .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
                    .build(),
                None,
            )
            .await
            .expect("insert ok");
        let id = match &r[0] {
            StepResult::Insert { id } => id.clone(),
            other => panic!("expected Insert, got {other:?}"),
        };

        let doc = c
            .run::<Value>(&TableQuery::get("items", &id))
            .expect("get ok");
        assert_eq!(doc["_id"], json!(id));
        assert_eq!(doc["name"], "a");
        assert_eq!(doc["status"], "todo");
        assert_eq!(doc["order"], 1);
        assert_eq!(doc["_version"], 1);
        assert!(doc["_creationTime"].is_number());
    }

    #[tokio::test]
    async fn query_get_returns_null_for_missing_id() {
        // Mirrors TS "point-reads a missing id as null". The server returns
        // JSON null for a missing point read (TS :916), not an error.
        let c = new_client();
        let v = c
            .run::<Value>(&TableQuery::get(
                "items",
                "0123456789abcdef0123456789abcdef",
            ))
            .expect("get resolves");
        assert!(v.is_null(), "missing get returns Value::Null, got: {v}");
    }

    #[tokio::test]
    async fn query_get_rejects_combinations() {
        // Ports the `get`-exclusivity guard at TS :895-914. `get` plus any
        // narrowing clause is BAD_REQUEST.
        let c = new_client();
        let q = Query {
            table: "items".into(),
            get: Some("x".into()),
            index: Some("by_name".into()),
            ..Default::default()
        };
        let err = c.run_query(&q).unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(
            err.message.contains("get cannot be combined"),
            "got: {}",
            err.message
        );
    }

    // ---- query: index eq + order + take ------------------------------

    #[tokio::test]
    async fn query_eq_prefix_with_order_asc_sorts_by_remaining_field() {
        // Mirrors TS "filters by an eq index prefix and orders by the remaining
        // index field" — the asc branch.
        let mut c = new_client();
        seed_query_rows(&mut c).await;

        let asc = c
            .run::<Vec<Value>>(
                &TableQuery::new("items")
                    .with_index("by_status_and_order", &[json!("todo")])
                    .order(Order::Asc)
                    .collect(),
            )
            .expect("asc ok");
        let orders: Vec<i64> = asc
            .iter()
            .map(|d| d["order"].as_i64().unwrap_or_default())
            .collect();
        assert_eq!(orders, vec![1, 2, 3], "asc order");
    }

    #[tokio::test]
    async fn query_eq_prefix_with_order_desc_and_take_n() {
        // Mirrors TS "filters by an eq index prefix and orders by the remaining
        // index field" — the desc+take(2) branch.
        let mut c = new_client();
        seed_query_rows(&mut c).await;

        let desc = c
            .run::<Vec<Value>>(
                &TableQuery::new("items")
                    .with_index("by_status_and_order", &[json!("todo")])
                    .order(Order::Desc)
                    .take(2),
            )
            .expect("desc+take ok");
        let orders: Vec<i64> = desc
            .iter()
            .map(|d| d["order"].as_i64().unwrap_or_default())
            .collect();
        assert_eq!(orders, vec![3, 2], "desc order, take 2");
    }

    #[tokio::test]
    async fn query_eq_on_single_field_index_returns_matching_rows() {
        // The brief calls out single-field eq match explicitly; `by_name` is
        // single-field. Two rows share `name="dup"`, the third doesn't.
        let mut c = new_client();
        for order in [1_i64, 2, 3] {
            let name = if order <= 2 { "dup" } else { "uniq" };
            c.mutate(
                &Mutation::new()
                    .insert(
                        "items",
                        json!({"name": name, "status": "todo", "order": order}),
                    )
                    .build(),
                None,
            )
            .await
            .unwrap();
        }
        let docs = c
            .run::<Vec<Value>>(
                &TableQuery::new("items")
                    .with_index("by_name", &[json!("dup")])
                    .collect(),
            )
            .expect("eq ok");
        assert_eq!(docs.len(), 2, "both dup rows match");
        for d in &docs {
            assert_eq!(d["name"], "dup");
        }
    }

    // ---- query: range bounds ----------------------------------------

    #[tokio::test]
    async fn query_range_filters_by_index_field() {
        // gt / lt / gte / lte over the remaining index field. `by_status_and_order`
        // has `status` then `order`; the eq prefix pins status, the range
        // narrows order. Seed order values [3,1,2] and assert each bound.
        let mut c = new_client();
        seed_query_rows(&mut c).await;

        let collect_range =
            |gt: Option<i64>, gte: Option<i64>, lt: Option<i64>, lte: Option<i64>| {
                let mut q =
                    TableQuery::new("items").with_index("by_status_and_order", &[json!("todo")]);
                if let Some(v) = gt {
                    q = q.gt(v);
                }
                if let Some(v) = gte {
                    q = q.gte(v);
                }
                if let Some(v) = lt {
                    q = q.lt(v);
                }
                if let Some(v) = lte {
                    q = q.lte(v);
                }
                c.run::<Vec<Value>>(&q.order(Order::Asc).collect())
                    .expect("range ok")
            };

        let orders = |docs: Vec<Value>| -> Vec<i64> {
            docs.iter()
                .map(|d| d["order"].as_i64().unwrap_or_default())
                .collect()
        };

        // gt=1 → {2,3}; gte=2 → {2,3}; lt=3 → {1,2}; lte=2 → {1,2}.
        assert_eq!(orders(collect_range(Some(1), None, None, None)), vec![2, 3]);
        assert_eq!(orders(collect_range(None, Some(2), None, None)), vec![2, 3]);
        assert_eq!(orders(collect_range(None, None, Some(3), None)), vec![1, 2]);
        assert_eq!(orders(collect_range(None, None, None, Some(2))), vec![1, 2]);
    }

    // ---- query: int64 index (numeric ordering + range) ----------------

    /// Schema for int64-indexable coverage: a single `by_ts` index over an
    /// `Int64` field, plus a string payload to identify rows in assertions.
    fn int64_test_schema() -> SchemaDef {
        Schema::builder()
            .table(
                "events",
                Table::new()
                    .field("ts", FieldType::Int64)
                    .field("kind", FieldType::String)
                    .index("by_ts", &["ts"]),
            )
            .build()
    }

    /// Client seeded with [`int64_test_schema`] and a deterministic incrementing
    /// clock so each insert gets a distinct `_id` (the default constant-RNG id
    /// collides within a single millisecond, which would make successive inserts
    /// overwrite each other).
    fn int64_client() -> InMemoryRtDbClient {
        let counter = Arc::new(Mutex::new(1_700_000_000_000_i64));
        let mut client = InMemoryRtDbClient::new(
            InMemoryRtDbClientOptions::default()
                .now(move || {
                    let mut g = counter.lock().expect("counter not poisoned");
                    let v = *g;
                    *g += 1;
                    v
                })
                .random(|| 0.0),
        );
        client.push_schema(&int64_test_schema()).unwrap();
        client
    }

    #[tokio::test]
    async fn int64_index_orders_and_ranges_numerically() {
        // Int64 indexes store decimal strings, but the index order has to be
        // numeric (3 < 20 < 100), not lexicographic (100 < 20 < 3). Seeds the
        // rows out of numeric order to catch a lexicographic regression on
        // both the sort path and the range-bound path.
        let mut c = int64_client();
        for (ts, kind) in [("100", "a"), ("20", "b"), ("3", "c")] {
            c.mutate(
                &Mutation::new()
                    .insert("events", json!({ "ts": ts, "kind": kind }))
                    .build(),
                None,
            )
            .await
            .unwrap();
        }

        let kinds = |docs: Vec<Value>| -> Vec<String> {
            docs.iter()
                .map(|d| d["kind"].as_str().unwrap_or_default().to_string())
                .collect()
        };

        // Ascending numeric sort over the by_ts index → 3, 20, 100.
        let asc = c
            .run::<Vec<Value>>(
                &TableQuery::new("events")
                    .with_index("by_ts", &[])
                    .order(Order::Asc)
                    .collect(),
            )
            .expect("asc ok");
        assert_eq!(
            kinds(asc),
            vec!["c".to_string(), "b".to_string(), "a".to_string()],
            "int64 index should sort numerically (3, 20, 100)"
        );

        // Range on the int64 field: gte=20 keeps {20, 100}, asc → [b, a].
        let ranged = c
            .run::<Vec<Value>>(
                &TableQuery::new("events")
                    .with_index("by_ts", &[])
                    .gte(json!("20"))
                    .order(Order::Asc)
                    .collect(),
            )
            .expect("range ok");
        assert_eq!(
            kinds(ranged),
            vec!["b".to_string(), "a".to_string()],
            "int64 range bound should compare numerically (gte=20 keeps 20, 100)"
        );
    }

    // ---- query: terminals -------------------------------------------

    #[tokio::test]
    async fn query_count_returns_number_of_matching_rows() {
        // Mirrors TS "counts matching rows over an eq prefix".
        let mut c = new_client();
        seed_query_rows(&mut c).await;
        let n = c
            .run::<i64>(
                &TableQuery::new("items")
                    .with_index("by_status", &[json!("todo")])
                    .count(),
            )
            .expect("count ok");
        assert_eq!(n, 3);
    }

    #[tokio::test]
    async fn query_unique_returns_doc_when_exactly_one_match() {
        let mut c = new_client();
        c.mutate(
            &Mutation::new()
                .insert(
                    "items",
                    json!({"name": "only", "status": "todo", "order": 1}),
                )
                .build(),
            None,
        )
        .await
        .unwrap();
        let doc = c
            .run::<Value>(
                &TableQuery::new("items")
                    .with_index("by_name", &[json!("only")])
                    .unique(),
            )
            .expect("unique ok");
        assert_eq!(doc["name"], "only");
    }

    #[tokio::test]
    async fn query_unique_throws_precondition_failed_when_multiple_match() {
        // Mirrors TS "unique throws PRECONDITION_FAILED when more than one doc
        // matches".
        let mut c = new_client();
        for order in [1_i64, 2] {
            c.mutate(
                &Mutation::new()
                    .insert(
                        "items",
                        json!({"name": "dup", "status": "todo", "order": order}),
                    )
                    .build(),
                None,
            )
            .await
            .unwrap();
        }
        let err = c
            .run_query(
                &TableQuery::new("items")
                    .with_index("by_name", &[json!("dup")])
                    .unique(),
            )
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::PreconditionFailed);
    }

    #[tokio::test]
    async fn query_unique_returns_null_when_zero_match() {
        // TS :1143 — `unique` with zero matches returns null (no precondition
        // to fail; only a multi-match is an error).
        let c = new_client();
        let v = c
            .run::<Value>(
                &TableQuery::new("items")
                    .with_index("by_name", &[json!("ghost")])
                    .unique(),
            )
            .expect("unique resolves");
        assert!(v.is_null(), "zero-match unique returns null, got: {v}");
    }

    #[tokio::test]
    async fn query_first_returns_first_or_null() {
        // Mirrors TS `first` terminal: the first row of the filtered+sorted
        // set, or null when empty.
        let mut c = new_client();
        // Empty table: first = null.
        let v = c
            .run::<Value>(
                &TableQuery::new("items")
                    .with_index("by_status", &[json!("todo")])
                    .first(),
            )
            .expect("first on empty");
        assert!(v.is_null(), "first on empty table is null");

        seed_query_rows(&mut c).await;
        // With rows sorted ascending, first is order=1.
        let first = c
            .run::<Value>(
                &TableQuery::new("items")
                    .with_index("by_status_and_order", &[json!("todo")])
                    .order(Order::Asc)
                    .first(),
            )
            .expect("first ok");
        assert_eq!(first["order"], 1, "first asc is order=1");
    }

    #[tokio::test]
    async fn query_take_caps_results_at_n() {
        let mut c = new_client();
        seed_query_rows(&mut c).await;
        let docs = c
            .run::<Vec<Value>>(
                &TableQuery::new("items")
                    .with_index("by_status", &[json!("todo")])
                    .order(Order::Asc)
                    .take(2),
            )
            .expect("take ok");
        assert_eq!(docs.len(), 2, "take(2) on 3 rows caps at 2");
    }

    // ---- query: validation rejections -------------------------------

    #[tokio::test]
    async fn query_rejects_eq_without_index() {
        let c = new_client();
        let err = c
            .run_query(&Query {
                table: "items".into(),
                eq: vec![json!("x")],
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(err.message.contains("eq requires an index"), "got: {err}");
    }

    #[tokio::test]
    async fn query_rejects_range_without_index() {
        let c = new_client();
        let err = c
            .run_query(&Query {
                table: "items".into(),
                gt: Some(json!(1)),
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(
            err.message.contains("range bound requires an index"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn query_rejects_range_without_remaining_field_after_eq() {
        // `by_name` has one field — a full-arity eq leaves no field for a
        // range bound.
        let c = new_client();
        let err = c
            .run_query(&Query {
                table: "items".into(),
                index: Some("by_name".into()),
                eq: vec![json!("a")],
                gt: Some(json!("z")),
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(
            err.message.contains("remaining index field after eq"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn query_rejects_eq_arity_above_index_field_count() {
        // `by_name` is single-field; two eq values is over-arity.
        let c = new_client();
        let err = c
            .run_query(&Query {
                table: "items".into(),
                index: Some("by_name".into()),
                eq: vec![json!("a"), json!("b")],
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(err.message.contains("expects at most"), "got: {err}");
    }

    #[tokio::test]
    async fn query_rejects_gt_and_gte_together() {
        let c = new_client();
        let err = c
            .run_query(&Query {
                table: "items".into(),
                index: Some("by_status_and_order".into()),
                eq: vec![json!("todo")],
                gt: Some(json!(1)),
                gte: Some(json!(1)),
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(err.message.contains("gt and gte"), "got: {err}");
    }

    #[tokio::test]
    async fn query_rejects_lt_and_lte_together() {
        let c = new_client();
        let err = c
            .run_query(&Query {
                table: "items".into(),
                index: Some("by_status_and_order".into()),
                eq: vec![json!("todo")],
                lt: Some(json!(1)),
                lte: Some(json!(1)),
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(err.message.contains("lt and lte"), "got: {err}");
    }

    #[tokio::test]
    async fn query_rejects_take_over_max_take() {
        // MAX_TAKE guard (TS :963-965).
        let c = new_client();
        let err = c
            .run_query(&Query {
                table: "items".into(),
                take: Some((MAX_TAKE as u32) + 1),
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(err.message.contains("maximum"), "got: {err}");
    }

    #[tokio::test]
    async fn query_accepts_take_at_max_take() {
        // `take == MAX_TAKE` is the boundary — accepted.
        let c = new_client();
        let docs = c
            .run::<Vec<Value>>(&Query {
                table: "items".into(),
                take: Some(MAX_TAKE as u32),
                ..Default::default()
            })
            .expect("take=MAX_TAKE ok");
        assert!(docs.is_empty(), "empty table → empty page");
    }

    /// One assertion per conflicting-terminal guard at TS :919-939. Each case
    /// is BAD_REQUEST; the needle distinguishes which guard fired.
    #[tokio::test]
    async fn query_rejects_conflicting_terminals() {
        let c = new_client();
        let base_index_query =
            |unique: bool, first: bool, count: bool, order: bool, take: Option<u32>| Query {
                table: "items".into(),
                index: Some("by_status".into()),
                eq: vec![json!("todo")],
                unique,
                first,
                count,
                order: order.then_some(Order::Asc),
                take,
                ..Default::default()
            };

        let cases: &[(Query, &str)] = &[
            // unique + take
            (
                base_index_query(true, false, false, false, Some(1)),
                "unique cannot be combined with take",
            ),
            // unique + order
            (
                base_index_query(true, false, false, true, None),
                "unique cannot be combined with take, order",
            ),
            // first + unique
            (
                base_index_query(true, true, false, false, None),
                "first cannot be combined with unique",
            ),
            // first + take
            (
                base_index_query(false, true, false, false, Some(1)),
                "first cannot be combined with take",
            ),
            // count + unique
            (
                base_index_query(true, false, true, false, None),
                "count cannot be combined with unique",
            ),
            // count + take
            (
                base_index_query(false, false, true, false, Some(1)),
                "count cannot be combined with take",
            ),
            // count + first
            (
                base_index_query(false, true, true, false, None),
                "count cannot be combined with first",
            ),
            // count + order
            (
                base_index_query(false, false, true, true, None),
                "count cannot be combined with order",
            ),
        ];
        for (q, needle) in cases {
            let err = c.run_query(q).unwrap_err();
            assert_eq!(
                err.code,
                ErrorCode::BadRequest,
                "case '{needle}': got {err:?}"
            );
            assert!(
                err.message.contains(needle),
                "case '{needle}' missing needle: got {}",
                err.message
            );
        }
    }

    // ---- query: distinct + aggregate terminals ---------------------
    //
    // Ports distinct/aggregate coverage from `ts-client/src/in_memory.ts`
    // (`executeQuery` :1355-1462) and the server's `execute_query` arms. Both
    // are standalone terminals over the index field immediately after the eq
    // prefix; they compose only with index/eq/range/filter.

    /// Seeds `items` with duplicated `order` values {3,1,2,1,2} (all "todo") so
    /// distinct dedupe and asc sort are both observable.
    async fn seed_dup_orders(c: &mut InMemoryRtDbClient) {
        for order in [3_i64, 1, 2, 1, 2] {
            c.mutate(
                &Mutation::new()
                    .insert(
                        "items",
                        json!({"name": format!("n{order}"), "status": "todo", "order": order}),
                    )
                    .build(),
                None,
            )
            .await
            .unwrap();
        }
    }

    /// Seeds `items` with two statuses so a `groupBy` over
    /// `by_status_and_order` has multiple groups: todo {1,2}, done {3,4}.
    async fn seed_group_rows(c: &mut InMemoryRtDbClient) {
        for (status, order) in [("todo", 1_i64), ("todo", 2), ("done", 3), ("done", 4)] {
            c.mutate(
                &Mutation::new()
                    .insert(
                        "items",
                        json!({"name": "n", "status": status, "order": order}),
                    )
                    .build(),
                None,
            )
            .await
            .unwrap();
        }
    }

    /// Seeds `items` with `status` values {charlie, alpha, bravo} so non-numeric
    /// MIN/MAX pick lexicographic extremes.
    async fn seed_status_rows(c: &mut InMemoryRtDbClient) {
        for (i, status) in ["charlie", "alpha", "bravo"].iter().enumerate() {
            c.mutate(
                &Mutation::new()
                    .insert(
                        "items",
                        json!({"name": "n", "status": status, "order": i as i64}),
                    )
                    .build(),
                None,
            )
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn distinct_returns_unique_index_field_values_sorted_asc() {
        let mut c = new_client();
        seed_query_rows(&mut c).await; // orders 3, 1, 2 — all "todo"
        let v = c
            .run_query(
                &TableQuery::new("items")
                    .with_index("by_status_and_order", &[json!("todo")])
                    .distinct(),
            )
            .expect("distinct ok");
        assert_eq!(v, json!([1, 2, 3]));
    }

    #[tokio::test]
    async fn distinct_dedupes_repeated_values() {
        let mut c = new_client();
        seed_dup_orders(&mut c).await; // orders 3,1,2,1,2
        let v = c
            .run_query(
                &TableQuery::new("items")
                    .with_index("by_status_and_order", &[json!("todo")])
                    .distinct(),
            )
            .expect("distinct ok");
        assert_eq!(v, json!([1, 2, 3]));
    }

    #[tokio::test]
    async fn distinct_composes_with_range_bound() {
        let mut c = new_client();
        seed_query_rows(&mut c).await; // orders 3, 1, 2
        let v = c
            .run_query(
                &TableQuery::new("items")
                    .with_index("by_status_and_order", &[json!("todo")])
                    .gte(2)
                    .distinct(),
            )
            .expect("distinct+range ok");
        assert_eq!(v, json!([2, 3]));
    }

    #[tokio::test]
    async fn distinct_empty_matching_set_returns_empty_array() {
        let mut c = new_client();
        seed_query_rows(&mut c).await;
        let v = c
            .run_query(
                &TableQuery::new("items")
                    .with_index("by_status_and_order", &[json!("missing")])
                    .distinct(),
            )
            .expect("distinct ok");
        assert_eq!(v, json!([]));
    }

    #[tokio::test]
    async fn distinct_requires_an_index_field_beyond_eq_prefix() {
        let c = new_client();
        // eq prefix [todo, 1] consumes both index fields of by_status_and_order.
        let err = c
            .run_query(
                &TableQuery::new("items")
                    .with_index("by_status_and_order", &[json!("todo"), json!(1)])
                    .distinct(),
            )
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(
            err.message
                .contains("distinct requires an index field beyond the eq prefix"),
            "got: {}",
            err.message
        );
    }

    #[tokio::test]
    async fn distinct_rejects_conflicting_terminals() {
        // Ownership mirrors the server's check order (query.rs :676-706): get,
        // unique, first, count are validated before distinct, so distinct+
        // {get,unique,first,count} surfaces *that* terminal's message; distinct
        // owns only take/order/aggregate.
        let c = new_client();
        let base = || Query {
            table: "items".into(),
            index: Some("by_status_and_order".into()),
            eq: vec![json!("todo")],
            ..Default::default()
        };
        let cases: &[(Query, &str)] = &[
            (
                Query {
                    distinct: true,
                    take: Some(1),
                    ..base()
                },
                "distinct cannot be combined with take",
            ),
            (
                Query {
                    distinct: true,
                    order: Some(Order::Asc),
                    ..base()
                },
                "distinct cannot be combined with order",
            ),
            (
                Query {
                    distinct: true,
                    aggregate: Some(AggregateSpec {
                        op: AggregateOp::Sum,
                        group_by: false,
                    }),
                    ..base()
                },
                "distinct cannot be combined with aggregate",
            ),
            (
                Query {
                    distinct: true,
                    unique: true,
                    ..base()
                },
                "unique cannot be combined with take, order, distinct, or aggregate",
            ),
            (
                Query {
                    distinct: true,
                    first: true,
                    ..base()
                },
                "first cannot be combined with distinct",
            ),
            (
                Query {
                    distinct: true,
                    count: true,
                    ..base()
                },
                "count cannot be combined with distinct",
            ),
            (
                Query {
                    distinct: true,
                    get: Some("x".into()),
                    ..base()
                },
                "get cannot be combined with",
            ),
        ];
        for (q, needle) in cases {
            let err = c.run_query(q).unwrap_err();
            assert_eq!(err.code, ErrorCode::BadRequest, "case '{needle}': {err:?}");
            assert!(
                err.message.contains(needle),
                "case '{needle}': got {}",
                err.message
            );
        }
    }

    #[tokio::test]
    async fn aggregate_sum_avg_min_max_over_numeric_field() {
        let mut c = new_client();
        seed_query_rows(&mut c).await; // orders 3, 1, 2

        let sum = c
            .run_query(
                &TableQuery::new("items")
                    .with_index("by_status_and_order", &[json!("todo")])
                    .aggregate(AggregateOp::Sum, false),
            )
            .expect("sum");
        assert_eq!(sum.as_f64(), Some(6.0));
        let avg = c
            .run_query(
                &TableQuery::new("items")
                    .with_index("by_status_and_order", &[json!("todo")])
                    .aggregate(AggregateOp::Avg, false),
            )
            .expect("avg");
        assert_eq!(avg.as_f64(), Some(2.0));
        let min = c
            .run_query(
                &TableQuery::new("items")
                    .with_index("by_status_and_order", &[json!("todo")])
                    .aggregate(AggregateOp::Min, false),
            )
            .expect("min");
        assert_eq!(min.as_f64(), Some(1.0));
        let max = c
            .run_query(
                &TableQuery::new("items")
                    .with_index("by_status_and_order", &[json!("todo")])
                    .aggregate(AggregateOp::Max, false),
            )
            .expect("max");
        assert_eq!(max.as_f64(), Some(3.0));
    }

    #[tokio::test]
    async fn aggregate_empty_matching_set_returns_null() {
        let mut c = new_client();
        seed_query_rows(&mut c).await;
        let v = c
            .run_query(
                &TableQuery::new("items")
                    .with_index("by_status_and_order", &[json!("missing")])
                    .aggregate(AggregateOp::Sum, false),
            )
            .expect("aggregate ok");
        assert!(v.is_null(), "empty aggregate is null, got: {v}");
    }

    #[tokio::test]
    async fn aggregate_sum_requires_a_numeric_field() {
        let mut c = new_client();
        seed_status_rows(&mut c).await;
        // by_status [status], empty eq → agg field is `status` (a string).
        let err = c
            .run_query(
                &TableQuery::new("items")
                    .with_index("by_status", &[])
                    .aggregate(AggregateOp::Sum, false),
            )
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(
            err.message
                .contains("aggregate op sum requires a numeric index field"),
            "got: {}",
            err.message
        );
    }

    #[tokio::test]
    async fn aggregate_min_max_over_string_field_are_lexicographic() {
        let mut c = new_client();
        seed_status_rows(&mut c).await; // statuses charlie, alpha, bravo
        let min = c
            .run_query(
                &TableQuery::new("items")
                    .with_index("by_status", &[])
                    .aggregate(AggregateOp::Min, false),
            )
            .expect("min");
        assert_eq!(min.as_str(), Some("alpha"));
        let max = c
            .run_query(
                &TableQuery::new("items")
                    .with_index("by_status", &[])
                    .aggregate(AggregateOp::Max, false),
            )
            .expect("max");
        assert_eq!(max.as_str(), Some("charlie"));
    }

    #[tokio::test]
    async fn aggregate_group_by_groups_and_aggregates() {
        let mut c = new_client();
        seed_group_rows(&mut c).await; // todo{1,2}, done{3,4}
        let v = c
            .run_query(
                &TableQuery::new("items")
                    .with_index("by_status_and_order", &[])
                    .aggregate(AggregateOp::Sum, true),
            )
            .expect("groupBy ok");
        let arr = v.as_array().expect("array of {key,value}");
        assert_eq!(arr.len(), 2);
        // Groups are ordered by key ascending: "done" < "todo".
        assert_eq!(arr[0]["key"].as_str(), Some("done"));
        assert_eq!(arr[0]["value"].as_f64(), Some(7.0));
        assert_eq!(arr[1]["key"].as_str(), Some("todo"));
        assert_eq!(arr[1]["value"].as_f64(), Some(3.0));
    }

    #[tokio::test]
    async fn aggregate_count_scalar_returns_matching_row_count() {
        let mut c = new_client();
        seed_query_rows(&mut c).await; // three "todo" rows
        let v = c
            .run_query(
                &TableQuery::new("items")
                    .with_index("by_status_and_order", &[json!("todo")])
                    .aggregate(AggregateOp::Count, false),
            )
            .expect("count ok");
        assert_eq!(v.as_i64(), Some(3));
    }

    #[tokio::test]
    async fn aggregate_count_scalar_empty_matching_set_is_zero() {
        let mut c = new_client();
        seed_query_rows(&mut c).await;
        // count over zero rows is 0 (never null, unlike sum/avg/min/max).
        let v = c
            .run_query(
                &TableQuery::new("items")
                    .with_index("by_status_and_order", &[json!("missing")])
                    .aggregate(AggregateOp::Count, false),
            )
            .expect("count ok");
        assert_eq!(v.as_i64(), Some(0));
    }

    #[tokio::test]
    async fn aggregate_count_grouped_returns_group_sizes() {
        let mut c = new_client();
        seed_group_rows(&mut c).await; // todo{1,2}, done{3,4}
        let v = c
            .run_query(
                &TableQuery::new("items")
                    .with_index("by_status_and_order", &[])
                    .aggregate(AggregateOp::Count, true),
            )
            .expect("groupBy count ok");
        let arr = v.as_array().expect("array of {key,value}");
        assert_eq!(arr.len(), 2);
        // Ordered by key ascending: "done" < "todo".
        assert_eq!(arr[0]["key"].as_str(), Some("done"));
        assert_eq!(arr[0]["value"].as_i64(), Some(2));
        assert_eq!(arr[1]["key"].as_str(), Some("todo"));
        assert_eq!(arr[1]["value"].as_i64(), Some(2));
    }

    #[tokio::test]
    async fn aggregate_count_consumes_no_aggregate_field() {
        // count needs no field beyond the eq prefix: by_status [status] with an
        // empty eq prefix would error for sum/avg ("requires an index field
        // beyond the eq prefix") but succeeds for count.
        let mut c = new_client();
        seed_status_rows(&mut c).await; // three rows
        let v = c
            .run_query(
                &TableQuery::new("items")
                    .with_index("by_status", &[])
                    .aggregate(AggregateOp::Count, false),
            )
            .expect("count needs no agg field");
        assert_eq!(v.as_i64(), Some(3));
    }

    #[tokio::test]
    async fn aggregate_group_by_requires_two_index_fields_beyond_prefix() {
        let c = new_client();
        // by_status [status], empty eq → only one field beyond the prefix.
        let err = c
            .run_query(
                &TableQuery::new("items")
                    .with_index("by_status", &[])
                    .aggregate(AggregateOp::Sum, true),
            )
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(
            err.message
                .contains("aggregate groupBy requires two index fields beyond the eq prefix"),
            "got: {}",
            err.message
        );
    }

    #[tokio::test]
    async fn aggregate_requires_an_index_field_beyond_eq_prefix() {
        let c = new_client();
        // eq prefix [todo, 1] consumes both fields of by_status_and_order.
        let err = c
            .run_query(
                &TableQuery::new("items")
                    .with_index("by_status_and_order", &[json!("todo"), json!(1)])
                    .aggregate(AggregateOp::Min, false),
            )
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(
            err.message
                .contains("aggregate requires an index field beyond the eq prefix"),
            "got: {}",
            err.message
        );
    }

    #[tokio::test]
    async fn aggregate_rejects_conflicting_terminals() {
        let c = new_client();
        let base = || Query {
            table: "items".into(),
            index: Some("by_status_and_order".into()),
            eq: vec![json!("todo")],
            ..Default::default()
        };
        let sum = || AggregateSpec {
            op: AggregateOp::Sum,
            group_by: false,
        };
        let cases: &[(Query, &str)] = &[
            (
                Query {
                    aggregate: Some(sum()),
                    take: Some(1),
                    ..base()
                },
                "aggregate cannot be combined with take",
            ),
            (
                Query {
                    aggregate: Some(sum()),
                    order: Some(Order::Asc),
                    ..base()
                },
                "aggregate cannot be combined with order",
            ),
            (
                Query {
                    aggregate: Some(sum()),
                    unique: true,
                    ..base()
                },
                "unique cannot be combined with take, order, distinct, or aggregate",
            ),
            (
                Query {
                    aggregate: Some(sum()),
                    first: true,
                    ..base()
                },
                "first cannot be combined with aggregate",
            ),
            (
                Query {
                    aggregate: Some(sum()),
                    count: true,
                    ..base()
                },
                "count cannot be combined with aggregate",
            ),
            (
                Query {
                    aggregate: Some(sum()),
                    distinct: true,
                    ..base()
                },
                "distinct cannot be combined with aggregate",
            ),
            (
                Query {
                    aggregate: Some(sum()),
                    get: Some("x".into()),
                    ..base()
                },
                "get cannot be combined with",
            ),
        ];
        for (q, needle) in cases {
            let err = c.run_query(q).unwrap_err();
            assert_eq!(err.code, ErrorCode::BadRequest, "case '{needle}': {err:?}");
            assert!(
                err.message.contains(needle),
                "case '{needle}': got {}",
                err.message
            );
        }
    }

    // ---- query: paginate (cursor keyset) -----------------------------
    //
    // Direct port of `describe("InMemoryRtDbClient — paginate (cursor keyset)")`
    // (`ts-client/tests/in_memory.test.ts:250-431`). The deterministic clock +
    // RNG make `_creationTime` and `_id` rise with insertion order, so an
    // ascending sort yields insertion order and a descending sort reverses it.

    /// Mirrors TS `seedItems` (`ts-client/tests/in_memory.test.ts:254-269`):
    /// insert `count` items with `order` = 1..count and `status` cycling
    /// through `statuses`. Returns the inserted ids in insertion order.
    async fn seed_items(c: &mut InMemoryRtDbClient, count: i64, statuses: &[&str]) -> Vec<String> {
        let mut ids = Vec::new();
        for i in 1..=count {
            let txn = Mutation::new()
                .insert(
                    "items",
                    json!({
                        "name": format!("n{i}"),
                        "status": statuses[((i - 1) as usize) % statuses.len()],
                        "order": i,
                    }),
                )
                .build();
            let results = c.mutate(&txn, None).await.expect("insert ok");
            match &results[0] {
                StepResult::Insert { id } => ids.push(id.clone()),
                other => panic!("expected Insert, got {other:?}"),
            }
        }
        ids
    }

    /// Walks the full cursor chain until `next_cursor` is absent — ports TS
    /// `walkPages` (`ts-client/tests/in_memory.test.ts:272-295`). Returns the
    /// observed page sizes, the per-page cursors (final one `None`), and all
    /// docs concatenated in page order.
    async fn walk_pages<F>(
        c: &InMemoryRtDbClient,
        build: F,
    ) -> (Vec<usize>, Vec<Option<String>>, Vec<Value>)
    where
        F: Fn(Option<&str>) -> Query,
    {
        let mut page_sizes = Vec::new();
        let mut cursors = Vec::new();
        let mut docs = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..1000 {
            let page: Paginated<Value> = c.run(&build(cursor.as_deref())).expect("paginate ok");
            page_sizes.push(page.docs.len());
            cursors.push(page.next_cursor.clone());
            docs.extend(page.docs);
            if page.next_cursor.is_none() {
                return (page_sizes, cursors, docs);
            }
            cursor = page.next_cursor;
        }
        panic!("pagination did not terminate");
    }

    #[tokio::test]
    async fn paginate_returns_empty_page_with_no_cursor_on_empty_table() {
        // Ports TS "returns an empty page with no nextCursor on an empty table".
        let c = new_client();
        let page: Paginated<Value> = c
            .run(&TableQuery::new("items").paginate(None, 3))
            .expect("paginate ok");
        assert!(page.docs.is_empty());
        assert!(page.next_cursor.is_none());
    }

    #[tokio::test]
    async fn paginate_walks_all_pages_terminating_on_short_last_page() {
        // Ports TS "walks all pages in order, terminating on a short last page".
        let mut c = new_client();
        seed_items(&mut c, 7, &["todo"]).await;
        let (page_sizes, cursors, docs) =
            walk_pages(&c, |cursor| TableQuery::new("items").paginate(cursor, 3)).await;
        // Page sizes 3, 3, 1; the walk must equal a plain collect() with no
        // skips or duplicates.
        assert_eq!(page_sizes, vec![3, 3, 1]);
        assert!(cursors[..cursors.len() - 1].iter().all(|x| x.is_some()));
        assert!(cursors.last().is_some_and(|x| x.is_none()));

        let collected: Vec<Value> = c
            .run::<Vec<Value>>(&TableQuery::new("items").collect())
            .expect("collect ok");
        let walked_ids: Vec<&str> = docs
            .iter()
            .map(|d| d["_id"].as_str().expect("id string"))
            .collect();
        let collected_ids: Vec<&str> = collected
            .iter()
            .map(|d| d["_id"].as_str().expect("id string"))
            .collect();
        assert_eq!(walked_ids, collected_ids);
        let mut unique = walked_ids.to_vec();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), walked_ids.len(), "no duplicates across pages");
    }

    #[tokio::test]
    async fn paginate_terminates_on_full_last_page_when_count_is_exact_multiple() {
        // Ports TS "terminates on a full last page when the count is an exact
        // multiple": the final page is full but `nextCursor` is None.
        let mut c = new_client();
        seed_items(&mut c, 6, &["todo"]).await;
        let (page_sizes, cursors, _docs) =
            walk_pages(&c, |cursor| TableQuery::new("items").paginate(cursor, 3)).await;
        assert_eq!(page_sizes, vec![3, 3]);
        assert!(cursors[0].is_some());
        assert!(cursors[1].is_none());
    }

    #[tokio::test]
    async fn paginate_within_eq_prefixed_index_in_index_order() {
        // Ports TS "paginates within an eq-prefixed multi-field index in index
        // order": status cycles todo/done/todo ⇒ todos are orders 1,3,4,6,7,9.
        let mut c = new_client();
        seed_items(&mut c, 9, &["todo", "done", "todo"]).await;
        let (page_sizes, _cursors, docs) = walk_pages(&c, |cursor| {
            TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .paginate(cursor, 4)
        })
        .await;
        assert_eq!(page_sizes, vec![4, 2]);
        let orders: Vec<i64> = docs
            .iter()
            .map(|d| d["order"].as_i64().expect("order number"))
            .collect();
        assert_eq!(orders, vec![1, 3, 4, 6, 7, 9]);
        assert!(docs.iter().all(|d| d["status"] == json!("todo")));
    }

    #[tokio::test]
    async fn paginate_descending_pages_in_reverse_index_order() {
        // Ports TS "walks descending pages in reverse index order": same seed
        // as the asc case, but order=desc ⇒ 9,7,6,4,3,1.
        let mut c = new_client();
        seed_items(&mut c, 9, &["todo", "done", "todo"]).await;
        let (page_sizes, _cursors, docs) = walk_pages(&c, |cursor| {
            TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .order(Order::Desc)
                .paginate(cursor, 4)
        })
        .await;
        assert_eq!(page_sizes, vec![4, 2]);
        let orders: Vec<i64> = docs
            .iter()
            .map(|d| d["order"].as_i64().expect("order number"))
            .collect();
        assert_eq!(orders, vec![9, 7, 6, 4, 3, 1]);
    }

    #[tokio::test]
    async fn paginate_cursor_round_trips_and_resumes_chain() {
        // Ports TS "emits cursors decodable by the live client; resume
        // continues the chain": the cursor decodes to the last row's
        // [order, _creationTime, _id] tuple — cursors are interchangeable.
        let mut c = new_client();
        seed_items(&mut c, 5, &["todo"]).await; // todo orders 1..5
        let first: Paginated<Value> = c
            .run(
                &TableQuery::new("items")
                    .with_index("by_status_and_order", &[json!("todo")])
                    .paginate(None, 2),
            )
            .expect("first page");
        let orders: Vec<i64> = first
            .docs
            .iter()
            .map(|d| d["order"].as_i64().expect("order number"))
            .collect();
        assert_eq!(orders, vec![1, 2]);
        let next_cursor = first.next_cursor.expect("expected a nextCursor");

        // Cursor decodes to [order, _creationTime, _id] of the page's last row.
        let decoded = crate::cursor::decode_cursor(&next_cursor).expect("cursor decodes");
        let last = &first.docs[1];
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0], last["order"]);
        assert_eq!(decoded[1], last["_creationTime"]);
        assert_eq!(decoded[2], last["_id"]);

        let second: Paginated<Value> = c
            .run(
                &TableQuery::new("items")
                    .with_index("by_status_and_order", &[json!("todo")])
                    .paginate(Some(&next_cursor), 2),
            )
            .expect("second page");
        let orders: Vec<i64> = second
            .docs
            .iter()
            .map(|d| d["order"].as_i64().expect("order number"))
            .collect();
        assert_eq!(orders, vec![3, 4]);
    }

    #[tokio::test]
    async fn paginate_rejects_malformed_cursor_as_bad_request() {
        // Ports TS "rejects a malformed (non-base64) cursor with BAD_REQUEST,
        // not INTERNAL" — the codec returns INTERNAL; the harness rewraps it.
        let mut c = new_client();
        seed_items(&mut c, 3, &["todo"]).await;
        let err = c
            .run_query(&Query {
                table: "items".into(),
                paginate: Some(Paginate {
                    cursor: Some("not-valid-base64!!!".into()),
                    num_items: 3,
                }),
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
    }

    #[tokio::test]
    async fn paginate_rejects_cursor_with_mismatched_arity() {
        // Ports TS "rejects a cursor whose arity mismatches the sort columns":
        // no-index query sorts over 2 columns (createdAt, id); 3 values
        // mismatch.
        let mut c = new_client();
        seed_items(&mut c, 3, &["todo"]).await;
        let bad = crate::cursor::encode_cursor(&[json!(1), json!(2), json!(3)]).expect("encode");
        let err = c
            .run_query(&Query {
                table: "items".into(),
                paginate: Some(Paginate {
                    cursor: Some(bad),
                    num_items: 3,
                }),
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(err.message.contains("sorts over 2 column(s)"), "got: {err}");
    }

    #[tokio::test]
    async fn paginate_rejects_cursor_whose_created_at_is_not_a_number() {
        // Ports TS "rejects a cursor whose created_at value is not a number":
        // no-index cursor = [createdAt, id]; a non-numeric createdAt fails
        // type-check.
        let mut c = new_client();
        seed_items(&mut c, 3, &["todo"]).await;
        let bad = crate::cursor::encode_cursor(&[
            json!("not-a-number"),
            json!("0123456789abcdef0123456789abcdef"),
        ])
        .expect("encode");
        let err = c
            .run_query(&Query {
                table: "items".into(),
                paginate: Some(Paginate {
                    cursor: Some(bad),
                    num_items: 3,
                }),
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(
            err.message.contains("created_at must be a number"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn paginate_rejects_combination_with_take_count_unique_or_first() {
        // Ports TS "rejects paginate combined with take or count" and extends
        // to unique/first — the validation cascade Task 3 collapsed is now
        // restored (TS :940-955).
        let mut c = new_client();
        seed_items(&mut c, 3, &["todo"]).await;
        for (needle, q) in [
            (
                "take",
                Query {
                    table: "items".into(),
                    paginate: Some(Paginate {
                        cursor: None,
                        num_items: 3,
                    }),
                    take: Some(3),
                    ..Default::default()
                },
            ),
            (
                "count",
                Query {
                    table: "items".into(),
                    paginate: Some(Paginate {
                        cursor: None,
                        num_items: 3,
                    }),
                    count: true,
                    ..Default::default()
                },
            ),
            (
                "unique",
                Query {
                    table: "items".into(),
                    paginate: Some(Paginate {
                        cursor: None,
                        num_items: 3,
                    }),
                    unique: true,
                    ..Default::default()
                },
            ),
            (
                "first",
                Query {
                    table: "items".into(),
                    paginate: Some(Paginate {
                        cursor: None,
                        num_items: 3,
                    }),
                    first: true,
                    ..Default::default()
                },
            ),
        ] {
            let err = c.run_query(&q).unwrap_err();
            assert_eq!(err.code, ErrorCode::BadRequest, "case '{needle}'");
            assert!(
                err.message.contains(needle),
                "case '{needle}' missing needle: got {}",
                err.message
            );
        }
    }

    #[tokio::test]
    async fn query_search_returns_empty_array_stub() {
        // No in-memory ts_rank — the cascade agrees with the server by
        // returning [] for a valid `search`, while still rejecting conflicting
        // combinations.
        let c = new_client();
        let v = c
            .run::<Vec<Value>>(
                &TableQuery::new("items")
                    .search("by_content", "hello", ())
                    .take(5),
            )
            .expect("search stub");
        assert!(v.is_empty(), "search stub returns []");
    }

    #[tokio::test]
    async fn query_search_rejects_conflicting_terminals() {
        let c = new_client();
        let err = c
            .run_query(&Query {
                table: "items".into(),
                search: Some(crate::wire::SearchQuery {
                    index: "by_content".into(),
                    query: "hello".into(),
                    filter: None,
                }),
                index: Some("by_name".into()),
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(
            err.message.contains("search cannot be combined"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn query_search_with_filter_returns_empty_after_narrowing() {
        // ts_rank is unavailable in-memory, so the search stub stays empty; the
        // carried `filter` is still validated and run through `matches_filter`
        // on the (empty) result set, exercising the narrowing path.
        let c = new_client();
        let v = c
            .run::<Vec<Value>>(
                &TableQuery::new("items")
                    .search(
                        "by_content",
                        "hello",
                        SearchOpts {
                            filter: Some(FilterExpr::Eq {
                                field: "status".into(),
                                value: "done".into(),
                            }),
                        },
                    )
                    .take(5),
            )
            .expect("search with filter narrows cleanly");
        assert!(v.is_empty(), "search stub still returns [] after narrowing");
    }

    #[tokio::test]
    async fn query_search_with_unknown_filter_field_is_bad_request() {
        // The search filter runs through `validate_filter` against the table's
        // declared fields, so an unknown field surfaces as BadRequest before
        // the (stub) result is returned.
        let c = new_client();
        let err = c
            .run::<Vec<Value>>(
                &TableQuery::new("items")
                    .search(
                        "by_content",
                        "hello",
                        SearchOpts {
                            filter: Some(FilterExpr::Eq {
                                field: "nonexistent".into(),
                                value: "x".into(),
                            }),
                        },
                    )
                    .take(5),
            )
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(err.message.contains("nonexistent"), "got: {err}");
    }

    #[tokio::test]
    async fn query_vector_search_returns_empty_array_stub() {
        // The TS harness rejects `vectorSearch` combined with any other
        // terminal (including `take`) — unlike `search`, vectorSearch carries
        // its own `limit`. So the bare-stub path is exercised without a
        // trailing terminal.
        let c = new_client();
        let v = c
            .run::<Vec<Value>>(
                &TableQuery::new("items")
                    .vector_search("by_embedding", vec![1.0, 0.0, 0.0], 5, ())
                    .build(),
            )
            .expect("vector stub");
        assert!(v.is_empty(), "vector stub returns []");
    }

    #[tokio::test]
    async fn query_vector_search_rejects_conflicting_terminals() {
        let c = new_client();
        let err = c
            .run_query(&Query {
                table: "items".into(),
                vector_search: Some(crate::wire::VectorSearchQuery {
                    index: "by_embedding".into(),
                    vector: vec![1.0],
                    limit: 5,
                    filter: None,
                }),
                index: Some("by_name".into()),
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(
            err.message.contains("vectorSearch cannot be combined"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn query_vector_search_with_filter_returns_empty_after_narrowing() {
        // No in-memory vector ranking, so the vector stub stays empty; the
        // carried `filter` (a `FilterExpr`) is still validated and run through
        // `matches_filter` on the (empty) candidate set, exercising the same
        // narrowing path as the `search` terminal.
        let c = new_client();
        let v = c
            .run::<Vec<Value>>(
                &TableQuery::new("items")
                    .vector_search(
                        "by_embedding",
                        vec![1.0, 0.0, 0.0],
                        5,
                        VectorSearchOpts {
                            filter: Some(FilterExpr::Eq {
                                field: "status".into(),
                                value: "done".into(),
                            }),
                        },
                    )
                    .build(),
            )
            .expect("vector search with filter narrows cleanly");
        assert!(v.is_empty(), "vector stub still returns [] after narrowing");
    }

    #[tokio::test]
    async fn query_vector_search_with_unknown_filter_field_is_bad_request() {
        // The vector-search filter runs through `validate_filter` against the
        // table's declared fields, so an unknown field surfaces as BadRequest
        // before the (stub) result is returned.
        let c = new_client();
        let err = c
            .run::<Vec<Value>>(
                &TableQuery::new("items")
                    .vector_search(
                        "by_embedding",
                        vec![1.0, 0.0, 0.0],
                        5,
                        VectorSearchOpts {
                            filter: Some(FilterExpr::Eq {
                                field: "nonexistent".into(),
                                value: "x".into(),
                            }),
                        },
                    )
                    .build(),
            )
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(err.message.contains("nonexistent"), "got: {err}");
    }

    // ---- filter: eval_filter_expr + validate_filter ----------------
    //
    // Direct unit tests for the filter evaluator + validator, ported verbatim
    // from `describe("evalFilterExpr + validateFilter")`
    // (`ts-client/tests/in_memory.test.ts:539-653`). These are the cases item C
    // fixed in the TS source — E must not regress them.

    /// The field set used by the unit tests below — mirrors the TS
    /// `new Set(["name", "age", "active", "score", "tags"])`.
    fn filter_unit_fields() -> BTreeSet<String> {
        ["name", "age", "active", "score", "tags"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    #[test]
    fn eval_filter_eq_neq_on_strings_compare_the_doc_field_text() {
        let fields = filter_unit_fields();
        validate_filter(
            &FilterExpr::Eq {
                field: "name".into(),
                value: json!("ada"),
            },
            &fields,
        )
        .expect("valid");
        assert!(eval_filter_expr(
            &FilterExpr::Eq {
                field: "name".into(),
                value: json!("ada"),
            },
            &json!({"name": "ada"}),
        ));
        assert!(!eval_filter_expr(
            &FilterExpr::Eq {
                field: "name".into(),
                value: json!("ada"),
            },
            &json!({"name": "bob"}),
        ));
        assert!(eval_filter_expr(
            &FilterExpr::Neq {
                field: "name".into(),
                value: json!("ada"),
            },
            &json!({"name": "bob"}),
        ));
    }

    #[test]
    fn eval_filter_number_domain_compares_numerically() {
        // gt/gte/lt/lte over a numeric doc field.
        assert!(eval_filter_expr(
            &FilterExpr::Gt {
                field: "age".into(),
                value: json!(30),
            },
            &json!({"age": 42}),
        ));
        assert!(!eval_filter_expr(
            &FilterExpr::Gt {
                field: "age".into(),
                value: json!(50),
            },
            &json!({"age": 42}),
        ));
        assert!(eval_filter_expr(
            &FilterExpr::Lte {
                field: "age".into(),
                value: json!(42),
            },
            &json!({"age": 42}),
        ));
    }

    #[test]
    fn eval_filter_string_ordering_is_lexicographic() {
        assert!(eval_filter_expr(
            &FilterExpr::Lt {
                field: "name".into(),
                value: json!("b"),
            },
            &json!({"name": "ada"}),
        ));
        assert!(eval_filter_expr(
            &FilterExpr::Gte {
                field: "name".into(),
                value: json!("a"),
            },
            &json!({"name": "ada"}),
        ));
    }

    #[test]
    fn eval_filter_boolean_domain_compares_booleans() {
        assert!(eval_filter_expr(
            &FilterExpr::Eq {
                field: "active".into(),
                value: json!(true),
            },
            &json!({"active": true}),
        ));
        assert!(!eval_filter_expr(
            &FilterExpr::Eq {
                field: "active".into(),
                value: json!(true),
            },
            &json!({"active": false}),
        ));
    }

    #[test]
    fn eval_filter_number_value_matches_a_numeric_string_field() {
        // float8 cast: doc field is the string "5", filter value is the number
        // 5 → match. Mirrors Postgres `(doc->>'field')::float8 = 5`.
        assert!(eval_filter_expr(
            &FilterExpr::Eq {
                field: "score".into(),
                value: json!(5),
            },
            &json!({"score": "5"}),
        ));
    }

    #[test]
    fn eval_filter_null_or_absent_doc_field_never_matches() {
        // SQL NULL exclusion: null/absent never matches any op (even neq).
        assert!(!eval_filter_expr(
            &FilterExpr::Eq {
                field: "name".into(),
                value: json!("ada"),
            },
            &json!({"name": null}),
        ));
        assert!(!eval_filter_expr(
            &FilterExpr::Eq {
                field: "name".into(),
                value: json!("ada"),
            },
            &json!({}),
        ));
        assert!(!eval_filter_expr(
            &FilterExpr::Neq {
                field: "name".into(),
                value: json!("ada"),
            },
            &json!({}),
        ));
    }

    #[test]
    fn eval_filter_and_or_nest_recursively() {
        let expr = FilterExpr::And {
            exprs: vec![
                FilterExpr::Gte {
                    field: "age".into(),
                    value: json!(30),
                },
                FilterExpr::Or {
                    exprs: vec![
                        FilterExpr::Eq {
                            field: "name".into(),
                            value: json!("ada"),
                        },
                        FilterExpr::Eq {
                            field: "name".into(),
                            value: json!("bob"),
                        },
                    ],
                },
            ],
        };
        assert!(eval_filter_expr(&expr, &json!({"age": 42, "name": "ada"})));
        assert!(!eval_filter_expr(&expr, &json!({"age": 42, "name": "zed"})));
        assert!(!eval_filter_expr(&expr, &json!({"age": 10, "name": "ada"})));
    }

    #[test]
    fn eval_filter_in_matches_membership() {
        assert!(eval_filter_expr(
            &FilterExpr::In {
                field: "name".into(),
                values: vec![json!("ada"), json!("bob")],
            },
            &json!({"name": "bob"}),
        ));
        assert!(!eval_filter_expr(
            &FilterExpr::In {
                field: "name".into(),
                values: vec![json!("ada"), json!("bob")],
            },
            &json!({"name": "zed"}),
        ));
    }

    #[test]
    fn validate_filter_rejects_an_unknown_field() {
        let fields = filter_unit_fields();
        let err = validate_filter(
            &FilterExpr::Eq {
                field: "missing".into(),
                value: json!("x"),
            },
            &fields,
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(err.message.contains("unknown field"), "got: {err}");
    }

    #[test]
    fn validate_filter_rejects_empty_and_or_and_empty_in() {
        let fields = filter_unit_fields();
        let err = validate_filter(&FilterExpr::And { exprs: vec![] }, &fields).unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(err.message.contains("at least one expr"), "got: {err}");

        let err = validate_filter(&FilterExpr::Or { exprs: vec![] }, &fields).unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(err.message.contains("at least one expr"), "got: {err}");

        let err = validate_filter(
            &FilterExpr::In {
                field: "name".into(),
                values: vec![],
            },
            &fields,
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(err.message.contains("at least one value"), "got: {err}");
    }

    #[test]
    fn validate_filter_rejects_a_non_string_number_boolean_value() {
        let fields = filter_unit_fields();
        let err = validate_filter(
            &FilterExpr::Eq {
                field: "name".into(),
                value: Value::Null,
            },
            &fields,
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(
            err.message.contains("string, number, or boolean"),
            "got: {err}"
        );

        let err = validate_filter(
            &FilterExpr::Eq {
                field: "tags".into(),
                value: json!(["a"]),
            },
            &fields,
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(
            err.message.contains("string, number, or boolean"),
            "got: {err}"
        );
    }

    #[test]
    fn validate_filter_accepts_a_well_formed_nested_filter() {
        let fields = filter_unit_fields();
        validate_filter(
            &FilterExpr::And {
                exprs: vec![
                    FilterExpr::Eq {
                        field: "name".into(),
                        value: json!("ada"),
                    },
                    FilterExpr::In {
                        field: "age".into(),
                        values: vec![json!(1), json!(2)],
                    },
                ],
            },
            &fields,
        )
        .expect("well-formed nested filter");
    }

    #[test]
    fn validate_filter_rejects_mixed_type_in_values() {
        let fields = filter_unit_fields();
        let err = validate_filter(
            &FilterExpr::In {
                field: "age".into(),
                values: vec![json!(5), json!("ada")],
            },
            &fields,
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(err.message.contains("same type"), "got: {err}");
    }

    #[test]
    fn validate_filter_accepts_same_type_in_values() {
        let fields = filter_unit_fields();
        validate_filter(
            &FilterExpr::In {
                field: "age".into(),
                values: vec![json!(5), json!(6), json!(7)],
            },
            &fields,
        )
        .expect("same-type in values");
    }

    // ---- query: filter end-to-end ----------------------------------
    //
    // Ports `describe("InMemoryRtDbClient filter")`
    // (`ts-client/tests/in_memory.test.ts:655-756`) — exercises the typed
    // `TableQuery.filter(...)` builder end-to-end through `run_query`, the
    // same surface live app code uses.

    /// Self-contained `users` schema so this block doesn't perturb the shared
    /// `items` harness above. Mirrors the TS `usersSchema`.
    fn users_schema() -> SchemaDef {
        Schema::builder()
            .table(
                "users",
                Table::new()
                    .field("name", FieldType::String)
                    .field("age", FieldType::Number)
                    .field("active", FieldType::Boolean)
                    .index("by_name", &["name"]),
            )
            .build()
    }

    fn new_users_client() -> InMemoryRtDbClient {
        let counter = Arc::new(Mutex::new(1_700_000_000_000_i64));
        let mut client = InMemoryRtDbClient::new(
            InMemoryRtDbClientOptions::default()
                .now(move || {
                    let mut g = counter.lock().expect("counter not poisoned");
                    let v = *g;
                    *g += 1;
                    v
                })
                .random(|| 0.0),
        );
        client.push_schema(&users_schema()).unwrap();
        client
    }

    async fn seed_users(c: &mut InMemoryRtDbClient) {
        for (name, age, active) in [("ada", 42_i64, true), ("bob", 17, false), ("cy", 65, true)] {
            c.mutate(
                &Mutation::new()
                    .insert("users", json!({"name": name, "age": age, "active": active}))
                    .build(),
                None,
            )
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn query_filter_reduces_the_result_set_to_matching_docs() {
        let mut c = new_users_client();
        seed_users(&mut c).await;
        let docs = c
            .run::<Vec<Value>>(
                &TableQuery::new("users")
                    .filter(FilterExpr::Gt {
                        field: "age".into(),
                        value: json!(20),
                    })
                    .collect(),
            )
            .expect("filter query ok");
        let mut names: Vec<String> = docs
            .iter()
            .map(|d| d["name"].as_str().unwrap().to_string())
            .collect();
        names.sort();
        assert_eq!(names, vec!["ada".to_string(), "cy".to_string()]);
    }

    #[tokio::test]
    async fn query_filter_composes_with_an_index_eq_prefix_and_take() {
        let mut c = new_users_client();
        seed_users(&mut c).await;
        let docs = c
            .run::<Vec<Value>>(
                &TableQuery::new("users")
                    .with_index("by_name", &[json!("ada")])
                    .filter(FilterExpr::Eq {
                        field: "active".into(),
                        value: json!(true),
                    })
                    .take(10),
            )
            .expect("filter+index ok");
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["name"], json!("ada"));
    }

    #[tokio::test]
    async fn query_and_or_in_filter_evaluates_correctly_end_to_end() {
        let mut c = new_users_client();
        seed_users(&mut c).await;

        let docs = c
            .run::<Vec<Value>>(
                &TableQuery::new("users")
                    .filter(FilterExpr::Or {
                        exprs: vec![
                            FilterExpr::Lt {
                                field: "age".into(),
                                value: json!(18),
                            },
                            FilterExpr::Gte {
                                field: "age".into(),
                                value: json!(65),
                            },
                        ],
                    })
                    .collect(),
            )
            .expect("or filter ok");
        let mut names: Vec<String> = docs
            .iter()
            .map(|d| d["name"].as_str().unwrap().to_string())
            .collect();
        names.sort();
        assert_eq!(names, vec!["bob".to_string(), "cy".to_string()]);

        let in_docs = c
            .run::<Vec<Value>>(
                &TableQuery::new("users")
                    .filter(FilterExpr::In {
                        field: "name".into(),
                        values: vec![json!("ada"), json!("cy")],
                    })
                    .collect(),
            )
            .expect("in filter ok");
        let mut names: Vec<String> = in_docs
            .iter()
            .map(|d| d["name"].as_str().unwrap().to_string())
            .collect();
        names.sort();
        assert_eq!(names, vec!["ada".to_string(), "cy".to_string()]);
    }

    #[tokio::test]
    async fn query_filter_unknown_field_throws_bad_request() {
        let mut c = new_users_client();
        seed_users(&mut c).await;
        let err = c
            .run_query(
                &TableQuery::new("users")
                    .filter(FilterExpr::Eq {
                        field: "nope".into(),
                        value: json!("x"),
                    })
                    .collect(),
            )
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
    }

    #[tokio::test]
    async fn query_filter_combined_with_get_is_rejected() {
        // Mirrors the server: `get` is exclusive of `filter` (and everything
        // else); the get-exclusivity guard fires before filter validation.
        let mut c = new_users_client();
        let r = c
            .mutate(
                &Mutation::new()
                    .insert("users", json!({"name": "ada", "age": 42, "active": true}))
                    .build(),
                None,
            )
            .await
            .unwrap();
        let id = match &r[0] {
            StepResult::Insert { id } => id.clone(),
            _ => unreachable!(),
        };
        let err = c
            .run_query(&Query {
                table: "users".into(),
                get: Some(id),
                filter: Some(FilterExpr::Eq {
                    field: "age".into(),
                    value: json!(42),
                }),
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
    }

    // ---- subscribe --------------------------------------------------------
    //
    // Ports `describe("InMemoryRtDbClient — subscribe")`
    // (`ts-client/tests/in_memory.test.ts:229-248`). The harness re-runs each
    // subscriber's query on a successful txn that touched its table, and fires
    // its callback iff the canonicalized result changed. The initial value is
    // delivered synchronously inside `subscribe`.

    /// Mirror of the TS `subscribe` test: a `count()` over `by_status=todo`
    /// starts at 0, goes to 1 on a todo insert, and stays at 1 on a done
    /// insert (different table-write, but same table — done doesn't change the
    /// todo count). Unsubscribing stops further updates.
    #[tokio::test]
    async fn subscribe_delivers_initial_value_and_recomputes_only_on_change() {
        let mut c = new_client();
        let updates: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));
        let updates_clone = updates.clone();
        let _unsub = c.subscribe(
            TableQuery::new("items")
                .with_index("by_status", &[json!("todo")])
                .count(),
            move |v| {
                if let Some(n) = v.as_i64() {
                    updates_clone.lock().expect("not poisoned").push(n);
                }
            },
        );
        assert_eq!(
            updates.lock().expect("not poisoned").as_slice(),
            &[0],
            "initial value delivered synchronously"
        );

        c.mutate(
            &Mutation::new()
                .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
                .build(),
            None,
        )
        .await
        .expect("insert todo");
        assert_eq!(
            updates.lock().expect("not poisoned").as_slice(),
            &[0, 1],
            "todo insert bumped the count"
        );

        // A write to a different status doesn't change the todo count, so the
        // callback is not invoked.
        c.mutate(
            &Mutation::new()
                .insert("items", json!({"name": "b", "status": "done", "order": 2}))
                .build(),
            None,
        )
        .await
        .expect("insert done");
        assert_eq!(
            updates.lock().expect("not poisoned").as_slice(),
            &[0, 1],
            "done insert did not change the todo count"
        );
    }

    #[tokio::test]
    async fn subscribe_unsubscribe_stops_further_updates() {
        let mut c = new_client();
        let updates: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));
        let updates_clone = updates.clone();
        let unsub = c.subscribe(
            TableQuery::new("items")
                .with_index("by_status", &[json!("todo")])
                .count(),
            move |v| {
                if let Some(n) = v.as_i64() {
                    updates_clone.lock().expect("not poisoned").push(n);
                }
            },
        );
        assert_eq!(updates.lock().expect("not poisoned").as_slice(), &[0]);

        // Explicit unsubscribe (the Drop path is exercised by the next test).
        unsub.unsubscribe();

        c.mutate(
            &Mutation::new()
                .insert("items", json!({"name": "c", "status": "todo", "order": 3}))
                .build(),
            None,
        )
        .await
        .expect("insert todo");
        assert_eq!(
            updates.lock().expect("not poisoned").as_slice(),
            &[0],
            "no further updates after unsubscribe"
        );
    }

    #[tokio::test]
    async fn subscribe_dropping_handle_unsubscribes() {
        // The RAII guard path: dropping the handle clears the listener.
        let mut c = new_client();
        let updates: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));
        let updates_clone = updates.clone();
        {
            let _unsub = c.subscribe(
                TableQuery::new("items")
                    .with_index("by_status", &[json!("todo")])
                    .count(),
                move |v| {
                    if let Some(n) = v.as_i64() {
                        updates_clone.lock().expect("not poisoned").push(n);
                    }
                },
            );
            assert_eq!(updates.lock().expect("not poisoned").as_slice(), &[0]);
        }
        c.mutate(
            &Mutation::new()
                .insert("items", json!({"name": "d", "status": "todo", "order": 4}))
                .build(),
            None,
        )
        .await
        .expect("insert todo");
        assert_eq!(
            updates.lock().expect("not poisoned").as_slice(),
            &[0],
            "drop(unsub) cleared the listener"
        );
    }

    // ---- schedules --------------------------------------------------------
    //
    // Ports `describe("InMemoryRtDbClient — schedules")`
    // (`ts-client/tests/in_memory.test.ts:432-537`). The harness mirrors the
    // server semantics: one-shot catches up if past due (fires once even when
    // `due_at < now`); cron steps by `CRON_STEP_MS` and skips missed windows.

    /// The TS `insertTxn` shared by every schedules test (`:433`).
    fn insert_todo_txn() -> Transaction {
        Mutation::new()
            .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
            .build()
    }

    /// Fixed-clock harness so schedule due-times are stable under `tick`
    /// (mirrors TS `newClockClient` `:33-38`). Returns the client and a setter
    /// for the clock.
    fn new_clock_client() -> (InMemoryRtDbClient, Arc<Mutex<i64>>) {
        let cell: Arc<Mutex<i64>> = Arc::new(Mutex::new(1_700_000_000_000_i64));
        let cell_for_closure = cell.clone();
        let mut client = InMemoryRtDbClient::new(
            InMemoryRtDbClientOptions::default()
                .now(move || *cell_for_closure.lock().expect("not poisoned"))
                .random(|| 0.0),
        );
        client.push_schema(&test_schema()).unwrap();
        (client, cell)
    }

    #[tokio::test]
    async fn schedule_and_tick_fires_a_due_oneshot_and_write_is_visible() {
        // Ports TS "schedule + tick fires a due one-shot and the write is
        // visible via query".
        let (mut c, clock) = new_clock_client();
        let id = c
            .schedule(insert_todo_txn(), ScheduleWhen::AfterMs { ms: 1000 })
            .expect("schedule ok");
        assert!(is_hex_id(&json!(id)), "id is 32 hex chars: {id}");

        *clock.lock().expect("not poisoned") += 2000; // past the due time
        c.tick(None);

        let docs = c
            .run::<Vec<Value>>(&TableQuery::new("items").collect())
            .expect("collect ok");
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["name"], json!("a"));
        // A fired one-shot is removed from the registry.
        let remaining = c.list_schedules();
        assert!(
            remaining.iter().all(|s| s.id != id),
            "fired oneshot removed"
        );
    }

    #[tokio::test]
    async fn tick_does_not_fire_a_not_yet_due_oneshot() {
        let (mut c, clock) = new_clock_client();
        c.schedule(insert_todo_txn(), ScheduleWhen::AfterMs { ms: 5000 })
            .expect("schedule ok");

        *clock.lock().expect("not poisoned") += 1000; // before the due time
        c.tick(None);

        let docs = c
            .run::<Vec<Value>>(&TableQuery::new("items").collect())
            .expect("collect ok");
        assert!(docs.is_empty(), "not yet due — no fire");
    }

    #[tokio::test]
    async fn tick_does_not_fire_a_paused_job() {
        // Ports TS "a paused scheduled job does not fire on tick".
        let (mut c, clock) = new_clock_client();
        let id = c
            .schedule(insert_todo_txn(), ScheduleWhen::AfterMs { ms: 1000 })
            .expect("schedule ok");
        c.pause_schedule(&id).expect("pause ok");

        *clock.lock().expect("not poisoned") += 2000; // due, but paused
        c.tick(None);

        let docs = c
            .run::<Vec<Value>>(&TableQuery::new("items").collect())
            .expect("collect ok");
        assert!(docs.is_empty(), "paused — no fire");
        let info = c
            .list_schedules()
            .into_iter()
            .find(|s| s.id == id)
            .expect("paused job still listed");
        assert_eq!(info.status.as_wire_str(), "paused");
    }

    #[tokio::test]
    async fn cancel_schedule_removes_the_job() {
        // Ports TS "cancelSchedule removes the job so it does not fire on tick".
        let (mut c, clock) = new_clock_client();
        let id = c
            .schedule(insert_todo_txn(), ScheduleWhen::AfterMs { ms: 1000 })
            .expect("schedule ok");
        c.cancel_schedule(&id).expect("cancel ok");
        assert!(
            c.list_schedules().iter().all(|s| s.id != id),
            "cancelled id no longer listed"
        );

        *clock.lock().expect("not poisoned") += 2000;
        c.tick(None);

        let docs = c
            .run::<Vec<Value>>(&TableQuery::new("items").collect())
            .expect("collect ok");
        assert!(docs.is_empty(), "cancelled — no fire");
    }

    #[tokio::test]
    async fn pause_then_resume_lets_the_job_fire_on_a_later_tick() {
        // Ports TS "pause then resume lets the job fire on a later tick".
        let (mut c, clock) = new_clock_client();
        let id = c
            .schedule(insert_todo_txn(), ScheduleWhen::AfterMs { ms: 1000 })
            .expect("schedule ok");
        c.pause_schedule(&id).expect("pause ok");
        *clock.lock().expect("not poisoned") += 2000;
        c.tick(None);
        assert_eq!(
            c.run::<Vec<Value>>(&TableQuery::new("items").collect())
                .expect("collect")
                .len(),
            0,
            "still paused at the first tick"
        );

        c.resume_schedule(&id).expect("resume ok");
        let info = c
            .list_schedules()
            .into_iter()
            .find(|s| s.id == id)
            .expect("resumed job listed");
        assert_eq!(info.status.as_wire_str(), "pending");

        c.tick(None);
        let docs = c
            .run::<Vec<Value>>(&TableQuery::new("items").collect())
            .expect("collect");
        assert_eq!(docs.len(), 1, "fired after resume");
    }

    #[tokio::test]
    async fn list_schedules_returns_server_aligned_info() {
        // Ports TS "listSchedules returns schedule info with server-aligned
        // status/kind names".
        let (mut c, _clock) = new_clock_client();
        let id = c
            .schedule(
                insert_todo_txn(),
                ScheduleWhen::Cron {
                    expr: "* * * * *".to_string(),
                },
            )
            .expect("schedule ok");

        let list = c.list_schedules();
        assert_eq!(list.len(), 1);
        let info = &list[0];
        assert_eq!(info.id, id);
        assert_eq!(info.kind.as_wire_str(), "cron");
        assert_eq!(info.status.as_wire_str(), "pending");
        assert_eq!(info.cron.as_deref(), Some("* * * * *"));
        assert_eq!(info.fired_count, 0);
        // dueAt / createdAt are present (numbers).
        let _ = info.due_at;
        let _ = info.created_at;
    }

    #[tokio::test]
    async fn cancel_pause_resume_on_unknown_id_returns_not_found() {
        // Ports TS "cancel/pause/resume on an unknown id reject with
        // NOT_FOUND".
        let (mut c, _clock) = new_clock_client();
        let err = c.cancel_schedule("nope").unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
        let err = c.pause_schedule("nope").unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
        let err = c.resume_schedule("nope").unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
    }

    #[tokio::test]
    async fn tick_cron_re_arms_and_fires_again_on_a_later_tick() {
        // The TS suite does not cover cron re-arm directly, but the brief calls
        // it out: cron steps by `CRON_STEP_MS` and fires again on a later tick.
        // Skipping missed windows is verified separately.
        let (mut c, clock) = new_clock_client();
        // The cron's initial due_at is `now + CRON_STEP_MS` (per `dueAtFor`),
        // so a tick at the schedule-time `now` does nothing. Advance one step
        // before the first fire.
        c.schedule(
            insert_todo_txn(),
            ScheduleWhen::Cron {
                expr: "* * * * *".to_string(),
            },
        )
        .expect("schedule ok");

        // First fire: advance one CRON_STEP_MS.
        *clock.lock().expect("not poisoned") += CRON_STEP_MS;
        c.tick(None);
        assert_eq!(
            c.run::<Vec<Value>>(&TableQuery::new("items").collect())
                .expect("collect")
                .len(),
            1,
            "cron fired once"
        );
        // Immediately re-ticking without advancing the clock does nothing —
        // the next due_at is now + CRON_STEP_MS.
        c.tick(None);
        assert_eq!(
            c.list_schedules().len(),
            1,
            "cron still registered (not removed after fire)"
        );
        let fired_count = c.list_schedules()[0].fired_count;
        assert_eq!(fired_count, 1, "fired_count tracks successful fires");

        // Advance the clock one CRON_STEP_MS — the cron should fire again.
        *clock.lock().expect("not poisoned") += CRON_STEP_MS;
        c.tick(None);
        assert_eq!(
            c.run::<Vec<Value>>(&TableQuery::new("items").collect())
                .expect("collect")
                .len(),
            2,
            "cron fired a second time after re-arm"
        );
        let fired_count = c.list_schedules()[0].fired_count;
        assert_eq!(fired_count, 2);
    }

    #[tokio::test]
    async fn tick_cron_skips_missed_windows_does_not_backfill() {
        // Brief: cron skips missed windows — no N-fires for N missed windows.
        // Advance the clock many CRON_STEP_MS beyond the due_at; the cron fires
        // exactly once and re-arms one step ahead of `now`.
        let (mut c, _clock) = new_clock_client();
        c.schedule(
            insert_todo_txn(),
            ScheduleWhen::Cron {
                expr: "* * * * *".to_string(),
            },
        )
        .expect("schedule ok");

        // Jump 10 × CRON_STEP_MS past the due time and tick once.
        let big_jump = CRON_STEP_MS * 10;
        c.tick(Some(1_700_000_000_000_i64 + big_jump));

        let docs = c
            .run::<Vec<Value>>(&TableQuery::new("items").collect())
            .expect("collect");
        assert_eq!(docs.len(), 1, "missed windows are not backfilled");
        let info = &c.list_schedules()[0];
        assert_eq!(info.fired_count, 1, "fired exactly once");
        // Re-armed to `now + CRON_STEP_MS` (not `due_at + N × CRON_STEP_MS`).
        assert_eq!(info.due_at, 1_700_000_000_000_i64 + big_jump + CRON_STEP_MS);
    }

    #[tokio::test]
    async fn tick_oneshot_in_the_past_fires_immediately_catch_up() {
        // Brief: one-shot catches up if past due — a `RunAt` in the past fires
        // once even when `due_at < now`.
        let (mut c, _clock) = new_clock_client();
        c.schedule(
            insert_todo_txn(),
            ScheduleWhen::RunAt {
                ms: 1_600_000_000_000, // 100B ms before the clock's starting value
            },
        )
        .expect("schedule ok");
        c.tick(None);
        let docs = c
            .run::<Vec<Value>>(&TableQuery::new("items").collect())
            .expect("collect");
        assert_eq!(docs.len(), 1, "past-due oneshot catches up");
        assert!(c.list_schedules().is_empty(), "oneshot removed after fire");
    }

    #[tokio::test]
    async fn tick_oneshot_with_failing_txn_marks_error_and_keeps_it() {
        // A failing txn records `last_error` and flips status to `Error`. The
        // TS source keeps a failed oneshot in the registry (only crons re-arm).
        let (mut c, _clock) = new_clock_client();
        let id = c
            .schedule(
                // Reference an unknown table to force a NOT_FOUND.
                Mutation::new().insert("missing", json!({"x": 1})).build(),
                ScheduleWhen::AfterMs { ms: 0 },
            )
            .expect("schedule ok");
        c.tick(None);
        let info = c
            .list_schedules()
            .into_iter()
            .find(|s| s.id == id)
            .expect("failed oneshot kept in registry");
        assert_eq!(info.status.as_wire_str(), "error");
        assert!(
            info.last_error.is_some(),
            "last_error recorded: {:?}",
            info.last_error
        );
    }

    // ---- storage ----------------------------------------------------------
    //
    // The TS suite does not cover storage directly (the harness ships it as an
    // honest stub); these exercise the surface so the wire shapes stay aligned
    // with the live HTTP client (`crate::http::UploadResult` /
    // `crate::http::FileMetadata`).

    #[test]
    fn upload_stores_bytes_and_returns_id_sha_size_and_content_type() {
        let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
        let bytes = b"hello world".to_vec();
        let result = c
            .upload(bytes.clone(), Some("text/plain".to_string()))
            .expect("upload ok");
        // Id is `f<base36>` — distinct in shape from a 32-hex-char doc id.
        assert!(result.id.starts_with('f'), "id shape: {}", result.id);
        assert_eq!(result.size, bytes.len() as i64);
        assert_eq!(result.content_type.as_deref(), Some("text/plain"));
        // SHA-256 of "hello world" is a known constant — verifies we computed
        // it correctly (not just non-empty).
        assert_eq!(
            result.sha256,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn upload_without_content_type_returns_none() {
        let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
        let result = c.upload(b"x".to_vec(), None).expect("upload ok");
        assert!(result.content_type.is_none());
    }

    #[test]
    fn upload_mints_distinct_ids_for_distinct_uploads() {
        let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
        let a = c.upload(b"a".to_vec(), None).expect("upload ok");
        let b = c.upload(b"b".to_vec(), None).expect("upload ok");
        assert_ne!(a.id, b.id, "ids distinct");
    }

    #[test]
    fn get_file_metadata_returns_size_and_creation_time() {
        // Mirrors the TS harness: getFileMetadata's sha256 is "" (only the
        // upload result carries the real digest).
        let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
        let up = c
            .upload(
                b"abc".to_vec(),
                Some("application/octet-stream".to_string()),
            )
            .expect("upload ok");
        let meta = c.get_file_metadata(&up.id).expect("metadata ok");
        assert_eq!(meta.id, up.id);
        assert_eq!(meta.size, 3);
        assert_eq!(meta.sha256, "");
        assert_eq!(
            meta.content_type.as_deref(),
            Some("application/octet-stream")
        );
        assert!(meta.creation_time > 0);
    }

    #[test]
    fn get_file_metadata_unknown_id_is_not_found() {
        let c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
        let err = c.get_file_metadata("f99").unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
    }

    #[test]
    fn delete_file_removes_the_blob_and_rejects_unknown_id_with_not_found() {
        let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
        let up = c.upload(b"x".to_vec(), None).expect("upload ok");
        c.delete_file(&up.id).expect("delete ok");
        // Second delete fails — NOT_FOUND (idempotent on the live server, but
        // the in-memory harness mirrors the TS surface which throws on miss).
        let err = c.delete_file(&up.id).unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
    }

    #[test]
    fn get_url_returns_synthetic_memory_handle() {
        let c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
        assert_eq!(c.get_url("f1"), "memory://f1");
    }

    // ---- unique / partial-unique index enforcement -------------------------
    //
    // Mirrors the TS `checkUniqueIndexes` suite: a `unique` index rejects a
    // colliding insert/patch/replace/upsert with `Conflict`; a partial unique
    // index (`where` predicate) constrains only rows matching the predicate;
    // uniqueness is on declared `fields` only (never `id`/`created_at`), and a
    // NULL/absent key field disables the constraint for that row (Postgres
    // UNIQUE treats NULLs as distinct). Rollback reuses the snapshot/restore
    // path shared with the `PreconditionFailed` checks.

    fn unique_users_schema() -> SchemaDef {
        // `users(email, org, archived)` with a unique `by_email` btree index.
        Schema::builder()
            .table(
                "users",
                Table::new()
                    .field("email", FieldType::String)
                    .field("org", FieldType::String)
                    .field("archived", FieldType::optional(FieldType::Boolean))
                    .index("by_email", &["email"])
                    .unique(),
            )
            .build()
    }

    /// A client whose injected clock advances one millisecond per call, so each
    /// `new_id()` (timestamp-prefixed) mints a distinct id even for back-to-back
    /// inserts in the same txn. The default options have a constant clock, which
    /// collapses same-txn inserts to identical ids (HashMap self-collision).
    fn unique_client() -> InMemoryRtDbClient {
        let counter = Arc::new(Mutex::new(1_700_000_000_000_i64));
        InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default().now(move || {
            let mut g = counter.lock().expect("counter not poisoned");
            let v = *g;
            *g += 1;
            v
        }))
    }

    fn partial_users_schema() -> SchemaDef {
        // A partial unique index: constrains `email` only for rows where
        // `archived != true` (i.e. active rows).
        Schema::builder()
            .table(
                "users",
                Table::new()
                    .field("email", FieldType::String)
                    .field("org", FieldType::String)
                    .field("archived", FieldType::optional(FieldType::Boolean))
                    .index("by_email_active", &["email"])
                    .unique()
                    .where_clause(FilterExpr::Neq {
                        field: "archived".into(),
                        value: json!(true),
                    }),
            )
            .build()
    }

    /// Collect the table's stored docs as a JSON array (a bare `collect` query).
    fn collect_table(c: &InMemoryRtDbClient, table: &str) -> Vec<Value> {
        let r = c
            .run_query(&Query {
                table: table.into(),
                ..Default::default()
            })
            .unwrap();
        r.as_array().expect("collect returns an array").clone()
    }

    #[tokio::test]
    async fn unique_index_rejects_duplicate_insert_with_conflict() {
        let mut c = unique_client();
        c.push_schema(&unique_users_schema()).unwrap();
        c.mutate(
            &Mutation::new()
                .insert("users", json!({"email": "a@b.com", "org": "x"}))
                .build(),
            None,
        )
        .await
        .unwrap();
        // A second insert with the same `email` violates `by_email`.
        let err = c
            .mutate(
                &Mutation::new()
                    .insert("users", json!({"email": "a@b.com", "org": "y"}))
                    .build(),
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::Conflict);
        assert!(
            err.message.contains("unique index 'by_email' violated"),
            "got: {err}"
        );
        // The whole txn rolled back: only the first row remains.
        assert_eq!(
            collect_table(&c, "users").len(),
            1,
            "conflicting insert rolled back"
        );
    }

    #[tokio::test]
    async fn unique_index_allows_distinct_keys() {
        let mut c = unique_client();
        c.push_schema(&unique_users_schema()).unwrap();
        c.mutate(
            &Mutation::new()
                .insert("users", json!({"email": "a@b.com", "org": "x"}))
                .insert("users", json!({"email": "c@d.com", "org": "y"}))
                .build(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(collect_table(&c, "users").len(), 2);
    }

    #[tokio::test]
    async fn unique_index_rejects_collision_via_patch_with_conflict() {
        // Patching an existing row's `email` to a value already taken by another
        // row must Conflict (the candidate row is self-excluded by `exclude_id`).
        let mut c = unique_client();
        c.push_schema(&unique_users_schema()).unwrap();
        let res = c
            .mutate(
                &Mutation::new()
                    .insert("users", json!({"email": "a@b.com", "org": "x"}))
                    .insert("users", json!({"email": "c@d.com", "org": "y"}))
                    .build(),
                None,
            )
            .await
            .unwrap();
        let second_id = match &res[1] {
            StepResult::Insert { id } => id.clone(),
            other => panic!("expected an insert step result, got {other:?}"),
        };
        // Patch the second row's email to collide with the first → Conflict.
        let err = c
            .mutate(
                &Mutation::new()
                    .patch("users", &second_id, json!({"email": "a@b.com"}))
                    .build(),
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::Conflict);
        // Patching to its OWN email (or any non-colliding value) is allowed —
        // the row is excluded from its own uniqueness check.
        c.mutate(
            &Mutation::new()
                .patch("users", &second_id, json!({"email": "c@d.com", "org": "z"}))
                .build(),
            None,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn unique_index_rejects_collision_via_replace_with_conflict() {
        let mut c = unique_client();
        c.push_schema(&unique_users_schema()).unwrap();
        let res = c
            .mutate(
                &Mutation::new()
                    .insert("users", json!({"email": "a@b.com", "org": "x"}))
                    .insert("users", json!({"email": "c@d.com", "org": "y"}))
                    .build(),
                None,
            )
            .await
            .unwrap();
        let second_id = match &res[1] {
            StepResult::Insert { id } => id.clone(),
            other => panic!("expected an insert step result, got {other:?}"),
        };
        let err = c
            .mutate(
                &Mutation::new()
                    .replace("users", &second_id, json!({"email": "a@b.com", "org": "y"}))
                    .build(),
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::Conflict);
    }

    #[tokio::test]
    async fn partial_unique_index_allows_predicate_excluded_duplicate() {
        // Predicate `archived != true`: a row with `archived: true` is excluded
        // from the constraint, so two archived rows may share an email.
        let mut c = unique_client();
        c.push_schema(&partial_users_schema()).unwrap();
        c.mutate(
            &Mutation::new()
                .insert(
                    "users",
                    json!({"email": "dup@b.com", "org": "x", "archived": true}),
                )
                .insert(
                    "users",
                    json!({"email": "dup@b.com", "org": "y", "archived": true}),
                )
                .build(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            collect_table(&c, "users").len(),
            2,
            "archived dupes are unconstrained"
        );
    }

    #[tokio::test]
    async fn partial_unique_index_rejects_predicate_matching_duplicate() {
        // Two active rows (archived explicitly false ⇒ `archived != true` holds)
        // sharing an email must Conflict. (A doc with `archived` absent evaluates
        // the predicate false — SQL NULL exclusion — and is unconstrained, so the
        // rows must carry `archived: false` to land inside the partial index.)
        let mut c = unique_client();
        c.push_schema(&partial_users_schema()).unwrap();
        c.mutate(
            &Mutation::new()
                .insert(
                    "users",
                    json!({"email": "dup@b.com", "org": "x", "archived": false}),
                )
                .build(),
            None,
        )
        .await
        .unwrap();
        let err = c
            .mutate(
                &Mutation::new()
                    .insert(
                        "users",
                        json!({"email": "dup@b.com", "org": "y", "archived": false}),
                    )
                    .build(),
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::Conflict);
        assert!(
            err.message
                .contains("unique index 'by_email_active' violated"),
            "got: {err}"
        );
    }

    // ---- presence ----------------------------------------------------------
    //
    // Ports the presence surface of `ts-client/src/in_memory.ts:1217-1285`.
    // A private PresenceRooms sees only self; a shared backing lets two clients
    // see each other's joins/updates/leaves — approximating the server's
    // per-connection registry for tests.

    fn new_presence_client(conn: &str, rooms: Arc<Mutex<PresenceRooms>>) -> InMemoryRtDbClient {
        InMemoryRtDbClient::new(
            InMemoryRtDbClientOptions::default()
                .connection_id(conn)
                .presence_user(AuthedUser {
                    kind: crate::wire::UserKind::User,
                    email: Some(format!("{conn}@x.com")),
                    name: None,
                    github_login: None,
                    github_id: None,
                })
                .presence_rooms(rooms),
        )
    }

    #[tokio::test]
    async fn presence_join_fires_initial_snapshot_with_self() {
        // Brief: join a room; callback fires immediately with a one-member
        // snapshot (the joining connection itself).
        let mut c =
            InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default().connection_id("c1"));
        let snaps: Arc<Mutex<Vec<Vec<PresenceMember>>>> = Arc::new(Mutex::new(Vec::new()));
        let snaps_clone = snaps.clone();
        let _h = c.presence("doc:1", Some(json!({"cursor": 5})), move |members| {
            snaps_clone.lock().unwrap().push(members);
        });
        let got = snaps.lock().unwrap();
        assert_eq!(got.len(), 1, "initial snapshot delivered on join");
        assert_eq!(got[0].len(), 1);
        assert_eq!(got[0][0].connection_id, "c1");
        assert_eq!(got[0][0].state, json!({"cursor": 5}));
    }

    #[tokio::test]
    async fn presence_update_broadcasts_new_state() {
        // Brief: update_presence fans out a fresh snapshot with the new state.
        let mut c =
            InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default().connection_id("c1"));
        let snaps: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
        let snaps_clone = snaps.clone();
        let _h = c.presence("room", None, move |members| {
            if let Some(m) = members.first() {
                snaps_clone.lock().unwrap().push(m.state.clone());
            }
        });
        c.update_presence("room", json!({"typing": true}), None);
        let got = snaps.lock().unwrap();
        assert_eq!(got.len(), 2, "initial + update");
        assert_eq!(got[1], json!({"typing": true}));
    }

    #[tokio::test]
    async fn presence_update_noop_for_unjoined_room() {
        // Brief: update_presence on a room we haven't joined does nothing.
        let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
        let snaps: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
        let snaps_clone = snaps.clone();
        let _h = c.presence("room", None, move |members| {
            snaps_clone.lock().unwrap().push(members.len());
        });
        // Update a different room — no fan-out for "room".
        c.update_presence("other", json!({}), None);
        assert_eq!(snaps.lock().unwrap().len(), 1, "no new snapshot");
    }

    #[tokio::test]
    async fn presence_leave_removes_member_and_drops_listeners() {
        // Brief: leave_presence removes the member and fans out; further updates
        // to the room from a peer do not invoke the (now-dropped) callback.
        let rooms = Arc::new(Mutex::new(PresenceRooms::default()));
        let mut c1 = new_presence_client("c1", rooms.clone());
        let mut c2 = new_presence_client("c2", rooms.clone());

        let c1_snaps: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
        let c1_snaps_clone = c1_snaps.clone();
        let h1 = c1.presence("room", None, move |members| {
            c1_snaps_clone.lock().unwrap().push(members.len());
        });

        // c2 joins → c1 sees 2 members.
        let _h2 = c2.presence("room", None, |_| {});
        assert_eq!(*c1_snaps.lock().unwrap(), [1, 2]);

        // c1 leaves → its listener is dropped; the fan-out goes to remaining
        // listeners only. h1 is now inert.
        c1.leave_presence("room");
        drop(h1);

        // c2 updates — c1's callback must not fire (listener dropped).
        c2.update_presence("room", json!({"x": 1}), None);
        assert_eq!(
            *c1_snaps.lock().unwrap(),
            [1, 2],
            "no further fire after leave"
        );
    }

    #[tokio::test]
    async fn presence_two_clients_on_shared_rooms_see_each_other() {
        // Brief: two clients sharing a PresenceRooms instance see each other's
        // joins and leaves — approximating the server's per-db registry.
        let rooms = Arc::new(Mutex::new(PresenceRooms::default()));
        let mut c1 = new_presence_client("c1", rooms.clone());
        let mut c2 = new_presence_client("c2", rooms.clone());

        let c1_snaps: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let c1_snaps_clone = c1_snaps.clone();
        let _h1 = c1.presence("room", None, move |members| {
            let ids: Vec<String> = members.into_iter().map(|m| m.connection_id).collect();
            c1_snaps_clone.lock().unwrap().push(ids);
        });

        // c2 joins → c1 sees [c1, c2].
        let _h2 = c2.presence("room", None, |_| {});
        {
            let got = c1_snaps.lock().unwrap();
            assert_eq!(got.len(), 2, "initial self + c2 join");
            assert_eq!(got[1], ["c1", "c2"]);
        }

        // c2 leaves → c1 sees [c1] again.
        c2.leave_presence("room");
        {
            let got = c1_snaps.lock().unwrap();
            assert_eq!(got.len(), 3);
            assert_eq!(got[2], ["c1"]);
        }
    }

    // ---- presence ttl (ENH-015) ------------------------------------------
    //
    // Mirrors `PresenceRooms.expire` + `update(..., ttlMs, now)` in
    // `ts-client/src/in_memory.ts`: a refresh with a ttl schedules an expiry
    // sweep that nulls this member's `state` to Value::Null at `now + ttl`
    // (the member stays listed); a refresh with no ttl clears any pending
    // expiry. Mirrors the live server's `expire_once`.
    //
    // These tests drive `PresenceRooms` directly with controlled `now` values
    // (the harness's `update`/`expire` take `now` explicitly) so the expiry
    // math is deterministic without relying on the client's injected clock.
    // The client-surface helper is covered separately below.

    fn presence_member(conn: &str, state: Value) -> PresenceMember {
        PresenceMember {
            connection_id: conn.to_string(),
            user: AuthedUser {
                kind: crate::wire::UserKind::User,
                email: Some(format!("{conn}@x.com")),
                name: None,
                github_login: None,
                github_id: None,
            },
            state,
        }
    }

    #[tokio::test]
    async fn presence_ttl_expires_state_to_null_member_stays() {
        // Brief: c1 and c2 share a PresenceRooms. c1 updates with ttl_ms = 1000
        // at t = 5000. At t = 5999 nothing has expired. At t = 6000+ the sweep
        // nulls c1's state, c2 observes the null, c1 is still a member.
        let mut rooms = PresenceRooms::default();

        let c2_states: Arc<Mutex<Vec<(Value, String)>>> = Arc::new(Mutex::new(Vec::new()));
        let c2_states_clone = c2_states.clone();
        let _h2 = rooms.subscribe("room", move |members| {
            if let Some(c1) = members.iter().find(|m| m.connection_id == "c1") {
                c2_states_clone
                    .lock()
                    .unwrap()
                    .push((c1.state.clone(), c1.connection_id.clone()));
            }
        });

        // c1 joins, then refreshes with a ttl at t = 5000.
        rooms.join("room", presence_member("c1", Value::Null));
        rooms.update("room", "c1", json!({"typing": true}), Some(1000), 5000);
        {
            let got = c2_states.lock().unwrap();
            // Two observations of c1's state so far: c1 join (null), c1 update
            // (typing). (c2 has no presence entry — it only subscribes.)
            assert_eq!(got.len(), 2);
            assert_eq!(got[1].0, json!({"typing": true}));
        }

        // Before expiry: no change, expire returns false.
        assert!(!rooms.expire(5999));
        {
            let got = c2_states.lock().unwrap();
            assert_eq!(got.len(), 2, "no fire before expiry");
        }

        // At/after expiry: state → null, member stays, expire returns true.
        assert!(rooms.expire(6000));
        {
            let got = c2_states.lock().unwrap();
            assert_eq!(got.len(), 3, "one fire on expiry");
            assert_eq!(got[2].0, Value::Null, "state cleared to null");
            assert_eq!(got[2].1, "c1", "member stays in the room");
        }
        let snap = rooms.snapshot("room");
        assert_eq!(snap.len(), 1, "member stays listed after expiry");
        assert_eq!(snap[0].state, Value::Null);

        // Idempotent: a second sweep at the same instant is a no-op.
        assert!(!rooms.expire(6000));
        {
            let got = c2_states.lock().unwrap();
            assert_eq!(got.len(), 3, "no further fire");
        }
    }

    #[tokio::test]
    async fn presence_ttl_refresh_without_ttl_clears_expiry() {
        // Brief: a refresh with ttl_ms = None clears any pending expiry — the
        // state persists past the original expiry instant.
        let mut rooms = PresenceRooms::default();
        rooms.join("room", presence_member("c1", Value::Null));
        rooms.update("room", "c1", json!({"typing": true}), Some(1000), 5000);
        rooms.update("room", "c1", json!({"typing": false}), None, 5500);
        // Past the original expiry instant — no expiry, state persists.
        assert!(!rooms.expire(10_000));
        let snap = rooms.snapshot("room");
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].state, json!({"typing": false}));
    }

    #[tokio::test]
    async fn presence_ttl_leave_clears_expiry_entry() {
        // Brief: leaving clears the expiry entry, so a re-join with the same
        // connectionId does not inherit a stale ttl.
        let mut rooms = PresenceRooms::default();
        rooms.join("room", presence_member("c1", Value::Null));
        rooms.update("room", "c1", json!({"typing": true}), Some(1000), 5000);
        rooms.leave("room", "c1");
        // After leave, the expiry map should be empty (no fire, no panic).
        assert!(!rooms.expire(10_000));
        // And re-join with the same connId does not carry the old ttl.
        rooms.join("room", presence_member("c1", json!({"fresh": true})));
        assert!(!rooms.expire(10_000));
        let snap = rooms.snapshot("room");
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].state, json!({"fresh": true}));
    }

    #[tokio::test]
    async fn presence_ttl_client_expire_presence_helper() {
        // Brief: the client's `expire_presence(now)` helper drives the same
        // sweep through the client's injected clock, mirroring `tick` for the
        // document reaper. Two clients on shared rooms; one updates with a
        // short ttl; the other observes the null at expiry.
        let t: Arc<Mutex<i64>> = Arc::new(Mutex::new(0));
        let t_clone = t.clone();
        let rooms = Arc::new(Mutex::new(PresenceRooms::default()));
        let make = |conn: &'static str| {
            let t = t_clone.clone();
            let rooms = rooms.clone();
            InMemoryRtDbClient::new(
                InMemoryRtDbClientOptions::default()
                    .connection_id(conn)
                    .now(move || *t.lock().unwrap())
                    .presence_user(AuthedUser {
                        kind: crate::wire::UserKind::User,
                        email: Some(format!("{conn}@x.com")),
                        name: None,
                        github_login: None,
                        github_id: None,
                    })
                    .presence_rooms(rooms),
            )
        };
        let mut c1 = make("c1");
        let mut c2 = make("c2");

        let c2_states: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
        let c2_states_clone = c2_states.clone();
        let _h2 = c2.presence("room", None, move |members| {
            if let Some(c1) = members.iter().find(|m| m.connection_id == "c1") {
                c2_states_clone.lock().unwrap().push(c1.state.clone());
            }
        });

        let _h1 = c1.presence("room", None, |_| {});

        // Advance the clock to t = 5000 and refresh c1 with a 1000ms ttl.
        *t.lock().unwrap() = 5000;
        c1.update_presence("room", json!({"typing": true}), Some(1000));

        // Before expiry: helper returns false, no new observation.
        assert!(!c2.expire_presence(Some(5999)));
        {
            let got = c2_states.lock().unwrap();
            assert!(got.len() >= 2);
            assert_eq!(got.last().unwrap(), &json!({"typing": true}));
        }

        // After expiry: helper returns true, c2 observes the null.
        assert!(c2.expire_presence(Some(6000)));
        {
            let got = c2_states.lock().unwrap();
            assert_eq!(got.last().unwrap(), &Value::Null);
        }
    }
}
