//! In-memory par-rt-db client for unit tests. No network, no Postgres; mirrors
//! server DSL/step-result/system-field semantics. Ports
//! `ts-client/src/in_memory.ts`.
//!
//! The server (`server/src/{txn,query,schema,protocol}.rs`) is the source of
//! truth for the declarative DSL, step-result shapes, system fields, and query
//! semantics; this client mirrors them so app code can exercise query/txn/schema
//! behavior with no network and no live Postgres. It exposes the same data
//! surface as the live clients - `push_schema`, `query` (one-shot, like
//! [`crate::RtDbHttpClient`]), `mutate`/transactions (like
//! [`crate::RtDbClient`]), and `subscribe` (reactive `query_update`s) - so a
//! test can swap it in behind a shared interface.
//!
//! Split into a module directory (QA-108): `presence` (rooms/handles),
//! `query` (the `run_query` engine with index/cursor/aggregate helpers),
//! `migrate` (directive application and destructive-change detection), and
//! `validate` (value/doc validators, coercion, `FilterExpr` evaluation). This
//! `mod.rs` holds the store, the executor (`execute_transaction`/`execute_step`
//! and the per-step `do_*` helpers), and the reactive/subscription/scheduling/
//! storage surfaces. The public surface (`par_rt_db_client::in_memory::*`) is
//! unchanged - moved items are re-exported below.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::error::{ErrorCode, RtDbError};
use crate::mutation::{Step, StepResult, Transaction};
use crate::query::{Order, Query};
use crate::schema::{
    FieldType, IndexDef, OnDeleteAction, SchemaDef, TableDef, is_widening_of, strip_on_delete,
};
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
/// FM-33: hard cap on the number of rows one initiating delete step's
/// `onDelete` cascade may touch (mirrors
/// `server/src/txn.rs::MAX_CASCADE_ROWS`) — children stamped/deleted/nulled
/// plus the initiator itself, one shared counter across every row of a
/// `deleteByQuery` step. Over → `conflict`, txn aborts atomically.
pub const MAX_CASCADE_ROWS: usize = 10_000;

/// SEC-104: total documents a txn could touch in the worst case. Per-id steps
/// count 1 each; `Schedule`/`CancelSchedule`/`StartWorkflow`/`CancelWorkflow`
/// count 0 (control-flow steps touch no documents); each `patchByQuery`/
/// `deleteByQuery` step counts up to its `limit` (default and cap
/// `MAX_BY_QUERY_ROWS`). Mirrors server `txn::worst_case_affected`. Used by
/// `execute_transaction`'s [`MAX_AFFECTED_ROWS_PER_TXN`] budget check.
pub fn worst_case_affected(txn: &Transaction) -> usize {
    txn.steps
        .iter()
        .map(|step| match step {
            Step::PatchByQuery { limit, .. } | Step::DeleteByQuery { limit, .. } => {
                (*limit).unwrap_or(MAX_BY_QUERY_ROWS).min(MAX_BY_QUERY_ROWS) as usize
            }
            Step::Schedule { .. }
            | Step::CancelSchedule { .. }
            | Step::StartWorkflow { .. }
            | Step::CancelWorkflow { .. } => 0,
            _ => 1,
        })
        .sum()
}

/// FM-28/FM-29: recursive step count — a `schedule` step counts as itself
/// plus every step in its nested txn, and a `startWorkflow` step counts as
/// itself plus the sum of its spec's step txns. Mirrors the server's recursive
/// gate against [`MAX_STEPS`] (a nested tree can't smuggle past the flat cap).
fn count_steps(txn: &Transaction) -> usize {
    let mut total = txn.steps.len();
    for step in &txn.steps {
        match step {
            Step::Schedule { txn: nested, .. } => total += count_steps(nested),
            Step::StartWorkflow { spec } => {
                // An `awaitSignal` step carries no txn, so it nests nothing.
                total += spec
                    .steps
                    .iter()
                    .map(|s| s.txn.as_ref().map_or(0, count_steps))
                    .sum::<usize>();
            }
            _ => {}
        }
    }
    total
}

/// The `onDelete` action `ty` declares when it references `parent_table`, or
/// `None` when the type is not an `id`/`optional<id>` pointing at it (or
/// declares no action). Push validation guarantees an `onDelete`-bearing `Id`
/// appears only at the top level or directly under one `Optional`, so this
/// two-shape walk is exhaustive. Mirrors `server/src/txn.rs::on_delete_ref`
/// (FM-33).
fn on_delete_ref(ty: &FieldType, parent_table: &str) -> Option<OnDeleteAction> {
    match ty {
        FieldType::Id {
            table,
            on_delete: Some(action),
        } if table == parent_table => Some(*action),
        FieldType::Optional { inner } => on_delete_ref(inner, parent_table),
        _ => None,
    }
}

/// Whether ANY table in `schema` declares an `onDelete` field referencing
/// `parent` — i.e. deleting a `parent` row has app-level FK consequences the
/// caller must honor (the TTL reaper's bulk-vs-cascade branch: a plain bulk
/// delete is safe only when this returns `false`). Mirrors
/// `server/src/txn.rs::has_on_delete_children` (FM-33).
fn has_on_delete_children(schema: &SchemaDef, parent: &str) -> bool {
    schema.tables.values().any(|td| {
        td.fields
            .values()
            .any(|ty| on_delete_ref(ty, parent).is_some())
    })
}

/// Approximate cron re-fire interval for the in-memory stub. Real 5-field cron
/// parsing is deferred to the server; the harness only needs crons to re-arm.
pub const CRON_STEP_MS: i64 = 60_000;

/// Upper bound on an interval job's `everyMs`: one year in ms. Guards
/// `now + every_ms` against i64 overflow and bounds the horizon a recurring
/// job can occupy the registry for. Mirrors
/// `server/src/scheduler.rs::MAX_EVERY_MS`.
pub const MAX_EVERY_MS: i64 = 365 * 24 * 60 * 60 * 1000;

/// A stored row: the user doc plus its identity/history, kept separate so the
/// system fields (`_id`/`_creationTime`/`_version`) are merged in only at read
/// time — exactly as the server stores `doc` jsonb alongside `id`/`created_at`/
/// `version` columns.
#[derive(Debug, Clone)]
pub struct StoredRow {
    /// Server-shaped opaque row id (uuid-v7 like the server mints).
    pub id: String,
    /// The document body without system fields.
    pub doc: Value,
    /// Optimistic-concurrency version, bumped on every write.
    pub version: i64,
    /// Creation timestamp, epoch milliseconds.
    pub created_at: i64,
    /// FM-33: soft-delete stamp — `Some(ms)` marks the row soft-deleted
    /// (invisible to every read and write lookup, restorable via the
    /// `undelete` step); `None` = live. Only a `softDelete` table ever
    /// stamps, mirroring the server's `deleted_at` column.
    pub deleted_at: Option<i64>,
}

/// A stored scheduled job in the in-memory harness. `tick` fires due non-paused
/// jobs by applying `txn` through the same atomic path as `mutate`. Ports the
/// `ScheduledJob` interface at `ts-client/src/in_memory.ts:75-85`.
#[derive(Debug, Clone)]
pub struct ScheduledJob {
    /// Opaque job id.
    pub id: String,
    /// One-shot, cron, or interval.
    pub kind: ScheduleKind,
    /// The declarative transaction to fire when due.
    pub txn: Transaction,
    /// Next due time, epoch milliseconds.
    pub due_at: i64,
    /// The cron expression for cron jobs.
    pub cron: Option<String>,
    /// The fixed recurrence for interval jobs.
    pub every_ms: Option<i64>,
    /// Pending / paused / running / done / cancelled.
    pub status: ScheduleStatus,
    /// Creation timestamp, epoch milliseconds.
    pub created_at: i64,
    /// How many times the job has fired.
    pub fired_count: i64,
    /// The last firing error, if any.
    pub last_error: Option<String>,
}

/// A stored file blob with its server-side metadata. Mirrors the TS
/// `{ bytes, contentType?, createdAt }` record (`ts-client/src/in_memory.ts:498-501`).
#[derive(Debug, Clone)]
pub struct StoredBlob {
    /// Raw blob bytes.
    pub bytes: Vec<u8>,
    /// The stored `Content-Type`, when the upload carried one.
    pub content_type: Option<String>,
    /// Upload timestamp, epoch milliseconds.
    pub created_at: i64,
    /// SHA-256 hex digest of `bytes`.
    pub sha256: String,
}

/// Result of [`InMemoryRtDbClient::upload`] — the server-computed file
/// identity, content hash, size in bytes, and (if the upload carried one) the
/// stored `contentType`. Mirrors [`crate::http::UploadResult`] (`http.rs:54`)
/// byte-for-byte but is defined locally so the `in_memory` feature does not
/// depend on the `http` feature.
#[derive(Debug, Clone)]
pub struct UploadResult {
    /// Server-assigned opaque file id.
    pub id: String,
    /// SHA-256 hex digest of the stored bytes.
    pub sha256: String,
    /// Size in bytes.
    pub size: i64,
    /// The upload's `Content-Type`, when recorded.
    pub content_type: Option<String>,
}

/// File metadata returned by [`InMemoryRtDbClient::get_file_metadata`]. Mirrors
/// [`crate::http::FileMetadata`] (`http.rs:66`) plus the server-recorded
/// `creation_time`; defined locally for the same feature-isolation reason as
/// [`UploadResult`].
#[derive(Debug, Clone)]
pub struct FileMetadata {
    /// Server-assigned opaque file id.
    pub id: String,
    /// SHA-256 hex digest of the stored bytes.
    pub sha256: String,
    /// Size in bytes.
    pub size: i64,
    /// The stored `Content-Type`, when recorded.
    pub content_type: Option<String>,
    /// Upload timestamp, epoch milliseconds.
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
    /// FM-37: per-table auto-increment counters — the in-memory stand-in for
    /// the server's per-table Postgres sequences, holding the LAST value
    /// handed out (absent = never handed out; the first stamp lazily
    /// initializes from the stored max, mirroring the server's `setval(max)`
    /// on sequence creation). Persisted across additive schema pushes — a
    /// re-push never disturbs a table's numbering.
    auto_increment_counters: HashMap<String, i64>,
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
            auto_increment_counters: HashMap::new(),
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
    /// `ddl.rs::detect_destructive_changes`. The schema is validated first
    /// ([`SchemaDef::validate`], mirroring the server's `schema.validate()`
    /// before `detect_destructive_changes`), so an invalid TTL or a
    /// non-indexable index field fails with [`ErrorCode::SchemaViolation`]
    /// exactly as the live server 422s.
    ///
    /// Ports `pushSchema` in `ts-client/src/in_memory.ts:512-519`. The Rust
    /// signature takes the typed [`SchemaDef`] directly (no `toSchemaJson`
    /// conversion needed since the builder already produces the wire shape).
    pub fn push_schema(&mut self, schema: &SchemaDef) -> Result<(), RtDbError> {
        schema.validate()?;
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

    /// Snapshot of the currently-installed schema (or `None` before
    /// `push_schema`). Returns a clone so callers can freely inspect/mutate.
    pub fn to_schema_json(&self) -> Option<SchemaDef> {
        self.schema.clone()
    }

    /// Minimal point read — returns the merged doc (system fields included) for
    /// `(table, id)`, or `None` if absent. Mirrors the server's `get(id)` read
    /// semantics; a soft-deleted row is absent (FM-33). The full query DSL
    /// (`withIndex`, `order`, `take`, `filter`, …) lands in Task 3; tests that
    /// need a quick read use this until then.
    pub fn get(&self, table: &str, id: &str) -> Option<Value> {
        self.docs
            .get(&(table.to_string(), id.to_string()))
            .filter(|row| row.deleted_at.is_none())
            .map(merge_doc)
    }

    /// Test/debug helper — every merged doc in `table`, in unspecified order.
    /// Soft-deleted rows are excluded (FM-33). Not part of the query DSL;
    /// Task 3 replaces callers with proper queries.
    pub fn collect_all(&self, table: &str) -> Vec<Value> {
        self.docs
            .iter()
            .filter(|((t, _), _)| t == table)
            .filter(|(_, row)| row.deleted_at.is_none())
            .map(|(_, row)| merge_doc(row))
            .collect()
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
        if count_steps(txn) > MAX_STEPS {
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
        // FM-28: a schedule/cancelSchedule step mutates the schedule store, so
        // a failed txn must roll it back with the docs (the server inserts/
        // deletes the scheduled_txns row on the open sqlx tx, which the
        // rollback aborts).
        let schedules_snapshot = self.schedules.clone();
        let mut results = Vec::with_capacity(txn.steps.len());
        let mut write_set: BTreeSet<String> = BTreeSet::new();
        for step in &txn.steps {
            match self.execute_step(step) {
                Ok((result, written_tables)) => {
                    results.push(result);
                    // FM-33: a cascading delete can write MULTIPLE tables in one
                    // step (children + parent), so the per-step write result is a
                    // Vec — the notify fan-out stays table-keyed.
                    write_set.extend(written_tables);
                }
                Err(error) => {
                    // Atomicity: any step's error rolls back everything already
                    // applied, mirroring the server's single-transaction semantics.
                    self.restore_docs(snapshot);
                    self.schedules = schedules_snapshot;
                    return Err(error);
                }
            }
        }
        self.notify_subs(&write_set);
        Ok(results)
    }

    /// Per-step executor — ports `executeStep` (`ts-client/src/in_memory.ts:747-805`).
    /// Each step validates against the live schema, mutates `self.docs` (or, for
    /// `Expect*`, just observes), and returns the [`StepResult`] plus every
    /// table that was written (the Task 5 notify path fans out by table; a
    /// cascading delete may write several).
    fn execute_step(&mut self, step: &Step) -> Result<(StepResult, Vec<String>), RtDbError> {
        match step {
            Step::Insert { table, doc } => {
                let table_def = self.require_table(table)?.clone();
                let id = self.do_insert(table, &table_def, doc)?;
                Ok((StepResult::Insert { id }, vec![table.clone()]))
            }
            Step::Patch { table, id, fields } => {
                let table_def = self.require_table(table)?.clone();
                self.do_patch(&table_def, table, id, fields)?;
                Ok((StepResult::Null, vec![table.clone()]))
            }
            Step::Replace { table, id, doc } => {
                let table_def = self.require_table(table)?.clone();
                self.do_replace(&table_def, table, id, doc)?;
                Ok((StepResult::Null, vec![table.clone()]))
            }
            Step::Delete { table, id } => {
                let table_def = self.require_table(table)?.clone();
                // FM-33: a soft-delete table stamps the row (never a cascade
                // trigger); a hard delete expands the app-level `onDelete`
                // rules with a FRESH visited set + budget, mirroring
                // `server/src/txn.rs::step_delete`.
                let mut touched = Vec::new();
                if table_def.soft_delete {
                    self.do_soft_delete(table, id)?;
                    touched.push(table.clone());
                } else {
                    let mut visited = HashSet::new();
                    let mut cascade_rows = 0usize;
                    self.delete_row_cascade(
                        table,
                        id,
                        &mut visited,
                        &mut cascade_rows,
                        false,
                        &mut touched,
                    )?;
                }
                Ok((StepResult::Null, touched))
            }
            Step::Undelete { table, id } => {
                let table_def = self.require_table(table)?.clone();
                self.do_undelete(&table_def, table, id)?;
                Ok((StepResult::Null, vec![table.clone()]))
            }
            Step::ExpectVersion { table, id, version } => {
                self.require_table(table)?;
                self.do_expect_version(table, id, *version)?;
                Ok((StepResult::Null, Vec::new()))
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
                Ok((StepResult::Null, Vec::new()))
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
                    // FM-36: the update branch restamps `updatedAtField` into
                    // the patch fields (server `step_upsert` update branch).
                    let patch = stamp_updated_at(&table_def, patch, (self.now)());
                    let merged = apply_patch(&table_def, &row.doc, &patch)?;
                    self.do_update(&table_def, table, &row.id, merged)?;
                    Ok((
                        StepResult::Upsert {
                            id: row.id.clone(),
                            inserted: false,
                        },
                        vec![table.clone()],
                    ))
                } else {
                    let id = self.do_insert(table, &table_def, insert)?;
                    Ok((
                        StepResult::Upsert { id, inserted: true },
                        vec![table.clone()],
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
                    vec![table.clone()],
                ))
            }
            Step::DeleteByQuery {
                table,
                filter,
                limit,
            } => {
                let ((deleted, truncated), touched) =
                    self.delete_by_query(table, filter, *limit)?;
                Ok((StepResult::DeleteByQuery { deleted, truncated }, touched))
            }
            Step::Schedule { when, txn } => {
                let schedule_id = self.schedule((**txn).clone(), when.clone())?;
                Ok((StepResult::Schedule { schedule_id }, Vec::new()))
            }
            Step::CancelSchedule { id } => {
                // Matches the server: `false` (not an error) when the id is
                // missing, already fired, or already cancelled.
                let cancelled = self.cancel_schedule(id).is_ok();
                Ok((StepResult::Cancelled { cancelled }, Vec::new()))
            }
            // FM-29: this harness does not model the workflow engine (the
            // ts/python harnesses do); workflow steps fail explicitly rather
            // than pretending to run. awaitSignal engine behavior (park /
            // deliver / timeout) is likewise pinned by the server's
            // integration tests, not ported here.
            Step::StartWorkflow { .. } | Step::CancelWorkflow { .. } => Err(RtDbError::new(
                ErrorCode::Internal,
                "workflow steps are not supported by the in-memory harness",
            )),
        }
    }

    /// Stamps the table's `autoIncrementField` (FM-37) with the next value of
    /// the table's counter, overwriting any client-supplied value — the same
    /// authority model as [`stamp_updated_at`]. Runs on the two insert paths
    /// only ([`Self::do_insert`] covers `Insert` and upsert's insert branch),
    /// AFTER defaults, so the stamp also wins over a `defaults` entry; after
    /// insert the field is immutable (`apply_patch` / [`Self::do_replace`]
    /// reject changes). The value is a decimal string, matching the int64
    /// wire convention. Mirrors server `txn::stamp_auto_increment`; the
    /// harness's counter is monotonic (a failed txn rolls its docs back but
    /// the consumed number is NOT reclaimed, matching the server's
    /// non-transactional `nextval`).
    ///
    /// A table's counter is created lazily on first stamp and positioned past
    /// the max value already stored in the table's rows (server DDL creates
    /// the sequence with `setval(max)` at push time, so a declaration added
    /// to a populated table continues past the stored max rather than
    /// restarting at 1). Until a stamp is requested the counter map holds no
    /// entry, so re-pushes never disturb an established numbering.
    fn stamp_auto_increment(
        &mut self,
        table_name: &str,
        table_def: &TableDef,
        mut doc: Map<String, Value>,
    ) -> Map<String, Value> {
        if let Some(field) = &table_def.auto_increment_field {
            let next = match self.auto_increment_counters.get_mut(table_name) {
                Some(counter) => {
                    *counter += 1;
                    *counter
                }
                None => {
                    // Lazy sequence creation: position past the stored max
                    // (0 on an empty table → first stamp is 1). Soft-deleted
                    // rows count — the server scans the physical column.
                    let max = self
                        .docs
                        .iter()
                        .filter(|(key, _)| key.0 == table_name)
                        .filter_map(|(_, row)| {
                            row.doc
                                .get(field)
                                .and_then(Value::as_str)
                                .and_then(|s| s.parse::<i64>().ok())
                        })
                        .max()
                        .unwrap_or(0);
                    self.auto_increment_counters
                        .insert(table_name.to_string(), max + 1);
                    max + 1
                }
            };
            doc.insert(field.clone(), Value::String(next.to_string()));
        }
        doc
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
        // One `now` for both stamps (the server's insert path stamps ttl and
        // updatedAt from the same instant), then defaults — a defaults entry
        // on the updatedAt field loses to the stamp (server insert order).
        let now = (self.now)();
        let stamped = stamp_ttl_default(table_def, doc, now);
        let stamped = stamp_updated_at(table_def, &stamped, now);
        let stamped = apply_defaults(table_def, &stamped);
        // FM-37: the auto-increment stamp runs LAST (server insert order),
        // so it overwrites any client value AND any defaults entry on the
        // field.
        let stamped = self.stamp_auto_increment(table_name, table_def, stamped);
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
                deleted_at: None,
            },
        );
        Ok(id)
    }

    /// Patches an existing doc with `fields`, bumping `_version`. Ports
    /// `doPatch` (`ts-client/src/in_memory.ts:815-824`) — apply then update.
    /// A soft-deleted row is absent to the lookup (FM-33).
    fn do_patch(
        &mut self,
        table_def: &TableDef,
        table_name: &str,
        id: &str,
        fields: &Map<String, Value>,
    ) -> Result<(), RtDbError> {
        let key = (table_name.to_string(), id.to_string());
        let row = self
            .docs
            .get(&key)
            .filter(|row| row.deleted_at.is_none())
            .cloned()
            .ok_or_else(|| {
                RtDbError::new(ErrorCode::NotFound, format!("document '{id}' not found"))
            })?;
        // FM-36: stamp `updatedAtField` into the patch fields before the
        // merge — the same seam the server stamps (step_patch, and via the
        // shared patch paths patchByQuery + cascade setNull land here too).
        let fields = stamp_updated_at(table_def, fields, (self.now)());
        let merged = apply_patch(table_def, &row.doc, &fields)?;
        self.do_update(table_def, table_name, id, merged)?;
        Ok(())
    }

    /// Replaces an existing doc whole, bumping `_version`. Ports `doReplace`
    /// (`ts-client/src/in_memory.ts:826-836`) with the unique-index check
    /// threaded in before the write (TS calls `checkUniqueIndexes` with the
    /// stored replacement doc and `excludeId = row.id`). A soft-deleted row is
    /// absent to the lookup (FM-33).
    fn do_replace(
        &mut self,
        table_def: &TableDef,
        table_name: &str,
        id: &str,
        doc: &Map<String, Value>,
    ) -> Result<(), RtDbError> {
        let key = (table_name.to_string(), id.to_string());
        let prev_doc = match self.docs.get(&key) {
            Some(row) if row.deleted_at.is_none() => row.doc.clone(),
            _ => {
                return Err(RtDbError::new(
                    ErrorCode::NotFound,
                    format!("document '{id}' not found"),
                ));
            }
        };
        // Replace gets defaults but never a ttl stamp — the server stamps
        // `default_duration_ms` on insert only. The updatedAt stamp DOES
        // apply (replace is a version-bumping write) and runs after
        // defaults, so it wins on the same field (server replace order).
        let stamped = apply_defaults(table_def, doc);
        let mut stamped = stamp_updated_at(table_def, &stamped, (self.now)());
        // FM-37: a replace validates as a complete document, so the
        // server-stamped counter must be present — an omitted/null field is
        // filled from the stored row (preserved, never re-assigned), and a
        // supplied value must equal the stored one (round-trip replace
        // works, changing the counter does not). A stored doc that PREDATES
        // the declaration (written before the counter was added) has no
        // value to preserve, so a replace may set one — first-set, like an
        // insert. Mirrors the preserve-or-reject in server
        // `txn::do_replace`; runs after defaults exactly as there.
        if let Some(auto) = &table_def.auto_increment_field
            && let Some(prev) = prev_doc.get(auto)
        {
            match stamped.get(auto) {
                None | Some(Value::Null) => {
                    stamped.insert(auto.clone(), prev.clone());
                }
                Some(value) if value != prev => {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        format!("autoIncrementField '{auto}' cannot be changed"),
                    ));
                }
                Some(_) => {}
            }
        }
        let doc_value = Value::Object(stamped);
        validate_doc(table_def, &doc_value)?;
        let stored = strip_unset_optionals(table_def, &doc_value);
        self.check_unique_indexes(table_def, table_name, &stored, Some(id))?;
        if let Some(row) = self.docs.get_mut(&key) {
            row.doc = stored;
            row.version += 1;
        }
        Ok(())
    }

    /// Deletes a doc by id (hard). Only used by the TTL reaper's bulk branch
    /// now; interactive deletes go through [`Self::delete_row_cascade`]
    /// (FM-33). Ports `doDelete` (`ts-client/src/in_memory.ts:838-842`).
    fn do_delete(&mut self, table_name: &str, id: &str) -> Result<(), RtDbError> {
        let key = (table_name.to_string(), id.to_string());
        self.docs.remove(&key).ok_or_else(|| {
            RtDbError::new(ErrorCode::NotFound, format!("document '{id}' not found"))
        })?;
        Ok(())
    }

    /// Stamps the row soft-deleted (FM-33): sets `deleted_at` and bumps
    /// `_version` (a stale client copy fails OCC against the stamped row).
    /// An absent OR already-stamped row is `NotFound`, matching the
    /// hard-delete miss — deleting an already-soft-deleted row is `NotFound`,
    /// exactly like deleting a physically absent one. Mirrors
    /// `server/src/txn.rs::do_soft_delete`.
    fn do_soft_delete(&mut self, table_name: &str, id: &str) -> Result<(), RtDbError> {
        let key = (table_name.to_string(), id.to_string());
        let Some(row) = self.docs.get_mut(&key) else {
            return Err(RtDbError::new(
                ErrorCode::NotFound,
                format!("document '{id}' not found"),
            ));
        };
        if row.deleted_at.is_some() {
            return Err(RtDbError::new(
                ErrorCode::NotFound,
                format!("document '{id}' not found"),
            ));
        }
        row.deleted_at = Some((self.now)());
        row.version += 1;
        Ok(())
    }

    /// `undelete` step executor (FM-33): restore a soft-deleted row — clear
    /// `deleted_at`, bump `_version` (a stale client copy fails OCC against
    /// the restored row). `NotFound` when absent; idempotent `Ok` when the
    /// row is present and already live; `BadRequest` on a table that does not
    /// declare `softDelete`. The doc body is untouched (a soft delete never
    /// modified it), so the restored row re-appears byte-identical. Restoring
    /// re-enters the partial unique indexes (`WHERE deleted_at IS NULL`), so
    /// a live duplicate holding the key conflicts — the server surfaces the
    /// index violation as `conflict` on the restoring UPDATE; the harness
    /// runs the same [`Self::check_unique_indexes`] pass the write paths use.
    /// Mirrors `server/src/txn.rs::step_undelete`.
    fn do_undelete(
        &mut self,
        table_def: &TableDef,
        table: &str,
        id: &str,
    ) -> Result<(), RtDbError> {
        if !table_def.soft_delete {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                format!("table '{table}' does not declare softDelete"),
            ));
        }
        let key = (table.to_string(), id.to_string());
        let Some(row) = self.docs.get(&key) else {
            return Err(RtDbError::new(
                ErrorCode::NotFound,
                format!("document '{id}' not found"),
            ));
        };
        if row.deleted_at.is_none() {
            // Idempotent: restoring a live row changes nothing.
            return Ok(());
        }
        // Clone out of the borrow so the unique check (a &self method) can
        // run before the restore mutates the row.
        let restored_doc = row.doc.clone();
        self.check_unique_indexes(table_def, table, &restored_doc, Some(id))?;
        if let Some(row) = self.docs.get_mut(&key) {
            row.deleted_at = None;
            row.version += 1;
        }
        Ok(())
    }

    /// Deletes row `id` of `table_name` expanding the app-level `onDelete`
    /// rules (FM-33), mirroring `server/src/txn.rs::delete_row_cascade`:
    ///
    /// - `softDelete` table (unless `force_hard`): the row is STAMPED, not
    ///   removed, and the recursion stops — nothing past a stamped row is
    ///   touched. Soft delete is never itself a cascade trigger.
    /// - Children first, parent last, walking every schema table field
    ///   declaring an `onDelete` action referencing this table (deterministic
    ///   BTreeMap order): `restrict` conflicts on the first live child
    ///   (naming `table.field`); `cascade` recurses per live child; `setNull`
    ///   patches `{field: null}` per live child (the key is REMOVED from the
    ///   doc body — `apply_patch`'s unset semantics — and `_version` bumps).
    /// - `visited` guards cycles (self- and mutual-reference) and lets a
    ///   `deleteByQuery` step skip rows an earlier row's cascade already
    ///   removed.
    /// - `cascade_rows` is the shared per-initiating-step budget
    ///   ([`MAX_CASCADE_ROWS`]): every stamped/deleted/nulled row plus each
    ///   initiator counts; over-budget is a `conflict`, so the txn rolls
    ///   back atomically (via the harness's snapshot/restore path).
    /// - `force_hard` (reaper) physically removes rows even on `softDelete`
    ///   tables and propagates through the recursion.
    /// - `touched` accumulates every table written, for the notify fan-out.
    fn delete_row_cascade(
        &mut self,
        table_name: &str,
        id: &str,
        visited: &mut HashSet<(String, String)>,
        cascade_rows: &mut usize,
        force_hard: bool,
        touched: &mut Vec<String>,
    ) -> Result<(), RtDbError> {
        let table_def = self.require_table(table_name)?.clone();
        if !visited.insert((table_name.to_string(), id.to_string())) {
            return Ok(());
        }
        if *cascade_rows >= MAX_CASCADE_ROWS {
            return Err(RtDbError::new(
                ErrorCode::Conflict,
                format!("onDelete cascade exceeds the limit of {MAX_CASCADE_ROWS} rows"),
            ));
        }
        *cascade_rows += 1;

        if table_def.soft_delete && !force_hard {
            self.do_soft_delete(table_name, id)?;
            touched.push(table_name.to_string());
            return Ok(());
        }

        // Collect the child-walk plan from the schema's BTreeMap (deterministic
        // order) BEFORE mutating: iterating the schema while calling `&mut
        // self` helpers would conflict borrows. Each entry carries the child
        // table def so the loop body never re-looks it up.
        let schema = self
            .schema
            .clone()
            .ok_or_else(|| RtDbError::internal("schema not pushed"))?;
        let mut plan: Vec<(String, TableDef, String, OnDeleteAction)> = Vec::new();
        for (child_table_name, child_table_def) in &schema.tables {
            for (field_name, field_type) in &child_table_def.fields {
                if let Some(action) = on_delete_ref(field_type, table_name) {
                    plan.push((
                        child_table_name.clone(),
                        child_table_def.clone(),
                        field_name.clone(),
                        action,
                    ));
                }
            }
        }
        for (child_table_name, child_table_def, field_name, action) in plan {
            match action {
                OnDeleteAction::Restrict => {
                    let hits = self.visible_child_ids(&child_table_name, &field_name, id, true);
                    if let Some(child_id) = hits.first() {
                        return Err(RtDbError::new(
                            ErrorCode::Conflict,
                            format!(
                                "cannot delete '{table_name}': '{child_table_name}.{field_name}' is referenced by document '{child_id}'"
                            ),
                        ));
                    }
                }
                OnDeleteAction::Cascade => {
                    let child_ids =
                        self.visible_child_ids(&child_table_name, &field_name, id, false);
                    for child_id in child_ids {
                        self.delete_row_cascade(
                            &child_table_name,
                            &child_id,
                            visited,
                            cascade_rows,
                            force_hard,
                            touched,
                        )?;
                    }
                }
                OnDeleteAction::SetNull => {
                    let child_ids =
                        self.visible_child_ids(&child_table_name, &field_name, id, false);
                    for child_id in child_ids {
                        if *cascade_rows >= MAX_CASCADE_ROWS {
                            return Err(RtDbError::new(
                                ErrorCode::Conflict,
                                format!(
                                    "onDelete cascade exceeds the limit of {MAX_CASCADE_ROWS} rows"
                                ),
                            ));
                        }
                        *cascade_rows += 1;
                        // `{field: null}` on the optional-id REMOVES the key
                        // (apply_patch's unset semantics) and bumps `_version`.
                        let mut fields = Map::new();
                        fields.insert(field_name.clone(), Value::Null);
                        self.do_patch(&child_table_def, &child_table_name, &child_id, &fields)?;
                        touched.push(child_table_name.clone());
                    }
                }
            }
        }

        self.do_delete(table_name, id)?;
        touched.push(table_name.to_string());
        Ok(())
    }

    /// Ids of live (non-soft-deleted) rows in `child_table` whose `field_name`
    /// references `parent_id` (FM-33). Soft-deleted children are invisible to
    /// every `onDelete` action. `limit_one` fetches a single hit (the
    /// `restrict` existence probe); otherwise the fetch is capped at the
    /// cascade row budget plus one (bounding memory on a pathological fan-out
    /// without ever dropping a row that could still be processed within
    /// budget — processing past the budget conflicts first). Mirrors
    /// `server/src/txn.rs::visible_child_ids`.
    fn visible_child_ids(
        &self,
        child_table: &str,
        field: &str,
        parent_id: &str,
        limit_one: bool,
    ) -> Vec<String> {
        let cap = if limit_one { 1 } else { MAX_CASCADE_ROWS + 1 };
        self.docs
            .iter()
            .filter(|((t, _), _)| t == child_table)
            .filter(|(_, row)| row.deleted_at.is_none())
            .filter(
                |(_, row)| matches!(row.doc.get(field), Some(Value::String(s)) if s == parent_id),
            )
            .take(cap)
            .map(|(_, row)| row.id.clone())
            .collect()
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
        validate_filter(filter, table_def)?;
        let limit = limit_opt
            .unwrap_or(MAX_BY_QUERY_ROWS)
            .min(MAX_BY_QUERY_ROWS);
        let mut matching: Vec<(i64, String)> = self
            .docs
            .iter()
            .filter(|((t, _), _)| t == table)
            // FM-33: a soft-deleted row is absent to every scan (the server's
            // compile_scan_where composes the same live-only predicate).
            .filter(|(_, row)| row.deleted_at.is_none())
            .filter(|(_, row)| matches_filter(filter, &row.doc, &table_def.fields))
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
    /// `onDelete`-aware path as a per-id `Delete` — `delete_row_cascade`
    /// unconditionally, whose soft branch stamps a `softDelete` table's row
    /// without walking children. `visited` and the row budget are shared
    /// across the whole step — a row already hard-deleted by an earlier row's
    /// cascade is skipped (not a NotFound abort), and one budget bounds every
    /// cascade the step starts. Returns the per-step result plus every table
    /// written (for the notify fan-out). Mirrors
    /// `server/src/txn.rs::step_delete_by_query` (FM-33).
    fn delete_by_query(
        &mut self,
        table: &str,
        filter: &FilterExpr,
        limit: Option<u32>,
    ) -> Result<((u32, bool), Vec<String>), RtDbError> {
        let table_def = self.require_table(table)?.clone();
        let (ids, truncated) = self.scan_ids_by_filter(&table_def, table, filter, limit)?;
        let deleted = ids.len() as u32;
        let mut touched = Vec::new();
        let mut visited = HashSet::new();
        let mut cascade_rows = 0usize;
        for id in &ids {
            self.delete_row_cascade(
                table,
                id,
                &mut visited,
                &mut cascade_rows,
                false,
                &mut touched,
            )?;
        }
        Ok(((deleted, truncated), touched))
    }

    /// Asserts a doc's current `_version` matches `expected`. Ports
    /// `doExpectVersion` (`ts-client/src/in_memory.ts:844-852`). A soft-deleted
    /// row is absent — `NotFound`, the same silent-miss as the server's
    /// live-only lookup (FM-33).
    fn do_expect_version(
        &self,
        table_name: &str,
        id: &str,
        expected: i64,
    ) -> Result<(), RtDbError> {
        let key = (table_name.to_string(), id.to_string());
        let row = self
            .docs
            .get(&key)
            .filter(|row| row.deleted_at.is_none())
            .ok_or_else(|| {
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
                && !eval_filter_expr(pred, candidate_doc, &table_def.fields)
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
                // FM-33: soft-deleted rows are excluded from unique indexes
                // (the server's partial predicate `AND "deleted_at" IS NULL`),
                // so the same key is re-insertable while soft-deleted.
                if row.deleted_at.is_some() {
                    continue;
                }
                if let Some(pred) = &index.r#where
                    && !eval_filter_expr(pred, &row.doc, &table_def.fields)
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
            // FM-33: soft-deleted rows are absent to `expectAbsent` and
            // `upsert` — upserting a soft-deleted key inserts a fresh row, and
            // expectAbsent passes.
            if row.deleted_at.is_some() {
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
            let initial_canon = diff_canonical(&initial, &query);
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
            let next_canon = diff_canonical(&next, &sub.query);
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
    // `dueAt`-driven re-arm cadence. One-shots catch up if past due (fire
    // once even if `due_at < now`); crons step by `CRON_STEP_MS` and
    // intervals by their `everyMs` — both skip missed windows (re-arm to the
    // next interval, never fire N times for N missed windows).

    /// Stores `txn` scheduled for `when` and returns its id. Cron validation
    /// is deferred to the live server; the harness accepts any expression,
    /// but an interval `everyMs` is validated here (positive and at most
    /// [`MAX_EVERY_MS`], mirroring the server's `resolve_when`). Ports
    /// `schedule` (`ts-client/src/in_memory.ts:600-617`).
    pub fn schedule(&mut self, txn: Transaction, when: ScheduleWhen) -> Result<String, RtDbError> {
        let every_ms = match &when {
            ScheduleWhen::Interval { every_ms } => {
                if *every_ms <= 0 {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        "everyMs must be positive".to_string(),
                    ));
                }
                if *every_ms > MAX_EVERY_MS {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        format!("everyMs must be at most {MAX_EVERY_MS}"),
                    ));
                }
                Some(*every_ms)
            }
            _ => None,
        };
        let id = self.new_id();
        let now = (self.now)();
        let kind = match &when {
            ScheduleWhen::Cron { .. } => ScheduleKind::Cron,
            ScheduleWhen::Interval { .. } => ScheduleKind::Interval,
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
            every_ms,
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

    /// Sets a paused schedule's status back to `Pending`, shifting an interval
    /// job's due time one full interval from now (windows elapsed while
    /// paused are skipped, never backfilled — mirrors the server's
    /// `set_paused`; cron recompute is deferred to the server and one-shots
    /// keep their due_at). NOT_FOUND if no such id. Ports `resumeSchedule`
    /// (`ts-client/src/in_memory.ts:629-631`).
    pub fn resume_schedule(&mut self, id: &str) -> Result<(), RtDbError> {
        let now = (self.now)();
        let job = self
            .schedules
            .iter_mut()
            .find(|j| j.id == id)
            .ok_or_else(|| {
                RtDbError::new(ErrorCode::NotFound, format!("schedule '{id}' not found"))
            })?;
        job.status = ScheduleStatus::Pending;
        if job.kind == ScheduleKind::Interval
            && let Some(ms) = job.every_ms
        {
            job.due_at = now + ms;
        }
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
    /// re-armed by `CRON_STEP_MS` and intervals by their `everyMs`. A job
    /// whose txn fails is marked `Error` but
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
            let every_ms = job.every_ms;
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
                            // Interval re-arms from each actual fire time (cron
                            // parity: windows missed during the fire's latency
                            // are skipped, not backfilled).
                            ScheduleKind::Interval => match every_ms {
                                Some(ms) => {
                                    j.due_at = now + ms;
                                    j.status = ScheduleStatus::Pending;
                                }
                                None => {
                                    j.status = ScheduleStatus::Error;
                                    j.last_error = Some("interval job missing everyMs".to_string());
                                }
                            },
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
                        if kind == ScheduleKind::Interval
                            && let Some(ms) = every_ms
                        {
                            j.due_at = now + ms;
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
    ///
    /// FM-33: when some table declares an `onDelete` field referencing an
    /// expired table's, a bulk remove would strand (cascade/setNull) or ignore
    /// (restrict) the children — so each expired row reaps through
    /// [`Self::delete_row_cascade`] with `force_hard = true` (TTL expiry is a
    /// real delete even on a `softDelete` table; the reaper is the collector
    /// of last resort, mirroring `server/src/committer.rs::handle_reaper`).
    /// `visited` is shared across each table's sweep (a row already cascaded
    /// by an earlier expired row's cascade is skipped, not an error) while the
    /// budget is fresh per initiating row; a per-row failure skips that row
    /// and continues (at-least-once — it retries on the next tick). Otherwise
    /// the bulk remove hard-deletes regardless of `soft_delete`, matching the
    /// server's unconditional bulk `DELETE`.
    fn reap_ttl(&mut self, now: i64) -> usize {
        let schema = match self.schema.clone() {
            Some(s) => s,
            None => return 0,
        };
        // Collect the (table, id) keys to remove — we can't mutate `self.docs`
        // while iterating it, so gather first then drain. A doc qualifies only
        // when its TTL field is a JSON number strictly less than `now`; a
        // missing or non-numeric TTL field is left alone (over-approximate
        // safely: never reap a doc that might still be live).
        let mut to_remove: Vec<(String, String)> = Vec::new();
        let mut cascade_tables: Vec<String> = Vec::new();
        for (table_name, table_def) in &self.tables {
            let Some(ttl) = &table_def.ttl else {
                continue;
            };
            if has_on_delete_children(&schema, table_name) {
                cascade_tables.push(table_name.clone());
            }
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
        // Group the removals by table: a cascade table's sweep shares ONE
        // `visited` across all of its expired rows (server
        // `committer.rs::handle_reaper` — a row already cascaded by an earlier
        // expired row's cascade is skipped, not an error), while the budget is
        // fresh per initiating row.
        let mut by_table: HashMap<String, Vec<String>> = HashMap::new();
        for (table_name, id) in to_remove {
            by_table.entry(table_name).or_default().push(id);
        }
        let mut removed = 0usize;
        let mut touched: BTreeSet<String> = BTreeSet::new();
        for (table_name, ids) in &by_table {
            if cascade_tables.contains(table_name) {
                let mut visited = HashSet::new();
                for id in ids {
                    let mut cascade_rows = 0usize;
                    let mut step_touched = Vec::new();
                    if self
                        .delete_row_cascade(
                            table_name,
                            id,
                            &mut visited,
                            &mut cascade_rows,
                            true,
                            &mut step_touched,
                        )
                        .is_ok()
                    {
                        removed += 1;
                        touched.extend(step_touched);
                    }
                    // A per-row failure skips the row (at-least-once: it
                    // remains expired and retries on the next tick).
                }
                continue;
            }
            for id in ids {
                if self
                    .docs
                    .remove(&(table_name.clone(), id.clone()))
                    .is_some()
                {
                    removed += 1;
                    touched.insert(table_name.clone());
                }
            }
        }
        if !touched.is_empty() {
            self.notify_subs(&touched);
        }
        removed
    }

    /// Initial `due_at` for a schedule's `when`, mirroring `dueAtFor`
    /// (`ts-client/src/in_memory.ts:708-717`). `afterMs` is relative to `now`,
    /// `runAt` is absolute (in the past = fire on the next tick), `cron`
    /// steps by `CRON_STEP_MS` from `now` (real cron parsing is server-side),
    /// and `interval` is one `everyMs` from `now`.
    fn due_at_for(&self, when: &ScheduleWhen, now: i64) -> i64 {
        match when {
            ScheduleWhen::AfterMs { ms } => now + ms,
            ScheduleWhen::RunAt { ms } => *ms,
            ScheduleWhen::Cron { .. } => now + CRON_STEP_MS,
            ScheduleWhen::Interval { every_ms } => now + every_ms,
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
/// Mirrors `toScheduleInfo` (`ts-client/src/in_memory.ts:727-743`): `cron`,
/// `every_ms`, and `last_error` are present only when set.
fn schedule_info(job: &ScheduledJob) -> ScheduleInfo {
    ScheduleInfo {
        id: job.id.clone(),
        kind: job.kind,
        due_at: job.due_at,
        cron: job.cron.clone(),
        every_ms: job.every_ms,
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

mod migrate;
mod presence;
mod query;
mod validate;

// Cross-module helpers (private to this module's descendants).
use migrate::detect_destructive_changes;
use query::{collect_index_key, diff_canonical, require_index};
use validate::{apply_defaults, matches_filter, stamp_ttl_default, stamp_updated_at};

// Public API re-exports (preserves `par_rt_db_client::in_memory::*`).
pub use presence::{PresenceHandle, PresenceRooms};
pub use query::{
    IndexedType, PgType, apply_aggregate, coerce_index_value, compare_index_values,
    index_column_type, merge_doc, type_tag,
};
pub use validate::{
    apply_patch, canonical, clone_value, eval_filter_expr, is_base64_string, is_hex_id,
    is_int64_string, is_plain_object, strip_unset_optionals, validate_doc, validate_filter,
    validate_value,
};

#[cfg(test)]
mod tests;
