//! Transaction execution — the write path. A `Transaction` is an ordered list of
//! steps (`Insert`/`Patch`/`Replace`/`Delete`/`ExpectVersion`/`ExpectAbsent`/
//! `Upsert`/`Undelete`) plus the predicate-driven bulk steps
//! `PatchByQuery`/`DeleteByQuery`, the scheduler control-flow steps
//! `Schedule`/`CancelSchedule`, which target the scheduler's `scheduled_txns`
//! table rather than document tables, and the workflow control-flow steps
//! `StartWorkflow`/`CancelWorkflow`, which target the per-db `workflows` table
//! the same way (FM-29).
//! Executes READ COMMITTED with no row locking and MUST run inside the
//! committer's serialized turn (never call `execute_txn` outside it). Row
//! visibility composes the client filter with `ownerField`/`collaboratorsField`/
//! `authorize` so an interactive caller touches only rows it could read;
//! `MAX_STEPS` (1024) bounds step count and `MAX_BY_QUERY_ROWS` (1000) bounds
//! rows per by-query step. Hard deletes expand app-level `onDelete` cascades
//! (`cascade`/`restrict`/`setNull`) inside the same sqlx tx, and `softDelete`
//! tables stamp `deleted_at` instead of removing the row (FM-33).

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};

use sqlx::PgPool;
use tracing::Instrument;

use crate::auth::{PrincipalCtx, authorize_table};
use crate::db::validate_db_name;
use crate::ddl::pg_schema;
use crate::dsl::StepTableExt;
use crate::error::{ErrorCode, RtDbError};
use crate::schema::SchemaDef;

// ARC-202: the wire types this module used to define live in `dsl.rs` now;
// re-exported so every `crate::txn::` path (and the integration tests'
// `rtdb_server::txn::` paths) keep resolving unchanged.
pub use crate::dsl::{EqBind, Step, Transaction, row_visible_to};
pub(crate) use crate::dsl::{eq_bind_for, eq_binds};

/// Maximum number of steps in a single transaction. A hard ceiling that bounds
/// how much work one serialized committer turn can do. Raised from 256 → 1024
/// (4× headroom) so reactive multi-writer apps can batch larger atomic units.
/// For bulk operations over many rows, prefer `PatchByQuery`/`DeleteByQuery`
/// (one step, server-side row cap) over unrolling per-id steps — that keeps a
/// txn under this limit AND avoids client-side read-all-then-patch patterns.
/// Raise further only if a measured workload genuinely needs >1024 atomic steps.
pub const MAX_STEPS: usize = 1024;

/// Recursive step count; the total tree must stay within `MAX_STEPS`. Thin
/// wrapper over [`par_rt_db_core::engine::count_steps`] (ARC-004 follow-up) —
/// see its doc comment for the full recursion semantics.
pub(crate) fn count_steps(txn: &Transaction) -> usize {
    par_rt_db_core::engine::count_steps(txn)
}

/// Hard cap on the number of rows a single `PatchByQuery`/`DeleteByQuery` step
/// may touch. A per-step safety backstop (these steps can affect many rows,
/// unlike the per-id steps which touch one): it bounds one serialized committer
/// turn and prevents a wildcard filter from sweeping an entire table. A step
/// whose match set exceeds its `limit` patches/deletes exactly `limit` and
/// reports `truncated: true` so the caller can re-run (the cron archiver
/// pattern). The step's optional `limit` is clamped to this ceiling; `None`
/// means "use this default". NOTE: a by-query step is ONE step, so the admin
/// `max_affected_docs` step-COUNT guardrail does not bound it — this const does.
const MAX_BY_QUERY_ROWS: u32 = 1000;

/// Hard cap on the number of by-query (`PatchByQuery`/`DeleteByQuery`) steps a
/// single transaction may contain. Each such step can sweep up to
/// [`MAX_BY_QUERY_ROWS`] rows, so without a step-count cap the worst-case
/// committer turn would be `MAX_STEPS * MAX_BY_QUERY_ROWS` (~1,000,000 rows) —
/// enough to stall the single-writer for a database for the duration and starve
/// every other writer/subscription on it. This const composes with
/// [`MAX_AFFECTED_ROWS_PER_TXN`] to bound the aggregate; the check runs before
/// any step executes so an over-cap txn never partially commits. SEC-104.
pub const MAX_BY_QUERY_STEPS_PER_TXN: usize = 16;

/// Aggregate worst-case affected-document budget for a single transaction: a
/// hard ceiling on [`worst_case_affected`] (per-id steps count 1 each; each
/// by-query step counts up to its `limit`, default [`MAX_BY_QUERY_ROWS`]).
/// Checked before execution so an over-budget txn commits nothing. This is the
/// single-writer stall bound — one `/api/mutate` cannot monopolize the
/// serialized committer turn. SEC-104.
pub const MAX_AFFECTED_ROWS_PER_TXN: usize = 10_000;

/// Hard cap on the number of rows one initiating delete step's `onDelete`
/// cascade may touch (FM-33) — children stamped/deleted/nulled plus the
/// initiator itself, one shared counter across every row of a `DeleteByQuery`
/// step. Cascades are not `Step`s, so neither the admin step-count cap nor
/// `MAX_AFFECTED_ROWS_PER_TXN`'s per-step estimate sees them — this const is
/// their bound (same philosophy as `MAX_BY_QUERY_ROWS`). Over → `conflict`,
/// txn aborts atomically.
pub(crate) const MAX_CASCADE_ROWS: usize = 10_000;

/// Per-statement timeout (ms) applied to every committer turn via
/// `SET LOCAL statement_timeout` inside the [`execute_txn`] transaction. Bounds
/// a pathological scan that escapes the row budget (e.g. a filter over an
/// unindexed field) so it aborts this transaction rather than stalling the
/// single-writer for the whole database. `SET LOCAL` scopes the value to this
/// transaction and reverts on commit/rollback, so it never leaks to other pool
/// users. SEC-104.
const STATEMENT_TIMEOUT_MS: u64 = 60_000;

/// The kind of write a step performed on a document. Recorded in `WriteSet.ops`
/// so downstream consumers (e.g. the activity feed) can stream what happened
/// without re-deriving it from the step list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OpKind {
    Insert,
    Patch,
    Replace,
    Delete,
    Upsert,
}

/// A single document write recorded by a transaction, with its op kind.
/// `Deserialize` (Stage 4c): a forwarded write's reply carries the owner's
/// `TxnOutcome` back to the origin replica.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DocOp {
    pub table: String,
    pub id: String,
    pub kind: OpKind,
}

/// The tables and documents a committed transaction wrote. `tables` drives
/// table-level subscription invalidation; `docs` — the `(table, id)` of every
/// written document — lets point-read subscriptions skip re-runs that don't
/// touch their document (see `subs::ReadSet`). `ops` records each write's
/// `OpKind` for the activity feed. `doc_values` carries, per written
/// `(table, id)`, the doc as it stood at txn START (`before`) and at txn END
/// (`after`); this lets `fan_out` decide whether a written doc crossed an
/// `Indexed` subscription's eq-prefix/range window, so a write to an
/// unrelated document can be skipped (see `subs::IndexedRead`).
///
/// Server-internal: the wire transports send only `TxnOutcome.results`, never
/// `write_set`. `doc_values` is `#[serde(skip)]` so it can never leak on the
/// wire even if `WriteSet` is serialized for logging/diagnostics.
///
/// `WriteSet` does NOT derive `Eq`: `serde_json::Map` is not `Eq` (JSON values
/// admit NaN-ish comparisons), and the derive is unused — no code compares
/// `WriteSet` with structural equality. `PartialEq` stays (all fields impl it).
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WriteSet {
    pub tables: BTreeSet<String>,
    pub docs: BTreeSet<(String, String)>,
    pub ops: Vec<DocOp>,
    #[serde(skip)]
    pub doc_values: BTreeMap<(String, String), DocValues>,
}

/// Per written `(table, id)`: the doc as it stood at txn START (`before`,
/// `None` when the doc was created inside this txn) and at txn END (`after`,
/// `None` when the doc was deleted inside this txn). Consumed only by
/// `subs::fan_out` to decide whether a written doc affects an `Indexed` or
/// `Ordered` subscription; never sent on the wire.
///
/// `created_at` is the row's creation timestamp — immutable after insert, and
/// NOT part of the stored `doc` body (it is a system column merged in at read
/// time). `subs::OrderedRead` needs it because every query's sort order ends
/// in `created_at, id`, so ranking a written doc against a top-N boundary is
/// impossible without it. `None` = not captured (e.g. a `Delete`, which
/// records no values), which `subs` treats as "unrankable ⇒ re-run".
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DocValues {
    pub before: Option<serde_json::Map<String, serde_json::Value>>,
    pub after: Option<serde_json::Map<String, serde_json::Value>>,
    pub created_at: Option<i64>,
}

impl WriteSet {
    /// Records that the transaction wrote document `id` in `table` as `kind`.
    pub(crate) fn touch(&mut self, table: &str, id: &str, kind: OpKind) {
        self.tables.insert(table.to_string());
        self.docs.insert((table.to_string(), id.to_string()));
        self.ops.push(DocOp {
            table: table.to_string(),
            id: id.to_string(),
            kind,
        });
    }

    /// Records the NET before/after state of `(table, id)` for `fan_out`'s
    /// `Indexed` skip decision. The net effect collapses a doc touched by
    /// several steps in one txn into one entry — the EARLIEST `before` (the
    /// first touch's pre-state) and the LATEST `after` (the last touch's
    /// post-state).
    ///
    /// `before`: `None` = this step records no before-state (used by `Delete`,
    ///   which never captures a value); `Some(None)` = the doc was created in
    ///   this txn (Insert / Upsert-insert); `Some(Some(map))` = the doc's
    ///   pre-state (Patch / Replace / Upsert-update fetched body). Applied
    ///   ONLY on the FIRST touch of `(table, id)` — the entry's `before` is
    ///   frozen at first capture so a later step in the same txn cannot
    ///   overwrite it (preserves the earliest pre-state, e.g. an Insert
    ///   followed by a Patch stays `before = None` = created).
    ///
    /// `after`: symmetric to `before` — `None` = this step records no
    ///   after-state; `Some(None)` = the doc was deleted (Delete); `Some(map)` =
    ///   the post-state. On an existing entry a `Some(_)` ALWAYS overwrites
    ///   (latest post-state wins), so a `Delete` following an earlier write of
    ///   the same id in this txn reliably clears `after` to `None`. Without
    ///   that, a stale `Some` after-state could make `fan_out` skip a `count`
    ///   subscription whose matching set shrank when the doc was removed — a
    ///   missed push.
    ///
    /// `created_at`: the row's (immutable) creation timestamp when this step
    ///   knows it, for `subs::OrderedRead`'s sort-key ranking. Recorded on the
    ///   first step that supplies it and never overwritten — every step that
    ///   supplies it supplies the same value.
    pub(crate) fn capture_doc(
        &mut self,
        table: &str,
        id: &str,
        before: Option<Option<&serde_json::Map<String, serde_json::Value>>>,
        after: Option<Option<&serde_json::Map<String, serde_json::Value>>>,
        created_at: Option<i64>,
    ) {
        let key = (table.to_string(), id.to_string());
        match self.doc_values.entry(key) {
            // Already touched this txn: `before` is frozen (earliest capture
            // wins per the spec); only `after` advances to the latest state.
            Entry::Occupied(mut e) => {
                if let Some(after_opt) = after {
                    e.get_mut().after = after_opt.cloned();
                }
                if e.get().created_at.is_none() {
                    e.get_mut().created_at = created_at;
                }
            }
            // First touch: record both `before` and `after` as given.
            Entry::Vacant(e) => {
                e.insert(DocValues {
                    before: before.and_then(|v| v.cloned()),
                    after: after.and_then(|v| v.cloned()),
                    created_at,
                });
            }
        }
    }
}

/// `Deserialize` (Stage 4c): the origin replica decodes the owner's forwarded
/// mutate outcome (results + write_set) out of the NOTIFY reply.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TxnOutcome {
    pub results: Vec<serde_json::Value>,
    pub write_set: WriteSet,
}

// Preserving id/doc/created_at/version explicitly (rather than minting new
// ones like `do_insert`) pushes this past clippy's default 7-argument
// threshold; every param is independently needed to replay a snapshot row.

/// Executes all of `txn`'s steps in one Postgres transaction; any step's
/// error aborts and rolls back everything already applied. See module docs
/// on `Step` for per-step semantics.
///
/// `owner` is the caller's per-row auth identity: `Some(uid)` stamps that user
/// as the owner on inserts into owner-gated tables and rejects mutations of
/// another user's docs with `Forbidden`; `None` bypasses both (machine tokens,
/// scheduled jobs). The check runs inside the sqlx transaction, so a
/// `Forbidden` from any step returns via `?` before `tx.commit()` and rolls
/// back the whole transaction — no partial write, no TOCTOU window.
///
/// Runs under READ COMMITTED with no row locking; correctness depends on all
/// writes for a database being serialized through the per-db committer.
/// Never call `execute_txn` from a non-committer production path.
/// Worst-case number of documents `txn` could affect, capped per by-query
/// step at [`MAX_BY_QUERY_ROWS`]. Used by [`execute_txn`]'s
/// [`MAX_AFFECTED_ROWS_PER_TXN`] budget check and the admin `max_affected_docs`
/// guardrail (admin/docs.rs, ws.rs). SEC-104. Thin wrapper over
/// [`par_rt_db_core::engine::worst_case_affected`] (ARC-004 follow-up) — see
/// its doc comment for the full estimate semantics.
pub fn worst_case_affected(txn: &Transaction) -> usize {
    par_rt_db_core::engine::worst_case_affected(txn, MAX_BY_QUERY_ROWS)
}

/// Internal signal (ARC-003) — never reaches the wire. `execute_txn` returns
/// it when the idempotency insert inside the write transaction found an
/// existing row, meaning a previous execution already committed under this
/// key; the transaction has been rolled back and nothing was double-applied.
/// `handle_mutate` matches it (via [`is_idempotency_replay`]) and replays the
/// stored result instead of surfacing an error. Matched as an `Internal`-code
/// error with this exact message, since `RtDbError` carries no private marker
/// field and a new `ErrorCode` would extend the client-visible wire enum.
pub const IDEMPOTENCY_REPLAY_MSG: &str = "idempotency key already committed";

pub fn is_idempotency_replay(err: &RtDbError) -> bool {
    err.code == ErrorCode::Internal && err.message == IDEMPOTENCY_REPLAY_MSG
}

/// ENH-047: which post-commit taps `execute_txn_with_side` has already
/// satisfied IN the write's transaction. When `audit`/`webhooks` are set, the
/// audit-log and webhook-delivery INSERTs run on the open transaction before
/// its commit (the outbox guarantee — they commit or roll back with the
/// documents) and the caller's `publish_taps` must skip them. `owner` for
/// those rows is the `PrincipalCtx`'s user id, matching what the arm's tap
/// used to pass.
#[derive(Debug, Clone, Copy)]
pub struct TxnSideWrites {
    /// Audit/webhook `source` tag — `"mutate"` / `"scheduled"` / `"workflow"`.
    pub source: &'static str,
    /// Write the audit-log rows in-transaction.
    pub audit: bool,
    /// Enqueue the webhook-delivery rows in-transaction.
    pub webhooks: bool,
}

/// Executes all of `txn`'s steps plus, when `side` requests them, the audit
/// and webhook side-writes on the SAME Postgres transaction; any step's or
/// side-write's error aborts and rolls back everything. This is the general
/// form — production committer arms pass the flags from `CommitterCtx` and
/// tell `publish_taps` to skip the satisfied taps; `execute_txn` is the
/// `side.audit = side.webhooks = false` wrapper the tests and non-tap callers
/// use. See [`execute_txn`] for the step semantics.
pub async fn execute_txn_with_side(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    txn: &Transaction,
    ctx: &PrincipalCtx,
    idem: Option<(&str, i64)>,
    side: TxnSideWrites,
) -> Result<TxnOutcome, RtDbError> {
    // ENH-018: `txn.execute` spans the write path so "the DSL is slow" vs
    // "Postgres is slow" is a distinguishable question. The step count is the
    // useful cardinal attribute (one per request shape, not per document). The
    // body runs in an instrumented `async` block because a sync `Span::enter`
    // guard is `!Send` and would poison this `Send` future across the `.await`s.
    let span = tracing::info_span!("txn.execute", db, steps = count_steps(txn));
    async {
        validate_db_name(db)?;
        // Task 5: `ctx` carries `user_id` + `email`; the row-auth helpers below use
        // only the uid, so derive the legacy `owner: Option<&str>` view once and
        // thread it unchanged — byte-identical ownerField/collaboratorsField behavior.
        let owner = ctx.user_id.as_deref();

        if count_steps(txn) > MAX_STEPS {
            return Err(RtDbError::bad_request(format!(
                "transaction exceeds maximum of {MAX_STEPS} steps  \
            (counted recursively, including scheduled txns)"
            )));
        }

        // SEC-104: compose the per-step caps into an aggregate affected-document
        // budget and a by-query step-count cap, both checked BEFORE any step
        // executes so an over-cap txn commits nothing. Commit 82650c2 introduced
        // by-query steps AND raised MAX_STEPS in the same change; without these
        // composite caps the worst-case committer turn was ~1,000,000 rows — a
        // single `/api/mutate` could stall the single-writer for a database and
        // starve every other writer/subscription on it. The by-query step cap is
        // the sharp bound; the affected-row budget is the blast-radius bound.
        let by_query_steps = txn
            .steps
            .iter()
            .filter(|s| matches!(s, Step::PatchByQuery { .. } | Step::DeleteByQuery { .. }))
            .count();
        if by_query_steps > MAX_BY_QUERY_STEPS_PER_TXN {
            return Err(RtDbError::bad_request(format!(
                "transaction has {by_query_steps} by-query steps,  \
            exceeding the limit of {MAX_BY_QUERY_STEPS_PER_TXN}"
            )));
        }
        let worst = worst_case_affected(txn);
        if worst > MAX_AFFECTED_ROWS_PER_TXN {
            return Err(RtDbError::bad_request(format!(
                "transaction could affect up to {worst} documents,  \
            exceeding the limit of {MAX_AFFECTED_ROWS_PER_TXN}"
            )));
        }

        let pg_schema_name = pg_schema(db);
        let mut results = Vec::with_capacity(txn.steps.len());
        let mut write_set = WriteSet::default();

        let mut tx = pool.begin().await?;
        // SEC-104: bound every statement in this committer turn. A pathological
        // scan (e.g. a filter over an unindexed field that escapes the row budget)
        // aborts this transaction rather than stalling the single-writer for the
        // whole database. `SET LOCAL` scopes the value to this transaction and
        // reverts on commit/rollback — it never leaks to other pool users. The
        // value is a const, never user input.
        sqlx::query(&format!(
            "SET LOCAL statement_timeout = {STATEMENT_TIMEOUT_MS}"
        ))
        .execute(&mut *tx)
        .await?;

        let mut sctx = StepCtx {
            tx: &mut tx,
            db,
            pg_schema_name: pg_schema_name.as_str(),
            schema,
            ctx,
            owner,
            write_set: &mut write_set,
            results: &mut results,
        };
        for step in &txn.steps {
            // ENH-005 Task 4: gate each step against the machine-token table
            // allowlist BEFORE any work. A scoped token cannot write a forbidden
            // table via any step variant. `tables = None` (admin/scheduled/`User`/
            // full-access machine tokens) bypasses; the gate is a pure read. Runs
            // inside the sqlx tx so a `Forbidden` returns via `?` before commit and
            // rolls back the whole transaction. The schedule control-flow steps
            // carry no table (`table() == None`); `Step::Schedule` checks its
            // NESTED steps recursively in `step_schedule`.
            if let Some(table) = step.table() {
                authorize_table(sctx.ctx, table)?;
            }
            match step {
                Step::Insert { table, doc } => step_insert(&mut sctx, table, doc).await?,
                Step::Patch { table, id, fields } => {
                    step_patch(&mut sctx, table, id, fields).await?
                }
                Step::AdjustCounter {
                    table,
                    id,
                    field,
                    delta,
                    min,
                    max,
                    expected,
                } => {
                    step_adjust_counter(
                        &mut sctx,
                        table,
                        id,
                        field,
                        *delta,
                        *min,
                        *max,
                        expected.as_ref(),
                    )
                    .await?
                }
                Step::Replace { table, id, doc } => step_replace(&mut sctx, table, id, doc).await?,
                Step::Delete { table, id } => step_delete(&mut sctx, table, id).await?,
                Step::Undelete { table, id } => step_undelete(&mut sctx, table, id).await?,
                Step::ExpectVersion { table, id, version } => {
                    step_expect_version(&mut sctx, table, id, *version).await?
                }
                Step::ExpectAbsent { table, index, eq } => {
                    step_expect_absent(&mut sctx, table, index, eq).await?
                }
                Step::Upsert {
                    table,
                    index,
                    eq,
                    insert,
                    patch,
                } => step_upsert(&mut sctx, table, index, eq, insert, patch).await?,
                Step::PatchByQuery {
                    table,
                    filter,
                    patch,
                    limit,
                } => step_patch_by_query(&mut sctx, table, filter, patch, *limit).await?,
                Step::DeleteByQuery {
                    table,
                    filter,
                    limit,
                } => step_delete_by_query(&mut sctx, table, filter, *limit).await?,
                Step::Schedule {
                    when,
                    txn,
                    external,
                } => step_schedule(&mut sctx, when, txn, external.is_some_and(|e| e)).await?,
                Step::CancelSchedule { id } => step_cancel_schedule(&mut sctx, id).await?,
                Step::StartWorkflow { spec } => step_start_workflow(&mut sctx, spec).await?,
                Step::CancelWorkflow { id } => step_cancel_workflow(&mut sctx, id).await?,
            }
        }

        // Durable change-feed stamp — INSIDE the transaction, so the documents
        // and their change rows commit (or roll back) together. This is the
        // mutate/scheduled/workflow choke point; the reaper/merge/migrate arms
        // append on their own transactions (see the change-feed design spec).
        // The tables are ensured at committer startup and db creation, so the
        // append itself never needs DDL.
        // ARC-003: record the dedup row in the SAME transaction as the writes,
        // so "committed" and "deduplicated" are atomic — a crash between the
        // write and a post-commit cache fill can no longer let a takeover
        // resubmit re-apply an already-committed mutation. A `false` return
        // means the key was already committed; rolling back here leaves the
        // first execution's result in place for the caller to replay.
        if let Some((key, ttl)) = idem {
            let inserted = crate::mutation_log::store_on(&mut tx, db, key, &results, ttl).await?;
            if !inserted {
                return Err(RtDbError::internal(IDEMPOTENCY_REPLAY_MSG));
            }
        }

        // ENH-047: the audit and webhook side-writes join the write's
        // transaction (before `change_log::append`, mirroring the previous
        // tap order) so they commit or roll back together with the documents
        // — a crash between commit and enqueue can no longer lose audit rows
        // or webhook deliveries. An insert failure propagates (`?`), rolling
        // the whole write back: the intended outbox semantics (documented in
        // docs/ARCHITECTURE.md). These are server-owned global tables ensured
        // at boot, so a failure means Postgres is unhealthy.
        if side.audit {
            crate::audit::write_audit_rows_on(&mut tx, db, owner, side.source, &write_set.ops)
                .await?;
        }
        if side.webhooks {
            crate::webhook::enqueue_for_ops_on(&mut tx, db, owner, side.source, &write_set.ops)
                .await?;
        }

        crate::change_log::append(&mut tx, pg_schema_name.as_str(), &write_set).await?;

        tx.commit().await?;
        Ok(TxnOutcome { results, write_set })
    }
    .instrument(span)
    .await
}

/// No-side-write form of [`execute_txn_with_side`]: the audit/webhook taps
/// stay post-commit at the caller. Used by the non-tap callers (tests, the
/// snapshot import) that publish nothing.
pub async fn execute_txn(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    txn: &Transaction,
    ctx: &PrincipalCtx,
    idem: Option<(&str, i64)>,
) -> Result<TxnOutcome, RtDbError> {
    execute_txn_with_side(
        pool,
        db,
        schema,
        txn,
        ctx,
        idem,
        TxnSideWrites {
            source: "",
            audit: false,
            webhooks: false,
        },
    )
    .await
}

mod auth;
mod cascade;
mod row_ops;
mod stamp;
mod steps;

pub(crate) use auth::{
    authorize_spec_tables, authorize_txn_tables, check_owner, check_owner_doc, doc_visible_to,
    step_cancel_schedule, step_cancel_workflow, step_schedule, step_start_workflow,
};
pub(crate) use cascade::{delete_row_cascade, has_on_delete_children};
pub(crate) use row_ops::{
    apply_patch, apply_update, do_expect_version, do_insert, do_patch, do_replace, do_soft_delete,
    eq_lookup, insert_snapshot_row,
};
pub(crate) use stamp::{
    apply_defaults, stamp_authorize, stamp_auto_increment, stamp_computed, stamp_owner,
    stamp_ttl_default, stamp_updated_at, verify_authorize_doc,
};
pub(crate) use steps::{
    StepCtx, step_adjust_counter, step_delete, step_delete_by_query, step_expect_absent,
    step_expect_version, step_insert, step_patch, step_patch_by_query, step_replace, step_undelete,
    step_upsert,
};
