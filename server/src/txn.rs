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
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::future::Future;
use std::pin::Pin;

use sqlx::{PgConnection, PgPool};
use tracing::Instrument;

use crate::auth::{PrincipalCtx, authorize_table};
use crate::db::{new_id, now_ms, validate_db_name};
use crate::ddl::{pg_col, pg_schema, pg_table};
use crate::dsl::{FilterExpr, filter_matches};
use crate::error::RtDbError;
use crate::scheduler;
use crate::schema::{
    FieldType, OnDeleteAction, SchemaDef, TableDef, indexed_column_type, validate_doc,
    validate_value,
};

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

/// Recursive step count: every step counts 1, a `Schedule` step adds its
/// nested txn's count on top, and a `StartWorkflow` step sums its spec's
/// step txns. The total tree must stay within `MAX_STEPS` —
/// this bounds one request body's serialized size and blocks the nesting
/// bomb (N steps each scheduling N steps). By-query caps are NOT applied to
/// nested txns here: the nested txn executes in a future committer turn and
/// `execute_txn` re-validates it fully at fire time.
pub(crate) fn count_steps(txn: &Transaction) -> usize {
    txn.steps
        .iter()
        .map(|step| match step {
            Step::Schedule { txn, .. } => 1 + count_steps(txn),
            Step::StartWorkflow { spec } => {
                1 + spec
                    .steps
                    .iter()
                    .map(|s| count_steps(&s.txn))
                    .sum::<usize>()
            }
            _ => 1,
        })
        .sum()
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
const MAX_CASCADE_ROWS: usize = 10_000;

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
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
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
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
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

#[derive(Debug, Clone, serde::Serialize)]
pub struct TxnOutcome {
    pub results: Vec<serde_json::Value>,
    pub write_set: WriteSet,
}

/// SQL bind for an indexed-column value extracted from a document, `None`
/// when the field is absent or explicitly null (stored as SQL NULL).
enum ColBind {
    Text(Option<String>),
    Num(Option<f64>),
    Bool(Option<bool>),
    I64(Option<i64>),
    /// pgvector text form `[a,b,c]` (NULL when `None`). Bound against a
    /// `$n::vector` placeholder whose column type is `vector(N)`.
    Vector(Option<String>),
}

/// The kind of an indexed column: a scalar stored in an `f_<field>` column,
/// or a vector stored in a `v_<index>` column.
enum ColumnKind {
    Scalar(FieldType),
    Vector,
}

/// One physical indexed column: its physical name (`f_<field>` or `v_<index>`),
/// the doc field its value is read from, and its kind.
struct TableColumn {
    col: String,
    field: String,
    kind: ColumnKind,
}

/// Every column a write must maintain beyond `id`/`doc`/`created_at`/`version`,
/// in a stable order: the `f_<field>` scalar columns for every btree/search
/// index field and every vector index's `filterFields`, plus one `v_<index>`
/// vector column per vector index (whose value is read from the index's single
/// vector field, not a typed `f_` column). Sorted by physical column name so
/// `do_insert`/`apply_update`/`insert_snapshot_row` emit columns and binds in
/// the same order.
fn table_columns(table: &TableDef) -> Result<Vec<TableColumn>, RtDbError> {
    use crate::ddl::pg_vector_col;

    // Scalar `f_<field>` columns: btree/search index fields + vector-index
    // filterFields. A vector index's own vector field is intentionally absent
    // here — it lives on the `v_<index>` column below.
    let mut scalar_fields: BTreeSet<String> = BTreeSet::new();
    for index in &table.indexes {
        if let Some(vec_spec) = &index.vector {
            for ff in &vec_spec.filter_fields {
                scalar_fields.insert(ff.clone());
            }
        } else {
            for field_name in &index.fields {
                scalar_fields.insert(field_name.clone());
            }
        }
    }
    let mut cols: Vec<TableColumn> = scalar_fields
        .into_iter()
        .map(|field| -> Result<TableColumn, RtDbError> {
            let ty = table.fields.get(&field).cloned().ok_or_else(|| {
                RtDbError::internal(format!("index references unknown field '{field}'"))
            })?;
            Ok(TableColumn {
                col: pg_col(&field),
                field: field.clone(),
                kind: ColumnKind::Scalar(ty),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Vector `v_<index>` columns: one per vector index, reading its vector field.
    for index in &table.indexes {
        if let Some(_vec_spec) = &index.vector {
            let field = index
                .fields
                .first()
                .cloned()
                .ok_or_else(|| RtDbError::internal("vector index missing its field"))?;
            cols.push(TableColumn {
                col: pg_vector_col(&index.name),
                field,
                kind: ColumnKind::Vector,
            });
        }
    }

    cols.sort_by(|a, b| a.col.cmp(&b.col));
    Ok(cols)
}

/// Extracts one SQL bind per `columns` entry from `doc`, shared by
/// insert/patch/upsert so every indexed column is always recomputed the
/// same way from the merged document.
fn column_binds(
    columns: &[TableColumn],
    doc: &serde_json::Map<String, serde_json::Value>,
) -> Result<Vec<ColBind>, RtDbError> {
    columns
        .iter()
        .map(|c| {
            let value = doc
                .get(&c.field)
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            column_bind_for(&c.kind, &value)
        })
        .collect()
}

fn column_bind_for(kind: &ColumnKind, value: &serde_json::Value) -> Result<ColBind, RtDbError> {
    match kind {
        ColumnKind::Scalar(ty) => scalar_bind(ty, value),
        ColumnKind::Vector => {
            if value.is_null() {
                return Ok(ColBind::Vector(None));
            }
            // Defensive only: schema validation already enforced exact length +
            // finiteness. pgvector parses the JSON-array text form `[a,b,c]`.
            Ok(ColBind::Vector(Some(value.to_string())))
        }
    }
}

/// Scalar bind for an `f_<field>` column, typed per `FieldType` (`Optional`
/// unwrapped). `None` when the value is null (stored as SQL NULL).
fn scalar_bind(ty: &FieldType, value: &serde_json::Value) -> Result<ColBind, RtDbError> {
    let (pg_type, _nullable) = indexed_column_type(ty)?;
    if value.is_null() {
        return match pg_type {
            "text" => Ok(ColBind::Text(None)),
            "double precision" => Ok(ColBind::Num(None)),
            "bigint" => Ok(ColBind::I64(None)),
            "boolean" => Ok(ColBind::Bool(None)),
            other => Err(RtDbError::internal(format!("unexpected pg type '{other}'"))),
        };
    }
    match pg_type {
        "text" => value
            .as_str()
            .map(|s| ColBind::Text(Some(s.to_string())))
            .ok_or_else(|| RtDbError::internal("expected string value for indexed column")),
        "double precision" => value
            .as_f64()
            .map(|n| ColBind::Num(Some(n)))
            .ok_or_else(|| RtDbError::internal("expected numeric value for indexed column")),
        "bigint" => value
            .as_str()
            .and_then(|s| s.parse::<i64>().ok())
            .map(|n| ColBind::I64(Some(n)))
            .ok_or_else(|| RtDbError::internal("expected int64 string value for indexed column")),
        "boolean" => value
            .as_bool()
            .map(|b| ColBind::Bool(Some(b)))
            .ok_or_else(|| RtDbError::internal("expected boolean value for indexed column")),
        other => Err(RtDbError::internal(format!("unexpected pg type '{other}'"))),
    }
}

/// Applies a patch's `fields` onto `doc`: unknown fields are a
/// `SchemaViolation`; an explicit `null` on an `Optional` field whose inner
/// type doesn't itself accept null removes the field; otherwise the value
/// is validated and set. The merged result is re-validated as a whole doc.
fn apply_patch(
    table: &TableDef,
    mut doc: serde_json::Map<String, serde_json::Value>,
    fields: &serde_json::Map<String, serde_json::Value>,
) -> Result<serde_json::Map<String, serde_json::Value>, RtDbError> {
    for (field_name, field_value) in fields {
        let field_type = table
            .fields
            .get(field_name)
            .ok_or_else(|| RtDbError::schema(format!("unknown field '{field_name}'")))?;

        if field_value.is_null()
            && let FieldType::Optional { inner } = field_type
            && !validate_value(inner, &serde_json::Value::Null)
        {
            doc.remove(field_name);
            continue;
        }

        if !validate_value(field_type, field_value) {
            return Err(RtDbError::schema(format!(
                "field '{field_name}' has an invalid value"
            )));
        }
        doc.insert(field_name.clone(), field_value.clone());
    }

    validate_doc(table, &doc)?;
    Ok(doc)
}

/// Strips keys whose value is an explicit JSON `null` for an `Optional`
/// field whose inner type does not itself accept `null`, matching
/// `apply_patch`'s treatment of a patch null as "unset" rather than a stored
/// null — so an inserted document and a patched-then-nulled document end up
/// in the same shape (key absent), not two different representations of the
/// same logical state.
fn strip_unset_optionals(
    table: &TableDef,
    mut doc: serde_json::Map<String, serde_json::Value>,
) -> serde_json::Map<String, serde_json::Value> {
    doc.retain(|field_name, value| {
        if !value.is_null() {
            return true;
        }
        !matches!(
            table.fields.get(field_name),
            Some(FieldType::Optional { inner }) if !validate_value(inner, &serde_json::Value::Null)
        )
    });
    doc
}

/// Inserts a new row for `doc` (already validated by the caller's schema
/// lookup): `doc` jsonb plus every indexed-field column, `created_at =
/// now_ms()`, `version` defaulting to 1. Returns the generated id, the
/// stamped+stripped doc as stored, and the stamped `created_at` (the caller
/// records all three on `WriteSet.doc_values` so `fan_out` can window-check
/// and rank the after-state).
async fn do_insert(
    conn: &mut PgConnection,
    pg_schema_name: &str,
    table_def: &TableDef,
    table_name: &str,
    doc: &serde_json::Map<String, serde_json::Value>,
) -> Result<(String, serde_json::Map<String, serde_json::Value>, i64), RtDbError> {
    validate_doc(table_def, doc)?;
    let stripped = strip_unset_optionals(table_def, doc.clone());

    let id = new_id();
    let created_at = now_ms();
    let columns = table_columns(table_def)?;
    let binds = column_binds(&columns, &stripped)?;

    let table_ident = pg_table(table_name);
    let mut col_names = vec![
        "\"id\"".to_string(),
        "\"doc\"".to_string(),
        "\"created_at\"".to_string(),
    ];
    let mut placeholders = vec!["$1".to_string(), "$2".to_string(), "$3".to_string()];
    let mut idx = 3usize;
    for c in &columns {
        idx += 1;
        col_names.push(format!("\"{}\"", c.col));
        let ph = match c.kind {
            ColumnKind::Vector => format!("${idx}::vector"),
            ColumnKind::Scalar(_) => format!("${idx}"),
        };
        placeholders.push(ph);
    }

    let sql = format!(
        "INSERT INTO \"{pg_schema_name}\".\"{table_ident}\" ({}) VALUES ({})",
        col_names.join(", "),
        placeholders.join(", ")
    );

    let doc_value = serde_json::Value::Object(stripped.clone());
    let mut query = sqlx::query(&sql)
        .bind(id.clone())
        .bind(doc_value)
        .bind(created_at);
    for bind in binds {
        query = match bind {
            ColBind::Text(v) => query.bind(v),
            ColBind::Num(v) => query.bind(v),
            ColBind::Bool(v) => query.bind(v),
            ColBind::I64(v) => query.bind(v),
            ColBind::Vector(v) => query.bind(v),
        };
    }
    query.execute(&mut *conn).await?;
    Ok((id, stripped, created_at))
}

/// Updates an existing row's `doc`, every indexed-field column recomputed
/// from `merged`, and bumps `version`. Shared by the `Patch` step and
/// `Upsert`'s patch path.
pub(crate) async fn apply_update(
    conn: &mut PgConnection,
    pg_schema_name: &str,
    table_def: &TableDef,
    table_name: &str,
    id: &str,
    merged: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), RtDbError> {
    let table_ident = pg_table(table_name);
    let columns = table_columns(table_def)?;
    let binds = column_binds(&columns, merged)?;

    let mut set_clauses = vec![
        "\"doc\" = $1".to_string(),
        "\"version\" = \"version\" + 1".to_string(),
    ];
    let mut idx = 2usize;
    for c in &columns {
        let cast = match c.kind {
            ColumnKind::Vector => "::vector",
            ColumnKind::Scalar(_) => "",
        };
        set_clauses.push(format!("\"{}\" = ${idx}{cast}", c.col));
        idx += 1;
    }
    let id_placeholder = idx;

    let sql = format!(
        "UPDATE \"{pg_schema_name}\".\"{table_ident}\" SET {} WHERE \"id\" = ${id_placeholder}",
        set_clauses.join(", ")
    );

    let doc_value = serde_json::Value::Object(merged.clone());
    let mut query = sqlx::query(&sql).bind(doc_value);
    for bind in binds {
        query = match bind {
            ColBind::Text(v) => query.bind(v),
            ColBind::Num(v) => query.bind(v),
            ColBind::Bool(v) => query.bind(v),
            ColBind::I64(v) => query.bind(v),
            ColBind::Vector(v) => query.bind(v),
        };
    }
    query = query.bind(id.to_string());
    query.execute(&mut *conn).await?;
    Ok(())
}

/// Fetches the current doc by id (`NotFound` if missing), merges `fields`
/// onto it via `apply_patch`, and applies the update. Returns the pre-merge
/// doc (for `WriteSet.doc_values`'s `before`), the merged doc (for `after`),
/// and the row's `created_at` (for `subs::OrderedRead`'s sort-key ranking).
async fn do_patch(
    conn: &mut PgConnection,
    pg_schema_name: &str,
    table_def: &TableDef,
    table_name: &str,
    id: &str,
    fields: &serde_json::Map<String, serde_json::Value>,
) -> Result<
    (
        serde_json::Map<String, serde_json::Value>,
        serde_json::Map<String, serde_json::Value>,
        i64,
    ),
    RtDbError,
> {
    let table_ident = pg_table(table_name);
    // FM-33: a soft-deleted row is absent to every write lookup.
    let live_only = if table_def.soft_delete {
        " AND \"deleted_at\" IS NULL"
    } else {
        ""
    };
    let row: Option<(serde_json::Value, i64)> = sqlx::query_as(&format!(
        "SELECT \"doc\", \"created_at\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE \"id\" = $1{live_only}"
    ))
    .bind(id)
    .fetch_optional(&mut *conn)
    .await?;

    let (doc_value, created_at) =
        row.ok_or_else(|| RtDbError::not_found(format!("document '{id}' not found")))?;
    let doc = match doc_value {
        serde_json::Value::Object(map) => map,
        _ => return Err(RtDbError::internal("stored doc is not a JSON object")),
    };

    // Snapshot the pre-merge body for `fan_out`'s `before` capture: the merge
    // below consumes `doc`, and the earliest pre-state across the txn is what
    // determines whether an `Indexed` window-membership change occurred.
    let pre_doc = doc.clone();
    let merged = apply_patch(table_def, doc, fields)?;
    apply_update(conn, pg_schema_name, table_def, table_name, id, &merged).await?;
    Ok((pre_doc, merged, created_at))
}

/// Fetches the current doc (`NotFound` if missing), then fully replaces its
/// `doc` with `new_doc` — validated as a complete document (like `Insert`),
/// not merged like `Patch` — recomputing every indexed column and bumping
/// `version` via the shared `apply_update`. Widened from a bare existence
/// `SELECT "id"` to `SELECT "doc", "created_at"` so the pre-replace body is
/// available for `WriteSet.doc_values`'s `before` capture and the row is
/// rankable for `subs::OrderedRead`. Returns the old doc (for `before`), the
/// new stripped doc (for `after`), and `created_at`.
async fn do_replace(
    conn: &mut PgConnection,
    pg_schema_name: &str,
    table_def: &TableDef,
    table_name: &str,
    id: &str,
    new_doc: &serde_json::Map<String, serde_json::Value>,
) -> Result<
    (
        serde_json::Map<String, serde_json::Value>,
        serde_json::Map<String, serde_json::Value>,
        i64,
    ),
    RtDbError,
> {
    let table_ident = pg_table(table_name);
    // FM-33: a soft-deleted row is absent to every write lookup.
    let live_only = if table_def.soft_delete {
        " AND \"deleted_at\" IS NULL"
    } else {
        ""
    };
    let row: Option<(serde_json::Value, i64)> = sqlx::query_as(&format!(
        "SELECT \"doc\", \"created_at\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE \"id\" = $1{live_only}"
    ))
    .bind(id)
    .fetch_optional(&mut *conn)
    .await?;
    let (old_doc_value, created_at) =
        row.ok_or_else(|| RtDbError::not_found(format!("document '{id}' not found")))?;
    let old_doc = match old_doc_value {
        serde_json::Value::Object(map) => map,
        _ => return Err(RtDbError::internal("stored doc is not a JSON object")),
    };

    validate_doc(table_def, new_doc)?;
    let new_doc = strip_unset_optionals(table_def, new_doc.clone());
    apply_update(conn, pg_schema_name, table_def, table_name, id, &new_doc).await?;
    Ok((old_doc, new_doc, created_at))
}

/// Inserts a row with an explicit id/created_at/version, preserving a document's
/// original identity and history instead of minting new ones like `do_insert`.
/// Indexed columns are recomputed from `doc` the same way `do_insert` does. Used
/// by `snapshot::import_database` to replay an exported row exactly.
// Preserving id/doc/created_at/version explicitly (rather than minting new
// ones like `do_insert`) pushes this past clippy's default 7-argument
// threshold; every param is independently needed to replay a snapshot row.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn insert_snapshot_row(
    conn: &mut PgConnection,
    pg_schema_name: &str,
    table_def: &TableDef,
    table_name: &str,
    id: &str,
    doc: &serde_json::Map<String, serde_json::Value>,
    created_at: i64,
    version: i64,
) -> Result<(), RtDbError> {
    validate_doc(table_def, doc)?;
    let doc = strip_unset_optionals(table_def, doc.clone());
    let columns = table_columns(table_def)?;
    let binds = column_binds(&columns, &doc)?;

    let table_ident = pg_table(table_name);
    let mut col_names = vec![
        "\"id\"".to_string(),
        "\"doc\"".to_string(),
        "\"created_at\"".to_string(),
        "\"version\"".to_string(),
    ];
    let mut placeholders = vec![
        "$1".to_string(),
        "$2".to_string(),
        "$3".to_string(),
        "$4".to_string(),
    ];
    let mut idx = 4usize;
    for c in &columns {
        idx += 1;
        col_names.push(format!("\"{}\"", c.col));
        let ph = match c.kind {
            ColumnKind::Vector => format!("${idx}::vector"),
            ColumnKind::Scalar(_) => format!("${idx}"),
        };
        placeholders.push(ph);
    }

    let sql = format!(
        "INSERT INTO \"{pg_schema_name}\".\"{table_ident}\" ({}) VALUES ({})",
        col_names.join(", "),
        placeholders.join(", ")
    );

    let doc_value = serde_json::Value::Object(doc);
    let mut query = sqlx::query(&sql)
        .bind(id.to_string())
        .bind(doc_value)
        .bind(created_at)
        .bind(version);
    for bind in binds {
        query = match bind {
            ColBind::Text(v) => query.bind(v),
            ColBind::Num(v) => query.bind(v),
            ColBind::Bool(v) => query.bind(v),
            ColBind::I64(v) => query.bind(v),
            ColBind::Vector(v) => query.bind(v),
        };
    }
    query.execute(&mut *conn).await?;
    Ok(())
}

/// Stamps the row `id` soft-deleted (FM-33): a live-row-guarded `UPDATE`
/// setting `deleted_at = now()` and bumping `version` (a stale client copy
/// fails OCC against the stamped row). 0 rows ⇒ `NotFound`, matching the
/// hard-delete miss — deleting an already-soft-deleted row is `NotFound`,
/// exactly like deleting a physically absent one. Callers guarantee the table
/// declares `softDelete` (the `Delete`/`DeleteByQuery` rows on one, and
/// `delete_row_cascade`'s stamp branch); hard deletes run inline in
/// `delete_row_cascade`.
async fn do_soft_delete(
    conn: &mut PgConnection,
    pg_schema_name: &str,
    table_name: &str,
    id: &str,
) -> Result<(), RtDbError> {
    let table_ident = pg_table(table_name);
    let result = sqlx::query(&format!(
        "UPDATE \"{pg_schema_name}\".\"{table_ident}\" \
         SET \"deleted_at\" = now(), \"version\" = \"version\" + 1 \
         WHERE \"id\" = $1 AND \"deleted_at\" IS NULL"
    ))
    .bind(id)
    .execute(&mut *conn)
    .await?;
    if result.rows_affected() == 0 {
        return Err(RtDbError::not_found(format!("document '{id}' not found")));
    }
    Ok(())
}

async fn do_expect_version(
    conn: &mut PgConnection,
    pg_schema_name: &str,
    table_def: &TableDef,
    table_name: &str,
    id: &str,
    expected: i64,
    ctx: &PrincipalCtx,
) -> Result<(), RtDbError> {
    let table_ident = pg_table(table_name);
    // FM-33: a soft-deleted row is absent — ExpectVersion on one is NotFound,
    // the same silent-miss as a non-visible row below.
    let live_only = if table_def.soft_delete {
        " AND \"deleted_at\" IS NULL"
    } else {
        ""
    };
    let row: Option<(i64, serde_json::Value)> = sqlx::query_as(&format!(
        "SELECT \"version\", \"doc\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE \"id\" = $1{live_only}"
    ))
    .bind(id)
    .fetch_optional(&mut *conn)
    .await?;
    let Some((actual, doc)) = row else {
        return Err(RtDbError::not_found(format!("document '{id}' not found")));
    };
    // Side-channel closure: a doc the caller cannot see is indistinguishable
    // from absent — same not_found outcome, so no version is ever leaked.
    if !doc_visible_to(&doc, table_def, ctx) {
        return Err(RtDbError::not_found(format!("document '{id}' not found")));
    }
    if actual != expected {
        return Err(RtDbError::precondition(format!(
            "version mismatch: expected {expected}, actual {actual}"
        )));
    }
    Ok(())
}

/// Looks up rows matching `eq` on `index` (full arity required: a
/// `BadRequest` otherwise), returning `(id, doc, created_at)` triples. Shared
/// by `ExpectAbsent` (existence only) and `Upsert` (whose update branch
/// records all three on `WriteSet.doc_values`).
async fn eq_lookup(
    conn: &mut PgConnection,
    pg_schema_name: &str,
    table_def: &TableDef,
    table_name: &str,
    index_name: &str,
    eq: &[serde_json::Value],
) -> Result<Vec<(String, serde_json::Value, i64)>, RtDbError> {
    let index_def = table_def.index(index_name)?;
    if eq.len() != index_def.fields.len() {
        return Err(RtDbError::bad_request(format!(
            "index '{index_name}' expects {} eq value(s), got {}",
            index_def.fields.len(),
            eq.len()
        )));
    }
    let binds = eq_binds(table_def, index_def, eq)?;

    let table_ident = pg_table(table_name);
    let mut conditions: Vec<String> = index_def
        .fields
        .iter()
        .enumerate()
        .map(|(i, field_name)| format!("\"{}\" = ${}", pg_col(field_name), i + 1))
        .collect();
    // FM-33: soft-deleted rows are absent to `ExpectAbsent` and `Upsert` —
    // upserting a soft-deleted key inserts a fresh row (the unique-index
    // partial predicate makes that conflict-free), and ExpectAbsent passes.
    // Literal, so the `$n` numbering above is unaffected.
    if table_def.soft_delete {
        conditions.push("\"deleted_at\" IS NULL".to_string());
    }
    let sql = format!(
        "SELECT \"id\", \"doc\", \"created_at\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE {}",
        conditions.join(" AND ")
    );

    let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64)>(&sql);
    for bind in binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
            EqBind::I64(v) => query.bind(v),
        };
    }
    let rows = query.fetch_all(&mut *conn).await?;
    Ok(rows)
}

/// Forces `doc[owner_field] = owner` for owner-gated tables when the caller is
/// a user, overwriting any client-supplied value. Bypass callers and
/// non-owner tables leave `doc` unchanged.
fn stamp_owner(
    table_def: &TableDef,
    mut doc: serde_json::Map<String, serde_json::Value>,
    owner: Option<&str>,
) -> serde_json::Map<String, serde_json::Value> {
    if let (Some(field), Some(uid)) = (&table_def.owner_field, owner) {
        doc.insert(field.clone(), serde_json::Value::String(uid.to_string()));
    }
    doc
}

/// Walks `expr` collecting the `field` of every `Eq { field, value: {"$user": true} }`
/// leaf reachable through `And`/`Or`. `Not` is intentionally NOT descended: a
/// negated equality (`Not(Eq{owner,$user})`) is a prohibition, not a stampable
/// ownership, and stamping it would invert the predicate's meaning. `Contains`,
/// `Exists`, `In`, the comparison operators, `Neq`, `{"$email":true}`, and any
/// non-marker `Eq` value contribute nothing — only an exact `Eq{field,$user}`
/// asserts "this field IS the caller" and is therefore stampable. De-dups
/// preserving first occurrence. Empty when `expr` has no stampable leaf, in
/// which case `stamp_authorize` is a no-op and the inserted doc must satisfy
/// the predicate from client values alone (else `verify_authorize_doc`
/// rejects with `Forbidden`).
fn user_eq_fields(expr: &FilterExpr) -> Vec<String> {
    /// `true` only for the exact principal marker `{"$user": true}` — the sole
    /// value form that makes an `Eq` leaf stampable. Mirrors `resolve_value`'s
    /// marker test in `query.rs` so the two paths agree on what `$user` is.
    fn is_user_marker(v: &serde_json::Value) -> bool {
        if let serde_json::Value::Object(map) = v
            && map.len() == 1
        {
            return map.get("$user").and_then(|x| x.as_bool()) == Some(true);
        }
        false
    }
    fn walk(expr: &FilterExpr, out: &mut Vec<String>) {
        match expr {
            FilterExpr::Eq { field, value } if is_user_marker(value) => {
                if !out.iter().any(|f| f == field) {
                    out.push(field.clone());
                }
            }
            FilterExpr::And { exprs } | FilterExpr::Or { exprs } => {
                for e in exprs {
                    walk(e, out);
                }
            }
            // `Not` is NOT descended — see the doc comment. Every other leaf
            // variant is non-stampable by construction.
            _ => {}
        }
    }
    let mut out = Vec::new();
    walk(expr, &mut out);
    out
}

/// For each `Eq { field, value: {"$user": true} }` leaf reachable through
/// `And`/`Or` in `table.authorize`, force `doc[field] = ctx.user_id`,
/// overwriting any client value — unforgeable, exactly like `stamp_owner`.
/// `Not`, `Contains`, `Exists`, and non-`$user` leaves are not stampable; a
/// table without `authorize` or a bypass caller (`user_id = None`) is a no-op.
/// Call AFTER `stamp_owner`; then call `verify_authorize_doc` to reject
/// predicates the stamp could not satisfy (no `$user` leaf, or an unsatisfied
/// `And`/literal branch).
fn stamp_authorize(
    table_def: &TableDef,
    mut doc: serde_json::Map<String, serde_json::Value>,
    ctx: &PrincipalCtx,
) -> serde_json::Map<String, serde_json::Value> {
    if let (Some(expr), Some(uid)) = (&table_def.authorize, &ctx.user_id) {
        for field in user_eq_fields(expr) {
            doc.insert(field, serde_json::Value::String(uid.clone()));
        }
    }
    doc
}

/// Post-write verification for every write path on an `authorize`-gated table:
/// a user caller must leave the doc satisfying the predicate. The stamp
/// satisfies every `Eq{field,$user}` leaf, but a predicate with no stampable
/// leaf (e.g. `Eq{visibility,"public"}` or `Contains{editors,$user}`) stamps
/// nothing — the client must satisfy it from supplied values, else `Forbidden`.
/// On Patch/Replace/Upsert-update the stamp re-stamps `$user` leaves (parity
/// with `stamp_owner`), and this verify catches the residual cases (e.g. patching
/// a no-`$user`-arm `Eq{visibility,"public"}` to `"private"`). Bypass callers
/// (`user_id = None`) and tables without `authorize` are no-ops. Runs inside the
/// serialized txn so a `Forbidden` rolls back the whole transaction (same
/// atomicity guarantee as the patch/replace/delete pre-check).
fn verify_authorize_doc(
    table_def: &TableDef,
    doc: &serde_json::Map<String, serde_json::Value>,
    ctx: &PrincipalCtx,
) -> Result<(), RtDbError> {
    if let Some(authorize) = &table_def.authorize
        && ctx.user_id.is_some()
        && !filter_matches(&serde_json::Value::Object(doc.clone()), authorize, ctx)
    {
        return Err(RtDbError::forbidden(
            "write conflicts with the table's authorize predicate",
        ));
    }
    Ok(())
}

/// Stamps the TTL field at insert time when the table declares a
/// `default_duration_ms` and the document omits the field. Both insert paths
/// stamp — the `Insert` step and upsert's insert branch (a doc born via
/// upsert is born at insert time; the engines' shared insert paths agree).
/// After this, the TTL field is ordinary (patch/replace manipulate it
/// normally). See `docs/superpowers/specs/2026-08-01-document-ttl-design.md`.
fn stamp_ttl_default(
    table_def: &TableDef,
    mut doc: serde_json::Map<String, serde_json::Value>,
    now: i64,
) -> serde_json::Map<String, serde_json::Value> {
    if let Some(ttl) = &table_def.ttl
        && let Some(duration) = ttl.default_duration_ms
        && !doc.contains_key(&ttl.field)
    {
        doc.insert(ttl.field.clone(), serde_json::Value::from(now + duration));
    }
    doc
}

/// Stamps the table's `updatedAtField` (FM-36) with the current epoch-ms,
/// overwriting any client-supplied value — the same authority model as
/// `stamp_owner`. Runs on every version-bumping write path: insert, patch,
/// replace, upsert (both branches), patchByQuery, and cascade setNull. The
/// value matches the field's wire convention (`validate_value` /
/// `scalar_bind`): a JSON number on `number`, a decimal string on `int64`.
/// Snapshot replay (`insert_snapshot_row`) is not a step path and preserves
/// the stored value verbatim — import never re-stamps.
fn stamp_updated_at(
    table_def: &TableDef,
    mut doc: serde_json::Map<String, serde_json::Value>,
    now: i64,
) -> serde_json::Map<String, serde_json::Value> {
    if let Some(field) = &table_def.updated_at_field {
        let value = match table_def.fields.get(field) {
            Some(FieldType::Int64) => serde_json::Value::String(now.to_string()),
            _ => serde_json::Value::from(now),
        };
        doc.insert(field.clone(), value);
    }
    doc
}

/// Applies the table's push-time-validated `defaults` (FM-32) to a NEW
/// document: every key the doc omits is stamped from the schema. Runs after
/// `stamp_ttl_default` (a ttl default on the same field wins) and before the
/// owner/authorize stamps (server-stamped principal values win). Callers are
/// exactly the new-document paths — insert, replace, upsert-insert; `patch`
/// (and upsert-update / patchByQuery) never re-apply, so clearing an optional
/// field stays cleared.
fn apply_defaults(
    table_def: &TableDef,
    mut doc: serde_json::Map<String, serde_json::Value>,
) -> serde_json::Map<String, serde_json::Value> {
    for (field, value) in &table_def.defaults {
        if !doc.contains_key(field) {
            doc.insert(field.clone(), value.clone());
        }
    }
    doc
}

/// Ownership + authorize pre-check for patch/replace/delete: fetches the doc
/// and rejects `Forbidden` when a user caller fails EITHER gate the table
/// declares — `ownerField`/`collaboratorsField` (OR-enforced by `row_visible_to`)
/// OR the `authorize` predicate (`filter_matches`). A table may declare both;
/// both must pass. A missing doc returns `Ok` (the subsequent do_* step reports
/// `NotFound`). Bypass caller (`ctx.user_id` None — machine/admin/scheduled) and
/// tables declaring neither gate: no-op.
async fn check_owner(
    conn: &mut PgConnection,
    pg_schema_name: &str,
    table_def: &TableDef,
    table_name: &str,
    id: &str,
    ctx: &PrincipalCtx,
) -> Result<(), RtDbError> {
    let owner_uid = row_auth_enforced_uid(table_def, ctx.user_id.as_deref());
    let authorize = table_def.authorize.as_ref();
    let user_is_some = ctx.user_id.is_some();
    // Neither gate applies (bypass caller, or table declares nothing) → no-op.
    if owner_uid.is_none() && !(authorize.is_some() && user_is_some) {
        return Ok(());
    }
    let table_ident = pg_table(table_name);
    // FM-33: a soft-deleted row is absent — the gate passes (Ok, missing) and
    // the subsequent do_* step reports `NotFound`, so per-row auth never turns
    // a soft-deleted row into a `Forbidden` oracle.
    let live_only = if table_def.soft_delete {
        " AND \"deleted_at\" IS NULL"
    } else {
        ""
    };
    let row: Option<(serde_json::Value,)> = sqlx::query_as(&format!(
        "SELECT \"doc\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE \"id\" = $1{live_only}"
    ))
    .bind(id)
    .fetch_optional(&mut *conn)
    .await?;
    let Some((doc,)) = row else {
        return Ok(());
    };
    if let Some(uid) = owner_uid
        && !row_visible_to(
            &doc,
            table_def.owner_field.as_deref(),
            table_def.collaborators_field.as_deref(),
            uid,
        )
    {
        return Err(RtDbError::forbidden(format!(
            "document '{id}' is not accessible to the caller"
        )));
    }
    if let Some(authorize) = authorize
        && user_is_some
        && !filter_matches(&doc, authorize, ctx)
    {
        return Err(RtDbError::forbidden(format!(
            "document '{id}' is not accessible to the caller"
        )));
    }
    Ok(())
}

/// Ownership + authorize check on a doc already in hand (upsert update branch).
/// Same composition as `check_owner`: a user caller must pass both the
/// `ownerField`/`collaboratorsField` gate and the `authorize` predicate when
/// the table declares them. Bypass/no-gate: no-op.
fn check_owner_doc(
    table_def: &TableDef,
    doc: &serde_json::Map<String, serde_json::Value>,
    id: &str,
    ctx: &PrincipalCtx,
) -> Result<(), RtDbError> {
    let owner_uid = row_auth_enforced_uid(table_def, ctx.user_id.as_deref());
    let authorize = table_def.authorize.as_ref();
    let user_is_some = ctx.user_id.is_some();
    if owner_uid.is_none() && !(authorize.is_some() && user_is_some) {
        return Ok(());
    }
    let doc_value = serde_json::Value::Object(doc.clone());
    if let Some(uid) = owner_uid
        && !row_visible_to(
            &doc_value,
            table_def.owner_field.as_deref(),
            table_def.collaborators_field.as_deref(),
            uid,
        )
    {
        return Err(RtDbError::forbidden(format!(
            "document '{id}' is not accessible to the caller"
        )));
    }
    if let Some(authorize) = authorize
        && user_is_some
        && !filter_matches(&doc_value, authorize, ctx)
    {
        return Err(RtDbError::forbidden(format!(
            "document '{id}' is not accessible to the caller"
        )));
    }
    Ok(())
}

/// Boolean twin of [`check_owner_doc`]: `true` iff `doc` is visible to `ctx`
/// under the table's per-row gates (`ownerField`/`collaboratorsField` and/or
/// `authorize`). Used by the read-only `ExpectVersion`/`ExpectAbsent`
/// preconditions to close the existence/version side-channel — a non-visible
/// doc is treated as absent rather than rejected with `Forbidden`, because a
/// `Forbidden` would itself be a louder oracle ("exists, but not yours").
///
/// Keep in lockstep with all three txn.rs per-row gate sites: `check_owner`
/// (the async DB-fetching variant), `check_owner_doc`, and this function. Any
/// new per-row gate must land in all three. The read-scan path in `query.rs`
/// inlines the same `owner_field`/`collaborators_field`/`authorize` composition
/// (no shared helper is extracted today).
fn doc_visible_to(doc: &serde_json::Value, table_def: &TableDef, ctx: &PrincipalCtx) -> bool {
    let owner_uid = row_auth_enforced_uid(table_def, ctx.user_id.as_deref());
    let authorize = table_def.authorize.as_ref();
    let user_is_some = ctx.user_id.is_some();
    if owner_uid.is_none() && !(authorize.is_some() && user_is_some) {
        return true; // no gate applies (bypass caller, or table declares nothing)
    }
    let mut visible = true;
    if let Some(uid) = owner_uid
        && !row_visible_to(
            doc,
            table_def.owner_field.as_deref(),
            table_def.collaborators_field.as_deref(),
            uid,
        )
    {
        visible = false;
    }
    if let Some(authorize) = authorize
        && user_is_some
        && !filter_matches(doc, authorize, ctx)
    {
        visible = false;
    }
    visible
}

/// Returns the caller's uid when per-row authorization applies: the caller is a
/// user (`owner` is `Some`) AND the table declares `ownerField` and/or
/// `collaboratorsField`. Returns `None` for bypass callers (machine tokens,
/// scheduled jobs, admin) and tables that declare neither field.
fn row_auth_enforced_uid<'a>(table_def: &'a TableDef, owner: Option<&'a str>) -> Option<&'a str> {
    if table_def.owner_field.is_some() || table_def.collaborators_field.is_some() {
        owner
    } else {
        None
    }
}

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
/// Worst-case number of documents `txn` could affect. Per-id steps
/// (`Insert`/`Patch`/`Replace`/`Delete`/`ExpectVersion`/`ExpectAbsent`/`Upsert`)
/// touch at most one each; `Schedule`/`CancelSchedule`/`StartWorkflow`/
/// `CancelWorkflow` count 0 (control-flow steps touch no documents); each
/// `PatchByQuery`/`DeleteByQuery` step touches
/// up to its `limit` (default and ceiling [`MAX_BY_QUERY_ROWS`]). The estimate
/// is an over-approximation — the actual count is lower when fewer rows match —
/// and is used by [`execute_txn`]'s [`MAX_AFFECTED_ROWS_PER_TXN`] budget check
/// and the admin `max_affected_docs` guardrail (admin/docs.rs, ws.rs). It must
/// never under-approximate (that would weaken both caps). SEC-104.
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

pub async fn execute_txn(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    txn: &Transaction,
    ctx: &PrincipalCtx,
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
            "transaction exceeds maximum of {MAX_STEPS} steps (counted recursively, including scheduled txns)"
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
            "transaction has {by_query_steps} by-query steps, exceeding the limit of {MAX_BY_QUERY_STEPS_PER_TXN}"
        )));
    }
    let worst = worst_case_affected(txn);
    if worst > MAX_AFFECTED_ROWS_PER_TXN {
        return Err(RtDbError::bad_request(format!(
            "transaction could affect up to {worst} documents, exceeding the limit of {MAX_AFFECTED_ROWS_PER_TXN}"
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
            Step::Patch { table, id, fields } => step_patch(&mut sctx, table, id, fields).await?,
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
            Step::Schedule { when, txn } => step_schedule(&mut sctx, when, txn).await?,
            Step::CancelSchedule { id } => step_cancel_schedule(&mut sctx, id).await?,
            Step::StartWorkflow { spec } => step_start_workflow(&mut sctx, spec).await?,
            Step::CancelWorkflow { id } => step_cancel_workflow(&mut sctx, id).await?,
        }
    }

    tx.commit().await?;
    Ok(TxnOutcome { results, write_set })
    }
    .instrument(span)
    .await
}

/// Shared, borrow-only context for the `step_*` per-step handlers in
/// [`execute_txn`]. Bundles the per-transaction borrows every step variant
/// reads (the sqlx transaction, the resolved schema name, the schema catalog,
/// the caller's principal, the derived owner id, the accumulating write set,
/// and the result sink) so each `step_*` signature stays flat instead of
/// carrying a 9+-argument list — the smell QA-002's per-step extraction
/// reintroduced (QA-105). Helpers take `&StepCtx` and reborrow the `&mut`
/// fields they need for the duration of each call. Follows the in-tree
/// precedents: `CommitterCtx` (ARC-002) and `QueryWindow` (`query.rs`).
///
/// Not every field is read by every variant (e.g. `step_delete` ignores
/// `owner`; the expect-* steps ignore `owner` and `write_set`); unused fields
/// are simply not accessed, so no per-variant subset struct is warranted.
struct StepCtx<'a> {
    tx: &'a mut PgConnection,
    db: &'a str,
    pg_schema_name: &'a str,
    schema: &'a SchemaDef,
    ctx: &'a PrincipalCtx,
    owner: Option<&'a str>,
    write_set: &'a mut WriteSet,
    results: &'a mut Vec<serde_json::Value>,
}

async fn step_insert(
    sctx: &mut StepCtx<'_>,
    table: &str,
    doc: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), RtDbError> {
    let table_def = sctx.schema.table(table)?;
    let now = now_ms();
    let doc = stamp_ttl_default(table_def, doc.clone(), now);
    let doc = stamp_updated_at(table_def, doc, now);
    let doc = apply_defaults(table_def, doc);
    let doc = stamp_owner(table_def, doc, sctx.owner);
    let doc = stamp_authorize(table_def, doc, sctx.ctx);
    verify_authorize_doc(table_def, &doc, sctx.ctx)?;
    let (id, stored, created_at) =
        do_insert(sctx.tx, sctx.pg_schema_name, table_def, table, &doc).await?;
    sctx.write_set.touch(table, &id, OpKind::Insert);
    // Created in this txn: before = None (created), after = stored doc.
    sctx.write_set.capture_doc(
        table,
        &id,
        Some(None),
        Some(Some(&stored)),
        Some(created_at),
    );
    sctx.results.push(serde_json::json!({ "id": id }));
    Ok(())
}

async fn step_patch(
    sctx: &mut StepCtx<'_>,
    table: &str,
    id: &str,
    fields: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), RtDbError> {
    let table_def = sctx.schema.table(table)?;
    check_owner(sctx.tx, sctx.pg_schema_name, table_def, table, id, sctx.ctx).await?;
    let fields = stamp_owner(table_def, fields.clone(), sctx.owner);
    let fields = stamp_authorize(table_def, fields, sctx.ctx);
    let fields = stamp_updated_at(table_def, fields, now_ms());
    let (pre_doc, merged, created_at) =
        do_patch(sctx.tx, sctx.pg_schema_name, table_def, table, id, &fields).await?;
    verify_authorize_doc(table_def, &merged, sctx.ctx)?;
    sctx.write_set.touch(table, id, OpKind::Patch);
    // `before` = pre-merge body (frozen on first touch by the helper
    // so a doc inserted earlier this txn stays `before = None`);
    // `after` = merged body.
    sctx.write_set.capture_doc(
        table,
        id,
        Some(Some(&pre_doc)),
        Some(Some(&merged)),
        Some(created_at),
    );
    sctx.results.push(serde_json::Value::Null);
    Ok(())
}

async fn step_replace(
    sctx: &mut StepCtx<'_>,
    table: &str,
    id: &str,
    doc: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), RtDbError> {
    let table_def = sctx.schema.table(table)?;
    check_owner(sctx.tx, sctx.pg_schema_name, table_def, table, id, sctx.ctx).await?;
    let doc = apply_defaults(table_def, doc.clone());
    let doc = stamp_updated_at(table_def, doc, now_ms());
    let doc = stamp_owner(table_def, doc, sctx.owner);
    let doc = stamp_authorize(table_def, doc, sctx.ctx);
    let (old_doc, new_doc, created_at) =
        do_replace(sctx.tx, sctx.pg_schema_name, table_def, table, id, &doc).await?;
    verify_authorize_doc(table_def, &new_doc, sctx.ctx)?;
    sctx.write_set.touch(table, id, OpKind::Replace);
    sctx.write_set.capture_doc(
        table,
        id,
        Some(Some(&old_doc)),
        Some(Some(&new_doc)),
        Some(created_at),
    );
    sctx.results.push(serde_json::Value::Null);
    Ok(())
}

async fn step_delete(sctx: &mut StepCtx<'_>, table: &str, id: &str) -> Result<(), RtDbError> {
    let table_def = sctx.schema.table(table)?;
    check_owner(sctx.tx, sctx.pg_schema_name, table_def, table, id, sctx.ctx).await?;
    if table_def.soft_delete {
        // FM-33: soft delete is a stamp, never a cascade trigger — a
        // soft-deleted parent leaves its children entirely untouched.
        do_soft_delete(sctx.tx, sctx.pg_schema_name, table, id).await?;
        sctx.write_set.touch(table, id, OpKind::Delete);
        // Delete records no value: `after = None` marks it deleted so
        // `fan_out` always re-runs (deleted ⇒ affects). `before` is left
        // for the helper to freeze at the earliest capture if this same
        // id was touched earlier in the txn, and `created_at` likewise
        // (a delete never fetches the row).
        sctx.write_set
            .capture_doc(table, id, None, Some(None), None);
    } else {
        // FM-33: a hard delete expands the app-level `onDelete` rules
        // (cascade/restrict/setNull) inside this same sqlx tx.
        let mut visited = HashSet::new();
        let mut cascade_rows = 0usize;
        delete_row_cascade(
            sctx.tx,
            sctx.pg_schema_name,
            sctx.schema,
            table,
            id,
            sctx.write_set,
            &mut visited,
            &mut cascade_rows,
            false,
        )
        .await?;
    }
    sctx.results.push(serde_json::Value::Null);
    Ok(())
}

async fn step_expect_version(
    sctx: &mut StepCtx<'_>,
    table: &str,
    id: &str,
    version: i64,
) -> Result<(), RtDbError> {
    let table_def = sctx.schema.table(table)?;
    do_expect_version(
        sctx.tx,
        sctx.pg_schema_name,
        table_def,
        table,
        id,
        version,
        sctx.ctx,
    )
    .await?;
    sctx.results.push(serde_json::Value::Null);
    Ok(())
}

async fn step_expect_absent(
    sctx: &mut StepCtx<'_>,
    table: &str,
    index: &str,
    eq: &[serde_json::Value],
) -> Result<(), RtDbError> {
    let table_def = sctx.schema.table(table)?;
    let rows = eq_lookup(sctx.tx, sctx.pg_schema_name, table_def, table, index, eq).await?;
    // Side-channel closure: only a matched doc the caller can see
    // counts as "present". A matched-but-invisible doc is "absent"
    // from the caller's view, so it does not fail the precondition.
    let present = rows
        .iter()
        .any(|(_id, doc, _created_at)| doc_visible_to(doc, table_def, sctx.ctx));
    if present {
        return Err(RtDbError::precondition(format!(
            "index '{index}' already has a matching document"
        )));
    }
    sctx.results.push(serde_json::Value::Null);
    Ok(())
}

async fn step_upsert(
    sctx: &mut StepCtx<'_>,
    table: &str,
    index: &str,
    eq: &[serde_json::Value],
    insert: &serde_json::Map<String, serde_json::Value>,
    patch: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), RtDbError> {
    let table_def = sctx.schema.table(table)?;
    let mut rows = eq_lookup(sctx.tx, sctx.pg_schema_name, table_def, table, index, eq).await?;
    if rows.len() > 1 {
        return Err(RtDbError::precondition("upsert matched multiple documents"));
    }
    match rows.pop() {
        None => {
            let now = now_ms();
            let insert = stamp_ttl_default(table_def, insert.clone(), now);
            let insert = stamp_updated_at(table_def, insert, now);
            let insert = apply_defaults(table_def, insert);
            let insert = stamp_owner(table_def, insert, sctx.owner);
            let insert = stamp_authorize(table_def, insert, sctx.ctx);
            verify_authorize_doc(table_def, &insert, sctx.ctx)?;
            let (id, stored, created_at) =
                do_insert(sctx.tx, sctx.pg_schema_name, table_def, table, &insert).await?;
            sctx.write_set.touch(table, &id, OpKind::Upsert);
            // Upsert-insert branch: same as Insert — created this txn.
            sctx.write_set.capture_doc(
                table,
                &id,
                Some(None),
                Some(Some(&stored)),
                Some(created_at),
            );
            sctx.results
                .push(serde_json::json!({ "id": id, "inserted": true }));
        }
        Some((id, doc_value, created_at)) => {
            let doc = match doc_value {
                serde_json::Value::Object(map) => map,
                _ => {
                    return Err(RtDbError::internal("stored doc is not a JSON object"));
                }
            };
            check_owner_doc(table_def, &doc, &id, sctx.ctx)?;
            let patch = stamp_owner(table_def, patch.clone(), sctx.owner);
            let patch = stamp_authorize(table_def, patch, sctx.ctx);
            let patch = stamp_updated_at(table_def, patch, now_ms());
            let pre_doc = doc.clone();
            let merged = apply_patch(table_def, doc, &patch)?;
            apply_update(sctx.tx, sctx.pg_schema_name, table_def, table, &id, &merged).await?;
            verify_authorize_doc(table_def, &merged, sctx.ctx)?;
            sctx.write_set.touch(table, &id, OpKind::Upsert);
            // Upsert-update branch: same as Patch — before = matched
            // body (first touch), after = merged.
            sctx.write_set.capture_doc(
                table,
                &id,
                Some(Some(&pre_doc)),
                Some(Some(&merged)),
                Some(created_at),
            );
            sctx.results
                .push(serde_json::json!({ "id": id, "inserted": false }));
        }
    }
    Ok(())
}

async fn step_patch_by_query(
    sctx: &mut StepCtx<'_>,
    table: &str,
    filter: &crate::query::FilterExpr,
    patch: &serde_json::Map<String, serde_json::Value>,
    limit_opt: Option<u32>,
) -> Result<(), RtDbError> {
    let pg_schema_name = sctx.pg_schema_name;
    let table_def = sctx.schema.table(table)?;
    let limit = limit_opt
        .unwrap_or(MAX_BY_QUERY_ROWS)
        .min(MAX_BY_QUERY_ROWS);
    let (where_sql, binds, limit_ph) =
        crate::query::compile_scan_where(table_def, sctx.ctx, sctx.owner, Some(filter))?;
    let table_ident = pg_table(table);
    let base = format!(
        "SELECT \"id\", \"doc\", \"created_at\" FROM \"{pg_schema_name}\".\"{table_ident}\""
    );
    let sql = if where_sql.is_empty() {
        format!("{base} ORDER BY \"created_at\", \"id\" LIMIT ${limit_ph}")
    } else {
        format!("{base} WHERE {where_sql} ORDER BY \"created_at\", \"id\" LIMIT ${limit_ph}")
    };
    let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64)>(&sql);
    for bind in binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
            EqBind::I64(v) => query.bind(v),
        };
    }
    // Fetch limit+1 so a full match set is detectable (`truncated`).
    query = query.bind(i64::from(limit) + 1);
    let rows = query.fetch_all(&mut *sctx.tx).await?;
    let truncated = rows.len() as u32 > limit;
    let take = std::cmp::min(rows.len(), limit as usize);
    for (id, doc_value, created_at) in rows.into_iter().take(take) {
        let doc = match doc_value {
            serde_json::Value::Object(map) => map,
            _ => return Err(RtDbError::internal("stored doc is not a JSON object")),
        };
        let pre_doc = doc.clone();
        let fields = stamp_owner(table_def, patch.clone(), sctx.owner);
        let fields = stamp_authorize(table_def, fields, sctx.ctx);
        let fields = stamp_updated_at(table_def, fields, now_ms());
        let merged = apply_patch(table_def, doc, &fields)?;
        apply_update(sctx.tx, pg_schema_name, table_def, table, &id, &merged).await?;
        verify_authorize_doc(table_def, &merged, sctx.ctx)?;
        sctx.write_set.touch(table, &id, OpKind::Patch);
        sctx.write_set.capture_doc(
            table,
            &id,
            Some(Some(&pre_doc)),
            Some(Some(&merged)),
            Some(created_at),
        );
    }
    sctx.results
        .push(serde_json::json!({ "patched": take, "truncated": truncated }));
    Ok(())
}

async fn step_delete_by_query(
    sctx: &mut StepCtx<'_>,
    table: &str,
    filter: &crate::query::FilterExpr,
    limit_opt: Option<u32>,
) -> Result<(), RtDbError> {
    let pg_schema_name = sctx.pg_schema_name;
    let table_def = sctx.schema.table(table)?;
    let limit = limit_opt
        .unwrap_or(MAX_BY_QUERY_ROWS)
        .min(MAX_BY_QUERY_ROWS);
    let (where_sql, binds, limit_ph) =
        crate::query::compile_scan_where(table_def, sctx.ctx, sctx.owner, Some(filter))?;
    let table_ident = pg_table(table);
    let base = format!("SELECT \"id\" FROM \"{pg_schema_name}\".\"{table_ident}\"");
    let sql = if where_sql.is_empty() {
        format!("{base} ORDER BY \"created_at\", \"id\" LIMIT ${limit_ph}")
    } else {
        format!("{base} WHERE {where_sql} ORDER BY \"created_at\", \"id\" LIMIT ${limit_ph}")
    };
    let mut query = sqlx::query_as::<_, (String,)>(&sql);
    for bind in binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
            EqBind::I64(v) => query.bind(v),
        };
    }
    query = query.bind(i64::from(limit) + 1);
    let rows = query.fetch_all(&mut *sctx.tx).await?;
    let truncated = rows.len() as u32 > limit;
    let take = std::cmp::min(rows.len(), limit as usize);
    let ids: Vec<String> = rows.into_iter().take(take).map(|(id,)| id).collect();
    let deleted = ids.len();
    // FM-33: each selected row deletes through the same `onDelete`-aware
    // path as a per-id Delete (stamp on a soft-delete table, else cascade).
    // `visited` and the row budget are shared across the whole step: a row
    // already hard-deleted by an earlier row's cascade is skipped (not a
    // NotFound abort), and one budget bounds every cascade the step starts.
    // Rows were selected in this same serialized txn, so there is no TOCTOU
    // gap (no concurrent writer can touch this db outside the committer
    // turn) — a visited id can only mean "cascaded earlier in this step".
    if !ids.is_empty() {
        let mut visited = HashSet::new();
        let mut cascade_rows = 0usize;
        for id in &ids {
            delete_row_cascade(
                sctx.tx,
                pg_schema_name,
                sctx.schema,
                table,
                id,
                sctx.write_set,
                &mut visited,
                &mut cascade_rows,
                false,
            )
            .await?;
        }
    }
    sctx.results
        .push(serde_json::json!({ "deleted": deleted, "truncated": truncated }));
    Ok(())
}

/// The `onDelete` action `ty` declares when it references `parent_table`, or
/// `None` when the type is not an `id`/`optional<id>` pointing at it (or
/// declares no action). Push validation guarantees an `onDelete`-bearing `Id`
/// appears only at the top level or directly under one `Optional`, so this
/// two-shape walk is exhaustive.
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
/// DELETE is safe only when this returns `false`).
pub(crate) fn has_on_delete_children(schema: &SchemaDef, parent: &str) -> bool {
    schema.tables.values().any(|td| {
        td.fields
            .values()
            .any(|ty| on_delete_ref(ty, parent).is_some())
    })
}

/// Ids of live (non-soft-deleted) rows in `child_table` whose `field_name`
/// references `parent_id`. Soft-deleted children are invisible to every
/// `onDelete` action (FM-33); per-row auth is deliberately NOT composed —
/// cascade semantics are deterministic from the schema, not from the deleting
/// caller's row visibility. `limit_one` fetches a single hit (the `restrict`
/// existence probe); otherwise the fetch is capped at the cascade row budget
/// plus one, which bounds memory on a pathological fan-out without ever
/// dropping a row that could still be processed within budget (processing
/// past the budget conflicts first).
async fn visible_child_ids(
    conn: &mut PgConnection,
    pg_schema_name: &str,
    child_table_def: &TableDef,
    child_table_name: &str,
    field_name: &str,
    parent_id: &str,
    limit_one: bool,
) -> Result<Vec<String>, RtDbError> {
    let table_ident = pg_table(child_table_name);
    let col = pg_col(field_name);
    let mut sql =
        format!("SELECT \"id\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE \"{col}\" = $1");
    if child_table_def.soft_delete {
        sql.push_str(" AND \"deleted_at\" IS NULL");
    }
    if limit_one {
        sql.push_str(" LIMIT 1");
    } else {
        sql.push_str(&format!(" LIMIT {}", MAX_CASCADE_ROWS + 1));
    }
    let rows: Vec<(String,)> = sqlx::query_as(&sql)
        .bind(parent_id)
        .fetch_all(&mut *conn)
        .await?;
    Ok(rows.into_iter().map(|(id,)| id).collect())
}

/// Deletes row `id` of `table_name` expanding the app-level `onDelete` rules
/// (FM-33), entirely on the caller's open sqlx transaction (single-writer
/// invariant: never a second connection). NOT SQL-FK — the graph is declared
/// in the pushed schema and walked here, so it composes with per-db schema
/// pushes and needs no physical FK constraints.
///
/// Semantics (per the FM-33 spec):
/// - `softDelete` table (unless `force_hard`): the row is STAMPED, not
///   removed, and the recursion stops — nothing past a stamped row is touched.
///   Soft delete is never itself a cascade trigger.
/// - Children first, parent last, walking every schema table field declaring
///   an `onDelete` action referencing this table (deterministic BTreeMap
///   order): `restrict` conflicts on the first live child (naming
///   `table.field`); `cascade` recurses per live child; `setNull` patches
///   `{field: null}` per live child (the key is REMOVED from the doc body —
///   `apply_patch`'s unset semantics — the typed column goes NULL, and
///   `version` bumps; a patch-shaped `DocOp`).
/// - `visited` guards cycles (self- and mutual-reference) and lets a
///   `DeleteByQuery` step skip rows an earlier row's cascade already removed.
/// - `cascade_rows` is the shared per-initiating-step budget
///   ([`MAX_CASCADE_ROWS`]): every stamped/deleted/nulled row plus each
///   initiator counts; over-budget is a `conflict`, so the txn rolls back
///   atomically.
/// - `force_hard` (reaper) physically removes rows even on `softDelete`
///   tables and propagates through the recursion.
// Params are independently needed (tx target, physical schema, whole-schema
// FK walk, row identity, tap recording, the two per-step guards, the reaper
// override); pushes past clippy's default 7-argument threshold like
// `insert_snapshot_row`.
// Boxed-return future: the recursion (a cascade child is itself deleted via
// `delete_row_cascade`) is illegal in a bare `async fn` (E0733) — the future's
// size is unbounded. `Box::pin` per level is the standard fix; depth is capped
// by MAX_CASCADE_ROWS, so the allocation chain is bounded by the same budget.
#[allow(clippy::too_many_arguments)]
pub(crate) fn delete_row_cascade<'a>(
    conn: &'a mut PgConnection,
    pg_schema_name: &'a str,
    schema: &'a SchemaDef,
    table_name: &'a str,
    id: &'a str,
    write_set: &'a mut WriteSet,
    visited: &'a mut HashSet<(String, String)>,
    cascade_rows: &'a mut usize,
    force_hard: bool,
) -> Pin<Box<dyn Future<Output = Result<(), RtDbError>> + Send + 'a>> {
    Box::pin(async move {
        let table_def = schema.table(table_name)?;
        if !visited.insert((table_name.to_string(), id.to_string())) {
            return Ok(());
        }
        if *cascade_rows >= MAX_CASCADE_ROWS {
            return Err(RtDbError::conflict(format!(
                "onDelete cascade exceeds the limit of {MAX_CASCADE_ROWS} rows"
            )));
        }
        *cascade_rows += 1;

        if table_def.soft_delete && !force_hard {
            do_soft_delete(conn, pg_schema_name, table_name, id).await?;
            write_set.touch(table_name, id, OpKind::Delete);
            // Stamped = deleted as far as every consumer is concerned: `after =
            // None` ⇒ `fan_out` always re-runs.
            write_set.capture_doc(table_name, id, None, Some(None), None);
            return Ok(());
        }

        for (child_table_name, child_table_def) in &schema.tables {
            for (field_name, field_type) in &child_table_def.fields {
                let Some(action) = on_delete_ref(field_type, table_name) else {
                    continue;
                };
                match action {
                    OnDeleteAction::Restrict => {
                        let hits = visible_child_ids(
                            conn,
                            pg_schema_name,
                            child_table_def,
                            child_table_name,
                            field_name,
                            id,
                            true,
                        )
                        .await?;
                        if let Some(child_id) = hits.first() {
                            return Err(RtDbError::conflict(format!(
                                "cannot delete '{table_name}': '{child_table_name}.{field_name}' is referenced by document '{child_id}'"
                            )));
                        }
                    }
                    OnDeleteAction::Cascade => {
                        let child_ids = visible_child_ids(
                            conn,
                            pg_schema_name,
                            child_table_def,
                            child_table_name,
                            field_name,
                            id,
                            false,
                        )
                        .await?;
                        for child_id in child_ids {
                            delete_row_cascade(
                                conn,
                                pg_schema_name,
                                schema,
                                child_table_name,
                                &child_id,
                                write_set,
                                visited,
                                cascade_rows,
                                force_hard,
                            )
                            .await?;
                        }
                    }
                    OnDeleteAction::SetNull => {
                        let child_ids = visible_child_ids(
                            conn,
                            pg_schema_name,
                            child_table_def,
                            child_table_name,
                            field_name,
                            id,
                            false,
                        )
                        .await?;
                        for child_id in child_ids {
                            if *cascade_rows >= MAX_CASCADE_ROWS {
                                return Err(RtDbError::conflict(format!(
                                    "onDelete cascade exceeds the limit of {MAX_CASCADE_ROWS} rows"
                                )));
                            }
                            *cascade_rows += 1;
                            // `{field: null}` on the optional-id REMOVES the key
                            // (apply_patch's unset semantics), so the typed column
                            // recomputes to NULL and `version` bumps.
                            let mut fields = serde_json::Map::new();
                            fields.insert(field_name.clone(), serde_json::Value::Null);
                            let fields = stamp_updated_at(child_table_def, fields, now_ms());
                            let (pre_doc, merged, created_at) = do_patch(
                                conn,
                                pg_schema_name,
                                child_table_def,
                                child_table_name,
                                &child_id,
                                &fields,
                            )
                            .await?;
                            write_set.touch(child_table_name, &child_id, OpKind::Patch);
                            write_set.capture_doc(
                                child_table_name,
                                &child_id,
                                Some(Some(&pre_doc)),
                                Some(Some(&merged)),
                                Some(created_at),
                            );
                        }
                    }
                }
            }
        }

        let table_ident = pg_table(table_name);
        let result = sqlx::query(&format!(
            "DELETE FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE \"id\" = $1"
        ))
        .bind(id)
        .execute(&mut *conn)
        .await?;
        if result.rows_affected() == 0 {
            return Err(RtDbError::not_found(format!("document '{id}' not found")));
        }
        write_set.touch(table_name, id, OpKind::Delete);
        write_set.capture_doc(table_name, id, None, Some(None), None);
        Ok(())
    })
}

/// `Undelete` step (FM-33): restore a soft-deleted row — `deleted_at = NULL`,
/// `version` + 1 (a stale client copy fails OCC against the restored row).
/// `NotFound` when absent; idempotent `Ok` when the row is present and already
/// live. `BadRequest` on a table that does not declare `softDelete`. The doc
/// body is untouched (a soft delete never modified it), so the restored row
/// re-appears byte-identical minus `deleted_at`.
async fn step_undelete(sctx: &mut StepCtx<'_>, table: &str, id: &str) -> Result<(), RtDbError> {
    let table_def = sctx.schema.table(table)?;
    if !table_def.soft_delete {
        return Err(RtDbError::bad_request(format!(
            "table '{table}' does not declare softDelete"
        )));
    }
    let table_ident = pg_table(table);
    let pg_schema_name = sctx.pg_schema_name;
    // Decode liveness as a boolean (`deleted_at IS NULL`) rather than decoding
    // the timestamptz — the stamp's value is never needed, only its presence.
    let row: Option<(serde_json::Value, i64, bool)> = sqlx::query_as(&format!(
        "SELECT \"doc\", \"created_at\", (\"deleted_at\" IS NULL) AS live \
         FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE \"id\" = $1"
    ))
    .bind(id)
    .fetch_optional(&mut *sctx.tx)
    .await?;
    let Some((doc_value, created_at, live)) = row else {
        return Err(RtDbError::not_found(format!("document '{id}' not found")));
    };
    let doc = match doc_value {
        serde_json::Value::Object(map) => map,
        _ => return Err(RtDbError::internal("stored doc is not a JSON object")),
    };
    // Per-row auth runs on the doc IN HAND, not via `check_owner` — whose
    // FM-33 live-only filter would silently PASS a soft-deleted row and let
    // any caller restore it. `check_owner_doc` gives the same Forbidden as
    // patch/replace/delete on a row the caller does not own.
    check_owner_doc(table_def, &doc, id, sctx.ctx)?;
    if live {
        // Idempotent: restoring a live row changes nothing.
        sctx.results.push(serde_json::Value::Null);
        return Ok(());
    }
    let result = sqlx::query(&format!(
        "UPDATE \"{pg_schema_name}\".\"{table_ident}\" \
         SET \"deleted_at\" = NULL, \"version\" = \"version\" + 1 \
         WHERE \"id\" = $1 AND \"deleted_at\" IS NOT NULL"
    ))
    .bind(id)
    .execute(&mut *sctx.tx)
    .await?;
    if result.rows_affected() == 0 {
        return Err(RtDbError::not_found(format!("document '{id}' not found")));
    }
    sctx.write_set.touch(table, id, OpKind::Patch);
    // `before = None` = created-this-txn semantics: the doc re-appears, so
    // `fan_out` re-runs every content-bearing subscription over it.
    sctx.write_set
        .capture_doc(table, id, Some(None), Some(Some(&doc)), Some(created_at));
    sctx.results.push(serde_json::Value::Null);
    Ok(())
}

/// Recursive table-scope check over every step in `txn`, including steps
/// nested inside `Schedule` payloads and inside `StartWorkflow` specs (via
/// [`authorize_spec_tables`], so a scheduled job cannot smuggle one either).
/// Runs at ENQUEUE time (the `Schedule` step here, and the standalone
/// Schedule-op surfaces) so a scoped machine token cannot smuggle a future
/// write into a forbidden table via a scheduled job. Bypass principals
/// (`tables = None` — admin/full-access/ interactive) are unaffected;
/// per-row rules are deliberately NOT pre-checked (rows change between
/// enqueue and fire; the firing job runs as the system principal —
/// documented behavior, see the FM-28 spec).
pub(crate) fn authorize_txn_tables(ctx: &PrincipalCtx, txn: &Transaction) -> Result<(), RtDbError> {
    for step in &txn.steps {
        if let Some(table) = step.table() {
            authorize_table(ctx, table)?;
        }
        if let Step::Schedule { txn, .. } = step {
            authorize_txn_tables(ctx, txn)?;
        }
        if let Step::StartWorkflow { spec } = step {
            authorize_spec_tables(ctx, spec)?;
        }
    }
    Ok(())
}

/// `Schedule` step: validate timing, recursively table-scope-check the
/// nested txn against the CURRENT caller, and insert the `scheduled_txns`
/// row on the open sqlx transaction — atomic with the enclosing txn's
/// document writes (FM-28). The row becomes visible at `tx.commit()`; the
/// scheduler's existing ≤2s poll picks it up from there.
async fn step_schedule(
    sctx: &mut StepCtx<'_>,
    when: &crate::protocol::ScheduleWhen,
    txn: &Transaction,
) -> Result<(), RtDbError> {
    authorize_txn_tables(sctx.ctx, txn)?;
    let (kind, due_at, cron) = scheduler::resolve_when(when.clone(), now_ms())?;
    let id = scheduler::insert_on(sctx.tx, sctx.db, kind, due_at, txn, cron.as_deref()).await?;
    sctx.results.push(serde_json::json!({ "scheduleId": id }));
    Ok(())
}

/// `CancelSchedule` step: DELETE the job row on the open sqlx transaction.
/// `false` (not an error) when the id is missing/already-fired/already
/// cancelled — matching the standalone cancel op. A fire in flight
/// completes; the cron finalize update then touches 0 rows.
async fn step_cancel_schedule(sctx: &mut StepCtx<'_>, id: &str) -> Result<(), RtDbError> {
    let cancelled = scheduler::cancel_on(sctx.tx, sctx.db, id).await?;
    sctx.results
        .push(serde_json::json!({ "cancelled": cancelled }));
    Ok(())
}

/// Recursive table-scope check over every step txn in a workflow spec,
/// INCLUDING steps nested inside `Schedule` payloads (via
/// `authorize_txn_tables`). Runs at SUBMIT time on every start surface so a
/// scoped machine token cannot smuggle a future write into a forbidden table
/// via a workflow step that fires later as bypass.
pub(crate) fn authorize_spec_tables(
    ctx: &PrincipalCtx,
    spec: &crate::protocol::WorkflowSpec,
) -> Result<(), RtDbError> {
    for step in &spec.steps {
        authorize_txn_tables(ctx, &step.txn)?;
    }
    Ok(())
}

/// `StartWorkflow` step: validate the spec, recursively table-scope-check it
/// against the CURRENT caller, and insert the `workflows` row on the open
/// sqlx transaction — atomic with the enclosing txn's document writes
/// (FM-29). The scheduler's existing poll claims the row from `tx.commit()`;
/// steps fire later as the system (bypass) principal.
async fn step_start_workflow(
    sctx: &mut StepCtx<'_>,
    spec: &crate::protocol::WorkflowSpec,
) -> Result<(), RtDbError> {
    crate::workflows::validate_spec(spec)?;
    authorize_spec_tables(sctx.ctx, spec)?;
    // Clamp before the u64→i64 cast (the `workflows::insert` hazard): a
    // serde-accepted `sleepBeforeMs` above i64::MAX would wrap negative and
    // produce an instantly-due gate.
    let sleep_ms = spec.steps[0]
        .sleep_before_ms
        .unwrap_or(0)
        .min(i64::MAX as u64) as i64;
    let gate = now_ms().saturating_add(sleep_ms);
    let id = crate::workflows::insert_on(sctx.tx, sctx.db, spec, gate).await?;
    sctx.results.push(serde_json::json!({ "workflowId": id }));
    Ok(())
}

/// `CancelWorkflow` step: flip the run row to `cancelled` on the open sqlx
/// transaction. `false` (not an error) when the id is missing or already
/// terminal; an advance in flight stops at its next step boundary.
async fn step_cancel_workflow(sctx: &mut StepCtx<'_>, id: &str) -> Result<(), RtDbError> {
    let cancelled = crate::workflows::cancel_on(sctx.tx, sctx.db, id).await?;
    sctx.results
        .push(serde_json::json!({ "cancelled": cancelled }));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_set_ops_records_kind() {
        let mut ws = WriteSet::default();
        ws.touch("projects", "id1", OpKind::Insert);
        ws.touch("projects", "id2", OpKind::Patch);
        ws.touch("tasks", "id3", OpKind::Delete);
        assert_eq!(ws.docs.len(), 3);
        assert_eq!(ws.ops.len(), 3);
        assert!(
            ws.ops
                .iter()
                .any(|o| o.id == "id1" && o.kind == OpKind::Insert)
        );
        assert!(
            ws.ops
                .iter()
                .any(|o| o.id == "id2" && o.kind == OpKind::Patch)
        );
        assert!(
            ws.ops
                .iter()
                .any(|o| o.table == "tasks" && o.kind == OpKind::Delete)
        );
    }

    #[test]
    fn count_steps_is_recursive() {
        let txn = Transaction {
            steps: vec![
                Step::Insert {
                    table: "t".to_string(),
                    doc: serde_json::Map::new(),
                },
                Step::Schedule {
                    when: crate::protocol::ScheduleWhen::AfterMs { ms: 1 },
                    txn: Box::new(Transaction {
                        steps: vec![
                            Step::Delete {
                                table: "t".to_string(),
                                id: "x".to_string(),
                            },
                            Step::Schedule {
                                when: crate::protocol::ScheduleWhen::RunAt { ms: 2 },
                                txn: Box::new(Transaction {
                                    steps: vec![Step::CancelSchedule {
                                        id: "j".to_string(),
                                    }],
                                }),
                            },
                        ],
                    }),
                },
            ],
        };
        assert_eq!(count_steps(&txn), 5);
    }
}
