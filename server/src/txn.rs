use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};

use sqlx::{PgConnection, PgPool};

use crate::auth::{PrincipalCtx, authorize_table};
use crate::db::{new_id, now_ms, validate_db_name};
use crate::ddl::{pg_col, pg_schema, pg_table};
use crate::error::RtDbError;
use crate::query::{FilterExpr, filter_matches};
use crate::schema::{
    FieldType, IndexDef, SchemaDef, TableDef, indexed_column_type, validate_doc, validate_value,
};

/// Maximum number of steps in a single transaction. A hard ceiling that bounds
/// how much work one serialized committer turn can do. Raised from 256 → 1024
/// (4× headroom) so reactive multi-writer apps can batch larger atomic units.
/// For bulk operations over many rows, prefer `PatchByQuery`/`DeleteByQuery`
/// (one step, server-side row cap) over unrolling per-id steps — that keeps a
/// txn under this limit AND avoids client-side read-all-then-patch patterns.
/// Raise further only if a measured workload genuinely needs >1024 atomic steps.
pub const MAX_STEPS: usize = 1024;

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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", deny_unknown_fields)]
pub enum Step {
    Insert {
        table: String,
        doc: serde_json::Map<String, serde_json::Value>,
    },
    Patch {
        table: String,
        id: String,
        fields: serde_json::Map<String, serde_json::Value>,
    },
    Replace {
        table: String,
        id: String,
        doc: serde_json::Map<String, serde_json::Value>,
    },
    Delete {
        table: String,
        id: String,
    },
    ExpectVersion {
        table: String,
        id: String,
        version: i64,
    },
    ExpectAbsent {
        table: String,
        index: String,
        eq: Vec<serde_json::Value>,
    },
    Upsert {
        table: String,
        index: String,
        eq: Vec<serde_json::Value>,
        insert: serde_json::Map<String, serde_json::Value>,
        patch: serde_json::Map<String, serde_json::Value>,
    },
    /// Find every row in `table` matching `filter` (the same `FilterExpr` the
    /// read path accepts) and apply `patch` to it, atomically, inside the
    /// serialized committer turn. Visibility matches the read path exactly: an
    /// interactive caller patches only rows they own/collaborate on and that
    /// satisfy the table's `authorize` predicate; a bypass principal (machine
    /// token/admin/scheduled) touches all matching rows. At most `limit` rows
    /// (default `MAX_BY_QUERY_ROWS`); a larger match set patches `limit` and
    /// reports `truncated: true`. Each patched row records a `DocOp`/`WriteSet`
    /// entry, so subscriptions, the op-feed, audit log, and webhooks all fire
    /// per row — the same contract as a per-id `Patch`.
    PatchByQuery {
        table: String,
        filter: crate::query::FilterExpr,
        patch: serde_json::Map<String, serde_json::Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u32>,
    },
    /// Find every row in `table` matching `filter` and delete it (same
    /// visibility/`limit`/`truncated` semantics as `PatchByQuery`). Enables
    /// server-side cascades and bulk cleanup (e.g. a scheduled job deleting
    /// expired rows by predicate) without a client-side read-all-then-delete.
    DeleteByQuery {
        table: String,
        filter: crate::query::FilterExpr,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u32>,
    },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Transaction {
    pub steps: Vec<Step>,
}

impl Step {
    /// The table this step targets. Every variant carries one; the table-scope
    /// gate (`authorize_table`) runs once per step inside `execute_txn` so a
    /// machine token scoped to a subset of tables cannot reach a forbidden table
    /// via any write path.
    pub fn table(&self) -> &str {
        match self {
            Step::Insert { table, .. }
            | Step::Patch { table, .. }
            | Step::Replace { table, .. }
            | Step::Delete { table, .. }
            | Step::ExpectVersion { table, .. }
            | Step::ExpectAbsent { table, .. }
            | Step::Upsert { table, .. }
            | Step::PatchByQuery { table, .. }
            | Step::DeleteByQuery { table, .. } => table,
        }
    }
}

/// The kind of write a step performed on a document. Recorded in `WriteSet.ops`
/// so downstream consumers (e.g. the activity feed) can stream what happened
/// without re-deriving it from the step list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
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
    fn touch(&mut self, table: &str, id: &str, kind: OpKind) {
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
    fn capture_doc(
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

/// SQL bind for an eq-lookup value, typed per the index field's `FieldType`
/// (`Optional` unwrapped). Prefix-friendly: callers may supply 0..=all of an
/// index's fields; full-arity enforcement is the caller's responsibility.
///
/// `Clone` + `PartialEq` are derived so `subs::IndexedRead` can store typed
/// binds (cloned from a query at registration) and compare a written doc's
/// typed field value against the wanted bind in `in_window`. `Eq` is NOT
/// derived: the `Num(f64)` arm admits NaN, which has no total order; the
/// binds compared here always originate from JSON (which cannot carry NaN),
/// so `PartialEq` is sound for the membership test.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EqBind {
    Text(String),
    Num(f64),
    Bool(bool),
    I64(i64),
}

/// Resolves `eq` (a prefix of `index`'s fields, 0..=all) into typed SQL binds.
/// Arity beyond the index's field count is a `BadRequest`; exact-arity
/// enforcement for Task 5's call sites happens in `eq_lookup`.
pub(crate) fn eq_binds(
    table: &TableDef,
    index: &IndexDef,
    eq: &[serde_json::Value],
) -> Result<Vec<EqBind>, RtDbError> {
    if eq.len() > index.fields.len() {
        return Err(RtDbError::bad_request(format!(
            "index '{}' expects at most {} eq value(s), got {}",
            index.name,
            index.fields.len(),
            eq.len()
        )));
    }

    index
        .fields
        .iter()
        .zip(eq.iter())
        .map(|(field_name, value)| {
            let field_type = table.fields.get(field_name).ok_or_else(|| {
                RtDbError::internal(format!("index references unknown field '{field_name}'"))
            })?;
            eq_bind_for(field_type, value)
        })
        .collect()
}

/// Shared with `query.rs`, which reuses this to type range-bound (`gt`/`gte`/`lt`/`lte`)
/// values the same way `eq` values are typed here.
pub(crate) fn eq_bind_for(ty: &FieldType, value: &serde_json::Value) -> Result<EqBind, RtDbError> {
    let (pg_type, _nullable) = indexed_column_type(ty)?;
    match pg_type {
        "text" => value
            .as_str()
            .map(|s| EqBind::Text(s.to_string()))
            .ok_or_else(|| RtDbError::bad_request("eq value must be a string")),
        "double precision" => value
            .as_f64()
            .map(EqBind::Num)
            .ok_or_else(|| RtDbError::bad_request("eq value must be a number")),
        "bigint" => value
            .as_str()
            .and_then(|s| s.parse::<i64>().ok())
            .map(EqBind::I64)
            .ok_or_else(|| RtDbError::bad_request("eq value must be an int64 string")),
        "boolean" => value
            .as_bool()
            .map(EqBind::Bool)
            .ok_or_else(|| RtDbError::bad_request("eq value must be a boolean")),
        other => Err(RtDbError::internal(format!(
            "unexpected pg type '{other}' for eq bind"
        ))),
    }
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
async fn apply_update(
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
    let row: Option<(serde_json::Value, i64)> = sqlx::query_as(&format!(
        "SELECT \"doc\", \"created_at\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE \"id\" = $1"
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
    let row: Option<(serde_json::Value, i64)> = sqlx::query_as(&format!(
        "SELECT \"doc\", \"created_at\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE \"id\" = $1"
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

async fn do_delete(
    conn: &mut PgConnection,
    pg_schema_name: &str,
    table_name: &str,
    id: &str,
) -> Result<(), RtDbError> {
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
    let row: Option<(i64, serde_json::Value)> = sqlx::query_as(&format!(
        "SELECT \"version\", \"doc\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE \"id\" = $1"
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
    let where_clause: Vec<String> = index_def
        .fields
        .iter()
        .enumerate()
        .map(|(i, field_name)| format!("\"{}\" = ${}", pg_col(field_name), i + 1))
        .collect();
    let sql = format!(
        "SELECT \"id\", \"doc\", \"created_at\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE {}",
        where_clause.join(" AND ")
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
/// `default_duration_ms` and the document omits the field. After this, the TTL
/// field is ordinary (patch/replace manipulate it normally). See
/// `docs/superpowers/specs/2026-08-01-document-ttl-design.md`.
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

/// Whether `uid` may access a row given the table's declared `ownerField`
/// and/or `collaboratorsField`: true when `uid` matches the doc's owner field
/// OR appears in the doc's collaborators array. A missing/null owner field and
/// a missing/null/empty/non-array collaborators array are treated as no-match.
/// Shared by the read path's point-read filter and the write path's pre-check
/// so OR-enforcement stays consistent across reads, writes, and subscriptions
/// (subscriptions re-run `execute_query`, which carries the same semantics).
pub fn row_visible_to(
    doc: &serde_json::Value,
    owner_field: Option<&str>,
    collab_field: Option<&str>,
    uid: &str,
) -> bool {
    let owner_match = owner_field
        .and_then(|f| doc.get(f))
        .and_then(|v| v.as_str())
        .is_some_and(|s| s == uid);
    let collab_match = collab_field
        .and_then(|f| doc.get(f))
        .and_then(|v| v.as_array())
        .is_some_and(|arr| arr.iter().any(|v| v.as_str() == Some(uid)));
    owner_match || collab_match
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
    let row: Option<(serde_json::Value,)> = sqlx::query_as(&format!(
        "SELECT \"doc\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE \"id\" = $1"
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
pub async fn execute_txn(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    txn: &Transaction,
    ctx: &PrincipalCtx,
) -> Result<TxnOutcome, RtDbError> {
    validate_db_name(db)?;
    // Task 5: `ctx` carries `user_id` + `email`; the row-auth helpers below use
    // only the uid, so derive the legacy `owner: Option<&str>` view once and
    // thread it unchanged — byte-identical ownerField/collaboratorsField behavior.
    let owner = ctx.user_id.as_deref();

    if txn.steps.len() > MAX_STEPS {
        return Err(RtDbError::bad_request(format!(
            "transaction exceeds maximum of {MAX_STEPS} steps"
        )));
    }

    let pg_schema_name = pg_schema(db);
    let mut results = Vec::with_capacity(txn.steps.len());
    let mut write_set = WriteSet::default();

    let mut tx = pool.begin().await?;

    for step in &txn.steps {
        // ENH-005 Task 4: gate each step against the machine-token table
        // allowlist BEFORE any work. A scoped token cannot write a forbidden
        // table via any step variant. `tables = None` (admin/scheduled/`User`/
        // full-access machine tokens) bypasses; the gate is a pure read. Runs
        // inside the sqlx tx so a `Forbidden` returns via `?` before commit and
        // rolls back the whole transaction.
        authorize_table(ctx, step.table())?;
        match step {
            Step::Insert { table, doc } => {
                step_insert(
                    &mut tx,
                    &pg_schema_name,
                    schema,
                    ctx,
                    owner,
                    &mut write_set,
                    &mut results,
                    table,
                    doc,
                )
                .await?
            }
            Step::Patch { table, id, fields } => {
                step_patch(
                    &mut tx,
                    &pg_schema_name,
                    schema,
                    ctx,
                    owner,
                    &mut write_set,
                    &mut results,
                    table,
                    id,
                    fields,
                )
                .await?
            }
            Step::Replace { table, id, doc } => {
                step_replace(
                    &mut tx,
                    &pg_schema_name,
                    schema,
                    ctx,
                    owner,
                    &mut write_set,
                    &mut results,
                    table,
                    id,
                    doc,
                )
                .await?
            }
            Step::Delete { table, id } => {
                step_delete(
                    &mut tx,
                    &pg_schema_name,
                    schema,
                    ctx,
                    &mut write_set,
                    &mut results,
                    table,
                    id,
                )
                .await?
            }
            Step::ExpectVersion { table, id, version } => {
                step_expect_version(
                    &mut tx,
                    &pg_schema_name,
                    schema,
                    ctx,
                    &mut results,
                    table,
                    id,
                    *version,
                )
                .await?
            }
            Step::ExpectAbsent { table, index, eq } => {
                step_expect_absent(
                    &mut tx,
                    &pg_schema_name,
                    schema,
                    ctx,
                    &mut results,
                    table,
                    index,
                    eq,
                )
                .await?
            }
            Step::Upsert {
                table,
                index,
                eq,
                insert,
                patch,
            } => {
                step_upsert(
                    &mut tx,
                    &pg_schema_name,
                    schema,
                    ctx,
                    owner,
                    &mut write_set,
                    &mut results,
                    table,
                    index,
                    eq,
                    insert,
                    patch,
                )
                .await?
            }
            Step::PatchByQuery {
                table,
                filter,
                patch,
                limit,
            } => {
                step_patch_by_query(
                    &mut tx,
                    &pg_schema_name,
                    schema,
                    ctx,
                    owner,
                    &mut write_set,
                    &mut results,
                    table,
                    filter,
                    patch,
                    *limit,
                )
                .await?
            }
            Step::DeleteByQuery {
                table,
                filter,
                limit,
            } => {
                step_delete_by_query(
                    &mut tx,
                    &pg_schema_name,
                    schema,
                    ctx,
                    owner,
                    &mut write_set,
                    &mut results,
                    table,
                    filter,
                    *limit,
                )
                .await?
            }
        }
    }

    tx.commit().await?;
    Ok(TxnOutcome { results, write_set })
}

#[allow(clippy::too_many_arguments)]
async fn step_insert(
    tx: &mut PgConnection,
    pg_schema_name: &str,
    schema: &SchemaDef,
    ctx: &PrincipalCtx,
    owner: Option<&str>,
    write_set: &mut WriteSet,
    results: &mut Vec<serde_json::Value>,
    table: &str,
    doc: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), RtDbError> {
    let table_def = schema.table(table)?;
    let doc = stamp_ttl_default(table_def, doc.clone(), now_ms());
    let doc = stamp_owner(table_def, doc, owner);
    let doc = stamp_authorize(table_def, doc, ctx);
    verify_authorize_doc(table_def, &doc, ctx)?;
    let (id, stored, created_at) = do_insert(tx, pg_schema_name, table_def, table, &doc).await?;
    write_set.touch(table, &id, OpKind::Insert);
    // Created in this txn: before = None (created), after = stored doc.
    write_set.capture_doc(
        table,
        &id,
        Some(None),
        Some(Some(&stored)),
        Some(created_at),
    );
    results.push(serde_json::json!({ "id": id }));
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn step_patch(
    tx: &mut PgConnection,
    pg_schema_name: &str,
    schema: &SchemaDef,
    ctx: &PrincipalCtx,
    owner: Option<&str>,
    write_set: &mut WriteSet,
    results: &mut Vec<serde_json::Value>,
    table: &str,
    id: &str,
    fields: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), RtDbError> {
    let table_def = schema.table(table)?;
    check_owner(tx, pg_schema_name, table_def, table, id, ctx).await?;
    let fields = stamp_owner(table_def, fields.clone(), owner);
    let fields = stamp_authorize(table_def, fields, ctx);
    let (pre_doc, merged, created_at) =
        do_patch(tx, pg_schema_name, table_def, table, id, &fields).await?;
    verify_authorize_doc(table_def, &merged, ctx)?;
    write_set.touch(table, id, OpKind::Patch);
    // `before` = pre-merge body (frozen on first touch by the helper
    // so a doc inserted earlier this txn stays `before = None`);
    // `after` = merged body.
    write_set.capture_doc(
        table,
        id,
        Some(Some(&pre_doc)),
        Some(Some(&merged)),
        Some(created_at),
    );
    results.push(serde_json::Value::Null);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn step_replace(
    tx: &mut PgConnection,
    pg_schema_name: &str,
    schema: &SchemaDef,
    ctx: &PrincipalCtx,
    owner: Option<&str>,
    write_set: &mut WriteSet,
    results: &mut Vec<serde_json::Value>,
    table: &str,
    id: &str,
    doc: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), RtDbError> {
    let table_def = schema.table(table)?;
    check_owner(tx, pg_schema_name, table_def, table, id, ctx).await?;
    let doc = stamp_owner(table_def, doc.clone(), owner);
    let doc = stamp_authorize(table_def, doc, ctx);
    let (old_doc, new_doc, created_at) =
        do_replace(tx, pg_schema_name, table_def, table, id, &doc).await?;
    verify_authorize_doc(table_def, &new_doc, ctx)?;
    write_set.touch(table, id, OpKind::Replace);
    write_set.capture_doc(
        table,
        id,
        Some(Some(&old_doc)),
        Some(Some(&new_doc)),
        Some(created_at),
    );
    results.push(serde_json::Value::Null);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn step_delete(
    tx: &mut PgConnection,
    pg_schema_name: &str,
    schema: &SchemaDef,
    ctx: &PrincipalCtx,
    write_set: &mut WriteSet,
    results: &mut Vec<serde_json::Value>,
    table: &str,
    id: &str,
) -> Result<(), RtDbError> {
    let table_def = schema.table(table)?;
    check_owner(tx, pg_schema_name, table_def, table, id, ctx).await?;
    do_delete(tx, pg_schema_name, table, id).await?;
    write_set.touch(table, id, OpKind::Delete);
    // Delete records no value: `after = None` marks it deleted so
    // `fan_out` always re-runs (deleted ⇒ affects). `before` is left
    // for the helper to freeze at the earliest capture if this same
    // id was touched earlier in the txn, and `created_at` likewise
    // (a delete never fetches the row).
    write_set.capture_doc(table, id, None, Some(None), None);
    results.push(serde_json::Value::Null);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn step_expect_version(
    tx: &mut PgConnection,
    pg_schema_name: &str,
    schema: &SchemaDef,
    ctx: &PrincipalCtx,
    results: &mut Vec<serde_json::Value>,
    table: &str,
    id: &str,
    version: i64,
) -> Result<(), RtDbError> {
    let table_def = schema.table(table)?;
    do_expect_version(tx, pg_schema_name, table_def, table, id, version, ctx).await?;
    results.push(serde_json::Value::Null);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn step_expect_absent(
    tx: &mut PgConnection,
    pg_schema_name: &str,
    schema: &SchemaDef,
    ctx: &PrincipalCtx,
    results: &mut Vec<serde_json::Value>,
    table: &str,
    index: &str,
    eq: &[serde_json::Value],
) -> Result<(), RtDbError> {
    let table_def = schema.table(table)?;
    let rows = eq_lookup(tx, pg_schema_name, table_def, table, index, eq).await?;
    // Side-channel closure: only a matched doc the caller can see
    // counts as "present". A matched-but-invisible doc is "absent"
    // from the caller's view, so it does not fail the precondition.
    let present = rows
        .iter()
        .any(|(_id, doc, _created_at)| doc_visible_to(doc, table_def, ctx));
    if present {
        return Err(RtDbError::precondition(format!(
            "index '{index}' already has a matching document"
        )));
    }
    results.push(serde_json::Value::Null);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn step_upsert(
    tx: &mut PgConnection,
    pg_schema_name: &str,
    schema: &SchemaDef,
    ctx: &PrincipalCtx,
    owner: Option<&str>,
    write_set: &mut WriteSet,
    results: &mut Vec<serde_json::Value>,
    table: &str,
    index: &str,
    eq: &[serde_json::Value],
    insert: &serde_json::Map<String, serde_json::Value>,
    patch: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), RtDbError> {
    let table_def = schema.table(table)?;
    let mut rows = eq_lookup(tx, pg_schema_name, table_def, table, index, eq).await?;
    if rows.len() > 1 {
        return Err(RtDbError::precondition("upsert matched multiple documents"));
    }
    match rows.pop() {
        None => {
            let insert = stamp_owner(table_def, insert.clone(), owner);
            let insert = stamp_authorize(table_def, insert, ctx);
            verify_authorize_doc(table_def, &insert, ctx)?;
            let (id, stored, created_at) =
                do_insert(tx, pg_schema_name, table_def, table, &insert).await?;
            write_set.touch(table, &id, OpKind::Upsert);
            // Upsert-insert branch: same as Insert — created this txn.
            write_set.capture_doc(
                table,
                &id,
                Some(None),
                Some(Some(&stored)),
                Some(created_at),
            );
            results.push(serde_json::json!({ "id": id, "inserted": true }));
        }
        Some((id, doc_value, created_at)) => {
            let doc = match doc_value {
                serde_json::Value::Object(map) => map,
                _ => {
                    return Err(RtDbError::internal("stored doc is not a JSON object"));
                }
            };
            check_owner_doc(table_def, &doc, &id, ctx)?;
            let patch = stamp_owner(table_def, patch.clone(), owner);
            let patch = stamp_authorize(table_def, patch, ctx);
            let pre_doc = doc.clone();
            let merged = apply_patch(table_def, doc, &patch)?;
            apply_update(tx, pg_schema_name, table_def, table, &id, &merged).await?;
            verify_authorize_doc(table_def, &merged, ctx)?;
            write_set.touch(table, &id, OpKind::Upsert);
            // Upsert-update branch: same as Patch — before = matched
            // body (first touch), after = merged.
            write_set.capture_doc(
                table,
                &id,
                Some(Some(&pre_doc)),
                Some(Some(&merged)),
                Some(created_at),
            );
            results.push(serde_json::json!({ "id": id, "inserted": false }));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn step_patch_by_query(
    tx: &mut PgConnection,
    pg_schema_name: &str,
    schema: &SchemaDef,
    ctx: &PrincipalCtx,
    owner: Option<&str>,
    write_set: &mut WriteSet,
    results: &mut Vec<serde_json::Value>,
    table: &str,
    filter: &crate::query::FilterExpr,
    patch: &serde_json::Map<String, serde_json::Value>,
    limit_opt: Option<u32>,
) -> Result<(), RtDbError> {
    let table_def = schema.table(table)?;
    let limit = limit_opt
        .unwrap_or(MAX_BY_QUERY_ROWS)
        .min(MAX_BY_QUERY_ROWS);
    let (where_sql, binds, limit_ph) =
        crate::query::compile_scan_where(table_def, ctx, owner, Some(filter))?;
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
    let rows = query.fetch_all(&mut *tx).await?;
    let truncated = rows.len() as u32 > limit;
    let take = std::cmp::min(rows.len(), limit as usize);
    for (id, doc_value, created_at) in rows.into_iter().take(take) {
        let doc = match doc_value {
            serde_json::Value::Object(map) => map,
            _ => return Err(RtDbError::internal("stored doc is not a JSON object")),
        };
        let pre_doc = doc.clone();
        let fields = stamp_owner(table_def, patch.clone(), owner);
        let fields = stamp_authorize(table_def, fields, ctx);
        let merged = apply_patch(table_def, doc, &fields)?;
        apply_update(tx, pg_schema_name, table_def, table, &id, &merged).await?;
        verify_authorize_doc(table_def, &merged, ctx)?;
        write_set.touch(table, &id, OpKind::Patch);
        write_set.capture_doc(
            table,
            &id,
            Some(Some(&pre_doc)),
            Some(Some(&merged)),
            Some(created_at),
        );
    }
    results.push(serde_json::json!({ "patched": take, "truncated": truncated }));
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn step_delete_by_query(
    tx: &mut PgConnection,
    pg_schema_name: &str,
    schema: &SchemaDef,
    ctx: &PrincipalCtx,
    owner: Option<&str>,
    write_set: &mut WriteSet,
    results: &mut Vec<serde_json::Value>,
    table: &str,
    filter: &crate::query::FilterExpr,
    limit_opt: Option<u32>,
) -> Result<(), RtDbError> {
    let table_def = schema.table(table)?;
    let limit = limit_opt
        .unwrap_or(MAX_BY_QUERY_ROWS)
        .min(MAX_BY_QUERY_ROWS);
    let (where_sql, binds, limit_ph) =
        crate::query::compile_scan_where(table_def, ctx, owner, Some(filter))?;
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
    let rows = query.fetch_all(&mut *tx).await?;
    let truncated = rows.len() as u32 > limit;
    let take = std::cmp::min(rows.len(), limit as usize);
    let ids: Vec<String> = rows.into_iter().take(take).map(|(id,)| id).collect();
    let deleted = ids.len();
    if !ids.is_empty() {
        // Bulk delete the selected ids in one statement. They were selected in
        // this same serialized txn, so there is no TOCTOU gap (no concurrent
        // writer can touch this db outside the committer turn).
        let placeholders: Vec<String> = (0..ids.len()).map(|i| format!("${}", i + 1)).collect();
        let del_sql = format!(
            "DELETE FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE \"id\" IN ({})",
            placeholders.join(", ")
        );
        let mut del = sqlx::query(&del_sql);
        for id in &ids {
            del = del.bind(id);
        }
        del.execute(&mut *tx).await?;
    }
    for id in &ids {
        write_set.touch(table, id, OpKind::Delete);
        // Delete records no value: `after = None` ⇒ `fan_out` always re-runs.
        write_set.capture_doc(table, id, None, Some(None), None);
    }
    results.push(serde_json::json!({ "deleted": deleted, "truncated": truncated }));
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
}
