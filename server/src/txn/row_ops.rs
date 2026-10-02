//! Per-row physical writes: the indexed-column binder, do_insert/apply_update/do_patch/do_replace/do_soft_delete/do_expect_version, eq_lookup, and insert_snapshot_row (ARC-008 split; pure move from the former single-file `txn.rs`).

use std::collections::BTreeSet;

use sqlx::PgConnection;

use crate::auth::PrincipalCtx;
use crate::db::{new_id, now_ms};
use crate::ddl::{pg_col, pg_table};
use crate::dsl::eq_binds;
use crate::error::RtDbError;
use crate::schema::{
    FieldType, TableDef, TableDefExt, indexed_column_type, validate_doc, validate_value,
};
use crate::txn::{EqBind, doc_visible_to, stamp_computed};

// ARC-202: the wire types this module used to define live in `dsl.rs` now;
// re-exported so every `crate::txn::` path (and the integration tests'
// `rtdb_server::txn::` paths) keep resolving unchanged.
/// SQL bind for an indexed-column value extracted from a document, `None`
/// when the field is absent or explicitly null (stored as SQL NULL).
pub(crate) enum ColBind {
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
pub(crate) enum ColumnKind {
    Scalar(FieldType),
    Vector,
}

/// One physical indexed column: its physical name (`f_<field>` or `v_<index>`),
/// the doc field its value is read from, and its kind.
pub(crate) struct TableColumn {
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
pub(crate) fn table_columns(table: &TableDef) -> Result<Vec<TableColumn>, RtDbError> {
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
pub(crate) fn column_binds(
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

pub(crate) fn column_bind_for(
    kind: &ColumnKind,
    value: &serde_json::Value,
) -> Result<ColBind, RtDbError> {
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
pub(crate) fn scalar_bind(ty: &FieldType, value: &serde_json::Value) -> Result<ColBind, RtDbError> {
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
pub(crate) fn apply_patch(
    table: &TableDef,
    mut doc: serde_json::Map<String, serde_json::Value>,
    fields: &serde_json::Map<String, serde_json::Value>,
) -> Result<serde_json::Map<String, serde_json::Value>, RtDbError> {
    // The auto-increment field is server-assigned and immutable after insert.
    // A patch may carry it back unchanged (round-trip friendly), but any
    // different value — including a type-shifted form of the same number —
    // is rejected. Covers `Patch`, upsert's update branch, and `PatchByQuery`
    // (every patch-shaped path funnels through here).
    if let Some(auto) = &table.auto_increment_field
        && let Some(value) = fields.get(auto)
        && doc.get(auto) != Some(value)
    {
        return Err(RtDbError::bad_request(format!(
            "autoIncrementField '{auto}' cannot be changed"
        )));
    }
    for (field_name, field_value) in fields {
        // A computed key in the patch is dropped, not merged: the stamp below
        // re-derives it from the final doc, so skipping here also keeps a
        // wrong-typed client value from failing validate_value first.
        if table.computed.contains_key(field_name) {
            continue;
        }
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

    let doc = stamp_computed(table, doc, now_ms())?;
    validate_doc(table, &doc)?;
    Ok(doc)
}

/// Strips keys whose value is an explicit JSON `null` for an `Optional`
/// field whose inner type does not itself accept `null`, matching
/// `apply_patch`'s treatment of a patch null as "unset" rather than a stored
/// null — so an inserted document and a patched-then-nulled document end up
/// in the same shape (key absent), not two different representations of the
/// same logical state.
pub(crate) fn strip_unset_optionals(
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
pub(crate) async fn do_insert(
    conn: &mut PgConnection,
    pg_schema_name: &str,
    table_def: &TableDef,
    table_name: &str,
    doc: &serde_json::Map<String, serde_json::Value>,
) -> Result<(String, serde_json::Map<String, serde_json::Value>, i64), RtDbError> {
    // Computed stamping is the LAST insert stamp — after the caller's ttl /
    // updatedAt / defaults / owner / authorize / autoIncrement chain — so
    // expressions see final inputs, and before validate_doc so a
    // client-supplied computed value never reaches validation.
    let doc = stamp_computed(table_def, doc.clone(), now_ms())?;
    validate_doc(table_def, &doc)?;
    let stripped = strip_unset_optionals(table_def, doc);

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
pub(crate) async fn do_patch(
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
        "SELECT \"doc\", \"created_at\" FROM \"{pg_schema_name}\".\"{table_ident}\"  \
        WHERE \"id\" = $1{live_only}"
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
pub(crate) async fn do_replace(
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
        "SELECT \"doc\", \"created_at\" FROM \"{pg_schema_name}\".\"{table_ident}\"  \
        WHERE \"id\" = $1{live_only}"
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

    // A replace validates as a complete document, so the server-stamped
    // auto-increment value must be present: an omitted field is filled from
    // the stored row (preserved, never re-assigned), and a supplied value
    // must equal the stored one — round-trip replace works, changing the
    // counter does not. A stored doc that PREDATES the declaration (written
    // before the counter was added) has no value to preserve, so a replace
    // may set one — first-set, like an insert.
    let mut new_doc = new_doc.clone();
    if let Some(auto) = &table_def.auto_increment_field
        && let Some(stored) = old_doc.get(auto)
    {
        match new_doc.get(auto) {
            None | Some(serde_json::Value::Null) => {
                new_doc.insert(auto.clone(), stored.clone());
            }
            Some(value) if value != stored => {
                return Err(RtDbError::bad_request(format!(
                    "autoIncrementField '{auto}' cannot be changed"
                )));
            }
            Some(_) => {}
        }
    }

    // Client-supplied computed values are dropped before validation: the
    // stamp re-derives them from the final doc, and a wrong-typed client
    // value must not fail validate_doc first.
    for name in table_def.computed.keys() {
        new_doc.remove(name);
    }
    let new_doc = stamp_computed(table_def, new_doc, now_ms())?;
    validate_doc(table_def, &new_doc)?;
    let new_doc = strip_unset_optionals(table_def, new_doc);
    apply_update(conn, pg_schema_name, table_def, table_name, id, &new_doc).await?;
    Ok((old_doc, new_doc, created_at))
}
/// Inserts a row with an explicit id/created_at/version, preserving a document's
/// original identity and history instead of minting new ones like `do_insert`.
/// Indexed columns are recomputed from `doc` the same way `do_insert` does. Used
/// by `snapshot::import_database` to replay an exported row exactly.
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
pub(crate) async fn do_soft_delete(
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

pub(crate) async fn do_expect_version(
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
        "SELECT \"version\", \"doc\" FROM \"{pg_schema_name}\".\"{table_ident}\"  \
        WHERE \"id\" = $1{live_only}"
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
pub(crate) async fn eq_lookup(
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
        "SELECT \"id\", \"doc\", \"created_at\" FROM \"{pg_schema_name}\".\"{table_ident}\"  \
        WHERE {}",
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
