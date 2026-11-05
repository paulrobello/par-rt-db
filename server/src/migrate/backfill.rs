//! Migration backfill engine (QA-003 split): the shared document-rewrite
//! machinery the DB appliers lean on — computed-field re-stamping, typed
//! column recomputation, doc-key rewrites, id scans, and value coercion.

use crate::ddl::{backfill_expr, indexed_fields, pg_col, pg_table};
use crate::error::RtDbError;
use crate::schema::{SchemaDef, TableDef, indexed_column_type};
use crate::txn::{DocOp, OpKind};

use crate::value_expr::{Cast, compile_value_expr};

use super::MigrateBind;
use super::apply::table_def;
use super::plan::require_field_ident;

pub(super) fn computed_field_names(table: &TableDef) -> Vec<String> {
    table.computed.keys().cloned().collect()
}

/// Names of the table's computed entries whose expression reads `field` — the
/// re-stamp set after a directive (`setDefault`/`changeType`) rewrote that
/// field's stored values. The walk covers `Field` nodes and `Case.whens`
/// filter fields, matching what `validate_computed` treats as a reference.
pub(super) fn computed_fields_referencing(table: &TableDef, field: &str) -> Vec<String> {
    table
        .computed
        .iter()
        .filter(|(_, expr)| {
            let mut referenced = false;
            crate::value_expr::walk_value_expr_fields(expr, &mut |f| {
                if f == field {
                    referenced = true;
                }
            });
            referenced
        })
        .map(|(name, _)| name.clone())
        .collect()
}

/// Re-derives the named `computed` entries into `doc` for exactly `ids`,
/// inside the caller's tx (ENH-028). Used by `evalExpr` (whose doc rewrite
/// changes computed inputs — the stored stamp would be stale until each row's
/// next write), by `setDefault`/`changeType` (same staleness, scoped to the
/// dependent entries), and by the push/restore backfill. Each entry runs as
/// one UPDATE over the ids with the SQL compiler's expression; a SQL-NULL
/// result REMOVES the key (the write path's `stamp_computed` convention — an
/// unset optional is an absent key), which a bare `jsonb_set` cannot express:
/// with a SQL-NULL value it nulls the whole `doc` and trips the column's NOT
/// NULL.
pub(super) async fn restamp_computed_fields(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    schema_name: &str,
    table_pg: &str,
    table_logical: &str,
    derived: &SchemaDef,
    fields: &[String],
    ids: &[String],
) -> Result<(), RtDbError> {
    if ids.is_empty() || fields.is_empty() {
        return Ok(());
    }
    let derived_table = table_def(derived, table_logical)?;
    for field in fields {
        let ve = derived_table.computed.get(field).ok_or_else(|| {
            // The callers build `fields` from this same map (evalExpr: its
            // keys; backfill: the old-vs-new diff), so this is unreachable
            // unless they drift apart.
            RtDbError::internal(format!(
                "computed field '{table_logical}.{field}' missing from derived schema"
            ))
        })?;
        // SEC-124 backstop: `field` is interpolated into the jsonb_set path
        // literal and the `doc - '{field}'` key, like every other site.
        require_field_ident(field)?;
        // `id = ANY($1)` binds first; the expression's placeholders start at 2.
        let mut binds: Vec<MigrateBind> = Vec::new();
        let expr_sql = compile_value_expr(ve, derived_table, 2, &mut binds)?;
        let sql = format!(
            "UPDATE \"{schema_name}\".\"{table_pg}\" \
             SET doc = CASE WHEN to_jsonb(({expr_sql})) IS NULL \
                 THEN doc - '{field}' \
                 ELSE jsonb_set(doc, '{{\"{field}\"}}', to_jsonb(({expr_sql})), true) END \
             WHERE id = ANY($1)"
        );
        let mut q = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(ids);
        for b in &binds {
            q = match b {
                MigrateBind::Text(s) => q.bind(s.as_str()),
                MigrateBind::Num(f) => q.bind(*f),
                MigrateBind::Bool(b) => q.bind(*b),
                MigrateBind::I64(i) => q.bind(*i),
                MigrateBind::Json(v) => q.bind(sqlx::types::Json(v)),
            };
        }
        q.execute(&mut **tx).await?;
    }
    Ok(())
}

/// Push/restore computed backfill (ENH-028 Task 4): re-derives `fields` — the
/// computed entries this apply ADDED or CHANGED, per the old-vs-new computed
/// diff in `ddl::apply_schema_additive` — for EVERY row of the table, then
/// refreshes the typed columns (`recompute_all_indexed`). Runs inside the
/// push/restore tx, after the new typed columns exist and before index
/// creation. Entries whose expression did NOT change are excluded, so a pure
/// re-push rewrites nothing (docs and `version` untouched) and a Now()-bearing
/// expr refreshes its timestamp only when its expression changed. Removal of a
/// computed entry backfills nothing — stored values stay and become ordinary
/// client-writable fields.
pub(crate) async fn backfill_computed(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    pg_schema_name: &str,
    table_logical: &str,
    derived: &SchemaDef,
    fields: &[String],
) -> Result<(), RtDbError> {
    let t = pg_table(table_logical);
    let ids = all_ids(tx, pg_schema_name, &t).await?;
    if ids.is_empty() {
        return Ok(());
    }
    restamp_computed_fields(tx, pg_schema_name, &t, table_logical, derived, fields, &ids).await?;
    recompute_all_indexed(tx, pg_schema_name, &t, table_logical, derived, &ids).await?;
    Ok(())
}

/// Reads the stored schema from the db's `meta` row inside `tx`. Before Task 6's
/// committer arm updates `meta`, this is the authoritative pre-migration schema.
pub(super) async fn load_schema_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    schema_name: &str,
) -> Result<Option<SchemaDef>, RtDbError> {
    let row: Option<(serde_json::Value,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT value FROM \"{schema_name}\".meta WHERE key = 'schema'"
    )))
    .fetch_optional(&mut **tx)
    .await?;
    match row {
        Some((value,)) => {
            let schema = serde_json::from_value(value).map_err(|err| {
                tracing::error!(error = %err, "failed to deserialize stored schema");
                RtDbError::internal("failed to read stored schema")
            })?;
            Ok(Some(schema))
        }
        None => Ok(None),
    }
}

pub(super) async fn rewrite_doc_key(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    schema_name: &str,
    table: &str,
    from: &str,
    to: &str,
) -> Result<i64, RtDbError> {
    // SEC-124: `from`/`to` are interpolated unbound into the SQL string
    // literals (`doc - '{from}'`, `doc->'{from}'`, the `jsonb_set` path
    // `{{"{to}"}}`). The caller (`apply_rename_field`) already checks both,
    // but this helper's literals are a distinct interpolation site — re-check
    // so a future caller can't bypass it (prior site had no backstop).
    require_field_ident(from)?;
    require_field_ident(to)?;
    let res = sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE \"{schema_name}\".\"{table}\" \
         SET doc = jsonb_set(doc - '{from}', '{{\"{to}\"}}', doc->'{from}', true) \
         WHERE doc ? '{from}'"
    )))
    .execute(&mut **tx)
    .await?;
    Ok(res.rows_affected() as i64)
}

/// Recomputes a typed `f_` column for `ids` using `cast_expr` (the
/// `ddl::backfill_expr` pattern), scoped to exactly the rows that just received
/// a default. `cast_expr` reads `doc->>'{field}'`, which is now populated.
pub(super) async fn recompute_columns_for_ids(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    schema_name: &str,
    table: &str,
    col: &str,
    cast_expr: &str,
    ids: &[String],
) -> Result<(), RtDbError> {
    if ids.is_empty() {
        return Ok(());
    }
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE \"{schema_name}\".\"{table}\" SET \"{col}\" = {cast_expr} WHERE id = ANY($1)"
    )))
    .bind(ids)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Recomputes EVERY indexed `f_` column on `table` from the current `doc`, for
/// exactly the rows in `ids`. Used by `evalExpr`, whose SQL expression may
/// populate a key that drives a typed column; rather than track which column
/// `set` maps to (it may not even be indexed, or may populate a not-yet-schemad
/// key the caller adds via a later push), recompute them all. Scoping by `ids`
/// (captured before the doc rewrite from the same `cond`) — not by re-evaluating
/// `cond` — is strictly correct when `expr` modifies a field that `cond` tests:
/// a `cond`-scoped recompute could otherwise miss an updated row whose new doc
/// no longer matches `cond`, leaving its `f_` column stale. No-op when the
/// table has no indexed fields or `ids` is empty. Field types come from the
/// `derived` (post-migration) schema — the authoritative source for each
/// column's current type.
pub(super) async fn recompute_all_indexed(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    schema_name: &str,
    table_pg: &str,
    table_logical: &str,
    derived: &SchemaDef,
    ids: &[String],
) -> Result<(), RtDbError> {
    if ids.is_empty() {
        return Ok(());
    }
    let derived_table = table_def(derived, table_logical)?;
    let indexed_set = indexed_fields(derived_table);
    let indexed: Vec<&String> = indexed_set.iter().collect();
    if indexed.is_empty() {
        return Ok(());
    }
    // One UPDATE setting every indexed column; each column's cast mirrors
    // `ddl::backfill_expr` reading the just-rewritten doc.
    let mut sets: Vec<String> = Vec::with_capacity(indexed.len());
    for field in &indexed {
        let fty = derived_table.fields.get(*field).ok_or_else(|| {
            RtDbError::internal(format!(
                "indexed field '{table_logical}.{field}' missing from derived schema"
            ))
        })?;
        let (pg_type, _) = indexed_column_type(fty)?;
        let col = pg_col(field);
        let cast_expr = backfill_expr(pg_type, field)?;
        sets.push(format!("\"{col}\" = {cast_expr}"));
    }
    let sets_clause = sets.join(", ");
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE \"{schema_name}\".\"{table_pg}\" SET {sets_clause} WHERE id = ANY($1)"
    )))
    .bind(ids)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(super) async fn all_ids(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    schema_name: &str,
    table: &str,
) -> Result<Vec<String>, RtDbError> {
    ids_where(tx, schema_name, table, "true", &[]).await
}

/// Selects `id` for rows matching `cond`. `cond` is composed from validated
/// field identifiers (regex-clean per the schema layer) for every caller EXCEPT
/// `apply_eval_expr`, where `cond` is the admin-supplied `where` SQL text gated
/// by the root admin_key (see `apply_eval_expr`). The `evalExpr` path is the
/// scoped raw-SQL escape hatch; all other callers pass `doc ? '{field}'` shapes
/// the same way `ddl::push_schema` does for its backfill.
pub(super) async fn ids_where(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    schema_name: &str,
    table: &str,
    cond: &str,
    binds: &[MigrateBind],
) -> Result<Vec<String>, RtDbError> {
    let sql = format!("SELECT id FROM \"{schema_name}\".\"{table}\" WHERE {cond}");
    let mut q = sqlx::query_as::<_, (String,)>(sqlx::AssertSqlSafe(sql));
    for b in binds {
        q = match b {
            MigrateBind::Text(s) => q.bind(s),
            MigrateBind::Num(f) => q.bind(f),
            MigrateBind::Bool(b) => q.bind(b),
            MigrateBind::I64(i) => q.bind(i),
            MigrateBind::Json(v) => q.bind(sqlx::types::Json(v)),
        };
    }
    let rows = q.fetch_all(&mut **tx).await?;
    Ok(rows.into_iter().map(|r| r.0).collect())
}

/// Runs an execute-only statement with the given binds (the UPDATE in
/// `apply_eval_expr`'s typed path). Every `MigrateBind` becomes a bound `$n`,
/// so no `ValueExpr` literal is ever interpolated into the SQL string.
pub(super) async fn bind_execute(
    sql: &str,
    binds: &[MigrateBind],
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), RtDbError> {
    let mut q = sqlx::query(sqlx::AssertSqlSafe(sql.to_owned()));
    for b in binds {
        q = match b {
            MigrateBind::Text(s) => q.bind(s),
            MigrateBind::Num(f) => q.bind(f),
            MigrateBind::Bool(b) => q.bind(b),
            MigrateBind::I64(i) => q.bind(i),
            MigrateBind::Json(v) => q.bind(sqlx::types::Json(v)),
        };
    }
    q.execute(&mut **tx).await?;
    Ok(())
}

pub(super) fn push_ops(ops: &mut Vec<DocOp>, table: &str, ids: &[String], kind: OpKind) {
    for id in ids {
        ops.push(DocOp {
            table: table.into(),
            id: id.clone(),
            kind,
        });
    }
}

/// Pure Rust coercion mirroring the SQL cast, used to decide default-vs-fail
/// per row without relying on a Postgres exception. Returns `None` if the
/// value cannot be coerced under this cast; the caller then substitutes
/// `default` if supplied or raises a row-named `BadRequest`.
///
/// `ToInt64` emits a decimal-string JSON value (int64 travels as a canonical
/// decimal string on this wire — see `schema::is_valid_int64` and
/// `FEATURE_MATRIX.md` #13); `ToNumber` emits a JSON number. The other casts
/// produce the natural JSON representation.
pub(super) fn coerce_value(cast: Cast, v: &serde_json::Value) -> Option<serde_json::Value> {
    use serde_json::Value;
    match (cast, v) {
        (Cast::ToString, Value::String(_)) => Some(v.clone()),
        (Cast::ToString, Value::Number(n)) => Some(Value::String(n.to_string())),
        (Cast::ToString, Value::Bool(b)) => Some(Value::String(b.to_string())),
        (Cast::ToString, _) => None,
        (Cast::ToNumber, Value::String(s)) => match s.parse::<f64>() {
            Ok(n) if n.is_finite() => Some(serde_json::json!(n)),
            _ => None,
        },
        (Cast::ToNumber, Value::Number(_)) => Some(v.clone()),
        (Cast::ToNumber, Value::Bool(b)) => Some(serde_json::json!(if *b { 1.0 } else { 0.0 })),
        (Cast::ToNumber, _) => None,
        (Cast::ToInt64, Value::String(s)) => s
            .parse::<i64>()
            .ok()
            .map(|i| serde_json::json!(i.to_string())),
        (Cast::ToInt64, Value::Number(n)) => n.as_i64().map(|i| serde_json::json!(i.to_string())),
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
