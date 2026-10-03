//! The per-step step_* handlers execute_txn dispatches to, the shared StepCtx borrow bundle, and the moved unit tests (ARC-008 split; pure move from the former single-file `txn.rs`).

use std::collections::HashSet;

use sqlx::PgConnection;

use crate::auth::PrincipalCtx;
use crate::db::now_ms;
use crate::ddl::pg_table;
use crate::error::{ErrorCode, RtDbError};
use crate::schema::{FieldType, SchemaDef, SchemaDefExt};
use crate::txn::{
    EqBind, MAX_BY_QUERY_ROWS, OpKind, WriteSet, apply_defaults, apply_patch, check_owner,
    check_owner_doc, delete_row_cascade, doc_visible_to, stamp_authorize, stamp_auto_increment,
    stamp_owner, stamp_ttl_default, stamp_updated_at, verify_authorize_doc,
};
use crate::txn::{
    apply_update, do_expect_version, do_insert, do_patch, do_replace, do_soft_delete, eq_lookup,
};

// ARC-202: the wire types this module used to define live in `dsl.rs` now;
// re-exported so every `crate::txn::` path (and the integration tests'
// `rtdb_server::txn::` paths) keep resolving unchanged.
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
pub(crate) struct StepCtx<'a> {
    pub(crate) tx: &'a mut PgConnection,
    pub(crate) db: &'a str,
    pub(crate) pg_schema_name: &'a str,
    pub(crate) schema: &'a SchemaDef,
    pub(crate) ctx: &'a PrincipalCtx,
    pub(crate) owner: Option<&'a str>,
    pub(crate) write_set: &'a mut WriteSet,
    pub(crate) results: &'a mut Vec<serde_json::Value>,
}
pub(crate) async fn step_insert(
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
    let doc = stamp_auto_increment(sctx.tx, sctx.pg_schema_name, table_def, table, doc).await?;
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

pub(crate) async fn step_patch(
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

const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;

fn expected_json_eq(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    match (left, right) {
        (serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
            let integer = |number: &serde_json::Number| {
                number
                    .as_i64()
                    .map(i128::from)
                    .or_else(|| number.as_u64().map(i128::from))
                    .or_else(|| {
                        let value = number.as_f64()?;
                        (value.is_finite()
                            && value.fract() == 0.0
                            && value.abs() < 2.0_f64.powi(127))
                        .then_some(value as i128)
                    })
            };
            match (integer(a), integer(b)) {
                (Some(a), Some(b)) => a == b,
                _ => a.as_f64() == b.as_f64(),
            }
        }
        (serde_json::Value::Array(a), serde_json::Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| expected_json_eq(a, b))
        }
        (serde_json::Value::Object(a), serde_json::Value::Object(b)) => {
            a.len() == b.len()
                && a.iter().all(|(key, value)| {
                    b.get(key)
                        .is_some_and(|other| expected_json_eq(value, other))
                })
        }
        _ => left == right,
    }
}

fn counter_value(
    field_type: &FieldType,
    value: &serde_json::Value,
    field: &str,
) -> Result<i64, RtDbError> {
    let n = match field_type {
        FieldType::Number => value
            .as_f64()
            .filter(|n| n.is_finite() && n.fract() == 0.0)
            .and_then(|n| {
                (n >= -(MAX_SAFE_INTEGER as f64) && n <= MAX_SAFE_INTEGER as f64)
                    .then_some(n as i64)
            }),
        FieldType::Optional { inner } if matches!(inner.as_ref(), FieldType::Number) => {
            return counter_value(inner, value, field);
        }
        _ => None,
    };
    n.ok_or_else(|| {
        RtDbError::bad_request(format!(
            "counter field '{field}' must contain a safe integer"
        ))
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn step_adjust_counter(
    sctx: &mut StepCtx<'_>,
    table: &str,
    id: &str,
    field: &str,
    delta: i64,
    min: Option<i64>,
    max: Option<i64>,
    expected: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Result<(), RtDbError> {
    let table_def = sctx.schema.table(table)?;
    if delta.unsigned_abs() > MAX_SAFE_INTEGER as u64
        || min.is_some_and(|n| n.unsigned_abs() > MAX_SAFE_INTEGER as u64)
        || max.is_some_and(|n| n.unsigned_abs() > MAX_SAFE_INTEGER as u64)
        || min.zip(max).is_some_and(|(lo, hi)| lo > hi)
    {
        return Err(RtDbError::bad_request(
            "counter delta and bounds must be safe integers with min <= max",
        ));
    }
    let field_type = table_def
        .fields
        .get(field)
        .ok_or_else(|| RtDbError::schema(format!("unknown field '{field}'")))?;
    if table_def.computed.contains_key(field) {
        return Err(RtDbError::bad_request(format!(
            "computed field '{field}' cannot be adjusted"
        )));
    }
    if table_def.auto_increment_field.as_deref() == Some(field) {
        return Err(RtDbError::bad_request(format!(
            "autoIncrementField '{field}' cannot be changed"
        )));
    }
    if table_def.updated_at_field.as_deref() == Some(field) {
        return Err(RtDbError::bad_request(format!(
            "updatedAtField '{field}' cannot be adjusted"
        )));
    }
    let numeric = matches!(field_type, FieldType::Number)
        || matches!(field_type, FieldType::Optional { inner } if matches!(inner.as_ref(), FieldType::Number));
    if !numeric {
        return Err(RtDbError::schema(format!(
            "counter field '{field}' must be number or optional(number)"
        )));
    }
    check_owner(sctx.tx, sctx.pg_schema_name, table_def, table, id, sctx.ctx).await?;
    let table_ident = pg_table(table);
    let live_only = if table_def.soft_delete {
        " AND \"deleted_at\" IS NULL"
    } else {
        ""
    };
    let row: Option<serde_json::Value> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT \"doc\" FROM \"{}\".\"{}\" WHERE \"id\" = $1{live_only} FOR UPDATE",
        sctx.pg_schema_name, table_ident
    )))
    .bind(id)
    .fetch_optional(&mut *sctx.tx)
    .await?;
    let doc = row.ok_or_else(|| RtDbError::not_found(format!("document '{id}' not found")))?;
    let doc = doc
        .as_object()
        .ok_or_else(|| RtDbError::internal("stored doc is not a JSON object"))?;
    if let Some(expected) = expected {
        for (key, wanted) in expected {
            if !table_def.fields.contains_key(key) {
                return Err(RtDbError::schema(format!("unknown expected field '{key}'")));
            }
            if !doc
                .get(key)
                .is_some_and(|actual| expected_json_eq(actual, wanted))
            {
                return Err(RtDbError::new(
                    ErrorCode::PreconditionFailed,
                    format!("expected field '{key}' did not match"),
                ));
            }
        }
    }
    let current_json = doc
        .get(field)
        .ok_or_else(|| RtDbError::bad_request(format!("counter field '{field}' is missing")))?;
    let current = counter_value(field_type, current_json, field)?;
    let next = current
        .checked_add(delta)
        .filter(|n| n.unsigned_abs() <= MAX_SAFE_INTEGER as u64)
        .ok_or_else(|| {
            RtDbError::bad_request("counter result is outside the safe integer range")
        })?;
    if min.is_some_and(|n| next < n) || max.is_some_and(|n| next > n) {
        return Err(RtDbError::new(
            ErrorCode::PreconditionFailed,
            "counter result is outside the configured bounds",
        ));
    }
    let next = serde_json::json!(next);
    let fields = serde_json::Map::from_iter([(field.to_string(), next)]);
    let fields = stamp_owner(table_def, fields, sctx.owner);
    let fields = stamp_authorize(table_def, fields, sctx.ctx);
    let fields = stamp_updated_at(table_def, fields, now_ms());
    let (pre_doc, merged, created_at) =
        do_patch(sctx.tx, sctx.pg_schema_name, table_def, table, id, &fields).await?;
    verify_authorize_doc(table_def, &merged, sctx.ctx)?;
    sctx.write_set.touch(table, id, OpKind::Patch);
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

pub(crate) async fn step_replace(
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

pub(crate) async fn step_delete(
    sctx: &mut StepCtx<'_>,
    table: &str,
    id: &str,
) -> Result<(), RtDbError> {
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

pub(crate) async fn step_expect_version(
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

pub(crate) async fn step_expect_absent(
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

pub(crate) async fn step_upsert(
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
            let insert =
                stamp_auto_increment(sctx.tx, sctx.pg_schema_name, table_def, table, insert)
                    .await?;
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

pub(crate) async fn step_patch_by_query(
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
    let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64)>(sqlx::AssertSqlSafe(sql));
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

pub(crate) async fn step_delete_by_query(
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
    let mut query = sqlx::query_as::<_, (String,)>(sqlx::AssertSqlSafe(sql));
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
/// `Undelete` step (FM-33): restore a soft-deleted row — `deleted_at = NULL`,
/// `version` + 1 (a stale client copy fails OCC against the restored row).
/// `NotFound` when absent; idempotent `Ok` when the row is present and already
/// live. `BadRequest` on a table that does not declare `softDelete`. The doc
/// body is untouched (a soft delete never modified it), so the restored row
/// re-appears byte-identical minus `deleted_at`.
pub(crate) async fn step_undelete(
    sctx: &mut StepCtx<'_>,
    table: &str,
    id: &str,
) -> Result<(), RtDbError> {
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
    let row: Option<(serde_json::Value, i64, bool)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT \"doc\", \"created_at\", (\"deleted_at\" IS NULL) AS live \
         FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE \"id\" = $1"
    )))
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
    let result = sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE \"{pg_schema_name}\".\"{table_ident}\" \
         SET \"deleted_at\" = NULL, \"version\" = \"version\" + 1 \
         WHERE \"id\" = $1 AND \"deleted_at\" IS NOT NULL"
    )))
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
#[cfg(test)]
mod tests {
    use super::*;
    use crate::txn::{OpKind, Step, Transaction, count_steps};

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
                    external: None,
                    when: crate::protocol::ScheduleWhen::AfterMs { ms: 1 },
                    txn: Box::new(Transaction {
                        steps: vec![
                            Step::Delete {
                                table: "t".to_string(),
                                id: "x".to_string(),
                            },
                            Step::Schedule {
                                external: None,
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
