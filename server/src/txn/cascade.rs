//! App-level onDelete cascade machinery: on_delete_ref, has_on_delete_children, visible_child_ids, delete_row_cascade (ARC-008 split; pure move from the former single-file `txn.rs`).

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;

use sqlx::PgConnection;

use crate::db::now_ms;
use crate::ddl::{pg_col, pg_table};
use crate::error::RtDbError;
use crate::schema::{FieldType, OnDeleteAction, SchemaDef, SchemaDefExt, TableDef};
use crate::txn::{MAX_CASCADE_ROWS, OpKind, WriteSet, do_patch, do_soft_delete, stamp_updated_at};

// ARC-202: the wire types this module used to define live in `dsl.rs` now;
// re-exported so every `crate::txn::` path (and the integration tests'
// `rtdb_server::txn::` paths) keep resolving unchanged.
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
                                "cannot delete '{table_name}': '{child_table_name}.{field_name}'  \
                                is referenced by document '{child_id}'"
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
