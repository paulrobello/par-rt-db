use std::collections::BTreeSet;

use sqlx::{PgConnection, PgPool};

use crate::db::{new_id, now_ms, validate_db_name};
use crate::ddl::{pg_col, pg_schema, pg_table};
use crate::error::RtDbError;
use crate::schema::{
    FieldType, IndexDef, SchemaDef, TableDef, indexed_column_type, validate_doc, validate_value,
};

const MAX_STEPS: usize = 256;

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
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Transaction {
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TxnOutcome {
    pub results: Vec<serde_json::Value>,
    pub write_set: BTreeSet<String>,
}

/// SQL bind for an eq-lookup value, typed per the index field's `FieldType`
/// (`Optional` unwrapped). Prefix-friendly: callers may supply 0..=all of an
/// index's fields; full-arity enforcement is the caller's responsibility.
pub(crate) enum EqBind {
    Text(String),
    Num(f64),
    Bool(bool),
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

fn eq_bind_for(ty: &FieldType, value: &serde_json::Value) -> Result<EqBind, RtDbError> {
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
}

/// Every field referenced by any index on `table`, paired with its type, in
/// a stable order. These are exactly the indexed-column values that must be
/// extracted from a document on insert/patch/upsert.
fn table_columns(table: &TableDef) -> Result<Vec<(String, FieldType)>, RtDbError> {
    let mut names: BTreeSet<String> = BTreeSet::new();
    for index in &table.indexes {
        for field_name in &index.fields {
            names.insert(field_name.clone());
        }
    }

    names
        .into_iter()
        .map(|name| {
            let ty = table.fields.get(&name).cloned().ok_or_else(|| {
                RtDbError::internal(format!("index references unknown field '{name}'"))
            })?;
            Ok((name, ty))
        })
        .collect()
}

/// Extracts one SQL bind per `columns` entry from `doc`, shared by
/// insert/patch/upsert so every indexed column is always recomputed the
/// same way from the merged document.
fn column_binds(
    columns: &[(String, FieldType)],
    doc: &serde_json::Map<String, serde_json::Value>,
) -> Result<Vec<ColBind>, RtDbError> {
    columns
        .iter()
        .map(|(name, ty)| {
            let value = doc.get(name).cloned().unwrap_or(serde_json::Value::Null);
            column_bind_for(ty, &value)
        })
        .collect()
}

fn column_bind_for(ty: &FieldType, value: &serde_json::Value) -> Result<ColBind, RtDbError> {
    let (pg_type, _nullable) = indexed_column_type(ty)?;
    if value.is_null() {
        return match pg_type {
            "text" => Ok(ColBind::Text(None)),
            "double precision" => Ok(ColBind::Num(None)),
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
/// now_ms()`, `version` defaulting to 1. Returns the generated id.
async fn do_insert(
    conn: &mut PgConnection,
    pg_schema_name: &str,
    table_def: &TableDef,
    table_name: &str,
    doc: &serde_json::Map<String, serde_json::Value>,
) -> Result<String, RtDbError> {
    validate_doc(table_def, doc)?;
    let doc = strip_unset_optionals(table_def, doc.clone());
    let doc = &doc;

    let id = new_id();
    let created_at = now_ms();
    let columns = table_columns(table_def)?;
    let binds = column_binds(&columns, doc)?;

    let table_ident = pg_table(table_name);
    let mut col_names = vec![
        "\"id\"".to_string(),
        "\"doc\"".to_string(),
        "\"created_at\"".to_string(),
    ];
    for (name, _) in &columns {
        col_names.push(format!("\"{}\"", pg_col(name)));
    }
    let placeholders: Vec<String> = (1..=col_names.len()).map(|i| format!("${i}")).collect();

    let sql = format!(
        "INSERT INTO \"{pg_schema_name}\".\"{table_ident}\" ({}) VALUES ({})",
        col_names.join(", "),
        placeholders.join(", ")
    );

    let doc_value = serde_json::Value::Object(doc.clone());
    let mut query = sqlx::query(&sql)
        .bind(id.clone())
        .bind(doc_value)
        .bind(created_at);
    for bind in binds {
        query = match bind {
            ColBind::Text(v) => query.bind(v),
            ColBind::Num(v) => query.bind(v),
            ColBind::Bool(v) => query.bind(v),
        };
    }
    query.execute(&mut *conn).await?;
    Ok(id)
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
    merged: serde_json::Map<String, serde_json::Value>,
) -> Result<(), RtDbError> {
    let table_ident = pg_table(table_name);
    let columns = table_columns(table_def)?;
    let binds = column_binds(&columns, &merged)?;

    let mut set_clauses = vec![
        "\"doc\" = $1".to_string(),
        "\"version\" = \"version\" + 1".to_string(),
    ];
    let mut idx = 2usize;
    for (name, _) in &columns {
        set_clauses.push(format!("\"{}\" = ${idx}", pg_col(name)));
        idx += 1;
    }
    let id_placeholder = idx;

    let sql = format!(
        "UPDATE \"{pg_schema_name}\".\"{table_ident}\" SET {} WHERE \"id\" = ${id_placeholder}",
        set_clauses.join(", ")
    );

    let doc_value = serde_json::Value::Object(merged);
    let mut query = sqlx::query(&sql).bind(doc_value);
    for bind in binds {
        query = match bind {
            ColBind::Text(v) => query.bind(v),
            ColBind::Num(v) => query.bind(v),
            ColBind::Bool(v) => query.bind(v),
        };
    }
    query = query.bind(id.to_string());
    query.execute(&mut *conn).await?;
    Ok(())
}

/// Fetches the current doc by id (`NotFound` if missing), merges `fields`
/// onto it via `apply_patch`, and applies the update.
async fn do_patch(
    conn: &mut PgConnection,
    pg_schema_name: &str,
    table_def: &TableDef,
    table_name: &str,
    id: &str,
    fields: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), RtDbError> {
    let table_ident = pg_table(table_name);
    let row: Option<(serde_json::Value,)> = sqlx::query_as(&format!(
        "SELECT \"doc\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE \"id\" = $1"
    ))
    .bind(id)
    .fetch_optional(&mut *conn)
    .await?;

    let (doc_value,) =
        row.ok_or_else(|| RtDbError::not_found(format!("document '{id}' not found")))?;
    let doc = match doc_value {
        serde_json::Value::Object(map) => map,
        _ => return Err(RtDbError::internal("stored doc is not a JSON object")),
    };

    let merged = apply_patch(table_def, doc, fields)?;
    apply_update(conn, pg_schema_name, table_def, table_name, id, merged).await
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
    table_name: &str,
    id: &str,
    expected: i64,
) -> Result<(), RtDbError> {
    let table_ident = pg_table(table_name);
    let row: Option<(i64,)> = sqlx::query_as(&format!(
        "SELECT \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE \"id\" = $1"
    ))
    .bind(id)
    .fetch_optional(&mut *conn)
    .await?;
    let (actual,) =
        row.ok_or_else(|| RtDbError::not_found(format!("document '{id}' not found")))?;
    if actual != expected {
        return Err(RtDbError::precondition(format!(
            "version mismatch: expected {expected}, actual {actual}"
        )));
    }
    Ok(())
}

/// Looks up rows matching `eq` on `index` (full arity required: a
/// `BadRequest` otherwise), returning `(id, doc)` pairs. Shared by
/// `ExpectAbsent` and `Upsert`.
async fn eq_lookup(
    conn: &mut PgConnection,
    pg_schema_name: &str,
    table_def: &TableDef,
    table_name: &str,
    index_name: &str,
    eq: &[serde_json::Value],
) -> Result<Vec<(String, serde_json::Value)>, RtDbError> {
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
        "SELECT \"id\", \"doc\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE {}",
        where_clause.join(" AND ")
    );

    let mut query = sqlx::query_as::<_, (String, serde_json::Value)>(&sql);
    for bind in binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
        };
    }
    let rows = query.fetch_all(&mut *conn).await?;
    Ok(rows)
}

/// Executes all of `txn`'s steps in one Postgres transaction; any step's
/// error aborts and rolls back everything already applied. See module docs
/// on `Step` for per-step semantics.
///
/// Runs under READ COMMITTED with no row locking; correctness depends on all
/// writes for a database being serialized through the per-db committer.
/// Never call `execute_txn` from a non-committer production path.
pub async fn execute_txn(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    txn: &Transaction,
) -> Result<TxnOutcome, RtDbError> {
    validate_db_name(db)?;

    if txn.steps.len() > MAX_STEPS {
        return Err(RtDbError::bad_request(format!(
            "transaction exceeds maximum of {MAX_STEPS} steps"
        )));
    }

    let pg_schema_name = pg_schema(db);
    let mut results = Vec::with_capacity(txn.steps.len());
    let mut write_set = BTreeSet::new();

    let mut tx = pool.begin().await?;

    for step in &txn.steps {
        match step {
            Step::Insert { table, doc } => {
                let table_def = schema.table(table)?;
                let id = do_insert(&mut tx, &pg_schema_name, table_def, table, doc).await?;
                write_set.insert(table.clone());
                results.push(serde_json::json!({ "id": id }));
            }
            Step::Patch { table, id, fields } => {
                let table_def = schema.table(table)?;
                do_patch(&mut tx, &pg_schema_name, table_def, table, id, fields).await?;
                write_set.insert(table.clone());
                results.push(serde_json::Value::Null);
            }
            Step::Delete { table, id } => {
                schema.table(table)?;
                do_delete(&mut tx, &pg_schema_name, table, id).await?;
                write_set.insert(table.clone());
                results.push(serde_json::Value::Null);
            }
            Step::ExpectVersion { table, id, version } => {
                schema.table(table)?;
                do_expect_version(&mut tx, &pg_schema_name, table, id, *version).await?;
                results.push(serde_json::Value::Null);
            }
            Step::ExpectAbsent { table, index, eq } => {
                let table_def = schema.table(table)?;
                let rows = eq_lookup(&mut tx, &pg_schema_name, table_def, table, index, eq).await?;
                if !rows.is_empty() {
                    return Err(RtDbError::precondition(format!(
                        "index '{index}' already has a matching document"
                    )));
                }
                results.push(serde_json::Value::Null);
            }
            Step::Upsert {
                table,
                index,
                eq,
                insert,
                patch,
            } => {
                let table_def = schema.table(table)?;
                let mut rows =
                    eq_lookup(&mut tx, &pg_schema_name, table_def, table, index, eq).await?;
                if rows.len() > 1 {
                    return Err(RtDbError::precondition("upsert matched multiple documents"));
                }
                match rows.pop() {
                    None => {
                        let id =
                            do_insert(&mut tx, &pg_schema_name, table_def, table, insert).await?;
                        write_set.insert(table.clone());
                        results.push(serde_json::json!({ "id": id, "inserted": true }));
                    }
                    Some((id, doc_value)) => {
                        let doc = match doc_value {
                            serde_json::Value::Object(map) => map,
                            _ => {
                                return Err(RtDbError::internal("stored doc is not a JSON object"));
                            }
                        };
                        let merged = apply_patch(table_def, doc, patch)?;
                        apply_update(&mut tx, &pg_schema_name, table_def, table, &id, merged)
                            .await?;
                        write_set.insert(table.clone());
                        results.push(serde_json::json!({ "id": id, "inserted": false }));
                    }
                }
            }
        }
    }

    tx.commit().await?;
    Ok(TxnOutcome { results, write_set })
}
