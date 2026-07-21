use std::collections::{BTreeSet, HashSet};

use sqlx::PgPool;

use crate::db::{load_schema, validate_db_name};
use crate::error::RtDbError;
use crate::schema::{FieldType, SchemaDef, TableDef, indexed_column_type};

pub fn pg_table(user_table: &str) -> String {
    format!("t_{}", user_table.to_lowercase())
}

pub fn pg_col(field: &str) -> String {
    format!("f_{}", field.to_lowercase())
}

pub fn pg_schema(db: &str) -> String {
    format!("db_{db}")
}

/// Union of every field referenced by any index on `table`.
fn indexed_fields(table: &TableDef) -> BTreeSet<String> {
    table
        .indexes
        .iter()
        .flat_map(|index| index.fields.iter().cloned())
        .collect()
}

fn field_type<'a>(table: &'a TableDef, field_name: &str) -> Result<&'a FieldType, RtDbError> {
    table.fields.get(field_name).ok_or_else(|| {
        RtDbError::internal(format!("index references unknown field '{field_name}'"))
    })
}

/// Backfill expression casting the document's JSON value for `field_name` to `pg_type`.
fn backfill_expr(pg_type: &str, field_name: &str) -> Result<String, RtDbError> {
    match pg_type {
        "text" => Ok(format!("(doc->>'{field_name}')")),
        "double precision" => Ok(format!("(doc->>'{field_name}')::float8")),
        "boolean" => Ok(format!("(doc->>'{field_name}')::boolean")),
        other => Err(RtDbError::internal(format!(
            "unsupported backfill cast for pg type '{other}'"
        ))),
    }
}

/// Compares `old` to `new` and rejects any destructive change: a removed table, a
/// removed field, a changed field type, a removed index, or a changed index field
/// list. Errors name the offending table, `table.field`, or index.
fn detect_destructive_changes(old: &SchemaDef, new: &SchemaDef) -> Result<(), RtDbError> {
    for (table_name, old_table) in &old.tables {
        let new_table = new
            .tables
            .get(table_name)
            .ok_or_else(|| RtDbError::bad_request(format!("removed table '{table_name}'")))?;

        for (field_name, old_field_type) in &old_table.fields {
            match new_table.fields.get(field_name) {
                None => {
                    return Err(RtDbError::bad_request(format!(
                        "removed field '{table_name}.{field_name}'"
                    )));
                }
                Some(new_field_type) if new_field_type != old_field_type => {
                    return Err(RtDbError::bad_request(format!(
                        "changed type of field '{table_name}.{field_name}'"
                    )));
                }
                _ => {}
            }
        }

        for old_index in &old_table.indexes {
            match new_table
                .indexes
                .iter()
                .find(|index| index.name == old_index.name)
            {
                None => {
                    return Err(RtDbError::bad_request(format!(
                        "removed index '{}'",
                        old_index.name
                    )));
                }
                Some(new_index) if new_index.fields != old_index.fields => {
                    return Err(RtDbError::bad_request(format!(
                        "changed fields of index '{}'",
                        old_index.name
                    )));
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// Validates `schema`, diffs it against whatever is currently pushed for `db`
/// (rejecting destructive changes), and applies the additive DDL — new tables,
/// new indexed-field columns with backfill, and new indexes — plus the `meta`
/// upsert, all inside a single transaction. Returns the applied schema.
pub async fn push_schema(
    pool: &PgPool,
    db: &str,
    schema: SchemaDef,
) -> Result<SchemaDef, RtDbError> {
    schema.validate()?;
    validate_db_name(db)?;

    let previous = load_schema(pool, db).await?;
    if let Some(old_schema) = &previous {
        detect_destructive_changes(old_schema, &schema)?;
    }

    let pg_schema_name = pg_schema(db);
    let mut tx = pool.begin().await?;

    for (table_name, new_table) in &schema.tables {
        let old_table = previous.as_ref().and_then(|s| s.tables.get(table_name));
        let table_ident = pg_table(table_name);
        let new_indexed = indexed_fields(new_table);

        match old_table {
            None => {
                let mut columns = vec![
                    "\"id\" text PRIMARY KEY".to_string(),
                    "\"doc\" jsonb NOT NULL".to_string(),
                    "\"created_at\" bigint NOT NULL".to_string(),
                    "\"version\" bigint NOT NULL DEFAULT 1".to_string(),
                ];
                for field_name in &new_indexed {
                    let ty = field_type(new_table, field_name)?;
                    let (pg_type, nullable) = indexed_column_type(ty)?;
                    let col = pg_col(field_name);
                    let not_null = if nullable { "" } else { " NOT NULL" };
                    columns.push(format!("\"{col}\" {pg_type}{not_null}"));
                }
                let sql = format!(
                    "CREATE TABLE \"{pg_schema_name}\".\"{table_ident}\" ({})",
                    columns.join(", ")
                );
                sqlx::query(&sql).execute(&mut *tx).await?;
            }
            Some(old_table) => {
                let old_indexed = indexed_fields(old_table);
                for field_name in new_indexed.difference(&old_indexed) {
                    let ty = field_type(new_table, field_name)?;
                    let (pg_type, _nullable) = indexed_column_type(ty)?;
                    let col = pg_col(field_name);

                    sqlx::query(&format!(
                        "ALTER TABLE \"{pg_schema_name}\".\"{table_ident}\" ADD COLUMN \"{col}\" {pg_type}"
                    ))
                    .execute(&mut *tx)
                    .await?;

                    let expr = backfill_expr(pg_type, field_name)?;
                    sqlx::query(&format!(
                        "UPDATE \"{pg_schema_name}\".\"{table_ident}\" SET \"{col}\" = {expr} WHERE doc ? '{field_name}'"
                    ))
                    .execute(&mut *tx)
                    .await?;
                }
            }
        }

        let old_index_names: HashSet<&str> = old_table
            .map(|t| t.indexes.iter().map(|index| index.name.as_str()).collect())
            .unwrap_or_default();
        for index in &new_table.indexes {
            if old_index_names.contains(index.name.as_str()) {
                continue;
            }
            let index_ident = format!(
                "i_{}_{}",
                table_name.to_lowercase(),
                index.name.to_lowercase()
            );
            let cols: Vec<String> = index
                .fields
                .iter()
                .map(|field_name| format!("\"{}\"", pg_col(field_name)))
                .collect();
            sqlx::query(&format!(
                "CREATE INDEX \"{index_ident}\" ON \"{pg_schema_name}\".\"{table_ident}\" ({}, \"created_at\")",
                cols.join(", ")
            ))
            .execute(&mut *tx)
            .await?;
        }
    }

    let schema_json =
        serde_json::to_value(&schema).map_err(|err| RtDbError::internal(err.to_string()))?;
    sqlx::query(&format!(
        "INSERT INTO \"{pg_schema_name}\".meta (key, value) VALUES ('schema', $1) \
         ON CONFLICT (key) DO UPDATE SET value = excluded.value"
    ))
    .bind(schema_json)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(schema)
}
