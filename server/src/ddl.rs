use std::collections::{BTreeSet, HashSet};

use sqlx::PgPool;

use crate::db::{database_exists, load_schema, validate_db_name};
use crate::error::RtDbError;
use crate::query::compile_filter_literal;
use crate::schema::{FieldType, SchemaDef, TableDef, indexed_column_type, is_widening_of};

pub fn pg_table(user_table: &str) -> String {
    format!("t_{}", user_table.to_lowercase())
}

pub fn pg_col(field: &str) -> String {
    format!("f_{}", field.to_lowercase())
}

/// Physical name of a search index's generated `tsvector` column. Columns are
/// table-scoped (no table prefix needed), so `s_` + the lowercased index name
/// stays well within Postgres's 63-byte identifier limit.
pub fn pg_search_col(index_name: &str) -> String {
    format!("s_{}", index_name.to_lowercase())
}

/// Physical name of a vector index's `vector(N)` column. Table-scoped, so `v_`
/// + the lowercased index name stays within Postgres's 63-byte identifier limit.
pub fn pg_vector_col(index_name: &str) -> String {
    format!("v_{}", index_name.to_lowercase())
}

pub fn pg_schema(db: &str) -> String {
    format!("db_{db}")
}

/// Union of every field referenced by any index on `table` that should get a
/// typed `f_` column. A btree or search index contributes all of its `fields`;
/// a vector index contributes only its `filter_fields` — its single vector
/// field is owned by the write-maintained `v_` column, not a typed column.
pub(crate) fn indexed_fields(table: &TableDef) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for index in &table.indexes {
        if let Some(vec_spec) = &index.vector {
            for ff in &vec_spec.filter_fields {
                names.insert(ff.clone());
            }
        } else {
            for field_name in &index.fields {
                names.insert(field_name.clone());
            }
        }
    }
    names
}

fn field_type<'a>(table: &'a TableDef, field_name: &str) -> Result<&'a FieldType, RtDbError> {
    table.fields.get(field_name).ok_or_else(|| {
        RtDbError::internal(format!("index references unknown field '{field_name}'"))
    })
}

/// Backfill expression casting the document's JSON value for `field_name` to `pg_type`.
pub(crate) fn backfill_expr(pg_type: &str, field_name: &str) -> Result<String, RtDbError> {
    match pg_type {
        "text" => Ok(format!("(doc->>'{field_name}')")),
        "double precision" => Ok(format!("(doc->>'{field_name}')::float8")),
        "bigint" => Ok(format!("(doc->>'{field_name}')::bigint")),
        "boolean" => Ok(format!("(doc->>'{field_name}')::boolean")),
        other => Err(RtDbError::internal(format!(
            "unsupported backfill cast for pg type '{other}'"
        ))),
    }
}

/// Compares `old` to `new` and rejects any destructive change: a removed table,
/// a removed field, a changed field type (except a safe literal-union widening,
/// which is additive and allowed — see `schema::is_widening_of`), a removed
/// index, or a changed index field list. Errors name the offending table,
/// `table.field`, or index.
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
                Some(new_field_type)
                    if new_field_type != old_field_type
                        && !is_widening_of(old_field_type, new_field_type) =>
                {
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
                Some(new_index) if new_index.search != old_index.search => {
                    return Err(RtDbError::bad_request(format!(
                        "changed kind of index '{}' (btree <-> search)",
                        old_index.name
                    )));
                }
                Some(new_index) if new_index.vector != old_index.vector => {
                    return Err(RtDbError::bad_request(format!(
                        "changed vector spec of index '{}'",
                        old_index.name
                    )));
                }
                Some(new_index) if new_index.unique != old_index.unique => {
                    return Err(RtDbError::bad_request(format!(
                        "changed uniqueness of index '{}'",
                        old_index.name
                    )));
                }
                Some(new_index) if new_index.r#where != old_index.r#where => {
                    return Err(RtDbError::bad_request(format!(
                        "changed partial predicate of index '{}'",
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

    if !database_exists(pool, db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }

    let previous = load_schema(pool, db).await?;
    if let Some(old_schema) = &previous {
        detect_destructive_changes(old_schema, &schema)?;
    }

    let pg_schema_name = pg_schema(db);
    let mut tx = pool.begin().await?;

    // Covers databases created before pgvector shipped: ensure the extension is
    // present the first time a vector-index schema is pushed. No-op if already
    // installed (Task 1 installs it at database creation).
    sqlx::query("CREATE EXTENSION IF NOT EXISTS vector")
        .execute(&mut *tx)
        .await?;

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
            if index.search {
                // A full-text search index: a generated `tsvector` column over
                // its text fields plus a GIN index on it. The referenced
                // `f_<field>` typed columns are created with the table (new) or
                // added and backfilled just above (existing), so they already
                // exist by this point. `to_tsvector(regconfig, text)` is
                // immutable, so it is allowed in a STORED generated column.
                let sv_col = pg_search_col(&index.name);
                let terms: Vec<String> = index
                    .fields
                    .iter()
                    .map(|field_name| format!("coalesce(\"{}\", '')", pg_col(field_name)))
                    .collect();
                sqlx::query(&format!(
                    "ALTER TABLE \"{pg_schema_name}\".\"{table_ident}\" \
                     ADD COLUMN \"{sv_col}\" tsvector GENERATED ALWAYS AS \
                     (to_tsvector('english', {})) STORED",
                    terms.join(" || ' ' || ")
                ))
                .execute(&mut *tx)
                .await?;
                sqlx::query(&format!(
                    "CREATE INDEX \"{index_ident}\" ON \"{pg_schema_name}\".\"{table_ident}\" \
                     USING GIN (\"{sv_col}\")"
                ))
                .execute(&mut *tx)
                .await?;
            } else if let Some(vec_spec) = &index.vector {
                // Vector index: a plain `vector(N)` column (write-maintained by
                // Task 5, not generated — pgvector has no jsonb->vector generated
                // cast) plus an HNSW cosine index. The filterFields' `f_` columns
                // already exist (created with the table / added+backfilled above).
                let v_col = pg_vector_col(&index.name);
                let dim = vec_spec.dimensions;
                let vfield = index
                    .fields
                    .first()
                    .ok_or_else(|| RtDbError::internal("vector index missing its field"))?;
                sqlx::query(&format!(
                    "ALTER TABLE \"{pg_schema_name}\".\"{table_ident}\" \
                     ADD COLUMN \"{v_col}\" vector({dim})"
                ))
                .execute(&mut *tx)
                .await?;
                // Backfill from existing rows (no-op on a brand-new table).
                // `vfield` is a doc field name validated by is_valid_identifier
                // in Task 3, and lives in a string literal here, not an identifier.
                sqlx::query(&format!(
                    "UPDATE \"{pg_schema_name}\".\"{table_ident}\" \
                     SET \"{v_col}\" = (doc->>'{vfield}')::vector \
                     WHERE doc ? '{vfield}'"
                ))
                .execute(&mut *tx)
                .await?;
                sqlx::query(&format!(
                    "CREATE INDEX \"{index_ident}\" ON \"{pg_schema_name}\".\"{table_ident}\" \
                     USING hnsw (\"{v_col}\" vector_cosine_ops)"
                ))
                .execute(&mut *tx)
                .await?;
            } else {
                let cols: Vec<String> = index
                    .fields
                    .iter()
                    .map(|field_name| format!("\"{}\"", pg_col(field_name)))
                    .collect();

                // Partial-index predicate (literal SQL — see
                // `compile_filter_literal`). `Option<String>` already carries a
                // leading " WHERE " when present; shadow-bind a `&str` for
                // interpolation (`Option` has no `Display` impl).
                let where_sql: Option<String> = match &index.r#where {
                    Some(pred) => {
                        let frag = compile_filter_literal(pred, new_table)?;
                        Some(format!(" WHERE {frag}"))
                    }
                    None => None,
                };
                let where_sql = where_sql.as_deref().unwrap_or("");

                // Pre-check for a clear CONFLICT before CREATE UNIQUE INDEX (the
                // CREATE itself remains the authoritative, race-free guarantee).
                if index.unique {
                    let grouped = cols.join(", ");
                    let sql = format!(
                        "SELECT {grouped} FROM \"{pg_schema_name}\".\"{table_ident}\"{where_sql} \
                         GROUP BY {grouped} HAVING count(*) > 1 LIMIT 5"
                    );
                    match sqlx::query(&sql).fetch_all(&mut *tx).await {
                        Ok(rows) if !rows.is_empty() => {
                            return Err(RtDbError::conflict(format!(
                                "unique index '{}' cannot be created: {} existing row(s) duplicate its key",
                                index.name,
                                rows.len()
                            )));
                        }
                        // A pre-check fetch error is unexpected (cols is
                        // non-empty and the table exists in the tx); warn and
                        // fall through — CREATE UNIQUE INDEX remains the
                        // authoritative, race-free check either way.
                        Err(e) => tracing::warn!(
                            error = %e,
                            "unique-index dup pre-check failed; deferring to CREATE UNIQUE INDEX",
                        ),
                        Ok(_) => {}
                    }
                }

                let unique_kw = if index.unique { "UNIQUE " } else { "" };
                // A UNIQUE index covers exactly its declared fields — appending a
                // per-row-distinct column like `created_at` would make every key
                // distinct and silently defeat the uniqueness guarantee. Non-
                // unique btree indexes keep the trailing `created_at` tiebreaker
                // so take/first/paginate scans get a deterministic physical order.
                let index_cols = if index.unique {
                    cols.join(", ")
                } else {
                    format!("{}, \"created_at\"", cols.join(", "))
                };
                sqlx::query(&format!(
                    "CREATE {unique_kw}INDEX \"{index_ident}\" ON \"{pg_schema_name}\".\"{table_ident}\" ({index_cols}){where_sql}"
                ))
                .execute(&mut *tx)
                .await?;
            }
        }
    }

    let schema_json = serde_json::to_value(&schema).map_err(|err| {
        tracing::error!(error = %err, db, "failed to serialize schema for storage");
        RtDbError::internal("failed to store schema")
    })?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn lit(s: &str) -> FieldType {
        FieldType::Literal {
            value: serde_json::Value::String(s.to_string()),
        }
    }

    fn union_of(vals: &[&str]) -> FieldType {
        FieldType::Union {
            variants: vals.iter().map(|v| lit(v)).collect(),
        }
    }

    fn single_table(
        table: &str,
        fields: BTreeMap<String, FieldType>,
    ) -> BTreeMap<String, TableDef> {
        let mut tables = BTreeMap::new();
        tables.insert(
            table.to_string(),
            TableDef {
                fields,
                indexes: vec![],
                owner_field: None,
                collaborators_field: None,
                ttl: None,
            },
        );
        tables
    }

    fn one_field_schema(table: &str, field: &str, ty: FieldType) -> SchemaDef {
        let mut fields = BTreeMap::new();
        fields.insert(field.to_string(), ty);
        SchemaDef {
            tables: single_table(table, fields),
        }
    }

    #[test]
    fn detect_allows_widening_a_literal_union() {
        let old = one_field_schema("items", "priority", union_of(&["low", "medium", "high"]));
        let new = one_field_schema(
            "items",
            "priority",
            union_of(&["low", "medium", "high", "critical"]),
        );
        assert!(detect_destructive_changes(&old, &new).is_ok());
    }

    #[test]
    fn detect_rejects_narrowing_a_literal_union() {
        let old = one_field_schema(
            "items",
            "priority",
            union_of(&["low", "medium", "high", "critical"]),
        );
        let new = one_field_schema("items", "priority", union_of(&["low", "medium", "high"]));
        let err = detect_destructive_changes(&old, &new).expect_err("narrowing rejected");
        assert!(
            err.message
                .contains("changed type of field 'items.priority'"),
            "{}",
            err.message
        );
    }

    #[test]
    fn detect_rejects_a_scalar_type_change() {
        let old = one_field_schema("items", "qty", FieldType::Number);
        let new = one_field_schema("items", "qty", FieldType::String);
        let err = detect_destructive_changes(&old, &new).expect_err("scalar swap rejected");
        assert!(
            err.message.contains("changed type of field"),
            "{}",
            err.message
        );
    }

    #[test]
    fn detect_still_rejects_a_removed_field() {
        let old = one_field_schema("items", "qty", FieldType::Number);
        let new = SchemaDef {
            tables: single_table("items", BTreeMap::new()),
        };
        let err = detect_destructive_changes(&old, &new).expect_err("field removal rejected");
        assert!(
            err.message.contains("removed field 'items.qty'"),
            "{}",
            err.message
        );
    }
}
