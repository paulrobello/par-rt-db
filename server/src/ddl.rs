//! Schema-to-Postgres DDL compilation. A pushed `SchemaDef` compiles to typed
//! table DDL — one typed column per indexed field plus the `doc` jsonb column —
//! and `CREATE [UNIQUE] INDEX` (optionally partial via a `where` predicate), plus
//! the generated tsvector and vector columns for `search`/`vector` indexes.
//! Identifier quoting and the 63-byte physical-name cap live here: every
//! identifier is validated and double-quoted, every value is bound via `$n`.

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
                // A search index's `regconfig` is baked into a STORED generated
                // column whose expression Postgres cannot alter in place, so a
                // language change is a breaking index change (reject, like a
                // vector-spec change) rather than a silent no-op.
                Some(new_index) if new_index.language != old_index.language => {
                    return Err(RtDbError::bad_request(format!(
                        "changed language of search index '{}'",
                        old_index.name
                    )));
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// Verifies every search-index `language` declared in `schema` names a real
/// Postgres text-search configuration (`pg_ts_config.cfgname`). The regconfig is
/// interpolated as a literal into `to_tsvector('<lang>'::regconfig, …)`, so a
/// typo must surface as a clear 400 here rather than a DDL-time 500. Format is
/// already gated by `schema::validate_structure`; this is the existence check.
async fn validate_search_languages(pool: &PgPool, schema: &SchemaDef) -> Result<(), RtDbError> {
    let wanted: HashSet<String> = schema
        .tables
        .values()
        .flat_map(|table| table.indexes.iter())
        .filter_map(|index| index.language.clone())
        .collect();
    if wanted.is_empty() {
        return Ok(());
    }
    let wanted_vec: Vec<String> = wanted.iter().cloned().collect();
    let known: HashSet<String> =
        sqlx::query_scalar::<_, String>("SELECT cfgname FROM pg_ts_config WHERE cfgname = ANY($1)")
            .bind(&wanted_vec)
            .fetch_all(pool)
            .await?
            .into_iter()
            .collect();
    let unknown: Vec<&str> = wanted
        .iter()
        .filter(|lang| !known.contains(*lang))
        .map(String::as_str)
        .collect();
    if unknown.is_empty() {
        Ok(())
    } else {
        Err(RtDbError::bad_request(format!(
            "unknown text-search language(s): {} (see pg_ts_config for available configs)",
            unknown.join(", ")
        )))
    }
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
    validate_search_languages(pool, &schema).await?;

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

    apply_schema_additive(&mut tx, &pg_schema_name, previous.as_ref(), &schema).await?;

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

/// Additive table + index DDL shared by `push_schema` and the destructive
/// reconcile. `previous` is the currently-applied schema (`None` = fresh); only
/// NEW tables/columns/indexes (in `schema` but not `previous`) are created.
/// Runs inside the caller's transaction. No `meta` upsert — the caller owns
/// that. Pure extraction of the per-table CREATE/ALTER + index-creation loop
/// that used to live inline in `push_schema`; behavior identical.
async fn apply_schema_additive(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    pg_schema_name: &str,
    previous: Option<&SchemaDef>,
    schema: &SchemaDef,
) -> Result<(), RtDbError> {
    for (table_name, new_table) in &schema.tables {
        let old_table = previous.and_then(|s| s.tables.get(table_name));
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
                sqlx::query(&sql).execute(&mut **tx).await?;
            }
            Some(old_table) => {
                let old_indexed = indexed_fields(old_table);
                for field_name in new_indexed.difference(&old_indexed) {
                    let ty = field_type(new_table, field_name)?;
                    let (pg_type, _nullable) = indexed_column_type(ty)?;
                    let col = pg_col(field_name);

                    sqlx::query(&format!(
                        "ALTER TABLE \"{pg_schema_name}\".\"{table_ident}\" ADD COLUMN IF NOT EXISTS \"{col}\" {pg_type}"
                    ))
                    .execute(&mut **tx)
                    .await?;

                    let expr = backfill_expr(pg_type, field_name)?;
                    sqlx::query(&format!(
                        "UPDATE \"{pg_schema_name}\".\"{table_ident}\" SET \"{col}\" = {expr} WHERE doc ? '{field_name}'"
                    ))
                    .execute(&mut **tx)
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
                // immutable, so it is allowed in a STORED generated column. The
                // `regconfig` is the index's declared `language` (default
                // `english`); the literal is format-validated in
                // `schema::validate_structure` and existence-checked in
                // `validate_search_languages`, so it is safe to interpolate.
                let sv_col = pg_search_col(&index.name);
                let regconfig = index.language.as_deref().unwrap_or("english");
                let terms: Vec<String> = index
                    .fields
                    .iter()
                    .map(|field_name| format!("coalesce(\"{}\", '')", pg_col(field_name)))
                    .collect();
                sqlx::query(&format!(
                    "ALTER TABLE \"{pg_schema_name}\".\"{table_ident}\" \
                     ADD COLUMN \"{sv_col}\" tsvector GENERATED ALWAYS AS \
                     (to_tsvector('{regconfig}'::regconfig, {})) STORED",
                    terms.join(" || ' ' || ")
                ))
                .execute(&mut **tx)
                .await?;
                sqlx::query(&format!(
                    "CREATE INDEX \"{index_ident}\" ON \"{pg_schema_name}\".\"{table_ident}\" \
                     USING GIN (\"{sv_col}\")"
                ))
                .execute(&mut **tx)
                .await?;
            } else if let Some(vec_spec) = &index.vector {
                // Vector index: a plain `vector(N)` column (write-maintained by
                // Task 5, not generated — pgvector has no jsonb->vector generated
                // cast) plus an HNSW index over the declared metric (cosine/l2/ip,
                // ENH-007). The filterFields' `f_` columns already exist (created
                // with the table / added+backfilled above).
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
                .execute(&mut **tx)
                .await?;
                // Backfill from existing rows (no-op on a brand-new table).
                // `vfield` is a doc field name validated by is_valid_identifier
                // in Task 3, and lives in a string literal here, not an identifier.
                sqlx::query(&format!(
                    "UPDATE \"{pg_schema_name}\".\"{table_ident}\" \
                     SET \"{v_col}\" = (doc->>'{vfield}')::vector \
                     WHERE doc ? '{vfield}'"
                ))
                .execute(&mut **tx)
                .await?;
                let opclass = vec_spec.metric.opclass();
                sqlx::query(&format!(
                    "CREATE INDEX \"{index_ident}\" ON \"{pg_schema_name}\".\"{table_ident}\" \
                     USING hnsw (\"{v_col}\" {opclass})"
                ))
                .execute(&mut **tx)
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
                    match sqlx::query(&sql).fetch_all(&mut **tx).await {
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
                .execute(&mut **tx)
                .await?;
            }
        }

        // Backfill the TTL field on existing rows when a `default_duration_ms`
        // is declared: stamp `f_<field> = created_at + default` for rows that
        // lack the field. Runs for both new and existing tables (no NULL rows
        // ⇒ no-op) and preserves any caller-set value via the `IS NULL` guard.
        // The typed column is updated alongside the jsonb `doc` because reads
        // (merge_doc) return the doc, not the column — updating only the column
        // would make the backfill invisible to queries. Identifiers are
        // validated/lowercased via `pg_*`; only the duration is bound, never
        // interpolated. The field name baked into `jsonb_build_object` is a
        // validated identifier (same literal-interpolation pattern as
        // `backfill_expr` above).
        if let Some(ttl) = &new_table.ttl
            && let Some(d) = ttl.default_duration_ms
        {
            let col = pg_col(&ttl.field);
            let field = &ttl.field;
            sqlx::query(&format!(
                "UPDATE \"{pg_schema_name}\".\"{table_ident}\" \
                 SET \"{col}\" = created_at + $1, \
                     doc = doc || jsonb_build_object('{field}', created_at + $1) \
                 WHERE \"{col}\" IS NULL"
            ))
            .bind(d)
            .execute(&mut **tx)
            .await?;
        }
    }
    Ok(())
}

/// Pure enumeration of the DDL needed to make `current`'s shape match `target`.
/// The inverse of `detect_destructive_changes`: instead of rejecting the first
/// difference, it lists everything to drop and (via `apply_schema_additive`) add.
pub(crate) struct ReconcileDiff {
    pub drop_tables: Vec<String>,
    /// `(table, index_name)` — drop these indexes (by their physical ident).
    pub drop_indexes: Vec<(String, String)>,
    /// `(table, field_name)` — drop these typed index columns (doc jsonb is preserved).
    pub drop_columns: Vec<(String, String)>,
    /// search indexes to drop also need their generated tsvector column removed.
    pub drop_search_cols: Vec<(String, String)>,
    /// vector indexes to drop also need their write-maintained `vector(N)`
    /// column removed (the `v_<index>` column; the vector field itself is NOT a
    /// typed `f_` column — it lives only in `doc` jsonb and the `v_` column).
    pub drop_vector_cols: Vec<(String, String)>,
}

pub(crate) fn reconcile_diff(current: &SchemaDef, target: &SchemaDef) -> ReconcileDiff {
    let mut drop_tables = Vec::new();
    let mut drop_indexes = Vec::new();
    let mut drop_columns = Vec::new();
    let mut drop_search_cols = Vec::new();
    let mut drop_vector_cols = Vec::new();

    for (table_name, cur_table) in &current.tables {
        match target.tables.get(table_name) {
            None => drop_tables.push(table_name.clone()),
            Some(tgt_table) => {
                let cur_indexed_set = indexed_fields(cur_table);
                let tgt_indexed_set = indexed_fields(tgt_table);
                let cur_indexed: HashSet<&str> =
                    cur_indexed_set.iter().map(String::as_str).collect();
                let tgt_indexed: HashSet<&str> =
                    tgt_indexed_set.iter().map(String::as_str).collect();
                for field in cur_indexed.difference(&tgt_indexed) {
                    drop_columns.push((table_name.clone(), field.to_string()));
                }
                let tgt_index_names: HashSet<&str> =
                    tgt_table.indexes.iter().map(|i| i.name.as_str()).collect();
                for idx in &cur_table.indexes {
                    if !tgt_index_names.contains(idx.name.as_str()) {
                        drop_indexes.push((table_name.clone(), idx.name.clone()));
                        if idx.search {
                            drop_search_cols.push((table_name.clone(), idx.name.clone()));
                        }
                        if idx.vector.is_some() {
                            drop_vector_cols.push((table_name.clone(), idx.name.clone()));
                        }
                    }
                }
            }
        }
    }
    ReconcileDiff {
        drop_tables,
        drop_indexes,
        drop_columns,
        drop_search_cols,
        drop_vector_cols,
    }
}

/// Destructive reconcile: drop tables/columns/indexes in `current` but not
/// `target`, add those in `target` but not `current`, inside the caller's tx.
/// Returns the set of touched table names (for subscription fan-out). Does NOT
/// touch `meta` — the caller upserts the target blob.
///
/// Drop order is indexes → search-generated tsvector columns → vector-index
/// `vector(N)` columns → typed `f_` columns → tables, so each index's
/// generated/maintained column is removed before any backing `f_` column, and
/// indexes always go before any column/table they depend on. Identifiers are
/// produced by the existing validated `pg_*` helpers and `IF EXISTS` guards
/// every drop, so a partial state from a prior failed reconcile is itself
/// reconcilable. Document jsonb is never touched — only the redundant
/// typed/index copies are — so a restore that removes an index column preserves
/// the doc data.
pub async fn reconcile_schema_destructive(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    db: &str,
    current: &SchemaDef,
    target: &SchemaDef,
) -> Result<Vec<String>, RtDbError> {
    let pg_schema_name = pg_schema(db);
    let diff = reconcile_diff(current, target);
    let mut touched: HashSet<String> = HashSet::new();

    for (table, index_name) in &diff.drop_indexes {
        let index_ident = format!("i_{}_{}", table.to_lowercase(), index_name.to_lowercase());
        sqlx::query(&format!(
            "DROP INDEX IF EXISTS \"{pg_schema_name}\".\"{index_ident}\""
        ))
        .execute(&mut **tx)
        .await?;
        touched.insert(table.clone());
    }
    for (table, index_name) in &diff.drop_search_cols {
        let table_ident = pg_table(table);
        let sv_col = pg_search_col(index_name);
        sqlx::query(&format!(
            "ALTER TABLE \"{pg_schema_name}\".\"{table_ident}\" DROP COLUMN IF EXISTS \"{sv_col}\""
        ))
        .execute(&mut **tx)
        .await?;
        touched.insert(table.clone());
    }
    for (table, index_name) in &diff.drop_vector_cols {
        let table_ident = pg_table(table);
        let v_col = pg_vector_col(index_name);
        sqlx::query(&format!(
            "ALTER TABLE \"{pg_schema_name}\".\"{table_ident}\" DROP COLUMN IF EXISTS \"{v_col}\""
        ))
        .execute(&mut **tx)
        .await?;
        touched.insert(table.clone());
    }
    for (table, field) in &diff.drop_columns {
        let table_ident = pg_table(table);
        let col = pg_col(field);
        sqlx::query(&format!(
            "ALTER TABLE \"{pg_schema_name}\".\"{table_ident}\" DROP COLUMN IF EXISTS \"{col}\""
        ))
        .execute(&mut **tx)
        .await?;
        touched.insert(table.clone());
    }
    for table in &diff.drop_tables {
        let table_ident = pg_table(table);
        sqlx::query(&format!(
            "DROP TABLE IF EXISTS \"{pg_schema_name}\".\"{table_ident}\""
        ))
        .execute(&mut **tx)
        .await?;
        touched.insert(table.clone());
    }

    // Additive side: anything in target not in (post-drop) current.
    apply_schema_additive(tx, &pg_schema_name, Some(current), target).await?;
    for table_name in target.tables.keys() {
        touched.insert(table_name.clone());
    }
    Ok(touched.into_iter().collect())
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
                authorize: None,
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
