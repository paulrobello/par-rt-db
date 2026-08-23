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
use crate::schema::{
    FieldType, SchemaDef, TableDef, indexed_column_type, is_widening_of, strip_on_delete,
};

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

/// Physical name of a table's auto-increment `SEQUENCE` (one per table that
/// declares `autoIncrementField`). `seq_` + the lowercased table name (capped
/// at 30 chars by `MAX_TABLE_NAME_LEN`) stays well within the 63-byte limit.
/// Standalone (not `OWNED BY` a column): the counter field needs no typed
/// column unless indexed, so the sequence outlives column changes and is
/// dropped explicitly by the destructive reconcile / migrate drop-table.
pub fn pg_sequence(table: &str) -> String {
    format!("seq_{}", table.to_lowercase())
}

/// Creates the table's sequence when `autoIncrementField` is newly declared
/// (fresh table, declaration added, or declaration changed), then
/// repositions it past any stored values — an existing populated table that
/// gains a counter, or a re-added declaration after a migrate rename, must
/// not restart at 1 and collide with stored docs. Runs inside the caller's
/// transaction. No-op for tables without a declaration.
async fn apply_sequence(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    pg_schema_name: &str,
    table_name: &str,
    old_table: Option<&TableDef>,
    new_table: &TableDef,
) -> Result<(), RtDbError> {
    let Some(field) = &new_table.auto_increment_field else {
        return Ok(());
    };
    let unchanged = old_table.is_some_and(|old| {
        old.auto_increment_field.as_deref() == new_table.auto_increment_field.as_deref()
    });
    if unchanged {
        return Ok(());
    }
    let seq_ident = pg_sequence(table_name);
    sqlx::query(&format!(
        "CREATE SEQUENCE IF NOT EXISTS \"{pg_schema_name}\".\"{seq_ident}\""
    ))
    .execute(&mut **tx)
    .await?;
    reposition_sequence(tx, pg_schema_name, table_name, field).await?;
    Ok(())
}

/// Repositions the table's sequence so the next `nextval` is strictly past
/// every stored counter value AND strictly past the sequence's current next
/// value — forward-only, so importing an old snapshot into a database whose
/// sequence has consumed higher numbers can never hand out a value the
/// database already stores. `is_called` distinguishes a fresh sequence
/// (`last_value` = 1, never handed out) from one parked ON its last value.
pub(crate) async fn reposition_sequence(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    pg_schema_name: &str,
    table_name: &str,
    field: &str,
) -> Result<(), RtDbError> {
    let table_ident = pg_table(table_name);
    let seq_ident = pg_sequence(table_name);
    // `field` is a validated identifier interpolated into a string literal
    // (same pattern as `backfill_expr`); the sequence/table idents come from
    // the validated `pg_*` helpers. `setval` takes the sequence name as a
    // regclass — a string literal of the double-quoted ident, NOT a quoted
    // identifier (which SQL would read as a table reference).
    let sql = format!(
        "SELECT setval('\"{pg_schema_name}\".\"{seq_ident}\"'::regclass, GREATEST( \
             COALESCE((SELECT max((doc->>'{field}')::bigint) \
                       FROM \"{pg_schema_name}\".\"{table_ident}\"), 0) + 1, \
             (SELECT last_value + CASE WHEN is_called THEN 1 ELSE 0 END \
              FROM \"{pg_schema_name}\".\"{seq_ident}\") \
         ), false)"
    );
    sqlx::query(&sql).execute(&mut **tx).await?;
    Ok(())
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
                // FM-33: compare with each side's `Id.on_delete` stripped —
                // adding or changing an `onDelete` action alters runtime delete
                // behavior only (no stored row shape), so it is additive, while
                // changing the referenced table is still a type change.
                Some(new_field_type)
                    if strip_on_delete(new_field_type) != strip_on_delete(old_field_type)
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
/// upsert, all inside a single transaction. Returns the applied schema plus
/// the set of tables whose EXISTING documents the apply rewrites (the
/// backfills below) — callers use it to re-run subscriptions, which a
/// backfill otherwise leaves serving stale values until the table's next
/// write.
pub async fn push_schema(
    pool: &PgPool,
    db: &str,
    schema: SchemaDef,
) -> Result<(SchemaDef, std::collections::BTreeSet<String>), RtDbError> {
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
    let backfilled = backfill_affected_tables(previous.as_ref(), &schema);

    let pg_schema_name = pg_schema(db);
    let mut tx = pool.begin().await?;

    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(crate::db::EXTENSION_LOCK_KEY)
        .execute(&mut *tx)
        .await?;

    // Covers databases created before pgvector shipped: ensure the extension is
    // present the first time a vector-index schema is pushed. No-op if already
    // installed (Task 1 installs it at database creation). Serialized by
    // EXTENSION_LOCK_KEY: concurrent IF NOT EXISTS inserts race on
    // pg_extension_name_index (see db.rs).
    sqlx::query("CREATE EXTENSION IF NOT EXISTS vector")
        .execute(&mut *tx)
        .await?;
    // Same for pg_trgm (FM-30 trigram search): installed at database creation
    // since 2026-08-15, backfilled here for older databases.
    sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_trgm")
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
    Ok((schema, backfilled))
}

/// Tables whose existing documents a push rewrites, derived from the same
/// conditions the two backfill UPDATEs run under: a computed entry ADDED or
/// CHANGED (removal backfills nothing — stored values stay), and a ttl
/// `defaultDurationMs` newly declared or changed (pre-declaration rows lack
/// the field; the UPDATE's `IS NULL` guard makes a routine re-push a no-op,
/// so unchanged declarations affect nothing). A brand-new table has no rows,
/// so it can never be backfill-affected.
pub(crate) fn backfill_affected_tables(
    previous: Option<&SchemaDef>,
    schema: &SchemaDef,
) -> std::collections::BTreeSet<String> {
    let mut out = std::collections::BTreeSet::new();
    for (name, new_table) in &schema.tables {
        let Some(old_table) = previous.and_then(|s| s.tables.get(name)) else {
            continue;
        };
        let computed_changed = new_table
            .computed
            .iter()
            .any(|(f, e)| old_table.computed.get(f) != Some(e));
        let new_ttl_default = new_table.ttl.as_ref().and_then(|t| t.default_duration_ms);
        let ttl_new = new_ttl_default.is_some()
            && old_table.ttl.as_ref().and_then(|t| t.default_duration_ms) != new_ttl_default;
        if computed_changed || ttl_new {
            out.insert(name.clone());
        }
    }
    out
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
                // FM-33 soft delete: the stamp column exists on every
                // soft-delete table from creation (nullable — NULL = live row).
                if new_table.soft_delete {
                    columns.push("\"deleted_at\" timestamptz".to_string());
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

                // FM-33 soft delete: adding the flag to an existing table adds
                // the stamp column (`ADD COLUMN IF NOT EXISTS`, same additive
                // pattern as the typed columns above). All existing rows have
                // `deleted_at = NULL` — live — by construction.
                if new_table.soft_delete && !old_table.soft_delete {
                    sqlx::query(&format!(
                        "ALTER TABLE \"{pg_schema_name}\".\"{table_ident}\" \
                         ADD COLUMN IF NOT EXISTS \"deleted_at\" timestamptz"
                    ))
                    .execute(&mut **tx)
                    .await?;
                }
            }
        }

        // Computed-field backfill (ENH-028): when this apply ADDED or CHANGED
        // an entry in the table's computed map, re-derive that field for every
        // existing row (plus its typed column, which the loop above may just
        // have added — the doc key it backfills from is absent until the
        // stamp runs). Runs before index creation so a new index builds over
        // final values. An unchanged map runs no UPDATE — docs and `version`
        // stay untouched on a pure re-push. This is the one additive-apply
        // site shared by push (`push_schema`) and restore
        // (`reconcile_schema_destructive` from `handle_restore_schema`), so
        // both paths backfill. Removing an entry backfills nothing: stored
        // values stay and become ordinary client-writable fields.
        if let Some(old_table) = old_table {
            let changed_computed: Vec<String> = new_table
                .computed
                .iter()
                .filter(|(f, e)| old_table.computed.get(*f) != Some(*e))
                .map(|(f, _)| f.clone())
                .collect();
            if !changed_computed.is_empty() {
                crate::migrate::backfill_computed(
                    tx,
                    pg_schema_name,
                    table_name,
                    schema,
                    &changed_computed,
                )
                .await?;
            }
        }

        // Auto-increment sequence: created (and repositioned past any stored
        // values) whenever the declaration is newly present — new table,
        // declaration added to an existing table, or declaration changed.
        // Unchanged declarations skip this so a routine re-push never
        // disturbs the sequence's position.
        apply_sequence(tx, pg_schema_name, table_name, old_table, new_table).await?;

        let old_index_names: HashSet<&str> = old_table
            .map(|t| t.indexes.iter().map(|index| index.name.as_str()).collect())
            .unwrap_or_default();
        for index in &new_table.indexes {
            // Trigram GIN over a search index's text `f_` columns (FM-30):
            // created for NEW and EXISTING search indexes alike — `IF NOT
            // EXISTS` makes re-pushes a no-op and backfills search indexes that
            // predate trgm mode (the backing `f_` columns exist by this point
            // either way). Accelerates `search` mode `trgm` ILIKE; the query
            // still works without it, just seq-scanned.
            if index.search {
                let trgm_ident = format!(
                    "tg_{}_{}",
                    table_name.to_lowercase(),
                    index.name.to_lowercase()
                );
                let trgm_cols = index
                    .fields
                    .iter()
                    .map(|field_name| format!("\"{}\" gin_trgm_ops", pg_col(field_name)))
                    .collect::<Vec<_>>()
                    .join(", ");
                sqlx::query(&format!(
                    "CREATE INDEX IF NOT EXISTS \"{trgm_ident}\" ON \"{pg_schema_name}\".\"{table_ident}\" \
                     USING GIN ({trgm_cols})"
                ))
                .execute(&mut **tx)
                .await?;
            }
            // FM-33: adding `softDelete` to an existing table widens every
            // unique index's partial predicate (`AND "deleted_at" IS NULL`, see
            // the where_sql composition below). The declared-schema diff sees
            // no index change (fields/where declared identically), so the
            // existing-index skip below would silently keep the narrow
            // predicate — bypass it and physically rebuild the unique ones.
            let soft_delete_newly_added =
                new_table.soft_delete && !old_table.is_some_and(|t| t.soft_delete);
            let rebuild_for_soft_delete =
                soft_delete_newly_added && index.unique && !index.search && index.vector.is_none();
            if old_index_names.contains(index.name.as_str()) && !rebuild_for_soft_delete {
                continue;
            }
            let index_ident = format!(
                "i_{}_{}",
                table_name.to_lowercase(),
                index.name.to_lowercase()
            );
            if rebuild_for_soft_delete {
                sqlx::query(&format!(
                    "DROP INDEX IF EXISTS \"{pg_schema_name}\".\"{index_ident}\""
                ))
                .execute(&mut **tx)
                .await?;
            }
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
                let mut where_sql = match &index.r#where {
                    Some(pred) => {
                        let frag = compile_filter_literal(pred, new_table)?;
                        format!(" WHERE {frag}")
                    }
                    None => String::new(),
                };
                // FM-33: a unique index on a soft-delete table excludes
                // soft-deleted rows — a stamped row holding a key must never
                // conflict with a fresh insert of the same key. The declared
                // `where` composes (`AND`); a bare unique index gains
                // `WHERE "deleted_at" IS NULL`. `render_filter_literal_node`
                // parenthesizes every And/Or node, so appending is
                // precedence-safe. Non-unique indexes are untouched (their
                // `where` is scan shaping, not correctness).
                if index.unique && new_table.soft_delete {
                    if where_sql.is_empty() {
                        where_sql = " WHERE \"deleted_at\" IS NULL".to_string();
                    } else {
                        where_sql.push_str(" AND \"deleted_at\" IS NULL");
                    }
                }
                let where_sql = where_sql.as_str();

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
    /// tables whose auto-increment sequence must be dropped — the table is
    /// going away, or its `autoIncrementField` declaration is removed/changed.
    /// Sequences are standalone (not OWNED BY a column), so nothing else
    /// cascades to them.
    pub drop_sequences: Vec<String>,
}

pub(crate) fn reconcile_diff(current: &SchemaDef, target: &SchemaDef) -> ReconcileDiff {
    let mut drop_tables = Vec::new();
    let mut drop_indexes = Vec::new();
    let mut drop_columns = Vec::new();
    let mut drop_search_cols = Vec::new();
    let mut drop_vector_cols = Vec::new();
    let mut drop_sequences = Vec::new();

    for (table_name, cur_table) in &current.tables {
        match target.tables.get(table_name) {
            None => {
                drop_tables.push(table_name.clone());
                if cur_table.auto_increment_field.is_some() {
                    drop_sequences.push(table_name.clone());
                }
            }
            Some(tgt_table) => {
                if cur_table.auto_increment_field.as_deref()
                    != tgt_table.auto_increment_field.as_deref()
                {
                    drop_sequences.push(table_name.clone());
                }
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
        drop_sequences,
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

    // Standalone sequences (no column dependency), so they can go first.
    for table in &diff.drop_sequences {
        let seq_ident = pg_sequence(table);
        sqlx::query(&format!(
            "DROP SEQUENCE IF EXISTS \"{pg_schema_name}\".\"{seq_ident}\""
        ))
        .execute(&mut **tx)
        .await?;
        touched.insert(table.clone());
    }

    for (table, index_name) in &diff.drop_indexes {
        let index_ident = format!("i_{}_{}", table.to_lowercase(), index_name.to_lowercase());
        sqlx::query(&format!(
            "DROP INDEX IF EXISTS \"{pg_schema_name}\".\"{index_ident}\""
        ))
        .execute(&mut **tx)
        .await?;
        // A search index also owns a trigram GIN (FM-30) beside its tsvector
        // GIN; btree/vector indexes have no `tg_` twin and the guarded drop is
        // a no-op for them.
        let trgm_ident = format!("tg_{}_{}", table.to_lowercase(), index_name.to_lowercase());
        sqlx::query(&format!(
            "DROP INDEX IF EXISTS \"{pg_schema_name}\".\"{trgm_ident}\""
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
                defaults: std::collections::BTreeMap::new(),
                computed: std::collections::BTreeMap::new(),
                fields,
                indexes: vec![],
                owner_field: None,
                collaborators_field: None,
                ttl: None,
                updated_at_field: None,
                auto_increment_field: None,
                authorize: None,

                soft_delete: false,
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
