//! Declarative schema migration: an ordered list of directives the server
//! applies transactionally to transform a database's schema and documents.
//! See docs/superpowers/specs/2026-07-31-schema-migration-backfill-design.md.
use crate::ddl::{
    backfill_expr, indexed_fields, pg_col, pg_schema, pg_search_col, pg_table, pg_vector_col,
};
use crate::error::RtDbError;
use crate::schema::{FieldType, SchemaDef, TableDef, indexed_column_type};
use crate::txn::{DocOp, OpKind};
use std::collections::BTreeSet;

/// One migration step. Wire shape mirrors `txn::Step`: `tag = "op"`,
/// camelCase, `deny_unknown_fields`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", deny_unknown_fields)]
pub enum Directive {
    RenameField {
        table: String,
        from: String,
        to: String,
    },
    RenameTable {
        from: String,
        to: String,
    },
    ChangeType {
        table: String,
        field: String,
        to: FieldType,
        cast: Cast,
        #[serde(default)]
        default: Option<serde_json::Value>,
    },
    DropField {
        table: String,
        field: String,
    },
    DropTable {
        name: String,
    },
    DropIndex {
        table: String,
        name: String,
    },
    SetDefault {
        table: String,
        field: String,
        value: serde_json::Value,
    },
    EvalExpr {
        table: String,
        set: String,
        expr: String,
        #[serde(default, rename = "where")]
        where_clause: Option<String>,
    },
}

/// Closed set of sound coercions for `Directive::ChangeType`.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Cast {
    ToString,
    ToNumber,
    ToInt64,
    ToBoolean,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrateRequest {
    pub directives: Vec<Directive>,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrateResult {
    pub applied: bool,
    pub schema: crate::schema::SchemaDef,
    pub directives: Vec<DirectiveReport>,
}

/// Per-directive outcome. `Default` is derived so later tasks can build it
/// incrementally with `..Default::default()`; the `op` and `affected_rows`
/// fields are always set explicitly.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DirectiveReport {
    pub op: String,
    pub affected_rows: i64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cast_failures: Vec<CastFailure>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sample_changes: Vec<SampleChange>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CastFailure {
    pub id: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SampleChange {
    pub id: String,
    pub before: serde_json::Value,
    pub after: serde_json::Value,
}

/// Validates `directives` against `old`, folding each into a working copy of
/// the schema in order, and returns the derived resulting `SchemaDef`. Pure
/// (no DB). Rejects: missing source table/field/index; a rename/changeType
/// target that already exists or is produced by an earlier directive; a cast
/// invalid for the old→new type pair; an out-of-scope `evalExpr` (contains a
/// `FROM`/JOIN or a DDL verb keyword, or targets a missing table).
pub fn plan_migration(old: &SchemaDef, directives: &[Directive]) -> Result<SchemaDef, RtDbError> {
    let mut schema = old.clone();
    for d in directives {
        validate_one(&mut schema, d)?;
    }
    Ok(schema)
}

fn validate_one(schema: &mut SchemaDef, d: &Directive) -> Result<(), RtDbError> {
    match d {
        Directive::RenameField { table, from, to } => {
            let t = table_mut(schema, table)?;
            if t.fields.contains_key(to) {
                return Err(RtDbError::bad_request(format!(
                    "rename target '{table}.{to}' already exists"
                )));
            }
            let ft = t.fields.remove(from).ok_or_else(|| {
                RtDbError::bad_request(format!("renamed field '{table}.{from}' does not exist"))
            })?;
            t.fields.insert(to.clone(), ft);
            // fix index references that used `from`
            for ix in t.indexes.iter_mut() {
                for f in ix.fields.iter_mut() {
                    if f == from {
                        *f = to.clone();
                    }
                }
            }
            if t.owner_field.as_deref() == Some(from.as_str()) {
                t.owner_field = Some(to.clone());
            }
            if t.collaborators_field.as_deref() == Some(from.as_str()) {
                t.collaborators_field = Some(to.clone());
            }
            if let Some(expr) = t.authorize.as_mut() {
                rename_filter_fields(expr, from, to);
            }
        }
        Directive::RenameTable { from, to } => {
            if schema.tables.contains_key(to) {
                return Err(RtDbError::bad_request(format!(
                    "rename target table '{to}' already exists"
                )));
            }
            let mut def = schema.tables.remove(from).ok_or_else(|| {
                RtDbError::bad_request(format!("renamed table '{from}' does not exist"))
            })?;
            // Id references to `from` follow the rename. The renamed table was
            // just removed from the map, so the loop over the remaining tables
            // would skip its own self-referential `Id { table: from }` fields —
            // rewrite those here first, lest the rename leave a dangling ref.
            for ft in def.fields.values_mut() {
                if let FieldType::Id { table } = ft
                    && table == from
                {
                    *table = to.clone();
                }
            }
            for t in schema.tables.values_mut() {
                for ft in t.fields.values_mut() {
                    if let FieldType::Id { table } = ft
                        && table == from
                    {
                        *table = to.clone();
                    }
                }
            }
            schema.tables.insert(to.clone(), def);
        }
        Directive::ChangeType {
            table,
            field,
            to: new_ty,
            cast,
            ..
        } => {
            let t = table_mut(schema, table)?;
            let old_ty = t.fields.get(field).ok_or_else(|| {
                RtDbError::bad_request(format!("changed field '{table}.{field}' does not exist"))
            })?;
            if !cast_valid_for(*cast, old_ty) {
                return Err(RtDbError::bad_request(format!(
                    "cast {cast:?} is not valid for {table}.{field}"
                )));
            }
            t.fields.insert(field.clone(), new_ty.clone());
        }
        Directive::DropField { table, field } => {
            let t = table_mut(schema, table)?;
            // `authorize` is load-bearing for auth: silently clearing it (as
            // ownerField/collaboratorsField are below) would widen access. A
            // field the predicate still references must be untied explicitly —
            // reject the migration so the caller amends `authorize` first.
            if let Some(expr) = &t.authorize
                && filter_expr_references_field(expr, field)
            {
                return Err(RtDbError::bad_request(format!(
                    "cannot drop field '{table}.{field}': still referenced by the authorize predicate (amend authorize first)"
                )));
            }
            if t.fields.remove(field).is_none() {
                return Err(RtDbError::bad_request(format!(
                    "dropped field '{table}.{field}' does not exist"
                )));
            }
            for ix in t.indexes.iter_mut() {
                ix.fields.retain(|f| f != field);
            }
            if t.owner_field.as_deref() == Some(field.as_str()) {
                t.owner_field = None;
            }
            if t.collaborators_field.as_deref() == Some(field.as_str()) {
                t.collaborators_field = None;
            }
        }
        Directive::DropTable { name } => {
            if schema.tables.remove(name).is_none() {
                return Err(RtDbError::bad_request(format!(
                    "dropped table '{name}' does not exist"
                )));
            }
        }
        Directive::DropIndex { table, name } => {
            let t = table_mut(schema, table)?;
            if !t.indexes.iter().any(|ix| &ix.name == name) {
                return Err(RtDbError::bad_request(format!(
                    "dropped index '{table}.{name}' does not exist"
                )));
            }
            t.indexes.retain(|ix| &ix.name != name);
        }
        Directive::SetDefault { table, field, .. } => {
            let t = table_mut(schema, table)?;
            if !t.fields.contains_key(field) {
                return Err(RtDbError::bad_request(format!(
                    "setDefault target '{table}.{field}' does not exist"
                )));
            }
            // data-only; schema unchanged
        }
        Directive::EvalExpr {
            table,
            set,
            expr,
            where_clause,
        } => {
            let _ = table_mut(schema, table)?; // table must exist
            // `set` is a field path; the field need not exist (evalExpr may populate a
            // new key the caller adds via a later additive push), but the name must be
            // a valid identifier. It is interpolated into the `jsonb_set` key literal,
            // so a stray quote or backslash would otherwise break the SQL string.
            if !crate::schema::is_valid_identifier(set, crate::schema::MAX_FIELD_NAME_LEN) {
                return Err(RtDbError::bad_request(format!(
                    "evalExpr 'set' must be a valid field name, got '{set}'"
                )));
            }
            if has_sql_violation(expr) || where_clause.as_deref().is_some_and(has_sql_violation) {
                return Err(RtDbError::bad_request(format!(
                    "evalExpr for '{table}.{set}' is out of scope (no FROM/joins or DDL verbs)"
                )));
            }
        }
    }
    Ok(())
}

fn table_mut<'a>(schema: &'a mut SchemaDef, table: &str) -> Result<&'a mut TableDef, RtDbError> {
    schema
        .tables
        .get_mut(table)
        .ok_or_else(|| RtDbError::bad_request(format!("table '{table}' does not exist")))
}

/// Rewrite every `field` reference in `expr` that equals `from` to `to`, in
/// place. Used by `RenameField` to carry an `authorize` predicate across a
/// field rename (mirroring the ownerField/collaboratorsField rewrite). Recurses
/// through `And`/`Or`/`Not`.
fn rename_filter_fields(expr: &mut crate::query::FilterExpr, from: &str, to: &str) {
    use crate::query::FilterExpr;
    match expr {
        FilterExpr::Eq { field, .. }
        | FilterExpr::Neq { field, .. }
        | FilterExpr::Gt { field, .. }
        | FilterExpr::Gte { field, .. }
        | FilterExpr::Lt { field, .. }
        | FilterExpr::Lte { field, .. }
        | FilterExpr::In { field, .. }
        | FilterExpr::Contains { field, .. }
        | FilterExpr::Exists { field } => {
            if field == from {
                *field = to.to_string();
            }
        }
        FilterExpr::And { exprs } | FilterExpr::Or { exprs } => {
            for e in exprs {
                rename_filter_fields(e, from, to);
            }
        }
        FilterExpr::Not { expr } => rename_filter_fields(expr, from, to),
    }
}

/// True if any `field` reference in `expr` equals `field`. Used by `DropField`
/// to reject dropping a field the `authorize` predicate still depends on.
fn filter_expr_references_field(expr: &crate::query::FilterExpr, field: &str) -> bool {
    use crate::query::FilterExpr;
    match expr {
        FilterExpr::Eq { field: f, .. }
        | FilterExpr::Neq { field: f, .. }
        | FilterExpr::Gt { field: f, .. }
        | FilterExpr::Gte { field: f, .. }
        | FilterExpr::Lt { field: f, .. }
        | FilterExpr::Lte { field: f, .. }
        | FilterExpr::In { field: f, .. }
        | FilterExpr::Contains { field: f, .. }
        | FilterExpr::Exists { field: f } => f == field,
        FilterExpr::And { exprs } | FilterExpr::Or { exprs } => {
            exprs.iter().any(|e| filter_expr_references_field(e, field))
        }
        FilterExpr::Not { expr } => filter_expr_references_field(expr, field),
    }
}

/// True if `cast` can coerce from `old`. Mirrors the matrix in the spec.
fn cast_valid_for(cast: Cast, old: &FieldType) -> bool {
    use FieldType::*;
    matches!(
        (cast, old),
        (Cast::ToString, String | Number | Boolean | Int64)
            | (Cast::ToNumber, String | Boolean | Int64)
            | (Cast::ToInt64, String | Number)
            | (Cast::ToBoolean, String | Number)
    )
}

/// Rejects the scoped-raw-SQL boundary violations: a `FROM`/`JOIN` (cross-table)
/// or any DDL verb. The admin is trusted; this is blast-radius scoping.
fn has_sql_violation(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    const FORBIDDEN: &[&str] = &[
        " FROM ",
        " JOIN ",
        " INTO ",
        "UPDATE ",
        "DELETE ",
        "INSERT ",
        "DROP ",
        "ALTER ",
        "TRUNCATE ",
        "CREATE ",
        "GRANT ",
        "REVOKE ",
    ];
    FORBIDDEN.iter().any(|kw| upper.contains(kw))
}

// ---- DB applier (Task 3) ---------------------------------------------------
//
// `apply_migration` runs each directive's DDL+DML inside a caller-supplied
// transaction. The invariant it upholds: every typed `f_<field>` column stays
// consistent with the `doc` jsonb after each directive. The pre-migration
// schema (for "is the source field indexed?") is read from the db's `meta` row
// inside the tx — the caller has not updated `meta` yet, so it reflects the
// pre-migration state — then advanced per directive into a `working` copy, so
// a later directive resolves an entity an earlier one renamed/changed by its
// current name. `derived` (from `plan_migration`) is the full post-batch schema
// and supplies each arm's final field type/column.

/// Per-directive outcome of applying a migration: per-directive `reports`, the
/// set of tables `touched` (drives subscription re-run in Task 6), and the
/// document `ops` (feed the op-feed / activity stream via Task 6's committer
/// arm). `Default` so future tasks can extend it incrementally.
#[derive(Default, Debug)]
pub struct MigrationEffects {
    pub reports: Vec<DirectiveReport>,
    pub touched: BTreeSet<String>,
    pub ops: Vec<DocOp>,
}

/// Applies already-validated `directives` inside `tx` against `db`'s physical
/// tables. `derived` is the post-migration schema (from `plan_migration`).
/// Bulk casts mirror `ddl::backfill_expr`. Does NOT commit; on `dry_run` the
/// caller rolls the tx back, and effects are still collected for the preview.
pub async fn apply_migration(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    db: &str,
    directives: &[Directive],
    derived: &SchemaDef,
    dry_run: bool,
) -> Result<MigrationEffects, RtDbError> {
    let schema_name = pg_schema(db);
    let old = load_schema_in_tx(tx, &schema_name)
        .await?
        .ok_or_else(|| RtDbError::internal("no schema stored for database"))?;
    // `working` is the schema as it stands before each directive — advanced
    // past every applied directive so a later directive resolves an entity an
    // earlier one renamed/changed by its current name. `old` alone (the
    // pre-batch snapshot) would miss that: a rename→modify-on-renamed-entity
    // batch passes `plan_migration` (which folds sequentially) but would fail
    // at apply time looking the renamed table up under its new name. `derived`
    // stays the post-batch schema for arms needing each field's final type.
    let mut working = old.clone();
    let mut fx = MigrationEffects::default();
    for d in directives {
        let report = apply_one(tx, &schema_name, &working, derived, d, &mut fx).await?;
        // Advance `working` past this directive. `plan_migration` already proved
        // the whole sequence folds cleanly on `old`, so this cannot error here.
        validate_one(&mut working, d)?;
        fx.reports.push(report);
    }
    let _ = dry_run; // dry_run only governs commit/rollback in the caller
    Ok(fx)
}

async fn apply_one(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    schema_name: &str,
    old: &SchemaDef,
    derived: &SchemaDef,
    d: &Directive,
    fx: &mut MigrationEffects,
) -> Result<DirectiveReport, RtDbError> {
    match d {
        Directive::RenameField { table, from, to } => {
            let t = pg_table(table);
            // Rename the typed column only if the source field is indexed
            // (checked on the pre-migration table — the column still bears the
            // old name). RENAME COLUMN preserves the column's type and values,
            // and Postgres rewrites index column references to follow it, so
            // the index keeps working. Do NOT recompute the column as text.
            let old_table = table_def(old, table)?;
            if indexed_fields(old_table).contains(from) {
                sqlx::query(&format!(
                    "ALTER TABLE \"{schema_name}\".\"{t}\" RENAME COLUMN \"{}\" TO \"{}\"",
                    pg_col(from),
                    pg_col(to)
                ))
                .execute(&mut **tx)
                .await?;
            }
            let ids = ids_where(tx, schema_name, &t, &format!("doc ? '{from}'")).await?;
            let n = rewrite_doc_key(tx, schema_name, &t, from, to).await?;
            fx.touched.insert(table.clone());
            push_ops(&mut fx.ops, table, &ids, OpKind::Patch);
            Ok(DirectiveReport {
                op: "renameField".into(),
                affected_rows: n,
                ..Default::default()
            })
        }
        Directive::RenameTable { from, to } => {
            // Physical table rename; docs are untouched -> no DocOps, but the
            // table is recorded as touched so subscriptions re-run.
            sqlx::query(&format!(
                "ALTER TABLE \"{schema_name}\".\"{}\" RENAME TO \"{}\"",
                pg_table(from),
                pg_table(to)
            ))
            .execute(&mut **tx)
            .await?;
            fx.touched.insert(to.clone());
            Ok(DirectiveReport {
                op: "renameTable".into(),
                affected_rows: 0,
                ..Default::default()
            })
        }
        Directive::DropField { table, field } => {
            let t = pg_table(table);
            // Reject dropping a field still referenced by an index — dropping
            // its typed column would desync the physical index from the derived
            // schema. Name the offending index(es) so the caller drops them first.
            let old_table = table_def(old, table)?;
            let blocking: Vec<&str> = old_table
                .indexes
                .iter()
                .filter(|ix| {
                    ix.fields.iter().any(|f| f == field)
                        || ix
                            .vector
                            .as_ref()
                            .is_some_and(|v| v.filter_fields.iter().any(|f| f == field))
                })
                .map(|ix| ix.name.as_str())
                .collect();
            if !blocking.is_empty() {
                return Err(RtDbError::bad_request(format!(
                    "drop index '{}' before dropping field '{table}.{field}'",
                    blocking.join("', '")
                )));
            }
            // Only rows that carry the field have their `doc` change (key
            // removal is a no-op on rows without it), so scope the rewrite, the
            // reported `affected_rows`, and the DocOps to those carriers —
            // matching the spec's "DocOps for the affected rows" and the other
            // data-bearing directives. The typed-column drop below is table-wide.
            let ids = ids_where(tx, schema_name, &t, &format!("doc ? '{field}'")).await?;
            sqlx::query(&format!(
                "UPDATE \"{schema_name}\".\"{t}\" SET doc = doc - '{field}' \
                 WHERE doc ? '{field}'"
            ))
            .execute(&mut **tx)
            .await?;
            // Drop the typed column (no-op if the field was never indexed).
            sqlx::query(&format!(
                "ALTER TABLE \"{schema_name}\".\"{t}\" DROP COLUMN IF EXISTS \"{}\"",
                pg_col(field)
            ))
            .execute(&mut **tx)
            .await?;
            fx.touched.insert(table.clone());
            push_ops(&mut fx.ops, table, &ids, OpKind::Patch);
            Ok(DirectiveReport {
                op: "dropField".into(),
                affected_rows: ids.len() as i64,
                ..Default::default()
            })
        }
        Directive::DropTable { name } => {
            let t = pg_table(name);
            let ids = all_ids(tx, schema_name, &t).await?;
            sqlx::query(&format!("DROP TABLE \"{schema_name}\".\"{t}\""))
                .execute(&mut **tx)
                .await?;
            fx.touched.insert(name.clone());
            push_ops(&mut fx.ops, name, &ids, OpKind::Delete);
            Ok(DirectiveReport {
                op: "dropTable".into(),
                affected_rows: ids.len() as i64,
                ..Default::default()
            })
        }
        Directive::DropIndex { table, name } => {
            // Index ident mirrors ddl::push_schema's `i_{table}_{name}` (both
            // lowercased). No DocOps; the table is touched so subscriptions re-run.
            let idx = format!("i_{}_{}", table.to_lowercase(), name.to_lowercase());
            sqlx::query(&format!("DROP INDEX IF EXISTS \"{schema_name}\".\"{idx}\""))
                .execute(&mut **tx)
                .await?;
            // `old` is the pre-migration schema (still has the dropped index);
            // `derived` is post-migration (the index is gone). Drop the index's
            // generated/maintained columns that no surviving index owns, so a
            // later push_schema/migrate re-creating the index can't collide on
            // an orphan column — parity with `ddl::reconcile_diff`'s
            // `drop_search_cols` / `drop_vector_cols` / `drop_columns`.
            let dropped_index = old
                .tables
                .get(table)
                .and_then(|t| t.indexes.iter().find(|i| i.name == *name));
            let t_ident = pg_table(table);
            // A dropped search index leaves its generated `s_` tsvector column.
            if dropped_index.map(|i| i.search).unwrap_or(false) {
                let sv_col = pg_search_col(name);
                sqlx::query(&format!(
                    "ALTER TABLE \"{schema_name}\".\"{t_ident}\" DROP COLUMN IF EXISTS \"{sv_col}\""
                ))
                .execute(&mut **tx)
                .await?;
            }
            // A dropped vector index leaves its write-maintained `v_` vector(N)
            // column (the vector field itself lives in `doc` jsonb, not a typed
            // `f_` column).
            if dropped_index.and_then(|i| i.vector.as_ref()).is_some() {
                let v_col = pg_vector_col(name);
                sqlx::query(&format!(
                    "ALTER TABLE \"{schema_name}\".\"{t_ident}\" DROP COLUMN IF EXISTS \"{v_col}\""
                ))
                .execute(&mut **tx)
                .await?;
            }
            // Drop backing `f_` columns the dropped index owned and NO remaining
            // index still uses. Without this, an orphan column makes a later
            // push_schema's ADD COLUMN fail with "column already exists".
            let still_indexed: BTreeSet<String> = derived
                .tables
                .get(table)
                .map(indexed_fields)
                .unwrap_or_default();
            let dropped_fields: Vec<String> =
                dropped_index.map(|i| i.fields.clone()).unwrap_or_default();
            for field_name in dropped_fields {
                if still_indexed.contains(&field_name) {
                    continue;
                }
                let col = pg_col(&field_name);
                sqlx::query(&format!(
                    "ALTER TABLE \"{schema_name}\".\"{t_ident}\" DROP COLUMN IF EXISTS \"{col}\""
                ))
                .execute(&mut **tx)
                .await?;
            }
            fx.touched.insert(table.clone());
            Ok(DirectiveReport {
                op: "dropIndex".into(),
                affected_rows: 0,
                ..Default::default()
            })
        }
        Directive::SetDefault {
            table,
            field,
            value,
        } => {
            let t = pg_table(table);
            let value_json =
                serde_json::to_string(value).map_err(|e| RtDbError::internal(e.to_string()))?;
            // Capture the rows lacking the field BEFORE the update — after the
            // update they have it, so the `WHERE NOT doc ? '{field}'` predicate
            // would no longer match them.
            let ids = ids_where(tx, schema_name, &t, &format!("NOT doc ? '{field}'")).await?;
            sqlx::query(&format!(
                "UPDATE \"{schema_name}\".\"{t}\" \
                 SET doc = jsonb_set(doc, '{{\"{field}\"}}', $1::jsonb, true) \
                 WHERE NOT doc ? '{field}'"
            ))
            .bind(&value_json)
            .execute(&mut **tx)
            .await?;
            // If the field is indexed, recompute its typed `f_` column for the
            // affected rows (they now carry the default). `derived` carries the
            // field's post-migration type.
            let derived_table = table_def(derived, table)?;
            if indexed_fields(derived_table).contains(field) {
                let fty = derived_table.fields.get(field).ok_or_else(|| {
                    RtDbError::internal(format!(
                        "setDefault targets absent field '{table}.{field}'"
                    ))
                })?;
                let (pg_type, _) = indexed_column_type(fty)?;
                let col = pg_col(field);
                let expr = backfill_expr(pg_type, field)?;
                recompute_columns_for_ids(tx, schema_name, &t, &col, &expr, &ids).await?;
            }
            let n = ids.len() as i64;
            fx.touched.insert(table.clone());
            push_ops(&mut fx.ops, table, &ids, OpKind::Patch);
            Ok(DirectiveReport {
                op: "setDefault".into(),
                affected_rows: n,
                ..Default::default()
            })
        }
        Directive::ChangeType {
            table,
            field,
            to,
            cast,
            default,
        } => {
            let t = pg_table(table);
            let (pg_type, _nullable) = indexed_column_type(to).map_err(|_| {
                RtDbError::bad_request(format!(
                    "changeType target for {table}.{field} is not indexable"
                ))
            })?;
            // Validate any supplied default is representable in the target type
            // before we touch a row. The column recompute below reads the doc we
            // just wrote; an unrepresentable default would otherwise break the
            // `ALTER ... USING` cast on the defaulted rows with a Postgres error
            // instead of a clean BadRequest naming the misconfiguration.
            if let Some(d) = default
                && coerce_value(*cast, d).is_none()
            {
                return Err(RtDbError::bad_request(format!(
                    "changeType default for {table}.{field} is not representable under cast {cast:?}"
                )));
            }
            // The typed `f_` column exists iff the source field was indexed on
            // the pre-migration table. Non-indexed fields carry their value
            // only in `doc` jsonb, so the ALTER below is gated on this.
            let old_table = table_def(old, table)?;
            let field_indexed = indexed_fields(old_table).contains(field);
            // Only rows that carry the field have a value to cast, so scan just
            // those — `affected_rows` and the DocOps cover exactly the carriers,
            // matching the spec's "DocOps for the affected rows".
            let ids = ids_where(tx, schema_name, &t, &format!("doc ? '{field}'")).await?;
            for id in &ids {
                let row: Option<(Option<serde_json::Value>,)> = sqlx::query_as(&format!(
                    "SELECT doc->'{field}' FROM \"{schema_name}\".\"{t}\" WHERE id = $1"
                ))
                .bind(id)
                .fetch_optional(&mut **tx)
                .await?;
                let Some((Some(val),)) = row else { continue };
                let coerced = coerce_value(*cast, &val);
                let new_val = match (coerced, default) {
                    (Some(v), _) => v,
                    // Up-front validation guarantees `coerce_value(*cast, d)` is
                    // `Some`, so store the coerced form — otherwise the doc would
                    // carry the default's original JSON type (e.g. bool `true`
                    // under `ToNumber`) and the `ALTER ... USING (doc->>'field')
                    // ::<pg>` re-cast below would reject it (e.g. `'true'::float8`
                    // errors in Postgres), turning a clean `BadRequest` into a 500.
                    (None, Some(d)) => coerce_value(*cast, d).unwrap_or_else(|| d.clone()),
                    (None, None) => {
                        return Err(RtDbError::bad_request(format!(
                            "changeType cannot coerce value in {table}.{id} ({val}) and no default given"
                        )));
                    }
                };
                let s = serde_json::to_string(&new_val)
                    .map_err(|e| RtDbError::internal(e.to_string()))?;
                sqlx::query(&format!(
                    "UPDATE \"{schema_name}\".\"{t}\" \
                     SET doc = jsonb_set(doc, '{{\"{field}\"}}', $1::jsonb, true) \
                     WHERE id = $2"
                ))
                .bind(&s)
                .bind(id)
                .execute(&mut **tx)
                .await?;
            }
            // Recast the typed column to the new pg type. The recompute reads
            // the already-updated `doc`, which per the loop above holds a value
            // that is coercible under the cast (or the default, validated up
            // front to be representable), so the `USING` cast cannot fail. A
            // non-indexed field has no `f_` column to alter.
            if field_indexed {
                let col = pg_col(field);
                let using_expr = backfill_expr(pg_type, field)?;
                sqlx::query(&format!(
                    "ALTER TABLE \"{schema_name}\".\"{t}\" \
                     ALTER COLUMN \"{col}\" TYPE {pg_type} USING ({using_expr})"
                ))
                .execute(&mut **tx)
                .await?;
            }
            fx.touched.insert(table.clone());
            push_ops(&mut fx.ops, table, &ids, OpKind::Patch);
            Ok(DirectiveReport {
                op: "changeType".into(),
                affected_rows: ids.len() as i64,
                ..Default::default()
            })
        }
        Directive::EvalExpr {
            table,
            set,
            expr,
            where_clause,
        } => {
            // Scope was already validated by `plan_migration`: `expr`/`where`
            // carry no `FROM`/`JOIN`/DDL verbs, and `set` is a regex-clean field
            // name. This is the scoped raw-SQL escape — the admin authors `expr`
            // and `where` as SQL text bounded to this one table's `doc`. Capture
            // the affected ids BEFORE the rewrite using the same `cond` so DocOps
            // cover exactly the rows about to change.
            let t = pg_table(table);
            let cond = where_clause.clone().unwrap_or_else(|| "true".to_string());
            let ids = ids_where(tx, schema_name, &t, &cond).await?;
            sqlx::query(&format!(
                "UPDATE \"{schema_name}\".\"{t}\" \
                 SET doc = jsonb_set(doc, '{{\"{set}\"}}', to_jsonb(({expr})), true) \
                 WHERE {cond}"
            ))
            .execute(&mut **tx)
            .await?;
            // Recompute every indexed `f_` column from the just-rewritten `doc`
            // for exactly the rows that were rewritten. Scoping by the captured
            // ids (rather than re-evaluating `cond`) is strictly correct even
            // when `expr` modifies a field that `cond` tests — a `cond`-scoped
            // recompute could otherwise miss an updated row whose new doc no
            // longer matches `cond`, leaving its `f_` column stale. Mirrors the
            // `setDefault` arm's `recompute_columns_for_ids` pattern.
            recompute_all_indexed(tx, schema_name, &t, table, derived, &ids).await?;
            let n = ids.len() as i64;
            fx.touched.insert(table.clone());
            push_ops(&mut fx.ops, table, &ids, OpKind::Patch);
            Ok(DirectiveReport {
                op: "evalExpr".into(),
                affected_rows: n,
                ..Default::default()
            })
        }
    }
}

fn table_def<'a>(schema: &'a SchemaDef, table: &str) -> Result<&'a TableDef, RtDbError> {
    schema
        .tables
        .get(table)
        .ok_or_else(|| RtDbError::internal(format!("table '{table}' missing from schema")))
}

/// Reads the stored schema from the db's `meta` row inside `tx`. Before Task 6's
/// committer arm updates `meta`, this is the authoritative pre-migration schema.
async fn load_schema_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    schema_name: &str,
) -> Result<Option<SchemaDef>, RtDbError> {
    let row: Option<(serde_json::Value,)> = sqlx::query_as(&format!(
        "SELECT value FROM \"{schema_name}\".meta WHERE key = 'schema'"
    ))
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

async fn rewrite_doc_key(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    schema_name: &str,
    table: &str,
    from: &str,
    to: &str,
) -> Result<i64, RtDbError> {
    let res = sqlx::query(&format!(
        "UPDATE \"{schema_name}\".\"{table}\" \
         SET doc = jsonb_set(doc - '{from}', '{{\"{to}\"}}', doc->'{from}', true) \
         WHERE doc ? '{from}'"
    ))
    .execute(&mut **tx)
    .await?;
    Ok(res.rows_affected() as i64)
}

/// Recomputes a typed `f_` column for `ids` using `cast_expr` (the
/// `ddl::backfill_expr` pattern), scoped to exactly the rows that just received
/// a default. `cast_expr` reads `doc->>'{field}'`, which is now populated.
async fn recompute_columns_for_ids(
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
    sqlx::query(&format!(
        "UPDATE \"{schema_name}\".\"{table}\" SET \"{col}\" = {cast_expr} WHERE id = ANY($1)"
    ))
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
async fn recompute_all_indexed(
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
    sqlx::query(&format!(
        "UPDATE \"{schema_name}\".\"{table_pg}\" SET {sets_clause} WHERE id = ANY($1)"
    ))
    .bind(ids)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn all_ids(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    schema_name: &str,
    table: &str,
) -> Result<Vec<String>, RtDbError> {
    ids_where(tx, schema_name, table, "true").await
}

/// Selects `id` for rows matching `cond`. `cond` is composed from validated
/// field identifiers (regex-clean per the schema layer), never user data — the
/// same pattern `ddl::push_schema` uses for its `WHERE doc ? '{field}'` backfill.
async fn ids_where(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    schema_name: &str,
    table: &str,
    cond: &str,
) -> Result<Vec<String>, RtDbError> {
    let rows: Vec<(String,)> = sqlx::query_as(&format!(
        "SELECT id FROM \"{schema_name}\".\"{table}\" WHERE {cond}"
    ))
    .fetch_all(&mut **tx)
    .await?;
    Ok(rows.into_iter().map(|r| r.0).collect())
}

fn push_ops(ops: &mut Vec<DocOp>, table: &str, ids: &[String], kind: OpKind) {
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
fn coerce_value(cast: Cast, v: &serde_json::Value) -> Option<serde_json::Value> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directive_round_trip() {
        let req = MigrateRequest {
            directives: vec![
                Directive::RenameField {
                    table: "users".into(),
                    from: "name".into(),
                    to: "fullName".into(),
                },
                Directive::ChangeType {
                    table: "users".into(),
                    field: "age".into(),
                    to: FieldType::String,
                    cast: Cast::ToString,
                    default: None,
                },
                Directive::EvalExpr {
                    table: "users".into(),
                    set: "upper".into(),
                    expr: "upper(doc->>'fullName')".into(),
                    where_clause: Some("doc ? 'fullName'".into()),
                },
            ],
            dry_run: true,
        };
        let json = serde_json::to_value(&req).unwrap();
        // tag is "op", camelCase keys, `where` alias.
        assert_eq!(json["directives"][0]["op"], "renameField");
        assert_eq!(json["directives"][1]["op"], "changeType");
        assert_eq!(json["directives"][1]["cast"], "toString");
        assert_eq!(json["directives"][2]["where"], "doc ? 'fullName'");
        assert_eq!(json["dryRun"], true);
        let back: MigrateRequest = serde_json::from_value(json).unwrap();
        assert!(back.dry_run);
        assert_eq!(back.directives.len(), 3);
    }

    #[test]
    fn directive_report_default_supports_spread() {
        // Later tasks construct DirectiveReport with `..Default::default()`.
        let report = DirectiveReport {
            op: "renameField".into(),
            affected_rows: 3,
            ..Default::default()
        };
        assert_eq!(report.op, "renameField");
        assert_eq!(report.affected_rows, 3);
        assert!(report.cast_failures.is_empty());
        assert!(report.sample_changes.is_empty());
    }

    use std::collections::BTreeMap;

    fn one_table_schema() -> SchemaDef {
        let mut fields = BTreeMap::new();
        fields.insert("name".into(), FieldType::String);
        fields.insert("age".into(), FieldType::Number);
        let mut tables = BTreeMap::new();
        tables.insert(
            "users".into(),
            TableDef {
                fields,
                indexes: vec![],
                owner_field: None,
                collaborators_field: None,
                ttl: None,
                authorize: None,
            },
        );
        SchemaDef { tables }
    }

    #[test]
    fn plan_rename_field_derives_schema() {
        let old = one_table_schema();
        let d = vec![Directive::RenameField {
            table: "users".into(),
            from: "name".into(),
            to: "fullName".into(),
        }];
        let got = plan_migration(&old, &d).unwrap();
        assert!(got.tables["users"].fields.contains_key("fullName"));
        assert!(!got.tables["users"].fields.contains_key("name"));
    }

    #[test]
    fn plan_rejects_missing_source_field() {
        let old = one_table_schema();
        let d = vec![Directive::RenameField {
            table: "users".into(),
            from: "nope".into(),
            to: "x".into(),
        }];
        assert!(plan_migration(&old, &d).is_err());
    }

    #[test]
    fn plan_rejects_taken_rename_target() {
        let old = one_table_schema();
        // renaming `name` -> `age` collides with the existing `age`
        let d = vec![Directive::RenameField {
            table: "users".into(),
            from: "name".into(),
            to: "age".into(),
        }];
        assert!(plan_migration(&old, &d).is_err());
    }

    #[test]
    fn cast_matrix_locked() {
        // Lock `cast_valid_for`'s matrix: one (ideally several) accepted and one
        // rejected source type per cast. Mirrors the spec's coercion table.
        for (cast, old) in [
            (Cast::ToString, FieldType::String),
            (Cast::ToString, FieldType::Number),
            (Cast::ToString, FieldType::Boolean),
            (Cast::ToString, FieldType::Int64),
            (Cast::ToNumber, FieldType::String),
            (Cast::ToNumber, FieldType::Boolean),
            (Cast::ToNumber, FieldType::Int64),
            (Cast::ToInt64, FieldType::String),
            (Cast::ToInt64, FieldType::Number),
            (Cast::ToBoolean, FieldType::String),
            (Cast::ToBoolean, FieldType::Number),
        ] {
            assert!(cast_valid_for(cast, &old), "{cast:?} should accept {old:?}");
        }
        // Rejected source types: Object has no sound coercion for any cast, and
        // a couple of near-miss scalar pairs that fall outside the matrix
        // (ToInt64 rejects Boolean; ToBoolean rejects Int64).
        let object = FieldType::Object {
            fields: BTreeMap::new(),
        };
        for (cast, old) in [
            (Cast::ToString, object.clone()),
            (Cast::ToNumber, object.clone()),
            (Cast::ToInt64, object.clone()),
            (Cast::ToBoolean, object.clone()),
            (Cast::ToInt64, FieldType::Boolean),
            (Cast::ToBoolean, FieldType::Int64),
        ] {
            assert!(
                !cast_valid_for(cast, &old),
                "{cast:?} should reject {old:?}"
            );
        }
    }

    #[test]
    fn plan_rejects_cast_with_no_accepted_source() {
        // ToString on an Object field has no valid coercion → rejected end-to-end.
        let mut fields = BTreeMap::new();
        fields.insert(
            "meta".into(),
            FieldType::Object {
                fields: BTreeMap::new(),
            },
        );
        let mut tables = BTreeMap::new();
        tables.insert(
            "things".into(),
            TableDef {
                fields,
                indexes: vec![],
                owner_field: None,
                collaborators_field: None,
                ttl: None,
                authorize: None,
            },
        );
        let schema = SchemaDef { tables };
        let d = vec![Directive::ChangeType {
            table: "things".into(),
            field: "meta".into(),
            to: FieldType::String,
            cast: Cast::ToString,
            default: None,
        }];
        assert!(plan_migration(&schema, &d).is_err());
    }

    #[test]
    fn plan_rejects_evalexpr_with_from_clause() {
        let old = one_table_schema();
        let d = vec![Directive::EvalExpr {
            table: "users".into(),
            set: "name".into(),
            expr: "x FROM other".into(),
            where_clause: None,
        }];
        assert!(plan_migration(&old, &d).is_err());
    }

    #[test]
    fn plan_drop_table_removes_it() {
        let old = one_table_schema();
        let d = vec![Directive::DropTable {
            name: "users".into(),
        }];
        let got = plan_migration(&old, &d).unwrap();
        assert!(got.tables.is_empty());
    }

    /// Table with an index, ownerField, and collaboratorsField so the rename/drop
    /// fixup arms have something to rewrite. `by_owner` indexes `ownerId` so a
    /// rename of `ownerId` must follow it in `index.fields`.
    fn schema_with_auth_and_index() -> SchemaDef {
        let mut fields = BTreeMap::new();
        fields.insert("email".into(), FieldType::String);
        fields.insert("ownerId".into(), FieldType::String);
        fields.insert(
            "collabs".into(),
            FieldType::Array {
                element: Box::new(FieldType::String),
            },
        );
        let mut tables = BTreeMap::new();
        tables.insert(
            "users".into(),
            TableDef {
                fields,
                indexes: vec![crate::schema::IndexDef {
                    name: "by_owner".into(),
                    fields: vec!["ownerId".into()],
                    search: false,
                    vector: None,
                    unique: false,
                    r#where: None,
                    language: None,
                }],
                owner_field: Some("ownerId".into()),
                collaborators_field: Some("collabs".into()),
                ttl: None,
                authorize: None,
            },
        );
        SchemaDef { tables }
    }

    // plan fixup: renameField rewrites index.fields + ownerField +
    // collaboratorsField that referenced the old name.
    #[test]
    fn plan_rename_field_fixes_index_owner_and_collab_refs() {
        let old = schema_with_auth_and_index();
        let d = vec![
            Directive::RenameField {
                table: "users".into(),
                from: "ownerId".into(),
                to: "uid".into(),
            },
            Directive::RenameField {
                table: "users".into(),
                from: "collabs".into(),
                to: "members".into(),
            },
        ];
        let got = plan_migration(&old, &d).unwrap();
        let t = &got.tables["users"];
        // index reference followed the rename.
        assert_eq!(t.indexes[0].fields, vec!["uid".to_string()]);
        // ownerField + collaboratorsField followed their respective renames.
        assert_eq!(t.owner_field.as_deref(), Some("uid"));
        assert_eq!(t.collaborators_field.as_deref(), Some("members"));
        // old field names are gone.
        assert!(!t.fields.contains_key("ownerId"));
        assert!(!t.fields.contains_key("collabs"));
    }

    // plan fixup: dropField prunes index.fields and clears ownerField /
    // collaboratorsField that named the dropped field.
    #[test]
    fn plan_drop_field_clears_index_owner_and_collab_refs() {
        let old = schema_with_auth_and_index();
        let d = vec![
            Directive::DropField {
                table: "users".into(),
                field: "ownerId".into(),
            },
            Directive::DropField {
                table: "users".into(),
                field: "collabs".into(),
            },
        ];
        let got = plan_migration(&old, &d).unwrap();
        let t = &got.tables["users"];
        assert!(
            t.indexes[0].fields.is_empty(),
            "index pruned of dropped field"
        );
        assert!(t.owner_field.is_none(), "ownerField cleared");
        assert!(
            t.collaborators_field.is_none(),
            "collaboratorsField cleared"
        );
    }

    // `authorize` carries across a field rename: every `field` reference inside
    // the predicate follows the rename (mirrors ownerField/collaboratorsField).
    #[test]
    fn plan_rename_field_carries_authorize_predicate() {
        use crate::query::FilterExpr;
        let mut old = schema_with_auth_and_index();
        // visibility OR owner==caller — references renamed field `ownerId`.
        old.tables
            .get_mut("users")
            .unwrap()
            .fields
            .insert("visibility".into(), FieldType::String);
        old.tables.get_mut("users").unwrap().authorize = Some(FilterExpr::Or {
            exprs: vec![
                FilterExpr::Eq {
                    field: "ownerId".into(),
                    value: serde_json::json!({"$user": true}),
                },
                FilterExpr::Eq {
                    field: "visibility".into(),
                    value: serde_json::json!("public"),
                },
            ],
        });
        let d = vec![Directive::RenameField {
            table: "users".into(),
            from: "ownerId".into(),
            to: "uid".into(),
        }];
        let got = plan_migration(&old, &d).unwrap();
        let t = &got.tables["users"];
        // The predicate now references `uid`, not `ownerId`, and the unrelated
        // `visibility` leaf is untouched.
        let expr = t.authorize.as_ref().expect("authorize preserved");
        match expr {
            FilterExpr::Or { exprs } => match (&exprs[0], &exprs[1]) {
                (FilterExpr::Eq { field: f0, .. }, FilterExpr::Eq { field: f1, .. }) => {
                    assert_eq!(f0, "uid");
                    assert_eq!(f1, "visibility");
                }
                other => panic!("expected two Eq leaves, got {other:?}"),
            },
            other => panic!("expected Or, got {other:?}"),
        }
    }

    // Dropping a field the `authorize` predicate still references is rejected:
    // silently clearing the predicate would widen access (load-bearing for auth).
    #[test]
    fn plan_drop_field_rejects_when_authorize_references_it() {
        use crate::error::ErrorCode;
        use crate::query::FilterExpr;
        let mut old = schema_with_auth_and_index();
        old.tables
            .get_mut("users")
            .unwrap()
            .fields
            .insert("visibility".into(), FieldType::String);
        old.tables.get_mut("users").unwrap().authorize = Some(FilterExpr::Eq {
            field: "visibility".into(),
            value: serde_json::json!("public"),
        });
        let d = vec![Directive::DropField {
            table: "users".into(),
            field: "visibility".into(),
        }];
        let err = plan_migration(&old, &d).unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(
            err.message.contains("authorize"),
            "should explain the authorize conflict: {}",
            err.message
        );
        // Dropping an unrelated field is unaffected.
        let d2 = vec![Directive::DropField {
            table: "users".into(),
            field: "email".into(),
        }];
        assert!(plan_migration(&old, &d2).is_ok());
    }

    // evalExpr `set` is interpolated into the jsonb_set key literal, so a stray
    // quote (the classic injection shape) must be rejected as BAD_REQUEST at plan
    // time — not reach the scoped-raw-SQL applier. Locks `is_valid_identifier`.
    #[test]
    fn plan_rejects_evalexpr_set_with_stray_quote() {
        let old = one_table_schema();
        let d = vec![Directive::EvalExpr {
            table: "users".into(),
            set: "name' WHERE 1=1".into(),
            expr: "'x'".into(),
            where_clause: None,
        }];
        let err = plan_migration(&old, &d).unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::BadRequest);
        assert!(
            err.message.contains("valid field name"),
            "should explain the set-name rejection: {}",
            err.message
        );
    }

    // renameTable rewrites a self-referential `Id { table: <old self> }` inside
    // the renamed table itself — the renamed table is removed from the map before
    // the cross-table fixup loop, so without the self-ref pass it would dangle.
    #[test]
    fn plan_rename_table_rewrites_self_referential_id() {
        let mut fields = BTreeMap::new();
        fields.insert("name".into(), FieldType::String);
        fields.insert(
            "parent".into(),
            FieldType::Id {
                table: "node".into(),
            },
        );
        let mut tables = BTreeMap::new();
        tables.insert(
            "node".into(),
            TableDef {
                fields,
                indexes: vec![],
                owner_field: None,
                collaborators_field: None,
                ttl: None,
                authorize: None,
            },
        );
        let old = SchemaDef { tables };
        let d = vec![Directive::RenameTable {
            from: "node".into(),
            to: "nodes".into(),
        }];
        let got = plan_migration(&old, &d).unwrap();
        assert!(got.tables.contains_key("nodes"));
        assert_eq!(
            got.tables["nodes"].fields["parent"],
            FieldType::Id {
                table: "nodes".into()
            },
            "self-referential Id must follow the rename"
        );
    }

    // renameTable rewrites Id references in OTHER tables (companion to the
    // self-ref case above); locks the pre-existing cross-table behavior.
    #[test]
    fn plan_rename_table_rewrites_cross_table_id_refs() {
        let mut user_fields = BTreeMap::new();
        user_fields.insert("name".into(), FieldType::String);
        let mut account_fields = BTreeMap::new();
        account_fields.insert(
            "owner".into(),
            FieldType::Id {
                table: "user".into(),
            },
        );
        let mut tables = BTreeMap::new();
        tables.insert(
            "user".into(),
            TableDef {
                fields: user_fields,
                indexes: vec![],
                owner_field: None,
                collaborators_field: None,
                ttl: None,
                authorize: None,
            },
        );
        tables.insert(
            "account".into(),
            TableDef {
                fields: account_fields,
                indexes: vec![],
                owner_field: None,
                collaborators_field: None,
                ttl: None,
                authorize: None,
            },
        );
        let old = SchemaDef { tables };
        let d = vec![Directive::RenameTable {
            from: "user".into(),
            to: "users".into(),
        }];
        let got = plan_migration(&old, &d).unwrap();
        assert_eq!(
            got.tables["account"].fields["owner"],
            FieldType::Id {
                table: "users".into()
            },
            "cross-table Id must follow the rename"
        );
    }

    #[test]
    fn coerce_value_emits_target_forms() {
        use serde_json::json;
        // ToNumber emits a JSON number (f64). NaN/inf strings parse as f64 but
        // are rejected by the `is_finite()` guard rather than collapsing to
        // Value::Null via serde_json's f64→Number path.
        assert_eq!(
            coerce_value(Cast::ToNumber, &json!("42")),
            Some(json!(42.0))
        );
        assert_eq!(coerce_value(Cast::ToNumber, &json!(true)), Some(json!(1.0)));
        assert_eq!(
            coerce_value(Cast::ToNumber, &json!(false)),
            Some(json!(0.0))
        );
        assert!(coerce_value(Cast::ToNumber, &json!("NaN")).is_none());
        assert!(coerce_value(Cast::ToNumber, &json!("inf")).is_none());
        assert!(coerce_value(Cast::ToNumber, &json!("not-a-number")).is_none());
        // ToInt64 emits a decimal-string JSON value (int64 wire convention —
        // see schema::is_valid_int64 and FEATURE_MATRIX.md #13).
        assert_eq!(
            coerce_value(Cast::ToInt64, &json!("123")),
            Some(json!("123"))
        );
        assert_eq!(coerce_value(Cast::ToInt64, &json!(456)), Some(json!("456")));
        // A float-valued JSON number with a fractional part does not fit i64.
        assert!(coerce_value(Cast::ToInt64, &json!(456.5)).is_none());
        assert!(coerce_value(Cast::ToInt64, &json!("not-an-int")).is_none());
        // ToString / ToBoolean round out the matrix.
        assert_eq!(coerce_value(Cast::ToString, &json!(42)), Some(json!("42")));
        assert_eq!(
            coerce_value(Cast::ToString, &json!(true)),
            Some(json!("true"))
        );
        assert_eq!(
            coerce_value(Cast::ToBoolean, &json!("true")),
            Some(json!(true))
        );
        assert_eq!(
            coerce_value(Cast::ToBoolean, &json!("0")),
            Some(json!(false))
        );
        assert_eq!(
            coerce_value(Cast::ToBoolean, &json!(0.0)),
            Some(json!(false))
        );
        assert_eq!(coerce_value(Cast::ToBoolean, &json!(3)), Some(json!(true)));
        assert!(coerce_value(Cast::ToBoolean, &json!("maybe")).is_none());
        // Objects/arrays/null have no coercion under any cast.
        assert!(coerce_value(Cast::ToString, &json!({"a":1})).is_none());
        assert!(coerce_value(Cast::ToNumber, &json!([1])).is_none());
        assert!(coerce_value(Cast::ToInt64, &json!(null)).is_none());
    }
}
