//! Migration DB applier (QA-003 split): runs each validated directive's
//! DDL + DML inside a caller-supplied transaction. The invariant it upholds:
//! every typed `f_<field>` column stays consistent with the `doc` jsonb after
//! each directive. The pre-migration schema is read from the db's `meta` row
//! inside the tx, then advanced per directive into a `working` copy, so a
//! later directive resolves an entity an earlier one renamed/changed by its
//! current name. `derived` (from `plan_migration`) is the full post-batch
//! schema and supplies each arm's final field type/column.

use crate::ddl::{
    backfill_expr, indexed_fields, pg_col, pg_schema, pg_search_col, pg_sequence, pg_table,
    pg_vector_col,
};
use crate::error::RtDbError;
use crate::schema::{
    FieldType, SchemaDef, TableDef, check_constraints_for_field, indexed_column_type,
};
use crate::txn::{DocOp, OpKind};
use std::collections::BTreeSet;

use super::backfill::{
    all_ids, bind_execute, coerce_value, computed_field_names, computed_fields_referencing,
    ids_where, load_schema_in_tx, push_ops, recompute_all_indexed, recompute_columns_for_ids,
    restamp_computed_fields, rewrite_doc_key,
};
use super::plan::{require_field_ident, require_index_ident, require_table_ident, validate_one};
use super::{CondSource, Directive, DirectiveReport, ExprSource, MigrateBind};
use crate::value_expr::Cast;
use crate::value_expr::compile_value_expr;

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

/// Bundles the per-directive apply context (the tx, the schema names, and the
/// effects accumulator) shared by every `apply_*` DB-applier and `apply_one`,
/// replacing the repeated argument tuple each used to carry individually.
struct MigrateCtx<'a, 'c> {
    tx: &'a mut sqlx::Transaction<'c, sqlx::Postgres>,
    schema_name: &'a str,
    old: &'a SchemaDef,
    derived: &'a SchemaDef,
    fx: &'a mut MigrationEffects,
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
        let mut ctx = MigrateCtx {
            tx: &mut *tx,
            schema_name: &schema_name,
            old: &working,
            derived,
            fx: &mut fx,
        };
        let report = apply_one(&mut ctx, d).await?;
        // Advance `working` past this directive. `plan_migration` already proved
        // the whole sequence folds cleanly on `old`, so this cannot error here.
        validate_one(&mut working, d)?;
        fx.reports.push(report);
    }
    let _ = dry_run; // dry_run only governs commit/rollback in the caller
    Ok(fx)
}

async fn apply_one(
    ctx: &mut MigrateCtx<'_, '_>,
    d: &Directive,
) -> Result<DirectiveReport, RtDbError> {
    match d {
        Directive::RenameField { table, from, to } => {
            apply_rename_field(ctx, table, from, to).await
        }
        Directive::RenameTable { from, to } => apply_rename_table(ctx, from, to).await,
        Directive::DropField { table, field } => apply_drop_field(ctx, table, field).await,
        Directive::DropTable { name } => apply_drop_table(ctx, name).await,
        Directive::DropIndex { table, name } => apply_drop_index(ctx, table, name).await,
        Directive::SetDefault {
            table,
            field,
            value,
        } => apply_set_default(ctx, table, field, value).await,
        Directive::ChangeType {
            table,
            field,
            to,
            cast,
            default,
        } => apply_change_type(ctx, table, field, to, cast, default).await,
        Directive::EvalExpr {
            table,
            set,
            expr,
            where_clause,
        } => apply_eval_expr(ctx, table, set, expr, where_clause).await,
    }
}

async fn apply_rename_field(
    ctx: &mut MigrateCtx<'_, '_>,
    table: &str,
    from: &str,
    to: &str,
) -> Result<DirectiveReport, RtDbError> {
    let tx = &mut *ctx.tx;
    let schema_name = ctx.schema_name;
    let old = ctx.old;
    let fx = &mut *ctx.fx;
    // SEC-124: `from` and `to` are interpolated into the SQL string literals
    // below (`doc ? '{from}'`, `doc - '{from}'`, `doc->'{from}'`, and the
    // `jsonb_set` path literal in `rewrite_doc_key`). A real check — the prior
    // `debug_assert!` compiled away under `--release` (the Dockerfile builds
    // release). `from` is also existence-checked in `validate_one`, and `to`
    // by `derived.validate()` upstream; this is the defense-in-depth backstop.
    require_field_ident(from)?;
    require_field_ident(to)?;
    let t = pg_table(table);
    // Rename the typed column only if the source field is indexed
    // (checked on the pre-migration table — the column still bears the
    // old name). RENAME COLUMN preserves the column's type and values,
    // and Postgres rewrites index column references to follow it, so
    // the index keeps working. Do NOT recompute the column as text.
    let old_table = table_def(old, table)?;
    if indexed_fields(old_table).contains(from) {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "ALTER TABLE \"{schema_name}\".\"{t}\" RENAME COLUMN \"{}\" TO \"{}\"",
            pg_col(from),
            pg_col(to)
        )))
        .execute(&mut **tx)
        .await?;
    }
    let ids = ids_where(tx, schema_name, &t, &format!("doc ? '{from}'"), &[]).await?;
    let n = rewrite_doc_key(tx, schema_name, &t, from, to).await?;
    fx.touched.insert(table.to_string());
    push_ops(&mut fx.ops, table, &ids, OpKind::Patch);
    Ok(DirectiveReport {
        op: "renameField".into(),
        affected_rows: n,
        ..Default::default()
    })
}

async fn apply_rename_table(
    ctx: &mut MigrateCtx<'_, '_>,
    from: &str,
    to: &str,
) -> Result<DirectiveReport, RtDbError> {
    let tx = &mut *ctx.tx;
    let schema_name = ctx.schema_name;
    let fx = &mut *ctx.fx;
    // SEC-124: `from`/`to` flow into a `pg_table()` physical ident inside a
    // double-quoted DDL literal; `pg_table` only lowercases + prefixes, so a
    // quote-bearing name would still break out of the `"..."` quoting. Real
    // check (prior site had no backstop at all).
    require_table_ident(from)?;
    require_table_ident(to)?;
    // Physical table rename; docs are untouched -> no DocOps, but the
    // table is recorded as touched so subscriptions re-run.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE \"{schema_name}\".\"{}\" RENAME TO \"{}\"",
        pg_table(from),
        pg_table(to)
    )))
    .execute(&mut **tx)
    .await?;
    // The auto-increment sequence is standalone and named after the table
    // (`seq_<table>`); it must follow the rename or the next insert's
    // `nextval` targets a missing relation.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER SEQUENCE IF EXISTS \"{schema_name}\".\"{}\" RENAME TO \"{}\"",
        pg_sequence(from),
        pg_sequence(to)
    )))
    .execute(&mut **tx)
    .await?;
    fx.touched.insert(to.to_string());
    Ok(DirectiveReport {
        op: "renameTable".into(),
        affected_rows: 0,
        ..Default::default()
    })
}

async fn apply_drop_field(
    ctx: &mut MigrateCtx<'_, '_>,
    table: &str,
    field: &str,
) -> Result<DirectiveReport, RtDbError> {
    let tx = &mut *ctx.tx;
    let schema_name = ctx.schema_name;
    let old = ctx.old;
    let fx = &mut *ctx.fx;
    // SEC-124: see RenameField. `field` is interpolated unbound into
    // `doc ? '{field}'` and `doc - '{field}'` literals.
    require_field_ident(field)?;
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
    let ids = ids_where(tx, schema_name, &t, &format!("doc ? '{field}'"), &[]).await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE \"{schema_name}\".\"{t}\" SET doc = doc - '{field}' \
         WHERE doc ? '{field}'"
    )))
    .execute(&mut **tx)
    .await?;
    // Drop the typed column (no-op if the field was never indexed).
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE \"{schema_name}\".\"{t}\" DROP COLUMN IF EXISTS \"{}\"",
        pg_col(field)
    )))
    .execute(&mut **tx)
    .await?;
    fx.touched.insert(table.to_string());
    push_ops(&mut fx.ops, table, &ids, OpKind::Patch);
    Ok(DirectiveReport {
        op: "dropField".into(),
        affected_rows: ids.len() as i64,
        ..Default::default()
    })
}

async fn apply_drop_table(
    ctx: &mut MigrateCtx<'_, '_>,
    name: &str,
) -> Result<DirectiveReport, RtDbError> {
    let tx = &mut *ctx.tx;
    let schema_name = ctx.schema_name;
    let fx = &mut *ctx.fx;
    let t = pg_table(name);
    let ids = all_ids(tx, schema_name, &t).await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP TABLE \"{schema_name}\".\"{t}\""
    )))
    .execute(&mut **tx)
    .await?;
    // Standalone auto-increment sequence: nothing cascades to it, so it is
    // dropped explicitly (guarded — tables without a counter are unaffected).
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP SEQUENCE IF EXISTS \"{schema_name}\".\"{}\"",
        pg_sequence(name)
    )))
    .execute(&mut **tx)
    .await?;
    fx.touched.insert(name.to_string());
    push_ops(&mut fx.ops, name, &ids, OpKind::Delete);
    Ok(DirectiveReport {
        op: "dropTable".into(),
        affected_rows: ids.len() as i64,
        ..Default::default()
    })
}

async fn apply_drop_index(
    ctx: &mut MigrateCtx<'_, '_>,
    table: &str,
    name: &str,
) -> Result<DirectiveReport, RtDbError> {
    let tx = &mut *ctx.tx;
    let schema_name = ctx.schema_name;
    let old = ctx.old;
    let derived = ctx.derived;
    let fx = &mut *ctx.fx;
    // SEC-124: `table` and `name` are composed into the `i_{table}_{name}`
    // index ident (both lowercased) and dropped inside a `"..."` literal;
    // a quote-bearing value would still escape the quoting (prior site had
    // no backstop). `name` is index-scoped (30-char cap, like push_schema).
    require_table_ident(table)?;
    require_index_ident(name)?;
    // Index ident mirrors ddl::push_schema's `i_{table}_{name}` (both
    // lowercased). No DocOps; the table is touched so subscriptions re-run.
    let idx = format!("i_{}_{}", table.to_lowercase(), name.to_lowercase());
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP INDEX IF EXISTS \"{schema_name}\".\"{idx}\""
    )))
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
        // …and its `tg_` trigram GIN (FM-30), dropped before any column it
        // references.
        let trgm_idx = format!("tg_{}_{}", table.to_lowercase(), name.to_lowercase());
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP INDEX IF EXISTS \"{schema_name}\".\"{trgm_idx}\""
        )))
        .execute(&mut **tx)
        .await?;
        let sv_col = pg_search_col(name);
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "ALTER TABLE \"{schema_name}\".\"{t_ident}\" DROP COLUMN IF EXISTS \"{sv_col}\""
        )))
        .execute(&mut **tx)
        .await?;
    }
    // A dropped vector index leaves its write-maintained `v_` vector(N)
    // column (the vector field itself lives in `doc` jsonb, not a typed
    // `f_` column).
    if dropped_index.and_then(|i| i.vector.as_ref()).is_some() {
        let v_col = pg_vector_col(name);
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "ALTER TABLE \"{schema_name}\".\"{t_ident}\" DROP COLUMN IF EXISTS \"{v_col}\""
        )))
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
    let dropped_fields: Vec<String> = dropped_index.map(|i| i.fields.clone()).unwrap_or_default();
    for field_name in dropped_fields {
        if still_indexed.contains(&field_name) {
            continue;
        }
        let col = pg_col(&field_name);
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "ALTER TABLE \"{schema_name}\".\"{t_ident}\" DROP COLUMN IF EXISTS \"{col}\""
        )))
        .execute(&mut **tx)
        .await?;
    }
    fx.touched.insert(table.to_string());
    Ok(DirectiveReport {
        op: "dropIndex".into(),
        affected_rows: 0,
        ..Default::default()
    })
}

async fn apply_set_default(
    ctx: &mut MigrateCtx<'_, '_>,
    table: &str,
    field: &str,
    value: &serde_json::Value,
) -> Result<DirectiveReport, RtDbError> {
    let tx = &mut *ctx.tx;
    let schema_name = ctx.schema_name;
    let derived = ctx.derived;
    let fx = &mut *ctx.fx;
    // SEC-124: see RenameField. `field` is interpolated unbound into
    // `NOT doc ? '{field}'` and a jsonb_set path literal.
    require_field_ident(field)?;
    let t = pg_table(table);
    let value_json =
        serde_json::to_string(value).map_err(|e| RtDbError::internal(e.to_string()))?;
    // Capture the rows lacking the field BEFORE the update — after the
    // update they have it, so the `WHERE NOT doc ? '{field}'` predicate
    // would no longer match them.
    let ids = ids_where(tx, schema_name, &t, &format!("NOT doc ? '{field}'"), &[]).await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE \"{schema_name}\".\"{t}\" \
         SET doc = jsonb_set(doc, '{{\"{field}\"}}', $1::jsonb, true) \
         WHERE NOT doc ? '{field}'"
    )))
    .bind(&value_json)
    .execute(&mut **tx)
    .await?;
    // If the field is indexed, recompute its typed `f_` column for the
    // affected rows (they now carry the default). `derived` carries the
    // field's post-migration type.
    let derived_table = table_def(derived, table)?;
    if indexed_fields(derived_table).contains(field) {
        let fty = derived_table.fields.get(field).ok_or_else(|| {
            RtDbError::internal(format!("setDefault targets absent field '{table}.{field}'"))
        })?;
        let (pg_type, _) = indexed_column_type(fty)?;
        let col = pg_col(field);
        let expr = backfill_expr(pg_type, field)?;
        recompute_columns_for_ids(tx, schema_name, &t, &col, &expr, &ids).await?;
    }
    // ENH-028 invariant (stored computed value == expr over the doc): the
    // defaulted field may feed a computed expression — after this rewrite the
    // stored stamp would be stale until each row's next write, so re-derive
    // the dependent entries for exactly the rows that received the default,
    // then refresh their typed columns (`recompute_all_indexed` — the
    // dependent computed fields may be indexed even when `field` is not).
    let dependents = computed_fields_referencing(derived_table, field);
    if !dependents.is_empty() {
        restamp_computed_fields(tx, schema_name, &t, table, derived, &dependents, &ids).await?;
        recompute_all_indexed(tx, schema_name, &t, table, derived, &ids).await?;
    }
    let n = ids.len() as i64;
    fx.touched.insert(table.to_string());
    push_ops(&mut fx.ops, table, &ids, OpKind::Patch);
    Ok(DirectiveReport {
        op: "setDefault".into(),
        affected_rows: n,
        ..Default::default()
    })
}

async fn apply_change_type(
    ctx: &mut MigrateCtx<'_, '_>,
    table: &str,
    field: &str,
    to: &FieldType,
    cast: &Cast,
    default: &Option<serde_json::Value>,
) -> Result<DirectiveReport, RtDbError> {
    let tx = &mut *ctx.tx;
    let schema_name = ctx.schema_name;
    let old = ctx.old;
    let derived = ctx.derived;
    let fx = &mut *ctx.fx;
    // SEC-124: see RenameField. `field` is interpolated unbound into
    // `doc ? '{field}'`, `doc->'{field}'`, and a jsonb_set path literal.
    require_field_ident(field)?;
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
    let ids = ids_where(tx, schema_name, &t, &format!("doc ? '{field}'"), &[]).await?;
    for id in &ids {
        let row: Option<(Option<serde_json::Value>,)> = sqlx::query_as(sqlx::AssertSqlSafe(
            format!("SELECT doc->'{field}' FROM \"{schema_name}\".\"{t}\" WHERE id = $1"),
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
        // Constraint re-validation on the coerced value: the retyped field's
        // declared constraints (if any) must hold in every migrated row, so a
        // `changeType` cannot push existing data outside its declared bounds
        // (the whole migrate txn fails and rolls back, naming the field).
        check_constraints_for_field(table_def(derived, table)?, field, &new_val)?;
        let s = serde_json::to_string(&new_val).map_err(|e| RtDbError::internal(e.to_string()))?;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE \"{schema_name}\".\"{t}\" \
             SET doc = jsonb_set(doc, '{{\"{field}\"}}', $1::jsonb, true) \
             WHERE id = $2"
        )))
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
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "ALTER TABLE \"{schema_name}\".\"{t}\" \
             ALTER COLUMN \"{col}\" TYPE {pg_type} USING ({using_expr})"
        )))
        .execute(&mut **tx)
        .await?;
    }
    // ENH-028 invariant (stored computed value == expr over the doc): the
    // cast rewrote the stored values of `field`; every computed expression
    // reading it must re-derive for the affected rows (the carriers captured
    // above — untouched rows keep their stamp), then refresh typed columns.
    // `derived` carries the post-batch computed map, so a directive sequence
    // (rename, then changeType on the renamed field) resolves dependents by
    // the field's current name — the same resolution `apply_eval_expr` uses.
    let dependents = computed_fields_referencing(table_def(derived, table)?, field);
    if !dependents.is_empty() {
        restamp_computed_fields(tx, schema_name, &t, table, derived, &dependents, &ids).await?;
        recompute_all_indexed(tx, schema_name, &t, table, derived, &ids).await?;
    }
    fx.touched.insert(table.to_string());
    push_ops(&mut fx.ops, table, &ids, OpKind::Patch);
    Ok(DirectiveReport {
        op: "changeType".into(),
        affected_rows: ids.len() as i64,
        ..Default::default()
    })
}

async fn apply_eval_expr(
    ctx: &mut MigrateCtx<'_, '_>,
    table: &str,
    set: &str,
    expr: &ExprSource,
    where_clause: &Option<CondSource>,
) -> Result<DirectiveReport, RtDbError> {
    let tx = &mut *ctx.tx;
    let schema_name = ctx.schema_name;
    let derived = ctx.derived;
    let fx = &mut *ctx.fx;
    // Capture the affected ids BEFORE the rewrite using the same predicate so
    // DocOps cover exactly the rows about to change. Scoping the indexed-column
    // recompute by these ids (not by re-evaluating the predicate) is strictly
    // correct even when `expr` modifies a field the predicate tests — a
    // predicate-scoped recompute could miss a row whose new doc no longer
    // matches it, leaving its `f_` column stale. Mirrors `setDefault`.
    let t = pg_table(table);
    let derived_table = table_def(derived, table)?;

    match (expr, where_clause) {
        // ENH-020 typed path: `expr` is a closed `ValueExpr`, `where` a
        // `FilterExpr`. Every literal is bound `$n`; every field reads
        // `doc->'field'`. The grammar is closed, so this path cannot carry the
        // SEC-107 injection — there is no raw-SQL node to interpolate.
        (ExprSource::Typed(ve), cond) => {
            // Compile the typed `where` once for the id-SELECT ($1..).
            let (cond_sql, cond_binds) = match cond {
                Some(CondSource::Typed(f)) => {
                    let (sql, binds) = crate::query::compile_filter(f, derived_table, 1, false)?;
                    (sql, binds)
                }
                Some(CondSource::Legacy(_)) => {
                    return Err(RtDbError::bad_request(
                        "evalExpr typed 'expr' requires a typed 'where' (FilterExpr); \
                         a legacy raw-SQL 'where' is not allowed on the typed path",
                    ));
                }
                None => ("true".to_string(), Vec::new()),
            };
            let cond_migrate_binds: Vec<MigrateBind> =
                cond_binds.iter().cloned().map(Into::into).collect();
            let ids = ids_where(tx, schema_name, &t, &cond_sql, &cond_migrate_binds).await?;

            // Compile `expr` ($1..), then re-compile `where` to continue
            // numbering ($m+1..) for the UPDATE statement.
            let mut expr_binds: Vec<MigrateBind> = Vec::new();
            let expr_sql = compile_value_expr(ve, derived_table, 1, &mut expr_binds)?;
            let (cond_sql2, cond_binds2) = match cond {
                Some(CondSource::Typed(f)) => {
                    let start = 1 + expr_binds.len();
                    crate::query::compile_filter(f, derived_table, start, false)?
                }
                _ => ("true".to_string(), Vec::new()),
            };
            let mut all_binds = expr_binds;
            all_binds.extend(cond_binds2.into_iter().map(Into::into));
            let update_sql = format!(
                "UPDATE \"{schema_name}\".\"{t}\" \
                 SET doc = jsonb_set(doc, '{{\"{set}\"}}', to_jsonb(({expr_sql})), true) \
                 WHERE {cond_sql2}"
            );
            bind_execute(&update_sql, &all_binds, tx).await?;

            restamp_computed_fields(
                tx,
                schema_name,
                &t,
                table,
                derived,
                &computed_field_names(derived_table),
                &ids,
            )
            .await?;
            recompute_all_indexed(tx, schema_name, &t, table, derived, &ids).await?;
            Ok(report(table, ids, fx))
        }
        // Legacy raw-SQL path (SEC-107 boundary, gated to root admin_key by
        // `admin_migrate`). `expr` and `where` run as-is against this db's
        // `doc`. Retained for one deprecation cycle under dual-accept.
        (ExprSource::Legacy(raw), where_clause) => {
            let (cond_sql, binds): (String, Vec<MigrateBind>) = match where_clause {
                Some(CondSource::Legacy(cond)) => (cond.clone(), Vec::new()),
                Some(CondSource::Typed(_)) => {
                    return Err(RtDbError::bad_request(
                        "evalExpr legacy raw-SQL 'expr' requires a legacy raw-SQL 'where'; \
                         a typed 'where' is not allowed on the legacy path",
                    ));
                }
                None => ("true".to_string(), Vec::new()),
            };
            let ids = ids_where(tx, schema_name, &t, &cond_sql, &binds).await?;
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE \"{schema_name}\".\"{t}\" \
                 SET doc = jsonb_set(doc, '{{\"{set}\"}}', to_jsonb(({raw})), true) \
                 WHERE {cond_sql}"
            )))
            .execute(&mut **tx)
            .await?;
            // The legacy rewrite feeds the same computed re-stamp: whichever
            // path rewrote the doc, computed values derived from pre-rewrite
            // inputs would be stale until each row's next write.
            restamp_computed_fields(
                tx,
                schema_name,
                &t,
                table,
                derived,
                &computed_field_names(derived_table),
                &ids,
            )
            .await?;
            recompute_all_indexed(tx, schema_name, &t, table, derived, &ids).await?;
            Ok(report(table, ids, fx))
        }
    }
}

/// Every computed field name on `table`, in declaration order — the re-stamp
/// set for a doc rewrite whose expression may feed any of them.
fn report(table: &str, ids: Vec<String>, fx: &mut MigrationEffects) -> DirectiveReport {
    let n = ids.len() as i64;
    fx.touched.insert(table.to_string());
    push_ops(&mut fx.ops, table, &ids, OpKind::Patch);
    DirectiveReport {
        op: "evalExpr".into(),
        affected_rows: n,
        ..Default::default()
    }
}

pub(super) fn table_def<'a>(schema: &'a SchemaDef, table: &str) -> Result<&'a TableDef, RtDbError> {
    schema
        .tables
        .get(table)
        .ok_or_else(|| RtDbError::internal(format!("table '{table}' missing from schema")))
}
