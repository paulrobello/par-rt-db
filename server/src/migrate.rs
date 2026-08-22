//! Declarative schema migration: an ordered list of directives the server
//! applies transactionally to transform a database's schema and documents.
//! See docs/superpowers/specs/2026-07-31-schema-migration-backfill-design.md.
use crate::ddl::{
    backfill_expr, indexed_fields, pg_col, pg_schema, pg_search_col, pg_sequence, pg_table,
    pg_vector_col,
};
use crate::error::RtDbError;
use crate::schema::{
    FieldType, MAX_FIELD_NAME_LEN, MAX_INDEX_NAME_LEN, MAX_TABLE_NAME_LEN, SchemaDef, TableDef,
    indexed_column_type, is_valid_identifier,
};
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
        /// ENH-020: dual-accept. A typed `ValueExpr` (safe, all-literals-bound
        /// path) or a legacy raw-SQL string (deprecated, gated to the root
        /// admin_key — the SEC-107 boundary until the string form is removed).
        expr: ExprSource,
        /// Dual-accept `where`: a typed `FilterExpr` (safe) or a legacy raw-SQL
        /// predicate string (deprecated, same root-admin gate).
        #[serde(default, skip_serializing_if = "Option::is_none", rename = "where")]
        where_clause: Option<CondSource>,
    },
}

use crate::value_expr::compile_value_expr;
/// The `ValueExpr` grammar and its `Cast`/`CaseWhen` companions live in
/// `crate::value_expr` (shared with computed fields, ENH-028); re-exported so
/// `crate::migrate::ValueExpr` paths and the `Directive` arms keep resolving.
pub use crate::value_expr::{CaseWhen, Cast, ValueExpr};

/// Dual-accept source for `EvalExpr.expr`: a typed `ValueExpr` (the safe path)
/// or a legacy raw-SQL string (the deprecated path, gated to root admin_key).
/// `#[serde(untagged)]` tries `Typed` first; a string fails `ValueExpr` (an
/// internally-tagged object) and falls through to `Legacy`. A hostile object
/// that is not a valid `ValueExpr` fails both arms and is rejected — it does
/// NOT silently become legacy.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum ExprSource {
    Typed(ValueExpr),
    Legacy(String),
}

/// Dual-accept source for `EvalExpr.where`: a typed `FilterExpr` or a legacy
/// raw-SQL predicate string. Same untagged discipline as `ExprSource`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum CondSource {
    Typed(crate::query::FilterExpr),
    Legacy(String),
}

/// A bind for the migrate expression path. `EqBind`'s four typed variants cover
/// `FilterExpr` values (compiled via `compile_filter`); `Json` covers
/// `ValueExpr::Literal` (any JSON value, bound as jsonb). The two coexist in one
/// UPDATE statement with contiguous `$n` numbering. `pub(crate)` because
/// `value_expr::compile_value_expr` fills this vec from the migrate call site.
#[derive(Debug, Clone)]
pub(crate) enum MigrateBind {
    Text(String),
    Num(f64),
    Bool(bool),
    I64(i64),
    Json(serde_json::Value),
}

impl From<crate::txn::EqBind> for MigrateBind {
    fn from(b: crate::txn::EqBind) -> Self {
        match b {
            crate::txn::EqBind::Text(s) => MigrateBind::Text(s),
            crate::txn::EqBind::Num(f) => MigrateBind::Num(f),
            crate::txn::EqBind::Bool(b) => MigrateBind::Bool(b),
            crate::txn::EqBind::I64(i) => MigrateBind::I64(i),
        }
    }
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
/// invalid for the old→new type pair; an `evalExpr` whose `set` field is not a
/// valid identifier, or that targets a missing table. `evalExpr`'s `expr`/`where`
/// SQL text is intentionally NOT parsed here — a denylist over SQL text cannot
/// be made sound, so containment is enforced structurally at the admin gate
/// (`admin_migrate` admits `evalExpr` only to the root admin_key holder, not to
/// delegated/OAuth-allowlist admins).
pub fn plan_migration(old: &SchemaDef, directives: &[Directive]) -> Result<SchemaDef, RtDbError> {
    let mut schema = old.clone();
    for d in directives {
        validate_one(&mut schema, d)?;
    }
    // Directive folding must not be able to invalidate a computed entry —
    // e.g. `changeType` retyping a computed field so its expression's static
    // kind no longer fits. Re-validate the derived maps here (pure) so the
    // plan fails before any DB work; the committer's `derived.validate()`
    // backstop covers the same rules through `validate_structure`.
    crate::schema::validate_computed(&schema)?;
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
            if t.auto_increment_field.as_deref() == Some(from.as_str()) {
                t.auto_increment_field = Some(to.clone());
            }
            if let Some(expr) = t.authorize.as_mut() {
                rename_filter_fields(expr, from, to);
            }
            // ENH-028: the computed map follows the rename the way `defaults`
            // does — an entry KEYED on the renamed field moves to the new name
            // (its declared field moved; leaving it keyed on `from` would fail
            // `validate_computed`'s declared-field rule on the derived schema),
            // and every expression's `Field` references (including
            // `Case.whens` predicates) are rewritten to read the renamed doc
            // key. Input values are unchanged by the rename, so stored
            // computed values stay correct; the next write re-stamps.
            if let Some(expr) = t.computed.remove(from) {
                t.computed.insert(to.clone(), expr);
            }
            for expr in t.computed.values_mut() {
                rename_value_expr_fields(expr, from, to);
            }
            if let Some(value) = t.defaults.remove(from) {
                t.defaults.insert(to.clone(), value);
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
                if let FieldType::Id { table, .. } = ft
                    && table == from
                {
                    *table = to.clone();
                }
            }
            for t in schema.tables.values_mut() {
                for ft in t.fields.values_mut() {
                    if let FieldType::Id { table, .. } = ft
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
            // A default was validated against the OLD type; the retyped field
            // may no longer accept it, so the entry goes rather than risking a
            // push-time-invalid derived schema (re-declare it on a later push).
            t.defaults.remove(field);
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
            if t.auto_increment_field.as_deref() == Some(field.as_str()) {
                t.auto_increment_field = None;
            }
            // ENH-028: a computed expression reading the dropped field would
            // dangle — every future write fails its stamp. Reject, naming the
            // computed field, so the caller amends the computed map first (a
            // push removing the entry leaves stored values in place).
            let mut computed_offender: Option<&String> = None;
            for (computed_field, expr) in &t.computed {
                let mut referenced = false;
                crate::value_expr::walk_value_expr_fields(expr, &mut |f| {
                    if f == field {
                        referenced = true;
                    }
                });
                if referenced {
                    computed_offender = Some(computed_field);
                    break;
                }
            }
            if let Some(computed_field) = computed_offender {
                return Err(RtDbError::bad_request(format!(
                    "cannot drop field '{table}.{field}': it is referenced by computed field '{table}.{computed_field}'; drop the computed field first"
                )));
            }
            // An entry KEYED on the dropped field goes with it (the `defaults`
            // discipline): the applier removes the stored key from every doc,
            // so leaving the entry would fail `validate_computed`'s
            // declared-field rule on the derived schema.
            t.computed.remove(field);
            t.defaults.remove(field);
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
            let t = table_mut(schema, table)?; // table must exist
            // `set` is a field path; the field need not exist (evalExpr may populate a
            // new key the caller adds via a later additive push), but the name must be
            // a valid identifier. It is interpolated into the `jsonb_set` key literal,
            // so a stray quote or backslash would otherwise break the SQL string.
            if !crate::schema::is_valid_identifier(set, crate::schema::MAX_FIELD_NAME_LEN) {
                return Err(RtDbError::bad_request(format!(
                    "evalExpr 'set' must be a valid field name, got '{set}'"
                )));
            }
            // ENH-020 / SEC-107: a typed `ValueExpr` payload is validated here —
            // every `Field` must name a declared field on this table, and the
            // grammar is closed (no subquery / function-call-by-name / raw-SQL
            // node), so a typed `expr` cannot carry an injection by construction.
            // The typed `where` (a `FilterExpr`) is likewise field-validated here
            // via the same `validate_filter_expr_fields` chokepoint the read path
            // uses. The legacy string `expr`/`where` forms remain raw SQL
            // interpolated unbound — their containment boundary is the admin gate
            // (`admin_migrate` admits a legacy `evalExpr` only to the root
            // admin_key holder, whose reach it does not expand), retained for one
            // deprecation cycle under the dual-accept rollout.
            match expr {
                ExprSource::Typed(ve) => validate_value_expr_fields(ve, t)?,
                ExprSource::Legacy(_) => {}
            }
            if let Some(CondSource::Typed(f)) = where_clause {
                crate::schema::validate_filter_expr_fields(f, t, false, false)
                    .map_err(|e| RtDbError::bad_request(e.message))?;
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
        | FilterExpr::Exists { field }
        | FilterExpr::OlderThan { field, .. } => {
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

/// The `ValueExpr` half of the rename: rewrites every `Field` reference equal
/// to `from` to `to`, in place — the `&mut` mirror of
/// `value_expr::walk_value_expr_fields`. `Case.whens` predicates reuse
/// `rename_filter_fields` (the same rewrite `authorize` gets), so a rename
/// carries computed expressions across intact. `to` is fresh (the RenameField
/// arm rejects an existing target), so no reference set can collide.
fn rename_value_expr_fields(expr: &mut ValueExpr, from: &str, to: &str) {
    match expr {
        ValueExpr::Field { field } => {
            if field == from {
                *field = to.to_string();
            }
        }
        ValueExpr::Literal { .. } | ValueExpr::Now => {}
        ValueExpr::Concat { parts } | ValueExpr::Coalesce { parts } => {
            for p in parts {
                rename_value_expr_fields(p, from, to);
            }
        }
        ValueExpr::Add { left, right }
        | ValueExpr::Sub { left, right }
        | ValueExpr::Mul { left, right }
        | ValueExpr::Div { left, right } => {
            rename_value_expr_fields(left, from, to);
            rename_value_expr_fields(right, from, to);
        }
        ValueExpr::Lower { value } | ValueExpr::Upper { value } | ValueExpr::Trim { value } => {
            rename_value_expr_fields(value, from, to);
        }
        ValueExpr::Cast { value, .. } => rename_value_expr_fields(value, from, to),
        ValueExpr::Case { whens, otherwise } => {
            for cw in whens {
                rename_filter_fields(&mut cw.when, from, to);
                rename_value_expr_fields(&mut cw.then, from, to);
            }
            rename_value_expr_fields(otherwise, from, to);
        }
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
        | FilterExpr::Exists { field: f }
        | FilterExpr::OlderThan { field: f, .. } => f == field,
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

// SEC-124 (recurring, prior SEC-008): the SQL-interpolation backstops here used
// to be `debug_assert!(is_valid_identifier(...))`, which the Dockerfile's
// `--release` build compiles away — so the stated control was absent from the
// deployed binary. These real checks return `BadRequest` instead, so a malformed
// identifier reaching the apply layer (a regression: upstream `SchemaDef::validate`
// and the existence checks in `validate_one` reject it first) is rejected loudly
// rather than emitted into SQL. Defense-in-depth — the bounds they enforce are
// also the bounds the interpolation depends on.
fn require_field_ident(name: &str) -> Result<(), RtDbError> {
    if !is_valid_identifier(name, MAX_FIELD_NAME_LEN) {
        return Err(RtDbError::bad_request(format!(
            "invalid field identifier '{name}'"
        )));
    }
    Ok(())
}

fn require_table_ident(name: &str) -> Result<(), RtDbError> {
    if !is_valid_identifier(name, MAX_TABLE_NAME_LEN) {
        return Err(RtDbError::bad_request(format!(
            "invalid table identifier '{name}'"
        )));
    }
    Ok(())
}

fn require_index_ident(name: &str) -> Result<(), RtDbError> {
    if !is_valid_identifier(name, MAX_INDEX_NAME_LEN) {
        return Err(RtDbError::bad_request(format!(
            "invalid index identifier '{name}'"
        )));
    }
    Ok(())
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
            apply_rename_field(tx, schema_name, old, table, from, to, fx).await
        }
        Directive::RenameTable { from, to } => {
            apply_rename_table(tx, schema_name, from, to, fx).await
        }
        Directive::DropField { table, field } => {
            apply_drop_field(tx, schema_name, old, table, field, fx).await
        }
        Directive::DropTable { name } => apply_drop_table(tx, schema_name, name, fx).await,
        Directive::DropIndex { table, name } => {
            apply_drop_index(tx, schema_name, old, derived, table, name, fx).await
        }
        Directive::SetDefault {
            table,
            field,
            value,
        } => apply_set_default(tx, schema_name, derived, table, field, value, fx).await,
        Directive::ChangeType {
            table,
            field,
            to,
            cast,
            default,
        } => {
            apply_change_type(
                tx,
                schema_name,
                old,
                derived,
                table,
                field,
                to,
                cast,
                default,
                fx,
            )
            .await
        }
        Directive::EvalExpr {
            table,
            set,
            expr,
            where_clause,
        } => apply_eval_expr(tx, schema_name, derived, table, set, expr, where_clause, fx).await,
    }
}

async fn apply_rename_field(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    schema_name: &str,
    old: &SchemaDef,
    table: &str,
    from: &str,
    to: &str,
    fx: &mut MigrationEffects,
) -> Result<DirectiveReport, RtDbError> {
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
        sqlx::query(&format!(
            "ALTER TABLE \"{schema_name}\".\"{t}\" RENAME COLUMN \"{}\" TO \"{}\"",
            pg_col(from),
            pg_col(to)
        ))
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
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    schema_name: &str,
    from: &str,
    to: &str,
    fx: &mut MigrationEffects,
) -> Result<DirectiveReport, RtDbError> {
    // SEC-124: `from`/`to` flow into a `pg_table()` physical ident inside a
    // double-quoted DDL literal; `pg_table` only lowercases + prefixes, so a
    // quote-bearing name would still break out of the `"..."` quoting. Real
    // check (prior site had no backstop at all).
    require_table_ident(from)?;
    require_table_ident(to)?;
    // Physical table rename; docs are untouched -> no DocOps, but the
    // table is recorded as touched so subscriptions re-run.
    sqlx::query(&format!(
        "ALTER TABLE \"{schema_name}\".\"{}\" RENAME TO \"{}\"",
        pg_table(from),
        pg_table(to)
    ))
    .execute(&mut **tx)
    .await?;
    // The auto-increment sequence is standalone and named after the table
    // (`seq_<table>`); it must follow the rename or the next insert's
    // `nextval` targets a missing relation.
    sqlx::query(&format!(
        "ALTER SEQUENCE IF EXISTS \"{schema_name}\".\"{}\" RENAME TO \"{}\"",
        pg_sequence(from),
        pg_sequence(to)
    ))
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
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    schema_name: &str,
    old: &SchemaDef,
    table: &str,
    field: &str,
    fx: &mut MigrationEffects,
) -> Result<DirectiveReport, RtDbError> {
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
    fx.touched.insert(table.to_string());
    push_ops(&mut fx.ops, table, &ids, OpKind::Patch);
    Ok(DirectiveReport {
        op: "dropField".into(),
        affected_rows: ids.len() as i64,
        ..Default::default()
    })
}

async fn apply_drop_table(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    schema_name: &str,
    name: &str,
    fx: &mut MigrationEffects,
) -> Result<DirectiveReport, RtDbError> {
    let t = pg_table(name);
    let ids = all_ids(tx, schema_name, &t).await?;
    sqlx::query(&format!("DROP TABLE \"{schema_name}\".\"{t}\""))
        .execute(&mut **tx)
        .await?;
    // Standalone auto-increment sequence: nothing cascades to it, so it is
    // dropped explicitly (guarded — tables without a counter are unaffected).
    sqlx::query(&format!(
        "DROP SEQUENCE IF EXISTS \"{schema_name}\".\"{}\"",
        pg_sequence(name)
    ))
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
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    schema_name: &str,
    old: &SchemaDef,
    derived: &SchemaDef,
    table: &str,
    name: &str,
    fx: &mut MigrationEffects,
) -> Result<DirectiveReport, RtDbError> {
    // SEC-124: `table` and `name` are composed into the `i_{table}_{name}`
    // index ident (both lowercased) and dropped inside a `"..."` literal;
    // a quote-bearing value would still escape the quoting (prior site had
    // no backstop). `name` is index-scoped (30-char cap, like push_schema).
    require_table_ident(table)?;
    require_index_ident(name)?;
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
        // …and its `tg_` trigram GIN (FM-30), dropped before any column it
        // references.
        let trgm_idx = format!("tg_{}_{}", table.to_lowercase(), name.to_lowercase());
        sqlx::query(&format!(
            "DROP INDEX IF EXISTS \"{schema_name}\".\"{trgm_idx}\""
        ))
        .execute(&mut **tx)
        .await?;
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
    let dropped_fields: Vec<String> = dropped_index.map(|i| i.fields.clone()).unwrap_or_default();
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
    fx.touched.insert(table.to_string());
    Ok(DirectiveReport {
        op: "dropIndex".into(),
        affected_rows: 0,
        ..Default::default()
    })
}

async fn apply_set_default(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    schema_name: &str,
    derived: &SchemaDef,
    table: &str,
    field: &str,
    value: &serde_json::Value,
    fx: &mut MigrationEffects,
) -> Result<DirectiveReport, RtDbError> {
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

#[allow(clippy::too_many_arguments)]
async fn apply_change_type(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    schema_name: &str,
    old: &SchemaDef,
    derived: &SchemaDef,
    table: &str,
    field: &str,
    to: &FieldType,
    cast: &Cast,
    default: &Option<serde_json::Value>,
    fx: &mut MigrationEffects,
) -> Result<DirectiveReport, RtDbError> {
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
        let s = serde_json::to_string(&new_val).map_err(|e| RtDbError::internal(e.to_string()))?;
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

#[allow(clippy::too_many_arguments)]
async fn apply_eval_expr(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    schema_name: &str,
    derived: &SchemaDef,
    table: &str,
    set: &str,
    expr: &ExprSource,
    where_clause: &Option<CondSource>,
    fx: &mut MigrationEffects,
) -> Result<DirectiveReport, RtDbError> {
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
            sqlx::query(&format!(
                "UPDATE \"{schema_name}\".\"{t}\" \
                 SET doc = jsonb_set(doc, '{{\"{set}\"}}', to_jsonb(({raw})), true) \
                 WHERE {cond_sql}"
            ))
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
fn computed_field_names(table: &TableDef) -> Vec<String> {
    table.computed.keys().cloned().collect()
}

/// Names of the table's computed entries whose expression reads `field` — the
/// re-stamp set after a directive (`setDefault`/`changeType`) rewrote that
/// field's stored values. The walk covers `Field` nodes and `Case.whens`
/// filter fields, matching what `validate_computed` treats as a reference.
fn computed_fields_referencing(table: &TableDef, field: &str) -> Vec<String> {
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
async fn restamp_computed_fields(
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
        let mut q = sqlx::query(&sql).bind(ids);
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

fn table_def<'a>(schema: &'a SchemaDef, table: &str) -> Result<&'a TableDef, RtDbError> {
    schema
        .tables
        .get(table)
        .ok_or_else(|| RtDbError::internal(format!("table '{table}' missing from schema")))
}

/// Walks a `ValueExpr` validating that every `Field` names a declared field on
/// `table` (the same chokepoint `FilterExpr` uses — `check_field_declared`).
/// Pure; called from `validate_one` so an unknown field fails the migration
/// plan before any DB work. `Case` branches recurse and their `when` predicates
/// go through the read path's `validate_filter_expr_fields`.
pub(crate) fn validate_value_expr_fields(
    ve: &ValueExpr,
    table: &TableDef,
) -> Result<(), RtDbError> {
    match ve {
        ValueExpr::Field { field } => {
            if !table.fields.contains_key(field) {
                return Err(RtDbError::bad_request(format!(
                    "evalExpr 'expr' references undeclared field '{field}'"
                )));
            }
        }
        ValueExpr::Literal { .. } | ValueExpr::Now => {}
        ValueExpr::Concat { parts } | ValueExpr::Coalesce { parts } => {
            for p in parts {
                validate_value_expr_fields(p, table)?;
            }
        }
        ValueExpr::Add { left, right }
        | ValueExpr::Sub { left, right }
        | ValueExpr::Mul { left, right }
        | ValueExpr::Div { left, right } => {
            validate_value_expr_fields(left, table)?;
            validate_value_expr_fields(right, table)?;
        }
        ValueExpr::Lower { value } | ValueExpr::Upper { value } | ValueExpr::Trim { value } => {
            validate_value_expr_fields(value, table)?
        }
        ValueExpr::Cast { value, .. } => validate_value_expr_fields(value, table)?,
        ValueExpr::Case { whens, otherwise } => {
            for cw in whens {
                crate::schema::validate_filter_expr_fields(&cw.when, table, false, false)
                    .map_err(|e| RtDbError::bad_request(e.message))?;
                validate_value_expr_fields(&cw.then, table)?;
            }
            validate_value_expr_fields(otherwise, table)?;
        }
    }
    Ok(())
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
    // SEC-124: `from`/`to` are interpolated unbound into the SQL string
    // literals (`doc - '{from}'`, `doc->'{from}'`, the `jsonb_set` path
    // `{{"{to}"}}`). The caller (`apply_rename_field`) already checks both,
    // but this helper's literals are a distinct interpolation site — re-check
    // so a future caller can't bypass it (prior site had no backstop).
    require_field_ident(from)?;
    require_field_ident(to)?;
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
    ids_where(tx, schema_name, table, "true", &[]).await
}

/// Selects `id` for rows matching `cond`. `cond` is composed from validated
/// field identifiers (regex-clean per the schema layer) for every caller EXCEPT
/// `apply_eval_expr`, where `cond` is the admin-supplied `where` SQL text gated
/// by the root admin_key (see `apply_eval_expr`). The `evalExpr` path is the
/// scoped raw-SQL escape hatch; all other callers pass `doc ? '{field}'` shapes
/// the same way `ddl::push_schema` does for its backfill.
async fn ids_where(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    schema_name: &str,
    table: &str,
    cond: &str,
    binds: &[MigrateBind],
) -> Result<Vec<String>, RtDbError> {
    let sql = format!("SELECT id FROM \"{schema_name}\".\"{table}\" WHERE {cond}");
    let mut q = sqlx::query_as::<_, (String,)>(&sql);
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
async fn bind_execute(
    sql: &str,
    binds: &[MigrateBind],
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), RtDbError> {
    let mut q = sqlx::query(sql);
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
                    expr: ExprSource::Legacy("upper(doc->>'fullName')".into()),
                    where_clause: Some(CondSource::Legacy("doc ? 'fullName'".into())),
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

    // ENH-028: directive folding must not be able to invalidate a computed
    // entry — a changeType that retypes a computed field so the expression's
    // static kind no longer fits fails the plan before any DB work.
    #[test]
    fn plan_migration_revalidates_computed_after_change_type() {
        let mut old = one_table_schema();
        let users = old.tables.get_mut("users").expect("users table");
        users.fields.insert("count".into(), FieldType::Number);
        users.computed.insert(
            "age".into(),
            ValueExpr::Add {
                left: Box::new(ValueExpr::Field {
                    field: "count".into(),
                }),
                right: Box::new(ValueExpr::Literal {
                    value: serde_json::json!(1),
                }),
            },
        );
        let d = vec![Directive::ChangeType {
            table: "users".into(),
            field: "age".into(),
            to: FieldType::String,
            cast: Cast::ToString,
            default: None,
        }];
        let err = plan_migration(&old, &d).unwrap_err();
        assert!(err.message.contains("produces a number"), "{}", err.message);
        // Without the computed entry the same fold still plans fine.
        let clean = one_table_schema();
        assert!(plan_migration(&clean, &d).is_ok());
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
    fn plan_rename_field_follows_auto_increment_declaration() {
        let mut old = one_table_schema();
        old.tables
            .get_mut("users")
            .expect("users")
            .fields
            .insert("ticketNum".into(), FieldType::Int64);
        old.tables
            .get_mut("users")
            .expect("users")
            .auto_increment_field = Some("ticketNum".into());
        let d = vec![Directive::RenameField {
            table: "users".into(),
            from: "ticketNum".into(),
            to: "number".into(),
        }];
        let got = plan_migration(&old, &d).unwrap();
        assert_eq!(
            got.tables["users"].auto_increment_field,
            Some("number".to_string()),
            "the declaration follows the renamed field"
        );
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

    // SEC-107: `plan_migration` intentionally does NOT inspect `expr`/`where` SQL
    // text. The prior `has_sql_violation` denylist was unsound (bypassed by
    // whitespace variants around ` FROM ` and by `SELECT` without `FROM`) and
    // was removed. Containment is now structural: `admin_migrate` admits an
    // evalExpr directive only under the root admin_key, never under a delegated
    // (OAuth-allowlist) admin — covered by the HTTP-layer tests in
    // migration_test.rs (`sec107_*`). This test pins the planner half: a
    // FROM-clause `expr` is ACCEPTED here (the gate is the control, not the
    // planner), so the next reader does not reintroduce a SQL denylist.
    #[test]
    fn plan_does_not_inspect_evalexpr_sql_text() {
        let old = one_table_schema();
        let d = vec![Directive::EvalExpr {
            table: "users".into(),
            set: "name".into(),
            expr: ExprSource::Legacy("x FROM other".into()),
            where_clause: None,
        }];
        assert!(
            plan_migration(&old, &d).is_ok(),
            "planner no longer SQL-gates evalExpr; the admin gate does"
        );
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
                defaults: std::collections::BTreeMap::new(),
                computed: std::collections::BTreeMap::new(),
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
                updated_at_field: None,
                auto_increment_field: None,
                authorize: None,

                soft_delete: false,
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

    // ENH-028: renameField carries the computed map — expression `Field`
    // references (and `Case.whens` predicate fields) follow the rename, and an
    // entry KEYED on the renamed field moves to the new name (the `defaults`
    // discipline; leaving it keyed on the old name would fail
    // `validate_computed`'s declared-field rule on the derived schema).
    #[test]
    fn plan_rename_field_rewrites_computed_refs_and_key() {
        use crate::query::FilterExpr;
        let mut old = one_table_schema();
        let users = old.tables.get_mut("users").expect("users table");
        users.fields.insert("first".into(), FieldType::String);
        users.fields.insert("nick".into(), FieldType::String);
        users.computed.insert(
            "name".into(),
            ValueExpr::Concat {
                parts: vec![
                    ValueExpr::Field {
                        field: "age".into(),
                    },
                    ValueExpr::Case {
                        whens: vec![CaseWhen {
                            when: FilterExpr::Exists {
                                field: "first".into(),
                            },
                            then: ValueExpr::Field {
                                field: "first".into(),
                            },
                        }],
                        otherwise: Box::new(ValueExpr::Literal {
                            value: serde_json::json!(""),
                        }),
                    },
                ],
            },
        );
        users.computed.insert(
            "nick".into(),
            ValueExpr::Field {
                field: "age".into(),
            },
        );
        let d = vec![
            Directive::RenameField {
                table: "users".into(),
                from: "first".into(),
                to: "givenName".into(),
            },
            Directive::RenameField {
                table: "users".into(),
                from: "nick".into(),
                to: "handle".into(),
            },
        ];
        let got = plan_migration(&old, &d).unwrap();
        let t = &got.tables["users"];
        // `name`'s expr reads the renamed field — in the Case `then` and the
        // Case `when` predicate alike (the `Field(age)` part is untouched).
        let name_expr = t.computed.get("name").expect("name entry preserved");
        let expected = ValueExpr::Concat {
            parts: vec![
                ValueExpr::Field {
                    field: "age".into(),
                },
                ValueExpr::Case {
                    whens: vec![CaseWhen {
                        when: FilterExpr::Exists {
                            field: "givenName".into(),
                        },
                        then: ValueExpr::Field {
                            field: "givenName".into(),
                        },
                    }],
                    otherwise: Box::new(ValueExpr::Literal {
                        value: serde_json::json!(""),
                    }),
                },
            ],
        };
        assert_eq!(name_expr, &expected);
        // The entry keyed on the renamed field moved; the old key is gone.
        assert!(t.computed.contains_key("handle"));
        assert!(!t.computed.contains_key("nick"));
    }

    // ENH-028: dropping a field a computed expression reads is rejected,
    // naming the computed field; dropping the field an entry is keyed on
    // removes the entry with it (the applier removes the stored key too).
    #[test]
    fn plan_drop_field_rejects_computed_reference_and_drops_keyed_entry() {
        let mut old = one_table_schema();
        let users = old.tables.get_mut("users").expect("users table");
        users.computed.insert(
            "age".into(),
            ValueExpr::Field {
                field: "name".into(),
            },
        );
        // Referenced field: rejected, naming the computed field.
        let d = vec![Directive::DropField {
            table: "users".into(),
            field: "name".into(),
        }];
        let err = plan_migration(&old, &d).unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::BadRequest);
        assert!(
            err.message.contains("computed field 'users.age'"),
            "should name the computed field: {}",
            err.message
        );
        // The keyed field: the entry goes with it, plan succeeds.
        let d2 = vec![Directive::DropField {
            table: "users".into(),
            field: "age".into(),
        }];
        let got = plan_migration(&old, &d2).unwrap();
        assert!(
            got.tables["users"].computed.is_empty(),
            "entry keyed on the dropped field is removed"
        );
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
            expr: ExprSource::Legacy("'x'".into()),
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
                on_delete: None,
            },
        );
        let mut tables = BTreeMap::new();
        tables.insert(
            "node".into(),
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
                table: "nodes".into(),
                on_delete: None,
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
                on_delete: None,
            },
        );
        let mut tables = BTreeMap::new();
        tables.insert(
            "user".into(),
            TableDef {
                defaults: std::collections::BTreeMap::new(),
                computed: std::collections::BTreeMap::new(),
                fields: user_fields,
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
        tables.insert(
            "account".into(),
            TableDef {
                defaults: std::collections::BTreeMap::new(),
                computed: std::collections::BTreeMap::new(),
                fields: account_fields,
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
        let old = SchemaDef { tables };
        let d = vec![Directive::RenameTable {
            from: "user".into(),
            to: "users".into(),
        }];
        let got = plan_migration(&old, &d).unwrap();
        assert_eq!(
            got.tables["account"].fields["owner"],
            FieldType::Id {
                table: "users".into(),
                on_delete: None,
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
