//! Migration planning (QA-003 split): the pure, no-DB half — validating a
//! directive batch against the current schema and folding it into the derived
//! post-migration `SchemaDef`. Rejects missing sources, taken targets, invalid
//! casts, and expressions referencing undeclared fields. The async DB-applier
//! half lives in `super::apply`.

use crate::error::RtDbError;
use crate::schema::{
    FieldType, MAX_FIELD_NAME_LEN, MAX_INDEX_NAME_LEN, MAX_TABLE_NAME_LEN, SchemaDef, TableDef,
    TableDefExt, is_valid_identifier, validate_constraints_entry,
};

use crate::value_expr::{Cast, ValueExpr};

use super::{CondSource, Directive, ExprSource};

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
    // Same discipline for the constraints maps: directive folding must not be
    // able to invalidate an entry (a backstop — each applicable rule already
    // fires in the per-directive planners above).
    for (table_name, table_def) in &schema.tables {
        table_def.validate_constraints(table_name)?;
    }
    Ok(schema)
}

pub(super) fn validate_one(schema: &mut SchemaDef, d: &Directive) -> Result<(), RtDbError> {
    match d {
        Directive::RenameField { table, from, to } => plan_rename_field(schema, table, from, to),
        Directive::RenameTable { from, to } => plan_rename_table(schema, from, to),
        Directive::ChangeType {
            table,
            field,
            to: new_ty,
            cast,
            ..
        } => plan_change_type(schema, table, field, new_ty, *cast),
        Directive::DropField { table, field } => plan_drop_field(schema, table, field),
        Directive::DropTable { name } => plan_drop_table(schema, name),
        Directive::DropIndex { table, name } => plan_drop_index(schema, table, name),
        Directive::SetDefault { table, field, .. } => plan_set_default(schema, table, field),
        Directive::EvalExpr {
            table,
            set,
            expr,
            where_clause,
        } => plan_eval_expr(schema, table, set, expr, where_clause.as_ref()),
    }
}

/// Rewrite every reference to field `from` on `table` to `to`, in place. This
/// is the single chokepoint for the "reference" half of a field rename — the
/// `fields` map key move and the `computed`/`defaults` map key moves are the
/// *declaration* half and stay in `plan_rename_field` alongside it. Every
/// other name-bearing surface on `TableDef` that can name a field goes here:
/// index field lists, `ownerField`, `collaboratorsField`, `autoIncrementField`,
/// `updatedAtField`, `ttl.field`, the `authorize` predicate, and every
/// `computed` expression's `Field` references (including `Case.whens`
/// predicates). Enumerated directly from the `TableDef` struct in
/// `schema.rs` — if a new name-bearing field is added there, add its rewrite
/// here too.
fn rename_field_refs(table: &mut TableDef, from: &str, to: &str) {
    for ix in table.indexes.iter_mut() {
        for f in ix.fields.iter_mut() {
            if f == from {
                *f = to.to_string();
            }
        }
    }
    if table.owner_field.as_deref() == Some(from) {
        table.owner_field = Some(to.to_string());
    }
    if table.collaborators_field.as_deref() == Some(from) {
        table.collaborators_field = Some(to.to_string());
    }
    if table.auto_increment_field.as_deref() == Some(from) {
        table.auto_increment_field = Some(to.to_string());
    }
    if table.updated_at_field.as_deref() == Some(from) {
        table.updated_at_field = Some(to.to_string());
    }
    if let Some(ttl) = table.ttl.as_mut()
        && ttl.field == from
    {
        ttl.field = to.to_string();
    }
    if let Some(expr) = table.authorize.as_mut() {
        rename_filter_fields(expr, from, to);
    }
    for expr in table.computed.values_mut() {
        rename_value_expr_fields(expr, from, to);
    }
}

fn plan_rename_field(
    schema: &mut SchemaDef,
    table: &str,
    from: &str,
    to: &str,
) -> Result<(), RtDbError> {
    let t = table_mut(schema, table)?;
    if t.fields.contains_key(to) {
        return Err(RtDbError::bad_request(format!(
            "rename target '{table}.{to}' already exists"
        )));
    }
    let ft = t.fields.remove(from).ok_or_else(|| {
        RtDbError::bad_request(format!("renamed field '{table}.{from}' does not exist"))
    })?;
    t.fields.insert(to.to_string(), ft);
    // ENH-028: the computed map follows the rename the way `defaults` does —
    // an entry KEYED on the renamed field moves to the new name (its declared
    // field moved; leaving it keyed on `from` would fail `validate_computed`'s
    // declared-field rule on the derived schema). Input values are unchanged
    // by the rename, so stored computed values stay correct; the next write
    // re-stamps.
    if let Some(expr) = t.computed.remove(from) {
        t.computed.insert(to.to_string(), expr);
    }
    if let Some(value) = t.defaults.remove(from) {
        t.defaults.insert(to.to_string(), value);
    }
    // The constraints map follows the rename the same way `defaults` does:
    // an entry KEYED on the renamed field moves to the new name.
    if let Some(fc) = t.constraints.remove(from) {
        t.constraints.insert(to.to_string(), fc);
    }
    rename_field_refs(t, from, to);
    Ok(())
}

fn plan_rename_table(schema: &mut SchemaDef, from: &str, to: &str) -> Result<(), RtDbError> {
    if schema.tables.contains_key(to) {
        return Err(RtDbError::bad_request(format!(
            "rename target table '{to}' already exists"
        )));
    }
    let mut def = schema
        .tables
        .remove(from)
        .ok_or_else(|| RtDbError::bad_request(format!("renamed table '{from}' does not exist")))?;
    // Id references to `from` follow the rename. The renamed table was just
    // removed from the map, so the loop over the remaining tables would skip
    // its own self-referential `Id { table: from }` fields — rewrite those
    // here first, lest the rename leave a dangling ref.
    for ft in def.fields.values_mut() {
        if let FieldType::Id { table, .. } = ft
            && table == from
        {
            *table = to.to_string();
        }
    }
    for t in schema.tables.values_mut() {
        for ft in t.fields.values_mut() {
            if let FieldType::Id { table, .. } = ft
                && table == from
            {
                *table = to.to_string();
            }
        }
    }
    schema.tables.insert(to.to_string(), def);
    Ok(())
}

fn plan_change_type(
    schema: &mut SchemaDef,
    table: &str,
    field: &str,
    new_ty: &FieldType,
    cast: Cast,
) -> Result<(), RtDbError> {
    let t = table_mut(schema, table)?;
    let old_ty = t.fields.get(field).ok_or_else(|| {
        RtDbError::bad_request(format!("changed field '{table}.{field}' does not exist"))
    })?;
    if !cast_valid_for(cast, old_ty) {
        return Err(RtDbError::bad_request(format!(
            "cast {cast:?} is not valid for {table}.{field}"
        )));
    }
    t.fields.insert(field.to_string(), new_ty.clone());
    // A default was validated against the OLD type; the retyped field may no
    // longer accept it, so the entry goes rather than risking a
    // push-time-invalid derived schema (re-declare it on a later push).
    t.defaults.remove(field);
    // Constraints re-validation: the entry (if any) must still apply to the
    // NEW type — a `changeType` that would leave `min` on a string field, or
    // a `pattern` on a number, is rejected here at plan time with the same
    // typed error the push path produces.
    if let Some(fc) = t.constraints.get(field) {
        validate_constraints_entry(field, new_ty, fc)?;
    }
    Ok(())
}

fn plan_drop_field(schema: &mut SchemaDef, table: &str, field: &str) -> Result<(), RtDbError> {
    let t = table_mut(schema, table)?;
    // `authorize` is load-bearing for auth: silently clearing it (as
    // ownerField/collaboratorsField are below) would widen access. A field
    // the predicate still references must be untied explicitly — reject the
    // migration so the caller amends `authorize` first.
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
    if t.owner_field.as_deref() == Some(field) {
        t.owner_field = None;
    }
    if t.collaborators_field.as_deref() == Some(field) {
        t.collaborators_field = None;
    }
    if t.auto_increment_field.as_deref() == Some(field) {
        t.auto_increment_field = None;
    }
    if t.updated_at_field.as_deref() == Some(field) {
        t.updated_at_field = None;
    }
    // Cleared the way `ownerField`/`collaboratorsField`/`autoIncrementField`
    // are above — the dropped field is gone, so the `ttl` declaration naming
    // it can no longer resolve.
    if t.ttl.as_ref().is_some_and(|ttl| ttl.field == field) {
        t.ttl = None;
    }
    // ENH-028: a computed expression reading the dropped field would dangle —
    // every future write fails its stamp. Reject, naming the computed field,
    // so the caller amends the computed map first (a push removing the entry
    // leaves stored values in place).
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
    // discipline): the applier removes the stored key from every doc, so
    // leaving the entry would fail `validate_computed`'s declared-field rule
    // on the derived schema.
    t.computed.remove(field);
    t.defaults.remove(field);
    t.constraints.remove(field);
    Ok(())
}

fn plan_drop_table(schema: &mut SchemaDef, name: &str) -> Result<(), RtDbError> {
    if schema.tables.remove(name).is_none() {
        return Err(RtDbError::bad_request(format!(
            "dropped table '{name}' does not exist"
        )));
    }
    Ok(())
}

fn plan_drop_index(schema: &mut SchemaDef, table: &str, name: &str) -> Result<(), RtDbError> {
    let t = table_mut(schema, table)?;
    if !t.indexes.iter().any(|ix| ix.name == name) {
        return Err(RtDbError::bad_request(format!(
            "dropped index '{table}.{name}' does not exist"
        )));
    }
    t.indexes.retain(|ix| ix.name != name);
    Ok(())
}

fn plan_set_default(schema: &mut SchemaDef, table: &str, field: &str) -> Result<(), RtDbError> {
    let t = table_mut(schema, table)?;
    if !t.fields.contains_key(field) {
        return Err(RtDbError::bad_request(format!(
            "setDefault target '{table}.{field}' does not exist"
        )));
    }
    // data-only; schema unchanged
    Ok(())
}

fn plan_eval_expr(
    schema: &mut SchemaDef,
    table: &str,
    set: &str,
    expr: &ExprSource,
    where_clause: Option<&CondSource>,
) -> Result<(), RtDbError> {
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
    // ENH-020 / SEC-107: a typed `ValueExpr` payload is validated here — every
    // `Field` must name a declared field on this table, and the grammar is
    // closed (no subquery / function-call-by-name / raw-SQL node), so a typed
    // `expr` cannot carry an injection by construction. The typed `where` (a
    // `FilterExpr`) is likewise field-validated here via the same
    // `validate_filter_expr_fields` chokepoint the read path uses. The legacy
    // string `expr`/`where` forms remain raw SQL interpolated unbound — their
    // containment boundary is the admin gate (`admin_migrate` admits a legacy
    // `evalExpr` only to the root admin_key holder, whose reach it does not
    // expand), retained for one deprecation cycle under the dual-accept
    // rollout.
    match expr {
        ExprSource::Typed(ve) => validate_value_expr_fields(ve, t)?,
        ExprSource::Legacy(_) => {}
    }
    if let Some(CondSource::Typed(f)) = where_clause {
        crate::schema::validate_filter_expr_fields(f, t, false, false)
            .map_err(|e| RtDbError::bad_request(e.message))?;
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
/// field rename (mirroring the ownerField/collaboratorsField rewrite).
/// `par_rt_db_core::engine::rename_filter_fields` (ARC-004) — shared with the
/// Rust client's in-memory engine.
fn rename_filter_fields(expr: &mut crate::query::FilterExpr, from: &str, to: &str) {
    par_rt_db_core::engine::rename_filter_fields(expr, from, to)
}

/// The `ValueExpr` half of the rename: rewrites every `Field` reference equal
/// to `from` to `to`, in place — the `&mut` mirror of
/// `value_expr::walk_value_expr_fields`. `Case.whens` predicates reuse
/// `rename_filter_fields` (the same rewrite `authorize` gets), so a rename
/// carries computed expressions across intact. `to` is fresh (the RenameField
/// arm rejects an existing target), so no reference set can collide.
/// `par_rt_db_core::engine::rename_value_expr_fields` (ARC-004) — shared with
/// the Rust client's in-memory engine.
fn rename_value_expr_fields(expr: &mut ValueExpr, from: &str, to: &str) {
    par_rt_db_core::engine::rename_value_expr_fields(expr, from, to)
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
pub(super) fn cast_valid_for(cast: Cast, old: &FieldType) -> bool {
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
pub(super) fn require_field_ident(name: &str) -> Result<(), RtDbError> {
    if !is_valid_identifier(name, MAX_FIELD_NAME_LEN) {
        return Err(RtDbError::bad_request(format!(
            "invalid field identifier '{name}'"
        )));
    }
    Ok(())
}

pub(super) fn require_table_ident(name: &str) -> Result<(), RtDbError> {
    if !is_valid_identifier(name, MAX_TABLE_NAME_LEN) {
        return Err(RtDbError::bad_request(format!(
            "invalid table identifier '{name}'"
        )));
    }
    Ok(())
}

pub(super) fn require_index_ident(name: &str) -> Result<(), RtDbError> {
    if !is_valid_identifier(name, MAX_INDEX_NAME_LEN) {
        return Err(RtDbError::bad_request(format!(
            "invalid index identifier '{name}'"
        )));
    }
    Ok(())
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
