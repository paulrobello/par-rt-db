//! Schema-migration directive application (`apply_migration_directive`) and the
//! destructive-change detector.
//!
//! Extracted from `in_memory.rs` (QA-108). Pure file movement. The directive
//! applier widens from private to `pub(super)` so `migrate_schema` (in `mod.rs`)
//! can call it; `detect_destructive_changes` likewise widens to `pub(super)` so
//! `push_schema` can call it. The `admin` feature gating is unchanged.

use super::*;

#[cfg(feature = "admin")]
impl InMemoryRtDbClient {
    pub(super) fn apply_migration_directive(
        &mut self,
        planned: &mut SchemaDef,
        d: &crate::wire::admin::Directive,
    ) -> Result<(crate::wire::admin::DirectiveReport, Option<String>), RtDbError> {
        use crate::wire::admin::Directive;
        match d {
            Directive::RenameField { table, from, to } => {
                self.apply_rename_field_directive(planned, table, from, to)
            }
            Directive::RenameTable { from, to } => {
                self.apply_rename_table_directive(planned, from, to)
            }
            Directive::ChangeType {
                table,
                field,
                to,
                cast,
                default,
            } => {
                self.apply_change_type_directive(planned, table, field, to, cast, default.as_ref())
            }
            Directive::DropField { table, field } => {
                self.apply_drop_field_directive(planned, table, field)
            }
            Directive::DropTable { name } => self.apply_drop_table_directive(planned, name),
            Directive::DropIndex { table, name } => {
                self.apply_drop_index_directive(planned, table, name)
            }
            Directive::SetDefault {
                table,
                field,
                value,
            } => self.apply_set_default_directive(planned, table, field, value),
            Directive::EvalExpr { table, .. } => self.apply_eval_expr_directive(table),
        }
    }

    /// `renameField` directive — one function per directive kind, dispatched
    /// by [`Self::apply_migration_directive`]. Mirrors the ts/python engines'
    /// `applyRenameFieldDirective` / `_apply_rename_field_directive`.
    fn apply_rename_field_directive(
        &mut self,
        planned: &mut SchemaDef,
        table: &str,
        from: &str,
        to: &str,
    ) -> Result<(crate::wire::admin::DirectiveReport, Option<String>), RtDbError> {
        use crate::wire::admin::DirectiveReport;
        let t = migrate_table_mut(planned, table)?;
        if t.fields.contains_key(to) {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                format!("rename target '{table}.{to}' already exists"),
            ));
        }
        let ft = t.fields.remove(from).ok_or_else(|| {
            RtDbError::new(
                ErrorCode::BadRequest,
                format!("renamed field '{table}.{from}' does not exist"),
            )
        })?;
        t.fields.insert(to.to_string(), ft);
        if let Some(indexes) = t.indexes.as_mut() {
            for ix in indexes.iter_mut() {
                for f in ix.fields.iter_mut() {
                    if f == from {
                        *f = to.to_string();
                    }
                }
            }
        }
        if t.owner_field.as_deref() == Some(from) {
            t.owner_field = Some(to.to_string());
        }
        if t.collaborators_field.as_deref() == Some(from) {
            t.collaborators_field = Some(to.to_string());
        }
        // ENH-028: the computed map follows the rename the way the field,
        // indexes, and owner/collaborator hints do — an entry KEYED on the
        // renamed field moves to the new name (its declared field moved;
        // leaving it keyed on `from` would fail `validate_computed`'s
        // declared-field rule on the derived schema), and every expression's
        // `Field` references (including `Case.whens` predicates) are
        // rewritten to read the renamed doc key. Input values are unchanged
        // by the rename, so stored computed values stay correct; the next
        // write re-stamps. Mirrors server `migrate::validate_one`'s RenameField
        // arm.
        if let Some(expr) = t.computed.remove(from) {
            t.computed.insert(to.to_string(), expr);
        }
        for expr in t.computed.values_mut() {
            rename_value_expr_fields(expr, from, to);
        }
        let mut affected = 0i64;
        for ((tname, _), row) in self.docs.iter_mut() {
            if tname != table {
                continue;
            }
            if let Some(obj) = row.doc.as_object_mut()
                && let Some(v) = obj.remove(from)
            {
                obj.insert(to.to_string(), v);
                affected += 1;
            }
        }
        Ok((
            DirectiveReport {
                op: "renameField".into(),
                affected_rows: affected,
                ..Default::default()
            },
            Some(table.to_string()),
        ))
    }

    /// `renameTable` directive — one function per directive kind, dispatched
    /// by [`Self::apply_migration_directive`]. Mirrors the ts/python engines'
    /// `applyRenameTableDirective` / `_apply_rename_table_directive`.
    fn apply_rename_table_directive(
        &mut self,
        planned: &mut SchemaDef,
        from: &str,
        to: &str,
    ) -> Result<(crate::wire::admin::DirectiveReport, Option<String>), RtDbError> {
        use crate::wire::admin::DirectiveReport;
        if planned.tables.contains_key(to) {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                format!("rename target table '{to}' already exists"),
            ));
        }
        let def = planned.tables.remove(from).ok_or_else(|| {
            RtDbError::new(
                ErrorCode::BadRequest,
                format!("renamed table '{from}' does not exist"),
            )
        })?;
        // Id references to `from` in other tables follow the rename.
        for other in planned.tables.values_mut() {
            for ft in other.fields.values_mut() {
                if let FieldType::Id { table, .. } = ft
                    && table == from
                {
                    *table = to.to_string();
                }
            }
        }
        planned.tables.insert(to.to_string(), def);
        // Rename the live doc keys `(from, id)` → `(to, id)`.
        let ids: Vec<String> = self
            .docs
            .keys()
            .filter_map(|(t, id)| if t == from { Some(id.clone()) } else { None })
            .collect();
        for id in ids {
            if let Some(row) = self.docs.remove(&(from.to_string(), id.clone())) {
                self.docs.insert((to.to_string(), id), row);
            }
        }
        Ok((
            DirectiveReport {
                op: "renameTable".into(),
                affected_rows: 0,
                ..Default::default()
            },
            Some(to.to_string()),
        ))
    }

    /// `changeType` directive — one function per directive kind, dispatched
    /// by [`Self::apply_migration_directive`]. Mirrors the ts/python engines'
    /// `applyChangeTypeDirective` / `_apply_change_type_directive`.
    fn apply_change_type_directive(
        &mut self,
        planned: &mut SchemaDef,
        table: &str,
        field: &str,
        to: &FieldType,
        cast: &crate::wire::admin::Cast,
        default: Option<&Value>,
    ) -> Result<(crate::wire::admin::DirectiveReport, Option<String>), RtDbError> {
        use crate::wire::admin::DirectiveReport;
        let t = migrate_table_mut(planned, table)?;
        let old_ty = t.fields.get(field).ok_or_else(|| {
            RtDbError::new(
                ErrorCode::BadRequest,
                format!("changed field '{table}.{field}' does not exist"),
            )
        })?;
        if !cast_valid_for(*cast, old_ty) {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                format!("cast {cast:?} is not valid for {table}.{field}"),
            ));
        }
        // Drop the immutable borrow of `old_ty` before mutating `self.docs`.
        let field_owned = field.to_string();
        let mut affected = 0i64;
        for ((tname, _), row) in self.docs.iter_mut() {
            if tname != table {
                continue;
            }
            let Some(obj) = row.doc.as_object_mut() else {
                continue;
            };
            let Some(val) = obj.get(&field_owned).cloned() else {
                continue;
            };
            affected += 1;
            if let Some(coerced) = coerce_value(*cast, &val) {
                obj.insert(field_owned.clone(), coerced);
            } else if let Some(d) = default {
                let dv = coerce_value(*cast, d).unwrap_or_else(|| d.clone());
                obj.insert(field_owned.clone(), dv);
            } else {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    format!(
                        "changeType cannot coerce value in {table}.{} ({val}) and no default given",
                        row.id
                    ),
                ));
            }
        }
        // Fold the new type into the planned schema (field is guaranteed
        // present by the lookup above).
        t.fields.insert(field_owned, to.clone());
        Ok((
            DirectiveReport {
                op: "changeType".into(),
                affected_rows: affected,
                ..Default::default()
            },
            Some(table.to_string()),
        ))
    }

    /// `dropField` directive — one function per directive kind, dispatched
    /// by [`Self::apply_migration_directive`]. Mirrors the ts/python engines'
    /// `applyDropFieldDirective` / `_apply_drop_field_directive`.
    fn apply_drop_field_directive(
        &mut self,
        planned: &mut SchemaDef,
        table: &str,
        field: &str,
    ) -> Result<(crate::wire::admin::DirectiveReport, Option<String>), RtDbError> {
        use crate::wire::admin::DirectiveReport;
        let t = migrate_table_mut(planned, table)?;
        if t.fields.remove(field).is_none() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                format!("dropped field '{table}.{field}' does not exist"),
            ));
        }
        if let Some(indexes) = t.indexes.as_mut() {
            for ix in indexes.iter_mut() {
                ix.fields.retain(|f| f != field);
            }
        }
        if t.owner_field.as_deref() == Some(field) {
            t.owner_field = None;
        }
        if t.collaborators_field.as_deref() == Some(field) {
            t.collaborators_field = None;
        }
        // ENH-028: a computed expression reading the dropped field would
        // dangle — every future write fails its stamp. Reject, naming the
        // computed field, so the caller amends the computed map first (a
        // push removing the entry leaves stored values in place). Mirrors
        // server `migrate::validate_one`'s DropField arm.
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
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                format!(
                    "cannot drop field '{table}.{field}': it is referenced by computed field '{table}.{computed_field}'; drop the computed field first"
                ),
            ));
        }
        // An entry KEYED on the dropped field goes with it: the applier
        // removes the stored key from every doc, so leaving the entry would
        // fail `validate_computed`'s declared-field rule on the derived
        // schema.
        t.computed.remove(field);
        // `affected_rows` counts only rows whose `doc` actually changes
        // (rows carrying the field) — server parity. `obj.remove` returns
        // the removed value, so count the row iff the key was present.
        let mut affected = 0i64;
        for ((tname, _), row) in self.docs.iter_mut() {
            if tname != table {
                continue;
            }
            if let Some(obj) = row.doc.as_object_mut()
                && obj.remove(field).is_some()
            {
                affected += 1;
            }
        }
        Ok((
            DirectiveReport {
                op: "dropField".into(),
                affected_rows: affected,
                ..Default::default()
            },
            Some(table.to_string()),
        ))
    }

    /// `dropTable` directive — one function per directive kind, dispatched
    /// by [`Self::apply_migration_directive`]. Mirrors the ts/python engines'
    /// `applyDropTableDirective` / `_apply_drop_table_directive`.
    fn apply_drop_table_directive(
        &mut self,
        planned: &mut SchemaDef,
        name: &str,
    ) -> Result<(crate::wire::admin::DirectiveReport, Option<String>), RtDbError> {
        use crate::wire::admin::DirectiveReport;
        if planned.tables.remove(name).is_none() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                format!("dropped table '{name}' does not exist"),
            ));
        }
        let to_remove: Vec<String> = self
            .docs
            .keys()
            .filter_map(|(t, id)| if t == name { Some(id.clone()) } else { None })
            .collect();
        let affected = to_remove.len() as i64;
        for id in to_remove {
            self.docs.remove(&(name.to_string(), id));
        }
        Ok((
            DirectiveReport {
                op: "dropTable".into(),
                affected_rows: affected,
                ..Default::default()
            },
            Some(name.to_string()),
        ))
    }

    /// `dropIndex` directive — one function per directive kind, dispatched
    /// by [`Self::apply_migration_directive`]. Mirrors the ts/python engines'
    /// `applyDropIndexDirective` / `_apply_drop_index_directive`.
    fn apply_drop_index_directive(
        &mut self,
        planned: &mut SchemaDef,
        table: &str,
        name: &str,
    ) -> Result<(crate::wire::admin::DirectiveReport, Option<String>), RtDbError> {
        use crate::wire::admin::DirectiveReport;
        let t = migrate_table_mut(planned, table)?;
        let indexes = t.indexes.as_mut().ok_or_else(|| {
            RtDbError::new(
                ErrorCode::BadRequest,
                format!("dropped index '{table}.{name}' does not exist"),
            )
        })?;
        if !indexes.iter().any(|ix| ix.name == name) {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                format!("dropped index '{table}.{name}' does not exist"),
            ));
        }
        indexes.retain(|ix| ix.name != name);
        Ok((
            DirectiveReport {
                op: "dropIndex".into(),
                affected_rows: 0,
                ..Default::default()
            },
            Some(table.to_string()),
        ))
    }

    /// `setDefault` directive — one function per directive kind, dispatched
    /// by [`Self::apply_migration_directive`]. Mirrors the ts/python engines'
    /// `applySetDefaultDirective` / `_apply_set_default_directive`.
    fn apply_set_default_directive(
        &mut self,
        planned: &mut SchemaDef,
        table: &str,
        field: &str,
        value: &Value,
    ) -> Result<(crate::wire::admin::DirectiveReport, Option<String>), RtDbError> {
        use crate::wire::admin::DirectiveReport;
        let t = migrate_table_mut(planned, table)?;
        if !t.fields.contains_key(field) {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                format!("setDefault target '{table}.{field}' does not exist"),
            ));
        }
        let mut affected = 0i64;
        for ((tname, _), row) in self.docs.iter_mut() {
            if tname != table {
                continue;
            }
            if let Some(obj) = row.doc.as_object_mut()
                && !obj.contains_key(field)
            {
                obj.insert(field.to_string(), value.clone());
                affected += 1;
            }
        }
        Ok((
            DirectiveReport {
                op: "setDefault".into(),
                affected_rows: affected,
                ..Default::default()
            },
            Some(table.to_string()),
        ))
    }

    /// `evalExpr` directive — no in-memory SQL engine exists, so both the
    /// ENH-020 typed `ValueExpr` path and the legacy raw-SQL path raise
    /// `BadRequest`. Mirrors the ts/python engines' `applyEvalExprDirective` /
    /// `_apply_eval_expr_directive`.
    fn apply_eval_expr_directive(
        &mut self,
        table: &str,
    ) -> Result<(crate::wire::admin::DirectiveReport, Option<String>), RtDbError> {
        Err(RtDbError::new(
            ErrorCode::BadRequest,
            format!("evalExpr unsupported in-memory (table '{table}')"),
        ))
    }
}

/// Resolves a mutable table definition from a working schema, returning the
/// server-shaped `BAD_REQUEST` when the table is absent. Ports
/// `migrateTable` (`ts-client/src/in_memory.ts:965-971`) and mirrors server
/// `migrate::table_mut`.
#[cfg(feature = "admin")]
fn migrate_table_mut<'a>(
    schema: &'a mut SchemaDef,
    table: &str,
) -> Result<&'a mut TableDef, RtDbError> {
    schema.tables.get_mut(table).ok_or_else(|| {
        RtDbError::new(
            ErrorCode::BadRequest,
            format!("table '{table}' does not exist"),
        )
    })
}

/// Rewrite every `field` reference in `expr` that equals `from` to `to`, in
/// place. Used by `RenameField` to carry `Case.whens` predicates (and any
/// other `FilterExpr` the schema carries) across a field rename. Mirrors
/// server `migrate::rename_filter_fields`.
#[cfg(feature = "admin")]
fn rename_filter_fields(expr: &mut FilterExpr, from: &str, to: &str) {
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

/// The `ValueExpr` half of the rename: rewrites every `Field` reference equal
/// to `from` to `to`, in place — the `&mut` mirror of
/// `value_expr::walk_value_expr_fields`. `Case.whens` predicates reuse
/// [`rename_filter_fields`], so a rename carries computed expressions across
/// intact. `to` is fresh (the RenameField arm rejects an existing target), so
/// no reference set can collide. Mirrors server
/// `migrate::rename_value_expr_fields`.
#[cfg(feature = "admin")]
fn rename_value_expr_fields(expr: &mut crate::value_expr::ValueExpr, from: &str, to: &str) {
    use crate::value_expr::ValueExpr;
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

/// True iff `cast` can coerce from `old` — a port of server
/// `migrate::cast_valid_for` and ts-client `castValidFor`. Locks the same
/// coercion matrix as the server (the closed set of sound source types per
/// cast).
#[cfg(feature = "admin")]
fn cast_valid_for(cast: crate::wire::admin::Cast, old: &FieldType) -> bool {
    use crate::wire::admin::Cast;
    use FieldType::*;
    matches!(
        (cast, old),
        (Cast::ToString, String | Number | Boolean | Int64)
            | (Cast::ToNumber, String | Boolean | Int64)
            | (Cast::ToInt64, String | Number)
            | (Cast::ToBoolean, String | Number)
    )
}

/// Pure Rust coercion mirroring server `migrate::coerce_value` and ts-client
/// `coerceValue`. Returns `None` if the value cannot be coerced under this cast;
/// the caller then substitutes `default` if supplied or raises a row-named
/// `BadRequest`. `ToInt64` emits a decimal-string JSON value (int64 travels as
/// a canonical decimal string on this wire — see `FEATURE_MATRIX.md` #13);
/// `ToNumber` emits a JSON number. The other casts produce the natural JSON
/// representation.
#[cfg(feature = "admin")]
fn coerce_value(cast: crate::wire::admin::Cast, v: &Value) -> Option<Value> {
    use crate::wire::admin::Cast;
    use serde_json::json;
    match (cast, v) {
        (Cast::ToString, Value::String(_)) => Some(v.clone()),
        (Cast::ToString, Value::Number(n)) => Some(Value::String(n.to_string())),
        (Cast::ToString, Value::Bool(b)) => Some(Value::String(b.to_string())),
        (Cast::ToString, _) => None,
        (Cast::ToNumber, Value::String(s)) => match s.parse::<f64>() {
            Ok(n) if n.is_finite() => Some(json!(n)),
            _ => None,
        },
        (Cast::ToNumber, Value::Number(_)) => Some(v.clone()),
        (Cast::ToNumber, Value::Bool(b)) => Some(json!(if *b { 1.0 } else { 0.0 })),
        (Cast::ToNumber, _) => None,
        (Cast::ToInt64, Value::String(s)) => s.parse::<i64>().ok().map(|i| json!(i.to_string())),
        (Cast::ToInt64, Value::Number(n)) => n.as_i64().map(|i| json!(i.to_string())),
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

/// Mirrors `server/src/ddl.rs::detect_destructive_changes`: walks `old` and
/// rejects any removed table, removed field, changed field type (except a safe
/// literal-union widening, which is additive and allowed — see
/// `schema::is_widening_of`), or removed/changed index with
/// [`ErrorCode::BadRequest`]. Additive changes (new tables, new fields, new
/// indexes, widening `Optional`/`Union`/`Any` inner types via a fresh push) pass
/// through — `push_schema` then folds the new schema into `self.tables` without
/// touching stored docs.
///
/// `FieldType`/`IndexDef`/`VectorIndexSpec` derive `PartialEq` (mirroring the
/// server), so structural equality is a direct `!=`. FM-33: field types are
/// compared with every `onDelete` action stripped (`strip_on_delete`) —
/// adding or changing an action alters runtime delete behavior only (no
/// stored row shape), so it is additive, while changing the referenced table
/// is still a type change. The `softDelete` flag is deliberately NOT compared,
/// matching the server's `detect_destructive_changes`.
pub(super) fn detect_destructive_changes(
    old: &SchemaDef,
    new: &SchemaDef,
) -> Result<(), RtDbError> {
    for (table_name, old_table) in &old.tables {
        let new_table = new.tables.get(table_name).ok_or_else(|| {
            RtDbError::new(
                ErrorCode::BadRequest,
                format!("removed table '{table_name}'"),
            )
        })?;
        for (field_name, old_field_type) in &old_table.fields {
            match new_table.fields.get(field_name) {
                None => {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        format!("removed field '{table_name}.{field_name}'"),
                    ));
                }
                Some(new_field_type)
                    if strip_on_delete(old_field_type) != strip_on_delete(new_field_type)
                        && !is_widening_of(old_field_type, new_field_type) =>
                {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        format!("changed type of field '{table_name}.{field_name}'"),
                    ));
                }
                _ => {}
            }
        }
        for old_index in old_table.indexes.iter().flatten() {
            let new_index = new_table
                .indexes
                .iter()
                .flatten()
                .find(|i| i.name == old_index.name);
            let new_index = match new_index {
                None => {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        format!("removed index '{}'", old_index.name),
                    ));
                }
                Some(i) => i,
            };
            if new_index.fields != old_index.fields {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    format!("changed fields of index '{}'", old_index.name),
                ));
            }
            if new_index.search != old_index.search {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    format!(
                        "changed kind of index '{}' (btree <-> search)",
                        old_index.name
                    ),
                ));
            }
            if new_index.vector != old_index.vector {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    format!("changed vector spec of index '{}'", old_index.name),
                ));
            }
            if new_index.unique != old_index.unique {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    format!("changed uniqueness of index '{}'", old_index.name),
                ));
            }
            if new_index.r#where != old_index.r#where {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    format!("changed partial predicate of index '{}'", old_index.name),
                ));
            }
            if new_index.language != old_index.language {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    format!("changed language of search index '{}'", old_index.name),
                ));
            }
        }
    }
    Ok(())
}

/// Computed-field push validation (ENH-028) — a port of server
/// `schema::TableDef::validate_computed`, the six rules in the same order:
/// 1. every `computed` key names a declared field;
/// 2. the key is not one of the server-stamped declaration fields
///    (`ownerField`/`collaboratorsField`/`autoIncrementField`) — those carry
///    their own stamping authority and a computed entry would fight it on
///    every write;
/// 3. every field the expression references (including `Case.when` filter
///    fields) is declared and not itself computed (no chained or cyclic
///    evaluation);
/// 4. `Case.when` filters reject principal markers — computed exprs run on
///    every write with no interactive principal, so a `$user`/`$email` marker
///    has no value to resolve;
/// 5. when the expression's result kind is statically known, the field's type
///    must accept a value of that kind;
/// 6. the table's `authorize` predicate references no computed field.
///
/// Unlike the other rules in [`SchemaDef::validate`] (which mirror the
/// server's `SCHEMA_VIOLATION`s), these reject with `BAD_REQUEST` — the same
/// code the server's `validate_computed` raises, which the semantics-corpus
/// `pushError` cases pin.
fn validate_computed(table_name: &str, table: &TableDef) -> Result<(), RtDbError> {
    use crate::value_expr::walk_filter_expr_fields;
    use crate::value_expr::walk_value_expr_fields;

    for (field, expr) in &table.computed {
        if !table.fields.contains_key(field) {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                format!("computed field '{table_name}.{field}' is not a declared field"),
            ));
        }
        if table.owner_field.as_deref() == Some(field.as_str()) {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                format!("computed field '{table_name}.{field}' must not be the table's ownerField"),
            ));
        }
        if table.collaborators_field.as_deref() == Some(field.as_str()) {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                format!(
                    "computed field '{table_name}.{field}' must not be the table's collaboratorsField"
                ),
            ));
        }
        if table.auto_increment_field.as_deref() == Some(field.as_str()) {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                format!(
                    "computed field '{table_name}.{field}' must not be the table's autoIncrementField"
                ),
            ));
        }
        // First offense wins; the walk covers `Field` nodes and every
        // `Case.when` filter field.
        let mut offender: Option<String> = None;
        walk_value_expr_fields(expr, &mut |referenced| {
            if offender.is_some() {
                return;
            }
            if !table.fields.contains_key(referenced) {
                offender = Some(format!(
                    "computed field '{table_name}.{field}' references undeclared field '{referenced}'"
                ));
            } else if table.computed.contains_key(referenced) {
                offender = Some(format!(
                    "computed field '{table_name}.{field}' references computed field '{referenced}' (computed fields may not reference each other)"
                ));
            }
        });
        if let Some(message) = offender {
            return Err(RtDbError::new(ErrorCode::BadRequest, message));
        }
        validate_computed_case_whens(expr)?;
        if let Some(kind) = infer_static_kind(expr) {
            let sample = match kind {
                StaticKind::String => Value::String("s".into()),
                StaticKind::Number => Value::from(1),
                StaticKind::Boolean => Value::Bool(true),
            };
            // `validate_value` is the wire contract, but int64's wire form is
            // a decimal STRING: a Number-kind result can never validate
            // (arithmetic yields JSON numbers), while a String-kind one can
            // ("42") — decimal-ness stays a runtime `validate_doc` check.
            // Optional unwrapping admits the nullable spelling.
            let mut inner = &table.fields[field];
            while let FieldType::Optional { inner: deeper } = inner {
                inner = deeper;
            }
            let accepts = validate_value(&table.fields[field], &sample)
                || (matches!(inner, FieldType::Int64) && matches!(kind, StaticKind::String));
            if !accepts {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    format!(
                        "computed field '{table_name}.{field}' produces {}, which the field type does not accept",
                        kind.as_str()
                    ),
                ));
            }
        }
    }
    // Rule 6: authorize runs pre-stamp on the insert paths, so a predicate
    // over a computed field would read client input.
    if let Some(authorize) = &table.authorize {
        let mut offender: Option<String> = None;
        walk_filter_expr_fields(authorize, &mut |referenced| {
            if offender.is_none() && table.computed.contains_key(referenced) {
                offender = Some(referenced.to_string());
            }
        });
        if let Some(field) = offender {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                format!(
                    "computed field '{table_name}.{field}' must not be referenced by the table's authorize predicate (authorize predicates may not reference computed fields)"
                ),
            ));
        }
    }
    Ok(())
}

/// Walks a computed expression's `Case` nodes rejecting principal markers in
/// each `when` filter — the engine-side half of server
/// `schema::validate_computed_case_whens` (the server reuses the query
/// boundary's marker-rejecting filter validation; the engine's filter
/// validation has no marker mode, so this is the marker rule alone — the
/// field-declared rule is already covered by rule 3's walk). `then`/
/// `otherwise` recurse so a `Case` nested inside a branch is covered.
fn validate_computed_case_whens(ve: &crate::value_expr::ValueExpr) -> Result<(), RtDbError> {
    use crate::value_expr::ValueExpr;
    match ve {
        ValueExpr::Case { whens, otherwise } => {
            for cw in whens {
                if let Some(field) = filter_expr_marker_field(&cw.when) {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        format!(
                            "principal markers ({{\"$user\":true}}/{{\"$email\":true}}) are not allowed in computed expressions (field '{field}')"
                        ),
                    ));
                }
                validate_computed_case_whens(&cw.then)?;
            }
            validate_computed_case_whens(otherwise)
        }
        ValueExpr::Concat { parts } | ValueExpr::Coalesce { parts } => {
            for p in parts {
                validate_computed_case_whens(p)?;
            }
            Ok(())
        }
        ValueExpr::Add { left, right }
        | ValueExpr::Sub { left, right }
        | ValueExpr::Mul { left, right }
        | ValueExpr::Div { left, right } => {
            validate_computed_case_whens(left)?;
            validate_computed_case_whens(right)
        }
        ValueExpr::Lower { value }
        | ValueExpr::Upper { value }
        | ValueExpr::Trim { value }
        | ValueExpr::Cast { value, .. } => validate_computed_case_whens(value),
        ValueExpr::Field { .. } | ValueExpr::Literal { .. } | ValueExpr::Now => Ok(()),
    }
}

/// The first leaf field whose value (or `in` value list) carries a principal
/// marker, if any — the marker-rejecting walk the server folds into
/// `validate_filter_expr_fields(_, _, false)`.
fn filter_expr_marker_field(expr: &FilterExpr) -> Option<String> {
    match expr {
        FilterExpr::Eq { field, value }
        | FilterExpr::Neq { field, value }
        | FilterExpr::Gt { field, value }
        | FilterExpr::Gte { field, value }
        | FilterExpr::Lt { field, value }
        | FilterExpr::Lte { field, value }
        | FilterExpr::Contains { field, value } => {
            is_principal_marker(value).then(|| field.clone())
        }
        FilterExpr::In { field, values } => values
            .iter()
            .any(is_principal_marker)
            .then(|| field.clone()),
        FilterExpr::Exists { .. } => None,
        FilterExpr::And { exprs } | FilterExpr::Or { exprs } => {
            exprs.iter().find_map(filter_expr_marker_field)
        }
        FilterExpr::Not { expr } => filter_expr_marker_field(expr),
    }
}

/// `true` iff `v` is a principal marker: `{"$user": true}` or
/// `{"$email": true}`. Mirrors server `schema::is_principal_marker`.
fn is_principal_marker(v: &Value) -> bool {
    if let Value::Object(map) = v
        && map.len() == 1
    {
        return matches!(map.get("$user").and_then(|x| x.as_bool()), Some(true))
            || matches!(map.get("$email").and_then(|x| x.as_bool()), Some(true));
    }
    false
}

/// The statically-known result kind of a `ValueExpr`, for the computed-field
/// push check. `None` means the result kind varies by input — `Field` (text
/// extraction of any JSON value), `Coalesce`/`Case` (whichever branch wins),
/// and the null / object / array literals whose runtime `validate_doc` check
/// is the only guard. Mirrors server `schema::StaticKind`.
enum StaticKind {
    String,
    Number,
    Boolean,
}

impl StaticKind {
    fn as_str(&self) -> &'static str {
        match self {
            StaticKind::String => "a string",
            StaticKind::Number => "a number",
            StaticKind::Boolean => "a boolean",
        }
    }
}

fn infer_static_kind(ve: &crate::value_expr::ValueExpr) -> Option<StaticKind> {
    use crate::value_expr::{Cast, ValueExpr};
    match ve {
        ValueExpr::Field { .. } | ValueExpr::Coalesce { .. } | ValueExpr::Case { .. } => None,
        ValueExpr::Literal { value } => match value {
            Value::String(_) => Some(StaticKind::String),
            Value::Number(_) => Some(StaticKind::Number),
            Value::Bool(_) => Some(StaticKind::Boolean),
            Value::Null | Value::Object(_) | Value::Array(_) => None,
        },
        ValueExpr::Concat { .. }
        | ValueExpr::Lower { .. }
        | ValueExpr::Upper { .. }
        | ValueExpr::Trim { .. }
        | ValueExpr::Cast {
            to: Cast::ToString, ..
        } => Some(StaticKind::String),
        ValueExpr::Add { .. }
        | ValueExpr::Sub { .. }
        | ValueExpr::Mul { .. }
        | ValueExpr::Div { .. }
        | ValueExpr::Cast {
            to: Cast::ToNumber | Cast::ToInt64,
            ..
        }
        | ValueExpr::Now => Some(StaticKind::Number),
        ValueExpr::Cast {
            to: Cast::ToBoolean,
            ..
        } => Some(StaticKind::Boolean),
    }
}

/// Push-time schema validation — the TTL, `updatedAtField`,
/// `autoIncrementField`, and index-field rules of server `schema::validate`
/// (`schema.rs::validate_indexes` + `validate_ttl` + `validate_updated_at` +
/// `validate_auto_increment`), so an
/// in-memory `push_schema` rejects with `SCHEMA_VIOLATION` what the live
/// server 422s: index fields must be declared and indexable, search indexes
/// must cover text fields, a TTL must name a numeric field carrying a
/// single-field, non-unique, non-partial btree index, `updatedAtField`
/// must name a declared number/int64 field distinct from `ttl.field`, and
/// `autoIncrementField` must name a declared int64 field distinct from both.
/// This is deliberately a subset — identifier formats, owner/collaborator
/// fields, defaults, and `onDelete` shapes stay server-side.
impl SchemaDef {
    /// Validates TTL and index-field rules (see the impl docs) — called by
    /// [`InMemoryRtDbClient::push_schema`] before the destructive-change check.
    pub fn validate(&self) -> Result<(), RtDbError> {
        for (table_name, table) in &self.tables {
            for index in table.indexes.iter().flatten() {
                if index.fields.is_empty() {
                    return Err(RtDbError::new(
                        ErrorCode::SchemaViolation,
                        format!(
                            "index '{}' on table '{table_name}' has no fields",
                            index.name
                        ),
                    ));
                }
                // A vector index's `fields[0]` is a Vector column, which is not
                // btree-indexable — the server validates vector specs in their
                // own branch and skips the per-field loop below.
                if index.vector.is_some() {
                    continue;
                }
                for field_name in &index.fields {
                    let field_type = table.fields.get(field_name).ok_or_else(|| {
                        RtDbError::new(
                            ErrorCode::SchemaViolation,
                            format!(
                                "index '{}' on table '{table_name}' references unknown field '{field_name}'",
                                index.name
                            ),
                        )
                    })?;
                    let indexed = index_column_type(field_type)?;
                    if index.search && indexed.pg != PgType::Text {
                        return Err(RtDbError::new(
                            ErrorCode::SchemaViolation,
                            format!(
                                "search index '{}' on table '{table_name}' has non-text field '{field_name}'",
                                index.name
                            ),
                        ));
                    }
                }
            }
            if let Some(ttl) = &table.ttl {
                let field_type = table.fields.get(&ttl.field).ok_or_else(|| {
                    RtDbError::new(
                        ErrorCode::SchemaViolation,
                        format!("ttl.field '{}' is not a declared field", ttl.field),
                    )
                })?;
                if !matches!(field_type, FieldType::Number | FieldType::Int64) {
                    return Err(RtDbError::new(
                        ErrorCode::SchemaViolation,
                        format!("ttl.field '{}' must be a number or bigint field", ttl.field),
                    ));
                }
                let has_ttl_index = table.indexes.iter().flatten().any(|idx| {
                    !idx.search
                        && idx.vector.is_none()
                        && !idx.unique
                        && idx.r#where.is_none()
                        && idx.fields.len() == 1
                        && idx.fields[0] == ttl.field
                });
                if !has_ttl_index {
                    return Err(RtDbError::new(
                        ErrorCode::SchemaViolation,
                        format!(
                            "ttl.field '{}' requires a single-field, non-unique, non-partial btree index on it",
                            ttl.field
                        ),
                    ));
                }
                if ttl.default_duration_ms.is_some_and(|d| d <= 0) {
                    return Err(RtDbError::new(
                        ErrorCode::SchemaViolation,
                        "ttl.defaultDurationMs must be greater than 0".to_string(),
                    ));
                }
            }
            // `updatedAtField` (FM-36) push validation — the same rules as the
            // server's `validate_updated_at` minus the identifier-format check
            // (identifier formats stay server-side, like the rest of this
            // subset): the field must be declared numeric and differ from
            // `ttl.field` (both stamps write unconditionally; a shared field
            // would silently drop the expiry).
            if let Some(field) = &table.updated_at_field {
                let fty = table.fields.get(field).ok_or_else(|| {
                    RtDbError::new(
                        ErrorCode::SchemaViolation,
                        format!("updatedAtField '{field}' is not a declared field"),
                    )
                })?;
                if !matches!(fty, FieldType::Number | FieldType::Int64) {
                    return Err(RtDbError::new(
                        ErrorCode::SchemaViolation,
                        format!("updatedAtField '{field}' must be a number or bigint field"),
                    ));
                }
                if table.ttl.as_ref().is_some_and(|ttl| &ttl.field == field) {
                    return Err(RtDbError::new(
                        ErrorCode::SchemaViolation,
                        format!(
                            "updatedAtField '{field}' must differ from ttl.field (both stamps write unconditionally; a shared field would drop the expiry)"
                        ),
                    ));
                }
            }
            // `autoIncrementField` (FM-37) push validation — the same rules
            // as the server's `validate_auto_increment` minus the
            // identifier-format check (identifier formats stay server-side,
            // like the rest of this subset): the field must be declared
            // `int64` exactly (the counter produces int64; a `number` would
            // lose precision) and differ from `ttl.field` and
            // `updatedAtField` (both stamp unconditionally on writes the
            // counter must survive verbatim).
            if let Some(field) = &table.auto_increment_field {
                let fty = table.fields.get(field).ok_or_else(|| {
                    RtDbError::new(
                        ErrorCode::SchemaViolation,
                        format!("autoIncrementField '{field}' is not a declared field"),
                    )
                })?;
                if !matches!(fty, FieldType::Int64) {
                    return Err(RtDbError::new(
                        ErrorCode::SchemaViolation,
                        format!("autoIncrementField '{field}' must be an int64 field"),
                    ));
                }
                if table.ttl.as_ref().is_some_and(|ttl| &ttl.field == field) {
                    return Err(RtDbError::new(
                        ErrorCode::SchemaViolation,
                        format!(
                            "autoIncrementField '{field}' must differ from ttl.field (the ttl reaper would delete counter rows)"
                        ),
                    ));
                }
                if table
                    .updated_at_field
                    .as_ref()
                    .is_some_and(|at| at == field)
                {
                    return Err(RtDbError::new(
                        ErrorCode::SchemaViolation,
                        format!(
                            "autoIncrementField '{field}' must differ from updatedAtField (the timestamp would overwrite the counter on every write)"
                        ),
                    ));
                }
            }
            // Computed-field rules (ENH-028) — BAD_REQUEST, not
            // SCHEMA_VIOLATION (see `validate_computed`'s docs).
            validate_computed(table_name, table)?;
        }
        Ok(())
    }
}
