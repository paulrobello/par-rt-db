//! Computed-field validation (ENH-028): `validate_computed_case_whens` (filter
//! validation for `Case` branches inside a computed expression), the
//! statically-known `StaticKind` of a `ValueExpr` result, and the schema-level
//! `validate_computed` entry point. The per-table structural checks (key
//! declared, references declared and non-computed, result kind acceptable to
//! the field's type) live on `TableDef::validate_computed` in `schema::validate`.

use crate::error::RtDbError;
use crate::value_expr::ValueExpr;

use super::filter::validate_filter_expr_fields;
use super::types::{SchemaDef, TableDef};
use super::validate::TableDefExt;

/// Walks a computed expression's `Case` nodes validating each `when` filter
/// with the marker-rejecting mode of `validate_filter_expr_fields` — the same
/// call the query boundary uses for client filters, and the opposite of the
/// `authorize` path's marker-allowing call. `then`/`otherwise` recurse so a
/// `Case` nested inside a branch is covered.
pub(super) fn validate_computed_case_whens(
    ve: &ValueExpr,
    table: &TableDef,
) -> Result<(), RtDbError> {
    match ve {
        ValueExpr::Case { whens, otherwise } => {
            for cw in whens {
                validate_filter_expr_fields(&cw.when, table, false, false)
                    .map_err(|e| RtDbError::bad_request(e.message))?;
                validate_computed_case_whens(&cw.then, table)?;
            }
            validate_computed_case_whens(otherwise, table)
        }
        ValueExpr::Concat { parts } | ValueExpr::Coalesce { parts } => {
            for p in parts {
                validate_computed_case_whens(p, table)?;
            }
            Ok(())
        }
        ValueExpr::Add { left, right }
        | ValueExpr::Sub { left, right }
        | ValueExpr::Mul { left, right }
        | ValueExpr::Div { left, right } => {
            validate_computed_case_whens(left, table)?;
            validate_computed_case_whens(right, table)
        }
        ValueExpr::Lower { value }
        | ValueExpr::Upper { value }
        | ValueExpr::Trim { value }
        | ValueExpr::Cast { value, .. } => validate_computed_case_whens(value, table),
        ValueExpr::Field { .. } | ValueExpr::Literal { .. } | ValueExpr::Now => Ok(()),
    }
}

/// The statically-known result kind of a `ValueExpr`, for the computed-field
/// push check. `None` means the result kind varies by input — `Field` (text
/// extraction of any JSON value), `Coalesce`/`Case` (whichever branch wins),
/// and the null / object / array literals whose runtime `validate_doc` check
/// is the only guard.
pub(super) enum StaticKind {
    String,
    Number,
    Boolean,
}

impl StaticKind {
    pub(super) fn as_str(&self) -> &'static str {
        match self {
            StaticKind::String => "a string",
            StaticKind::Number => "a number",
            StaticKind::Boolean => "a boolean",
        }
    }
}

pub(super) fn infer_static_kind(ve: &ValueExpr) -> Option<StaticKind> {
    match ve {
        ValueExpr::Field { .. } | ValueExpr::Coalesce { .. } | ValueExpr::Case { .. } => None,
        ValueExpr::Literal { value } => match value {
            serde_json::Value::String(_) => Some(StaticKind::String),
            serde_json::Value::Number(_) => Some(StaticKind::Number),
            serde_json::Value::Bool(_) => Some(StaticKind::Boolean),
            serde_json::Value::Null
            | serde_json::Value::Object(_)
            | serde_json::Value::Array(_) => None,
        },
        ValueExpr::Concat { .. }
        | ValueExpr::Lower { .. }
        | ValueExpr::Upper { .. }
        | ValueExpr::Trim { .. }
        | ValueExpr::Cast {
            to: crate::value_expr::Cast::ToString,
            ..
        } => Some(StaticKind::String),
        ValueExpr::Add { .. }
        | ValueExpr::Sub { .. }
        | ValueExpr::Mul { .. }
        | ValueExpr::Div { .. }
        | ValueExpr::Cast {
            to: crate::value_expr::Cast::ToNumber | crate::value_expr::Cast::ToInt64,
            ..
        }
        | ValueExpr::Now => Some(StaticKind::Number),
        ValueExpr::Cast {
            to: crate::value_expr::Cast::ToBoolean,
            ..
        } => Some(StaticKind::Boolean),
    }
}

/// Validates every table's computed-field map (ENH-028) — the schema-level
/// entry point behind `TableDef::validate_structure`, also called directly by
/// `migrate::plan_migration` so directive folding (e.g. `changeType`) that
/// invalidates a computed entry fails at plan time, before any DB work.
pub fn validate_computed(schema: &SchemaDef) -> Result<(), RtDbError> {
    for (table_name, table) in &schema.tables {
        table.validate_computed(table_name)?;
    }
    Ok(())
}
