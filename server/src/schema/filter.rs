//! Filter-expression field validation shared by client `.filter()` queries and
//! server-declared `authorize` predicates: `validate_filter_expr_fields` walks a
//! `FilterExpr` checking every field reference against the table's declared
//! fields, principal-marker eligibility, and (SEC-007) the depth/list-length
//! caps below.

use crate::error::RtDbError;
use crate::query::FilterExpr;

use super::types::{FieldType, TableDef};
use super::value::{indexed_column_type, is_string_array_field};

/// True if `v` is a principal marker: `{"$user": true}` or `{"$email": true}`.
/// Markers are valid only in a server-declared `authorize` predicate; the query
/// boundary (Task 6) rejects them in client `.filter()` expressions via
/// `validate_filter_expr_fields(_, _, allow_principal_markers = false)`.
fn is_principal_marker(v: &serde_json::Value) -> bool {
    if let serde_json::Value::Object(map) = v
        && map.len() == 1
    {
        return matches!(map.get("$user").and_then(|x| x.as_bool()), Some(true))
            || matches!(map.get("$email").and_then(|x| x.as_bool()), Some(true));
    }
    false
}

fn check_field_declared(field: &str, table: &TableDef) -> Result<(), RtDbError> {
    if !table.fields.contains_key(field) {
        return Err(RtDbError::schema(format!(
            "filter references undeclared field '{field}'"
        )));
    }
    Ok(())
}

/// SEC-007: hard ceiling on `and`/`or`/`not` nesting in a filter expression.
/// `serde_json`'s 128-level recursion limit and the 64 KiB WS frame cap bound
/// this incidentally today; this is the project-owned limit, enforced in the
/// one place every filter passes through.
pub const MAX_FILTER_DEPTH: usize = 32;

/// SEC-007: hard ceiling on `in` list length. Each value becomes one bound
/// placeholder, so an unbounded list is an unbounded query plan.
pub const MAX_IN_VALUES: usize = 1000;

/// Walk `expr` validating its field references against `table`'s declared
/// fields. Reused by `validate_structure` (for the server-declared `authorize`
/// predicate, `allow_principal_markers = true`) and by the query boundary in
/// Task 6 (for client `.filter()` expressions, `allow_principal_markers = false`).
///
/// - Every `field` (eq/neq/in/gt/gte/lt/lte/contains/exists/olderThan) must
///   name a declared field.
/// - `Contains` requires an array-of-strings field (`is_string_array_field`),
///   since it compiles to a jsonb membership test against a bound text uid.
/// - Comparison fields (gt/gte/lt/lte) must be scalar-indexable
///   (`indexed_column_type` ok) so the SQL and doc evaluators can order them.
/// - When `allow_principal_markers` is false, any principal marker appearing in
///   a value position is rejected (Task 6's client-filter guard).
/// - `olderThan` is accepted only when `allow_relative_time` is true (the
///   by-query step filters — `compile_scan_where`); everywhere else (read
///   filters, `authorize`, computed `case` whens, migrate filters) it is
///   rejected. Requires a declared `number`/`int64` field and `ms >= 0`.
/// - `And`/`Or`/`Not` recurse.
pub fn validate_filter_expr_fields(
    expr: &FilterExpr,
    table: &TableDef,
    allow_principal_markers: bool,
    allow_relative_time: bool,
) -> Result<(), RtDbError> {
    validate_filter_expr_fields_at(expr, table, allow_principal_markers, allow_relative_time, 1)
}

fn validate_filter_expr_fields_at(
    expr: &FilterExpr,
    table: &TableDef,
    allow_principal_markers: bool,
    allow_relative_time: bool,
    depth: usize,
) -> Result<(), RtDbError> {
    if depth > MAX_FILTER_DEPTH {
        return Err(RtDbError::bad_request(format!(
            "filter nesting exceeds {MAX_FILTER_DEPTH} levels"
        )));
    }
    match expr {
        FilterExpr::Eq { field, value }
        | FilterExpr::Neq { field, value }
        | FilterExpr::Gt { field, value }
        | FilterExpr::Gte { field, value }
        | FilterExpr::Lt { field, value }
        | FilterExpr::Lte { field, value } => {
            check_field_declared(field, table)?;
            if !allow_principal_markers && is_principal_marker(value) {
                return Err(RtDbError::schema(format!(
                    "principal markers ({{\"$user\":true}}/{{\"$email\":true}}) are not allowed in client filters (field '{field}')"
                )));
            }
            if matches!(
                expr,
                FilterExpr::Gt { .. }
                    | FilterExpr::Gte { .. }
                    | FilterExpr::Lt { .. }
                    | FilterExpr::Lte { .. }
            ) {
                let fty = &table.fields[field];
                if indexed_column_type(fty).is_err() {
                    return Err(RtDbError::schema(format!(
                        "field '{field}' must be a scalar indexable type for comparison"
                    )));
                }
            }
        }
        FilterExpr::OlderThan { field, ms } => {
            if !allow_relative_time {
                return Err(RtDbError::schema(
                    "olderThan filter is only allowed in patchByQuery/deleteByQuery filters"
                        .to_string(),
                ));
            }
            if *ms < 0 {
                return Err(RtDbError::schema("olderThan ms must be >= 0".to_string()));
            }
            check_field_declared(field, table)?;
            let fty = &table.fields[field];
            let inner = match fty {
                FieldType::Optional { inner } => inner.as_ref(),
                _ => fty,
            };
            if !matches!(inner, FieldType::Number | FieldType::Int64) {
                return Err(RtDbError::schema(format!(
                    "field '{field}' must be a number or int64 field for olderThan"
                )));
            }
        }
        FilterExpr::In { field, values } => {
            check_field_declared(field, table)?;
            if values.len() > MAX_IN_VALUES {
                return Err(RtDbError::bad_request(format!(
                    "in: at most {MAX_IN_VALUES} values"
                )));
            }
            if !allow_principal_markers {
                for v in values {
                    if is_principal_marker(v) {
                        return Err(RtDbError::schema(format!(
                            "principal markers ({{\"$user\":true}}/{{\"$email\":true}}) are not allowed in client filters (field '{field}')"
                        )));
                    }
                }
            }
        }
        FilterExpr::Contains { field, value } => {
            check_field_declared(field, table)?;
            let fty = &table.fields[field];
            if !is_string_array_field(fty) {
                return Err(RtDbError::schema(format!(
                    "field '{field}' must be an array-of-strings (or array-of-id) field for contains"
                )));
            }
            if !allow_principal_markers && is_principal_marker(value) {
                return Err(RtDbError::schema(format!(
                    "principal markers ({{\"$user\":true}}/{{\"$email\":true}}) are not allowed in client filters (field '{field}')"
                )));
            }
        }
        FilterExpr::Exists { field } => {
            check_field_declared(field, table)?;
        }
        FilterExpr::And { exprs } | FilterExpr::Or { exprs } => {
            for e in exprs {
                validate_filter_expr_fields_at(
                    e,
                    table,
                    allow_principal_markers,
                    allow_relative_time,
                    depth + 1,
                )?;
            }
        }
        FilterExpr::Not { expr } => {
            validate_filter_expr_fields_at(
                expr,
                table,
                allow_principal_markers,
                allow_relative_time,
                depth + 1,
            )?;
        }
    }
    Ok(())
}
