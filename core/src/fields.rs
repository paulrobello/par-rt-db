//! Field walks over the shared wire grammar (ARC-004).
//!
//! Both crates need the same answer to "which declared fields does this
//! expression read?" — the server type-checks computed fields and plans
//! migrations with it, the client validates a schema push and drives its
//! in-memory engine with it. One walk, one answer.

use crate::wire::{FilterExpr, ValueExpr};

/// Visits every field name a `ValueExpr` reads: each `Field` node, every
/// `Case` branch's `then`/`otherwise`, and every `FilterExpr` field inside
/// `Case.whens` — the same field set computed-field push validation
/// type-checks, exposed as a callback walk.
pub fn walk_value_expr_fields(ve: &ValueExpr, f: &mut impl FnMut(&str)) {
    match ve {
        ValueExpr::Field { field } => f(field),
        ValueExpr::Literal { .. } | ValueExpr::Now => {}
        ValueExpr::Concat { parts } | ValueExpr::Coalesce { parts } => {
            for p in parts {
                walk_value_expr_fields(p, f);
            }
        }
        ValueExpr::Add { left, right }
        | ValueExpr::Sub { left, right }
        | ValueExpr::Mul { left, right }
        | ValueExpr::Div { left, right } => {
            walk_value_expr_fields(left, f);
            walk_value_expr_fields(right, f);
        }
        ValueExpr::Lower { value } | ValueExpr::Upper { value } | ValueExpr::Trim { value } => {
            walk_value_expr_fields(value, f);
        }
        ValueExpr::Cast { value, .. } => walk_value_expr_fields(value, f),
        ValueExpr::Case { whens, otherwise } => {
            for cw in whens {
                walk_filter_expr_fields(&cw.when, f);
                walk_value_expr_fields(&cw.then, f);
            }
            walk_value_expr_fields(otherwise, f);
        }
    }
}

/// The `FilterExpr` half of the walk: `And`/`Or`/`Not` recurse; every leaf
/// variant carries `field: String`.
pub fn walk_filter_expr_fields(expr: &FilterExpr, f: &mut impl FnMut(&str)) {
    match expr {
        FilterExpr::Eq { field, .. }
        | FilterExpr::Neq { field, .. }
        | FilterExpr::Gt { field, .. }
        | FilterExpr::Gte { field, .. }
        | FilterExpr::Lt { field, .. }
        | FilterExpr::Lte { field, .. }
        | FilterExpr::In { field, .. }
        | FilterExpr::Contains { field, .. }
        | FilterExpr::OlderThan { field, .. }
        | FilterExpr::Exists { field } => f(field),
        FilterExpr::And { exprs } | FilterExpr::Or { exprs } => {
            for e in exprs {
                walk_filter_expr_fields(e, f);
            }
        }
        FilterExpr::Not { expr } => walk_filter_expr_fields(expr, f),
    }
}
