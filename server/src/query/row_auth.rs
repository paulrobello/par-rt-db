//! Per-row authorization predicate rendering — the owner/collaborator SQL
//! fragment, the client-filter owner wrap, principal-marker resolution, and
//! the `authorize`-predicate compiler. ARC-203 mechanical split of the former
//! `query.rs`.

use super::filter::compile_filter_node;
use crate::auth::PrincipalCtx;
use crate::dsl::{EqBind, FilterExpr, resolve_value};
use crate::error::RtDbError;
use crate::schema::TableDef;

/// Wraps the client-supplied `filter` with the owner equality predicate when
/// the table declares an `ownerField` and the caller is a user (`owner`).
/// Bypass callers (`None`) and tables without an `ownerField` get the original
/// filter back unchanged — no enforcement. The owner value is `$n`-bound by
/// `compile_filter`, never interpolated into SQL.
pub(crate) fn owner_filter(
    client_filter: Option<&FilterExpr>,
    owner_field: Option<&str>,
    owner: Option<&str>,
) -> Option<FilterExpr> {
    match (client_filter, owner_field, owner) {
        (Some(f), Some(field), Some(uid)) => Some(FilterExpr::And {
            exprs: vec![
                f.clone(),
                FilterExpr::Eq {
                    field: field.to_string(),
                    value: serde_json::Value::String(uid.to_string()),
                },
            ],
        }),
        (None, Some(field), Some(uid)) => Some(FilterExpr::Eq {
            field: field.to_string(),
            value: serde_json::Value::String(uid.to_string()),
        }),
        (Some(f), _, _) => Some(f.clone()),
        (None, _, _) => None,
    }
}

/// Returns the caller's uid when per-row authorization applies: the caller is a
/// user (`owner` is `Some`) AND the table declares `ownerField` and/or
/// `collaboratorsField`. Returns `None` for bypass callers (machine tokens,
/// scheduled jobs, admin) and tables that declare neither field — those paths
/// enforce nothing.
pub(crate) fn row_auth_enforced_uid<'a>(
    owner_field: Option<&'a str>,
    collab_field: Option<&'a str>,
    owner: Option<&'a str>,
) -> Option<&'a str> {
    if owner_field.is_some() || collab_field.is_some() {
        owner
    } else {
        None
    }
}

/// Per-row auth predicate body (no leading `AND`). The schema-validated
/// `owner_field` and `collab_field` identifiers are interpolated into jsonb
/// extraction positions; the uid is bound once via `${ph}` and reused on both
/// sides of the OR when both fields are declared. Owner-only (no
/// `collaboratorsField`) emits the single-predicate form byte-identical to the
/// pre-collaborators SQL. The jsonb `?` operator tests whether the bound uid
/// appears as a top-level element of the collaborators array (missing/null
/// array → NULL → false).
pub(crate) fn row_auth_predicate_body(
    owner_field: Option<&str>,
    collab_field: Option<&str>,
    ph: usize,
) -> String {
    match (owner_field, collab_field) {
        (Some(of), Some(cf)) => {
            format!("((doc->>'{of}') = ${ph} OR (doc->'{cf}') ? ${ph})")
        }
        (Some(of), None) => format!("(doc->>'{of}') = ${ph}"),
        (None, Some(cf)) => format!("(doc->'{cf}') ? ${ph}"),
        // Unreachable when called via `row_auth_enforced_uid`; emit a `true`
        // predicate so the call site's `where_conditions.push(...)` is a
        // well-formed no-op if a future caller bypasses the gate.
        (None, None) => "(TRUE)".to_string(),
    }
}

/// Clones `expr`, resolving principal markers (`{"$user":true}` → the caller's
/// uid, `{"$email":true}` → the caller's email) at every value position via
/// `resolve_value`. Turns a server-declared `authorize` predicate into a
/// concrete `FilterExpr` whose markers are replaced by bound literals, so
/// `compile_filter` can `$n`-bind them like any other value. A marker whose
/// principal field is `None` resolves to `Null` — the safe no-match
/// over-approximation (a predicate can never match erroneously on a missing
/// identity; it simply fails to match).
fn resolve_predicate_markers(expr: &FilterExpr, ctx: &PrincipalCtx) -> FilterExpr {
    match expr {
        FilterExpr::Eq { field, value } => FilterExpr::Eq {
            field: field.clone(),
            value: resolve_value(value, ctx),
        },
        FilterExpr::Neq { field, value } => FilterExpr::Neq {
            field: field.clone(),
            value: resolve_value(value, ctx),
        },
        FilterExpr::Gt { field, value } => FilterExpr::Gt {
            field: field.clone(),
            value: resolve_value(value, ctx),
        },
        FilterExpr::Gte { field, value } => FilterExpr::Gte {
            field: field.clone(),
            value: resolve_value(value, ctx),
        },
        FilterExpr::Lt { field, value } => FilterExpr::Lt {
            field: field.clone(),
            value: resolve_value(value, ctx),
        },
        FilterExpr::Lte { field, value } => FilterExpr::Lte {
            field: field.clone(),
            value: resolve_value(value, ctx),
        },
        FilterExpr::In { field, values } => FilterExpr::In {
            field: field.clone(),
            values: values.iter().map(|v| resolve_value(v, ctx)).collect(),
        },
        FilterExpr::Contains { field, value } => FilterExpr::Contains {
            field: field.clone(),
            value: resolve_value(value, ctx),
        },
        FilterExpr::Exists { field } => FilterExpr::Exists {
            field: field.clone(),
        },
        FilterExpr::And { exprs } => FilterExpr::And {
            exprs: exprs
                .iter()
                .map(|e| resolve_predicate_markers(e, ctx))
                .collect(),
        },
        FilterExpr::Or { exprs } => FilterExpr::Or {
            exprs: exprs
                .iter()
                .map(|e| resolve_predicate_markers(e, ctx))
                .collect(),
        },
        FilterExpr::Not { expr } => FilterExpr::Not {
            expr: Box::new(resolve_predicate_markers(expr, ctx)),
        },
    }
}

/// Compiles `table.authorize` into a SQL fragment (no leading `AND`) suitable
/// for appending to `where_conditions`. Returns `Some(fragment)` only when the
/// table declares an `authorize` predicate AND the caller is a user
/// (`ctx.user_id.is_some()`): principal markers are resolved against `ctx` and
/// the predicate compiled with the same `$n`-bind discipline as a client
/// filter. Returns `None` for bypass callers (`Machine`/admin/scheduled,
/// `user_id = None`) and tables without `authorize` — those paths enforce
/// nothing (the db-level gate still ran first). Appends the fragment's typed
/// binds to `binds`; `start_pos` is the absolute 1-based position of the
/// fragment's first bind, and `compile_filter_node`'s `start_pos + binds.len()`
/// rule numbers each placeholder, so the caller must pass the SAME shared bind
/// accumulator it uses for the preceding predicates.
pub(crate) fn authorize_predicate_body(
    table: &TableDef,
    ctx: &PrincipalCtx,
    start_pos: usize,
    binds: &mut Vec<EqBind>,
) -> Result<Option<String>, RtDbError> {
    let Some(expr) = &table.authorize else {
        return Ok(None);
    };
    if ctx.user_id.is_none() {
        return Ok(None);
    }
    let resolved = resolve_predicate_markers(expr, ctx);
    let fragment = compile_filter_node(&resolved, table, start_pos, binds)?;
    Ok(Some(fragment))
}
