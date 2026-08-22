//! Filter/scan WHERE compilation — the `filter()` DSL to SQL, the shared
//! scan-WHERE composer (client filter + per-row auth predicates + soft-delete
//! literal), and the literal renderers `ddl.rs` uses for default/index
//! expressions. ARC-203 mechanical split of the former `query.rs`.

use super::row_auth::{authorize_predicate_body, row_auth_enforced_uid, row_auth_predicate_body};
use crate::auth::PrincipalCtx;
use crate::ddl::pg_col;
use crate::dsl::{EqBind, FilterExpr, eq_bind_for};
use crate::error::RtDbError;
use crate::schema::{FieldType, TableDef, validate_filter_expr_fields};

/// Pushes `bind` onto `binds` and returns its 1-based SQL placeholder (`$N`),
/// where `N = start_pos + binds.len()` evaluated BEFORE the push. Every
/// placeholder emission in `compile_filter`/`compile_filter_node` routes through
/// here so the offset arithmetic has one source of truth instead of being
/// inlined (with the "compute pos, then push" ordering) across each leaf.
fn push_filter_bind(start_pos: usize, binds: &mut Vec<EqBind>, bind: EqBind) -> String {
    let placeholder = format!("${}", start_pos + binds.len());
    binds.push(bind);
    placeholder
}

/// Compiles the WHERE clause for a by-query scan (`PatchByQuery`/`DeleteByQuery`
/// in `txn.rs`): the client `filter` AND the per-row visibility predicates
/// (`ownerField`/`collaboratorsField` OR, and the `authorize` predicate), so a
/// by-query write touches exactly the rows the caller could read — the same set
/// `compile_query_window` produces for a read. Returns the WHERE fragment (no
/// leading `WHERE`; empty when nothing restricts the scan, i.e. a bypass caller
/// with no filter on an un-gated table), its typed binds with `$n` placeholders
/// numbered from 1, and the 1-based position of the next placeholder for the
/// caller's `LIMIT` bind.
///
/// Unlike `compile_query_window` this always appends the standalone
/// `row_auth_predicate_body` for an enforced uid (the read path instead merges
/// the owner equality into the filter via `owner_filter` when the table is
/// owner-only, as an index-usage optimization). The two are semantically
/// identical; a by-query scan never index-seeks on the owner field, so the
/// merge optimization is irrelevant here and the standalone form is simpler.
pub(crate) fn compile_scan_where(
    table_def: &TableDef,
    ctx: &PrincipalCtx,
    owner: Option<&str>,
    filter: Option<&FilterExpr>,
) -> Result<(String, Vec<EqBind>, usize), RtDbError> {
    let owner_field = table_def.owner_field.as_deref();
    let collaborators_field = table_def.collaborators_field.as_deref();
    let mut where_conditions: Vec<String> = Vec::new();
    let mut binds: Vec<EqBind> = Vec::new();
    if let Some(f) = filter {
        // `allow_relative_time = true` is what makes a by-query scan the one
        // surface that accepts `olderThan` — its cutoff is computed from the
        // clock at execution time (per fire for a scheduled txn), which is
        // the whole point of the operator.
        let (fragment, filter_binds) = compile_filter(f, table_def, 1, true)?;
        where_conditions.push(fragment);
        binds.extend(filter_binds);
    }
    if let Some(uid) = row_auth_enforced_uid(owner_field, collaborators_field, owner) {
        let ph = 1 + binds.len();
        where_conditions.push(row_auth_predicate_body(
            owner_field,
            collaborators_field,
            ph,
        ));
        binds.push(EqBind::Text(uid.to_string()));
    }
    if let Some(fragment) = authorize_predicate_body(table_def, ctx, 1, &mut binds)? {
        where_conditions.push(fragment);
    }
    // FM-33 soft delete: by-query writes match exactly the rows the caller
    // could read, and reads never see soft-deleted rows — so neither does a
    // PatchByQuery/DeleteByQuery scan. Unconditional (no admin escape hatch on
    // this path); the literal is bindless so `$n` numbering is unaffected.
    if table_def.soft_delete {
        where_conditions.push("\"deleted_at\" IS NULL".to_string());
    }
    let limit_placeholder = 1 + binds.len();
    Ok((where_conditions.join(" AND "), binds, limit_placeholder))
}

/// Compiles a `filter` into a fully-parenthesized SQL predicate plus its typed
/// binds, with `$n` placeholders numbered from 1-based `start_pos`. Every leaf
/// emits at least one bind, so the fragment is never empty.
///
/// `allow_relative_time` admits the `olderThan` leaf — only `compile_scan_where`
/// (the by-query step filters) passes `true`; every read-path caller passes
/// `false`, rejecting it there.
pub(crate) fn compile_filter(
    filter: &FilterExpr,
    table: &TableDef,
    start_pos: usize,
    allow_relative_time: bool,
) -> Result<(String, Vec<EqBind>), RtDbError> {
    // SEC-125: validate at the single compilation chokepoint so EVERY client
    // filter entry point (q.filter, compile_scan_where's client filter,
    // search.filter, vectorSearch.filter) gets the Contains-array and
    // orderable-field guards. `allow_principal_markers = false` is correct for
    // all of these — the authorize predicate is the only path that permits
    // principal markers, and it bypasses `compile_filter` (compiling via
    // `compile_filter_node` directly after `resolve_predicate_markers`).
    validate_filter_expr_fields(filter, table, false, allow_relative_time)
        .map_err(|e| RtDbError::bad_request(e.message))?;
    let mut binds: Vec<EqBind> = Vec::new();
    let sql = compile_filter_node(filter, table, start_pos, &mut binds)?;
    Ok((sql, binds))
}

pub(crate) fn compile_filter_node(
    node: &FilterExpr,
    table: &TableDef,
    start_pos: usize,
    binds: &mut Vec<EqBind>,
) -> Result<String, RtDbError> {
    match node {
        FilterExpr::And { exprs } | FilterExpr::Or { exprs } => {
            if exprs.is_empty() {
                return Err(RtDbError::bad_request(format!(
                    "{} filter requires at least one expr",
                    if matches!(node, FilterExpr::And { .. }) {
                        "and"
                    } else {
                        "or"
                    }
                )));
            }
            let joiner = if matches!(node, FilterExpr::And { .. }) {
                " AND "
            } else {
                " OR "
            };
            let mut parts: Vec<String> = Vec::with_capacity(exprs.len());
            for expr in exprs {
                parts.push(compile_filter_node(expr, table, start_pos, binds)?);
            }
            Ok(format!("({})", parts.join(joiner)))
        }
        FilterExpr::Eq { field, value } => {
            compile_comparison(field, "=", value, table, start_pos, binds)
        }
        FilterExpr::Neq { field, value } => {
            compile_comparison(field, "<>", value, table, start_pos, binds)
        }
        FilterExpr::Gt { field, value } => {
            compile_comparison(field, ">", value, table, start_pos, binds)
        }
        FilterExpr::Gte { field, value } => {
            compile_comparison(field, ">=", value, table, start_pos, binds)
        }
        FilterExpr::Lt { field, value } => {
            compile_comparison(field, "<", value, table, start_pos, binds)
        }
        FilterExpr::Lte { field, value } => {
            compile_comparison(field, "<=", value, table, start_pos, binds)
        }
        FilterExpr::In { field, values } => {
            if values.is_empty() {
                return Err(RtDbError::bad_request(
                    "in filter requires at least one value",
                ));
            }
            let (lhs, first_bind) = field_lhs_and_bind(field, &values[0], table)?;
            let mut placeholders: Vec<String> =
                vec![push_filter_bind(start_pos, binds, first_bind)];
            for value in &values[1..] {
                let (this_lhs, bind) = field_lhs_and_bind(field, value, table)?;
                if this_lhs != lhs {
                    return Err(RtDbError::bad_request(
                        "in filter values must all be the same type",
                    ));
                }
                placeholders.push(push_filter_bind(start_pos, binds, bind));
            }
            Ok(format!("{lhs} IN ({})", placeholders.join(", ")))
        }
        // SEC-117: wrap the inner predicate in COALESCE(..., FALSE) before
        // negating. Postgres's three-valued logic would otherwise turn
        // `NOT (lhs = $1)` into `NOT NULL` (= NULL, row excluded) when `lhs`
        // is NULL — i.e. when the doc omits the field. The Rust doc evaluator
        // (`filter_matches`) is two-valued and sees the absent field as a
        // non-match, so `!false` = true = row visible. That divergence made
        // the write path (which uses the doc evaluator) more permissive than
        // the SQL read path — an authorization bypass for predicates like
        // `authorize = Not(Eq{field:"banned", value:true})` over a doc that
        // omits `banned`. COALESCE coerces the inner result to a real boolean
        // before negation, so `NOT COALESCE(NULL, FALSE)` = `NOT FALSE` =
        // TRUE = row visible — the SQL and Rust paths agree.
        FilterExpr::Not { expr } => Ok(format!(
            "NOT COALESCE(({}), FALSE)",
            compile_filter_node(expr, table, start_pos, binds)?
        )),
        FilterExpr::Contains { field, value } => {
            let lhs = jsonb_field_lhs(field, table)?;
            let (_, bind) = field_lhs_and_bind(field, value, table)?;
            let ph = push_filter_bind(start_pos, binds, bind);
            Ok(format!("{lhs} ? {ph}"))
        }
        FilterExpr::Exists { field } => {
            jsonb_field_lhs(field, table)?;
            Ok(format!("(doc ? '{field}' AND doc->>'{field}' IS NOT NULL)"))
        }
        // Execution-time-relative cutoff: `lhs < now − ms`, with `now` read
        // here (compile == execution: this runs inside the committer turn
        // that executes the step, so a scheduled txn re-derives the cutoff
        // on every fire). The clock read is a separate `now_ms()` from the
        // step's other stamps by microseconds at most — the same looseness
        // the per-row stamp sites already have.
        FilterExpr::OlderThan { field, ms } => {
            let (lhs, bind) = older_than_lhs_and_bind(field, *ms, table)?;
            let placeholder = push_filter_bind(start_pos, binds, bind);
            Ok(format!("{lhs} < {placeholder}"))
        }
    }
}

/// Inlines a typed `EqBind` as a SQL literal (DDL-only — partial-index
/// predicates cannot use `$n` binds). Strings use SQL-standard `''` doubling.
fn render_literal(bind: &EqBind) -> String {
    match bind {
        EqBind::Text(s) => format!("'{}'", s.replace('\'', "''")),
        EqBind::Bool(b) => {
            if *b {
                "true".into()
            } else {
                "false".into()
            }
        }
        EqBind::Num(n) => n.to_string(),
        EqBind::I64(n) => n.to_string(),
    }
}

/// Like `compile_filter`, but emits **literal** values instead of `$n` binds.
/// Used only at DDL time to bake a partial-index predicate into
/// `CREATE INDEX … WHERE <sql>`. Reuses `field_lhs_and_bind` for identifier
/// validation/double-quoting and value typing, so the predicate is as tightly
/// validated as a query-time `filter()`.
pub(crate) fn compile_filter_literal(
    filter: &FilterExpr,
    table: &TableDef,
) -> Result<String, RtDbError> {
    render_filter_literal_node(filter, table)
}

/// Shared body of the six comparison arms in `render_filter_literal_node`:
/// emits `lhs <op> <literal>`. Splitting the arms per variant (rather than an
/// `Eq | Neq | …` OR-pattern with an inner re-match) makes adding a new
/// comparison variant a compile error in the outer match instead of a runtime
/// `unreachable!()` (QA-006).
fn render_comparison_literal(
    op: &str,
    field: &str,
    value: &serde_json::Value,
    table: &TableDef,
) -> Result<String, RtDbError> {
    let (lhs, bind) = field_lhs_and_bind(field, value, table)?;
    Ok(format!("{lhs} {op} {}", render_literal(&bind)))
}

fn render_filter_literal_node(node: &FilterExpr, table: &TableDef) -> Result<String, RtDbError> {
    match node {
        FilterExpr::And { exprs } | FilterExpr::Or { exprs } => {
            if exprs.is_empty() {
                return Err(RtDbError::bad_request(format!(
                    "{} filter requires at least one expr",
                    if matches!(node, FilterExpr::And { .. }) {
                        "and"
                    } else {
                        "or"
                    }
                )));
            }
            let joiner = if matches!(node, FilterExpr::And { .. }) {
                " AND "
            } else {
                " OR "
            };
            let parts: Vec<String> = exprs
                .iter()
                .map(|e| render_filter_literal_node(e, table))
                .collect::<Result<_, _>>()?;
            Ok(format!("({})", parts.join(joiner)))
        }
        FilterExpr::Eq { field, value } => render_comparison_literal("=", field, value, table),
        FilterExpr::Neq { field, value } => render_comparison_literal("<>", field, value, table),
        FilterExpr::Gt { field, value } => render_comparison_literal(">", field, value, table),
        FilterExpr::Gte { field, value } => render_comparison_literal(">=", field, value, table),
        FilterExpr::Lt { field, value } => render_comparison_literal("<", field, value, table),
        FilterExpr::Lte { field, value } => render_comparison_literal("<=", field, value, table),
        FilterExpr::In { field, values } => {
            if values.is_empty() {
                return Err(RtDbError::bad_request(
                    "in filter requires at least one value",
                ));
            }
            let (lhs, first) = field_lhs_and_bind(field, &values[0], table)?;
            let mut lits = vec![render_literal(&first)];
            for value in &values[1..] {
                let (this_lhs, bind) = field_lhs_and_bind(field, value, table)?;
                if this_lhs != lhs {
                    return Err(RtDbError::bad_request(
                        "in filter values must all be the same type",
                    ));
                }
                lits.push(render_literal(&bind));
            }
            Ok(format!("{lhs} IN ({})", lits.join(", ")))
        }
        // SEC-117: COALESCE the inner predicate before negation for the same
        // reason as `compile_filter_node` — keeps the partial-index predicate
        // consistent with the read scan and the Rust doc evaluator over
        // absent fields.
        FilterExpr::Not { expr } => Ok(format!(
            "NOT COALESCE(({}), FALSE)",
            render_filter_literal_node(expr, table)?
        )),
        FilterExpr::Contains { field, value } => {
            let lhs = jsonb_field_lhs(field, table)?;
            let (_, bind) = field_lhs_and_bind(field, value, table)?;
            Ok(format!("{lhs} ? {}", render_literal(&bind)))
        }
        FilterExpr::Exists { field } => {
            jsonb_field_lhs(field, table)?;
            Ok(format!("(doc ? '{field}' AND doc->>'{field}' IS NOT NULL)"))
        }
        // A partial-index predicate is baked into DDL as a literal — an
        // execution-time-relative cutoff has no static meaning there.
        FilterExpr::OlderThan { .. } => Err(RtDbError::bad_request(
            "olderThan filter is not allowed in a partial-index predicate",
        )),
    }
}

/// Resolves an `olderThan` field to its numeric LHS and the execution-time
/// cutoff bind (`now_ms() − ms`). Indexed fields compare against their typed
/// column (`double precision` for `number`, `bigint` for `int64` — the bind
/// typed to match, per `indexed_column_type`); a declared-but-unindexed field
/// uses jsonb text extraction cast to `float8` (the same cast path
/// `jsonb_lhs_and_bind` gives numeric comparisons — epoch-ms magnitudes are
/// exact well within f64). A null or absent stored value compares as SQL
/// NULL, which never matches the strict `<`.
fn older_than_lhs_and_bind(
    field: &str,
    ms: i64,
    table: &TableDef,
) -> Result<(String, EqBind), RtDbError> {
    let field_type = table.fields.get(field).ok_or_else(|| {
        RtDbError::bad_request(format!("filter references unknown field '{field}'"))
    })?;
    let inner = match field_type {
        FieldType::Optional { inner } => inner.as_ref(),
        _ => field_type,
    };
    let cutoff = crate::db::now_ms() - ms;
    let is_indexed = table
        .indexes
        .iter()
        .any(|idx| idx.fields.iter().any(|f| f == field));
    match inner {
        // The indexed int64 column is `bigint`; compare in i64 so a stored
        // value near i64::MAX stays exact on the typed-column path.
        FieldType::Int64 if is_indexed => {
            Ok((format!("\"{}\"", pg_col(field)), EqBind::I64(cutoff)))
        }
        FieldType::Int64 | FieldType::Number => {
            let cutoff_f = cutoff as f64;
            Ok((
                if is_indexed {
                    format!("\"{}\"", pg_col(field))
                } else {
                    format!("(doc->>'{field}')::float8")
                },
                EqBind::Num(cutoff_f),
            ))
        }
        _ => Err(RtDbError::bad_request(format!(
            "field '{field}' must be a number or int64 field for olderThan"
        ))),
    }
}

/// Compiles a binary comparison leaf into `lhs OP $pos` and pushes one typed bind.
fn compile_comparison(
    field: &str,
    op: &str,
    value: &serde_json::Value,
    table: &TableDef,
    start_pos: usize,
    binds: &mut Vec<EqBind>,
) -> Result<String, RtDbError> {
    let (lhs, bind) = field_lhs_and_bind(field, value, table)?;
    let placeholder = push_filter_bind(start_pos, binds, bind);
    Ok(format!("{lhs} {op} {placeholder}"))
}

/// Resolves a filter field to its jsonb-extraction LHS `(doc->'<field>')` after
/// the same unknown-field check `field_lhs_and_bind` performs. Used by
/// `Contains`/`Exists`, which need the raw jsonb value (for the `?` membership
/// operator and key presence) rather than the text extraction (`doc->>'field'`)
/// that `jsonb_lhs_and_bind` produces for scalar comparisons. The field name is
/// schema-validated, so it is safe inside the jsonb string literal.
fn jsonb_field_lhs(field: &str, table: &TableDef) -> Result<String, RtDbError> {
    if !table.fields.contains_key(field) {
        return Err(RtDbError::bad_request(format!(
            "filter references unknown field '{field}'"
        )));
    }
    Ok(format!("(doc->'{field}')"))
}

/// Resolves a filter field to its SQL left-hand side and types the comparison
/// value into a bind. Indexed fields compare against their typed column (value
/// typed via the field's declared `FieldType`, reusing the `eq` conversion);
/// other declared fields fall back to jsonb extraction with a value-kind cast.
fn field_lhs_and_bind(
    field: &str,
    value: &serde_json::Value,
    table: &TableDef,
) -> Result<(String, EqBind), RtDbError> {
    let field_type = table.fields.get(field).ok_or_else(|| {
        RtDbError::bad_request(format!("filter references unknown field '{field}'"))
    })?;
    let is_indexed = table
        .indexes
        .iter()
        .any(|idx| idx.fields.iter().any(|f| f == field));
    if is_indexed {
        Ok((
            format!("\"{}\"", pg_col(field)),
            eq_bind_for(field_type, value)?,
        ))
    } else {
        // SEC-126: reject a value whose JSON kind is incompatible with the
        // field's declared type. Without this, `Gt{field:"title", value:5}`
        // on a String field would compile to `(doc->>'title')::float8`, which
        // Postgres evaluates per row and errors on the first non-numeric
        // stored value — for a subscription re-running on every write, that
        // is a fan_out re-run that fails forever and silently never pushes.
        validate_jsonb_comparison_value(field, field_type, value)?;
        jsonb_lhs_and_bind(field, value)
    }
}

/// Returns `Ok(())` when `value`'s JSON kind can be ordered against a field of
/// declared type `ty`, else `Err(BadRequest)`. The `Optional` wrapper is
/// unwrapped. The indexed path (`eq_bind_for`) already enforces this; this
/// guards the jsonb path, which would otherwise cast on the value's kind and
/// fail at execution time. Complex/unknown field types (Any, Union, Object,
/// …) accept any scalar so existing callers are not widened.
fn validate_jsonb_comparison_value(
    field: &str,
    ty: &FieldType,
    value: &serde_json::Value,
) -> Result<(), RtDbError> {
    let inner = match ty {
        FieldType::Optional { inner } => inner.as_ref(),
        _ => ty,
    };
    let ok = match inner {
        FieldType::String | FieldType::Id { .. } | FieldType::Bytes => value.is_string(),
        FieldType::Number | FieldType::Int64 => value.is_number(),
        FieldType::Boolean => value.is_boolean(),
        // Any / Literal / Union / Array / Object / Record / Vector / Null:
        // no reliable static check; accept any scalar (existing behavior).
        _ => matches!(
            value,
            serde_json::Value::String(_)
                | serde_json::Value::Number(_)
                | serde_json::Value::Bool(_)
        ),
    };
    if ok {
        Ok(())
    } else {
        Err(RtDbError::bad_request(format!(
            "filter on field '{field}' value kind does not match declared field type"
        )))
    }
}

/// jsonb-extraction path for a declared-but-not-indexed field: compare
/// `doc->>'field'` directly for text, or cast to `float8`/`boolean` when the
/// value is a number/boolean. The field name is a schema-validated identifier,
/// so it is safe inside the jsonb string literal.
fn jsonb_lhs_and_bind(
    field: &str,
    value: &serde_json::Value,
) -> Result<(String, EqBind), RtDbError> {
    match value {
        serde_json::Value::String(s) => Ok((format!("(doc->>'{field}')"), EqBind::Text(s.clone()))),
        serde_json::Value::Number(n) => {
            let f = n.as_f64().ok_or_else(|| {
                RtDbError::bad_request("filter number value is out of representable range")
            })?;
            Ok((format!("(doc->>'{field}')::float8"), EqBind::Num(f)))
        }
        serde_json::Value::Bool(b) => Ok((format!("(doc->>'{field}')::boolean"), EqBind::Bool(*b))),
        _ => Err(RtDbError::bad_request(
            "filter value must be a string, number, or boolean",
        )),
    }
}
