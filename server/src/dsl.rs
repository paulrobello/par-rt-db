//! Pure wire/DSL vocabulary shared by the read and write paths: the
//! serde-derived request/response types clients put on the wire (`Query`,
//! `FilterExpr`, `Transaction`, `Step`, the search/aggregate terminal specs),
//! the typed bind helpers (`EqBind`/`eq_binds`/`eq_bind_for`) both executors
//! share, and the pure in-memory predicate evaluators (`filter_matches`,
//! `row_visible_to`). This module has NO sqlx/tokio dependency by design —
//! `query.rs` (read executor) and `txn.rs` (write executor) both build on it,
//! so neither imports the other's types (ARC-202: this extraction broke the
//! former `query.rs` <-> `txn.rs` type cycle; both re-export what they used to
//! own so existing `crate::query::` / `crate::txn::` paths keep resolving).
//!
//! The types here are the server's implementation of the four-way wire
//! contract (`server/src/protocol.rs`, `ts-client/src/protocol.ts`,
//! `rust-client/src/wire.rs`, `python-client/src/par_rt_db/wire.py`): every
//! serde tag, field name, and casing is load-bearing and deliberately
//! non-uniform — do not normalize them.

use std::cmp::Ordering;

use crate::auth::PrincipalCtx;
use crate::error::RtDbError;
use crate::schema::{FieldType, IndexDef, TableDef, indexed_column_type};
pub use par_rt_db_core::wire::FilterExpr;

// ARC-007: the wire `Query` and its terminal/helper types are now defined once
// in `par_rt_db_core::query` (same reasoning as `FilterExpr`/`Step` before it:
// two Rust copies of one wire type can only drift). Re-exported here at their
// historical path so every `crate::dsl::` call site keeps resolving.
// Server-only behavior lives in the extension traits below — the orphan rule
// forbids inherent impls on these foreign types.
pub use par_rt_db_core::query::{
    AggregateGroup, AggregateMultiGroup, AggregateOp, AggregateSpec, GroupBy, HybridSearchQuery,
    Order, Paginate, Query, SearchMode, SearchQuery, VectorSearchQuery,
};

/// Server-side extension methods on the core [`Query`] (orphan rule: an
/// inherent `impl` on the foreign `par_rt_db_core::query::Query` is not
/// allowed). Bring this trait into scope alongside `Query` for the
/// `.terminal_name()` call syntax.
pub trait QueryTerminalNameExt {
    /// The wire label for this query's terminal — the output mode an operator
    /// sees in the subscription inspector (`GET /admin/subscriptions`).
    /// Terminals are mutually exclusive (validated in `execute_query`), so the
    /// first set field wins; a query with no terminal is a plain `collect`.
    fn terminal_name(&self) -> &'static str;
}

impl QueryTerminalNameExt for Query {
    fn terminal_name(&self) -> &'static str {
        if self.get.is_some() {
            "get"
        } else if self.count {
            "count"
        } else if self.distinct {
            "distinct"
        } else if self.aggregate.is_some() {
            "aggregate"
        } else if self.search.is_some() {
            "search"
        } else if self.vector_search.is_some() {
            "vectorSearch"
        } else if self.hybrid_search.is_some() {
            "hybridSearch"
        } else if self.paginate.is_some() {
            "paginate"
        } else if self.first {
            "first"
        } else if self.unique {
            "unique"
        } else if self.take.is_some() {
            "take"
        } else {
            "collect"
        }
    }
}

/// Server-side extension methods on the core [`AggregateOp`] (orphan rule —
/// see [`QueryTerminalNameExt`]).
pub trait AggregateOpSqlExt {
    /// The SQL aggregate function name (uppercase, matching the SQL keyword).
    fn sql_fn(self) -> &'static str;
    /// Whether this op aggregates a field value (and so needs an aggregate
    /// index field beyond the eq prefix / group field). `Count` counts rows and
    /// consumes no field.
    fn needs_field(self) -> bool;
}

impl AggregateOpSqlExt for AggregateOp {
    fn sql_fn(self) -> &'static str {
        match self {
            AggregateOp::Sum => "SUM",
            AggregateOp::Avg => "AVG",
            AggregateOp::Min => "MIN",
            AggregateOp::Max => "MAX",
            AggregateOp::Count => "COUNT",
        }
    }

    fn needs_field(self) -> bool {
        !matches!(self, AggregateOp::Count)
    }
}

/// Server-side extension methods on the core [`AggregateSpec`] (orphan rule —
/// see [`QueryTerminalNameExt`]).
pub trait AggregateSpecOpsExt {
    /// The ops to evaluate as `(alias, op)` pairs in deterministic order. A
    /// single-`op` spec aliases its result by the lowercase op name (used only
    /// by the wire-v2 result shapes). Enforces exactly-one-of and the alias
    /// charset — aliases become `jsonb_build_object` labels, so anything
    /// outside `[A-Za-z0-9_]` (≤ 64 chars) is rejected rather than escaped.
    fn aliased_ops(&self) -> Result<Vec<(String, AggregateOp)>, RtDbError>;
}

impl AggregateSpecOpsExt for AggregateSpec {
    fn aliased_ops(&self) -> Result<Vec<(String, AggregateOp)>, RtDbError> {
        match (&self.op, &self.aggregates) {
            (Some(op), None) => Ok(vec![(op.sql_fn().to_lowercase(), *op)]),
            (None, Some(map)) => {
                if map.is_empty() {
                    return Err(RtDbError::bad_request("aggregates must not be empty"));
                }
                for alias in map.keys() {
                    if alias.is_empty() || alias.len() > 64 {
                        return Err(RtDbError::bad_request(
                            "aggregates alias must be 1-64 characters",
                        ));
                    }
                    if !alias.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                        return Err(RtDbError::bad_request(
                            "aggregates alias may contain only [A-Za-z0-9_]",
                        ));
                    }
                }
                Ok(map.iter().map(|(a, o)| (a.clone(), *o)).collect())
            }
            (Some(_), Some(_)) => Err(RtDbError::bad_request(
                "aggregate op and aggregates are mutually exclusive",
            )),
            (None, None) => Err(RtDbError::bad_request(
                "aggregate requires op or aggregates",
            )),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(untagged)]
pub enum QueryResult {
    Doc(Option<serde_json::Value>),       // get / unique: doc or null
    Docs(Vec<serde_json::Value>),         // take / collect
    Count(i64),                           // count: total matching rows, uncapped by MAX_TAKE
    Paginated(PaginatedResult),           // paginate: page of docs + optional next cursor
    Distinct(Vec<serde_json::Value>), // distinct: unique values of index.fields[eq.len()] over the matching set
    Aggregate(serde_json::Value), // aggregate: bare scalar (null if no rows match), e.g. `42`, `"x"`, `null`
    AggregateGroups(Vec<AggregateGroup>), // aggregate groupBy: array of `{key, value}` rows
    /// Wire v2 `aggregates` map, ungrouped: one object alias → value, e.g.
    /// `{"revenue": 1250.5, "orders": 42}`. A NULL aggregate surfaces as JSON
    /// null per alias.
    AggregateMulti(serde_json::Value),
    /// Wire v2 multi-op / multi-field groupBy rows: `keys` are the group's
    /// values of the groupBy fields in declared order; `values` maps each
    /// alias (or, for a single-`op` spec, the lowercase op name) to its
    /// aggregate. Ordered by group keys ascending, capped by `MAX_TAKE`.
    AggregateMultiGroups(Vec<AggregateMultiGroup>),
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PaginatedResult {
    pub docs: Vec<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// `Step`/`Transaction` are now `par_rt_db_core::mutation` types (ARC-004
/// follow-up), re-exported here at their historical path so every existing
/// `crate::dsl::{Step, Transaction}` call site keeps resolving.
pub use par_rt_db_core::mutation::{Step, Transaction};

/// `Step::table()` is an extension trait rather than an inherent `impl`
/// because `Step` is now a foreign type (owned by `par_rt_db_core`) — the
/// orphan rule forbids `impl Step { .. }` here. Bring this trait into scope
/// alongside `Step` to keep the `step.table()` call syntax.
pub trait StepTableExt {
    /// The document table this step targets, or `None` for the schedule and
    /// workflow control-flow steps (they touch no documents; the per-step
    /// table-scope gate in `execute_txn` skips them, and `Step::Schedule` /
    /// `Step::StartWorkflow` check their NESTED steps recursively via
    /// `authorize_txn_tables` / `authorize_spec_tables` instead).
    fn table(&self) -> Option<&str>;
}

impl StepTableExt for Step {
    fn table(&self) -> Option<&str> {
        match self {
            Step::Insert { table, .. }
            | Step::Patch { table, .. }
            | Step::AdjustCounter { table, .. }
            | Step::Replace { table, .. }
            | Step::Delete { table, .. }
            | Step::ExpectVersion { table, .. }
            | Step::ExpectAbsent { table, .. }
            | Step::Upsert { table, .. }
            | Step::PatchByQuery { table, .. }
            | Step::DeleteByQuery { table, .. }
            | Step::Undelete { table, .. } => Some(table),
            Step::Schedule { .. }
            | Step::CancelSchedule { .. }
            | Step::StartWorkflow { .. }
            | Step::CancelWorkflow { .. } => None,
        }
    }
}

/// SQL bind for an eq-lookup value, typed per the index field's `FieldType`
/// (`Optional` unwrapped). Prefix-friendly: callers may supply 0..=all of an
/// index's fields; full-arity enforcement is the caller's responsibility.
///
/// `Clone` + `PartialEq` are derived so `subs::IndexedRead` can store typed
/// binds (cloned from a query at registration) and compare a written doc's
/// typed field value against the wanted bind in `in_window`. `Eq` is NOT
/// derived: the `Num(f64)` arm admits NaN, which has no total order; the
/// binds compared here always originate from JSON (which cannot carry NaN),
/// so `PartialEq` is sound for the membership test.
#[derive(Debug, Clone, PartialEq)]
pub enum EqBind {
    Text(String),
    Num(f64),
    Bool(bool),
    I64(i64),
}

/// Resolves `eq` (a prefix of `index`'s fields, 0..=all) into typed SQL binds.
/// Arity beyond the index's field count is a `BadRequest`; exact-arity
/// enforcement for Task 5's call sites happens in `eq_lookup`.
pub(crate) fn eq_binds(
    table: &TableDef,
    index: &IndexDef,
    eq: &[serde_json::Value],
) -> Result<Vec<EqBind>, RtDbError> {
    if eq.len() > index.fields.len() {
        return Err(RtDbError::bad_request(format!(
            "index '{}' expects at most {} eq value(s), got {}",
            index.name,
            index.fields.len(),
            eq.len()
        )));
    }

    index
        .fields
        .iter()
        .zip(eq.iter())
        .map(|(field_name, value)| {
            let field_type = table.fields.get(field_name).ok_or_else(|| {
                RtDbError::internal(format!("index references unknown field '{field_name}'"))
            })?;
            eq_bind_for(field_type, value)
        })
        .collect()
}

/// Shared with `query.rs`, which reuses this to type range-bound (`gt`/`gte`/`lt`/`lte`)
/// values the same way `eq` values are typed here.
pub(crate) fn eq_bind_for(ty: &FieldType, value: &serde_json::Value) -> Result<EqBind, RtDbError> {
    let (pg_type, _nullable) = indexed_column_type(ty)?;
    match pg_type {
        "text" => value
            .as_str()
            .map(|s| EqBind::Text(s.to_string()))
            .ok_or_else(|| RtDbError::bad_request("eq value must be a string")),
        "double precision" => value
            .as_f64()
            .map(EqBind::Num)
            .ok_or_else(|| RtDbError::bad_request("eq value must be a number")),
        "bigint" => value
            .as_str()
            .and_then(|s| s.parse::<i64>().ok())
            .map(EqBind::I64)
            .ok_or_else(|| RtDbError::bad_request("eq value must be an int64 string")),
        "boolean" => value
            .as_bool()
            .map(EqBind::Bool)
            .ok_or_else(|| RtDbError::bad_request("eq value must be a boolean")),
        other => Err(RtDbError::internal(format!(
            "unexpected pg type '{other}' for eq bind"
        ))),
    }
}

/// Whether `uid` may access a row given the table's declared `ownerField`
/// and/or `collaboratorsField`: true when `uid` matches the doc's owner field
/// OR appears in the doc's collaborators array. A missing/null owner field and
/// a missing/null/empty/non-array collaborators array are treated as no-match.
/// Shared by the read path's point-read filter and the write path's pre-check
/// so OR-enforcement stays consistent across reads, writes, and subscriptions
/// (subscriptions re-run `execute_query`, which carries the same semantics).
pub fn row_visible_to(
    doc: &serde_json::Value,
    owner_field: Option<&str>,
    collab_field: Option<&str>,
    uid: &str,
) -> bool {
    let owner_match = owner_field
        .and_then(|f| doc.get(f))
        .and_then(|v| v.as_str())
        .is_some_and(|s| s == uid);
    let collab_match = collab_field
        .and_then(|f| doc.get(f))
        .and_then(|v| v.as_array())
        .is_some_and(|arr| arr.iter().any(|v| v.as_str() == Some(uid)));
    owner_match || collab_match
}

// ============ Doc-level filter evaluator (model C `authorize`) ============

/// Resolves a principal marker to its `Value`: `{"$user": true}` → the caller's
/// `user_id`, `{"$email": true}` → the caller's `email`. A marker whose
/// principal field is `None` (a `Machine`/admin/scheduled caller) resolves to
/// `Null`, which cannot equal any non-null doc field — i.e. the predicate
/// silently fails to match, the safe over-approximation. Non-marker values pass
/// through unchanged.
pub(crate) fn resolve_value(v: &serde_json::Value, ctx: &PrincipalCtx) -> serde_json::Value {
    if let serde_json::Value::Object(map) = v
        && map.len() == 1
    {
        if let Some(true) = map.get("$user").and_then(|x| x.as_bool()) {
            return ctx
                .user_id
                .clone()
                .map(serde_json::Value::String)
                .unwrap_or(serde_json::Value::Null);
        }
        if let Some(true) = map.get("$email").and_then(|x| x.as_bool()) {
            return ctx
                .email
                .clone()
                .map(serde_json::Value::String)
                .unwrap_or(serde_json::Value::Null);
        }
    }
    v.clone()
}

/// Typed comparison between a doc field value (`a`, `None` when the field is
/// absent) and a resolved predicate value (`b`). Numbers compare as `f64`,
/// strings compare as `str`, booleans compare as `bool`. Returns `None` on any
/// type-mismatch or missing field — the over-approximation that ensures a
/// `Gt`/`Gte`/`Lt`/`Lte` leaf with doubt can never match erroneously (the
/// comparison arms in `filter_matches` treat `None` as "no match").
fn cmp_json(a: Option<&serde_json::Value>, b: &serde_json::Value) -> Option<Ordering> {
    let a = a?;
    // Numbers compare as f64 regardless of integer/float width.
    if let (Some(an), Some(bn)) = (a.as_f64(), b.as_f64()) {
        return an.partial_cmp(&bn);
    }
    if let (Some(as_), Some(bs)) = (a.as_str(), b.as_str()) {
        return Some(as_.cmp(bs));
    }
    if let (Some(ab), Some(bb)) = (a.as_bool(), b.as_bool()) {
        return Some(ab.cmp(&bb));
    }
    None
}

/// Evaluates `expr` against an in-memory `doc` with principal markers resolved
/// against `ctx`. This is the Rust-level counterpart to the SQL `compile_filter`
/// path, used where a doc is already materialized: point-reads, write
/// pre-checks, and insert verification (Tasks 7-9).
///
/// Over-approximation rule: any missing-field or type-mismatch doubt in a
/// comparison yields "no match" (`cmp_json` returns `None` ⇒ the comparison
/// arms are `false`), so `filter_matches` never matches erroneously. It is a
/// pure boolean; the caller decides the security default on a `false` result
/// (Forbidden for writes, excluded for reads) — `filter_matches` does NOT
/// decide Forbidden vs. filter.
///
/// `Contains` uses deep equality (`Value ==`) to match the three clients'
/// in-memory evaluators (Task 2). The server's SQL path compiles to jsonb `?`,
/// a text-level containment test; the two agree for scalar/string array
/// elements — the only realistic auth case (e.g. `$user ∈ doc.editors[]`).
///
/// `Not` note: `Not(Eq { field, value })` when `field` is absent evaluates the
/// inner `Eq` to `false`, so `Not` yields `true` (the row is visible). The SQL
/// compile path (`compile_filter_node`) matches this: it emits
/// `NOT COALESCE((<inner>), FALSE)`, so a NULL inner result (absent field) is
/// coerced to FALSE before negation, yielding TRUE = row visible (SEC-117).
/// The two evaluators agree over absent fields — this is load-bearing for
/// `authorize` predicates, which use `filter_matches` on the write path and the
/// SQL compile on the read path; a divergence there was an auth bypass.
/// Reviewers adding a new comparison variant must keep both arms consistent.
pub fn filter_matches(doc: &serde_json::Value, expr: &FilterExpr, ctx: &PrincipalCtx) -> bool {
    match expr {
        FilterExpr::Eq { field, value } => doc
            .get(field)
            .is_some_and(|d| d == &resolve_value(value, ctx)),
        FilterExpr::Neq { field, value } => doc
            .get(field)
            .is_some_and(|d| d != &resolve_value(value, ctx)),
        FilterExpr::Gt { field, value } => cmp_json(doc.get(field), &resolve_value(value, ctx))
            .is_some_and(|o| o == Ordering::Greater),
        FilterExpr::Gte { field, value } => cmp_json(doc.get(field), &resolve_value(value, ctx))
            .is_some_and(|o| o != Ordering::Less),
        FilterExpr::Lt { field, value } => cmp_json(doc.get(field), &resolve_value(value, ctx))
            .is_some_and(|o| o == Ordering::Less),
        FilterExpr::Lte { field, value } => cmp_json(doc.get(field), &resolve_value(value, ctx))
            .is_some_and(|o| o != Ordering::Greater),
        FilterExpr::In { field, values } => doc
            .get(field)
            .is_some_and(|d| values.iter().any(|v| d == &resolve_value(v, ctx))),
        FilterExpr::And { exprs } => exprs.iter().all(|e| filter_matches(doc, e, ctx)),
        FilterExpr::Or { exprs } => exprs.iter().any(|e| filter_matches(doc, e, ctx)),
        // `Not` inverts the inner boolean. The SQL compile path emits
        // `NOT COALESCE((<inner>), FALSE)` so a NULL inner (absent field)
        // becomes FALSE→TRUE here and on SQL — the two agree (SEC-117).
        FilterExpr::Not { expr } => !filter_matches(doc, expr, ctx),
        FilterExpr::Contains { field, value } => doc
            .get(field)
            .and_then(|v| v.as_array())
            .is_some_and(|arr| arr.iter().any(|v| v == &resolve_value(value, ctx))),
        FilterExpr::Exists { field } => doc.get(field).is_some_and(|v| !v.is_null()),
        // Unreachable through validation: relative-time predicates are
        // by-query-only (accepted with `allow_relative_time = true` there,
        // rejected in authorize / case-when / read filters), and
        // `filter_matches` evaluates only authorize predicates and case
        // whens. `false` is the fail-closed answer if one ever reaches here
        // anyway (deny the write / take the `otherwise` branch).
        FilterExpr::OlderThan { .. } => false,
    }
}
