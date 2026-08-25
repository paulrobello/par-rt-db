//! Read path — query compilation and execution. Compiles a `Query` (terminal +
//! index/range/filter/order) into SQL against the typed index columns, merging
//! the `doc` jsonb with system fields at read time. Terminals: `get`, `collect`,
//! `first`, `unique`, `count`, `distinct`, `aggregate` (sum/avg/min/max/count),
//! `paginate`, and the ranked `search`/`vectorSearch`/`hybridSearch`. Read
//! visibility composes the client filter with the per-row authorization
//! predicates (`ownerField`/`collaboratorsField`/`authorize`) and the
//! `FilterExpr` variants those share. `MAX_TAKE` (4096) bounds result/group
//! counts — scale via `paginate`, not larger collects.
//!
//! ARC-203 split: this directory module keeps the public surface (re-exports),
//! `execute_query` + the terminal-combination validation cascade, and the
//! filter-shape warning helper. SQL construction lives in `filter.rs`
//! (filter/scan WHERE compilation), the btree-terminal compilers + compile/
//! execute routing in `terminals.rs`, the ranked search family + their shared
//! ctx in `search.rs`, and the per-row auth predicate rendering in `row_auth.rs`.

#[cfg(test)]
mod combinations_coverage;
mod filter;
mod row_auth;
mod search;
mod terminals;

use sqlx::PgPool;
use tracing::Instrument;

use crate::auth::PrincipalCtx;
use crate::error::RtDbError;
use crate::schema::{SchemaDef, SchemaDefExt, TableDef};
use search::{SearchCtx, execute_hybrid_search, execute_search, execute_vector_search};
use terminals::{
    PaginateExecCtx, compile_query_window, execute_aggregate_terminal, execute_collect_terminal,
    execute_count_terminal, execute_distinct_terminal, execute_paginate_terminal, point_read,
};

// ARC-202: the wire/DSL types this module used to define live in `dsl.rs`
// now; re-exported so every `crate::query::` path (and the integration
// tests' `rtdb_server::query::` paths) keep resolving unchanged.
pub use crate::dsl::{
    AggregateGroup, AggregateOp, AggregateSpec, FilterExpr, HybridSearchQuery, Order, Paginate,
    PaginatedResult, Query, QueryResult, SearchMode, SearchQuery, VectorSearchQuery,
    filter_matches,
};
// ARC-203: the compile/execute surface that used to live in this single file.
pub(crate) use filter::{compile_filter, compile_filter_literal, compile_scan_where};
pub use terminals::{CompiledQuery, compile_query};

/// Hard cap on rows returned by a single query, whether via an explicit
/// `take` or a `take`-less collect. Also bounds `distinct`, aggregate group
/// counts, and paginate page sizes. **Scaling guidance:** every result row is
/// materialized server-side, so this is a per-query memory ceiling shared
/// across all tenants on a multi-tenant instance. For larger scans, paginate
/// (`take(N)` / `paginate`) rather than collecting unbounded result sets —
/// pages stream and are not bound by this cap beyond the page size. Raise this
/// const only if a measured single-query workload genuinely needs >4096 rows
/// AND cannot paginate (bulk operations should use `PatchByQuery`/
/// `DeleteByQuery`, not large collects). `MAX_STEPS` (the txn step cap) was
/// raised to 1024 for larger atomic units; this collect cap is kept at 4096
/// deliberately (memory cost scales with it).
const MAX_TAKE: u32 = 4096;

/// Build the wire-corpus clause-presence set for `q` — one canonical clause
/// name (matching `wire-corpus/query-combinations.json`'s `clauses` array)
/// per `Query` field that is actually set. Fed to
/// `par_rt_db_core::query_combinations::check_query_combinations`.
fn query_clauses(q: &Query) -> std::collections::HashSet<&'static str> {
    let mut set = std::collections::HashSet::new();
    if q.get.is_some() {
        set.insert("get");
    }
    if q.index.is_some() {
        set.insert("index");
    }
    if !q.eq.is_empty() {
        set.insert("eq");
    }
    if q.gt.is_some() {
        set.insert("gt");
    }
    if q.gte.is_some() {
        set.insert("gte");
    }
    if q.lt.is_some() {
        set.insert("lt");
    }
    if q.lte.is_some() {
        set.insert("lte");
    }
    if q.order.is_some() {
        set.insert("order");
    }
    if q.take.is_some() {
        set.insert("take");
    }
    if q.unique {
        set.insert("unique");
    }
    if q.first {
        set.insert("first");
    }
    if q.count {
        set.insert("count");
    }
    if q.distinct {
        set.insert("distinct");
    }
    if q.aggregate.is_some() {
        set.insert("aggregate");
    }
    if q.paginate.is_some() {
        set.insert("paginate");
    }
    if q.filter.is_some() {
        set.insert("filter");
    }
    if q.search.is_some() {
        set.insert("search");
    }
    if q.vector_search.is_some() {
        set.insert("vectorSearch");
    }
    if q.hybrid_search.is_some() {
        set.insert("hybridSearch");
    }
    set
}

/// ENH-028 phase 2: the single compile-time combination check, replacing the
/// pre-refactor per-terminal `GET_PEERS`/`UNIQUE_PEERS`/`*_INCOMPATIBLES`/
/// `*_PEERS` cascade with one call into the table-driven evaluator shared
/// with the Rust client (`par_rt_db_core::query_combinations`, itself driven
/// by `wire-corpus/query-combinations.json`, the cross-runner semantics
/// corpus). Declaration order in the JSON only decides which message a
/// multi-violation query gets — accept/reject is unaffected — so calling this
/// once up front (rather than cascading per terminal) does not change
/// behavior; see the module doc on `par_rt_db_core::query_combinations`.
pub(crate) fn check_query_combinations(q: &Query) -> Result<(), RtDbError> {
    let present = query_clauses(q);
    par_rt_db_core::query_combinations::check_query_combinations(&present)
        .map_err(|violation| RtDbError::bad_request(violation.message))
}

/// Compile-time warnings about a query's filter shape. Currently a single
/// check: a `filter` predicate on a field the table declares but no index
/// covers — such a filter compiles to a jsonb extraction and forces a
/// sequential scan over the matching set. Walks the `FilterExpr` tree
/// (And/Or/Not recurse; every leaf variant carries `field: String`) and emits
/// one warning per offending field reference. Returns an empty vec when the
/// query has no filter, or when every filtered field is covered by some index.
/// A field counts as "indexed" iff it appears in any index's `fields` list or
/// in any vector index's `filter_fields` list.
pub fn collect_filter_warnings(schema: &SchemaDef, q: &Query) -> Vec<String> {
    let table_def = match schema.table(&q.table) {
        Ok(td) => td,
        Err(_) => return Vec::new(),
    };
    let mut warnings = Vec::new();
    if let Some(filter) = &q.filter {
        collect_unindexed_filter_fields(filter, table_def, &mut warnings);
    }
    if let Some(search) = &q.search
        && let Some(filter) = &search.filter
    {
        collect_unindexed_filter_fields(filter, table_def, &mut warnings);
    }
    if let Some(vs) = &q.vector_search
        && let Some(filter) = &vs.filter
    {
        collect_unindexed_filter_fields(filter, table_def, &mut warnings);
    }
    // hybridSearch has no client filter field (only per-axis index resolution).
    warnings.sort();
    warnings.dedup();
    warnings
}

/// Walk a `FilterExpr` tree and push one warning per leaf reference to a field
/// the table declares but no index covers. And/Or/Not recurse; every leaf
/// variant carries `field: String`. Unknown fields (not in `table_def.fields`)
/// are skipped — `compile_filter` will reject them with a `BadRequest` at
/// compile time, so warning about them here would be noise.
fn collect_unindexed_filter_fields(
    filter: &FilterExpr,
    table_def: &TableDef,
    out: &mut Vec<String>,
) {
    match filter {
        FilterExpr::And { exprs } | FilterExpr::Or { exprs } => {
            for e in exprs {
                collect_unindexed_filter_fields(e, table_def, out);
            }
        }
        FilterExpr::Not { expr } => {
            collect_unindexed_filter_fields(expr, table_def, out);
        }
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
            if !table_def.fields.contains_key(field) {
                return;
            }
            if field_is_indexed(table_def, field) {
                return;
            }
            out.push(format!(
                "filter on field '{field}' has no index; compiles to a jsonb extraction (sequential scan)"
            ));
        }
    }
}

/// A field counts as indexed iff it appears in any index's `fields` list or in
/// any vector index's `filter_fields` list.
fn field_is_indexed(table_def: &TableDef, field: &str) -> bool {
    table_def
        .indexes
        .iter()
        .any(|idx| idx.fields.iter().any(|f| f == field))
        || table_def.indexes.iter().any(|idx| {
            idx.vector
                .as_ref()
                .map(|spec| spec.filter_fields.iter().any(|f| f == field))
                .unwrap_or(false)
        })
}

/// Validate a `Query::fields` projection against the table: every name must be
/// a declared field or one of the system fields (`_id`/`_creationTime`/
/// `_version` — always included, so listing them is an allowed no-op). Anything
/// else — including typo'd system names and other `_`-prefixed names — is
/// `BadRequest` at compile time, the same gate `/explain` runs. `Some([])`
/// (system fields only) validates trivially.
pub fn validate_projection(table_def: &TableDef, fields: &[String]) -> Result<(), RtDbError> {
    const SYSTEM_FIELDS: [&str; 3] = ["_id", "_creationTime", "_version"];
    for name in fields {
        if SYSTEM_FIELDS.contains(&name.as_str()) || table_def.fields.contains_key(name) {
            continue;
        }
        return Err(RtDbError::bad_request(format!(
            "unknown projection field '{name}'"
        )));
    }
    Ok(())
}

/// Apply a `Query::fields` projection to an executed result: each doc keeps
/// its `_`-prefixed keys and the listed user fields; every other user field is
/// dropped. `_`-prefixed keys are exactly the system fields plus synthetic
/// result fields (`_searchSnippet`) — user fields can never be `_`-prefixed
/// (`validate_doc` rejects them at write time) — so this rule IS "system
/// fields are always present". Doc-less terminals (`Count`/`Distinct`/
/// `Aggregate`) are unaffected by construction. `Map::retain` preserves key
/// order, so `canonical` output stays stable across re-runs.
pub fn project_result(result: &mut QueryResult, fields: &[String]) {
    fn project(doc: &mut serde_json::Value, fields: &[String]) {
        if let serde_json::Value::Object(map) = doc {
            map.retain(|k, _| k.starts_with('_') || fields.iter().any(|f| f == k));
        }
    }
    match result {
        QueryResult::Doc(Some(doc)) => project(doc, fields),
        QueryResult::Docs(docs) => {
            for doc in docs {
                project(doc, fields);
            }
        }
        QueryResult::Paginated(page) => {
            for doc in &mut page.docs {
                project(doc, fields);
            }
        }
        _ => {}
    }
}

/// The canonical a subscription diffs against for push decisions. For an
/// UNPROJECTED query this is the plain [`canonical`] — byte-identical push
/// semantics to pre-projection behavior. For a PROJECTED query (`fields` set)
/// the volatile `_version` is stripped from every doc before serializing:
/// `_version` bumps on every write, so an unstripped canonical would push on
/// any member write even when no projected field changed — defeating the
/// payload-width point of a projected subscription. Pushed payloads still
/// carry `_version`; only the change-detection comparison ignores it. (A
/// subscriber that must see every `_version` bump should use an unprojected
/// subscription.)
pub fn diff_canonical(result: &QueryResult, q: &Query) -> String {
    if q.fields.is_none() {
        return canonical(result);
    }
    fn strip_version(doc: &mut serde_json::Value) {
        if let serde_json::Value::Object(map) = doc {
            map.remove("_version");
        }
    }
    let mut stripped = result.clone();
    match &mut stripped {
        QueryResult::Doc(Some(doc)) => strip_version(doc),
        QueryResult::Docs(docs) => docs.iter_mut().for_each(strip_version),
        QueryResult::Paginated(page) => page.docs.iter_mut().for_each(strip_version),
        _ => {}
    }
    canonical(&stripped)
}

pub async fn execute_query(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    q: &Query,
    ctx: &PrincipalCtx,
    include_deleted: bool,
) -> Result<QueryResult, RtDbError> {
    // ENH-018: wrap the read path in a `query.execute` span. The db/table/
    // terminal attributes are bounded (one per database/table, not per
    // document), so they are safe span attributes — never doc ids or content.
    // Nesting: a query issued by a subscriber's re-run lands as a child of the
    // `subs.rerun` span; a one-shot HTTP query is a root. The body runs inside
    // an `async` block `.instrument`-ed with the span because a sync
    // `Span::enter()` guard is `!Send` and would poison the future across the
    // `.await`s below.
    let span = tracing::info_span!(
        "query.execute",
        db,
        table = %q.table,
        terminal = q.terminal_name(),
    );
    async {
        // The read path is now compile-then-execute: `compile_query` produces
        // the exact SQL + ordered binds (the same compile the /explain admin
        // route uses), and this body binds them into sqlx and fetches. The
        // cascade (validate_db_name, authorize_table, peer-rejection, take cap,
        // range-bound mutual exclusion, terminal routing) runs inside
        // `compile_query`; this body is the execute tail.
        let (cq, _warnings) = compile_query(db, schema, q, ctx, include_deleted)?;
        let owner = ctx.user_id.as_deref();
        let table_def = schema.table(&q.table)?;
        let owner_field = table_def.owner_field.as_deref();
        let collaborators_field = table_def.collaborators_field.as_deref();
        let sctx = SearchCtx {
            pool,
            table_def,
            owner_field,
            collaborators_field,
            ctx,
        };
        let mut result = match cq.terminal {
            "get" => point_read(&sctx, cq, owner).await,
            // `snippet` travels on the query, not the CompiledQuery (whose
            // shape is the /explain contract), so the execute tail gets it
            // re-derived here — the same re-derive pattern the `paginate`
            // arm below uses.
            "search" => {
                let snippet = q.search.as_ref().is_some_and(|s| s.snippet == Some(true));
                execute_search(cq, pool, snippet).await
            }
            "vectorSearch" => execute_vector_search(cq, pool).await,
            "hybridSearch" => execute_hybrid_search(cq, pool).await,
            "count" => execute_count_terminal(cq, pool).await,
            "distinct" => execute_distinct_terminal(cq, pool).await,
            "aggregate" => execute_aggregate_terminal(cq, pool).await,
            "paginate" => {
                // Re-derive the PaginateExecCtx the compile step produced. The
                // ctx is pure metadata (index_def + eq_len + num_items); we
                // can't thread it through compile_query's single return type
                // without complicating the /explain contract (which doesn't
                // need it), so the execute path re-resolves it from the same
                // inputs compile used. The values are deterministic functions
                // of (table_def, q, ctx) — `compile_query_window` is pure.
                let w = compile_query_window(
                    table_def,
                    q,
                    ctx,
                    owner,
                    owner_field,
                    collaborators_field,
                    include_deleted,
                )?;
                let num_items = q
                    .paginate
                    .as_ref()
                    .map(|p| p.num_items.min(MAX_TAKE))
                    .unwrap_or(0);
                let paginate_ctx = PaginateExecCtx {
                    index_def: w.index_def,
                    eq_len: w.eq_len,
                    num_items,
                };
                execute_paginate_terminal(cq, paginate_ctx, pool).await
            }
            "unique" | "first" | "collect" => execute_collect_terminal(cq, pool).await,
            other => Err(RtDbError::internal(format!(
                "unknown compiled terminal '{other}'"
            ))),
        }?;
        // Apply the caller's field projection at this one seam: every
        // terminal's docs flow back through here, so one pass covers HTTP
        // one-shots, the WS initial subscribe push, and every subscription
        // re-run — whose canonical diff then sees the projected shape (a
        // write to a non-projected field re-runs the sub but leaves the
        // canonical unchanged, so nothing is pushed).
        if let Some(fields) = &q.fields {
            project_result(&mut result, fields);
        }
        Ok(result)
    }
    .instrument(span)
    .await
}

// ============ execute_query cascade helpers (QA-002) ============
//
// `execute_query` was the repo's worst complexity hotspot (cc216): a terminal
// cascade — setup → early-return terminals → validation flags → index-window
// compilation → inline terminals — all inline. The helpers below decompose it
// into a routing table without changing behavior: `compile_query_window` lifts
// the index-window compilation block verbatim, and each `compile_*_terminal`
// lifts one inline terminal's SQL-building block verbatim. Each terminal's
// `execute_*` companion then binds the `CompiledQuery.binds` into sqlx in order
// and fetches — the SQL string itself never varies between the compile and
// execute paths because there is now only one SQL-builder per terminal.

/// Stable string form for change detection (jsonb key order is canonical in Postgres).
pub fn canonical(result: &QueryResult) -> String {
    serde_json::to_string(result).unwrap_or_else(|err| {
        tracing::error!(error = %err, "failed to serialize query result");
        String::new()
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::{FilterExpr, compile_filter, compile_filter_literal, filter_matches};
    use crate::auth::PrincipalCtx;
    use crate::schema::{FieldType, IndexDef, TableDef};

    /// Builds a one-field `TableDef` whose single field is indexed, so
    /// `field_lhs_and_bind` takes its typed-column path (`"f_<field>"`) rather
    /// than the jsonb-extraction fallback. The exact rendered column name is
    /// part of what these tests lock down.
    fn one_indexed_field_table(field: &str, ft: FieldType) -> TableDef {
        let mut fields = BTreeMap::new();
        fields.insert(field.to_string(), ft);
        TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields,
            indexes: vec![IndexDef {
                name: format!("by_{field}"),
                fields: vec![field.to_string()],
                search: false,
                vector: None,
                unique: false,
                r#where: None,
                language: None,
            }],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        }
    }

    /// Builds a `TableDef` with the named `String` fields and no indexes, so
    /// filter compilation takes the jsonb-extraction path (`doc->>'field'`).
    fn test_table_with_fields(fields: &[&str]) -> TableDef {
        let mut fields_map = BTreeMap::new();
        for field in fields {
            fields_map.insert(field.to_string(), FieldType::String);
        }
        TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields: fields_map,
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        }
    }

    #[test]
    fn compile_filter_literal_emits_typed_literals_not_binds() {
        // eq on an indexed boolean column -> literal `false`, not `$1`.
        let table = one_indexed_field_table("deleted", FieldType::Boolean);
        let pred = FilterExpr::Eq {
            field: "deleted".into(),
            value: serde_json::json!(false),
        };
        let sql = compile_filter_literal(&pred, &table).unwrap();
        assert_eq!(sql, "\"f_deleted\" = false");
    }

    #[test]
    fn compile_filter_literal_escapes_string_literals() {
        let table = one_indexed_field_table("name", FieldType::String);
        let pred = FilterExpr::Eq {
            field: "name".into(),
            value: serde_json::json!("O'Brien"),
        };
        let sql = compile_filter_literal(&pred, &table).unwrap();
        assert_eq!(sql, "\"f_name\" = 'O''Brien'");
    }

    /// SEC-006 regression: a string literal carrying a classic SQL-injection
    /// payload MUST be rendered inert by the single-quote doubling in
    /// `render_literal` / `compile_filter_literal`. The emitted SQL stays a
    /// single string literal — the payload's embedded `'` is doubled to `''`,
    /// so the trailing `; DROP TABLE x; --` cannot terminate the literal and
    /// become a statement of its own. This is a defense-in-depth check: the
    /// value would also be bound via `$n` on the doc-eval path; this asserts
    /// the literal path (used by indexed-column filter compilation) is safe.
    #[test]
    fn compile_filter_literal_neutralizes_sql_injection_payload() {
        let table = one_indexed_field_table("name", FieldType::String);
        let pred = FilterExpr::Eq {
            field: "name".into(),
            value: serde_json::json!("'; DROP TABLE x; --"),
        };
        let sql = compile_filter_literal(&pred, &table).unwrap();
        // The payload's lone single quote is doubled; no statement terminator
        // appears outside the string literal, so the DROP cannot execute.
        assert_eq!(sql, "\"f_name\" = '''; DROP TABLE x; --'");
        // Belt-and-suspenders: the rendered fragment contains no unescaped
        // statement terminator — every `;` is inside the literal.
        let outside_literal: String = sql
            .split('\'')
            .enumerate()
            .filter(|(i, _)| *i % 2 == 0)
            .map(|(_, s)| s)
            .collect();
        assert!(
            !outside_literal.contains(';'),
            "statement terminator leaked outside string literal: {outside_literal}"
        );
    }

    #[test]
    fn compile_not_contains_exists() {
        // `editors` is an array-of-strings so `Contains` is a valid
        // predicate (SEC-125's validate_filter_expr_fields enforces this).
        let mut fields_map = BTreeMap::new();
        fields_map.insert("owner".to_string(), FieldType::String);
        fields_map.insert(
            "editors".to_string(),
            FieldType::Array {
                element: Box::new(FieldType::String),
            },
        );
        fields_map.insert("archivedat".to_string(), FieldType::String);
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields: fields_map,
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        // Not
        let (sql, binds) = compile_filter(
            &FilterExpr::Not {
                expr: Box::new(FilterExpr::Eq {
                    field: "owner".into(),
                    value: serde_json::json!("a"),
                }),
            },
            &table,
            1,
            false,
        )
        .unwrap();
        // SEC-117: Not wraps the inner in COALESCE(..., FALSE) so the SQL scan
        // path matches the two-valued Rust doc evaluator over absent fields.
        assert_eq!(sql, "NOT COALESCE(((doc->>'owner') = $1), FALSE)");
        assert_eq!(binds.len(), 1);
        // Contains: value in doc.editors[] -> jsonb membership
        let (sql, _) = compile_filter(
            &FilterExpr::Contains {
                field: "editors".into(),
                value: serde_json::json!("a"),
            },
            &table,
            1,
            false,
        )
        .unwrap();
        assert_eq!(sql, "(doc->'editors') ? $1");
        // Exists: field present and non-null
        let (sql, _) = compile_filter(
            &FilterExpr::Exists {
                field: "archivedat".into(),
            },
            &table,
            1,
            false,
        )
        .unwrap();
        assert_eq!(
            sql,
            "(doc ? 'archivedat' AND doc->>'archivedat' IS NOT NULL)"
        );
    }

    /// Exercises every `FilterExpr` variant plus the two principal markers
    /// (`$user`, `$email`) for the Rust-level doc evaluator. Variant happy-paths
    /// live here; the security-critical over-approximation property (missing
    /// field / type mismatch NEVER matches) lives in the companion
    /// `filter_matches_overapproximates_safely`.
    #[test]
    fn filter_matches_all_variants_and_principal() {
        let ctx = PrincipalCtx {
            user_id: Some("u1".to_string()),
            email: Some("e@x".to_string()),
            ..Default::default()
        };
        let doc = json!({"owner":"u1","editors":["u1","u2"],"visibility":"public","archivedat":null,"n":5});

        // Eq with `$user` marker resolves to ctx.user_id ("u1") -> matches doc.owner.
        assert!(filter_matches(
            &doc,
            &FilterExpr::Eq {
                field: "owner".into(),
                value: json!({"$user": true}),
            },
            &ctx,
        ));
        // Different principal -> no match.
        assert!(!filter_matches(
            &doc,
            &FilterExpr::Eq {
                field: "owner".into(),
                value: json!({"$user": true}),
            },
            &PrincipalCtx {
                user_id: Some("u9".to_string()),
                email: None,
                ..Default::default()
            },
        ));
        // Neq: a present-and-unequal field matches; present-and-equal does not.
        assert!(filter_matches(
            &doc,
            &FilterExpr::Neq {
                field: "owner".into(),
                value: json!("u9"),
            },
            &ctx,
        ));
        assert!(!filter_matches(
            &doc,
            &FilterExpr::Neq {
                field: "owner".into(),
                value: json!("u1"),
            },
            &ctx,
        ));
        // Gt/Gte/Lt/Lte over a numeric field (doc.n == 5).
        assert!(filter_matches(
            &doc,
            &FilterExpr::Gt {
                field: "n".into(),
                value: json!(3),
            },
            &ctx,
        ));
        assert!(!filter_matches(
            &doc,
            &FilterExpr::Gt {
                field: "n".into(),
                value: json!(7),
            },
            &ctx,
        ));
        assert!(filter_matches(
            &doc,
            &FilterExpr::Gte {
                field: "n".into(),
                value: json!(5),
            },
            &ctx,
        ));
        assert!(filter_matches(
            &doc,
            &FilterExpr::Lt {
                field: "n".into(),
                value: json!(7),
            },
            &ctx,
        ));
        assert!(filter_matches(
            &doc,
            &FilterExpr::Lte {
                field: "n".into(),
                value: json!(5),
            },
            &ctx,
        ));
        assert!(!filter_matches(
            &doc,
            &FilterExpr::Lte {
                field: "n".into(),
                value: json!(3),
            },
            &ctx,
        ));
        // In: member present -> true; absent -> false.
        assert!(filter_matches(
            &doc,
            &FilterExpr::In {
                field: "owner".into(),
                values: vec![json!("u1"), json!("u3")],
            },
            &ctx,
        ));
        assert!(!filter_matches(
            &doc,
            &FilterExpr::In {
                field: "owner".into(),
                values: vec![json!("u9")],
            },
            &ctx,
        ));
        // Contains with `$user` marker against editors[] (deep-equality semantics).
        assert!(filter_matches(
            &doc,
            &FilterExpr::Contains {
                field: "editors".into(),
                value: json!({"$user": true}),
            },
            &ctx,
        ));
        // And: both children true -> true; one false -> false.
        assert!(filter_matches(
            &doc,
            &FilterExpr::And {
                exprs: vec![
                    FilterExpr::Eq {
                        field: "owner".into(),
                        value: json!({"$user": true}),
                    },
                    FilterExpr::Eq {
                        field: "visibility".into(),
                        value: json!("public"),
                    },
                ],
            },
            &ctx,
        ));
        assert!(!filter_matches(
            &doc,
            &FilterExpr::And {
                exprs: vec![
                    FilterExpr::Eq {
                        field: "owner".into(),
                        value: json!({"$user": true}),
                    },
                    FilterExpr::Eq {
                        field: "visibility".into(),
                        value: json!("private"),
                    },
                ],
            },
            &ctx,
        ));
        // Or: visibility public OR owner is caller.
        assert!(filter_matches(
            &doc,
            &FilterExpr::Or {
                exprs: vec![
                    FilterExpr::Eq {
                        field: "visibility".into(),
                        value: json!("public"),
                    },
                    FilterExpr::Eq {
                        field: "owner".into(),
                        value: json!({"$user": true}),
                    },
                ],
            },
            &ctx,
        ));
        // Not(Exists{archivedat}) -> Exists is false (field present but null) -> Not is true.
        // The absent-field case is covered in the over-approximation companion.
        assert!(filter_matches(
            &doc,
            &FilterExpr::Not {
                expr: Box::new(FilterExpr::Exists {
                    field: "archivedat".into(),
                }),
            },
            &ctx,
        ));
        // `$email` marker resolves to ctx.email. Here ctx.email == "u1" matches owner.
        assert!(filter_matches(
            &doc,
            &FilterExpr::Eq {
                field: "owner".into(),
                value: json!({"$email": true}),
            },
            &PrincipalCtx {
                user_id: Some("u1".to_string()),
                email: Some("u1".to_string()),
                ..Default::default()
            },
        ));
    }

    /// Security-critical property: the over-approximation rule. A missing field
    /// or type mismatch NEVER matches — `cmp_json` returns `None` on doubt, so
    /// the comparison arms yield `false`. This is what makes `filter_matches`
    /// safe to use as the read/write auth predicate: it can never over-allow on
    /// ambiguous data. Also pins the absent-vs-present-null distinction for
    /// `Not(Exists)` and the permissive `Not(Eq)` over an absent field.
    #[test]
    fn filter_matches_overapproximates_safely() {
        let ctx = PrincipalCtx {
            user_id: Some("u1".to_string()),
            email: Some("e@x".to_string()),
            ..Default::default()
        };
        let doc = json!({"owner":"u1","archivedat":null,"n":5});

        // 1. Missing field on a comparison leaf -> false (no match), for every
        //    comparison kind. This is the core over-approximation guarantee:
        //    `Gt` over an absent field cannot accidentally pass.
        assert!(!filter_matches(
            &doc,
            &FilterExpr::Gt {
                field: "nosuchfield".into(),
                value: json!(1),
            },
            &ctx,
        ));
        // `Neq` over a missing field is ALSO false: `is_some_and` means "absent"
        // is treated as "not a match" rather than "trivially unequal". This is
        // the conservative direction — a row with a missing field does NOT get
        // included by a `Neq` predicate.
        assert!(!filter_matches(
            &doc,
            &FilterExpr::Neq {
                field: "nosuchfield".into(),
                value: json!("x"),
            },
            &ctx,
        ));
        // Eq over a missing field is likewise false.
        assert!(!filter_matches(
            &doc,
            &FilterExpr::Eq {
                field: "nosuchfield".into(),
                value: json!("x"),
            },
            &ctx,
        ));

        // 2. Type mismatch on a comparison leaf -> false. `doc.owner` is a
        //    string; comparing it numerically yields `cmp_json == None`, so
        //    every ordering arm is false rather than coercing or erroring.
        assert!(!filter_matches(
            &doc,
            &FilterExpr::Gt {
                field: "owner".into(),
                value: json!(1),
            },
            &ctx,
        ));
        assert!(!filter_matches(
            &doc,
            &FilterExpr::Lt {
                field: "owner".into(),
                value: json!(1),
            },
            &ctx,
        ));

        // 3. Not(Exists) over an ABSENT field -> true. Distinct from the
        //    present-null case (Exists{null} is also false); here the field is
        //    genuinely missing. Both reduce to `Not(false) == true`, but this
        //    pins the absent-field behavior explicitly.
        assert!(filter_matches(
            &doc,
            &FilterExpr::Not {
                expr: Box::new(FilterExpr::Exists {
                    field: "absentfield".into(),
                }),
            },
            &ctx,
        ));

        // 4. Not(Eq) over an absent field -> true (the permissive direction
        //    called out in the implementation doc comment). The inner Eq is
        //    false (field missing), so Not yields true. The SQL compile path
        //    emits `NOT COALESCE((<inner>), FALSE)`, so it agrees over the
        //    absent-field case (SEC-117); a cross-evaluator agreement test
        //    lives in `per_row_auth_test.rs`. Acceptable for a server-declared
        //    /validated authorize predicate; pinned here so the behavior is
        //    explicit and visible to reviewers of Tasks 7-9.
        assert!(filter_matches(
            &doc,
            &FilterExpr::Not {
                expr: Box::new(FilterExpr::Eq {
                    field: "nosuchfield".into(),
                    value: json!("anything"),
                }),
            },
            &ctx,
        ));
    }

    /// SEC-126: a comparison whose value kind does not match the declared field
    /// type MUST be rejected at compile time, rather than compiling to a
    /// per-row cast (`(doc->>'title')::float8`) that errors on the first
    /// non-conforming stored value. For a subscription re-running on every
    /// write, that silent failure mode never pushes — the dangerous case
    /// CLAUDE.md calls out. This pins the compile-time gate.
    #[test]
    fn compile_filter_rejects_type_mismatched_comparison() {
        let table = test_table_with_fields(&["title"]);
        // Number value against a String field — would compile to
        // `(doc->>'title')::float8` without the SEC-126 check.
        let pred = FilterExpr::Gt {
            field: "title".into(),
            value: json!(5),
        };
        let err = compile_filter(&pred, &table, 1, false).unwrap_err();
        assert!(
            err.message.contains("value kind does not match"),
            "expected type-mismatch message, got: {}",
            err.message
        );

        // Sanity: same field with a matching String value still compiles.
        let ok = FilterExpr::Gt {
            field: "title".into(),
            value: json!("a"),
        };
        assert!(compile_filter(&ok, &table, 1, false).is_ok());
    }
}
