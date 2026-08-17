//! Read path — query compilation and execution. Compiles a `Query` (terminal +
//! index/range/filter/order) into SQL against the typed index columns, merging
//! the `doc` jsonb with system fields at read time. Terminals: `get`, `collect`,
//! `first`, `unique`, `count`, `distinct`, `aggregate` (sum/avg/min/max/count),
//! `paginate`, and the ranked `search`/`vectorSearch`/`hybridSearch`. Read
//! visibility composes the client filter with the per-row authorization
//! predicates (`ownerField`/`collaboratorsField`/`authorize`) and the
//! `FilterExpr` variants those share. `MAX_TAKE` (4096) bounds result/group
//! counts — scale via `paginate`, not larger collects.

use sqlx::PgPool;
use tracing::Instrument;

use crate::auth::{PrincipalCtx, authorize_table};
use crate::db::validate_db_name;
use crate::ddl::{pg_col, pg_schema, pg_search_col, pg_table, pg_vector_col};
use crate::dsl::{EqBind, eq_bind_for, eq_binds, resolve_value, row_visible_to};
use crate::error::RtDbError;
use crate::pagination::{decode_cursor, encode_cursor};
use crate::schema::{FieldType, IndexDef, SchemaDef, TableDef, validate_filter_expr_fields};

// ARC-202: the wire/DSL types this module used to define live in `dsl.rs`
// now; re-exported so every `crate::query::` path (and the integration
// tests' `rtdb_server::query::` paths) keep resolving unchanged.
pub use crate::dsl::{
    AggregateGroup, AggregateOp, AggregateSpec, FilterExpr, HybridSearchQuery, Order, Paginate,
    PaginatedResult, Query, QueryResult, SearchMode, SearchQuery, VectorSearchQuery,
    filter_matches,
};

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

/// Hard cap on `vectorSearch` `limit`.
const VECTOR_SEARCH_MAX_LIMIT: u32 = 256;

/// Server-fixed `ts_headline` options for `snippet: true` search results
/// (FM-31). The client opts in with a boolean and can supply none of these —
/// word bounds and highlight delimiters are server-owned. `<mark>` renders
/// directly in HTML/React; MaxWords/MinWords are the PostgreSQL defaults made
/// explicit so the bound is visible and owned here.
const SNIPPET_HEADLINE_OPTS: &str = "StartSel=<mark>, StopSel=</mark>, MaxWords=35, MinWords=15";

// ============ Validation cascade infrastructure (QA-002) ============
//
// The validation cascade in `execute_query` is a sequence of "if terminal X is
// set AND peer Y is set → BadRequest" guards. Each terminal historically carried
// its own hand-written if-chain, so adding a terminal meant adding 5–10 clauses
// across server + TS — exactly the drift that produced QA-001 (the TS `get`
// guard omitted `filter`/`search`/`vectorSearch`).
//
// The infrastructure below lets each terminal DECLARE its incompatible peers;
// `execute_query` consults the active terminal's list once. Adding a terminal is
// now a one-line addition to the relevant const table, and the cross-client
// combination-matrix test (`server/tests/query_combinations.rs` and its TS
// mirror `ts-client/tests/query_combinations.test.ts`) catches any drift.
//
// Behavior-preserving: same peers checked, same messages emitted, same
// first-match-wins ordering as the pre-refactor inline cascade.

/// A `Query` field that can participate in a combination-rejection rule. Each
/// variant wraps the "is this field set?" predicate for one field, so peer lists
/// read as data instead of being inlined as boolean chains.
#[derive(Copy, Clone)]
enum Peer {
    Get,
    Index,
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
    Order,
    Take,
    Unique,
    First,
    Count,
    Distinct,
    Aggregate,
    Paginate,
    Filter,
    Search,
    VectorSearch,
    HybridSearch,
}

impl Peer {
    fn is_set(self, q: &Query) -> bool {
        match self {
            Self::Get => q.get.is_some(),
            Self::Index => q.index.is_some(),
            Self::Eq => !q.eq.is_empty(),
            Self::Gt => q.gt.is_some(),
            Self::Gte => q.gte.is_some(),
            Self::Lt => q.lt.is_some(),
            Self::Lte => q.lte.is_some(),
            Self::Order => q.order.is_some(),
            Self::Take => q.take.is_some(),
            Self::Unique => q.unique,
            Self::First => q.first,
            Self::Count => q.count,
            Self::Distinct => q.distinct,
            Self::Aggregate => q.aggregate.is_some(),
            Self::Paginate => q.paginate.is_some(),
            Self::Filter => q.filter.is_some(),
            Self::Search => q.search.is_some(),
            Self::VectorSearch => q.vector_search.is_some(),
            Self::HybridSearch => q.hybrid_search.is_some(),
        }
    }
}

/// A peer that conflicts with a terminal, plus the message to emit when it's
/// set. Per-peer entries let a terminal emit different messages for different
/// peers (matching the pre-refactor cascade's per-peer messages for
/// `first`/`count`/`paginate`).
struct Incompatible {
    peer: Peer,
    message: &'static str,
}

/// Aggregate-message helper: if ANY of `peers` is set on `q`, return
/// `BadRequest(message)`. Used by terminals whose pre-refactor check emitted a
/// single message regardless of which peer was set (`get`, `unique`,
/// `vector_search`, `search`).
fn reject_if_any_set(q: &Query, peers: &[Peer], message: &str) -> Result<(), RtDbError> {
    if peers.iter().any(|p| p.is_set(q)) {
        return Err(RtDbError::bad_request(message));
    }
    Ok(())
}

/// Per-peer helper: for each entry in `entries` (in declaration order), if its
/// peer is set on `q`, return `BadRequest(entry.message)`. Used by terminals
/// whose pre-refactor check emitted peer-specific messages (`first`, `count`,
/// `paginate`). Declaration order preserves the original cascade's
/// first-match-wins ordering, so the same combination produces the same message.
fn reject_per_peer_set(q: &Query, entries: &[Incompatible]) -> Result<(), RtDbError> {
    for entry in entries {
        if entry.peer.is_set(q) {
            return Err(RtDbError::bad_request(entry.message));
        }
    }
    Ok(())
}

// Per-terminal incompatible-peer tables. Order within each table mirrors the
// pre-refactor cascade's check order.

const GET_PEERS: &[Peer] = &[
    Peer::Index,
    Peer::Eq,
    Peer::Gt,
    Peer::Gte,
    Peer::Lt,
    Peer::Lte,
    Peer::Order,
    Peer::Take,
    Peer::Unique,
    Peer::First,
    Peer::Count,
    Peer::Distinct,
    Peer::Aggregate,
    Peer::Paginate,
    Peer::Filter,
    Peer::Search,
    Peer::VectorSearch,
    Peer::HybridSearch,
];
const GET_MESSAGE: &str = "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, distinct, aggregate, paginate, filter, search, or vector search";

const UNIQUE_PEERS: &[Peer] = &[Peer::Take, Peer::Order, Peer::Distinct, Peer::Aggregate];
const UNIQUE_MESSAGE: &str = "unique cannot be combined with take, order, distinct, or aggregate";

const FIRST_INCOMPATIBLES: &[Incompatible] = &[
    Incompatible {
        peer: Peer::Unique,
        message: "first cannot be combined with unique",
    },
    Incompatible {
        peer: Peer::Take,
        message: "first cannot be combined with take",
    },
    Incompatible {
        peer: Peer::Distinct,
        message: "first cannot be combined with distinct",
    },
    Incompatible {
        peer: Peer::Aggregate,
        message: "first cannot be combined with aggregate",
    },
];

const COUNT_INCOMPATIBLES: &[Incompatible] = &[
    Incompatible {
        peer: Peer::Unique,
        message: "count cannot be combined with unique",
    },
    Incompatible {
        peer: Peer::Take,
        message: "count cannot be combined with take",
    },
    Incompatible {
        peer: Peer::First,
        message: "count cannot be combined with first",
    },
    Incompatible {
        peer: Peer::Order,
        message: "count cannot be combined with order",
    },
    Incompatible {
        peer: Peer::Distinct,
        message: "count cannot be combined with distinct",
    },
    Incompatible {
        peer: Peer::Aggregate,
        message: "count cannot be combined with aggregate",
    },
];

const DISTINCT_INCOMPATIBLES: &[Incompatible] = &[
    Incompatible {
        peer: Peer::Get,
        message: "distinct cannot be combined with get",
    },
    Incompatible {
        peer: Peer::Take,
        message: "distinct cannot be combined with take",
    },
    Incompatible {
        peer: Peer::Unique,
        message: "distinct cannot be combined with unique",
    },
    Incompatible {
        peer: Peer::First,
        message: "distinct cannot be combined with first",
    },
    Incompatible {
        peer: Peer::Count,
        message: "distinct cannot be combined with count",
    },
    Incompatible {
        peer: Peer::Aggregate,
        message: "distinct cannot be combined with aggregate",
    },
    Incompatible {
        peer: Peer::Order,
        message: "distinct cannot be combined with order",
    },
    Incompatible {
        peer: Peer::Paginate,
        message: "distinct cannot be combined with paginate",
    },
    Incompatible {
        peer: Peer::Search,
        message: "distinct cannot be combined with search",
    },
    Incompatible {
        peer: Peer::VectorSearch,
        message: "distinct cannot be combined with vector search",
    },
    Incompatible {
        peer: Peer::HybridSearch,
        message: "distinct cannot be combined with hybrid search",
    },
];

/// `aggregate` is a standalone terminal like `distinct`/`count`: it rejects
/// every other terminal except `index`/`eq`/range bounds/`filter` (which
/// narrow the matching set the aggregate runs over). `take` is also rejected
/// — groups are capped internally by `MAX_TAKE` instead, so the matrix stays
/// simple (the alternative was a `take`-caps-groups carve-out that crossed
/// two terminals).
const AGGREGATE_INCOMPATIBLES: &[Incompatible] = &[
    Incompatible {
        peer: Peer::Get,
        message: "aggregate cannot be combined with get",
    },
    Incompatible {
        peer: Peer::Take,
        message: "aggregate cannot be combined with take",
    },
    Incompatible {
        peer: Peer::Unique,
        message: "aggregate cannot be combined with unique",
    },
    Incompatible {
        peer: Peer::First,
        message: "aggregate cannot be combined with first",
    },
    Incompatible {
        peer: Peer::Count,
        message: "aggregate cannot be combined with count",
    },
    Incompatible {
        peer: Peer::Distinct,
        message: "aggregate cannot be combined with distinct",
    },
    Incompatible {
        peer: Peer::Order,
        message: "aggregate cannot be combined with order",
    },
    Incompatible {
        peer: Peer::Paginate,
        message: "aggregate cannot be combined with paginate",
    },
    Incompatible {
        peer: Peer::Search,
        message: "aggregate cannot be combined with search",
    },
    Incompatible {
        peer: Peer::VectorSearch,
        message: "aggregate cannot be combined with vector search",
    },
    Incompatible {
        peer: Peer::HybridSearch,
        message: "aggregate cannot be combined with hybrid search",
    },
];

const PAGINATE_INCOMPATIBLES: &[Incompatible] = &[
    Incompatible {
        peer: Peer::Get,
        message: "paginate cannot be combined with get",
    },
    Incompatible {
        peer: Peer::Count,
        message: "paginate cannot be combined with count",
    },
    Incompatible {
        peer: Peer::Distinct,
        message: "paginate cannot be combined with distinct",
    },
    Incompatible {
        peer: Peer::Aggregate,
        message: "paginate cannot be combined with aggregate",
    },
    Incompatible {
        peer: Peer::Unique,
        message: "paginate cannot be combined with unique",
    },
    Incompatible {
        peer: Peer::First,
        message: "paginate cannot be combined with first",
    },
    Incompatible {
        peer: Peer::Take,
        message: "paginate cannot be combined with take",
    },
];

const VECTOR_SEARCH_PEERS: &[Peer] = &[
    Peer::Index,
    Peer::Eq,
    Peer::Gt,
    Peer::Gte,
    Peer::Lt,
    Peer::Lte,
    Peer::Order,
    Peer::Unique,
    Peer::First,
    Peer::Count,
    Peer::Distinct,
    Peer::Aggregate,
    Peer::Paginate,
    Peer::Filter,
    Peer::Search,
    Peer::Take,
    Peer::HybridSearch,
];
const VECTOR_SEARCH_MESSAGE: &str = "vectorSearch cannot be combined with any other terminal";

const SEARCH_PEERS: &[Peer] = &[
    Peer::Index,
    Peer::Eq,
    Peer::Gt,
    Peer::Gte,
    Peer::Lt,
    Peer::Lte,
    Peer::Order,
    Peer::Unique,
    Peer::First,
    Peer::Count,
    Peer::Distinct,
    Peer::Aggregate,
    Peer::Paginate,
    Peer::Filter,
    Peer::VectorSearch,
    Peer::HybridSearch,
];
const SEARCH_MESSAGE: &str = "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, aggregate, paginate, filter, or vector search";

const HYBRID_SEARCH_PEERS: &[Peer] = &[
    Peer::Index,
    Peer::Eq,
    Peer::Gt,
    Peer::Gte,
    Peer::Lt,
    Peer::Lte,
    Peer::Order,
    Peer::Take,
    Peer::Unique,
    Peer::First,
    Peer::Count,
    Peer::Distinct,
    Peer::Aggregate,
    Peer::Paginate,
    Peer::Filter,
    Peer::Search,
    Peer::VectorSearch,
];
const HYBRID_SEARCH_MESSAGE: &str = "hybridSearch cannot be combined with any other terminal";

/// Result docs = stored doc merged with {"_id", "_creationTime", "_version"}.
/// get: point SELECT, null if missing. unique: error PreconditionFailed "unique query matched
/// multiple documents" if >1 row, null if 0. eq len may be a PREFIX of index fields (0..=all),
/// each typed like Task 5. Sort: unbound index fields in index order, then created_at, then id —
/// all in `order` direction. No index => eq must be empty, sort by (created_at, id).
/// `gt`/`gte`/`lt`/`lte` add an optional inequality bound on the single index field immediately
/// after the `eq` prefix (`index.fields[eq.len()]`): at most one of `gt`/`gte` and at most one of
/// `lt`/`lte` may be set, both may be set together for a bounded range, and the bound value is
/// typed via the same `eq_binds`/`eq_bind_for` conversion `txn.rs` uses for `eq`. A range bound
/// requires an index and a remaining (unconsumed by `eq`) index field -> BadRequest otherwise.
/// `first` is sugar over `take(1)`: applies the same eq/range/order filters with LIMIT 1 and
/// returns `Doc(Some)` (or `Doc(None)` if nothing matched) instead of `Docs`; mutually exclusive
/// with `take` and `unique`.
/// `count` is a terminal that runs `SELECT COUNT(*)` over the same eq/range WHERE clause as every
/// other terminal (no index required, same as collect), skipping ORDER BY/LIMIT entirely and
/// returning `Count(n)` uncapped by `MAX_TAKE`; mutually exclusive with `get`, `take`, `unique`,
/// `first`, and `order` (a count has no rows to order).
/// `distinct` is a terminal that runs `SELECT DISTINCT` over the index field immediately after the
/// `eq` prefix (`index.fields[eq.len()]`) using the same eq/range WHERE clause every other terminal
/// builds, returning `Distinct(values)` — a JSON array of those scalar values, ordered ascending
/// for deterministic output and capped by `MAX_TAKE`. Useful for autocomplete/facet UIs. Requires
/// both an `index` and an index field beyond the eq prefix → BadRequest otherwise. Mutually
/// exclusive with every other terminal except `eq`/range bounds/`filter` (which narrow the
/// matching set the distinct values are drawn from).
/// `aggregate` is a terminal that runs `SUM`/`AVG`/`MIN`/`MAX` over the index field immediately
/// after the `eq` prefix using the same eq/range WHERE clause, returning `Aggregate(value)` — a
/// bare scalar (null if no rows match). With `groupBy: true`, it groups by `index.fields[eq.len()]`
/// and aggregates `index.fields[eq.len()+1]`, returning `AggregateGroups([{key,value},…])`
/// ordered by group key and capped by `MAX_TAKE`. `sum`/`avg` require a numeric aggregate field
/// (only `number` is numeric among indexable types) → BadRequest otherwise. Requires an index AND
/// a field beyond the eq prefix (TWO fields beyond for `groupBy`) → BadRequest otherwise. Mutually
/// exclusive with every other terminal except `eq`/range bounds/`filter` (which narrow the
/// matching set); `take` is also rejected — group count is capped internally by `MAX_TAKE`.
/// Unknown table -> NotFound; unknown index / eq too long / get+query mix / unique+take /
/// first+take / first+unique / count+take / count+unique / count+first / count+order -> BadRequest.
/// `take: 0` is valid and returns an empty `Docs([])`, not an error.
/// `unique` without an `index` scans the whole table (LIMIT 2) and applies the same 0/1/>1 rule.
/// Compile a `Query` into a [`CompiledQuery`] — the SQL string and ordered
/// typed binds the server will execute — plus a list of compile-time warnings
/// (currently: filter on a declared-but-unindexed field). This is the single
/// compile chokepoint for the read path: `execute_query` calls it then binds +
/// fetches the result, and `POST /admin/db/{db}/explain` calls it to show the
/// operator exactly the SQL the server would run (the "no second compiler that
/// drifts" guarantee). Pure — takes the same args as `execute_query` MINUS the
/// `pool`, performs no I/O, and is not async.
///
/// Mirrors `execute_query`'s terminal routing exactly: validate_db_name +
/// authorize_table + table_def resolve → early returns for get/search/vector/
/// hybrid (their compile fns) → compile_query_window → terminal-specific compile
/// (count/distinct/aggregate/paginate/collect). The cascade-order invariants
/// (peer rejection, take cap, range-bound mutual exclusion) are re-checked here
/// so a request that would fail at execute time fails at compile time too — the
/// compile path and the execute path reject the same shapes for the same
/// reasons.
pub fn compile_query(
    db: &str,
    schema: &SchemaDef,
    q: &Query,
    ctx: &PrincipalCtx,
    include_deleted: bool,
) -> Result<(CompiledQuery, Vec<String>), RtDbError> {
    let warnings = collect_filter_warnings(schema, q);
    validate_db_name(db)?;
    authorize_table(ctx, &q.table)?;
    let owner = ctx.user_id.as_deref();
    let table_def = schema.table(&q.table)?;
    let owner_field = table_def.owner_field.as_deref();
    let collaborators_field = table_def.collaborators_field.as_deref();
    let sctx = CompileSearchCtx {
        db,
        table_def,
        table_name: q.table.as_str(),
        owner_field,
        collaborators_field,
        ctx,
        include_deleted,
    };
    let _ = owner;

    if let Some(id) = &q.get {
        reject_if_any_set(q, GET_PEERS, GET_MESSAGE)?;
        return Ok((compile_point_read(&sctx, id)?, warnings));
    }

    if q.unique {
        reject_if_any_set(q, UNIQUE_PEERS, UNIQUE_MESSAGE)?;
    }
    if q.first {
        reject_per_peer_set(q, FIRST_INCOMPATIBLES)?;
    }
    if q.count {
        reject_per_peer_set(q, COUNT_INCOMPATIBLES)?;
    }
    if q.distinct {
        reject_per_peer_set(q, DISTINCT_INCOMPATIBLES)?;
    }
    if q.aggregate.is_some() {
        reject_per_peer_set(q, AGGREGATE_INCOMPATIBLES)?;
    }
    if q.paginate.is_some() {
        reject_per_peer_set(q, PAGINATE_INCOMPATIBLES)?;
    }
    if q.gt.is_some() && q.gte.is_some() {
        return Err(RtDbError::bad_request("gt and gte cannot both be set"));
    }
    if q.lt.is_some() && q.lte.is_some() {
        return Err(RtDbError::bad_request("lt and lte cannot both be set"));
    }
    if let Some(take) = q.take
        && take > MAX_TAKE
    {
        return Err(RtDbError::bad_request(format!(
            "take exceeds maximum of {MAX_TAKE}"
        )));
    }

    if let Some(vs) = &q.vector_search {
        reject_if_any_set(q, VECTOR_SEARCH_PEERS, VECTOR_SEARCH_MESSAGE)?;
        return Ok((compile_vector_search(&sctx, vs)?, warnings));
    }
    if let Some(hs) = &q.hybrid_search {
        reject_if_any_set(q, HYBRID_SEARCH_PEERS, HYBRID_SEARCH_MESSAGE)?;
        return Ok((compile_hybrid_search(&sctx, hs)?, warnings));
    }
    if let Some(search) = &q.search {
        reject_if_any_set(q, SEARCH_PEERS, SEARCH_MESSAGE)?;
        return Ok((compile_search(&sctx, search, q.take)?, warnings));
    }

    let w = compile_query_window(
        table_def,
        q,
        ctx,
        owner,
        owner_field,
        collaborators_field,
        include_deleted,
    )?;

    if q.count {
        return Ok((compile_count_terminal(w, db, &q.table)?, warnings));
    }
    if q.distinct {
        return Ok((compile_distinct_terminal(w, db, &q.table)?, warnings));
    }
    if let Some(agg) = &q.aggregate {
        return Ok((
            compile_aggregate_terminal(w, table_def, agg, db, &q.table)?,
            warnings,
        ));
    }

    let mut sort_cols: Vec<String> = match w.index_def {
        Some(idx) => idx.fields[w.eq_len..]
            .iter()
            .map(|field_name| format!("\"{}\"", pg_col(field_name)))
            .collect(),
        None => Vec::new(),
    };
    sort_cols.push("\"created_at\"".to_string());
    sort_cols.push("\"id\"".to_string());

    let dir = match q.order {
        Some(Order::Desc) => "DESC",
        _ => "ASC",
    };
    let order_by = sort_cols
        .iter()
        .map(|col| format!("{col} {dir}"))
        .collect::<Vec<_>>()
        .join(", ");

    if let Some(paginate) = &q.paginate {
        let (cq, _ctx) = compile_paginate_terminal(
            w, table_def, paginate, sort_cols, dir, &order_by, db, &q.table,
        )?;
        return Ok((cq, warnings));
    }

    Ok((
        compile_collect_terminal(w, q.unique, q.first, q.take, &order_by, db, &q.table)?,
        warnings,
    ))
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
        | FilterExpr::Exists { field } => {
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
            db,
            table_def,
            table_name: q.table.as_str(),
            owner_field,
            collaborators_field,
            ctx,
        };
        match cq.terminal {
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
        }
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

/// A compiled read query: the SQL string (with `$n` placeholders) plus the
/// ordered typed binds to fill them, plus the wire terminal name for telemetry.
/// Produced by [`compile_query`] (and the per-terminal `compile_*_terminal`
/// helpers it dispatches to) and consumed by the matching `execute_*_terminal`
/// helper, which binds `binds` into a sqlx query in order and fetches. The
/// `/explain` admin route also consumes it — the SQL it shows the operator is
/// exactly the string this struct carries, never a second compiler's output.
///
/// `binds` is `Vec<EqBind>` rather than a broader enum because every value a
/// read path binds — eq/range binds, client filter binds, owner/collab uids,
/// `authorize` predicate binds, LIMIT/cursor values, and the search/vector
/// family's text queries, query vectors (as `[a,b,c]` text), and RRF `k` —
/// fits one of the four `EqBind` variants. Encoding them all uniformly is what
/// lets the execute tail be one shared bind loop per terminal.
#[derive(Debug, Clone)]
pub struct CompiledQuery {
    pub sql: String,
    pub binds: Vec<EqBind>,
    pub terminal: &'static str,
}

/// The compiled index window shared by every non-early-return terminal in
/// `execute_query`. Built once by `compile_query_window` and consumed by-value
/// on exactly one terminal's return path. The field set is precisely what the
/// inline terminals read — no speculative fields.
struct QueryWindow<'a> {
    index_def: Option<&'a IndexDef>,
    binds: Vec<EqBind>,
    eq_len: usize,
    range_binds: Vec<EqBind>,
    where_conditions: Vec<String>,
    filter_binds: Vec<EqBind>,
    limit_placeholder: usize,
}

/// Compiles the eq/range binds, the client `filter`, and the owner/collaborator/
/// `authorize` predicates into a `QueryWindow`. Verbatim lift of the block that
/// previously lived inline in `execute_query`; the locals it left behind
/// (`index_def`, `binds`, `eq_len`, `range_binds`, `where_conditions`,
/// `filter_binds`, `limit_placeholder`) became the returned struct's fields.
fn compile_query_window<'a>(
    table_def: &'a TableDef,
    q: &Query,
    ctx: &PrincipalCtx,
    owner: Option<&str>,
    owner_field: Option<&str>,
    collaborators_field: Option<&str>,
    include_deleted: bool,
) -> Result<QueryWindow<'a>, RtDbError> {
    let index_def: Option<&IndexDef> = match &q.index {
        Some(name) => Some(table_def.index(name)?),
        None => {
            if !q.eq.is_empty() {
                return Err(RtDbError::bad_request("eq requires an index"));
            }
            None
        }
    };

    let binds = match index_def {
        Some(idx) => eq_binds(table_def, idx, &q.eq)?,
        None => Vec::new(),
    };
    let eq_len = binds.len();

    let has_range_bound = q.gt.is_some() || q.gte.is_some() || q.lt.is_some() || q.lte.is_some();
    let range_field_name: Option<&str> = if has_range_bound {
        let idx =
            index_def.ok_or_else(|| RtDbError::bad_request("range bound requires an index"))?;
        if eq_len >= idx.fields.len() {
            return Err(RtDbError::bad_request(
                "range bound requires a remaining index field after eq",
            ));
        }
        Some(idx.fields[eq_len].as_str())
    } else {
        None
    };

    let mut range_where: Vec<String> = Vec::new();
    let mut range_binds: Vec<EqBind> = Vec::new();
    if let Some(field_name) = range_field_name {
        let field_type = table_def.fields.get(field_name).ok_or_else(|| {
            RtDbError::internal(format!("index references unknown field '{field_name}'"))
        })?;
        let col = pg_col(field_name);
        if let Some(v) = &q.gt {
            range_where.push(format!("\"{col}\" > ${}", eq_len + range_binds.len() + 1));
            range_binds.push(eq_bind_for(field_type, v)?);
        } else if let Some(v) = &q.gte {
            range_where.push(format!("\"{col}\" >= ${}", eq_len + range_binds.len() + 1));
            range_binds.push(eq_bind_for(field_type, v)?);
        }
        if let Some(v) = &q.lt {
            range_where.push(format!("\"{col}\" < ${}", eq_len + range_binds.len() + 1));
            range_binds.push(eq_bind_for(field_type, v)?);
        } else if let Some(v) = &q.lte {
            range_where.push(format!("\"{col}\" <= ${}", eq_len + range_binds.len() + 1));
            range_binds.push(eq_bind_for(field_type, v)?);
        }
    }
    let mut where_conditions: Vec<String> = match index_def {
        Some(idx) => idx.fields[..eq_len]
            .iter()
            .enumerate()
            .map(|(i, field_name)| format!("\"{}\" = ${}", pg_col(field_name), i + 1))
            .collect(),
        None => Vec::new(),
    };
    where_conditions.extend(range_where);

    // `filter` is an additional WHERE predicate composed after the eq/range
    // conditions. It binds after the eq+range binds, so the LIMIT and cursor
    // placeholder offsets below account for `filter_binds.len()`. When the
    // caller is an authenticated user and the table declares an `ownerField`
    // (and no `collaboratorsField`), `owner_filter` wraps the client filter
    // with a server-side equality predicate so the user sees only their own
    // rows; bypass callers (`None`) and tables without an `ownerField` get the
    // original filter back unchanged. When `collaboratorsField` is also
    // declared, the owner OR collaborator predicate is appended below as a
    // separate where_condition (FilterExpr has no jsonb-array membership leaf).
    let owner_only = owner_field.is_some() && collaborators_field.is_none();
    let effective_filter = if owner_only {
        owner_filter(q.filter.as_ref(), owner_field, owner)
    } else {
        q.filter.as_ref().cloned()
    };
    // Absolute 1-based position of the first client-filter bind; shared by the
    // client filter, the owner/collab uid bind, and the `authorize` predicate
    // fragment below. Each appends to `filter_binds`, so `start_pos +
    // binds.len()` (via `compile_filter_node` / `push_filter_bind`) keeps every
    // placeholder correctly numbered.
    let filter_start = eq_len + range_binds.len() + 1;
    let mut filter_binds: Vec<EqBind> = match &effective_filter {
        Some(filter) => {
            let (fragment, binds) = compile_filter(filter, table_def, filter_start)?;
            where_conditions.push(fragment);
            binds
        }
        None => Vec::new(),
    };
    // Owner OR collaborator predicate appended after the client filter. The
    // schema-validated identifiers are interpolated; the uid is `$n`-bound once
    // and reused on both sides of the OR. Same single-bind shape as owner-only.
    let row_auth_uid = row_auth_enforced_uid(owner_field, collaborators_field, owner);
    if let Some(uid) = row_auth_uid {
        let ph = filter_start + filter_binds.len();
        where_conditions.push(row_auth_predicate_body(
            owner_field,
            collaborators_field,
            ph,
        ));
        filter_binds.push(EqBind::Text(uid.to_string()));
    }
    // `authorize` predicate (model C): when the table declares `authorize` and
    // the caller is a user, AND the resolved predicate into the scan. Composes
    // with owner/collab above (both must pass) — a table may declare both.
    // Bypass callers (`user_id = None`) and tables without `authorize` get no
    // fragment. Shares `filter_binds` so placeholders stay correctly numbered.
    if let Some(frag) = authorize_predicate_body(table_def, ctx, filter_start, &mut filter_binds)? {
        where_conditions.push(frag);
    }
    // FM-33 soft delete: a soft-delete table hides its stamped rows from every
    // read terminal unless the caller passed `include_deleted` (admin docs
    // pass-through only). The literal is bindless, so appending it here does
    // not shift `filter_start`-based placeholder numbering.
    if table_def.soft_delete && !include_deleted {
        where_conditions.push("\"deleted_at\" IS NULL".to_string());
    }
    let limit_placeholder = filter_start + filter_binds.len();

    Ok(QueryWindow {
        index_def,
        binds,
        eq_len,
        range_binds,
        where_conditions,
        filter_binds,
        limit_placeholder,
    })
}

/// `count` terminal SQL compilation: `SELECT COUNT(*)` over the compiled
/// window's WHERE clause. The compile half of the former inline `if q.count`
/// block — SQL and bind-order are byte-for-byte identical to the pre-refactor
/// cascade; only the sqlx bind+fetch tail lives in `execute_count_terminal`.
fn compile_count_terminal(
    w: QueryWindow<'_>,
    db: &str,
    table: &str,
) -> Result<CompiledQuery, RtDbError> {
    let QueryWindow {
        binds,
        range_binds,
        where_conditions,
        filter_binds,
        ..
    } = w;
    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(table);
    let mut sql = format!("SELECT COUNT(*) FROM \"{pg_schema_name}\".\"{table_ident}\"");
    if !where_conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_conditions.join(" AND "));
    }
    let mut all = Vec::with_capacity(binds.len() + range_binds.len() + filter_binds.len());
    all.extend(binds);
    all.extend(range_binds);
    all.extend(filter_binds);
    Ok(CompiledQuery {
        sql,
        binds: all,
        terminal: "count",
    })
}

/// `count` terminal execute tail: bind `CompiledQuery.binds` into a scalar
/// `i64` query in order and fetch the count.
async fn execute_count_terminal(
    cq: CompiledQuery,
    pool: &PgPool,
) -> Result<QueryResult, RtDbError> {
    let CompiledQuery { sql, binds, .. } = cq;
    let mut query = sqlx::query_scalar::<_, i64>(&sql);
    for bind in binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
            EqBind::I64(v) => query.bind(v),
        };
    }
    let count = query.fetch_one(pool).await?;
    Ok(QueryResult::Count(count))
}

/// Distinct terminal SQL compilation: `SELECT DISTINCT` of the index field
/// immediately after the eq prefix over the same eq/range WHERE clause every
/// other terminal builds. The combination cascade already rejected every other
/// terminal; `distinct` composes only with `index`/`eq`/range bounds. The
/// preconditions below reject the no-index and no-remaining-field cases with
/// the same BadRequest shape as a missing-index `eq` bind. Capped by `MAX_TAKE`
/// for parity with `collect` (a distinct set bounded by the matching row count).
/// Compile half of the former inline `if q.distinct { … }` block — SQL and
/// bind-order byte-for-byte identical to the pre-refactor cascade.
fn compile_distinct_terminal(
    w: QueryWindow<'_>,
    db: &str,
    table: &str,
) -> Result<CompiledQuery, RtDbError> {
    let QueryWindow {
        index_def,
        binds,
        eq_len,
        range_binds,
        where_conditions,
        filter_binds,
        limit_placeholder,
    } = w;
    let idx = index_def.ok_or_else(|| {
        RtDbError::bad_request("distinct requires an index field beyond the eq prefix")
    })?;
    if eq_len >= idx.fields.len() {
        return Err(RtDbError::bad_request(
            "distinct requires an index field beyond the eq prefix",
        ));
    }
    let field_name = idx.fields[eq_len].as_str();
    // The field's existence is guaranteed by the schema's index definition
    // (validated at schema push), so no extra lookup is needed here.
    let col = pg_col(field_name);
    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(table);
    // Project the column to jsonb so a single `serde_json::Value` decoder
    // handles text/number/boolean columns uniformly. The physical column
    // name and schema/table identifiers are schema-validated and double-
    // quoted; only the LIMIT is `$n`-bound.
    let mut sql = format!(
        "SELECT DISTINCT to_jsonb(\"{col}\") AS v FROM \"{pg_schema_name}\".\"{table_ident}\""
    );
    if !where_conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_conditions.join(" AND "));
    }
    sql.push_str(&format!(" ORDER BY v LIMIT ${limit_placeholder}"));
    let mut all = Vec::with_capacity(binds.len() + range_binds.len() + filter_binds.len() + 1);
    all.extend(binds);
    all.extend(range_binds);
    all.extend(filter_binds);
    all.push(EqBind::I64(i64::from(MAX_TAKE)));
    Ok(CompiledQuery {
        sql,
        binds: all,
        terminal: "distinct",
    })
}

/// Distinct terminal execute tail: bind + fetch, project rows to `Value`s.
async fn execute_distinct_terminal(
    cq: CompiledQuery,
    pool: &PgPool,
) -> Result<QueryResult, RtDbError> {
    let CompiledQuery { sql, binds, .. } = cq;
    let mut query = sqlx::query_as::<_, (serde_json::Value,)>(&sql);
    for bind in binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
            EqBind::I64(v) => query.bind(v),
        };
    }
    let rows = query.fetch_all(pool).await?;
    let values: Vec<serde_json::Value> = rows.into_iter().map(|(v,)| v).collect();
    Ok(QueryResult::Distinct(values))
}

/// Aggregate terminal SQL compilation: runs `<OP>("<col>")` (SUM/AVG/MIN/MAX)
/// over the same eq/range WHERE clause every other terminal builds, returning
/// one scalar (`Aggregate(value)`). With `group_by: true`, it groups by the
/// index field after the eq prefix and aggregates the one after that, returning
/// `AggregateGroups([{key,value},…])`. The combination cascade already rejected
/// every other terminal; `aggregate` composes only with `index`/`eq`/range
/// bounds/`filter`. The preconditions below reject the no-index,
/// no-remaining-field, and (for sum/avg) non-numeric-field cases. Group count
/// is capped by `MAX_TAKE` for parity with `collect`. Compile half of the
/// former inline `if let Some(agg) = &q.aggregate { … }` block — SQL and
/// bind-order byte-for-byte identical to the pre-refactor cascade.
fn compile_aggregate_terminal(
    w: QueryWindow<'_>,
    table_def: &TableDef,
    agg: &AggregateSpec,
    db: &str,
    table: &str,
) -> Result<CompiledQuery, RtDbError> {
    let QueryWindow {
        index_def,
        binds,
        eq_len,
        range_binds,
        where_conditions,
        filter_binds,
        limit_placeholder,
    } = w;
    // Resolve the group column (groupBy) and the aggregate field. `count`
    // aggregates rows, not a field — it consumes no aggregate index field (a
    // scalar `count` needs no index at all; a grouped `count` needs one index
    // field beyond the eq prefix to group by). Every other op needs an aggregate
    // field: the one after the eq prefix (plain), or the one after the group
    // field (groupBy). The groupcol for groupBy is the same field `distinct` uses.
    let group_col: Option<String> = if agg.group_by {
        let idx = index_def.ok_or_else(|| {
            RtDbError::bad_request("aggregate groupBy requires an index field beyond the eq prefix")
        })?;
        let group_field = idx.fields.get(eq_len).ok_or_else(|| {
            RtDbError::bad_request("aggregate groupBy requires an index field beyond the eq prefix")
        })?;
        Some(pg_col(group_field))
    } else {
        None
    };
    let agg_field_name: Option<&str> = if !agg.op.needs_field() {
        None
    } else {
        let idx = index_def.ok_or_else(|| {
            RtDbError::bad_request("aggregate requires an index field beyond the eq prefix")
        })?;
        let agg_field = if agg.group_by {
            idx.fields.get(eq_len + 1).ok_or_else(|| {
                RtDbError::bad_request(
                    "aggregate groupBy requires two index fields beyond the eq prefix",
                )
            })?
        } else {
            idx.fields.get(eq_len).ok_or_else(|| {
                RtDbError::bad_request("aggregate requires an index field beyond the eq prefix")
            })?
        };
        Some(agg_field.as_str())
    };
    // Validate the aggregate field's schema type and sum/avg's numeric
    // requirement. count/min/max skip the numeric check; count has no field.
    if let Some(name) = agg_field_name {
        let agg_field_type = table_def.fields.get(name).ok_or_else(|| {
            RtDbError::internal(format!("index references unknown field '{name}'"))
        })?;
        if matches!(agg.op, AggregateOp::Sum | AggregateOp::Avg)
            && !is_numeric_index_field(agg_field_type)
        {
            return Err(RtDbError::bad_request(format!(
                "aggregate op {} requires a numeric index field",
                agg.op.sql_fn().to_lowercase()
            )));
        }
    }
    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(table);
    let op_sql = agg.op.sql_fn();
    // The aggregate expression: COUNT(*) for `count` (no column), else
    // OP("agg_col") over the resolved aggregate field.
    let agg_expr = match agg_field_name {
        Some(name) => {
            let agg_col = pg_col(name);
            format!("{op_sql}(\"{agg_col}\")")
        }
        None => "COUNT(*)".to_string(),
    };
    // Project via `to_jsonb` so a single `serde_json::Value` decoder handles
    // text/number/boolean columns uniformly, exactly like `distinct`. A
    // scalar SUM/AVG/MIN/MAX over zero matching rows yields one row with
    // SQL NULL → `serde_json::Value::Null`; COUNT(*) over zero rows yields 0.
    if let Some(group_col) = group_col {
        return compile_aggregate_grouped(
            group_col,
            agg_expr,
            binds,
            range_binds,
            where_conditions,
            filter_binds,
            limit_placeholder,
            &pg_schema_name,
            &table_ident,
        );
    }
    compile_aggregate_scalar(
        agg_expr,
        binds,
        range_binds,
        where_conditions,
        filter_binds,
        &pg_schema_name,
        &table_ident,
    )
}

/// Grouped aggregate SQL compilation:
/// `SELECT to_jsonb(group_col), to_jsonb(OP(agg_col)) … GROUP BY … ORDER BY k LIMIT $`.
/// Verbatim lift of the former grouped branch's SQL builder.
#[allow(clippy::too_many_arguments)]
fn compile_aggregate_grouped(
    group_col: String,
    agg_expr: String,
    binds: Vec<EqBind>,
    range_binds: Vec<EqBind>,
    where_conditions: Vec<String>,
    filter_binds: Vec<EqBind>,
    limit_placeholder: usize,
    pg_schema_name: &str,
    table_ident: &str,
) -> Result<CompiledQuery, RtDbError> {
    let mut sql = format!(
        "SELECT to_jsonb(\"{group_col}\") AS k, to_jsonb({agg_expr}) AS v FROM \"{pg_schema_name}\".\"{table_ident}\""
    );
    if !where_conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_conditions.join(" AND "));
    }
    sql.push_str(&format!(
        " GROUP BY \"{group_col}\" ORDER BY k LIMIT ${limit_placeholder}"
    ));
    let mut all = Vec::with_capacity(binds.len() + range_binds.len() + filter_binds.len() + 1);
    all.extend(binds);
    all.extend(range_binds);
    all.extend(filter_binds);
    all.push(EqBind::I64(i64::from(MAX_TAKE)));
    Ok(CompiledQuery {
        sql,
        binds: all,
        terminal: "aggregate",
    })
}

/// Scalar (ungrouped) aggregate SQL compilation:
/// `SELECT COALESCE(to_jsonb(OP(agg_col)), 'null'::jsonb) …`.
/// Verbatim lift of the former scalar branch's SQL builder.
#[allow(clippy::too_many_arguments)]
fn compile_aggregate_scalar(
    agg_expr: String,
    binds: Vec<EqBind>,
    range_binds: Vec<EqBind>,
    where_conditions: Vec<String>,
    filter_binds: Vec<EqBind>,
    pg_schema_name: &str,
    table_ident: &str,
) -> Result<CompiledQuery, RtDbError> {
    let mut sql = format!(
        "SELECT COALESCE(to_jsonb({agg_expr}), 'null'::jsonb) AS v FROM \"{pg_schema_name}\".\"{table_ident}\""
    );
    if !where_conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_conditions.join(" AND "));
    }
    let mut all = Vec::with_capacity(binds.len() + range_binds.len() + filter_binds.len());
    all.extend(binds);
    all.extend(range_binds);
    all.extend(filter_binds);
    Ok(CompiledQuery {
        sql,
        binds: all,
        terminal: "aggregate",
    })
}

/// Aggregate terminal execute tail: branch on the `terminal` tag, bind, and
/// fetch. The group vs scalar shape difference is encoded by which `query_as`
/// decoder is used; `terminal` disambiguates which to call.
async fn execute_aggregate_terminal(
    cq: CompiledQuery,
    pool: &PgPool,
) -> Result<QueryResult, RtDbError> {
    let CompiledQuery {
        sql,
        binds,
        terminal,
    } = cq;
    // The grouped path is tagged `aggregate` and emits a 2-column row shape
    // (k, v); the scalar path is also tagged `aggregate` but emits 1 column.
    // The compiled SQL itself encodes the shape (its SELECT list), so dispatch
    // on the SQL's projection rather than the tag. Both paths carry the same
    // tag because the wire terminal is the same; the executor reads the SQL.
    if sql.contains("GROUP BY") {
        let mut query = sqlx::query_as::<_, (serde_json::Value, serde_json::Value)>(&sql);
        for bind in binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
                EqBind::I64(v) => query.bind(v),
            };
        }
        let rows = query.fetch_all(pool).await?;
        let groups: Vec<AggregateGroup> = rows
            .into_iter()
            .map(|(k, v)| AggregateGroup { key: k, value: v })
            .collect();
        Ok(QueryResult::AggregateGroups(groups))
    } else {
        let _ = terminal; // shape encoded by SQL; tag unused on this path
        let mut query = sqlx::query_as::<_, (serde_json::Value,)>(&sql);
        for bind in binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
                EqBind::I64(v) => query.bind(v),
            };
        }
        let (v,) = query.fetch_one(pool).await?;
        Ok(QueryResult::Aggregate(v))
    }
}

/// Paginate terminal SQL compilation: keyset-paginated scan over the compiled
/// window's WHERE clause, using `sort_cols` (the unbound index fields +
/// `created_at` + `id`) and `dir`/`order_by` computed by the caller. Fetches
/// one extra row to detect a next page; the cursor encodes the last row's
/// sort-column values. Compile half of the former inline
/// `if let Some(paginate) = &q.paginate { … }` block — SQL and bind-order
/// byte-for-byte identical to the pre-refactor cascade. The `index_def` +
/// `eq_len` are returned alongside the [`CompiledQuery`] so the executor can
/// build the next-page cursor from the last row's projected fields.
#[allow(clippy::too_many_arguments)]
fn compile_paginate_terminal<'a>(
    w: QueryWindow<'a>,
    table_def: &TableDef,
    paginate: &Paginate,
    sort_cols: Vec<String>,
    dir: &str,
    order_by: &str,
    db: &str,
    table: &str,
) -> Result<(CompiledQuery, PaginateExecCtx<'a>), RtDbError> {
    let QueryWindow {
        index_def,
        binds,
        eq_len,
        range_binds,
        mut where_conditions,
        filter_binds,
        ..
    } = w;
    let num_items = paginate.num_items.min(MAX_TAKE);

    // Sort-column types parallel `sort_cols`, for cursor bind typing.
    let sort_col_types: Vec<SortCol> = {
        let mut v: Vec<SortCol> = match index_def {
            Some(idx) => idx.fields[eq_len..]
                .iter()
                .map(|fname| {
                    let ft = table_def.fields.get(fname).ok_or_else(|| {
                        RtDbError::internal(format!("index references unknown field '{fname}'"))
                    })?;
                    Ok(SortCol::IndexField(ft))
                })
                .collect::<Result<Vec<_>, RtDbError>>()?,
            None => Vec::new(),
        };
        v.push(SortCol::CreatedAt);
        v.push(SortCol::Id);
        v
    };

    // Decode the cursor (if any) and append the keyset resume predicate to
    // the eq/range WHERE already built for this query.
    let cursor_start = eq_len + range_binds.len() + filter_binds.len() + 1;
    let cursor_binds: Vec<EqBind> = if let Some(cursor) = &paginate.cursor {
        let cursor_values = decode_cursor(cursor)?;
        if cursor_values.len() != sort_cols.len() {
            return Err(RtDbError::bad_request(format!(
                "cursor has {} value(s) but this query sorts over {} column(s)",
                cursor_values.len(),
                sort_cols.len()
            )));
        }
        let (clause, binds) = build_cursor_conditions(
            &cursor_values,
            &sort_cols,
            &sort_col_types,
            dir,
            cursor_start,
        )?;
        where_conditions.push(clause);
        binds
    } else {
        Vec::new()
    };

    let limit_placeholder = cursor_start + cursor_binds.len();
    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(table);
    let mut sql = format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\""
    );
    if !where_conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_conditions.join(" AND "));
    }
    sql.push_str(" ORDER BY ");
    sql.push_str(order_by);
    sql.push_str(&format!(" LIMIT ${limit_placeholder}"));

    let mut all = Vec::with_capacity(
        binds.len() + range_binds.len() + filter_binds.len() + cursor_binds.len() + 1,
    );
    all.extend(binds);
    all.extend(range_binds);
    all.extend(filter_binds);
    all.extend(cursor_binds);
    // Fetch one extra row so a next page can be detected without a second
    // round-trip; the extra is discarded after the has-next check.
    all.push(EqBind::I64(i64::from(num_items) + 1));

    let cq = CompiledQuery {
        sql,
        binds: all,
        terminal: "paginate",
    };
    Ok((
        cq,
        PaginateExecCtx {
            index_def,
            eq_len,
            num_items,
        },
    ))
}

/// Postgres-fetch context the paginate executor needs to build the next-page
/// cursor after the rows come back. Pure metadata — no SQL of its own.
struct PaginateExecCtx<'a> {
    index_def: Option<&'a IndexDef>,
    eq_len: usize,
    num_items: u32,
}

/// Paginate terminal execute tail: bind + fetch, then build the next-page
/// cursor from the last row's sort-column values.
async fn execute_paginate_terminal(
    cq: CompiledQuery,
    ctx: PaginateExecCtx<'_>,
    pool: &PgPool,
) -> Result<QueryResult, RtDbError> {
    let CompiledQuery { sql, binds, .. } = cq;
    let PaginateExecCtx {
        index_def,
        eq_len,
        num_items,
    } = ctx;
    let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64, i64)>(&sql);
    for bind in binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
            EqBind::I64(v) => query.bind(v),
        };
    }
    let mut rows = query.fetch_all(pool).await?;

    let has_next = rows.len() > num_items as usize;
    if has_next {
        rows.pop();
    }

    // The next cursor is built from the last row of the page (after the
    // extra is discarded); absent when the page is empty or last.
    let next_cursor = if has_next && let Some((last_id, last_doc, last_created_at, _)) = rows.last()
    {
        let mut cursor_values: Vec<serde_json::Value> = Vec::new();
        if let Some(idx) = index_def {
            for fname in &idx.fields[eq_len..] {
                let val = last_doc.get(fname).ok_or_else(|| {
                    RtDbError::internal(format!("stored doc is missing indexed field '{fname}'"))
                })?;
                cursor_values.push(val.clone());
            }
        }
        cursor_values.push(serde_json::json!(*last_created_at));
        cursor_values.push(serde_json::Value::String(last_id.clone()));
        Some(encode_cursor(&cursor_values)?)
    } else {
        None
    };

    let docs = rows
        .into_iter()
        .map(|(id, doc, created_at, version)| merge_doc(id, doc, created_at, version))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(QueryResult::Paginated(PaginatedResult {
        docs,
        next_cursor,
    }))
}

/// `unique`/`first`/`collect` fall-through terminal SQL compilation: scans the
/// compiled window's WHERE clause ordered by `order_by`, applies a `limit`
/// derived from `unique` (2), `first` (1), or `take` (defaulting to `MAX_TAKE`),
/// and shapes the result (`Doc`/`Doc(None)` for unique/first, `Docs` for
/// collect). Compile half of the former inline fall-through block — SQL and
/// bind-order byte-for-byte identical to the pre-refactor cascade.
fn compile_collect_terminal(
    w: QueryWindow<'_>,
    unique: bool,
    first: bool,
    take: Option<u32>,
    order_by: &str,
    db: &str,
    table: &str,
) -> Result<CompiledQuery, RtDbError> {
    let QueryWindow {
        binds,
        range_binds,
        where_conditions,
        filter_binds,
        limit_placeholder,
        ..
    } = w;
    let limit: u32 = if unique {
        2
    } else if first {
        1
    } else {
        take.unwrap_or(MAX_TAKE)
    };

    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(table);
    let mut sql = format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\""
    );
    if !where_conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_conditions.join(" AND "));
    }
    sql.push_str(" ORDER BY ");
    sql.push_str(order_by);
    sql.push_str(&format!(" LIMIT ${limit_placeholder}"));

    let mut all = Vec::with_capacity(binds.len() + range_binds.len() + filter_binds.len() + 1);
    all.extend(binds);
    all.extend(range_binds);
    all.extend(filter_binds);
    all.push(EqBind::I64(i64::from(limit)));
    // The wire terminal name distinguishes the three shapes (unique/first/
    // collect) for the executor's result-shaping switch and for /explain.
    let terminal: &'static str = if unique {
        "unique"
    } else if first {
        "first"
    } else {
        "collect"
    };
    Ok(CompiledQuery {
        sql,
        binds: all,
        terminal,
    })
}

/// `unique`/`first`/`collect` terminal execute tail: bind + fetch, then shape
/// per the wire `terminal` tag (`unique`/`first` → `Doc`, `collect` → `Docs`).
async fn execute_collect_terminal(
    cq: CompiledQuery,
    pool: &PgPool,
) -> Result<QueryResult, RtDbError> {
    let CompiledQuery {
        sql,
        binds,
        terminal,
    } = cq;
    let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64, i64)>(&sql);
    for bind in binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
            EqBind::I64(v) => query.bind(v),
        };
    }
    let mut rows = query.fetch_all(pool).await?;

    if terminal == "unique" {
        if rows.len() > 1 {
            return Err(RtDbError::precondition(
                "unique query matched multiple documents",
            ));
        }
        return match rows.pop() {
            Some((id, doc, created_at, version)) => Ok(QueryResult::Doc(Some(merge_doc(
                id, doc, created_at, version,
            )?))),
            None => Ok(QueryResult::Doc(None)),
        };
    }

    if terminal == "first" {
        return match rows.pop() {
            Some((id, doc, created_at, version)) => Ok(QueryResult::Doc(Some(merge_doc(
                id, doc, created_at, version,
            )?))),
            None => Ok(QueryResult::Doc(None)),
        };
    }

    let docs = rows
        .into_iter()
        .map(|(id, doc, created_at, version)| merge_doc(id, doc, created_at, version))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(QueryResult::Docs(docs))
}

/// A sort column's nature, used to type cursor binds. The sort order is always
/// the unbound index fields (those after the `eq` prefix) followed by
/// `created_at` then `id`.
enum SortCol<'a> {
    /// An unbound indexed user field — typed via its declared `FieldType`, so
    /// cursor binds reuse the same `eq_bind_for` path as `eq` prefixes.
    IndexField(&'a FieldType),
    /// `created_at` column — stored as `bigint`. The cursor value is bound as
    /// float8 and cast to bigint in the SQL (`$n::bigint`) so the comparison
    /// is integer-vs-integer regardless of the float8 wire type.
    CreatedAt,
    /// `id` column — stored as `text`.
    Id,
}

impl SortCol<'_> {
    /// Returns the SQL placeholder (with cast when needed) and the typed bind
    /// for one cursor value at 1-based position `pos`.
    fn cursor_bind(
        &self,
        value: &serde_json::Value,
        pos: usize,
    ) -> Result<(String, EqBind), RtDbError> {
        match self {
            SortCol::IndexField(ft) => Ok((format!("${pos}"), eq_bind_for(ft, value)?)),
            SortCol::CreatedAt => {
                let n = value.as_f64().ok_or_else(|| {
                    RtDbError::bad_request("cursor value for created_at must be a number")
                })?;
                Ok((format!("${pos}::bigint"), EqBind::Num(n)))
            }
            SortCol::Id => {
                let s = value.as_str().ok_or_else(|| {
                    RtDbError::bad_request("cursor value for id must be a string")
                })?;
                Ok((format!("${pos}"), EqBind::Text(s.to_string())))
            }
        }
    }
}

/// Builds the keyset-pagination resume predicate for a cursor over `sort_cols`
/// in direction `dir` ("ASC"/"DESC"). The cursor stores one value per sort
/// column, in order; the predicate is the standard row-value comparison
/// expanded to OR-of-AND:
///
/// ```text
/// (c0 OP v0)
///   OR (c0 = v0 AND c1 OP v1)
///   OR ...
///   OR (c0 = v0 AND ... AND cN-1 = vN-1 AND cN OP vN)
/// ```
///
/// where OP is `>` (ASC) or `<` (DESC). Because `id` is always the final sort
/// column and is globally unique, this fully determines a stable order — no
/// row is skipped or duplicated across pages. `next_bind_idx` is the 1-based
/// position of the first new bind. Returns the fully parenthesized predicate
/// and the binds in placeholder order.
fn build_cursor_conditions(
    cursor_values: &[serde_json::Value],
    sort_cols: &[String],
    sort_col_types: &[SortCol<'_>],
    dir: &str,
    next_bind_idx: usize,
) -> Result<(String, Vec<EqBind>), RtDbError> {
    let op = if dir == "DESC" { "<" } else { ">" };
    let mut binds: Vec<EqBind> = Vec::new();
    let mut branches: Vec<String> = Vec::new();
    for i in 0..sort_cols.len() {
        let mut conjuncts: Vec<String> = Vec::new();
        for j in 0..=i {
            let pos = next_bind_idx + binds.len();
            let (placeholder, bind) = sort_col_types[j].cursor_bind(&cursor_values[j], pos)?;
            let cmp = if j < i { "=" } else { op };
            conjuncts.push(format!("{} {cmp} {placeholder}", sort_cols[j]));
            binds.push(bind);
        }
        branches.push(conjuncts.join(" AND "));
    }
    Ok((format!("({})", branches.join(" OR ")), binds))
}

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
        let (fragment, filter_binds) = compile_filter(f, table_def, 1)?;
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
pub(crate) fn compile_filter(
    filter: &FilterExpr,
    table: &TableDef,
    start_pos: usize,
) -> Result<(String, Vec<EqBind>), RtDbError> {
    // SEC-125: validate at the single compilation chokepoint so EVERY client
    // filter entry point (q.filter, compile_scan_where's client filter,
    // search.filter, vectorSearch.filter) gets the Contains-array and
    // orderable-field guards. `allow_principal_markers = false` is correct for
    // all of these — the authorize predicate is the only path that permits
    // principal markers, and it bypasses `compile_filter` (compiling via
    // `compile_filter_node` directly after `resolve_predicate_markers`).
    validate_filter_expr_fields(filter, table, false)
        .map_err(|e| RtDbError::bad_request(e.message))?;
    let mut binds: Vec<EqBind> = Vec::new();
    let sql = compile_filter_node(filter, table, start_pos, &mut binds)?;
    Ok((sql, binds))
}

fn compile_filter_node(
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

/// `websearch_to_tsquery` SQL fragment honoring an optional search-index
/// `language` (a `schema::validate_structure`-checked regconfig name
/// interpolated as a literal). The query text stays a `$ph` bind, so user
/// input can never inject tsquery syntax or escape the regconfig literal;
/// `websearch_to_tsquery` additionally honors quoted phrases, the bare word
/// `or`, and `-term` negation in the bound text (FM-31) — a pure superset of
/// the former `plainto_tsquery` for plain terms. The tsvector column and the
/// tsquery must share a regconfig for `@@` to match correctly, so every search
/// path builds both from the same index `language`.
fn websearch_tsquery_sql(language: Option<&str>, ph: usize) -> String {
    match language {
        Some(lang) => format!("websearch_to_tsquery('{lang}'::regconfig, ${ph})"),
        None => format!("websearch_to_tsquery(${ph})"),
    }
}

/// Full-text search terminal: matches a search index's generated tsvector
/// against `websearch_to_tsquery(<query text>)` and ranks by `ts_rank` descending,
/// with `(created_at, id)` tie-breakers. Composes with `take` (defaulting to
/// `MAX_TAKE`); the caller has already rejected every other terminal. The query
/// text is bound once via `$1` and reused in the `ORDER BY ts_rank`, so user
/// text can never inject tsquery syntax. Unknown index / empty query →
/// `BadRequest`, never a 500.
/// Shared, borrow-only context for the read-path terminals that don't use the
/// btree `QueryWindow` — `point_read` and the `search`/`vectorSearch`/
/// `hybridSearch` family. Those four terminals share the same caller-resolved
/// inputs (pool, db, the resolved table definition, the table name, and the
/// per-row-auth fields derived from it, plus the principal), so `SearchCtx`
/// bundles them the way `QueryWindow` bundles the index-window inputs and
/// `StepCtx` (`txn.rs`) bundles the write-path inputs. Borrow-only: every
/// field is a shared reference (these terminals read, never mutate), so it
/// threads through `&SearchCtx` cleanly. QA-105.
/// Shared borrow-only context for the four read-path terminals that don't
/// use the btree `QueryWindow` (point_read + the search family). Constructed
/// once from the caller-resolved inputs and passed by shared reference; each
/// terminal destructures it back into the same locals the inline body used.
struct SearchCtx<'a> {
    pool: &'a PgPool,
    // `db` and `table_name` are read by the compile fns via `CompileSearchCtx`;
    // they remain on this struct for symmetry and for any future execute tail
    // that needs them (point_read today reads only pool + the auth helpers).
    #[allow(dead_code)]
    db: &'a str,
    table_def: &'a TableDef,
    #[allow(dead_code)]
    table_name: &'a str,
    owner_field: Option<&'a str>,
    collaborators_field: Option<&'a str>,
    ctx: &'a PrincipalCtx,
}

/// Compile-only view of [`SearchCtx`] — every field EXCEPT `pool`. The compile
/// fns are pure (no I/O), so they take this smaller context; the execute tails
/// take the pool directly. The fields here are the same names/types as
/// `SearchCtx`'s, so the compile bodies read identically to the pre-refactor
/// inline bodies.
struct CompileSearchCtx<'a> {
    db: &'a str,
    table_def: &'a TableDef,
    table_name: &'a str,
    owner_field: Option<&'a str>,
    collaborators_field: Option<&'a str>,
    ctx: &'a PrincipalCtx,
    /// FM-33: when `true` (admin `includeDeleted` pass-through), the
    /// soft-delete literal is NOT composed — soft-deleted rows surface.
    include_deleted: bool,
}

/// Full-text search terminal SQL compilation. Compile half of the former
/// inline `execute_search` body. Bind order: `$1` is the search query text;
/// in `trgm` mode `$2` is the server-built `'%…%'` ILIKE pattern (so
/// downstream placeholders shift by one); the optional client `filter`
/// compiles next, then the per-row owner/collaborator and `authorize`
/// predicates, then `LIMIT`. The search text/pattern and the limit are
/// encoded as `EqBind::Text` / `EqBind::I64` respectively so the executor
/// uses the same bind loop every other terminal does. `snippet: true`
/// (tsquery mode only) appends one trailing `ts_headline` column to the
/// SELECT, reusing `$1` — bind order and count are unchanged.
fn compile_search(
    sctx: &CompileSearchCtx<'_>,
    search: &SearchQuery,
    take: Option<u32>,
) -> Result<CompiledQuery, RtDbError> {
    let db = sctx.db;
    let table_def = sctx.table_def;
    let table_name = sctx.table_name;
    let owner_field = sctx.owner_field;
    let collaborators_field = sctx.collaborators_field;
    let ctx = sctx.ctx;
    if search.query.trim().is_empty() {
        return Err(RtDbError::bad_request(
            "search query text must not be empty",
        ));
    }
    let owner = ctx.user_id.as_deref();
    let index_def = table_def
        .indexes
        .iter()
        .find(|index| index.name == search.index && index.search)
        .ok_or_else(|| {
            RtDbError::bad_request(format!("search index '{}' not found", search.index))
        })?;
    let sv_col = pg_search_col(&index_def.name);
    let tsq = websearch_tsquery_sql(index_def.language.as_deref(), 1);
    let limit = take.unwrap_or(MAX_TAKE);
    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(table_name);
    let mode = search.mode.unwrap_or_default();
    // `snippet` needs a tsquery tree to highlight; trgm mode matches raw
    // substrings, so the combination is rejected rather than silently
    // ignored.
    let snippet = search.snippet.unwrap_or(false);
    if snippet && mode == SearchMode::Trgm {
        return Err(RtDbError::bad_request(
            "snippet is only supported in tsquery mode",
        ));
    }
    // The headline renders from the same `websearch_to_tsquery($1)` the WHERE
    // matched, so a snippet highlights exactly why the doc is a hit — phrases
    // included. Source text is the index's fields in declared order
    // (`concat_ws` skips NULL columns; a doc that matched the tsvector
    // necessarily carries text in at least one of them). Options are the
    // `SNIPPET_HEADLINE_OPTS` server constant, never client-supplied.
    let snippet_col = if snippet {
        let src = index_def
            .fields
            .iter()
            .map(|field_name| format!("\"{}\"", pg_col(field_name)))
            .collect::<Vec<_>>()
            .join(", ");
        let cfg = match index_def.language.as_deref() {
            Some(lang) => format!("'{lang}'::regconfig, "),
            None => String::new(),
        };
        format!(", ts_headline({cfg}concat_ws(' ', {src}), {tsq}, '{SNIPPET_HEADLINE_OPTS}')")
    } else {
        String::new()
    };

    // `$1` is the search query text in both modes (the tsquery in `tsquery`
    // mode, the similarity argument in `trgm` mode). The optional client
    // `filter` compiles next, then the per-row owner/collaborator and
    // `authorize` predicates, then `LIMIT` — the same compose order
    // `compile_scan_where` uses for reads. One shared accumulator keeps
    // `compile_filter_node`'s `start_pos + binds.len()` placeholders correctly
    // numbered. Schema-validated identifiers are interpolated; every value is
    // `$n`-bound. With no filter and an owner-only table this emits the
    // single-predicate form byte-identical to the pre-filter SQL.
    let mut binds: Vec<EqBind> = Vec::new();
    let mut extra = String::new();
    let start = match mode {
        SearchMode::Tsquery => 2usize,
        SearchMode::Trgm => 3,
    };
    if let Some(filter) = &search.filter {
        let (fragment, filter_binds) = compile_filter(filter, table_def, start)?;
        binds.extend(filter_binds);
        extra.push_str(" AND (");
        extra.push_str(&fragment);
        extra.push(')');
    }
    let enforced_uid = row_auth_enforced_uid(owner_field, collaborators_field, owner);
    if let Some(uid) = enforced_uid {
        let ph = start + binds.len();
        extra.push_str(" AND ");
        extra.push_str(&row_auth_predicate_body(
            owner_field,
            collaborators_field,
            ph,
        ));
        binds.push(EqBind::Text(uid.to_string()));
    }
    if let Some(frag) = authorize_predicate_body(table_def, ctx, start, &mut binds)? {
        extra.push_str(" AND ");
        extra.push_str(&frag);
    }
    // FM-33: hide soft-deleted rows from ranked search unless the admin
    // `includeDeleted` pass-through is set. Bindless literal — `limit_ph`
    // below is unaffected.
    if table_def.soft_delete && !sctx.include_deleted {
        extra.push_str(" AND \"deleted_at\" IS NULL");
    }
    let limit_ph = start + binds.len();
    let sql = match mode {
        SearchMode::Tsquery => format!(
            "SELECT \"id\", \"doc\", \"created_at\", \"version\"{snippet_col} FROM \"{pg_schema_name}\".\"{table_ident}\" \
             WHERE \"{sv_col}\" @@ {tsq}{extra} \
             ORDER BY ts_rank(\"{sv_col}\", {tsq}) DESC, \"created_at\" DESC, \"id\" DESC \
             LIMIT ${limit_ph}"
        ),
        // Trgm (FM-30): substring match over the index's text `f_` columns.
        // `$1` (raw query text) feeds `similarity`; `$2` is the server-built
        // `'%' || query || '%'` ILIKE pattern, bound — never interpolated. A
        // result row ILIKE-matched some field, so at least one `similarity`
        // argument is non-NULL and GREATEST is well-defined. The `created_at`/
        // `id` tiebreaks keep ordering deterministic like the tsquery arm.
        SearchMode::Trgm => {
            let ilike = index_def
                .fields
                .iter()
                .map(|field_name| format!("\"{}\" ILIKE $2", pg_col(field_name)))
                .collect::<Vec<_>>()
                .join(" OR ");
            let sim = index_def
                .fields
                .iter()
                .map(|field_name| format!("similarity(\"{}\", $1)", pg_col(field_name)))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\" \
                 WHERE ({ilike}){extra} \
                 ORDER BY GREATEST({sim}) DESC, \"created_at\" DESC, \"id\" DESC \
                 LIMIT ${limit_ph}"
            )
        }
    };
    // The leading bind(s) are the search text (and, in trgm mode, its
    // `%…%` ILIKE pattern); the trailing bind is the LIMIT. All are folded
    // into the same Vec<EqBind> the executor drains in order.
    let mut all = Vec::with_capacity(start - 1 + binds.len() + 1);
    all.push(EqBind::Text(search.query.clone()));
    if mode == SearchMode::Trgm {
        all.push(EqBind::Text(format!("%{}%", search.query)));
    }
    all.extend(binds);
    all.push(EqBind::I64(i64::from(limit)));
    Ok(CompiledQuery {
        sql,
        binds: all,
        terminal: "search",
    })
}

/// Execute tail for the `search` terminal. Two row shapes, selected by the
/// caller-re-derived `snippet` flag: the 4-column default, or a 5-column
/// form whose trailing `ts_headline` render becomes each doc's
/// `_searchSnippet` field (inserted after `merge_doc` — write-time
/// validation rejects `_`-prefixed keys, so the additive field never
/// collides with stored data).
async fn execute_search(
    cq: CompiledQuery,
    pool: &PgPool,
    snippet: bool,
) -> Result<QueryResult, RtDbError> {
    let CompiledQuery { sql, binds, .. } = cq;
    if snippet {
        let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64, i64, String)>(&sql);
        for bind in binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
                EqBind::I64(v) => query.bind(v),
            };
        }
        let rows = query.fetch_all(pool).await?;
        let docs = rows
            .into_iter()
            .map(|(id, doc, created_at, version, snippet_text)| {
                let mut merged = merge_doc(id, doc, created_at, version)?;
                merged["_searchSnippet"] = serde_json::Value::String(snippet_text);
                Ok::<_, RtDbError>(merged)
            })
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(QueryResult::Docs(docs));
    }
    let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64, i64)>(&sql);
    for bind in binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
            EqBind::I64(v) => query.bind(v),
        };
    }
    let rows = query.fetch_all(pool).await?;
    let docs = rows
        .into_iter()
        .map(|(id, doc, created_at, version)| merge_doc(id, doc, created_at, version))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(QueryResult::Docs(docs))
}

/// Vector-similarity terminal SQL compilation: ranks rows by the index's
/// declared metric distance (`<=>`/`<->`/`<#>` for cosine/l2/ip) between the
/// index's `v_<index>` column and the query vector, ascending, limited to
/// `limit`. Optional `filter` eq-binds over the index's declared `filterFields`.
/// Unknown index / length mismatch / unknown filter key / out-of-range limit
/// → `BadRequest`. Bind order: filter eq-binds occupy `$1..$k`, then (when the
/// table is owner-gated and the caller is a user) the owner id occupies
/// `$(k+1)`, then the query vector (`$n::vector`), then `limit`. Compile half
/// of the former inline `execute_vector_search` body — SQL and bind-order
/// byte-for-byte identical to the pre-refactor cascade; the query vector (in
/// its `[a,b,c]` text form) and the limit are encoded as `EqBind::Text` and
/// `EqBind::I64` so the executor uses the same bind loop every other terminal
/// does.
fn compile_vector_search(
    sctx: &CompileSearchCtx<'_>,
    vs: &VectorSearchQuery,
) -> Result<CompiledQuery, RtDbError> {
    let db = sctx.db;
    let table_def = sctx.table_def;
    let table_name = sctx.table_name;
    let owner_field = sctx.owner_field;
    let collaborators_field = sctx.collaborators_field;
    let ctx = sctx.ctx;
    let index_def = table_def
        .indexes
        .iter()
        .find(|index| index.name == vs.index && index.vector.is_some())
        .ok_or_else(|| RtDbError::bad_request(format!("vector index '{}' not found", vs.index)))?;
    // Unreachable given the find predicate above, but keep the error path
    // explicit instead of panicking on a future predicate change.
    let vec_spec = index_def
        .vector
        .as_ref()
        .ok_or_else(|| RtDbError::internal("matched vector index has no vector spec"))?;

    if vs.vector.len() != vec_spec.dimensions as usize {
        return Err(RtDbError::bad_request(format!(
            "vectorSearch vector length {} != index '{}' dimensions {}",
            vs.vector.len(),
            vs.index,
            vec_spec.dimensions
        )));
    }
    // serde_json can't carry NaN/Infinity, but a Rust-constructed query can —
    // reject before binding so pgvector never sees a non-finite value (which
    // would surface as a 500 instead of a clean BadRequest).
    if !vs.vector.iter().all(|v| v.is_finite()) {
        return Err(RtDbError::bad_request(
            "vectorSearch query vector must contain only finite numbers",
        ));
    }
    if !(1..=VECTOR_SEARCH_MAX_LIMIT).contains(&vs.limit) {
        return Err(RtDbError::bad_request(format!(
            "vectorSearch limit must be 1..={VECTOR_SEARCH_MAX_LIMIT}"
        )));
    }

    let v_col = pg_vector_col(&index_def.name);
    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(table_name);

    // pgvector accepts the text form `[a,b,c]` for a `::vector`-cast bind.
    let qvec_text = format!(
        "[{}]",
        vs.vector
            .iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );

    // Bind numbering: the optional client `filter` (full `FilterExpr`, reusing
    // `compile_filter`) compiles first at `$1`, then the per-row owner/
    // collaborator uid and the `authorize` predicate, then the query vector
    // (cast to `vector`), then `limit` — the same compose order `execute_search`
    // uses. One shared accumulator keeps `compile_filter_node`'s
    // `start_pos + binds.len()` placeholders correctly numbered. The WHERE always
    // excludes rows whose vector column is NULL, so undimensioned rows never
    // surface as bogus nearest-neighbors. Filtering is no longer restricted to
    // declared `filterFields` (any field works — typed column when indexed, jsonb
    // extraction otherwise); declared filterFields still create indexed columns.
    let owner = ctx.user_id.as_deref();
    let enforced_uid = row_auth_enforced_uid(owner_field, collaborators_field, owner);
    let mut binds: Vec<EqBind> = Vec::new();
    let mut extra = String::new();
    let start = 1usize;
    if let Some(filter) = &vs.filter {
        let (fragment, filter_binds) = compile_filter(filter, table_def, start)?;
        binds.extend(filter_binds);
        extra.push_str(" AND (");
        extra.push_str(&fragment);
        extra.push(')');
    }
    if let Some(uid) = enforced_uid {
        let ph = start + binds.len();
        extra.push_str(" AND ");
        extra.push_str(&row_auth_predicate_body(
            owner_field,
            collaborators_field,
            ph,
        ));
        binds.push(EqBind::Text(uid.to_string()));
    }
    if let Some(frag) = authorize_predicate_body(table_def, ctx, start, &mut binds)? {
        extra.push_str(" AND ");
        extra.push_str(&frag);
    }
    // FM-33: hide soft-deleted rows from vector ranking unless the admin
    // `includeDeleted` pass-through is set. Bindless literal — `qvec_ph`/`
    // `limit_ph` below are unaffected.
    if table_def.soft_delete && !sctx.include_deleted {
        extra.push_str(" AND \"deleted_at\" IS NULL");
    }
    let qvec_ph = start + binds.len();
    let limit_ph = qvec_ph + 1;

    let dist_op = vec_spec.metric.distance_op();
    let where_clause = format!("\"{v_col}\" IS NOT NULL{extra}");
    let sql = format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\" \
         WHERE {where_clause} \
         ORDER BY \"{v_col}\" {dist_op} ${qvec_ph}::vector \
         LIMIT ${limit_ph}"
    );

    let mut all = Vec::with_capacity(binds.len() + 2);
    all.extend(binds);
    all.push(EqBind::Text(qvec_text));
    all.push(EqBind::I64(i64::from(vs.limit)));
    Ok(CompiledQuery {
        sql,
        binds: all,
        terminal: "vectorSearch",
    })
}

async fn execute_vector_search(cq: CompiledQuery, pool: &PgPool) -> Result<QueryResult, RtDbError> {
    let CompiledQuery { sql, binds, .. } = cq;
    let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64, i64)>(&sql);
    for bind in binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
            EqBind::I64(v) => query.bind(v),
        };
    }
    let rows = query.fetch_all(pool).await?;
    let docs = rows
        .into_iter()
        .map(|(id, doc, created_at, version)| merge_doc(id, doc, created_at, version))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(QueryResult::Docs(docs))
}

/// Hybrid search terminal SQL compilation: fuses full-text (`ts_rank`) and
/// vector ranking (the resolved vector index's metric operator) over the same
/// table via RRF. The table must declare BOTH a search index (tsvector) AND a
/// vector index; if either is missing → `BadRequest`. The candidate set is the
/// UNION of rows matching `websearch_to_tsquery($text)` and rows with a non-null
/// vector; both rankings are computed in a single statement with window
/// functions and fused as `1/(k + r_text) + 1/(k + r_vec)` (default `k = 60`).
/// Bind order: the text query (`$1`, referenced in WHERE/ts_rank), the owner id
/// (`$2`) when owner-enforced, the query vector (`$n::vector`), the RRF constant
/// `k`, then `limit`. Column names come from `pg_search_col`/`pg_vector_col`
/// over the resolved indexes (auto-selected when not named); all identifiers
/// are schema-validated and double-quoted; every value is `$n`-bound. Compile
/// half of the former inline `execute_hybrid_search` body — SQL and bind-order
/// byte-for-byte identical to the pre-refactor cascade; the text query, query
/// vector (as `[a,b,c]` text), RRF `k`, and limit are all folded into the
/// single `binds: Vec<EqBind>` the executor drains in order.
fn compile_hybrid_search(
    sctx: &CompileSearchCtx<'_>,
    hs: &HybridSearchQuery,
) -> Result<CompiledQuery, RtDbError> {
    let db = sctx.db;
    let table_def = sctx.table_def;
    let table_name = sctx.table_name;
    let owner_field = sctx.owner_field;
    let collaborators_field = sctx.collaborators_field;
    let ctx = sctx.ctx;
    if hs.query.trim().is_empty() {
        return Err(RtDbError::bad_request(
            "hybrid search query text must not be empty",
        ));
    }

    // Resolve the search index (named or first search index on the table).
    let search_index = match &hs.search_index {
        Some(name) => table_def
            .indexes
            .iter()
            .find(|index| index.name == name.as_str() && index.search)
            .ok_or_else(|| RtDbError::bad_request(format!("search index '{}' not found", name)))?,
        None => table_def
            .indexes
            .iter()
            .find(|index| index.search)
            .ok_or_else(|| {
                RtDbError::bad_request(
                    "hybrid search requires both a search index and a vector index on the table",
                )
            })?,
    };
    // Resolve the vector index (named or first vector index on the table).
    let vector_index = match &hs.vector_index {
        Some(name) => table_def
            .indexes
            .iter()
            .find(|index| index.name == name.as_str() && index.vector.is_some())
            .ok_or_else(|| RtDbError::bad_request(format!("vector index '{}' not found", name)))?,
        None => table_def
            .indexes
            .iter()
            .find(|index| index.vector.is_some())
            .ok_or_else(|| {
                RtDbError::bad_request(
                    "hybrid search requires both a search index and a vector index on the table",
                )
            })?,
    };
    let vec_spec = vector_index
        .vector
        .as_ref()
        .ok_or_else(|| RtDbError::internal("matched vector index has no vector spec"))?;

    // Validate the query vector against the index dimensions and finiteness
    // (mirrors `execute_vector_search` so pgvector never sees a bad vector).
    if hs.vector.len() != vec_spec.dimensions as usize {
        return Err(RtDbError::bad_request(format!(
            "hybridSearch vector length {} != vector index '{}' dimensions {}",
            hs.vector.len(),
            vector_index.name,
            vec_spec.dimensions
        )));
    }
    if !hs.vector.iter().all(|v| v.is_finite()) {
        return Err(RtDbError::bad_request(
            "hybridSearch query vector must contain only finite numbers",
        ));
    }
    if hs.limit > MAX_TAKE {
        return Err(RtDbError::bad_request(format!(
            "hybridSearch limit exceeds maximum of {MAX_TAKE}"
        )));
    }

    let sv_col = pg_search_col(&search_index.name);
    let v_col = pg_vector_col(&vector_index.name);
    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(table_name);
    // RRF constant (default 60). `k + r` never divides by zero because
    // ROW_NUMBER starts at 1; k=0 is therefore safe but unusual.
    let k = i64::from(hs.k.unwrap_or(60));

    // pgvector accepts the text form `[a,b,c]` for a `::vector`-cast bind.
    let qvec_text = format!(
        "[{}]",
        hs.vector
            .iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );

    // Bind numbering: text query ($1, referenced in WHERE and ts_rank), the
    // per-row auth uid ($2) when owner/collaborators-enforced, the `authorize`
    // predicate's binds (when declared + user caller), then the query vector
    // (cast to `vector`), the RRF constant `k`, and `limit`. The per-row auth
    // predicate interpolates only the schema-validated `ownerField` /
    // `collaboratorsField` into jsonb string-literal positions; the uid is
    // `$n`-bound once. Owner uid and `authorize` binds share one accumulator
    // numbered from `auth_start` so `compile_filter_node`'s `start_pos +
    // binds.len()` rule keeps placeholders correct.
    let owner = ctx.user_id.as_deref();
    let enforced_uid = row_auth_enforced_uid(owner_field, collaborators_field, owner);
    let text_ph = 1usize;
    let tsq = websearch_tsquery_sql(search_index.language.as_deref(), text_ph);
    let mut auth_binds: Vec<EqBind> = Vec::new();
    let mut auth_clause = String::new();
    let auth_start = 2usize;
    if let Some(uid) = enforced_uid {
        let ph = auth_start + auth_binds.len();
        auth_clause.push_str(" AND ");
        auth_clause.push_str(&row_auth_predicate_body(
            owner_field,
            collaborators_field,
            ph,
        ));
        auth_binds.push(EqBind::Text(uid.to_string()));
    }
    if let Some(frag) = authorize_predicate_body(table_def, ctx, auth_start, &mut auth_binds)? {
        auth_clause.push_str(" AND ");
        auth_clause.push_str(&frag);
    }
    // FM-33: hide soft-deleted rows from hybrid ranking unless the admin
    // `includeDeleted` pass-through is set. Bindless literal — `qvec_ph`/`
    // `k_ph`/`limit_ph` below are unaffected.
    if table_def.soft_delete && !sctx.include_deleted {
        auth_clause.push_str(" AND \"deleted_at\" IS NULL");
    }
    let qvec_ph = auth_start + auth_binds.len();
    let k_ph = qvec_ph + 1;
    let limit_ph = k_ph + 1;

    // RRF over the union of text matches and vector-bearing rows. Rows that
    // don't match the text get ts_rank = 0 (ranked last on the text axis); rows
    // with a null vector get dist NULL (ranked last on the vector axis via NULLS
    // LAST). The final ORDER BY tie-breakers (created_at, id) keep output
    // deterministic when RRF scores collide.
    let dist_op = vec_spec.metric.distance_op();
    let sql = format!(
        "WITH matched AS ( \
           SELECT \"id\", \"doc\", \"created_at\", \"version\", \
                  ts_rank(\"{sv_col}\", {tsq}) AS trank, \
                  (\"{v_col}\" {dist_op} ${qvec_ph}::vector) AS dist \
           FROM \"{pg_schema_name}\".\"{table_ident}\" \
           WHERE (\"{sv_col}\" @@ {tsq} OR \"{v_col}\" IS NOT NULL){auth_clause} \
         ), ranked AS ( \
           SELECT \"id\", \"doc\", \"created_at\", \"version\", \
                  ROW_NUMBER() OVER (ORDER BY trank DESC, \"created_at\" DESC, \"id\" DESC) AS r_text, \
                  ROW_NUMBER() OVER (ORDER BY dist ASC NULLS LAST, \"created_at\" DESC, \"id\" DESC) AS r_vec \
           FROM matched \
         ) \
         SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM ranked \
         ORDER BY (1.0/(${k_ph} + r_text) + 1.0/(${k_ph} + r_vec)) DESC, \"created_at\" DESC, \"id\" DESC \
         LIMIT ${limit_ph}"
    );

    let mut all = Vec::with_capacity(1 + auth_binds.len() + 3);
    all.push(EqBind::Text(hs.query.clone()));
    all.extend(auth_binds);
    all.push(EqBind::Text(qvec_text));
    all.push(EqBind::I64(k));
    all.push(EqBind::I64(i64::from(hs.limit)));
    Ok(CompiledQuery {
        sql,
        binds: all,
        terminal: "hybridSearch",
    })
}

async fn execute_hybrid_search(cq: CompiledQuery, pool: &PgPool) -> Result<QueryResult, RtDbError> {
    let CompiledQuery { sql, binds, .. } = cq;
    let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64, i64)>(&sql);
    for bind in binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
            EqBind::I64(v) => query.bind(v),
        };
    }
    let rows = query.fetch_all(pool).await?;
    let docs = rows
        .into_iter()
        .map(|(id, doc, created_at, version)| merge_doc(id, doc, created_at, version))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(QueryResult::Docs(docs))
}

/// `get` (point read) terminal SQL compilation. Compile half of the former
/// inline `point_read` body — SQL byte-for-byte identical to the pre-refactor
/// cascade. The id bind is encoded as `EqBind::Text` so the executor uses the
/// same bind loop every other terminal does. Per-row owner/collaborator and
/// `authorize` filtering happen post-fetch in the executor (silent filter —
/// Convex-like), so neither appears in the SQL.
fn compile_point_read(sctx: &CompileSearchCtx<'_>, id: &str) -> Result<CompiledQuery, RtDbError> {
    let db = sctx.db;
    let table_name = sctx.table_name;
    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(table_name);
    // FM-33: a soft-delete table's point read hides stamped rows the same way
    // the scan terminals do — a deleted doc reads as absent unless the admin
    // `includeDeleted` pass-through is set. Bindless literal appended to `$1`.
    let soft_filter = if sctx.table_def.soft_delete && !sctx.include_deleted {
        " AND \"deleted_at\" IS NULL"
    } else {
        ""
    };
    let sql = format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE \"id\" = $1{soft_filter}"
    );
    Ok(CompiledQuery {
        sql,
        binds: vec![EqBind::Text(id.to_string())],
        terminal: "get",
    })
}

async fn point_read(
    sctx: &SearchCtx<'_>,
    cq: CompiledQuery,
    owner: Option<&str>,
) -> Result<QueryResult, RtDbError> {
    let pool = sctx.pool;
    let table_def = sctx.table_def;
    let owner_field = sctx.owner_field;
    let collaborators_field = sctx.collaborators_field;
    let ctx = sctx.ctx;
    let CompiledQuery { sql, binds, .. } = cq;
    let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64, i64)>(&sql);
    for bind in binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
            EqBind::I64(v) => query.bind(v),
        };
    }
    let row: Option<(String, serde_json::Value, i64, i64)> = query.fetch_optional(pool).await?;

    match row {
        Some((id, doc, created_at, version)) => {
            // Per-row: a user may only point-read a doc they own OR appear in
            // the collaborators array of. Silent filter (Convex-like) — docs
            // neither owned-by nor shared-with the caller read as absent.
            if let Some(uid) = row_auth_enforced_uid(owner_field, collaborators_field, owner)
                && !row_visible_to(&doc, owner_field, collaborators_field, uid)
            {
                return Ok(QueryResult::Doc(None));
            }
            // `authorize` predicate gate (user-only; bypass callers and
            // owner/collab-only tables are unaffected). Composes with the
            // owner/collab check above — both must pass when a table declares
            // both. Silent on a false predicate (Doc(None)), matching the
            // Convex-style point-read filtering above.
            if let Some(expr) = &table_def.authorize
                && ctx.user_id.is_some()
                && !filter_matches(&doc, expr, ctx)
            {
                return Ok(QueryResult::Doc(None));
            }
            Ok(QueryResult::Doc(Some(merge_doc(
                id, doc, created_at, version,
            )?)))
        }
        None => Ok(QueryResult::Doc(None)),
    }
}

/// Wraps the client-supplied `filter` with the owner equality predicate when
/// the table declares an `ownerField` and the caller is a user (`owner`).
/// Bypass callers (`None`) and tables without an `ownerField` get the original
/// filter back unchanged — no enforcement. The owner value is `$n`-bound by
/// `compile_filter`, never interpolated into SQL.
fn owner_filter(
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
fn row_auth_enforced_uid<'a>(
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
fn row_auth_predicate_body(
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
fn authorize_predicate_body(
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

/// Whether an indexed field's declared type is numeric (the only numeric
/// Whether an indexed field's declared type is numeric enough for `sum`/`avg`.
/// `Number` and `Int64` both qualify (`Optional<…>` unwraps one layer). Note:
/// `SUM(bigint)`/`AVG(bigint)` return Postgres `numeric`, which serializes as a
/// JSON number (f64) — precision is lost past 2^53; accepted trade-off.
fn is_numeric_index_field(ty: &FieldType) -> bool {
    match ty {
        FieldType::Number | FieldType::Int64 => true,
        FieldType::Optional { inner } => is_numeric_index_field(inner),
        _ => false,
    }
}

/// Merges a stored doc with its system fields. Result docs never collide on
/// these keys: `validate_doc` rejects any `"_"`-prefixed field at write time.
fn merge_doc(
    id: String,
    doc: serde_json::Value,
    created_at: i64,
    version: i64,
) -> Result<serde_json::Value, RtDbError> {
    let mut map = match doc {
        serde_json::Value::Object(map) => map,
        _ => return Err(RtDbError::internal("stored doc is not a JSON object")),
    };
    map.insert("_id".to_string(), serde_json::Value::String(id));
    map.insert("_creationTime".to_string(), serde_json::json!(created_at));
    map.insert("_version".to_string(), serde_json::json!(version));
    Ok(serde_json::Value::Object(map))
}

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
            fields: fields_map,
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
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
            fields: fields_map,
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
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
        let err = compile_filter(&pred, &table, 1).unwrap_err();
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
        assert!(compile_filter(&ok, &table, 1).is_ok());
    }
}
