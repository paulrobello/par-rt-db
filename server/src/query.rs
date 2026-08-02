use std::cmp::Ordering;
use std::collections::BTreeMap;

use sqlx::PgPool;

use crate::auth::PrincipalCtx;
use crate::db::validate_db_name;
use crate::ddl::{pg_col, pg_schema, pg_search_col, pg_table, pg_vector_col};
use crate::error::RtDbError;
use crate::pagination::{decode_cursor, encode_cursor};
use crate::schema::{FieldType, IndexDef, SchemaDef, TableDef};
use crate::txn::{EqBind, eq_bind_for, eq_binds, row_visible_to};

/// Hard cap on rows returned by a single query, whether via an explicit
/// `take` or a `take`-less collect.
const MAX_TAKE: u32 = 4096;

/// Hard cap on `vectorSearch` `limit`.
const VECTOR_SEARCH_MAX_LIMIT: u32 = 256;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Order {
    Asc,
    Desc,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    pub table: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub get: Option<String>, // point read by id; excludes all below
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub eq: Vec<serde_json::Value>, // prefix binds on index fields
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gt: Option<serde_json::Value>, // exclusive lower bound on the index field after the eq prefix
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gte: Option<serde_json::Value>, // inclusive lower bound; mutually exclusive with gt
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lt: Option<serde_json::Value>, // exclusive upper bound on the index field after the eq prefix
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lte: Option<serde_json::Value>, // inclusive upper bound; mutually exclusive with lt
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<Order>, // default Asc
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub take: Option<u32>, // cap 4096; absent => collect (cap 4096)
    #[serde(default, skip_serializing_if = "is_false")]
    pub unique: bool, // with unique, take/order must be absent
    #[serde(default, skip_serializing_if = "is_false")]
    pub first: bool, // sugar over take(1); returns Doc(Some) or Doc(None); mutually exclusive with take/unique
    #[serde(default, skip_serializing_if = "is_false")]
    pub count: bool, // terminal: SELECT COUNT(*) over the same eq/range WHERE; mutually exclusive with get/take/unique/first/order
    #[serde(default, skip_serializing_if = "is_false")]
    pub distinct: bool, // terminal: SELECT DISTINCT of index.fields[eq.len()] over the same eq/range WHERE; mutually exclusive with every other terminal
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregate: Option<AggregateSpec>, // terminal: <OP>("<col>") [GROUP BY "<groupcol>"] over the same eq/range WHERE; mutually exclusive with every other terminal
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paginate: Option<Paginate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<FilterExpr>, // additional WHERE predicate over doc fields; composes with index/order/take/cursor
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<SearchQuery>, // full-text search terminal: ranks by ts_rank over a search index's tsvector; composes with take
    #[serde(
        default,
        rename = "vectorSearch",
        skip_serializing_if = "Option::is_none"
    )]
    pub vector_search: Option<VectorSearchQuery>, // vector-similarity terminal: ranks by cosine distance over a vector index; carries its own limit
    #[serde(
        default,
        rename = "hybridSearch",
        skip_serializing_if = "Option::is_none"
    )]
    pub hybrid_search: Option<HybridSearchQuery>, // hybrid terminal: fuses full-text (ts_rank) and vector (cosine) ranking via Reciprocal Rank Fusion; carries its own limit
}

/// Serde skip predicate for `bool` fields whose default is `false`. Keeps the
/// wire form minimal — `unique`/`first`/`count` are omitted unless `true`,
/// matching the TS client's `JSON.stringify` (which drops `undefined`) and the
/// rust-client's mirror struct (ARC-008 wire-parity).
fn is_false(b: &bool) -> bool {
    !*b
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Paginate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    pub num_items: u32,
}

/// A full-text search terminal over a declared search index. `index` names a
/// search index on the query's table; `query` is free-form user text matched
/// via `plainto_tsquery` so it can't inject tsquery syntax.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SearchQuery {
    pub index: String,
    pub query: String,
}

/// A vector-similarity terminal over a declared vector index. `vector` is the
/// caller-supplied query embedding (length must equal the index dimensions);
/// ranked by cosine distance (`<=>`) ascending. `filter` is an optional eq-map
/// over the index's declared `filterFields`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VectorSearchQuery {
    pub index: String,
    pub vector: Vec<f32>,
    pub limit: u32,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub filter: BTreeMap<String, serde_json::Value>,
}

/// A hybrid search terminal that fuses full-text (`search`) and vector
/// (`vectorSearch`) ranking over the SAME table into one result list via
/// Reciprocal Rank Fusion (RRF). The table must declare BOTH a search index
/// (tsvector) and a vector index; if either is missing → `BadRequest`. `query`
/// is the text (matched via `plainto_tsquery`, like `search`); `vector` is the
/// query embedding (length must equal the chosen vector index's dimensions).
/// `search_index`/`vector_index` optionally name the indexes to use; when
/// `None`, the table's first search index / first vector index is auto-selected.
/// `limit` is the result count (capped by `MAX_TAKE`); `k` is the RRF constant
/// (default 60).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HybridSearchQuery {
    pub query: String,
    pub vector: Vec<f32>,
    pub limit: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search_index: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector_index: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub k: Option<u32>,
}

/// Aggregate operator for the `aggregate` terminal. Mirrors the SQL aggregate
/// of the same name. `Sum`/`Avg` require a numeric index field; `Min`/`Max`
/// work on any orderable indexed field. Serializes lowercase (`"sum"`/`"avg"`/
/// `"min"`/`"max"`) — byte-identical to the TS/Rust/Python client mirrors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AggregateOp {
    Sum,
    Avg,
    Min,
    Max,
}

impl AggregateOp {
    /// The SQL aggregate function name (uppercase, matching the SQL keyword).
    fn sql_fn(self) -> &'static str {
        match self {
            AggregateOp::Sum => "SUM",
            AggregateOp::Avg => "AVG",
            AggregateOp::Min => "MIN",
            AggregateOp::Max => "MAX",
        }
    }
}

/// `aggregate` terminal spec. `op` selects the SQL aggregate run over the index
/// field after the eq prefix (`index.fields[eq.len()]`); `group_by` shifts the
/// terminal to a grouped aggregate — groups by `index.fields[eq.len()]` and
/// aggregates `index.fields[eq.len()+1]`, returning `{key, value}` rows. Wire
/// shape is camelCase (`groupBy`) to match the rest of the protocol.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AggregateSpec {
    pub op: AggregateOp,
    #[serde(default)]
    pub group_by: bool,
}

/// One `{key, value}` row from a grouped `aggregate` (`groupBy: true`) terminal.
/// `key` is the group's value of the index field after the eq prefix; `value`
/// is the aggregate over the field after that. Serializes camelCase.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregateGroup {
    pub key: serde_json::Value,
    pub value: serde_json::Value,
}

/// A db-side predicate appended to a query's WHERE clause. Leaves compare one
/// declared field to a value (`in` to a non-empty list); `and`/`or` nest
/// arbitrarily. Compilation: an *indexed* field compares against its typed
/// column (value typed via the field's declared `FieldType`, exactly like `eq`);
/// any other *declared* field uses jsonb extraction (`doc->>'field'`, cast for
/// non-text value kinds). Field names are schema-validated identifiers, so they
/// are safe to emit inside a quoted column name or a jsonb string literal.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "op", rename_all = "lowercase", deny_unknown_fields)]
pub enum FilterExpr {
    Eq {
        field: String,
        value: serde_json::Value,
    },
    Neq {
        field: String,
        value: serde_json::Value,
    },
    Gt {
        field: String,
        value: serde_json::Value,
    },
    Gte {
        field: String,
        value: serde_json::Value,
    },
    Lt {
        field: String,
        value: serde_json::Value,
    },
    Lte {
        field: String,
        value: serde_json::Value,
    },
    In {
        field: String,
        values: Vec<serde_json::Value>,
    },
    And {
        exprs: Vec<FilterExpr>,
    },
    Or {
        exprs: Vec<FilterExpr>,
    },
    Not {
        expr: Box<FilterExpr>,
    },
    Contains {
        field: String,
        value: serde_json::Value,
    },
    Exists {
        field: String,
    },
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
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PaginatedResult {
    pub docs: Vec<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

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
pub async fn execute_query(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    q: &Query,
    owner: Option<&str>,
) -> Result<QueryResult, RtDbError> {
    validate_db_name(db)?;
    let table_def = schema.table(&q.table)?;
    let owner_field = table_def.owner_field.as_deref();
    let collaborators_field = table_def.collaborators_field.as_deref();

    if let Some(id) = &q.get {
        reject_if_any_set(q, GET_PEERS, GET_MESSAGE)?;
        return point_read(
            pool,
            db,
            &q.table,
            id,
            owner_field,
            collaborators_field,
            owner,
        )
        .await;
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

    // Vector-similarity terminal. Incompatible with every other terminal; it
    // carries its own `limit` and does not compose with `take` (or anything
    // else). Resolution and bind construction live in `execute_vector_search`.
    if let Some(vs) = &q.vector_search {
        reject_if_any_set(q, VECTOR_SEARCH_PEERS, VECTOR_SEARCH_MESSAGE)?;
        return execute_vector_search(
            pool,
            db,
            table_def,
            &q.table,
            vs,
            owner_field,
            collaborators_field,
            owner,
        )
        .await;
    }

    // Hybrid search terminal. Incompatible with every other terminal (including
    // `search` and `vectorSearch` — hybrid IS their combination); it carries its
    // own `limit` and fuses ts_rank + cosine distance via RRF. Resolution, bind
    // construction, and the fused SQL live in `execute_hybrid_search`.
    if let Some(hs) = &q.hybrid_search {
        reject_if_any_set(q, HYBRID_SEARCH_PEERS, HYBRID_SEARCH_MESSAGE)?;
        return execute_hybrid_search(
            pool,
            db,
            table_def,
            &q.table,
            hs,
            owner_field,
            collaborators_field,
            owner,
        )
        .await;
    }

    // Full-text search terminal. It ranks over a search index's tsvector and is
    // incompatible with every index-based terminal; `take` (already capped) is
    // the only field it composes with.
    if let Some(search) = &q.search {
        reject_if_any_set(q, SEARCH_PEERS, SEARCH_MESSAGE)?;
        return execute_search(
            pool,
            db,
            table_def,
            &q.table,
            search,
            q.take,
            owner_field,
            collaborators_field,
            owner,
        )
        .await;
    }

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
    let mut filter_binds: Vec<EqBind> = match &effective_filter {
        Some(filter) => {
            let (fragment, binds) =
                compile_filter(filter, table_def, eq_len + range_binds.len() + 1)?;
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
        let ph = eq_len + range_binds.len() + filter_binds.len() + 1;
        where_conditions.push(row_auth_predicate_body(
            owner_field,
            collaborators_field,
            ph,
        ));
        filter_binds.push(EqBind::Text(uid.to_string()));
    }
    let limit_placeholder = eq_len + range_binds.len() + filter_binds.len() + 1;

    if q.count {
        let pg_schema_name = pg_schema(db);
        let table_ident = pg_table(&q.table);
        let mut sql = format!("SELECT COUNT(*) FROM \"{pg_schema_name}\".\"{table_ident}\"");
        if !where_conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_conditions.join(" AND "));
        }
        let mut query = sqlx::query_scalar::<_, i64>(&sql);
        for bind in binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
                EqBind::I64(v) => query.bind(v),
            };
        }
        for bind in range_binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
                EqBind::I64(v) => query.bind(v),
            };
        }
        for bind in &filter_binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
                EqBind::I64(v) => query.bind(v),
            };
        }
        let count = query.fetch_one(pool).await?;
        return Ok(QueryResult::Count(count));
    }

    // Distinct terminal: SELECT DISTINCT of the index field immediately after
    // the eq prefix over the same eq/range WHERE clause every other terminal
    // builds. The combination cascade already rejected every other terminal;
    // `distinct` composes only with `index`/`eq`/range bounds. The preconditions
    // below reject the no-index and no-remaining-field cases with the same
    // BadRequest shape as a missing-index `eq` bind. Capped by `MAX_TAKE` for
    // parity with `collect` (a distinct set bounded by the matching row count).
    if q.distinct {
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
        let table_ident = pg_table(&q.table);
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
        let mut query = sqlx::query_as::<_, (serde_json::Value,)>(sql.as_str());
        for bind in binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
                EqBind::I64(v) => query.bind(v),
            };
        }
        for bind in range_binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
                EqBind::I64(v) => query.bind(v),
            };
        }
        for bind in &filter_binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
                EqBind::I64(v) => query.bind(v),
            };
        }
        query = query.bind(i64::from(MAX_TAKE));
        let rows = query.fetch_all(pool).await?;
        let values: Vec<serde_json::Value> = rows.into_iter().map(|(v,)| v).collect();
        return Ok(QueryResult::Distinct(values));
    }

    // Aggregate terminal: runs `<OP>("<col>")` (SUM/AVG/MIN/MAX) over the same
    // eq/range WHERE clause every other terminal builds, returning one scalar
    // (`Aggregate(value)`). With `group_by: true`, it groups by the index field
    // after the eq prefix and aggregates the one after that, returning
    // `AggregateGroups([{key,value},…])`. The combination cascade already
    // rejected every other terminal; `aggregate` composes only with `index`/
    // `eq`/range bounds/`filter`. The preconditions below reject the no-index,
    // no-remaining-field, and (for sum/avg) non-numeric-field cases. Group count
    // is capped by `MAX_TAKE` for parity with `collect`.
    if let Some(agg) = &q.aggregate {
        let idx = index_def.ok_or_else(|| {
            RtDbError::bad_request("aggregate requires an index field beyond the eq prefix")
        })?;
        // Resolve the aggregate field (the one after the eq prefix for plain
        // aggregate, the one after that for groupBy) and validate the schema
        // field type for sum/avg's numeric requirement. The groupcol for the
        // groupBy case is the same field distinct would use.
        let (group_col, agg_field_name) = if agg.group_by {
            if eq_len + 1 >= idx.fields.len() {
                return Err(RtDbError::bad_request(
                    "aggregate groupBy requires two index fields beyond the eq prefix",
                ));
            }
            let group_field = idx.fields[eq_len].as_str();
            let agg_field = idx.fields[eq_len + 1].as_str();
            (Some(pg_col(group_field)), agg_field)
        } else {
            if eq_len >= idx.fields.len() {
                return Err(RtDbError::bad_request(
                    "aggregate requires an index field beyond the eq prefix",
                ));
            }
            (None, idx.fields[eq_len].as_str())
        };
        let agg_field_type = table_def.fields.get(agg_field_name).ok_or_else(|| {
            RtDbError::internal(format!("index references unknown field '{agg_field_name}'"))
        })?;
        if matches!(agg.op, AggregateOp::Sum | AggregateOp::Avg)
            && !is_numeric_index_field(agg_field_type)
        {
            return Err(RtDbError::bad_request(format!(
                "aggregate op {} requires a numeric index field",
                agg.op.sql_fn().to_lowercase()
            )));
        }
        let agg_col = pg_col(agg_field_name);
        let pg_schema_name = pg_schema(db);
        let table_ident = pg_table(&q.table);
        let op_sql = agg.op.sql_fn();
        // Project via `to_jsonb` so a single `serde_json::Value` decoder handles
        // text/number/boolean columns uniformly, exactly like `distinct`. A
        // scalar SUM/AVG/MIN/MAX over zero matching rows yields one row with
        // SQL NULL → `serde_json::Value::Null`.
        if let Some(group_col) = group_col {
            let mut sql = format!(
                "SELECT to_jsonb(\"{group_col}\") AS k, to_jsonb({op_sql}(\"{agg_col}\")) AS v FROM \"{pg_schema_name}\".\"{table_ident}\""
            );
            if !where_conditions.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&where_conditions.join(" AND "));
            }
            sql.push_str(&format!(
                " GROUP BY \"{group_col}\" ORDER BY k LIMIT ${limit_placeholder}"
            ));
            let mut query = sqlx::query_as::<_, (serde_json::Value, serde_json::Value)>(&sql);
            for bind in binds {
                query = match bind {
                    EqBind::Text(v) => query.bind(v),
                    EqBind::Num(v) => query.bind(v),
                    EqBind::Bool(v) => query.bind(v),
                    EqBind::I64(v) => query.bind(v),
                };
            }
            for bind in range_binds {
                query = match bind {
                    EqBind::Text(v) => query.bind(v),
                    EqBind::Num(v) => query.bind(v),
                    EqBind::Bool(v) => query.bind(v),
                    EqBind::I64(v) => query.bind(v),
                };
            }
            for bind in &filter_binds {
                query = match bind {
                    EqBind::Text(v) => query.bind(v),
                    EqBind::Num(v) => query.bind(v),
                    EqBind::Bool(v) => query.bind(v),
                    EqBind::I64(v) => query.bind(v),
                };
            }
            query = query.bind(i64::from(MAX_TAKE));
            let rows = query.fetch_all(pool).await?;
            let groups: Vec<AggregateGroup> = rows
                .into_iter()
                .map(|(k, v)| AggregateGroup { key: k, value: v })
                .collect();
            return Ok(QueryResult::AggregateGroups(groups));
        }
        let mut sql = format!(
            "SELECT COALESCE(to_jsonb({op_sql}(\"{agg_col}\")), 'null'::jsonb) AS v FROM \"{pg_schema_name}\".\"{table_ident}\""
        );
        if !where_conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_conditions.join(" AND "));
        }
        let mut query = sqlx::query_as::<_, (serde_json::Value,)>(&sql);
        for bind in binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
                EqBind::I64(v) => query.bind(v),
            };
        }
        for bind in range_binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
                EqBind::I64(v) => query.bind(v),
            };
        }
        for bind in &filter_binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
                EqBind::I64(v) => query.bind(v),
            };
        }
        let (v,) = query.fetch_one(pool).await?;
        return Ok(QueryResult::Aggregate(v));
    }

    let mut sort_cols: Vec<String> = match index_def {
        Some(idx) => idx.fields[eq_len..]
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
        let table_ident = pg_table(&q.table);
        let mut sql = format!(
            "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\""
        );
        if !where_conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_conditions.join(" AND "));
        }
        sql.push_str(" ORDER BY ");
        sql.push_str(&order_by);
        sql.push_str(&format!(" LIMIT ${limit_placeholder}"));

        let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64, i64)>(&sql);
        for bind in binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
                EqBind::I64(v) => query.bind(v),
            };
        }
        for bind in range_binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
                EqBind::I64(v) => query.bind(v),
            };
        }
        for bind in &filter_binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
                EqBind::I64(v) => query.bind(v),
            };
        }
        for bind in cursor_binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
                EqBind::I64(v) => query.bind(v),
            };
        }
        // Fetch one extra row so a next page can be detected without a second
        // round-trip; the extra is discarded after the has-next check.
        query = query.bind(i64::from(num_items) + 1);
        let mut rows = query.fetch_all(pool).await?;

        let has_next = rows.len() > num_items as usize;
        if has_next {
            rows.pop();
        }

        // The next cursor is built from the last row of the page (after the
        // extra is discarded); absent when the page is empty or last.
        let next_cursor =
            if has_next && let Some((last_id, last_doc, last_created_at, _)) = rows.last() {
                let mut cursor_values: Vec<serde_json::Value> = Vec::new();
                if let Some(idx) = index_def {
                    for fname in &idx.fields[eq_len..] {
                        let val = last_doc.get(fname).ok_or_else(|| {
                            RtDbError::internal(format!(
                                "stored doc is missing indexed field '{fname}'"
                            ))
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
        return Ok(QueryResult::Paginated(PaginatedResult {
            docs,
            next_cursor,
        }));
    }

    let limit: u32 = if q.unique {
        2
    } else if q.first {
        1
    } else {
        q.take.unwrap_or(MAX_TAKE)
    };

    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(&q.table);
    let mut sql = format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\""
    );
    if !where_conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_conditions.join(" AND "));
    }
    sql.push_str(" ORDER BY ");
    sql.push_str(&order_by);
    sql.push_str(&format!(" LIMIT ${limit_placeholder}"));

    let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64, i64)>(&sql);
    for bind in binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
            EqBind::I64(v) => query.bind(v),
        };
    }
    for bind in range_binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
            EqBind::I64(v) => query.bind(v),
        };
    }
    for bind in &filter_binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
            EqBind::I64(v) => query.bind(v),
        };
    }
    query = query.bind(i64::from(limit));
    let mut rows = query.fetch_all(pool).await?;

    if q.unique {
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

    if q.first {
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

/// Compiles a `filter` into a fully-parenthesized SQL predicate plus its typed
/// binds, with `$n` placeholders numbered from 1-based `start_pos`. Every leaf
/// emits at least one bind, so the fragment is never empty.
fn compile_filter(
    filter: &FilterExpr,
    table: &TableDef,
    start_pos: usize,
) -> Result<(String, Vec<EqBind>), RtDbError> {
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
        FilterExpr::Not { expr } => Ok(format!(
            "NOT ({})",
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
        FilterExpr::Eq { field, value }
        | FilterExpr::Neq { field, value }
        | FilterExpr::Gt { field, value }
        | FilterExpr::Gte { field, value }
        | FilterExpr::Lt { field, value }
        | FilterExpr::Lte { field, value } => {
            let op = match node {
                FilterExpr::Eq { .. } => "=",
                FilterExpr::Neq { .. } => "<>",
                FilterExpr::Gt { .. } => ">",
                FilterExpr::Gte { .. } => ">=",
                FilterExpr::Lt { .. } => "<",
                FilterExpr::Lte { .. } => "<=",
                _ => unreachable!(),
            };
            let (lhs, bind) = field_lhs_and_bind(field, value, table)?;
            Ok(format!("{lhs} {op} {}", render_literal(&bind)))
        }
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
        FilterExpr::Not { expr } => Ok(format!(
            "NOT ({})",
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

// ============ Doc-level filter evaluator (model C `authorize`) ============

/// Resolves a principal marker to its `Value`: `{"$user": true}` → the caller's
/// `user_id`, `{"$email": true}` → the caller's `email`. A marker whose
/// principal field is `None` (a `Machine`/admin/scheduled caller) resolves to
/// `Null`, which cannot equal any non-null doc field — i.e. the predicate
/// silently fails to match, the safe over-approximation. Non-marker values pass
/// through unchanged.
fn resolve_value(v: &serde_json::Value, ctx: &PrincipalCtx) -> serde_json::Value {
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
/// inner `Eq` to `false`, so `Not` yields `true`. For a server-declared
/// `authorize` predicate this is acceptable — the predicate is validated and
/// inserts are stamped/verified — but a `Not(Eq)` over an absent field is
/// permissive by construction. Reviewers should be aware.
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
        // `Not` inverts the inner boolean; see the doc comment for the
        // absent-field-permissive subtlety.
        FilterExpr::Not { expr } => !filter_matches(doc, expr, ctx),
        FilterExpr::Contains { field, value } => doc
            .get(field)
            .and_then(|v| v.as_array())
            .is_some_and(|arr| arr.iter().any(|v| v == &resolve_value(value, ctx))),
        FilterExpr::Exists { field } => doc.get(field).is_some_and(|v| !v.is_null()),
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
        jsonb_lhs_and_bind(field, value)
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

/// Full-text search terminal: matches a search index's generated tsvector
/// against `plainto_tsquery(<query text>)` and ranks by `ts_rank` descending,
/// with `(created_at, id)` tie-breakers. Composes with `take` (defaulting to
/// `MAX_TAKE`); the caller has already rejected every other terminal. The query
/// text is bound once via `$1` and reused in the `ORDER BY ts_rank`, so user
/// text can never inject tsquery syntax. Unknown index / empty query →
/// `BadRequest`, never a 500.
#[allow(clippy::too_many_arguments)]
async fn execute_search(
    pool: &PgPool,
    db: &str,
    table_def: &TableDef,
    table_name: &str,
    search: &SearchQuery,
    take: Option<u32>,
    owner_field: Option<&str>,
    collaborators_field: Option<&str>,
    owner: Option<&str>,
) -> Result<QueryResult, RtDbError> {
    if search.query.trim().is_empty() {
        return Err(RtDbError::bad_request(
            "search query text must not be empty",
        ));
    }
    let index_def = table_def
        .indexes
        .iter()
        .find(|index| index.name == search.index && index.search)
        .ok_or_else(|| {
            RtDbError::bad_request(format!("search index '{}' not found", search.index))
        })?;
    let sv_col = pg_search_col(&index_def.name);
    let limit = take.unwrap_or(MAX_TAKE);
    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(table_name);

    // Per-row auth predicate: when the caller is a user and the table declares
    // `ownerField` and/or `collaboratorsField`, restrict to rows the caller
    // owns OR appears in as a collaborator. The schema-validated identifiers
    // are interpolated; the uid is `$2`-bound once and reused on both sides of
    // the OR. Owner-only emits the single-predicate form byte-identical to the
    // pre-collaborators SQL. The uid bind occupies `$2`, pushing `LIMIT` to $3.
    let enforced_uid = row_auth_enforced_uid(owner_field, collaborators_field, owner);
    let (limit_ph, owner_clause) = match enforced_uid {
        Some(_) => (
            3,
            format!(
                " AND {}",
                row_auth_predicate_body(owner_field, collaborators_field, 2)
            ),
        ),
        None => (2, String::new()),
    };
    let sql = format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\" \
         WHERE \"{sv_col}\" @@ plainto_tsquery($1){owner_clause} \
         ORDER BY ts_rank(\"{sv_col}\", plainto_tsquery($1)) DESC, \"created_at\" DESC, \"id\" DESC \
         LIMIT ${limit_ph}"
    );
    let mut query =
        sqlx::query_as::<_, (String, serde_json::Value, i64, i64)>(&sql).bind(&search.query);
    if let Some(uid) = enforced_uid {
        query = query.bind(uid);
    }
    let rows = query.bind(i64::from(limit)).fetch_all(pool).await?;
    let docs = rows
        .into_iter()
        .map(|(id, doc, created_at, version)| merge_doc(id, doc, created_at, version))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(QueryResult::Docs(docs))
}

/// Vector-similarity terminal: ranks rows by cosine distance (`<=>`) between
/// the index's `v_<index>` column and the query vector, ascending, limited to
/// `limit`. Optional `filter` eq-binds over the index's declared `filterFields`.
/// Unknown index / length mismatch / unknown filter key / out-of-range limit
/// → `BadRequest`. Bind order: filter eq-binds occupy `$1..$k`, then (when the
/// table is owner-gated and the caller is a user) the owner id occupies
/// `$(k+1)`, then the query vector (`$n::vector`), then `limit`.
#[allow(clippy::too_many_arguments)]
async fn execute_vector_search(
    pool: &PgPool,
    db: &str,
    table_def: &TableDef,
    table_name: &str,
    vs: &VectorSearchQuery,
    owner_field: Option<&str>,
    collaborators_field: Option<&str>,
    owner: Option<&str>,
) -> Result<QueryResult, RtDbError> {
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

    // Build eq-binds for any filter entries; each key must be a declared
    // filterField of this index. The field's declared `FieldType` types the
    // value the same way `eq` prefixes are typed in `txn::eq_bind_for`.
    let mut filter_binds: Vec<EqBind> = Vec::new();
    let mut filter_cols: Vec<String> = Vec::new();
    for (key, value) in &vs.filter {
        if !vec_spec.filter_fields.iter().any(|f| f == key) {
            return Err(RtDbError::bad_request(format!(
                "vectorSearch filter key '{key}' is not a declared filterField of index '{}'",
                vs.index
            )));
        }
        let field_type = table_def.fields.get(key).ok_or_else(|| {
            RtDbError::internal(format!("filterField '{key}' missing from table fields"))
        })?;
        filter_binds.push(eq_bind_for(field_type, value)?);
        filter_cols.push(pg_col(key));
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

    // Bind numbering: filter eq-binds first ($1..$k), then the per-row auth
    // uid ($k+1) when owner/collaborators-enforced, then the query vector
    // (cast to `vector`), then `limit`. The WHERE clause always excludes rows
    // whose vector column is NULL, so undimensioned rows never surface as
    // bogus nearest-neighbors. The per-row auth predicate (`doc->>'<field>'`
    // = $n OR `doc->'<collabField>' ? $n) mirrors the main query path: both
    // field names are `is_valid_identifier`-validated at schema push, so they
    // are safe to interpolate into jsonb string-literal positions; the uid is
    // `$n`-bound once, never interpolated.
    let enforced_uid = row_auth_enforced_uid(owner_field, collaborators_field, owner);
    let mut bind_idx = 1usize;
    let mut filter_placeholders: Vec<String> = Vec::with_capacity(filter_cols.len());
    for _ in &filter_cols {
        filter_placeholders.push(format!("${bind_idx}"));
        bind_idx += 1;
    }
    let owner_ph: Option<usize> = if enforced_uid.is_some() {
        let ph = bind_idx;
        bind_idx += 1;
        Some(ph)
    } else {
        None
    };
    let qvec_ph = bind_idx;
    bind_idx += 1;
    let limit_ph = bind_idx;

    let mut where_clause = format!("\"{v_col}\" IS NOT NULL");
    if !filter_cols.is_empty() {
        let conds: Vec<String> = filter_cols
            .iter()
            .zip(filter_placeholders.iter())
            .map(|(col, ph)| format!("\"{col}\" = {ph}"))
            .collect();
        where_clause = format!("{where_clause} AND {}", conds.join(" AND "));
    }
    if let Some(ph) = owner_ph {
        where_clause.push_str(" AND ");
        where_clause.push_str(&row_auth_predicate_body(
            owner_field,
            collaborators_field,
            ph,
        ));
    }

    let sql = format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\" \
         WHERE {where_clause} \
         ORDER BY \"{v_col}\" <=> ${qvec_ph}::vector \
         LIMIT ${limit_ph}"
    );

    let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64, i64)>(&sql);
    for bind in filter_binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
            EqBind::I64(v) => query.bind(v),
        };
    }
    if let Some(uid) = enforced_uid {
        query = query.bind(uid);
    }
    let rows = query
        .bind(qvec_text)
        .bind(i64::from(vs.limit))
        .fetch_all(pool)
        .await?;
    let docs = rows
        .into_iter()
        .map(|(id, doc, created_at, version)| merge_doc(id, doc, created_at, version))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(QueryResult::Docs(docs))
}

/// Hybrid search terminal: fuses full-text (`ts_rank`) and vector (cosine
/// `<=>`) ranking over the same table via Reciprocal Rank Fusion (RRF). The
/// table must declare BOTH a search index (tsvector) AND a vector index; if
/// either is missing → `BadRequest`. The candidate set is the UNION of rows
/// matching `plainto_tsquery($text)` and rows with a non-null vector; both
/// rankings are computed in a single statement with window functions and fused
/// as `1/(k + r_text) + 1/(k + r_vec)` (default `k = 60`). Bind order: the text
/// query (`$1`, referenced in WHERE/ts_rank), the owner id (`$2`) when
/// owner-enforced, the query vector (`$n::vector`), the RRF constant `k`, then
/// `limit`. Column names come from `pg_search_col`/`pg_vector_col` over the
/// resolved indexes (auto-selected when not named); all identifiers are
/// schema-validated and double-quoted; every value is `$n`-bound.
#[allow(clippy::too_many_arguments)]
async fn execute_hybrid_search(
    pool: &PgPool,
    db: &str,
    table_def: &TableDef,
    table_name: &str,
    hs: &HybridSearchQuery,
    owner_field: Option<&str>,
    collaborators_field: Option<&str>,
    owner: Option<&str>,
) -> Result<QueryResult, RtDbError> {
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
    // per-row auth uid ($2) when owner/collaborators-enforced, then the query
    // vector (cast to `vector`), the RRF constant `k`, and `limit`. The per-row
    // auth predicate interpolates only the schema-validated `ownerField` /
    // `collaboratorsField` into jsonb string-literal positions; the uid is
    // `$n`-bound once.
    let enforced_uid = row_auth_enforced_uid(owner_field, collaborators_field, owner);
    let text_ph = 1usize;
    let mut bind_idx = 2usize;
    let owner_ph: Option<usize> = if enforced_uid.is_some() {
        let ph = bind_idx;
        bind_idx += 1;
        Some(ph)
    } else {
        None
    };
    let qvec_ph = bind_idx;
    bind_idx += 1;
    let k_ph = bind_idx;
    bind_idx += 1;
    let limit_ph = bind_idx;

    let owner_clause = match owner_ph {
        Some(ph) => format!(
            " AND {}",
            row_auth_predicate_body(owner_field, collaborators_field, ph)
        ),
        None => String::new(),
    };

    // RRF over the union of text matches and vector-bearing rows. Rows that
    // don't match the text get ts_rank = 0 (ranked last on the text axis); rows
    // with a null vector get dist NULL (ranked last on the vector axis via NULLS
    // LAST). The final ORDER BY tie-breakers (created_at, id) keep output
    // deterministic when RRF scores collide.
    let sql = format!(
        "WITH matched AS ( \
           SELECT \"id\", \"doc\", \"created_at\", \"version\", \
                  ts_rank(\"{sv_col}\", plainto_tsquery(${text_ph})) AS trank, \
                  (\"{v_col}\" <=> ${qvec_ph}::vector) AS dist \
           FROM \"{pg_schema_name}\".\"{table_ident}\" \
           WHERE (\"{sv_col}\" @@ plainto_tsquery(${text_ph}) OR \"{v_col}\" IS NOT NULL){owner_clause} \
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

    let mut query =
        sqlx::query_as::<_, (String, serde_json::Value, i64, i64)>(&sql).bind(&hs.query);
    if let Some(uid) = enforced_uid {
        query = query.bind(uid);
    }
    let rows = query
        .bind(qvec_text)
        .bind(k)
        .bind(i64::from(hs.limit))
        .fetch_all(pool)
        .await?;
    let docs = rows
        .into_iter()
        .map(|(id, doc, created_at, version)| merge_doc(id, doc, created_at, version))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(QueryResult::Docs(docs))
}

async fn point_read(
    pool: &PgPool,
    db: &str,
    table_name: &str,
    id: &str,
    owner_field: Option<&str>,
    collaborators_field: Option<&str>,
    owner: Option<&str>,
) -> Result<QueryResult, RtDbError> {
    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(table_name);
    let row: Option<(String, serde_json::Value, i64, i64)> = sqlx::query_as(&format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE \"id\" = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?;

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
            fields,
            indexes: vec![IndexDef {
                name: format!("by_{field}"),
                fields: vec![field.to_string()],
                search: false,
                vector: None,
                unique: false,
                r#where: None,
            }],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
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
            fields: fields_map,
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
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

    #[test]
    fn compile_not_contains_exists() {
        let table = test_table_with_fields(&["owner", "editors", "archivedat"]);
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
        assert_eq!(sql, "NOT ((doc->>'owner') = $1)");
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
    /// (`$user`, `$email`) for the Rust-level doc evaluator. This is the
    /// RED-GREEN gate for Task 3 of the per-row-auth-predicate SDD.
    #[test]
    fn filter_matches_all_variants_and_principal() {
        let ctx = PrincipalCtx {
            user_id: Some("u1".to_string()),
            email: Some("e@x".to_string()),
        };
        let doc =
            json!({"owner":"u1","editors":["u1","u2"],"visibility":"public","archivedat":null});

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
            },
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
        // Not(Exists{archivedat}) -> Exists is false (null) -> Not is true.
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
            },
        ));
    }
}
