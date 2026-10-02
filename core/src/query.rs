//! The read-`Query` vocabulary shared by the server and the Rust client.
//!
//! Before ARC-007 each crate carried its own copy of `Query` and the
//! terminal/helper types (`SearchQuery`, `VectorSearchQuery`, `AggregateSpec`,
//! …), kept byte-identical by review and by the wire corpus. Like
//! [`crate::wire::FilterExpr`] before it, the single definition here makes the
//! two crates structurally incapable of disagreeing: a serde-attr drift on
//! either side is a compile error instead of a corpus surprise.
//!
//! The serde attributes ARE the wire contract (four-way, with the TS/Python/
//! Swift/Go mirrors) and are deliberately non-uniform — do not normalize them.
//! Where the two copies historically disagreed, the server's shape won:
//! `AggregateSpec.group_by` always serializes (matching the Go client's
//! `groupBy must always serialize` test), and vectors are `f64` — the
//! wire-precision ruling from the rust-client's ARC-008(a) (the wire is JSON
//! numbers; an `f32` copy silently dropped precision on round-trip).
//!
//! Server-only behavior (`Query::terminal_name`, `AggregateOp::sql_fn`,
//! `AggregateSpec::aliased_ops`) is NOT here — the core crate holds no server
//! error types and the orphan rule forbids the server reimplementing inherent
//! methods on these foreign types, so the server keeps those as extension
//! traits in `server/src/dsl.rs` (the same pattern as `StepTableExt`).
//!
//! Both crates re-export these at their historical paths
//! (`rtdb_server::dsl::Query`, `par_rt_db_client::query::Query`,
//! `par_rt_db_client::wire::AggregateSpec`, …), so no call site moved.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

/// Sort direction for `order` (wire `asc`/`desc`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Order {
    /// Ascending.
    Asc,
    /// Descending.
    Desc,
}

/// The wire `Query` — one table plus at most one read terminal. Terminals are
/// mutually exclusive (validated at compile time by the combination rules in
/// [`crate::query_combinations`]).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    /// Table name.
    pub table: String,
    /// Point-read terminal: the document id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub get: Option<String>,
    /// Index name for eq/range access.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<String>,
    /// Eq-prefix values bound to the index's leading fields.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub eq: Vec<serde_json::Value>,
    /// Exclusive lower bound on the index field after the eq prefix.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gt: Option<serde_json::Value>,
    /// Inclusive lower bound; mutually exclusive with `gt`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gte: Option<serde_json::Value>,
    /// Exclusive upper bound on the index field after the eq prefix.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lt: Option<serde_json::Value>,
    /// Inclusive upper bound; mutually exclusive with `lt`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lte: Option<serde_json::Value>,
    /// Sort direction over the index (default `Asc`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<Order>,
    /// `take(N)` terminal: first N rows (cap 4096); absent ⇒ collect (cap 4096).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub take: Option<u32>,
    /// `unique` terminal: the one matching row or `null` (error on >1). With
    /// `unique`, `take`/`order` must be absent.
    #[serde(default, skip_serializing_if = "is_false")]
    pub unique: bool,
    /// `first` terminal: the first matching row or `null`; mutually exclusive
    /// with `take`/`unique`.
    #[serde(default, skip_serializing_if = "is_false")]
    pub first: bool,
    /// `count` terminal: number of matching rows; mutually exclusive with
    /// `get`/`take`/`unique`/`first`/`order`.
    #[serde(default, skip_serializing_if = "is_false")]
    pub count: bool,
    /// `distinct` terminal: unique values of `index.fields[eq.len()]` over the
    /// matching set; mutually exclusive with every other terminal.
    #[serde(default, skip_serializing_if = "is_false")]
    pub distinct: bool,
    /// `aggregate` terminal spec; mutually exclusive with every other terminal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregate: Option<AggregateSpec>,
    /// Cursor-pagination terminal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paginate: Option<Paginate>,
    /// Additional WHERE predicate over doc fields; composes with
    /// index/order/take/cursor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<crate::wire::FilterExpr>,
    /// Full-text search terminal: ranks by `ts_rank` over a search index's
    /// tsvector; composes with `take`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<SearchQuery>,
    /// Vector-similarity terminal: ranks by the vector index's metric distance;
    /// carries its own limit. Wire key is camelCase `vectorSearch`.
    #[serde(
        default,
        rename = "vectorSearch",
        skip_serializing_if = "Option::is_none"
    )]
    pub vector_search: Option<VectorSearchQuery>,
    /// Hybrid terminal: fuses full-text and vector ranking via Reciprocal Rank
    /// Fusion; carries its own limit. Wire key is camelCase `hybridSearch`.
    #[serde(
        default,
        rename = "hybridSearch",
        skip_serializing_if = "Option::is_none"
    )]
    pub hybrid_search: Option<HybridSearchQuery>,
    /// Projection: keep only these user fields per result doc; `_`-prefixed
    /// system fields are always kept. `Some(vec![])` = system fields only;
    /// `None` = full docs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,
}

/// Serde skip predicate for `bool` fields whose default is `false`. Keeps the
/// wire form minimal — `unique`/`first`/`count` are omitted unless `true`,
/// matching the TS client's `JSON.stringify` (which drops `undefined`).
fn is_false(b: &bool) -> bool {
    !*b
}

/// Cursor-pagination terminal parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Paginate {
    /// Opaque cursor from a previous page; `None` starts at the first page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Page size.
    pub num_items: u32,
}

/// A full-text search terminal over a declared search index. `index` names a
/// search index on the query's table; `query` is free-form user text matched
/// via `websearch_to_tsquery`. `filter` is an optional db-side predicate
/// narrowed into the search WHERE; `mode` selects the match strategy (FM-30);
/// `snippet` (FM-31) opts each hit into a server-rendered `_searchSnippet`
/// field.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SearchQuery {
    /// The declared search index.
    pub index: String,
    /// Free-form user text (web-search operator syntax).
    pub query: String,
    /// Optional [`crate::wire::FilterExpr`] narrowing the WHERE.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<crate::wire::FilterExpr>,
    /// `None` = tsquery (default); `Trgm` = substring matching.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<SearchMode>,
    /// `Some(true)` adds a `_searchSnippet` per hit (tsquery mode only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snippet: Option<bool>,
}

/// Match mode for the `search` terminal. `Tsquery` (the default) matches
/// stemmed words via `tsvector @@ websearch_to_tsquery`, ranked by `ts_rank`.
/// `Trgm` matches substrings case-insensitively (`ILIKE '%query%'`) over the
/// search index's text fields, ranked by `GREATEST(similarity(field, query))`.
/// Wire form is lowercase (`"tsquery"` | `"trgm"`); serialized only when the
/// caller opts in, so existing traffic stays byte-identical.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SearchMode {
    /// Stemmed full-text via `websearch_to_tsquery` (default).
    #[default]
    Tsquery,
    /// Case-insensitive substring/autocomplete via `pg_trgm`.
    Trgm,
}

/// A vector-similarity terminal over a declared vector index. `vector` is the
/// caller-supplied query embedding (length must equal the index dimensions);
/// ranked by the index's declared metric distance ascending. `vector` is f64 —
/// the wire is JSON numbers, so a narrower element type silently drops
/// precision on round-trip (rust-client ARC-008(a)).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VectorSearchQuery {
    /// The declared vector index.
    pub index: String,
    /// Query embedding (length = index dimensions).
    pub vector: Vec<f64>,
    /// Max neighbors to return.
    pub limit: u32,
    /// Optional [`crate::wire::FilterExpr`] narrowing the WHERE.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<crate::wire::FilterExpr>,
}

/// A hybrid search terminal that fuses full-text (`search`) and vector
/// (`vectorSearch`) ranking over the SAME table via Reciprocal Rank Fusion.
/// The table must declare BOTH a search index and a vector index.
/// `search_index`/`vector_index` optionally name the indexes (auto-selected
/// server-side when omitted); `k` is the RRF constant (default 60).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HybridSearchQuery {
    /// Full-text query text.
    pub query: String,
    /// Query embedding (length = index dimensions; f64 for wire precision).
    pub vector: Vec<f64>,
    /// Fused result size.
    pub limit: u32,
    /// Named search index (auto-selected when `None`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search_index: Option<String>,
    /// Named vector index (auto-selected when `None`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector_index: Option<String>,
    /// RRF constant (server default 60).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub k: Option<u32>,
}

/// Aggregate operator for the `aggregate` terminal. `Sum`/`Avg` require a
/// numeric index field; `Min`/`Max` work on any orderable indexed field;
/// `Count` counts matching rows and consumes no aggregate field. Serializes
/// lowercase — byte-identical to the TS/Rust/Python client mirrors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AggregateOp {
    /// Sum (numeric field required).
    Sum,
    /// Average (numeric field required).
    Avg,
    /// Minimum.
    Min,
    /// Maximum.
    Max,
    /// Row count (no aggregate field consumed).
    Count,
}

/// Wire v2 widening of the aggregate `groupBy` clause: `false`/`true` keep the
/// legacy wire bytes byte-identical, while a field list groups by those
/// declared index fields.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GroupBy {
    /// Legacy scalar grouping flag.
    Bool(bool),
    /// Explicit declared index fields, in result-key order.
    Fields(Vec<String>),
}

impl Default for GroupBy {
    fn default() -> Self {
        Self::Bool(false)
    }
}

/// `aggregate` terminal spec. Exactly one of `op` (single scalar op) or
/// `aggregates` (wire v2: alias → op map, evaluated in one pass) must be set —
/// the server rejects both or neither. `group_by` shifts the terminal to a
/// grouped aggregate. Wire shape is camelCase; `group_by` ALWAYS serializes
/// (the server's shape, which wins where the historical copies disagreed —
/// matching the Go client's `groupBy must always serialize` test).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AggregateSpec {
    /// Legacy single aggregate operation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op: Option<AggregateOp>,
    /// Wire v2: alias → op. All field-needing ops share the ONE index-derived
    /// aggregate field; aliases become the keys of the multi-aggregate result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregates: Option<std::collections::BTreeMap<String, AggregateOp>>,
    /// Legacy boolean or wire-v2 explicit field list. Always serialized
    /// (`false` included) — see the struct doc.
    #[serde(default)]
    pub group_by: GroupBy,
}

/// One `{key, value}` row from a legacy grouped `aggregate` (`groupBy: true`)
/// terminal. `key` is the group's value of the index field after the eq
/// prefix; `value` is the aggregate over the field after that.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregateGroup {
    /// The group's key value.
    pub key: serde_json::Value,
    /// The group's aggregate.
    pub value: serde_json::Value,
}

/// One `{keys, values}` row from a wire-v2 composite grouped aggregate. `keys`
/// are the group's values of the groupBy fields in declared order; `values`
/// maps each alias (or, for a single-`op` spec, the lowercase op name) to its
/// aggregate.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregateMultiGroup {
    /// Group keys in the requested field order.
    pub keys: Vec<serde_json::Value>,
    /// Alias-to-result values.
    pub values: std::collections::BTreeMap<String, serde_json::Value>,
}

/// Build the wire-corpus clause-presence set for `q` — one canonical clause
/// name (matching `wire-corpus/query-combinations.json`'s `clauses` array)
/// per [`Query`] field that is actually set. Fed to
/// [`crate::query_combinations::check_query_combinations`]. Defined once here
/// because the server and the Rust client previously carried byte-identical
/// copies that could drift; with one definition, they cannot.
pub fn query_clauses(q: &Query) -> HashSet<&'static str> {
    let mut set = HashSet::new();
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn bare_query_omits_everything_but_table() {
        let q = Query {
            table: "items".into(),
            ..Default::default()
        };
        assert_eq!(serde_json::to_value(&q).unwrap(), json!({"table":"items"}));
    }

    #[test]
    fn boolean_terminals_omit_false() {
        let q = Query {
            table: "items".into(),
            first: true,
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"table":"items","first":true})
        );
    }

    #[test]
    fn aggregate_spec_group_by_always_serializes() {
        // Server-won serde attr: `groupBy: false` rides the wire (matching the
        // Go client's "groupBy must always serialize" test).
        let spec = AggregateSpec {
            op: Some(AggregateOp::Sum),
            aggregates: None,
            group_by: GroupBy::Bool(false),
        };
        assert_eq!(
            serde_json::to_value(&spec).unwrap(),
            json!({"op":"sum","groupBy":false})
        );
    }

    #[test]
    fn vector_search_uses_camel_case_key() {
        let q = Query {
            table: "docs".into(),
            vector_search: Some(VectorSearchQuery {
                index: "by_embedding".into(),
                vector: vec![1.0, 0.0],
                limit: 5,
                filter: None,
            }),
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"table":"docs","vectorSearch":{"index":"by_embedding","vector":[1.0,0.0],"limit":5}})
        );
    }

    #[test]
    fn query_clauses_enumerates_set_fields() {
        let q = Query {
            table: "items".into(),
            index: Some("by_status".into()),
            eq: vec![json!("backlog")],
            count: true,
            ..Default::default()
        };
        let present = query_clauses(&q);
        assert!(present.contains("index"));
        assert!(present.contains("eq"));
        assert!(present.contains("count"));
        assert!(!present.contains("take"));
        assert_eq!(present.len(), 3);
    }

    #[test]
    fn order_wire_form_is_lowercase() {
        assert_eq!(serde_json::to_value(Order::Desc).unwrap(), json!("desc"));
    }
}
