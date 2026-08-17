//! Query DSL: builds the exact `Query` JSON the server expects, and parses untagged results.

use crate::wire::{AggregateOp, AggregateSpec, FilterExpr, SearchMode, SearchQuery};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
/// Sort direction for `order` (wire `asc`/`desc`).
pub enum Order {
    /// Ascending.
    Asc,
    /// Descending.
    Desc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// Cursor-pagination terminal parameters.
pub struct Paginate {
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Opaque cursor from a previous page; `None` starts at the first page.
    pub cursor: Option<String>,
    /// Page size.
    pub num_items: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
/// The wire `Query` — one table plus at most one read terminal. Built via
/// [`TableQuery`] (or constructed directly); sent with `POST /api/query` / WS
/// `Query`.
pub struct Query {
    /// Table name.
    pub table: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Point-read terminal: the document id.
    pub get: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Index name for eq/range access.
    pub index: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    /// Eq-prefix values bound to the index's leading fields.
    pub eq: Vec<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Exclusive lower bound on the index field after the eq prefix.
    pub gt: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Inclusive lower bound on the index field after the eq prefix.
    pub gte: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Exclusive upper bound on the index field after the eq prefix.
    pub lt: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Inclusive upper bound on the index field after the eq prefix.
    pub lte: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Sort direction over the index.
    pub order: Option<Order>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// `take(N)` terminal: first N rows.
    pub take: Option<u32>,
    #[serde(default, skip_serializing_if = "is_false")]
    /// `unique` terminal: the one matching row or `null` (error on >1).
    pub unique: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    /// `first` terminal: the first matching row or `null`.
    pub first: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    /// `count` terminal: number of matching rows.
    pub count: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    /// `distinct` terminal: unique values of the index field after the eq prefix.
    pub distinct: bool,
    /// Aggregate terminal: SUM/AVG/MIN/MAX over the index field after the eq
    /// prefix; `group_by` shifts to a grouped aggregate. Mutually exclusive with
    /// every other terminal except `eq`/range bounds/`filter`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregate: Option<AggregateSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Cursor-pagination terminal.
    pub paginate: Option<Paginate>,
    /// Additional db-side WHERE predicate over doc fields; composes with
    /// index/order/take/cursor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<FilterExpr>,
    /// Full-text search terminal: ranks by `ts_rank` over a search index's
    /// tsvector; composes with `take`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<SearchQuery>,
    /// Vector-similarity terminal: ranks by cosine distance over a vector index;
    /// carries its own limit. The wire key is camelCase `vectorSearch` (matches
    /// the server's explicit `#[serde(rename = "vectorSearch")]`; this struct
    /// has no `rename_all`, so the rename must be explicit).
    #[serde(
        default,
        rename = "vectorSearch",
        skip_serializing_if = "Option::is_none"
    )]
    pub vector_search: Option<crate::wire::VectorSearchQuery>,
    /// Hybrid terminal: fuses full-text (`search`) and vector (`vectorSearch`)
    /// ranking via Reciprocal Rank Fusion; carries its own limit. The wire key
    /// is camelCase `hybridSearch` (explicit rename, matching `vector_search`).
    #[serde(
        default,
        rename = "hybridSearch",
        skip_serializing_if = "Option::is_none"
    )]
    pub hybrid_search: Option<crate::wire::HybridSearchQuery>,
}

fn is_false(b: &bool) -> bool {
    !*b
}

// ARC-130: response-shaped (returned by the paginate terminal). The caller
// reads it; the server constructs it. `#[non_exhaustive]` lets the wire shape
// gain fields later without a SemVer break for consumers that destructure it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
/// One page of cursor-paginated results. Feed `next_cursor` back into the
/// next `paginate` call; `None` after the last page. `#[non_exhaustive]` so
/// the wire shape can gain fields without breaking destructuring consumers.
pub struct Paginated<T> {
    /// This page's rows.
    pub docs: Vec<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Cursor for the next page; `None` when exhausted.
    pub next_cursor: Option<String>,
}

/// Optional arguments for [`TableQuery::vector_search`]: `filter` is an
/// optional `FilterExpr` (the db-side `filter()` DSL) that narrows the vector
/// search `WHERE` server-side. Defaults to `None` (no filter on the wire). The
/// vector-search filter is nested on the terminal — distinct from the top-level
/// `.filter()` builder, which is mutually exclusive with `vectorSearch` — so
/// callers pass it through the `.vector_search(idx, vec, lim, { filter })`
/// opts, not a chained `.filter()`. `From<()>` lets callers omit it
/// (`.vector_search(idx, vec, lim, ())`); a `VectorSearchOpts { filter }`
/// literal names the field.
#[derive(Debug, Clone, Default)]
pub struct VectorSearchOpts {
    /// Server-side `FilterExpr` narrowing the vector search `WHERE`.
    pub filter: Option<FilterExpr>,
}

impl From<()> for VectorSearchOpts {
    fn from(_: ()) -> Self {
        Self::default()
    }
}

/// Optional arguments for [`TableQuery::hybrid_search`]: `search_index`/
/// `vector_index` optionally name the indexes (auto-selected server-side when
/// `None`); `k` is the RRF constant (server-defaults when `None`). All default
/// to `None`, so callers can write `.hybrid_search(query, vector, limit, ())`
/// for the auto-select case, or name individual fields by struct literal
/// without minding positional order.
#[derive(Debug, Clone, Default)]
pub struct HybridSearchOpts {
    /// Name the search index (auto-selected when `None`).
    pub search_index: Option<String>,
    /// Name the vector index (auto-selected when `None`).
    pub vector_index: Option<String>,
    /// RRF constant (server default when `None`).
    pub k: Option<u32>,
}

impl From<()> for HybridSearchOpts {
    fn from(_: ()) -> Self {
        Self::default()
    }
}

/// Optional arguments for [`TableQuery::search`]: `filter` is an optional
/// `FilterExpr` (the db-side `filter()` DSL) that narrows the search `WHERE`
/// server-side; `mode` selects the match strategy (FM-30) — `None` keeps the
/// default full-text (`tsquery`) behavior, `Some(SearchMode::Trgm)` opts into
/// substring/autocomplete matching over the index's text fields; `snippet`
/// (FM-31) opts each hit into a server-rendered `_searchSnippet` highlight
/// fragment (tsquery mode only — the server rejects it with `trgm`). All
/// default to `None` (nothing extra on the wire). The search filter is nested
/// on the terminal — distinct from the top-level `.filter()` builder, which is
/// mutually exclusive with `search` — so callers pass it through the
/// `.search(idx, text, { filter, mode, snippet })` opts, not a chained
/// `.filter()`. `From<()>` lets callers omit them (`.search(idx, text, ())`);
/// a `SearchOpts { .. }` literal names the fields.
#[derive(Debug, Clone, Default)]
pub struct SearchOpts {
    /// Server-side `FilterExpr` narrowing the search `WHERE`.
    pub filter: Option<FilterExpr>,
    /// `Some(SearchMode::Trgm)` opts into substring/autocomplete matching (FM-30).
    pub mode: Option<SearchMode>,
    /// `Some(true)` attaches a `_searchSnippet` highlight per hit (FM-31, tsquery mode only).
    pub snippet: Option<bool>,
}

impl From<()> for SearchOpts {
    fn from(_: ()) -> Self {
        Self::default()
    }
}

/// A built query is just the wire `Query` (terminals consume the builder).
pub struct TableQuery {
    q: Query,
}

impl TableQuery {
    /// Start building a query over `table`.
    pub fn new(table: &str) -> Self {
        Self {
            q: Query {
                table: table.into(),
                ..Default::default()
            },
        }
    }
    /// Build a point-read for `id` in `table`. Unlike `new()` (which returns the
    /// builder so further clauses can be chained), this returns the finished wire
    /// `Query` directly — a point-read is already a complete query — so it can be
    /// passed straight to `RtDbHttpClient::run(impl Into<Query>)`.
    pub fn get(table: &str, id: &str) -> Query {
        Query {
            table: table.into(),
            get: Some(id.into()),
            ..Default::default()
        }
    }

    /// Use `index` with the given eq-prefix values (one per leading index
    /// field).
    pub fn with_index(mut self, index: &str, eq: &[serde_json::Value]) -> Self {
        self.q.index = Some(index.into());
        self.q.eq = eq.to_vec();
        self
    }
    /// Exclusive lower bound on the index field after the eq prefix.
    pub fn gt(mut self, v: impl Into<serde_json::Value>) -> Self {
        self.q.gt = Some(v.into());
        self
    }
    /// Inclusive lower bound on the index field after the eq prefix.
    pub fn gte(mut self, v: impl Into<serde_json::Value>) -> Self {
        self.q.gte = Some(v.into());
        self
    }
    /// Exclusive upper bound on the index field after the eq prefix.
    pub fn lt(mut self, v: impl Into<serde_json::Value>) -> Self {
        self.q.lt = Some(v.into());
        self
    }
    /// Inclusive upper bound on the index field after the eq prefix.
    pub fn lte(mut self, v: impl Into<serde_json::Value>) -> Self {
        self.q.lte = Some(v.into());
        self
    }
    /// Sort direction over the index.
    pub fn order(mut self, o: Order) -> Self {
        self.q.order = Some(o);
        self
    }

    /// Append a db-side `filter` predicate. Composes with `with_index`/range
    /// bounds/`order`/`take`; the server validates terminal combinations.
    pub fn filter(mut self, expr: FilterExpr) -> Self {
        self.q.filter = Some(expr);
        self
    }

    /// Full-text `search` terminal over a declared search index. Composes only
    /// with `take` (e.g. `.search("idx", "text", ()).take(10)`); the server
    /// rejects every other terminal alongside it.
    ///
    /// `opts` accepts any `Into<SearchOpts>`: pass `()` to omit the filter
    /// (`.search(idx, text, ())`), or a `SearchOpts { filter, mode, snippet }`
    /// literal to narrow results server-side via a `FilterExpr`, opt into
    /// substring/autocomplete matching via `SearchMode::Trgm` (FM-30), and/or
    /// ask for a `_searchSnippet` highlight on each hit via `snippet:
    /// Some(true)` (FM-31) — the query text itself honors web-search operator
    /// syntax server-side (quoted phrases, bare `or`, `-term` exclusion).
    /// `mode`/`snippet` are omitted on the wire when `None`, so both forms
    /// serialize identically when the default behavior is wanted. The nested
    /// filter is distinct from the top-level `.filter()` builder (which is
    /// mutually exclusive with `search`) and is likewise omitted on the wire
    /// when `None`.
    pub fn search(mut self, index: &str, query: &str, opts: impl Into<SearchOpts>) -> Self {
        let opts = opts.into();
        self.q.search = Some(SearchQuery {
            index: index.into(),
            query: query.into(),
            filter: opts.filter,
            mode: opts.mode,
            snippet: opts.snippet,
        });
        self
    }

    /// Vector-similarity `vectorSearch` over a declared vector index. The server
    /// ranks by cosine distance and applies the carried `limit`; `filter` is an
    /// optional `FilterExpr` (the db-side `filter()` DSL) that narrows the
    /// vector search `WHERE` server-side. Standalone terminal — unlike `search`,
    /// it carries its own `limit` and does NOT compose with `take`/`collect`
    /// (the server rejects `vectorSearch` combined with any other terminal).
    ///
    /// `opts` accepts any `Into<VectorSearchOpts>`: pass `()` to omit the
    /// filter (`.vector_search(idx, vec, lim, ())`), or a
    /// `VectorSearchOpts { filter }` literal to narrow results server-side via
    /// a `FilterExpr`. The nested filter is distinct from the top-level
    /// `.filter()` builder (which is mutually exclusive with `vectorSearch`)
    /// and is omitted on the wire when `None`, so both forms serialize
    /// identically when no filter is wanted.
    pub fn vector_search(
        mut self,
        index: &str,
        vector: Vec<f64>,
        limit: u32,
        opts: impl Into<VectorSearchOpts>,
    ) -> Self {
        let opts = opts.into();
        self.q.vector_search = Some(crate::wire::VectorSearchQuery {
            index: index.into(),
            vector,
            limit,
            filter: opts.filter,
        });
        self
    }

    /// Hybrid `hybridSearch` terminal: fuses full-text and vector ranking over
    /// the same table via Reciprocal Rank Fusion. The table must declare BOTH a
    /// search index and a vector index. Standalone terminal — like
    /// `vector_search`, it carries its own `limit` and does NOT compose with
    /// `take`/`collect` (the server rejects `hybridSearch` combined with any
    /// other terminal).
    ///
    /// `opts` accepts any `Into<HybridSearchOpts>`: pass `()` for the
    /// auto-select / server-default-`k` case
    /// (`.hybrid_search(query, vector, limit, ())`), or a `HybridSearchOpts`
    /// literal to name `search_index`/`vector_index`/`k` by field — no positional
    /// `Option` ordering. All three fields default to `None` and are omitted on
    /// the wire when absent.
    pub fn hybrid_search(
        mut self,
        query: &str,
        vector: Vec<f64>,
        limit: u32,
        opts: impl Into<HybridSearchOpts>,
    ) -> Self {
        let opts = opts.into();
        self.q.hybrid_search = Some(crate::wire::HybridSearchQuery {
            query: query.into(),
            vector,
            limit,
            search_index: opts.search_index,
            vector_index: opts.vector_index,
            k: opts.k,
        });
        self
    }

    /// Finish with the `take(N)` terminal.
    pub fn take(mut self, n: u32) -> Query {
        self.q.take = Some(n);
        self.q
    }
    /// Finish with the collect-all terminal (`Vec<T>`).
    pub fn collect(self) -> Query {
        self.q
    }
    /// Finish with the `unique` terminal (one row or `null`).
    pub fn unique(mut self) -> Query {
        self.q.unique = true;
        self.q
    }
    /// Finish with the `first` terminal (first row or `null`).
    pub fn first(mut self) -> Query {
        self.q.first = true;
        self.q
    }
    /// Finish with the `count` terminal (`i64`).
    pub fn count(mut self) -> Query {
        self.q.count = true;
        self.q
    }
    /// Distinct-values terminal: returns the unique values of the index field
    /// immediately after the `eq` prefix over the matching set (an array of
    /// scalar values). Server rejects when no index is set or the eq prefix
    /// consumes every index field; mutually exclusive with every other terminal
    /// except `eq`/range bounds/`filter`. The caller passes the desired Vec
    /// type to `parse_result` (typically `Vec<serde_json::Value>` or
    /// `Vec<String>`/`Vec<f64>` for a homogeneous index field).
    pub fn distinct(mut self) -> Query {
        self.q.distinct = true;
        self.q
    }
    /// Aggregate terminal: runs `<op>` (SUM/AVG/MIN/MAX) over the index field
    /// immediately after the `eq` prefix. Without `group_by`, returns one scalar
    /// (`null` if no rows match) — parse with `parse_result::<Option<serde_json::Value>>`
    /// or `Option<f64>` for a numeric aggregate. With `group_by = true`, groups by
    /// the index field after the eq prefix and aggregates the one after that,
    /// returning `Vec<AggregateGroup>` (`{key, value}` rows ordered by group key).
    /// `sum`/`avg` require a numeric aggregate field; the server rejects
    /// non-numeric, no-index, or no-field-beyond-prefix cases. Mutually exclusive
    /// with every other terminal except `eq`/range bounds/`filter`.
    pub fn aggregate(mut self, op: AggregateOp, group_by: bool) -> Query {
        self.q.aggregate = Some(AggregateSpec { op, group_by });
        self.q
    }
    /// Finish with the cursor-pagination terminal. Pass the previous page's
    /// `next_cursor` (or `None` to start) and the page size.
    pub fn paginate(mut self, cursor: Option<&str>, num_items: u32) -> Query {
        self.q.paginate = Some(Paginate {
            cursor: cursor.map(|c| c.into()),
            num_items,
        });
        self.q
    }
    /// Finish with no terminal wrapper — return the raw `Query`.
    pub fn build(self) -> Query {
        self.q
    }
}

// `Query: From<TableQuery>` lets `run(impl Into<Query>)` accept a `TableQuery`
// directly; `Query: From<Query>` is already provided by the standard library's
// reflexive `impl<T> From<T> for T`, so it is not re-declared here.
impl From<TableQuery> for Query {
    fn from(b: TableQuery) -> Query {
        b.q
    }
}

/// Deserialize the server's untagged `QueryResult` payload into the caller's type.
/// Shape is chosen by the terminal used: array → `Vec<T>`, object/null → `Option<T>`,
/// number → `i64`, `{docs,nextCursor?}` → `Paginated<T>`. serde does the discrimination
/// from `T` directly, so one generic covers all four.
pub fn parse_result<T: DeserializeOwned>(
    value: serde_json::Value,
) -> Result<T, crate::error::RtDbError> {
    serde_json::from_value::<T>(value)
        .map_err(|e| crate::error::RtDbError::internal(format!("invalid query result: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::AggregateGroup;
    use serde_json::json;

    #[test]
    fn bare_table_query() {
        let q = TableQuery::new("items").collect();
        assert_eq!(serde_json::to_value(&q).unwrap(), json!({"table":"items"}));
    }

    #[test]
    fn point_get() {
        let q = TableQuery::get("items", "abc");
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"table":"items","get":"abc"})
        );
    }

    #[test]
    fn index_eq_unique() {
        let q = TableQuery::new("items")
            .with_index("by_project", &[json!("p1")])
            .unique();
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"table":"items","index":"by_project","eq":["p1"],"unique":true})
        );
    }

    #[test]
    fn range_order_take() {
        let q = TableQuery::new("items")
            .with_index("by_project", &[json!("p1")])
            .gte("a")
            .lte("m")
            .order(Order::Desc)
            .take(10);
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"table":"items","index":"by_project","eq":["p1"],"gte":"a","lte":"m","order":"desc","take":10})
        );
    }

    #[test]
    fn count_terminal() {
        let q = TableQuery::new("items")
            .with_index("by_status", &[json!("backlog")])
            .count();
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"table":"items","index":"by_status","eq":["backlog"],"count":true})
        );
    }

    #[test]
    fn distinct_terminal() {
        // `distinct` consumes one eq prefix value and distincts on the next
        // index field. Wire shape: omitted unless true (skip_serializing_if).
        let q = TableQuery::new("items")
            .with_index("by_project_and_status", &[json!("p1")])
            .distinct();
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"table":"items","index":"by_project_and_status","eq":["p1"],"distinct":true})
        );
    }

    #[test]
    fn aggregate_terminal() {
        // Aggregate without groupBy: bare `{op}` wire shape (groupBy defaults
        // false and is omitted on the wire by the rust-client mirror, which
        // round-trips back via `#[serde(default)]`).
        let q = TableQuery::new("items")
            .with_index("by_project_and_order", &[json!("p1")])
            .aggregate(AggregateOp::Sum, false);
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"table":"items","index":"by_project_and_order","eq":["p1"],"aggregate":{"op":"sum"}})
        );
    }

    #[test]
    fn aggregate_terminal_group_by() {
        // groupBy=true emits the camelCase flag on the wire.
        let q = TableQuery::new("items")
            .with_index("by_project_status_order", &[json!("p1")])
            .aggregate(AggregateOp::Sum, true);
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"table":"items","index":"by_project_status_order","eq":["p1"],"aggregate":{"op":"sum","groupBy":true}})
        );
    }

    #[test]
    fn parse_aggregate_scalar_from_value() {
        // Aggregate scalar result is a bare JSON value (number, string, or null
        // when no rows match) — server QueryResult::Aggregate.
        let n: serde_json::Value = parse_result(serde_json::json!(42)).unwrap();
        assert_eq!(n, json!(42));
        let null: serde_json::Value = parse_result(serde_json::Value::Null).unwrap();
        assert_eq!(null, serde_json::Value::Null);
    }

    #[test]
    fn parse_aggregate_groups_from_array() {
        // AggregateGroups result is `[{key, value}, ...]` (server
        // QueryResult::AggregateGroups). Decoded via `Vec<AggregateGroup>`.
        let rows: Vec<AggregateGroup> =
            parse_result(json!([{"key":"backlog","value":4}, {"key":"done","value":7}])).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].key, json!("backlog"));
        assert_eq!(rows[0].value, json!(4));
        assert_eq!(rows[1].key, json!("done"));
        assert_eq!(rows[1].value, json!(7));
    }

    #[test]
    fn parse_distinct_from_array() {
        // Distinct result is a JSON array of scalar values (server QueryResult::Distinct).
        let values: Vec<serde_json::Value> =
            parse_result(serde_json::json!(["alice", "bob", "carol"])).unwrap();
        assert_eq!(values, vec![json!("alice"), json!("bob"), json!("carol")]);
    }

    #[test]
    fn paginate_terminal() {
        let q = TableQuery::new("items")
            .with_index("by_status", &[json!("backlog")])
            .paginate(None, 20);
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"table":"items","index":"by_status","eq":["backlog"],"paginate":{"numItems":20}})
        );
    }

    #[test]
    fn parse_count_from_number() {
        let n: i64 = parse_result(serde_json::json!(42)).unwrap();
        assert_eq!(n, 42);
    }

    #[test]
    fn parse_docs_from_array() {
        let docs: Vec<serde_json::Value> =
            parse_result(serde_json::json!([{"_id":"a"},{"_id":"b"}])).unwrap();
        assert_eq!(docs.len(), 2);
    }

    #[test]
    fn parse_doc_from_object_or_null() {
        let some: Option<serde_json::Value> = parse_result(serde_json::json!({"_id":"a"})).unwrap();
        assert!(some.is_some());
        let none: Option<serde_json::Value> = parse_result(serde_json::Value::Null).unwrap();
        assert!(none.is_none());
    }

    #[test]
    fn parse_paginated() {
        let p: Paginated<serde_json::Value> =
            parse_result(json!({"docs":[{"_id":"a"}],"nextCursor":"zzz"})).unwrap();
        assert_eq!(p.docs.len(), 1);
        assert_eq!(p.next_cursor.as_deref(), Some("zzz"));
    }

    #[test]
    fn filter_builder_serializes_predicate() {
        let q = TableQuery::new("items")
            .filter(FilterExpr::Eq {
                field: "status".into(),
                value: json!("done"),
            })
            .collect();
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"table":"items","filter":{"op":"eq","field":"status","value":"done"}})
        );
    }

    #[test]
    fn filter_composes_with_index_and_take() {
        let q = TableQuery::new("items")
            .with_index("by_project", &[json!("p1")])
            .filter(FilterExpr::Gt {
                field: "order".into(),
                value: json!(0),
            })
            .take(10);
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"table":"items","index":"by_project","eq":["p1"],"filter":{"op":"gt","field":"order","value":0},"take":10})
        );
    }

    #[test]
    fn filter_nests_combinators() {
        let q = TableQuery::new("items")
            .filter(FilterExpr::Or {
                exprs: vec![
                    FilterExpr::In {
                        field: "status".into(),
                        values: vec![json!("blocked"), json!("backlog")],
                    },
                    FilterExpr::Lte {
                        field: "order".into(),
                        value: json!(3),
                    },
                ],
            })
            .collect();
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"table":"items","filter":{"op":"or","exprs":[
                {"op":"in","field":"status","values":["blocked","backlog"]},
                {"op":"lte","field":"order","value":3}
            ]}})
        );
    }

    #[test]
    fn search_builder_serializes_terminal() {
        // `()` → `SearchOpts::default()` (filter None) → omitted on the wire,
        // so the terminal serializes identically to the pre-filter shape.
        let q = TableQuery::new("notes")
            .search("search_content", "hello world", ())
            .take(10);
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"table":"notes","search":{"index":"search_content","query":"hello world"},"take":10})
        );
    }

    #[test]
    fn search_with_opts_struct_carries_filter() {
        // Named-field opts bag — a `FilterExpr` narrows the search server-side.
        // Non-None filter is emitted on the wire, nested under the terminal.
        let q = TableQuery::new("messages")
            .search(
                "search_body",
                "hello",
                SearchOpts {
                    filter: Some(FilterExpr::And {
                        exprs: vec![
                            FilterExpr::Eq {
                                field: "channel".into(),
                                value: "#general".into(),
                            },
                            FilterExpr::Gt {
                                field: "createdAt".into(),
                                value: 1_780_000_000_000_i64.into(),
                            },
                        ],
                    }),
                    mode: None,
                    snippet: None,
                },
            )
            .take(10);
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({
                "table":"messages",
                "search":{
                    "index":"search_body",
                    "query":"hello",
                    "filter":{
                        "op":"and",
                        "exprs":[
                            {"op":"eq","field":"channel","value":"#general"},
                            {"op":"gt","field":"createdAt","value":1780000000000_i64}
                        ]
                    }
                },
                "take":10
            })
        );
    }

    #[test]
    fn search_with_opts_struct_carries_mode() {
        // `mode` set → emitted on the wire as the lowercase variant, nested on
        // the terminal (FM-30). Default `()` stays byte-identical to the
        // pre-mode shape (covered by `search_builder_serializes_terminal`).
        let q = TableQuery::new("notes")
            .search(
                "search_body",
                "conv",
                SearchOpts {
                    filter: None,
                    mode: Some(SearchMode::Trgm),
                    snippet: None,
                },
            )
            .take(10);
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({
                "table":"notes",
                "search":{"index":"search_body","query":"conv","mode":"trgm"},
                "take":10
            })
        );

        // Explicit `Tsquery` is honored when a caller names it (the server
        // accepts it; clients never emit it implicitly).
        let explicit = TableQuery::new("notes")
            .search(
                "search_body",
                "conv",
                SearchOpts {
                    filter: None,
                    mode: Some(SearchMode::Tsquery),
                    snippet: None,
                },
            )
            .take(10);
        assert_eq!(
            serde_json::to_value(&explicit).unwrap(),
            json!({
                "table":"notes",
                "search":{"index":"search_body","query":"conv","mode":"tsquery"},
                "take":10
            })
        );
    }

    #[test]
    fn search_with_opts_struct_carries_snippet() {
        // `snippet` set → emitted on the wire nested on the terminal (FM-31),
        // alongside operator-syntax query text (plain string bytes). Default
        // `()` stays byte-identical to the pre-snippet shape (covered by
        // `search_builder_serializes_terminal`).
        let q = TableQuery::new("notes")
            .search(
                "search_body",
                "\"exact phrase\" or -excluded",
                SearchOpts {
                    filter: None,
                    mode: None,
                    snippet: Some(true),
                },
            )
            .take(10);
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({
                "table":"notes",
                "search":{
                    "index":"search_body",
                    "query":"\"exact phrase\" or -excluded",
                    "snippet":true
                },
                "take":10
            })
        );
    }

    #[test]
    fn vector_builder_serializes_terminal() {
        // `()` → `VectorSearchOpts::default()` (filter None) → omitted on the
        // wire. Wire key is camelCase `vectorSearch` (matches server).
        let q = TableQuery::new("docs")
            .vector_search("by_embedding", vec![1.0, 0.0, 0.0], 5, ())
            .take(10);
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"table":"docs","vectorSearch":{"index":"by_embedding","vector":[1.0,0.0,0.0],"limit":5},"take":10})
        );
    }

    #[test]
    fn vector_search_omits_filter_via_unit() {
        // `()` → `VectorSearchOpts::default()` (filter None) → omitted on the
        // wire. Byte-identical to the bare form above.
        let q = TableQuery::new("docs")
            .vector_search("by_embedding", vec![1.0, 0.0, 0.0], 5, ())
            .take(10);
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"table":"docs","vectorSearch":{"index":"by_embedding","vector":[1.0,0.0,0.0],"limit":5},"take":10})
        );
    }

    #[test]
    fn vector_search_with_opts_struct_carries_filter() {
        // Named-field opts bag — a `FilterExpr` narrows the vector search
        // server-side. Non-empty filter is emitted on the wire as the
        // `FilterExpr` tagged shape, mirroring the `search` terminal.
        let q = TableQuery::new("docs")
            .vector_search(
                "by_embedding",
                vec![1.0, 0.0, 0.0],
                5,
                VectorSearchOpts {
                    filter: Some(FilterExpr::Eq {
                        field: "userId".into(),
                        value: "u1".into(),
                    }),
                },
            )
            .take(10);
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({
                "table":"docs",
                "vectorSearch":{
                    "index":"by_embedding",
                    "vector":[1.0,0.0,0.0],
                    "limit":5,
                    "filter":{"op":"eq","field":"userId","value":"u1"}
                },
                "take":10
            })
        );
    }

    #[test]
    fn hybrid_builder_serializes_terminal() {
        // `()` → `HybridSearchOpts::default()` (all None) — optionals omitted on
        // the wire. Replaces the legacy `None, None, None` positional tail.
        let q = TableQuery::new("docs")
            .hybrid_search("hello world", vec![1.0, 0.0, 0.0], 5, ())
            .take(10);
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"table":"docs","hybridSearch":{"query":"hello world","vector":[1.0,0.0,0.0],"limit":5},"take":10})
        );
        // Named-field opts bag — `searchIndex`/`vectorIndex`/`k` set by field,
        // no positional-`Option` mis-ordering. Round-trips as camelCase keys.
        let q_full = TableQuery::new("docs")
            .hybrid_search(
                "hello",
                vec![1.0, 0.0, 0.0],
                5,
                HybridSearchOpts {
                    search_index: Some("search_body".into()),
                    vector_index: Some("by_embedding".into()),
                    k: Some(42),
                },
            )
            .collect();
        assert_eq!(
            serde_json::to_value(&q_full).unwrap(),
            json!({"table":"docs","hybridSearch":{
                "query":"hello","vector":[1.0,0.0,0.0],"limit":5,
                "searchIndex":"search_body","vectorIndex":"by_embedding","k":42
            }})
        );
    }

    #[test]
    fn hybrid_search_opts_partial_defaults_to_none_on_wire() {
        // Setting only `k` leaves the index names as `None` (auto-selected
        // server-side) and omits them on the wire — the named-field shape makes
        // the partial spec unambiguous, unlike the old positional tail.
        let q = TableQuery::new("docs")
            .hybrid_search(
                "hi",
                vec![1.0, 0.0, 0.0],
                5,
                HybridSearchOpts {
                    k: Some(7),
                    ..Default::default()
                },
            )
            .collect();
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"table":"docs","hybridSearch":{"query":"hi","vector":[1.0,0.0,0.0],"limit":5,"k":7}})
        );
    }

    #[test]
    fn bare_query_omits_filter_and_search() {
        // A query with neither filter nor search omits both keys (skip_serializing_if),
        // so existing request shapes are unchanged.
        let q = TableQuery::new("items").collect();
        let v = serde_json::to_value(&q).unwrap();
        assert_eq!(v, json!({"table":"items"}));
        // The camelCase `vectorSearch` key is also absent on a bare query.
        assert!(
            !v.as_object()
                .expect("query is object")
                .contains_key("vectorSearch")
        );
    }
}
