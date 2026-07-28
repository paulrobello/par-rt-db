//! Query DSL: builds the exact `Query` JSON the server expects, and parses untagged results.

use crate::wire::{AggregateOp, AggregateSpec, FilterExpr, SearchQuery};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Order {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Paginate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    pub num_items: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Query {
    pub table: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub get: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub eq: Vec<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gt: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gte: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lt: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lte: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<Order>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub take: Option<u32>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub unique: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub first: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub count: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub distinct: bool,
    /// Aggregate terminal: SUM/AVG/MIN/MAX over the index field after the eq
    /// prefix; `group_by` shifts to a grouped aggregate. Mutually exclusive with
    /// every other terminal except `eq`/range bounds/`filter`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregate: Option<AggregateSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Paginated<T> {
    pub docs: Vec<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// A built query is just the wire `Query` (terminals consume the builder).
pub struct TableQuery {
    q: Query,
}

impl TableQuery {
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

    pub fn with_index(mut self, index: &str, eq: &[serde_json::Value]) -> Self {
        self.q.index = Some(index.into());
        self.q.eq = eq.to_vec();
        self
    }
    pub fn gt(mut self, v: impl Into<serde_json::Value>) -> Self {
        self.q.gt = Some(v.into());
        self
    }
    pub fn gte(mut self, v: impl Into<serde_json::Value>) -> Self {
        self.q.gte = Some(v.into());
        self
    }
    pub fn lt(mut self, v: impl Into<serde_json::Value>) -> Self {
        self.q.lt = Some(v.into());
        self
    }
    pub fn lte(mut self, v: impl Into<serde_json::Value>) -> Self {
        self.q.lte = Some(v.into());
        self
    }
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
    /// with `take` (e.g. `.search("idx", "text").take(10)`); the server rejects
    /// every other terminal alongside it.
    pub fn search(mut self, index: &str, query: &str) -> Self {
        self.q.search = Some(SearchQuery {
            index: index.into(),
            query: query.into(),
        });
        self
    }

    /// Vector-similarity `vectorSearch` over a declared vector index. The server
    /// ranks by cosine distance and applies the carried `limit`; `filter` is an
    /// eq-map over the index's declared `filterFields`. Standalone terminal —
    /// unlike `search`, it carries its own `limit` and does NOT compose with
    /// `take`/`collect` (the server rejects `vectorSearch` combined with any
    /// other terminal).
    pub fn vector_search(
        mut self,
        index: &str,
        vector: Vec<f64>,
        limit: u32,
        filter: BTreeMap<String, serde_json::Value>,
    ) -> Self {
        self.q.vector_search = Some(crate::wire::VectorSearchQuery {
            index: index.into(),
            vector,
            limit,
            filter,
        });
        self
    }

    /// Hybrid `hybridSearch` terminal: fuses full-text and vector ranking over
    /// the same table via Reciprocal Rank Fusion. The table must declare BOTH a
    /// search index and a vector index. `search_index`/`vector_index` optionally
    /// name the indexes (auto-selected server-side when `None`); `k` is the RRF
    /// constant (default 60). Standalone terminal — like `vector_search`, it
    /// carries its own `limit` and does NOT compose with `take`/`collect` (the
    /// server rejects `hybridSearch` combined with any other terminal).
    pub fn hybrid_search(
        mut self,
        query: &str,
        vector: Vec<f64>,
        limit: u32,
        search_index: Option<&str>,
        vector_index: Option<&str>,
        k: Option<u32>,
    ) -> Self {
        self.q.hybrid_search = Some(crate::wire::HybridSearchQuery {
            query: query.into(),
            vector,
            limit,
            search_index: search_index.map(|s| s.into()),
            vector_index: vector_index.map(|s| s.into()),
            k,
        });
        self
    }

    pub fn take(mut self, n: u32) -> Query {
        self.q.take = Some(n);
        self.q
    }
    pub fn collect(self) -> Query {
        self.q
    }
    pub fn unique(mut self) -> Query {
        self.q.unique = true;
        self.q
    }
    pub fn first(mut self) -> Query {
        self.q.first = true;
        self.q
    }
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
    pub fn paginate(mut self, cursor: Option<&str>, num_items: u32) -> Query {
        self.q.paginate = Some(Paginate {
            cursor: cursor.map(|c| c.into()),
            num_items,
        });
        self.q
    }
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
    use std::collections::BTreeMap;

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
        let q = TableQuery::new("notes")
            .search("search_content", "hello world")
            .take(10);
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"table":"notes","search":{"index":"search_content","query":"hello world"},"take":10})
        );
    }

    #[test]
    fn vector_builder_serializes_terminal() {
        let q = TableQuery::new("docs")
            .vector_search("by_embedding", vec![1.0, 0.0, 0.0], 5, BTreeMap::new())
            .take(10);
        // Wire key is camelCase `vectorSearch` (matches server); empty `filter`
        // is skipped on the wire.
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"table":"docs","vectorSearch":{"index":"by_embedding","vector":[1.0,0.0,0.0],"limit":5},"take":10})
        );
    }

    #[test]
    fn hybrid_builder_serializes_terminal() {
        // Required fields only — optionals omitted on the wire.
        let q = TableQuery::new("docs")
            .hybrid_search("hello world", vec![1.0, 0.0, 0.0], 5, None, None, None)
            .take(10);
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"table":"docs","hybridSearch":{"query":"hello world","vector":[1.0,0.0,0.0],"limit":5},"take":10})
        );
        // Explicit searchIndex/vectorIndex/k round-trip as camelCase keys.
        let q_full = TableQuery::new("docs")
            .hybrid_search(
                "hello",
                vec![1.0, 0.0, 0.0],
                5,
                Some("search_body"),
                Some("by_embedding"),
                Some(42),
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
