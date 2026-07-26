//! Query DSL: builds the exact `Query` JSON the server expects, and parses untagged results.

use crate::wire::{FilterExpr, SearchQuery};
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
    /// ranks by cosine distance and applies `limit`; `filter` is an eq-map over
    /// the index's declared `filterFields`. Terminal — composes only with the
    /// trailing terminal (`take`/`collect`/etc.), like `search`.
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
