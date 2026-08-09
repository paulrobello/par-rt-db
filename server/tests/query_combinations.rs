//! QA-001 / QA-002 cross-client combination-matrix safety net.
//!
//! Enumerates the validation cascade's accept/reject decision for every
//! terminal × peer combination in `execute_query`, and asserts the outcome
//! matches the expectation encoded below. The TS client mirror
//! (`ts-client/tests/query_combinations.test.ts`) runs the SAME matrix against
//! `InMemoryRtDbClient.executeQuery`; the two must agree. Adding a new
//! terminal? Add cases here AND in the TS mirror — the matrix exists so the
//! next terminal addition fails the gate on whichever side forgets (this is
//! exactly the drift class that produced QA-001: the TS `get` guard omitted
//! `filter`/`search`/`vectorSearch` and silently returned the wrong result).

mod common;

use std::sync::Arc;

use common::test_state;
use rtdb_server::AppState;
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::ddl::push_schema;
use rtdb_server::error::ErrorCode;
use rtdb_server::query::{
    AggregateOp, AggregateSpec, FilterExpr, HybridSearchQuery, Order, Paginate, Query, SearchQuery,
    VectorSearchQuery, execute_query,
};
use rtdb_server::schema::SchemaDef;
use serde_json::json;

/// Schema with btree + search + vector indexes on one table — enough to drive
/// every terminal in the matrix without separate schemas per terminal family.
/// The 2-field `by_title_count` index covers aggregate's `eq prefix + aggregate
/// field` shape and the groupBy 2-fields-beyond-prefix shape.
fn matrix_schema_json() -> serde_json::Value {
    serde_json::json!({"tables":{"items":{
        "fields":{
            "title":{"type":"string"},
            "body":{"type":"string"},
            "count":{"type":"number"},
            "embedding":{"type":"vector","dimensions":3}
        },
        "indexes":[
            {"name":"by_title","fields":["title"]},
            {"name":"by_title_count","fields":["title","count"]},
            {"name":"search_body","fields":["title","body"],"search":true},
            {"name":"by_embedding","fields":["embedding"],"vector":{"dimensions":3}}
        ]
    }}})
}

fn matrix_schema() -> SchemaDef {
    serde_json::from_value(matrix_schema_json()).expect("parse matrix schema")
}

async fn matrix_db(state: &Arc<AppState>) -> (common::TestDb, SchemaDef) {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create matrix-test database");
    let schema = matrix_schema();
    push_schema(&state.pool, &name, schema.clone())
        .await
        .expect("push matrix-test schema");
    (common::wrap_test_db(name), schema)
}

const ID: &str = "0123456789abcdef0123456789abcdef";

/// Base query with every field defaulted; cases mutate it.
fn base_query() -> Query {
    Query {
        table: "items".to_string(),
        get: None,
        index: None,
        eq: vec![],
        gt: None,
        gte: None,
        lt: None,
        lte: None,
        order: None,
        take: None,
        unique: false,
        first: false,
        count: false,
        distinct: false,
        paginate: None,
        filter: None,
        search: None,
        vector_search: None,
        hybrid_search: None,
        aggregate: None,
    }
}

fn filter_eq_title_x() -> FilterExpr {
    FilterExpr::Eq {
        field: "title".to_string(),
        value: json!("x"),
    }
}

fn search_body_x() -> SearchQuery {
    SearchQuery {
        index: "search_body".to_string(),
        query: "x".to_string(),
        filter: None,
    }
}

fn vector_embedding_limit_1() -> VectorSearchQuery {
    VectorSearchQuery {
        index: "by_embedding".to_string(),
        vector: vec![0.0, 0.0, 0.0],
        limit: 1,
        filter: std::collections::BTreeMap::new(),
    }
}

fn hybrid_query_database_x() -> HybridSearchQuery {
    HybridSearchQuery {
        query: "x".to_string(),
        vector: vec![0.0, 0.0, 0.0],
        limit: 1,
        search_index: None,
        vector_index: None,
        k: None,
    }
}

fn paginate_num_1() -> Paginate {
    Paginate {
        cursor: None,
        num_items: 1,
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Outcome {
    Accept,
    Reject,
}

/// A single matrix case: mutates a base query and expects a cascade outcome.
struct Case {
    name: &'static str,
    build: fn(&mut Query),
    expected: Outcome,
}

const CASES: &[Case] = &[
    // ============ Solo accepts (each terminal alone is valid baseline) ============
    Case {
        name: "solo: get",
        build: solo_get,
        expected: Outcome::Accept,
    },
    Case {
        name: "solo: collect",
        build: noop,
        expected: Outcome::Accept,
    },
    Case {
        name: "solo: index",
        build: solo_index,
        expected: Outcome::Accept,
    },
    Case {
        name: "solo: eq",
        build: solo_eq,
        expected: Outcome::Accept,
    },
    Case {
        name: "solo: gt",
        build: solo_gt,
        expected: Outcome::Accept,
    },
    Case {
        name: "solo: gte",
        build: solo_gte,
        expected: Outcome::Accept,
    },
    Case {
        name: "solo: lt",
        build: solo_lt,
        expected: Outcome::Accept,
    },
    Case {
        name: "solo: lte",
        build: solo_lte,
        expected: Outcome::Accept,
    },
    Case {
        name: "solo: order",
        build: solo_order,
        expected: Outcome::Accept,
    },
    Case {
        name: "solo: take",
        build: solo_take,
        expected: Outcome::Accept,
    },
    Case {
        name: "solo: unique",
        build: solo_unique,
        expected: Outcome::Accept,
    },
    Case {
        name: "solo: first",
        build: solo_first,
        expected: Outcome::Accept,
    },
    Case {
        name: "solo: count",
        build: solo_count,
        expected: Outcome::Accept,
    },
    Case {
        name: "solo: distinct",
        build: solo_distinct,
        expected: Outcome::Accept,
    },
    Case {
        name: "solo: paginate",
        build: solo_paginate,
        expected: Outcome::Accept,
    },
    Case {
        name: "solo: filter",
        build: solo_filter,
        expected: Outcome::Accept,
    },
    Case {
        name: "solo: search",
        build: solo_search,
        expected: Outcome::Accept,
    },
    Case {
        name: "solo: vectorSearch",
        build: solo_vector,
        expected: Outcome::Accept,
    },
    Case {
        name: "solo: hybridSearch",
        build: solo_hybrid,
        expected: Outcome::Accept,
    },
    // ============ get rejects every peer (QA-001: last 3 are the drift) ============
    Case {
        name: "get+index",
        build: get_index,
        expected: Outcome::Reject,
    },
    Case {
        name: "get+eq",
        build: get_eq,
        expected: Outcome::Reject,
    },
    Case {
        name: "get+gt",
        build: get_gt,
        expected: Outcome::Reject,
    },
    Case {
        name: "get+gte",
        build: get_gte,
        expected: Outcome::Reject,
    },
    Case {
        name: "get+lt",
        build: get_lt,
        expected: Outcome::Reject,
    },
    Case {
        name: "get+lte",
        build: get_lte,
        expected: Outcome::Reject,
    },
    Case {
        name: "get+order",
        build: get_order,
        expected: Outcome::Reject,
    },
    Case {
        name: "get+take",
        build: get_take,
        expected: Outcome::Reject,
    },
    Case {
        name: "get+unique",
        build: get_unique,
        expected: Outcome::Reject,
    },
    Case {
        name: "get+first",
        build: get_first,
        expected: Outcome::Reject,
    },
    Case {
        name: "get+count",
        build: get_count,
        expected: Outcome::Reject,
    },
    Case {
        name: "get+paginate",
        build: get_paginate,
        expected: Outcome::Reject,
    },
    Case {
        name: "get+filter",
        build: get_filter,
        expected: Outcome::Reject,
    },
    Case {
        name: "get+search",
        build: get_search,
        expected: Outcome::Reject,
    },
    Case {
        name: "get+vectorSearch",
        build: get_vector,
        expected: Outcome::Reject,
    },
    Case {
        name: "get+hybridSearch",
        build: get_hybrid,
        expected: Outcome::Reject,
    },
    // ============ unique rejects take, order ============
    Case {
        name: "unique+take",
        build: unique_take,
        expected: Outcome::Reject,
    },
    Case {
        name: "unique+order",
        build: unique_order,
        expected: Outcome::Reject,
    },
    // ============ first rejects unique, take ============
    Case {
        name: "first+unique",
        build: first_unique,
        expected: Outcome::Reject,
    },
    Case {
        name: "first+take",
        build: first_take,
        expected: Outcome::Reject,
    },
    // ============ count rejects unique, take, first, order, distinct ============
    Case {
        name: "count+unique",
        build: count_unique,
        expected: Outcome::Reject,
    },
    Case {
        name: "count+take",
        build: count_take,
        expected: Outcome::Reject,
    },
    Case {
        name: "count+first",
        build: count_first,
        expected: Outcome::Reject,
    },
    Case {
        name: "count+order",
        build: count_order,
        expected: Outcome::Reject,
    },
    Case {
        name: "count+distinct",
        build: count_distinct,
        expected: Outcome::Reject,
    },
    // ============ distinct rejects get, take, unique, first, count, order,
    //              paginate, search, vectorSearch (standalone terminal like count) ============
    Case {
        name: "distinct+get",
        build: distinct_get,
        expected: Outcome::Reject,
    },
    Case {
        name: "distinct+take",
        build: distinct_take,
        expected: Outcome::Reject,
    },
    Case {
        name: "distinct+unique",
        build: distinct_unique,
        expected: Outcome::Reject,
    },
    Case {
        name: "distinct+first",
        build: distinct_first,
        expected: Outcome::Reject,
    },
    Case {
        name: "distinct+count",
        build: distinct_count,
        expected: Outcome::Reject,
    },
    Case {
        name: "distinct+order",
        build: distinct_order,
        expected: Outcome::Reject,
    },
    Case {
        name: "distinct+paginate",
        build: distinct_paginate,
        expected: Outcome::Reject,
    },
    Case {
        name: "distinct+search",
        build: distinct_search,
        expected: Outcome::Reject,
    },
    Case {
        name: "distinct+vectorSearch",
        build: distinct_vector,
        expected: Outcome::Reject,
    },
    Case {
        name: "distinct+hybridSearch",
        build: distinct_hybrid,
        expected: Outcome::Reject,
    },
    // ============ aggregate rejects get, take, unique, first, count, order,
    //              paginate, search, vectorSearch (standalone terminal like
    //              count/distinct); composes with index/eq/range/filter ============
    Case {
        name: "aggregate+get",
        build: aggregate_get,
        expected: Outcome::Reject,
    },
    Case {
        name: "aggregate+take",
        build: aggregate_take,
        expected: Outcome::Reject,
    },
    Case {
        name: "aggregate+unique",
        build: aggregate_unique,
        expected: Outcome::Reject,
    },
    Case {
        name: "aggregate+first",
        build: aggregate_first,
        expected: Outcome::Reject,
    },
    Case {
        name: "aggregate+count",
        build: aggregate_count,
        expected: Outcome::Reject,
    },
    Case {
        name: "aggregate+distinct",
        build: aggregate_distinct,
        expected: Outcome::Reject,
    },
    Case {
        name: "aggregate+order",
        build: aggregate_order,
        expected: Outcome::Reject,
    },
    Case {
        name: "aggregate+paginate",
        build: aggregate_paginate,
        expected: Outcome::Reject,
    },
    Case {
        name: "aggregate+search",
        build: aggregate_search,
        expected: Outcome::Reject,
    },
    Case {
        name: "aggregate+vectorSearch",
        build: aggregate_vector,
        expected: Outcome::Reject,
    },
    Case {
        name: "aggregate+hybridSearch",
        build: aggregate_hybrid,
        expected: Outcome::Reject,
    },
    // solo aggregate (no peers) and aggregate+eq/range/filter compositions accept.
    Case {
        name: "solo: aggregate",
        build: solo_aggregate,
        expected: Outcome::Accept,
    },
    Case {
        name: "compose: aggregate+eq",
        build: aggregate_eq,
        expected: Outcome::Accept,
    },
    Case {
        name: "compose: aggregate+filter",
        build: aggregate_filter,
        expected: Outcome::Accept,
    },
    // ============ paginate rejects count, unique, first, take (get covered above) ============
    Case {
        name: "paginate+count",
        build: paginate_count,
        expected: Outcome::Reject,
    },
    Case {
        name: "paginate+unique",
        build: paginate_unique,
        expected: Outcome::Reject,
    },
    Case {
        name: "paginate+first",
        build: paginate_first,
        expected: Outcome::Reject,
    },
    Case {
        name: "paginate+take",
        build: paginate_take,
        expected: Outcome::Reject,
    },
    // ============ range-bound incompatibilities ============
    Case {
        name: "gt+gte",
        build: gt_gte,
        expected: Outcome::Reject,
    },
    Case {
        name: "lt+lte",
        build: lt_lte,
        expected: Outcome::Reject,
    },
    // ============ vectorSearch rejects every peer (take included) ============
    Case {
        name: "vectorSearch+index",
        build: vs_index,
        expected: Outcome::Reject,
    },
    Case {
        name: "vectorSearch+eq",
        build: vs_eq,
        expected: Outcome::Reject,
    },
    Case {
        name: "vectorSearch+gt",
        build: vs_gt,
        expected: Outcome::Reject,
    },
    Case {
        name: "vectorSearch+gte",
        build: vs_gte,
        expected: Outcome::Reject,
    },
    Case {
        name: "vectorSearch+lt",
        build: vs_lt,
        expected: Outcome::Reject,
    },
    Case {
        name: "vectorSearch+lte",
        build: vs_lte,
        expected: Outcome::Reject,
    },
    Case {
        name: "vectorSearch+order",
        build: vs_order,
        expected: Outcome::Reject,
    },
    Case {
        name: "vectorSearch+unique",
        build: vs_unique,
        expected: Outcome::Reject,
    },
    Case {
        name: "vectorSearch+first",
        build: vs_first,
        expected: Outcome::Reject,
    },
    Case {
        name: "vectorSearch+count",
        build: vs_count,
        expected: Outcome::Reject,
    },
    Case {
        name: "vectorSearch+paginate",
        build: vs_paginate,
        expected: Outcome::Reject,
    },
    Case {
        name: "vectorSearch+filter",
        build: vs_filter,
        expected: Outcome::Reject,
    },
    Case {
        name: "vectorSearch+search",
        build: vs_search,
        expected: Outcome::Reject,
    },
    Case {
        name: "vectorSearch+take",
        build: vs_take,
        expected: Outcome::Reject,
    },
    Case {
        name: "vectorSearch+hybridSearch",
        build: vs_hybrid,
        expected: Outcome::Reject,
    },
    // ============ search rejects every peer except take ============
    Case {
        name: "search+index",
        build: sr_index,
        expected: Outcome::Reject,
    },
    Case {
        name: "search+eq",
        build: sr_eq,
        expected: Outcome::Reject,
    },
    Case {
        name: "search+gt",
        build: sr_gt,
        expected: Outcome::Reject,
    },
    Case {
        name: "search+gte",
        build: sr_gte,
        expected: Outcome::Reject,
    },
    Case {
        name: "search+lt",
        build: sr_lt,
        expected: Outcome::Reject,
    },
    Case {
        name: "search+lte",
        build: sr_lte,
        expected: Outcome::Reject,
    },
    Case {
        name: "search+order",
        build: sr_order,
        expected: Outcome::Reject,
    },
    Case {
        name: "search+unique",
        build: sr_unique,
        expected: Outcome::Reject,
    },
    Case {
        name: "search+first",
        build: sr_first,
        expected: Outcome::Reject,
    },
    Case {
        name: "search+count",
        build: sr_count,
        expected: Outcome::Reject,
    },
    Case {
        name: "search+paginate",
        build: sr_paginate,
        expected: Outcome::Reject,
    },
    Case {
        name: "search+filter",
        build: sr_filter,
        expected: Outcome::Reject,
    },
    Case {
        name: "search+vectorSearch",
        build: sr_vector,
        expected: Outcome::Reject,
    },
    Case {
        name: "search+hybridSearch",
        build: sr_hybrid,
        expected: Outcome::Reject,
    },
    // ============ hybridSearch rejects every peer (standalone, like
    //              vectorSearch — it carries its own limit and IS the search+
    //              vector combination, so neither composes with it) ============
    Case {
        name: "hybridSearch+index",
        build: hs_index,
        expected: Outcome::Reject,
    },
    Case {
        name: "hybridSearch+eq",
        build: hs_eq,
        expected: Outcome::Reject,
    },
    Case {
        name: "hybridSearch+gt",
        build: hs_gt,
        expected: Outcome::Reject,
    },
    Case {
        name: "hybridSearch+gte",
        build: hs_gte,
        expected: Outcome::Reject,
    },
    Case {
        name: "hybridSearch+lt",
        build: hs_lt,
        expected: Outcome::Reject,
    },
    Case {
        name: "hybridSearch+lte",
        build: hs_lte,
        expected: Outcome::Reject,
    },
    Case {
        name: "hybridSearch+order",
        build: hs_order,
        expected: Outcome::Reject,
    },
    Case {
        name: "hybridSearch+unique",
        build: hs_unique,
        expected: Outcome::Reject,
    },
    Case {
        name: "hybridSearch+first",
        build: hs_first,
        expected: Outcome::Reject,
    },
    Case {
        name: "hybridSearch+count",
        build: hs_count,
        expected: Outcome::Reject,
    },
    Case {
        name: "hybridSearch+distinct",
        build: hs_distinct,
        expected: Outcome::Reject,
    },
    Case {
        name: "hybridSearch+aggregate",
        build: hs_aggregate,
        expected: Outcome::Reject,
    },
    Case {
        name: "hybridSearch+paginate",
        build: hs_paginate,
        expected: Outcome::Reject,
    },
    Case {
        name: "hybridSearch+filter",
        build: hs_filter,
        expected: Outcome::Reject,
    },
    Case {
        name: "hybridSearch+take",
        build: hs_take,
        expected: Outcome::Reject,
    },
    // ============ composition accepts (smoke that valid combos don't false-reject) ============
    Case {
        name: "compose: search+take",
        build: search_take,
        expected: Outcome::Accept,
    },
    Case {
        name: "compose: index+take",
        build: index_take,
        expected: Outcome::Accept,
    },
    Case {
        name: "compose: index+eq+take",
        build: index_eq_take,
        expected: Outcome::Accept,
    },
    Case {
        name: "compose: index+order",
        build: index_order,
        expected: Outcome::Accept,
    },
    Case {
        name: "compose: index+gt+lt",
        build: index_gt_lt,
        expected: Outcome::Accept,
    },
    Case {
        name: "compose: take+filter",
        build: take_filter,
        expected: Outcome::Accept,
    },
];

// ---- builder helpers --------------------------------------------------------

fn noop(_q: &mut Query) {}

fn solo_get(q: &mut Query) {
    q.get = Some(ID.to_string());
}
fn solo_index(q: &mut Query) {
    q.index = Some("by_title".to_string());
}
fn solo_eq(q: &mut Query) {
    q.index = Some("by_title".to_string());
    q.eq.push(json!("x"));
}
fn solo_gt(q: &mut Query) {
    q.index = Some("by_title".to_string());
    q.gt = Some(json!("x"));
}
fn solo_gte(q: &mut Query) {
    q.index = Some("by_title".to_string());
    q.gte = Some(json!("x"));
}
fn solo_lt(q: &mut Query) {
    q.index = Some("by_title".to_string());
    q.lt = Some(json!("x"));
}
fn solo_lte(q: &mut Query) {
    q.index = Some("by_title".to_string());
    q.lte = Some(json!("x"));
}
fn solo_order(q: &mut Query) {
    q.order = Some(Order::Asc);
}
fn solo_take(q: &mut Query) {
    q.take = Some(1);
}
fn solo_unique(q: &mut Query) {
    q.unique = true;
}
fn solo_first(q: &mut Query) {
    q.first = true;
}
fn solo_count(q: &mut Query) {
    q.count = true;
}
fn solo_distinct(q: &mut Query) {
    // `distinct` requires an index with at least one field beyond the eq prefix;
    // `by_title` has one field, so a no-eq prefix distinct over title is valid.
    q.distinct = true;
    q.index = Some("by_title".to_string());
}
fn solo_paginate(q: &mut Query) {
    q.paginate = Some(paginate_num_1());
}
fn solo_filter(q: &mut Query) {
    q.filter = Some(filter_eq_title_x());
}
fn solo_search(q: &mut Query) {
    q.search = Some(search_body_x());
}
fn solo_vector(q: &mut Query) {
    q.vector_search = Some(vector_embedding_limit_1());
}
fn solo_hybrid(q: &mut Query) {
    q.hybrid_search = Some(hybrid_query_database_x());
}
fn solo_aggregate(q: &mut Query) {
    // MIN over the post-prefix field of `by_title` (string field, orderable).
    q.aggregate = Some(AggregateSpec {
        op: AggregateOp::Min,
        group_by: false,
    });
    q.index = Some("by_title".to_string());
}

// get + peer
fn get_index(q: &mut Query) {
    q.get = Some(ID.to_string());
    q.index = Some("by_title".to_string());
}
fn get_eq(q: &mut Query) {
    q.get = Some(ID.to_string());
    q.eq.push(json!("x"));
}
fn get_gt(q: &mut Query) {
    q.get = Some(ID.to_string());
    q.gt = Some(json!("x"));
}
fn get_gte(q: &mut Query) {
    q.get = Some(ID.to_string());
    q.gte = Some(json!("x"));
}
fn get_lt(q: &mut Query) {
    q.get = Some(ID.to_string());
    q.lt = Some(json!("x"));
}
fn get_lte(q: &mut Query) {
    q.get = Some(ID.to_string());
    q.lte = Some(json!("x"));
}
fn get_order(q: &mut Query) {
    q.get = Some(ID.to_string());
    q.order = Some(Order::Asc);
}
fn get_take(q: &mut Query) {
    q.get = Some(ID.to_string());
    q.take = Some(1);
}
fn get_unique(q: &mut Query) {
    q.get = Some(ID.to_string());
    q.unique = true;
}
fn get_first(q: &mut Query) {
    q.get = Some(ID.to_string());
    q.first = true;
}
fn get_count(q: &mut Query) {
    q.get = Some(ID.to_string());
    q.count = true;
}
fn get_paginate(q: &mut Query) {
    q.get = Some(ID.to_string());
    q.paginate = Some(paginate_num_1());
}
fn get_filter(q: &mut Query) {
    q.get = Some(ID.to_string());
    q.filter = Some(filter_eq_title_x());
}
fn get_search(q: &mut Query) {
    q.get = Some(ID.to_string());
    q.search = Some(search_body_x());
}
fn get_vector(q: &mut Query) {
    q.get = Some(ID.to_string());
    q.vector_search = Some(vector_embedding_limit_1());
}
fn get_hybrid(q: &mut Query) {
    q.get = Some(ID.to_string());
    q.hybrid_search = Some(hybrid_query_database_x());
}

// unique/first/count/paginate peers
fn unique_take(q: &mut Query) {
    q.unique = true;
    q.take = Some(1);
}
fn unique_order(q: &mut Query) {
    q.unique = true;
    q.order = Some(Order::Asc);
}
fn first_unique(q: &mut Query) {
    q.first = true;
    q.unique = true;
}
fn first_take(q: &mut Query) {
    q.first = true;
    q.take = Some(1);
}
fn count_unique(q: &mut Query) {
    q.count = true;
    q.unique = true;
}
fn count_take(q: &mut Query) {
    q.count = true;
    q.take = Some(1);
}
fn count_first(q: &mut Query) {
    q.count = true;
    q.first = true;
}
fn count_order(q: &mut Query) {
    q.count = true;
    q.order = Some(Order::Asc);
}
fn count_distinct(q: &mut Query) {
    q.count = true;
    q.distinct = true;
}

// distinct + peer (rejects every other terminal; composes only with
// index/eq/range/filter — filter is exercised under composition accepts below).
fn distinct_get(q: &mut Query) {
    q.distinct = true;
    q.index = Some("by_title".to_string());
    q.get = Some(ID.to_string());
}
fn distinct_take(q: &mut Query) {
    q.distinct = true;
    q.index = Some("by_title".to_string());
    q.take = Some(1);
}
fn distinct_unique(q: &mut Query) {
    q.distinct = true;
    q.index = Some("by_title".to_string());
    q.unique = true;
}
fn distinct_first(q: &mut Query) {
    q.distinct = true;
    q.index = Some("by_title".to_string());
    q.first = true;
}
fn distinct_count(q: &mut Query) {
    q.distinct = true;
    q.index = Some("by_title".to_string());
    q.count = true;
}
fn distinct_order(q: &mut Query) {
    q.distinct = true;
    q.index = Some("by_title".to_string());
    q.order = Some(Order::Asc);
}
fn distinct_paginate(q: &mut Query) {
    q.distinct = true;
    q.index = Some("by_title".to_string());
    q.paginate = Some(paginate_num_1());
}
fn distinct_search(q: &mut Query) {
    q.distinct = true;
    q.index = Some("by_title".to_string());
    q.search = Some(search_body_x());
}
fn distinct_vector(q: &mut Query) {
    q.distinct = true;
    q.index = Some("by_title".to_string());
    q.vector_search = Some(vector_embedding_limit_1());
}
fn distinct_hybrid(q: &mut Query) {
    q.distinct = true;
    q.index = Some("by_title".to_string());
    q.hybrid_search = Some(hybrid_query_database_x());
}

// aggregate + peer (rejects every other terminal; composes only with
// index/eq/range/filter — exercised under composition accepts below).
fn aggregate_min(q: &mut Query) {
    q.aggregate = Some(AggregateSpec {
        op: AggregateOp::Min,
        group_by: false,
    });
    q.index = Some("by_title".to_string());
}
fn aggregate_get(q: &mut Query) {
    aggregate_min(q);
    q.get = Some(ID.to_string());
}
fn aggregate_take(q: &mut Query) {
    aggregate_min(q);
    q.take = Some(1);
}
fn aggregate_unique(q: &mut Query) {
    aggregate_min(q);
    q.unique = true;
}
fn aggregate_first(q: &mut Query) {
    aggregate_min(q);
    q.first = true;
}
fn aggregate_count(q: &mut Query) {
    aggregate_min(q);
    q.count = true;
}
fn aggregate_distinct(q: &mut Query) {
    aggregate_min(q);
    q.distinct = true;
}
fn aggregate_order(q: &mut Query) {
    aggregate_min(q);
    q.order = Some(Order::Asc);
}
fn aggregate_paginate(q: &mut Query) {
    aggregate_min(q);
    q.paginate = Some(paginate_num_1());
}
fn aggregate_search(q: &mut Query) {
    aggregate_min(q);
    q.search = Some(search_body_x());
}
fn aggregate_vector(q: &mut Query) {
    aggregate_min(q);
    q.vector_search = Some(vector_embedding_limit_1());
}
fn aggregate_hybrid(q: &mut Query) {
    aggregate_min(q);
    q.hybrid_search = Some(hybrid_query_database_x());
}
// aggregate compositions that must NOT false-reject.
fn aggregate_eq(q: &mut Query) {
    // by_title_count has [title, count]; consuming `title` in the eq prefix
    // leaves `count` as the aggregate field — numeric, so SUM is valid.
    q.aggregate = Some(AggregateSpec {
        op: AggregateOp::Sum,
        group_by: false,
    });
    q.index = Some("by_title_count".to_string());
    q.eq.push(json!("x"));
}
fn aggregate_filter(q: &mut Query) {
    q.aggregate = Some(AggregateSpec {
        op: AggregateOp::Min,
        group_by: false,
    });
    q.index = Some("by_title".to_string());
    q.filter = Some(filter_eq_title_x());
}
fn paginate_count(q: &mut Query) {
    q.paginate = Some(paginate_num_1());
    q.count = true;
}
fn paginate_unique(q: &mut Query) {
    q.paginate = Some(paginate_num_1());
    q.unique = true;
}
fn paginate_first(q: &mut Query) {
    q.paginate = Some(paginate_num_1());
    q.first = true;
}
fn paginate_take(q: &mut Query) {
    q.paginate = Some(paginate_num_1());
    q.take = Some(1);
}

// range-bound incompatibilities
fn gt_gte(q: &mut Query) {
    q.index = Some("by_title".to_string());
    q.gt = Some(json!("x"));
    q.gte = Some(json!("x"));
}
fn lt_lte(q: &mut Query) {
    q.index = Some("by_title".to_string());
    q.lt = Some(json!("x"));
    q.lte = Some(json!("x"));
}

// vectorSearch + peer (rejects all peers including take)
fn vs_index(q: &mut Query) {
    q.vector_search = Some(vector_embedding_limit_1());
    q.index = Some("by_title".to_string());
}
fn vs_eq(q: &mut Query) {
    q.vector_search = Some(vector_embedding_limit_1());
    q.eq.push(json!("x"));
}
fn vs_gt(q: &mut Query) {
    q.vector_search = Some(vector_embedding_limit_1());
    q.gt = Some(json!("x"));
}
fn vs_gte(q: &mut Query) {
    q.vector_search = Some(vector_embedding_limit_1());
    q.gte = Some(json!("x"));
}
fn vs_lt(q: &mut Query) {
    q.vector_search = Some(vector_embedding_limit_1());
    q.lt = Some(json!("x"));
}
fn vs_lte(q: &mut Query) {
    q.vector_search = Some(vector_embedding_limit_1());
    q.lte = Some(json!("x"));
}
fn vs_order(q: &mut Query) {
    q.vector_search = Some(vector_embedding_limit_1());
    q.order = Some(Order::Asc);
}
fn vs_unique(q: &mut Query) {
    q.vector_search = Some(vector_embedding_limit_1());
    q.unique = true;
}
fn vs_first(q: &mut Query) {
    q.vector_search = Some(vector_embedding_limit_1());
    q.first = true;
}
fn vs_count(q: &mut Query) {
    q.vector_search = Some(vector_embedding_limit_1());
    q.count = true;
}
fn vs_paginate(q: &mut Query) {
    q.vector_search = Some(vector_embedding_limit_1());
    q.paginate = Some(paginate_num_1());
}
fn vs_filter(q: &mut Query) {
    q.vector_search = Some(vector_embedding_limit_1());
    q.filter = Some(filter_eq_title_x());
}
fn vs_search(q: &mut Query) {
    q.vector_search = Some(vector_embedding_limit_1());
    q.search = Some(search_body_x());
}
fn vs_take(q: &mut Query) {
    q.vector_search = Some(vector_embedding_limit_1());
    q.take = Some(1);
}
fn vs_hybrid(q: &mut Query) {
    q.vector_search = Some(vector_embedding_limit_1());
    q.hybrid_search = Some(hybrid_query_database_x());
}

// search + peer (rejects all peers except take)
fn sr_index(q: &mut Query) {
    q.search = Some(search_body_x());
    q.index = Some("by_title".to_string());
}
fn sr_eq(q: &mut Query) {
    q.search = Some(search_body_x());
    q.eq.push(json!("x"));
}
fn sr_gt(q: &mut Query) {
    q.search = Some(search_body_x());
    q.gt = Some(json!("x"));
}
fn sr_gte(q: &mut Query) {
    q.search = Some(search_body_x());
    q.gte = Some(json!("x"));
}
fn sr_lt(q: &mut Query) {
    q.search = Some(search_body_x());
    q.lt = Some(json!("x"));
}
fn sr_lte(q: &mut Query) {
    q.search = Some(search_body_x());
    q.lte = Some(json!("x"));
}
fn sr_order(q: &mut Query) {
    q.search = Some(search_body_x());
    q.order = Some(Order::Asc);
}
fn sr_unique(q: &mut Query) {
    q.search = Some(search_body_x());
    q.unique = true;
}
fn sr_first(q: &mut Query) {
    q.search = Some(search_body_x());
    q.first = true;
}
fn sr_count(q: &mut Query) {
    q.search = Some(search_body_x());
    q.count = true;
}
fn sr_paginate(q: &mut Query) {
    q.search = Some(search_body_x());
    q.paginate = Some(paginate_num_1());
}
fn sr_filter(q: &mut Query) {
    q.search = Some(search_body_x());
    q.filter = Some(filter_eq_title_x());
}
fn sr_vector(q: &mut Query) {
    q.search = Some(search_body_x());
    q.vector_search = Some(vector_embedding_limit_1());
}
fn sr_hybrid(q: &mut Query) {
    q.search = Some(search_body_x());
    q.hybrid_search = Some(hybrid_query_database_x());
}

// hybridSearch + peer (rejects all peers including take — hybrid carries its
// own limit and IS the search+vector combination, so neither composes with it)
fn hs_index(q: &mut Query) {
    q.hybrid_search = Some(hybrid_query_database_x());
    q.index = Some("by_title".to_string());
}
fn hs_eq(q: &mut Query) {
    q.hybrid_search = Some(hybrid_query_database_x());
    q.eq.push(json!("x"));
}
fn hs_gt(q: &mut Query) {
    q.hybrid_search = Some(hybrid_query_database_x());
    q.gt = Some(json!("x"));
}
fn hs_gte(q: &mut Query) {
    q.hybrid_search = Some(hybrid_query_database_x());
    q.gte = Some(json!("x"));
}
fn hs_lt(q: &mut Query) {
    q.hybrid_search = Some(hybrid_query_database_x());
    q.lt = Some(json!("x"));
}
fn hs_lte(q: &mut Query) {
    q.hybrid_search = Some(hybrid_query_database_x());
    q.lte = Some(json!("x"));
}
fn hs_order(q: &mut Query) {
    q.hybrid_search = Some(hybrid_query_database_x());
    q.order = Some(Order::Asc);
}
fn hs_unique(q: &mut Query) {
    q.hybrid_search = Some(hybrid_query_database_x());
    q.unique = true;
}
fn hs_first(q: &mut Query) {
    q.hybrid_search = Some(hybrid_query_database_x());
    q.first = true;
}
fn hs_count(q: &mut Query) {
    q.hybrid_search = Some(hybrid_query_database_x());
    q.count = true;
}
fn hs_distinct(q: &mut Query) {
    q.hybrid_search = Some(hybrid_query_database_x());
    q.distinct = true;
}
fn hs_aggregate(q: &mut Query) {
    q.hybrid_search = Some(hybrid_query_database_x());
    q.aggregate = Some(AggregateSpec {
        op: AggregateOp::Min,
        group_by: false,
    });
}
fn hs_paginate(q: &mut Query) {
    q.hybrid_search = Some(hybrid_query_database_x());
    q.paginate = Some(paginate_num_1());
}
fn hs_filter(q: &mut Query) {
    q.hybrid_search = Some(hybrid_query_database_x());
    q.filter = Some(filter_eq_title_x());
}
fn hs_take(q: &mut Query) {
    q.hybrid_search = Some(hybrid_query_database_x());
    q.take = Some(1);
}

// composition accepts (valid combos that must not false-reject)
fn search_take(q: &mut Query) {
    q.search = Some(search_body_x());
    q.take = Some(1);
}
fn index_take(q: &mut Query) {
    q.index = Some("by_title".to_string());
    q.take = Some(1);
}
fn index_eq_take(q: &mut Query) {
    q.index = Some("by_title".to_string());
    q.eq.push(json!("x"));
    q.take = Some(1);
}
fn index_order(q: &mut Query) {
    q.index = Some("by_title".to_string());
    q.order = Some(Order::Asc);
}
fn index_gt_lt(q: &mut Query) {
    q.index = Some("by_title".to_string());
    q.gt = Some(json!("a"));
    q.lt = Some(json!("z"));
}
fn take_filter(q: &mut Query) {
    q.take = Some(1);
    q.filter = Some(filter_eq_title_x());
}

// ---- the matrix test --------------------------------------------------------

#[tokio::test]
async fn combination_matrix_cascade_outcomes_match_expectations() -> anyhow::Result<()> {
    let state = test_state().await;
    let (db, schema) = matrix_db(&state).await;

    for case in CASES {
        let mut q = base_query();
        (case.build)(&mut q);
        let result = execute_query(&state.pool, &db, &schema, &q, &PrincipalCtx::bypass()).await;
        let actual = match result {
            Ok(_) => Outcome::Accept,
            Err(e) if e.code == ErrorCode::BadRequest => Outcome::Reject,
            Err(e) => panic!(
                "case '{}': expected {:?} but got non-cascade error {:?}: {}",
                case.name, case.expected, e.code, e.message
            ),
        };
        assert_eq!(
            actual, case.expected,
            "case '{}': cascade outcome mismatch (matrix drift between server and TS mirror)",
            case.name
        );
    }

    Ok(())
}
