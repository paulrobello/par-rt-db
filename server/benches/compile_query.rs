//! ENH-033 micro-benchmark: `query::compile_query` — the SQL compiler for the
//! read path. No Postgres: `compile_query` is pure (schema + query in,
//! `(CompiledQuery, warnings)` out). Compiles a fixed set of ~20 representative
//! queries spanning eq-prefix lookups, range bounds, filters up to depth 3,
//! order, search, vector search, and hybrid search — the shapes the read path
//! sees in practice.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::query::{FilterExpr, Query, compile_query};
use rtdb_server::schema::SchemaDef;

/// A `docs` table with btree, search, and vector indexes — enough surface to
/// exercise every terminal `compile_query` supports.
fn bench_schema() -> SchemaDef {
    serde_json::from_value(serde_json::json!({"tables":{
        "docs":{
            "fields":{
                "title":{"type":"string"},
                "body":{"type":"string"},
                "status":{"type":"union","variants":[
                    {"type":"literal","value":"draft"},
                    {"type":"literal","value":"published"},
                    {"type":"literal","value":"archived"}]},
                "owner":{"type":"string"},
                "priority":{"type":"number"},
                "views":{"type":"number"},
                "tags":{"type":"array","element":{"type":"string"}},
                "createdAt":{"type":"number"},
                "embedding":{"type":"vector","dimensions":8}
            },
            "indexes":[
                {"name":"by_status","fields":["status"]},
                {"name":"by_owner_status","fields":["owner","status"]},
                {"name":"by_owner_priority","fields":["owner","priority"]},
                {"name":"by_created","fields":["createdAt"]},
                {"name":"by_priority","fields":["priority"]},
                {"name":"search_body","fields":["title","body"],"search":true},
                {"name":"by_embedding","fields":["embedding"],
                 "vector":{"dimensions":8,"filterFields":["status"]}}
            ]
        }
    }}))
    .expect("parse bench schema")
}

/// Depth-3 nested filter: `And[ Or[ Eq, Eq ], Not(Eq) ]`.
fn deep_filter() -> FilterExpr {
    FilterExpr::And {
        exprs: vec![
            FilterExpr::Or {
                exprs: vec![
                    FilterExpr::Eq {
                        field: "owner".into(),
                        value: serde_json::json!("alice"),
                    },
                    FilterExpr::Eq {
                        field: "owner".into(),
                        value: serde_json::json!("bob"),
                    },
                ],
            },
            FilterExpr::Not {
                expr: Box::new(FilterExpr::Eq {
                    field: "status".into(),
                    value: serde_json::json!("archived"),
                }),
            },
        ],
    }
}

/// ~20 representative queries: eq-prefix, range bounds, deep filters, order,
/// take/paginate/count/distinct/aggregate, search, vector search, hybrid
/// search, and a plain point read.
fn bench_queries() -> Vec<(&'static str, Query)> {
    let mk = |json: serde_json::Value| -> Query {
        serde_json::from_value(json).expect("parse bench query")
    };
    let mut queries: Vec<(&'static str, Query)> = vec![
        ("get", mk(serde_json::json!({"table":"docs","get":"doc_1"}))),
        (
            "eq_prefix",
            mk(serde_json::json!({"table":"docs","index":"by_status","eq":["published"]})),
        ),
        (
            "eq_prefix_two",
            mk(
                serde_json::json!({"table":"docs","index":"by_owner_status","eq":["alice","published"]}),
            ),
        ),
        (
            "range_gt_lt",
            mk(serde_json::json!({"table":"docs","index":"by_created","gt":100,"lt":900})),
        ),
        (
            "order_desc_take",
            mk(serde_json::json!({"table":"docs","index":"by_priority","order":"desc","take":50})),
        ),
        (
            "count",
            mk(serde_json::json!({"table":"docs","index":"by_status","eq":["draft"],"count":true})),
        ),
        (
            "distinct",
            mk(
                serde_json::json!({"table":"docs","index":"by_owner_status","eq":["alice"],"distinct":true}),
            ),
        ),
        (
            "aggregate_sum",
            mk(
                serde_json::json!({"table":"docs","index":"by_owner_priority","eq":["alice"],
                "aggregate":{"op":"sum"}}),
            ),
        ),
        (
            "aggregate_group",
            mk(
                serde_json::json!({"table":"docs","index":"by_owner_status","eq":["alice"],
                "aggregate":{"op":"count","groupBy":true}}),
            ),
        ),
        (
            "paginate",
            mk(
                serde_json::json!({"table":"docs","index":"by_status","eq":["published"],
                "paginate":{"numItems":25}}),
            ),
        ),
        (
            "first",
            mk(
                serde_json::json!({"table":"docs","index":"by_status","eq":["published"],"first":true}),
            ),
        ),
        (
            "unique",
            mk(
                serde_json::json!({"table":"docs","index":"by_owner_status","eq":["alice","published"],"unique":true}),
            ),
        ),
        (
            "search",
            mk(
                serde_json::json!({"table":"docs","search":{"index":"search_body","query":"release notes"}}),
            ),
        ),
        (
            "search_with_filter",
            mk(
                serde_json::json!({"table":"docs","search":{"index":"search_body","query":"release notes",
                "filter":{"op":"eq","field":"status","value":"published"}}}),
            ),
        ),
        (
            "vector_search",
            mk(
                serde_json::json!({"table":"docs","vectorSearch":{"index":"by_embedding",
                "vector":[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8],"limit":10}}),
            ),
        ),
        (
            "vector_search_filtered",
            mk(
                serde_json::json!({"table":"docs","vectorSearch":{"index":"by_embedding",
                "vector":[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8],"limit":10,
                "filter":{"op":"eq","field":"status","value":"published"}}}),
            ),
        ),
        (
            "hybrid_search",
            mk(
                serde_json::json!({"table":"docs","hybridSearch":{"query":"release notes",
                "vector":[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8],"limit":10}}),
            ),
        ),
        (
            "collect_full_scan",
            mk(serde_json::json!({"table":"docs","take":100})),
        ),
        (
            "projection",
            mk(
                serde_json::json!({"table":"docs","index":"by_status","eq":["published"],
                "fields":["title","owner"]}),
            ),
        ),
    ];

    // Two deep-filter queries (depth 3), attached to plain eq-prefix reads.
    let mut deep_a: Query =
        mk(serde_json::json!({"table":"docs","index":"by_status","eq":["published"],"take":50}));
    deep_a.filter = Some(deep_filter());
    queries.push(("filter_depth3_a", deep_a));

    let mut deep_b: Query = mk(
        serde_json::json!({"table":"docs","index":"by_owner_status","eq":["alice","published"]}),
    );
    deep_b.filter = Some(deep_filter());
    queries.push(("filter_depth3_b", deep_b));

    queries
}

fn bench_compile_query(c: &mut Criterion) {
    let schema = bench_schema();
    let ctx = PrincipalCtx::bypass();
    let queries = bench_queries();

    let mut group = c.benchmark_group("compile_query");
    for (name, query) in &queries {
        group.bench_function(*name, |b| {
            b.iter(|| {
                let result = compile_query(
                    black_box("bench_db"),
                    black_box(&schema),
                    black_box(query),
                    black_box(&ctx),
                    false,
                );
                black_box(result).expect("compile query");
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_compile_query);
criterion_main!(benches);
