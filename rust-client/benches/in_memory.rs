//! ENH-033 micro-benchmark: the `in_memory` engine's write path
//! (`InMemoryRtDbClient::mutate`, which drives the per-step `execute_step`
//! executor) and read path (`InMemoryRtDbClient::run`, the `run_query`
//! engine) over a 10k-row table. No network, no server — the engine is the
//! client-side optimistic path every live client mirrors.

#![cfg(feature = "in_memory")]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use par_rt_db_client::in_memory::{InMemoryRtDbClient, InMemoryRtDbClientOptions};
use par_rt_db_client::query::TableQuery;
use par_rt_db_client::schema::{FieldType, Table};
use par_rt_db_client::{Mutation, Order, SchemaBuilderExt, SchemaDef};
use serde_json::{Value, json};

const ROW_COUNT: usize = 10_000;

fn bench_schema() -> SchemaDef {
    SchemaDef::builder()
        .table(
            "rows",
            Table::new()
                .field("name", FieldType::String)
                .field("value", FieldType::Number)
                .field(
                    "status",
                    FieldType::union([
                        FieldType::literal("active"),
                        FieldType::literal("paused"),
                        FieldType::literal("done"),
                    ]),
                )
                .index("by_status", &["status"]),
        )
        .build()
}

/// A fresh client with the bench schema pushed, no rows.
fn fresh_client() -> InMemoryRtDbClient {
    let mut client = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    client.push_schema(&bench_schema()).expect("push_schema");
    client
}

/// A client with `ROW_COUNT` rows seeded into `rows`, distributed evenly over
/// the three `status` values so `by_status` lookups return a realistic slice.
async fn seeded_client() -> InMemoryRtDbClient {
    let mut client = fresh_client();
    let statuses = ["active", "paused", "done"];
    // Batches of 500 steps per txn (comfortably under MAX_STEPS = 1024 and
    // MAX_AFFECTED_ROWS_PER_TXN = 10_000).
    const BATCH: usize = 500;
    let mut inserted = 0usize;
    while inserted < ROW_COUNT {
        let mut txn = Mutation::new();
        let batch_end = (inserted + BATCH).min(ROW_COUNT);
        for i in inserted..batch_end {
            txn = txn.insert(
                "rows",
                json!({
                    "name": format!("row-{i}"),
                    "value": i as f64,
                    "status": statuses[i % statuses.len()],
                }),
            );
        }
        client
            .mutate(&txn.build(), None)
            .await
            .expect("seed batch insert");
        inserted = batch_end;
    }
    client
}

fn bench_mutate_insert(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("build tokio runtime");
    let mut client = rt.block_on(seeded_client());
    let mut i = 0u64;

    c.bench_function("in_memory_mutate_insert", |b| {
        b.iter(|| {
            i += 1;
            let txn = Mutation::new()
                .insert(
                    "rows",
                    json!({
                        "name": format!("bench-insert-{i}"),
                        "value": i as f64,
                        "status": "active",
                    }),
                )
                .build();
            let result = rt.block_on(client.mutate(black_box(&txn), None));
            black_box(result).expect("insert row");
        });
    });
}

fn bench_mutate_patch(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("build tokio runtime");
    let mut client = rt.block_on(seeded_client());
    // A stable target row, patched repeatedly — steady-state, no growth.
    let insert_txn = Mutation::new()
        .insert(
            "rows",
            json!({"name": "patch-target", "value": 0.0, "status": "active"}),
        )
        .build();
    let results = rt
        .block_on(client.mutate(&insert_txn, None))
        .expect("insert patch target");
    let target_id = match results.as_slice() {
        [par_rt_db_client::StepResult::Insert { id }] => id.clone(),
        other => panic!("expected a single Insert result, got {other:?}"),
    };

    c.bench_function("in_memory_mutate_patch", |b| {
        b.iter(|| {
            let txn = Mutation::new()
                .patch("rows", &target_id, json!({"value": 1.0}))
                .build();
            let result = rt.block_on(client.mutate(black_box(&txn), None));
            black_box(result).expect("patch row");
        });
    });
}

fn bench_query(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("build tokio runtime");
    let client = rt.block_on(seeded_client());

    let mut group = c.benchmark_group("in_memory_query");

    group.bench_function("eq_index_take", |b| {
        b.iter(|| {
            let query = TableQuery::new("rows")
                .with_index("by_status", &[json!("active")])
                .take(50);
            let result: Result<Vec<Value>, _> = client.run(black_box(&query));
            black_box(result).expect("eq index query");
        });
    });

    group.bench_function("eq_index_order_take", |b| {
        b.iter(|| {
            let query = TableQuery::new("rows")
                .with_index("by_status", &[json!("paused")])
                .order(Order::Desc)
                .take(100);
            let result: Result<Vec<Value>, _> = client.run(black_box(&query));
            black_box(result).expect("ordered eq index query");
        });
    });

    group.bench_function("count", |b| {
        b.iter(|| {
            let query = TableQuery::new("rows")
                .with_index("by_status", &[json!("done")])
                .count();
            let result: Result<i64, _> = client.run(black_box(&query));
            black_box(result).expect("count query");
        });
    });

    group.bench_function("full_collect_take", |b| {
        b.iter(|| {
            let query = TableQuery::new("rows").take(200);
            let result: Result<Vec<Value>, _> = client.run(black_box(&query));
            black_box(result).expect("full collect query");
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_mutate_insert,
    bench_mutate_patch,
    bench_query
);
criterion_main!(benches);
