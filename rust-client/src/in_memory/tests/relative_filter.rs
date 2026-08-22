//! Execution-time-relative `olderThan` predicates in by-query steps — the
//! engine mirror of `server/tests/relative_filter_test.rs`. The filter op
//! whose cutoff (`now − ms`) is derived from the engine's injected clock at
//! execution — per fire for a scheduled txn — instead of a literal frozen at
//! schedule time. Pins the by-query-only acceptance boundary and the
//! deterministic match margins: OLD (1) is below any cutoff for centuries,
//! FUTURE (9e15) is above it, so the clock's exact value never matters.
use super::*;
use crate::value_expr::{CaseWhen, ValueExpr};
use crate::wire::ScheduleWhen;

/// Below `now − SWEEP_MS` for centuries (epoch-ms today is ~1.8e12; the
/// cutoff is ~0.8e12 and rising by 1/year).
const OLD: i64 = 1;
/// 9e15 — above `now − 0` effectively forever; f64-exact, within i64.
const FUTURE: i64 = 9_000_000_000_000_000;
const SWEEP_MS: i64 = 1_000_000_000_000;

fn number_schema() -> SchemaDef {
    Schema::builder()
        .table(
            "tasks",
            Table::new()
                .field("title", FieldType::String)
                .field("updatedAt", FieldType::Number)
                .index("by_title", &["title"]),
        )
        .build()
}

/// `updatedAt` as int64 and indexed, so the comparison takes the exact i64
/// path over the decimal-string wire form (the server's typed `bigint`
/// column) rather than the `float8` extraction.
fn int64_indexed_schema() -> SchemaDef {
    Schema::builder()
        .table(
            "tasks",
            Table::new()
                .field("title", FieldType::String)
                .field("updatedAt", FieldType::Int64)
                .index("by_title", &["title"])
                .index("by_updatedAt", &["updatedAt"]),
        )
        .build()
}

fn older_than(field: &str, ms: i64) -> FilterExpr {
    FilterExpr::OlderThan {
        field: field.to_string(),
        ms,
    }
}

/// A client with the number schema and a monotonic epoch-ms clock (the
/// corpus runner's ~1.7e12 base) — each seed mints a distinct id, and the
/// sweep margins hold regardless of the exact read.
fn sweep_client(schema: &SchemaDef) -> InMemoryRtDbClient {
    let counter = Arc::new(Mutex::new(1_700_000_000_000_i64));
    let mut client = InMemoryRtDbClient::new(
        InMemoryRtDbClientOptions::default()
            .now(move || {
                let mut g = counter.lock().expect("counter not poisoned");
                let v = *g;
                *g += 1;
                v
            })
            .random(|| 0.0),
    );
    client.push_schema(schema).expect("push schema");
    client
}

/// `updated_at` is passed as a JSON value so the int64 schema can seed the
/// field's decimal-string wire form.
async fn seed(c: &mut InMemoryRtDbClient, title: &str, updated_at: Value) {
    c.mutate(
        &Mutation::new()
            .insert("tasks", json!({ "title": title, "updatedAt": updated_at }))
            .build(),
        None,
    )
    .await
    .expect("seed insert");
}

async fn count_title(c: &mut InMemoryRtDbClient, title: &str) -> usize {
    let docs = c
        .run::<Vec<Value>>(
            &TableQuery::new("tasks")
                .with_index("by_title", &[json!(title)])
                .collect(),
        )
        .expect("count scan");
    docs.len()
}

#[tokio::test]
async fn patch_by_query_older_than_patches_old_rows_only() {
    let mut c = sweep_client(&number_schema());
    seed(&mut c, "old", json!(OLD)).await;
    seed(&mut c, "future", json!(FUTURE)).await;

    let results = c
        .mutate(
            &Mutation::new()
                .patch_by_query(
                    "tasks",
                    older_than("updatedAt", SWEEP_MS),
                    json!({ "title": "swept" }),
                    None,
                )
                .build(),
            None,
        )
        .await
        .expect("sweep txn");
    assert!(
        matches!(
            &results[0],
            StepResult::PatchByQuery {
                patched: 1,
                truncated: false
            }
        ),
        "only the old row matches, got {results:?}"
    );
    assert_eq!(count_title(&mut c, "swept").await, 1);
    assert_eq!(count_title(&mut c, "future").await, 1);
}

#[tokio::test]
async fn delete_by_query_older_than_deletes_old_rows_only() {
    let mut c = sweep_client(&number_schema());
    seed(&mut c, "old", json!(OLD)).await;
    seed(&mut c, "future", json!(FUTURE)).await;

    let results = c
        .mutate(
            &Mutation::new()
                .delete_by_query("tasks", older_than("updatedAt", SWEEP_MS), None)
                .build(),
            None,
        )
        .await
        .expect("sweep txn");
    assert!(
        matches!(
            &results[0],
            StepResult::DeleteByQuery {
                deleted: 1,
                truncated: false
            }
        ),
        "only the old row matches, got {results:?}"
    );
    assert_eq!(count_title(&mut c, "old").await, 0);
    assert_eq!(count_title(&mut c, "future").await, 1);
}

#[tokio::test]
async fn patch_by_query_older_than_takes_the_int64_wire_form() {
    let mut c = sweep_client(&int64_indexed_schema());
    // int64 wire form is a decimal string, end to end.
    seed(&mut c, "old", json!(OLD.to_string())).await;
    seed(&mut c, "future", json!(FUTURE.to_string())).await;

    let results = c
        .mutate(
            &Mutation::new()
                .patch_by_query(
                    "tasks",
                    older_than("updatedAt", SWEEP_MS),
                    json!({ "title": "swept" }),
                    None,
                )
                .build(),
            None,
        )
        .await
        .expect("sweep txn");
    assert!(
        matches!(
            &results[0],
            StepResult::PatchByQuery {
                patched: 1,
                truncated: false
            }
        ),
        "the decimal-string old row matches, got {results:?}"
    );
    assert_eq!(count_title(&mut c, "future").await, 1);
}

#[tokio::test]
async fn read_query_filter_older_than_is_rejected() {
    let c = sweep_client(&number_schema());
    let err = c
        .run::<Vec<Value>>(
            &TableQuery::new("tasks")
                .filter(older_than("updatedAt", SWEEP_MS))
                .collect(),
        )
        .expect_err("read filters reject olderThan");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message
            .contains("only allowed in patchByQuery/deleteByQuery"),
        "got: {}",
        err.message
    );
}

#[tokio::test]
async fn patch_by_query_older_than_rejects_non_numeric_field_and_negative_ms() {
    // `updatedAt` retyped to string: the olderThan leaf now targets a
    // non-numeric field; `title` with -1 ms is a negative window. Both are
    // BAD_REQUEST at the by-query validation chokepoint.
    let schema = Schema::builder()
        .table(
            "tasks",
            Table::new()
                .field("title", FieldType::String)
                .field("updatedAt", FieldType::String)
                .index("by_title", &["title"]),
        )
        .build();
    let mut c = sweep_client(&schema);

    for filter in [older_than("updatedAt", SWEEP_MS), older_than("title", -1)] {
        let which = if matches!(filter, FilterExpr::OlderThan { ms: -1, .. }) {
            "negative ms"
        } else {
            "non-numeric field"
        };
        let err = c
            .mutate(
                &Mutation::new()
                    .patch_by_query("tasks", filter, json!({ "title": "swept" }), None)
                    .build(),
                None,
            )
            .await
            .expect_err("validation must reject");
        assert_eq!(err.code, ErrorCode::BadRequest, "{which}: got {err:?}");
        let expected = if which == "negative ms" {
            "ms must be >= 0"
        } else {
            "must be a number or int64"
        };
        assert!(
            err.message.contains(expected),
            "{which}: got {}",
            err.message
        );
    }
}

#[test]
fn authorize_index_where_and_case_when_older_than_rejected_at_push() {
    // authorize predicate carrying olderThan — SCHEMA_VIOLATION (server
    // `validate_structure`'s `validate_filter_expr_fields(.., false)`).
    let with_authorize = Schema::builder()
        .table(
            "tasks",
            Table::new()
                .field("title", FieldType::String)
                .field("updatedAt", FieldType::Number)
                .index("by_title", &["title"])
                .authorize(older_than("updatedAt", SWEEP_MS)),
        )
        .build();
    let err = with_authorize.validate().unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation, "got {err:?}");
    assert!(
        err.message
            .contains("only allowed in patchByQuery/deleteByQuery"),
        "authorize: got {}",
        err.message
    );

    // partial-index where-predicate carrying olderThan — the server rejects
    // it in `compile_filter_literal` (BAD_REQUEST): a baked DDL literal has
    // no execution-time clock to read.
    let with_where = Schema::builder()
        .table(
            "tasks",
            Table::new()
                .field("title", FieldType::String)
                .field("updatedAt", FieldType::Number)
                .index("by_title", &["title"])
                .index("by_updatedAt", &["updatedAt"])
                .where_clause(older_than("updatedAt", SWEEP_MS)),
        )
        .build();
    let err = with_where.validate().unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest, "got {err:?}");
    assert!(
        err.message
            .contains("not allowed in a partial-index predicate"),
        "partial index: got {}",
        err.message
    );

    // computed `case`-when filter carrying olderThan — BAD_REQUEST like every
    // computed rule (the server maps the shared filter validation's error
    // through `validate_computed_case_whens`).
    let with_case = Schema::builder()
        .table(
            "tasks",
            Table::new()
                .field("title", FieldType::String)
                .field("updatedAt", FieldType::Number)
                .field("sweepFlag", FieldType::Boolean)
                .index("by_title", &["title"])
                .computed(
                    "sweepFlag",
                    ValueExpr::Case {
                        whens: vec![CaseWhen {
                            when: older_than("updatedAt", SWEEP_MS),
                            then: ValueExpr::Literal { value: json!(true) },
                        }],
                        otherwise: Box::new(ValueExpr::Literal {
                            value: json!(false),
                        }),
                    },
                ),
        )
        .build();
    let err = with_case.validate().unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest, "got {err:?}");
    assert!(
        err.message
            .contains("only allowed in patchByQuery/deleteByQuery"),
        "case when: got {}",
        err.message
    );
}

/// The load-bearing property for scheduled sweeps: the stored txn carries no
/// cutoff literal — the engine's fire re-derives `now − ms` from the injected
/// clock, so the same schedule stays fresh with no client re-scheduling
/// (the engine mirror of the server's scheduler-driven test).
#[tokio::test]
async fn scheduled_sweep_fires_with_the_engine_clock() {
    let mut c = sweep_client(&number_schema());
    seed(&mut c, "old", json!(OLD)).await;
    seed(&mut c, "future", json!(FUTURE)).await;

    // One-shot due immediately: fires on the first tick.
    let txn = Mutation::new()
        .delete_by_query("tasks", older_than("updatedAt", SWEEP_MS), None)
        .build();
    c.schedule(txn, ScheduleWhen::AfterMs { ms: 0 })
        .expect("schedule sweep");
    c.tick(None);

    assert_eq!(count_title(&mut c, "old").await, 0, "expired row swept");
    assert_eq!(count_title(&mut c, "future").await, 1, "live row kept");
}
