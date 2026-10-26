//! `ExpectNoMatch` / `insert_if`: filter-based absence precondition. A visible
//! live row matching the filter aborts the whole transaction with
//! `PRECONDITION_FAILED`; soft-deleted rows are invisible; `olderThan` is
//! accepted (same scan context as the by-query steps).
use super::*;

const OLD: i64 = 1;
const FUTURE: i64 = 9_000_000_000_000_000;

fn schema(soft: bool) -> SchemaDef {
    let mut t = Table::new()
        .field("title", FieldType::String)
        .field("updatedAt", FieldType::Number)
        .index("by_title", &["title"]);
    if soft {
        t = t.soft_delete();
    }
    Schema::builder().table("tasks", t).build()
}

fn client(soft: bool) -> InMemoryRtDbClient {
    let counter = Arc::new(Mutex::new(1_700_000_000_000_i64));
    let mut c = InMemoryRtDbClient::new(
        InMemoryRtDbClientOptions::default()
            .now(move || {
                let mut g = counter.lock().expect("counter not poisoned");
                let v = *g;
                *g += 1;
                v
            })
            .random(|| 0.0),
    );
    c.push_schema(&schema(soft)).expect("push schema");
    c
}

fn title_is(t: &str) -> FilterExpr {
    FilterExpr::Eq {
        field: "title".into(),
        value: json!(t),
    }
}

async fn count_title(c: &mut InMemoryRtDbClient, t: &str) -> usize {
    c.run::<Vec<Value>>(
        &TableQuery::new("tasks")
            .with_index("by_title", &[json!(t)])
            .collect(),
    )
    .expect("count scan")
    .len()
}

fn assert_no_match_abort(err: &RtDbError) {
    assert!(
        matches!(err.code, ErrorCode::PreconditionFailed),
        "got: {:?} ({err})",
        err.code
    );
    assert_eq!(err.message, "filter matched an existing document");
}

#[tokio::test]
async fn expect_no_match_passes_when_nothing_matches() {
    let mut c = client(false);
    let results = c
        .mutate(
            &Mutation::new()
                .insert_if("tasks", title_is("a"), json!({"title":"a","updatedAt":1}))
                .build(),
            None,
        )
        .await
        .expect("insert_if on empty table");
    assert!(matches!(results[0], StepResult::Null), "got {results:?}");
    assert!(matches!(results[1], StepResult::Insert { .. }));
    assert_eq!(count_title(&mut c, "a").await, 1);
}

#[tokio::test]
async fn expect_no_match_aborts_when_a_row_matches() {
    let mut c = client(false);
    c.mutate(
        &Mutation::new()
            .insert("tasks", json!({"title":"a","updatedAt":1}))
            .build(),
        None,
    )
    .await
    .expect("seed");
    let err = c
        .mutate(
            &Mutation::new()
                .insert("tasks", json!({"title":"b","updatedAt":1}))
                .insert_if("tasks", title_is("a"), json!({"title":"a","updatedAt":2}))
                .build(),
            None,
        )
        .await
        .unwrap_err();
    assert_no_match_abort(&err);
    // The whole txn rolled back, including the earlier insert.
    assert_eq!(count_title(&mut c, "a").await, 1);
    assert_eq!(count_title(&mut c, "b").await, 0);
}

#[tokio::test]
async fn expect_no_match_sees_earlier_steps_in_the_same_txn() {
    let mut c = client(false);
    let err = c
        .mutate(
            &Mutation::new()
                .insert("tasks", json!({"title":"a","updatedAt":1}))
                .expect_no_match("tasks", title_is("a"))
                .build(),
            None,
        )
        .await
        .unwrap_err();
    assert_no_match_abort(&err);
    assert_eq!(count_title(&mut c, "a").await, 0);
}

#[tokio::test]
async fn expect_no_match_ignores_soft_deleted_rows() {
    let mut c = client(true);
    let results = c
        .mutate(
            &Mutation::new()
                .insert("tasks", json!({"title":"a","updatedAt":1}))
                .build(),
            None,
        )
        .await
        .expect("seed");
    let id = match &results[0] {
        StepResult::Insert { id } => id.clone(),
        other => panic!("expected Insert, got {other:?}"),
    };
    c.mutate(&Mutation::new().delete("tasks", &id).build(), None)
        .await
        .expect("soft delete");
    c.mutate(
        &Mutation::new()
            .insert_if("tasks", title_is("a"), json!({"title":"a","updatedAt":2}))
            .build(),
        None,
    )
    .await
    .expect("soft-deleted row is invisible to expectNoMatch");
    assert_eq!(count_title(&mut c, "a").await, 1);
}

#[tokio::test]
async fn expect_no_match_accepts_older_than() {
    let mut c = client(false);
    c.mutate(
        &Mutation::new()
            .insert("tasks", json!({"title":"old","updatedAt":OLD}))
            .insert("tasks", json!({"title":"new","updatedAt":FUTURE}))
            .build(),
        None,
    )
    .await
    .expect("seed");
    let older = FilterExpr::OlderThan {
        field: "updatedAt".into(),
        ms: 1_000_000_000_000,
    };
    let err = c
        .mutate(
            &Mutation::new().expect_no_match("tasks", older).build(),
            None,
        )
        .await
        .unwrap_err();
    assert_no_match_abort(&err);

    // Nothing is older than a cutoff reaching past the FUTURE row.
    c.mutate(
        &Mutation::new()
            .expect_no_match(
                "tasks",
                FilterExpr::And {
                    exprs: vec![
                        title_is("new"),
                        FilterExpr::OlderThan {
                            field: "updatedAt".into(),
                            ms: 1_000_000_000_000,
                        },
                    ],
                },
            )
            .build(),
        None,
    )
    .await
    .expect("the future row is not older than the cutoff");
}
