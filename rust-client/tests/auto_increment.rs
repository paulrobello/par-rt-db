//! Server-assigned `autoIncrementField` (FM-37) — in-memory harness counter
//! semantics. The table's declared `int64` field is stamped with the next
//! per-table counter value on insert (and upsert's insert branch), AFTER
//! defaults, overwriting any client-supplied value (and any `defaults` entry
//! on the field). After insert the field is immutable: a patch / upsert-update
//! patch / patchByQuery supplying a different value is rejected, and a replace
//! must round-trip the stored value (omitted/null is filled back in; a doc
//! predating the declaration may be first-set). The counter is monotonic but
//! not gap-free — a failed txn consumes its numbers, matching the server's
//! non-transactional `nextval`. A declaration added to a populated table
//! continues past the stored max (the server creates the sequence with
//! `setval(max)`), and a re-push never disturbs an established numbering.
//! Push-time validation mirrors the server: undeclared, non-int64, and
//! ttl/updatedAt-colliding declarations reject. Mirrors server
//! `tests/auto_increment_test.rs` minus its snapshot-import and concurrency
//! cases (the harness has no snapshot surface, and its `&mut self` API admits
//! no concurrent mutates); the wire-shape serde tests live in `src/schema.rs`.

#![cfg(feature = "in_memory")]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use par_rt_db_client::in_memory::InMemoryRtDbClientOptions;
use par_rt_db_client::schema::Table;
use par_rt_db_client::{
    ErrorCode, FieldType, FilterExpr, InMemoryRtDbClient, Mutation, SchemaBuilderExt, SchemaDef,
    StepResult, TableQuery,
};
use serde_json::json;

fn clock_client(schema: &SchemaDef) -> (InMemoryRtDbClient, Arc<Mutex<i64>>) {
    let cell: Arc<Mutex<i64>> = Arc::new(Mutex::new(1_700_000_000_000_i64));
    let cell_for_closure = cell.clone();
    let rng = Arc::new(AtomicU64::new(0));
    let rng_for_closure = rng.clone();
    let mut client = InMemoryRtDbClient::new(
        InMemoryRtDbClientOptions::default()
            .now(move || *cell_for_closure.lock().expect("clock not poisoned"))
            .random(move || {
                let n = rng_for_closure.fetch_add(1, Ordering::SeqCst);
                ((n.wrapping_mul(2_654_435_761) % 1_000_000) as f64) / 1_000_000.0
            }),
    );
    client.push_schema(schema).expect("push_schema ok");
    (client, cell)
}

/// `tickets` with an int64 `num` stamped from the table's counter.
fn counter_schema() -> SchemaDef {
    SchemaDef::builder()
        .table(
            "tickets",
            Table::new()
                .field("title", FieldType::String)
                .field("num", FieldType::Int64)
                .index("by_title", &["title"])
                .auto_increment_field("num"),
        )
        .build()
}

/// Extract the id from a single-step Insert result, panicking on any other
/// shape (the tests only mint one-row inserts).
fn insert_id(results: Vec<StepResult>) -> String {
    match results.as_slice() {
        [StepResult::Insert { id }] => id.clone(),
        other => panic!("expected a single Insert result, got {other:?}"),
    }
}

async fn insert(c: &mut InMemoryRtDbClient, doc: serde_json::Value) -> String {
    insert_id(
        c.mutate(&Mutation::new().insert("tickets", doc).build(), None)
            .await
            .expect("insert ok"),
    )
}

/// The counter value on a stored doc, as a decimal string (the int64 wire
/// convention).
fn counter(c: &InMemoryRtDbClient, id: &str) -> String {
    let doc = c.get("tickets", id).expect("doc present");
    doc["num"]
        .as_str()
        .expect("counter is a decimal string")
        .to_string()
}

/// The error of a failed mutate, unwrapped to the client's RtDbError shape.
async fn mutate_err(
    c: &mut InMemoryRtDbClient,
    txn: &par_rt_db_client::Transaction,
) -> par_rt_db_client::RtDbError {
    c.mutate(txn, None).await.expect_err("mutate fails")
}

// ---- push-time validation ----

#[test]
fn push_rejects_undeclared_auto_increment_field() {
    let schema = SchemaDef::builder()
        .table(
            "tickets",
            Table::new()
                .field("title", FieldType::String)
                .field("num", FieldType::Int64)
                .index("by_title", &["title"])
                .auto_increment_field("nope"),
        )
        .build();
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let err = c.push_schema(&schema).unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert!(
        err.message
            .contains("autoIncrementField 'nope' is not a declared field"),
        "unexpected error: {}",
        err.message
    );
}

#[test]
fn push_rejects_non_int64_auto_increment_field() {
    for ty in [FieldType::String, FieldType::Number] {
        let schema = SchemaDef::builder()
            .table(
                "tickets",
                Table::new()
                    .field("title", FieldType::String)
                    .field("num", ty)
                    .index("by_title", &["title"])
                    .auto_increment_field("num"),
            )
            .build();
        let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
        let err = c.push_schema(&schema).unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);
        assert!(
            err.message
                .contains("autoIncrementField 'num' must be an int64 field"),
            "unexpected error: {}",
            err.message
        );
    }
}

#[test]
fn push_rejects_counter_colliding_with_ttl_or_updated_at() {
    // ttl collision: the reaper would delete counter rows. (The ttl
    // declaration itself needs a single-field btree index on its field to
    // validate; `by_num` satisfies it so the rejection is the collision.)
    let ttl_schema = SchemaDef::builder()
        .table(
            "tickets",
            Table::new()
                .field("title", FieldType::String)
                .field("num", FieldType::Int64)
                .index("by_title", &["title"])
                .index("by_num", &["num"])
                .ttl("num", None)
                .auto_increment_field("num"),
        )
        .build();
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let err = c.push_schema(&ttl_schema).unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert!(
        err.message
            .contains("autoIncrementField 'num' must differ from ttl.field"),
        "unexpected error: {}",
        err.message
    );

    // updatedAtField collision: the timestamp would overwrite the counter on
    // every write.
    let updated_schema = SchemaDef::builder()
        .table(
            "tickets",
            Table::new()
                .field("title", FieldType::String)
                .field("num", FieldType::Int64)
                .index("by_title", &["title"])
                .updated_at_field("num")
                .auto_increment_field("num"),
        )
        .build();
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let err = c.push_schema(&updated_schema).unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert!(
        err.message
            .contains("autoIncrementField 'num' must differ from updatedAtField"),
        "unexpected error: {}",
        err.message
    );
}

// ---- insert authority ----

#[tokio::test]
async fn insert_assigns_sequential_values_and_overwrites_client_value() {
    let (mut c, _) = clock_client(&counter_schema());

    // A client-supplied value (even a plausible one) is overwritten: the
    // first insert is 1 regardless.
    let id = insert(&mut c, json!({"title": "A", "num": "999"})).await;
    assert_eq!(counter(&c, &id), "1");

    let id2 = insert(&mut c, json!({"title": "B"})).await;
    assert_eq!(counter(&c, &id2), "2");

    let id3 = insert(&mut c, json!({"title": "C"})).await;
    assert_eq!(counter(&c, &id3), "3");
}

#[tokio::test]
async fn stamp_wins_over_defaults_entry() {
    // A `defaults` entry on the same field loses to the stamp (the stamp
    // runs after defaults on insert, overwriting the filled value).
    let schema = SchemaDef::builder()
        .table(
            "tickets",
            Table::new()
                .field("title", FieldType::String)
                .field("num", FieldType::Int64)
                .index("by_title", &["title"])
                .auto_increment_field("num")
                .defaults(&[("num", json!("42"))]),
        )
        .build();
    let (mut c, _) = clock_client(&schema);
    let id = insert(&mut c, json!({"title": "A"})).await;
    assert_eq!(
        counter(&c, &id),
        "1",
        "the sequence stamp wins over a defaults entry on the same field"
    );
}

#[tokio::test]
async fn failed_txn_consumes_its_numbers() {
    // The counter is monotonic but not gap-free: a txn whose insert succeeds
    // but rolls back on a later step still consumes its number (the server's
    // `nextval` is non-transactional).
    let (mut c, _) = clock_client(&counter_schema());
    let id = insert(&mut c, json!({"title": "A"})).await;
    assert_eq!(counter(&c, &id), "1");

    let err = mutate_err(
        &mut c,
        &Mutation::new()
            .insert("tickets", json!({"title": "B"}))
            .patch("tickets", "missing-id", json!({"title": "X"}))
            .build(),
    )
    .await;
    assert_eq!(err.code, ErrorCode::NotFound, "second step fails");
    let rows: i64 = c
        .run(&TableQuery::new("tickets").count())
        .expect("count ok");
    assert_eq!(rows, 1, "the rolled-back insert left no row");

    let id3 = insert(&mut c, json!({"title": "C"})).await;
    assert_eq!(
        counter(&c, &id3),
        "3",
        "the rolled-back insert's number (2) is not reclaimed"
    );
}

// ---- post-insert immutability ----

#[tokio::test]
async fn patch_cannot_change_the_counter() {
    let (mut c, _) = clock_client(&counter_schema());
    let id = insert(&mut c, json!({"title": "A"})).await;

    // Changing the value is rejected.
    let err = mutate_err(
        &mut c,
        &Mutation::new()
            .patch("tickets", &id, json!({"num": "99"}))
            .build(),
    )
    .await;
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message
            .contains("autoIncrementField 'num' cannot be changed"),
        "unexpected error: {}",
        err.message
    );

    // Round-tripping the same value is allowed.
    c.mutate(
        &Mutation::new()
            .patch("tickets", &id, json!({"num": "1", "title": "A2"}))
            .build(),
        None,
    )
    .await
    .expect("round-trip patch ok");
    assert_eq!(counter(&c, &id), "1");
    assert_eq!(
        c.get("tickets", &id).expect("doc present")["title"],
        json!("A2"),
        "the non-counter fields of the patch applied"
    );
}

#[tokio::test]
async fn replace_preserves_or_rejects_the_counter() {
    let (mut c, _) = clock_client(&counter_schema());
    let id = insert(&mut c, json!({"title": "A"})).await;

    // A replace that omits the field keeps the stored value (it validates as
    // a complete document only because the engine fills it back in).
    c.mutate(
        &Mutation::new()
            .replace("tickets", &id, json!({"title": "A2"}))
            .build(),
        None,
    )
    .await
    .expect("replace omitting the counter ok");
    assert_eq!(counter(&c, &id), "1");
    assert_eq!(
        c.get("tickets", &id).expect("doc present")["title"],
        json!("A2")
    );

    // A replace that changes the value is rejected.
    let err = mutate_err(
        &mut c,
        &Mutation::new()
            .replace("tickets", &id, json!({"title": "A3", "num": "5"}))
            .build(),
    )
    .await;
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message
            .contains("autoIncrementField 'num' cannot be changed"),
        "unexpected error: {}",
        err.message
    );

    // Round-tripping the stored value works.
    c.mutate(
        &Mutation::new()
            .replace("tickets", &id, json!({"title": "A4", "num": "1"}))
            .build(),
        None,
    )
    .await
    .expect("round-trip replace ok");
    assert_eq!(counter(&c, &id), "1");
    assert_eq!(
        c.get("tickets", &id).expect("doc present")["title"],
        json!("A4")
    );
}

#[tokio::test]
async fn upsert_insert_assigns_and_update_preserves() {
    let (mut c, _) = clock_client(&counter_schema());

    // Insert branch: no match on by_title — the `insert` doc is stamped.
    let r1 = c
        .mutate(
            &Mutation::new()
                .upsert(
                    "tickets",
                    "by_title",
                    &[json!("A")],
                    json!({"title": "A"}),
                    json!({"title": "A"}),
                )
                .build(),
            None,
        )
        .await
        .expect("upsert ok");
    let id = match r1.as_slice() {
        [StepResult::Upsert { id, inserted: true }] => id.clone(),
        other => panic!("expected Upsert inserted=true, got {other:?}"),
    };
    assert_eq!(counter(&c, &id), "1");

    // Update branch: a patch without the counter preserves it.
    let r2 = c
        .mutate(
            &Mutation::new()
                .upsert(
                    "tickets",
                    "by_title",
                    &[json!("A")],
                    json!({"title": "A"}),
                    json!({"title": "A2"}),
                )
                .build(),
            None,
        )
        .await
        .expect("upsert ok");
    match r2.as_slice() {
        [
            StepResult::Upsert {
                id: id2,
                inserted: false,
            },
        ] => assert_eq!(id2, &id),
        other => panic!("expected Upsert inserted=false, got {other:?}"),
    }
    assert_eq!(counter(&c, &id), "1");

    // Update branch: changing the counter is rejected.
    let err = mutate_err(
        &mut c,
        &Mutation::new()
            .upsert(
                "tickets",
                "by_title",
                &[json!("A2")],
                json!({"title": "A2"}),
                json!({"num": "7"}),
            )
            .build(),
    )
    .await;
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message
            .contains("autoIncrementField 'num' cannot be changed"),
        "unexpected error: {}",
        err.message
    );
}

#[tokio::test]
async fn patch_by_query_cannot_change_the_counter() {
    let (mut c, _) = clock_client(&counter_schema());
    let id = insert(&mut c, json!({"title": "A"})).await;

    let err = mutate_err(
        &mut c,
        &Mutation::new()
            .patch_by_query(
                "tickets",
                FilterExpr::Eq {
                    field: "title".into(),
                    value: json!("A"),
                },
                json!({"num": "50"}),
                None,
            )
            .build(),
    )
    .await;
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message
            .contains("autoIncrementField 'num' cannot be changed"),
        "unexpected error: {}",
        err.message
    );
    assert_eq!(counter(&c, &id), "1");
}

// ---- counter positioning across schema pushes ----

#[tokio::test]
async fn declaration_added_to_populated_table_repositions_past_max() {
    // v1: plain int64 field, client-supplied values 1..=5 (no counter yet).
    let plain = SchemaDef::builder()
        .table(
            "tickets",
            Table::new()
                .field("title", FieldType::String)
                .field("num", FieldType::Int64)
                .index("by_title", &["title"]),
        )
        .build();
    let (mut c, _) = clock_client(&plain);
    for i in 1..=5 {
        insert(
            &mut c,
            json!({"title": format!("t{i}"), "num": i.to_string()}),
        )
        .await;
    }

    // v2: same schema plus the declaration — additive push.
    c.push_schema(&counter_schema()).expect("additive push ok");

    let id = insert(&mut c, json!({"title": "new"})).await;
    assert_eq!(
        counter(&c, &id),
        "6",
        "the counter is repositioned past the stored max, not restarted at 1"
    );
}

#[tokio::test]
async fn re_push_does_not_disturb_the_counter() {
    let (mut c, _) = clock_client(&counter_schema());
    insert(&mut c, json!({"title": "A"})).await;
    insert(&mut c, json!({"title": "B"})).await;

    // An unrelated additive push (new optional field) must not reposition
    // anything.
    let evolved = SchemaDef::builder()
        .table(
            "tickets",
            Table::new()
                .field("title", FieldType::String)
                .field("num", FieldType::Int64)
                .field("owner", FieldType::optional(FieldType::String))
                .index("by_title", &["title"])
                .auto_increment_field("num"),
        )
        .build();
    c.push_schema(&evolved).expect("additive push ok");

    let id = insert(&mut c, json!({"title": "C"})).await;
    assert_eq!(counter(&c, &id), "3");
}

#[tokio::test]
async fn replace_first_sets_on_doc_predating_the_declaration() {
    // v1 has no `num` field at all; the doc written under it PREDATES the
    // declaration and has no value to preserve, so a replace may set one —
    // first-set, like an insert.
    let v1 = SchemaDef::builder()
        .table(
            "tickets",
            Table::new()
                .field("title", FieldType::String)
                .index("by_title", &["title"]),
        )
        .build();
    let (mut c, _) = clock_client(&v1);
    let id = insert(&mut c, json!({"title": "A"})).await;

    // v2 adds the int64 field AND the declaration — additive push.
    c.push_schema(&counter_schema()).expect("additive push ok");

    c.mutate(
        &Mutation::new()
            .replace("tickets", &id, json!({"title": "A2", "num": "9"}))
            .build(),
        None,
    )
    .await
    .expect("first-set replace ok");
    assert_eq!(counter(&c, &id), "9");

    // A doc predating the declaration but first-set by replace then anchors
    // the counter: the next insert continues past the set value.
    let id2 = insert(&mut c, json!({"title": "B"})).await;
    assert_eq!(
        counter(&c, &id2),
        "10",
        "the first-set value repositions the lazy counter past it"
    );
}

// ---- unique index ----

#[tokio::test]
async fn unique_index_counter_stays_distinct_and_first_set_duplicate_conflicts() {
    // The counter can never hand out duplicates, so sequential inserts stay
    // conflict-free under a unique index; the CONFLICT path is a first-set
    // replace duplicating another row's value (the server's equivalent
    // arrives by snapshot replay, which the harness has no surface for).
    let schema = SchemaDef::builder()
        .table(
            "tickets",
            Table::new()
                .field("title", FieldType::String)
                .field("num", FieldType::Int64)
                .index("by_title", &["title"])
                .index("by_num", &["num"])
                .unique()
                .auto_increment_field("num"),
        )
        .build();
    let (mut c, _) = clock_client(&schema);
    let a = insert(&mut c, json!({"title": "A"})).await;
    let b = insert(&mut c, json!({"title": "B"})).await;
    assert_ne!(counter(&c, &a), counter(&c, &b), "distinct counters");

    // First-set replace onto a duplicate value conflicts: a v1 doc with NO
    // num field predates the declaration (first-set allowed), but the value
    // it sets collides with a counter-assigned row under the unique index.
    // (The server's equivalent duplicate arrives by snapshot replay, which
    // the harness has no surface for.)
    let v1 = SchemaDef::builder()
        .table(
            "tickets",
            Table::new()
                .field("title", FieldType::String)
                .index("by_title", &["title"]),
        )
        .build();
    let (mut c2, _) = clock_client(&v1);
    let old = insert(&mut c2, json!({"title": "A"})).await;
    c2.push_schema(&schema).expect("additive push ok");
    let b2 = insert(&mut c2, json!({"title": "B"})).await;
    let b2_num = counter(&c2, &b2);
    let err = mutate_err(
        &mut c2,
        &Mutation::new()
            .replace("tickets", &old, json!({"title": "A2", "num": b2_num}))
            .build(),
    )
    .await;
    assert_eq!(
        err.code,
        ErrorCode::Conflict,
        "first-set duplicate under the unique index conflicts"
    );
}
