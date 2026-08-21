//! Server-stamped `updatedAtField` (FM-36) — in-memory harness stamp
//! semantics. The table's declared field is stamped with the current
//! epoch-ms on every version-bumping write path — insert, patch, replace,
//! upsert (both branches), patchByQuery, and cascade setNull — overwriting
//! any client-supplied value. The value form follows the field's wire
//! convention: a JSON number on `number`, a decimal string on `int64`. The
//! stamp wins over a `defaults` entry on the same field (same authority
//! family as the ttl default). Push-time validation mirrors the server:
//! undeclared, non-numeric, and ttl-colliding declarations reject. Mirrors
//! server `tests/updated_at_test.rs`; the wire-shape serde tests live in
//! `src/schema.rs`. Ports the TS client's updatedAt behavior
//! (`ts-client/src/in_memory.ts`).

#![cfg(feature = "in_memory")]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use par_rt_db_client::in_memory::InMemoryRtDbClientOptions;
use par_rt_db_client::schema::Table;
use par_rt_db_client::{
    FieldType, FilterExpr, InMemoryRtDbClient, Mutation, OnDeleteAction, SchemaDef, StepResult,
};
use serde_json::json;

/// Fixed-clock base (mirrors `tests/defaults.rs`): every stamped value below
/// is asserted against this cell, so restamps are exact (no wall-clock sleeps
/// — advance the cell between writes).
const T0: i64 = 1_700_000_000_000_i64;

fn clock_client(schema: &SchemaDef) -> (InMemoryRtDbClient, Arc<Mutex<i64>>) {
    let cell: Arc<Mutex<i64>> = Arc::new(Mutex::new(T0));
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

/// Advance the fixed clock by `ms` (the deterministic stand-in for the
/// server test's `tick()` sleep — restamps must be strictly newer).
fn advance(clock: &Arc<Mutex<i64>>, ms: i64) {
    *clock.lock().expect("clock not poisoned") += ms;
}

/// `tasks` with a number-typed, server-stamped `updatedAt`.
fn number_schema() -> SchemaDef {
    SchemaDef::builder()
        .table(
            "tasks",
            Table::new()
                .field("title", FieldType::String)
                .field("updatedAt", FieldType::Number)
                .index("by_title", &["title"])
                .updated_at_field("updatedAt"),
        )
        .build()
}

/// `tasks` with an int64-typed, server-stamped `updatedAt` (decimal-string
/// wire form).
fn int64_schema() -> SchemaDef {
    SchemaDef::builder()
        .table(
            "tasks",
            Table::new()
                .field("title", FieldType::String)
                .field("updatedAt", FieldType::Int64)
                .index("by_title", &["title"])
                .updated_at_field("updatedAt"),
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
        c.mutate(&Mutation::new().insert("tasks", doc).build(), None)
            .await
            .expect("insert ok"),
    )
}

/// The stamp on a number-typed field, as i64.
fn stamped_number(doc: &serde_json::Value) -> i64 {
    doc["updatedAt"].as_i64().expect("numeric updatedAt stamp")
}

// ---- push-time validation ----

#[test]
fn push_rejects_undeclared_updated_at_field() {
    let schema = SchemaDef::builder()
        .table(
            "tasks",
            Table::new()
                .field("title", FieldType::String)
                .field("updatedAt", FieldType::Number)
                .index("by_title", &["title"])
                .updated_at_field("nope"),
        )
        .build();
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let err = c.push_schema(&schema).unwrap_err();
    assert_eq!(err.code, par_rt_db_client::ErrorCode::SchemaViolation);
    assert!(
        err.message
            .contains("updatedAtField 'nope' is not a declared field"),
        "unexpected error: {}",
        err.message
    );
}

#[test]
fn push_rejects_non_numeric_updated_at_field() {
    let schema = SchemaDef::builder()
        .table(
            "tasks",
            Table::new()
                .field("title", FieldType::String)
                .field("updatedAt", FieldType::String)
                .index("by_title", &["title"])
                .updated_at_field("updatedAt"),
        )
        .build();
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let err = c.push_schema(&schema).unwrap_err();
    assert_eq!(err.code, par_rt_db_client::ErrorCode::SchemaViolation);
    assert!(
        err.message
            .contains("updatedAtField 'updatedAt' must be a number or bigint field"),
        "unexpected error: {}",
        err.message
    );
}

#[test]
fn push_rejects_updated_at_field_matching_ttl_field() {
    let schema = SchemaDef::builder()
        .table(
            "sessions",
            Table::new()
                .field("token", FieldType::String)
                .field("expiresAt", FieldType::Number)
                .index("by_token", &["token"])
                .index("by_expiresAt", &["expiresAt"])
                .ttl("expiresAt", None)
                .updated_at_field("expiresAt"),
        )
        .build();
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let err = c.push_schema(&schema).unwrap_err();
    assert_eq!(err.code, par_rt_db_client::ErrorCode::SchemaViolation);
    assert!(
        err.message.contains("must differ from ttl.field"),
        "unexpected error: {}",
        err.message
    );
}

// ---- stamp semantics: number field ----

#[tokio::test]
async fn insert_stamps_and_overwrites_client_value() {
    let (mut c, _) = clock_client(&number_schema());
    let id = insert(&mut c, json!({"title": "A", "updatedAt": 123})).await;
    let doc = c.get("tasks", &id).expect("doc present");
    // The client's 123 is overwritten with the (injected-clock) epoch-ms.
    assert_eq!(stamped_number(&doc), T0);
}

#[tokio::test]
async fn insert_stamps_int64_field_as_decimal_string() {
    let (mut c, _) = clock_client(&int64_schema());
    let id = insert(&mut c, json!({"title": "A", "updatedAt": "123"})).await;
    let doc = c.get("tasks", &id).expect("doc present");
    // int64 fields hold decimal strings end to end (wire convention).
    let stamped = doc["updatedAt"]
        .as_str()
        .expect("int64 stamp is a decimal string")
        .parse::<i64>()
        .expect("parses as i64");
    assert_eq!(stamped, T0);
}

#[tokio::test]
async fn patch_restamps_and_overwrites_client_value() {
    let (mut c, clock) = clock_client(&number_schema());
    let id = insert(&mut c, json!({"title": "A"})).await;
    let first = stamped_number(&c.get("tasks", &id).expect("doc present"));
    advance(&clock, 5);

    c.mutate(
        &Mutation::new()
            .patch("tasks", &id, json!({"title": "B", "updatedAt": 1}))
            .build(),
        None,
    )
    .await
    .expect("patch ok");
    let doc = c.get("tasks", &id).expect("doc present");
    let second = stamped_number(&doc);
    assert_eq!(second, first + 5, "patch restamps over the client's 1");
    assert_eq!(doc["title"], "B");
}

#[tokio::test]
async fn replace_restamps() {
    let (mut c, clock) = clock_client(&number_schema());
    let id = insert(&mut c, json!({"title": "A"})).await;
    let first = stamped_number(&c.get("tasks", &id).expect("doc present"));
    advance(&clock, 5);

    c.mutate(
        &Mutation::new()
            .replace("tasks", &id, json!({"title": "A2", "updatedAt": 7}))
            .build(),
        None,
    )
    .await
    .expect("replace ok");
    let second = stamped_number(&c.get("tasks", &id).expect("doc present"));
    assert_eq!(second, first + 5, "replace restamps over the client's 7");
}

#[tokio::test]
async fn upsert_insert_stamps_and_update_restamps() {
    let (mut c, clock) = clock_client(&number_schema());
    // Insert branch: no match on by_title — the `insert` doc is stamped.
    let r1 = c
        .mutate(
            &Mutation::new()
                .upsert(
                    "tasks",
                    "by_title",
                    &[json!("A")],
                    json!({"title": "A", "updatedAt": 9}),
                    json!({}),
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
    let first = stamped_number(&c.get("tasks", &id).expect("doc present"));
    assert_eq!(first, T0, "upsert-insert stamps over the client's 9");
    advance(&clock, 5);

    // Update branch: match on by_title — the `patch` fields are stamped.
    let r2 = c
        .mutate(
            &Mutation::new()
                .upsert(
                    "tasks",
                    "by_title",
                    &[json!("A")],
                    json!({"title": "A"}),
                    json!({"title": "A3", "updatedAt": 5}),
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
        ] => assert_eq!(id2, &id, "second upsert patched the same doc"),
        other => panic!("expected Upsert inserted=false, got {other:?}"),
    }
    let second = stamped_number(&c.get("tasks", &id).expect("doc present"));
    assert_eq!(
        second,
        first + 5,
        "upsert-update restamps over the client's 5"
    );
}

#[tokio::test]
async fn patch_by_query_restamps() {
    let (mut c, clock) = clock_client(&number_schema());
    let id = insert(&mut c, json!({"title": "A"})).await;
    let first = stamped_number(&c.get("tasks", &id).expect("doc present"));
    advance(&clock, 5);

    c.mutate(
        &Mutation::new()
            .patch_by_query(
                "tasks",
                FilterExpr::Eq {
                    field: "title".into(),
                    value: json!("A"),
                },
                json!({"updatedAt": 3}),
                None,
            )
            .build(),
        None,
    )
    .await
    .expect("patch_by_query ok");
    let second = stamped_number(&c.get("tasks", &id).expect("doc present"));
    assert_eq!(
        second,
        first + 5,
        "patchByQuery restamps over the client's 3"
    );
}

#[tokio::test]
async fn cascade_set_null_restamps_child() {
    // parents/children with `onDelete: setNull` — deleting the parent patches
    // the child (key removed), which is a version-bumping write and restamps.
    let schema = SchemaDef::builder()
        .table(
            "parents",
            Table::new()
                .field("name", FieldType::String)
                .index("by_name", &["name"]),
        )
        .table(
            "children",
            Table::new()
                .field(
                    "parentId",
                    FieldType::optional(
                        FieldType::id("parents").on_delete(OnDeleteAction::SetNull),
                    ),
                )
                .field("title", FieldType::String)
                .field("updatedAt", FieldType::Number)
                .index("by_parentId", &["parentId"])
                .updated_at_field("updatedAt"),
        )
        .build();
    let (mut c, clock) = clock_client(&schema);

    let parent = insert_id(
        c.mutate(
            &Mutation::new()
                .insert("parents", json!({"name": "P"}))
                .build(),
            None,
        )
        .await
        .expect("insert parent"),
    );
    let child = insert_id(
        c.mutate(
            &Mutation::new()
                .insert("children", json!({"parentId": parent, "title": "C"}))
                .build(),
            None,
        )
        .await
        .expect("insert child"),
    );
    let first = stamped_number(&c.get("children", &child).expect("child present"));
    advance(&clock, 5);

    c.mutate(&Mutation::new().delete("parents", &parent).build(), None)
        .await
        .expect("delete parent");
    let doc = c.get("children", &child).expect("child present");
    assert!(
        !doc.as_object()
            .expect("doc is an object")
            .contains_key("parentId"),
        "setNull removed the ref"
    );
    let second = stamped_number(&doc);
    assert_eq!(second, first + 5, "cascade setNull restamps the child");
}

#[tokio::test]
async fn stamp_wins_over_defaults_entry() {
    // A `defaults` entry on the same field loses to the stamp (the stamp
    // runs before defaults on insert, so the key is present and is skipped).
    let schema = SchemaDef::builder()
        .table(
            "tasks",
            Table::new()
                .field("title", FieldType::String)
                .field("updatedAt", FieldType::Number)
                .index("by_title", &["title"])
                .updated_at_field("updatedAt")
                .defaults(&[("updatedAt", json!(12345))]),
        )
        .build();
    let (mut c, _) = clock_client(&schema);
    let id = insert(&mut c, json!({"title": "A"})).await;
    let doc = c.get("tasks", &id).expect("doc present");
    assert_eq!(
        stamped_number(&doc),
        T0,
        "server stamp wins over the defaults entry (not 12345)"
    );
}
