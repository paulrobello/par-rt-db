//! Field-level default values (FM-32) — in-memory harness apply semantics.
//!
//! A table's `defaults` map stamps a NEW document (insert / replace /
//! upsert-insert) for every key it omits, right after the ttl stamp (so a ttl
//! `default_duration_ms` on the same field wins) and before any principal
//! stamps. `patch`, upsert-update, and patch-by-query never re-apply, so
//! clearing an optional field stays cleared. Mirrors server
//! `txn.rs::apply_defaults`; the wire-shape serde tests live in
//! `src/schema.rs`. Ports the TS client's defaults behavior
//! (`ts-client/src/in_memory.ts`).

#![cfg(feature = "in_memory")]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use par_rt_db_client::in_memory::InMemoryRtDbClientOptions;
use par_rt_db_client::schema::Table;
use par_rt_db_client::{
    FieldType, FilterExpr, InMemoryRtDbClient, Mutation, SchemaDef, StepResult,
};
use serde_json::json;

/// Fixed-clock harness (mirrors `tests/ttl.rs`'s `new_clock_client`) so the
/// ttl-wins test's stamped `now + default_duration_ms` is deterministic, and
/// ids stay distinct across inserts (the RNG advances per call).
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

/// `tasks` carries one default per kind: a required string, a required number,
/// and an optional string (so the "cleared optional stays cleared" cases have a
/// field to clear).
fn tasks_schema() -> SchemaDef {
    SchemaDef::builder()
        .table(
            "tasks",
            Table::new()
                .field("title", FieldType::String)
                .field("status", FieldType::String)
                .field("priority", FieldType::Number)
                .field("note", FieldType::optional(FieldType::String))
                .index("by_title", &["title"])
                .defaults(&[
                    ("status", json!("backlog")),
                    ("priority", json!(0)),
                    ("note", json!("n/a")),
                ]),
        )
        .build()
}

/// `expiresAt` is targeted by BOTH a ttl `default_duration_ms` and a `defaults`
/// entry — the ttl stamp must win. `kind` shows the rest of the defaults map
/// still applies.
fn ttl_conflict_schema() -> SchemaDef {
    SchemaDef::builder()
        .table(
            "sessions",
            Table::new()
                .field("expiresAt", FieldType::Number)
                .field("kind", FieldType::String)
                .index("by_expiry", &["expiresAt"])
                .ttl("expiresAt", Some(1000))
                .defaults(&[
                    ("expiresAt", json!(9_000_000_000_000_i64)),
                    ("kind", json!("web")),
                ]),
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

/// Assert `field` is entirely absent from the stored doc (the single
/// representation of an unset optional) — indexing a missing key would yield
/// `Value::Null` and conflate "cleared" with "null".
fn assert_key_absent(doc: &serde_json::Value, field: &str) {
    assert!(
        !doc.as_object()
            .expect("doc is an object")
            .contains_key(field),
        "field '{field}' must stay absent after being cleared, got: {doc}"
    );
}

#[tokio::test]
async fn insert_stamps_defaults_for_omitted_keys() {
    let (mut c, _) = clock_client(&tasks_schema());
    // `status`/`priority` are REQUIRED and omitted — the defaults must stamp
    // them before validation, or the insert would fail with SchemaViolation.
    let id = insert_id(
        c.mutate(
            &Mutation::new()
                .insert("tasks", json!({"title": "a"}))
                .build(),
            None,
        )
        .await
        .expect("insert ok"),
    );
    let doc = c.get("tasks", &id).expect("doc present");
    assert_eq!(doc["status"], json!("backlog"));
    assert_eq!(doc["priority"], json!(0));
    assert_eq!(doc["note"], json!("n/a"));
}

#[tokio::test]
async fn insert_keeps_client_values_over_defaults() {
    let (mut c, _) = clock_client(&tasks_schema());
    let id = insert_id(
        c.mutate(
            &Mutation::new()
                .insert(
                    "tasks",
                    json!({"title": "b", "status": "done", "priority": 5, "note": "hi"}),
                )
                .build(),
            None,
        )
        .await
        .expect("insert ok"),
    );
    let doc = c.get("tasks", &id).expect("doc present");
    assert_eq!(doc["status"], json!("done"));
    assert_eq!(doc["priority"], json!(5));
    assert_eq!(doc["note"], json!("hi"));
}

#[tokio::test]
async fn patch_does_not_reapply_after_clearing_an_optional() {
    let (mut c, _) = clock_client(&tasks_schema());
    let id = insert_id(
        c.mutate(
            &Mutation::new()
                .insert("tasks", json!({"title": "c"}))
                .build(),
            None,
        )
        .await
        .expect("insert ok"),
    );
    let doc = c.get("tasks", &id).expect("doc present");
    assert_eq!(doc["note"], json!("n/a"), "insert stamped the default note");

    // Clearing the optional to null deletes the key; a later read must NOT
    // re-stamp the default (patch never re-applies).
    c.mutate(
        &Mutation::new()
            .patch("tasks", &id, json!({"note": null}))
            .build(),
        None,
    )
    .await
    .expect("patch ok");
    let doc = c.get("tasks", &id).expect("doc present");
    assert_key_absent(&doc, "note");
    // Sibling defaults are untouched by the patch path.
    assert_eq!(doc["status"], json!("backlog"));
}

#[tokio::test]
async fn replace_reapplies_defaults() {
    let (mut c, _) = clock_client(&tasks_schema());
    let id = insert_id(
        c.mutate(
            &Mutation::new()
                .insert("tasks", json!({"title": "d", "status": "done"}))
                .build(),
            None,
        )
        .await
        .expect("insert ok"),
    );
    // Replace with a doc omitting every defaulted field — replace is a
    // new-document path, so the defaults re-apply (and satisfy the required
    // `status`/`priority` fields).
    c.mutate(
        &Mutation::new()
            .replace("tasks", &id, json!({"title": "d"}))
            .build(),
        None,
    )
    .await
    .expect("replace ok");
    let doc = c.get("tasks", &id).expect("doc present");
    assert_eq!(
        doc["status"],
        json!("backlog"),
        "replace re-applied default"
    );
    assert_eq!(doc["priority"], json!(0));
    assert_eq!(doc["note"], json!("n/a"));
    assert_eq!(doc["_version"], 2, "replace bumped version");
}

#[tokio::test]
async fn upsert_insert_applies_and_update_does_not() {
    let (mut c, _) = clock_client(&tasks_schema());
    // Insert branch: no match on by_title, so the `insert` doc lands with its
    // omitted defaulted fields stamped.
    let r1 = c
        .mutate(
            &Mutation::new()
                .upsert(
                    "tasks",
                    "by_title",
                    &[json!("e")],
                    json!({"title": "e"}),
                    json!({"priority": 9}),
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
    let doc = c.get("tasks", &id).expect("doc present");
    assert_eq!(
        doc["status"],
        json!("backlog"),
        "upsert-insert stamped default"
    );

    // Update branch: match on by_title, the `patch` clears the optional
    // default — the update path never re-applies, so it stays cleared.
    let r2 = c
        .mutate(
            &Mutation::new()
                .upsert(
                    "tasks",
                    "by_title",
                    &[json!("e")],
                    json!({"title": "e"}),
                    json!({"note": null}),
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
    let doc = c.get("tasks", &id).expect("doc present");
    assert_key_absent(&doc, "note");
    assert_eq!(
        doc["status"],
        json!("backlog"),
        "a previously stamped default survives the update branch untouched"
    );
}

#[tokio::test]
async fn patch_by_query_does_not_reapply_defaults() {
    let (mut c, _) = clock_client(&tasks_schema());
    let id = insert_id(
        c.mutate(
            &Mutation::new()
                .insert("tasks", json!({"title": "f"}))
                .build(),
            None,
        )
        .await
        .expect("insert ok"),
    );
    c.mutate(
        &Mutation::new()
            .patch_by_query(
                "tasks",
                FilterExpr::Eq {
                    field: "title".into(),
                    value: json!("f"),
                },
                json!({"note": null}),
                None,
            )
            .build(),
        None,
    )
    .await
    .expect("patch_by_query ok");
    let doc = c.get("tasks", &id).expect("doc present");
    assert_key_absent(&doc, "note");
}

#[tokio::test]
async fn ttl_default_wins_over_defaults_entry_on_same_field() {
    let (mut c, clock) = clock_client(&ttl_conflict_schema());
    let now = *clock.lock().expect("clock not poisoned");
    let id = insert_id(
        c.mutate(&Mutation::new().insert("sessions", json!({})).build(), None)
            .await
            .expect("insert ok"),
    );
    let doc = c.get("sessions", &id).expect("doc present");
    // The ttl stamp runs first, so `apply_defaults` sees the key present and
    // leaves it — the ttl value wins over the same-field defaults entry.
    assert_eq!(
        doc["expiresAt"],
        json!(now + 1000),
        "ttl default_duration_ms must win over a defaults entry on the same field"
    );
    // Defaults on OTHER fields still apply.
    assert_eq!(doc["kind"], json!("web"));
}
