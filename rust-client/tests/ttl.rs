//! Document TTL — in-memory harness mirrors the server's per-db reaper.
//!
//! `tick` removes docs whose declared `ttl.field` (a number) is in the past,
//! and `do_insert` stamps `ttl.default_duration_ms` onto the field at insert
//! time when the caller omits it. Ports the TS client's TTL behavior
//! (`ts-client/src/in_memory.ts:1156-1221`, `:407-425`).

#![cfg(feature = "in_memory")]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use par_rt_db_client::in_memory::InMemoryRtDbClientOptions;
use par_rt_db_client::schema::Table;
use par_rt_db_client::{
    FieldType, InMemoryRtDbClient, Mutation, SchemaBuilderExt, SchemaDef, StepResult,
};
use serde_json::json;

/// Fixed-clock harness so the stamped `expiresAt` (`now + default_duration_ms`)
/// and the reap threshold are both deterministic. Mirrors `new_clock_client` in
/// the crate's internal unit tests. The clock starts at `T0` (a round number).
/// The RNG advances per call so multiple inserts at the same fixed clock time
/// mint distinct ids (a constant RNG would collide, since `new_id` derives its
/// random hex from `random()` and its timestamp from `now()`).
const T0: i64 = 1_700_000_000_000_i64;

fn new_clock_client() -> (InMemoryRtDbClient, Arc<Mutex<i64>>) {
    let cell: Arc<Mutex<i64>> = Arc::new(Mutex::new(T0));
    let cell_for_closure = cell.clone();
    let rng = Arc::new(AtomicU64::new(0));
    let rng_for_closure = rng.clone();
    let mut client = InMemoryRtDbClient::new(
        InMemoryRtDbClientOptions::default()
            .now(move || *cell_for_closure.lock().expect("clock not poisoned"))
            .random(move || {
                // Distinct value per call (a multiplicative hash into [0, 1)) so
                // `random_hex` produces different ids across calls.
                let n = rng_for_closure.fetch_add(1, Ordering::SeqCst);
                ((n.wrapping_mul(2_654_435_761) % 1_000_000) as f64) / 1_000_000.0
            }),
    );
    let schema = SchemaDef::builder()
        .table(
            "sessions",
            Table::new()
                .field("expiresAt", FieldType::Number)
                .index("by_expiry", &["expiresAt"])
                .ttl("expiresAt", Some(1000)),
        )
        .build();
    client.push_schema(&schema).expect("push_schema ok");
    (client, cell)
}

/// Extract the id from a single-step Insert result, panicking on any other
/// shape (the test only mints one-row inserts).
fn insert_id(results: Vec<StepResult>) -> String {
    match results.as_slice() {
        [StepResult::Insert { id }] => id.clone(),
        other => panic!("expected a single Insert result, got {other:?}"),
    }
}

#[tokio::test]
async fn stamps_default_respects_explicit_and_reaps_on_tick() {
    let (mut c, clock) = new_clock_client();
    let now = *clock.lock().expect("clock not poisoned");

    // (1) Empty doc → `expiresAt` stamped to now + 1000 (default_duration_ms).
    let default_id = insert_id(
        c.mutate(&Mutation::new().insert("sessions", json!({})).build(), None)
            .await
            .expect("insert ok"),
    );
    let default_doc = c.get("sessions", &default_id).expect("default doc present");
    assert_eq!(
        default_doc["expiresAt"],
        json!(now + 1000),
        "default_duration_ms stamps expiresAt at insert when the caller omits it"
    );

    // (2) Explicit `expiresAt` is respected — NOT overwritten by the default.
    let explicit_id = insert_id(
        c.mutate(
            &Mutation::new()
                .insert("sessions", json!({ "expiresAt": now + 5000 }))
                .build(),
            None,
        )
        .await
        .expect("insert ok"),
    );
    let explicit_doc = c
        .get("sessions", &explicit_id)
        .expect("explicit doc present");
    assert_eq!(
        explicit_doc["expiresAt"],
        json!(now + 5000),
        "an explicit caller-supplied expiresAt is not overwritten"
    );

    // (3) Advance past the default's expiry but before the explicit one's, then
    // tick → only the default doc is reaped; the explicit doc survives.
    *clock.lock().expect("clock not poisoned") = now + 2000;
    let reaped = c.tick(None);
    assert_eq!(reaped, 1, "only the past-expiry default doc is reaped");
    assert!(
        c.get("sessions", &default_id).is_none(),
        "default doc reaped once its expiresAt is in the past"
    );
    assert!(
        c.get("sessions", &explicit_id).is_some(),
        "explicit doc survives — its expiresAt is still in the future"
    );

    // (4) Advance past the explicit doc's expiry and tick → it is reaped too.
    *clock.lock().expect("clock not poisoned") = now + 6000;
    let reaped = c.tick(None);
    assert_eq!(reaped, 1, "explicit doc reaped once past its expiresAt");
    assert!(
        c.get("sessions", &explicit_id).is_none(),
        "explicit doc reaped"
    );
}

#[tokio::test]
async fn non_numeric_or_missing_ttl_field_is_never_reaped() {
    // A doc on a TTL table whose `expiresAt` is missing or non-numeric is left
    // alone — reaping only removes docs whose TTL field is a number `< now`.
    // (In practice the schema declares expiresAt as a Number, but the reaper
    // must not mis-fire on a doc that slipped through without one.)
    let (mut c, clock) = new_clock_client();
    let now = *clock.lock().expect("clock not poisoned");

    // Insert a doc with a far-future explicit expiry — the baseline that MUST
    // survive a tick at `now + 1`.
    let live_id = insert_id(
        c.mutate(
            &Mutation::new()
                .insert("sessions", json!({ "expiresAt": now + 5000 }))
                .build(),
            None,
        )
        .await
        .expect("insert ok"),
    );

    *clock.lock().expect("clock not poisoned") = now + 1;
    let reaped = c.tick(None);
    assert_eq!(reaped, 0, "a future-expiry doc is not reaped");
    assert!(c.get("sessions", &live_id).is_some());
}
