//! Fine-grained subscription invalidation v2 — soundness matrix.
//!
//! Integration tests for the `Indexed` (eq-prefix + range) skip in
//! `subs::fan_out`. These exercise the real committer path
//! (`state.realtime.committers.mutate`) so fan_out actually runs, and assert
//! pushes / no-pushes by draining the subscription's `tx` receiver. The
//! `mutate().await?` call returns only after the committer has fully processed
//! the txn INCLUDING fan_out, so any push is already buffered (or the skip
//! already decided) by the time we assert — `try_recv()` is reliable with no
//! grace period. NEVER call `execute_txn` directly here: it bypasses the
//! committer and won't fan out.
//!
//! Unit-level coverage (ReadSet derivation, `in_window` typing) lives in
//! `src/subs.rs`'s `#[cfg(test)]` module. These integration tests cover the
//! end-to-end push/no-push behavior against the real dev Postgres.

mod common;

use std::time::Duration;

use common::{fresh_db, test_state};
use rtdb_server::ddl;
use rtdb_server::protocol::ServerMessage;
use rtdb_server::query::Query;
use rtdb_server::schema::SchemaDef;
use rtdb_server::subs::next_conn_id;
use rtdb_server::txn::{Step, Transaction, TxnOutcome};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::error::TryRecvError;

// ---- doc + txn helpers (kanban workItems fixture) ----

const PROJ: &str = "0123456789abcdef0123456789abcdef";
const OTHER_PROJ: &str = "fedcba9876543210fedcba9876543210";

fn work_item(status: &str, order: f64) -> serde_json::Map<String, serde_json::Value> {
    serde_json::json!({
        "projectId": PROJ,
        "title": "item",
        "status": status,
        "order": order,
        "completedAt": null
    })
    .as_object()
    .expect("json object")
    .clone()
}

fn insert(status: &str, order: f64) -> Transaction {
    Transaction {
        steps: vec![Step::Insert {
            table: "workItems".to_string(),
            doc: work_item(status, order),
        }],
    }
}

/// Patches exactly one field on `id`. `field` must be a declared workItems field.
fn patch_field(id: &str, field: &str, value: serde_json::Value) -> Transaction {
    let mut fields = serde_json::Map::new();
    fields.insert(field.to_string(), value);
    Transaction {
        steps: vec![Step::Patch {
            table: "workItems".to_string(),
            id: id.to_string(),
            fields,
        }],
    }
}

fn delete(id: &str) -> Transaction {
    Transaction {
        steps: vec![Step::Delete {
            table: "workItems".to_string(),
            id: id.to_string(),
        }],
    }
}

/// Extracts the id from an Insert step's `{ "id": "..." }` result.
fn id_of(outcome: &TxnOutcome) -> String {
    outcome.results[0]["id"]
        .as_str()
        .expect("insert result has an id")
        .to_string()
}

// ---- query helpers ----

fn count_by_status(status: &str) -> Query {
    serde_json::from_value(serde_json::json!({
        "table": "workItems",
        "index": "by_status",
        "eq": [status],
        "count": true
    }))
    .expect("parse query")
}

fn collect_by_status(status: &str) -> Query {
    serde_json::from_value(serde_json::json!({
        "table": "workItems",
        "index": "by_status",
        "eq": [status]
    }))
    .expect("parse query")
}

fn collect_by_project_gte_order(proj: &str, gte: f64) -> Query {
    serde_json::from_value(serde_json::json!({
        "table": "workItems",
        "index": "by_project_and_order",
        "eq": [proj],
        "gte": gte
    }))
    .expect("parse query")
}

fn unique_by_project_status(proj: &str, status: &str) -> Query {
    serde_json::from_value(serde_json::json!({
        "table": "workItems",
        "index": "by_project_and_status",
        "eq": [proj, status],
        "unique": true
    }))
    .expect("parse query")
}

fn take_by_status(status: &str, n: u32) -> Query {
    serde_json::from_value(serde_json::json!({
        "table": "workItems",
        "index": "by_status",
        "eq": [status],
        "take": n
    }))
    .expect("parse query")
}

// ---- assertion helpers ----

/// Drains the initial QueryUpdate sent right after `subscribe`. Panics if
/// absent — the committer always sends exactly one before registering.
fn drain_initial(rx: &mut UnboundedReceiver<ServerMessage>) {
    match rx.try_recv() {
        Ok(ServerMessage::QueryUpdate { .. }) => {}
        other => panic!("expected initial QueryUpdate, got {other:?}"),
    }
}

/// Asserts a QueryUpdate was pushed within a short timeout.
async fn expect_update(rx: &mut UnboundedReceiver<ServerMessage>, label: &str) {
    match tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
        Ok(Some(ServerMessage::QueryUpdate { .. })) => {}
        Ok(other) => panic!("{label}: expected QueryUpdate, got {other:?}"),
        Err(_) => panic!("{label}: timed out waiting for push"),
    }
}

/// Asserts NO QueryUpdate is pending. Safe without a grace period because
/// `mutate().await?` returns only after the committer's fan_out has fully
/// completed — the push (if any) is already buffered, or the skip already
/// decided, by the time this runs.
fn expect_no_update(rx: &mut UnboundedReceiver<ServerMessage>, label: &str) {
    match rx.try_recv() {
        Err(TryRecvError::Empty) => {}
        Ok(other) => panic!("{label}: expected no update, got {other:?}"),
        Err(e) => panic!("{label}: channel error {e:?}"),
    }
}

/// Subscribes and returns the receiver after draining the initial result.
async fn sub(
    state: &std::sync::Arc<rtdb_server::AppState>,
    db: &str,
    query: Query,
) -> UnboundedReceiver<ServerMessage> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .realtime
        .committers
        .subscribe(db, next_conn_id(), "q".to_string(), query, tx, None)
        .await
        .expect("subscribe");
    let mut rx = rx;
    drain_initial(&mut rx);
    rx
}

// =====================================================================
// 1. count / collect / unique skip on out-of-window inserts; push in-window
// =====================================================================

#[tokio::test]
async fn count_skips_out_of_window_pushes_in_window() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let mut rx = sub(&state, &db, count_by_status("backlog")).await;

    // Out-of-window insert (status=done, sub is on backlog) — must NOT push.
    state
        .realtime
        .committers
        .mutate(&db, None, insert("done", 1.0), None)
        .await?;
    expect_no_update(&mut rx, "out-of-window insert");

    // In-window insert (status=backlog) — must push (count 0 → 1).
    state
        .realtime
        .committers
        .mutate(&db, None, insert("backlog", 2.0), None)
        .await?;
    expect_update(&mut rx, "in-window insert").await;

    Ok(())
}

#[tokio::test]
async fn collect_skips_out_of_window_pushes_in_window() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let mut rx = sub(&state, &db, collect_by_status("backlog")).await;

    state
        .realtime
        .committers
        .mutate(&db, None, insert("done", 1.0), None)
        .await?;
    expect_no_update(&mut rx, "out-of-window insert");

    state
        .realtime
        .committers
        .mutate(&db, None, insert("backlog", 2.0), None)
        .await?;
    expect_update(&mut rx, "in-window insert").await;

    Ok(())
}

#[tokio::test]
async fn unique_skips_out_of_window_pushes_in_window() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let mut rx = sub(&state, &db, unique_by_project_status(PROJ, "backlog")).await;

    // Wrong status on the same project — does not match the eq-prefix.
    state
        .realtime
        .committers
        .mutate(&db, None, insert("done", 1.0), None)
        .await?;
    expect_no_update(&mut rx, "wrong-status insert");

    // Wrong project, right status — does not match the eq-prefix either.
    let mut other = work_item("backlog", 2.0);
    other.insert(
        "projectId".to_string(),
        serde_json::Value::String(OTHER_PROJ.to_string()),
    );
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            Transaction {
                steps: vec![Step::Insert {
                    table: "workItems".to_string(),
                    doc: other,
                }],
            },
            None,
        )
        .await?;
    expect_no_update(&mut rx, "wrong-project insert");

    // Matching insert — unique goes from Doc(None) to Doc(Some) → push.
    state
        .realtime
        .committers
        .mutate(&db, None, insert("backlog", 3.0), None)
        .await?;
    expect_update(&mut rx, "matching insert").await;

    Ok(())
}

// =====================================================================
// 2. Range: collect with eq=[x], gte=N — in/out of range
// =====================================================================

#[tokio::test]
async fn range_collect_skips_below_range_pushes_in_range() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let mut rx = sub(&state, &db, collect_by_project_gte_order(PROJ, 10.0)).await;

    // In eq-prefix (project) but below the range bound (order=5 < 10) — skip.
    state
        .realtime
        .committers
        .mutate(&db, None, insert("backlog", 5.0), None)
        .await?;
    expect_no_update(&mut rx, "below-range insert");

    // In eq-prefix AND in range (order=20 >= 10) — push.
    state
        .realtime
        .committers
        .mutate(&db, None, insert("backlog", 20.0), None)
        .await?;
    expect_update(&mut rx, "in-range insert").await;

    // Wrong project (different eq-prefix) — skip regardless of order.
    let mut other = work_item("backlog", 30.0);
    other.insert(
        "projectId".to_string(),
        serde_json::Value::String(OTHER_PROJ.to_string()),
    );
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            Transaction {
                steps: vec![Step::Insert {
                    table: "workItems".to_string(),
                    doc: other,
                }],
            },
            None,
        )
        .await?;
    expect_no_update(&mut rx, "wrong-project insert");

    Ok(())
}

// =====================================================================
// 3. Patch-a-member pushes for collect (content-bearing) but NOT count
// =====================================================================

#[tokio::test]
async fn body_patch_pushes_collect_not_count() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    // Seed one backlog item.
    let outcome = state
        .realtime
        .committers
        .mutate(&db, None, insert("backlog", 1.0), None)
        .await?;
    let id = id_of(&outcome);

    // Two subs on the same eq window: collect (content-bearing) + count.
    let mut rx_collect = sub(&state, &db, collect_by_status("backlog")).await;
    let mut rx_count = sub(&state, &db, count_by_status("backlog")).await;

    // Patch the title — eq unchanged, body changed.
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            patch_field(&id, "title", serde_json::Value::String("renamed".into())),
            None,
        )
        .await?;

    // collect returns doc bodies ⇒ body change matters ⇒ push.
    expect_update(&mut rx_collect, "collect body patch").await;
    // count returns only a cardinality ⇒ membership unchanged ⇒ no push.
    expect_no_update(&mut rx_count, "count body patch");

    Ok(())
}

// =====================================================================
// 4. Patch OUT of the window pushes (regression guard for `before` capture)
//    This is the case that would silently drop a push if `before` were not
//    captured: with only `after` visible, fan_out would see the doc outside
//    the window and skip — missing that it WAS a member.
// =====================================================================

#[tokio::test]
async fn patch_out_of_window_pushes_collect() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let outcome = state
        .realtime
        .committers
        .mutate(&db, None, insert("backlog", 1.0), None)
        .await?;
    let id = id_of(&outcome);

    let mut rx = sub(&state, &db, collect_by_status("backlog")).await;

    // Move the doc OUT of the window (backlog → done). It was a member, so
    // collect (content_bearing: in_window(before)=true) ⇒ affects ⇒ push.
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            patch_field(&id, "status", serde_json::Value::String("done".into())),
            None,
        )
        .await?;
    expect_update(&mut rx, "collect patch-out-of-window").await;

    Ok(())
}

#[tokio::test]
async fn patch_out_of_window_pushes_count() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let outcome = state
        .realtime
        .committers
        .mutate(&db, None, insert("backlog", 1.0), None)
        .await?;
    let id = id_of(&outcome);

    let mut rx = sub(&state, &db, count_by_status("backlog")).await;

    // backlog → done: membership flipped (in→out) ⇒ count decreased ⇒ push.
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            patch_field(&id, "status", serde_json::Value::String("done".into())),
            None,
        )
        .await?;
    expect_update(&mut rx, "count patch-out-of-window").await;

    Ok(())
}

#[tokio::test]
async fn patch_into_window_pushes_count_and_collect() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    // Seed a `done` item (outside the backlog window).
    let outcome = state
        .realtime
        .committers
        .mutate(&db, None, insert("done", 1.0), None)
        .await?;
    let id = id_of(&outcome);

    let mut rx_collect = sub(&state, &db, collect_by_status("backlog")).await;
    let mut rx_count = sub(&state, &db, count_by_status("backlog")).await;

    // done → backlog: membership flipped (out→in) ⇒ both push.
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            patch_field(&id, "status", serde_json::Value::String("backlog".into())),
            None,
        )
        .await?;
    expect_update(&mut rx_collect, "collect patch-into-window").await;
    expect_update(&mut rx_count, "count patch-into-window").await;

    Ok(())
}

// =====================================================================
// 5. Delete always pushes
// =====================================================================

#[tokio::test]
async fn delete_always_pushes() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let outcome = state
        .realtime
        .committers
        .mutate(&db, None, insert("backlog", 1.0), None)
        .await?;
    let id = id_of(&outcome);

    let mut rx_collect = sub(&state, &db, collect_by_status("backlog")).await;
    let mut rx_count = sub(&state, &db, count_by_status("backlog")).await;

    // Deleting a member always affects (after=None ⇒ re-run).
    state
        .realtime
        .committers
        .mutate(&db, None, delete(&id), None)
        .await?;
    expect_update(&mut rx_collect, "collect delete").await;
    expect_update(&mut rx_count, "count delete").await;

    Ok(())
}

#[tokio::test]
async fn delete_of_out_of_window_doc_still_pushes() -> anyhow::Result<()> {
    // A delete always re-runs (deleted ⇒ affects=true per the spec), even for
    // a doc that was NEVER in the window. The re-run is the conservative
    // choice (after=None ⇒ always-affecting); the canonical diff then
    // suppresses the push because deleting a doc that wasn't in the result
    // leaves the result unchanged. So from the outside this looks like "no
    // push" — the re-run is internal and not observable here. The
    // affects=true guarantee is unit-tested in `affects_deleted_is_always_true`.
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let outcome = state
        .realtime
        .committers
        .mutate(&db, None, insert("done", 1.0), None)
        .await?;
    let id = id_of(&outcome);

    let mut rx = sub(&state, &db, count_by_status("backlog")).await;

    state
        .realtime
        .committers
        .mutate(&db, None, delete(&id), None)
        .await?;
    // Re-run happens (affects=true) but result unchanged (count stays 0) ⇒
    // diff suppresses the push ⇒ no observable push.
    expect_no_update(
        &mut rx,
        "delete of out-of-window doc (re-run, diff-suppressed)",
    );

    Ok(())
}

// =====================================================================
// 6. Truncating/value-sensitive terminals stay table-level
//    (ReadSet derivation is unit-tested in subs.rs; here we confirm these
//    subs still push correctly for in-window writes — correctness preserved.)
// =====================================================================

#[tokio::test]
async fn take_sub_pushes_for_in_window_write() -> anyhow::Result<()> {
    // A `take` sub is `Ordered` (v3): it re-runs when a written doc is inside
    // the eq window AND ranks at or before the last result's boundary. Here
    // the sub starts empty (unfull ⇒ unbounded boundary), so any in-window
    // insert affects it ⇒ push.
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let mut rx = sub(&state, &db, take_by_status("backlog", 10)).await;

    state
        .realtime
        .committers
        .mutate(&db, None, insert("backlog", 1.0), None)
        .await?;
    expect_update(&mut rx, "take in-window insert").await;

    Ok(())
}

// =====================================================================
// 7. Owner-filtered + filter-bearing collect over-approximates
//    (filter is ignored in the skip decision; owner filtering is applied
//    at re-run time, so a not-visible-but-matching doc re-runs harmlessly)
// =====================================================================

/// A schema with an owner-gated `notes` table (ownerField=userId), used to
/// exercise per-row auth interaction with Indexed invalidation.
fn owner_schema_json() -> serde_json::Value {
    serde_json::json!({"tables":{
        "notes":{
            "fields":{
                "userId":{"type":"string"},
                "category":{"type":"string"},
                "body":{"type":"string"}
            },
            "indexes":[{"name":"by_category","fields":["category"]}],
            "ownerField":"userId"
        }
    }})
}

async fn fresh_owner_db(state: &std::sync::Arc<rtdb_server::AppState>) -> String {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create db");
    let schema: SchemaDef =
        serde_json::from_value(owner_schema_json()).expect("parse owner schema");
    ddl::push_schema(&state.pool, &name, schema)
        .await
        .expect("push owner schema");
    name
}

fn note(category: &str, body: &str) -> serde_json::Map<String, serde_json::Value> {
    serde_json::json!({ "category": category, "body": body })
        .as_object()
        .unwrap()
        .clone()
}

fn insert_note(category: &str, body: &str) -> Transaction {
    Transaction {
        steps: vec![Step::Insert {
            table: "notes".to_string(),
            doc: note(category, body),
        }],
    }
}

fn collect_notes_by_category(category: &str) -> Query {
    serde_json::from_value(serde_json::json!({
        "table": "notes",
        "index": "by_category",
        "eq": [category]
    }))
    .expect("parse query")
}

#[tokio::test]
async fn owner_filtered_collect_over_approximates_for_other_users_doc() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_owner_db(&state).await;

    // Subscribe as userA: collect notes by_category="work". The subscription's
    // re-runs are filtered to userA's rows (owner=userA).
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .realtime
        .committers
        .subscribe(
            &db,
            next_conn_id(),
            "q".to_string(),
            collect_notes_by_category("work"),
            tx,
            Some("userA".to_string()),
        )
        .await?;
    drain_initial(&mut rx);

    // userA writes a matching doc — visible, so push.
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_note("work", "a"),
            Some("userA".to_string()),
        )
        .await?;
    expect_update(&mut rx, "userA matching insert").await;

    // userB writes a matching-eq doc (category=work). Indexed: in_window(eq=
    // "work") ⇒ true ⇒ re-run (over-approximation). Re-run filters to userA ⇒
    // userB's doc invisible ⇒ result unchanged ⇒ no push (diff-suppressed).
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_note("work", "b-secret"),
            Some("userB".to_string()),
        )
        .await?;
    expect_no_update(
        &mut rx,
        "userB matching insert (over-approx, diff-suppressed)",
    );

    // userB writes a NON-matching-eq doc (category=personal). Indexed:
    // in_window(eq="work") ⇒ false ⇒ skip ⇒ no push.
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_note("personal", "c"),
            Some("userB".to_string()),
        )
        .await?;
    expect_no_update(&mut rx, "userB non-matching insert (skip)");

    Ok(())
}

#[tokio::test]
async fn filter_bearing_collect_ignores_filter_in_skip_decision() -> anyhow::Result<()> {
    // A collect sub with eq + filter: the filter narrows the result but is
    // IGNORED in the skip decision (over-approximation). A doc matching the
    // eq but not the filter causes a re-run whose diff suppresses the push.
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let query: Query = serde_json::from_value(serde_json::json!({
        "table": "workItems",
        "index": "by_status",
        "eq": ["backlog"],
        "filter": { "op": "eq", "field": "order", "value": 1 }
    }))
    .expect("parse query");
    let mut rx = sub(&state, &db, query).await;

    // Matches eq (status=backlog) but not filter (order=2 ≠ 1). Indexed:
    // in_window(eq="backlog") ⇒ true ⇒ re-run. Re-run applies filter order=1
    // ⇒ doc excluded ⇒ result unchanged ⇒ no push (diff-suppressed).
    state
        .realtime
        .committers
        .mutate(&db, None, insert("backlog", 2.0), None)
        .await?;
    expect_no_update(
        &mut rx,
        "matches-eq-not-filter (over-approx, diff-suppressed)",
    );

    // Matches eq AND filter — push.
    state
        .realtime
        .committers
        .mutate(&db, None, insert("backlog", 1.0), None)
        .await?;
    expect_update(&mut rx, "matches-eq-and-filter").await;

    Ok(())
}

// =====================================================================
// 8. Multi-step txn: net before/after collapse across steps
// =====================================================================

#[tokio::test]
async fn multi_step_txn_insert_and_delete_fans_out_correctly() -> anyhow::Result<()> {
    // A multi-step txn that inserts a new (in-window) doc AND deletes a
    // pre-existing member in one atomic unit. fan_out sees both written docs;
    // each is in-window (the insert's after is in-window; the delete is always
    // affecting) ⇒ re-run. The result swapped one doc for another (different
    // ids) ⇒ the canonical form changes ⇒ push. This confirms the per-doc
    // affects check iterates all written docs on the table, not just the first.
    let state = test_state().await;
    let db = fresh_db(&state).await;

    // Seed one backlog item (will be deleted by the multi-step txn).
    let seed = state
        .realtime
        .committers
        .mutate(&db, None, insert("backlog", 1.0), None)
        .await?;
    let seed_id = id_of(&seed);

    // Subscribe AFTER seeding so the initial result contains the seed doc.
    let mut rx = sub(&state, &db, collect_by_status("backlog")).await;

    // One txn: insert a new backlog doc + delete the seed. Net count is
    // unchanged (1 → 1) but the doc identity changed (different id) so the
    // canonical collect result differs ⇒ push.
    let txn = Transaction {
        steps: vec![
            Step::Insert {
                table: "workItems".to_string(),
                doc: work_item("backlog", 2.0),
            },
            Step::Delete {
                table: "workItems".to_string(),
                id: seed_id,
            },
        ],
    };
    state
        .realtime
        .committers
        .mutate(&db, None, txn, None)
        .await?;
    expect_update(&mut rx, "multi-step insert+delete").await;

    Ok(())
}

// =====================================================================
// 9. Regression: patch-then-delete the SAME doc in one txn must push.
//    Guards the `capture_doc` rule that a `Delete` step clears `after` to
//    `None` even when an earlier step in the same txn already recorded an
//    `after` for that id. Without it, `fan_out` would see a stale in-window
//    `after`, find `in_window(before) != in_window(after)` false, and skip a
//    `count` subscription whose matching set just shrank — a missed push.
// =====================================================================

#[tokio::test]
async fn patch_then_delete_same_doc_pushes_count() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let outcome = state
        .realtime
        .committers
        .mutate(&db, None, insert("backlog", 1.0), None)
        .await?;
    let id = id_of(&outcome);

    let mut rx = sub(&state, &db, count_by_status("backlog")).await;

    // One txn: rename the title (status stays backlog ⇒ still in-window), then
    // delete the SAME doc. Net: the matching set lost its one member ⇒ count
    // 1 → 0 ⇒ must push.
    let mut patch_fields = serde_json::Map::new();
    patch_fields.insert(
        "title".to_string(),
        serde_json::Value::String("renamed".into()),
    );
    let txn = Transaction {
        steps: vec![
            Step::Patch {
                table: "workItems".to_string(),
                id: id.clone(),
                fields: patch_fields,
            },
            Step::Delete {
                table: "workItems".to_string(),
                id,
            },
        ],
    };
    state
        .realtime
        .committers
        .mutate(&db, None, txn, None)
        .await?;
    expect_update(&mut rx, "patch-then-delete same doc (count must drop)").await;

    Ok(())
}

// =====================================================================
// 9. Regression: int64 range subscription must re-run for in-window writes.
//    Mirrors `range_collect_skips_below_range_pushes_in_range` but on an
//    `Int64` index. Before the `cmp_binds` I64 arm was added, `cmp_binds`
//    returned `None` for `(I64, I64)` (the `_` arm), which propagated as
//    `satisfies_lower == false` → `in_window == false` → `indexed_affects ==
//    false` → fan_out hit `if !affects { continue; }` and SKIPPED the in-range
//    insert, silently dropping the push. That is under-approximation and
//    violates the committer's load-bearing invariant.
// =====================================================================

fn int64_events_schema_json() -> serde_json::Value {
    serde_json::json!({"tables":{
      "events":{
        "fields":{
          "ts": { "type": "int64" },
          "kind": { "type": "string" }
        },
        "indexes":[{"name":"by_ts","fields":["ts"]}]
      }
    }})
}

/// Creates a fresh DB and pushes the int64-events schema (mirrors
/// `fresh_db`'s shape but with our custom schema, since `fresh_db` is
/// hardcoded to push the kanban schema).
async fn fresh_int64_db(state: &std::sync::Arc<rtdb_server::AppState>) -> String {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create db");
    let schema: SchemaDef =
        serde_json::from_value(int64_events_schema_json()).expect("parse int64 schema");
    ddl::push_schema(&state.pool, &name, schema)
        .await
        .expect("push int64 schema");
    name
}

fn insert_event(ts: &str, kind: &str) -> Transaction {
    Transaction {
        steps: vec![Step::Insert {
            table: "events".to_string(),
            doc: serde_json::json!({ "ts": ts, "kind": kind })
                .as_object()
                .expect("json object")
                .clone(),
        }],
    }
}

fn patch_event_ts(id: &str, ts: &str) -> Transaction {
    let mut fields = serde_json::Map::new();
    fields.insert("ts".to_string(), serde_json::Value::String(ts.to_string()));
    Transaction {
        steps: vec![Step::Patch {
            table: "events".to_string(),
            id: id.to_string(),
            fields,
        }],
    }
}

/// count on `by_ts` with `gte=N` — exercises the range path through cmp_binds.
fn count_events_by_ts_gte(gte: &str) -> Query {
    serde_json::from_value(serde_json::json!({
        "table": "events",
        "index": "by_ts",
        "eq": [],
        "gte": gte,
        "count": true
    }))
    .expect("parse query")
}

#[tokio::test]
async fn int64_range_count_skips_out_of_window_pushes_in_window() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_int64_db(&state).await;

    // Sub: count events with ts >= 10.
    let mut rx = sub(&state, &db, count_events_by_ts_gte("10")).await;

    // Out-of-window insert (ts=5, below the gte=10 bound) — must SKIP.
    state
        .realtime
        .committers
        .mutate(&db, None, insert_event("5", "a"), None)
        .await?;
    expect_no_update(&mut rx, "below-range int64 insert");

    // In-window insert (ts=20, >= 10) — must PUSH.
    // REGRESSION: without the `(I64, I64)` arm in cmp_binds, this was skipped
    // (None → satisfies_lower=false → in_window=false → fan_out `continue`),
    // dropping the push.
    state
        .realtime
        .committers
        .mutate(&db, None, insert_event("20", "b"), None)
        .await?;
    expect_update(&mut rx, "in-range int64 insert").await;

    Ok(())
}

#[tokio::test]
async fn int64_range_count_patch_into_window_pushes() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_int64_db(&state).await;

    // Seed one event below the window (ts=5).
    let outcome = state
        .realtime
        .committers
        .mutate(&db, None, insert_event("5", "a"), None)
        .await?;
    let id = id_of(&outcome);

    // Sub: count events with ts >= 10.
    let mut rx = sub(&state, &db, count_events_by_ts_gte("10")).await;

    // Patch ts 5 → 50: crosses the lower bound from outside to inside.
    // REGRESSION: without the I64 cmp_binds arm, the `after` state (ts=50)
    // would evaluate `in_window == false`, the `before` state (ts=5) also
    // `false`, so `count`'s `in_window(before) != in_window(after)` would be
    // `false != false == false` → indexed_affects=false → skip → dropped push.
    state
        .realtime
        .committers
        .mutate(&db, None, patch_event_ts(&id, "50"), None)
        .await?;
    expect_update(&mut rx, "patch into int64 range").await;

    Ok(())
}

// =====================================================================
// 12. Ordered top-N boundary tracking (v3): take / first / paginate
//
//     These subs re-run only when a written doc is inside the eq window AND
//     ranks at or before the last result's final row (the boundary). The
//     dangerous direction is a MISSED push, so most cases below assert a push;
//     the no-push cases assert the complementary consistency property (the
//     result genuinely did not change). Which of "skipped" vs "re-ran and
//     diff-suppressed" produced a no-push is not observable here by design —
//     the skip decision itself is unit-tested in `src/subs.rs`.
// =====================================================================

/// `take(n)` over one project's items, ordered by the `order` index field
/// (the field after the eq-prefix), so ranking is fully controlled by the
/// test rather than by insertion timing.
fn take_by_project_order(proj: &str, n: u32, desc: bool) -> Query {
    let mut q = serde_json::json!({
        "table": "workItems",
        "index": "by_project_and_order",
        "eq": [proj],
        "take": n
    });
    if desc {
        q["order"] = serde_json::json!("desc");
    }
    serde_json::from_value(q).expect("parse query")
}

fn first_by_project_order(proj: &str) -> Query {
    serde_json::from_value(serde_json::json!({
        "table": "workItems",
        "index": "by_project_and_order",
        "eq": [proj],
        "first": true
    }))
    .expect("parse query")
}

fn paginate_by_project_order(proj: &str, num_items: u32) -> Query {
    serde_json::from_value(serde_json::json!({
        "table": "workItems",
        "index": "by_project_and_order",
        "eq": [proj],
        "paginate": { "numItems": num_items }
    }))
    .expect("parse query")
}

/// Seeds `orders` as backlog items and returns their ids, in order.
async fn seed(
    state: &std::sync::Arc<rtdb_server::AppState>,
    db: &str,
    orders: &[f64],
) -> anyhow::Result<Vec<String>> {
    let mut ids = Vec::new();
    for order in orders {
        let outcome = state
            .realtime
            .committers
            .mutate(db, None, insert("backlog", *order), None)
            .await?;
        ids.push(id_of(&outcome));
    }
    Ok(ids)
}

#[tokio::test]
async fn take_skips_inserts_beyond_the_boundary() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    seed(&state, &db, &[10.0, 20.0]).await?;

    // Top-2 ascending = [10, 20]; full ⇒ boundary = order 20.
    let mut rx = sub(&state, &db, take_by_project_order(PROJ, 2, false)).await;

    // Ranks after the boundary ⇒ cannot enter the top 2 ⇒ result unchanged.
    state
        .realtime
        .committers
        .mutate(&db, None, insert("backlog", 30.0), None)
        .await?;
    expect_no_update(&mut rx, "insert beyond boundary");

    // Ranks ahead of the boundary ⇒ enters the top 2 ⇒ push.
    state
        .realtime
        .committers
        .mutate(&db, None, insert("backlog", 5.0), None)
        .await?;
    expect_update(&mut rx, "insert ahead of boundary").await;

    Ok(())
}

#[tokio::test]
async fn take_pushes_for_member_body_and_rank_changes() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let ids = seed(&state, &db, &[10.0, 20.0, 30.0]).await?;

    // Top-2 = [10, 20], boundary = order 20.
    let mut rx = sub(&state, &db, take_by_project_order(PROJ, 2, false)).await;

    // A member's body change: `take` returns doc bodies ⇒ push.
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            patch_field(
                &ids[0],
                "title",
                serde_json::Value::String("renamed".into()),
            ),
            None,
        )
        .await?;
    expect_update(&mut rx, "member body patch").await;

    // A member moving OUT of the window pulls the beyond-boundary doc in.
    // This is the regression guard for dropping the `before` state.
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            patch_field(&ids[1], "status", serde_json::Value::String("done".into())),
            None,
        )
        .await?;
    expect_update(&mut rx, "member leaves the window").await;

    Ok(())
}

#[tokio::test]
async fn take_pushes_when_a_beyond_boundary_doc_moves_ahead_of_it() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let ids = seed(&state, &db, &[10.0, 20.0, 30.0]).await?;

    // Top-2 = [10, 20], boundary = order 20.
    let mut rx = sub(&state, &db, take_by_project_order(PROJ, 2, false)).await;

    // The order-30 doc (beyond the boundary in its BEFORE state) moves to
    // order 1 — its AFTER state ranks ahead of the boundary ⇒ push. A rule
    // that only looked at the before-state would miss this.
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            patch_field(&ids[2], "order", serde_json::json!(1.0)),
            None,
        )
        .await?;
    expect_update(&mut rx, "beyond-boundary doc moves into the top-N").await;

    Ok(())
}

#[tokio::test]
async fn take_boundary_is_refreshed_after_the_result_shrinks() -> anyhow::Result<()> {
    // The boundary must track EVERY re-run, not just the initial result. A
    // stale boundary here would silently drop the final push.
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let ids = seed(&state, &db, &[10.0, 20.0]).await?;

    // Top-2 = [10, 20], boundary = order 20.
    let mut rx = sub(&state, &db, take_by_project_order(PROJ, 2, false)).await;

    // Delete a member: the result shrinks to [20], which is no longer full,
    // so the boundary must be cleared (unbounded).
    state
        .realtime
        .committers
        .mutate(&db, None, delete(&ids[0]), None)
        .await?;
    expect_update(&mut rx, "member delete").await;

    // With the boundary cleared this in-window insert affects the sub ⇒ push.
    // Against the STALE boundary (order 20) it would rank beyond and be
    // skipped — a missed realtime update.
    state
        .realtime
        .committers
        .mutate(&db, None, insert("backlog", 50.0), None)
        .await?;
    expect_update(&mut rx, "insert after the boundary was cleared").await;

    Ok(())
}

#[tokio::test]
async fn take_desc_inverts_the_boundary() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    seed(&state, &db, &[10.0, 20.0, 30.0]).await?;

    // Newest-first style feed: top-2 descending = [30, 20], boundary = 20.
    let mut rx = sub(&state, &db, take_by_project_order(PROJ, 2, true)).await;

    // Below the boundary in DESC order ⇒ beyond the window ⇒ unchanged.
    state
        .realtime
        .committers
        .mutate(&db, None, insert("backlog", 1.0), None)
        .await?;
    expect_no_update(&mut rx, "desc insert beyond boundary");

    // Above the boundary in DESC order ⇒ enters the top 2 ⇒ push.
    state
        .realtime
        .committers
        .mutate(&db, None, insert("backlog", 99.0), None)
        .await?;
    expect_update(&mut rx, "desc insert ahead of boundary").await;

    Ok(())
}

#[tokio::test]
async fn take_ignores_writes_outside_the_eq_window() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    seed(&state, &db, &[10.0, 20.0]).await?;

    let mut rx = sub(&state, &db, take_by_project_order(PROJ, 2, false)).await;

    // Another project's item ranks ahead of the boundary but is outside the
    // eq window ⇒ the result cannot change.
    let mut other = work_item("backlog", 1.0);
    other.insert(
        "projectId".to_string(),
        serde_json::Value::String(OTHER_PROJ.to_string()),
    );
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            Transaction {
                steps: vec![Step::Insert {
                    table: "workItems".to_string(),
                    doc: other,
                }],
            },
            None,
        )
        .await?;
    expect_no_update(&mut rx, "other project's insert");

    Ok(())
}

#[tokio::test]
async fn first_tracks_its_single_row_boundary() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    seed(&state, &db, &[10.0]).await?;

    // `first` = take(1): boundary = the one returned doc (order 10).
    let mut rx = sub(&state, &db, first_by_project_order(PROJ)).await;

    // Ranks after it ⇒ still not first ⇒ unchanged.
    state
        .realtime
        .committers
        .mutate(&db, None, insert("backlog", 20.0), None)
        .await?;
    expect_no_update(&mut rx, "insert after the first row");

    // Ranks ahead of it ⇒ becomes the new first ⇒ push.
    state
        .realtime
        .committers
        .mutate(&db, None, insert("backlog", 1.0), None)
        .await?;
    expect_update(&mut rx, "insert ahead of the first row").await;

    Ok(())
}

#[tokio::test]
async fn paginate_full_page_without_next_cursor_stays_unbounded() -> anyhow::Result<()> {
    // A page holding exactly `numItems` docs but NO next cursor must NOT be
    // treated as bounded: an insert beyond its last row flips `hasNext` on and
    // mints a cursor, which changes the result even though the docs don't.
    let state = test_state().await;
    let db = fresh_db(&state).await;
    seed(&state, &db, &[10.0, 20.0]).await?;

    let mut rx = sub(&state, &db, paginate_by_project_order(PROJ, 2)).await;

    state
        .realtime
        .committers
        .mutate(&db, None, insert("backlog", 30.0), None)
        .await?;
    expect_update(&mut rx, "insert flips hasNext on a last page").await;

    Ok(())
}

#[tokio::test]
async fn paginate_skips_writes_beyond_a_bounded_page() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let ids = seed(&state, &db, &[10.0, 20.0, 30.0]).await?;

    // Page of 2 with a third doc behind it ⇒ next cursor issued ⇒ bounded by
    // order 20.
    let mut rx = sub(&state, &db, paginate_by_project_order(PROJ, 2)).await;

    // Patching the beyond-boundary doc's body leaves page + cursor identical.
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            patch_field(
                &ids[2],
                "title",
                serde_json::Value::String("renamed".into()),
            ),
            None,
        )
        .await?;
    expect_no_update(&mut rx, "patch beyond the page boundary");

    // A doc entering ahead of the boundary shifts the page ⇒ push.
    state
        .realtime
        .committers
        .mutate(&db, None, insert("backlog", 1.0), None)
        .await?;
    expect_update(&mut rx, "insert ahead of the page boundary").await;

    Ok(())
}

#[tokio::test]
async fn bare_take_without_an_index_ranks_on_creation_time() -> anyhow::Result<()> {
    // No index: the sort order is `created_at, id` alone, so the boundary is
    // purely temporal. An ascending take holds the OLDEST rows, which a fresh
    // insert (newest created_at) can never join.
    let state = test_state().await;
    let db = fresh_db(&state).await;
    seed(&state, &db, &[10.0, 20.0]).await?;

    let query: Query = serde_json::from_value(serde_json::json!({
        "table": "workItems",
        "take": 2
    }))
    .expect("parse query");
    let mut rx = sub(&state, &db, query).await;

    state
        .realtime
        .committers
        .mutate(&db, None, insert("backlog", 30.0), None)
        .await?;
    expect_no_update(&mut rx, "newer insert cannot join the oldest-2");

    Ok(())
}

#[tokio::test]
async fn take_pushes_on_any_delete() -> anyhow::Result<()> {
    // `Delete` captures no doc values, so an ordered sub can neither
    // window-check nor rank the deleted row — it always re-runs. Deleting a
    // member changes the result, so the push is observable.
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let ids = seed(&state, &db, &[10.0, 20.0, 30.0]).await?;

    let mut rx = sub(&state, &db, take_by_project_order(PROJ, 2, false)).await;

    state
        .realtime
        .committers
        .mutate(&db, None, delete(&ids[1]), None)
        .await?;
    expect_update(&mut rx, "delete of a member").await;

    Ok(())
}
