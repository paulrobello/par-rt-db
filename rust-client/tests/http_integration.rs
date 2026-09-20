//! Opt-in live-server HTTP integration test. Skipped by default (`#[ignore]`);
//! run with `--ignored` after pointing the env vars at a running server:
//!   RTDB_TEST_SERVER_URL=http://127.0.0.1:8300 \
//!   RTDB_TEST_ADMIN_KEY=dev-admin-key \
//!   cargo test --test http_integration -- --ignored

#![cfg(feature = "http")]

mod common;

use common::{env, setup};
use par_rt_db_client::{
    ErrorCode, Mutation, Order, RtDbAdminClient, RtDbHttpClient, StepResult, TableQuery,
};
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Item {
    _id: String,
    name: String,
    n: i64,
}

#[tokio::test]
#[ignore = "set RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY and run with --ignored"]
async fn http_round_trip() {
    if env().is_none() {
        return;
    }
    let ctx = setup().await;
    let c = RtDbHttpClient::new(&ctx.url, &ctx.db, &ctx.token);

    // insert two docs
    let txn = Mutation::new()
        .insert("items", json!({"name":"a","n":1}))
        .insert("items", json!({"name":"b","n":2}))
        .build();
    let res = c.mutate(&txn, None).await.unwrap();
    assert_eq!(res.len(), 2);
    // Capture the first inserted id; `expect_version` only yields
    // PreconditionFailed for a version mismatch on an *existing* doc
    // (a missing id returns NotFound — see server `do_expect_version`).
    let first_id = match &res[0] {
        StepResult::Insert { id } => id.clone(),
        other => panic!("expected Insert result, got {other:?}"),
    };

    // ordered scan returns both, ascending by n
    let docs: Vec<Item> = c
        .run(
            TableQuery::new("items")
                .with_index("by_n", &[])
                .order(Order::Asc)
                .take(10),
        )
        .await
        .unwrap();
    assert_eq!(docs.len(), 2);
    assert_eq!(docs[0].name, "a");

    // count terminal
    let n: i64 = c
        .run(TableQuery::new("items").with_index("by_n", &[]).count())
        .await
        .unwrap();
    assert_eq!(n, 2);

    // precondition failure: wrong version on an existing doc → PreconditionFailed
    let bad = Mutation::new()
        .expect_version("items", &first_id, 999)
        .build();
    let err = c.mutate(&bad, None).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::PreconditionFailed);
}

/// `POST /api/mutate-batch` against a live server: slots aligned with the
/// input txns; a failing entry isolates into its own error slot without
/// rolling back the others; a per-entry idempotency key dedupes on replay.
#[tokio::test]
#[ignore = "set RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY and run with --ignored"]
async fn http_batch_mutate_isolates_and_dedupes() {
    let ctx = setup().await;
    let c = RtDbHttpClient::new(&ctx.url, &ctx.db, &ctx.token);

    let ok_txn = Mutation::new()
        .insert("items", json!({"name":"batch-a","n":1}))
        .build();
    let bad_txn = Mutation::new()
        .insert("noSuchTable", json!({"name":"x","n":1}))
        .build();

    // Mixed batch: slot 0 commits, slot 1 fails in isolation.
    let slots = c
        .batch_mutate(&[(&ok_txn, None), (&bad_txn, Some("bm-key"))])
        .await
        .unwrap();
    assert_eq!(slots.len(), 2);
    assert!(slots[0].ok, "slot 0 should commit: {:?}", slots[0]);
    assert!(slots[0].results.is_some());
    assert!(!slots[1].ok);
    assert!(slots[1].error.is_some());

    // Surviving entries committed; the failed one did not write anything.
    let n: i64 = c
        .run(TableQuery::new("items").with_index("by_n", &[]).count())
        .await
        .unwrap();
    assert_eq!(n, 1);

    // Same key replays the first outcome instead of re-executing.
    let replay = c.batch_mutate(&[(&ok_txn, Some("bm-key"))]).await.unwrap();
    assert_eq!(replay.len(), 1);
    assert!(replay[0].ok);
    let n: i64 = c
        .run(TableQuery::new("items").with_index("by_n", &[]).count())
        .await
        .unwrap();
    assert_eq!(n, 1, "idempotent replay must not double-write");
}

/// Exercises the admin control-plane end-to-end against a live server. Creates a
/// fresh `t<uuid>` database (never touches a db it didn't create), pushes a
/// schema, mints a token, lists dbs/allowlist, exports, and revokes.
#[cfg(feature = "admin")]
#[tokio::test]
#[ignore = "set RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY and run with --ignored"]
async fn admin_control_plane() {
    let Some((url, admin_key)) = env() else {
        return;
    };
    let admin = RtDbAdminClient::new(&url, &admin_key);
    let new_db = format!("t{}", common::uuid_v7());

    admin.create_db(&new_db).await.unwrap();

    let schema: par_rt_db_client::SchemaDef =
        serde_json::from_value(json!({"tables":{"notes":{"fields":{"body":{"type":"string"}}}}}))
            .unwrap();
    admin.push_schema(&new_db, &schema).await.unwrap();

    let minted = admin.mint_token(&new_db, "live").await.unwrap();
    assert!(!minted.token.is_empty());
    assert!(!minted.token_id.is_empty());

    let dbs = admin.list_dbs().await.unwrap();
    assert!(dbs.contains(&new_db), "list_dbs missing freshly created db");

    admin.allowlist_add(&new_db, "x@y.com").await.unwrap();
    let emails = admin.allowlist_list(&new_db).await.unwrap();
    assert!(
        emails.contains(&"x@y.com".to_string()),
        "allowlist_list missing added email"
    );

    let jsonl = admin.export_db(&new_db).await.unwrap();
    assert!(!jsonl.is_empty());
    assert!(
        jsonl.contains("\"kind\":\"schema\""),
        "export_db should start with the schema line"
    );

    admin.revoke_token(&minted.token_id).await.unwrap();

    // ENH-010 live subscription inspector: both the db-scoped and global calls
    // should succeed and deserialize (fresh db has no active subscriptions).
    let _scoped = admin.list_subscriptions(Some(&new_db)).await.unwrap();
    let _global = admin.list_subscriptions(None).await.unwrap();
}

/// Change feed: writes land in the durable log with post-images, the cursor
/// contract holds (idempotent re-read, short page → nextSeq = head), and a
/// stale cursor returns the typed `CURSOR_EXPIRED` (F7). Extends the same
/// provisioned db the round-trip test uses.
#[tokio::test]
#[ignore = "set RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY and run with --ignored"]
async fn change_feed_round_trip() {
    if env().is_none() {
        return;
    }
    let ctx = setup().await;
    let c = RtDbHttpClient::new(&ctx.url, &ctx.db, &ctx.token);

    // Fresh view of the log (the provisioned db may carry earlier writes).
    let head_page = c.changes(0, None, Some(1000)).await.unwrap();
    let start = head_page.next_seq.min(head_page.head);

    // Two inserts → two observable ops with post-images.
    let txn = Mutation::new()
        .insert("items", json!({"name":"cf-a","n":101}))
        .insert("items", json!({"name":"cf-b","n":102}))
        .build();
    c.mutate(&txn, None).await.unwrap();

    let page = c.changes(start, None, Some(1000)).await.unwrap();
    let mine: Vec<_> = page
        .ops
        .iter()
        .filter(|op| op.kind == "insert" && op.doc.is_some())
        .collect();
    assert!(
        mine.iter()
            .any(|op| op.doc.as_ref().unwrap()["name"] == json!("cf-a")),
        "inserts land in the feed with post-images: {:?}",
        page.ops
    );
    // Short page (or filtered tail) ⇒ nextSeq == head, never a livelock.
    assert_eq!(page.next_seq, page.head);

    // Re-reading the same cursor is idempotent.
    let again = c.changes(start, None, Some(1000)).await.unwrap();
    assert_eq!(again.ops.len(), page.ops.len());

    // A cursor ahead of the log is a typed error, never an empty success.
    let err = c.changes(page.head + 10_000, None, None).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::CursorExpired);

    // Table filter narrows ops but keeps the global cursor world.
    let _scoped = c.changes(start, Some("items"), Some(1)).await.unwrap();
}
