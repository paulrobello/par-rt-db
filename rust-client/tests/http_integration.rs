//! Opt-in live-server HTTP integration test. Skipped by default (`#[ignore]`);
//! run with `--ignored` after pointing the env vars at a running server:
//!   RTDB_TEST_SERVER_URL=http://127.0.0.1:8300 \
//!   RTDB_TEST_ADMIN_KEY=dev-admin-key \
//!   cargo test --test http_integration -- --ignored

#![cfg(feature = "http")]

mod common;

use common::{env, setup};
use par_rt_db_client::{ErrorCode, Mutation, Order, RtDbHttpClient, StepResult, TableQuery};
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
    let admin = RtDbHttpClient::new(&url, "", &admin_key);
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
}
