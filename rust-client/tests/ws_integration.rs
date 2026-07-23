//! Opt-in live-server reactive-WS integration test. Skipped by default
//! (`#[ignore]`); run with `--ignored` against a running server:
//!   make dev-db-up   # start the dev server on 127.0.0.1:8300
//!   RTDB_TEST_SERVER_URL=http://127.0.0.1:8300 \
//!   RTDB_TEST_ADMIN_KEY=dev-admin-key \
//!   cargo test --test ws_integration --features ws -- --ignored

#![cfg(feature = "ws")]

mod common;

use std::time::Duration;

use common::{env, setup};
use par_rt_db_client::{RtDbClient, TableQuery};
use serde_json::json;
use tokio::time::timeout;

/// Wait up to `deadline` for the subscription to receive its first non-pending
/// snapshot, returning it.
async fn first_snapshot(
    sub: &mut par_rt_db_client::Subscription,
    deadline: Duration,
) -> Option<par_rt_db_client::Snapshot> {
    let snap = sub.snapshot();
    if !matches!(snap, par_rt_db_client::Snapshot::Pending) {
        return Some(snap);
    }
    timeout(deadline, sub.changed()).await.ok()?.ok()?;
    Some(sub.snapshot())
}

#[tokio::test]
#[ignore = "set RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY and run with --ignored"]
async fn ws_subscribe_and_live_update() {
    let Some((url, _admin)) = env() else {
        return;
    };
    let ctx = setup().await;
    let token = ctx.token.clone();
    let client = RtDbClient::new(&url, &ctx.db, move || {
        let t = token.clone();
        async move { Some(t) }
    });
    client.connect();

    // Subscribe to the items table; the first queryUpdate carries the (empty)
    // initial result.
    let mut sub = client.subscribe(TableQuery::new("items").collect());
    let snap = first_snapshot(&mut sub, Duration::from_secs(10))
        .await
        .expect("initial queryUpdate");
    let docs = match snap {
        par_rt_db_client::Snapshot::Value(v) => v,
        other => panic!("expected initial value, got {other:?}"),
    };
    assert!(docs.as_array().map(|a| a.is_empty()).unwrap_or(true));

    // A WS mutation inserts a doc; the live query should reflect it.
    let txn = par_rt_db_client::Mutation::new()
        .insert("items", json!({"name":"ws-live","n":7}))
        .build();
    client.mutate(&txn, None).await.expect("ws mutate");

    // Wait for an update that includes the inserted doc.
    let updated = timeout(Duration::from_secs(10), async {
        loop {
            sub.changed().await.expect("subscription alive");
            if let par_rt_db_client::Snapshot::Value(v) = sub.snapshot()
                && v.to_string().contains("ws-live")
            {
                return v;
            }
        }
    })
    .await
    .expect("live queryUpdate reflecting the insert");
    assert!(updated.to_string().contains("ws-live"));

    client.close();
}
