//! ARC-201: cross-replica hot-config coherence + PATCH /admin/config
//! single-flight, on the shared `common::cluster` harness (`Cluster::two_bare`)
//! — two real replicas on one backing database, real HTTP edges, the real
//! `rtdb_config` NOTIFY + config listener. Assertions read each replica's
//! in-memory `ArcSwap` directly (exact, no redaction) and never touch
//! `rtdb_config` outside the API:
//!
//! 1. patch on A converges on B without restart; a sequenced patch on B does
//!    not revert A's field anywhere (the stale-replica durable-revert bug);
//! 2. five concurrent PATCHes on ONE replica serialize under the config
//!    single-flight lock and every field survives (no last-store-win).

use std::time::{Duration, Instant};

use serde_json::json;

use crate::common::cluster::{Cluster, ReplicaOpts};

/// Bounded wait for cross-replica convergence via the rtdb_config NOTIFY +
/// config listener. Generous; far below any test deadline.
const CONVERGE_WAIT: Duration = Duration::from_secs(10);

/// `PATCH /admin/config` with the harness admin bearer. Asserts 200 and
/// panics with the body otherwise.
async fn patch_config(addr: std::net::SocketAddr, body: serde_json::Value) {
    let resp = reqwest::Client::new()
        .patch(format!("http://{addr}/admin/config"))
        .header("Authorization", "Bearer test-admin-key")
        .json(&body)
        .send()
        .await
        .expect("send PATCH /admin/config");
    let status = resp.status();
    assert!(
        status.is_success(),
        "PATCH /admin/config failed: {status} {}",
        resp.text().await.unwrap_or_default()
    );
}

/// Bounded poll until `pred` holds on `get`'s current value, re-reading on
/// every tick. Panics with `what` if it never converges.
async fn poll_until<T, F, G>(mut get: G, pred: F, what: &str)
where
    G: FnMut() -> T,
    F: Fn(&T) -> bool,
{
    let deadline = Instant::now() + CONVERGE_WAIT;
    loop {
        let v = get();
        if pred(&v) {
            return;
        }
        if Instant::now() >= deadline {
            panic!("convergence timeout: {what}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Two replicas, no schema — the config path needs no tables.
async fn cluster() -> Cluster {
    Cluster::two_bare(
        ReplicaOpts {
            label: "cfg-a".into(),
            ..ReplicaOpts::default()
        },
        ReplicaOpts {
            label: "cfg-b".into(),
            ..ReplicaOpts::default()
        },
    )
    .await
}

#[tokio::test]
async fn patch_converges_across_replicas_without_restart() {
    let cluster = cluster().await;
    let a = cluster.replica(crate::common::cluster::ReplicaId::A);
    let b = cluster.replica(crate::common::cluster::ReplicaId::B);

    // Patch on A: B's in-memory view converges via the listener, no restart.
    patch_config(a.addr, json!({ "perTokenRpm": 1111 })).await;
    let hot_b = b.state.runtime.hot.clone();
    poll_until(
        || hot_b.load().per_token_rpm,
        |v| *v == 1111,
        "B did not converge to A's perTokenRpm patch",
    )
    .await;

    // Sequenced patch on B (its view already carries A's field): the new
    // authoritative-reload path must not revert A's field anywhere.
    patch_config(b.addr, json!({ "perDbRpm": 2222 })).await;
    let hot_a = a.state.runtime.hot.clone();
    poll_until(
        || {
            let v = hot_a.load();
            (v.per_token_rpm, v.per_db_rpm)
        },
        |v| *v == (1111, 2222),
        "A did not converge to B's perDbRpm patch while keeping perTokenRpm",
    )
    .await;
    let hot_b = b.state.runtime.hot.clone();
    let (t, d) = {
        let v = hot_b.load();
        (v.per_token_rpm, v.per_db_rpm)
    };
    assert_eq!((t, d), (1111, 2222), "B lost a field after its own patch");
}

#[tokio::test]
async fn concurrent_patches_same_replica_all_survive() {
    let cluster = cluster().await;
    let a = cluster.replica(crate::common::cluster::ReplicaId::A);
    let b = cluster.replica(crate::common::cluster::ReplicaId::B);

    // Five concurrent PATCHes on ONE replica, each touching a different
    // field. Serialized by runtime.config_lock with an authoritative reload
    // per turn, so all five must survive — the old unserialized
    // load-apply-save could last-store-win and drop four of them.
    let patches = [
        json!({ "perTokenRpm": 101 }),
        json!({ "perDbRpm": 202 }),
        json!({ "sessionTtlDays": 7 }),
        json!({ "idempotencyTtlMs": 456_789 }),
        json!({ "maxTablesPerDb": 9 }),
    ];
    let mut handles = Vec::new();
    for body in patches {
        let addr = a.addr;
        handles.push(tokio::spawn(async move {
            patch_config(addr, body).await;
        }));
    }
    for h in handles {
        h.await.expect("patch task");
    }

    let expected = (101, 202, 7, 456_789, 9);
    let hot_a = a.state.runtime.hot.clone();
    poll_until(
        || {
            let v = hot_a.load();
            (
                v.per_token_rpm,
                v.per_db_rpm,
                v.session_ttl_days,
                v.idempotency_ttl_ms,
                v.max_tables_per_db,
            )
        },
        |v| *v == expected,
        "A did not retain all five concurrent patch fields",
    )
    .await;
    let hot_b = b.state.runtime.hot.clone();
    poll_until(
        || {
            let v = hot_b.load();
            (
                v.per_token_rpm,
                v.per_db_rpm,
                v.session_ttl_days,
                v.idempotency_ttl_ms,
                v.max_tables_per_db,
            )
        },
        |v| *v == expected,
        "B did not converge to all five patch fields",
    )
    .await;
}
