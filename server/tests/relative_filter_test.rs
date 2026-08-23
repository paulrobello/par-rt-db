//! Execution-time-relative `olderThan` predicates in by-query steps: the
//! filter op whose cutoff (`now − ms`) is derived from the server clock at
//! execution — per fire for a scheduled txn — instead of a literal frozen at
//! schedule time. Pins the by-query-only acceptance boundary and the
//! deterministic match margins: OLD (1) is below any cutoff for centuries,
//! FUTURE (9e15) is above it, so the wall-clock's exact value never matters.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use common::test_state;
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::committer::{CommitterConfig, Committers};
use rtdb_server::db::SchemaCache;
use rtdb_server::ddl;
use rtdb_server::metrics::Metrics;
use rtdb_server::op_feed::OpFeed;
use rtdb_server::query::{FilterExpr, Query, QueryResult, execute_query};
use rtdb_server::quota;
use rtdb_server::scheduler;
use rtdb_server::schema::SchemaDef;
use rtdb_server::subs::SubscriptionManager;
use rtdb_server::txn::{Step, Transaction, execute_txn};
use sqlx::PgPool;

/// Below `now − SWEEP_MS` for centuries (epoch-ms today is ~1.8e12; the
/// cutoff is ~0.8e12 and rising by 1/year).
const OLD: i64 = 1;
/// 9e15 — above `now − 0` effectively forever; f64-exact, within i64.
const FUTURE: i64 = 9_000_000_000_000_000;
const SWEEP_MS: i64 = 1_000_000_000_000;

fn number_schema_json() -> serde_json::Value {
    serde_json::json!({"tables": {
      "tasks": {
        "fields": {
          "title": { "type": "string" },
          "updatedAt": { "type": "number" }
        },
        "indexes": [{ "name": "by_title", "fields": ["title"] }]
      }
    }})
}

/// `updatedAt` as int64 and indexed, so scans take the typed bigint column
/// path (`EqBind::I64`) rather than the jsonb `::float8` extraction.
fn int64_indexed_schema_json() -> serde_json::Value {
    serde_json::json!({"tables": {
      "tasks": {
        "fields": {
          "title": { "type": "string" },
          "updatedAt": { "type": "int64" }
        },
        "indexes": [
            { "name": "by_title", "fields": ["title"] },
            { "name": "by_updatedAt", "fields": ["updatedAt"] }
        ]
      }
    }})
}

/// Mirrors `scheduled_test.rs`'s `unique_db`: a bare uniquely-named database
/// (no kanban fixture — each test pushes its own schema).
async fn unique_db(pool: &PgPool) -> common::TestDb {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(pool, &name)
        .await
        .expect("create fresh database");
    common::wrap_test_db(name)
}

async fn push(
    pool: &PgPool,
    db: &str,
    json: serde_json::Value,
) -> Result<SchemaDef, rtdb_server::error::RtDbError> {
    let schema: SchemaDef = serde_json::from_value(json).expect("parse schema");
    ddl::push_schema(pool, db, schema.clone()).await?;
    Ok(schema)
}

/// `updated_at` is passed as a JSON value so the int64 schema can seed the
/// field's decimal-string wire form.
async fn seed(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    title: &str,
    updated_at: serde_json::Value,
) {
    execute_txn(
        pool,
        db,
        schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "tasks".to_string(),
                doc: serde_json::json!({ "title": title, "updatedAt": updated_at })
                    .as_object()
                    .unwrap()
                    .clone(),
            }],
        },
        &PrincipalCtx::bypass(),
    )
    .await
    .unwrap();
}

async fn count_titles(pool: &PgPool, db: &str, schema: &SchemaDef, title: &str) -> i64 {
    let q = Query {
        table: "tasks".to_string(),
        get: None,
        index: Some("by_title".to_string()),
        eq: vec![serde_json::json!(title)],
        gt: None,
        gte: None,
        lt: None,
        lte: None,
        order: None,
        take: None,
        unique: false,
        first: false,
        count: true,
        distinct: false,
        aggregate: None,
        paginate: None,
        filter: None,
        search: None,
        vector_search: None,
        hybrid_search: None,
        fields: None,
    };
    match execute_query(pool, db, schema, &q, &PrincipalCtx::bypass(), false)
        .await
        .unwrap()
    {
        QueryResult::Count(n) => n,
        other => panic!("expected Count, got {other:?}"),
    }
}

fn older_than(field: &str, ms: i64) -> FilterExpr {
    FilterExpr::OlderThan {
        field: field.to_string(),
        ms,
    }
}

#[tokio::test]
async fn patch_by_query_older_than_patches_old_rows_only() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = unique_db(&pool).await;
    let schema = push(&pool, &db, number_schema_json()).await?;

    seed(&pool, &db, &schema, "old", serde_json::json!(OLD)).await;
    seed(&pool, &db, &schema, "future", serde_json::json!(FUTURE)).await;

    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::PatchByQuery {
                table: "tasks".to_string(),
                filter: older_than("updatedAt", SWEEP_MS),
                patch: serde_json::json!({ "title": "swept" })
                    .as_object()
                    .unwrap()
                    .clone(),
                limit: None,
            }],
        },
        &PrincipalCtx::bypass(),
    )
    .await?;
    assert_eq!(outcome.results[0]["patched"], 1, "only the old row matches");
    assert_eq!(count_titles(&pool, &db, &schema, "swept").await, 1);
    assert_eq!(count_titles(&pool, &db, &schema, "future").await, 1);
    Ok(())
}

#[tokio::test]
async fn delete_by_query_older_than_deletes_old_rows_only() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = unique_db(&pool).await;
    let schema = push(&pool, &db, number_schema_json()).await?;

    seed(&pool, &db, &schema, "old", serde_json::json!(OLD)).await;
    seed(&pool, &db, &schema, "future", serde_json::json!(FUTURE)).await;

    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::DeleteByQuery {
                table: "tasks".to_string(),
                filter: older_than("updatedAt", SWEEP_MS),
                limit: None,
            }],
        },
        &PrincipalCtx::bypass(),
    )
    .await?;
    assert_eq!(outcome.results[0]["deleted"], 1, "only the old row matches");
    assert_eq!(count_titles(&pool, &db, &schema, "old").await, 0);
    assert_eq!(count_titles(&pool, &db, &schema, "future").await, 1);
    Ok(())
}

#[tokio::test]
async fn patch_by_query_older_than_takes_the_int64_column_path() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = unique_db(&pool).await;
    let schema = push(&pool, &db, int64_indexed_schema_json()).await?;

    // int64 wire form is a decimal string.
    seed(
        &pool,
        &db,
        &schema,
        "old",
        serde_json::json!(OLD.to_string()),
    )
    .await;
    seed(
        &pool,
        &db,
        &schema,
        "future",
        serde_json::json!(FUTURE.to_string()),
    )
    .await;

    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::PatchByQuery {
                table: "tasks".to_string(),
                filter: older_than("updatedAt", SWEEP_MS),
                patch: serde_json::json!({ "title": "swept" })
                    .as_object()
                    .unwrap()
                    .clone(),
                limit: None,
            }],
        },
        &PrincipalCtx::bypass(),
    )
    .await?;
    assert_eq!(outcome.results[0]["patched"], 1);
    assert_eq!(count_titles(&pool, &db, &schema, "future").await, 1);
    Ok(())
}

#[tokio::test]
async fn read_query_filter_older_than_is_rejected() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = unique_db(&pool).await;
    let schema = push(&pool, &db, number_schema_json()).await?;

    let q = Query {
        table: "tasks".to_string(),
        get: None,
        index: None,
        eq: vec![],
        gt: None,
        gte: None,
        lt: None,
        lte: None,
        order: None,
        take: None,
        unique: false,
        first: false,
        count: false,
        distinct: false,
        aggregate: None,
        paginate: None,
        filter: Some(older_than("updatedAt", SWEEP_MS)),
        search: None,
        vector_search: None,
        hybrid_search: None,
        fields: None,
    };
    let err = execute_query(&pool, &db, &schema, &q, &PrincipalCtx::bypass(), false)
        .await
        .expect_err("read filters reject olderThan");
    assert!(
        err.message
            .contains("only allowed in patchByQuery/deleteByQuery"),
        "got: {}",
        err.message
    );
    Ok(())
}

#[tokio::test]
async fn patch_by_query_older_than_rejects_non_numeric_field_and_negative_ms() -> anyhow::Result<()>
{
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = unique_db(&pool).await;
    let mut json = number_schema_json();
    json["tables"]["tasks"]["fields"]["updatedAt"] = serde_json::json!({ "type": "string" });
    let schema = push(&pool, &db, json).await?;

    for filter in [older_than("updatedAt", SWEEP_MS), older_than("title", -1)] {
        // `title` is a string field and -1 ms is negative; both are
        // BAD_REQUEST at the by-query validation chokepoint. The first filter
        // also targets the (now string-typed) updatedAt.
        let which = if matches!(filter, FilterExpr::OlderThan { ms: -1, .. }) {
            "negative ms"
        } else {
            "non-numeric field"
        };
        let err = execute_txn(
            &pool,
            &db,
            &schema,
            &Transaction {
                steps: vec![Step::PatchByQuery {
                    table: "tasks".to_string(),
                    filter,
                    patch: serde_json::json!({ "title": "swept" })
                        .as_object()
                        .unwrap()
                        .clone(),
                    limit: None,
                }],
            },
            &PrincipalCtx::bypass(),
        )
        .await
        .unwrap_err();
        let expected = if which == "negative ms" {
            "ms must be >= 0"
        } else {
            "must be a number or int64"
        };
        assert!(
            err.message.contains(expected),
            "{which}: got {}",
            err.message
        );
    }
    Ok(())
}

#[tokio::test]
async fn authorize_and_partial_index_older_than_rejected_at_push() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = unique_db(&pool).await;

    // authorize predicate carrying olderThan
    let mut with_authorize = number_schema_json();
    with_authorize["tables"]["tasks"]["authorize"] =
        serde_json::json!({ "op": "olderThan", "field": "updatedAt", "ms": SWEEP_MS });
    let err = push(&pool, &db, with_authorize).await.unwrap_err();
    assert!(
        err.message
            .contains("only allowed in patchByQuery/deleteByQuery"),
        "authorize: got {}",
        err.message
    );

    // partial-index where-predicate carrying olderThan (fresh db so the
    // failed push above left nothing behind)
    let db2 = unique_db(&pool).await;
    let mut with_where = number_schema_json();
    with_where["tables"]["tasks"]["indexes"] = serde_json::json!([
        { "name": "by_title", "fields": ["title"] },
        {
            "name": "by_updatedAt",
            "fields": ["updatedAt"],
            "where": { "op": "olderThan", "field": "updatedAt", "ms": SWEEP_MS }
        }
    ]);
    let err = push(&pool, &db2, with_where).await.unwrap_err();
    assert!(
        err.message
            .contains("not allowed in a partial-index predicate"),
        "partial index: got {}",
        err.message
    );
    Ok(())
}

/// The load-bearing property for scheduled sweeps: the stored txn carries no
/// cutoff literal — the scheduler's fire re-derives `now − ms` from the
/// server clock, so the same schedule stays fresh forever with no client
/// re-scheduling.
#[tokio::test]
async fn scheduled_sweep_fires_with_the_server_clock() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = unique_db(&pool).await;
    let schema = push(&pool, &db, number_schema_json()).await?;

    seed(&pool, &db, &schema, "old", serde_json::json!(OLD)).await;
    seed(&pool, &db, &schema, "future", serde_json::json!(FUTURE)).await;

    let committers = Committers::new(
        pool.clone(),
        SubscriptionManager::new(),
        SchemaCache::new(),
        OpFeed::new(64, 32),
        Arc::new(ArcSwap::from_pointee(common::test_hot())),
        Metrics::new(),
        CommitterConfig {
            quotas: Arc::new(quota::UsageCache::new()),
            audit_log_enabled: false,
            webhooks_enabled: false,
            ttl_sweep_interval_secs: 60,
            ttl_batch: 5000,
            quota_cache_ttl_secs: 60,
            idle_reclaim_secs: 0,
            instance_id: String::new(),
            multi_instance: false,
            forwarder: None,
        },
    );

    // One-shot due in the past: fires on the scheduler's first wake.
    let txn = Transaction {
        steps: vec![Step::DeleteByQuery {
            table: "tasks".to_string(),
            filter: older_than("updatedAt", SWEEP_MS),
            limit: None,
        }],
    };
    scheduler::insert(&pool, &db, "oneshot", 1, &txn, None, None)
        .await
        .unwrap();

    // Lazy-spawn db's committer + scheduler tasks via a no-op mutate (both
    // spawn inside `channel_for` on first use).
    committers
        .mutate(
            &db,
            None,
            Transaction { steps: vec![] },
            PrincipalCtx::bypass(),
        )
        .await
        .expect("warm-up mutate");

    // The old row disappears once the schedule fires; the future row never
    // matches at any fire time.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let old = count_titles(&pool, &db, &schema, "old").await;
        if old == 0 {
            break;
        }
        assert!(Instant::now() < deadline, "scheduled sweep never fired");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(count_titles(&pool, &db, &schema, "future").await, 1);
    Ok(())
}
