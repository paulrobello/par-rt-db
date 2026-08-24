//! FM-27 Task 2: the `RunMergeUsers` committer arm re-stamps principal-bearing
//! fields (ownerField / collaboratorsField / authorize `$user` fields) from the
//! anon uid to the real uid, bumps versions, fires subscription fan-out, and
//! reports unique-index conflicts instead of failing the merge.
//! Task 3: `merge::merge_users` orchestrates the per-db committer merges plus
//! storage owner swap, session re-point, and the guarded anon-row delete.

mod common;

use common::{test_state, wrap_test_db};
use rtdb_server::AppState;
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::db;
use rtdb_server::ddl::push_schema;
use rtdb_server::error::ErrorCode;
use rtdb_server::protocol::ServerMessage;
use rtdb_server::schema::SchemaDef;
use rtdb_server::subs::next_conn_id;
use rtdb_server::txn::{Step, Transaction};
use serde_json::{Value, json};
use std::sync::Arc;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// `docs` table with `ownerField`/`collaboratorsField` per-row auth and a
/// `by_owner` btree index. `unique_owner_title` swaps in a unique
/// `(owner, title)` index for the conflict test.
fn owned_schema(unique_owner_title: bool) -> SchemaDef {
    let index = if unique_owner_title {
        json!({"name": "by_owner_title", "fields": ["owner", "title"], "unique": true})
    } else {
        json!({"name": "by_owner", "fields": ["owner"]})
    };
    serde_json::from_value(json!({
        "tables": {
            "docs": {
                "fields": {
                    "title": {"type": "string"},
                    "owner": {"type": "string"},
                    "editors": {"type": "array", "element": {"type": "string"}}
                },
                "indexes": [index],
                "ownerField": "owner",
                "collaboratorsField": "editors"
            }
        }
    }))
    .expect("parse owned schema")
}

async fn owned_db(state: &Arc<AppState>, unique_owner_title: bool) -> anyhow::Result<String> {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name).await?;
    let db = wrap_test_db(name);
    push_schema(&state.pool, &db, owned_schema(unique_owner_title)).await?;
    Ok(String::from(db))
}

fn insert_doc(table: &str, doc: Value) -> Transaction {
    Transaction {
        steps: vec![Step::Insert {
            table: table.to_string(),
            doc: doc.as_object().expect("object doc").clone(),
        }],
    }
}

async fn count_sql(pool: &sqlx::PgPool, db: &str, where_sql: &str, bind: &str) -> i64 {
    let (n,): (i64,) = sqlx::query_as(&format!(
        "SELECT COUNT(*) FROM \"{}\".\"t_docs\" WHERE {where_sql}",
        rtdb_server::ddl::pg_schema(db)
    ))
    .bind(bind)
    .fetch_one(pool)
    .await
    .expect("count docs");
    n
}

async fn owned_doc_count(pool: &sqlx::PgPool, db: &str, uid: &str) -> i64 {
    count_sql(pool, db, "\"doc\"->'owner' = to_jsonb($1::text)", uid).await
}

#[tokio::test]
async fn merge_users_restamps_owner_collaborators_and_bumps_version() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = owned_db(&state, false).await?;

    let anon = "user_anon_1";
    let real = "user_real_1";
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_doc(
                "docs",
                json!({ "title": "a", "owner": anon, "editors": [] }),
            ),
            PrincipalCtx::bypass(),
        )
        .await?;
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_doc(
                "docs",
                json!({ "title": "b", "owner": "user_other", "editors": [anon] }),
            ),
            PrincipalCtx::bypass(),
        )
        .await?;

    let result = state
        .realtime
        .committers
        .merge_users(&db, anon, real)
        .await?;
    assert_eq!(result.tables.get("docs"), Some(&2));
    assert!(result.conflicts.is_empty());

    // Owner swapped on the anon-owned row only.
    assert_eq!(owned_doc_count(&state.pool, &db, real).await, 1);
    // Collaborator entry swapped on the other row.
    assert_eq!(
        count_sql(
            &state.pool,
            &db,
            "\"doc\"->'editors' @> to_jsonb($1::text)",
            real
        )
        .await,
        1
    );
    assert_eq!(
        count_sql(
            &state.pool,
            &db,
            "\"doc\"->'editors' @> to_jsonb($1::text)",
            anon
        )
        .await,
        0
    );
    // Version bumped on the restamped rows (inserts start at version 1).
    let (v,): (i64,) = sqlx::query_as(&format!(
        "SELECT \"version\" FROM \"{}\".\"t_docs\" WHERE \"doc\"->'title' = to_jsonb('a'::text)",
        rtdb_server::ddl::pg_schema(&db)
    ))
    .fetch_one(&state.pool)
    .await?;
    assert_eq!(v, 2);

    // Idempotent: a second run finds no anon references and touches nothing.
    let again = state
        .realtime
        .committers
        .merge_users(&db, anon, real)
        .await?;
    assert!(again.tables.values().all(|&n| n == 0));
    assert!(again.conflicts.is_empty());
    Ok(())
}

/// ENH-028 + FM-27: a computed field over a principal-bearing field. The
/// merge path bypasses the `do_*` write functions, so the committer must
/// re-stamp computed fields on the rewritten doc.
fn computed_owner_schema() -> SchemaDef {
    serde_json::from_value(json!({
        "tables": {
            "docs": {
                "fields": {
                    "title": {"type": "string"},
                    "owner": {"type": "string"},
                    "ownerLabel": {"type": "string"}
                },
                "indexes": [
                    {"name": "by_owner", "fields": ["owner"]},
                    {"name": "by_ownerLabel", "fields": ["ownerLabel"]}
                ],
                "ownerField": "owner",
                "computed": {
                    "ownerLabel": {"op": "concat", "parts": [
                        {"op": "literal", "value": "owner:"},
                        {"op": "field", "field": "owner"}
                    ]}
                }
            }
        }
    }))
    .expect("parse computed owner schema")
}

/// `merge::merge_users` iterates every `rtdb_auth.databases` row. Aborted test
/// runs leak rows whose backing schema is gone (RAII never fires on SIGKILL),
/// and each stale row cost a full committer spawn plus ensure-table DDL round
/// trips inside the loop — at 3,575 leaked rows one merge_users took tens of
/// minutes and merge_test appeared to hang (2026-08-23). The loop must skip
/// schema-less rows up front: a db with no schema holds no docs and no storage
/// rows, so skipping is semantically identical to the NotFound /
/// undefined-table tolerations inside the loop.
#[tokio::test]
async fn merge_users_skips_stale_registry_rows_without_paying_a_committer_spawn()
-> anyhow::Result<()> {
    let state = test_state().await;
    let suffix = uuid::Uuid::now_v7().simple().to_string();

    // 50 leaked-row shapes: registry rows whose `db_<name>` schema does not
    // exist. Inserted directly (never via create_database) and NOT wrapped in
    // TestDb — they have nothing to drop, and the test cleans its own rows.
    for i in 0..50 {
        sqlx::query("INSERT INTO rtdb_auth.databases (name, created_at) VALUES ($1, $2)")
            .bind(format!("stale{i}{}", &suffix[..10]))
            .bind(db::now_ms())
            .execute(&state.pool)
            .await?;
    }

    // One real db so the merge still does (and reports) real work amid the
    // stale rows, plus two anon/real pairs so the differential below can run
    // the baseline and the measured pass without the first merge consuming
    // the second pair's anon row.
    let db = owned_db(&state, false).await?;
    let anon = format!("anon_stale_{}", &suffix[..16]);
    let real = format!("real_stale_{}", &suffix[..16]);
    let anon2 = format!("anon2_stale_{}", &suffix[..15]);
    let real2 = format!("real2_stale_{}", &suffix[..15]);
    for (id, is_anon) in [
        (&anon, true),
        (&real, false),
        (&anon2, true),
        (&real2, false),
    ] {
        sqlx::query(
            "INSERT INTO rtdb_auth.users (id, login, email, anonymous, created_at) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(id)
        .bind(if is_anon { "anonymous" } else { "github" })
        .bind(if is_anon {
            None
        } else {
            Some(format!("{id}@example.com"))
        })
        .bind(is_anon)
        .bind(db::now_ms())
        .execute(&state.pool)
        .await?;
    }
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_doc(
                "docs",
                json!({ "title": "a", "owner": anon, "editors": [] }),
            ),
            PrincipalCtx::bypass(),
        )
        .await?;

    // The regression guard is a DIFFERENTIAL, not a wall-clock: a full merge
    // legitimately costs real work per LIVE sibling-test db (spawns, schema
    // scans, storage swaps) and that cost swings with machine load and
    // parallel-test population. Run the same merge twice — baseline before
    // the stale rows exist, then with 50 of them — and bound only the DELTA.
    // Pre-fix each stale row cost a committer spawn + failed DDL round trips
    // (~0.2–0.3s each, and worse: its INTERNAL error aborted the whole
    // merge); the filtered pass skips them in one bulk schemata probe.
    let started = std::time::Instant::now();
    rtdb_server::merge::merge_users(&state, &anon, &real).await?;
    let baseline = started.elapsed();

    let started = std::time::Instant::now();
    let report = rtdb_server::merge::merge_users(&state, &anon2, &real2).await?;
    let with_stale = started.elapsed();
    assert!(
        with_stale < baseline + std::time::Duration::from_secs(3),
        "50 stale registry rows added {:?} over the {:?} baseline — \
         stale rows must be skipped without committer spawns",
        with_stale - baseline,
        baseline
    );

    // The real db was still merged and reported. Sibling tests run in
    // parallel against the shared registry and their live dbs legitimately
    // appear (as zero-restamp entries) — but no STALE name may: nothing is
    // restamped in a schema-less db.
    assert!(report.dbs.contains_key(&db));
    assert!(
        report.dbs.keys().all(|name| !name.starts_with("stale")),
        "a schema-less db leaked into the merge report: {:?}",
        report.dbs.keys().collect::<Vec<_>>()
    );
    assert_eq!(owned_doc_count(&state.pool, &db, &real).await, 1);
    assert_eq!(
        report.sessions_repointed, 0,
        "the anon rows were created directly, not via sessions"
    );

    // Clean up: these rows were inserted outside RAII on purpose.
    sqlx::query("DELETE FROM rtdb_auth.databases WHERE name LIKE $1")
        .bind(format!("stale%{}", &suffix[..10]))
        .execute(&state.pool)
        .await?;
    Ok(())
}

#[tokio::test]
async fn merge_users_restamps_computed_fields_over_principal_fields() -> anyhow::Result<()> {
    let state = test_state().await;
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name).await?;
    let db = String::from(wrap_test_db(name));
    push_schema(&state.pool, &db, computed_owner_schema()).await?;

    let anon = "user_anon_1";
    let real = "user_real_1";
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_doc("docs", json!({"title": "a", "owner": anon})),
            PrincipalCtx::bypass(),
        )
        .await?;

    let result = state
        .realtime
        .committers
        .merge_users(&db, anon, real)
        .await?;
    assert_eq!(result.tables.get("docs"), Some(&1));
    assert!(result.conflicts.is_empty());

    // The computed value was re-derived from the REWRITTEN owner, in both the
    // doc body and the typed column.
    let expected = format!("owner:{real}");
    let (doc, col): (Value, Option<String>) = sqlx::query_as(&format!(
        "SELECT \"doc\", \"f_ownerlabel\" FROM \"{}\".\"t_docs\" \
         WHERE \"doc\"->'owner' = to_jsonb($1::text)",
        rtdb_server::ddl::pg_schema(&db)
    ))
    .bind(real)
    .fetch_one(&state.pool)
    .await?;
    assert_eq!(doc["ownerLabel"], serde_json::json!(expected));
    assert_eq!(col.as_deref(), Some(expected.as_str()));
    Ok(())
}

#[tokio::test]
async fn merge_users_skips_unique_conflict_and_reports_it() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = owned_db(&state, true).await?;

    let anon = "user_anon_2";
    let real = "user_real_2";
    // The real user already owns ("dup-title"); the anon user owns the
    // colliding row plus one free row.
    for (owner, title) in [(real, "dup-title"), (anon, "dup-title"), (anon, "free")] {
        state
            .realtime
            .committers
            .mutate(
                &db,
                None,
                insert_doc(
                    "docs",
                    json!({ "title": title, "owner": owner, "editors": [] }),
                ),
                PrincipalCtx::bypass(),
            )
            .await?;
    }

    let result = state
        .realtime
        .committers
        .merge_users(&db, anon, real)
        .await?;
    // The free row restamped; the colliding row skipped and reported.
    assert_eq!(result.tables.get("docs"), Some(&1));
    assert_eq!(result.conflicts.len(), 1);
    assert_eq!(result.conflicts[0].table, "docs");
    // The conflicting row keeps the anon owner.
    assert_eq!(owned_doc_count(&state.pool, &db, anon).await, 1);
    Ok(())
}

#[tokio::test]
async fn merge_users_fires_subscription_fan_out() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = owned_db(&state, false).await?;
    let anon = "user_anon_3";
    let real = "user_real_3";
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_doc(
                "docs",
                json!({ "title": "a", "owner": anon, "editors": [] }),
            ),
            PrincipalCtx::bypass(),
        )
        .await?;

    // Subscribe to the by_owner eq-window on the anon uid, mirroring
    // subs_test.rs's subscribe pattern (query built from JSON there too).
    let query: rtdb_server::query::Query = serde_json::from_value(json!({
        "table": "docs",
        "index": "by_owner",
        "eq": [anon]
    }))
    .expect("parse query");
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .realtime
        .committers
        .subscribe(
            &db,
            next_conn_id(),
            "q1".to_string(),
            query,
            tx,
            PrincipalCtx::bypass(),
        )
        .await?;
    match rx.try_recv().expect("initial query update") {
        ServerMessage::QueryUpdate { result, .. } => {
            assert_eq!(result.as_array().expect("docs array").len(), 1);
        }
        other => panic!("expected QueryUpdate, got {other:?}"),
    }

    state
        .realtime
        .committers
        .merge_users(&db, anon, real)
        .await?;
    // The eq:[anon] window is now empty — fan-out must have pushed the new
    // (empty) result before merge_users replied.
    match rx
        .try_recv()
        .expect("fan-out pushed a query update after the merge")
    {
        ServerMessage::QueryUpdate { result, .. } => {
            assert_eq!(result.as_array().expect("docs array").len(), 0);
        }
        other => panic!("expected QueryUpdate, got {other:?}"),
    }
    Ok(())
}

#[tokio::test]
async fn merge_users_publishes_committed_restamps_when_it_aborts_mid_way() -> anyhow::Result<()> {
    let state = test_state().await;
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name).await?;
    let db = wrap_test_db(name);
    // Two principal-bearing tables; `schema.tables` is a BTreeMap, so "alpha"
    // is merged before "beta" deterministically.
    let schema: SchemaDef = serde_json::from_value(json!({
        "tables": {
            "alpha": {
                "fields": {
                    "title": {"type": "string"},
                    "owner": {"type": "string"},
                    "editors": {"type": "array", "element": {"type": "string"}}
                },
                "indexes": [{"name": "by_owner", "fields": ["owner"]}],
                "ownerField": "owner",
                "collaboratorsField": "editors"
            },
            "beta": {
                "fields": {
                    "title": {"type": "string"},
                    "owner": {"type": "string"},
                    "editors": {"type": "array", "element": {"type": "string"}}
                },
                "indexes": [{"name": "by_owner", "fields": ["owner"]}],
                "ownerField": "owner",
                "collaboratorsField": "editors"
            }
        }
    }))
    .expect("parse two-table schema");
    push_schema(&state.pool, &db, schema).await?;

    let anon = "user_anon_4";
    let real = "user_real_4";
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_doc(
                "alpha",
                json!({ "title": "a", "owner": anon, "editors": [] }),
            ),
            PrincipalCtx::bypass(),
        )
        .await?;
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_doc(
                "beta",
                json!({ "title": "a", "owner": anon, "editors": [] }),
            ),
            PrincipalCtx::bypass(),
        )
        .await?;

    // Fault injection for the abort path: a CHECK constraint that only the
    // restamped beta row violates (a non-23505 statement error). The anon
    // inserts pass it; `apply_update` restamping owner to `real` fails it.
    sqlx::query(&format!(
        "ALTER TABLE \"{}\".\"t_beta\" ADD CONSTRAINT \"merge_bomb\" \
         CHECK (NOT (\"f_owner\" = '{real}' AND \"doc\"->>'title' = 'bomb'))",
        rtdb_server::ddl::pg_schema(&db)
    ))
    .execute(&state.pool)
    .await?;
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_doc(
                "beta",
                json!({ "title": "bomb", "owner": anon, "editors": [] }),
            ),
            PrincipalCtx::bypass(),
        )
        .await?;

    // Subscribe to alpha's by_owner eq-window on the anon uid.
    let query: rtdb_server::query::Query = serde_json::from_value(json!({
        "table": "alpha",
        "index": "by_owner",
        "eq": [anon]
    }))
    .expect("parse query");
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .realtime
        .committers
        .subscribe(
            &db,
            next_conn_id(),
            "q1".to_string(),
            query,
            tx,
            PrincipalCtx::bypass(),
        )
        .await?;
    match rx.try_recv().expect("initial query update") {
        ServerMessage::QueryUpdate { result, .. } => {
            assert_eq!(result.as_array().expect("docs array").len(), 1);
        }
        other => panic!("expected QueryUpdate, got {other:?}"),
    }

    // The merge aborts on beta's bomb row — non-conflict, so the whole arm
    // errors — but alpha's restamp is already durable.
    let err = state
        .realtime
        .committers
        .merge_users(&db, anon, real)
        .await
        .expect_err("merge must abort on the CHECK constraint");
    assert_eq!(err.code, ErrorCode::Internal);

    // The committed alpha restamp still published: the eq:[anon] window went
    // empty and the subscriber was pushed the new result.
    match rx
        .try_recv()
        .expect("fan-out pushed for the committed restamp despite the abort")
    {
        ServerMessage::QueryUpdate { result, .. } => {
            assert_eq!(result.as_array().expect("docs array").len(), 0);
        }
        other => panic!("expected QueryUpdate, got {other:?}"),
    }
    // Durable state: alpha restamped, beta untouched (both rows keep anon).
    let (alpha_real,): (i64,) = sqlx::query_as(&format!(
        "SELECT COUNT(*) FROM \"{}\".\"t_alpha\" WHERE \"doc\"->'owner' = to_jsonb($1::text)",
        rtdb_server::ddl::pg_schema(&db)
    ))
    .bind(real)
    .fetch_one(&state.pool)
    .await?;
    assert_eq!(alpha_real, 1);
    // Beta: the "a" row restamped before the bomb row aborted; the bomb row
    // keeps the anon owner (its UPDATE failed).
    let (beta_real,): (i64,) = sqlx::query_as(&format!(
        "SELECT COUNT(*) FROM \"{}\".\"t_beta\" WHERE \"doc\"->'owner' = to_jsonb($1::text)",
        rtdb_server::ddl::pg_schema(&db)
    ))
    .bind(real)
    .fetch_one(&state.pool)
    .await?;
    assert_eq!(beta_real, 1);
    let (beta_anon,): (i64,) = sqlx::query_as(&format!(
        "SELECT COUNT(*) FROM \"{}\".\"t_beta\" WHERE \"doc\"->'owner' = to_jsonb($1::text)",
        rtdb_server::ddl::pg_schema(&db)
    ))
    .bind(anon)
    .fetch_one(&state.pool)
    .await?;
    assert_eq!(beta_anon, 1);
    // Metric accounting ran for BOTH committed restamps (alpha + beta "a").
    let snap = state
        .runtime
        .metrics
        .snapshot(
            &state.pool,
            &state.realtime.subs,
            state.runtime.started_at,
            0,
            0,
        )
        .await;
    assert_eq!(snap.merge_docs_total, 2);
    Ok(())
}

#[tokio::test]
async fn merge_users_aborts_on_scan_failure_and_publishes_committed_restamps() -> anyhow::Result<()>
{
    // This test deliberately leaves a registered db with a broken physical
    // table (RENAME) while the db stays alive. On the shared test PG database
    // that state is visible to every concurrently-running orchestrator test
    // (merge_users walks ALL registered dbs) and could leak via the async
    // cleanup worker — so it runs on its own throwaway PG database, dropped
    // synchronously before the test returns.
    let pg_db = format!("t{}", uuid::Uuid::now_v7().simple());
    let base_url = std::env::var("RTDB_TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://rtdb:rtdb@127.0.0.1:55434/rtdb".into());
    let server = {
        let trimmed = base_url.trim_end_matches('/');
        &trimmed[..trimmed.rfind('/').expect("database url has a path")]
    };
    let base_pool = sqlx::PgPool::connect(&base_url).await?;
    sqlx::query(&format!("CREATE DATABASE \"{pg_db}\""))
        .execute(&base_pool)
        .await?;
    let mut config = common::test_config();
    config.database_url = format!("{server}/{pg_db}");
    let pool = sqlx::PgPool::connect(&config.database_url).await?;
    db::bootstrap(&pool).await?;
    let state = rtdb_server::AppState::new(pool, config, common::test_hot());

    let result: anyhow::Result<()> = async {
        let name = format!("t{}", uuid::Uuid::now_v7().simple());
        db::create_database(&state.pool, &name).await?;
        let db = name;
        // BTreeMap order: "alpha" merges (and commits) before "beta"'s scan fails.
        let schema: SchemaDef = serde_json::from_value(json!({
            "tables": {
                "alpha": {
                    "fields": {
                        "title": {"type": "string"},
                        "owner": {"type": "string"},
                        "editors": {"type": "array", "element": {"type": "string"}}
                    },
                    "indexes": [{"name": "by_owner", "fields": ["owner"]}],
                    "ownerField": "owner",
                    "collaboratorsField": "editors"
                },
                "beta": {
                    "fields": {
                        "title": {"type": "string"},
                        "owner": {"type": "string"},
                        "editors": {"type": "array", "element": {"type": "string"}}
                    },
                    "indexes": [{"name": "by_owner", "fields": ["owner"]}],
                    "ownerField": "owner",
                    "collaboratorsField": "editors"
                }
            }
        }))
        .expect("parse two-table schema");
        push_schema(&state.pool, &db, schema).await?;

        let anon = "user_anon_5";
        let real = "user_real_5";
        state
            .realtime
            .committers
            .mutate(
                &db,
                None,
                insert_doc(
                    "alpha",
                    json!({ "title": "a", "owner": anon, "editors": [] }),
                ),
                PrincipalCtx::bypass(),
            )
            .await?;
        state
            .realtime
            .committers
            .mutate(
                &db,
                None,
                insert_doc(
                    "beta",
                    json!({ "title": "b", "owner": anon, "editors": [] }),
                ),
                PrincipalCtx::bypass(),
            )
            .await?;

        // Fault injection for the SCAN path: rename beta's physical table out from
        // under the schema. The candidate SELECT then fails (undefined_table) while
        // `database_exists` stays true — a db-alive scan failure must abort, not
        // silently skip the table.
        sqlx::query(&format!(
            "ALTER TABLE \"{}\".\"t_beta\" RENAME TO \"t_beta_gone\"",
            rtdb_server::ddl::pg_schema(&db)
        ))
        .execute(&state.pool)
        .await?;

        // Subscribe to alpha's by_owner eq-window on the anon uid.
        let query: rtdb_server::query::Query = serde_json::from_value(json!({
            "table": "alpha",
            "index": "by_owner",
            "eq": [anon]
        }))
        .expect("parse query");
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        state
            .realtime
            .committers
            .subscribe(
                &db,
                next_conn_id(),
                "q1".to_string(),
                query,
                tx,
                PrincipalCtx::bypass(),
            )
            .await?;
        match rx.try_recv().expect("initial query update") {
            ServerMessage::QueryUpdate { result, .. } => {
                assert_eq!(result.as_array().expect("docs array").len(), 1);
            }
            other => panic!("expected QueryUpdate, got {other:?}"),
        }

        // The scan of beta fails with the db alive: the merge must abort (never
        // reach the orchestrator's session re-point / guarded anon delete) yet
        // still publish alpha's committed restamp.
        let err = state
            .realtime
            .committers
            .merge_users(&db, anon, real)
            .await
            .expect_err("merge must abort when a table scan fails and the db is alive");
        assert_eq!(err.code, ErrorCode::Internal);

        // The committed alpha restamp still published: the eq:[anon] window went
        // empty and the subscriber was pushed the new result.
        match rx
            .try_recv()
            .expect("fan-out pushed for the committed restamp despite the abort")
        {
            ServerMessage::QueryUpdate { result, .. } => {
                assert_eq!(result.as_array().expect("docs array").len(), 0);
            }
            other => panic!("expected QueryUpdate, got {other:?}"),
        }
        // Durable state: alpha restamped to the real owner; beta untouched (its
        // doc still carries the anon owner under the renamed table).
        let (alpha_real,): (i64,) = sqlx::query_as(&format!(
            "SELECT COUNT(*) FROM \"{}\".\"t_alpha\" WHERE \"doc\"->'owner' = to_jsonb($1::text)",
            rtdb_server::ddl::pg_schema(&db)
        ))
        .bind(real)
        .fetch_one(&state.pool)
        .await?;
        assert_eq!(alpha_real, 1);
        let (beta_anon,): (i64,) = sqlx::query_as(&format!(
        "SELECT COUNT(*) FROM \"{}\".\"t_beta_gone\" WHERE \"doc\"->'owner' = to_jsonb($1::text)",
        rtdb_server::ddl::pg_schema(&db)
    ))
    .bind(anon)
    .fetch_one(&state.pool)
    .await?;
        assert_eq!(beta_anon, 1);
        // Metric accounting ran for alpha's committed restamp.
        let snap = state
            .runtime
            .metrics
            .snapshot(
                &state.pool,
                &state.realtime.subs,
                state.runtime.started_at,
                0,
                0,
            )
            .await;
        assert_eq!(snap.merge_docs_total, 1);
        Ok(())
    }
    .await;

    // Teardown runs even when the body errored: close the state's pool (its
    // background tasks hold connections open), then force-drop the throwaway
    // PG database so nothing leaks into later runs.
    state.pool.close().await;
    sqlx::query(&format!("DROP DATABASE IF EXISTS \"{pg_db}\" WITH (FORCE)"))
        .execute(&base_pool)
        .await?;
    base_pool.close().await;
    result
}

#[tokio::test]
async fn merge_users_orchestrates_sessions_storage_and_guarded_delete() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = owned_db(&state, false).await?;

    // Mint a real anon user + session directly (mirrors the /auth/anonymous
    // handler's INSERTs — rtdb_auth.users/sessions DDL in db.rs: users key on
    // id with login/email/anonymous/created_at, sessions on token_hash).
    let anon_id = format!("anon_{}", uuid::Uuid::now_v7().simple());
    let real_id = format!("real_{}", uuid::Uuid::now_v7().simple());
    let anon_token = format!("tok_{}", uuid::Uuid::now_v7().simple());
    let now = db::now_ms();
    sqlx::query(
        "INSERT INTO rtdb_auth.users (id, login, email, anonymous, created_at) \
         VALUES ($1, 'anonymous', NULL, TRUE, $2)",
    )
    .bind(&anon_id)
    .bind(now)
    .execute(&state.pool)
    .await?;
    sqlx::query(
        "INSERT INTO rtdb_auth.users (id, login, email, anonymous, created_at) \
         VALUES ($1, 'github', $2, FALSE, $3)",
    )
    .bind(&real_id)
    .bind(format!("{real_id}@example.com"))
    .bind(now)
    .execute(&state.pool)
    .await?;
    sqlx::query(
        "INSERT INTO rtdb_auth.sessions (token_hash, user_id, created_at, expires_at) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(db::sha256_hex(&anon_token))
    .bind(&anon_id)
    .bind(now)
    .bind(now + 86_400_000)
    .execute(&state.pool)
    .await?;

    // An owned doc + a storage blob owned by the anon user.
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_doc(
                "docs",
                json!({ "title": "a", "owner": anon_id, "editors": [] }),
            ),
            PrincipalCtx::bypass(),
        )
        .await?;
    rtdb_server::storage::ensure_table(&state.pool, &db).await?;
    let blob_id = rtdb_server::storage::put(
        &state.pool,
        &db,
        &format!("sha_{}", uuid::Uuid::now_v7().simple()),
        4,
        Some("application/octet-stream"),
        Some(&anon_id),
        b"blob",
    )
    .await?;

    let report = rtdb_server::merge::merge_users(&state, &anon_id, &real_id).await?;
    assert_eq!(
        report.dbs.get(&db).and_then(|r| r.tables.get("docs")),
        Some(&1)
    );
    assert_eq!(report.storage_repointed, 1);
    assert_eq!(report.sessions_repointed, 1);
    assert!(report.anon_deleted);

    // The blob now belongs to the real user.
    let (owner,): (Option<String>,) = sqlx::query_as(&format!(
        "SELECT owner_id FROM \"{}\".\"storage\" WHERE id = $1",
        rtdb_server::ddl::pg_schema(&db)
    ))
    .bind(&blob_id)
    .fetch_one(&state.pool)
    .await?;
    assert_eq!(owner.as_deref(), Some(real_id.as_str()));

    // The session token now resolves to the REAL user (re-point promoted it).
    match rtdb_server::auth::resolve_bearer(&state.pool, &anon_token).await? {
        rtdb_server::auth::Principal::User {
            user_id, anonymous, ..
        } => {
            assert_eq!(user_id, real_id);
            assert!(!anonymous);
        }
        other => panic!("expected user principal, got {other:?}"),
    }

    // Guarded delete: the anon row is gone; a re-run is a no-op.
    let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM rtdb_auth.users WHERE id = $1")
        .bind(&anon_id)
        .fetch_one(&state.pool)
        .await?;
    assert_eq!(n, 0);
    let again = rtdb_server::merge::merge_users(&state, &anon_id, &real_id).await?;
    assert_eq!(again.sessions_repointed, 0);
    assert!(!again.anon_deleted);

    // Refusal: a non-anon (real) source row is rejected.
    let real2 = format!("real2_{}", uuid::Uuid::now_v7().simple());
    sqlx::query(
        "INSERT INTO rtdb_auth.users (id, login, email, anonymous, created_at) \
         VALUES ($1, 'github', $2, FALSE, $3)",
    )
    .bind(&real2)
    .bind(format!("{real2}@example.com"))
    .bind(now)
    .execute(&state.pool)
    .await?;
    assert!(
        rtdb_server::merge::merge_users(&state, &real_id, &real2)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn merge_users_swaps_storage_blobs_in_a_schema_less_db() -> anyhow::Result<()> {
    let state = test_state().await;
    // A registered db with NO pushed schema: uploads require only auth +
    // storage::ensure_table (upload_handler), so an anon user can own blobs
    // here. The merge must skip the doc restamp (no schema -> NotFound from
    // the committer arm) but still swap the blob owner — otherwise the blob
    // is permanently owned by a user the guarded delete then removes.
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name).await?;
    let db = wrap_test_db(name);

    let anon_id = format!("anon_{}", uuid::Uuid::now_v7().simple());
    let real_id = format!("real_{}", uuid::Uuid::now_v7().simple());
    let now = db::now_ms();
    sqlx::query(
        "INSERT INTO rtdb_auth.users (id, login, email, anonymous, created_at) \
         VALUES ($1, 'anonymous', NULL, TRUE, $2)",
    )
    .bind(&anon_id)
    .bind(now)
    .execute(&state.pool)
    .await?;
    sqlx::query(
        "INSERT INTO rtdb_auth.users (id, login, email, anonymous, created_at) \
         VALUES ($1, 'github', $2, FALSE, $3)",
    )
    .bind(&real_id)
    .bind(format!("{real_id}@example.com"))
    .bind(now)
    .execute(&state.pool)
    .await?;

    rtdb_server::storage::ensure_table(&state.pool, &db).await?;
    let blob_id = rtdb_server::storage::put(
        &state.pool,
        &db,
        &format!("sha_{}", uuid::Uuid::now_v7().simple()),
        4,
        Some("application/octet-stream"),
        Some(&anon_id),
        b"blob",
    )
    .await?;

    let report = rtdb_server::merge::merge_users(&state, &anon_id, &real_id).await?;
    // Doc restamp skipped (no schema -> no report entry), storage swapped,
    // anon row deleted.
    assert!(!report.dbs.contains_key(db.as_str()));
    assert_eq!(report.storage_repointed, 1);
    assert!(report.anon_deleted);
    let (owner,): (Option<String>,) = sqlx::query_as(&format!(
        "SELECT owner_id FROM \"{}\".\"storage\" WHERE id = $1",
        rtdb_server::ddl::pg_schema(&db)
    ))
    .bind(&blob_id)
    .fetch_one(&state.pool)
    .await?;
    assert_eq!(owner.as_deref(), Some(real_id.as_str()));

    // Self-merge refusal: anon == real would re-stamp docs then delete the
    // target row itself.
    assert!(
        rtdb_server::merge::merge_users(&state, &anon_id, &anon_id)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn admin_merge_users_endpoint_requires_confirm_and_runs_merge() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = owned_db(&state, false).await?;

    let anon_id = format!("anon_{}", uuid::Uuid::now_v7().simple());
    let real_id = format!("real_{}", uuid::Uuid::now_v7().simple());
    let now = db::now_ms();
    sqlx::query(
        "INSERT INTO rtdb_auth.users (id, login, email, anonymous, created_at) \
         VALUES ($1, 'anonymous', NULL, TRUE, $2)",
    )
    .bind(&anon_id)
    .bind(now)
    .execute(&state.pool)
    .await?;
    sqlx::query(
        "INSERT INTO rtdb_auth.users (id, login, email, anonymous, created_at) \
         VALUES ($1, 'github', $2, FALSE, $3)",
    )
    .bind(&real_id)
    .bind(format!("{real_id}@example.com"))
    .bind(now)
    .execute(&state.pool)
    .await?;
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_doc(
                "docs",
                json!({ "title": "a", "owner": anon_id, "editors": [] }),
            ),
            PrincipalCtx::bypass(),
        )
        .await?;

    let addr = common::spawn_app(state.clone()).await;
    let client = reqwest::Client::new();

    // Wrong confirm -> 400, nothing merged.
    let resp = client
        .post(format!("http://{addr}/admin/merge-users"))
        .header("authorization", "Bearer test-admin-key")
        .json(&json!({ "anonUserId": anon_id, "realUserId": real_id, "confirm": "nope" }))
        .send()
        .await?;
    assert_eq!(resp.status(), 400);
    // Nothing merged: the doc still carries the anon owner.
    assert_eq!(owned_doc_count(&state.pool, &db, &anon_id).await, 1);

    // Correct confirm -> report.
    let resp = client
        .post(format!("http://{addr}/admin/merge-users"))
        .header("authorization", "Bearer test-admin-key")
        .json(&json!({ "anonUserId": anon_id, "realUserId": real_id, "confirm": real_id }))
        .send()
        .await?;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["dbs"][&db]["tables"]["docs"], json!(1));
    assert_eq!(body["anonDeleted"], json!(true));

    // Metric visible on the Prometheus scrape.
    let metrics = client
        .get(format!("http://{addr}/metrics"))
        .send()
        .await?
        .text()
        .await?;
    assert!(metrics.contains("rtdb_merge_docs_total 1"));

    // Unauthorized without the admin key.
    let resp = client
        .post(format!("http://{addr}/admin/merge-users"))
        .json(&json!({ "anonUserId": anon_id, "realUserId": real_id, "confirm": real_id }))
        .send()
        .await?;
    assert_eq!(resp.status(), 401);

    // Missing anon row -> refusal (the orchestrator treats a missing row as a
    // completed merge, so the endpoint must refuse it itself). Nothing merged.
    let ghost = format!("ghost_{}", uuid::Uuid::now_v7().simple());
    let resp = client
        .post(format!("http://{addr}/admin/merge-users"))
        .header("authorization", "Bearer test-admin-key")
        .json(&json!({ "anonUserId": ghost, "realUserId": real_id, "confirm": real_id }))
        .send()
        .await?;
    assert_eq!(resp.status(), 404);
    let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM rtdb_auth.users WHERE id = $1")
        .bind(&ghost)
        .fetch_one(&state.pool)
        .await?;
    assert_eq!(n, 0);
    Ok(())
}

#[tokio::test]
async fn merge_users_treats_missing_storage_table_as_zero_rows() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = owned_db(&state, false).await?;

    let anon_id = format!("anon_{}", uuid::Uuid::now_v7().simple());
    let real_id = format!("real_{}", uuid::Uuid::now_v7().simple());
    let now = db::now_ms();
    sqlx::query(
        "INSERT INTO rtdb_auth.users (id, login, email, anonymous, created_at) \
         VALUES ($1, 'anonymous', NULL, TRUE, $2)",
    )
    .bind(&anon_id)
    .bind(now)
    .execute(&state.pool)
    .await?;
    sqlx::query(
        "INSERT INTO rtdb_auth.users (id, login, email, anonymous, created_at) \
         VALUES ($1, 'github', $2, FALSE, $3)",
    )
    .bind(&real_id)
    .bind(format!("{real_id}@example.com"))
    .bind(now)
    .execute(&state.pool)
    .await?;

    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_doc(
                "docs",
                json!({ "title": "a", "owner": anon_id, "editors": [] }),
            ),
            PrincipalCtx::bypass(),
        )
        .await?;
    // The mutate spawned the committer, whose startup ran storage::ensure_table
    // (committer.rs); the running committer never re-ensures, so dropping the
    // relation makes the merge's owner swap hit 42P01 (undefined_table) —
    // which must be tolerated as zero rows, not fail the merge.
    sqlx::query(&format!(
        "DROP TABLE \"{}\".\"storage\"",
        rtdb_server::ddl::pg_schema(&db)
    ))
    .execute(&state.pool)
    .await?;

    let report = rtdb_server::merge::merge_users(&state, &anon_id, &real_id).await?;
    assert_eq!(
        report.dbs.get(&db).and_then(|r| r.tables.get("docs")),
        Some(&1)
    );
    assert_eq!(report.storage_repointed, 0);
    assert!(report.anon_deleted);
    Ok(())
}

// --- FM-27 Task 5: OAuth-triggered anon→real merge, end to end -------------
//
// The wiremock helpers below are copied from oauth_test.rs (kept local per the
// task brief — a surgical diff beats factoring shared test plumbing mid-task).

/// A pseudo-random positive `i64` for `github_id`, unique across parallel tests
/// AND across runs (the shared `rtdb_auth.users` table persists between runs
/// and enforces `github_id` uniqueness). 15 hex nibbles stay under `i64::MAX`.
fn unique_github_id() -> i64 {
    i64::from_str_radix(&db::random_token()[..15], 16).expect("parse hex as i64")
}

/// Mounts the three GitHub endpoints the callback hits, each expected to be
/// called exactly once — wiremock's drop-time verification fails the test if
/// the handler re-fetches after a replay. Copied from oauth_test.rs.
async fn mount_github_user_mocks(
    mock: &MockServer,
    github_id: i64,
    login: &str,
    email_body: Value,
) {
    Mock::given(method("POST"))
        .and(path("/login/oauth/access_token"))
        .and(header("accept", "application/json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "gh-access-token",
            "token_type": "bearer"
        })))
        .expect(1)
        .mount(mock)
        .await;

    Mock::given(method("GET"))
        .and(path("/user"))
        .and(header("user-agent", "par-rt-db"))
        .and(header("authorization", "Bearer gh-access-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": github_id,
            "login": login,
            "name": "Merge User"
        })))
        .expect(1)
        .mount(mock)
        .await;

    Mock::given(method("GET"))
        .and(path("/user/emails"))
        .and(header("user-agent", "par-rt-db"))
        .and(header("authorization", "Bearer gh-access-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(email_body))
        .expect(1)
        .mount(mock)
        .await;
}

/// `GET /auth/github/begin?origin=` returning the minted state token. Copied
/// from oauth_test.rs.
async fn begin_login(client: &reqwest::Client, addr: std::net::SocketAddr, origin: &str) -> String {
    let resp = client
        .get(format!("http://{addr}/auth/github/begin?origin={origin}"))
        .send()
        .await
        .expect("send github begin");
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.expect("begin json");
    body["state"]
        .as_str()
        .expect("state field in begin response")
        .to_string()
}

/// Full anonymous→GitHub-wiremock→merge flow: mint an anon session, create an
/// owned doc as the anon user, begin GitHub login FROM the anon session (the
/// cookie jar carries `rtdb_session`, so `/begin` resolves the anon principal
/// server-side and binds its id), complete the login through the callback, and
/// assert the merge ran: the doc is re-stamped to the real user, the anon row
/// is gone, and the anon session token now resolves as the real user.
#[tokio::test]
async fn oauth_login_merges_anon_footprint_end_to_end() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    // Distinct github_id + email per run so parallel tests and re-runs never
    // collide in the persistent rtdb_auth.users table.
    let github_id = unique_github_id();
    let email = format!("merge-{}@example.com", db::new_id());
    mount_github_user_mocks(
        &mock,
        github_id,
        "mergeuser",
        json!([{ "email": email, "verified": true, "primary": true }]),
    )
    .await;

    let mut cfg = common::test_config();
    cfg.github_base_url = mock.uri();
    cfg.github_api_url = mock.uri();
    cfg.github_client_id = Some("test-client".into());
    cfg.github_client_secret = Some("test-secret".into());
    cfg.auth_anonymous_enabled = true;

    let pool = sqlx::PgPool::connect(&cfg.database_url).await?;
    db::bootstrap(&pool).await?;
    let state = AppState::new(pool, cfg, common::test_hot());
    let addr = common::spawn_app(state.clone()).await;

    let db_name = owned_db(&state, false).await?;

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .cookie_store(true)
        .build()?;

    // 1. Mint the anonymous session (token in the body + rtdb_session cookie).
    let resp = client
        .post(format!("http://{addr}/auth/anonymous"))
        .send()
        .await?;
    assert_eq!(resp.status(), 200);
    let anon_body: Value = resp.json().await?;
    let anon_token = anon_body["token"]
        .as_str()
        .expect("anon session token")
        .to_string();

    let anon_id = match rtdb_server::auth::resolve_bearer(&state.pool, &anon_token).await? {
        rtdb_server::auth::Principal::User { user_id, .. } => user_id,
        other => panic!("expected user principal, got {other:?}"),
    };

    // 2. An owned doc created as the anon user.
    state
        .realtime
        .committers
        .mutate(
            &db_name,
            None,
            insert_doc(
                "docs",
                json!({ "title": "guest work", "owner": anon_id, "editors": [] }),
            ),
            PrincipalCtx::bypass(),
        )
        .await?;

    // 3. Begin GitHub login FROM the anon session; the binding was recorded
    //    server-side from the verified session, never caller-supplied.
    let state_token = begin_login(&client, addr, "http://localhost:5173").await;
    let (bound,): (Option<String>,) =
        sqlx::query_as("SELECT anon_user_id FROM rtdb_auth.oauth_states WHERE state = $1")
            .bind(&state_token)
            .fetch_one(&state.pool)
            .await?;
    assert_eq!(bound.as_deref(), Some(anon_id.as_str()));

    // 4. Complete the login via the wiremock callback.
    let resp = client
        .get(format!(
            "http://{addr}/auth/callback?code=any-code&state={state_token}"
        ))
        .send()
        .await?;
    assert_eq!(resp.status(), 200);

    // 5. Assertions: doc re-stamped to the real user; anon row gone; the anon
    //    token now resolves as the real (non-anonymous) user.
    let (real_row,): (String,) = sqlx::query_as("SELECT id FROM rtdb_auth.users WHERE email = $1")
        .bind(&email)
        .fetch_one(&state.pool)
        .await?;
    assert_eq!(owned_doc_count(&state.pool, &db_name, &real_row).await, 1);
    assert_eq!(owned_doc_count(&state.pool, &db_name, &anon_id).await, 0);

    let (gone,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM rtdb_auth.users WHERE id = $1")
        .bind(&anon_id)
        .fetch_one(&state.pool)
        .await?;
    assert_eq!(gone, 0);

    match rtdb_server::auth::resolve_bearer(&state.pool, &anon_token).await? {
        rtdb_server::auth::Principal::User {
            user_id, anonymous, ..
        } => {
            assert_eq!(user_id, real_row);
            assert!(!anonymous);
        }
        other => panic!("expected user principal, got {other:?}"),
    }
    Ok(())
}
