//! Streaming snapshot export/import (card 01a187ddd3e375b4a38600c0fb238052).
//!
//! The `/admin/export-db` and `/admin/import-db` HTTP handlers consume and
//! produce byte-identical JSONL to the buffered `export_database` /
//! `import_database` pair (clone-db's path), but with bounded server memory:
//! export streams one row at a time from a server-side cursor, and import
//! decodes one line at a time with a hard per-line byte cap, flushing
//! change-feed stamps in bounded batches inside the import transaction.
//!
//! Rollback contract: a malformed line or a transport-interrupted body aborts
//! the import transaction, so a truncated or corrupt snapshot never commits
//! (documents; the additive schema push may persist, as on the buffered path).
//! A clean close at a full-record boundary is indistinguishable from a complete
//! snapshot without a manifest — JSONL truncation is detected only when the
//! transport itself reports it (declared-length / chunked framing), and these
//! tests exercise exactly that framing.

use std::time::Duration;

use futures_util::stream::{self, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::common::{fresh_db, spawn_app, test_state, wrap_test_db};
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::change_log;
use rtdb_server::db;
use rtdb_server::schema::SchemaDef;
use rtdb_server::snapshot;
use rtdb_server::snapshot::MAX_SNAPSHOT_LINE_BYTES;
use rtdb_server::txn::{Step, Transaction, execute_txn};

fn kanban_schema() -> SchemaDef {
    serde_json::from_value(crate::common::kanban_schema_json()).expect("parse kanban schema")
}

fn project_doc(name: &str, description: &str) -> serde_json::Map<String, serde_json::Value> {
    serde_json::json!({
        "name": name,
        "description": description,
        "status": "active",
        "tags": ["a"],
        "updatedAt": 1.0
    })
    .as_object()
    .expect("json object")
    .clone()
}

async fn insert_project(
    pool: &sqlx::PgPool,
    db_name: &str,
    name: &str,
    description: &str,
) -> anyhow::Result<()> {
    let schema = kanban_schema();
    execute_txn(
        pool,
        db_name,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "projects".to_string(),
                doc: project_doc(name, description),
            }],
        },
        &PrincipalCtx::bypass(),
        None,
    )
    .await?;
    Ok(())
}

fn schema_line_json() -> String {
    format!(
        "{{\"kind\":\"schema\",\"schema\":{}}}\n",
        serde_json::to_string(&kanban_schema()).expect("serialize schema")
    )
}

fn doc_line(table: &str, id: &str, doc: serde_json::Map<String, serde_json::Value>) -> String {
    format!(
        "{{\"kind\":\"doc\",\"table\":\"{table}\",\"id\":\"{id}\",\"doc\":{},\"createdAt\":1735689600000,\"version\":1}}\n",
        serde_json::Value::Object(doc)
    )
}

async fn projects_row_count(pool: &sqlx::PgPool, db_name: &str) -> anyhow::Result<i64> {
    let schema = pg_schema(db_name);
    let (count,): (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT COUNT(*) FROM \"{schema}\".\"t_projects\""
    )))
    .fetch_one(pool)
    .await?;
    Ok(count)
}

fn pg_schema(db_name: &str) -> String {
    format!("db_{db_name}")
}

/// Fills a bare target database by name through the live HTTP import handler.
async fn create_bare_target(
    state: &std::sync::Arc<rtdb_server::AppState>,
) -> crate::common::TestDb {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name)
        .await
        .expect("create bare target");
    wrap_test_db(name)
}

// ---------------------------------------------------------------------------
// In-process core: streaming export is byte-identical to buffered export.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn export_stream_matches_buffered_export_byte_for_byte() -> anyhow::Result<()> {
    let state = test_state().await;
    let source = fresh_db(&state).await;
    insert_project(&state.pool, &source, "alpha", "first").await?;
    insert_project(&state.pool, &source, "beta", "second").await?;
    insert_project(&state.pool, &source, "gamma", "third").await?;
    let schema = state.schemas.get(&state.pool, &source).await?;

    let buffered = snapshot::export_database(&state.pool, &source, &schema).await?;

    let streamed =
        snapshot::export_stream(state.pool.clone(), source.to_string(), (*schema).clone())?;
    let mut streamed_bytes: Vec<u8> = Vec::new();
    futures_util::pin_mut!(streamed);
    while let Some(item) = streamed.next().await {
        streamed_bytes.extend_from_slice(item?.as_ref());
    }

    assert_eq!(
        std::str::from_utf8(&streamed_bytes)?,
        buffered,
        "streamed export must be byte-identical to the buffered export"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// In-process core: chunked line decoding + rollback on malformed lines.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn import_lines_rolls_back_malformed_line_and_preserves_id_fields() -> anyhow::Result<()> {
    let state = test_state().await;
    let target = fresh_db(&state).await;

    // Split the snapshot across odd chunk boundaries so the reader must
    // reassemble lines spanning multiple transport chunks; include leading
    // blank lines (the buffered path skips them), interior blank lines, and
    // a trailing malformed doc line.
    let mut jsonl = String::from("\n\n");
    jsonl.push_str(&schema_line_json());
    jsonl.push_str(&doc_line("projects", "keep-1", project_doc("one", "d1")));
    jsonl.push_str("\n\n");
    jsonl.push_str(&doc_line("projects", "keep-2", project_doc("two", "d2")));
    jsonl.push_str("{\"kind\":\"doc\",\"table\":\"projects\",broken\n");
    jsonl.push_str(&doc_line(
        "projects",
        "never-imported",
        project_doc("three", "d3"),
    ));

    let mut reader = snapshot::jsonl_lines(stream::iter(
        jsonl
            .as_bytes()
            .chunks(7)
            .map(Ok::<_, std::convert::Infallible>),
    ));
    let err = snapshot::import_lines(&state.pool, &target, &mut reader)
        .await
        .expect_err("malformed doc line must abort the import");
    assert_eq!(err.status(), reqwest::StatusCode::BAD_REQUEST);

    // Every doc insert ran inside one transaction: the malformed line rolls
    // ALL of them back, even though the schema push already committed.
    assert_eq!(
        projects_row_count(&state.pool, &target).await?,
        0,
        "no snapshot document may survive a malformed-line abort"
    );

    // The rolled-back import left no change-feed stamps either.
    let head = change_log::head(&state.pool, &target).await?;
    assert_eq!(head.seq, 0, "rollback must not leave change-feed stamps");

    // The same target still accepts a clean snapshot afterwards.
    let mut clean = schema_line_json();
    clean.push_str(&doc_line("projects", "clean-1", project_doc("one", "d1")));
    clean.push_str(&doc_line("projects", "clean-2", project_doc("two", "d2")));
    let mut reader = snapshot::jsonl_lines(stream::iter(
        clean
            .as_bytes()
            .chunks(5)
            .map(Ok::<_, std::convert::Infallible>),
    ));
    let applied = snapshot::import_lines(&state.pool, &target, &mut reader).await?;
    assert_eq!(applied.tables.len(), kanban_schema().tables.len());

    assert_eq!(projects_row_count(&state.pool, &target).await?, 2);
    // id/createdAt/version are preserved verbatim through the replay.
    let schema = pg_schema(&target);
    let rows: Vec<(String, i64, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT \"id\", \"created_at\", \"version\" FROM \"{schema}\".\"t_projects\" ORDER BY \"id\""
    )))
    .fetch_all(&state.pool)
    .await?;
    assert_eq!(
        rows,
        vec![
            ("clean-1".to_string(), 1_735_689_600_000, 1),
            ("clean-2".to_string(), 1_735_689_600_000, 1),
        ]
    );
    let head = change_log::head(&state.pool, &target).await?;
    assert_eq!(head.seq, 2, "restored rows are observable on the feed");

    Ok(())
}

#[tokio::test]
async fn import_lines_rejects_line_over_the_bounded_cap() -> anyhow::Result<()> {
    let state = test_state().await;
    let target = fresh_db(&state).await;

    let mut jsonl = schema_line_json();
    let mut oversized = project_doc("big", "");
    oversized.insert(
        "description".to_string(),
        serde_json::Value::String("x".repeat(MAX_SNAPSHOT_LINE_BYTES + 64)),
    );
    jsonl.push_str(&doc_line("projects", "big-doc", oversized));

    let mut reader = snapshot::jsonl_lines(stream::iter(
        jsonl
            .as_bytes()
            .chunks(64 * 1024)
            .map(Ok::<_, std::convert::Infallible>),
    ));
    let err = snapshot::import_lines(&state.pool, &target, &mut reader)
        .await
        .expect_err("over-cap line must abort the import");
    assert_eq!(err.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(projects_row_count(&state.pool, &target).await?, 0);
    Ok(())
}

// ---------------------------------------------------------------------------
// HTTP surface: byte-identical streaming roundtrip, truncation rollback.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn http_export_import_roundtrip_is_byte_identical() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let source = fresh_db(&state).await;
    insert_project(&state.pool, &source, "alpha", "first").await?;
    insert_project(&state.pool, &source, "beta", "second").await?;
    insert_project(&state.pool, &source, "gamma", "third").await?;

    let resp = crate::common::admin_get(addr, &format!("/admin/export-db?db={source}")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers()["content-type"],
        "application/x-ndjson",
        "wire-compatible export content type"
    );
    let exported = resp.text().await?;

    let target = create_bare_target(&state).await;
    let resp = crate::common::admin_post_raw(
        addr,
        &format!("/admin/import-db?db={target}"),
        exported.clone(),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["ok"], true);

    let resp = crate::common::admin_get(addr, &format!("/admin/export-db?db={target}")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let reexported = resp.text().await?;
    assert_eq!(
        exported, reexported,
        "export -> import -> export must round-trip byte-identically"
    );
    assert_eq!(projects_row_count(&state.pool, &target).await?, 3);

    let head = change_log::head(&state.pool, &target).await?;
    assert_eq!(head.seq, 3, "import stamps every restored row");
    Ok(())
}

/// Raw HTTP/1.1 POST with a `Content-Length` longer than the bytes actually
/// written, then a clean half-close: hyper must report the incomplete body to
/// the handler and the import transaction must roll back. The observable
/// contract is database state, not the (possibly undeliverable) error response.
#[tokio::test]
async fn http_import_truncated_body_rolls_back() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let target = create_bare_target(&state).await;

    let mut jsonl = schema_line_json();
    for i in 0..3 {
        jsonl.push_str(&doc_line(
            "projects",
            &format!("trunc-{i}"),
            project_doc(&format!("doc{i}"), "payload"),
        ));
    }

    let mut stream = tokio::net::TcpStream::connect(addr).await?;
    let declared = jsonl.len() as u64 + 500;
    let request = format!(
        "POST /admin/import-db?db={target} HTTP/1.1\r\n\
         Host: {addr}\r\n\
         Authorization: Bearer test-admin-key\r\n\
         Content-Type: application/x-ndjson\r\n\
         Content-Length: {declared}\r\n\
         Connection: close\r\n\
         \r\n"
    );
    stream.write_all(request.as_bytes()).await?;
    stream.write_all(jsonl.as_bytes()).await?;
    stream.shutdown().await?;

    // Drain whatever the server sends back (it may close before responding —
    // either outcome is acceptable; the DB state below is the assertion).
    let mut sink = Vec::new();
    let _ = stream.read_to_end(&mut sink).await;

    tokio::time::sleep(Duration::from_millis(1_000)).await;
    assert_eq!(
        projects_row_count(&state.pool, &target).await?,
        0,
        "a transport-truncated import must commit nothing"
    );
    let head = change_log::head(&state.pool, &target).await?;
    assert_eq!(head.seq, 0);
    Ok(())
}

#[tokio::test]
async fn http_import_rejects_oversized_line_as_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let target = create_bare_target(&state).await;

    let mut jsonl = schema_line_json();
    let mut oversized = project_doc("big", "");
    oversized.insert(
        "description".to_string(),
        serde_json::Value::String("y".repeat(MAX_SNAPSHOT_LINE_BYTES + 16)),
    );
    jsonl.push_str(&doc_line("projects", "big-doc", oversized));

    let resp =
        crate::common::admin_post_raw(addr, &format!("/admin/import-db?db={target}"), jsonl).await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], "BAD_REQUEST");
    assert_eq!(projects_row_count(&state.pool, &target).await?, 0);
    Ok(())
}
