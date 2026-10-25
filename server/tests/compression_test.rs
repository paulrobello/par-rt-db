//! HTTP gzip compression on the JSON API + admin surface (Accept-Encoding
//! negotiated). The layer sits on the merged admin/http_api routers BEFORE the
//! `/sync` merge, so the WS upgrade is structurally outside it. Proves:
//! - a >1 KiB `/api/query` response is `Content-Encoding: gzip` on the wire and
//!   decodes to the identical JSON,
//! - a small response stays uncompressed (`SizeAbove(1024)`),
//! - the streaming `/admin/export-db` JSONL round-trips byte-identical through
//!   gzip (tower-http's encoder is incremental over body chunks — no
//!   whole-body buffering added anywhere).

use std::net::SocketAddr;

use crate::common::{admin_post, fresh_db, spawn_app, test_state};
use serde_json::json;

/// A client that neither requests nor decodes gzip: it sees the raw wire
/// bytes, so tests can assert the `Content-Encoding` header and compressed
/// payload directly.
fn raw_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_gzip()
        .build()
        .expect("build no-gzip client")
}

async fn mint_token(addr: SocketAddr, db: &str) -> String {
    let resp = admin_post(
        addr,
        "/admin/mint-token",
        json!({"db": db, "name": "gzip-test"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("parse mint-token response");
    body["token"].as_str().expect("token").to_string()
}

/// Insert `count` work items with long titles so a query response clears the
/// 1 KiB SizeAbove threshold. Returns the titles in insertion order.
async fn seed_docs(addr: SocketAddr, db: &str, token: &str, count: usize) -> Vec<String> {
    let filler = "x".repeat(120);
    let titles: Vec<String> = (0..count)
        .map(|i| format!("item {i:04} {filler}"))
        .collect();
    let steps: Vec<serde_json::Value> = titles
        .iter()
        .map(|title| {
            json!({"op": "insert", "table": "workItems", "doc": {
                "projectId": "0".repeat(32),
                "title": title,
                "status": "backlog",
                "order": 1.0,
                "completedAt": null
            }})
        })
        .collect();
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/mutate"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({"db": db, "txn": {"steps": steps}}))
        .send()
        .await
        .expect("send mutate");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    titles
}

async fn query_all(addr: SocketAddr, db: &str, token: &str) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("http://{addr}/api/query"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({"db": db, "query": {"table": "workItems"}}))
        .send()
        .await
        .expect("send query")
}

// A request with `Accept-Encoding: gzip` to /api/query gets a gzipped wire
// body that decodes to the identical JSON a plain request returns.
#[tokio::test]
async fn query_response_gzips_and_decodes_to_identical_json() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;
    let token = mint_token(addr, &name).await;
    let titles = seed_docs(addr, &name, &token, 40).await;

    let raw = raw_client()
        .post(format!("http://{addr}/api/query"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept-Encoding", "gzip")
        .json(&json!({"db": &name, "query": {"table": "workItems"}}))
        .send()
        .await?;
    assert_eq!(raw.status(), reqwest::StatusCode::OK);
    assert_eq!(
        raw.headers()
            .get(reqwest::header::CONTENT_ENCODING)
            .and_then(|v| v.to_str().ok()),
        Some("gzip"),
        "large query response must be gzipped on the wire"
    );
    let wire_bytes = raw.bytes().await?;

    // The auto-decoding client transparently sends Accept-Encoding: gzip and
    // inflates the body; its JSON must equal what an uncompressed request sees.
    let decoded: serde_json::Value = query_all(addr, &name, &token).await.json().await?;
    let plain: serde_json::Value = raw_client()
        .post(format!("http://{addr}/api/query"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept-Encoding", "identity")
        .json(&json!({"db": &name, "query": {"table": "workItems"}}))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(decoded, plain);
    let plain_len = serde_json::to_vec(&plain)?.len();
    assert!(
        plain_len > 1024,
        "fixture must exceed the SizeAbove(1024) threshold; got {plain_len} bytes"
    );
    assert!(
        (wire_bytes.len() as usize) < plain_len,
        "gzipped body ({} bytes) must be smaller than the plain body ({plain_len} bytes)",
        wire_bytes.len()
    );
    let decoded_titles: Vec<&str> = decoded["result"]
        .as_array()
        .expect("result array")
        .iter()
        .map(|doc| doc["title"].as_str().expect("title"))
        .collect();
    let expected: Vec<&str> = titles.iter().map(String::as_str).collect();
    assert_eq!(decoded_titles, expected);

    Ok(())
}

// A response under the 1 KiB threshold stays uncompressed even when the client
// offers gzip.
#[tokio::test]
async fn small_query_response_stays_uncompressed() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;
    let token = mint_token(addr, &name).await;

    let raw = raw_client()
        .post(format!("http://{addr}/api/query"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept-Encoding", "gzip")
        .json(&json!({"db": &name, "query": {"table": "workItems"}}))
        .send()
        .await?;
    assert_eq!(raw.status(), reqwest::StatusCode::OK);
    assert!(
        raw.headers()
            .get(reqwest::header::CONTENT_ENCODING)
            .is_none(),
        "sub-threshold response must not carry Content-Encoding"
    );

    Ok(())
}

// The streaming /admin/export-db JSONL compresses on the wire and its gzip
// body decodes to exactly the uncompressed export. tower-http wraps the body
// stream with an incremental encoder, so nothing buffers the export whole.
#[tokio::test]
async fn export_db_gzip_round_trips_identical_jsonl() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;
    let token = mint_token(addr, &name).await;
    seed_docs(addr, &name, &token, 40).await;

    let path = format!("http://{addr}/admin/export-db?db={name}");
    let auth = reqwest::header::HeaderValue::from_static("Bearer test-admin-key");

    let gzipped = raw_client()
        .get(&path)
        .header("Authorization", auth.clone())
        .header("Accept-Encoding", "gzip")
        .send()
        .await?;
    assert_eq!(gzipped.status(), reqwest::StatusCode::OK);
    assert_eq!(
        gzipped
            .headers()
            .get(reqwest::header::CONTENT_ENCODING)
            .and_then(|v| v.to_str().ok()),
        Some("gzip"),
        "export-db response must be gzipped on the wire"
    );
    let wire_bytes = gzipped.bytes().await?;

    let plain = raw_client()
        .get(&path)
        .header("Authorization", auth.clone())
        .header("Accept-Encoding", "identity")
        .send()
        .await?
        .text()
        .await?;
    assert!(
        (plain.len() as u64) > 1024,
        "export must clear the threshold"
    );

    // The decoding client inflates the wire body; result must be byte-identical.
    let decoded = reqwest::Client::new()
        .get(&path)
        .header("Authorization", auth)
        .send()
        .await?
        .text()
        .await?;
    assert_eq!(decoded, plain, "gzip round-trip must preserve the JSONL");
    assert!(
        wire_bytes.len() < plain.len(),
        "gzipped export ({} bytes) must be smaller than the plain export ({} bytes)",
        wire_bytes.len(),
        plain.len()
    );
    let schema_lines = decoded.lines().filter(|l| l.contains("\"schema\"")).count();
    assert_eq!(schema_lines, 1, "export must contain the schema line");
    assert!(decoded.lines().count() > 1, "export must contain doc lines");

    Ok(())
}
