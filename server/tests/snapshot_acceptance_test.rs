//! Acceptance: the full 59,582,485-byte synthetic snapshot fixture, imported
//! and exported through the real HTTP endpoints in an isolated process, with
//! peak server-process RSS sampled throughout. A separate [[test]] target so
//! the process-wide RSS sampler measures only this test's own server (in-suite
//! concurrency would inflate the reading). Ignored by default — CI stays lean;
//! run with `cargo test --test snapshot_acceptance -- --ignored --nocapture`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

// Only a slice of the shared fixture module is used here; silence the rest.
#[allow(dead_code)]
mod common;

use std::collections::BTreeMap;
use std::process::id;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

use crate::common::{spawn_app, test_state, wrap_test_db};
use rtdb_server::db;
use rtdb_server::schema::SchemaDef;

fn kanban_schema() -> SchemaDef {
    serde_json::from_value(crate::common::kanban_schema_json()).expect("parse kanban schema")
}

fn schema_line_json() -> String {
    format!(
        "{{\"kind\":\"schema\",\"schema\":{}}}\n",
        serde_json::to_string(&kanban_schema()).expect("serialize schema")
    )
}

async fn create_bare_target(
    state: &std::sync::Arc<rtdb_server::AppState>,
) -> crate::common::TestDb {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name)
        .await
        .expect("create bare target");
    wrap_test_db(name)
}

async fn projects_row_count(pool: &sqlx::PgPool, db_name: &str) -> anyhow::Result<i64> {
    let schema = format!("db_{db_name}");
    let (count,): (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT COUNT(*) FROM \"{schema}\".\"t_projects\""
    )))
    .fetch_one(pool)
    .await?;
    Ok(count)
}

// ---------------------------------------------------------------------------
// Acceptance: the full 59,582,485-byte synthetic fixture, real isolated
// HTTP roundtrip, chunked on both legs, with peak server-process RSS sampled
// throughout. Ignored by default (CI stays lean); run with
// `cargo test --test snapshot_acceptance -- --ignored`.
// ---------------------------------------------------------------------------

/// Deterministic JSONL snapshot generator over the kanban schema. Doc field
/// order inside `doc` follows the export's lexicographic rendering
/// (serde_json Map = BTreeMap after the jsonb round trip) so the fixture
/// re-exports byte-identically; envelope
/// keys follow `SnapshotLine`'s serde declaration order. `total_bytes` pins
/// the exact fixture size by padding the final doc's description.
struct FixtureGen {
    schema_line: String,
    doc_count: usize,
    doc_len: usize,
    tail_pad: usize,
    total_bytes: usize,
    pos: usize,
}

/// Builds one fixture doc line with the doc's keys in serde_json BTreeMap
/// (lexicographic) order — what the export re-serializes after the jsonb round
/// trip is parsed back into `serde_json::Map` — and the envelope keys in
/// `SnapshotLine`'s serde declaration order. All values are strings/ints so
/// serde_json and jsonb cannot disagree on rendering.
fn fixture_doc_line(i: usize, pad: usize) -> String {
    format!(
        "{{\"kind\":\"doc\",\"table\":\"projects\",\"id\":\"doc{i:08}\",\
         \"doc\":{{\"description\":\"{pad_to}\",\"name\":\"proj{i:08}\",\"status\":\"active\",\
         \"tags\":[\"alpha\"],\"updatedAt\":1735689600000}},\
         \"createdAt\":1735689600000,\"version\":1}}\n",
        pad_to = "x".repeat(pad),
    )
}

impl FixtureGen {
    fn new(total_bytes: usize) -> Self {
        let schema_line = schema_line_json();
        let doc_len = fixture_doc_line(0, 3_700).len();
        let body = total_bytes - schema_line.len();
        let doc_count = body / doc_len;
        let tail_pad = body % doc_len;
        assert!(doc_count > 0, "fixture must contain at least one doc");
        Self {
            schema_line,
            doc_count,
            doc_len,
            tail_pad,
            total_bytes,
            pos: 0,
        }
    }

    fn doc_line_at(&self, i: usize) -> String {
        let pad = 3_700
            + if i + 1 == self.doc_count {
                self.tail_pad
            } else {
                0
            };
        fixture_doc_line(i, pad)
    }

    /// FNV-1a over the exact byte stream `take` yields, for fixture-vs-export
    /// comparison without buffering the fixture.
    fn fingerprint(&self) -> (u64, usize, usize) {
        let mut fixture = FixtureGen::new(self.total_bytes);
        let mut hash: u64 = 0xcbf29ce484222325;
        let mut bytes = 0usize;
        let mut lines = 0usize;
        while let Some(chunk) = fixture.take(usize::MAX) {
            for &b in &chunk {
                hash ^= b as u64;
                hash = hash.wrapping_mul(0x100000001b3);
                if b == b'\n' {
                    lines += 1;
                }
            }
            bytes += chunk.len();
        }
        (hash, bytes, lines)
    }

    fn take(&mut self, want: usize) -> Option<Vec<u8>> {
        if self.pos >= self.total_bytes {
            return None;
        }
        let want = want.min(self.total_bytes - self.pos);
        let mut out = Vec::with_capacity(want);
        while out.len() < want {
            let produced = if self.pos < self.schema_line.len() {
                let end = (self.pos + (want - out.len())).min(self.schema_line.len());
                out.extend_from_slice(&self.schema_line.as_bytes()[self.pos..end]);
                end - self.pos
            } else {
                let body_pos = self.pos - self.schema_line.len();
                let index = body_pos / self.doc_len;
                let offset = body_pos % self.doc_len;
                let line = self.doc_line_at(index);
                let end = (offset + (want - out.len())).min(line.len());
                out.extend_from_slice(&line.as_bytes()[offset..end]);
                end - offset
            };
            self.pos += produced;
            if produced == 0 {
                break;
            }
        }
        Some(out)
    }
}

/// Peak RSS (bytes) of the test process — the in-process server shares it —
/// sampled every 100 ms by a background task between `baseline` and drop.
struct RssPeak {
    peak_rx: tokio::sync::oneshot::Receiver<usize>,
}

impl RssPeak {
    fn start(baseline_bytes: u64) -> Self {
        let pid = id();
        let (peak_tx, peak_rx) = tokio::sync::oneshot::channel::<usize>();
        let _task = tokio::spawn(async move {
            let mut peak = 0u64;
            for _ in 0..600 {
                let output = tokio::process::Command::new("ps")
                    .args(["-o", "rss=", "-p", &pid.to_string()])
                    .output()
                    .await;
                if let Ok(text) = output.map(|o| String::from_utf8_lossy(&o.stdout).into_owned())
                    && let Ok(kib) = text.trim().parse::<u64>()
                {
                    peak = peak.max(kib * 1024);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let _ = peak_tx.send(peak.saturating_sub(baseline_bytes) as usize);
        });
        Self { peak_rx }
    }
}

impl Drop for RssPeak {
    fn drop(&mut self) {
        // oneshot sender dies with the aborted task; the receiver returns Err.
    }
}

/// Streams `fixture` to `POST /admin/import-db?db=<db>` as a chunked HTTP/1.1 body
/// (no Content-Length: the transport framing the truncation contract relies
/// on), folding the request fingerprint on the way out. Returns the response
/// status and body.
async fn chunked_import(
    addr: std::net::SocketAddr,
    db_name: &str,
    fixture: &mut FixtureGen,
) -> anyhow::Result<(u16, Vec<u8>)> {
    let mut stream = tokio::net::TcpStream::connect(addr).await?;
    let request = format!(
        "POST /admin/import-db?db={db_name} HTTP/1.1\r\n\
         Host: {addr}\r\n\
         Authorization: Bearer test-admin-key\r\n\
         Content-Type: application/x-ndjson\r\n\
         Transfer-Encoding: chunked\r\n\
         Connection: close\r\n\
         \r\n"
    );
    stream.write_all(request.as_bytes()).await?;
    while let Some(chunk) = fixture.take(256 * 1024) {
        stream
            .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
            .await?;
        stream.write_all(&chunk).await?;
        stream.write_all(b"\r\n").await?;
    }
    stream.write_all(b"0\r\n\r\n").await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    let (status, body) = split_http_response(&response).map_err(|err| {
        anyhow::anyhow!(
            "{err}; raw response head: {:?}",
            String::from_utf8_lossy(&response[..response.len().min(600)])
        )
    })?;
    Ok((status, body))
}

/// GETs an export and folds the (dechunked) body fingerprint without
/// buffering it. Returns (status, fnv, bytes, newline count).
async fn chunked_export(
    addr: std::net::SocketAddr,
    db_name: &str,
) -> anyhow::Result<(u16, u64, usize, usize)> {
    let mut stream = BufReader::new(tokio::net::TcpStream::connect(addr).await?);
    let request = format!(
        "GET /admin/export-db?db={db_name} HTTP/1.1\r\n\
         Host: {addr}\r\n\
         Authorization: Bearer test-admin-key\r\n\
         Connection: close\r\n\
         \r\n"
    );
    stream.get_mut().write_all(request.as_bytes()).await?;

    let mut status = String::new();
    stream.read_line(&mut status).await?;
    let code: u16 = status
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| anyhow::anyhow!("bad status line: {status}"))?;
    let mut headers = BTreeMap::new();
    loop {
        let mut line = String::new();
        stream.read_line(&mut line).await?;
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.to_ascii_lowercase(), value.trim().to_string());
        }
    }

    let chunked = headers
        .get("transfer-encoding")
        .is_some_and(|v| v.eq_ignore_ascii_case("chunked"));
    let mut hash: u64 = 0xcbf29ce484222325;
    let mut bytes = 0usize;
    let mut lines = 0usize;
    if chunked {
        loop {
            let mut size_line = String::new();
            if stream.read_line(&mut size_line).await? == 0 {
                break;
            }
            let size = usize::from_str_radix(size_line.trim(), 16)?;
            if size == 0 {
                break;
            }
            let mut chunk = vec![0u8; size];
            stream.read_exact(&mut chunk).await?;
            for &b in &chunk {
                hash ^= b as u64;
                hash = hash.wrapping_mul(0x100000001b3);
                if b == b'\n' {
                    lines += 1;
                }
            }
            bytes += size;
            let mut crlf = [0u8; 2];
            stream.read_exact(&mut crlf).await?;
        }
    } else {
        let mut chunk = vec![0u8; 64 * 1024];
        loop {
            let read = stream.read(&mut chunk).await?;
            if read == 0 {
                break;
            }
            for &b in &chunk[..read] {
                hash ^= b as u64;
                hash = hash.wrapping_mul(0x100000001b3);
                if b == b'\n' {
                    lines += 1;
                }
            }
            bytes += read;
        }
    }
    Ok((code, hash, bytes, lines))
}

fn split_http_response(raw: &[u8]) -> anyhow::Result<(u16, Vec<u8>)> {
    let header_end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| anyhow::anyhow!("no header terminator in response"))?;
    let head = std::str::from_utf8(&raw[..header_end])?;
    let code: u16 = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| anyhow::anyhow!("bad status line: {head}"))?;
    // Body here is a small JSON envelope; dechunk if framed as chunked.
    let body = &raw[header_end + 4..];
    let body = if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        let mut out = Vec::new();
        let mut pos = 0usize;
        loop {
            let line_end = body[pos..]
                .windows(2)
                .position(|w| w == b"\r\n")
                .ok_or_else(|| anyhow::anyhow!("bad chunk header"))?
                + pos;
            let size =
                usize::from_str_radix(std::str::from_utf8(&body[pos..line_end])?.trim(), 16)?;
            let data_start = line_end + 2;
            if size == 0 {
                break;
            }
            out.extend_from_slice(&body[data_start..data_start + size]);
            pos = data_start + size + 2;
        }
        out
    } else {
        body.to_vec()
    };
    Ok((code, body))
}

#[tokio::test]
#[ignore = "acceptance: full 59,582,485-byte fixture roundtrip; run with --ignored (CI stays lean)"]
async fn acceptance_full_fixture_roundtrip_bounded_memory() -> anyhow::Result<()> {
    const TARGET_BYTES: usize = 59_582_485;
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;

    let fixture = FixtureGen::new(TARGET_BYTES);
    let (want_hash, want_bytes, want_lines) = fixture.fingerprint();
    assert_eq!(
        want_bytes, TARGET_BYTES,
        "generator must pin the exact size"
    );

    let baseline = {
        let output = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &id().to_string()])
            .output()?;
        String::from_utf8(output.stdout)?.trim().parse::<u64>()? * 1024
    };
    let mut sampler = RssPeak::start(baseline);

    // Leg 1: import the whole fixture into a bare db over a chunked body.
    let target_a = create_bare_target(&state).await;
    let mut fixture = FixtureGen::new(TARGET_BYTES);
    let (status, body) = chunked_import(addr, &target_a, &mut fixture).await?;
    assert_eq!(
        status,
        200,
        "import failed: {}",
        String::from_utf8_lossy(&body)
    );

    // Leg 2: stream the export back and compare fingerprints.
    let (status, hash_a, bytes_a, lines_a) = chunked_export(addr, &target_a).await?;
    assert_eq!(status, 200);
    assert_eq!(bytes_a, want_bytes, "export byte count");
    assert_eq!(lines_a, want_lines, "export line count");
    assert_eq!(
        hash_a, want_hash,
        "export must be byte-identical to the fixture"
    );

    assert_eq!(
        projects_row_count(&state.pool, &target_a).await? as usize,
        want_lines - 1,
        "every doc line stored"
    );

    // Leg 3: isolated second import + export must match the first exactly.
    let target_b = create_bare_target(&state).await;
    let mut fixture = FixtureGen::new(TARGET_BYTES);
    let (status, body) = chunked_import(addr, &target_b, &mut fixture).await?;
    assert_eq!(
        status,
        200,
        "second import failed: {}",
        String::from_utf8_lossy(&body)
    );
    let (status, hash_b, bytes_b, _lines_b) = chunked_export(addr, &target_b).await?;
    assert_eq!(status, 200);
    assert_eq!(
        hash_b, hash_a,
        "independent roundtrips must agree byte-for-byte"
    );
    assert_eq!(bytes_b, bytes_a);

    let peak_delta =
        match std::mem::replace(&mut sampler.peak_rx, tokio::sync::oneshot::channel().1).await {
            Ok(v) => v,
            Err(_) => anyhow::bail!("RSS sampler task died before reporting peak"),
        };
    println!(
        "SYNTHETIC byte-matched fixture (generated in-test, NOT the retained co-op file): \
         {TARGET_BYTES} bytes, {want_lines} lines, \
         peak RSS delta during roundtrip: {peak_delta} bytes ({} MiB)",
        peak_delta / (1024 * 1024)
    );
    // Bounded memory: peak RSS growth must stay BELOW the snapshot size — the
    // server must never hold the whole 59 MB body. The budget tracks the body
    // size (not a fixed ceiling) because the sampler reads the whole test
    // process, so concurrent in-suite tests legitimately add working set; the
    // isolated run (`--ignored` alone) is the clean evidence run.
    assert!(
        (peak_delta as usize) < TARGET_BYTES,
        "peak RSS delta {peak_delta} bytes: the server buffered the whole snapshot"
    );
    Ok(())
}
