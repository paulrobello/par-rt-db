//! Opt-in live-server integration tests for the `rtdb` binary. Skipped by
//! default (`#[ignore]`); run with `--ignored` after pointing the env vars at
//! a running server (mirrors `rust-client/tests/http_integration.rs`):
//!   RTDB_TEST_SERVER_URL=http://127.0.0.1:8300 \
//!   RTDB_TEST_ADMIN_KEY=dev-admin-key \
//!   cargo test --manifest-path cli/Cargo.toml --test live -- --ignored

use assert_cmd::Command;
use predicates::str::contains;
use std::io::{BufRead, BufReader, Write};
use std::sync::mpsc;
use std::time::Duration;

/// Every read of the tail is bounded: a blocking `lines()` on a child's stdout
/// has no timeout of its own, so a subscription that never delivers would hang
/// the harness forever instead of failing.
const TAIL_TIMEOUT: Duration = Duration::from_secs(20);

/// Read the opt-in env vars. `None` unless both `RTDB_TEST_SERVER_URL` and
/// `RTDB_TEST_ADMIN_KEY` are set — tests call this to guard early.
fn env() -> Option<(String, String)> {
    let url = std::env::var("RTDB_TEST_SERVER_URL").ok()?;
    let admin = std::env::var("RTDB_TEST_ADMIN_KEY").ok()?;
    Some((url, admin))
}

// Minimal unique suffix without pulling `uuid` into the harness (mirrors
// `rust-client/tests/common::uuid_v7`).
fn unique_suffix() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{ms:x}{n:x}")
}

fn rtdb(url: &str) -> Command {
    let mut cmd = Command::cargo_bin("rtdb").expect("rtdb binary built");
    cmd.arg("--url").arg(url);
    cmd
}

/// `db create` / `db list` / `push-schema` / `mint-token` / `query` /
/// `mutate`, end to end against a live server. There is no `db delete`
/// subcommand on `rtdb` (see `cli/src/args.rs`'s `Command` enum — only
/// `list-dbs`/`create-db`/`clone-db` exist), so the fresh `t<suffix>`
/// database this test creates is left in place, same as the rust-client
/// live-server tests (`rust-client/tests/common::setup`) do.
#[test]
#[ignore = "set RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY and run with --ignored"]
fn cli_round_trip() {
    let Some((url, admin_key)) = env() else {
        return;
    };
    let db = format!("t{}", unique_suffix());

    // db create
    rtdb(&url)
        .arg("--admin-key")
        .arg(&admin_key)
        .arg("create-db")
        .arg(&db)
        .assert()
        .success()
        .stderr(contains(format!("created database {db}")));

    // db list
    rtdb(&url)
        .arg("--admin-key")
        .arg(&admin_key)
        .arg("list-dbs")
        .assert()
        .success()
        .stdout(contains(db.clone()));

    // push-schema. `_id` is reserved (server-assigned) and must not appear
    // as a declared field.
    let schema_path =
        std::env::temp_dir().join(format!("rtdb-cli-live-schema-{}.json", unique_suffix()));
    {
        let mut f = std::fs::File::create(&schema_path).expect("create temp schema file");
        write!(
            f,
            r#"{{"tables":{{"items":{{"fields":{{"name":{{"type":"string"}},"n":{{"type":"number"}}}},"indexes":[{{"name":"by_n","fields":["n"]}}]}}}}}}"#
        )
        .unwrap();
    }
    rtdb(&url)
        .arg("--admin-key")
        .arg(&admin_key)
        .arg("--db")
        .arg(&db)
        .arg("push-schema")
        .arg(&schema_path)
        .assert()
        .success()
        .stderr(contains(format!("pushed schema to {db}")));

    // mint-token — parse the printed `{"tokenId":..,"token":..}` for query/mutate.
    let minted = rtdb(&url)
        .arg("--admin-key")
        .arg(&admin_key)
        .arg("mint-token")
        .arg(&db)
        .arg("cli-live-test")
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let minted: serde_json::Value = serde_json::from_slice(&minted).unwrap();
    let token = minted["token"]
        .as_str()
        .expect("token in mint-token output");

    // mutate — insert one doc.
    rtdb(&url)
        .arg("--db")
        .arg(&db)
        .arg("--token")
        .arg(token)
        .arg("mutate")
        .arg(r#"{"steps":[{"op":"insert","table":"items","doc":{"name":"a","n":1}}]}"#)
        .assert()
        .success()
        // StepResult is deliberately #[serde(untagged)] (see
        // rust-client/src/mutation.rs) — an insert result is a bare `{"id": ..}`,
        // never an op-tagged shape.
        .stdout(contains("\"id\""));

    // query — scan it back via the by_n index.
    rtdb(&url)
        .arg("--db")
        .arg(&db)
        .arg("--token")
        .arg(token)
        .arg("query")
        .arg(r#"{"table":"items","index":"by_n","take":10}"#)
        .assert()
        .success()
        .stdout(contains("\"name\": \"a\""));
}

/// Provision a fresh database with the `items` table and return
/// `(db, machine token)`. Mirrors the first half of [`cli_round_trip`]; kept
/// separate so that test stays byte-identical.
fn provision(url: &str, admin_key: &str) -> (String, String) {
    let db = format!("t{}", unique_suffix());
    rtdb(url)
        .arg("--admin-key")
        .arg(admin_key)
        .arg("create-db")
        .arg(&db)
        .assert()
        .success();

    let schema_path =
        std::env::temp_dir().join(format!("rtdb-cli-live-schema-{}.json", unique_suffix()));
    {
        let mut f = std::fs::File::create(&schema_path).expect("create temp schema file");
        write!(
            f,
            r#"{{"tables":{{"items":{{"fields":{{"name":{{"type":"string"}},"n":{{"type":"number"}}}},"indexes":[{{"name":"by_n","fields":["n"]}}]}}}}}}"#
        )
        .unwrap();
    }
    rtdb(url)
        .arg("--admin-key")
        .arg(admin_key)
        .arg("--db")
        .arg(&db)
        .arg("push-schema")
        .arg(&schema_path)
        .assert()
        .success();
    std::fs::remove_file(&schema_path).ok();

    let minted = rtdb(url)
        .arg("--admin-key")
        .arg(admin_key)
        .arg("mint-token")
        .arg(&db)
        .arg("cli-watch-test")
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let minted: serde_json::Value = serde_json::from_slice(&minted).unwrap();
    let token = minted["token"]
        .as_str()
        .expect("token in mint-token output")
        .to_string();
    (db, token)
}

/// Insert one `items` row as a second client, the way another process would.
fn insert_row(url: &str, db: &str, token: &str, name: &str, n: u32) {
    rtdb(url)
        .arg("--db")
        .arg(db)
        .arg("--token")
        .arg(token)
        .arg("mutate")
        .arg(format!(
            r#"{{"steps":[{{"op":"insert","table":"items","doc":{{"name":"{name}","n":{n}}}}}]}}"#
        ))
        .assert()
        .success();
}

/// `rtdb watch` prints the initial result, then a fresh line for every
/// subsequent `queryUpdate` a *second* client causes, and exits 0 on Ctrl-C.
///
/// Output is NDJSON (one compact JSON line per result), which is what makes
/// "initial" and "update" separable inside a stream — a pretty-printed block
/// could not be delimited without a parser.
#[test]
#[ignore = "set RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY and run with --ignored"]
fn cli_watch_tails_live_updates() {
    let Some((url, admin_key)) = env() else {
        return;
    };
    let (db, token) = provision(&url, &admin_key);

    // Seed before watching so the initial result is non-empty, and therefore
    // distinguishable from the live updates that follow.
    insert_row(&url, &db, &token, "alpha", 1);

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("rtdb"))
        .args([
            "--url",
            &url,
            "--db",
            &db,
            "--token",
            &token,
            "watch",
            r#"{"table":"items","index":"by_n","take":10}"#,
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn rtdb watch");

    let stdout = child.stdout.take().expect("piped stdout");
    let (tx, rx) = mpsc::channel::<String>();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if tx.send(line).is_err() {
                break;
            }
        }
    });

    // Collect everything first and only assert after the child is reaped: a
    // panic between spawn and kill would otherwise leak the process.
    let initial = rx.recv_timeout(TAIL_TIMEOUT);
    if initial.is_ok() {
        insert_row(&url, &db, &token, "bravo", 2);
    }
    let update = rx.recv_timeout(TAIL_TIMEOUT);

    // Ctrl-C is SIGINT. Exit 0 proves the handler ran and shut down cleanly;
    // had it never installed, the default disposition would report 130.
    #[cfg(unix)]
    let exit_code = {
        std::process::Command::new("kill")
            .args(["-INT", &child.id().to_string()])
            .status()
            .expect("send SIGINT");
        let mut waited = Duration::ZERO;
        let step = Duration::from_millis(100);
        loop {
            match child.try_wait() {
                Ok(Some(status)) => break status.code(),
                Ok(None) if waited < Duration::from_secs(10) => {
                    std::thread::sleep(step);
                    waited += step;
                }
                _ => break None,
            }
        }
    };
    let _ = child.kill();
    let _ = child.wait();

    let initial = initial.expect("initial result line within the timeout");
    assert!(
        initial.contains("alpha"),
        "initial result should carry the seeded row, got: {initial}"
    );
    let update = update.expect("live update line within the timeout");
    assert!(
        update.contains("bravo") && update.contains("alpha"),
        "live update should carry the second client's write, got: {update}"
    );
    // Each line is a self-contained compact JSON array — the NDJSON contract.
    for line in [&initial, &update] {
        let parsed: serde_json::Value =
            serde_json::from_str(line).expect("each line parses as standalone JSON");
        assert!(parsed.is_array(), "expected a result array, got: {line}");
    }

    #[cfg(unix)]
    assert_eq!(
        exit_code,
        Some(0),
        "Ctrl-C (SIGINT) should exit cleanly with 0"
    );
}

/// A rejected credential must surface the standard `{code, message}` envelope
/// and exit, not hang. The ws driver parks in `ConnectionState::Idle` on an
/// auth failure without erroring the subscription, so this is the regression
/// guard for the status-receiver arm in `run_watch`.
#[test]
#[ignore = "set RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY and run with --ignored"]
fn cli_watch_bad_token_prints_error_envelope() {
    let Some((url, admin_key)) = env() else {
        return;
    };
    let (db, _token) = provision(&url, &admin_key);

    let mut cmd = Command::cargo_bin("rtdb").expect("rtdb binary built");
    cmd.arg("--url")
        .arg(&url)
        .arg("--db")
        .arg(&db)
        .arg("--token")
        .arg("not-a-real-token")
        .arg("watch")
        .arg(r#"{"table":"items","take":10}"#)
        .timeout(TAIL_TIMEOUT)
        .assert()
        .failure()
        .stderr(contains("UNAUTHORIZED"));
}

/// A subscribe rejection (unknown table) surfaces the server's own envelope
/// verbatim rather than tailing an empty stream.
#[test]
#[ignore = "set RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY and run with --ignored"]
fn cli_watch_subscribe_failure_prints_error_envelope() {
    let Some((url, admin_key)) = env() else {
        return;
    };
    let (db, token) = provision(&url, &admin_key);

    let mut cmd = Command::cargo_bin("rtdb").expect("rtdb binary built");
    cmd.arg("--url")
        .arg(&url)
        .arg("--db")
        .arg(&db)
        .arg("--token")
        .arg(&token)
        .arg("watch")
        .arg(r#"{"table":"no_such_table","take":10}"#)
        .timeout(TAIL_TIMEOUT)
        .assert()
        .failure()
        .stderr(contains("NOT_FOUND"));
}

/// `rtdb ops watch` tails the admin op feed: every committed document op shows
/// up as one compact JSON line, `--db` scopes the feed to that database, and
/// Ctrl-C exits 0.
///
/// Mirrors `cli_watch_tails_live_updates`, one plane up — that one tails a
/// single query with a machine token, this one tails every write on the
/// instance with the admin key over `/admin/stream`.
#[test]
#[ignore = "set RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY and run with --ignored"]
fn cli_ops_watch_tails_live_op_events() {
    let Some((url, admin_key)) = env() else {
        return;
    };
    let (db, token) = provision(&url, &admin_key);
    // A second database whose writes must NOT appear: without it, a feed that
    // ignores `--db` would look identical to one that honors it.
    let (other_db, other_token) = provision(&url, &admin_key);

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("rtdb"))
        .args([
            "--url",
            &url,
            "--admin-key",
            &admin_key,
            "ops",
            "watch",
            "--db",
            &db,
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn rtdb ops watch");

    let stdout = child.stdout.take().expect("piped stdout");
    let (tx, rx) = mpsc::channel::<String>();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if tx.send(line).is_err() {
                break;
            }
        }
    });

    // The socket needs to be up before the write, or the op lands in the replay
    // ring instead of the live broadcast (still delivered, but then the test
    // would not be proving the live path).
    std::thread::sleep(Duration::from_secs(2));
    insert_row(&url, &other_db, &other_token, "filtered-out", 9);
    insert_row(&url, &db, &token, "alpha", 1);

    // Collect first, assert after reaping, so a panic cannot leak the child.
    let first = rx.recv_timeout(TAIL_TIMEOUT);

    #[cfg(unix)]
    let exit_code = {
        std::process::Command::new("kill")
            .args(["-INT", &child.id().to_string()])
            .status()
            .expect("send SIGINT");
        let mut waited = Duration::ZERO;
        let step = Duration::from_millis(100);
        loop {
            match child.try_wait() {
                Ok(Some(status)) => break status.code(),
                Ok(None) if waited < Duration::from_secs(10) => {
                    std::thread::sleep(step);
                    waited += step;
                }
                _ => break None,
            }
        }
    };
    let _ = child.kill();
    let _ = child.wait();

    // Everything the tail emitted before the SIGINT, including anything that
    // arrived while the child was shutting down.
    let mut lines = Vec::new();
    if let Ok(line) = first {
        lines.push(line);
    }
    while let Ok(line) = rx.recv_timeout(Duration::from_millis(200)) {
        lines.push(line);
    }
    assert!(
        !lines.is_empty(),
        "expected at least one op event line within the timeout"
    );

    for line in &lines {
        // NDJSON: each line stands alone, and carries the server's own OpEvent
        // shape (camelCase `docId`, not a re-spelled one).
        let parsed: serde_json::Value =
            serde_json::from_str(line).expect("each line parses as standalone JSON");
        assert!(
            parsed.get("docId").is_some(),
            "expected an OpEvent with docId, got: {line}"
        );
        // `--db` filters the feed: the other database's write must never show.
        assert_eq!(
            parsed.get("db").and_then(|v| v.as_str()),
            Some(db.as_str()),
            "--db should scope the feed to {db}, got: {line}"
        );
    }
    assert!(
        lines.iter().any(|l| l.contains(r#""kind":"insert""#)),
        "expected the insert to appear on the feed, got: {lines:?}"
    );

    #[cfg(unix)]
    assert_eq!(
        exit_code,
        Some(0),
        "Ctrl-C (SIGINT) should exit cleanly with 0"
    );
}

/// A rejected admin key must surface the `{code, message}` envelope and exit
/// non-zero. The upgrade is gated before WS negotiation, so this is a plain
/// 401 — the regression guard against retrying it forever, which from the
/// outside is indistinguishable from a hang.
#[test]
#[ignore = "set RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY and run with --ignored"]
fn cli_ops_watch_bad_admin_key_prints_error_envelope() {
    let Some((url, _admin_key)) = env() else {
        return;
    };

    let mut cmd = Command::cargo_bin("rtdb").expect("rtdb binary built");
    cmd.arg("--url")
        .arg(&url)
        .arg("--admin-key")
        .arg("not-a-real-admin-key")
        .arg("ops")
        .arg("watch")
        .timeout(TAIL_TIMEOUT)
        .assert()
        .failure()
        .stderr(contains("UNAUTHORIZED"));
}
