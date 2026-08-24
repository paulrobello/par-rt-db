//! Opt-in live-server integration tests for the `rtdb` binary. Skipped by
//! default (`#[ignore]`); run with `--ignored` after pointing the env vars at
//! a running server (mirrors `rust-client/tests/http_integration.rs`):
//!   RTDB_TEST_SERVER_URL=http://127.0.0.1:8300 \
//!   RTDB_TEST_ADMIN_KEY=dev-admin-key \
//!   cargo test --manifest-path cli/Cargo.toml --test live -- --ignored

use assert_cmd::Command;
use predicates::str::contains;
use std::io::Write;

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
        .stdout(contains("Insert"));

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
