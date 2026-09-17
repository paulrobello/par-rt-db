//! Data plane and query introspection: `query`, `watch`, `mutate` (machine
//! token) plus the admin-side `explain` and `slow-queries`.

use anyhow::{Context, Result};
use par_rt_db_client::{
    ConnectionState, ErrorCode, Query, RtDbClient, RtDbError, Snapshot, Transaction,
};

use crate::args::Cli;
use crate::output::map_err;

use super::{admin_client, data_client, read_json_arg, require_admin, require_db, require_token};

pub(crate) async fn run_query(cli: &Cli, query: &str) -> Result<()> {
    let db = require_db(cli)?;
    let token = require_token(cli)?;
    let json = read_json_arg(query)?;
    let q: Query = serde_json::from_str(&json).context("parsing Query JSON")?;
    let c = data_client(cli, &db, &token);
    let result: serde_json::Value = c.run(q).await.map_err(map_err)?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

/// Map a connection state to the terminal failure it represents, if any.
///
/// `Idle` after a `connect()` is the auth rejection: the ws driver parks there
/// on `SessionOutcome::AuthFailed` and never retries, and it does not error the
/// subscription — so without this the tail would print nothing and hang
/// forever on a bad token. `Connecting` / `Reconnecting` are transient by
/// design and must keep the tail alive across a blip.
fn watch_terminal_error(state: ConnectionState) -> Option<RtDbError> {
    match state {
        ConnectionState::Idle => Some(RtDbError::new(
            ErrorCode::Unauthorized,
            "authentication failed — check --token (or RTDB_TOKEN) and --db",
        )),
        ConnectionState::Closed => Some(RtDbError::internal("connection closed")),
        ConnectionState::Connecting
        | ConnectionState::Connected
        | ConnectionState::Reconnecting => None,
    }
}

/// Tail a live query: subscribe over the reactive ws client and print the
/// initial result plus every subsequent `queryUpdate` until Ctrl-C.
///
/// Output is NDJSON — one compact JSON line per result — so the stream pipes
/// into `jq`/`while read`; `run_query` pretty-prints its single result instead.
/// The progress note goes to stderr so stdout stays pure NDJSON.
pub(crate) async fn run_watch(cli: &Cli, query: &str) -> Result<()> {
    let db = require_db(cli)?;
    let token = require_token(cli)?;
    let json = read_json_arg(query)?;
    let q: Query = serde_json::from_str(&json).context("parsing Query JSON")?;

    let client = RtDbClient::new(&cli.url, &db, move || {
        let token = token.clone();
        async move { Some(token) }
    });
    // Order is load-bearing: `connect` publishes `Connecting` synchronously,
    // so taking the receiver after it marks that value seen and the first
    // observed transition is a real one. Taken before, the receiver would
    // start on the default `Idle` and report a spurious auth failure.
    client.connect();
    let mut status = client.status_receiver();
    let mut sub = client.subscribe(q);
    eprintln!("watching {db} — Ctrl-C to stop");

    // One signal listener for the whole loop: re-creating the future each
    // iteration would drop and re-register the handler, and a SIGINT landing
    // in that window would be missed.
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    loop {
        tokio::select! {
            signal = &mut ctrl_c => {
                signal.context("waiting for Ctrl-C")?;
                client.close();
                return Ok(());
            }
            changed = sub.changed() => {
                changed.map_err(map_err)?;
                match sub.snapshot() {
                    Snapshot::Value(v) => println!("{}", serde_json::to_string(&v)?),
                    Snapshot::Error(e) => return Err(map_err(e)),
                    // The receiver starts at Pending and only ever leaves it.
                    Snapshot::Pending => {}
                }
            }
            changed = status.changed() => {
                if changed.is_err() {
                    return Err(map_err(RtDbError::internal("client driver stopped")));
                }
                if let Some(e) = watch_terminal_error(client.status().state) {
                    return Err(map_err(e));
                }
            }
        }
    }
}

pub(crate) async fn run_mutate(cli: &Cli, txn: &str) -> Result<()> {
    let db = require_db(cli)?;
    let token = require_token(cli)?;
    let json = read_json_arg(txn)?;
    let t: Transaction = serde_json::from_str(&json).context("parsing Transaction JSON")?;
    let c = data_client(cli, &db, &token);
    let results = c.mutate(&t, None).await.map_err(map_err)?;
    println!("{}", serde_json::to_string_pretty(&results)?);
    Ok(())
}

pub(crate) async fn run_explain(cli: &Cli, query: &str) -> Result<()> {
    let db = require_db(cli)?;
    require_admin(cli)?;
    let json = read_json_arg(query)?;
    let q: Query = serde_json::from_str(&json).context("parsing Query JSON")?;
    let c = admin_client(cli)?;
    let result = c.explain_query(&db, &q).await.map_err(map_err)?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

pub(crate) async fn run_slow_queries(
    cli: &Cli,
    db: &Option<String>,
    limit: Option<u32>,
) -> Result<()> {
    require_admin(cli)?;
    let c = admin_client(cli)?;
    let result = c
        .get_slow_queries(db.as_deref(), limit)
        .await
        .map_err(map_err)?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
