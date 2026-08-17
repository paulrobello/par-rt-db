//! Data plane and query introspection: `query`, `mutate` (machine token)
//! plus the admin-side `explain` and `slow-queries`.

use anyhow::{Context, Result};
use par_rt_db_client::{Query, Transaction};

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
