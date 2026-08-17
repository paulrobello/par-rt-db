//! Schema lifecycle: `push-schema` (declarative SchemaDef push) and
//! `migrate` (migration directives, with `--dry-run` preview).

use anyhow::{Context, Result};
use par_rt_db_client::{MigrateRequestOwned, SchemaDef};
use std::path::PathBuf;

use crate::args::Cli;
use crate::output::map_err;

use super::{admin_client, require_db};

pub(crate) async fn run_push_schema(cli: &Cli, file: &PathBuf) -> Result<()> {
    let db = require_db(cli)?;
    let json =
        std::fs::read_to_string(file).with_context(|| format!("reading {}", file.display()))?;
    let schema: SchemaDef = serde_json::from_str(&json).context("parsing SchemaDef JSON")?;
    let c = admin_client(cli)?;
    c.push_schema(&db, &schema).await.map_err(map_err)?;
    eprintln!("pushed schema to {db}");
    Ok(())
}

pub(crate) async fn run_migrate(cli: &Cli, file: &PathBuf, dry_run_flag: bool) -> Result<()> {
    let db = require_db(cli)?;
    let json =
        std::fs::read_to_string(file).with_context(|| format!("reading {}", file.display()))?;
    let req: MigrateRequestOwned = serde_json::from_str(&json).context("parsing migrate JSON")?;
    // CLI flag forces dry-run on; a `dryRun: true` in the file is also
    // honored so a checked-in preview request can't be silently applied.
    let dry_run = dry_run_flag || req.dry_run;
    let c = admin_client(cli)?;
    let result = c
        .migrate_schema(&db, &req.directives, dry_run)
        .await
        .map_err(map_err)?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    if !result.applied {
        eprintln!("dry-run only — nothing applied (re-run without --dry-run to apply)");
    }
    Ok(())
}
