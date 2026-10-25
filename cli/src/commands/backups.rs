//! Instance backup lifecycle: `backup create|list|download|delete`, `restore`.

use anyhow::{Context, Result, anyhow};
use std::io::Write;

use crate::args::{BackupCommand, Cli};
use crate::output::map_err;

use super::admin_client;

pub(crate) async fn run_backup(cli: &Cli, command: &BackupCommand) -> Result<()> {
    if let BackupCommand::Delete { name, confirm } = command {
        require_confirmation(name, confirm)?;
    }
    let c = admin_client(cli)?;
    match command {
        BackupCommand::Create => {
            c.backup_now().await.map_err(map_err)?;
            eprintln!("backup started; poll `backup list` for completion");
        }
        BackupCommand::List => {
            let result = c.list_backups().await.map_err(map_err)?;
            for backup in &result.backups {
                println!(
                    "{}\t{}\t{}",
                    backup.name, backup.size_bytes, backup.created_ms
                );
            }
            eprintln!(
                "{} backup(s), running={}",
                result.backups.len(),
                result.running
            );
        }
        BackupCommand::Download { name, file } => {
            let bytes = c.download_backup(name).await.map_err(map_err)?;
            let mut output = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(file)
                .with_context(|| format!("creating {}", file.display()))?;
            output
                .write_all(&bytes)
                .with_context(|| format!("writing {}", file.display()))?;
            eprintln!(
                "downloaded {name} to {} ({} bytes)",
                file.display(),
                bytes.len()
            );
        }
        BackupCommand::Delete { name, .. } => {
            c.delete_backup(name).await.map_err(map_err)?;
            eprintln!("deleted backup {name}");
        }
    }
    Ok(())
}

pub(crate) async fn run_restore(cli: &Cli, name: &str, confirm: &str) -> Result<()> {
    require_confirmation(name, confirm)?;
    let c = admin_client(cli)?;
    let result = c.restore_backup(name).await.map_err(map_err)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "target": result.target,
            "instructions": result.instructions,
        }))?
    );
    Ok(())
}

fn require_confirmation(name: &str, confirm: &str) -> Result<()> {
    if confirm != name {
        return Err(anyhow!("--confirm must equal the backup name ({name})"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Typed confirmation guards both restore and backup delete. They must
    // refuse a mismatched (or missing — clap enforces presence) confirm text
    // before any client is built, so the check is testable without a server.
    #[test]
    fn confirmation_must_match_backup_name() {
        assert!(require_confirmation("dump.dump", "dump.dump").is_ok());
        let err = require_confirmation("dump.dump", "other.dump").unwrap_err();
        assert!(err.to_string().contains("dump.dump"), "got: {err}");
        assert!(require_confirmation("dump.dump", "").is_err());
    }
}
