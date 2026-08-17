//! Subcommand handlers, grouped one file per command family, plus the
//! shared client builders, credential gates, and input-file helpers every
//! family uses.

pub(crate) mod data;
pub(crate) mod dbs;
pub(crate) mod schema;
pub(crate) mod sessions;
pub(crate) mod tokens;
pub(crate) mod workflows;

use anyhow::{Context, Result, anyhow};
use par_rt_db_client::{RtDbAdminClient, RtDbHttpClient};

use crate::args::Cli;

/// Build a client for an admin subcommand (ARC-121: admin control plane now
/// has its own `RtDbAdminClient` type). The admin key is the sole bearer.
pub(crate) fn admin_client(cli: &Cli) -> Result<RtDbAdminClient> {
    let admin_key = require_admin(cli)?;
    Ok(RtDbAdminClient::new(&cli.url, &admin_key))
}

/// Build a client for a data-plane subcommand (`query` / `mutate`): machine
/// token + db, both required and validated by the caller before this is reached.
pub(crate) fn data_client(cli: &Cli, db: &str, token: &str) -> RtDbHttpClient {
    RtDbHttpClient::new(&cli.url, db, token)
}

pub(crate) fn require_db(cli: &Cli) -> Result<String> {
    cli.db
        .clone()
        .ok_or_else(|| anyhow!("--db (or RTDB_DB) is required for this subcommand"))
}

pub(crate) fn require_token(cli: &Cli) -> Result<String> {
    cli.token
        .clone()
        .ok_or_else(|| anyhow!("--token (or RTDB_TOKEN) is required for this subcommand"))
}

pub(crate) fn require_admin(cli: &Cli) -> Result<String> {
    cli.admin_key
        .clone()
        .ok_or_else(|| anyhow!("--admin-key (or RTDB_ADMIN_KEY) is required for this subcommand"))
}

/// Read a JSON argument: `@path` reads from a file, everything else is treated
/// as the literal JSON string. Used for the `query` / `mutate` positionals.
pub(crate) fn read_json_arg(arg: &str) -> Result<String> {
    if let Some(path) = arg.strip_prefix('@') {
        std::fs::read_to_string(path).with_context(|| format!("reading {path}"))
    } else {
        Ok(arg.to_string())
    }
}

/// Read a `--file` argument (`workflows start`): a filesystem path, optionally
/// `@`-prefixed to match the `query`/`mutate` `@file` convention. Unlike
/// [`read_json_arg`] a bare value is always a path, never inline JSON.
pub(crate) fn read_spec_file(arg: &str) -> Result<String> {
    let path = arg.strip_prefix('@').unwrap_or(arg);
    std::fs::read_to_string(path).with_context(|| format!("reading {path}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::args::Command;

    #[test]
    fn read_json_arg_inline_returns_literal() {
        let s = r#"{"table":"x"}"#;
        assert_eq!(read_json_arg(s).unwrap(), s);
    }

    #[test]
    fn read_json_arg_at_file_reads_path() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path =
            std::env::temp_dir().join(format!("rtdb-cli-test-{}-{nonce}.json", std::process::id()));
        let body = r#"{"table":"x"}"#;
        std::fs::write(&path, body).unwrap();
        let arg = format!("@{}", path.display());
        assert_eq!(read_json_arg(&arg).unwrap(), body);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn read_json_arg_at_missing_file_errors() {
        assert!(read_json_arg("@/nonexistent/rtdb-cli-test-does-not-exist.json").is_err());
    }

    #[test]
    fn require_helpers_return_value_or_error() {
        let missing = Cli {
            url: "http://x".into(),
            db: None,
            token: None,
            admin_key: None,
            command: Command::ListDbs,
        };
        assert!(require_db(&missing).is_err());
        assert!(require_token(&missing).is_err());
        assert!(require_admin(&missing).is_err());

        let present = Cli {
            url: "http://x".into(),
            db: Some("d".into()),
            token: Some("t".into()),
            admin_key: Some("a".into()),
            command: Command::ListDbs,
        };
        assert_eq!(require_db(&present).unwrap(), "d");
        assert_eq!(require_token(&present).unwrap(), "t");
        assert_eq!(require_admin(&present).unwrap(), "a");
    }

    #[test]
    fn read_spec_file_reads_bare_and_at_prefixed_paths() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path =
            std::env::temp_dir().join(format!("rtdb-cli-spec-{}-{nonce}.json", std::process::id()));
        let body = r#"{"name":"n","steps":[]}"#;
        std::fs::write(&path, body).unwrap();
        assert_eq!(read_spec_file(&path.display().to_string()).unwrap(), body);
        assert_eq!(
            read_spec_file(&format!("@{}", path.display())).unwrap(),
            body
        );
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn read_spec_file_missing_path_errors() {
        assert!(read_spec_file("/nonexistent/rtdb-cli-spec-does-not-exist.json").is_err());
        assert!(read_spec_file("@/nonexistent/rtdb-cli-spec-does-not-exist.json").is_err());
    }
}
