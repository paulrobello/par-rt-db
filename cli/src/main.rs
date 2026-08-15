//! `rtdb` — operator + CI CLI for [par-rt-db](https://github.com/paulrobello/par-rt-db).
//!
//! Thin wrapper over the `par-rt-db-client` rust-client. Covers operator
//! workflows (schema push, db list/create, token mint/revoke) and CI seed
//! scripts (one-shot query/mutate) without reaching for the dashboard or raw
//! curl. The server URL and credentials may be supplied via flags or the
//! `RTDB_URL` / `RTDB_DB` / `RTDB_TOKEN` / `RTDB_ADMIN_KEY` env vars.
//!
//! Admin subcommands (`list-dbs`, `create-db`, `clone-db`, `push-schema`,
//! `mint-token`, `revoke-token`, `sessions list|revoke`, `merge-users`) send
//! the instance admin key as the bearer. Data-plane subcommands (`query`,
//! `mutate`) send a machine token scoped to `--db`.

use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand};
use par_rt_db_client::{
    MigrateRequestOwned, Query, RtDbAdminClient, RtDbError, RtDbHttpClient, SchemaDef, Transaction,
};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "rtdb", version, about = "Operator + CI CLI for par-rt-db")]
struct Cli {
    /// Server base URL (e.g. https://rtdb.pardev.net). [env: RTDB_URL]
    #[arg(long, env = "RTDB_URL")]
    url: String,

    /// Database name — used by `query`, `mutate`, and `push-schema`. [env: RTDB_DB]
    #[arg(long, env = "RTDB_DB")]
    db: Option<String>,

    /// Machine token for `query` / `mutate`. [env: RTDB_TOKEN]
    #[arg(long, env = "RTDB_TOKEN")]
    token: Option<String>,

    /// Instance admin key — bearer for every admin subcommand. [env: RTDB_ADMIN_KEY]
    #[arg(long, env = "RTDB_ADMIN_KEY")]
    admin_key: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// List every database on the instance. (admin)
    ListDbs,
    /// Create a new database. (admin)
    CreateDb {
        /// Database name to create.
        name: String,
    },
    /// Clone a database (schema + documents) into a new one. (admin)
    CloneDb {
        /// Source database to clone from.
        from: String,
        /// Destination database name (must not already exist).
        to: String,
    },
    /// Push a SchemaDef JSON file to `--db`. (admin)
    PushSchema {
        /// Path to a JSON file containing a `SchemaDef` (wire shape:
        /// `{"tables": {<name>: {"fields": {..}}}}`).
        file: PathBuf,
    },
    /// Mint a machine token for a database. (admin)
    MintToken {
        /// Database to mint the token for.
        db: String,
        /// Human-readable token name (e.g. "ci-seed").
        name: String,
    },
    /// Revoke a machine token by id. (admin)
    RevokeToken {
        /// Token id (`tok_…`) to revoke.
        id: String,
    },
    /// Manage active interactive sessions. (admin)
    Sessions {
        #[command(subcommand)]
        command: SessionsCommand,
    },
    /// Merge an anonymous user into a real one, synchronously. (admin)
    MergeUsers {
        /// Anonymous user id whose data is merged away.
        #[arg(long)]
        anon: String,
        /// Real user id that receives the anon user's data.
        #[arg(long)]
        real: String,
        /// Typed confirmation — must equal `--real`.
        #[arg(long)]
        confirm: String,
    },
    /// Run a Query JSON against `--db` and print the result. (machine token)
    Query {
        /// Query JSON, e.g. `{"table":"items","take":10}`. Prefix with `@` to
        /// read from a file (`@query.json`).
        query: String,
    },
    /// Run a Transaction JSON against `--db` and print step results. (machine token)
    Mutate {
        /// Transaction JSON (`{"steps":[..]}`). Prefix with `@` to read from a
        /// file (`@seed.json`).
        txn: String,
    },
    /// Apply (or preview with `--dry-run`) a migration directives JSON file to
    /// `--db`. (admin)
    Migrate {
        /// Path to a JSON file containing a `MigrateRequestOwned` body (wire
        /// shape: `{"directives":[...], "dryRun"?: bool}`).
        file: PathBuf,
        /// Preview only — nothing is applied. The request's `dryRun` field is
        /// also honored; this flag forces it on.
        #[arg(long)]
        dry_run: bool,
    },
    /// Explain a Query's compiled SQL against `--db` without running it. (admin)
    Explain {
        /// Query JSON, e.g. `{"table":"items","take":10}`. Prefix with `@` to
        /// read from a file (`@query.json`).
        query: String,
    },
    /// List recent slow queries across the instance. (admin)
    SlowQueries {
        /// Filter to one database.
        #[arg(long)]
        db: Option<String>,
        /// Cap the result count.
        #[arg(long)]
        limit: Option<u32>,
    },
}

#[derive(Subcommand, Debug)]
enum SessionsCommand {
    /// List active interactive sessions, newest-first.
    List {
        /// Filter by user id or email.
        #[arg(long)]
        user: Option<String>,
        /// Cap the result count (server default 200, clamped to [1, 1000]).
        #[arg(long)]
        limit: Option<u64>,
    },
    /// Revoke a single session by token hash, or every session for a user.
    Revoke {
        /// Token hash (sha256 digest) of the session to revoke. Mutually
        /// exclusive with `--user`.
        #[arg(long)]
        token_hash: Option<String>,
        /// Revoke every session for this user id. Mutually exclusive with
        /// `--token-hash`.
        #[arg(long)]
        user: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    dispatch(&cli).await
}

/// Route a parsed `Cli` to its subcommand handler. Thin dispatcher so each
/// subcommand's entry point is individually addressable from tests (the
/// credential-validation and argument-validation paths return `Err` before any
/// network I/O, so they are unit-testable without a live server).
async fn dispatch(cli: &Cli) -> Result<()> {
    match &cli.command {
        Command::ListDbs => run_list_dbs(cli).await,
        Command::CreateDb { name } => run_create_db(cli, name).await,
        Command::CloneDb { from, to } => run_clone_db(cli, from, to).await,
        Command::PushSchema { file } => run_push_schema(cli, file).await,
        Command::MintToken { db, name } => run_mint_token(cli, db, name).await,
        Command::RevokeToken { id } => run_revoke_token(cli, id).await,
        Command::Sessions { command } => run_sessions(cli, command).await,
        Command::MergeUsers {
            anon,
            real,
            confirm,
        } => run_merge_users(cli, anon, real, confirm).await,
        Command::Query { query } => run_query(cli, query).await,
        Command::Mutate { txn } => run_mutate(cli, txn).await,
        Command::Migrate { file, dry_run } => run_migrate(cli, file, *dry_run).await,
        Command::Explain { query } => run_explain(cli, query).await,
        Command::SlowQueries { db, limit } => run_slow_queries(cli, db, *limit).await,
    }
}

async fn run_list_dbs(cli: &Cli) -> Result<()> {
    let c = admin_client(cli)?;
    for db in c.list_dbs().await.map_err(map_err)? {
        println!("{db}");
    }
    Ok(())
}

async fn run_create_db(cli: &Cli, name: &str) -> Result<()> {
    let c = admin_client(cli)?;
    c.create_db(name).await.map_err(map_err)?;
    eprintln!("created database {name}");
    Ok(())
}

async fn run_clone_db(cli: &Cli, from: &str, to: &str) -> Result<()> {
    let c = admin_client(cli)?;
    c.clone_db(from, to).await.map_err(map_err)?;
    eprintln!("cloned database {from} into {to}");
    Ok(())
}

async fn run_push_schema(cli: &Cli, file: &PathBuf) -> Result<()> {
    let db = require_db(cli)?;
    let json =
        std::fs::read_to_string(file).with_context(|| format!("reading {}", file.display()))?;
    let schema: SchemaDef = serde_json::from_str(&json).context("parsing SchemaDef JSON")?;
    let c = admin_client(cli)?;
    c.push_schema(&db, &schema).await.map_err(map_err)?;
    eprintln!("pushed schema to {db}");
    Ok(())
}

async fn run_mint_token(cli: &Cli, db: &str, name: &str) -> Result<()> {
    let c = admin_client(cli)?;
    let minted = c.mint_token(db, name).await.map_err(map_err)?;
    // `MintedToken` is response-only (Deserialize), so rebuild the wire
    // shape `{tokenId, token}` for output.
    let out = serde_json::json!({ "tokenId": minted.token_id, "token": minted.token });
    println!("{}", serde_json::to_string_pretty(&out)?);
    Ok(())
}

async fn run_revoke_token(cli: &Cli, id: &str) -> Result<()> {
    let c = admin_client(cli)?;
    c.revoke_token(id).await.map_err(map_err)?;
    eprintln!("revoked token {id}");
    Ok(())
}

async fn run_sessions(cli: &Cli, command: &SessionsCommand) -> Result<()> {
    match command {
        SessionsCommand::List { user, limit } => {
            let c = admin_client(cli)?;
            let opts = par_rt_db_client::SessionListOptions {
                user: user.clone(),
                limit: limit.as_ref().map(|n| *n as i64),
            };
            let rows = c.list_sessions(Some(&opts)).await.map_err(map_err)?;
            for s in &rows {
                let email = s.email.as_deref().unwrap_or("-");
                let kind = if s.anonymous { "anon" } else { "user" };
                println!(
                    "{}\t{}\t{}\t{}\texp={}",
                    s.token_hash, s.user_id, kind, email, s.expires_at
                );
            }
            eprintln!("{} session(s)", rows.len());
        }
        SessionsCommand::Revoke { token_hash, user } => {
            // Validate the flag combination before acquiring a client so an
            // arg error is surfaced (and is testable) without credentials.
            let target = resolve_revoke_target(token_hash.as_deref(), user.as_deref())?;
            let c = admin_client(cli)?;
            match target {
                RevokeTarget::TokenHash(hash) => {
                    c.revoke_session(&hash).await.map_err(map_err)?;
                    eprintln!("revoked session {hash}");
                }
                RevokeTarget::User(uid) => {
                    let r = c.revoke_user_sessions(&uid).await.map_err(map_err)?;
                    eprintln!("revoked {} session(s) for user {uid}", r.revoked);
                }
            }
        }
    }
    Ok(())
}

async fn run_merge_users(cli: &Cli, anon: &str, real: &str, confirm: &str) -> Result<()> {
    // Typed-confirmation guard — same pattern as the server's `merge-users`
    // check. Validated before the credential gate so an arg error is surfaced
    // (and is testable) without credentials.
    if confirm != real {
        return Err(anyhow!("--confirm must equal --real ({real})"));
    }
    let c = admin_client(cli)?;
    let report = c.merge_users(anon, real).await.map_err(map_err)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

async fn run_query(cli: &Cli, query: &str) -> Result<()> {
    let db = require_db(cli)?;
    let token = require_token(cli)?;
    let json = read_json_arg(query)?;
    let q: Query = serde_json::from_str(&json).context("parsing Query JSON")?;
    let c = data_client(cli, &db, &token);
    let result: serde_json::Value = c.run(q).await.map_err(map_err)?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

async fn run_mutate(cli: &Cli, txn: &str) -> Result<()> {
    let db = require_db(cli)?;
    let token = require_token(cli)?;
    let json = read_json_arg(txn)?;
    let t: Transaction = serde_json::from_str(&json).context("parsing Transaction JSON")?;
    let c = data_client(cli, &db, &token);
    let results = c.mutate(&t, None).await.map_err(map_err)?;
    println!("{}", serde_json::to_string_pretty(&results)?);
    Ok(())
}

async fn run_migrate(cli: &Cli, file: &PathBuf, dry_run_flag: bool) -> Result<()> {
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

async fn run_explain(cli: &Cli, query: &str) -> Result<()> {
    let db = require_db(cli)?;
    require_admin(cli)?;
    let json = read_json_arg(query)?;
    let q: Query = serde_json::from_str(&json).context("parsing Query JSON")?;
    let c = admin_client(cli)?;
    let result = c.explain_query(&db, &q).await.map_err(map_err)?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

async fn run_slow_queries(cli: &Cli, db: &Option<String>, limit: Option<u32>) -> Result<()> {
    require_admin(cli)?;
    let c = admin_client(cli)?;
    let result = c
        .get_slow_queries(db.as_deref(), limit)
        .await
        .map_err(map_err)?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

/// Build a client for an admin subcommand (ARC-121: admin control plane now
/// has its own `RtDbAdminClient` type). The admin key is the sole bearer.
fn admin_client(cli: &Cli) -> Result<RtDbAdminClient> {
    let admin_key = require_admin(cli)?;
    Ok(RtDbAdminClient::new(&cli.url, &admin_key))
}

/// Build a client for a data-plane subcommand (`query` / `mutate`): machine
/// token + db, both required and validated by the caller before this is reached.
fn data_client(cli: &Cli, db: &str, token: &str) -> RtDbHttpClient {
    RtDbHttpClient::new(&cli.url, db, token)
}

fn require_db(cli: &Cli) -> Result<String> {
    cli.db
        .clone()
        .ok_or_else(|| anyhow!("--db (or RTDB_DB) is required for this subcommand"))
}

fn require_token(cli: &Cli) -> Result<String> {
    cli.token
        .clone()
        .ok_or_else(|| anyhow!("--token (or RTDB_TOKEN) is required for this subcommand"))
}

fn require_admin(cli: &Cli) -> Result<String> {
    cli.admin_key
        .clone()
        .ok_or_else(|| anyhow!("--admin-key (or RTDB_ADMIN_KEY) is required for this subcommand"))
}

/// Read a JSON argument: `@path` reads from a file, everything else is treated
/// as the literal JSON string. Used for the `query` / `mutate` positionals.
fn read_json_arg(arg: &str) -> Result<String> {
    if let Some(path) = arg.strip_prefix('@') {
        std::fs::read_to_string(path).with_context(|| format!("reading {path}"))
    } else {
        Ok(arg.to_string())
    }
}

/// Surface an `RtDbError` as `<CODE>: <message>`. `RtDbError`'s own Display
/// (via thiserror) is just the message, so the wire code is recovered here by
/// serializing `ErrorCode` (it carries `#[serde(rename_all = "SCREAMING_SNAKE_CASE")]`).
fn map_err(e: RtDbError) -> anyhow::Error {
    let code = serde_json::to_value(e.code)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("{:?}", e.code));
    anyhow!("{code}: {}", e.message)
}

/// Which session(s) `sessions revoke` should target, resolved from the
/// `--token-hash` / `--user` flags by [`resolve_revoke_target`].
#[derive(Debug)]
enum RevokeTarget {
    TokenHash(String),
    User(String),
}

/// Resolve `sessions revoke` flags into a single target. `--token-hash` and
/// `--user` are mutually exclusive; at least one is required. Pure validation
/// extracted from the handler so it is unit-testable without a server.
fn resolve_revoke_target(token_hash: Option<&str>, user: Option<&str>) -> Result<RevokeTarget> {
    match (token_hash, user) {
        (Some(hash), None) => Ok(RevokeTarget::TokenHash(hash.to_string())),
        (None, Some(uid)) => Ok(RevokeTarget::User(uid.to_string())),
        (Some(_), Some(_)) => Err(anyhow!("--token-hash and --user are mutually exclusive")),
        (None, None) => Err(anyhow!("sessions revoke requires --token-hash or --user")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use par_rt_db_client::ErrorCode;

    #[test]
    fn parses_admin_subcommands() {
        let cli =
            Cli::try_parse_from(["rtdb", "--url", "http://x", "--admin-key", "k", "list-dbs"])
                .unwrap();
        assert!(matches!(cli.command, Command::ListDbs));
        assert_eq!(cli.url, "http://x");
        assert_eq!(cli.admin_key.as_deref(), Some("k"));

        let cli = Cli::try_parse_from([
            "rtdb",
            "--url",
            "http://x",
            "--admin-key",
            "k",
            "create-db",
            "mydb",
        ])
        .unwrap();
        let Command::CreateDb { name } = cli.command else {
            panic!("expected CreateDb");
        };
        assert_eq!(name, "mydb");

        let cli = Cli::try_parse_from([
            "rtdb",
            "--url",
            "http://x",
            "--admin-key",
            "k",
            "clone-db",
            "src",
            "dst",
        ])
        .unwrap();
        let Command::CloneDb { from, to } = cli.command else {
            panic!("expected CloneDb");
        };
        assert_eq!(from, "src");
        assert_eq!(to, "dst");

        let cli = Cli::try_parse_from([
            "rtdb",
            "--url",
            "http://x",
            "--db",
            "d",
            "--admin-key",
            "k",
            "push-schema",
            "schema.json",
        ])
        .unwrap();
        let Command::PushSchema { file } = cli.command else {
            panic!("expected PushSchema");
        };
        assert_eq!(file, PathBuf::from("schema.json"));

        let cli = Cli::try_parse_from([
            "rtdb",
            "--url",
            "http://x",
            "--admin-key",
            "k",
            "mint-token",
            "d",
            "ci",
        ])
        .unwrap();
        let Command::MintToken { db, name } = cli.command else {
            panic!("expected MintToken");
        };
        assert_eq!(db, "d");
        assert_eq!(name, "ci");

        let cli = Cli::try_parse_from([
            "rtdb",
            "--url",
            "http://x",
            "--admin-key",
            "k",
            "revoke-token",
            "tok_1",
        ])
        .unwrap();
        let Command::RevokeToken { id } = cli.command else {
            panic!("expected RevokeToken");
        };
        assert_eq!(id, "tok_1");

        // `sessions list` parses with optional filters.
        let cli = Cli::try_parse_from([
            "rtdb",
            "--url",
            "http://x",
            "--admin-key",
            "k",
            "sessions",
            "list",
            "--user",
            "u1",
            "--limit",
            "50",
        ])
        .unwrap();
        let Command::Sessions { command } = cli.command else {
            panic!("expected Sessions");
        };
        let SessionsCommand::List { user, limit } = command else {
            panic!("expected SessionsCommand::List");
        };
        assert_eq!(user.as_deref(), Some("u1"));
        assert_eq!(limit, Some(50));

        // `sessions revoke --token-hash` and `--user` both parse.
        let cli = Cli::try_parse_from([
            "rtdb",
            "--url",
            "http://x",
            "--admin-key",
            "k",
            "sessions",
            "revoke",
            "--token-hash",
            "abc123",
        ])
        .unwrap();
        let Command::Sessions { command } = cli.command else {
            panic!("expected Sessions");
        };
        let SessionsCommand::Revoke { token_hash, user } = command else {
            panic!("expected SessionsCommand::Revoke");
        };
        assert_eq!(token_hash.as_deref(), Some("abc123"));
        assert_eq!(user, None);

        // `merge-users` parses its three required flags.
        let cli = Cli::try_parse_from([
            "rtdb",
            "--url",
            "http://x",
            "--admin-key",
            "k",
            "merge-users",
            "--anon",
            "u-anon",
            "--real",
            "u-real",
            "--confirm",
            "u-real",
        ])
        .unwrap();
        let Command::MergeUsers {
            anon,
            real,
            confirm,
        } = cli.command
        else {
            panic!("expected MergeUsers");
        };
        assert_eq!(anon, "u-anon");
        assert_eq!(real, "u-real");
        assert_eq!(confirm, "u-real");
    }

    #[test]
    fn parses_query_and_mutate() {
        let q = r#"{"table":"items","take":5}"#;
        let cli = Cli::try_parse_from([
            "rtdb", "--url", "http://x", "--db", "d", "--token", "t", "query", q,
        ])
        .unwrap();
        let Command::Query { query } = cli.command else {
            panic!("expected Query");
        };
        assert_eq!(query, q);

        let txn = r#"{"steps":[{"op":"insert","table":"items","doc":{}}]}"#;
        let cli = Cli::try_parse_from([
            "rtdb", "--url", "http://x", "--db", "d", "--token", "t", "mutate", txn,
        ])
        .unwrap();
        let Command::Mutate { txn: t } = cli.command else {
            panic!("expected Mutate");
        };
        assert_eq!(t, txn);
    }

    #[test]
    fn parses_migrate_dry_run_flag() {
        let cli = Cli::try_parse_from([
            "rtdb",
            "--url",
            "http://x",
            "--db",
            "d",
            "--admin-key",
            "k",
            "migrate",
            "mig.json",
            "--dry-run",
        ])
        .unwrap();
        let Command::Migrate { file, dry_run } = cli.command else {
            panic!("expected Migrate");
        };
        assert_eq!(file, PathBuf::from("mig.json"));
        assert!(dry_run);

        let cli = Cli::try_parse_from([
            "rtdb",
            "--url",
            "http://x",
            "--db",
            "d",
            "--admin-key",
            "k",
            "migrate",
            "mig.json",
        ])
        .unwrap();
        let Command::Migrate { dry_run, .. } = cli.command else {
            panic!("expected Migrate variant");
        };
        assert!(
            !dry_run,
            "migrate without --dry-run should set dry_run=false"
        );
    }

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
    fn map_err_surfaces_code_and_message() {
        let e = RtDbError::new(ErrorCode::NotFound, "missing thing");
        assert_eq!(map_err(e).to_string(), "NOT_FOUND: missing thing");
    }

    #[test]
    fn env_vars_supply_credentials_when_flags_absent() {
        // clap only consults these env vars when the matching flag is absent
        // from argv; every other test passes flags explicitly, so setting them
        // here cannot flake the rest of the suite.
        // SAFETY: tests run single-threaded by default and no other test in
        // this module reads these vars; we remove them at the end so the
        // environment is clean for any future concurrent test executor.
        unsafe {
            std::env::set_var("RTDB_URL", "http://env");
            std::env::set_var("RTDB_ADMIN_KEY", "env-admin");
            std::env::set_var("RTDB_DB", "envdb");
            std::env::set_var("RTDB_TOKEN", "env-tok");
        }
        let cli = Cli::try_parse_from(["rtdb", "list-dbs"]).unwrap();
        assert_eq!(cli.url, "http://env");
        assert_eq!(cli.admin_key.as_deref(), Some("env-admin"));
        assert_eq!(cli.db.as_deref(), Some("envdb"));
        assert_eq!(cli.token.as_deref(), Some("env-tok"));
        // SAFETY: same single-threaded test, cleanup only.
        unsafe {
            std::env::remove_var("RTDB_URL");
            std::env::remove_var("RTDB_ADMIN_KEY");
            std::env::remove_var("RTDB_DB");
            std::env::remove_var("RTDB_TOKEN");
        }
    }

    /// Build a `Cli` with the given command and no credentials. Used to verify
    /// each subcommand fails fast (credential or arg validation) before network.
    fn cli_with_command(command: Command) -> Cli {
        Cli {
            url: "http://x".into(),
            db: None,
            token: None,
            admin_key: None,
            command,
        }
    }

    #[tokio::test]
    async fn dispatch_admin_subcommands_error_without_admin_key() {
        // Each admin subcommand fails at the credential gate (require_admin)
        // before any network I/O when no admin key is supplied.
        assert!(dispatch(&cli_with_command(Command::ListDbs)).await.is_err());
        assert!(
            dispatch(&cli_with_command(Command::CreateDb { name: "d".into() }))
                .await
                .is_err()
        );
        assert!(
            dispatch(&cli_with_command(Command::CloneDb {
                from: "a".into(),
                to: "b".into()
            }))
            .await
            .is_err()
        );
        assert!(
            dispatch(&cli_with_command(Command::MintToken {
                db: "d".into(),
                name: "n".into()
            }))
            .await
            .is_err()
        );
        assert!(
            dispatch(&cli_with_command(Command::RevokeToken { id: "t1".into() }))
                .await
                .is_err()
        );
        assert!(
            dispatch(&cli_with_command(Command::Sessions {
                command: SessionsCommand::List {
                    user: None,
                    limit: None
                }
            }))
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn dispatch_push_schema_and_migrate_require_db() {
        // push-schema and migrate need --db before they reach the admin gate;
        // both error before any network when --db is absent.
        assert!(
            dispatch(&cli_with_command(Command::PushSchema {
                file: "schema.json".into()
            }))
            .await
            .is_err()
        );
        assert!(
            dispatch(&cli_with_command(Command::Migrate {
                file: "mig.json".into(),
                dry_run: false
            }))
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn dispatch_query_and_mutate_require_db_and_token() {
        // data-plane subcommands need both --db and --token; error without them.
        assert!(
            dispatch(&cli_with_command(Command::Query {
                query: r#"{"table":"x"}"#.into()
            }))
            .await
            .is_err()
        );
        assert!(
            dispatch(&cli_with_command(Command::Mutate {
                txn: r#"{"steps":[]}"#.into()
            }))
            .await
            .is_err()
        );
        // With --db but no --token, still errors (require_token fires).
        let with_db_no_token = Cli {
            url: "http://x".into(),
            db: Some("d".into()),
            token: None,
            admin_key: None,
            command: Command::Query {
                query: r#"{"table":"x"}"#.into(),
            },
        };
        assert!(dispatch(&with_db_no_token).await.is_err());
    }

    #[tokio::test]
    async fn dispatch_sessions_revoke_validates_flag_combination() {
        // resolve_revoke_target fires before the credential gate, so the arg
        // validation is reachable (and its specific error surfaces) without a key.
        let neither = cli_with_command(Command::Sessions {
            command: SessionsCommand::Revoke {
                token_hash: None,
                user: None,
            },
        });
        let err = dispatch(&neither).await.unwrap_err().to_string();
        assert!(
            err.contains("requires --token-hash or --user"),
            "got: {err}"
        );

        let both = cli_with_command(Command::Sessions {
            command: SessionsCommand::Revoke {
                token_hash: Some("h".into()),
                user: Some("u".into()),
            },
        });
        let err = dispatch(&both).await.unwrap_err().to_string();
        assert!(err.contains("mutually exclusive"), "got: {err}");
    }

    #[tokio::test]
    async fn dispatch_merge_users_validates_confirm_matches_real() {
        // The typed-confirmation guard fires before the credential gate, so a
        // mismatch surfaces its specific error without a key; a match proceeds
        // to the credential gate and errors there instead.
        let mismatch = cli_with_command(Command::MergeUsers {
            anon: "u-anon".into(),
            real: "u-real".into(),
            confirm: "wrong".into(),
        });
        let err = dispatch(&mismatch).await.unwrap_err().to_string();
        assert!(err.contains("--confirm must equal --real"), "got: {err}");

        let match_no_key = cli_with_command(Command::MergeUsers {
            anon: "u-anon".into(),
            real: "u-real".into(),
            confirm: "u-real".into(),
        });
        let err = dispatch(&match_no_key).await.unwrap_err().to_string();
        assert!(
            err.contains("--admin-key"),
            "expected the credential-gate error, got: {err}"
        );
    }

    #[test]
    fn resolve_revoke_target_token_hash_only() {
        let t = resolve_revoke_target(Some("abc"), None).unwrap();
        assert!(matches!(t, RevokeTarget::TokenHash(h) if h == "abc"));
    }

    #[test]
    fn resolve_revoke_target_user_only() {
        let t = resolve_revoke_target(None, Some("uid")).unwrap();
        assert!(matches!(t, RevokeTarget::User(u) if u == "uid"));
    }

    #[test]
    fn resolve_revoke_target_both_is_error() {
        let err = resolve_revoke_target(Some("h"), Some("u"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("mutually exclusive"), "got: {err}");
    }

    #[test]
    fn resolve_revoke_target_neither_is_error() {
        let err = resolve_revoke_target(None, None).unwrap_err().to_string();
        assert!(err.contains("requires"), "got: {err}");
    }
}
