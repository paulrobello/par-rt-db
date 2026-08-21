//! Argument definitions: the clap [`Cli`] parser, the [`Command`] /
//! [`WorkflowsCommand`] / [`SessionsCommand`] subcommand enums, and the
//! SEC-204 argv-secret predicate that warns when credentials are passed as
//! flags instead of env vars.

use clap::{CommandFactory, Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
// term_width is pinned so `--help` output — and the README section generated
// from it by `gen-cli-docs` — is byte-identical regardless of terminal size.
#[command(
    name = "rtdb",
    version,
    about = "Operator + CI CLI for par-rt-db",
    term_width = 80
)]
pub(crate) struct Cli {
    /// Server base URL (e.g. https://rtdb.example.com).
    #[arg(long, env = "RTDB_URL")]
    pub(crate) url: String,

    /// Database name — used by `query`, `mutate`, and `push-schema`.
    #[arg(long, env = "RTDB_DB")]
    pub(crate) db: Option<String>,

    /// Machine token for `query` / `mutate`.
    #[arg(long, env = "RTDB_TOKEN", hide_env_values = true)]
    pub(crate) token: Option<String>,

    /// Instance admin key — bearer for every admin subcommand.
    #[arg(long, env = "RTDB_ADMIN_KEY", hide_env_values = true)]
    pub(crate) admin_key: Option<String>,

    #[command(subcommand)]
    pub(crate) command: Command,
}

/// The `rtdb` clap definition, shared by the shipped binary (`main` parses
/// through it) and the README generator (`cli/src/bin/gen-cli-docs.rs`), so
/// the documented reference and the real `--help` output come from one
/// source.
pub(crate) fn cli() -> clap::Command {
    Cli::command()
}

#[derive(Subcommand, Debug)]
pub(crate) enum Command {
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
    /// Manage durable workflow runs in `--db`. (admin)
    Workflows {
        #[command(subcommand)]
        command: WorkflowsCommand,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum WorkflowsCommand {
    /// List workflow runs in `--db`, newest first.
    List {
        /// Filter by run status:
        /// pending|running|waiting|success|failed|cancelled.
        #[arg(long)]
        status: Option<String>,
        /// Cap the result count (server default 100, capped at 500).
        #[arg(long)]
        limit: Option<u32>,
    },
    /// Print one workflow run: the info row plus the per-step outcome trail.
    Get {
        /// Workflow run id to fetch.
        #[arg(long)]
        id: String,
    },
    /// Start a new workflow run from a WorkflowSpec JSON file.
    Start {
        /// Path to a JSON file containing a `WorkflowSpec` (wire shape:
        /// `{"name": .., "steps": [{"txn": ..}]}`). An optional `@` prefix
        /// matches the `query`/`mutate` file convention.
        #[arg(long)]
        file: String,
    },
    /// Cancel a workflow run.
    Cancel {
        /// Workflow run id to cancel.
        #[arg(long)]
        id: String,
    },
    /// Deliver a named signal to a waiting run (releases an `awaitSignal`
    /// step).
    Signal {
        /// Workflow run id to signal.
        #[arg(long)]
        id: String,
        /// Signal name (must match the parked step's `awaitSignal.name`).
        #[arg(long)]
        name: String,
        /// Optional JSON payload for the signal, e.g. `'{"approvedBy":"u1"}'`.
        #[arg(long)]
        payload_json: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum SessionsCommand {
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

/// SEC-204: `--token` / `--admin-key` values on the command line are visible
/// to every process on the host (`ps`) and persist in shell history. The
/// flags stay (scripts depend on them) — this only detects them so `main` can
/// point at the env vars, which clap treats as an equal source. Exact names
/// and their `=`-joined forms only, so sibling flags like `--token-hash`
/// never match.
pub(crate) fn secrets_on_argv<I, S>(args: I) -> bool
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    use std::ffi::OsStr;
    args.into_iter().any(|a| {
        let a = a.as_ref();
        a == OsStr::new("--token")
            || a == OsStr::new("--admin-key")
            || a.to_string_lossy().starts_with("--token=")
            || a.to_string_lossy().starts_with("--admin-key=")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // SEC-204: the argv-secret warning must fire for --token/--admin-key in
    // either spaced or `=`-joined form, and not when credentials come from
    // the environment (no flag on the command line). Sibling flags like
    // --token-hash must not trigger it. (The eprintln itself is one line in
    // main over this predicate — the harness here can't capture stderr.)
    #[test]
    fn argv_secret_detection() {
        assert!(secrets_on_argv([
            "rtdb", "--token", "sekret", "query", "{}"
        ]));
        assert!(secrets_on_argv([
            "rtdb",
            "--admin-key",
            "sekret",
            "list-dbs"
        ]));
        assert!(secrets_on_argv(["rtdb", "--token=sekret", "query", "{}"]));
        assert!(secrets_on_argv(["rtdb", "--admin-key=sekret", "list-dbs"]));
        assert!(!secrets_on_argv(["rtdb", "--url", "http://x", "list-dbs"]));
        assert!(!secrets_on_argv([
            "rtdb",
            "--url",
            "http://x",
            "sessions",
            "revoke",
            "--token-hash",
            "abc",
        ]));
    }

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
    fn parses_workflows_subcommands() {
        // `workflows list` parses bare and with both filters.
        let cli = Cli::try_parse_from([
            "rtdb",
            "--url",
            "http://x",
            "--db",
            "d",
            "--admin-key",
            "k",
            "workflows",
            "list",
        ])
        .unwrap();
        let Command::Workflows { command } = cli.command else {
            panic!("expected Workflows");
        };
        let WorkflowsCommand::List { status, limit } = command else {
            panic!("expected WorkflowsCommand::List");
        };
        assert_eq!(status, None);
        assert_eq!(limit, None);

        let cli = Cli::try_parse_from([
            "rtdb",
            "--url",
            "http://x",
            "--db",
            "d",
            "--admin-key",
            "k",
            "workflows",
            "list",
            "--status",
            "running",
            "--limit",
            "25",
        ])
        .unwrap();
        let Command::Workflows { command } = cli.command else {
            panic!("expected Workflows");
        };
        let WorkflowsCommand::List { status, limit } = command else {
            panic!("expected WorkflowsCommand::List");
        };
        assert_eq!(status.as_deref(), Some("running"));
        assert_eq!(limit, Some(25));

        // `workflows get --id`.
        let cli = Cli::try_parse_from([
            "rtdb",
            "--url",
            "http://x",
            "--db",
            "d",
            "--admin-key",
            "k",
            "workflows",
            "get",
            "--id",
            "run1",
        ])
        .unwrap();
        let Command::Workflows { command } = cli.command else {
            panic!("expected Workflows");
        };
        let WorkflowsCommand::Get { id } = command else {
            panic!("expected WorkflowsCommand::Get");
        };
        assert_eq!(id, "run1");

        // `workflows start --file` parses a bare path and an `@`-prefixed one.
        for arg in ["spec.json", "@spec.json"] {
            let cli = Cli::try_parse_from([
                "rtdb",
                "--url",
                "http://x",
                "--db",
                "d",
                "--admin-key",
                "k",
                "workflows",
                "start",
                "--file",
                arg,
            ])
            .unwrap();
            let Command::Workflows { command } = cli.command else {
                panic!("expected Workflows");
            };
            let WorkflowsCommand::Start { file } = command else {
                panic!("expected WorkflowsCommand::Start");
            };
            assert_eq!(file, arg);
        }

        // `workflows cancel --id`.
        let cli = Cli::try_parse_from([
            "rtdb",
            "--url",
            "http://x",
            "--db",
            "d",
            "--admin-key",
            "k",
            "workflows",
            "cancel",
            "--id",
            "run1",
        ])
        .unwrap();
        let Command::Workflows { command } = cli.command else {
            panic!("expected Workflows");
        };
        let WorkflowsCommand::Cancel { id } = command else {
            panic!("expected WorkflowsCommand::Cancel");
        };
        assert_eq!(id, "run1");

        // `workflows signal --id --name [--payload-json]`.
        let cli = Cli::try_parse_from([
            "rtdb",
            "--url",
            "http://x",
            "--db",
            "d",
            "--admin-key",
            "k",
            "workflows",
            "signal",
            "--id",
            "run1",
            "--name",
            "approve",
            "--payload-json",
            r#"{"approvedBy":"u1"}"#,
        ])
        .unwrap();
        let Command::Workflows { command } = cli.command else {
            panic!("expected Workflows");
        };
        let WorkflowsCommand::Signal {
            id,
            name,
            payload_json,
        } = command
        else {
            panic!("expected WorkflowsCommand::Signal");
        };
        assert_eq!(id, "run1");
        assert_eq!(name, "approve");
        assert_eq!(payload_json.as_deref(), Some(r#"{"approvedBy":"u1"}"#));

        // payload is optional.
        let cli = Cli::try_parse_from([
            "rtdb",
            "--url",
            "http://x",
            "--db",
            "d",
            "--admin-key",
            "k",
            "workflows",
            "signal",
            "--id",
            "run1",
            "--name",
            "approve",
        ])
        .unwrap();
        let Command::Workflows { command } = cli.command else {
            panic!("expected Workflows");
        };
        let WorkflowsCommand::Signal {
            id,
            name,
            payload_json,
        } = command
        else {
            panic!("expected WorkflowsCommand::Signal");
        };
        assert_eq!(id, "run1");
        assert_eq!(name, "approve");
        assert_eq!(payload_json, None);
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
}
