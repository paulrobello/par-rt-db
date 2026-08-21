//! `rtdb` — operator + CI CLI for [par-rt-db](https://github.com/paulrobello/par-rt-db).
//!
//! Thin wrapper over the `par-rt-db-client` rust-client. Covers operator
//! workflows (schema push, db list/create, token mint/revoke) and CI seed
//! scripts (one-shot query/mutate) without reaching for the dashboard or raw
//! curl. The server URL and credentials may be supplied via flags or the
//! `RTDB_URL` / `RTDB_DB` / `RTDB_TOKEN` / `RTDB_ADMIN_KEY` env vars.
//!
//! Admin subcommands (`list-dbs`, `create-db`, `clone-db`, `push-schema`,
//! `mint-token`, `revoke-token`, `sessions list|revoke`, `merge-users`,
//! `workflows list|get|start|cancel|signal`) send the instance admin key as the
//! bearer. Data-plane subcommands (`query`, `mutate`) send a machine token
//! scoped to `--db`.
//!
//! Layout: [`args`] holds the clap definitions and the SEC-204 argv-secret
//! predicate, [`commands`] the per-family handlers plus the shared
//! client/credential/input helpers, and [`output`] the error-formatting
//! helper. This root is parse → dispatch only.

mod args;
mod commands;
mod output;

use anyhow::Result;
use args::{Cli, Command};
use clap::FromArgMatches;

#[tokio::main]
async fn main() -> Result<()> {
    // SEC-204: emitted before parsing so it always precedes any connection
    // error the subcommand produces.
    if args::secrets_on_argv(std::env::args_os()) {
        eprintln!(
            "warning: --token/--admin-key on the command line is visible in ps and shell history; prefer RTDB_TOKEN / RTDB_ADMIN_KEY"
        );
    }
    // Parse through the same args::cli() command gen-cli-docs renders from,
    // so the shipped binary and the documented reference share one clap
    // definition (including its pinned term_width).
    let cli = Cli::from_arg_matches(&args::cli().get_matches())?;
    dispatch(&cli).await
}

/// Route a parsed `Cli` to its subcommand handler. Thin dispatcher so each
/// subcommand's entry point is individually addressable from tests (the
/// credential-validation and argument-validation paths return `Err` before any
/// network I/O, so they are unit-testable without a live server).
async fn dispatch(cli: &Cli) -> Result<()> {
    match &cli.command {
        Command::ListDbs => commands::dbs::run_list_dbs(cli).await,
        Command::CreateDb { name } => commands::dbs::run_create_db(cli, name).await,
        Command::CloneDb { from, to } => commands::dbs::run_clone_db(cli, from, to).await,
        Command::PushSchema { file } => commands::schema::run_push_schema(cli, file).await,
        Command::MintToken { db, name } => commands::tokens::run_mint_token(cli, db, name).await,
        Command::RevokeToken { id } => commands::tokens::run_revoke_token(cli, id).await,
        Command::Sessions { command } => commands::sessions::run_sessions(cli, command).await,
        Command::MergeUsers {
            anon,
            real,
            confirm,
        } => commands::sessions::run_merge_users(cli, anon, real, confirm).await,
        Command::Query { query } => commands::data::run_query(cli, query).await,
        Command::Mutate { txn } => commands::data::run_mutate(cli, txn).await,
        Command::Migrate { file, dry_run } => {
            commands::schema::run_migrate(cli, file, *dry_run).await
        }
        Command::Explain { query } => commands::data::run_explain(cli, query).await,
        Command::SlowQueries { db, limit } => {
            commands::data::run_slow_queries(cli, db, *limit).await
        }
        Command::Workflows { command } => commands::workflows::run_workflows(cli, command).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use args::{SessionsCommand, WorkflowsCommand};

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

    #[tokio::test]
    async fn dispatch_workflows_list_validates_status_before_credentials() {
        // --db supplied, no admin key: a bad --status surfaces its specific
        // error (validation fires before the credential gate, no network); a
        // good one proceeds to the credential gate and errors there instead.
        let bad = Cli {
            url: "http://x".into(),
            db: Some("d".into()),
            token: None,
            admin_key: None,
            command: Command::Workflows {
                command: WorkflowsCommand::List {
                    status: Some("bogus".into()),
                    limit: None,
                },
            },
        };
        let err = dispatch(&bad).await.unwrap_err().to_string();
        assert!(err.contains("invalid --status"), "got: {err}");

        let good = Cli {
            url: "http://x".into(),
            db: Some("d".into()),
            token: None,
            admin_key: None,
            command: Command::Workflows {
                command: WorkflowsCommand::List {
                    status: Some("running".into()),
                    limit: None,
                },
            },
        };
        let err = dispatch(&good).await.unwrap_err().to_string();
        assert!(
            err.contains("--admin-key"),
            "expected the credential-gate error, got: {err}"
        );
    }

    #[tokio::test]
    async fn dispatch_workflows_subcommands_require_db() {
        // Every workflows subcommand needs --db before it reaches the admin
        // gate; all error before any network when --db is absent.
        for command in [
            WorkflowsCommand::List {
                status: None,
                limit: None,
            },
            WorkflowsCommand::Get { id: "run1".into() },
            WorkflowsCommand::Start {
                file: "spec.json".into(),
            },
            WorkflowsCommand::Cancel { id: "run1".into() },
            WorkflowsCommand::Signal {
                id: "run1".into(),
                name: "approve".into(),
                payload_json: None,
            },
        ] {
            let err = dispatch(&cli_with_command(Command::Workflows { command }))
                .await
                .unwrap_err()
                .to_string();
            assert!(err.contains("--db"), "got: {err}");
        }
    }

    #[tokio::test]
    async fn dispatch_workflows_start_parses_spec_before_credentials() {
        // The spec file is read + parsed before the credential gate: an
        // invalid file surfaces its parse error without a key; a valid one
        // proceeds to the credential gate and errors there instead.
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let bad_path = std::env::temp_dir().join(format!(
            "rtdb-cli-wfspec-bad-{}-{nonce}.json",
            std::process::id()
        ));
        let good_path = std::env::temp_dir().join(format!(
            "rtdb-cli-wfspec-good-{}-{nonce}.json",
            std::process::id()
        ));
        std::fs::write(&bad_path, r#"{"nope":true}"#).unwrap();
        std::fs::write(&good_path, r#"{"name":"n","steps":[]}"#).unwrap();

        let mk = |file: String| Cli {
            url: "http://x".into(),
            db: Some("d".into()),
            token: None,
            admin_key: None,
            command: Command::Workflows {
                command: WorkflowsCommand::Start { file },
            },
        };

        let err = dispatch(&mk(bad_path.display().to_string()))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("parsing WorkflowSpec JSON"), "got: {err}");

        let err = dispatch(&mk(good_path.display().to_string()))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("--admin-key"),
            "expected the credential-gate error, got: {err}"
        );

        std::fs::remove_file(&bad_path).ok();
        std::fs::remove_file(&good_path).ok();
    }

    #[tokio::test]
    async fn dispatch_workflows_signal_parses_payload_before_credentials() {
        // The payload is parsed before the credential gate (the `start` spec
        // pattern): invalid JSON surfaces its parse error without a key; valid
        // JSON proceeds to the credential gate and errors there instead.
        let mk = |payload_json: Option<String>| Cli {
            url: "http://x".into(),
            db: Some("d".into()),
            token: None,
            admin_key: None,
            command: Command::Workflows {
                command: WorkflowsCommand::Signal {
                    id: "run1".into(),
                    name: "approve".into(),
                    payload_json,
                },
            },
        };

        let err = dispatch(&mk(Some("{not json".into())))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("parsing payload JSON"), "got: {err}");

        let err = dispatch(&mk(Some(r#"{"approvedBy":"u1"}"#.into())))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("--admin-key"),
            "expected the credential-gate error, got: {err}"
        );

        let err = dispatch(&mk(None)).await.unwrap_err().to_string();
        assert!(
            err.contains("--admin-key"),
            "expected the credential-gate error, got: {err}"
        );
    }
}
