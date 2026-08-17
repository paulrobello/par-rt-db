//! Interactive sessions and user identity: `sessions list|revoke` and
//! `merge-users`.

use anyhow::{Result, anyhow};

use crate::args::{Cli, SessionsCommand};
use crate::output::map_err;

use super::admin_client;

pub(crate) async fn run_sessions(cli: &Cli, command: &SessionsCommand) -> Result<()> {
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

pub(crate) async fn run_merge_users(
    cli: &Cli,
    anon: &str,
    real: &str,
    confirm: &str,
) -> Result<()> {
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
