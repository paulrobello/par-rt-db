//! Machine-token management: `mint-token`, `revoke-token`.

use anyhow::Result;

use crate::args::Cli;
use crate::output::map_err;

use super::admin_client;

pub(crate) async fn run_mint_token(cli: &Cli, db: &str, name: &str) -> Result<()> {
    let c = admin_client(cli)?;
    let minted = c.mint_token(db, name).await.map_err(map_err)?;
    // `MintedToken` is response-only (Deserialize), so rebuild the wire
    // shape `{tokenId, token}` for output.
    let out = serde_json::json!({ "tokenId": minted.token_id, "token": minted.token });
    println!("{}", serde_json::to_string_pretty(&out)?);
    Ok(())
}

pub(crate) async fn run_revoke_token(cli: &Cli, id: &str) -> Result<()> {
    let c = admin_client(cli)?;
    c.revoke_token(id).await.map_err(map_err)?;
    eprintln!("revoked token {id}");
    Ok(())
}
