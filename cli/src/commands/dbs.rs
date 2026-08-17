//! Database lifecycle: `list-dbs`, `create-db`, `clone-db`.

use anyhow::Result;

use crate::args::Cli;
use crate::output::map_err;

use super::admin_client;

pub(crate) async fn run_list_dbs(cli: &Cli) -> Result<()> {
    let c = admin_client(cli)?;
    for db in c.list_dbs().await.map_err(map_err)? {
        println!("{db}");
    }
    Ok(())
}

pub(crate) async fn run_create_db(cli: &Cli, name: &str) -> Result<()> {
    let c = admin_client(cli)?;
    c.create_db(name).await.map_err(map_err)?;
    eprintln!("created database {name}");
    Ok(())
}

pub(crate) async fn run_clone_db(cli: &Cli, from: &str, to: &str) -> Result<()> {
    let c = admin_client(cli)?;
    c.clone_db(from, to).await.map_err(map_err)?;
    eprintln!("cloned database {from} into {to}");
    Ok(())
}
