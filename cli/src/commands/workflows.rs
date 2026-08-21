//! Durable workflows (FM-29): `workflows list|get|start|cancel|signal`.

use anyhow::{Context, Result, anyhow};
use par_rt_db_client::{WorkflowListOptions, WorkflowSpec, WorkflowStatus};

use crate::args::{Cli, WorkflowsCommand};
use crate::output::map_err;

use super::{admin_client, read_spec_file, require_db};

pub(crate) async fn run_workflows(cli: &Cli, command: &WorkflowsCommand) -> Result<()> {
    match command {
        WorkflowsCommand::List { status, limit } => {
            let db = require_db(cli)?;
            // Validate before the credential gate so a bad value surfaces its
            // specific error (and is testable) without credentials.
            let status = status.as_deref().map(parse_workflow_status).transpose()?;
            let c = admin_client(cli)?;
            let opts = WorkflowListOptions {
                status,
                limit: *limit,
            };
            let rows = c.list_workflows(&db, Some(&opts)).await.map_err(map_err)?;
            println!("{}", serde_json::to_string_pretty(&rows)?);
        }
        WorkflowsCommand::Get { id } => {
            let db = require_db(cli)?;
            let c = admin_client(cli)?;
            let full = c.get_workflow(&db, id).await.map_err(map_err)?;
            println!("{}", serde_json::to_string_pretty(&full)?);
        }
        WorkflowsCommand::Start { file } => {
            let db = require_db(cli)?;
            let json = read_spec_file(file)?;
            let spec: WorkflowSpec =
                serde_json::from_str(&json).context("parsing WorkflowSpec JSON")?;
            let c = admin_client(cli)?;
            let id = c.start_workflow(&db, &spec).await.map_err(map_err)?;
            let out = serde_json::json!({ "id": id });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
        WorkflowsCommand::Cancel { id } => {
            let db = require_db(cli)?;
            let c = admin_client(cli)?;
            let ok = c.cancel_workflow(&db, id).await.map_err(map_err)?;
            let out = serde_json::json!({ "ok": ok });
            println!("{}", serde_json::to_string_pretty(&out)?);
            if !ok {
                // ok:false = unknown or already-terminal run — a legitimate
                // no-op on the server, not an error.
                eprintln!("no-op: workflow run {id} is unknown or already terminal");
            }
        }
        WorkflowsCommand::Signal {
            id,
            name,
            payload_json,
        } => {
            let db = require_db(cli)?;
            // Parse before the credential gate (the `start` spec pattern) so a
            // bad payload surfaces its specific error without credentials.
            let payload = payload_json
                .as_deref()
                .map(serde_json::from_str)
                .transpose()
                .context("parsing payload JSON")?;
            let c = admin_client(cli)?;
            let ok = c
                .signal_workflow(&db, id, name, payload.as_ref())
                .await
                .map_err(map_err)?;
            let out = serde_json::json!({ "ok": ok });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
    }
    Ok(())
}

/// Validate `workflows list --status` against the six snake_case wire values.
/// Delegates to `WorkflowStatus::from_str` (the closed wire domain) so the CLI
/// can never send a status string the server doesn't define. Pure validation,
/// extracted from the handler so it is unit-testable without a server.
fn parse_workflow_status(raw: &str) -> Result<WorkflowStatus> {
    raw.parse::<WorkflowStatus>().map_err(|_| {
        anyhow!(
            "invalid --status '{raw}' — expected pending|running|waiting|success|failed|cancelled"
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_workflow_status_accepts_the_six_wire_values() {
        for (raw, expected) in [
            ("pending", WorkflowStatus::Pending),
            ("running", WorkflowStatus::Running),
            ("waiting", WorkflowStatus::Waiting),
            ("success", WorkflowStatus::Success),
            ("failed", WorkflowStatus::Failed),
            ("cancelled", WorkflowStatus::Cancelled),
        ] {
            assert_eq!(parse_workflow_status(raw).unwrap(), expected, "raw={raw}");
        }
    }

    #[test]
    fn list_output_prints_waiting_for_only_while_waiting() {
        // `workflows list` renders rows via `to_string_pretty`; the wait fields
        // are skip-serialized when absent, so they appear exactly while a run
        // is parked on an `awaitSignal` step.
        let waiting = serde_json::from_value::<par_rt_db_client::WorkflowInfo>(serde_json::json!({
            "id": "w1", "name": "n", "status": "waiting", "currentStep": 1,
            "stepCount": 2, "attempts": 0, "createdAt": 1, "updatedAt": 2,
            "waitingFor": "approve", "waitedSince": 1234
        }))
        .unwrap();
        let out = serde_json::to_string_pretty(&vec![waiting]).unwrap();
        assert!(out.contains("\"waitingFor\": \"approve\""), "got: {out}");
        assert!(out.contains("\"waitedSince\": 1234"), "got: {out}");

        let running = serde_json::from_value::<par_rt_db_client::WorkflowInfo>(serde_json::json!({
            "id": "w2", "name": "n", "status": "running", "currentStep": 0,
            "stepCount": 2, "attempts": 0, "createdAt": 1, "updatedAt": 2
        }))
        .unwrap();
        let out = serde_json::to_string_pretty(&vec![running]).unwrap();
        assert!(!out.contains("waitingFor"), "got: {out}");
        assert!(!out.contains("waitedSince"), "got: {out}");
    }

    #[test]
    fn parse_workflow_status_rejects_non_wire_values() {
        // The wire domain is closed and snake_case-only: uppercase and the
        // one-l spelling of cancelled are rejected alongside garbage.
        for raw in ["bogus", "RUNNING", "canceled", ""] {
            let err = parse_workflow_status(raw).unwrap_err().to_string();
            assert!(err.contains("invalid --status"), "raw={raw} got: {err}");
        }
    }
}
