//! Shared workflow arm helpers.

use crate::committer::*;

pub(in crate::committer) use super::workflow_advance::handle_workflow_advance;

/// Terminal record for an advance that failed before any step could run
/// (schema load) or on a corrupt row (step index out of range).
pub(in crate::committer) fn failed_outcome(
    row: &crate::workflows::WorkflowRow,
    error: &str,
) -> crate::protocol::StepOutcome {
    crate::protocol::StepOutcome {
        step_index: row.current_step,
        status: crate::protocol::OutcomeStatus::Failed,
        attempts: row.attempts.max(1),
        at: now_ms(),
        error: Some(error.to_string()),
        signal: None,
    }
}
