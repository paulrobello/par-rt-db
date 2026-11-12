//! Background bulk-op vocabulary (ARC-005): splits a large deleteByQuery
//! across committer turns so one op cannot monopolize the single serialized
//! writer. The registry is in-memory only — a server restart loses op state
//! while the remaining rows stay, the status GET returns NotFound, and the
//! client re-submits (a delete is idempotent: an id that vanished between the
//! freeze and its chunk is skipped, not an error). The op's ids are frozen
//! once at start inside the committer's serialized turn under the caller's
//! per-row auth; each chunk deletes EXACTLY those ids, so no re-scan ever
//! races concurrent writes. Subscribers see progressive updates for free:
//! every chunk is a normal committer turn with the post-commit fan-out.
//!
//! NOT a second writer: `run_op` only submits `RunBulkDeleteChunk` arms; every
//! durable delete happens inside the per-db committer's serialized turn.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::committer::CommitterRequest;
use crate::error::RtDbError;

/// Rows one bulk-delete chunk deletes per committer turn. Bounds a chunk's
/// serialized-turn hold to a fraction of the pre-split multi-second baseline.
pub(crate) const BULK_DELETE_CHUNK_ROWS: usize = 500;

/// Upper bound on documents one bulk delete may freeze. Over-cap submissions
/// fail BadRequest ("narrow the filter") instead of pinning the registry with
/// an unbounded id list.
pub(crate) const MAX_BULK_DELETE_IDS: usize = 100_000;

/// One bulk op's progress. `ids` is the frozen matching set (order and
/// membership fixed at start); `cursor` indexes it. The admission epoch is
/// captured at submit time and re-verified transactionally in every chunk
/// turn, so a read-only/quiesce flip mid-op fails the remaining chunks.
pub struct BulkOpState {
    pub(crate) id: String,
    pub(crate) db: String,
    pub(crate) table: String,
    pub(crate) owner: Option<String>,
    pub(crate) ids: Vec<String>,
    pub(crate) admission_epoch: i64,
    pub(crate) cursor: usize,
    pub(crate) phase: BulkPhase,
    pub(crate) matched: usize,
    pub(crate) deleted: u64,
    pub(crate) error_text: Option<String>,
    #[allow(dead_code)] // diagnostics surface; the status wire omits it today
    pub(crate) created_at: i64,
}

/// Lifecycle of a bulk op. `Failed` records the error text verbatim; the ids
/// still present stay in the table and the client may re-submit.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum BulkPhase {
    Running,
    Complete,
    Failed,
}

/// The per-instance op map, keyed by unguessable op id. Ops are per-db but the
/// key space is global — the id is a uuid, so a db never sees another db's id.
pub struct BulkRegistry {
    ops: Mutex<HashMap<String, BulkOpState>>,
}

impl BulkRegistry {
    pub(crate) fn new() -> Self {
        Self {
            ops: Mutex::new(HashMap::new()),
        }
    }

    /// Registers a freshly-started op in the Running phase. The id was drawn
    /// from a uuid, so the key cannot collide.
    pub(crate) fn register(&self, op: BulkOpState) {
        let id = op.id.clone();
        self.ops
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(id, op);
    }

    /// The status projection the HTTP GET returns. `None` = unknown id
    /// (including an op a restart lost).
    pub(crate) fn status(&self, op_id: &str) -> Option<BulkStatus> {
        let map = self.ops.lock().unwrap_or_else(|e| e.into_inner());
        let op = map.get(op_id)?;
        Some(BulkStatus {
            db: op.db.clone(),
            phase: op.phase,
            error: op.error_text.clone(),
            matched: op.matched,
            deleted: op.deleted,
        })
    }

    /// Advances an op after a successful chunk: `processed` ids left the
    /// pending window (deleted, or already gone before their chunk — both are
    /// done), `deleted` more rows actually left the table.
    pub(crate) fn advance(&self, op_id: &str, processed: usize, deleted: u64) {
        let mut map = self.ops.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(op) = map.get_mut(op_id) {
            op.cursor += processed;
            op.deleted = op.deleted.saturating_add(deleted);
            if op.cursor >= op.ids.len() {
                op.phase = BulkPhase::Complete;
            }
        }
    }

    /// Marks an op Failed with the error's wire text.
    pub(crate) fn fail(&self, op_id: &str, error: &RtDbError) {
        let mut map = self.ops.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(op) = map.get_mut(op_id) {
            op.phase = BulkPhase::Failed;
            op.error_text = Some(error.message.clone());
        }
    }

    /// The reply channel dropped (committer task exit) — no error text
    /// traveled, so record the generic internal the mutate path surfaces.
    pub(crate) fn fail_internal(&self, op_id: &str, message: &str) {
        let mut map = self.ops.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(op) = map.get_mut(op_id) {
            op.phase = BulkPhase::Failed;
            op.error_text = Some(message.to_string());
        }
    }

    /// The driver's next chunk: `None` ends the loop (op finished, failed, or
    /// unknown).
    pub(crate) fn next_chunk(&self, op_id: &str) -> Option<NextChunk> {
        let mut map = self.ops.lock().unwrap_or_else(|e| e.into_inner());
        let op = map.get_mut(op_id)?;
        if !matches!(op.phase, BulkPhase::Running) {
            return None;
        }
        if op.cursor >= op.ids.len() {
            op.phase = BulkPhase::Complete;
            return None;
        }
        let end = (op.cursor + BULK_DELETE_CHUNK_ROWS).min(op.ids.len());
        Some(NextChunk {
            table: op.table.clone(),
            owner: op.owner.clone(),
            ids: op.ids[op.cursor..end].to_vec(),
            admission_epoch: op.admission_epoch,
        })
    }
}

/// The slice the driver submits as one chunk turn.
pub(crate) struct NextChunk {
    pub(crate) table: String,
    pub(crate) owner: Option<String>,
    pub(crate) ids: Vec<String>,
    pub(crate) admission_epoch: i64,
}

/// The status GET's wire projection (serialized by `http_api::bulk`).
pub(crate) struct BulkStatus {
    pub(crate) db: String,
    pub(crate) phase: BulkPhase,
    pub(crate) error: Option<String>,
    pub(crate) matched: usize,
    pub(crate) deleted: u64,
}

/// The per-op driver task: one chunk per committer turn, no sleep between
/// chunks — the committer queue order gives interactive writes their turn
/// between chunk turns, which is the whole point of the split. Runs until the
/// op reaches Complete or Failed; the op is registered before this task
/// spawns, so the first `next_chunk` always finds it.
pub(crate) async fn run_op(
    registry: Arc<BulkRegistry>,
    committers: crate::committer::Committers,
    db: String,
    op_id: String,
) {
    loop {
        let Some(chunk) = registry.next_chunk(&op_id) else {
            return;
        };
        let processed = chunk.ids.len();
        let (reply, reply_rx) = tokio::sync::oneshot::channel();
        let submit_result = committers
            .submit(
                &db,
                CommitterRequest::RunBulkDeleteChunk {
                    table: chunk.table,
                    ids: chunk.ids,
                    owner: chunk.owner,
                    admission_epoch: chunk.admission_epoch,
                    reply,
                },
            )
            .await;
        if let Err(error) = submit_result {
            registry.fail(&op_id, &error);
            tracing::warn!(
                db = %db,
                op_id = %op_id,
                error = %error,
                "bulk op: chunk submit failed"
            );
            return;
        }
        match reply_rx.await {
            Ok(Ok(deleted)) => {
                registry.advance(&op_id, processed, deleted);
            }
            Ok(Err(error)) => {
                registry.fail(&op_id, &error);
                tracing::warn!(
                    db = %db,
                    op_id = %op_id,
                    error = %error,
                    "bulk op: chunk turn failed"
                );
                return;
            }
            Err(_) => {
                registry.fail_internal(&op_id, "committer task dropped the chunk reply");
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(ids: Vec<&str>) -> (String, BulkOpState) {
        (
            "op1".to_string(),
            BulkOpState {
                id: "op1".to_string(),
                db: "db".to_string(),
                table: "items".to_string(),
                owner: None,
                ids: ids.into_iter().map(String::from).collect(),
                admission_epoch: 1,
                cursor: 0,
                phase: BulkPhase::Running,
                matched: 3,
                deleted: 0,
                error_text: None,
                created_at: 0,
            },
        )
    }

    #[test]
    fn completes_when_cursor_reaches_end() {
        let registry = BulkRegistry::new();
        let (id, state) = op(vec!["a", "b"]);
        registry.register(state);
        registry.advance(&id, 1, 1);
        let status = registry.status(&id).expect("op present");
        assert!(matches!(status.phase, BulkPhase::Running));
        registry.advance(&id, 1, 0);
        let status = registry.status(&id).expect("op present");
        assert!(matches!(status.phase, BulkPhase::Complete));
        assert_eq!(status.deleted, 1);
        assert_eq!(status.matched, 3);
    }

    #[test]
    fn failed_op_reports_error_and_stops_chunking() {
        let registry = BulkRegistry::new();
        let (id, state) = op(vec!["a", "b", "c"]);
        registry.register(state);
        registry.advance(&id, 2, 2);
        registry.fail_internal(&id, "boom");
        let status = registry.status(&id).expect("op present");
        assert!(matches!(status.phase, BulkPhase::Failed));
        assert_eq!(status.error.as_deref(), Some("boom"));
        assert!(registry.next_chunk(&id).is_none());
    }

    #[test]
    fn status_unknown_id_is_none() {
        let registry = BulkRegistry::new();
        assert!(registry.status("missing").is_none());
    }

    #[test]
    fn next_chunk_slices_by_chunk_rows() {
        let registry = BulkRegistry::new();
        let (id, state) = op(vec!["a", "b"]);
        registry.register(state);
        // next_chunk does not advance the cursor — a driver crash between
        // fetching the slice and its reply replays the same chunk, which the
        // idempotent deletes make safe. Completion happens only via advance.
        let chunk = registry.next_chunk(&id).expect("chunk");
        assert_eq!(chunk.ids.len(), 2);
        let replay = registry.next_chunk(&id).expect("same window until advance");
        assert_eq!(replay.ids.len(), 2);
        registry.advance(&id, 2, 0);
        assert!(registry.next_chunk(&id).is_none());
        let status = registry.status(&id).expect("op present");
        assert!(matches!(status.phase, BulkPhase::Complete));
    }
}
