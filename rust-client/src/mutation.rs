//! Atomic multi-step transaction DSL for writing documents to par-rt-db.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::wire::{FilterExpr, ScheduleWhen, WorkflowSpec, WorkflowStepSpec};

#[derive(Debug, Clone, Serialize, Deserialize)]
/// An ordered list of steps executed atomically by the server's committer.
pub struct Transaction {
    /// The steps, applied in order; any failure rolls the whole txn back.
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", deny_unknown_fields)]
/// One write/control step (tagged `{"op": "..."}`, camelCase).
pub enum Step {
    /// Insert a new document; result is its id.
    Insert {
        /// Target table.
        table: String,
        /// The document body.
        doc: Map<String, Value>,
    },
    /// Merge `fields` into an existing document; result is `null`.
    Patch {
        /// Target table.
        table: String,
        /// Document id.
        id: String,
        /// Keys to merge in.
        fields: Map<String, Value>,
    },
    /// Overwrite the whole document; result is `null`.
    Replace {
        /// Target table.
        table: String,
        /// Document id.
        id: String,
        /// The full replacement body.
        doc: Map<String, Value>,
    },
    /// Delete a document; result is `null`.
    Delete {
        /// Target table.
        table: String,
        /// Document id.
        id: String,
    },
    /// Precondition: the row must be at exactly `version`.
    ExpectVersion {
        /// Target table.
        table: String,
        /// Document id.
        id: String,
        /// The required current version.
        version: i64,
    },
    /// Precondition: no row may match the index eq-prefix.
    ExpectAbsent {
        /// Target table.
        table: String,
        /// Index to probe.
        index: String,
        /// The eq-prefix values.
        eq: Vec<Value>,
    },
    /// Insert-or-patch keyed by an index eq-prefix match.
    Upsert {
        /// Target table.
        table: String,
        /// Index whose eq-prefix locates the row.
        index: String,
        /// The eq-prefix values.
        eq: Vec<Value>,
        /// Body applied when inserting.
        insert: Map<String, Value>,
        /// Keys merged when the row exists.
        patch: Map<String, Value>,
    },
    /// Patch every row in `table` matching `filter`. At most `limit` rows
    /// (default server cap 1000); a larger match set patches `limit` and reports
    /// `truncated: true`. Mirrors `server/src/txn.rs::Step::PatchByQuery`
    /// byte-for-byte.
    PatchByQuery {
        /// Target table.
        table: String,
        /// Which rows match.
        filter: FilterExpr,
        /// Keys to merge into every match.
        patch: Map<String, Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// Row cap (server default/max 1000); `None` = server default.
        limit: Option<u32>,
    },
    /// Delete every row in `table` matching `filter` (same `limit`/`truncated`
    /// semantics as `PatchByQuery`). Mirrors
    /// `server/src/txn.rs::Step::DeleteByQuery` byte-for-byte.
    DeleteByQuery {
        /// Target table.
        table: String,
        /// Which rows match.
        filter: FilterExpr,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// Row cap (server default/max 1000); `None` = server default.
        limit: Option<u32>,
    },
    /// Schedule `txn` to run later. Mirrors
    /// `server/src/txn.rs::Step::Schedule` byte-for-byte (FM-28).
    Schedule {
        /// One-shot delay/absolute time, a cron expression, or a fixed
        /// interval.
        when: ScheduleWhen,
        /// The nested transaction to fire when due.
        txn: Box<Transaction>,
    },
    /// Cancel a previously scheduled job. Mirrors
    /// `server/src/txn.rs::Step::CancelSchedule` byte-for-byte (FM-28).
    CancelSchedule {
        /// The schedule id to cancel.
        id: String,
    },
    /// Start a durable workflow run. Mirrors
    /// `server/src/txn.rs::Step::StartWorkflow` byte-for-byte (FM-29).
    StartWorkflow {
        /// The run's spec, snapshotted per run server-side.
        spec: Box<WorkflowSpec>,
    },
    /// Cancel a workflow run. Mirrors
    /// `server/src/txn.rs::Step::CancelWorkflow` byte-for-byte (FM-29).
    CancelWorkflow {
        /// The workflow run id.
        id: String,
    },
    /// Restore a soft-deleted row (only legal on a table that declares
    /// `softDelete`). `NotFound` when the row is absent; idempotent `Ok` when
    /// it is present and already live. Mirrors
    /// `server/src/txn.rs::Step::Undelete` byte-for-byte (FM-33).
    Undelete {
        /// Target table (must declare `softDelete`).
        table: String,
        /// Document id.
        id: String,
    },
}

/// One entry of `mutateOk.results`, positionally aligned with `steps`.
///
/// Variant order matters: `Upsert` must precede `Insert` because serde's
/// `untagged` deserializer tries variants in declaration order and struct
/// variants ignore unknown fields — so `{id, inserted}` would otherwise be
/// greedily captured by `Insert`, silently dropping `inserted`. `PatchByQuery`
/// and `DeleteByQuery` carry distinct fields (`patched`/`deleted` + `truncated`)
/// that never collide with `{id}` / `{id, inserted}`, so their order relative
/// to the others is unconstrained, and `Schedule`/`Cancelled` carry
/// `scheduleId`/`cancelled`, which likewise never collide.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StepResult {
    /// `{id, inserted}` from an upsert step.
    Upsert {
        /// The row's id.
        id: String,
        /// Whether the step inserted (vs patched).
        inserted: bool,
    },
    /// `{id}` from an insert step.
    Insert {
        /// The inserted row's id.
        id: String,
    },
    /// `{patched, truncated}` from a patchByQuery step.
    PatchByQuery {
        /// Rows patched.
        patched: u32,
        /// Whether the match set exceeded `limit`.
        truncated: bool,
    },
    /// `{deleted, truncated}` from a deleteByQuery step.
    DeleteByQuery {
        /// Rows deleted.
        deleted: u32,
        /// Whether the match set exceeded `limit`.
        truncated: bool,
    },
    /// `{scheduleId}` from a schedule step.
    Schedule {
        #[serde(rename = "scheduleId")]
        /// The created job's id (wire key `scheduleId`).
        schedule_id: String,
    },
    /// `{cancelled}` from a cancelSchedule step.
    Cancelled {
        /// Whether a pending job was actually cancelled.
        cancelled: bool,
    },
    /// `{workflowId}` from a startWorkflow step.
    WorkflowId {
        #[serde(rename = "workflowId")]
        /// The started run's id (wire key `workflowId`).
        workflow_id: String,
    },
    /// `null` — the result of patch/delete/expect*/undelete steps.
    Null,
}

/// `null` on the wire deserializes to `StepResult::Null`.
impl Default for StepResult {
    fn default() -> Self {
        StepResult::Null
    }
}

/// Parse the `results` array returned by `/api/mutate`, `/admin/db/{db}/mutate`,
/// and the WS `mutateOk` frame into typed [`StepResult`]s. Shared by the HTTP,
/// admin, and WS mutate paths so the untagged-enum decode lives in one place.
#[cfg(any(feature = "http", feature = "ws"))]
pub(crate) fn parse_step_results(
    values: Vec<Value>,
) -> Result<Vec<StepResult>, crate::error::RtDbError> {
    values
        .into_iter()
        .map(|v| {
            serde_json::from_value::<StepResult>(v)
                .map_err(|e| crate::error::RtDbError::internal(format!("invalid step result: {e}")))
        })
        .collect()
}

/// Chainable builder producing a [`Transaction`].
pub struct Mutation {
    steps: Vec<Step>,
}

impl Mutation {
    /// Start an empty transaction.
    pub fn new() -> Self {
        Self { steps: Vec::new() }
    }

    fn obj(v: Value) -> Map<String, Value> {
        match v {
            Value::Object(m) => m,
            // Non-object input is a caller bug; send an empty object so the server
            // rejects it with SCHEMA_VIOLATION rather than panicking client-side.
            _ => Map::new(),
        }
    }

    /// Queue an insert step.
    pub fn insert(mut self, table: &str, doc: Value) -> Self {
        self.steps.push(Step::Insert {
            table: table.into(),
            doc: Self::obj(doc),
        });
        self
    }
    /// Queue a patch step (merge `fields` into the row).
    pub fn patch(mut self, table: &str, id: &str, fields: Value) -> Self {
        self.steps.push(Step::Patch {
            table: table.into(),
            id: id.into(),
            fields: Self::obj(fields),
        });
        self
    }
    /// Queue a replace step (overwrite the row).
    pub fn replace(mut self, table: &str, id: &str, doc: Value) -> Self {
        self.steps.push(Step::Replace {
            table: table.into(),
            id: id.into(),
            doc: Self::obj(doc),
        });
        self
    }
    /// Queue a delete step.
    pub fn delete(mut self, table: &str, id: &str) -> Self {
        self.steps.push(Step::Delete {
            table: table.into(),
            id: id.into(),
        });
        self
    }
    /// Queue a version precondition step.
    pub fn expect_version(mut self, table: &str, id: &str, version: i64) -> Self {
        self.steps.push(Step::ExpectVersion {
            table: table.into(),
            id: id.into(),
            version,
        });
        self
    }
    /// Queue an index-absence precondition step.
    pub fn expect_absent(mut self, table: &str, index: &str, eq: &[Value]) -> Self {
        self.steps.push(Step::ExpectAbsent {
            table: table.into(),
            index: index.into(),
            eq: eq.to_vec(),
        });
        self
    }
    /// Queue an upsert step keyed by the index eq-prefix.
    pub fn upsert(
        mut self,
        table: &str,
        index: &str,
        eq: &[Value],
        insert: Value,
        patch: Value,
    ) -> Self {
        self.steps.push(Step::Upsert {
            table: table.into(),
            index: index.into(),
            eq: eq.to_vec(),
            insert: Self::obj(insert),
            patch: Self::obj(patch),
        });
        self
    }

    /// Patch every row in `table` matching `filter`. `limit` defaults to the
    /// server cap (1000) when `None`; a larger match set patches `limit` rows and
    /// reports `truncated: true` in the result.
    pub fn patch_by_query(
        mut self,
        table: &str,
        filter: FilterExpr,
        patch: Value,
        limit: Option<u32>,
    ) -> Self {
        self.steps.push(Step::PatchByQuery {
            table: table.into(),
            filter,
            patch: Self::obj(patch),
            limit,
        });
        self
    }

    /// Delete every row in `table` matching `filter` (same `limit`/`truncated`
    /// semantics as [`patch_by_query`](Self::patch_by_query)).
    pub fn delete_by_query(mut self, table: &str, filter: FilterExpr, limit: Option<u32>) -> Self {
        self.steps.push(Step::DeleteByQuery {
            table: table.into(),
            filter,
            limit,
        });
        self
    }

    /// Schedule `txn` to run later — `Step::Schedule` (FM-28).
    pub fn schedule(mut self, when: ScheduleWhen, txn: Transaction) -> Self {
        self.steps.push(Step::Schedule {
            when,
            txn: Box::new(txn),
        });
        self
    }

    /// Cancel a previously scheduled job — `Step::CancelSchedule` (FM-28).
    pub fn cancel_schedule(mut self, id: impl Into<String>) -> Self {
        self.steps.push(Step::CancelSchedule { id: id.into() });
        self
    }

    /// Start a durable workflow run — `Step::StartWorkflow` (FM-29). The
    /// server snapshots `spec` per run and returns the run id as the step's
    /// result (`{"workflowId": "..."}`).
    pub fn start_workflow(mut self, spec: WorkflowSpec) -> Self {
        self.steps.push(Step::StartWorkflow {
            spec: Box::new(spec),
        });
        self
    }

    /// Cancel a workflow run — `Step::CancelWorkflow` (FM-29). The step's
    /// result is `{"cancelled": <bool>}` (`false` = run already terminal,
    /// a no-op not an error).
    pub fn cancel_workflow(mut self, id: impl Into<String>) -> Self {
        self.steps.push(Step::CancelWorkflow { id: id.into() });
        self
    }

    /// Restore a soft-deleted row — `Step::Undelete` (FM-33). Only legal on a
    /// table that declares `softDelete` (the server rejects otherwise with
    /// `BAD_REQUEST`); `NOT_FOUND` when the row is absent, and an idempotent
    /// `null` result when it is present and already live. The step's result is
    /// `null`.
    pub fn undelete(mut self, table: &str, id: &str) -> Self {
        self.steps.push(Step::Undelete {
            table: table.into(),
            id: id.into(),
        });
        self
    }

    /// Finish to the wire `Transaction`.
    pub fn build(self) -> Transaction {
        Transaction { steps: self.steps }
    }
}

impl Default for Mutation {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkflowStepSpec {
    /// Build an `awaitSignal` step: park the run until a signal named `name`
    /// is delivered (`WorkflowStepSpec::await_signal`, spec §Wire — exactly
    /// one of `txn`/`awaitSignal` per step; the server's `validate_spec`
    /// enforces it). `timeout_ms` bounds each wait attempt; `None` waits
    /// indefinitely (cancel is the escape).
    pub fn await_signal(name: impl Into<String>, timeout_ms: Option<u64>) -> Self {
        Self {
            txn: None,
            await_signal: Some(crate::wire::AwaitSignalSpec {
                name: name.into(),
                timeout_ms,
            }),
            retry: None,
            sleep_before_ms: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{StepRetry, WorkflowStepSpec};
    use serde_json::json;

    #[test]
    fn builder_serializes_all_step_kinds() {
        let txn = Mutation::new()
            .insert("items", json!({"projectId":"p1","title":"a"}))
            .patch("items", "i1", json!({"title":"b"}))
            .replace("items", "i4", json!({"projectId":"p1","title":"c"}))
            .delete("items", "i2")
            .expect_version("items", "i3", 7)
            .expect_absent(
                "items",
                "by_project_and_title",
                &[json!("p1"), json!("dup")],
            )
            .upsert(
                "items",
                "by_project",
                &[json!("p1")],
                json!({"projectId":"p1"}),
                json!({"title":"u"}),
            )
            .build();
        assert_eq!(
            serde_json::to_value(&txn).unwrap(),
            json!({
                "steps": [
                    {"op":"insert","table":"items","doc":{"projectId":"p1","title":"a"}},
                    {"op":"patch","table":"items","id":"i1","fields":{"title":"b"}},
                    {"op":"replace","table":"items","id":"i4","doc":{"projectId":"p1","title":"c"}},
                    {"op":"delete","table":"items","id":"i2"},
                    {"op":"expectVersion","table":"items","id":"i3","version":7},
                    {"op":"expectAbsent","table":"items","index":"by_project_and_title","eq":["p1","dup"]},
                    {"op":"upsert","table":"items","index":"by_project","eq":["p1"],"insert":{"projectId":"p1"},"patch":{"title":"u"}}
                ]
            })
        );
    }

    #[test]
    fn step_result_parses_insert_and_null() {
        let ins: StepResult = serde_json::from_value(json!({"id":"x"})).unwrap();
        assert!(matches!(ins, StepResult::Insert { id } if id == "x"));
        let nul: StepResult = serde_json::from_value(json!(null)).unwrap();
        assert!(matches!(nul, StepResult::Null));
    }

    #[test]
    fn step_result_parses_upsert() {
        let ins: StepResult = serde_json::from_value(json!({"id":"x","inserted":true})).unwrap();
        assert!(matches!(ins, StepResult::Upsert { inserted: true, .. }));
        let pat: StepResult = serde_json::from_value(json!({"id":"x","inserted":false})).unwrap();
        assert!(matches!(
            pat,
            StepResult::Upsert {
                inserted: false,
                ..
            }
        ));
    }

    #[test]
    fn patch_by_query_serializes() {
        let txn = Mutation::new()
            .patch_by_query(
                "items",
                FilterExpr::Eq {
                    field: "status".into(),
                    value: json!("backlog"),
                },
                json!({"status":"done"}),
                None,
            )
            .build();
        // limit omitted on the wire when None.
        assert_eq!(
            serde_json::to_value(&txn).unwrap(),
            json!({
                "steps": [
                    {
                        "op":"patchByQuery",
                        "table":"items",
                        "filter":{"op":"eq","field":"status","value":"backlog"},
                        "patch":{"status":"done"}
                    }
                ]
            })
        );
    }

    #[test]
    fn delete_by_query_serializes_with_limit() {
        let txn = Mutation::new()
            .delete_by_query(
                "items",
                FilterExpr::Eq {
                    field: "status".into(),
                    value: json!("archived"),
                },
                Some(50),
            )
            .build();
        assert_eq!(
            serde_json::to_value(&txn).unwrap(),
            json!({
                "steps": [
                    {
                        "op":"deleteByQuery",
                        "table":"items",
                        "filter":{"op":"eq","field":"status","value":"archived"},
                        "limit":50
                    }
                ]
            })
        );
    }

    #[test]
    fn step_result_parses_patch_and_delete_by_query() {
        let patched: StepResult =
            serde_json::from_value(json!({"patched":3,"truncated":false})).unwrap();
        assert!(matches!(
            patched,
            StepResult::PatchByQuery {
                patched: 3,
                truncated: false
            }
        ));
        let deleted: StepResult =
            serde_json::from_value(json!({"deleted":1000,"truncated":true})).unwrap();
        assert!(matches!(
            deleted,
            StepResult::DeleteByQuery {
                deleted: 1000,
                truncated: true
            }
        ));
    }

    #[test]
    fn schedule_and_cancel_schedule_serialize() {
        let txn = Mutation::new()
            .schedule(
                ScheduleWhen::AfterMs { ms: 60_000 },
                Transaction {
                    steps: vec![Step::Insert {
                        table: "workItems".into(),
                        doc: Mutation::obj(json!({"title":"later"})),
                    }],
                },
            )
            .cancel_schedule("j1")
            .build();
        assert_eq!(
            serde_json::to_value(&txn).unwrap(),
            json!({
                "steps": [
                    { "op": "schedule", "when": { "type": "afterMs", "ms": 60000 },
                      "txn": { "steps": [ { "op": "insert", "table": "workItems", "doc": { "title": "later" } } ] } },
                    { "op": "cancelSchedule", "id": "j1" }
                ]
            })
        );
    }

    #[test]
    fn step_result_parses_schedule_and_cancelled() {
        let sched: StepResult = serde_json::from_value(json!({"scheduleId":"s1"})).unwrap();
        assert!(matches!(sched, StepResult::Schedule { schedule_id } if schedule_id == "s1"));
        let cancelled: StepResult = serde_json::from_value(json!({"cancelled":true})).unwrap();
        assert!(matches!(
            cancelled,
            StepResult::Cancelled { cancelled: true }
        ));
    }

    #[test]
    fn start_and_cancel_workflow_serialize() {
        let spec = WorkflowSpec {
            name: "drip".into(),
            steps: vec![
                WorkflowStepSpec {
                    txn: Some(Transaction {
                        steps: vec![Step::Insert {
                            table: "workItems".into(),
                            doc: Mutation::obj(json!({"title":"first"})),
                        }],
                    }),
                    await_signal: None,
                    retry: None,
                    sleep_before_ms: None,
                },
                WorkflowStepSpec {
                    txn: Some(Transaction { steps: vec![] }),
                    await_signal: None,
                    retry: Some(StepRetry {
                        max_attempts: 5,
                        initial_retry_ms: 500,
                        max_retry_ms: 2_000,
                    }),
                    sleep_before_ms: Some(86_400_000),
                },
            ],
        };
        let txn = Mutation::new()
            .start_workflow(spec)
            .cancel_workflow("wf1")
            .build();
        assert_eq!(
            serde_json::to_value(&txn).unwrap(),
            json!({
                "steps": [
                    { "op": "startWorkflow",
                      "spec": {
                        "name": "drip",
                        "steps": [
                          { "txn": { "steps": [ { "op": "insert", "table": "workItems", "doc": { "title": "first" } } ] } },
                          { "txn": { "steps": [] },
                            "retry": { "maxAttempts": 5, "initialRetryMs": 500, "maxRetryMs": 2000 },
                            "sleepBeforeMs": 86400000 }
                        ]
                      } },
                    { "op": "cancelWorkflow", "id": "wf1" }
                ]
            })
        );
    }

    #[test]
    fn step_result_parses_workflow_id() {
        let wf: StepResult = serde_json::from_value(json!({"workflowId":"wf9"})).unwrap();
        assert!(matches!(wf, StepResult::WorkflowId { workflow_id } if workflow_id == "wf9"));
        // cancelWorkflow's `{"cancelled":<bool>}` result reuses the Cancelled
        // variant (same wire shape as cancelSchedule's).
        let cancelled: StepResult = serde_json::from_value(json!({"cancelled":false})).unwrap();
        assert!(matches!(
            cancelled,
            StepResult::Cancelled { cancelled: false }
        ));
    }

    #[test]
    fn await_signal_step_builder_serializes() {
        // The builder emits ONLY the awaitSignal key — no txn, no retry, no
        // sleepBeforeMs (absent optionals are skipped, corpus parity).
        let spec = WorkflowSpec {
            name: "gate".into(),
            steps: vec![WorkflowStepSpec::await_signal("approve", Some(3_600_000))],
        };
        assert_eq!(
            serde_json::to_value(&spec).unwrap(),
            json!({
                "name": "gate",
                "steps": [ { "awaitSignal": { "name": "approve", "timeoutMs": 3600000 } } ]
            })
        );
        // Indefinite wait omits timeoutMs.
        let spec = WorkflowSpec {
            name: "gate".into(),
            steps: vec![WorkflowStepSpec::await_signal("approve", None)],
        };
        assert_eq!(
            serde_json::to_value(&spec).unwrap(),
            json!({
                "name": "gate",
                "steps": [ { "awaitSignal": { "name": "approve" } } ]
            })
        );
    }

    #[test]
    fn undelete_serializes_and_round_trips() {
        // FM-33: `undelete` emits `{"op":"undelete","table":...,"id":...}` —
        // the same wire shape as `delete`, mirroring
        // `server/src/txn.rs::Step::Undelete` byte-for-byte. The step's result
        // is `null` (StepResult::Null — no new result variant).
        let txn = Mutation::new().undelete("projects", "p1").build();
        assert_eq!(
            serde_json::to_value(&txn).unwrap(),
            json!({
                "steps": [
                    {"op":"undelete","table":"projects","id":"p1"}
                ]
            })
        );
        // Round-trips back through the wire type.
        let back: Transaction =
            serde_json::from_value(serde_json::to_value(&txn).unwrap()).unwrap();
        assert!(matches!(
            back.steps.as_slice(),
            [Step::Undelete { table, id }] if table == "projects" && id == "p1"
        ));
    }
}
