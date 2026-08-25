//! Mutation/transaction DSL wire types shared by the server and Rust client
//! (ARC-004 follow-up): `Transaction`, `Step`, `ScheduleWhen`, `ScheduleKind`,
//! `StepRetry`, `AwaitSignalSpec`, `WorkflowStepSpec`, `WorkflowSpec`, and
//! `WorkflowStatus`. These were two hand-kept copies — one in
//! `server/src/dsl.rs`/`server/src/protocol.rs`, one in
//! `rust-client/src/mutation.rs`/`rust-client/src/wire.rs` — verified
//! structurally identical (same variants, same fields, same serde tags)
//! before being moved here. Wire shapes use
//! `#[serde(tag = "op"/"type", rename_all = "camelCase", deny_unknown_fields)]`
//! and are mirrored field-for-field by the ts/python/swift client SDKs.
//!
//! `Step`/`Transaction` carry a `pub fn table()` on the server
//! (`server/src/dsl.rs`) that is NOT ported here as an inherent `impl` —
//! `Step` is a foreign type from each crate's point of view once re-exported,
//! so the orphan rule forces that helper into a per-crate extension trait at
//! its historical call site (see `server/src/dsl.rs::StepTableExt` and the
//! crate-private table helper in `rust-client/src/optimistic.rs`, which is a
//! genuinely different, ExpectAbsent-masking helper and was never a
//! duplicate).
//!
//! `StepResult` (the untagged step-result decode enum) and the chainable
//! `Mutation` builder are rust-client-only ergonomic sugar with no server
//! equivalent (the server only ever produces the raw JSON these decode) and
//! stay in `rust-client/src/mutation.rs`.

use crate::wire::FilterExpr;

/// One write/control step (tagged `{"op": "..."}`, camelCase).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", deny_unknown_fields)]
pub enum Step {
    /// Insert a new document; result is its id.
    Insert {
        /// Target table.
        table: String,
        /// The document body.
        doc: serde_json::Map<String, serde_json::Value>,
    },
    /// Merge `fields` into an existing document; result is `null`.
    Patch {
        /// Target table.
        table: String,
        /// Document id.
        id: String,
        /// Keys to merge in.
        fields: serde_json::Map<String, serde_json::Value>,
    },
    /// Overwrite the whole document; result is `null`.
    Replace {
        /// Target table.
        table: String,
        /// Document id.
        id: String,
        /// The full replacement body.
        doc: serde_json::Map<String, serde_json::Value>,
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
        eq: Vec<serde_json::Value>,
    },
    /// Insert-or-patch keyed by an index eq-prefix match.
    Upsert {
        /// Target table.
        table: String,
        /// Index whose eq-prefix locates the row.
        index: String,
        /// The eq-prefix values.
        eq: Vec<serde_json::Value>,
        /// Body applied when inserting.
        insert: serde_json::Map<String, serde_json::Value>,
        /// Keys merged when the row exists.
        patch: serde_json::Map<String, serde_json::Value>,
    },
    /// Find every row in `table` matching `filter` (the same `FilterExpr` the
    /// read path accepts) and apply `patch` to it, atomically, inside the
    /// serialized committer turn. Visibility matches the read path exactly: an
    /// interactive caller patches only rows they own/collaborate on and that
    /// satisfy the table's `authorize` predicate; a bypass principal (machine
    /// token/admin/scheduled) touches all matching rows. At most `limit` rows
    /// (default server cap 1000); a larger match set patches `limit` and
    /// reports `truncated: true`. Each patched row records a `DocOp`/`WriteSet`
    /// entry, so subscriptions, the op-feed, audit log, and webhooks all fire
    /// per row — the same contract as a per-id `Patch`.
    PatchByQuery {
        /// Target table.
        table: String,
        /// Which rows match.
        filter: FilterExpr,
        /// Keys to merge into every match.
        patch: serde_json::Map<String, serde_json::Value>,
        /// Row cap (server default/max 1000); `None` = server default.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u32>,
    },
    /// Find every row in `table` matching `filter` and delete it (same
    /// visibility/`limit`/`truncated` semantics as `PatchByQuery`). Enables
    /// server-side cascades and bulk cleanup (e.g. a scheduled job deleting
    /// expired rows by predicate) without a client-side read-all-then-delete.
    DeleteByQuery {
        /// Target table.
        table: String,
        /// Which rows match.
        filter: FilterExpr,
        /// Row cap (server default/max 1000); `None` = server default.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u32>,
    },
    /// Schedule `txn` to run later (FM-28). The `scheduled_txns` row is
    /// inserted on the OPEN sqlx transaction, so the enqueue commits (or
    /// rolls back) atomically with this txn's document writes. Step result is
    /// `{"scheduleId": "<id>"}`; the job fires through the unchanged
    /// scheduler → `RunScheduled` committer path as the system (bypass)
    /// principal. Nested steps are table-scope-checked recursively at enqueue
    /// (`authorize_txn_tables`) and fully re-validated by `execute_txn` at
    /// fire time.
    Schedule {
        /// One-shot delay/absolute time, a cron expression, or a fixed
        /// interval.
        when: ScheduleWhen,
        /// The nested transaction to fire when due.
        txn: Box<Transaction>,
    },
    /// Cancel a previously scheduled job by id, on the open sqlx transaction.
    /// Step result `{"cancelled": <bool>}` — `false` (not an error) when the
    /// id is missing, already fired, or already cancelled. A fire currently
    /// in flight completes; the job never fires again (the cron finalize
    /// update touches 0 rows).
    CancelSchedule {
        /// The schedule id to cancel.
        id: String,
    },
    /// Start a durable workflow run (FM-29). The `workflows` row is inserted on
    /// the OPEN sqlx transaction — "write doc + start drip" is atomic; a
    /// rolled-back txn leaves no orphan run. Step result `{"workflowId": "<id>"}`.
    /// The spec is validated and table-scope-checked recursively at submit time;
    /// steps fire later as the system (bypass) principal in the committer's
    /// `RunWorkflowAdvance` turn.
    StartWorkflow {
        /// The run's spec, snapshotted per run server-side.
        spec: Box<WorkflowSpec>,
    },
    /// Cancel a workflow run by id, on the open sqlx transaction. Step result
    /// `{"cancelled": <bool>}` — `false` when missing or already terminal. A run
    /// whose advance is in flight stops at its next step boundary.
    CancelWorkflow {
        /// The workflow run id.
        id: String,
    },
    /// Restore a soft-deleted row (FM-33): `UPDATE … SET deleted_at = NULL,
    /// version = version + 1`. `NotFound` when the row is absent; idempotent
    /// `Ok` when it is present and already live. Only legal on a table that
    /// declares `softDelete`. Patch-shaped `DocOp` — the doc re-appears, so
    /// content-bearing subscriptions re-run.
    Undelete {
        /// Target table (must declare `softDelete`).
        table: String,
        /// Document id.
        id: String,
    },
}

/// An ordered list of steps executed atomically by the server's committer.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Transaction {
    /// The steps, applied in order; any failure rolls the whole txn back.
    pub steps: Vec<Step>,
}

/// How a caller wants a transaction scheduled. Mirrored byte-for-byte in
/// `ts-client/src/protocol.ts`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum ScheduleWhen {
    /// Fire `ms` milliseconds from now.
    AfterMs {
        /// Milliseconds from now.
        ms: i64,
    },
    /// Fire at this UTC epoch-ms instant (in the past = fire immediately).
    RunAt {
        /// Absolute UTC epoch-ms instant.
        ms: i64,
    },
    /// Fire on this 5-field cron schedule (UTC, min-first). The server validates
    /// the expression; the client does no cron parsing.
    Cron {
        /// 5-field cron expression (UTC, min-first).
        expr: String,
    },
    /// Fire every `every_ms` milliseconds, starting one interval from now.
    /// Missed windows (downtime, pause) are skipped, never backfilled —
    /// each fire re-arms from its actual fire time, like cron recompute.
    Interval {
        /// The fixed recurrence in milliseconds.
        #[serde(rename = "everyMs")]
        every_ms: i64,
    },
}

/// Whether a scheduled job fires once (`ScheduleWhen::AfterMs`/`RunAt`) or
/// repeats (`Cron` on an expression, `Interval` every N ms). Closed domain —
/// was a free `String` (ARC-004/QA-008). Serializes as `"oneshot"` / `"cron"`
/// / `"interval"`, byte-identical to the prior stringly-typed bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleKind {
    /// Fires once.
    Oneshot,
    /// Repeats on a cron schedule.
    Cron,
    /// Repeats on a fixed interval.
    Interval,
}

impl ScheduleKind {
    /// The wire string (`"oneshot"` / `"cron"` / `"interval"`).
    pub fn as_wire_str(&self) -> &'static str {
        match self {
            ScheduleKind::Oneshot => "oneshot",
            ScheduleKind::Cron => "cron",
            ScheduleKind::Interval => "interval",
        }
    }
}

impl From<ScheduleKind> for &'static str {
    fn from(k: ScheduleKind) -> &'static str {
        k.as_wire_str()
    }
}

impl std::str::FromStr for ScheduleKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "oneshot" => Ok(ScheduleKind::Oneshot),
            "cron" => Ok(ScheduleKind::Cron),
            "interval" => Ok(ScheduleKind::Interval),
            other => Err(format!("unknown ScheduleKind: {other}")),
        }
    }
}

/// Per-step retry policy (FM-29). `maxAttempts` counts TOTAL attempts — the
/// first try included. Defaults when a step omits `retry`: 3 attempts, 1s
/// initial backoff doubling to a 60s cap.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StepRetry {
    /// TOTAL attempts including the first.
    pub max_attempts: u32,
    /// First backoff delay; doubles per retry.
    #[serde(default = "default_initial_retry_ms")]
    pub initial_retry_ms: u64,
    /// Backoff cap.
    #[serde(default = "default_max_retry_ms")]
    pub max_retry_ms: u64,
}

fn default_initial_retry_ms() -> u64 {
    1_000
}

fn default_max_retry_ms() -> u64 {
    60_000
}

impl Default for StepRetry {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_retry_ms: 1_000,
            max_retry_ms: 60_000,
        }
    }
}

/// An `awaitSignal` step's wait declaration (spec §Wire): park the run
/// until a signal named `name` is delivered; `timeoutMs` bounds each wait
/// attempt (omitted = wait indefinitely, cancel is the escape).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AwaitSignalSpec {
    /// The signal name to wait for.
    pub name: String,
    /// Per-attempt wait bound; `None` = wait indefinitely.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

/// One workflow step: either an ordinary `Transaction` or an
/// [`AwaitSignalSpec`] wait (exactly one — `validate_spec` enforces it),
/// plus policy. The txn may itself carry `Schedule`/`CancelSchedule` steps
/// (FM-28 rules apply).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkflowStepSpec {
    /// The step's transaction (`None` on an `awaitSignal` step).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub txn: Option<Transaction>,
    /// The wait declaration (`None` on a `txn` step).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub await_signal: Option<AwaitSignalSpec>,
    /// Per-step policy (server default 3/1s/60s when `None`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<StepRetry>,
    /// Gate the step until this many ms have passed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_before_ms: Option<u64>,
}

/// A submitted workflow definition. Stored verbatim per run — a run
/// snapshots its spec, so template edits never drift a live run.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkflowSpec {
    /// Run name (operator-facing).
    pub name: String,
    /// Ordered steps; snapshot per run.
    pub steps: Vec<WorkflowStepSpec>,
}

/// Run lifecycle (FM-29). Closed domain (ARC-004/QA-008 pattern — was never a
/// free string). Snake-case wire: pending|running|waiting|success|failed|cancelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus {
    /// Not yet advanced.
    Pending,
    /// Mid-run (or crashed mid-run, pre-recovery).
    Running,
    /// Parked on an `awaitSignal` step (non-terminal — a matching signal
    /// or cancel resumes/ends it).
    Waiting,
    /// All steps completed.
    Success,
    /// A step exhausted its retries (terminal).
    Failed,
    /// Cancelled by request (terminal).
    Cancelled,
}

impl WorkflowStatus {
    /// The wire string (`"pending"` etc.).
    pub fn as_wire_str(&self) -> &'static str {
        match self {
            WorkflowStatus::Pending => "pending",
            WorkflowStatus::Running => "running",
            WorkflowStatus::Waiting => "waiting",
            WorkflowStatus::Success => "success",
            WorkflowStatus::Failed => "failed",
            WorkflowStatus::Cancelled => "cancelled",
        }
    }
}

impl std::str::FromStr for WorkflowStatus {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(WorkflowStatus::Pending),
            "running" => Ok(WorkflowStatus::Running),
            "waiting" => Ok(WorkflowStatus::Waiting),
            "success" => Ok(WorkflowStatus::Success),
            "failed" => Ok(WorkflowStatus::Failed),
            "cancelled" => Ok(WorkflowStatus::Cancelled),
            other => Err(format!("unknown WorkflowStatus: {other}")),
        }
    }
}
