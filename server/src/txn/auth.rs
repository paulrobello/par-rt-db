//! Write-path row authorization: check_owner, verify_authorize_doc, the authorize_*_tables table-scope checks, and row-visibility helpers (ARC-008 split; pure move from the former single-file `txn.rs`).

use sqlx::PgConnection;

use crate::auth::{PrincipalCtx, authorize_table};
use crate::db::now_ms;
use crate::ddl::pg_table;
use crate::dsl::{StepTableExt, filter_matches};
use crate::error::RtDbError;
use crate::scheduler;
use crate::schema::TableDef;
use crate::txn::{Step, StepCtx, Transaction, row_visible_to};

// ARC-202: the wire types this module used to define live in `dsl.rs` now;
// re-exported so every `crate::txn::` path (and the integration tests'
// `rtdb_server::txn::` paths) keep resolving unchanged.
/// Ownership + authorize pre-check for patch/replace/delete: fetches the doc
/// and rejects `Forbidden` when a user caller fails EITHER gate the table
/// declares — `ownerField`/`collaboratorsField` (OR-enforced by `row_visible_to`)
/// OR the `authorize` predicate (`filter_matches`). A table may declare both;
/// both must pass. A missing doc returns `Ok` (the subsequent do_* step reports
/// `NotFound`). Bypass caller (`ctx.user_id` None — machine/admin/scheduled) and
/// tables declaring neither gate: no-op.
pub(crate) async fn check_owner(
    conn: &mut PgConnection,
    pg_schema_name: &str,
    table_def: &TableDef,
    table_name: &str,
    id: &str,
    ctx: &PrincipalCtx,
) -> Result<(), RtDbError> {
    let owner_uid = row_auth_enforced_uid(table_def, ctx.user_id.as_deref());
    let authorize = table_def.authorize.as_ref();
    let user_is_some = ctx.user_id.is_some();
    // Neither gate applies (bypass caller, or table declares nothing) → no-op.
    if owner_uid.is_none() && !(authorize.is_some() && user_is_some) {
        return Ok(());
    }
    let table_ident = pg_table(table_name);
    // FM-33: a soft-deleted row is absent — the gate passes (Ok, missing) and
    // the subsequent do_* step reports `NotFound`, so per-row auth never turns
    // a soft-deleted row into a `Forbidden` oracle.
    let live_only = if table_def.soft_delete {
        " AND \"deleted_at\" IS NULL"
    } else {
        ""
    };
    let row: Option<(serde_json::Value,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT \"doc\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE \"id\" = $1{live_only}"
    )))
    .bind(id)
    .fetch_optional(&mut *conn)
    .await?;
    let Some((doc,)) = row else {
        return Ok(());
    };
    if let Some(uid) = owner_uid
        && !row_visible_to(
            &doc,
            table_def.owner_field.as_deref(),
            table_def.collaborators_field.as_deref(),
            uid,
        )
    {
        return Err(RtDbError::forbidden(format!(
            "document '{id}' is not accessible to the caller"
        )));
    }
    if let Some(authorize) = authorize
        && user_is_some
        && !filter_matches(&doc, authorize, ctx)
    {
        return Err(RtDbError::forbidden(format!(
            "document '{id}' is not accessible to the caller"
        )));
    }
    Ok(())
}

/// Ownership + authorize check on a doc already in hand (upsert update branch).
/// Same composition as `check_owner`: a user caller must pass both the
/// `ownerField`/`collaboratorsField` gate and the `authorize` predicate when
/// the table declares them. Bypass/no-gate: no-op.
pub(crate) fn check_owner_doc(
    table_def: &TableDef,
    doc: &serde_json::Map<String, serde_json::Value>,
    id: &str,
    ctx: &PrincipalCtx,
) -> Result<(), RtDbError> {
    let owner_uid = row_auth_enforced_uid(table_def, ctx.user_id.as_deref());
    let authorize = table_def.authorize.as_ref();
    let user_is_some = ctx.user_id.is_some();
    if owner_uid.is_none() && !(authorize.is_some() && user_is_some) {
        return Ok(());
    }
    let doc_value = serde_json::Value::Object(doc.clone());
    if let Some(uid) = owner_uid
        && !row_visible_to(
            &doc_value,
            table_def.owner_field.as_deref(),
            table_def.collaborators_field.as_deref(),
            uid,
        )
    {
        return Err(RtDbError::forbidden(format!(
            "document '{id}' is not accessible to the caller"
        )));
    }
    if let Some(authorize) = authorize
        && user_is_some
        && !filter_matches(&doc_value, authorize, ctx)
    {
        return Err(RtDbError::forbidden(format!(
            "document '{id}' is not accessible to the caller"
        )));
    }
    Ok(())
}

/// Boolean twin of [`check_owner_doc`]: `true` iff `doc` is visible to `ctx`
/// under the table's per-row gates (`ownerField`/`collaboratorsField` and/or
/// `authorize`). Used by the read-only `ExpectVersion`/`ExpectAbsent`
/// preconditions to close the existence/version side-channel — a non-visible
/// doc is treated as absent rather than rejected with `Forbidden`, because a
/// `Forbidden` would itself be a louder oracle ("exists, but not yours").
///
/// Keep in lockstep with all three txn.rs per-row gate sites: `check_owner`
/// (the async DB-fetching variant), `check_owner_doc`, and this function. Any
/// new per-row gate must land in all three. The read-scan path in `query.rs`
/// inlines the same `owner_field`/`collaborators_field`/`authorize` composition
/// (no shared helper is extracted today).
pub(crate) fn doc_visible_to(
    doc: &serde_json::Value,
    table_def: &TableDef,
    ctx: &PrincipalCtx,
) -> bool {
    let owner_uid = row_auth_enforced_uid(table_def, ctx.user_id.as_deref());
    let authorize = table_def.authorize.as_ref();
    let user_is_some = ctx.user_id.is_some();
    if owner_uid.is_none() && !(authorize.is_some() && user_is_some) {
        return true; // no gate applies (bypass caller, or table declares nothing)
    }
    let mut visible = true;
    if let Some(uid) = owner_uid
        && !row_visible_to(
            doc,
            table_def.owner_field.as_deref(),
            table_def.collaborators_field.as_deref(),
            uid,
        )
    {
        visible = false;
    }
    if let Some(authorize) = authorize
        && user_is_some
        && !filter_matches(doc, authorize, ctx)
    {
        visible = false;
    }
    visible
}

/// Returns the caller's uid when per-row authorization applies: the caller is a
/// user (`owner` is `Some`) AND the table declares `ownerField` and/or
/// `collaboratorsField`. Returns `None` for bypass callers (machine tokens,
/// scheduled jobs, admin) and tables that declare neither field.
fn row_auth_enforced_uid<'a>(table_def: &'a TableDef, owner: Option<&'a str>) -> Option<&'a str> {
    if table_def.owner_field.is_some() || table_def.collaborators_field.is_some() {
        owner
    } else {
        None
    }
}
/// Recursive table-scope check over every step in `txn`, including steps
/// nested inside `Schedule` payloads and inside `StartWorkflow` specs (via
/// [`authorize_spec_tables`], so a scheduled job cannot smuggle one either).
/// Runs at ENQUEUE time (the `Schedule` step here, and the standalone
/// Schedule-op surfaces) so a scoped machine token cannot smuggle a future
/// write into a forbidden table via a scheduled job. Bypass principals
/// (`tables = None` — admin/full-access/ interactive) are unaffected;
/// per-row rules are deliberately NOT pre-checked (rows change between
/// enqueue and fire; the firing job runs as the system principal —
/// documented behavior, see the FM-28 spec).
pub(crate) fn authorize_txn_tables(ctx: &PrincipalCtx, txn: &Transaction) -> Result<(), RtDbError> {
    for step in &txn.steps {
        if let Some(table) = step.table() {
            authorize_table(ctx, table)?;
        }
        if let Step::Schedule { txn, .. } = step {
            authorize_txn_tables(ctx, txn)?;
        }
        if let Step::StartWorkflow { spec } = step {
            authorize_spec_tables(ctx, spec)?;
        }
    }
    Ok(())
}

/// `Schedule` step: validate timing, recursively table-scope-check the
/// nested txn against the CURRENT caller, and insert the `scheduled_txns`
/// row on the open sqlx transaction — atomic with the enclosing txn's
/// document writes (FM-28). The row becomes visible at `tx.commit()`; the
/// scheduler's existing ≤2s poll picks it up from there.
pub(crate) async fn step_schedule(
    sctx: &mut StepCtx<'_>,
    when: &crate::protocol::ScheduleWhen,
    txn: &Transaction,
    external: bool,
) -> Result<(), RtDbError> {
    authorize_txn_tables(sctx.ctx, txn)?;
    let (kind, due_at, cron, every_ms, tz) = scheduler::resolve_when(when.clone(), now_ms())?;
    // SEC-001: the enqueuer identity rides the PrincipalCtx (filled by
    // `row_ctx` for a User, `None` for bypass/machine), so a `Schedule` step
    // inside a user's mutate enqueues the row AS THAT USER and the job fires
    // with their row rights. `None` keeps the bypass fire path.
    let id = scheduler::insert_on(
        sctx.tx,
        sctx.db,
        kind,
        due_at,
        txn,
        cron.as_deref(),
        every_ms,
        tz.as_deref(),
        external,
        sctx.ctx.enqueuer.as_ref(),
    )
    .await?;
    sctx.results.push(serde_json::json!({ "scheduleId": id }));
    Ok(())
}

/// `CancelSchedule` step: DELETE the job row on the open sqlx transaction.
/// `false` (not an error) when the id is missing/already-fired/already
/// cancelled — matching the standalone cancel op. A fire in flight
/// completes; the cron finalize update then touches 0 rows.
pub(crate) async fn step_cancel_schedule(
    sctx: &mut StepCtx<'_>,
    id: &str,
) -> Result<(), RtDbError> {
    let cancelled = scheduler::cancel_on(sctx.tx, sctx.db, id).await?;
    sctx.results
        .push(serde_json::json!({ "cancelled": cancelled }));
    Ok(())
}

/// Recursive table-scope check over every step txn in a workflow spec,
/// INCLUDING steps nested inside `Schedule` payloads (via
/// `authorize_txn_tables`). Runs at SUBMIT time on every start surface so a
/// scoped machine token cannot smuggle a future write into a forbidden table
/// via a workflow step that fires later as bypass.
pub(crate) fn authorize_spec_tables(
    ctx: &PrincipalCtx,
    spec: &crate::protocol::WorkflowSpec,
) -> Result<(), RtDbError> {
    for step in &spec.steps {
        // An awaitSignal step carries no txn and touches no tables.
        if let Some(txn) = &step.txn {
            authorize_txn_tables(ctx, txn)?;
        }
    }
    Ok(())
}

/// `StartWorkflow` step: validate the spec, recursively table-scope-check it
/// against the CURRENT caller, and insert the `workflows` row on the open
/// sqlx transaction — atomic with the enclosing txn's document writes
/// (FM-29). The scheduler's existing poll claims the row from `tx.commit()`;
/// steps fire later as the system (bypass) principal.
pub(crate) async fn step_start_workflow(
    sctx: &mut StepCtx<'_>,
    spec: &crate::protocol::WorkflowSpec,
) -> Result<(), RtDbError> {
    crate::workflows::validate_spec(spec)?;
    authorize_spec_tables(sctx.ctx, spec)?;
    // Clamp before the u64→i64 cast (the `workflows::insert` hazard): a
    // serde-accepted `sleepBeforeMs` above i64::MAX would wrap negative and
    // produce an instantly-due gate.
    let sleep_ms = spec.steps[0]
        .sleep_before_ms
        .unwrap_or(0)
        .min(i64::MAX as u64) as i64;
    let gate = now_ms().saturating_add(sleep_ms);
    // SEC-001: same enqueuer capture as `step_schedule` — a workflow started
    // from inside a user's mutate advances with the user's row rights.
    let id = crate::workflows::insert_on(sctx.tx, sctx.db, spec, gate, sctx.ctx.enqueuer.as_ref())
        .await?;
    sctx.results.push(serde_json::json!({ "workflowId": id }));
    Ok(())
}

/// `CancelWorkflow` step: flip the run row to `cancelled` on the open sqlx
/// transaction. `false` (not an error) when the id is missing or already
/// terminal; an advance in flight stops at its next step boundary.
pub(crate) async fn step_cancel_workflow(
    sctx: &mut StepCtx<'_>,
    id: &str,
) -> Result<(), RtDbError> {
    let cancelled = crate::workflows::cancel_on(sctx.tx, sctx.db, id).await?;
    sctx.results
        .push(serde_json::json!({ "cancelled": cancelled }));
    Ok(())
}
