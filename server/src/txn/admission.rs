//! Transaction admission — every check `execute_txn_with_side` runs before
//! the step loop (QA-001). Three seams, in execution order:
//!
//! 1. [`validate_static_admission`] — pure, no I/O: db-name validation, the
//!    recursive step cap, the by-query step-count cap, and the aggregate
//!    affected-document budget (SEC-104). An over-cap txn never opens a
//!    transaction.
//! 2. [`check_quiesce_admission`] — the pre-transaction quiesce fence;
//!    captures the quiesce generation the write is admitted under.
//! 3. [`run_mutate_preamble`] — inside the write transaction: advisory-lock
//!    and admission-epoch acquisition, then the ENH-048 combined
//!    `set_config` preamble (statement timeout, read-only freeze, quiesce
//!    generation, idempotency-cache lookup — all in the BEGIN →
//!    first-statement round trip the turn already paid). Returns the
//!    [`TxnAdmission`] verdict.

use sqlx::{PgConnection, PgPool};

use crate::db::{now_ms, quiesce_key};
use crate::dsl::{Step, Transaction};
use crate::error::RtDbError;

use super::{
    MAX_AFFECTED_ROWS_PER_TXN, MAX_BY_QUERY_STEPS_PER_TXN, MAX_STEPS, MutatePreamble,
    STATEMENT_TIMEOUT_MS, count_steps, worst_case_affected,
};

/// The admission verdict for one write turn (QA-001). `execute_txn_with_side`
/// matches it right after [`run_mutate_preamble`]: `Replay` returns the
/// stored outcome, `ReadOnly` surfaces `READ_ONLY`, `Admitted` falls through
/// to the step loop. On both non-`Admitted` arms the caller must
/// `tx.rollback()` before returning — the caller owns the transaction, so the
/// rollback stays with the ownership; nothing commits on any rejected path.
pub(crate) enum TxnAdmission {
    /// Idempotency-cache hit: a previous execution already committed under
    /// this key. Replay these stored results verbatim instead of executing.
    Replay(Vec<serde_json::Value>),
    /// Fenced off: read-only freeze, quiesce, or a quiesce-generation change
    /// behind the admission's back. Surface `READ_ONLY`.
    ReadOnly,
    /// Every admission check passed; the transaction is live and the step
    /// loop may execute. Carries no payload: the quiesce generation the
    /// write was admitted under is consumed *inside*
    /// [`run_mutate_preamble`] (a mismatch converts to `ReadOnly`), so
    /// nothing downstream of admission reads it.
    Admitted,
}

/// The pure pre-transaction admission guards: database-name validation, the
/// recursive step cap ([`MAX_STEPS`]), the by-query step-count cap, and the
/// aggregate affected-document budget (both SEC-104). No I/O — checked
/// before the transaction opens so an over-cap txn never begins, let alone
/// partially commits.
pub(crate) fn validate_static_admission(txn: &Transaction, db: &str) -> Result<(), RtDbError> {
    crate::db::validate_db_name(db)?;

    if count_steps(txn) > MAX_STEPS {
        return Err(RtDbError::bad_request(format!(
            "transaction exceeds maximum of {MAX_STEPS} steps  \
        (counted recursively, including scheduled txns)"
        )));
    }

    // SEC-104: compose the per-step caps into an aggregate affected-document
    // budget and a by-query step-count cap, both checked BEFORE any step
    // executes so an over-cap txn commits nothing. Commit 82650c2 introduced
    // by-query steps AND raised MAX_STEPS in the same change; without these
    // composite caps the worst-case committer turn was ~1,000,000 rows — a
    // single `/api/mutate` could stall the single-writer for a database and
    // starve every other writer/subscription on it. The by-query step cap is
    // the sharp bound; the affected-row budget is the blast-radius bound.
    let by_query_steps = txn
        .steps
        .iter()
        .filter(|s| {
            matches!(
                s,
                Step::PatchByQuery { .. } | Step::DeleteByQuery { .. } | Step::ExpectNoMatch { .. }
            )
        })
        .count();
    if by_query_steps > MAX_BY_QUERY_STEPS_PER_TXN {
        return Err(RtDbError::bad_request(format!(
            "transaction has {by_query_steps} by-query steps,  \
        exceeding the limit of {MAX_BY_QUERY_STEPS_PER_TXN}"
        )));
    }
    let worst = worst_case_affected(txn);
    if worst > MAX_AFFECTED_ROWS_PER_TXN {
        return Err(RtDbError::bad_request(format!(
            "transaction could affect up to {worst} documents,  \
        exceeding the limit of {MAX_AFFECTED_ROWS_PER_TXN}"
        )));
    }
    Ok(())
}

/// The pre-transaction quiesce admission fence (the cutover barrier). A
/// quiesced database rejects the write with `READ_ONLY` before a transaction
/// opens; an open one returns the quiesce generation the write is admitted
/// under — [`run_mutate_preamble`] re-checks that generation inside the
/// transaction and rejects if it moved behind the admission's back.
/// `check_quiesce = false` (no fence requested) yields `None`.
pub(crate) async fn check_quiesce_admission(
    pool: &PgPool,
    db: &str,
    check_quiesce: bool,
) -> Result<Option<i64>, RtDbError> {
    if !check_quiesce {
        return Ok(None);
    }
    let (quiesced, generation) = crate::db::quiesce_admission(pool, db).await?;
    if quiesced {
        return Err(RtDbError::read_only());
    }
    Ok(Some(generation))
}

/// The in-transaction preamble: takes the per-database advisory xact lock,
/// fences on the transport's admission epoch, and runs the first statement —
/// which (combined branch) answers the statement-timeout `set_config`, the
/// read-only freeze flag, the quiesce state, and the idempotency-cache
/// lookup in the single BEGIN → first-statement round trip the turn already
/// paid (ENH-048). See [`TxnAdmission`] for the verdict semantics.
pub(crate) async fn run_mutate_preamble(
    tx: &mut PgConnection,
    db: &str,
    pg_schema_name: &str,
    preamble: MutatePreamble<'_>,
    admission_generation: Option<i64>,
) -> Result<TxnAdmission, RtDbError> {
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(quiesce_key(db))
        .execute(&mut *tx)
        .await?;
    if let Some(epoch) = preamble.admission_epoch {
        crate::db::ensure_admission_epoch(tx, db, epoch).await?;
    }
    // SEC-104: bound every statement in this committer turn. A pathological
    // scan (e.g. a filter over an unindexed field that escapes the row budget)
    // aborts this transaction rather than stalling the single-writer for the
    // whole database. `set_config(..., true)` has the same transaction scope as
    // `SET LOCAL` — it reverts on commit/rollback, never leaking to other pool
    // users. The value is a const, never user input.
    //
    // ENH-048: the freeze flag and the idempotency lookup ride this first
    // statement instead of costing separate pre-transaction round trips.
    // Ordering is the documented ruling: a cached result is an idempotent
    // replay (rollback, return the cached results) BEFORE the freeze check;
    // only a non-replayed write sees `READ_ONLY`. Both paths roll back —
    // nothing commits. System arms (`check_freeze = false`, no key) fall
    // back to the plain set_config so the combined query is not wasted on
    // subselects they would ignore.
    if preamble.idem_key.is_none() && !preamble.check_freeze && admission_generation.is_none() {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "SET LOCAL statement_timeout = {STATEMENT_TIMEOUT_MS}"
        )))
        .execute(&mut *tx)
        .await?;
        return Ok(TxnAdmission::Admitted);
    }
    let combined = format!(
        "SELECT set_config('statement_timeout', $1, true), \
         COALESCE((SELECT read_only FROM rtdb_auth.databases WHERE name = $2), FALSE), \
         COALESCE((SELECT quiesced FROM rtdb_auth.databases WHERE name = $2), FALSE), \
         COALESCE((SELECT quiesce_generation FROM rtdb_auth.databases WHERE name = $2), 0), \
         (SELECT result FROM \"{pg_schema_name}\".mutation_cache_v2 \
          WHERE mut_id = $3 AND expires_at > $4)"
    );
    let row: (String, bool, bool, i64, Option<serde_json::Value>) =
        sqlx::query_as(sqlx::AssertSqlSafe(combined))
            .bind(STATEMENT_TIMEOUT_MS.to_string())
            .bind(db)
            .bind(preamble.idem_key)
            .bind(now_ms())
            .fetch_one(&mut *tx)
            .await?;
    if let Some(value) = row.4 {
        let results: Vec<serde_json::Value> = serde_json::from_value(value).map_err(|err| {
            tracing::error!(error = %err, db, "failed to deserialize cached mutation result");
            RtDbError::internal("failed to read cached mutation result")
        })?;
        return Ok(TxnAdmission::Replay(results));
    }
    // The freeze exemption is client-plane only (ENH-048): system
    // arms (`check_freeze = false`) ignore `read_only` exactly as
    // they did when this branch was reserved for client arms. The
    // quiesce fence is the cutover barrier and stops system writes
    // too, as does any generation change behind the admission's back.
    let frozen_client_write = row.1 && preamble.check_freeze;
    if frozen_client_write
        || row.2
        || admission_generation.is_some_and(|generation| row.3 != generation)
    {
        return Ok(TxnAdmission::ReadOnly);
    }
    Ok(TxnAdmission::Admitted)
}
