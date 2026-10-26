//! Pure engine helpers shared by the server's Postgres-backed engine and the
//! Rust client's in-memory engine (ARC-004 part B, plus the mutation-DSL
//! follow-up). Each function here is a byte-for-byte port of what used to be
//! two hand-kept copies — one in `server/src/ddl.rs`/`server/src/migrate.rs`/
//! `server/src/txn.rs`, one in `rust-client/src/in_memory/migrate.rs`/
//! `rust-client/src/in_memory/mod.rs` — verified identical before being moved
//! here. [`worst_case_affected`] takes `max_by_query_rows` as a parameter
//! rather than a baked-in constant because each crate keeps its own copy of
//! that cap (`MAX_BY_QUERY_ROWS`) alongside its other budget constants.
//!
//! Several other functions that LOOK like duplicates deliberately stay put in
//! each crate because they carry real behavioral divergence (not just a
//! rename): see the doc comments on `server/src/schema/value.rs::validate_value`
//! and `rust-client/src/in_memory/validate.rs::validate_value` (the `Int64`
//! and `Bytes` arms accept different inputs — a leading `+` on an int64
//! string, and base64 shape-checking vs actually decoding), and
//! `server/src/value_expr.rs::eval_value_expr` vs
//! `rust-client/src/in_memory/value_expr.rs::eval_value_expr` (and their
//! callers `stamp_computed`/`apply_patch`/`validate_doc`), which take a
//! `PrincipalCtx` on the server vs a plain field map on the client.
//!
//! [`detect_destructive_changes`] returns a plain `Result<(), String>` rather
//! than either crate's `RtDbError` — the two error envelopes are themselves
//! separate per-crate types (not yet unified), so each caller translates the
//! message into its own error type at the call site with a one-line
//! `.map_err(...)`.

use crate::mutation::{Step, Transaction};
use crate::schema::{SchemaDef, is_type_change_rejected};
use crate::wire::{FilterExpr, ValueExpr};

/// Validates this transaction's deadline guards and returns the earliest one.
/// Scheduled/workflow child transactions have independent execution boundaries.
pub fn transaction_deadline(txn: &Transaction) -> Result<Option<i64>, String> {
    let mut earliest: Option<i64> = None;
    for step in &txn.steps {
        if let Step::ExpectBefore { deadline_ms } = step {
            if !(0..=9_007_199_254_740_991).contains(deadline_ms) {
                return Err("deadlineMs must be a nonnegative safe integer".into());
            }
            earliest = Some(earliest.map_or(*deadline_ms, |value| value.min(*deadline_ms)));
        }
    }
    Ok(earliest)
}

/// Compares `old` to `new` and rejects any destructive change: a removed
/// table, a removed field, a changed field type (except a safe literal-union
/// widening — see [`is_widening_of`]), a removed index, or a changed index
/// field list/kind/uniqueness/partial-predicate/language. `Err` names the
/// offending table, `table.field`, or index; `Ok(())` means the push is
/// additive-only. FM-33: field types are compared with every `Id.on_delete`
/// action stripped ([`strip_on_delete`]) — adding or changing an `onDelete`
/// action alters runtime delete behavior only (no stored row shape), so it is
/// additive, while changing the referenced table is still a type change. The
/// `softDelete` flag is deliberately NOT compared.
pub fn detect_destructive_changes(old: &SchemaDef, new: &SchemaDef) -> Result<(), String> {
    for (table_name, old_table) in &old.tables {
        let new_table = new
            .tables
            .get(table_name)
            .ok_or_else(|| format!("removed table '{table_name}'"))?;

        for (field_name, old_field_type) in &old_table.fields {
            match new_table.fields.get(field_name) {
                None => {
                    return Err(format!("removed field '{table_name}.{field_name}'"));
                }
                Some(new_field_type) if is_type_change_rejected(old_field_type, new_field_type) => {
                    return Err(format!("changed type of field '{table_name}.{field_name}'"));
                }
                _ => {}
            }
        }

        for old_index in &old_table.indexes {
            let new_index = new_table
                .indexes
                .iter()
                .find(|index| index.name == old_index.name);
            let new_index = match new_index {
                None => {
                    return Err(format!("removed index '{}'", old_index.name));
                }
                Some(i) => i,
            };
            if new_index.fields != old_index.fields {
                return Err(format!("changed fields of index '{}'", old_index.name));
            }
            if new_index.search != old_index.search {
                return Err(format!(
                    "changed kind of index '{}' (btree <-> search)",
                    old_index.name
                ));
            }
            if new_index.vector != old_index.vector {
                return Err(format!("changed vector spec of index '{}'", old_index.name));
            }
            if new_index.unique != old_index.unique {
                return Err(format!("changed uniqueness of index '{}'", old_index.name));
            }
            if new_index.r#where != old_index.r#where {
                return Err(format!(
                    "changed partial predicate of index '{}'",
                    old_index.name
                ));
            }
            // A search index's `regconfig` is baked into a STORED generated
            // column whose expression Postgres cannot alter in place, so a
            // language change is a breaking index change (reject, like a
            // vector-spec change) rather than a silent no-op.
            if new_index.language != old_index.language {
                return Err(format!(
                    "changed language of search index '{}'",
                    old_index.name
                ));
            }
        }
    }
    Ok(())
}

/// FM-30: grandfathers `trgm` on `new` for any search index that was already
/// a search index in `old` (i.e. predates the `trgm` flag) and did not
/// explicitly declare `trgm` on this push. Mutates `new` in place; a no-op
/// when `old` is `None` (first-ever push — nothing to grandfather) or for a
/// table/index absent from `old` (a genuinely new search index is opt-in, as
/// designed). Idempotent: re-running it once already grandfathered changes
/// nothing.
///
/// Called before [`detect_destructive_changes`] (which deliberately does
/// NOT compare `trgm` — a create-time-only flag, like `softDelete`) so old
/// and new always agree on it by the time that comparison runs, and before
/// persisting `new` as the stored schema, so the grandfather is durable
/// rather than recomputed at every query/DDL site.
pub fn grandfather_trgm(old: Option<&SchemaDef>, new: &mut SchemaDef) {
    let Some(old) = old else { return };
    for (table_name, new_table) in new.tables.iter_mut() {
        let Some(old_table) = old.tables.get(table_name) else {
            continue;
        };
        for new_index in new_table.indexes.iter_mut() {
            if new_index.search && !new_index.trgm {
                let was_search = old_table
                    .indexes
                    .iter()
                    .any(|old_index| old_index.name == new_index.name && old_index.search);
                if was_search {
                    new_index.trgm = true;
                }
            }
        }
    }
}

/// Rewrite every `field` reference in `expr` that equals `from` to `to`, in
/// place. Used by the `RenameField` migration directive to carry an
/// `authorize` predicate (or any other `FilterExpr` the schema carries)
/// across a field rename. Recurses through `And`/`Or`/`Not`.
pub fn rename_filter_fields(expr: &mut FilterExpr, from: &str, to: &str) {
    match expr {
        FilterExpr::Eq { field, .. }
        | FilterExpr::Neq { field, .. }
        | FilterExpr::Gt { field, .. }
        | FilterExpr::Gte { field, .. }
        | FilterExpr::Lt { field, .. }
        | FilterExpr::Lte { field, .. }
        | FilterExpr::In { field, .. }
        | FilterExpr::Contains { field, .. }
        | FilterExpr::Exists { field }
        | FilterExpr::OlderThan { field, .. } => {
            if field == from {
                *field = to.to_string();
            }
        }
        FilterExpr::And { exprs } | FilterExpr::Or { exprs } => {
            for e in exprs {
                rename_filter_fields(e, from, to);
            }
        }
        FilterExpr::Not { expr } => rename_filter_fields(expr, from, to),
    }
}

/// The `ValueExpr` half of the rename: rewrites every `Field` reference equal
/// to `from` to `to`, in place — the `&mut` mirror of
/// `fields::walk_value_expr_fields`. `Case.whens` predicates reuse
/// [`rename_filter_fields`] (the same rewrite `authorize` gets), so a rename
/// carries computed expressions across intact. `to` is fresh (the
/// `RenameField` directive rejects an existing target), so no reference set
/// can collide.
pub fn rename_value_expr_fields(expr: &mut ValueExpr, from: &str, to: &str) {
    match expr {
        ValueExpr::Field { field } => {
            if field == from {
                *field = to.to_string();
            }
        }
        ValueExpr::Literal { .. } | ValueExpr::Now => {}
        ValueExpr::Concat { parts } | ValueExpr::Coalesce { parts } => {
            for p in parts {
                rename_value_expr_fields(p, from, to);
            }
        }
        ValueExpr::Add { left, right }
        | ValueExpr::Sub { left, right }
        | ValueExpr::Mul { left, right }
        | ValueExpr::Div { left, right } => {
            rename_value_expr_fields(left, from, to);
            rename_value_expr_fields(right, from, to);
        }
        ValueExpr::Lower { value } | ValueExpr::Upper { value } | ValueExpr::Trim { value } => {
            rename_value_expr_fields(value, from, to);
        }
        ValueExpr::Cast { value, .. } => rename_value_expr_fields(value, from, to),
        ValueExpr::Case { whens, otherwise } => {
            for cw in whens {
                rename_filter_fields(&mut cw.when, from, to);
                rename_value_expr_fields(&mut cw.then, from, to);
            }
            rename_value_expr_fields(otherwise, from, to);
        }
    }
}

/// FM-28/FM-29: recursive step count — a `Schedule` step counts as itself
/// plus every step in its nested txn, and a `StartWorkflow` step counts as
/// itself plus the sum of its spec's step txns (an `awaitSignal` step carries
/// no txn, so it nests nothing). Bounds one request body's serialized size
/// against each crate's `MAX_STEPS` cap and blocks the nesting bomb (N steps
/// each scheduling N steps) — by-query caps are NOT applied to nested txns
/// here: the nested txn executes in a future committer turn and is
/// re-validated fully at fire time. ARC-004 follow-up: a byte-for-byte port
/// of what used to be two hand-kept copies, `server/src/txn.rs` and
/// `rust-client/src/in_memory/mod.rs` (the two prior implementations differed
/// in code shape — a `map().sum()` vs an accumulator loop — but were verified
/// behaviorally identical before being unified into this one shape).
pub fn count_steps(txn: &Transaction) -> usize {
    txn.steps
        .iter()
        .map(|step| match step {
            Step::Schedule { txn, .. } => 1 + count_steps(txn),
            Step::StartWorkflow { spec } => {
                1 + spec
                    .steps
                    .iter()
                    .map(|s| s.txn.as_ref().map_or(0, count_steps))
                    .sum::<usize>()
            }
            _ => 1,
        })
        .sum()
}

/// SEC-104: total documents `txn` could touch in the worst case. Per-id
/// steps (`Insert`/`Patch`/`Replace`/`Delete`/`ExpectVersion`/
/// `ExpectAbsent`/`Upsert`/`Undelete`) touch at most one each;
/// `Schedule`/`CancelSchedule`/`StartWorkflow`/`CancelWorkflow` count 0
/// (control-flow steps touch no documents); each `PatchByQuery`/
/// `DeleteByQuery` step touches up to its `limit`, capped at
/// `max_by_query_rows` (the server's `MAX_BY_QUERY_ROWS` / the Rust client's
/// mirror of the same cap — threaded in rather than baked in here since each
/// crate keeps its own copy of the constant). The estimate is an
/// over-approximation — the actual count is lower when fewer rows match —
/// and must never under-approximate (that would weaken both crates' budget
/// checks: the server's `MAX_AFFECTED_ROWS_PER_TXN` guard in `execute_txn`
/// and the admin `max_affected_docs` guardrail, and the in-memory engine's
/// mirror of the same check). ARC-004 follow-up: a byte-for-byte port of
/// what used to be two hand-kept copies, `server/src/txn.rs` and
/// `rust-client/src/in_memory/mod.rs`.
pub fn worst_case_affected(txn: &Transaction, max_by_query_rows: u32) -> usize {
    txn.steps
        .iter()
        .map(|step| match step {
            Step::PatchByQuery { limit, .. } | Step::DeleteByQuery { limit, .. } => {
                (*limit).unwrap_or(max_by_query_rows).min(max_by_query_rows) as usize
            }
            Step::InsertMany { docs, .. } | Step::UpsertMany { docs, .. } => docs.len(),
            Step::ExpectBefore { .. }
            | Step::ExpectNoMatch { .. }
            | Step::Schedule { .. }
            | Step::CancelSchedule { .. }
            | Step::StartWorkflow { .. }
            | Step::CancelWorkflow { .. } => 0,
            _ => 1,
        })
        .sum()
}

#[cfg(test)]
mod deadline_tests {
    use super::*;

    #[test]
    fn deadlines_are_safe_exclusive_epoch_milliseconds_and_touch_no_documents() {
        let txn = Transaction {
            steps: vec![Step::ExpectBefore { deadline_ms: 1001 }],
        };
        assert_eq!(transaction_deadline(&txn).unwrap(), Some(1001));
        assert_eq!(worst_case_affected(&txn, 1000), 0);
        for deadline_ms in [-1, 9_007_199_254_740_992, i64::MAX] {
            assert!(
                transaction_deadline(&Transaction {
                    steps: vec![Step::ExpectBefore { deadline_ms }]
                })
                .is_err()
            );
        }
    }

    #[test]
    fn only_this_execution_boundary_contributes_deadlines() {
        let child = Transaction {
            steps: vec![Step::ExpectBefore { deadline_ms: 1 }],
        };
        let txn = Transaction {
            steps: vec![
                Step::ExpectBefore { deadline_ms: 1002 },
                Step::ExpectBefore { deadline_ms: 1001 },
                Step::Schedule {
                    when: crate::mutation::ScheduleWhen::AfterMs { ms: 1 },
                    txn: Box::new(child),
                    external: None,
                },
            ],
        };
        assert_eq!(transaction_deadline(&txn).unwrap(), Some(1001));
        assert_eq!(
            transaction_deadline(&Transaction { steps: vec![] }).unwrap(),
            None
        );
    }

    #[test]
    fn deadline_step_round_trips_with_exact_field_names_and_rejects_unknown_fields() {
        let value = serde_json::json!({ "op": "expectBefore", "deadlineMs": 1001 });
        let step: Step = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(step).unwrap(), value);
        assert!(
            serde_json::from_value::<Step>(
                serde_json::json!({ "op": "expectBefore", "deadlineMs": 1001, "extra": true })
            )
            .is_err()
        );
        assert!(
            serde_json::from_value::<Step>(serde_json::json!({ "op": "expectBefore" })).is_err()
        );
    }
}
