//! Skip evaluation (QA-003 split): whether a written table forces a
//! subscription re-run, given its `ReadSet` and the transaction's `WriteSet`.
//! Over-approximation is always safe (a needless re-run is diff-suppressed);
//! under-approximation is a missed realtime update, so every uncertain case
//! answers re-run.

use super::read_set::{ReadSet, indexed_affects, ordered_affects};
use crate::metrics::SkipClass;
use crate::txn::{DocValues, WriteSet};

/// What `fan_out` should do with one subscription whose table was written.
pub(super) enum Decision {
    Rerun,
    /// Skip: `class`'s read set proved every written document irrelevant.
    Skip(SkipClass),
}

/// Decide whether a subscription on `table` must re-run for this transaction.
/// The caller has already established that `table` was written.
///
/// Over-approximation is always safe (a needless re-run is diff-suppressed);
/// under-approximation is a missed realtime update, so every uncertain case
/// answers `Rerun`.
pub(super) fn decide(read_set: &ReadSet, table: &str, write_set: &WriteSet) -> Decision {
    let (class, irrelevant) = match read_set {
        // Today's coarse behavior: any write to the table re-runs.
        ReadSet::Table => return Decision::Rerun,
        // A `get(id)` point read depends only on its one document, so a write
        // that didn't touch it cannot change the result.
        ReadSet::Point { id } => (
            SkipClass::Point,
            !write_set.docs.contains(&(table.to_string(), id.clone()))
                && !table_touched_without_docs(write_set, table),
        ),
        // count / collect / unique: re-run only if some written doc crossed
        // the eq-prefix/range window boundary. Sound only because every
        // `EqBind` variant is comparable in `cmp_binds` — `Window::contains`
        // returns false on comparison doubt, which would UNDER-approximate.
        ReadSet::Indexed(indexed) => (
            SkipClass::Indexed,
            !any_written_affects(write_set, table, |values| indexed_affects(indexed, values)),
        ),
        // take / first / paginate: re-run only if some written doc is inside
        // the window AND ranks at or before the last result's boundary.
        ReadSet::Ordered(ordered) => (
            SkipClass::Ordered,
            !any_written_affects(write_set, table, |values| ordered_affects(ordered, values)),
        ),
    };
    if irrelevant {
        Decision::Skip(class)
    } else {
        Decision::Rerun
    }
}

/// True when `table` is touched in `write_set` with no per-doc pairs — a
/// table-level invalidation (push/restore backfill, schema reconcile) that
/// carries no window information. Same soundness rule as the capture-gap arm
/// below: no per-doc data ⇒ conservatively affecting, never a silent skip —
/// a backfilled doc's values are unknowable at decide time, so windowed and
/// point subscriptions on the table must re-run.
fn table_touched_without_docs(write_set: &WriteSet, table: &str) -> bool {
    write_set.tables.iter().any(|t| t == table) && !write_set.docs.iter().any(|(t, _)| t == table)
}

/// Whether any document written to `table` satisfies `affects`. A written
/// `(table, id)` with no `doc_values` entry (shouldn't happen — every `touch`
/// site also captures) counts as affecting, so a capture gap can never turn
/// into a missed push.
fn any_written_affects(
    write_set: &WriteSet,
    table: &str,
    mut affects: impl FnMut(&DocValues) -> bool,
) -> bool {
    if table_touched_without_docs(write_set, table) {
        return true;
    }
    for (doc_table, doc_id) in &write_set.docs {
        if doc_table != table {
            continue;
        }
        match write_set
            .doc_values
            .get(&(doc_table.clone(), doc_id.clone()))
        {
            None => return true,
            Some(values) => {
                if affects(values) {
                    return true;
                }
            }
        }
    }
    false
}
