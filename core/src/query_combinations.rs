//! Shared read-`Query` combination-rule evaluator (ENH-028 phase 2). Phase 1
//! (`wire-corpus/query-combinations.json` + one `wire-corpus/semantics/
//! query-combo-<id>.json` corpus case per rule) found zero behavioral drift
//! between the server's hand-written cascade (`server/src/query/mod.rs`'s
//! `GET_PEERS`/`UNIQUE_PEERS`/`*_INCOMPATIBLES`/`*_PEERS` tables, consumed by
//! `terminals.rs::compile_query`) and the Rust client's hand-written
//! `check_query_combinations` — this module replaces both with one
//! table-driven evaluator neither crate has to hand-maintain.
//!
//! The table has two rule shapes: `forbid` (every listed clause present at
//! once is rejected) and `atMostOne` (more than one listed clause present at
//! once is rejected — used only where the listed set is a full pairwise
//! clique, so it never wrongly forbids two clauses that ARE legal together).
//! `atMostOne` cliques (`terminal-exclusive`, `search-mode-exclusive`) are
//! phase 1 CONSOLIDATIONS of what used to be many individually-worded
//! pairwise checks (e.g. the pre-refactor `AGGREGATE_INCOMPATIBLES` table's
//! per-peer `"aggregate cannot be combined with X"` messages), so evaluation
//! checks every `forbid` rule (in declared order) before any `atMostOne`
//! rule (in declared order): a `forbid` rule's message is always at least as
//! specific as a clique's generic one, so it wins when both fire on the same
//! query. This tie-break only decides which `message` a multi-violation query
//! gets — accept/reject is a boolean OR over every rule regardless of
//! evaluation order — and the corpus asserts `code` only, per
//! `wire-corpus/README.md`'s determinism ruling 4, so it is not part of the
//! enforced contract. Every declared rule shares `code: "BAD_REQUEST"` today.
//!
//! Known gap (ENH-028 phase 2, filed against phase 1's table, NOT fixed
//! here): most pairs inside the `terminal-exclusive` clique
//! (`aggregate`/`count`/`distinct`/`first`/`get`/`paginate`/`take`/`unique`)
//! have no matching `forbid` rule in `wire-corpus/query-combinations.json`
//! and so fall through to the clique's generic `"only one terminal may be
//! set"` message instead of the pre-refactor per-peer wording — a
//! `code`-level no-op (still `BAD_REQUEST`) but a message regression.
//! Confirmed (2026-08-25, via a scratch probe against this evaluator, not
//! committed) missing pairs: `get`+`unique`/`distinct`/`aggregate`,
//! `unique`+`take`, `first`+`unique`/`take`, `count`+`unique`/`take`/`first`,
//! `aggregate`+`take`, `distinct`+`take`, `paginate`+`take`/`count`/`unique`/
//! `first` — only the `*-excludes-order` pairs (`order-excludes-unique`,
//! `count-excludes-order`, `distinct-excludes-order`,
//! `aggregate-excludes-order`) got an individual rule; every other
//! terminal-vs-terminal pair relies solely on the clique. This breaks four
//! pre-existing `rust-client` unit tests that assert the old per-peer
//! wording (`in_memory::tests::{aggregate::{aggregate,distinct}_rejects_conflicting_terminals,
//! paginate::paginate_rejects_combination_with_take_count_unique_or_first,
//! query::query_rejects_conflicting_terminals}`). Closing it means adding a
//! `forbid` rule per missing pair (or restoring per-peer messages some other
//! way) plus a `wire-corpus/semantics/query-combo-<id>.json` case per new
//! rule id (see `server/src/query/combinations_coverage.rs`'s coverage
//! guard) — out of this phase's touch scope:
//! `wire-corpus/query-combinations.json` is a cross-runner file four other
//! language agents are consuming concurrently.

use std::collections::HashSet;
use std::sync::LazyLock;

use serde::Deserialize;

/// `wire-corpus/query-combinations.json`, embedded at compile time so neither
/// crate needs a runtime file read (and so a missing/relocated file is a
/// build error, not a startup surprise).
static RULES_JSON: &str = include_str!("../../wire-corpus/query-combinations.json");

#[derive(Debug, Deserialize)]
struct RuleTable {
    rules: Vec<Rule>,
}

#[derive(Debug, Deserialize)]
struct Rule {
    #[serde(default)]
    forbid: Option<Vec<String>>,
    #[serde(default, rename = "atMostOne")]
    at_most_one: Option<Vec<String>>,
    code: String,
    message: String,
}

static RULES: LazyLock<RuleTable> = LazyLock::new(|| {
    let table: RuleTable = serde_json::from_str(RULES_JSON)
        .expect("wire-corpus/query-combinations.json must parse as a RuleTable");
    for rule in &table.rules {
        assert!(
            rule.forbid.is_some() != rule.at_most_one.is_some(),
            "wire-corpus/query-combinations.json: a rule must declare exactly one of \
             forbid/atMostOne"
        );
    }
    table
});

/// One violated query-combination rule. `code` mirrors the JSON table's
/// `code` (currently always `"BAD_REQUEST"`); `message` is the table's
/// canonical wording. This is a plain data seam, not either crate's error
/// type — `server`'s `RtDbError` and `rust-client`'s `RtDbError` are separate,
/// non-`core` types, so each caller translates a `RuleViolation` into its own
/// error at the call site, the same seam [`crate::engine::detect_destructive_changes`]
/// already uses for its `Result<(), String>`.
#[derive(Debug, Clone)]
pub struct RuleViolation {
    /// The rule's error code (currently always `"BAD_REQUEST"`).
    pub code: String,
    /// The rule's canonical human-readable message.
    pub message: String,
}

/// Evaluate every declared query-combination rule against `present` — the
/// wire-corpus canonical clause names (see the JSON table's `clauses` array,
/// e.g. `"get"`, `"index"`, `"vectorSearch"`) for every clause the query
/// actually sets — and return the first violated rule, or `Ok(())` if none
/// fire. Callers build `present` once per query (map each optional/boolean
/// `Query` field to its clause name) and call this once, replacing the
/// server's per-terminal peer-rejection cascade and the Rust client's
/// hand-written `check_query_combinations` ladder.
///
/// Evaluation order: every `forbid` rule (table declaration order), THEN
/// every `atMostOne` rule (table declaration order) — see the module doc's
/// tie-break rationale. This never changes accept/reject, only which
/// `message` a multi-violation query gets.
pub fn check_query_combinations(present: &HashSet<&str>) -> Result<(), RuleViolation> {
    let forbid_violation = RULES
        .rules
        .iter()
        .filter_map(|rule| rule.forbid.as_ref().map(|clauses| (rule, clauses)))
        .find(|(_, clauses)| clauses.iter().all(|c| present.contains(c.as_str())));
    if let Some((rule, _)) = forbid_violation {
        return Err(RuleViolation {
            code: rule.code.clone(),
            message: rule.message.clone(),
        });
    }

    let at_most_one_violation = RULES
        .rules
        .iter()
        .filter_map(|rule| rule.at_most_one.as_ref().map(|clauses| (rule, clauses)))
        .find(|(_, clauses)| {
            clauses
                .iter()
                .filter(|c| present.contains(c.as_str()))
                .count()
                > 1
        });
    if let Some((rule, _)) = at_most_one_violation {
        return Err(RuleViolation {
            code: rule.code.clone(),
            message: rule.message.clone(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_parses_and_is_nonempty() {
        assert!(!RULES.rules.is_empty());
    }

    #[test]
    fn empty_query_has_no_violation() {
        let present: HashSet<&str> = HashSet::new();
        assert!(check_query_combinations(&present).is_ok());
    }

    #[test]
    fn get_and_index_together_is_rejected() {
        let present: HashSet<&str> = ["get", "index"].into_iter().collect();
        assert!(check_query_combinations(&present).is_err());
    }

    #[test]
    fn index_and_eq_together_is_allowed() {
        let present: HashSet<&str> = ["index", "eq"].into_iter().collect();
        assert!(check_query_combinations(&present).is_ok());
    }

    #[test]
    fn two_terminals_at_once_is_rejected() {
        let present: HashSet<&str> = ["count", "distinct"].into_iter().collect();
        assert!(check_query_combinations(&present).is_err());
    }
}
