//! ENH-028 phase 1: coverage guard for `wire-corpus/query-combinations.json`.
//!
//! This does NOT validate the rule table's semantics (that is phase 2's
//! `combinations.rs` evaluator, which does not exist yet — the five checkers
//! in `terminals.rs`/`mod.rs` and the four client engines still enforce these
//! rules by hand). It only guards the TABLE ITSELF: every rule id declared in
//! `query-combinations.json` must be exercised by at least one
//! `wire-corpus/semantics/query-combo-<rule-id>.json` case, so a rule added to
//! the table without a pinning case fails loudly here instead of silently
//! drifting out of the corpus.

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs;
    use std::path::PathBuf;

    use serde_json::Value;

    fn workspace_root() -> PathBuf {
        PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/.."))
    }

    /// Every rule `id` in `wire-corpus/query-combinations.json` must have a
    /// matching `wire-corpus/semantics/query-combo-<id>.json` case file. This
    /// is a naming-convention check, not a semantic one: it does not run the
    /// case, only confirms it exists, so it catches "table grew, corpus
    /// didn't" without needing a live database.
    #[test]
    fn every_rule_has_a_semantics_case() {
        let root = workspace_root();
        let table_path = root.join("wire-corpus/query-combinations.json");
        let table_raw = fs::read_to_string(&table_path)
            .unwrap_or_else(|e| panic!("read {}: {e}", table_path.display()));
        let table: Value = serde_json::from_str(&table_raw)
            .unwrap_or_else(|e| panic!("parse {}: {e}", table_path.display()));

        let rules = table
            .get("rules")
            .and_then(Value::as_array)
            .unwrap_or_else(|| panic!("{}: missing `rules` array", table_path.display()));
        assert!(
            !rules.is_empty(),
            "{}: `rules` must not be empty",
            table_path.display()
        );

        let rule_ids: BTreeSet<String> = rules
            .iter()
            .map(|r| {
                r.get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_else(|| panic!("a rule in {} is missing `id`", table_path.display()))
                    .to_string()
            })
            .collect();
        assert_eq!(
            rule_ids.len(),
            rules.len(),
            "{}: rule ids must be unique",
            table_path.display()
        );

        let semantics_dir = root.join("wire-corpus/semantics");
        let entries = fs::read_dir(&semantics_dir)
            .unwrap_or_else(|e| panic!("read {}: {e}", semantics_dir.display()));
        let case_stems: BTreeSet<String> = entries
            .map(|e| e.expect("read dir entry").path())
            .filter(|p| p.extension().is_some_and(|x| x == "json"))
            .map(|p| {
                p.file_stem()
                    .expect("file stem")
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();

        let mut missing = Vec::new();
        for id in &rule_ids {
            let expected_stem = format!("query-combo-{id}");
            if !case_stems.contains(&expected_stem) {
                missing.push(expected_stem);
            }
        }
        assert!(
            missing.is_empty(),
            "rule id(s) with no matching wire-corpus/semantics/query-combo-<id>.json case: {missing:?}"
        );
    }

    /// Every `query-combo-*.json` case must correspond to a declared rule —
    /// guards the other direction (a stale case left behind after a rule is
    /// renamed or removed from the table).
    #[test]
    fn every_query_combo_case_maps_to_a_rule() {
        let root = workspace_root();
        let table_path = root.join("wire-corpus/query-combinations.json");
        let table_raw = fs::read_to_string(&table_path)
            .unwrap_or_else(|e| panic!("read {}: {e}", table_path.display()));
        let table: Value = serde_json::from_str(&table_raw)
            .unwrap_or_else(|e| panic!("parse {}: {e}", table_path.display()));
        let rule_ids: BTreeSet<String> = table["rules"]
            .as_array()
            .expect("rules array")
            .iter()
            .map(|r| r["id"].as_str().expect("rule id").to_string())
            .collect();

        let semantics_dir = root.join("wire-corpus/semantics");
        let entries = fs::read_dir(&semantics_dir)
            .unwrap_or_else(|e| panic!("read {}: {e}", semantics_dir.display()));
        let mut orphaned = Vec::new();
        for entry in entries {
            let path = entry.expect("read dir entry").path();
            if path.extension().is_none_or(|x| x != "json") {
                continue;
            }
            let stem = path
                .file_stem()
                .expect("file stem")
                .to_string_lossy()
                .into_owned();
            let Some(rule_id) = stem.strip_prefix("query-combo-") else {
                continue;
            };
            if !rule_ids.contains(rule_id) {
                orphaned.push(stem);
            }
        }
        assert!(
            orphaned.is_empty(),
            "wire-corpus/semantics case(s) named query-combo-* with no matching rule id in \
             query-combinations.json (stale after a rename/removal?): {orphaned:?}"
        );
    }
}
