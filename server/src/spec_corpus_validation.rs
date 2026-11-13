//! ENH-052 family 5 — the push-schema validation spec-table corpus
//! (`wire-corpus/spec/validation.json`). The server is the single source:
//! every row's outcome is computed by running this crate's own
//! `SchemaDefExt::validate` over the row's wire schema fragment (the input
//! matrix lives in `spec_corpus_validation_rows.json`), so a rule change
//! flips the pinned row automatically. The runner additions in the six
//! clients push each fragment through their own `pushSchema` path and assert
//! the same accept/reject outcome with the same code.
//!
//! `server/src/bin/gen-spec-validation.rs` writes the fixture;
//! `make gen-spec-validation-check` (in `checkall`) diffs the committed
//! fixture against a fresh generation.

use serde::de::Error as DeError;
use serde_json::{Value, json};

use crate::schema::{SchemaDef, SchemaDefExt};

/// The rule families the matrix enumerates — the drift net's list. A new
/// validation rule family needs an entry here (a compile error until the
/// list names it), a `rule` spelling in the input matrix, and rows whose
/// outcomes the server computes.
const RULE_FAMILIES: [&str; 8] = [
    "index",
    "ttl",
    "updated-at",
    "auto-increment",
    "on-delete",
    "constraints",
    "computed",
    "accept",
];

/// The committed input matrix.
const INPUT_ROWS: &str = include_str!("spec_corpus_validation_rows.json");

/// One fixture row per input row: the wire schema fragment and the outcome
/// the server's own validator produces — `{"accepts": true}` or
/// `{"error": {"code": <wire code>}}`.
fn validation_spec_fixture_rows() -> Result<Vec<Value>, serde_json::Error> {
    fn bad_input(msg: String) -> serde_json::Error {
        serde_json::Error::custom(msg)
    }
    let doc: Value = serde_json::from_str(INPUT_ROWS).map_err(|e| bad_input(e.to_string()))?;
    let rows = doc["rows"]
        .as_array()
        .ok_or_else(|| bad_input("input matrix has no rows array".to_string()))?;

    // The drift net: every rule family must appear in the matrix, and ids
    // must be unique — a family without a representative row fails
    // generation.
    let mut seen_families: Vec<&str> = Vec::new();
    let mut seen_ids: Vec<&str> = Vec::new();
    for row in rows {
        let family = row["rule"]
            .as_str()
            .ok_or_else(|| bad_input("input row has no rule".to_string()))?;
        assert!(
            RULE_FAMILIES.contains(&family),
            "unknown rule family '{family}'"
        );
        if !seen_families.contains(&family) {
            seen_families.push(family);
        }
        let id = row["id"]
            .as_str()
            .ok_or_else(|| bad_input("input row has no id".to_string()))?;
        assert!(!seen_ids.contains(&id), "duplicate row id '{id}'");
        seen_ids.push(id);
    }
    for family in RULE_FAMILIES {
        assert!(
            seen_families.contains(&family),
            "no input row exercises rule family '{family}'"
        );
    }

    let mut out = Vec::new();
    for row in rows {
        let row_id = row["id"].as_str().unwrap_or("?");
        let schema: SchemaDef = serde_json::from_value(row["schema"].clone())
            .map_err(|e| bad_input(format!("row {row_id}: schema fragment does not parse: {e}")))?;
        let expect = match schema.validate() {
            Ok(()) => json!({ "accepts": true }),
            Err(err) => json!({ "error": { "code": serde_json::to_value(&err)?["code"] } }),
        };
        out.push(json!({
            "id": row["id"],
            "schema": row["schema"],
            "expect": expect,
        }));
    }
    Ok(out)
}

/// Render the committed fixture's exact bytes (pretty JSON + trailing
/// newline).
pub fn render_validation_spec() -> Result<String, serde_json::Error> {
    let doc = json!({
        "$comment": "SPEC-TABLE CORPUS (ENH-052 family 5) — GENERATED, do not edit. \
                     The push-schema validation decision table, computed by running \
                     the server's own SchemaDef::validate over every row of the input \
                     matrix (server/src/spec_corpus_validation_rows.json) via \
                     server/src/spec_corpus_validation.rs by \
                     'make gen-spec-validation'. One row per representative per rule \
                     the wire outcome distinguishes; each reject row violates exactly \
                     one rule, so cascade ordering never changes the outcome. \
                     Index/ttl/updatedAtField/autoIncrementField/onDelete/constraints \
                     rules reject as SCHEMA_VIOLATION; computed rules reject as \
                     BAD_REQUEST; the accept rows pin the inclusive boundary cases \
                     (ttl with its required index, int64 autoIncrement, optional-id \
                     setNull with its required index, min == max bounds, the \
                     string-kind-over-int64 computed special case, search and vector \
                     indexes, and every declaration combined in one table). Each \
                     runner pushes the row's schema fragment through its own \
                     pushSchema path and asserts the same outcome with the same \
                     code. Deliberately excluded (see wire-corpus/README.md \
                     'Spec-table corpus'): rules the existing semantics corpus \
                     already pins and rules that stay server-side (the in-memory \
                     engines deliberately mirror a subset). A rule change \
                     regenerates this file in the same commit — see \
                     wire-corpus/README.md 'Spec-table corpus'.",
        "rows": validation_spec_fixture_rows()?,
    });
    let mut out = serde_json::to_string_pretty(&doc)?;
    out.push('\n');
    Ok(out)
}
