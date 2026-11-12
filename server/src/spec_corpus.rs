//! The spec-table corpus generator (ENH-052, family 1): renders
//! `wire-corpus/spec/index-column-types.json` — the FieldType → Postgres
//! column-type decision table every in-memory engine mirrors
//! (`schema::indexed_column_type` and its six client ports, the
//! `indexColumnType` family). The server crate is the single source: the rows
//! are computed by calling [`crate::schema::indexed_column_type`] itself, and
//! the generator's exhaustive match over `FieldType` makes a variant added
//! without a representative row a compile error.
//!
//! `server/src/bin/gen-spec-index-column-types.rs` writes the fixture;
//! `make gen-spec-index-column-types-check` (in `checkall`) diffs the
//! committed fixture against a fresh generation, and the test below pins the
//! same property in-crate.

use std::collections::BTreeMap;

use serde_json::{Value, json};

use crate::schema::{FieldType, OnDeleteAction, indexed_column_type};

/// One representative `FieldType` per enum variant, plus the sub-shapes the
/// mapping distinguishes within a variant (`Literal` string vs non-string,
/// `Union` all-string-literals vs empty vs mixed, `Optional` over two inner
/// types, `Id` with and without `onDelete`). Order is the committed fixture's
/// row order.
fn spec_field_types() -> Vec<FieldType> {
    fn lit(value: Value) -> FieldType {
        FieldType::Literal { value }
    }
    fn string_literal_union(variants: Vec<&str>) -> FieldType {
        FieldType::Union {
            variants: variants
                .into_iter()
                .map(|s| lit(serde_json::Value::from(s)))
                .collect(),
        }
    }
    let mut object_fields = BTreeMap::new();
    object_fields.insert("a".to_string(), FieldType::String);
    vec![
        FieldType::String,
        FieldType::Number,
        FieldType::Boolean,
        FieldType::Int64,
        FieldType::Id {
            table: "t".to_string(),
            on_delete: None,
        },
        FieldType::Id {
            table: "t".to_string(),
            on_delete: Some(OnDeleteAction::Cascade),
        },
        lit(json!("x")),
        lit(json!(42)),
        FieldType::Optional {
            inner: Box::new(FieldType::String),
        },
        FieldType::Optional {
            inner: Box::new(FieldType::Number),
        },
        string_literal_union(vec!["a", "b"]),
        FieldType::Union {
            variants: Vec::new(),
        },
        FieldType::Union {
            variants: vec![lit(json!("a")), FieldType::Number],
        },
        FieldType::Null,
        FieldType::Array {
            element: Box::new(FieldType::String),
        },
        FieldType::Object {
            fields: object_fields,
        },
        FieldType::Bytes,
        FieldType::Any,
        FieldType::Record {
            value: Box::new(FieldType::String),
        },
        FieldType::Vector { dimensions: 3 },
    ]
}

/// The compile-time drift net: names every `FieldType` variant with no
/// wildcard arm, so a variant added to the enum without a representative row
/// in [`spec_field_types`] fails this crate to compile. Keep the arm list in
/// the enum's declaration order.
fn exhaustiveness_guard(ty: &FieldType) {
    match ty {
        FieldType::String
        | FieldType::Number
        | FieldType::Boolean
        | FieldType::Null
        | FieldType::Id { .. }
        | FieldType::Literal { .. }
        | FieldType::Optional { .. }
        | FieldType::Union { .. }
        | FieldType::Array { .. }
        | FieldType::Object { .. }
        | FieldType::Int64
        | FieldType::Bytes
        | FieldType::Any
        | FieldType::Record { .. }
        | FieldType::Vector { .. } => {}
    }
}

/// One fixture row per representative: the wire `FieldType` JSON (serde's own
/// serialization — the same spelling every client parses for pushSchema) plus
/// the mapping's outcome, `{"columnType", "nullable"}` for an indexable type
/// or `{"error": true}` for a non-indexable one.
fn spec_rows() -> Result<Vec<Value>, serde_json::Error> {
    let mut rows = Vec::new();
    for ty in spec_field_types() {
        exhaustiveness_guard(&ty);
        let field_type = serde_json::to_value(&ty)?;
        let row = match indexed_column_type(&ty) {
            Ok((column_type, nullable)) => json!({
                "fieldType": field_type,
                "columnType": column_type,
                "nullable": nullable,
            }),
            Err(_) => json!({
                "fieldType": field_type,
                "error": true,
            }),
        };
        rows.push(row);
    }
    Ok(rows)
}

/// Render the committed fixture's exact bytes (pretty JSON + trailing
/// newline).
pub fn render_index_column_type_spec() -> Result<String, serde_json::Error> {
    let doc = json!({
        "$comment": "SPEC-TABLE CORPUS (ENH-052 family 1) — GENERATED, do not edit. \
                     The FieldType -> Postgres column-type decision table, one row per \
                     representative per server FieldType variant, computed from \
                     server/src/schema/value.rs::indexed_column_type by \
                     'make gen-spec-index-column-types'. `columnType` is the server's raw \
                     Postgres type string and `nullable` whether the source field was \
                     wrapped in Optional; non-indexable rows carry `error: true`. Each \
                     runner asserts its own port of the mapping (the client PgType \
                     four-value enum mapped back to these raw strings) against every row, \
                     and that its own FieldType variant set is covered. A mapping change \
                     regenerates this file in the same commit — see wire-corpus/README.md \
                     'Spec-table corpus'.",
        "rows": spec_rows()?,
    });
    let mut out = serde_json::to_string_pretty(&doc)?;
    out.push('\n');
    Ok(out)
}

#[cfg(test)]
mod tests {
    /// The server IS the source: the committed fixture must equal a fresh
    /// generation. A mapping or variant change that skips
    /// `make gen-spec-index-column-types` fails here.
    #[test]
    fn committed_spec_fixture_matches_generation() {
        let committed = include_str!("../../wire-corpus/spec/index-column-types.json");
        let generated = super::render_index_column_type_spec().expect("render spec fixture");
        assert_eq!(
            committed, generated,
            "wire-corpus/spec/index-column-types.json is stale — run \
             'make gen-spec-index-column-types' and commit the result"
        );
    }
}
