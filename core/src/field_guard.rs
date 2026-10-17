//! Bounded document-field equality guards shared by both Rust engines.

use std::collections::{BTreeMap, HashSet};

use serde_json::Value;

use crate::mutation::FieldExpectation;
use crate::schema::{FieldType, TableDef};

/// Invalid guard arguments, distinct from a valid assertion that does not match.
#[derive(Debug, PartialEq, Eq)]
pub enum FieldGuardError {
    /// Malformed or over-budget assertions, reported as BAD_REQUEST.
    InvalidRequest(&'static str),
    /// A path is outside the table's declared schema, reported as SCHEMA_VIOLATION.
    UnknownPath,
}

/// Validates 1–64 distinct object paths, each 1–16 keys of 1–256 UTF-8 bytes.
/// Across all assertions, strings, keys and path keys may consume at most 65,536
/// UTF-8 bytes, with at most 8,192 JSON nodes and 32 levels of value nesting.
/// Call only after row visibility is established so invisible rows remain absent.
pub fn validate_expectations(
    table: &TableDef,
    expected: &[FieldExpectation],
) -> Result<(), FieldGuardError> {
    if expected.is_empty() || expected.len() > 64 {
        return Err(FieldGuardError::InvalidRequest(
            "expected must contain 1-64 field assertions",
        ));
    }
    let mut paths = HashSet::new();
    let mut nodes = 0usize;
    let mut bytes = 0usize;
    for item in expected {
        if item.path.is_empty()
            || item.path.len() > 16
            || item
                .path
                .iter()
                .any(|key| key.is_empty() || key.len() > 256)
        {
            return Err(FieldGuardError::InvalidRequest(
                "field paths require 1-16 keys of 1-256 UTF-8 bytes",
            ));
        }
        if !paths.insert(&item.path) {
            return Err(FieldGuardError::InvalidRequest(
                "expected contains a duplicate field path",
            ));
        }
        bytes += item.path.iter().map(String::len).sum::<usize>();
        validate_value_budget(&item.value, 0, &mut nodes, &mut bytes)?;
        if !declared_path(&table.fields, &item.path) {
            return Err(FieldGuardError::UnknownPath);
        }
    }
    Ok(())
}

fn validate_value_budget(
    value: &Value,
    depth: usize,
    nodes: &mut usize,
    bytes: &mut usize,
) -> Result<(), FieldGuardError> {
    *nodes += 1;
    if depth > 32 || *nodes > 8192 || *bytes > 65_536 {
        return Err(FieldGuardError::InvalidRequest(
            "expected field values exceed the depth, node or UTF-8 byte budget",
        ));
    }
    match value {
        Value::String(text) => *bytes += text.len(),
        Value::Array(values) => {
            for value in values {
                validate_value_budget(value, depth + 1, nodes, bytes)?;
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                *bytes += key.len();
                validate_value_budget(value, depth + 1, nodes, bytes)?;
            }
        }
        _ => {}
    }
    if *bytes > 65_536 {
        return Err(FieldGuardError::InvalidRequest(
            "expected field values exceed the depth, node or UTF-8 byte budget",
        ));
    }
    Ok(())
}

fn declared_path(fields: &BTreeMap<String, FieldType>, path: &[String]) -> bool {
    fields
        .get(&path[0])
        .is_some_and(|field| declared_child(field, &path[1..]))
}

fn declared_child(field: &FieldType, path: &[String]) -> bool {
    if path.is_empty() {
        return true;
    }
    match field {
        FieldType::Any => true,
        FieldType::Optional { inner } => declared_child(inner, path),
        FieldType::Object { fields } => declared_path(fields, path),
        FieldType::Record { value } => declared_child(value, &path[1..]),
        FieldType::Union { variants } => variants.iter().any(|field| declared_child(field, path)),
        _ => false,
    }
}

/// Tests validated expectations against a visible document. Only object keys
/// are traversed. Selected objects and arrays compare exactly, not as subsets.
pub fn expectations_match(
    doc: &serde_json::Map<String, Value>,
    expected: &[FieldExpectation],
) -> bool {
    expected.iter().all(|item| {
        item.path
            .split_first()
            .and_then(|(root, tail)| {
                tail.iter()
                    .try_fold(doc.get(root)?, |value, key| value.as_object()?.get(key))
            })
            .is_some_and(|actual| json_eq(actual, &item.value))
    })
}

/// Deep JSON equality with numeric integer/float equivalence and distinct booleans.
pub fn json_eq(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(a), Value::Number(b)) => {
            let integer = |number: &serde_json::Number| {
                number
                    .as_i64()
                    .map(i128::from)
                    .or_else(|| number.as_u64().map(i128::from))
                    .or_else(|| {
                        let value = number.as_f64()?;
                        (value.is_finite()
                            && value.fract() == 0.0
                            && value.abs() < 2.0_f64.powi(127))
                        .then_some(value as i128)
                    })
            };
            match (integer(a), integer(b)) {
                (Some(a), Some(b)) => a == b,
                _ => a.as_f64() == b.as_f64(),
            }
        }
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| json_eq(a, b))
        }
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(key, value)| b.get(key).is_some_and(|other| json_eq(value, other)))
        }
        _ => left == right,
    }
}
