//! Value-level validation and typing: `validate_value`/`validate_doc` (checked
//! against `FieldType` at write time) and `indexed_column_type` (the
//! typed-column mapping shared with `ddl`). `is_widening_of` (the safe-widening
//! rule for additive schema pushes) lives in `par_rt_db_core::schema` (ARC-004)
//! and is re-exported from `schema::types`. Structural schema validation lives
//! in `schema::validate`.

use base64::{Engine as _, engine::general_purpose::STANDARD};

use crate::error::RtDbError;

use super::types::{FieldType, TableDef};

/// Short tag naming a field type's kind, for error messages.
fn type_tag(ty: &FieldType) -> &'static str {
    match ty {
        FieldType::String => "string",
        FieldType::Number => "number",
        FieldType::Boolean => "boolean",
        FieldType::Null => "null",
        FieldType::Id { .. } => "id",
        FieldType::Literal { .. } => "literal",
        FieldType::Optional { .. } => "optional",
        FieldType::Union { .. } => "union",
        FieldType::Array { .. } => "array",
        FieldType::Object { .. } => "object",
        FieldType::Int64 => "int64",
        FieldType::Bytes => "bytes",
        FieldType::Any => "any",
        FieldType::Record { .. } => "record",
        FieldType::Vector { .. } => "vector",
    }
}

fn is_string_literal_union(variants: &[FieldType]) -> bool {
    !variants.is_empty()
        && variants
            .iter()
            .all(|variant| matches!(variant, FieldType::Literal { value } if value.is_string()))
}

/// Whether a field type is array-of-strings-compatible: `Array<T>` (or
/// `Optional<Array<T>>`) where `T`'s `indexed_column_type` maps to Postgres
/// `text`. Used by `collaboratorsField` validation — the jsonb `?` membership
/// test is sound against a bound text uid only when the array elements are
/// themselves string-compatible. Unwraps one layer of `Optional` to admit
/// nullable collaborator arrays, mirroring `owner_field`'s Optional handling.
pub(super) fn is_string_array_field(ty: &FieldType) -> bool {
    let inner = match ty {
        FieldType::Optional { inner } => inner,
        other => other,
    };
    if let FieldType::Array { element } = inner {
        indexed_column_type(element).is_ok_and(|(pg_type, _)| pg_type == "text")
    } else {
        false
    }
}

/// Column type for an indexed field. Indexable types: `String`->"text",
/// `Number`->"double precision", `Int64`->"bigint", `Boolean`->"boolean",
/// `Id`->"text", `Literal(string)`->"text", `Union` where every variant is
/// `Literal(string)`->"text". `Optional<indexable>` -> (pg_type, nullable=true).
/// Anything else is an error.
pub fn indexed_column_type(ty: &FieldType) -> Result<(&'static str, bool), RtDbError> {
    match ty {
        FieldType::String => Ok(("text", false)),
        FieldType::Number => Ok(("double precision", false)),
        FieldType::Int64 => Ok(("bigint", false)),
        FieldType::Boolean => Ok(("boolean", false)),
        FieldType::Id { .. } => Ok(("text", false)),
        FieldType::Literal { value } if value.is_string() => Ok(("text", false)),
        FieldType::Union { variants } if is_string_literal_union(variants) => Ok(("text", false)),
        FieldType::Optional { inner } => {
            let (pg_type, _) = indexed_column_type(inner)?;
            Ok((pg_type, true))
        }
        other => Err(RtDbError::schema(format!(
            "field type '{}' is not indexable",
            type_tag(other)
        ))),
    }
}

fn is_valid_id(value: &serde_json::Value) -> bool {
    match value.as_str() {
        Some(s) => {
            s.len() == 32
                && s.chars()
                    .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
        }
        None => false,
    }
}

fn is_valid_int64(value: &serde_json::Value) -> bool {
    match value.as_str() {
        Some(s) => s.parse::<i64>().is_ok(),
        None => false,
    }
}

fn is_valid_base64(value: &serde_json::Value) -> bool {
    match value.as_str() {
        Some(s) => STANDARD.decode(s).is_ok(),
        None => false,
    }
}

/// Validate a single value against a type (recursive; used by validate_doc and patch).
pub fn validate_value(ty: &FieldType, value: &serde_json::Value) -> bool {
    match ty {
        FieldType::String => value.is_string(),
        FieldType::Number => value.is_number(),
        FieldType::Boolean => value.is_boolean(),
        FieldType::Null => value.is_null(),
        FieldType::Id { .. } => is_valid_id(value),
        FieldType::Literal { value: expected } => value == expected,
        FieldType::Optional { inner } => value.is_null() || validate_value(inner, value),
        FieldType::Union { variants } => variants
            .iter()
            .any(|variant| validate_value(variant, value)),
        FieldType::Array { element } => match value.as_array() {
            Some(items) => items.iter().all(|item| validate_value(element, item)),
            None => false,
        },
        FieldType::Object { fields } => match value.as_object() {
            Some(obj) => {
                if obj.keys().any(|key| !fields.contains_key(key)) {
                    return false;
                }
                fields
                    .iter()
                    .all(|(field_name, field_type)| match obj.get(field_name) {
                        Some(field_value) => validate_value(field_type, field_value),
                        None => matches!(field_type, FieldType::Optional { .. }),
                    })
            }
            None => false,
        },
        FieldType::Int64 => is_valid_int64(value),
        FieldType::Bytes => is_valid_base64(value),
        FieldType::Any => true,
        FieldType::Record { value: value_ty } => match value.as_object() {
            Some(obj) => obj.values().all(|v| validate_value(value_ty, v)),
            None => false,
        },
        FieldType::Vector { dimensions } => match value.as_array() {
            Some(items) => {
                items.len() == *dimensions as usize
                    && items
                        .iter()
                        .all(|el| el.as_f64().is_some_and(|n| n.is_finite()))
            }
            None => false,
        },
    }
}

/// Full-document validation: every schema field present-and-valid (Optional may be absent),
/// no unknown fields, no "_"-prefixed fields. Errors are SchemaViolation naming the field path.
pub fn validate_doc(
    table: &TableDef,
    doc: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), RtDbError> {
    for key in doc.keys() {
        if key.starts_with('_') {
            return Err(RtDbError::schema(format!("field '{key}' is reserved")));
        }
        if !table.fields.contains_key(key) {
            return Err(RtDbError::schema(format!("unknown field '{key}'")));
        }
    }

    for (field_name, field_type) in &table.fields {
        match doc.get(field_name) {
            Some(value) => {
                if !validate_value(field_type, value) {
                    return Err(RtDbError::schema(format!(
                        "field '{field_name}' has an invalid value"
                    )));
                }
            }
            None => {
                if !matches!(field_type, FieldType::Optional { .. }) {
                    return Err(RtDbError::schema(format!(
                        "field '{field_name}' is required"
                    )));
                }
            }
        }
    }

    Ok(())
}
