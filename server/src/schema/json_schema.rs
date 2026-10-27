//! JSON Schema (draft 2020-12) export of a [`SchemaDef`] for external codegen
//! and validation tooling (`GET /admin/db/{db}/schema/json-schema`).
//!
//! One `$defs` entry per table, describing the document as READ back from the
//! server (user fields plus the `_id` / `_creationTime` / `_version` system
//! fields). The mapping mirrors `validate_value` so a stored document always
//! validates against its table definition.

use std::collections::BTreeMap;

use serde_json::{Map, Value, json};

use super::{FieldConstraints, FieldType, SchemaDef, TableDef};

const DRAFT: &str = "https://json-schema.org/draft/2020-12/schema";
const ID_PATTERN: &str = "^[0-9a-f]{32}$";
const INT64_PATTERN: &str = "^-?[0-9]+$";
const BASE64_PATTERN: &str = "^[A-Za-z0-9+/]*={0,2}$";

/// Compiles the whole schema into one JSON Schema document.
pub fn export_json_schema(schema: &SchemaDef) -> Value {
    let defs: Map<String, Value> = schema
        .tables
        .iter()
        .map(|(name, table)| (name.clone(), table_schema(name, table)))
        .collect();
    json!({ "$schema": DRAFT, "$defs": defs })
}

fn table_schema(name: &str, table: &TableDef) -> Value {
    let mut properties = Map::new();
    let mut required: Vec<Value> = Vec::new();
    properties.insert(
        "_id".into(),
        json!({"type": "string", "pattern": ID_PATTERN, "description": "Document id (system field)"}),
    );
    properties.insert(
        "_creationTime".into(),
        json!({"type": "integer", "description": "Creation time, epoch milliseconds (system field)"}),
    );
    properties.insert(
        "_version".into(),
        json!({"type": "integer", "description": "Row version, bumped on every write (system field)"}),
    );
    for key in ["_id", "_creationTime", "_version"] {
        required.push(json!(key));
    }
    for (field, ty) in &table.fields {
        let mut prop = field_schema(ty, table.constraints.get(field));
        if let (Some(desc), Some(obj)) = (table.field_descriptions.get(field), prop.as_object_mut())
        {
            obj.insert("description".into(), json!(desc));
        }
        if let (Some(default), Some(obj)) = (table.defaults.get(field), prop.as_object_mut()) {
            obj.insert("default".into(), default.clone());
        }
        properties.insert(field.clone(), prop);
        if !matches!(ty, FieldType::Optional { .. }) {
            required.push(json!(field));
        }
    }
    let mut out = json!({
        "type": "object",
        "title": name,
        "properties": properties,
        "required": required,
        "additionalProperties": false,
    });
    if let (Some(desc), Some(obj)) = (&table.description, out.as_object_mut()) {
        obj.insert("description".into(), json!(desc));
    }
    out
}

fn field_schema(ty: &FieldType, constraints: Option<&FieldConstraints>) -> Value {
    match ty {
        FieldType::String => {
            let mut v = json!({"type": "string"});
            apply_string_constraints(&mut v, constraints);
            v
        }
        FieldType::Number => {
            let mut v = json!({"type": "number"});
            if let (Some(c), Some(obj)) = (constraints, v.as_object_mut()) {
                if let Some(min) = &c.min {
                    obj.insert("minimum".into(), Value::Number(min.clone()));
                }
                if let Some(max) = &c.max {
                    obj.insert("maximum".into(), Value::Number(max.clone()));
                }
            }
            v
        }
        FieldType::Boolean => json!({"type": "boolean"}),
        FieldType::Null => json!({"type": "null"}),
        FieldType::Id { table, .. } => json!({
            "type": "string",
            "pattern": ID_PATTERN,
            "description": format!("Id of a document in table '{table}'"),
            "x-rtdb-table": table,
        }),
        FieldType::Literal { value } => json!({"const": value}),
        FieldType::Optional { inner } => {
            json!({"anyOf": [field_schema(inner, constraints), {"type": "null"}]})
        }
        FieldType::Union { variants } => {
            let members: Vec<Value> = variants.iter().map(|v| field_schema(v, None)).collect();
            json!({"anyOf": members})
        }
        FieldType::Array { element } => {
            json!({"type": "array", "items": field_schema(element, None)})
        }
        FieldType::Object { fields } => object_schema(fields),
        FieldType::Int64 => json!({
            "type": "string",
            "pattern": INT64_PATTERN,
            "format": "int64",
            "description": "64-bit integer encoded as an exact decimal string",
        }),
        FieldType::Bytes => json!({
            "type": "string",
            "pattern": BASE64_PATTERN,
            "contentEncoding": "base64",
        }),
        FieldType::Any => json!({}),
        FieldType::Record { value } => {
            json!({"type": "object", "additionalProperties": field_schema(value, None)})
        }
        FieldType::Vector { dimensions } => json!({
            "type": "array",
            "items": {"type": "number"},
            "minItems": dimensions,
            "maxItems": dimensions,
        }),
    }
}

fn object_schema(fields: &BTreeMap<String, FieldType>) -> Value {
    let properties: Map<String, Value> = fields
        .iter()
        .map(|(k, t)| (k.clone(), field_schema(t, None)))
        .collect();
    let required: Vec<&String> = fields
        .iter()
        .filter(|(_, t)| !matches!(t, FieldType::Optional { .. }))
        .map(|(k, _)| k)
        .collect();
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false,
    })
}

fn apply_string_constraints(v: &mut Value, constraints: Option<&FieldConstraints>) {
    let (Some(c), Some(obj)) = (constraints, v.as_object_mut()) else {
        return;
    };
    if let Some(n) = c.min_length {
        obj.insert("minLength".into(), json!(n));
    }
    if let Some(n) = c.max_length {
        obj.insert("maxLength".into(), json!(n));
    }
    if let Some(p) = &c.pattern {
        obj.insert("pattern".into(), json!(p));
    }
}
