use std::collections::{BTreeMap, HashSet};

use crate::error::RtDbError;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum FieldType {
    String,
    Number,
    Boolean,
    Null,
    Id { table: String },
    Literal { value: serde_json::Value },
    Optional { inner: Box<FieldType> },
    Union { variants: Vec<FieldType> },
    Array { element: Box<FieldType> },
    Object { fields: BTreeMap<String, FieldType> },
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IndexDef {
    pub name: String,
    pub fields: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TableDef {
    pub fields: BTreeMap<String, FieldType>,
    #[serde(default)]
    pub indexes: Vec<IndexDef>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SchemaDef {
    pub tables: BTreeMap<String, TableDef>,
}

/// Table/field identifier regex: `^[a-zA-Z][a-zA-Z0-9_]{0,62}$`.
fn is_valid_identifier(s: &str) -> bool {
    if s.is_empty() || s.len() > 63 {
        return false;
    }
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Structural validation of a single field type: `Literal` must carry a scalar
/// JSON value, `Union` must have at least one variant, and `Optional` may not
/// directly wrap another `Optional`. Recurses into nested types.
fn validate_field_type(ty: &FieldType) -> Result<(), RtDbError> {
    match ty {
        FieldType::String
        | FieldType::Number
        | FieldType::Boolean
        | FieldType::Null
        | FieldType::Id { .. } => Ok(()),
        FieldType::Literal { value } => {
            if value.is_string() || value.is_number() || value.is_boolean() {
                Ok(())
            } else {
                Err(RtDbError::schema(
                    "literal value must be a string, number, or boolean",
                ))
            }
        }
        FieldType::Optional { inner } => {
            if matches!(**inner, FieldType::Optional { .. }) {
                return Err(RtDbError::schema(
                    "optional cannot directly wrap another optional",
                ));
            }
            validate_field_type(inner)
        }
        FieldType::Union { variants } => {
            if variants.is_empty() {
                return Err(RtDbError::schema("union must have at least one variant"));
            }
            for variant in variants {
                validate_field_type(variant)?;
            }
            Ok(())
        }
        FieldType::Array { element } => validate_field_type(element),
        FieldType::Object { fields } => {
            for field_type in fields.values() {
                validate_field_type(field_type)?;
            }
            Ok(())
        }
    }
}

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
    }
}

fn is_string_literal_union(variants: &[FieldType]) -> bool {
    !variants.is_empty()
        && variants
            .iter()
            .all(|variant| matches!(variant, FieldType::Literal { value } if value.is_string()))
}

/// Column type for an indexed field. Indexable types: `String`->"text",
/// `Number`->"double precision", `Boolean`->"boolean", `Id`->"text",
/// `Literal(string)`->"text", `Union` where every variant is `Literal(string)`->"text".
/// `Optional<indexable>` -> (pg_type, nullable=true). Anything else is an error.
pub fn indexed_column_type(ty: &FieldType) -> Result<(&'static str, bool), RtDbError> {
    match ty {
        FieldType::String => Ok(("text", false)),
        FieldType::Number => Ok(("double precision", false)),
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

impl TableDef {
    fn validate_structure(&self, table_name: &str) -> Result<(), RtDbError> {
        for (field_name, field_type) in &self.fields {
            if !is_valid_identifier(field_name) {
                return Err(RtDbError::schema(format!(
                    "table '{table_name}' has invalid field name '{field_name}'"
                )));
            }
            validate_field_type(field_type)?;
        }

        let mut index_names = HashSet::new();
        for index in &self.indexes {
            if !is_valid_identifier(&index.name) {
                return Err(RtDbError::schema(format!(
                    "table '{table_name}' has invalid index name '{}'",
                    index.name
                )));
            }
            if !index_names.insert(index.name.as_str()) {
                return Err(RtDbError::schema(format!(
                    "table '{table_name}' has duplicate index name '{}'",
                    index.name
                )));
            }
            if index.fields.is_empty() {
                return Err(RtDbError::schema(format!(
                    "index '{}' on table '{table_name}' has no fields",
                    index.name
                )));
            }
            let mut seen_fields = HashSet::new();
            for field_name in &index.fields {
                if !seen_fields.insert(field_name.as_str()) {
                    return Err(RtDbError::schema(format!(
                        "index '{}' on table '{table_name}' has duplicate field '{field_name}'",
                        index.name
                    )));
                }
                let field_type = self.fields.get(field_name).ok_or_else(|| {
                    RtDbError::schema(format!(
                        "index '{}' on table '{table_name}' references unknown field '{field_name}'",
                        index.name
                    ))
                })?;
                indexed_column_type(field_type)?;
            }
        }
        Ok(())
    }

    pub fn index(&self, name: &str) -> Result<&IndexDef, RtDbError> {
        self.indexes
            .iter()
            .find(|index| index.name == name)
            .ok_or_else(|| RtDbError::bad_request(format!("index '{name}' not found")))
    }
}

impl SchemaDef {
    /// Structural validation: identifier regexes (Global Constraints), case-insensitive
    /// table uniqueness, index names unique per table and matching the field-name regex,
    /// index fields exist and are indexable, Literal values scalar, Union non-empty,
    /// reserved field names rejected: any starting with "_" .
    pub fn validate(&self) -> Result<(), RtDbError> {
        let mut lower_names = HashSet::new();
        for (table_name, table_def) in &self.tables {
            if !is_valid_identifier(table_name) {
                return Err(RtDbError::schema(format!(
                    "invalid table name '{table_name}'"
                )));
            }
            if !lower_names.insert(table_name.to_lowercase()) {
                return Err(RtDbError::schema(format!(
                    "table name '{table_name}' collides case-insensitively with another table"
                )));
            }
            table_def.validate_structure(table_name)?;
        }
        Ok(())
    }

    pub fn table(&self, name: &str) -> Result<&TableDef, RtDbError> {
        self.tables
            .get(name)
            .ok_or_else(|| RtDbError::not_found(format!("table '{name}' not found")))
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

#[cfg(test)]
mod tests {
    use super::*;

    pub fn kanban_schema_json() -> serde_json::Value {
        serde_json::json!({"tables":{
          "projects":{
            "fields":{
              "name":{"type":"string"},
              "description":{"type":"optional","inner":{"type":"string"}},
              "status":{"type":"union","variants":[
                {"type":"literal","value":"active"},{"type":"literal","value":"paused"},
                {"type":"literal","value":"dormant"},{"type":"literal","value":"archived"}]},
              "tags":{"type":"array","element":{"type":"string"}},
              "updatedAt":{"type":"number"}},
            "indexes":[{"name":"by_name","fields":["name"]},{"name":"by_status","fields":["status"]}]},
          "workItems":{
            "fields":{
              "projectId":{"type":"id","table":"projects"},
              "title":{"type":"string"},
              "status":{"type":"union","variants":[
                {"type":"literal","value":"backlog"},{"type":"literal","value":"in_progress"},
                {"type":"literal","value":"blocked"},{"type":"literal","value":"done"}]},
              "order":{"type":"number"},
              "completedAt":{"type":"optional","inner":{"type":"number"}}},
            "indexes":[{"name":"by_project","fields":["projectId"]},
                       {"name":"by_status","fields":["status"]},
                       {"name":"by_project_and_status","fields":["projectId","status"]}]}
        }})
    }

    fn simple_table() -> TableDef {
        TableDef {
            fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
            indexes: vec![],
        }
    }

    fn kanban_schema() -> SchemaDef {
        serde_json::from_value(kanban_schema_json()).unwrap()
    }

    fn work_items_table() -> TableDef {
        kanban_schema().tables.get("workItems").unwrap().clone()
    }

    fn valid_work_item_doc() -> serde_json::Map<String, serde_json::Value> {
        serde_json::json!({
            "projectId": "0123456789abcdef0123456789abcdef",
            "title": "Do the thing",
            "status": "backlog",
            "order": 1.0,
            "completedAt": null
        })
        .as_object()
        .unwrap()
        .clone()
    }

    // (a) full kanban-shaped schema deserializes and validate() passes.
    #[test]
    fn kanban_schema_deserializes_and_validates() {
        let schema = kanban_schema();
        assert!(schema.validate().is_ok());
    }

    // (b) bad identifiers rejected.
    #[test]
    fn rejects_table_name_starting_with_digit() {
        let schema = SchemaDef {
            tables: BTreeMap::from([("1bad".to_string(), simple_table())]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn rejects_field_name_with_invalid_chars() {
        let table = TableDef {
            fields: BTreeMap::from([("a-b".to_string(), FieldType::String)]),
            indexes: vec![],
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn rejects_identifier_longer_than_63_chars() {
        let long_name = "a".repeat(64);
        let schema = SchemaDef {
            tables: BTreeMap::from([(long_name, simple_table())]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn rejects_field_name_starting_with_underscore() {
        let table = TableDef {
            fields: BTreeMap::from([("_secret".to_string(), FieldType::String)]),
            indexes: vec![],
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    // (c) case-insensitive table collision rejected.
    #[test]
    fn rejects_case_insensitive_table_name_collision() {
        let schema = SchemaDef {
            tables: BTreeMap::from([
                ("Items".to_string(), simple_table()),
                ("items".to_string(), simple_table()),
            ]),
        };
        assert!(schema.validate().is_err());
    }

    // (d) index over Array field rejected by validate().
    #[test]
    fn rejects_index_over_array_field() {
        let table = TableDef {
            fields: BTreeMap::from([(
                "tags".to_string(),
                FieldType::Array {
                    element: Box::new(FieldType::String),
                },
            )]),
            indexes: vec![IndexDef {
                name: "by_tags".to_string(),
                fields: vec!["tags".to_string()],
            }],
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn rejects_index_with_empty_fields() {
        let table = TableDef {
            fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
            indexes: vec![IndexDef {
                name: "by_nothing".to_string(),
                fields: vec![],
            }],
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn rejects_index_with_duplicate_fields() {
        let table = TableDef {
            fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
            indexes: vec![IndexDef {
                name: "by_name".to_string(),
                fields: vec!["name".to_string(), "name".to_string()],
            }],
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn rejects_duplicate_index_names() {
        let table = TableDef {
            fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
            indexes: vec![
                IndexDef {
                    name: "by_name".to_string(),
                    fields: vec!["name".to_string()],
                },
                IndexDef {
                    name: "by_name".to_string(),
                    fields: vec!["name".to_string()],
                },
            ],
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn rejects_index_name_with_invalid_chars() {
        let table = TableDef {
            fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
            indexes: vec![IndexDef {
                name: "by-name".to_string(),
                fields: vec!["name".to_string()],
            }],
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn rejects_index_referencing_unknown_field() {
        let table = TableDef {
            fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
            indexes: vec![IndexDef {
                name: "by_missing".to_string(),
                fields: vec!["missing".to_string()],
            }],
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn rejects_literal_with_non_scalar_value() {
        let table = TableDef {
            fields: BTreeMap::from([(
                "x".to_string(),
                FieldType::Literal {
                    value: serde_json::json!(null),
                },
            )]),
            indexes: vec![],
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn rejects_empty_union() {
        let table = TableDef {
            fields: BTreeMap::from([("x".to_string(), FieldType::Union { variants: vec![] })]),
            indexes: vec![],
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn rejects_optional_wrapping_optional() {
        let table = TableDef {
            fields: BTreeMap::from([(
                "x".to_string(),
                FieldType::Optional {
                    inner: Box::new(FieldType::Optional {
                        inner: Box::new(FieldType::String),
                    }),
                },
            )]),
            indexes: vec![],
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    // (e) indexed_column_type matrix.
    #[test]
    fn indexed_column_type_matrix() {
        assert_eq!(
            indexed_column_type(&FieldType::String).unwrap(),
            ("text", false)
        );
        assert_eq!(
            indexed_column_type(&FieldType::Number).unwrap(),
            ("double precision", false)
        );
        assert_eq!(
            indexed_column_type(&FieldType::Boolean).unwrap(),
            ("boolean", false)
        );
        assert_eq!(
            indexed_column_type(&FieldType::Id {
                table: "projects".to_string()
            })
            .unwrap(),
            ("text", false)
        );
        assert_eq!(
            indexed_column_type(&FieldType::Literal {
                value: serde_json::json!("active")
            })
            .unwrap(),
            ("text", false)
        );
        assert_eq!(
            indexed_column_type(&FieldType::Optional {
                inner: Box::new(FieldType::String)
            })
            .unwrap(),
            ("text", true)
        );
        let union = FieldType::Union {
            variants: vec![
                FieldType::Literal {
                    value: serde_json::json!("a"),
                },
                FieldType::Literal {
                    value: serde_json::json!("b"),
                },
            ],
        };
        assert_eq!(indexed_column_type(&union).unwrap(), ("text", false));
        assert!(
            indexed_column_type(&FieldType::Array {
                element: Box::new(FieldType::String)
            })
            .is_err()
        );
        assert!(indexed_column_type(&FieldType::Null).is_err());
        assert!(
            indexed_column_type(&FieldType::Literal {
                value: serde_json::json!(1)
            })
            .is_err()
        );
    }

    // (f) validate_doc: accepts valid doc, rejects unknown/missing/wrong-typed/_-prefixed fields.
    #[test]
    fn validate_doc_accepts_valid_document() {
        let table = work_items_table();
        assert!(validate_doc(&table, &valid_work_item_doc()).is_ok());
    }

    #[test]
    fn validate_doc_rejects_unknown_field() {
        let table = work_items_table();
        let mut doc = valid_work_item_doc();
        doc.insert("bogus".to_string(), serde_json::json!(true));
        assert!(validate_doc(&table, &doc).is_err());
    }

    #[test]
    fn validate_doc_rejects_missing_required_field() {
        let table = work_items_table();
        let mut doc = valid_work_item_doc();
        doc.remove("title");
        assert!(validate_doc(&table, &doc).is_err());
    }

    #[test]
    fn validate_doc_rejects_wrong_typed_field() {
        let table = work_items_table();
        let mut doc = valid_work_item_doc();
        doc.insert("order".to_string(), serde_json::json!("not a number"));
        assert!(validate_doc(&table, &doc).is_err());
    }

    #[test]
    fn validate_doc_rejects_underscore_prefixed_field() {
        let table = work_items_table();
        let mut doc = valid_work_item_doc();
        doc.insert(
            "_id".to_string(),
            serde_json::json!("0123456789abcdef0123456789abcdef"),
        );
        assert!(validate_doc(&table, &doc).is_err());
    }

    // (g) Optional field explicit null accepted.
    #[test]
    fn validate_doc_accepts_explicit_null_for_optional_field() {
        let table = work_items_table();
        let mut doc = valid_work_item_doc();
        doc.insert("completedAt".to_string(), serde_json::Value::Null);
        assert!(validate_doc(&table, &doc).is_ok());
    }

    #[test]
    fn validate_value_optional_accepts_null_and_inner_type() {
        let ty = FieldType::Optional {
            inner: Box::new(FieldType::Number),
        };
        assert!(validate_value(&ty, &serde_json::Value::Null));
        assert!(validate_value(&ty, &serde_json::json!(42)));
        assert!(!validate_value(&ty, &serde_json::json!("nope")));
    }

    #[test]
    fn validate_value_id_requires_32_lowercase_hex_chars() {
        let ty = FieldType::Id {
            table: "projects".to_string(),
        };
        assert!(validate_value(
            &ty,
            &serde_json::json!("0123456789abcdef0123456789abcdef")
        ));
        assert!(!validate_value(
            &ty,
            &serde_json::json!("0123456789ABCDEF0123456789abcdef")
        ));
        assert!(!validate_value(&ty, &serde_json::json!("short")));
    }

    #[test]
    fn schema_table_lookup() {
        let schema = kanban_schema();
        assert!(schema.table("projects").is_ok());
        assert!(schema.table("missing").is_err());
    }

    #[test]
    fn table_index_lookup() {
        let schema = kanban_schema();
        let table = schema.table("projects").unwrap();
        assert!(table.index("by_name").is_ok());
        assert!(table.index("missing").is_err());
    }
}
