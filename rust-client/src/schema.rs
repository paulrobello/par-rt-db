//! Schema DSL: builds the exact `SchemaDef` JSON consumed by `POST /admin/push-schema`.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    Int64,
    Bytes,
    Any,
    Record { value: Box<FieldType> },
}

impl FieldType {
    pub fn id(table: &str) -> Self {
        FieldType::Id {
            table: table.into(),
        }
    }
    pub fn optional(inner: FieldType) -> Self {
        FieldType::Optional {
            inner: Box::new(inner),
        }
    }
    pub fn literal(value: impl Into<serde_json::Value>) -> Self {
        FieldType::Literal {
            value: value.into(),
        }
    }
    pub fn union(variants: impl IntoIterator<Item = FieldType>) -> Self {
        FieldType::Union {
            variants: variants.into_iter().collect(),
        }
    }
    pub fn array(element: FieldType) -> Self {
        FieldType::Array {
            element: Box::new(element),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    pub name: String,
    pub fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDef {
    pub fields: BTreeMap<String, FieldType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexes: Option<Vec<IndexDef>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDef {
    pub tables: BTreeMap<String, TableDef>,
}

/// Finished schema (alias for the wire type).
pub type Schema = SchemaDef;

pub struct TableBuilder {
    fields: BTreeMap<String, FieldType>,
    indexes: Vec<IndexDef>,
}

impl TableBuilder {
    pub fn new() -> Self {
        Self {
            fields: BTreeMap::new(),
            indexes: Vec::new(),
        }
    }
    pub fn field(mut self, name: &str, ft: FieldType) -> Self {
        self.fields.insert(name.into(), ft);
        self
    }
    pub fn index(mut self, name: &str, fields: &[&str]) -> Self {
        self.indexes.push(IndexDef {
            name: name.into(),
            fields: fields.iter().map(|s| (*s).into()).collect(),
        });
        self
    }
    fn finish(self) -> TableDef {
        let indexes = if self.indexes.is_empty() {
            None
        } else {
            Some(self.indexes)
        };
        TableDef {
            fields: self.fields,
            indexes,
        }
    }
}

impl Default for TableBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Convenience alias used in builder closures for readability.
pub type Table = TableBuilder;

#[derive(Default)]
pub struct SchemaBuilder {
    tables: BTreeMap<String, TableDef>,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn table(mut self, name: &str, build: impl OnceTable) -> Self {
        self.tables.insert(name.into(), build.finish_table());
        self
    }
    pub fn build(self) -> SchemaDef {
        SchemaDef {
            tables: self.tables,
        }
    }
}

/// Anything that can produce a `TableDef`. Implemented for the builder and (later) closures.
pub trait OnceTable {
    fn finish_table(self) -> TableDef;
}

impl OnceTable for TableBuilder {
    fn finish_table(self) -> TableDef {
        TableBuilder::finish(self)
    }
}

impl SchemaDef {
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn field_type_wire_tags() {
        assert_eq!(
            serde_json::to_value(FieldType::String).unwrap(),
            json!({"type":"string"})
        );
        assert_eq!(
            serde_json::to_value(FieldType::Id {
                table: "projects".into()
            })
            .unwrap(),
            json!({"type":"id","table":"projects"})
        );
        assert_eq!(
            serde_json::to_value(FieldType::Optional {
                inner: Box::new(FieldType::Boolean)
            })
            .unwrap(),
            json!({"type":"optional","inner":{"type":"boolean"}})
        );
        assert_eq!(
            serde_json::to_value(FieldType::Literal {
                value: json!("active")
            })
            .unwrap(),
            json!({"type":"literal","value":"active"})
        );
        assert_eq!(
            serde_json::to_value(FieldType::Int64).unwrap(),
            json!({"type":"int64"})
        );
    }

    #[test]
    fn builder_serializes_full_schema() {
        let schema = Schema::builder()
            .table(
                "projects",
                Table::new()
                    .field("name", FieldType::String)
                    .field("archived", FieldType::optional(FieldType::Boolean))
                    .index("by_name", &["name"]),
            )
            .table(
                "items",
                Table::new()
                    .field("projectId", FieldType::id("projects"))
                    .field("title", FieldType::String)
                    .field(
                        "status",
                        FieldType::union([
                            FieldType::literal("backlog"),
                            FieldType::literal("done"),
                        ]),
                    )
                    .field("order", FieldType::Number)
                    .index("by_project", &["projectId"])
                    .index("by_project_and_title", &["projectId", "title"]),
            )
            .build();
        let v = serde_json::to_value(&schema).unwrap();
        assert_eq!(
            v,
            json!({
                "tables": {
                    "projects": {
                        "fields": {
                            "name": {"type":"string"},
                            "archived": {"type":"optional","inner":{"type":"boolean"}}
                        },
                        "indexes": [{"name":"by_name","fields":["name"]}]
                    },
                    "items": {
                        "fields": {
                            "projectId": {"type":"id","table":"projects"},
                            "title": {"type":"string"},
                            "status": {"type":"union","variants":[{"type":"literal","value":"backlog"},{"type":"literal","value":"done"}]},
                            "order": {"type":"number"}
                        },
                        "indexes": [
                            {"name":"by_project","fields":["projectId"]},
                            {"name":"by_project_and_title","fields":["projectId","title"]}
                        ]
                    }
                }
            })
        );
    }

    #[test]
    fn table_with_no_indexes_omits_key() {
        let schema = Schema::builder()
            .table("solo", Table::new().field("x", FieldType::Number))
            .build();
        let v = serde_json::to_value(&schema).unwrap();
        assert!(v["tables"]["solo"]["indexes"].is_null());
    }
}
