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
    Vector { dimensions: u32 },
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
    pub fn vector(dimensions: u32) -> Self {
        FieldType::Vector { dimensions }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    pub name: String,
    pub fields: Vec<String>,
    /// `true` marks a full-text search index (mirrors server `schema.rs`: the
    /// server tsvectorizes its text `fields` into a GIN-indexed generated column
    /// ranked via the `search` query terminal). Omitted on the wire for ordinary
    /// btree indexes, so existing schemas deserialize unchanged.
    #[serde(default, skip_serializing_if = "is_false")]
    pub search: bool,
    /// When present, marks this as a vector index: `fields[0]` must name a
    /// `Vector { dimensions }` field whose dimensions match `vector.dimensions`,
    /// and `filter_fields` (if any) names scalar columns used to pre-filter
    /// nearest-neighbor queries. Omitted on the wire for btree/search indexes,
    /// so existing schemas deserialize unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<VectorIndexSpec>,
}

/// Declaration of a vector (approximate nearest-neighbor) index. Wire shape is
/// camelCase (`filterFields`) to match the rest of the protocol. Mirrors
/// `server/src/schema.rs::VectorIndexSpec` byte-for-byte.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorIndexSpec {
    pub dimensions: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub filter_fields: Vec<String>,
}

fn is_false(b: &bool) -> bool {
    !*b
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
            search: false,
            vector: None,
        });
        self
    }

    /// Declare a full-text search index. The server tsvectorizes the (text)
    /// `fields` and ranks matches via the `search` query terminal.
    pub fn search_index(mut self, name: &str, fields: &[&str]) -> Self {
        self.indexes.push(IndexDef {
            name: name.into(),
            fields: fields.iter().map(|s| (*s).into()).collect(),
            search: true,
            vector: None,
        });
        self
    }

    /// Declare a vector index over a `Vector`-typed `field`. The server stores a
    /// pgvector column ranked by cosine distance via the `vectorSearch` terminal;
    /// `filter_fields` names scalar fields usable as eq-filters there.
    pub fn vector_index(
        mut self,
        name: &str,
        field: &str,
        dimensions: u32,
        filter_fields: &[&str],
    ) -> Self {
        self.indexes.push(IndexDef {
            name: name.into(),
            fields: vec![field.into()],
            search: false,
            vector: Some(VectorIndexSpec {
                dimensions,
                filter_fields: filter_fields.iter().map(|s| (*s).into()).collect(),
            }),
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
        // Indexing a missing key and a present-null key both yield `Value::Null`,
        // so verify absence on the underlying object rather than `is_null()`.
        assert!(
            !v["tables"]["solo"]
                .as_object()
                .expect("solo is an object")
                .contains_key("indexes")
        );
    }

    #[test]
    fn search_index_serializes_and_round_trips() {
        // A btree index omits `search`; a search index carries `search: true`.
        let schema = Schema::builder()
            .table(
                "notes",
                Table::new()
                    .field("title", FieldType::String)
                    .field("body", FieldType::String)
                    .index("by_title", &["title"])
                    .search_index("search_content", &["title", "body"]),
            )
            .build();
        let v = serde_json::to_value(&schema).unwrap();
        assert_eq!(
            v["tables"]["notes"]["indexes"],
            json!([
                {"name":"by_title","fields":["title"]},
                {"name":"search_content","fields":["title","body"],"search":true}
            ])
        );
        // Round-trips back: a `search: true` index is read, and a btree index
        // absent the flag deserializes to `search: false`.
        let back: SchemaDef = serde_json::from_value(v).unwrap();
        let notes = back.tables.get("notes").expect("notes present");
        let search = notes
            .indexes
            .as_ref()
            .and_then(|idxs| idxs.iter().find(|i| i.name == "search_content"))
            .expect("search index present");
        assert!(search.search);
        let by_title = notes
            .indexes
            .as_ref()
            .and_then(|idxs| idxs.iter().find(|i| i.name == "by_title"))
            .expect("btree index present");
        assert!(!by_title.search);
    }

    #[test]
    fn vector_index_serializes_and_round_trips() {
        // A vector index carries `vector: {dimensions, filterFields}` (camelCase);
        // a btree index in the same schema omits it (vector: None).
        let schema = Schema::builder()
            .table(
                "notes",
                Table::new()
                    .field("title", FieldType::String)
                    .field("embedding", FieldType::vector(4))
                    .index("by_title", &["title"])
                    .vector_index("by_embedding", "embedding", 4, &["userId"]),
            )
            .build();
        let v = serde_json::to_value(&schema).unwrap();
        assert_eq!(
            v["tables"]["notes"]["indexes"],
            json!([
                {"name":"by_title","fields":["title"]},
                {"name":"by_embedding","fields":["embedding"],"vector":{"dimensions":4,"filterFields":["userId"]}}
            ])
        );
        // Round-trips: vector spec retained; btree index has vector: None.
        let back: SchemaDef = serde_json::from_value(v).unwrap();
        let notes = back.tables.get("notes").expect("notes present");
        let by_embedding = notes
            .indexes
            .as_ref()
            .and_then(|idxs| idxs.iter().find(|i| i.name == "by_embedding"))
            .expect("vector index present");
        let vspec = by_embedding.vector.as_ref().expect("vector spec present");
        assert_eq!(vspec.dimensions, 4);
        assert_eq!(vspec.filter_fields, vec!["userId"]);
        let by_title = notes
            .indexes
            .as_ref()
            .and_then(|idxs| idxs.iter().find(|i| i.name == "by_title"))
            .expect("btree index present");
        assert!(by_title.vector.is_none());
    }
}
