//! Schema DSL: builds the exact `SchemaDef` JSON consumed by `POST /admin/push-schema`.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::wire::FilterExpr;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

/// Returns `true` when changing a field's declared type from `old` to `new` is a
/// safe widening — every value valid under `old` remains valid under `new`, so no
/// existing row is orphaned and no data migration is required. The only widening
/// currently recognized is over finite literal sets: a lone `Literal` or a `Union`
/// whose variants are all `Literal`s, where the new literal set is a superset of
/// the old one (e.g. adding a variant to an enum-like union). Every other type
/// change — narrowing a union (drops a variant some rows may hold), `union <->
/// scalar`, any scalar-type change, `Optional`, `Object`, and mixed-kind unions —
/// is NOT a widening and stays rejected by `detect_destructive_changes`.
pub fn is_widening_of(old: &FieldType, new: &FieldType) -> bool {
    match (literal_set(old), literal_set(new)) {
        (Some(old_vals), Some(new_vals)) => old_vals.iter().all(|old_v| new_vals.contains(old_v)),
        _ => false,
    }
}

/// Finite set of accepted values for a literal-only type: `Some` for a lone
/// `Literal` or a `Union` whose variants are all `Literal`s; `None` for any other
/// type (unions mixing in non-literal variants, scalars, `Optional`, `Object`).
/// Variant order and duplicates are irrelevant — the result is used only for
/// membership tests. `serde_json::Value` is `PartialEq` but not `Ord`/`Hash`, so
/// this returns a `Vec<&Value>` for linear `.contains()` membership rather than a set.
fn literal_set(ty: &FieldType) -> Option<Vec<&serde_json::Value>> {
    match ty {
        FieldType::Literal { value } => Some(vec![value]),
        FieldType::Union { variants } => {
            let vals: Vec<&serde_json::Value> = variants
                .iter()
                .filter_map(|v| match v {
                    FieldType::Literal { value } => Some(value),
                    _ => None,
                })
                .collect();
            // Finite only when every variant is a Literal. An empty union is
            // refused so is_widening_of never returns a vacuous true for it
            // (empty unions are also rejected at validation time).
            if vals.len() == variants.len() && !variants.is_empty() {
                Some(vals)
            } else {
                None
            }
        }
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    /// `true` marks a unique btree index (mirrors server `schema.rs`: the
    /// server emits `CREATE UNIQUE INDEX`, constraining tuples over `fields`).
    /// Omitted on the wire when false, so existing schemas deserialize
    /// unchanged. May not combine with `search` or `vector`.
    #[serde(default, skip_serializing_if = "is_false")]
    pub unique: bool,
    /// Optional partial-index predicate (mirrors server `schema.rs::IndexDef`):
    /// when present the index constrains only rows matching this filter.
    /// Serialized as the wire key `where` (the field is the raw-identifier
    /// `r#where` so the JSON key matches the server/TS/Python clients exactly),
    /// and omitted on the wire when `None`.
    #[serde(default, rename = "where", skip_serializing_if = "Option::is_none")]
    pub r#where: Option<FilterExpr>,
}

/// Declaration of a vector (approximate nearest-neighbor) index. Wire shape is
/// camelCase (`filterFields`) to match the rest of the protocol. Mirrors
/// `server/src/schema.rs::VectorIndexSpec` byte-for-byte.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorIndexSpec {
    pub dimensions: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub filter_fields: Vec<String>,
}

fn is_false(b: &bool) -> bool {
    !*b
}

/// Declarative document TTL (auto-expiry). `field` names a declared numeric
/// field whose value is each document's absolute epoch-ms expiry; the in-memory
/// harness's [`crate::in_memory::InMemoryRtDbClient::tick`] reaps rows whose
/// value is in the past (mirroring the live server's per-db reaper).
/// `default_duration_ms` stamps the field at insert time when the caller omits
/// it. Mirrors `server/src/schema.rs::TtlDef` byte-for-byte (camelCase wire).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TtlDef {
    pub field: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_duration_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableDef {
    pub fields: BTreeMap<String, FieldType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexes: Option<Vec<IndexDef>>,
    /// Opt-in per-row authorization: names a declared, string-compatible field
    /// whose value is the owning user's id. When set, an authenticated user
    /// reads/mutates only their own rows on this table; machine tokens and
    /// scheduled jobs bypass. Server-enforced; clients only declare it.
    /// Mirrors `server/src/schema.rs::TableDef` byte-for-byte — the explicit
    /// `rename` is required because this struct has no container `rename_all`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "ownerField"
    )]
    pub owner_field: Option<String>,
    /// Opt-in extension of `owner_field`: names a declared array-of-strings (or
    /// array-of-id) field whose values are additional user ids that may
    /// read/mutate the row (owner OR collaborator). May be declared alone.
    /// Mirrors `server/src/schema.rs::TableDef` byte-for-byte.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "collaboratorsField"
    )]
    pub collaborators_field: Option<String>,
    /// Declarative document TTL. When `Some`, the in-memory harness's `tick`
    /// reaps rows whose `ttl.field` value is in the past. Additive — schemas
    /// without it deserialize unchanged. Mirrors `server/src/schema.rs::TableDef`
    /// byte-for-byte (wire key `ttl`).
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "ttl")]
    pub ttl: Option<TtlDef>,
    /// Opt-in per-row authorization predicate (Model C). A general `FilterExpr`
    /// over this table's declared doc fields and the principal's markers
    /// (`{"$user":true}` / `{"$email":true}`). Enforced on the same
    /// read/write/subscription seams as `owner_field`; additive to it. Marker
    /// values are valid only here — client `.filter()` queries reject them.
    /// Server-enforced; the client only declares it. Mirrors
    /// `server/src/schema.rs::TableDef` byte-for-byte (wire key `authorize`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorize: Option<FilterExpr>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaDef {
    pub tables: BTreeMap<String, TableDef>,
}

/// Finished schema (alias for the wire type).
pub type Schema = SchemaDef;

pub struct TableBuilder {
    fields: BTreeMap<String, FieldType>,
    indexes: Vec<IndexDef>,
    owner_field: Option<String>,
    collaborators_field: Option<String>,
    ttl: Option<TtlDef>,
    authorize: Option<FilterExpr>,
    /// Index of the most recently pushed [`IndexDef`] in [`Self::indexes`], so
    /// the chainable `.unique()` / `.where_clause()` setters can configure it
    /// after `index`/`search_index`/`vector_index` returned `self`. `None`
    /// until the first index is declared (the setters then no-op).
    last_index: Option<usize>,
}

impl TableBuilder {
    pub fn new() -> Self {
        Self {
            fields: BTreeMap::new(),
            indexes: Vec::new(),
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            authorize: None,
            last_index: None,
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
            unique: false,
            r#where: None,
        });
        self.last_index = Some(self.indexes.len() - 1);
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
            unique: false,
            r#where: None,
        });
        self.last_index = Some(self.indexes.len() - 1);
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
            unique: false,
            r#where: None,
        });
        self.last_index = Some(self.indexes.len() - 1);
        self
    }

    /// Mark the most recently declared index as unique (`.index(...).unique()`),
    /// mirroring the TS client's chainable `.unique()`. The server emits
    /// `CREATE UNIQUE INDEX`, constraining tuples over the index's `fields`
    /// (never `id`/`created_at`). May not combine with `search` or `vector`;
    /// the server rejects that at push-schema time. No-ops if no index has
    /// been declared yet.
    pub fn unique(mut self) -> Self {
        if let Some(i) = self.last_index {
            self.indexes[i].unique = true;
        }
        self
    }

    /// Attach a partial-index predicate to the most recently declared index
    /// (`.index(...).where_clause(pred)`). Serialized as the wire key `where`
    /// (the field is the raw-identifier `r#where`); the method avoids the `where`
    /// keyword at the call site while keeping the on-wire key byte-identical to
    /// the server/TS/Python clients. No-ops if no index has been declared yet.
    pub fn where_clause(mut self, predicate: FilterExpr) -> Self {
        if let Some(i) = self.last_index {
            self.indexes[i].r#where = Some(predicate);
        }
        self
    }

    /// Declare the per-row owner field for authorization. `field` names a
    /// declared string-compatible field whose value is the owning user's id.
    /// Server-enforced; the client only declares it and round-trips it on the
    /// wire as `ownerField`.
    pub fn owner_field(mut self, field: &str) -> Self {
        self.owner_field = Some(field.into());
        self
    }

    /// Declare the per-row collaborators field for authorization. `field`
    /// names a declared array-of-strings (or array-of-id) field whose values
    /// are additional user ids that may read/mutate the row (owner OR
    /// collaborator). Server-enforced; the client only declares it and
    /// round-trips it on the wire as `collaboratorsField`.
    pub fn collaborators_field(mut self, field: &str) -> Self {
        self.collaborators_field = Some(field.into());
        self
    }

    /// Declare document TTL (auto-expiry). `field` names a declared numeric
    /// field whose value is each document's absolute epoch-ms expiry; the
    /// in-memory harness's `tick` reaps rows whose value is in the past.
    /// `default_duration_ms` stamps the field at insert time when the caller
    /// omits it. Server-enforced on the live server; the client only declares
    /// it and round-trips it on the wire as `ttl`. Mirrors the TS client's
    /// chainable `.ttl(field, defaultDurationMs?)`.
    pub fn ttl(mut self, field: &str, default_duration_ms: Option<i64>) -> Self {
        self.ttl = Some(TtlDef {
            field: field.into(),
            default_duration_ms,
        });
        self
    }

    /// Declare the per-row authorization predicate (Model C). `predicate` is a
    /// `FilterExpr` over this table's declared doc fields and the principal's
    /// markers (`{"$user":true}` / `{"$email":true}`). Enforced on the same
    /// read/write/subscription seams as `owner_field`; additive to it. Marker
    /// values are valid only here — client `.filter()` queries reject them.
    /// Server-enforced; the client only declares it and round-trips it on the
    /// wire as `authorize`.
    pub fn authorize(mut self, predicate: FilterExpr) -> Self {
        self.authorize = Some(predicate);
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
            owner_field: self.owner_field,
            collaborators_field: self.collaborators_field,
            ttl: self.ttl,
            authorize: self.authorize,
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

    #[test]
    fn vector_index_with_empty_filter_fields_omits_key() {
        // `filter_fields: &[]` must serialize `vector` as `{"dimensions":N}` with
        // no `filterFields` key, mirroring the server's `skip_serializing_if =
        // "Vec::is_empty"` on the wire.
        let schema = Schema::builder()
            .table(
                "notes",
                Table::new()
                    .field("embedding", FieldType::vector(8))
                    .vector_index("by_embedding", "embedding", 8, &[]),
            )
            .build();
        let v = serde_json::to_value(&schema).unwrap();
        let idx = &v["tables"]["notes"]["indexes"][0];
        assert_eq!(idx["name"], json!("by_embedding"));
        assert_eq!(idx["vector"], json!({"dimensions": 8}));
        assert!(
            idx["vector"]
                .as_object()
                .expect("vector spec is object")
                .get("filterFields")
                .is_none(),
            "empty filter_fields must omit filterFields on the wire"
        );
    }

    #[test]
    fn owner_field_serializes_and_round_trips() {
        // `ownerField` is an opt-in, string-typed authorization hint: present on
        // the wire (camelCase) when set, omitted entirely when absent, mirroring
        // `server/src/schema.rs::TableDef` byte-for-byte. Enforcement is
        // server-only; the client only declares and round-trips it.
        let td = Table::new()
            .field("userId", FieldType::String)
            .field("title", FieldType::String)
            .index("by_user", &["userId"])
            .owner_field("userId")
            .finish();
        let json = serde_json::to_value(&td).unwrap();
        assert_eq!(json["ownerField"], "userId");
        // Round-trips back through the wire type.
        let back: TableDef = serde_json::from_value(json).unwrap();
        assert_eq!(back.owner_field.as_deref(), Some("userId"));
        // Absent -> omitted entirely (not serialized as null).
        let none = Table::new().field("title", FieldType::String).finish();
        assert!(
            !serde_json::to_string(&none).unwrap().contains("ownerField"),
            "ownerField must be omitted on the wire when unset"
        );
    }

    #[test]
    fn authorize_serializes_and_round_trips() {
        // `authorize` is the Model C opt-in predicate: present on the wire when
        // set, omitted entirely when absent, mirroring `server/src/schema.rs`
        // byte-for-byte. The predicate (with principal markers) survives a round
        // trip unchanged.
        let td = Table::new()
            .field("owner", FieldType::String)
            .field("visibility", FieldType::String)
            .authorize(FilterExpr::Or {
                exprs: vec![
                    FilterExpr::Eq {
                        field: "owner".into(),
                        value: json!({"$user": true}),
                    },
                    FilterExpr::Eq {
                        field: "visibility".into(),
                        value: json!("public"),
                    },
                ],
            })
            .finish();
        let json = serde_json::to_value(&td).unwrap();
        assert_eq!(
            json["authorize"],
            json!({
                "op": "or",
                "exprs": [
                    {"op":"eq","field":"owner","value":{"$user":true}},
                    {"op":"eq","field":"visibility","value":"public"}
                ]
            })
        );
        // Round-trips back through the wire type.
        let back: TableDef = serde_json::from_value(json).unwrap();
        assert!(back.authorize.is_some());
        // Absent -> omitted entirely (not serialized as null).
        let none = Table::new().field("title", FieldType::String).finish();
        assert!(
            !serde_json::to_string(&none).unwrap().contains("authorize"),
            "authorize must be omitted on the wire when unset"
        );
    }

    #[test]
    fn collaborators_field_serializes_and_round_trips() {
        // `collaboratorsField` mirrors `ownerField`'s opt-in, camelCase wire
        // shape: present when set, omitted entirely when absent. Composes with
        // `ownerField` (owner OR collaborator) and stands alone.
        let td = Table::new()
            .field("userId", FieldType::String)
            .field(
                "collaborators",
                FieldType::Array {
                    element: Box::new(FieldType::String),
                },
            )
            .field("title", FieldType::String)
            .index("by_user", &["userId"])
            .owner_field("userId")
            .collaborators_field("collaborators")
            .finish();
        let json = serde_json::to_value(&td).unwrap();
        assert_eq!(json["ownerField"], "userId");
        assert_eq!(json["collaboratorsField"], "collaborators");
        // Round-trips back through the wire type.
        let back: TableDef = serde_json::from_value(json).unwrap();
        assert_eq!(back.owner_field.as_deref(), Some("userId"));
        assert_eq!(back.collaborators_field.as_deref(), Some("collaborators"));
        // Absent -> omitted entirely (not serialized as null).
        let none = Table::new().field("title", FieldType::String).finish();
        assert!(
            !serde_json::to_string(&none)
                .unwrap()
                .contains("collaboratorsField"),
            "collaboratorsField must be omitted on the wire when unset"
        );
    }

    #[test]
    fn unique_index_builder_and_wire_shape() {
        // `.index(...).unique()` marks the most recently declared index as
        // unique, mirroring the TS client's chainable `.unique()`. On the wire
        // `unique: true` is emitted, and `where` stays omitted (the index is
        // not partial). A plain `.index(...)` omits both.
        let td = Table::new()
            .field("email", FieldType::String)
            .field("org", FieldType::String)
            .index("by_email", &["email"])
            .unique()
            .index("by_org", &["org"])
            .finish();
        // The unique flag lands on `by_email` only; `by_org` is a plain btree.
        let by_email = td
            .indexes
            .as_ref()
            .and_then(|i| i.iter().find(|x| x.name == "by_email"))
            .expect("by_email present");
        assert!(by_email.unique);
        assert!(by_email.r#where.is_none());
        let by_org = td
            .indexes
            .as_ref()
            .and_then(|i| i.iter().find(|x| x.name == "by_org"))
            .expect("by_org present");
        assert!(!by_org.unique);

        let v = serde_json::to_value(&td).unwrap();
        let email_idx = &v["indexes"][0];
        assert_eq!(email_idx["name"], json!("by_email"));
        assert_eq!(email_idx["unique"], json!(true));
        assert!(
            email_idx
                .as_object()
                .expect("by_email is object")
                .get("where")
                .is_none(),
            "where must be omitted when the index is not partial"
        );
        let org_idx = &v["indexes"][1];
        assert_eq!(org_idx["name"], json!("by_org"));
        assert!(
            org_idx
                .as_object()
                .expect("by_org is object")
                .get("unique")
                .is_none(),
            "unique must be omitted when false"
        );
        assert!(
            org_idx
                .as_object()
                .expect("by_org is object")
                .get("where")
                .is_none(),
            "where must be omitted when absent"
        );
        // Round-trips back: `unique: true` is read and a plain index
        // deserializes to `unique: false` / `where: None`.
        let back: TableDef = serde_json::from_value(v).unwrap();
        let by_email_back = back
            .indexes
            .as_ref()
            .and_then(|i| i.iter().find(|x| x.name == "by_email"))
            .expect("by_email present");
        assert!(by_email_back.unique);
        let by_org_back = back
            .indexes
            .as_ref()
            .and_then(|i| i.iter().find(|x| x.name == "by_org"))
            .expect("by_org present");
        assert!(!by_org_back.unique);
    }

    #[test]
    fn partial_unique_index_builder_and_wire_shape() {
        // `.where_clause(pred)` attaches a partial-index predicate to the most
        // recent index; the field is `r#where` so the on-wire key is `where`
        // (byte-identical to the server/TS/Python clients). Composes with
        // `.unique()` for a partial unique index.
        let td = Table::new()
            .field("email", FieldType::String)
            .field("archived", FieldType::optional(FieldType::Boolean))
            .index("by_email", &["email"])
            .unique()
            .where_clause(FilterExpr::Neq {
                field: "archived".into(),
                value: json!(true),
            })
            .finish();
        let v = serde_json::to_value(&td).unwrap();
        let idx = &v["indexes"][0];
        assert_eq!(idx["unique"], json!(true));
        assert_eq!(
            idx["where"],
            json!({"op":"neq","field":"archived","value":true})
        );
        // Round-trips: the predicate comes back as `where: Some(...)`.
        let back: TableDef = serde_json::from_value(v.clone()).unwrap();
        let idx_back = back
            .indexes
            .as_ref()
            .and_then(|i| i.first())
            .expect("index present");
        assert!(idx_back.unique);
        let pred = idx_back.r#where.as_ref().expect("predicate present");
        assert!(matches!(pred, FilterExpr::Neq { field, .. } if field == "archived"));
        // The snake-case field name never appears on the wire.
        assert!(!v.to_string().contains("r#where"));
        assert!(!v.to_string().contains("\"where_clause\""));
    }

    #[test]
    fn unique_and_where_setters_noop_before_any_index() {
        // Calling `.unique()` / `.where_clause()` before any `.index(...)`
        // must not panic (last_index is None → setters no-op), leaving the
        // table index-free.
        let td = Table::new()
            .field("email", FieldType::String)
            .unique()
            .where_clause(FilterExpr::Eq {
                field: "email".into(),
                value: json!("x"),
            })
            .finish();
        assert!(td.indexes.is_none());
    }

    // ---- is_widening_of (mirrors server/src/schema.rs tests) --------------

    fn union_of(vals: &[&str]) -> FieldType {
        FieldType::Union {
            variants: vals
                .iter()
                .map(|v| FieldType::Literal {
                    value: serde_json::Value::String((*v).to_string()),
                })
                .collect(),
        }
    }

    fn lit(s: &str) -> FieldType {
        FieldType::Literal {
            value: serde_json::Value::String(s.to_string()),
        }
    }

    #[test]
    fn is_widening_of_allows_adding_a_union_variant() {
        let old = union_of(&["low", "medium", "high"]);
        let new = union_of(&["low", "medium", "high", "critical"]);
        assert!(is_widening_of(&old, &new));
    }

    #[test]
    fn is_widening_of_rejects_narrowing_a_union() {
        let old = union_of(&["low", "medium", "high", "critical"]);
        let new = union_of(&["low", "medium", "high"]);
        assert!(!is_widening_of(&old, &new));
    }

    #[test]
    fn is_widening_of_allows_literal_to_union() {
        let old = FieldType::Literal {
            value: serde_json::Value::String("a".to_string()),
        };
        let new = union_of(&["a", "b"]);
        assert!(is_widening_of(&old, &new));
    }

    #[test]
    fn is_widening_of_rejects_scalar_swap_and_nonliteral_types() {
        assert!(!is_widening_of(&FieldType::Number, &FieldType::String));
        assert!(!is_widening_of(&FieldType::String, &union_of(&["a"])));
        // A union mixing a Literal with a non-literal variant is an open type.
        let mixed = FieldType::Union {
            variants: vec![
                FieldType::Literal {
                    value: serde_json::Value::String("a".to_string()),
                },
                FieldType::String,
            ],
        };
        assert!(!is_widening_of(&mixed, &union_of(&["a", "b"])));
        assert!(!is_widening_of(&union_of(&["a", "b"]), &mixed));
    }

    #[test]
    fn is_widening_of_rejects_replacing_a_single_literal() {
        // Lit("a") -> Lit("b"): "a" is not in {"b"} -> narrowing, rejected.
        assert!(!is_widening_of(&lit("a"), &lit("b")));
    }

    #[test]
    fn is_widening_of_rejects_collapsing_union_to_literal() {
        // Union{a,b} -> Lit(a): "b" is dropped -> rejected.
        assert!(!is_widening_of(&union_of(&["a", "b"]), &lit("a")));
    }

    #[test]
    fn is_widening_of_rejects_empty_union() {
        // An empty union accepts no values; refuse it outright rather than
        // returning a vacuous superset `true`.
        let empty = FieldType::Union { variants: vec![] };
        assert!(!is_widening_of(&empty, &union_of(&["a", "b"])));
        assert!(!is_widening_of(&union_of(&["a", "b"]), &empty));
    }
}
