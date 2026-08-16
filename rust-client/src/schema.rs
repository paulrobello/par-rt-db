//! Schema DSL: builds the exact `SchemaDef` JSON consumed by `POST /admin/push-schema`.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::wire::FilterExpr;

/// Referential action applied to child rows when the referenced parent row is
/// hard-deleted (FM-33). Carried on the CHILD table's `id` field as an
/// additive `onDelete` wire key (`cascade` | `restrict` | `setNull`); the
/// cascade executes app-level inside the server's `execute_txn` (not a SQL
/// FK) so every cascaded row is a first-class op. Mirrors
/// `server/src/schema.rs::OnDeleteAction` byte-for-byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OnDeleteAction {
    Cascade,
    Restrict,
    SetNull,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum FieldType {
    String,
    Number,
    Boolean,
    Null,
    Id {
        table: String,
        /// `onDelete` referential action (FM-33). Legal only on a TOP-LEVEL
        /// field of the table (`Id` directly, or one `Optional` wrapping an
        /// `Id`; server push validation enforces this). Omitted on the wire
        /// when `None`, so existing schemas deserialize unchanged. Mirrors
        /// `server/src/schema.rs::FieldType` byte-for-byte.
        #[serde(default, rename = "onDelete", skip_serializing_if = "Option::is_none")]
        on_delete: Option<OnDeleteAction>,
    },
    Literal {
        value: serde_json::Value,
    },
    Optional {
        inner: Box<FieldType>,
    },
    Union {
        variants: Vec<FieldType>,
    },
    Array {
        element: Box<FieldType>,
    },
    Object {
        fields: BTreeMap<String, FieldType>,
    },
    Int64,
    Bytes,
    Any,
    Record {
        value: Box<FieldType>,
    },
    Vector {
        dimensions: u32,
    },
}

impl FieldType {
    pub fn id(table: &str) -> Self {
        FieldType::Id {
            table: table.into(),
            on_delete: None,
        }
    }
    /// Declare the `onDelete` referential action on an id field (FM-33):
    /// `.on_delete(OnDeleteAction::Cascade)` after `FieldType::id(table)`.
    /// Mirrors the TS client's chainable `.onDelete(action)`. Only the `Id`
    /// variant carries the action — on any other variant this is a no-op
    /// (server push validation rejects a mis-placed `onDelete` anyway, and
    /// only a top-level `Id` or `Optional<Id>` is legal).
    pub fn on_delete(mut self, action: OnDeleteAction) -> Self {
        if let FieldType::Id { on_delete, .. } = &mut self {
            *on_delete = Some(action);
        }
        self
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

/// Recursively strips every `Id`'s `on_delete` action from `ty`, keeping the
/// referenced `table` (FM-33). Used by the in-memory harness's
/// `detect_destructive_changes` (mirroring `server/src/ddl.rs`): adding or
/// changing an `onDelete` action alters runtime delete behavior only (no
/// stored row shape), so it is additive, while changing the referenced table
/// is still a type change. Public for the same reason as
/// [`is_widening_of`]: its only in-crate consumer is the `in_memory` feature,
/// and `pub` keeps the feature-less lib build free of dead-code warnings.
pub fn strip_on_delete(ty: &FieldType) -> FieldType {
    match ty {
        FieldType::Id { table, .. } => FieldType::Id {
            table: table.clone(),
            on_delete: None,
        },
        FieldType::Optional { inner } => FieldType::Optional {
            inner: Box::new(strip_on_delete(inner)),
        },
        FieldType::Union { variants } => FieldType::Union {
            variants: variants.iter().map(strip_on_delete).collect(),
        },
        FieldType::Array { element } => FieldType::Array {
            element: Box::new(strip_on_delete(element)),
        },
        FieldType::Object { fields } => FieldType::Object {
            fields: fields
                .iter()
                .map(|(k, v)| (k.clone(), strip_on_delete(v)))
                .collect(),
        },
        FieldType::Record { value } => FieldType::Record {
            value: Box::new(strip_on_delete(value)),
        },
        other => other.clone(),
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
    /// Full-text search language for a search index — a Postgres `regconfig`
    /// name (e.g. `"english"`, `"simple"`, `"spanish"`) that the server uses to
    /// tsvectorize the index's text `fields`. Only meaningful when `search: true`;
    /// the server default (field absent) behaves as `english`. Mirrors
    /// `server/src/schema.rs::IndexDef` byte-for-byte: omitted on the wire when
    /// `None`, so existing schemas serialize unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
}

/// Distance metric for a vector index. Mirrors
/// `server/src/schema.rs::DistanceMetric` byte-for-byte: serializes as
/// lowercase `"cosine" | "l2" | "ip"`. The default (`Cosine`) is omitted on
/// the wire by `VectorIndexSpec`'s `skip_serializing_if`, so existing schemas
/// serialize identically — backward compatible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DistanceMetric {
    #[default]
    Cosine,
    L2,
    Ip,
}

impl DistanceMetric {
    /// Returns `true` for the default metric, so `VectorIndexSpec` can omit it
    /// on the wire (backward compatible with pre-metric schemas).
    fn is_cosine(&self) -> bool {
        matches!(self, Self::Cosine)
    }
}

/// Declaration of a vector (approximate nearest-neighbor) index. Wire shape is
/// camelCase (`filterFields`, and `metric` as lowercase `"cosine"|"l2"|"ip"`)
/// to match the rest of the protocol. Mirrors `server/src/schema.rs::VectorIndexSpec`
/// byte-for-byte — including the optional `metric` field (default `Cosine`,
/// omitted on the wire).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorIndexSpec {
    pub dimensions: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub filter_fields: Vec<String>,
    /// Distance metric used by this index (default `Cosine`). Omitted on the
    /// wire when `Cosine`, so existing schemas serialize identically.
    #[serde(default, skip_serializing_if = "DistanceMetric::is_cosine")]
    pub metric: DistanceMetric,
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
    /// Field-level default values (FM-32). Applied to a NEW document
    /// (insert / replace / upsert-insert) when it omits the key; `patch`
    /// never re-applies, so clearing an optional field stays cleared.
    /// Values are literals the server validates at push time against the
    /// field's type. Stamped server values (ttl default, ownerField,
    /// authorize `$user`) win over a default on the same field. Additive —
    /// schemas without it deserialize unchanged. Mirrors
    /// `server/src/schema.rs::TableDef` byte-for-byte.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub defaults: BTreeMap<String, serde_json::Value>,
    /// Soft delete (FM-33): `Delete`/`DeleteByQuery` rows on this table are
    /// STAMPED (`deleted_at`) instead of removed — invisible to every read and
    /// write lookup, restorable via the `undelete` mutation step, physically
    /// removed only by the TTL reaper. Omitted on the wire when false, so
    /// existing schemas deserialize unchanged. Mirrors
    /// `server/src/schema.rs::TableDef` byte-for-byte.
    #[serde(default, rename = "softDelete", skip_serializing_if = "is_false")]
    pub soft_delete: bool,
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
    defaults: BTreeMap<String, serde_json::Value>,
    soft_delete: bool,
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
            defaults: BTreeMap::new(),
            soft_delete: false,
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
            language: None,
        });
        self.last_index = Some(self.indexes.len() - 1);
        self
    }

    /// Declare a full-text search index. The server tsvectorizes the (text)
    /// `fields` and ranks matches via the `search` query terminal. Pass `language`
    /// to override the server's default (`english`) Postgres `regconfig` (e.g.
    /// `"spanish"`, `"simple"`); `None` omits the field on the wire and lets the
    /// server default apply.
    pub fn search_index(mut self, name: &str, fields: &[&str], language: Option<&str>) -> Self {
        self.indexes.push(IndexDef {
            name: name.into(),
            fields: fields.iter().map(|s| (*s).into()).collect(),
            search: true,
            vector: None,
            unique: false,
            r#where: None,
            language: language.map(|s| s.into()),
        });
        self.last_index = Some(self.indexes.len() - 1);
        self
    }

    /// Declare a vector index over a `Vector`-typed `field`. The server stores a
    /// pgvector column ranked by the given `metric` (`Cosine` by default) via
    /// the `vectorSearch` terminal; `filter_fields` names scalar fields usable
    /// as eq-filters there.
    pub fn vector_index(
        mut self,
        name: &str,
        field: &str,
        dimensions: u32,
        filter_fields: &[&str],
        metric: DistanceMetric,
    ) -> Self {
        self.indexes.push(IndexDef {
            name: name.into(),
            fields: vec![field.into()],
            search: false,
            vector: Some(VectorIndexSpec {
                dimensions,
                filter_fields: filter_fields.iter().map(|s| (*s).into()).collect(),
                metric,
            }),
            unique: false,
            r#where: None,
            language: None,
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

    /// Declare field-level default values (FM-32). Each entry's key must name a
    /// declared field and its value a non-null literal satisfying that field's
    /// type (the server validates this at push time). The server stamps a
    /// default onto a NEW document (insert / replace / upsert-insert) when it
    /// omits the key; `patch` never re-applies. Server-stamped values (ttl
    /// default, ownerField) win over a default on the same field.
    pub fn defaults(mut self, entries: &[(&str, serde_json::Value)]) -> Self {
        for (field, value) in entries {
            self.defaults.insert((*field).to_string(), value.clone());
        }
        self
    }

    /// Declare soft delete (FM-33): rows on this table are stamped
    /// (`deleted_at`) instead of removed on delete — invisible to every read
    /// and write lookup, restorable via the `undelete` mutation step. Mirrors
    /// the TS client's chainable `.softDelete()`; round-trips on the wire as
    /// `softDelete: true` (omitted when false).
    pub fn soft_delete(mut self) -> Self {
        self.soft_delete = true;
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
            defaults: self.defaults,
            soft_delete: self.soft_delete,
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
                table: "projects".into(),
                on_delete: None,
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
                    .search_index("search_content", &["title", "body"], None),
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
    fn search_index_language_serializes_and_round_trips() {
        // A search index WITHOUT `language` omits the key on the wire (the server
        // default `english` applies), and one WITH a language carries it and
        // round-trips. Mirrors `server/src/schema.rs::IndexDef`'s
        // `skip_serializing_if = "Option::is_none"` on the `language` field.
        let schema = Schema::builder()
            .table(
                "notes",
                Table::new()
                    .field("title", FieldType::String)
                    .field("body", FieldType::String)
                    .search_index("search_default", &["title", "body"], None)
                    .search_index("search_spanish", &["title", "body"], Some("spanish")),
            )
            .build();
        let v = serde_json::to_value(&schema).unwrap();
        let idxs = &v["tables"]["notes"]["indexes"];
        // No-language index: `language` key is absent on the wire.
        let default_idx = &idxs[0];
        assert_eq!(default_idx["name"], json!("search_default"));
        assert_eq!(default_idx["search"], json!(true));
        assert!(
            default_idx
                .as_object()
                .expect("default search index is object")
                .get("language")
                .is_none(),
            "language must be omitted on the wire when None"
        );
        // With-language index: `language` is present and carries the regconfig.
        let spanish_idx = &idxs[1];
        assert_eq!(spanish_idx["name"], json!("search_spanish"));
        assert_eq!(spanish_idx["language"], json!("spanish"));

        // Round-trips: `None` stays `None`; `Some("spanish")` is preserved.
        let back: SchemaDef = serde_json::from_value(v).unwrap();
        let notes = back.tables.get("notes").expect("notes present");
        let default_back = notes
            .indexes
            .as_ref()
            .and_then(|i| i.iter().find(|x| x.name == "search_default"))
            .expect("default search index present");
        assert!(default_back.language.is_none());
        let spanish_back = notes
            .indexes
            .as_ref()
            .and_then(|i| i.iter().find(|x| x.name == "search_spanish"))
            .expect("spanish search index present");
        assert_eq!(spanish_back.language.as_deref(), Some("spanish"));

        // A schema that never carried a `language` key (legacy) deserializes to
        // `language: None`, so existing wire payloads stay compatible.
        let legacy = json!({
            "tables": {
                "notes": {
                    "fields": {"body": {"type":"string"}},
                    "indexes": [{"name":"search_content","fields":["body"],"search":true}]
                }
            }
        });
        let from_legacy: SchemaDef = serde_json::from_value(legacy).unwrap();
        let legacy_idx = from_legacy
            .tables
            .get("notes")
            .and_then(|t| t.indexes.as_ref())
            .and_then(|i| i.first())
            .expect("legacy index present");
        assert!(legacy_idx.language.is_none());
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
                    .vector_index(
                        "by_embedding",
                        "embedding",
                        4,
                        &["userId"],
                        DistanceMetric::Cosine,
                    ),
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
                    .vector_index("by_embedding", "embedding", 8, &[], DistanceMetric::Cosine),
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
    fn vector_index_metric_serializes_and_round_trips() {
        // `metric` serializes as a lowercase string and is omitted on the wire
        // when `Cosine` (the default), mirroring `server/src/schema.rs`'s
        // `skip_serializing_if = "DistanceMetric::is_cosine"`. A non-cosine
        // metric round-trips; a missing metric deserializes to `Cosine`.
        let l2 = VectorIndexSpec {
            dimensions: 4,
            filter_fields: vec![],
            metric: DistanceMetric::L2,
        };
        let l2_json = serde_json::to_value(&l2).unwrap();
        assert_eq!(l2_json["metric"], json!("l2"));

        let cosine = VectorIndexSpec {
            dimensions: 4,
            filter_fields: vec![],
            metric: DistanceMetric::Cosine,
        };
        let cosine_json = serde_json::to_value(&cosine).unwrap();
        assert!(
            cosine_json
                .as_object()
                .expect("vector spec is object")
                .get("metric")
                .is_none(),
            "metric must be omitted on the wire when Cosine"
        );

        // Round-trip: `"l2"` deserializes back to `DistanceMetric::L2`.
        let back: VectorIndexSpec = serde_json::from_value(l2_json).unwrap();
        assert_eq!(back.metric, DistanceMetric::L2);
        // A spec serialized without a `metric` key (as all pre-metric schemas
        // carry) deserializes to the default `Cosine`.
        let legacy = json!({"dimensions": 4, "filterFields": []});
        let from_legacy: VectorIndexSpec = serde_json::from_value(legacy).unwrap();
        assert_eq!(from_legacy.metric, DistanceMetric::Cosine);
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
    fn defaults_serializes_and_round_trips() {
        // `defaults` (FM-32) is a field-name → literal map: present on the wire
        // when non-empty, omitted entirely when empty, mirroring
        // `server/src/schema.rs::TableDef`'s `skip_serializing_if =
        // "BTreeMap::is_empty"` byte-for-byte.
        let td = Table::new()
            .field(
                "status",
                FieldType::union([FieldType::literal("backlog"), FieldType::literal("done")]),
            )
            .field("priority", FieldType::Number)
            .defaults(&[("status", json!("backlog")), ("priority", json!(0))])
            .finish();
        let v = serde_json::to_value(&td).unwrap();
        assert_eq!(v["defaults"], json!({"status": "backlog", "priority": 0}));
        // Round-trips back through the wire type.
        let back: TableDef = serde_json::from_value(v).unwrap();
        assert_eq!(back.defaults.get("status"), Some(&json!("backlog")));
        assert_eq!(back.defaults.get("priority"), Some(&json!(0)));
        // Empty -> omitted entirely (not serialized as `{}` or null).
        let none = Table::new().field("title", FieldType::String).finish();
        assert!(
            !serde_json::to_string(&none).unwrap().contains("defaults"),
            "defaults must be omitted on the wire when empty"
        );
        // A table that never carried a `defaults` key (legacy wire payload)
        // deserializes to an empty map.
        let legacy = json!({"fields": {"title": {"type": "string"}}});
        let from_legacy: TableDef = serde_json::from_value(legacy).unwrap();
        assert!(from_legacy.defaults.is_empty());
    }

    #[test]
    fn on_delete_serializes_and_round_trips() {
        // FM-33: `onDelete` rides on the id field — camelCase action tags
        // (`cascade` | `restrict` | `setNull`), present only when set, mirroring
        // `server/src/schema.rs::FieldType` byte-for-byte. A plain
        // `FieldType::id(...)` and a legacy wire payload both stay `None`, so
        // existing schemas serialize/deserialize unchanged.
        for (action, wire) in [
            (OnDeleteAction::Cascade, "cascade"),
            (OnDeleteAction::Restrict, "restrict"),
            (OnDeleteAction::SetNull, "setNull"),
        ] {
            let v = serde_json::to_value(FieldType::id("projects").on_delete(action)).unwrap();
            assert_eq!(
                v,
                json!({"type":"id","table":"projects","onDelete":wire}),
                "action {wire:?} must serialize as the camelCase wire tag"
            );
            // Round-trips back through the wire type.
            let back: FieldType = serde_json::from_value(v).unwrap();
            assert_eq!(back, FieldType::id("projects").on_delete(action));
        }

        // `None` omits the key entirely (not serialized as null).
        let none = serde_json::to_value(FieldType::id("projects")).unwrap();
        assert_eq!(none, json!({"type":"id","table":"projects"}));

        // setNull composes with the `Optional` wrapper — the legal shape for a
        // nullable reference (server push validation requires the wrapper).
        let optional =
            FieldType::optional(FieldType::id("projects").on_delete(OnDeleteAction::SetNull));
        let v = serde_json::to_value(&optional).unwrap();
        assert_eq!(
            v,
            json!({"type":"optional","inner":{"type":"id","table":"projects","onDelete":"setNull"}})
        );
        let back: FieldType = serde_json::from_value(v).unwrap();
        assert_eq!(back, optional);
    }

    #[test]
    fn on_delete_builder_is_additive_and_noop_off_id() {
        // `.on_delete(...)` chains after `FieldType::id(...)` without disturbing
        // existing call sites, and no-ops on a non-Id variant (server push
        // validation rejects a misplaced onDelete anyway).
        let with = FieldType::id("projects").on_delete(OnDeleteAction::Cascade);
        assert_eq!(
            with,
            FieldType::Id {
                table: "projects".into(),
                on_delete: Some(OnDeleteAction::Cascade),
            }
        );
        // Calling it twice overwrites (last wins), matching a struct-field set.
        let swapped = FieldType::id("projects")
            .on_delete(OnDeleteAction::Cascade)
            .on_delete(OnDeleteAction::Restrict);
        assert_eq!(
            swapped,
            FieldType::Id {
                table: "projects".into(),
                on_delete: Some(OnDeleteAction::Restrict),
            }
        );
        // A non-Id variant passes through unchanged.
        let passthrough = FieldType::optional(FieldType::String).on_delete(OnDeleteAction::Cascade);
        assert_eq!(passthrough, FieldType::optional(FieldType::String));
    }

    #[test]
    fn soft_delete_serializes_and_round_trips() {
        // FM-33: `softDelete` is a table flag — present (camelCase) when true,
        // omitted entirely when false, mirroring `server/src/schema.rs::TableDef`
        // byte-for-byte. A legacy payload without the key deserializes to false.
        let td = Table::new()
            .field("title", FieldType::String)
            .soft_delete()
            .finish();
        let v = serde_json::to_value(&td).unwrap();
        assert_eq!(v["softDelete"], json!(true));
        // Round-trips back through the wire type.
        let back: TableDef = serde_json::from_value(v).unwrap();
        assert!(back.soft_delete);
        // False -> omitted entirely (not serialized as null or false).
        let none = Table::new().field("title", FieldType::String).finish();
        let none_json = serde_json::to_string(&none).unwrap();
        assert!(
            !none_json.contains("softDelete"),
            "softDelete must be omitted on the wire when false"
        );
        let from_none: TableDef = serde_json::from_str(&none_json).unwrap();
        assert!(!from_none.soft_delete);
        // A table that never carried a `softDelete` key (legacy wire payload)
        // deserializes to false.
        let legacy = json!({"fields": {"title": {"type": "string"}}});
        let from_legacy: TableDef = serde_json::from_value(legacy).unwrap();
        assert!(!from_legacy.soft_delete);
    }

    #[test]
    fn strip_on_delete_removes_actions_keeps_tables() {
        // FM-33: strip every Id's onDelete recursively (through
        // Optional/Union/Array/Object/Record) while keeping the referenced
        // table — the harness's detect_destructive_changes compares
        // stripped types so adding/changing an action is additive.
        let ty = FieldType::Object {
            fields: BTreeMap::from([
                (
                    "parent".into(),
                    FieldType::optional(
                        FieldType::id("projects").on_delete(OnDeleteAction::SetNull),
                    ),
                ),
                (
                    "aliases".into(),
                    FieldType::Array {
                        element: Box::new(
                            FieldType::id("users").on_delete(OnDeleteAction::Cascade),
                        ),
                    },
                ),
                ("plain".into(), FieldType::id("teams")),
                ("name".into(), FieldType::String),
            ]),
        };
        let stripped = strip_on_delete(&ty);
        assert_eq!(
            stripped,
            FieldType::Object {
                fields: BTreeMap::from([
                    (
                        "parent".into(),
                        FieldType::optional(FieldType::id("projects")),
                    ),
                    (
                        "aliases".into(),
                        FieldType::Array {
                            element: Box::new(FieldType::id("users")),
                        },
                    ),
                    ("plain".into(), FieldType::id("teams")),
                    ("name".into(), FieldType::String),
                ]),
            },
            "every Id keeps its table but loses the action"
        );
        // Stripping is idempotent, and a differing ACTION strips to equal
        // while a differing TABLE does not (that stays a type change).
        assert_eq!(strip_on_delete(&stripped), stripped);
        let cascade = FieldType::id("projects").on_delete(OnDeleteAction::Cascade);
        let restrict = FieldType::id("projects").on_delete(OnDeleteAction::Restrict);
        assert_eq!(strip_on_delete(&cascade), strip_on_delete(&restrict));
        assert_ne!(
            strip_on_delete(&cascade),
            strip_on_delete(&FieldType::id("users"))
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
