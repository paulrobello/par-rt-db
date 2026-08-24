//! Schema DSL type definitions -- `FieldType` (15 variants), `IndexDef` (btree /
//! full-text search / vector), `TtlDef`, `TableDef` (fields, indexes, per-row
//! `ownerField`/`collaboratorsField`/`authorize`, `ttl`), and `SchemaDef`. Wire
//! shapes use `#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]`
//! and are mirrored field-for-field by the three client SDKs. See `schema::validate`
//! for structural validation and `schema::value` for value-level validation.

use std::collections::BTreeMap;

use crate::query::FilterExpr;

/// Referential action applied to child rows when the referenced parent row is
/// hard-deleted (FM-33). Carried on the CHILD table's `id` field as an
/// additive `onDelete` wire key (`cascade` | `restrict` | `setNull`); the
/// cascade executes app-level inside `execute_txn` (not a SQL FK) so every
/// cascaded row is a first-class `DocOp`. See
/// `docs/superpowers/specs/2026-08-16-cascade-delete-soft-delete-design.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OnDeleteAction {
    Cascade,
    Restrict,
    SetNull,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
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
        /// `Id`). Omitted on the wire when `None`, so existing schemas
        /// deserialize unchanged.
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

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IndexDef {
    pub name: String,
    pub fields: Vec<String>,
    /// `true` marks a full-text search index: its text `fields` are tsvectorized
    /// into a generated column with a GIN index and ranked by `ts_rank` via the
    /// `search` query terminal. Omitted on the wire for ordinary btree indexes,
    /// so existing schemas (and client payloads carrying only `name`/`fields`)
    /// deserialize unchanged.
    #[serde(default, skip_serializing_if = "is_false")]
    pub search: bool,
    /// When present, marks this as a vector index: `fields[0]` must name a
    /// `Vector { dimensions }` field whose dimensions match `vector.dimensions`,
    /// and `filter_fields` (if any) must be scalar-indexable columns used to
    /// pre-filter nearest-neighbor queries. Omitted on the wire for btree/search
    /// indexes, so existing schemas deserialize unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<VectorIndexSpec>,
    /// `true` compiles to `CREATE UNIQUE INDEX`. Legal only on a plain btree
    /// index (rejected alongside `search`/`vector`). Omitted on the wire when
    /// false, so existing schemas deserialize unchanged.
    #[serde(default, skip_serializing_if = "is_false")]
    pub unique: bool,
    /// Optional partial-index predicate baked into `CREATE INDEX … WHERE`. Same
    /// `FilterExpr` type as the query-time `filter()` terminal, but compiled to
    /// literal SQL at DDL time (Postgres forbids bind params here). Omitted on
    /// the wire when `None`. Wire key is `where` (Rust keyword ⇒ raw identifier).
    #[serde(default, rename = "where", skip_serializing_if = "Option::is_none")]
    pub r#where: Option<FilterExpr>,
    /// Optional full-text-search language for a search index: a Postgres
    /// `regconfig` name (e.g. `english`, `simple`, `spanish`) used to build the
    /// index's generated `tsvector` column and to parse `search`/`hybridSearch`
    /// query text, so non-English corpora get correct stemming and stop-words.
    /// Valid only on a search index; format-checked here and existence-checked
    /// against `pg_ts_config` at push time. Omitted on the wire when `None`, so
    /// existing schemas deserialize unchanged and behave exactly as today
    /// (`english`). See ENH-006.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
}

/// Distance metric for a vector index. Selects the pgvector opclass used to
/// build the HNSW index and the operator used to rank `vectorSearch`/hybrid
/// results. Wire form is lowercase (`cosine` | `l2` | `ip`); default `cosine`
/// (today's only behavior), omitted on the wire so existing schemas stay
/// byte-identical. See ENH-007.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DistanceMetric {
    #[default]
    Cosine,
    L2,
    Ip,
}

impl DistanceMetric {
    /// pgvector access-method opclass for this metric (HNSW `CREATE INDEX`).
    pub fn opclass(self) -> &'static str {
        match self {
            Self::Cosine => "vector_cosine_ops",
            Self::L2 => "vector_l2_ops",
            Self::Ip => "vector_ip_ops",
        }
    }

    /// pgvector distance operator for this metric (`ORDER BY ... <op> $q`).
    pub fn distance_op(self) -> &'static str {
        match self {
            Self::Cosine => "<=>",
            Self::L2 => "<->",
            // Negative inner product; ascending order = most-similar-first,
            // consistent with `<=>`/`<->`.
            Self::Ip => "<#>",
        }
    }

    fn is_cosine(&self) -> bool {
        matches!(self, Self::Cosine)
    }
}

/// Declaration of a vector (approximate nearest-neighbor) index. Carried
/// alongside the btree/search knobs on `IndexDef`. Wire shape is camelCase
/// (`filterFields`, `metric`) to match the rest of the protocol.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorIndexSpec {
    pub dimensions: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub filter_fields: Vec<String>,
    #[serde(default, skip_serializing_if = "DistanceMetric::is_cosine")]
    pub metric: DistanceMetric,
}

fn is_false(b: &bool) -> bool {
    !b
}

/// Declarative document TTL (auto-expiry). `field` names a declared numeric
/// field whose value is each document's absolute epoch-ms expiry; a per-db
/// reaper deletes rows whose value is in the past. `default_duration_ms`
/// stamps the field at insert time when the client omits it. See
/// `docs/superpowers/specs/2026-08-01-document-ttl-design.md`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TtlDef {
    pub field: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_duration_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TableDef {
    pub fields: BTreeMap<String, FieldType>,
    #[serde(default)]
    pub indexes: Vec<IndexDef>,
    /// Opt-in per-row authorization: names a declared, string-compatible
    /// field whose value is the owning user's `user_id`. When set, an
    /// authenticated user reads/mutates only their own rows on this table;
    /// machine tokens and scheduled jobs bypass. Server-enforced; clients
    /// only declare it. Additive — schemas without it deserialize unchanged.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "ownerField"
    )]
    pub owner_field: Option<String>,
    /// Opt-in extension of `owner_field` per-row authorization: names a declared
    /// array-of-strings (or array-of-id) field whose values are additional user
    /// ids that may read/mutate the row. When set alongside `owner_field`, a
    /// user may access a row if they are the owner OR appear in the array. May
    /// be declared alone (collaborators-only). Additive — schemas without it
    /// deserialize unchanged and behave exactly as owner-only.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "collaboratorsField"
    )]
    pub collaborators_field: Option<String>,
    /// Declarative document TTL. When `Some`, a per-db reaper deletes rows
    /// whose `ttl.field` value is in the past. Additive — schemas without it
    /// deserialize unchanged. See `TtlDef`.
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "ttl")]
    pub ttl: Option<TtlDef>,
    /// Server-stamped update timestamp (FM-36): names a declared
    /// `number`/`int64` field the server stamps with the current epoch-ms on
    /// every version-bumping write to the row — insert, patch, replace,
    /// upsert-update, patchByQuery, and cascade setNull — overwriting any
    /// client-supplied value (the `ownerField` authority model). Snapshot
    /// import replays stored docs verbatim and never re-stamps. Additive —
    /// schemas without it deserialize unchanged.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "updatedAtField"
    )]
    pub updated_at_field: Option<String>,
    /// Server-assigned per-table monotonic counter: names a declared `int64`
    /// field stamped from a per-table Postgres sequence on insert (and
    /// upsert's insert branch), overwriting any client-supplied value (the
    /// `ownerField` authority model). Immutable after insert — a patch or
    /// replace that changes the stored value is rejected. Legal in a unique
    /// index (the ticket-number guarantee). Gaps are possible on rolled-back
    /// transactions: sequences are not gap-free. Snapshot import replays
    /// stored values verbatim and repositions each sequence past the imported
    /// max, so numbering continues after a restore. Additive — schemas
    /// without it deserialize unchanged.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "autoIncrementField"
    )]
    pub auto_increment_field: Option<String>,
    /// Opt-in per-row authorization predicate (Model C). A general
    /// `FilterExpr` over this table's declared doc fields and the principal's
    /// markers (`{"$user":true}` / `{"$email":true}`). Enforced on the same
    /// read/write/subscription seams as `owner_field`; additive to it. When
    /// unset, behavior is unchanged. Marker values are valid only here —
    /// client `.filter()` queries reject them (Task 6, via
    /// `validate_filter_expr_fields` with `allow_principal_markers = false`).
    /// Server-validated at schema-push and migrate time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorize: Option<FilterExpr>,
    /// Field-level default values (FM-32). Applied to a NEW document
    /// (insert / replace / upsert-insert) when it omits the key; `patch`
    /// never re-applies, so clearing an optional field stays cleared.
    /// Values are literals validated at push time against the field's
    /// type. Stamped server values (ttl default, ownerField, authorize
    /// `$user`) win over a default on the same field. Additive — schemas
    /// without it deserialize unchanged.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub defaults: BTreeMap<String, serde_json::Value>,
    /// Computed fields (ENH-028): field name → expression. The server
    /// re-evaluates each expression on every write and stores the result in
    /// the doc (overwriting any client-supplied value — the `ownerField`
    /// authority model); a null result removes the key. Push-time validation
    /// is `validate_computed`: keys must be declared non-stamped fields,
    /// referenced fields declared and non-computed, `Case.whens` reject
    /// principal markers, and a statically-known result kind must be
    /// acceptable to the field's type. Additive — schemas without it
    /// deserialize unchanged.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub computed: BTreeMap<String, crate::value_expr::ValueExpr>,
    /// Soft delete (FM-33): `Delete`/`DeleteByQuery` on this table stamp a
    /// real `deleted_at timestamptz` column (row persists, invisible to every
    /// read terminal and write lookup) instead of removing the row; unique
    /// indexes gain `AND "deleted_at" IS NULL` so soft-deleted rows never
    /// conflict; `Step::Undelete` restores. The TTL reaper still hard-deletes.
    /// Omitted on the wire when false, so existing schemas deserialize
    /// unchanged.
    #[serde(default, rename = "softDelete", skip_serializing_if = "is_false")]
    pub soft_delete: bool,
}

/// Strips every `Id` variant's `on_delete` action (recursively through
/// `Optional`/`Union`/`Array`/`Record`/`Object`), keeping the referenced
/// `table`. Used by the additive-push comparison (`ddl::detect_destructive_changes`
/// and the advisory `schema_diff`) so adding or changing `onDelete` on an
/// existing field is additive, while changing the referenced table is still a
/// type change. A change to the action alters runtime delete behavior only —
/// no stored row shape — which is why it is not destructive.
pub(crate) fn strip_on_delete(ty: &FieldType) -> FieldType {
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

#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct SchemaDef {
    pub tables: BTreeMap<String, TableDef>,
}

/// Table and index names cap at 30 chars, and field names at 60 chars, so that
/// the DDL layer's physical names (`t_`/`f_` prefixes, `i_<table>_<index>`
/// composition, all lowercased) stay within Postgres's 63-byte identifier limit:
/// worst case `i_` + 30 + `_` + 30 = 63, exactly at the limit.
pub(crate) const MAX_TABLE_NAME_LEN: usize = 30;
pub(crate) const MAX_INDEX_NAME_LEN: usize = 30;
pub(crate) const MAX_FIELD_NAME_LEN: usize = 60;
