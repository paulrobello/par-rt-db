//! Schema DSL type definitions -- `FieldType` (15 variants), `IndexDef` (btree /
//! full-text search / vector), `TtlDef`, `TableDef` (fields, indexes, per-row
//! `ownerField`/`collaboratorsField`/`authorize`, `ttl`), and `SchemaDef`. Wire
//! shapes use `#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]`
//! and are mirrored field-for-field by the swift/ts/python client SDKs (the Rust
//! client and server share this one definition, ARC-004). Structural and
//! value-level validation stay in each crate (`server::schema::value`,
//! `rust-client::in_memory::validate`) since they carry crate-specific error
//! types.

use std::collections::BTreeMap;

use crate::wire::{FilterExpr, ValueExpr};

/// Referential action applied to child rows when the referenced parent row is
/// hard-deleted (FM-33). Carried on the CHILD table's `id` field as an
/// additive `onDelete` wire key (`cascade` | `restrict` | `setNull`); the
/// cascade executes app-level inside `execute_txn` (not a SQL FK) so every
/// cascaded row is a first-class `DocOp`. See
/// `docs/superpowers/specs/2026-08-16-cascade-delete-soft-delete-design.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OnDeleteAction {
    /// Delete the child rows too.
    Cascade,
    /// Block the parent delete while live children reference it (Conflict).
    Restrict,
    /// Clear the child's referencing field (the key is removed).
    SetNull,
}

/// A field's declared type — the wire shape shared with the server (tagged
/// `{"type": "..."}`, camelCase).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum FieldType {
    /// JSON string.
    String,
    /// JSON number (f64).
    Number,
    /// JSON boolean.
    Boolean,
    /// JSON null.
    Null,
    /// Reference to a document in `table` (an id string on the wire).
    Id {
        /// The referenced table's name.
        table: String,
        /// `onDelete` referential action (FM-33). Legal only on a TOP-LEVEL
        /// field of the table (`Id` directly, or one `Optional` wrapping an
        /// `Id`). Omitted on the wire when `None`, so existing schemas
        /// deserialize unchanged.
        #[serde(default, rename = "onDelete", skip_serializing_if = "Option::is_none")]
        on_delete: Option<OnDeleteAction>,
    },
    /// Exactly one accepted literal value (enum-like).
    Literal {
        /// The accepted value.
        value: serde_json::Value,
    },
    /// `T | null`.
    Optional {
        /// The wrapped type.
        inner: Box<FieldType>,
    },
    /// Any one of the variants.
    Union {
        /// The accepted member types.
        variants: Vec<FieldType>,
    },
    /// Array of `element`.
    Array {
        /// The per-item type.
        element: Box<FieldType>,
    },
    /// Fixed-shape nested object.
    Object {
        /// The nested field names and types.
        fields: BTreeMap<String, FieldType>,
    },
    /// 64-bit integer (wire-encoded as a string to keep JSON precision).
    Int64,
    /// Binary payload (base64 on the wire).
    Bytes,
    /// Any JSON value.
    Any,
    /// Dynamic-key map with a uniform value type.
    Record {
        /// The per-value type.
        value: Box<FieldType>,
    },
    /// Embedding vector of fixed `dimensions` (pgvector).
    Vector {
        /// The fixed dimension count.
        dimensions: u32,
    },
}

impl FieldType {
    /// Shorthand for an id reference without an `onDelete` action.
    pub fn id(table: &str) -> Self {
        FieldType::Id {
            table: table.into(),
            on_delete: None,
        }
    }
    /// Declare the `onDelete` referential action on an id field (FM-33):
    /// `.on_delete(OnDeleteAction::Cascade)` after `FieldType::id(table)`.
    /// Only the `Id` variant carries the action — on any other variant this
    /// is a no-op (server push validation rejects a mis-placed `onDelete`
    /// anyway, and only a top-level `Id` or `Optional<Id>` is legal).
    pub fn on_delete(mut self, action: OnDeleteAction) -> Self {
        if let FieldType::Id { on_delete, .. } = &mut self {
            *on_delete = Some(action);
        }
        self
    }
    /// Wrap `inner` as `Optional`.
    pub fn optional(inner: FieldType) -> Self {
        FieldType::Optional {
            inner: Box::new(inner),
        }
    }
    /// A lone accepted literal value.
    pub fn literal(value: impl Into<serde_json::Value>) -> Self {
        FieldType::Literal {
            value: value.into(),
        }
    }
    /// A union over the given variants.
    pub fn union(variants: impl IntoIterator<Item = FieldType>) -> Self {
        FieldType::Union {
            variants: variants.into_iter().collect(),
        }
    }
    /// An array of `element`.
    pub fn array(element: FieldType) -> Self {
        FieldType::Array {
            element: Box::new(element),
        }
    }
    /// An embedding vector type of `dimensions`.
    pub fn vector(dimensions: u32) -> Self {
        FieldType::Vector { dimensions }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
/// One declared index on a table (btree, search, or vector).
pub struct IndexDef {
    /// Index name (used in queries' `with_index`).
    pub name: String,
    /// The indexed field names, in key order.
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
    /// Cosine distance (default).
    #[default]
    Cosine,
    /// Euclidean L2 distance.
    L2,
    /// Inner product.
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

    /// Returns `true` for the default metric, so `VectorIndexSpec` can omit it
    /// on the wire (backward compatible with pre-metric schemas).
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
    /// Vector dimension count.
    pub dimensions: u32,
    /// Scalar fields usable as eq-filters in `vectorSearch`.
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
/// field whose value is each document's absolute epoch-ms expiry; a per-db
/// reaper deletes rows whose value is in the past. `default_duration_ms`
/// stamps the field at insert time when the client omits it. See
/// `docs/superpowers/specs/2026-08-01-document-ttl-design.md`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TtlDef {
    /// The declared numeric field holding each doc's epoch-ms expiry.
    pub field: String,
    /// Stamped at insert time when the document omits `field`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_duration_ms: Option<i64>,
}

/// One table: fields, indexes, and opt-in per-row rules / TTL / defaults.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TableDef {
    /// Field name → declared type.
    pub fields: BTreeMap<String, FieldType>,
    /// Declared indexes, if any.
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
    pub computed: BTreeMap<String, ValueExpr>,
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

/// A whole schema: named tables. Pushed via `POST /admin/push-schema`.
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct SchemaDef {
    /// Table name → definition.
    pub tables: BTreeMap<String, TableDef>,
}

/// Table and index names cap at 30 chars, and field names at 60 chars, so that
/// the DDL layer's physical names (`t_`/`f_` prefixes, `i_<table>_<index>`
/// composition, all lowercased) stay within Postgres's 63-byte identifier limit:
/// worst case `i_` + 30 + `_` + 30 = 63, exactly at the limit.
pub const MAX_TABLE_NAME_LEN: usize = 30;
/// See [`MAX_TABLE_NAME_LEN`].
pub const MAX_INDEX_NAME_LEN: usize = 30;
/// See [`MAX_TABLE_NAME_LEN`].
pub const MAX_FIELD_NAME_LEN: usize = 60;

/// Strips every `Id` variant's `on_delete` action (recursively through
/// `Optional`/`Union`/`Array`/`Record`/`Object`), keeping the referenced
/// `table`. Used by the additive-push comparison (`ddl::detect_destructive_changes`
/// and the advisory `schema_diff`) so adding or changing `onDelete` on an
/// existing field is additive, while changing the referenced table is still a
/// type change. A change to the action alters runtime delete behavior only —
/// no stored row shape — which is why it is not destructive.
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
pub fn literal_set(ty: &FieldType) -> Option<Vec<&serde_json::Value>> {
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
