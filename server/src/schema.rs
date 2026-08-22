//! Schema DSL — `FieldType` (15 variants), `IndexDef` (btree / full-text search
//! / vector), `TtlDef`, `TableDef` (fields, indexes, per-row `ownerField`/
//! `collaboratorsField`/`authorize`, `ttl`), and `SchemaDef`. Wire shapes use
//! `#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]` and
//! are mirrored field-for-field by the three client SDKs. Validation
//! (`validate_doc`/`validate_value`/`validate_filter_expr_fields`) is shared with
//! the read and write paths; the index-value typing here must stay aligned with
//! `ddl` (one typed column per indexed field). Schema changes are additive-only.

use std::collections::{BTreeMap, HashSet};

use base64::{Engine as _, engine::general_purpose::STANDARD};

use crate::error::RtDbError;
use crate::query::FilterExpr;
use crate::value_expr::ValueExpr;

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

/// Identifier regex `^[a-zA-Z][a-zA-Z0-9_]*$` bounded to `max_len` total chars.
pub(crate) fn is_valid_identifier(s: &str, max_len: usize) -> bool {
    if s.is_empty() || s.len() > max_len {
        return false;
    }
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// A Postgres text-search `regconfig` name (`pg_ts_config.cfgname`): a lowercase
/// identifier like `english`, `simple`, `spanish`. This only gates the literal
/// interpolated into `to_tsvector('<lang>'::regconfig, …)` so it can never break
/// out of the string or inject SQL; existence is re-checked against
/// `pg_ts_config` at push time (`ddl::validate_search_languages`).
pub(crate) fn is_valid_regconfig(s: &str) -> bool {
    if s.is_empty() || s.len() > 63 {
        return false;
    }
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_lowercase() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
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
        | FieldType::Id { .. }
        | FieldType::Int64
        | FieldType::Bytes
        | FieldType::Any => Ok(()),
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
        FieldType::Record { value } => validate_field_type(value),
        FieldType::Vector { .. } => Ok(()),
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

/// True if `v` is a principal marker: `{"$user": true}` or `{"$email": true}`.
/// Markers are valid only in a server-declared `authorize` predicate; the query
/// boundary (Task 6) rejects them in client `.filter()` expressions via
/// `validate_filter_expr_fields(_, _, allow_principal_markers = false)`.
fn is_principal_marker(v: &serde_json::Value) -> bool {
    if let serde_json::Value::Object(map) = v
        && map.len() == 1
    {
        return matches!(map.get("$user").and_then(|x| x.as_bool()), Some(true))
            || matches!(map.get("$email").and_then(|x| x.as_bool()), Some(true));
    }
    false
}

fn check_field_declared(field: &str, table: &TableDef) -> Result<(), RtDbError> {
    if !table.fields.contains_key(field) {
        return Err(RtDbError::schema(format!(
            "filter references undeclared field '{field}'"
        )));
    }
    Ok(())
}

/// Walk `expr` validating its field references against `table`'s declared
/// fields. Reused by `validate_structure` (for the server-declared `authorize`
/// predicate, `allow_principal_markers = true`) and by the query boundary in
/// Task 6 (for client `.filter()` expressions, `allow_principal_markers = false`).
///
/// - Every `field` (eq/neq/in/gt/gte/lt/lte/contains/exists/olderThan) must
///   name a declared field.
/// - `Contains` requires an array-of-strings field (`is_string_array_field`),
///   since it compiles to a jsonb membership test against a bound text uid.
/// - Comparison fields (gt/gte/lt/lte) must be scalar-indexable
///   (`indexed_column_type` ok) so the SQL and doc evaluators can order them.
/// - When `allow_principal_markers` is false, any principal marker appearing in
///   a value position is rejected (Task 6's client-filter guard).
/// - `olderThan` is accepted only when `allow_relative_time` is true (the
///   by-query step filters — `compile_scan_where`); everywhere else (read
///   filters, `authorize`, computed `case` whens, migrate filters) it is
///   rejected. Requires a declared `number`/`int64` field and `ms >= 0`.
/// - `And`/`Or`/`Not` recurse.
pub fn validate_filter_expr_fields(
    expr: &FilterExpr,
    table: &TableDef,
    allow_principal_markers: bool,
    allow_relative_time: bool,
) -> Result<(), RtDbError> {
    match expr {
        FilterExpr::Eq { field, value }
        | FilterExpr::Neq { field, value }
        | FilterExpr::Gt { field, value }
        | FilterExpr::Gte { field, value }
        | FilterExpr::Lt { field, value }
        | FilterExpr::Lte { field, value } => {
            check_field_declared(field, table)?;
            if !allow_principal_markers && is_principal_marker(value) {
                return Err(RtDbError::schema(format!(
                    "principal markers ({{\"$user\":true}}/{{\"$email\":true}}) are not allowed in client filters (field '{field}')"
                )));
            }
            if matches!(
                expr,
                FilterExpr::Gt { .. }
                    | FilterExpr::Gte { .. }
                    | FilterExpr::Lt { .. }
                    | FilterExpr::Lte { .. }
            ) {
                let fty = &table.fields[field];
                if indexed_column_type(fty).is_err() {
                    return Err(RtDbError::schema(format!(
                        "field '{field}' must be a scalar indexable type for comparison"
                    )));
                }
            }
        }
        FilterExpr::OlderThan { field, ms } => {
            if !allow_relative_time {
                return Err(RtDbError::schema(
                    "olderThan filter is only allowed in patchByQuery/deleteByQuery filters"
                        .to_string(),
                ));
            }
            if *ms < 0 {
                return Err(RtDbError::schema("olderThan ms must be >= 0".to_string()));
            }
            check_field_declared(field, table)?;
            let fty = &table.fields[field];
            let inner = match fty {
                FieldType::Optional { inner } => inner.as_ref(),
                _ => fty,
            };
            if !matches!(inner, FieldType::Number | FieldType::Int64) {
                return Err(RtDbError::schema(format!(
                    "field '{field}' must be a number or int64 field for olderThan"
                )));
            }
        }
        FilterExpr::In { field, values } => {
            check_field_declared(field, table)?;
            if !allow_principal_markers {
                for v in values {
                    if is_principal_marker(v) {
                        return Err(RtDbError::schema(format!(
                            "principal markers ({{\"$user\":true}}/{{\"$email\":true}}) are not allowed in client filters (field '{field}')"
                        )));
                    }
                }
            }
        }
        FilterExpr::Contains { field, value } => {
            check_field_declared(field, table)?;
            let fty = &table.fields[field];
            if !is_string_array_field(fty) {
                return Err(RtDbError::schema(format!(
                    "field '{field}' must be an array-of-strings (or array-of-id) field for contains"
                )));
            }
            if !allow_principal_markers && is_principal_marker(value) {
                return Err(RtDbError::schema(format!(
                    "principal markers ({{\"$user\":true}}/{{\"$email\":true}}) are not allowed in client filters (field '{field}')"
                )));
            }
        }
        FilterExpr::Exists { field } => {
            check_field_declared(field, table)?;
        }
        FilterExpr::And { exprs } | FilterExpr::Or { exprs } => {
            for e in exprs {
                validate_filter_expr_fields(
                    e,
                    table,
                    allow_principal_markers,
                    allow_relative_time,
                )?;
            }
        }
        FilterExpr::Not { expr } => {
            validate_filter_expr_fields(expr, table, allow_principal_markers, allow_relative_time)?;
        }
    }
    Ok(())
}

/// Whether a field type is array-of-strings-compatible: `Array<T>` (or
/// `Optional<Array<T>>`) where `T`'s `indexed_column_type` maps to Postgres
/// `text`. Used by `collaboratorsField` validation — the jsonb `?` membership
/// test is sound against a bound text uid only when the array elements are
/// themselves string-compatible. Unwraps one layer of `Optional` to admit
/// nullable collaborator arrays, mirroring `owner_field`'s Optional handling.
fn is_string_array_field(ty: &FieldType) -> bool {
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

impl TableDef {
    fn validate_structure(&self, table_name: &str) -> Result<(), RtDbError> {
        // QA-002: extracted the six independent cascade stages into named
        // helpers so this reads as a routing table. Order matters — early
        // failures short-circuit later checks exactly as before.
        self.validate_field_names(table_name)?;
        self.validate_owner_field()?;
        self.validate_collaborators_field()?;
        if let Some(authorize) = &self.authorize {
            // Principal markers are valid here (rejected in client filters by
            // Task 6 at the query boundary via the same walker with `false`).
            validate_filter_expr_fields(authorize, self, true, false)?;
        }
        self.validate_indexes(table_name)?;
        self.validate_ttl()?;
        self.validate_updated_at()?;
        self.validate_auto_increment()?;
        self.validate_defaults(table_name)?;
        self.validate_computed(table_name)?;
        self.validate_on_delete(table_name)?;
        Ok(())
    }

    fn validate_field_names(&self, table_name: &str) -> Result<(), RtDbError> {
        let mut lower_field_names = HashSet::new();
        for (field_name, field_type) in &self.fields {
            if !is_valid_identifier(field_name, MAX_FIELD_NAME_LEN) {
                return Err(RtDbError::schema(format!(
                    "table '{table_name}' has invalid field name '{field_name}'"
                )));
            }
            if !lower_field_names.insert(field_name.to_lowercase()) {
                return Err(RtDbError::schema(format!(
                    "table '{table_name}' has field name '{field_name}' that collides case-insensitively with another field"
                )));
            }
            validate_field_type(field_type)?;
        }
        Ok(())
    }

    fn validate_owner_field(&self) -> Result<(), RtDbError> {
        if let Some(owner) = &self.owner_field {
            if !is_valid_identifier(owner, MAX_FIELD_NAME_LEN) {
                return Err(RtDbError::schema(format!(
                    "ownerField '{owner}' is not a valid identifier"
                )));
            }
            let field_type = self.fields.get(owner).ok_or_else(|| {
                RtDbError::schema(format!("ownerField '{owner}' is not a declared field"))
            })?;
            // The owner value is a user_id (string); the field must be
            // string-compatible so the equality predicate is sound and (if indexed)
            // can back a typed text column. `indexed_column_type` admits Number/
            // Boolean too, so require the resulting pg type to be "text".
            let (pg_type, _) = indexed_column_type(field_type)?;
            if pg_type != "text" {
                return Err(RtDbError::schema(format!(
                    "ownerField '{owner}' must be a string-compatible field (string/id/literal/union of strings)"
                )));
            }
        }
        Ok(())
    }

    fn validate_collaborators_field(&self) -> Result<(), RtDbError> {
        if let Some(collab) = &self.collaborators_field {
            if !is_valid_identifier(collab, MAX_FIELD_NAME_LEN) {
                return Err(RtDbError::schema(format!(
                    "collaboratorsField '{collab}' is not a valid identifier"
                )));
            }
            let field_type = self.fields.get(collab).ok_or_else(|| {
                RtDbError::schema(format!(
                    "collaboratorsField '{collab}' is not a declared field"
                ))
            })?;
            // The collaborators value is a jsonb array of user_ids; the element
            // type must be string-compatible so the jsonb `?` membership test is
            // sound against the bound uid. Admit `Optional<Array<String>>` for
            // the same reason `owner_field` admits `Optional<String>`.
            if !is_string_array_field(field_type) {
                return Err(RtDbError::schema(format!(
                    "collaboratorsField '{collab}' must be an array-of-strings (or array-of-id) field"
                )));
            }
        }
        Ok(())
    }

    fn validate_indexes(&self, table_name: &str) -> Result<(), RtDbError> {
        let mut index_names = HashSet::new();
        for index in &self.indexes {
            if !is_valid_identifier(&index.name, MAX_INDEX_NAME_LEN) {
                return Err(RtDbError::schema(format!(
                    "table '{table_name}' has invalid index name '{}'",
                    index.name
                )));
            }
            if !index_names.insert(index.name.to_lowercase()) {
                return Err(RtDbError::schema(format!(
                    "table '{table_name}' has duplicate index name '{}' (case-insensitive)",
                    index.name
                )));
            }
            if index.fields.is_empty() {
                return Err(RtDbError::schema(format!(
                    "index '{}' on table '{table_name}' has no fields",
                    index.name
                )));
            }
            // An index is exactly one of: btree, search, or vector. A vector
            // index is validated here and skips the btree per-field loop below
            // (its `fields[0]` is a Vector column, which is not btree-indexable).
            if index.search && index.vector.is_some() {
                return Err(RtDbError::schema(format!(
                    "index '{}' cannot be both search and vector",
                    index.name
                )));
            }
            // `unique` and `where` are btree-only knobs: a unique partial index
            // compiles to `CREATE UNIQUE INDEX … WHERE`, which is meaningless on
            // a GIN tsvector (search) or HNSW vector index. Reject them here so
            // a DDL-time failure never reaches `ddl.rs`.
            if index.unique || index.r#where.is_some() {
                if index.search {
                    return Err(RtDbError::schema(format!(
                        "index '{}' cannot combine unique/where with search",
                        index.name
                    )));
                }
                if index.vector.is_some() {
                    return Err(RtDbError::schema(format!(
                        "index '{}' cannot combine unique/where with a vector index",
                        index.name
                    )));
                }
            }
            // `language` selects a search index's tsvector `regconfig`; it is
            // meaningless on a btree or vector index, and the literal is later
            // interpolated into DDL, so both its placement and format are gated
            // here. Existence against `pg_ts_config` is checked at push time.
            if let Some(lang) = &index.language {
                if !index.search {
                    return Err(RtDbError::schema(format!(
                        "index '{}' declares language '{}' but is not a search index",
                        index.name, lang
                    )));
                }
                if !is_valid_regconfig(lang) {
                    return Err(RtDbError::schema(format!(
                        "search index '{}' has invalid language '{}' (expected a lowercase regconfig name like 'english')",
                        index.name, lang
                    )));
                }
            }
            if let Some(vec_spec) = &index.vector {
                if vec_spec.dimensions == 0 {
                    return Err(RtDbError::schema(format!(
                        "vector index '{}' must declare a positive dimensions count",
                        index.name
                    )));
                }
                if index.fields.len() != 1 {
                    return Err(RtDbError::schema(format!(
                        "vector index '{}' must declare exactly one vector field",
                        index.name
                    )));
                }
                let vfield = &index.fields[0];
                let fty = self.fields.get(vfield).ok_or_else(|| {
                    RtDbError::schema(format!(
                        "vector index '{}' references unknown field '{vfield}'",
                        index.name
                    ))
                })?;
                match fty {
                    FieldType::Vector { dimensions } if *dimensions == vec_spec.dimensions => {}
                    _ => {
                        return Err(RtDbError::schema(format!(
                            "vector index '{}' field '{vfield}' must be Vector{{dimensions:{}}}",
                            index.name, vec_spec.dimensions
                        )));
                    }
                }
                for ff in &vec_spec.filter_fields {
                    let fty = self.fields.get(ff).ok_or_else(|| {
                        RtDbError::schema(format!(
                            "vector index '{}' filterField '{ff}' is not a declared field",
                            index.name
                        ))
                    })?;
                    if indexed_column_type(fty).is_err() {
                        return Err(RtDbError::schema(format!(
                            "vector index '{}' filterField '{ff}' must be a scalar indexable type",
                            index.name
                        )));
                    }
                }
                continue;
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
                let (pg_type, _) = indexed_column_type(field_type)?;
                if index.search && pg_type != "text" {
                    return Err(RtDbError::schema(format!(
                        "search index '{}' on table '{table_name}' has non-text field '{field_name}'",
                        index.name
                    )));
                }
            }
        }
        Ok(())
    }

    fn validate_ttl(&self) -> Result<(), RtDbError> {
        if let Some(ttl) = &self.ttl {
            if !is_valid_identifier(&ttl.field, MAX_FIELD_NAME_LEN) {
                return Err(RtDbError::schema(format!(
                    "ttl.field '{}' is not a valid identifier",
                    ttl.field
                )));
            }
            let fty = self.fields.get(&ttl.field).ok_or_else(|| {
                RtDbError::schema(format!("ttl.field '{}' is not a declared field", ttl.field))
            })?;
            if !matches!(fty, FieldType::Number | FieldType::Int64) {
                return Err(RtDbError::schema(format!(
                    "ttl.field '{}' must be a number or bigint field",
                    ttl.field
                )));
            }
            let has_ttl_index = self.indexes.iter().any(|idx| {
                !idx.search
                    && idx.vector.is_none()
                    && !idx.unique
                    && idx.r#where.is_none()
                    && idx.fields.len() == 1
                    && idx.fields[0] == ttl.field
            });
            if !has_ttl_index {
                return Err(RtDbError::schema(format!(
                    "ttl.field '{}' requires a single-field, non-unique, non-partial btree index on it",
                    ttl.field
                )));
            }
            if let Some(d) = ttl.default_duration_ms
                && d <= 0
            {
                return Err(RtDbError::schema(
                    "ttl.defaultDurationMs must be greater than 0".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// `updatedAtField` push validation: the field must be declared with a
    /// numeric type (the stamp is an epoch-ms number — a decimal string on an
    /// `int64` field, matching the int64 wire convention) and must differ
    /// from `ttl.field` (both stamps write unconditionally, so a shared field
    /// would silently drop the expiry). No index is required: the stamp never
    /// queries the field, it is indexable like any other numeric field.
    fn validate_updated_at(&self) -> Result<(), RtDbError> {
        if let Some(field) = &self.updated_at_field {
            if !is_valid_identifier(field, MAX_FIELD_NAME_LEN) {
                return Err(RtDbError::schema(format!(
                    "updatedAtField '{field}' is not a valid identifier"
                )));
            }
            let fty = self.fields.get(field).ok_or_else(|| {
                RtDbError::schema(format!("updatedAtField '{field}' is not a declared field"))
            })?;
            if !matches!(fty, FieldType::Number | FieldType::Int64) {
                return Err(RtDbError::schema(format!(
                    "updatedAtField '{field}' must be a number or bigint field"
                )));
            }
            if self.ttl.as_ref().is_some_and(|ttl| &ttl.field == field) {
                return Err(RtDbError::schema(format!(
                    "updatedAtField '{field}' must differ from ttl.field (both stamps write unconditionally; a shared field would drop the expiry)"
                )));
            }
        }
        Ok(())
    }

    /// `autoIncrementField` push validation: the field must be declared
    /// `int64` exactly (the sequence produces int64; a `number` would lose
    /// precision, an `optional` would admit a missing counter) and must
    /// differ from `ttl.field` and `updatedAtField` (both stamp
    /// unconditionally on writes the counter must survive verbatim). A
    /// `defaults` entry on the field is allowed but always loses to the
    /// stamp — same authority family as the ttl default.
    fn validate_auto_increment(&self) -> Result<(), RtDbError> {
        if let Some(field) = &self.auto_increment_field {
            if !is_valid_identifier(field, MAX_FIELD_NAME_LEN) {
                return Err(RtDbError::schema(format!(
                    "autoIncrementField '{field}' is not a valid identifier"
                )));
            }
            let fty = self.fields.get(field).ok_or_else(|| {
                RtDbError::schema(format!(
                    "autoIncrementField '{field}' is not a declared field"
                ))
            })?;
            if !matches!(fty, FieldType::Int64) {
                return Err(RtDbError::schema(format!(
                    "autoIncrementField '{field}' must be an int64 field"
                )));
            }
            if self.ttl.as_ref().is_some_and(|ttl| &ttl.field == field) {
                return Err(RtDbError::schema(format!(
                    "autoIncrementField '{field}' must differ from ttl.field (the ttl reaper would delete counter rows)"
                )));
            }
            if self.updated_at_field.as_ref().is_some_and(|at| at == field) {
                return Err(RtDbError::schema(format!(
                    "autoIncrementField '{field}' must differ from updatedAtField (the timestamp would overwrite the counter on every write)"
                )));
            }
        }
        Ok(())
    }

    fn validate_defaults(&self, table_name: &str) -> Result<(), RtDbError> {
        for (field, value) in &self.defaults {
            let fty = self.fields.get(field).ok_or_else(|| {
                RtDbError::schema(format!(
                    "defaults key '{field}' is not a declared field of table '{table_name}'"
                ))
            })?;
            if value.is_null() {
                return Err(RtDbError::schema(format!(
                    "defaults value for '{table_name}.{field}' must not be null"
                )));
            }
            if !validate_value(fty, value) {
                return Err(RtDbError::schema(format!(
                    "defaults value for '{table_name}.{field}' does not match the field type"
                )));
            }
        }
        Ok(())
    }

    /// Computed-field push validation (ENH-028). Rules, in order:
    /// 1. every `computed` key names a declared field;
    /// 2. the key is not one of the server-stamped declaration fields
    ///    (`ownerField`/`collaboratorsField`/`autoIncrementField`) — those
    ///    carry their own stamping authority and a computed entry would fight
    ///    it on every write;
    /// 3. every field the expression references (including `Case.when`
    ///    filter fields) is declared and not itself computed (no chained or
    ///    cyclic evaluation);
    /// 4. `Case.when` filters reject principal markers — computed exprs run
    ///    on every write with no interactive principal, so a `$user`/`$email`
    ///    marker has no value to resolve;
    /// 5. when the expression's result kind is statically known, the field's
    ///    type must accept a value of that kind;
    /// 6. the table's `authorize` predicate references no computed field —
    ///    on insert/upsert-insert `verify_authorize_doc` runs before computed
    ///    stamping, so such a predicate would evaluate forgeable client
    ///    input instead of the server-derived value.
    fn validate_computed(&self, table_name: &str) -> Result<(), RtDbError> {
        for (field, expr) in &self.computed {
            if !self.fields.contains_key(field) {
                return Err(RtDbError::bad_request(format!(
                    "computed field '{table_name}.{field}' is not a declared field"
                )));
            }
            if self.owner_field.as_deref() == Some(field.as_str()) {
                return Err(RtDbError::bad_request(format!(
                    "computed field '{table_name}.{field}' must not be the table's ownerField"
                )));
            }
            if self.collaborators_field.as_deref() == Some(field.as_str()) {
                return Err(RtDbError::bad_request(format!(
                    "computed field '{table_name}.{field}' must not be the table's collaboratorsField"
                )));
            }
            if self.auto_increment_field.as_deref() == Some(field.as_str()) {
                return Err(RtDbError::bad_request(format!(
                    "computed field '{table_name}.{field}' must not be the table's autoIncrementField"
                )));
            }
            // First offense wins; the walk covers `Field` nodes and every
            // `Case.when` filter field.
            let mut offender: Option<String> = None;
            crate::value_expr::walk_value_expr_fields(expr, &mut |referenced| {
                if offender.is_some() {
                    return;
                }
                if !self.fields.contains_key(referenced) {
                    offender = Some(format!(
                        "computed field '{table_name}.{field}' references undeclared field '{referenced}'"
                    ));
                } else if self.computed.contains_key(referenced) {
                    offender = Some(format!(
                        "computed field '{table_name}.{field}' references computed field '{referenced}' (computed fields may not reference each other)"
                    ));
                }
            });
            if let Some(message) = offender {
                return Err(RtDbError::bad_request(message));
            }
            validate_computed_case_whens(expr, self)?;
            if let Some(kind) = infer_static_kind(expr) {
                let sample = match kind {
                    StaticKind::String => serde_json::json!("s"),
                    StaticKind::Number => serde_json::json!(1),
                    StaticKind::Boolean => serde_json::json!(true),
                };
                // `validate_value` is the wire contract, but int64's wire form
                // is a decimal STRING: a Number-kind result can never validate
                // (arithmetic yields JSON numbers), while a String-kind one
                // can ("42") — decimal-ness stays a runtime `validate_doc`
                // check. Optional unwrapping admits the nullable spelling.
                let mut inner = &self.fields[field];
                while let FieldType::Optional { inner: deeper } = inner {
                    inner = deeper;
                }
                let accepts = validate_value(&self.fields[field], &sample)
                    || (matches!(inner, FieldType::Int64) && matches!(kind, StaticKind::String));
                if !accepts {
                    return Err(RtDbError::bad_request(format!(
                        "computed field '{table_name}.{field}' produces {}, which the field type does not accept",
                        kind.as_str()
                    )));
                }
            }
        }
        // Rule 6: authorize runs pre-stamp on the insert paths, so a
        // predicate over a computed field would read client input.
        if let Some(authorize) = &self.authorize {
            let mut offender: Option<String> = None;
            crate::value_expr::walk_filter_expr_fields(authorize, &mut |referenced| {
                if offender.is_none() && self.computed.contains_key(referenced) {
                    offender = Some(referenced.to_string());
                }
            });
            if let Some(field) = offender {
                return Err(RtDbError::bad_request(format!(
                    "computed field '{table_name}.{field}' must not be referenced by the table's authorize predicate (authorize predicates may not reference computed fields)"
                )));
            }
        }
        Ok(())
    }

    /// `onDelete` push validation (FM-33). An action is legal only on a
    /// TOP-LEVEL field in one of two shapes — `Id { on_delete: Some(_) }` or
    /// `Optional { inner: Id { on_delete: Some(_) } }` (deeper nesting has no
    /// well-defined "the ref field" to index or null). `setNull` additionally
    /// requires the `Optional` wrapper (the cleared doc must stay valid — a
    /// required id field cannot hold null). The field must carry a
    /// single-field, non-unique, non-partial btree index on it, mirroring the
    /// ttl rule: the cascade lookup `WHERE f_<field> = $1` must be an index
    /// scan, and a partial `where` could hide children and orphan them.
    /// Referenced-table existence needs whole-schema access and is checked in
    /// [`SchemaDef::validate`]'s second pass. Self-reference is legal.
    fn validate_on_delete(&self, table_name: &str) -> Result<(), RtDbError> {
        for (field_name, field_type) in &self.fields {
            // Resolve the (optional) top-level shape; anything else with an
            // action embedded deeper is rejected below by the walker.
            let (action, is_optional) = match field_type {
                FieldType::Id { on_delete, .. } => (*on_delete, false),
                FieldType::Optional { inner } => match &**inner {
                    FieldType::Id { on_delete, .. } => (*on_delete, true),
                    _ => {
                        // No top-level action here — but an action nested
                        // deeper (union/object/array/deeper optional) is
                        // illegal.
                        if self.field_has_nested_on_delete(field_type) {
                            return Err(RtDbError::schema(format!(
                                "field '{field_name}' on table '{table_name}': onDelete is legal only on a top-level id or optional-id field"
                            )));
                        }
                        continue;
                    }
                },
                _ => {
                    if self.field_has_nested_on_delete(field_type) {
                        return Err(RtDbError::schema(format!(
                            "field '{field_name}' on table '{table_name}': onDelete is legal only on a top-level id or optional-id field"
                        )));
                    }
                    continue;
                }
            };
            let Some(action) = action else {
                continue;
            };
            if action == OnDeleteAction::SetNull && !is_optional {
                return Err(RtDbError::schema(format!(
                    "field '{field_name}' on table '{table_name}': onDelete 'setNull' requires the id field to be optional"
                )));
            }
            let has_ref_index = self.indexes.iter().any(|idx| {
                !idx.search
                    && idx.vector.is_none()
                    && !idx.unique
                    && idx.r#where.is_none()
                    && idx.fields.len() == 1
                    && idx.fields[0] == *field_name
            });
            if !has_ref_index {
                return Err(RtDbError::schema(format!(
                    "onDelete field '{field_name}' on table '{table_name}' requires a single-field, non-unique, non-partial btree index on it"
                )));
            }
        }
        Ok(())
    }

    /// Whether any `Id` variant reachable through the type's compositors
    /// carries an `onDelete` action — used to reject actions nested deeper
    /// than the two legal top-level shapes.
    fn field_has_nested_on_delete(&self, ty: &FieldType) -> bool {
        match ty {
            FieldType::Id { on_delete, .. } => on_delete.is_some(),
            FieldType::Optional { inner }
            | FieldType::Array { element: inner }
            | FieldType::Record { value: inner } => self.field_has_nested_on_delete(inner),
            FieldType::Union { variants } => {
                variants.iter().any(|v| self.field_has_nested_on_delete(v))
            }
            FieldType::Object { fields } => {
                fields.values().any(|v| self.field_has_nested_on_delete(v))
            }
            _ => false,
        }
    }

    pub fn index(&self, name: &str) -> Result<&IndexDef, RtDbError> {
        self.indexes
            .iter()
            .find(|index| index.name == name)
            .ok_or_else(|| RtDbError::bad_request(format!("index '{name}' not found")))
    }
}

/// Walks a computed expression's `Case` nodes validating each `when` filter
/// with the marker-rejecting mode of `validate_filter_expr_fields` — the same
/// call the query boundary uses for client filters, and the opposite of the
/// `authorize` path's marker-allowing call. `then`/`otherwise` recurse so a
/// `Case` nested inside a branch is covered.
fn validate_computed_case_whens(ve: &ValueExpr, table: &TableDef) -> Result<(), RtDbError> {
    match ve {
        ValueExpr::Case { whens, otherwise } => {
            for cw in whens {
                validate_filter_expr_fields(&cw.when, table, false, false)
                    .map_err(|e| RtDbError::bad_request(e.message))?;
                validate_computed_case_whens(&cw.then, table)?;
            }
            validate_computed_case_whens(otherwise, table)
        }
        ValueExpr::Concat { parts } | ValueExpr::Coalesce { parts } => {
            for p in parts {
                validate_computed_case_whens(p, table)?;
            }
            Ok(())
        }
        ValueExpr::Add { left, right }
        | ValueExpr::Sub { left, right }
        | ValueExpr::Mul { left, right }
        | ValueExpr::Div { left, right } => {
            validate_computed_case_whens(left, table)?;
            validate_computed_case_whens(right, table)
        }
        ValueExpr::Lower { value }
        | ValueExpr::Upper { value }
        | ValueExpr::Trim { value }
        | ValueExpr::Cast { value, .. } => validate_computed_case_whens(value, table),
        ValueExpr::Field { .. } | ValueExpr::Literal { .. } | ValueExpr::Now => Ok(()),
    }
}

/// The statically-known result kind of a `ValueExpr`, for the computed-field
/// push check. `None` means the result kind varies by input — `Field` (text
/// extraction of any JSON value), `Coalesce`/`Case` (whichever branch wins),
/// and the null / object / array literals whose runtime `validate_doc` check
/// is the only guard.
enum StaticKind {
    String,
    Number,
    Boolean,
}

impl StaticKind {
    fn as_str(&self) -> &'static str {
        match self {
            StaticKind::String => "a string",
            StaticKind::Number => "a number",
            StaticKind::Boolean => "a boolean",
        }
    }
}

fn infer_static_kind(ve: &ValueExpr) -> Option<StaticKind> {
    match ve {
        ValueExpr::Field { .. } | ValueExpr::Coalesce { .. } | ValueExpr::Case { .. } => None,
        ValueExpr::Literal { value } => match value {
            serde_json::Value::String(_) => Some(StaticKind::String),
            serde_json::Value::Number(_) => Some(StaticKind::Number),
            serde_json::Value::Bool(_) => Some(StaticKind::Boolean),
            serde_json::Value::Null
            | serde_json::Value::Object(_)
            | serde_json::Value::Array(_) => None,
        },
        ValueExpr::Concat { .. }
        | ValueExpr::Lower { .. }
        | ValueExpr::Upper { .. }
        | ValueExpr::Trim { .. }
        | ValueExpr::Cast {
            to: crate::value_expr::Cast::ToString,
            ..
        } => Some(StaticKind::String),
        ValueExpr::Add { .. }
        | ValueExpr::Sub { .. }
        | ValueExpr::Mul { .. }
        | ValueExpr::Div { .. }
        | ValueExpr::Cast {
            to: crate::value_expr::Cast::ToNumber | crate::value_expr::Cast::ToInt64,
            ..
        }
        | ValueExpr::Now => Some(StaticKind::Number),
        ValueExpr::Cast {
            to: crate::value_expr::Cast::ToBoolean,
            ..
        } => Some(StaticKind::Boolean),
    }
}

/// Validates every table's computed-field map (ENH-028) — the schema-level
/// entry point behind `TableDef::validate_structure`, also called directly by
/// `migrate::plan_migration` so directive folding (e.g. `changeType`) that
/// invalidates a computed entry fails at plan time, before any DB work.
pub fn validate_computed(schema: &SchemaDef) -> Result<(), RtDbError> {
    for (table_name, table) in &schema.tables {
        table.validate_computed(table_name)?;
    }
    Ok(())
}

impl SchemaDef {
    /// Structural validation: identifier regexes (Global Constraints), case-insensitive
    /// table uniqueness, index names unique per table and matching the field-name regex,
    /// index fields exist and are indexable, Literal values scalar, Union non-empty,
    /// reserved field names rejected: any starting with "_" .
    pub fn validate(&self) -> Result<(), RtDbError> {
        let mut lower_names = HashSet::new();
        for (table_name, table_def) in &self.tables {
            if !is_valid_identifier(table_name, MAX_TABLE_NAME_LEN) {
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
        // FM-33 second pass (needs whole-schema access): every top-level
        // `onDelete` id field must reference a table declared in this schema.
        for (table_name, table_def) in &self.tables {
            for (field_name, field_type) in &table_def.fields {
                let ref_table = match field_type {
                    FieldType::Id {
                        table,
                        on_delete: Some(_),
                    } => Some(table),
                    FieldType::Optional { inner } => match &**inner {
                        FieldType::Id {
                            table,
                            on_delete: Some(_),
                        } => Some(table),
                        _ => continue,
                    },
                    _ => continue,
                };
                if let Some(ref_table) = ref_table
                    && !self.tables.contains_key(ref_table)
                {
                    return Err(RtDbError::schema(format!(
                        "onDelete field '{field_name}' on table '{table_name}' references unknown table '{ref_table}'"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Reject a schema whose table count exceeds `cap`. `cap == 0` is unlimited.
    /// Counted as `tables.len()` (user-declared tables only).
    pub fn check_table_quota(&self, cap: usize) -> Result<(), RtDbError> {
        if cap > 0 && self.tables.len() > cap {
            return Err(RtDbError::quota_exceeded(format!(
                "db has {} table(s), limit is {cap}",
                self.tables.len()
            )));
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
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
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
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields: BTreeMap::from([("a-b".to_string(), FieldType::String)]),
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn accepts_table_name_at_max_length_30() {
        let name = "a".repeat(30);
        let schema = SchemaDef {
            tables: BTreeMap::from([(name, simple_table())]),
        };
        assert!(schema.validate().is_ok());
    }

    #[test]
    fn rejects_table_name_over_max_length_30() {
        let name = "a".repeat(31);
        let schema = SchemaDef {
            tables: BTreeMap::from([(name, simple_table())]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn accepts_field_name_at_max_length_60() {
        let field_name = "a".repeat(60);
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields: BTreeMap::from([(field_name, FieldType::String)]),
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_ok());
    }

    #[test]
    fn rejects_field_name_over_max_length_60() {
        let field_name = "a".repeat(61);
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields: BTreeMap::from([(field_name, FieldType::String)]),
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn accepts_index_name_at_max_length_30() {
        let index_name = "a".repeat(30);
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
            indexes: vec![IndexDef {
                name: index_name,
                fields: vec!["name".to_string()],
                search: false,
                vector: None,
                unique: false,
                r#where: None,
                language: None,
            }],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_ok());
    }

    #[test]
    fn rejects_index_name_over_max_length_30() {
        let index_name = "a".repeat(31);
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
            indexes: vec![IndexDef {
                name: index_name,
                fields: vec!["name".to_string()],
                search: false,
                vector: None,
                unique: false,
                r#where: None,
                language: None,
            }],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn rejects_case_insensitive_field_name_collision() {
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields: BTreeMap::from([
                ("status".to_string(), FieldType::String),
                ("Status".to_string(), FieldType::String),
            ]),
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn rejects_case_insensitive_index_name_collision() {
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
            indexes: vec![
                IndexDef {
                    name: "by_x".to_string(),
                    fields: vec!["name".to_string()],
                    search: false,
                    vector: None,
                    unique: false,
                    r#where: None,
                    language: None,
                },
                IndexDef {
                    name: "By_X".to_string(),
                    fields: vec!["name".to_string()],
                    search: false,
                    vector: None,
                    unique: false,
                    r#where: None,
                    language: None,
                },
            ],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn rejects_field_name_starting_with_underscore() {
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields: BTreeMap::from([("_secret".to_string(), FieldType::String)]),
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
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
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields: BTreeMap::from([(
                "tags".to_string(),
                FieldType::Array {
                    element: Box::new(FieldType::String),
                },
            )]),
            indexes: vec![IndexDef {
                name: "by_tags".to_string(),
                fields: vec!["tags".to_string()],
                search: false,
                vector: None,
                unique: false,
                r#where: None,
                language: None,
            }],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn rejects_index_with_empty_fields() {
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
            indexes: vec![IndexDef {
                name: "by_nothing".to_string(),
                fields: vec![],
                search: false,
                vector: None,
                unique: false,
                r#where: None,
                language: None,
            }],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn rejects_index_with_duplicate_fields() {
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
            indexes: vec![IndexDef {
                name: "by_name".to_string(),
                fields: vec!["name".to_string(), "name".to_string()],
                search: false,
                vector: None,
                unique: false,
                r#where: None,
                language: None,
            }],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn rejects_duplicate_index_names() {
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
            indexes: vec![
                IndexDef {
                    name: "by_name".to_string(),
                    fields: vec!["name".to_string()],
                    search: false,
                    vector: None,
                    unique: false,
                    r#where: None,
                    language: None,
                },
                IndexDef {
                    name: "by_name".to_string(),
                    fields: vec!["name".to_string()],
                    search: false,
                    vector: None,
                    unique: false,
                    r#where: None,
                    language: None,
                },
            ],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn rejects_index_name_with_invalid_chars() {
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
            indexes: vec![IndexDef {
                name: "by-name".to_string(),
                fields: vec!["name".to_string()],
                search: false,
                vector: None,
                unique: false,
                r#where: None,
                language: None,
            }],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn rejects_index_referencing_unknown_field() {
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
            indexes: vec![IndexDef {
                name: "by_missing".to_string(),
                fields: vec!["missing".to_string()],
                search: false,
                vector: None,
                unique: false,
                r#where: None,
                language: None,
            }],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn rejects_literal_with_non_scalar_value() {
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields: BTreeMap::from([(
                "x".to_string(),
                FieldType::Literal {
                    value: serde_json::json!(null),
                },
            )]),
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn rejects_empty_union() {
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields: BTreeMap::from([("x".to_string(), FieldType::Union { variants: vec![] })]),
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn rejects_optional_wrapping_optional() {
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields: BTreeMap::from([(
                "x".to_string(),
                FieldType::Optional {
                    inner: Box::new(FieldType::Optional {
                        inner: Box::new(FieldType::String),
                    }),
                },
            )]),
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
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
            indexed_column_type(&FieldType::Int64).unwrap(),
            ("bigint", false)
        );
        assert_eq!(
            indexed_column_type(&FieldType::Boolean).unwrap(),
            ("boolean", false)
        );
        assert_eq!(
            indexed_column_type(&FieldType::Id {
                table: "projects".to_string(),
                on_delete: None,
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
            on_delete: None,
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

    // Extra validators: record/int64/any/bytes wire tags, structural validation, and
    // document/value validation (FEATURE_MATRIX rank 13).
    #[test]
    fn new_variants_serialize_with_expected_wire_tags() {
        assert_eq!(
            serde_json::to_value(FieldType::Int64).unwrap(),
            serde_json::json!({"type": "int64"})
        );
        assert_eq!(
            serde_json::to_value(FieldType::Bytes).unwrap(),
            serde_json::json!({"type": "bytes"})
        );
        assert_eq!(
            serde_json::to_value(FieldType::Any).unwrap(),
            serde_json::json!({"type": "any"})
        );
        assert_eq!(
            serde_json::to_value(FieldType::Record {
                value: Box::new(FieldType::String)
            })
            .unwrap(),
            serde_json::json!({"type": "record", "value": {"type": "string"}})
        );
    }

    #[test]
    fn record_field_validates_structurally_and_recurses() {
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields: BTreeMap::from([(
                "meta".to_string(),
                FieldType::Record {
                    value: Box::new(FieldType::Number),
                },
            )]),
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_ok());
    }

    #[test]
    fn record_value_validates_every_entry() {
        let ty = FieldType::Record {
            value: Box::new(FieldType::String),
        };
        assert!(validate_value(
            &ty,
            &serde_json::json!({"a": "x", "b": "y"})
        ));
        assert!(validate_value(&ty, &serde_json::json!({})));
        assert!(!validate_value(&ty, &serde_json::json!({"a": 1})));
        assert!(!validate_value(&ty, &serde_json::json!(["a", "b"])));
    }

    #[test]
    fn any_field_accepts_every_json_value() {
        let ty = FieldType::Any;
        assert!(validate_value(&ty, &serde_json::json!("x")));
        assert!(validate_value(&ty, &serde_json::json!(42)));
        assert!(validate_value(&ty, &serde_json::json!(true)));
        assert!(validate_value(&ty, &serde_json::Value::Null));
        assert!(validate_value(&ty, &serde_json::json!([1, "a", null])));
        assert!(validate_value(
            &ty,
            &serde_json::json!({"nested": {"x": 1}})
        ));
    }

    #[test]
    fn bytes_field_validates_standard_padded_base64() {
        let ty = FieldType::Bytes;
        assert!(validate_value(&ty, &serde_json::json!("aGVsbG8gd29ybGQ=")));
        assert!(!validate_value(&ty, &serde_json::json!("not base64!!!")));
        // Missing required padding is rejected under STANDARD (not STANDARD_NO_PAD).
        assert!(!validate_value(&ty, &serde_json::json!("aGVsbG8")));
        assert!(!validate_value(&ty, &serde_json::json!(123)));
    }

    #[test]
    fn int64_field_validates_decimal_string_in_i64_range() {
        let ty = FieldType::Int64;
        assert!(validate_value(&ty, &serde_json::json!("0")));
        assert!(validate_value(&ty, &serde_json::json!("-42")));
        assert!(validate_value(
            &ty,
            &serde_json::json!("9223372036854775807")
        )); // i64::MAX
        assert!(validate_value(
            &ty,
            &serde_json::json!("-9223372036854775808")
        )); // i64::MIN
        assert!(!validate_value(
            &ty,
            &serde_json::json!("9223372036854775808")
        )); // overflow
        assert!(!validate_value(&ty, &serde_json::json!("not a number")));
        assert!(!validate_value(&ty, &serde_json::json!("1.5")));
        assert!(!validate_value(&ty, &serde_json::json!(42))); // must be a string, not a JSON number
    }

    #[test]
    fn indexed_column_type_rejects_new_non_indexable_types() {
        assert!(indexed_column_type(&FieldType::Bytes).is_err());
        assert!(indexed_column_type(&FieldType::Any).is_err());
        assert!(
            indexed_column_type(&FieldType::Record {
                value: Box::new(FieldType::String)
            })
            .is_err()
        );
    }

    #[test]
    fn rejects_index_over_record_field() {
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields: BTreeMap::from([(
                "meta".to_string(),
                FieldType::Record {
                    value: Box::new(FieldType::String),
                },
            )]),
            indexes: vec![IndexDef {
                name: "by_meta".to_string(),
                fields: vec!["meta".to_string()],
                search: false,
                vector: None,
                unique: false,
                r#where: None,
                language: None,
            }],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    // (h) full-text search index: additive `search: true` flag on IndexDef.
    #[test]
    fn search_index_round_trips_and_validates() {
        // A search index is declared by carrying `search: true`; a btree index
        // omits it. Both deserialize from existing wire, and the flag round-trips
        // through serde (btree omits `search`, search keeps `search: true`).
        let schema = serde_json::json!({"tables":{"notes":{
            "fields":{"title":{"type":"string"},"body":{"type":"string"}},
            "indexes":[
                {"name":"by_title","fields":["title"]},
                {"name":"search_content","fields":["title","body"],"search":true}
            ]
        }}});
        let parsed: SchemaDef = serde_json::from_value(schema).unwrap();
        assert!(parsed.validate().is_ok());
        let notes = parsed.tables.get("notes").unwrap();
        let by_title = notes.indexes.iter().find(|i| i.name == "by_title").unwrap();
        let search = notes
            .indexes
            .iter()
            .find(|i| i.name == "search_content")
            .unwrap();
        assert!(!by_title.search);
        assert!(search.search);
        // Btree omits `search` on the wire; the search index keeps it.
        assert_eq!(
            serde_json::to_value(by_title).unwrap(),
            serde_json::json!({"name":"by_title","fields":["title"]})
        );
        assert_eq!(
            serde_json::to_value(search).unwrap(),
            serde_json::json!({"name":"search_content","fields":["title","body"],"search":true})
        );
    }

    #[test]
    fn rejects_search_index_over_non_text_field() {
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields: BTreeMap::from([("count".to_string(), FieldType::Number)]),
            indexes: vec![IndexDef {
                name: "search_count".to_string(),
                fields: vec!["count".to_string()],
                search: true,
                vector: None,
                unique: false,
                r#where: None,
                language: None,
            }],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn accepts_search_index_over_optional_text_field() {
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields: BTreeMap::from([(
                "body".to_string(),
                FieldType::Optional {
                    inner: Box::new(FieldType::String),
                },
            )]),
            indexes: vec![IndexDef {
                name: "search_body".to_string(),
                fields: vec!["body".to_string()],
                search: true,
                vector: None,
                unique: false,
                r#where: None,
                language: None,
            }],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_ok());
    }

    // (i) Vector field type: wire round-trip, not btree-indexable, and value
    // validation requires an array of exactly `dimensions` finite numbers.
    #[test]
    fn vector_field_type_round_trips() {
        let v = FieldType::Vector { dimensions: 1536 };
        let json = serde_json::to_value(&v).unwrap();
        assert_eq!(json, serde_json::json!({"type":"vector","dimensions":1536}));
        let back: FieldType = serde_json::from_value(json).unwrap();
        assert_eq!(back, v);
    }

    #[test]
    fn vector_is_not_btree_indexable() {
        assert!(indexed_column_type(&FieldType::Vector { dimensions: 3 }).is_err());
    }

    #[test]
    fn vector_validate_accepts_exact_length_finite() {
        let ty = FieldType::Vector { dimensions: 3 };
        assert!(validate_value(&ty, &serde_json::json!([1.0, -2.5, 0.0])));
    }

    #[test]
    fn vector_validate_rejects_wrong_length() {
        let ty = FieldType::Vector { dimensions: 3 };
        assert!(!validate_value(&ty, &serde_json::json!([1.0, 2.0])));
    }

    #[test]
    fn vector_validate_rejects_non_finite() {
        let ty = FieldType::Vector { dimensions: 2 };
        assert!(!validate_value(
            &ty,
            &serde_json::json!([1.0, serde_json::Value::from(f64::NAN)])
        ));
    }

    // (j) Vector index declaration: `IndexDef.vector` carries dimensions and
    // optional scalar filterFields; an ordinary btree index omits `vector`
    // entirely on the wire.
    #[test]
    fn vector_index_round_trips_and_btree_omits_it() {
        let json = serde_json::json!({
            "name": "by_embedding",
            "fields": ["embedding"],
            "vector": {"dimensions": 4, "filterFields": ["userId"]}
        });
        let idx: IndexDef = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(idx.vector.as_ref().unwrap().dimensions, 4);
        assert_eq!(
            idx.vector.as_ref().unwrap().filter_fields,
            vec!["userId".to_string()]
        );
        // round-trips byte-identical
        assert_eq!(serde_json::to_value(&idx).unwrap(), json);

        // a btree index omits `vector` entirely
        let btree: IndexDef =
            serde_json::from_value(serde_json::json!({"name":"by_name","fields":["name"]}))
                .unwrap();
        assert!(btree.vector.is_none());
        assert!(
            serde_json::to_value(&btree)
                .unwrap()
                .get("vector")
                .is_none()
        );
    }

    #[test]
    fn vector_index_rejects_dimension_mismatch() {
        let mut fields = BTreeMap::new();
        fields.insert("embedding".to_string(), FieldType::Vector { dimensions: 4 });
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields,
            indexes: vec![IndexDef {
                name: "by_emb".to_string(),
                fields: vec!["embedding".to_string()],
                search: false,
                vector: Some(VectorIndexSpec {
                    dimensions: 8,
                    filter_fields: vec![],
                    metric: DistanceMetric::Cosine,
                }),
                unique: false,
                r#where: None,
                language: None,
            }],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        assert!(table.validate_structure("docs").is_err());
    }

    #[test]
    fn vector_index_accepts_matching_dims_and_filter_fields() {
        let mut fields = BTreeMap::new();
        fields.insert("embedding".to_string(), FieldType::Vector { dimensions: 4 });
        fields.insert("userId".to_string(), FieldType::String);
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields,
            indexes: vec![IndexDef {
                name: "by_emb".to_string(),
                fields: vec!["embedding".to_string()],
                search: false,
                vector: Some(VectorIndexSpec {
                    dimensions: 4,
                    filter_fields: vec!["userId".to_string()],
                    metric: DistanceMetric::Cosine,
                }),
                unique: false,
                r#where: None,
                language: None,
            }],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        assert!(table.validate_structure("docs").is_ok());
    }

    #[test]
    fn vector_index_rejects_search_and_vector_both_set() {
        let mut fields = BTreeMap::new();
        fields.insert("embedding".to_string(), FieldType::Vector { dimensions: 4 });
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields,
            indexes: vec![IndexDef {
                name: "by_emb".to_string(),
                fields: vec!["embedding".to_string()],
                search: true,
                vector: Some(VectorIndexSpec {
                    dimensions: 4,
                    filter_fields: vec![],
                    metric: DistanceMetric::Cosine,
                }),
                unique: false,
                r#where: None,
                language: None,
            }],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        assert!(table.validate_structure("docs").is_err());
    }

    // A 0-dimensional vector is meaningless and would produce a degenerate
    // index; rejecting it here gives a clear schema error.
    #[test]
    fn vector_index_rejects_zero_dimensions() {
        let mut fields = BTreeMap::new();
        fields.insert("embedding".to_string(), FieldType::Vector { dimensions: 0 });
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields,
            indexes: vec![IndexDef {
                name: "by_emb".to_string(),
                fields: vec!["embedding".to_string()],
                search: false,
                vector: Some(VectorIndexSpec {
                    dimensions: 0,
                    filter_fields: vec![],
                    metric: DistanceMetric::Cosine,
                }),
                unique: false,
                r#where: None,
                language: None,
            }],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        assert!(table.validate_structure("docs").is_err());
    }

    #[test]
    fn vector_index_rejects_two_fields() {
        let mut fields = BTreeMap::new();
        fields.insert("a".to_string(), FieldType::Vector { dimensions: 4 });
        fields.insert("b".to_string(), FieldType::Vector { dimensions: 4 });
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields,
            indexes: vec![IndexDef {
                name: "by_emb".to_string(),
                fields: vec!["a".to_string(), "b".to_string()],
                search: false,
                vector: Some(VectorIndexSpec {
                    dimensions: 4,
                    filter_fields: vec![],
                    metric: DistanceMetric::Cosine,
                }),
                unique: false,
                r#where: None,
                language: None,
            }],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        assert!(table.validate_structure("docs").is_err());
    }

    #[test]
    fn vector_index_rejects_non_vector_field() {
        let mut fields = BTreeMap::new();
        fields.insert("title".to_string(), FieldType::String);
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields,
            indexes: vec![IndexDef {
                name: "by_title".to_string(),
                fields: vec!["title".to_string()],
                search: false,
                vector: Some(VectorIndexSpec {
                    dimensions: 4,
                    filter_fields: vec![],
                    metric: DistanceMetric::Cosine,
                }),
                unique: false,
                r#where: None,
                language: None,
            }],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        assert!(table.validate_structure("docs").is_err());
    }

    #[test]
    fn vector_index_rejects_unknown_filter_field() {
        let mut fields = BTreeMap::new();
        fields.insert("embedding".to_string(), FieldType::Vector { dimensions: 4 });
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields,
            indexes: vec![IndexDef {
                name: "by_emb".to_string(),
                fields: vec!["embedding".to_string()],
                search: false,
                vector: Some(VectorIndexSpec {
                    dimensions: 4,
                    filter_fields: vec!["userId".to_string()],
                    metric: DistanceMetric::Cosine,
                }),
                unique: false,
                r#where: None,
                language: None,
            }],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        assert!(table.validate_structure("docs").is_err());
    }

    #[test]
    fn vector_index_rejects_non_scalar_filter_field() {
        let mut fields = BTreeMap::new();
        fields.insert("embedding".to_string(), FieldType::Vector { dimensions: 4 });
        fields.insert(
            "meta".to_string(),
            FieldType::Object {
                fields: BTreeMap::new(),
            },
        );
        let table = TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields,
            indexes: vec![IndexDef {
                name: "by_emb".to_string(),
                fields: vec!["embedding".to_string()],
                search: false,
                vector: Some(VectorIndexSpec {
                    dimensions: 4,
                    filter_fields: vec!["meta".to_string()],
                    metric: DistanceMetric::Cosine,
                }),
                unique: false,
                r#where: None,
                language: None,
            }],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        };
        assert!(table.validate_structure("docs").is_err());
    }

    #[test]
    fn owner_field_round_trips_and_validates() {
        let json = r#"{"fields":{"title":{"type":"string"},"userId":{"type":"string"}},"indexes":[{"name":"by_user","fields":["userId"]}],"ownerField":"userId"}"#;
        let td: TableDef = serde_json::from_str(json).unwrap();
        assert_eq!(td.owner_field.as_deref(), Some("userId"));
        // camelCase wire key survives a round trip
        let re = serde_json::to_value(&td).unwrap();
        assert_eq!(re["ownerField"], "userId");

        // validates as part of a schema
        let mut tables = std::collections::BTreeMap::new();
        tables.insert("notes".to_string(), td);
        let schema = SchemaDef { tables };
        schema.validate().unwrap();

        // absent ownerField is omitted from the wire and deserializes as None
        let none_json = r#"{"fields":{"title":{"type":"string"}}}"#;
        let td2: TableDef = serde_json::from_str(none_json).unwrap();
        assert!(td2.owner_field.is_none());
        assert!(!serde_json::to_string(&td2).unwrap().contains("ownerField"));
    }

    #[test]
    fn owner_field_validation_rejects_bad_declarations() {
        use crate::error::ErrorCode;

        fn validate_owner(fields_json: &str, owner: &str) -> Result<(), RtDbError> {
            let json = format!(r#"{{"fields":{fields_json},"ownerField":"{owner}"}}"#);
            let td: TableDef = serde_json::from_str(&json).unwrap();
            let mut tables = std::collections::BTreeMap::new();
            tables.insert("t".to_string(), td);
            SchemaDef { tables }.validate()
        }

        // names an undeclared field — schema violation (422), not bad_request (400)
        let err = validate_owner(r#"{"title":{"type":"string"}}"#, "missing").unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);

        // names a non-string field (number) — not string-compatible
        let err = validate_owner(r#"{"num":{"type":"number"}}"#, "num").unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);

        // a plain string field is valid
        validate_owner(r#"{"title":{"type":"string"}}"#, "title").unwrap();

        // Accepted-types matrix: every field type that maps to Postgres "text".
        validate_owner(r#"{"o":{"type":"id","table":"users"}}"#, "o").unwrap();
        validate_owner(r#"{"o":{"type":"literal","value":"admin"}}"#, "o").unwrap();
        validate_owner(
            r#"{"o":{"type":"union","variants":[{"type":"literal","value":"a"},{"type":"literal","value":"b"}]}}"#,
            "o",
        )
        .unwrap();
        validate_owner(
            r#"{"o":{"type":"optional","inner":{"type":"string"}}}"#,
            "o",
        )
        .unwrap();

        // Rejected-types matrix: non-text scalars.
        let err = validate_owner(r#"{"o":{"type":"boolean"}}"#, "o").unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);
    }

    #[test]
    fn collaborators_field_round_trips_and_validates() {
        // `collaboratorsField` is an opt-in, array-of-strings authorization hint:
        // present on the wire (camelCase) when set, omitted entirely when absent,
        // mirroring `owner_field` byte-for-byte. Round-trips alongside `owner_field`.
        let json = r#"{"fields":{"title":{"type":"string"},"userId":{"type":"string"},"collaborators":{"type":"array","element":{"type":"string"}}},"indexes":[{"name":"by_user","fields":["userId"]}],"ownerField":"userId","collaboratorsField":"collaborators"}"#;
        let td: TableDef = serde_json::from_str(json).unwrap();
        assert_eq!(td.owner_field.as_deref(), Some("userId"));
        assert_eq!(td.collaborators_field.as_deref(), Some("collaborators"));
        let re = serde_json::to_value(&td).unwrap();
        assert_eq!(re["ownerField"], "userId");
        assert_eq!(re["collaboratorsField"], "collaborators");

        let mut tables = std::collections::BTreeMap::new();
        tables.insert("notes".to_string(), td);
        SchemaDef { tables }.validate().unwrap();

        // Absent collaboratorsField is omitted from the wire and deserializes as None.
        let none_json = r#"{"fields":{"title":{"type":"string"}}}"#;
        let td2: TableDef = serde_json::from_str(none_json).unwrap();
        assert!(td2.collaborators_field.is_none());
        assert!(
            !serde_json::to_string(&td2)
                .unwrap()
                .contains("collaboratorsField")
        );
    }

    #[test]
    fn collaborators_field_validation_rejects_bad_declarations() {
        use crate::error::ErrorCode;

        fn validate_collab(fields_json: &str, collab: &str) -> Result<(), RtDbError> {
            let json = format!(r#"{{"fields":{fields_json},"collaboratorsField":"{collab}"}}"#);
            let td: TableDef = serde_json::from_str(&json).unwrap();
            let mut tables = std::collections::BTreeMap::new();
            tables.insert("t".to_string(), td);
            SchemaDef { tables }.validate()
        }

        // names an undeclared field — schema violation (422).
        let err = validate_collab(r#"{"title":{"type":"string"}}"#, "missing").unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);

        // names a non-array field (string) — not array-of-strings.
        let err = validate_collab(r#"{"c":{"type":"string"}}"#, "c").unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);

        // names an array-of-numbers field — element type not string-compatible.
        let err = validate_collab(r#"{"c":{"type":"array","element":{"type":"number"}}}"#, "c")
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);

        // Accepted-types matrix: array of every string-compatible element type.
        validate_collab(r#"{"c":{"type":"array","element":{"type":"string"}}}"#, "c").unwrap();
        validate_collab(
            r#"{"c":{"type":"array","element":{"type":"id","table":"users"}}}"#,
            "c",
        )
        .unwrap();
        validate_collab(
            r#"{"c":{"type":"array","element":{"type":"literal","value":"admin"}}}"#,
            "c",
        )
        .unwrap();
        validate_collab(
            r#"{"c":{"type":"array","element":{"type":"union","variants":[{"type":"literal","value":"a"},{"type":"literal","value":"b"}]}}}"#,
            "c",
        )
        .unwrap();
        // Optional<Array<String>> admits a nullable collaborators column.
        validate_collab(
            r#"{"c":{"type":"optional","inner":{"type":"array","element":{"type":"string"}}}}"#,
            "c",
        )
        .unwrap();
    }

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

    fn table_with_ttl(ttl: Option<TtlDef>) -> TableDef {
        let mut fields = BTreeMap::new();
        fields.insert("expiresAt".to_string(), FieldType::Number);
        TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields,
            indexes: vec![IndexDef {
                name: "by_expiresAt".to_string(),
                fields: vec!["expiresAt".to_string()],
                unique: false,
                search: false,
                vector: None,
                r#where: None,
                language: None,
            }],
            owner_field: None,
            collaborators_field: None,
            ttl,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        }
    }

    #[test]
    fn ttl_accepts_numeric_field_with_single_btree_index() {
        let mut schema = SchemaDef::default();
        schema.tables.insert(
            "t".to_string(),
            table_with_ttl(Some(TtlDef {
                field: "expiresAt".to_string(),
                default_duration_ms: Some(86_400_000),
            })),
        );
        assert!(schema.validate().is_ok());
    }

    #[test]
    fn ttl_rejects_missing_index() {
        let mut table = table_with_ttl(Some(TtlDef {
            field: "expiresAt".to_string(),
            default_duration_ms: None,
        }));
        table.indexes.clear();
        let mut schema = SchemaDef::default();
        schema.tables.insert("t".to_string(), table);
        let err = schema.validate().unwrap_err();
        assert!(
            err.message
                .contains("requires a single-field, non-unique, non-partial btree index"),
            "{}",
            err.message
        );
    }

    #[test]
    fn ttl_rejects_non_numeric_field() {
        let mut table = table_with_ttl(Some(TtlDef {
            field: "name".to_string(),
            default_duration_ms: None,
        }));
        table.fields.insert("name".to_string(), FieldType::String);
        let mut schema = SchemaDef::default();
        schema.tables.insert("t".to_string(), table);
        assert!(schema.validate().is_err());
    }

    #[test]
    fn ttl_rejects_unique_or_partial_or_multifield_index() {
        for bad in [
            IndexDef {
                name: "x".to_string(),
                fields: vec!["expiresAt".to_string()],
                unique: true,
                search: false,
                vector: None,
                r#where: None,
                language: None,
            },
            IndexDef {
                name: "x".to_string(),
                fields: vec!["expiresAt".to_string()],
                unique: false,
                search: false,
                vector: None,
                r#where: Some(FilterExpr::Gt {
                    field: "expiresAt".to_string(),
                    value: serde_json::json!(0),
                }),
                language: None,
            },
            IndexDef {
                name: "x".to_string(),
                fields: vec!["expiresAt".to_string(), "expiresAt".to_string()],
                unique: false,
                search: false,
                vector: None,
                r#where: None,
                language: None,
            },
        ] {
            let mut table = table_with_ttl(Some(TtlDef {
                field: "expiresAt".to_string(),
                default_duration_ms: None,
            }));
            table.indexes = vec![bad];
            let mut schema = SchemaDef::default();
            schema.tables.insert("t".to_string(), table);
            assert!(
                schema.validate().is_err(),
                "should reject this index variant"
            );
        }
    }

    #[test]
    fn ttl_rejects_non_positive_default_duration() {
        let mut schema = SchemaDef::default();
        schema.tables.insert(
            "t".to_string(),
            table_with_ttl(Some(TtlDef {
                field: "expiresAt".to_string(),
                default_duration_ms: Some(0),
            })),
        );
        assert!(schema.validate().is_err());
    }

    // ---- TableDef.authorize: per-row predicate declaration (Model C) ----

    /// Helper: a TableDef with the named string fields plus `editors`
    /// (array-of-strings) and `count` (number), so every authorize-validation
    /// branch has a field of the right shape to target.
    fn table_with_string_fields(field_names: &[&str]) -> TableDef {
        let mut fields = BTreeMap::new();
        for f in field_names {
            fields.insert((*f).to_string(), FieldType::String);
        }
        fields.insert(
            "editors".to_string(),
            FieldType::Array {
                element: Box::new(FieldType::String),
            },
        );
        fields.insert("count".to_string(), FieldType::Number);
        TableDef {
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields,
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,
        }
    }

    #[test]
    fn authorize_validates_fields_and_markers() {
        let mut t = table_with_string_fields(&["owner", "visibility"]);
        // valid: public OR owned (principal marker allowed in authorize)
        t.authorize = Some(FilterExpr::Or {
            exprs: vec![
                FilterExpr::Eq {
                    field: "owner".into(),
                    value: serde_json::json!({"$user": true}),
                },
                FilterExpr::Eq {
                    field: "visibility".into(),
                    value: serde_json::json!("public"),
                },
            ],
        });
        assert!(t.validate_structure("posts").is_ok());

        // invalid: unknown field
        let mut bad = t.clone();
        bad.authorize = Some(FilterExpr::Eq {
            field: "nope".into(),
            value: serde_json::json!(1),
        });
        assert!(bad.validate_structure("posts").is_err());

        // invalid: Contains on a non-array field
        let mut bad2 = t.clone();
        bad2.authorize = Some(FilterExpr::Contains {
            field: "visibility".into(),
            value: serde_json::json!("x"),
        });
        assert!(bad2.validate_structure("posts").is_err());

        // valid: Contains on an array-of-strings field
        let mut good = t.clone();
        good.authorize = Some(FilterExpr::Contains {
            field: "editors".into(),
            value: serde_json::json!({"$user": true}),
        });
        assert!(good.validate_structure("posts").is_ok());

        // valid: And/Not recurse; principal markers resolve in any leaf
        let mut nested = t.clone();
        nested.authorize = Some(FilterExpr::And {
            exprs: vec![
                FilterExpr::Not {
                    expr: Box::new(FilterExpr::Exists {
                        field: "count".into(),
                    }),
                },
                FilterExpr::Eq {
                    field: "owner".into(),
                    value: serde_json::json!({"$email": true}),
                },
            ],
        });
        assert!(nested.validate_structure("posts").is_ok());

        // invalid: comparison against a non-scalar (array) field — not type-compatible
        let mut bad3 = t.clone();
        bad3.authorize = Some(FilterExpr::Gt {
            field: "editors".into(),
            value: serde_json::json!(1),
        });
        assert!(bad3.validate_structure("posts").is_err());
    }

    #[test]
    fn authorize_round_trips_and_absent_is_unchanged() {
        // present: wire key `authorize` survives a round trip. (Server TableDef
        // always serializes `indexes`, so the expected JSON includes it.)
        let json = serde_json::json!({
            "fields": {
                "owner": {"type":"string"},
                "visibility": {"type":"string"}
            },
            "indexes": [],
            "authorize": {"op":"or","exprs":[
                {"op":"eq","field":"owner","value":{"$user":true}},
                {"op":"eq","field":"visibility","value":"public"}
            ]}
        });
        let td: TableDef = serde_json::from_value(json.clone()).unwrap();
        assert!(td.authorize.is_some());
        assert_eq!(serde_json::to_value(&td).unwrap(), json);

        // validates as part of a schema
        let mut tables = BTreeMap::new();
        tables.insert("posts".to_string(), td);
        SchemaDef { tables }.validate().unwrap();

        // absent authorize is omitted from the wire and deserializes as None
        let none_json = r#"{"fields":{"title":{"type":"string"}}}"#;
        let td2: TableDef = serde_json::from_str(none_json).unwrap();
        assert!(td2.authorize.is_none());
        assert!(
            !serde_json::to_string(&td2).unwrap().contains("authorize"),
            "authorize must be omitted on the wire when unset"
        );
    }

    #[test]
    fn validate_filter_expr_fields_rejects_principal_markers_when_disallowed() {
        // The walker is reused by Task 6 to reject principal markers in client
        // .filter() queries. Here we lock the flag's behavior directly.
        let table = table_with_string_fields(&["owner", "visibility"]);
        let with_marker = FilterExpr::Eq {
            field: "owner".into(),
            value: serde_json::json!({"$user": true}),
        };
        assert!(validate_filter_expr_fields(&with_marker, &table, true, false).is_ok());
        assert!(validate_filter_expr_fields(&with_marker, &table, false, false).is_err());
        let email_marker = FilterExpr::Eq {
            field: "owner".into(),
            value: serde_json::json!({"$email": true}),
        };
        assert!(validate_filter_expr_fields(&email_marker, &table, true, false).is_ok());
        assert!(validate_filter_expr_fields(&email_marker, &table, false, false).is_err());
        // a marker nested under And is still rejected
        let nested = FilterExpr::And {
            exprs: vec![
                FilterExpr::Eq {
                    field: "visibility".into(),
                    value: serde_json::json!("public"),
                },
                FilterExpr::Contains {
                    field: "editors".into(),
                    value: serde_json::json!({"$user": true}),
                },
            ],
        };
        assert!(validate_filter_expr_fields(&nested, &table, false, false).is_err());
        // a non-marker value passes regardless of the flag
        let plain = FilterExpr::Eq {
            field: "visibility".into(),
            value: serde_json::json!("public"),
        };
        assert!(validate_filter_expr_fields(&plain, &table, true, false).is_ok());
        assert!(validate_filter_expr_fields(&plain, &table, false, false).is_ok());
    }

    // ---- computed fields (ENH-028) ----

    fn field(name: &str) -> ValueExpr {
        ValueExpr::Field {
            field: name.to_string(),
        }
    }

    fn literal(value: serde_json::Value) -> ValueExpr {
        ValueExpr::Literal { value }
    }

    /// Base table with a field of every kind the computed rules distinguish.
    /// Computed targets (`fullName`, `slug`, `total`, `label`, `rankText`) are
    /// declared but the `computed` map starts empty — each test installs only
    /// the entries it exercises.
    fn computed_table() -> TableDef {
        TableDef {
            defaults: BTreeMap::new(),
            fields: BTreeMap::from([
                ("first".to_string(), FieldType::String),
                ("last".to_string(), FieldType::String),
                (
                    "nickname".to_string(),
                    FieldType::Optional {
                        inner: Box::new(FieldType::String),
                    },
                ),
                ("score".to_string(), FieldType::Number),
                ("rank".to_string(), FieldType::Int64),
                ("active".to_string(), FieldType::Boolean),
                (
                    "status".to_string(),
                    FieldType::Union {
                        variants: vec![
                            FieldType::Literal {
                                value: serde_json::json!("admin"),
                            },
                            FieldType::Literal {
                                value: serde_json::json!("user"),
                            },
                        ],
                    },
                ),
                ("owner".to_string(), FieldType::String),
                (
                    "collaborators".to_string(),
                    FieldType::Array {
                        element: Box::new(FieldType::String),
                    },
                ),
                ("fullName".to_string(), FieldType::String),
                (
                    "slug".to_string(),
                    FieldType::Optional {
                        inner: Box::new(FieldType::String),
                    },
                ),
                ("total".to_string(), FieldType::Number),
                (
                    "label".to_string(),
                    FieldType::Union {
                        variants: vec![
                            FieldType::Literal {
                                value: serde_json::json!("staff"),
                            },
                            FieldType::Literal {
                                value: serde_json::json!("guest"),
                            },
                        ],
                    },
                ),
                ("rankText".to_string(), FieldType::Int64),
            ]),
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,
            computed: BTreeMap::new(),

            soft_delete: false,
        }
    }

    fn computed_schema(table: TableDef) -> SchemaDef {
        SchemaDef {
            tables: BTreeMap::from([("users".to_string(), table)]),
        }
    }

    fn one_computed(name: &str, expr: ValueExpr) -> BTreeMap<String, ValueExpr> {
        BTreeMap::from([(name.to_string(), expr)])
    }

    // (a) a computed key must be a declared field.
    #[test]
    fn computed_key_must_be_declared() {
        let mut table = computed_table();
        table.computed = one_computed(
            "bogus",
            ValueExpr::Concat {
                parts: vec![field("first"), field("last")],
            },
        );
        let err = validate_computed(&computed_schema(table)).unwrap_err();
        assert!(
            err.message.contains("not a declared field"),
            "{}",
            err.message
        );
        assert!(err.message.contains("users.bogus"), "{}", err.message);
    }

    // The rule also rides `SchemaDef::validate` (the push/migrate chokepoint).
    #[test]
    fn computed_validation_rides_schema_validate() {
        let mut table = computed_table();
        table.computed = one_computed("bogus", field("first"));
        assert!(computed_schema(table).validate().is_err());
    }

    // (b) every referenced field must be declared.
    #[test]
    fn computed_reference_must_be_declared() {
        let mut table = computed_table();
        table.computed = one_computed(
            "fullName",
            ValueExpr::Concat {
                parts: vec![field("first"), field("bogus")],
            },
        );
        let err = validate_computed(&computed_schema(table)).unwrap_err();
        assert!(
            err.message.contains("references undeclared field 'bogus'"),
            "{}",
            err.message
        );
    }

    // (c) computed fields may not reference each other.
    #[test]
    fn computed_reference_must_not_be_computed() {
        let mut table = computed_table();
        table.computed = BTreeMap::from([
            (
                "fullName".to_string(),
                ValueExpr::Concat {
                    parts: vec![field("first"), field("last")],
                },
            ),
            (
                "slug".to_string(),
                ValueExpr::Lower {
                    value: Box::new(field("fullName")),
                },
            ),
        ]);
        let err = validate_computed(&computed_schema(table)).unwrap_err();
        assert!(
            err.message.contains("references computed field 'fullName'"),
            "{}",
            err.message
        );
    }

    // The authorize predicate may not reference a computed field: it runs
    // before computed stamping on the insert paths, so it would evaluate
    // forgeable client input.
    #[test]
    fn authorize_must_not_reference_computed_field() {
        let mut table = computed_table();
        table.computed = one_computed(
            "fullName",
            ValueExpr::Concat {
                parts: vec![field("first"), field("last")],
            },
        );
        table.authorize = Some(FilterExpr::Eq {
            field: "fullName".into(),
            value: serde_json::json!("x"),
        });
        let err = validate_computed(&computed_schema(table)).unwrap_err();
        assert!(
            err.message
                .contains("authorize predicates may not reference computed fields"),
            "{}",
            err.message
        );
        assert!(err.message.contains("users.fullName"), "{}", err.message);
        // a predicate over a plain (non-computed) field stays legal
        let mut table = computed_table();
        table.computed = one_computed(
            "fullName",
            ValueExpr::Concat {
                parts: vec![field("first"), field("last")],
            },
        );
        table.authorize = Some(FilterExpr::Eq {
            field: "status".into(),
            value: serde_json::json!("admin"),
        });
        assert!(validate_computed(&computed_schema(table)).is_ok());
    }

    // (d) principal markers are rejected inside Case.when filters.
    #[test]
    fn computed_case_when_rejects_principal_markers() {
        let mut table = computed_table();
        table.computed = one_computed(
            "label",
            ValueExpr::Case {
                whens: vec![crate::value_expr::CaseWhen {
                    when: FilterExpr::Eq {
                        field: "status".into(),
                        value: serde_json::json!({"$user": true}),
                    },
                    then: literal(serde_json::json!("staff")),
                }],
                otherwise: Box::new(literal(serde_json::json!("guest"))),
            },
        );
        let err = validate_computed(&computed_schema(table.clone())).unwrap_err();
        assert!(err.message.contains("principal markers"), "{}", err.message);
        // The email marker is rejected the same way.
        table.computed = one_computed(
            "label",
            ValueExpr::Case {
                whens: vec![crate::value_expr::CaseWhen {
                    when: FilterExpr::Eq {
                        field: "status".into(),
                        value: serde_json::json!({"$email": true}),
                    },
                    then: literal(serde_json::json!("staff")),
                }],
                otherwise: Box::new(literal(serde_json::json!("guest"))),
            },
        );
        assert!(validate_computed(&computed_schema(table)).is_err());
    }

    // (e) static-kind rejects: Concat into number, arithmetic into int64,
    // Lower into boolean.
    #[test]
    fn computed_rejects_concat_into_number_field() {
        let mut table = computed_table();
        table.computed = one_computed(
            "total",
            ValueExpr::Concat {
                parts: vec![field("first")],
            },
        );
        let err = validate_computed(&computed_schema(table)).unwrap_err();
        assert!(err.message.contains("produces a string"), "{}", err.message);
    }

    #[test]
    fn computed_rejects_arithmetic_into_int64_field() {
        let mut table = computed_table();
        table.computed = one_computed(
            "rank",
            ValueExpr::Add {
                left: Box::new(field("score")),
                right: Box::new(literal(serde_json::json!(1))),
            },
        );
        let err = validate_computed(&computed_schema(table)).unwrap_err();
        assert!(err.message.contains("produces a number"), "{}", err.message);
    }

    #[test]
    fn computed_rejects_lower_into_boolean_field() {
        let mut table = computed_table();
        table.computed = one_computed(
            "active",
            ValueExpr::Lower {
                value: Box::new(field("first")),
            },
        );
        let err = validate_computed(&computed_schema(table)).unwrap_err();
        assert!(err.message.contains("produces a string"), "{}", err.message);
    }

    // (f) the canonical shapes all pass — one per-shape test each below.

    #[test]
    fn computed_accepts_concat_on_string() {
        let mut table = computed_table();
        table.computed = one_computed(
            "fullName",
            ValueExpr::Concat {
                parts: vec![field("first"), field("last")],
            },
        );
        assert!(validate_computed(&computed_schema(table)).is_ok());
    }

    #[test]
    fn computed_accepts_lower_trim_on_optional_string() {
        let mut table = computed_table();
        table.computed = one_computed(
            "slug",
            ValueExpr::Lower {
                value: Box::new(ValueExpr::Trim {
                    value: Box::new(field("nickname")),
                }),
            },
        );
        assert!(validate_computed(&computed_schema(table)).is_ok());
    }

    #[test]
    fn computed_accepts_arithmetic_on_number() {
        let mut table = computed_table();
        table.computed = one_computed(
            "total",
            ValueExpr::Add {
                left: Box::new(field("score")),
                right: Box::new(field("score")),
            },
        );
        assert!(validate_computed(&computed_schema(table)).is_ok());
    }

    #[test]
    fn computed_accepts_case_on_union() {
        let mut table = computed_table();
        table.computed = one_computed(
            "label",
            ValueExpr::Case {
                whens: vec![crate::value_expr::CaseWhen {
                    when: FilterExpr::Eq {
                        field: "status".into(),
                        value: serde_json::json!("admin"),
                    },
                    then: literal(serde_json::json!("staff")),
                }],
                otherwise: Box::new(literal(serde_json::json!("guest"))),
            },
        );
        assert!(validate_computed(&computed_schema(table)).is_ok());
    }

    #[test]
    fn computed_accepts_now_on_number() {
        let mut table = computed_table();
        table.computed = one_computed("total", ValueExpr::Now);
        assert!(validate_computed(&computed_schema(table)).is_ok());
    }

    // Int64's wire form is a decimal string, so a String-kind expression is
    // the accepted shape (the plan's Int64 note) while Number-kind is rejected.
    #[test]
    fn computed_accepts_cast_to_string_into_int64() {
        let mut table = computed_table();
        table.computed = one_computed(
            "rankText",
            ValueExpr::Cast {
                value: Box::new(field("score")),
                to: crate::value_expr::Cast::ToString,
            },
        );
        assert!(validate_computed(&computed_schema(table)).is_ok());
    }

    // (g) computed keys must not collide with the stamped declaration fields.
    #[test]
    fn computed_rejects_owner_field_conflict() {
        let mut table = computed_table();
        table.owner_field = Some("owner".to_string());
        table.computed = one_computed("owner", field("first"));
        let err = validate_computed(&computed_schema(table)).unwrap_err();
        assert!(err.message.contains("ownerField"), "{}", err.message);
    }

    #[test]
    fn computed_rejects_collaborators_field_conflict() {
        let mut table = computed_table();
        table.collaborators_field = Some("collaborators".to_string());
        table.computed = one_computed("collaborators", field("first"));
        let err = validate_computed(&computed_schema(table)).unwrap_err();
        assert!(
            err.message.contains("collaboratorsField"),
            "{}",
            err.message
        );
    }

    #[test]
    fn computed_rejects_auto_increment_field_conflict() {
        let mut table = computed_table();
        table.auto_increment_field = Some("rank".to_string());
        table.computed = one_computed(
            "rank",
            ValueExpr::Cast {
                value: Box::new(field("score")),
                to: crate::value_expr::Cast::ToString,
            },
        );
        let err = validate_computed(&computed_schema(table)).unwrap_err();
        assert!(
            err.message.contains("autoIncrementField"),
            "{}",
            err.message
        );
    }

    // Additive wire: `computed` is omitted when empty and absent JSON still
    // deserializes.
    #[test]
    fn computed_wire_is_additive() {
        let table = simple_table();
        let json = serde_json::to_value(&table).unwrap();
        assert!(json.get("computed").is_none());
        let back: TableDef = serde_json::from_value(json).unwrap();
        assert!(back.computed.is_empty());
        // pre-ENH-028 schema JSON (no computed key) deserializes unchanged
        let legacy: TableDef = serde_json::from_value(serde_json::json!({
            "fields": {"name": {"type": "string"}}
        }))
        .unwrap();
        assert!(legacy.computed.is_empty());
    }

    // A computed entry round-trips through the wire with its expr intact.
    #[test]
    fn computed_wire_round_trips_expression() {
        let mut table = simple_table();
        let expr = ValueExpr::Concat {
            parts: vec![field("name"), literal(serde_json::json!("!"))],
        };
        table.computed = one_computed("name", expr.clone());
        let json = serde_json::to_value(&table).unwrap();
        assert_eq!(json["computed"]["name"]["op"], "concat");
        let back: TableDef = serde_json::from_value(json).unwrap();
        assert_eq!(back.computed.get("name"), Some(&expr));
    }
}
