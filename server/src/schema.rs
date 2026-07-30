use std::collections::{BTreeMap, HashSet};

use base64::{Engine as _, engine::general_purpose::STANDARD};

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
    Int64,
    Bytes,
    Any,
    Record { value: Box<FieldType> },
    Vector { dimensions: u32 },
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
}

/// Declaration of a vector (approximate nearest-neighbor) index. Carried
/// alongside the btree/search knobs on `IndexDef`. Wire shape is camelCase
/// (`filterFields`) to match the rest of the protocol.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorIndexSpec {
    pub dimensions: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub filter_fields: Vec<String>,
}

fn is_false(b: &bool) -> bool {
    !b
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
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SchemaDef {
    pub tables: BTreeMap<String, TableDef>,
}

/// Table and index names cap at 30 chars, and field names at 60 chars, so that
/// the DDL layer's physical names (`t_`/`f_` prefixes, `i_<table>_<index>`
/// composition, all lowercased) stay within Postgres's 63-byte identifier limit:
/// worst case `i_` + 30 + `_` + 30 = 63, exactly at the limit.
const MAX_TABLE_NAME_LEN: usize = 30;
const MAX_INDEX_NAME_LEN: usize = 30;
const MAX_FIELD_NAME_LEN: usize = 60;

/// Identifier regex `^[a-zA-Z][a-zA-Z0-9_]*$` bounded to `max_len` total chars.
fn is_valid_identifier(s: &str, max_len: usize) -> bool {
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
            fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
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
            owner_field: None,
            collaborators_field: None,
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
            fields: BTreeMap::from([(field_name, FieldType::String)]),
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
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
            fields: BTreeMap::from([(field_name, FieldType::String)]),
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
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
            fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
            indexes: vec![IndexDef {
                name: index_name,
                fields: vec!["name".to_string()],
                search: false,
                vector: None,
            }],
            owner_field: None,
            collaborators_field: None,
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
            fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
            indexes: vec![IndexDef {
                name: index_name,
                fields: vec!["name".to_string()],
                search: false,
                vector: None,
            }],
            owner_field: None,
            collaborators_field: None,
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn rejects_case_insensitive_field_name_collision() {
        let table = TableDef {
            fields: BTreeMap::from([
                ("status".to_string(), FieldType::String),
                ("Status".to_string(), FieldType::String),
            ]),
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn rejects_case_insensitive_index_name_collision() {
        let table = TableDef {
            fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
            indexes: vec![
                IndexDef {
                    name: "by_x".to_string(),
                    fields: vec!["name".to_string()],
                    search: false,
                    vector: None,
                },
                IndexDef {
                    name: "By_X".to_string(),
                    fields: vec!["name".to_string()],
                    search: false,
                    vector: None,
                },
            ],
            owner_field: None,
            collaborators_field: None,
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn rejects_field_name_starting_with_underscore() {
        let table = TableDef {
            fields: BTreeMap::from([("_secret".to_string(), FieldType::String)]),
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
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
                search: false,
                vector: None,
            }],
            owner_field: None,
            collaborators_field: None,
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
                search: false,
                vector: None,
            }],
            owner_field: None,
            collaborators_field: None,
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
                search: false,
                vector: None,
            }],
            owner_field: None,
            collaborators_field: None,
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
                    search: false,
                    vector: None,
                },
                IndexDef {
                    name: "by_name".to_string(),
                    fields: vec!["name".to_string()],
                    search: false,
                    vector: None,
                },
            ],
            owner_field: None,
            collaborators_field: None,
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
                search: false,
                vector: None,
            }],
            owner_field: None,
            collaborators_field: None,
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
                search: false,
                vector: None,
            }],
            owner_field: None,
            collaborators_field: None,
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
            owner_field: None,
            collaborators_field: None,
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
            owner_field: None,
            collaborators_field: None,
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
            owner_field: None,
            collaborators_field: None,
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
            fields: BTreeMap::from([(
                "meta".to_string(),
                FieldType::Record {
                    value: Box::new(FieldType::Number),
                },
            )]),
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
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
            }],
            owner_field: None,
            collaborators_field: None,
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
            fields: BTreeMap::from([("count".to_string(), FieldType::Number)]),
            indexes: vec![IndexDef {
                name: "search_count".to_string(),
                fields: vec!["count".to_string()],
                search: true,
                vector: None,
            }],
            owner_field: None,
            collaborators_field: None,
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn accepts_search_index_over_optional_text_field() {
        let table = TableDef {
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
            }],
            owner_field: None,
            collaborators_field: None,
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
            fields,
            indexes: vec![IndexDef {
                name: "by_emb".to_string(),
                fields: vec!["embedding".to_string()],
                search: false,
                vector: Some(VectorIndexSpec {
                    dimensions: 8,
                    filter_fields: vec![],
                }),
            }],
            owner_field: None,
            collaborators_field: None,
        };
        assert!(table.validate_structure("docs").is_err());
    }

    #[test]
    fn vector_index_accepts_matching_dims_and_filter_fields() {
        let mut fields = BTreeMap::new();
        fields.insert("embedding".to_string(), FieldType::Vector { dimensions: 4 });
        fields.insert("userId".to_string(), FieldType::String);
        let table = TableDef {
            fields,
            indexes: vec![IndexDef {
                name: "by_emb".to_string(),
                fields: vec!["embedding".to_string()],
                search: false,
                vector: Some(VectorIndexSpec {
                    dimensions: 4,
                    filter_fields: vec!["userId".to_string()],
                }),
            }],
            owner_field: None,
            collaborators_field: None,
        };
        assert!(table.validate_structure("docs").is_ok());
    }

    #[test]
    fn vector_index_rejects_search_and_vector_both_set() {
        let mut fields = BTreeMap::new();
        fields.insert("embedding".to_string(), FieldType::Vector { dimensions: 4 });
        let table = TableDef {
            fields,
            indexes: vec![IndexDef {
                name: "by_emb".to_string(),
                fields: vec!["embedding".to_string()],
                search: true,
                vector: Some(VectorIndexSpec {
                    dimensions: 4,
                    filter_fields: vec![],
                }),
            }],
            owner_field: None,
            collaborators_field: None,
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
            fields,
            indexes: vec![IndexDef {
                name: "by_emb".to_string(),
                fields: vec!["embedding".to_string()],
                search: false,
                vector: Some(VectorIndexSpec {
                    dimensions: 0,
                    filter_fields: vec![],
                }),
            }],
            owner_field: None,
            collaborators_field: None,
        };
        assert!(table.validate_structure("docs").is_err());
    }

    #[test]
    fn vector_index_rejects_two_fields() {
        let mut fields = BTreeMap::new();
        fields.insert("a".to_string(), FieldType::Vector { dimensions: 4 });
        fields.insert("b".to_string(), FieldType::Vector { dimensions: 4 });
        let table = TableDef {
            fields,
            indexes: vec![IndexDef {
                name: "by_emb".to_string(),
                fields: vec!["a".to_string(), "b".to_string()],
                search: false,
                vector: Some(VectorIndexSpec {
                    dimensions: 4,
                    filter_fields: vec![],
                }),
            }],
            owner_field: None,
            collaborators_field: None,
        };
        assert!(table.validate_structure("docs").is_err());
    }

    #[test]
    fn vector_index_rejects_non_vector_field() {
        let mut fields = BTreeMap::new();
        fields.insert("title".to_string(), FieldType::String);
        let table = TableDef {
            fields,
            indexes: vec![IndexDef {
                name: "by_title".to_string(),
                fields: vec!["title".to_string()],
                search: false,
                vector: Some(VectorIndexSpec {
                    dimensions: 4,
                    filter_fields: vec![],
                }),
            }],
            owner_field: None,
            collaborators_field: None,
        };
        assert!(table.validate_structure("docs").is_err());
    }

    #[test]
    fn vector_index_rejects_unknown_filter_field() {
        let mut fields = BTreeMap::new();
        fields.insert("embedding".to_string(), FieldType::Vector { dimensions: 4 });
        let table = TableDef {
            fields,
            indexes: vec![IndexDef {
                name: "by_emb".to_string(),
                fields: vec!["embedding".to_string()],
                search: false,
                vector: Some(VectorIndexSpec {
                    dimensions: 4,
                    filter_fields: vec!["userId".to_string()],
                }),
            }],
            owner_field: None,
            collaborators_field: None,
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
            fields,
            indexes: vec![IndexDef {
                name: "by_emb".to_string(),
                fields: vec!["embedding".to_string()],
                search: false,
                vector: Some(VectorIndexSpec {
                    dimensions: 4,
                    filter_fields: vec!["meta".to_string()],
                }),
            }],
            owner_field: None,
            collaborators_field: None,
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
}
