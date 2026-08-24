//! Structural schema validation: identifier/regconfig format checks, per-field
//! `FieldType` structural validation, and `TableDef`/`SchemaDef`'s
//! `validate`/`validate_structure` entry points (table/field/index names,
//! `ownerField`/`collaboratorsField`/`ttl`/`updatedAtField`/`autoIncrementField`
//! declarations, `authorize`, `computed`, `onDelete`). Value-level validation
//! lives in `schema::value`; filter-expression validation in `schema::filter`;
//! computed-expression validation in `schema::computed`.

use std::collections::HashSet;

use crate::error::RtDbError;

use super::computed::{StaticKind, infer_static_kind, validate_computed_case_whens};
use super::filter::validate_filter_expr_fields;
use super::types::{
    FieldType, IndexDef, MAX_FIELD_NAME_LEN, MAX_INDEX_NAME_LEN, MAX_TABLE_NAME_LEN,
    OnDeleteAction, SchemaDef, TableDef,
};
use super::value::{indexed_column_type, is_string_array_field, validate_value};

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

impl TableDef {
    pub(super) fn validate_structure(&self, table_name: &str) -> Result<(), RtDbError> {
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
    pub(super) fn validate_computed(&self, table_name: &str) -> Result<(), RtDbError> {
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
