//! Schema DSL — `FieldType` (15 variants), `IndexDef` (btree / full-text search
//! / vector), `TtlDef`, `TableDef` (fields, indexes, per-row `ownerField`/
//! `collaboratorsField`/`authorize`, `ttl`), and `SchemaDef`. Wire shapes use
//! `#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]` and
//! are mirrored field-for-field by the three client SDKs. Validation
//! (`validate_doc`/`validate_value`/`validate_filter_expr_fields`) is shared with
//! the read and write paths; the index-value typing here must stay aligned with
//! `ddl` (one typed column per indexed field). Schema changes are additive-only.
//!
//! Split by concern (QA-005): `types` (the wire DSL structs/enums), `validate`
//! (structural schema validation), `filter` (filter-expression field
//! validation, including the SEC-007 depth/list-length caps), `computed`
//! (computed-expression validation), and `value` (value-level validation and
//! the index-typing shared with `ddl`). Every item below is re-exported here so
//! `crate::schema::X` keeps resolving unchanged for the rest of the crate.

mod computed;
mod filter;
mod types;
mod validate;
mod value;

#[cfg(test)]
mod tests;

pub use computed::*;
pub use filter::*;
pub use types::*;
pub(crate) use validate::*;
pub use validate::{SchemaDefExt, TableDefExt};
pub use value::*;
