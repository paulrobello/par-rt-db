//! Schema DSL type definitions -- `FieldType` (15 variants), `IndexDef` (btree /
//! full-text search / vector), `TtlDef`, `TableDef` (fields, indexes, per-row
//! `ownerField`/`collaboratorsField`/`authorize`, `ttl`), and `SchemaDef`. Wire
//! shapes use `#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]`
//! and are mirrored field-for-field by the three client SDKs. See `schema::validate`
//! for structural validation and `schema::value` for value-level validation.
//!
//! These types (and the pure `strip_on_delete`/`is_widening_of`/`literal_set`
//! helpers) live in `par_rt_db_core::schema` (ARC-004) — the server and Rust
//! client share one definition instead of two hand-kept mirrors. Re-exported
//! here so every existing `crate::schema::types::X` / `crate::schema::X` call
//! site keeps resolving unchanged.

pub use par_rt_db_core::schema::{
    DistanceMetric, FieldType, IndexDef, OnDeleteAction, SchemaDef, TableDef, TtlDef,
    VectorIndexSpec, is_widening_of, literal_set, strip_on_delete,
};
pub(crate) use par_rt_db_core::schema::{
    MAX_FIELD_NAME_LEN, MAX_INDEX_NAME_LEN, MAX_TABLE_NAME_LEN,
};
