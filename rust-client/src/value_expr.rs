//! The closed `ValueExpr` grammar — the typed, injection-safe expression
//! language shared by migrate's `Directive::EvalExpr` backfill (ENH-020) and
//! computed fields (ENH-028). Mirrors `server/src/value_expr.rs` byte-for-byte
//! on the wire: one shape, two consumers. The migrate path serializes it inside
//! [`crate::wire::admin::Directive::EvalExpr`] (HTTP-only, feature `admin`);
//! the computed-fields path serializes it inside
//! [`crate::schema::TableDef::computed`] (core, always compiled) — hence this
//! unconditional home, with `wire::admin` re-exporting so both spellings name
//! one type.
//!
//! The in-memory interpreter for this grammar lives in
//! `in_memory/value_expr.rs` (feature `in_memory`); the field walkers below are
//! unconditional because schema push validation and migrate planning both use
//! them.

/// The closed `ValueExpr` grammar and its `Cast`/`CaseWhen` companions,
/// defined once in `par-rt-db-core` (ARC-004) and re-exported here at their
/// historical paths. `ValueExpr`'s builder constructors live with the type.
pub use par_rt_db_core::wire::{CaseWhen, Cast, ValueExpr};

/// The field walks over the shared grammar, defined once in `par-rt-db-core`
/// (ARC-004) and re-exported here at their historical paths.
pub use par_rt_db_core::fields::{walk_filter_expr_fields, walk_value_expr_fields};
