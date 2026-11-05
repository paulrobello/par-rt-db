//! Declarative schema migration: an ordered list of directives the server
//! applies transactionally to transform a database's schema and documents.
//! See docs/superpowers/specs/2026-07-31-schema-migration-backfill-design.md.

mod apply;
mod backfill;
mod plan;

#[cfg(test)]
mod tests;

pub use apply::{MigrationEffects, apply_migration};
pub(crate) use backfill::backfill_computed;
pub use plan::plan_migration;

use crate::schema::FieldType;

/// One migration step. Wire shape mirrors `txn::Step`: `tag = "op"`,
/// camelCase, `deny_unknown_fields`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", deny_unknown_fields)]
pub enum Directive {
    RenameField {
        table: String,
        from: String,
        to: String,
    },
    RenameTable {
        from: String,
        to: String,
    },
    ChangeType {
        table: String,
        field: String,
        to: FieldType,
        cast: Cast,
        #[serde(default)]
        default: Option<serde_json::Value>,
    },
    DropField {
        table: String,
        field: String,
    },
    DropTable {
        name: String,
    },
    DropIndex {
        table: String,
        name: String,
    },
    SetDefault {
        table: String,
        field: String,
        value: serde_json::Value,
    },
    EvalExpr {
        table: String,
        set: String,
        /// ENH-020: dual-accept. A typed `ValueExpr` (safe, all-literals-bound
        /// path) or a legacy raw-SQL string (deprecated, gated to the root
        /// admin_key — the SEC-107 boundary until the string form is removed).
        expr: ExprSource,
        /// Dual-accept `where`: a typed `FilterExpr` (safe) or a legacy raw-SQL
        /// predicate string (deprecated, same root-admin gate).
        #[serde(default, skip_serializing_if = "Option::is_none", rename = "where")]
        where_clause: Option<CondSource>,
    },
}

/// The `ValueExpr` grammar and its `Cast`/`CaseWhen` companions live in
/// `crate::value_expr` (shared with computed fields, ENH-028); re-exported so
/// `crate::migrate::ValueExpr` paths and the `Directive` arms keep resolving.
pub use crate::value_expr::{CaseWhen, Cast, ValueExpr};

/// Dual-accept source for `EvalExpr.expr`: a typed `ValueExpr` (the safe path)
/// or a legacy raw-SQL string (the deprecated path, gated to root admin_key).
/// `#[serde(untagged)]` tries `Typed` first; a string fails `ValueExpr` (an
/// internally-tagged object) and falls through to `Legacy`. A hostile object
/// that is not a valid `ValueExpr` fails both arms and is rejected — it does
/// NOT silently become legacy.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum ExprSource {
    Typed(ValueExpr),
    Legacy(String),
}

/// Dual-accept source for `EvalExpr.where`: a typed `FilterExpr` or a legacy
/// raw-SQL predicate string. Same untagged discipline as `ExprSource`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum CondSource {
    Typed(crate::query::FilterExpr),
    Legacy(String),
}

/// A bind for the migrate expression path. `EqBind`'s four typed variants cover
/// `FilterExpr` values (compiled via `compile_filter`); `Json` covers
/// `ValueExpr::Literal` (any JSON value, bound as jsonb). The two coexist in one
/// UPDATE statement with contiguous `$n` numbering. `pub(crate)` because
/// `value_expr::compile_value_expr` fills this vec from the migrate call site.
#[derive(Debug, Clone)]
pub(crate) enum MigrateBind {
    Text(String),
    Num(f64),
    Bool(bool),
    I64(i64),
    Json(serde_json::Value),
}

impl From<crate::txn::EqBind> for MigrateBind {
    fn from(b: crate::txn::EqBind) -> Self {
        match b {
            crate::txn::EqBind::Text(s) => MigrateBind::Text(s),
            crate::txn::EqBind::Num(f) => MigrateBind::Num(f),
            crate::txn::EqBind::Bool(b) => MigrateBind::Bool(b),
            crate::txn::EqBind::I64(i) => MigrateBind::I64(i),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrateRequest {
    pub directives: Vec<Directive>,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrateResult {
    pub applied: bool,
    pub schema: crate::schema::SchemaDef,
    pub directives: Vec<DirectiveReport>,
}

/// Per-directive outcome. `Default` is derived so later tasks can build it
/// incrementally with `..Default::default()`; the `op` and `affected_rows`
/// fields are always set explicitly.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DirectiveReport {
    pub op: String,
    pub affected_rows: i64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cast_failures: Vec<CastFailure>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sample_changes: Vec<SampleChange>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CastFailure {
    pub id: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SampleChange {
    pub id: String,
    pub before: serde_json::Value,
    pub after: serde_json::Value,
}
