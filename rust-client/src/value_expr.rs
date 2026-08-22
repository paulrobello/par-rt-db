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

use crate::wire::FilterExpr;

/// Closed set of sound coercions. Shared by `Directive::ChangeType` and
/// `ValueExpr::Cast` — the four scalar casts that are sound to backfill.
/// Mirrors server `value_expr::Cast` (camelCase on the wire).
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Cast {
    /// Coerce to string.
    ToString,
    /// Coerce to JSON number.
    ToNumber,
    /// Coerce to 64-bit integer.
    ToInt64,
    /// Coerce to boolean.
    ToBoolean,
}

/// A closed, typed expression grammar for [`Directive::EvalExpr`](crate::wire::admin::Directive::EvalExpr)'s
/// backfill expression (ENH-020 Stage 1, closing SEC-107) and computed fields
/// (ENH-028). Mirrors server `value_expr::ValueExpr` byte-for-byte:
/// `tag = "op"`, camelCase, `deny_unknown_fields` (the same serde conventions
/// as [`crate::wire::FilterExpr`]). Every `Literal` is bound as a parameter on
/// the server; every `Field` resolves through the table's `TableDef` and reads
/// `doc->'field'`. There is deliberately **no** subquery node, no
/// function-call-by-name node, and no raw-SQL escape — the grammar is closed,
/// so the SEC-107 injection concern cannot arise from a `ValueExpr` payload.
/// The only way to reach raw SQL is the deprecated `Legacy(String)` source,
/// which remains gated to the root admin_key.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", deny_unknown_fields)]
pub enum ValueExpr {
    /// A declared field on this table (validated against `TableDef`). Reads
    /// `doc->'field'` (jsonb). The field must be declared; the write target
    /// (`EvalExpr.set`) need not be.
    Field {
        /// The declared field to read.
        field: String,
    },
    /// Any JSON literal. Bound as `$n::jsonb`, so objects/arrays/null round-trip.
    Literal {
        /// The literal value.
        value: serde_json::Value,
    },
    /// String concatenation. Postgres `concat(...)`, which ignores NULL args
    /// (treats them as empty) — wrap operands in `Coalesce` for explicit control.
    Concat {
        /// The concatenation operands.
        parts: Vec<ValueExpr>,
    },
    /// Numeric arithmetic. Operands are cast to `::numeric`; the result is a
    /// JSON number via the surrounding `to_jsonb`. Division by zero errors at
    /// runtime — guard with `Case`/`Coalesce` when the divisor may be zero.
    Add {
        /// Left operand (+).
        left: Box<ValueExpr>,
        /// Right operand (+).
        right: Box<ValueExpr>,
    },
    /// Subtraction (`left - right`).
    Sub {
        /// Left operand (-).
        left: Box<ValueExpr>,
        /// Right operand (-).
        right: Box<ValueExpr>,
    },
    /// Multiplication (`left * right`).
    Mul {
        /// Left operand (*).
        left: Box<ValueExpr>,
        /// Right operand (*).
        right: Box<ValueExpr>,
    },
    /// Division (`left / right`); by-zero errors at runtime.
    Div {
        /// Left operand (/).
        left: Box<ValueExpr>,
        /// Right operand (/).
        right: Box<ValueExpr>,
    },
    /// `COALESCE(parts...)` — first non-null, or NULL.
    Coalesce {
        /// First-non-null candidates.
        parts: Vec<ValueExpr>,
    },
    /// Text casing / trim. Operand cast to `::text`.
    Lower {
        /// Operand to lowercase.
        value: Box<ValueExpr>,
    },
    /// Uppercase.
    Upper {
        /// Operand to uppercase.
        value: Box<ValueExpr>,
    },
    /// Trim surrounding whitespace.
    Trim {
        /// Operand to trim.
        value: Box<ValueExpr>,
    },
    /// A closed scalar coercion. Reuses [`Directive::ChangeType`](crate::wire::admin::Directive::ChangeType)'s [`Cast`].
    Cast {
        /// Operand to coerce.
        value: Box<ValueExpr>,
        /// Target scalar type.
        to: Cast,
    },
    /// Current timestamp (`now()`), as jsonb.
    Now,
    /// Conditional: first matching `when`'s `then`, else `otherwise`. Each
    /// `when` is a [`crate::wire::FilterExpr`] (field references schema-
    /// validated, values bound).
    Case {
        /// Branch conditions, in order.
        whens: Vec<CaseWhen>,
        /// Fallback when no `when` matches.
        otherwise: Box<ValueExpr>,
    },
}

/// One branch of [`ValueExpr::Case`]. Wire shape `{ when, then }`. Mirrors
/// server `value_expr::CaseWhen` (camelCase, `deny_unknown_fields`).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CaseWhen {
    /// The branch condition.
    pub when: FilterExpr,
    /// The value when it matches.
    pub then: ValueExpr,
}

impl ValueExpr {
    /// A declared-field read (`doc->>'field'` text extraction).
    pub fn field(name: &str) -> Self {
        ValueExpr::Field { field: name.into() }
    }

    /// Any JSON literal.
    pub fn literal(value: impl Into<serde_json::Value>) -> Self {
        ValueExpr::Literal {
            value: value.into(),
        }
    }

    /// String concatenation; null parts are skipped.
    pub fn concat(parts: impl IntoIterator<Item = ValueExpr>) -> Self {
        ValueExpr::Concat {
            parts: parts.into_iter().collect(),
        }
    }

    /// `left + right` (IEEE double, SQL-NULL propagation).
    // These construct expression NODES (mirroring the wire variants and
    // the other four clients' builders) — they do not perform arithmetic,
    // so the std::ops trait shape does not apply.
    #[allow(clippy::should_implement_trait)]
    pub fn add(left: ValueExpr, right: ValueExpr) -> Self {
        ValueExpr::Add {
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// `left - right`.
    // These construct expression NODES (mirroring the wire variants and
    // the other four clients' builders) — they do not perform arithmetic,
    // so the std::ops trait shape does not apply.
    #[allow(clippy::should_implement_trait)]
    pub fn sub(left: ValueExpr, right: ValueExpr) -> Self {
        ValueExpr::Sub {
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// `left * right`.
    // These construct expression NODES (mirroring the wire variants and
    // the other four clients' builders) — they do not perform arithmetic,
    // so the std::ops trait shape does not apply.
    #[allow(clippy::should_implement_trait)]
    pub fn mul(left: ValueExpr, right: ValueExpr) -> Self {
        ValueExpr::Mul {
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// `left / right`; a zero divisor errors at evaluation time.
    // These construct expression NODES (mirroring the wire variants and
    // the other four clients' builders) — they do not perform arithmetic,
    // so the std::ops trait shape does not apply.
    #[allow(clippy::should_implement_trait)]
    pub fn div(left: ValueExpr, right: ValueExpr) -> Self {
        ValueExpr::Div {
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// First non-null part, else null.
    pub fn coalesce(parts: impl IntoIterator<Item = ValueExpr>) -> Self {
        ValueExpr::Coalesce {
            parts: parts.into_iter().collect(),
        }
    }

    /// ASCII lowercase of the operand's text form.
    pub fn lower(value: ValueExpr) -> Self {
        ValueExpr::Lower {
            value: Box::new(value),
        }
    }

    /// ASCII uppercase of the operand's text form.
    pub fn upper(value: ValueExpr) -> Self {
        ValueExpr::Upper {
            value: Box::new(value),
        }
    }

    /// Strip leading/trailing spaces (only) from the operand's text form.
    pub fn trim(value: ValueExpr) -> Self {
        ValueExpr::Trim {
            value: Box::new(value),
        }
    }

    /// A closed scalar coercion.
    pub fn cast(value: ValueExpr, to: Cast) -> Self {
        ValueExpr::Cast {
            value: Box::new(value),
            to,
        }
    }

    /// The current epoch-ms timestamp (a JSON number at evaluation time).
    pub fn now() -> Self {
        ValueExpr::Now
    }

    /// First matching branch's `then`, else `otherwise`.
    pub fn case(whens: Vec<CaseWhen>, otherwise: ValueExpr) -> Self {
        ValueExpr::Case {
            whens,
            otherwise: Box::new(otherwise),
        }
    }
}

/// Visits every field name a `ValueExpr` reads: each `Field` node, every
/// `Case` branch's `then`/`otherwise`, and every `FilterExpr` field inside
/// `Case.whens` — the same field set computed-field push validation
/// type-checks, exposed as a callback walk. Mirrors server
/// `value_expr::walk_value_expr_fields`.
pub fn walk_value_expr_fields(ve: &ValueExpr, f: &mut impl FnMut(&str)) {
    match ve {
        ValueExpr::Field { field } => f(field),
        ValueExpr::Literal { .. } | ValueExpr::Now => {}
        ValueExpr::Concat { parts } | ValueExpr::Coalesce { parts } => {
            for p in parts {
                walk_value_expr_fields(p, f);
            }
        }
        ValueExpr::Add { left, right }
        | ValueExpr::Sub { left, right }
        | ValueExpr::Mul { left, right }
        | ValueExpr::Div { left, right } => {
            walk_value_expr_fields(left, f);
            walk_value_expr_fields(right, f);
        }
        ValueExpr::Lower { value } | ValueExpr::Upper { value } | ValueExpr::Trim { value } => {
            walk_value_expr_fields(value, f);
        }
        ValueExpr::Cast { value, .. } => walk_value_expr_fields(value, f),
        ValueExpr::Case { whens, otherwise } => {
            for cw in whens {
                walk_filter_expr_fields(&cw.when, f);
                walk_value_expr_fields(&cw.then, f);
            }
            walk_value_expr_fields(otherwise, f);
        }
    }
}

/// The `FilterExpr` half of the walk: `And`/`Or`/`Not` recurse; every leaf
/// variant carries `field: String`. Mirrors server
/// `value_expr::walk_filter_expr_fields`.
pub fn walk_filter_expr_fields(expr: &FilterExpr, f: &mut impl FnMut(&str)) {
    match expr {
        FilterExpr::Eq { field, .. }
        | FilterExpr::Neq { field, .. }
        | FilterExpr::Gt { field, .. }
        | FilterExpr::Gte { field, .. }
        | FilterExpr::Lt { field, .. }
        | FilterExpr::Lte { field, .. }
        | FilterExpr::In { field, .. }
        | FilterExpr::Contains { field, .. }
        | FilterExpr::Exists { field } => f(field),
        FilterExpr::And { exprs } | FilterExpr::Or { exprs } => {
            for e in exprs {
                walk_filter_expr_fields(e, f);
            }
        }
        FilterExpr::Not { expr } => walk_filter_expr_fields(expr, f),
    }
}
