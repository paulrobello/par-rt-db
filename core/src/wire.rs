//! The par-rt-db wire vocabulary shared by the server and the Rust client.
//!
//! `FilterExpr` and the `ValueExpr` grammar are not "the server's types that
//! the client mirrors" — they are ONE type with two consumers. Before ARC-004
//! each crate carried its own copy, kept byte-identical by review and by the
//! wire corpus; a drift showed up as a corpus failure rather than a compile
//! error. Defining them once here makes the two crates structurally incapable
//! of disagreeing.
//!
//! Both crates re-export these at their historical paths
//! (`rtdb_server::query::FilterExpr`, `par_rt_db_client::wire::FilterExpr`, …),
//! so no call site moved.

use serde::{Deserialize, Serialize};

/// A db-side predicate appended to a query's WHERE clause. Internally tagged by `op`
/// (lowercase), `deny_unknown_fields`. Leaves compare one declared field to a
/// value (`In` to a non-empty list); `And`/`Or` nest arbitrarily; `Not` wraps
/// a nested expr; `Contains` tests membership of `value` in `doc.field[]`
/// (reverse of `In`); `Exists` tests the field is present and non-null.
///
/// Construct variants directly (`FilterExpr::Eq { field, value }`) — inherent
/// constructors named `eq`/`gt`/`lt` are avoided because they shadow
/// `PartialEq`/`PartialOrd` trait methods (`clippy::should_implement_trait`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase", deny_unknown_fields)]
pub enum FilterExpr {
    /// `field == value`.
    Eq {
        /// The declared field to compare.
        field: String,
        /// The value to compare against.
        value: serde_json::Value,
    },
    /// `field != value`.
    Neq {
        /// The declared field to compare.
        field: String,
        /// The value to compare against.
        value: serde_json::Value,
    },
    /// `field > value`.
    Gt {
        /// The declared field to compare.
        field: String,
        /// The value to compare against.
        value: serde_json::Value,
    },
    /// `field >= value`.
    Gte {
        /// The declared field to compare.
        field: String,
        /// The value to compare against.
        value: serde_json::Value,
    },
    /// `field < value`.
    Lt {
        /// The declared field to compare.
        field: String,
        /// The value to compare against.
        value: serde_json::Value,
    },
    /// `field <= value`.
    Lte {
        /// The declared field to compare.
        field: String,
        /// The value to compare against.
        value: serde_json::Value,
    },
    /// `field` equals any of `values` (non-empty).
    In {
        /// The declared field to compare.
        field: String,
        /// The accepted values.
        values: Vec<serde_json::Value>,
    },
    /// Every sub-expression matches.
    And {
        /// The conjuncts.
        exprs: Vec<FilterExpr>,
    },
    /// Any sub-expression matches.
    Or {
        /// The disjuncts.
        exprs: Vec<FilterExpr>,
    },
    /// Negation.
    Not {
        /// The negated expression.
        expr: Box<FilterExpr>,
    },
    /// `value` is a member of `doc.field[]`.
    Contains {
        /// The array field to test.
        field: String,
        /// The candidate member.
        value: serde_json::Value,
    },
    /// The field is present and non-null.
    Exists {
        /// The field to test.
        field: String,
    },
    /// Execution-time-relative age predicate: the field's epoch-ms value is
    /// strictly older than `now − ms`, with `now` read from the clock AT
    /// EXECUTION TIME — a scheduled txn's stored filter stays fresh on every
    /// fire instead of freezing a literal at schedule time (server-side
    /// sweeps: archive done rows older than 7d, expire claim leases). Valid
    /// ONLY in the by-query step filters (`patchByQuery`/`deleteByQuery`);
    /// read-path filters, `authorize` predicates, partial-index `where`
    /// predicates, and computed `case` whens reject it. The field must be
    /// declared `number` or `int64` (a null or absent value never matches),
    /// and `ms >= 0`.
    #[serde(rename = "olderThan")]
    OlderThan {
        /// The declared field to test.
        field: String,
        /// Age window in milliseconds (`>= 0`).
        ms: i64,
    },
}

/// Closed set of sound coercions. Shared by `Directive::ChangeType` and
/// `ValueExpr::Cast` — the four scalar casts that are sound to backfill.
/// Serializes camelCase.
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

/// A closed, typed expression grammar for `Directive::EvalExpr`'s
/// backfill expression (ENH-020 Stage 1, closing SEC-107) and computed fields
/// (ENH-028). `tag = "op"`, camelCase, `deny_unknown_fields` (the same serde conventions
/// as [`FilterExpr`]). Every `Literal` is bound as a parameter on
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
    /// A closed scalar coercion. Reuses `Directive::ChangeType`'s [`Cast`].
    Cast {
        /// Operand to coerce.
        value: Box<ValueExpr>,
        /// Target scalar type.
        to: Cast,
    },
    /// Current timestamp (`now()`), as jsonb.
    Now,
    /// Conditional: first matching `when`'s `then`, else `otherwise`. Each
    /// `when` is a [`FilterExpr`] (field references schema-
    /// validated, values bound).
    Case {
        /// Branch conditions, in order.
        whens: Vec<CaseWhen>,
        /// Fallback when no `when` matches.
        otherwise: Box<ValueExpr>,
    },
}

/// One branch of [`ValueExpr::Case`]. Wire shape `{ when, then }`. Serializes camelCase with `deny_unknown_fields`.
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
