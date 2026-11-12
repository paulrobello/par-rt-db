//! The spec-table corpus generator (ENH-052, family 1): renders
//! `wire-corpus/spec/index-column-types.json` — the FieldType → Postgres
//! column-type decision table every in-memory engine mirrors
//! (`schema::indexed_column_type` and its six client ports, the
//! `indexColumnType` family). The server crate is the single source: the rows
//! are computed by calling [`crate::schema::indexed_column_type`] itself, and
//! the generator's exhaustive match over `FieldType` makes a variant added
//! without a representative row a compile error.
//!
//! `server/src/bin/gen-spec-index-column-types.rs` writes the fixture;
//! `make gen-spec-index-column-types-check` (in `checkall`) diffs the
//! committed fixture against a fresh generation, and the test below pins the
//! same property in-crate.

use std::collections::BTreeMap;

use serde_json::{Value, json};

use crate::dsl::FilterExpr;
use crate::schema::{FieldType, OnDeleteAction, TableDef, indexed_column_type};
use crate::txn::stamp_computed;
use crate::value_expr::{CaseWhen, Cast, ValueExpr};

/// One representative `FieldType` per enum variant, plus the sub-shapes the
/// mapping distinguishes within a variant (`Literal` string vs non-string,
/// `Union` all-string-literals vs empty vs mixed, `Optional` over two inner
/// types, `Id` with and without `onDelete`). Order is the committed fixture's
/// row order.
fn spec_field_types() -> Vec<FieldType> {
    fn lit(value: Value) -> FieldType {
        FieldType::Literal { value }
    }
    fn string_literal_union(variants: Vec<&str>) -> FieldType {
        FieldType::Union {
            variants: variants
                .into_iter()
                .map(|s| lit(serde_json::Value::from(s)))
                .collect(),
        }
    }
    let mut object_fields = BTreeMap::new();
    object_fields.insert("a".to_string(), FieldType::String);
    vec![
        FieldType::String,
        FieldType::Number,
        FieldType::Boolean,
        FieldType::Int64,
        FieldType::Id {
            table: "t".to_string(),
            on_delete: None,
        },
        FieldType::Id {
            table: "t".to_string(),
            on_delete: Some(OnDeleteAction::Cascade),
        },
        lit(json!("x")),
        lit(json!(42)),
        FieldType::Optional {
            inner: Box::new(FieldType::String),
        },
        FieldType::Optional {
            inner: Box::new(FieldType::Number),
        },
        string_literal_union(vec!["a", "b"]),
        FieldType::Union {
            variants: Vec::new(),
        },
        FieldType::Union {
            variants: vec![lit(json!("a")), FieldType::Number],
        },
        FieldType::Null,
        FieldType::Array {
            element: Box::new(FieldType::String),
        },
        FieldType::Object {
            fields: object_fields,
        },
        FieldType::Bytes,
        FieldType::Any,
        FieldType::Record {
            value: Box::new(FieldType::String),
        },
        FieldType::Vector { dimensions: 3 },
    ]
}

/// The compile-time drift net: names every `FieldType` variant with no
/// wildcard arm, so a variant added to the enum without a representative row
/// in [`spec_field_types`] fails this crate to compile. Keep the arm list in
/// the enum's declaration order.
fn exhaustiveness_guard(ty: &FieldType) {
    match ty {
        FieldType::String
        | FieldType::Number
        | FieldType::Boolean
        | FieldType::Null
        | FieldType::Id { .. }
        | FieldType::Literal { .. }
        | FieldType::Optional { .. }
        | FieldType::Union { .. }
        | FieldType::Array { .. }
        | FieldType::Object { .. }
        | FieldType::Int64
        | FieldType::Bytes
        | FieldType::Any
        | FieldType::Record { .. }
        | FieldType::Vector { .. } => {}
    }
}

/// One fixture row per representative: the wire `FieldType` JSON (serde's own
/// serialization — the same spelling every client parses for pushSchema) plus
/// the mapping's outcome, `{"columnType", "nullable"}` for an indexable type
/// or `{"error": true}` for a non-indexable one.
fn spec_rows() -> Result<Vec<Value>, serde_json::Error> {
    let mut rows = Vec::new();
    for ty in spec_field_types() {
        exhaustiveness_guard(&ty);
        let field_type = serde_json::to_value(&ty)?;
        let row = match indexed_column_type(&ty) {
            Ok((column_type, nullable)) => json!({
                "fieldType": field_type,
                "columnType": column_type,
                "nullable": nullable,
            }),
            Err(_) => json!({
                "fieldType": field_type,
                "error": true,
            }),
        };
        rows.push(row);
    }
    Ok(rows)
}

/// Render the committed fixture's exact bytes (pretty JSON + trailing
/// newline).
pub fn render_index_column_type_spec() -> Result<String, serde_json::Error> {
    let doc = json!({
        "$comment": "SPEC-TABLE CORPUS (ENH-052 family 1) — GENERATED, do not edit. \
                     The FieldType -> Postgres column-type decision table, one row per \
                     representative per server FieldType variant, computed from \
                     server/src/schema/value.rs::indexed_column_type by \
                     'make gen-spec-index-column-types'. `columnType` is the server's raw \
                     Postgres type string and `nullable` whether the source field was \
                     wrapped in Optional; non-indexable rows carry `error: true`. Each \
                     runner asserts its own port of the mapping (the client PgType \
                     four-value enum mapped back to these raw strings) against every row, \
                     and that its own FieldType variant set is covered. A mapping change \
                     regenerates this file in the same commit — see wire-corpus/README.md \
                     'Spec-table corpus'.",
        "rows": spec_rows()?,
    });
    let mut out = serde_json::to_string_pretty(&doc)?;
    out.push('\n');
    Ok(out)
}

// ---------------------------------------------------------------------------
// ENH-052 family 2: computed-field stamping
// ---------------------------------------------------------------------------
//
// The stamping decision table (ENH-028): the server re-evaluates every
// `computed` entry over the final doc on every write
// (`txn::stamp_computed` driving `value_expr::eval_value_expr`). The rows
// below cover the closed `ValueExpr` grammar exhaustively (every node, every
// `Cast` target) plus the stamping-layer rules (null removes the key,
// non-null overwrites any client-supplied value, errors name the field and
// fail the write as `bad_request`). Each runner decodes each row's wire
// schema fragment through its OWN schema parser and asserts its own
// `stampComputed` port against the stamped output.

/// One representative `ValueExpr` per grammar node and per behavior the
/// stamping contract distinguishes. Order is the committed fixture's row
/// order; every row's `computed` map carries the expression(s), `fields` the
/// declared types the expressions read and write.
struct ComputedSpecRow {
    id: &'static str,
    fields: BTreeMap<String, FieldType>,
    computed: BTreeMap<String, ValueExpr>,
    input_doc: serde_json::Map<String, Value>,
    now: i64,
}
/// `Optional` over `inner` — the spelling every declared computed field uses,
/// so a null-stamped result leaves a valid doc (key absent) under
/// `validate_doc`'s shape convention.
fn opt(inner: FieldType) -> FieldType {
    FieldType::Optional {
        inner: Box::new(inner),
    }
}

fn spec_fields(entries: &[(&str, FieldType)]) -> BTreeMap<String, FieldType> {
    entries
        .iter()
        .map(|(name, ty)| (name.to_string(), ty.clone()))
        .collect()
}

fn spec_computed(entries: &[(&str, ValueExpr)]) -> BTreeMap<String, ValueExpr> {
    entries
        .iter()
        .map(|(name, expr)| (name.to_string(), expr.clone()))
        .collect()
}

fn spec_doc(entries: &[(&str, Value)]) -> serde_json::Map<String, Value> {
    entries
        .iter()
        .map(|(name, value)| (name.to_string(), value.clone()))
        .collect()
}

fn case_when(when: FilterExpr, then: ValueExpr) -> CaseWhen {
    CaseWhen { when, then }
}

/// The compile-time drift net for the expression grammar: a `ValueExpr` node,
/// a `Cast` target, or a `FilterExpr` predicate shape without representative
/// coverage fails this crate to compile. Recursive, so nested nodes are
/// guarded too.
fn expr_guard(expr: &ValueExpr) {
    match expr {
        ValueExpr::Field { field } => {
            let _ = field;
        }
        ValueExpr::Literal { value } => {
            let _ = value;
        }
        ValueExpr::Concat { parts } => parts.iter().for_each(expr_guard),
        ValueExpr::Add { left, right }
        | ValueExpr::Sub { left, right }
        | ValueExpr::Mul { left, right }
        | ValueExpr::Div { left, right } => {
            expr_guard(left);
            expr_guard(right);
        }
        ValueExpr::Coalesce { parts } => parts.iter().for_each(expr_guard),
        ValueExpr::Lower { value } | ValueExpr::Upper { value } | ValueExpr::Trim { value } => {
            expr_guard(value)
        }
        ValueExpr::Cast { value, to } => {
            expr_guard(value);
            match to {
                Cast::ToString | Cast::ToNumber | Cast::ToInt64 | Cast::ToBoolean => {}
            }
        }
        ValueExpr::Now => {}
        ValueExpr::Case { whens, otherwise } => {
            for cw in whens {
                match &cw.when {
                    FilterExpr::Eq { field, value }
                    | FilterExpr::Neq { field, value }
                    | FilterExpr::Gt { field, value }
                    | FilterExpr::Gte { field, value }
                    | FilterExpr::Lt { field, value }
                    | FilterExpr::Lte { field, value } => {
                        let _ = (field, value);
                    }
                    FilterExpr::In { field, values } => {
                        let _ = (field, values);
                    }
                    FilterExpr::And { exprs } | FilterExpr::Or { exprs } => {
                        let _ = exprs;
                    }
                    FilterExpr::Not { expr } => {
                        let _ = expr;
                    }
                    FilterExpr::Contains { field, value } => {
                        let _ = (field, value);
                    }
                    FilterExpr::Exists { field } => {
                        let _ = field;
                    }
                    FilterExpr::OlderThan { field, ms } => {
                        let _ = (field, ms);
                    }
                }
                expr_guard(&cw.then);
            }
            expr_guard(otherwise);
        }
    }
}

/// The representative rows. Every computed field is declared `optional`, so a
/// null-stamped result leaves a `validate_doc`-valid doc. Field reads pull
/// from `input_doc`; `now` feeds the `Now` node. Order is the committed
/// fixture's row order.
fn computed_spec_rows() -> Vec<ComputedSpecRow> {
    fn s() -> FieldType {
        FieldType::String
    }
    fn n() -> FieldType {
        FieldType::Number
    }
    fn b() -> FieldType {
        FieldType::Boolean
    }
    fn any() -> FieldType {
        FieldType::Any
    }
    fn obj_ab(elem: FieldType) -> FieldType {
        FieldType::Object {
            fields: spec_fields(&[("a", elem.clone()), ("b", elem)]),
        }
    }

    let mut rows: Vec<ComputedSpecRow> = Vec::new();
    macro_rules! row {
        ($id:expr, $fields:expr, $computed:expr, $doc:expr) => {
            rows.push(ComputedSpecRow {
                id: $id,
                fields: spec_fields(&$fields),
                computed: spec_computed(&$computed),
                input_doc: spec_doc(&$doc),
                now: 0,
            });
        };
    }
    macro_rules! row_now {
        ($id:expr, $fields:expr, $computed:expr, $doc:expr, $now:expr) => {
            rows.push(ComputedSpecRow {
                id: $id,
                fields: spec_fields(&$fields),
                computed: spec_computed(&$computed),
                input_doc: spec_doc(&$doc),
                now: $now,
            });
        };
    }

    // -- Field: text extraction (`doc->>'field'` semantics) ------------------
    row!(
        "field-reads-text-form-string",
        [("name", s()), ("echo", opt(s()))],
        [("echo", ValueExpr::field("name"))],
        [("name", json!("Ada"))]
    );
    row!(
        "field-absent-key-yields-null-removes",
        [("ghost", opt(s())), ("echo", opt(s()))],
        [("echo", ValueExpr::field("ghost"))],
        []
    );
    row!(
        "field-json-null-yields-null-removes",
        [("ghost", opt(s())), ("echo", opt(s()))],
        [("echo", ValueExpr::field("ghost"))],
        [("ghost", Value::Null)]
    );
    row!(
        "field-number-text-form",
        [("num", n()), ("echo", opt(s()))],
        [("echo", ValueExpr::field("num"))],
        [("num", json!(42))]
    );
    row!(
        "field-float-text-form",
        [("num", n()), ("echo", opt(s()))],
        [("echo", ValueExpr::field("num"))],
        [("num", json!(42.5))]
    );
    row!(
        "field-bool-text-form",
        [("flag", b()), ("echo", opt(s()))],
        [("echo", ValueExpr::field("flag"))],
        [("flag", json!(true))]
    );
    row!(
        "field-object-compact-json-text",
        [("obj", opt(obj_ab(n()))), ("echo", opt(s()))],
        [("echo", ValueExpr::field("obj"))],
        [("obj", json!({"a": 1, "b": 2}))]
    );
    row!(
        "field-array-compact-json-text",
        [
            (
                "arr",
                opt(FieldType::Array {
                    element: Box::new(any())
                })
            ),
            ("echo", opt(s()))
        ],
        [("echo", ValueExpr::field("arr"))],
        [("arr", json!(["x", 2, true, null]))]
    );

    // -- Literal --------------------------------------------------------------
    row!(
        "literal-string",
        [("k", opt(s()))],
        [("k", ValueExpr::literal(json!("v")))],
        []
    );
    row!(
        "literal-number",
        [("k", opt(n()))],
        [("k", ValueExpr::literal(json!(42)))],
        []
    );
    row!(
        "literal-float",
        [("k", opt(n()))],
        [("k", ValueExpr::literal(json!(42.5)))],
        []
    );
    row!(
        "literal-boolean",
        [("k", opt(b()))],
        [("k", ValueExpr::literal(json!(true)))],
        []
    );
    row!(
        "literal-object-round-trips",
        [("k", opt(obj_ab(s())))],
        [("k", ValueExpr::literal(json!({"a": "x", "b": "y"})))],
        []
    );
    row!(
        "literal-array-round-trips",
        [(
            "k",
            opt(FieldType::Array {
                element: Box::new(s())
            })
        )],
        [("k", ValueExpr::literal(json!(["p", "q"])))],
        []
    );
    row!(
        "literal-null-removes-client-value",
        [("k", opt(s()))],
        [("k", ValueExpr::literal(Value::Null))],
        [("k", json!("client"))]
    );

    // -- Concat: Postgres concat(), null parts skipped -------------------------
    row!(
        "concat-joins-text-forms",
        [("name", s()), ("full", opt(s()))],
        [(
            "full",
            ValueExpr::concat([ValueExpr::literal(json!("Ms. ")), ValueExpr::field("name")])
        )],
        [("name", json!("Ada"))]
    );
    row!(
        "concat-skips-null-parts",
        [("ghost", opt(s())), ("full", opt(s()))],
        [(
            "full",
            ValueExpr::concat([ValueExpr::field("ghost"), ValueExpr::literal(json!("x"))])
        )],
        []
    );
    row!(
        "concat-renders-numbers-and-bools",
        [("num", n()), ("flag", b()), ("full", opt(s()))],
        [(
            "full",
            ValueExpr::concat([ValueExpr::field("num"), ValueExpr::field("flag")])
        )],
        [("num", json!(42)), ("flag", json!(true))]
    );

    // -- Arithmetic: IEEE doubles, SQL-NULL propagation ------------------------
    row!(
        "add-literal-numbers",
        [("sum", opt(n()))],
        [(
            "sum",
            ValueExpr::add(ValueExpr::literal(json!(2)), ValueExpr::literal(json!(3)))
        )],
        []
    );
    row!(
        "add-parses-numeric-strings",
        [("a", opt(s())), ("bb", opt(s())), ("sum", opt(n()))],
        [(
            "sum",
            ValueExpr::add(ValueExpr::field("a"), ValueExpr::field("bb"))
        )],
        [("a", json!("3")), ("bb", json!("4"))]
    );
    row!(
        "add-trims-string-operands",
        [("a", opt(s())), ("sum", opt(n()))],
        [(
            "sum",
            ValueExpr::add(ValueExpr::field("a"), ValueExpr::literal(json!(1)))
        )],
        [("a", json!(" 3 "))]
    );
    row!(
        "sub-literal-numbers",
        [("diff", opt(n()))],
        [(
            "diff",
            ValueExpr::sub(ValueExpr::literal(json!(10)), ValueExpr::literal(json!(4)))
        )],
        []
    );
    row!(
        "sub-float-result",
        [("diff", opt(n()))],
        [(
            "diff",
            ValueExpr::sub(
                ValueExpr::literal(json!(2.5)),
                ValueExpr::literal(json!(1.25))
            )
        )],
        []
    );
    row!(
        "mul-literal-numbers",
        [("prod", opt(n()))],
        [(
            "prod",
            ValueExpr::mul(ValueExpr::literal(json!(6)), ValueExpr::literal(json!(7)))
        )],
        []
    );
    row!(
        "div-fractional-result",
        [("quot", opt(n()))],
        [(
            "quot",
            ValueExpr::div(ValueExpr::literal(json!(7)), ValueExpr::literal(json!(2)))
        )],
        []
    );
    row!(
        "add-null-propagation-removes-key",
        [("ghost", opt(n())), ("sum", opt(n()))],
        [(
            "sum",
            ValueExpr::add(ValueExpr::field("ghost"), ValueExpr::literal(json!(5)))
        )],
        []
    );
    row!(
        "div-by-zero-errors",
        [("quot", opt(n()))],
        [(
            "quot",
            ValueExpr::div(ValueExpr::literal(json!(5)), ValueExpr::literal(json!(0)))
        )],
        []
    );
    row!(
        "null-divisor-propagates-before-zero-check",
        [("ghost", opt(n())), ("quot", opt(n()))],
        [(
            "quot",
            ValueExpr::div(ValueExpr::field("ghost"), ValueExpr::literal(json!(0)))
        )],
        []
    );
    row!(
        "mul-overflow-errors",
        [("prod", opt(n()))],
        [(
            "prod",
            ValueExpr::mul(
                ValueExpr::literal(json!(1e308)),
                ValueExpr::literal(json!(10))
            )
        )],
        []
    );
    row!(
        "add-non-numeric-string-errors",
        [("str", opt(s())), ("sum", opt(n()))],
        [(
            "sum",
            ValueExpr::add(ValueExpr::field("str"), ValueExpr::literal(json!(1)))
        )],
        [("str", json!("abc"))]
    );
    row!(
        "add-bool-operand-errors",
        [("sum", opt(n()))],
        [(
            "sum",
            ValueExpr::add(
                ValueExpr::literal(json!(true)),
                ValueExpr::literal(json!(1))
            )
        )],
        []
    );

    // -- Coalesce: first non-null, lazy ----------------------------------------
    row!(
        "coalesce-first-non-null",
        [("ghost", opt(s())), ("picked", opt(s()))],
        [(
            "picked",
            ValueExpr::coalesce([
                ValueExpr::field("ghost"),
                ValueExpr::literal(json!("fallback"))
            ])
        )],
        []
    );
    row!(
        "coalesce-all-null-removes-key",
        [("ghost", opt(s())), ("picked", opt(s()))],
        [(
            "picked",
            ValueExpr::coalesce([ValueExpr::field("ghost"), ValueExpr::literal(Value::Null)])
        )],
        []
    );
    row!(
        "coalesce-short-circuits-lazily",
        [("picked", opt(s()))],
        [(
            "picked",
            ValueExpr::coalesce([
                ValueExpr::literal(json!("first")),
                ValueExpr::div(ValueExpr::literal(json!(1)), ValueExpr::literal(json!(0)))
            ])
        )],
        []
    );

    // -- Lower / Upper / Trim --------------------------------------------------
    row!(
        "lower-text",
        [("name", s()), ("low", opt(s()))],
        [("low", ValueExpr::lower(ValueExpr::field("name")))],
        [("name", json!("Ada"))]
    );
    row!(
        "upper-renders-number-text",
        [("num", n()), ("up", opt(s()))],
        [("up", ValueExpr::upper(ValueExpr::field("num")))],
        [("num", json!(42))]
    );
    row!(
        "trim-strips-spaces-only",
        [("str", opt(s())), ("trimmed", opt(s()))],
        [("trimmed", ValueExpr::trim(ValueExpr::field("str")))],
        [("str", json!("  a  "))]
    );
    row!(
        "trim-tab-survives",
        [("str", opt(s())), ("trimmed", opt(s()))],
        [("trimmed", ValueExpr::trim(ValueExpr::field("str")))],
        [("str", json!("\ta "))]
    );
    row!(
        "lower-null-removes-key",
        [("ghost", opt(s())), ("low", opt(s()))],
        [("low", ValueExpr::lower(ValueExpr::field("ghost")))],
        []
    );

    // -- Cast: every target ----------------------------------------------------
    row!(
        "cast-toString-number-text",
        [("num", n()), ("out", opt(s()))],
        [(
            "out",
            ValueExpr::cast(ValueExpr::field("num"), Cast::ToString)
        )],
        [("num", json!(42))]
    );
    row!(
        "cast-toString-null-removes-key",
        [("ghost", opt(n())), ("out", opt(s()))],
        [(
            "out",
            ValueExpr::cast(ValueExpr::field("ghost"), Cast::ToString)
        )],
        []
    );
    row!(
        "cast-toNumber-string",
        [("out", opt(n()))],
        [(
            "out",
            ValueExpr::cast(ValueExpr::literal(json!("12.5")), Cast::ToNumber)
        )],
        []
    );
    row!(
        "cast-toNumber-integer-string",
        [("out", opt(n()))],
        [(
            "out",
            ValueExpr::cast(ValueExpr::literal(json!("42")), Cast::ToNumber)
        )],
        []
    );
    row!(
        "cast-toNumber-bool-errors",
        [("out", opt(n()))],
        [(
            "out",
            ValueExpr::cast(ValueExpr::literal(json!(true)), Cast::ToNumber)
        )],
        []
    );
    row!(
        "cast-toInt64-string",
        [("out", opt(FieldType::Int64))],
        [(
            "out",
            ValueExpr::cast(ValueExpr::literal(json!("42")), Cast::ToInt64)
        )],
        []
    );
    row!(
        "cast-toInt64-non-integer-string-errors",
        [("out", opt(FieldType::Int64))],
        [(
            "out",
            ValueExpr::cast(ValueExpr::literal(json!("42.5")), Cast::ToInt64)
        )],
        []
    );
    row!(
        "cast-toInt64-bool-errors",
        [("out", opt(FieldType::Int64))],
        [(
            "out",
            ValueExpr::cast(ValueExpr::literal(json!(false)), Cast::ToInt64)
        )],
        []
    );
    row!(
        "cast-toBoolean-true-words-case-insensitive",
        [("out", opt(b()))],
        [(
            "out",
            ValueExpr::cast(ValueExpr::literal(json!("YES")), Cast::ToBoolean)
        )],
        []
    );
    row!(
        "cast-toBoolean-false-words",
        [("out", opt(b()))],
        [(
            "out",
            ValueExpr::cast(ValueExpr::literal(json!("off")), Cast::ToBoolean)
        )],
        []
    );
    row!(
        "cast-toBoolean-numbers-one-and-zero",
        [("t1", opt(b())), ("f0", opt(b()))],
        [
            (
                "t1",
                ValueExpr::cast(ValueExpr::literal(json!(1)), Cast::ToBoolean)
            ),
            (
                "f0",
                ValueExpr::cast(ValueExpr::literal(json!(0)), Cast::ToBoolean)
            ),
        ],
        []
    );
    row!(
        "cast-toBoolean-invalid-word-errors",
        [("out", opt(b()))],
        [(
            "out",
            ValueExpr::cast(ValueExpr::literal(json!("maybe")), Cast::ToBoolean)
        )],
        []
    );
    row!(
        "cast-toBoolean-out-of-range-number-errors",
        [("out", opt(b()))],
        [(
            "out",
            ValueExpr::cast(ValueExpr::literal(json!(2)), Cast::ToBoolean)
        )],
        []
    );
    row!(
        "cast-toBoolean-null-removes-key",
        [("ghost", opt(b())), ("out", opt(b()))],
        [(
            "out",
            ValueExpr::cast(ValueExpr::field("ghost"), Cast::ToBoolean)
        )],
        []
    );

    // -- Now -------------------------------------------------------------------
    row_now!(
        "now-stamps-epoch-ms",
        [("ts", opt(n()))],
        [("ts", ValueExpr::now())],
        [],
        1_700_000_000_000_i64
    );

    // -- Case: predicate arms + fall-through -----------------------------------
    row!(
        "case-first-matching-when-wins",
        [("status", s()), ("tier", opt(s()))],
        [(
            "tier",
            ValueExpr::case(
                vec![case_when(
                    FilterExpr::Eq {
                        field: "status".into(),
                        value: json!("vip")
                    },
                    ValueExpr::literal(json!("gold"))
                )],
                ValueExpr::literal(json!("standard")),
            )
        )],
        [("status", json!("vip"))]
    );
    row!(
        "case-second-when-matches",
        [("status", s()), ("tier", opt(s()))],
        [(
            "tier",
            ValueExpr::case(
                vec![
                    case_when(
                        FilterExpr::Eq {
                            field: "status".into(),
                            value: json!("vip")
                        },
                        ValueExpr::literal(json!("gold"))
                    ),
                    case_when(
                        FilterExpr::Eq {
                            field: "status".into(),
                            value: json!("regular")
                        },
                        ValueExpr::literal(json!("silver"))
                    ),
                ],
                ValueExpr::literal(json!("standard")),
            )
        )],
        [("status", json!("regular"))]
    );
    row!(
        "case-falls-through-to-otherwise",
        [("status", s()), ("tier", opt(s()))],
        [(
            "tier",
            ValueExpr::case(
                vec![case_when(
                    FilterExpr::Eq {
                        field: "status".into(),
                        value: json!("vip")
                    },
                    ValueExpr::literal(json!("gold"))
                )],
                ValueExpr::literal(json!("standard")),
            )
        )],
        [("status", json!("other"))]
    );
    row!(
        "case-then-branch-arithmetic",
        [("base", n()), ("adj", opt(n()))],
        [(
            "adj",
            ValueExpr::case(
                vec![case_when(
                    FilterExpr::Gt {
                        field: "base".into(),
                        value: json!(10)
                    },
                    ValueExpr::add(ValueExpr::field("base"), ValueExpr::literal(json!(100)))
                )],
                ValueExpr::field("base"),
            )
        )],
        [("base", json!(50))]
    );
    row!(
        "case-predicate-contains-arm",
        [
            (
                "tags",
                opt(FieldType::Array {
                    element: Box::new(s())
                })
            ),
            ("status", s()),
            ("score", n()),
            ("label", opt(s()))
        ],
        [(
            "label",
            ValueExpr::case(
                vec![
                    case_when(
                        FilterExpr::Contains {
                            field: "tags".into(),
                            value: json!("vip")
                        },
                        ValueExpr::literal(json!("contains-arm"))
                    ),
                    case_when(
                        FilterExpr::In {
                            field: "status".into(),
                            values: vec![json!("x"), json!("y")]
                        },
                        ValueExpr::literal(json!("in-arm"))
                    ),
                    case_when(
                        FilterExpr::Lte {
                            field: "score".into(),
                            value: json!(5)
                        },
                        ValueExpr::literal(json!("lte-arm"))
                    ),
                ],
                ValueExpr::literal(json!("none")),
            )
        )],
        [
            ("tags", json!(["a", "vip"])),
            ("status", json!("z")),
            ("score", json!(3))
        ]
    );
    row!(
        "case-predicate-in-arm",
        [
            (
                "tags",
                opt(FieldType::Array {
                    element: Box::new(s())
                })
            ),
            ("status", s()),
            ("score", n()),
            ("label", opt(s()))
        ],
        [(
            "label",
            ValueExpr::case(
                vec![
                    case_when(
                        FilterExpr::Contains {
                            field: "tags".into(),
                            value: json!("vip")
                        },
                        ValueExpr::literal(json!("contains-arm"))
                    ),
                    case_when(
                        FilterExpr::In {
                            field: "status".into(),
                            values: vec![json!("x"), json!("y")]
                        },
                        ValueExpr::literal(json!("in-arm"))
                    ),
                    case_when(
                        FilterExpr::Lte {
                            field: "score".into(),
                            value: json!(5)
                        },
                        ValueExpr::literal(json!("lte-arm"))
                    ),
                ],
                ValueExpr::literal(json!("none")),
            )
        )],
        [
            ("tags", json!(["a"])),
            ("status", json!("y")),
            ("score", json!(9))
        ]
    );
    row!(
        "case-predicate-lte-arm",
        [
            (
                "tags",
                opt(FieldType::Array {
                    element: Box::new(s())
                }),
            ),
            ("status", s()),
            ("score", n()),
            ("label", opt(s()))
        ],
        [(
            "label",
            ValueExpr::case(
                vec![
                    case_when(
                        FilterExpr::Contains {
                            field: "tags".into(),
                            value: json!("vip")
                        },
                        ValueExpr::literal(json!("contains-arm"))
                    ),
                    case_when(
                        FilterExpr::In {
                            field: "status".into(),
                            values: vec![json!("x"), json!("y")]
                        },
                        ValueExpr::literal(json!("in-arm"))
                    ),
                    case_when(
                        FilterExpr::Lte {
                            field: "score".into(),
                            value: json!(5)
                        },
                        ValueExpr::literal(json!("lte-arm"))
                    ),
                ],
                ValueExpr::literal(json!("none")),
            )
        )],
        [
            ("tags", json!([])),
            ("status", json!("z")),
            ("score", json!(3))
        ]
    );
    row!(
        "case-predicate-all-false-uses-otherwise",
        [
            (
                "tags",
                opt(FieldType::Array {
                    element: Box::new(s())
                })
            ),
            ("status", s()),
            ("score", n()),
            ("label", opt(s()))
        ],
        [(
            "label",
            ValueExpr::case(
                vec![
                    case_when(
                        FilterExpr::Contains {
                            field: "tags".into(),
                            value: json!("vip")
                        },
                        ValueExpr::literal(json!("contains-arm"))
                    ),
                    case_when(
                        FilterExpr::In {
                            field: "status".into(),
                            values: vec![json!("x"), json!("y")]
                        },
                        ValueExpr::literal(json!("in-arm"))
                    ),
                    case_when(
                        FilterExpr::Lte {
                            field: "score".into(),
                            value: json!(5)
                        },
                        ValueExpr::literal(json!("lte-arm"))
                    ),
                ],
                ValueExpr::literal(json!("none")),
            )
        )],
        [
            ("tags", json!([])),
            ("status", json!("z")),
            ("score", json!(9))
        ]
    );

    // -- Stamping-layer rules ---------------------------------------------------
    row!(
        "overwrite-client-supplied-computed-value",
        [("a", n()), ("bb", n()), ("total", opt(n()))],
        [(
            "total",
            ValueExpr::add(ValueExpr::field("a"), ValueExpr::field("bb"))
        )],
        [("a", json!(2)), ("bb", json!(3)), ("total", json!(999))]
    );
    row!(
        "multiple-computed-entries-stamp-independently",
        [
            ("a", n()),
            ("bb", s()),
            ("double", opt(n())),
            ("shout", opt(s()))
        ],
        [
            (
                "double",
                ValueExpr::mul(ValueExpr::field("a"), ValueExpr::literal(json!(2)))
            ),
            ("shout", ValueExpr::upper(ValueExpr::field("bb"))),
        ],
        [("a", json!(21)), ("bb", json!("hi"))]
    );

    // The drift net: every row's expressions must walk the full grammar so a
    // node added without a representative row fails to compile.
    for r in &rows {
        for expr in r.computed.values() {
            expr_guard(expr);
        }
    }

    rows
}

/// One fixture row per representative: the wire schema fragment (`fields` +
/// `computed`, serde's own serialization — the same spelling every client
/// parses for pushSchema), the `inputDoc`, and the stamp's outcome —
/// `{"stampedValues": {...}}` where a JSON-null entry means the key must be
/// ABSENT after stamping, or `{"error": true}`.
fn computed_spec_fixture_rows() -> Result<Vec<Value>, serde_json::Error> {
    let mut rows = Vec::new();
    for row in computed_spec_rows() {
        let fields = serde_json::to_value(&row.fields)?;
        let computed = serde_json::to_value(&row.computed)?;
        let table = TableDef {
            fields: row.fields.clone(),
            computed: row.computed.clone(),
            ..Default::default()
        };
        let expect = match stamp_computed(&table, row.input_doc.clone(), row.now) {
            Ok(stamped) => {
                let mut stamped_values = serde_json::Map::new();
                for name in row.computed.keys() {
                    stamped_values.insert(
                        name.clone(),
                        stamped.get(name).cloned().unwrap_or(Value::Null),
                    );
                }
                json!({ "stampedValues": Value::Object(stamped_values) })
            }
            Err(_) => json!({ "error": true }),
        };
        rows.push(json!({
            "id": row.id,
            "fields": fields,
            "computed": computed,
            "inputDoc": Value::Object(row.input_doc),
            "now": row.now,
            "expect": expect,
        }));
    }
    Ok(rows)
}

/// Render the committed fixture's exact bytes (pretty JSON + trailing
/// newline).
pub fn render_computed_stamping_spec() -> Result<String, serde_json::Error> {
    let doc = json!({
        "$comment": "SPEC-TABLE CORPUS (ENH-052 family 2) — GENERATED, do not edit. \
                     The computed-field stamping decision table (ENH-028), one row per \
                     representative per ValueExpr grammar node, Cast target, and \
                     stamping-layer rule, computed from \
                     server/src/txn/stamp.rs::stamp_computed + \
                     server/src/value_expr.rs::eval_value_expr by \
                     'make gen-spec-computed-stamping'. Each row carries the wire schema \
                     fragment (`fields` + `computed`) and an `inputDoc`; \
                     `expect.stampedValues` maps every computed field to its stamped \
                     output, where a JSON null means the key must be ABSENT after \
                     stamping, and `expect.error` marks rows whose stamp must fail \
                     (BAD_REQUEST). Each runner decodes the fragment with its own \
                     schema parser and asserts its own stampComputed port against every \
                     row. Two rows the JS number model cannot express are excluded (see \
                     wire-corpus/README.md): cast-toInt64 of a float-spelled integer \
                     payload ('3.0') and the text form of a float-spelled integral \
                     number ('5.0') — JSON.parse erases both distinctions. A semantics \
                     change regenerates this file in the same commit — see \
                     wire-corpus/README.md 'Spec-table corpus'.",
        "rows": computed_spec_fixture_rows()?,
    });
    let mut out = serde_json::to_string_pretty(&doc)?;
    out.push('\n');
    Ok(out)
}

#[cfg(test)]
mod tests {
    /// The server IS the source: the committed fixture must equal a fresh
    /// generation. A mapping or variant change that skips
    /// `make gen-spec-computed-stamping` fails here.
    #[test]
    fn committed_computed_stamping_fixture_matches_generation() {
        let committed = include_str!("../../wire-corpus/spec/computed-stamping.json");
        let generated = super::render_computed_stamping_spec().expect("render spec fixture");
        assert_eq!(
            committed, generated,
            "wire-corpus/spec/computed-stamping.json is stale — run \
             'make gen-spec-computed-stamping' and commit the result"
        );
    }
}
