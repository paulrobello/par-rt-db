//! The closed `ValueExpr` grammar — the typed, injection-safe expression
//! language shared by migrate's `Directive::EvalExpr` backfill (ENH-020) and
//! computed fields (ENH-028). One wire shape, two executions: the SQL compiler
//! (`compile_value_expr`) for the one-shot migrate UPDATE, and the in-memory
//! interpreter (`eval_value_expr`) for per-write stamping. The interpreter's
//! semantics are pinned by the computed-fields plan's "ValueExpr interpreter
//! semantics" table, which is authoritative for this server and all four
//! client engines.
use crate::auth::PrincipalCtx;
use crate::error::RtDbError;
use crate::migrate::MigrateBind;
use crate::query::FilterExpr;
use crate::schema::TableDef;

/// Closed set of sound coercions. Shared by `Directive::ChangeType` and
/// `ValueExpr::Cast` — the four scalar casts that are sound to backfill.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Cast {
    ToString,
    ToNumber,
    ToInt64,
    ToBoolean,
}

/// A closed, typed expression grammar shared by `Directive::EvalExpr`'s
/// backfill expression (ENH-020 Stage 1, closing SEC-107) and computed fields
/// (ENH-028). Mirrors `query::FilterExpr`'s serde conventions: `tag = "op"`,
/// camelCase, `deny_unknown_fields`. Every `Literal` compiles to a bound `$n`
/// placeholder (as jsonb); every `Field` resolves through the table's
/// `TableDef` (errors on an unknown field) and reads `doc->'field'`. There is
/// deliberately **no** subquery node, no function-call-by-name node, and no
/// raw-SQL escape — the grammar is closed, so the SEC-107 injection concern
/// cannot arise from a `ValueExpr` payload. The only way to reach raw SQL is
/// the deprecated `Legacy(String)` source, which remains gated to the root
/// admin_key (see `admin_migrate`).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", deny_unknown_fields)]
pub enum ValueExpr {
    /// A declared field on this table (validated against `TableDef`). Reads
    /// `doc->'field'` (jsonb). The field must be declared; the write target
    /// (`EvalExpr.set`) need not be.
    Field {
        field: String,
    },
    /// Any JSON literal. Bound as `$n::jsonb`, so objects/arrays/null round-trip.
    Literal {
        value: serde_json::Value,
    },
    /// String concatenation. Postgres `concat(...)`, which ignores NULL args
    /// (treats them as empty) — wrap operands in `Coalesce` for explicit control.
    Concat {
        parts: Vec<ValueExpr>,
    },
    /// Numeric arithmetic. Operands are cast to `::numeric`; the result is a
    /// JSON number via the surrounding `to_jsonb`. Division by zero errors at
    /// runtime — guard with `Case`/`Coalesce` when the divisor may be zero.
    Add {
        left: Box<ValueExpr>,
        right: Box<ValueExpr>,
    },
    Sub {
        left: Box<ValueExpr>,
        right: Box<ValueExpr>,
    },
    Mul {
        left: Box<ValueExpr>,
        right: Box<ValueExpr>,
    },
    Div {
        left: Box<ValueExpr>,
        right: Box<ValueExpr>,
    },
    /// `COALESCE(parts...)` — first non-null, or NULL.
    Coalesce {
        parts: Vec<ValueExpr>,
    },
    /// Text casing / trim. Operand cast to `::text`.
    Lower {
        value: Box<ValueExpr>,
    },
    Upper {
        value: Box<ValueExpr>,
    },
    Trim {
        value: Box<ValueExpr>,
    },
    /// A closed scalar coercion. Reuses `Directive::ChangeType`'s `Cast` enum.
    Cast {
        value: Box<ValueExpr>,
        to: Cast,
    },
    /// Current timestamp as epoch milliseconds (a JSON number) — the same
    /// value `txn`'s `now_ms()` stamps, on both the SQL and interpreter paths.
    Now,
    /// Conditional: first matching `when`'s `then`, else `otherwise`. Each
    /// `when` is a `FilterExpr` (compiled via the read path's `compile_filter`,
    /// so its field references are schema-validated and its values bound).
    Case {
        whens: Vec<CaseWhen>,
        otherwise: Box<ValueExpr>,
    },
}

/// One branch of `ValueExpr::Case`. Wire shape `{ when, then }`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CaseWhen {
    pub when: crate::query::FilterExpr,
    pub then: ValueExpr,
}

/// Compiles a `ValueExpr` into a SQL fragment plus its typed binds, with `$n`
/// placeholders numbered from 1-based `start_pos`. Every `Literal` emits one
/// bind (as jsonb); every `Field` inlines `doc->'field'` (the field name is
/// schema-validated by `validate_value_expr_fields` and a safe jsonb key). The
/// result is intended for `to_jsonb((<expr>))`, so each branch yields a value
/// compatible with jsonb coercion. `Case` branches reuse the read path's
/// `query::compile_filter` for their predicates — no forked compiler, so the
/// SEC-117 three-valued-logic guards (COALESCE-wrapped negation, etc.) apply.
/// There is no raw-SQL node — the grammar is closed, which is the SEC-107
/// boundary.
pub(crate) fn compile_value_expr(
    ve: &ValueExpr,
    table: &TableDef,
    start_pos: usize,
    binds: &mut Vec<MigrateBind>,
) -> Result<String, RtDbError> {
    Ok(match ve {
        ValueExpr::Field { field } => {
            // Text extraction — the expr result feeds `to_jsonb((EXPR))`, so a
            // field yields text (mirrors the legacy `doc->>'field'` reads). The
            // field name is schema-validated by `validate_value_expr_fields`.
            format!("doc->>'{field}'")
        }
        ValueExpr::Literal { value } => {
            // Placeholder numbering is `start_pos + binds.len()` — `start_pos`
            // is the base for THIS compilation, and `binds.len()` is the offset
            // accumulated by earlier siblings (the vec is shared across the
            // recursion, mirroring `compile_filter_node`). Recursive calls pass
            // `start_pos` unchanged so the offset is not double-counted.
            let ph = start_pos + binds.len();
            match value {
                serde_json::Value::String(s) => {
                    binds.push(MigrateBind::Text(s.clone()));
                    format!("${ph}::text")
                }
                serde_json::Value::Number(n) => {
                    binds.push(MigrateBind::Num(n.as_f64().unwrap_or(0.0)));
                    format!("${ph}::numeric")
                }
                serde_json::Value::Bool(b) => {
                    binds.push(MigrateBind::Bool(*b));
                    format!("${ph}::boolean")
                }
                serde_json::Value::Null => "NULL".to_string(),
                other => {
                    binds.push(MigrateBind::Json(other.clone()));
                    format!("${ph}::jsonb")
                }
            }
        }
        ValueExpr::Concat { parts } => {
            let compiled: Vec<String> = parts
                .iter()
                .map(|p| compile_value_expr(p, table, start_pos, binds))
                .collect::<Result<_, _>>()?;
            format!("concat({})", compiled.join(", "))
        }
        ValueExpr::Add { left, right }
        | ValueExpr::Sub { left, right }
        | ValueExpr::Mul { left, right }
        | ValueExpr::Div { left, right } => {
            let op = match ve {
                ValueExpr::Add { .. } => "+",
                ValueExpr::Sub { .. } => "-",
                ValueExpr::Mul { .. } => "*",
                ValueExpr::Div { .. } => "/",
                _ => unreachable!(),
            };
            let l = compile_value_expr(left, table, start_pos, binds)?;
            let r = compile_value_expr(right, table, start_pos, binds)?;
            format!("(({})::numeric {op} ({}))::numeric", l, r)
        }
        ValueExpr::Coalesce { parts } => {
            let compiled: Vec<String> = parts
                .iter()
                .map(|p| compile_value_expr(p, table, start_pos, binds))
                .collect::<Result<_, _>>()?;
            format!("COALESCE({})", compiled.join(", "))
        }
        ValueExpr::Lower { value } => {
            format!(
                "lower(({})::text)",
                compile_value_expr(value, table, start_pos, binds)?
            )
        }
        ValueExpr::Upper { value } => {
            format!(
                "upper(({})::text)",
                compile_value_expr(value, table, start_pos, binds)?
            )
        }
        ValueExpr::Trim { value } => {
            format!(
                "btrim(({})::text)",
                compile_value_expr(value, table, start_pos, binds)?
            )
        }
        ValueExpr::Cast { value, to } => {
            let cast_sql = match to {
                Cast::ToString => "::text",
                Cast::ToNumber => "::numeric",
                Cast::ToInt64 => "::bigint",
                Cast::ToBoolean => "::boolean",
            };
            format!(
                "({}){}",
                compile_value_expr(value, table, start_pos, binds)?,
                cast_sql
            )
        }
        // Epoch-ms, matching the interpreter's `now_ms` — a bare `now()`
        // here would yield a timestamptz that `to_jsonb` renders as an ISO
        // string, desynchronizing the one-shot path from per-write stamping.
        ValueExpr::Now => "((extract(epoch from now()) * 1000)::bigint)".to_string(),
        ValueExpr::Case { whens, otherwise } => {
            let mut fragments: Vec<String> = Vec::with_capacity(whens.len() + 1);
            for cw in whens {
                // Compile the predicate from the current tail (`start_pos +
                // binds.len()`), then push its binds before compiling `then` so
                // the then-expression numbers after them. `start_pos` is passed
                // unchanged to the recursive `then`/`otherwise` calls — the
                // shared `binds.len()` tracks the running offset.
                let cur = start_pos + binds.len();
                let (cond_sql, cond_binds) =
                    crate::query::compile_filter(&cw.when, table, cur, false)?;
                binds.extend(cond_binds.into_iter().map(Into::into));
                let then_sql = compile_value_expr(&cw.then, table, start_pos, binds)?;
                fragments.push(format!("WHEN {cond_sql} THEN {then_sql}"));
            }
            let else_sql = compile_value_expr(otherwise, table, start_pos, binds)?;
            fragments.push(format!("ELSE {else_sql}"));
            format!("CASE {} END", fragments.join(" "))
        }
    })
}

/// In-memory interpreter for `ValueExpr` — the per-write counterpart of
/// `compile_value_expr`'s SQL, used by computed-field stamping. Field reads are
/// text extraction (mirroring `doc->>'field'`), arithmetic is IEEE doubles
/// with SQL-NULL propagation, and a non-finite result is an error. `Case`
/// predicates reuse `dsl::filter_matches`; `ctx` exists for that arm only, and
/// push validation rejects principal markers inside computed exprs, so a
/// bypass ctx is semantically equivalent.
pub fn eval_value_expr(
    ve: &ValueExpr,
    doc: &serde_json::Map<String, serde_json::Value>,
    now_ms: i64,
    ctx: &PrincipalCtx,
) -> Result<serde_json::Value, RtDbError> {
    match ve {
        ValueExpr::Field { field } => Ok(match doc.get(field).and_then(to_text) {
            Some(text) => serde_json::Value::String(text),
            None => serde_json::Value::Null,
        }),
        ValueExpr::Literal { value } => Ok(value.clone()),
        ValueExpr::Concat { parts } => {
            let mut out = String::new();
            for p in parts {
                // to_text is None exactly for null parts — Postgres concat()
                // skips them rather than nulling the result.
                if let Some(text) = to_text(&eval_value_expr(p, doc, now_ms, ctx)?) {
                    out.push_str(&text);
                }
            }
            Ok(serde_json::Value::String(out))
        }
        ValueExpr::Add { left, right }
        | ValueExpr::Sub { left, right }
        | ValueExpr::Mul { left, right }
        | ValueExpr::Div { left, right } => {
            let l = to_numeric(&eval_value_expr(left, doc, now_ms, ctx)?)?;
            let r = to_numeric(&eval_value_expr(right, doc, now_ms, ctx)?)?;
            match (l, r) {
                (Some(l), Some(r)) => {
                    // `r == 0.0` is true for -0.0 too (IEEE equality), so both
                    // zero spellings are the same divisor error.
                    if matches!(ve, ValueExpr::Div { .. }) && r == 0.0 {
                        return Err(RtDbError::bad_request("division by zero"));
                    }
                    let x = match ve {
                        ValueExpr::Add { .. } => l + r,
                        ValueExpr::Sub { .. } => l - r,
                        ValueExpr::Mul { .. } => l * r,
                        _ => l / r,
                    };
                    finite_number(x)
                }
                // Either operand SQL-NULL → NULL; propagation precedes the
                // zero-divisor and finiteness checks (null / 0 is null).
                _ => Ok(serde_json::Value::Null),
            }
        }
        ValueExpr::Coalesce { parts } => {
            for p in parts {
                let v = eval_value_expr(p, doc, now_ms, ctx)?;
                if !v.is_null() {
                    return Ok(v);
                }
            }
            Ok(serde_json::Value::Null)
        }
        ValueExpr::Lower { value } => {
            Ok(match to_text(&eval_value_expr(value, doc, now_ms, ctx)?) {
                Some(text) => serde_json::Value::String(text.to_lowercase()),
                None => serde_json::Value::Null,
            })
        }
        ValueExpr::Upper { value } => {
            Ok(match to_text(&eval_value_expr(value, doc, now_ms, ctx)?) {
                Some(text) => serde_json::Value::String(text.to_uppercase()),
                None => serde_json::Value::Null,
            })
        }
        ValueExpr::Trim { value } => {
            Ok(match to_text(&eval_value_expr(value, doc, now_ms, ctx)?) {
                // Spaces only — Postgres btrim's default, not Unicode whitespace:
                // a leading tab survives.
                Some(text) => serde_json::Value::String(text.trim_matches(' ').to_string()),
                None => serde_json::Value::Null,
            })
        }
        ValueExpr::Cast { value, to } => {
            let v = eval_value_expr(value, doc, now_ms, ctx)?;
            match to {
                Cast::ToString => Ok(match to_text(&v) {
                    Some(text) => serde_json::Value::String(text),
                    None => serde_json::Value::Null,
                }),
                Cast::ToNumber => match to_numeric(&v)? {
                    Some(x) => finite_number(x),
                    None => Ok(serde_json::Value::Null),
                },
                Cast::ToInt64 => cast_to_int64(&v),
                Cast::ToBoolean => cast_to_boolean(&v),
            }
        }
        ValueExpr::Now => Ok(serde_json::Value::from(now_ms)),
        ValueExpr::Case { whens, otherwise } => {
            // filter_matches takes a whole-document Value; the clone is paid
            // only on the Case arm — the no-Case hot path never builds one.
            let doc_value = serde_json::Value::Object(doc.clone());
            for cw in whens {
                if crate::dsl::filter_matches(&doc_value, &cw.when, ctx) {
                    return eval_value_expr(&cw.then, doc, now_ms, ctx);
                }
            }
            eval_value_expr(otherwise, doc, now_ms, ctx)
        }
    }
}

/// JSON value → text, mirroring the SQL `doc->>'field'` extraction the compile
/// path emits. `None` means SQL NULL (JSON `null`) — only `Value::Null` maps to
/// `None`. Numbers use their JSON number text form; objects/arrays use compact
/// JSON text (`{"a":1}` — deliberately not Postgres's spaced jsonb text; the
/// semantics table pins this convention for all five implementations).
fn to_text(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        other => serde_json::to_string(other).ok(),
    }
}

/// JSON value → f64 for the arithmetic nodes. `Ok(None)` means SQL NULL (JSON
/// `null` — propagation, not an error). Numbers yield their f64; strings are
/// trimmed and strictly parsed (the whole string must be the number);
/// bool/object/array are type errors.
fn to_numeric(v: &serde_json::Value) -> Result<Option<f64>, RtDbError> {
    match v {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::Number(n) => n
            .as_f64()
            .map(Some)
            .ok_or_else(|| RtDbError::bad_request("cannot cast to number")),
        serde_json::Value::String(s) => s
            .trim()
            .parse::<f64>()
            .map(Some)
            .map_err(|_| RtDbError::bad_request(format!("cannot cast {s:?} to number"))),
        serde_json::Value::Bool(_) | serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
            Err(RtDbError::bad_request("cannot cast to number"))
        }
    }
}

/// IEEE double → JSON number. `Number::from_f64` is `None` exactly for
/// non-finite results (NaN, ±inf — overflow-shaped arithmetic), which the
/// semantics table makes an error rather than a stored value.
fn finite_number(x: f64) -> Result<serde_json::Value, RtDbError> {
    serde_json::Number::from_f64(x)
        .map(serde_json::Value::Number)
        .ok_or_else(|| RtDbError::bad_request("numeric result is not finite"))
}

/// `Cast:ToInt64` — a Number must be integral per `as_i64` (a float payload
/// like `3.0` is not), a String is trimmed and strictly parsed. The result is
/// a JSON number; the int64 *string* wire convention applies only to stored
/// int64 fields (the plan's "Int64 note").
fn cast_to_int64(v: &serde_json::Value) -> Result<serde_json::Value, RtDbError> {
    match v {
        serde_json::Value::Null => Ok(serde_json::Value::Null),
        serde_json::Value::Number(n) => n
            .as_i64()
            .map(serde_json::Value::from)
            .ok_or_else(|| RtDbError::bad_request(format!("cannot cast {n} to int64"))),
        serde_json::Value::String(s) => s
            .trim()
            .parse::<i64>()
            .map(serde_json::Value::from)
            .map_err(|_| RtDbError::bad_request(format!("cannot cast {s:?} to int64"))),
        serde_json::Value::Bool(_) | serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
            Err(RtDbError::bad_request("cannot cast to int64"))
        }
    }
}

/// `Cast::ToBoolean` — bools pass through; numbers accept exactly `1`/`0`
/// (numeric equality, so `1.0`/`0.0` agree with the JS/Python engines);
/// strings match case-insensitively against Postgres's boolean literal set.
fn cast_to_boolean(v: &serde_json::Value) -> Result<serde_json::Value, RtDbError> {
    const TRUE_WORDS: [&str; 5] = ["true", "t", "yes", "on", "1"];
    const FALSE_WORDS: [&str; 5] = ["false", "f", "no", "off", "0"];
    match v {
        serde_json::Value::Null => Ok(serde_json::Value::Null),
        serde_json::Value::Bool(b) => Ok(serde_json::Value::Bool(*b)),
        serde_json::Value::Number(n) => match n.as_f64() {
            Some(1.0) => Ok(serde_json::Value::Bool(true)),
            Some(0.0) => Ok(serde_json::Value::Bool(false)),
            _ => Err(RtDbError::bad_request(format!(
                "cannot cast {n} to boolean"
            ))),
        },
        serde_json::Value::String(s) => {
            if TRUE_WORDS.iter().any(|w| s.eq_ignore_ascii_case(w)) {
                Ok(serde_json::Value::Bool(true))
            } else if FALSE_WORDS.iter().any(|w| s.eq_ignore_ascii_case(w)) {
                Ok(serde_json::Value::Bool(false))
            } else {
                Err(RtDbError::bad_request(format!(
                    "cannot cast {s:?} to boolean"
                )))
            }
        }
        serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
            Err(RtDbError::bad_request("cannot cast to boolean"))
        }
    }
}

/// Visits every field name a `ValueExpr` reads: each `Field` node, every
/// `Case` branch's `then`/`otherwise`, and every `FilterExpr` field inside
/// `Case.whens` — the same field set `validate_value_expr_fields` type-checks,
/// exposed as a callback walk for push-time validation and backfill planning.
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
/// variant carries `field: String` (same shape as the query path's
/// `collect_unindexed_filter_fields`).
pub(crate) fn walk_filter_expr_fields(expr: &FilterExpr, f: &mut impl FnMut(&str)) {
    match expr {
        FilterExpr::Eq { field, .. }
        | FilterExpr::Neq { field, .. }
        | FilterExpr::Gt { field, .. }
        | FilterExpr::Gte { field, .. }
        | FilterExpr::Lt { field, .. }
        | FilterExpr::Lte { field, .. }
        | FilterExpr::In { field, .. }
        | FilterExpr::Contains { field, .. }
        | FilterExpr::Exists { field }
        | FilterExpr::OlderThan { field, .. } => f(field),
        FilterExpr::And { exprs } | FilterExpr::Or { exprs } => {
            for e in exprs {
                walk_filter_expr_fields(e, f);
            }
        }
        FilterExpr::Not { expr } => walk_filter_expr_fields(expr, f),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(pairs: &[(&str, serde_json::Value)]) -> serde_json::Map<String, serde_json::Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn eval(
        ve: &ValueExpr,
        doc: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<serde_json::Value, RtDbError> {
        // Markers are push-rejected inside computed exprs, so the bypass ctx
        // every machine-principal path uses is semantically equivalent here.
        eval_value_expr(ve, doc, 0, &PrincipalCtx::bypass())
    }

    fn field(name: &str) -> ValueExpr {
        ValueExpr::Field { field: name.into() }
    }

    #[test]
    fn field_reads_are_text_and_absent_is_null() {
        let d = doc(&[
            ("s", serde_json::json!("x")),
            ("n", serde_json::json!(42)),
            ("f", serde_json::json!(42.5)),
            ("b", serde_json::json!(true)),
            ("o", serde_json::json!({"a": 1})),
            ("nil", serde_json::json!(null)),
        ]);
        assert_eq!(eval(&field("s"), &d).unwrap(), serde_json::json!("x"));
        assert_eq!(eval(&field("n"), &d).unwrap(), serde_json::json!("42"));
        assert_eq!(eval(&field("f"), &d).unwrap(), serde_json::json!("42.5"));
        assert_eq!(eval(&field("b"), &d).unwrap(), serde_json::json!("true"));
        assert_eq!(
            eval(&field("o"), &d).unwrap(),
            serde_json::json!("{\"a\":1}")
        );
        assert_eq!(eval(&field("nil"), &d).unwrap(), serde_json::json!(null));
        assert_eq!(
            eval(&field("missing"), &d).unwrap(),
            serde_json::json!(null)
        );
    }

    #[test]
    fn literal_passes_through() {
        for v in [
            serde_json::json!("s"),
            serde_json::json!(42),
            serde_json::json!(42.5),
            serde_json::json!(true),
            serde_json::json!({"a": [1, 2]}),
            serde_json::json!(null),
        ] {
            assert_eq!(
                eval(&ValueExpr::Literal { value: v.clone() }, &doc(&[])).unwrap(),
                v
            );
        }
    }

    #[test]
    fn concat_skips_nulls_and_casts_numbers_to_text() {
        let mut d = serde_json::Map::new();
        d.insert("first".into(), serde_json::json!("Ada"));
        d.insert("n".into(), serde_json::json!(42));
        let expr = ValueExpr::Concat {
            parts: vec![
                ValueExpr::Field {
                    field: "first".into(),
                },
                ValueExpr::Field {
                    field: "missing".into(),
                },
                ValueExpr::Field { field: "n".into() },
            ],
        };
        assert_eq!(
            eval_value_expr(&expr, &d, 0, &PrincipalCtx::bypass()).unwrap(),
            serde_json::json!("Ada42")
        );
    }

    #[test]
    fn concat_all_null_parts_is_empty_string() {
        let expr = ValueExpr::Concat {
            parts: vec![
                field("missing"),
                ValueExpr::Literal {
                    value: serde_json::json!(null),
                },
            ],
        };
        assert_eq!(eval(&expr, &doc(&[])).unwrap(), serde_json::json!(""));
    }

    #[test]
    fn add_coerces_string_fields_to_numeric() {
        // Arithmetic runs on f64, so the result is a float JSON number (43.0,
        // not 43) — serde_json numbers are representation-sensitive.
        let d = doc(&[
            ("a", serde_json::json!("42")),
            ("b", serde_json::json!("1")),
        ]);
        let expr = ValueExpr::Add {
            left: Box::new(field("a")),
            right: Box::new(field("b")),
        };
        assert_eq!(eval(&expr, &d).unwrap(), serde_json::json!(43.0));
    }

    #[test]
    fn arithmetic_propagates_null_over_operands() {
        let missing = || field("missing");
        let one = || ValueExpr::Literal {
            value: serde_json::json!(1),
        };
        let exprs = [
            ValueExpr::Add {
                left: Box::new(missing()),
                right: Box::new(one()),
            },
            ValueExpr::Sub {
                left: Box::new(one()),
                right: Box::new(missing()),
            },
            ValueExpr::Mul {
                left: Box::new(missing()),
                right: Box::new(one()),
            },
            ValueExpr::Div {
                left: Box::new(one()),
                right: Box::new(missing()),
            },
        ];
        for e in &exprs {
            assert_eq!(eval(e, &doc(&[])).unwrap(), serde_json::json!(null));
        }
        // Null precedes the zero check: null / 0 is null, not an error.
        let null_div_zero = ValueExpr::Div {
            left: Box::new(missing()),
            right: Box::new(ValueExpr::Literal {
                value: serde_json::json!(0),
            }),
        };
        assert_eq!(
            eval(&null_div_zero, &doc(&[])).unwrap(),
            serde_json::json!(null)
        );
    }

    #[test]
    fn div_by_zero_errors() {
        let expr = ValueExpr::Div {
            left: Box::new(ValueExpr::Literal {
                value: serde_json::json!(1),
            }),
            right: Box::new(ValueExpr::Literal {
                value: serde_json::json!(0),
            }),
        };
        let err = eval(&expr, &doc(&[])).unwrap_err();
        assert_eq!(err.message, "division by zero");

        let neg_zero = ValueExpr::Div {
            left: Box::new(ValueExpr::Literal {
                value: serde_json::json!(1),
            }),
            right: Box::new(ValueExpr::Literal {
                value: serde_json::json!(-0.0),
            }),
        };
        assert_eq!(
            eval(&neg_zero, &doc(&[])).unwrap_err().message,
            "division by zero"
        );
    }

    #[test]
    fn div_non_finite_result_errors() {
        let expr = ValueExpr::Div {
            left: Box::new(ValueExpr::Literal {
                value: serde_json::json!(1e308),
            }),
            right: Box::new(ValueExpr::Literal {
                value: serde_json::json!(1e-10),
            }),
        };
        assert_eq!(
            eval(&expr, &doc(&[])).unwrap_err().message,
            "numeric result is not finite"
        );
    }

    #[test]
    fn div_happy_path_is_f64() {
        let expr = ValueExpr::Div {
            left: Box::new(ValueExpr::Literal {
                value: serde_json::json!(1),
            }),
            right: Box::new(ValueExpr::Literal {
                value: serde_json::json!(4),
            }),
        };
        assert_eq!(eval(&expr, &doc(&[])).unwrap(), serde_json::json!(0.25));
    }

    #[test]
    fn coalesce_returns_first_non_null_else_null() {
        let first_missing = ValueExpr::Coalesce {
            parts: vec![
                field("missing"),
                ValueExpr::Literal {
                    value: serde_json::json!(7),
                },
            ],
        };
        assert_eq!(
            eval(&first_missing, &doc(&[])).unwrap(),
            serde_json::json!(7)
        );

        let all_missing = ValueExpr::Coalesce {
            parts: vec![field("a"), field("b")],
        };
        assert_eq!(
            eval(&all_missing, &doc(&[])).unwrap(),
            serde_json::json!(null)
        );
    }

    #[test]
    fn lower_upper_trim() {
        let d = doc(&[
            ("mixed", serde_json::json!("MiXeD")),
            ("padded", serde_json::json!("  x  ")),
            ("tabbed", serde_json::json!("  \tx  ")),
        ]);
        let lower = ValueExpr::Lower {
            value: Box::new(field("mixed")),
        };
        assert_eq!(eval(&lower, &d).unwrap(), serde_json::json!("mixed"));
        let upper = ValueExpr::Upper {
            value: Box::new(field("mixed")),
        };
        assert_eq!(eval(&upper, &d).unwrap(), serde_json::json!("MIXED"));
        let trim = ValueExpr::Trim {
            value: Box::new(field("padded")),
        };
        assert_eq!(eval(&trim, &d).unwrap(), serde_json::json!("x"));
        // Spaces only — the tab survives btrim's default.
        let tabbed = ValueExpr::Trim {
            value: Box::new(field("tabbed")),
        };
        assert_eq!(eval(&tabbed, &d).unwrap(), serde_json::json!("\tx"));
        let lower_null = ValueExpr::Lower {
            value: Box::new(field("missing")),
        };
        assert_eq!(eval(&lower_null, &d).unwrap(), serde_json::json!(null));
    }

    #[test]
    fn cast_to_string_uses_text_extraction() {
        let d = doc(&[
            ("n", serde_json::json!(42)),
            ("o", serde_json::json!({"a": 1})),
        ]);
        let num = ValueExpr::Cast {
            value: Box::new(field("n")),
            to: Cast::ToString,
        };
        assert_eq!(eval(&num, &d).unwrap(), serde_json::json!("42"));
        let obj = ValueExpr::Cast {
            value: Box::new(field("o")),
            to: Cast::ToString,
        };
        assert_eq!(eval(&obj, &d).unwrap(), serde_json::json!("{\"a\":1}"));
        let null = ValueExpr::Cast {
            value: Box::new(field("missing")),
            to: Cast::ToString,
        };
        assert_eq!(eval(&null, &d).unwrap(), serde_json::json!(null));
    }

    #[test]
    fn cast_to_number_parses_trimmed_strings() {
        let d = doc(&[
            ("s", serde_json::json!("  3.5 ")),
            ("bad", serde_json::json!("abc")),
            ("b", serde_json::json!(true)),
        ]);
        let ok = ValueExpr::Cast {
            value: Box::new(field("s")),
            to: Cast::ToNumber,
        };
        assert_eq!(eval(&ok, &d).unwrap(), serde_json::json!(3.5));
        let bad = ValueExpr::Cast {
            value: Box::new(field("bad")),
            to: Cast::ToNumber,
        };
        assert!(eval(&bad, &d).is_err());
        // A bool FIELD reaches the cast as its text form ("true"), so it fails
        // the string parse; a bool LITERAL hits the type-error arm directly.
        let bool_field_err = ValueExpr::Cast {
            value: Box::new(field("b")),
            to: Cast::ToNumber,
        };
        assert!(eval(&bool_field_err, &d).is_err());
        let bool_literal_err = ValueExpr::Cast {
            value: Box::new(ValueExpr::Literal {
                value: serde_json::json!(true),
            }),
            to: Cast::ToNumber,
        };
        assert_eq!(
            eval(&bool_literal_err, &d).unwrap_err().message,
            "cannot cast to number"
        );
        let null = ValueExpr::Cast {
            value: Box::new(field("missing")),
            to: Cast::ToNumber,
        };
        assert_eq!(eval(&null, &d).unwrap(), serde_json::json!(null));
    }

    #[test]
    fn cast_to_int64_requires_integral_numbers() {
        let d = doc(&[
            ("i", serde_json::json!(42)),
            ("float", serde_json::json!(3.5)),
            ("s", serde_json::json!("  7 ")),
            ("bad", serde_json::json!("8x")),
            ("b", serde_json::json!(true)),
        ]);
        let int = ValueExpr::Cast {
            value: Box::new(field("i")),
            to: Cast::ToInt64,
        };
        assert_eq!(eval(&int, &d).unwrap(), serde_json::json!(42));
        let from_str = ValueExpr::Cast {
            value: Box::new(field("s")),
            to: Cast::ToInt64,
        };
        assert_eq!(eval(&from_str, &d).unwrap(), serde_json::json!(7));
        let float = ValueExpr::Cast {
            value: Box::new(field("float")),
            to: Cast::ToInt64,
        };
        assert!(eval(&float, &d).is_err());
        let bad = ValueExpr::Cast {
            value: Box::new(field("bad")),
            to: Cast::ToInt64,
        };
        assert!(eval(&bad, &d).is_err());
        // Text extraction again: a bool FIELD arrives as "true" (string parse
        // error); a bool LITERAL hits the type-error arm.
        let bool_field_err = ValueExpr::Cast {
            value: Box::new(field("b")),
            to: Cast::ToInt64,
        };
        assert!(eval(&bool_field_err, &d).is_err());
        let bool_literal_err = ValueExpr::Cast {
            value: Box::new(ValueExpr::Literal {
                value: serde_json::json!(true),
            }),
            to: Cast::ToInt64,
        };
        assert_eq!(
            eval(&bool_literal_err, &d).unwrap_err().message,
            "cannot cast to int64"
        );
        let null = ValueExpr::Cast {
            value: Box::new(field("missing")),
            to: Cast::ToInt64,
        };
        assert_eq!(eval(&null, &d).unwrap(), serde_json::json!(null));
    }

    #[test]
    fn cast_to_boolean_accepts_postgres_literal_set() {
        let d = doc(&[
            ("b", serde_json::json!(true)),
            ("two", serde_json::json!(2)),
        ]);
        let pass = ValueExpr::Cast {
            value: Box::new(field("b")),
            to: Cast::ToBoolean,
        };
        assert_eq!(eval(&pass, &d).unwrap(), serde_json::json!(true));

        let one = ValueExpr::Literal {
            value: serde_json::json!(1),
        };
        let zero = ValueExpr::Literal {
            value: serde_json::json!(0),
        };
        for (input, want) in [(one.clone(), true), (zero.clone(), false)] {
            let e = ValueExpr::Cast {
                value: Box::new(input),
                to: Cast::ToBoolean,
            };
            assert_eq!(eval(&e, &d).unwrap(), serde_json::json!(want));
        }
        for (word, want) in [
            ("TRUE", true),
            ("t", true),
            ("Yes", true),
            ("on", true),
            ("1", true),
            ("False", false),
            ("f", false),
            ("No", false),
            ("OFF", false),
            ("0", false),
        ] {
            let e = ValueExpr::Cast {
                value: Box::new(ValueExpr::Literal {
                    value: serde_json::json!(word),
                }),
                to: Cast::ToBoolean,
            };
            assert_eq!(
                eval(&e, &d).unwrap(),
                serde_json::json!(want),
                "word {word}"
            );
        }
        let maybe = ValueExpr::Literal {
            value: serde_json::json!("maybe"),
        };
        let e = ValueExpr::Cast {
            value: Box::new(maybe),
            to: Cast::ToBoolean,
        };
        assert!(eval(&e, &d).is_err());
        let two = ValueExpr::Cast {
            value: Box::new(field("two")),
            to: Cast::ToBoolean,
        };
        assert!(eval(&two, &d).is_err());
        let null = ValueExpr::Cast {
            value: Box::new(field("missing")),
            to: Cast::ToBoolean,
        };
        assert_eq!(eval(&null, &d).unwrap(), serde_json::json!(null));
    }

    #[test]
    fn now_yields_epoch_ms_as_number() {
        assert_eq!(
            eval_value_expr(
                &ValueExpr::Now,
                &doc(&[]),
                1234567890,
                &PrincipalCtx::bypass()
            )
            .unwrap(),
            serde_json::json!(1234567890)
        );
    }

    #[test]
    fn case_takes_first_match_then_otherwise() {
        let d = doc(&[
            ("status", serde_json::json!("admin")),
            ("n", serde_json::json!(5)),
        ]);
        let whens = vec![
            CaseWhen {
                when: FilterExpr::Eq {
                    field: "status".into(),
                    value: serde_json::json!("user"),
                },
                then: ValueExpr::Literal {
                    value: serde_json::json!(1),
                },
            },
            CaseWhen {
                when: FilterExpr::Eq {
                    field: "status".into(),
                    value: serde_json::json!("admin"),
                },
                then: ValueExpr::Literal {
                    value: serde_json::json!(2),
                },
            },
        ];
        let matched = ValueExpr::Case {
            whens: whens.clone(),
            otherwise: Box::new(ValueExpr::Literal {
                value: serde_json::json!(4),
            }),
        };
        assert_eq!(eval(&matched, &d).unwrap(), serde_json::json!(2));

        let unmatched_whens = vec![CaseWhen {
            when: FilterExpr::Gt {
                field: "n".into(),
                value: serde_json::json!(10),
            },
            then: ValueExpr::Literal {
                value: serde_json::json!(3),
            },
        }];
        let otherwise = ValueExpr::Case {
            whens: unmatched_whens,
            otherwise: Box::new(field("status")),
        };
        assert_eq!(eval(&otherwise, &d).unwrap(), serde_json::json!("admin"));
    }

    #[test]
    fn walk_visits_fields_and_case_when_fields() {
        let expr = ValueExpr::Concat {
            parts: vec![
                field("a"),
                ValueExpr::Case {
                    whens: vec![
                        CaseWhen {
                            when: FilterExpr::And {
                                exprs: vec![
                                    FilterExpr::Eq {
                                        field: "b".into(),
                                        value: serde_json::json!(1),
                                    },
                                    FilterExpr::Not {
                                        expr: Box::new(FilterExpr::Contains {
                                            field: "c".into(),
                                            value: serde_json::json!("x"),
                                        }),
                                    },
                                ],
                            },
                            then: field("d"),
                        },
                        CaseWhen {
                            when: FilterExpr::Exists { field: "e".into() },
                            then: ValueExpr::Literal {
                                value: serde_json::json!(1),
                            },
                        },
                    ],
                    otherwise: Box::new(field("f")),
                },
                ValueExpr::Add {
                    left: Box::new(field("g")),
                    right: Box::new(ValueExpr::Div {
                        left: Box::new(field("h")),
                        right: Box::new(ValueExpr::Coalesce {
                            parts: vec![field("i")],
                        }),
                    }),
                },
            ],
        };
        let mut seen: Vec<String> = Vec::new();
        walk_value_expr_fields(&expr, &mut |name| seen.push(name.to_string()));
        seen.sort();
        let want: Vec<String> = ["a", "b", "c", "d", "e", "f", "g", "h", "i"]
            .into_iter()
            .map(String::from)
            .collect();
        assert_eq!(seen, want);
    }
}
