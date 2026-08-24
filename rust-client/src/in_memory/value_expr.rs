//! In-memory `ValueExpr` interpreter (ENH-028) — the engine's per-write
//! counterpart of the server's `eval_value_expr` (`server/src/value_expr.rs`).
//!
//! Field reads are text extraction (mirroring `doc->>'field'`), arithmetic is
//! IEEE doubles with SQL-NULL propagation, and a non-finite result is an
//! error. `Case` predicates reuse the query path's [`eval_filter_expr`];
//! push validation rejects principal markers inside computed expressions, so
//! there is no principal ctx to thread (the server's bypass ctx is
//! semantically irrelevant for the same reason). [`stamp_computed`] wraps the
//! interpreter for the write choke points.

use super::*;
use crate::value_expr::{Cast, ValueExpr};

/// Evaluates `ve` over `doc` — a port of server
/// `value_expr::eval_value_expr` per the computed-fields plan's interpreter
/// semantics table. `now_ms` feeds [`ValueExpr::Now`]; `fields` is the table's
/// declared field map, used only by `Case` branch predicates. Returns the JSON
/// result or a `BAD_REQUEST` error (cast failures, division by zero,
/// non-finite arithmetic) — the caller (`stamp_computed`) names the computed
/// field.
pub fn eval_value_expr(
    ve: &ValueExpr,
    doc: &Map<String, Value>,
    now_ms: i64,
    fields: &BTreeMap<String, FieldType>,
) -> Result<Value, RtDbError> {
    match ve {
        ValueExpr::Field { field } => Ok(match doc.get(field).and_then(to_text) {
            Some(text) => Value::String(text),
            None => Value::Null,
        }),
        ValueExpr::Literal { value } => Ok(value.clone()),
        ValueExpr::Concat { parts } => {
            let mut out = String::new();
            for p in parts {
                // to_text is None exactly for null parts — Postgres concat()
                // skips them rather than nulling the result.
                if let Some(text) = to_text(&eval_value_expr(p, doc, now_ms, fields)?) {
                    out.push_str(&text);
                }
            }
            Ok(Value::String(out))
        }
        ValueExpr::Add { left, right }
        | ValueExpr::Sub { left, right }
        | ValueExpr::Mul { left, right }
        | ValueExpr::Div { left, right } => {
            let l = to_numeric(&eval_value_expr(left, doc, now_ms, fields)?)?;
            let r = to_numeric(&eval_value_expr(right, doc, now_ms, fields)?)?;
            match (l, r) {
                (Some(l), Some(r)) => {
                    // `r == 0.0` is true for -0.0 too (IEEE equality), so both
                    // zero spellings are the same divisor error.
                    if matches!(ve, ValueExpr::Div { .. }) && r == 0.0 {
                        return Err(RtDbError::new(
                            ErrorCode::BadRequest,
                            "division by zero".to_string(),
                        ));
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
                _ => Ok(Value::Null),
            }
        }
        ValueExpr::Coalesce { parts } => {
            for p in parts {
                let v = eval_value_expr(p, doc, now_ms, fields)?;
                if !v.is_null() {
                    return Ok(v);
                }
            }
            Ok(Value::Null)
        }
        ValueExpr::Lower { value } => Ok(
            match to_text(&eval_value_expr(value, doc, now_ms, fields)?) {
                Some(text) => Value::String(text.to_lowercase()),
                None => Value::Null,
            },
        ),
        ValueExpr::Upper { value } => Ok(
            match to_text(&eval_value_expr(value, doc, now_ms, fields)?) {
                Some(text) => Value::String(text.to_uppercase()),
                None => Value::Null,
            },
        ),
        ValueExpr::Trim { value } => {
            Ok(
                match to_text(&eval_value_expr(value, doc, now_ms, fields)?) {
                    // Spaces only — Postgres btrim's default, not Unicode
                    // whitespace: a leading tab survives.
                    Some(text) => Value::String(text.trim_matches(' ').to_string()),
                    None => Value::Null,
                },
            )
        }
        ValueExpr::Cast { value, to } => {
            let v = eval_value_expr(value, doc, now_ms, fields)?;
            match to {
                Cast::ToString => Ok(match to_text(&v) {
                    Some(text) => Value::String(text),
                    None => Value::Null,
                }),
                Cast::ToNumber => match to_numeric(&v)? {
                    Some(x) => finite_number(x),
                    None => Ok(Value::Null),
                },
                Cast::ToInt64 => cast_to_int64(&v),
                Cast::ToBoolean => cast_to_boolean(&v),
            }
        }
        ValueExpr::Now => Ok(Value::from(now_ms)),
        ValueExpr::Case { whens, otherwise } => {
            // eval_filter_expr takes a whole-document Value; the clone is paid
            // only on the Case arm — the no-Case hot path never builds one.
            let doc_value = Value::Object(doc.clone());
            for cw in whens {
                if eval_filter_expr(&cw.when, &doc_value, fields) {
                    return eval_value_expr(&cw.then, doc, now_ms, fields);
                }
            }
            eval_value_expr(otherwise, doc, now_ms, fields)
        }
    }
}

/// Stamps the table's computed fields (ENH-028): every `computed` entry is
/// re-evaluated against the final doc and stored — a null result REMOVES the
/// key (an unset optional field is an absent key, `strip_unset_optionals`'
/// shape convention) and a non-null result overwrites whatever is there (the
/// ownerField authority model: client-supplied values never survive). An
/// evaluation error fails the whole write as `BAD_REQUEST`, naming the field.
/// Mirrors server `txn::stamp_computed` and runs at the same choke points:
/// last in the insert/replace stamp chains and inside `apply_patch` (so patch,
/// upsert's update branch, patchByQuery, and cascade setNull are all covered),
/// always before `validate_doc`.
pub(super) fn stamp_computed(
    table_def: &TableDef,
    mut doc: Map<String, Value>,
    now: i64,
) -> Result<Map<String, Value>, RtDbError> {
    for (name, expr) in &table_def.computed {
        let value = eval_value_expr(expr, &doc, now, &table_def.fields).map_err(|e| {
            RtDbError::new(
                ErrorCode::BadRequest,
                format!("computed field '{name}': {}", e.message),
            )
        })?;
        if value.is_null() {
            doc.remove(name);
        } else {
            doc.insert(name.clone(), value);
        }
    }
    Ok(doc)
}

/// JSON value → text, mirroring the SQL `doc->>'field'` extraction the server
/// compiles to. `None` means SQL NULL (JSON `null`) — only `Value::Null` maps
/// to `None`. Numbers use their JSON number text form; objects/arrays use
/// compact JSON text (`{"a":1}` — the convention the semantics table pins for
/// all five implementations).
fn to_text(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        other => serde_json::to_string(other).ok(),
    }
}

/// JSON value → f64 for the arithmetic nodes. `Ok(None)` means SQL NULL (JSON
/// `null` — propagation, not an error). Numbers yield their f64; strings are
/// trimmed and strictly parsed (the whole string must be the number);
/// bool/object/array are type errors.
fn to_numeric(v: &Value) -> Result<Option<f64>, RtDbError> {
    match v {
        Value::Null => Ok(None),
        Value::Number(n) => n
            .as_f64()
            .map(Some)
            .ok_or_else(|| RtDbError::new(ErrorCode::BadRequest, "cannot cast to number")),
        Value::String(s) => s.trim().parse::<f64>().map(Some).map_err(|_| {
            RtDbError::new(
                ErrorCode::BadRequest,
                format!("cannot cast {s:?} to number"),
            )
        }),
        Value::Bool(_) | Value::Object(_) | Value::Array(_) => Err(RtDbError::new(
            ErrorCode::BadRequest,
            "cannot cast to number".to_string(),
        )),
    }
}

/// IEEE double → JSON number. `Number::from_f64` is `None` exactly for
/// non-finite results (NaN, ±inf — overflow-shaped arithmetic), which the
/// semantics table makes an error rather than a stored value.
fn finite_number(x: f64) -> Result<Value, RtDbError> {
    serde_json::Number::from_f64(x)
        .map(Value::Number)
        .ok_or_else(|| {
            RtDbError::new(
                ErrorCode::BadRequest,
                "numeric result is not finite".to_string(),
            )
        })
}

/// `Cast:ToInt64` — a Number must be integral per `as_i64` (a float payload
/// like `3.0` is not), a String is trimmed and strictly parsed. The result is
/// a JSON number; the int64 *string* wire convention applies only to stored
/// int64 fields (the plan's "Int64 note").
fn cast_to_int64(v: &Value) -> Result<Value, RtDbError> {
    match v {
        Value::Null => Ok(Value::Null),
        Value::Number(n) => n.as_i64().map(Value::from).ok_or_else(|| {
            RtDbError::new(ErrorCode::BadRequest, format!("cannot cast {n} to int64"))
        }),
        Value::String(s) => s.trim().parse::<i64>().map(Value::from).map_err(|_| {
            RtDbError::new(ErrorCode::BadRequest, format!("cannot cast {s:?} to int64"))
        }),
        Value::Bool(_) | Value::Object(_) | Value::Array(_) => Err(RtDbError::new(
            ErrorCode::BadRequest,
            "cannot cast to int64".to_string(),
        )),
    }
}

/// `Cast::ToBoolean` — bools pass through; numbers accept exactly `1`/`0`
/// (numeric equality, so `1.0`/`0.0` agree with the JS/Python engines);
/// strings match case-insensitively against Postgres's boolean literal set.
fn cast_to_boolean(v: &Value) -> Result<Value, RtDbError> {
    const TRUE_WORDS: [&str; 5] = ["true", "t", "yes", "on", "1"];
    const FALSE_WORDS: [&str; 5] = ["false", "f", "no", "off", "0"];
    match v {
        Value::Null => Ok(Value::Null),
        Value::Bool(b) => Ok(Value::Bool(*b)),
        Value::Number(n) => match n.as_f64() {
            Some(1.0) => Ok(Value::Bool(true)),
            Some(0.0) => Ok(Value::Bool(false)),
            _ => Err(RtDbError::new(
                ErrorCode::BadRequest,
                format!("cannot cast {n} to boolean"),
            )),
        },
        Value::String(s) => {
            if TRUE_WORDS.iter().any(|w| s.eq_ignore_ascii_case(w)) {
                Ok(Value::Bool(true))
            } else if FALSE_WORDS.iter().any(|w| s.eq_ignore_ascii_case(w)) {
                Ok(Value::Bool(false))
            } else {
                Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    format!("cannot cast {s:?} to boolean"),
                ))
            }
        }
        Value::Object(_) | Value::Array(_) => Err(RtDbError::new(
            ErrorCode::BadRequest,
            "cannot cast to boolean".to_string(),
        )),
    }
}
