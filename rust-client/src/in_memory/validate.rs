//! Value/document validators, coercion, and `FilterExpr` evaluation.
//!
//! Extracted from `in_memory.rs` (QA-108). Pure file movement. `stamp_ttl_default`
//! and `matches_filter` widen from private to `pub(super)` so the store/executor
//! (in `mod.rs`) and `run_query` (in `query.rs`) can call them.

use std::collections::BTreeMap;

use super::*;

/// Deep clone of a JSON doc. Docs are pure JSON — safe to round-trip — so
/// cloning is just [`Value::clone`]. Named to mirror the TS helper.
pub fn clone_value(value: &Value) -> Value {
    value.clone()
}

/// Canonical string form for change detection, independent of key order.
/// `serde_json` with default features uses a `BTreeMap`-backed `Map`, so
/// [`Value`] already serializes with sorted keys — `to_string` is canonical.
/// If `preserve_order` is ever enabled on the `serde_json` dep, replace this
/// with a key-sorting canonicalizer (same caveat as `optimistic.rs`).
pub fn canonical(value: &Value) -> String {
    value.to_string()
}

/// `true` iff `value` is a 32-char lowercase hex string (an `_id`). Mirrors
/// the TS `/^[0-9a-f]+$/` (lowercase only).
pub fn is_hex_id(value: &Value) -> bool {
    match value.as_str() {
        Some(s) if s.len() == 32 => s
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        _ => false,
    }
}

/// `true` iff `value` is a syntactically-valid integer string within `i64`
/// range (the wire form of an `int64` field). Mirrors the BigInt range check in
/// the TS source.
pub fn is_int64_string(value: &Value) -> bool {
    let s = match value.as_str() {
        Some(s) => s,
        None => return false,
    };
    // Strict `^-?\d+$`: an optional leading '-' then one or more ASCII digits.
    let digits = s.strip_prefix('-').unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    // i64 parse handles both sign and range.
    s.parse::<i64>().is_ok()
}

/// `true` iff `value` is a base64-shaped string: length a multiple of 4, body in
/// `[A-Za-z0-9+/]`, at most two trailing `=`. Mirrors the TS regex
/// `/^[A-Za-z0-9+/]*={0,2}$/`.
pub fn is_base64_string(value: &Value) -> bool {
    let s = match value.as_str() {
        Some(s) => s,
        None => return false,
    };
    if s.len() % 4 != 0 {
        return false;
    }
    let bytes = s.as_bytes();
    let eq_count = bytes.iter().rev().take_while(|&&b| b == b'=').count();
    eq_count <= 2
        && bytes[..bytes.len() - eq_count]
            .iter()
            .all(|&b| b.is_ascii_alphanumeric() || b == b'+' || b == b'/')
}

/// `true` iff `value` is a non-null, non-array JSON object. In `serde_json` the
/// only object kind is `Value::Object`, so this is `value.is_object()`.
pub fn is_plain_object(value: &Value) -> bool {
    value.is_object()
}

/// Recursive value validator — a port of server `schema::validate_value` and
/// the TS `validateValue` at `ts-client/src/in_memory.ts:150-198`. Switches on
/// the [`FieldType`] variant.
pub fn validate_value(ty: &FieldType, value: &Value) -> bool {
    match ty {
        FieldType::String => value.is_string(),
        FieldType::Number => value.is_number(),
        FieldType::Boolean => value.is_boolean(),
        FieldType::Null => value.is_null(),
        FieldType::Id { .. } => is_hex_id(value),
        FieldType::Literal { value: lit } => value == lit,
        FieldType::Optional { inner } => value.is_null() || validate_value(inner, value),
        FieldType::Union { variants } => variants.iter().any(|v| validate_value(v, value)),
        FieldType::Array { element } => value
            .as_array()
            .is_some_and(|arr| arr.iter().all(|item| validate_value(element, item))),
        FieldType::Object { fields } => {
            let map = match value.as_object() {
                Some(m) => m,
                None => return false,
            };
            // Reject unknown keys.
            for key in map.keys() {
                if !fields.contains_key(key) {
                    return false;
                }
            }
            // Declared fields: present-and-valid, or absent-and-optional.
            for (field, field_ty) in fields {
                match map.get(field) {
                    Some(v) => {
                        if !validate_value(field_ty, v) {
                            return false;
                        }
                    }
                    None if !matches!(field_ty, FieldType::Optional { .. }) => return false,
                    None => {}
                }
            }
            true
        }
        FieldType::Int64 => is_int64_string(value),
        FieldType::Bytes => is_base64_string(value),
        FieldType::Any => true,
        FieldType::Record { value: value_ty } => value
            .as_object()
            .is_some_and(|m| m.values().all(|v| validate_value(value_ty, v))),
        FieldType::Vector { dimensions } => {
            let arr = match value.as_array() {
                Some(a) => a,
                None => return false,
            };
            arr.len() == (*dimensions as usize)
                && arr
                    .iter()
                    .all(|v| v.as_f64().is_some_and(|f| f.is_finite()))
        }
    }
}

/// Full-document validator — a port of server `schema::validate_doc` and the TS
/// `validateDoc` at `ts-client/src/in_memory.ts:200-219`. Returns the first
/// violation as an [`RtDbError`] with code `SCHEMA_VIOLATION`.
///
/// Reserved (`_`-prefixed) and unknown fields are rejected, every declared
/// field is either present-and-valid or absent-and-optional.
pub fn validate_doc(table: &TableDef, doc: &Value) -> Result<(), RtDbError> {
    let map = doc.as_object();
    let map = match map {
        Some(m) => m,
        None => {
            return Err(RtDbError::new(
                ErrorCode::SchemaViolation,
                "document must be a JSON object",
            ));
        }
    };
    for key in map.keys() {
        if key.starts_with('_') {
            return Err(RtDbError::new(
                ErrorCode::SchemaViolation,
                format!("field '{key}' is reserved"),
            ));
        }
        if !table.fields.contains_key(key) {
            return Err(RtDbError::new(
                ErrorCode::SchemaViolation,
                format!("unknown field '{key}'"),
            ));
        }
    }
    for (field, field_ty) in &table.fields {
        match map.get(field) {
            Some(v) => {
                if !validate_value(field_ty, v) {
                    return Err(RtDbError::new(
                        ErrorCode::SchemaViolation,
                        format!("field '{field}' has an invalid value"),
                    ));
                }
            }
            None if !matches!(field_ty, FieldType::Optional { .. }) => {
                return Err(RtDbError::new(
                    ErrorCode::SchemaViolation,
                    format!("field '{field}' is required"),
                ));
            }
            None => {}
        }
    }
    Ok(())
}

/// Removes keys whose value is `null` for an `Optional` field whose inner type
/// does not itself accept `null` — a port of server `strip_unset_optionals` and
/// the TS helper at `ts-client/src/in_memory.ts:225-240`. An
/// inserted/patched-then-nulled optional lands as "key absent", matching the
/// server's single representation of an unset optional.
pub fn strip_unset_optionals(table: &TableDef, doc: &Value) -> Value {
    let map = match doc.as_object() {
        Some(m) => m,
        None => return doc.clone(),
    };
    let mut out = Map::new();
    for (key, value) in map {
        if value.is_null()
            && let Some(FieldType::Optional { inner }) = table.fields.get(key)
            && !validate_value(inner, value)
        {
            continue;
        }
        out.insert(key.clone(), value.clone());
    }
    Value::Object(out)
}

/// Stamps the TTL field at insert when the table declares a
/// `default_duration_ms` and the document omits the field. After this, the TTL
/// field is ordinary (patch/replace manipulate it normally). Mirrors server
/// `txn::stamp_ttl_default` and the TS `stampTtlDefault`
/// (`ts-client/src/in_memory.ts:407-425`); runs BEFORE validation so the
/// stamped value satisfies a required numeric field. Returns a cloned map with
/// the field set when a stamp is applied, otherwise the original doc cloned
/// unchanged.
pub(super) fn stamp_ttl_default(
    table_def: &TableDef,
    doc: &Map<String, Value>,
    now: i64,
) -> Map<String, Value> {
    if let Some(ttl) = &table_def.ttl
        && let Some(d) = ttl.default_duration_ms
        && !doc.contains_key(&ttl.field)
    {
        let mut out = doc.clone();
        out.insert(ttl.field.clone(), Value::from(now + d));
        out
    } else {
        doc.clone()
    }
}

/// Stamps the table's `updatedAtField` (FM-36) with `now`, overwriting any
/// client-supplied value — the same authority model as the server's owner
/// stamp. Runs on every version-bumping write path: insert, patch, replace,
/// upsert (both branches), patchByQuery, and cascade setNull. The value
/// matches the field's wire convention: a JSON number on `number`, a decimal
/// string on `int64`. Mirrors server `txn::stamp_updated_at`; like
/// [`stamp_ttl_default`] it runs BEFORE validation so the stamp satisfies a
/// required field. Returns a cloned map with the field stamped, otherwise the
/// original doc cloned unchanged.
pub(super) fn stamp_updated_at(
    table_def: &TableDef,
    doc: &Map<String, Value>,
    now: i64,
) -> Map<String, Value> {
    if let Some(field) = &table_def.updated_at_field {
        let mut out = doc.clone();
        let value = match table_def.fields.get(field) {
            Some(FieldType::Int64) => Value::String(now.to_string()),
            _ => Value::from(now),
        };
        out.insert(field.clone(), value);
        out
    } else {
        doc.clone()
    }
}

/// Applies the table's push-time-validated `defaults` (FM-32) to a NEW
/// document: every key the doc omits is stamped from the schema. Runs after
/// [`stamp_ttl_default`] (a ttl default on the same field wins) and before the
/// owner/principal stamps (server-stamped values win). Callers are exactly the
/// new-document paths — insert, replace, upsert-insert; `patch` (and
/// upsert-update / patch-by-query) never re-apply, so clearing an optional
/// field stays cleared. Mirrors server `txn::apply_defaults`; like
/// `stamp_ttl_default` it runs BEFORE validation so a stamped value satisfies
/// a required field. Returns a cloned map with the defaults set, otherwise the
/// original doc cloned unchanged.
pub(super) fn apply_defaults(table_def: &TableDef, doc: &Map<String, Value>) -> Map<String, Value> {
    let mut out = doc.clone();
    for (field, value) in &table_def.defaults {
        if !out.contains_key(field) {
            out.insert(field.clone(), value.clone());
        }
    }
    out
}

/// Applies a patch's `fields` onto `doc` — a port of server `txn::apply_patch`
/// and the TS `applyPatch` (`ts-client/src/in_memory.ts:243-265`). A `null`
/// onto an `Optional` field whose inner type doesn't itself accept `null`
/// deletes the key (mirroring `strip_unset_optionals`'s single representation
/// of an unset optional); the merged doc is then re-validated whole.
pub fn apply_patch(
    table: &TableDef,
    doc: &Value,
    fields: &Map<String, Value>,
) -> Result<Value, RtDbError> {
    let mut merged = match doc.as_object() {
        Some(m) => m.clone(),
        None => Map::new(),
    };
    for (field, value) in fields {
        let field_ty = match table.fields.get(field) {
            Some(t) => t,
            None => {
                return Err(RtDbError::new(
                    ErrorCode::SchemaViolation,
                    format!("unknown field '{field}'"),
                ));
            }
        };
        // null on an Optional<String> (or any Optional whose inner rejects null)
        // deletes the key — the server's strip_unset_optionals semantics.
        let strip = if let FieldType::Optional { inner } = field_ty {
            value.is_null() && !validate_value(inner, value)
        } else {
            false
        };
        if strip {
            merged.remove(field);
            continue;
        }
        if !validate_value(field_ty, value) {
            return Err(RtDbError::new(
                ErrorCode::SchemaViolation,
                format!("field '{field}' has an invalid value"),
            ));
        }
        merged.insert(field.clone(), value.clone());
    }
    let merged_value = Value::Object(merged);
    validate_doc(table, &merged_value)?;
    Ok(merged_value)
}

// ---------------------------------------------------------------------------
// Filter evaluation — a port of `validateFilter`/`evalFilterExpr` and the leaf
// helpers in `ts-client/src/in_memory.ts:361-488`. The server compiles a
// `FilterExpr` once against the table's declared fields
// (`query::compile_filter`), then evaluates the compiled predicate per row
// (`query::jsonb_lhs_and_bind`). This harness mirrors that two-phase split:
// [`validate_filter`] runs once in `run_query` before the row loop,
// [`eval_filter_expr`] runs per row inside [`matches_filter`].
// ---------------------------------------------------------------------------

/// The six leaf comparison operators, mirroring `FilterLeafOp` in the TS
/// source. Used as the dispatch key for [`compare_leaf`]/[`compare_values`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
}

/// Value-kind domain that picks the comparison semantics for a leaf, mirroring
/// `inValueKind`'s three variants. Post-[`check_leaf_value`] the
/// `Boolean` fallthrough is unreachable — every value is one of the three.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ValueKind {
    String,
    Number,
    Boolean,
}

/// Structural + kind validation of a [`FilterExpr`] against a table's declared
/// fields, mirroring server `query::compile_filter` (both its
/// `validate_filter_expr_fields` pre-checks and the per-leaf
/// `field_lhs_and_bind` typing) and the TS `validateFilter`
/// (`ts-client/src/in_memory.ts:361-386`). Returns `BAD_REQUEST` for: an empty
/// `and`/`or`, an empty `in`, an unknown field, a non-string/number/boolean
/// leaf value, mixed-type `in` values, or a value whose JSON kind does not
/// match the field's declared type (SEC-126 — indexed fields type through the
/// eq-bind conversion, other declared fields through the jsonb kind check).
/// Call once before evaluating per row.
pub fn validate_filter(expr: &FilterExpr, table: &TableDef) -> Result<(), RtDbError> {
    match expr {
        FilterExpr::And { exprs } => {
            if exprs.is_empty() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "and filter requires at least one expr",
                ));
            }
            for e in exprs {
                validate_filter(e, table)?;
            }
            Ok(())
        }
        FilterExpr::Or { exprs } => {
            if exprs.is_empty() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "or filter requires at least one expr",
                ));
            }
            for e in exprs {
                validate_filter(e, table)?;
            }
            Ok(())
        }
        FilterExpr::In { field, values } => {
            if values.is_empty() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "in filter requires at least one value",
                ));
            }
            for v in values {
                check_leaf_value(field, v, table)?;
            }
            let first_kind = in_value_kind(&values[0]);
            for v in &values[1..] {
                if in_value_kind(v) != first_kind {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        "in filter values must all be the same type",
                    ));
                }
            }
            for v in values {
                check_leaf_kind(field, v, table)?;
            }
            Ok(())
        }
        FilterExpr::Eq { field, value }
        | FilterExpr::Neq { field, value }
        | FilterExpr::Gt { field, value }
        | FilterExpr::Gte { field, value }
        | FilterExpr::Lt { field, value }
        | FilterExpr::Lte { field, value }
        | FilterExpr::Contains { field, value } => check_leaf(field, value, table),
        FilterExpr::Not { expr } => validate_filter(expr, table),
        FilterExpr::Exists { field } => {
            leaf_field_type(field, table)?;
            Ok(())
        }
    }
}

/// Resolves a filter field to its declared type, `BAD_REQUEST`ing on an
/// unknown field. The guard every leaf shape runs before its value checks.
fn leaf_field_type<'a>(field: &str, table: &'a TableDef) -> Result<&'a FieldType, RtDbError> {
    table.fields.get(field).ok_or_else(|| {
        RtDbError::new(
            ErrorCode::BadRequest,
            format!("filter references unknown field '{field}'"),
        )
    })
}

/// `BAD_REQUEST` if `field` is not in the table's declared fields or `value`
/// is not a string/number/boolean. Mirrors `checkLeafValue`
/// (`ts-client/src/in_memory.ts:388-395`).
fn check_leaf_value(field: &str, value: &Value, table: &TableDef) -> Result<(), RtDbError> {
    leaf_field_type(field, table)?;
    if !matches!(value, Value::String(_) | Value::Number(_) | Value::Bool(_)) {
        return Err(RtDbError::new(
            ErrorCode::BadRequest,
            "filter value must be a string, number, or boolean",
        ));
    }
    Ok(())
}

/// One value-carrying leaf after the scalar guard: the SEC-126 kind check.
fn check_leaf(field: &str, value: &Value, table: &TableDef) -> Result<(), RtDbError> {
    check_leaf_value(field, value, table)?;
    check_leaf_kind(field, value, table)
}

/// SEC-126: type-checks a leaf value against the field's declared type,
/// mirroring server `query::field_lhs_and_bind`. A field appearing in any
/// declared index types through the same eq-bind conversion as index `eq`
/// prefixes ([`coerce_index_value`], the `eq_bind_for` mirror — an indexed
/// int64 field takes its decimal-STRING wire form); any other declared field
/// requires only the value's JSON kind to match the declared type
/// ([`validate_jsonb_comparison_value`]). Mismatch is a `BAD_REQUEST` before
/// any row is evaluated, not a permissive no-match.
fn check_leaf_kind(field: &str, value: &Value, table: &TableDef) -> Result<(), RtDbError> {
    let field_ty = leaf_field_type(field, table)?;
    let indexed = table
        .indexes
        .iter()
        .flatten()
        .any(|idx| idx.fields.iter().any(|f| f == field));
    if indexed {
        return coerce_index_value(table, field, value).map(|_| ());
    }
    validate_jsonb_comparison_value(field, field_ty, value)
}

/// Returns `Ok(())` when `value`'s JSON kind can be compared against a
/// declared-but-not-indexed field of type `ty`, else `Err(BadRequest)` — a
/// port of server `query::validate_jsonb_comparison_value`. The `Optional`
/// wrapper is unwrapped. Note the asymmetry with the indexed path: a
/// non-indexed int64 filter takes JSON numbers and rejects the decimal-string
/// wire form (the server casts `(doc->>'f')::float8`, never a typed `bigint`
/// bind, on this path). Complex/unknown field types (Any, Literal, Union,
/// Array, Object, …) accept any scalar so existing callers are not widened.
fn validate_jsonb_comparison_value(
    field: &str,
    ty: &FieldType,
    value: &Value,
) -> Result<(), RtDbError> {
    let inner = match ty {
        FieldType::Optional { inner } => inner.as_ref(),
        _ => ty,
    };
    let ok = match inner {
        FieldType::String | FieldType::Id { .. } | FieldType::Bytes => value.is_string(),
        FieldType::Number | FieldType::Int64 => value.is_number(),
        FieldType::Boolean => value.is_boolean(),
        // Any / Literal / Union / Array / Object / Record / Vector / Null:
        // no reliable static check; accept any scalar (existing behavior).
        _ => matches!(value, Value::String(_) | Value::Number(_) | Value::Bool(_)),
    };
    if ok {
        Ok(())
    } else {
        Err(RtDbError::new(
            ErrorCode::BadRequest,
            format!("filter on field '{field}' value kind does not match declared field type"),
        ))
    }
}

/// Value-kind domain for an `in` value, mirroring `inValueKind`
/// (`ts-client/src/in_memory.ts:397-401`).
fn in_value_kind(value: &Value) -> ValueKind {
    match value {
        Value::String(_) => ValueKind::String,
        Value::Number(_) => ValueKind::Number,
        _ => ValueKind::Boolean,
    }
}

/// Evaluate a [`FilterExpr`] predicate against a stored doc, mirroring server
/// `query::field_lhs_and_bind` / `jsonb_lhs_and_bind` and the TS
/// `evalFilterExpr` (`ts-client/src/in_memory.ts:410-421`): the filter value's
/// kind picks the comparison domain — string compares the doc field's `->>`
/// text, number compares it as `float8`, boolean as `boolean` — EXCEPT on a
/// declared `int64` field, where a string value (the wire form the server
/// types as a `bigint` bind, whether via an index's typed column or the jsonb
/// path) compares numerically: decimal strings must order `-605 < -1 < 15`,
/// not lexicographically (ENH-027 parity fix). A null/absent field never
/// matches (SQL NULL exclusion). `fields` is the table's declared field map
/// (pass an empty map for type-less evaluation, e.g. unit tests). Assumes
/// [`validate_filter`] already passed.
pub fn eval_filter_expr(
    expr: &FilterExpr,
    doc: &Value,
    fields: &BTreeMap<String, FieldType>,
) -> bool {
    match expr {
        FilterExpr::And { exprs } => exprs.iter().all(|e| eval_filter_expr(e, doc, fields)),
        FilterExpr::Or { exprs } => exprs.iter().any(|e| eval_filter_expr(e, doc, fields)),
        FilterExpr::In { field, values } => values
            .iter()
            .any(|v| compare_leaf(FilterOp::Eq, field, v, doc, fields)),
        FilterExpr::Eq { field, value } => compare_leaf(FilterOp::Eq, field, value, doc, fields),
        FilterExpr::Neq { field, value } => compare_leaf(FilterOp::Neq, field, value, doc, fields),
        FilterExpr::Gt { field, value } => compare_leaf(FilterOp::Gt, field, value, doc, fields),
        FilterExpr::Gte { field, value } => compare_leaf(FilterOp::Gte, field, value, doc, fields),
        FilterExpr::Lt { field, value } => compare_leaf(FilterOp::Lt, field, value, doc, fields),
        FilterExpr::Lte { field, value } => compare_leaf(FilterOp::Lte, field, value, doc, fields),
        FilterExpr::Not { expr } => !eval_filter_expr(expr, doc, fields),
        FilterExpr::Contains { field, value } => match doc.get(field) {
            Some(Value::Array(arr)) => arr.iter().any(|v| v == value),
            _ => false,
        },
        FilterExpr::Exists { field } => matches!(doc.get(field), Some(v) if !v.is_null()),
    }
}

/// Per-leaf comparison, mirroring `compareLeaf`
/// (`ts-client/src/in_memory.ts:423-444`) plus the server's typed-column arm
/// for declared `int64` fields (ENH-027 parity fix — see
/// [`eval_filter_expr`]). `doc[field]` null/absent → `false` (SQL NULL
/// exclusion); the filter value's kind picks the comparison domain, except
/// that a string value against a declared `int64` field compares numerically.
fn compare_leaf(
    op: FilterOp,
    field: &str,
    filter_value: &Value,
    doc: &Value,
    fields: &BTreeMap<String, FieldType>,
) -> bool {
    let doc_val = match doc.get(field) {
        Some(v) if !v.is_null() => v,
        _ => return false,
    };
    if let Value::String(s) = filter_value
        && is_int64_field(fields.get(field))
    {
        // The server binds a string filter value on an int64 field as
        // `EqBind::I64` against the typed `bigint` column (indexed fields);
        // `validate_filter` (SEC-126) rejects the string form on the
        // non-indexed jsonb path, so a validated string here is the typed
        // bind. Parse both sides exactly as i64 (i64::MAX is not
        // f64-exact); an unparseable value never matches.
        let Some(lhs) = doc_val.as_str().and_then(|s| s.parse::<i64>().ok()) else {
            return false;
        };
        return match s.parse::<i64>() {
            Ok(rhs) => compare_values(op, &lhs, &rhs),
            Err(_) => false,
        };
    }
    match filter_value {
        Value::String(s) => {
            let lhs = doc_to_text(doc_val);
            compare_values(op, &lhs, s)
        }
        Value::Number(_) => match doc_to_number(doc_val) {
            Some(lhs) => match filter_value.as_f64() {
                Some(rhs) => compare_values(op, &lhs, &rhs),
                None => false,
            },
            None => false,
        },
        Value::Bool(b) => match doc_val {
            Value::Bool(db) => compare_values(op, db, b),
            _ => false,
        },
        // Unreachable post-validate (`check_leaf_value` rejects non-string/
        // number/boolean values); defensively treat as no-match.
        _ => false,
    }
}

/// Whether a declared field type is `int64` (an `optional<int64>` unwraps to
/// it — `eq_bind_for` unwraps `Optional` the same way).
fn is_int64_field(ty: Option<&FieldType>) -> bool {
    match ty {
        Some(FieldType::Int64) => true,
        Some(FieldType::Optional { inner }) => matches!(**inner, FieldType::Int64),
        _ => false,
    }
}

/// Mirrors Postgres `doc->>'field'`: the JSON text of the value. Ports
/// `docToText` (`ts-client/src/in_memory.ts:447-452`) — string→as-is,
/// number→`JSON.stringify(n)` (integer-valued numbers render without a decimal
/// point, matching JS), boolean→"true"/"false", else JSON text.
fn doc_to_text(doc_val: &Value) -> String {
    match doc_val {
        Value::String(s) => s.clone(),
        // JS `JSON.stringify` renders integer-valued numbers with no decimal
        // (`5`, not `5.0`); serde_json emits `"5.0"` for a float-backed
        // integer, so route integer-backed numbers to their exact form and
        // canonicalize integer-valued floats within JS's safe-integer range.
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.to_string()
            } else if let Some(u) = n.as_u64() {
                u.to_string()
            } else if let Some(f) = n.as_f64() {
                if f.is_finite() && f.fract() == 0.0 && f.abs() <= 9_007_199_254_740_992.0 {
                    (f as i64).to_string()
                } else {
                    n.to_string()
                }
            } else {
                n.to_string()
            }
        }
        Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

/// Mirrors Postgres `(doc->>'field')::float8`: a finite number, or a parsed
/// numeric string. Ports `docToNumber` (`ts-client/src/in_memory.ts:455-462`).
fn doc_to_number(doc_val: &Value) -> Option<f64> {
    match doc_val {
        Value::Number(n) => n.as_f64().filter(|f| f.is_finite()),
        Value::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                return None;
            }
            trimmed.parse::<f64>().ok().filter(|f| f.is_finite())
        }
        _ => None,
    }
}

/// Op dispatch over a same-typed pair (string/number/boolean — the filter
/// value's kind fixes the domain, so the operands never mix). Ports
/// `compareValues` (`ts-client/src/in_memory.ts:464-483`).
fn compare_values<T: PartialEq + PartialOrd>(op: FilterOp, lhs: &T, rhs: &T) -> bool {
    match op {
        FilterOp::Eq => lhs == rhs,
        FilterOp::Neq => lhs != rhs,
        FilterOp::Gt => lhs > rhs,
        FilterOp::Gte => lhs >= rhs,
        FilterOp::Lt => lhs < rhs,
        FilterOp::Lte => lhs <= rhs,
    }
}

/// Filter hook for [`InMemoryRtDbClient::run_query`] and the by-query write
/// steps. Delegates to [`eval_filter_expr`]; validation runs once in
/// `run_query` before the row loop, so by the time this runs the filter is
/// structurally sound.
pub(super) fn matches_filter(
    expr: &FilterExpr,
    doc: &Value,
    fields: &BTreeMap<String, FieldType>,
) -> bool {
    eval_filter_expr(expr, doc, fields)
}
