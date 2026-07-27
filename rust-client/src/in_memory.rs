//! In-memory par-rt-db client for unit tests. No network, no Postgres; mirrors
//! server DSL/step-result/system-field semantics. Ports
//! `ts-client/src/in_memory.ts`.
//!
//! The server (`server/src/{txn,query,schema,protocol}.rs`) is the source of
//! truth for the declarative DSL, step-result shapes, system fields, and query
//! semantics; this client mirrors them so app code can exercise query/txn/schema
//! behavior with no network and no live Postgres. It exposes the same data
//! surface as the live clients — `push_schema`, `query` (one-shot, like
//! [`crate::RtDbHttpClient`]), `mutate`/transactions (like
//! [`crate::RtDbClient`]), and `subscribe` (reactive `query_update`s) — so a
//! test can swap it in behind a shared interface.
//!
//! Parity is deliberately scoped to the documented core (schema push, insert /
//! patch / replace / delete / expect_version / expect_absent / upsert, point
//! reads, index eq + range queries with order/take/unique/first/count, and
//! reactive subscriptions). Gaps are marked with `TODO` and return an `INTERNAL`
//! error rather than silently misbehaving.
//!
//! This module is the scaffold (Task 1): struct + options + `push_schema` + the
//! validation/id/format helpers. Subsequent tasks fill in mutate, queries,
//! subscriptions, scheduling, and storage.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value};

use crate::error::{ErrorCode, RtDbError};
use crate::schema::{FieldType, SchemaDef, TableDef};

/// Maximum number of steps in a single transaction (mirrors the server cap).
pub const MAX_STEPS: usize = 256;
/// Maximum rows returned from a single `take`/`collect` (mirrors the server cap).
pub const MAX_TAKE: usize = 4096;
/// Approximate cron re-fire interval for the in-memory stub. Real 5-field cron
/// parsing is deferred to the server; the harness only needs crons to re-arm.
pub const CRON_STEP_MS: i64 = 60_000;

/// A stored row: the user doc plus its identity/history, kept separate so the
/// system fields (`_id`/`_creationTime`/`_version`) are merged in only at read
/// time — exactly as the server stores `doc` jsonb alongside `id`/`created_at`/
/// `version` columns.
#[derive(Debug, Clone)]
pub struct StoredRow {
    pub id: String,
    pub doc: Value,
    pub version: i64,
    pub created_at: i64,
}

/// Injectable clock and RNG for deterministic id minting and `_creationTime`.
///
/// Mirrors `InMemoryRtDbClientOptions` in `ts-client/src/in_memory.ts:91-96`.
/// Both `now` and `random` are optional; `InMemoryRtDbClient::new` supplies
/// defaults (system clock for `now`, a constant `0.5` for `random` — tests that
/// need determinism should always inject both).
#[derive(Default)]
pub struct InMemoryRtDbClientOptions {
    now: Option<Arc<dyn Fn() -> i64 + Send + Sync>>,
    random: Option<Arc<dyn Fn() -> f64 + Send + Sync>>,
}

impl InMemoryRtDbClientOptions {
    /// Inject a clock (epoch millis) for deterministic `_creationTime` and id
    /// minting.
    pub fn now(mut self, f: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        self.now = Some(Arc::new(f));
        self
    }
    /// Inject an RNG in `[0, 1)` for deterministic id minting.
    pub fn random(mut self, f: impl Fn() -> f64 + Send + Sync + 'static) -> Self {
        self.random = Some(Arc::new(f));
        self
    }
}

/// In-memory par-rt-db client for unit tests. See the
/// [module docs](crate::in_memory) for the parity scope and deferred gaps.
pub struct InMemoryRtDbClient {
    #[expect(dead_code, reason = "consumed by task 2 (insert/_creationTime)")]
    now: Arc<dyn Fn() -> i64 + Send + Sync>,
    #[expect(dead_code, reason = "consumed by task 2 (id minting)")]
    random: Arc<dyn Fn() -> f64 + Send + Sync>,
    schema: Option<SchemaDef>,
    /// Per-table schema defs, keyed by table name. Separate from `schema` so
    /// Task 2+'s hot paths (validate-on-write, table lookups) don't re-walk the
    /// whole schema.
    tables: HashMap<String, TableDef>,
    /// Document store keyed by `(table_name, id)` — flat representation of the
    /// TS `Map<string, Map<string, StoredRow>>`.
    docs: HashMap<(String, String), StoredRow>,
    #[expect(dead_code, reason = "consumed by task 6 (storage id minting)")]
    id_counter: u64,
    idempotency: HashMap<String, Vec<Value>>,
    #[expect(dead_code, reason = "consumed by task 4 (scheduling)")]
    schedules: Vec<Value>,
    #[expect(dead_code, reason = "consumed by task 5 (subscriptions)")]
    subscribers: Vec<Value>,
    #[expect(dead_code, reason = "consumed by task 6 (storage)")]
    storage: HashMap<String, Value>,
}

impl InMemoryRtDbClient {
    /// Construct a new harness. `options.now` and `options.random` default to
    /// the system clock and a constant `0.5` respectively; tests that need
    /// deterministic ids/timestamps should always inject both.
    pub fn new(options: InMemoryRtDbClientOptions) -> Self {
        Self {
            now: options.now.unwrap_or_else(|| {
                Arc::new(|| {
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0)
                })
            }),
            random: options.random.unwrap_or_else(|| Arc::new(|| 0.5)),
            schema: None,
            tables: HashMap::new(),
            docs: HashMap::new(),
            id_counter: 0,
            idempotency: HashMap::new(),
            schedules: Vec::new(),
            subscribers: Vec::new(),
            storage: HashMap::new(),
        }
    }

    /// Installs `schema` as this client's sole in-memory database schema. Clears
    /// any previously-stored documents so each push starts from a clean slate.
    /// (The live server is additive-only; full additive evolution is deferred.)
    ///
    /// Ports `pushSchema` in `ts-client/src/in_memory.ts:512-519`. The Rust
    /// signature takes the typed [`SchemaDef`] directly (no `toSchemaJson`
    /// conversion needed since the builder already produces the wire shape).
    pub fn push_schema(&mut self, schema: &SchemaDef) {
        self.schema = Some(schema.clone());
        self.tables.clear();
        self.docs.clear();
        self.idempotency.clear();
        for (name, def) in &schema.tables {
            self.tables.insert(name.clone(), def.clone());
        }
    }

    /// Snapshot of the currently-installed schema (or `None` before
    /// `push_schema`). Returns a clone so callers can freely inspect/mutate.
    pub fn to_schema_json(&self) -> Option<SchemaDef> {
        self.schema.clone()
    }
}

// ---------------------------------------------------------------------------
// Free helpers — ports of the module-private functions in
// `ts-client/src/in_memory.ts`. Kept `pub` so task tests can exercise them
// directly (the TS source exports them via the module surface too).
// ---------------------------------------------------------------------------

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Schema, Table};
    use serde_json::json;

    /// The test schema mirrored from `ts-client/tests/in_memory.test.ts:10-20`.
    fn test_schema() -> SchemaDef {
        Schema::builder()
            .table(
                "items",
                Table::new()
                    .field("name", FieldType::String)
                    .field("status", FieldType::String)
                    .field("order", FieldType::Number)
                    .field("note", FieldType::optional(FieldType::String))
                    .index("by_name", &["name"])
                    .index("by_status", &["status"])
                    .index("by_status_and_order", &["status", "order"]),
            )
            .build()
    }

    fn items_table(schema: &SchemaDef) -> &TableDef {
        schema.tables.get("items").expect("items table present")
    }

    // ---- schema push ---------------------------------------------------

    #[test]
    fn push_schema_stores_the_schema() {
        // Mirrors the TS "schema push" suite: after pushSchema, the schema is
        // installed and the table is known (the TS suite verifies this by
        // running `query().collect()` and getting `[]`; here we verify the
        // schema snapshot directly because query/collect land in task 3).
        let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
        let schema = test_schema();
        c.push_schema(&schema);
        let stored = c.to_schema_json().expect("schema installed");
        assert!(stored.tables.contains_key("items"));
        assert!(c.tables.contains_key("items"));
    }

    #[test]
    fn push_schema_replaces_the_previous_schema() {
        // The TS harness replaces (not additive-merges) on each push and clears
        // stored docs/idempotency so each push starts from a clean slate. (The
        // live server is additive-only; that evolution is deferred here.)
        let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
        c.push_schema(&test_schema());
        let only_other = Schema::builder()
            .table("solo", Table::new().field("x", FieldType::Number))
            .build();
        c.push_schema(&only_other);
        let stored = c.to_schema_json().expect("schema installed");
        assert!(stored.tables.contains_key("solo"));
        assert!(!stored.tables.contains_key("items"));
        assert!(!c.tables.contains_key("items"));
    }

    // ---- validate_doc --------------------------------------------------

    #[test]
    fn validate_doc_rejects_unknown_field() {
        let schema = test_schema();
        let bad = json!({"name": "a", "status": "todo", "order": 1, "bogus": 9});
        let err = validate_doc(items_table(&schema), &bad).unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);
        assert!(err.message.contains("bogus"), "got: {}", err.message);
    }

    #[test]
    fn validate_doc_rejects_reserved_field() {
        let schema = test_schema();
        let bad = json!({"name": "a", "status": "todo", "order": 1, "_id": "x"});
        let err = validate_doc(items_table(&schema), &bad).unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);
        assert!(err.message.contains("_id"), "got: {}", err.message);
    }

    #[test]
    fn validate_doc_rejects_wrong_field_type() {
        // The "invalid field type on a doc is rejected" case from the brief.
        let schema = test_schema();
        let bad = json!({"name": 42, "status": "todo", "order": 1});
        let err = validate_doc(items_table(&schema), &bad).unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);
        assert!(err.message.contains("name"), "got: {}", err.message);
    }

    #[test]
    fn validate_doc_rejects_missing_required_field() {
        let schema = test_schema();
        let bad = json!({"name": "a", "order": 1}); // missing required "status"
        let err = validate_doc(items_table(&schema), &bad).unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);
        assert!(err.message.contains("status"), "got: {}", err.message);
    }

    #[test]
    fn validate_doc_accepts_a_valid_doc_with_optional_absent() {
        let schema = test_schema();
        let good = json!({"name": "a", "status": "todo", "order": 1});
        validate_doc(items_table(&schema), &good).expect("valid doc");
    }

    #[test]
    fn validate_doc_accepts_an_optional_field_set_to_null() {
        // `note` is `Optional<String>`; null is accepted at the doc level
        // because Optional accepts null. `strip_unset_optionals` is what
        // converts it to "absent" for storage.
        let schema = test_schema();
        let good = json!({"name": "a", "status": "todo", "order": 1, "note": null});
        validate_doc(items_table(&schema), &good).expect("valid doc");
    }

    // ---- strip_unset_optionals ----------------------------------------

    #[test]
    fn strip_unset_optionals_drops_null_optional_string() {
        // `note: Optional<String>` set to null → key is stripped (the inner
        // String doesn't accept null, so this is "unset").
        let schema = test_schema();
        let doc = json!({"name": "a", "status": "todo", "order": 1, "note": null});
        let stripped = strip_unset_optionals(items_table(&schema), &doc);
        assert_eq!(stripped, json!({"name": "a", "status": "todo", "order": 1}));
    }

    #[test]
    fn strip_unset_optionals_keeps_null_for_optional_that_accepts_null() {
        // `Optional<Null>` does accept null as its inner value, so the key is
        // preserved.
        let schema = Schema::builder()
            .table(
                "t",
                Table::new().field("x", FieldType::optional(FieldType::Null)),
            )
            .build();
        let table = schema.tables.get("t").expect("table present");
        let doc = json!({"x": null});
        let stripped = strip_unset_optionals(table, &doc);
        assert_eq!(stripped, json!({"x": null}));
    }

    // ---- id/format helpers --------------------------------------------

    #[test]
    fn is_hex_id_checks_32_lowercase_hex_chars() {
        assert!(is_hex_id(&json!("0123456789abcdef0123456789abcdef")));
        assert!(!is_hex_id(&json!("0123456789ABCDEF0123456789ABCDEF"))); // uppercase
        assert!(!is_hex_id(&json!("0123456789abcdef"))); // too short
        assert!(!is_hex_id(&json!(42)));
        assert!(!is_hex_id(&json!(null)));
    }

    #[test]
    fn is_int64_string_accepts_i64_range_only() {
        assert!(is_int64_string(&json!("0")));
        assert!(is_int64_string(&json!("-1")));
        assert!(is_int64_string(&json!("9223372036854775807"))); // i64::MAX
        assert!(is_int64_string(&json!("-9223372036854775808"))); // i64::MIN
        // Out of i64 range:
        assert!(!is_int64_string(&json!("9223372036854775808")));
        assert!(!is_int64_string(&json!("-9223372036854775809")));
        // Bad shape:
        assert!(!is_int64_string(&json!("1.5")));
        assert!(!is_int64_string(&json!("-")));
        assert!(!is_int64_string(&json!("")));
        assert!(!is_int64_string(&json!(42)));
    }

    #[test]
    fn is_base64_string_matches_the_ts_regex() {
        assert!(is_base64_string(&json!("")));
        assert!(is_base64_string(&json!("ABCD")));
        assert!(is_base64_string(&json!("ABC=")));
        assert!(is_base64_string(&json!("AB==")));
        assert!(is_base64_string(&json!("YWJjZA=="))); // "abcd"
        // Length not a multiple of 4:
        assert!(!is_base64_string(&json!("ABC")));
        // Too much padding:
        assert!(!is_base64_string(&json!("A===")));
        // Bad body char:
        assert!(!is_base64_string(&json!("ABC!")));
        assert!(!is_base64_string(&json!(42)));
    }

    #[test]
    fn validate_value_handles_each_field_type_variant() {
        // A sanity sweep over the variants; full per-variant coverage lives in
        // the schema tests. Here we just confirm routing works.
        assert!(validate_value(&FieldType::String, &json!("hi")));
        assert!(!validate_value(&FieldType::String, &json!(2)));
        assert!(validate_value(&FieldType::Number, &json!(2.5)));
        assert!(validate_value(&FieldType::Boolean, &json!(true)));
        assert!(validate_value(&FieldType::Null, &json!(null)));
        assert!(validate_value(&FieldType::Any, &json!(null)));
        assert!(validate_value(
            &FieldType::Id { table: "x".into() },
            &json!("0123456789abcdef0123456789abcdef")
        ));
        assert!(validate_value(
            &FieldType::Literal { value: json!("a") },
            &json!("a")
        ));
        assert!(validate_value(
            &FieldType::Optional {
                inner: Box::new(FieldType::String)
            },
            &json!(null)
        ));
        assert!(validate_value(
            &FieldType::Union {
                variants: vec![FieldType::String, FieldType::Number]
            },
            &json!(2)
        ));
        assert!(validate_value(
            &FieldType::Array {
                element: Box::new(FieldType::Number)
            },
            &json!([1, 2, 3])
        ));
        assert!(validate_value(&FieldType::Int64, &json!("42")));
        assert!(validate_value(&FieldType::Bytes, &json!("YWJjZA==")));
        assert!(validate_value(
            &FieldType::Vector { dimensions: 3 },
            &json!([1.0, 2.0, 3.0])
        ));
    }

    #[test]
    fn canonical_is_key_order_independent() {
        // serde_json's default BTreeMap-backed Map serializes with sorted keys,
        // so canonical(a) == canonical(b) even when the source maps had
        // different insertion order.
        let a = json!({"b": 1, "a": 2});
        let b = json!({"a": 2, "b": 1});
        assert_eq!(canonical(&a), canonical(&b));
    }
}
