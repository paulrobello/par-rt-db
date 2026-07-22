# Extra Schema Validators (record/int64/any/bytes) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add four new `FieldType` schema validators — `record`, `int64`, `any`, `bytes` — end to end (server structural/value validation, wire protocol, TypeScript client factories + inferred types), per FEATURE_MATRIX.md rank-13.

**Architecture:** Each new variant slots into the existing `FieldType` enum in `server/src/schema.rs` exactly like the ten existing variants: a wire tag via serde, a `validate_field_type` structural-validation arm, a `validate_value` document/value-validation arm, and (only where applicable) an `indexed_column_type` arm. None of the four get an indexed-column/DDL path — `record`/`any`/`bytes` aren't scalar-comparable, and `int64` is deliberately left non-indexable in this pass (documented in Task 1) — so **zero changes are needed in `ddl.rs` or `txn.rs`**: `indexed_column_type` already has a catch-all `Err` arm for non-indexable types, `FieldType`'s derived `PartialEq` already covers new variants for destructive-schema-change detection, and `txn.rs`'s `Optional`-null-unset logic already composes correctly for any inner type via `validate_value`. The TS client mirrors the wire shape 1:1 in `protocol.ts`'s `FieldTypeJson`, and adds `t.record()/t.any()/t.bytes()/t.int64()` factories in `schema.ts`.

**Tech Stack:** Rust (axum/tokio/sqlx/serde), the `base64` crate (already transitively locked at 0.22.1 — this plan promotes it to a direct dependency), TypeScript (bun/vitest/biome), no client-side runtime marshaling framework (the SDK is entirely schema-type-erased at runtime — see the int64 design note in Task 3).

## Global Constraints

- Every failure surfaces as `RtDbError::schema(...)` → wire code `SCHEMA_VIOLATION` (per CLAUDE.md's error envelope invariant) — never a raw parse/decode error string.
- No `unwrap()`/`expect()` outside `#[cfg(test)]` (CLAUDE.md invariant).
- `make checkall` (fmt-check + clippy `-D warnings` + typecheck + tests, both packages) must be fully green before this is considered done.
- Client `client/src/protocol.ts`'s `FieldTypeJson` must stay byte-identical in shape to server `schema.rs`'s `FieldType` serde output (CLAUDE.md wire-coupling invariant).
- Follow existing code style exactly: Rust helper functions match the shape of `is_valid_id`; new enum arms are added to existing exhaustive `match` blocks (compiler enforces every site is covered — this is intentional, don't add wildcard arms that would hide a missed site); TS factories match the shape of `t.array`/`t.object`.

### Design decisions locked in for this plan (do not re-litigate mid-implementation)

1. **`record(valueValidator)`** — wire shape `{"type": "record", "value": <FieldTypeJson>}`. Dynamic string-keyed map; every entry validated against `valueValidator`. Stored as `doc` jsonb, no indexed column, no restriction on key names (record keys are application data, not schema field names — the reserved-`_`-prefix and identifier-regex rules that apply to *table* field names do not apply here).
2. **`any()`** — wire shape `{"type": "any"}`. Accepts and stores any parsed JSON value (including `null`) with zero validation. TypeScript type is `unknown` (not `any`) — forces the app to narrow before use, the safer idiom, and is a pure type-level choice with no wire-format consequence.
3. **`bytes()`** — wire shape `{"type": "bytes"}`. Wire value is a JSON string that must be valid **standard base64 with required padding** (RFC 4648 §4, alphabet `A-Za-z0-9+/`, padded with `=`) — validated via the `base64` crate's `general_purpose::STANDARD` engine, which enforces canonical padding (rejects both missing and malformed padding, and rejects the URL-safe alphabet). This is the same convention `btoa`/`Buffer.from(x, 'base64')` produce by default, so app authors don't need a special encoder.
4. **`int64()`** — wire shape `{"type": "int64"}`. Wire value is a JSON **string** of decimal digits that must parse via Rust's `i64::from_str` (handles optional leading `-`, digit-only body, i64 range checking) — chosen because JSON numbers are IEEE-754 doubles and cannot exactly represent the full `i64` range past `Number.MAX_SAFE_INTEGER` (2^53−1), so a `int64` value would silently lose precision if it were a JSON number.
   - **TypeScript type: a branded string `Int64` (`string & { readonly __int64Brand: unique symbol }`), NOT a real `bigint`.** This is the key design call, worth spelling out: every existing validator in this client is **schema-type-erased at runtime** — `TxnBuilder`, `TableQuery`, and `createApi` (in `mutation.ts`/`query.ts`) never inspect field types at runtime, only at the TypeScript type level, and `client.ts`'s `send`/`handleMessage` do a bare `JSON.stringify`/`JSON.parse` with no replacer or reviver anywhere. A real `bigint` would break that pattern in two ways a branded string doesn't: (a) `JSON.stringify` throws `TypeError: Do not know how to serialize a BigInt` with no configured replacer today, so every mutate call touching an int64 field would need one; (b) converting the wire decimal string back into a `bigint` on the read path would require threading per-field schema awareness through the entire generic query-result pipeline, which no other validator does and which is out of scope for an "add validators" backlog item. A branded string costs zero runtime code on either path — exactly the same trick already used for `Id<TableName>` — while still being distinct from a plain `t.string()` at the type level. `toInt64(value: bigint | number): Int64` and `fromInt64(value: Int64): bigint` are exported as opt-in helpers for app code that wants real `bigint` arithmetic.
   - **Not indexed in this pass.** `indexed_column_type` has a catch-all error arm; `int64` simply is not given an arm there, so attempting to index an `int64` field is rejected the same way indexing `array`/`object` is today ("field type 'int64' is not indexable"). A `bigint` Postgres column + decimal-string-aware `eq_bind`/`column_bind` support is a reasonable future follow-up but adds scope (new pg_type across `indexed_column_type`, `txn.rs`'s `EqBind`/`ColBind`, and `ddl.rs`'s `backfill_expr`) beyond what this S–M-effort backlog row calls for.

---

## Task 1: Server — `FieldType` enum, validation, base64 dependency

**Files:**
- Modify: `server/Cargo.toml`
- Modify: `server/src/schema.rs`

**Interfaces:**
- Produces: `FieldType::Int64`, `FieldType::Bytes`, `FieldType::Any`, `FieldType::Record { value: Box<FieldType> }` (all `pub` via the existing `pub enum FieldType`) — consumed by Task 2 (server integration tests) and mirrored by Task 3 (client `protocol.ts`/`schema.ts`).
- Wire tags (serde `#[serde(tag = "type", rename_all = "camelCase")]`): `"int64"`, `"bytes"`, `"any"`, `"record"` (with a `"value"` field on `record`). Task 1's own tests assert these exact strings — Task 2/3 depend on them being correct.

- [ ] **Step 1: Add the `base64` dependency**

`server/Cargo.toml` already resolves `base64` transitively (locked at `0.22.1` via `Cargo.lock`) but doesn't depend on it directly. Add it as a direct dependency, right after `reqwest`:

```toml
[dependencies]
axum = { version = "0.8", features = ["ws"] }
tokio = { version = "1", features = ["full"] }
tokio-util = "0.7"
tower-http = { version = "0.6", features = ["trace", "cors"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
sqlx = { version = "0.8", features = ["runtime-tokio", "tls-rustls", "postgres", "json"] }
thiserror = "2"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
uuid = { version = "1", features = ["v7"] }
sha2 = "0.10"
hex = "0.4"
rand = "0.8"
subtle = "2"
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
base64 = "0.22"
```

Run: `cd server && cargo check`
Expected: builds cleanly (adding an already-locked transitive dependency directly does not change the resolved version — `Cargo.lock` should show no version bump for `base64`).

- [ ] **Step 2: Write the new tests (they will not compile yet — this is the "red" state for Rust)**

Open `server/src/schema.rs` and, inside the existing `#[cfg(test)] mod tests { ... }` block (ends at the file's last line, after `table_index_lookup`), add these tests. They reference `FieldType::Int64`, `FieldType::Bytes`, `FieldType::Any`, and `FieldType::Record` which don't exist yet:

```rust
    // Extra validators: record/int64/any/bytes wire tags, structural validation, and
    // document/value validation (FEATURE_MATRIX rank 13).
    #[test]
    fn new_variants_serialize_with_expected_wire_tags() {
        assert_eq!(
            serde_json::to_value(FieldType::Int64).unwrap(),
            serde_json::json!({"type": "int64"})
        );
        assert_eq!(
            serde_json::to_value(FieldType::Bytes).unwrap(),
            serde_json::json!({"type": "bytes"})
        );
        assert_eq!(
            serde_json::to_value(FieldType::Any).unwrap(),
            serde_json::json!({"type": "any"})
        );
        assert_eq!(
            serde_json::to_value(FieldType::Record {
                value: Box::new(FieldType::String)
            })
            .unwrap(),
            serde_json::json!({"type": "record", "value": {"type": "string"}})
        );
    }

    #[test]
    fn record_field_validates_structurally_and_recurses() {
        let table = TableDef {
            fields: BTreeMap::from([(
                "meta".to_string(),
                FieldType::Record {
                    value: Box::new(FieldType::Number),
                },
            )]),
            indexes: vec![],
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_ok());
    }

    #[test]
    fn record_value_validates_every_entry() {
        let ty = FieldType::Record {
            value: Box::new(FieldType::String),
        };
        assert!(validate_value(&ty, &serde_json::json!({"a": "x", "b": "y"})));
        assert!(validate_value(&ty, &serde_json::json!({})));
        assert!(!validate_value(&ty, &serde_json::json!({"a": 1})));
        assert!(!validate_value(&ty, &serde_json::json!(["a", "b"])));
    }

    #[test]
    fn any_field_accepts_every_json_value() {
        let ty = FieldType::Any;
        assert!(validate_value(&ty, &serde_json::json!("x")));
        assert!(validate_value(&ty, &serde_json::json!(42)));
        assert!(validate_value(&ty, &serde_json::json!(true)));
        assert!(validate_value(&ty, &serde_json::Value::Null));
        assert!(validate_value(&ty, &serde_json::json!([1, "a", null])));
        assert!(validate_value(&ty, &serde_json::json!({"nested": {"x": 1}})));
    }

    #[test]
    fn bytes_field_validates_standard_padded_base64() {
        let ty = FieldType::Bytes;
        assert!(validate_value(&ty, &serde_json::json!("aGVsbG8gd29ybGQ=")));
        assert!(!validate_value(&ty, &serde_json::json!("not base64!!!")));
        // Missing required padding is rejected under STANDARD (not STANDARD_NO_PAD).
        assert!(!validate_value(&ty, &serde_json::json!("aGVsbG8")));
        assert!(!validate_value(&ty, &serde_json::json!(123)));
    }

    #[test]
    fn int64_field_validates_decimal_string_in_i64_range() {
        let ty = FieldType::Int64;
        assert!(validate_value(&ty, &serde_json::json!("0")));
        assert!(validate_value(&ty, &serde_json::json!("-42")));
        assert!(validate_value(&ty, &serde_json::json!("9223372036854775807"))); // i64::MAX
        assert!(validate_value(&ty, &serde_json::json!("-9223372036854775808"))); // i64::MIN
        assert!(!validate_value(&ty, &serde_json::json!("9223372036854775808"))); // overflow
        assert!(!validate_value(&ty, &serde_json::json!("not a number")));
        assert!(!validate_value(&ty, &serde_json::json!("1.5")));
        assert!(!validate_value(&ty, &serde_json::json!(42))); // must be a string, not a JSON number
    }

    #[test]
    fn indexed_column_type_rejects_new_non_indexable_types() {
        assert!(indexed_column_type(&FieldType::Int64).is_err());
        assert!(indexed_column_type(&FieldType::Bytes).is_err());
        assert!(indexed_column_type(&FieldType::Any).is_err());
        assert!(
            indexed_column_type(&FieldType::Record {
                value: Box::new(FieldType::String)
            })
            .is_err()
        );
    }

    #[test]
    fn rejects_index_over_record_field() {
        let table = TableDef {
            fields: BTreeMap::from([(
                "meta".to_string(),
                FieldType::Record {
                    value: Box::new(FieldType::String),
                },
            )]),
            indexes: vec![IndexDef {
                name: "by_meta".to_string(),
                fields: vec!["meta".to_string()],
            }],
        };
        let schema = SchemaDef {
            tables: BTreeMap::from([("items".to_string(), table)]),
        };
        assert!(schema.validate().is_err());
    }
```

- [ ] **Step 3: Run tests to verify a compile error (the "red" state)**

Run: `cd server && cargo test --lib schema:: 2>&1 | head -30`
Expected: COMPILE ERROR — `no variant or associated item named 'Int64' found for enum 'FieldType'` (and similarly for `Bytes`/`Any`/`Record`).

- [ ] **Step 4: Implement the four new `FieldType` variants and their validation**

In `server/src/schema.rs`, add the base64 import right after the `std::collections` import:

```rust
use std::collections::{BTreeMap, HashSet};

use base64::{Engine as _, engine::general_purpose::STANDARD};

use crate::error::RtDbError;
```

Extend the `FieldType` enum (append after the existing `Object` variant):

```rust
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum FieldType {
    String,
    Number,
    Boolean,
    Null,
    Id { table: String },
    Literal { value: serde_json::Value },
    Optional { inner: Box<FieldType> },
    Union { variants: Vec<FieldType> },
    Array { element: Box<FieldType> },
    Object { fields: BTreeMap<String, FieldType> },
    Int64,
    Bytes,
    Any,
    Record { value: Box<FieldType> },
}
```

Update `validate_field_type` — add the three no-payload variants to the first arm, and a `Record` arm that recurses like `Array` does:

```rust
fn validate_field_type(ty: &FieldType) -> Result<(), RtDbError> {
    match ty {
        FieldType::String
        | FieldType::Number
        | FieldType::Boolean
        | FieldType::Null
        | FieldType::Id { .. }
        | FieldType::Int64
        | FieldType::Bytes
        | FieldType::Any => Ok(()),
        FieldType::Literal { value } => {
            if value.is_string() || value.is_number() || value.is_boolean() {
                Ok(())
            } else {
                Err(RtDbError::schema(
                    "literal value must be a string, number, or boolean",
                ))
            }
        }
        FieldType::Optional { inner } => {
            if matches!(**inner, FieldType::Optional { .. }) {
                return Err(RtDbError::schema(
                    "optional cannot directly wrap another optional",
                ));
            }
            validate_field_type(inner)
        }
        FieldType::Union { variants } => {
            if variants.is_empty() {
                return Err(RtDbError::schema("union must have at least one variant"));
            }
            for variant in variants {
                validate_field_type(variant)?;
            }
            Ok(())
        }
        FieldType::Array { element } => validate_field_type(element),
        FieldType::Object { fields } => {
            for field_type in fields.values() {
                validate_field_type(field_type)?;
            }
            Ok(())
        }
        FieldType::Record { value } => validate_field_type(value),
    }
}
```

Update `type_tag` — add the four new arms:

```rust
fn type_tag(ty: &FieldType) -> &'static str {
    match ty {
        FieldType::String => "string",
        FieldType::Number => "number",
        FieldType::Boolean => "boolean",
        FieldType::Null => "null",
        FieldType::Id { .. } => "id",
        FieldType::Literal { .. } => "literal",
        FieldType::Optional { .. } => "optional",
        FieldType::Union { .. } => "union",
        FieldType::Array { .. } => "array",
        FieldType::Object { .. } => "object",
        FieldType::Int64 => "int64",
        FieldType::Bytes => "bytes",
        FieldType::Any => "any",
        FieldType::Record { .. } => "record",
    }
}
```

Add two new helper functions right after `is_valid_id`:

```rust
fn is_valid_int64(value: &serde_json::Value) -> bool {
    match value.as_str() {
        Some(s) => s.parse::<i64>().is_ok(),
        None => false,
    }
}

fn is_valid_base64(value: &serde_json::Value) -> bool {
    match value.as_str() {
        Some(s) => STANDARD.decode(s).is_ok(),
        None => false,
    }
}
```

Update `validate_value` — add the four new arms after `Object`:

```rust
pub fn validate_value(ty: &FieldType, value: &serde_json::Value) -> bool {
    match ty {
        FieldType::String => value.is_string(),
        FieldType::Number => value.is_number(),
        FieldType::Boolean => value.is_boolean(),
        FieldType::Null => value.is_null(),
        FieldType::Id { .. } => is_valid_id(value),
        FieldType::Literal { value: expected } => value == expected,
        FieldType::Optional { inner } => value.is_null() || validate_value(inner, value),
        FieldType::Union { variants } => variants
            .iter()
            .any(|variant| validate_value(variant, value)),
        FieldType::Array { element } => match value.as_array() {
            Some(items) => items.iter().all(|item| validate_value(element, item)),
            None => false,
        },
        FieldType::Object { fields } => match value.as_object() {
            Some(obj) => {
                if obj.keys().any(|key| !fields.contains_key(key)) {
                    return false;
                }
                fields
                    .iter()
                    .all(|(field_name, field_type)| match obj.get(field_name) {
                        Some(field_value) => validate_value(field_type, field_value),
                        None => matches!(field_type, FieldType::Optional { .. }),
                    })
            }
            None => false,
        },
        FieldType::Int64 => is_valid_int64(value),
        FieldType::Bytes => is_valid_base64(value),
        FieldType::Any => true,
        FieldType::Record { value: value_ty } => match value.as_object() {
            Some(obj) => obj.values().all(|v| validate_value(value_ty, v)),
            None => false,
        },
    }
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd server && cargo test --lib schema::`
Expected: PASS — all existing schema.rs tests plus the 7 new ones added in Step 2.

- [ ] **Step 6: Run the full server check**

Run: `cd server && cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo check --all-targets`
Expected: no formatting diffs beyond this task's edits, zero clippy warnings, clean compile.

- [ ] **Step 7: Commit**

```bash
git add server/Cargo.toml server/Cargo.lock server/src/schema.rs
git commit -m "feat(server): add record/int64/any/bytes schema validators"
```

---

## Task 2: Server — integration tests (DDL generation + insert/patch/query round trip)

**Depends on:** Task 1 (uses `FieldType::Record`/`Int64`/`Bytes`/`Any` and their wire tags).

**Files:**
- Create: `server/tests/schema_validators_test.rs`

**Interfaces:**
- Consumes: `rtdb_server::db::create_database`, `rtdb_server::ddl::{push_schema, pg_schema, pg_table}`, `rtdb_server::schema::SchemaDef`, `rtdb_server::txn::{Step, Transaction, execute_txn}`, `rtdb_server::query::{Query, QueryResult, execute_query}`, `rtdb_server::error::ErrorCode`, and `common::test_state` (existing helper in `server/tests/common/mod.rs` — do not modify that file; this task's fixtures are local to the new file, matching how `txn_test.rs`/`query_test.rs` each keep their own local `doc()`/schema helpers).

- [ ] **Step 1: Write the test file**

Create `server/tests/schema_validators_test.rs`:

```rust
mod common;

use common::test_state;
use rtdb_server::db;
use rtdb_server::ddl;
use rtdb_server::error::ErrorCode;
use rtdb_server::query::{Query, QueryResult, execute_query};
use rtdb_server::schema::SchemaDef;
use rtdb_server::txn::{Step, Transaction, execute_txn};

fn doc(value: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    value.as_object().expect("json object").clone()
}

/// A `widgets` table exercising all four new field types, none indexed.
fn widgets_schema_json() -> serde_json::Value {
    serde_json::json!({"tables":{
      "widgets":{
        "fields":{
          "tags": {"type": "record", "value": {"type": "string"}},
          "payload": {"type": "any"},
          "blob": {"type": "bytes"},
          "big": {"type": "int64"}
        },
        "indexes": []
      }
    }})
}

fn valid_widget_doc() -> serde_json::Map<String, serde_json::Value> {
    doc(serde_json::json!({
        "tags": {"a": "x", "b": "y"},
        "payload": {"anything": ["goes", 1, true, null], "n": 7},
        "blob": "aGVsbG8gd29ybGQ=",
        "big": "9223372036854775807"
    }))
}

async fn fresh_widgets_db(state: &std::sync::Arc<rtdb_server::AppState>) -> (String, SchemaDef) {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name)
        .await
        .expect("create fresh database");
    let schema: SchemaDef = serde_json::from_value(widgets_schema_json())
        .expect("parse widgets schema fixture");
    let applied = ddl::push_schema(&state.pool, &name, schema)
        .await
        .expect("push widgets schema");
    (name, applied)
}

// (a) DDL generation: none of the four new types get an indexed/typed column.
#[tokio::test]
async fn push_schema_creates_table_with_no_extra_typed_columns() -> anyhow::Result<()> {
    let state = test_state().await;
    let (db_name, _schema) = fresh_widgets_db(&state).await;

    let pg_schema_name = ddl::pg_schema(&db_name);
    let table_ident = ddl::pg_table("widgets");
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2",
    )
    .bind(&pg_schema_name)
    .bind(&table_ident)
    .fetch_all(&state.pool)
    .await?;
    let mut columns: Vec<String> = rows.into_iter().map(|(c,)| c).collect();
    columns.sort();
    assert_eq!(columns, vec!["created_at", "doc", "id", "version"]);
    Ok(())
}

// (b) Attempting to index any of the four new types is rejected at push_schema time.
#[tokio::test]
async fn push_schema_rejects_index_over_int64_field() -> anyhow::Result<()> {
    let state = test_state().await;
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name).await?;

    let mut json = widgets_schema_json();
    json["tables"]["widgets"]["indexes"] =
        serde_json::json!([{"name": "by_big", "fields": ["big"]}]);
    let schema: SchemaDef = serde_json::from_value(json)?;

    let err = ddl::push_schema(&state.pool, &name, schema)
        .await
        .expect_err("indexing an int64 field must be rejected");
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    Ok(())
}

// (c) insert rejects an invalid value for each new type.
#[tokio::test]
async fn insert_rejects_invalid_values_for_each_new_type() -> anyhow::Result<()> {
    let state = test_state().await;
    let (db_name, schema) = fresh_widgets_db(&state).await;

    let cases: [(&str, serde_json::Value); 3] = [
        ("tags", serde_json::json!(["not", "an", "object"])),
        ("blob", serde_json::json!("not base64!")),
        ("big", serde_json::json!("not a number")),
    ];
    for (field, bad_value) in cases {
        let mut fields = valid_widget_doc();
        fields.insert(field.to_string(), bad_value);
        let err = execute_txn(
            &state.pool,
            &db_name,
            &schema,
            &Transaction {
                steps: vec![Step::Insert {
                    table: "widgets".to_string(),
                    doc: fields,
                }],
            },
        )
        .await
        .expect_err("invalid value must be a schema violation");
        assert_eq!(err.code, ErrorCode::SchemaViolation, "field {field}");
    }
    Ok(())
}

// (d) document round-trips through insert, patch, and query unchanged.
#[tokio::test]
async fn document_round_trips_through_insert_patch_and_query() -> anyhow::Result<()> {
    let state = test_state().await;
    let (db_name, schema) = fresh_widgets_db(&state).await;

    let outcome = execute_txn(
        &state.pool,
        &db_name,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "widgets".to_string(),
                doc: valid_widget_doc(),
            }],
        },
    )
    .await?;
    let id = outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string();

    execute_txn(
        &state.pool,
        &db_name,
        &schema,
        &Transaction {
            steps: vec![Step::Patch {
                table: "widgets".to_string(),
                id: id.clone(),
                fields: doc(serde_json::json!({
                    "big": "-9223372036854775808",
                    "payload": {"changed": true}
                })),
            }],
        },
    )
    .await?;

    let result = execute_query(
        &state.pool,
        &db_name,
        &schema,
        &Query {
            table: "widgets".to_string(),
            get: Some(id.clone()),
            index: None,
            eq: vec![],
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
        },
    )
    .await?;

    let found = match result {
        QueryResult::Doc(Some(d)) => d,
        other => panic!("expected Doc(Some), got {other:?}"),
    };
    assert_eq!(found["tags"], serde_json::json!({"a": "x", "b": "y"}));
    assert_eq!(found["blob"], serde_json::json!("aGVsbG8gd29ybGQ="));
    assert_eq!(found["big"], serde_json::json!("-9223372036854775808"));
    assert_eq!(found["payload"], serde_json::json!({"changed": true}));
    Ok(())
}
```

- [ ] **Step 2: Bring up the dev database**

Run: `make dev-db-up` (from repo root)
Expected: Postgres container is up and healthy.

- [ ] **Step 3: Run the new integration test file**

Run: `cd server && cargo test --test schema_validators_test`
Expected: PASS — 4 tests (`push_schema_creates_table_with_no_extra_typed_columns`, `push_schema_rejects_index_over_int64_field`, `insert_rejects_invalid_values_for_each_new_type`, `document_round_trips_through_insert_patch_and_query`).

If a test fails, diagnose before adjusting: a wrong column-name assertion likely means the actual Postgres column set differs (re-check with a manual `psql`/`information_schema` query against the test db before changing the assertion); a wrong error-code assertion likely means validation didn't reject what was expected (re-check Task 1's `validate_value` arm for that type).

- [ ] **Step 4: Run full server check**

Run: `cd server && cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test`
Expected: no formatting diffs beyond this task's file, zero clippy warnings, full suite green.

- [ ] **Step 5: Commit**

```bash
git add server/tests/schema_validators_test.rs
git commit -m "test(server): cover record/int64/any/bytes DDL and insert/patch/query round trip"
```

---

## Task 3: Client — `protocol.ts` wire shape + `schema.ts` factories

**Files:**
- Modify: `client/src/protocol.ts`
- Modify: `client/src/schema.ts`

**Interfaces:**
- Produces: `t.record()`, `t.any()`, `t.bytes()`, `t.int64()` (added to the existing exported `t` object), the exported type `Int64`, and exported helpers `toInt64`/`fromInt64` — consumed by Task 4 (client tests) and any future application code.
- `FieldTypeJson` gains four new members mirroring `server/src/schema.rs`'s wire tags exactly (`"int64"`, `"bytes"`, `"any"`, `{"type": "record", "value": FieldTypeJson}`) — this is the wire-coupling contract from CLAUDE.md.

- [ ] **Step 1: Extend `FieldTypeJson` in `client/src/protocol.ts`**

Find:

```ts
/** Mirrors server `schema::FieldType` (tag `type`). */
export type FieldTypeJson =
  | { type: "string" }
  | { type: "number" }
  | { type: "boolean" }
  | { type: "null" }
  | { type: "id"; table: string }
  | { type: "literal"; value: string | number | boolean }
  | { type: "optional"; inner: FieldTypeJson }
  | { type: "union"; variants: FieldTypeJson[] }
  | { type: "array"; element: FieldTypeJson }
  | { type: "object"; fields: Record<string, FieldTypeJson> };
```

Replace with:

```ts
/** Mirrors server `schema::FieldType` (tag `type`). */
export type FieldTypeJson =
  | { type: "string" }
  | { type: "number" }
  | { type: "boolean" }
  | { type: "null" }
  | { type: "id"; table: string }
  | { type: "literal"; value: string | number | boolean }
  | { type: "optional"; inner: FieldTypeJson }
  | { type: "union"; variants: FieldTypeJson[] }
  | { type: "array"; element: FieldTypeJson }
  | { type: "object"; fields: Record<string, FieldTypeJson> }
  | { type: "int64" }
  | { type: "bytes" }
  | { type: "any" }
  | { type: "record"; value: FieldTypeJson };
```

- [ ] **Step 2: Add the `Int64` branded type and helpers in `client/src/schema.ts`**

Find the `Id` branded type near the top of the file:

```ts
/** Branded id string. `Id<"projects">` is assignable to `string` but distinct across tables. */
export type Id<TableName extends string> = string & { readonly __idBrand: TableName };
```

Insert immediately after it:

```ts
/**
 * Branded decimal-string int64. The wire value is a JSON string of canonical
 * decimal digits (whatever `i64::from_str` on the server accepts) — JSON has
 * no 64-bit integer type, and a JS `number` cannot exactly represent the full
 * `i64` range past `Number.MAX_SAFE_INTEGER`. Branded rather than a real
 * `bigint` because this SDK is entirely schema-type-erased at runtime (see
 * `t.int64` below): a `bigint` would need a `JSON.stringify` replacer on every
 * write and schema-aware result marshaling on every read, which no other
 * validator needs today. Use `toInt64`/`fromInt64` to convert at the edges.
 */
export type Int64 = string & { readonly __int64Brand: unique symbol };

export function toInt64(value: bigint | number): Int64 {
  return String(value) as Int64;
}

export function fromInt64(value: Int64): bigint {
  return BigInt(value);
}
```

- [ ] **Step 3: Add the four new `t.*` factories**

Find:

```ts
  object: <S extends Record<string, Validator<unknown, boolean>>>(
    fields: S,
  ): Validator<{ [K in keyof S]: Infer<S[K]> }> =>
    makeValidator({ type: "object", fields: fieldsToJson(fields) }),
};
```

Replace with:

```ts
  object: <S extends Record<string, Validator<unknown, boolean>>>(
    fields: S,
  ): Validator<{ [K in keyof S]: Infer<S[K]> }> =>
    makeValidator({ type: "object", fields: fieldsToJson(fields) }),
  record: <T>(value: Validator<T, boolean>): Validator<Record<string, T>> =>
    makeValidator({ type: "record", value: value.json }),
  any: (): Validator<unknown> => makeValidator({ type: "any" }),
  bytes: (): Validator<string> => makeValidator({ type: "bytes" }),
  int64: (): Validator<Int64> => makeValidator({ type: "int64" }),
};
```

- [ ] **Step 4: Typecheck and lint**

Run: `cd client && bun run typecheck && bun run lint`
Expected: clean — no errors. (`biome.json` already has `noExplicitAny` off for this file's pre-existing generic positions; this task introduces no new `any` types — `t.any()` returns `Validator<unknown>`.)

- [ ] **Step 5: Commit**

```bash
git add client/src/protocol.ts client/src/schema.ts
git commit -m "feat(client): add record/int64/any/bytes t.* validators"
```

---

## Task 4: Client — tests for the new factories and inferred types

**Depends on:** Task 3.

**Files:**
- Modify: `client/tests/schema.test.ts`
- Modify: `client/tests/schema.types.test.ts`

**Interfaces:**
- Consumes: `t.record`, `t.any`, `t.bytes`, `t.int64`, `Int64`, `toInt64`, `fromInt64` from Task 3.

- [ ] **Step 1: Add runtime factory tests to `client/tests/schema.test.ts`**

Find the closing of the existing `describe("schema builder", ...)` block:

```ts
  it("serializes nested object and id-array field types", () => {
    const s = defineSchema({
      t1: defineTable({
        meta: t.object({ a: t.string(), b: t.optional(t.number()) }),
        refs: t.array(t.id("t1")),
      }),
    });
    expect(s.toJSON().tables.t1.fields).toEqual({
      meta: {
        type: "object",
        fields: { a: { type: "string" }, b: { type: "optional", inner: { type: "number" } } },
      },
      refs: { type: "array", element: { type: "id", table: "t1" } },
    });
  });
});
```

Replace with (adds two new tests, closing the `describe` after them):

```ts
  it("serializes nested object and id-array field types", () => {
    const s = defineSchema({
      t1: defineTable({
        meta: t.object({ a: t.string(), b: t.optional(t.number()) }),
        refs: t.array(t.id("t1")),
      }),
    });
    expect(s.toJSON().tables.t1.fields).toEqual({
      meta: {
        type: "object",
        fields: { a: { type: "string" }, b: { type: "optional", inner: { type: "number" } } },
      },
      refs: { type: "array", element: { type: "id", table: "t1" } },
    });
  });

  it("serializes record/any/bytes/int64 field types", () => {
    const s = defineSchema({
      widgets: defineTable({
        tags: t.record(t.string()),
        payload: t.any(),
        blob: t.bytes(),
        big: t.int64(),
      }),
    });
    expect(s.toJSON().tables.widgets.fields).toEqual({
      tags: { type: "record", value: { type: "string" } },
      payload: { type: "any" },
      blob: { type: "bytes" },
      big: { type: "int64" },
    });
  });

  it("serializes a record of optional numbers", () => {
    const s = defineSchema({
      widgets: defineTable({ counts: t.record(t.optional(t.number())) }),
    });
    expect(s.toJSON().tables.widgets.fields).toEqual({
      counts: { type: "record", value: { type: "optional", inner: { type: "number" } } },
    });
  });
});
```

- [ ] **Step 2: Run the runtime tests**

Run: `cd client && bunx vitest run tests/schema.test.ts`
Expected: PASS — all existing tests plus the 2 new ones.

- [ ] **Step 3: Add type-level tests to `client/tests/schema.types.test.ts`**

Replace the full file content with:

```ts
import { describe, expectTypeOf, it } from "vitest";
import type { Doc, Id, Int64, WithoutSystemFields } from "../src/schema.js";
import { defineSchema, defineTable, fromInt64, t, toInt64 } from "../src/schema.js";

const schema = defineSchema({
  projects: defineTable({
    name: t.string(),
    status: t.union(t.literal("active"), t.literal("paused")),
    order: t.number(),
    archived: t.optional(t.boolean()),
  }).index("by_name", ["name"]),
  items: defineTable({
    projectId: t.id("projects"),
    title: t.string(),
  }).index("by_project", ["projectId"]),
  widgets: defineTable({
    tags: t.record(t.string()),
    payload: t.any(),
    blob: t.bytes(),
    big: t.int64(),
  }),
});

type Project = Doc<typeof schema, "projects">;
type NewProject = WithoutSystemFields<typeof schema, "projects">;
type Widget = Doc<typeof schema, "widgets">;

describe("schema type inference", () => {
  it("derives read docs with system fields and correct optionality", () => {
    expectTypeOf<Project["name"]>().toEqualTypeOf<string>();
    expectTypeOf<Project["status"]>().toEqualTypeOf<"active" | "paused">();
    expectTypeOf<Project["order"]>().toEqualTypeOf<number>();
    // optional field -> optional key
    expectTypeOf<Project>().toHaveProperty("archived");
    expectTypeOf<Project["_id"]>().toEqualTypeOf<Id<"projects">>();
    expectTypeOf<Project["_creationTime"]>().toEqualTypeOf<number>();
    expectTypeOf<Project["_version"]>().toEqualTypeOf<number>();
  });

  it("excludes system fields from insert input", () => {
    expectTypeOf<NewProject>().not.toHaveProperty("_id");
    expectTypeOf<NewProject>().not.toHaveProperty("_creationTime");
  });

  it("brands ids per table", () => {
    expectTypeOf<Doc<typeof schema, "items">["projectId"]>().toEqualTypeOf<Id<"projects">>();
  });

  it("infers record/any/bytes/int64 field types", () => {
    expectTypeOf<Widget["tags"]>().toEqualTypeOf<Record<string, string>>();
    expectTypeOf<Widget["payload"]>().toEqualTypeOf<unknown>();
    expectTypeOf<Widget["blob"]>().toEqualTypeOf<string>();
    expectTypeOf<Widget["big"]>().toEqualTypeOf<Int64>();
  });

  it("Int64 is a branded string convertible via toInt64/fromInt64", () => {
    expectTypeOf<Int64>().toMatchTypeOf<string>();
    expectTypeOf(toInt64).returns.toEqualTypeOf<Int64>();
    expectTypeOf(fromInt64).parameter(0).toEqualTypeOf<Int64>();
    expectTypeOf(fromInt64).returns.toEqualTypeOf<bigint>();
  });
});
```

- [ ] **Step 4: Typecheck (this is the real gate for this file — `expectTypeOf` assertions are checked by `tsc`, not by running the test)**

Run: `cd client && bun run typecheck`
Expected: clean — no type errors.

- [ ] **Step 5: Run full client check**

Run: `cd client && bun run fmt && bun run lint && bun run typecheck && bun run test`
Expected: no formatting diffs beyond this task's files, zero lint errors, clean typecheck, full test suite green.

- [ ] **Step 6: Commit**

```bash
git add client/tests/schema.test.ts client/tests/schema.types.test.ts
git commit -m "test(client): cover record/int64/any/bytes t.* factories and inferred types"
```

---

## Task 5: Update `FEATURE_MATRIX.md` rank-13 row

**Depends on:** Tasks 1–4 (describes what was actually built).

**Files:**
- Modify: `FEATURE_MATRIX.md`

- [ ] **Step 1: Replace the rank-13 row**

Find (in the `## 2. Gap matrix` table):

```
| 13 | 1 | Extra validators: `record`, `int64`, `any`, `bytes` | ✅ | ❌ | Low–Med | S–M | `record` (dynamic string-keyed maps) is the one apps actually hit; jsonb storage needs no DDL change. `int64` needs a bigint wire convention; `bytes` base64. |
```

Replace with:

```
| 13 | 1 | Extra validators: `record`, `int64`, `any`, `bytes` | ✅ | ✅ | Low–Med | S–M | Implemented — four new `FieldType` variants (`schema.rs`): `record` (dynamic string-keyed map, each entry validated against its `value` validator), `any` (accepts and stores any JSON value with zero validation), `bytes` (a JSON string validated as standard base64 with required padding, RFC 4648 §4), and `int64` (a JSON string of canonical decimal digits validated via `i64::from_str` — chosen because JSON numbers are IEEE-754 doubles and cannot exactly represent the full `i64` range past `Number.MAX_SAFE_INTEGER`). None of the four get a DDL-indexed column — `record`/`any`/`bytes` aren't scalar-comparable, and `int64` is deliberately left non-indexable in this pass. Mirrored end-to-end: `protocol.ts`'s `FieldTypeJson` and the client's `t.record()/t.any()/t.bytes()/t.int64()` factories, with schema/DDL/round-trip coverage in `schema_validators_test.rs` and factory/type coverage in `schema.test.ts`/`schema.types.test.ts`. **`int64` wire convention:** decimal-string on the wire, typed as a branded `Int64` string (not a real `bigint`) on the TS client — the client is entirely schema-type-erased at runtime (no codegen, no marshaling for any existing validator), so a real `bigint` would need a `JSON.stringify` replacer on writes and schema-aware result marshaling on reads that no other type needs; `Int64` instead follows the same zero-runtime-cost branded-string pattern already used for `Id<TableName>`, with `toInt64()`/`fromInt64()` helpers for apps that want actual `bigint` arithmetic. |
```

- [ ] **Step 2: Commit**

```bash
git add FEATURE_MATRIX.md
git commit -m "docs(feature-matrix): mark rank-13 extra validators implemented"
```

---

## Final Step (orchestrator, not a subagent task): full-repo verification, merge, and CLAUDE.md check

After Tasks 1–5 are all committed and individually green:

1. From the repo root, run the literal `make checkall` and fix anything that only surfaces at the whole-repo level (e.g. cross-package formatting drift).
2. If this plan was executed in a worktree or feature branch, rebase it onto the latest `main`, fast-forward merge locally, and clean up the worktree/branch.
3. Do a final holistic read of `server/CLAUDE.md`'s schema/type-model description (the "The data pipeline: schema → DDL → txn/query" section) to confirm it still accurately reflects the validator set after this change, correcting only that section if it's now stale — leave the rest of CLAUDE.md untouched.
4. Do not push. Do not touch the kanban board (the user is tracking that separately).
