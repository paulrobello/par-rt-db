# par-rt-db Rust Client — Core + HTTP (Plan 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `par-rt-db-client`, a Rust crate with the wire-contract types, the schema/query/mutation DSL, and a typed HTTP client (query/mutate/auth-me) — the par-hack-M2-unblocking critical path.

**Architecture:** Approach C (hybrid): runtime builders serialize to par-rt-db's exact JSON wire shapes, and query results deserialize generically into caller-supplied serde structs (`run::<T>(query)`). Wire types are re-declared byte-identical to `server/src/protocol.rs` and `client/src/protocol.ts` (the third implementation of the contract), locked by round-trip parity tests. Crate lives in `rust-client/`, feature-flagged; this plan ships the always-on core plus the `http` feature.

**Tech Stack:** Rust, edition 2024, stable toolchain. `serde`/`serde_json`, `thiserror`, `base64`, `reqwest` (rustls-tls). Dev: `tokio`, `wiremock`.

## Global Constraints

- **Wire contract is load-bearing.** Re-declared types must serialize **byte-identically** to the server. Discriminator and casing rules differ per type — copy them exactly:
  - `ClientMessage` / `ServerMessage`: `#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]`. `ClientMessage` is also `deny_unknown_fields`.
  - `Query`: **no `rename_all`** (snake_case field names on the wire), `deny_unknown_fields`.
  - `Order`: `#[serde(rename_all = "lowercase")]` → `"asc"`/`"desc"`.
  - `Paginate`: `#[serde(rename_all = "camelCase", deny_unknown_fields)]` → `cursor`/`numItems`.
  - `Step`: `#[serde(tag = "op", rename_all = "camelCase", deny_unknown_fields)]`.
  - `FieldType`: `#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]`.
  - `ErrorCode`: `#[serde(rename_all = "SCREAMING_SNAKE_CASE")]`.
- **Error envelope** is always `{ "code": "<SCREAMING_SNAKE_CASE>", "message": "..." }`. Codes: `UNAUTHORIZED, FORBIDDEN, NOT_FOUND, SCHEMA_VIOLATION, PRECONDITION_FAILED, BAD_REQUEST, INTERNAL`.
- **No `unwrap`/`expect`/`panic`** outside `#[cfg(test)]`. Zero clippy warnings under `-D warnings`. `[lints.rust] warnings = "deny"` (matches `par-hack`).
- **HTTP auth** is always `Authorization: Bearer <token>`. The `/api/query` body is `{"db","query"}` → `{"result"}`; `/api/mutate` body is `{"db","txn","idempotencyKey"?}` → `{"results"}`; `GET /auth/me` → `{"user"}` (session token only; machine tokens get 401).
- **Deterministic serialization**: use `BTreeMap` (not `HashMap`) for schema field/table maps so parity tests and (later) WS dedup are stable.
- **Tests that need a live server are opt-in** (gated by `RTDB_TEST_SERVER_URL` + `RTDB_TEST_ADMIN_KEY`) and `#[ignore]` by default, mirroring the TS `tests/integration/**` model. Every other test is hermetic.
- **`int64` and `Id` are JSON strings** on the wire, never numbers.
- The dev/test Postgres is `make dev-db-up` (loopback `127.0.0.1:55434`) — **required before any live-server integration test run**.

---

### Task 1: Scaffold the crate

**Files:**
- Create: `rust-client/Cargo.toml`
- Create: `rust-client/src/lib.rs`
- Modify: `Makefile` (root) — add `rust-client` cargo lines to `build`, `fmt`, `fmt-check`, `lint`, `typecheck`, `test`
- Test: `cargo` build + root `make typecheck`

**Interfaces:**
- Produces: an empty, compiling crate `par-rt-db-client` with feature gates and module stubs that later tasks fill in.

- [ ] **Step 1: Create `rust-client/Cargo.toml`**

```toml
[package]
name = "par-rt-db-client"
version = "0.1.0"
edition = "2024"
license = "MIT"
authors = ["Paul Robello <user@example.com>"]
description = "Rust client for par-rt-db (HTTP + reactive WebSocket)"
repository = "https://github.com/paulrobello/par-rt-db"

[features]
default = ["http"]
http = ["dep:reqwest"]
ws = ["dep:tokio", "dep:tokio-tungstenite", "dep:futures-util"]
admin = ["dep:reqwest"]

[lints.rust]
warnings = "deny"

[lints.clippy]
all = { level = "warn", priority = -1 }

[dependencies]
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "2"
base64 = "0.22"
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"], optional = true }
tokio = { version = "1", features = ["macros", "rt", "sync", "time"], optional = true }
tokio-tungstenite = { version = "0.26", features = ["rustls-tls-webpki-roots"], optional = true }
futures-util = { version = "0.3", optional = true }

[dev-dependencies]
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
wiremock = "0.6"
```

- [ ] **Step 2: Create `rust-client/src/lib.rs`** (declares modules only — re-exports are added in Task 9 once the symbols exist)

```rust
//! Rust client for par-rt-db. See `docs/superpowers/specs/2026-07-22-rust-client-design.md`.

pub mod error;
pub mod schema;
pub mod query;
pub mod cursor;
pub mod mutation;
pub mod wire;

#[cfg(feature = "http")]
pub mod http;
```

(Do **not** add `pub use` re-exports yet — the module stubs are empty, so re-exporting their symbols would fail to compile. The public API is reached as `par_rt_db_client::error::RtDbError` etc. until Task 9 adds root re-exports.)

- [ ] **Step 3: Add the stub modules so `lib.rs` compiles**

Create each as an empty file with just a doc comment:
`rust-client/src/error.rs`, `schema.rs`, `query.rs`, `cursor.rs`, `mutation.rs`, `wire.rs`, `http.rs` — each containing only `//! (module doc)` for now. Later tasks fill them.

- [ ] **Step 4: Extend the root `Makefile`**

Add a `cd rust-client && cargo ...` line to each of these targets (append after the existing `client` line in each):

- `build`: add `	cd rust-client && cargo build --all-features`
- `fmt`: add `	cd rust-client && cargo fmt --all`
- `fmt-check`: add `	cd rust-client && cargo fmt --all -- --check`
- `lint`: add `	cd rust-client && cargo clippy --all-targets --all-features -- -D warnings`
- `typecheck`: add `	cd rust-client && cargo check --all-targets --all-features`
- `test`: add `	cd rust-client && cargo test --all-features`

- [ ] **Step 5: Verify it builds clean under all feature combos**

Run:
```bash
cd rust-client && cargo build --all-features && cargo build --no-default-features && cargo clippy --all-targets --all-features -- -D warnings
```
Expected: compiles with no warnings; clippy clean.

Then from the repo root:
```bash
make typecheck
```
Expected: `cargo check` runs for `server/`, `client/`, **and** `rust-client/` with no errors.

- [ ] **Step 6: Commit**

```bash
git add rust-client Makefile
git commit -m "feat(rust-client): scaffold par-rt-db-client crate"
```

---

### Task 2: Error types (`error.rs`)

**Files:**
- Modify: `rust-client/src/error.rs`
- Test: `rust-client/src/error.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Produces: `ErrorCode` (serde `SCREAMING_SNAKE_CASE`), `ErrorEnvelope { code, message }`, `RtDbError { code, message }` (serializes to `{code, message}`, is a `thiserror::Error`), `RtDbError::new`, `RtDbError::from_envelope`, and `retry_on_precondition`.

- [ ] **Step 1: Write the failing tests**

Append to `rust-client/src/error.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_code_serializes_screaming_snake() {
        assert_eq!(
            serde_json::to_value(ErrorCode::PreconditionFailed).unwrap(),
            serde_json::json!("PRECONDITION_FAILED")
        );
        assert_eq!(
            serde_json::to_value(ErrorCode::SchemaViolation).unwrap(),
            serde_json::json!("SCHEMA_VIOLATION")
        );
    }

    #[test]
    fn error_code_round_trips_all_variants() {
        let all = [
            ErrorCode::Unauthorized,
            ErrorCode::Forbidden,
            ErrorCode::NotFound,
            ErrorCode::SchemaViolation,
            ErrorCode::PreconditionFailed,
            ErrorCode::BadRequest,
            ErrorCode::Internal,
        ];
        for c in all {
            let v = serde_json::to_value(c).unwrap();
            let back: ErrorCode = serde_json::from_value(v).unwrap();
            assert_eq!(c, back);
        }
    }

    #[test]
    fn rtdb_error_serializes_envelope() {
        let e = RtDbError::new(ErrorCode::NotFound, "missing doc");
        assert_eq!(
            serde_json::to_value(&e).unwrap(),
            serde_json::json!({"code":"NOT_FOUND","message":"missing doc"})
        );
    }

    #[test]
    fn rtdb_error_deserializes_envelope() {
        let e: RtDbError =
            serde_json::from_value(serde_json::json!({"code":"BAD_REQUEST","message":"bad"})).
                unwrap();
        assert_eq!(e.code, ErrorCode::BadRequest);
        assert_eq!(e.message, "bad");
    }

    #[tokio::test]
    async fn retry_retries_only_on_precondition() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let attempts = AtomicU32::new(0);
        let f = || {
            let n = attempts.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 2 {
                    Err(RtDbError::new(ErrorCode::PreconditionFailed, "conflict"))
                } else {
                    Ok(7_i64)
                }
            }
        };
        let got: i64 = retry_on_precondition(f, 5).await.unwrap();
        assert_eq!(got, 7);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retry_does_not_retry_other_errors() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let attempts = AtomicU32::new(0);
        let f = || {
            attempts.fetch_add(1, Ordering::SeqCst);
            async move { Err::<i64, _>(RtDbError::new(ErrorCode::NotFound, "x")) }
        };
        let err = retry_on_precondition::<_, _, i64>(f, 5).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd rust-client && cargo test --lib error`
Expected: FAIL — `ErrorCode` / `RtDbError` / `retry_on_precondition` not defined.

- [ ] **Step 3: Implement `error.rs`**

Replace the file body (above the tests) with:

```rust
//! Error envelope and retry helper. Mirrors the server's `{code, message}` wire shape.

use serde::{Deserialize, Serialize};
use std::future::Future;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    Unauthorized,
    Forbidden,
    NotFound,
    SchemaViolation,
    PreconditionFailed,
    BadRequest,
    Internal,
}

/// Raw `{code, message}` as it appears on the wire (HTTP body / WS error frame).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorEnvelope {
    pub code: ErrorCode,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
#[error("{message}")]
pub struct RtDbError {
    pub code: ErrorCode,
    pub message: String,
}

impl RtDbError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self { code, message: message.into() }
    }

    pub fn from_envelope(env: ErrorEnvelope) -> Self {
        Self { code: env.code, message: env.message }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Internal, message)
    }
}

/// Retries a read-modify-write closure only on `PRECONDITION_FAILED`.
/// `retries` is the number of retries after the first attempt.
pub async fn retry_on_precondition<F, Fut, T>(mut f: F, retries: u32) -> Result<T, RtDbError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, RtDbError>>,
{
    let mut left = retries;
    loop {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if e.code == ErrorCode::PreconditionFailed && left > 0 => {
                left -= 1;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd rust-client && cargo test --lib error`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add rust-client/src/error.rs
git commit -m "feat(rust-client): error envelope, codes, and precondition retry"
```

---

### Task 3: Schema DSL (`schema.rs`)

**Files:**
- Modify: `rust-client/src/schema.rs`
- Test: `rust-client/src/schema.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Produces: `FieldType` (serde `tag="type"`, camelCase, deny_unknown_fields), `IndexDef { name, fields }`, `TableDef { fields, indexes? }`, `SchemaDef { tables }`, and `Schema`/`Table` builders (`Schema::builder().table(name, |t| t.field(..).index(..))`).

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn field_type_wire_tags() {
        assert_eq!(serde_json::to_value(FieldType::String).unwrap(), json!({"type":"string"}));
        assert_eq!(
            serde_json::to_value(FieldType::Id { table: "projects".into() }).unwrap(),
            json!({"type":"id","table":"projects"})
        );
        assert_eq!(
            serde_json::to_value(FieldType::Optional { inner: Box::new(FieldType::Boolean) }).unwrap(),
            json!({"type":"optional","inner":{"type":"boolean"}})
        );
        assert_eq!(
            serde_json::to_value(FieldType::Literal { value: json!("active") }).unwrap(),
            json!({"type":"literal","value":"active"})
        );
        assert_eq!(serde_json::to_value(FieldType::Int64).unwrap(), json!({"type":"int64"}));
    }

    #[test]
    fn builder_serializes_full_schema() {
        let schema = Schema::builder()
            .table(
                "projects",
                Table::new()
                    .field("name", FieldType::String)
                    .field("archived", FieldType::optional(FieldType::Boolean))
                    .index("by_name", &["name"]),
            )
            .table(
                "items",
                Table::new()
                    .field("projectId", FieldType::id("projects"))
                    .field("title", FieldType::String)
                    .field("status", FieldType::union([FieldType::literal("backlog"), FieldType::literal("done")]))
                    .field("order", FieldType::Number)
                    .index("by_project", &["projectId"])
                    .index("by_project_and_title", &["projectId", "title"]),
            )
            .build();
        let v = serde_json::to_value(&schema).unwrap();
        assert_eq!(
            v,
            json!({
                "tables": {
                    "projects": {
                        "fields": {
                            "name": {"type":"string"},
                            "archived": {"type":"optional","inner":{"type":"boolean"}}
                        },
                        "indexes": [{"name":"by_name","fields":["name"]}]
                    },
                    "items": {
                        "fields": {
                            "projectId": {"type":"id","table":"projects"},
                            "title": {"type":"string"},
                            "status": {"type":"union","variants":[{"type":"literal","value":"backlog"},{"type":"literal","value":"done"}]},
                            "order": {"type":"number"}
                        },
                        "indexes": [
                            {"name":"by_project","fields":["projectId"]},
                            {"name":"by_project_and_title","fields":["projectId","title"]}
                        ]
                    }
                }
            })
        );
    }

    #[test]
    fn table_with_no_indexes_omits_key() {
        let schema = Schema::builder()
            .table("solo", Table::new().field("x", FieldType::Number))
            .build();
        let v = serde_json::to_value(&schema).unwrap();
        assert!(v["tables"]["solo"]["indexes"].is_null());
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd rust-client && cargo test --lib schema`
Expected: FAIL — types undefined.

- [ ] **Step 3: Implement `schema.rs`**

```rust
//! Schema DSL: builds the exact `SchemaDef` JSON consumed by `POST /admin/push-schema`.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl FieldType {
    pub fn id(table: &str) -> Self { FieldType::Id { table: table.into() } }
    pub fn optional(inner: FieldType) -> Self { FieldType::Optional { inner: Box::new(inner) } }
    pub fn literal(value: impl Into<serde_json::Value>) -> Self { FieldType::Literal { value: value.into() } }
    pub fn union(variants: impl IntoIterator<Item = FieldType>) -> Self { FieldType::Union { variants: variants.into_iter().collect() } }
    pub fn array(element: FieldType) -> Self { FieldType::Array { element: Box::new(element) } }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    pub name: String,
    pub fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDef {
    pub fields: BTreeMap<String, FieldType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexes: Option<Vec<IndexDef>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDef {
    pub tables: BTreeMap<String, TableDef>,
}

/// Finished schema (alias for the wire type).
pub type Schema = SchemaDef;

pub struct TableBuilder {
    fields: BTreeMap<String, FieldType>,
    indexes: Vec<IndexDef>,
}

impl TableBuilder {
    pub fn new() -> Self { Self { fields: BTreeMap::new(), indexes: Vec::new() } }
    pub fn field(mut self, name: &str, ft: FieldType) -> Self { self.fields.insert(name.into(), ft); self }
    pub fn index(mut self, name: &str, fields: &[&str]) -> Self {
        self.indexes.push(IndexDef { name: name.into(), fields: fields.iter().map(|s| (*s).into()).collect() });
        self
    }
    fn finish(self) -> TableDef {
        let indexes = if self.indexes.is_empty() { None } else { Some(self.indexes) };
        TableDef { fields: self.fields, indexes }
    }
}

/// Convenience alias used in builder closures for readability.
pub type Table = TableBuilder;

#[derive(Default)]
pub struct SchemaBuilder {
    tables: BTreeMap<String, TableDef>,
}

impl SchemaBuilder {
    pub fn new() -> Self { Self::default() }
    pub fn table(mut self, name: &str, build: impl OnceTable) -> Self {
        self.tables.insert(name.into(), build.finish_table());
        self
    }
    pub fn build(self) -> SchemaDef { SchemaDef { tables: self.tables } }
}

/// Anything that can produce a `TableDef`. Implemented for the builder and (later) closures.
pub trait OnceTable {
    fn finish_table(self) -> TableDef;
}

impl OnceTable for TableBuilder {
    fn finish_table(self) -> TableDef { TableBuilder::finish(self) }
}

impl SchemaDef {
    pub fn builder() -> SchemaBuilder { SchemaBuilder::new() }
}
```

Note: the `table(name, |t| …)` closure form is **not** used because closures returning `Self` fight the borrow checker. Call sites pass a `Table::new()…` chain directly (as in the tests). Keep `OnceTable` so a future closure form can be added without breaking the API.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd rust-client && cargo test --lib schema`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add rust-client/src/schema.rs
git commit -m "feat(rust-client): schema DSL (FieldType, SchemaDef, builders)"
```

---

### Task 4: Query DSL (`query.rs`)

**Files:**
- Modify: `rust-client/src/query.rs`
- Test: `rust-client/src/query.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Produces: `Query` (wire struct, snake_case, `deny_unknown_fields`), `Order`, `Paginate`, `Paginated<T>`, and `TableQuery` builder (mutators return `Self`, terminals return `Query`). Plus result-parse helpers `parse_result::<T>` used by the HTTP client.

**Consumes:** `crate::cursor` (Task 5) for `Paginated` cursor round-trip only if you wire it here; otherwise cursors stay opaque strings.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn bare_table_query() {
        let q = TableQuery::new("items").collect();
        assert_eq!(serde_json::to_value(&q).unwrap(), json!({"table":"items"}));
    }

    #[test]
    fn point_get() {
        let q = TableQuery::get("items", "abc");
        assert_eq!(serde_json::to_value(&q).unwrap(), json!({"table":"items","get":"abc"}));
    }

    #[test]
    fn index_eq_unique() {
        let q = TableQuery::new("items").with_index("by_project", &[json!("p1")]).unique();
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"table":"items","index":"by_project","eq":["p1"],"unique":true})
        );
    }

    #[test]
    fn range_order_take() {
        let q = TableQuery::new("items")
            .with_index("by_project", &[json!("p1")])
            .gte("a")
            .lte("m")
            .order(Order::Desc)
            .take(10);
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"table":"items","index":"by_project","eq":["p1"],"gte":"a","lte":"m","order":"desc","take":10})
        );
    }

    #[test]
    fn count_terminal() {
        let q = TableQuery::new("items").with_index("by_status", &[json!("backlog")]).count();
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"table":"items","index":"by_status","eq":["backlog"],"count":true})
        );
    }

    #[test]
    fn paginate_terminal() {
        let q = TableQuery::new("items")
            .with_index("by_status", &[json!("backlog")])
            .paginate(None, 20);
        assert_eq!(
            serde_json::to_value(&q).unwrap(),
            json!({"table":"items","index":"by_status","eq":["backlog"],"paginate":{"numItems":20}})
        );
    }

    #[test]
    fn parse_count_from_number() {
        let n: i64 = parse_result(serde_json::json!(42)).unwrap();
        assert_eq!(n, 42);
    }

    #[test]
    fn parse_docs_from_array() {
        let docs: Vec<serde_json::Value> = parse_result(serde_json::json!([{"_id":"a"},{"_id":"b"}])).unwrap();
        assert_eq!(docs.len(), 2);
    }

    #[test]
    fn parse_doc_from_object_or_null() {
        let some: Option<serde_json::Value> = parse_result(serde_json::json!({"_id":"a"})).unwrap();
        assert!(some.is_some());
        let none: Option<serde_json::Value> = parse_result(serde_json::Value::Null).unwrap();
        assert!(none.is_none());
    }

    #[test]
    fn parse_paginated() {
        let p: Paginated<serde_json::Value> =
            parse_result(json!({"docs":[{"_id":"a"}],"nextCursor":"zzz"})).unwrap();
        assert_eq!(p.docs.len(), 1);
        assert_eq!(p.next_cursor.as_deref(), Some("zzz"));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd rust-client && cargo test --lib query`
Expected: FAIL — types undefined.

- [ ] **Step 3: Implement `query.rs`**

```rust
//! Query DSL: builds the exact `Query` JSON the server expects, and parses untagged results.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Order {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Paginate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    pub num_items: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Query {
    pub table: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub get: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub eq: Vec<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gt: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gte: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lt: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lte: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<Order>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub take: Option<u32>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub unique: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub first: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub count: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paginate: Option<Paginate>,
}

fn is_false(b: bool) -> bool { !b }

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Paginated<T> {
    pub docs: Vec<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// A built query is just the wire `Query` (terminals consume the builder).
pub struct TableQuery {
    q: Query,
}

impl TableQuery {
    pub fn new(table: &str) -> Self {
        Self { q: Query { table: table.into(), ..Default::default() } }
    }
    pub fn get(table: &str, id: &str) -> Self {
        Self { q: Query { table: table.into(), get: Some(id.into()), ..Default::default() } }
    }

    // mutators
    pub fn with_index(mut self, index: &str, eq: &[serde_json::Value]) -> Self {
        self.q.index = Some(index.into());
        self.q.eq = eq.to_vec();
        self
    }
    pub fn gt(mut self, v: impl Into<serde_json::Value>) -> Self { self.q.gt = Some(v.into()); self }
    pub fn gte(mut self, v: impl Into<serde_json::Value>) -> Self { self.q.gte = Some(v.into()); self }
    pub fn lt(mut self, v: impl Into<serde_json::Value>) -> Self { self.q.lt = Some(v.into()); self }
    pub fn lte(mut self, v: impl Into<serde_json::Value>) -> Self { self.q.lte = Some(v.into()); self }
    pub fn order(mut self, o: Order) -> Self { self.q.order = Some(o); self }

    // terminals — return the finished wire Query
    pub fn take(mut self, n: u32) -> Query { self.q.take = Some(n); self.q }
    pub fn collect(self) -> Query { self.q }
    pub fn unique(mut self) -> Query { self.q.unique = true; self.q }
    pub fn first(mut self) -> Query { self.q.first = true; self.q }
    pub fn count(mut self) -> Query { self.q.count = true; self.q }
    pub fn paginate(mut self, cursor: Option<&str>, num_items: u32) -> Query {
        self.q.paginate = Some(Paginate { cursor: cursor.map(|c| c.into()), num_items });
        self.q
    }
    pub fn build(self) -> Query { self.q }
}

/// Deserialize the server's untagged `QueryResult` payload into the caller's type.
/// Shape is chosen by the terminal used: array → `Vec<T>`, object/null → `Option<T>`,
/// number → `i64`, `{docs,nextCursor?}` → `Paginated<T>`. serde does the discrimination
/// from `T` directly, so one generic covers all four.
pub fn parse_result<T: DeserializeOwned>(value: serde_json::Value) -> Result<T, crate::RtDbError> {
    serde_json::from_value::<T>(value)
        .map_err(|e| crate::RtDbError::internal(format!("invalid query result: {e}")))
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd rust-client && cargo test --lib query`
Expected: PASS (9 tests).

- [ ] **Step 5: Commit**

```bash
git add rust-client/src/query.rs
git commit -m "feat(rust-client): query DSL and untagged result parser"
```

---

### Task 5: Cursor codec (`cursor.rs`)

**Files:**
- Modify: `rust-client/src/cursor.rs`
- Test: `rust-client/src/cursor.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Produces: `encode_cursor(values: &[Value]) -> String` and `decode_cursor(s: &str) -> Result<Vec<Value>>`, standard base64 of a JSON array (server format: `[indexValues..., createdAt, id]`). The client treats cursors as opaque in normal use; these are parity/test helpers.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn round_trip() {
        let values = vec![json!("p1"), json!("backlog"), json!(1_700_000_000_000_i64), json!("id1")];
        let s = encode_cursor(&values).unwrap();
        // standard base64 (with padding) of the JSON array
        let raw = decode_cursor(&s).unwrap();
        assert_eq!(raw, values);
    }

    #[test]
    fn decode_rejects_garbage() {
        assert!(decode_cursor("!!!not-base64!!!").is_err());
    }

    #[test]
    fn decode_rejects_non_array() {
        // base64 of `"hello"` (a JSON string, not an array)
        let s = base64::engine::general_purpose::STANDARD.encode(b"\"hello\"");
        assert!(decode_cursor(&s).is_err());
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd rust-client && cargo test --lib cursor`
Expected: FAIL — functions undefined.

- [ ] **Step 3: Implement `cursor.rs`**

```rust
//! Pagination cursor codec. Server cursors are standard base64 of a JSON array
//! `[indexValues..., createdAt, id]`. The client normally passes cursors through
//! opaquely; these helpers exist for parity and tests.

use base64::Engine;
use serde_json::Value;

pub fn encode_cursor(values: &[Value]) -> Result<String, crate::RtDbError> {
    let json = serde_json::to_string(values)
        .map_err(|e| crate::RtDbError::internal(format!("cursor encode failed: {e}")))?;
    Ok(base64::engine::general_purpose::STANDARD.encode(json))
}

pub fn decode_cursor(s: &str) -> Result<Vec<Value>, crate::RtDbError> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| crate::RtDbError::internal(format!("invalid cursor base64: {e}")))?;
    let v: Vec<Value> = serde_json::from_slice(&bytes)
        .map_err(|e| crate::RtDbError::internal(format!("invalid cursor json: {e}")))?;
    Ok(v)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd rust-client && cargo test --lib cursor`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add rust-client/src/cursor.rs
git commit -m "feat(rust-client): pagination cursor codec"
```

---

### Task 6: Mutation DSL (`mutation.rs`)

**Files:**
- Modify: `rust-client/src/mutation.rs`
- Test: `rust-client/src/mutation.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Produces: `Transaction { steps }`, `Step` (tag `"op"`, camelCase, deny_unknown_fields), `StepResult` (untagged: `Insert{id}`, `Upsert{id, inserted}`, `Null`), and `Mutation` builder.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn builder_serializes_all_step_kinds() {
        let txn = Mutation::new()
            .insert("items", json!({"projectId":"p1","title":"a"}))
            .patch("items", "i1", json!({"title":"b"}))
            .replace("items", "i4", json!({"projectId":"p1","title":"c"}))
            .delete("items", "i2")
            .expect_version("items", "i3", 7)
            .expect_absent("items", "by_project_and_title", &["p1", "dup"])
            .upsert(
                "items",
                "by_project",
                &["p1"],
                json!({"projectId":"p1"}),
                json!({"title":"u"}),
            )
            .build();
        assert_eq!(
            serde_json::to_value(&txn).unwrap(),
            json!({
                "steps": [
                    {"op":"insert","table":"items","doc":{"projectId":"p1","title":"a"}},
                    {"op":"patch","table":"items","id":"i1","fields":{"title":"b"}},
                    {"op":"replace","table":"items","id":"i4","doc":{"projectId":"p1","title":"c"}},
                    {"op":"delete","table":"items","id":"i2"},
                    {"op":"expectVersion","table":"items","id":"i3","version":7},
                    {"op":"expectAbsent","table":"items","index":"by_project_and_title","eq":["p1","dup"]},
                    {"op":"upsert","table":"items","index":"by_project","eq":["p1"],"insert":{"projectId":"p1"},"patch":{"title":"u"}}
                ]
            })
        );
    }

    #[test]
    fn step_result_parses_insert_and_null() {
        let ins: StepResult = serde_json::from_value(json!({"id":"x"})).unwrap();
        assert!(matches!(ins, StepResult::Insert { id } if id == "x"));
        let nul: StepResult = serde_json::from_value(json!(null)).unwrap();
        assert!(matches!(nul, StepResult::Null));
    }

    #[test]
    fn step_result_parses_upsert() {
        let ins: StepResult = serde_json::from_value(json!({"id":"x","inserted":true})).unwrap();
        assert!(matches!(ins, StepResult::Upsert { inserted: true, .. }));
        let pat: StepResult = serde_json::from_value(json!({"id":"x","inserted":false})).unwrap();
        assert!(matches!(pat, StepResult::Upsert { inserted: false, .. }));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd rust-client && cargo test --lib mutation`
Expected: FAIL — types undefined.

- [ ] **Step 3: Implement `mutation.rs`**

```rust
//! Mutation DSL: builds the exact `Transaction` JSON the server expects.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", deny_unknown_fields)]
pub enum Step {
    Insert { table: String, doc: Map<String, Value> },
    Patch { table: String, id: String, fields: Map<String, Value> },
    Replace { table: String, id: String, doc: Map<String, Value> },
    Delete { table: String, id: String },
    ExpectVersion { table: String, id: String, version: i64 },
    ExpectAbsent { table: String, index: String, eq: Vec<Value> },
    Upsert {
        table: String,
        index: String,
        eq: Vec<Value>,
        insert: Map<String, Value>,
        patch: Map<String, Value>,
    },
}

/// One entry of `mutateOk.results`, positionally aligned with `steps`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StepResult {
    Insert { id: String },
    Upsert { id: String, inserted: bool },
    Null,
}

/// `null` on the wire deserializes to `StepResult::Null`.
impl Default for StepResult {
    fn default() -> Self { StepResult::Null }
}

pub struct Mutation {
    steps: Vec<Step>,
}

impl Mutation {
    pub fn new() -> Self { Self { steps: Vec::new() } }

    fn obj(v: Value) -> Map<String, Value> {
        match v {
            Value::Object(m) => m,
            // Non-object input is a caller bug; send an empty object so the server
            // rejects it with SCHEMA_VIOLATION rather than panicking client-side.
            _ => Map::new(),
        }
    }

    pub fn insert(mut self, table: &str, doc: Value) -> Self {
        self.steps.push(Step::Insert { table: table.into(), doc: Self::obj(doc) });
        self
    }
    pub fn patch(mut self, table: &str, id: &str, fields: Value) -> Self {
        self.steps.push(Step::Patch { table: table.into(), id: id.into(), fields: Self::obj(fields) });
        self
    }
    pub fn replace(mut self, table: &str, id: &str, doc: Value) -> Self {
        self.steps.push(Step::Replace { table: table.into(), id: id.into(), doc: Self::obj(doc) });
        self
    }
    pub fn delete(mut self, table: &str, id: &str) -> Self {
        self.steps.push(Step::Delete { table: table.into(), id: id.into() });
        self
    }
    pub fn expect_version(mut self, table: &str, id: &str, version: i64) -> Self {
        self.steps.push(Step::ExpectVersion { table: table.into(), id: id.into(), version });
        self
    }
    pub fn expect_absent(mut self, table: &str, index: &str, eq: &[Value]) -> Self {
        self.steps.push(Step::ExpectAbsent { table: table.into(), index: index.into(), eq: eq.to_vec() });
        self
    }
    pub fn upsert(
        mut self,
        table: &str,
        index: &str,
        eq: &[Value],
        insert: Value,
        patch: Value,
    ) -> Self {
        self.steps.push(Step::Upsert {
            table: table.into(),
            index: index.into(),
            eq: eq.to_vec(),
            insert: Self::obj(insert),
            patch: Self::obj(patch),
        });
        self
    }

    pub fn build(self) -> Transaction { Transaction { steps: self.steps } }
}

impl Default for Mutation {
    fn default() -> Self { Self::new() }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd rust-client && cargo test --lib mutation`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add rust-client/src/mutation.rs
git commit -m "feat(rust-client): mutation DSL (Transaction, Step, StepResult)"
```

---

### Task 7: Wire messages (`wire.rs`) + round-trip parity

**Files:**
- Modify: `rust-client/src/wire.rs`
- Test: `rust-client/src/wire.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: `crate::error::RtDbError`, `crate::query::Query`, `crate::mutation::Transaction`.
- Produces: `ClientMessage`, `ServerMessage`, `AuthedUser` — byte-identical to `server/src/protocol.rs`. (Used by the WS client in Plan 2; here we lock the contract with parity tests.)

- [ ] **Step 1: Write the failing tests** (expected JSON copied from `server/src/protocol.rs`'s own tests)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutation::Transaction;
    use crate::query::TableQuery;
    use serde_json::json;

    fn sample_query() -> Query {
        TableQuery::new("workItems").collect()
    }
    fn empty_txn() -> Transaction {
        Transaction { steps: vec![] }
    }

    #[test]
    fn client_message_tags_and_fields() {
        assert_eq!(
            serde_json::to_value(ClientMessage::Auth { token: "t".into(), db: "d".into() }).unwrap(),
            json!({"type":"auth","token":"t","db":"d"})
        );
        let sub = serde_json::to_value(ClientMessage::Subscribe {
            query_id: "q1".into(),
            query: Box::new(sample_query()),
        }).unwrap();
        assert_eq!(sub["type"], json!("subscribe"));
        assert_eq!(sub["query"], json!({"table":"workItems"}));
        assert_eq!(
            serde_json::to_value(ClientMessage::Unsubscribe { query_id: "q1".into() }).unwrap(),
            json!({"type":"unsubscribe","queryId":"q1"})
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::Mutate {
                mut_id: "m1".into(),
                idempotency_key: None,
                txn: empty_txn(),
            }).unwrap(),
            json!({"type":"mutate","mutId":"m1","txn":{"steps":[]}})
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::Mutate {
                mut_id: "m1".into(),
                idempotency_key: Some("key1".into()),
                txn: empty_txn(),
            }).unwrap(),
            json!({"type":"mutate","mutId":"m1","idempotencyKey":"key1","txn":{"steps":[]}})
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::Ping).unwrap(),
            json!({"type":"ping"})
        );
    }

    #[test]
    fn client_message_rejects_unknown_fields() {
        let raw = json!({"type":"auth","token":"t","db":"d","bogus":true});
        assert!(serde_json::from_value::<ClientMessage>(raw).is_err());
    }

    #[test]
    fn server_message_tags_and_fields() {
        let ok = serde_json::to_value(ServerMessage::AuthOk {
            user: AuthedUser { kind: "user".into(), email: Some("a@b.com".into()), name: None },
        }).unwrap();
        assert_eq!(ok["type"], json!("authOk"));
        assert_eq!(
            serde_json::to_value(ServerMessage::QueryUpdate {
                query_id: "q1".into(),
                result: json!([]),
            }).unwrap(),
            json!({"type":"queryUpdate","queryId":"q1","result":[]})
        );
        assert_eq!(
            serde_json::to_value(ServerMessage::MutateOk { mut_id: "m1".into(), results: vec![] }).unwrap(),
            json!({"type":"mutateOk","mutId":"m1","results":[]})
        );
        let err = serde_json::to_value(ServerMessage::MutateErr {
            mut_id: "m1".into(),
            error: crate::RtDbError::new(crate::ErrorCode::NotFound, "x"),
        }).unwrap();
        assert_eq!(err["type"], json!("mutateErr"));
        let serr = serde_json::to_value(ServerMessage::SubscribeErr {
            query_id: "q1".into(),
            error: crate::RtDbError::new(crate::ErrorCode::BadRequest, "bad index"),
        }).unwrap();
        assert_eq!(
            serr,
            json!({"type":"subscribeErr","queryId":"q1","error":{"code":"BAD_REQUEST","message":"bad index"}})
        );
        assert_eq!(
            serde_json::to_value(ServerMessage::Pong).unwrap(),
            json!({"type":"pong"})
        );
    }

    #[test]
    fn client_message_round_trips_through_json() {
        let msg = ClientMessage::Subscribe { query_id: "q1".into(), query: Box::new(sample_query()) };
        let value = serde_json::to_value(&msg).unwrap();
        let restored: ClientMessage = serde_json::from_value(value).unwrap();
        assert!(matches!(restored, ClientMessage::Subscribe { query_id, .. } if query_id == "q1"));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd rust-client && cargo test --lib wire`
Expected: FAIL — types undefined.

- [ ] **Step 3: Implement `wire.rs`**

```rust
//! Wire vocabulary — the third implementation of the protocol contract
//! (server `protocol.rs` first, TS `protocol.ts` second). Tags/fields are load-bearing.

use crate::error::RtDbError;
use crate::mutation::Transaction;
use crate::query::Query;
use serde::{Deserialize, Serialize};

pub type QueryRef = Query;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase", deny_unknown_fields)]
pub enum ClientMessage {
    Auth { token: String, db: String },
    Subscribe { query_id: String, query: Box<Query> },
    Unsubscribe { query_id: String },
    Mutate {
        mut_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        idempotency_key: Option<String>,
        txn: Transaction,
    },
    Ping,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum ServerMessage {
    AuthOk { user: AuthedUser },
    AuthErr { error: RtDbError },
    QueryUpdate { query_id: String, result: serde_json::Value },
    MutateOk { mut_id: String, results: Vec<serde_json::Value> },
    MutateErr { mut_id: String, error: RtDbError },
    SubscribeErr { query_id: String, error: RtDbError },
    Pong,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthedUser {
    pub kind: String,
    pub email: Option<String>,
    pub name: Option<String>,
}
```

Note: the test module references `Query` directly, so re-export it here for convenience: the `pub type QueryRef = Query;` line keeps `Query` in scope without extra imports. (`use crate::query::Query;` already brings it in.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd rust-client && cargo test --lib wire`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add rust-client/src/wire.rs
git commit -m "feat(rust-client): wire messages with round-trip parity tests"
```

---

### Task 8: HTTP client (`http.rs`)

**Files:**
- Modify: `rust-client/src/http.rs`
- Test: `rust-client/src/http.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: `crate::query::{Query, TableQuery, parse_result}`, `crate::mutation::{Transaction, StepResult}`, `crate::error::{RtDbError, ErrorEnvelope}`, `crate::wire::AuthedUser`.
- Produces: `RtDbHttpClient { new, run, get, mutate, auth_me }`. Auth header `Authorization: Bearer <token>` on every call.

- [ ] **Step 1: Write the failing tests** (wiremock — hermetic, no live server)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutation::Mutation;
    use crate::query::TableQuery;
    use serde_json::{json, Value};
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn doc(id: &str) -> Value { json!({"_id": id, "name": format!("n-{id}")}) }

    async fn setup() -> (MockServer, RtDbHttpClient) {
        let server = MockServer::start().await;
        let client = RtDbHttpClient::new(server.uri().as_str(), "t<uuid>", "machine-token");
        (server, client)
    }

    #[tokio::test]
    async fn run_collect_posts_query_and_parses_result() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/api/query"))
            .and(header("authorization", "Bearer machine-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": [doc("a"), doc("b")]
            })))
            .mount(&server)
            .await;
        let q = TableQuery::new("items").with_index("by_status", &[json!("active")]).take(2);
        let got: Vec<Value> = client.run(q).await.unwrap();
        assert_eq!(got.len(), 2);
    }

    #[tokio::test]
    async fn run_count_parses_number() {
        let (server, client) = setup().await;
        Mock::given(method("POST")).and(path("/api/query"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result": 5})))
            .mount(&server).await;
        let n: i64 = client.run(TableQuery::new("items").count()).await.unwrap();
        assert_eq!(n, 5);
    }

    #[tokio::test]
    async fn get_returns_optional_doc() {
        let (server, client) = setup().await;
        Mock::given(method("POST")).and(path("/api/query"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result": doc("a")})))
            .mount(&server).await;
        let some: Option<Value> = client.get("items", "a").await.unwrap();
        assert!(some.is_some());
    }

    #[tokio::test]
    async fn mutate_posts_and_parses_results() {
        let (server, client) = setup().await;
        Mock::given(method("POST")).and(path("/api/mutate"))
            .and(header("authorization", "Bearer machine-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "results": [{"id":"new1"}, null]
            })))
            .mount(&server).await;
        let txn = Mutation::new().insert("items", json!({"name":"x"})).patch("items", "i1", json!({"y":1})).build();
        let res = client.mutate(&txn, None).await.unwrap();
        assert_eq!(res.len(), 2);
        assert!(matches!(res[0], crate::mutation::StepResult::Insert { ref id } if id == "new1"));
    }

    #[tokio::test]
    async fn mutate_sends_idempotency_key() {
        let (server, client) = setup().await;
        Mock::given(method("POST")).and(path("/api/mutate"))
            .and(wiremock::matchers::body_partial_json(json!({"idempotencyKey":"k1"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"results":[]})))
            .mount(&server).await;
        let txn = Mutation::new().delete("items", "i1").build();
        client.mutate(&txn, Some("k1")).await.unwrap();
    }

    #[tokio::test]
    async fn error_envelope_becomes_rtdb_error() {
        let (server, client) = setup().await;
        Mock::given(method("POST")).and(path("/api/query"))
            .respond_with(ResponseTemplate::new(409)
                .set_body_json(json!({"code":"PRECONDITION_FAILED","message":"version mismatch"})))
            .mount(&server).await;
        let err = client.run::<i64>(TableQuery::new("items").count()).await.unwrap_err();
        assert_eq!(err.code, crate::ErrorCode::PreconditionFailed);
        assert_eq!(err.message, "version mismatch");
    }

    #[tokio::test]
    async fn non_envelope_error_is_internal() {
        let (server, client) = setup().await;
        Mock::given(method("POST")).and(path("/api/query"))
            .respond_with(ResponseTemplate::new(500).set_body_string("gateway down"))
            .mount(&server).await;
        let err = client.run::<i64>(TableQuery::new("items").count()).await.unwrap_err();
        assert_eq!(err.code, crate::ErrorCode::Internal);
    }

    #[tokio::test]
    async fn auth_me_returns_user() {
        let (server, client) = setup().await;
        Mock::given(method("GET")).and(path("/auth/me"))
            .and(header("authorization", "Bearer machine-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "user": {"kind":"user","email":"a@b.com","name":null}
            })))
            .mount(&server).await;
        let user = client.auth_me().await.unwrap();
        assert_eq!(user.kind, "user");
        assert_eq!(user.email.as_deref(), Some("a@b.com"));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd rust-client && cargo test --lib http`
Expected: FAIL — `RtDbHttpClient` undefined.

- [ ] **Step 3: Implement `http.rs`**

```rust
//! One-shot HTTP client for par-rt-db. `Authorization: Bearer <token>` on every call.

use crate::error::{ErrorEnvelope, RtDbError};
use crate::mutation::{StepResult, Transaction};
use crate::query::{parse_result, TableQuery};
use crate::wire::AuthedUser;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub struct RtDbHttpClient {
    url: String,
    db: String,
    token: String,
    client: reqwest::Client,
}

impl RtDbHttpClient {
    pub fn new(url: &str, db: &str, token: &str) -> Self {
        let url = url.trim_end_matches('/').to_string();
        Self {
            url,
            db: db.to_string(),
            token: token.to_string(),
            client: reqwest::Client::new(),
        }
    }

    /// Run any built query; deserialize `{result}` into `T`.
    /// Use the terminal that matches `T` (`collect`→`Vec<T>`, `first/unique/get`→`Option<T>`,
    /// `count`→`i64`, `paginate`→`Paginated<T>`).
    pub async fn run<T: DeserializeOwned>(&self, query: impl Into<crate::query::Query>) -> Result<T, RtDbError> {
        #[derive(Serialize)]
        struct Body<'a> { db: &'a str, query: &'a crate::query::Query }
        let query = query.into();
        let body = Body { db: &self.db, query: &query };
        let resp = self.client
            .post(format!("{}/api/query", self.url))
            .bearer_auth(&self.token)
            .json(&body)
            .send().await
            .map_err(|e| RtDbError::internal(format!("query request failed: {e}")))?;
        self.json_result::<T>(resp).await
    }

    /// Point read: `{"table","get:id"}` → `Option<T>`.
    pub async fn get<T: DeserializeOwned>(&self, table: &str, id: &str) -> Result<Option<T>, RtDbError> {
        self.run(TableQuery::get(table, id)).await
    }

    /// Run a transaction; returns one `StepResult` per step.
    pub async fn mutate(&self, txn: &Transaction, idempotency_key: Option<&str>) -> Result<Vec<StepResult>, RtDbError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body<'a> {
            db: &'a str,
            txn: &'a Transaction,
            #[serde(skip_serializing_if = "Option::is_none")]
            idempotency_key: Option<&'a str>,
        }
        let body = Body { db: &self.db, txn, idempotency_key };
        let resp = self.client
            .post(format!("{}/api/mutate", self.url))
            .bearer_auth(&self.token)
            .json(&body)
            .send().await
            .map_err(|e| RtDbError::internal(format!("mutate request failed: {e}")))?;
        #[derive(serde::Deserialize)]
        struct MutateResponse { results: Vec<serde_json::Value> }
        let parsed = self.deserialize::<MutateResponse>(resp).await?;
        parsed.results.into_iter()
            .map(|v| serde_json::from_value::<StepResult>(v)
                .map_err(|e| RtDbError::internal(format!("invalid step result: {e}"))))
            .collect()
    }

    /// Validate the bearer (session) token via `GET /auth/me`. Machine tokens get 401.
    pub async fn auth_me(&self) -> Result<AuthedUser, RtDbError> {
        let resp = self.client
            .get(format!("{}/auth/me", self.url))
            .bearer_auth(&self.token)
            .send().await
            .map_err(|e| RtDbError::internal(format!("auth_me request failed: {e}")))?;
        #[derive(serde::Deserialize)]
        struct MeResponse { user: AuthedUser }
        let parsed = self.deserialize::<MeResponse>(resp).await?;
        Ok(parsed.user)
    }

    async fn json_result<T: DeserializeOwned>(&self, resp: reqwest::Response) -> Result<T, RtDbError> {
        #[derive(serde::Deserialize)]
        struct QueryResponse { result: serde_json::Value }
        let parsed = self.deserialize::<QueryResponse>(resp).await?;
        parse_result::<T>(parsed.result)
    }

    async fn deserialize<T: DeserializeOwned>(&self, resp: reqwest::Response) -> Result<T, RtDbError> {
        let status = resp.status();
        if status.is_success() {
            return resp.json::<T>().await
                .map_err(|e| RtDbError::internal(format!("invalid response body: {e}")));
        }
        // Error path: try to parse {code,message}, else INTERNAL.
        match resp.json::<ErrorEnvelope>().await {
            Ok(env) => Err(RtDbError::from_envelope(env)),
            Err(_) => Err(RtDbError::internal(format!("request failed with status {}", status.as_u16()))),
        }
    }
}
```

Also add the `From<TableQuery> for Query` impl in `query.rs` (so `impl Into<Query>` works):

```rust
impl From<TableQuery> for Query {
    fn from(b: TableQuery) -> Query { b.q }
}
impl From<Query> for Query {
    fn from(q: Query) -> Query { q }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd rust-client && cargo test --lib http`
Expected: PASS (8 tests).

- [ ] **Step 5: Commit**

```bash
git add rust-client/src/http.rs rust-client/src/query.rs
git commit -m "feat(rust-client): typed HTTP client (query/mutate/auth-me)"
```

---

### Task 9: Live-server integration test (opt-in)

**Files:**
- Create: `rust-client/tests/common/mod.rs` — shared harness: create uniquely-named db, push schema, mint token.
- Create: `rust-client/tests/http_integration.rs` — `#[ignore]` tests gated on `RTDB_TEST_SERVER_URL` + `RTDB_TEST_ADMIN_KEY`.

**Interfaces:**
- Consumes: the admin HTTP endpoints (called via `reqwest` directly with the admin key — the typed `RtDbAdminClient` is Plan 2) and `RtDbHttpClient`.

- [ ] **Step 1: Write the harness + tests**

`rust-client/tests/common/mod.rs`:

```rust
use serde::{Deserialize, Serialize};

pub struct Ctx {
    pub url: String,
    pub db: String,
    pub token: String,
    admin_key: String,
}

pub fn env() -> Option<(String, String)> {
    let url = std::env::var("RTDB_TEST_SERVER_URL").ok()?;
    let admin = std::env::var("RTDB_TEST_ADMIN_KEY").ok()?;
    Some((url, admin))
}

pub async fn setup() -> Ctx {
    let (url, admin_key) = env().expect("RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY must be set");
    let client = reqwest::Client::new();
    // unique db name
    let db = format!("t{}", uuid_v7());
    #[derive(Serialize)] struct CreateDb<'a> { name: &'a str }
    #[derive(Deserialize)] struct Ok { ok: bool }
    post::<CreateDb, Ok>(&client, &url, "/admin/create-db", &admin_key, &CreateDb { name: &db }).await;

    // push a tiny schema
    let schema = serde_json::json!({
        "tables": {
            "items": {
                "fields": { "name": {"type":"string"}, "n": {"type":"number"} },
                "indexes": [ {"name":"by_n","fields":["n"]} ]
            }
        }
    });
    #[derive(Serialize)] struct Push<'a> { db: &'a str, schema: serde_json::Value }
    post::<Push, Ok>(&client, &url, "/admin/push-schema", &admin_key, &Push { db: &db, schema }).await;

    // mint a machine token for this db
    #[derive(Serialize)] struct Mint<'a> { db: &'a str, name: &'a str }
    #[derive(Deserialize)] struct Minted { #[serde(rename="tokenId")] token_id: String, token: String }
    let minted = post::<Mint, Minted>(&client, &url, "/admin/mint-token", &admin_key,
        &Mint { db: &db, name: "test" }).await;

    Ctx { url, db, token: minted.token, admin_key }
}

async fn post<B: Serialize, R: for<'de> Deserialize<'de>>(
    client: &reqwest::Client, url: &str, path: &str, admin_key: &str, body: &B,
) -> R {
    let resp = client.post(format!("{url}{path}")).bearer_auth(admin_key).json(body).send().await.unwrap();
    resp.json::<R>().await.unwrap()
}

// minimal uuidv7-ish unique string without pulling uuid into common
fn uuid_v7() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
    format!("{ms:012x}{:020x}", rand_counter())
}
fn rand_counter() -> u128 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static C: AtomicU64 = AtomicU64::new(0);
    (C.fetch_add(1, Ordering::SeqCst) as u128) | 0x8000
}
```

`rust-client/tests/http_integration.rs`:

```rust
mod common;

use common::{env, setup};
use par_rt_db_client::{Mutation, RtDbHttpClient, TableQuery};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Deserialize)]
struct Item { _id: String, name: String, n: i64 }

fn maybe(cfg: Option<()>) -> Option<()> { cfg }

#[tokio::test]
#[ignore = "set RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY and run with --ignored"]
async fn http_round_trip() {
    if env().is_none() { return; }
    let ctx = setup().await;
    let c = RtDbHttpClient::new(&ctx.url, &ctx.db, &ctx.token);

    // insert
    let txn = Mutation::new().insert("items", json!({"name":"a","n":1})).insert("items", json!({"name":"b","n":2})).build();
    let res = c.mutate(&txn, None).await.unwrap();
    assert_eq!(res.len(), 2);

    // query all ordered by n
    let docs: Vec<Item> = c.run(
        TableQuery::new("items").with_index("by_n", &[]).order(Order::Asc).take(10)
    ).await.unwrap();
    assert_eq!(docs.len(), 2);
    assert_eq!(docs[0].name, "a");

    // count
    let n: i64 = c.run(TableQuery::new("items").with_index("by_n", &[]).count()).await.unwrap();
    assert_eq!(n, 2);

    // precondition failure
    let bad = Mutation::new().expect_version("items", "nope", 999).build();
    let err = c.mutate(&bad, None).await.unwrap_err();
    assert_eq!(err.code, par_rt_db_client::ErrorCode::PreconditionFailed);
}

use par_rt_db_client::Order;
```

- [ ] **Step 2: Export the public API from `lib.rs`**

Add to the end of `rust-client/src/lib.rs` (these symbols now exist, so the re-exports compile):

```rust
pub use error::{ErrorCode, ErrorEnvelope, RtDbError, retry_on_precondition};
pub use mutation::{Mutation, StepResult, Transaction};
pub use query::{Order, Paginated, Paginate, Query, TableQuery};
pub use schema::{FieldType, IndexDef, SchemaDef, TableDef};
pub use wire::{AuthedUser, ClientMessage, ServerMessage};
#[cfg(feature = "http")]
pub use http::RtDbHttpClient;
```

- [ ] **Step 3: Run the opt-in test against a live dev server**

Start the dev DB and server, then run the ignored test:
```bash
make dev-db-up
# in another shell, run the server on :8300 (per server/ README) with an admin key, e.g.:
RTDB_ADMIN_KEY=dev-admin-key cargo run -p rtdb-server &
# then:
cd rust-client && RTDB_TEST_SERVER_URL=http://127.0.0.1:8300 RTDB_TEST_ADMIN_KEY=dev-admin-key cargo test --test http_integration -- --ignored
```
Expected: PASS. (If the server isn't built/runnable in this pass, run `cargo test --test http_integration` without `--ignored` and confirm it compiles and the non-ignored guard returns early.)

- [ ] **Step 4: Verify the full gate**

Run:
```bash
cd rust-client && cargo test --all-features
cd rust-client && cargo clippy --all-targets --all-features -- -D warnings
cd rust-client && cargo fmt --all -- --check
make checkall   # from repo root — runs server + client + rust-client
```
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add rust-client/tests rust-client/src/lib.rs
git commit -m "test(rust-client): opt-in live-server HTTP integration test + harness"
```

---

## Self-Review notes (for the implementer)

- **Spec coverage (Plan 1 scope):** core wire types (Task 7), schema DSL (Task 3), query DSL + result parsing (Tasks 4–5), mutation DSL (Task 6), HTTP query/mutate/auth-me (Task 8), testing strategy incl. opt-in integration (Task 9). Admin (Plan 2) and WS reactive (Plan 3) are out of scope here.
- **Wire-parity safety net:** Task 7 tests assert byte-identity vs `server/src/protocol.rs`'s own fixtures; Tasks 3/4/6 assert builder output vs the TS fixtures from `client/src/`. If any assertion drifts, fix the serde attributes, not the test.
- **Type consistency:** `TableQuery` terminals all return `Query`; `From<TableQuery> for Query` (Task 8) lets `RtDbHttpClient::run` accept either. `parse_result<T>` is the single deserialization seam for query results.
- **`#![deny(warnings)]` posture:** every `unwrap` lives under `#[cfg(test)]`. The one `.expect()` in `cursor.rs` is justified inline. Watch for unused-import warnings from feature gating.
