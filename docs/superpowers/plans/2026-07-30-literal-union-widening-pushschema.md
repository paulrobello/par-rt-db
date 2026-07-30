# Literal-Union Widening in pushSchema — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow `pushSchema` to accept a safe widening of a literal-union field (adding enum variants), so additive schema changes that widen a union are no longer rejected alongside their sibling additions.

**Architecture:** `server/src/ddl.rs::detect_destructive_changes` currently rejects *any* field-type change. Add a sound subtype check `schema::is_widening_of(old, new)` that returns true only when the new type's accepted-value set is a superset of the old's — today, a lone `Literal` or an all-`Literal` `Union` whose literal set grew. Consult it at the one reject site (`ddl.rs:90`) so a widening is allowed (additive, no migration) while every narrowing/scalar-swap/mixed-union change stays rejected with the existing `changed type of field` `BadRequest`. No DDL change is needed: a string-literal union's indexed column is `text` regardless of variant count (`indexed_column_type`), so widening never alters an existing column.

**Tech Stack:** Rust (axum/tokio + sqlx/Postgres 17); `serde_json::Value` (which is `PartialEq` but **not** `Ord`/`Hash`, so set membership uses linear `.any()` over a `Vec`, not a `HashSet`/`BTreeSet`).

## Global Constraints

- **Server-only.** Touches `server/src/schema.rs` and `server/src/ddl.rs` plus one new `server/tests/` file. No SQL, no identifier quoting, no wire/protocol change.
- **Soundness is the whole point.** `is_widening_of` must be an *over-approximation of rejection*: it may decline a safe change, but it must NEVER accept a change that could orphan existing rows. Narrowing a union (dropping a variant some rows may hold), any scalar-type change, `union <-> scalar`, mixed-kind unions, and `Optional`/`Object` changes MUST stay rejected.
- **Preserve the exact rejection message.** Rejected changes still error `changed type of field '{table}.{field}'` via `RtDbError::bad_request`. Existing assertions (`tests/vector_test.rs`, `tests/admin_test.rs`) and clients depend on this string.
- **No new DDL.** The additive `push_schema` path only adds new tables/columns/indexes; it never `ALTER`s existing columns. A widened string-literal union keeps `indexed_column_type == "text"`, so nothing to alter. Do not add any column-alteration code.
- **Rust discipline.** No `unwrap()`/`expect()` outside `#[cfg(test)]`. Zero clippy warnings under `-D warnings`.
- **Clients mirror the core.** The ts/rust/python in-memory harnesses each port `detect_destructive_changes` (FEATURE_MATRIX #19) and now diverge from the server. Do NOT silently let them drift — Task 3 files the mirror gap as a tracked backlog item and notes the divergence in FEATURE_MATRIX.
- **Definition of done:** `make checkall` green (fmt-check + clippy `-D warnings` + typecheck + tests). The dev Postgres on `127.0.0.1:55434` is required for the integration test and the gate; if the port is already taken by a healthy `*-postgres-1` container from another worktree, reuse it (`cd server && RTDB_TEST_DATABASE_URL="postgres://rtdb:rtdb@127.0.0.1:55434/rtdb" cargo test`) rather than fighting for the port.

---

## File Structure

- **`server/src/schema.rs`** (modify) — add `pub fn is_widening_of(old, new) -> bool` + private `fn literal_set(ty) -> Option<Vec<&Value>>`, plus unit tests in the existing `#[cfg(test)] mod tests`. This is `FieldType`'s home; type-subtype semantics live here, not in `ddl.rs`.
- **`server/src/ddl.rs`** (modify) — import `is_widening_of`, consult it in the `detect_destructive_changes` field-type guard (`ddl.rs:90`), update the function doc comment, and add a `#[cfg(test)] mod tests` (the file has none today) proving the policy: widening allowed, narrowing/scalar-swap/field-removal rejected.
- **`server/tests/schema_evolution_test.rs`** (create) — end-to-end test through `push_schema` (mirrors the `tests/vector_test.rs` harness): a widened union push succeeds and persists; a narrowed union push is rejected with the field-named `BadRequest`.
- **`FEATURE_MATRIX.md`** (modify, Task 3) — rows 43 (Schema migration on push), 73 (in-memory harness), and the §1 comparison row (~line 231): note safe literal-union widening is now allowed server-side and that the client harness mirror is tracked.

---

## Task 1: Add widening support (predicate + wire + unit tests)

**Files:**
- Modify: `server/src/schema.rs` (add `is_widening_of` + `literal_set` near the other `FieldType` helpers, after `indexed_column_type` closes ~line 237; add tests to the existing `#[cfg(test)] mod tests`)
- Modify: `server/src/ddl.rs:7` (import), `ddl.rs:73-75` (doc comment), `ddl.rs:90-94` (guard); add `#[cfg(test)] mod tests` at the end of the file

**Interfaces:**
- Produces: `pub fn is_widening_of(old: &FieldType, new: &FieldType) -> bool` in `schema.rs`. Semantics: true iff `old`'s accepted-value set ⊆ `new`'s; today only the finite-literal-set cases (`Literal`, or `Union` of all-`Literal`) where the new literal set is a superset. Everything else → false.
- Consumes: none (leaf helper).

- [ ] **Step 1: Write the failing predicate tests in `schema.rs`'s test module**

Append to the existing `#[cfg(test)] mod tests` in `server/src/schema.rs` (it already `use super::*` and constructs `FieldType::Literal` inline around line 932 — reuse any existing literal-builder helper if present; otherwise add `union_of`):

```rust
    fn union_of(vals: &[&str]) -> FieldType {
        FieldType::Union {
            variants: vals
                .iter()
                .map(|v| FieldType::Literal {
                    value: serde_json::Value::String((*v).to_string()),
                })
                .collect(),
        }
    }

    #[test]
    fn is_widening_of_allows_adding_a_union_variant() {
        let old = union_of(&["low", "medium", "high"]);
        let new = union_of(&["low", "medium", "high", "critical"]);
        assert!(is_widening_of(&old, &new));
    }

    #[test]
    fn is_widening_of_rejects_narrowing_a_union() {
        let old = union_of(&["low", "medium", "high", "critical"]);
        let new = union_of(&["low", "medium", "high"]);
        assert!(!is_widening_of(&old, &new));
    }

    #[test]
    fn is_widening_of_allows_literal_to_union() {
        let old = FieldType::Literal { value: serde_json::Value::String("a".to_string()) };
        let new = union_of(&["a", "b"]);
        assert!(is_widening_of(&old, &new));
    }

    #[test]
    fn is_widening_of_rejects_scalar_swap_and_nonliteral_types() {
        assert!(!is_widening_of(&FieldType::Number, &FieldType::String));
        assert!(!is_widening_of(&FieldType::String, &union_of(&["a"])));
        // A union mixing a Literal with a non-literal variant is an open type.
        let mixed = FieldType::Union {
            variants: vec![
                FieldType::Literal { value: serde_json::Value::String("a".to_string()) },
                FieldType::String,
            ],
        };
        assert!(!is_widening_of(&mixed, &union_of(&["a", "b"])));
        assert!(!is_widening_of(&union_of(&["a", "b"]), &mixed));
    }
```

- [ ] **Step 2: Run the predicate tests to verify they fail**

Run: `cd server && cargo test --lib schema::tests::is_widening_of`
Expected: COMPILE ERROR (`cannot find function is_widening_of`).

- [ ] **Step 3: Implement `is_widening_of` + `literal_set` in `schema.rs`**

Add just after `indexed_column_type` (after its `match` closes, ~line 237), keeping them with the other `FieldType` helpers:

```rust
/// Returns `true` when changing a field's declared type from `old` to `new` is a
/// safe widening — every value valid under `old` remains valid under `new`, so no
/// existing row is orphaned and no data migration is required. The only widening
/// currently recognized is over finite literal sets: a lone `Literal` or a `Union`
/// whose variants are all `Literal`s, where the new literal set is a superset of
/// the old one (e.g. adding a variant to an enum-like union). Every other type
/// change — narrowing a union (drops a variant some rows may hold), `union <->
/// scalar`, any scalar-type change, `Optional`, `Object`, and mixed-kind unions —
/// is NOT a widening and stays rejected by `detect_destructive_changes`.
pub fn is_widening_of(old: &FieldType, new: &FieldType) -> bool {
    match (literal_set(old), literal_set(new)) {
        (Some(old_vals), Some(new_vals)) => old_vals
            .iter()
            .all(|old_v| new_vals.iter().any(|new_v| *new_v == *old_v)),
        _ => false,
    }
}

/// Finite set of accepted values for a literal-only type: `Some` for a lone
/// `Literal` or a `Union` whose variants are all `Literal`s; `None` for any other
/// type (unions mixing in non-literal variants, scalars, `Optional`, `Object`).
/// Variant order and duplicates are irrelevant — the result is used only for
/// membership tests. `serde_json::Value` is `PartialEq` but not `Ord`/`Hash`, so
/// this returns a `Vec<&Value>` for linear `.any()` checks rather than a set.
fn literal_set(ty: &FieldType) -> Option<Vec<&serde_json::Value>> {
    match ty {
        FieldType::Literal { value } => Some(vec![value]),
        FieldType::Union { variants } => {
            let vals: Vec<&serde_json::Value> = variants
                .iter()
                .filter_map(|v| match v {
                    FieldType::Literal { value } => Some(value),
                    _ => None,
                })
                .collect();
            // Finite only when every variant is a Literal.
            if vals.len() == variants.len() {
                Some(vals)
            } else {
                None
            }
        }
        _ => None,
    }
}
```

- [ ] **Step 4: Run the predicate tests to verify they pass**

Run: `cd server && cargo test --lib schema::tests::is_widening_of`
Expected: PASS (5 tests).

- [ ] **Step 5: Write the failing policy tests in a new `ddl.rs` test module**

Append to the end of `server/src/ddl.rs` (the file has no test module today):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn lit(s: &str) -> FieldType {
        FieldType::Literal {
            value: serde_json::Value::String(s.to_string()),
        }
    }

    fn union_of(vals: &[&str]) -> FieldType {
        FieldType::Union {
            variants: vals.iter().map(|v| lit(*v)).collect(),
        }
    }

    fn single_table(table: &str, fields: BTreeMap<String, FieldType>) -> BTreeMap<String, TableDef> {
        let mut tables = BTreeMap::new();
        tables.insert(
            table.to_string(),
            TableDef {
                fields,
                indexes: vec![],
                owner_field: None,
                collaborators_field: None,
            },
        );
        tables
    }

    fn one_field_schema(table: &str, field: &str, ty: FieldType) -> SchemaDef {
        let mut fields = BTreeMap::new();
        fields.insert(field.to_string(), ty);
        SchemaDef {
            tables: single_table(table, fields),
        }
    }

    #[test]
    fn detect_allows_widening_a_literal_union() {
        let old = one_field_schema("items", "priority", union_of(&["low", "medium", "high"]));
        let new = one_field_schema(
            "items",
            "priority",
            union_of(&["low", "medium", "high", "critical"]),
        );
        assert!(detect_destructive_changes(&old, &new).is_ok());
    }

    #[test]
    fn detect_rejects_narrowing_a_literal_union() {
        let old = one_field_schema(
            "items",
            "priority",
            union_of(&["low", "medium", "high", "critical"]),
        );
        let new = one_field_schema("items", "priority", union_of(&["low", "medium", "high"]));
        let err = detect_destructive_changes(&old, &new).expect_err("narrowing rejected");
        assert!(
            err.message.contains("changed type of field 'items.priority'"),
            "{}",
            err.message
        );
    }

    #[test]
    fn detect_rejects_a_scalar_type_change() {
        let old = one_field_schema("items", "qty", FieldType::Number);
        let new = one_field_schema("items", "qty", FieldType::String);
        let err = detect_destructive_changes(&old, &new).expect_err("scalar swap rejected");
        assert!(
            err.message.contains("changed type of field"),
            "{}",
            err.message
        );
    }

    #[test]
    fn detect_still_rejects_a_removed_field() {
        let old = one_field_schema("items", "qty", FieldType::Number);
        let new = SchemaDef {
            tables: single_table("items", BTreeMap::new()),
        };
        let err = detect_destructive_changes(&old, &new).expect_err("field removal rejected");
        assert!(
            err.message.contains("removed field 'items.qty'"),
            "{}",
            err.message
        );
    }
}
```

- [ ] **Step 6: Run the policy tests to verify they fail**

Run: `cd server && cargo test --lib ddl::tests`
Expected: `detect_allows_widening_a_literal_union` FAILS (`changed type of field 'items.priority'`); the other three PASS (they assert unchanged rejection behavior).

- [ ] **Step 7: Wire `is_widening_of` into `detect_destructive_changes`**

In `server/src/ddl.rs`:

(a) Add `is_widening_of` to the schema import (line 7):

```rust
use crate::schema::{FieldType, SchemaDef, TableDef, indexed_column_type, is_widening_of};
```

(b) Update the function doc comment (lines 73-75) so it no longer claims *all* type changes are rejected:

```rust
/// Compares `old` to `new` and rejects any destructive change: a removed table,
/// a removed field, a changed field type (except a safe literal-union widening,
/// which is additive and allowed — see `schema::is_widening_of`), a removed
/// index, or a changed index field list. Errors name the offending table,
/// `table.field`, or index.
```

(c) Loosen the field-type guard (lines 90-94) — keep rejecting unless the change is a widening:

```rust
                Some(new_field_type)
                    if new_field_type != old_field_type
                        && !is_widening_of(old_field_type, new_field_type) =>
                {
                    return Err(RtDbError::bad_request(format!(
                        "changed type of field '{table_name}.{field_name}'"
                    )));
                }
```

(Exact line wrapping will be normalized by `cargo fmt`; the logic is `reject iff (changed) && (!widening)`.)

- [ ] **Step 8: Run the policy tests to verify they pass**

Run: `cd server && cargo test --lib ddl::tests`
Expected: PASS (4 tests).

- [ ] **Step 9: Format + lint the touched files**

Run: `cd server && cargo fmt && cargo clippy --all-targets -- -D warnings`
Expected: clean. Fix anything flagged before committing.

- [ ] **Step 10: Commit**

```bash
cd /Users/probello/Repos/par-rt-db
git add server/src/schema.rs server/src/ddl.rs
git commit -m "feat(schema): allow safe literal-union widening in pushSchema

detect_destructive_changes rejected every field-type change, including the
backward-compatible case of adding variants to a literal union. Add
schema::is_widening_of (old accepted-values ⊆ new) and consult it at the
ddl.rs reject site, so widening is allowed additively while narrowing,
scalar swaps, union<->scalar, and mixed-kind unions stay rejected with the
existing 'changed type of field' BadRequest. No DDL change: a widened
string-literal union keeps indexed_column_type 'text'. Unblocks the projects
repo deploy (items.priority: low|medium|high -> +critical)."
```

---

## Task 2: End-to-end integration test through push_schema

**Files:**
- Create: `server/tests/schema_evolution_test.rs`

**Interfaces:**
- Consumes: the Task 1 behavior (widened union push succeeds; narrowed push rejected). Mirrors the `tests/vector_test.rs` harness: `mod common; use common::test_state;`, manual `rtdb_server::db::create_database` with a `uuid::Uuid::now_v7()` name (avoids `fresh_db`'s default-schema coupling), `push_schema` twice, assert on `err.code`/`err.message`.

- [ ] **Step 1: Create the integration test file**

Create `server/tests/schema_evolution_test.rs`:

```rust
mod common;

use common::test_state;
use rtdb_server::ddl::push_schema;
use rtdb_server::error::ErrorCode;
use rtdb_server::schema::{FieldType, IndexDef, SchemaDef, TableDef};
use std::collections::BTreeMap;

/// One table `items` whose `priority` is a string-literal union indexed by
/// `by_priority` — mirrors the projects repo's `items.priority` field whose
/// widening (low|medium|high -> +critical) blocked its deploy.
fn priority_schema(variants: &[&str]) -> SchemaDef {
    let union = FieldType::Union {
        variants: variants
            .iter()
            .map(|v| FieldType::Literal {
                value: serde_json::Value::String((*v).to_string()),
            })
            .collect(),
    };
    let mut fields = BTreeMap::new();
    fields.insert("priority".to_string(), union);
    let indexes = vec![IndexDef {
        name: "by_priority".to_string(),
        fields: vec!["priority".to_string()],
        search: false,
        vector: None,
    }];
    let mut tables = BTreeMap::new();
    tables.insert(
        "items".to_string(),
        TableDef {
            fields,
            indexes,
            owner_field: None,
            collaborators_field: None,
        },
    );
    SchemaDef { tables }
}

async fn fresh_empty_db(state: &std::sync::Arc<rtdb_server::AppState>) -> String {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create database");
    name
}

#[tokio::test]
async fn widening_a_literal_union_push_succeeds() {
    let state = test_state().await;
    let db = fresh_empty_db(&state).await;
    push_schema(&state.pool, &db, priority_schema(&["low", "medium", "high"]))
        .await
        .expect("initial push");
    // Adding a variant is a safe widening: accepted additively, no migration,
    // and the indexed f_priority text column is unchanged.
    push_schema(
        &state.pool,
        &db,
        priority_schema(&["low", "medium", "high", "critical"]),
    )
    .await
    .expect("widened push");
}

#[tokio::test]
async fn narrowing_a_literal_union_push_is_rejected() {
    let state = test_state().await;
    let db = fresh_empty_db(&state).await;
    push_schema(
        &state.pool,
        &db,
        priority_schema(&["low", "medium", "high", "critical"]),
    )
    .await
    .expect("initial push");
    let err = push_schema(&state.pool, &db, priority_schema(&["low", "medium", "high"]))
        .await
        .expect_err("narrowing must be rejected");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("changed type of field 'items.priority'"),
        "{}",
        err.message
    );
}
```

- [ ] **Step 2: Run the integration tests (dev DB required)**

Ensure the dev Postgres is up: `make dev-db-up` (or reuse an existing healthy `*-postgres-1` on `127.0.0.1:55434`).

Run: `cd server && cargo test --test schema_evolution_test`
Expected: PASS (2 tests).

- [ ] **Step 3: Format + lint**

Run: `cd server && cargo fmt && cargo clippy --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
cd /Users/probello/Repos/par-rt-db
git add server/tests/schema_evolution_test.rs
git commit -m "test(schema): push_schema accepts literal-union widening end-to-end

Mirrors the projects deploy scenario (items.priority indexed string-literal
union): a widened push succeeds and persists; a narrowed push is rejected
with the field-named BadRequest. Covers the additive-DDL path on an indexed
widened union (f_priority stays text)."
```

---

## Task 3: Docs, client-mirror gap, and the full gate

**Files:**
- Modify: `FEATURE_MATRIX.md` (rows 43, 73, and the §1 comparison row ~line 231)
- Board: file one backlog item for the client in-memory mirror; move the pushSchema item to `done`

**Interfaces:**
- Consumes: Task 1 + Task 2 complete and committed.

- [ ] **Step 1: Update `FEATURE_MATRIX.md`**

(a) Row 43 ("Schema migration on push", the `🟡 additive-only` cell's note) — append that safe widening is now allowed:

> Additive-only; destructive pushes rejected by design. **Safe widening of a literal-union field (adding enum variants) is allowed** — backward-compatible (old value set ⊆ new), no backfill. Convex allows more (with backfill).

(b) Row ~231 (the §1 comparison "Schema changes" row, par-rt-db cell):

> additive-only; destructive push rejected (safe literal-union widening allowed)

(c) Row 73 ("Client test harness", FEATURE_MATRIX #19) — its note says both harnesses port `detect_destructive_changes`. Append a divergence note so the doc does not contradict the server:

> …porting `ddl.rs::detect_destructive_changes`… **Server-side literal-union widening (`schema::is_widening_of`, 2026-07-30) is not yet mirrored in the harness ports — tracked as backlog; the harnesses still reject all type changes.**

- [ ] **Step 2: File the client-mirror gap on the board**

```bash
kanban item add --project par-rt-db \
  --title "Mirror literal-union widening in client in-memory harnesses (ts/rust/python)" \
  --priority low --estimate S --tags client,parity \
  --description "Server pushSchema now allows safe literal-union widening (schema::is_widening_of, 2026-07-30). The ts/rust/python in-memory harnesses each port detect_destructive_changes and still reject ALL type changes, so they diverge from the server." \
  --notes "Each harness has its own detect_destructive_changes port (ts-client/src/in_memory.ts, rust-client/src/in_memory.rs, python-client in-memory harness). Port is_widening_of into each (literal-set superset check; serde_json::Value uses linear membership, not a HashSet) and consult it at the changed-field reject site exactly as ddl.rs:90 now does. Tests per harness: widening allowed, narrowing rejected. Gate: make checkall. Low priority — test-only fidelity gap; no wire/protocol change."
```

- [ ] **Step 3: Run the full gate**

Run: `cd /Users/probello/Repos/par-rt-db && make checkall`
Expected: green (fmt-check + clippy `-D warnings` + typecheck + the whole test suite). If the dev-db port is taken by a healthy sibling container, reuse it per the Global Constraints rather than re-running `make test`.

- [ ] **Step 4: Move the kanban item to done**

```bash
kanban item done --id 019fb4df7ffd7b73b32cb8f0313a3c78
```

(Only after Step 3 is green — the board's `done` means verified against the repo's gate, not "code written".)

- [ ] **Step 5: Commit the docs**

```bash
cd /Users/probello/Repos/par-rt-db
git add FEATURE_MATRIX.md
git commit -m "docs(feature-matrix): note literal-union widening in pushSchema (+ client gap)"
```

---

## Self-Review

**1. Spec coverage.** The item spec required: (a) allow literal-union widening at `ddl.rs:90` — Task 1, guard loosened + `is_widening_of`. (b) reject narrowing / union<->scalar / scalar-type change — Task 1 unit tests pin all three (and mixed-kind unions). (c) three tests — Task 1 covers widen-succeeds, narrow-fails, scalar-swap-fails (plus literal→union and removed-field guard); Task 2 adds the dev-DB `push_schema` success path the item asked for ("against a dev DB, a pushSchema with a widened union succeeds"). (d) `make checkall` — Task 3 Step 3. (e) downstream projects unblock + SEC-001 cookie-capable-server gate — those are deploy-time follow-ups *outside this repo* (noted in the item's notes; this plan delivers the code change they wait on). (f) "Clients mirror the core" invariant — Task 3 Step 2 files the harness gap rather than letting it drift.

**2. Placeholder scan.** No TBD/TODO/"add appropriate" — every code step contains real Rust and real shell commands against verified file paths, signatures, and the existing `tests/vector_test.rs` harness shape.

**3. Type consistency.** `is_widening_of(old: &FieldType, new: &FieldType) -> bool` is defined in Task 1 Step 3 and consumed identically in Task 1 Step 7. `union_of` is defined separately in the `schema.rs` and `ddl.rs` test modules (intentional — they are different modules and cannot share a private helper). `FieldType::Literal { value }` / `FieldType::Union { variants }` match `schema.rs:15-17`. `TableDef { fields, indexes, owner_field, collaborators_field }` and `SchemaDef { tables }` match `tests/vector_test.rs:37-44`. `err.code` / `err.message` / `ErrorCode::BadRequest` match `tests/vector_test.rs:161-176`.

**One known conservative rejection (by design, not a bug):** `Union{a} -> String` *is* technically a widening (a single literal opening to all strings) but `is_widening_of` returns false because `String` is not a finite literal set. This is a safe over-approximation of rejection — it never accepts an unsafe change — and matches the item scope ("literal-union superset"). Recursive widening through `Optional`/`Object` is likewise out of scope and stays rejected. Document both in the `is_widening_of` doc comment (done in Task 1 Step 3).
