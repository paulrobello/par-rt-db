# Vector Search (#17) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add pgvector-backed semantic similarity search as a reactive `vectorSearch` query terminal, mirrored across server + ts-client + rust-client.

**Architecture:** Mirrors the existing full-text `search` feature (#11) end to end. A new `Vector` field type stores embeddings as a JSON array on documents; a vector index on `IndexDef` compiles to a write-maintained `vector(N)` column + HNSW `vector_cosine_ops` index; a `vectorSearch` query terminal ranks by cosine distance (`<=>`), composes with an eq-`filter` over declared `filterFields`, and rides the committer's existing table-level invalidation (reactive). Embeddings are client-supplied — no server-side generation (architecture forbids it).

**Tech Stack:** Rust (axum/tokio/sqlx 0.8/Postgres 17), pgvector extension (text-cast binds, no new crate dep), TypeScript (ts-client, bun/vitest), Rust client crate.

**Spec:** `docs/superpowers/specs/2026-07-23-vector-search-design.md`. Read it before starting; it is authoritative on the wire contract and invariants.

## Global Constraints

- **Wire contract is load-bearing and byte-identical across three files** — `server/src/{schema.rs,query.rs}`, `ts-client/src/protocol.ts`, `rust-client/src/{schema.rs,query.rs,wire.rs}`. Tags/field names are non-uniform and deliberate; match exactly.
  - Field type: `{"type":"vector","dimensions":N}` (serde `tag="type"`, `rename_all="camelCase"`).
  - Vector index on `IndexDef`: `vector` is `Option<VectorIndexSpec>` / optional, **omitted on the wire when absent** (`skip_serializing_if`), so existing btree/search indexes deserialize unchanged. `VectorIndexSpec` is camelCase: `{"dimensions":N,"filterFields":[...]}` (`filterFields` omitted when empty).
  - Terminal: Query-level field is Rust `vector_search` **renamed to `vectorSearch`** on the wire. `VectorSearchQuery` is camelCase `deny_unknown_fields`: `{index, vector, limit, filter?}`.
- **SQL construction**: validate + double-quote every identifier; bind every value via `$n`. The vector column uses placeholder `$n::vector` (text param explicitly cast to pgvector). Physical names lowercased + capped to 63 bytes (existing `pg_*` helpers).
- **Errors**: `RtDbError` envelope `{code,message}`. Schema violations → `SCHEMA`; query contract violations → `BAD_REQUEST`. Client-facing 500s carry a generic message; never stringify sqlx/pgvector errors into the body (log via `tracing`).
- **No `unwrap()`/`expect()` outside `#[cfg(test)]`**. Zero clippy warnings under `-D warnings`.
- **Single-writer invariant**: vector search is read-only and rides existing committer invalidation — never add a second writer.
- **Definition of done**: `make checkall` (fmt-check + clippy `-D warnings` + typecheck + tests) passes. Dev Postgres must be running (`make dev-db-up`) for tests.

## File Structure

**Server (`server/src/` + `server/tests/`):**
- `schema.rs` — `FieldType::Vector`, `VectorIndexSpec` on `IndexDef`, validation (`validate_field_type`, `validate_structure`, `validate_value`), `type_tag`, `indexed_column_type` unchanged (Vector not btree-indexable). *(Tasks 2, 3)*
- `ddl.rs` — `pg_vector_col()`, extend `indexed_fields()` (vector `filterFields` in, vector field out), vector-index DDL (column + HNSW), `CREATE EXTENSION vector` in `push_schema`, destructive-spec guard. *(Task 4)*
- `txn.rs` — refactor `table_columns`→`TableColumn`/`ColumnKind`, `ColBind::Vector`, `column_bind_for`, `$n::vector` placeholders in `do_insert`/`apply_update`/`insert_snapshot_row`. *(Task 5)*
- `query.rs` — `VectorSearchQuery`, `Query.vector_search`, mutual-exclusion guard, `execute_vector_search`, `VECTOR_SEARCH_MAX_LIMIT`. *(Task 6)*
- `db.rs` — `CREATE EXTENSION vector` in `create_database`. *(Task 1)*
- `tests/vector_test.rs` — new integration binary: extension guard + vector schema/ddl/txn/query coverage. *(Tasks 1, 4, 5, 6)*

**Deploy:** `docker-compose.dev.yml`, `docker-compose.yml` — image swap. *(Task 1)*

**ts-client (`ts-client/src/` + `tests/`):** `protocol.ts` (wire types), `schema.ts` (`t.vector`, `vectorIndex`), `query.ts` (`.vectorSearch`), `index.ts` (re-exports), `tests/{schema,query}.test.ts`. *(Task 7)*

**rust-client (`rust-client/src/` + inline tests):** `schema.rs` (FieldType, IndexDef, builders), `query.rs` (Query, TableQuery), `wire.rs` (VectorSearchQuery), `lib.rs` (re-exports). *(Task 8)*

**Docs:** `FEATURE_MATRIX.md` (#17 flip), `deploy/README.md` (image note). *(Task 9)*

---

## Task 1: Dev + prod Postgres image → pgvector, extension in create_database

**Files:**
- Modify: `docker-compose.dev.yml` (image line)
- Modify: `docker-compose.yml` (image line)
- Modify: `server/src/db.rs` (one statement in `create_database`)
- Test: `server/tests/vector_test.rs` (create)

**Interfaces:**
- Consumes: `db::create_database` (existing).
- Produces: the `vector` Postgres extension available in every database (new + existing, the latter via Task 4's `push_schema`); a guard test proving it.

- [ ] **Step 1: Swap the dev image**

In `docker-compose.dev.yml`, change the `postgres` service image:

```yaml
    image: pgvector/pgvector:pg17
```

(was `postgres:17` — same Postgres 17, adds the `vector` extension.)

- [ ] **Step 2: Swap the prod image**

In `docker-compose.yml`, same one-line change:

```yaml
    image: pgvector/pgvector:pg17
```

- [ ] **Step 3: Recreate the dev DB container with the new image**

Run:
```bash
make dev-db-down
make dev-db-up
```
Expected: a healthy `pgvector/pgvector:pg17` container on `127.0.0.1:55434`. (The named volume `rtdb-dev-pg` is reused; data persists.)

- [ ] **Step 4: Add `CREATE EXTENSION vector` to `create_database`**

In `server/src/db.rs`, inside `create_database`, immediately after the `CREATE SCHEMA "{schema_name}"` statement and before the `meta` table creation, add:

```rust
    sqlx::query("CREATE EXTENSION IF NOT EXISTS vector")
        .execute(&mut *tx)
        .await?;
```

Extensions are database-level in Postgres (not per-schema), and par-rt-db stores every "database" as a schema inside the single `rtdb` Postgres database — so this installs `vector` once into `rtdb`, available to all schemas. `IF NOT EXISTS` makes it idempotent. `rtdb` is a superuser in both compose setups, so no privilege grant is needed.

- [ ] **Step 5: Write the extension-availability guard test**

Create `server/tests/vector_test.rs`:

```rust
mod common;

use common::test_state;
use sqlx::Row;

#[tokio::test]
async fn pgvector_extension_available_after_db_create() {
    let state = test_state().await;
    // fresh_db creates a database, which now runs CREATE EXTENSION vector.
    let db_name = common::fresh_db(&state).await;

    let row = sqlx::query("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
        .fetch_one(&state.pool)
        .await
        .expect("vector extension row");
    let version: String = row.get("extversion");
    assert!(!version.is_empty(), "vector extension installed: {version}");

    // And the cosine-distance operator resolves (proves the extension is usable).
    let dist: f64 = sqlx::query_scalar("SELECT '[1,0,0]'::vector <=> '[0,1,0]'::vector")
        .fetch_one(&state.pool)
        .await
        .expect("cosine distance");
    assert!((dist - 1.0).abs() < 1e-6, "orthogonal vectors have cosine distance 1, got {dist}");

    let _ = db_name; // created; isolation by unique name
}
```

- [ ] **Step 6: Run the test**

Run: `cargo test --test vector_test pgvector_extension_available_after_db_create`
Expected: PASS. If it fails with `extension "vector" does not exist`, the dev image did not pick up the change — re-run Step 3.

- [ ] **Step 7: Commit**

```bash
git add docker-compose.dev.yml docker-compose.yml server/src/db.rs server/tests/vector_test.rs
git commit -m "feat(server): pgvector image + CREATE EXTENSION vector (#17)"
```

---

## Task 2: `Vector` field type (schema.rs)

**Files:**
- Modify: `server/src/schema.rs`
- Test: `server/tests/vector_test.rs` (append), `server/src/schema.rs` inline tests

**Interfaces:**
- Consumes: `FieldType`, `validate_value`, `type_tag`, `indexed_column_type` (existing).
- Produces: `FieldType::Vector { dimensions: u32 }` (wire `{"type":"vector","dimensions":N}`); validation that a `Vector` value is an array of exactly `dimensions` finite numbers; `Vector` is rejected by `indexed_column_type` (not btree-indexable).

- [ ] **Step 1: Add the variant + a failing wire round-trip test**

In `server/src/schema.rs`, add `Vector` to the `FieldType` enum (after `Record`):

```rust
    Record { value: Box<FieldType> },
    Vector { dimensions: u32 },
```

Add a failing test in the `#[cfg(test)] mod tests` block:

```rust
    #[test]
    fn vector_field_type_round_trips() {
        let v = FieldType::Vector { dimensions: 1536 };
        let json = serde_json::to_value(&v).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"type":"vector","dimensions":1536})
        );
        let back: FieldType = serde_json::from_value(json).unwrap();
        assert_eq!(back, v);
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test --lib vector_field_type_round_trips`
Expected: FAIL — `Vector { dimensions }` does not serialize with `deny_unknown_fields` / the enum derive needs the variant to compile (it compiles, but `type_tag`/validation may not cover it yet). If it already passes, proceed.

- [ ] **Step 3: Add `Vector` to `type_tag`**

Find `fn type_tag` in `schema.rs` and add the arm:

```rust
        FieldType::Vector { .. } => "vector",
```

- [ ] **Step 4: Make `Vector` not btree-indexable**

`indexed_column_type` already falls through to its `other => Err(...` arm for any unhandled variant. Verify `FieldType::Vector { .. }` is not listed there (it must reach the `other` arm). No code change unless a prior arm matches — it should not. Add a focused test in the inline tests:

```rust
    #[test]
    fn vector_is_not_btree_indexable() {
        assert!(indexed_column_type(&FieldType::Vector { dimensions: 3 }).is_err());
    }
```

- [ ] **Step 5: Add `Vector` validation to `validate_value`**

Find `fn validate_value` (the recursive validator) and add a branch handling `Vector`. It must accept only a JSON array of exactly `dimensions` finite numbers. Locate the existing `match ty` in `validate_value` and add (mirror the style of the `Array` arm):

```rust
        FieldType::Vector { dimensions } => {
            let arr = value.as_array().ok_or_else(|| {
                RtDbError::schema(format!("vector field expects an array of {dimensions} numbers"))
            })?;
            if arr.len() != *dimensions as usize {
                return Err(RtDbError::schema(format!(
                    "vector field expects {dimensions} numbers, got {}",
                    arr.len()
                )));
            }
            for el in arr {
                let n = el
                    .as_f64()
                    .ok_or_else(|| RtDbError::schema("vector field has a non-numeric entry"))?;
                if !n.is_finite() {
                    return Err(RtDbError::schema(
                        "vector field has a non-finite (NaN/Infinity) entry",
                    ));
                }
            }
            Ok(())
        }
```

- [ ] **Step 6: Write the validation tests**

Append to the inline tests:

```rust
    #[test]
    fn vector_validate_accepts_exact_length_finite() {
        let ty = FieldType::Vector { dimensions: 3 };
        assert!(validate_value(&ty, &serde_json::json!([1.0, -2.5, 0.0])).is_ok());
    }

    #[test]
    fn vector_validate_rejects_wrong_length() {
        let ty = FieldType::Vector { dimensions: 3 };
        assert!(validate_value(&ty, &serde_json::json!([1.0, 2.0])).is_err());
    }

    #[test]
    fn vector_validate_rejects_non_finite() {
        let ty = FieldType::Vector { dimensions: 2 };
        assert!(validate_value(
            &ty,
            &serde_json::json!([1.0, serde_json::Value::from(f64::NAN)])
        )
        .is_err());
    }
```

- [ ] **Step 7: Run the tests**

Run: `cargo test --lib vector_`
Expected: all PASS.

- [ ] **Step 8: Commit**

```bash
git add server/src/schema.rs
git commit -m "feat(server): Vector field type with dim/finiteness validation (#17)"
```

---

## Task 3: Vector index declaration on `IndexDef` (schema.rs)

**Files:**
- Modify: `server/src/schema.rs`
- Test: `server/src/schema.rs` inline tests

**Interfaces:**
- Consumes: `IndexDef`, `TableDef::validate_structure`, `indexed_column_type` (Task 2).
- Produces: `IndexDef.vector: Option<VectorIndexSpec>` (camelCase wire, omitted when absent); validation rules: exactly one of `search`/`vector`, single `Vector` field with matching dimensions, `filterFields` are scalar-indexable types.

- [ ] **Step 1: Add the spec type + field, with a failing round-trip test**

In `server/src/schema.rs`, add the spec struct next to `IndexDef`:

```rust
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorIndexSpec {
    pub dimensions: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub filter_fields: Vec<String>,
}
```

Add the field to `IndexDef` (after `search`):

```rust
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<VectorIndexSpec>,
```

Add a failing inline test:

```rust
    #[test]
    fn vector_index_round_trips_and_btree_omits_it() {
        let json = serde_json::json!({
            "name": "by_embedding",
            "fields": ["embedding"],
            "vector": {"dimensions": 4, "filterFields": ["userId"]}
        });
        let idx: IndexDef = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(idx.vector.as_ref().unwrap().dimensions, 4);
        assert_eq!(idx.vector.as_ref().unwrap().filter_fields, vec!["userId".to_string()]);
        // round-trips byte-identical
        assert_eq!(serde_json::to_value(&idx).unwrap(), json);

        // a btree index omits `vector` entirely
        let btree: IndexDef =
            serde_json::from_value(serde_json::json!({"name":"by_name","fields":["name"]})).unwrap();
        assert!(btree.vector.is_none());
        assert!(serde_json::to_value(&btree).unwrap().get("vector").is_none());
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test --lib vector_index_round_trips`
Expected: FAIL (existing `IndexDef` literals in the codebase now lack `vector`; the test itself should compile once the field is added — if other code constructs `IndexDef` literally, those call sites need `vector: None` added; fix them in Step 3).

- [ ] **Step 3: Add `vector: None` to every existing `IndexDef { … }` literal**

Search the server for `IndexDef {` struct literals (test fixtures and any construction). Add `vector: None,` to each. (There should be only a handful in `schema.rs` tests.) Run `cargo build --tests` to surface every missing field; fix each.

- [ ] **Step 4: Add validation to `TableDef::validate_structure`**

In `validate_structure`, the existing loop iterates `self.indexes` and validates `index.name`. Extend the per-index body. After the existing `search`-specific checks (if any — search currently validates field text-ness elsewhere), add the vector-index rules. Locate where each `index` is checked and add:

```rust
            // An index is exactly one of: btree, search, or vector.
            if index.search && index.vector.is_some() {
                return Err(RtDbError::schema(format!(
                    "index '{}' cannot be both search and vector",
                    index.name
                )));
            }
            if let Some(vec_spec) = &index.vector {
                // exactly one field, naming a Vector field of matching dimensions
                if index.fields.len() != 1 {
                    return Err(RtDbError::schema(format!(
                        "vector index '{}' must declare exactly one vector field",
                        index.name
                    )));
                }
                let vfield = &index.fields[0];
                let fty = table.fields.get(vfield).ok_or_else(|| {
                    RtDbError::schema(format!(
                        "vector index '{}' references unknown field '{vfield}'",
                        index.name
                    ))
                })?;
                match fty {
                    FieldType::Vector { dimensions } if *dimensions == vec_spec.dimensions => {}
                    _ => {
                        return Err(RtDbError::schema(format!(
                            "vector index '{}' field '{vfield}' must be Vector{{dimensions:{}}}",
                            index.name, vec_spec.dimensions
                        )))
                    }
                }
                // filterFields must be scalar-indexable
                for ff in &vec_spec.filter_fields {
                    let fty = table.fields.get(ff).ok_or_else(|| {
                        RtDbError::schema(format!(
                            "vector index '{}' filterField '{ff}' is not a declared field",
                            index.name
                        ))
                    })?;
                    if indexed_column_type(fty).is_err() {
                        return Err(RtDbError::schema(format!(
                            "vector index '{}' filterField '{ff}' must be a scalar indexable type",
                            index.name
                        )));
                    }
                }
            }
```

(`table` is the `&TableDef` being validated — `self`; if the loop uses `self.fields`, adjust the receiver name to match.)

- [ ] **Step 5: Write validation tests**

Append to inline tests:

```rust
    #[test]
    fn vector_index_rejects_dimension_mismatch() {
        let mut fields = BTreeMap::new();
        fields.insert("embedding".to_string(), FieldType::Vector { dimensions: 4 });
        let table = TableDef {
            fields,
            indexes: vec![IndexDef {
                name: "by_emb".to_string(),
                fields: vec!["embedding".to_string()],
                search: false,
                vector: Some(VectorIndexSpec { dimensions: 8, filter_fields: vec![] }),
            }],
        };
        assert!(table.validate_structure("docs").is_err());
    }

    #[test]
    fn vector_index_accepts_matching_dims_and_filter_fields() {
        let mut fields = BTreeMap::new();
        fields.insert("embedding".to_string(), FieldType::Vector { dimensions: 4 });
        fields.insert("userId".to_string(), FieldType::String);
        let table = TableDef {
            fields,
            indexes: vec![IndexDef {
                name: "by_emb".to_string(),
                fields: vec!["embedding".to_string()],
                search: false,
                vector: Some(VectorIndexSpec {
                    dimensions: 4,
                    filter_fields: vec!["userId".to_string()],
                }),
            }],
        };
        assert!(table.validate_structure("docs").is_ok());
    }

    #[test]
    fn vector_index_rejects_search_and_vector_both_set() {
        let mut fields = BTreeMap::new();
        fields.insert("embedding".to_string(), FieldType::Vector { dimensions: 4 });
        let table = TableDef {
            fields,
            indexes: vec![IndexDef {
                name: "by_emb".to_string(),
                fields: vec!["embedding".to_string()],
                search: true,
                vector: Some(VectorIndexSpec { dimensions: 4, filter_fields: vec![] }),
            }],
        };
        assert!(table.validate_structure("docs").is_err());
    }
```

(`validate_structure` is a method on `TableDef`; if it is private to the module, the inline `mod tests` inside `schema.rs` can still call it. If it takes `&self` plus `table_name: &str`, match that signature.)

- [ ] **Step 6: Run the tests**

Run: `cargo test --lib vector_index`
Expected: all PASS.

- [ ] **Step 7: Run the full schema test binary + clippy**

Run: `cargo test --test schema_validators_test` then `cargo clippy --all-targets -- -D warnings`
Expected: PASS (no regressions; no new warnings).

- [ ] **Step 8: Commit**

```bash
git add server/src/schema.rs
git commit -m "feat(server): vector index declaration on IndexDef (#17)"
```

---

## Task 4: DDL — vector column, HNSW index, `indexed_fields` extension, destructive guard

**Files:**
- Modify: `server/src/ddl.rs`
- Test: `server/tests/vector_test.rs` (append)

**Interfaces:**
- Consumes: `IndexDef.vector`, `FieldType::Vector` (Tasks 2–3); `pg_col`, `pg_schema`, `pg_table`, `pg_search_col` (existing); `create_database` extension (Task 1).
- Produces: `pg_vector_col()`; a `vector(N)` column `v_<index>` per vector index + `USING hnsw (col vector_cosine_ops)`; `CREATE EXTENSION vector` in `push_schema`; `indexed_fields` extended so vector `filterFields` get `f_` columns (and the vector field does not); a destructive-change guard on the vector spec.

- [ ] **Step 1: Add `pg_vector_col` and a failing DDL test**

In `server/src/ddl.rs`, next to `pg_search_col`, add:

```rust
/// Physical name of a vector index's `vector(N)` column. Table-scoped, so `v_`
/// + the lowercased index name stays within Postgres's 63-byte limit.
pub fn pg_vector_col(index_name: &str) -> String {
    format!("v_{}", index_name.to_lowercase())
}
```

Append a failing test to `server/tests/vector_test.rs`:

```rust
use rtdb_server::ddl::push_schema;
use rtdb_server::schema::{FieldType, IndexDef, SchemaDef, TableDef, VectorIndexSpec};
use std::collections::BTreeMap;

fn vector_schema(dim: u32, with_filter: bool) -> SchemaDef {
    let mut fields = BTreeMap::new();
    fields.insert("embedding".to_string(), FieldType::Vector { dimensions: dim });
    if with_filter {
        fields.insert("userId".to_string(), FieldType::String);
    }
    let mut indexes = vec![IndexDef {
        name: "by_embedding".to_string(),
        fields: vec!["embedding".to_string()],
        search: false,
        vector: Some(VectorIndexSpec {
            dimensions: dim,
            filter_fields: if with_filter { vec!["userId".to_string()] } else { vec![] },
        }),
    }];
    let _ = &mut indexes;
    let mut tables = BTreeMap::new();
    tables.insert("docs".to_string(), TableDef { fields, indexes });
    SchemaDef { tables }
}

#[tokio::test]
async fn push_schema_creates_vector_column_and_hnsw_index() {
    let state = common::test_state().await;
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &db).await.unwrap();
    push_schema(&state.pool, &db, vector_schema(3, true)).await.unwrap();

    // The vector column exists with type vector(3).
    let col: (String,) = sqlx::query_as(
        "SELECT format_type(a.atttypid, a.atttypmod) \
         FROM pg_attribute a JOIN pg_class c ON a.attrelid = c.oid \
         JOIN pg_namespace n ON c.relnamespace = n.oid \
         WHERE n.nspname = $1 AND c.relname = $2 AND a.attname = 'v_by_embedding'",
    )
    .bind(format!("db_{db}"))
    .bind("t_docs")
    .fetch_one(&state.pool)
    .await
    .unwrap();
    assert_eq!(col.0, "vector(3)");

    // An HNSW index exists on it.
    let idx: (i64,) = sqlx::query_as(
        "SELECT count(*) FROM pg_indexes \
         WHERE schemaname = $1 AND tablename = $2 AND indexname = 'i_docs_by_embedding'",
    )
    .bind(format!("db_{db}"))
    .bind("t_docs")
    .fetch_one(&state.pool)
    .await
    .unwrap();
    assert_eq!(idx.0, 1);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test --test vector_test push_schema_creates_vector_column_and_hnsw_index`
Expected: FAIL — no `v_by_embedding` column yet.

- [ ] **Step 3: Extend `indexed_fields` for vector filterFields**

In `server/src/ddl.rs`, find `fn indexed_fields(table: &TableDef) -> BTreeSet<String>`. Replace its body so it includes vector-index `filter_fields` and **excludes** the vector index's single vector field (which is owned by the `v_` column):

```rust
fn indexed_fields(table: &TableDef) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for index in &table.indexes {
        if let Some(vec_spec) = &index.vector {
            // vector index: its filterFields get typed f_ columns; its single
            // vector field does NOT (it is not scalar-indexable — owned by v_).
            for ff in &vec_spec.filter_fields {
                names.insert(ff.clone());
            }
        } else {
            // btree or search index: all of its `fields` get f_ columns.
            for field_name in &index.fields {
                names.insert(field_name.clone());
            }
        }
    }
    names
}
```

- [ ] **Step 4: Ensure `CREATE EXTENSION vector` runs in `push_schema`**

In `push_schema`, as the **first** statement after `let mut tx = pool.begin().await?;` (and before the `for (table_name, …)` loop), add:

```rust
    sqlx::query("CREATE EXTENSION IF NOT EXISTS vector")
        .execute(&mut *tx)
        .await?;
```

This covers existing databases (created before Task 1 shipped) the first time a vector-index schema is pushed.

- [ ] **Step 5: Add vector-index DDL in the index loop**

In `push_schema`'s index loop (`for index in &new_table.indexes`), the `if index.search { … } else { /* btree */ }` branch gains a third arm. Restructure to:

```rust
            if index.search {
                // …existing tsvector + GIN block unchanged…
            } else if let Some(vec_spec) = &index.vector {
                // Vector index: a plain vector(N) column (write-maintained, not
                // generated — pgvector has no jsonb->vector generated cast) plus
                // an HNSW cosine index. filterFields' f_ columns already exist
                // (created with the table / added+backfilled just above).
                let v_col = pg_vector_col(&index.name);
                let dim = vec_spec.dimensions;
                let vfield = index
                    .fields
                    .first()
                    .expect("validated: vector index has one field");
                sqlx::query(&format!(
                    "ALTER TABLE \"{pg_schema_name}\".\"{table_ident}\" \
                     ADD COLUMN \"{v_col}\" vector({dim})"
                ))
                .execute(&mut *tx)
                .await?;
                // Backfill from existing rows (no-op on a brand-new table).
                sqlx::query(&format!(
                    "UPDATE \"{pg_schema_name}\".\"{table_ident}\" \
                     SET \"{v_col}\" = (doc->>'{vfield}')::vector \
                     WHERE doc ? '{vfield}'"
                ))
                .execute(&mut *tx)
                .await?;
                sqlx::query(&format!(
                    "CREATE INDEX \"{index_ident}\" ON \"{pg_schema_name}\".\"{table_ident}\" \
                     USING hnsw (\"{v_col}\" vector_cosine_ops)"
                ))
                .execute(&mut *tx)
                .await?;
            } else {
                // …existing btree block unchanged…
            }
```

(`vfield` is the doc field name; it is a validated identifier, safe in `doc->>'{vfield}'` which is a string literal not an identifier — but it was validated by `is_valid_identifier` in Task 3's validation, so injection-safe. `index_ident`, `pg_schema_name`, `table_ident` are existing locals.)

- [ ] **Step 6: Add the destructive-spec guard**

In `detect_destructive_changes`, the index-match arm currently checks `new_index.fields != old_index.fields` and `new_index.search != old_index.search`. Add a vector-spec guard alongside them:

```rust
                Some(new_index) if new_index.vector != old_index.vector => {
                    return Err(RtDbError::bad_request(format!(
                        "changed vector spec of index '{}'",
                        old_index.name
                    )));
                }
```

(`VectorIndexSpec` derives `PartialEq` — added in Task 3 — and `Option` equals, so this compiles.)

- [ ] **Step 7: Run the test**

Run: `cargo test --test vector_test push_schema_creates_vector_column_and_hnsw_index`
Expected: PASS.

- [ ] **Step 8: Add a destructive-change test**

Append to `server/tests/vector_test.rs`:

```rust
#[tokio::test]
async fn changing_vector_dims_is_rejected() {
    let state = common::test_state().await;
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &db).await.unwrap();
    push_schema(&state.pool, &db, vector_schema(3, false)).await.unwrap();
    let err = push_schema(&state.pool, &db, vector_schema(4, false)).await;
    assert!(err.is_err(), "changing dimensions must be rejected");
}
```

Run: `cargo test --test vector_test changing_vector_dims_is_rejected` — Expected: PASS.

- [ ] **Step 9: Run clippy + commit**

Run: `cargo clippy --all-targets -- -D warnings` — Expected: clean.
```bash
git add server/src/ddl.rs server/tests/vector_test.rs
git commit -m "feat(server): vector column + HNSW DDL, indexed_fields extension (#17)"
```

---

## Task 5: Write path — maintain the `v_` column on insert/patch/replace

**Files:**
- Modify: `server/src/txn.rs`
- Test: `server/tests/vector_test.rs` (append)

**Interfaces:**
- Consumes: `table_columns`, `column_binds`, `ColBind`, `do_insert`, `apply_update`, `insert_snapshot_row` (existing); `pg_vector_col` (Task 4).
- Produces: `TableColumn`/`ColumnKind` types; `table_columns` returning `Vec<TableColumn>` (scalar f_ cols incl. vector filterFields, plus one `v_<index>` vector col per vector index); `ColBind::Vector(Option<String>)`; `$n::vector` placeholders in insert/update so the column is written from the doc's vector field.

- [ ] **Step 1: Define `TableColumn`/`ColumnKind` and a failing write test**

Near the top of `server/src/txn.rs` (after `ColBind`), add:

```rust
/// The kind of an indexed column: a scalar value stored in an `f_<field>`
/// column, or a vector stored in a `v_<index>` column.
enum ColumnKind {
    Scalar(FieldType),
    Vector(u32),
}

/// One physical indexed column: its physical name (`f_<field>` or `v_<index>`),
/// the doc field its value is read from, and its kind.
struct TableColumn {
    col: String,
    field: String,
    kind: ColumnKind,
}
```

Append a failing test to `server/tests/vector_test.rs`:

```rust
use rtdb_server::query::{Query, QueryResult, execute_query};
use rtdb_server::txn::{Step, Transaction, execute_txn};

fn vec_doc(emb: Vec<f64>) -> serde_json::Map<String, serde_json::Value> {
    serde_json::json!({ "embedding": emb }).as_object().unwrap().clone()
}

async fn vec_db(state: &std::sync::Arc<rtdb_server::AppState>) -> String {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name).await.unwrap();
    push_schema(&state.pool, &name, vector_schema(3, false)).await.unwrap();
    name
}

#[tokio::test]
async fn insert_writes_vector_column_and_search_ranks() {
    let state = common::test_state().await;
    let db = vec_db(&state).await;
    let schema = vector_schema(3, false);
    for emb in [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.9, 0.1, 0.0]] {
        execute_txn(
            &state.pool, &db, &schema,
            &Transaction { steps: vec![Step::Insert { table: "docs".into(), doc: vec_doc(emb.into_iter().map(|x| x as f64).collect()) }] },
        ).await.unwrap();
    }
    // Query nearest to [1,0,0]: the identical vector ranks first, then [0.9,0.1,0].
    let q = Query {
        table: "docs".into(),
        vector_search: Some(rtdb_server::query::VectorSearchQuery {
            index: "by_embedding".into(),
            vector: vec![1.0, 0.0, 0.0],
            limit: 2,
            filter: Default::default(),
        }),
        ..Default::default()
    };
    let res = execute_query(&state.pool, &db, &schema, &q).await.unwrap();
    let docs = match res { QueryResult::Docs(d) => d, _ => panic!("expected Docs") };
    assert_eq!(docs.len(), 2);
    // The identical [1,0,0] doc is closest (distance 0) -> first.
    assert_eq!(docs[0]["embedding"], serde_json::json!([1.0, 0.0, 0.0]));
}
```

(Note: this test references `Query::vector_search`, `VectorSearchQuery`, and `execute_query` handling of the terminal — those land in Task 6. To keep Task 5 independently testable, run it against the DB directly with raw SQL for the assertion until Task 6 lands. **Alternative Step 1 test** below avoids the Task 6 dependency.)

- [ ] **Step 1b (use this until Task 6): raw-SQL write test**

Replace the query portion of the test above with a direct column read, so Task 5 verifies the write path alone:

```rust
#[tokio::test]
async fn insert_maintains_vector_column() {
    let state = common::test_state().await;
    let db = vec_db(&state).await;
    let schema = vector_schema(3, false);
    execute_txn(
        &state.pool, &db, &schema,
        &Transaction { steps: vec![Step::Insert { table: "docs".into(), doc: vec_doc(vec![1.0, 2.0, 3.0]) }] },
    ).await.unwrap();
    let row: (Option<String>,) = sqlx::query_as(&format!(
        "SELECT \"v_by_embedding\"::text FROM \"db_{db}\".\"t_docs\""
    )).fetch_one(&state.pool).await.unwrap();
    assert_eq!(row.0.as_deref(), Some("[1,2,3]"));
}
```

Run: `cargo test --test vector_test insert_maintains_vector_column` — Expected: FAIL (column written NULL or wrong).

- [ ] **Step 2: Refactor `table_columns` to return `Vec<TableColumn>`**

Replace `fn table_columns`:

```rust
fn table_columns(table: &TableDef) -> Result<Vec<TableColumn>, RtDbError> {
    use crate::ddl::pg_vector_col;
    // Scalar f_<field> columns: btree/search index fields + vector-index filterFields.
    let mut scalar_fields: BTreeSet<String> = BTreeSet::new();
    for index in &table.indexes {
        if let Some(vec_spec) = &index.vector {
            for ff in &vec_spec.filter_fields {
                scalar_fields.insert(ff.clone());
            }
        } else {
            for f in &index.fields {
                scalar_fields.insert(f.clone());
            }
        }
    }
    let mut cols: Vec<TableColumn> = scalar_fields
        .into_iter()
        .map(|field| {
            let ty = table.fields.get(&field).cloned().ok_or_else(|| {
                RtDbError::internal(format!("index references unknown field '{field}'"))
            })?;
            Ok(TableColumn {
                col: pg_col(&field),
                field: field.clone(),
                kind: ColumnKind::Scalar(ty),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    // Vector v_<index> columns: one per vector index, reading its vector field.
    for index in &table.indexes {
        if let Some(vec_spec) = &index.vector {
            let field = index
                .fields
                .first()
                .cloned()
                .ok_or_else(|| RtDbError::internal("vector index missing field"))?;
            cols.push(TableColumn {
                col: pg_vector_col(&index.name),
                field,
                kind: ColumnKind::Vector(vec_spec.dimensions),
            });
        }
    }
    // Deterministic order: sort by physical column name so insert/update line up.
    cols.sort_by(|a, b| a.col.cmp(&b.col));
    Ok(cols)
}
```

- [ ] **Step 3: Extend `ColBind` and `column_binds`/`column_bind_for`**

Add the variant:

```rust
enum ColBind {
    Text(Option<String>),
    Num(Option<f64>),
    Bool(Option<bool>),
    /// pgvector text form "[a,b,c]" (NULL when None). Bound against a `$n::vector`
    /// placeholder; the column type is `vector(N)`.
    Vector(Option<String>),
}
```

Change `column_binds` to take `&[TableColumn]` and `column_bind_for` to take `&ColumnKind`:

```rust
fn column_binds(
    columns: &[TableColumn],
    doc: &serde_json::Map<String, serde_json::Value>,
) -> Result<Vec<ColBind>, RtDbError> {
    columns
        .iter()
        .map(|c| {
            let value = doc.get(&c.field).cloned().unwrap_or(serde_json::Value::Null);
            column_bind_for(&c.kind, &value)
        })
        .collect()
}

fn column_bind_for(kind: &ColumnKind, value: &serde_json::Value) -> Result<ColBind, RtDbError> {
    match kind {
        ColumnKind::Scalar(ty) => {
            // existing scalar logic, unchanged — call indexed_column_type(ty)? and
            // match on pg_type exactly as today, returning Text/Num/Bool.
            scalar_bind(ty, value)
        }
        ColumnKind::Vector(_dim) => {
            if value.is_null() {
                return Ok(ColBind::Vector(None));
            }
            // Defensive only: schema validation already enforced exact length +
            // finiteness. pgvector parses the JSON-array text form "[a,b,c]".
            Ok(ColBind::Vector(Some(value.to_string())))
        }
    }
}
```

Where `scalar_bind(ty, value)` is the **existing** body of the old `column_bind_for` (rename the old fn to `scalar_bind`, keep its logic byte-for-byte — the `indexed_column_type` + `match pg_type` returning `Text`/`Num`/`Bool`).

- [ ] **Step 4: Thread `$n::vector` placeholders through `do_insert`**

In `do_insert`, replace the uniform placeholder construction with per-column placeholders. The column list and binds now come from `Vec<TableColumn>`. Replace the `col_names`/`placeholders` block with:

```rust
    let columns = table_columns(table_def)?;
    let binds = column_binds(&columns, doc)?;

    let table_ident = pg_table(table_name);
    let mut col_names = vec![
        "\"id\"".to_string(),
        "\"doc\"".to_string(),
        "\"created_at\"".to_string(),
    ];
    let mut placeholders = vec!["$1".to_string(), "$2".to_string(), "$3".to_string()];
    let mut idx = 3usize;
    for c in &columns {
        idx += 1;
        col_names.push(format!("\"{}\"", c.col));
        let ph = match c.kind {
            ColumnKind::Vector(_) => format!("${idx}::vector"),
            _ => format!("${idx}"),
        };
        placeholders.push(ph);
    }
```

And extend the bind loop:

```rust
    for bind in binds {
        query = match bind {
            ColBind::Text(v) => query.bind(v),
            ColBind::Num(v) => query.bind(v),
            ColBind::Bool(v) => query.bind(v),
            ColBind::Vector(v) => query.bind(v),
        };
    }
```

- [ ] **Step 5: Thread `::vector` through `apply_update` (patch/replace)**

In `apply_update`, the set-clause loop currently does `format!("\"{}\" = ${idx}", pg_col(name))`. Change it to iterate `&columns` (`Vec<TableColumn>`) and cast vector columns:

```rust
    let columns = table_columns(table_def)?;
    let binds = column_binds(&columns, &merged)?;
    let mut set_clauses = vec![
        "\"doc\" = $1".to_string(),
        "\"version\" = \"version\" + 1".to_string(),
    ];
    let mut idx = 2usize;
    for c in &columns {
        let cast = match c.kind {
            ColumnKind::Vector(_) => "::vector",
            _ => "",
        };
        set_clauses.push(format!("\"{}\" = ${idx}{cast}", c.col));
        idx += 1;
    }
    let id_placeholder = idx;
```

Extend the bind loop identically to Step 4 (`ColBind::Vector(v) => query.bind(v)`).

- [ ] **Step 6: Thread `::vector` through `insert_snapshot_row`**

`insert_snapshot_row` mirrors `do_insert`. Apply the same per-column placeholder change (its fixed prefix is `id, doc, created_at, version` → `$1..$4`, columns start at `$5`). Extend its bind loop with the `Vector` arm too.

- [ ] **Step 7: Update the patch path (vector column recomputed from merged doc)**

`do_patch` calls `apply_update` with `merged`. Since `table_columns` now includes the vector column and `column_binds` reads `merged[vectorField]`, a patch that changes the embedding recomputes `v_<index>`. Verify `apply_patch` lets a `Vector` field be patched (it calls `validate_value`, which now accepts `Vector` from Task 2). No code change expected here — confirm by test.

- [ ] **Step 8: Run the write test + the existing txn suite**

Run: `cargo test --test vector_test insert_maintains_vector_column`
Run: `cargo test --test txn_test`
Expected: both PASS (no regressions in existing insert/patch/upsert/replace).

- [ ] **Step 9: Run clippy + commit**

Run: `cargo clippy --all-targets -- -D warnings` — Expected: clean.
```bash
git add server/src/txn.rs server/tests/vector_test.rs
git commit -m "feat(server): maintain vector column on insert/patch/replace (#17)"
```

---

## Task 6: Read path — `vectorSearch` terminal + reactive re-run

**Files:**
- Modify: `server/src/query.rs`
- Test: `server/tests/vector_test.rs` (append)

**Interfaces:**
- Consumes: `Query`, `QueryResult`, `execute_search` (template), `merge_doc`, `pg_vector_col` (Task 4), `eq_bind_for`/`EqBind` (existing), `table_def.index(name)`.
- Produces: `VectorSearchQuery { index, vector, limit, filter }`; `Query.vector_search` (wire `vectorSearch`); `VECTOR_SEARCH_MAX_LIMIT = 256`; `execute_vector_search`; the terminal's mutual-exclusion guard.

- [ ] **Step 1: Add the wire type + Query field + a failing ranking test**

In `server/src/query.rs`, near `SearchQuery`, add:

```rust
/// A vector-similarity terminal over a declared vector index. `vector` is the
/// caller-supplied query embedding (length must equal the index dimensions);
/// ranked by cosine distance (`<=>`) ascending. `filter` is an optional eq-map
/// over the index's declared `filterFields`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VectorSearchQuery {
    pub index: String,
    pub vector: Vec<f32>,
    pub limit: u32,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub filter: BTreeMap<String, serde_json::Value>,
}
```

Add the field to `Query` (after `search`):

```rust
    #[serde(default, rename = "vectorSearch")]
    pub vector_search: Option<VectorSearchQuery>,
```

Add the constant near `MAX_TAKE`:

```rust
/// Hard cap on `vectorSearch` `limit`.
const VECTOR_SEARCH_MAX_LIMIT: u32 = 256;
```

(If `BTreeMap` is not already imported in `query.rs`, add `use std::collections::BTreeMap;`.)

Append a failing ranking test to `server/tests/vector_test.rs` (replace the placeholder from Task 5 Step 1 with this real query test):

```rust
#[tokio::test]
async fn vector_search_ranks_by_cosine_and_applies_limit() {
    let state = common::test_state().await;
    let db = vec_db(&state).await;
    let schema = vector_schema(3, false);
    for emb in [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.9, 0.4, 0.0]] {
        execute_txn(&state.pool, &db, &schema,
            &Transaction { steps: vec![Step::Insert { table: "docs".into(), doc: vec_doc(emb.to_vec()) }] }
        ).await.unwrap();
    }
    let q = serde_json::from_value::<Query>(serde_json::json!({
        "table": "docs",
        "vectorSearch": {"index": "by_embedding", "vector": [1.0, 0.0, 0.0], "limit": 2}
    })).unwrap();
    let res = execute_query(&state.pool, &db, &schema, &q).await.unwrap();
    let docs = match res { QueryResult::Docs(d) => d, _ => panic!("expected Docs") };
    assert_eq!(docs.len(), 2, "limit honored");
    assert_eq!(docs[0]["embedding"], serde_json::json!([1.0, 0.0, 0.0]), "identical vector ranks first");
    // The omitted doc is [0,1,0] (farthest) — confirms ranking, not insertion order.
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test --test vector_test vector_search_ranks_by_cosine_and_applies_limit`
Expected: FAIL — `execute_query` does not yet dispatch `vectorSearch`.

- [ ] **Step 3: Add the mutual-exclusion guard + dispatch**

In `execute_query`, mirror the `search` dispatch block. Update the `get` exclusion list (add `|| q.vector_search.is_some()` and append `, or vector search` to its message), then add a vectorSearch block **before** the `search` block (or after — they are mutually exclusive):

```rust
    // Vector-similarity terminal. Incompatible with every other terminal; carries
    // its own limit (does not compose with take).
    if let Some(vs) = &q.vector_search {
        if q.index.is_some()
            || !q.eq.is_empty()
            || q.gt.is_some()
            || q.gte.is_some()
            || q.lt.is_some()
            || q.lte.is_some()
            || q.order.is_some()
            || q.unique
            || q.first
            || q.count
            || q.paginate.is_some()
            || q.filter.is_some()
            || q.search.is_some()
            || q.take.is_some()
        {
            return Err(RtDbError::bad_request(
                "vectorSearch cannot be combined with any other terminal",
            ));
        }
        return execute_vector_search(pool, db, table_def, &q.table, vs).await;
    }
```

Also add `|| q.vector_search.is_some()` to the `search` block's exclusion condition and to the `get` block's condition (and their error messages).

- [ ] **Step 4: Implement `execute_vector_search`**

Add near `execute_search` (it is the template):

```rust
/// Vector-similarity terminal: ranks rows by cosine distance (`<=>`) between the
/// index's `v_<index>` column and the query vector, ascending, limited to `limit`.
/// Optional `filter` eq-binds over the index's declared `filterFields`. Unknown
/// index / length mismatch / unknown filter key / out-of-range limit → BadRequest.
async fn execute_vector_search(
    pool: &PgPool,
    db: &str,
    table_def: &TableDef,
    table_name: &str,
    vs: &VectorSearchQuery,
) -> Result<QueryResult, RtDbError> {
    let index_def = table_def
        .indexes
        .iter()
        .find(|index| index.name == vs.index && index.vector.is_some())
        .ok_or_else(|| RtDbError::bad_request(format!("vector index '{}' not found", vs.index)))?;
    let vec_spec = index_def.vector.as_ref().expect("matched vector index");

    if vs.vector.len() != vec_spec.dimensions as usize {
        return Err(RtDbError::bad_request(format!(
            "vectorSearch vector length {} != index '{}' dimensions {}",
            vs.vector.len(), vs.index, vec_spec.dimensions
        )));
    }
    if !(1..=VECTOR_SEARCH_MAX_LIMIT).contains(&vs.limit) {
        return Err(RtDbError::bad_request(format!(
            "vectorSearch limit must be 1..={VECTOR_SEARCH_MAX_LIMIT}"
        )));
    }

    // Build eq-binds for any filter entries (must be declared filterFields).
    let mut filter_binds: Vec<EqBind> = Vec::new();
    let mut filter_cols: Vec<String> = Vec::new();
    for (k, v) in &vs.filter {
        if !vec_spec.filter_fields.iter().any(|f| f == k) {
            return Err(RtDbError::bad_request(format!(
                "vectorSearch filter key '{k}' is not a declared filterField of index '{}'",
                vs.index
            )));
        }
        let fty = table_def.fields.get(k).ok_or_else(|| {
            RtDbError::internal(format!("filterField '{k}' missing from table fields"))
        })?;
        filter_binds.push(eq_bind_for(fty, v)?);
        filter_cols.push(pg_col(k));
    }

    let v_col = crate::ddl::pg_vector_col(&index_def.name);
    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(table_name);

    // Query vector -> pgvector text "[a,b,c]".
    let qvec_text = format!(
        "[{}]",
        vs.vector.iter().map(|f| f.to_string()).collect::<Vec<_>>().join(",")
    );

    let mut where_clause = String::from("\"{v_col}\" IS NOT NULL");
    let mut bind_idx = 1usize;
    // filter eq-binds ($1..) first, then the query vector ($n), then limit.
    let mut placeholders: Vec<String> = Vec::new();
    for _ in &filter_cols {
        placeholders.push(format!("${bind_idx}"));
        bind_idx += 1;
    }
    let qvec_ph = bind_idx; bind_idx += 1;
    let limit_ph = bind_idx;
    if !filter_cols.is_empty() {
        let conds: Vec<String> = filter_cols
            .iter()
            .zip(placeholders.iter())
            .map(|(col, ph)| format!("\"{col}\" = {ph}"))
            .collect();
        where_clause = format!("{where_clause} AND {}", conds.join(" AND "));
    }

    let sql = format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\" \
         WHERE {where_clause} \
         ORDER BY \"{v_col}\" <=> ${qvec_ph}::vector \
         LIMIT ${limit_ph}"
    );

    let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64, i64)>(&sql);
    for b in filter_binds {
        query = match b {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
        };
    }
    let rows = query
        .bind(qvec_text)
        .bind(i64::from(vs.limit))
        .fetch_all(pool)
        .await?;
    let docs = rows
        .into_iter()
        .map(|(id, doc, created_at, version)| merge_doc(id, doc, created_at, version))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(QueryResult::Docs(docs))
}
```

(Confirm `eq_bind_for`'s signature `(ty, value) -> Result<EqBind>` and `EqBind` variants match the existing ones — they do, per `txn.rs`. `merge_doc` and `pg_col` are existing imports in `query.rs`/`ddl.rs`.)

- [ ] **Step 5: Add rejection tests**

Append to `server/tests/vector_test.rs`:

```rust
#[tokio::test]
async fn vector_search_rejects_length_mismatch() {
    let state = common::test_state().await;
    let db = vec_db(&state).await;
    let schema = vector_schema(3, false);
    let q = serde_json::from_value::<Query>(serde_json::json!({
        "table": "docs", "vectorSearch": {"index": "by_embedding", "vector": [1.0, 0.0], "limit": 1}
    })).unwrap();
    let err = execute_query(&state.pool, &db, &schema, &q).await;
    assert!(err.is_err());
}

#[tokio::test]
async fn vector_search_rejects_unknown_index() {
    let state = common::test_state().await;
    let db = vec_db(&state).await;
    let schema = vector_schema(3, false);
    let q = serde_json::from_value::<Query>(serde_json::json!({
        "table": "docs", "vectorSearch": {"index": "nope", "vector": [1.0, 0.0, 0.0], "limit": 1}
    })).unwrap();
    assert!(execute_query(&state.pool, &db, &schema, &q).await.is_err());
}

#[tokio::test]
async fn vector_search_applies_eq_filter() {
    let state = common::test_state().await;
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &db).await.unwrap();
    push_schema(&state.pool, &db, vector_schema(3, true)).await.unwrap();
    let schema = vector_schema(3, true);
    for (u, emb) in [("a", [1.0,0.0,0.0]), ("b", [1.0,0.0,0.0])] {
        let mut doc = vec_doc(emb.to_vec());
        doc.insert("userId".to_string(), serde_json::json!(u));
        execute_txn(&state.pool, &db, &schema,
            &Transaction { steps: vec![Step::Insert { table: "docs".into(), doc }] }).await.unwrap();
    }
    let q = serde_json::from_value::<Query>(serde_json::json!({
        "table": "docs",
        "vectorSearch": {"index": "by_embedding", "vector": [1.0,0.0,0.0], "limit": 10, "filter": {"userId": "a"}}
    })).unwrap();
    let res = execute_query(&state.pool, &db, &schema, &q).await.unwrap();
    let docs = match res { QueryResult::Docs(d) => d, _ => panic!("expected Docs") };
    assert_eq!(docs.len(), 1, "filter restricts to userId=a");
    assert_eq!(docs[0]["userId"], "a");
}
```

- [ ] **Step 6: Add a wire round-trip test (protocol)**

Append to `server/tests/vector_test.rs`:

```rust
#[test]
fn vector_search_wire_round_trips() {
    let q = serde_json::from_value::<Query>(serde_json::json!({
        "table": "docs",
        "vectorSearch": {"index": "by_embedding", "vector": [0.1, 0.2], "limit": 5, "filter": {"userId": "u1"}}
    })).unwrap();
    let back = serde_json::to_value(&q).unwrap();
    assert_eq!(back["vectorSearch"]["index"], "by_embedding");
    assert_eq!(back["vectorSearch"]["limit"], 5);
    assert_eq!(back["vectorSearch"]["filter"]["userId"], "u1");
    // camelCase on the wire
    assert!(back.get("vector_search").is_none());
}
```

- [ ] **Step 7: Run the full vector_test binary + clippy**

Run: `cargo test --test vector_test` — Expected: all PASS.
Run: `cargo clippy --all-targets -- -D warnings` — Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add server/src/query.rs server/tests/vector_test.rs
git commit -m "feat(server): vectorSearch query terminal, cosine-ranked, eq-filter (#17)"
```

- [ ] **Step 9 (reactivity): confirm push-on-change via a subscription test**

The terminal rides the committer's existing table-level invalidation — verify with a WS subscription. Append:

```rust
// Reactive re-run: a vectorSearch subscription updates when a new near-vector is inserted.
// (Use the ws_test harness pattern: subscribe to a vectorSearch query, insert a closer
// vector, assert the pushed QueryUpdate reorders.)
```

If the existing `ws_test.rs` harness makes this awkward, add it there instead, mirroring an existing `search` re-run test. If no `search` re-run test exists to mirror, skip the explicit reactive test for v1 (the path is identical to `search`, which is already covered) and note it in the commit message. **Decision:** mirror if a precedent exists; otherwise rely on the shared committer path and document the reliance.

---

## Task 7: ts-client mirror (wire + builders + tests)

**Files:**
- Modify: `ts-client/src/protocol.ts`
- Modify: `ts-client/src/schema.ts`
- Modify: `ts-client/src/query.ts`
- Modify: `ts-client/src/index.ts` (re-exports)
- Test: `ts-client/tests/schema.test.ts`, `ts-client/tests/query.test.ts`

**Interfaces:**
- Consumes: the server wire contract (Tasks 2–6): `{type:"vector",dimensions:N}`, `IndexJson.vector`, `QueryJson.vectorSearch`.
- Produces: `VectorQueryJson` type; `t.vector(dimensions)`; `TableDefinition.vectorIndex(name, field, dimensions, filterFields?)`; `TableQuery.vectorSearch(index, vector, opts)`; re-exports. (In-memory execution is intentionally out of scope for v1 — matches the existing `search` surface, which is wire/builder-only in the harness. Note as follow-up.)

**Reference (mirror these exactly):** `searchIndex` (`schema.ts:101-109`), `.search` (`query.ts:47-49`), `SearchQuery` (`protocol.ts:46-50`), `t.*` factories (`schema.ts:55-80`), `IndexJson.search` (`protocol.ts:157-162`).

- [ ] **Step 1: Wire types in `protocol.ts`**

Add the vector variant to `FieldTypeJson` (after `record`):

```ts
  | { type: "vector"; dimensions: number }
```

Add the vector index spec + extend `IndexJson`:

```ts
/** Mirrors server `VectorIndexSpec` (camelCase). Omitted on the wire when the index isn't a vector index. */
export interface VectorIndexSpec {
  dimensions: number;
  filterFields?: string[];
}
```

Extend `IndexJson` (add the optional field; keep `search`):

```ts
export interface IndexJson {
  name: string;
  fields: string[];
  /** `true` marks a full-text search index; omitted on the wire for ordinary btree indexes. */
  search?: boolean;
  /** Present marks a vector index; omitted otherwise. */
  vector?: VectorIndexSpec;
}
```

Add the terminal payload + extend `QueryJson`:

```ts
/** Mirrors server `VectorSearchQuery` byte-for-byte (camelCase, deny_unknown_fields). */
export interface VectorQuery {
  index: string;
  vector: number[];
  limit: number;
  filter?: Record<string, unknown>;
}
```

Add to `QueryJson` (after `search`):

```ts
  vectorSearch?: VectorQuery;
```

- [ ] **Step 2: `t.vector` factory in `schema.ts`**

Add to the `t` object (after `int64`):

```ts
  vector: (dimensions: number): Validator<number[]> =>
    makeValidator({ type: "vector", dimensions }),
```

- [ ] **Step 3: `vectorIndex` builder in `schema.ts`**

Add to `TableDefinition`, mirroring `searchIndex` (returns a new `TableDefinition` with one more index; widens the `Indexes` union):

```ts
  /** Declare a vector index. `field` is a Vector-typed field; the server stores a
   * pgvector column ranked by cosine distance via the `vectorSearch` query terminal.
   * `filterFields` are scalar fields usable as eq-filters in a vectorSearch. */
  vectorIndex<Name extends string>(
    name: Name,
    field: keyof Fields & string,
    dimensions: number,
    filterFields: (keyof Fields & string)[] = [],
  ): TableDefinition<Fields, Indexes | Name> {
    return new TableDefinition(this.fields, [
      ...this.indexes,
      {
        name,
        fields: [field],
        vector: { dimensions, ...(filterFields.length > 0 ? { filterFields: [...filterFields] } : {}) },
      },
    ]);
  }
```

- [ ] **Step 4: `.vectorSearch` builder in `query.ts`**

Add to `TableQuery`, mirroring `.search` (returns `Self` for chaining into `.take` if desired, though the terminal carries its own limit):

```ts
  /** Vector-similarity `vectorSearch` over a declared vector index. The server
   * ranks by cosine distance and applies `limit`; `filter` is an eq-map over the
   * index's declared filterFields. Terminal — the server rejects other terminals. */
  vectorSearch(
    index: string,
    vector: number[],
    opts: { limit: number; filter?: Record<string, unknown> },
  ): TableQuery<DocT, Indexes> {
    const vectorSearch: VectorQuery = { index, vector, limit: opts.limit, ...(opts.filter ? { filter: opts.filter } : {}) };
    return new TableQuery({ ...this.json, vectorSearch });
  }
```

(Import `VectorQuery` from `./protocol`.)

- [ ] **Step 5: Re-export from `index.ts`**

Add `VectorQuery` and `VectorIndexSpec` to the public re-exports (alongside `SearchQuery`/`IndexJson`).

- [ ] **Step 6: Tests**

In `tests/schema.test.ts`, add a `describe("vectorIndex builder", …)` mirroring the `searchIndex` block — assert `vectorIndex("by_embedding", "embedding", 4, ["userId"])` emits `{name, fields:["embedding"], vector:{dimensions:4, filterFields:["userId"]}}`, and a btree `.index()` omits `vector`.

In `tests/query.test.ts`, add `describe("TableQuery.vectorSearch", …)` mirroring `.search`:

```ts
  it("builds a vectorSearch terminal with limit and filter", () => {
    const q = api.docs
      .query()
      .vectorSearch("by_embedding", [1, 0, 0], { limit: 5, filter: { userId: "u1" } })
      .take(0 as any) /* terminal not needed; vectorSearch carries limit */;
    // vectorSearch is itself near-terminal; assert the json directly:
  });
```

(Adjust to assert `q.json.vectorSearch` equals `{index:"by_embedding", vector:[1,0,0], limit:5, filter:{userId:"u1"}}`. Drop the `.take(...)` if the builder returns a `TableQuery` — call a terminal or read `.json` via the appropriate path; mirror how the existing `.search` test reads `q.json`.)

- [ ] **Step 7: Run the ts-client tests + typecheck**

Run:
```bash
cd ts-client && bunx vitest run tests/schema.test.ts tests/query.test.ts && bunx tsc --noEmit
```
Expected: PASS, no type errors.

- [ ] **Step 8: Commit**

```bash
git add ts-client/src ts-client/tests
git commit -m "feat(ts-client): vectorSearch wire + t.vector/vectorIndex builders (#17)"
```

---

## Task 8: rust-client mirror (wire + builders + tests)

**Files:**
- Modify: `rust-client/src/schema.rs` (FieldType, IndexDef, VectorIndexSpec, builders)
- Modify: `rust-client/src/query.rs` (Query, TableQuery)
- Modify: `rust-client/src/wire.rs` (VectorSearchQuery)
- Modify: `rust-client/src/lib.rs` (re-exports)
- Test: inline `#[cfg(test)] mod tests` in each file

**Interfaces:**
- Consumes: the server wire contract (Tasks 2–6).
- Produces: `FieldType::Vector { dimensions: u32 }`; `VectorIndexSpec` + `IndexDef.vector`; `VectorSearchQuery` + `Query.vector_search`; `FieldType::vector(dim)`; `TableBuilder::vector_index(name, field, dim, filter_fields)`; `TableQuery::vector_search(index, vector, limit, filter)`. Re-exports on the crate root.

**Reference (mirror exactly):** `FieldType` variants + constructors (`schema.rs:6-51`), `IndexDef` + `search_index` (`schema.rs:53-67, 100-118`), `Query` (`query.rs:22-64`), `TableQuery.search` (`query.rs:133-142`), `SearchQuery` (`wire.rs:157-165`), `lib.rs` re-exports (`lib.rs:20-27`).

- [ ] **Step 1: `FieldType::Vector` + constructor (schema.rs)**

Add the variant (the field is named `dimensions` to match the server wire exactly):

```rust
    Record { value: Box<FieldType> },
    Vector { dimensions: u32 },
```

Add a constructor in `impl FieldType`:

```rust
    pub fn vector(dimensions: u32) -> Self {
        FieldType::Vector { dimensions }
    }
```

- [ ] **Step 2: `VectorIndexSpec` + `IndexDef.vector` (schema.rs)**

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorIndexSpec {
    pub dimensions: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub filter_fields: Vec<String>,
}
```

Add to `IndexDef` (after `search`):

```rust
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<VectorIndexSpec>,
```

Add `vector: None` to the existing `index`/`search_index` builder literals (they construct `IndexDef { name, fields, search }`).

- [ ] **Step 3: `TableBuilder::vector_index` (schema.rs)**

Mirror `search_index`:

```rust
    /// Declare a vector index over a Vector-typed `field`. The server stores a
    /// pgvector column ranked by cosine distance via the `vectorSearch` terminal.
    pub fn vector_index(
        mut self,
        name: &str,
        field: &str,
        dimensions: u32,
        filter_fields: &[&str],
    ) -> Self {
        self.indexes.push(IndexDef {
            name: name.into(),
            fields: vec![field.into()],
            search: false,
            vector: Some(VectorIndexSpec {
                dimensions,
                filter_fields: filter_fields.iter().map(|s| (*s).into()).collect(),
            }),
        });
        self
    }
```

- [ ] **Step 4: `VectorSearchQuery` (wire.rs) + `Query.vector_search` (query.rs)**

In `wire.rs`:

```rust
/// A vector-similarity terminal over a declared vector index. Mirrors
/// `server/src/query.rs::VectorSearchQuery` byte-for-byte (camelCase, deny_unknown_fields).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VectorSearchQuery {
    pub index: String,
    pub vector: Vec<f32>,
    pub limit: u32,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub filter: BTreeMap<String, serde_json::Value>,
}
```

In `query.rs`, add to `Query` (after `search`):

```rust
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector_search: Option<crate::wire::VectorSearchQuery>,
```

Add `use std::collections::BTreeMap;` to `wire.rs` if missing.

- [ ] **Step 5: `TableQuery::vector_search` (query.rs)**

Mirror `.search`:

```rust
    /// Vector-similarity `vectorSearch` over a declared vector index. The server
    /// ranks by cosine distance and applies `limit`; `filter` is an eq-map over the
    /// index's declared filterFields. Terminal.
    pub fn vector_search(
        mut self,
        index: &str,
        vector: Vec<f32>,
        limit: u32,
        filter: BTreeMap<String, serde_json::Value>,
    ) -> Self {
        self.q.vector_search = Some(crate::wire::VectorSearchQuery {
            index: index.into(),
            vector,
            limit,
            filter,
        });
        self
    }
```

- [ ] **Step 6: Re-exports (lib.rs)**

Add `VectorSearchQuery` to the `wire` re-export and `VectorIndexSpec` to the `schema` re-export (the `lib.rs:20-27` `pub use` lines).

- [ ] **Step 7: Tests (clone the four search tests)**

In `schema.rs` tests, add `vector_index_serializes_and_round_trips` (mirror `search_index_serializes_and_round_trips` at `schema.rs:288-325`): assert `vector_index("by_embedding","embedding",4,&["userId"])` emits `{name, fields:["embedding"], vector:{dimensions:4, filterFields:["userId"]}}` and round-trips; a btree `.index()` deserializes with `vector: None`.

In `query.rs` tests, add `vector_builder_serializes_terminal` (mirror `search_builder_serializes_terminal` at `query.rs:345-354`): assert `TableQuery::new("docs").vector_search("by_embedding", vec![1.0,0.0,0.0], 5, BTreeMap::new())` serializes with `vectorSearch` present and camelCase.

In `wire.rs` tests, add `vector_search_query_wire_shape` (mirror `search_query_wire_shape` at `wire.rs:407-421`): assert `VectorSearchQuery` serializes to `{index, vector, limit}` (camelCase) and round-trips.

- [ ] **Step 8: Run rust-client tests + clippy**

Run:
```bash
cd rust-client && cargo test && cargo clippy --all-targets -- -D warnings
```
Expected: PASS, clean.

- [ ] **Step 9: Commit**

```bash
git add rust-client/src
git commit -m "feat(rust-client): vectorSearch wire + vector/vector_index builders (#17)"
```

---

## Task 9: Docs — FEATURE_MATRIX flip + deploy note

**Files:**
- Modify: `FEATURE_MATRIX.md`
- Modify: `deploy/README.md`

- [ ] **Step 1: Flip FEATURE_MATRIX row #17**

In `FEATURE_MATRIX.md`, change row #17 from `❌` to `✅` and replace the implementation-sketch cell with a done-note mirroring the style of the other completed rows. Include: pgvector extension (per-db `CREATE EXTENSION IF NOT EXISTS vector` in `create_database` + `push_schema`), `Vector { dimensions }` field type, vector index on `IndexDef` (`vector: { dimensions, filterFields }`) compiling to a write-maintained `vector(N)` column + HNSW `vector_cosine_ops` index, `vectorSearch` terminal (cosine `<=>`, own `limit` ≤256, eq-`filter` over `filterFields`), reactive via the committer's table invalidation, client-supplied embeddings. Note two deliberate divergences from Convex: **reactive** (Convex is one-shot) and **client-supplied embeddings** (no server-side generation). State mirror status: server + ts-client + rust-client.

Also update the "Recommended order" / "Remaining gaps" section: remove #17 from remaining gaps; the new remaining list is `#18 (data-browser dashboard)`, `#20 (per-row auth rules)`.

- [ ] **Step 2: Note the image change in deploy/README.md**

Add a one-line note that the Postgres image is `pgvector/pgvector:pg17` (required for vector search) wherever the compose/stack is described. If `deploy/README.md` lists the image, update it; otherwise add a short "Postgres image" note.

- [ ] **Step 3: Commit**

```bash
git add FEATURE_MATRIX.md deploy/README.md
git commit -m "docs: vector search (#17) — FEATURE_MATRIX flip + deploy image note"
```

---

## Final verification (run before declaring done)

- [ ] `make checkall` from the repo root — must pass (fmt-check + clippy `-D warnings` + typecheck + full test suite).
- [ ] Confirm the kanban item moves to `done` only after this passes (it is `in_progress` now).
- [ ] Note in the commit/PR: dev + prod compose now use `pgvector/pgvector:pg17`; a prod redeploy is required for vector search to work live (the image change is inert until deployed — surface this to the user, do not deploy without confirmation).

## Self-review notes (plan author)

- **Spec coverage:** §2.1 Vector field type → Task 2; §2.2 vector index + column ownership → Tasks 3–4; §2.3 vectorSearch terminal → Task 6; §3.1 extension → Tasks 1 & 4; §3.2 DDL → Task 4; §3.3 write path → Task 5; §3.4 read path → Task 6; §3.5 reactivity → Task 6 Step 9; §4 deployment → Tasks 1 & 9; §5 errors → every task's rejection tests; §6 binding mechanism → settled on text-cast `::vector` (no new dep), proven by Task 1's guard test which exercises `::vector` casts; §7 client mirror → Tasks 7–8; §8 testing → each task; §9 parity → Task 9.
- **Type consistency:** `VectorIndexSpec` (camelCase `filterFields`) and `VectorSearchQuery` (`index`/`vector`/`limit`/`filter`) names match across server + both clients; `pg_vector_col` used in ddl/query/txn; `VECTOR_SEARCH_MAX_LIMIT = 256`; `FieldType::Vector { dimensions }` everywhere (named `dimensions`, not `dim`, for wire identity).
- **Open risk:** the `$n::vector` text-cast bind (Task 5) and the `(doc->>'field')::vector` backfill (Task 4) depend on pgvector accepting JSON-array text. Task 1's guard test asserts `'[1,0,0]'::vector` resolves; if the write-path serialization (`value.to_string()` → `[1.0,2.0,3.0]`) is rejected, switch the serialization to a manual `[a,b,c]` formatter (drop trailing `.0`) — one-line change, localized to `column_bind_for`.
