# Unique + Partial Index Constraints Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add declarative `UNIQUE` btree indexes (including partial unique indexes with a `WHERE` predicate) to par-rt-db, enforced atomically by Postgres and mirrored across all four clients.

**Architecture:** Two additive `skip_serializing_if`-omitted flags on `IndexDef` (`unique`, `where`) compile, inside the existing `push_schema` btree branch, to `CREATE [UNIQUE] INDEX … [WHERE <literal>]`. The `WHERE` is produced by a DDL-only literal-inlining sibling of `compile_filter` (Postgres forbids bind params in a partial-index predicate). Postgres enforces uniqueness inside `execute_txn`; a `unique_violation` (SQLSTATE 23505) is mapped to a new `CONFLICT` (409) wire code in the single blanket `From<sqlx::Error>` impl. A new error code + two index flags are mirrored byte-identically across ts/rust/python clients and their in-memory harnesses.

**Tech Stack:** Rust (axum/sqlx/Postgres 17), TypeScript (ts-client + dashboard), Rust (rust-client), Python (python-client). Spec: `docs/superpowers/specs/2026-08-01-unique-indexes-design.md`.

## Global Constraints

- **Wire casing is non-uniform and load-bearing** — match the four wire files byte-for-byte. New wire keys: `unique` (bool), `where` (a `FilterExpr`), error code `CONFLICT`. Omit `unique`/`where` from the wire when absent (`skip_serializing_if`) so existing schemas deserialize unchanged.
- **SQL construction:** double-quote every identifier; the partial-predicate values are typed+validated then literal-inlined (string values use SQL-standard `''` doubling). Physical names stay lowercased + 63-byte capped (existing `pg_col`/index-ident helpers) — caps unchanged.
- **Errors:** uniqueness violations surface as the typed `CONFLICT` envelope, never a stringified sqlx body; log via `tracing`. No `unwrap()`/`expect()` outside `#[cfg(test)]`. Zero clippy warnings under `-D warnings`.
- **Single-writer invariant untouched:** enforcement is Postgres-side inside the existing `execute_txn`; no new writer, no new committer arm. (`push_schema` already runs outside the committer; unchanged.)
- **Gate:** `make checkall` (fmt-check + clippy `-D warnings` + typecheck + tests) across all five packages must pass. Server tests need `make dev-db-up` (real Postgres on `127.0.0.1:55434`). First-time: `make ts-client-install`, `make dashboard-install`, `make python-client-install`; and `make ts-client-build` so the dashboard typecheck resolves `@par-rt-db/client`.
- **Clients mirror the core:** every server wire change lands in ts/rust/python in the same task or its dedicated mirror task.

---

## File Map

**Server (source of truth):**
- `server/src/error.rs` — `Conflict` code + status + `conflict()` helper + 23505 mapping in `From<sqlx::Error>`.
- `server/src/schema.rs` — `IndexDef.unique` + `IndexDef.r#where` + `validate()` rules.
- `server/src/query.rs` — `render_literal(EqBind)` + `compile_filter_literal(FilterExpr, &TableDef)`.
- `server/src/ddl.rs` — btree branch emits `CREATE [UNIQUE] INDEX … [WHERE …]`; dup pre-check; +2 `detect_destructive_changes` arms.
- Tests: `server/tests/schema_validators_test.rs`, `server/tests/schema_evolution_test.rs`, `server/tests/txn_test.rs`, plus a `query.rs` unit test for the literal compiler.

**ts-client:** `ts-client/src/protocol.ts` (index wire type + `FilterExpr` already here), `ts-client/src/errors.ts` (`RtDbErrorCode` + `CODES`), `ts-client/src/schema.ts` (index builder), `ts-client/src/in_memory.ts` (enforcement).

**rust-client:** `rust-client/src/wire.rs` (`ErrorCode::Conflict`; `IndexDef` is in `schema.rs`), `rust-client/src/schema.rs` (`IndexDef` + `TableBuilder`), `rust-client/src/in_memory.rs` (enforcement).

**python-client:** `python-client/src/par_rt_db/errors.py` (`ErrorCode.CONFLICT` + status map), `python-client/src/par_rt_db/schema.py` (`IndexDef` + serializer + `TableBuilder.index`), `python-client/src/par_rt_db/in_memory.py` (enforcement).

**Docs:** `FEATURE_MATRIX.md` (new row), `CLAUDE.md` (note) — final task.

---

### Task 1: Server — `CONFLICT` error code + SQLSTATE 23505 mapping

**Files:**
- Modify: `server/src/error.rs:7-16` (enum), `:85-96` (`status()`), `:99-106` (`From<sqlx::Error>`), and the helper block `:46-83`.
- Test: `server/src/error.rs` `#[cfg(test)] mod tests`.

**Interfaces:**
- Produces: `ErrorCode::Conflict` (wire `"CONFLICT"`), `RtDbError::conflict(msg)`, and a `From<sqlx::Error>` that maps SQLSTATE `23505` → `Conflict`. Later tasks rely on `RtDbError::conflict(..)` and on the wire string `"CONFLICT"`.

- [ ] **Step 1: Add the failing tests**

Append to `error.rs` tests:

```rust
#[test]
fn conflict_error_maps_to_http_409() {
    assert_eq!(RtDbError::conflict("dup").status(), StatusCode::CONFLICT);
}

#[test]
fn conflict_error_serializes_to_wire_envelope() {
    let err = RtDbError::conflict("unique index 'i_t_by_email' violated");
    assert_eq!(
        serde_json::to_value(&err).unwrap(),
        json!({"code": "CONFLICT", "message": "unique index 'i_t_by_email' violated"})
    );
}
```

Also extend `status_maps_each_code_to_expected_http_status` with:

```rust
        assert_eq!(
            RtDbError::conflict("x").status(),
            StatusCode::CONFLICT
        );
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd server && cargo test --lib error::tests`
Expected: FAIL — `conflict` not found / `Conflict` variant unknown.

- [ ] **Step 3: Implement**

In the enum add `Conflict,` after `RateLimited,` (or anywhere — order is not load-bearing):

```rust
pub enum ErrorCode {
    Unauthorized,
    Forbidden,
    NotFound,
    SchemaViolation,
    PreconditionFailed,
    BadRequest,
    Internal,
    RateLimited,
    Conflict,
}
```

Add the status arm (note: `PreconditionFailed` already maps to `CONFLICT` (409); `Conflict` sharing 409 is intentional — clients branch on the wire `code`, not the HTTP status):

```rust
            ErrorCode::Conflict => StatusCode::CONFLICT,
```

Add the helper near the other constructors:

```rust
    /// A uniqueness / conflict violation (HTTP 409). Used for a Postgres
    /// `unique_violation` (SQLSTATE 23505) on a `UNIQUE` index, both at
    /// `CREATE UNIQUE INDEX` time and on a colliding write inside `execute_txn`.
    pub fn conflict(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::Conflict, msg)
    }
```

Enhance `From<sqlx::Error>` so a unique violation is surfaced as `Conflict` (with the constraint name when Postgres provides it) instead of the generic 500, and is **not** logged at ERROR (it is an expected client error, like a failed precondition):

```rust
impl From<sqlx::Error> for RtDbError {
    fn from(err: sqlx::Error) -> Self {
        if let Some(db) = err.as_database_error() {
            if db.code().as_deref() == Some("23505") {
                let constraint = db.constraint().map(|c| format!(" '{c}'")).unwrap_or_default();
                return Self::conflict(format!("unique constraint{constraint} violated"));
            }
        }
        tracing::error!(error = %err, "sqlx error");
        // Never leak Postgres error text (relation/column/constraint names);
        // the full error is already logged above.
        Self::internal("internal error")
    }
}
```

> Note on `db.code()` / `db.constraint()`: these are methods on sqlx's `DatabaseError` trait (`code() -> Option<Cow<str>>`, `constraint() -> Option<&str>`). If the bound sqlx version's trait spells them differently, adjust to the trait's actual names — the intent is: SQLSTATE == `23505` ⇒ `Conflict`, and surface `constraint()` when present. (This mapping's end-to-end proof is Task 5's integration test; the unit tests here cover `status()` + wire shape.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd server && cargo test --lib error::tests`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add server/src/error.rs
git commit -m "feat(server): add CONFLICT error code + unique_violation (23505) mapping"
```

---

### Task 2: Server schema — `IndexDef.unique` + `IndexDef.r#where` + validation

**Files:**
- Modify: `server/src/schema.rs:28-45` (`IndexDef`), the `validate()` path (locate via `schema.validate`), and add `use crate::query::FilterExpr;`.
- Test: `server/tests/schema_validators_test.rs`.

**Interfaces:**
- Consumes: `crate::query::FilterExpr` (defined in `server/src/query.rs`, internally tagged by `op`).
- Produces: `IndexDef { unique: bool, r#where: Option<FilterExpr> }` — read by Task 4 (DDL). Wire keys `unique` / `where`.

- [ ] **Step 1: Add the failing tests**

In `schema_validators_test.rs`, add (mirroring the existing search/vector round-trip + additive-omission tests):

```rust
#[tokio::test]
async fn unique_index_round_trips_and_omits_when_absent() {
    // A plain unique index carries `unique` but not `where`; absent `where` is
    // omitted so a non-unique, non-partial index still round-trips as {"name","fields"} only.
    let json = serde_json::json!({
        "tables": {
            "t": {
                "fields": { "email": "string" },
                "indexes": [{ "name": "by_email", "fields": ["email"], "unique": true }]
            }
        }
    });
    let schema: SchemaDef = serde_json::from_value(json.clone()).unwrap();
    let idx = schema.tables["t"].indexes.as_ref().unwrap()[0].clone();
    assert!(idx.unique);
    assert!(idx.r#where.is_none());
    // Round-trip keeps `unique` and omits the absent `where`.
    let re = serde_json::to_value(&schema).unwrap();
    assert_eq!(re["tables"]["t"]["indexes"][0]["unique"], true);
    assert!(re["tables"]["t"]["indexes"][0].get("where").is_none());
}

#[tokio::test]
async fn partial_unique_index_round_trips() {
    let json = serde_json::json!({
        "tables": {
            "t": {
                "fields": { "slug": "string", "deleted": "boolean" },
                "indexes": [{
                    "name": "by_slug", "fields": ["slug"], "unique": true,
                    "where": { "op": "eq", "field": "deleted", "value": false }
                }]
            }
        }
    });
    let schema: SchemaDef = serde_json::from_value(json).unwrap();
    let idx = &schema.tables["t"].indexes.as_ref().unwrap()[0];
    assert!(idx.unique);
    assert!(idx.r#where.is_some());
}
```

And a validation-rejection test (add once `validate()` enforces it in Step 3 — write it now so Step 2 fails for the right reason after the struct compiles):

```rust
#[tokio::test]
async fn unique_where_rejected_on_search_index() {
    let json = serde_json::json!({
        "tables": {
            "t": {
                "fields": { "body": "string" },
                "indexes": [{ "name": "body", "fields": ["body"], "search": true, "unique": true }]
            }
        }
    });
    let schema: SchemaDef = serde_json::from_value(json).unwrap();
    let err = schema.validate().unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd server && cargo test --test schema_validators_test unique_ partial_`
Expected: FAIL — `unique` / `r#where` fields unknown (serde error or compile error).

- [ ] **Step 3: Implement**

At the top of `schema.rs`, add:

```rust
use crate::query::FilterExpr;
```

Extend `IndexDef`:

```rust
pub struct IndexDef {
    pub name: String,
    pub fields: Vec<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub search: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<VectorIndexSpec>,
    /// `true` compiles to `CREATE UNIQUE INDEX`. Legal only on a plain btree
    /// index (rejected alongside `search`/`vector`). Omitted on the wire when
    /// false, so existing schemas deserialize unchanged.
    #[serde(default, skip_serializing_if = "is_false")]
    pub unique: bool,
    /// Optional partial-index predicate baked into `CREATE INDEX … WHERE`. Same
    /// `FilterExpr` type as the query-time `filter()` terminal, but compiled to
    /// literal SQL at DDL time (Postgres forbids bind params here). Omitted on
    /// the wire when `None`. Wire key is `where` (Rust keyword ⇒ raw identifier).
    #[serde(default, rename = "where", skip_serializing_if = "Option::is_none")]
    pub r#where: Option<FilterExpr>,
}
```

In every existing `IndexDef { .. }` literal in `server/src/` (schema tests, `ddl.rs` if any are constructed, etc.), add `unique: false, r#where: None,`. (`rg -n 'search: false' server/src` finds them.)

In `validate()` (or the index-validation helper it calls), after the existing `search`/`vector` exclusivity check, add:

```rust
if index.unique || index.r#where.is_some() {
    if index.search {
        return Err(RtDbError::schema(format!(
            "index '{}' cannot combine unique/where with search",
            index.name
        )));
    }
    if index.vector.is_some() {
        return Err(RtDbError::schema(format!(
            "index '{}' cannot combine unique/where with a vector index",
            index.name
        )));
    }
}
```

Match the existing rejection's exact `RtDbError` constructor + message style (look at how `search`+`vector` conflicts are rejected in the same function and mirror it).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd server && cargo test --test schema_validators_test` && `cd server && cargo test --lib schema`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add server/src/schema.rs server/tests/schema_validators_test.rs
git commit -m "feat(schema): add unique + where (partial) index flags"
```

---

### Task 3: Server — `compile_filter_literal` (partial-index WHERE as literal SQL)

**Files:**
- Modify: `server/src/query.rs` (add near `compile_filter`, ~line 1452). `EqBind` lives in `server/src/txn.rs` — it is `pub(crate)`, so `query.rs` already sees it (confirm `use crate::txn::EqBind;` exists; add if not).
- Test: `server/src/query.rs` `#[cfg(test)] mod tests` (add one), or the existing query unit-test module.

**Interfaces:**
- Consumes: `field_lhs_and_bind(field, value, table) -> (String, EqBind)` and the `FilterExpr` `and`/`or` shape from `compile_filter_node`.
- Produces: `pub(crate) fn compile_filter_literal(filter: &FilterExpr, table: &TableDef) -> Result<String, RtDbError>` — used by Task 4. Returns a fully-parenthesized, self-contained SQL fragment with literals inlined (no `$n`).

- [ ] **Step 1: Add the failing test**

```rust
#[test]
fn compile_filter_literal_emits_typed_literals_not_binds() {
    // table with an indexed boolean `deleted` and a string `slug`.
    let mut table = TableDef { fields: BTreeMap::new(), indexes: None,
        owner_field: None, collaborators_field: None };
    table.fields.insert("deleted".into(), FieldType::Boolean);
    // eq on a boolean column -> literal `false`, not `$1`.
    let pred = FilterExpr::Eq { field: "deleted".into(), value: serde_json::json!(false) };
    let sql = compile_filter_literal(&pred, &table).unwrap();
    assert_eq!(sql, "\"f_deleted\" = false");
}

#[test]
fn compile_filter_literal_escapes_string_literals() {
    let mut table = TableDef { fields: BTreeMap::new(), indexes: None,
        owner_field: None, collaborators_field: None };
    table.fields.insert("name".into(), FieldType::String);
    let pred = FilterExpr::Eq { field: "name".into(), value: serde_json::json!("O'Brien") };
    let sql = compile_filter_literal(&pred, &table).unwrap();
    assert_eq!(sql, "\"f_name\" = 'O''Brien'");
}
```

> Match the exact `TableDef` literal + `FieldType` import style already used in `query.rs`'s test module. The LHS column name (`f_<field>`) comes from `field_lhs_and_bind` — verify the exact rendered column name by reading that function and assert the real value it produces (if it differs from `f_deleted`, fix the assertion to match reality; do not change `field_lhs_and_bind`).

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd server && cargo test --lib compile_filter_literal`
Expected: FAIL — function not defined.

- [ ] **Step 3: Implement**

Add next to `compile_filter`:

```rust
/// Inlines a typed `EqBind` as a SQL literal (DDL-only — partial-index
/// predicates cannot use `$n` binds). Strings use SQL-standard `''` doubling.
fn render_literal(bind: &EqBind) -> String {
    match bind {
        EqBind::Text(s) => format!("'{}'", s.replace('\'', "''")),
        EqBind::Bool(b) => if *b { "true".into() } else { "false".into() },
        EqBind::Num(n) => n.to_string(),
        EqBind::I64(n) => n.to_string(),
    }
}

/// Like `compile_filter`, but emits **literal** values instead of `$n` binds.
/// Used only at DDL time to bake a partial-index predicate into
/// `CREATE INDEX … WHERE <sql>`. Reuses `field_lhs_and_bind` for identifier
/// validation/double-quoting and value typing, so the predicate is as tightly
/// validated as a query-time `filter()`.
pub(crate) fn compile_filter_literal(
    filter: &FilterExpr,
    table: &TableDef,
) -> Result<String, RtDbError> {
    Ok(render_filter_literal_node(filter, table)?)
}

fn render_filter_literal_node(
    node: &FilterExpr,
    table: &TableDef,
) -> Result<String, RtDbError> {
    match node {
        FilterExpr::And { exprs } | FilterExpr::Or { exprs } => {
            if exprs.is_empty() {
                return Err(RtDbError::bad_request(format!(
                    "{} filter requires at least one expr",
                    if matches!(node, FilterExpr::And { .. }) { "and" } else { "or" }
                )));
            }
            let joiner = if matches!(node, FilterExpr::And { .. }) { " AND " } else { " OR " };
            let parts: Vec<String> = exprs
                .iter()
                .map(|e| render_filter_literal_node(e, table))
                .collect::<Result<_, _>>()?;
            Ok(format!("({})", parts.join(joiner)))
        }
        FilterExpr::Eq { field, value }
        | FilterExpr::Neq { field, value }
        | FilterExpr::Gt { field, value }
        | FilterExpr::Gte { field, value }
        | FilterExpr::Lt { field, value }
        | FilterExpr::Lte { field, value } => {
            let op = match node {
                FilterExpr::Eq { .. } => "=",
                FilterExpr::Neq { .. } => "<>",
                FilterExpr::Gt { .. } => ">",
                FilterExpr::Gte { .. } => ">=",
                FilterExpr::Lt { .. } => "<",
                FilterExpr::Lte { .. } => "<=",
                _ => unreachable!(),
            };
            let (lhs, bind) = field_lhs_and_bind(field, value, table)?;
            Ok(format!("{lhs} {op} {}", render_literal(&bind)))
        }
        FilterExpr::In { field, values } => {
            if values.is_empty() {
                return Err(RtDbError::bad_request("in filter requires at least one value"));
            }
            let (lhs, first) = field_lhs_and_bind(field, &values[0], table)?;
            let mut lits = vec![render_literal(&first)];
            for value in &values[1..] {
                let (this_lhs, bind) = field_lhs_and_bind(field, value, table)?;
                if this_lhs != lhs {
                    return Err(RtDbError::bad_request(
                        "in filter values must all be the same type",
                    ));
                }
                lits.push(render_literal(&bind));
            }
            Ok(format!("{lhs} IN ({})", lits.join(", ")))
        }
    }
}
```

> `field_lhs_and_bind` returns `(String, EqBind)` and already double-quotes identifiers + types the value. Verify its exact signature/return in `query.rs` and adjust the destructuring if it returns a tuple variant or different bind type. If `EqBind` is not in scope, add `use crate::txn::EqBind;`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd server && cargo test --lib compile_filter_literal`
Expected: PASS. (If the asserted LHS column name differs, fix the assertion to the real `field_lhs_and_bind` output — the point of the test is literal-vs-bind + escaping, which is independent of the column name.)

- [ ] **Step 5: Commit**

```bash
git add server/src/query.rs
git commit -m "feat(query): compile_filter_literal for partial-index WHERE predicates"
```

---

### Task 4: Server DDL — `CREATE [UNIQUE] INDEX … [WHERE]` + dup pre-check + destructive arms

**Files:**
- Modify: `server/src/ddl.rs` — the btree `else` branch (~line 296), add the pre-check just before it, and `detect_destructive_changes` (~line 78).
- Test: `server/tests/schema_evolution_test.rs` (destructive arms), `server/tests/schema_validators_test.rs` or a new `server/tests/unique_index_test.rs` (introspection via `pg_indexes`).

**Interfaces:**
- Consumes: `IndexDef.unique` / `IndexDef.r#where` (Task 2), `compile_filter_literal` (Task 3).
- Produces: durable `UNIQUE` / partial indexes in Postgres, enforced from this point on.

- [ ] **Step 1: Add the failing tests**

In `schema_evolution_test.rs` (mirror the existing search⇄btree flip test):

```rust
#[tokio::test]
async fn flipping_unique_on_existing_index_is_destructive() {
    let db = unique_db("flip_unique").await;
    let s1 = schema!(t: { email: string, indexes: [{ name: "by_email", fields: ["email"] }] });
    push(&db, s1).await.unwrap();
    let s2 = schema!(t: { email: string, indexes: [{ name: "by_email", fields: ["email"], unique: true }] });
    let err = push(&db, s2).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("uniqueness"), "got: {}", err.message);
}
```

(Use the test helpers/macros — `unique_db`, `push`, `schema!` — already defined in that file; match their exact spellings.)

And a new introspection test proving the UNIQUE index is actually created (create a small `server/tests/unique_index_test.rs` if a dedicated file is cleaner, else add to `schema_validators_test.rs`):

```rust
#[tokio::test]
async fn unique_index_is_created_as_unique_on_postgres() {
    let db = unique_db("unique_created").await;
    let s = schema!(t: { email: string, indexes: [{ name: "by_email", fields: ["email"], unique: true }] });
    push(&db, s).await.unwrap();
    // Introspect: the index must exist with `unique = true`.
    let row: (bool,) = sqlx::query_as(
        "SELECT indisunique FROM pg_indexes i, pg_index pi
         WHERE i.schemaname = $1 AND i.indexname = $2 AND pi.indexrelid = regexp_replace(i.indexdef, '.*USING btree \\((.*)\\)', '\\1')::regclass"
    ).bind(&format!("rtdb_{}", db)).bind("i_t_by_email").fetch_one(&pool).await.unwrap();
    // Simpler + robust: query pg_indexes for the indexdef containing 'UNIQUE'.
    let def: (String,) = sqlx::query_as(
        "SELECT indexdef FROM pg_indexes WHERE schemaname = $1 AND indexname = $2"
    ).bind(&format!("rtdb_{}", db)).bind("i_t_by_email").fetch_one(&pool).await.unwrap();
    assert!(def.0.to_uppercase().contains("CREATE UNIQUE INDEX"), "got: {}", def.0);
}
```

> Prefer the second (indexdef contains `CREATE UNIQUE INDEX`) assertion — it is robust to identifier quoting. Use the test harness's pooled `pool`/`unique_db` helpers exactly as neighboring tests do. Delete the unused first query.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd server && cargo test --test schema_evolution_test flipping_unique` and `cargo test --test schema_validators_test unique_index_is_created`
Expected: FAIL — flip is accepted (no arm yet) / index is created non-unique.

- [ ] **Step 3: Implement — DDL btree branch**

In the btree `else` branch of `push_schema`, generalize the statement and add the dup pre-check. Replace the existing `CREATE INDEX` block:

```rust
            } else {
                let cols: Vec<String> = index
                    .fields
                    .iter()
                    .map(|field_name| format!("\"{}\"", pg_col(field_name)))
                    .collect();

                // Partial-index predicate (literal SQL — see compile_filter_literal).
                let where_sql = match &index.r#where {
                    Some(pred) => {
                        let frag = compile_filter_literal(pred, new_table)?;
                        Some(format!(" WHERE {frag}"))
                    }
                    None => None,
                };

                // Pre-check for a clear CONFLICT before CREATE UNIQUE INDEX (the
                // CREATE itself remains the authoritative, race-free guarantee).
                if index.unique {
                    let grouped = cols.join(", ");
                    let sql = format!(
                        "SELECT {grouped} FROM \"{pg_schema_name}\".\"{table_ident}\"{where_sql} \
                         GROUP BY {grouped} HAVING count(*) > 1 LIMIT 5"
                    );
                    let dupes: Result<Vec<sqlx::postgres::PgRow>, _> =
                        sqlx::query(&sql).fetch_all(&mut *tx).await;
                    if let Ok(rows) = dupes {
                        if !rows.is_empty() {
                            return Err(RtDbError::conflict(format!(
                                "unique index '{}' cannot be created: {} existing row(s) duplicate its key",
                                index.name, rows.len()
                            )));
                        }
                    }
                    // A fetch error here (e.g. brand-new table has no columns to
                    // group is impossible — cols is non-empty for an index) is
                    // unexpected; fall through and let CREATE UNIQUE INDEX decide.
                }

                let unique_kw = if index.unique { "UNIQUE " } else { "" };
                sqlx::query(&format!(
                    "CREATE {unique_kw}INDEX \"{index_ident}\" ON \"{pg_schema_name}\".\"{table_ident}\" ({}, \"created_at\"){where_sql}",
                    cols.join(", ")
                ))
                .execute(&mut *tx)
                .await?;
            }
```

> `where_sql` is an `Option<String>` that already includes the leading ` WHERE ` when present; it is interpolated into both the pre-check SELECT and the CREATE. The `cols`/`grouped` values are all `pg_col`-produced double-quoted identifiers — safe to interpolate. Literal predicate values are validated+escaped by `compile_filter_literal`.

- [ ] **Step 4: Implement — destructive arms**

In `detect_destructive_changes`, after the existing `search`/`vector` checks inside the index loop, add:

```rust
                Some(new_index) if new_index.unique != old_index.unique => {
                    return Err(RtDbError::bad_request(format!(
                        "changed uniqueness of index '{}'",
                        old_index.name
                    )));
                }
                Some(new_index) if new_index.r#where != old_index.r#where => {
                    return Err(RtDbError::bad_request(format!(
                        "changed partial predicate of index '{}'",
                        old_index.name
                    )));
                }
```

`FilterExpr` derives `PartialEq` (verify it does — if not, compare via `serde_json::to_value(a) == serde_json::to_value(b)` instead and note why).

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd server && cargo test --test schema_evolution_test` && `cargo test --test schema_validators_test`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add server/src/ddl.rs server/tests/schema_evolution_test.rs server/tests/schema_validators_test.rs
git commit -m "feat(ddl): CREATE [UNIQUE] INDEX with WHERE + dup pre-check + destructive arms"
```

---

### Task 5: Server — end-to-end enforcement tests (23505 → CONFLICT, partial semantics)

**Files:**
- Test: `server/tests/txn_test.rs` (no production change — proves Tasks 1+4 together).

**Interfaces:** Consumes the full server stack. This is the correctness proof for the `From<sqlx::Error>` 23505 mapping and the partial-unique semantics.

- [ ] **Step 1: Add the failing tests**

```rust
#[tokio::test]
async fn duplicate_insert_on_unique_index_is_conflict_and_rolls_back() {
    let db = unique_db("dup_insert").await;
    push(&db, schema!(t: { email: string, n: number, indexes: [{ name: "by_email", fields: ["email"], unique: true }] })).await.unwrap();
    mutate(&db, txn![insert("t", { "email": "a@x", "n": 1 })]).await.unwrap();
    // Second insert with the same email -> CONFLICT, and the whole txn aborts.
    let err = mutate(&db, txn![insert("t", { "email": "a@x", "n": 2 })]).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::Conflict);
    // Rollback proof: a subsequent count of t is still 1.
    let n = collect_count(&db, "t").await;
    assert_eq!(n, 1);
}

#[tokio::test]
async fn partial_unique_allows_excluded_duplicate() {
    let db = unique_db("partial_unique").await;
    push(&db, schema!(t: { slug: string, deleted: boolean, indexes: [{ name: "by_slug", fields: ["slug"], unique: true, where: { op: "eq", field: "deleted", value: false } }] })).await.unwrap();
    // Two rows, same slug, BOTH deleted (excluded by the predicate) -> allowed.
    mutate(&db, txn![insert("t", { "slug": "x", "deleted": true })]).await.unwrap();
    mutate(&db, txn![insert("t", { "slug": "x", "deleted": true })]).await.unwrap();
    // A non-deleted row colliding with another non-deleted row -> CONFLICT.
    mutate(&db, txn![insert("t", { "slug": "x", "deleted": false })]).await.unwrap();
    let err = mutate(&db, txn![insert("t", { "slug": "x", "deleted": false })]).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::Conflict);
}

#[tokio::test]
async fn patch_creating_collision_is_conflict() {
    let db = unique_db("patch_collision").await;
    push(&db, schema!(t: { email: string, indexes: [{ name: "by_email", fields: ["email"], unique: true }] })).await.unwrap();
    mutate(&db, txn![insert("t", { "email": "a@x" })]).await.unwrap();
    let id_b = mutate(&db, txn![insert("t", { "email": "b@x" })]).await.unwrap()[0]["id"].clone();
    let err = mutate(&db, txn![patch("t", id_b, { "email": "a@x" })]).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::Conflict);
}
```

> Use the exact test helpers (`unique_db`, `push`, `mutate`, `txn!`, `insert`, `patch`, `collect_count`) already defined in `txn_test.rs` — match their real signatures (read the file's existing tests). `collect_count` may not exist; if so, issue a `query` with `count: true` against table `t` via the existing query-test helper, or count via a direct SQL `SELECT count(*)`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd server && cargo test --test txn_test duplicate_insert partial_unique patch_creating`
Expected: If Tasks 1–4 are correct, these PASS already (this task is the verification gate). If any FAIL, the defect is in Tasks 1–4 — diagnose with the failing output, do not weaken the test.

- [ ] **Step 3: Run the full server gate**

Run: `cd server && make dev-db-up && cargo test`
Expected: PASS (all server tests green).

- [ ] **Step 4: Commit**

```bash
git add server/tests/txn_test.rs
git commit -m "test(server): unique + partial-unique index enforcement → CONFLICT"
```

---

### Task 6: ts-client mirror — wire, builder, in-memory enforcement

**Files:**
- Modify: `ts-client/src/protocol.ts` (index wire type — wherever `search`/`vector` are represented on the index), `ts-client/src/errors.ts` (`RtDbErrorCode` + `CODES`), `ts-client/src/schema.ts` (index builder), `ts-client/src/in_memory.ts` (enforcement).
- Test: `ts-client/tests/schema.test.ts` (wire/builder), `ts-client/tests/in_memory.test.ts` (dupe rejection).

**Interfaces:** Mirror server wire keys `unique` / `where` and code `"CONFLICT"` exactly.

- [ ] **Step 1: Write failing tests**

In `schema.test.ts` — assert a `.unique()` / `.where(pred)` builder produces `{ name, fields, unique: true, where: {...} }` on the wire and that `unique`/`where` are omitted when unset. In `in_memory.test.ts` — push a unique-indexed schema, insert a doc, insert a colliding doc → expect an `RtDbError` with `code === "CONFLICT"`; and a partial-unique excluded dupe is allowed. (Mirror the existing `PRECONDITION_FAILED` upsert-collision test shape in `in_memory.ts`.)

- [ ] **Step 2: Run — verify fail**

Run: `cd ts-client && bunx vitest run tests/schema.test.ts tests/in_memory.test.ts`
Expected: FAIL.

- [ ] **Step 3: Implement**

- `errors.ts`: add `| "CONFLICT"` to `RtDbErrorCode`; add `"CONFLICT"` to the `CODES` set.
- `protocol.ts`: on the index wire type, add `unique?: boolean` and `where?: FilterExpr` (mirror how `search`/`vector` are declared there).
- `schema.ts`: in the index builder, add `.unique()` and `.where(predicate: FilterExpr)` setters (the builder returns `this` / a new object — match the existing `searchIndex`/`vectorIndex` pattern); they set the two new fields.
- `in_memory.ts`: in the insert/patch/replace/upsert path, after computing the would-be stored row, for each `unique` index on the table, check whether an existing row (that satisfies the index's `where` predicate, evaluated via the existing `evalFilterExpr`) already has the same key values; if so, throw `new RtDbError("CONFLICT", "unique index '<name>' violated")` and roll back the txn (same rollback path as the existing `PRECONDITION_FAILED` checks).

- [ ] **Step 4: Run — verify pass**

Run: `cd ts-client && bunx vitest run tests/schema.test.ts tests/in_memory.test.ts`
Expected: PASS.

- [ ] **Step 5: Build + commit**

```bash
cd ts-client && bun run build   # regenerates dist/ the dashboard typecheck needs
git add ts-client/src/errors.ts ts-client/src/protocol.ts ts-client/src/schema.ts ts-client/src/in_memory.ts ts-client/tests/schema.test.ts ts-client/tests/in_memory.test.ts
git commit -m "feat(ts-client): mirror unique/where indexes + CONFLICT + in-memory enforcement"
```

---

### Task 7: rust-client mirror — wire, builder, in-memory enforcement

**Files:**
- Modify: `rust-client/src/wire.rs` (`ErrorCode::Conflict`), `rust-client/src/schema.rs` (`IndexDef` + `TableBuilder`), `rust-client/src/in_memory.rs` (enforcement). (`FilterExpr` is already in `wire.rs`.)
- Test: `rust-client/tests/` (builder/wire) and the `in_memory.rs` `#[cfg(test)]` module.

**Interfaces:** Mirror server wire keys `unique` / `where` and code `Conflict` → `"CONFLICT"`.

- [ ] **Step 1: Write failing tests**

Builder test: `.index("by_email", &["email"]).unique()` produces an `IndexDef { unique: true, .. }` and serializes to JSON with `"unique": true` and no `where`. In-memory test (in `in_memory.rs::tests`): push a unique-indexed schema, insert a colliding doc → `Err` with `code == Conflict`; partial-unique excluded dupe allowed.

- [ ] **Step 2: Run — verify fail**

Run: `cd rust-client && cargo test`
Expected: FAIL.

- [ ] **Step 3: Implement**

- `wire.rs`: add `Conflict,` to `ErrorCode` and its serde mapping (mirror the existing variants; wire string `"CONFLICT"`).
- `schema.rs` `IndexDef`: add
  ```rust
  #[serde(default, skip_serializing_if = "is_false")]
  pub unique: bool,
  #[serde(default, rename = "where", skip_serializing_if = "Option::is_none")]
  pub r#where: Option<FilterExpr>,
  ```
  Update every existing `IndexDef { .. }` literal (`search_index`, `vector_index`, `index`) to set `unique: false, r#where: None`.
- `schema.rs` `TableBuilder`: add a transient `last_index: Option<usize>` field (the index of the most recently pushed `IndexDef`), set it at the end of `index`/`search_index`/`vector_index`, and add chainable setters:
  ```rust
  pub fn unique(mut self) -> Self {
      if let Some(i) = self.last_index { self.indexes[i].unique = true; }
      self
  }
  pub fn r#where(mut self, predicate: FilterExpr) -> Self {
      if let Some(i) = self.last_index { self.indexes[i].r#where = Some(predicate); }
      self
  }
  ```
  (Name the `where` setter to avoid the keyword at the call site per Rust rules — expose it as `.r#where(pred)` or, if the version dislikes raw identifiers in method chains, as `.where_clause(pred)` with a doc note; pick whichever compiles and is clearest, and use it consistently in the test.)
- `in_memory.rs`: mirror the ts-client enforcement — on insert/patch/replace/upsert, for each `unique` index, reject a colliding row (evaluating the `where` predicate via the existing `eval_filter_expr`) with `RtDbError` code `Conflict`, rolling back the txn.

- [ ] **Step 4: Run — verify pass**

Run: `cd rust-client && cargo test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add rust-client/src/wire.rs rust-client/src/schema.rs rust-client/src/in_memory.rs rust-client/tests/
git commit -m "feat(rust-client): mirror unique/where indexes + Conflict + in-memory enforcement"
```

---

### Task 8: python-client mirror — wire, builder, in-memory enforcement

**Files:**
- Modify: `python-client/src/par_rt_db/errors.py` (`ErrorCode.CONFLICT` + status map), `python-client/src/par_rt_db/schema.py` (`IndexDef` + `_drop_absent_flags` serializer + `TableBuilder.index`), `python-client/src/par_rt_db/in_memory.py` (enforcement).
- Test: `python-client/tests/` (schema builder/serialization, in-memory dupe).

**Interfaces:** Mirror server wire keys `unique` / `where` and code `"CONFLICT"`.

- [ ] **Step 1: Write failing tests**

`tests/test_schema.py`: `TableBuilder().field(...).index("by_email", ["email"]).unique()` serializes with `"unique": true` and no `where`; a plain index omits both. `tests/test_in_memory.py` (or the existing harness test): a colliding insert on a unique index raises `RtDbError` with `code == ErrorCode.CONFLICT`; a partial-unique excluded dupe is allowed.

- [ ] **Step 2: Run — verify fail**

Run: `cd python-client && uv run pytest -q tests/test_schema.py tests/test_in_memory.py`
Expected: FAIL.

- [ ] **Step 3: Implement**

- `errors.py`: add `CONFLICT = "CONFLICT"` to `ErrorCode`; add `ErrorCode.CONFLICT: 409` to the status map.
- `schema.py` `IndexDef`: add fields `unique: bool | None = None` and `where: FilterExpr | None = None` (matching the `search`/`vector` typing style). In `_drop_absent_flags`, after the existing drops, add:
  ```python
  if not out.get("unique"):
      out.pop("unique", None)
  if out.get("where") is None:
      out.pop("where", None)
  ```
- `schema.py` `TableBuilder.index`: make `index` return `self` (it already does) and add chainable setters `.unique(self) -> Self` and `.where(self, predicate) -> Self` that record the intent on the most recently added index (store a `self._last_index` pointer, mirroring the rust approach, or mutate `self.indexes[-1]`).
- `in_memory.py`: enforce unique/partial-unique on insert/patch/replace/upsert using the existing `_eval_filter_expr` for the predicate; raise `RtDbError(ErrorCode.CONFLICT, ...)` on collision and roll back.

- [ ] **Step 4: Run — verify pass**

Run: `cd python-client && uv run pytest -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python-client/src/par_rt_db/errors.py python-client/src/par_rt_db/schema.py python-client/src/par_rt_db/in_memory.py python-client/tests/
git commit -m "feat(python-client): mirror unique/where indexes + CONFLICT + in-memory enforcement"
```

---

### Task 9: Docs + full gate + board close-out

**Files:**
- Modify: `FEATURE_MATRIX.md` (new row), `CLAUDE.md` (one-line note in the schema/migrate area).
- Run: `make checkall`.

- [ ] **Step 1: Update `FEATURE_MATRIX.md`**

Add a §1/§2 row (follow the existing row style) documenting `unique` + `where` (partial) indexes — Postgres-native `CREATE [UNIQUE] INDEX … [WHERE]`, the `CONFLICT` code, four-client mirror status, and that it is a par-rt-db advantage (Convex lacks a declarative unique constraint). Update the §7 status line date.

- [ ] **Step 2: Update `CLAUDE.md`**

Add a one-line note in the "Data pipeline" architecture bullet: unique + partial indexes compile to `CREATE [UNIQUE] INDEX … [WHERE]`; a uniqueness violation surfaces as `CONFLICT` (409).

- [ ] **Step 3: Run the full gate**

```bash
make dev-db-up
make ts-client-build
make checkall
```
Expected: PASS across server + ts-client + rust-client + dashboard + python-client. Fix any fallout before committing.

- [ ] **Step 4: Commit + close the board**

```bash
git add FEATURE_MATRIX.md CLAUDE.md
git commit -m "docs: unique + partial indexes (feature matrix + CLAUDE.md)"
```

Mark the kanban item done:
```bash
kanban item done --id <item-id-from-board>
```

---

## Self-Review (run after writing, fix inline)

**Spec coverage:** §1 schema flags → Task 2. §2 DDL → Task 4. §3 `compile_filter_literal` → Task 3. §4 destructive arms → Task 4. §5 dup pre-check → Task 4. §6 `CONFLICT` + 23505 mapping → Tasks 1+5. §7 four-client wire mirror → Tasks 6–8. §8 in-memory enforcement → Tasks 6–8. §9 unchanged subs/op-feed → no task needed (asserted by the gate). §10 testing → embedded TDD in every task. §11 invariants → Global Constraints. No spec section unaddressed.

**Placeholder scan:** every code step shows concrete code or names the exact existing helper to mirror; test steps show real assertions. No "TODO"/"add error handling".

**Type consistency:** `IndexDef.unique: bool` + `r#where: Option<FilterExpr>` (server) mirrored as `unique?: boolean`/`where?: FilterExpr` (ts), `unique: bool`/`r#where: Option<FilterExpr>` (rust), `unique`/`where` (python). Wire keys `unique`/`where` and code `CONFLICT`/`Conflict` used consistently. `compile_filter_literal` signature identical wherever referenced.

## Execution

Plan complete and saved to `docs/superpowers/plans/2026-08-01-unique-indexes.md`. Per the user's standing preference, executing via **subagent-driven-development** (fresh subagent per task, review between tasks). Task dependencies: 1→2→3→4 are sequential (server core, each consumes the prior); 5 depends on 1–4; **6, 7, 8 are independent of each other** (parallelizable) but depend on the server wire being final (1–4).
