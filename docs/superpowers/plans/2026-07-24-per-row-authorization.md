# Per-Row Authorization (v1: Owner-Field Match) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in, per-table owner-field authorization rule so an authenticated user can read/mutate only their own rows on tables that declare an `ownerField`, enforced server-side on query, mutation, and subscription re-run — machine tokens and scheduled jobs bypass.

**Architecture:** The owner filter is enforced *inside* the executors (`execute_query`, `execute_txn`) via a new `owner: Option<&str>` parameter (`Some(user_id)` = enforce; `None` = bypass for machine/scheduled). No new `Principal` variant — a small `owner_of(&Principal) -> Option<&str>` helper derives it. Reads inject a server-side `FilterExpr::Eq{ownerField, user_id}` (and a post-fetch check for `get`). Writes stamp the owner on insert and run an ownership pre-check on patch/replace/delete/upsert inside the existing serialized transaction, so `RtDbError::forbidden(...)` rolls the whole txn back atomically. Subscriptions store the subscriber's `owner` on the `SubEntry` so `fan_out` re-runs the query with it — the stored query stays original.

**Tech Stack:** Rust (axum/tokio, sqlx, Postgres 17) server; TypeScript (bun) `ts-client`; Rust `rust-client`. Wire contract across `server/src/protocol.rs` + schema structs in all three.

**Approved spec:** `docs/superpowers/specs/2026-07-24-per-row-authorization-design.md` (read it before starting).

## Global Constraints

Copied from the spec + repo invariants; every task's requirements implicitly include these.

- **No embedded JS runtime.** Rules are declarative — `ownerField` is the whole v1 DSL. No arbitrary auth code.
- **Single-writer invariant preserved.** All writes (incl. the ownership check) go through the one per-db committer → `execute_txn`. Never call `execute_txn`/`execute_query` from a non-committer production path. Scheduled txns run via the committer's `RunScheduled` arm with `owner = None` (bypass).
- **Additive-only schema.** `owner_field: Option<String>` on `TableDef`, `#[serde(default, skip_serializing_if = "Option::is_none", rename = "ownerField")]`. Existing schemas/databases deserialize unchanged. Toggling `ownerField` is non-destructive (no column created/dropped).
- **SQL safety.** Every identifier schema-validated + double-quoted; every value bound via `$n`. Never interpolate an unvalidated value. The owner `user_id` flows only through `$n`-bound filter binds / `fetch_optional` rows — never into SQL text.
- **Errors.** Unauthorized reads **filter silently** (the user sees fewer/no rows — `Doc(None)` for `get`). Unauthorized writes return `RtDbError::forbidden(...)` → envelope `{code:"FORBIDDEN", message}`, HTTP 403, aborting the txn atomically (the sqlx guard rolls back on `?`). No new error variant.
- **Three clients mirror the schema.** `ownerField` round-trips byte-identically across server `schema.rs`, `ts-client` (`TableJson` + builder), and `rust-client` (`TableDef` + builder). Enforcement is server-only; clients only declare.
- **No `unwrap()`/`expect()` outside `#[cfg(test)]`.** Zero clippy warnings under `-D warnings`.
- **Fresh-state editing.** Line numbers below are accurate as of this plan but drift the moment you edit. Re-`Read` each region before applying an edit (R6); trust the *code shown*, re-derive the *line number*.
- **Verification gate.** `make checkall` (fmt-check + clippy `-D warnings` + typecheck + tests) must pass before any commit. Integration tests need the dev DB: `make dev-db-up`.

## File Structure

| File | Responsibility | Touched by task |
|---|---|---|
| `server/src/schema.rs` | `TableDef.owner_field` + `validate_structure` validation + round-trip test | 1 |
| `server/src/auth/mod.rs` | `owner_of(&Principal) -> Option<&str>` helper | 2 |
| `server/src/query.rs` | `execute_query` gains `owner`; main-path owner-filter injection; `point_read` owner check; (search/vector in task 4) | 2, 4 |
| `server/src/subs.rs` | `SubEntry.owner`; `register` takes owner; `fan_out` passes owner to `execute_query` | 3 |
| `server/src/committer.rs` | thread owner through `CommitterRequest::{Subscribe,Mutate}`, `Committers::{subscribe,mutate}`, `handle_subscribe`/`handle_mutate`; `handle_scheduled` passes `None` | 3, 5 |
| `server/src/ws.rs` | Subscribe + Mutate arms pass `owner_of(&principal)` | 3, 5 |
| `server/src/http_api.rs` | query + mutate handlers pass `owner_of(&principal)` | 2, 5 |
| `server/src/txn.rs` | `execute_txn` gains `owner`; insert stamp + patch/replace/delete/upsert ownership checks | 5 |
| `server/tests/per_row_auth_test.rs` | new integration binary: read filtering, write enforcement, bypass, subscriptions | 2, 3, 5 |
| `server/tests/common/mod.rs` | `mint_user_session(pool, user_id, email) -> token` helper | 7 |
| `server/tests/{txn,query,subs,...}_test.rs` | direct `execute_txn`/`execute_query` call sites gain the `owner` arg (`None`) | 2, 5 |
| `ts-client/src/protocol.ts`, `ts-client/src/schema.ts` | `TableJson.ownerField?` + `.ownerField()` builder + tests | 6 |
| `rust-client/src/schema.rs` | `TableDef.owner_field` + `TableBuilder::owner_field()` + tests | 6 |
| `FEATURE_MATRIX.md`, spec, `CLAUDE.md`, READMEs | flip #20 ❌→✅; spec status Design→Implemented | 8 |

---

## Task 1: Schema `ownerField` (server + validation)

**Files:**
- Modify: `server/src/schema.rs` (`TableDef` ~`:62-67`; `validate_structure` ~`:198-312`, after the fields loop ~`:212`)
- Test: `server/src/schema.rs` (new tests, pattern of `search_index_round_trips_and_validates` ~`:1151-1183`)

**Interfaces:**
- Produces: `TableDef { fields, indexes, owner_field: Option<String> }`. Later tasks read `table_def.owner_field.as_deref()`.

- [ ] **Step 1: Read the current `TableDef` and the additive-field precedent**

Run: `Read server/src/schema.rs` around `:60-95` (TableDef + IndexDef + constants) and `:1151-1183` (the `search_index_round_trips_and_validates` test). Confirm `is_valid_identifier(name, MAX_FIELD_NAME_LEN)` (~`:83`), `indexed_column_type(&FieldType)` (~`:178-195`), and `MAX_FIELD_NAME_LEN` (~`:78`).

- [ ] **Step 2: Add the `owner_field` field to `TableDef`**

```rust
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TableDef {
    pub fields: BTreeMap<String, FieldType>,
    #[serde(default)]
    pub indexes: Vec<IndexDef>,
    /// Opt-in per-row authorization: names a declared, string-compatible
    /// field whose value is the owning user's `user_id`. When set, an
    /// authenticated user reads/mutates only their own rows on this table;
    /// machine tokens and scheduled jobs bypass. Server-enforced; clients
    /// only declare it. Additive — schemas without it deserialize unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "ownerField")]
    pub owner_field: Option<String>,
}
```

- [ ] **Step 3: Add validation in `validate_structure`**

In `TableDef::validate_structure` (~`:198-312`), immediately after the fields loop closes (~`:212`), before the index loop, add:

```rust
if let Some(owner) = &self.owner_field {
    if !is_valid_identifier(owner, MAX_FIELD_NAME_LEN) {
        return Err(RtDbError::bad_request(format!(
            "ownerField '{owner}' is not a valid identifier"
        )));
    }
    let field_type = self.fields.get(owner).ok_or_else(|| {
        RtDbError::bad_request(format!("ownerField '{owner}' is not a declared field"))
    })?;
    // The owner value is a user_id (string); the field must be
    // string-compatible so the equality predicate is sound and (if indexed)
    // can back a typed column.
    if indexed_column_type(field_type).is_err() {
        return Err(RtDbError::bad_request(format!(
            "ownerField '{owner}' must be a string-compatible field (string/id/literal/union of strings)"
        )));
    }
}
```

Confirm `indexed_column_type` returns the text-mapping types (`String`/`Id`/`Literal`/string-`Union`) as `Ok` and everything else as `Err` (it does — it's the function the DDL/index path already uses). If its error type isn't `is_err()`-checkable, adapt to `matches!(indexed_column_type(field_type), Ok(_))`.

- [ ] **Step 4: Write the failing test**

Add to `server/src/schema.rs` tests (clone the shape of `search_index_round_trips_and_validates`):

```rust
#[test]
fn owner_field_round_trips_and_validates() {
    let json = r#"{"fields":{"title":{"type":"string"},"userId":{"type":"string"}},"indexes":[{"name":"by_user","fields":["userId"]}],"ownerField":"userId"}"#;
    let td: TableDef = serde_json::from_str(json).unwrap();
    assert_eq!(td.owner_field.as_deref(), Some("userId"));
    // camelCase wire key survives a round trip
    let re = serde_json::to_value(&td).unwrap();
    assert_eq!(re["ownerField"], "userId");

    // validates as part of a schema
    let mut tables = std::collections::BTreeMap::new();
    tables.insert("notes".to_string(), td);
    let schema = SchemaDef { tables };
    schema.validate().unwrap();

    // absent ownerField is omitted from the wire and deserializes as None
    let none_json = r#"{"fields":{"title":{"type":"string"}}}"#;
    let td2: TableDef = serde_json::from_str(none_json).unwrap();
    assert!(td2.owner_field.is_none());
    assert!(!serde_json::to_string(&td2).unwrap().contains("ownerField"));
}

#[test]
fn owner_field_validation_rejects_bad_declarations() {
    fn validate_owner(owner_json: &str) -> Result<(), RtDbError> {
        let json = format!(
            r#"{{"fields":{{"title":{{"type":"string"}},"num":{{"type":"number"}}}},"ownerField":{owner_json}}}"#
        );
        let td: TableDef = serde_json::from_str(&json).unwrap();
        let mut tables = std::collections::BTreeMap::new();
        tables.insert("t".to_string(), td);
        SchemaDef { tables }.validate()
    }
    // names an undeclared field
    assert!(validate_owner(r#""missing""#).is_err());
    // names a non-string field (number) — not string-compatible
    assert!(validate_owner(r#""num""#).is_err());
    // valid
    assert!(validate_owner(r#""title""#).is_ok());
}
```

If `SchemaDef { tables }` or `.validate()` don't match the real construction (check how the existing test builds a `SchemaDef`), mirror that exact construction instead.

- [ ] **Step 5: Run the tests**

Run: `cd server && cargo test --lib schema:: -- --nocapture` then `cargo test --lib owner_field`
Expected: PASS. (Unit schema tests don't need the DB.)

- [ ] **Step 6: Commit**

```bash
git add server/src/schema.rs
git commit -m "feat(server): add ownerField to TableDef (per-row auth v1 schema)"
```

---

## Task 2: Read enforcement — `execute_query` (main path + `get`)

**Files:**
- Modify: `server/src/auth/mod.rs` (new `owner_of` helper)
- Modify: `server/src/query.rs` (`execute_query` ~`:179`; filter block ~`:402-413`; `point_read` ~`:1088-1109`)
- Modify: `server/src/committer.rs` (`handle_subscribe` ~`:402` — pass `None` for now), `server/src/subs.rs` (`fan_out` ~`:151` — pass `None` for now), `server/src/http_api.rs` (`query_handler` ~`:71`)
- Modify: every direct `execute_query(...)` call site in `server/tests/**` (pass `None`)
- Test: `server/tests/per_row_auth_test.rs` (new file)

**Interfaces:**
- Consumes: `TableDef.owner_field` (Task 1).
- Produces: `pub fn owner_of(&Principal) -> Option<&str>`; `execute_query(pool, db, schema, q, owner: Option<&str>)`. Subscription wiring of the *real* owner is Task 3; here `handle_subscribe`/`fan_out` pass `None` (today's behavior preserved).

- [ ] **Step 1: Add `owner_of` helper**

In `server/src/auth/mod.rs`, after the `Principal` enum:

```rust
/// Per-row authorization identity for `principal`. `Some(user_id)` means
/// "enforce owner-field equality against this user" on any table that
/// declares an `ownerField`; `None` means bypass (machine tokens). Scheduled
/// jobs pass `None` directly — they have no caller.
pub fn owner_of(principal: &Principal) -> Option<&str> {
    match principal {
        Principal::User { user_id, .. } => Some(user_id.as_str()),
        Principal::Machine { .. } => None,
    }
}
```

- [ ] **Step 2: Write the failing tests (new file)**

Create `server/tests/per_row_auth_test.rs`. It pushes a schema with an owner-gated `notes` table and a plain `open` table, seeds rows for two users, and asserts filtering. Use the **direct-executor pattern** (call `execute_query`/`execute_txn` directly — no HTTP/WS yet), building `Principal::User` inline (all fields are `pub`):

```rust
mod common;

use common::{kanban_schema, test_state};
use par_rt_db_server::auth::Principal;
use par_rt_db_server::query::execute_query;
use par_rt_db_server::schema::{FieldType, IndexDef, SchemaDef, TableDef};
use par_rt_db_server::txn::{execute_txn, Step, Transaction};
use std::collections::BTreeMap;

fn owner_schema() -> SchemaDef {
    // `notes` is owner-gated; `open` is not.
    let mut notes_fields = BTreeMap::new();
    notes_fields.insert("title".to_string(), FieldType::String);
    notes_fields.insert("userId".to_string(), FieldType::String);
    let mut notes_indexes = Vec::new();
    notes_indexes.push(IndexDef { name: "by_user".into(), fields: vec!["userId".into()], search: false, vector: None });
    let mut tables = BTreeMap::new();
    tables.insert("notes".to_string(), TableDef { fields: notes_fields, indexes: notes_indexes, owner_field: Some("userId".into()) });
    // an open table (no owner_field) — reuse a trivial shape
    let mut open_fields = BTreeMap::new();
    open_fields.insert("name".to_string(), FieldType::String);
    tables.insert("open".to_string(), TableDef { fields: open_fields, indexes: vec![], owner_field: None });
    SchemaDef { tables }
}

fn user(id: &str) -> Principal {
    Principal::User {
        user_id: id.into(), email: format!("{id}@x"), name: None,
        expires_at: i64::MAX, github_id: None, github_login: None,
    }
}
```

(Confirm `IndexDef`'s exact field set and `FieldType::String` spelling against `schema.rs` before writing; adjust if the real struct differs. `i64::MAX` for `expires_at` keeps the session from expiring — `authorize` isn't called on the direct path anyway.)

Add the read-filtering tests:

```rust
#[tokio::test]
async fn user_reads_only_own_rows_on_owner_table() {
    let state = test_state().await;
    let db = format!("t{}", uuid::Uuid::new_v4());
    par_rt_db_server::db::create_database(&state.pool, &db).await.unwrap();
    par_rt_db_server::ddl::push_schema(&state.pool, &db, owner_schema()).await.unwrap();
    let schema = owner_schema();

    // seed: alice owns 2, bob owns 1 (insert as bypass = None so ownership stamps aren't forced)
    for (title, uid) in [("a1","alice"), ("a2","alice"), ("b1","bob")] {
        let mut doc = serde_json::Map::new();
        doc.insert("title".into(), title.into());
        doc.insert("userId".into(), uid.into());
        execute_txn(&state.pool, &db, &schema,
            &Transaction { steps: vec![Step::Insert { table: "notes".into(), doc }] }, None).await.unwrap();
    }

    let res = execute_query(&state.pool, &db, &schema,
        &par_rt_db_server::query::Query { table: "notes".into(), take: Some(100), ..Default::default() },
        Some("alice")).await.unwrap();
    let titles: Vec<&str> = docs_titles(&res);
    assert_eq!(titles, vec!["a1", "a2"]); // bob's row filtered out
}

#[tokio::test]
async fn bypass_owner_reads_all_rows() {
    // same seed; query with owner = None (machine/scheduled) sees all 3
    // ... assert 3 titles ...
}

#[tokio::test]
async fn get_point_read_filters_unowned() {
    // seed alice's doc, get its id; bob's get(id) -> Doc(None); alice's get(id) -> Doc(Some)
}

#[tokio::test]
async fn non_owner_table_is_unaffected_by_owner() {
    // insert into `open`; query with Some("alice") returns all `open` rows (no owner_field -> no filter)
}
```

(`Query::Default::default()` requires `Query: Default`; if it isn't derived, construct it field-by-field the way `query_test.rs` does — read one existing query test first and mirror its `Query { .. }` construction. Provide a `docs_titles` helper that extracts `title` from `QueryResult::Docs`.)

- [ ] **Step 3: Run tests to verify they fail**

Run: `make dev-db-up && cd server && cargo test --test per_row_auth_test`
Expected: FAIL to compile — `execute_query`/`execute_txn` don't yet take `owner`.

- [ ] **Step 4: Add the `owner` param + main-path injection in `execute_query`**

Change the signature and inject the owner filter. The filter block (~`:405-413`) currently reads `&q.filter`; replace it with an `effective_filter`:

```rust
pub async fn execute_query(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    q: &Query,
    owner: Option<&str>,
) -> Result<QueryResult, RtDbError> {
    validate_db_name(db)?;
    let table_def = schema.table(&q.table)?;
    let owner_field = table_def.owner_field.as_deref();

    if let Some(id) = &q.get {
        // ...existing validation unchanged...
        return point_read(pool, db, &q.table, id, owner_field, owner).await;
    }
    // ... (search/vector branches handled in Task 4 — pass owner_field+owner through when implemented;
    //      for now leave them calling execute_search/execute_vector_search without owner; Task 4 closes this) ...
```

At the filter block, replace `match &q.filter { ... }` with:

```rust
    let effective_filter = owner_filter(q.filter.as_ref(), owner_field, owner);
    let filter_binds: Vec<EqBind> = match &effective_filter {
        Some(filter) => {
            let (fragment, binds) =
                compile_filter(filter, table_def, eq_len + range_binds.len() + 1)?;
            where_conditions.push(fragment);
            binds
        }
        None => Vec::new(),
    };
```

Add the helper near `compile_filter`:

```rust
/// Wraps the client-supplied `filter` with the owner equality predicate when
/// the table declares an `ownerField` and the caller is a user (`owner`).
/// Bypass callers (`None`) and tables without `ownerField` get the original
/// filter back unchanged — no enforcement. The owner value is `$n`-bound by
/// `compile_filter`, never interpolated into SQL.
fn owner_filter(
    client_filter: Option<&FilterExpr>,
    owner_field: Option<&str>,
    owner: Option<&str>,
) -> Option<FilterExpr> {
    match (client_filter, owner_field, owner) {
        (Some(f), Some(field), Some(uid)) => Some(FilterExpr::And {
            exprs: vec![
                f.clone(),
                FilterExpr::Eq { field: field.to_string(), value: serde_json::Value::String(uid.to_string()) },
            ],
        }),
        (None, Some(field), Some(uid)) => Some(FilterExpr::Eq {
            field: field.to_string(),
            value: serde_json::Value::String(uid.to_string()),
        }),
        (Some(f), _, _) => Some(f.clone()),
        (None, _, _) => None,
    }
}
```

- [ ] **Step 5: Add the owner check to `point_read`**

```rust
async fn point_read(
    pool: &PgPool,
    db: &str,
    table_name: &str,
    id: &str,
    owner_field: Option<&str>,
    owner: Option<&str>,
) -> Result<QueryResult, RtDbError> {
    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(table_name);
    let row: Option<(String, serde_json::Value, i64, i64)> = sqlx::query_as(&format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE \"id\" = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?;

    match row {
        Some((id, doc, created_at, version)) => {
            // Per-row: a user may only point-read a doc they own. Silent
            // filter (Convex-like) — unowned docs read as absent.
            if let (Some(field), Some(uid)) = (owner_field, owner) {
                if doc.get(field).and_then(|v| v.as_str()) != Some(uid) {
                    return Ok(QueryResult::Doc(None));
                }
            }
            Ok(QueryResult::Doc(Some(merge_doc(id, doc, created_at, version)?)))
        }
        None => Ok(QueryResult::Doc(None)),
    }
}
```

- [ ] **Step 6: Update call sites (pass `owner`; `None` where not yet wired)**

- `server/src/http_api.rs` `query_handler` (~`:71`): `execute_query(&state.pool, &body.db, &schema, &body.query, auth::owner_of(&principal)).await?` (`principal` is in scope at ~`:67`).
- `server/src/committer.rs` `handle_subscribe` (~`:402`): `execute_query(&ctx.pool, &ctx.db, &schema, &query, None).await?` (Task 3 passes the real owner).
- `server/src/subs.rs` `fan_out` (~`:151`): `execute_query(pool, db, schema, &entry.query, None).await` (Task 3 passes `entry.owner`).
- Every `execute_query(` call in `server/tests/**`: add `, None` (or `, Some(...)` for the new tests). Find them with `cd server && grep -rn "execute_query(" tests/`.

- [ ] **Step 7: Run tests to verify they pass**

Run: `make dev-db-up && cd server && cargo test --test per_row_auth_test && cargo test --test query_test`
Expected: PASS (new read-filtering tests green; existing query tests unaffected).

- [ ] **Step 8: Verify the full gate**

Run: `make checkall`
Expected: PASS (fmt + clippy + typecheck + all tests). Fix any clippy/type errors (e.g. unused imports).

- [ ] **Step 9: Commit**

```bash
git add server/src/auth/mod.rs server/src/query.rs server/src/http_api.rs server/src/committer.rs server/src/subs.rs server/tests/
git commit -m "feat(server): enforce owner-field read filtering in execute_query (#20)"
```

---

## Task 3: Read enforcement — subscriptions

Make `fan_out` re-run each subscription's query with *that subscriber's* owner, so a write by user B never pushes B's rows to A's subscription.

**Files:**
- Modify: `server/src/subs.rs` (`SubEntry` ~`:45-53`; `register` ~`:95-115`; `fan_out` ~`:151`)
- Modify: `server/src/committer.rs` (`CommitterRequest::Subscribe` ~`:26-32`; `Committers::subscribe` ~`:165-188`; `handle_subscribe` ~`:394-428`)
- Modify: `server/src/ws.rs` (Subscribe arm ~`:295-311`)
- Test: `server/tests/per_row_auth_test.rs`

**Interfaces:**
- Consumes: `execute_query(..., owner)` (Task 2), `owner_of` (Task 2).
- Produces: `SubEntry.owner: Option<String>`; `register(..., owner: Option<String>)`; `Committers::subscribe(..., owner: Option<String>)`; `CommitterRequest::Subscribe { ..., owner: Option<String> }`.

- [ ] **Step 1: Write the failing test**

Add to `server/tests/per_row_auth_test.rs` a subscription test. Two WS connections authenticated as alice and bob subscribe to `notes`; bob inserts; assert alice's subscription does **not** receive bob's row, bob's does. This needs two real user sessions over WS — use the `mint_user_session` helper **built in Task 7**. Since that helper doesn't exist yet, write this test now but mark the whole test file's WS section behind Task 7's helper; **for Task 3, verify via a unit-style simulation instead**: call `fan_out` directly after registering two `SubEntry`s with different owners and assert only the matching subscriber is pushed.

Unit-style subscription test (no WS):

```rust
#[tokio::test]
async fn fan_out_does_not_push_cross_user_rows() {
    let state = test_state().await;
    let db = fresh_owner_db(&state).await; // helper: create db + push owner_schema
    let schema = owner_schema();
    // seed alice + bob rows (bypass insert) ...
    // register two subs on `notes`, one owned by alice, one by bob, each with an
    // mpsc receiver; call subs.fan_out after a write touching bob's doc.
    // assert alice's rx stays empty (or unchanged) and bob's rx gets bob's doc.
}
```

(Confirm `SubscriptionManager`'s public surface — `register`/`fan_out` visibility and the `ConnId`/`tx` types — by reading `subs.rs` first; the test must use whatever the committer uses. If `fan_out`/`register` are `pub(crate)`, the test must live in `server/tests/` which can reach `pub(crate)` via the crate — confirm, or expose a test helper.)

- [ ] **Step 2: Run to verify it fails**

Run: `make dev-db-up && cd server && cargo test --test per_row_auth_test fan_out_does_not_push_cross_user_rows`
Expected: FAIL (compile — `register`/`fan_out` don't take/pass owner yet).

- [ ] **Step 3: Add `owner` to `SubEntry` and `register`**

`server/src/subs.rs`:

```rust
struct SubEntry {
    query: Query,
    tx: UnboundedSender<ServerMessage>,
    last: String,
    read_set: ReadSet,
    owner: Option<String>,   // NEW — the subscriber's user_id (None = bypass)
}
```

In `register` (~`:95-115`), add `owner: Option<String>` param and store it:

```rust
pub(crate) async fn register(
    &self,
    db: &str,
    conn: ConnId,
    query_id: String,
    query: Query,
    tx: UnboundedSender<ServerMessage>,
    last: String,
    owner: Option<String>,   // NEW
) {
    let read_set = ReadSet::from_query(&query);
    // ...
    guard.entry(db.to_string()).or_default().insert(
        (conn, query_id),
        SubEntry { query, tx, last, read_set, owner },   // NEW
    );
}
```

- [ ] **Step 4: Pass `owner` in `fan_out`**

In `fan_out` (~`:151`):

```rust
let result = match execute_query(pool, db, schema, &entry.query, entry.owner.as_deref()).await {
```

- [ ] **Step 5: Thread `owner` through the committer**

`server/src/committer.rs`:

`CommitterRequest::Subscribe` gains `owner: Option<String>`. `Committers::subscribe` gains `owner: Option<String>` and forwards it. `handle_subscribe` passes it to both `execute_query` (replacing the `None` from Task 2) and `register`:

```rust
async fn handle_subscribe(
    ctx: &CommitterCtx,
    conn: ConnId,
    query_id: String,
    query: Query,
    tx: UnboundedSender<ServerMessage>,
    owner: Option<String>,   // NEW
) -> Result<(), RtDbError> {
    let schema = ctx.schemas.get(&ctx.pool, &ctx.db).await?;
    let result = execute_query(&ctx.pool, &ctx.db, &schema, &query, owner.as_deref()).await?;  // owner
    // ... existing push ...
    ctx.subs
        .register(&ctx.db, conn, query_id, query, tx, last, owner)   // owner
        .await;
    Ok(())
}
```

Update the `Subscribe` dispatch in `run_committer` (~`:248`) to forward `owner`.

- [ ] **Step 6: Pass `owner` from the WS Subscribe arm**

`server/src/ws.rs` (~`:295-311`): the arm has `principal: &Principal` in scope. Pass `auth::owner_of(principal).map(|s| s.to_string())` as the new `owner` arg to `state.committers.subscribe(...)`.

- [ ] **Step 7: Run the subscription test**

Run: `make dev-db-up && cd server && cargo test --test per_row_auth_test`
Expected: PASS — bob's write does not push to alice's subscription.

- [ ] **Step 8: Verify the full gate**

Run: `make checkall`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add server/src/subs.rs server/src/committer.rs server/src/ws.rs server/tests/per_row_auth_test.rs
git commit -m "feat(server): capture subscriber owner so fan_out never pushes cross-user rows (#20)"
```

---

## Task 4: Read enforcement — search + vectorSearch

`search` and `vectorSearch` reject `q.filter`, so the Task-2 filter-injection doesn't cover them. Add an owner predicate directly to their SQL.

**Files:**
- Modify: `server/src/query.rs` (`execute_query` search/vector branches ~`:292-338`; `execute_search` ~`:940+`; `execute_vector_search`)
- Test: `server/tests/per_row_auth_test.rs`

**Interfaces:**
- Consumes: `owner_field`, `owner` (in scope in `execute_query`).
- Produces: `execute_search`/`execute_vector_search` accept `owner_field: Option<&str>, owner: Option<&str>` and AND a `(doc->>'<ownerField>') = $n` clause when both are set.

- [ ] **Step 1: Read the search/vector SQL builders**

Run: `Read server/src/query.rs` for `execute_search` and `execute_vector_search` (search ~`:940+`; vector nearby). Note their WHERE-clause assembly and bind numbering.

- [ ] **Step 2: Write failing tests**

```rust
#[tokio::test]
async fn search_filters_to_own_rows() { /* owner-gated table with a search index; alice+bob docs; alice search -> only alice's */ }
#[tokio::test]
async fn vector_search_filters_to_own_rows() { /* owner-gated table with a vector index; same shape */ }
```

(Use the existing `search_test.rs`/`vector_test.rs` seed patterns for declaring search/vector indexes; add `owner_field` to the table.)

- [ ] **Step 3: Run to verify they fail**

Run: `make dev-db-up && cd server && cargo test --test per_row_auth_test search_filters`
Expected: FAIL — search returns other users' rows (the security gap this task closes).

- [ ] **Step 4: Thread `owner_field`/`owner` into the search/vector calls**

In `execute_query`, the search branch (~`:337`) and vector branch (~`:312`) currently call `execute_search(pool, db, table_def, &q.table, search, q.take)` / `execute_vector_search(pool, db, table_def, &q.table, vs)`. Add `owner_field, owner` args to both calls.

- [ ] **Step 5: AND the owner predicate in `execute_search` / `execute_vector_search`**

In each, when `(owner_field, owner)` = `(Some(field), Some(uid))`, add a WHERE condition `(doc->>'<field>') = $n` bound to `uid`, incrementing the placeholder offset for subsequent binds (LIMIT, etc.). Use the existing identifier-validation/`pg_col`-style quoting if `field` could be non-trivial — but `owner_field` is already `is_valid_identifier`-validated at schema push, so `doc->>'{field}'` interpolated into SQL text is safe (it passed the `^[a-zA-Z][a-zA-Z0-9_]*$` regex). Bind `uid` via `$n`. Pattern:

```rust
let mut owner_clause = String::new();
let mut owner_bind: Option<String> = None;
if let (Some(field), Some(uid)) = (owner_field, owner) {
    owner_clause = format!(" AND (doc->>'{field}') = ${owner_placeholder}");
    owner_bind = Some(uid.to_string());
}
// splice owner_clause into the WHERE; push owner_bind before the LIMIT bind
```

(Adapt to each function's exact bind ordering — re-read before editing.)

- [ ] **Step 6: Run the tests**

Run: `make dev-db-up && cd server && cargo test --test per_row_auth_test && cargo test --test search_test --test vector_test`
Expected: PASS (new tests green; existing search/vector tests unaffected — they pass `None`).

- [ ] **Step 7: Verify the full gate**

Run: `make checkall`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add server/src/query.rs server/tests/per_row_auth_test.rs
git commit -m "feat(server): enforce owner-field filtering on search + vectorSearch (#20)"
```

---

## Task 5: Write enforcement — insert stamp + ownership checks

**Files:**
- Modify: `server/src/txn.rs` (`execute_txn` ~`:681`; step loop ~`:701-778`; new helpers `stamp_owner`/`check_owner`/`check_owner_doc`)
- Modify: `server/src/committer.rs` (`CommitterRequest::Mutate` ~`:20-25`; `Committers::mutate` ~`:142-161`; `handle_mutate` ~`:265-312`; `handle_scheduled` ~`:334` passes `None`)
- Modify: `server/src/ws.rs` (Mutate arm ~`:316-338`), `server/src/http_api.rs` (`mutate_handler` ~`:98-101`)
- Modify: every direct `execute_txn(...)` call site in `server/tests/**` (pass `None`)
- Test: `server/tests/per_row_auth_test.rs`

**Interfaces:**
- Consumes: `TableDef.owner_field` (Task 1).
- Produces: `execute_txn(pool, db, schema, txn, owner: Option<&str>)`; `Committers::mutate(db, key, txn, owner: Option<String>)`; `CommitterRequest::Mutate { ..., owner: Option<String> }`.

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test]
async fn insert_auto_stamps_owner() {
    // alice inserts a note WITHOUT setting userId; assert the stored doc's userId == "alice"
}
#[tokio::test]
async fn insert_cannot_forge_another_users_owner() {
    // alice inserts with userId="bob"; assert stored userId == "alice" (server overwrites)
}
#[tokio::test]
async fn patch_on_unowned_doc_is_forbidden_and_atomic() {
    // txn = [insert alice's doc into `open` (unrelated, committed-then-rolled-back? no):
    //        patch bob's note (fields title="x")] by alice -> Err(FORBIDDEN);
    //        assert bob's note unchanged AND no partial write leaked
}
#[tokio::test]
async fn delete_and_replace_on_unowned_doc_are_forbidden() { /* same shape for delete + replace */ }
#[tokio::test]
async fn upsert_insert_branch_stamps_and_update_branch_checks_owner() {
    // upsert no-match -> stamps owner; upsert match on bob's doc by alice -> FORBIDDEN
}
#[tokio::test]
async fn machine_bypass_ignores_ownership() {
    // owner=None patches bob's doc as bob would via a machine token -> succeeds
}
```

For the atomicity test: a two-step txn `[ Step that would succeed, Step::Patch on bob's doc ]` by alice must fail AND roll back the first step. Assert via the error code (`FORBIDDEN`) and that the first step's effect is absent.

- [ ] **Step 2: Run to verify they fail**

Run: `make dev-db-up && cd server && cargo test --test per_row_auth_test insert_auto_stamps`
Expected: FAIL (compile — `execute_txn` doesn't take `owner`).

- [ ] **Step 3: Add the helpers in `txn.rs`**

```rust
/// Forces `doc[owner_field] = owner` for owner-gated tables when the caller is
/// a user, overwriting any client-supplied value. Bypass callers and
/// non-owner tables leave `doc` unchanged.
fn stamp_owner(
    table_def: &TableDef,
    mut doc: serde_json::Map<String, serde_json::Value>,
    owner: Option<&str>,
) -> serde_json::Map<String, serde_json::Value> {
    if let (Some(field), Some(uid)) = (&table_def.owner_field, owner) {
        doc.insert(field.clone(), serde_json::Value::String(uid.to_string()));
    }
    doc
}

/// Ownership pre-check for patch/replace/delete: fetches the doc and rejects
/// `Forbidden` if a user caller doesn't own it. A missing doc returns `Ok`
/// (the subsequent do_* step reports `NotFound`). Bypass/no-owner-table: no-op.
async fn check_owner(
    conn: &mut PgConnection,
    pg_schema_name: &str,
    table_def: &TableDef,
    table_name: &str,
    id: &str,
    owner: Option<&str>,
) -> Result<(), RtDbError> {
    let (Some(field), Some(uid)) = (&table_def.owner_field, owner) else {
        return Ok(());
    };
    let table_ident = pg_table(table_name);
    let row: Option<(serde_json::Value,)> = sqlx::query_as(&format!(
        "SELECT \"doc\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE \"id\" = $1"
    ))
    .bind(id)
    .fetch_optional(&mut *conn)
    .await?;
    match row {
        None => Ok(()),
        Some((doc,)) => {
            if doc.get(field).and_then(|v| v.as_str()) != Some(uid) {
                return Err(RtDbError::forbidden(format!(
                    "document '{id}' is not owned by the caller"
                )));
            }
            Ok(())
        }
    }
}

/// Ownership check on a doc already in hand (upsert update branch).
fn check_owner_doc(
    table_def: &TableDef,
    doc: &serde_json::Map<String, serde_json::Value>,
    id: &str,
    owner: Option<&str>,
) -> Result<(), RtDbError> {
    let (Some(field), Some(uid)) = (&table_def.owner_field, owner) else {
        return Ok(());
    };
    if doc.get(field).and_then(|v| v.as_str()) != Some(uid) {
        return Err(RtDbError::forbidden(format!(
            "document '{id}' is not owned by the caller"
        )));
    }
    Ok(())
}
```

(Confirm `PgConnection` is the connection type in scope — `txn.rs` uses `&mut tx` where `tx` is a sqlx transaction; `do_*` take `conn: &mut PgConnection`. Match that.)

- [ ] **Step 4: Add `owner` to `execute_txn` and wire the step loop**

```rust
pub async fn execute_txn(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    txn: &Transaction,
    owner: Option<&str>,   // NEW
) -> Result<TxnOutcome, RtDbError> {
    // ...
    for step in &txn.steps {
        match step {
            Step::Insert { table, doc } => {
                let table_def = schema.table(table)?;
                let doc = stamp_owner(table_def, doc.clone(), owner);   // NEW
                let id = do_insert(&mut tx, &pg_schema_name, table_def, table, &doc).await?;
                write_set.touch(table, &id);
                results.push(serde_json::json!({ "id": id }));
            }
            Step::Patch { table, id, fields } => {
                let table_def = schema.table(table)?;
                check_owner(&mut tx, &pg_schema_name, table_def, table, id, owner).await?;   // NEW
                do_patch(&mut tx, &pg_schema_name, table_def, table, id, fields).await?;
                write_set.touch(table, id);
                results.push(serde_json::Value::Null);
            }
            Step::Replace { table, id, doc } => {
                let table_def = schema.table(table)?;
                check_owner(&mut tx, &pg_schema_name, table_def, table, id, owner).await?;   // NEW
                do_replace(&mut tx, &pg_schema_name, table_def, table, id, doc).await?;
                write_set.touch(table, id);
                results.push(serde_json::Value::Null);
            }
            Step::Delete { table, id } => {
                let table_def = schema.table(table)?;
                check_owner(&mut tx, &pg_schema_name, table_def, table, id, owner).await?;   // NEW
                do_delete(&mut tx, &pg_schema_name, table, id).await?;
                write_set.touch(table, id);
                results.push(serde_json::Value::Null);
            }
            // ExpectVersion / ExpectAbsent: unchanged (not in the v1 spec's
            // write-enforcement list — see plan notes).
            Step::Upsert { table, index, eq, insert, patch } => {
                let table_def = schema.table(table)?;
                let mut rows = eq_lookup(&mut tx, &pg_schema_name, table_def, table, index, eq).await?;
                if rows.len() > 1 {
                    return Err(RtDbError::precondition("upsert matched multiple documents"));
                }
                match rows.pop() {
                    None => {
                        let insert = stamp_owner(table_def, insert.clone(), owner);   // NEW
                        let id = do_insert(&mut tx, &pg_schema_name, table_def, table, &insert).await?;
                        write_set.touch(table, &id);
                        results.push(serde_json::json!({ "id": id, "inserted": true }));
                    }
                    Some((id, doc_value)) => {
                        let doc = match doc_value {
                            serde_json::Value::Object(map) => map,
                            _ => return Err(RtDbError::internal("stored doc is not a JSON object")),
                        };
                        check_owner_doc(table_def, &doc, &id, owner)?;   // NEW
                        let merged = apply_patch(table_def, doc, patch)?;
                        apply_update(&mut tx, &pg_schema_name, table_def, table, &id, merged).await?;
                        write_set.touch(table, &id);
                        results.push(serde_json::json!({ "id": id, "inserted": false }));
                    }
                }
            }
            // ExpectVersion / ExpectAbsent arms unchanged
        }
    }
    // ...
}
```

- [ ] **Step 5: Thread `owner` through the committer + transports**

`server/src/committer.rs`:
- `CommitterRequest::Mutate { idempotency_key, txn, owner: Option<String>, reply }` (new field).
- `Committers::mutate(db, idempotency_key, txn, owner: Option<String>)` forwards it.
- `handle_mutate(ctx, idempotency_key, txn, owner)` → `execute_txn(&ctx.pool, &ctx.db, &schema, &txn, owner.as_deref()).await?`.
- `handle_scheduled` (~`:334`): `execute_txn(&ctx.pool, &ctx.db, &schema, &txn, None).await` (scheduled = bypass).
- Update the `Mutate` dispatch in `run_committer` (~`:238`) to forward `owner`.

`server/src/ws.rs` Mutate arm (~`:322`): pass `auth::owner_of(principal).map(|s| s.to_string())` to `state.committers.mutate(...)`.

`server/src/http_api.rs` `mutate_handler` (~`:98`): pass `auth::owner_of(&principal).map(|s| s.to_string())` to `state.commiters.mutate(...)`.

- [ ] **Step 6: Update direct `execute_txn(` call sites in tests**

Run: `cd server && grep -rn "execute_txn(" tests/` — add `, None` (or `, Some(...)` for the new tests) to each.

- [ ] **Step 7: Run the write-enforcement tests**

Run: `make dev-db-up && cd server && cargo test --test per_row_auth_test && cargo test --test txn_test`
Expected: PASS.

- [ ] **Step 8: Verify the full gate**

Run: `make checkall`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add server/src/txn.rs server/src/committer.rs server/src/ws.rs server/src/http_api.rs server/tests/
git commit -m "feat(server): enforce owner-field write rules — stamp on insert, ownership-check on mutate (#20)"
```

---

## Task 6: Client mirror — `ownerField` builders (ts-client + rust-client)

Enforcement is server-only; the clients just declare `ownerField` and round-trip it byte-identically.

**Files:**
- Modify: `ts-client/src/protocol.ts` (`TableJson` ~`:184-187`), `ts-client/src/schema.ts` (`TableDefinition` ~`:84-143`, `toJSON` ~`:136-142`)
- Modify: `rust-client/src/schema.rs` (`TableDef` ~`:91-96`; `TableBuilder` ~`:106-176`)
- Test: `ts-client/tests/schema.test.ts`, `rust-client/src/schema.rs` tests

**Interfaces:**
- Produces: `TableJson.ownerField?: string` (TS) + `.ownerField(name)` builder; `TableDef.owner_field: Option<String>` (rust-client) + `.owner_field(name: &str)` builder.

- [ ] **Step 1: ts-client wire + builder**

`ts-client/src/protocol.ts` `TableJson`:
```ts
export interface TableJson {
  fields: Record<string, FieldTypeJson>;
  indexes?: IndexJson[];
  ownerField?: string;   // NEW
}
```

`ts-client/src/schema.ts` `TableDefinition`:
- Add `private readonly ownerField?: string` ctor field.
- Add a chainable builder next to `index`/`searchIndex`:
```ts
ownerField(field: string): TableDefinition<Fields, Indexes> {
  this.ownerField = field;  // (store on the instance)
  return this;
}
```
- In `toJSON()`, emit conditionally (mirroring `indexes`):
```ts
toJSON(): TableJson {
  const json: TableJson = { fields: fieldsToJson(this.fields) };
  if (this.indexes.length > 0) json.indexes = this.indexes;
  if (this.ownerField) json.ownerField = this.ownerField;   // NEW
  return json;
}
```
(Match the real `TableDefinition` field-storage style — read it first; the class may store fields/indexes on the instance or in ctor locals. Mirror whatever `searchIndex` does.)

- [ ] **Step 2: ts-client test**

```ts
test("ownerField serializes and is omitted when absent", () => {
  const withOwner = defineTable({ userId: t.string(), title: t.string() })
    .index("by_user", ["userId"])
    .ownerField("userId");
  expect((withOwner as any).toJSON()).toMatchObject({ ownerField: "userId" });
  const without = defineTable({ title: t.string() });
  expect((without as any).toJSON()).not.toHaveProperty("ownerField");
});
```

- [ ] **Step 3: rust-client `TableDef` + builder**

`rust-client/src/schema.rs` `TableDef`:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDef {
    pub fields: BTreeMap<String, FieldType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexes: Option<Vec<IndexDef>>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "ownerField")]
    pub owner_field: Option<String>,   // NEW
}
```

`TableBuilder`: add `owner_field: Option<String>` field; a builder method next to `.index`:
```rust
pub fn owner_field(mut self, field: &str) -> Self { self.owner_field = Some(field.to_string()); self }
```
Carry it through `finish()` into the `TableDef`.

- [ ] **Step 4: rust-client test**

```rust
#[test]
fn owner_field_serializes_and_round_trips() {
    let td = Table::new().field("userId", FieldType::String).index("by_user", &["userId"]).owner_field("userId").finish();
    let json = serde_json::to_value(&td).unwrap();
    assert_eq!(json["ownerField"], "userId");
    let back: TableDef = serde_json::from_value(json).unwrap();
    assert_eq!(back.owner_field.as_deref(), Some("userId"));
    // absent -> omitted
    let none = Table::new().field("title", FieldType::String).finish();
    assert!(!serde_json::to_string(&none).unwrap().contains("ownerField"));
}
```
(Confirm `Table::new()` / `FieldType::String` / `.finish()` spellings against the existing `search_index_serializes_and_round_trips` test ~`:335-371` first.)

- [ ] **Step 5: Run client tests**

Run: `cd ts-client && bunx vitest run tests/schema.test.ts` and `cd rust-client && cargo test owner_field`
Expected: PASS.

- [ ] **Step 6: Verify the full gate**

Run: `make checkall`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add ts-client/src/protocol.ts ts-client/src/schema.ts ts-client/tests/schema.test.ts rust-client/src/schema.rs
git commit -m "feat(clients): add ownerField schema builder to ts-client + rust-client (#20)"
```

---

## Task 7: Integration tests — `mint_user_session` + HTTP/WS end-to-end

The Tasks 2–5 unit tests call executors directly. This task adds the one missing piece for true end-to-end coverage: a helper that mints real `Principal::User` sessions with caller-chosen `user_id`s, and HTTP/WS tests proving enforcement over the wire.

**Files:**
- Modify: `server/tests/common/mod.rs` (new `mint_user_session`)
- Modify: `server/tests/per_row_auth_test.rs` (HTTP + WS e2e tests; also unblocks the WS subscription test sketched in Task 3)

**Interfaces:**
- Produces: `common::mint_user_session(pool, user_id, email) -> String` (a bearer session token).

- [ ] **Step 1: Add `mint_user_session`**

Factor the inline seed at `oauth_test.rs:601-639` (`expired_session_returns_unauthorized`) into a reusable helper in `server/tests/common/mod.rs`:

```rust
/// Seeds a real `rtdb_auth.users` + `sessions` row for `user_id`/`email` and
/// returns a bearer session token that resolves to `Principal::User { user_id, .. }`.
/// Caller must allowlist `email` for the db separately. Distinct users need
/// distinct `github_id`s (the users table enforces uniqueness).
pub async fn mint_user_session(pool: &PgPool, user_id: &str, email: &str) -> String {
    let github_id: i64 = /* derive a stable distinct int from user_id (e.g. hash) */;
    sqlx::query("INSERT INTO rtdb_auth.users (id, github_id, login, email, created_at) VALUES ($1,$2,$3,$4,$5)")
        .bind(user_id).bind(github_id).bind(email).bind(email).bind(/* now_ms */)
        .execute(pool).await.unwrap();
    let token = /* db::random_token() or similar */;
    sqlx::query("INSERT INTO rtdb_auth.sessions (token_hash, user_id, expires_at, created_at) VALUES (sha256_hex($1),$2,$3,$4)")
        .bind(&token).bind(user_id).bind(/* far-future */).bind(/* now_ms */)
        .execute(pool).await.unwrap();
    token
}
```

Read `oauth_test.rs:601-639` first and copy its exact column names / `sha256_hex` / `now_ms` / id helpers. Confirm `db::random_token` / `db::new_id` visibility. Distinct `github_id`s: derive from `user_id` bytes so two calls with different ids never collide.

- [ ] **Step 2: Write HTTP + WS e2e tests**

```rust
#[tokio::test]
async fn http_query_filters_by_owner_over_the_wire() {
    // create db, push owner_schema, allowlist both emails, mint alice+bob sessions,
    // seed via admin/machine, then POST /api/query as alice -> only alice's rows
}
#[tokio::test]
async fn http_mutate_forbidden_on_unowned_doc() {
    // POST /api/mutate as alice patching bob's note -> 403 FORBIDDEN
}
#[tokio::test]
async fn ws_subscription_no_cross_user_push() {
    // two WS connections (alice, bob) subscribed to notes; bob mutates;
    // assert alice's QueryUpdate never contains bob's doc, bob's does
}
```

Use `spawn_app` + an HTTP client (mirror `http_api_test.rs`) and the WS auth flow (mirror `oauth_test.rs`'s `ws_auth`). Push the schema via `admin_post("/admin/push-schema", ...)`. Allowlist via `/admin/allowlist`.

- [ ] **Step 3: Run the e2e tests**

Run: `make dev-db-up && cd server && cargo test --test per_row_auth_test`
Expected: PASS.

- [ ] **Step 4: Verify the full gate**

Run: `make checkall`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add server/tests/common/mod.rs server/tests/per_row_auth_test.rs
git commit -m "test(server): per-row auth end-to-end over HTTP + WS; mint_user_session helper (#20)"
```

---

## Task 8: Docs — FEATURE_MATRIX #20, spec status, READMEs

**Files:**
- Modify: `FEATURE_MATRIX.md` (row #20 ~`:72`; "Recommended order" ~`:105-129`)
- Modify: `docs/superpowers/specs/2026-07-24-per-row-authorization-design.md` (Status line ~`:3`)
- Modify: `CLAUDE.md` (Auth section), server/ts-client/rust-client READMEs as needed

- [ ] **Step 1: Flip FEATURE_MATRIX #20**

Change row #20's `par-rt-db` cell from `❌ allowlist = full access` to `✅ owner-field match`, effort `✅ done`, and rewrite the Implementation-sketch cell in the same detailed style as rows #11/#17 (owner-field declaration mirrored across all three clients; server-enforced on query/mutate/subscription re-run; machine + scheduled bypass; insert auto-stamps; patch/replace/delete/upsert ownership-checked atomically; `get`/search/vector filtered; collaborator/role (B) and general predicate DSL (C) deferred). Mirror the exact phrasing conventions of neighboring "Implemented" rows.

- [ ] **Step 2: Update "Recommended order"**

In the §5 prose (~`:105-129`): move #20 into the "done" list with a one-line summary; remove it from "Remaining gaps" (~`:129`, which becomes just #18).

- [ ] **Step 3: Update the spec status line**

`docs/superpowers/specs/2026-07-24-per-row-authorization-design.md` ~`:3`: change `Status: Design (not implemented)` to `Status: Implemented (v1)` and add the commit/feature reference.

- [ ] **Step 4: Update CLAUDE.md + READMEs**

In `CLAUDE.md`'s "Auth" architecture bullet, note that per-row owner-field auth is now an additional layer (opt-in per table, enforced server-side on query/mutate/subscription, machine/scheduled bypass). Update any README that lists auth capabilities.

- [ ] **Step 5: Verify the full gate (docs don't break it, but confirm)**

Run: `make checkall`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add FEATURE_MATRIX.md docs/superpowers/specs/2026-07-24-per-row-authorization-design.md CLAUDE.md README.md server/README.md ts-client/README.md rust-client/README.md
git commit -m "docs: per-row authorization rules (#20) shipped — flip FEATURE_MATRIX, mark spec implemented"
```

---

## Self-Review (run before handing off)

**Spec coverage:** every spec section maps to a task — schema declaration (T1), principal/bypass model (T2 `owner_of`, T5 scheduled `None`), read enforcement query+subscription (T2/T3), write enforcement insert/patch/replace/delete/upsert (T5), search/vector reads (T4), scheduled bypass (T5), client mirror (T6), testing incl. the spec's enumerated cases (T2/T3/T5/T7), files list (T1/T2/T5). **Gap noted:** `ExpectVersion`/`ExpectAbsent` are intentionally not owner-checked in v1 (not in the spec's write-enforcement list) — documented in T5.

**Placeholder scan:** code shown for every novel step (helpers, injection, step-loop wiring, client builders). Steps that say "re-read X first" do so only where exact current text must be confirmed before a mechanical edit — the *change* is fully specified.

**Type consistency:** `owner: Option<&str>` on executors, `Option<String>` across the channel; `owner_of -> Option<&str>`; `SubEntry.owner: Option<String>`; helper names `stamp_owner`/`check_owner`/`check_owner_doc`/`owner_filter` used consistently. `TableDef.owner_field` spelled `owner_field` (Rust) / `ownerField` (wire + TS) everywhere.

## Execution

Plan complete and saved to `docs/superpowers/plans/2026-07-24-per-row-authorization.md`. Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.
**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
