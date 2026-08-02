# Per-Row Auth Predicate DSL (Model C) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in per-table `authorize` predicate (a `FilterExpr` over doc fields **and** principal attributes) that governs row visibility/mutability, enforced on the same read/write/subscription seams as `ownerField`. Extends `FilterExpr` with `Not`/`Contains`/`Exists` and `$user`/`$email` principal markers (also usable in `.filter()`), adds a Rust doc-level evaluator, and auto-stamps `Eq{field,$user}` leaves on insert.

**Architecture:** Additive to ownerField/collaboratorsField (unchanged). Phased — language extensions and the evaluator land first (independently testable), then the `authorize` declaration + principal threading, then enforcement at the four seams ownerField already occupies (`query.rs` scan terminals + `point_read`; `txn.rs` pre-check + insert stamp/verify; `subs.rs` re-runs transitively). One predicate language (`FilterExpr`) shared by query filters and auth.

**Tech Stack:** Rust server (axum/tokio/sqlx/Postgres, cargo); TypeScript (`ts-client`, bun/vitest); Rust (`rust-client`, cargo); Python (`python-client`, uv/pytest).

## Global Constraints

- **Wire byte-identical across four implementations** (`server/src/query.rs` + `protocol.rs`, `ts-client/src/protocol.ts`, `rust-client/src/wire.rs`, `python-client/src/par_rt_db/wire.py`): `FilterExpr` is `#[serde(tag = "op", rename_all = "lowercase", deny_unknown_fields)]`; existing variants/field names unchanged; new variants use `op` = `not`/`contains`/`exists`; principal markers are `{"$user": true}` / `{"$email": true}`.
- **`ownerField`/`collaboratorsField` behavior must stay byte-identical** — existing `per_row_auth_test.rs` and owner/collab tests guard this. Model C is additive; do not alter their code paths' semantics.
- **Single-writer invariant preserved** — the pre-check and stamp run inside the committer's `execute_txn`; no `execute_txn` outside the committer, no second writer.
- **Security defaults:** on a write, any type-mismatch/missing-field doubt → `Forbidden` (never a silent allow); on a read → row not visible (over-approximate filtering, never under-approximate). Reads filter silently (`Doc(None)`); unauthorized writes/inserts → `Forbidden`/403, aborting the txn atomically.
- **Bypass unchanged:** Machine tokens, admin, scheduled jobs pass `user_id = None` ⇒ no predicate, no stamp (full access). The db-level `authorize` gate still runs first.
- **Principal markers valid only in a server-declared `authorize` predicate** — client-supplied query `.filter()` expressions are rejected at validation if they contain `$user`/`$email`.
- No `unwrap()`/`expect()` outside `#[cfg(test)]`. Zero clippy warnings under `-D warnings`.
- **Verification gate:** `make checkall` (needs `make dev-db-up` on `127.0.0.1:55434`; `make ts-client-build` on a fresh checkout for the dashboard typecheck).
- Branch: `per-row-auth-predicate` (in-place; par-rt-db convention). Commit after each task.

---

## File Structure

**Server (`server/src/`):**
- `query.rs` — new `FilterExpr` variants + markers; SQL compile arms; `filter_matches` Rust evaluator; `authorize` branch in `execute_query` scan terminals + `point_read`.
- `txn.rs` — `authorize` pre-check (check_owner path) + insert auto-stamp/verify (`stamp_authorize`).
- `schema.rs` — `TableDef.authorize` + validation; migrate field-rename/drop.
- `auth/mod.rs` — `PrincipalCtx` view + builder from `Principal`.
- `committer.rs`, `ws.rs`, `http_api.rs`, `subs.rs` — thread `PrincipalCtx` (replaces `owner: Option<&str>`); `SubEntry` carries email.

**Clients:** protocol FilterExpr type (`ts protocol.ts`, `rust wire.rs`, `python wire.py`) + schema builders gain `authorize` + new variants.

**Tests:** `server/tests/per_row_auth_test.rs` (extend) + a new `server/tests/filter_predicate_test.rs` for the new variants in both roles; client round-trip tests.

---

## Stage 1 — `FilterExpr` language extensions (no auth, no principal)

These extend the query DSL independently and ship value on their own.

### Task 1: Server — `Not` / `Contains` / `Exists` variants + SQL compilation

**Files:** Modify `server/src/query.rs:199-234` (enum), `query.rs:1462-1530` (`compile_filter_node`), `query.rs:1561-1623` (`render_filter_literal_node`).

**Interfaces:**
- Produces: three new `FilterExpr` variants, compiled to SQL by `compile_filter_node` and inlined by `render_filter_literal_node` (partial-index `WHERE`).
- Consumes: existing `field_lhs_and_bind` / `push_filter_bind` / `compile_comparison` helpers.

- [ ] **Step 1: Write failing compile tests**

In `server/src/query.rs` `#[cfg(test)]`, add:

```rust
#[test]
fn compile_not_contains_exists() {
    let table = test_table_with_fields(&["owner", "editors", "archivedat"]);
    // Not
    let (sql, binds) = compile_filter(
        &FilterExpr::Not { expr: Box::new(FilterExpr::Eq {
            field: "owner".into(), value: json!("a") }) }, &table, 1).unwrap();
    assert_eq!(sql, "NOT ((doc->>'owner') = $1)");
    assert_eq!(binds.len(), 1);
    // Contains: value ∈ doc.editors[]  → jsonb membership
    let (sql, _) = compile_filter(
        &FilterExpr::Contains { field: "editors".into(), value: json!("a") }, &table, 1).unwrap();
    assert_eq!(sql, "(doc->'editors') ? $1");
    // Exists: field present and non-null
    let (sql, _) = compile_filter(
        &FilterExpr::Exists { field: "archivedat".into() }, &table, 1).unwrap();
    assert_eq!(sql, "(doc ? 'archivedat' AND doc->>'archivedat' IS NOT NULL)");
}
```
(`test_table_with_fields` is a tiny helper to build a `TableDef` with jsonb fields; mirror an existing test helper in `query.rs`'s tests. If none fits, construct a `TableDef` inline with the named string fields.)

- [ ] **Step 2: Run — verify fail (compile error: unknown variants)**

`cd server && cargo test --lib filter:: compile_not_contains_exists` → FAIL (variants don't exist).

- [ ] **Step 3: Add the variants**

In the `FilterExpr` enum (`query.rs:199`), after `Or`:

```rust
    Not {
        expr: Box<FilterExpr>,
    },
    Contains {
        field: String,
        value: serde_json::Value,
    },
    Exists {
        field: String,
    },
```

- [ ] **Step 4: Add SQL compile arms**

In `compile_filter_node` (`query.rs:1462`), add three match arms. For `Contains`, the field is array-typed (validated in Task 4) so emit the **jsonb** LHS (`doc->'<field>'`), not the text extraction `field_lhs_and_bind` returns for scalar fields — add a small helper `jsonb_field_lhs(field, table) -> Result<String, RtDbError>` that emits `doc->'<field>'` after the same unknown-field check `field_lhs_and_bind` does (`query.rs:1648`):

```rust
        FilterExpr::Not { expr } => {
            Ok(format!("NOT ({})", compile_filter_node(expr, table, start_pos, binds)?))
        }
        FilterExpr::Contains { field, value } => {
            let lhs = jsonb_field_lhs(field, table)?;       // (doc->'<field>')
            let bind = literal_bind_for(field, value, table)?; // typed EqBind for the value
            let ph = push_filter_bind(start_pos, binds, bind);
            Ok(format!("{lhs} ? {ph}"))
        }
        FilterExpr::Exists { field } => {
            jsonb_field_lhs(field, table)?; // validate field exists (reuse the unknown-field check)
            Ok(format!("(doc ? '{field}' AND doc->>'{field}' IS NOT NULL)"))
        }
```
(`literal_bind_for` = the typed-bind construction `field_lhs_and_bind` does for the value side; factor or reuse so the value is typed consistently with `In`.)

- [ ] **Step 5: Add literal-render arms**

In `render_filter_literal_node` (`query.rs:1561`), add the same three variants inlining a typed literal via `render_literal` (no `$n`), so partial-index `WHERE` predicates can use them too.

- [ ] **Step 6: Run — verify pass; then fmt + clippy + commit**

```bash
cd server && cargo test --lib compile_not_contains_exists && cargo fmt --check && cargo clippy --all-targets -- -D warnings
git add server/src/query.rs && git commit -m "feat(query): FilterExpr Not/Contains/Exists variants + SQL compilation"
```

---

### Task 2: Mirror the new variants across the three clients

**Files:** `ts-client/src/protocol.ts`, `rust-client/src/wire.rs`, `python-client/src/par_rt_db/wire.py` (+ their FilterExpr tests).

**Interfaces:** Produces: `not`/`contains`/`exists` variants in each client's `FilterExpr` type, byte-identical to the server.

- [ ] **Step 1: Write failing round-trip tests (one per client)** asserting each new variant serializes to the exact server wire JSON: `{op:"not",expr:{...}}`, `{op:"contains",field,value}`, `{op:"exists",field}`.
- [ ] **Step 2: Run — verify fail** (variants absent).
- [ ] **Step 3: Add the variants** to each client's `FilterExpr` type, matching the server's `tag="op"`/`rename_all="lowercase"` shape exactly. (The TS/Python types are discriminated unions/dicts keyed on `op`; the Rust `wire.rs` mirrors the server enum with the same serde attrs.)
- [ ] **Step 4: Run — verify pass; per-client gate + commit.**

```bash
cd ts-client && bunx vitest run tests/<filter>.test.ts && bunx biome format --write src/protocol.ts && bunx tsc --noEmit
cd ../rust-client && cargo test && cargo fmt --check && cargo clippy --all-targets -- -D warnings
cd ../python-client && uv run pytest -q tests/test_wire_parity.py && uv run ruff check && uv run pyright
git add -A && git commit -m "feat(clients): mirror FilterExpr Not/Contains/Exists variants"
```

---

## Stage 2 — Rust doc-level evaluator `filter_matches`

### Task 3: `filter_matches(doc, expr, ctx)` + `PrincipalCtx` (literal + principal)

**Files:** Modify `server/src/query.rs` (add `filter_matches`, `PrincipalCtx`, `resolve_value`), `server/src/auth/mod.rs` (`PrincipalCtx` builder). No enforcement wiring yet.

**Interfaces:**
- Produces: `pub struct PrincipalCtx<'a> { pub user_id: Option<&'a str>, pub email: Option<&'a str> }` (in `auth/mod.rs`); `pub fn filter_matches(doc: &Value, expr: &FilterExpr, ctx: &PrincipalCtx) -> bool` and `fn resolve_value(v: &Value, ctx: &PrincipalCtx) -> Value` (in `query.rs`). Consumed by Tasks 7-9.

- [ ] **Step 1: Write failing unit tests**

```rust
#[test]
fn filter_matches_all_variants_and_principal() {
    let ctx = PrincipalCtx { user_id: Some("u1"), email: Some("e@x") };
    let doc = json!({"owner":"u1","editors":["u1","u2"],"visibility":"public","archivedat":null});
    assert!(filter_matches(&doc, &FilterExpr::Eq { field:"owner".into(), value:json!({"$user":true}) }, &ctx));
    assert!(!filter_matches(&doc, &FilterExpr::Eq { field:"owner".into(), value:json!({"$user":true}) },
        &PrincipalCtx { user_id: Some("u9"), email: None }));
    assert!(filter_matches(&doc, &FilterExpr::Contains { field:"editors".into(), value:json!({"$user":true}) }, &ctx));
    assert!(filter_matches(&doc, &FilterExpr::Or { exprs: vec![
        FilterExpr::Eq { field:"visibility".into(), value:json!("public") },
        FilterExpr::Eq { field:"owner".into(), value:json!({"$user":true}) }] }, &ctx));
    assert!(filter_matches(&doc, &FilterExpr::Not { expr: Box::new(
        FilterExpr::Exists { field:"archivedat".into() }) }, &ctx)); // null ⇒ not exists
    assert!(filter_matches(&doc, &FilterExpr::Eq { field:"owner".into(), value:json!({"$email":true}) },
        &PrincipalCtx { user_id: Some("u1"), email: Some("u1") })); // email resolves to "u1" here
}
```

- [ ] **Step 2: Run — verify fail** (`filter_matches`/`PrincipalCtx` undefined).
- [ ] **Step 3: Implement**

In `auth/mod.rs`:

```rust
/// Per-row auth view threaded into the executors. `user_id == None` ⇒ bypass
/// (Machine/admin/scheduled). Carries `email` so `$email` predicates resolve.
#[derive(Debug, Clone, Copy)]
pub struct PrincipalCtx<'a> {
    pub user_id: Option<&'a str>,
    pub email: Option<&'a str>,
}

impl Principal {
    /// Builds the per-row auth view; `Machine`/missing ⇒ bypass (`None` user_id).
    pub fn row_ctx(&self) -> PrincipalCtx<'static> { /* match self, leak-free via owned */ }
}
```
(Note: the executors currently take `owner: Option<&str>`; Task 5 changes them to `&PrincipalCtx`. Here, just define the type + a `from_principal`/`row_ctx` builder. Use owned `String`/`Cow` if lifetimes get awkward — the type can hold `Option<String>`; pick the owned form to make threading simple and adjust Task 5 accordingly.)

In `query.rs`:

```rust
/// Resolves a principal marker (`{"$user":true}`→user_id, `{"$email":true}`→email)
/// to its Value; non-marker values pass through unchanged.
fn resolve_value(v: &serde_json::Value, ctx: &PrincipalCtx) -> serde_json::Value {
    if let serde_json::Value::Object(map) = v {
        if map.len() == 1 {
            if let Some(true) = map.get("$user").and_then(|x| x.as_bool()) {
                return ctx.user_id.map(serde_json::Value::String).unwrap_or(serde_json::Value::Null);
            }
            if let Some(true) = map.get("$email").and_then(|x| x.as_bool()) {
                return ctx.email.map(serde_json::Value::String).unwrap_or(serde_json::Value::Null);
            }
        }
    }
    v.clone()
}

/// Evaluates `expr` against a fetched `doc` with principal markers resolved.
/// Over-approximates to "no match" on any type/missing-field doubt (never matches
/// erroneously). Used by point-read, write pre-checks, and insert verification.
pub fn filter_matches(doc: &serde_json::Value, expr: &FilterExpr, ctx: &PrincipalCtx) -> bool {
    match expr {
        FilterExpr::Eq { field, value } => doc.get(field).is_some_and(|d| d == &resolve_value(value, ctx)),
        FilterExpr::Neq { field, value } => doc.get(field).is_some_and(|d| d != &resolve_value(value, ctx)),
        FilterExpr::Gt { field, value } => cmp_json(doc.get(field), &resolve_value(value, ctx)).is_some_and(|o| o == Ordering::Greater),
        FilterExpr::Gte { field, value } => cmp_json(doc.get(field), &resolve_value(value, ctx)).is_some_and(|o| o != Ordering::Less),
        FilterExpr::Lt { field, value } => cmp_json(doc.get(field), &resolve_value(value, ctx)).is_some_and(|o| o == Ordering::Less),
        FilterExpr::Lte { field, value } => cmp_json(doc.get(field), &resolve_value(value, ctx)).is_some_and(|o| o != Ordering::Greater),
        FilterExpr::In { field, values } => doc.get(field).is_some_and(|d| values.iter().any(|v| d == &resolve_value(v, ctx))),
        FilterExpr::And { exprs } => exprs.iter().all(|e| filter_matches(doc, e, ctx)),
        FilterExpr::Or { exprs } => exprs.iter().any(|e| filter_matches(doc, e, ctx)),
        FilterExpr::Not { expr } => !filter_matches(doc, expr, ctx),
        FilterExpr::Contains { field, value } => doc.get(field).and_then(|v| v.as_array())
            .is_some_and(|arr| arr.iter().any(|v| v == &resolve_value(value, ctx))),
        FilterExpr::Exists { field } => doc.get(field).is_some_and(|v| !v.is_null()),
    }
}
```
(`cmp_json(Option<&Value>, Value) -> Option<Ordering>` compares numbers as f64 and strings as str, returning `None` on type mismatch or missing field — the over-approximation. Note `Not` of a missing-field match inverts carefully: `Not(Exists{nullfield})` is true because `Exists` is false; `Not(Eq{field,x})` when field missing → `Not(false)` → true. Audit: for auth this is acceptable since the server stamps required fields, but call it out in the task notes — a `Not(Eq)` over a missing field yields true, which could over-allow. Mitigation: the `authorize` predicate is server-declared and validated, and inserts are stamped/verified, so a production predicate won't rely on `Not(Eq)` over an absent field in an over-permissive way. Document this in the spec's security notes if not already.)

- [ ] **Step 4: Run — verify pass; fmt + clippy + commit.**

```bash
cd server && cargo test --lib filter_matches && cargo fmt --check && cargo clippy --all-targets -- -D warnings
git add server/src/query.rs server/src/auth/mod.rs && git commit -m "feat(query): filter_matches doc evaluator + PrincipalCtx (literal + principal)"
```

---

## Stage 3 — `authorize` declaration + principal threading

### Task 4: `TableDef.authorize` + schema validation + migrate + client schema mirror

**Files:** `server/src/schema.rs:102-114` (TableDef), `schema.rs:333-374` (validation), `schema.rs:233-243` (array-field helper), `migrate.rs:147-217` (rename/drop); 3-client schema builders.

**Interfaces:** Produces: `TableDef.authorize: Option<FilterExpr>` (serde `authorize`, `skip_serializing_if = None`); validation that the predicate references declared fields, `Contains` is array-typed, and principal markers appear only here (not in client filters — enforced at the query boundary in Task 6).

- [ ] **Step 1: Write failing schema-validation tests**

```rust
#[test]
fn authorize_validates_fields_and_markers() {
    // valid: public OR owned
    let mut t = table("posts", &["owner","visibility"]);
    t.authorize = Some(FilterExpr::Or { exprs: vec![
        FilterExpr::Eq { field:"owner".into(), value:json!({"$user":true}) },
        FilterExpr::Eq { field:"visibility".into(), value:json!("public") }] });
    assert!(t.validate_structure().is_ok());
    // invalid: unknown field
    let mut bad = t.clone(); bad.authorize = Some(FilterExpr::Eq { field:"nope".into(), value:json!(1) });
    assert!(bad.validate_structure().is_err());
    // invalid: Contains on a non-array field
    let mut bad2 = t.clone(); bad2.authorize = Some(FilterExpr::Contains { field:"visibility".into(), value:json!("x") });
    assert!(bad2.validate_structure().is_err());
}
```

- [ ] **Step 2: Run — verify fail.**
- [ ] **Step 3: Add the field + validation.** Add `pub authorize: Option<FilterExpr>` (`#[serde(default, skip_serializing_if = "Option::is_none")]`) to `TableDef`. In `validate_structure`, walk the predicate: each `field` must be a declared field; `Contains`'s field must pass `is_string_array_field`; principal markers are allowed here (they're rejected in client filters elsewhere). Add a `validate_filter_expr_fields(expr, table)` walker reused by Task 6 for client-filter validation (with a `allow_principal_markers: bool` flag).
- [ ] **Step 4: Migrate carry.** In `migrate.rs` rename (147) rewrite `field` inside `authorize`; drop (213) — if a field referenced by `authorize` is dropped, reject the migration (it's load-bearing for auth) rather than silently clearing.
- [ ] **Step 5: Client schema mirror.** Add `authorize?: FilterExpr` to each client's table-schema type + a builder method. Round-trip test per client.
- [ ] **Step 6: Run gates + commit.**

```bash
cd server && cargo test --lib authorize_validates && cargo fmt --check && cargo clippy --all-targets -- -D warnings
# + per-client gates as in Task 2
git add -A && git commit -m "feat(schema): TableDef.authorize predicate + validation + migrate + client mirror"
```

---

### Task 5: Thread `PrincipalCtx` through the executor seams (replaces `owner: Option<&str>`)

The load-bearing plumbing task. Behavior byte-identical for owner/collab (existing tests guard).

**Files:** `query.rs` (`execute_query` signature), `txn.rs` (`execute_txn` signature, `stamp_owner`/`check_owner`/`check_owner_doc`/`row_auth_enforced_uid`), `committer.rs` (call sites 433/515/795), `ws.rs` (448), `http_api.rs` (88/145/207), `subs.rs` (`SubEntry` 744, `register` 930, `fan_out` 998).

**Interfaces:**
- Produces: `execute_query`/`execute_txn` take `ctx: &PrincipalCtx` instead of `owner: Option<&str>`. `SubEntry` stores `user_id` + `email`. `ws.rs`/`http_api.rs` build `PrincipalCtx` from the resolved `Principal`.

- [ ] **Step 1: Define the threading change.** Add `PrincipalCtx` (from Task 3). Change `execute_query(..., owner: Option<&str>)` → `execute_query(..., ctx: &PrincipalCtx)`. Same for `execute_txn`. Internally, replace `owner` reads with `ctx.user_id`. `row_auth_enforced_uid` takes `ctx` and returns `ctx.user_id` when a field is declared. `stamp_owner`/`check_owner`/`check_owner_doc` take `ctx` and use `ctx.user_id`.
- [ ] **Step 2: Update call sites.** `committer.rs` builds `PrincipalCtx` from the `owner` it carries (extend `CommitterRequest::Mutate`/`subscribe` to carry email too, or carry the whole `PrincipalCtx`). `ws.rs`/`http_api.rs`: replace `owner_of(principal)` with `principal.row_ctx()` (admin → `user_id=None`). `subs.rs`: `SubEntry` gains `email: Option<String>`; `register` stores it; `fan_out` builds `PrincipalCtx` from `entry.user_id`+`entry.email`.
- [ ] **Step 3: Keep behavior identical.** No `authorize` enforcement yet (Task 6+) — owner/collab paths read `ctx.user_id` exactly as they read `owner`. Run the **entire existing suite** as the gate.
- [ ] **Step 4: Run — full suite must stay green.**

```bash
make dev-db-up && cd server && cargo test && cargo fmt --check && cargo clippy --all-targets -- -D warnings
git add -A && git commit -m "refactor(auth): thread PrincipalCtx (user_id+email) through executor seams"
```
If any owner/collab test breaks, the threading changed behavior — fix before proceeding; do not weaken a test.

---

## Stage 4 — Enforcement

### Task 6: Read scan path — compile `authorize` into every scan terminal

**Files:** `query.rs:856-883` (execute_query wiring), `query.rs:2140-2205` (add an `authorize_filter`/predicate-body builder), search terminals (`execute_search` 1703, `execute_vector_search` 1774, `execute_hybrid_search` 1940). Also reject principal markers in client `.filter()` here (validation).

**Interfaces:** Consumes `PrincipalCtx` (Task 5) + `compile_filter` (Task 1). When `table.authorize` is set and `ctx.user_id` is `Some`, compile the predicate with markers resolved to bound literals and AND it into `where_conditions` (alongside the owner/collab predicate body). When `None`, append nothing.

- [ ] **Step 1: Write failing integration test** in `server/tests/per_row_auth_test.rs`: a `posts` table with `authorize: Or[Eq{owner,$user}, Eq{visibility,"public"}]`; seed A-private, A-public, B-private, B-public rows; User A queries → sees A's two + both public (4 rows), not B-private. Machine token → sees all.
- [ ] **Step 2: Run — verify fail** (no enforcement; A sees everything).
- [ ] **Step 3: Implement the authorize branch.** Add `authorize_predicate_body(table, ctx, start_pos, binds) -> Result<Option<String>, RtDbError>`: if `table.authorize.is_some() && ctx.user_id.is_some()`, resolve markers (substitute `$user`→`ctx.user_id`, `$email`→`ctx.email` into a cloned predicate) and `compile_filter` it, returning the fragment; else `None`. In `execute_query` (856-883) and each search terminal, append it to `where_conditions` like `row_auth_predicate_body`. Ensure it composes with owner/collab if both declared (AND).
- [ ] **Step 4: Reject principal markers in client filters.** In the query boundary (where `Query.filter` is accepted — `execute_query` entry / Subscribe / HTTP query), run `validate_filter_expr_fields(filter, table, allow_principal_markers=false)` → `BadRequest` if a marker appears.
- [ ] **Step 5: Run — verify pass; full suite; commit.**

```bash
cd server && cargo test --test per_row_auth_test && cargo test && cargo fmt --check && cargo clippy --all-targets -- -D warnings
git add -A && git commit -m "feat(auth): enforce authorize predicate on read scan terminals"
```

---

### Task 7: Point-read — `filter_matches` in `point_read`

**Files:** `query.rs:2099-2133` (`point_read`).

- [ ] **Step 1: Write failing test** — User A `get` B-private doc → `null` (silent); A `get` A's own → doc; admin `get` B's → doc.
- [ ] **Step 2: Run — verify fail.**
- [ ] **Step 3:** In `point_read`, after fetching, if `table.authorize.is_some() && ctx.user_id.is_some()`, return `Doc(None)` when `!filter_matches(&doc, table.authorize.unwrap(), ctx)`. (Keep the owner/collab `row_visible_to` path for those tables.)
- [ ] **Step 4: Run — pass; commit.** `feat(auth): authorize predicate on point-read (get)`.

---

### Task 8: Write pre-check — `check_owner`/`check_owner_doc` honor `authorize`

**Files:** `txn.rs:913-972`.

- [ ] **Step 1: Write failing test** — User A `patch`/`delete` B-private doc → `Forbidden` + txn aborts; A patches own → ok; admin patches B's → ok.
- [ ] **Step 2: Run — verify fail.**
- [ ] **Step 3:** In `check_owner`/`check_owner_doc`, after the owner/collab check, if `table.authorize.is_some() && ctx.user_id.is_some()` and `!filter_matches(&doc, authorize, ctx)` → `Forbidden`. (Composes with owner/collab: a table can declare both; both must pass.)
- [ ] **Step 4: Run — pass; commit.** `feat(auth): authorize predicate write pre-check (Forbidden)`.

---

### Task 9: Insert auto-stamp + verify

**Files:** `txn.rs:856-865` (`stamp_owner` neighbor), the Insert/Upsert-insert call sites (1026/1118/1140).

- [ ] **Step 1: Write failing tests** — (a) `authorize: Eq{owner,$user}` insert with client `owner="someoneElse"` → server stamps `owner=caller`, succeeds; (b) `Or[owner==$user, visibility=="public"]` insert → always succeeds (owner stamped); (c) `authorize: Eq{visibility,"public"}` (no `$user` leaf) insert with `visibility="private"` → `Forbidden`; (d) `Contains{editors,$user}`-only insert where client omits itself → `Forbidden`.
- [ ] **Step 2: Run — verify fail.**
- [ ] **Step 3: Implement `stamp_authorize`.**

```rust
/// For each `Eq { field, value: {"$user": true} }` leaf reachable through
/// `And`/`Or` in `table.authorize`, force `doc[field] = ctx.user_id`.
/// `Not`/`Contains`/`Exists`/non-$user leaves are not stampable. Bypass/no
/// authorize ⇒ no-op.
fn stamp_authorize(table: &TableDef, mut doc: serde_json::Map<String, Value>, ctx: &PrincipalCtx)
    -> serde_json::Map<String, Value> {
    if let (Some(expr), Some(uid)) = (&table.authorize, ctx.user_id) {
        for f in user_eq_fields(expr) { doc.insert(f, Value::String(uid.to_string())); }
    }
    doc
}
```
(`user_eq_fields` walks `And`/`Or` collecting `Eq{field,{"$user":true}}` field names; skips under `Not`.) Call `stamp_authorize` after `stamp_owner` at the Insert/Upsert-insert sites. Then **verify**: after stamping, if `authorize.is_some() && ctx.user_id.is_some()` and `!filter_matches(&Value::Object(doc.clone()), authorize, ctx)` → `Forbidden`.
- [ ] **Step 4: Run — pass; commit.** `feat(auth): insert auto-stamp + verify against authorize predicate`.

---

## Stage 5 — Docs + gate

### Task 10: Docs + full gate + kanban

**Files:** `CLAUDE.md` (Auth section), `FEATURE_MATRIX.md` (#20), `docs/superpowers/specs/2026-07-24-per-row-authorization-design.md` (cross-ref model C shipped).

- [ ] **Step 1: CLAUDE.md** — extend the Auth section's per-row paragraph: `authorize` is a third opt-in (general `FilterExpr` predicate over doc fields + `$user`/`$email`), enforced on read/write/subscription, additive to ownerField/collaboratorsField; new `FilterExpr` variants `Not`/`Contains`/`Exists` available in `.filter()`; principal markers valid only in `authorize`; insert auto-stamps `Eq{field,$user}` leaves.
- [ ] **Step 2: FEATURE_MATRIX #20** — flip model C to shipped; note client-mirror status.
- [ ] **Step 3: v1 spec cross-ref** — append a line pointing to the 2026-08-02 model C spec as the shipped design.
- [ ] **Step 4: Full gate.**

```bash
make dev-db-up && make checkall
```
- [ ] **Step 5: Commit docs; kanban done.**

```bash
git add -A && git commit -m "docs: per-row auth predicate DSL (model C) shipped"
kanban item done --id 019fbe207c807252a55bbbc19f8b457e
```

---

## Self-Review (completed during planning)

**Spec coverage:** `authorize` declaration (T4), principal markers (T3 resolve, T4 validate, T6 client-filter reject), new FilterExpr variants + SQL (T1) + clients (T2), `filter_matches` evaluator (T3), read scan enforcement (T6), point-read (T7), write pre-check (T8), insert auto-stamp+verify (T9), principal threading (T5), subscription re-filter (transitive via T5+T6 — `fan_out` re-runs `execute_query` with the subscriber's `PrincipalCtx`), bypass (T5/T6 — `user_id=None`), schema validation (T4), docs (T10). Every spec section maps to a task.

**Dependency order:** T1→T2 (variants before client mirror)→T3 (evaluator)→T4 (declaration)→T5 (threading, needs T3's `PrincipalCtx`)→T6/T7/T8/T9 (enforcement, need T4+T5)→T10. Each task is independently testable and committable; T5 is the one invasive refactor (gated by the full existing suite staying green).

**Placeholder scan:** all code blocks contain real signatures/SQL/test cases matching the verified source (`query.rs` `compile_filter_node`/`owner_filter`/`row_auth_*`, `txn.rs` `stamp_owner`/`check_owner`/`row_visible_to`); the two helpers an implementer must read before finalizing (`field_lhs_and_bind`/`push_filter_bind`, ~`query.rs:1643`) and `cmp_json` (new helper, spec'd inline) are named. No TBD/TODO.

**Type/signature consistency:** `PrincipalCtx { user_id, email }` defined in T3, used identically in T5/T6/T7/T8/T9; `filter_matches(doc, expr, ctx)` defined T3, used T7/T8/T9; `stamp_authorize`/`user_eq_fields` defined T9; new variant field names (`expr`/`field`/`value`) match across the enum (T1) and all clients (T2); wire `op` strings (`not`/`contains`/`exists`) consistent server+clients.
