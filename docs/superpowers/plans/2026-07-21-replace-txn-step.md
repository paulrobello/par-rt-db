# Replace Transaction Step Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `replace` transaction step (FEATURE_MATRIX.md rank 6) — a full-document overwrite that validates the complete document against the schema, recomputes every indexed `f_<field>` column, and bumps `version`, keeping the same `_id`. Mirrored end-to-end in the Rust server and the TypeScript client SDK.

**Architecture:** `replace` sits alongside the existing `Step` variants in `server/src/txn.rs` (`Insert`, `Patch`, `Delete`, `ExpectVersion`, `ExpectAbsent`, `Upsert`). Unlike `Patch` (merges `fields` onto the existing doc via `apply_patch`), `Replace` takes a complete `doc` and validates it the same way `Insert` does (`validate_doc` + `strip_unset_optionals`), then reuses the existing `apply_update` helper (already shared by `Patch` and `Upsert`) to recompute indexed columns and bump `version`. The step-result shape is `null` (same as `patch`/`delete`) since the row already exists and no new id is generated.

**Tech Stack:** Rust (axum/sqlx/Postgres) server in `server/`; TypeScript client SDK in `client/`.

## Global Constraints

- Wire tags/fields are `camelCase` and must stay byte-identical between `server/src/txn.rs`'s `Step` serde shape and `client/src/protocol.ts`'s `StepJson` — this is the wire coupling documented in the root `CLAUDE.md`.
- Step-result shape contract (root `CLAUDE.md`): `insert` → `{id}`, `upsert` → `{id, inserted}`, `patch`/`delete`/`expectVersion`/`expectAbsent` → `null`. `replace` joins the `null`-result group.
- No `unwrap()`/`expect()` outside `#[cfg(test)]`. Zero clippy warnings under `-D warnings`.
- Every failure is the `RtDbError` envelope; a missing id is `NOT_FOUND`, an invalid full document is `SCHEMA_VIOLATION`.
- `make checkall` (fmt-check + clippy `-D warnings` + typecheck + tests, both packages) is the project's definition of done and must be run from the repo root.
- `server/src/protocol.rs` needs **no code change**: `Step`'s own `#[serde(tag = "op", rename_all = "camelCase")]` derive already flows through `Transaction`/`ClientMessage::Mutate` generically. Confirmed via `grep -rn "Step::" server/src/` that no file outside `txn.rs` exhaustively matches `Step` variants.

---

### Task 1: Server — `Step::Replace` + integration tests

**Files:**
- Modify: `server/src/txn.rs` (Step enum ~line 16-47; add `do_replace` near `do_patch` at line 351-376; add match arm in `execute_txn` near line 501-506)
- Test: `server/tests/txn_test.rs` (add 4 tests after `patch_unknown_field_is_schema_violation`, line 314)

**Interfaces:**
- Produces: `Step::Replace { table: String, id: String, doc: serde_json::Map<String, serde_json::Value> }` — wire tag `"replace"`. Result: `serde_json::Value::Null` pushed to `TxnOutcome.results`, table added to `write_set`.

- [ ] **Step 1: Write the failing tests**

Add to `server/tests/txn_test.rs`, immediately after `patch_unknown_field_is_schema_violation` (after line 314, before the `// (e) delete.` comment):

```rust
// (d2) replace fully overwrites doc, recomputes every typed column, bumps version.
#[tokio::test]
async fn replace_overwrites_doc_updates_typed_columns_and_bumps_version() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let insert_outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "projects".to_string(),
                doc: valid_project_doc(),
            }],
        },
    )
    .await?;
    let id = insert_outcome.results[0]["id"]
        .as_str()
        .expect("id")
        .to_string();

    let replacement = doc(serde_json::json!({
        "name": "Beta",
        "description": "new description",
        "status": "paused",
        "tags": ["z"],
        "updatedAt": 9.0
    }));

    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Replace {
                table: "projects".to_string(),
                id: id.clone(),
                doc: replacement,
            }],
        },
    )
    .await?;
    assert_eq!(outcome.results, vec![serde_json::Value::Null]);
    assert_eq!(outcome.write_set, BTreeSet::from(["projects".to_string()]));

    let pg_schema = format!("db_{db}");
    let row: (String, String, i64, serde_json::Value) = sqlx::query_as(&format!(
        "SELECT \"f_name\", \"f_status\", \"version\", \"doc\" FROM \"{pg_schema}\".\"t_projects\" WHERE \"id\" = $1"
    ))
    .bind(&id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(row.0, "Beta");
    assert_eq!(row.1, "paused");
    assert_eq!(row.2, 2);
    assert_eq!(row.3["description"], serde_json::json!("new description"));

    Ok(())
}

// (d3) replace on a missing id -> NotFound (404).
#[tokio::test]
async fn replace_missing_id_returns_not_found() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Replace {
                table: "projects".to_string(),
                id: "0".repeat(32),
                doc: valid_project_doc(),
            }],
        },
    )
    .await
    .expect_err("expected not found");
    assert_eq!(err.code, ErrorCode::NotFound);

    Ok(())
}

// (d4) replace with a doc violating the schema -> SchemaViolation (422).
#[tokio::test]
async fn replace_schema_violation_is_rejected() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let insert_outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "projects".to_string(),
                doc: valid_project_doc(),
            }],
        },
    )
    .await?;
    let id = insert_outcome.results[0]["id"]
        .as_str()
        .expect("id")
        .to_string();

    let bad_doc = doc(serde_json::json!({
        "name": "Beta",
        "description": null,
        "status": "not-a-valid-status",
        "tags": ["z"],
        "updatedAt": 9.0
    }));

    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Replace {
                table: "projects".to_string(),
                id,
                doc: bad_doc,
            }],
        },
    )
    .await
    .expect_err("expected schema violation");
    assert_eq!(err.code, ErrorCode::SchemaViolation);

    Ok(())
}

// (d5) replace inside a multi-step txn rolled back by a later failed step.
#[tokio::test]
async fn replace_rolled_back_by_later_failed_step() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let insert_outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "projects".to_string(),
                doc: valid_project_doc(),
            }],
        },
    )
    .await?;
    let id = insert_outcome.results[0]["id"]
        .as_str()
        .expect("id")
        .to_string();

    let replacement = doc(serde_json::json!({
        "name": "Beta",
        "description": null,
        "status": "paused",
        "tags": ["z"],
        "updatedAt": 9.0
    }));

    let txn = Transaction {
        steps: vec![
            Step::Replace {
                table: "projects".to_string(),
                id: id.clone(),
                doc: replacement,
            },
            Step::Delete {
                table: "projects".to_string(),
                id: "0".repeat(32),
            },
        ],
    };

    let result = execute_txn(&pool, &db, &schema, &txn).await;
    assert!(result.is_err());

    let pg_schema = format!("db_{db}");
    let row: (String, i64) = sqlx::query_as(&format!(
        "SELECT \"f_name\", \"version\" FROM \"{pg_schema}\".\"t_projects\" WHERE \"id\" = $1"
    ))
    .bind(&id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(row.0, "Alpha");
    assert_eq!(row.1, 1);

    Ok(())
}
```

- [ ] **Step 2: Run the new tests to verify they fail to compile**

Run: `cd server && make dev-db-up 2>/dev/null; cargo test --test txn_test replace_ -- --list` (from repo root: `cd ~/Repos/par-rt-db && make dev-db-up && cd server && cargo test --test txn_test replace_ -- --list`)
Expected: compile error — `no variant named 'Replace' found for enum 'Step'`.

- [ ] **Step 3: Add the `Step::Replace` variant**

In `server/src/txn.rs`, in the `Step` enum (starts line 16), insert after the `Patch` variant (after line 25's closing `},`) and before `Delete`:

```rust
    Replace {
        table: String,
        id: String,
        doc: serde_json::Map<String, serde_json::Value>,
    },
```

- [ ] **Step 4: Add `do_replace`**

In `server/src/txn.rs`, immediately after `do_patch` (after its closing `}` at line 376) and before `do_delete`:

```rust
/// Fetches to confirm the row exists (`NotFound` if missing), then fully
/// replaces its `doc` with `new_doc` — validated as a complete document (like
/// `Insert`), not merged like `Patch` — recomputing every indexed column and
/// bumping `version` via the shared `apply_update`.
async fn do_replace(
    conn: &mut PgConnection,
    pg_schema_name: &str,
    table_def: &TableDef,
    table_name: &str,
    id: &str,
    new_doc: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), RtDbError> {
    let table_ident = pg_table(table_name);
    let row: Option<(String,)> = sqlx::query_as(&format!(
        "SELECT \"id\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE \"id\" = $1"
    ))
    .bind(id)
    .fetch_optional(&mut *conn)
    .await?;
    row.ok_or_else(|| RtDbError::not_found(format!("document '{id}' not found")))?;

    validate_doc(table_def, new_doc)?;
    let doc = strip_unset_optionals(table_def, new_doc.clone());
    apply_update(conn, pg_schema_name, table_def, table_name, id, doc).await
}
```

- [ ] **Step 5: Wire the match arm in `execute_txn`**

In `server/src/txn.rs`, in `execute_txn`'s `match step` block, insert after the `Step::Patch { .. }` arm (after line 506's closing `}`) and before `Step::Delete`:

```rust
            Step::Replace { table, id, doc } => {
                let table_def = schema.table(table)?;
                do_replace(&mut tx, &pg_schema_name, table_def, table, id, doc).await?;
                write_set.insert(table.clone());
                results.push(serde_json::Value::Null);
            }
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cd ~/Repos/par-rt-db/server && cargo test --test txn_test replace_`
Expected: 4 tests PASS (`replace_overwrites_doc_updates_typed_columns_and_bumps_version`, `replace_missing_id_returns_not_found`, `replace_schema_violation_is_rejected`, `replace_rolled_back_by_later_failed_step`).

- [ ] **Step 7: Commit**

```bash
cd ~/Repos/par-rt-db
git add server/src/txn.rs server/tests/txn_test.rs
git commit -m "feat(server): add replace transaction step"
```

---

### Task 2: Client SDK — `.replace()` step + FEATURE_MATRIX.md

**Files:**
- Modify: `client/src/protocol.ts` (`StepJson` union, line 22-36)
- Modify: `client/src/mutation.ts` (`TxnBuilder`, add method after `patch`, line 12-15)
- Test: `client/tests/mutation.test.ts` (extend the existing combined-steps test, line 5-37)
- Modify: `FEATURE_MATRIX.md` (row 6, line 58)

**Interfaces:**
- Consumes: server wire tag `"replace"` with fields `table`, `id`, `doc` (Task 1).
- Produces: `StepJson` variant `{ op: "replace"; table: string; id: string; doc: Record<string, unknown> }`; `TxnBuilder.replace(table, id, doc): this`.

- [ ] **Step 1: Write the failing test**

In `client/tests/mutation.test.ts`, replace the first `it` block (lines 5-37) with:

```ts
  it("builds an ordered multi-step txn with table on every step", () => {
    const txn = mutation()
      .insert("items", { projectId: "p1", title: "a" })
      .patch("items", "i1", { title: "b" })
      .replace("items", "i4", { projectId: "p1", title: "c" })
      .delete("items", "i2")
      .expectVersion("items", "i3", 7)
      .expectAbsent("items", "by_project_and_title", ["p1", "dup"])
      .upsert("items", {
        index: "by_project_and_title",
        eq: ["p1", "x"],
        insert: { projectId: "p1", title: "x" },
        patch: { title: "x2" },
      })
      .build();

    expect(txn).toEqual({
      steps: [
        { op: "insert", table: "items", doc: { projectId: "p1", title: "a" } },
        { op: "patch", table: "items", id: "i1", fields: { title: "b" } },
        { op: "replace", table: "items", id: "i4", doc: { projectId: "p1", title: "c" } },
        { op: "delete", table: "items", id: "i2" },
        { op: "expectVersion", table: "items", id: "i3", version: 7 },
        { op: "expectAbsent", table: "items", index: "by_project_and_title", eq: ["p1", "dup"] },
        {
          op: "upsert",
          table: "items",
          index: "by_project_and_title",
          eq: ["p1", "x"],
          insert: { projectId: "p1", title: "x" },
          patch: { title: "x2" },
        },
      ],
    });
  });
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd ~/Repos/par-rt-db/client && bunx vitest run tests/mutation.test.ts`
Expected: FAIL — `mutation(...).replace is not a function` (TxnBuilder has no `replace` method yet), and a TS type error on `StepJson` not having an `"replace"` op.

- [ ] **Step 3: Add `StepJson` variant**

In `client/src/protocol.ts`, in the `StepJson` union (starts line 23), insert after the `"patch"` variant (after line 25) and before `"delete"`:

```ts
  | { op: "replace"; table: string; id: string; doc: Record<string, unknown> }
```

- [ ] **Step 4: Add `TxnBuilder.replace()`**

In `client/src/mutation.ts`, insert after the `patch` method (after its closing `}` at line 15) and before `delete`:

```ts
  replace(table: string, id: string, doc: Record<string, unknown>): this {
    this.steps.push({ op: "replace", table, id, doc });
    return this;
  }
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd ~/Repos/par-rt-db/client && bunx vitest run tests/mutation.test.ts`
Expected: PASS.

- [ ] **Step 6: Update `FEATURE_MATRIX.md` row 6**

In `FEATURE_MATRIX.md`, replace line 58:

```
| 6 | 1 | **`replace`** step (full-document overwrite) | ✅ | ❌ | Med | S | Like `patch` but validates the complete doc and rewrites all indexed columns. Straightforward `Step` variant in `txn.rs`. |
```

with:

```
| 6 | 1 | **`replace`** step (full-document overwrite) | ✅ | ✅ | Med | S | Implemented — a `Step::Replace { table, id, doc }` variant in `txn.rs`: like `Insert`, the full document is validated against the schema and every indexed `f_<field>` column is recomputed from it (not merged like `Patch`), plus the row's `version` is bumped; `NotFound` if `id` doesn't exist. Mirrored end-to-end: `protocol.ts` wire shape and `TxnBuilder.replace()` in the TS client, with integration coverage in `txn_test.rs` and builder-shape coverage in `mutation.test.ts`. |
```

- [ ] **Step 7: Commit**

```bash
cd ~/Repos/par-rt-db
git add client/src/protocol.ts client/src/mutation.ts client/tests/mutation.test.ts FEATURE_MATRIX.md
git commit -m "feat(client): add .replace() transaction step, mark FEATURE_MATRIX rank 6 implemented"
```

---

### Task 3: Full verification

**Files:** none (verification only)

- [ ] **Step 1: Run the full gate from the repo root**

Run: `cd ~/Repos/par-rt-db && make checkall`
Expected: PASS — fmt-check, clippy `-D warnings`, typecheck, and the full test suite (both `server/` and `client/`) all green.

- [ ] **Step 2: Fix any failures**

If `make checkall` fails for a reason connected to this change, fix it and re-run Step 1 until green. If blocked solely by another already-running dev-db bound to the same port, document the conflict and verify equivalently (`cargo test` / `bunx vitest run` directly against the already-running instance) instead of claiming `make checkall` passed.

---

## Execution Handoff Note

Per standing project instructions, execute this plan via `superpowers:subagent-driven-development` — Subagent-Driven, no confirmation needed. After Task 3 is green, rebase `feat/replace-txn-step` onto `main` and fast-forward merge locally (no push), then delete the branch — per the original task instructions, not a separate plan task.
