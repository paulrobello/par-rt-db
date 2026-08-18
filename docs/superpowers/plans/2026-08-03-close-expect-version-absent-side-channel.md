# Close the ExpectVersion/ExpectAbsent Per-Row-Auth Side-Channel — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `ExpectVersion` and `ExpectAbsent` precondition steps honor per-row visibility (`ownerField` / `collaboratorsField` / `authorize`) so a user can no longer use them as an existence/version oracle over other users' documents.

**Architecture:** Add one helper, `doc_visible_to(doc, table_def, ctx) -> bool`, as the boolean twin of the existing write-path `check_owner_doc` (same gate composition). Thread it into `do_expect_version` and the `ExpectAbsent` dispatch arm. A non-visible document is mapped to the *absent* outcome (`NotFound` for `ExpectVersion`, success for `ExpectAbsent`) — indistinguishable from a genuinely-missing doc, collapsing the oracle without a louder `Forbidden`. No wire/protocol change; no client mirror.

**Tech Stack:** Rust (axum/tokio), sqlx, Postgres 17. Tests: `tokio::test` integration tests against a real Postgres.

## Global Constraints

- **Server-only.** No changes to `protocol.rs`, `query.rs`, `subs.rs`, any client (`ts-client`/`rust-client`/`python-client`), or the dashboard. The two steps already exist on the wire.
- **No `unwrap()`/`expect()` outside `#[cfg(test)]`.** Zero clippy warnings under `-D warnings`.
- **Oracle closure must be byte-identical:** the non-visible outcome for `ExpectVersion` uses the *same* `RtDbError::not_found(format!("document '{id}' not found"))` constructor and message as the genuinely-absent path (do not invent a new message).
- **`doc_visible_to` must stay in lockstep with `check_owner_doc`.** Any future per-row gate is added in both places — note this in a code comment.
- **Integration tests require the dev Postgres** on `127.0.0.1:55434: run `make dev-db-up` first.
- **Definition of done:** `make dev-db-up` then `make checkall` green (fmt-check + clippy `-D warnings` + typecheck + tests).

## File Structure

- **`server/src/txn.rs`** (modify) — the only source file touched.
  - New `fn doc_visible_to(...)` placed immediately after `check_owner_doc` (~line 1097).
  - `do_expect_version` (~line 784): add `table_def: &TableDef` and `ctx: &PrincipalCtx` params; `SELECT` the `doc` column too; add the visibility→`NotFound` branch.
  - `ExpectVersion` dispatch arm (~line 1223): resolve `table_def` (bind the currently-discarded `schema.table(table)?` result) and pass `ctx`.
  - `ExpectAbsent` dispatch arm (~line 1228): filter `eq_lookup` rows through `doc_visible_to` before deciding "present".
- **`server/tests/per_row_auth_test.rs`** (modify) — new integration tests, reusing the existing `setup()`/`setup_collab()`/`setup_authorize()` helpers and `seed_note`/`seed_collab_note`/`seed_post`.
- **Docs** (Task 3): `FEATURE_MATRIX.md` row #20 + the two per-row-auth specs.

Existing symbols this plan consumes (already in `txn.rs`, in scope): `row_auth_enforced_uid`, `row_visible_to`, `filter_matches`, `RtDbError::{not_found, precondition}`, `TableDef`, `PrincipalCtx`, `pg_table`. `execute_txn` already receives `ctx: &PrincipalCtx` and it is in scope at both dispatch arms.

---

### Task 1: Close the `ExpectVersion` oracle (+ introduce `doc_visible_to`)

**Files:**
- Modify: `server/src/txn.rs` — add `doc_visible_to`; rewrite `do_expect_version`; update the `ExpectVersion` dispatch arm.
- Test: `server/tests/per_row_auth_test.rs` — add 4 integration tests.

**Interfaces:**
- Consumes: `check_owner_doc`'s gate-composition pattern (mirror it), the existing `row_auth_enforced_uid`/`row_visible_to`/`filter_matches` helpers.
- Produces: `fn doc_visible_to(doc: &serde_json::Value, table_def: &TableDef, ctx: &PrincipalCtx) -> bool` — consumed by Task 2.

- [ ] **Step 1: Write the failing tests** — append to `server/tests/per_row_auth_test.rs` (anywhere in the file; these use only existing helpers):

```rust
// ExpectVersion must not leak another user's doc version: every probe of an
// unowned doc returns NotFound (the absent outcome), never Ok or PreconditionFailed.
#[tokio::test]
async fn expect_version_does_not_leak_unowned_doc() -> anyhow::Result<()> {
    let (pool, db, schema) = setup().await;
    let bob_id = seed_note(&pool, &db, &schema, "bob's note", "bob").await;
    let alice = PrincipalCtx { user_id: Some("alice".into()), email: None };

    for probe in [0_i64, 1, 2, 99] {
        let err = execute_txn(
            &pool,
            &db,
            &schema,
            &Transaction {
                steps: vec![Step::ExpectVersion {
                    table: "notes".into(),
                    id: bob_id.clone(),
                    version: probe,
                }],
            },
            &alice,
        )
        .await
        .expect_err("expectVersion on unowned doc must not succeed");
        assert_eq!(
            err.code,
            ErrorCode::NotFound,
            "probe version {probe} leaked existence/version: {err:?}"
        );
    }
    Ok(())
}

// Own-doc optimistic concurrency is preserved: NotFound / PreconditionFailed / Ok.
#[tokio::test]
async fn expect_version_own_doc_behaves_as_before() -> anyhow::Result<()> {
    let (pool, db, schema) = setup().await;
    let id = seed_note(&pool, &db, &schema, "alice's note", "alice").await;
    let real_version = fetch_doc(&pool, &db, &schema, &id)
        .await
        .expect("seeded")["_version"]
        .as_i64()
        .expect("version");
    let alice = PrincipalCtx { user_id: Some("alice".into()), email: None };

    let err = execute_txn(
        &pool, &db, &schema,
        &Transaction { steps: vec![Step::ExpectVersion {
            table: "notes".into(), id: id.clone(), version: real_version + 1,
        }] },
        &alice,
    ).await.expect_err("wrong version must fail");
    assert_eq!(err.code, ErrorCode::PreconditionFailed);

    execute_txn(
        &pool, &db, &schema,
        &Transaction { steps: vec![Step::ExpectVersion {
            table: "notes".into(), id: id.clone(), version: real_version,
        }] },
        &alice,
    ).await.expect("matching version succeeds");

    let err = execute_txn(
        &pool, &db, &schema,
        &Transaction { steps: vec![Step::ExpectVersion {
            table: "notes".into(), id: "does-not-exist".into(), version: real_version,
        }] },
        &alice,
    ).await.expect_err("absent doc must fail");
    assert_eq!(err.code, ErrorCode::NotFound);
    Ok(())
}

// Bypass caller (machine/admin/scheduled) is unaffected: version compare still runs.
#[tokio::test]
async fn expect_version_bypass_is_unaffected() -> anyhow::Result<()> {
    let (pool, db, schema) = setup().await;
    let bob_id = seed_note(&pool, &db, &schema, "bob's note", "bob").await;
    let real_version = fetch_doc(&pool, &db, &schema, &bob_id)
        .await.expect("seeded")["_version"].as_i64().expect("version");

    execute_txn(
        &pool, &db, &schema,
        &Transaction { steps: vec![Step::ExpectVersion {
            table: "notes".into(), id: bob_id.clone(), version: real_version,
        }] },
        &PrincipalCtx::bypass(),
    ).await.expect("bypass sees the doc");

    let err = execute_txn(
        &pool, &db, &schema,
        &Transaction { steps: vec![Step::ExpectVersion {
            table: "notes".into(), id: bob_id, version: real_version + 1,
        }] },
        &PrincipalCtx::bypass(),
    ).await.expect_err("wrong version must fail for bypass too");
    assert_eq!(err.code, ErrorCode::PreconditionFailed);
    Ok(())
}

// authorize-only table: a doc the predicate hides from the caller yields NotFound
// even on the correct version; the owning user sees PreconditionFailed/Ok normally.
#[tokio::test]
async fn expect_version_authorize_hides_invisible_doc() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_authorize().await;
    // bob's PRIVATE post: predicate = owner==$user OR visibility=="public".
    // alice (owner!=alice, visibility!=public) cannot see it.
    let bob_id = seed_post(&pool, &db, &schema, "bob private", "bob", "private").await;
    let mut q = posts_query();
    q.get = Some(bob_id.clone());
    let real_version = match execute_query(&pool, &db, &schema, &q, &PrincipalCtx::bypass())
        .await
        .expect("fetch bob's post")
    {
        QueryResult::Doc(Some(d)) => d["_version"].as_i64().expect("version"),
        other => panic!("expected Doc(Some), got {other:?}"),
    };
    let alice = PrincipalCtx { user_id: Some("alice".into()), email: None };

    // alice cannot see bob's private post -> NotFound even on the real version.
    let err = execute_txn(
        &pool, &db, &schema,
        &Transaction { steps: vec![Step::ExpectVersion {
            table: "posts".into(), id: bob_id.clone(), version: real_version,
        }] },
        &alice,
    ).await.expect_err("invisible doc must not be probeable");
    assert_eq!(err.code, ErrorCode::NotFound);

    // bob owns it -> wrong version is PreconditionFailed (predicate lets him see it).
    let err = execute_txn(
        &pool, &db, &schema,
        &Transaction { steps: vec![Step::ExpectVersion {
            table: "posts".into(), id: bob_id.clone(), version: real_version + 1,
        }] },
        &PrincipalCtx { user_id: Some("bob".into()), email: None },
    ).await.expect_err("owner sees a wrong version");
    assert_eq!(err.code, ErrorCode::PreconditionFailed);
    Ok(())
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd server && make -C .. dev-db-up && cargo test --test per_row_auth_test expect_version_ -- --nocapture`
Expected: `expect_version_does_not_leak_unowned_doc` and `expect_version_authorize_hides_invisible_doc` **FAIL** (the unowned probe returns `PreconditionFailed`/`Ok` instead of `NotFound` — the leak). `expect_version_own_doc_behaves_as_before` and `expect_version_bypass_is_unaffected` should already PASS (they assert unchanged behavior).

- [ ] **Step 3: Add the `doc_visible_to` helper** — in `server/src/txn.rs`, immediately after `check_owner_doc` (ends ~line 1097):

```rust
/// Boolean twin of [`check_owner_doc`]: `true` iff `doc` is visible to `ctx`
/// under the table's per-row gates (`ownerField`/`collaboratorsField` and/or
/// `authorize`). Used by the read-only `ExpectVersion`/`ExpectAbsent`
/// preconditions to close the existence/version side-channel — a non-visible
/// doc is treated as absent rather than rejected with `Forbidden`, because a
/// `Forbidden` would itself be a louder oracle ("exists, but not yours").
///
/// Keep in lockstep with `check_owner_doc`: any new per-row gate must be added
/// in both places.
fn doc_visible_to(doc: &serde_json::Value, table_def: &TableDef, ctx: &PrincipalCtx) -> bool {
    let owner_uid = row_auth_enforced_uid(table_def, ctx.user_id.as_deref());
    let authorize = table_def.authorize.as_ref();
    let user_is_some = ctx.user_id.is_some();
    if owner_uid.is_none() && !(authorize.is_some() && user_is_some) {
        return true; // no gate applies (bypass caller, or table declares nothing)
    }
    let mut visible = true;
    if let Some(uid) = owner_uid
        && !row_visible_to(
            doc,
            table_def.owner_field.as_deref(),
            table_def.collaborators_field.as_deref(),
            uid,
        )
    {
        visible = false;
    }
    if let Some(authorize) = authorize
        && user_is_some
        && !filter_matches(doc, authorize, ctx)
    {
        visible = false;
    }
    visible
}
```

- [ ] **Step 4: Rewrite `do_expect_version`** — replace the existing function (currently `server/src/txn.rs:784`) with:

```rust
async fn do_expect_version(
    conn: &mut PgConnection,
    pg_schema_name: &str,
    table_def: &TableDef,
    table_name: &str,
    id: &str,
    expected: i64,
    ctx: &PrincipalCtx,
) -> Result<(), RtDbError> {
    let table_ident = pg_table(table_name);
    let row: Option<(i64, serde_json::Value)> = sqlx::query_as(&format!(
        "SELECT \"version\", \"doc\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE \"id\" = $1"
    ))
    .bind(id)
    .fetch_optional(&mut *conn)
    .await?;
    let Some((actual, doc)) = row else {
        return Err(RtDbError::not_found(format!("document '{id}' not found")));
    };
    // Side-channel closure: a doc the caller cannot see is indistinguishable
    // from absent — same not_found outcome, so no version is ever leaked.
    if !doc_visible_to(&doc, table_def, ctx) {
        return Err(RtDbError::not_found(format!("document '{id}' not found")));
    }
    if actual != expected {
        return Err(RtDbError::precondition(format!(
            "version mismatch: expected {expected}, actual {actual}"
        )));
    }
    Ok(())
}
```

- [ ] **Step 5: Update the `ExpectVersion` dispatch arm** — in the `match step` in `execute_txn` (currently `server/src/txn.rs:1223`), replace:

```rust
            Step::ExpectVersion { table, id, version } => {
                schema.table(table)?;
                do_expect_version(&mut tx, &pg_schema_name, table, id, *version).await?;
                results.push(serde_json::Value::Null);
            }
```

with:

```rust
            Step::ExpectVersion { table, id, version } => {
                let table_def = schema.table(table)?;
                do_expect_version(&mut tx, &pg_schema_name, table_def, table, id, *version, ctx).await?;
                results.push(serde_json::Value::Null);
            }
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd server && cargo test --test per_row_auth_test expect_version_ -- --nocapture`
Expected: all 4 PASS.

- [ ] **Step 7: Lint + format the touched files**

Run: `cd server && cargo fmt && cargo clippy --all-targets -- -D warnings 2>&1 | tail -5`
Expected: no warnings. (If clippy flags the `if let ... && ...` let-chain style, match whatever `check_owner`/`check_owner_doc` already use — they use the same idiom, so it should be clean.)

- [ ] **Step 8: Commit**

```bash
cd ~/Repos/par-rt-db
git add server/src/txn.rs server/tests/per_row_auth_test.rs
git commit -m "fix(auth): close ExpectVersion per-row-auth side-channel

ExpectVersion now applies the ownerField/collaboratorsField/authorize
visibility gate: a doc the caller cannot see returns NotFound (identical to
a genuinely-absent doc) instead of leaking its version via PreconditionFailed
or Ok. Introduces doc_visible_to, the boolean twin of check_owner_doc. Own-doc
OCC and bypass callers are byte-identical."
```

---

### Task 2: Close the `ExpectAbsent` oracle

**Files:**
- Modify: `server/src/txn.rs` — `ExpectAbsent` dispatch arm only (filter `eq_lookup` rows).
- Test: `server/tests/per_row_auth_test.rs` — add 4 integration tests.

**Interfaces:**
- Consumes: `doc_visible_to` from Task 1; the existing `eq_lookup` (returns `Vec<(String, serde_json::Value, i64)>`).
- Produces: nothing new (final code change).

- [ ] **Step 1: Write the failing tests** — append to `server/tests/per_row_auth_test.rs`:

```rust
// ExpectAbsent must not leak another user's doc existence: a match on an
// unowned doc is invisible to the caller, so the precondition succeeds (Ok).
#[tokio::test]
async fn expect_absent_does_not_leak_unowned_doc() -> anyhow::Result<()> {
    let (pool, db, schema) = setup().await;
    seed_note(&pool, &db, &schema, "bob's note", "bob").await;
    let alice = PrincipalCtx { user_id: Some("alice".into()), email: None };

    // bob owns the userId="bob" row; alice probing that key sees "absent" -> Ok.
    execute_txn(
        &pool, &db, &schema,
        &Transaction { steps: vec![Step::ExpectAbsent {
            table: "notes".into(), index: "by_user".into(), eq: vec![json!("bob")],
        }] },
        &alice,
    ).await.expect("alice sees bob's key as absent");

    // an unused key is also absent -> Ok (control: same outcome, no leak).
    execute_txn(
        &pool, &db, &schema,
        &Transaction { steps: vec![Step::ExpectAbsent {
            table: "notes".into(), index: "by_user".into(), eq: vec![json!("nobody")],
        }] },
        &alice,
    ).await.expect("unused key is absent");
    Ok(())
}

// Own-doc ExpectAbsent is unchanged: a match on the caller's own row fails.
#[tokio::test]
async fn expect_absent_own_doc_behaves_as_before() -> anyhow::Result<()> {
    let (pool, db, schema) = setup().await;
    seed_note(&pool, &db, &schema, "alice's note", "alice").await;
    let alice = PrincipalCtx { user_id: Some("alice".into()), email: None };

    let err = execute_txn(
        &pool, &db, &schema,
        &Transaction { steps: vec![Step::ExpectAbsent {
            table: "notes".into(), index: "by_user".into(), eq: vec![json!("alice")],
        }] },
        &alice,
    ).await.expect_err("own row must be present");
    assert_eq!(err.code, ErrorCode::PreconditionFailed);
    Ok(())
}

// Bypass caller sees every match: ExpectAbsent fails on bob's row.
#[tokio::test]
async fn expect_absent_bypass_is_unaffected() -> anyhow::Result<()> {
    let (pool, db, schema) = setup().await;
    seed_note(&pool, &db, &schema, "bob's note", "bob").await;

    let err = execute_txn(
        &pool, &db, &schema,
        &Transaction { steps: vec![Step::ExpectAbsent {
            table: "notes".into(), index: "by_user".into(), eq: vec![json!("bob")],
        }] },
        &PrincipalCtx::bypass(),
    ).await.expect_err("bypass sees bob's row as present");
    assert_eq!(err.code, ErrorCode::PreconditionFailed);
    Ok(())
}

// collaboratorsField: a non-member sees the owner's key as absent (Ok); a
// declared collaborator sees it as present (PreconditionFailed).
#[tokio::test]
async fn expect_absent_collaborators_visibility() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_collab().await;
    // bob owns; alice is a collaborator; carol is neither.
    seed_collab_note(&pool, &db, &schema, "shared", "bob", &["alice"]).await;

    execute_txn(
        &pool, &db, &schema,
        &Transaction { steps: vec![Step::ExpectAbsent {
            table: "notes".into(), index: "by_user".into(), eq: vec![json!("bob")],
        }] },
        &PrincipalCtx { user_id: Some("carol".into()), email: None },
    ).await.expect("non-collaborator sees the key as absent");

    let err = execute_txn(
        &pool, &db, &schema,
        &Transaction { steps: vec![Step::ExpectAbsent {
            table: "notes".into(), index: "by_user".into(), eq: vec![json!("bob")],
        }] },
        &PrincipalCtx { user_id: Some("alice".into()), email: None },
    ).await.expect_err("collaborator sees the row as present");
    assert_eq!(err.code, ErrorCode::PreconditionFailed);
    Ok(())
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd server && cargo test --test per_row_auth_test expect_absent_ -- --nocapture`
Expected: `expect_absent_does_not_leak_unowned_doc` and the carol case of `expect_absent_collaborators_visibility` **FAIL** (alice/carol see bob's row as present → `PreconditionFailed` — the leak). The own-doc and bypass tests already PASS.

- [ ] **Step 3: Update the `ExpectAbsent` dispatch arm** — in `execute_txn` (currently `server/src/txn.rs:1228`), replace:

```rust
            Step::ExpectAbsent { table, index, eq } => {
                let table_def = schema.table(table)?;
                let rows = eq_lookup(&mut tx, &pg_schema_name, table_def, table, index, eq).await?;
                if !rows.is_empty() {
                    return Err(RtDbError::precondition(format!(
                        "index '{index}' already has a matching document"
                    )));
                }
                results.push(serde_json::Value::Null);
            }
```

with:

```rust
            Step::ExpectAbsent { table, index, eq } => {
                let table_def = schema.table(table)?;
                let rows = eq_lookup(&mut tx, &pg_schema_name, table_def, table, index, eq).await?;
                // Side-channel closure: only a matched doc the caller can see
                // counts as "present". A matched-but-invisible doc is "absent"
                // from the caller's view, so it does not fail the precondition.
                let present = rows
                    .iter()
                    .any(|(_id, doc, _created_at)| doc_visible_to(doc, table_def, ctx));
                if present {
                    return Err(RtDbError::precondition(format!(
                        "index '{index}' already has a matching document"
                    )));
                }
                results.push(serde_json::Value::Null);
            }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd server && cargo test --test per_row_auth_test expect_absent_ -- --nocapture`
Expected: all 4 PASS.

- [ ] **Step 5: Lint + format**

Run: `cd server && cargo fmt && cargo clippy --all-targets -- -D warnings 2>&1 | tail -5`
Expected: no warnings.

- [ ] **Step 6: Commit**

```bash
cd ~/Repos/par-rt-db
git add server/src/txn.rs server/tests/per_row_auth_test.rs
git commit -m "fix(auth): close ExpectAbsent per-row-auth side-channel

ExpectAbsent now filters matched rows through doc_visible_to: only a visible
match fails the precondition. A matched doc the caller cannot see is 'absent'
from their view, so probing another user's key succeeds (Ok) instead of leaking
existence via PreconditionFailed. Bypass callers and own-row probes unchanged."
```

---

### Task 3: Doc follow-ups + full gate

**Files:**
- Modify: `FEATURE_MATRIX.md` (row #20 note).
- Modify: `docs/superpowers/specs/2026-07-24-per-row-authorization-design.md` (threat-model item 7).
- Modify: `docs/superpowers/specs/2026-08-02-per-row-auth-predicate-dsl-design.md` (security-invariant item 7).

- [ ] **Step 1: Update `FEATURE_MATRIX.md` row #20** — find the sentence ending *"Still deferred: `ExpectVersion`/`ExpectAbsent` (an existence/version side-channel, not owner-checked)."* and replace with:

> `ExpectVersion`/`ExpectAbsent` now apply the per-row visibility gate (ownerField/collaboratorsField/`authorize`): a doc the caller cannot see is indistinguishable from absent (`ExpectVersion`→`NotFound`, `ExpectAbsent`→`Ok`), closing the existence/version oracle. `ExpectAbsent` is thereby per-user — a real `unique` index remains the global hard guarantee. See `docs/superpowers/specs/2026-08-03-close-expect-version-absent-side-channel-design.md`.

Also update the §7 *Status* bullet that says *"`ExpectVersion`/`ExpectAbsent` … remains deferred"* to *"`ExpectVersion`/`ExpectAbsent` side-channel closed (2026-08-03)."*

- [ ] **Step 2: Update the v1 per-row-auth spec** — in `2026-07-24-per-row-authorization-design.md`, threat-model item 7, change the heading/lead from *"Side-channel not closed in v1 (accepted)"* to ***"Side-channel — RESOLVED (2026-08-03)"*** and append: *"Closed per `docs/superpowers/specs/2026-08-03-close-expect-version-absent-side-channel-design.md`: both steps now apply the visibility gate; a non-visible doc maps to the absent outcome."*

- [ ] **Step 3: Update the Model C per-row-auth spec** — in `2026-08-02-per-row-auth-predicate-dsl-design.md`, security-invariant item 7, change *"Side-channel (carried forward from v1, accepted)"* to ***"Side-channel — RESOLVED (2026-08-03)"*** with the same pointer.

- [ ] **Step 4: Run the full gate**

Run: `cd ~/Repos/par-rt-db && make dev-db-up && make checkall`
Expected: green (fmt-check + clippy `-D warnings` + typecheck + the whole suite). The new tests are in `per_row_auth_test`; the existing per-row suites confirm no regression.

- [ ] **Step 5: Commit**

```bash
cd ~/Repos/par-rt-db
git add FEATURE_MATRIX.md docs/superpowers/specs/2026-07-24-per-row-authorization-design.md docs/superpowers/specs/2026-08-02-per-row-auth-predicate-dsl-design.md
git commit -m "docs(auth): mark ExpectVersion/ExpectAbsent side-channel resolved

FEATURE_MATRIX row #20 and both per-row-auth specs (2026-07-24 threat-model
#7, 2026-08-02 security-invariant #7) flip from deferred to resolved, pointing
at the 2026-08-03 closure design. Notes the per-user ExpectAbsent semantics
vs. the global unique-index guarantee."
```

---

## Self-Review (completed)

**Spec coverage:** Every spec section maps to a task — `doc_visible_to` (Task 1), `ExpectVersion` enforcement (Task 1), `ExpectAbsent` enforcement (Task 2), bypass/no-gate no-op (Tasks 1 & 2 bypass tests), authorize-only model (Task 1 authorize test), collaborators model (Task 2 collaborators test), the per-user `ExpectAbsent` interaction (Task 3 doc note), testing matrix (Tasks 1 & 2), follow-on doc updates (Task 3).

**Placeholder scan:** None. (Pre-flight resolved the one soft spot — `QueryResult` has no `as_doc()` accessor, so the authorize test destructures `QueryResult::Doc(Some(d))` directly, mirroring the existing `fetch_doc` pattern.)

**Type consistency:** `doc_visible_to(&serde_json::Value, &TableDef, &PrincipalCtx) -> bool` is defined in Task 1 and consumed identically in Task 2. `do_expect_version`'s new signature matches the call site updated in Task 1 Step 5. `eq_lookup`'s return tuple `(String, serde_json::Value, i64)` matches the destructure `(_id, doc, _created_at)` in Task 2 Step 3.
