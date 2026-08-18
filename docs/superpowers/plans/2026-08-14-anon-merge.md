# Anonymous → Real Account Merge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When a user who signed in anonymously later completes an OAuth login, re-stamp their entire footprint (documents via `ownerField`/`collaboratorsField`/`authorize` `$user` fields, storage blobs, sessions) to the real user id, inside each db's committer turn, then retire the anon user row — plus an admin escape hatch.

**Architecture:** A nullable `anon_user_id` column on `rtdb_auth.oauth_states` records the binding at `/begin` (resolved from the caller's anonymous session). The callback runs a synchronous merge before `set_outcome`: a new `CommitterRequest::RunMergeUsers` arm rewrites principal-bearing doc fields per database (SELECT candidates → rewrite in Rust → per-row `apply_update` on `ctx.pool`, mirroring `handle_reaper`'s no-explicit-transaction pattern), then storage owner swap, session re-point, and a guarded anon-row delete run as direct SQL. All document writes flow through `publish_taps(source = "merge")` so subscriptions/op-feed/audit/webhooks fire. **Note:** the spec's Execution section sketches the doc rewrite as one batched SQL UPDATE with jsonb transforms; this plan supersedes that with the per-row Rust-rewrite + `apply_update` approach (settled at design review) — same semantics, but indexed columns and `version` are recomputed by the existing tested helper instead of hand-written SQL, and a per-row unique violation is caught without savepoints.

**Tech Stack:** Rust (axum/tokio/sqlx, Postgres 17). No new crates. Server-only — no wire, protocol, or client changes.

**Spec:** `docs/superpowers/specs/2026-08-14-anon-merge-design.md` (read it first; the plan argues from it)

## Global Constraints

- `make dev-db-up` before any test run (integration tests hit real Postgres on `127.0.0.1:55434`).
- Gate: `make -C ~/Repos/par-rt-db checkall` (fmt + clippy `-D warnings` + typecheck + tests). Run from the repo root or with `-C` — a `cd`-shifted cwd silently gates only one package.
- No `unwrap()`/`expect()` outside `#[cfg(test)]`. Zero clippy warnings.
- SQL: double-quote every identifier built from names (`ddl::pg_table`/`pg_col`/`pg_schema`), bind every value via `$n`.
- Single-writer invariant: document writes only inside the committer turn. Storage/session/user-row SQL is direct (storage bypasses the committer by design; auth tables are not document tables).
- Merge rewrites replace **only** occurrences of the anon `user_id` string — never touch other users' values.
- Security: this work touches auth (`auth/provider.rs`, `db.rs`) — the final session report must explicitly flag the auth-touching commits for manual review (standing rule).
- Branch: work on `feat/anon-merge` (spec already committed there as `4ae1bf4`). Commit each task with Bash `timeout: 600000` (pre-commit clippy >2 min).

---

### Task 1: Pure principal-field derivation and doc rewrite (`merge.rs`)

**Files:**
- Create: `server/src/merge.rs`
- Modify: `server/src/lib.rs` (add `pub mod merge;` next to the other module declarations — public like `auth`/`db`, because `server/tests/merge_test.rs` reaches it as `rtdb_server::merge::…`)

**Interfaces:**
- Consumes: `crate::schema::{FieldType, TableDef}`, `crate::query::FilterExpr`.
- Produces (used by Tasks 2–5):
  - `pub(crate) enum FieldKind { Scalar, Array }`
  - `pub(crate) struct PrincipalField { pub field: String, pub kind: FieldKind }`
  - `pub(crate) fn principal_bearing_fields(table: &TableDef) -> Vec<PrincipalField>`
  - `pub(crate) fn rewrite_doc(doc: &mut serde_json::Map<String, serde_json::Value>, fields: &[PrincipalField], anon: &str, real: &str) -> bool`
  - `#[derive(Debug, Clone, Default, serde::Serialize)] #[serde(rename_all = "camelCase")] pub struct MergeDbResult { pub tables: BTreeMap<String, usize>, pub conflicts: Vec<MergeConflict> }`
  - `#[derive(Debug, Clone, serde::Serialize)] #[serde(rename_all = "camelCase")] pub struct MergeConflict { pub table: String, pub id: String }`

- [ ] **Step 1: Write the failing unit tests**

Create `server/src/merge.rs` with only the test module (plus minimal type stubs so it compiles), or write tests first if you prefer the file to compile red — either way these exact tests must exist:

```rust
//! Anon→real account merge (FM-27): pure derivation of principal-bearing
//! fields from a table def, the doc rewrite, and the cross-database
//! orchestration. See docs/superpowers/specs/2026-08-14-anon-merge-design.md.

use std::collections::BTreeMap;

use crate::query::FilterExpr;
use crate::schema::{FieldType, TableDef};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FieldKind {
    /// A scalar string field: rewrite is a whole-value swap.
    Scalar,
    /// An array-of-strings field: rewrite is element swap + dedupe.
    Array,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PrincipalField {
    pub field: String,
    pub kind: FieldKind,
}

/// Per-db outcome of `RunMergeUsers`: restamped-doc counts per table and the
/// rows skipped because the restamp would violate a unique index.
#[derive(Debug, Clone, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MergeDbResult {
    pub tables: BTreeMap<String, usize>,
    pub conflicts: Vec<MergeConflict>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MergeConflict {
    pub table: String,
    pub id: String,
}

// ... principal_bearing_fields + rewrite_doc implemented in Step 3 ...

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn table(fields: &[(&str, FieldType)]) -> TableDef {
        let mut map = BTreeMap::new();
        for (name, ty) in fields {
            map.insert((*name).to_string(), ty.clone());
        }
        TableDef {
            fields: map,
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
            authorize: None,
            ttl: None,
        }
    }

    fn user_marker() -> serde_json::Value {
        json!({ "$user": true })
    }

    #[test]
    fn derives_owner_and_collaborators_fields() {
        let mut def = table(&[
            ("owner", FieldType::String),
            ("editors", FieldType::Array { element: Box::new(FieldType::String) }),
        ]);
        def.owner_field = Some("owner".into());
        def.collaborators_field = Some("editors".into());
        let fields = principal_bearing_fields(&def);
        assert_eq!(fields.len(), 2);
        assert!(fields.contains(&PrincipalField { field: "owner".into(), kind: FieldKind::Scalar }));
        assert!(fields.contains(&PrincipalField { field: "editors".into(), kind: FieldKind::Array }));
    }

    #[test]
    fn walks_authorize_across_all_variants_including_not_and_in() {
        let mut def = table(&[
            ("uid", FieldType::String),
            ("members", FieldType::Array { element: Box::new(FieldType::String) }),
            ("count", FieldType::Number),
        ]);
        def.authorize = Some(FilterExpr::And { exprs: vec![
            FilterExpr::Or { exprs: vec![
                FilterExpr::Eq { field: "uid".into(), value: user_marker() },
                FilterExpr::Contains { field: "members".into(), value: user_marker() },
            ]},
            FilterExpr::Not { expr: Box::new(FilterExpr::In { field: "uid".into(), values: vec![json!("x"), user_marker()] }) },
            FilterExpr::Neq { field: "uid".into(), value: user_marker() },
        ]});
        // "uid" arrives from Eq/In/Neq — deduped; "members" from Contains.
        let fields = principal_bearing_fields(&def);
        assert_eq!(fields.len(), 2);
        assert!(fields.iter().any(|f| f.field == "uid" && f.kind == FieldKind::Scalar));
        assert!(fields.iter().any(|f| f.field == "members" && f.kind == FieldKind::Array));
    }

    #[test]
    fn skips_non_string_and_non_array_of_string_fields() {
        let mut def = table(&[
            ("count", FieldType::Number),
            ("flags", FieldType::Array { element: Box::new(FieldType::Number) }),
            ("uid", FieldType::String),
        ]);
        def.authorize = Some(FilterExpr::Eq { field: "count".into(), value: user_marker() });
        def.owner_field = Some("flags".into()); // declared wrongly; must be skipped
        def.collaborators_field = Some("uid".into()); // declared wrongly; scalar field
        let fields = principal_bearing_fields(&def);
        // "flags" (array of number) skipped; "uid" as collaboratorsField on a
        // scalar string field degrades to Scalar (a scalar swap is the only
        // sound rewrite); "count" skipped.
        assert!(fields.iter().any(|f| f.field == "uid" && f.kind == FieldKind::Scalar));
        assert!(!fields.iter().any(|f| f.field == "count"));
        assert!(!fields.iter().any(|f| f.field == "flags"));
    }

    #[test]
    fn rewrite_swaps_scalar_and_array_elements_only_for_anon() {
        let fields = vec![
            PrincipalField { field: "owner".into(), kind: FieldKind::Scalar },
            PrincipalField { field: "editors".into(), kind: FieldKind::Array },
        ];
        let mut doc = serde_json::Map::new();
        doc.insert("owner".into(), json!("user_anon"));
        doc.insert("editors".into(), json!(["user_other", "user_anon"]));
        doc.insert("title".into(), json!("user_anon")); // not principal-bearing: untouched
        let changed = rewrite_doc(&mut doc, &fields, "user_anon", "user_real");
        assert!(changed);
        assert_eq!(doc["owner"], json!("user_real"));
        assert_eq!(doc["editors"], json!(["user_other", "user_real"]));
        assert_eq!(doc["title"], json!("user_anon"));
    }

    #[test]
    fn rewrite_dedupes_real_already_present_and_reports_no_change() {
        let fields = vec![PrincipalField { field: "editors".into(), kind: FieldKind::Array }];
        let mut doc = serde_json::Map::new();
        doc.insert("editors".into(), json!(["user_real", "user_anon"]));
        assert!(rewrite_doc(&mut doc, &fields, "user_anon", "user_real"));
        assert_eq!(doc["editors"], json!(["user_real"]));

        let mut untouched = serde_json::Map::new();
        untouched.insert("editors".into(), json!(["user_other"]));
        assert!(!rewrite_doc(&mut untouched, &fields, "user_anon", "user_real"));
    }
}
```

Note: `TableDef` may have more fields than the struct literal above shows — construct it the way other unit tests in the crate do; if `TableDef` doesn't implement a convenient constructor, build it via `serde_json::from_value` with a JSON literal matching the wire shape (`deny_unknown_fields` is NOT set on TableDef; check `server/src/schema.rs:149` first and mirror whatever pattern `schema_validators_test.rs` uses for in-memory TableDefs).

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --manifest-path ~/Repos/par-rt-db/server/Cargo.toml --lib merge::` (no dev-db needed — pure unit tests)
Expected: FAIL — `principal_bearing_fields`/`rewrite_doc` not defined.

- [ ] **Step 3: Implement the two pure functions**

Add to `server/src/merge.rs`:

```rust
/// Whether values of this declared type are string-comparable (so a scalar
/// swap is sound). `Optional` unwraps; a `Union` qualifies only if every
/// variant does; a string-valued `Literal` qualifies.
fn string_compatible(ty: &FieldType) -> bool {
    match ty {
        FieldType::String | FieldType::Id { .. } => true,
        FieldType::Optional { inner } | FieldType::Array { element: inner } => {
            string_compatible(inner)
        }
        FieldType::Union { variants } => variants.iter().all(string_compatible),
        FieldType::Literal { value } => value.is_string(),
        _ => false,
    }
}

/// The rewrite kind a declared field supports: `Scalar` for string-compatible
/// types, `Array` for array-of-strings, `None` when neither (skip with a
/// warning — over-approximate to skipping, never fail the merge).
fn rewrite_kind(ty: &FieldType) -> Option<FieldKind> {
    match ty {
        FieldType::Array { element } if string_compatible(element) => Some(FieldKind::Array),
        FieldType::Optional { inner } => rewrite_kind(inner),
        ty if string_compatible(ty) => Some(FieldKind::Scalar),
        _ => None,
    }
}

/// `true` for the exact principal marker `{"$user": true}` anywhere in a
/// value — including nested inside an `In` array. Mirrors
/// `txn.rs::user_eq_fields`' marker test, but broader: the merge walker must
/// find EVERY field referencing the anon uid, not only stampable Eq leaves.
fn mentions_user_marker(v: &serde_json::Value) -> bool {
    if let serde_json::Value::Object(map) = v
        && map.len() == 1
    {
        return map.get("$user").and_then(|x| x.as_bool()) == Some(true);
    }
    v.as_array().is_some_and(|arr| arr.iter().any(mentions_user_marker))
}

/// Collects every field that can carry a user principal for this table:
/// `ownerField`, `collaboratorsField`, and every field of the `authorize`
/// predicate whose comparison value mentions the `$user` marker (the walk
/// descends `And`/`Or`/`Not` and checks every value-bearing variant — a new
/// `FilterExpr` variant is a compile-visible change site here). The rewrite
/// kind comes from the field's declared type, so a field arriving from two
/// sources (ownerField AND authorize) dedupes consistently.
pub(crate) fn principal_bearing_fields(table: &TableDef) -> Vec<PrincipalField> {
    let mut out: Vec<PrincipalField> = Vec::new();
    let mut push = |name: &str, out: &mut Vec<PrincipalField>| match table.fields.get(name) {
        Some(ty) => match rewrite_kind(ty) {
            Some(kind) => {
                if !out.iter().any(|f| f.field == name) {
                    out.push(PrincipalField { field: name.to_string(), kind });
                }
            }
            None => tracing::warn!(
                table = table.fields.keys().next().map(String::as_str).unwrap_or(""),
                field = name,
                "merge: principal-bearing field is not string or array-of-strings; skipping"
            ),
        },
        None => tracing::warn!(field = name, "merge: principal-bearing field not declared; skipping"),
    };

    if let Some(owner) = &table.owner_field {
        push(owner, &mut out);
    }
    if let Some(collab) = &table.collaborators_field {
        push(collab, &mut out);
    }
    if let Some(authorize) = &table.authorize {
        let mut walk = |expr: &FilterExpr, out: &mut Vec<PrincipalField>| match expr {
            FilterExpr::Eq { field, value }
            | FilterExpr::Neq { field, value }
            | FilterExpr::Gt { field, value }
            | FilterExpr::Gte { field, value }
            | FilterExpr::Lt { field, value }
            | FilterExpr::Lte { field, value }
            | FilterExpr::Contains { field, value } => {
                if mentions_user_marker(value) {
                    push(field, out);
                }
            }
            FilterExpr::In { field, values } => {
                if values.iter().any(mentions_user_marker) {
                    push(field, out);
                }
            }
            FilterExpr::And { exprs } | FilterExpr::Or { exprs } => {
                for e in exprs {
                    walk(e, out);
                }
            }
            FilterExpr::Not { expr } => walk(expr, out),
            FilterExpr::Exists { .. } => {}
        };
        walk(authorize, &mut out);
    }
    out
}

/// Rewrites occurrences of `anon` to `real` in `doc` for exactly the given
/// principal-bearing fields. Scalar: whole-value swap when the value equals
/// `anon`. Array: drop `anon` elements, append `real` unless already present.
/// Returns whether anything changed. Never touches other values.
pub(crate) fn rewrite_doc(
    doc: &mut serde_json::Map<String, serde_json::Value>,
    fields: &[PrincipalField],
    anon: &str,
    real: &str,
) -> bool {
    let mut changed = false;
    for pf in fields {
        let Some(value) = doc.get_mut(&pf.field) else { continue };
        match pf.kind {
            FieldKind::Scalar => {
                if value.as_str() == Some(anon) {
                    *value = serde_json::Value::String(real.to_string());
                    changed = true;
                }
            }
            FieldKind::Array => {
                let Some(arr) = value.as_array_mut() else { continue };
                let had_anon = arr.iter().any(|v| v.as_str() == Some(anon));
                if !had_anon {
                    continue;
                }
                arr.retain(|v| v.as_str() != Some(anon));
                if !arr.iter().any(|v| v.as_str() == Some(real)) {
                    arr.push(serde_json::Value::String(real.to_string()));
                }
                changed = true;
            }
        }
    }
    changed
}
```

Remove the `tracing::warn!` `table =` line's awkward key derivation if it doesn't compile cleanly — a plain `tracing::warn!(field = name, ...)` is fine (the table name isn't available in the pure fn; callers log it).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --manifest-path ~/Repos/par-rt-db/server/Cargo.toml --lib merge::`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git -C ~/Repos/par-rt-db add server/src/merge.rs server/src/lib.rs
git -C ~/Repos/par-rt-db commit -m "feat(merge): pure principal-bearing field derivation + doc rewrite (FM-27 task 1)"
```

Verify staging with `git -C ~/Repos/par-rt-db show --stat HEAD` (both files present).

---

### Task 2: `RunMergeUsers` committer arm + `merge_docs_total` metric

**Files:**
- Modify: `server/src/committer.rs` (new request variant, match arm, `Committers::merge_users`, `handle_merge_users`)
- Modify: `server/src/txn.rs` (open `apply_update`, `WriteSet::touch`, `WriteSet::capture_doc` as `pub(crate)`)
- Modify: `server/src/metrics.rs` (mirror `ttl_expired_total` end-to-end as `merge_docs_total`)
- Test: `server/tests/merge_test.rs` (new)

**Interfaces:**
- Consumes: Task 1's `principal_bearing_fields`/`rewrite_doc`/`MergeDbResult`/`MergeConflict`; `txn::{apply_update, WriteSet, OpKind}`; `ddl::{pg_schema, pg_table, pg_col, indexed_fields}`; existing `publish_taps(ctx, &schema, &write_set, owner, source, docop_taps, refresh_quota_cache)`.
- Produces (used by Tasks 3–5):
  - `CommitterRequest::RunMergeUsers { anon_id: String, real_id: String, reply: oneshot::Sender<Result<MergeDbResult, RtDbError>> }`
  - `impl Committers { pub async fn merge_users(&self, db: &str, anon_id: &str, real_id: &str) -> Result<MergeDbResult, RtDbError> }`
  - `Metrics::record_merge_doc()` + `rtdb_merge_docs_total` counter.

- [ ] **Step 1: Open the txn.rs helpers**

In `server/src/txn.rs`, change three declarations from private to `pub(crate)`:
- `async fn apply_update(` → `pub(crate) async fn apply_update(` (line ~662)
- `fn touch(&mut self` → `pub(crate) fn touch(&mut self` (line ~233)
- `fn capture_doc(&mut self` → `pub(crate) fn capture_doc(&mut self` (line ~271)

- [ ] **Step 2: Add the metric**

In `server/src/metrics.rs`, mirror every `ttl_expired_total` site for `merge_docs_total` (sites found at lines 295, 485–486, 594, 646, 786–791, and the three test/construction sites 914/974/1102):

1. Field: `merge_docs_total: AtomicU64,` next to `ttl_expired_total: AtomicU64,`
2. Recorder:
```rust
/// Documents re-stamped by the anon→real merge (FM-27), across all dbs/tables.
pub fn record_merge_doc(&self) {
    self.merge_docs_total.fetch_add(1, Ordering::Relaxed);
}
```
3. Snapshot load: `merge_docs_total: self.merge_docs_total.load(Ordering::Relaxed),`
4. Snapshot struct field: `pub merge_docs_total: u64,`
5. Prometheus exposition (mirror the HELP/TYPE/push_str block for `rtdb_ttl_expired_total`):
```rust
s.push_str("# HELP rtdb_merge_docs_total Total documents re-stamped by the anon-to-real user merge across all dbs/tables.\n");
s.push_str("# TYPE rtdb_merge_docs_total counter\n");
s.push_str(&format!("rtdb_merge_docs_total {}\n", snap.merge_docs_total));
```
6. All `Metrics` construction sites (including test constructors) get `merge_docs_total: 0,`.

- [ ] **Step 3: Write the failing integration tests**

Create `server/tests/merge_test.rs`. Fixture pattern: mirror `subs_test.rs`/`anonymous_auth_test.rs` — `test_state()`, create a db with `db::create_database` + `ddl::push_schema`, insert docs via `state.realtime.committers.mutate(&db, None, txn, PrincipalCtx::bypass())`.

```rust
mod common;

use std::collections::BTreeMap;

use common::{test_state, wrap_test_db};
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::db;
use rtdb_server::ddl::push_schema;
use rtdb_server::merge::MergeDbResult;
use rtdb_server::protocol::{Step, Transaction};
use rtdb_server::schema::{FieldType, IndexDef, SchemaDef, TableDef};
use serde_json::{Value, json};

fn owned_schema() -> SchemaDef {
    let mut fields = BTreeMap::new();
    fields.insert("title".to_string(), FieldType::String);
    fields.insert("owner".to_string(), FieldType::String);
    fields.insert("editors".to_string(), FieldType::Array { element: Box::new(FieldType::String) });
    let mut tables = BTreeMap::new();
    let mut table = TableDef {
        fields,
        indexes: vec![IndexDef {
            name: "by_owner".to_string(),
            fields: vec!["owner".to_string()],
            search: false,
            vector: None,
            unique: false,
            where_clause: None,
            ttl: None,
        }],
        owner_field: Some("owner".to_string()),
        collaborators_field: Some("editors".to_string()),
        authorize: None,
        ttl: None,
    };
    // If TableDef/IndexDef carry more fields than this literal shows, add
    // them with their defaults — check server/src/schema.rs first.
    let _ = &mut table;
    tables.insert("docs".to_string(), table);
    SchemaDef { tables }
}

fn insert_doc(table: &str, doc: Value) -> Transaction {
    Transaction { steps: vec![Step::Insert {
        table: table.to_string(),
        doc: doc.as_object().expect("object doc").clone(),
    }]}
}

async fn owned_doc_count(pool: &sqlx::PgPool, db: &str, uid: &str) -> i64 {
    let (n,): (i64,) = sqlx::query_as(
        &format!("SELECT COUNT(*) FROM \"{}\".\"t_docs\" WHERE \"doc\"->'owner' = to_jsonb($1::text)", db::pg_schema(db)),
    )
    .bind(uid)
    .fetch_one(pool)
    .await
    .expect("count owned docs");
    n
}

#[tokio::test]
async fn merge_users_restamps_owner_collaborators_and_bumps_version() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = wrap_test_db(&state).await;
    push_schema(&state.pool, &db, owned_schema()).await?;

    let anon = "user_anon_1";
    let real = "user_real_1";
    state.realtime.committers.mutate(&db, None,
        insert_doc("docs", json!({ "title": "a", "owner": anon, "editors": [] })), PrincipalCtx::bypass()).await?;
    state.realtime.committers.mutate(&db, None,
        insert_doc("docs", json!({ "title": "b", "owner": "user_other", "editors": [anon] })), PrincipalCtx::bypass()).await?;

    let result = state.realtime.committers.merge_users(&db, anon, real).await?;
    assert_eq!(result.tables.get("docs"), Some(&2));
    assert!(result.conflicts.is_empty());

    assert_eq!(owned_doc_count(&state.pool, &db, real).await, 1);
    // collaborator entry swapped
    let (n,): (i64,) = sqlx::query_as(
        &format!("SELECT COUNT(*) FROM \"{}\".\"t_docs\" WHERE \"doc\"->'editors' @> to_jsonb($1::text)", db::pg_schema(db)),
    ).bind(real).fetch_one(&state.pool).await?;
    assert_eq!(n, 1);
    // version bumped on the restamped rows (inserts start at version 1)
    let (v,): (i64,) = sqlx::query_as(
        &format!("SELECT \"version\" FROM \"{}\".\"t_docs\" WHERE \"doc\"->'title' = 'a'", db::pg_schema(db)),
    ).fetch_one(&state.pool).await?;
    assert_eq!(v, 2);

    // Idempotent: second run touches nothing.
    let again = state.realtime.committers.merge_users(&db, anon, real).await?;
    assert!(again.tables.values().all(|&n| n == 0));
    assert!(again.conflicts.is_empty());
    Ok(())
}

#[tokio::test]
async fn merge_users_skips_unique_conflict_and_reports_it() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = wrap_test_db(&state).await;
    // schema: unique index on (owner, title) so restamping anon->real can collide
    let mut schema = owned_schema();
    let table = schema.tables.get_mut("docs").expect("docs table");
    table.indexes[0] = IndexDef {
        name: "by_owner_title".to_string(),
        fields: vec!["owner".to_string(), "title".to_string()],
        search: false,
        vector: None,
        unique: true,
        where_clause: None,
        ttl: None,
    };
    push_schema(&state.pool, &db, schema).await?;

    let anon = "user_anon_2";
    let real = "user_real_2";
    // real user already owns ("t", "dup-title"); anon owns the colliding row and one free row
    for (owner, title) in [(real, "dup-title"), (anon, "dup-title"), (anon, "free")] {
        state.realtime.committers.mutate(&db, None,
            insert_doc("docs", json!({ "title": title, "owner": owner, "editors": [] })), PrincipalCtx::bypass()).await?;
    }

    let result = state.realtime.committers.merge_users(&db, anon, real).await?;
    assert_eq!(result.tables.get("docs"), Some(&1)); // the free row
    assert_eq!(result.conflicts.len(), 1);
    // the conflicting row keeps the anon owner
    assert_eq!(owned_doc_count(&state.pool, &db, anon).await, 1);
    Ok(())
}

#[tokio::test]
async fn merge_users_fires_subscription_fan_out() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = wrap_test_db(&state).await;
    push_schema(&state.pool, &db, owned_schema()).await?;
    let anon = "user_anon_3";
    let real = "user_real_3";
    state.realtime.committers.mutate(&db, None,
        insert_doc("docs", json!({ "title": "a", "owner": anon, "editors": [] })), PrincipalCtx::bypass()).await?;

    // Subscribe to the by_owner eq-window on the anon uid (bypass principal),
    // exactly the subscribe call pattern from server/tests/subs_test.rs.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    state.realtime.committers.subscribe(
        &db,
        1,                       // conn id — mirror subs_test.rs's next_conn_id() helper
        "q1".to_string(),
        rtdb_server::protocol::Query {
            table: "docs".to_string(),
            index: Some("by_owner".to_string()),
            eq: vec![json!(anon)],
            // remaining fields: mirror collect_work_items()'s construction in
            // subs_test.rs (terminal = take/collect with its default fields)
            ..Default::default()
        },
        tx,
        PrincipalCtx::bypass(),
    ).await?;
    let _initial = rx.try_recv().expect("initial query update");

    state.realtime.committers.merge_users(&db, anon, real).await?;
    let update = rx.try_recv().expect("fan-out pushed a query update after the merge");
    // The eq:[anon] window is now empty (owner restamped to real) — the push
    // carries the new result. Assert it serialized without the doc / with an
    // empty docs list, mirroring how subs_test.rs reads update payloads.
    assert!(!format!("{update:?}").contains("user_anon_3"));
    Ok(())
}
```

Adapt freely where this sketch's assumptions don't match the real types (Query's exact field set, `wrap_test_db`'s signature, physical table naming `t_docs` — verify with `ddl::pg_table("docs")`); the behavioral assertions are the contract.

- [ ] **Step 4: Run tests to verify they fail**

Run: `make -C ~/Repos/par-rt-db dev-db-up && cargo test --manifest-path ~/Repos/par-rt-db/server/Cargo.toml --test merge_test`
Expected: FAIL — `merge_users` doesn't exist on `Committers` (compile error).

- [ ] **Step 5: Implement the arm**

In `server/src/committer.rs`:

1. Request variant (next to `RunReaper` in the `CommitterRequest` enum):
```rust
/// FM-27: re-stamp every principal-bearing field referencing `anon_id` to
/// `real_id` across this db's tables, inside the serialized committer turn.
RunMergeUsers {
    anon_id: String,
    real_id: String,
    reply: oneshot::Sender<Result<crate::merge::MergeDbResult, RtDbError>>,
},
```

2. Match arm in `run_committer` (mirror `RunReaper`):
```rust
CommitterRequest::RunMergeUsers { anon_id, real_id, reply } => {
    let span = tracing::info_span!("committer.merge_users", db = %ctx.db);
    let outcome = handle_merge_users(&ctx, &anon_id, &real_id).instrument(span).await;
    if let Err(err) = &outcome {
        tracing::error!(db = %ctx.db, error = %err, "merge users handling failed");
    }
    let _ = reply.send(outcome);
}
```

3. Public method on `Committers` (mirror `mutate`'s reply plumbing exactly — same oneshot + `submit` + `reply_rx.await` shape):
```rust
/// Runs the FM-27 anon→real merge for one database inside its serialized
/// committer turn. Document rewrites happen here (single-writer invariant);
/// storage/session/user-row steps live in `merge::merge_users`.
pub async fn merge_users(
    &self,
    db: &str,
    anon_id: &str,
    real_id: &str,
) -> Result<crate::merge::MergeDbResult, RtDbError> {
    let (reply, reply_rx) = oneshot::channel();
    self.submit(
        db,
        CommitterRequest::RunMergeUsers {
            anon_id: anon_id.to_string(),
            real_id: real_id.to_string(),
            reply,
        },
    )
    .await?;
    reply_rx
        .await
        .map_err(|_| RtDbError::internal("committer task dropped the reply"))?
}
```

4. The handler (model: `handle_reaper` — statements issue directly on `&ctx.pool`, NO explicit transaction, so a per-row 23505 aborts only that row):
```rust
/// FM-27 committer arm: per table, select candidate rows whose
/// principal-bearing fields reference `anon_id`, rewrite the docs in Rust,
/// and apply per-row updates via `txn::apply_update` (recomputes indexed
/// columns + bumps version). A unique-index collision on one row (surfaced
/// by the sqlx→RtDbError mapping as ErrorCode::Conflict) skips that row into
/// `conflicts` and continues. Publishes through `publish_taps` with
/// `source = "merge"`, `owner = None` (system-initiated) so subscriptions,
/// op-feed, audit, and webhooks all fire.
async fn handle_merge_users(
    ctx: &CommitterCtx,
    anon_id: &str,
    real_id: &str,
) -> Result<crate::merge::MergeDbResult, RtDbError> {
    use crate::merge::{MergeConflict, MergeDbResult, principal_bearing_fields, rewrite_doc};
    use crate::txn::{OpKind, WriteSet};

    let schema = ctx.schemas.get(&ctx.pool, &ctx.db).await?;
    let pg_schema_name = crate::ddl::pg_schema(&ctx.db);
    let mut result = MergeDbResult::default();
    let mut write_set = WriteSet::default();
    let mut restamped = 0usize;

    for (table_name, table_def) in &schema.tables {
        let fields = principal_bearing_fields(table_def);
        if fields.is_empty() {
            continue;
        }
        let indexed = crate::ddl::indexed_fields(table_def);
        let table_ident = crate::ddl::pg_table(table_name);

        // One predicate per principal-bearing field, OR-joined; each binds
        // the anon uid once. Scalar fields use their typed f_ column when
        // indexed, else the jsonb doc path; arrays use jsonb containment.
        let mut predicates: Vec<String> = Vec::new();
        let mut binds = 0usize;
        for pf in &fields {
            binds += 1;
            let ph = format!("${binds}");
            predicates.push(match pf.kind {
                crate::merge::FieldKind::Scalar if indexed.contains(&pf.field) => {
                    format!("\"{}\" = {ph}", crate::ddl::pg_col(&pf.field))
                }
                crate::merge::FieldKind::Scalar => {
                    format!("\"doc\"->'{}' = to_jsonb({ph}::text)", pf.field)
                }
                crate::merge::FieldKind::Array => {
                    format!("\"doc\"->'{}' @> to_jsonb({ph}::text)", pf.field)
                }
            });
        }
        let sql = format!(
            "SELECT \"id\", \"doc\", \"created_at\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE {}",
            predicates.join(" OR ")
        );
        let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64)>(&sql);
        for _ in 0..binds {
            query = query.bind(anon_id);
        }
        let rows = match query.fetch_all(&ctx.pool).await {
            Ok(rows) => rows,
            Err(err) => {
                // Dropped-db guard, mirroring handle_reaper's tolerance.
                if crate::db::database_exists(&ctx.pool, &ctx.db).await.unwrap_or(false) {
                    tracing::warn!(db = %ctx.db, table = %table_name, error = %err, "merge: table scan failed");
                }
                continue;
            }
        };

        let mut table_count = 0usize;
        for (id, doc_value, created_at) in rows {
            let mut doc = match doc_value {
                serde_json::Value::Object(map) => map,
                _ => continue,
            };
            if !rewrite_doc(&mut doc, &fields, anon_id, real_id) {
                continue;
            }
            let pre_doc = doc.clone();
            let mut conn = ctx.pool.acquire().await?;
            match crate::txn::apply_update(
                &mut conn,
                &pg_schema_name,
                table_def,
                table_name,
                &id,
                &doc,
            )
            .await
            {
                Ok(()) => {
                    write_set.touch(table_name, &id, OpKind::Patch);
                    write_set.capture_doc(
                        table_name,
                        &id,
                        Some(Some(&pre_doc)),
                        Some(Some(&doc)),
                        Some(created_at),
                    );
                    table_count += 1;
                }
                Err(err) if matches!(err.code(), crate::error::ErrorCode::Conflict) => {
                    // 23505: the restamped row would collide with a row the
                    // real user already owns. Skip, report, keep going.
                    tracing::warn!(db = %ctx.db, table = %table_name, id = %id, "merge: unique conflict, row keeps anon owner");
                    result.conflicts.push(MergeConflict {
                        table: table_name.clone(),
                        id,
                    });
                }
                Err(err) => return Err(err),
            }
        }
        if table_count > 0 {
            result.tables.insert(table_name.clone(), table_count);
            restamped += table_count;
        }
    }

    if !write_set.ops.is_empty() {
        publish_taps(ctx, &schema, &write_set, None, "merge", true, false).await;
    }
    for _ in 0..restamped {
        ctx.metrics.record_merge_doc();
    }
    Ok(result)
}
```

Adapt details to the real code: `err.code()` accessor vs `err.code` field (see `error.rs`), `CommitterCtx`'s exact schema-cache field name (`ctx.schemas` — same as handle_reaper), and `Query`'s field names in the test.

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test --manifest-path ~/Repos/par-rt-db/server/Cargo.toml --test merge_test`
Expected: PASS (3 tests).

- [ ] **Step 7: Commit**

```bash
git -C ~/Repos/par-rt-db add server/src/committer.rs server/src/txn.rs server/src/metrics.rs server/tests/merge_test.rs
git -C ~/Repos/par-rt-db commit -m "feat(merge): RunMergeUsers committer arm restamps principal-bearing docs (FM-27 task 2)"
```
Verify with `git show --stat HEAD` (4 files).

---

### Task 3: Cross-database orchestration (`merge::merge_users`)

**Files:**
- Modify: `server/src/merge.rs` (add the orchestrator)
- Test: `server/tests/merge_test.rs` (append)

**Interfaces:**
- Consumes: Task 2's `Committers::merge_users`; `db::{list_databases, database_exists}`; `AppState` (`state.realtime.committers`, `state.pool`).
- Produces (used by Tasks 4–5):
```rust
#[derive(Debug, Clone, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MergeReport {
    pub dbs: BTreeMap<String, MergeDbResult>,
    pub storage_repointed: u64,
    pub sessions_repointed: u64,
    pub anon_deleted: bool,
}

pub async fn merge_users(
    state: &std::sync::Arc<crate::AppState>,
    anon_id: &str,
    real_id: &str,
) -> Result<MergeReport, RtDbError>
```

- [ ] **Step 1: Write the failing test**

Append to `server/tests/merge_test.rs`:

```rust
#[tokio::test]
async fn merge_users_orchestrates_sessions_storage_and_guarded_delete() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = wrap_test_db(&state).await;
    push_schema(&state.pool, &db, owned_schema()).await?;

    // Mint a real anon user + session directly (mirrors the /auth/anonymous
    // handler's INSERTs — see auth/provider.rs anonymous()).
    let anon_id = format!("anon_{}", uuid::Uuid::now_v7().simple());
    let real_id = format!("real_{}", uuid::Uuid::now_v7().simple());
    let anon_token = format!("tok_{}", uuid::Uuid::now_v7().simple());
    let now = rtdb_server::db::now_ms();
    sqlx::query("INSERT INTO rtdb_auth.users (id, login, email, anonymous, created_at) VALUES ($1, 'anonymous', NULL, TRUE, $2)")
        .bind(&anon_id).bind(now).execute(&state.pool).await?;
    sqlx::query("INSERT INTO rtdb_auth.users (id, login, email, anonymous, created_at) VALUES ($1, 'github', $2, FALSE, $3)")
        .bind(&real_id).bind("x@example.com").bind(now).execute(&state.pool).await?;
    sqlx::query("INSERT INTO rtdb_auth.sessions (token_hash, user_id, created_at, expires_at) VALUES ($1, $2, $3, $4)")
        .bind(rtdb_server::db::sha256_hex(&anon_token)).bind(&anon_id).bind(now).bind(now + 86_400_000)
        .execute(&state.pool).await?;
    // An owned doc + a storage blob owned by the anon user.
    state.realtime.committers.mutate(&db, None,
        insert_doc("docs", json!({ "title": "a", "owner": anon_id, "editors": [] })), PrincipalCtx::bypass()).await?;
    // storage table: mirror storage.rs's ensure + insert (owner_id column).
    // If a helper exists in storage.rs to create the table for a db, use it;
    // else create it with the same DDL storage.rs uses.

    let report = rtdb_server::merge::merge_users(&state, &anon_id, &real_id).await?;
    assert_eq!(report.dbs.get(&db).and_then(|r| r.tables.get("docs")), Some(&1));
    assert_eq!(report.sessions_repointed, 1);
    assert!(report.anon_deleted);

    // The session token now resolves to the REAL user (re-point promoted it).
    match rtdb_server::auth::resolve_bearer(&state.pool, &anon_token).await? {
        rtdb_server::auth::Principal::User { user_id, anonymous, .. } => {
            assert_eq!(user_id, real_id);
            assert!(!anonymous);
        }
        other => panic!("expected user principal, got {other:?}"),
    }
    // Guarded delete: the anon row is gone; a re-run is a no-op.
    let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM rtdb_auth.users WHERE id = $1")
        .bind(&anon_id).fetch_one(&state.pool).await?;
    assert_eq!(n, 0);
    let again = rtdb_server::merge::merge_users(&state, &anon_id, &real_id).await?;
    assert_eq!(again.sessions_repointed, 0);
    assert!(!again.anon_deleted);

    // Refusal: a non-anon (real) source row is rejected.
    let real2 = format!("real2_{}", uuid::Uuid::now_v7().simple());
    sqlx::query("INSERT INTO rtdb_auth.users (id, login, email, anonymous, created_at) VALUES ($1, 'github', $2, FALSE, $3)")
        .bind(&real2).bind("y@example.com").bind(now).execute(&state.pool).await?;
    assert!(rtdb_server::merge::merge_users(&state, &real_id, &real2).await.is_err());
    Ok(())
}
```

Check the real column lists of `rtdb_auth.users`/`rtdb_auth.sessions` in `db.rs`/`session.rs` before writing the INSERTs (e.g. `login` vs `provider`, whether `sessions` keys on `token_hash` vs `token`) and mirror them exactly; the behavioral assertions are the contract.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path ~/Repos/par-rt-db/server/Cargo.toml --test merge_test orchestrates`
Expected: FAIL — `merge::merge_users` / `MergeReport` not defined.

- [ ] **Step 3: Implement the orchestrator**

Add to `server/src/merge.rs`:

```rust
use std::sync::Arc;

use crate::error::RtDbError;

/// Full-instance merge outcome across every database plus the auth/storage
/// steps. Returned by `POST /admin/merge-users` and logged by the OAuth
/// callback hook.
#[derive(Debug, Clone, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MergeReport {
    pub dbs: BTreeMap<String, MergeDbResult>,
    pub storage_repointed: u64,
    pub sessions_repointed: u64,
    pub anon_deleted: bool,
}

/// Anon→real merge, crash-safe by ordering (spec §"Merge order"):
/// 1. document re-stamps per db, each inside that db's committer turn;
/// 2. storage blob owner swap per db (direct SQL — storage bypasses the
///    committer by design);
/// 3. session re-point (`UPDATE ... SET user_id`, NOT delete — an open WS or
///    stored SDK token promotes to the real principal on its next op);
/// 4. guarded anon-row delete (`AND anonymous = TRUE` makes re-runs inert).
/// Any interruption is recovered by signing in again: every step is
/// idempotent and `/begin` re-records the binding while the anon row exists.
pub async fn merge_users(
    state: &Arc<crate::AppState>,
    anon_id: &str,
    real_id: &str,
) -> Result<MergeReport, RtDbError> {
    // Guard: the source row must exist and be anonymous. This both refuses
    // admin mistakes (real→real) and makes the callback path idempotent.
    let row: Option<(bool,)> =
        sqlx::query_as("SELECT anonymous FROM rtdb_auth.users WHERE id = $1")
            .bind(anon_id)
            .fetch_optional(&state.pool)
            .await?;
    match row {
        Some((true,)) => {}
        Some((false,)) => {
            return Err(RtDbError::bad_request(
                "source user is not anonymous; refusing merge",
            ));
        }
        None => return Err(RtDbError::bad_request("anonymous user not found")),
    }

    let mut report = MergeReport::default();
    for db in crate::db::list_databases(&state.pool).await? {
        match state.realtime.committers.merge_users(&db, anon_id, real_id).await {
            Ok(res) => {
                report.dbs.insert(db.clone(), res);
            }
            Err(err) => {
                // A deleted-mid-flight db or a torn-down committer is not a
                // merge failure; everything else propagates.
                if crate::db::database_exists(&state.pool, &db).await.unwrap_or(false) {
                    return Err(err);
                }
            }
        }

        // Storage owner swap. The table is lazy-created; a db with no uploads
        // yet has no relation — treat undefined_table (42P01) as zero rows.
        let schema_name = crate::ddl::pg_schema(&db);
        let swapped = sqlx::query(&format!(
            "UPDATE \"{schema_name}\".\"storage\" SET \"owner_id\" = $1 WHERE \"owner_id\" = $2"
        ))
        .bind(real_id)
        .bind(anon_id)
        .execute(&state.pool)
        .await;
        match swapped {
            Ok(res) => report.storage_repointed += res.rows_affected(),
            Err(err)
                if err
                    .as_database_error()
                    .and_then(|d| d.code().as_deref())
                    .is_some_and(|c| c == "42P01") => {}
            Err(err) => {
                if crate::db::database_exists(&state.pool, &db).await.unwrap_or(false) {
                    return Err(err.into());
                }
            }
        }
    }

    let repointed = sqlx::query(
        "UPDATE rtdb_auth.sessions SET user_id = $1 WHERE user_id = $2",
    )
    .bind(real_id)
    .bind(anon_id)
    .execute(&state.pool)
    .await?;
    report.sessions_repointed = repointed.rows_affected();

    let deleted = sqlx::query(
        "DELETE FROM rtdb_auth.users WHERE id = $1 AND anonymous = TRUE",
    )
    .bind(anon_id)
    .execute(&state.pool)
    .await?;
    report.anon_deleted = deleted.rows_affected() == 1;

    Ok(report)
}
```

(If the sqlx error is already converted to `RtDbError` at that point, adapt the 42P01 match to whatever `error.rs` exposes; the behavior — undefined_table on the storage table is zero rows — is the contract.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --manifest-path ~/Repos/par-rt-db/server/Cargo.toml --test merge_test`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git -C ~/Repos/par-rt-db add server/src/merge.rs server/tests/merge_test.rs
git -C ~/Repos/par-rt-db commit -m "feat(merge): cross-db orchestration + storage swap + session re-point + guarded delete (FM-27 task 3)"
```

---

### Task 4: Admin escape hatch `POST /admin/merge-users`

**Files:**
- Create: `server/src/admin/merge.rs`
- Modify: `server/src/admin/mod.rs` (`mod merge;` + route registration next to `/admin/delete-db`)

**Interfaces:**
- Consumes: Task 3's `merge::merge_users` + `MergeReport`; `require_admin` (admin/mod.rs:134; note `require_admin_mw` already gates the router — the handler needs no inline gate, but mirror what sibling handlers like `delete_db` do and keep parity with them).
- Produces: `POST /admin/merge-users` body `{ "anonUserId": "...", "realUserId": "...", "confirm": "..." }` → 200 `MergeReport` JSON. `confirm != realUserId` → 400; missing/non-anon anon row → 400 (from `merge_users`).

- [ ] **Step 1: Write the failing test**

Append to `server/tests/merge_test.rs` (mirror the admin-HTTP test pattern from `admin_test.rs` — use its `spawn_app`/admin-request helpers from `common`):

```rust
#[tokio::test]
async fn admin_merge_users_endpoint_requires_confirm_and_runs_merge() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = wrap_test_db(&state).await;
    push_schema(&state.pool, &db, owned_schema()).await?;
    let addr = common::spawn_app(state.clone()).await;

    let anon_id = format!("anon_{}", uuid::Uuid::now_v7().simple());
    let real_id = format!("real_{}", uuid::Uuid::now_v7().simple());
    let now = rtdb_server::db::now_ms();
    sqlx::query("INSERT INTO rtdb_auth.users (id, login, email, anonymous, created_at) VALUES ($1, 'anonymous', NULL, TRUE, $2)")
        .bind(&anon_id).bind(now).execute(&state.pool).await?;
    sqlx::query("INSERT INTO rtdb_auth.users (id, login, email, anonymous, created_at) VALUES ($1, 'github', $2, FALSE, $3)")
        .bind(&real_id).bind("z@example.com").bind(now).execute(&state.pool).await?;
    state.realtime.committers.mutate(&db, None,
        insert_doc("docs", json!({ "title": "a", "owner": anon_id, "editors": [] })), PrincipalCtx::bypass()).await?;

    let client = reqwest::Client::new();

    // Wrong confirm -> 400, nothing merged.
    let resp = client.post(format!("http://{addr}/admin/merge-users"))
        .header("authorization", "Bearer test-admin-key")
        .json(&json!({ "anonUserId": anon_id, "realUserId": real_id, "confirm": "nope" }))
        .send().await?;
    assert_eq!(resp.status(), 400);

    // Correct confirm -> report.
    let resp = client.post(format!("http://{addr}/admin/merge-users"))
        .header("authorization", "Bearer test-admin-key")
        .json(&json!({ "anonUserId": anon_id, "realUserId": real_id, "confirm": real_id }))
        .send().await?;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["dbs"][&db]["tables"]["docs"], json!(1));
    assert_eq!(body["anonDeleted"], json!(true));

    // Metric visible on the Prometheus scrape.
    let metrics = client.get(format!("http://{addr}/metrics")).send().await?.text().await?;
    assert!(metrics.contains("rtdb_merge_docs_total 1"));

    // Unauthorized without the admin key.
    let resp = client.post(format!("http://{addr}/admin/merge-users"))
        .json(&json!({ "anonUserId": anon_id, "realUserId": real_id, "confirm": real_id }))
        .send().await?;
    assert_eq!(resp.status(), 401);
    Ok(())
}
```

(Adapt the auth header/key to what `admin_test.rs` actually uses — `test-admin-key` per `common::test_config()`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path ~/Repos/par-rt-db/server/Cargo.toml --test merge_test admin_merge`
Expected: FAIL — route doesn't exist (404/405 or handler mismatch).

- [ ] **Step 3: Implement the handler**

`server/src/admin/merge.rs`:

```rust
//! FM-27 admin escape hatch: run the anon→real merge synchronously and get
//! the full report. Use: crash-window cleanup (the inert-orphan case between
//! steps 3 and 4 of the merge order), manual consolidation, testing.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;

use crate::AppState;
use crate::error::RtDbError;
use crate::merge;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MergeUsersBody {
    anon_user_id: String,
    real_user_id: String,
    confirm: String,
}

pub(crate) async fn merge_users_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<MergeUsersBody>,
) -> Response {
    // Typed-confirm guard, same pattern as delete-db/restore.
    if body.confirm != body.real_user_id {
        return RtDbError::bad_request("confirm must equal realUserId").into_response();
    }
    match merge::merge_users(&state, &body.anon_user_id, &body.real_user_id).await {
        Ok(report) => (StatusCode::OK, Json(report)).into_response(),
        Err(err) => err.into_response(),
    }
}
```

In `server/src/admin/mod.rs`: add `mod merge;` to the module list (line ~15) and register the route exactly where `/admin/delete-db` is registered (~line 268):
```rust
.route("/admin/merge-users", post(merge::merge_users_handler))
```
 exporting the handler from the module as sibling handlers do (`pub(crate) async fn` + whatever re-export path the router uses — mirror `sessions` or `dbs`).

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --manifest-path ~/Repos/par-rt-db/server/Cargo.toml --test merge_test`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git -C ~/Repos/par-rt-db add server/src/admin/merge.rs server/src/admin/mod.rs server/tests/merge_test.rs
git -C ~/Repos/par-rt-db commit -m "feat(admin): POST /admin/merge-users escape hatch with typed confirm (FM-27 task 4)"
```

---

### Task 5: OAuth trigger wiring (`anon_user_id` binding + callback merge)

**Files:**
- Modify: `server/src/db.rs` (oauth_states `anon_user_id` column, idempotent ALTER at the CREATE TABLE site ~line 238)
- Modify: `server/src/auth/provider.rs` (`provider_begin` binding, `claim_pending` RETURNING, `provider_callback` + `apple_callback` hooks, `merge_anon_into_real` helper)

**Interfaces:**
- Consumes: Task 3's `merge::merge_users`; `auth::resolve_bearer`; existing `bearer_token(headers)` (provider.rs ~208), `set_outcome` (~262).
- Produces: no new public surface; behavior only.

- [ ] **Step 1: Add the column**

In `db.rs`, immediately after the `oauth_states` CREATE TABLE statement (~line 247), add the idempotent migration for existing installs (same pattern as storage.rs's `owner_id` retrofit):

```rust
sqlx::query(
    "ALTER TABLE rtdb_auth.oauth_states ADD COLUMN IF NOT EXISTS anon_user_id text",
)
.execute(&mut *conn)
.await?;
```

- [ ] **Step 2: Bind the anon caller at `/begin`**

In `provider.rs::provider_begin` (line ~393): before the INSERT, resolve the caller and, if the session is anonymous, bind the user id. Add to the INSERT's column list and VALUES:

```rust
// FM-27: if the caller holds an anonymous session, record its user id so the
// callback can merge the anon footprint into the real account after login.
// Server-side resolution of a verified session — never caller-supplied.
let anon_user_id = match bearer_token(&headers).map(|t| t.to_string()) {
    Some(token) => match crate::auth::resolve_bearer(&state.pool, &token).await {
        Ok(crate::auth::Principal::User { anonymous: true, user_id, .. }) => Some(user_id),
        _ => None,
    },
    None => None,
};
```

INSERT becomes (mirroring the existing statement's bind style):
```rust
sqlx::query(
    "INSERT INTO rtdb_auth.oauth_states \
     (state, provider, status, created_at, expires_at, anon_user_id) \
     VALUES ($1, $2, $3, $4, $5, $6)",
)
// ...existing binds...
.bind(&anon_user_id)
```

- [ ] **Step 3: Return the binding from `claim_pending`**

Change `claim_pending` (~line 236) from returning `bool` to `Option<Option<String>>` — outer `None` = claim failed, inner = the row's `anon_user_id`:

```rust
async fn claim_pending(
    state: &Arc<AppState>,
    state_token: &str,
    expected_provider: &str,
) -> Option<Option<String>> {
    let now = crate::db::now_ms();
    let row: Option<(Option<String>,)> = sqlx::query(
        "UPDATE rtdb_auth.oauth_states SET status = $1 \
         WHERE state = $2 AND provider = $3 AND status = $4 AND expires_at > $5 \
         RETURNING anon_user_id",
    )
    .bind(STATUS_CLAIMING)
    .bind(state_token)
    .bind(expected_provider)
    .bind(STATUS_PENDING)
    .bind(now)
    .fetch_optional(&state.pool)
    .await
    .ok()
    .flatten();
    row.map(|(anon_user_id,)| anon_user_id)
}
```

(Keep the existing function's parameter list and constants exactly; only the statement, return type, and body change.)

- [ ] **Step 4: Add the merge helper**

In `provider.rs`:

```rust
/// FM-27: after a successful provider login whose state row was minted from
/// an anonymous session, synchronously merge the anon footprint into the real
/// account BEFORE `set_outcome` records the terminal state (so a crash before
/// the merge simply leaves the login pending and the next sign-in re-runs it —
/// every merge step is idempotent). Merge failures are logged at ERROR and
/// never fail the login.
async fn merge_anon_into_real(state: &Arc<AppState>, anon_id: &str, session_token: &str) {
    let real_id = match crate::auth::resolve_bearer(&state.pool, session_token).await {
        Ok(crate::auth::Principal::User { user_id, .. }) if user_id != anon_id => user_id,
        Ok(_) => return, // anon row was already merged away; nothing to do
        Err(err) => {
            tracing::error!(error = %err, "anon merge: could not resolve the fresh session");
            return;
        }
    };
    if let Err(err) = crate::merge::merge_users(state, anon_id, &real_id).await {
        tracing::error!(
            anon = %anon_id,
            real = %real_id,
            error = %err,
            "anon->real merge failed; recovered by the next sign-in"
        );
    }
}
```

- [ ] **Step 5: Hook both callbacks**

In `provider_callback` (~line 491): change the claim to capture the binding and call the helper between `complete_login` and `set_outcome`:

```rust
let anon_user_id = match claim_pending(&state, &params.state, P::name()).await {
    Some(binding) => binding,
    None => return /* the existing invalid/expired-state error response, unchanged */,
};
match provider.complete_login(&state, &params.code).await {
    Ok(token) => {
        if let Some(anon_id) = &anon_user_id {
            merge_anon_into_real(&state, anon_id, &token).await;
        }
        set_outcome(&state, &params.state, Some(&token)).await;
        callback_close_response(&token, secure)
    }
    Err(err) => {
        set_outcome(&state, &params.state, None).await;
        /* existing error response, unchanged */
    }
}
```

Apply the identical change to `apple_callback` (~line 538; its claim uses the Apple provider name and `params.code` — same insertion point between `complete_login` and `set_outcome`).

- [ ] **Step 6: Write the failing e2e test**

Append to `server/tests/merge_test.rs` — full anonymous→GitHub-wiremock→merge flow, using the harness pattern from `oauth_test.rs` (helpers `oauth_state`, `mount_github_user_mocks`, `extract_token_from_cookie`, `no_redirect_client`, `begin_login` live in that file; either re-declare them in merge_test.rs or factor them into `tests/common/mod.rs` — prefer copying into merge_test.rs to keep the diff surgical):

```rust
use wiremock::{MockServer, ResponseTemplate};
use wiremock::matchers::{method, path};

#[tokio::test]
async fn oauth_login_merges_anon_footprint_end_to_end() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    let mut cfg = common::test_config();
    cfg.github_base_url = mock.uri();
    cfg.github_api_url = mock.uri();
    cfg.github_client_id = Some("test-client".into());
    cfg.github_client_secret = Some("test-secret".into());
    cfg.auth_anonymous_enabled = true;

    let pool = sqlx::PgPool::connect(&cfg.database_url).await?;
    db::bootstrap(&pool).await?;
    let state = Arc::new(AppState::new(pool, cfg, common::test_hot()));
    let addr = common::spawn_app(state.clone()).await;

    // Distinct github id so parallel tests don't collide in rtdb_auth.users.
    mount_github_user_mocks(&mock, 4201, "mergeuser", json!([
        { "email": "mergeuser@example.com", "verified": true, "primary": true }
    ])).await;

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .cookie_store(true)
        .build()?;

    // 1. Mint the anonymous session.
    let resp = client.post(format!("http://{addr}/auth/anonymous")).send().await?;
    assert_eq!(resp.status(), 200);
    let anon_body: serde_json::Value = resp.json().await?;
    let anon_token = anon_body["token"].as_str().expect("anon session token").to_string();

    // 2. Create a db + owned doc as the anon user.
    let db_name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &db_name).await?;
    push_schema(&state.pool, &db_name, owned_schema()).await?;
    let anon_id = match rtdb_server::auth::resolve_bearer(&state.pool, &anon_token).await? {
        rtdb_server::auth::Principal::User { user_id, .. } => user_id,
        _ => panic!("anon principal"),
    };
    state.realtime.committers.mutate(&db_name, None,
        insert_doc("docs", json!({ "title": "guest work", "owner": anon_id, "editors": [] })),
        rtdb_server::auth::PrincipalCtx { user_id: Some(anon_id.clone()), ..Default::default() },
    ).await?;

    // 3. Begin GitHub login FROM the anon session (cookie jar carries it).
    let state_token = begin_login(&client, addr, "http://localhost:5173").await;
    // The binding was recorded server-side.
    let (bound,): (Option<String>,) = sqlx::query_as(
        "SELECT anon_user_id FROM rtdb_auth.oauth_states WHERE state = $1",
    ).bind(&state_token).fetch_one(&state.pool).await?;
    assert_eq!(bound.as_deref(), Some(anon_id.as_str()));

    // 4. Complete the login via the wiremock callback.
    let resp = client.get(format!(
        "http://{addr}/auth/callback?code=any-code&state={state_token}"
    )).send().await?;
    assert_eq!(resp.status(), 200);

    // 5. Assertions: doc re-stamped to the real user; anon row gone; the
    //    anon token now resolves as the real (non-anonymous) user.
    let (real_row,): (String,) = sqlx::query_as(
        "SELECT id FROM rtdb_auth.users WHERE email = 'mergeuser@example.com'",
    ).fetch_one(&state.pool).await?;
    let (n,): (i64,) = sqlx::query_as(
        &format!("SELECT COUNT(*) FROM \"{}\".\"t_docs\" WHERE \"doc\"->'owner' = to_jsonb($1::text)", db::pg_schema(&db_name)),
    ).bind(&real_row).fetch_one(&state.pool).await?;
    assert_eq!(n, 1);

    let (gone,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM rtdb_auth.users WHERE id = $1",
    ).bind(&anon_id).fetch_one(&state.pool).await?;
    assert_eq!(gone, 0);

    match rtdb_server::auth::resolve_bearer(&state.pool, &anon_token).await? {
        rtdb_server::auth::Principal::User { user_id, anonymous, .. } => {
            assert_eq!(user_id, real_row);
            assert!(!anonymous);
        }
        other => panic!("expected user principal, got {other:?}"),
    }
    Ok(())
}
```

(Match the real field names — `PrincipalCtx` construction, `db::create_database`'s signature, the anon token's response body key, `begin_login`'s origin handling — against the source; behavioral assertions are the contract. Reuse `insert_doc`/`owned_schema` already in this file.)

- [ ] **Step 7: Run test to verify it fails, then implement is already done — verify it passes**

Steps 2–5 are the implementation; run the e2e now:
Run: `cargo test --manifest-path ~/Repos/par-rt-db/server/Cargo.toml --test merge_test end_to_end`
Expected: PASS. If the binding assertion (step 3 of the test) fails, check `/begin` is reached with the cookie jar (the anon cookie must ride the GET — `cookie_store(true)` handles it) and that `bearer_token(headers)` reads the `rtdb_session` cookie.

- [ ] **Step 8: Commit (auth-touching — flag for manual review)**

```bash
git -C ~/Repos/par-rt-db add server/src/db.rs server/src/auth/provider.rs server/tests/merge_test.rs
git -C ~/Repos/par-rt-db commit -m "feat(auth): bind anon session at OAuth /begin and merge into the real account at callback (FM-27 task 5)"
```
This commit changes auth behavior — name it in the final report for manual review.

---

### Task 6: Docs + full gate

**Files:**
- Modify: `FEATURE_MATRIX.md` (row 7/39 auth row — strike "the anon→real merge on a later OAuth sign-in is a follow-up", replace with shipped description incl. `POST /admin/merge-users` and `rtdb_merge_docs_total`)
- Modify: `CLAUDE.md` (committer tap-site list in "Op-feed tap" invariant: add `handle_merge_users`; auth section: replace "The anon→real merge on a later OAuth sign-in is a follow-up (not yet shipped)" with the shipped behavior + spec link)
- Modify: `server/src/auth/provider.rs` doc comment on the `anonymous` handler (~line 743): strike "The anon→real merge on a later OAuth sign-in is a follow-up."
- Modify: `README.md` (root) auth section: one sentence on the merge behavior + the admin endpoint (check which README documents anonymous auth — root and/or `server/README.md` — and update where it lives).

- [ ] **Step 1: Update the four doc sites**

Each edit is a sentence-level strike-and-replace; keep the surrounding text intact. CLAUDE.md tap-site list currently reads "`handle_mutate`, `handle_scheduled`, `handle_migrate`, `handle_reaper`, and `handle_restore_schema`" — append `handle_merge_users` and note `source = "merge"`.

- [ ] **Step 2: Run the full gate**

```bash
make -C ~/Repos/par-rt-db dev-db-up
make -C ~/Repos/par-rt-db checkall
```
Expected: green (fmt + clippy `-D warnings` + typecheck + all tests, including the 6 new merge tests). Check the exit code directly (`echo $?` right after), not through a pipe.

- [ ] **Step 3: Commit**

```bash
git -C ~/Repos/par-rt-db add FEATURE_MATRIX.md CLAUDE.md README.md server/src/auth/provider.rs
git -C ~/Repos/par-rt-db commit -m "docs: anon-to-real merge shipped across FEATURE_MATRIX/CLAUDE.md/README (FM-27)"
```

- [ ] **Step 4: Board + acceptance criteria**

Card `[FM-27]` (id `01a00347811e743095ba735d549b7049`). Check each criterion individually with `kanban item check --id 01a00347811e743095ba735d549b7049 --criterion <N|text> --note "<evidence>"`:
1. "OAuth login with pre-existing anon user re-stamps owned docs across all databases inside the committer turn" — evidence: `merge_users_restamps...` + `oauth_login_merges_anon_footprint_end_to_end` green.
2. "merge fires subscription fan-out + op-feed (no silent merge)" — evidence: `merge_users_fires_subscription_fan_out` green; op-feed rides the same `publish_taps(source="merge")` call (documented in CLAUDE.md tap list).
3. "integration test covers anon writes doc → OAuth sign-in → real session read/write + anon session retired" — evidence: the e2e test asserts the doc restamp, the promoted anon token, and the deleted anon row.
4. "make checkall green" — evidence: gate output from Step 2.
Then `kanban item done --id 01a00347811e743095ba735d549b7049`.
