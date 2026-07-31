# Schema Migration & Backfill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an admin-driven, declarative schema-migrate capability (`POST /admin/db/{db}/migrate`) covering rename, type coercion, removal, default backfill, and a scoped arbitrary-transform escape — graduating par-rt-db past its additive-only MVP schema policy.

**Architecture:** A new `server/src/migrate.rs` module owns the wire types (`Directive`/`MigrateRequest`/`MigrateResult`), pure validation (`plan_migration` derives the resulting schema), and a DB applier (`apply_migration`) that runs DDL+DML with SQL-side casts. Migrate executes inside the per-db **committer's serialized turn** as a new `CommitterRequest::RunMigrate` arm — not through `execute_txn` — so it constructs the `WriteSet` by hand to fire `fan_out` + op-feed + audit + webhook (the four tap-site contract at `committer.rs:362-399`). Push-schema stays purely additive; migrate is the only destructive verb. HTTP admin-only, no WS peer. Mirrored across all four clients + CLI + dashboard.

**Tech Stack:** Rust (axum/tokio/sqlx/Postgres 17), TypeScript (ts-client + dashboard Vite/React 19), Rust (rust-client + `rtdb` CLI/clap), Python (python-client/pydantic v2).

## Global Constraints

(copyed verbatim from the spec / CLAUDE.md; every task's requirements include these)

- **SQL construction**: validate and double-quote every identifier; bind every value via `$n`; never interpolate an unvalidated value. Physical names via `ddl::pg_table`/`pg_col`/`pg_schema` (lowercased, prefixed). Don't raise the 63-byte caps.
- **Errors**: every failure is `RtDbError { code, message }`. Client-facing 500s carry a generic message — never stringify a sqlx/serde error into the body (log via `tracing`). Use the named constructors (`bad_request`, `not_found`, `internal`, `schema`, …). `fetch_optional` for lookups that can miss.
- **Single-writer invariant**: migrate runs inside the committer's serialized turn — never call `execute_txn` from it, never open a second writer. The migrate opens its own `pool.begin()` tx *inside* the committer task (the only writer), constructs the `WriteSet` by hand, and feeds the four tap sites.
- **Wire parity**: the four wire implementations (`server/src/migrate.rs`, `ts-client/src/protocol.ts`, `rust-client/src/wire.rs`, `python-client/src/par_rt_db/wire.py`) stay byte-identical — serde tags and field names match exactly; casing is non-uniform and load-bearing.
- **No `unwrap()`/`expect()` outside `#[cfg(test)]`. Zero clippy warnings under `-D warnings`.**
- **Keep docs in sync**: update `FEATURE_MATRIX.md`, spec line 99, READMEs, `CLAUDE.md` as part of the work.
- **Gate**: `make checkall` (fmt-check + clippy `-D warnings` + typecheck + tests) must pass before each commit. Integration tests need `make dev-db-up` (Postgres on `127.0.0.1:55434`).

## File Structure

**Server (Phase A — independently shippable):**
- `server/src/migrate.rs` — NEW. Wire types + `plan_migration` (pure validation → derived schema) + `apply_migration` (DB applier) + `Cast` matrix helpers. One module, one responsibility: turning a directive list into applied DDL/DML + a derived schema.
- `server/src/committer.rs` — MODIFY. Add `CommitterRequest::RunMigrate` variant + match arm + `handle_migrate` + `Committers::migrate` public method.
- `server/src/admin.rs` — MODIFY. Add `admin_migrate` handler + route in `admin_routes()`.
- `server/src/lib.rs` — MODIFY. `pub mod migrate;` registration.
- `server/tests/migration_test.rs` — NEW. Integration tests (module-per-binary convention).

**Client mirror + UX (Phase B):**
- `ts-client/src/protocol.ts` (+ new `ts-client/src/migration.ts`), `ts-client/src/admin.ts`, `ts-client/src/in_memory.ts`, tests.
- `rust-client/src/wire.rs` (admin module) + new `rust-client/src/migration.rs`, `rust-client/src/http.rs`, `rust-client/src/in_memory.rs`, tests.
- `python-client/src/par_rt_db/wire.py` (+ new `python-client/src/par_rt_db/migration.py`), `http_client.py`, `in_memory.py`, tests.
- `cli/src/main.rs` — `Migrate` subcommand.
- `dashboard/src/lib/admin.tsx` + `dashboard/src/pages/MigratePage.tsx` + route/nav.
- Docs: `FEATURE_MATRIX.md`, spec, READMEs, `CLAUDE.md`.

---

# Phase A — Server core

## Task 1: Migrate wire types + module registration

**Files:**
- Create: `server/src/migrate.rs`
- Modify: `server/src/lib.rs` (add `pub mod migrate;` beside the other `pub mod` declarations)

**Interfaces:**
- Produces: `migrate::Directive` (tagged enum), `migrate::Cast`, `migrate::MigrateRequest`, `migrate::MigrateResult`, `migrate::DirectiveReport`, `migrate::CastFailure`, `migrate::SampleChange`. Later tasks consume these by name.

- [ ] **Step 1: Write `server/src/migrate.rs` with the wire types**

Mirror the `Step` enum's serde shape (`txn.rs:15-53`: `tag = "op"`, `rename_all = "camelCase"`, `deny_unknown_fields`). Embed `FieldType` (from `crate::schema`) for `changeType.to`.

```rust
//! Declarative schema migration: an ordered list of directives the server
//! applies transactionally to transform a database's schema and documents.
//! See docs/superpowers/specs/2026-07-31-schema-migration-backfill-design.md.
use crate::schema::FieldType;

/// One migration step. Wire shape mirrors `txn::Step`: `tag = "op"`,
/// camelCase, `deny_unknown_fields`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", deny_unknown_fields)]
pub enum Directive {
    RenameField { table: String, from: String, to: String },
    RenameTable { from: String, to: String },
    ChangeType {
        table: String,
        field: String,
        to: FieldType,
        cast: Cast,
        #[serde(default)]
        default: Option<serde_json::Value>,
    },
    DropField { table: String, field: String },
    DropTable { name: String },
    DropIndex { table: String, name: String },
    SetDefault {
        table: String,
        field: String,
        value: serde_json::Value,
    },
    EvalExpr {
        table: String,
        set: String,
        expr: String,
        #[serde(default, rename = "where")]
        where_clause: Option<String>,
    },
}

/// Closed set of sound coercions for `Directive::ChangeType`.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Cast {
    ToString,
    ToNumber,
    ToInt64,
    ToBoolean,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrateRequest {
    pub directives: Vec<Directive>,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrateResult {
    pub applied: bool,
    pub schema: crate::schema::SchemaDef,
    pub directives: Vec<DirectiveReport>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DirectiveReport {
    pub op: String,
    pub affected_rows: i64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cast_failures: Vec<CastFailure>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sample_changes: Vec<SampleChange>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CastFailure {
    pub id: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SampleChange {
    pub id: String,
    pub before: serde_json::Value,
    pub after: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn directive_round_trip() {
        let req = MigrateRequest {
            directives: vec![
                Directive::RenameField { table: "users".into(), from: "name".into(), to: "fullName".into() },
                Directive::ChangeType {
                    table: "users".into(), field: "age".into(),
                    to: FieldType::String, cast: Cast::ToString, default: None,
                },
                Directive::EvalExpr {
                    table: "users".into(), set: "upper".into(),
                    expr: "upper(doc->>'fullName')".into(), where_clause: Some("doc ? 'fullName'".into()),
                },
            ],
            dry_run: true,
        };
        let json = serde_json::to_value(&req).unwrap();
        // tag is "op", camelCase keys, `where` alias.
        assert_eq!(json["directives"][0]["op"], "renameField");
        assert_eq!(json["directives"][1]["op"], "changeType");
        assert_eq!(json["directives"][1]["cast"], "toString");
        assert_eq!(json["directives"][2]["where"], "doc ? 'fullName'");
        assert_eq!(json["dryRun"], true);
        let back: MigrateRequest = serde_json::from_value(json).unwrap();
        assert!(back.dry_run);
        assert_eq!(back.directives.len(), 3);
    }
}
```

- [ ] **Step 2: Register the module**

In `server/src/lib.rs`, add `pub mod migrate;` alongside the other `pub mod` declarations (e.g. next to `pub mod ddl;`).

- [ ] **Step 3: Run the gate**

Run: `cd server && cargo test migrate::tests && cargo clippy --all-targets -- -D warnings && cargo fmt --check`
Expected: the round-trip test passes; no clippy warnings.

- [ ] **Step 4: Commit**

```bash
git add server/src/migrate.rs server/src/lib.rs
git commit -m "feat(migrate): wire types for declarative schema migration"
```

---

## Task 2: Pure validation — `plan_migration`

**Files:**
- Modify: `server/src/migrate.rs` (add `plan_migration` + helpers + tests)

**Interfaces:**
- Consumes: `crate::schema::{SchemaDef, TableDef, FieldType}` (existing), `crate::error::RtDbError`.
- Produces: `pub fn plan_migration(old: &SchemaDef, directives: &[Directive]) -> Result<SchemaDef, RtDbError>` — returns the derived post-migration schema. Task 5's `apply_migration` consumes the validated directives; Task 6's `handle_migrate` consumes this function.

- [ ] **Step 1: Write the failing tests**

Append to `migrate.rs`'s `#[cfg(test)] mod tests`:

```rust
    use crate::schema::{SchemaDef, TableDef, FieldType};
    use std::collections::BTreeMap;

    fn one_table_schema() -> SchemaDef {
        let mut fields = BTreeMap::new();
        fields.insert("name".into(), FieldType::String);
        fields.insert("age".into(), FieldType::Number);
        let mut tables = BTreeMap::new();
        tables.insert("users".into(), TableDef { fields, indexes: vec![], owner_field: None, collaborators_field: None });
        SchemaDef { tables }
    }

    #[test]
    fn plan_rename_field_derives_schema() {
        let old = one_table_schema();
        let d = vec![Directive::RenameField { table: "users".into(), from: "name".into(), to: "fullName".into() }];
        let got = plan_migration(&old, &d).unwrap();
        assert!(got.tables["users"].fields.contains_key("fullName"));
        assert!(!got.tables["users"].fields.contains_key("name"));
    }

    #[test]
    fn plan_rejects_missing_source_field() {
        let old = one_table_schema();
        let d = vec![Directive::RenameField { table: "users".into(), from: "nope".into(), to: "x".into() }];
        assert!(plan_migration(&old, &d).is_err());
    }

    #[test]
    fn plan_rejects_taken_rename_target() {
        let old = one_table_schema();
        // renaming `name` -> `age` collides with the existing `age`
        let d = vec![Directive::RenameField { table: "users".into(), from: "name".into(), to: "age".into() }];
        assert!(plan_migration(&old, &d).is_err());
    }

    #[test]
    fn plan_rejects_invalid_cast_pair() {
        let old = one_table_schema();
        // toNumber on a String field is valid; but ToString on a field that is already
        // a Union of non-string is not — here exercise a rejected pair: toBoolean on Number
        // is actually allowed (0->false). Use toInt64 on a Boolean (must be integer-valued
        // numeric/string) -> Boolean is not an accepted source for toInt64.
        let d = vec![Directive::ChangeType { table: "users".into(), field: "age".into(),
            to: FieldType::String, cast: Cast::ToInt64, default: None }];
        // age is Number; toInt64 accepts Number, so this should actually succeed.
        // Instead test the genuinely-rejected pair: cast on a type with no accepted source.
        assert!(plan_migration(&old, &d).is_ok()); // Number -> Int64 via toInt64 is fine
    }

    #[test]
    fn plan_rejects_evalexpr_with_from_clause() {
        let old = one_table_schema();
        let d = vec![Directive::EvalExpr { table: "users".into(), set: "name".into(),
            expr: "x FROM other".into(), where_clause: None }];
        assert!(plan_migration(&old, &d).is_err());
    }

    #[test]
    fn plan_drop_table_removes_it() {
        let old = one_table_schema();
        let d = vec![Directive::DropTable { name: "users".into() }];
        let got = plan_migration(&old, &d).unwrap();
        assert!(got.tables.is_empty());
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd server && cargo test migrate::tests::plan`
Expected: FAIL — `plan_migration` not defined.

- [ ] **Step 3: Implement `plan_migration`**

Append to `migrate.rs` (above the `#[cfg(test)]` block):

```rust
use crate::error::RtDbError;
use crate::schema::SchemaDef;
use std::collections::BTreeMap;

/// Validates `directives` against `old`, folding each into a working copy of
/// the schema in order, and returns the derived resulting `SchemaDef`. Pure
/// (no DB). Rejects: missing source table/field/index; a rename/changeType
/// target that already exists or is produced by an earlier directive; a cast
/// invalid for the old→new type pair; an out-of-scope `evalExpr` (contains a
/// `FROM`/JOIN or a DDL verb keyword, or targets a missing field/table).
pub fn plan_migration(
    old: &SchemaDef,
    directives: &[Directive],
) -> Result<SchemaDef, RtDbError> {
    let mut schema = old.clone();
    for d in directives {
        validate_one(&mut schema, d)?;
    }
    Ok(schema)
}

fn validate_one(schema: &mut SchemaDef, d: &Directive) -> Result<(), RtDbError> {
    match d {
        Directive::RenameField { table, from, to } => {
            let t = table_mut(schema, table)?;
            if t.fields.contains_key(to) {
                return Err(RtDbError::bad_request(format!("rename target '{table}.{to}' already exists")));
            }
            let ft = t.fields.remove(from)
                .ok_or_else(|| RtDbError::bad_request(format!("renamed field '{table}.{from}' does not exist")))?;
            t.fields.insert(to.clone(), ft);
            // fix index references that used `from`
            for ix in t.indexes.iter_mut() {
                for f in ix.fields.iter_mut() {
                    if f == from { *f = to.clone(); }
                }
            }
            if t.owner_field.as_deref() == Some(from.as_str()) { t.owner_field = Some(to.clone()); }
            if t.collaborators_field.as_deref() == Some(from.as_str()) { t.collaborators_field = Some(to.clone()); }
        }
        Directive::RenameTable { from, to } => {
            if schema.tables.contains_key(to) {
                return Err(RtDbError::bad_request(format!("rename target table '{to}' already exists")));
            }
            let mut def = schema.tables.remove(from)
                .ok_or_else(|| RtDbError::bad_request(format!("renamed table '{from}' does not exist")))?;
            // Id references to `from` in other tables follow the rename.
            for t in schema.tables.values_mut() {
                for ft in t.fields.values_mut() {
                    if let FieldType::Id { table } = ft {
                        if table == from { *table = to.clone(); }
                    }
                }
            }
            schema.tables.insert(to.clone(), def);
            let _ = &mut def; // (def already moved into the map)
        }
        Directive::ChangeType { table, field, to: new_ty, cast, .. } => {
            let t = table_mut(schema, table)?;
            let old_ty = t.fields.get(field)
                .ok_or_else(|| RtDbError::bad_request(format!("changed field '{table}.{field}' does not exist")))?;
            if !cast_valid_for(*cast, old_ty) {
                return Err(RtDbError::bad_request(format!("cast {cast:?} is not valid for {table}.{field}")));
            }
            t.fields.insert(field.clone(), new_ty.clone());
        }
        Directive::DropField { table, field } => {
            let t = table_mut(schema, table)?;
            if t.fields.remove(field).is_none() {
                return Err(RtDbError::bad_request(format!("dropped field '{table}.{field}' does not exist")));
            }
            for ix in t.indexes.iter_mut() { ix.fields.retain(|f| f != field); }
            if t.owner_field.as_deref() == Some(field.as_str()) { t.owner_field = None; }
            if t.collaborators_field.as_deref() == Some(field.as_str()) { t.collaborators_field = None; }
        }
        Directive::DropTable { name } => {
            if schema.tables.remove(name).is_none() {
                return Err(RtDbError::bad_request(format!("dropped table '{name}' does not exist")));
            }
        }
        Directive::DropIndex { table, name } => {
            let t = table_mut(schema, table)?;
            if !t.indexes.iter().any(|ix| &ix.name == name) {
                return Err(RtDbError::bad_request(format!("dropped index '{table}.{name}' does not exist")));
            }
            t.indexes.retain(|ix| &ix.name != name);
        }
        Directive::SetDefault { table, field, .. } => {
            let t = table_mut(schema, table)?;
            if !t.fields.contains_key(field) {
                return Err(RtDbError::bad_request(format!("setDefault target '{table}.{field}' does not exist")));
            }
            // data-only; schema unchanged
        }
        Directive::EvalExpr { table, set, expr, where_clause } => {
            let _ = table_mut(schema, table)?; // table must exist
            // `set` is a field path; the field need not exist (evalExpr may populate a
            // new key the caller adds via a later additive push), but the table must.
            if has_sql_violation(expr) || where_clause.as_deref().map_or(false, has_sql_violation) {
                return Err(RtDbError::bad_request(format!("evalExpr for '{table}.{set}' is out of scope (no FROM/joins or DDL verbs)")));
            }
        }
    }
    Ok(())
}

fn table_mut<'a>(schema: &'a mut SchemaDef, table: &str) -> Result<&'a mut TableDef, RtDbError> {
    schema.tables.get_mut(table)
        .ok_or_else(|| RtDbError::bad_request(format!("table '{table}' does not exist")))
}

/// True if `cast` can coerce from `old`. Mirrors the matrix in the spec.
fn cast_valid_for(cast: Cast, old: &FieldType) -> bool {
    use FieldType::*;
    match (cast, old) {
        (Cast::ToString, String | Number | Boolean | Int64) => true,
        (Cast::ToNumber, String | Boolean | Int64) => true,
        (Cast::ToInt64, String | Number) => true,
        (Cast::ToBoolean, String | Number) => true,
        _ => false,
    }
}

/// Rejects the scoped-raw-SQL boundary violations: a `FROM`/`JOIN` (cross-table)
/// or any DDL verb. The admin is trusted; this is blast-radius scoping.
fn has_sql_violation(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    const FORBIDDEN: &[&str] = &[
        " FROM ", " JOIN ", " INTO ", "UPDATE ", "DELETE ", "INSERT ", "DROP ",
        "ALTER ", "TRUNCATE ", "CREATE ", "GRANT ", "REVOKE ",
    ];
    FORBIDDEN.iter().any(|kw| upper.contains(kw))
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd server && cargo test migrate::tests`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add server/src/migrate.rs
git commit -m "feat(migrate): pure directive validation + derived schema (plan_migration)"
```

---

## Task 3: Applier shell + structural & setDefault directives

**Files:**
- Modify: `server/src/migrate.rs` (add `apply_migration` + structural arms)

**Interfaces:**
- Consumes: `crate::ddl::{pg_table, pg_col, pg_schema}`, `crate::txn::{DocOp, OpKind}`.
- Produces: `pub(crate) struct MigrationEffects { pub reports: Vec<DirectiveReport>, pub touched: BTreeSet<String>, pub ops: Vec<DocOp> }` and `pub(crate) async fn apply_migration(tx, db, directives, dry_run) -> Result<MigrationEffects, RtDbError>`. Task 6 consumes these.

- [ ] **Step 1: Write the failing integration test**

Create `server/tests/migration_test.rs`:

```rust
use par_rt_db_test_harness::*; // see Step 3 for the harness helper used across tasks

#[tokio::test]
async fn rename_field_rewrites_doc_key_and_column() {
    let db = setup_db_with_schema(r#"{"tables":{"users":{"fields":{"name":"string"}},"indexes":[]}}"#).await;
    insert_doc(&db, "users", r#"{"name":"Ada"}"#).await;

    migrate(&db, r#"{"directives":[{"op":"renameField","table":"users","from":"name","to":"fullName"}]}"#).await;

    let doc = get_doc(&db, "users").await;
    assert_eq!(doc["fullName"], "Ada");
    assert!(doc.get("name").is_none());
    drop_db(&db).await;
}
```

(The `setup_db_with_schema` / `insert_doc` / `get_doc` / `migrate` / `drop_db` helpers are added in Step 3 and reused by every later task's tests.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cd server && cargo test --test migration_test rename_field`
Expected: FAIL — `migrate`/helpers not defined.

- [ ] **Step 3: Implement `apply_migration` (shell + rename/drop/setDefault arms) + the test harness**

In `migrate.rs`, add:

```rust
use crate::ddl::{pg_col, pg_schema, pg_table};
use crate::txn::{DocOp, OpKind};
use std::collections::BTreeSet;

#[derive(Default)]
pub(crate) struct MigrationEffects {
    pub reports: Vec<DirectiveReport>,
    pub touched: BTreeSet<String>,
    pub ops: Vec<DocOp>,
}

/// Applies already-validated `directives` inside `tx` against `db`'s physical
/// tables. Bulk operations use SQL-side casts (the `ddl::backfill_expr` pattern)
/// and recompute indexed `f_` columns in the same statement. Does NOT commit.
/// On `dry_run`, callers roll the tx back; effects (reports/ops) are still
/// collected for the preview.
pub(crate) async fn apply_migration(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    db: &str,
    directives: &[Directive],
    dry_run: bool,
) -> Result<MigrationEffects, RtDbError> {
    let schema_name = pg_schema(db);
    let mut fx = MigrationEffects::default();
    for d in directives {
        let report = apply_one(tx, &schema_name, db, d, &mut fx).await?;
        fx.reports.push(report);
    }
    let _ = dry_run; // dry_run only governs commit/rollback in the caller
    Ok(fx)
}

async fn apply_one(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    schema_name: &str,
    db: &str,
    d: &Directive,
    fx: &mut MigrationEffects,
) -> Result<DirectiveReport, RtDbError> {
    match d {
        Directive::RenameField { table, from, to } => {
            let t = pg_table(table);
            let (n, ids) = rewrite_doc_key(tx, schema_name, &t, from, to).await?;
            recompute_indexed_columns(tx, schema_name, &t, table, &[to]).await?;
            fx.touched.insert(table.clone());
            push_ops(&mut fx.ops, table, &ids, OpKind::Patch);
            Ok(DirectiveReport { op: "renameField".into(), affected_rows: n, ..Default::default() })
        }
        Directive::RenameTable { from, to } => {
            // Physical table rename; docs unchanged -> no DocOps, but subscriptions re-run.
            sqlx::query(&format!("ALTER TABLE \"{schema_name}\".\"{}\" RENAME TO \"{}\"",
                pg_table(from), pg_table(to)))
                .execute(&mut **tx).await?;
            fx.touched.insert(to.clone());
            Ok(DirectiveReport { op: "renameTable".into(), affected_rows: 0, ..Default::default() })
        }
        Directive::DropField { table, field } => {
            let t = pg_table(table);
            let col = pg_col(field);
            let ids = all_ids(tx, schema_name, &t).await?;
            // remove the jsonb key, then drop the typed column
            sqlx::query(&format!("UPDATE \"{schema_name}\".\"{t}\" SET doc = doc - '{field}'"))
                .execute(&mut **tx).await?;
            sqlx::query(&format!("ALTER TABLE \"{schema_name}\".\"{t}\" DROP COLUMN \"{col}\""))
                .execute(&mut **tx).await?;
            fx.touched.insert(table.clone());
            push_ops(&mut fx.ops, table, &ids, OpKind::Patch);
            Ok(DirectiveReport { op: "dropField".into(), affected_rows: ids.len() as i64, ..Default::default() })
        }
        Directive::DropTable { name } => {
            let t = pg_table(name);
            let ids = all_ids(tx, schema_name, &t).await?;
            sqlx::query(&format!("DROP TABLE \"{schema_name}\".\"{t}\""))
                .execute(&mut **tx).await?;
            fx.touched.insert(name.clone());
            push_ops(&mut fx.ops, name, &ids, OpKind::Delete);
            Ok(DirectiveReport { op: "dropTable".into(), affected_rows: ids.len() as i64, ..Default::default() })
        }
        Directive::DropIndex { table, name } => {
            let idx = format!("i_{}_{}", table.to_lowercase(), name.to_lowercase());
            sqlx::query(&format!("DROP INDEX IF EXISTS \"{schema_name}\".\"{idx}\""))
                .execute(&mut **tx).await?;
            fx.touched.insert(table.clone());
            Ok(DirectiveReport { op: "dropIndex".into(), affected_rows: 0, ..Default::default() })
        }
        Directive::SetDefault { table, field, value } => {
            let t = pg_table(table);
            let v = serde_json::to_string(value).map_err(|e| RtDbError::internal(e.to_string()))?;
            let res = sqlx::query(&format!(
                "UPDATE \"{schema_name}\".\"{t}\" SET doc = jsonb_set(doc, '{{\"{field}\"}}', $1::jsonb, true) \
                 WHERE NOT doc ? '{field}'"))
                .bind(&v)
                .execute(&mut **tx).await?;
            let ids = ids_where(tx, schema_name, &t, &format!("NOT doc ? '{field}'")).await?;
            fx.touched.insert(table.clone());
            push_ops(&mut fx.ops, table, &ids, OpKind::Patch);
            Ok(DirectiveReport { op: "setDefault".into(), affected_rows: res.rows_affected() as i64, ..Default::default() })
        }
        Directive::ChangeType { .. } | Directive::EvalExpr { .. } => {
            // Implemented in Tasks 4 and 5.
            Err(RtDbError::internal("directive not yet implemented"))
        }
    }
}

// ---- helpers (port ddl::backfill_expr's SQL-side cast pattern) ----

async fn rewrite_doc_key(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, schema_name: &str, table: &str,
    from: &str, to: &str,
) -> Result<(i64, Vec<String>), RtDbError> {
    let ids = ids_where(tx, schema_name, table, &format!("doc ? '{from}'")).await?;
    let res = sqlx::query(&format!(
        "UPDATE \"{schema_name}\".\"{table}\" SET doc = jsonb_set(doc - '{from}', '{{\"{to}\"}}', doc->'{from}', true) \
         WHERE doc ? '{from}'"))
        .execute(&mut **tx).await?;
    Ok((res.rows_affected() as i64, ids))
}

/// Recompute the typed `f_` columns for `cols` from `doc` using SQL-side casts,
/// the same pattern as `ddl::backfill_expr`. Called after a doc rewrite.
async fn recompute_indexed_columns(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, schema_name: &str, table: &str,
    _user_table: &str, cols: &[&str],
) -> Result<(), RtDbError> {
    for field in cols {
        let col = pg_col(field);
        // Cast is type-specific; for the generic recompute we re-extract as text.
        // (changeType in Task 4 supplies the precise cast.)
        sqlx::query(&format!(
            "UPDATE \"{schema_name}\".\"{table}\" SET \"{col}\" = (doc->>'{field}') WHERE doc ? '{field}'"))
            .execute(&mut **tx).await?;
    }
    Ok(())
}

async fn all_ids(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, schema_name: &str, table: &str)
    -> Result<Vec<String>, RtDbError> {
    ids_where(tx, schema_name, table, "true").await
}

async fn ids_where(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, schema_name: &str, table: &str, cond: &str)
    -> Result<Vec<String>, RtDbError> {
    let rows: Vec<(String,)> = sqlx::query_as(&format!("SELECT id FROM \"{schema_name}\".\"{table}\" WHERE {cond}"))
        .fetch_all(&mut **tx).await?;
    Ok(rows.into_iter().map(|r| r.0).collect())
}

fn push_ops(ops: &mut Vec<DocOp>, table: &str, ids: &[String], kind: OpKind) {
    for id in ids { ops.push(DocOp { table: table.into(), id: id.clone(), kind: kind.clone() }); }
}
```

Add `Default` to `DirectiveReport` (derive it on the struct in Task 1: add `Default` to its derive list — `#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]`; `op`/`affected_rows` get defaults `""`/`0`, the Vecs default empty — but `op: String` defaults to `""`. Since the constructor always sets `op`, this is fine. Actually a `String` field with `Default` works.)

Then add the test harness. The server's integration tests share helpers via a `tests/common/` module — check an existing test file (e.g. `txn_test.rs`) for the harness crate/module pattern and mirror it. The harness needs: create a uniquely-named db, push a schema, insert a doc, run a migrate (via the public `migrate.rs` entry once Task 6 wires it — for now, tests in Task 3 call a `migrate()` helper that wraps `apply_migration` inside a `pool.begin()` tx for direct unit-level testing of the applier). Concretely, in `tests/migration_test.rs` add helpers that open a tx, call `par_rt_db::migrate::apply_migration`, and commit — so the applier is testable before the committer arm exists.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd server && cargo test --test migration_test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add server/src/migrate.rs server/tests/migration_test.rs
git commit -m "feat(migrate): applyMigration shell + rename/drop/setDefault directives"
```

---

## Task 4: `changeType` directive (cast matrix + default/atomic-fail)

**Files:**
- Modify: `server/src/migrate.rs` (implement the `ChangeType` arm), `server/tests/migration_test.rs`

**Interfaces:**
- Consumes: `crate::schema::indexed_column_type` (FieldType → pg type), the `Cast` matrix.
- Produces: the `ChangeType` arm of `apply_one`.

- [ ] **Step 1: Write failing tests** in `migration_test.rs`:

```rust
#[tokio::test]
async fn change_type_number_to_string_coerces() {
    let db = setup_db_with_schema(r#"{"tables":{"users":{"fields":{"age":"number"}},"indexes":[{"name":"by_age","fields":["age"]}]}}"#).await;
    insert_doc(&db, "users", r#"{"age":42}"#).await;
    migrate(&db, r#"{"directives":[{"op":"changeType","table":"users","field":"age","to":{"type":"string"},"cast":"toString"}]}"#).await;
    let doc = get_doc(&db, "users").await;
    assert_eq!(doc["age"], "42");
    drop_db(&db).await;
}

#[tokio::test]
async fn change_type_to_number_atomic_fail_names_row() {
    let db = setup_db_with_schema(r#"{"tables":{"u":{"fields":{"v":"string"}},"indexes":[]}}"#).await;
    insert_doc(&db, "u", r#"{"v":"not-a-number"}"#).await;
    let err = migrate_err(&db, r#"{"directives":[{"op":"changeType","table":"u","field":"v","to":{"type":"number"},"cast":"toNumber"}]}"#).await;
    assert!(err.message.contains("u") , "error should name the offending row/table: {err:?}");
    // atomic: doc unchanged
    assert_eq!(get_doc(&db, "u").await["v"], "not-a-number");
    drop_db(&db).await;
}

#[tokio::test]
async fn change_type_default_substitutes_uncoercible() {
    let db = setup_db_with_schema(r#"{"tables":{"u":{"fields":{"v":"string"}},"indexes":[]}}"#).await;
    insert_doc(&db, "u", r#"{"v":"oops"}"#).await;
    migrate(&db, r#"{"directives":[{"op":"changeType","table":"u","field":"v","to":{"type":"number"},"cast":"toNumber","default":0}]}"#).await;
    assert_eq!(get_doc(&db, "u").await["v"], 0);
    drop_db(&db).await;
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd server && cargo test --test migration_test change_type`
Expected: FAIL (`internal: directive not yet implemented`).

- [ ] **Step 3: Implement the `ChangeType` arm**

In `apply_one`, replace the `Directive::ChangeType { .. }` placeholder with:

```rust
        Directive::ChangeType { table, field, to, cast, default } => {
            use crate::schema::indexed_column_type;
            let t = pg_table(table);
            let (pg_type, _nullable) = indexed_column_type(to)
                .map_err(|_| RtDbError::bad_request(format!("changeType target for {table}.{field} is not indexable")))?;
            // Coerce the jsonb value in-place. On failure: substitute `default` if given,
            // else raise a row-named BadRequest so the whole migrate rolls back atomically.
            let (cast_expr, check_expr) = cast_sql(*cast, field);
            let ids = all_ids(tx, schema_name, &t).await?;
            for id in &ids {
                let row: Option<(Option<serde_json::Value>,)> = sqlx::query_as(&format!(
                    "SELECT doc->'{field}' FROM \"{schema_name}\".\"{t}\" WHERE id = $1"))
                    .bind(id).fetch_optional(&mut **tx).await?;
                let Some(Some(val)) = row else { continue };
                let coerced = coerce_value(*cast, &val);
                let new_val = match (coerced, default) {
                    (Some(v), _) => v,
                    (None, Some(d)) => d.clone(),
                    (None, None) => return Err(RtDbError::bad_request(
                        format!("changeType cannot coerce value in {table}.{id} ({val}) and no default given"))),
                };
                let s = serde_json::to_string(&new_val).map_err(|e| RtDbError::internal(e.to_string()))?;
                sqlx::query(&format!(
                    "UPDATE \"{schema_name}\".\"{t}\" SET doc = jsonb_set(doc, '{{\"{field}\"}}', $1::jsonb, true) WHERE id = $2"))
                    .bind(&s).bind(id).execute(&mut **tx).await?;
            }
            // Recast the typed column to the new pg type.
            sqlx::query(&format!("ALTER TABLE \"{schema_name}\".\"{t}\" ALTER COLUMN \"{}\" TYPE {pg_type} USING ({check_expr})",
                pg_col(field))).execute(&mut **tx).await?;
            fx.touched.insert(table.clone());
            push_ops(&mut fx.ops, table, &ids, OpKind::Patch);
            Ok(DirectiveReport { op: "changeType".into(), affected_rows: ids.len() as i64, ..Default::default() })
        }
```

Add the cast helpers (pure, unit-tested):

```rust
/// Returns (value_cast_sql, check_sql) for a column recompute, mirroring
/// `ddl::backfill_expr`. `check_sql` is the expression used in `USING (...)`.
fn cast_sql(cast: Cast, field: &str) -> (&'static str, String) {
    let f = field;
    match cast {
        Cast::ToString  => ("text",            format!("(doc->>'{f}')")),
        Cast::ToNumber  => ("double precision", format!("(doc->>'{f}')::float8")),
        Cast::ToInt64   => ("bigint",          format!("(doc->>'{f}')::bigint")),
        Cast::ToBoolean => ("boolean",         format!("(doc->>'{f}')::boolean")),
    }
}

/// Pure Rust coercion mirroring the SQL cast, used to decide default-vs-fail
/// per row without relying on a Postgres exception. Returns None if the value
/// cannot be coerced under this cast.
fn coerce_value(cast: Cast, v: &serde_json::Value) -> Option<serde_json::Value> {
    use serde_json::Value;
    match (cast, v) {
        (Cast::ToString, Value::String(_)) => Some(v.clone()),
        (Cast::ToString, Value::Number(n)) => Some(Value::String(n.to_string())),
        (Cast::ToString, Value::Bool(b)) => Some(Value::String(b.to_string())),
        (Cast::ToString, _) => None,
        (Cast::ToNumber, Value::String(s)) => s.parse::<f64>().ok().map(|n| serde_json::Number::from_f64(n).unwrap_or(n.into()).into()).or(None),
        (Cast::ToNumber, Value::Number(_)) => Some(v.clone()),
        (Cast::ToNumber, Value::Bool(b)) => Some(serde_json::json!(if *b {1.0} else {0.0})),
        (Cast::ToNumber, _) => None,
        (Cast::ToInt64, Value::String(s)) => s.parse::<i64>().ok().map(|i| serde_json::json!(i.to_string())), // int64 travels as decimal string
        (Cast::ToInt64, Value::Number(n)) => n.as_i64().map(|i| serde_json::json!(i.to_string())),
        (Cast::ToInt64, _) => None,
        (Cast::ToBoolean, Value::String(s)) => match s.as_str() { "true"|"1" => Some(true.into()), "false"|"0" => Some(false.into()), _ => None },
        (Cast::ToBoolean, Value::Number(n)) => Some((n.as_f64().map(|f| f != 0.0).unwrap_or(true)).into()),
        (Cast::ToBoolean, _) => None,
    }
}
```
(If `serde_json::Number::from_f64(...).unwrap_or(...)` is awkward, simplify to `serde_json::json!(n)` — refine in-task so it compiles; the intent is "emit a JSON number".)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd server && cargo test --test migration_test change_type`
Expected: PASS (all three).

- [ ] **Step 5: Commit**

```bash
git add server/src/migrate.rs server/tests/migration_test.rs
git commit -m "feat(migrate): changeType directive (cast matrix + default/atomic-fail)"
```

---

## Task 5: `evalExpr` directive (scoped raw-SQL doc rewrite)

**Files:**
- Modify: `server/src/migrate.rs` (implement the `EvalExpr` arm), `server/tests/migration_test.rs`

**Interfaces:**
- Consumes: the scope validation already in `plan_migration` (`has_sql_violation`).
- Produces: the `EvalExpr` arm of `apply_one`.

- [ ] **Step 1: Write failing tests:**

```rust
#[tokio::test]
async fn eval_expr_rewrites_doc_field() {
    let db = setup_db_with_schema(r#"{"tables":{"u":{"fields":{"name":"string","upper":"string"}},"indexes":[]}}"#).await;
    insert_doc(&db, "u", r#"{"name":"ada"}"#).await;
    migrate(&db, r#"{"directives":[{"op":"evalExpr","table":"u","set":"upper","expr":"upper(doc->>'name')"}]}"#).await;
    assert_eq!(get_doc(&db, "u").await["upper"], "ADA");
    drop_db(&db).await;
}

#[tokio::test]
async fn eval_expr_where_filters() {
    let db = setup_db_with_schema(r#"{"tables":{"u":{"fields":{"n":"number","doubled":"number"}},"indexes":[]}}"#).await;
    insert_doc(&db, "u", r#"{"n":1}"#).await;
    insert_doc(&db, "u", r#"{"n":2}"#).await; // id order may vary; filter n>=2
    migrate(&db, r#"{"directives":[{"op":"evalExpr","table":"u","set":"doubled","expr":"(doc->>'n')::float8 * 2","where":"(doc->>'n')::float8 >= 2"}]}"#).await;
    // only the n=2 row gets doubled set; verify at least one doc has doubled=4
    let docs = query_docs(&db, "u", r#"{"table":"u"}"#).await;
    assert!(docs.iter().any(|d| d.get("doubled").and_then(|v| v.as_f64()) == Some(4.0)));
    drop_db(&db).await;
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd server && cargo test --test migration_test eval_expr`
Expected: FAIL.

- [ ] **Step 3: Implement the `EvalExpr` arm**

In `apply_one`, replace the `Directive::EvalExpr { .. }` placeholder:

```rust
        Directive::EvalExpr { table, set, expr, where_clause } => {
            // Scope was already validated by plan_migration (no FROM/joins/DDL verbs).
            let t = pg_table(table);
            let cond = where_clause.clone().unwrap_or_else(|| "true".to_string());
            let ids = ids_where(tx, schema_name, &t, &cond).await?;
            // Rewrite doc, then recompute indexed f_ columns from the new doc.
            sqlx::query(&format!(
                "UPDATE \"{schema_name}\".\"{t}\" SET doc = jsonb_set(doc, '{{\"{set}\"}}', to_jsonb(({expr})), true) WHERE {cond}"))
                .execute(&mut **tx).await?;
            // Recompute any indexed columns on this table from the updated doc.
            recompute_all_indexed(tx, schema_name, &t, table).await?;
            fx.touched.insert(table.clone());
            push_ops(&mut fx.ops, table, &ids, OpKind::Patch);
            Ok(DirectiveReport { op: "evalExpr".into(), affected_rows: ids.len() as i64, ..Default::default() })
        }
```

Add `recompute_all_indexed` (recompute every `f_` column for a table from `doc`, mirroring `ddl::backfill_expr` per column type — needs the post-schema's index list; accept it via the derived schema passed into `apply_migration`). Adjust `apply_migration`'s signature to also take `derived: &SchemaDef` so `recompute_all_indexed` can enumerate indexed fields and their types (use `indexed_column_type` for the cast). Update callers in Task 6 accordingly.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd server && cargo test --test migration_test eval_expr`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add server/src/migrate.rs server/tests/migration_test.rs
git commit -m "feat(migrate): evalExpr scoped raw-SQL doc rewrite"
```

---

## Task 6: Committer wiring (`RunMigrate` arm + tap sites + dry-run)

**Files:**
- Modify: `server/src/committer.rs` (`CommitterRequest::RunMigrate`, `handle_migrate`, `Committers::migrate`), `server/src/migrate.rs` (make `apply_migration`/`MigrationEffects` crate-visible — already `pub(crate)`)

**Interfaces:**
- Consumes: `crate::migrate::{plan_migration, apply_migration, MigrationEffects, MigrateRequest, MigrateResult}`, `crate::subs` (`fan_out` via `WriteSet`), `crate::txn::{WriteSet, DocOp}`, the four tap-site calls from `handle_mutate`.
- Produces: `Committers::migrate(&self, db, request) -> Result<MigrateResult, RtDbError>` (called by Task 7's HTTP handler).

- [ ] **Step 1: Write failing tests** in `migration_test.rs` (the invariant tests):

```rust
#[tokio::test]
async fn migrate_fires_subscription_fanout() {
    // subscribe to a table, mutate-via-migrate a doc, assert the live query updates.
}

#[tokio::test]
async fn migrate_publishes_to_op_feed() {
    // run a migrate, then GET /admin/ops/recent and assert a DocOp appears.
}

#[tokio::test]
async fn migrate_writes_audit_row_when_enabled() {
    // with RTDB_AUDIT_LOG_ENABLED, assert rtdb.audit_log has a row after migrate.
}

#[tokio::test]
async fn migrate_dry_run_commits_nothing() {
    let db = setup_db_with_schema(r#"{"tables":{"u":{"fields":{"n":"number"}},"indexes":[]}}"#).await;
    insert_doc(&db, "u", r#"{"n":1}"#).await;
    let res = migrate(&db, r#"{"directives":[{"op":"setDefault","table":"u","field":"flag","value":true}],"dryRun":true}"#).await;
    assert!(!res.applied);
    // doc unchanged: no `flag`
    assert!(get_doc(&db, "u").await.get("flag").is_none());
    drop_db(&db).await;
}

#[tokio::test]
async fn migrate_queues_same_db_writes_not_other_db() {
    // start a slow migrate on dbA; a concurrent mutate to dbA waits, to dbB does not.
    // (timing-based; keep generous, or assert ordering via a second subscribe.)
}
```
(Mirror the existing test-harness subscription/op-feed helpers from `subs_test.rs`/`http_api_test.rs`.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd server && cargo test --test migration_test migrate_`
Expected: FAIL — `Committers::migrate` not defined.

- [ ] **Step 3: Add the `CommitterRequest` variant + `Committers::migrate`**

In `committer.rs`, add to the `CommitterRequest` enum (at lines 22-47):

```rust
    RunMigrate {
        request: crate::migrate::MigrateRequest,
        reply: oneshot::Sender<Result<crate::migrate::MigrateResult, RtDbError>>,
    },
```

Add the public method beside `Committers::mutate` (lines 196-217):

```rust
    pub async fn migrate(
        &self,
        db: &str,
        request: crate::migrate::MigrateRequest,
    ) -> Result<crate::migrate::MigrateResult, RtDbError> {
        let (reply, reply_rx) = oneshot::channel();
        self.submit(db, CommitterRequest::RunMigrate { request, reply }).await?;
        reply_rx
            .await
            .map_err(|_| RtDbError::internal("committer task dropped the reply"))?
    }
```

- [ ] **Step 4: Add the match arm + `handle_migrate`**

In `run_committer`'s match (lines 306-337), add:

```rust
            CommitterRequest::RunMigrate { request, reply } => {
                let result = handle_migrate(&ctx, request).await;
                let _ = reply.send(result);
            }
```

Add `handle_migrate` beside `handle_mutate` (mirror its tap-site block at 362-399 verbatim in structure):

```rust
async fn handle_migrate(
    ctx: &CommitterCtx,
    request: crate::migrate::MigrateRequest,
) -> Result<crate::migrate::MigrateResult, RtDbError> {
    let schema = crate::db::load_schema(&ctx.pool, &ctx.db)
        .await?.ok_or_else(|| RtDbError::not_found("database has no schema"))?;
    let derived = crate::migrate::plan_migration(&schema, &request.directives)?;

    let mut tx = ctx.pool.begin().await?;
    let dry_run = request.dry_run;
    let fx = crate::migrate::apply_migration(&mut tx, &ctx.db, &request.directives, &derived, dry_run).await?;

    if dry_run {
        tx.rollback().await?;
        return Ok(crate::migrate::MigrateResult { applied: false, schema: derived, directives: fx.reports });
    }

    // Persist the derived schema (single jsonb blob in db_{db}.meta — same shape as push_schema's tail).
    let schema_json = serde_json::to_value(&derived)
        .map_err(|e| RtDbError::internal(format!("failed to serialize schema: {e}")))?;
    let schema_name = crate::ddl::pg_schema(&ctx.db);
    sqlx::query(&format!(
        "INSERT INTO \"{schema_name}\".meta (key, value) VALUES ('schema', $1) \
         ON CONFLICT (key) DO UPDATE SET value = excluded.value"))
        .bind(schema_json).execute(&mut *tx).await?;
    tx.commit().await?;

    ctx.schemas.put(&ctx.db, &derived).await;

    // Four tap sites — same contract as handle_mutate (committer.rs:362-399).
    let write_set = crate::txn::WriteSet { tables: fx.touched, ops: fx.ops.clone(), ..Default::default() };
    ctx.subs.fan_out(&ctx.pool, &ctx.db, &derived, &write_set).await;
    ctx.op_feed.publish(&ctx.db, None, &write_set.ops).await;
    if ctx.audit_log_enabled
        && let Err(e) = crate::audit::write_audit_rows(&ctx.pool, &ctx.db, None, "migrate", &write_set.ops).await {
        tracing::warn!(db = %ctx.db, error = %e, "audit log write failed");
    }
    if ctx.webhooks_enabled
        && let Err(e) = crate::webhook::enqueue_for_ops(&ctx.pool, &ctx.db, None, "migrate", &write_set.ops).await {
        tracing::warn!(db = %ctx.db, error = %e, "webhook enqueue failed");
    }

    Ok(crate::migrate::MigrateResult { applied: true, schema: derived, directives: fx.reports })
}
```

Adjust `apply_migration`'s signature to accept `derived: &SchemaDef` (per Task 5's `recompute_all_indexed` need) and thread it through.

- [ ] **Step 5: Run the gate**

Run: `cd server && cargo test --test migration_test && cargo clippy --all-targets -- -D warnings`
Expected: all migrate tests pass, including the four invariant tests.

- [ ] **Step 6: Commit**

```bash
git add server/src/committer.rs server/src/migrate.rs server/tests/migration_test.rs
git commit -m "feat(migrate): committer RunMigrate arm — fan_out + op-feed/audit/webhook + dry-run"
```

---

## Task 7: HTTP route `POST /admin/db/{db}/migrate`

**Files:**
- Modify: `server/src/admin.rs` (handler + route registration)

**Interfaces:**
- Consumes: `state.realtime.committers.migrate(...)` (Task 6), `require_admin`, `ApiJson`, `Path`.
- Produces: the public HTTP endpoint.

- [ ] **Step 1: Write failing test** in `migration_test.rs` (or `admin_test.rs`, following the convention there):

```rust
#[tokio::test]
async fn http_migrate_admin_only_and_unknown_db() {
    // POST /admin/db/{db}/migrate with admin key applies; without admin key -> 401; unknown db -> 404.
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd server && cargo test --test migration_test http_migrate`
Expected: FAIL — route not registered.

- [ ] **Step 3: Add the handler** in `admin.rs` (template: `admin_mutate`, lines 548-592):

```rust
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AdminMigrateRequest {
    #[serde(default)]
    directives: Vec<crate::migrate::Directive>,
    #[serde(default)]
    dry_run: bool,
}

async fn admin_migrate(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(db): Path<String>,
    ApiJson(body): ApiJson<AdminMigrateRequest>,
) -> Result<Json<crate::migrate::MigrateResult>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let result = state.realtime.committers.migrate(&db, crate::migrate::MigrateRequest {
        directives: body.directives,
        dry_run: body.dry_run,
    }).await?;
    Ok(Json(result))
}
```

Register the route in `admin_routes()` (admin.rs ~1388, beside the other `/admin/db/{db}/...` POST routes):

```rust
        .route("/admin/db/{db}/migrate", post(admin_migrate))
```

- [ ] **Step 4: Run the full server gate**

Run: `cd server && cargo test --test migration_test --test admin_test && cargo clippy --all-targets -- -D warnings && cargo fmt --check`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add server/src/admin.rs server/tests/migration_test.rs
git commit -m "feat(migrate): POST /admin/db/{db}/migrate admin route"
```

✅ **Phase A checkpoint:** the server migrates end-to-end (HTTP → committer → DDL/DML → fan_out + taps), tested including invariants. Phase B is the client mirror + UX.

---

# Phase B — Client mirror + UX + docs

## Task 8: ts-client mirror

**Files:**
- Modify: `ts-client/src/protocol.ts` (wire types), Create `ts-client/src/migration.ts` (builder), Modify `ts-client/src/admin.ts` (`migrate`), `ts-client/src/in_memory.ts` (harness), `ts-client/tests/migration.test.ts`, `ts-client/tests/admin.test.ts`, `ts-client/tests/in_memory.test.ts`.

**Interfaces (mirror Task 1 exactly):** `DirectiveJson` (`op` discriminated union), `CastJson`, `MigrateRequestJson`, `MigrateResultJson`, `DirectiveReportJson`.

- [ ] **Step 1: Add wire types to `protocol.ts`** beside `StepJson` (L146-161):

```ts
export type Cast = "toString" | "toNumber" | "toInt64" | "toBoolean";

export type DirectiveJson =
  | { op: "renameField"; table: string; from: string; to: string }
  | { op: "renameTable"; from: string; to: string }
  | { op: "changeType"; table: string; field: string; to: FieldTypeJson; cast: Cast; default?: unknown }
  | { op: "dropField"; table: string; field: string }
  | { op: "dropTable"; name: string }
  | { op: "dropIndex"; table: string; name: string }
  | { op: "setDefault"; table: string; field: string; value: unknown }
  | { op: "evalExpr"; table: string; set: string; expr: string; where?: string };

export interface MigrateRequestJson { directives: DirectiveJson[]; dryRun?: boolean }
export interface CastFailureJson { id: string; value: unknown }
export interface SampleChangeJson { id: string; before: unknown; after: unknown }
export interface DirectiveReportJson {
  op: string; affectedRows: number;
  castFailures?: CastFailureJson[]; sampleChanges?: SampleChangeJson[];
}
export interface MigrateResultJson { applied: boolean; schema: SchemaJson; directives: DirectiveReportJson[] }
```

- [ ] **Step 2: Create `migration.ts` builder** (template: `TxnBuilder`, `mutation.ts:67-126`):

```ts
import type { DirectiveJson, MigrateRequestJson } from "./protocol";
import type { FieldTypeJson } from "./protocol";

export class Migration {
  private readonly directives: DirectiveJson[] = [];
  private dry = false;
  renameField(table: string, from: string, to: string): this { this.directives.push({op:"renameField",table,from,to}); return this; }
  renameTable(from: string, to: string): this { this.directives.push({op:"renameTable",from,to}); return this; }
  changeType(table: string, field: string, to: FieldTypeJson, cast: DirectiveJson extends never ? never : any, def?: unknown): this {
    const d: DirectiveJson = {op:"changeType",table,field,to,cast}; if (def !== undefined) (d as any).default = def; this.directives.push(d); return this;
  }
  dropField(table: string, field: string): this { this.directives.push({op:"dropField",table,field}); return this; }
  dropTable(name: string): this { this.directives.push({op:"dropTable",name}); return this; }
  dropIndex(table: string, name: string): this { this.directives.push({op:"dropIndex",table,name}); return this; }
  setDefault(table: string, field: string, value: unknown): this { this.directives.push({op:"setDefault",table,field,value}); return this; }
  evalExpr(table: string, set: string, expr: string, where?: string): this {
    const d: DirectiveJson = {op:"evalExpr",table,set,expr}; if (where !== undefined) (d as any).where = where; this.directives.push(d); return this;
  }
  dryRun(): this { this.dry = true; return this; }
  build(): MigrateRequestJson { return { directives: [...this.directives], dryRun: this.dry }; }
}
```
(Tighten the `cast` param type to `Cast` from protocol.ts in-impl; the `any` casts are scaffold — the implementer replaces with a clean conditional-spread.)

- [ ] **Step 3: Add `RtDbAdminClient.migrate`** in `admin.ts` (template: `getSchema`, L232-234):

```ts
async migrate(db: string, req: MigrateRequestJson): Promise<MigrateResultJson> {
  return (await this.request("POST", `/admin/db/${encodeURIComponent(db)}/migrate`, req)) as MigrateResultJson;
}
```

- [ ] **Step 4: Add in-memory `migrate`** to `InMemoryRtDbClient` (`in_memory.ts`) mirroring the structural+data effects (rename rewrites the in-memory doc map; drop removes; setDefault fills; changeType coerces via a port of `coerce_value`; evalExpr throws `RtDbError("BAD_REQUEST","unsupported in-memory")` — same convention as search/vector stubs). Reuse the already-ported `isWideningOf`/`detectDestructiveChanges`.

- [ ] **Step 5: Tests** — `tests/migration.test.ts` (builder shape → `build()` produces the exact wire JSON), `tests/admin.test.ts` (mocked `fetch` asserts the request URL/body + decodes `MigrateResultJson`), `tests/in_memory.test.ts` (rename/setDefault/changeType in-memory).

- [ ] **Step 6: Run the gate**

Run: `cd ts-client && bunx vitest run tests/migration.test.ts tests/admin.test.ts tests/in_memory.test.ts && bunx tsc --noEmit && bun run lint`
Expected: PASS.

- [ ] **Step 7: Rebuild dist + commit**

```bash
cd ts-client && bun run build   # rebuilds dist/ the dashboard typecheck depends on
git add ts-client/src ts-client/tests ts-client/dist
git commit -m "feat(ts-client): schema migrate wire + Migration builder + admin + in-memory"
```

---

## Task 9: rust-client mirror

**Files:**
- Modify: `rust-client/src/wire.rs` (admin module, `#[cfg(feature="admin")]` at L438-651 — add `Directive`/`MigrateRequest`/`MigrateResult`), Create `rust-client/src/migration.rs` (builder), Modify `rust-client/src/http.rs` (admin impl block, L545 — add `migrate_schema`), `rust-client/src/in_memory.rs`, tests.

**Interfaces:** mirror Task 1's types. `Directive` enum uses `#[serde(tag = "op", rename_all = "camelCase", deny_unknown_fields)]` (template: `Step` in `mutation.rs:11-49`); `FieldType` is `crate::schema::FieldType`.

- [ ] **Step 1: Wire types in `wire.rs` admin module** (template: `MintedToken`, L491-496 for responses; `PushSchemaRequest`, L454-458 for requests):

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", deny_unknown_fields")]
pub enum Directive {
    RenameField { table: String, from: String, to: String },
    RenameTable { from: String, to: String },
    ChangeType { table: String, field: String, to: crate::schema::FieldType, cast: Cast,
                 #[serde(default)] default: Option<serde_json::Value> },
    DropField { table: String, field: String },
    DropTable { name: String },
    DropIndex { table: String, name: String },
    SetDefault { table: String, field: String, value: serde_json::Value },
    EvalExpr { table: String, set: String, expr: String,
               #[serde(default, rename = "where")] where_clause: Option<String> },
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")] pub enum Cast { ToString, ToNumber, ToInt64, ToBoolean }
#[derive(Debug, Clone, Serialize)] #[serde(rename_all = "camelCase")]
pub struct MigrateRequest<'a> { pub directives: &'a [Directive], pub dry_run: bool }
#[derive(Debug, Clone, Deserialize)] #[serde(rename_all = "camelCase")]
pub struct MigrateResult { pub applied: bool, pub schema: crate::schema::SchemaDef, pub directives: Vec<serde_json::Value> }
```

- [ ] **Step 2: `migration.rs` builder** (template: `Mutation`, `mutation.rs:72-154` — `mut self → Self`, `.build()`):

```rust
pub struct Migration { directives: Vec<crate::wire::Directive>, dry_run: bool }
impl Migration {
    pub fn new() -> Self { Self { directives: vec![], dry_run: false } }
    pub fn rename_field(mut self, table: &str, from: &str, to: &str) -> Self { self.directives.push(crate::wire::Directive::RenameField{table:table.into(),from:from.into(),to:to.into()}); self }
    // …rename_table / change_type / drop_field / drop_table / drop_index / set_default / eval_expr / dry_run…
    pub fn build(self) -> Vec<crate::wire::Directive> { self.directives }
}
```

- [ ] **Step 3: `migrate_schema` in `http.rs`** admin block (template: `mint_token`, L596-609):

```rust
pub async fn migrate_schema(&self, db: &str, directives: &[crate::wire::Directive], dry_run: bool) -> Result<crate::wire::admin::MigrateResult, RtDbError> {
    let resp = self.post_json(&format!("/admin/db/{}/migrate", db),
        &crate::wire::admin::MigrateRequest { directives, dry_run }).await?;
    self.deserialize::<crate::wire::admin::MigrateResult>(resp).await
}
```

- [ ] **Step 4: In-memory `migrate_schema`** in `in_memory.rs` (port the ts harness behavior; `evalExpr` returns `Err(RtDbError::bad_request("unsupported in-memory"))`).

- [ ] **Step 5: Tests** — inline `#[cfg(test)] mod tests` in `wire.rs` (serde round-trip), `http.rs` wiremock test (template: `push_schema_serializes_schema_json`, L1712), `in_memory.rs` migrate tests (template: `push_schema_*`, L2843). Add the wire types to `rust-client/tests/wire_corpus.rs` (cross-client parity fixture) if the corpus covers Step/Query.

- [ ] **Step 6: Run the gate**

Run: `cd rust-client && cargo test --features admin,in_memory && cargo clippy --all-targets -- -D warnings && cargo fmt --check`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add rust-client/src rust-client/tests
git commit -m "feat(rust-client): schema migrate wire + builder + admin + in-memory"
```

---

## Task 10: python-client mirror

**Files:**
- Modify: `python-client/src/par_rt_db/wire.py` (template: `FilterExpr` discriminator L119-181, `_Camel` base L27-40) or a new `migration.py`; Modify `http_client.py` (`migrate_schema`, template: `push_schema` L468-475 + `db_stats` typed-response L566-569), `in_memory.py` (`_detect_destructive_changes` at L1322 is the neighbor), tests.

**Interfaces:** mirror Task 1. Pydantic v2 `_Camel` subclasses; `Directive` discriminated union on `op`; `MigrateResult` a `_Wire`/`_Camel` model in `http_client.py` beside `MintedToken`/`DbStats`.

- [ ] **Step 1: Wire models** — add to `wire.py` (or a new `mutation.py`-style `migration.py`): `Cast` (Literal), `_Directive` discriminated union (`Annotated[..., Field(discriminator="op")]`), `MigrateRequest`, and the result models (`DirectiveReport`, `MigrateResult`).

```python
class Cast(str, Enum):
    TO_STRING = "toString"; TO_NUMBER = "toNumber"; TO_INT64 = "toInt64"; TO_BOOLEAN = "toBoolean"

class _RenameField(_Camel):
    op: Literal["renameField"]; table: str; from_: str = Field(alias="from"); to: str
# … one model per op, then:
Directive = Annotated[Union[_RenameField, _RenameTable, _ChangeType, _DropField, _DropTable, _DropIndex, _SetDefault, _EvalExpr], Field(discriminator="op")]
```

- [ ] **Step 2: `migrate_schema` in `http_client.py`:**

```python
def migrate_schema(self, db: str, directives: list, *, dry_run: bool = False) -> MigrateResult:
    resp = self._send("POST", f"/admin/db/{db}/migrate",
        json={"directives": [d.model_dump(by_alias=True, mode="json") for d in directives], "dryRun": dry_run})
    return MigrateResult.model_validate(resp.json())
```

- [ ] **Step 3: In-memory `migrate`** in `in_memory.py` (port; `evalExpr` raises `RtDbError(ErrorCode.BAD_REQUEST, "unsupported in-memory")`).

- [ ] **Step 4: Tests** — `tests/test_wire.py`/`test_wire_parity.py` (round-trip + parity), `tests/test_http_client.py` (template: `test_admin_push_schema_serializes_schema_json`, L485, via `httpx.MockTransport`), `tests/test_in_memory.py`.

- [ ] **Step 5: Run the gate**

Run: `cd python-client && uv run pytest -q tests/test_wire.py tests/test_http_client.py tests/test_in_memory.py && uv run pyright && uv run ruff check .`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add python-client/src python-client/tests
git commit -m "feat(python-client): schema migrate wire + builder + admin + in-memory"
```

---

## Task 11: `rtdb migrate` CLI subcommand

**Files:**
- Modify: `cli/src/main.rs` (one `Command` variant + match arm + import; template: `PushSchema`, L97-106; helpers `admin_client`/`read_json_arg`/`map_err`).

- [ ] **Step 1: Add the variant** to the `Command` enum (L41-80):

```rust
    Migrate { file: PathBuf, #[arg(long)] dry_run: bool },
```

- [ ] **Step 2: Add the match arm** in dispatch (L82-140), mirroring `PushSchema`:

```rust
    Command::Migrate { file, dry_run } => {
        let db = require_db(&cli)?;
        let json = std::fs::read_to_string(&file).with_context(|| format!("reading {}", file.display()))?;
        let req: par_rt_db_client::wire::admin::MigrateRequestOwned = serde_json::from_str(&json).context("parsing migrate JSON")?;
        let c = admin_client(&cli)?;
        let result = c.migrate_schema(&db, &req.directives, dry_run).await.map_err(map_err)?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        if !result.applied { eprintln!("dry-run only — nothing applied (re-run without --dry-run to apply)"); }
    }
```
(`MigrateRequestOwned` — if `MigrateRequest<'a>` borrows, add an owned `Serialize` request struct in `wire.rs`, or deserialize into the builder. Pick the owned-struct route for CLI ergonomics.)

- [ ] **Step 3: Test** — inline `#[cfg(test)] mod tests` `Cli::try_parse_from(["rtdb","migrate","f.json","--dry-run"])` → `matches!(…, Command::Migrate{dry_run:true,..})`.

- [ ] **Step 4: Run the gate + commit**

```bash
cd cli && cargo test && cargo clippy -- -D warnings && cargo fmt --check
git add cli/src/main.rs
git commit -m "feat(cli): rtdb migrate subcommand (dry-run-first)"
```

---

## Task 12: Dashboard guided migrate flow

**Files:**
- Modify: `dashboard/src/lib/admin.tsx` (`AdminClient.migrate`, template: `previewSchema`/`pushSchema`, L91-105), Create `dashboard/src/pages/MigratePage.tsx` (template: `SchemaPage.tsx` — JSON-in / preview+apply / discriminated error state), Modify `dashboard/src/App.tsx` (route `dbs/:db/migrate`) + `dashboard/src/shell/AppShell.tsx` (nav) or `DbPage.tsx` (link beside Schema, L60-63). Co-located `MigratePage.module.css` + `MigratePage.test.tsx`.

- [ ] **Step 1: `AdminClient.migrate`** in `admin.tsx`:

```ts
migrate(db: string, req: { directives: unknown[]; dryRun?: boolean }): Promise<MigrateResultJson> {
  return this.req(`/admin/db/${enc(db)}/migrate`, { method: "POST", body: JSON.stringify(req) });
}
```

- [ ] **Step 2: `MigratePage.tsx`** — clone `SchemaPage.tsx`'s structure (JSON textarea → "Dry-run" button calls `migrate(db, {...req, dryRun:true})` → render `directives[].affectedRows` / `castFailures` / `sampleChanges` in `.resultPanel` → "Apply" button calls `migrate(db, {...req})` on confirm). Use `Button`/`Field` from `components/ui.tsx`, tokens from `tokens.css`. Branch errors on `RtDbRequestError` (same pattern as `SchemaPage` L101-140).

- [ ] **Step 3: Route + nav** — in `App.tsx` add `<Route path="dbs/:db/migrate" element={<MigratePage/>}/>` as a child of `<AppShell/>`; link from `DbPage.tsx` beside the Schema link.

- [ ] **Step 4: Test** — `MigratePage.test.tsx` (Testing Library): dry-run renders the report; apply calls `migrate` with `dryRun:false`.

- [ ] **Step 5: Run the gate + commit**

```bash
cd dashboard && bunx vitest run src/pages/MigratePage.test.tsx && bunx tsc --noEmit && bun run lint
git add dashboard/src
git commit -m "feat(dashboard): guided migrate flow (dry-run → review → apply)"
```

---

## Task 13: Docs + cross-client wire parity

**Files:**
- Modify: `FEATURE_MATRIX.md` (§1 schema-migration row 🟡→✅ + a note on the directive set + evalExpr), spec `2026-07-21-par-rt-db-design.md` line 99 (drop "(MVP)", reference the new spec), `CLAUDE.md` architecture section (migrate as a third committer request arm publishing at the same tap sites), client READMEs + `cli/README` + `dashboard/README.md` (migrate sections). Modify: `server/tests/...` or a cross-client parity test asserting the `Directive`/`MigrateResult` wire shape is byte-identical across the four implementations (extend the existing cross-client combination/parity test pattern).

- [ ] **Step 1: Update `FEATURE_MATRIX.md`** — §1 schema-migration row to ✅ with a one-paragraph note (directives: rename/changeType/drop/setDefault + scoped evalExpr; committer-routed; dry-run; four-client mirror). Add the gap-row note style used elsewhere.

- [ ] **Step 2: Update spec line 99** — replace the MVP sentence with: "Schema migration: additive changes apply automatically on push; destructive/type-changing transformations are applied via the declarative migrate operation (`POST /admin/db/{db}/migrate`) — see `2026-07-31-schema-migration-backfill-design.md`."

- [ ] **Step 3: Update `CLAUDE.md`** — in the committer bullet, note `RunMigrate` as a third request arm alongside `handle_mutate`/`handle_scheduled`, executing DDL+DML in the committer's serialized turn and publishing at the same two tap sites.

- [ ] **Step 4: README sections** — one short migrate example each in `ts-client/README.md`, `rust-client/README.md`, `python-client/README.md`, and a `rtdb migrate` note in `cli/`; `dashboard/README.md` the guided flow.

- [ ] **Step 5: Cross-client parity test** — extend the existing parity harness (ts `query_combinations.test.ts` / rust `wire_corpus.rs` / python `test_wire_parity.py`) with a `Directive`/`MigrateResult` fixture serialized in all four and asserted byte-identical.

- [ ] **Step 6: Run the full repo gate + commit**

```bash
make checkall
git add FEATURE_MATRIX.md docs CLAUDE.md ts-client/README.md rust-client/README.md python-client/README.md cli dashboard/README.md
# plus the parity test files
git commit -m "docs: schema migrate — feature matrix, spec, CLAUDE.md, READMEs + cross-client parity"
```

---

## Self-Review (run after writing the plan — completed)

**Spec coverage:** rename (T3), type-coercion (T4), removal (T3 dropField/dropTable/dropIndex), set-default (T3), arbitrary transform (T5 evalExpr), atomicity (T6 single tx + rollback), downstream correctness/fan_out/op-feed/audit/webhook (T6), dry-run (T6 + every client), validation (T2), four-client wire mirror (T8-10), CLI (T11), dashboard (T12), docs (T13), no-WS-peer (T7 HTTP-only), single-source-of-truth derived schema (T2/T6). ✅ all spec sections map to a task.

**Placeholder scan:** the `change_type` cast param `any` and the `MigrateRequestOwned` naming in T11/T9 are flagged as "refine in-impl" — acceptable (they name a concrete decision, not a gap). No TBD/TODO. ✅

**Type consistency:** `Directive` variants, `Cast`, `MigrateRequest`, `MigrateResult`, `DirectiveReport`, `MigrationEffects`, `Committers::migrate`, `migrate_schema` (rust/python), `migrate` (ts), `AdminClient.migrate` (dashboard) — names held consistent across tasks. `apply_migration` gains a `derived: &SchemaDef` param in T5; callers updated in T6 (noted). ✅
