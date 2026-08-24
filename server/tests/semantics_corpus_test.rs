//! ENH-023: behavioral-semantics corpus runner (server view — Postgres-backed).
//!
//! Enumerates every `*.json` case in `wire-corpus/semantics/` (repo root — the
//! single source of truth; one self-contained case per file carrying its own
//! schema, seed, operation, and expected result) and executes each against a
//! real Postgres database, comparing normalized results. The same fixture
//! files are consumed by the ts-client, rust-client, and python-client
//! engines; the server is the source of truth for every expected value, so
//! this runner is both the fourth corpus consumer and the first live
//! validation of each fixture.
//!
//! The runner implements `wire-corpus/README.md`'s "How a runner executes a
//! case" algorithm exactly: runtime directory enumeration (the directory IS the
//! case count — no hardcoded constant), per-case fresh database, seed-via-
//! `execute_txn` with `$id` label capture, `{"$idRef": ...}` substitution
//! throughout `op`/`then.query`, the `"$prev"` paginate-cursor sentinel, error
//! cases asserting the `ErrorCode` wire name only, `normalize` projection
//! applied recursively to both trees, `unordered` multiset comparison via
//! canonical-JSON sort, numeric-tolerant equality, and structural
//! `expect_next_cursor` presence. No clocks are advanced and no reaper runs
//! between seeding and the op — the corpus pins synchronous semantics.
//!
//! Two additive case kinds (ENH-028): a `pushError` case asserts the schema
//! PUSH itself fails with the given code (push is the whole case — no seed, no
//! op), and an `op.migrate` case runs the real migrate machinery
//! (`plan_migration` → `apply_migration` on a pool tx, the committer's
//! `RunMigrate` shape minus committer-side quota/history/tap machinery) with
//! the `MigrateResult` compared like any op result; a follow-up `then` reads
//! against the DERIVED schema.

use std::collections::HashMap;
use std::path::PathBuf;

use crate::common::{test_state, wrap_test_db};
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::db;
use rtdb_server::ddl;
use rtdb_server::error::{ErrorCode, RtDbError};
use rtdb_server::migrate::{MigrateRequest, apply_migration, plan_migration};
use rtdb_server::query::{Query, QueryResult, execute_query};
use rtdb_server::schema::SchemaDef;
use rtdb_server::txn::{Step, Transaction, TxnOutcome, execute_txn};
use serde_json::{Map, Value};

/// System fields minted at run time and projected out of both sides unless a
/// case's `normalize` list replaces the default (README "Semantics corpus
/// format").
const DEFAULT_NORMALIZE: [&str; 3] = ["_id", "_creationTime", "_version"];

/// A valid db name is `^[a-z][a-z0-9_]{0,32}$` (max 33 bytes); kebab-case stems
/// must be sanitized and truncated, with a uuid suffix keeping names unique
/// across runs of the same case.
fn db_name_for(case: &str) -> String {
    let stem: String = case
        .chars()
        .take(17)
        .map(|c| {
            if c.is_ascii_lowercase() || c.is_ascii_digit() {
                c
            } else {
                '_'
            }
        })
        .collect();
    let uid = uuid::Uuid::now_v7().simple().to_string();
    format!("sc_{stem}_{}", &uid[..12])
}

/// Resolve one `seed` entry into `(table, doc, label)`. A wrapped entry is an
/// object with a `doc` key whose value is an object (with optional `table` and
/// `$id` siblings); any other object is a plain doc, legal only when the
/// schema declares exactly one table (the disambiguation rule the corpus
/// README states).
fn parse_seed_entry(
    entry: &Value,
    single_table: Option<&str>,
    case: &str,
) -> (String, Map<String, Value>, Option<String>) {
    let obj = entry
        .as_object()
        .unwrap_or_else(|| panic!("{case}: seed entry must be a JSON object"));
    if obj.get("doc").is_some_and(Value::is_object) {
        let table = match obj.get("table").and_then(Value::as_str) {
            Some(t) => t.to_string(),
            None => single_table.map(str::to_string).unwrap_or_else(|| {
                panic!("{case}: wrapped seed entry without `table` requires a single-table schema")
            }),
        };
        let label = obj.get("$id").and_then(Value::as_str).map(str::to_string);
        let doc = obj.get("doc").and_then(Value::as_object).cloned().unwrap();
        (table, doc, label)
    } else {
        let table = single_table
            .map(str::to_string)
            .unwrap_or_else(|| panic!("{case}: plain-doc seed requires a single-table schema"));
        (table, obj.clone(), None)
    }
}

/// Replace every `{"$idRef": "<label>"}` object anywhere in the tree with the
/// minted id recorded for that seed label (README "Substitution placeholders").
fn substitute(node: &Value, ids: &HashMap<String, String>, case: &str) -> Value {
    match node {
        Value::Object(m) => {
            if m.len() == 1 && m.contains_key("$idRef") {
                let label = m["$idRef"]
                    .as_str()
                    .unwrap_or_else(|| panic!("{case}: $idRef label must be a string"));
                let id = ids.get(label).unwrap_or_else(|| {
                    panic!("{case}: $idRef references unknown seed label '{label}'")
                });
                return Value::String(id.clone());
            }
            Value::Object(
                m.iter()
                    .map(|(k, v)| (k.clone(), substitute(v, ids, case)))
                    .collect(),
            )
        }
        Value::Array(a) => Value::Array(a.iter().map(|v| substitute(v, ids, case)).collect()),
        leaf => leaf.clone(),
    }
}

/// Remove every `keys` member from every object in the tree, recursively — the
/// README's `normalize` projection applies to every object in both the actual
/// and expected trees (docs inside `paginate.docs`, step results, ...).
fn project_recursive(node: &mut Value, keys: &[String]) {
    match node {
        Value::Object(m) => {
            for k in keys {
                m.remove(k);
            }
            for v in m.values_mut() {
                project_recursive(v, keys);
            }
        }
        Value::Array(a) => {
            for v in a.iter_mut() {
                project_recursive(v, keys);
            }
        }
        _ => {}
    }
}

/// Canonical JSON for the unordered multiset sort: compact serialization with
/// object keys sorted recursively. serde_json's `Map` is a `BTreeMap` here (the
/// `preserve_order` feature is not enabled), so plain serialization is exactly
/// that canonical form.
fn canonical(v: &Value) -> String {
    serde_json::to_string(v).expect("canonical serialize")
}

/// Compare two JSON values, treating two numbers as equal when their f64
/// representations match (so SQL `numeric` `6` and client `f64` `6.0` agree —
/// the same tolerance golden-vector applies).
fn json_eq_numeric(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            let xi = x.as_f64();
            let yi = y.as_f64();
            match (xi, yi) {
                (Some(xf), Some(yf)) => (xf - yf).abs() < f64::EPSILON || xf == yf,
                _ => x == y,
            }
        }
        (Value::Null, Value::Null) => true,
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(a, b)| json_eq_numeric(a, b))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .all(|(k, v)| y.get(k).is_some_and(|yv| json_eq_numeric(v, yv)))
        }
        _ => a == b,
    }
}

/// Assert actual == expected under `normalize` projection already applied:
/// `unordered` compares the two arrays as multisets (each side sorted by
/// canonical JSON, then element-wise numeric-tolerant), otherwise the values
/// compare in place, recursively numeric-tolerant.
fn assert_expected(got: &Value, want: &Value, unordered: bool, msg: &str) {
    if json_eq_numeric(got, want) {
        return; // equal as sequences — also covers every unordered case
    }
    if !unordered {
        panic!("{msg}\n got {got}\nwant {want}");
    }
    let (Some(g), Some(w)) = (got.as_array(), want.as_array()) else {
        panic!("{msg}: unordered comparison requires arrays — got {got}, want {want}");
    };
    if g.len() != w.len() {
        panic!(
            "{msg}: row count mismatch (unordered) — got {}, want {}",
            g.len(),
            w.len()
        );
    }
    let mut gs: Vec<&Value> = g.iter().collect();
    let mut ws: Vec<&Value> = w.iter().collect();
    gs.sort_by_key(|v| canonical(v));
    ws.sort_by_key(|v| canonical(v));
    for (i, (gv, wv)) in gs.iter().zip(ws.iter()).enumerate() {
        if !json_eq_numeric(gv, wv) {
            panic!("{msg}: row {i} mismatch (unordered compare)\n got {got}\nwant {want}");
        }
    }
    // Lengths equal and every sorted row matched: the multisets agree, so the
    // values differ only in order — exactly what `unordered` forgives.
}

/// The effective `normalize` key list for an expect block: a present list
/// REPLACES the default; absent falls back to `fallback` (the case-level list,
/// itself defaulted — `then` inherits the case's list unless it overrides).
fn normalize_keys(block: &Value, fallback: &[String], case: &str) -> Vec<String> {
    block
        .get("normalize")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .map(|v| {
                    v.as_str()
                        .unwrap_or_else(|| panic!("{case}: normalize entries must be strings"))
                        .to_string()
                })
                .collect()
        })
        .unwrap_or_else(|| fallback.to_vec())
}

/// Error-case assertion: only the code is compared, never the message.
fn assert_error_code(err: &RtDbError, expect: &Value, case: &str) {
    let want: ErrorCode = serde_json::from_value(expect["error"]["code"].clone())
        .unwrap_or_else(|e| panic!("{case}: expected error code does not parse: {e}"));
    assert_eq!(
        err.code, want,
        "{case}: error code mismatch — server message: {}",
        err.message
    );
}

/// Compare an op/then success result against its `expect` block: apply the
/// `normalize` projection to both trees, structurally assert `next_cursor`
/// presence when the case pins it (paginate), then ordered/unordered compare.
fn assert_result(case: &str, actual: Value, block: &Value, keys: &[String], unordered: bool) {
    let mut got = actual;
    let mut want = block["expect"].clone();
    if let Some(want_cursor) = block.get("expect_next_cursor").and_then(Value::as_bool) {
        let has = got.get("nextCursor").is_some();
        assert_eq!(
            has, want_cursor,
            "{case}: nextCursor presence mismatch (got {has}, want {want_cursor})"
        );
        let mut projected = keys.to_vec();
        projected.push("nextCursor".to_string());
        project_recursive(&mut got, &projected);
        project_recursive(&mut want, &projected);
    } else {
        project_recursive(&mut got, keys);
        project_recursive(&mut want, keys);
    }
    assert_expected(&got, &want, unordered, &format!("{case}: result mismatch"));
}

/// Execute one `op.migrate` case body the way the committer's `RunMigrate` arm
/// does — `plan_migration` (plus `validate`, the computed-field changeType
/// re-validation path) then `apply_migration` on a pool transaction, committing
/// for real or rolling back on `dryRun`. The committer-side machinery the
/// corpus runner cannot reach (quota gates, schema-history capture, tap
/// publication) is omitted — the same in-process shape the migrate/computed
/// integration tests exercise. Returns the derived schema (a follow-up `then`
/// reads against it) plus the serialized `MigrateResult` (`applied` / derived
/// `schema` / per-directive reports). Migrate-domain errors (plan, derived-
/// schema validation, apply) return `Err` so an error case can assert the
/// envelope; infrastructure failures panic.
async fn run_migrate(
    pool: &sqlx::PgPool,
    db_name: &str,
    schema: &SchemaDef,
    req: &MigrateRequest,
    case_name: &str,
) -> Result<(SchemaDef, Value), RtDbError> {
    let derived = plan_migration(schema, &req.directives)?;
    derived.validate()?;
    let mut tx = pool
        .begin()
        .await
        .unwrap_or_else(|e| panic!("{case_name}: begin migrate tx: {e:?}"));
    let fx = match apply_migration(&mut tx, db_name, &req.directives, &derived, req.dry_run).await {
        Ok(fx) => fx,
        // The open tx drops here, rolling back — an apply failure is a
        // migrate-domain error the case may assert, never a partial commit.
        Err(e) => return Err(e),
    };
    if req.dry_run {
        tx.rollback()
            .await
            .unwrap_or_else(|e| panic!("{case_name}: rollback dry-run: {e:?}"));
    } else {
        tx.commit()
            .await
            .unwrap_or_else(|e| panic!("{case_name}: commit migrate: {e:?}"));
    }
    let result = rtdb_server::migrate::MigrateResult {
        applied: !req.dry_run,
        schema: derived.clone(),
        directives: fx.reports,
    };
    let value = serde_json::to_value(&result).expect("serialize MigrateResult");
    Ok((derived, value))
}

/// Execute one corpus case end to end. Every failure names the case.
async fn run_case(pool: &sqlx::PgPool, case_name: &str, case: &Value) {
    // Fresh database per case: unique name derived from the case stem, RAII
    // guard held for the case's lifetime so cleanup cannot race the reads
    // (same pattern as golden_vector_test::seed_db).
    let db_name = db_name_for(case_name);
    db::create_database(pool, &db_name)
        .await
        .unwrap_or_else(|e| panic!("{case_name}: create database: {e:?}"));
    let _guard = wrap_test_db(db_name.clone());

    let mut schema: SchemaDef = serde_json::from_value(case["schema"].clone())
        .unwrap_or_else(|e| panic!("{case_name}: schema does not parse: {e}"));

    // A `pushError` case asserts the schema PUSH itself fails (README format:
    // the value carries the same `{code}` object `expect.error` does; only the
    // code is asserted, never the message). Push is the whole case — a
    // stray seed/op/then/expect is an authoring error.
    if let Some(push_err) = case.get("pushError") {
        for stray in ["seed", "op", "then", "expect"] {
            assert!(
                case.get(stray).is_none(),
                "{case_name}: a pushError case must not carry `{stray}` — push is the whole case"
            );
        }
        let want: ErrorCode = serde_json::from_value(push_err["code"].clone())
            .unwrap_or_else(|e| panic!("{case_name}: pushError.code does not parse: {e}"));
        let err = ddl::push_schema(pool, &db_name, schema)
            .await
            .err()
            .unwrap_or_else(|| panic!("{case_name}: pushError case — the push must fail"));
        assert_eq!(
            err.code, want,
            "{case_name}: push error code mismatch — server message: {}",
            err.message
        );
        return;
    }

    ddl::push_schema(pool, &db_name, schema.clone())
        .await
        .unwrap_or_else(|e| panic!("{case_name}: push_schema: {e:?}"));

    let single_table =
        (schema.tables.len() == 1).then(|| schema.tables.keys().next().cloned().unwrap());

    // Seed in array order through the normal insert path, recording
    // `label -> minted id` for `$id`-labeled entries.
    let mut ids: HashMap<String, String> = HashMap::new();
    let seed = case["seed"]
        .as_array()
        .unwrap_or_else(|| panic!("{case_name}: seed must be an array"));
    for (i, entry) in seed.iter().enumerate() {
        let (table, doc, label) = parse_seed_entry(entry, single_table.as_deref(), case_name);
        let txn = Transaction {
            steps: vec![Step::Insert {
                table: table.clone(),
                doc,
            }],
        };
        let outcome: TxnOutcome =
            execute_txn(pool, &db_name, &schema, &txn, &PrincipalCtx::bypass())
                .await
                .unwrap_or_else(|e| panic!("{case_name}: seed #{i} into '{table}': {e:?}"));
        if let Some(label) = label {
            let id = outcome.results[0]
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or_else(|| panic!("{case_name}: seed #{i}: insert result missing id"))
                .to_string();
            ids.insert(label, id);
        }
    }

    let expect = case
        .get("expect")
        .unwrap_or_else(|| panic!("{case_name}: missing expect"));
    let expects_error = expect.pointer("/error/code").is_some();
    let case_keys = normalize_keys(case, &DEFAULT_NORMALIZE.map(|s| s.to_string()), case_name);

    // Execute the op. A query op first resolves the `"$prev"` paginate-cursor
    // sentinel (README step 4): run the cursor-less query, take its
    // nextCursor, then run the real query with it. `expect` describes the
    // SECOND page.
    let op_result: Value = if let Some(txn_json) = case.pointer("/op/txn") {
        let txn: Transaction = serde_json::from_value(substitute(txn_json, &ids, case_name))
            .unwrap_or_else(|e| panic!("{case_name}: op.txn does not parse: {e}"));
        match execute_txn(pool, &db_name, &schema, &txn, &PrincipalCtx::bypass()).await {
            Ok(outcome) => Value::Array(outcome.results),
            Err(e) => {
                if !expects_error {
                    panic!(
                        "{case_name}: unexpected txn error ({:?}): {}",
                        e.code, e.message
                    );
                }
                assert_error_code(&e, expect, case_name);
                return; // a failed op has no `then` follow-up
            }
        }
    } else if let Some(query_json) = case.pointer("/op/query") {
        let mut q_json = substitute(query_json, &ids, case_name);
        if q_json.pointer("/paginate/cursor").and_then(Value::as_str) == Some("$prev") {
            let mut first_json = q_json.clone();
            first_json
                .pointer_mut("/paginate")
                .and_then(Value::as_object_mut)
                .expect("paginate cursor sentinel without paginate block")
                .remove("cursor");
            let first: Query = serde_json::from_value(first_json).unwrap_or_else(|e| {
                panic!("{case_name}: $prev first-page query does not parse: {e}")
            });
            let first_result = execute_query(
                pool,
                &db_name,
                &schema,
                &first,
                &PrincipalCtx::bypass(),
                false,
            )
            .await
            .unwrap_or_else(|e| panic!("{case_name}: $prev first page: {e:?}"));
            let cursor = match first_result {
                QueryResult::Paginated(p) => p.next_cursor,
                other => panic!("{case_name}: $prev first page: expected Paginated, got {other:?}"),
            }
            .unwrap_or_else(|| panic!("{case_name}: $prev: first page has no nextCursor"));
            *q_json
                .pointer_mut("/paginate/cursor")
                .expect("cursor slot vanished") = Value::String(cursor);
        }
        let q: Query = serde_json::from_value(q_json)
            .unwrap_or_else(|e| panic!("{case_name}: op.query does not parse: {e}"));
        match execute_query(pool, &db_name, &schema, &q, &PrincipalCtx::bypass(), false).await {
            Ok(r) => serde_json::to_value(&r).expect("serialize query result"),
            Err(e) => {
                if !expects_error {
                    panic!(
                        "{case_name}: unexpected query error ({:?}): {}",
                        e.code, e.message
                    );
                }
                assert_error_code(&e, expect, case_name);
                return; // a failed op has no `then` follow-up
            }
        }
    } else if let Some(migrate_json) = case.pointer("/op/migrate") {
        let req: MigrateRequest = serde_json::from_value(substitute(migrate_json, &ids, case_name))
            .unwrap_or_else(|e| panic!("{case_name}: op.migrate does not parse: {e}"));
        match run_migrate(pool, &db_name, &schema, &req, case_name).await {
            // The derived schema replaces the case schema for `then` follow-ups
            // — post-migrate reads resolve fields through it.
            Ok((derived, result)) => {
                schema = derived;
                result
            }
            Err(e) => {
                if !expects_error {
                    panic!(
                        "{case_name}: unexpected migrate error ({:?}): {}",
                        e.code, e.message
                    );
                }
                assert_error_code(&e, expect, case_name);
                return; // a failed op has no `then` follow-up
            }
        }
    } else {
        panic!("{case_name}: op must carry `query`, `txn`, or `migrate`");
    };

    assert!(
        !expects_error,
        "{case_name}: expected error {:?}, got success {op_result}",
        expect["error"]["code"]
    );
    let unordered = case
        .get("unordered")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    assert_result(case_name, op_result, case, &case_keys, unordered);

    // Follow-up read after a successful op (write-then-read visibility cases).
    if let Some(then) = case.get("then") {
        let q_json = substitute(
            then.get("query")
                .unwrap_or_else(|| panic!("{case_name}: then requires query")),
            &ids,
            case_name,
        );
        let q: Query = serde_json::from_value(q_json)
            .unwrap_or_else(|e| panic!("{case_name}: then.query does not parse: {e}"));
        let r = execute_query(pool, &db_name, &schema, &q, &PrincipalCtx::bypass(), false)
            .await
            .unwrap_or_else(|e| panic!("{case_name}: then.query: {e:?}"));
        let actual = serde_json::to_value(&r).expect("serialize then result");
        let keys = normalize_keys(then, &case_keys, case_name);
        let unordered = then
            .get("unordered")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        assert_result(case_name, actual, then, &keys, unordered);
    }
}

#[tokio::test]
async fn semantics_corpus_runner() -> anyhow::Result<()> {
    // Enumerate the corpus at RUNTIME — the directory IS the count ("bumped
    // only by adding files", never by editing a constant here).
    let dir = std::fs::read_dir(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../wire-corpus/semantics"
    ))
    .expect("read wire-corpus/semantics directory");
    let mut files: Vec<PathBuf> = dir
        .map(|e| e.expect("read dir entry").path())
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    files.sort();
    assert!(
        !files.is_empty(),
        "wire-corpus/semantics contains no fixture files"
    );

    let state = test_state().await;
    let pool = state.pool.clone();

    let mut executed = 0usize;
    let mut skipped = 0usize;
    for path in &files {
        let stem = path
            .file_stem()
            .expect("fixture file stem")
            .to_string_lossy()
            .to_string();
        let raw = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("{stem}: read {}: {e}", path.display()));
        let case: Value =
            serde_json::from_str(&raw).unwrap_or_else(|e| panic!("{stem}: parse: {e}"));
        let name = case["name"]
            .as_str()
            .unwrap_or_else(|| panic!("{stem}: missing name"))
            .to_string();
        assert_eq!(
            name, stem,
            "{stem}: case `name` must equal the filename stem"
        );
        // A named runner may skip loudly, with the reason surfaced here.
        if let Some(reason) = case.pointer("/skip/server").and_then(Value::as_str) {
            eprintln!("skip: {stem} ({reason})");
            skipped += 1;
            continue;
        }
        run_case(&pool, &stem, &case).await;
        executed += 1;
        eprintln!("ok: {stem}");
    }
    // Every parsed file was executed or explicitly skipped — nothing dropped.
    assert_eq!(
        executed + skipped,
        files.len(),
        "every corpus file must be executed or explicitly skipped"
    );
    eprintln!(
        "semantics corpus: {} files, {} executed, {} skipped",
        files.len(),
        executed,
        skipped
    );
    Ok(())
}
