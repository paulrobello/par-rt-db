//! ENH-023: behavioral-semantics corpus runner (rust-client in-memory view).
//!
//! Enumerates every `*.json` case in `wire-corpus/semantics/` (repo root — the
//! single source of truth; one self-contained case per file carrying its own
//! schema, seed, operation, and expected result) and executes each against a
//! fresh in-memory engine instance, comparing normalized results. The same
//! fixture files are consumed by the server (Postgres), ts-client, and
//! python-client runners; the server is the source of truth for every expected
//! value, so a divergence here is an engine bug (or a stale fixture).
//!
//! The runner implements `wire-corpus/README.md`'s "How a runner executes a
//! case" algorithm exactly, mirroring the server runner
//! (`server/tests/semantics_corpus_test.rs` — the reference implementation):
//! runtime directory enumeration (the directory IS the case count — no
//! hardcoded constant), per-case fresh instance, seed through the normal
//! `mutate` insert path with `$id` label capture, `{"$idRef": ...}`
//! substitution throughout `op`/`then.query`, the `"$prev"` paginate-cursor
//! sentinel, error cases asserting the `ErrorCode` wire name only, `normalize`
//! projection applied recursively to both trees, `unordered` multiset
//! comparison via canonical-JSON sort, numeric-tolerant equality, and
//! structural `expect_next_cursor` presence. The engine is driven only through
//! its public surface (`push_schema` + `run_query`/`mutate`), never internals,
//! and no clock advance / scheduler tick / TTL reap happens between seeding
//! and the op — the corpus pins synchronous semantics.
//!
//! Two additive case kinds (ENH-028, mirroring the server runner): a
//! `pushError` case asserts the schema PUSH itself fails with the given code
//! (push is the whole case — no seed, no op), and an `op.migrate` case runs
//! the engine's `migrate_schema` (the InMemoryMigrate port — plan fold +
//! derived-schema validation + apply, apply-persisted unless `dryRun`) with
//! the `MigrateResult` compared like any op result; a follow-up `then` reads
//! against the DERIVED schema.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};

use par_rt_db_client::in_memory::InMemoryRtDbClientOptions;
use par_rt_db_client::mutation::Step;
use par_rt_db_client::schema::SchemaDef;
use par_rt_db_client::wire::admin::MigrateRequestOwned;
use par_rt_db_client::{ErrorCode, InMemoryRtDbClient, Query, StepResult, Transaction};
use serde_json::{Map, Value};

/// System fields minted at run time and projected out of both sides unless a
/// case's `normalize` list replaces the default (README "Semantics corpus
/// format").
const DEFAULT_NORMALIZE: [&str; 3] = ["_id", "_creationTime", "_version"];

/// Deterministic clock shared by every case's engine: a monotonically
/// increasing counter (mirrors `golden_vector.rs`) so each insert mints a
/// distinct `_id`/`_creationTime` even with the constant RNG below — ids are
/// timestamp-derived, and normalize projects them out anyway.
static CLOCK: AtomicI64 = AtomicI64::new(1_700_000_000_000);

/// A fresh in-memory instance per case with the deterministic clock + constant
/// RNG injected (the same harness golden_vector uses).
fn new_client() -> InMemoryRtDbClient {
    InMemoryRtDbClient::new(
        InMemoryRtDbClientOptions::default()
            .now(|| CLOCK.fetch_add(1, Ordering::SeqCst))
            .random(|| 0.0),
    )
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
        (Value::Number(x), Value::Number(y)) => match (x.as_f64(), y.as_f64()) {
            (Some(xf), Some(yf)) => (xf - yf).abs() < f64::EPSILON || xf == yf,
            _ => x == y,
        },
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
fn assert_error_code(err: &par_rt_db_client::RtDbError, expect: &Value, case: &str) {
    let want: ErrorCode = serde_json::from_value(expect["error"]["code"].clone())
        .unwrap_or_else(|e| panic!("{case}: expected error code does not parse: {e}"));
    assert_eq!(
        err.code, want,
        "{case}: error code mismatch — engine message: {}",
        err.message
    );
}

/// Compare an op/then success result against its `expect` block: apply the
/// `normalize` projection to both trees, structurally assert `nextCursor`
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

/// Execute a query op through the engine: substitute placeholders first (README
/// step 3), then resolve the `"$prev"` paginate-cursor sentinel when present
/// (README step 4) — run the cursor-less query, take its `nextCursor` (fail
/// loudly if absent), then run the query with it — `expect` describes the
/// SECOND page. The first-page query runs on the substituted tree, matching
/// the server reference runner's step order.
fn execute_query(
    client: &InMemoryRtDbClient,
    q_json: Value,
    ids: &HashMap<String, String>,
    case: &str,
) -> Result<Value, par_rt_db_client::RtDbError> {
    let mut q_json = substitute(&q_json, ids, case);
    if q_json.pointer("/paginate/cursor").and_then(Value::as_str) == Some("$prev") {
        let mut first_json = q_json.clone();
        first_json
            .pointer_mut("/paginate")
            .and_then(Value::as_object_mut)
            .unwrap_or_else(|| panic!("{case}: paginate cursor sentinel without paginate block"))
            .remove("cursor");
        let first: Query = serde_json::from_value(first_json)
            .unwrap_or_else(|e| panic!("{case}: $prev first-page query does not parse: {e}"));
        let first_result = client
            .run_query(&first)
            .unwrap_or_else(|e| panic!("{case}: $prev first page: {e:?}"));
        let cursor = first_result
            .get("nextCursor")
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("{case}: $prev: first page has no nextCursor"))
            .to_string();
        *q_json
            .pointer_mut("/paginate/cursor")
            .expect("cursor slot vanished") = Value::String(cursor);
    }
    let q: Query = serde_json::from_value(q_json)
        .unwrap_or_else(|e| panic!("{case}: op.query does not parse: {e}"));
    client.run_query(&q)
}

/// Execute one corpus case end to end through the engine's public surface.
/// Every failure names the case.
async fn run_case(case_name: &str, case: &Value) {
    let schema: SchemaDef = serde_json::from_value(case["schema"].clone())
        .unwrap_or_else(|e| panic!("{case_name}: schema does not parse: {e}"));
    let mut client = new_client();

    // A `pushError` case asserts the schema PUSH itself fails (the value
    // carries the same `{code}` object `expect.error` does; only the code is
    // asserted, never the message). Push is the whole case — a stray
    // seed/op/then/expect is an authoring error.
    if let Some(push_err) = case.get("pushError") {
        for stray in ["seed", "op", "then", "expect"] {
            assert!(
                case.get(stray).is_none(),
                "{case_name}: a pushError case must not carry `{stray}` — push is the whole case"
            );
        }
        let want: ErrorCode = serde_json::from_value(push_err["code"].clone())
            .unwrap_or_else(|e| panic!("{case_name}: pushError.code does not parse: {e}"));
        let err = client
            .push_schema(&schema)
            .err()
            .unwrap_or_else(|| panic!("{case_name}: pushError case — the push must fail"));
        assert_eq!(
            err.code, want,
            "{case_name}: push error code mismatch — engine message: {}",
            err.message
        );
        return;
    }

    client
        .push_schema(&schema)
        .unwrap_or_else(|e| panic!("{case_name}: push_schema: {e:?}"));

    let single_table =
        (schema.tables.len() == 1).then(|| schema.tables.keys().next().cloned().unwrap());

    // Seed in array order through the normal insert path (`mutate` with a
    // single Insert step), recording `label -> minted id` for `$id`-labeled
    // entries.
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
        let results = client
            .mutate(&txn, None)
            .await
            .unwrap_or_else(|e| panic!("{case_name}: seed #{i} into '{table}': {e:?}"));
        let id = match &results[0] {
            StepResult::Insert { id } => id.clone(),
            other => panic!("{case_name}: seed #{i}: expected Insert, got {other:?}"),
        };
        if let Some(label) = label {
            ids.insert(label, id);
        }
    }

    let expect = case
        .get("expect")
        .unwrap_or_else(|| panic!("{case_name}: missing expect"));
    let expects_error = expect.pointer("/error/code").is_some();
    let case_keys = normalize_keys(case, &DEFAULT_NORMALIZE.map(|s| s.to_string()), case_name);

    // Execute the op. Error cases assert the code and stop (no `then`).
    let op_result: Value = if let Some(txn_json) = case.pointer("/op/txn") {
        let txn: Transaction = serde_json::from_value(substitute(txn_json, &ids, case_name))
            .unwrap_or_else(|e| panic!("{case_name}: op.txn does not parse: {e}"));
        match client.mutate(&txn, None).await {
            Ok(results) => serde_json::to_value(&results).expect("serialize step results"),
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
    } else if case.pointer("/op/query").is_some() {
        match execute_query(
            &client,
            case.pointer("/op/query")
                .expect("op.query presence checked above")
                .clone(),
            &ids,
            case_name,
        ) {
            Ok(r) => r,
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
        // The admin MigrateRequest wire shape (`{directives, dryRun}`),
        // routed to the engine's migrate_schema (the InMemoryMigrate port):
        // directive fold + derived-schema validation, then apply — persisted
        // for real, or rolled back on `dryRun`. A follow-up `then` reads
        // against the derived schema + data effects (the engine's live
        // schema was swapped by the migrate).
        let req: MigrateRequestOwned =
            serde_json::from_value(substitute(migrate_json, &ids, case_name))
                .unwrap_or_else(|e| panic!("{case_name}: op.migrate does not parse: {e}"));
        match client.migrate_schema(&req.directives, req.dry_run) {
            Ok(result) => serde_json::to_value(&result).expect("serialize MigrateResult"),
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
        let actual = client
            .run_query(&q)
            .unwrap_or_else(|e| panic!("{case_name}: then.query: {e:?}"));
        let keys = normalize_keys(then, &case_keys, case_name);
        let unordered = then
            .get("unordered")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        assert_result(case_name, actual, then, &keys, unordered);
    }
}

#[tokio::test]
async fn semantics_corpus_runner() {
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
        if let Some(reason) = case.pointer("/skip/rust").and_then(Value::as_str) {
            eprintln!("skip: {stem} ({reason})");
            skipped += 1;
            continue;
        }
        run_case(&stem, &case).await;
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
}
