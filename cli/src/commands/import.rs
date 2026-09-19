//! `rtdb import` — bulk-load a JSONL file into one table of `--db` with a
//! normal machine token. One JSON object per line, wrapped into bounded
//! insert/upsert transactions that respect the server's per-txn step caps
//! (`MAX_STEPS` = 1024 on the server), progress on stderr, non-zero exit on
//! the first failed batch with the offending line range — earlier committed
//! batches stay committed (documented, not rolled back). `--dry-run` fetches
//! the pushed schema (`GET /api/db/{db}/schema`) and validates every line
//! with the rust client's corpus-verified `validate_doc` — the same check the
//! server runs at insert time — without writing anything.

use std::path::Path;

use anyhow::{Context, Result, anyhow};
use par_rt_db_client::Mutation;
use par_rt_db_client::schema::{SchemaDef, TableDef};
use serde_json::Value;

use crate::args::Cli;
use crate::output::map_err;

use super::{data_client, require_db, require_token};

/// Default batch size: lines per transaction. Leaves a wide margin under the
/// server's 1024-step cap, since every imported line is exactly one step.
pub(crate) const DEFAULT_BATCH: usize = 500;

/// Hard batch ceiling — above the server's 1024 `MAX_STEPS` a transaction is
/// rejected wholesale, so clamping here turns that into a local argument error.
pub(crate) const MAX_BATCH: usize = 1000;

/// What happens when an imported row already exists. `Insert` is the default;
/// the two upsert modes key on `--key` (a single-field index).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Plain insert steps; the server mints ids.
    Insert,
    /// Upsert steps: on a `--key` match, merge the whole body into the row.
    Update,
    /// Upsert steps: on a `--key` match, leave the existing row untouched
    /// (an upsert whose patch body is empty).
    Skip,
}

/// A parsed line: its 1-based line number in the file (blank lines are
/// skipped but keep counting) plus the JSON object it holds.
type Line = (usize, Value);

pub(crate) async fn run_import(
    cli: &Cli,
    table: &str,
    file: &Path,
    on_conflict: Option<&str>,
    key: Option<&str>,
    batch: usize,
    dry_run: bool,
) -> Result<()> {
    let db = require_db(cli)?;
    let token = require_token(cli)?;
    let mode = resolve_mode(on_conflict, key)?;
    let batch = batch.clamp(1, MAX_BATCH);
    let content =
        std::fs::read_to_string(file).with_context(|| format!("reading {}", file.display()))?;
    let lines = parse_jsonl(&content)?;
    let client = data_client(cli, &db, &token);

    // `--dry-run` needs the schema; `Skip`/`Update` need it to resolve the
    // index backing `--key`. Plain insert mode validates server-side and
    // needs no extra round trip.
    let schema = if dry_run || mode != Mode::Insert {
        Some(
            client
                .fetch_schema()
                .await
                .map_err(map_err)
                .context("fetching the pushed schema")?,
        )
    } else {
        None
    };

    if dry_run {
        let schema = schema.expect("schema fetched for dry run");
        let table_def = resolve_table(&schema, table)?;
        for (n, doc) in &lines {
            validate_line(table_def, mode, key, doc).map_err(|e| anyhow!("line {n}: {e}"))?;
        }
        println!(
            "{} lines valid against table '{table}' (dry run — nothing written)",
            lines.len()
        );
        return Ok(());
    }

    if lines.is_empty() {
        eprintln!(
            "import: nothing to do — no document lines in {}",
            file.display()
        );
        return Ok(());
    }

    // Plain insert mode never resolves an index; the upsert modes required a
    // schema above, so the key index is resolved from it.
    let key_index = match (mode, key) {
        (Mode::Insert, _) => None,
        (_, Some(key)) => {
            let schema = schema.as_ref().expect("schema fetched for upsert mode");
            Some(resolve_key_index(schema, table, key)?)
        }
        (_, None) => unreachable!("resolve_mode pairs every non-insert mode with a key"),
    };

    let total = lines.len();
    let total_batches = total.div_ceil(batch);
    for (i, chunk) in lines.chunks(batch).enumerate() {
        let first = chunk[0].0;
        let last = chunk[chunk.len() - 1].0;
        let txn = build_txn(table, mode, key_index.as_deref(), key, chunk);
        client
            .mutate(&txn, None)
            .await
            .map_err(|e| {
                anyhow!(
                    "batch {}/{total_batches} (lines {first}-{last}) failed: {}\nBatches before this one remain committed; fix the file (or re-run with --dry-run to find the first bad line) and import the remainder.",
                    i + 1,
                    map_err(e)
                )
            })?;
        eprintln!(
            "import: batch {}/{total_batches} committed ({}/{} rows)",
            i + 1,
            ((i + 1) * batch).min(total),
            total
        );
    }
    eprintln!("import: done — {total} rows into {db}.{table}");
    Ok(())
}

/// Resolve `--on-conflict` / `--key` into a [`Mode`], rejecting the invalid
/// combinations before any I/O.
fn resolve_mode(on_conflict: Option<&str>, key: Option<&str>) -> Result<Mode> {
    match (on_conflict, key) {
        (None, Some(_)) => Err(anyhow!("--key requires --on-conflict update|skip")),
        (Some(_), None) => Err(anyhow!("--on-conflict requires --key <field>")),
        (Some(x), Some(_)) => match x {
            "update" => Ok(Mode::Update),
            "skip" => Ok(Mode::Skip),
            other => Err(anyhow!(
                "invalid --on-conflict '{other}' (expected 'update' or 'skip')"
            )),
        },
        (None, None) => Ok(Mode::Insert),
    }
}

/// Parse JSONL content into `(line number, object)` pairs. Blank lines are
/// skipped (but keep counting toward line numbers); anything else must be one
/// JSON object per line.
fn parse_jsonl(content: &str) -> Result<Vec<Line>> {
    let mut out = Vec::new();
    for (idx, line) in content.lines().enumerate() {
        let n = idx + 1;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: Value =
            serde_json::from_str(trimmed).with_context(|| format!("line {n}: invalid JSON"))?;
        if !value.is_object() {
            return Err(anyhow!("line {n}: every line must be a JSON object"));
        }
        out.push((n, value));
    }
    Ok(out)
}

fn resolve_table<'a>(schema: &'a SchemaDef, table: &str) -> Result<&'a TableDef> {
    schema.tables.get(table).ok_or_else(|| {
        anyhow!("table '{table}' is not in the pushed schema — push it first (rtdb push-schema)")
    })
}

/// One line's validation against the pushed schema — the same doc-level check
/// the server's insert path runs (unknown/`_`-reserved fields, required
/// fields, declared types), plus the upsert modes' key presence.
fn validate_line(
    table_def: &TableDef,
    mode: Mode,
    key: Option<&str>,
    doc: &Value,
) -> Result<(), String> {
    if let Some(key) = key
        && mode != Mode::Insert
        && doc.get(key).is_none_or(Value::is_null)
    {
        return Err(format!("--key '{key}' is missing (or null) on this line"));
    }
    par_rt_db_client::in_memory::validate_doc(table_def, doc)
        .map_err(|e| format!("{}: {}", map_err_code(&e), e.message))
}

/// Serialize just the wire code of an `RtDbError` (mirrors `output::map_err`).
fn map_err_code(e: &par_rt_db_client::RtDbError) -> String {
    serde_json::to_value(e.code)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("{:?}", e.code))
}

/// Resolve `--key` to the single-field btree index the server's upsert step
/// locates rows with. Search/vector and multi-field indexes are rejected
/// upfront — the server would reject the step later with a coarser error.
fn resolve_key_index(schema: &SchemaDef, table: &str, key: &str) -> Result<String> {
    let table_def = resolve_table(schema, table)?;
    table_def
        .indexes
        .iter()
        .find(|ix| !ix.search && ix.vector.is_none() && ix.fields.len() == 1 && ix.fields[0] == key)
        .map(|ix| ix.name.clone())
        .ok_or_else(|| {
            anyhow!(
                "--key '{key}' must name a single-field index on '{table}' (a unique index is recommended — a non-unique key aborts the batch when it matches several rows)"
            )
        })
}

fn build_txn(
    table: &str,
    mode: Mode,
    key_index: Option<&str>,
    key: Option<&str>,
    chunk: &[Line],
) -> par_rt_db_client::Transaction {
    let mut m = Mutation::new();
    for (_, doc) in chunk {
        match mode {
            Mode::Insert => {
                m = m.insert(table, doc.clone());
            }
            Mode::Update | Mode::Skip => {
                let key = key.expect("resolve_mode pairs every upsert mode with a key");
                let eq = [doc.get(key).cloned().unwrap_or(Value::Null)];
                let index = key_index.expect("key index resolved for upsert mode");
                let patch = match mode {
                    Mode::Update => doc.clone(),
                    _ => serde_json::json!({}),
                };
                m = m.upsert(table, index, &eq, doc.clone(), patch);
            }
        }
    }
    m.build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use par_rt_db_client::schema::{FieldType, SchemaBuilder, TableBuilder};

    fn table_def() -> TableDef {
        // Built through the client's own DSL builder (TableDef has no
        // `Default`) — the same shape a pushed schema round-trips as.
        let schema = SchemaBuilder::new()
            .table(
                "items",
                TableBuilder::new()
                    .field("slug", FieldType::String)
                    .field("qty", FieldType::Number)
                    .index("by_slug", &["slug"])
                    .unique(),
            )
            .build();
        schema.tables.get("items").expect("items declared").clone()
    }

    #[test]
    fn parse_jsonl_numbers_lines_and_skips_blanks() {
        let content = "\n{\"a\":1}\n\n{\"b\":2}\nnot json\n";
        let err = parse_jsonl(content).unwrap_err().to_string();
        assert!(err.contains("line 5"), "got: {err}");

        let ok = parse_jsonl("{\"a\":1}\n\n{\"b\":2}\n").unwrap();
        assert_eq!(
            ok,
            vec![
                (1, serde_json::json!({"a":1})),
                (3, serde_json::json!({"b":2}))
            ]
        );
    }

    #[test]
    fn parse_jsonl_rejects_non_object_lines() {
        let err = parse_jsonl("[1,2]\n").unwrap_err().to_string();
        assert!(err.contains("line 1"), "got: {err}");
        assert!(err.contains("JSON object"), "got: {err}");
    }

    #[test]
    fn resolve_mode_rejects_partial_and_unknown_combinations() {
        assert!(resolve_mode(None, Some("k")).is_err());
        assert!(resolve_mode(Some("update"), None).is_err());
        assert!(resolve_mode(Some("replace"), Some("k")).is_err());
        assert!(matches!(resolve_mode(None, None), Ok(Mode::Insert)));
        assert!(matches!(
            resolve_mode(Some("skip"), Some("k")),
            Ok(Mode::Skip)
        ));
    }

    #[test]
    fn dry_run_validation_names_line_and_reason() {
        let table_def = table_def();
        // Unknown field → SCHEMA_VIOLATION from validate_doc.
        let err = validate_line(
            &table_def,
            Mode::Insert,
            None,
            &serde_json::json!({"slug": "a", "nope": 1}),
        )
        .unwrap_err();
        assert!(err.contains("SCHEMA_VIOLATION"), "got: {err}");
        // Upsert mode without the key on the line.
        let err = validate_line(
            &table_def,
            Mode::Update,
            Some("slug"),
            &serde_json::json!({"qty": 1}),
        )
        .unwrap_err();
        assert!(err.contains("--key 'slug'"), "got: {err}");
        // A good line passes.
        assert!(
            validate_line(
                &table_def,
                Mode::Insert,
                None,
                &serde_json::json!({"slug": "a", "qty": 1})
            )
            .is_ok()
        );
    }

    #[test]
    fn resolve_key_index_requires_single_field_btree() {
        let mut schema = SchemaDef::default();
        schema.tables.insert("items".to_string(), table_def());
        assert_eq!(
            resolve_key_index(&schema, "items", "slug").unwrap(),
            "by_slug"
        );
        let err = resolve_key_index(&schema, "items", "qty")
            .unwrap_err()
            .to_string();
        assert!(err.contains("single-field index"), "got: {err}");
        assert!(resolve_key_index(&schema, "other", "slug").is_err());
    }

    #[test]
    fn build_txn_emits_one_step_per_line() {
        let lines = vec![
            (1, serde_json::json!({"slug": "a", "qty": 1})),
            (2, serde_json::json!({"slug": "b", "qty": 2})),
        ];
        let insert_txn = build_txn("items", Mode::Insert, None, None, &lines);
        assert_eq!(insert_txn.steps.len(), 2);

        let upsert_txn = build_txn("items", Mode::Update, Some("by_slug"), Some("slug"), &lines);
        assert_eq!(upsert_txn.steps.len(), 2);
    }

    #[test]
    fn batch_argument_clamps_to_the_server_step_cap() {
        // MAX_BATCH must stay under the server's 1024-step transaction cap.
        const { assert!(MAX_BATCH < 1024) }
        assert_eq!(0usize.clamp(1, MAX_BATCH), 1);
        assert_eq!(50_000usize.clamp(1, MAX_BATCH), MAX_BATCH);
        assert_eq!(DEFAULT_BATCH, 500);
    }
}
