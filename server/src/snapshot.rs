//! Database snapshot and restore over a JSONL wire format. A snapshot is a
//! leading `schema` line carrying the pushed `SchemaDef`, followed by one `doc`
//! line per stored document (raw `doc` jsonb plus its `id`/`createdAt`/`version`
//! columns). Restore replays a snapshot into a fresh database through the normal
//! `push_schema` + `insert_snapshot_row` path. Distinct from the `pg_dump`-based
//! backup/restore in `backup` / `admin/backups`.

use sqlx::PgPool;

use crate::db::validate_db_name;
use crate::ddl::{pg_schema, pg_table, push_schema, reposition_sequence};
use crate::error::RtDbError;
use crate::schema::{SchemaDef, SchemaDefExt};
use crate::txn::insert_snapshot_row;

/// One line of a database snapshot's JSONL wire format: a leading `schema` line
/// carries the pushed `SchemaDef`, followed by one `doc` line per stored document
/// (raw `doc` jsonb plus its `id`/`createdAt`/`version` columns — see `query.rs`'s
/// `merge_doc` for how these become `_id`/`_creationTime`/`_version` on read).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum SnapshotLine {
    Schema {
        schema: SchemaDef,
    },
    Doc {
        table: String,
        id: String,
        doc: serde_json::Map<String, serde_json::Value>,
        #[serde(rename = "createdAt")]
        created_at: i64,
        version: i64,
    },
}

/// Renders `db`'s current schema and every row of every table as JSONL: a `schema`
/// line first, then `doc` lines in schema-table order (`SchemaDef::tables` is a
/// `BTreeMap`, so this is deterministic), rows within a table ordered by
/// `(created_at, id)` to match `query.rs`'s default sort.
pub async fn export_database(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
) -> Result<String, RtDbError> {
    validate_db_name(db)?;
    let pg_schema_name = pg_schema(db);
    let mut out = String::new();

    let schema_line = SnapshotLine::Schema {
        schema: schema.clone(),
    };
    out.push_str(&serde_json::to_string(&schema_line).map_err(|err| {
        tracing::error!(error = %err, db, "failed to serialize snapshot schema line");
        RtDbError::internal("failed to serialize snapshot schema line")
    })?);
    out.push('\n');

    for table_name in schema.tables.keys() {
        let table_ident = pg_table(table_name);
        let rows: Vec<(String, serde_json::Value, i64, i64)> = sqlx::query_as(&format!(
            "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\" ORDER BY \"created_at\", \"id\""
        ))
        .fetch_all(pool)
        .await?;

        for (id, doc_value, created_at, version) in rows {
            let doc = match doc_value {
                serde_json::Value::Object(map) => map,
                _ => return Err(RtDbError::internal("stored doc is not a JSON object")),
            };
            let line = SnapshotLine::Doc {
                table: table_name.clone(),
                id,
                doc,
                created_at,
                version,
            };
            out.push_str(&serde_json::to_string(&line).map_err(|err| {
                tracing::error!(error = %err, db, table = %table_name, "failed to serialize snapshot doc line");
                RtDbError::internal("failed to serialize snapshot doc line")
            })?);
            out.push('\n');
        }
    }

    Ok(out)
}

/// ARC-002 guard for [`import_database`]: rejects a target that already holds
/// documents. Import writes document tables directly (no committer), so a
/// populated target would race the single-writer invariant. Callers that
/// legitimately import guarantee an empty target: `admin/dbs.rs::import_db`
/// rejects non-empty up front; clone-db and backup restore create fresh
/// databases. An empty database with a pushed schema but zero rows passes.
pub async fn ensure_target_empty(pool: &PgPool, db: &str) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema_name = pg_schema(db);
    // Whatever schema has been pushed (if any), probe each of its tables for
    // at least one row. Schema-absent databases are trivially empty.
    if let Some(schema) = crate::db::load_schema(pool, db).await? {
        for table_name in schema.tables.keys() {
            let sql = format!(
                "SELECT EXISTS(SELECT 1 FROM \"{schema_name}\".\"{}\" LIMIT 1)",
                pg_table(table_name)
            );
            let (has_rows,): (bool,) = sqlx::query_as(&sql).fetch_one(pool).await?;
            if has_rows {
                return Err(RtDbError::conflict(
                    "import-db requires an empty database; create a fresh database or use clone-db",
                ));
            }
        }
    }
    Ok(())
}

/// Loads a snapshot produced by `export_database` into `db`: the first non-blank
/// line must be a `schema` line, applied via `ddl::push_schema` (creates `db`'s
/// tables/indexes when empty, or additively updates them like any other schema
/// push); every following `doc` line is inserted with its original id, `doc`,
/// `createdAt`, and `version` preserved exactly via `txn::insert_snapshot_row`.
/// Blank lines are skipped. Malformed JSON or a doc line before the schema line is
/// a `BadRequest`; a doc naming a table absent from the schema is a `NotFound`.
/// Returns the applied schema so the caller can refresh its schema cache.
pub async fn import_database(pool: &PgPool, db: &str, jsonl: &str) -> Result<SchemaDef, RtDbError> {
    validate_db_name(db)?;
    let mut lines = jsonl.lines().filter(|line| !line.trim().is_empty());

    let first = lines
        .next()
        .ok_or_else(|| RtDbError::bad_request("snapshot is empty"))?;
    let schema = match serde_json::from_str::<SnapshotLine>(first) {
        Ok(SnapshotLine::Schema { schema }) => schema,
        Ok(SnapshotLine::Doc { .. }) => {
            return Err(RtDbError::bad_request(
                "snapshot must start with a schema line",
            ));
        }
        Err(err) => {
            return Err(RtDbError::bad_request(format!(
                "invalid snapshot schema line: {err}"
            )));
        }
    };

    // Import replays into a freshly-created database (no subscribers, no
    // concurrent writers), so the backfill-affected set is always empty here.
    // ARC-002 contract: the caller guarantees an EMPTY target — import_db
    // rejects a database that already holds documents; clone-db and backup
    // restore create fresh databases. Import itself does not enforce this.
    let (applied, _) = push_schema(pool, db, schema).await?;
    let pg_schema_name = pg_schema(db);
    let mut tx = pool.begin().await?;

    // Restored rows, for the change-feed stamp below: (table, id, post-image,
    // created_at) in replay order.
    let mut stamped: Vec<(
        String,
        String,
        serde_json::Map<String, serde_json::Value>,
        i64,
    )> = Vec::new();
    for line in lines {
        let parsed: SnapshotLine = serde_json::from_str(line)
            .map_err(|err| RtDbError::bad_request(format!("invalid snapshot doc line: {err}")))?;
        let (table, id, doc, created_at, version) = match parsed {
            SnapshotLine::Doc {
                table,
                id,
                doc,
                created_at,
                version,
            } => (table, id, doc, created_at, version),
            SnapshotLine::Schema { .. } => {
                return Err(RtDbError::bad_request("schema line must be the first line"));
            }
        };
        let table_def = applied.table(&table)?;
        insert_snapshot_row(
            &mut tx,
            &pg_schema_name,
            table_def,
            &table,
            &id,
            &doc,
            created_at,
            version,
        )
        .await?;
        stamped.push((table, id, doc, created_at));
    }

    // Imported docs replay their counter values verbatim; reposition each
    // declared table's sequence past the imported max so post-restore inserts
    // continue the numbering instead of restarting at 1 (and colliding with
    // restored rows under a unique index). Forward-only — see
    // `reposition_sequence`.
    for (table_name, table_def) in &applied.tables {
        if let Some(field) = &table_def.auto_increment_field {
            reposition_sequence(&mut tx, &pg_schema_name, table_name, field).await?;
        }
    }

    // Change-feed stamp: imported documents are observable like any write. Under
    // the ARC-002 empty-target contract the target is fresh, so these stamps are
    // bookkeeping — one `Insert` op with the post-image per restored row, on
    // this same transaction (the change_head row lock serializes the import
    // against the committer's counter UPDATE, so seq order still equals commit
    // order). Clone-db and backup restore ride this same path into fresh
    // databases, where the stamps are pure bookkeeping.
    if !stamped.is_empty() {
        let mut write_set = crate::txn::WriteSet::default();
        for (table, id, doc, created_at) in &stamped {
            write_set.touch(table, id, crate::txn::OpKind::Insert);
            write_set.capture_doc(table, id, Some(None), Some(Some(doc)), Some(*created_at));
        }
        crate::change_log::append(&mut tx, &pg_schema_name, &write_set).await?;
    }

    tx.commit().await?;
    Ok(applied)
}
