//! Database snapshot and restore over a JSONL wire format. A snapshot is a
//! leading `schema` line carrying the pushed `SchemaDef`, followed by one `doc`
//! line per stored document (raw `doc` jsonb plus its `id`/`createdAt`/`version`
//! columns). Restore replays a snapshot into a fresh database through the normal
//! `push_schema` + `insert_snapshot_row` path. Distinct from the `pg_dump`-based
//! backup/restore in `backup` / `admin/backups`.

use sqlx::PgPool;

use crate::db::validate_db_name;
use crate::ddl::{pg_schema, pg_table, push_schema};
use crate::error::RtDbError;
use crate::schema::SchemaDef;
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
        RtDbError::internal(format!("failed to serialize snapshot schema line: {err}"))
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
                RtDbError::internal(format!("failed to serialize snapshot doc line: {err}"))
            })?);
            out.push('\n');
        }
    }

    Ok(out)
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

    let applied = push_schema(pool, db, schema).await?;
    let pg_schema_name = pg_schema(db);
    let mut tx = pool.begin().await?;

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
    }

    tx.commit().await?;
    Ok(applied)
}
