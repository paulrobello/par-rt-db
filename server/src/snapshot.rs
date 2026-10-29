//! Database snapshot and restore over a JSONL wire format. A snapshot is a
//! leading `schema` line carrying the pushed `SchemaDef`, followed by one `doc`
//! line per stored document (raw `doc` jsonb plus its `id`/`createdAt`/`version`
//! columns). Restore replays a snapshot into a fresh database through the normal
//! `push_schema` + `insert_snapshot_row` path. Distinct from the `pg_dump`-based
//! backup/restore in `backup` / `admin/backups`.

use std::fmt::Display;

use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use sqlx::{AssertSqlSafe, PgPool};

use crate::db::validate_db_name;
use crate::ddl::{pg_schema, pg_table, push_schema, reposition_sequence};
use crate::error::RtDbError;
use crate::schema::{SchemaDef, SchemaDefExt};
use crate::txn::insert_snapshot_row;

/// Hard per-line memory bound for streamed imports ([`JsonlLineReader`]). A
/// line carries one document plus its envelope; anything larger is a corrupt
/// or hostile stream, rejected as `BAD_REQUEST` before it can balloon the
/// buffer. Generous above any legitimate document size, small enough that an
/// import's peak memory is bounded regardless of snapshot size.
pub const MAX_SNAPSHOT_LINE_BYTES: usize = 16 * 1024 * 1024;

/// Change-feed stamp batching bounds for the shared import core: the
/// accumulated `WriteSet` is appended to the feed once EITHER bound trips.
/// `change_log::append` assigns seqs op-by-op in append order and every append
/// runs on the import's own transaction, so splitting the stamps across appends
/// leaves the observable feed (seq values, per-op order) identical to the old
/// single end-of-import append.
const STAMP_FLUSH_ROWS: usize = 1_000;
const STAMP_FLUSH_BYTES: usize = 8 * 1024 * 1024;

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

/// Serializes one snapshot wire line (plus its trailing `'\n'`) as JSONL text.
/// The single encoder shared by the buffered [`export_database`] and the
/// streaming [`export_stream`], so both paths emit byte-identical JSONL.
fn encode_snapshot_line(
    db: &str,
    table: Option<&str>,
    line: &SnapshotLine,
) -> Result<String, RtDbError> {
    let mut out = serde_json::to_string(line).map_err(|err| {
        tracing::error!(error = %err, db, table = ?table, "failed to serialize snapshot line");
        RtDbError::internal("failed to serialize snapshot line")
    })?;
    out.push('\n');
    Ok(out)
}

/// Renders `db`'s current schema and every row of every table as JSONL: a `schema`
/// line first, then `doc` lines in schema-table order (`SchemaDef::tables` is a
/// `BTreeMap`, so this is deterministic), rows within a table ordered by
/// `(created_at, id)` to match `query.rs`'s default sort.
///
/// Buffered variant that materializes the whole JSONL in memory. Kept for
/// in-process callers (`clone-db`, backup restore) whose call sites want a
/// `String`; the HTTP export endpoint streams via [`export_stream`]. Both share
/// [`encode_snapshot_line`], so they are byte-identical by construction.
pub async fn export_database(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
) -> Result<String, RtDbError> {
    validate_db_name(db)?;
    let pg_schema_name = pg_schema(db);
    let mut out = String::new();

    let head = encode_snapshot_line(
        db,
        None,
        &SnapshotLine::Schema {
            schema: schema.clone(),
        },
    )?;
    out.push_str(&head);

    for table_name in schema.tables.keys() {
        let table_ident = pg_table(table_name);
        let rows: Vec<(String, serde_json::Value, i64, i64)> = sqlx::query_as(AssertSqlSafe(format!(
            "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\" ORDER BY \"created_at\", \"id\""
        )))
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
            out.push_str(&encode_snapshot_line(db, Some(table_name), &line)?);
        }
    }

    Ok(out)
}

/// Rows per keyset-paginated `fetch_all` batch on the export cursor. Bounded
/// so a multi-GB table streams in batches instead of materializing per table.
const EXPORT_BATCH_ROWS: usize = 500;

/// Bounded-memory export: the schema line, then one `doc` line per row,
/// fetched per table in keyset-paginated batches (`WHERE (created_at, id) >`
/// the last emitted row, `LIMIT {EXPORT_BATCH_ROWS}`). Byte-format identical
/// to the buffered [`export_database`] via the shared [`encode_snapshot_line`];
/// the HTTP export endpoint serves this stream directly.
///
/// Implemented as an `async-stream` generator: sqlx `fetch` streams cannot
/// carry an owned per-table SQL string at `'static`, while the generator
/// awaits each batched `fetch_all` inside its own body — per-batch SQL borrows
/// stay local and the returned stream stays `'static`, with memory bounded by
/// one batch plus one encoded line.
pub fn export_stream(
    pool: PgPool,
    db: String,
    schema: SchemaDef,
) -> Result<impl Stream<Item = Result<Bytes, RtDbError>> + Send + 'static, RtDbError> {
    validate_db_name(&db)?;
    let head = encode_snapshot_line(
        &db,
        None,
        &SnapshotLine::Schema {
            schema: schema.clone(),
        },
    )?;
    let pg_schema_name = pg_schema(&db);
    let tables: Vec<String> = schema.tables.keys().cloned().collect();
    Ok(async_stream::try_stream! {
        yield Bytes::from(head);
        for table in tables {
            let table_ident = pg_table(&table);
            let mut cursor: Option<(i64, String)> = None;
            loop {
                let sql = match &cursor {
                    None => format!(
                        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\" ORDER BY \"created_at\", \"id\" LIMIT {EXPORT_BATCH_ROWS}"
                    ),
                    Some((_created_at, _id)) => format!(
                        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE (\"created_at\", \"id\") > ($1, $2) ORDER BY \"created_at\", \"id\" LIMIT {EXPORT_BATCH_ROWS}"
                    ),
                };
                let mut query =
                    sqlx::query_as::<_, (String, serde_json::Value, i64, i64)>(AssertSqlSafe(
                        sql,
                    ));
                if let Some((created_at, id)) = &cursor {
                    query = query.bind(created_at).bind(id);
                }
                let rows: Vec<(String, serde_json::Value, i64, i64)> =
                    query.fetch_all(&pool).await.map_err(RtDbError::from)?;
                if rows.is_empty() {
                    break;
                }
                let last = &rows[rows.len() - 1];
                let next_cursor = (last.2, last.0.clone());
                for (id, doc_value, created_at, version) in rows {
                    let doc = match doc_value {
                        serde_json::Value::Object(map) => map,
                        _ => Err(RtDbError::internal("stored doc is not a JSON object"))?,
                    };
                    let line = SnapshotLine::Doc {
                        table: table.clone(),
                        id,
                        doc,
                        created_at,
                        version,
                    };
                    yield Bytes::from(encode_snapshot_line(&db, Some(&table), &line)?);
                }
                cursor = Some(next_cursor);
            }
        }
    })
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
            let (has_rows,): (bool,) = sqlx::query_as(AssertSqlSafe(sql)).fetch_one(pool).await?;
            if has_rows {
                return Err(RtDbError::conflict(
                    "import-db requires an empty database; create a fresh database or use clone-db",
                ));
            }
        }
    }
    Ok(())
}

/// Bounded JSONL line reader over any byte-chunk stream. Buffers at most one
/// line (plus the arriving chunk) at a time; a line longer than
/// [`MAX_SNAPSHOT_LINE_BYTES`] is a `BAD_REQUEST`. Line semantics match the
/// buffered path's `str::lines`: a `'\n'`-terminated line is yielded without
/// the terminator (a single trailing `'\r'` is stripped), blank lines are
/// passed through (the import core filters them), and a final unterminated
/// segment still counts as a line.
pub struct JsonlLineReader<S, B, E> {
    stream: S,
    buf: BytesMut,
    source_done: bool,
    _types: std::marker::PhantomData<fn() -> (B, E)>,
}

/// Wraps a byte-chunk stream (`request.into_data_stream()`, a `Vec<u8>` chunk
/// iterator, ...) in a [`JsonlLineReader`].
pub fn jsonl_lines<S, B, E>(stream: S) -> JsonlLineReader<S, B, E>
where
    S: Stream<Item = Result<B, E>> + Unpin,
    B: AsRef<[u8]>,
    E: Display,
{
    JsonlLineReader {
        stream,
        buf: BytesMut::new(),
        source_done: false,
        _types: std::marker::PhantomData,
    }
}

impl<S, B, E> JsonlLineReader<S, B, E>
where
    S: Stream<Item = Result<B, E>> + Unpin,
    B: AsRef<[u8]>,
    E: Display,
{
    /// Next JSONL line, or `None` at end of stream. Transport errors
    /// surface here as `RtDbError`s so the import aborts and rolls back.
    pub async fn next_line(&mut self) -> Result<Option<Vec<u8>>, RtDbError> {
        loop {
            if let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
                let mut line = self.buf.split_to(pos + 1);
                line.truncate(line.len() - 1);
                if line.last() == Some(&b'\r') {
                    line.truncate(line.len() - 1);
                }
                if line.len() > MAX_SNAPSHOT_LINE_BYTES {
                    return Err(RtDbError::bad_request(
                        "snapshot line exceeds the maximum line length",
                    ));
                }
                return Ok(Some(line.to_vec()));
            }
            if self.buf.len() > MAX_SNAPSHOT_LINE_BYTES {
                return Err(RtDbError::bad_request(
                    "snapshot line exceeds the maximum line length",
                ));
            }
            if self.source_done {
                // A final unterminated segment counts, like `str::lines`.
                if self.buf.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(self.buf.split().to_vec()));
            }
            match self.stream.next().await {
                Some(Ok(chunk)) => {
                    self.buf.extend_from_slice(chunk.as_ref());
                }
                Some(Err(err)) => {
                    return Err(RtDbError::bad_request(format!(
                        "snapshot body read failed: {err}"
                    )));
                }
                None => self.source_done = true,
            }
        }
    }
}

/// Shared import core for both wire paths: consumes JSONL lines from `reader`,
/// pushing the schema line via `ddl::push_schema`, replaying each `doc` line
/// with `txn::insert_snapshot_row` (id/createdAt/version preserved), and
/// stamping the change feed in bounded batches — flush once
/// [`STAMP_FLUSH_ROWS`] rows or [`STAMP_FLUSH_BYTES`] bytes of raw line input
/// have accumulated, whichever comes first. All doc writes and stamps run on
/// ONE transaction: any error — malformed JSON, unknown table, oversized line,
/// transport failure — aborts and rolls back every inserted row. The schema
/// push (DDL) commits before the transaction opens, matching the buffered
/// path's long-standing behavior.
pub async fn import_lines<S, B, E>(
    pool: &PgPool,
    db: &str,
    reader: &mut JsonlLineReader<S, B, E>,
) -> Result<SchemaDef, RtDbError>
where
    S: Stream<Item = Result<B, E>> + Unpin,
    B: AsRef<[u8]>,
    E: Display,
{
    validate_db_name(db)?;

    // Skip leading blank lines before the schema line — the buffered path's
    // `lines().filter(!blank)` semantics.
    let first = loop {
        match reader.next_line().await? {
            None => return Err(RtDbError::bad_request("snapshot is empty")),
            Some(line) if !is_blank(&line) => break line,
            Some(_) => continue,
        }
    };
    let schema = match serde_json::from_slice::<SnapshotLine>(&first) {
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

    let mut stamps = crate::txn::WriteSet::default();
    let mut stamp_rows: usize = 0;
    let mut stamp_bytes: usize = 0;
    let mut saw_any_doc = false;
    loop {
        let line = match reader.next_line().await? {
            Some(line) => line,
            None => break,
        };
        if is_blank(&line) {
            continue;
        }
        let parsed: SnapshotLine = serde_json::from_slice(&line)
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
        stamps.touch(&table, &id, crate::txn::OpKind::Insert);
        stamps.capture_doc(&table, &id, Some(None), Some(Some(&doc)), Some(created_at));
        stamp_rows += 1;
        stamp_bytes += line.len();
        saw_any_doc = true;
        if stamp_rows >= STAMP_FLUSH_ROWS || stamp_bytes >= STAMP_FLUSH_BYTES {
            crate::change_log::append(&mut tx, &pg_schema_name, &stamps).await?;
            stamps = crate::txn::WriteSet::default();
            stamp_rows = 0;
            stamp_bytes = 0;
        }
    }
    if saw_any_doc && stamp_rows > 0 {
        crate::change_log::append(&mut tx, &pg_schema_name, &stamps).await?;
    }

    // Imported rows replay counter values verbatim; reposition each declared
    // table's sequence past the imported max (forward-only — see
    // `reposition_sequence`) so post-restore inserts continue the numbering
    // instead of restarting at 1.
    for (table_name, table_def) in &applied.tables {
        if let Some(field) = &table_def.auto_increment_field {
            reposition_sequence(&mut tx, &pg_schema_name, table_name, field).await?;
        }
    }

    tx.commit().await?;
    Ok(applied)
}

/// `true` when a line is empty or all-ASCII-whitespace — the buffered path's
/// `line.trim().is_empty()` skip rule, applied in the shared core so both wire
/// paths filter identically.
fn is_blank(line: &[u8]) -> bool {
    line.iter().all(|b| b.is_ascii_whitespace())
}

/// Loads a snapshot produced by `export_database` into `db` through the shared
/// line-reader core ([`import_lines`]). The first non-blank line must be a
/// `schema` line, applied via `ddl::push_schema`; every following `doc` line is
/// inserted with its original id, `doc`, `createdAt`, and `version` preserved
/// exactly via `txn::insert_snapshot_row`. Blank lines are skipped. Malformed
/// JSON or a doc line before the schema line is a `BadRequest`; a doc naming a
/// table absent from the schema is a `NotFound`. Returns the applied schema so
/// the caller can refresh its schema cache.
pub async fn import_database(pool: &PgPool, db: &str, jsonl: &str) -> Result<SchemaDef, RtDbError> {
    validate_db_name(db)?;
    // Feed bounded byte chunks (NOT `str::lines()` items — the reader splits on
    // `'\n'` bytes, and terminator-less items would concatenate into one line).
    let mut reader = jsonl_lines(futures::stream::iter(
        jsonl
            .as_bytes()
            .chunks(64 * 1024)
            .map(Ok::<_, std::convert::Infallible>),
    ));
    import_lines(pool, db, &mut reader).await
}
