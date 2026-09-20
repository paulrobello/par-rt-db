//! Per-database durable change feed — the seq-cursor primitive behind
//! `GET /api/db/{db}/changes`. The committer stamps one `changes` row per
//! written `(table, id)` inside the SAME Postgres transaction as the document
//! writes (see the design spec), so a committed write is always observable and
//! a rolled-back write never leaves a row behind.
//!
//! Sequence assignment is a transactional counter row, not `nextval()`: a
//! SEQUENCE burns numbers on rollback, which would put gaps in the retained
//! seqs and make the expired-cursor test inexact. Contention is impossible by
//! construction — only the serialized committer turn (or a whole-db admin
//! import, which appends on its own transaction) advances the counter.
//!
//! Retention keeps the newest [`HotConfig::change_log_max_rows`] rows; the
//! head row and the newest row are never trimmed, so an empty `changes` table
//! means "nothing was ever committed" and the expired-cursor test stays
//! exact. Trimming rides `mutation_log::run_cleanup`'s per-db loop, off the
//! write path.

use serde_json::Value;
use sqlx::{PgConnection, PgPool};

use crate::db::{new_id, validate_db_name};
use crate::ddl::pg_schema;
use crate::error::RtDbError;
use crate::txn::{OpKind, WriteSet};

/// Default retention floor: rows kept per db when
/// `RTDB_CHANGE_LOG_MAX_ROWS` is unset.
pub const DEFAULT_MAX_ROWS: usize = 100_000;

/// A configured max below this is clamped up at PATCH/env time — a few dozen
/// rows of retention would expire every cursor almost immediately, which is
/// never what an operator wants.
pub const MIN_MAX_ROWS: usize = 1_000;

/// The `changes` wire kind for an [`OpKind`] — the same lowercase wire form
/// `OpKind`'s serde attributes produce, spelled out so the stored TEXT column
/// is stable even if the enum's serde shape ever moves.
pub(crate) fn kind_str(kind: OpKind) -> &'static str {
    match kind {
        OpKind::Insert => "insert",
        OpKind::Patch => "patch",
        OpKind::Replace => "replace",
        OpKind::Delete => "delete",
        OpKind::Upsert => "upsert",
    }
}

/// Ensures the per-db change-log tables exist and the singleton head row is
/// seeded. Idempotent — safe to call from `create_database`, every committer
/// startup, and the read handler (old dbs, or a db whose committer never ran
/// on this replica). `log_id` is minted once at seed time and echoed on every
/// read so a consumer can detect the one hole seqs cannot: a db dropped and
/// recreated under the same name restarts at seq 0.
pub async fn ensure_table(pool: &PgPool, db: &str) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS \"{schema}\".change_head (
            ok     boolean PRIMARY KEY DEFAULT true,
            seq    bigint NOT NULL DEFAULT 0,
            log_id text   NOT NULL DEFAULT ''
        )"
    ))
    .execute(pool)
    .await?;
    // Seed the singleton row (idempotent) so the counter UPDATE always
    // matches. `log_id` mints only on the inserting call — the ON CONFLICT
    // keeps the original mint for the life of the database.
    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".change_head (ok, seq, log_id)
         VALUES (true, 0, $1) ON CONFLICT (ok) DO NOTHING"
    ))
    .bind(&new_id()[..16])
    .execute(pool)
    .await?;
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS \"{schema}\".changes (
            seq        bigint PRIMARY KEY,
            table_name text NOT NULL,
            doc_id     text NOT NULL,
            kind       text NOT NULL,
            doc        jsonb,
            ts         bigint NOT NULL
        )"
    ))
    .execute(pool)
    .await?;
    // The table=-filtered poll walks (table_name, seq) — without this index
    // every filtered read scans up to the whole retained window.
    sqlx::query(&format!(
        // CREATE INDEX takes no schema qualifier — the index lands in the
        // table's schema automatically.
        "CREATE INDEX IF NOT EXISTS changes_table_seq
         ON \"{schema}\".changes (table_name, seq)"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

/// The current end of the log plus its identity, read even when `changes` is
/// empty (the head row is seeded, never trimmed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Head {
    pub seq: i64,
    pub log_id: String,
}

pub async fn head(pool: &PgPool, db: &str) -> Result<Head, RtDbError> {
    let schema = pg_schema(db);
    let row: (i64, String) = sqlx::query_as(&format!(
        "SELECT seq, log_id FROM \"{schema}\".change_head WHERE ok"
    ))
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| RtDbError::internal("change head row missing — ensure_table was not called"))?;
    Ok(Head {
        seq: row.0,
        log_id: row.1,
    })
}

/// The oldest retained seq, or `None` on a log with no rows yet. The
/// expired-cursor test compares the caller's cursor against this BEFORE the
/// `table` filter is applied — a filtered page can be short while trimmed
/// unfiltered rows still sit between the cursor and the window.
pub async fn min_seq(pool: &PgPool, db: &str) -> Result<Option<i64>, RtDbError> {
    let schema = pg_schema(db);
    let row: Option<(Option<i64>,)> =
        sqlx::query_as(&format!("SELECT min(seq) FROM \"{schema}\".changes"))
            .fetch_optional(pool)
            .await?;
    Ok(row.and_then(|(min,)| min))
}

/// Appends the transaction's net write set to the change log, ON the open
/// transaction — the caller commits the documents and their change rows
/// together or not at all.
///
/// One row per written `(table, id)` (the same net collapse `doc_values`
/// performs): `kind` is the LAST touch's op, `doc` is the id's end-of-txn
/// state — `NULL` when the txn leaves the id with no visible end state
/// (deleted, txn-local insert+delete, or a payload-less backfill op). The
/// consumer resolves `doc = NULL` by re-fetching the id, which makes soft
/// deletes and hard deletes read identically — both are invisible to it.
pub(crate) async fn append(
    tx: &mut PgConnection,
    pg_schema_name: &str,
    write_set: &WriteSet,
) -> Result<(), RtDbError> {
    if write_set.docs.is_empty() {
        return Ok(());
    }
    let count = write_set.docs.len() as i64;
    let base: (i64,) = sqlx::query_as(&format!(
        "UPDATE \"{pg_schema_name}\".change_head SET seq = seq + $1 WHERE ok RETURNING seq"
    ))
    .bind(count)
    .fetch_one(&mut *tx)
    .await?;

    // Last touch wins for the kind — walk the ops backwards and keep the
    // first kind seen per (table, id).
    let mut last_kind: std::collections::HashMap<(&str, &str), OpKind> =
        std::collections::HashMap::with_capacity(write_set.ops.len());
    for op in write_set.ops.iter().rev() {
        last_kind
            .entry((op.table.as_str(), op.id.as_str()))
            .or_insert(op.kind);
    }

    let ts = crate::db::now_ms();
    let mut seqs = Vec::with_capacity(count as usize);
    let mut tables = Vec::with_capacity(count as usize);
    let mut ids = Vec::with_capacity(count as usize);
    let mut kind_strs = Vec::with_capacity(count as usize);
    let mut docs = Vec::with_capacity(count as usize);
    for (i, (table, id)) in write_set.docs.iter().enumerate() {
        let kind = last_kind
            .get(&(table.as_str(), id.as_str()))
            .copied()
            .ok_or_else(|| RtDbError::internal("write_set op missing for touched document"))?;
        // base is the head AFTER the increment; this txn's rows own
        // seqs base-count+1 ..= base.
        seqs.push(base.0 - count + i as i64 + 1);
        tables.push(table.as_str());
        ids.push(id.as_str());
        kind_strs.push(kind_str(kind));
        docs.push(
            write_set
                .doc_values
                .get(&(table.clone(), id.clone()))
                .and_then(|dv| dv.after.clone())
                .map(Value::Object),
        );
    }

    // One statement for the whole write set: the rows travel as one jsonb
    // array and unnest back into columns. A 10_000-op txn (the
    // MAX_AFFECTED_ROWS_PER_TXN ceiling) stays a single round trip.
    let rows = serde_json::Value::Array(
        (0..count as usize)
            .map(|i| {
                serde_json::json!({
                    "seq": seqs[i], "table": tables[i], "id": ids[i],
                    "kind": kind_strs[i], "doc": docs[i], "ts": ts,
                })
            })
            .collect(),
    );
    sqlx::query(&format!(
        // The CASE matters: a missing doc key or a JSON null must land as SQL
        // NULL (no post-image), never as jsonb `null` — the consumer contract
        // is `doc IS NULL`.
        "INSERT INTO \"{pg_schema_name}\".changes (seq, table_name, doc_id, kind, doc, ts)
         SELECT (r->>'seq')::bigint, r->>'table', r->>'id', r->>'kind',
                (CASE WHEN r ? 'doc' AND jsonb_typeof(r->'doc') <> 'null'
                      THEN r->'doc' END),
                (r->>'ts')::bigint
         FROM jsonb_array_elements($1::jsonb) AS r"
    ))
    .bind(rows)
    .execute(&mut *tx)
    .await?;
    Ok(())
}

/// One committed change as the read path returns it. `doc` mirrors the stored
/// post-image (`None` when the row carries no end-state — the consumer
/// re-fetches).
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangeRow {
    pub seq: i64,
    pub table: String,
    pub doc_id: String,
    pub kind: String,
    pub doc: Option<Value>,
    pub ts: i64,
}

/// A page of ops strictly after `since` (oldest first), plus the two cursor
/// inputs the handler needs.
#[derive(Debug, Clone, PartialEq)]
pub struct Page {
    pub ops: Vec<ChangeRow>,
    pub min_seq: Option<i64>,
}

pub async fn read_page(
    pool: &PgPool,
    db: &str,
    since: i64,
    table: Option<&str>,
    limit: i64,
) -> Result<Page, RtDbError> {
    let schema = pg_schema(db);
    let min = min_seq(pool, db).await?;
    let query = match table {
        Some(_) => {
            format!(
                "SELECT seq, table_name, doc_id, kind, doc, ts
                 FROM \"{schema}\".changes
                 WHERE seq > $1 AND table_name = $2
                 ORDER BY seq LIMIT $3"
            )
        }
        None => format!(
            "SELECT seq, table_name, doc_id, kind, doc, ts
             FROM \"{schema}\".changes
             WHERE seq > $1
             ORDER BY seq LIMIT $2"
        ),
    };
    let mut q =
        sqlx::query_as::<_, (i64, String, String, String, Option<Value>, i64)>(&query).bind(since);
    if let Some(table) = table {
        q = q.bind(table);
    }
    q = q.bind(limit);
    let rows = q.fetch_all(pool).await?;
    Ok(Page {
        ops: rows
            .into_iter()
            .map(|(seq, table, doc_id, kind, doc, ts)| ChangeRow {
                seq,
                table,
                doc_id,
                kind,
                doc,
                ts,
            })
            .collect(),
        min_seq: min,
    })
}

/// Deletes rows beyond the retention window (`head - max_rows` and older).
/// Called only from the per-db cleanup loop — never on the write path. The
/// head row and the newest row are never deleted, so an empty table keeps its
/// "nothing was ever committed" meaning. Returns the rows deleted so the
/// caller's sweep backoff can react.
pub async fn trim_expired(pool: &PgPool, db: &str, max_rows: i64) -> Result<u64, RtDbError> {
    let head = head(pool, db).await?.seq;
    if head <= max_rows {
        return Ok(0);
    }
    let schema = pg_schema(db);
    let res = sqlx::query(&format!("DELETE FROM \"{schema}\".changes WHERE seq <= $1"))
        .bind(head - max_rows)
        .execute(pool)
        .await?;
    Ok(res.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_strings_match_the_opkind_wire_form() {
        // The TEXT column must equal what OpKind's serde emits (lowercase) so
        // clients can map the stored kind back onto the wire enum.
        let check = |kind: OpKind, wire: &str| {
            let json = serde_json::to_string(&kind).unwrap();
            assert_eq!(json, format!("\"{wire}\""), "OpKind serde drifted");
            assert_eq!(kind_str(kind), wire);
        };
        check(OpKind::Insert, "insert");
        check(OpKind::Patch, "patch");
        check(OpKind::Replace, "replace");
        check(OpKind::Delete, "delete");
        check(OpKind::Upsert, "upsert");
    }

    #[test]
    fn retention_bounds_are_sane() {
        const { assert!(MIN_MAX_ROWS >= 1) }
        const { assert!(DEFAULT_MAX_ROWS >= MIN_MAX_ROWS) }
    }
}
