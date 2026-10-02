//! Durable audit log — the persistent counterpart to the in-memory `OpFeed`.
//!
//! When `Config::audit_log_enabled` is set at boot, `execute_txn` writes one
//! `rtdb.audit_log` row per `DocOp` INSIDE the write's transaction (ENH-047 —
//! the outbox guarantee: audit rows commit or roll back with the documents) on
//! the mutate/scheduled/workflow paths. The other durable-write arms
//! (ttl/migrate/merge) keep the best-effort post-commit tap via
//! `write_audit_rows`.

use sqlx::PgPool;

use crate::db::now_ms;
use crate::error::RtDbError;
use crate::txn::{DocOp, OpKind};

/// Lowercase wire name of an `OpKind`, matching `OpKind`'s `rename_all =
/// "lowercase"` serde attribute so the audit row's `op` column agrees with the
/// op-feed's serialized `kind`.
fn op_kind_name(kind: OpKind) -> &'static str {
    match kind {
        OpKind::Insert => "insert",
        OpKind::Patch => "patch",
        OpKind::Replace => "replace",
        OpKind::Delete => "delete",
        OpKind::Upsert => "upsert",
    }
}

/// Inserts one audit row per `DocOp` on the given connection — the
/// transactional form. Inside `execute_txn` the insert joins the write's
/// Postgres transaction (the outbox guarantee: audit rows commit or roll back
/// together with the documents). All values are `$n`-bound; the table/column
/// identifiers are fixed literals. `ts_ms` is read once for the call so a
/// multi-op batch shares a single timestamp (matches `OpFeed::publish`).
/// Errors propagate: on the txn path the whole write rolls back.
pub async fn write_audit_rows_on(
    conn: &mut sqlx::PgConnection,
    db: &str,
    owner: Option<&str>,
    source: &str,
    ops: &[DocOp],
) -> Result<(), RtDbError> {
    if ops.is_empty() {
        return Ok(());
    }
    let ts_ms = now_ms();
    let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
        "INSERT INTO rtdb.audit_log (ts_ms, db, tbl, op, doc_id, principal, source) ",
    );
    builder.push_values(ops, |mut b, op| {
        b.push_bind(ts_ms)
            .push_bind(db)
            .push_bind(&op.table)
            .push_bind(op_kind_name(op.kind))
            .push_bind(&op.id)
            .push_bind(owner)
            .push_bind(source);
    });
    builder.build().execute(conn).await?;
    Ok(())
}

/// Pool form of [`write_audit_rows_on`] for non-committer callers: acquires a
/// connection and delegates.
pub async fn write_audit_rows(
    pool: &PgPool,
    db: &str,
    owner: Option<&str>,
    source: &str,
    ops: &[DocOp],
) -> Result<(), RtDbError> {
    let mut conn = pool.acquire().await?;
    write_audit_rows_on(&mut conn, db, owner, source, ops).await
}

/// One row of the audit log, in the shape served by `GET /admin/audit`.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditEntry {
    pub id: i64,
    pub ts_ms: i64,
    pub db: String,
    #[serde(rename = "table")]
    pub tbl: String,
    pub op: Option<String>,
    pub doc_id: String,
    pub principal: Option<String>,
    pub source: String,
}

/// The optional equality filters accepted by `fetch_audit_rows`, bundled so
/// the function stays under clippy's argument-count threshold. Each field
/// filters when `Some` (combined with AND); an absent filter matches all rows.
#[derive(Debug, Default, Clone, Copy)]
pub struct AuditRowFilter<'a> {
    pub db: Option<&'a str>,
    pub table: Option<&'a str>,
    pub op: Option<&'a str>,
    pub principal: Option<&'a str>,
    pub source: Option<&'a str>,
}

/// Reads audit rows newest-first (by `ts_ms DESC`, then `id DESC` as a stable
/// tie-breaker). `filter`'s fields each restrict the result when `Some`,
/// combined with AND; an absent filter matches all rows. `limit`/`offset`
/// page. Called by the admin endpoint; returns an empty `Vec` when the table
/// does not exist (audit disabled at boot) — the caller need not distinguish.
pub async fn fetch_audit_rows(
    pool: &PgPool,
    filter: AuditRowFilter<'_>,
    limit: i64,
    offset: i64,
) -> Result<Vec<AuditEntry>, RtDbError> {
    let AuditRowFilter {
        db,
        table,
        op,
        principal,
        source,
    } = filter;
    // Factored into a type alias so the `query_as` annotation stays readable
    // (clippy::type_complexity) and the SELECT column order is self-documenting
    // at the call site.
    type AuditRow = (
        i64,            // id
        i64,            // ts_ms
        String,         // db
        String,         // tbl
        Option<String>, // op
        String,         // doc_id
        Option<String>, // principal
        String,         // source
    );
    let rows: Vec<AuditRow> = sqlx::query_as(
        "SELECT id, ts_ms, db, tbl, op, doc_id, principal, source \
             FROM rtdb.audit_log \
             WHERE ($1::text IS NULL OR db = $1) \
               AND ($2::text IS NULL OR tbl = $2) \
               AND ($3::text IS NULL OR op = $3) \
               AND ($4::text IS NULL OR principal = $4) \
               AND ($5::text IS NULL OR source = $5) \
             ORDER BY ts_ms DESC, id DESC \
             LIMIT $6 OFFSET $7",
    )
    .bind(db)
    .bind(table)
    .bind(op)
    .bind(principal)
    .bind(source)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(id, ts_ms, db, tbl, op, doc_id, principal, source)| AuditEntry {
                id,
                ts_ms,
                db,
                tbl,
                op,
                doc_id,
                principal,
                source,
            },
        )
        .collect())
}
