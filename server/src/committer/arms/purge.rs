//! The `RunPurge` arm: admin-initiated reclamation of insertOnly rows. Both
//! the preview and the confirmed delete execute inside the committer's
//! serialized turn, so the confirm handshake is verified against the same
//! single-writer snapshot that performs the delete.

use crate::committer::*;
use std::collections::HashSet;

/// Reply payload of a `RunPurge` arm, phase-shaped like the HTTP contract:
/// the preview carries the count + token; a confirmed purge carries the
/// delete outcome. Crosses replicas as JSON through the forward pipeline, so
/// it must round-trip.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "phase", rename_all = "camelCase")]
pub enum PurgeResult {
    #[serde(rename_all = "camelCase")]
    Preview {
        matching: i64,
        confirm_token: String,
        limit: u32,
    },
    #[serde(rename_all = "camelCase")]
    Confirmed { deleted: u64, truncated: bool },
}

/// Deterministic token over the purge's identity: db, table, serialized
/// filter, the table's schema shape, the effective limit, and the ordered
/// matching id set (capped at limit + 1). Recomputing it inside the
/// serialized turn is the stale-confirmation guard — any insert or delete
/// that changes which rows the predicate selects at the head of the batch
/// changes the token, so a stale or cross-request confirm refuses with 409
/// and forces a re-preview. Deterministic is fine: the admin key already
/// authorizes the action; the token is an operator-safety handshake, not a
/// security boundary.
fn confirm_token(
    db: &str,
    table: &str,
    filter: Option<&crate::query::FilterExpr>,
    table_def: &crate::schema::TableDef,
    limit: u32,
    ids: &[(String,)],
) -> String {
    let filter_json =
        serde_json::to_string(&filter).unwrap_or_else(|_| "unserializable-filter".to_string());
    let schema_json =
        serde_json::to_string(table_def).unwrap_or_else(|_| "unserializable-schema".to_string());
    let ids_material = ids
        .iter()
        .map(|(id,)| id.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let material = format!("purge|{db}|{table}|{filter_json}|{schema_json}|{limit}|{ids_material}");
    crate::storage::sha256_hex_bytes(material.as_bytes())
}

/// Runs one admin purge. Preview (`confirm = None`) opens the same guarded
/// transaction as the confirm phase but deletes nothing (read-only turn,
/// rolled back). The confirmed phase deletes the predicate's head batch
/// through [`crate::txn::delete_row_cascade`] on ONE transaction with the
/// durable change feed and the audit rows committed atomically — a committed
/// purge is observable and auditable, a rolled-back one leaves nothing
/// behind. Audit is unconditional (`source = "purge"`): a destructive admin
/// reclamation is auditable even where ordinary audit logging is disabled.
/// System-initiated like the TTL reaper (`owner = None`, bypassing per-row
/// auth) — the admin gate on the HTTP route is the authorization, and the
/// HTTP route restricts that gate to the admin key. The table gate is
/// re-checked here so a future caller cannot skip the HTTP handler's check.
///
/// Single-writer invariant: runs inside the committer task's serialized turn
/// on `ctx.pool`, mirroring `execute_txn`'s preamble (quiesce advisory lock,
/// admission epoch, transactional re-read of the freeze flags) and its
/// statement timeout. A delete captures no `doc_values`, so `fan_out`
/// table-level re-runs (sound over-approximation). Each initiating row
/// deletes through `delete_row_cascade` with `force_hard = true` so
/// softDelete rows physically leave storage and `onDelete` children are
/// handled; the per-initiator cascade budget stays `MAX_CASCADE_ROWS`, with
/// an aggregate batch budget on top so one purge call cannot cascade
/// unboundedly across initiators.
pub(in crate::committer) async fn handle_purge(
    ctx: &CommitterCtx,
    table_name: &str,
    filter: Option<&crate::query::FilterExpr>,
    limit: u32,
    confirm: Option<&str>,
    admission_epoch: i64,
) -> Result<PurgeResult, RtDbError> {
    let schema = ctx.schemas.get(&ctx.pool, &ctx.db).await?;
    let table_def = schema
        .tables
        .get(table_name)
        .ok_or_else(|| RtDbError::not_found(format!("table '{table_name}' not found")))?;
    if !table_def.insert_only {
        return Err(RtDbError::bad_request(format!(
            "purge applies only to insertOnly tables ('{table_name}' is not insertOnly)"
        )));
    }
    // Admission capture mirrors the reaper: quiesce state read off-turn, the
    // generation re-verified transactionally below.
    let (quiesced_at_admission, admission_generation) =
        crate::db::quiesce_admission(&ctx.read_pool, &ctx.db).await?;
    if quiesced_at_admission {
        return Err(RtDbError::read_only());
    }

    let pg_schema_name = crate::ddl::pg_schema(&ctx.db);
    let table_ident = crate::ddl::pg_table(table_name);
    let (where_sql, binds, _limit_ph) = crate::query::compile_scan_where(
        table_def,
        &crate::auth::PrincipalCtx::bypass(),
        None,
        filter,
    )?;
    let base = format!("FROM \"{pg_schema_name}\".\"{table_ident}\"");
    let where_clause = if where_sql.is_empty() {
        String::new()
    } else {
        format!(" WHERE {where_sql}")
    };
    let count_sql = format!("SELECT count(*) {base}{where_clause}");
    let ids_sql = format!(
        "SELECT \"id\" {base}{where_clause} \
         ORDER BY \"created_at\", \"id\" LIMIT ${}",
        binds.len() + 1
    );

    let mut tx = ctx.pool.begin().await?;
    // Quiesce fence: the advisory lock orders this turn against a concurrent
    // quiesce cutover; the epoch check rejects writes admitted before a fence
    // bump.
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(crate::db::quiesce_key(&ctx.db))
        .execute(&mut *tx)
        .await?;
    crate::db::ensure_admission_epoch(&mut tx, &ctx.db, admission_epoch).await?;
    // Statement timeout bounds every statement in this turn. The freeze flags
    // are re-read transactionally so a read-only or quiesce flip behind the
    // off-turn admission read cannot slip a purge through.
    let row: (String, bool, bool, i64) = sqlx::query_as(sqlx::AssertSqlSafe(
        "SELECT set_config('statement_timeout', $1, true), \
         COALESCE((SELECT read_only FROM rtdb_auth.databases WHERE name = $2), FALSE), \
         COALESCE((SELECT quiesced FROM rtdb_auth.databases WHERE name = $2), FALSE), \
         COALESCE((SELECT quiesce_generation FROM rtdb_auth.databases WHERE name = $2), 0)",
    ))
    .bind(crate::txn::STATEMENT_TIMEOUT_MS.to_string())
    .bind(&ctx.db)
    .fetch_one(&mut *tx)
    .await?;
    if row.1 || row.2 || row.3 != admission_generation {
        tx.rollback().await?;
        return Err(RtDbError::read_only());
    }

    let mut count_query = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(count_sql));
    let mut ids_query = sqlx::query_as::<_, (String,)>(sqlx::AssertSqlSafe(ids_sql));
    for bind in &binds {
        match bind {
            crate::dsl::EqBind::Text(v) => {
                count_query = count_query.bind(v.clone());
                ids_query = ids_query.bind(v.clone());
            }
            crate::dsl::EqBind::Num(v) => {
                count_query = count_query.bind(*v);
                ids_query = ids_query.bind(*v);
            }
            crate::dsl::EqBind::Bool(v) => {
                count_query = count_query.bind(*v);
                ids_query = ids_query.bind(*v);
            }
            crate::dsl::EqBind::I64(v) => {
                count_query = count_query.bind(*v);
                ids_query = ids_query.bind(*v);
            }
        }
    }
    // +1 so `truncated` is knowable without a second scan; the head batch
    // this token binds is exactly the batch the confirm will delete.
    ids_query = ids_query.bind(i64::from(limit) + 1);
    let matching: i64 = count_query.fetch_one(&mut *tx).await?;
    let ids: Vec<(String,)> = ids_query.fetch_all(&mut *tx).await?;
    let truncated = ids.len() as u32 > limit;
    let token = confirm_token(&ctx.db, table_name, filter, table_def, limit, &ids);

    let Some(confirm) = confirm else {
        // Preview: read-only turn — the transaction rolls back, so the
        // preview can never write.
        tx.rollback().await?;
        return Ok(PurgeResult::Preview {
            matching,
            confirm_token: token,
            limit,
        });
    };
    if confirm != token {
        tx.rollback().await?;
        return Err(RtDbError::precondition(format!(
            "confirm token does not match the current matching set ({matching} row(s) now \
             match '{table_name}'); re-run the preview to get a fresh token"
        )));
    }

    let take = std::cmp::min(ids.len(), limit as usize);
    let mut write_set = WriteSet::default();
    let mut visited: HashSet<(String, String)> = HashSet::new();
    // `visited` is shared across the batch so a row already cascaded by an
    // earlier row's delete is skipped, not an error; the per-initiator budget
    // stays `MAX_CASCADE_ROWS` (fresh counter per initiating row), and the
    // aggregate check below bounds the whole batch.
    let aggregate_budget = usize::try_from(limit)
        .unwrap_or(usize::MAX)
        .saturating_add(crate::txn::MAX_CASCADE_ROWS);
    let mut aggregate_rows: usize = 0;
    let mut deleted: u64 = 0;
    for (id,) in &ids[..take] {
        let mut cascade_rows = 0usize;
        crate::txn::delete_row_cascade(
            &mut tx,
            &pg_schema_name,
            &schema,
            table_name,
            id,
            &mut write_set,
            &mut visited,
            &mut cascade_rows,
            true,
        )
        .await?;
        aggregate_rows = aggregate_rows.saturating_add(cascade_rows);
        if aggregate_rows > aggregate_budget {
            // Every statement so far is on the open transaction; returning
            // rolls it back atomically — nothing partial survives.
            return Err(RtDbError::conflict(format!(
                "purge cascade exceeds the aggregate limit of {aggregate_budget} rows; \
                 the purge was rolled back"
            )));
        }
        deleted += 1;
    }

    if deleted == 0 {
        tx.commit().await?;
        return Ok(PurgeResult::Confirmed {
            deleted: 0,
            truncated,
        });
    }

    // Durable change feed + audit rows commit with the deletes (outbox
    // discipline). Audit is unconditional for purge — see the module docs.
    crate::change_log::append(&mut tx, &pg_schema_name, &write_set).await?;
    crate::audit::write_audit_rows_on(&mut tx, &ctx.db, None, "purge", &write_set.ops).await?;
    tx.commit().await?;

    // Four-tap publication on the committed result (fan_out → op-feed →
    // audit → webhook), with the audit tap marked satisfied so the
    // post-commit pass skips it. Webhooks stay best-effort post-commit like
    // the reaper's. No quota refresh — the purge only frees storage.
    publish_taps_skip_side_writes(
        ctx,
        &schema,
        &write_set,
        None,
        "purge",
        true,
        false,
        crate::txn::TxnSideWrites {
            source: "purge",
            audit: true,
            webhooks: false,
        },
    )
    .await;
    tracing::info!(
        db = %ctx.db,
        table = %table_name,
        deleted,
        truncated,
        "admin purge committed"
    );
    Ok(PurgeResult::Confirmed { deleted, truncated })
}
