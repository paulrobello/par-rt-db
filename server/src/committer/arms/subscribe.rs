//! The `Subscribe` arm. Not a write: it registers a subscription and sends the
//! initial query result, inside the committer turn so it cannot interleave
//! with a write.

use crate::committer::*;

pub(in crate::committer) async fn handle_subscribe(
    ctx: &CommitterCtx,
    conn: ConnId,
    query_id: String,
    query: Query,
    tx: UnboundedSender<ServerMessage>,
    principal_ctx: PrincipalCtx,
) -> Result<(), RtDbError> {
    let schema = ctx.schemas.get(&ctx.pool, &ctx.db).await?;
    let result = execute_query(&ctx.pool, &ctx.db, &schema, &query, &principal_ctx, false).await?;
    let last = diff_canonical(&result, &query);
    // Mirror `subs::fan_out`: a serialization failure is logged and surfaced
    // as an internal error so the subscriber sees an explicit error rather
    // than a silently-pushed `{"result": null}` (QA-004). In practice
    // `QueryResult` has only serializable leaves, so this never fires today —
    // but the failure shape is no longer silent.
    let value = serde_json::to_value(&result).map_err(|err| {
        tracing::error!(error = %err, db = %ctx.db, "failed to serialize initial query result");
        RtDbError::internal("failed to serialize initial query result")
    })?;

    if tx
        .send(ServerMessage::QueryUpdate {
            query_id: query_id.clone(),
            result: value,
        })
        .is_err()
    {
        tracing::debug!(
            db = %ctx.db,
            query_id,
            "subscribe: connection already gone, not registering"
        );
        return Ok(());
    }

    // Resolve the table def so `register` can derive a fine-grained ReadSet —
    // `Indexed` (count / collect / unique on an eq-prefix window) or `Ordered`
    // (take / first / paginate, whose top-N boundary is seeded from `result`).
    // `execute_query` above already resolved the same table successfully, so
    // this lookup won't miss in practice; propagating its error (rather than
    // falling back to `Table`) matches today's behavior — a subscription whose
    // table has vanished between execute and register is already a transient
    // error path.
    let table_def = schema.table(&query.table)?;

    // ENH-011: enforce the per-db concurrent-subscription cap (RTDB_MAX_SUBS_PER_DB,
    // hot-reloadable). Uniform — no admin bypass — because `PrincipalCtx` cannot
    // distinguish an admin from a machine token at the committer (both arrive as
    // `PrincipalCtx::bypass()`, `user_id == None`); the db-level gate has already
    // authorized the connection. Runs before registration so a rejected subscribe
    // never enters the shard. `count_for_db` is approximate (a concurrent
    // unsubscribe can drop the count), which is acceptable — the cap is a guard
    // rail, not an exact budget, and a near-concurrent subscribe still lands within
    // `cap + (concurrent subscribers)` of the limit.
    let sub_cap = ctx.hot.load().max_subs_per_db;
    if sub_cap > 0 {
        let n = ctx.subs.count_for_db(&ctx.db).await;
        if n >= sub_cap {
            ctx.metrics
                .record_quota_rejection(&ctx.db, crate::metrics::QuotaKind::Subs);
            return Err(RtDbError::quota_exceeded(format!(
                "db '{}' has {} active subscription(s), limit is {sub_cap}",
                ctx.db, n
            )));
        }
    }

    ctx.subs
        .register(
            &ctx.db,
            conn,
            query_id,
            query,
            tx,
            last,
            principal_ctx,
            table_def,
            &result,
        )
        .await?;
    Ok(())
}
