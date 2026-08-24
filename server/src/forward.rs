//! Stage 4c — forward non-owner writes to the committer lease owner over
//! pg_notify (ENH-022; docs/superpowers/specs/
//! 2026-08-22-multi-instance-stage4-design.md, Part A "Non-owners").
//!
//! With the Stage 4 ownership lease, a write arriving at a replica that does
//! NOT own the database used to reply CONFLICT ("writes must reach the owning
//! replica"). Stage 4c is that last mile: the non-owner broadcasts the write
//! over NOTIFY, the owner (and only the owner) executes it inside its own
//! committer turn and notifies the reply back, and the non-owner returns the
//! owner's outcome to its client. If no owner responds within
//! `RTDB_FORWARD_TIMEOUT_MS`, the non-owner attempts the lease takeover
//! itself (in `committer.rs`) — the acquire path is the failover path.
//!
//! ## Channel layout
//!
//! - `rtdb_write_fwd`: request broadcast. Every replica LISTENs. Each payload
//!   names its origin instance; the origin skips its own request (it is a
//!   non-owner by construction), and every replica that does not hold `db`'s
//!   ownership lease drops it silently. Only the owner executes + replies, so
//!   a fleet of N replicas produces exactly one execution per forwarded write.
//! - `rtdb_write_replies`: reply broadcast. Payloads name their target
//!   instance; only the target resolves the pending oneshot.
//!
//! ## The spool (ARC-002)
//!
//! A `pg_notify` payload is capped at 8000 bytes and Postgres rejects anything
//! larger, so neither channel carries the body: both carry a 36-byte row id
//! into `rtdb_auth.forward_queue`, and the request/reply JSON lives in that
//! table's `payload jsonb` column. A request row is read by every replica (the
//! lease on `request.db` decides who acts) and deleted by the one that
//! executed; a reply row is claimed with an atomic `DELETE … RETURNING` that
//! doubles as the target filter. `run_forward_sweeper` reclaims anything left
//! behind by a crashed consumer. The request/reply wire structs are unchanged
//! — only the transport moved.
//!
//! ## Trust and authz
//!
//! The principal that authorized the write on the origin replica travels in
//! the payload (`PrincipalCtx`); the owner re-uses it verbatim, exactly as
//! the design specifies. NOTIFY is only writable by sessions with Postgres
//! credentials — the same trust domain as the database itself — so a peer
//! that can forge a forwarded write can already write the tables directly.
//! Rate limits stay checked at the origin (the client-facing edge); the
//! owner's committer enforces quotas inside its turn as always.
//!
//! ## Gating
//!
//! The listener task is spawned only under `RTDB_MULTI_INSTANCE`, and the
//! origin-side forward path only engages for a write submitted against a
//! SHADOW committer (a non-owner). A single-instance deploy never touches
//! either channel.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::postgres::PgListener;
use tokio::sync::{Mutex, oneshot};

use crate::auth::PrincipalCtx;
use crate::committer::{CommitterRequest, Committers};
use crate::error::RtDbError;
use crate::migrate::MigrateRequest;
use crate::schema::SchemaDef;
use crate::txn::Transaction;

/// NOTIFY channel for forwarded write requests. Fixed across every instance —
/// all replicas LISTEN; only the lease owner acts.
pub const WRITE_FORWARD_CHANNEL: &str = "rtdb_write_fwd";

/// NOTIFY channel for forwarded-write replies. Fixed across every instance;
/// payloads carry the target instance id and non-targets skip them.
pub const WRITE_REPLY_CHANNEL: &str = "rtdb_write_replies";

/// Hard ceiling on a spooled forward payload (ARC-002). Postgres would happily
/// take far more into a `jsonb` column, but a forwarded write this large is a
/// client bug rather than a workload — reject it at the edge instead of
/// pushing tens of megabytes through the fleet's shared Postgres.
const MAX_FORWARD_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

/// Insert one spool row. `target` is the instance the row is addressed to, or
/// `""` for a broadcast (every request is a broadcast; only the lease owner
/// acts on it).
async fn spool_insert(
    pool: &PgPool,
    id: uuid::Uuid,
    kind: &str,
    target: &str,
    payload_json: &str,
) -> Result<(), RtDbError> {
    sqlx::query(
        "INSERT INTO rtdb_auth.forward_queue (id, kind, target, payload) \
         VALUES ($1::uuid, $2, $3, $4::jsonb)",
    )
    .bind(id.to_string())
    .bind(kind)
    .bind(target)
    .bind(payload_json)
    .execute(pool)
    .await
    .map_err(|e| {
        tracing::warn!(error = %e, kind, "forward: spool insert failed");
        RtDbError::internal("failed to spool forwarded write")
    })?;
    Ok(())
}

/// Delete one spool row by id. Best-effort: the sweeper is the backstop.
async fn spool_delete(pool: &PgPool, id: uuid::Uuid) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM rtdb_auth.forward_queue WHERE id = $1::uuid")
        .bind(id.to_string())
        .execute(pool)
        .await
        .map(|_| ())
}

/// Load a spooled request body. Every replica reads it (the payload names the
/// database whose lease decides who executes), so this does NOT delete —
/// only the executing owner deletes, after it has replied. `None` means the
/// row is already gone: the owner consumed it, or the sweeper reclaimed it.
async fn spool_load_request(pool: &PgPool, id: uuid::Uuid) -> Option<ForwardRequest> {
    let row = sqlx::query_scalar::<_, String>(
        "SELECT payload::text FROM rtdb_auth.forward_queue \
         WHERE id = $1::uuid AND kind = 'request'",
    )
    .bind(id.to_string())
    .fetch_optional(pool)
    .await
    .unwrap_or_else(|e| {
        tracing::warn!(error = %e, "forward: spool request load failed");
        None
    })?;
    match serde_json::from_str::<ForwardRequest>(&row) {
        Ok(req) => Some(req),
        Err(e) => {
            tracing::warn!(error = %e, "forward: spooled request failed to decode; skipping");
            None
        }
    }
}

/// Claim a spooled reply addressed to `target`. The `DELETE … RETURNING` makes
/// the claim atomic, so a non-target replica gets `None` in one round trip and
/// the target can never process the same reply twice.
async fn spool_claim_reply(pool: &PgPool, id: uuid::Uuid, target: &str) -> Option<ForwardReply> {
    let row = sqlx::query_scalar::<_, String>(
        "DELETE FROM rtdb_auth.forward_queue \
         WHERE id = $1::uuid AND kind = 'reply' AND target = $2 RETURNING payload::text",
    )
    .bind(id.to_string())
    .bind(target)
    .fetch_optional(pool)
    .await
    .unwrap_or_else(|e| {
        tracing::warn!(error = %e, "forward: spool reply claim failed");
        None
    })?;
    match serde_json::from_str::<ForwardReply>(&row) {
        Ok(reply) => Some(reply),
        Err(e) => {
            tracing::warn!(error = %e, "forward: spooled reply failed to decode; skipping");
            None
        }
    }
}

/// One forwarded write request. `origin` tags the sending replica (the
/// origin's own listener skips it); `request_id` correlates the reply.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ForwardRequest {
    pub request_id: String,
    pub origin: String,
    pub db: String,
    pub write: ForwardWrite,
}

/// The write itself — one variant per reply-carrying committer write arm.
/// Fire-and-forget arms (scheduler/reaper/workflow) only ever originate from
/// an owner's own pollers, so they never need forwarding. Every field type is
/// the same serde type the client wire already uses, so replicas running the
/// same build agree byte-for-byte.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum ForwardWrite {
    Mutate {
        idempotency_key: Option<String>,
        txn: Transaction,
        principal: PrincipalCtx,
    },
    Migrate {
        request: MigrateRequest,
    },
    PushSchema {
        schema: SchemaDef,
    },
    MergeUsers {
        anon_id: String,
        real_id: String,
    },
    RestoreSchema {
        target_version: i64,
    },
}

/// The owner's outcome for a forwarded write. Exactly one of `ok` / `error`
/// is set: `ok` carries the arm's concrete result serialized (the origin
/// deserializes it back into the arm's type); `error` carries the owner's
/// `RtDbError` verbatim — a genuine execution failure, since "not the owner"
/// never produces a reply at all (the request is dropped and the origin
/// times out into takeover). Explicit fields rather than a `Result` type:
/// serde externally tags `Result` as `{"Ok": …}` / `{"Err": …}`, and the
/// explicit shape keeps the wire self-describing across replica versions.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ForwardReply {
    pub request_id: String,
    pub target: String,
    pub ok: Option<serde_json::Value>,
    pub error: Option<RtDbError>,
}

impl ForwardReply {
    /// Build a successful reply carrying the arm's serialized result.
    fn success(request_id: String, target: String, value: serde_json::Value) -> Self {
        Self {
            request_id,
            target,
            ok: Some(value),
            error: None,
        }
    }

    /// Build a failed reply carrying the owner's error verbatim.
    fn failure(request_id: String, target: String, error: RtDbError) -> Self {
        Self {
            request_id,
            target,
            ok: None,
            error: Some(error),
        }
    }

    /// The reply as the origin's submit path consumes it.
    fn outcome(&self) -> Result<serde_json::Value, RtDbError> {
        match (&self.ok, &self.error) {
            (Some(value), None) => Ok(value.clone()),
            (None, Some(err)) => Err(err.clone()),
            _ => Err(RtDbError::internal(
                "forwarded reply carried both ok and error (wire corruption)",
            )),
        }
    }
}

/// Why a forward attempt produced no reply. Both variants route the origin
/// into the takeover path — either nobody was notified, or nobody answered.
#[derive(Debug)]
pub enum ForwardFail {
    /// The `pg_notify` itself failed (Postgres unreachable) — no owner was
    /// told about the write.
    Notify(RtDbError),
    /// No owner replied before `RTDB_FORWARD_TIMEOUT_MS` elapsed. Either the
    /// owner died between the origin's last observation and now, or it is
    /// alive but too slow to answer; the takeover attempt distinguishes the
    /// two (a live owner's lease makes the acquire fail, and the write
    /// surfaces as CONFLICT to the client instead of hanging).
    Timeout,
}

/// Origin-side half of Stage 4c: registers pending requests, broadcasts them,
/// and resolves replies (delivered by [`run_forward_listener`]) back to the
/// waiting submitter. Shared between `Committers` (the submitter) and the
/// listener task via `Arc`.
pub struct Forwarder {
    pool: PgPool,
    instance_id: String,
    timeout: std::time::Duration,
    pending: Mutex<HashMap<String, oneshot::Sender<ForwardReply>>>,
}

impl Forwarder {
    pub fn new(pool: PgPool, instance_id: String, timeout: std::time::Duration) -> Arc<Self> {
        Arc::new(Self {
            pool,
            instance_id,
            timeout,
            pending: Mutex::new(HashMap::new()),
        })
    }

    /// Broadcast `write` for `db` and wait for the owner's reply. The caller
    /// (the non-owner's submit path) handles [`ForwardFail`] by attempting
    /// the lease takeover.
    pub async fn forward(
        &self,
        db: &str,
        write: ForwardWrite,
    ) -> Result<Result<serde_json::Value, RtDbError>, ForwardFail> {
        let row_id = uuid::Uuid::now_v7();
        let request_id = row_id.simple().to_string();
        let payload = ForwardRequest {
            request_id: request_id.clone(),
            origin: self.instance_id.clone(),
            db: db.to_string(),
            write,
        };
        // Every field is a plain serde type; this cannot fail in practice.
        let payload_json = serde_json::to_string(&payload).map_err(|e| {
            ForwardFail::Notify(RtDbError::internal(format!(
                "failed to serialize forwarded write: {e}"
            )))
        })?;
        if payload_json.len() > MAX_FORWARD_PAYLOAD_BYTES {
            return Err(ForwardFail::Notify(RtDbError::bad_request(
                "forwarded write too large",
            )));
        }
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(request_id.clone(), tx);
        // ARC-002: spool the body, notify only the row id. The NOTIFY payload
        // is a 36-byte uuid, comfortably inside Postgres's 8000-byte cap, and
        // the spooled row also survives a listener reconnect that would have
        // dropped an in-flight NOTIFY.
        if let Err(e) = spool_insert(&self.pool, row_id, "request", "", &payload_json).await {
            self.pending.lock().await.remove(&request_id);
            return Err(ForwardFail::Notify(e));
        }
        if let Err(e) = sqlx::query("SELECT pg_notify($1, $2)")
            .bind(WRITE_FORWARD_CHANNEL)
            .bind(row_id.to_string())
            .execute(&self.pool)
            .await
        {
            self.pending.lock().await.remove(&request_id);
            let _ = spool_delete(&self.pool, row_id).await;
            return Err(ForwardFail::Notify(RtDbError::internal(format!(
                "forward pg_notify failed: {e}"
            ))));
        }
        let reply = tokio::time::timeout(self.timeout, rx).await;
        match reply {
            Ok(Ok(reply)) => Ok(reply.outcome()),
            // The sender is only dropped by the reply path, which always
            // sends before dropping — treat a closed channel like a timeout.
            Ok(Err(_)) => Err(ForwardFail::Timeout),
            Err(_) => {
                self.pending.lock().await.remove(&request_id);
                Err(ForwardFail::Timeout)
            }
        }
    }

    /// Resolve a pending request's oneshot with `reply`. A missing entry is a
    /// late reply racing the origin's timeout — the submitter already moved
    /// on to takeover, so drop it (logged) rather than erroring.
    pub async fn handle_reply(&self, reply: ForwardReply) {
        let mut pending = self.pending.lock().await;
        if let Some(tx) = pending.remove(&reply.request_id) {
            let _ = tx.send(reply);
        } else {
            tracing::debug!(
                request_id = %reply.request_id,
                "forward reply arrived after the origin timed out; dropped"
            );
        }
    }
}

/// Await a forwarded arm's committer reply. The outer `Result` is the
/// channel (a dropped sender means the committer task died); the inner is
/// the arm's genuine outcome — both surface as `Err` so the origin learns
/// the write failed rather than hanging.
async fn recv_forwarded<T>(rx: oneshot::Receiver<Result<T, RtDbError>>) -> Result<T, RtDbError> {
    match rx.await {
        Ok(inner) => inner,
        Err(_) => Err(RtDbError::internal(
            "committer task dropped the forwarded reply",
        )),
    }
}

/// Owner-side half of Stage 4c, run inside the shared listener: execute a
/// forwarded write on THIS replica's committer. Returns `None` when this
/// replica does not hold `db`'s ownership lease (checked immediately before
/// submit) — the silent drop that makes the broadcast exactly-once, since
/// every non-owner returns `None` and only the owner replies.
async fn execute_as_owner(
    committers: &Committers,
    db: &str,
    write: ForwardWrite,
) -> Option<Result<serde_json::Value, RtDbError>> {
    if !committers.is_owner(db).await {
        return None;
    }
    // `submit_owned` bypasses the forwarding interception: this replica IS
    // the owner (verified above), so the write must execute locally —
    // re-forwarding here would loop the broadcast.
    let result = match write {
        ForwardWrite::Mutate {
            idempotency_key,
            txn,
            principal,
        } => {
            let (reply, reply_rx) = oneshot::channel();
            committers
                .submit_owned(
                    db,
                    CommitterRequest::Mutate {
                        idempotency_key,
                        txn,
                        principal_ctx: principal,
                        enqueued_at: std::time::Instant::now(),
                        reply,
                    },
                )
                .await
                .ok()?;
            recv_forwarded(reply_rx).await.and_then(serialize_result)
        }
        ForwardWrite::Migrate { request } => {
            let (reply, reply_rx) = oneshot::channel();
            committers
                .submit_owned(db, CommitterRequest::RunMigrate { request, reply })
                .await
                .ok()?;
            recv_forwarded(reply_rx).await.and_then(serialize_result)
        }
        ForwardWrite::PushSchema { schema } => {
            let (reply, reply_rx) = oneshot::channel();
            committers
                .submit_owned(db, CommitterRequest::RunPushSchema { schema, reply })
                .await
                .ok()?;
            recv_forwarded(reply_rx).await.and_then(serialize_result)
        }
        ForwardWrite::MergeUsers { anon_id, real_id } => {
            let (reply, reply_rx) = oneshot::channel();
            committers
                .submit_owned(
                    db,
                    CommitterRequest::RunMergeUsers {
                        anon_id,
                        real_id,
                        reply,
                    },
                )
                .await
                .ok()?;
            recv_forwarded(reply_rx).await.and_then(serialize_result)
        }
        ForwardWrite::RestoreSchema { target_version } => {
            let (reply, reply_rx) = oneshot::channel();
            committers
                .submit_owned(
                    db,
                    CommitterRequest::RunRestoreSchema {
                        target_version,
                        reply,
                    },
                )
                .await
                .ok()?;
            recv_forwarded(reply_rx).await.and_then(serialize_result)
        }
    };
    Some(result)
}

/// Spool a reply and notify its row id (ARC-002). Best-effort in the same
/// sense the old direct `pg_notify` was: a failure here means the origin times
/// out and takes over, which is the documented failover path.
async fn publish_reply(pool: &PgPool, reply: ForwardReply) {
    let target = reply.target.clone();
    let Ok(json) = serde_json::to_string(&reply) else {
        tracing::warn!("forward listener: reply failed to serialize");
        return;
    };
    let row_id = uuid::Uuid::now_v7();
    if spool_insert(pool, row_id, "reply", &target, &json)
        .await
        .is_err()
    {
        return;
    }
    if let Err(e) = sqlx::query("SELECT pg_notify($1, $2)")
        .bind(WRITE_REPLY_CHANNEL)
        .bind(row_id.to_string())
        .execute(pool)
        .await
    {
        tracing::warn!(error = %e, "forward listener: reply pg_notify failed");
        let _ = spool_delete(pool, row_id).await;
    }
}

/// Periodic reclaim of spool rows nobody consumed (ARC-002): an origin that
/// timed out before its request was picked up, a reply whose target died, or a
/// row orphaned by a listener reconnect. `retention` is derived from the
/// forward timeout — anything older than twice the timeout can no longer
/// matter to a live request.
pub async fn run_forward_sweeper(pool: PgPool, retention: std::time::Duration) {
    let tick = retention.max(std::time::Duration::from_secs(1));
    let retention_ms = retention.as_millis().min(i64::MAX as u128) as i64;
    loop {
        tokio::time::sleep(tick).await;
        if let Err(e) = sqlx::query(
            "DELETE FROM rtdb_auth.forward_queue \
             WHERE created_at < now() - make_interval(secs => $1::double precision)",
        )
        .bind(retention_ms as f64 / 1000.0)
        .execute(&pool)
        .await
        {
            tracing::warn!(error = %e, "forward sweeper: delete failed; retrying next tick");
        }
    }
}

/// Serialize an arm's concrete result for the reply payload. Infallible for
/// these plain serde types; a failure still maps to an internal error rather
/// than panicking inside the listener.
fn serialize_result<T: serde::Serialize>(value: T) -> Result<serde_json::Value, RtDbError> {
    serde_json::to_value(value)
        .map_err(|e| RtDbError::internal(format!("forward reply serialize: {e}")))
}

/// Long-lived LISTEN loop for Stage 4c, spawned by `AppState::new` only when
/// `RTDB_MULTI_INSTANCE` is true. Subscribes to both forward channels:
///
/// - `rtdb_write_fwd` requests: skip self-originated payloads, spawn the
///   owner-side execution (a spawned task per request so a busy committer
///   queue on one db cannot head-of-line-block forwarded writes for other
///   dbs), and notify the reply when this replica is the owner.
/// - `rtdb_write_replies` replies: resolve the local pending oneshot when
///   this replica is the target.
///
/// Same resilience contract as the op-feed listener (`notify::run_listener`):
/// connect/listen errors log and retry on a 2s backoff; a malformed payload
/// (version skew between replicas) is skipped, never fatal.
pub async fn run_forward_listener(
    pool: PgPool,
    committers: Committers,
    forwarder: Arc<Forwarder>,
    own_instance_id: String,
) {
    let backoff = std::time::Duration::from_secs(2);
    loop {
        let mut listener = match PgListener::connect_with(&pool).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "forward listener: connect_with failed; retrying in {:?}",
                    backoff
                );
                tokio::time::sleep(backoff).await;
                continue;
            }
        };
        if let Err(e) = listener
            .listen_all([WRITE_FORWARD_CHANNEL, WRITE_REPLY_CHANNEL])
            .await
        {
            tracing::error!(
                error = %e,
                "forward listener: listen_all failed; retrying in {:?}",
                backoff
            );
            tokio::time::sleep(backoff).await;
            continue;
        }
        tracing::info!(
            "forward listener: LISTENing on '{WRITE_FORWARD_CHANNEL}' + '{WRITE_REPLY_CHANNEL}' (instance_id={own_instance_id})"
        );
        loop {
            let notif = match listener.recv().await {
                Ok(n) => n,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "forward listener: recv failed; reconnecting in {:?}",
                        backoff
                    );
                    break;
                }
            };
            match notif.channel() {
                WRITE_REPLY_CHANNEL => {
                    // ARC-002: the NOTIFY carries only the spool row id; the
                    // atomic `DELETE … RETURNING` both claims the reply and
                    // filters out replicas this reply is not addressed to.
                    let Ok(id) = notif.payload().parse::<uuid::Uuid>() else {
                        tracing::warn!("forward listener: reply notify was not a row id; skipping");
                        continue;
                    };
                    let Some(reply) = spool_claim_reply(&pool, id, &own_instance_id).await else {
                        continue;
                    };
                    forwarder.handle_reply(reply).await;
                }
                _ => {
                    let Ok(id) = notif.payload().parse::<uuid::Uuid>() else {
                        tracing::warn!(
                            "forward listener: request notify was not a row id; skipping"
                        );
                        continue;
                    };
                    // Every replica loads the body (the lease on `request.db`
                    // decides who executes). A miss means the owner already
                    // consumed it, or the sweeper reclaimed a stale row.
                    let Some(request) = spool_load_request(&pool, id).await else {
                        continue;
                    };
                    // Self-dedupe: the origin is a non-owner by construction,
                    // and re-processing its own broadcast would loop.
                    if request.origin == own_instance_id {
                        continue;
                    }
                    let committers = committers.clone();
                    let notify_pool = pool.clone();
                    tokio::spawn(async move {
                        match execute_as_owner(&committers, &request.db, request.write).await {
                            Some(result) => {
                                let reply = match result {
                                    Ok(value) => ForwardReply::success(
                                        request.request_id,
                                        request.origin.clone(),
                                        value,
                                    ),
                                    Err(err) => ForwardReply::failure(
                                        request.request_id,
                                        request.origin,
                                        err,
                                    ),
                                };
                                publish_reply(&notify_pool, reply).await;
                                // The request row's work is done on the only
                                // replica that will ever execute it.
                                let _ = spool_delete(&notify_pool, id).await;
                            }
                            None => {
                                // Not the owner (or lost the lease between the
                                // check and the submit) — stay silent so the
                                // origin times out into takeover.
                                tracing::debug!(
                                    db = %request.db,
                                    "forward listener: not the owner; dropping forwarded write"
                                );
                            }
                        }
                    });
                }
            }
        }
        tracing::warn!(
            "forward listener: connection lost; reconnecting in {:?}",
            backoff
        );
        tokio::time::sleep(backoff).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forward_write_round_trips_through_json() {
        // The payload types cross replicas as JSON; every variant must
        // round-trip losslessly (the serde tag shape is internal to the
        // server, but a drift here breaks same-version replicas silently).
        let txn: Transaction = serde_json::from_value(serde_json::json!({
            "steps": [{ "op": "insert", "table": "items", "doc": { "title": "x" } }]
        }))
        .unwrap();
        let cases = vec![
            ForwardWrite::Mutate {
                idempotency_key: Some("k".into()),
                txn: txn.clone(),
                principal: PrincipalCtx {
                    user_id: Some("u1".into()),
                    email: Some("u1@example.com".into()),
                    tables: None,
                },
            },
            ForwardWrite::MergeUsers {
                anon_id: "anon".into(),
                real_id: "real".into(),
            },
            ForwardWrite::RestoreSchema { target_version: 3 },
        ];
        for case in cases {
            let json = serde_json::to_string(&case).unwrap();
            let back: ForwardWrite = serde_json::from_str(&json).unwrap();
            let json2 = serde_json::to_string(&back).unwrap();
            assert_eq!(json, json2, "variant must round-trip stably");
        }
    }

    #[test]
    fn forward_reply_carries_error_verbatim() {
        let reply = ForwardReply::failure(
            "r1".into(),
            "replica-a".into(),
            RtDbError::conflict("owned elsewhere"),
        );
        let json = serde_json::to_string(&reply).unwrap();
        let back: ForwardReply = serde_json::from_str(&json).unwrap();
        match back.outcome() {
            Err(e) => {
                assert_eq!(e.code, crate::error::ErrorCode::Conflict);
                assert_eq!(e.message, "owned elsewhere");
            }
            Ok(v) => panic!("expected an error reply, got {v}"),
        }
    }
}
