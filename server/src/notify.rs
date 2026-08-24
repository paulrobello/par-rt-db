//! Cross-instance op-feed fan-out via Postgres LISTEN/NOTIFY (ENH-022 Stage 2),
//! plus cross-replica presence gossip (Stage 3) and cross-replica subscription
//! invalidation (ARC-001).
//!
//! Three publish/listen pairs live here, all gated on `RTDB_MULTI_INSTANCE`
//! and all self-deduping on `instance_id`:
//!
//! - `rtdb_ops` — the admin activity ring ([`publish_ops`] / [`run_listener`]).
//! - `rtdb_presence` — per-room member snapshots ([`run_presence_listener`]).
//! - `rtdb_write_sets` — subscription invalidation
//!   ([`publish_write_set`] / [`run_write_set_listener`]). This is the one that
//!   keeps a client's live query correct when the write landed on a different
//!   replica; the op-feed channel below only feeds the dashboard.
//!
//! The op-feed ring (`op_feed::OpFeed`) is in-process and instance-local: a
//! write committed on replica A publishes into A's ring only, so a dashboard
//! streaming `/admin/stream` on replica B never sees A's writes. Stage 2 lifts
//! that boundary for the op-feed without touching the single-writer invariant.
//!
//! ## How it works
//!
//! After every durable write commits inside the committer's serialized turn,
//! `publish_taps` calls [`publish_ops`] here — best-effort, exactly where the
//! audit/webhook taps already run, so the "every durable write publishes here"
//! contract extends to NOTIFY at the same single enforcement point. Each `DocOp`
//! is serialized to a small JSON payload and sent over `pg_notify('rtdb_ops',
//! …)`.
//!
//! A long-lived listener task ([`run_listener`]) on each instance holds a
//! `PgListener` subscribed to the `rtdb_ops` channel. On each notification it
//! reconstructs an [`OpEvent`](crate::op_feed::OpEvent) and feeds it into the
//! local ring via `OpFeed::publish_injected`. The listener performs NO write
//! and NO committer interaction — it only mirrors the notification into local
//! memory — so the single-writer invariant is preserved. A second replica is
//! not a second writer.
//!
//! ## Self-dedupe
//!
//! Postgres delivers a session's own `pg_notify` back to that same session, so
//! a process always receives its own notifications. Each payload carries the
//! origin `instance_id`; the listener skips any notification whose id matches
//! its own. Without this, every local write would land in the ring twice (once
//! from the direct `publish`, once from the echoed NOTIFY), which would double
//! the `/admin/stream` event count and confuse the dashboard.
//!
//! ## Gating
//!
//! Both sides are gated on `RTDB_MULTI_INSTANCE` (default false):
//! - The publish tap in `publish_taps` only fires when `multi_instance` is true,
//!   so a single-instance deploy pays zero `pg_notify` cost.
//! - The listener task is only spawned when `multi_instance` is true.
//!
//! A single-instance deploy is unchanged. `RTDB_INSTANCE_ID` is an optional
//! stable replica id; when unset, one is generated at boot.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::postgres::PgListener;

use crate::db::now_ms;
use crate::op_feed::{OpEvent, OpFeed};
use crate::presence::{PresenceManager, PresenceNotifyPayload};
use crate::txn::{DocOp, OpKind, WriteSet};

/// Postgres NOTIFY channel name for op-feed fan-out. Fixed across every
/// instance — all replicas LISTEN and NOTIFY on the same channel.
pub const OP_FEED_CHANNEL: &str = "rtdb_ops";

/// Postgres NOTIFY channel name for cross-instance presence gossip
/// (ENH-022 Stage 3). Fixed across every instance — all replicas LISTEN and
/// NOTIFY on the same channel. The payload is a [`PresenceNotifyPayload`]:
/// a full per-room local snapshot. See `presence::gossip_publish`.
pub const PRESENCE_CHANNEL: &str = "rtdb_presence";

/// Postgres NOTIFY channel for cross-replica subscription invalidation
/// (ARC-001). Every durable write publishes its `WriteSet` here so replicas
/// that did not execute the write still re-run the subscriptions it touched.
/// See [`publish_write_set`] and [`run_write_set_listener`].
pub const WRITE_SET_CHANNEL: &str = "rtdb_write_sets";

/// Serialized-size threshold above which a write set travels through the
/// forward spool instead of inline in the NOTIFY. Postgres caps a `pg_notify`
/// payload at 8000 bytes; 7500 leaves headroom for multi-byte escaping in the
/// table/id strings.
const WRITE_SET_INLINE_LIMIT: usize = 7500;

/// One cross-replica subscription-invalidation payload (ARC-001). `write_set`
/// carries `tables`/`docs`/`ops`; `doc_values` is `#[serde(skip)]` on
/// `WriteSet` and therefore never travels, so `Indexed`/`Ordered`
/// subscriptions on the receiving replica degrade to their conservative
/// "unrankable ⇒ re-run" fallback — never a missed push.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteSetNotifyPayload {
    pub instance_id: String,
    pub db: String,
    pub write_set: WriteSet,
}

/// One NOTIFY payload per `DocOp`. `camelCase` on the wire for consistency with
/// `OpEvent`. `instance_id` is the origin replica's id (self-dedupe); `source`
/// is the same `&'static str` tag the tap-site helper receives ("mutate" /
/// "scheduled" / "ttl" / "migrate"), owned as `String` for the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpNotifyPayload {
    pub instance_id: String,
    pub db: String,
    pub table: String,
    pub doc_id: String,
    pub kind: OpKind,
    pub ts: i64,
    pub owner: Option<String>,
    pub source: String,
}

/// Generate a short random hex instance id (8 hex chars = 4 bytes). Used when
/// `RTDB_INSTANCE_ID` is unset; NOT cryptographically significant — it only
/// needs to be unique among the replicas sharing one Postgres, and to stay
/// stable for the process lifetime so self-dedupe works. Uses `ring` (already a
/// dependency) for a CSPRNG so the id is not guessable across instances on a
/// shared DB, mirroring the repo's `auth::apple` randomness pattern.
pub fn generate_instance_id() -> String {
    let rng = ring::rand::SystemRandom::new();
    let mut bytes = [0u8; 4];
    // `fill` is infallible for `SystemRandom`; an error means the system RNG is
    // unavailable, which is a fatal environment condition — fall back to a
    // zero id rather than panicking in a boot path.
    let _ = ring::rand::SecureRandom::fill(&rng, &mut bytes);
    hex::encode(bytes)
}

/// Publish one NOTIFY per `DocOp`, best-effort. Called from `publish_taps` after
/// the local op-feed publish, inside the committer's serialized turn. A failure
/// logs a `warn!` and continues — the write has already committed (matching the
/// audit/webhook tap semantics: these taps can never fail the mutation). Uses a
/// single `ts` for all ops in the call, mirroring `OpFeed::publish` (all ops in
/// one txn share a timestamp).
pub async fn publish_ops(
    pool: &PgPool,
    instance_id: &str,
    db: &str,
    owner: Option<&str>,
    source: &str,
    ops: &[DocOp],
) {
    let ts = now_ms();
    let owner = owner.map(|s| s.to_string());
    for op in ops {
        let payload = OpNotifyPayload {
            instance_id: instance_id.to_string(),
            db: db.to_string(),
            table: op.table.clone(),
            doc_id: op.id.clone(),
            kind: op.kind,
            ts,
            owner: owner.clone(),
            source: source.to_string(),
        };
        let payload_json = match serde_json::to_string(&payload) {
            Ok(s) => s,
            Err(e) => {
                // Serialization cannot fail for this struct in practice (no
                // custom serialize), but a future field could change that. Log
                // and move on so one bad op never blocks the committer turn.
                tracing::warn!(
                    db = %db,
                    table = %op.table,
                    doc_id = %op.id,
                    error = %e,
                    "notify: failed to serialize payload; skipping this op"
                );
                continue;
            }
        };
        if let Err(e) = sqlx::query("SELECT pg_notify($1, $2)")
            .bind(OP_FEED_CHANNEL)
            .bind(&payload_json)
            .execute(pool)
            .await
        {
            tracing::warn!(
                db = %db,
                table = %op.table,
                error = %e,
                "notify: pg_notify failed (best-effort; write already committed)"
            );
        }
    }
}

/// Publish one write set for cross-replica subscription invalidation (ARC-001),
/// best-effort, once per commit. Called from `publish_taps` inside the
/// committer's serialized turn, alongside the op-feed NOTIFY.
///
/// Without this, a subscription living on replica B never re-runs for a write
/// the OWNER (replica A) executed: the op-feed NOTIFY only feeds the admin
/// activity ring, and the origin-side fan-out in `complete_forwarded_reply`
/// only covers the narrow case where B itself forwarded the write. Every
/// owner-side write — an HTTP mutate that reached the owner directly, a
/// scheduled job, the TTL reaper, a migration — was invisible to B's clients
/// until their next local write.
///
/// A `WriteSet` for a bulk write easily exceeds Postgres's 8000-byte
/// `pg_notify` cap, so payloads at or above [`WRITE_SET_INLINE_LIMIT`] travel
/// through the forward spool and the NOTIFY carries only the row id. The
/// receiving side tells the two apart by shape: a JSON object starts with `{`,
/// a spool reference is a bare uuid.
pub async fn publish_write_set(pool: &PgPool, instance_id: &str, db: &str, write_set: &WriteSet) {
    let payload = WriteSetNotifyPayload {
        instance_id: instance_id.to_string(),
        db: db.to_string(),
        write_set: write_set.clone(),
    };
    let json = match serde_json::to_string(&payload) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(db = %db, error = %e, "notify: write set failed to serialize; skipping");
            return;
        }
    };
    let notify_payload = if json.len() < WRITE_SET_INLINE_LIMIT {
        json
    } else {
        match crate::forward::spool_broadcast(pool, "writeset", &json).await {
            Ok(id) => id.to_string(),
            Err(_) => return,
        }
    };
    if let Err(e) = sqlx::query("SELECT pg_notify($1, $2)")
        .bind(WRITE_SET_CHANNEL)
        .bind(&notify_payload)
        .execute(pool)
        .await
    {
        tracing::warn!(
            db = %db,
            error = %e,
            "notify: write-set pg_notify failed (best-effort; write already committed)"
        );
    }
}

/// Long-lived LISTEN loop for the `rtdb_write_sets` channel (ARC-001). Spawned
/// by `AppState::new` only when `RTDB_MULTI_INSTANCE` is true. For each
/// non-self notification it loads the write set (inline or from the spool),
/// resolves the database's schema, and re-runs THIS replica's subscriptions
/// against it — the same `subs.fan_out` call the committer makes locally.
///
/// Performs NO write and NO committer interaction: `fan_out` issues read
/// queries only, and reads are safe outside the serialized turn under READ
/// COMMITTED. The single-writer invariant is intact — this is a second
/// *reader*, not a second writer.
pub async fn run_write_set_listener(
    pool: PgPool,
    subs: Arc<crate::subs::SubscriptionManager>,
    schemas: crate::db::SchemaCache,
    own_instance_id: String,
) {
    let backoff = std::time::Duration::from_secs(2);
    loop {
        let mut listener = match PgListener::connect_with(&pool).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "write-set listener: connect_with failed; retrying in {:?}",
                    backoff
                );
                tokio::time::sleep(backoff).await;
                continue;
            }
        };
        if let Err(e) = listener.listen_all([WRITE_SET_CHANNEL]).await {
            tracing::error!(
                error = %e,
                "write-set listener: listen_all failed; retrying in {:?}",
                backoff
            );
            tokio::time::sleep(backoff).await;
            continue;
        }
        tracing::info!(
            "write-set listener: LISTENing on '{}' for cross-replica subscription invalidation (instance_id={})",
            WRITE_SET_CHANNEL,
            own_instance_id
        );
        loop {
            let notif = match listener.recv().await {
                Ok(n) => n,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "write-set listener: recv failed; reconnecting in {:?}",
                        backoff
                    );
                    break;
                }
            };
            let raw = notif.payload().to_string();
            let json = if raw.starts_with('{') {
                raw
            } else {
                let Ok(id) = raw.parse::<uuid::Uuid>() else {
                    tracing::warn!("write-set listener: payload was neither JSON nor a row id");
                    continue;
                };
                match crate::forward::spool_load_broadcast(&pool, id, "writeset").await {
                    Some(body) => body,
                    None => continue,
                }
            };
            let payload = match serde_json::from_str::<WriteSetNotifyPayload>(&json) {
                Ok(p) => p,
                Err(e) => {
                    // Version skew between replicas — skip, never fatal.
                    tracing::warn!(
                        error = %e,
                        "write-set listener: failed to decode payload; skipping"
                    );
                    continue;
                }
            };
            // Self-notification dedupe: this replica already fanned out inside
            // its own committer turn.
            if payload.instance_id == own_instance_id {
                continue;
            }
            let schema = match schemas.get(&pool, &payload.db).await {
                Ok(schema) => schema,
                Err(e) => {
                    tracing::warn!(
                        db = %payload.db,
                        error = %e,
                        "write-set listener: schema fetch failed; skipping invalidation"
                    );
                    continue;
                }
            };
            subs.fan_out(&pool, &payload.db, &schema, &payload.write_set)
                .await;
        }
        tracing::warn!(
            "write-set listener: connection lost; reconnecting in {:?}",
            backoff
        );
        tokio::time::sleep(backoff).await;
    }
}

/// Long-lived LISTEN loop for the `rtdb_ops` channel. Spawned by `AppState::new`
/// only when `RTDB_MULTI_INSTANCE` is true. For each notification: decode the
/// payload, skip self-notifications (dedupe), reconstruct the `OpEvent`, and
/// inject it into the local `OpFeed` ring + broadcast. Performs NO write and NO
/// committer interaction — the single-writer invariant is intact.
///
/// Resilient: a connect/listen error is logged at `error!` and retried with a
/// 2s backoff. The listener is the whole point of cross-instance fan-out, so a
/// transient Postgres blip must not kill it silently — it reconnects and keeps
/// mirroring events once Postgres is reachable again. Bounded-growth backoff
/// caps at 2s (short enough that a brief restart recovers within one tick;
/// long enough not to spin during an outage).
pub async fn run_listener(pool: PgPool, op_feed: std::sync::Arc<OpFeed>, own_instance_id: String) {
    let backoff = std::time::Duration::from_secs(2);
    loop {
        let mut listener = match PgListener::connect_with(&pool).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "notify listener: connect_with failed; retrying in {:?}",
                    backoff
                );
                tokio::time::sleep(backoff).await;
                continue;
            }
        };
        if let Err(e) = listener.listen_all([OP_FEED_CHANNEL]).await {
            tracing::error!(
                error = %e,
                "notify listener: listen_all failed; retrying in {:?}",
                backoff
            );
            tokio::time::sleep(backoff).await;
            continue;
        }
        tracing::info!(
            "notify listener: LISTENing on '{}' for cross-instance op-feed (instance_id={})",
            OP_FEED_CHANNEL,
            own_instance_id
        );
        // `recv` resolves to `Err` on a connection failure. Reconnect from the
        // top — the loop re-runs `connect_with` + `listen_all` and resumes.
        loop {
            let notif = match listener.recv().await {
                Ok(n) => n,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "notify listener: recv failed; reconnecting in {:?}",
                        backoff
                    );
                    break;
                }
            };
            let payload = match serde_json::from_str::<OpNotifyPayload>(notif.payload()) {
                Ok(p) => p,
                Err(e) => {
                    // A malformed payload most likely means a version skew
                    // between replicas (a newer field a peer sends that this
                    // process doesn't know). Skip it; never kill the listener.
                    tracing::warn!(
                        error = %e,
                        "notify listener: failed to decode payload; skipping"
                    );
                    continue;
                }
            };
            // Self-notification dedupe: a process always receives its own
            // `pg_notify`. The local `publish` already put this event in the
            // ring, so re-injecting would double-count it.
            if payload.instance_id == own_instance_id {
                continue;
            }
            let event = OpEvent {
                db: payload.db,
                table: payload.table,
                doc_id: payload.doc_id,
                kind: payload.kind,
                ts: payload.ts,
                owner: payload.owner,
            };
            op_feed.publish_injected(event).await;
        }
        tracing::warn!(
            "notify listener: connection lost; reconnecting in {:?}",
            backoff
        );
        tokio::time::sleep(backoff).await;
    }
}

/// Long-lived LISTEN loop for the `rtdb_presence` channel (ENH-022 Stage 3).
/// Spawned by `AppState::new` only when BOTH `RTDB_MULTI_INSTANCE` and
/// `RTDB_PRESENCE_ENABLED` are true. For each notification: decode the
/// [`PresenceNotifyPayload`], skip self-notifications (the origin instance is
/// already the source of those members locally), and call
/// `PresenceManager::ingest_peer_snapshot` to refresh the shadow map entry +
/// `last_beat` and mark the room dirty so the next flush broadcasts the union.
///
/// Performs NO write and NO committer interaction — peer presence lives only
/// in the in-memory shadow map inside `PresenceManager`. The single-writer
/// invariant is intact; this listener is a second *reader* of the NOTIFY
/// channel, not a second writer of document tables.
///
/// Resilient: same connect/listen error + 2s backoff loop as
/// [`run_listener`]; the presence listener is the whole point of cross-
/// instance presence, so a transient Postgres blip must not kill it silently.
pub async fn run_presence_listener(
    pool: PgPool,
    presence: Arc<PresenceManager>,
    own_instance_id: String,
) {
    let backoff = std::time::Duration::from_secs(2);
    loop {
        let mut listener = match PgListener::connect_with(&pool).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "presence listener: connect_with failed; retrying in {:?}",
                    backoff
                );
                tokio::time::sleep(backoff).await;
                continue;
            }
        };
        if let Err(e) = listener.listen_all([PRESENCE_CHANNEL]).await {
            tracing::error!(
                error = %e,
                "presence listener: listen_all failed; retrying in {:?}",
                backoff
            );
            tokio::time::sleep(backoff).await;
            continue;
        }
        tracing::info!(
            "presence listener: LISTENing on '{}' for cross-instance presence (instance_id={})",
            PRESENCE_CHANNEL,
            own_instance_id
        );
        loop {
            let notif = match listener.recv().await {
                Ok(n) => n,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "presence listener: recv failed; reconnecting in {:?}",
                        backoff
                    );
                    break;
                }
            };
            let payload = match serde_json::from_str::<PresenceNotifyPayload>(notif.payload()) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "presence listener: failed to decode payload; skipping"
                    );
                    continue;
                }
            };
            // Self-notification dedupe: same contract as the op-feed listener.
            // A process always receives its own `pg_notify`; the local op
            // already published into the local ring, so re-injecting would
            // double-count. For presence, ingest would only duplicate members
            // the local `peers` map already has under this instance id (which
            // never happens — local members aren't in `peers`), so dedupe is
            // defensive rather than load-bearing here, but skip for clarity.
            if payload.instance_id == own_instance_id {
                continue;
            }
            presence
                .ingest_peer_snapshot(
                    &payload.instance_id,
                    &payload.db,
                    &payload.room,
                    payload.members,
                )
                .await;
        }
        tracing::warn!(
            "presence listener: connection lost; reconnecting in {:?}",
            backoff
        );
        tokio::time::sleep(backoff).await;
    }
}
