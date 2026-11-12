//! Subscription manager — live-query registration, invalidation, and fan-out.
//!
//! Runs inside the committer's serialized turn so a push never observes a
//! partial write. Each subscription records a `ReadSet` (point / indexed /
//! ordered) used to skip re-runs soundly when a written document provably cannot
//! affect the result; any doubt over-approximates to re-run. Two safety nets
//! guard the skip logic: an exhaustive `cmp_binds` match (a new `EqBind` variant
//! is a compile error, not a silent under-approximation) and the
//! `RTDB_SUBS_VERIFY_SKIP_EVERY` shadow-verify probe. `distinct`/`search`/
//! `vector`/`hybrid` stay table-level by design — their results depend on
//! member values (beyond the eq/range window) or a ranking function.
//! `aggregate` (`sum`/`avg`/`min`/`max`/`count`) shares `count`/`collect`'s
//! `ReadSet::Indexed` window: a write provably outside the eq-prefix/range
//! window cannot move an aggregate scoped to that window.
//!
//! QA-003 split: read-set classification lives in `read_set`, the skip
//! decision in `decide`, credential liveness gating in `liveness`, and
//! this module keeps the registry and fan-out execution.

mod decide;
mod liveness;
mod read_set;

#[cfg(test)]
mod tests;

use read_set::ReadSet;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::StreamExt;
use serde::Serialize;
use sqlx::PgPool;
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedSender;
use tracing::Instrument;

use crate::auth::{PrincipalCtx, authorize_table};
use crate::dsl::QueryTerminalNameExt;
use crate::error::RtDbError;
use crate::metrics::Metrics;
use crate::protocol::ServerMessage;
use crate::query::{Query, QueryResult, diff_canonical, execute_query};
use crate::schema::{SchemaDef, TableDef};
use crate::txn::WriteSet;

use decide::{Decision, decide};
use liveness::{
    apply_host_liveness, apply_session_liveness, host_liveness, host_may_push, session_liveness,
    session_may_push,
};

pub type ConnId = u64;

/// Bound on concurrent subscription re-runs within one `fan_out` call
/// (ENH-034 / ARC-006). Each re-run is a Postgres round-trip against the
/// shared pool, so this caps how many connections one bulk write's fan-out
/// can hold at once — sized well under `RTDB_POOL_MAX_CONNECTIONS`' default
/// (75), which already budgets for "one committer task + N sub re-runs per
/// db" (`config/mod.rs`).
const FAN_OUT_CONCURRENCY: usize = 16;

static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

/// Allocates a fresh, process-unique connection id.
pub fn next_conn_id() -> ConnId {
    NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed)
}

struct SubEntry {
    query: Query,
    tx: UnboundedSender<ServerMessage>,
    last: String,
    read_set: ReadSet,
    /// The subscriber's per-row auth context, captured at subscribe time.
    /// `user_id = None` = bypass (machine tokens / scheduled jobs / admin);
    /// `Some` = re-run this subscription's query filtered to that user's rows,
    /// and (Task 6+) resolve `$email` markers against this identity.
    principal_ctx: PrincipalCtx,
    live_host_principal: Option<crate::auth::Principal>,
}

/// One database's subscriptions, keyed by `(connection, queryId)`.
type DbSubs = HashMap<(ConnId, String), SubEntry>;

/// Registered live-query subscriptions, sharded per database. Each database's
/// subscriptions live behind its own `Arc<Mutex<DbSubs>>`, so a per-db
/// committer's `fan_out` (a Postgres re-run per affected subscription) holds
/// ONLY its own shard lock — it does not collide with other databases' writes,
/// subscribes, or teardowns. The outer `Mutex<HashMap<..>>` is acquired only
/// long enough to look up or insert the target shard's `Arc` (an in-memory
/// op with no I/O), then dropped before any per-shard lock is taken.
///
/// `register` and `fan_out` are called only from the per-db committer task
/// (see `committer.rs`), which serializes them against every mutation;
/// `remove`/`remove_conn` may be called from anywhere (e.g. connection
/// teardown).
///
/// Lock discipline: the outer lock is NEVER held while waiting on an inner
/// shard lock — every path drops outer before taking inner, so there is no
/// lock-ordering cycle. Empty shards are RETAINED (lazy) rather than evicted
/// under a second lock acquire: evicting a now-empty shard would race a
/// concurrent `register` that already cloned this same Arc (before the
/// eviction) and would orphan its subscription — `fan_out` would no longer
/// find the shard. Shard count is bounded by the number of databases (each
/// persistent), and an empty shard is a single empty HashMap, so retaining
/// them costs nothing measurable.
pub struct SubscriptionManager {
    subs: Mutex<HashMap<String, Arc<Mutex<DbSubs>>>>,
    host_principals: Mutex<HashMap<ConnId, crate::auth::Principal>>,
    /// Where `fan_out` records skip/re-run effectiveness and verification
    /// outcomes. `None` = don't record (the bare `new()` used by tests that
    /// assert nothing about instrumentation).
    metrics: Option<Arc<Metrics>>,
    /// Verify 1 skip in every N (0 = off). See `Config::subs_verify_skip_every`.
    verify_skip_every: u64,
    /// Monotonic skip counter driving the deterministic 1-in-N sampler. Only
    /// read when `verify_skip_every > 0`.
    skip_seq: AtomicU64,
}

/// One active subscription, as surfaced by the inspector endpoint
/// (`GET /admin/subscriptions`). camelCase on the wire. The registry keys on
/// `(connection, queryId)`, so each row is ONE subscriber's ONE query — there
/// is intentionally no per-sub subscriber count.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubSnapshot {
    pub db: String,
    pub table: String,
    pub terminal: String,
    pub read_set_class: String,
    /// The subscriber's interactive identity, when it has one. `None` for
    /// system/bypass principals (machine tokens, scheduled jobs, admin).
    pub principal: Option<SubPrincipal>,
}

/// The subscriber identity on a [`SubSnapshot`]. Surfaced only when the
/// subscription carries an interactive user (`userId` and/or `email`); a
/// machine token has neither, so its row reports `principal: null`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubPrincipal {
    pub user_id: Option<String>,
    pub email: Option<String>,
}

fn principal_snapshot(ctx: &PrincipalCtx) -> Option<SubPrincipal> {
    if ctx.user_id.is_none() && ctx.email.is_none() {
        return None;
    }
    Some(SubPrincipal {
        user_id: ctx.user_id.clone(),
        email: ctx.email.clone(),
    })
}

impl SubscriptionManager {
    /// Uninstrumented manager: no metrics recording, no skip verification.
    pub fn new() -> Arc<Self> {
        Self::with_instrumentation(None, 0)
    }

    /// Manager wired to the process metrics and the skip-verification sampler
    /// (`AppState::new` passes `Config::subs_verify_skip_every`).
    pub fn with_instrumentation(
        metrics: Option<Arc<Metrics>>,
        verify_skip_every: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            subs: Mutex::new(HashMap::new()),
            host_principals: Mutex::new(HashMap::new()),
            metrics,
            verify_skip_every,
            skip_seq: AtomicU64::new(0),
        })
    }

    /// Whether this skip should be shadow-verified. Deterministic every-Nth
    /// sampling rather than random, so a test can pin the rate and get exact
    /// counts. Counts every skip so the stride is stable regardless of which
    /// read-set class produced it.
    fn sample_skip_verification(&self) -> bool {
        let every = self.verify_skip_every;
        if every == 0 {
            return false;
        }
        let seq = self.skip_seq.fetch_add(1, Ordering::Relaxed);
        seq.is_multiple_of(every)
    }

    /// Get-or-insert the shard for `db` (used by `register`). The outer lock
    /// is held only across the entry API; the returned `Arc` clones out.
    async fn shard_insert(&self, db: &str) -> Arc<Mutex<DbSubs>> {
        let mut guard = self.subs.lock().await;
        guard
            .entry(db.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(HashMap::new())))
            .clone()
    }

    /// Clone the shard `Arc` for `db` if it exists (used by `remove`,
    /// `remove_conn`, `fan_out`). The outer lock is held only across the get.
    async fn shard_get(&self, db: &str) -> Option<Arc<Mutex<DbSubs>>> {
        let guard = self.subs.lock().await;
        guard.get(db).cloned()
    }

    /// Whether this manager has a shard for `db`. Shards are retained after
    /// teardown, so a concurrent registration cannot race a notification into
    /// the fast path as an apparent non-subscriber.
    pub(crate) async fn has_db(&self, db: &str) -> bool {
        self.shard_get(db).await.is_some()
    }

    pub async fn remove(&self, db: &str, conn: ConnId, query_id: &str) {
        let Some(shard) = self.shard_get(db).await else {
            return;
        };
        let mut db_subs = shard.lock().await;
        db_subs.remove(&(conn, query_id.to_string()));
        // Empty shard is retained (lazy) — see the struct doc. Evicting here
        // would orphan a concurrently-registered subscription.
    }

    pub(crate) async fn authorize_host_push(
        &self,
        pool: &PgPool,
        db: &str,
        conn: ConnId,
        ctx: &PrincipalCtx,
    ) -> Result<(), RtDbError> {
        if ctx.host_capability.is_none() {
            return Ok(());
        }
        let principal = self
            .host_principals
            .lock()
            .await
            .get(&conn)
            .cloned()
            .ok_or_else(|| RtDbError::forbidden("host subscription requires live identity"))?;
        crate::auth::authorize(pool, &principal, db).await
    }

    pub(crate) async fn bind_host_principal(
        &self,
        conn: ConnId,
        principal: &crate::auth::Principal,
    ) {
        if principal.row_ctx().host_capability.is_some() {
            self.host_principals
                .lock()
                .await
                .insert(conn, principal.clone());
        }
    }

    pub async fn remove_conn(&self, db: &str, conn: ConnId) {
        self.host_principals.lock().await.remove(&conn);
        let Some(shard) = self.shard_get(db).await else {
            return;
        };
        let mut db_subs = shard.lock().await;
        db_subs.retain(|(c, _), _| *c != conn);
        // Empty shard is retained (lazy) — see the struct doc.
    }

    /// Drops every subscription for `db` and removes its shard, used by
    /// `delete-db` to evict a deleted database's live-query state. Live
    /// `/sync` connections to `db` will see errors on their next op (the
    /// schema is gone), which is acceptable for a deleted database. Unlike
    /// `remove`/`remove_conn`, this drops the shard Arc entirely — fine here
    /// because no future `register` will target a deleted db (the next
    /// `submit` for it 404s at `database_exists` before reaching the committer).
    pub async fn drop_db(&self, db: &str) {
        let mut guard = self.subs.lock().await;
        guard.remove(db);
    }

    /// Total active subscriptions across all databases (a dashboard gauge).
    /// Approximate by design — each shard is locked individually after the
    /// outer map is released, so a subscribe/unsubscribe racing this call can
    /// shift the count by one. That is acceptable for a metrics gauge and
    /// avoids holding the outer lock while waiting on every shard's inner
    /// lock (which would re-introduce the global serialization ARC-001
    /// removed).
    pub async fn count(&self) -> usize {
        let shards: Vec<Arc<Mutex<DbSubs>>> = {
            let guard = self.subs.lock().await;
            guard.values().cloned().collect()
        };
        let mut total = 0;
        for shard in shards {
            total += shard.lock().await.len();
        }
        total
    }

    /// Active subscription count for one database. Same lock discipline as
    /// [`count`](Self::count): clone the shard `Arc` under the outer lock, drop
    /// it, then lock the shard — the outer lock is never held while waiting on
    /// the shard, so this cannot deadlock against `fan_out`/`register`/`remove`.
    /// Approximate by design (a concurrent subscribe/unsubscribe can shift the
    /// count by one), which is acceptable for a cap check that runs once per
    /// subscribe before registration.
    pub async fn count_for_db(&self, db: &str) -> usize {
        let shard = { self.subs.lock().await.get(db).cloned() };
        match shard {
            Some(s) => s.lock().await.len(),
            None => 0,
        }
    }

    /// Snapshot every active subscription for the inspector endpoint
    /// (`GET /admin/subscriptions`), optionally filtered to one db. Same lock
    /// discipline as [`count`](Self::count): clone the matching shard `Arc`s
    /// under the outer map lock, drop it, then lock shards one at a time — the
    /// outer lock is never held while waiting on a shard, so this cannot
    /// deadlock against `fan_out`/`register`/`remove`. Approximate by design
    /// (a subscribe/unsubscribe racing this call can add or drop a row), which
    /// is acceptable for an operator inspection view. Sorted by `(db, table)`
    /// for stable output; does not touch the single-writer invariant (this is
    /// a read of the shared registry, not a txn execution).
    pub async fn snapshot(&self, db_filter: Option<&str>) -> Vec<SubSnapshot> {
        // Clone the matching shard Arcs under the outer lock, then drop it.
        let shards: Vec<(String, Arc<Mutex<DbSubs>>)> = {
            let guard = self.subs.lock().await;
            guard
                .iter()
                .filter(|(db, _)| db_filter.is_none_or(|f| db.as_str() == f))
                .map(|(db, shard)| (db.clone(), shard.clone()))
                .collect()
        };
        let mut out = Vec::new();
        for (db, shard) in shards {
            let db_subs = shard.lock().await;
            for entry in db_subs.values() {
                out.push(SubSnapshot {
                    db: db.clone(),
                    table: entry.query.table.clone(),
                    terminal: entry.query.terminal_name().to_string(),
                    read_set_class: entry.read_set.class_name().to_string(),
                    principal: principal_snapshot(&entry.principal_ctx),
                });
            }
        }
        out.sort_by(|a, b| a.db.cmp(&b.db).then(a.table.cmp(&b.table)));
        out
    }

    /// Registers a subscription that has already sent its initial
    /// `QueryUpdate` with `last` as the canonical form of that initial result.
    /// Called only by the committer task, immediately after the initial send,
    /// so no fan-out between execute and register can be missed. `owner` is
    /// the subscriber's per-row auth identity (see `SubEntry::owner`).
    /// `table_def` resolves the query's index/field types so an `Indexed` or
    /// `Ordered` ReadSet can be derived for fine-grained invalidation; a `get`
    /// query ignores it. The def is borrowed only for derivation — the stored
    /// `ReadSet` owns its binds, so the subscription survives a later schema
    /// evolution (an `Indexed` referencing a since-changed field biases to
    /// re-run via `Window::contains`'s "any doubt ⇒ outside" rule).
    /// `initial` is the result just pushed, from which an `Ordered` read seeds
    /// its top-N boundary; every other shape ignores it.
    ///
    /// ENH-005 Task 4: the machine-token table allowlist is enforced here, at
    /// registration, so a scoped token cannot subscribe to a forbidden table.
    /// `fan_out` re-runs need no check — they reuse the stored `principal_ctx`
    /// that was already gated here, and the re-run's `execute_query` call would
    /// re-check harmlessly anyway. Returns `Forbidden` if `principal_ctx.tables`
    /// is a non-empty allowlist that does not contain the query's table; the
    /// sole caller (`handle_subscribe`) propagates the error to the WS client.
    // Each arg is independently required by the committer's register path;
    // bundling them into a context struct would add indirection without
    // reducing coupling (same call as `ws::handle_text_frame`).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn register(
        &self,
        db: &str,
        conn: ConnId,
        query_id: String,
        query: Query,
        tx: UnboundedSender<ServerMessage>,
        last: String,
        principal_ctx: PrincipalCtx,
        table_def: &TableDef,
        initial: &QueryResult,
    ) -> Result<(), RtDbError> {
        authorize_table(&principal_ctx, &query.table)?;
        let live_host_principal = self.host_principals.lock().await.get(&conn).cloned();
        if principal_ctx.host_capability.is_some() && live_host_principal.is_none() {
            return Err(RtDbError::forbidden(
                "host subscription requires live identity",
            ));
        }
        let mut read_set = ReadSet::from_query(&query, table_def);
        if let ReadSet::Ordered(ordered) = &mut read_set {
            ordered.boundary = ordered.boundary_from_result(initial);
        }
        let shard = self.shard_insert(db).await;
        let mut db_subs = shard.lock().await;
        db_subs.insert(
            (conn, query_id),
            SubEntry {
                query,
                tx,
                last,
                read_set,
                principal_ctx,
                live_host_principal,
            },
        );
        Ok(())
    }

    /// Re-runs every subscription on `db` whose query table is in
    /// `write_set`, pushing a `QueryUpdate` only when the canonical result
    /// changed. A subscriber whose re-run errors (e.g. an evolved schema) is
    /// logged and skipped, never fails the caller. Send errors (receiver
    /// dropped) are ignored; connection teardown is expected to call
    /// `remove_conn` separately.
    ///
    /// Records skip/re-run effectiveness on `self.metrics`, and — for 1 skip in
    /// every `verify_skip_every` — SHADOW-VERIFIES the skip: the query runs
    /// anyway and its result is compared against the last pushed one. A
    /// divergence means the read set under-approximated (a realtime update
    /// would have been dropped), so it is logged at ERROR, counted, and the
    /// corrected result IS pushed — verification repairs as well as reports.
    /// Only read-set skips are verified; the `write_set.tables` fast path above
    /// it is trivially sound (a query reads exactly one table) and verifying it
    /// would cost a round-trip per subscription per write.
    pub(crate) async fn fan_out(
        &self,
        pool: &PgPool,
        db: &str,
        schema: &SchemaDef,
        write_set: &WriteSet,
    ) {
        // ENH-018: wrap the whole re-run pass so a slow subscription (one whose
        // re-run query is expensive) is identifiable in a trace as a child of
        // `committer.mutate`/`committer.scheduled`/…. The per-sub `subs.rerun`
        // spans nest under this one. The body runs in an instrumented `async`
        // block because a sync `Span::enter` guard is `!Send` and would poison
        // this `Send` future across the `.await`s in `fan_out_inner`.
        let span = tracing::info_span!("subs.fan_out", db, tables = ?write_set.tables);
        self.fan_out_inner(pool, db, schema, write_set)
            .instrument(span)
            .await;
    }

    async fn fan_out_inner(
        &self,
        pool: &PgPool,
        db: &str,
        schema: &SchemaDef,
        write_set: &WriteSet,
    ) {
        // Clone the shard Arc out under the outer lock, then drop the outer
        // guard before the per-subscription re-runs. This is the heart of
        // ARC-001: the re-runs (each a Postgres round-trip) hold only this
        // db's shard lock, never the global map lock, so a slow `fan_out` on
        // db A cannot stall writes/subscribes/teardowns on db B.
        let Some(shard) = self.shard_get(db).await else {
            return;
        };
        let mut db_subs = shard.lock().await;

        // ENH-034: decide pass. Classify every matching subscription exactly as
        // before (`decide` is unchanged), but only collect what a concurrent
        // re-run needs — the mutable `entry` borrow doesn't need to span the
        // `.await`s below, so it doesn't have to.
        let mut pending = Vec::new();
        for (key, entry) in db_subs.iter() {
            // A query with lookups reads its parent table plus every lookup
            // table, so the dispatch gate consults the union: a write naming a
            // lookup table must reach the re-run pass even though the
            // parent-table fast path (`write_set.tables.contains`) misses it.
            let parent_touched = write_set.tables.contains(&entry.query.table);
            let lookup_touched = entry
                .query
                .lookups
                .iter()
                .any(|lk| write_set.tables.contains(&lk.table));
            if !parent_touched && !lookup_touched {
                continue;
            }
            // When only a lookup table was written, the parent-table window
            // skip classes (`ReadSet` reasons only about parent-table writes)
            // are unsound — force the re-run and let the canonical diff absorb
            // the cost (cheap v1 rule; fine-grained skip optimization across
            // joined tables is deferred).
            let verifying = if lookup_touched {
                if let Some(metrics) = &self.metrics {
                    metrics.record_subs_rerun(db);
                }
                None
            } else {
                match decide(&entry.read_set, &entry.query.table, write_set) {
                    Decision::Rerun => {
                        if let Some(metrics) = &self.metrics {
                            metrics.record_subs_rerun(db);
                        }
                        None
                    }
                    Decision::Skip(class) => {
                        if let Some(metrics) = &self.metrics {
                            metrics.record_subs_skip(db, class);
                        }
                        if !self.sample_skip_verification() {
                            continue;
                        }
                        if let Some(metrics) = &self.metrics {
                            metrics.record_subs_skip_verification();
                        }
                        Some(class)
                    }
                }
            };
            pending.push((
                key.clone(),
                entry.query.clone(),
                entry.principal_ctx.clone(),
                verifying,
            ));
        }

        // ENH-034 / ARC-006: execute pass. Each re-run is an independent,
        // read-only Postgres round-trip, so run them concurrently instead of
        // one at a time — this is what was holding the single serialized
        // committer turn open for the full sum of every subscriber's re-run
        // latency (measured: ~3.4s median with 100 subscribers on a bulk
        // delete). Bounded via `FAN_OUT_CONCURRENCY` so a bulk write with many
        // subscribers doesn't open one connection per subscription against the
        // shared pool — `RTDB_POOL_MAX_CONNECTIONS`' default sizing already
        // accounts for "one committer task + N sub re-runs per db"
        // (`config/mod.rs`). `db_subs` stays locked and untouched across this
        // pass (no other task can observe or mutate it), so the apply pass
        // below always finds each entry.
        let results: Vec<_> = futures::stream::iter(pending)
            .map(|(key, query, principal_ctx, verifying)| {
                let query_id = key.1.as_str();
                // ENH-018: one span per subscription re-run so an expensive
                // subscription's query is identifiable by table/terminal in a
                // trace. `read_set_class` names which skip class this would
                // have been when verifying; `None` for a genuine re-run. The
                // span instruments the `execute_query` future directly (a sync
                // `enter()` guard would be `!Send` across the `.await`).
                let rerun_span = tracing::info_span!(
                    "subs.rerun",
                    db,
                    query_id,
                    table = %query.table,
                    terminal = query.terminal_name(),
                    read_set_class = ?verifying,
                );
                async move {
                    let result = execute_query(pool, db, schema, &query, &principal_ctx, false)
                        .instrument(rerun_span)
                        .await;
                    (key, verifying, result)
                }
            })
            .buffer_unordered(FAN_OUT_CONCURRENCY)
            .collect()
            .await;

        // One bounded-concurrency live lookup per credential, after queries and
        // before publication. No awaited lookups remain in the apply pass.
        let host_states = host_liveness(&db_subs, db, |principal| async move {
            crate::auth::authorize(pool, &principal, db).await
        })
        .await;
        apply_host_liveness(&mut db_subs, &host_states);

        // Session liveness (same bounded shape): a user session revoked by any
        // path — or expired — has its subscriptions dropped before publication,
        // closing the push hole the per-op checks can't reach (a stale fan-out
        // has no "next op"). Subs without a session hash (machine/system) are
        // unaffected.
        let session_states = session_liveness(&db_subs, |hash| async move {
            crate::auth::session::session_fan_out_alive(pool, &hash).await
        })
        .await;
        apply_session_liveness(&mut db_subs, &session_states);

        // Apply pass: identical bookkeeping to the former sequential loop,
        // just driven from the collected results instead of an in-place
        // iteration.
        for (key, verifying, result) in results {
            let query_id = key.1.as_str();
            let Some(entry) = db_subs.get_mut(&key) else {
                continue;
            };
            let result = match result {
                Ok(result) => result,
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        db,
                        query_id,
                        "subscription re-run failed; skipping"
                    );
                    continue;
                }
            };

            // Refresh an `Ordered` read's top-N boundary from the result just
            // computed, BEFORE the canonical diff: the boundary describes the
            // most recent COMPUTED result (not the last pushed one), which is
            // exactly the baseline the next skip decision reasons against. A
            // diff-suppressed re-run recomputes the same boundary, and a
            // re-run that errored above left the previous one in place —
            // still correct, since the previous result is then still current.
            if let ReadSet::Ordered(ordered) = &mut entry.read_set {
                ordered.boundary = ordered.boundary_from_result(&result);
            }

            let canon = diff_canonical(&result, &entry.query);
            if canon == entry.last {
                // Unchanged. For a verification pass this is the expected
                // outcome — the skip was correct, and it cost one round-trip to
                // prove it.
                continue;
            }

            if let Some(class) = verifying {
                // The skip decision was WRONG: we had decided this write could
                // not change the result, and it did. That is the one failure
                // mode invalidation must never have, and it is silent without
                // this check — so shout, count it, and fall through to push the
                // corrected result so the subscriber is repaired rather than
                // left stale.
                if let Some(metrics) = &self.metrics {
                    metrics.record_subs_missed_push(db);
                }
                tracing::error!(
                    db,
                    query_id,
                    class = ?class,
                    table = %entry.query.table,
                    read_set = ?entry.read_set,
                    "MISSED PUSH: subscription invalidation skipped a re-run whose result had \
                     changed — this is an invalidation soundness bug. Pushing the corrected \
                     result. Report the class + read_set + query shape."
                );
            }

            let value = match serde_json::to_value(&result) {
                Ok(value) => value,
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        db,
                        query_id,
                        "failed to serialize query result; skipping"
                    );
                    continue;
                }
            };

            if !host_may_push(entry, &host_states) || !session_may_push(entry, &session_states) {
                continue;
            }
            entry.last = canon;
            let _ = entry.tx.send(ServerMessage::QueryUpdate {
                query_id: query_id.to_string(),
                result: value,
            });
        }
    }
}
