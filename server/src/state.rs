//! Ordered builders for the server composition root.

use super::{
    Auth, BackgroundTasks, Config, HotConfig, Limits, OP_FEED_BROADCAST_CAP, OP_FEED_RING_CAP,
    Realtime, Runtime, SchemaCache, SubscriptionManager, committer, forward, image_transform,
    metrics, notify, op_feed, pool_watch, presence, quota, rate_limit, schema_gate, signed_url,
    watchdog,
};
use arc_swap::ArcSwap;
use committer::Committers;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, SystemTime};

/// Shared handles created before the committer and reused by the state groups.
pub(super) struct Dependencies {
    metrics: Arc<metrics::Metrics>,
    subs: Arc<SubscriptionManager>,
    op_feed: Arc<op_feed::OpFeed>,
    hot: Arc<ArcSwap<HotConfig>>,
    quotas: Arc<quota::UsageCache>,
    instance_id: String,
    rate_limiter: Arc<rate_limit::RateLimiter>,
    forwarder: Option<Arc<forward::Forwarder>>,
    schema_gate: Arc<schema_gate::SchemaGate>,
}

pub(super) fn build_dependencies(
    pool: &sqlx::PgPool,
    config: &Config,
    hot: HotConfig,
) -> Dependencies {
    let metrics = metrics::Metrics::with_slow_query_capacity(config.slow_query_capacity);
    // ENH-049: the multi-instance gauge is a boot-time constant of this
    // replica; it is Prometheus-only (see `render_prometheus`).
    metrics.set_multi_instance(config.multi_instance.enabled);
    // The subscription manager records invalidation effectiveness on the
    // same `Metrics` the dashboard reads, and owns the skip-verification
    // sampler — so it is built after metrics and before the committers.
    let subs = SubscriptionManager::with_instrumentation(
        Some(metrics.clone()),
        config.subs_verify_skip_every,
    );
    let op_feed = op_feed::OpFeed::new(OP_FEED_BROADCAST_CAP, OP_FEED_RING_CAP);
    let hot = Arc::new(ArcSwap::from_pointee(hot));
    // Per-db storage-usage cache (ENH-011). Built before the committers so
    // it can be threaded into `Committers::new` (the committer arms enforce
    // `maxStorageBytesPerDb` on every growing write) and Arc-shared onto
    // `AppState` for the storage/upload paths.
    let quotas = Arc::new(quota::UsageCache::new());
    // ENH-022 Stage 2: resolve the instance id once. An explicit
    // RTDB_INSTANCE_ID wins; otherwise generate a short hex id for this
    // process. The id tags NOTIFY payloads so a receiver can skip its own
    // notifications (self-dedupe). Generated before the committers so it can
    // be threaded into `Committers::new`.
    let instance_id = config
        .multi_instance
        .instance_id
        .clone()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(notify::generate_instance_id);
    // ARC-002: the resolved id tags NOTIFY payloads (self-dedupe) and the
    // multi-instance identity threading below — an empty id would make a
    // replica filter its own notifications wrong. This is the one
    // construction invariant that is data-dependent rather than
    // dependency ordering: builders receive the shared handles they need.
    debug_assert!(!instance_id.trim().is_empty());
    let multi_instance = config.multi_instance.enabled;
    let rate_limit_exact = config.limits.exact;
    // ENH-022 Stage 4 / ARC-007: in multi-instance mode the counters live
    // in Postgres so every replica shares one budget per token/db/ip;
    // single-instance (the default) keeps the in-memory limiter and
    // never touches the table. Shared with the approximate flush task and `Limits`.
    let rate_limiter = if multi_instance {
        rate_limit::RateLimiter::new_pg(pool.clone(), rate_limit_exact)
    } else {
        rate_limit::RateLimiter::new()
    };
    // ENH-022 Stage 4c: the origin-side forwarding handle. Built before
    // the committers (they hold it for the shadow-write submit path) and
    // shared with the forward listener task spawned below, which resolves
    // its replies. `None` in single-instance mode — the forward path is
    // unreachable without a shadow, and a single-instance deploy pays for
    // neither the handle nor the listener.
    let forwarder = if multi_instance {
        Some(forward::Forwarder::new(
            pool.clone(),
            instance_id.clone(),
            std::time::Duration::from_millis(config.multi_instance.forward_timeout_ms),
        ))
    } else {
        None
    };
    // 2026-11-05 incident: the schema-DDL admission gate is shared by the
    // committers' DDL arms, the managed backup task, and the manual
    // `/admin/backup` trigger, so it must be built once here — before both
    // consumers — and cloned into `Runtime` (for the backup paths) and
    // `CommitterConfig` (for the DDL arms).
    let schema_gate = Arc::new(schema_gate::SchemaGate::new());
    Dependencies {
        metrics,
        subs,
        op_feed,
        hot,
        quotas,
        instance_id,
        rate_limiter,
        forwarder,
        schema_gate,
    }
}

pub(super) fn build_committers(
    pool: &sqlx::PgPool,
    config: &Config,
    schemas: &SchemaCache,
    deps: &Dependencies,
) -> Committers {
    Committers::new(
        pool.clone(),
        deps.subs.clone(),
        schemas.clone(),
        deps.op_feed.clone(),
        deps.hot.clone(),
        deps.metrics.clone(),
        committer::CommitterConfig::from_config(
            config,
            deps.quotas.clone(),
            deps.instance_id.clone(),
            deps.forwarder.clone(),
            deps.schema_gate.clone(),
        ),
    )
}

pub(super) fn build_background(committers: &Committers) -> Arc<BackgroundTasks> {
    // ARC-009: cancellation + join handles for every background task
    // spawned below. Built here so every `background.spawn`/`.track` call
    // that follows can reach it.
    let background = Arc::new(BackgroundTasks::new());
    // ARC-102 step 4: spawn the server-wide idle-reclamation sweep. A no-op
    // when `db_idle_reclaim_secs` is 0 (the default), so a server that does
    // not opt in pays zero background cost.
    //
    // ARC-009: the sweep hands back its `JoinHandle` (`None` when
    // disabled) so it is tracked here and stops with every other
    // background task on shutdown.
    if let Some(handle) = committers.spawn_idle_reclaimer() {
        background.track(handle);
    }
    background
}

pub(super) fn build_limits(config: &Config, deps: &Dependencies) -> Limits {
    // Image transform cache shares the same `Arc<Metrics>` as Runtime and
    // the committers so its hit/miss/error counters surface on the dashboard.
    let signed_url_key = Arc::new(signed_url::derive_key(&config.admin_key));
    let image = Arc::new(image_transform::TransformCache::new(
        image_transform::TransformConfig::from_config(config),
        config.storage.image.cache_bytes,
        config.storage.image.concurrency,
        deps.metrics.clone(),
    ));
    Limits {
        rate_limiter: deps.rate_limiter.clone(),
        image,
        quotas: deps.quotas.clone(),
        signed_url_key,
    }
}

pub(super) fn build_realtime(
    pool: &sqlx::PgPool,
    config: &Config,
    schemas: &SchemaCache,
    deps: &Dependencies,
    committers: Committers,
    background: &BackgroundTasks,
) -> Realtime {
    // Presence manager (ENH-015) shares the same `Arc<Metrics>` so its
    // update/broadcast counters surface on the dashboard. It is a sibling
    // of the committer, NOT routed through it: the periodic flush task is
    // spawned only when `presence_enabled` (default on) — a disabled
    // server still has the manager (so `presenceErr` works) but no
    // spinning task.
    //
    // ENH-022 Stage 3: in multi-instance mode the manager carries the
    // instance id + pool so it can `pg_notify` per-room snapshots to peer
    // replicas, and the flush task also fires a liveness beat every
    // `RTDB_PRESENCE_BEAT_INTERVAL_MS` (default 5s). Single-instance mode
    // passes `multi_instance=false, pool=None` — the manager never touches
    // NOTIFY, and every wire byte is unchanged.
    let presence_cfg = presence::PresenceConfig::from_config(config);
    let presence = presence::PresenceManager::new(
        Some(deps.metrics.clone()),
        presence_cfg,
        config.multi_instance.enabled,
        deps.instance_id.clone(),
        if config.multi_instance.enabled {
            Some(pool.clone())
        } else {
            None
        },
    );
    if presence_cfg.enabled {
        // ARC-009: `run_flush_task` already returns its own `JoinHandle`
        // (unlike the `tokio::spawn` sites below), so it is `track`ed
        // rather than wrapped in `background.spawn`'s `select!` —
        // cancellation relies on `abort` in `BackgroundTasks::cancel`.
        background.track(presence.clone().run_flush_task());
    }
    spawn_listeners(
        pool,
        config,
        schemas,
        deps,
        &committers,
        &presence,
        background,
    );
    spawn_monitors(pool, config, deps, background);
    Realtime {
        subs: deps.subs.clone(),
        committers,
        op_feed: deps.op_feed.clone(),
        presence,
    }
}

fn spawn_listeners(
    pool: &sqlx::PgPool,
    config: &Config,
    schemas: &SchemaCache,
    deps: &Dependencies,
    committers: &Committers,
    presence: &Arc<presence::PresenceManager>,
    background: &BackgroundTasks,
) {
    // ENH-022 Stage 2: cross-instance op-feed LISTEN task. Only spawned when
    // `RTDB_MULTI_INSTANCE=true` — a single-instance deploy never pays the
    // `PgListener` connection. Runs for the process lifetime (or until
    // `background.token` cancels), reconnects on transient Postgres blips,
    // and self-dedupe is handled inside it via `instance_id`.
    if config.multi_instance.enabled {
        background.spawn(notify::run_listener(
            pool.clone(),
            deps.op_feed.clone(),
            deps.instance_id.clone(),
        ));
        // ARC-001: cross-replica subscription invalidation. Same shape as
        // the op-feed listener — reads only, no committer interaction, so
        // the single-writer invariant is untouched.
        background.spawn(notify::run_write_set_listener(
            pool.clone(),
            deps.subs.clone(),
            schemas.clone(),
            deps.instance_id.clone(),
            deps.metrics.clone(),
        ));
        // ARC-004: cross-replica schema-cache invalidation. A push/migrate/
        // restore on one replica drops the cached schema entry on every
        // other replica; the next read reloads from Postgres. Reads only.
        background.spawn(notify::run_schema_listener(
            pool.clone(),
            schemas.clone(),
            deps.instance_id.clone(),
        ));
        // ARC-201: cross-replica hot-config coherence. Same shape as the
        // schema listener — reads the rtdb_config row and swaps the local
        // ArcSwap only; performs NO write and NO committer interaction.
        background.spawn(notify::run_config_listener(
            pool.clone(),
            deps.hot.clone(),
            deps.instance_id.clone(),
        ));
    }
    // ENH-022 Stage 3: cross-instance presence LISTEN task. Only spawned
    // when BOTH `RTDB_MULTI_INSTANCE=true` AND `RTDB_PRESENCE_ENABLED=true`
    // — a single-instance deploy never pays the second `PgListener`
    // connection. Runs for the process lifetime (or until `background.token`
    // cancels), reconnects on transient Postgres blips, self-dedupes by
    // `instance_id`. Performs NO write and NO committer interaction.
    if config.multi_instance.enabled && config.presence_enabled {
        background.spawn(notify::run_presence_listener(
            pool.clone(),
            presence.clone(),
            deps.instance_id.clone(),
        ));
    }
    // ENH-022 Stage 4c: forwarded-write LISTEN task. Only spawned when
    // `RTDB_MULTI_INSTANCE=true` — a single-instance deploy never pays
    // the third `PgListener` connection. Unlike the op-feed/presence
    // listeners this one DOES interact with the committers: it is how a
    // forwarded write reaches the owning replica's committer turn (the
    // owner is the single writer; this listener only injects INTO that
    // existing serialized turn, it never executes a write itself).
    // Runs for the process lifetime (or until `background.token` cancels),
    // reconnects on transient Postgres blips, self-dedupes by
    // `instance_id`, and drops requests for databases it does not own.
    if config.multi_instance.enabled
        && let Some(forwarder) = deps.forwarder.as_ref()
    {
        background.spawn(forward::run_forward_listener(
            pool.clone(),
            committers.clone(),
            forwarder.clone(),
            deps.instance_id.clone(),
            config.multi_instance.forward_concurrency,
        ));
        // ARC-002: reclaim spool rows nobody consumed. Retention is twice
        // the forward timeout — past that, no live request can still care.
        background.spawn(forward::run_forward_sweeper(
            pool.clone(),
            std::time::Duration::from_millis(
                config.multi_instance.forward_timeout_ms.saturating_mul(2),
            ),
        ));
    }
}

fn spawn_monitors(
    pool: &sqlx::PgPool,
    config: &Config,
    deps: &Dependencies,
    background: &BackgroundTasks,
) {
    // Prod degradation hardening: committer write-path watchdog. Detection
    // only — samples the committer queue gauges and flips the additive
    // /healthz degraded_reason field when queue-wait p99 stays at wedge
    // scale for 60s continuously (see `watchdog.rs`). Runs for the process
    // lifetime regardless of multi-instance mode; the single-writer
    // invariant is untouched (the watchdog only reads gauges).
    background.spawn(watchdog::run(deps.metrics.clone()));
    // Prod degradation hardening, postgres-side class (2026-11-16
    // evidence): episodic host I/O degradation stalls postgres and the
    // connection pool stalls SILENTLY — the episode windows logged zero
    // WARN/ERROR while every request hung, and the committer watchdog
    // (queue-gauge based) correctly stayed quiet because the committer
    // was never wedged. This monitor probes the pool on a 1s cadence
    // (bounded acquire + `SELECT 1`), WARN-logs every slow/failed probe,
    // and flips the additive /healthz degraded_reason "pool_stalled".
    // Detection only (see `pool_watch.rs`); the probe holds at most one
    // pool slot for at most 5s.
    background.spawn(pool_watch::run(pool.clone(), deps.metrics.clone()));
    // ENH-022 Stage 4: rate-counter sweep. In multi-instance mode the
    // limiter's counters live in `rtdb_auth.rate_counters` (one row per
    // key per minute); this task drops buckets older than the current
    // minute so the table stays bounded. Runs for the process lifetime
    // (or until `background.token` cancels); a failed sweep logs and
    // retries next tick.
    if config.multi_instance.enabled {
        background.spawn(rate_limit::run_counter_sweep(pool.clone()));
        // ARC-007: approximate limiter flush. Only when the limiter is
        // NOT running the exact per-request `check_pg` path — the exact
        // path never accumulates local deltas, so there is nothing to
        // flush. Runs for the process lifetime (or until
        // `background.token` cancels).
        if !config.limits.exact {
            background.spawn(rate_limit::run_approx_flush(
                deps.rate_limiter.clone(),
                Duration::from_millis(config.limits.sync_ms),
            ));
        }
    }
}

pub(super) fn build_auth() -> Auth {
    // ARC-114: one shared HTTP client for every OAuth provider's outbound
    // calls, so logins reuse a warm connection pool instead of building a
    // fresh client (and TLS handshake) per login. A build failure is non-fatal — it
    // falls back to `Client::new()` (the prior per-login behavior) so a
    // misconfigured system still boots.
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());
    Auth { http }
}

pub(super) fn build_runtime(deps: Dependencies) -> Runtime {
    Runtime {
        hot: deps.hot,
        metrics: deps.metrics,
        started_at: SystemTime::now(),
        backup_running: Arc::new(AtomicBool::new(false)),
        instance_id: deps.instance_id,
        schema_gate: deps.schema_gate,
        config_lock: tokio::sync::Mutex::new(()),
    }
}
