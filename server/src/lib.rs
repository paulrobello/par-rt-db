//! par-rt-db server crate — a self-hosted, Convex-inspired realtime document
//! database (axum/tokio + Postgres 17).
//!
//! Clients send a declarative JSON DSL — typed queries and atomic multi-step
//! transactions — over WebSocket (`/sync`) or one-shot HTTP; one generic server
//! hosts many named databases. This crate root wires the runtime together:
//! [`AppState`] (shared DB pool, hot config, caches, rate limiters), the axum
//! [`build_router`] mounting every route, and static-SPA hosting via
//! `RTDB_STATIC_DIR`. The committer (`committer`) is the correctness core; the
//! authoritative design lives in
//! `docs/superpowers/specs/2026-07-21-par-rt-db-design.md`.

// Production code answers every failure with the `RtDbError` envelope; a panic
// in a request path takes the whole server's tokio worker with it. Test code is
// exempt because a panic there IS the failure report.
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

pub mod admin;
pub mod audit;
pub mod auth;
pub mod backup;
pub mod committer;
pub mod config;
pub mod db;
pub mod ddl;
pub mod dsl;
pub mod error;
pub mod forward;
pub mod health;
pub mod http_api;
pub mod image_transform;
pub mod merge;
pub mod metrics;
pub mod migrate;
pub mod mutation_log;
pub mod notify;
pub mod op_feed;
pub mod pagination;
pub mod presence;
pub mod privacy;
pub mod protocol;
pub mod query;
pub mod quota;
pub mod rate_limit;
pub mod reaper;
pub mod scheduler;
pub mod schema;
pub mod schema_diff;
pub mod schema_history;
pub mod signed_url;
pub mod snapshot;
pub mod storage;
pub mod subs;
pub mod tracing_setup;
pub mod txn;
pub mod value_expr;
pub mod webhook;
pub mod workflows;
pub mod ws;

use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use arc_swap::ArcSwap;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header};
use axum::middleware::{Next, from_fn, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::{Router, routing::get};
use committer::Committers;
use config::{Config, HotConfig};
use db::SchemaCache;
use subs::SubscriptionManager;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tower::ServiceBuilder;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::services::{ServeDir, ServeFile};
use tower_http::trace::TraceLayer;

/// ARC-009: handle to every fire-and-forget task `AppState::new` spawns (the
/// idle reclaimer, presence flush, the `PgListener` loops, the rate-limit
/// sweep, and the forward listener/sweeper), plus the [`CancellationToken`]
/// that stops them. Nothing joined or cancelled these before this type
/// existed — a graceful shutdown in `main.rs` only stopped the axum server,
/// and each of the ~15 `test_state_*` helpers in `tests/common/mod.rs` leaked
/// a fresh set of listeners (and their Postgres connections) for the whole
/// test-binary lifetime.
///
/// Most of the spawned loops live in `notify.rs`/`forward.rs`/`rate_limit.rs`/
/// `presence.rs`/`committer/` and are not written to poll a token themselves,
/// so [`BackgroundTasks::spawn`] races each future against
/// `token.cancelled()` from the outside: cancellation drops the inner future
/// at its current await point (closing sockets/listeners via their own `Drop`
/// impls) without requiring the task body to cooperate. [`BackgroundTasks::track`]
/// is for a handle obtained elsewhere (`PresenceManager::run_flush_task`
/// already returns its own `JoinHandle`) — those rely on `abort` instead of a
/// wrapping `select!`.
pub struct BackgroundTasks {
    pub token: CancellationToken,
    handles: Mutex<Vec<JoinHandle<()>>>,
}

impl BackgroundTasks {
    fn new() -> Self {
        Self {
            token: CancellationToken::new(),
            handles: Mutex::new(Vec::new()),
        }
    }

    /// Spawn `fut`, racing it against `token.cancelled()`. Cancellation drops
    /// `fut` immediately rather than waiting for it to notice — the intended
    /// shape for the existing listener/sweep loops, none of which currently
    /// poll a token themselves.
    fn spawn<F>(&self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let token = self.token.clone();
        let handle = tokio::spawn(async move {
            tokio::select! {
                () = token.cancelled() => {}
                () = fut => {}
            }
        });
        self.push(handle);
    }

    /// Track a `JoinHandle` obtained elsewhere. Shutdown relies on `abort`
    /// for these rather than cooperative polling. `pub` so callers outside
    /// this crate that spawn their own task against `AppState` (the test
    /// harness's `spawn_app` tracks its detached server task this way) can
    /// still have it stopped by `cancel`/`shutdown`.
    pub fn track(&self, handle: JoinHandle<()>) {
        self.push(handle);
    }

    fn push(&self, handle: JoinHandle<()>) {
        self.handles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(handle);
    }

    /// Synchronous shutdown signal: cancel the token and abort every tracked
    /// handle. Safe to call from `Drop` (no `.await`) — this is what the test
    /// harness's wrapper type uses so a test's listeners die with the test
    /// even though `Drop` cannot await the join.
    pub fn cancel(&self) {
        self.token.cancel();
        for h in self
            .handles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
        {
            h.abort();
        }
    }

    /// `main.rs` graceful shutdown: `cancel()`, then wait up to `timeout` for
    /// every task to actually finish, logging what did or did not exit in
    /// time. Aborted tasks resolve quickly, so `timeout` only guards against
    /// one wedged in a non-cancellable await (e.g. a stuck Postgres call).
    pub async fn shutdown(&self, timeout: Duration) {
        self.cancel();
        let handles: Vec<_> = self
            .handles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
            .collect();
        let n = handles.len();
        if n == 0 {
            return;
        }
        match tokio::time::timeout(timeout, futures::future::join_all(handles)).await {
            Ok(_) => tracing::info!(count = n, "background tasks exited on shutdown"),
            Err(_) => tracing::warn!(
                count = n,
                timeout_ms = timeout.as_millis() as u64,
                "background tasks did not all exit within the shutdown timeout"
            ),
        }
    }
}

/// Realtime execution core: subscription state, the per-db committer tasks,
/// and the live op-feed tap they publish to. Grouped so handlers that only
/// need the reactive surface can reach for `state.realtime` as a unit.
///
/// **Op-feed is cross-instance under ENH-022 Stage 2** when
/// `RTDB_MULTI_INSTANCE=true`: durable writes emit one `pg_notify` per DocOp
/// and a per-process LISTEN task mirrors peer replicas' notifications into the
/// local ring. Self-notifications are deduped by `instance_id`, so the
/// single-publish contract still holds. Presence remains instance-local
/// (ARC-126, Stage 3): a second replica still sees neither the other's
/// presence sessions, so a browser's live roster reflects only its own
/// replica. Rate-limit counters (on `AppState`) remain instance-local too.
pub struct Realtime {
    pub subs: Arc<SubscriptionManager>,
    pub committers: Committers,
    pub op_feed: Arc<op_feed::OpFeed>,
    /// Transient in-memory presence (ENH-015). Sibling to the committer —
    /// presence does NOT route through the committer or any `handle_*` tap
    /// site; the flush task is spawned only when `presence_enabled`.
    pub presence: Arc<presence::PresenceManager>,
}

/// Runtime-wide mutable/process state: hot-reloaded config, metrics, the
/// server's boot timestamp, the manual-backup in-progress flag, and this
/// process's replica id. Grouped so the `/admin/*` and health surfaces can
/// reach for `state.runtime` as a unit.
pub struct Runtime {
    pub hot: Arc<ArcSwap<HotConfig>>,
    pub metrics: Arc<metrics::Metrics>,
    pub started_at: SystemTime,
    /// In-progress flag for the manual `/admin/backup` trigger. Set
    /// synchronously in the handler before the spawned `pg_dump` task and
    /// cleared on completion (success or failure). Read by `GET /admin/backups`
    /// to populate `running`, and checked-and-set by the trigger to enforce
    /// "one manual backup at a time" (409 on conflict). Atomic because the
    /// trigger handler, the spawned dump task, and the listing handler all
    /// touch it without serialization — the same way the cron backup task
    /// (which never touches this flag) runs alongside them.
    pub backup_running: Arc<AtomicBool>,
    /// This process's replica id (ENH-022 Stage 2). Tags NOTIFY payloads for
    /// cross-instance op-feed fan-out; auto-generated when `RTDB_INSTANCE_ID`
    /// is unset. Surfaced for diagnostics + tests.
    pub instance_id: String,
}

/// Per-instance auth bookkeeping that is neither a config value nor a realtime
/// concern. Grouped so `state.auth` is the only OAuth-session seam.
///
/// **Cross-replica safe (ENH-022 Stage 1):** the single-use OAuth `state`
/// token minted at `/auth/{provider}/begin` and consumed at `/auth/callback`
/// now lives in the `rtdb_auth.oauth_states` table, not an in-process map. A
/// callback load-balanced to a different replica finds the same row, so a
/// login begun on one replica completes on another. The op-feed and presence
/// maps remain instance-local (see `Realtime`) until later ENH-022 stages.
pub struct Auth {
    /// Shared HTTP client for all OAuth providers' outbound calls (token
    /// exchange, userinfo fetch, JWKS fetch). One client for the process keeps
    /// a warm connection pool + TLS session across logins instead of paying the
    /// handshake per login as `reqwest::Client::new()` did (ARC-114). The 10s
    /// timeout matches the in-tree convention Microsoft's provider already used
    /// via its former module-level `HTTP_CLIENT`; for the other providers it is
    /// a sensible upper bound where previously there was none (a hung exchange
    /// now fails at 10s instead of hanging indefinitely). Redirect policy is
    /// reqwest's default (follow up to 10), matching `Client::new()`.
    pub http: reqwest::Client,
}

/// Request-path limiters, caches, and derived keys, built once at boot: the
/// fixed-window rate limiter, the image-transform cache, the per-db
/// storage-usage cache, and the HMAC key for signed storage URLs. Grouped so
/// the HTTP surfaces that enforce caps and serve derived content can reach
/// for `state.limits` as a unit.
pub struct Limits {
    /// Instance-local (ARC-126): the fixed-window rate limiter is an
    /// in-process counter map. A second replica sees none of the first's
    /// requests, so a client's effective budget becomes `N × replicas` per
    /// window — a silent weakening of the cap. This server is single-instance
    /// by design (see the boot WARN in `main.rs`).
    pub rate_limiter: Arc<rate_limit::RateLimiter>,
    /// On-the-fly image transform cache (ENH-014). `Arc` because every storage
    /// serve request shares the one moka cache + concurrency semaphore.
    pub image: Arc<image_transform::TransformCache>,
    /// Per-db storage-usage cache (ENH-011). `Arc` — read on every growing
    /// write, refreshed lazily + eagerly.
    pub quotas: Arc<quota::UsageCache>,
    /// HMAC key for signing time-limited storage URLs (derived once at boot from
    /// `config.admin_key`). Shared by every request via `Arc`.
    pub signed_url_key: Arc<ring::hmac::Key>,
}

pub struct AppState {
    pub pool: sqlx::PgPool,
    pub config: Config,
    pub schemas: SchemaCache,
    pub realtime: Realtime,
    pub runtime: Runtime,
    pub auth: Auth,
    pub limits: Limits,
    /// ARC-009: cancellation + join handles for every task `AppState::new`
    /// spawns. `main.rs` drains this on graceful shutdown; tests cancel it
    /// via a `Drop` guard (`tests/common/mod.rs`'s `background_guard`) so
    /// listeners die with the test instead of leaking for the whole
    /// test-binary lifetime. `Arc`-wrapped so that guard can hold its own
    /// clone independent of the (often leaked — see `spawn_app`) `AppState`
    /// itself.
    pub background: Arc<BackgroundTasks>,
}

impl AppState {
    pub fn new(pool: sqlx::PgPool, config: Config, hot: HotConfig) -> Arc<Self> {
        let schemas = SchemaCache::with_capacity(config.schema_cache_max_entries);
        let metrics = metrics::Metrics::with_slow_query_capacity(config.slow_query_capacity);
        // The subscription manager records invalidation effectiveness on the
        // same `Metrics` the dashboard reads, and owns the skip-verification
        // sampler — so it is built after metrics and before the committers.
        let subs = SubscriptionManager::with_instrumentation(
            Some(metrics.clone()),
            config.subs_verify_skip_every,
        );
        let op_feed = op_feed::OpFeed::new(1024, 500);
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
        // Read before `config` is destructured into the AppState literal below
        // (and before `pool` moves into it).
        let multi_instance = config.multi_instance.enabled;
        let limiter_pool = pool.clone();
        let rate_limit_exact = config.limits.exact;
        let rate_limit_sync_ms = config.limits.sync_ms;
        // ENH-022 Stage 4 / ARC-007: in multi-instance mode the counters live
        // in Postgres so every replica shares one budget per token/db/ip;
        // single-instance (the default) keeps the in-memory limiter and
        // never touches the table. Built here (not inline in the `Limits`
        // literal below) so the approximate flush task spawned further down
        // can hold its own `Arc` clone.
        let rate_limiter = if multi_instance {
            rate_limit::RateLimiter::new_pg(limiter_pool, rate_limit_exact)
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
        let committers = Committers::new(
            pool.clone(),
            subs.clone(),
            schemas.clone(),
            op_feed.clone(),
            hot.clone(),
            metrics.clone(),
            committer::CommitterConfig::from_config(
                &config,
                quotas.clone(),
                instance_id.clone(),
                forwarder.clone(),
            ),
        );
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
        // Image transform cache shares the same `Arc<Metrics>` as Runtime and
        // the committers so its hit/miss/error counters surface on the dashboard.
        // Built before the struct literal so `metrics` can still move into Runtime.
        let signed_url_key = Arc::new(signed_url::derive_key(&config.admin_key));
        let image = Arc::new(image_transform::TransformCache::new(
            image_transform::TransformConfig::from_config(&config),
            config.storage.image.cache_bytes,
            config.storage.image.concurrency,
            metrics.clone(),
        ));
        // Presence manager (ENH-015) shares the same `Arc<Metrics>` so its
        // update/broadcast counters surface on the dashboard. It is a sibling
        // of the committer, NOT routed through it: the periodic flush task is
        // spawned only when `presence_enabled` (default on) — a disabled
        // server still has the manager (so `presenceErr` works) but no
        // spinning task. Built before the struct literal so `metrics` can
        // still move into Runtime.
        //
        // ENH-022 Stage 3: in multi-instance mode the manager carries the
        // instance id + pool so it can `pg_notify` per-room snapshots to peer
        // replicas, and the flush task also fires a liveness beat every
        // `RTDB_PRESENCE_BEAT_INTERVAL_MS` (default 5s). Single-instance mode
        // passes `multi_instance=false, pool=None` — the manager never touches
        // NOTIFY, and every wire byte is unchanged.
        let presence_cfg = presence::PresenceConfig::from_config(&config);
        let presence = presence::PresenceManager::new(
            Some(metrics.clone()),
            presence_cfg,
            config.multi_instance.enabled,
            instance_id.clone(),
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
        // ENH-022 Stage 2: cross-instance op-feed LISTEN task. Only spawned when
        // `RTDB_MULTI_INSTANCE=true` — a single-instance deploy never pays the
        // `PgListener` connection. Runs for the process lifetime (or until
        // `background.token` cancels), reconnects on transient Postgres blips,
        // and self-dedupe is handled inside it via `instance_id`.
        if config.multi_instance.enabled {
            background.spawn(notify::run_listener(
                pool.clone(),
                op_feed.clone(),
                instance_id.clone(),
            ));
            // ARC-001: cross-replica subscription invalidation. Same shape as
            // the op-feed listener — reads only, no committer interaction, so
            // the single-writer invariant is untouched.
            background.spawn(notify::run_write_set_listener(
                pool.clone(),
                subs.clone(),
                schemas.clone(),
                instance_id.clone(),
            ));
        }
        // ENH-022 Stage 3: cross-instance presence LISTEN task. Only spawned
        // when BOTH `RTDB_MULTI_INSTANCE=true` AND `RTDB_PRESENCE_ENABLED=true`
        // — a single-instance deploy never pays the second `PgListener`
        // connection. Runs for the process lifetime (or until `background.token`
        // cancels), reconnects on transient Postgres blips, self-dedupes by
        // `instance_id`. Performs NO write and NO committer interaction.
        if config.multi_instance.enabled && presence_cfg.enabled {
            background.spawn(notify::run_presence_listener(
                pool.clone(),
                presence.clone(),
                instance_id.clone(),
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
            && let Some(forwarder) = forwarder.as_ref()
        {
            background.spawn(forward::run_forward_listener(
                pool.clone(),
                committers.clone(),
                forwarder.clone(),
                instance_id.clone(),
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
            if !rate_limit_exact {
                background.spawn(rate_limit::run_approx_flush(
                    rate_limiter.clone(),
                    Duration::from_millis(rate_limit_sync_ms),
                ));
            }
        }
        // ARC-114: one shared HTTP client for every OAuth provider's outbound
        // calls, so logins reuse a warm connection pool instead of building a
        // fresh client (and TLS handshake) per login. Built before the struct
        // literal so it can move into `Auth`. A build failure is non-fatal — it
        // falls back to `Client::new()` (the prior per-login behavior) so a
        // misconfigured system still boots.
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Arc::new(Self {
            pool,
            config,
            schemas,
            realtime: Realtime {
                subs,
                committers,
                op_feed,
                presence,
            },
            runtime: Runtime {
                hot,
                metrics,
                started_at: SystemTime::now(),
                backup_running: Arc::new(AtomicBool::new(false)),
                instance_id,
            },
            auth: Auth { http },
            limits: Limits {
                rate_limiter,
                image,
                quotas,
                signed_url_key,
            },
            background,
        })
    }
}

/// Origin allowlist check for the WebSocket upgrade handlers (SEC-105). CORS
/// does not apply to the WS handshake, so a cookie-authenticated `/sync` or
/// `/admin/stream` upgrade would otherwise admit any same-site origin (and the
/// `SameSite=Lax` session cookie is scoped to the registrable domain, so every
/// `*.example.com` host shares it). Browsers always send `Origin` on a WS
/// handshake; absent Origin = non-browser client (CLI/SDK/machine token), and
/// the existing auth gates — the post-upgrade `Auth` frame on `/sync`, the
/// `Authorization` header or `Sec-WebSocket-Protocol: rtdb-admin.<token>`
/// subprotocol on `/admin/stream` — still validate the credential. A present
/// `Origin` must therefore match `hot.allowed_origins` or `public_url`; an
/// absent `Origin` is admitted.
pub(crate) fn origin_allowed(
    headers: &HeaderMap,
    hot: &Arc<ArcSwap<HotConfig>>,
    public_url: &str,
) -> bool {
    let Some(origin) = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()) else {
        return true;
    };
    if origin == public_url {
        return true;
    }
    hot.load()
        .allowed_origins
        .iter()
        .any(|allowed| allowed.as_str() == origin)
}

/// Origins are decided per request from live `HotConfig`, so `PATCH /admin/config`
/// can add an origin and have it take effect without a restart. The layer itself
/// is still constructed once at router build time; only the origin decision is
/// dynamic. WS is CORS-exempt (Origin is enforced at OAuth start, unchanged).
fn cors_layer(hot: Arc<ArcSwap<HotConfig>>) -> CorsLayer {
    CorsLayer::new()
        .allow_origin(AllowOrigin::predicate(move |origin, _parts| {
            let h = hot.load();
            match origin.to_str() {
                Ok(val) => h
                    .allowed_origins
                    .iter()
                    .any(|allowed| allowed.as_str() == val),
                Err(_) => false,
            }
        }))
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PATCH,
            Method::DELETE,
            Method::OPTIONS,
        ])
        // ARC-013: `X-Rtdb-Protocol` rides every SDK HTTP call, so a
        // cross-origin browser caller's preflight lists it in
        // `Access-Control-Request-Headers` — omitting it here fails the
        // preflight and blocks the request before it reaches a handler.
        .allow_headers([
            header::AUTHORIZATION,
            header::CONTENT_TYPE,
            HeaderName::from_static(http_api::PROTOCOL_HEADER),
        ])
        .allow_credentials(true)
}

/// Sets `Cache-Control` on static responses from their Content-Type: the SPA
/// shell (text/html — including the index served for unknown paths by the SPA
/// fallback) is `no-cache` so a new deploy's index.html is always fetched (and
/// then references the newest hashed assets); every other static asset is
/// `immutable`. Wraps only the static `ServeDir`, never the API/admin/WS/auth
/// routes.
async fn set_static_cache_headers(req: Request, next: Next) -> Response {
    let mut resp = next.run(req).await;
    let no_cache = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("text/html"));
    let cc = if no_cache {
        "no-cache, no-store, must-revalidate"
    } else {
        "public, max-age=31536000, immutable"
    };
    if let Ok(value) = HeaderValue::from_str(cc) {
        resp.headers_mut().insert(header::CACHE_CONTROL, value);
    }
    resp
}

/// Router-wide security headers (SEC-111): applied to every response so the
/// admin SPA and the API surface both carry baseline defenses. Sets
/// `X-Content-Type-Options`, `Referrer-Policy`, and `X-Frame-Options`
/// unconditionally; `Content-Security-Policy` only on HTML responses (the
/// dashboard bundle is same-origin and self-contained, so `'self'` holds); and
/// `Strict-Transport-Security` only when the request arrived over HTTPS per
/// `auth::cookie::request_is_secure` — the Cloudflare tunnel sets
/// `X-Forwarded-Proto: https`, and SEC-201 gates that read on
/// `RTDB_TRUSTED_PROXY` so a spoofed header cannot inject HSTS on a
/// directly-reachable deploy (unconditional HSTS would also break plain-http
/// local dev).
///
/// A response that already carries a `Content-Security-Policy` header (the
/// OAuth callback page at `auth/provider.rs` sets a stricter per-page one) is
/// left alone — one source of truth per route, never overwrite an existing
/// policy.
async fn security_headers(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Response {
    let https = auth::cookie::request_is_secure(req.headers(), state.config.trusted_proxy);
    let mut resp = next.run(req).await;
    let headers = resp.headers_mut();
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("strict-origin-when-cross-origin"),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    // CSP is scoped to HTML (the SPA shell) so JSON/protobuf/Prometheus
    // responses stay unaffected. Skip when the response already carries one —
    // the OAuth callback's stricter per-page policy wins there.
    let is_html = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("text/html"));
    if is_html && !headers.contains_key("content-security-policy") {
        headers.insert(
            header::CONTENT_SECURITY_POLICY,
            HeaderValue::from_static(
                "default-src 'self'; frame-ancestors 'none'; object-src 'none'; base-uri 'self'",
            ),
        );
    }
    if https {
        headers.insert(
            header::STRICT_TRANSPORT_SECURITY,
            HeaderValue::from_static("max-age=31536000; includeSubDomains"),
        );
    }
    resp
}

/// SEC-129: best-effort admin check for the build fingerprint fields on the
/// otherwise-unauthenticated `/metrics` and `/healthz` routes. Returns the
/// `(version, git_commit)` tuple only when an admin bearer is present and
/// validates; returns `None` on absence or any auth failure so liveness is
/// never gated. The exact build version and commit sha are admin-only; the
/// unauthenticated response keeps aggregate metrics / status only.
async fn admin_fingerprint(
    state: &AppState,
    headers: &HeaderMap,
) -> Option<(&'static str, &'static str)> {
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))?;
    crate::admin::authenticate_admin(state, token)
        .await
        .ok()
        .map(|_| (env!("CARGO_PKG_VERSION"), env!("BUILD_GIT_COMMIT")))
}

/// `GET /metrics`: unauthenticated Prometheus text-exposition scrape endpoint.
///
/// Content-negotiates on `Accept` so the operator dashboard's browser route at
/// `/metrics` (SPA) still works: a browser (`Accept: text/html,...`) is served
/// the SPA's `index.html` when `RTDB_STATIC_DIR` is configured; everything else
/// (Prometheus's scraper sends `application/openmetrics-text;...,text/plain;...`,
/// curl, API-only deploys) gets Prometheus text. Safe to expose without auth —
/// `MetricsSnapshot` is aggregate-only (no per-db, no principal data), same
/// posture as `/healthz`. The existing admin JSON snapshot stays at
/// `/admin/metrics` (admin-gated); this endpoint does not replace it.
async fn prometheus_metrics_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let wants_html = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|a| a.contains("text/html"));

    if wants_html
        && let Some(dir) = state.config.static_dir.as_deref()
        && let Ok(index) = tokio::fs::read_to_string(format!("{dir}/index.html")).await
    {
        let mut h = HeaderMap::new();
        h.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/html; charset=utf-8"),
        );
        h.insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("no-cache, no-store, must-revalidate"),
        );
        return (StatusCode::OK, h, index).into_response();
    }

    let (presence_rooms, presence_sessions) = state.realtime.presence.counts().await;
    let snap = state
        .runtime
        .metrics
        .snapshot(
            &state.pool,
            &state.realtime.subs,
            state.runtime.started_at,
            presence_rooms,
            presence_sessions,
        )
        .await;
    let body = metrics::render_prometheus(&snap, admin_fingerprint(&state, &headers).await);
    let mut h = HeaderMap::new();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    h.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-store, must-revalidate"),
    );
    (StatusCode::OK, h, body).into_response()
}

pub fn build_router(state: Arc<AppState>) -> Router {
    let cors = cors_layer(state.runtime.hot.clone());

    let mut router = Router::new()
        .route("/healthz", get(health::handler))
        .route("/privacy", get(privacy::handler))
        .route("/metrics", get(prometheus_metrics_handler))
        .merge(admin::admin_routes(state.clone()))
        .merge(http_api::http_api_routes())
        .merge(ws::ws_routes())
        .merge(auth::provider::auth_routes());

    // Static SPA hosting, mounted LAST as the fallback so it can never shadow a
    // real route. Only when RTDB_STATIC_DIR is set and the directory exists;
    // otherwise the server is API-only (today's behavior). `static_dir` is
    // cloned out so `state` can still move into `with_state` below.
    if let Some(dir) = state.config.static_dir.clone()
        && Path::new(&dir).is_dir()
    {
        let serve_dir =
            ServeDir::new(dir.clone()).fallback(ServeFile::new(format!("{dir}/index.html")));
        router = router.fallback_service(
            ServiceBuilder::new()
                .layer(from_fn(set_static_cache_headers))
                .service(serve_dir),
        );
    }

    router
        .layer(from_fn_with_state(state.clone(), security_headers))
        .layer(
            // SEC-121: the default `TraceLayer::new_for_http()` records the full
            // request URI (including query string) in the span. The OAuth `state`
            // and provider `code` transit `/auth/callback?…&state=…` and
            // `/auth/state?state=…` — those would otherwise land in this server's
            // trace logs and any OTel collector downstream. Override `make_span_with`
            // to record the PATH ONLY for those two routes (enough for correlation);
            // all other paths keep the full URI (default behavior).
            TraceLayer::new_for_http().make_span_with(|request: &Request<_>| {
                tracing::debug_span!(
                    "http.request",
                    method = %request.method(),
                    uri = %redacted_request_uri(request.uri())
                )
            }),
        )
        .layer(cors)
        .with_state(state)
}

/// SEC-121: true for paths whose query string carries OAuth secrets (`state`,
/// `code`). Covers the generic callback (`/auth/callback`), every provider-
/// scoped callback (`/auth/{provider}/callback`), and the state poll
/// (`/auth/state`). For these, the TraceLayer records the PATH ONLY — never the
/// query string. All other paths record the full URI (the default behavior).
fn trace_should_redact_query(path: &str) -> bool {
    path == "/auth/state"
        || path == "/auth/callback"
        || (path.ends_with("/callback") && path.starts_with("/auth/"))
}

/// SEC-121: returns the URI string to record in the HTTP trace span. For paths
/// flagged by [`trace_should_redact_query`], the query string is stripped (the
/// `state` and `code` params are secrets-in-URL). For everything else, the full
/// URI is returned verbatim. Extracted as a pure function so it is unit-testable.
fn redacted_request_uri(uri: &axum::http::Uri) -> std::borrow::Cow<'_, str> {
    let path = uri.path();
    if trace_should_redact_query(path) {
        path.into()
    } else {
        uri.to_string().into()
    }
}

#[cfg(test)]
mod lib_tests {
    use super::*;
    use axum::http::Uri;

    #[test]
    fn redacts_query_on_oauth_callback_and_state_paths() {
        // SEC-121: /auth/callback?code=…&state=SECRET must record the path only.
        let uri: Uri = "/auth/callback?code=abc&state=SECRET".parse().unwrap();
        assert_eq!(redacted_request_uri(&uri), "/auth/callback");

        // /auth/state?state=SECRET likewise — path only.
        let uri: Uri = "/auth/state?state=SECRET".parse().unwrap();
        assert_eq!(redacted_request_uri(&uri), "/auth/state");

        // Provider-scoped callbacks (/auth/github/callback, /auth/google/callback,
        // etc.) also match the /auth/callback prefix.
        let uri: Uri = "/auth/github/callback?code=abc&state=SECRET"
            .parse()
            .unwrap();
        assert_eq!(redacted_request_uri(&uri), "/auth/github/callback");
    }

    #[test]
    fn preserves_full_uri_on_non_sensitive_paths() {
        // Non-sensitive paths keep the full URI (query string included) — the
        // default behavior, so storage transforms (?w=&h=&format=) and admin
        // query params (?db=&limit=) remain visible for correlation.
        let uri: Uri = "/storage/abc?w=100&h=100&format=jpeg".parse().unwrap();
        assert_eq!(
            redacted_request_uri(&uri),
            "/storage/abc?w=100&h=100&format=jpeg"
        );

        let uri: Uri = "/admin/audit?db=mydb&limit=50".parse().unwrap();
        assert_eq!(redacted_request_uri(&uri), "/admin/audit?db=mydb&limit=50");
    }

    #[test]
    fn redacts_even_when_query_is_absent_on_sensitive_paths() {
        // A sensitive path with no query still records just the path.
        let uri: Uri = "/auth/callback".parse().unwrap();
        assert_eq!(redacted_request_uri(&uri), "/auth/callback");
    }
}
