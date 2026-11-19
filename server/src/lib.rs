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

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod admin;
pub mod audit;
pub mod auth;
pub mod backup;
pub mod bulk;
pub mod change_log;
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
pub mod openapi;
pub mod pagination;
pub mod pool_watch;
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
pub mod schema_gate;
pub mod schema_history;
pub mod shutdown;
pub mod signed_url;
pub mod snapshot;
pub mod spec_corpus;
pub mod spec_corpus_cursor_encoding;
pub mod spec_corpus_patch_semantics;
mod state;
pub mod storage;
pub mod subs;
pub mod tracing_setup;
pub mod txn;
mod util;
pub mod value_expr;
pub mod watchdog;
pub mod webhook;
pub mod workflows;
pub mod ws;
pub mod ws_feed;

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
use tower_http::compression::{
    CompressionLayer,
    predicate::{DefaultPredicate, Predicate, SizeAbove},
};
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
/// so `BackgroundTasks::spawn` races each future against
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
    /// Schema-DDL admission gate (2026-11-05 incident): caps concurrent
    /// schema-DDL passes so lock waits cannot exhaust the query pool, and
    /// serializes them against `pg_dump` backups in both directions. Shared
    /// with the per-db committers (via `CommitterConfig`), the managed backup
    /// task, and the manual `/admin/backup` trigger. See `schema_gate`.
    pub schema_gate: Arc<schema_gate::SchemaGate>,
    /// ARC-201: serializes `PATCH /admin/config` load-apply-save turns so
    /// concurrent patches cannot last-store-win within this process (ARC-203).
    /// The patch path re-reads the authoritative `rtdb_config` row while
    /// holding it; cross-replica convergence rides the `rtdb_config` NOTIFY
    /// plus `notify::run_config_listener`. Two replicas patching in the same
    /// instant is a documented residual — each patch reloads first, so a
    /// sequenced patch chain never reverts an earlier one.
    pub config_lock: tokio::sync::Mutex<()>,
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
    /// Instance-local (ARC-126): an in-process counter map in the default
    /// single-instance mode — a second replica would see none of the first's
    /// requests, so a client's effective budget becomes `N × replicas` per
    /// window, silently weakening the cap. Under `RTDB_MULTI_INSTANCE` the
    /// constructor swaps in the Postgres-backed `RateLimiter::new_pg` (counters
    /// in `rtdb_auth.rate_counters`, one shared budget per token/db/ip — see
    /// the construction in `AppState::new`).
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

/// OpFeed sizing, boot-pinned with no config surface (ARC-007). The ring
/// replays the last [`OP_FEED_RING_CAP`] events on (re)connect — the value
/// ARCHITECTURE.md's "Op-feed ring" note documents as "the last 500 events" —
/// and [`OP_FEED_BROADCAST_CAP`] bounds each lagging receiver's broadcast
/// buffer. Update the docs in the same change as these consts, or the
/// documented numbers drift.
const OP_FEED_BROADCAST_CAP: usize = 1024;
const OP_FEED_RING_CAP: usize = 500;

/// Server-wide shared state: the Postgres pool, boot config, schema cache, and
/// the grouped realtime/runtime/auth/limits handles, built once by
/// [`AppState::new`].
pub struct AppState {
    pub pool: sqlx::PgPool,
    pub config: Config,
    pub schemas: SchemaCache,
    pub realtime: Realtime,
    pub runtime: Runtime,
    pub auth: Auth,
    pub limits: Limits,
    /// ARC-005 background bulk-op registry (in-memory, per-instance — see
    /// `bulk`'s module docs for the restart semantics).
    pub bulk: Arc<bulk::BulkRegistry>,
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
    /// Builds the shared [`AppState`] — caches, rate limiter, op feed,
    /// committers — and spawns every background task it owns; returns it
    /// `Arc`-wrapped for sharing across the HTTP and WS surfaces.
    pub fn new(pool: sqlx::PgPool, config: Config, hot: HotConfig) -> Arc<Self> {
        let schemas = SchemaCache::with_capacity(config.schema_cache_max_entries);
        let dependencies = state::build_dependencies(&pool, &config, hot);
        let committers = state::build_committers(&pool, &config, &schemas, &dependencies);
        let background = state::build_background(&committers);
        let limits = state::build_limits(&config, &dependencies);
        let realtime = state::build_realtime(
            &pool,
            &config,
            &schemas,
            &dependencies,
            committers,
            &background,
        );
        let auth = state::build_auth();
        let bulk = Arc::new(bulk::BulkRegistry::new());
        let runtime = state::build_runtime(dependencies);
        Arc::new(Self {
            pool,
            config,
            schemas,
            bulk,
            realtime,
            runtime,
            auth,
            limits,
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
/// OAuth callback page at `auth/provider/callback.rs` sets a stricter per-page one) is
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
/// curl, API-only deploys) gets Prometheus text. SEC-205: the Prometheus text
/// carries `rtdb_build_info`, which fingerprints the exact build, so the text
/// branch is admin-gated; the HTML branch stays public — it is the SPA fallback
/// and serves only static content. The admin JSON snapshot remains at
/// `/admin/metrics`.
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

    // SEC-205: the Prometheus export carries `rtdb_build_info` (exact build
    // identity), so it is gated like the admin JSON snapshot at /admin/metrics.
    // The HTML branch above stays public: it is the SPA fallback.
    if let Err(e) = crate::admin::require_admin(&state, &headers).await {
        return e.into_response();
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
    let body = metrics::render_prometheus(
        &snap,
        &state.runtime.metrics,
        admin_fingerprint(&state, &headers).await,
        &state.runtime.instance_id,
    );
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

/// Assembles the complete axum [`Router`] — health/ops, client HTTP+WS, admin,
/// storage, and the SPA static fallback — onto `state`.
pub fn build_router(state: Arc<AppState>) -> Router {
    let cors = cors_layer(state.runtime.hot.clone());

    let mut router = Router::new()
        .route("/healthz", get(health::handler))
        .route("/privacy", get(privacy::handler))
        .route("/metrics", get(prometheus_metrics_handler))
        // OPENAPI: machine-readable description of the HTTP surface; unauthenticated
        // (shapes, not secrets) and present even when RTDB_STATIC_DIR is unset.
        .route("/openapi.json", get(openapi::handler))
        .merge(admin::admin_routes(state.clone()))
        .merge(http_api::http_api_routes(state.clone()))
        // Gzip only on the JSON API + admin surface (including the streaming
        // /admin/export-db JSONL and /admin/backups downloads): SizeAbove(1 KiB)
        // leaves tiny payloads uncompressed, and DefaultPredicate already skips
        // SSE. Applied before the /sync merge so the WS upgrade is untouched.
        .layer(
            CompressionLayer::new()
                .compress_when(DefaultPredicate::new().and(SizeAbove::new(1024))),
        )
        .merge(ws::ws_routes())
        .merge(auth::provider::auth_routes())
        // Blob byte-serving merges after the gzip layer: range requests and
        // the Accept-Ranges/Content-Length contract must survive content
        // negotiation, and blob bodies are usually already-compressed media.
        .merge(http_api::storage_serve_routes(state.clone()));

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
/// scoped callback (`/auth/{provider}/callback`), the state poll
/// (`/auth/state`), and the CLI login bootstrap (`/auth/login`). For these,
/// the TraceLayer records the PATH ONLY — never the query string. All other
/// paths record the full URI (the default behavior).
fn trace_should_redact_query(path: &str) -> bool {
    path == "/auth/state"
        || path == "/auth/callback"
        || path == "/auth/login"
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

        // /auth/login?state=SECRET (the CLI bootstrap) — path only.
        let uri: Uri = "/auth/login?state=SECRET".parse().unwrap();
        assert_eq!(redacted_request_uri(&uri), "/auth/login");
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
