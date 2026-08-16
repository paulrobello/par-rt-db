//! Admin observability routes: metrics snapshot, op-feed ring, durable audit
//! log, live-subscription inspector, and the `/admin/stream` WebSocket that
//! replays + streams filtered op events and periodic gauges.

use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::extract::{FromRequest, Path, Query as QueryParams, Request, State};
use axum::http::HeaderMap;
use axum::response::Response;
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::error::RtDbError;

use super::{authenticate_admin, bearer_from_subprotocol, bearer_value};

pub(super) async fn metrics_handler(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
) -> Result<Json<crate::metrics::MetricsSnapshot>, RtDbError> {
    let (presence_rooms, presence_sessions) = state.realtime.presence.counts().await;
    let mut snap = state
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
    // Per-db workflow status counts are a live side-table read (one GROUP BY
    // per database), populated here rather than inside `snapshot` so the
    // periodic `/admin/stream` gauge ticks don't fan it out every second.
    snap.per_db_workflows = crate::metrics::per_db_workflows_rows(&state.pool).await;
    Ok(Json(snap))
}

#[derive(Deserialize)]
pub(super) struct OpsRecentParams {
    db: Option<String>,
    table: Option<String>,
    #[serde(default = "default_ops_n")]
    n: usize,
}
fn default_ops_n() -> usize {
    100
}

#[derive(Deserialize)]
pub(super) struct AuditParams {
    db: Option<String>,
    #[serde(default)]
    table: Option<String>,
    #[serde(default)]
    op: Option<String>,
    #[serde(default)]
    principal: Option<String>,
    #[serde(default)]
    source: Option<String>,
    #[serde(default = "default_audit_limit")]
    limit: i64,
    #[serde(default = "default_audit_offset")]
    offset: i64,
}
fn default_audit_limit() -> i64 {
    100
}
fn default_audit_offset() -> i64 {
    0
}

#[derive(Serialize)]
pub(super) struct AuditResponse {
    entries: Vec<crate::audit::AuditEntry>,
}

/// `GET /admin/audit?db=<optional>&table=<optional>&op=<optional>&principal=<optional>&source=<optional>&limit=<n>&offset=<m>`
/// — durable audit log, newest-first. `table`/`op`/`principal`/`source` are
/// optional equality filters that combine with AND; an absent filter matches
/// all rows. `limit` defaults to 100 and is capped at 1000; `offset` defaults
/// to 0. When audit is disabled at boot (`!config.audit_log_enabled`) this
/// short-circuits to an empty list — the `rtdb.audit_log` table may not exist,
/// and an operator who turned audit off should not see stale rows from a
/// previous enabled run either.
pub(super) async fn audit_recent(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    QueryParams(params): QueryParams<AuditParams>,
) -> Result<Json<AuditResponse>, RtDbError> {
    if !state.config.audit_log_enabled {
        return Ok(Json(AuditResponse {
            entries: Vec::new(),
        }));
    }
    // Clamp to [1, 1000]; negative limits/offsets are nonsensical but
    // otherwise accepted by Postgres, so guard at the API edge.
    let limit = params.limit.clamp(1, 1000);
    let offset = params.offset.max(0);
    let entries = crate::audit::fetch_audit_rows(
        &state.pool,
        params.db.as_deref(),
        params.table.as_deref(),
        params.op.as_deref(),
        params.principal.as_deref(),
        params.source.as_deref(),
        limit,
        offset,
    )
    .await?;
    Ok(Json(AuditResponse { entries }))
}

#[derive(Deserialize)]
pub(super) struct SubscriptionsParams {
    #[serde(default)]
    db: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SubscriptionsResponse {
    subscriptions: Vec<crate::subs::SubSnapshot>,
    /// Global subscription-invalidation counters (the same totals exposed on
    /// `/admin/metrics`), repeated here so an operator inspecting live queries
    /// sees the invalidation behavior in one place.
    subs_reruns_total: u64,
    subs_skips_point_total: u64,
    subs_skips_indexed_total: u64,
    subs_skips_ordered_total: u64,
    subs_missed_pushes_total: u64,
    /// Per-db breakdown of those counters (ENH-010); sorted by db.
    per_db: Vec<crate::metrics::DbSubCounterRow>,
}

/// `GET /admin/subscriptions?db=<optional>` — the live subscription inspector
/// (ENH-010). Snapshots the registry (read-only; same lock discipline as the
/// `active_subscriptions` gauge) so an operator can see WHAT is subscribed per
/// database — table, terminal, read-set class, principal — alongside the
/// per-db skip/re-run/missed counters that explain invalidation behavior. Does
/// not execute txns and does not touch the single-writer invariant.
pub(super) async fn list_subscriptions(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    QueryParams(params): QueryParams<SubscriptionsParams>,
) -> Result<Json<SubscriptionsResponse>, RtDbError> {
    // Reuse the metrics snapshot for the global + per-db counters (the inspector
    // is a low-frequency admin poll, so the extra pool/latency reads are
    // negligible) and the registry snapshot for the per-subscription rows.
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
    let subscriptions = state.realtime.subs.snapshot(params.db.as_deref()).await;
    Ok(Json(SubscriptionsResponse {
        subscriptions,
        subs_reruns_total: snap.subs_reruns_total,
        subs_skips_point_total: snap.subs_skips_point_total,
        subs_skips_indexed_total: snap.subs_skips_indexed_total,
        subs_skips_ordered_total: snap.subs_skips_ordered_total,
        subs_missed_pushes_total: snap.subs_missed_pushes_total,
        per_db: snap.per_db_subs,
    }))
}

#[derive(Serialize)]
pub(super) struct OpsRecentResponse {
    ops: Vec<crate::op_feed::OpEvent>,
}

/// Recent document-op events from the in-memory ring, filtered by optional
/// `db`/`table`, newest-first, capped at `n` (max 500).
pub(super) async fn ops_recent(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    QueryParams(params): QueryParams<OpsRecentParams>,
) -> Result<Json<OpsRecentResponse>, RtDbError> {
    let ops = state
        .realtime
        .op_feed
        .recent(
            params.db.as_deref(),
            params.table.as_deref(),
            params.n.min(500),
        )
        .await;
    Ok(Json(OpsRecentResponse { ops }))
}

#[derive(Deserialize)]
pub(super) struct StreamParams {
    db: Option<String>,
    table: Option<String>,
}

/// `/admin/stream` WebSocket: admin-gated at the HTTP upgrade (a missing/invalid
/// bearer is rejected before WS negotiation), then replays the filtered ring and
/// streams live op events plus a ~1s gauge snapshot. `db`/`table` filter both the
/// replay and the live broadcast.
///
/// The gate runs BEFORE `WebSocketUpgrade` is extracted from the request, so a
/// missing bearer on a plain GET (or a real upgrade attempt) yields 401/403 and
/// never reaches WS negotiation; the WS extractor is invoked by hand after the
/// gate clears. The bearer is taken from the `Authorization` header when present
/// (CLI/automation), otherwise from the `Sec-WebSocket-Protocol: rtdb-admin.<token>`
/// subprotocol — browsers cannot set request headers on a WS handshake, so the
/// dashboard authenticates through that subprotocol instead.
pub(super) async fn admin_stream(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    QueryParams(params): QueryParams<StreamParams>,
    req: Request,
) -> Result<Response, RtDbError> {
    // SEC-105: WS handshakes are CORS-exempt — reject a cookie-authenticated
    // upgrade from a disallowed Origin before WS negotiation begins. Browsers
    // always send `Origin`; absent Origin = non-browser (CLI/automation) and
    // the bearer/subprotocol gate still authenticates.
    if !crate::origin_allowed(&_headers, &state.runtime.hot, &state.config.public_url) {
        return Err(RtDbError::forbidden("websocket origin not allowed"));
    }
    // Bearer from the Authorization header (CLI/automation) or, failing that,
    // the `rtdb-admin.<token>` subprotocol (browser dashboard — browsers can't
    // set headers on a WS handshake). When the subprotocol path is used we echo
    // it back: a client that offered a subprotocol requires the server to
    // negotiate one (tokio-tungstenite errors otherwise; browsers are lenient
    // but the echo is the spec-correct 101 response).
    let (token, offered_subprotocol) = match bearer_value(&_headers) {
        Ok(t) => (t, None),
        Err(_) => {
            let t = bearer_from_subprotocol(&_headers)?;
            (t, Some(format!("rtdb-admin.{t}")))
        }
    };
    let _ = authenticate_admin(&state, token).await?;
    let mut ws = WebSocketUpgrade::from_request(req, &state)
        .await
        .map_err(|_| RtDbError::bad_request("expected websocket upgrade request"))?;
    if let Some(proto) = offered_subprotocol {
        ws = ws.protocols([proto]);
    }
    Ok(ws.on_upgrade(move |socket| run_admin_stream(socket, state, params.db, params.table)))
}

async fn run_admin_stream(
    mut socket: WebSocket,
    state: Arc<AppState>,
    db: Option<String>,
    table: Option<String>,
) {
    for ev in state
        .realtime
        .op_feed
        .recent(db.as_deref(), table.as_deref(), 200)
        .await
    {
        if send_stream_json(&mut socket, &serde_json::json!({"kind":"op","event":ev}))
            .await
            .is_err()
        {
            return;
        }
    }
    let mut rx = state.realtime.op_feed.subscribe();
    let mut gauge_tick = tokio::time::interval(Duration::from_secs(1));
    gauge_tick.tick().await; // skip immediate
    loop {
        tokio::select! {
            ev = rx.recv() => {
                let Ok(ev) = ev else { break };
                if db.as_deref().is_none_or(|d| ev.db == d)
                    && table.as_deref().is_none_or(|t| ev.table == t)
                    && send_stream_json(&mut socket, &serde_json::json!({"kind":"op","event":ev}))
                        .await
                        .is_err()
                {
                    break;
                }
            }
            _ = gauge_tick.tick() => {
                let (presence_rooms, presence_sessions) =
                    state.realtime.presence.counts().await;
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
                if send_stream_json(&mut socket, &serde_json::json!({"kind":"gauges","gauges":snap}))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    }
}

async fn send_stream_json(
    socket: &mut WebSocket,
    value: &serde_json::Value,
) -> Result<(), axum::Error> {
    use axum::extract::ws::Message;
    let text = serde_json::to_string(value).unwrap_or_else(|_| "{}".into());
    socket.send(Message::Text(text.into())).await
}

/// Body for `POST /admin/db/{db}/explain`: a single Query DSL document
/// against the named database, identical in shape to `POST /api/query`. The
/// handler compiles it (no execution) and returns the SQL + ordered binds +
/// any warnings the compile produced.
#[derive(Deserialize)]
pub(super) struct ExplainBody {
    query: crate::query::Query,
}

/// One row of the `/explain` response. `binds` is the ordered list of typed
/// parameters the executor would pass to Postgres in `$1..$n` order; `params`
/// is the same list formatted as strings for at-a-glance reading (booleans as
/// `"true"`/`"false"`, numbers via `Display`). `sql` is the exact string the
/// real query path executes — never interpolated from `params`, never
/// re-rendered from a different code path.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ExplainResponse {
    sql: String,
    params: Vec<String>,
    terminal: String,
    warnings: Vec<String>,
}

/// `POST /admin/db/{db}/explain` — compile a Query DSL body for inspection
/// (ENH-019). The route runs the SAME `compile_query` the read path uses, so
/// the returned `sql` is byte-identical to what `POST /api/query` would
/// execute against that database. `params` is the ordered bind list (the
/// `$1..$n` values, formatted as strings — the wire does not carry typed
/// binds because clients display this, they do not re-execute it). No Postgres
/// round-trip — `compile_query` is pure. `warnings` surfaces compile-time
/// concerns (today: a filter on a declared-but-unindexed field).
///
/// Errors surface as the standard `RtDbError` envelope: `Forbidden` when the
/// caller is not an admin (the admin middleware rejects before we get here,
/// but `compile_query` also runs `authorize_table`), `BadRequest` for an
/// unknown table or peer-incompatible terminals, and `Internal` only for
/// genuinely unreachable states (e.g. a `schema.table()` lookup that the
/// schema-cache invariant says cannot miss).
pub(super) async fn admin_explain(
    State(state): State<Arc<AppState>>,
    Path(db): Path<String>,
    Json(body): Json<ExplainBody>,
) -> Result<Json<ExplainResponse>, RtDbError> {
    let schema = state.schemas.get(&state.pool, &db).await?;
    // Admins bypass the per-db allowlist but `compile_query` still runs
    // `authorize_table` (machine-token tables allowlist gate). An admin
    // explaining a query against an allowlisted-only table that the admin
    // context can't reach would surface as `Forbidden` here — the desired
    // posture, since `/explain` is a read-style introspection route.
    // Construct a bypass `PrincipalCtx` (admin path; no user id) so the
    // compile sees the same row-auth posture the admin-mutate paths use.
    let principal_ctx = crate::auth::PrincipalCtx::bypass();
    let (cq, _warnings) = crate::query::compile_query(&db, &schema, &body.query, &principal_ctx)?;
    // Recompute warnings separately so the response can carry them even when
    // the caller is a bypass principal (compile_query's warning pass is
    // schema-only and independent of the principal).
    let warnings = crate::query::collect_filter_warnings(&schema, &body.query);
    let params = cq
        .binds
        .iter()
        .map(|bind| match bind {
            crate::txn::EqBind::Text(v) => v.clone(),
            crate::txn::EqBind::Num(v) => v.to_string(),
            crate::txn::EqBind::Bool(v) => v.to_string(),
            crate::txn::EqBind::I64(v) => v.to_string(),
        })
        .collect::<Vec<String>>();
    Ok(Json(ExplainResponse {
        sql: cq.sql,
        params,
        terminal: cq.terminal.to_string(),
        warnings,
    }))
}

#[derive(Deserialize)]
pub(super) struct SlowQueriesParams {
    #[serde(default)]
    db: Option<String>,
    #[serde(default = "default_slow_queries_limit")]
    limit: usize,
}
fn default_slow_queries_limit() -> usize {
    100
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SlowQueriesResponse {
    queries: Vec<crate::metrics::SlowQueryRecord>,
    /// The configured threshold (`RTDB_SLOW_QUERY_MS`) echoed back so the
    /// dashboard can render "slow = > N ms" without a separate config call.
    threshold_ms: u64,
    /// The configured ring-buffer cap (`RTDB_SLOW_QUERY_CAPACITY`); the
    /// response never returns more than this many rows regardless of `limit`.
    capacity: usize,
}

/// `GET /admin/slow-queries?db=<optional>&limit=<n>` — the slow-query log
/// (ENH-019). Returns the bounded in-memory ring newest-first, optionally
/// filtered by database. `limit` defaults to 100 and is capped at the
/// configured capacity (so an operator requesting more than the buffer can
/// hold gets the buffer, not an error). When the log is off
/// (`RTDB_SLOW_QUERY_MS=0`, the default) the response is an empty list plus
/// the configured threshold so the dashboard can show "logging disabled".
pub(super) async fn list_slow_queries(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    QueryParams(params): QueryParams<SlowQueriesParams>,
) -> Result<Json<SlowQueriesResponse>, RtDbError> {
    let threshold_ms = state.config.slow_query_ms;
    let capacity = state.config.slow_query_capacity;
    let limit = if capacity == 0 {
        0
    } else {
        params.limit.min(capacity).max(1)
    };
    let mut rows = state.runtime.metrics.recent_slow_queries();
    if let Some(db) = params.db.as_deref()
        && !db.is_empty()
    {
        rows.retain(|r| r.db == db);
    }
    rows.truncate(limit);
    Ok(Json(SlowQueriesResponse {
        queries: rows,
        threshold_ms,
        capacity,
    }))
}
