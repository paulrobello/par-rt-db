//! Admin observability routes: metrics snapshot, op-feed ring, durable audit
//! log, live-subscription inspector, and the `/admin/stream` WebSocket that
//! replays + streams filtered op events and periodic gauges.

use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::extract::{FromRequest, Query as QueryParams, Request, State};
use axum::http::HeaderMap;
use axum::response::Response;
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::error::RtDbError;

use super::{authenticate_admin, bearer_from_subprotocol, bearer_value, require_admin};

pub(super) async fn metrics_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<crate::metrics::MetricsSnapshot>, RtDbError> {
    require_admin(&state, &headers).await?;
    let (presence_rooms, presence_sessions) = state.realtime.presence.counts().await;
    Ok(Json(
        state
            .runtime
            .metrics
            .snapshot(
                &state.pool,
                &state.realtime.subs,
                state.runtime.started_at,
                presence_rooms,
                presence_sessions,
            )
            .await,
    ))
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
    headers: HeaderMap,
    QueryParams(params): QueryParams<AuditParams>,
) -> Result<Json<AuditResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
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
    headers: HeaderMap,
    QueryParams(params): QueryParams<SubscriptionsParams>,
) -> Result<Json<SubscriptionsResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
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
    headers: HeaderMap,
    QueryParams(params): QueryParams<OpsRecentParams>,
) -> Result<Json<OpsRecentResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
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
    headers: HeaderMap,
    QueryParams(params): QueryParams<StreamParams>,
    req: Request,
) -> Result<Response, RtDbError> {
    // SEC-105: WS handshakes are CORS-exempt — reject a cookie-authenticated
    // upgrade from a disallowed Origin before WS negotiation begins. Browsers
    // always send `Origin`; absent Origin = non-browser (CLI/automation) and
    // the bearer/subprotocol gate still authenticates.
    if !crate::origin_allowed(&headers, &state.runtime.hot, &state.config.public_url) {
        return Err(RtDbError::forbidden("websocket origin not allowed"));
    }
    // Bearer from the Authorization header (CLI/automation) or, failing that,
    // the `rtdb-admin.<token>` subprotocol (browser dashboard — browsers can't
    // set headers on a WS handshake). When the subprotocol path is used we echo
    // it back: a client that offered a subprotocol requires the server to
    // negotiate one (tokio-tungstenite errors otherwise; browsers are lenient
    // but the echo is the spec-correct 101 response).
    let (token, offered_subprotocol) = match bearer_value(&headers) {
        Ok(t) => (t, None),
        Err(_) => {
            let t = bearer_from_subprotocol(&headers)?;
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
