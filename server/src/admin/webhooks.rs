//! Admin webhook routes: list/create/edit/delete webhooks and browse their
//! delivery outbox. `crate::webhook` is the backing store; SSRF validation
//! (SEC-001) runs on create and on any URL-supplying edit.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, Query as QueryParams, State};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};

use crate::error::RtDbError;
use crate::http_api::ApiJson;
use crate::{AppState, db};

use super::OkResponse;

#[derive(Serialize)]
pub(super) struct WebhooksResponse {
    webhooks: Vec<crate::webhook::Webhook>,
}

/// `GET /admin/db/{db}/webhooks` — list webhooks registered for `db`. When
/// webhooks are disabled at boot the table is permitted to not exist, so this
/// returns an empty list rather than erroring (mirrors `/admin/audit`).
pub(super) async fn admin_list_webhooks(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path(db): Path<String>,
) -> Result<Json<WebhooksResponse>, RtDbError> {
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    if !state.config.webhooks_enabled {
        return Ok(Json(WebhooksResponse {
            webhooks: Vec::new(),
        }));
    }
    let webhooks = crate::webhook::list_webhooks(&state.pool, &db).await?;
    Ok(Json(WebhooksResponse { webhooks }))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AdminCreateWebhookRequest {
    url: String,
    #[serde(default)]
    table: Option<String>,
    #[serde(default)]
    events: Option<Vec<String>>,
    /// When omitted the webhook is created enabled (the historical behavior).
    #[serde(default)]
    enabled: Option<bool>,
}

#[derive(Serialize)]
pub(super) struct AdminCreateWebhookResponse {
    id: i64,
}

/// The closed set of event names a webhook may subscribe to: the five op kinds
/// (matching `OpKind`'s lowercase serde form) plus `*` (all events). Used to
/// reject typos at registration time so a misspelled event never silently fails
/// to match.
fn is_valid_event(name: &str) -> bool {
    matches!(
        name,
        "*" | "insert" | "patch" | "replace" | "delete" | "upsert"
    )
}

/// `POST /admin/db/{db}/webhooks` — register a webhook URL for delivery on
/// matching document changes. `table` omitted/null = all tables; `events`
/// omitted defaults to `["*"]` (all events). Rejected with a 400 when webhooks
/// are disabled at boot or the events list contains an unknown name.
pub(super) async fn admin_create_webhook(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path(db): Path<String>,
    ApiJson(body): ApiJson<AdminCreateWebhookRequest>,
) -> Result<Json<AdminCreateWebhookResponse>, RtDbError> {
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    if !state.config.webhooks_enabled {
        return Err(RtDbError::bad_request(
            "webhooks are disabled on this server (set RTDB_WEBHOOKS_ENABLED=true at boot)",
        ));
    }
    let url = body.url.trim();
    if url.is_empty() {
        return Err(RtDbError::bad_request("url is required"));
    }
    // SSRF guard (SEC-001): https-only by default, plus a private/loopback/
    // metadata IP-range denylist + DNS resolution check. The dev flag
    // `RTDB_WEBHOOK_ALLOW_HTTP` opts back into http + private targets so the
    // integration tests can point at a local receiver.
    crate::webhook::validate_webhook_url(url, state.config.webhook_allow_http)
        .await
        .map_err(RtDbError::bad_request)?;
    // An empty `table` string is treated as "all tables" (None), matching how
    // NULL is interpreted by the enqueue matcher.
    let table = body
        .table
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let events = body.events.unwrap_or_else(|| vec!["*".to_string()]);
    if events.is_empty() {
        return Err(RtDbError::bad_request("events must not be empty"));
    }
    for ev in &events {
        if !is_valid_event(ev) {
            return Err(RtDbError::bad_request(format!(
                "unknown event '{ev}'; expected one of insert, patch, replace, delete, upsert, or *"
            )));
        }
    }
    let webhook = crate::webhook::create_webhook(
        &state.pool,
        &db,
        table,
        url,
        &events,
        body.enabled.unwrap_or(true),
    )
    .await?;
    Ok(Json(AdminCreateWebhookResponse { id: webhook.id }))
}

/// `DELETE /admin/db/{db}/webhooks/{id}` — remove a webhook (cascading its
/// pending deliveries via the FK). A non-numeric id is a 400; a missing id is a
/// 404.
pub(super) async fn admin_delete_webhook(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
) -> Result<Json<OkResponse>, RtDbError> {
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let id: i64 = id
        .parse()
        .map_err(|_| RtDbError::bad_request("webhook id must be an integer"))?;
    if !state.config.webhooks_enabled {
        // Table may not exist; treat as already-gone.
        return Ok(Json(OkResponse { ok: false }));
    }
    let ok = crate::webhook::delete_webhook(&state.pool, id, &db).await?;
    Ok(Json(OkResponse { ok }))
}

/// Deserialize a present-as-`null` JSON value as `Some(None)` rather than the
/// serde default of collapsing `null` on an `Option<T>` field to `None`. This
/// is what lets `table: Option<Option<String>>` distinguish "field omitted"
/// (`None` → leave alone) from `"table": null` (`Some(None)` → clear to
/// all-tables) from `"table": "x"` (`Some(Some("x"))` → set). Standard serde
/// idiom for the double-Option patch-edit pattern.
fn deserialize_some<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
where
    T: Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    Deserialize::deserialize(deserializer).map(Some)
}

/// Body for `PUT /admin/db/{db}/webhooks/{id}` — every field optional, absent =
/// unchanged. `table` is a nested `Option<Option<String>>`: outer `None` leaves
/// the table filter alone, `Some(None)` (JSON `null`) clears it to all-tables,
/// and `Some(Some(t))` sets it to `t`. The other fields are flat `Option<T>` —
/// present sets, absent keeps the existing value. `rotateSecret: true`
/// generates a fresh server-side signing secret (SEC-115); the secret value
/// itself is never accepted from the client.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AdminEditWebhookRequest {
    #[serde(default)]
    url: Option<String>,
    #[serde(default, deserialize_with = "deserialize_some")]
    table: Option<Option<String>>,
    #[serde(default)]
    events: Option<Vec<String>>,
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    rotate_secret: bool,
}

/// `PUT /admin/db/{db}/webhooks/{id}` — partial-edit a webhook's `url`,
/// `table`, `events`, or `enabled`. Only fields present in the body are
/// updated; absent fields keep their existing value. Rejected with a 400 when
/// webhooks are disabled at boot or `events` contains an unknown name; a
/// missing `(id, db)` row is a 404. Returns the updated webhook.
pub(super) async fn admin_edit_webhook(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
    ApiJson(body): ApiJson<AdminEditWebhookRequest>,
) -> Result<Json<crate::webhook::Webhook>, RtDbError> {
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let id: i64 = id
        .parse()
        .map_err(|_| RtDbError::bad_request("webhook id must be an integer"))?;
    if !state.config.webhooks_enabled {
        return Err(RtDbError::bad_request(
            "webhooks are disabled on this server (set RTDB_WEBHOOKS_ENABLED=true at boot)",
        ));
    }
    let url = body.url.as_deref().map(str::trim);
    if let Some(u) = url
        && u.is_empty()
    {
        return Err(RtDbError::bad_request("url must not be empty"));
    }
    // SSRF guard (SEC-001): when a new URL is supplied it must clear the same
    // validator the create path uses (https-only + private/loopback/metadata
    // denylist), so a `PUT` can't bypass registration-time validation.
    if let Some(u) = url {
        crate::webhook::validate_webhook_url(u, state.config.webhook_allow_http)
            .await
            .map_err(RtDbError::bad_request)?;
    }
    // Normalize `table`: an empty string is treated as "all tables" (None),
    // matching the create path. Trim the inner value when present. Built as an
    // owned `Option<Option<String>>` so we can hand `edit_webhook` a borrowed
    // `Option<Option<&str>>` view below without borrowing the closure's local.
    let tbl: Option<Option<String>> = body.table.map(|inner| {
        inner.and_then(|s| {
            let t = s.trim();
            if t.is_empty() {
                None
            } else {
                Some(t.to_string())
            }
        })
    });
    let tbl_ref = tbl.as_ref().map(|i| i.as_deref());
    let events = if let Some(ev) = body.events {
        if ev.is_empty() {
            return Err(RtDbError::bad_request("events must not be empty"));
        }
        for name in &ev {
            if !is_valid_event(name) {
                return Err(RtDbError::bad_request(format!(
                    "unknown event '{name}'; expected one of insert, patch, replace, delete, upsert, or *"
                )));
            }
        }
        Some(ev)
    } else {
        None
    };
    let updated = crate::webhook::edit_webhook(
        &state.pool,
        id,
        &db,
        crate::webhook::WebhookPatch {
            url,
            tbl: tbl_ref,
            events: events.as_deref(),
            enabled: body.enabled,
            rotate_secret: body.rotate_secret,
        },
    )
    .await?
    .ok_or_else(|| RtDbError::not_found("webhook not found for this database"))?;
    Ok(Json(updated))
}

#[derive(Deserialize)]
pub(super) struct DeliveryListParams {
    #[serde(default)]
    status: Option<String>,
    #[serde(default = "default_delivery_limit")]
    limit: i64,
    #[serde(default = "default_delivery_offset")]
    offset: i64,
}
fn default_delivery_limit() -> i64 {
    50
}
fn default_delivery_offset() -> i64 {
    0
}

#[derive(Serialize)]
pub(super) struct DeliveriesResponse {
    deliveries: Vec<crate::webhook::DeliveryRow>,
}

/// `GET /admin/db/{db}/webhooks/{id}/deliveries?status=&limit=&offset=` — list
/// the delivery outbox for a webhook, newest `next_attempt` first. `status`
/// filters by `pending|retrying|delivered|failed`; `limit` defaults to 50 and
/// clamps to `[1,1000]`; `offset` defaults to 0. When webhooks are disabled at
/// boot this returns an empty list (the table may not exist). A non-numeric id
/// is a 400; the webhook not existing yields an empty list rather than a 404 —
/// the outbox is scoped by `webhook_id`, so a never-existed id simply has no
/// rows (mirrors how `admin_list_webhooks` returns `[]` for a db with none).
pub(super) async fn admin_list_deliveries(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
    QueryParams(params): QueryParams<DeliveryListParams>,
) -> Result<Json<DeliveriesResponse>, RtDbError> {
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let id: i64 = id
        .parse()
        .map_err(|_| RtDbError::bad_request("webhook id must be an integer"))?;
    if !state.config.webhooks_enabled {
        return Ok(Json(DeliveriesResponse {
            deliveries: Vec::new(),
        }));
    }
    let limit = params.limit.clamp(1, 1000);
    let offset = params.offset.max(0);
    let deliveries = crate::webhook::fetch_deliveries(
        &state.pool,
        id,
        &db,
        params.status.as_deref(),
        limit,
        offset,
    )
    .await?;
    Ok(Json(DeliveriesResponse { deliveries }))
}
