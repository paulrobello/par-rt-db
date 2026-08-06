//! Webhook delivery registry — the native answer to "trigger external work on
//! document changes" in a no-embedded-JS architecture.
//!
//! When `Config::webhooks_enabled` is set at boot, the committer calls
//! `enqueue_for_ops` at its four op-feed tap sites (`handle_mutate`,
//! `handle_scheduled`, `handle_migrate`, and `handle_reaper`) so every durable
//! document mutation fans out one `rtdb.webhook_deliveries` row per matching
//! webhook. A background worker
//! (`run_delivery_worker`) drains that outbox and POSTs each payload
//! at-least-once to the registered URL with exponential backoff (capped) and a
//! hard attempt ceiling. Enqueue is best-effort by contract: a logging failure
//! is warned and never fails a durable mutation, mirroring `audit::write_audit_rows`.

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use sqlx::PgPool;

use crate::db::now_ms;
use crate::error::RtDbError;
use crate::txn::{DocOp, OpKind};

/// Maximum delivery attempts before a row is marked `failed`. After this many
/// failures the row stops being picked up by `drain_once` (it no longer matches
/// `status IN ('pending','retrying')`).
const MAX_ATTEMPTS: i32 = 6;

/// Backoff ceiling: `2^attempts` seconds, capped at five minutes so a stuck
/// endpoint does not park a row for the lifetime of the process.
const BACKOFF_CAP_MS: i64 = 5 * 60 * 1000;

/// Per-delivery HTTP timeout. Bounded so a slow endpoint cannot stall the
/// worker's single-threaded drain loop indefinitely.
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(10);

/// Poll interval for the delivery worker. Short enough that a freshly-enqueued
/// row is picked up promptly; long enough that an idle worker does not spin.
const WORKER_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Maximum deliveries processed per `drain_once` pass. Bounds the work per tick
/// so the worker stays responsive to new rows and never monopolizes a
/// connection.
const DRAIN_BATCH: i64 = 50;

/// Truncation ceiling for `last_error` so a verbose upstream response body
/// cannot bloat the deliveries table.
const MAX_ERROR_LEN: usize = 500;

/// Lowercase wire name of an `OpKind`, matching `OpKind`'s `rename_all =
/// "lowercase"` serde attribute so the webhook payload's `kind` field agrees
/// with the op-feed's serialized `kind` (and the audit row's `op` column).
fn op_kind_name(kind: OpKind) -> &'static str {
    match kind {
        OpKind::Insert => "insert",
        OpKind::Patch => "patch",
        OpKind::Replace => "replace",
        OpKind::Delete => "delete",
        OpKind::Upsert => "upsert",
    }
}

/// The JSON body POSTed to each registered webhook URL. Shape matches
/// `op_feed::OpEvent` (`{db, table, docId, kind, ts, owner}`, camelCase) with a
/// `source` tag indicating which committer tap produced it (`"mutate"` or
/// `"scheduled"`).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WebhookPayload {
    pub db: String,
    #[serde(rename = "table")]
    pub table: String,
    pub doc_id: String,
    pub kind: String,
    pub ts: i64,
    pub owner: Option<String>,
    pub source: &'static str,
}

/// One registered webhook. `tbl = None` means "all tables"; `events` contains
/// op names (`insert`/`patch`/`replace`/`delete`/`upsert`) or the single
/// element `*` to match every event.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Webhook {
    pub id: i64,
    pub db: String,
    #[serde(rename = "table")]
    pub tbl: Option<String>,
    pub url: String,
    pub events: Vec<String>,
    pub created_at: i64,
    pub enabled: bool,
}

/// Exponential backoff in milliseconds for the next retry after `attempts`
/// failures: `2^attempts` seconds, capped at [`BACKOFF_CAP_MS`]. Monotonically
/// non-decreasing in `attempts` until the cap, and overflow-safe for any
/// sane input (a `checked_pow`/`saturating_mul` chain, never panics).
fn backoff_ms(attempts: i32) -> i64 {
    let exp = attempts.max(0) as u32;
    let secs = 2_i64.checked_pow(exp).unwrap_or(i64::MAX);
    secs.saturating_mul(1000).min(BACKOFF_CAP_MS)
}

/// Char-boundary-safe truncation of an error/status string for the
/// `last_error` column, with an ellipsis marker when truncated.
fn truncate_error(s: &str) -> String {
    if s.len() <= MAX_ERROR_LEN {
        return s.to_string();
    }
    let mut end = MAX_ERROR_LEN;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &s[..end])
}

/// Enqueues one `webhook_deliveries` row per webhook matching any of `ops`,
/// best-effort: a DB error is returned to the caller (the committer tap) which
/// logs and continues, so a delivery-table hiccup never fails a durable
/// mutation. Matching webhooks for a `(db, table, kind)` triple are those whose
/// `tbl IS NULL OR tbl = table` and whose `events` array is either exactly
/// `{*}` (all events) or contains the op's lowercase kind name. The payload is
/// an [`WebhookPayload`] serialized to JSONB.
pub async fn enqueue_for_ops(
    pool: &PgPool,
    db: &str,
    owner: Option<&str>,
    source: &'static str,
    ops: &[DocOp],
) -> Result<(), RtDbError> {
    if ops.is_empty() {
        return Ok(());
    }
    let ts = now_ms();
    let owner = owner.map(|s| s.to_string());
    for op in ops {
        let kind = op_kind_name(op.kind);
        // Match webhooks for this (db, table, kind). `tbl IS NULL` = all tables;
        // `events = '{*}'` = the literal single-element wildcard array; otherwise
        // the op kind must appear in the events array. `enabled` excludes paused
        // webhooks so they produce no deliveries while retaining their config.
        let matches: Vec<(i64,)> = sqlx::query_as(
            "SELECT id FROM rtdb.webhooks \
             WHERE db = $1 \
               AND (tbl IS NULL OR tbl = $2) \
               AND (events = '{*}' OR $3 = ANY(events)) \
               AND enabled",
        )
        .bind(db)
        .bind(&op.table)
        .bind(kind)
        .fetch_all(pool)
        .await?;
        if matches.is_empty() {
            continue;
        }
        let payload = serde_json::to_value(&WebhookPayload {
            db: db.to_string(),
            table: op.table.clone(),
            doc_id: op.id.clone(),
            kind: kind.to_string(),
            ts,
            owner: owner.clone(),
            source,
        })
        .map_err(|e| RtDbError::internal(format!("encode webhook payload: {e}")))?;
        let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO rtdb.webhook_deliveries \
             (webhook_id, payload, attempts, next_attempt, status) ",
        );
        builder.push_values(matches, |mut b, (webhook_id,)| {
            b.push_bind(webhook_id)
                .push_bind(payload.clone())
                .push_bind(0_i32)
                .push_bind(ts)
                .push_bind("pending");
        });
        builder.build().execute(pool).await?;
    }
    Ok(())
}

/// Selects up to [`DRAIN_BATCH`] due deliveries joined with their webhook URL,
/// POSTs each payload via reqwest, and updates the row to `delivered` (2xx) or
/// bumps `attempts`/`last_error` and sets either `retrying` (under
/// [`MAX_ATTEMPTS`]) or `failed` (at the ceiling). Returns the count processed.
/// Best-effort per row: a single delivery's update failure is logged and the
/// loop continues to the next row so one bad row cannot stall the outbox.
async fn drain_once_with_client(
    pool: &PgPool,
    client: &reqwest::Client,
) -> Result<usize, RtDbError> {
    type DueRow = (i64, String, serde_json::Value, i32);
    let rows: Vec<DueRow> = sqlx::query_as(
        "SELECT d.id, w.url, d.payload, d.attempts \
         FROM rtdb.webhook_deliveries d \
         JOIN rtdb.webhooks w ON w.id = d.webhook_id \
         WHERE d.status IN ('pending', 'retrying') AND d.next_attempt <= $1 \
         ORDER BY d.next_attempt \
         LIMIT $2",
    )
    .bind(now_ms())
    .bind(DRAIN_BATCH)
    .fetch_all(pool)
    .await?;

    let count = rows.len();
    for (id, url, payload, attempts) in rows {
        let result = client.post(&url).json(&payload).send().await;
        let outcome = match result {
            Ok(resp) if resp.status().is_success() => Ok(()),
            Ok(resp) => Err(format!("HTTP {}", resp.status().as_u16())),
            Err(err) => Err(format!("request error: {err}")),
        };
        match outcome {
            Ok(()) => {
                if let Err(err) = sqlx::query(
                    "UPDATE rtdb.webhook_deliveries \
                     SET status = 'delivered', last_error = NULL \
                     WHERE id = $1",
                )
                .bind(id)
                .execute(pool)
                .await
                {
                    tracing::warn!(delivery_id = id, error = %err, "webhook: mark delivered failed");
                }
            }
            Err(msg) => {
                let new_attempts = attempts + 1;
                let status = if new_attempts >= MAX_ATTEMPTS {
                    "failed"
                } else {
                    "retrying"
                };
                let next_attempt = now_ms() + backoff_ms(new_attempts);
                let last_error = truncate_error(&msg);
                if let Err(err) = sqlx::query(
                    "UPDATE rtdb.webhook_deliveries \
                     SET attempts = $2, status = $3, next_attempt = $4, last_error = $5 \
                     WHERE id = $1",
                )
                .bind(id)
                .bind(new_attempts)
                .bind(status)
                .bind(next_attempt)
                .bind(&last_error)
                .execute(pool)
                .await
                {
                    tracing::warn!(delivery_id = id, error = %err, "webhook: mark retry failed");
                }
            }
        }
    }
    Ok(count)
}

/// One drain pass with a freshly-built HTTP client. Exposed for tests so the
/// full enqueue → drain → receiver round-trip can be exercised without running
/// the infinite worker loop. The worker itself reuses one client across ticks
/// via `drain_once_with_client` to avoid reconnect churn.
pub async fn drain_once(pool: &PgPool) -> Result<usize, RtDbError> {
    let client = reqwest::Client::builder()
        .timeout(DELIVERY_TIMEOUT)
        .build()
        .map_err(|e| RtDbError::internal(format!("build webhook HTTP client: {e}")))?;
    drain_once_with_client(pool, &client).await
}

/// The delivery worker loop: drain, sleep [`WORKER_POLL_INTERVAL`], repeat.
/// Never panics — every drain error is logged and the loop continues, so a
/// transient DB or network blip does not kill the worker. Runs until the
/// process exits (the task is `tokio::spawn`ed at boot when webhooks are
/// enabled).
pub async fn run_delivery_worker(pool: PgPool) {
    let client = match reqwest::Client::builder().timeout(DELIVERY_TIMEOUT).build() {
        Ok(c) => c,
        Err(err) => {
            tracing::error!(error = %err, "webhook worker: failed to build HTTP client; worker exiting");
            return;
        }
    };
    let pool = Arc::new(pool);
    loop {
        if let Err(err) = drain_once_with_client(&pool, &client).await {
            tracing::warn!(error = %err, "webhook delivery drain failed");
        }
        tokio::time::sleep(WORKER_POLL_INTERVAL).await;
    }
}

/// Lists webhooks registered for `db`, ordered by id. Called by
/// `GET /admin/db/{db}/webhooks`.
pub async fn list_webhooks(pool: &PgPool, db: &str) -> Result<Vec<Webhook>, RtDbError> {
    type Row = (i64, String, Option<String>, String, Vec<String>, i64, bool);
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT id, db, tbl, url, events, created_at, enabled \
         FROM rtdb.webhooks WHERE db = $1 ORDER BY id",
    )
    .bind(db)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(id, db, tbl, url, events, created_at, enabled)| Webhook {
            id,
            db,
            tbl,
            url,
            events,
            created_at,
            enabled,
        })
        .collect())
}

/// Inserts a webhook for `db` and returns the stored row. `tbl = None` means
/// all tables; `events` is stored verbatim (use `["*"]` for all events).
/// `enabled = false` registers the webhook paused (no deliveries until flipped
/// back on). Called by `POST /admin/db/{db}/webhooks`.
pub async fn create_webhook(
    pool: &PgPool,
    db: &str,
    tbl: Option<&str>,
    url: &str,
    events: &[String],
    enabled: bool,
) -> Result<Webhook, RtDbError> {
    type Row = (i64, String, Option<String>, String, Vec<String>, i64, bool);
    let row: Row = sqlx::query_as(
        "INSERT INTO rtdb.webhooks (db, tbl, url, events, created_at, enabled) \
         VALUES ($1, $2, $3, $4, $5, $6) \
         RETURNING id, db, tbl, url, events, created_at, enabled",
    )
    .bind(db)
    .bind(tbl)
    .bind(url)
    .bind(events)
    .bind(now_ms())
    .bind(enabled)
    .fetch_one(pool)
    .await?;
    Ok(Webhook {
        id: row.0,
        db: row.1,
        tbl: row.2,
        url: row.3,
        events: row.4,
        created_at: row.5,
        enabled: row.6,
    })
}

/// Deletes webhook `id` (cascading its deliveries via the FK). Returns true if
/// a row was removed. Called by `DELETE /admin/db/{db}/webhooks/{id}`.
pub async fn delete_webhook(pool: &PgPool, id: i64) -> Result<bool, RtDbError> {
    let res = sqlx::query("DELETE FROM rtdb.webhooks WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(res.rows_affected() > 0)
}

/// One delivery row from the `rtdb.webhook_deliveries` outbox. The wire shape is
/// camelCase (`nextAttempt`/`lastError`); `payload` is the raw JSONB body queued
/// at enqueue time, passed through verbatim so an operator can inspect the exact
/// event the worker will/did POST.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeliveryRow {
    pub id: i64,
    pub attempts: i32,
    pub status: String,
    pub next_attempt: i64,
    pub last_error: Option<String>,
    pub payload: serde_json::Value,
}

/// Applies a partial update to webhook `id` scoped to `db`, returning the
/// updated row or `None` if no such `(id, db)` row exists. Each `Option`
/// argument is itself "unchanged when `None`": `url`/`events`/`enabled` take
/// `Some(value)` to set, and `tbl` is a nested `Option<Option<&str>>` so the
/// caller can distinguish "leave the table filter alone" (`None`) from "set it
/// to all-tables" (`Some(None)`) from "set it to this table" (`Some(Some(t))`.
/// When every field is `None` this short-circuits to a plain SELECT — no
/// empty-SET SQL is synthesized. Called by `PUT /admin/db/{db}/webhooks/{id}`.
pub async fn edit_webhook(
    pool: &PgPool,
    id: i64,
    db: &str,
    url: Option<&str>,
    tbl: Option<Option<&str>>,
    events: Option<&[String]>,
    enabled: Option<bool>,
) -> Result<Option<Webhook>, RtDbError> {
    type Row = (i64, String, Option<String>, String, Vec<String>, i64, bool);
    // No fields → just read the current row.
    if url.is_none() && tbl.is_none() && events.is_none() && enabled.is_none() {
        let row: Option<Row> = sqlx::query_as(
            "SELECT id, db, tbl, url, events, created_at, enabled \
             FROM rtdb.webhooks WHERE id = $1 AND db = $2",
        )
        .bind(id)
        .bind(db)
        .fetch_optional(pool)
        .await?;
        return Ok(row.map(|r| Webhook {
            id: r.0,
            db: r.1,
            tbl: r.2,
            url: r.3,
            events: r.4,
            created_at: r.5,
            enabled: r.6,
        }));
    }
    let mut q = sqlx::QueryBuilder::<sqlx::Postgres>::new("UPDATE rtdb.webhooks SET ");
    let mut need_comma = false;
    if let Some(u) = url {
        q.push("url = ");
        q.push_bind(u);
        need_comma = true;
    }
    if let Some(t) = tbl {
        if need_comma {
            q.push(", ");
        }
        q.push("tbl = ");
        q.push_bind(t);
        need_comma = true;
    }
    if let Some(ev) = events {
        if need_comma {
            q.push(", ");
        }
        q.push("events = ");
        q.push_bind(ev);
        need_comma = true;
    }
    if let Some(en) = enabled {
        if need_comma {
            q.push(", ");
        }
        q.push("enabled = ");
        q.push_bind(en);
    }
    q.push(" WHERE id = ");
    q.push_bind(id);
    q.push(" AND db = ");
    q.push_bind(db);
    q.push(" RETURNING id, db, tbl, url, events, created_at, enabled");
    // `build()` yields a `Query` whose rows decode as the raw `PgRow` (no tuple
    // indexing); `build_query_as::<Row>()` attaches the tuple decoder so
    // `fetch_optional` returns `Option<Row>` directly.
    let row: Option<Row> = q.build_query_as::<Row>().fetch_optional(pool).await?;
    Ok(row.map(|r| Webhook {
        id: r.0,
        db: r.1,
        tbl: r.2,
        url: r.3,
        events: r.4,
        created_at: r.5,
        enabled: r.6,
    }))
}

/// Reads delivery rows for `webhook_id` newest-first (by `next_attempt DESC`
/// then `id DESC` as a stable tie-breaker). `status` filters when `Some`;
/// `limit`/`offset` page. Called by `GET /admin/db/{db}/webhooks/{id}/deliveries`.
pub async fn fetch_deliveries(
    pool: &PgPool,
    webhook_id: i64,
    status: Option<&str>,
    limit: i64,
    offset: i64,
) -> Result<Vec<DeliveryRow>, RtDbError> {
    type Row = (i64, i32, String, i64, Option<String>, serde_json::Value);
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT id, attempts, status, next_attempt, last_error, payload \
         FROM rtdb.webhook_deliveries \
         WHERE webhook_id = $1 AND ($2::text IS NULL OR status = $2) \
         ORDER BY next_attempt DESC, id DESC \
         LIMIT $3 OFFSET $4",
    )
    .bind(webhook_id)
    .bind(status)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(id, attempts, status, next_attempt, last_error, payload)| DeliveryRow {
                id,
                attempts,
                status,
                next_attempt,
                last_error,
                payload,
            },
        )
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_is_monotonic_until_capped() {
        // 2^attempts seconds — strictly increasing through the powers of two
        // up to the cap.
        let mut prev = backoff_ms(0);
        for attempts in 1..=8 {
            let next = backoff_ms(attempts);
            assert!(
                next > prev,
                "backoff must increase: {next} not > {prev} at attempts={attempts}"
            );
            prev = next;
        }
        // backoff(8) = 2^8 s = 256_000 ms, still under the 5-min cap.
        assert_eq!(backoff_ms(8), 256_000);
    }

    #[test]
    fn backoff_is_capped_at_five_minutes() {
        assert_eq!(backoff_ms(9), BACKOFF_CAP_MS); // 2^9 s = 512 s > 300 s
        assert_eq!(backoff_ms(20), BACKOFF_CAP_MS);
        assert_eq!(backoff_ms(i32::MAX), BACKOFF_CAP_MS); // overflow-safe
    }

    #[test]
    fn backoff_has_expected_values_within_retry_window() {
        // The retry window is attempts 1..=MAX_ATTEMPTS (6). Within it the
        // formula is exactly 2^attempts seconds.
        assert_eq!(backoff_ms(1), 2_000);
        assert_eq!(backoff_ms(2), 4_000);
        assert_eq!(backoff_ms(3), 8_000);
        assert_eq!(backoff_ms(4), 16_000);
        assert_eq!(backoff_ms(5), 32_000);
        assert_eq!(backoff_ms(6), 64_000); // final retry scheduled at 64s
    }

    #[test]
    fn payload_serializes_camel_case_with_source() {
        let payload = WebhookPayload {
            db: "kanban".into(),
            table: "projects".into(),
            doc_id: "abc123".into(),
            kind: "insert".into(),
            ts: 1_700_000_000_000,
            owner: Some("user@example.com".into()),
            source: "mutate",
        };
        let v = serde_json::to_value(&payload).expect("serialize payload");
        let obj = v.as_object().expect("payload is object");
        // camelCase keys (no snake_case leakage), plus the source tag.
        assert!(obj.contains_key("db"));
        assert!(obj.contains_key("table"));
        assert!(obj.contains_key("docId"));
        assert!(obj.contains_key("kind"));
        assert!(obj.contains_key("ts"));
        assert!(obj.contains_key("owner"));
        assert!(obj.contains_key("source"));
        assert!(!obj.contains_key("doc_id"));
        assert_eq!(obj["db"], "kanban");
        assert_eq!(obj["table"], "projects");
        assert_eq!(obj["docId"], "abc123");
        assert_eq!(obj["kind"], "insert");
        assert_eq!(obj["ts"], 1_700_000_000_000_i64);
        assert_eq!(obj["owner"], "user@example.com");
        assert_eq!(obj["source"], "mutate");
    }

    #[test]
    fn truncate_error_respects_char_boundaries_and_marks_truncation() {
        // Short string is returned verbatim with no marker.
        assert_eq!(truncate_error("short"), "short");
        // A long ASCII string is cut to the ceiling plus the marker.
        let long = "x".repeat(MAX_ERROR_LEN + 50);
        let truncated = truncate_error(&long);
        assert!(truncated.ends_with("..."));
        assert_eq!(truncated.len(), MAX_ERROR_LEN + 3);
        // Multibyte input never splits a code point: the truncate point walks
        // back to a char boundary, so the result is valid UTF-8 by construction.
        let emoji = "😀".repeat(MAX_ERROR_LEN); // 4 bytes per emoji
        let truncated = truncate_error(&emoji);
        assert!(truncated.ends_with("..."));
        assert!(truncated.len() <= MAX_ERROR_LEN + 3);
    }
}
