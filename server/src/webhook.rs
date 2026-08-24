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

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use rand::RngCore;
use rand::rngs::OsRng;
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
/// element `*` to match every event. `secret` is the per-webhook HMAC key used
/// to sign each delivery (`X-Rtdb-Signature`); always set after boot backfill,
/// surfaced in the admin response so an operator can copy it to the receiver
/// (SEC-115).
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
    pub secret: Option<String>,
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

/// `true` for IP addresses a webhook must never target — the SSRF denylist
/// (SEC-001). Covers loopback, private (RFC1918), link-local (including the
/// `169.254.169.254` cloud-metadata IP), unspecified, multicast, broadcast,
/// and IPv6 unique-local/link-local. Pure (no I/O) so it is unit-testable.
pub fn is_blocked_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v) => {
            let oct = v.octets();
            // 0.0.0.0/8 — "this network" / unspecified source.
            oct[0] == 0
            // 10.0.0.0/8 — RFC1918 private.
            || oct[0] == 10
            // 172.16.0.0/12 — RFC1918 private.
            || (oct[0] == 172 && (oct[1] & 0xf0) == 16)
            // 192.168.0.0/16 — RFC1918 private.
            || (oct[0] == 192 && oct[1] == 168)
            // 127.0.0.0/8 — loopback.
            || oct[0] == 127
            // 169.254.0.0/16 — link-local, which includes the cloud-metadata
            // IP 169.254.169.254 (AWS/Azure/GCP).
            || (oct[0] == 169 && oct[1] == 254)
            // 224.0.0.0/4 — multicast.
            || (oct[0] & 0xf0) == 224
            // 255.255.255.255 — broadcast.
            || (oct[0] == 255 && oct[1] == 255 && oct[2] == 255 && oct[3] == 255)
        }
        IpAddr::V6(v) => {
            // ::ffff:a.b.c.d — IPv4-mapped. The `url` crate parses
            // `[::ffff:169.254.169.254]` as an Ipv6 host, so without this the
            // V4 table below would never be consulted for it.
            if let Some(v4) = v.to_ipv4_mapped() {
                return is_blocked_ip(IpAddr::V4(v4));
            }
            let blocked =
                // ::1 — loopback.
                v.is_loopback()
                // :: — unspecified.
                || v.is_unspecified()
                // fe80::/10 — link-local.
                || v.is_unicast_link_local()
                // ff00::/8 — multicast.
                || v.is_multicast()
                // fc00::/7 — unique-local (RFC4193), the IPv6 RFC1918 analog.
                || (v.octets()[0] & 0xfe) == 0xfc;
            if blocked {
                return true;
            }
            // ::a.b.c.d — deprecated IPv4-compatible form, still routable on
            // some stacks. Must stay after the checks above: `to_ipv4` also
            // matches `::1`/`::`, which map to V4 addresses the table allows.
            if let Some(v4) = v.to_ipv4() {
                return is_blocked_ip(IpAddr::V4(v4));
            }
            false
        }
    }
}

/// Generates a fresh 256-bit webhook signing secret as 64 hex chars. Backed by
/// `OsRng` (CSPRNG); the value never leaves the server except in the admin
/// list/edit response so an operator can copy it to the receiver. Used at
/// `create_webhook`, on a `rotateSecret` edit, and to backfill any NULL rows
/// at boot.
pub fn generate_secret() -> String {
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// Hex HMAC-SHA256 over `"{ts}.{body}"` using the webhook's secret. The body is
/// the exact byte sequence POSTed to the receiver, so the receiver can
/// recompute the tag over the bytes it received without any normalization
/// ambiguity. Hex (not base64) keeps the header free of `+/=` hazards.
pub fn compute_signature(secret: &str, ts_ms: i64, body: &[u8]) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
    let mut msg = Vec::with_capacity(20 + body.len());
    msg.extend_from_slice(ts_ms.to_string().as_bytes());
    msg.push(b'.');
    msg.extend_from_slice(body);
    hex::encode(ring::hmac::sign(&key, &msg).as_ref())
}

/// Constant-time verification of a delivery signature, mirroring
/// `signed_url::verify`. Returns `false` for a non-hex signature, a mismatched
/// key, or any difference in `ts`/`body` (the compare itself is constant-time
/// via `ring::hmac::verify`; the `false` return for bad hex reveals only
/// "malformed", not a near-miss).
pub fn verify_signature(secret: &str, ts_ms: i64, body: &[u8], sig_hex: &str) -> bool {
    let Ok(sig_bytes) = hex::decode(sig_hex) else {
        return false;
    };
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
    let mut msg = Vec::with_capacity(20 + body.len());
    msg.extend_from_slice(ts_ms.to_string().as_bytes());
    msg.push(b'.');
    msg.extend_from_slice(body);
    ring::hmac::verify(&key, &msg, &sig_bytes).is_ok()
}

/// reqwest DNS resolver that re-runs the SSRF denylist at connect time (SEC-114
/// TOCTOU close). `validate_webhook_url` already vets the URL at registration,
/// but reqwest performed an INDEPENDENT resolution at connect time with no
/// re-check — a DNS-rebinding attack could land a public-at-registration host
/// on a private/metadata IP at delivery. Wiring this resolver into the delivery
/// client means reqwest's connect-time resolution passes through the same
/// `is_blocked_ip` filter, so a rebind to a blocked IP is rejected (the
/// resolver returns an error → the delivery fails and retries). The single
/// shared delivery client is preserved; no per-delivery client build.
#[derive(Clone, Default)]
struct WebhookDnsResolver;

impl reqwest::dns::Resolve for WebhookDnsResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_string();
        Box::pin(async move {
            // `tokio::net::lookup_host` needs a `host:port` string; the port is
            // irrelevant to resolution (DNS is port-agnostic) and reqwest
            // replaces port 0 with the scheme's conventional port, so 0 is a
            // safe placeholder.
            let resolved: Vec<SocketAddr> = tokio::net::lookup_host(format!("{host}:0"))
                .await
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                    format!("DNS resolution failed for '{host}': {e}").into()
                })?
                .filter(|addr| !is_blocked_ip(addr.ip()))
                .collect();
            if resolved.is_empty() {
                return Err(format!(
                    "'{host}' resolves only to blocked IPs (private/loopback/metadata)"
                )
                .into());
            }
            // Hand back the vetted SocketAddrs (port 0; reqwest fills the real
            // port from the URL scheme). The order is whatever the OS resolver
            // returned — reqwest connects to the first reachable one.
            Ok(Box::new(resolved.into_iter()) as Box<dyn Iterator<Item = SocketAddr> + Send>)
        })
    }
}

/// Validates a webhook target URL against the SSRF policy (SEC-001). When
/// `allow_http` is true (dev flag `RTDB_WEBHOOK_ALLOW_HTTP`) the scheme check
/// and the IP-range denylist are both relaxed so a developer may point a
/// webhook at a local HTTP receiver (`http://127.0.0.1:<port>/...`); this is
/// the only escape hatch and is off by default. In production (`allow_http =
/// false`):
///   - URL must parse with the `https://` scheme (and no embedded credentials).
///   - The host literal, if an IP, must not be in [`is_blocked_ip`]; the host
///     literal, if a name, must not be a known cloud-metadata hostname.
///   - The hostname is resolved via `tokio::net::lookup_host` and rejected if
///     ANY resolved address is in [`is_blocked_ip`]. The delivery client
///     additionally routes connect-time resolution through [`WebhookDnsResolver`]
///     so a DNS rebind between registration and delivery cannot land the
///     connection on a blocked IP (SEC-114); the worker's
///     `redirect(Policy::none())` closes the redirect bypass.
pub async fn validate_webhook_url(url: &str, allow_http: bool) -> Result<(), String> {
    let parsed = url::Url::parse(url).map_err(|e| format!("invalid URL: {e}"))?;
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err("URL must not contain embedded credentials".into());
    }
    let scheme = parsed.scheme().to_ascii_lowercase();
    let scheme_ok = scheme == "https" || (allow_http && scheme == "http");
    if !scheme_ok {
        return Err(if allow_http {
            "URL scheme must be http or https".into()
        } else {
            "URL scheme must be https (set RTDB_WEBHOOK_ALLOW_HTTP=true for http in dev)".into()
        });
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| "URL must have a host".to_string())?;
    // Defense-in-depth on the textual hostname: reject known metadata-service
    // names even before DNS resolution. Cheap and stable; DNS could rebind.
    if matches!(
        host,
        "metadata.google.internal" | "metadata" | "169.254.169.254"
    ) {
        return Err(format!("host '{host}' is a known cloud-metadata endpoint"));
    }
    // Dev escape hatch: skip the IP-range denylist entirely. Lets the
    // integration tests point at `http://127.0.0.1:<port>/...` receivers.
    if allow_http {
        return Ok(());
    }
    // IP literal? Check directly with no DNS round-trip.
    let ip_literal = match parsed.host() {
        Some(url::Host::Ipv4(ip)) => Some(IpAddr::V4(ip)),
        Some(url::Host::Ipv6(ip)) => Some(IpAddr::V6(ip)),
        Some(url::Host::Domain(_)) | None => host.parse::<IpAddr>().ok(),
    };
    if let Some(ip) = ip_literal {
        if is_blocked_ip(ip) {
            return Err(format!(
                "host IP {ip} is in a blocked range (private/loopback/metadata)"
            ));
        }
        return Ok(());
    }
    // Hostname: resolve and reject if any address lands in a blocked range.
    let port = parsed
        .port_or_known_default()
        .expect("http(s) always have a default port");
    let resolved = tokio::net::lookup_host(format!("{host}:{port}"))
        .await
        .map_err(|e| format!("DNS resolution failed for '{host}': {e}"))?;
    for addr in resolved {
        if is_blocked_ip(addr.ip()) {
            return Err(format!(
                "'{host}' resolves to blocked IP {} (private/loopback/metadata)",
                addr.ip()
            ));
        }
    }
    Ok(())
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

/// Selects up to [`DRAIN_BATCH`] due deliveries joined with their webhook URL
/// and signing secret, POSTs each payload via reqwest with an
/// `X-Rtdb-Signature` header (SEC-115), and updates the row to `delivered`
/// (2xx) or bumps `attempts`/`last_error` and sets either `retrying` (under
/// [`MAX_ATTEMPTS`]) or `failed` (at the ceiling). Returns the count processed.
/// Best-effort per row: a single delivery's update failure is logged and the
/// loop continues to the next row so one bad row cannot stall the outbox.
///
/// The body is serialized ONCE and the same bytes are both POSTed and
/// HMAC'd, so the receiver can recompute the tag over exactly the bytes it
/// received with no normalization ambiguity.
async fn drain_once_with_client(
    pool: &PgPool,
    client: &reqwest::Client,
) -> Result<usize, RtDbError> {
    type DueRow = (i64, String, Option<String>, serde_json::Value, i32);
    let rows: Vec<DueRow> = sqlx::query_as(
        "SELECT d.id, w.url, w.secret, d.payload, d.attempts \
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
    for (id, url, secret, payload, attempts) in rows {
        // Serialize once; these exact bytes are both sent and signed.
        let body_bytes = match serde_json::to_vec(&payload) {
            Ok(b) => b,
            Err(err) => {
                let msg = truncate_error(&format!("encode payload: {err}"));
                mark_retry(pool, id, attempts, msg).await;
                continue;
            }
        };
        let ts = now_ms();
        let mut req = client
            .post(&url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body_bytes.clone());
        match secret.as_deref() {
            Some(s) if !s.is_empty() => {
                req = req.header(
                    "X-Rtdb-Signature",
                    format!("t={ts},v1={}", compute_signature(s, ts, &body_bytes)),
                );
            }
            // Should not happen after boot backfill, but degrade gracefully:
            // deliver unsigned rather than drop the event. Receivers that
            // require a signature will reject; the row retries and surfaces.
            _ => tracing::warn!(
                delivery_id = id,
                "webhook delivery skipped signature: webhook has no secret"
            ),
        }
        let result = req.send().await;
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
                mark_retry(pool, id, attempts, truncate_error(&msg)).await;
            }
        }
    }
    Ok(count)
}

/// Bumps `attempts`, computes backoff, and sets the row to `retrying` (under
/// [`MAX_ATTEMPTS`]) or `failed` (at the ceiling). Best-effort: a DB error here
/// is logged and swallowed so a single bad update cannot stall the outbox loop.
/// `raw_error` should already be truncated by the caller (the encode-failure
/// path passes a truncated message; so does the send/HTTP-error path).
async fn mark_retry(pool: &PgPool, id: i64, attempts: i32, last_error: String) {
    let new_attempts = attempts + 1;
    let status = if new_attempts >= MAX_ATTEMPTS {
        "failed"
    } else {
        "retrying"
    };
    let next_attempt = now_ms() + backoff_ms(new_attempts);
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

/// One drain pass with a freshly-built HTTP client. Exposed for tests so the
/// full enqueue → drain → receiver round-trip can be exercised without running
/// the infinite worker loop. The worker itself reuses one client across ticks
/// via `drain_once_with_client` to avoid reconnect churn. The client is built
/// with `redirect(Policy::none())` so a 3xx response is surfaced as the
/// delivery's terminal status instead of being followed — a redirect to an
/// internal host is the SSRF bypass vector (SEC-001).
pub async fn drain_once(pool: &PgPool) -> Result<usize, RtDbError> {
    let client = build_delivery_client()?;
    drain_once_with_client(pool, &client).await
}

/// Builds the webhook-delivery `reqwest::Client`: bounded timeout, **no
/// redirect-following**, and a custom DNS resolver that re-runs the SSRF
/// denylist at connect time. A redirect to an internal host is the SSRF bypass
/// vector — the registration validator ([`validate_webhook_url`]) vets the
/// registered URL, but a benign-on-registration URL that later returns 3xx to
/// `169.254.169.254` would still exfiltrate payloads. Surfacing 3xx as the
/// delivery's `last_error` (via the `Ok(resp) if resp.status().is_success()`
/// arm) closes that path without a per-redirect denylist (SEC-001). The
/// [`WebhookDnsResolver`] closes the SEC-114 residual: reqwest no longer does
/// an independent unvetted resolution at connect time.
fn build_delivery_client() -> Result<reqwest::Client, RtDbError> {
    reqwest::Client::builder()
        .timeout(DELIVERY_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .dns_resolver(Arc::new(WebhookDnsResolver))
        .build()
        .map_err(|e| RtDbError::internal(format!("build webhook HTTP client: {e}")))
}

/// The delivery worker loop: drain, sleep [`WORKER_POLL_INTERVAL`], repeat.
/// Never panics — every drain error is logged and the loop continues, so a
/// transient DB or network blip does not kill the worker. Runs until the
/// process exits (the task is `tokio::spawn`ed at boot when webhooks are
/// enabled).
pub async fn run_delivery_worker(pool: PgPool) {
    let client = match build_delivery_client() {
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
    type Row = (
        i64,
        String,
        Option<String>,
        String,
        Vec<String>,
        i64,
        bool,
        Option<String>,
    );
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT id, db, tbl, url, events, created_at, enabled, secret \
         FROM rtdb.webhooks WHERE db = $1 ORDER BY id",
    )
    .bind(db)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(id, db, tbl, url, events, created_at, enabled, secret)| Webhook {
                id,
                db,
                tbl,
                url,
                events,
                created_at,
                enabled,
                secret,
            },
        )
        .collect())
}

/// Inserts a webhook for `db` and returns the stored row. `tbl = None` means
/// all tables; `events` is stored verbatim (use `["*"]` for all events).
/// `enabled = false` registers the webhook paused (no deliveries until flipped
/// back on). A 256-bit `secret` is generated server-side (never client-supplied)
/// and used to sign each delivery; it is returned here so the admin response
/// can surface it to the operator. Called by `POST /admin/db/{db}/webhooks`.
pub async fn create_webhook(
    pool: &PgPool,
    db: &str,
    tbl: Option<&str>,
    url: &str,
    events: &[String],
    enabled: bool,
) -> Result<Webhook, RtDbError> {
    type Row = (
        i64,
        String,
        Option<String>,
        String,
        Vec<String>,
        i64,
        bool,
        Option<String>,
    );
    let secret = generate_secret();
    let row: Row = sqlx::query_as(
        "INSERT INTO rtdb.webhooks (db, tbl, url, events, created_at, enabled, secret) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) \
         RETURNING id, db, tbl, url, events, created_at, enabled, secret",
    )
    .bind(db)
    .bind(tbl)
    .bind(url)
    .bind(events)
    .bind(now_ms())
    .bind(enabled)
    .bind(&secret)
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
        secret: row.7,
    })
}

/// Deletes webhook `id` scoped to `db` (cascading its deliveries via the FK).
/// Returns true if a row was removed. The `db` scope is defense-in-depth — all
/// callers are server-wide admins today — matching `edit_webhook`'s `id AND db`
/// pattern so a future less-privileged caller can't delete another database's
/// webhook by guessing its numeric id (SEC-134). Called by
/// `DELETE /admin/db/{db}/webhooks/{id}`.
pub async fn delete_webhook(pool: &PgPool, id: i64, db: &str) -> Result<bool, RtDbError> {
    let res = sqlx::query("DELETE FROM rtdb.webhooks WHERE id = $1 AND db = $2")
        .bind(id)
        .bind(db)
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
/// `rotate_secret = true` generates a fresh server-side signing secret
/// (SEC-115); the secret value is never accepted from the client. When every
/// field is `None`/`false` this short-circuits to a plain SELECT — no empty-SET
/// SQL is synthesized. Called by `PUT /admin/db/{db}/webhooks/{id}`.
#[allow(clippy::too_many_arguments)]
pub async fn edit_webhook(
    pool: &PgPool,
    id: i64,
    db: &str,
    url: Option<&str>,
    tbl: Option<Option<&str>>,
    events: Option<&[String]>,
    enabled: Option<bool>,
    rotate_secret: bool,
) -> Result<Option<Webhook>, RtDbError> {
    type Row = (
        i64,
        String,
        Option<String>,
        String,
        Vec<String>,
        i64,
        bool,
        Option<String>,
    );
    // No fields → just read the current row.
    if url.is_none() && tbl.is_none() && events.is_none() && enabled.is_none() && !rotate_secret {
        let row: Option<Row> = sqlx::query_as(
            "SELECT id, db, tbl, url, events, created_at, enabled, secret \
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
            secret: r.7,
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
        need_comma = true;
    }
    if rotate_secret {
        if need_comma {
            q.push(", ");
        }
        q.push("secret = ");
        q.push_bind(generate_secret());
    }
    q.push(" WHERE id = ");
    q.push_bind(id);
    q.push(" AND db = ");
    q.push_bind(db);
    q.push(" RETURNING id, db, tbl, url, events, created_at, enabled, secret");
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
        secret: r.7,
    }))
}

/// Backfills `secret` for any webhooks with a NULL value (legacy rows from
/// before SEC-115). Called once at boot after `ensure_webhooks_tables` so every
/// webhook has a signing secret before the delivery worker ever drains. Each
/// generated secret is 256 bits via [`generate_secret`]. Best-effort: a per-row
/// UPDATE failure is logged and the row is skipped (it will deliver unsigned
/// until a later boot or a `rotateSecret` edit regenerates it).
pub async fn backfill_webhook_secrets(pool: &PgPool) -> Result<(), RtDbError> {
    let ids: Vec<(i64,)> =
        sqlx::query_as("SELECT id FROM rtdb.webhooks WHERE secret IS NULL ORDER BY id")
            .fetch_all(pool)
            .await?;
    for (id,) in ids {
        let secret = generate_secret();
        if let Err(err) = sqlx::query("UPDATE rtdb.webhooks SET secret = $2 WHERE id = $1")
            .bind(id)
            .bind(&secret)
            .execute(pool)
            .await
        {
            tracing::warn!(webhook_id = id, error = %err, "webhook: backfill secret failed");
        }
    }
    Ok(())
}

/// Reads delivery rows for `webhook_id` (which must belong to `db`) newest-first
/// (by `next_attempt DESC` then `id DESC` as a stable tie-breaker). `status`
/// filters when `Some`; `limit`/`offset` page. The `db` scope joins to
/// `rtdb.webhooks` (the deliveries table has no `db` column of its own) so a
/// delivery listing can't be reached by guessing another database's webhook id
/// (SEC-134) — the same `id AND db` guarantee `edit_webhook` provides. Called by
/// `GET /admin/db/{db}/webhooks/{id}/deliveries`.
pub async fn fetch_deliveries(
    pool: &PgPool,
    webhook_id: i64,
    db: &str,
    status: Option<&str>,
    limit: i64,
    offset: i64,
) -> Result<Vec<DeliveryRow>, RtDbError> {
    type Row = (i64, i32, String, i64, Option<String>, serde_json::Value);
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT d.id, d.attempts, d.status, d.next_attempt, d.last_error, d.payload \
         FROM rtdb.webhook_deliveries d \
         JOIN rtdb.webhooks w ON w.id = d.webhook_id \
         WHERE d.webhook_id = $1 AND w.db = $2 AND ($3::text IS NULL OR d.status = $3) \
         ORDER BY d.next_attempt DESC, d.id DESC \
         LIMIT $4 OFFSET $5",
    )
    .bind(webhook_id)
    .bind(db)
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

    // ===================== SEC-001 URL validator =====================
    // Pure IP-range denylist checks first (no network). DNS resolution is
    // exercised in production code; the unit tests use IP literals and the
    // dev escape hatch to avoid flakiness from real lookups.

    #[test]
    fn is_blocked_ip_catches_loopback_private_linklocal_metadata() {
        use std::net::{Ipv4Addr, Ipv6Addr};
        // Loopback.
        assert!(is_blocked_ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))));
        // RFC1918 private ranges.
        assert!(is_blocked_ip(IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3))));
        assert!(is_blocked_ip(IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1))));
        assert!(is_blocked_ip(IpAddr::V4(Ipv4Addr::new(172, 31, 255, 255))));
        assert!(is_blocked_ip(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))));
        // Link-local — and the cloud-metadata IP specifically.
        assert!(is_blocked_ip(IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254))));
        assert!(is_blocked_ip(IpAddr::V4(Ipv4Addr::new(169, 254, 0, 1))));
        // Multicast + broadcast + unspecified-source.
        assert!(is_blocked_ip(IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1))));
        assert!(is_blocked_ip(IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255))));
        assert!(is_blocked_ip(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0))));
        // IPv6 loopback / unspecified / link-local / ULA / multicast.
        assert!(is_blocked_ip(IpAddr::V6(Ipv6Addr::LOCALHOST)));
        assert!(is_blocked_ip(IpAddr::V6(Ipv6Addr::UNSPECIFIED)));
        assert!(is_blocked_ip("fe80::1".parse().unwrap()));
        assert!(is_blocked_ip("fc00::1".parse().unwrap()));
        assert!(is_blocked_ip("fd00::1".parse().unwrap()));
        assert!(is_blocked_ip("ff02::1".parse().unwrap()));
    }

    #[test]
    fn is_blocked_ip_catches_ipv4_mapped_and_compat_forms() {
        // `[::ffff:a.b.c.d]` parses as an Ipv6 host, so the V4 table has to be
        // consulted through the mapping or the whole denylist is bypassable.
        assert!(is_blocked_ip("::ffff:127.0.0.1".parse().unwrap()));
        assert!(is_blocked_ip("::ffff:169.254.169.254".parse().unwrap()));
        assert!(is_blocked_ip("::ffff:10.0.0.1".parse().unwrap()));
        assert!(is_blocked_ip("::ffff:192.168.1.1".parse().unwrap()));
        assert!(is_blocked_ip("::ffff:172.16.0.1".parse().unwrap()));
        // A public address stays reachable through the mapped spelling.
        assert!(!is_blocked_ip("::ffff:8.8.8.8".parse().unwrap()));
        // Deprecated `::a.b.c.d` compat form resolves through the V4 table too.
        assert!(is_blocked_ip("::10.0.0.1".parse().unwrap()));
        assert!(is_blocked_ip("::169.254.169.254".parse().unwrap()));
        // The compat conversion must not un-block ::1 / :: (they map to
        // 0.0.0.1 / 0.0.0.0, and 0.0.0.1 is not in the V4 table).
        assert!(is_blocked_ip("::1".parse().unwrap()));
        assert!(is_blocked_ip("::".parse().unwrap()));
    }

    #[test]
    fn is_blocked_ip_allows_public_addresses() {
        use std::net::Ipv4Addr;
        // A documented public DNS resolver (8.8.8.8) and a generic 203.0.113/24
        // (TEST-NET-3) address are both non-private.
        assert!(!is_blocked_ip(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))));
        assert!(!is_blocked_ip(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10))));
        assert!(!is_blocked_ip("2606:4700:4700::1111".parse().unwrap()));
    }

    #[tokio::test]
    async fn validate_rejects_http_scheme_by_default() {
        let err = validate_webhook_url("http://example.com/hook", false)
            .await
            .expect_err("http rejected when allow_http=false");
        assert!(
            err.contains("https") && err.contains("RTDB_WEBHOOK_ALLOW_HTTP"),
            "error should name the scheme rule and the dev flag: {err}"
        );
    }

    #[tokio::test]
    async fn validate_rejects_bare_schemeless_url() {
        let err = validate_webhook_url("example.com/hook", false)
            .await
            .expect_err("schemeless url is invalid");
        // url::Url treats `example.com/...` as a relative path with no scheme.
        assert!(
            err.contains("invalid URL") || err.contains("scheme"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn validate_rejects_embedded_credentials() {
        let err = validate_webhook_url("https://user:pass@example.com/hook", false)
            .await
            .expect_err("embedded credentials are rejected");
        assert!(err.contains("credentials"), "got: {err}");
    }

    #[tokio::test]
    async fn validate_rejects_ip_literal_in_blocked_range() {
        // Loopback, private, link-local, and the metadata IP all rejected
        // regardless of port/path. No DNS round-trip needed.
        let cases = [
            "https://127.0.0.1/hook",
            "https://10.0.0.1:8443/hook",
            "https://192.168.1.1/hook",
            "https://169.254.169.254/latest/meta-data/",
            "https://[::1]/hook",
            "https://[fe80::1]/hook",
            "https://[::ffff:169.254.169.254]/latest/meta-data/",
            "https://[::ffff:127.0.0.1]/hook",
            "https://[::ffff:10.0.0.1]/hook",
        ];
        for url in cases {
            let err = validate_webhook_url(url, false).await.unwrap_err();
            assert!(
                err.contains("blocked") || err.contains("metadata"),
                "url {url}: expected blocked-range error, got {err}"
            );
        }
    }

    #[tokio::test]
    async fn validate_rejects_known_metadata_hostnames() {
        // The textual cloud-metadata hostnames are rejected before any DNS
        // lookup, so the test is fast and network-independent.
        for host in ["metadata.google.internal", "169.254.169.254"] {
            let url = format!("https://{host}/");
            let err = validate_webhook_url(&url, false).await.unwrap_err();
            assert!(err.contains("metadata"), "{url}: got {err}");
        }
    }

    #[tokio::test]
    async fn validate_allow_http_opens_dev_hatch_for_loopback() {
        // With the dev flag on, http + 127.0.0.1 is permitted — this is the
        // path the integration test's local axum receiver exercises.
        validate_webhook_url("http://127.0.0.1:9999/hook", true)
            .await
            .expect("dev hatch permits local http");
        // The https requirement is also relaxed.
        validate_webhook_url("http://example.com/hook", true)
            .await
            .expect("dev hatch permits http scheme");
    }

    #[tokio::test]
    async fn validate_allow_http_still_requires_a_real_scheme_and_host() {
        // The dev flag is not a free pass for junk: an empty host or unknown
        // scheme must still fail.
        assert!(
            validate_webhook_url("ftp://example.com/hook", true)
                .await
                .is_err()
        );
        assert!(validate_webhook_url("http://", true).await.is_err());
    }

    #[tokio::test]
    async fn validate_accepts_https_ip_literal_in_public_range() {
        // A public IP literal with https must pass the literal check (no DNS).
        validate_webhook_url("https://203.0.113.10/hook", false)
            .await
            .expect("public IP literal with https is allowed");
    }

    // ===================== SEC-115 delivery signing =====================

    #[test]
    fn generate_secret_is_64_hex_chars_and_unique() {
        let a = generate_secret();
        let b = generate_secret();
        // 256 bits → 32 bytes → 64 hex chars.
        assert_eq!(a.len(), 64, "secret is 64 hex chars: {a}");
        assert!(
            a.chars().all(|c| c.is_ascii_hexdigit()),
            "secret is hex: {a}"
        );
        // Two consecutive draws differ (CSPRNG; collision over 256 bits is
        // cryptographically infeasible — a repeat here means OsRng is broken).
        assert_ne!(a, b, "two generated secrets must differ");
    }

    #[test]
    fn signature_roundtrips_and_rejects_tamper() {
        let secret = "whsec-abc";
        let body = br#"{"db":"x","table":"t","docId":"d1","kind":"insert","ts":1}"#;
        let ts = 1_700_000_000_000_i64;
        let sig = compute_signature(secret, ts, body);
        // Verifies against the same inputs.
        assert!(
            verify_signature(secret, ts, body, &sig),
            "signature verifies against the same inputs"
        );
        // Hex tag is 64 chars (SHA-256 → 32 bytes → 64 hex).
        assert_eq!(sig.len(), 64);
        // Tampered body fails.
        assert!(!verify_signature(secret, ts, b"different", &sig));
        // Tampered timestamp fails.
        assert!(!verify_signature(secret, ts + 1, body, &sig));
        // Different secret fails.
        assert!(!verify_signature("whsec-other", ts, body, &sig));
        // Non-hex signature fails (constant-time path short-circuits on
        // malformed input — reveals only "malformed", not a near-miss).
        assert!(!verify_signature(secret, ts, body, "not-hex!!"));
    }
}
