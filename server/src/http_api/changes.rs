//! The durable per-db change feed: `GET /api/db/{db}/changes`
//! (ARC-008 split; pure move from the former single-file `http_api.rs`).

use serde::Deserialize;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};

use crate::AppState;
use crate::auth::Principal;
use crate::error::RtDbError;
use crate::http_api::authed;
use crate::rate_limit::check_http_rate_limits;
use axum::extract::Query as AxumQuery;
use axum::http::HeaderMap;

/// GET /api/db/{db}/schema — the pushed `SchemaDef`, authorized with the same
/// per-db bearer the query/mutate routes accept (machine token or session).
/// Field metadata only — no document bytes — so a table-scoped token may read
/// the full def; the tables it cannot touch are already enforced per step.
/// Query params for `GET /api/db/{db}/changes`.
#[derive(Debug, Deserialize)]
pub(crate) struct ChangesQuery {
    /// Return ops with `seq` strictly greater than this (default 0 = the
    /// oldest retained row).
    since: Option<i64>,
    /// Restrict the page to one table. `nextSeq`/`head` stay global.
    table: Option<String>,
    /// Page size (default 500, clamped to 1000).
    limit: Option<i64>,
}

/// `GET /api/db/{db}/changes?since=&table=&limit=` — the resumable per-db
/// change feed. Machine tokens only (the feed's consumer is HTTP-only
/// machines/agents; user-principal per-row filtering is future work). Op
/// filtering uses the same `authorize_table` gate as every other executor
/// surface — `tables: None` AND `Some([])` mean unrestricted. An impossible
/// cursor (older than retention, or ahead of the log) is a typed
/// `CURSOR_EXPIRED`, never an empty success — see the design spec.
pub(crate) async fn changes_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(db): Path<String>,
    AxumQuery(query): AxumQuery<ChangesQuery>,
) -> Result<Json<crate::protocol::ChangeFeedResponse>, RtDbError> {
    let principal = authed(&state, &headers, &db).await?;
    let Principal::Machine { .. } = &principal else {
        return Err(RtDbError::forbidden("change feed requires a machine token"));
    };
    check_http_rate_limits(&state, &principal, &db).await?;
    // Idempotent: covers old dbs and replicas whose committer never ran —
    // a read must never 500 on a missing table, and must seed the head row.
    crate::change_log::ensure_table(&state.pool, &db).await?;
    let head = crate::change_log::head(&state.pool, &db).await?;
    let since = query.since.unwrap_or(0);
    if since > head.seq {
        return Err(RtDbError::new(
            crate::error::ErrorCode::CursorExpired,
            format!(
                "cursor {since} is ahead of the log (head {}) — the database was replaced or rewound; resync from 0",
                head.seq
            ),
        ));
    }
    let limit = query.limit.unwrap_or(500).clamp(1, 1000);
    let page = crate::change_log::read_page(&state.pool, &db, since, query.table.as_deref(), limit)
        .await?;
    // Expired test BEFORE the table filter — a filtered page can be short
    // while trimmed rows sit between the cursor and the window.
    if let Some(min) = page.min_seq
        && since + 1 < min
    {
        return Err(RtDbError::new(
            crate::error::ErrorCode::CursorExpired,
            format!(
                "cursor {since} predates the retention window (oldest retained seq {min}) — resync from 0"
            ),
        ));
    }
    // Same table-allowlist gate the write path and query executor use.
    let principal_ctx = principal.row_ctx();
    let ops: Vec<_> = page
        .ops
        .into_iter()
        .filter(|op| crate::auth::authorize_table(&principal_ctx, &op.table).is_ok())
        .collect();
    let next_seq = if ops.len() == limit as usize {
        // A full page always has rows; fall back to head defensively rather
        // than panicking the handler.
        ops.last().map(|op| op.seq).unwrap_or(head.seq)
    } else {
        head.seq
    };
    Ok(Json(crate::protocol::ChangeFeedResponse {
        ops,
        next_seq,
        head: head.seq,
        log_id: head.log_id,
    }))
}
