//! The admin view of the durable change feed: `GET /admin/db/{db}/changes`
//! — the same retained log the data-plane route serves, under the admin
//! control plane so the dashboard can browse document history without a
//! separate machine token. Auth is the shared admin gate at the router layer
//! (`require_admin_mw`: admin-key bearer or dashboard-cookie session); no
//! additional per-table filtering applies — the admin gate is the
//! authorization boundary and it grants the whole database.

use serde::Deserialize;
use std::sync::Arc;

use axum::Json;
use axum::extract::Query as AxumQuery;
use axum::extract::{Path, State};

use crate::error::RtDbError;
use crate::protocol::ChangeFeedResponse;
use crate::{AppState, db};

/// Query params — same shape and clamps as the data-plane
/// `GET /api/db/{db}/changes`.
#[derive(Debug, Deserialize)]
pub(super) struct AdminChangesQuery {
    /// Return ops with `seq` strictly greater than this (default 0 = the
    /// oldest retained row).
    since: Option<i64>,
    /// Restrict the page to one table. `nextSeq`/`head` stay global.
    table: Option<String>,
    /// Page size (default 500, clamped to 1000).
    limit: Option<i64>,
}

/// `GET /admin/db/{db}/changes?since=&table=&limit=` — the retained per-db
/// change feed, admin-authenticated. Cursor semantics are byte-identical to
/// the data-plane handler: an impossible cursor (older than retention, or
/// ahead of the log) is a typed `CURSOR_EXPIRED`, never an empty success, and
/// the response carries the log's `logId` identity so a consumer can detect a
/// database dropped and recreated under the same name. Unlike the data-plane
/// route there is no per-table allowlist filter — the admin gate is the
/// authorization boundary and it grants the whole database.
pub(super) async fn admin_changes(
    State(state): State<Arc<AppState>>,
    Path(db): Path<String>,
    AxumQuery(query): AxumQuery<AdminChangesQuery>,
) -> Result<Json<ChangeFeedResponse>, RtDbError> {
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
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
    // No per-table allowlist filter here: the admin gate already grants the
    // whole database, so `nextSeq` follows the same full-page rule directly
    // on the stored rows.
    let next_seq = if page.ops.len() == limit as usize {
        // A full page always has rows; fall back to head defensively rather
        // than panicking the handler.
        page.ops.last().map(|op| op.seq).unwrap_or(head.seq)
    } else {
        head.seq
    };
    Ok(Json(ChangeFeedResponse {
        ops: page.ops,
        next_seq,
        head: head.seq,
        log_id: head.log_id,
    }))
}
