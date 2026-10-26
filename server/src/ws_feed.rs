//! WebSocket push channel over the durable change feed (`feedSubscribe`).
//!
//! The channel reads straight from the durable `changes` log (the same rows
//! `GET /api/db/{db}/changes` serves), NOT from the in-memory op-feed ring:
//! the ring is per-process, lossy, and resets on restart, so it cannot honor a
//! resume cursor. The op-feed broadcast is used only as a wake-up signal (it
//! also carries peer-replica writes under `RTDB_MULTI_INSTANCE`), with a
//! one-second poll as the backstop. Replay is therefore bounded by the log's
//! retention window (`RTDB_CHANGE_LOG_MAX_ROWS`); a cursor outside it is a
//! typed `CURSOR_EXPIRED`, identical to the HTTP feed.
//!
//! Delivery is at-least-once: batches are sent oldest-first and the cursor is
//! never rolled back, but a reconnecting consumer resumes from its last
//! *processed* seq, so ops it received but never acked replay. Dedupe on
//! `(feedEpoch, seq)`. The channel is read-side only: no write path.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{Mutex, broadcast, watch};
use tokio::task::JoinHandle;

use crate::AppState;
use crate::auth::{Principal, authorize, authorize_table};
use crate::change_log::{self, ChangeRow};
use crate::error::{ErrorCode, RtDbError};
use crate::protocol::ServerMessage;

/// Ops per `feedEvents` frame.
const BATCH: i64 = 500;
/// Unacked ops allowed in flight before the sender pauses for a `feedAck`.
const WINDOW: i64 = 4 * BATCH;
/// Backstop poll when no op-feed wake-up arrives.
const POLL: Duration = Duration::from_secs(1);

pub(crate) struct FeedHandle {
    ack: watch::Sender<i64>,
    task: JoinHandle<()>,
}

/// Per-connection registry of live feeds, keyed by `feedId`.
pub(crate) type Feeds = Arc<Mutex<HashMap<String, FeedHandle>>>;

pub(crate) fn new_registry() -> Feeds {
    Arc::new(Mutex::new(HashMap::new()))
}

/// Stops every feed of a closing connection.
pub(crate) async fn shutdown(feeds: &Feeds) {
    for (_, handle) in feeds.lock().await.drain() {
        handle.task.abort();
    }
}

pub(crate) async fn unsubscribe(feeds: &Feeds, feed_id: &str) {
    if let Some(handle) = feeds.lock().await.remove(feed_id) {
        handle.task.abort();
    }
}

pub(crate) async fn ack(feeds: &Feeds, feed_id: &str, seq: i64) {
    if let Some(handle) = feeds.lock().await.get(feed_id) {
        handle.ack.send_if_modified(|cur| {
            if seq > *cur {
                *cur = seq;
                true
            } else {
                false
            }
        });
    }
}

pub(crate) struct Subscribe {
    pub feed_id: String,
    pub since: Option<i64>,
    pub feed_epoch: Option<String>,
    pub tables: Vec<String>,
}

/// Starts (or restarts) one feed. Machine tokens only, like the HTTP feed.
pub(crate) async fn subscribe(
    feeds: &Feeds,
    state: &Arc<AppState>,
    principal: &Principal,
    db: &str,
    out_tx: &UnboundedSender<ServerMessage>,
    req: Subscribe,
) {
    let Principal::Machine { .. } = principal else {
        let _ = out_tx.send(ServerMessage::FeedErr {
            feed_id: req.feed_id,
            error: RtDbError::forbidden("change feed requires a machine token"),
        });
        return;
    };
    let (ack_tx, ack_rx) = watch::channel(i64::MIN);
    let feed_id = req.feed_id.clone();
    let task = tokio::spawn(run_feed(
        state.clone(),
        principal.clone(),
        db.to_string(),
        out_tx.clone(),
        ack_rx,
        req,
    ));
    let old = feeds
        .lock()
        .await
        .insert(feed_id, FeedHandle { ack: ack_tx, task });
    if let Some(old) = old {
        old.task.abort();
    }
}

fn expired(msg: String) -> RtDbError {
    RtDbError::new(ErrorCode::CursorExpired, msg)
}

async fn run_feed(
    state: Arc<AppState>,
    principal: Principal,
    db: String,
    out_tx: UnboundedSender<ServerMessage>,
    ack_rx: watch::Receiver<i64>,
    req: Subscribe,
) {
    let feed_id = req.feed_id.clone();
    if let Err(error) = stream(&state, &principal, &db, &out_tx, ack_rx, req).await {
        let _ = out_tx.send(ServerMessage::FeedErr { feed_id, error });
    }
}

async fn stream(
    state: &Arc<AppState>,
    principal: &Principal,
    db: &str,
    out_tx: &UnboundedSender<ServerMessage>,
    mut ack_rx: watch::Receiver<i64>,
    req: Subscribe,
) -> Result<(), RtDbError> {
    let ctx = principal.row_ctx();
    // Requested filter intersected with the token's allowlist; a request
    // naming only forbidden tables must not widen to "all tables".
    let tables: Vec<String> = req.tables.clone();
    let scoped = match &ctx.tables {
        Some(list) if !list.is_empty() => Some(list.clone()),
        _ => None,
    };
    let effective: Vec<String> = match (&scoped, tables.is_empty()) {
        (Some(list), true) => list.clone(),
        (Some(list), false) => tables.into_iter().filter(|t| list.contains(t)).collect(),
        (None, _) => tables,
    };
    let nothing_allowed = !req.tables.is_empty() && effective.is_empty();

    change_log::ensure_table(&state.pool, db).await?;
    let mut wake = state.realtime.op_feed.subscribe();
    let head = change_log::head(&state.pool, db).await?;
    if let Some(epoch) = &req.feed_epoch
        && *epoch != head.log_id
    {
        return Err(expired(format!(
            "feedEpoch {epoch} does not match the log ({}) — the database was replaced; resync",
            head.log_id
        )));
    }
    let mut cursor = req.since.unwrap_or(head.seq);
    if cursor > head.seq {
        return Err(expired(format!(
            "cursor {cursor} is ahead of the log (head {}) — resync from 0",
            head.seq
        )));
    }
    let base = cursor;
    let mut sent_through = cursor;
    let mut confirmed = false;
    loop {
        // Backpressure: wait for the consumer to ack before sending more.
        while sent_through - (*ack_rx.borrow()).max(base) > WINDOW {
            if ack_rx.changed().await.is_err() {
                return Ok(());
            }
        }
        authorize(&state.pool, principal, db).await?;
        // Read BEFORE the page: every op <= head_now is then visible to the
        // page read, so a consumer may treat `head` as a safe resume cursor
        // when a (non-full) frame carries no ops.
        let head_now = change_log::head(&state.pool, db).await?;
        let page = if nothing_allowed {
            change_log::Page {
                ops: Vec::new(),
                min_seq: change_log::min_seq(&state.pool, db).await?,
            }
        } else {
            change_log::read_page_tables(&state.pool, db, cursor, &effective, BATCH).await?
        };
        if let Some(min) = page.min_seq
            && cursor + 1 < min
        {
            return Err(expired(format!(
                "cursor {cursor} predates the retention window (oldest retained seq {min}) — resync from 0"
            )));
        }
        let full = page.ops.len() as i64 == BATCH;
        let last = page.ops.last().map(|op| op.seq);
        let ops: Vec<ChangeRow> = page
            .ops
            .into_iter()
            .filter(|op| authorize_table(&ctx, &op.table).is_ok())
            .collect();
        let ops_last = ops.last().map(|op| op.seq);
        if !ops.is_empty() || !confirmed {
            let frame = ServerMessage::FeedEvents {
                feed_id: req.feed_id.clone(),
                feed_epoch: head_now.log_id,
                ops,
                head: head_now.seq,
            };
            if let Some(op) = ops_last {
                sent_through = op;
            }
            if out_tx.send(frame).is_err() {
                return Ok(());
            }
            confirmed = true;
        }
        if let Some(last) = last {
            cursor = last;
        }
        if full {
            continue;
        }
        tokio::select! {
            r = wake.recv() => {
                if matches!(r, Err(broadcast::error::RecvError::Closed)) {
                    tokio::time::sleep(POLL).await;
                }
            }
            _ = tokio::time::sleep(POLL) => {}
        }
    }
}
