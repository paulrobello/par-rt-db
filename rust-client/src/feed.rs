//! Durable change-feed push channel over the WebSocket (`feedSubscribe`).
//!
//! The client remembers the last delivered `(feedEpoch, seq)`. On every
//! (re)connect the driver re-sends `feedSubscribe` with that cursor, so the
//! server replays whatever the consumer has not seen (at-least-once). Replayed
//! ops with `seq <=` the cursor are dropped here, so the consumer observes each
//! `(feedEpoch, seq)` at most once per feed handle. A `feedErr` (notably
//! `CURSOR_EXPIRED`) is terminal and never retried: resync over the HTTP
//! `changes` path and open a new feed.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

use crate::error::RtDbError;
use crate::wire::{ChangeOp, ClientMessage, ServerMessage};

/// Options for [`RtDbClient::watch_changes`](crate::RtDbClient::watch_changes).
#[derive(Debug, Clone, Default)]
pub struct ChangeFeedOptions {
    /// Server-side table filter (empty = every table the token may see).
    pub tables: Vec<String>,
    /// Last processed seq; `None` starts at the log's current head.
    pub since: Option<i64>,
    /// Epoch (`logId`) the `since` cursor belongs to, if known.
    pub feed_epoch: Option<String>,
}

/// One delivered batch: ops are oldest first and strictly after the previous
/// batch's last seq.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangeBatch {
    /// The change log's identity; dedupe key with each op's `seq`.
    pub feed_epoch: String,
    /// The ops.
    pub ops: Vec<ChangeOp>,
    /// The log's end when the server read the batch.
    pub head: i64,
}

#[derive(Default)]
struct Cursor {
    since: Option<i64>,
    epoch: Option<String>,
}

pub(crate) struct FeedState {
    pub(crate) feed_id: String,
    tables: Vec<String>,
    cursor: Mutex<Cursor>,
    tx: mpsc::UnboundedSender<Result<ChangeBatch, RtDbError>>,
}

#[derive(Default)]
pub(crate) struct FeedMaps {
    by_id: HashMap<String, Arc<FeedState>>,
}

impl FeedMaps {
    pub(crate) fn insert(&mut self, state: Arc<FeedState>) {
        self.by_id.insert(state.feed_id.clone(), state);
    }

    pub(crate) fn remove(&mut self, feed_id: &str) {
        self.by_id.remove(feed_id);
    }

    /// The `feedSubscribe` frames a fresh session must send.
    pub(crate) fn replay_frames(&self) -> Vec<ClientMessage> {
        self.by_id.values().map(|s| s.subscribe_frame()).collect()
    }

    pub(crate) fn frame_for(&self, feed_id: &str) -> Option<ClientMessage> {
        self.by_id.get(feed_id).map(|s| s.subscribe_frame())
    }

    pub(crate) fn get(&self, feed_id: &str) -> Option<Arc<FeedState>> {
        self.by_id.get(feed_id).cloned()
    }
}

impl FeedState {
    pub(crate) fn new(
        feed_id: String,
        opts: ChangeFeedOptions,
    ) -> (
        Arc<Self>,
        mpsc::UnboundedReceiver<Result<ChangeBatch, RtDbError>>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        let state = Arc::new(Self {
            feed_id,
            tables: opts.tables,
            cursor: Mutex::new(Cursor {
                since: opts.since,
                epoch: opts.feed_epoch,
            }),
            tx,
        });
        (state, rx)
    }

    fn subscribe_frame(&self) -> ClientMessage {
        let cur = self.cursor.lock().unwrap_or_else(|p| p.into_inner());
        ClientMessage::FeedSubscribe {
            feed_id: self.feed_id.clone(),
            since: cur.since,
            feed_epoch: cur.epoch.clone(),
            tables: self.tables.clone(),
        }
    }

    /// Applies one `feedEvents` frame: dedupes against the cursor, delivers
    /// the remainder, and returns the seq to acknowledge (if any).
    pub(crate) fn on_events(&self, epoch: String, ops: Vec<ChangeOp>, head: i64) -> Option<i64> {
        let mut cur = self.cursor.lock().unwrap_or_else(|p| p.into_inner());
        let same_epoch = cur.epoch.as_deref().is_none_or(|e| e == epoch);
        let floor = if same_epoch { cur.since } else { None };
        let fresh: Vec<ChangeOp> = ops
            .into_iter()
            .filter(|op| floor.is_none_or(|f| op.seq > f))
            .collect();
        cur.epoch = Some(epoch.clone());
        let ack = fresh.last().map(|op| op.seq);
        // An empty batch means nothing matched up to `head`, so the head is a
        // safe resume point (the server read it before the page).
        cur.since = ack.or(Some(head.max(cur.since.unwrap_or(0))));
        drop(cur);
        if fresh.is_empty() {
            return None;
        }
        let _ = self.tx.send(Ok(ChangeBatch {
            feed_epoch: epoch,
            ops: fresh,
            head,
        }));
        ack
    }

    pub(crate) fn fail(&self, error: RtDbError) {
        let _ = self.tx.send(Err(error));
    }
}

/// Routes a feed server frame; returns the `(feed_id, seq)` ack to send.
pub(crate) fn on_server_message(
    maps: &Mutex<FeedMaps>,
    msg: ServerMessage,
) -> Option<(String, i64)> {
    match msg {
        ServerMessage::FeedEvents {
            feed_id,
            feed_epoch,
            ops,
            head,
        } => {
            let state = maps
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(&feed_id)?;
            let ack = state.on_events(feed_epoch, ops, head)?;
            Some((feed_id, ack))
        }
        ServerMessage::FeedErr { feed_id, error } => {
            let mut guard = maps.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(state) = guard.get(&feed_id) {
                state.fail(error);
            }
            guard.remove(&feed_id);
            None
        }
        _ => None,
    }
}

/// A live change-feed handle. Dropping it closes the feed.
pub struct ChangeFeed {
    rx: mpsc::UnboundedReceiver<Result<ChangeBatch, RtDbError>>,
    feed_id: String,
    on_drop: Box<dyn Fn() + Send + Sync>,
}

impl ChangeFeed {
    pub(crate) fn new(
        rx: mpsc::UnboundedReceiver<Result<ChangeBatch, RtDbError>>,
        feed_id: String,
        on_drop: Box<dyn Fn() + Send + Sync>,
    ) -> Self {
        Self {
            rx,
            feed_id,
            on_drop,
        }
    }

    /// The next batch. `Some(Err(e))` is terminal (`CURSOR_EXPIRED` ⇒ resync
    /// over HTTP, then open a new feed); `None` means the feed ended.
    pub async fn next(&mut self) -> Option<Result<ChangeBatch, RtDbError>> {
        self.rx.recv().await
    }
}

impl Drop for ChangeFeed {
    fn drop(&mut self) {
        (self.on_drop)();
    }
}

impl std::fmt::Debug for ChangeFeed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChangeFeed")
            .field("feed_id", &self.feed_id)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(seq: i64) -> ChangeOp {
        ChangeOp {
            seq,
            table: "t".into(),
            doc_id: format!("d{seq}"),
            kind: "insert".into(),
            doc: None,
            ts: 0,
        }
    }

    #[test]
    fn replayed_ops_are_deduped_and_cursor_resumes_with_epoch() {
        let (state, mut rx) = FeedState::new("f".into(), ChangeFeedOptions::default());
        assert_eq!(state.on_events("e1".into(), vec![op(1), op(2)], 2), Some(2));
        // Replay after a reconnect overlaps seq 2.
        assert_eq!(state.on_events("e1".into(), vec![op(2), op(3)], 3), Some(3));
        let first = rx.try_recv().expect("batch").expect("ok");
        assert_eq!(first.ops.len(), 2);
        let second = rx.try_recv().expect("batch").expect("ok");
        assert_eq!(
            second.ops.iter().map(|o| o.seq).collect::<Vec<_>>(),
            vec![3]
        );
        match state.subscribe_frame() {
            ClientMessage::FeedSubscribe {
                since, feed_epoch, ..
            } => {
                assert_eq!(since, Some(3));
                assert_eq!(feed_epoch.as_deref(), Some("e1"));
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn empty_confirmation_advances_cursor_to_head_without_delivering() {
        let (state, mut rx) = FeedState::new("f".into(), ChangeFeedOptions::default());
        assert_eq!(state.on_events("e1".into(), vec![], 9), None);
        assert!(rx.try_recv().is_err());
        match state.subscribe_frame() {
            ClientMessage::FeedSubscribe { since, .. } => assert_eq!(since, Some(9)),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn feed_err_is_delivered_once_and_unregisters_the_feed() {
        let maps = Mutex::new(FeedMaps::default());
        let (state, mut rx) = FeedState::new("f".into(), ChangeFeedOptions::default());
        maps.lock().unwrap().insert(state);
        let err = RtDbError::new(crate::error::ErrorCode::CursorExpired, "gone");
        let ack = on_server_message(
            &maps,
            ServerMessage::FeedErr {
                feed_id: "f".into(),
                error: err,
            },
        );
        assert!(ack.is_none());
        let got = rx.try_recv().expect("err").expect_err("terminal");
        assert_eq!(got.code, crate::error::ErrorCode::CursorExpired);
        assert!(maps.lock().unwrap().get("f").is_none());
    }
}
