//! Realtime document-activity feed. The committer publishes one `OpEvent` per
//! `DocOp` after each successful commit. A bounded ring replays recent events on
//! (re)connect; a `broadcast` channel fans live events to `/admin/stream`. Non-durable.
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, broadcast};

use crate::db::now_ms;
use crate::txn::{DocOp, OpKind};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OpEvent {
    pub db: String,
    pub table: String,
    pub doc_id: String,
    pub kind: OpKind,
    pub ts: i64,
    pub owner: Option<String>,
    /// Monotonic per-feed sequence, assigned as the event enters this feed's
    /// ring (1-based). The ring replays on every (re)connect; a consumer that
    /// tracks the max `seq` seen per `feedEpoch` observes each event exactly
    /// once across replay + live windows, and a gap in `seq` means events the
    /// ring evicted or the broadcast dropped — never a reordering.
    pub seq: u64,
    /// The feed instance's identity (a boot-time UUID, minted once per
    /// [`OpFeed::new`]). An epoch change means the counter reset (server
    /// restart or feed recreation) — not dropped events. Dedup key:
    /// `(feedEpoch, seq)`.
    pub feed_epoch: String,
}

pub struct OpFeed {
    tx: broadcast::Sender<OpEvent>,
    ring: Mutex<VecDeque<OpEvent>>,
    ring_cap: usize,
    seq: AtomicU64,
    feed_epoch: String,
}

impl OpFeed {
    pub fn new(broadcast_cap: usize, ring_cap: usize) -> Arc<Self> {
        let (tx, _rx) = broadcast::channel(broadcast_cap);
        Arc::new(Self {
            tx,
            ring: Mutex::new(VecDeque::with_capacity(ring_cap)),
            ring_cap,
            seq: AtomicU64::new(0),
            feed_epoch: uuid::Uuid::now_v7().simple().to_string(),
        })
    }

    /// One `OpEvent` per `DocOp`. The feed is best-effort and non-durable: the ring
    /// is bounded (evicts oldest), and if a subscriber lags beyond the broadcast
    /// capacity `broadcast::send` returns an error that we ignore here — lagged
    /// events are dropped for that subscriber only. Never fails the commit.
    pub async fn publish(&self, db: &str, owner: Option<&str>, ops: &[DocOp]) {
        let ts = now_ms();
        let owner = owner.map(|s| s.to_string());
        let mut ring = self.ring.lock().await;
        for op in ops {
            let event = OpEvent {
                db: db.to_string(),
                table: op.table.clone(),
                doc_id: op.id.clone(),
                kind: op.kind,
                ts,
                owner: owner.clone(),
                // Stamped with this feed's (seq, epoch) by `push_event`.
                seq: 0,
                feed_epoch: String::new(),
            };
            push_event(
                &mut ring,
                self.ring_cap,
                &self.tx,
                &self.seq,
                &self.feed_epoch,
                event,
            );
        }
    }

    /// Inject a single pre-built event into the ring + broadcast, WITHOUT
    /// stamping a new `ts` (the event carries its origin-instance timestamp).
    /// Used by the cross-instance NOTIFY listener (ENH-022 Stage 2) to replay an
    /// event a peer replica already published, preserving the original write's
    /// wall-clock time. Same ring/broadcast semantics as `publish` — this is the
    /// listener's single entry into the local feed; it performs no write and no
    /// committer interaction, so the single-writer invariant is intact.
    /// The event is re-stamped into THIS feed's `(feedEpoch, seq)` series (the
    /// origin `ts` above is what survives from the peer), so every consumer of
    /// this feed sees one monotonic sequence regardless of which replica
    /// committed the write.
    pub async fn publish_injected(&self, event: OpEvent) {
        let mut ring = self.ring.lock().await;
        push_event(
            &mut ring,
            self.ring_cap,
            &self.tx,
            &self.seq,
            &self.feed_epoch,
            event,
        );
    }

    /// Recent events (oldest-first), filtered by optional db/table, capped at `n`.
    pub async fn recent(&self, db: Option<&str>, table: Option<&str>, n: usize) -> Vec<OpEvent> {
        let ring = self.ring.lock().await;
        ring.iter()
            .rev()
            .filter(|e| db.is_none_or(|d| e.db == d))
            .filter(|e| table.is_none_or(|t| e.table == t))
            .take(n)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<OpEvent> {
        self.tx.subscribe()
    }
}

/// Shared ring-push + broadcast for `publish` and `publish_injected`. Stamps the
/// feed-local `(seq, feedEpoch)` onto the event — including re-stamping an
/// injected peer event — then evicts the oldest entry when the ring is at
/// `ring_cap` and broadcasts to live subscribers. A lagged receiver (slower than
/// the broadcast capacity) drops the event — that subscriber catches up via the
/// ring on its next replay (the drop reads as a `seq` gap).
fn push_event(
    ring: &mut VecDeque<OpEvent>,
    ring_cap: usize,
    tx: &broadcast::Sender<OpEvent>,
    seq: &AtomicU64,
    feed_epoch: &str,
    mut event: OpEvent,
) {
    event.seq = seq.fetch_add(1, Ordering::Relaxed) + 1;
    event.feed_epoch = feed_epoch.to_string();
    if ring.len() >= ring_cap {
        ring.pop_front();
    }
    ring.push_back(event.clone());
    let _ = tx.send(event);
}
