//! Realtime document-activity feed. The committer publishes one `OpEvent` per
//! `DocOp` after each successful commit. A bounded ring replays recent events on
//! (re)connect; a `broadcast` channel fans live events to `/admin/stream`. Non-durable.
use std::collections::VecDeque;
use std::sync::Arc;

use serde::Serialize;
use tokio::sync::{Mutex, broadcast};

use crate::db::now_ms;
use crate::txn::{DocOp, OpKind};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OpEvent {
    pub db: String,
    pub table: String,
    pub doc_id: String,
    pub kind: OpKind,
    pub ts: i64,
    pub owner: Option<String>,
}

pub struct OpFeed {
    tx: broadcast::Sender<OpEvent>,
    ring: Mutex<VecDeque<OpEvent>>,
    ring_cap: usize,
}

impl OpFeed {
    pub fn new(broadcast_cap: usize, ring_cap: usize) -> Arc<Self> {
        let (tx, _rx) = broadcast::channel(broadcast_cap);
        Arc::new(Self {
            tx,
            ring: Mutex::new(VecDeque::with_capacity(ring_cap)),
            ring_cap,
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
            };
            push_event(&mut ring, self.ring_cap, &self.tx, event);
        }
    }

    /// Inject a single pre-built event into the ring + broadcast, WITHOUT
    /// stamping a new `ts` (the event carries its origin-instance timestamp).
    /// Used by the cross-instance NOTIFY listener (ENH-022 Stage 2) to replay an
    /// event a peer replica already published, preserving the original write's
    /// wall-clock time. Same ring/broadcast semantics as `publish` — this is the
    /// listener's single entry into the local feed; it performs no write and no
    /// committer interaction, so the single-writer invariant is intact.
    pub async fn publish_injected(&self, event: OpEvent) {
        let mut ring = self.ring.lock().await;
        push_event(&mut ring, self.ring_cap, &self.tx, event);
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

/// Shared ring-push + broadcast for `publish` and `publish_injected`. Evicts the
/// oldest entry when the ring is at `ring_cap`, then broadcasts to live
/// subscribers. A lagged receiver (slower than the broadcast capacity) drops the
/// event — that subscriber catches up via the ring on its next replay.
fn push_event(
    ring: &mut VecDeque<OpEvent>,
    ring_cap: usize,
    tx: &broadcast::Sender<OpEvent>,
    event: OpEvent,
) {
    if ring.len() >= ring_cap {
        ring.pop_front();
    }
    ring.push_back(event.clone());
    let _ = tx.send(event);
}
