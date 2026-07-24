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

    /// One `OpEvent` per `DocOp`. Ring is bounded (evicts oldest); `broadcast::send`
    /// is a no-op with no subscribers. Never fails the commit.
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
            if ring.len() >= self.ring_cap {
                ring.pop_front();
            }
            ring.push_back(event.clone());
            let _ = self.tx.send(event);
        }
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
