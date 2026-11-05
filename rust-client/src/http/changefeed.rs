use super::RtDbHttpClient;
use crate::error::{ErrorCode, RtDbError};
use crate::http_common::AuthedRequest;
use crate::wire::ChangeOp;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

impl RtDbHttpClient {
    /// Poll the durable per-db change feed via
    /// `GET /api/db/{db}/changes` — every committed document op strictly
    /// after `since`, oldest first, with post-image payloads. Machine-token
    /// scoped; a cursor older than the server's retention window (or ahead
    /// of the log) errors with `ErrorCode::CursorExpired` — resync from 0.
    /// Loop `while resp.next_seq < resp.head`; re-reading a cursor is
    /// idempotent. `table` restricts the page to one table (`next_seq`/`head`
    /// stay global); `limit` is the page size (server clamps to 1000).
    pub async fn changes(
        &self,
        since: i64,
        table: Option<&str>,
        limit: Option<i64>,
    ) -> Result<crate::wire::ChangeFeedResponse, RtDbError> {
        let mut req = self
            .client
            .get(format!("{}/api/db/{}/changes", self.url, self.db))
            .query(&[("since", since.to_string())])
            .authed(&self.token);
        if let Some(table) = table {
            req = req.query(&[("table", table)]);
        }
        if let Some(limit) = limit {
            req = req.query(&[("limit", limit.to_string())]);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("changes request failed: {e}")))?;
        crate::http_common::deserialize::<crate::wire::ChangeFeedResponse>(resp).await
    }

    /// Opens an auto-resuming stream over the durable per-db change feed
    /// (machine tokens only). Each [`next`](ChangeFeedStream::next) call
    /// yields one committed op and advances the resume cursor
    /// ([`cursor`](ChangeFeedStream::cursor)) to the last yielded op's seq;
    /// a failed poll retries from that cursor on a jittered exponential
    /// backoff, and an empty page waits `poll_interval` before polling
    /// again. `since` (default 0) is the starting cursor — pass the value
    /// persisted by a previous run to resume after a restart. Stop via
    /// [`stop`](ChangeFeedStream::stop); `ErrorCode::CursorExpired` and a
    /// mid-stream `log_id` change are terminal — the error is returned once,
    /// then the stream ends, mirroring `rtdb changes --follow` (never a
    /// silent resync).
    pub fn stream_changes(
        &self,
        since: i64,
        table: Option<&str>,
        limit: Option<i64>,
        opts: ChangeFeedStreamOptions,
    ) -> ChangeFeedStream {
        ChangeFeedStream {
            client: self.clone(),
            cursor: since,
            table: table.map(str::to_owned),
            limit,
            log_id: None,
            opts,
            stopped: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            notify: std::sync::Arc::new(tokio::sync::Notify::new()),
            pending: std::collections::VecDeque::new(),
            attempt: 0,
            terminal: false,
            terminal_err: None,
        }
    }
}

/// Tuning for [`RtDbHttpClient::stream_changes`]. Defaults mirror the
/// ts-client's `streamChanges`: a 1s caught-up poll interval and a
/// 500ms→15s jittered retry backoff.
#[derive(Debug, Clone)]
pub struct ChangeFeedStreamOptions {
    /// Wait after an empty page (the stream is caught up).
    pub poll_interval: Duration,
    /// Base for the jittered exponential retry backoff after a failed poll.
    pub backoff_base: Duration,
    /// Cap for the retry backoff.
    pub backoff_max: Duration,
}

impl Default for ChangeFeedStreamOptions {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
            backoff_base: Duration::from_millis(500),
            backoff_max: Duration::from_secs(15),
        }
    }
}

/// Jittered exponential backoff: `min(max, base * 2^attempt) * (0.5 + rand*0.5)`,
/// matching `ts-client` and `ws::backoff_delay`.
fn backoff_delay(base: Duration, max: Duration, attempt: u32) -> Duration {
    let exp = base.saturating_mul(2u32.saturating_pow(attempt.min(20)));
    let raw = exp.min(max);
    let frac = 0.5 + jitter_fraction() * 0.5;
    raw.mul_f64(frac)
}

/// Per-process pseudo-random fraction in `[0, 1)` sourced from the hasher of a
/// fresh `RandomState` — no external RNG dependency, sufficient for backoff jitter.
fn jitter_fraction() -> f64 {
    use std::hash::{BuildHasher, Hasher};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(n);
    (h.finish() >> 11) as f64 / ((1u64 << 53) as f64)
}

/// Auto-resuming handle over the durable per-db change feed
/// (`GET /api/db/{db}/changes`, machine tokens only), created by
/// [`RtDbHttpClient::stream_changes`]. Each [`next`](Self::next) call yields
/// one committed op in seq order and advances [`cursor`](Self::cursor) to
/// the last yielded op's seq — persist that value (the caller owns cursor
/// durability) and pass it back as `since` after a restart. A failed poll
/// retries from the same cursor on the jittered backoff, so an error never
/// skips or replays an op; an empty page waits `poll_interval`. The stream
/// ends (returns `None`) after [`stop`](Self::stop), after a terminal error
/// (`CursorExpired` or a `log_id` change — the db was replaced), or when
/// the page loop can no longer continue.
pub struct ChangeFeedStream {
    client: RtDbHttpClient,
    cursor: i64,
    table: Option<String>,
    limit: Option<i64>,
    log_id: Option<String>,
    opts: ChangeFeedStreamOptions,
    stopped: Arc<AtomicBool>,
    notify: Arc<Notify>,
    pending: VecDeque<ChangeOp>,
    attempt: u32,
    /// Set once a terminal error has been surfaced — the stream ends after
    /// handing it to the consumer.
    terminal: bool,
    terminal_err: Option<RtDbError>,
}

impl ChangeFeedStream {
    /// The resume cursor: the last op's seq handed to the consumer.
    pub fn cursor(&self) -> i64 {
        self.cursor
    }

    /// Ends the stream — a pending poll/backoff wait wakes and the next
    /// [`next`](Self::next) call returns `None`.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
        self.notify.notify_one();
    }

    /// Yields the next committed op, or `None` once the stream has ended.
    /// `Some(Err(..))` carries a failed poll exactly once before the stream
    /// ends; transient failures never surface here — they retry internally
    /// on the backoff.
    pub async fn next(&mut self) -> Option<Result<ChangeOp, RtDbError>> {
        loop {
            if self.terminal || self.stopped.load(Ordering::Relaxed) {
                return None;
            }
            if let Some(err) = self.terminal_err.take() {
                self.terminal = true;
                return Some(Err(err));
            }
            if let Some(op) = self.pending.pop_front() {
                self.cursor = op.seq;
                return Some(Ok(op));
            }
            match self
                .client
                .changes(self.cursor, self.table.as_deref(), self.limit)
                .await
            {
                Ok(page) => {
                    self.attempt = 0;
                    match &self.log_id {
                        None => self.log_id = Some(page.log_id.clone()),
                        Some(seen) if *seen != page.log_id => {
                            self.terminal_err = Some(RtDbError::internal(format!(
                                "change feed logId changed ({seen} -> {}): the database was dropped and recreated; resync from since 0",
                                page.log_id
                            )));
                            continue;
                        }
                        Some(_) => {}
                    }
                    if page.ops.is_empty() {
                        if !self.sleep(self.opts.poll_interval).await {
                            return None;
                        }
                    } else {
                        self.pending.extend(page.ops);
                    }
                }
                Err(e) if e.code == ErrorCode::CursorExpired => {
                    self.terminal_err = Some(e);
                }
                Err(_) => {
                    self.attempt += 1;
                    let delay =
                        backoff_delay(self.opts.backoff_base, self.opts.backoff_max, self.attempt);
                    if !self.sleep(delay).await {
                        return None;
                    }
                    continue;
                }
            }
        }
    }

    /// Waits `d`, returning `false` if woken early by [`stop`](Self::stop).
    async fn sleep(&self, d: Duration) -> bool {
        tokio::select! {
            _ = tokio::time::sleep(d) => true,
            _ = self.notify.notified() => false,
        }
    }
}
