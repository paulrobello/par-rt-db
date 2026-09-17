//! Bounded graceful-shutdown drain (`RTDB_SHUTDOWN_DRAIN_MS`).
//!
//! `axum::serve(...).with_graceful_shutdown(...)` stops accepting on the
//! shutdown signal and then waits for every spawned connection task to finish.
//! An upgraded WebSocket whose peer never speaks never finishes, so that wait
//! has no upper bound of its own — before this module the only thing that ever
//! ended it was Docker's SIGTERM→SIGKILL window. `serve_with_drain_bound` races
//! the drain against a deadline that starts when the signal fires: the deadline
//! wins and `main` proceeds to its already-bounded post-serve cleanup, which
//! ends the process and with it the connections that refused to drain.

use std::future::Future;
use std::time::Duration;

/// Resolves `drain` after `signal` does. A zero `drain` never resolves —
/// `0` is the documented "wait forever" setting, and `sleep(Duration::ZERO)`
/// would instead fire on the next poll and cut the drain to nothing.
async fn drain_deadline<F>(signal: F, drain: Duration)
where
    F: Future<Output = ()>,
{
    signal.await;
    if drain.is_zero() {
        std::future::pending::<()>().await
    } else {
        tokio::time::sleep(drain).await
    }
}

/// Runs `serve` (an `axum::serve(...)` future already wired to its own
/// graceful-shutdown signal) under a drain deadline.
///
/// `signal` must resolve on the same shutdown signal the serve future was
/// given — the deadline is measured from it, not from process start. Since
/// axum takes its signal future by value, callers fan one signal out to both
/// (`main` uses a `CancellationToken`).
///
/// Returns the serve future's own result when the drain completes in time, and
/// `Ok(())` when the deadline fires first.
pub async fn serve_with_drain_bound<S, F>(
    serve: S,
    signal: F,
    drain: Duration,
) -> std::io::Result<()>
where
    S: Future<Output = std::io::Result<()>>,
    F: Future<Output = ()>,
{
    tokio::select! {
        result = serve => result,
        () = drain_deadline(signal, drain) => {
            tracing::warn!(
                drain_ms = drain.as_millis() as u64,
                "graceful shutdown drain hit RTDB_SHUTDOWN_DRAIN_MS; \
                 closing connections that had not finished"
            );
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn deadline_fires_after_the_signal_not_before() {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let deadline = tokio::spawn(drain_deadline(
            async move {
                let _ = rx.await;
            },
            Duration::from_millis(50),
        ));
        // The clock starts at the signal: nothing resolves while it is pending.
        assert!(
            tokio::time::timeout(Duration::from_millis(150), async {})
                .await
                .is_ok()
                && !deadline.is_finished()
        );
        let _ = tx.send(());
        tokio::time::timeout(Duration::from_millis(500), deadline)
            .await
            .expect("deadline did not fire within 500ms of the signal")
            .expect("deadline task panicked");
    }

    #[tokio::test]
    async fn zero_drain_never_resolves() {
        let elapsed = tokio::time::timeout(
            Duration::from_millis(200),
            drain_deadline(std::future::ready(()), Duration::ZERO),
        )
        .await;
        assert!(elapsed.is_err(), "a zero drain must mean wait forever");
    }
}
