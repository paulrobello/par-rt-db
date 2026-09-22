//! Admin op feed: `ops watch`, the WebSocket tail of `/admin/stream`.
//!
//! The counterpart to `rtdb watch` on the admin plane. `watch` tails one live
//! query with a machine token; this tails every committed document op on the
//! instance with the admin key, and is the streaming twin of the one-shot
//! `GET /admin/ops/recent`.
//!
//! Output is NDJSON — one compact JSON line per op event on stdout — so the
//! tail pipes into `jq`/`while read`. Progress and reconnect notices go to
//! stderr so stdout stays pure.

use std::io::Write;

use anyhow::{Context, Result};
use par_rt_db_client::{
    AdminStreamFrame, Config, ErrorCode, RtDbAdminClient, RtDbError, admin::AdminStream,
};

use crate::args::Cli;
use crate::output::map_err;

use super::{admin_client, require_admin};

/// Render one frame as an output line, or `None` for a frame that is not an op.
///
/// The server interleaves a `gauges` metrics snapshot into the feed roughly
/// once a second whether or not anything is happening. Emitting those would put
/// a metrics blob per second between the op lines and break the NDJSON contract
/// this command exists to provide, so they are consumed and dropped; a frame
/// kind this build does not know is dropped the same way.
fn frame_line(frame: &AdminStreamFrame, pretty: bool) -> Option<String> {
    let AdminStreamFrame::Op { event } = frame else {
        return None;
    };
    let rendered = if pretty {
        serde_json::to_string_pretty(event)
    } else {
        serde_json::to_string(event)
    };
    rendered.ok()
}

/// Is this failure worth giving up on, rather than reconnecting through?
///
/// The `rtdb watch` counterpart is `data::watch_terminal_error`: a tail that
/// retries an auth rejection forever looks identical to a hang, so a rejected
/// credential has to stop the command. `/admin/stream` produces one at two
/// points — a 401/403 on the upgrade, and a mid-stream close `4401` when the
/// key is revoked under an already-open socket (SEC-006). Both arrive as the
/// same typed code, so one predicate covers them. Everything else — a dropped
/// socket, a restarting server, a timed-out dial — is transient by definition
/// and must keep the tail alive.
fn is_terminal(err: &RtDbError) -> bool {
    matches!(err.code, ErrorCode::Unauthorized | ErrorCode::Forbidden)
}

/// `rtdb ops watch` — tail the admin op feed until Ctrl-C.
pub(crate) async fn run_ops_watch(cli: &Cli, db: &Option<String>, pretty: bool) -> Result<()> {
    require_admin(cli)?;
    let client = admin_client(cli)?;
    match db {
        Some(db) => eprintln!("watching ops in {db} — Ctrl-C to stop"),
        None => eprintln!("watching ops across every db — Ctrl-C to stop"),
    }
    let mut out = std::io::stdout();
    // One signal listener for the whole run: re-creating the future per
    // iteration would drop and re-register the handler, and a SIGINT landing in
    // that window would be missed (the `rtdb watch` note, which matters more
    // here because of the outer reconnect loop).
    let shutdown = tokio::signal::ctrl_c();
    tail_ops(&client, db.as_deref(), pretty, &mut out, shutdown).await
}

/// The reconnecting tail, with the socket, the sink, and the stop condition all
/// injected so it can be driven against an in-process server in tests.
///
/// Returns `Ok(())` only when `shutdown` fires; a terminal failure comes back as
/// `Err` so the process exits non-zero.
async fn tail_ops<W, S>(
    client: &RtDbAdminClient,
    db: Option<&str>,
    pretty: bool,
    out: &mut W,
    shutdown: S,
) -> Result<()>
where
    W: Write,
    S: Future<Output = std::io::Result<()>>,
{
    tokio::pin!(shutdown);
    let backoff = Config::default();
    let mut attempt: u32 = 0;

    loop {
        let connected = tokio::select! {
            signal = &mut shutdown => { signal.context("waiting for Ctrl-C")?; return Ok(()); }
            stream = client.stream_admin(db, None) => stream,
        };
        match connected {
            Ok(stream) => {
                attempt = 0;
                // Every connection replays up to 200 ring events before going
                // live, so a reconnect re-emits the tail of the last session.
                // Each `OpEvent` carries `(feedEpoch, seq)`, so a consumer
                // piping this NDJSON into its own dedup can drop replays by
                // tracking the max `seq` seen per `feedEpoch`; this tail
                // itself does no deduping and surfaces every line as-is.
                match drain(stream, pretty, out, &mut shutdown).await? {
                    // Returning here is load-bearing: `shutdown` has completed,
                    // and the backoff arm below would poll it a second time.
                    DrainEnd::Shutdown => return Ok(()),
                    DrainEnd::Failed(err) if is_terminal(&err) => return Err(map_err(err)),
                    DrainEnd::Failed(err) => {
                        eprintln!("ops watch: {} — reconnecting", err.message)
                    }
                    DrainEnd::Closed => eprintln!("ops watch: stream closed — reconnecting"),
                }
            }
            Err(err) if is_terminal(&err) => return Err(map_err(err)),
            Err(err) => eprintln!("ops watch: {} — reconnecting", err.message),
        }

        let delay = backoff.backoff_for(attempt);
        attempt = attempt.saturating_add(1);
        tokio::select! {
            signal = &mut shutdown => { signal.context("waiting for Ctrl-C")?; return Ok(()); }
            _ = tokio::time::sleep(delay) => {}
        }
    }
}

/// Why [`drain`] stopped reading one connection.
///
/// `Shutdown` is distinct from `Closed` on purpose: a shared `shutdown` future
/// resolves once, so conflating the two would send the caller into a backoff
/// that awaits an already-completed signal.
enum DrainEnd {
    /// Ctrl-C arrived; the tail is over.
    Shutdown,
    /// The peer closed; reconnecting is in order.
    Closed,
    /// The stream reported a reason for the caller to classify.
    Failed(RtDbError),
}

/// Print one connection's worth of op events.
///
/// `Err` is reserved for the sink failing (a closed pipe), which is the
/// caller's problem rather than the server's.
async fn drain<W, S>(
    mut stream: AdminStream,
    pretty: bool,
    out: &mut W,
    shutdown: &mut std::pin::Pin<&mut S>,
) -> Result<DrainEnd>
where
    W: Write,
    S: Future<Output = std::io::Result<()>>,
{
    loop {
        let item = tokio::select! {
            signal = &mut *shutdown => {
                signal.context("waiting for Ctrl-C")?;
                return Ok(DrainEnd::Shutdown);
            }
            item = stream.next() => item,
        };
        match item {
            Some(Ok(frame)) => {
                if let Some(line) = frame_line(&frame, pretty) {
                    writeln!(out, "{line}").context("writing op event")?;
                    // Flushed per line: an operator tailing through a pipe must
                    // see each event as it commits, not when a buffer fills.
                    out.flush().context("flushing op event")?;
                }
            }
            Some(Err(err)) => return Ok(DrainEnd::Failed(err)),
            None => return Ok(DrainEnd::Closed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use std::time::Duration;
    use tokio::net::{TcpListener, TcpStream};
    use tokio_tungstenite::WebSocketStream;
    use tokio_tungstenite::tungstenite::Message as WsMessage;
    use tokio_tungstenite::tungstenite::protocol::frame::{CloseFrame, coding::CloseCode};

    /// Everything here talks to a loopback socket, so a hang is a bug.
    const DEADLINE: Duration = Duration::from_secs(10);

    fn op_json(db: &str, doc_id: &str) -> String {
        serde_json::json!({
            "kind": "op",
            "event": {
                "db": db, "table": "items", "docId": doc_id,
                "kind": "insert", "ts": 1_700_000_000_000i64, "owner": null,
                "seq": 1, "feedEpoch": "test-epoch",
            }
        })
        .to_string()
    }

    fn gauges_json() -> String {
        serde_json::json!({
            "kind": "gauges",
            "gauges": {
                "queriesTotal": 1, "mutationsTotal": 2, "uploadsTotal": 0,
                "wsConnections": 1, "activeSubscriptions": 1, "poolSize": 5,
                "poolIdle": 4, "uptimeSeconds": 42,
                "queryLatency": {"p50": 1, "p95": 2, "p99": 3},
                "mutateLatency": {"p50": 1, "p95": 2, "p99": 3},
                "subscribeLatency": {"p50": 1, "p95": 2, "p99": 3},
            }
        })
        .to_string()
    }

    fn parse(frame: &str) -> AdminStreamFrame {
        serde_json::from_str(frame).expect("fixture parses as a stream frame")
    }

    async fn accept(listener: &TcpListener) -> WebSocketStream<TcpStream> {
        let (socket, _) = listener.accept().await.expect("accept");
        tokio_tungstenite::accept_async(socket)
            .await
            .expect("ws handshake")
    }

    #[test]
    fn frame_line_renders_op_events_and_drops_gauges() {
        let line = frame_line(&parse(&op_json("kanban", "d1")), false).expect("op renders");
        // Compact NDJSON, camelCase as the server spells it.
        assert!(!line.contains('\n'), "NDJSON must be one line: {line}");
        assert!(line.contains(r#""docId":"d1""#), "got: {line}");
        assert!(line.contains(r#""db":"kanban""#), "got: {line}");

        // The ~1s gauge snapshot is feed noise, not an op — it must never reach
        // stdout, or the NDJSON stream is a metrics blob per second.
        assert!(frame_line(&parse(&gauges_json()), false).is_none());
        assert!(frame_line(&parse(&gauges_json()), true).is_none());
    }

    #[test]
    fn frame_line_pretty_expands_the_event() {
        let pretty = frame_line(&parse(&op_json("kanban", "d1")), true).expect("op renders");
        assert!(
            pretty.contains('\n'),
            "--pretty should expand the event: {pretty}"
        );
        assert!(pretty.contains("\"docId\": \"d1\""), "got: {pretty}");
    }

    #[test]
    fn only_credential_failures_are_terminal() {
        // A rejected credential must stop the tail; anything else is a blip to
        // reconnect through.
        assert!(is_terminal(&RtDbError::new(ErrorCode::Unauthorized, "no")));
        assert!(is_terminal(&RtDbError::new(ErrorCode::Forbidden, "no")));
        assert!(!is_terminal(&RtDbError::internal("connection lost")));
        assert!(!is_terminal(&RtDbError::new(
            ErrorCode::Internal,
            "timeout"
        )));
    }

    /// The reconnect path end to end: the server drops the first connection
    /// mid-tail, the tail comes back on its own and keeps printing, and a
    /// revoked credential on the second connection stops it with a clear error
    /// instead of looping forever.
    #[tokio::test]
    async fn tail_reconnects_after_a_drop_then_stops_on_a_revoked_credential() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("addr"));

        tokio::spawn(async move {
            // Connection 1: one event, then vanish without a close frame.
            let mut ws = accept(&listener).await;
            ws.send(WsMessage::Text(op_json("kanban", "before").into()))
                .await
                .expect("send");
            ws.close(None).await.expect("close");
            drop(ws);

            // Connection 2: proof the tail came back, then SEC-006 revocation.
            let mut ws = accept(&listener).await;
            ws.send(WsMessage::Text(op_json("kanban", "after").into()))
                .await
                .expect("send");
            ws.send(WsMessage::Close(Some(CloseFrame {
                code: CloseCode::Library(4401),
                reason: "admin credential no longer valid".into(),
            })))
            .await
            .expect("close 4401");
            while ws.next().await.is_some() {}
        });

        let client = RtDbAdminClient::new(&url, "admin-key");
        let mut out: Vec<u8> = Vec::new();
        let err = tokio::time::timeout(
            DEADLINE,
            tail_ops(
                &client,
                Some("kanban"),
                false,
                &mut out,
                std::future::pending::<std::io::Result<()>>(),
            ),
        )
        .await
        .expect("the tail terminates instead of hanging")
        .expect_err("a revoked credential is a failure");

        let printed = String::from_utf8(out).expect("utf8 output");
        let lines: Vec<&str> = printed.lines().collect();
        assert_eq!(
            lines.len(),
            2,
            "expected one line per connection, got: {printed}"
        );
        assert!(lines[0].contains(r#""docId":"before""#), "got: {printed}");
        // The second line can only exist if the tail reconnected on its own.
        assert!(lines[1].contains(r#""docId":"after""#), "got: {printed}");
        assert!(
            err.to_string().contains("no longer valid"),
            "expected the server's reason, got: {err}"
        );
    }

    /// A rejected upgrade is terminal on the first attempt: no retry loop, no
    /// silent hang, and the server's own message reaches the operator.
    #[tokio::test]
    async fn tail_stops_immediately_when_the_upgrade_is_rejected() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("addr"));

        let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = attempts.clone();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.expect("accept");
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // Drain the upgrade request before answering: writing the
                // response while the peer is still sending, then dropping the
                // socket, lets a reset overtake the body and the client sees a
                // bare status instead of the envelope.
                let mut buf = Vec::new();
                let mut byte = [0u8; 1];
                while !buf.ends_with(b"\r\n\r\n") {
                    match socket.read(&mut byte).await {
                        Ok(0) => break,
                        Ok(_) => buf.push(byte[0]),
                        Err(_) => break,
                    }
                }
                // One write, so head and body share a segment: tungstenite
                // only captures the body bytes already buffered behind the
                // headers, so two writes let the reader wake in between.
                let body = r#"{"code":"UNAUTHORIZED","message":"invalid admin key"}"#;
                let response = format!(
                    "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket
                    .write_all(response.as_bytes())
                    .await
                    .expect("401 response");
                socket.flush().await.expect("flush");
                // Hold the socket open until the client is done reading.
                let _ = socket.read(&mut byte).await;
            }
        });

        let client = RtDbAdminClient::new(&url, "wrong-key");
        let mut out: Vec<u8> = Vec::new();
        let err = tokio::time::timeout(
            DEADLINE,
            tail_ops(
                &client,
                None,
                false,
                &mut out,
                std::future::pending::<std::io::Result<()>>(),
            ),
        )
        .await
        .expect("the tail returns instead of retrying forever")
        .expect_err("a 401 upgrade is a failure");

        // The code, not the prose: tungstenite captures the response body only
        // when it shares a segment with the headers, so asserting the server's
        // message here would be asserting on TCP framing. The envelope-message
        // path has its own test in `rust-client/src/admin/stream_tests.rs`;
        // what this one owns is that a 401 is classified terminal.
        assert!(
            err.to_string().contains("UNAUTHORIZED"),
            "expected an unauthorized failure, got: {err}"
        );
        assert!(
            out.is_empty(),
            "nothing should reach stdout on an auth failure"
        );
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "an auth rejection must not be retried"
        );
    }

    /// Ctrl-C while a stream is open and idle ends the tail cleanly.
    ///
    /// The regression guard for the shared shutdown future: `drain` has to
    /// report the signal distinctly from a closed peer, or the caller falls
    /// through to a backoff that polls an already-completed `ctrl_c`.
    #[tokio::test]
    async fn tail_returns_ok_when_shutdown_fires_mid_stream() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("addr"));

        tokio::spawn(async move {
            let mut ws = accept(&listener).await;
            ws.send(WsMessage::Text(op_json("kanban", "d1").into()))
                .await
                .expect("send");
            // Then go quiet and stay open: the tail is parked on `next()` when
            // the signal lands, which is where a real Ctrl-C usually arrives.
            while ws.next().await.is_some() {}
        });

        let client = RtDbAdminClient::new(&url, "admin-key");
        let mut out: Vec<u8> = Vec::new();
        let result = tokio::time::timeout(
            DEADLINE,
            tail_ops(&client, None, false, &mut out, async {
                tokio::time::sleep(Duration::from_millis(300)).await;
                Ok(())
            }),
        )
        .await
        .expect("shutdown is honored while the stream is open");
        assert!(result.is_ok(), "Ctrl-C should exit 0, got: {result:?}");
        // The event that arrived before the signal still made it out.
        let printed = String::from_utf8(out).expect("utf8");
        assert!(printed.contains(r#""docId":"d1""#), "got: {printed}");
    }

    /// Ctrl-C ends the tail cleanly (exit 0) rather than erroring.
    #[tokio::test]
    async fn tail_returns_ok_when_shutdown_fires() {
        // Nothing is listening on this port, so the tail is mid-reconnect when
        // the shutdown signal lands — the state a Ctrl-C most often interrupts.
        let client = RtDbAdminClient::new("http://127.0.0.1:1", "admin-key");
        let mut out: Vec<u8> = Vec::new();
        let result = tokio::time::timeout(
            DEADLINE,
            tail_ops(&client, None, false, &mut out, async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(())
            }),
        )
        .await
        .expect("shutdown is honored promptly");
        assert!(result.is_ok(), "Ctrl-C should exit 0, got: {result:?}");
        assert!(out.is_empty());
    }
}
