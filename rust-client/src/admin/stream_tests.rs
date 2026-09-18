//! `/admin/stream` consumer tests, driven by an in-process WebSocket server
//! rather than a live par-rt-db: the handshake this code has to get right
//! (bearer header, `db`/`table` query filter) and the three ways the stream can
//! end (clean close, credential revoked mid-stream, rejected upgrade) are all
//! observable from the socket alone.
//!
//! The mini server is deliberately hand-rolled at the TCP layer for the
//! rejected-upgrade case: a real `/admin/stream` rejection is an ordinary HTTP
//! 401 with the `{code, message}` envelope, emitted *before* WS negotiation
//! begins, so replaying those exact bytes is what proves the client surfaces
//! the server's message instead of a bare status.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::protocol::frame::{CloseFrame, coding::CloseCode};

use super::RtDbAdminClient;
use crate::error::ErrorCode;
use crate::wire::admin::AdminStreamFrame;

use futures_util::{SinkExt, StreamExt};

/// Every assertion is against a local socket, so a hang is a bug, not slowness.
const DEADLINE: Duration = Duration::from_secs(10);

fn op_frame(db: &str, doc_id: &str) -> String {
    serde_json::json!({
        "kind": "op",
        "event": {
            "db": db,
            "table": "items",
            "docId": doc_id,
            "kind": "insert",
            "ts": 1_700_000_000_000i64,
            "owner": null,
        }
    })
    .to_string()
}

fn gauge_frame() -> String {
    serde_json::json!({
        "kind": "gauges",
        "gauges": {
            "queriesTotal": 1,
            "mutationsTotal": 2,
            "uploadsTotal": 0,
            "wsConnections": 1,
            "activeSubscriptions": 1,
            "poolSize": 5,
            "poolIdle": 4,
            "uptimeSeconds": 42,
            "queryLatency": {"p50": 100, "p95": 200, "p99": 300},
            "mutateLatency": {"p50": 100, "p95": 200, "p99": 300},
            "subscribeLatency": {"p50": 100, "p95": 200, "p99": 300},
        }
    })
    .to_string()
}

/// Bind an ephemeral loopback port and return it plus the listener.
async fn listener() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");
    (listener, format!("http://{addr}"))
}

/// What the client put on the upgrade request, for the tests that assert on it.
#[derive(Clone, Default)]
struct RequestHead {
    /// Origin-form target, e.g. `/admin/stream?db=kanban`.
    uri: String,
    /// The `Authorization` header value, empty when absent.
    authorization: String,
}

/// Complete the WS handshake, capturing the request head on the way through.
///
/// The callback is the only place the request is visible: reading the socket
/// directly would eat the handshake bytes the handshake itself needs, leaving
/// the client waiting for a 101 that never comes.
// The callback's `Result` shape is fixed by tungstenite's `Callback` trait —
// its `Err` is the rejection `Response`, which this one never returns.
#[allow(clippy::result_large_err)]
async fn accept_capturing(
    socket: TcpStream,
) -> (
    tokio_tungstenite::WebSocketStream<TcpStream>,
    std::sync::Arc<std::sync::Mutex<RequestHead>>,
) {
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

    let head = std::sync::Arc::new(std::sync::Mutex::new(RequestHead::default()));
    let sink = head.clone();
    let ws = tokio_tungstenite::accept_hdr_async(socket, move |req: &Request, resp: Response| {
        if let Ok(mut slot) = sink.lock() {
            slot.uri = req.uri().to_string();
            slot.authorization = req
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string();
        }
        Ok(resp)
    })
    .await
    .expect("ws handshake");
    (ws, head)
}

/// Park the server socket until the client goes away.
///
/// Returning from the task instead would drop the socket, and a TCP reset that
/// overtakes the frames just written turns a delivered frame into
/// "Connection reset without closing handshake" — a flake, not a finding.
async fn hold_until_client_closes(mut ws: tokio_tungstenite::WebSocketStream<TcpStream>) {
    while ws.next().await.is_some() {}
}

/// The raw HTTP request head, for the tests that reject the upgrade instead of
/// completing it (no handshake follows, so consuming the bytes is safe).
async fn read_request_head(socket: &mut TcpStream) -> String {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    while !buf.ends_with(b"\r\n\r\n") {
        match socket.read(&mut byte).await {
            Ok(0) => break,
            Ok(_) => buf.push(byte[0]),
            Err(e) => panic!("reading request head: {e}"),
        }
    }
    String::from_utf8_lossy(&buf).to_string()
}

#[tokio::test]
async fn stream_admin_yields_op_and_gauge_frames() {
    let (listener, url) = listener().await;
    let (head_tx, head_rx) = tokio::sync::oneshot::channel::<RequestHead>();
    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept");
        let (mut ws, head) = accept_capturing(socket).await;
        let _ = head_tx.send(head.lock().expect("head lock").clone());
        ws.send(WsMessage::Text(op_frame("kanban", "d1").into()))
            .await
            .expect("send op");
        ws.send(WsMessage::Text(gauge_frame().into()))
            .await
            .expect("send gauges");
        hold_until_client_closes(ws).await;
    });

    let client = RtDbAdminClient::new(&url, "admin-key");
    let mut stream = client
        .stream_admin(None, None)
        .await
        .expect("stream opens against the mini server");

    let first = timeout(DEADLINE, stream.next())
        .await
        .expect("op frame arrives")
        .expect("stream still open")
        .expect("frame parses");
    match first {
        AdminStreamFrame::Op { event } => {
            assert_eq!(event.db, "kanban");
            assert_eq!(event.doc_id, "d1");
            assert_eq!(event.kind, "insert");
        }
        other => panic!("expected an op frame, got {other:?}"),
    }

    let second = timeout(DEADLINE, stream.next())
        .await
        .expect("gauge frame arrives")
        .expect("stream still open")
        .expect("frame parses");
    assert!(
        matches!(second, AdminStreamFrame::Gauges { .. }),
        "expected a gauges frame, got {second:?}"
    );

    // The handshake carried the admin key as a bearer header — the CLI path the
    // server documents, not the browser subprotocol.
    let head = timeout(DEADLINE, head_rx)
        .await
        .expect("request head captured")
        .expect("server task sent it");
    assert_eq!(head.authorization, "Bearer admin-key");
    // No filter passed ⇒ no query string, so the server streams every db.
    assert_eq!(head.uri, "/admin/stream");
}

#[tokio::test]
async fn stream_admin_sends_db_and_table_filter_as_query_params() {
    let (listener, url) = listener().await;
    let (head_tx, head_rx) = tokio::sync::oneshot::channel::<RequestHead>();
    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept");
        let (mut ws, head) = accept_capturing(socket).await;
        let _ = head_tx.send(head.lock().expect("head lock").clone());
        ws.send(WsMessage::Text(op_frame("my db", "d1").into()))
            .await
            .expect("send op");
        hold_until_client_closes(ws).await;
    });

    let client = RtDbAdminClient::new(&url, "admin-key");
    let mut stream = client
        .stream_admin(Some("my db"), Some("items"))
        .await
        .expect("stream opens");
    timeout(DEADLINE, stream.next())
        .await
        .expect("op frame arrives")
        .expect("stream still open")
        .expect("frame parses");

    // The filter rides the query string the server's `StreamParams` reads, and
    // a value needing escaping is percent-encoded rather than interpolated raw.
    let head = timeout(DEADLINE, head_rx)
        .await
        .expect("request head captured")
        .expect("server task sent it");
    assert_eq!(head.uri, "/admin/stream?db=my+db&table=items");
}

#[tokio::test]
async fn stream_admin_skips_frames_it_cannot_parse() {
    let (listener, url) = listener().await;
    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept");
        let mut ws = tokio_tungstenite::accept_async(socket)
            .await
            .expect("ws handshake");
        // A frame kind this client version does not know, then a good one. A
        // newer server must not be able to kill an operator's tail.
        ws.send(WsMessage::Text(r#"{"kind":"fromTheFuture"}"#.into()))
            .await
            .expect("send unknown");
        ws.send(WsMessage::Text("not json at all".into()))
            .await
            .expect("send garbage");
        ws.send(WsMessage::Text(op_frame("kanban", "d9").into()))
            .await
            .expect("send op");
        hold_until_client_closes(ws).await;
    });

    let client = RtDbAdminClient::new(&url, "admin-key");
    let mut stream = client.stream_admin(None, None).await.expect("stream opens");
    let frame = timeout(DEADLINE, stream.next())
        .await
        .expect("the parseable frame arrives")
        .expect("stream still open")
        .expect("frame parses");
    match frame {
        AdminStreamFrame::Op { event } => assert_eq!(event.doc_id, "d9"),
        other => panic!("expected the op frame that followed the junk, got {other:?}"),
    }
}

#[tokio::test]
async fn stream_admin_ends_with_none_on_a_clean_close() {
    let (listener, url) = listener().await;
    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept");
        let mut ws = tokio_tungstenite::accept_async(socket)
            .await
            .expect("ws handshake");
        ws.send(WsMessage::Text(op_frame("kanban", "d1").into()))
            .await
            .expect("send op");
        ws.close(None).await.expect("close");
    });

    let client = RtDbAdminClient::new(&url, "admin-key");
    let mut stream = client.stream_admin(None, None).await.expect("stream opens");
    timeout(DEADLINE, stream.next())
        .await
        .expect("op frame arrives")
        .expect("stream still open")
        .expect("frame parses");

    // A server-side close is not an error: it is the transient case a caller
    // reconnects from. It must terminate rather than hang.
    let end = timeout(DEADLINE, stream.next())
        .await
        .expect("the stream ends instead of hanging");
    assert!(end.is_none(), "expected a clean end, got {end:?}");
}

#[tokio::test]
async fn stream_admin_reports_close_4401_as_unauthorized() {
    let (listener, url) = listener().await;
    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept");
        let mut ws = tokio_tungstenite::accept_async(socket)
            .await
            .expect("ws handshake");
        // SEC-006: the server closes an open stream with 4401 when the
        // credential stops validating on the gauge tick.
        ws.send(WsMessage::Close(Some(CloseFrame {
            code: CloseCode::Library(4401),
            reason: "admin credential no longer valid".into(),
        })))
        .await
        .expect("close 4401");
    });

    let client = RtDbAdminClient::new(&url, "admin-key");
    let mut stream = client.stream_admin(None, None).await.expect("stream opens");
    let err = timeout(DEADLINE, stream.next())
        .await
        .expect("the revocation surfaces instead of hanging")
        .expect("an item, not a silent end")
        .expect_err("4401 is an error, not a frame");
    assert_eq!(err.code, ErrorCode::Unauthorized);
    assert!(
        err.message.contains("no longer valid"),
        "expected the server's close reason, got: {}",
        err.message
    );
}

#[tokio::test]
async fn stream_admin_surfaces_the_servers_envelope_on_a_rejected_upgrade() {
    let (listener, url) = listener().await;
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        read_request_head(&mut socket).await;
        // Exactly what `admin_stream`'s pre-negotiation gate emits for a bad
        // bearer: a plain HTTP 401 carrying the `{code, message}` envelope.
        //
        // Written in ONE call so the body shares a segment with the headers.
        // tungstenite fills the error body from whatever is already buffered
        // behind the headers and never waits on `Content-Length`, so splitting
        // the write would make this assert on TCP framing rather than on the
        // client's parsing.
        let body = r#"{"code":"UNAUTHORIZED","message":"invalid admin key"}"#;
        let response = format!(
            "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        socket
            .write_all(response.as_bytes())
            .await
            .expect("write 401");
        socket.flush().await.expect("flush");
    });

    let client = RtDbAdminClient::new(&url, "wrong-key");
    let err = timeout(DEADLINE, client.stream_admin(None, None))
        .await
        .expect("the rejection returns instead of hanging")
        .expect_err("a 401 upgrade is an error");
    assert_eq!(err.code, ErrorCode::Unauthorized);
    assert_eq!(err.message, "invalid admin key");
}

#[tokio::test]
async fn stream_admin_maps_a_bodyless_403_to_forbidden() {
    let (listener, url) = listener().await;
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        read_request_head(&mut socket).await;
        socket
            .write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .expect("write 403");
        socket.flush().await.expect("flush");
    });

    let client = RtDbAdminClient::new(&url, "admin-key");
    let err = timeout(DEADLINE, client.stream_admin(None, None))
        .await
        .expect("the rejection returns instead of hanging")
        .expect_err("a 403 upgrade is an error");
    assert_eq!(err.code, ErrorCode::Forbidden);
}
