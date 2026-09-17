//! `RTDB_SHUTDOWN_DRAIN_MS`: the graceful-shutdown drain is bounded.
//!
//! These run the real composed `serve_with_drain_bound` against a real
//! listener and a real `axum::serve(...).with_graceful_shutdown(...)`. No
//! Postgres: the drain is a transport concern, so the routers here are one
//! route each rather than the full app.
//!
//! What holds the drain open is a connection whose hyper connection future has
//! not resolved — in practice a response body that is still streaming, such as
//! a large storage download to a client that stopped reading. An upgraded
//! WebSocket is NOT one of those: `axum::extract::ws`'s `on_upgrade` hands the
//! socket to a detached `tokio::spawn`, so the connection future resolves at
//! the handshake and the drain never waits for it (axum 0.8.9,
//! `src/extract/ws.rs`). The third test pins that, so a future axum that
//! starts tracking upgraded connections fails here instead of silently
//! reintroducing an unbounded wait.
//!
//! What they cannot cover: `main.rs` is a binary, so its own call site is not
//! reachable from an integration test. These pin the helper and the wiring
//! shape `main` uses, not `main` itself.

use std::future::IntoFuture;
use std::net::SocketAddr;
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::response::Response;
use axum::routing::get;
use futures_util::{SinkExt, StreamExt};
use rtdb_server::shutdown::serve_with_drain_bound;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

/// Sends response headers and then never another byte — the shape of a
/// download to a client that stopped reading.
async fn stalled_body() -> Response {
    let stream = futures_util::stream::unfold((), |()| async {
        tokio::time::sleep(Duration::from_secs(3600)).await;
        Some((Ok::<_, std::io::Error>(Bytes::from_static(b"x")), ()))
    });
    Response::new(Body::from_stream(stream))
}

/// Upgrades and then holds the socket open forever, like a subscribed client
/// sitting idle between pushes. Echoes so a test can prove the connection
/// survived shutdown rather than merely assuming it.
async fn hold_open(ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(|mut socket: WebSocket| async move {
        while let Some(Ok(msg)) = socket.recv().await {
            if socket.send(msg).await.is_err() {
                break;
            }
        }
    })
}

/// Binds an ephemeral port and serves `app` under a graceful-shutdown token,
/// wrapped in the drain bound exactly as `main` wraps it.
async fn spawn_bounded(
    app: Router,
    drain: Duration,
) -> (
    SocketAddr,
    CancellationToken,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local addr");
    let token = CancellationToken::new();

    let serve = axum::serve(listener, app)
        .with_graceful_shutdown({
            let token = token.clone();
            async move { token.cancelled().await }
        })
        .into_future();

    let server = tokio::spawn(serve_with_drain_bound(
        serve,
        {
            let token = token.clone();
            async move { token.cancelled().await }
        },
        drain,
    ));

    (addr, token, server)
}

/// Starts a request whose response body never completes and returns once its
/// headers have arrived, so the connection is mid-response when shutdown fires.
async fn start_stalled_request(addr: SocketAddr) -> reqwest::Response {
    let resp = reqwest::get(format!("http://{addr}/stalled"))
        .await
        .expect("stalled request headers");
    assert!(
        resp.status().is_success(),
        "stalled route returned an error"
    );
    resp
}

#[tokio::test]
async fn stalled_response_body_blocks_an_unbounded_drain() {
    // The premise the bound exists for, and that the test below rests on:
    // with a response still streaming, axum's graceful drain does not finish
    // on its own. 0 is also the documented "wait forever" setting, so this
    // pins both at once.
    let (addr, token, mut server) = spawn_bounded(
        Router::new().route("/stalled", get(stalled_body)),
        Duration::ZERO,
    )
    .await;
    let _stalled = start_stalled_request(addr).await;

    token.cancel();

    let outcome = tokio::time::timeout(Duration::from_millis(300), &mut server).await;
    assert!(
        outcome.is_err(),
        "serve future resolved under a zero (wait-forever) drain — either the \
         bound leaked in, or a stalled response body no longer blocks the drain"
    );
    // Still pending by design; nothing else will ever end it.
    server.abort();
}

#[tokio::test]
async fn drain_bound_ends_a_wait_a_stalled_body_would_never_end() {
    let (addr, token, server) = spawn_bounded(
        Router::new().route("/stalled", get(stalled_body)),
        Duration::from_millis(50),
    )
    .await;
    let _stalled = start_stalled_request(addr).await;

    token.cancel();

    // 500ms is 10x the bound: generous enough not to flake on a loaded
    // machine, far short of the unbounded wait the test above pins.
    let outcome = tokio::time::timeout(Duration::from_millis(500), server)
        .await
        .expect("serve future did not resolve within 500ms despite a 50ms drain bound")
        .expect("serve task panicked");
    assert!(outcome.is_ok(), "bounded drain reported a server error");
}

#[tokio::test]
async fn silent_websocket_does_not_block_the_drain() {
    // Counterpart to the two above: an idle upgraded WebSocket never held
    // shutdown open in the first place, because `on_upgrade` detaches the
    // socket into its own task. A zero (wait-forever) drain therefore still
    // resolves promptly. If axum ever tracks upgraded connections in its
    // close-set, this assertion fails and the drain bound becomes the thing
    // that ends the wait.
    let (addr, token, server) =
        spawn_bounded(Router::new().route("/ws", get(hold_open)), Duration::ZERO).await;
    // Silent until after shutdown, so it contributes nothing the drain could
    // be waiting on.
    let (mut client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .expect("websocket upgrade");

    token.cancel();

    let outcome = tokio::time::timeout(Duration::from_millis(500), server)
        .await
        .expect("drain did not finish despite the WebSocket being detached at upgrade")
        .expect("serve task panicked");
    assert!(outcome.is_ok(), "drain reported a server error");

    // The drain finished without closing this socket — the detached handler
    // still round-trips. This is what makes the assertion above meaningful:
    // the connection was live across shutdown, not already gone.
    client
        .send(Message::Text("still here".to_string().into()))
        .await
        .expect("send on a socket the drain should not have closed");
    let echoed = tokio::time::timeout(Duration::from_millis(500), client.next())
        .await
        .expect("no echo within 500ms")
        .expect("websocket stream ended")
        .expect("websocket error");
    assert_eq!(
        echoed.into_text().expect("text frame"),
        "still here",
        "the upgraded socket did not survive the drain"
    );
}
