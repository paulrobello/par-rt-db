//! Regression test: the WS liveness check must count app-level pongs.
//! The heartbeat is an app-level `{type:"ping"}` TEXT frame and the server
//! answers with an app-level `{type:"pong"}` TEXT frame — neither side sends
//! protocol-level pings, so before the fix `Liveness::last_pong` never
//! refreshed and every session was force-reconnected at exactly `2 × heartbeat`.
#![cfg(feature = "ws")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use par_rt_db_client::{Config, ConnectionState, RtDbClient};
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message as WsMessage;

/// Minimal `/sync` stand-in: completes the auth handshake, then answers every
/// app-level ping with an app-level pong. Sends no protocol-level pings.
/// Counts accepted connections and observed pings for the assertions below.
async fn mock_sync_server(
    listener: TcpListener,
    connections: Arc<AtomicU32>,
    pings: Arc<AtomicU32>,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        connections.fetch_add(1, Ordering::SeqCst);
        let pings = Arc::clone(&pings);
        tokio::spawn(async move {
            let Ok(mut ws) = accept_async(stream).await else {
                return;
            };
            // Auth must be the first frame; complete the handshake like the
            // real server (no token validation — this tests session liveness,
            // not auth).
            let Some(Ok(WsMessage::Text(auth))) = ws.next().await else {
                return;
            };
            let Ok(auth_json) = serde_json::from_str::<serde_json::Value>(auth.as_str()) else {
                return;
            };
            if auth_json["type"] != "auth" {
                return;
            }
            let ok = r#"{"type":"authOk","user":{"kind":"user","email":null,"name":null}}"#;
            if ws.send(WsMessage::Text(ok.into())).await.is_err() {
                return;
            }
            while let Some(Ok(msg)) = ws.next().await {
                match msg {
                    WsMessage::Text(t) => {
                        let Ok(v) = serde_json::from_str::<serde_json::Value>(t.as_str()) else {
                            continue;
                        };
                        if v["type"] == "ping" {
                            pings.fetch_add(1, Ordering::SeqCst);
                            let pong = r#"{"type":"pong"}"#.to_string();
                            if ws.send(WsMessage::Text(pong.into())).await.is_err() {
                                return;
                            }
                        }
                    }
                    WsMessage::Close(_) => break,
                    _ => {}
                }
            }
        });
    }
}

#[tokio::test]
async fn session_survives_past_2x_heartbeat_on_app_pongs() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let connections = Arc::new(AtomicU32::new(0));
    let pings = Arc::new(AtomicU32::new(0));
    tokio::spawn(mock_sync_server(
        listener,
        Arc::clone(&connections),
        Arc::clone(&pings),
    ));

    let heartbeat = Duration::from_millis(100);
    let client = RtDbClient::with_config(
        &format!("ws://{addr}"),
        "liveness-test-db",
        {
            let token = "test-token".to_string();
            move || {
                let token = token.clone();
                async move { Some(token) }
            }
        },
        Config {
            heartbeat,
            ..Config::default()
        },
    );
    client.connect();

    // Watch the status channel for the whole window: any Reconnecting/Closed
    // transition means the driver dropped a healthy session.
    let mut status = client.status_receiver();
    let saw_drop = Arc::new(AtomicBool::new(false));
    let monitor = {
        let saw_drop = Arc::clone(&saw_drop);
        tokio::spawn(async move {
            while let Ok(()) = status.changed().await {
                let st = status.borrow().clone();
                if !matches!(
                    st.state,
                    ConnectionState::Connecting | ConnectionState::Connected
                ) {
                    saw_drop.store(true, Ordering::SeqCst);
                }
            }
        })
    };

    // The liveness window is 2 × heartbeat = 200ms; run well past it with no
    // traffic other than the heartbeat itself.
    tokio::time::sleep(heartbeat * 4 + Duration::from_millis(50)).await;

    assert!(
        !saw_drop.load(Ordering::SeqCst),
        "status must never leave Connected while app-level pongs keep arriving"
    );
    assert_eq!(
        connections.load(Ordering::SeqCst),
        1,
        "a session answering app-level pongs must not be reconnected"
    );
    assert!(
        pings.load(Ordering::SeqCst) >= 2,
        "heartbeats must have actually flowed for this test to mean anything"
    );
    assert!(matches!(client.status().state, ConnectionState::Connected));

    monitor.abort();
    client.close();
}
