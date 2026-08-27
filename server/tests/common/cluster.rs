//! ENH-029: Multi-instance test harness cluster module.
//! Implements Cluster, Replica, and ReplicaId.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rtdb_server::AppState;
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::error::ErrorCode;
use rtdb_server::protocol::{ClientMessage, ServerMessage};
use rtdb_server::schema::SchemaDef;
use rtdb_server::txn::{Step, Transaction, TxnOutcome, WriteSet};
use sqlx::PgPool;
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

const RETRY_DEADLINE: Duration = Duration::from_secs(30);
const SERVER_STOP_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaId {
    A,
    B,
}

#[derive(Debug, Clone)]
pub struct ReplicaOpts {
    pub label: String,
    pub per_token_rpm: u32,
    pub per_db_rpm: u32,
    pub exact_limits: bool,
    pub forward_timeout_ms: u64,
    pub forward_concurrency: usize,
    pub presence_enabled: bool,
    pub presence_broadcast_interval_ms: u64,
}

impl Default for ReplicaOpts {
    fn default() -> Self {
        Self {
            label: "replica".to_string(),
            per_token_rpm: 0,
            per_db_rpm: 0,
            exact_limits: true, // defaults to true matching stage4 replica() helper
            forward_timeout_ms: 2_000,
            forward_concurrency: 64,
            presence_enabled: false,
            presence_broadcast_interval_ms: 50,
        }
    }
}

pub struct Replica {
    pub state: Arc<AppState>,
    pub instance_id: String,
    pub app: axum::Router,
    pub addr: SocketAddr,
    pub shutdown: Option<oneshot::Sender<()>>,
    /// Axum serve task joined by `Cluster::kill` for guaranteed HTTP teardown.
    task: Option<tokio::task::JoinHandle<()>>,
}
impl Drop for Replica {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
        self.state.background.cancel();
    }
}

pub struct Cluster {
    pub a: Option<Replica>,
    pub b: Option<Replica>,
    pub db: crate::common::TestDb,
    /// Bearer token for mutate_http/ws client connections, minted lazily on first call.
    auth: tokio::sync::OnceCell<String>,
}

impl Cluster {
    pub async fn two(schema: SchemaDef) -> Cluster {
        let a_opts = ReplicaOpts {
            label: "cluster-a".to_string(),
            ..ReplicaOpts::default()
        };
        let b_opts = ReplicaOpts {
            label: "cluster-b".to_string(),
            ..ReplicaOpts::default()
        };

        Self::two_with(schema, a_opts, b_opts).await
    }

    pub async fn two_with(
        schema: SchemaDef,
        mut a_opts: ReplicaOpts,
        mut b_opts: ReplicaOpts,
    ) -> Cluster {
        let cluster_id = uuid::Uuid::now_v7().simple().to_string();
        a_opts.label = format!("{}-{cluster_id}", a_opts.label);
        b_opts.label = format!("{}-{cluster_id}", b_opts.label);
        let cluster = Self::two_bare(a_opts, b_opts).await;

        // A's push takes the lease (A becomes owner).
        cluster
            .a
            .as_ref()
            .unwrap()
            .state
            .realtime
            .committers
            .push_schema(&cluster.db, schema)
            .await
            .expect("cluster push_schema failed");

        // Wait until ownership is settled.
        cluster.wait_owner_settled().await;
        cluster
    }

    pub async fn two_bare(a_opts: ReplicaOpts, b_opts: ReplicaOpts) -> Cluster {
        let pool = shared_pool().await;
        let name = format!("t{}", uuid::Uuid::now_v7().simple());
        rtdb_server::db::create_database(&pool, &name)
            .await
            .expect("create cluster database");
        let db = crate::common::wrap_test_db(name.clone());

        let a = spawn_replica(&pool, &name, a_opts).await;
        let b = spawn_replica(&pool, &name, b_opts).await;

        // Let background PgListeners connect and LISTEN to PG channels.
        tokio::time::sleep(Duration::from_millis(500)).await;

        Cluster {
            a: Some(a),
            b: Some(b),
            db,
            auth: tokio::sync::OnceCell::new(),
        }
    }

    pub fn replica(&self, id: ReplicaId) -> &Replica {
        match id {
            ReplicaId::A => self.a.as_ref(),
            ReplicaId::B => self.b.as_ref(),
        }
        .unwrap_or_else(|| panic!("replica {id:?} has been killed"))
    }

    pub async fn owner(&self) -> ReplicaId {
        let found = crate::common::wait_until(Duration::from_secs(5), || async {
            self.is_owner_of(ReplicaId::A).await || self.is_owner_of(ReplicaId::B).await
        })
        .await;

        assert!(
            found,
            "cluster: no owner holds the lease for {} within 5s",
            self.db.0
        );

        if self.is_owner_of(ReplicaId::A).await {
            ReplicaId::A
        } else {
            ReplicaId::B
        }
    }

    pub async fn ws(&self, id: ReplicaId) -> WsClient {
        let token = self.token().await;
        let addr = self.replica(id).addr;
        WsClient::connect(addr, &self.db, token).await
    }

    pub async fn mutate_http(&self, id: ReplicaId, txn: Transaction) -> TxnOutcome {
        let token = self.token().await;
        let addr = self.replica(id).addr;

        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/api/mutate"))
            .header("Authorization", format!("Bearer {token}"))
            .json(&serde_json::json!({
                "db": self.db.0.as_str(),
                "txn": txn
            }))
            .send()
            .await
            .expect("send HTTP mutate request");

        let status = resp.status();
        let body: serde_json::Value = resp.json().await.expect("parse HTTP mutate response");
        assert_eq!(
            status,
            reqwest::StatusCode::OK,
            "mutate_http failed: {body:?}"
        );

        let results = body["results"].as_array().cloned().unwrap_or_default();

        TxnOutcome {
            results,
            write_set: WriteSet::default(), // HTTP does not return write_set
        }
    }

    pub async fn kill(&mut self, id: ReplicaId) {
        let mut replica = match id {
            ReplicaId::A => self.a.take(),
            ReplicaId::B => self.b.take(),
        }
        .unwrap_or_else(|| panic!("replica {id:?} already killed"));

        // 1. Release the ownership lease + per-db committer task (the generalized
        //    `committers.drop_db` pattern).
        replica.state.realtime.committers.drop_db(&self.db).await;

        // 2. Trigger graceful HTTP shutdown.
        if let Some(tx) = replica.shutdown.take() {
            let _ = tx.send(());
        }
        // 3. Await graceful HTTP shutdown; fallback to hard abort on timeout.
        if let Some(task) = replica.task.take() {
            let mut task = task;
            if tokio::time::timeout(SERVER_STOP_TIMEOUT, &mut task)
                .await
                .is_err()
            {
                task.abort();
                let _ = task.await;
            }
        }

        // 4. Cancel background listeners and sweepers.
        replica
            .state
            .background
            .shutdown(Duration::from_secs(5))
            .await;

        // 5. Drop state Arc (releases the remaining database connection clones).
        drop(replica);
    }

    pub async fn wait_takeover(&self, id: ReplicaId) {
        let ok = crate::common::wait_until(Duration::from_secs(30), || async {
            self.is_owner_of(id).await
        })
        .await;

        assert!(
            ok,
            "cluster: replica {id:?} did not take over ownership of {} within 30s",
            self.db.0
        );
    }

    #[cfg(feature = "test-support")]
    pub async fn drop_replies(&self, id: ReplicaId, on: bool) {
        let replica = self.replica(id);
        if let Some(forwarder) = replica.state.realtime.committers.forwarder() {
            forwarder.set_drop_replies(on);
        }
    }

    #[cfg(feature = "test-support")]
    pub async fn delay_listener(&self, id: ReplicaId, delay: Duration) {
        let replica = self.replica(id);
        if let Some(forwarder) = replica.state.realtime.committers.forwarder() {
            forwarder.set_delay_listener(Some(delay));
        }
    }

    async fn wait_owner_settled(&self) {
        let ok = crate::common::wait_until(Duration::from_secs(10), || async {
            self.is_owner_of(ReplicaId::A).await
        })
        .await;
        assert!(ok, "cluster: replica A did not hold the lease within 10s");
    }

    async fn is_owner_of(&self, id: ReplicaId) -> bool {
        let replica = match id {
            ReplicaId::A => &self.a,
            ReplicaId::B => &self.b,
        };
        if let Some(r) = replica {
            r.state.realtime.committers.is_owner(&self.db).await
        } else {
            false
        }
    }

    async fn token(&self) -> &str {
        self.auth
            .get_or_init(|| async {
                let addr = if let Some(r) = &self.a {
                    r.addr
                } else if let Some(r) = &self.b {
                    r.addr
                } else {
                    panic!("both replicas are killed, cannot mint token");
                };

                let resp = crate::common::admin_post(
                    addr,
                    "/admin/mint-token",
                    serde_json::json!({
                        "db": self.db.0.as_str(),
                        "name": "cluster-test-token"
                    }),
                )
                .await;

                assert_eq!(
                    resp.status(),
                    reqwest::StatusCode::OK,
                    "failed to mint token for cluster"
                );

                let body: serde_json::Value = resp.json().await.expect("parse token response");
                body["token"].as_str().expect("token string").to_string()
            })
            .await
    }
}

pub struct WsClient {
    pub sink: futures_util::stream::SplitSink<
        WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
        Message,
    >,
    pub stream:
        futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>>,
}

impl WsClient {
    pub async fn connect(addr: SocketAddr, db: &str, token: &str) -> Self {
        let (ws, _) = connect_async(format!("ws://{addr}/sync"))
            .await
            .expect("connect /sync websocket");
        let (mut sink, mut stream) = ws.split();

        let auth_msg = ClientMessage::Auth {
            token: Some(token.to_string()),
            db: db.to_string(),
            protocol_version: None,
        };
        let payload = serde_json::to_string(&auth_msg).expect("serialize Auth message");
        sink.send(Message::Text(payload.into()))
            .await
            .expect("send Auth payload");

        loop {
            match stream.next().await {
                Some(Ok(Message::Text(text))) => {
                    let msg: ServerMessage =
                        serde_json::from_str(&text).expect("parse ServerMessage");
                    match msg {
                        ServerMessage::AuthOk { .. } => break,
                        other => panic!("expected AuthOk, got {other:?}"),
                    }
                }
                Some(Ok(_)) => continue,
                Some(Err(e)) => panic!("websocket error during auth: {e}"),
                None => panic!("websocket closed before AuthOk"),
            }
        }

        Self { sink, stream }
    }

    pub async fn send(&mut self, msg: &ClientMessage) {
        let payload = serde_json::to_string(msg).expect("serialize ClientMessage");
        self.sink
            .send(Message::Text(payload.into()))
            .await
            .expect("send WS message");
    }

    pub async fn recv(&mut self) -> Option<ServerMessage> {
        while let Some(msg) = self.stream.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    return Some(serde_json::from_str(&text).expect("parse ServerMessage"));
                }
                Ok(_) => continue,
                Err(e) => panic!("websocket read error: {e}"),
            }
        }
        None
    }

    pub async fn recv_timeout(&mut self, timeout: Duration) -> Option<ServerMessage> {
        tokio::time::timeout(timeout, self.recv())
            .await
            .ok()
            .flatten()
    }
}

async fn spawn_replica(pool: &PgPool, _db: &str, opts: ReplicaOpts) -> Replica {
    let state = replica_state(pool, &opts).await;
    let instance_id = state
        .config
        .multi_instance
        .instance_id
        .clone()
        .expect("instance_id must be populated");
    let app = rtdb_server::build_router(state.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local addr");

    let (tx, rx) = oneshot::channel();
    let serve = axum::serve(
        listener,
        app.clone()
            .into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        let _ = rx.await;
    });

    let task = tokio::spawn(async move {
        let _ = serve.await;
    });

    Replica {
        state,
        instance_id,
        app,
        addr,
        shutdown: Some(tx),
        task: Some(task),
    }
}

pub async fn replica_state(pool: &PgPool, opts: &ReplicaOpts) -> Arc<AppState> {
    let mut cfg = crate::common::test_config();
    cfg.multi_instance.enabled = true;
    cfg.multi_instance.instance_id = Some(crate::common::unique_instance_id(&opts.label));
    cfg.limits.per_token_rpm = opts.per_token_rpm;
    cfg.limits.per_db_rpm = opts.per_db_rpm;
    cfg.limits.exact = opts.exact_limits;
    cfg.multi_instance.forward_timeout_ms = opts.forward_timeout_ms;
    cfg.multi_instance.forward_concurrency = opts.forward_concurrency;
    cfg.presence_enabled = opts.presence_enabled;
    cfg.presence_broadcast_interval_ms = opts.presence_broadcast_interval_ms;
    AppState::new(pool.clone(), cfg, crate::common::test_hot())
}

// --- Moved Helpers from multi_instance_stage4_test.rs ---

pub async fn replica(
    pool: &PgPool,
    instance_id: &str,
    per_token_rpm: u32,
    per_db_rpm: u32,
) -> Arc<AppState> {
    let opts = ReplicaOpts {
        label: instance_id.to_string(),
        per_token_rpm,
        per_db_rpm,
        ..Default::default()
    };
    replica_state(pool, &opts).await
}

pub async fn shared_pool() -> PgPool {
    let config = crate::common::test_config();
    crate::common::test_shared_pool(&config.database_url, 11)
        .await
        .expect("connect to shared cluster test postgres")
}

pub fn insert_item(title: &str) -> Transaction {
    Transaction {
        steps: vec![Step::Insert {
            table: "items".to_string(),
            doc: serde_json::json!({ "title": title })
                .as_object()
                .unwrap()
                .clone(),
        }],
    }
}

pub async fn mutate_until_landed(
    state: &Arc<AppState>,
    db: &str,
    txn: Transaction,
    principal: PrincipalCtx,
) -> anyhow::Result<TxnOutcome> {
    let deadline = std::time::Instant::now() + RETRY_DEADLINE;
    loop {
        let result = state
            .realtime
            .committers
            .mutate(db, None, txn.clone(), principal.clone())
            .await;
        match result {
            Ok(outcome) => return Ok(outcome),
            Err(err) if err.code == ErrorCode::Conflict => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "write kept conflicting past the deadline: {err}"
                );
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            Err(err) => return Err(err.into()),
        }
    }
}
