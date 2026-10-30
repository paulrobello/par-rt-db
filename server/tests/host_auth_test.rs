//! Host machine authorization: enrollment, propagation, and operation policy.

use crate::common::{admin_get, admin_post, fresh_db, spawn_app, test_state};
use par_rt_db_core::host_capability::HostCapability;
use rtdb_server::auth::tokens::mint_token;
use rtdb_server::auth::{Principal, PrincipalCtx};
use rtdb_server::db::{now_ms, sha256_hex};
use serde_json::json;
use std::net::SocketAddr;

async fn mesh_fixture() -> (
    std::sync::Arc<rtdb_server::AppState>,
    SocketAddr,
    crate::common::TestDb,
    String,
    String,
) {
    let state = test_state().await;
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create mesh db");
    let db = crate::common::wrap_test_db(name);
    let mut agents = serde_json::Map::new();
    for field in [
        "addr",
        "name",
        "host",
        "project",
        "harness",
        "delivery",
        "status",
        "session_ref",
        "socket",
    ] {
        agents.insert(field.into(), json!({"type":"string"}));
    }
    for field in ["last_seen_ms", "expires_ms"] {
        agents.insert(field.into(), json!({"type":"number"}));
    }
    let mut messages = serde_json::Map::new();
    for field in [
        "msgid",
        "from_addr",
        "to_addr",
        "target",
        "host",
        "priority",
        "subject",
        "body",
        "trace",
        "state",
        "ack_note",
    ] {
        messages.insert(field.into(), json!({"type":"string"}));
    }
    for field in ["ts_ms", "delivered_ms", "acked_ms"] {
        messages.insert(field.into(), json!({"type":"number"}));
    }
    let schema = serde_json::from_value(json!({"tables":{
        "agents":{"fields":agents,"indexes":[{"name":"by_addr","fields":["addr"]}]},
        "messages":{"fields":messages,"indexes":[{"name":"by_msgid","fields":["msgid"]},{"name":"by_ts","fields":["ts_ms"]},{"name":"search_body","fields":["body"],"search":true}]}
    }})).expect("mesh schema");
    rtdb_server::ddl::push_schema(&state.pool, &db, schema)
        .await
        .expect("push mesh schema");
    let addr = spawn_app(state.clone()).await;
    let mut tokens = Vec::new();
    for host in ["testhost", "otherhost"] {
        let minted: serde_json::Value = admin_post(
            addr,
            "/admin/mint-token",
            json!({"db":db,"name":host,"hostCapability":{"type":"parMeshV1","host":host}}),
        )
        .await
        .json()
        .await
        .expect("mint mesh token");
        tokens.push(minted["token"].as_str().expect("token").to_owned());
    }
    (state, addr, db, tokens.remove(0), tokens.remove(0))
}

fn agent_doc(host: &str) -> serde_json::Value {
    json!({"addr":format!("worker@{host}:mesh"),"name":"worker","host":host,"project":"mesh","harness":"test","delivery":"socket","status":"online","session_ref":"s","socket":"s","last_seen_ms":1,"expires_ms":9999999999999_f64})
}

fn message_doc(sender: &str, recipient: &str) -> serde_json::Value {
    json!({"msgid":uuid::Uuid::now_v7().to_string(),"from_addr":format!("worker@{sender}:mesh"),"to_addr":format!("*@{recipient}:mesh"),"target":format!("worker@{recipient}:mesh"),"host":recipient,"priority":"next-turn","subject":"hello","body":"secret","trace":"","state":"queued","ack_note":"","ts_ms":1,"delivered_ms":0,"acked_ms":0})
}

async fn mesh_post(
    addr: SocketAddr,
    token: &str,
    path: &str,
    body: serde_json::Value,
) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("http://{addr}{path}"))
        .bearer_auth(token)
        .json(&body)
        .send()
        .await
        .expect("mesh request")
}

#[tokio::test]
async fn mesh_positive_directory_cross_host_send_and_receipts() {
    let (_state, addr, db, local, remote) = mesh_fixture().await;
    for (host, token) in [("testhost", &local), ("otherhost", &remote)] {
        let response = mesh_post(addr, token, "/api/mutate", json!({"db":db,"txn":{"steps":[{"op":"insert","table":"agents","doc":agent_doc(host)}]}})).await;
        assert!(
            response.status().is_success(),
            "{}",
            response.text().await.unwrap()
        );
    }
    let response = mesh_post(
        addr,
        &local,
        "/api/query",
        json!({"db":db,"query":{"table":"agents","count":true}}),
    )
    .await;
    assert_eq!(
        response.json::<serde_json::Value>().await.unwrap()["result"],
        json!(2)
    );
    for transitions in [
        vec![
            json!({"state":"delivered","delivered_ms":2}),
            json!({"state":"acked","acked_ms":3,"ack_note":"done"}),
        ],
        vec![json!({"state":"acked","acked_ms":3,"ack_note":"done"})],
    ] {
        let response = mesh_post(addr, &local, "/api/mutate", json!({"db":db,"txn":{"steps":[{"op":"insert","table":"messages","doc":message_doc("testhost","otherhost")}]}})).await;
        assert!(
            response.status().is_success(),
            "{}",
            response.text().await.unwrap()
        );
        let body: serde_json::Value = response.json().await.unwrap();
        let id = body["results"][0]["id"].as_str().expect("insert id");
        for fields in transitions {
            let response = mesh_post(addr, &remote, "/api/mutate", json!({"db":db,"txn":{"steps":[{"op":"patch","table":"messages","id":id,"fields":fields}]}})).await;
            assert!(
                response.status().is_success(),
                "{}",
                response.text().await.unwrap()
            );
        }
        for token in [&local, &remote] {
            let response = mesh_post(
                addr,
                token,
                "/api/query",
                json!({"db":db,"query":{"table":"messages","get":id}}),
            )
            .await;
            assert_eq!(
                response.json::<serde_json::Value>().await.unwrap()["result"]["state"],
                "acked"
            );
        }
    }
}

#[tokio::test]
async fn mesh_sender_spoof_and_cross_host_agent_insert_denied_atomically() {
    let (_state, addr, db, local, _remote) = mesh_fixture().await;
    for step in [
        json!({"op":"insert","table":"agents","doc":agent_doc("otherhost")}),
        json!({"op":"insert","table":"messages","doc":message_doc("otherhost","testhost")}),
    ] {
        let response = mesh_post(addr, &local, "/api/mutate", json!({"db":db,"txn":{"steps":[{"op":"insert","table":"agents","doc":agent_doc("testhost")},step]}})).await;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::FORBIDDEN,
            "{}",
            response.text().await.unwrap()
        );
        let response = mesh_post(
            addr,
            &local,
            "/api/query",
            json!({"db":db,"query":{"table":"agents","count":true}}),
        )
        .await;
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap()["result"],
            json!(0)
        );
    }
}

#[tokio::test]
async fn mesh_receipt_and_alternate_mutations_denied() {
    let (_state, addr, db, local, remote) = mesh_fixture().await;
    let response = mesh_post(addr, &local, "/api/mutate", json!({"db":db,"txn":{"steps":[{"op":"insert","table":"messages","doc":message_doc("testhost","otherhost")}]}})).await;
    let body: serde_json::Value = response.json().await.unwrap();
    let id = body["results"][0]["id"].as_str().unwrap();
    let mut steps = vec![
        json!({"op":"delete","table":"messages","id":id}),
        json!({"op":"replace","table":"messages","id":id,"doc":message_doc("testhost","otherhost")}),
        json!({"op":"undelete","table":"messages","id":id}),
        json!({"op":"adjustCounter","table":"messages","id":id,"field":"ts_ms","delta":1}),
        json!({"op":"upsert","table":"messages","index":"by_msgid","eq":["missing"],"insert":message_doc("testhost","otherhost"),"patch":{}}),
        json!({"op":"deleteByQuery","table":"messages","filter":{"op":"eq","field":"msgid","value":"missing"}}),
        json!({"op":"patchByQuery","table":"messages","filter":{"op":"eq","field":"msgid","value":"missing"},"patch":{"state":"acked"}}),
        json!({"op":"schedule","when":{"type":"afterMs","ms":60000},"txn":{"steps":[{"op":"startWorkflow","spec":{"name":"nested","steps":[]}}]}}),
        json!({"op":"startWorkflow","spec":{"name":"nested","steps":[{"txn":{"steps":[{"op":"schedule","when":{"type":"afterMs","ms":60000},"txn":{"steps":[]}}]}}]}}),
        json!({"op":"cancelSchedule","id":"missing"}),
        json!({"op":"cancelWorkflow","id":"missing"}),
        json!({"op":"expectBefore","deadlineMs":now_ms()+60000}),
    ];
    for fields in [
        json!({"state":"delivered","delivered_ms":0}),
        json!({"state":"acked","acked_ms":2,"ack_note":"","body":"forged"}),
        json!({"state":"queued"}),
        json!({"body":"forged"}),
    ] {
        steps.push(json!({"op":"patch","table":"messages","id":id,"fields":fields}));
    }
    for step in steps {
        let response = mesh_post(
            addr,
            &remote,
            "/api/mutate",
            json!({"db":db,"txn":{"steps":[{"op":"insert","table":"agents","doc":agent_doc("otherhost")},step.clone()]}}),
        )
        .await;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::FORBIDDEN,
            "step={step}: {}",
            response.text().await.unwrap()
        );
        let count = mesh_post(
            addr,
            &remote,
            "/api/query",
            json!({"db":db,"query":{"table":"agents","count":true}}),
        )
        .await
        .json::<serde_json::Value>()
        .await
        .unwrap();
        assert_eq!(
            count["result"],
            json!(0),
            "denied {step} must roll back earlier insert"
        );
    }
    let response = mesh_post(addr,&local,"/api/mutate",json!({"db":db,"txn":{"steps":[{"op":"patch","table":"messages","id":id,"fields":{"state":"acked","acked_ms":2,"ack_note":"forged"}}]}})).await;
    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn mesh_invisible_rows_are_absent_in_queries_and_preconditions() {
    let (_state, addr, db, local, remote) = mesh_fixture().await;
    let doc = message_doc("otherhost", "otherhost");
    let msgid = doc["msgid"].clone();
    let response = mesh_post(
        addr,
        &remote,
        "/api/mutate",
        json!({"db":db,"txn":{"steps":[{"op":"insert","table":"messages","doc":doc}]}}),
    )
    .await;
    let body: serde_json::Value = response.json().await.unwrap();
    let id = body["results"][0]["id"].as_str().unwrap();
    for (query, expected) in [
        (json!({"table":"messages","get":id}), json!(null)),
        (json!({"table":"messages","count":true}), json!(0)),
        (
            json!({"table":"messages","index":"by_msgid","distinct":true}),
            json!([]),
        ),
        (
            json!({"table":"messages","index":"by_ts","aggregate":{"op":"sum"}}),
            json!(null),
        ),
        (json!({"table":"messages","take":1}), json!([])),
        (
            json!({"table":"messages","search":{"index":"search_body","query":"secret"}}),
            json!([]),
        ),
    ] {
        let response = mesh_post(addr, &local, "/api/query", json!({"db":db,"query":query})).await;
        assert!(
            response.status().is_success(),
            "{}",
            response.text().await.unwrap()
        );
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap()["result"],
            expected
        );
    }
    let response = mesh_post(addr,&local,"/api/mutate",json!({"db":db,"txn":{"steps":[{"op":"expectAbsent","table":"messages","index":"by_msgid","eq":[msgid]}]}})).await;
    assert!(
        response.status().is_success(),
        "{}",
        response.text().await.unwrap()
    );
    let response = mesh_post(addr,&local,"/api/mutate",json!({"db":db,"txn":{"steps":[{"op":"expectVersion","table":"messages","id":id,"version":1}]}})).await;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn mesh_non_document_http_routes_denied() {
    let (_state, addr, db, local, _remote) = mesh_fixture().await;
    for path in ["/api/schedules", "/api/workflows/list"] {
        let response = mesh_post(addr, &local, path, json!({"db":db})).await;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::FORBIDDEN,
            "{path}: {}",
            response.text().await.unwrap()
        );
    }
    for path in [
        format!("/api/db/{db}/schema"),
        format!("/api/db/{db}/changes"),
        format!("/api/storage/{db}/missing/metadata"),
        format!("/api/storage/{db}/missing/signed-url"),
        format!("/api/storage/{db}/missing"),
    ] {
        let response = reqwest::Client::new()
            .get(format!("http://{addr}{path}"))
            .bearer_auth(&local)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            reqwest::StatusCode::FORBIDDEN,
            "{path}: {}",
            response.text().await.unwrap()
        );
    }
}

#[tokio::test]
async fn mesh_ws_subscription_rerun_revocation_and_denials() {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let (state, addr, db, local, remote) = mesh_fixture().await;
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/sync"))
        .await
        .unwrap();
    ws.send(Message::Text(
        json!({"type":"auth","db":db,"token":local})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let reply = ws.next().await.unwrap().unwrap();
    assert!(reply.to_text().unwrap().contains("authOk"), "{reply}");
    ws.send(Message::Text(
        json!({"type":"subscribe","queryId":"directory","query":{"table":"agents"}})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let reply = ws.next().await.unwrap().unwrap();
    assert!(reply.to_text().unwrap().contains("queryUpdate"), "{reply}");
    let response = mesh_post(addr,&remote,"/api/mutate",json!({"db":db,"txn":{"steps":[{"op":"insert","table":"agents","doc":agent_doc("otherhost")}]}})).await;
    assert!(response.status().is_success());
    let reply = tokio::time::timeout(std::time::Duration::from_secs(3), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        reply.to_text().unwrap().contains("worker@otherhost:mesh"),
        "{reply}"
    );
    ws.send(Message::Text(json!({"type":"mutate","mutId":"spoof","txn":{"steps":[{"op":"insert","table":"messages","doc":message_doc("otherhost","testhost")}]}}).to_string().into())).await.unwrap();
    let reply = ws.next().await.unwrap().unwrap();
    assert!(reply.to_text().unwrap().contains("FORBIDDEN"), "{reply}");
    sqlx::query("UPDATE rtdb_auth.machine_tokens SET revoked = true WHERE token_hash = $1")
        .bind(sha256_hex(&local))
        .execute(&state.pool)
        .await
        .unwrap();
    let mut doc = agent_doc("otherhost");
    doc["addr"] = json!("second@otherhost:mesh");
    doc["name"] = json!("second");
    let response = mesh_post(
        addr,
        &remote,
        "/api/mutate",
        json!({"db":db,"txn":{"steps":[{"op":"insert","table":"agents","doc":doc}]}}),
    )
    .await;
    assert!(response.status().is_success());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), ws.next())
            .await
            .is_err(),
        "revoked subscription pushed data"
    );
    ws.send(Message::Text(
        json!({"type":"subscribe","queryId":"again","query":{"table":"agents"}})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let reply = ws.next().await.unwrap().unwrap();
    assert!(reply.to_text().unwrap().contains("UNAUTHORIZED"), "{reply}");
}

#[tokio::test]
async fn mesh_agent_patch_routing_and_completed_receipt_denials() {
    let (_state, addr, db, local, remote) = mesh_fixture().await;
    let response = mesh_post(addr,&local,"/api/mutate",json!({"db":db,"txn":{"steps":[{"op":"insert","table":"agents","doc":agent_doc("testhost")}]}})).await;
    let body: serde_json::Value = response.json().await.unwrap();
    let id = body["results"][0]["id"].as_str().unwrap();
    let response = mesh_post(addr,&local,"/api/mutate",json!({"db":db,"txn":{"steps":[{"op":"patch","table":"agents","id":id,"fields":{"status":"offline"}}]}})).await;
    assert!(response.status().is_success());
    for (token, fields) in [
        (&remote, json!({"status":"offline"})),
        (&local, json!({"host":"otherhost"})),
        (&local, json!({"addr":"*@testhost:mesh"})),
        (&local, json!({"project":"different"})),
    ] {
        let response = mesh_post(addr,token,"/api/mutate",json!({"db":db,"txn":{"steps":[{"op":"patch","table":"agents","id":id,"fields":fields}]}})).await;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::FORBIDDEN,
            "{}",
            response.text().await.unwrap()
        );
    }
    for (field, value) in [
        ("from_addr", "*@testhost:mesh"),
        ("target", "*@otherhost:mesh"),
        ("host", "testhost"),
        ("to_addr", "worker@otherhost:wrong"),
        ("priority", "urgent"),
        ("state", "acked"),
        ("ack_note", "forged"),
    ] {
        let mut doc = message_doc("testhost", "otherhost");
        doc[field] = json!(value);
        let response = mesh_post(
            addr,
            &local,
            "/api/mutate",
            json!({"db":db,"txn":{"steps":[{"op":"insert","table":"messages","doc":doc}]}}),
        )
        .await;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::FORBIDDEN,
            "{field}: {}",
            response.text().await.unwrap()
        );
    }
    let response = mesh_post(addr,&local,"/api/mutate",json!({"db":db,"txn":{"steps":[{"op":"insert","table":"messages","doc":message_doc("testhost","otherhost")}]}})).await;
    let body: serde_json::Value = response.json().await.unwrap();
    let id = body["results"][0]["id"].as_str().unwrap();
    let fields = json!({"state":"acked","acked_ms":2,"ack_note":"done"});
    let response = mesh_post(addr,&remote,"/api/mutate",json!({"db":db,"txn":{"steps":[{"op":"patch","table":"messages","id":id,"fields":fields}]}})).await;
    assert!(response.status().is_success());
    let response = mesh_post(addr,&remote,"/api/mutate",json!({"db":db,"txn":{"steps":[{"op":"patch","table":"messages","id":id,"fields":fields}]}})).await;
    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn mesh_ws_presence_denied() {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let (_state, addr, db, local, _remote) = mesh_fixture().await;
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/sync"))
        .await
        .unwrap();
    ws.send(Message::Text(
        json!({"type":"auth","db":db,"token":local})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    ws.next().await.unwrap().unwrap();
    ws.send(Message::Text(
        json!({"type":"presence","room":"mesh","state":{}})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let reply = ws.next().await.unwrap().unwrap();
    assert!(reply.to_text().unwrap().contains("FORBIDDEN"), "{reply}");
}

#[tokio::test]
async fn mesh_idempotency_does_not_replay_other_hosts_results() {
    let (_state, addr, db, local, remote) = mesh_fixture().await;
    let response=mesh_post(addr,&remote,"/api/mutate",json!({"db":db,"idempotencyKey":"shared-key","txn":{"steps":[{"op":"insert","table":"messages","doc":message_doc("otherhost","otherhost")}]}})).await;
    assert!(response.status().is_success());
    let response=mesh_post(addr,&local,"/api/mutate",json!({"db":db,"idempotencyKey":"shared-key","txn":{"steps":[{"op":"delete","table":"messages","id":"invisible"}]}})).await;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::FORBIDDEN,
        "{}",
        response.text().await.unwrap()
    );
}

#[tokio::test]
async fn mesh_forwarded_context_is_enforced_by_owner() {
    use crate::common::cluster::{Cluster, ReplicaId, mutate_until_landed};
    let schema = serde_json::from_value(
        json!({"tables":{"items":{"fields":{"title":{"type":"string"}},"indexes":[]}}}),
    )
    .unwrap();
    let cluster = Cluster::two(schema).await;
    let shadow = &cluster.replica(ReplicaId::B).state;
    let mut ctx = PrincipalCtx::bypass();
    ctx.host_capability = Some(HostCapability::ParMeshV1 {
        host: "testhost".into(),
    });
    let txn = serde_json::from_value(
        json!({"steps":[{"op":"insert","table":"items","doc":{"title":"forbidden"}}]}),
    )
    .unwrap();
    let error = mutate_until_landed(shadow, &cluster.db, txn, ctx)
        .await
        .expect_err("owner enforces forwarded capability");
    assert_eq!(
        error
            .downcast_ref::<rtdb_server::error::RtDbError>()
            .expect("forwarded error")
            .code,
        rtdb_server::error::ErrorCode::Forbidden
    );
    let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM \"db_{}\".\"t_items\"",
        cluster.db
    )))
    .fetch_one(&shadow.pool)
    .await
    .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn mesh_malformed_legacy_rows_invisible_and_legacy_bypass_unchanged() {
    let (state, addr, db, local, _remote) = mesh_fixture().await;
    let (_, legacy) = mint_token(&state.pool, &db, "legacy", None, false, None)
        .await
        .unwrap();
    let mut malformed = message_doc("testhost", "otherhost");
    malformed["to_addr"] = json!("wrong@otherhost:otherproject");
    let msgid = malformed["msgid"].clone();
    let response = mesh_post(
        addr,
        &legacy,
        "/api/mutate",
        json!({"db":db,"txn":{"steps":[{"op":"insert","table":"messages","doc":malformed}]}}),
    )
    .await;
    assert!(response.status().is_success());
    let body: serde_json::Value = response.json().await.unwrap();
    let id = body["results"][0]["id"].as_str().unwrap();
    for query in [
        json!({"table":"messages","get":id}),
        json!({"table":"messages","count":true}),
        json!({"table":"messages","search":{"index":"search_body","query":"secret"}}),
    ] {
        let response = mesh_post(addr, &local, "/api/query", json!({"db":db,"query":query})).await;
        assert!(response.status().is_success());
        let result = response.json::<serde_json::Value>().await.unwrap()["result"].clone();
        assert!(
            result.is_null() || result == json!(0) || result == json!([]),
            "{result}"
        );
    }
    let response=mesh_post(addr,&local,"/api/mutate",json!({"db":db,"txn":{"steps":[{"op":"expectFields","table":"messages","id":id,"expected":[{"path":["state"],"value":"queued"}]}]}})).await;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::NOT_FOUND,
        "{}",
        response.text().await.unwrap()
    );
    let response=mesh_post(addr,&local,"/api/mutate",json!({"db":db,"txn":{"steps":[{"op":"expectAbsent","table":"messages","index":"by_msgid","eq":[msgid]}]}})).await;
    assert!(response.status().is_success());
    let response = mesh_post(
        addr,
        &legacy,
        "/api/mutate",
        json!({"db":db,"txn":{"steps":[{"op":"delete","table":"messages","id":id}]}}),
    )
    .await;
    assert!(response.status().is_success());
}

#[tokio::test]
async fn review_public_storage_preserves_ambient_credentials() {
    let (state, addr, db, host, _) = mesh_fixture().await;
    let (_, legacy) = mint_token(&state.pool, &db, "storage", None, false, None)
        .await
        .unwrap();
    let user = crate::common::mint_user_session(
        &state.pool,
        "storage-review-user",
        "storage-review@example.com",
    )
    .await;
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{addr}/api/storage/{db}"))
        .bearer_auth(&legacy)
        .body("review blob")
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let id = response.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let response = client
        .get(format!("http://{addr}/api/storage/{db}/{id}/signed-url"))
        .bearer_auth(&legacy)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let signed = response.json::<serde_json::Value>().await.unwrap()["url"]
        .as_str()
        .unwrap()
        .to_owned();
    let login = client
        .post(format!("http://{addr}/admin/login"))
        .json(&json!({"adminKey":"test-admin-key"}))
        .send()
        .await
        .unwrap();
    assert!(login.status().is_success());
    let cookie = login
        .headers()
        .get_all("set-cookie")
        .iter()
        .find_map(|v| {
            let s = v.to_str().unwrap();
            s.starts_with("rtdb_session=")
                .then(|| s.split(';').next().unwrap().to_owned())
        })
        .unwrap();
    for url in [
        format!("http://{addr}/storage/{id}"),
        format!(
            "http://{addr}{}",
            signed.strip_prefix("http://localhost:0").unwrap_or(&signed)
        ),
    ] {
        for (header, value) in [
            ("cookie", cookie.as_str()),
            ("cookie", "rtdb_session=stale"),
            ("authorization", "Bearer unrelated"),
            ("authorization", &format!("Bearer {legacy}")),
            ("authorization", &format!("Bearer {user}")),
        ] {
            let response = client.get(&url).header(header, value).send().await.unwrap();
            assert_eq!(
                response.status(),
                reqwest::StatusCode::OK,
                "{header} {url}: {}",
                response.text().await.unwrap()
            );
        }
        assert_eq!(
            client
                .get(&url)
                .bearer_auth(&host)
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::FORBIDDEN
        );
    }
    assert_eq!(
        client
            .post(format!("http://{addr}/api/storage/{db}"))
            .bearer_auth(&host)
            .body("denied")
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );
    assert_eq!(
        client
            .delete(format!("http://{addr}/api/storage/{db}/{id}"))
            .bearer_auth(&host)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );
    assert_eq!(
        client
            .get(format!("http://{addr}/storage/{id}"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap(),
        "review blob"
    );
    assert!(
        client
            .delete(format!("http://{addr}/api/storage/{db}/{id}"))
            .bearer_auth(&legacy)
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
}

#[tokio::test]
async fn review_replay_namespaces_are_disjoint() {
    let (state, addr, db, host, remote) = mesh_fixture().await;
    let (_, legacy) = mint_token(&state.pool, &db, "legacy", None, false, None)
        .await
        .unwrap();
    let restricted: serde_json::Value = admin_post(
        addr,
        "/admin/mint-token",
        json!({"db":db,"name":"narrow","tables":["messages"],"hostCapability":host_cap()}),
    )
    .await
    .json()
    .await
    .unwrap();
    let narrow = restricted["token"].as_str().unwrap().to_owned();
    let derived = |key: &str| format!("host:{}", sha256_hex(&json!([host_cap(), key]).to_string()));
    for (first, first_key, first_doc, second, second_key, second_doc) in [
        (
            &host,
            "scope-key".to_owned(),
            message_doc("testhost", "testhost"),
            &narrow,
            "scope-key".to_owned(),
            message_doc("testhost", "testhost"),
        ),
        (
            &legacy,
            json!([host_cap(), null, "literal-tuple"]).to_string(),
            message_doc("otherhost", "otherhost"),
            &host,
            "literal-tuple".to_owned(),
            message_doc("testhost", "testhost"),
        ),
        (
            &legacy,
            derived("legacy-first"),
            message_doc("otherhost", "otherhost"),
            &host,
            "legacy-first".to_owned(),
            message_doc("testhost", "testhost"),
        ),
        (
            &host,
            "host-first".to_owned(),
            message_doc("testhost", "testhost"),
            &legacy,
            derived("host-first"),
            message_doc("otherhost", "otherhost"),
        ),
        (
            &host,
            "same-key".to_owned(),
            message_doc("testhost", "testhost"),
            &remote,
            "same-key".to_owned(),
            message_doc("otherhost", "otherhost"),
        ),
    ] {
        let mut ids = Vec::new();
        for (token, key, doc) in [
            (first, first_key, first_doc),
            (second, second_key, second_doc),
        ] {
            let request = json!({"db":db,"idempotencyKey":key,"txn":{"steps":[{"op":"insert","table":"messages","doc":doc}]}});
            let response = mesh_post(addr, token, "/api/mutate", request.clone()).await;
            assert!(response.status().is_success());
            let body = response.json::<serde_json::Value>().await.unwrap();
            let repeated = mesh_post(addr, token, "/api/mutate", request)
                .await
                .json::<serde_json::Value>()
                .await
                .unwrap();
            assert_eq!(body, repeated, "same-identity retry");
            ids.push(body["results"][0]["id"].clone());
        }
        assert_ne!(
            ids[0], ids[1],
            "different identities must execute their own allowed insert"
        );
    }
}

#[tokio::test]
async fn review_ranked_and_paginated_visibility_matrix() {
    let (state, addr, db, local, remote) = mesh_fixture().await;
    let schema = state.schemas.get(&state.pool, &db).await.unwrap();
    let mut schema = (*schema).clone();
    let table = schema.tables.get_mut("messages").unwrap();
    table.fields.insert(
        "embedding".into(),
        rtdb_server::schema::FieldType::Vector { dimensions: 3 },
    );
    table.indexes.push(serde_json::from_value(json!({"name":"by_embedding","fields":["embedding"],"vector":{"dimensions":3,"metric":"cosine"}})).unwrap());
    rtdb_server::ddl::push_schema(&state.pool, &db, schema)
        .await
        .unwrap();
    state.schemas.invalidate(&db).await;
    let mut visible_id = serde_json::Value::Null;
    for (token, sender) in [(&remote, "otherhost"), (&local, "testhost")] {
        let mut doc = message_doc(sender, sender);
        doc["embedding"] = json!([1, 0, 0]);
        let response = mesh_post(
            addr,
            token,
            "/api/mutate",
            json!({"db":db,"txn":{"steps":[{"op":"insert","table":"messages","doc":doc}]}}),
        )
        .await;
        assert!(
            response.status().is_success(),
            "{}",
            response.text().await.unwrap()
        );
        if sender == "testhost" {
            visible_id =
                response.json::<serde_json::Value>().await.unwrap()["results"][0]["id"].clone();
        }
    }
    for mut query in [
        json!({"table":"messages","vectorSearch":{"index":"by_embedding","vector":[1,0,0],"limit":1}}),
        json!({"table":"messages","hybridSearch":{"query":"secret","vector":[1,0,0],"limit":1}}),
        json!({"table":"messages","paginate":{"numItems":1}}),
    ] {
        for paginated in [false, true] {
            if paginated {
                query["paginate"] = json!({"numItems":1});
            }
            let response =
                mesh_post(addr, &local, "/api/query", json!({"db":db,"query":query})).await;
            assert!(
                response.status().is_success(),
                "{}",
                response.text().await.unwrap()
            );
            let body = response.json::<serde_json::Value>().await.unwrap();
            let result = &body["result"];
            let docs = result
                .as_array()
                .or_else(|| result["docs"].as_array())
                .unwrap();
            assert_eq!(docs.len(), 1, "{body}");
            assert_eq!(docs[0]["_id"], visible_id, "{body}");
        }
    }
}

#[tokio::test]
async fn review_message_subscription_matrix() {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let (state, addr, db, sender, recipient) = mesh_fixture().await;
    let minted: serde_json::Value = admin_post(
        addr,
        "/admin/mint-token",
        json!({"db":db,"name":"third","hostCapability":{"type":"parMeshV1","host":"thirdhost"}}),
    )
    .await
    .json()
    .await
    .unwrap();
    let unrelated = minted["token"].as_str().unwrap();
    let (_, legacy) = mint_token(&state.pool, &db, "legacy", None, false, None)
        .await
        .unwrap();
    let mut malformed = message_doc("testhost", "otherhost");
    malformed["to_addr"] = json!("wrong@otherhost:wrong");
    let response=mesh_post(addr,&legacy,"/api/mutate",json!({"db":db,"txn":{"steps":[{"op":"insert","table":"messages","doc":malformed},{"op":"insert","table":"messages","doc":message_doc("testhost","otherhost")}]}})).await;
    assert!(response.status().is_success());
    let body: serde_json::Value = response.json().await.unwrap();
    let id = body["results"][1]["id"].as_str().unwrap();
    let mut sockets = Vec::new();
    for (token, expected) in [(&sender, 1), (&recipient, 1), (&unrelated.to_owned(), 0)] {
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/sync"))
            .await
            .unwrap();
        ws.send(Message::Text(
            json!({"type":"auth","db":db,"token":token})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        assert!(
            ws.next()
                .await
                .unwrap()
                .unwrap()
                .to_text()
                .unwrap()
                .contains("authOk")
        );
        ws.send(Message::Text(
            json!({"type":"subscribe","queryId":"messages","query":{"table":"messages"}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        let reply = ws.next().await.unwrap().unwrap();
        let update: serde_json::Value = serde_json::from_str(reply.to_text().unwrap()).unwrap();
        assert_eq!(
            update["result"].as_array().unwrap().len(),
            expected,
            "{update}"
        );
        sockets.push(ws);
    }
    let response=mesh_post(addr,&recipient,"/api/mutate",json!({"db":db,"txn":{"steps":[{"op":"patch","table":"messages","id":id,"fields":{"state":"delivered","delivered_ms":2}}]}})).await;
    assert!(response.status().is_success());
    for ws in &mut sockets[..2] {
        let reply = tokio::time::timeout(std::time::Duration::from_secs(3), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(reply.to_text().unwrap().contains("delivered"), "{reply}");
    }
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(150), sockets[2].next())
            .await
            .is_err()
    );
    sqlx::query("UPDATE rtdb_auth.machine_tokens SET revoked=true WHERE token_hash=$1")
        .bind(sha256_hex(&sender))
        .execute(&state.pool)
        .await
        .unwrap();
    let response=mesh_post(addr,&recipient,"/api/mutate",json!({"db":db,"txn":{"steps":[{"op":"patch","table":"messages","id":id,"fields":{"state":"acked","acked_ms":3,"ack_note":"done"}}]}})).await;
    assert!(response.status().is_success());
    let reply = tokio::time::timeout(std::time::Duration::from_secs(3), sockets[1].next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(reply.to_text().unwrap().contains("acked"));
    for i in [0, 2] {
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(150), sockets[i].next())
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn review_invisible_patch_is_absent_before_validation() {
    let (_state, addr, db, local, remote) = mesh_fixture().await;
    let response=mesh_post(addr,&remote,"/api/mutate",json!({"db":db,"txn":{"steps":[{"op":"insert","table":"messages","doc":message_doc("otherhost","otherhost")}]}})).await;
    let body: serde_json::Value = response.json().await.unwrap();
    let foreign = body["results"][0]["id"].as_str().unwrap();
    for fields in [
        json!({}),
        json!({"ts_ms":"invalid"}),
        json!({"_creationTime":0}),
        json!({"body":{"invalid":true}}),
    ] {
        let mut outcomes = Vec::new();
        for id in ["missing-id", foreign] {
            let response=mesh_post(addr,&local,"/api/mutate",json!({"db":db,"txn":{"steps":[{"op":"insert","table":"agents","doc":agent_doc("testhost")},{"op":"patch","table":"messages","id":id,"fields":fields}]}})).await;
            assert_eq!(
                response.status(),
                reqwest::StatusCode::NOT_FOUND,
                "{}",
                response.text().await.unwrap()
            );
            outcomes.push(response.json::<serde_json::Value>().await.unwrap()["code"].clone());
            let count = mesh_post(
                addr,
                &local,
                "/api/query",
                json!({"db":db,"query":{"table":"agents","count":true}}),
            )
            .await
            .json::<serde_json::Value>()
            .await
            .unwrap();
            assert_eq!(count["result"], json!(0));
        }
        assert_eq!(outcomes[0], outcomes[1]);
    }
}

#[tokio::test]
async fn review_validate_denies_active_and_expired_host_only() {
    let (state, addr, db, host, _) = mesh_fixture().await;
    let (_, legacy) = mint_token(&state.pool, &db, "legacy", None, false, None)
        .await
        .unwrap();
    let user = crate::common::mint_user_session(
        &state.pool,
        "validate-review",
        "validate-review@example.com",
    )
    .await;
    let client = reqwest::Client::new();
    for token in [&legacy, &user] {
        assert_eq!(
            client
                .get(format!("http://{addr}/auth/validate"))
                .bearer_auth(token)
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::OK
        );
    }
    for expired in [false, true] {
        if expired {
            sqlx::query("UPDATE rtdb_auth.machine_tokens SET expires_at=$1 WHERE token_hash=$2")
                .bind(now_ms() - 1000)
                .bind(sha256_hex(&host))
                .execute(&state.pool)
                .await
                .unwrap();
        }
        assert_eq!(
            client
                .get(format!("http://{addr}/auth/validate"))
                .bearer_auth(&host)
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::FORBIDDEN
        );
    }
}

#[tokio::test]
async fn enrollment_grant_dies_with_database_and_cannot_resurrect() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let grant = create_grant(addr, &db).await;
    rtdb_server::db::drop_database(&state.pool, &db)
        .await
        .unwrap();
    let unknown = enroll(addr, json!({"grant":"unknown"}))
        .await
        .json::<serde_json::Value>()
        .await
        .unwrap();
    for recreated in [false, true] {
        if recreated {
            rtdb_server::db::create_database(&state.pool, &db)
                .await
                .unwrap();
        }
        let response = enroll(addr, json!({"grant":grant["grant"]})).await;
        assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
        assert_eq!(response.json::<serde_json::Value>().await.unwrap(), unknown);
    }
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM rtdb_auth.machine_tokens WHERE db_name=$1")
            .bind(db.as_str())
            .fetch_one(&state.pool)
            .await
            .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn enrollment_creation_and_redemption_wait_for_database_deletion_lock() {
    use rtdb_server::auth::enrollment::{create_grant as create, redeem_grant};
    for redeem in [false, true] {
        let state = test_state().await;
        let db = fresh_db(&state).await;
        let cap = HostCapability::ParMeshV1 {
            host: "testhost".into(),
        };
        let grant = create(&state.pool, &db, "race", &cap, now_ms() + 60000, None)
            .await
            .unwrap();
        let mut deletion = state.pool.begin().await.unwrap();
        sqlx::query("SELECT name FROM rtdb_auth.databases WHERE name=$1 FOR UPDATE")
            .bind(db.as_str())
            .fetch_one(&mut *deletion)
            .await
            .unwrap();
        let pool = state.pool.clone();
        let name = db.as_str().to_owned();
        let mut operation = tokio::spawn(async move {
            if redeem {
                redeem_grant(&pool, &grant.grant).await.map(|_| ())
            } else {
                create(&pool, &name, "late", &cap, now_ms() + 60000, None)
                    .await
                    .map(|_| ())
            }
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(150), &mut operation)
                .await
                .is_err(),
            "enrollment must wait for database lifecycle lock"
        );
        sqlx::query("DELETE FROM rtdb_auth.databases WHERE name=$1")
            .bind(db.as_str())
            .execute(&mut *deletion)
            .await
            .unwrap();
        sqlx::query("DELETE FROM rtdb_auth.machine_tokens WHERE db_name=$1")
            .bind(db.as_str())
            .execute(&mut *deletion)
            .await
            .unwrap();
        deletion.commit().await.unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(3), operation)
            .await
            .unwrap()
            .unwrap();
        assert!(result.is_err());
        let tokens: i64 =
            sqlx::query_scalar("SELECT count(*) FROM rtdb_auth.machine_tokens WHERE db_name=$1")
                .bind(db.as_str())
                .fetch_one(&state.pool)
                .await
                .unwrap();
        let grants: i64 =
            sqlx::query_scalar("SELECT count(*) FROM rtdb_auth.enrollment_grants WHERE db_name=$1")
                .bind(db.as_str())
                .fetch_one(&state.pool)
                .await
                .unwrap();
        assert_eq!((tokens, grants), (0, 0));
        // Recreate registry so TestDb can cascade-drop the test's tenant schema.
        sqlx::query("INSERT INTO rtdb_auth.databases(name,created_at) VALUES($1,$2)")
            .bind(db.as_str())
            .bind(now_ms())
            .execute(&state.pool)
            .await
            .unwrap();
    }
}

fn host_cap() -> serde_json::Value {
    serde_json::json!({"type": "parMeshV1", "host": "testhost"})
}

/// `POST /auth/enroll` with the given body, unauthenticated.
async fn enroll(addr: SocketAddr, body: serde_json::Value) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("http://{addr}/auth/enroll"))
        .json(&body)
        .send()
        .await
        .expect("send enroll request")
}

/// Creates a standard 60-second grant for `db` on host `testhost`.
async fn create_grant(addr: SocketAddr, db: &str) -> serde_json::Value {
    admin_post(
        addr,
        "/admin/enrollment-grants",
        json!({
            "db": db,
            "name": "host-token",
            "capability": host_cap(),
            "expiresAt": now_ms() + 60_000,
        }),
    )
    .await
    .json()
    .await
    .expect("parse grant response")
}

// --- Capability persistence and propagation --------------------------------

/// A legacy mint (no `hostCapability`) stores NULL and resolves to a Machine
/// principal without a capability — pre-feature behavior byte-identical.
#[tokio::test]
async fn legacy_mint_stores_null_capability() {
    let state = test_state().await;
    let (id, plaintext) = mint_token(&state.pool, "dbx", "legacy", None, false, None)
        .await
        .expect("mint legacy token");

    let (capability,): (Option<serde_json::Value>,) =
        sqlx::query_as("SELECT capability FROM rtdb_auth.machine_tokens WHERE id = $1")
            .bind(&id)
            .fetch_one(&state.pool)
            .await
            .expect("token row");
    assert_eq!(capability, None);

    let principal = rtdb_server::auth::resolve_bearer(&state.pool, &plaintext)
        .await
        .expect("resolve legacy token");
    let Principal::Machine {
        host_capability, ..
    } = principal
    else {
        panic!("machine token must resolve to a Machine principal");
    };
    assert_eq!(
        host_capability, None,
        "legacy principal must carry no capability"
    );
}

/// `POST /admin/mint-token` with `hostCapability` persists it and
/// `GET /admin/tokens` echoes it back; `resolve_bearer` threads it onto the
/// Machine principal.
#[tokio::test]
async fn minted_capability_persists_lists_and_propagates() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;

    let minted: serde_json::Value = admin_post(
        addr,
        "/admin/mint-token",
        json!({"db": db, "name": "host", "hostCapability": host_cap()}),
    )
    .await
    .json()
    .await
    .expect("parse mint response");
    let token_id = minted["tokenId"].as_str().expect("tokenId").to_string();
    let token = minted["token"].as_str().expect("token").to_string();

    let listed: serde_json::Value = admin_get(addr, &format!("/admin/tokens?db={db}"))
        .await
        .json()
        .await
        .expect("parse token list");
    let row = listed["tokens"]
        .as_array()
        .expect("tokens array")
        .iter()
        .find(|r| r["id"].as_str() == Some(token_id.as_str()))
        .expect("minted row listed")
        .clone();
    assert_eq!(row["hostCapability"], host_cap());

    let principal = rtdb_server::auth::resolve_bearer(&state.pool, &token)
        .await
        .expect("resolve scoped token");
    let Principal::Machine {
        host_capability, ..
    } = principal
    else {
        panic!("machine token must resolve to a Machine principal");
    };
    assert_eq!(
        host_capability,
        Some(HostCapability::ParMeshV1 {
            host: "testhost".to_string()
        }),
        "capability must thread onto the principal"
    );
}

// --- Grant creation ---------------------------------------------------------

/// Creation returns the grant plaintext exactly once and stores only the
/// sha256 digest — the plaintext appears nowhere in the persisted row.
#[tokio::test]
async fn grant_create_returns_plaintext_once_and_stores_only_the_digest() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;

    let grant = create_grant(addr, &db).await;
    let grant_id = grant["grantId"].as_str().expect("grantId").to_string();
    let secret = grant["grant"].as_str().expect("grant").to_string();
    assert_eq!(grant["db"], json!(db.as_str()));
    assert_eq!(grant["capability"], host_cap());
    assert!(!secret.is_empty());

    let row: (String, String, String, String, serde_json::Value) = sqlx::query_as(
        "SELECT db_name, name, token_hash, expires_at::text, capability \
         FROM rtdb_auth.enrollment_grants WHERE id = $1",
    )
    .bind(&grant_id)
    .fetch_one(&state.pool)
    .await
    .expect("grant row");
    assert_eq!(row.0, db.as_str());
    assert_eq!(row.1, "host-token");
    assert_eq!(row.2, sha256_hex(&secret), "only the digest is stored");
    assert_eq!(
        row.4,
        host_cap(),
        "the fixed capability is persisted with the grant"
    );
    // The plaintext must appear nowhere in the row's text columns.
    let all_text: (String, String, String) = sqlx::query_as(
        "SELECT id, db_name, token_hash FROM rtdb_auth.enrollment_grants WHERE id = $1",
    )
    .bind(&grant_id)
    .fetch_one(&state.pool)
    .await
    .expect("grant row");
    for col in [&all_text.0, &all_text.1, &all_text.2] {
        assert_ne!(col, &secret, "grant plaintext must never be stored");
    }
}

/// Expiry validation: an `expiresAt` in the past, or more than 15 minutes out,
/// is rejected; an unknown database is rejected; a malformed capability host
/// is rejected.
#[tokio::test]
async fn grant_create_validates_expiry_db_and_capability() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let now = now_ms();

    let past = admin_post(
        addr,
        "/admin/enrollment-grants",
        json!({
            "db": db, "name": "h", "capability": host_cap(), "expiresAt": now - 1_000,
        }),
    )
    .await;
    assert_eq!(
        past.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "past expiry must 400"
    );

    let far = admin_post(
        addr,
        "/admin/enrollment-grants",
        json!({
            "db": db, "name": "h", "capability": host_cap(), "expiresAt": now + 16 * 60_000,
        }),
    )
    .await;
    assert_eq!(
        far.status(),
        reqwest::StatusCode::BAD_REQUEST,
        ">15-minute expiry must 400"
    );

    let unknown_db = admin_post(
        addr,
        "/admin/enrollment-grants",
        json!({
            "db": "no_such_db_xyz", "name": "h", "capability": host_cap(), "expiresAt": now + 60_000,
        }),
    )
    .await;
    assert_eq!(unknown_db.status(), reqwest::StatusCode::BAD_REQUEST);

    let bad_host = admin_post(
        addr,
        "/admin/enrollment-grants",
        json!({
            "db": db, "name": "h",
            "capability": {"type": "parMeshV1", "host": "Bad Host!"},
            "expiresAt": now + 60_000,
        }),
    )
    .await;
    assert_eq!(bad_host.status(), reqwest::StatusCode::BAD_REQUEST);

    // A grant inside the window is accepted.
    let ok = admin_post(
        addr,
        "/admin/enrollment-grants",
        json!({
            "db": db, "name": "h", "capability": host_cap(), "expiresAt": now + 60_000,
        }),
    )
    .await;
    assert_eq!(ok.status(), reqwest::StatusCode::OK);
}

/// A strict request DTO: unknown fields on the grant-creation body are denied.
#[tokio::test]
async fn grant_create_denies_unknown_fields() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;

    let resp = admin_post(
        addr,
        "/admin/enrollment-grants",
        json!({
            "db": db, "name": "h", "capability": host_cap(),
            "expiresAt": now_ms() + 60_000,
            "host": "spoofed-host",
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
}

/// Non-admin callers cannot create or revoke grants.
#[tokio::test]
async fn non_admin_cannot_create_or_revoke_grants() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;

    let create = reqwest::Client::new()
        .post(format!("http://{addr}/admin/enrollment-grants"))
        .json(&json!({
            "db": db, "name": "h", "capability": host_cap(),
            "expiresAt": now_ms() + 60_000,
        }))
        .send()
        .await
        .expect("send unauthed grant create");
    assert_eq!(create.status(), reqwest::StatusCode::UNAUTHORIZED);

    let revoke = reqwest::Client::new()
        .post(format!("http://{addr}/admin/revoke-enrollment-grant"))
        .json(&json!({"grantId": "whatever"}))
        .send()
        .await
        .expect("send unauthed grant revoke");
    assert_eq!(revoke.status(), reqwest::StatusCode::UNAUTHORIZED);
}

// --- Redemption -------------------------------------------------------------

/// A valid grant redeems exactly once into a working host-scoped machine
/// token; the second attempt gets the generic unauthorized.
#[tokio::test]
async fn enroll_redeems_exactly_once() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let secret = create_grant(addr, &db).await["grant"]
        .as_str()
        .expect("grant")
        .to_string();

    let first = enroll(addr, json!({"grant": secret})).await;
    assert_eq!(
        first.status(),
        reqwest::StatusCode::OK,
        "first redeem must succeed"
    );
    let body: serde_json::Value = first.json().await.expect("parse enroll response");
    assert_eq!(body["db"], json!(db.as_str()));
    assert_eq!(body["capability"], host_cap());
    let token_id = body["tokenId"].as_str().expect("tokenId").to_string();
    let token = body["token"].as_str().expect("token").to_string();
    assert!(!token.is_empty());

    // The minted credential is a live machine token carrying the capability.
    let principal = rtdb_server::auth::resolve_bearer(&state.pool, &token)
        .await
        .expect("resolve enrolled token");
    let Principal::Machine {
        token_id: resolved_id,
        host_capability,
        ..
    } = principal
    else {
        panic!("enrolled token must resolve to a Machine principal");
    };
    assert_eq!(resolved_id, token_id);
    assert_eq!(
        host_capability,
        Some(HostCapability::ParMeshV1 {
            host: "testhost".to_string()
        }),
        "enrolled token carries the grant's capability"
    );

    let second = enroll(addr, json!({"grant": secret})).await;
    assert_eq!(second.status(), reqwest::StatusCode::UNAUTHORIZED);

    // Exactly one machine credential was created for the grant.
    let (count,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM rtdb_auth.machine_tokens WHERE db_name = $1 AND name = 'host-token'",
    )
    .bind(db.as_str())
    .fetch_one(&state.pool)
    .await
    .expect("count tokens");
    assert_eq!(count, 1);
}

/// Invalid, expired, revoked, and unknown grants all receive the same generic
/// unauthorized response — no signal about which check failed.
#[tokio::test]
async fn enroll_rejects_invalid_expired_revoked_and_unknown_grants() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;

    // Unknown grant.
    let unknown = enroll(addr, json!({"grant": "not-a-real-grant"})).await;
    assert_eq!(unknown.status(), reqwest::StatusCode::UNAUTHORIZED);

    // Expired grant: seeded directly with a past expires_at (creation
    // validates expiry, so this row can only come from time passing). The
    // id/secret are per-run unique — the shared auth schema outlives runs.
    let grant_id = uuid::Uuid::now_v7().simple().to_string();
    let secret = format!("expired-{}", grant_id);
    sqlx::query(
        "INSERT INTO rtdb_auth.enrollment_grants \
         (id, db_name, name, token_hash, capability, expires_at, token_expires_at, redeemed_at, revoked, created_at) \
         VALUES ($1, $2, 'h', $3, $4, $5, NULL, NULL, false, $5)",
    )
    .bind(&grant_id)
    .bind(db.as_str())
    .bind(sha256_hex(&secret))
    .bind(host_cap())
    .bind(now_ms() - 1_000)
    .execute(&state.pool)
    .await
    .expect("seed expired grant");
    let expired = enroll(addr, json!({"grant": secret})).await;
    assert_eq!(expired.status(), reqwest::StatusCode::UNAUTHORIZED);

    // Revoked grant: created normally, then revoked via the admin surface.
    let created = create_grant(addr, &db).await;
    let revoked_resp = admin_post(
        addr,
        "/admin/revoke-enrollment-grant",
        json!({"grantId": created["grantId"]}),
    )
    .await;
    assert_eq!(revoked_resp.status(), reqwest::StatusCode::OK);
    let revoked = enroll(addr, json!({"grant": created["grant"]})).await;
    assert_eq!(revoked.status(), reqwest::StatusCode::UNAUTHORIZED);

    // Revoking an unknown grant id is NotFound.
    let missing = admin_post(
        addr,
        "/admin/revoke-enrollment-grant",
        json!({"grantId": "no-such-grant"}),
    )
    .await;
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
}

/// Redemption accepts ONLY `{"grant": ...}` — no host, database, capability,
/// or scope override sneaks through.
#[tokio::test]
async fn enroll_body_is_strict() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let secret = create_grant(addr, &db).await["grant"]
        .as_str()
        .expect("grant")
        .to_string();

    let override_attempt = enroll(
        addr,
        json!({"grant": secret, "db": "other-db", "host": "evil", "capability": host_cap()}),
    )
    .await;
    assert_eq!(
        override_attempt.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "extra fields on the enroll body must be denied"
    );

    // The grant survives untouched for a clean redemption.
    let clean = enroll(addr, json!({"grant": secret})).await;
    assert_eq!(clean.status(), reqwest::StatusCode::OK);
}

/// N concurrent redemptions of one grant yield exactly one success and one
/// created token.
#[tokio::test]
async fn concurrent_redemption_yields_exactly_one_success() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let secret = create_grant(addr, &db).await["grant"]
        .as_str()
        .expect("grant")
        .to_string();

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let secret = secret.clone();
        tasks.spawn(async move { enroll(addr, json!({"grant": secret})).await.status() });
    }
    let mut successes = 0;
    while let Some(joined) = tasks.join_next().await {
        let status = joined.expect("join redeem task");
        if status == reqwest::StatusCode::OK {
            successes += 1;
        }
    }
    assert_eq!(successes, 1, "exactly one concurrent redemption wins");

    let (count,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM rtdb_auth.machine_tokens WHERE db_name = $1 AND name = 'host-token'",
    )
    .bind(db.as_str())
    .fetch_one(&state.pool)
    .await
    .expect("count tokens");
    assert_eq!(count, 1, "exactly one token was minted");
}

/// The enroll mint honours the grant's optional `tokenExpiresAt`.
#[tokio::test]
async fn grant_token_expiry_threads_to_the_minted_token() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token_expires_at = now_ms() + 86_400_000;
    let created: serde_json::Value = admin_post(
        addr,
        "/admin/enrollment-grants",
        json!({
            "db": db, "name": "host-token", "capability": host_cap(),
            "expiresAt": now_ms() + 60_000,
            "tokenExpiresAt": token_expires_at,
        }),
    )
    .await
    .json()
    .await
    .expect("parse grant response");
    let secret = created["grant"].as_str().expect("grant").to_string();

    let body: serde_json::Value = enroll(addr, json!({"grant": secret}))
        .await
        .json()
        .await
        .expect("parse enroll");
    assert!(!body["token"].as_str().expect("token").is_empty());

    let (expires_at,): (Option<i64>,) =
        sqlx::query_as("SELECT expires_at FROM rtdb_auth.machine_tokens WHERE id = $1")
            .bind(body["tokenId"].as_str().expect("tokenId"))
            .fetch_one(&state.pool)
            .await
            .expect("token row");
    assert_eq!(expires_at, Some(token_expires_at));
}
// --- Review fixes (F1–F3) ----------------------------------------------------

/// F1: a supplied `tokenExpiresAt` in the past is rejected with a clear
/// error — never silently filtered into SQL NULL, which would mint a
/// never-expiring credential.
#[tokio::test]
async fn grant_rejects_past_token_expiry() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;

    let resp = admin_post(
        addr,
        "/admin/enrollment-grants",
        json!({
            "db": db, "name": "h", "capability": host_cap(),
            "expiresAt": now_ms() + 60_000,
            "tokenExpiresAt": now_ms() - 1_000,
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    // And the grant was not created at all.
    let (count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM rtdb_auth.enrollment_grants WHERE db_name = $1")
            .bind(db.as_str())
            .fetch_one(&state.pool)
            .await
            .expect("count grants");
    assert_eq!(count, 0);
}

/// F2: expiry is evaluated with the database clock AFTER the grant row's
/// lock is released. A redemption that waits on a concurrent row lock while
/// the grant expires must be rejected — not redeemed with a timestamp
/// captured before the wait.
#[tokio::test]
async fn expired_grant_cannot_redeem_after_claim_races_expiry() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let created: serde_json::Value = admin_post(
        addr,
        "/admin/enrollment-grants",
        json!({
            "db": db, "name": "host-token", "capability": host_cap(),
            "expiresAt": now_ms() + 800,
        }),
    )
    .await
    .json()
    .await
    .expect("parse grant response");
    let grant_id = created["grantId"].as_str().expect("grantId").to_string();
    let secret = created["grant"].as_str().expect("grant").to_string();

    // An independent connection holds the grant row's lock, stalling the
    // claim statement until after the grant has expired.
    let url = std::env::var("RTDB_TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://rtdb:rtdb@127.0.0.1:55434/rtdb".into());
    let blocker = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("connect blocker pool");
    let mut blocker_tx = blocker.begin().await.expect("begin blocker tx");
    sqlx::query("SELECT id FROM rtdb_auth.enrollment_grants WHERE id = $1 FOR UPDATE")
        .bind(&grant_id)
        .fetch_one(&mut *blocker_tx)
        .await
        .expect("lock grant row");

    // The redemption blocks on the claim while the clock passes expiry.
    let redeem = tokio::spawn(async move { enroll(addr, json!({"grant": secret})).await.status() });
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    blocker_tx.commit().await.expect("release the row lock");
    let status = redeem.await.expect("join redeem task");

    assert_eq!(
        status,
        reqwest::StatusCode::UNAUTHORIZED,
        "a grant that expired during the lock wait must not redeem"
    );

    // Nothing was redeemed and nothing was minted.
    let (redeemed,): (Option<i64>,) =
        sqlx::query_as("SELECT redeemed_at FROM rtdb_auth.enrollment_grants WHERE id = $1")
            .bind(&grant_id)
            .fetch_one(&state.pool)
            .await
            .expect("grant row");
    assert_eq!(redeemed, None, "the grant must remain unredeemed");
    let (count,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM rtdb_auth.machine_tokens WHERE db_name = $1 AND name = 'host-token'",
    )
    .bind(db.as_str())
    .fetch_one(&state.pool)
    .await
    .expect("count tokens");
    assert_eq!(
        count, 0,
        "no credential may be minted from an expired grant"
    );
}

/// F3: the Machine principal's capability propagates into the per-row
/// `PrincipalCtx` (and stays `None` for legacy tokens).
#[tokio::test]
async fn row_ctx_propagates_host_capability() {
    let scoped = Principal::Machine {
        db: "d".to_string(),
        token_id: "t".to_string(),
        read_only: false,
        tables: None,
        host_capability: Some(HostCapability::ParMeshV1 {
            host: "macdaddy".to_string(),
        }),
    };
    let ctx = scoped.row_ctx();
    assert_eq!(
        ctx.host_capability,
        Some(HostCapability::ParMeshV1 {
            host: "macdaddy".to_string()
        }),
        "row_ctx must carry the capability"
    );

    let legacy = Principal::Machine {
        db: "d".to_string(),
        token_id: "t".to_string(),
        read_only: false,
        tables: None,
        host_capability: None,
    };
    assert_eq!(
        legacy.row_ctx().host_capability,
        None,
        "legacy tokens carry no capability into the ctx"
    );
}

/// F3: `PrincipalCtx` round-trips the capability through serde and decodes
/// a body without the field as `None` (legacy/mixed-fleet compatibility).
#[test]
fn principal_ctx_capability_round_trips_and_defaults_to_none() {
    let ctx = PrincipalCtx {
        user_id: None,
        email: None,
        tables: None,
        host_capability: Some(HostCapability::ParMeshV1 {
            host: "macdaddy".to_string(),
        }),
        enqueuer: None,
        admin: false,
        session_hash: None,
    };
    let json = serde_json::to_value(&ctx).expect("serialize ctx");
    // `PrincipalCtx` has no rename_all: its serde names are the Rust field
    // names (this payload is server-internal — forwarded writes and fire
    // contexts — not public wire vocabulary).
    assert_eq!(
        json["host_capability"],
        serde_json::json!({"type": "parMeshV1", "host": "macdaddy"}),
        "the ctx serializes the capability under host_capability"
    );
    let back: PrincipalCtx = serde_json::from_value(json).expect("decode ctx");
    assert_eq!(
        back.host_capability,
        Some(HostCapability::ParMeshV1 {
            host: "macdaddy".to_string()
        })
    );

    let legacy_json = serde_json::json!({
        "user_id": null, "email": null, "tables": null, "enqueuer": null
    });
    let legacy: PrincipalCtx = serde_json::from_value(legacy_json).expect("decode legacy ctx");
    assert_eq!(legacy.host_capability, None);
}

/// F3: a persisted capability that fails the strict deserializer must fail
/// closed on bearer resolution — never resolve as legacy (NULL semantics).
#[tokio::test]
async fn malformed_stored_capability_fails_closed() {
    let state = test_state().await;
    let (id, plaintext) = mint_token(&state.pool, "dbx", "broken", None, false, None)
        .await
        .expect("mint token");
    sqlx::query("UPDATE rtdb_auth.machine_tokens SET capability = $1 WHERE id = $2")
        .bind(serde_json::json!({"type": "parMeshV9", "host": "macdaddy"}))
        .bind(&id)
        .execute(&state.pool)
        .await
        .expect("corrupt stored capability");

    let result = rtdb_server::auth::resolve_bearer(&state.pool, &plaintext).await;
    assert!(
        result.is_err(),
        "a malformed stored capability must fail closed, not resolve as legacy"
    );
}
