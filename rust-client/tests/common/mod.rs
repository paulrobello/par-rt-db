//! Shared harness for opt-in live-server integration tests.
//! Creates a uniquely-named `t<uuid>` database, pushes a tiny schema, and mints
//! a machine token. Tests never touch a db they didn't create.

use serde::{Deserialize, Serialize};

#[allow(dead_code)]
pub struct Ctx {
    pub url: String,
    pub db: String,
    pub token: String,
    /// Retained for future teardown/revoke helpers (Plan 2 admin tests).
    pub admin_key: String,
}

/// Read the opt-in env vars. `None` unless both `RTDB_TEST_SERVER_URL` and
/// `RTDB_TEST_ADMIN_KEY` are set — tests call this to guard early.
pub fn env() -> Option<(String, String)> {
    let url = std::env::var("RTDB_TEST_SERVER_URL").ok()?;
    let admin = std::env::var("RTDB_TEST_ADMIN_KEY").ok()?;
    Some((url, admin))
}

pub async fn setup() -> Ctx {
    let (url, admin_key) = env().expect("RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY must be set");
    let client = reqwest::Client::new();
    let db = format!("t{}", uuid_v7());

    #[derive(Serialize)]
    struct CreateDb<'a> {
        name: &'a str,
    }
    #[derive(Deserialize)]
    #[allow(dead_code)]
    struct OkResp {
        ok: bool,
    }
    post::<CreateDb, OkResp>(
        &client,
        &url,
        "/admin/create-db",
        &admin_key,
        &CreateDb { name: &db },
    )
    .await;

    let schema = serde_json::json!({
        "tables": {
            "items": {
                "fields": { "name": {"type":"string"}, "n": {"type":"number"} },
                "indexes": [ {"name":"by_n","fields":["n"]} ]
            }
        }
    });
    #[derive(Serialize)]
    struct Push<'a> {
        db: &'a str,
        schema: serde_json::Value,
    }
    post::<Push, OkResp>(
        &client,
        &url,
        "/admin/push-schema",
        &admin_key,
        &Push { db: &db, schema },
    )
    .await;

    #[derive(Serialize)]
    struct Mint<'a> {
        db: &'a str,
        name: &'a str,
    }
    #[derive(Deserialize)]
    #[allow(dead_code)]
    struct Minted {
        #[serde(rename = "tokenId")]
        token_id: String,
        token: String,
    }
    let minted = post::<Mint, Minted>(
        &client,
        &url,
        "/admin/mint-token",
        &admin_key,
        &Mint {
            db: &db,
            name: "test",
        },
    )
    .await;

    Ctx {
        url,
        db,
        token: minted.token,
        admin_key,
    }
}

async fn post<B: Serialize, R: for<'de> Deserialize<'de>>(
    client: &reqwest::Client,
    url: &str,
    path: &str,
    admin_key: &str,
    body: &B,
) -> R {
    let resp = client
        .post(format!("{url}{path}"))
        .bearer_auth(admin_key)
        .json(body)
        .send()
        .await
        .unwrap();
    resp.json::<R>().await.unwrap()
}

// Minimal unique suffix without pulling `uuid` into the harness.
fn uuid_v7() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("{ms:012x}{:020x}", rand_counter())
}

fn rand_counter() -> u128 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static C: AtomicU64 = AtomicU64::new(0);
    (C.fetch_add(1, Ordering::SeqCst) as u128) | 0x8000
}
