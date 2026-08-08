# Signed, time-limited storage URLs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add HMAC-signed, expiry-bounded read URLs for storage blobs — a client mints a URL that grants access to one blob until an absolute expiry, verified by the server with no DB auth lookup.

**Architecture:** A new `signed_url.rs` module derives an HMAC key from the already-required `admin_key` and provides `sign`/`verify`. The public serve route `GET /storage/{id}` gains an additive verify step: when `?exp=&sig=` are present it checks the HMAC and expiry (403 on failure), otherwise it behaves exactly as today. A new bearer-authorized mint endpoint `GET /api/storage/{db}/{id}/signed-url` returns `{url, expiresAt}`. All four client SDKs gain a `getSignedUrl`/`get_signed_url` method that calls the mint endpoint. No committer, protocol, WS, or env-var changes.

**Tech Stack:** Rust server (axum/tokio, `ring::hmac` for HMAC + constant-time verify, `hex` for encoding — both already deps), TypeScript SDK (Vitest + injected `fetch` mock), Rust client (wiremock), Python client (`httpx.MockTransport`, pydantic `_Wire`).

## Global Constraints

- **Zero new environment variables.** The signing key is derived from `admin_key` (`HMAC-SHA256(admin_key, "rtdb-storage-signing-v1")`); do NOT add any `RTDB_*` var, and do NOT touch `.env.example` or `docker-compose.yml` (the env-drift gate would otherwise fail).
- **No new vendor crates.** Use `ring::hmac` and `hex` (hex is already a runtime dep via `storage.rs`). NOTE: `ring` was a `[dev-dependencies]` entry in `server/Cargo.toml`; Task 1 promoted it to `[dependencies]`. It is already compiled transitively via rustls (sqlx `tls-rustls` + reqwest `rustls-tls`), so promotion adds zero new build/binary weight. (Corrects the original spec/plan's incorrect claim that ring was already a runtime dep.)
- **Wire shape is camelCase** `{ "url": "<string>", "expiresAt": <i64 epoch-ms> }` across server + all clients — match exactly.
- **Additive only.** A request to `GET /storage/{id}` with no `exp`/`sig` must behave exactly as before (public 200). Never gate the existing public route.
- **Constant-time verification** via `ring::hmac::verify`; signature failures return `RtDbError::forbidden` (HTTP 403, code `FORBIDDEN`, generic message "invalid or expired signature").
- **No `unwrap()`/`expect()` outside `#[cfg(test)]`.** Zero clippy warnings under `-D warnings`.
- **Clients mirror the server.** All four clients (TS `http.ts` + `client.ts`, Rust `http.rs`, Python `http_client.py` + `aio_http_client.py`) get the new method; the server is the source of truth for the wire shape.
- **Server tests need the dev Postgres** — run `make dev-db-up` before any `cargo test`. Tests create a `t<uuid-v7>` database each via the existing harness.
- **Trunk-based commits** on `main` (matches this repo's history). Commit with `timeout: 600000` — the pre-commit `cargo clippy` hook is slow. Verify the real exit code, not a pipe's tail.

## File Structure

**Server**
- `server/src/signed_url.rs` (new) — HMAC key derivation, sign, verify, TTL consts. One responsibility: the signed-URL cryptographic primitives. Pure functions (no I/O), unit-tested inline.
- `server/src/lib.rs` — register `pub mod signed_url;`; add `pub signed_url_key: Arc<ring::hmac::Key>` to `AppState`, derived once in `AppState::new`.
- `server/src/http_api.rs` — `signed_url_handler` (mint) + `SignedUrlResponse`; additive verify block inside `serve_public_handler`; route registration in `http_api_routes`.
- `server/tests/storage_signed_url_test.rs` (new) — integration tests (mint + serve, expiry, tamper, partial, public fallback, TTL clamp, auth).

**Clients**
- `ts-client/src/http.ts` — `SignedUrl` interface + `getSignedUrl` on `RtDbHttpClient`.
- `ts-client/src/client.ts` — `getSignedUrl` delegate on `RtDbClient` via `httpForStorage()`.
- `ts-client/tests/http.test.ts` — `getSignedUrl` request/parse tests.
- `rust-client/src/http.rs` — `SignedUrl` struct + `get_signed_url` on `RtDbHttpClient` + wiremock test.
- `python-client/src/par_rt_db/http_client.py` — `SignedUrl` (`_Wire`) + sync `get_signed_url`.
- `python-client/src/par_rt_db/aio_http_client.py` — import `SignedUrl`, async `get_signed_url`.
- `python-client/tests/test_http_client.py` + `python-client/tests/test_aio_http_client.py` — `MockTransport` tests.

**Docs**
- `FEATURE_MATRIX.md`, `server/README.md`, `rust-client/README.md`, `python-client` README, `CLAUDE.md`.

---

### Task 1: `signed_url` module + `AppState` signing key

**Files:**
- Create: `server/src/signed_url.rs`
- Modify: `server/src/lib.rs` (module decl at top + `AppState` field + derivation in `AppState::new`)

**Interfaces:**
- Produces: `signed_url::derive_key(admin_key: &str) -> ring::hmac::Key`, `signed_url::sign(key: &ring::hmac::Key, id: &str, exp_ms: i64) -> String` (hex), `signed_url::verify(key: &ring::hmac::Key, id: &str, exp_ms: i64, sig_hex: &str) -> bool`, `signed_url::DEFAULT_SIGNED_URL_TTL_SECS: u64`, `signed_url::MAX_SIGNED_URL_TTL_SECS: u64`, and `AppState.signed_url_key: Arc<ring::hmac::Key>`. Tasks 2–5 consume these.

- [ ] **Step 1: Write the failing unit tests**

Create `server/src/signed_url.rs` with only the tests first (the functions won't exist yet — this is the red phase):

```rust
//! Signed, time-limited storage URLs.
//!
//! A signed URL is `GET /storage/{id}?exp=<unix-ms>&sig=<hex>` where
//! `sig = HMAC-SHA256(signing_key, "{id}.{exp}")`. The signing key is derived
//! once at boot from the server's required `admin_key` and held on `AppState`,
//! so the feature needs no extra configuration. Rotating `admin_key` changes the
//! derived key and invalidates every outstanding signed URL (a desirable
//! "revoke all signed access" side effect). See
//! docs/superpowers/specs/2026-08-08-signed-storage-urls-design.md.

/// Domain-separation label mixed into key derivation so a signed URL (which
/// exposes only `id`, `exp`, and the signature — never the key) cannot be
/// turned into an admin credential, and `admin_key` is never placed directly on
/// the public serve path.
const LABEL: &[u8] = b"rtdb-storage-signing-v1";

/// Default TTL when a mint request omits `ttlSeconds`: 1 hour.
pub const DEFAULT_SIGNED_URL_TTL_SECS: u64 = 3600;

/// Upper bound on a minted TTL: 7 days. A compile-time const (not an env knob)
/// keeps the feature zero-config; raising it is a code change.
pub const MAX_SIGNED_URL_TTL_SECS: u64 = 7 * 24 * 60 * 60;

#[cfg(test)]
mod tests {
    use super::*;

    const EXP: i64 = 1_700_000_000_000;

    #[test]
    fn sign_verify_roundtrip() {
        let key = derive_key("secret-admin-key");
        let sig = sign(&key, "fileid123", EXP);
        assert!(verify(&key, "fileid123", EXP, &sig));
    }

    #[test]
    fn verify_rejects_tampered_signature() {
        let key = derive_key("secret-admin-key");
        let sig = sign(&key, "fileid123", EXP);
        // Flip the last hex digit to a different valid hex char.
        let mut chars: Vec<char> = sig.chars().collect();
        let last = chars[chars.len() - 1];
        chars[chars.len() - 1] = if last == '0' { '1' } else { '0' };
        let tampered: String = chars.into_iter().collect();
        assert!(!verify(&key, "fileid123", EXP, &tampered));
    }

    #[test]
    fn verify_rejects_tampered_id() {
        let key = derive_key("secret-admin-key");
        let sig = sign(&key, "fileid123", EXP);
        assert!(!verify(&key, "tampered", EXP, &sig));
    }

    #[test]
    fn verify_rejects_tampered_exp() {
        let key = derive_key("secret-admin-key");
        let sig = sign(&key, "fileid123", EXP);
        assert!(!verify(&key, "fileid123", EXP + 1, &sig));
    }

    #[test]
    fn verify_rejects_different_key() {
        let sig = sign(&derive_key("key-a"), "fileid123", EXP);
        assert!(!verify(&derive_key("key-b"), "fileid123", EXP, &sig));
    }

    #[test]
    fn verify_rejects_non_hex_signature() {
        let key = derive_key("secret-admin-key");
        assert!(!verify(&key, "fileid123", EXP, "not-hex!!"));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd server && cargo test --lib signed_url`
Expected: FAIL to compile — `derive_key` / `sign` / `verify` not defined.

- [ ] **Step 3: Write the implementation**

Append the three functions to `server/src/signed_url.rs`, above the `#[cfg(test)]` block:

```rust
/// Derives the storage signing key from `admin_key`. Two-level HKDF-style
/// derivation: the label is HMAC'd under the raw admin key, and the result
/// becomes the HMAC key for signing URLs. `ring::hmac::Key` is `Send + Sync`,
/// so it is safe to share via `Arc<ring::hmac::Key>` on `AppState`.
pub fn derive_key(admin_key: &str) -> ring::hmac::Key {
    let seed =
        ring::hmac::sign(&ring::hmac::Key::new(ring::hmac::HMAC_SHA256, admin_key.as_bytes()), LABEL);
    ring::hmac::Key::new(ring::hmac::HMAC_SHA256, seed.as_ref())
}

/// Hex HMAC-SHA256 over `"{id}.{exp}"`. Hex (not base64) keeps the URL free of
/// `+/=` URL-encoding hazards.
pub fn sign(key: &ring::hmac::Key, id: &str, exp_ms: i64) -> String {
    let msg = format!("{id}.{exp_ms}");
    hex::encode(ring::hmac::sign(key, msg.as_bytes()).as_ref())
}

/// Constant-time verification. Returns `false` for a non-hex signature, a
/// mismatched key, or any difference in `id`/`exp` (the compare itself is
/// constant-time via `ring::hmac::verify`; the `false` return for bad hex is
/// not timing-sensitive because it reveals only "malformed", not a near-miss).
pub fn verify(key: &ring::hmac::Key, id: &str, exp_ms: i64, sig_hex: &str) -> bool {
    let Ok(sig_bytes) = hex::decode(sig_hex) else {
        return false;
    };
    let msg = format!("{id}.{exp_ms}");
    ring::hmac::verify(key, msg.as_bytes(), &sig_bytes).is_ok()
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd server && cargo test --lib signed_url`
Expected: PASS — all 6 tests green.

- [ ] **Step 5: Register the module and wire the key onto `AppState`**

In `server/src/lib.rs`, add the module declaration alongside the other `pub mod` lines (alphabetical, after `pub mod snapshot;`):

```rust
pub mod signed_url;
```

Add the field to the `AppState` struct (after `pub quotas: Arc<quota::UsageCache>,`):

```rust
    /// HMAC key for signing time-limited storage URLs (derived once at boot from
    /// `config.admin_key`). Shared by every request via `Arc`.
    pub signed_url_key: Arc<ring::hmac::Key>,
```

In `AppState::new` (in `server/src/lib.rs`), derive the key **before** the `Arc::new(Self { ... })` literal (so it can borrow `config.admin_key` before `config` moves into the struct). Place it just above the `let image = …` line:

```rust
        let signed_url_key = Arc::new(signed_url::derive_key(&config.admin_key));
```

Then add the field to the struct literal (after `quotas,`):

```rust
            quotas,
            signed_url_key,
```

- [ ] **Step 6: Verify the server builds + clippy is clean**

Run: `cd server && cargo build && cargo clippy --all-targets -- -D warnings 2>&1 | tail -5` then check `$?`
Expected: builds with no warnings. (If a future `ring` version makes `hmac::Key` non-`Send+Sync`, store `Arc<[u8]>` raw bytes on `AppState` and construct the `ring::hmac::Key` inside `sign`/`verify` instead — but `ring` 0.17's `Key` is `Send+Sync`.)

- [ ] **Step 7: Commit**

```bash
git add server/src/signed_url.rs server/src/lib.rs
git commit -m "feat(storage): signed-URL crypto module + AppState signing key"
```

---

### Task 2: Server mint endpoint + verify-on-serve + integration tests

**Files:**
- Create: `server/tests/storage_signed_url_test.rs`
- Modify: `server/src/http_api.rs` (new `signed_url_handler` + `SignedUrlResponse`; verify block in `serve_public_handler`; route in `http_api_routes`)

**Interfaces:**
- Consumes: `signed_url::{sign, verify, DEFAULT_SIGNED_URL_TTL_SECS, MAX_SIGNED_URL_TTL_SECS}` and `state.signed_url_key` (from Task 1); `bearer_token`, `resolve_bearer`, `authorize`, `check_http_rate_limits`, `now_ms`, `RtDbError::forbidden` (all existing).
- Produces: HTTP route `GET /api/storage/{db}/{id}/signed-url` → `{url, expiresAt}`; additive `?exp=&sig=` verification on `GET /storage/{id}`. Tasks 3–5 test against this contract.

- [ ] **Step 1: Write the failing integration tests**

Create `server/tests/storage_signed_url_test.rs`:

```rust
//! Integration tests for signed, time-limited storage URLs.
//!
//! Covers the mint endpoint (`GET /api/storage/{db}/{id}/signed-url`) and the
//! additive signature verification on the public serve route
//! (`GET /storage/{id}?exp=&sig=`). The mint endpoint's returned `url` embeds
//! `config.public_url` (`http://localhost:0` in tests), so tests extract the
//! `/storage/...?...` path+query and re-fetch against the ephemeral test addr.

mod common;

use common::{admin_post, fresh_db, spawn_app, test_state};
use rtdb_server::signed_url;
use rtdb_server::storage;
use serde_json::json;
use std::net::SocketAddr;

/// Mint a machine token authorized for `db` (mirrors `storage_test`'s helper).
async fn mint_token(addr: SocketAddr, db: &str) -> String {
    let resp = admin_post(addr, "/admin/mint-token", json!({ "db": db })).await;
    let body: serde_json::Value = resp.json().await.expect("parse mint-token response");
    body["token"].as_str().expect("token present").to_string()
}

/// Seed a blob directly and return its id + the bytes written.
async fn seed(state: &std::sync::Arc<rtdb_server::AppState>, db: &str) -> (String, Vec<u8>) {
    let bytes = b"signed-url payload".to_vec();
    let sha = storage::sha256_hex_bytes(&bytes);
    let id = storage::put(state, db, &sha, bytes.len() as i64, Some("text/plain"), &bytes)
        .await
        .expect("seed blob");
    (id, bytes)
}

#[tokio::test]
async fn mint_then_serve_returns_bytes() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let (id, bytes) = seed(&state, &db).await;
    let token = mint_token(addr, &db).await;

    // Mint via the endpoint.
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/api/storage/{db}/{id}/signed-url"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await?;
    let url = body["url"].as_str().expect("url present");
    let exp = body["expiresAt"].as_i64().expect("expiresAt present");
    assert!(exp > rtdb_server::db::now_ms());

    // The returned URL embeds the test public_url host; swap to the live addr.
    let path = &url[url.find("/storage/").expect("path present")..];
    let served = reqwest::get(format!("http://{addr}{path}")).await?;
    assert_eq!(served.status(), 200);
    assert_eq!(served.bytes().await?.as_ref(), &bytes[..]);
    Ok(())
}

#[tokio::test]
async fn expired_signature_returns_403() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let (id, _) = seed(&state, &db).await;

    let past = rtdb_server::db::now_ms() - 1000;
    let sig = signed_url::sign(&state.signed_url_key, &id, past);
    let resp = reqwest::get(format!("http://{addr}/storage/{id}?exp={past}&sig={sig}")).await?;
    assert_eq!(resp.status(), 403);
    Ok(())
}

#[tokio::test]
async fn tampered_signature_returns_403() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let (id, _) = seed(&state, &db).await;

    let exp = rtdb_server::db::now_ms() + 60_000;
    let sig = signed_url::sign(&state.signed_url_key, &id, exp);
    let mut chars: Vec<char> = sig.chars().collect();
    let last = chars[chars.len() - 1];
    chars[chars.len() - 1] = if last == '0' { '1' } else { '0' };
    let bad: String = chars.into_iter().collect();

    let resp = reqwest::get(format!("http://{addr}/storage/{id}?exp={exp}&sig={bad}")).await?;
    assert_eq!(resp.status(), 403);
    Ok(())
}

#[tokio::test]
async fn tampered_id_returns_403() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let (id, _) = seed(&state, &db).await;

    let exp = rtdb_server::db::now_ms() + 60_000;
    let sig = signed_url::sign(&state.signed_url_key, &id, exp);
    // Valid sig for `id`, fetched against a different id in the path.
    let resp = reqwest::get(format!("http://{addr}/storage/other-id?exp={exp}&sig={sig}")).await?;
    assert_eq!(resp.status(), 403);
    Ok(())
}

#[tokio::test]
async fn partial_signature_returns_403() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let (id, _) = seed(&state, &db).await;

    let exp = rtdb_server::db::now_ms() + 60_000;
    let sig = signed_url::sign(&state.signed_url_key, &id, exp);

    // exp without sig
    let r1 = reqwest::get(format!("http://{addr}/storage/{id}?exp={exp}")).await?;
    assert_eq!(r1.status(), 403);
    // sig without exp
    let r2 = reqwest::get(format!("http://{addr}/storage/{id}?sig={sig}")).await?;
    assert_eq!(r2.status(), 403);
    Ok(())
}

#[tokio::test]
async fn no_signature_remains_public() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let (id, bytes) = seed(&state, &db).await;

    let served = reqwest::get(format!("http://{addr}/storage/{id}")).await?;
    assert_eq!(served.status(), 200);
    assert_eq!(served.bytes().await?.as_ref(), &bytes[..]);
    Ok(())
}

#[tokio::test]
async fn ttl_is_clamped() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let (id, _) = seed(&state, &db).await;
    let token = mint_token(addr, &db).await;
    let now = rtdb_server::db::now_ms();

    // Over the cap clamps to MAX (7d); ttl=0 clamps to the 1s minimum.
    let over = reqwest::Client::new()
        .get(format!("http://{addr}/api/storage/{db}/{id}/signed-url?ttlSeconds=999999999"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    let exp_over = over.json::<serde_json::Value>().await?["expiresAt"].as_i64().unwrap();
    assert!(exp_over <= now + (signed_url::MAX_SIGNED_URL_TTL_SECS as i64) * 1000);

    let zero = reqwest::Client::new()
        .get(format!("http://{addr}/api/storage/{db}/{id}/signed-url?ttlSeconds=0"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    let exp_zero = zero.json::<serde_json::Value>().await?["expiresAt"].as_i64().unwrap();
    assert!(exp_zero >= now + 1000); // at least the 1s floor
    Ok(())
}

#[tokio::test]
async fn mint_requires_db_authorization() -> anyhow::Result<()> {
    let state = test_state().await;
    let db_a = fresh_db(&state).await;
    let db_b = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let (id_a, _) = seed(&state, &db_a).await;
    let token_b = mint_token(addr, &db_b).await; // authorized for db_b, not db_a

    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/api/storage/{db_a}/{id_a}/signed-url"))
        .header("Authorization", format!("Bearer {token_b}"))
        .send()
        .await?;
    assert_eq!(resp.status(), 403);

    // No bearer at all → 401.
    let noauth = reqwest::Client::new()
        .get(format!("http://{addr}/api/storage/{db_a}/{id_a}/signed-url"))
        .send()
        .await?;
    assert_eq!(noauth.status(), 401);
    Ok(())
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `make dev-db-up && cd server && cargo test --test storage_signed_url_test`
Expected: FAIL — `mint_then_serve_returns_bytes` gets 404 (route `/api/storage/{db}/{id}/signed-url` not registered); the manual-sig tests likely get 200 (no verify step yet) instead of the expected 403.

- [ ] **Step 3: Add the mint handler and response struct**

In `server/src/http_api.rs`, add `use crate::signed_url;` to the imports near `use crate::storage;`. Then add the response struct and handler (place them just before `async fn serve_public_handler`):

```rust
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SignedUrlResponse {
    url: String,
    expires_at: i64,
}

/// Mint a signed, time-limited URL for `{id}`. Same auth as authed serve
/// (`bearer → authorize(db)`); the returned URL is fetched via
/// `GET /storage/{id}?exp=&sig=` until `expiresAt`. Minting is pure
/// computation — no DB write, no committer.
async fn signed_url_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
    AxumQuery(q): AxumQuery<HashMap<String, String>>,
) -> Result<Json<SignedUrlResponse>, RtDbError> {
    let token = bearer_token(&headers)?;
    let principal = resolve_bearer(&state.pool, token).await?;
    authorize(&state.pool, &principal, &db).await?;
    check_http_rate_limits(&state, &principal, &db).await?;
    let ttl = q
        .get("ttlSeconds")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(signed_url::DEFAULT_SIGNED_URL_TTL_SECS)
        .clamp(1, signed_url::MAX_SIGNED_URL_TTL_SECS);
    let exp = now_ms() + (ttl as i64) * 1000;
    let sig = signed_url::sign(&state.signed_url_key, &id, exp);
    let base = state.config.public_url.trim_end_matches('/');
    let url = format!("{base}/storage/{id}?exp={exp}&sig={sig}");
    Ok(Json(SignedUrlResponse { url, expires_at: exp }))
}
```

- [ ] **Step 4: Add the verify step to the public serve handler**

In `serve_public_handler`, insert this block immediately after the `check_storage_public_rate_limit(...)` line and before `let db = storage::resolve_db(...)`:

```rust
    // Additive signed-URL verification: if `exp`+`sig` are present, the request
    // is a signed URL and must pass HMAC + expiry before serving. If absent,
    // behavior is unchanged (public by opaque id).
    if let Some(exp_s) = q.get("exp") {
        let sig = q
            .get("sig")
            .ok_or_else(|| RtDbError::forbidden("invalid or expired signature"))?;
        let exp: i64 = exp_s
            .parse()
            .map_err(|_| RtDbError::forbidden("invalid or expired signature"))?;
        if now_ms() > exp {
            return Err(RtDbError::forbidden("invalid or expired signature"));
        }
        if !signed_url::verify(&state.signed_url_key, &id, exp, sig) {
            return Err(RtDbError::forbidden("invalid or expired signature"));
        }
    }
```

- [ ] **Step 5: Register the route**

In `http_api_routes` (in `server/src/http_api.rs`), add the route just before the public `.route("/storage/{id}", …)` line:

```rust
        // Mint a signed, time-limited URL — same auth as authed serve; the
        // holder fetches via `GET /storage/{id}?exp=&sig=` until expiry.
        .route(
            "/api/storage/{db}/{id}/signed-url",
            get(signed_url_handler),
        )
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd server && cargo test --test storage_signed_url_test`
Expected: PASS — all 8 tests green. If `mint_requires_db_authorization`'s 401 case returns 403 instead, that is still an auth rejection and acceptable; adjust the assertion to `assert!(noauth.status() == 401 || noauth.status() == 403)` only if the existing `bearer_token` behavior returns 403 for a missing bearer (it returns 401 via `RtDbError::unauthorized`, so 401 is expected — keep the strict assertion).

- [ ] **Step 7: clippy + commit**

Run: `cd server && cargo clippy --all-targets -- -D warnings` (check `$?`)
```bash
git add server/src/http_api.rs server/tests/storage_signed_url_test.rs
git commit -m "feat(storage): signed-URL mint endpoint + verify-on-serve"
```

---

### Task 3: TS client — `getSignedUrl`

**Files:**
- Modify: `ts-client/src/http.ts` (add `SignedUrl` interface + `getSignedUrl` method)
- Modify: `ts-client/src/client.ts` (add `getSignedUrl` delegate on `RtDbClient`)
- Test: `ts-client/tests/http.test.ts`

**Interfaces:**
- Consumes: the server contract `GET /api/storage/{db}/{id}/signed-url[?ttlSeconds=]` → `{url, expiresAt}`.
- Produces: `RtDbHttpClient.getSignedUrl(id, ttlSeconds?)` and `RtDbClient.getSignedUrl(id, ttlSeconds?)` returning `{url: string; expiresAt: number}`.

- [ ] **Step 1: Write the failing test**

In `ts-client/tests/http.test.ts`, add inside the `describe("RtDbHttpClient", …)` block:

```typescript
  it("mints a signed URL via GET /api/storage/{db}/{id}/signed-url", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({ url: "http://h:8300/storage/f1?exp=100&sig=abc", expiresAt: 100 }),
    );
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });

    const res = await client.getSignedUrl("f1");

    expect(res).toEqual({ url: "http://h:8300/storage/f1?exp=100&sig=abc", expiresAt: 100 });
    const [calledUrl, init] = fetchMock.mock.calls[0];
    expect(calledUrl).toBe("http://h:8300/api/storage/kanban/f1/signed-url");
    expect(init.method).toBe("GET");
    expect(init.headers.Authorization).toBe("Bearer tok");
  });

  it("getSignedUrl appends ttlSeconds only when provided", async () => {
    const withTtl = vi.fn().mockResolvedValue(jsonResponse({ url: "u", expiresAt: 1 }));
    const c1 = new RtDbHttpClient({ url: "http://h:8300", db: "kanban", token: "tok", fetch: withTtl });
    await c1.getSignedUrl("f1", 120);
    expect(withTtl.mock.calls[0][0]).toBe("http://h:8300/api/storage/kanban/f1/signed-url?ttlSeconds=120");

    const noTtl = vi.fn().mockResolvedValue(jsonResponse({ url: "u", expiresAt: 1 }));
    const c2 = new RtDbHttpClient({ url: "http://h:8300", db: "kanban", token: "tok", fetch: noTtl });
    await c2.getSignedUrl("f1");
    expect(noTtl.mock.calls[0][0]).toBe("http://h:8300/api/storage/kanban/f1/signed-url");
  });
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd ts-client && bunx vitest run tests/http.test.ts -t "signed URL"`
Expected: FAIL — `getSignedUrl is not a function`.

- [ ] **Step 3: Implement the interface + method**

In `ts-client/src/http.ts`, add the `SignedUrl` interface next to `FileMetadata`:

```typescript
/** A signed, time-limited storage URL: `url` works until `expiresAt` (epoch ms). */
export interface SignedUrl {
  url: string;
  expiresAt: number;
}
```

Add the method to `RtDbHttpClient` (next to `getFileMetadata`):

```typescript
  /** Mint a signed, time-limited URL for `id` via the server (GET mint endpoint). */
  async getSignedUrl(id: string, ttlSeconds?: number): Promise<SignedUrl> {
    let path = `/api/storage/${encodeURIComponent(this.db)}/${encodeURIComponent(id)}/signed-url`;
    if (ttlSeconds !== undefined) {
      path += `?ttlSeconds=${ttlSeconds}`;
    }
    return (await this.get(path, this.token)) as SignedUrl;
  }
```

In `ts-client/src/client.ts`, add the delegate next to `getFileMetadata`:

```typescript
  getSignedUrl(id: string, ttlSeconds?: number) {
    return this.httpForStorage().getSignedUrl(id, ttlSeconds);
  }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd ts-client && bunx vitest run tests/http.test.ts -t "signed URL"`
Expected: PASS.

- [ ] **Step 5: typecheck + commit**

Run: `cd ts-client && bunx tsc --noEmit` (check `$?`)
```bash
git add ts-client/src/http.ts ts-client/src/client.ts ts-client/tests/http.test.ts
git commit -m "feat(ts-client): getSignedUrl for time-limited storage URLs"
```

---

### Task 4: Rust client — `get_signed_url`

**Files:**
- Modify: `rust-client/src/http.rs` (add `SignedUrl` struct + `get_signed_url` method + wiremock test)

**Interfaces:**
- Consumes: server contract `GET /api/storage/{db}/{id}/signed-url[?ttlSeconds=]` → `{url, expiresAt}`.
- Produces: `RtDbHttpClient::get_signed_url(id, ttl_seconds: Option<u64>) -> Result<SignedUrl, RtDbError>`.

- [ ] **Step 1: Write the failing test**

In `rust-client/src/http.rs`, in the `#[cfg(test)] mod tests` block, ensure `query_param` is imported alongside the other wiremock matchers (add `query_param` to the existing `use wiremock::matchers::{…}` line). Then add:

```rust
    #[tokio::test]
    async fn get_signed_url_builds_get_with_optional_ttl() {
        let (server, client) = setup().await;
        // With ttl — asserts the query param is sent.
        Mock::given(method("GET"))
            .and(path("/api/storage/t%3Cuuid%3E/f1/signed-url"))
            .and(query_param("ttlSeconds", "120"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "url": "http://x/storage/f1?exp=9&sig=ab", "expiresAt": 9
            })))
            .mount(&server)
            .await;
        let r = client.get_signed_url("f1", Some(120)).await.unwrap();
        assert_eq!(r.url, "http://x/storage/f1?exp=9&sig=ab");
        assert_eq!(r.expires_at, 9);

        // Without ttl — no query param matcher, still matches by path.
        Mock::given(method("GET"))
            .and(path("/api/storage/t%3Cuuid%3E/f1/signed-url"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "url": "u", "expiresAt": 7
            })))
            .mount(&server)
            .await;
        let r = client.get_signed_url("f1", None).await.unwrap();
        assert_eq!(r.expires_at, 7);
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd rust-client && cargo test get_signed_url`
Expected: FAIL — no method `get_signed_url` / no type `SignedUrl`.

- [ ] **Step 3: Implement the struct + method**

In `rust-client/src/http.rs`, add the `SignedUrl` struct next to `FileMetadata`:

```rust
/// A signed, time-limited storage URL minted by the server
/// (`GET /api/storage/{db}/{id}/signed-url`). `expires_at` is epoch
/// milliseconds; the URL stops working after it.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignedUrl {
    pub url: String,
    pub expires_at: i64,
}
```

Add the method to `impl RtDbHttpClient` (next to `get_file_metadata`):

```rust
    pub async fn get_signed_url(
        &self,
        id: &str,
        ttl_seconds: Option<u64>,
    ) -> Result<SignedUrl, RtDbError> {
        let mut req = self
            .client
            .get(format!("{}/api/storage/{}/{id}/signed-url", self.url, self.db))
            .bearer_auth(&self.token);
        if let Some(ttl) = ttl_seconds {
            req = req.query(&[("ttlSeconds", ttl)]);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("signed url request failed: {e}")))?;
        self.deserialize::<SignedUrl>(resp).await
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd rust-client && cargo test get_signed_url`
Expected: PASS.

- [ ] **Step 5: clippy + commit**

Run: `cd rust-client && cargo clippy --all-targets -- -D warnings` (check `$?`)
```bash
git add rust-client/src/http.rs
git commit -m "feat(rust-client): get_signed_url for time-limited storage URLs"
```

---

### Task 5: Python client — `get_signed_url` (sync + async)

**Files:**
- Modify: `python-client/src/par_rt_db/http_client.py` (add `SignedUrl` dataclass + sync `get_signed_url`)
- Modify: `python-client/src/par_rt_db/aio_http_client.py` (import `SignedUrl`, add async `get_signed_url`)
- Test: `python-client/tests/test_http_client.py`, `python-client/tests/test_aio_http_client.py`

**Interfaces:**
- Consumes: server contract `GET /api/storage/{db}/{id}/signed-url[?ttlSeconds=]` → `{url, expiresAt}`.
- Produces: `RtDbHttpClient.get_signed_url(id, *, ttl_seconds=None) -> SignedUrl` and the async equivalent on `RtDbAsyncHttpClient`.

- [ ] **Step 1: Write the failing sync test**

In `python-client/tests/test_http_client.py`, add (the file already imports `httpx`, `pytest`, and defines `_client(handler)` + `_handler_map`):

```python
def test_get_signed_url_omits_ttl_by_default():
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.startswith("/api/storage/") and request.url.path.endswith("/signed-url"):
            seen["query"] = str(request.url.params)
            return httpx.Response(200, json={"url": "http://x/storage/f1?exp=9&sig=ab", "expiresAt": 9})
        return httpx.Response(404)

    c = _client(handler)
    r = c.get_signed_url("f1")
    from par_rt_db.http_client import SignedUrl

    assert isinstance(r, SignedUrl)
    assert r.url == "http://x/storage/f1?exp=9&sig=ab"
    assert r.expires_at == 9
    assert seen["query"] == ""  # no ttlSeconds param


def test_get_signed_url_passes_ttl_seconds():
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["ttl"] = request.url.params.get("ttlSeconds")
        return httpx.Response(200, json={"url": "u", "expiresAt": 9})

    c = _client(handler)
    c.get_signed_url("f1", ttl_seconds=120)
    assert seen["ttl"] == "120"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd python-client && uv run pytest -q tests/test_http_client.py -k get_signed_url`
Expected: FAIL — `AttributeError: get_signed_url` (and `ImportError` for `SignedUrl`).

- [ ] **Step 3: Implement the sync dataclass + method**

In `python-client/src/par_rt_db/http_client.py`, add the dataclass next to `FileMetadata`:

```python
class SignedUrl(_Wire):
    """``GET /api/storage/{db}/{id}/signed-url`` response: a time-limited signed
    serve URL and its absolute expiry (epoch milliseconds)."""

    url: str
    expires_at: int
```

Add the sync method in the storage section (next to `get_file_metadata`):

```python
    def get_signed_url(self, id: str, *, ttl_seconds: int | None = None) -> SignedUrl:
        """``GET /api/storage/{db}/{id}/signed-url`` → ``SignedUrl``.

        ``ttl_seconds`` is optional (server default 1h, capped at 7d); when
        ``None`` the request omits the query parameter.
        """
        params = {"ttlSeconds": ttl_seconds} if ttl_seconds is not None else None
        resp = self._send("GET", f"/api/storage/{self._db}/{id}/signed-url", params=params)
        return SignedUrl.model_validate(resp.json())
```

In `python-client/src/par_rt_db/aio_http_client.py`, add `SignedUrl,` to the `from .http_client import (…)` block (alphabetical, near `FileMetadata,`), then add the async method in its storage section:

```python
    async def get_signed_url(self, id: str, *, ttl_seconds: int | None = None) -> SignedUrl:
        """``GET /api/storage/{db}/{id}/signed-url`` → ``SignedUrl`` (async)."""
        params = {"ttlSeconds": ttl_seconds} if ttl_seconds is not None else None
        resp = await self._send("GET", f"/api/storage/{self._db}/{id}/signed-url", params=params)
        return SignedUrl.model_validate(resp.json())
```

- [ ] **Step 4: Run the sync test + add+run an async test**

In `python-client/tests/test_aio_http_client.py`, add (it already defines an async `_client`/`make_async_client(handler)` helper):

```python
async def test_async_get_signed_url_passes_ttl():
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["ttl"] = request.url.params.get("ttlSeconds")
        return httpx.Response(200, json={"url": "u", "expiresAt": 9})

    c = make_async_client(handler)
    r = await c.get_signed_url("f1", ttl_seconds=90)
    assert r.expires_at == 9
    assert seen["ttl"] == "90"
```

Run: `cd python-client && uv run pytest -q tests/test_http_client.py tests/test_aio_http_client.py -k get_signed_url`
Expected: PASS — sync + async green.

> Note: confirm the async helper is named `make_async_client` (used in `test_aio_http_client.py`); if it is named differently in that file, use the existing name. `SignedUrl` mirrors `FileMetadata` (defined in `http_client.py`, imported by `aio_http_client.py`, not added to `__all__`) — so no `__init__.py` change is needed.

- [ ] **Step 5: typecheck + lint + commit**

Run: `cd python-client && uv run pyright src/par_rt_db/http_client.py src/par_rt_db/aio_http_client.py && uv run ruff check .` (check `$?`)
```bash
git add python-client/src/par_rt_db/http_client.py python-client/src/par_rt_db/aio_http_client.py python-client/tests/test_http_client.py python-client/tests/test_aio_http_client.py
git commit -m "feat(python-client): get_signed_url for time-limited storage URLs"
```

---

### Task 6: Docs

**Files:**
- Modify: `FEATURE_MATRIX.md`, `server/README.md`, `rust-client/README.md`, `python-client/README.md` (or its main docs), `CLAUDE.md`

- [ ] **Step 1: Update FEATURE_MATRIX**

Find the file-storage row in `FEATURE_MATRIX.md` and append a note that signed, time-limited URLs now ship (server + ts/rust/python clients). Example sentence to add to that row's notes:

```markdown
Signed, time-limited storage URLs (`GET /storage/{id}?exp=&sig=` + `getSignedUrl`/`get_signed_url` mint) — ✅ all four clients.
```

- [ ] **Step 2: Update server README**

In `server/README.md`'s "File storage" section, add a short subsection:

```markdown
### Signed, time-limited URLs

`GET /api/storage/{db}/{id}/signed-url?ttlSeconds=3600` (bearer-authorized for
`{db}`) mints a URL that grants read access to one blob until an absolute
expiry (default 1h, max 7d). The URL is `GET /storage/{id}?exp=<unix-ms>&sig=<hex>`,
verified by an HMAC key derived from `admin_key` — no DB lookup, and a request
with no `exp`/`sig` still serves publicly as before.
```

- [ ] **Step 3: Update client READMEs**

Add a one-line mention of `getSignedUrl`/`get_signed_url` to the "File storage" section of `rust-client/README.md` and the python client docs, mirroring how `getUrl`/`get_url` is documented there.

- [ ] **Step 4: Update CLAUDE.md**

In `CLAUDE.md`'s "File storage is HTTP-only…" invariant paragraph (the one that says "`GET /storage/{id}` is the one unauthenticated route"), append one sentence:

```markdown
Signed, time-limited URLs (ENH-016+) are an additive capability on that public route: when `?exp=&sig=` are present the route HMAC-verifies (key derived from `admin_key`) and enforces expiry (403 on failure); absent, behavior is unchanged. A bearer-authorized `GET /api/storage/{db}/{id}/signed-url` mints them.
```

- [ ] **Step 5: Commit**

```bash
git add FEATURE_MATRIX.md server/README.md rust-client/README.md python-client/README.md CLAUDE.md
git commit -m "docs: signed, time-limited storage URLs"
```
(Adjust the `git add` list to the files actually changed — only stage files you edited.)

---

### Task 7: Full gate + push

- [ ] **Step 1: Run the complete gate**

Run: `make checkall`
Expected: fmt-check + clippy `-D warnings` (server, rust-client, cli) + typecheck (ts-client, dashboard, python-client) + tests all pass. Verify the real exit code (`echo "EXIT=$?"` immediately after, not via a pipe tail).

> Prereqs on a fresh checkout: `make dev-db-up`, `make ts-client-install`, `make dashboard-install`, `make python-client-install`, and `make ts-client-build` (the dashboard typecheck resolves `@par-rt-db/client` from `ts-client/dist`).

- [ ] **Step 2: Push**

```bash
git push origin main
```
(Private repo, trunk-based — push the accumulated feature commits once the gate is green.)

## Self-Review

**Spec coverage:** every spec section maps to a task — key derivation (T1), token format + verify path + mint endpoint + mint semantics (T2), client surface across all four clients (T3–T5), docs (T6), gate (T7). Out-of-scope items (private-storage mode, signing transform params, per-IP binding, opaque-token route) are correctly absent.

**Placeholder scan:** no TBD/TODO; every code step shows real code; the two "if named differently / adjust the add list" notes are concrete correctness guards, not fill-ins.

**Type consistency:** `sign(key, id, exp_ms: i64)` / `verify(key, id, exp_ms: i64, sig_hex)` match across T1, T2, and the T2 tests. Wire shape `{url, expiresAt}` (camelCase) matches across the server `SignedUrlResponse` and all client types. `getSignedUrl`/`get_signed_url` names are consistent within each client. `state.signed_url_key: Arc<ring::hmac::Key>` is consistent between T1 and T2.
