# Client Sweep — Item A-rust: rust-client Admin Endpoint Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring the rust-client `RtDbHttpClient` admin surface from 10 to full machine-relevant parity by adding the 11 missing admin endpoints (admin allowlist, schema/stats read-back, owner-bypass docs, tokens, metrics, hot config, op feed).

**Architecture:** Mirrors the existing `#[cfg(feature = "admin")] impl RtDbHttpClient` block (`http.rs:504-688`) — each new method is a thin wrapper over the existing `post_json`/`get_json`/`deserialize` helpers (the envelope-aware error path), plus two new 3-line helpers (`delete_json`, `patch_json`) for the DELETE/PATCH routes. Response types are new, defined in `wire::admin` (`wire.rs:357`) with `#[serde(rename_all = "camelCase")]` (snake_case Rust fields), re-exported from `lib.rs`. **Excluded** (rust is a server-side machine client): `login`/`logout` (cookie-session) and `stream` (WS) + any OAuth helper.

**Tech Stack:** Rust (edition 2024), reqwest, serde, `#![deny(warnings)]` + clippy `-D warnings`. Run cargo from `rust-client/`. The `admin` feature implies `http`.

## Global Constraints

- **No wire/protocol changes** — `protocol.rs`/server untouched. Reuse existing types (`SchemaDef`, `Query`, `Transaction`, `StepResult`). New admin response types go in `wire::admin` (`wire.rs`), re-exported from `lib.rs:67-68`.
- **Match server wire casing**: every new response struct uses `#[serde(rename_all = "camelCase")]` (snake_case Rust fields: `created_at`, `row_count`, `size_bytes`, `github_id`, `total_size_bytes`, `allowed_origins`, `session_ttl_days`, `max_file_size`, `doc_id`). **Do NOT** add `deny_unknown_fields` (admin responses stay forward-compat; none of the existing ones use it).
- Auth: `.bearer_auth(&self.token)` (admin key passed as the client `token`), same as the existing 10 methods. Admin methods take `db` as a parameter (the client is constructed with `db=""` for admin use).
- `HotConfigPatch` is `pub` (callers build it) with `#[serde(default, skip_serializing_if = "Option::is_none")]` per optional field; all other new types are `pub` response types (`Deserialize`).
- `#![deny(warnings)]` + clippy `-D warnings`; no `unwrap`/`expect` outside `#[cfg(test)]`.
- Tests go in the existing `#[cfg(all(test, feature = "admin"))] mod admin_tests` (`http.rs:1327-1510`) using the `wiremock` harness + `setup()` helper + `BEARER` const already there. One test per method (assert path + method + body + auth + unwrap) + one error-path test.
- Verification: each task runs `cd rust-client && cargo test --features admin` (+ clippy + fmt before commit). Full `make checkall` at branch finish.
- Re-Read regions before editing (R9): `http.rs`/`wire.rs` line numbers from the exploration drift as you edit.

## Reference: server contract (the 11 endpoints to add)

| Method | Route | Body / params | Response (server) | rust return |
|---|---|---|---|---|
| `admins_list(db)` | GET `/admin/admins` | — | `{admins: [{email, githubId?}]}` | `Vec<AdminMember>` |
| `admins_add(db, email, github_id?)` | POST `/admin/admins` | `{email, githubId?}` | `{ok}` | `()` |
| `admins_remove(db, email)` | DELETE `/admin/admins` | `{email}` (body-on-DELETE) | `{ok}` | `()` |
| `get_schema(db)` | GET `/admin/dbs/{db}/schema` | path `db` | bare `SchemaDef` | `SchemaDef` |
| `db_stats(db)` | GET `/admin/dbs/{db}/stats` | path `db` | `{tables: [{name, rowCount, sizeBytes}], totalSizeBytes}` | `DbStats` |
| `list_tokens(db)` | GET `/admin/tokens?db=` | query `db` | `{tokens: [{id, name, createdAt, revoked}]}` | `Vec<TokenInfo>` |
| `metrics(db)` | GET `/admin/metrics` | — | bare `MetricsSnapshot` (8 camelCase fields) | `MetricsSnapshot` |
| `get_config(db)` | GET `/admin/config` | — | bare `ConfigResponse` (redacted) | `ConfigResponse` |
| `patch_config(db, patch)` | PATCH `/admin/config` | `{allowedOrigins?, sessionTtlDays?, maxFileSize?}` | bare `ConfigResponse` | `ConfigResponse` |
| `ops_recent(db, opts?)` | GET `/admin/ops/recent?db=&table=&n=` | optional query | `{ops: OpEvent[]}` | `Vec<OpEvent>` |
| `admin_query(db, query)` | POST `/admin/db/{db}/query` | `{query}` | `{result}` | typed `T` |
| `admin_mutate(db, txn, idempotency_key?)` | POST `/admin/db/{db}/mutate` | `{txn, idempotencyKey?}` | `{results}` | `Vec<StepResult>` |

(All take `db` as a param even when it's only in the path/query — matches the existing admin methods' signature convention. Singular `/admin/db/{db}` vs plural `/admin/dbs/{db}`.)

---

## File Structure

- `rust-client/src/wire.rs` — add the 9 new types to the `admin` module (`:357-414`).
- `rust-client/src/http.rs` — add `delete_json` + `patch_json` helpers; add the 11 methods to the `#[cfg(feature="admin")] impl` block (`:504`); add unit tests to `admin_tests` (`:1327`).
- `rust-client/src/lib.rs` — re-export the new pub types (`:67-68`, `#[cfg(feature="admin")]`).
- `rust-client/tests/http_integration.rs` — extend the `admin_control_plane` live test (`:79`) with a couple new endpoints (secondary coverage; optional).
- `FEATURE_MATRIX.md` — note rust-client admin parity.

---

## Task 1: helpers + admin allowlist

**Files:** `rust-client/src/wire.rs` (AdminMember), `rust-client/src/http.rs` (delete_json/patch_json helpers + 3 methods + tests), `rust-client/src/lib.rs` (re-export AdminMember).

**Interfaces:**
- Consumes: existing `post_json`/`get_json`/`deserialize`/`expect_ok` helpers.
- Produces: `delete_json<Req: Serialize>(path, body) -> Result<reqwest::Response, RtDbError>`, `patch_json<Req: Serialize>(path, body) -> Result<reqwest::Response, RtDbError>`; `AdminMember`; `admins_list`/`admins_add`/`admins_remove`.

- [ ] **Step 1: Write the failing tests** in `admin_tests` (`http.rs:1327+`), mirroring the existing `allowlist_*` tests:

```rust
#[tokio::test]
async fn admins_list_unwraps_admins_array() {
    let (server, client) = setup().await;
    Mock::given(method("GET")).and(path("/admin/admins"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200)
            .set_body_json(json!({"admins":[{"email":"a@x.com","githubId":1},{"email":"b@x.com"}]})))
        .mount(&server).await;
    let rows = client.admins_list("kanban").await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].email, "a@x.com");
    assert_eq!(rows[0].github_id, Some(1));
    assert_eq!(rows[1].github_id, None);
}

#[tokio::test]
async fn admins_add_posts_email_and_optional_github_id() {
    let (server, client) = setup().await;
    Mock::given(method("POST")).and(path("/admin/admins"))
        .and(header("authorization", BEARER))
        .and(body_partial_json(json!({"email":"a@x.com","githubId":7})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok":true})))
        .mount(&server).await;
    client.admins_add("kanban", "a@x.com", Some(7)).await.unwrap();
}

#[tokio::test]
async fn admins_remove_uses_delete_with_body() {
    let (server, client) = setup().await;
    Mock::given(method("DELETE")).and(path("/admin/admins"))
        .and(header("authorization", BEARER))
        .and(body_partial_json(json!({"email":"a@x.com"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok":true})))
        .mount(&server).await;
    client.admins_remove("kanban", "a@x.com").await.unwrap();
}
```

- [ ] **Step 2: RED** — `cd rust-client && cargo test --features admin admins` (compile errors: methods/types don't exist).

- [ ] **Step 3: Implement.** In `wire.rs` admin module, add:

```rust
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminMember {
    pub email: String,
    pub github_id: Option<i64>,
}
```

In `http.rs`, add two helpers alongside `post_json` (`:641`):

```rust
async fn delete_json<Req: serde::Serialize>(&self, path: &str, body: &Req) -> Result<reqwest::Response, RtDbError> {
    self.client.delete(format!("{}{}", self.url, path)).bearer_auth(&self.token).json(body).send().await
        .map_err(|e| RtDbError::internal(format!("request failed: {e}")))
}
async fn patch_json<Req: serde::Serialize>(&self, path: &str, body: &Req) -> Result<reqwest::Response, RtDbError> {
    self.client.patch(format!("{}{}", self.url, path)).bearer_auth(&self.token).json(body).send().await
        .map_err(|e| RtDbError::internal(format!("request failed: {e}")))
}
```

Add the 3 methods to the admin impl block (mirror the existing `allowlist_*` style; for the `{admins}` wrapper use a small private struct). `admins_add` builds the body omitting `github_id` when `None` — use a `#[derive(Serialize)] #[serde(rename_all="camelCase")] struct Body { email, #[serde(skip_serializing_if="Option::is_none")] github_id: Option<i64> }`:

```rust
pub async fn admins_list(&self, _db: &str) -> Result<Vec<crate::wire::admin::AdminMember>, RtDbError> {
    #[derive(serde::Deserialize)]
    struct Resp { admins: Vec<crate::wire::admin::AdminMember> }
    Ok(self.get_json::<Resp>("/admin/admins", &[]).await?.admins)
}
pub async fn admins_add(&self, _db: &str, email: &str, github_id: Option<i64>) -> Result<(), RtDbError> {
    #[derive(serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Body { email: String, #[serde(skip_serializing_if = "Option::is_none")] github_id: Option<i64> }
    self.expect_ok(self.post_json("/admin/admins", &Body { email: email.to_string(), github_id }).await?).await
}
pub async fn admins_remove(&self, _db: &str, email: &str) -> Result<(), RtDbError> {
    #[derive(serde::Serialize)] struct Body { email: String }
    self.expect_ok(self.delete_json("/admin/admins", &Body { email: email.to_string() }).await?).await
}
```

In `lib.rs:67-68`, add `AdminMember` to the `#[cfg(feature = "admin")] pub use wire::admin::{...}` re-export.

- [ ] **Step 4: GREEN** — `cd rust-client && cargo test --features admin` (new + pre-existing pass) + `cargo clippy --features admin -- -D warnings` + `cargo fmt --check`.

- [ ] **Step 5: Commit** — `cd rust-client && cargo fmt && git add rust-client/src/{wire,http,lib}.rs && git commit -m "feat(rust-client): admin allowlist endpoints + delete/patch helpers"`.

---

## Task 2: schema/stats read-back + tokens + metrics + config + ops

**Files:** `wire.rs` (8 types), `http.rs` (7 methods + tests), `lib.rs` (re-exports).

- [ ] **Step 1: Tests** in `admin_tests` — one per method (path + body/params + auth + unwrap). For `ops_recent`, assert the query string (`.and(query_param("db","kanban")).and(query_param("n","50"))`). For `patch_config`, assert `method("PATCH")` + the body. Include one error-path test (e.g. `patch_config` 400 envelope → `RtDbError` with the right code).

- [ ] **Step 2: RED.**

- [ ] **Step 3: Types** in `wire.rs` admin module (all `#[derive(Debug, Clone, Deserialize)] #[serde(rename_all = "camelCase")]`, no `deny_unknown_fields`):

```rust
pub struct TableStat { pub name: String, pub row_count: i64, pub size_bytes: i64 }
pub struct DbStats { pub tables: Vec<TableStat>, pub total_size_bytes: i64 }
pub struct TokenInfo { pub id: String, pub name: String, pub created_at: i64, pub revoked: bool }
pub struct MetricsSnapshot {
    pub queries_total: i64, pub mutations_total: i64, pub uploads_total: i64,
    pub ws_connections: i64, pub active_subscriptions: i64, pub pool_size: i64,
    pub pool_idle: i64, pub uptime_seconds: i64,
}
pub struct HotConfig { pub allowed_origins: Vec<String>, pub session_ttl_days: i64, pub max_file_size: i64 }
pub struct ConfigResponse {
    pub port: i64, pub public_url: String, pub github_base_url: String, pub github_api_url: String,
    pub database_url_configured: bool, pub admin_key_configured: bool, pub github_configured: bool,
    pub google_configured: bool, pub hot: HotConfig, pub version: String, pub git_commit: String,
    pub admins: Vec<AdminMember>,
}
#[derive(Debug, Clone, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HotConfigPatch {
    #[serde(default, skip_serializing_if = "Option::is_none")] pub allowed_origins: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")] pub session_ttl_days: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")] pub max_file_size: Option<i64>,
}
pub struct OpEvent { pub db: String, pub table: String, pub doc_id: String, pub kind: String, pub ts: i64, pub owner: Option<String> }
```
(`OpEvent.kind` is a `String` — the admin client passes it through; consumers match on it. `OpEvent.owner` is `Option<String>` for the `string | null` wire.)

- [ ] **Step 4: Methods** in the admin impl block:

```rust
pub async fn get_schema(&self, db: &str) -> Result<crate::schema::SchemaDef, RtDbError> {
    self.get_json(&format!("/admin/dbs/{}/schema", urlencoding(db)), &[]).await
}
pub async fn db_stats(&self, db: &str) -> Result<crate::wire::admin::DbStats, RtDbError> {
    self.get_json(&format!("/admin/dbs/{}/stats", urlencoding(db)), &[]).await
}
pub async fn list_tokens(&self, db: &str) -> Result<Vec<crate::wire::admin::TokenInfo>, RtDbError> {
    #[derive(serde::Deserialize)] struct Resp { tokens: Vec<crate::wire::admin::TokenInfo> }
    Ok(self.get_json::<Resp>("/admin/tokens", &[("db", db)]).await?.tokens)
}
pub async fn metrics(&self) -> Result<crate::wire::admin::MetricsSnapshot, RtDbError> {
    self.get_json("/admin/metrics", &[]).await
}
pub async fn get_config(&self) -> Result<crate::wire::admin::ConfigResponse, RtDbError> {
    self.get_json("/admin/config", &[]).await
}
pub async fn patch_config(&self, patch: &crate::wire::admin::HotConfigPatch) -> Result<crate::wire::admin::ConfigResponse, RtDbError> {
    self.deserialize(self.patch_json("/admin/config", patch).await?).await
}
pub async fn ops_recent(&self, db: &str, table: Option<&str>, n: Option<u32>) -> Result<Vec<crate::wire::admin::OpEvent>, RtDbError> {
    #[derive(serde::Deserialize)] struct Resp { ops: Vec<crate::wire::admin::OpEvent> }
    let mut q: Vec<(&str, String)> = vec![("db", db.to_string())];
    if let Some(t) = table { q.push(("table", t.to_string())); }
    let nstr;
    if let Some(n) = n { nstr = n.to_string(); q.push(("n", nstr)); }
    let qref: Vec<(&str, &str)> = q.iter().map(|(k,v)| (*k, v.as_str())).collect();
    Ok(self.get_json::<Resp>("/admin/ops/recent", &qref).await?.ops)
}
```

Note: `metrics`/`get_config`/`patch_config` don't take `db` (server-wide). `get_json`'s query param is `&[(&str, &str)]` — confirm the exact signature at `http.rs:655` and adapt the `ops_recent` query-building to it (the borrow juggling above is a sketch; make it compile cleanly). Path-segment encoding: confirm whether the existing methods URL-encode `db` in paths or pass it raw (check `export_db`/`push_schema`); match that — `urlencoding()` is a placeholder name; use whatever the codebase uses (likely `reqwest`'s `.query()` handles query encoding, and path segments may be raw — match the existing admin methods' convention).

Re-export the 8 new pub types from `lib.rs:67-68`.

- [ ] **Step 5: GREEN** + clippy + fmt; commit `feat(rust-client): admin read-back/tokens/metrics/config/ops endpoints`.

---

## Task 3: owner-bypass query/mutate

**Files:** `http.rs` (2 methods + tests).

- [ ] **Step 1: Tests** in `admin_tests` — `admin_query` POSTs `{query}` to `/admin/db/{db}/query` (singular) and unwraps `{result}`; `admin_mutate` POSTs `{txn, idempotencyKey?}` to `/admin/db/{db}/mutate` and unwraps `{results}`; omit-when-no-key.

- [ ] **Step 2: RED.**

- [ ] **Step 3: Methods** — mirror non-admin `run` (`http.rs:89`, uses `json_result`) and `mutate` (`http.rs:142`, uses `deserialize::<{results: Vec<Value>}>` + per-element `StepResult` parse). Drop the `db` body field (path carries it):

```rust
pub async fn admin_query<T: serde::de::DeserializeOwned>(&self, db: &str, query: &crate::query::Query) -> Result<T, RtDbError> {
    #[derive(serde::Serialize)] struct Body<'a> { query: &'a crate::query::Query }
    let resp = self.post_json(&format!("/admin/db/{}/query", /* encode db */), &Body { query }).await?;
    self.json_result::<T>(resp).await
}
pub async fn admin_mutate(&self, db: &str, txn: &crate::mutation::Transaction, idempotency_key: Option<&str>) -> Result<Vec<crate::mutation::StepResult>, RtDbError> {
    #[derive(serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Body<'a> { txn: &'a crate::mutation::Transaction, #[serde(skip_serializing_if = "Option::is_none")] idempotency_key: Option<&'a str> }
    let resp = self.post_json(&format!("/admin/db/{}/mutate", /* encode db */), &Body { txn, idempotency_key }).await?;
    #[derive(serde::Deserialize)] struct Resp { results: Vec<serde_json::Value> }
    let parsed = self.deserialize::<Resp>(resp).await?;
    parsed.results.into_iter()
        .map(|v| serde_json::from_value::<crate::mutation::StepResult>(v)
            .map_err(|e| RtDbError::internal(format!("invalid step result: {e}")))).collect()
}
```

(Confirm `json_result` is `pub(crate)`-reachable from the admin impl block — it's at `http.rs:465`; it's a private method on the same struct, so callable. Match the db-path-encoding convention from Task 2.)

- [ ] **Step 4: GREEN** + clippy + fmt; commit `feat(rust-client): admin owner-bypass query/mutate endpoints`.

---

## Task 4: docs + gate

**Files:** `FEATURE_MATRIX.md`, `rust-client/README.md` (only if it itemizes admin methods), `rust-client/tests/http_integration.rs` (optional live coverage).

- [ ] **Step 1**: `FEATURE_MATRIX.md` "Admin control plane" row — note rust-client now covers the full server admin HTTP surface (machine subset; login/logout/stream excluded by design).
- [ ] **Step 2**: README — only if it itemizes admin methods; otherwise no change.
- [ ] **Step 3 (optional)**: extend the `admin_control_plane` live integration test (`http_integration.rs:79`) with `admins_list`/`metrics`/`get_config` round-trips (secondary; the wiremock unit tests are the gate).
- [ ] **Step 4 — gate**: `cd rust-client && cargo test --features admin && cargo clippy --features admin -- -D warnings && cargo fmt --check`. (Full `make checkall` at branch finish.)
- [ ] **Step 5 — commit**: `docs: rust-client admin endpoint parity`.

---

## Self-Review (completed during authoring)

- **Spec coverage:** A-rust = 11 machine-relevant admin methods. Task 1 = helpers + allowlist (3); Task 2 = read-back/config/ops (7); Task 3 = query/mutate (2); Task 4 = docs/gate. ✅
- **Scope:** login/logout/stream + OAuth excluded by design (rust = machine client). ✅
- **Casing:** all new types `rename_all="camelCase"`, snake_case Rust fields, no `deny_unknown_fields`; `HotConfigPatch` pub + skip_serializing_if. Matches the server wire + the codebase's `AuthedUser`/`UploadResult`/`ScheduleInfo` convention. ✅
- **Helpers:** `delete_json`/`patch_json` added for the 2 non-POST routes; the other 9 reuse `post_json`/`get_json`/`deserialize`/`json_result`. ✅
- **Placeholders:** method bodies + types are real Rust; the flagged verify-points (the `get_json` query-param signature; the db-path-encoding convention; `json_result` reachability) are confirm-against-existing-code items for the implementer, not placeholders. ✅
