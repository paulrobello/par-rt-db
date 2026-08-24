# Audit Remediation Playbook

> Companion to `AUDIT.md` (2026-08-23). One entry per issue, ordered to match the
> `## Remediation Plan` phases. `/fix-audit` points each phase agent at its own entries.
> `/fix-audit` writes its *report* to `AUDIT-REMEDIATION.md`; this file is the *plan*.

## Ground rules for every entry

- The gate is `make checkall` from the repo root (`make -C /Users/probello/Repos/par-rt-db checkall`). It needs the dev Postgres (`make dev-db-up`, port 55434) and a built `ts-client/dist` (`make ts-client-build`). A green vitest run is not a typecheck; run the real gate.
- Single server test: `cargo test --manifest-path server/Cargo.toml --test <file> <name>`. Never `cd` in a compound command (it shifts cwd for later calls); use `--manifest-path`, `--cwd`, `--directory`, `make -C`.
- Server invariants that every code entry must preserve: all writes through the committer; every committing arm calls `publish_taps`; identifiers validated + double-quoted, values bound with `$n`; client-facing 500s carry generic text; `fetch_optional` for lookups that can miss; no `unwrap`/`expect` outside `#[cfg(test)]`; clippy `-D warnings` clean.
- Any wire-visible change (new field, tag, error code) must be mirrored in `ts-client/src/protocol.ts`, `rust-client/src/wire.rs`, `python-client/src/par_rt_db/wire.py`, `swift-client/Sources/ParRtDbClient/Wire.swift`, ship a `wire-corpus/` case, and update `FEATURE_MATRIX.md`. A missing `skip_serializing_if` on an `Option` shows up as a `where:null` drift in the client corpus test.
- New `RTDB_*` env var ⇒ add to `.env.example` and `docker-compose.yml`'s environment block or `env-drift-check` fails.
- New rust-client `[[test]]` target ⇒ add its stub to `Dockerfile:26-35` (until ARC-011 lands).
- par-mem: `repository_id: "par-rt-db"`. Before moving/renaming a symbol run `get_impact(symbol)` and `get_symbol_context(symbol)` to enumerate callers; the index lags the working tree, so re-read files before editing.
- Line numbers below are from HEAD `2609de3`; earlier entries shift later ones. Re-read before editing.

---

## Phase 1 — Promoted Security (sequential)

### [SEC-001] IPv4-mapped IPv6 SSRF bypass in `is_blocked_ip`
- **Files**: `server/src/webhook.rs:153-164` (V6 arm of `is_blocked_ip`); tests in the same file's `#[cfg(test)]` module.
- **Steps**:
  1. At the top of the `IpAddr::V6(v) => { ... }` arm add, before the existing checks:
     ```rust
     if let Some(v4) = v.to_ipv4_mapped() {
         return is_blocked_ip(IpAddr::V4(v4));
     }
     if let Some(v4) = v.to_ipv4() {
         // deprecated ::a.b.c.d compat form (not ::ffff:), still routable on some stacks
         return is_blocked_ip(IpAddr::V4(v4));
     }
     ```
     Because `to_ipv4()` also matches `::1`/`::` as `0.0.0.1`/`0.0.0.0`, keep it after `to_ipv4_mapped()` and confirm `::1` still returns `true` (it will: `0.0.0.1` is not blocked by the V4 table, so keep the existing `v.is_loopback()` check before the compat conversion, or order the compat conversion after the loopback/unspecified checks).
  2. Add unit tests: `"::ffff:127.0.0.1"`, `"::ffff:169.254.169.254"`, `"::ffff:10.0.0.1"`, `"::ffff:8.8.8.8"` (false), and `validate_webhook_url("https://[::ffff:169.254.169.254]/x")` returns the same error the plain `169.254.169.254` literal does.
- **Method**: The `url` crate yields `Host::Ipv6` for `[::ffff:a.b.c.d]`, so the fix must be inside `is_blocked_ip` (used by both the literal path at `:294-302` and `WebhookDnsResolver::resolve` at `:220-247`), not in the caller. Do not add a new denylist copy.
- **Verify**: `cargo test --manifest-path server/Cargo.toml --lib webhook`; `cargo test --manifest-path server/Cargo.toml --test webhook_test`; `make checkall`.

### [SEC-003] Bind transform parameters into the signed storage URL
- **Files**: `server/src/signed_url.rs:35-60` (`sign`, `verify`); `server/src/http_api.rs:853-900` (`serve_public_handler`), the mint handler (grep `signed_url::sign(` in `server/src/http_api.rs` and `server/src/admin/`); `server/src/image_transform.rs` (`TransformParams::parse`, add a `canonical()` string); client mirrors if any mint locally.
- **Steps**:
  1. `grep -rn "signed_url\|sign(" ts-client/src rust-client/src python-client/src swift-client/Sources | grep -i sign` — confirm no client mints signatures locally (the audit found none; verify).
  2. Add `TransformParams::canonical(&self) -> String` producing a deterministic `w=..&h=..&q=..&fit=..&format=..` with absent params omitted, keys sorted.
  3. Change `sign`/`verify` message to `format!("{id}.{exp_ms}.{canon}")` where `canon` is `""` for an un-transformed serve. Keep the function signatures but add a `transform: &str` parameter.
  4. In the mint path, accept optional transform params on the mint request, canonicalize, sign with them, and echo them into the returned URL. In `serve_public_handler`, parse the query into `TransformParams`, canonicalize, verify against that string; reject with `UNAUTHORIZED` (existing code) on mismatch.
  5. Update unit tests in `signed_url.rs` and the storage HTTP tests (`server/tests/storage_*`); add a case: URL minted for `w=100` rejected for `w=200` and for no transform.
  6. README storage section (`README.md`, "signed URLs"): state that a signature covers exactly one render and that pre-deploy URLs are invalidated.
- **Method**: This invalidates URLs minted before deploy; note it in the CHANGELOG `Changed` (DOC-001 picks it up). Alternative accepted by the audit if the product wants "one signature, any render": document it and make `RTDB_STORAGE_RATE_LIMIT_RPM` (the public per-IP limit) mandatory when `RTDB_STORAGE_REQUIRE_SIGNED_URLS=true`.
- **Verify**: `cargo test --manifest-path server/Cargo.toml --lib signed_url`; `cargo test --manifest-path server/Cargo.toml --test storage_test` (and any `storage_*` files); `make checkall`.

### [SEC-005] Route-namespace the per-IP rate bucket
- **Files**: `server/src/rate_limit.rs:50-54` (`RateKey`), `:97-135` (`check`), `:191-227` (`check_pg`, the `("ip", ip)` tuple at `:200`), `:295` (storage caller); `server/src/admin/login.rs:52`; `server/src/auth/provider.rs:882`.
- **Steps**:
  1. Change `RateKey::Ip(String)` to `RateKey::Ip { route: &'static str, ip: String }`.
  2. Update the three callers with routes `"admin_login"`, `"anon_mint"`, `"storage"`.
  3. In `check_pg`, map to `key_type = "ip"` and `key_text = format!("{route}:{ip}")` so the shared table schema (`rtdb_auth.rate_counters(key_type, key, minute_bucket)`) is unchanged.
  4. Update the in-memory `HashMap` key derivation in `check` (it hashes the enum, so the derive handles it).
  5. Tests: extend `rate_limit.rs` unit tests so 11 storage hits from one IP leave `admin_login` allowed; add the same to `server/tests/rate_limit_test.rs` (or the file that covers `check_pg`).
- **Method**: Keep the table schema untouched so multi-instance deployments need no migration. ARC-007 (local bucket) builds on this key shape, so land this first.
- **Verify**: `cargo test --manifest-path server/Cargo.toml --lib rate_limit`; `cargo test --manifest-path server/Cargo.toml --test rate_limit_test`; `make checkall`.

### [SEC-006] Re-validate the admin credential on the `/admin/stream` tick
- **Files**: `server/src/admin/observability.rs:220-252` (stream loop); `server/src/admin/mod.rs` (`authenticate_admin`, session validity helper — grep `session_still_valid` / `authenticate_admin`).
- **Steps**:
  1. Capture the resolved admin principal (admin key vs. session token hash) after the handshake.
  2. On the existing 1 s gauge tick, if the principal is a session, run the same DB check the admin middleware runs (`admin/mod.rs:161-198`); on failure send a close frame with code 4401 and break. Admin-key principals need no re-check (the key is static config).
  3. Test in `server/tests/admin_stream_test.rs` (or the file covering `/admin/stream`): open the stream with a session, revoke via `DELETE /admin/sessions`, assert the socket closes within ~2 s.
- **Method**: Mirrors the `/sync` SEC-004 invariant (`ws.rs:124-131`). Don't re-check on every event; the tick is the cheap point.
- **Verify**: the new test; `make checkall`.

### [SEC-007] Explicit filter depth, `In` length and search-query caps
- **Files**: `server/src/schema.rs:486` (`validate_filter_expr_fields`, the one place every filter passes through — add the depth/length checks here); `server/src/query/search.rs:123-126` (`search.query` length); `server/src/query/filter.rs:152-172` (keep the empty-`In` check); `server/src/error.rs` (use existing `BAD_REQUEST`).
- **Steps**:
  1. Add consts in `schema.rs`: `MAX_FILTER_DEPTH: usize = 32`, `MAX_IN_VALUES: usize = 1000`; in `search.rs`: `MAX_SEARCH_QUERY_BYTES: usize = 4096`.
  2. Thread a `depth` counter through the recursive walk in `validate_filter_expr_fields`; return `RtDbError::bad_request("filter nesting exceeds 32 levels")` and `"in: at most 1000 values"`.
  3. Reject over-long `search.query` with `bad_request` before compilation.
  4. Add unit tests for depth 33, 1001 `In` values, 4097-byte query; add a `wire-corpus/semantics/` case for each cap and mirror the cap in the four in-memory engines (they share the corpus, so the case will fail on each engine until mirrored).
- **Method**: Server-side `serde_json` (128-level default) and the 64 KB WS frame cap already bound this today; the point is a project-owned limit. Because engines must agree, land the server + corpus case + four engine mirrors together (ts `in_memory/query.ts` / `store.ts`, rust `in_memory/query.rs`, python `in_memory/query.py`, swift `InMemoryEngine.swift`).
- **Verify**: `cargo test --manifest-path server/Cargo.toml --lib schema`; `cargo test --manifest-path server/Cargo.toml --test semantics_corpus`; `bunx vitest run --root ts-client tests/semantics-corpus.test.ts`; `uv run --directory python-client pytest -q tests/test_semantics_corpus.py`; `make checkall`.

---

## Phase 2 — Critical Architecture (sequential, in this order)

### [ARC-002] Spool forwarded requests/replies; enforce the `pg_notify` cap
- **Files**: `server/src/forward.rs:195-235` (`Forwarder::forward`), `:425-490` (listener), reply path `:452-460`; `server/src/db.rs` (`bootstrap_ddl`, add the table); `server/src/reaper.rs` or `forward.rs` (sweep); `server/tests/multi_instance_stage4_test.rs`.
- **Steps**:
  1. In `db::bootstrap_ddl` add `CREATE TABLE IF NOT EXISTS rtdb_auth.forward_queue (id uuid PRIMARY KEY, kind text NOT NULL CHECK (kind IN ('request','reply')), target text NOT NULL, payload jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now())` plus an index on `(kind, target, created_at)`.
  2. `Forwarder::forward`: `INSERT INTO rtdb_auth.forward_queue ... RETURNING id` with the serialized `ForwardRequest`, then `pg_notify(WRITE_FORWARD_CHANNEL, id)`. The NOTIFY payload becomes a 36-byte id.
  3. Listener request branch: on notification, `SELECT payload FROM rtdb_auth.forward_queue WHERE id=$1 AND kind='request'` (`fetch_optional`; a miss means already consumed → skip), decode, then `DELETE` the row after the reply is written (or after execution).
  4. Reply path: same shape — insert the `ForwardReply` row with `kind='reply'`, `target=origin`, notify the id on `WRITE_REPLY_CHANNEL`; `handle_reply` loads and deletes it.
  5. Sweep: in the existing forward listener loop (or the rate-sweep task in `lib.rs`) delete rows older than `2 * RTDB_FORWARD_TIMEOUT_MS`.
  6. Keep a hard guard: if the serialized payload exceeds, say, 16 MiB, return `ForwardFail::Notify(RtDbError::bad_request("forwarded write too large"))`.
  7. Tests: a forwarded `Mutate` with a 20 KB document; a `RunPushSchema` with a 30-table schema; a `patchByQuery` reply > 8 KB. Assert success end-to-end across two `AppState`s with distinct `instance_id` (the file already builds two states; reuse its helper).
  8. Update the design spec's "As built" section and README's multi-instance paragraph (DOC-004 will restructure, but note the spool here).
- **Method**: Spooling fixes the cap and makes forwarded requests durable across a listener reconnect. Keep the request/reply wire structs unchanged; only the transport changes. Don't put the spool in a per-db schema (forwarding is instance-level).
- **Verify**: `cargo test --manifest-path server/Cargo.toml --test multi_instance_stage4_test`; `cargo test --manifest-path server/Cargo.toml --test notify_test`; `make checkall`.

### [ARC-001] Cross-replica subscription invalidation for owner-side writes
- **Files**: `server/src/notify.rs:104-152` (`publish_ops`), `:169+` (`run_listener`); `server/src/committer.rs` (`publish_taps` call site ~`:1538-1548`; `complete_forwarded_reply` `:685-740` shows the fan-out call to copy); `server/src/lib.rs:274-278` (listener spawn — pass `subs` and `schemas`); `server/src/subs.rs` (`SubscriptionManager::fan_out`, `WriteSet`); `server/tests/multi_instance_stage4_test.rs`.
- **Steps**:
  1. Define a new channel `WRITE_SET_CHANNEL = "rtdb_write_sets"` (or extend the op-feed payload). Payload: `{instance_id, db, write_set: WriteSet}` with `doc_values` skipped (already `#[serde(skip)]`). Because a `WriteSet` for a bulk write can exceed 8000 bytes, route it through the ARC-002 spool table (`kind='writeset'`, `target=''`, broadcast) when the serialized size ≥ 7500 bytes, otherwise inline in the NOTIFY.
  2. In `publish_taps` (every committing arm reaches it), after the local `subs.fan_out`, when `multi_instance` publish the write set once per commit (not per op).
  3. Extend `run_listener` (or add `run_write_set_listener`) to take `Arc<SubscriptionManager>`, `SchemaCache`, `PgPool`; on a non-self payload, `schemas.get(db)` then `subs.fan_out(&pool, db, &schema, &write_set)` — the exact call `complete_forwarded_reply` already makes.
  4. Remove the now-redundant fan-out from `complete_forwarded_reply` only if the listener path is guaranteed to fire on the origin too (it is, since the owner publishes to all). Keep it if you want the origin's push to be earlier; then dedupe by `request_id` is unnecessary (fan-out is idempotent — a second re-run pushes nothing on no change).
  5. Test: two `AppState`s; subscribe via `/sync` on B; mutate through A's HTTP `/api/mutate/{db}` directly (not forwarded); assert B pushes `queryUpdate` within the forward timeout. Second test: scheduler-driven write on A (use `RunScheduled` via the admin schedule route) invalidates B.
  6. README multi-instance section and `docs/superpowers/specs/2026-08-22-multi-instance-stage4-design.md` "As built": state the guarantee (owner-side writes invalidate every replica's subscriptions).
- **Method**: Do not send `doc_values`; Indexed/Ordered subscriptions degrade to the conservative re-run path (`subs.rs` documents this). The listener performs reads only — no committer interaction — so the single-writer invariant holds.
- **Verify**: the two new tests; `cargo test --manifest-path server/Cargo.toml --test subs_test`; `make checkall`.

### [ARC-003] Server-minted idempotency key on forwarded mutates
- **Files**: `server/src/committer.rs` (`submit` → `forward_or_takeover` `:632-669`, `takeover_submit`); `server/src/forward.rs` (`ForwardWrite::Mutate` carries the txn + key — grep `forward_write_of`); the `Mutate` request arm's `idempotency_key` field.
- **Steps**:
  1. In `forward_or_takeover`, before `forwarder.forward`, if `req` is `Mutate { idempotency_key: None, .. }`, set it to `Some(uuid::Uuid::now_v7().simple().to_string())` on the request *and* in the `ForwardWrite` payload.
  2. `takeover_submit` re-submits the same `req` (now carrying the key) so a duplicate execution hits the shared `mutations` dedup table and returns the first outcome.
  3. Test in `multi_instance_stage4_test.rs`: forward a mutate, simulate a late reply (drop the reply row / delay), force takeover, assert one insert.
  4. README `:1057-1059`: replace "send an idempotency key" with the new guarantee (client keys are still honored).
- **Method**: Only `Mutate` needs this; migrate/push/restore/merge are idempotent by construction. Keep the minted key out of the client response (it's internal).
- **Verify**: the new test; `cargo test --manifest-path server/Cargo.toml --test mutation_dedup_test`; `make checkall`.

### [ARC-005] Split `committer.rs` into a module (pure move)
- **Files**: `server/src/committer.rs` (2754 lines) → `server/src/committer/{mod.rs, lease.rs, forwarding.rs, supervisor.rs, taps.rs, arms/{mod.rs, mutate.rs, scheduled.rs, workflow.rs, reaper.rs, merge.rs, migrate.rs, schema.rs}}`; `server/src/lib.rs` (`mod committer;` unchanged); every `crate::committer::` importer (run `get_impact("committer::Committers")` and `grep -rn "committer::" server/src server/tests`).
- **Steps**:
  1. Run par-mem `get_symbol_context` on `Committers`, `CommitterRequest`, `publish_taps`, `run_committer` to list external callers; the public surface must stay identical (`pub use` from `mod.rs`).
  2. Move regions: lease (`:131-186`) → `lease.rs`; forward shims (`:188-338`) → `forwarding.rs`; `Committers` + `submit`/`channel_for` (`:340-960`) stay in `mod.rs`; reclaimer + quota warmer (`:1117-1335`) → `supervisor.rs`; `run_committer` + `publish_taps` (`:1336-1585`) → `mod.rs` + `taps.rs`; each `handle_*` (`:1586-2754`) → `arms/<name>.rs`.
  3. Keep `publish_taps` `pub(super)` so only arms can call it (the tap invariant enforced by visibility). Keep `execute_txn` calls only inside `arms/`.
  4. `cargo fmt --all`; fix `use` paths; no logic edits. Update `docs/ARCHITECTURE.md` path references (DOC-003/004 will rewrite the section; leave a note).
- **Method**: This is a pure move; the diff should be relocations plus `use` lines. Do it after ARC-001/002/003 so their small edits aren't re-targeted. Because the pre-commit clippy takes > 2 min, commit with a long timeout.
- **Verify**: `cargo clippy --manifest-path server/Cargo.toml --all-targets -- -D warnings`; full `make checkall`; `grep -rn "publish_taps(" server/src | wc -l` still 8.

### [ARC-004] Extract `par-rt-db-core` (wire types + pure engine helpers)
- **Files**: new `core/` crate (`Cargo.toml` workspace member); `server/src/{value_expr.rs, schema.rs (helpers only), txn.rs (worst_case_affected), ddl.rs (detect_destructive_changes), dsl.rs (FilterExpr/ValueExpr), migrate.rs (helpers)}`; `rust-client/src/{value_expr.rs, schema.rs, wire.rs}`, `rust-client/src/in_memory/{mod.rs, value_expr.rs, migrate.rs, validate.rs}`; `server/Cargo.toml`, `rust-client/Cargo.toml`, root `Cargo.toml`; `Dockerfile` (dependency layer must `COPY core/`); `Makefile` (fmt/lint/test targets per ARC-014 or add `core`).
- **Steps** (two commits):
  1. Wire types first: create `core/src/wire.rs` holding `FilterExpr`, `ValueExpr`, and any serde types that are byte-identical between `server/src/dsl.rs:291` and `rust-client/src/wire.rs:903` (diff them first: `diff <(sed -n 'X,Yp' server/src/dsl.rs) <(sed -n 'A,Bp' rust-client/src/wire.rs)`). Re-export from both crates at their current paths (`pub use par_rt_db_core::wire::FilterExpr;`) so no call site changes.
  2. Pure helpers second: `walk_value_expr_fields`, `literal_set`, `worst_case_affected`, `eval_value_expr`, `detect_destructive_changes`, `strip_on_delete`, `stamp_computed`, `rename_value_expr_fields`, `validate_value`/`validate_doc`, `apply_patch`. For each, run `find_duplicate_code` / `get_symbol_context` in both crates, confirm the bodies are identical or reconcile the delta (the corpus should catch a semantic difference), move to `core`, re-export.
  3. `core` must have no tokio/sqlx/axum deps; `serde`, `serde_json`, `chrono`/`uuid` as needed.
  4. Add `core` to the Dockerfile dependency layer (`COPY core/Cargo.toml` + stub `src/lib.rs`) and to CI's cargo commands.
- **Method**: Re-exports keep every existing path working, so the diff is additive plus deletions of the duplicates. Run the wire-corpus after each move; it is the drift detector. Watch `Dockerfile` — a new crate not copied into the dependency layer breaks `make deploy` but not `checkall`.
- **Verify**: `cargo test --workspace`; `cargo test --manifest-path server/Cargo.toml --test semantics_corpus`; `cargo test --manifest-path rust-client/Cargo.toml --test semantics_corpus --test wire_corpus`; `make checkall`; `docker build -t rtdb-test .` (or `make build` if it exercises the Dockerfile) to prove the dependency layer.

### [ARC-010] Consolidate server integration tests into one binary
- **Files**: `server/tests/*.rs` (55 files) → `server/tests/main.rs` + `server/tests/<name>.rs` as `mod`s (or `server/tests/suite/<name>.rs`); `server/tests/common/mod.rs`; `docker-compose.dev.yml` (`max_connections`); `.github/workflows/ci.yml` (remove the disk-exhaustion workaround if it exists); `Dockerfile` if it stubs server tests.
- **Steps**:
  1. Create `server/tests/main.rs` with `mod common; mod audit_test; mod backup_test; ...` for every current file (keep file names; cargo treats a file with a `main.rs` sibling as a module only if it is declared, so also add `[[test]] name = "main" path = "tests/main.rs"` and set `autotests = false` in `server/Cargo.toml`).
  2. Remove `#[allow(dead_code)]` per-function attributes in `common/mod.rs` (QA-012 is subsumed) and replace the per-binary pool creation with a `OnceCell<PgPool>` (`common::pool()`).
  3. Keep `TestDb` RAII per test unchanged.
  4. Raise nothing else; run the suite with `--test-threads` default and check pool usage; consider lowering `max_connections` in `docker-compose.dev.yml` afterwards.
- **Method**: One link instead of 55 cuts pre-commit clippy time and pool contention (the `oauth_test` PoolTimedOut flake). Tests that rely on process-level state (env vars via `std::env::set_var`) must be audited: grep `set_var` in `server/tests` and convert to per-test config injection, since one binary shares the process.
- **Verify**: `cargo test --manifest-path server/Cargo.toml --test main`; `time make checkall` before/after; three consecutive green full-suite runs.

### [ARC-011] Generate/gate the Dockerfile rust-client test stub list
- **Files**: `Dockerfile:26-35`; `Makefile` (`checkall` list at `:200`); new `scripts/dockerfile-stub-check.sh`.
- **Steps**:
  1. Write `scripts/dockerfile-stub-check.sh`: extract `[[test]]` `path = "..."` (and `name`) entries from `rust-client/Cargo.toml` (and `cli/Cargo.toml`, `server/Cargo.toml` if they declare any), extract the `touch`ed paths from the Dockerfile dependency layer, diff, exit 1 with the missing list.
  2. Add `dockerfile-stub-check` to `checkall` in the Makefile (next to `env-drift-check`).
  3. Optionally replace the hand list in the Dockerfile with a generated loop: `RUN for t in $(grep -A2 '^\[\[test\]\]' rust-client/Cargo.toml | grep path | cut -d'"' -f2); do mkdir -p "rust-client/$(dirname $t)" && echo 'fn main(){}' > "rust-client/$t"; done`.
- **Method**: The gate is the durable fix; the generated loop removes the list entirely. `cargo chef` is the larger alternative — only if the Dockerfile is being reworked anyway.
- **Verify**: `bash scripts/dockerfile-stub-check.sh` passes; temporarily add a fake `[[test]]` and confirm it fails; `make checkall`.

---

## Phase 3a — Security (remaining)

### [SEC-002] Generic messages for quota-measure and image-transform errors
- **Files**: `server/src/quota.rs:82`; `server/src/image_transform.rs:200, 215, 220, 415-417`.
- **Steps**:
  1. `quota.rs:82`: `tracing::error!(db = %db, error = %e, "measure db storage failed"); RtDbError::internal("failed to measure storage")`.
  2. `image_transform.rs:200/215/220`: log the `image::ImageError` at `warn!` with the blob id, and construct `TransformError::Internal("image transform failed".into())`; at `:415-417` map `Internal` to `RtDbError::internal("image transform failed")`.
  3. Test: in the image transform unit tests, feed a corrupt JPEG and assert the returned `RtDbError.message` equals the fixed string.
- **Method**: Mirrors the generic `From<sqlx::Error>` behavior in `error.rs`. The other `internal(format!(..))` sites listed in AUDIT.md SEC-002 are serde/encode errors and should be left alone.
- **Verify**: `cargo test --manifest-path server/Cargo.toml --lib image_transform`; `cargo test --manifest-path server/Cargo.toml --test storage_test`; `make checkall`.

### [SEC-004] Verify Apple id_token signature via JWKS
- **Files**: `server/src/auth/apple.rs:232-296` (`decode_id_token_claims`); `server/src/auth/microsoft.rs:457-470` (JWKS fetch + cache to reuse); `server/src/auth/mod.rs` (hoist a shared `Jwks` cache if Microsoft's is private); `server/tests/oauth_apple_test.rs` (or the provider test file).
- **Steps**:
  1. Extract Microsoft's JWKS fetch/cache into `auth/jwks.rs` with `Jwks::verify(token, expected_alg, iss, aud)` supporting RS256 and ES256 (`jsonwebtoken` supports both; filter keys by `kty` — `RSA` for MS, `EC` for Apple).
  2. Apple: fetch `https://appleid.apple.com/auth/keys`, verify ES256 before reading claims; keep the existing `iss`/`aud`/`exp` checks.
  3. Tests: generate an ES256 keypair at runtime (gitleaks fires on PEM header literals — never commit a PEM), serve it via wiremock as the JWKS, sign a valid token and a tampered one; assert accept/reject. Follow the existing Microsoft test pattern.
  4. Update `docs/OAUTH_SETUP.md` Apple section (remove the "signature not verified" note) and remove the accepted-risk comment.
- **Method**: Reuse, do not duplicate, the JWKS cache. Keep Microsoft behavior byte-identical.
- **Verify**: the provider tests; `make checkall`.

---

## Phase 3b — Architecture (remaining)

### [ARC-006] Batch / off-turn cross-replica op-feed notify
- **Files**: `server/src/notify.rs:104-152` (`publish_ops`); the `publish_taps` call site in `server/src/committer/taps.rs` (post-ARC-005).
- **Steps**:
  1. Change `publish_ops` to build `Vec<OpNotifyPayload>` chunks whose serialized JSON stays under 7500 bytes and send one `pg_notify` per chunk (payload becomes a JSON array).
  2. Update `run_listener` to decode either a single object (backward compat during rolling deploy) or an array.
  3. Move the call off the committer turn: `tokio::spawn` it after the reply is sent, the same pattern as the quota refresh at the old `committer.rs:1570-1583`.
  4. Test in `notify_test.rs`: 2000-op delete produces ≤ N notifications and every op reaches replica B's op feed.
- **Method**: Ordering across chunks is preserved by a single `ts`; the ring on the receiving side already tolerates batching.
- **Verify**: `cargo test --manifest-path server/Cargo.toml --test notify_test`; `make checkall`.

### [ARC-007] Local token bucket with periodic Postgres reconciliation
- **Files**: `server/src/rate_limit.rs` (`check`, `check_pg`, the sweep task); `server/src/lib.rs:369` (`RateLimiter::new_pg`); `server/src/config.rs` (new `RTDB_RATE_LIMIT_SYNC_MS`, default 1000; `RTDB_RATE_LIMIT_EXACT=false`); `.env.example`, `docker-compose.yml`; README config table.
- **Steps**:
  1. Keep `check_pg` as the exact path behind `RTDB_RATE_LIMIT_EXACT=true`.
  2. Default path: increment a local per-key counter; every `sync_ms`, a background task flushes local deltas with one batched `INSERT ... ON CONFLICT DO UPDATE SET count = count + EXCLUDED.count RETURNING key, count` and updates the local view of the shared count. Deny when local + shared-seen ≥ limit.
  3. Document the approximation (≤ replicas × sync-window overshoot) in README.
  4. Tests: unit test for the merge math; integration test that two limiters converge within two sync windows.
- **Method**: Depends on SEC-005's route-namespaced key. Keep the sweep of old minute buckets.
- **Verify**: `cargo test --manifest-path server/Cargo.toml --lib rate_limit`; `cargo test --manifest-path server/Cargo.toml --test rate_limit_test`; `make checkall` (env-drift-check for the new vars).

### [ARC-008] Bound forward-listener concurrency
- **Files**: `server/src/forward.rs:425-490`; `server/src/config.rs` (`RTDB_FORWARD_CONCURRENCY`, default 64); `.env.example`, `docker-compose.yml`; `server/src/error.rs` (reuse `RATE_LIMITED`).
- **Steps**:
  1. Create `Arc<Semaphore>` in `run_forward_listener`; `try_acquire_owned()` before `tokio::spawn`; on `Err`, write a `ForwardReply::failure(request_id, origin, RtDbError::rate_limited(...))` immediately so the origin returns a retryable error instead of timing out into takeover.
  2. Test: saturate with N+1 concurrent forwards using a blocked committer; assert the extra one gets `RATE_LIMITED` within the timeout.
- **Verify**: `cargo test --manifest-path server/Cargo.toml --test multi_instance_stage4_test`; `make checkall`.

### [ARC-009] `BackgroundTasks` handle + cancellation on shutdown
- **Files**: `server/src/lib.rs:177-341` (`AppState::new`); `server/src/main.rs` (graceful shutdown); `server/tests/common/mod.rs:112-320` (`test_state_*`).
- **Steps**:
  1. Add `tokio_util::sync::CancellationToken` to `AppState` (or a `BackgroundTasks { token, handles: Vec<JoinHandle<()>> }` returned alongside). Every spawned loop (`idle reclaimer`, presence flush, three `PgListener` loops, rate sweep, forward listener) selects on `token.cancelled()`.
  2. `main.rs`: after `with_graceful_shutdown` resolves, `token.cancel()` and `join_all` with a 5 s timeout; drain committers via the existing `draining` flag.
  3. Tests: `TestState` wrapper whose `Drop` cancels the token; use it in `test_state_*`.
- **Method**: Sequence after ARC-010 (shared test harness). `tokio_util` may already be a dep; check `server/Cargo.toml`.
- **Verify**: `make checkall`; a manual `cargo run` + SIGTERM shows the listeners exit in logs.

### [ARC-012] Nested `Config` sub-structs; table-driven `from_env`
- **Files**: `server/src/config.rs:43-350` (`Config`), `:763-986` (`from_env`), the `*Env` helper impls, `HotConfig`; every `state.config.<field>` consumer (run `get_impact("config::Config")` — 40+ modules).
- **Steps** (phased, ≤ 5 files per commit):
  1. Move `HotConfig` and its parsing to `server/src/config/hot.rs`.
  2. Introduce sub-structs one group at a time (`oauth: OAuthConfig { github, google, ... }`, `limits: LimitsConfig`, `multi_instance: MultiInstanceConfig`, `storage: StorageConfig`, `backup: BackupConfig`), each with `fn from_env() -> Result<Self, String>` using shared `env_u64`/`env_bool`/`env_parsed` helpers; update consumers with `sed`-style path rewrites after `get_impact` lists them.
  3. Keep `CommitterConfig::from_config`-style projections as field copies of one sub-struct.
  4. Update `scripts/env-drift-check.sh` grep if helper names change (see DOC-019).
- **Verify**: `make checkall` after each group; `bash scripts/env-drift-check.sh`.

### [ARC-013] Protocol version negotiation
- **Files**: `server/src/protocol.rs` (`ClientMessage::Auth`, `ServerMessage::Authed`), `server/src/ws.rs` (auth handshake), `server/src/http_api.rs` (read `X-Rtdb-Protocol`), `server/src/error.rs` (`UNSUPPORTED_PROTOCOL`, 400); four client wire files + their connect paths; `wire-corpus/` (golden vector for `auth` with `protocolVersion`); `FEATURE_MATRIX.md`; README WS section.
- **Steps**:
  1. Add `pub const PROTOCOL_VERSION: u32 = 1;` and an optional `protocolVersion` on `Auth` (`#[serde(default, skip_serializing_if = "Option::is_none")]`) and `Authed`. Absent ⇒ treated as 1 (compat).
  2. Server rejects `> PROTOCOL_VERSION` with `UNSUPPORTED_PROTOCOL`; HTTP reads the header the same way.
  3. Clients send the version on connect; wire-corpus golden vector for the new frame; corpus case for the error.
- **Method**: Additive and optional so a rolling deploy cannot break; the `skip_serializing_if` is what keeps the corpus drift check green.
- **Verify**: all five corpus runners; `make checkall`.

### [ARC-014] Workspace-level cargo invocations in Makefile
- **Files**: `Makefile:31-60`; `.pre-commit-config.yaml:14-25`.
- **Steps**: replace the per-crate `cd X && cargo ...` lines with `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo test --workspace` (keep per-package targets as aliases for the docs). In pre-commit, scope the Rust hook to `files: \.rs$` and run only `cargo fmt`/`clippy`, not the multi-language `make fmt`/`make lint`.
- **Verify**: `make checkall`; `time` before/after; `pre-commit run --all-files`.

### [ARC-015] Replace drain sleep-poll with `Notify`
- **Files**: `server/src/committer/supervisor.rs` (post-split; was `committer.rs:811-822`).
- **Steps**: add `drained: Arc<tokio::sync::Notify>` to the channel entry; the supervisor calls `notify_waiters()` on exit; `channel_for` awaits `notified()` under the existing 5 s deadline instead of `sleep(2ms)` looping. Name the deadline const (QA-018).
- **Verify**: `cargo test --manifest-path server/Cargo.toml --test committer_test` (or the file covering drain/reclaim); `make checkall`.

### [ARC-016] Handle `is_owner` TOCTOU in `execute_as_owner`
- **Files**: `server/src/forward.rs:275-280`.
- **Steps**: after `submit_owned`, if the error is the shadow's `CONFLICT` (match on code + the specific message the backstop emits), return `None` (stay silent so the origin takes over) instead of `Some(Err(CONFLICT))`. Add a doc comment explaining the window. Test by forcing a lease loss between check and submit (inject via a test-only hook or accept a unit test on the mapping function).
- **Verify**: `cargo test --manifest-path server/Cargo.toml --test multi_instance_stage4_test`; `make checkall`.

### [ARC-017] Wire-corpus case enumerating error codes
- **Files**: `wire-corpus/error-codes.json` (new), `server/src/error.rs`, `ts-client/src/errors.ts`, `rust-client/src/error.rs`, `python-client/src/par_rt_db/errors.py`, `swift-client/Sources/ParRtDbClient/Errors.swift`, each package's wire-corpus test.
- **Steps**: emit the list of `{code, httpStatus}` from `error.rs` (a `#[test]` that writes/compares the JSON), and add a test in each client that every code in the file is known to its `RtDbError` code enum/set. Document in `wire-corpus/README.md`.
- **Verify**: all five corpus tests; `make checkall`.

---

## Phase 3c — Code Quality

### [QA-001] Decompose `apply_schema_additive`
- **Files**: `server/src/ddl.rs:391-732`.
- **Steps**:
  1. Read the function once end to end; note the per-table loop's five concerns.
  2. Extract, in order: `fn create_table_ddl(db: &str, table: &TableDef) -> Vec<String>`, `async fn add_missing_columns(tx, db, old: Option<&TableDef>, new: &TableDef)`, `async fn backfill_computed(tx, db, old, new)`, `async fn ensure_soft_delete_column(...)`, `async fn create_indexes(tx, db, old, new)`.
  3. `apply_schema_additive` becomes the ordered driver; keep identifier quoting inside each helper (never build SQL from unvalidated names).
- **Method**: Behavior is pinned by `server/tests/schema_*` and the corpus. Keep the exact DDL statement text — schema tests may assert on it.
- **Verify**: `cargo test --manifest-path server/Cargo.toml --test schema_test --test schema_migrate_test` (all `schema_*` files); `make checkall`; `find_most_complex_functions` shows `apply_schema_additive` well under 20.

### [QA-002] Per-directive functions + `rename_field_refs`
- **Files**: `server/src/migrate.rs:190-416` (`validate_one`); engine mirrors `rust-client/src/in_memory/migrate.rs:1013`, `ts-client/src/in_memory/migrate.ts:153`, `python-client/src/par_rt_db/in_memory/migrate.py`, `swift-client/Sources/ParRtDbClient/InMemoryMigrate.swift`; `wire-corpus/semantics/`.
- **Steps**:
  1. Server: one `fn apply_<directive>(table: &mut TableDef, ...) -> Result<(), RtDbError>` per `Directive` variant; a single `fn rename_field_refs(table: &mut TableDef, from: &str, to: &str)` owning the list of name-bearing surfaces (indexes, `ownerField`, `collaboratorsField`, auto-increment, `authorize`, defaults, computed exprs, soft-delete field).
  2. Add one corpus case per surface (`migrate-rename-field-<surface>.json`) asserting the rename propagates.
  3. Mirror the helper shape in the four engines so the new cases pass.
- **Method**: If ARC-004 has landed, `rename_value_expr_fields` lives in `core` — call it, don't copy it. Do the server + corpus first; the mirrors are mechanical.
- **Verify**: `cargo test --manifest-path server/Cargo.toml --test migrate_test --test semantics_corpus`; each engine's corpus test; `make checkall`.

### [QA-003] De-duplicate Python sync/async and admin ops
- **Files**: `python-client/src/par_rt_db/{admin.py, http_client.py, aio_http_client.py}`, new `python-client/src/par_rt_db/_http_common.py`.
- **Steps**:
  1. (S) Move `transform_url` (`http_client.py:532`, `aio_http_client.py:425`) to `_http_common.py`; import in both.
  2. (M) Route `RtDbHttpClient`/`AsyncRtDbHttpClient` admin methods (`mint_token`, `ops_recent`, `admin_mutate`, `migrate_schema`, …) through the existing `_op_*` request builders in `admin.py:254-870` with the two executors; delete the hand-written twins.
  3. (L) Generate sync from async: either adopt `unasync` (build step in `pyproject.toml`, generated module committed and drift-checked in `make python-client-lint`) or a decorator-table that produces both classes from one method table. Add a parity test asserting `dir(RtDbAdminClient) == dir(AsyncRtDbAdminClient)` and the same for the HTTP clients.
- **Method**: Public method names and signatures must not change (SDK API). The parity test is the guard the corpus does not provide.
- **Verify**: `uv run --directory python-client pytest -q`; `make python-client-checkall` (or `make checkall`).

### [QA-004] Single `resolve_user` for OAuth providers
- **Files**: `server/src/auth/mod.rs` (new `ProviderIdentity`, `resolve_user`); `server/src/auth/{apple.rs:98, github.rs:62, gitlab.rs:64, google.rs:67, microsoft.rs:134, oidc.rs:76}`; `server/tests/oauth_*`.
- **Steps**:
  1. Define `struct ProviderIdentity<'a> { provider_id_column: &'static str, provider_id: &'a str, login: &'a str, email: &'a str }`.
  2. Implement `resolve_user(pool, id) -> Result<UserRow>`: (a) match by `provider_id_column = provider_id`; (b) else match by email and link the provider id; (c) else insert. This is GitHub's current semantics (`upsert_user`) generalized; it fixes the email-changed-on-Google case.
  3. Replace the six inline blocks; keep token exchange and claim parsing per provider.
  4. Tests: returning user with changed email per provider (reuse the wiremock GitHub e2e; begin-route tests for the others).
- **Method**: Land after SEC-004 so the Apple change is applied once. Behavior change (b) is intentional — call it out in the CHANGELOG `Changed`.
- **Verify**: `cargo test --manifest-path server/Cargo.toml --test oauth_test` (retry once on PoolTimedOut — known contention flake); `make checkall`.

### [QA-005] Split `schema.rs` into a module
- **Files**: `server/src/schema.rs` (3850) → `server/src/schema/{mod.rs, types.rs, validate.rs, filter.rs, computed.rs, value.rs, tests/*.rs}`.
- **Steps**: same recipe as ARC-005: `get_symbol_context` on `SchemaDef`, `TableDef`, `validate_filter_expr_fields`, `validate_value`; move regions (`impl TableDef` 677-1235 → `types.rs`/`validate.rs`; filter checking → `filter.rs`; computed inference → `computed.rs`; value validation → `value.rs`; `mod tests` 1515-3850 → `tests/` split by topic); `pub use` everything from `mod.rs`.
- **Method**: After SEC-007 (adds caps here) and ARC-004 (moves helpers out). Pure move.
- **Verify**: `make checkall`; `grep -c "" server/src/schema/*.rs` — no file > 1000 lines.

### [QA-006] Log fire-and-forget status-write failures
- **Files**: `server/src/committer/arms/{scheduled,workflow}.rs` (post-split; was `committer.rs:1678, 1695, 1765-1794, 1821, 1581`); `server/src/scheduler.rs:437` (`mark_error`).
- **Steps**: replace each `let _ = X.await;` with `if let Err(e) = X.await { tracing::warn!(db = %db, id = %id, error = %e, "<op> failed"); }`. Simpler: make `scheduler::mark_error`, `reschedule_recurring_error`, `workflows::mark_failed` log internally and return `()`; then the call sites stay one line.
- **Verify**: `cargo clippy --manifest-path server/Cargo.toml --all-targets -- -D warnings`; `cargo test --manifest-path server/Cargo.toml --test scheduled_test --test workflows_test`; `make checkall`.

### [QA-007] Remove avoidable unwrap/expect; gate the invariant
- **Files**: `server/src/backup.rs:135`; `server/src/http_api.rs:1230-1234`; `server/src/webhook.rs:311`; `server/src/auth/cookie.rs:69, 118, 210`; `server/src/lib.rs` (crate attr); `CLAUDE.md`.
- **Steps**:
  1. `http_api.rs:1230-1234`: replace `.any()` + `.find().expect()` with a single `find` and `match Some/None`.
  2. `backup.rs:135`: `DateTime::<Utc>::UNIX_EPOCH` (or `from_timestamp(0,0)` with a `LazyLock`).
  3. `cookie.rs:69/118/210`: `static TEMPLATE: LazyLock<HeaderValue> = LazyLock::new(|| HeaderValue::from_static(...))` — `from_static` is infallible and needs no `expect`.
  4. `webhook.rs:311`: `.unwrap_or(443)` (scheme is already restricted to http/https).
  5. Add to `server/src/lib.rs`: `#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]`; if any remaining site is provably infallible, `#[allow(clippy::expect_used)]` with a one-line justification.
  6. `CLAUDE.md`: keep the invariant text; add "enforced by `clippy::unwrap_used`/`expect_used` in `server/src/lib.rs`".
- **Verify**: `cargo clippy --manifest-path server/Cargo.toml --all-targets -- -D warnings`; `make checkall`.

### [QA-008] Migrate dashboard pages to `useAsync`
- **Files**: `dashboard/src/lib/useAsync.ts`; `dashboard/src/pages/{Audit,Admins,Config,Backups,Db,QueryConsole,ScheduledJobs,Storage,SchemaHistory,Schema,Tokens,Workflows,Subscriptions,Webhooks}Page.tsx`; `dashboard/src/**/*.test.tsx`.
- **Steps**: extend `useAsync` (or add `useAdminQuery(client, fetcher, deps, { enabled })`) to cover the `if (!db) return` guard and the reset-on-error choice; convert pages in batches of ≤ 5 files; keep each page's visible loading/error UI identical.
- **Verify**: `make dashboard-checkall` (typecheck + vitest); `bunx vitest run --root dashboard`; `make checkall`.

### [QA-009] `PendingQueues` in client dispatchers
- **Files**: `rust-client/src/ws.rs:1194-1482` (`run_session`), `:1616-1762` (`apply_server_message`); `ts-client/src/client.ts` (`handleMessage`); `swift-client/Sources/ParRtDbClient/WsClient.swift:1417` (`route`).
- **Steps**:
  1. Rust: `struct Pending<R> { by_id: HashMap<String, oneshot::Sender<R>>, unsent: VecDeque<(String, ClientMessage)> }` and `struct PendingQueues { mutate: Pending<MutReply>, schedule: Pending<SchedReply>, workflow: Pending<WfReply> }`; `run_session` takes `&mut PendingQueues`; drop the `#[allow(clippy::too_many_arguments)]`; split `apply_server_message` into `on_mutate_reply`, `on_schedule_reply`, `on_workflow_reply`, `on_query_update`, `on_presence`.
  2. ts/swift: same shape (a `Map`-backed generic pending class), per-family handlers.
- **Method**: Land before the Go client starts. The rust-client wire_corpus + ws tests pin behavior.
- **Verify**: `cargo test --manifest-path rust-client/Cargo.toml`; `bunx vitest run --root ts-client tests/client.test.ts`; `make swift-client-checkall`; `make checkall`.

### [QA-010] `TxnCtx`/`QueryCtx` parameter structs
- **Files**: `server/src/subs.rs:1042`, `audit.rs:82`, `webhook.rs:691`, `migrate.rs:1002, 1116`, `txn.rs:758, 1998`, `auth/provider.rs:161`, `query/terminals.rs:676, 713, 807`, `workflows.rs:219, 688, 756`, `rust-client/src/ws.rs:1196` (handled by QA-009).
- **Steps**: for each `#[allow(clippy::too_many_arguments)]`, `get_symbol_context` the function, introduce a `<Name>Ctx<'a> { pool, db, schema, principal, ... }` struct for the repeated tuple (`PrincipalCtx` in `txn.rs` is the model), update callers, delete the allow. ≤ 5 files per commit.
- **Verify**: `grep -rn "too_many_arguments\|type_complexity" server/src` shows 0 production sites; `make checkall`.

### [QA-011] `wait_until` helper replacing fixed sleeps
- **Files**: `server/tests/common/mod.rs`; `server/tests/{workflows,ttl,scheduled,presence_xreplica,webhook,notify,mutation_dedup,query}_test.rs`; `ts-client/tests/{client.test.ts, react.test.tsx}`.
- **Steps**: add `pub async fn wait_until<F, Fut>(timeout: Duration, mut pred: F) -> bool` (poll every 25 ms); replace each `sleep(N)` that waits for a condition with `assert!(wait_until(...).await)`; keep sleeps that intentionally let a TTL elapse but shorten them where the TTL is configurable. ts: a `waitFor` helper in `tests/helpers.ts`.
- **Method**: After ARC-010 (harness move). Python already has the pattern at `test_presence.py:224` — mirror its shape.
- **Verify**: three consecutive `make checkall` runs green; suite wall-clock does not increase.

### [QA-012] Module-level `allow(dead_code)` in test common
- **Files**: `server/tests/common/mod.rs:98-579`; `rust-client/tests/common/mod.rs:7, 34, 75`.
- **Steps**: replace the per-function attributes with a single `#![allow(dead_code)]` at the top of each file (subsumed for server by ARC-010; still apply to rust-client).
- **Verify**: `make checkall`.

### [QA-013] Hoist in-package exact duplicates
- **Files**: `rust-client/src/admin/mod.rs:1343` + `rust-client/src/http.rs:841` → `rust-client/src/http_common.rs` (`error_response`, `json_result`); `ts-client/src/in_memory/store.ts:546` + `ts-client/src/optimistic.ts:51` → `ts-client/src/canonical.ts`.
- **Verify**: `cargo test --manifest-path rust-client/Cargo.toml`; `bun run --cwd ts-client typecheck && bunx vitest run --root ts-client`; `make checkall`.

### [QA-014] Fix stale TS in-memory header comment
- **Files**: `ts-client/src/in_memory/store.ts:19-23`. Replace the "gaps marked TODO" paragraph with a sentence stating the engine runs every case in `wire-corpus/semantics/`.
- **Verify**: `make ts-client-checkall`.

### [QA-015] `AnySchema` alias
- **Files**: `ts-client/src/schema.ts:625, 627, 630, 635, 650, 660`; `ts-client/src/admin.ts:422, 527, 537`. Add `export type AnySchema = SchemaDefinition<Record<string, TableDefinition>>;` and replace `SchemaDefinition<any>`.
- **Verify**: `bun run --cwd ts-client typecheck`; `make checkall`.

### [QA-016] Delete `_unused_vector_spec`
- **Files**: `server/src/subs.rs:1315`. Delete the function and its `#[allow(dead_code)]`.
- **Verify**: `cargo clippy --manifest-path server/Cargo.toml --all-targets -- -D warnings`.

### [QA-017] Generic storage stream error
- **Files**: `server/src/storage.rs:523, 543`. `tracing::warn!(error = ?e, "storage stream read failed"); std::io::Error::other("storage read failed")`.
- **Verify**: `cargo test --manifest-path server/Cargo.toml --test storage_test`.

### [QA-018] Name inline durations
- **Files**: `server/src/committer/**` (was `committer.rs:775, 812, 1135`), `server/src/rate_limit.rs:179`, `server/src/admin/observability.rs:276`. Add `const DRAIN_DEADLINE`, `DRAIN_POLL`, `RECLAIM_INTERVAL`, `RATE_SWEEP_INTERVAL`, `GAUGE_TICK`.
- **Verify**: `make checkall`.

### [QA-019] CLI integration tests
- **Files**: `cli/tests/live.rs` (new), `cli/Cargo.toml` (`assert_cmd`, `predicates` dev-deps; `[[test]]`), `Dockerfile` stub list (or ARC-011's gate).
- **Steps**: env-gated (`RTDB_TEST_SERVER_URL`, `RTDB_TEST_ADMIN_KEY`) `#[ignore]` tests mirroring `rust-client/tests/http_integration.rs`: `rtdb db create/list/delete`, `push-schema`, `query`, `mutate`. Run locally with a `cargo run` server on :8300 per the memory note.
- **Verify**: `cargo test --manifest-path cli/Cargo.toml -- --ignored` against a live server; `make checkall`; `bash scripts/dockerfile-stub-check.sh`.

---

## Phase 3d — Documentation

### [DOC-011] Demote the 2026-07-21 spec (do first)
- **Files**: `docs/ARCHITECTURE.md:7-11`; `CLAUDE.md` ("Authoritative sources" paragraph); `PRODUCT.md:59`; `docs/superpowers/specs/2026-07-21-par-rt-db-design.md:1-6`.
- **Steps**: add a top banner to the spec ("Historical design record — superseded by README.md, FEATURE_MATRIX.md, docs/ARCHITECTURE.md and wire-corpus/"); rewrite the three references to say the spec is the original design and the README/matrix/corpus are authoritative.
- **Verify**: `grep -rn "2026-07-21-par-rt-db-design" --include=*.md .` — every reference says historical.

### [DOC-002] Fix non-compiling client README examples
- **Files**: `ts-client/README.md:36-38`; `rust-client/README.md:185-187, 321-327, 344`; `rust-client/src/lib.rs:84`; `python-client/README.md:30, 231, 501-503`.
- **Steps**: ts: use the op-tagged form from line 87. rust: `txn: Some(txn)`; `migrate_schema(db, &req.directives, ...)` per `admin/mod.rs:125`, one `dry_run`; add `pub use http::{UploadResult, FileMetadata, SignedUrl};` to `lib.rs` (or change the import to `par_rt_db_client::http::UploadResult`). python: replace `OptimisticStore` with `RtDbClient(optimistic_updates=True)` and `project`; remove the `_id` key; define `client = RtDbHttpClient(...)` and call `client.push_schema("db", schema)`.
- **Verify**: paste each snippet into a scratch `examples/` or doctest and compile (`cargo test --manifest-path rust-client/Cargo.toml --doc` if converted to doctests; `bun run --cwd ts-client typecheck` for a scratch `.ts`; `uv run --directory python-client python -c '...'`).

### [DOC-003] Eight committer arms
- **Files**: `docs/ARCHITECTURE.md:44-59, 143, 614-621`; `CONTRIBUTING.md:259-261`.
- **Steps**: change "seven" to "eight"; add `handle_push_schema` (source `"push"`, `docop_taps=false`, path `committer/arms/schema.rs` after ARC-005) to the diagram and tap list; fix the "third committer request arm" sentence.
- **Verify**: `grep -rn "publish_taps(" server/src | wc -l` equals the documented count; `grep -n "seven" docs/ARCHITECTURE.md CONTRIBUTING.md` returns nothing about arms.

### [DOC-004] Multi-instance section; README promotion; SPEC_STATUS; CLAUDE.md invariant
- **Files**: `docs/ARCHITECTURE.md` (new section + ToC + table diagram: `rtdb_auth.rate_counters`, `forward_queue`); `README.md:1039-1060, 1114`; `CLAUDE.md` invariants; `docs/superpowers/SPEC_STATUS.md`.
- **Steps**: write "Multi-instance coordination" covering the advisory-lock lease, shadow committers → CONFLICT, forwarding (spool table per ARC-002), takeover on timeout, server-minted idempotency (ARC-003), cross-replica op-feed and subscription invalidation (ARC-001), shared rate counters, presence gossip; move README's paragraph to a "Multi-instance" section and delete the roadmap bullet; add the three specs to SPEC_STATUS; add "the lease/shadow model — never bypass `Committers::submit`" to CLAUDE.md's invariant list.
- **Method**: Write after Phase 2 lands so it describes corrected behavior.
- **Verify**: every symbol/path named in the section exists (`find_symbol`); `make checkall` (markdown is not gated, so review by reading).

### [DOC-005] FEATURE_MATRIX §4 and coverage rows
- **Files**: `FEATURE_MATRIX.md:3, 44, 45, 63, 66, 70, 73, 77, 81, 85, 86, 88, 155-158, 179-181, 260, 383`.
- **Steps**: rewrite §4 for multi-instance; for each listed row verify the symbol in all four SDKs (`find_symbol` per package) and write "all four"; replace "53 cases" with "every file in `wire-corpus/semantics/`" (or `ls wire-corpus/semantics | wc -l` = 82 today); recount admin methods or drop the numbers; fix §7 row count; add a #27 stub row; bump the date.
- **Verify**: `ls wire-corpus/semantics | wc -l` matches any stated count; no row says "three clients".

### [DOC-006] Swift in clients.md / docs index / server README
- **Files**: `docs/clients.md:3, 12-16, 32, 40-44, 48, 52`; `docs/README.md:9, 12, 39`; `server/README.md` opening paragraph.
- **Steps**: add a Swift row/column (WS, HTTP, admin, in-memory, optimistic, `ParRtDbUI`); "three" → "four" SDKs, "three engines" → "four", six packages → seven; flip TS optimistic to ✓ (`ts-client/src/optimistic.ts`); link `swift-client/README.md`.
- **Verify**: `grep -n "three" docs/clients.md docs/README.md server/README.md` returns nothing about clients.

### [DOC-007] Presence frame names
- **Files**: `README.md:959-960`. Replace with `presenceState` (fields per `server/src/protocol.rs:94-99`, incl. `ttlMs`) and the `presenceSnapshot`/`presenceErr` server frames.
- **Verify**: names match `protocol.rs` serde tags exactly.

### [DOC-008] Dashboard auth model
- **Files**: `dashboard/README.md:72-73, 83-88, 103`. Rewrite around the HttpOnly `rtdb_session` cookie, `/auth/me` on mount, CSRF header (`dashboard/src/lib/session.tsx:27-43`, `admin.tsx:49,107-110`), and `/admin/stream` op-feed refresh with polling only while the stream is down (`useLiveTable.ts:22-30,95-101`).
- **Verify**: no mention of `Sec-WebSocket-Protocol` or "2s poll" remains.

### [DOC-009] Install instructions
- **Files**: `ts-client/README.md:8-12`; `rust-client/README.md:92`; `python-client/README.md:46-63`; `swift-client/README.md:39-43`; `docs/RELEASING.md`.
- **Steps**: each README: "Not yet published to <registry>. Install from the repo:" with the workspace/path/git form (`"@par-rt-db/client": "file:../ts-client"`, `par-rt-db-client = { git = "https://github.com/paulrobello/par-rt-db", tag = "v0.1.0" }` (verify path/`package` if the crate is a subdir — cargo git deps find workspace members by name), `uv add "par-rt-db @ git+https://github.com/paulrobello/par-rt-db#subdirectory=python-client"`, Swift `.package(url:..., from:)` is not possible without a root `Package.swift` — say "add `swift-client` as a local package" and align RELEASING.md). Optional: `publish = false` in the two Cargo.toml files and `"private": true` in ts-client — a code change; note it in the CHANGELOG if done.
- **Verify**: each install command tested once from a scratch directory.

### [DOC-010] Document the Dockerfile stub-list trap / gate
- **Files**: `deploy/README.md` (Troubleshooting), `CONTRIBUTING.md` (invariants). One bullet each describing the `[[test]]` ⇒ Dockerfile stub rule and the `dockerfile-stub-check` gate from ARC-011.
- **Verify**: read.

### [DOC-012] Canonical gate table
- **Files**: `README.md:218, 984, 993, 1018`; `CLAUDE.md:25`; `CONTRIBUTING.md:79-85, 101`; `server/README.md` Develop block; `ts-client/README.md:537, 542`.
- **Steps**: one table in README listing every `checkall` stage from `Makefile:200` (`env-drift-check cli-docs-check fmt-check lint typecheck test rust-client-check-features`, plus `dockerfile-stub-check` after ARC-011) and what `make test` runs (incl. cli and swift); other docs link to it; "six packages" → "seven".
- **Verify**: `grep -n "checkall" Makefile` stages all appear in the table.

### [DOC-013] CHANGELOG corrections and format
- **Files**: `CHANGELOG.md:29-41, 640, 747` + Unreleased structure.
- **Steps**: rewrite the Stage 4 entry (forwarding shipped in `1136c94`, spool/idempotency/invalidation per this cycle); `clone-db` clones the full snapshot; fix the `admin/mod.rs` link; regroup Unreleased into Added/Changed/Fixed/Security with a "Breaking" subsection.
- **Verify**: read; DOC-001 builds on this structure.

### [DOC-014] Full API sections in client READMEs
- **Files**: four client READMEs. Add a "Full API" table per README covering the surfaces listed in AUDIT.md DOC-014, each row pointing at the symbol/file.
- **Verify**: every symbol named exists (`find_symbol` per package).

### [DOC-015] rust-client `cargo doc` + crate docs
- **Files**: `rust-client/src/admin/mod.rs:166, 501, 578`; `rust-client/src/in_memory/value_expr.rs:20`; `rust-client/src/lib.rs:15-31`; `rust-client/README.md:18, 103-104`; `Makefile` (`rust-client-check-features`).
- **Steps**: fix the four intra-doc links; add `cargo doc --no-deps --all-features --manifest-path rust-client/Cargo.toml` (with `RUSTDOCFLAGS="-D warnings"`) to `rust-client-check-features`; update the `lib.rs` crate docs to list `RtDbAdminClient`, `hybrid_search`, `fields`, `distinct`, `aggregate`, `batch_query`, `upload_stream`, signed URLs; README `src/admin/mod.rs`; strip the `#` markers.
- **Verify**: `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features --manifest-path rust-client/Cargo.toml`; `make checkall`.

### [DOC-016] ts/python docstrings
- **Files**: `ts-client/src/mutation.ts:42` (patch returns `null`); `ts-client/src/{client.ts:245-321, query.ts:26-197, migration.ts:30-67,110, admin.ts:517-570, react.tsx:86,163,269-282}`; `python-client/README.md:67-70`; `python-client/src/par_rt_db/{wire.py:73-88,731, __init__.py:3-5}`.
- **Steps**: fix the patch docstring; add TSDoc to the listed public members; either enable `[tool.ruff.lint.pydocstyle] convention = "google"` + `select = ["D"]` (L, fix violations) or soften the README claim; document `AfterMs/RunAt/Cron/Interval`, `PresenceMember`; "fourth client" → neutral wording.
- **Verify**: `make ts-client-checkall`; `make python-client-lint`.

### [DOC-017] Swift README counts + doc comments
- **Files**: `swift-client/README.md:353, 440, 449, 485`; `swift-client/Sources/ParRtDbClient/{Wire.swift:61-67, Transport.swift:28-99, Errors.swift:33-38}`.
- **Steps**: replace counts with directory references; fix `.cast(value:to:)` prose (`Migrate.swift:56`); add `///` docs to the public transport/wire/error types.
- **Verify**: `make swift-client-checkall` (Darwin).

### [DOC-018] Ops config docs
- **Files**: `deploy/README.md:35-39, 58-61, 274-301, 344-345`; `README.md:403-433`; `server/README.md` HTTP surface section.
- **Steps**: add `RTDB_TRUSTED_PROXY` (code default false, compose true, required behind the tunnel), `RTDB_COOKIE_SECURE`, `DEPLOY_HOST`/`DEPLOY_PATH`, and align the rsync filter with `Makefile:209`; point backup semantics at `config.rs:596-605`/`.env.example:271-272`; add a "Topology & security" row group to the README table (`RTDB_INSTANCE_ID`, `RTDB_MULTI_INSTANCE`, `RTDB_FORWARD_TIMEOUT_MS` (100 ms floor), `RTDB_TRUSTED_PROXY`, `RTDB_COOKIE_SECURE`, `RTDB_POOL_MAX_CONNECTIONS`, `RTDB_ADMIN_EMAILS`, `RTDB_BACKUP_*`); fix the `.env.example` "full list" sentence and the server/README claim.
- **Verify**: every var named exists in `.env.example` (`bash scripts/env-drift-check.sh`).

### [DOC-019] env-drift-check scope
- **Files**: `scripts/env-drift-check.sh:41-45, 61`; `README.md:1010`.
- **Steps**: widen the grep to `(std::env::var|env_parsed|env_bool)\("RTDB_[A-Z0-9_]+"` (verify the helper names in `config.rs:351,371`, and any ARC-012 renames); fix the script comment; keep the README claim.
- **Verify**: `bash scripts/env-drift-check.sh` passes; temporarily add a fake `env_bool("RTDB_FAKE")` and confirm it fails.

### [DOC-020] Broken file references
- **Files/edits**: `README.md:236` → `server/src/static/privacy.html`; `docs/RELEASING.md:43` → `../deploy/README.md`; `deploy/README.md:17` anchor → `#tracing-opentelemetry--otlp-enh-018`; `FEATURE_MATRIX.md:72-73` → `ts-client/src/in_memory/index.ts`, `rust-client/src/in_memory/tests/`; `docs/ARCHITECTURE.md:240, 414`, `PRODUCT.md:61` → `server/src/query/`; `docs/fable/ENH-023-behavioral-semantics-corpus.md:17` (directories); `docs/fable/ENH-025-generated-cli-reference.md:8` (note the kanban repo); `dashboard/README.md:47-48` → root `CLAUDE.md`; add "paths are relative to `server/src/`" to `docs/ARCHITECTURE.md`, `server/README.md:29-33`, `CHANGELOG.md:629, 716`.
- **Verify**: par-mem `find_broken_doc_links(repository_id: "par-rt-db")` after reindex shows none of these; `test -e` each target.

### [DOC-021] server/README scheduling + layout
- **Files**: `server/README.md`. Add `interval` (`everyMs`) to the `when` kinds; SPA serving row → `lib.rs` fallback (`src/static/` holds only `privacy.html`); add a `forward.rs` row.
- **Verify**: read.

### [DOC-022] WS frame-rate close
- **Files**: `README.md:459, 530-560`. Add a paragraph: 200 frames / 10 s closes the socket (`ws.rs:37-38, 351-354`), `MAX_FRAME_BYTES` (64 KB) close, codes 4400/4401 (`ws.rs:56-59`); qualify the `RATE_LIMITED` row.
- **Verify**: constants match `ws.rs`.

### [DOC-023] CONTRIBUTING test-db cleanup
- **Files**: `CONTRIBUTING.md:221-226`. Rewrite: `TestDb` drops on `Drop`; a bounded tail can leak; `make dev-db-clean` sweeps `db_t…` and `sc_…`.
- **Verify**: read against `server/tests/common/mod.rs:420,468`, `scripts/dev-db-clean.sql:20-24`.

### [DOC-024] ARCHITECTURE.md coverage gaps
- **Files**: `docs/ARCHITECTURE.md`. Add a presence section, a wire-corpus paragraph under "Wire contract and clients", the admin CSRF/self-heal note, fix the #20 → #35 cross-ref, add a mermaid sequence diagram for the write path and the forward path, merge single-sentence paragraphs.
- **Verify**: read; mermaid renders (paste into the dashboard's markdown preview or `bunx @mermaid-js/mermaid-cli`).

### [DOC-025] TOC on the four client READMEs
- **Files**: four client READMEs. Add a TOC after the intro per `docs/DOCUMENTATION_STYLE_GUIDE.md`.
- **Verify**: anchors resolve.

### [DOC-026] Internal tracking ids in prose
- **Files**: `ts-client/README.md:44, 48, 56, 66, 150, 324, 395, 476`. Remove FM-/ENH-/SEC- tags or move them to a footnote.
- **Verify**: `grep -n "FM-\|ENH-\|SEC-" ts-client/README.md` empty.

### [DOC-027] wire-corpus/README.md nits
- **Files**: `wire-corpus/README.md:100, 193-194`; `wire-corpus/golden-vector.json:2`. Fix the stray comma; delete the "bump the runner count" sentence; `$comment` → "four in-memory engines / five implementations"; name the python runner file after checking `ls python-client/tests | grep -i corpus`.
- **Verify**: read; corpus tests still pass (JSON edit).

### [DOC-028] python README minor
- **Files**: `python-client/README.md:26, 61, 238, 297, 355, 526`. `src/par_rt_db/...` paths; `[aio]` aliases `[http]`; quote `"par-rt-db[http]"`.
- **Verify**: read against `pyproject.toml:14`.

### [DOC-029] OAUTH_SETUP.md and runbook structure
- **Files**: `docs/OAUTH_SETUP.md`; `deploy/README.md`. Add `RTDB_SESSION_TTL_DAYS`, `RTDB_ANONYMOUS_SESSION_TTL_DAYS`, `RTDB_COOKIE_SECURE`, a "disable a provider" rollback; add a summary sentence after the deploy H1.
- **Verify**: defaults match `config.rs`.

### [DOC-030] Undocumented make targets
- **Files**: `README.md` (or `CONTRIBUTING.md`) make-target table. Add `python-client-fmt/lint/typecheck`, `rust-client-check-features`, `swift-client-*`.
- **Verify**: `make -qp | grep -E '^[a-z-]+:' | sort` vs the table.

### [DOC-031] Go client planned row
- **Files**: `FEATURE_MATRIX.md` §7; `docs/clients.md`. Add a "planned" Go row referencing the board card.
- **Verify**: read.

### [DOC-001] CHANGELOG `[Unreleased]` (write LAST)
- **Files**: `CHANGELOG.md:15-204`.
- **Steps**: `git log --oneline v0.1.0..HEAD`; for each commit listed in AUDIT.md DOC-001 plus every fix landed by this remediation cycle, add an entry under Added/Changed/Fixed/Security (structure from DOC-013). Mark `2609de3` (missing-schema `NOT_FOUND`) and SEC-003 (signed-URL message) and QA-004 (email-link semantics) as behavior changes.
- **Verify**: every commit hash in the range is represented by at least one entry or is a chore; read.
