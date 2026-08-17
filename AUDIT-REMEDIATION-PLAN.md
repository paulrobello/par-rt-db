# Audit Remediation Playbook — 2026-08-16 (Fable 5 cycle)

> Consumed by `/fix-audit`. One entry per AUDIT.md issue, ordered by the Remediation Plan phases.
> Each entry is written to be executed without re-deriving the analysis. Fix agents: re-read the
> current file state before editing (a prior phase may have moved code), and run the named Verify
> command(s) — `make checkall` from the **repo root** is the final gate for every phase.
> par-mem is indexed (`repository_id: repo-8c7ef29abc9a87896deebf1678d4f973`); use `get_impact` /
> `get_symbol_context` before moving any symbol, and remember the index lags your own edits.

---

## Phase 1 — DOC-201 + promoted Security (sequential, in this order)

### [DOC-201] Fix restore-cutover runbook (compose never reads `.env` for `RTDB_DATABASE_URL`)
- **Files**: `deploy/README.md` (cutover step + Troubleshooting bullet), optionally `deploy/docker-compose.yml`
- **Steps**:
  1. Read `deploy/docker-compose.yml` and confirm `RTDB_DATABASE_URL` is hardcoded in the `environment:` block (interpolating only `${POSTGRES_PASSWORD}`) and that no `env_file:` key exists.
  2. Choose the doc-only fix (preferred — this command's constraint is minimal change): rewrite the cutover step in `deploy/README.md` to say: edit the `RTDB_DATABASE_URL:` line in `docker-compose.yml` to point at `rtdb_restored_<stamp>`, then `docker compose up -d --build rtdb` (match the README's existing restart phrasing).
  3. Fix the Troubleshooting bullet the same way ("check the `RTDB_DATABASE_URL` in `docker-compose.yml`'s environment block", not `.env`).
  4. Append a verification sub-step to the cutover: `curl -fsS localhost:8300/healthz` (or the tunnel URL) and one query against a row known to exist only post-restore.
  5. Alternative (only if the fix agent prefers config indirection): change compose to `RTDB_DATABASE_URL: ${RTDB_DATABASE_URL:-postgres://rtdb:${POSTGRES_PASSWORD}@postgres:5432/rtdb}`, add `RTDB_DATABASE_URL` to `.env.example` commented out, and keep the runbook as written. If you take this path, `make env-drift-check` must still pass — run it explicitly.
- **Method**: The README's own env-drift section already states `environment:` is an explicit allowlist; the runbook contradicts it. Doc-only is zero-risk; the compose-indirection alternative touches the deployed contract and needs the env-drift gate. Do not touch the live host — this fixes the *documented* procedure only.
- **Verify**: `grep -n "RTDB_DATABASE_URL" deploy/README.md deploy/docker-compose.yml` shows the runbook referencing compose (or the indirection in place); `make env-drift-check` passes.

### [SEC-201] Trusted-proxy gating for `CF-Connecting-IP` / `X-Forwarded-For`
- **Files**: `server/src/config.rs` (new setting), `server/src/http_api.rs:869-884` (`client_ip_key`), `server/src/auth/cookie.rs:218` (`request_is_secure`), `.env.example`, `deploy/docker-compose.yml` (environment block), plus the call sites that must now pass peer info if they don't already (`server/src/admin/login.rs:46`, `server/src/auth/provider.rs:829`, `server/src/http_api.rs:813`)
- **Steps**:
  1. Add `RTDB_TRUSTED_PROXY` to `Config` (suggested shape: `trusted_proxy: bool`, default `false`; a CIDR list is over-engineering for the current single-tunnel deployment — note this choice in the commit message). Parse in `Config::from_env` next to the other security flags; expose via `HotConfig` **only if** other consumers already read comparable flags hot — otherwise keep it boot-time static like the auth flags (check how `cookie_secure` is handled and mirror it).
  2. In `client_ip_key`, consult `CF-Connecting-IP`/XFF only when `trusted_proxy` is true; otherwise return the socket peer address (`ConnectInfo<SocketAddr>` is already available on axum handlers — check how the function currently receives peer info and thread it if missing).
  3. Apply the same gate to the `X-Forwarded-Proto` read in `request_is_secure` (`auth/cookie.rs:218`). `RTDB_COOKIE_SECURE` defaulting true already covers most of this; the gate is for consistency.
  4. Add `RTDB_TRUSTED_PROXY` to `.env.example` (set `true` with a comment: "true when behind the Cloudflare tunnel/compose loopback bind — leave false if the port is directly reachable") and to `docker-compose.yml`'s environment block set to `"true"` (the shipped deployment IS behind the tunnel).
  5. Add/extend a test in the http_api or auth test files: with `trusted_proxy=false`, a request carrying `CF-Connecting-IP: 1.2.3.4` rate-limits under the peer address, not 1.2.3.4; with `true`, the header wins. Follow the existing rate-limit test patterns (grep tests for `RATE_LIMIT` to find them).
- **Method**: This is a security behavior change on a self-hosted product — default `false` (don't trust headers) is the safe default, and the shipped compose opts in explicitly. New env var ⇒ **both** `.env.example` and `docker-compose.yml` or `make checkall` fails at env-drift-check. Land before ARC-205 (which restructures `from_env`). Flag this change for the user's manual review in the fix report (standing security rule).
- **Verify**: new tests pass; `make env-drift-check` passes; `make checkall` green.

### [SEC-203] Non-zero rate-limit defaults
- **Files**: `server/src/config.rs:462-465, 592`, `.env.example`
- **Steps**:
  1. Change code defaults from 0 to safe non-zero values: `RTDB_ADMIN_RATE_LIMIT_PER_IP_RPM` → 10, `RTDB_ANONYMOUS_RATE_LIMIT_PER_IP_RPM` → 10 (matching what `.env.example` already recommends), `RTDB_STORAGE_RATE_LIMIT_PER_IP_RPM` → 300 (blob serving is high-volume legitimate traffic; bound abuse without breaking galleries). Leave `RTDB_RATE_LIMIT_PER_TOKEN_RPM` / `RTDB_RATE_LIMIT_PER_DB_RPM` at 0 — those bound *authenticated* traffic and a surprise default there can break real apps; instead document them.
  2. Keep `0` as explicit "off" semantics; update the doc comments on the constants to say "0 disables".
  3. Update `.env.example`: set the storage limit to the new default with a comment; ensure admin/anonymous entries match the new code defaults.
  4. Check `docker-compose.yml` — if these vars appear in the environment block, keep values consistent.
  5. Grep the server config tests (`config` tests or `http_api` rate-limit tests) for assertions on the old `0` defaults and update them.
- **Method**: Defaults-on protects the operator who configures by hand; the deliberate carve-out for per-token/per-db limits avoids breaking authenticated workloads. This is a behavior change for bare-env deployments — call it out in the fix report and CHANGELOG. Land before ARC-205 (same file).
- **Verify**: `make checkall` (config tests + env-drift-check).

### [SEC-204] Stop accepting secrets via argv silently
- **Files**: `cli/src/main.rs:34-40`, `cli/README.md` (touch only the flag docs here; DOC-202 does the full README rewrite later)
- **Steps**:
  1. Keep the `--token`/`--admin-key` flags (breaking removal is not warranted) but add `hide_env_values = true` on the clap args if not present, and emit a one-line `eprintln!` warning when the value arrived via argv rather than env (clap exposes this via `ValueSource` — `matches.value_source("token") == Some(ValueSource::CommandLine)`; if main.rs uses derive-only clap without `ArgMatches` access, the simplest correct form is to check `std::env::args()` for the literal flag names before parsing).
  2. Warning text: `warning: --token/--admin-key on the command line is visible in ps and shell history; prefer RTDB_TOKEN / RTDB_ADMIN_KEY`.
  3. In `cli/README.md`, add one sentence under the auth flags documenting the env vars as the primary path (DOC-202 will restructure the section).
  4. Add/extend a CLI test (inline test module) asserting the warning fires on argv and not on env, if the existing test harness can capture stderr; if it can't, note that in the commit.
- **Method**: Warn-don't-break preserves scripts while removing the silent hazard. Land before ARC-206 (which relocates these arg definitions). Security-flagged change — reviewed commit.
- **Verify**: `cargo test` from `cli/` (or `make checkall`); manual: `cargo run -p rtdb-cli -- --token x --url http://127.0.0.1:1 ping 2>&1 | grep -i warning` (any command; the warning must precede the connection error).

---

## Phase 2 — Promoted Architecture (sequential, in this order)

### [ARC-202] Extract pure wire/DSL module; break the `query.rs` ↔ `txn.rs` cycle
- **Files**: `server/src/query.rs`, `server/src/txn.rs`, `server/src/protocol.rs`, new `server/src/dsl.rs` (or `wire.rs` — pick `dsl.rs` to avoid confusion with the clients' wire files), `server/src/lib.rs` (module registration)
- **Steps**:
  1. Enumerate the moving set first with par-mem: `get_symbol_context` on `FilterExpr`, `Query`, `Transaction`, `Step`, `EqBind`; `get_impact` on each to list all callers. The moving set is: the serde-derived DSL/wire types currently in `query.rs` and `txn.rs`, plus the shared value-typing helpers `EqBind`, `eq_bind_for`, `eq_binds`, and the pure predicate `filter_matches` (it is interpretation, not SQL — verify it has no sqlx imports before moving; if it does, move only the types and leave a re-export).
  2. Create `server/src/dsl.rs` containing those types/helpers verbatim — **do not edit any serde attribute, field name, or tag**. Move `row_visible_to` only if it is SQL-free; otherwise leave it in `txn.rs`.
  3. Replace the originals with `pub use crate::dsl::*;`-style re-exports in `query.rs` and `txn.rs` so every existing import path keeps compiling (this keeps the diff reviewable; dropping the re-exports can be a later cleanup).
  4. Point `protocol.rs`'s imports at `dsl` directly.
  5. Confirm the cycle is broken: `query.rs` must no longer `use crate::txn::` for types (only for genuinely executor-level calls, if any remain), and `txn.rs` must not `use crate::query::` for `FilterExpr`/`filter_matches`.
- **Method**: The wire contract is byte-identical serde output — the move must be purely structural. The golden-vector/wire-corpus tests in all four packages are the drift gate; if any corpus test fails, a serde attribute changed — revert and re-diff. No client-side change is needed if they stay green. Re-exports keep the blast radius near zero.
- **Verify**: `cargo test --test golden_vector_test` from `server/`, then full `make checkall` (covers all four packages' corpus tests).

### [ARC-203] Split `server/src/query.rs` into a `query/` directory module
- **Files**: `server/src/query.rs` → `server/src/query/{mod.rs,terminals.rs,filter.rs,search.rs,row_auth.rs}`
- **Steps**:
  1. Run after ARC-202 (the DSL types are gone from the file, shrinking the move).
  2. Mechanical split, no logic edits: `filter.rs` (filter compilation), `terminals.rs` (the eight terminal compilers + `compile_query`/`compile_query_window`), `search.rs` (FTS/vector/hybrid + `SearchCtx`), `row_auth.rs` (per-row auth predicate rendering), `mod.rs` (re-exports preserving the existing `crate::query::` public surface exactly — `pub use` everything that was `pub`).
  3. Keep the three `#[allow(clippy::too_many_arguments)]` where they are — retiring them is ARC-204's opportunistic pattern, not this change.
  4. `cargo fmt` (never bare `rustfmt`) after the move.
- **Method**: Public-surface-preserving split; `mod.rs` re-exports mean zero caller edits. The read/write index-value typing alignment (CLAUDE.md invariant) is untouched because no logic changes. Do not combine with QA-206 (dead-field removal) — that lands after, as its own small commit.
- **Verify**: `make checkall` (clippy `-D warnings` will catch any visibility slip); `git diff --stat` shows only `server/src/query*` and `lib.rs`.

### [ARC-205] Decompose `Config::from_env` per subsystem
- **Files**: `server/src/config.rs:376` (+ optionally colocated structs)
- **Steps**:
  1. Run after SEC-201/SEC-203 (both add parsing to this function).
  2. Group parsing into per-subsystem constructors, top candidates: one `from_env` per OAuth provider config struct (six providers — the biggest win), `TransformConfig::from_env`, `PresenceConfig::from_env`, a `RateLimits::from_env` (the five `*_RPM` vars incl. SEC-203's new defaults), storage/quota clusters.
  3. `Config::from_env` becomes composition: parse the flat scalars it truly owns, then call each subsystem constructor.
  4. Do not rename any env var, change any default, or alter validation messages — this is pure motion. The SEC-110 admin-key validation block moves intact.
  5. Keep every constant (`DEFAULT_*`) adjacent to its new parser.
- **Method**: Highest-churn function in the repo — the value is that future features edit a subsystem parser, not the monolith. Existing config tests pin behavior; if coverage looks thin for a moved cluster, add one round-trip test per subsystem constructor (set env → assert struct) using the existing test style. Env-drift-check is unaffected (var names unchanged).
- **Verify**: `make checkall`; `cargo test` config tests pass unchanged.

### [ARC-201] Decompose the three in-memory engines
- **Files**: `ts-client/src/in_memory.ts` (3,865 lines), `python-client/src/par_rt_db/in_memory.py` (4,120 lines), `rust-client/src/in_memory/query.rs` (`run_query` CC 119), `rust-client/src/in_memory/migrate.rs` (`apply_migration_directive` CC 56)
- **Steps** (three per-client sub-tasks; run them as separate sub-agents or sequential commits — one client per commit):
  1. **TS**: split `in_memory.ts` into `src/in_memory/{index.ts,query.ts,migrate.ts,validate.ts,store.ts}` mirroring the rust-client layout; preserve the public export surface via `index.ts` (check `src/index.ts` / package exports for what is public). Then decompose `executeQuery` (CC 105) into per-terminal executors following the pattern already present (`executeGetTerminal`, `executeSearchTerminal`), and `applyMigrationDirective` (CC 42) into one function per directive kind with a dispatcher.
  2. **Python**: split `in_memory.py` into a `par_rt_db/in_memory/` package (`__init__.py` re-exporting `InMemoryRtDbClient`, `query.py`, `migrate.py`, `validate.py`, `store.py`); decompose `run_query` (CC 112) per terminal, same dispatcher shape as TS.
  3. **Rust**: the module split already exists; decompose `run_query` (CC 119) per terminal and `apply_migration_directive` (CC 56) per directive kind, keeping `in_memory/tests.rs` untouched (QA-205 splits it later).
  4. Keep the three decompositions **structurally identical** (same function-per-terminal names modulo language casing) — the mirroring is the point; a reader diffing TS against Python should see the same shape.
  5. After each client: run that client's full test suite AND the golden-vector/wire-corpus test before starting the next.
- **Method**: This is the repo's top churn×complexity hotspot; the refactor is mechanical extraction with zero behavior change, gated by the strongest test net in the repo (golden-vector + each client's in-memory suites, which are large: rust 6,088 test lines). Do NOT run concurrently with any wire/DSL change (ARC-202 must be complete and committed first — it touches server only, but a protocol change mid-refactor would land into moving code). Per-client par-mem queries: `find_symbol executeQuery` / `run_query` scoped to the client, `get_impact` to confirm the only callers are the client's own public API and tests.
- **Verify**: per client — `cd ts-client && bunx vitest run` + `bun run typecheck`; `cd python-client && uv run pytest -q`; `cargo test -p par-rt-db-client`; final `make checkall` at root. Golden-vector parity tests must pass in all four packages.

---

## Phase 3a — Security (remaining, parallel-safe)

### [SEC-202] Accept RUSTSEC-2023-0071 in `audit.toml`
- **Files**: new `audit.toml` at the workspace root (or `.cargo/audit.toml` — check `cargo audit` docs for the path it reads; workspace root `audit.toml` is standard)
- **Steps**:
  1. Create `audit.toml` with `[advisories] ignore = ["RUSTSEC-2023-0071"]` and a comment: rsa 0.9.x Marvin timing side-channel; reached only via jsonwebtoken RSA id_token *verification* (public-key ops) — the vulnerable private-key path is not exercised; no fixed release exists; re-evaluate when jsonwebtoken adopts rsa 0.10.
  2. If CI runs `cargo audit`, confirm it picks the file up (run `cargo audit` locally).
- **Method**: Documented acceptance beats a perpetually-red advisory that trains people to ignore audit output.
- **Verify**: `cargo audit` exits 0 from the workspace root.

### [SEC-205] Log OAuth token-exchange failures by key names only
- **Files**: `server/src/auth/provider.rs:132` (`extract_access_token`)
- **Steps**:
  1. Replace `tracing::warn!(response = ?value, ...)` with the Apple pattern (`apple.rs:144`): collect `value.as_object().map(|o| o.keys().collect::<Vec<_>>())` and log `present_keys = ?keys` plus the provider name.
  2. Check the same function (and nearby error paths) for any other full-body logging of provider responses; apply the same treatment.
- **Method**: A failed exchange can still carry `id_token`/`refresh_token` fragments; key names preserve debuggability without landing credentials in logs.
- **Verify**: `make checkall`; `grep -n "response = ?" server/src/auth/provider.rs` returns nothing.

### [SEC-206] `random_token`: `thread_rng` → `OsRng` (own commit, security-flagged)
- **Files**: `server/src/db.rs:663-667`
- **Steps**:
  1. Replace `rand::thread_rng()` with `rand::rngs::OsRng` in `random_token` (match the existing usage in `webhook.rs:174` for idiom: `OsRng.fill_bytes(&mut buf)` or the `Rng` trait call the file already uses).
  2. Nothing else in the commit. Commit message notes: standardizes security-token minting on the OS CSPRNG; no format/length change; existing sessions unaffected (tokens are random bytes, not derived).
- **Method**: `get_impact` on `random_token` confirms consumers (session tokens, machine tokens, OAuth state, CSRF nonces) — all agnostic to the RNG source. This does NOT regenerate or invalidate any existing token (standing rule: never auto-replace live secrets). Flag for manual review in the fix report.
- **Verify**: `make checkall`; auth/session integration tests pass.

## Phase 3b — Architecture (remaining, parallel-safe)

### [ARC-204] `CommitterConfig` parameter object
- **Files**: `server/src/committer.rs:176` (`Committers::new`), its construction site in `server/src/lib.rs`
- **Steps**:
  1. Define `pub struct CommitterConfig` in `committer.rs` holding the eight config scalars (`audit_log_enabled`, `webhooks_enabled`, `ttl_sweep_interval_secs`, `ttl_batch`, `quota_cache_ttl_secs`, `idle_reclaim_secs`, `instance_id`, `multi_instance`).
  2. Add a `CommitterConfig::from_config(&Config)` constructor (one place derives it); change `Committers::new` to `(pool, subs, schemas, op_feed, hot, metrics, quotas, cfg: CommitterConfig)`.
  3. Update the construction site(s) — `get_impact` on `Committers::new` to enumerate (expect one real call site plus tests).
  4. Remove the `#[allow(clippy::too_many_arguments)]` at :176. Leave the other ~10 allow sites alone (opportunistic-only per the audit).
- **Method**: Struct fields are named, killing positional-transposition risk. Do not fold the six non-config handles (pool/subs/etc.) into the struct — they are dependencies, not config.
- **Verify**: `make checkall`; the allow at committer.rs:176 is gone.

### [ARC-206] Split `cli/src/main.rs` into modules
- **Files**: `cli/src/main.rs` → `cli/src/{main.rs,args.rs,output.rs,commands/mod.rs,commands/*.rs}`
- **Steps**:
  1. Run after SEC-204 (its warning code moves with the arg definitions).
  2. `args.rs`: the clap derives (global flags + `Command` enum). `commands/`: one file per command family (data, admin, tokens, sessions, backup — group by the enum's natural clusters, ~5 files). `output.rs`: the formatting helpers. `main.rs` shrinks to parse → dispatch.
  3. Move the inline tests to the module they test (or a `tests` submodule per file).
  4. Wire-type note (from vault): CLI wire structs need both `Serialize` and `Deserialize`; don't touch derives while moving.
- **Method**: Pure motion, no behavior change; `cargo clippy -D warnings` catches visibility slips. Keep each command's handler byte-identical.
- **Verify**: `cargo test` from `cli/`; `make checkall`; `cargo run -p rtdb-cli -- --help` output identical to before the split (capture before/after and diff).

### [ARC-207] Fold loose `AppState` fields into substructs
- **Files**: `server/src/lib.rs:126-161` and every `state.<field>` consumer
- **Steps**:
  1. `get_impact` on `AppState` field accesses first — this touches many handlers; enumerate before moving.
  2. Fold `rate_limiter`, `image`, `quotas`, `signed_url_key` into a new `Limits` (or extend `Runtime`) substruct; `backup_running` and `instance_id` into `Runtime`. Mechanical rename of access paths (`state.quotas` → `state.limits.quotas`).
  3. Keep it to one substruct decision — don't redesign `AppState`.
- **Method**: Low priority; if the diff exceeds ~200 lines of mechanical renames, it is still fine (rename-only), but do not batch with any behavioral change.
- **Verify**: `make checkall`.

### [ARC-208] Add `rust-client-check-features` to `checkall`
- **Files**: `Makefile:137` (the `checkall` target)
- **Steps**: append `rust-client-check-features` to `checkall`'s dependency list (match how the other sub-targets are listed); confirm the target name via `grep -n "rust-client-check-features" Makefile .github/workflows/ci.yml`.
- **Method**: Closes the local-vs-CI gate gap; the target is a fast `cargo check` loop.
- **Verify**: `make checkall` runs the feature checks (visible in output) and passes.

### [ARC-209] Dev-mode export condition for `@par-rt-db/client`
- **Files**: `ts-client/package.json` (exports map), possibly `dashboard/vite.config.ts` / `dashboard/tsconfig.json`
- **Steps**:
  1. Add a `development` condition to ts-client's `exports` pointing at `./src/index.ts` (keep `types`/`default` → `dist` as-is). Vite resolves the `development` condition in dev/serve mode automatically.
  2. Verify dashboard dev-serve works without `ts-client/dist` present: `rm -rf ts-client/dist && cd dashboard && bun run dev` (smoke: page loads). Then restore: `make ts-client-build`.
  3. Verify the *build/typecheck* path still uses dist (tsc resolves `types`, not `development`) — `make checkall` after a fresh `ts-client-build`.
  4. If tsc or vitest resolution breaks (the condition leaking into typecheck), abandon the exports change and instead document the constraint — do not force it.
- **Method**: Fixes the fresh-worktree gotcha at the resolver level, but resolution conditions are subtle — the escape hatch in step 4 is real: this is a Low finding, not worth fighting the toolchain.
- **Verify**: both step 2 (dev without dist) and step 3 (full `make checkall`) pass.

### [ARC-210] Rerun-ratio observability threshold
- **Files**: `deploy/README.md` (monitoring section); optionally `server/src/subs.rs` (only if a ratio metric is genuinely missing)
- **Steps**:
  1. Confirm the metric names in `server/src/metrics.rs` / `subs.rs` (`rtdb_subs_skips_total`, `rtdb_subs_reruns_total` per the audit; verify by grep).
  2. Add a short "Monitoring the invalidation canary" subsection to deploy/README.md: the PromQL ratio expression (`rate(rtdb_subs_reruns_total[5m]) / (rate(rtdb_subs_reruns_total[5m]) + rate(rtdb_subs_skips_total[5m]))`), what a high ratio means (table-level re-runs dominating — a db heavy in distinct/aggregate/search subs throttling its writes), and a suggested alert threshold (e.g. sustained > 0.5).
  3. Server-side code change only if the counters lack a per-db label needed for the ratio — check first; the audit believed they exist. If a code change is needed, keep cardinality bounded (the repo deliberately avoids per-db labels on the open `/metrics` — respect that: aggregate ratio only, no new per-db labels).
- **Method**: This is observability documentation for an accepted design, not a code fix. The related capacity work is enhancement ENH-024 — do not implement dashboards here.
- **Verify**: `make checkall` (if code touched); doc renders correctly.

## Phase 3c — Code Quality (parallel-safe)

### [QA-201] Extract dashboard god components
- **Files**: `dashboard/src/pages/WebhooksPage.tsx:60`, `dashboard/src/pages/SchemaHistoryPage.tsx:47`, new components under `dashboard/src/components/` (follow the existing components directory convention — check where e.g. table/form components live)
- **Steps**:
  1. WebhooksPage: extract `WebhookCreateForm` (create-form state), `WebhookEditPanel` (inline-edit state), `DeliveriesPanel` (drill-down state) as child components owning their own `useState`; the page keeps list state + selection.
  2. SchemaHistoryPage: same treatment (likely a history table + a restore/preview panel).
  3. Use the existing `useLiveTable`/`useAsync`-style hooks where they fit (coordinate with QA-202 — if QA-202 chose "adopt", these pages are candidates; read `useAsync.ts`'s current state before assuming it exists).
  4. Add a component test per extracted component if the dashboard test harness supports it cheaply (these pages currently have none — even a render smoke test is a win). Follow the existing ~12 component tests' patterns.
- **Method**: State-cluster extraction, no behavior change; each extracted component takes its data + callbacks as props. Visual verification: `make checkall` covers typecheck + existing tests; a dev-server smoke check of both pages is worth 2 minutes if the fix agent can run one.
- **Verify**: `make checkall`; new component tests pass.

### [QA-202] Adopt or delete `useAsync`
- **Files**: `dashboard/src/lib/useAsync.ts`, simple list pages if adopting
- **Steps**:
  1. Decision: **adopt** (preferred — the hook's docstring says it exists for exactly this, and QA-201 wants it) in 2–3 simple list pages (Sessions, Webhooks list-load, SlowQueries — pick pages whose load pattern matches its contract exactly); otherwise delete the file and its `biome-ignore`.
  2. If adopting: convert each page's inline `useEffect`+`try/catch`+`setX` load to `useAsync`, behavior-identical (loading/error states preserved).
  3. If any page's pattern doesn't fit cleanly, don't force it — partial adoption (≥1 real consumer) already resolves "dead code".
- **Method**: The finding is limbo, not the hook's quality. One real consumer or zero — either ends the limbo.
- **Verify**: `make checkall`; `grep -rn "useAsync" dashboard/src --include="*.tsx" --include="*.ts"` shows ≥1 non-definition hit (adopt) or none incl. the file (delete).

### [QA-203] Type `_sched_op`/`_wf_op`; drop stale type-ignores
- **Files**: `python-client/src/par_rt_db/ws_client.py:629-665`, `python-client/pyproject.toml`
- **Steps**:
  1. Read the two helpers; type them with `@overload` signatures keyed by the op-name literal (or a `TypeVar` + per-call annotation if the return shapes don't map cleanly from the op name — read the actual return-shape variety first and pick the lighter mechanism).
  2. Delete the nine `# type: ignore[return-value]` comments.
  3. In `pyproject.toml`, enable `reportUnnecessaryTypeIgnoreComment` (the config comment says this is what's deferred); fix anything it newly surfaces (~10 expected, per the config's own note).
  4. Mirror-safety: this touches types only, no wire behavior — no client-mirror propagation needed.
- **Method**: The pyright config already documents the intent; this executes it. Keep the sync/async twin surfaces consistent (check whether `client.py`/sync mirrors the same helpers).
- **Verify**: `cd python-client && uv run pyright && uv run pytest -q`; `make checkall`.

### [QA-204] Move `admin.rs` inline tests out
- **Files**: `rust-client/src/admin.rs` (3,844 lines) → `rust-client/src/admin/` (`mod.rs` + `tests.rs`) or `#[path]`-included `admin_tests.rs`
- **Steps**: convert `admin.rs` to `admin/mod.rs` (implementation, ~1,300 lines) + `admin/tests.rs` (`#[cfg(test)] mod tests` content, wiremock suites), mirroring the `in_memory/` layout. Pure motion.
- **Verify**: `cargo test -p par-rt-db-client admin`; `make checkall`.

### [QA-205] Split `in_memory/tests.rs` by feature area — **after ARC-201**
- **Files**: `rust-client/src/in_memory/tests.rs` (6,088 lines)
- **Steps**: split into `tests/` submodule files by feature area (query terminals, filters, migrate, ttl/reaper, workflows, storage — derive the clusters from the test names). Test count before == after (`cargo test -p par-rt-db-client in_memory -- --list | wc -l` before and after).
- **Verify**: identical test count; `make checkall`.

### [QA-206] Drop dead `SearchCtx` fields — **after ARC-202/ARC-203**
- **Files**: post-split `server/src/query/search.rs` (pre-split: `server/src/query.rs:2636-2643`)
- **Steps**: delete `SearchCtx.db` and `SearchCtx.table_name`, their initializers, and the `#[allow(dead_code)]`; clippy confirms nothing read them.
- **Verify**: `make checkall`.

## Phase 3d — Documentation (parallel-safe; DOC-207 before DOC-206; DOC-202 after SEC-204/ARC-206)

> Shared method for every DOC entry: verify each claim against code *at fix time* (files may have
> moved in Phases 1–2 — especially anything referencing `query.rs` paths or CLI flags), follow
> `docs/DOCUMENTATION_STYLE_GUIDE.md` (one H1, language-tagged fences, no brittle line numbers,
> no duplicated manifest versions), and keep diffs surgical. Verify = re-read the changed section
> against the named source files; `make checkall` still gates (docs changes can't break it, but
> run it once at the end of the phase).

### [DOC-202] Rewrite CLI README
- **Files**: `cli/README.md`; source of truth `cli/src/main.rs` (post-ARC-206: `cli/src/args.rs`)
- **Steps**: (1) Fix the `--url` claim: required, env fallback `RTDB_URL`, no default. (2) Add a Configuration section documenting `RTDB_URL`, `RTDB_DB`, `RTDB_TOKEN`, `RTDB_ADMIN_KEY` (+ the SEC-204 argv warning). (3) Document all sixteen commands — enumerate from the `Command` enum, one subsection or table row each, including `sessions list|revoke`, `merge-users`, `clone-db`, `explain`, `slow-queries`, `mint-token`, `revoke-token`. (4) Generate the command list from `cargo run -p rtdb-cli -- --help` output to guarantee accuracy (paste as a fenced block; full generation tooling is ENH-025, not this fix).
- **Verify**: every documented flag/command exists in `--help` output; every `--help` command appears in the README (manual diff of the two lists).

### [DOC-203] Seven-arm tap invariant in CONTRIBUTING
- **Files**: `CONTRIBUTING.md`; truth: `grep -n "publish_taps" server/src/committer.rs` (expect 7 call sites)
- **Steps**: replace the stale enumeration with the seven-arm list (mutate, scheduled, migrate, reaper, restore_schema, merge_users, workflow_advance) **and** a pointer to `docs/ARCHITECTURE.md` as the canonical list, phrased so the doc survives arm #8 ("every `handle_*` arm calling `publish_taps` — currently seven; see ARCHITECTURE.md").
- **Verify**: the grep count matches the documented count.

### [DOC-204] Deploy README: rollback + env reference + coverage
- **Files**: `deploy/README.md`
- **Steps**: (1) Real rollback procedure: `git checkout <prior-commit>` → `make deploy` (re-rsync + rebuild on lenny2) → verify `/healthz` + spot query; note that there are no image tags so rollback = redeploy-older-commit. (2) Link `.env.example` as the canonical env reference in Secrets. (3) Add short pointers: slow-query log (`RTDB_SLOW_QUERY_*`, the `_LOG_PARAMS` privacy tradeoff), anonymous auth (`RTDB_AUTH_ANONYMOUS_ENABLED` + per-db toggle), `RTDB_INSTANCE_ID` under multi-instance, link `docs/OAUTH_SETUP.md` from the OAuth credentials part. (4) Refresh the ToC (add Topology, Tracing). (5) Rephrase the "standing canary at 200" claim as a recommendation. Coordinate with DOC-201/DOC-208 edits to the same file — one agent should own all deploy/README.md changes this cycle.
- **Verify**: every named env var exists in `.env.example`; ToC anchors resolve.

### [DOC-205] Fix ts-client README APIs
- **Files**: `ts-client/README.md`; truth: `src/schema.ts`, `src/admin.ts`, `src/http.ts`
- **Steps**: (1) `.ttl({field:"expiresAt"})` → `.ttl("expiresAt")` (mention the optional `defaultDurationMs` second arg). (2) `revokeSessionsForUser` → `revokeUserSessions(userId)`. (3) Workflow section: `http.startWorkflow` returns `{ id: string }` (reactive client returns `WorkflowInfo`); bind every variable in the snippet. (4) Add `transformUrl` and `batchQuery` to listings. (5) Compile-check each snippet mentally against the actual signatures — or paste them into a scratch `.ts` file in `ts-client` and run `bunx tsc --noEmit` on it, then delete it.
- **Verify**: each corrected name greps to a real export in `ts-client/src/`.

### [DOC-206] Refresh SPEC_STATUS.md — **after DOC-207**
- **Files**: `docs/superpowers/SPEC_STATUS.md`; truth: `ls docs/superpowers/specs/` + `FEATURE_MATRIX.md`
- **Steps**: (1) Add rows for the seven missing specs (anon-merge, step-schedule, phrase-search-snippets, trgm-search, workflows, cascade-delete-soft-delete, field-defaults) with statuses from FEATURE_MATRIX. (2) Fix the "#1–#26" legend references (now #35). (3) Reconcile the dashboard row with the post-DOC-207 FEATURE_MATRIX row 18. (4) Cross-check total row count == spec file count (40).
- **Verify**: `ls docs/superpowers/specs/*.md | wc -l` equals the table's row count.

### [DOC-207] Fix FEATURE_MATRIX rows 11 & 18
- **Files**: `FEATURE_MATRIX.md`; truth: `grep -n "websearch_to_tsquery\|plainto_tsquery" server/src/query.rs` (post-split: `server/src/query/search.rs`)
- **Steps**: (1) Row 11: `plainto_tsquery` → `websearch_to_tsquery`, add "(see row 31/FM-31)". (2) Row 18: "In progress —" prefix → "Implemented —".
- **Verify**: the grep shows only `websearch_to_tsquery` in the search compile path; rows no longer contradict each other.

### [DOC-208] Correct verify-skip default in two docs
- **Files**: `deploy/README.md`, `CHANGELOG.md`; truth: `DEFAULT_SUBS_VERIFY_SKIP_EVERY` in `server/src/config.rs`
- **Steps**: (1) deploy/README: "(default 0 = off)" → "ships enabled at 1000; set 0 to disable"; adjust the "enable it" phrasing. (2) CHANGELOG: fix the entry and append a one-line note that the shipped default is 1000 (don't silently rewrite the historical entry's meaning).
- **Verify**: `grep -rn "VERIFY_SKIP" deploy/README.md CHANGELOG.md .env.example server/src/config.rs` all agree on 1000.

### [DOC-209] Fix rust-client README
- **Files**: `rust-client/README.md`; truth: `rust-client/src/`
- **Steps**: (1) `src/in_memory.rs` → `src/in_memory/`. (2) Rewrite the migration example via `db.admin_client().migrate_schema(...)` (compile the snippet shape against `admin.rs`'s actual signature). (3) Add `transform_url`, `upload_stream`, `batch_query` to the method listings.
- **Verify**: `cargo doc -p par-rt-db-client --no-deps` builds; no `#[deprecated]` API appears in examples.

### [DOC-210] Fix server README
- **Files**: `server/README.md`
- **Steps**: (1) Prefix the make commands with "from the repo root" (or use `make -C ..`). (2) Add `db.rs`, `notify.rs`, `privacy.rs`, `static/` to the layout table (post-Phase-2 layout: also reflect `query/` and `dsl.rs` if landed). (3) Add `GET /privacy`, `GET /metrics`, `POST /api/query-batch` to the route list. (4) Replace the 8-of-46 test enumeration with a pattern description ("one integration binary per feature area under `server/tests/`; run one with `cargo test --test <name>`"). (5) Link the root README Configuration section instead of name-dropping env vars.
- **Verify**: layout table matches `ls server/src/`; routes match the router in `lib.rs`.

### [DOC-211] SDK doc-comments
- **Files**: `rust-client/src/lib.rs`, `rust-client/src/http.rs`, `ts-client/src/schema.ts`, `ts-client/src/react.tsx`, `ts-client/src/client.ts`
- **Steps**: (1) Add `#![warn(missing_docs)]` to `rust-client/src/lib.rs`; document every warning it surfaces in `http.rs` (`RtDbHttpClient`, `new`, sessions/merge/file methods) and elsewhere — content from the method bodies + README, one or two sentences each, `# Errors` sections where the README documents failure modes. Do NOT use `deny` (warn keeps the gate at clippy's `-D warnings` level — confirm whether the workspace lints promote warns to deny; if they do, the crate must be warning-clean before commit, which is the point). (2) ts-client: JSDoc on `defineTable`, `defineSchema`, `toInt64`, `fromInt64`, `useQuery`, `useMutation`, `useRtDbAuth`, `RtDbProvider`, and the `client.ts` storage methods.
- **Verify**: `cargo doc -p par-rt-db-client --no-deps` with no missing_docs warnings; `make checkall`.

### [DOC-212] Fix python-client README
- **Files**: `python-client/README.md`
- **Steps**: (1) Replace the pydantic/httpx version pins with "see `pyproject.toml` for supported versions" (per the style guide's Version References rule). (2) Fix the workflows snippet: bind `db` (mirror the README's earlier connect example).
- **Verify**: no version numbers duplicated from pyproject in the README; snippet variables all bound.

### [DOC-213] docs/README counts
- **Files**: `docs/README.md`
- **Steps**: drop the "(33 files)"/"(58 files)" counts entirely (style guide calls counts brittle — this finding is the proof).
- **Verify**: no file counts remain in the index.

### [DOC-214] Module-level docs for `auth/`
- **Files**: `server/src/auth/*.rs` (10 files), `server/src/admin/mod.rs`, `server/src/main.rs`
- **Steps**: add a 2–5 line `//!` header per file: what the module owns, its main entry points, and the SEC-ledger invariants it enforces (e.g. cookie.rs: attribute-injection guards fail closed; provider.rs: state single-use + CSRF double-submit). Source the content from ARCHITECTURE.md's auth section — do not invent new claims.
- **Verify**: `grep -L "^//!" server/src/auth/*.rs` returns nothing; `make checkall`.

### [DOC-215] OAUTH_SETUP closing section
- **Files**: `docs/OAUTH_SETUP.md`
- **Steps**: (1) Reword the closing "Adding a new provider (Microsoft, Apple, …)" — those two shipped; use a hypothetical name. (2) Add a cross-reference to the anonymous-auth flow and anon→real merge where the `/begin` flow is described.
- **Verify**: read-through; no shipped provider named as "future".

### [DOC-216] Root README minor omissions
- **Files**: `README.md`
- **Steps**: (1) Add `GET /privacy` to the endpoints table (verify against the router in `lib.rs`). (2) Add a one-line prerequisites pointer to CONTRIBUTING's list near the quickstart.
- **Verify**: endpoints table matches the router.

### [DOC-217] Changelog versioning note
- **Files**: `CHANGELOG.md`
- **Steps**: this cycle, only add a short note under `[Unreleased]` acknowledging no release has been tagged yet; the actual v0.1.0 cut is enhancement ENH-026 (do not tag anything here).
- **Verify**: note present; no tags created.

### [DOC-218] Mermaid diagrams for ARCHITECTURE.md
- **Files**: `docs/ARCHITECTURE.md`
- **Steps**: add two Mermaid diagrams sourced strictly from the existing prose: (1) a component diagram — per-db committer, the seven `handle_*` arms, `publish_taps` fan-out to op-feed/audit/webhooks, and the background tasks (scheduler/reaper/workflows) enqueueing back through committer arms; (2) the side-table schema map the prose describes. Model the style on the root README's sequence diagram. Use the dark-mode palette from the user's global CLAUDE.md if styling is applied.
- **Verify**: diagrams render (GitHub/mermaid preview); every box/edge corresponds to a prose claim.

### [DOC-219] Python docstring convention
- **Files**: `python-client/src/par_rt_db/` (16 files)
- **Steps**: (1) Add the convention choice (Google style) to `python-client/README.md` or a CONTRIBUTING note. (2) Convert the public entry-point modules only this cycle (`client.py`/`ws_client.py` public methods): reshape existing prose into `Args:`/`Returns:`/`Raises:` — content-preserving, no new claims. Full rollout is incremental by design.
- **Verify**: `uv run ruff check` passes (enable pydocstyle rules only if the diff stays surgical — otherwise leave lint config alone and note it).

---

## Post-phase wrap-up (for `/fix-audit`)

1. Final gate: `make checkall` from the repo root (remember: dev Postgres must be up — `make dev-db-up`; ts-client dist must be built).
2. `make dev-db-clean` if test runs leaked schemas.
3. Close the board cards tagged `audit-2026-08-16` for issues actually fixed and verified — per-criterion, never blanket.
4. Security-flagged commits requiring the user's manual review: SEC-201, SEC-203, SEC-204, SEC-206.
5. CHANGELOG entries for behavior changes: SEC-201 (new env var), SEC-203 (default changes), SEC-206 (RNG source).
