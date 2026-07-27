# Client Sweep — Item E: rust-client In-Memory Test Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port `ts-client/src/in_memory.ts` (the `InMemoryRtDbClient` test harness, 1,391 lines) to a new `rust-client/src/in_memory.rs`, giving the Rust client the same server-faithful, no-process-needed test harness the TS client has — schema push, insert/patch/replace/delete/upsert, the full query executor, the **correct** recursive `FilterExpr` evaluator (ported from C's fix), cursor-keyset pagination, the scheduler harness, and honest `[]` stubs for search/vector/storage.

**Architecture:** A pure port — `in_memory.rs` consumes types that **already exist** in rust-client (`crate::wire::FilterExpr`, `crate::query::Query`, `crate::cursor`, `crate::mutation::{Transaction,StepResult,Mutation}`, `crate::schema::SchemaDef`, `serde_json::Value` for docs). **No new wire surface, no protocol changes.** The harness holds `HashMap<(table, id), StoredRow>` + a schema map + a schedule list + subscriber callbacks in memory; `mutate`/`run`/`tick` operate on it directly. `validate_filter`/`eval_filter_expr` mirror `ts-client/src/in_memory.ts:361-488` (the C-corrected logic: value-kind domain for `in`, `null`/absent → no match, 5 BAD_REQUEST cases), evaluated against `FilterExpr` from `wire.rs`.

**Tech Stack:** Rust (edition 2024), serde_json, `#![deny(warnings)]` + clippy `-D warnings`. Run cargo from `rust-client/`. Unit tests are pure (no server, no Postgres) — the harness IS the test fixture.

## Global Constraints

- **No wire/protocol changes** — `protocol.rs`/server untouched; `wire.rs`/`query.rs`/`mutation.rs`/`schema.rs`/`cursor.rs` are consumed read-only. The ONLY new file is `rust-client/src/in_memory.rs` (+ its `#[cfg(test)] mod tests`), and a `pub mod in_memory;` + re-export line in `lib.rs`. No new types in `wire.rs`.
- **Reuse, don't redefine**: `FilterExpr` from `crate::wire` (variants `Eq`/`Neq`/`Gt`/`Gte`/`Lt`/`Lte`/`In`/`And`/`Or`/`Not` — confirm the exact variant names at `wire.rs:316` and match them); `Query`/`Order`/`Paginated`/`Paginate` from `crate::query`; `encode_cursor`/`decode_cursor` from `crate::cursor`; `Transaction`/`StepResult`/`Mutation` from `crate::mutation`; `SchemaDef`/`TableDef`/field types from `crate::schema`; docs as `serde_json::Value` (matches `optimistic.rs`).
- **Faithful to the TS source** — `in_memory.rs` mirrors `in_memory.ts` method-for-method and helper-for-helper. When the TS and a Rust idiom diverge (e.g. `Record<string,unknown>` → `serde_json::Value`; `structuredClone` → `serde_json::Value` deep clone via round-trip or `clone()`), pick the Rust idiom that preserves behavior, and note it. The reference for *behavior* is `in_memory.ts` + the server (`server/src/query.rs`, `txn.rs`, `schema.rs`); the reference for *types* is rust-client's existing modules.
- **Filter is the C-corrected logic** — port `validateFilter` + `evalFilterExpr` + their leaf helpers (`checkLeafValue`, `inValueKind`, `compareLeaf`, `compareValues`, `docToText`, `docToNumber`) verbatim from `in_memory.ts:361-488` and `:423-475`. The 5 BAD_REQUEST cases (unknown field, wrong value-kind for the op, `in` with mixed-type values, etc.) must all surface. This is the bug C fixed; E must not regress it.
- **Stubs are honest**: `search`, `vector_search`, `upload`, `delete_file`, `get_file_metadata`, `get_url` return empty/stub results with the same shapes the TS harness returns (`in_memory.ts` search/vector → `[]`; storage → an in-memory `HashMap<id, bytes>` like the TS harness at `:157-191`). Do NOT implement real text/vector ranking (design doc: out of scope, low test-harness value).
- **MAX_STEPS = 256, MAX_TAKE = 4096, CRON_STEP_MS = 60_000** — match `in_memory.ts:40-41,89` exactly.
- **`#![deny(warnings)]` + clippy `-D warnings`**; no `unwrap()`/`expect()` outside `#[cfg(test)]`. Harness methods that can fail (validation, cursor decode) return `Result<_, RtDbError>`; pure helpers that mirror TS `throw` paths return `Result` or panic-via-test-only.
- **Reference tests**: port `ts-client/tests/in_memory.test.ts` (756 lines, 50 cases, 10 describe blocks) to `#[cfg(test)] mod tests` inside `in_memory.rs`. One Rust test per TS `it()`; keep the same fixture shapes and assertions. The TS tests are the spec for harness behavior.
- **Verification**: each task runs `cd rust-client && cargo test in_memory` (+ clippy + fmt before commit). Full `make checkall` at branch finish (Task 6).
- **Re-Read before editing (R9)**: `in_memory.ts` and the rust-client modules' line numbers drift; always read the current file when porting a region.
- **Model routing**: implementer = Sonnet (mechanical port from a precise TS source); task + final review = Opus (this is the largest item; the filter evaluator + query executor have correctness subtleties).

## Reference: the TS source to port (`ts-client/src/in_memory.ts`)

| Region | Lines | Ported in |
|---|---|---|
| `InMemoryRtDbClientOptions`, constants (`MAX_STEPS`, `MAX_TAKE`, `CRON_STEP_MS`) | 40-41, 89-96 | Task 1 |
| `toSchemaJson`, `clone`, `canonical`, id validators (`isHexId`, `isInt64String`, `isBase64String`), `isPlainObject` | 98-149 | Task 1 |
| `validateValue`, `validateDoc`, `stripUnsetOptionals` | 150-242 | Task 1 |
| `applyPatch`, `typeTag`, `indexColumnType`, `coerceIndexValue`, `compareIndexValues` | 243-360 | Task 2 |
| `validateFilter`, `evalFilterExpr` + leaf helpers (`checkLeafValue`, `inValueKind`, `compareLeaf`, `docToText`, `docToNumber`, `compareValues`) | 361-488 | Task 4 |
| `InMemoryRtDbClient` class: constructor, `pushSchema`, `mutate` (step switch), `schedule`/`cancel`/`pause`/`resume`/`listSchedules`/`tick`, storage stubs, `subscribe`, query execution (`runQuery` switch + `mergeDoc`) | 490-1391 | Tasks 1-6 |
| Reference tests (`in_memory.test.ts`, 10 describe blocks) | 1-756 | Tasks 1-6 |

**Reference test blocks → tasks:**
- `describe("InMemoryRtDbClient — schema push")` → Task 1
- `describe("… — insert + read")`, `… transactions`, `… upsert by index` → Task 2
- `… query by index` → Task 3
- `describe("evalFilterExpr + validateFilter")`, `… InMemoryRtDbClient filter` → Task 4
- `… paginate (cursor keyset)` → Task 5
- `… schedules`, `… subscribe` → Task 6

---

## File Structure

- **Create:** `rust-client/src/in_memory.rs` — the harness (`InMemoryRtDbClient` struct + impl, the ~17 helper fns, `validate_filter`/`eval_filter_expr`, and `#[cfg(test)] mod tests`).
- **Modify:** `rust-client/src/lib.rs` — add `#[cfg(feature = "in_memory")] pub mod in_memory;` + re-export `InMemoryRtDbClient` (gate behind a new `in_memory` feature so the harness is opt-in and doesn't bloat the default build; add the feature to `rust-client/Cargo.toml` `[features]`). **Confirm the feature-gating approach with the existing crate's feature style** (`http`/`ws`/`admin` are the existing gates — `in_memory` parallels them; default-off).
- **Modify:** `rust-client/Cargo.toml` — add the `in_memory` feature (no new deps; `serde_json` already present).
- **Modify:** `rust-client/README.md` — document the `in_memory` feature (one row in the features table + a short "test harness" note).
- **Modify:** `FEATURE_MATRIX.md` — flip/note row #19 (in-memory test harness) now that rust-client ships it.

---

## Task 1: schema push + doc validation core

**Files:** `rust-client/src/in_memory.rs` (new — struct + options + push_schema + the validation/id helpers), `rust-client/src/lib.rs` (mod + re-export), `rust-client/Cargo.toml` (`in_memory` feature).

**Interfaces:**
- Consumes: `crate::schema::{SchemaDef, TableDef}` (+ field-type types), `crate::wire` for `SchemaJson` if needed, `serde_json::Value`.
- Produces: `pub struct InMemoryRtDbClient { ... }` with `new(options: InMemoryRtDbClientOptions) -> Self` and `push_schema(&mut self, schema: &SchemaDef)`; helpers `to_schema_json`, `clone_value`, `canonical`, `is_hex_id`, `is_int64_string`, `is_base64_string`, `is_plain_object`, `validate_value(field_ty, value) -> bool`, `validate_doc(table_def, doc) -> Result<(), RtDbError>`, `strip_unset_optionals`. Constants `MAX_STEPS`, `MAX_TAKE`, `CRON_STEP_MS`.

- [ ] **Step 1: Write the failing tests** — port `describe("InMemoryRtDbClient — schema push")` from `ts-client/tests/in_memory.test.ts:42-54`: pushing a schema stores it; re-pushing is additive/validates; an invalid field type on a doc is rejected. Add the `#[cfg(test)] mod tests` block to `in_memory.rs`.

- [ ] **Step 2: RED** — `cd rust-client && cargo test --features in_memory in_memory` (compile errors: module/struct don't exist).

- [ ] **Step 3: Implement** — create `in_memory.rs`: the `InMemoryRtDbClient` struct (schema map `HashMap<String, TableDef-ish>`, docs store `HashMap<(String,String), StoredRow>`, schedules `Vec<...>`, subscribers, storage blobs — define `StoredRow` with `id`, `doc: Value`, `version`, `created_at`), `InMemoryRtDbClientOptions` (mirror `:91-96`), `new`, and `push_schema` (mirror `:23-38`, including `toSchemaJson` at `:98`). Port the id/format helpers (`:105-149`) and `validate_value`/`validate_doc`/`strip_unset_optionals` (`:150-242`). Add `pub mod in_memory;` + `pub use in_memory::InMemoryRtDbClient;` under `#[cfg(feature="in_memory")]` in `lib.rs`; add `in_memory = []` to `Cargo.toml [features]`.

- [ ] **Step 4: GREEN** — `cargo test --features in_memory in_memory` passes; `cargo clippy --features in_memory -- -D warnings` + `cargo fmt --check` clean.

- [ ] **Step 5: Commit** — `feat(rust-client): in-memory harness scaffold + schema/doc validation`.

---

## Task 2: storage model + mutate executor

**Files:** `rust-client/src/in_memory.rs` (mutate + step switch + index coercion + patch).

**Interfaces:**
- Consumes: Task 1's `StoredRow`/schema map; `crate::mutation::{Transaction, StepResult, Mutation}` (the step enum — confirm variant names: insert/patch/replace/delete/upsert), `crate::wire::FilterExpr` (upsert's match).
- Produces: `pub async fn mutate(&mut self, txn: &Transaction, mut_id: Option<&str>) -> Result<Vec<StepResult>, RtDbError>` (mirror `:39-110` incl. `MAX_STEPS` guard + `mut_id` dedup cache), plus `apply_patch`, `type_tag`, `index_column_type`, `coerce_index_value`, `compare_index_values` (`:243-360`).

- [ ] **Step 1: Tests** — port `describe("… insert + read")` (`:55-96`), `… transactions` (`:176-228`), `… upsert by index` (`:97-132`): insert then read back (with system fields merged via `merge_doc`); multi-step txn atomicity semantics as the TS harness models them; upsert insert-vs-patch by index; the `MAX_STEPS` overflow rejection.

- [ ] **Step 2: RED.**

- [ ] **Step 3: Implement** — port `apply_patch`/`strip_unset_optionals` (`:243-266`), the index helpers (`:267-360`), and the `mutate` step switch (`:263-360` region + the executor at the class's `mutate` body `:39-110`). Each step (`Insert`/`Patch`/`Replace`/`Delete`/`Upssert`) updates the docs store, bumps `version`, and produces a `StepResult`. Mirror the TS exactly — including ownership/version checks the harness models. `merge_doc` (`:1154`) merges stored `doc` + system fields (`_id`, `_version`, `_creationTime`) into the returned `Value`.

- [ ] **Step 4: GREEN** + clippy + fmt.

- [ ] **Step 5: Commit** — `feat(rust-client): in-memory mutate executor (insert/patch/replace/delete/upsert)`.

---

## Task 3: query execution

**Files:** `rust-client/src/in_memory.rs` (the `run_query` switch + index/range filtering + terminals).

**Interfaces:**
- Consumes: `crate::query::{Query, Order}` (eq/gt/gte/lt/lte/take/order/get/first/unique/count/paginate/search/vector_search/filter), Task 2's index-compare helpers + `merge_doc`.
- Produces: `pub async fn run<T: DeserializeOwned>(&self, query: &Query) -> Result<T, RtDbError>` (mirror `:406-545` validation + the terminal dispatch; reuse `crate::query::parse_result` for the `{result}` unwrap if it fits, else deserialize directly).

- [ ] **Step 1: Tests** — port `describe("… query by index")` (`:133-175`): eq-match on a single-field index; range (`gt`/`lt`/`gte`/`lte`); `take`/`order` (asc/desc); `get`/`first`/`unique`/`count` terminals; the validation rejections (`MAX_TAKE`, conflicting terminals like `count`+`first`, `gt`+`gte` together) from `:423-481`.

- [ ] **Step 2: RED.**

- [ ] **Step 3: Implement** — port the query-validation switch (`:406-481`) and the row-fetch + terminal dispatch (`:483-545`, `:916`, `:1143-1194`): filter rows by index `eq` then range bounds using `compare_index_values`, apply `filter` (Task 4 wires `eval_filter_expr`; for Task 3 leave the filter hook as a `todo!()`/pass-through and have Task 4 fill it — OR port the filter inline if cleaner; coordinate with Task 4's brief), then reduce to the terminal (`get`→first match or None, `first`→first, `unique`→exactly-one-or-err, `count`→len, `take`/`collect`→`Vec`, `order` sort). `merge_doc` every emitted row.

- [ ] **Step 4: GREEN** + clippy + fmt.

- [ ] **Step 5: Commit** — `feat(rust-client): in-memory query executor (index/range/terminals)`.

---

## Task 4: filter evaluation (the C-corrected logic)

**Files:** `rust-client/src/in_memory.rs` (`validate_filter` + `eval_filter_expr` + leaf helpers, wired into Task 3's row-loop).

**Interfaces:**
- Consumes: `crate::wire::FilterExpr` (recursive — `Eq`/`Neq`/`Gt`/`Gte`/`Lt`/`Lte`/`In`/`And`/`Or`/`Not`; confirm exact variants at `wire.rs:316`), `serde_json::Value` docs.
- Produces: `pub fn validate_filter(expr: &FilterExpr, fields: &BTreeSet<String>) -> Result<(), RtDbError>` and `pub fn eval_filter_expr(expr: &FilterExpr, doc: &Value) -> bool`, plus private leaf helpers `check_leaf_value`, `in_value_kind`, `compare_leaf`, `doc_to_text`, `doc_to_number`, `compare_values`.

- [ ] **Step 1: Tests** — port `describe("evalFilterExpr + validateFilter")` (`:539-654`) and `describe("InMemoryRtDbClient filter")` (`:655-756`): every `FilterExpr` op evaluates correctly against a doc; `null`/absent field → no match (never a match for any op); the 5 BAD_REQUEST cases from `validateFilter` (unknown field; `in` with mixed-type values — `inValueKind` returns the value-kind domain and rejects a mix; wrong value-kind for a comparison op; etc.). **These are the cases C fixed — port them verbatim.**

- [ ] **Step 2: RED.**

- [ ] **Step 3: Implement** — port `checkLeafValue`/`inValueKind`/`compareLeaf`/`docToText`/`docToNumber`/`compareValues` (`:388-475`) and `validateFilter`/`evalFilterExpr` (`:361-488`) **verbatim in behavior**. Map TS `unknown`→`&serde_json::Value`, `ReadonlySet<string>`→`&BTreeSet<String>`. The value-kind domain: string→text compare, number→float8 compare, boolean→bool; `in` requires all values share one kind (else BAD_REQUEST). Wire `eval_filter_expr` into Task 3's row-filter hook so a non-matching doc is excluded.

- [ ] **Step 4: GREEN** + clippy + fmt.

- [ ] **Step 5: Commit** — `feat(rust-client): in-memory FilterExpr evaluator (C-corrected logic)`.

---

## Task 5: cursor-keyset pagination

**Files:** `rust-client/src/in_memory.rs` (the `paginate` terminal + cursor reuse).

**Interfaces:**
- Consumes: `crate::cursor::{encode_cursor, decode_cursor}`, `crate::query::{Paginate, Paginated}`, `crate::query::Order`.
- Produces: the `paginate` branch of the query executor — sort by the order key(s), slice `[cursor_end .. cursor_end+limit]`, encode the new cursor, return `Paginated<T>` (or the harness's shape — match `in_memory.ts`).

- [ ] **Step 1: Tests** — port `describe("… paginate (cursor keyset)")` (`:250-431`): forward/backward paging over a sorted set; cursor round-trips; the `continueCursor`/`done` semantics the TS harness models; stable ordering ties (by creation time then id, matching the server).

- [ ] **Step 2: RED.**

- [ ] **Step 3: Implement** — port the paginate terminal (`:451-467` + the keyset slice logic in the executor). Reuse `encode_cursor`/`decode_cursor` for the opaque cursor (the server and rust-client already share this shape — `cursor.rs:8,14`). Match the TS harness's page-boundary + `done` semantics.

- [ ] **Step 4: GREEN** + clippy + fmt.

- [ ] **Step 5: Commit** — `feat(rust-client): in-memory cursor-keyset pagination`.

---

## Task 6: schedules + storage stubs + subscribe + gate

**Files:** `rust-client/src/in_memory.rs` (schedule harness + storage + search/vector stubs + subscribe), `rust-client/README.md`, `FEATURE_MATRIX.md`.

**Interfaces:**
- Consumes: `crate::wire::{ScheduleWhen, ScheduleInfo}` (confirm names), `crate::mutation::Transaction`, `crate::wire::{UploadResult, FileMetadata}` (or the local types — match `http.rs`).
- Produces: `schedule(txn, when) -> {id}`, `cancel_schedule`/`pause_schedule`/`resume_schedule`/`list_schedules`/`tick(now_ms)` (`:111-153, 194-262`), storage `upload`/`delete_file`/`get_file_metadata`/`get_url` (`:157-191`), `subscribe(query, on_update)` (`:229-249`), and the `search`/`vector_search` `[]` stubs in the query executor.

- [ ] **Step 1: Tests** — port `describe("… schedules")` (`:432-538`) and `… subscribe` (`:229-249`): one-shot + cron schedule registration; `tick` fires due jobs (oneshot catches up if past due; cron steps by `CRON_STEP_MS` and skips missed windows); pause/resume; cancel. Subscribe: an initial callback fires, then a mutate that affects the query triggers an update.

- [ ] **Step 2: RED.**

- [ ] **Step 3: Implement** — port the schedule model (`ScheduledJob` with `status`/`due_at`/`kind`/`cron`/`last_error`, `:194-262`), the storage stubs (in-memory `HashMap<id, (bytes, content_type, created_at)>`), `subscribe` (store `(query, callback)`; on `mutate`/`tick`, re-run affected queries and call back on change), and the `search`/`vector_search` `[]` returns in the query executor (`:484-545`). Keep stubs honest (same empty shape as the TS harness).

- [ ] **Step 4: GREEN** + clippy + fmt.

- [ ] **Step 5: Docs** — `rust-client/README.md`: add the `in_memory` feature row + a one-paragraph "In-memory test harness" note (what it is, that it mirrors the TS harness, search/vector are stubs). `FEATURE_MATRIX.md`: flip/note row #19 — rust-client now ships the in-memory test harness (parity with ts-client).

- [ ] **Step 6: Full gate** — `cd rust-client && cargo test --all-features && cargo clippy --all-features -- -D warnings && cargo fmt --check`; then `make checkall` from the repo root (needs dev-db on 55434 for server tests — reuse the running instance; if `make dev-db-up` port-conflicts, run the 5 package test suites directly against the existing 55434 db, as in A-rust).

- [ ] **Step 7: Commit** — `feat(rust-client): in-memory schedules + storage stubs + subscribe; close #19`.

---

## Self-Review (completed during authoring)

- **Spec coverage:** E = the rust in-memory test harness (#19). Task 1 = scaffold + schema/doc validation; Task 2 = mutate executor; Task 3 = query executor; Task 4 = filter eval (C-corrected); Task 5 = pagination; Task 6 = schedules + stubs + subscribe + docs/gate. Every `in_memory.ts` region (lines 40-1391) and every reference test block (10 describes, 50 cases) is assigned to a task. ✅
- **Scope:** search/vector/storage are honest stubs (design doc: out of scope). No new wire surface — `FilterExpr`/`Query`/`cursor`/`Transaction`/`SchemaDef` all reused. ✅
- **Type reuse verified:** `crate::wire::FilterExpr` (wire.rs:316), `crate::query::{Query,Order,Paginated,Paginate}` (query.rs), `crate::cursor::{encode_cursor,decode_cursor}` (cursor.rs:8,14), `crate::mutation::{Transaction,StepResult,Mutation}`, `crate::schema::SchemaDef`, `serde_json::Value` docs (matches optimistic.rs). ✅
- **C-correctness preserved:** Task 4 ports the filter eval verbatim from the C-corrected `in_memory.ts:361-488`, including all 5 BAD_REQUEST cases and the `in` value-kind domain. ✅
- **Placeholders:** every task names the exact `in_memory.ts` line ranges + test blocks to port and the rust-client types to reuse; the "confirm variant names" notes are verify-against-existing-code items for the implementer (the variants exist; the implementer reads them), not placeholders. ✅
- **Feature-gating:** new `in_memory` feature (default-off) parallels `http`/`ws`/`admin` — opt-in, doesn't bloat the default build. ✅
