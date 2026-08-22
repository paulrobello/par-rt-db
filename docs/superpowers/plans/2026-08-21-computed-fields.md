# Declarative Computed Fields (ENH-028) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A schema field declared `computed: <ValueExpr>` is re-evaluated by the server on every write (insert / patch / replace / upsert / patchByQuery / cascade setNull) and stored in the doc + typed column, making derived values indexable — declarative denormalization with no server code.

**Architecture:** The ENH-020 typed `ValueExpr` grammar (today SQL-compiled only for migrate's one-shot `evalExpr`) becomes persistent: a new `TableDef.computed: BTreeMap<String, ValueExpr>` wire map declares computed fields; a new **Rust interpreter** `eval_value_expr` (mirroring `compile_value_expr`'s semantics) runs inside the write-path stamp chain (`stamp_computed`, the FM-32/36 stamping pattern); migrate `renameField` rewrites expression references, `dropField` on a referenced field is rejected, and `evalExpr` + push-backfill re-stamp so stored values never go stale. Four client mirrors add the wire type, schema-DSL declaration, engine interpreter, and engine stamping, kept honest by new semantics-corpus cases.

**Tech Stack:** Rust (axum/tokio/sqlx, serde), TypeScript (bun), Python (uv), Swift; wire-corpus semantics fixtures.

**Spec:** kanban card `01a022504be97ab0bcd47b8f0e34e316` ("Declarative computed fields (write-maintained ValueExpr)") — title, acceptance criteria, sketch, and the pinned design decisions appended to its notes on 2026-08-21. This plan argues from that card.

## Global Constraints

- `make checkall` (fmt-check + clippy `-D warnings` + typecheck + tests) is the definition of done; dev Postgres must be up (`make dev-db-up`, port 55434). If another healthy `*-postgres-1` container already holds 55434, reuse it and run the steps directly instead of fighting for the port.
- The wire contract must stay byte-identical across five implementations: `server/src/{schema,protocol}.rs`, `ts-client/src/`, `rust-client/src/wire.rs`, `python-client/src/par_rt_db/wire.py`, `swift-client/Sources/ParRtDbClient/`. Serde casing is deliberately non-uniform and load-bearing.
- **Additive wire only:** every new schema field uses `#[serde(default, skip_serializing_if = …)]` so existing schemas deserialize and re-serialize unchanged. The new `computed` map is omitted when empty (`BTreeMap::is_empty`), wire key `computed`.
- No `unwrap()`/`expect()` outside `#[cfg(test)]`; zero clippy warnings under `-D warnings`; every client-facing error is the `RtDbError` envelope `{code, message}` with a generic message for 500s.
- Every behavior change ships with semantics-corpus cases in the same change (wire-corpus README authoring rule).
- Docs updated in the same change: `FEATURE_MATRIX.md`, server README schema section, each client README, the design spec `docs/superpowers/specs/2026-07-21-par-rt-db-design.md`.
- **Sub-agent execution rules for this plan:** Tasks 6–9 (the four client mirrors) are file-disjoint and MUST be dispatched in parallel. Implementer sub-agents do NOT commit (the orchestrator verifies each batch and commits after the last sibling finishes — the repo pre-commit hook stashes unstaged files, so a commit during a sibling's edit window risks a conflicted restore). Tasks 1–5 (server) are serial. Client tasks must not edit anything outside their package directory; corpus/docs files are owned by Task 5.

## ValueExpr interpreter semantics (authoritative for ALL FIVE implementations)

The interpreter mirrors `server/src/migrate.rs::compile_value_expr` (the SQL compiler) with the pinned deviations below. Evaluation is over a doc (`serde_json::Map<String, Value>` / `Record<string, unknown>` / `dict` / `[String: JSONValue]`) and a `now_ms: i64` integer. Result is a JSON value or a `bad_request` error whose message names the computed field.

**Text conversion** (`to_text(v) -> string | null`) — used by `Field`, `Concat` parts, `Lower`/`Upper`/`Trim` operands, `Cast:ToString`:
- `null` or absent key → SQL NULL (represented as JSON `null`)
- String → as-is
- Number → its JSON number text form (`42` → `"42"`, `42.5` → `"42.5"`)
- Bool → `"true"` / `"false"`
- Object/Array → **compact** JSON text (`{"a":1}` — deliberately NOT Postgres's spaced jsonb text; we define the convention here and all five implementations use compact)

**Numeric conversion** (`to_numeric(v) -> f64 | null | error`):
- `null` → null (SQL NULL propagation)
- Number → its f64 value
- String → trimmed, strict `f64` parse; unparseable → error
- Bool / Object / Array → error ("cannot cast to number")

Node rules:

| Node | Semantics |
|---|---|
| `Field { field }` | `to_text(doc[field])` — **always text** (mirrors SQL `doc->>'field'`); absent key or JSON null → null |
| `Literal { value }` | the JSON value itself (strings/numbers/bools as-is; objects/arrays pass through as jsonb) |
| `Concat { parts }` | evaluate parts in order; skip nulls; `to_text` each non-null part; concatenate. All-null → `""` (Postgres `concat()` of only NULLs is the empty string) |
| `Add/Sub/Mul { l, r }` | `to_numeric` both; either null → null; IEEE double arithmetic; non-finite result → error |
| `Div { l, r }` | as arithmetic; right operand `0` (or `-0`) → error "division by zero" |
| `Coalesce { parts }` | first non-null result, else null |
| `Lower/Upper { v }` | `to_text`; null → null; else ASCII-lowercase/uppercase (Rust `to_lowercase`/JS `toLowerCase` agree on ASCII; pin corpus cases to ASCII) |
| `Trim { v }` | `to_text`; null → null; else strip leading/trailing **spaces only** (Postgres `btrim` default — NOT full Unicode whitespace) |
| `Cast { to: ToString }` | `to_text` → string; null → null |
| `Cast { to: ToNumber }` | `to_numeric` → JSON number; null → null |
| `Cast { to: ToInt64 }` | Number: must be integral (`as_i64`, else error); String: trimmed strict i64 parse; Bool/Object/Array → error; null → null. Result is a JSON **number** here (the int64 *string* wire convention applies only to stored int64 fields — see "Int64 note") |
| `Cast { to: ToBoolean }` | Bool → as-is; Number: `1`→true, `0`→false, else error; String (case-insensitive): `true/t/yes/on/1` → true, `false/f/no/off/0` → false, else error; null → null |
| `Now` | `now_ms` as a JSON number (epoch-ms — see "Now() alignment") |
| `Case { whens, otherwise }` | whens in order: evaluate `when` with the package's existing `FilterExpr`-matches-doc function (server: `crate::dsl::filter_matches`; engines: their query-path matcher) with principal markers unavailable (push validation rejects `$user`/`$email` markers inside computed exprs, so any ctx is semantically irrelevant). First match → its `then`; none → `otherwise` |

**Storing the result** (`stamp_computed` rule): evaluate every entry of the table's `computed` map against the final doc; a null result **removes the key** from the doc (matches `strip_unset_optionals`' shape convention — an unset optional field is an absent key, not a stored null); a non-null result overwrites whatever is there (the `ownerField` authority model — client-supplied values never survive). An evaluation error fails the whole write with `bad_request`, message naming the computed field.

**Int64 note:** a computed field whose declared type is `int64` must produce the int64 *decimal-string* wire form to pass `validate_doc` (same convention as `stamp_updated_at`/`stamp_auto_increment`: `Int64` fields validate decimal strings). Arithmetic produces JSON numbers, so the push-time static check (Task 2) rejects a Number-kind expression on an Int64 field; wrap in `Cast:ToString` to store into an int64 field.

**Now() alignment (deliberate semantic change to migrate):** migrate's SQL `compile_value_expr` currently emits `now()` for `ValueExpr::Now` (yielding an ISO timestamp string via `to_jsonb`). This change aligns it to epoch-ms by emitting `((extract(epoch from now()) * 1000)::bigint)` so the one-shot path and the per-write path produce the same value. Update the migrate docs row + any examples in the same commit.

## File Structure

**Server (Tasks 1–5):**
- Create `server/src/value_expr.rs` — `ValueExpr`, `Cast`, `CaseWhen` types (moved out of `migrate.rs`), the SQL compiler `compile_value_expr` (moved), and the new interpreter `eval_value_expr` + helpers (`to_text`, `to_numeric`)
- Modify `server/src/migrate.rs` — re-export moved types; `validate_one` arms (renameField rewrite, dropField reject, changeType re-validate); `evalExpr` re-stamp; Now() epoch-ms
- Modify `server/src/schema.rs` — `TableDef.computed`; computed validation (`validate_computed`, static type inference) called from push + migrate derived-schema validation
- Modify `server/src/txn.rs` — `stamp_computed`; choke-point calls in `apply_patch`, `do_insert`, `do_replace`; cascade setNull site
- Modify `server/src/ddl.rs` (or `admin/schema_ops.rs` where additive push applies) — one-time backfill when a table's computed map is added/changed
- Create `server/tests/computed_test.rs` — integration tests (criteria 1–3 server half)
- Modify `wire-corpus/semantics/*` (new case files), `wire-corpus/wire-corpus.json` (schema wire shape), `FEATURE_MATRIX.md`, `README.md`, `docs/superpowers/specs/2026-07-21-par-rt-db-design.md`

**Clients (Tasks 6–9, one package each, parallel):**
- `ts-client/src/schema.ts` (+ `in_memory/store.ts`, `in_memory/migrate.ts`), tests, README
- `rust-client/src/schema.rs`, `wire.rs` (+ `in_memory/*`), tests, README
- `python-client/src/par_rt_db/schema.py`, `wire.py` (+ `in_memory/*`), tests, README
- `swift-client/Sources/ParRtDbClient/SchemaDsl.swift`, `Wire.swift` (+ `InMemoryEngine.swift`, `InMemoryMigrate.swift`), tests

---

### Task 1: Server — `value_expr.rs` module with the interpreter

**Files:**
- Create: `server/src/value_expr.rs`
- Modify: `server/src/migrate.rs` (move `ValueExpr`/`Cast`/`CaseWhen` + `compile_value_expr` out, re-export), `server/src/lib.rs` (register module)

**Interfaces:**
- Produces: `pub enum ValueExpr` (unchanged wire shape, `#[serde(tag = "op", rename_all = "camelCase", deny_unknown_fields)]`), `pub enum Cast`, `pub struct CaseWhen`, `pub fn compile_value_expr(ve, table, start_pos, binds) -> Result<String, RtDbError>` (moved verbatim, now `pub(crate)`), and **new**:
  - `pub fn eval_value_expr(ve: &ValueExpr, doc: &serde_json::Map<String, serde_json::Value>, now_ms: i64, ctx: &crate::auth::PrincipalCtx) -> Result<serde_json::Value, RtDbError>`
  - `fn to_text(v: &serde_json::Value) -> Option<String>` (None = null), `fn to_numeric(v: &serde_json::Value) -> Result<Option<f64>, RtDbError>`
  - `pub fn walk_value_expr_fields(ve: &ValueExpr, f: &mut impl FnMut(&str))` — visits every `Field` name AND every `FilterExpr` field inside `Case.whens` (reuse whatever field-walker `query` uses for `FilterExpr`, e.g. the one behind `validate_filter_expr_fields`; if it is not reusable, write a small recursive walk over `FilterExpr` variants)

- [ ] **Step 1: Write failing unit tests** in `server/src/value_expr.rs` (`#[cfg(test)] mod tests`) covering the semantics table: Field text extraction (string/number/bool/absent→null), Literal passthrough, Concat null-skip + all-null→`""`, Add over string fields (`"42"` + `"1"` → 43), null propagation, Div-by-zero error, Div non-finite error, Coalesce, Lower/Upper/Trim (spaces-only: `"\tx"` stays `"\tx"`), all four Casts (happy + error paths, ToInt64 non-integral error, ToBoolean's accepted string set), Now → number, Case first-match/otherwise. Example:

```rust
#[test]
fn concat_skips_nulls_and_casts_numbers_to_text() {
    let mut doc = serde_json::Map::new();
    doc.insert("first".into(), serde_json::json!("Ada"));
    doc.insert("n".into(), serde_json::json!(42));
    let expr = ValueExpr::Concat {
        parts: vec![
            ValueExpr::Field { field: "first".into() },
            ValueExpr::Field { field: "missing".into() },
            ValueExpr::Field { field: "n".into() },
        ],
    };
    let ctx = crate::auth::PrincipalCtx::bypass(); // markers are push-rejected; ctx is irrelevant
    assert_eq!(
        eval_value_expr(&expr, &doc, 0, &ctx).unwrap(),
        serde_json::json!("Ada42")
    );
}
```

(If `PrincipalCtx` has no `bypass()` constructor, use whatever `txn.rs`'s machine/admin path constructs; check `auth/` for the bypass ctx shape.)

- [ ] **Step 2: Run** `cargo test --lib value_expr --manifest-path server/Cargo.toml` — expect compile failure (module doesn't exist).
- [ ] **Step 3: Implement** the move + interpreter exactly per the semantics table. `Case` evaluation: `crate::dsl::filter_matches(&serde_json::Value::Object(doc.clone()), &cw.when, ctx)` — clone only when a Case is present (hot path avoids it). Numeric results: `serde_json::Number::from_f64(x)` (error when `None`). Division: `right == 0.0` → `RtDbError::bad_request("division by zero")`.
- [ ] **Step 4: Run** `cargo test --lib value_expr --manifest-path server/Cargo.toml` — PASS; then `cargo test --manifest-path server/Cargo.toml --test migrate_test` (or the existing migrate test file — locate via `ls server/tests`) — PASS unchanged.
- [ ] **Step 5:** fmt + clippy clean: `cargo fmt --manifest-path server/Cargo.toml && cargo clippy --manifest-path server/Cargo.toml --all-targets -- -D warnings`. **Do not commit** (orchestrator commits).

### Task 2: Server — `TableDef.computed` wire + push validation

**Files:**
- Modify: `server/src/schema.rs` (TableDef + `validate_computed`), the pushSchema handler in `server/src/admin/schema_ops.rs` (call validation), `server/src/migrate.rs::plan_migration` (validate the derived schema's computed map after folding)

**Interfaces:**
- Consumes: `ValueExpr`, `walk_value_expr_fields` from Task 1; existing `validate_value(field_type, &Value) -> bool`, `validate_filter_expr_fields` (in `query` — check its `allow_principal_markers` parameter)
- Produces: `TableDef { ..., #[serde(default, skip_serializing_if = "BTreeMap::is_empty")] pub computed: BTreeMap<String, crate::value_expr::ValueExpr> }`; `pub fn validate_computed(schema: &SchemaDef) -> Result<(), RtDbError>`; `fn infer_static_kind(ve: &ValueExpr) -> Option<StaticKind>` with `enum StaticKind { String, Number, Boolean }`

- [ ] **Step 1: Failing unit tests** (`schema.rs` tests): (a) rejects a computed key not declared in `fields`; (b) rejects a `Field` reference to an undeclared field; (c) rejects a reference to another computed field; (d) rejects a `$user`/`$email` marker inside a `Case.when`; (e) static-kind reject: `Concat` into a `number` field, arithmetic into an `int64` field, `Lower` into a `boolean` field; (f) accepts the canonical schemas (fullName/concat on string, slug/lower-trim on optional string, arithmetic on number, Case on a union, Now on number, Cast(ToString) into int64); (g) rejects computed on `ownerField`/`collaboratorsField`/`autoIncrementField`.
- [ ] **Step 2: Run** — expect failure.
- [ ] **Step 3: Implement.** Validation rules, in order, per table:
  1. every `computed` key must exist in `fields`
  2. `computed` key must not be the table's `owner_field`, `collaborators_field`, or `auto_increment_field` (`bad_request` naming the conflict)
  3. `walk_value_expr_fields` over each expr: every referenced field must be declared AND not itself in `computed`
  4. `Case.whens` filters validated with the marker-rejecting mode of `validate_filter_expr_fields` (same call the `authorize` path uses for its predicate, `allow_principal_markers = false` — locate the exact signature and mirror it)
  5. static check when `infer_static_kind` is `Some(k)`: `validate_value(field_type, sample)` must hold for the kind's sample — `String→json!("s")`, `Number→json!(1)`, `Boolean→json!(true)`; else `bad_request("computed field 'x' produces <kind>, which field type does not accept")`. `Field`/`Coalesce`/`Case`/null-literal/obj-literal infer `None` (runtime `validate_doc` guards).
  Call `validate_computed` from the pushSchema handler next to the existing schema validation, and from `plan_migration` against the final derived schema (so `changeType` folding re-validates — plus explicit checks in Task 4).
- [ ] **Step 4: Run** tests; `cargo fmt` + clippy clean. **Do not commit.**

### Task 3: Server — `stamp_computed` on every write path

**Files:**
- Modify: `server/src/txn.rs`
- Create: `server/tests/computed_test.rs`

**Interfaces:**
- Consumes: `eval_value_expr` (Task 1), `TableDef.computed` (Task 2)
- Produces: `fn stamp_computed(table_def: &TableDef, mut doc: Map, now: i64, ctx: &PrincipalCtx) -> Result<Map, RtDbError>`; choke-point calls at exactly four sites (see Step 3)

- [ ] **Step 1: Failing integration tests** in `server/tests/computed_test.rs` (follow an existing integration test's harness — e.g. how `txn_test.rs` builds a test db; use `wrap_test_db` for a bare db, NOT `fresh_db` which seeds a fixture). Schema: `users { first: string, last: string, fullName: string computed concat(field first, " ", field last) }` + index `by_fullName [fullName]`; push via the HTTP handler or `ddl::push_schema` + capture-history path the tests use (mirror `txn_test.rs`). Tests (criterion 2):
  1. insert with a client-supplied `fullName: "WRONG"` → stored doc's `fullName` is the concat result
  2. patch `{ first: "Grace" }` → `fullName` recomputed
  3. replace → recomputed
  4. upsert insert-branch and update-branch → recomputed
  5. query `order(fullName, desc)` + `count` with filter on `fullName` → indexable (criterion 2's "order and count work")
  6. patchByQuery changing `first` → recomputed
  7. optional computed field whose expr yields null (Coalesce over a missing field) → key absent from stored doc
  8. runtime error: `Div` by zero computed field → write fails `BAD_REQUEST`, doc unchanged
- [ ] **Step 2: Run** `cargo test --manifest-path server/Cargo.toml --test computed_test` — FAIL.
- [ ] **Step 3: Implement.** `stamp_computed` per the semantics table's storing rule (null → `remove` key; else `insert`; error message prefixed `computed field '<name>': `). Strip client-supplied computed values pre-merge: in `apply_patch`'s per-field loop, skip+drop keys present in `table_def.computed`; in `do_replace` and the insert paths the post-stamp overwrite handles them, but ALSO drop them in `do_replace` before `validate_doc` so a client-supplied wrong-typed value can't fail validation before the stamp overwrites it. Call sites (study each; the stamp must run AFTER merges/other stamps so exprs see final inputs, BEFORE `validate_doc`):
  1. `apply_patch` — after the merge loop, before its trailing `validate_doc` (covers `do_patch`, upsert-update, `patch_by_query`)
  2. `do_insert` — before `validate_doc` (covers Insert + upsert-insert)
  3. `do_replace` — after auto-increment preservation, before `validate_doc`
  4. cascade setNull inside `delete_row_cascade` — locate where it nulls the ref field in child docs (it already re-stamps updatedAt there per FM-36); stamp computed after that doc rewrite. If it bypasses `apply_patch` with a raw SQL jsonb update, route the child doc through `eval_value_expr` in Rust and write via the existing update path it uses.
  Also: `stamp_ttl_default`/`apply_defaults` run BEFORE computed (computed wins on conflict — document in a comment); `stamp_updated_at` order vs computed is irrelevant because computed fields may not reference undeclared fields and `_updatedAt`-style stamps are on declared fields — but note: an expr referencing the `updatedAtField` field sees the freshly stamped value only if computed runs after `stamp_updated_at`; at every site above it does (verify at each call site and note in the comment).
  Snapshot import (`insert_snapshot_row`) is NOT touched — replay never re-stamps.
- [ ] **Step 4: Run** computed_test — PASS; run the full server suite `cargo test --manifest-path server/Cargo.toml` (dev-db up; reuse a healthy 55434 container if present) — no regressions. fmt + clippy clean. **Do not commit.**

### Task 4: Server — migrate interplay + push backfill + Now() alignment

**Files:**
- Modify: `server/src/migrate.rs`, `server/src/ddl.rs` (or `admin/schema_ops.rs` — wherever additive push applies DDL; find where `setDefault`-style backfills or `recompute_all_indexed` are invoked from push)

**Interfaces:**
- Consumes: `validate_computed` (Task 2), `compile_value_expr` (Task 1), `recompute_all_indexed` / the jsonb_set UPDATE pattern in `migrate.rs`
- Produces: renameField/dropField/changeType computed handling; `evalExpr` re-stamp; `pub(crate) async fn backfill_computed(...)` called from additive push when a table's computed map was added/changed; Now() epoch-ms SQL

- [ ] **Step 1: Failing tests** — extend `server/tests/computed_test.rs` (criterion 1 + 3 server half):
  1. push rejects: expr referencing an undeclared field; `Concat` into a number field (criterion 1, through the real push path)
  2. migrate `renameField users.first → givenName` → derived schema's computed expr references `givenName`; a subsequent patch recomputes from `givenName` (criterion 3)
  3. migrate `dropField` on `first` (referenced) → `BAD_REQUEST` naming `fullName`; dropping an unrelated field still works
  4. migrate `changeType` of a referenced field to a type the expr can't produce (e.g. referenced field → boolean while expr feeds Concat… choose: change `first` to boolean → derived-schema `validate_computed` rejects when the static check catches it) → rejected
  5. migrate `evalExpr` setting `first = "New"` on rows → affected rows' `fullName` re-stamped in the same migrate (no stale value)
  6. push ADDING a computed field to a table with existing rows → existing rows backfilled once (query shows computed value); pushing an unrelated change does NOT re-backfill (assert via a Now()-free deterministic expr and `_version`/doc stability — a pure push must not bump versions)
- [ ] **Step 2: Run** — FAIL.
- [ ] **Step 3: Implement.**
  - `validate_one` `RenameField` arm (migrate.rs:275): after the existing index/owner/collabs rewrite, rewrite the table's `computed` map: walk each expr with a mutator (mirror of `walk_value_expr_fields` with `&mut`), renaming `Field == from` → `to`, and the same inside `Case.whens`' `FilterExpr`s (reuse whatever field-rewrite mechanism the `authorize` predicate gets on rename if one exists; if none exists for `authorize`, write the small recursive renamer and note it).
  - `DropField` arm: before removing, `walk_value_expr_fields` over every computed expr of the table; if any references the field → `bad_request("field 'x' is referenced by computed field 'y'; drop the computed field first")`.
  - `changeType` arm: the derived schema already flows through `plan_migration`; Task 2 made `plan_migration` call `validate_computed` on the final schema — verify the error surfaces as migrate rejection; if folding order matters (rename THEN changeType in one directive list), the final-schema check covers it.
  - `evalExpr` apply: after the doc-rewrite UPDATE and before/with `recompute_all_indexed`, for each computed field of the table run one `UPDATE … SET doc = jsonb_set(doc, '{<field>}', to_jsonb((<compile_value_expr>)), true) WHERE id = ANY($1)` over the affected ids, then the existing `recompute_all_indexed` (which then also refreshes the computed field's indexed column). Binds: reuse `MigrateBind`.
  - Push backfill: in the additive-push path, diff old vs new `computed` map per table; when entries were added or their expr changed, run the same jsonb_set UPDATE for ALL rows of that table (idempotent; Now()-bearing exprs refresh timestamps on expr change — acceptable, documented) + `recompute_all_indexed` for the table. Removal of a computed entry: leave stored values in place (they become ordinary client-writable fields — document this in the README).
  - Now() alignment: in `compile_value_expr`, `ValueExpr::Now => "((extract(epoch from now()) * 1000)::bigint)"`. Sweep docs/examples mentioning the old ISO-string output (FEATURE_MATRIX row text, README migrate section, any dashboard help text — grep `now()` usage docs).
- [ ] **Step 4: Run** computed_test + migrate tests + full server suite. fmt + clippy. **Do not commit.**

### Task 5: Server — corpus cases + docs

**Files:**
- Create: `wire-corpus/semantics/computed-insert-patch-recompute.json`, `computed-push-validation.json`, `computed-migrate-rename.json`, `computed-null-optional.json`
- Modify: `wire-corpus/wire-corpus.json` (if push-schema payloads appear there — check `client_messages`; add one schema with a computed map if so), `FEATURE_MATRIX.md`, `README.md`, `docs/superpowers/specs/2026-07-21-par-rt-db-design.md`

**Interfaces:**
- Consumes: Tasks 1–4 server behavior; `wire-corpus/README.md` case format (read it first — schema + seed + operation + expected, substitution placeholders)

- [ ] **Step 1: Author the four semantics cases** (server is the source of truth for expected values — run each scenario against the server to capture them; never hand-compute). Case contents:
  1. `computed-insert-patch-recompute` — concat fullName; insert (client-supplied value overwritten), patch first → recompute, order+count on the indexed computed field (criterion 2 as a corpus case)
  2. `computed-push-validation` — two pushes: undeclared-field ref → error envelope; static-kind mismatch → error envelope (criterion 1). NOTE: engines must produce the same error codes from their push paths — Tasks 6–9 implement engine-side push validation for exactly these cases.
  3. `computed-migrate-rename` — renameField rewrites the expr (assert derived schema + a post-migrate patch recomputes); dropField-referenced → error (criterion 3)
  4. `computed-null-optional` — optional computed field, Coalesce→null on missing input → key absent in stored/read doc
  Determinism: no `Now()` in corpus cases (nondeterministic); cover Now in `computed_test.rs` only (assert `is number` + monotonic).
- [ ] **Step 2: Run the server's corpus runner** (locate it: `ls server/tests | grep -i corpus`; run that test) — server must pass all four new cases.
- [ ] **Step 3: Docs.** `FEATURE_MATRIX.md`: add/flip the computed-fields row (the card originated from the 2026-08-20 feature-matrix review — find the ranked gap row for computed fields and flip it to ✅ with the established "Implemented — … Mirrored end-to-end: …, with integration coverage in `server/tests/computed_test.rs` and corpus cases `computed-*.json`" style; if no row exists, add one to the schema section following §1's format). Server `README.md`: schema section — `computed` declaration, semantics summary (authority model, null-removal, indexability, Now epoch-ms, push-backfill, remove-computed leaves values), a concat + lower/trim example. Spec `2026-07-21-par-rt-db-design.md`: add a computed-fields subsection to the schema chapter documenting the wire shape and invariants. Update the migrate row's Now wording if it mentions timestamps.
- [ ] **Step 4: `cargo test --manifest-path server/Cargo.toml --test corpus*` (or equivalent) green; docs grep-check** (no stale "Now() produces" claims: `grep -rn "now()" README.md FEATURE_MATRIX.md docs/`). **Do not commit.**

### Task 6: ts-client mirror (PARALLEL with Tasks 7–9; after Task 5)

**Files:** `ts-client/src/schema.ts`, `ts-client/src/in_memory/store.ts`, `ts-client/src/in_memory/migrate.ts`, wire types where `SchemaJson`/`TableDefJson` live (grep `updatedAtField` in `ts-client/src` to find every site), `ts-client/tests/`, `ts-client/README.md`. NOTHING outside `ts-client/`.

**Interfaces:**
- Consumes: Task 5's corpus cases (runner must pass them), the wire shape from Task 2, the semantics table above.
- Produces: `computed` on the table DSL; `ValueExprJson` builders if absent; engine `evalValueExpr` + `stampComputed`; engine push validation mirror.

- [ ] **Step 1:** Wire: add `computed?: Record<string, ValueExprJson>` to the table wire type (check whether `ValueExprJson` already exists from the migrate mirror — `store.ts` references it; if the typed union is missing, add it to match `server/src/value_expr.rs::ValueExpr`'s serde shape exactly: `tag: "op"`, camelCase, unknown ops rejected).
- [ ] **Step 2:** DSL: mirror the `defaultsMap`/`updatedAtFieldName` pattern — add a `computedMap` constructor param + fluent `.computed(name, expr)` method on the table builder. Provide `ValueExpr` builder helpers (`ve.field(name)`, `ve.literal(v)`, `ve.concat(...parts)`, `ve.add(l, r)` … `ve.case(whens, otherwise)`, `ve.now()`, casts) as a small exported `ve` namespace if no equivalent exists from the migrate mirror.
- [ ] **Step 3:** Engine (`in_memory/store.ts`): implement `evalValueExpr(expr, doc, nowMs)` per the semantics table (its `FilterExpr` matcher is the engine's existing query filter matcher); add `stampComputed` to the engine's write choke points mirroring Task 3's four sites (the engine already mirrors `stampUpdated`/defaults — locate those call chains and follow them); client-supplied computed values dropped/overwritten identically; engine push validation rejecting the Task-2 rule set (the `computed-push-validation` corpus case drives this).
- [ ] **Step 4:** Tests + corpus: add unit tests mirroring Task 1's semantics cases in vitest; run the full client gate — `cd ts-client && bun run typecheck && bunx vitest run` (includes the corpus runner — all four new cases green). README: document `.computed(...)` with an example.
- [ ] **Step 5:** Package hygiene: `bun run lint` (biome) AND `bun run fmt-check` — both are in pre-commit; run both. **Do not commit; do not run `make checkall` (orchestrator's job).**

### Task 7: rust-client mirror (PARALLEL with Tasks 6/8/9; after Task 5)

**Files:** `rust-client/src/schema.rs`, `rust-client/src/wire.rs` (schema wire type; the admin `ValueExpr` in `wire/admin.rs` stays as-is — it already mirrors the grammar), `rust-client/src/in_memory/{mod,migrate,validate}.rs`, `rust-client/tests/`, `rust-client/README.md`. NOTHING outside `rust-client/`.

- [ ] **Step 1:** Wire: `computed: BTreeMap<String, ValueExpr>` on the table wire type with `#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]`. The client needs a local `ValueExpr` for the schema path — either reuse `wire::admin::ValueExpr` if its serde shape matches the server's exactly (it should — five-way parity) via a type alias, or add a schema-local mirror; choose whichever keeps `wire.rs` consistent and byte-identical.
- [ ] **Step 2:** DSL: mirror `updated_at_field`'s declaration style on the table builder (`.computed(name, expr)`), with `ValueExpr` builder free functions or methods matching the rust-client's builder idiom.
- [ ] **Step 3:** Engine: `eval_value_expr` in `in_memory/` per the semantics table (reuse the engine's `FilterExpr` matcher from its query path); `stamp_computed` at the write choke points; push validation mirror for the corpus error case.
- [ ] **Step 4:** Tests: unit tests mirroring Task 1's cases + corpus runner green (`cargo test --manifest-path rust-client/Cargo.toml`); `cargo fmt --check` + `cargo clippy --manifest-path rust-client/Cargo.toml --all-targets -- -D warnings`. README example. **Do not commit.**

### Task 8: python-client mirror (PARALLEL with Tasks 6/7/9; after Task 5)

**Files:** `python-client/src/par_rt_db/schema.py`, `wire.py`, `in_memory/store.py` (+ migrate/validate modules), `python-client/tests/`, `python-client/README.md`. NOTHING outside `python-client/`.

- [ ] **Step 1:** Wire: `computed: dict[str, ValueExpr]` on the table dataclass/pydantic model mirroring `updated_at_field`'s optional-field style; `ValueExpr` as a tagged-union (Literal/Pydantic discriminated union or dataclass + `to_json`) matching the server serde shape exactly.
- [ ] **Step 2:** DSL: `.computed(name, expr)` on the table builder + expression helpers module if absent.
- [ ] **Step 3:** Engine: interpreter + stamping + push validation mirror (the engine's filter matcher is in its query path).
- [ ] **Step 4:** Tests: pytest mirroring Task 1's cases + corpus runner green. **Pre-commit runs BOTH `ruff check` AND `ruff format --check`** — run both (`uv run ruff check . && uv run ruff format --check .` from `python-client/`). README example. **Do not commit.**

### Task 9: swift-client mirror (PARALLEL with Tasks 6–8; after Task 5)

**Files:** `swift-client/Sources/ParRtDbClient/SchemaDsl.swift`, `Wire.swift` (schema wire), `InMemoryEngine.swift`, `InMemoryMigrate.swift`, `swift-client/Tests/`. NOTHING outside `swift-client/`.

- [ ] **Step 1:** Wire: `computed: [String: ValueExpr]?` on the table wire struct (`CodingKeys` + `encodeIfNil`-style omission mirroring `updatedAtField`); the migrate `ValueExpr` already exists in `Migrate.swift` — reuse or mirror it for the schema path so one grammar type serves both.
- [ ] **Step 2:** DSL: `.computed(name, expr)` builder method mirroring the existing field/updatedAt declaration style.
- [ ] **Step 3:** Engine: interpreter + stamping + push validation mirror (`InMemoryEngine.swift`'s filter matcher reused for Case).
- [ ] **Step 4:** Tests: XCTest cases mirroring Task 1's + corpus runner green (`make swift-client-test` or the package's test invocation — check the Makefile's swift targets from repo root). **Do not commit.**

### Task 10: Orchestrator closeout (main session only)

- [ ] Verify each batch with the real gates (run per package), then `make checkall` from the repo root (needs `ts-client` dist built: `make ts-client-build` first if dashboard typecheck fails).
- [ ] Commits (each with `timeout 600000` — pre-commit clippy exceeds the default): 1) plan doc `docs: add computed fields implementation plan` — commit this FIRST, standalone; 2) server `feat(server): declarative computed fields (ENH-028)` (Tasks 1–5 files + corpus + docs); 3) one commit per client `feat(<pkg>-client): mirror computed fields (ENH-028)`. Verify staging with `git show --stat HEAD` after each.
- [ ] Criteria check — per criterion, with evidence, then `kanban item check` each; `git push origin main` (repo is public, push-after-merge rule); mark card `done`; reconcile board.

## Self-Review (done at authoring time)

- **Spec coverage:** card sketch bullets → declaration (T2), push validation (T2/T4), write path incl. upsert/patchByQuery (T3), migrate interplay all three verbs (T4), mirrors four clients + engines (T6–9), examples (READMEs T5–9). Criteria 1 (T2+T4 tests + corpus), 2 (T3 tests + corpus), 3 (T4 + T6–9). Now alignment documented. Backfill covered. ✔
- **Placeholders:** every task names files, symbols, semantics, and test contents; mirror tasks carry per-language specifics + the shared semantics table lives in Global Constraints (content, not a pointer elsewhere). ✔
- **Type consistency:** `eval_value_expr`/`evalValueExpr` signatures, `stamp_computed`/`stampComputed`, `computed` wire key, `StaticKind` samples — consistent across tasks. ✔
