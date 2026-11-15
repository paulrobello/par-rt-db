# ARC-202 — Decomposition analysis: `rust-client/src/in_memory/query.rs`

**Source**: Fable audit 2026-11-13, finding ARC-202 criterion 1. Analysis date 2026-11-14 at HEAD 33687c5; evidence from parsight (`audit_repository` decomposition + cyclomatic complexity) plus direct reads of the file. ARC-202 criterion 1 requires a decomposition analysis with a chosen seam; this document is that analysis. The no-codegen mirror contract stands: the hand-maintained engine is not collapsed, codegen'd, or deduplicated across the seven clients — the goal is only that the next behavior change lands in one module instead of shotgun-editing 2,798 lines.

## 1. Current structure map

The file is one flat module with nine dense regions and no submodules. Its complexity is breadth, not depth: parsight's repo-wide `most_complex_functions` top 15 contains **zero** functions from this file. The worst offenders here sample at complexity 26–30 (`execute_search_terminal` 30, `prepare_scan` 29, `aggregate_multi_op` 26, all "Critical" band), while the file's headline 474 is the *sum* across ~65 symbols that each sit in the 3–30 range. A single-function rewrite would miss the problem; the problem is that nine separable concerns share one file and one namespace.

Regions, top to bottom:

| Lines | Region | Contents | Notes |
| --- | --- | --- | --- |
| 10–1111 | `impl InMemoryRtDbClient` — dispatch + terminals | `run_query` (42), `execute_query` (76–199, terminal dispatch), `fetch_filtered_rows` (200), `execute_get_terminal` (274), `execute_vector_search_terminal` (299), `execute_hybrid_search_terminal` (274–405 group), `execute_search_terminal` (406–600, 194 lines — the file's largest function: it interleaves six BAD_REQUEST compile guards, index resolution, tsquery/trgm mode split, scoring, ranking, snippets, and take-cap in one body), `execute_count_terminal` (601), `execute_distinct_terminal` (612), `execute_aggregate_terminal` (667), `aggregate_multi_op` (703–870), `aggregate_legacy` (871–1006), `legacy_count` (1007), `execute_collect_terminal` (1077) | The dispatch enumerates the DSL terminals (`get` / `vectorSearch` / `hybridSearch` / `search` / `count` / `distinct` / `aggregate` / `paginate` / `collect`) as sequential `if let` arms in `execute_query`. Filter-comparison operator evaluation is NOT here — it already lives in `validate.rs` (`eval_filter_expr`, `matches_filter`), a prior extraction that worked |
| 1113–1232 | Projection | `PROJECTION_SYSTEM_FIELDS`, `validate_projection`, `map_result_docs`, `project_result`, `diff_canonical` | Pure functions over the result envelope |
| 1234–1518 | Lookups | lookup caps (`MAX_LOOKUPS`, `MAX_JOINED_ROWS`, …), `validate_lookups`, `is_array_lookup`, `lookup_docs_mut`, `expand_lookups` | Pure + doc-tree mutation; no `self` except via passed store refs |
| 1520–1736 | Scan planning | `ScanPlan`, `prepare_scan` (113 lines, complexity 29), `sort_filtered_rows`, `execute_paginate_terminal`, `query_clauses`, `check_query_combinations` | The plan/execute boundary already exists as a data seam (`ScanPlan` with `typed_eq`/`index_def`) |
| 1738–1968 | Search machinery | `SearchOperand`, `WebsearchQuery`, `parse_websearch_query`, `search_operand_matches`, `positive_lexemes`, `fts_stringify`, `fts_tokens`, `tsquery_score`, `search_all` field-text helpers, `attach_search_snippets` | Pure websearch/fts parsing, matching, scoring, snippets |
| 1970–2184 | Index typing/encoding (pub API) | `type_tag`, `PgType`, `IndexedType`, `index_column_type`, `coerce_index_value`, `compare_index_values` | All `pub`, already re-exported through `in_memory/mod.rs`; zero `self` access — pure leaf code |
| 2185–2257 | Aggregate math | `apply_aggregate`, `f64_aggregate`, `numeric_value` | `apply_aggregate` is pub re-exported; pure |
| 2258–2295 | `merge_doc` (pub) | StoredRow → merged doc Value | Pure; parsight's clusters group it with projection, not encoding |
| 2297–2729 | Cursors + ranked pagination | `dir_order`, `SortCol`, `paginate_result`, `validate_cursor_values`, `is_after_cursor`, `keyset_is_after`, `RankedRow`, `rank_value`, `sort_ranked_rows`, `paginate_ranked`, `validate_ranked_cursor_values`, `sort_value`, `require_index` | Keyset cursor encoding/decoding shared by plain and ranked paging |
| 2731–2798 | Grouping | `finish_groups` | Aggregate grouping tail |

parsight's decomposition (modularity 0.56, 251 co-change pairs) independently derives six structural clusters that align almost one-to-one with those regions — dispatch+scan, aggregate+index-typing, search+ranked, projection+lookups+`merge_doc`, cursor/pagination — and flags exactly the cross-region re-use (e.g. `require_index` and `coerce_index_value` serving both scan and aggregate clusters) that a module tree needs to mediate.

**What the largest functions interleave.** `execute_search_terminal` (194 lines) mixes compile-phase guard ladder, index lookup, and two complete evaluation modes in one body. `aggregate_multi_op` (167) and `aggregate_legacy` (135) keep two aggregate semantics alive side by side. `prepare_scan` (113) is heavy because it validates index typing while building `ScanPlan`. None is a candidate for function-level surgery; all are candidates for *containment* inside a module that owns their concern.

## 2. The chosen seam: convert `query.rs` into a `query/` submodule tree

One move, following the QA-108 precedent this file's own header records ("Pure file movement — behavior unchanged"): split the file into a directory module whose children own one region each, with `query/mod.rs` preserving the current public surface byte-for-byte. No signatures change, no codegen, no trait abstraction, no shared crate. The structure follows parsight's clusters so co-change partners land together:

```
rust-client/src/in_memory/query/
  mod.rs          run_query + execute_query + fetch_filtered_rows + get/vector/hybrid terminals
                  (the dispatch impl block, lines 10–405), plus re-exports of every child
  scan.rs         query_clauses, check_query_combinations, ScanPlan, prepare_scan,
                  sort_filtered_rows                          (1520–1736 region)
  projection.rs   projection + lookups + merge_doc            (1113–1518 + merge_doc)
  aggregate.rs    count/distinct/aggregate/legacy methods (impl block) + apply_aggregate,
                  f64_aggregate, numeric_value                (601–1076 + 2185–2257)
  search.rs       execute_search_terminal (impl block) + SearchOperand/WebsearchQuery/
                  parse/match/fts/tsquery/snippets            (406–600 + 1738–1968)
  cursor.rs       execute_paginate_terminal + dir_order, SortCol, paginate_result,
                  cursor validation, RankedRow + ranked paging, sort_value,
                  finish_groups                               (1698–1736 + 2297–2798)
  index.rs        type_tag, PgType, IndexedType, index_column_type, coerce_index_value,
                  compare_index_values                        (1970–2184)
```

Each child carries `use super::*;` and — where the region contains `InMemoryRtDbClient` methods — its own `impl InMemoryRtDbClient` block, exactly the pattern the file already uses as a second impl block today. `query/mod.rs` re-exports (`pub use index::*;` etc.) so `in_memory/mod.rs`'s two import lines and the crate's public names stay untouched.

**What moves:** every line of the current file, redistributed. **What stays:** `InMemoryRtDbClient` remains defined in `in_memory/mod.rs`; `run_query`'s signature and the `execute_query` dispatch order are frozen (dispatch order is semantics — the compile-cascade error ordering the file's comments pin to the server). Cross-module helpers (`collect_index_key`, `diff_canonical`, `require_index`) keep their `pub(super)` visibility reachable at the `query::` path via the mod.rs re-exports.

**First mechanical extraction step, if someone starts tomorrow:** convert `query.rs` to the directory form with `mod.rs` holding the file verbatim, then perform one leaf move — the index-encoding block (lines 1970–2184: `type_tag`, `PgType`, `IndexedType`, `index_column_type`, `coerce_index_value`, `compare_index_values`) into `query/index.rs` — re-exported with `mod index; pub use index::*;`. This is the ideal opener: the block is already all-`pub`, pure, and `self`-free, so the move cannot change behavior, the re-export makes the eight-item public surface (`IndexedType, PgType, apply_aggregate, coerce_index_value, compare_index_values, index_column_type, merge_doc, type_tag` per `in_memory/mod.rs`) resolve unchanged, and it forces the directory conversion that every later move reuses. Verify with `cargo check -p par_rt_db_client` and the in-memory test suite, commit. Subsequent moves in coupling order: `search.rs` (region 1738–1968 plus the search terminal — co-change cluster "positive_lexemes" proves search machinery and ranked rows travel together), then `aggregate.rs`, `cursor.rs`, `projection.rs`, `scan.rs`.

## 3. Blast radius

**Callers.** The engine's only true entry point is the method `InMemoryRtDbClient::run_query(&Query) -> Result<Value, RtDbError>`. parsight's `find_callers` for the bare name is noisy (it also matches the Python `run_query` and a server test helper), but the Rust surface is enumerable directly: 92 `run_query(` call sites outside the file itself, all in `rust-client/src/in_memory/tests/*.rs` (11 test files) plus the `query()` wrapper in `in_memory/mod.rs`. Every one goes through the impl method, which does not move or change signature — the module tree is invisible to all of them. The wire-corpus runner (`rust-client/tests/wire_corpus.rs`) drives the engine through the same public client API, so it is likewise unaffected by layout.

**What must keep resolving.**

- `in_memory/mod.rs` lines 2710–2721: `use query::{collect_index_key, diff_canonical, require_index};` (private helpers) and `pub use query::{IndexedType, PgType, apply_aggregate, coerce_index_value, compare_index_values, index_column_type, merge_doc, type_tag};` (public surface). Both keep working untouched as long as `query/mod.rs` re-exports the same names.
- Any code reaching the pub items via `par_rt_db_client::in_memory::*` — the re-export chain preserves it.

**What breaks or must move:**

1. Visibility adjustments: items moved into child modules that are currently file-private but consumed across regions (e.g. `require_index`, `dir_order`, `fts_*` helpers) need widening to `pub(super)` within the `query` tree. This is the main mechanical risk — expect the compiler to enumerate every one.
2. Tests: nothing moves. The `in_memory/tests/` tree targets the client API, not file layout. Corpus and golden-vector tests are the behavior net for the whole program of moves.
3. No `pub use` outside `in_memory` changes; no Cargo.toml change (same crate, same feature set).

**Why this seam beats the alternatives:**

- **Same-file impl-block reorganization** (splitting the impl into labeled blocks in one file): zero navigability gain; the shotgun-edit problem is file-shaped, not block-shaped.
- **Function-level extraction** (carve up `execute_search_terminal`): shuffles complexity without establishing boundaries; the search terminal interleaves compile guards with two evaluation modes whose co-change partner is the fts machinery — splitting them across files without the module tree would worsen the coupling.
- **Plan/execute split only** (extract `ScanPlan`/`prepare_scan` alone): the plan layer is small (113 lines) and its `typed_eq`/`index_def` outputs feed every terminal; isolating it alone creates a one-way dependency without reducing the file.
- **Cross-engine consolidation/codegen** (hoist shared engine code into a common crate or generate mirrors): explicitly out — the no-codegen mirror contract is deliberate, and ARC-202's premise is that the hand-maintained engines stay.

The chosen seam is reversible per-move, gate-verifiable at every step, and each step is a "pure file movement" commit the corpus net already knows how to absorb.

## 4. What must NOT change

- **The no-codegen mirror contract.** The seven engines are hand-maintained, byte-mirrored by the corpus; no shared engine crate, no code generation, no collapsing of `query.rs` into less code. The decomposition reorganizes one engine's file layout only.
- **The wire-corpus authoring rule remains the enforcement mechanism** (ARC-202 criterion 3 confirmed): `wire-corpus/README.md`, "The authoring rule" (§ lines 91–99) — "Every change to server behavior that alters a query result, a step result, an error code, or document visibility ships with at least one semantics case exercising the new behavior — in the same change," and "A semantics change without a fixture fails review." Note the scope in the rule's own words: it binds *behavior* changes. A pure-move decomposition commit under this seam changes no query result, step result, error code, or visibility, so it correctly ships with no new corpus case; the first behavior change landed after the split must be the one that adds its case, in the same commit, and it lands in the single module that now owns that terminal.
- **Frozen semantics anchors**: `run_query`'s compile-cascade ordering (lookup validation → projection validation → execution → lookup expansion → projection, per the comments pinned to the server's `compile_query` order) and the `execute_query` terminal-dispatch precedence. Comments in the file cite server functions as the source of truth; moves must carry those comments with their code intact.
