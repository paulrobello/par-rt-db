# ENH-053 — Extend the spec-table pattern to the remaining semantics rule families

**Consumer**: `/enhancement-all` (Opus 5 implementer). **Source**: Fable audit 2026-11-13, finding ARC-202/QA-002. **Predecessor**: ENH-052 (landed spec-table machinery).

## Goal

Shift more of the seven client mirrors' shared semantics from reimplemented code to data: extend ENH-052's generated spec-table pattern (server decision functions run at gate time produce JSON tables; clients assert against them) to the rule families that still have none. A client port then becomes "make the table assertion pass" instead of "re-derive the semantics by reading the server".

## Current-state context

ENH-052 already generates these tables in the gate (see `make checkall` legs writing `spec-table` temp JSON files: index-column-types, computed-stamping, aggregate-terminals, pagination, validation). Each is produced by running the server's own decision functions — the tables cannot drift from the server by construction. The clients assert against the checked-in copies.

Rule families that still have NO spec table (the remaining drift surface):

1. **Filter/comparison operator semantics** — operator set per field type, null handling, cross-type coercion rejections. Server: `server/src/query/` (filter compilation and evaluation).
2. **Patch/merge semantics** — nested-object merge vs replace, array behaviors, null-deletion. Server: `server/src/txn/` patch application + `validate_value`.
3. **Counter/atomic-op semantics** — adjustCounter clamping, overflow, non-numeric rejection. Server: `server/src/query/terminals.rs` counter terminal.
4. **Search-mode selection semantics** — tsvector vs trgm mode selection, `mode:` validation, BAD_REQUEST cases. Server: `server/src/query/search.rs`.
5. **Cursor/pagination encoding** — already partially covered by the pagination table; the cursor *encoding* (opaque token shape, sort-tiebreaker) is not. Server: `server/src/query/pagination`.

## Implementation steps

1. **Survey**: read `scripts/` (or wherever the gate generates the existing tables — follow the existing `spec-table` temp-file legs in the root `Makefile`) to learn the table-generation harness. Do not invent a new mechanism; extend it.
2. **Add generators**, one per family, following the existing generator shape exactly: import the server's decision function, enumerate the inputs (fixed vocabularies per family), emit JSON. Name them `filter-operators.json`, `patch-semantics.json`, `counter-ops.json`, `search-modes.json`, `cursor-encoding.json`.
3. **Add client assertion tests**, one per family per language, following how the existing tables are asserted (grep a checked-in table filename in each client's tests to find the pattern). Seven runners: ts-client, rust-client, python-client, go-client, swift-client, kotlin-client (+ dashboard where it embeds an engine — check first; if the dashboard does not assert existing tables, skip it here too).
4. **Wire into the gate**: each family's generator runs in the same `make checkall` leg as the existing tables; the checked-in JSONs live beside the existing ones.
5. **Ship tables + assertions in ONE commit per family** (server generator + JSON + all client assertions together) so no runner is left behind mid-family.

## Files to touch

- The existing spec-table generator harness (follow it from the `make checkall` spec-table legs).
- 5 new generator functions + 5 checked-in JSON tables.
- Client test files per language per family (mirroring the existing table-assertion test layout).
- `Makefile` (only if a new leg is unavoidable — prefer extending the existing leg).

## Verify

- `make checkall` — green with the new generator legs running.
- Break-test: temporarily flip one value in a checked-in JSON and confirm the corresponding client test fails (proves the assertion reads the table), then restore.
- `grep -c` the new table filenames across the seven client test trees — every runner that asserts existing tables asserts the new ones.

## Rollback

Each family lands as one atomic commit; revert the single commit. The tables are additive (no server behavior change), so rollback carries zero wire risk.

## Acceptance criteria (for the card)

- All five families have generated tables produced by the server's own decision functions.
- Every runner that asserts existing tables asserts the five new ones.
- `make checkall` green with the break-test demonstrated once.
