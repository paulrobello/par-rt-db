# ENH-006 — Configurable full-text search language

Date: 2026-08-05 · Status: done · Source: `ENHANCEMENTS.md` ENH-006

## Goal

Let a full-text search index declare an optional `language` (a Postgres
`regconfig` name like `english` / `simple` / `spanish`) instead of the hardcoded
`to_tsvector('english', …)`, so non-English corpora get correct stemming and
stop-words. Backward compatible: existing schemas carry no `language` and behave
exactly as today (`english`).

## Wire contract

Add an optional `language: Option<String>` to `IndexDef`. Wire key is `language`
(camelCase == the Rust field name). Values are lowercase `regconfig` names.
**Omitted on the wire when `None`** → byte-identical to pre-change schemas. The
language lives on the **index**, not the query — the `search`/`hybridSearch` DSL
is unchanged; the server resolves the regconfig from the resolved index.

The tsvector column and the query tsquery must share a regconfig for `@@` to
match correctly, so every search path builds both from the same index `language`:

- DDL generated column: `to_tsvector('<lang>'::regconfig, …)`
- query tsquery: `plainto_tsquery('<lang>'::regconfig, $1)`

## Server — `server/src/`

1. **`schema.rs`** — add `#[serde(default, skip_serializing_if = "Option::is_none")]
   pub language: Option<String>` to `IndexDef`; add `is_valid_regconfig`
   (`^[a-z][a-z0-9_]*$`, ≤63). In `validate_structure`: reject `language` on a
   non-search index, and reject a malformed regconfig (format gate; placement +
   format only — no DB here).
2. **`ddl.rs`** — `validate_search_languages` existence-checks every declared
   `language` against `pg_ts_config` (batched `ANY($1)`, 400 on miss), called in
   `push_schema` after `schema.validate()`. The generated column uses
   `index.language.as_deref().unwrap_or("english")` interpolated as a literal
   (format-validated ⇒ injection-safe; bind params are not allowed in a STORED
   generated expression). `detect_destructive_changes` rejects a language change
   on an existing search index as a breaking change — Postgres cannot alter a
   STORED generated expression in place.
3. **`query.rs`** — `plainto_tsquery_sql(language, ph)` renders the tsquery
   fragment (with/without the regconfig), used in `execute_search` and the hybrid
   search CTE (both the `@@` match and the `ts_rank` argument).
4. **`tests/search_test.rs`** — `simple` matches an exact word; `simple` does not
   stem a plural while `english` does (the behavioral proof); unknown language →
   400; `language` on a btree index → `SchemaViolation`; language change → 400.

## Client mirrors (parallel, disjoint files)

Each mirrors the wire type and its schema builder; the query DSL is unchanged.

- **ts-client** — `IndexJson.language?: string`; `searchIndex(name, fields,
  language?)` conditional-spread include; schema-test round-trip.
- **rust-client** — `IndexDef.language: Option<String>` (+ `language: None` at
  every literal); `search_index` builder gains an optional language (mirroring
  `vector_index`/`metric`); `#[test]` round-trip.
- **python-client** — `IndexDef.language: str | None = None` (+ omit-when-None in
  the wrap serializer); `search_index(..., language=None)` builder; pytest.
- **dashboard** — `SchemaPage` FTS tag surfaces the language: `FTS·<lang>` when
  declared, bare `FTS` otherwise (mirrors the `VEC·<metric>` tag); two tests.

## Closeout

- Flip the ENH-006 box in `ENHANCEMENTS.md`; note the regconfig threading +
  client-mirror status in `FEATURE_MATRIX.md` row #11.
- Full `make checkall` (all five packages) before commit.
- Commit `feat(search): ENH-006 configurable full-text search language`;
  move the card to done.
