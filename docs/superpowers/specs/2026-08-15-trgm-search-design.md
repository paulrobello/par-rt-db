# Substring / Autocomplete Search via pg_trgm Design

**Date:** 2026-08-15
**Status:** approved design (board card `[FM-30]`, id `01a003478c74725298b559da321e3e50`)
**Effort:** S–M · server wire+DDL change, mirrored in all four clients (FEATURE_MATRIX gap #30)

## Problem

The `search` terminal compiles to `tsvector @@ plainto_tsquery(...)` ranked by
`ts_rank`. Full-text matching is word/stem-based: `"convex"` does not match
`"convexity"`, short strings and infixes (`search_indexes` inside
`wire_corpus_test.rs`) never match, and there is no typo tolerance. Autocomplete
UIs need prefix/substring hits. This is the most common reason to drop to the
psql escape hatch today — Convex serves this with `search_index` prefixes; we can
serve it Postgres-natively with trigram matching, which Convex cannot do at all.

## Goals

1. Additive `mode` on the `search` terminal: `"tsquery"` (default — today's
   behavior, byte-identical) | `"trgm"` — substring matches via `ILIKE`, ranked
   by trigram `similarity()`.
2. `trgm` composes with the existing `filter` and `take`, exactly as `tsquery`
   mode does (same validation surface in `execute_query`).
3. Trigram GIN indexes (`gin_trgm_ops`) over every search index's text fields,
   created idempotently at schema push **and backfilled for existing search
   indexes** on the next push of an unchanged schema.
4. Mirrored in all four clients (wire type + `.search()` builder) and the three
   in-memory harnesses (substring match, approximate similarity ranking).

## Non-goals

- No change to `hybridSearch` (fuses tsquery FTS + vector; trigram fusion is out
  of scope), `vectorSearch`, or the btree index surface.
- No new schema declaration: a search index's existing `fields` are the trigram
  surface; there is no separate `trgm` index kind to declare. (Tradeoff below.)
- No similarity *threshold* knob: matching is pure substring (`ILIKE '%q%'`);
  `similarity()` only orders results. No `pg_trgm.similarity_threshold` SET.

## Wire change

`SearchQuery` (server `query.rs` — the wire source of truth) gains one optional
field; `deny_unknown_fields` stays:

```rust
#[derive(...)] #[serde(rename_all = "lowercase")]
pub enum SearchMode { #[default] Tsquery, Trgm }

pub struct SearchQuery {
    pub index: String,
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<FilterExpr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<SearchMode>,
}
```

`None` = `tsquery`, so an omitted `mode` deserializes to exactly today's
behavior, and clients that never set it serialize byte-identically to today
(skip_serializing_if in every mirror — the wire-corpus drift rule). `"tsquery"`
is accepted on the wire but never emitted by the server or clients. A unknown
mode string is rejected by serde (`deny_unknown_fields` + enum) → `BadRequest`.
Version skew: an old server rejects `mode` as an unknown field, but clients only
send it when the caller explicitly opts in, so existing code paths can't hit the
skew.

## DDL

1. `CREATE EXTENSION IF NOT EXISTS pg_trgm` — at the two sites that already do
   this for `vector`: `db.rs` database creation and `ddl::push_schema`'s tx (the
   push-site CREATE covers databases created before this feature). `pg_trgm` is
   a trusted extension; same privilege story as `vector`.
2. For every search index on every additive push
   (`apply_schema_additive`), after the existing new-index tsvector DDL:

   ```sql
   CREATE INDEX IF NOT EXISTS "tg_<table>_<index>"
     ON "<schema>"."<table>" USING gin ("f_<a>" gin_trgm_ops, "f_<b>" gin_trgm_ops)
   ```

   Physical name follows the `i_`/`s_`/`v_` conventions (`tg_` prefix, lowercased
   table+index). Deliberately **outside** the "new indexes only" branch and
   guarded by `IF NOT EXISTS`: this is the backfill — an existing deployment's
   already-declared search indexes get their trigram GIN on the next schema
   push, no operator action. A `trgm` query on a table whose push predates this
   still works without the index (seq-scan `ILIKE`), just unaccelerated.
3. Drop path (`reconcile_schema_destructive`'s `drop_indexes` loop): also
   `DROP INDEX IF EXISTS "tg_<table>_<index>"` beside the existing `i_` drop, so
   removing a search index from a schema removes both GIN indexes.

**Tradeoff (documented, accepted):** every search index now carries a second GIN
index (trigram) alongside its tsvector GIN — roughly doubling search-index
storage and adding write amplification on the indexed text fields, paid by every
search index whether or not `trgm` queries ever run. Chosen over an opt-in
schema flag to keep the wire/DSL additive-only and the query surface
schema-independent; revisit if index bloat shows up in practice.

## Query compilation (`compile_search`, trgm arm)

Same overall shape as the tsquery arm — same bind discipline, same
filter/owner/authorize `extra` composition, same `LIMIT` — differing only in
the match predicate and rank expression. For a search index with fields
`a`, `b`:

```sql
SELECT "id","doc","created_at","version" FROM "<schema>"."<table>"
WHERE ("f_a" ILIKE $2 OR "f_b" ILIKE $2) {extra}
ORDER BY GREATEST(similarity("f_a",$1), similarity("f_b",$1)) DESC,
         "created_at" DESC, "id" DESC
LIMIT $n
```

- `$1` = the raw query text (feeds `similarity`); `$2` = `'%' + query + '%'`
  (the ILIKE pattern, built server-side and bound — nothing is interpolated).
  Both lead the bind vector as `EqBind::Text`, then filter/owner/authorize
  binds, then `LIMIT` — mirroring the tsquery arm's bind order.
- `ILIKE` = case-insensitive substring; `%`/`_` typed by the caller act as
  wildcards (substring/autocomplete semantics; harmless — the pattern is bound,
  never interpolated).
- `GREATEST(similarity(...))` per indexed field: a doc ranks by its best-matching
  field. A row in the result set always ILIKE-matched some field, so at least
  one `similarity` argument is non-NULL; an all-NULL-field row cannot match and
  never reaches the ORDER BY. `created_at`/`id` tiebreaks keep ordering
  deterministic, matching the tsquery arm.
- The tsvector column (`s_<index>`) is not referenced by trgm mode; the
  generated column still exists and `tsquery` mode is untouched.
- Empty/whitespace query text is rejected before compilation (existing check,
  both modes).

Subscriptions are unaffected: `search` read-sets stay table-level
(over-approximate) by design, in both modes.

## Client mirrors

All three SDKs mirror the wire (`SearchMode` type, optional `mode` with
omit-when-unset serialization) and their `.search()` builder gains an opt-in:

- **ts-client**: `.search(index, query, opts?: { filter?, mode?: "tsquery" | "trgm" })`.
- **rust-client**: `SearchOpts` gains `mode: Option<SearchMode>` (builder-style,
  alongside the existing filter opt).
- **python-client**: `search(index, query, *, filter=..., mode=...)`.

In-memory harnesses (ts `executeSearchTerminal`, rust `execute_search_terminal`,
python `_execute_search_terminal`): `trgm` = case-insensitive substring match
over the index's fields (terms contain the query as a substring), ranked by an
approximate similarity (simple trigram-set overlap is fine — approximate ranking
is acceptable, substring *matching* is not). Default mode path byte-identical.

## Testing

- **Server** (`server/tests/search_test.rs`): infix/autocomplete matches
  (`"conv"` finds `"convexity"` and `"convex"` — documents tsquery mode cannot
  match); case-insensitivity; similarity ordering (closer match ranks first);
  composition with `filter` + `take`; omitted mode = tsquery behavior
  (regression covered by the existing suite staying green); explicit
  `mode:"tsquery"` accepted; invalid mode → 400; trigram GIN present after
  push (`pg_indexes`), idempotent on re-push, dropped by destructive reconcile.
- **Clients**: builder emits `mode` only when set (wire-corpus fixture updated
  in all four corpus tests); harness trgm match/rank tests per client.

## Docs

FEATURE_MATRIX #30 flips to ✅ (server + all clients) with the trigram
index-size tradeoff noted; server README's search section and the three client
READMEs' `.search()` docs gain the `mode` opt-in.
