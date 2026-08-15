# Phrase/Operator Search + Snippets Design

**Date:** 2026-08-15
**Status:** approved design (board card `[FM-31]`, id `01a0034790fb79b2854afb5934ecbde1`)
**Effort:** S · server SQL + additive wire field, mirrored in all four clients (FEATURE_MATRIX gap #31)

## Problem

The `search` terminal matches via `plainto_tsquery`, which supports plain words
only. A quoted phrase (`"database notes"`) cannot be *required* — the words
match anywhere in the doc, any order, adjacency ignored; `or` cannot widen a
query; `-term` cannot exclude. And every search UI wants a highlighted snippet
of *why* a doc matched; today a client must fetch whole docs and slice/mark
them itself. Convex offers neither (par-rt-db leads).

## Goals

1. Switch `plainto_tsquery` → `websearch_to_tsquery(regconfig, $n)` on the
   text-matching paths: quoted phrases match adjacent words, the bare word
   `or` unions alternatives, `-term` excludes, plain terms stay AND. It is a
   pure superset for plain input — still injection-safe (the query text stays
   a `$n` bind; the regconfig literal is the same schema-validated,
   generated-column-matching one), so it ships unconditionally, not behind a
   flag. `hybridSearch` shares the tsquery builder and inherits the upgrade.
2. Plain-query behavior pinned equivalent by tests: existing search fixtures
   run unchanged (they are the pin), plus an explicit test that a plain
   multi-word query keeps AND semantics and today's ranking.
3. Optional `snippet: true` on the `search` terminal: each hit gains a
   `_searchSnippet` string rendered by `ts_headline` with server-fixed
   options (the client cannot pass headline options — bounds are the
   server's). Valid only with the default tsquery mode; `snippet: true` +
   `mode: "trgm"` → `BadRequest` (trgm matches substrings, not tsqueries, so
   there is no query tree to highlight).
4. Mirrored in all four clients (additive wire field + `.search()` builder
   opt) and the three in-memory harnesses (substring-excerpt stub).

## Non-goals

- No client-supplied headline options (`MaxWords`/`MinWords`/selectors stay
  server constants).
- No `snippet` on `hybridSearch`, `vectorSearch`, or `trgm` mode; no schema
  declaration change; no DDL/migration; no change to `filter`/`take`
  composition.

## Wire change

`SearchQuery` (server `query.rs` — the wire source of truth) gains one
optional field; `deny_unknown_fields` stays:

```rust
pub struct SearchQuery {
    pub index: String,
    pub query: String,
    pub filter: Option<FilterExpr>,      // (existing)
    pub mode: Option<SearchMode>,        // (existing, FM-30)
    pub snippet: Option<bool>,           // NEW — omit-when-none
}
```

Omitted/false ⇒ today's 4-column SQL, byte-identical. `true` ⇒ one extra
trailing SELECT column and a `_searchSnippet` field on each result doc.

## SQL

`tsquery` mode with `snippet: true` (binds unchanged in kind and order — `$1`
is the query text, reused by the headline; the option string is a server
constant, so nothing user-controlled is ever interpolated):

```sql
SELECT "id", "doc", "created_at", "version",
       ts_headline(['<lang>'::regconfig, ] concat_ws(' ', "f_title", "f_body"),
                   websearch_to_tsquery([<'lang>'::regconfig, ] $1),
                   'StartSel=<mark>, StopSel=</mark>, MaxWords=35, MinWords=15')
FROM "<schema>"."<table>"
WHERE "sv_<index>" @@ websearch_to_tsquery([...], $1) AND …
ORDER BY ts_rank("sv_<index>", websearch_to_tsquery([...], $1)) DESC, …
LIMIT $n
```

- Headline source text: `concat_ws(' ', "f_<field>", …)` over the search
  index's declared `fields` in declared order — NULL columns skip, and a doc
  that matched the tsvector necessarily has text in at least one of them.
- Options: `MaxWords=35, MinWords=15` (the PostgreSQL defaults, made
  explicit so the bound is server-owned and visible), `<mark>`/`</mark>`
  delimiters (renders directly in HTML/React without a transform step).
- The executor branch: the dispatch site re-derives `snippet` from the query
  (the same re-derive pattern the `paginate` arm uses) and passes it to the
  search execute tail, which decodes a 5-tuple instead of a 4-tuple and
  inserts `_searchSnippet` into each doc after `merge_doc`. `_`-prefixed
  keys are rejected at write time, so the additive field never collides.
- Subscriptions: `search` stays a table-level read-set; re-runs execute the
  same compiled query, so live snippets diff and push like any doc change.

## Harness behavior (in-memory stubs)

- Matching: the tsquery-mode approximation upgrades from token-AND to
  websearch semantics — quoted phrases match as adjacent-word sequences
  (case-insensitive `contains` on normalized text), bare `or` unions
  alternative groups, `-term` excludes; ranking stays the existing
  deterministic approximation.
- Snippet: a plain excerpt stub — a window of ≤35 words around the first
  matched term (or the doc's leading words when nothing marks cleanly),
  wrapping matched terms in the same `<mark>` delimiters. Close enough for
  match/no-match and shape parity; never byte-compared to Postgres output.

## Testing

- Phrase: `"database notes"` matches only the doc with the words adjacent;
  the same words unquoted still match both (AND) — the equivalence pin,
  alongside every existing fixture running unchanged.
- `or` unions; `-term` excludes; language-bearing indexes (regconfig path)
  take phrases too.
- Snippet: `_searchSnippet` present and `<mark>`-wrapped when on, absent
  when omitted and when `false`; composes with `filter` and `take`;
  `snippet:true` + `mode:"trgm"` → 400; word-count bound holds.
- Wire corpus: `queries` gains a phrase/operator entry and a `snippet:true`
  entry (round-tripped by every client's corpus test).
