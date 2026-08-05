# ENH-007 — Vector distance metric options (L2 / inner-product)

Date: 2026-08-05 · Status: implementing · Source: `ENHANCEMENTS.md` ENH-007

## Goal

Let a vector index declare its distance metric (`cosine` default | `l2` | `ip`) instead
of the hardcoded HNSW `vector_cosine_ops` / cosine `<=>`. The chosen metric compiles to the
matching pgvector opclass and distance operator. Backward compatible: existing schemas carry
no metric and behave exactly as today (cosine).

## Wire contract (the one design decision)

Add an optional `metric` field to `VectorIndexSpec`, carried in the same camelCase wire shape
the four implementations already share:

```jsonc
{ "dimensions": 8, "filterFields": ["k"], "metric": "l2" }   // metric omitted when cosine
```

- Values: `"cosine"` | `"l2"` | `"ip"` (lowercase). Unknown value → schema rejected (400).
- Default `cosine`; **omitted on the wire when cosine** → byte-identical to pre-change schemas.
- The metric lives on the **index**, not the query. The `vectorSearch`/`hybridSearch` query DSL
  is unchanged; the server resolves the operator from the resolved index's metric.

pgvector mapping:

| metric  | opclass           | operator | notes                                   |
|---------|-------------------|----------|-----------------------------------------|
| cosine  | `vector_cosine_ops` | `<=>`  | cosine distance (today's behavior)      |
| l2      | `vector_l2_ops`     | `<->`  | Euclidean distance                      |
| ip      | `vector_ip_ops`     | `<#>`  | negative inner product; ascending order = most-similar-first, consistent with the other two |

## Server (source of truth) — `server/src/`

1. **`schema.rs`** — add `DistanceMetric` enum (`#[serde(rename_all = "lowercase")]`,
   `Default = Cosine`) with `opclass()`, `distance_op()`, `is_cosine()`. Add
   `#[serde(default, skip_serializing_if = "DistanceMetric::is_cosine")] metric: DistanceMetric`
   to `VectorIndexSpec`. No new validation needed (serde rejects unknown variants; friendly
   400 is already how malformed schema fields surface).
2. **`ddl.rs:300`** — `USING hnsw ("{v_col}" <opclass>)` via `vec_spec.metric.opclass()`. Update
   the surrounding comment ("cosine index" → "metric-chosen index"). The additive-change guard
   at `ddl.rs:129` (`new_index.vector != old_index.vector`) already compares the whole spec, so a
   metric change is correctly treated as a breaking index change.
3. **`query.rs:2143`** — `ORDER BY "{v_col}" <op> ${qvec_ph}::vector` via
   `vec_spec.metric.distance_op()` (vec_spec is in scope at 2030).
4. **`query.rs:2325`** (hybrid) — same operator threading from the hybrid-resolved `vec_spec`
   (in scope at 2239); update the 2177 doc comment.
5. **`server/tests/vector_test.rs`** — add an `l2` and an `ip` vector index; assert nearest-neighbor
   ordering under each metric differs from cosine in the expected direction (sanity), and that
   `vectorSearch` over them returns the right ranked rows.

Verify: `make -C server fmt-check`, `make -C server lint` (clippy `-D warnings`), and the
server test subset before touching clients.

## Client mirrors (parallel, disjoint files)

Each mirrors the wire type and its schema builder; the query DSL is unchanged.

- **ts-client** (`protocol.ts` `VectorIndexSpec` + `metric?`; `schema.ts` `vectorIndex(...)`
  optional `metric` arg; export `DistanceMetric` type) + a `tests/` assertion that metric round-trips.
- **rust-client** (`schema.rs` mirror enum + field + builder) + a `#[test]`.
- **python-client** (`schema.py` `VectorIndexSpec.metric: Literal[...]` with omit-when-cosine
  serializer matching the existing `filterFields` idiom; `vector_index` builder arg) + a pytest.

## Closeout

- Flip the ENH-007 box in `ENHANCEMENTS.md`; note client-mirror status in `FEATURE_MATRIX.md`
  if it enumerates vector features.
- Full `make checkall` (all five packages) before commit.
- Commit `feat: ENH-007 vector distance metric options (cosine/l2/ip)`; move the card to done.
