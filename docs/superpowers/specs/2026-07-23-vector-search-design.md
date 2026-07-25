# Vector Search (#17) — Design

**Date:** 2026-07-23
**Status:** Implemented — live and verified in production on `rtdb.pardev.net` as of 2026-07-25 (FEATURE_MATRIX #17 ✅). `pgvector/pgvector:pg17` (vector 0.8.5) in both dev and prod. Mirrored end-to-end: server + `ts-client` + `rust-client` ship `t.vector(n)` / `vectorIndex(...)` / `.vector_search(...)`; `python-client` ships `t.vector(n)` / `vector_index(...)` / `TableQuery.vector_search(...)`. Current source of truth: `server/src/query.rs` (terminal), `server/src/schema.rs` (`FieldType::Vector`, `VectorIndexSpec`), `server/src/ddl.rs` (`vector(N)` column + HNSW index), and FEATURE_MATRIX #17.
**Parent:** `docs/superpowers/specs/2026-07-21-par-rt-db-design.md`, `FEATURE_MATRIX.md` row #17
**Precedent:** Full-text search (#11) — this feature mirrors its shape end to end.

## 1. Goal & scope

Add pgvector-backed semantic similarity search as a reactive `Query` terminal
(`vectorSearch`), so apps can store pre-computed embeddings on documents and
query nearest neighbors by cosine distance.

par-rt-db has **no embedded JS runtime and no per-app server code**, so — like
Convex, whose `vectorSearch` runs in an action — **embeddings are supplied by the
caller, never generated server-side.** An app (or an external worker with a
machine token) computes embeddings with whatever model it likes, writes them
onto documents, and queries with a query vector. The server only stores vectors
and runs the similarity query.

This is a Postgres-native fit: no external service, no dedicated search
component — `pgvector` does in the database what Convex needs a separate index
component for. Per the standing decision, par-rt-db is vendor-locked to Postgres
(`bytea` for blobs, `pgvector` for search); no disk/S3/object-store, no storage
trait abstraction.

### In scope (v1)

- New `Vector` field type (fixed dimensions).
- Vector index declared on a table (vector field + dimensions + optional
  scalar `filterFields`).
- `vectorSearch` query terminal: reactive, top-K by cosine distance, optional
  eq-filter over declared `filterFields`.
- Server: pgvector extension, schema validation, DDL, write-path column
  maintenance, read-path execution.
- Mirrored to **all three clients** (server, ts-client, rust-client).
- Deployment: switch dev + prod Postgres images to include pgvector.
- Flip `FEATURE_MATRIX.md` row #17 ❌ → ✅.

### Non-goals (v1, may follow)

- Server-side embedding generation (architecture forbids it — permanent).
- Distance metrics other than cosine (`<=>`). L2 (`<->`) / inner-product (`<#>`)
  can be added later if needed; YAGNI now.
- Composition with `take`/`order`/`paginate`/`count`/the db-side `filter()` DSL.
  `vectorSearch` carries its own `limit` and a constrained eq-`filter` map.
- IVFFlat indexes, tuned HNSW parameters (`m`, `ef_construction`). v1 uses
  pgvector defaults.
- Returning a similarity `_score` alongside docs. (Easy to add later; omitted to
  keep the result shape identical to every other terminal — `Docs`.)

## 2. Wire contract (load-bearing)

Three implementations of this protocol — `server/src/protocol.rs` +
`server/src/schema.rs` + `server/src/query.rs`, `ts-client/src/protocol.ts`, and
`rust-client/src/wire.rs` — must stay byte-identical. Tags and field names are
non-uniform and deliberate; match exactly.

### 2.1 New field type `Vector`

Add a `FieldType` variant:

```rust
pub enum FieldType {
    // …existing…
    Vector { dimensions: u32 },
}
```

Wire: `{"type":"vector","dimensions":1536}` (`serde(tag="type", rename_all="camelCase")`
on `FieldType`, so the variant serializes `{"type":"vector","dimensions":N}`).

- Stored in the document's `doc` jsonb as a **JSON array of numbers**:
  `"embedding": [0.12, -0.03, …]`.
- Validated on every write: must be an array of **exactly `dimensions`** numbers,
  each finite (reject `NaN`/`Infinity`). Length or finiteness violation →
  `RtDbError::schema` (a `SchemaViolation`, same as every other validator).
- **Not btree-indexable** — `indexed_column_type(Vector { .. })` returns
  `Err` (like `record`/`any`/`bytes`/`int64`). It never produces an `f_<field>`
  column. Its only indexing path is a vector index (below).

### 2.2 Vector index on `IndexDef`

Extend `IndexDef` with an optional vector descriptor, mirroring the additive
`search: bool` flag (omitted on the wire when absent so existing btree/search
indexes deserialize unchanged):

```rust
pub struct IndexDef {
    pub name: String,
    pub fields: Vec<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub search: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<VectorIndexSpec>,
}

pub struct VectorIndexSpec {
    pub dimensions: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub filter_fields: Vec<String>,
}
```

`VectorIndexSpec` uses `#[serde(rename_all = "camelCase")]`, so `filter_fields`
serializes as `filterFields`. Wire for a vector index:

```json
{ "name": "by_embedding", "fields": ["embedding"], "vector": { "dimensions": 1536, "filterFields": ["userId"] } }
```

**Validation rules** (in `TableDef::validate_structure`, extended):

- Exactly one of `search` / `vector` is set on an index (a third kind beyond
  btree and search). Both set → `SchemaViolation`.
- A vector index's `fields` is **exactly one element**, naming a field whose
  declared type is `Vector { dimensions }` **equal to** `vector.dimensions`.
  Mismatched dimensions, missing field, or non-`Vector` field → `SchemaViolation`.
- Every name in `filter_fields` must be a declared field of a **scalar-indexable**
  type (`String`/`Number`/`Boolean`/`Id`/string-`Literal`/string-literal-`Union`,
  or `Optional` thereof — the same set `indexed_column_type` accepts). Non-scalar
  or unknown → `SchemaViolation`.

**Column ownership (load-bearing).** A vector index owns two distinct sets of
physical columns:

- **`v_<index>`** — one `vector(N)` column per vector index, holding the vector
  field's value. Maintained at write time by the new vector-bind path (§3.3).
- **`f_<filterField>`** — one typed scalar column per declared `filterFields`
  entry, so the eq-filter binds a real indexed column. These are created and
  maintained exactly like a btree index's `fields`.

Because the existing `f_`-column plumbing (`indexed_fields` in `ddl.rs` and
`table_columns` in `txn.rs`) scans only `index.fields`, it must be extended for
vector indexes: **include `vector.filter_fields`** in the `f_` set, and
**exclude the vector index's single vector field** from it (a `Vector` field is
not scalar-indexable — `indexed_column_type` rejects it — and is owned by the
`v_` path instead). Concretely, when iterating an index's columns for the `f_`
path, a vector index contributes its `filter_fields` (not its `fields`), while
btree and search indexes contribute their `fields` as today.

### 2.3 `vectorSearch` query terminal

Add an optional terminal to `Query`:

```rust
pub struct Query {
    // …existing fields…
    #[serde(default, rename = "vectorSearch")]
    pub vector_search: Option<VectorSearchQuery>,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VectorSearchQuery {
    pub index: String,
    pub vector: Vec<f32>,
    pub limit: u32,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub filter: BTreeMap<String, serde_json::Value>,
}
```

Wire:

```json
{ "table": "documents", "vectorSearch": { "index": "by_embedding", "vector": [0.1, …], "limit": 10, "filter": {"userId":"u_123"} } }
```

- The Query-level field is `vector_search` in Rust renamed to **`vectorSearch`**
  on the wire. `Query` has no container `rename_all` (every existing terminal is a
  single word, so casing is unobservable); a per-field `rename` pins the first
  multi-word terminal to camelCase, matching the nested-struct + client idiom.
- `vector` is the query embedding; its length **must equal** the index's
  `dimensions` (else `BadRequest`).
- `limit` is required, `1..=VECTOR_SEARCH_MAX_LIMIT` (constant, **256** — Convex's
  cap; well above any reasonable top-K, below unbounded scan cost). Out of range →
  `BadRequest`.
- `filter` is optional. Keys must be a subset of the index's declared
  `filterFields`; each value is an eq against that field's typed column (reusing
  the existing `eq_bind_for` conversion). Unknown field, wrong-type value, or a
  field not in `filterFields` → `BadRequest`.

**Mutual exclusion:** `vectorSearch` is a terminal exclusive with `get`, `index`,
`eq`, range bounds, `order`, `take`, `unique`, `first`, `count`, `paginate`,
`filter`, and `search` — the same rule `search` enforces (extend the existing
guard's error message and condition list). It does **not** compose with `take`
(it carries its own `limit`).

## 3. Server design

### 3.1 pgvector extension (`db.rs`)

`CREATE EXTENSION IF NOT EXISTS vector` in **two** places, both idempotent:

1. `create_database` — immediately after `CREATE SCHEMA "<schema>"`, inside the
   existing transaction. New databases get the extension at birth.
2. `push_schema` (`ddl.rs`) — as the first statement inside its transaction,
   before any table/index DDL. This covers **existing databases** created before
   this code shipped: the first time a schema with a vector index is pushed, the
   extension is ensured. Idempotent and cheap when already present.

The `rtdb` Postgres user is a superuser in both `docker-compose.dev.yml` and
`docker-compose.yml`, so `CREATE EXTENSION` needs no privilege grant.

### 3.2 DDL (`ddl.rs`)

New helper, parallel to `pg_search_col`:

```rust
pub fn pg_vector_col(index_name: &str) -> String {
    format!("v_{}", index_name.to_lowercase())
}
```

For a vector index, after the table and its `f_<field>` columns exist (new table)
or are added+backfilled (existing table) — and per §2.2's column-ownership model,
that `f_` set now includes the index's `filterFields`:

1. Add a column `"v_<index>" vector(N)` — a **plain column**, **not** a
   `GENERATED` column. (pgvector has no clean `jsonb → vector` cast usable in a
   `STORED` generated expression; the vector is populated at write time like the
   btree `f_` columns.) When adding a vector index to an existing table, the
   column is added and then **backfilled** from each row's `doc->vectorField`
   using the same text-cast bind used at write time (§3.3).
2. `CREATE INDEX "i_<table>_<index>" ON "<schema>"."<table>" USING hnsw ("v_<index>" vector_cosine_ops)`.

`detect_destructive_changes` gains a guard rejecting a change to an index's
`vector` spec (present↔absent, or changed `dimensions`/`filter_fields`), parallel
to the existing `search != search` btree↔search guard.

### 3.3 Write path (`txn.rs`)

The vector index's column is maintained alongside the btree `f_<field>` columns:

- Extend `table_columns` to also emit vector-index columns: for each index with
  `vector` set, emit a `(pg_vector_col(index.name), <vector field>)` entry tagged
  so the binder knows it's a vector column (not a btree column). Keep stable
  ordering (existing `BTreeSet` discipline).
- Extend `ColBind` with `Vector(Option<String>)` holding the serialized pgvector
  text form `"[v1,v2,…]"` (or use the `pgvector` crate's `PgVector` — see §6).
- Extend `column_bind_for` for the vector case: validate length == dimensions and
  finiteness (defensive — schema validation already did this), serialize the
  JSON array to `[a,b,c]` text. `None` when the field is absent/null → SQL NULL.
- `do_insert` / `do_patch` / `do_replace` / `do_upsert` include the vector column
  in their column lists and binds. The vector column is bound with an explicit
  `::vector` cast (e.g. the placeholder is `"$n::vector"` rather than plain
  `"$n"`), because pgvector does not implicitly coerce a text param into
  `vector(N)` for prepared-statement binds.

> **Note:** schema-level `validate_value` for `Vector` is the source of truth for
> length/finiteness. By the time `column_bind_for` runs the array is already
> known-good; the bind-time check is defensive only.

### 3.4 Read path (`query.rs`)

`execute_vector_search(pool, db, table_def, table_name, vs, …)`:

```sql
SELECT "doc"
FROM   "<schema>"."<table>"
WHERE  "v_<index>" IS NOT NULL
  [AND "f_<filterField>" = $eq …]
ORDER  BY "v_<index>" <=> $query_vector      -- cosine distance, ascending
LIMIT  $limit
```

- Resolve the index by name; reject with `BadRequest` if absent or not a vector
  index.
- Bind `$query_vector` as the serialized text of `vs.vector` cast `::vector` (or
  via the `pgvector` crate). Validate `vs.vector.len() == dimensions` first →
  else `BadRequest`.
- For each `filter` entry: the key must be in the index's `filter_fields`; build
  an eq bind via the existing `eq_bind_for` against the field's `f_<field>` typed
  column (same conversion the `eq` prefix uses). Unknown field → `BadRequest`.
- `<=>` is cosine **distance** (0 = identical, 2 = opposite); ascending order
  returns most-similar first.
- Result variant is `QueryResult::Docs(Vec<Value>)` — identical shape to a
  `collect`/`take`/`search` result. `canonical()` already serializes `Docs`, so
  subscription diffing works unchanged (§4).
- The `vectorSearch` arm in the terminal router (where `search` is dispatched,
  ~`query.rs:270-290`) calls `execute_vector_search` and returns early, exactly
  like `execute_search`.

### 3.5 Reactivity

`vectorSearch` is an ordinary `Query`, so it rides the committer's existing
table-level invalidation unchanged: any committed write to the query's table
re-runs every affected `vectorSearch` subscription and pushes only on canonical
diff. **No new committer code.** This is a deliberate divergence from Convex
(whose `vectorSearch` is a one-shot action); par-rt-db makes it reactive for free
and notes it as an advantage in `FEATURE_MATRIX.md`. At personal-app scale the
per-write HNSW query cost (~ms) is acceptable.

## 4. Deployment

Switch the Postgres image in **both** compose files from `postgres:17` to
**`pgvector/pgvector:pg17`** (the official image with the extension pre-built —
same Postgres 17, adds `vector`):

- `docker-compose.dev.yml` (dev, `make dev-db-up`) — required so `make test`
  passes (integration tests hit the real DB).
- `docker-compose.yml` (prod, lenny2) — required for the live deploy.

`CREATE EXTENSION IF NOT EXISTS vector` is per-database and idempotent, so no
data migration: existing volumes keep their data; each database gets the
extension on its next `create_database` (new) or `push_schema` (existing). Update
`deploy/README.md` to note the image change. The `Dockerfile` (the Rust server
image) is unaffected — pgvector lives in the Postgres image, not the app image.

## 5. Error handling

All failures use the existing `RtDbError` envelope `{code, message}`:

- Schema violations (bad `Vector` value, bad vector-index declaration) →
  `SCHEMA` (`RtDbError::schema`).
- Query-time contract violations (unknown index, index not a vector index,
  vector length ≠ dimensions, `filter` key not in `filterFields`, `limit` out of
  range, `vectorSearch` combined with another terminal) → `BAD_REQUEST`
  (`RtDbError::bad_request`).
- pgvector parse errors should be unreachable post-validation; if one surfaces,
  map to a **generic** internal error and log via `tracing` — never stringify a
  sqlx/pgvector error into the response body (existing discipline).

## 6. Open implementation detail (resolved in the plan)

**Vector binding mechanism.** Two viable approaches; the plan picks one and
proves it with a spike before the write-path task:

1. The `pgvector` crate (`pgvector = "0.x"`, sqlx feature) — idiomatic `PgVector`
   type for both binding column values and the query vector, typed distance op.
2. Raw SQL with serialized text + explicit `::vector` cast on the placeholder,
   no new dependency.

Recommendation: **(1) the `pgvector` crate** for clean, typed binding and to avoid
hand-rolling the text format; the crate is small and purpose-built for this. The
spike confirms sqlx interop under the existing `sqlx::query` (non-macro) style
this codebase uses.

## 7. Client mirror

Both clients add the same three surfaces (no codegen):

- **ts-client** (`protocol.ts` + schema/query builders): `FieldTypeJson` gains
  the `vector` variant; `t.vector(dimensions)` schema factory; `vectorIndex(...)`
  declaration; `TableQuery.vectorSearch(vector, { limit, filter })` builder; types
  inferred from the schema object. The in-memory test harness (`in_memory.ts`)
  gains vector search semantics (rank by cosine over stored arrays) so tests run
  without Postgres.
- **rust-client** (`wire.rs` + schema/query builders): mirror the wire types and
  add `vector_index(...)` schema declaration + `.vector_search(...)` query
  builder, paralleling the existing `search_index()`/`.search()`.

## 8. Testing

- **Server `schema`/`schema_validators`:** `Vector` round-trips; dims/length/
  finiteness validation; vector-index validation (single `Vector` field w/
  matching dims; `filterFields` scalar-typed; mutual exclusion with `search`).
- **Server `ddl`/`txn`:** vector index creates `v_<index>` column + HNSW index;
  insert/patch/replace/upsert maintain the vector column; backfill populates it
  when a vector index is added to a table with existing rows.
- **Server `query`:** `vectorSearch` returns ranked docs (verify order by cosine);
  `limit` cap; eq-`filter` restricts to a `filterField`; rejects length mismatch,
  unknown index, non-vector index, unknown `filter` key, out-of-range `limit`,
  and combination with other terminals.
- **Server `protocol`:** wire round-trip for `Vector`, a vector `IndexDef`, and
  the `vectorSearch` terminal (camelCase `vectorSearch`/`filterFields`).
- **Reactivity:** a `vectorSearch` subscription pushes an updated ranking after a
  write to the table (mirror the existing `search` re-run test).
- **Clients:** `query.test.ts`/rust-client builder tests for
  `.vectorSearch()`; `schema.test.ts`/schema-types for `t.vector(n)` +
  `vectorIndex()`; in-memory harness coverage in ts-client.

## 9. Convex parity

Flip `FEATURE_MATRIX.md` row #17 ❌ → ✅ with the mirror-status note. Two
deliberate divergences to record: (a) **reactive** (Convex `vectorSearch` is a
one-shot action; par-rt-db re-runs and pushes live), and (b) **client-supplied
embeddings** enforced by the no-server-code architecture (apps/external workers
compute them). Both are par-rt-db-native fits — pgvector in-database where Convex
needs a dedicated component.
