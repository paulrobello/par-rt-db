# File storage — design

**Status:** Implemented (2026-08-10) — `storage.rs` bytea blobs + global `storage_index`; `POST /api/storage/{db}`, `GET /storage/{id}`; FEATURE_MATRIX #16.

## Problem

par-rt-db can store structured documents but not the blobs that real apps attach
to them — an uploaded image on a kanban card, an avatar, a PDF. Today every app
on the instance either forgoes attachments or bolts on its own object store.
FEATURE_MATRIX #16 (Med-High utility, L effort) is the #1 remaining Convex-parity
gap: "Needed the moment any app wants image upload."

Convex solves this with file storage: `generateUploadUrl` → POST bytes → an
`Id<"_storage">`; `getUrl(id)` returns a public URL; metadata is queryable as the
`_storage` system table. par-rt-db can match this with **one new per-db table and
one new HTTP route family**, reusing the existing per-db side-table lifecycle,
bearer auth, and `export_db`-style binary responses — and no new infrastructure.

The user has explicitly approved **vendor-locking to Postgres** for this: bytes
live in Postgres `bytea` (TOAST-managed), not a disk directory or S3/object
store, and there is no pluggable storage trait. Postgres already holds every
app's data, so native blob storage keeps everything in one `pg_dump` and avoids a
second ops surface. (The same steer pre-decides a future vector-search feature to
use `pgvector` natively.)

## Scope

This design covers server blob storage with **both** a public and an authenticated
serve path, the metadata surface, and the client SDK surface on all three clients.

Server: `db.rs` (a new per-db `storage` table at `create_database` time, and a
new global `rtdb.storage_index` table at startup), a new `server/src/storage.rs`
(table access + sha256/size), `http_api.rs` (the new HTTP route family and the
per-route body-limit layer), `config.rs` (a max-file-size knob), and `lib.rs`
(wire the routes into `build_router`).

Clients: the TS SDK (`http.ts`, `client.ts`, the in-memory test harness) and the
Rust client (`http`) gain `upload` / `deleteFile` / `getFileMetadata` /
`getUrl`. No client-side WS surface — storage is not reactive (see below).

Out of scope: the Convex two-step `generateUploadUrl` upload-URL flow (v1 uses a
direct authenticated upload — see "Upload model"); reference-based orphan garbage
collection (unreliable to detect; explicit `delete` only); making `storage`
queryable through the full declarative query DSL (Convex's `_storage` table) — a
metadata `GET` is enough for v1; HTTP range requests (`206 Partial Content`) for
video streaming; per-row authorization of who may read a given file; and a React
storage helper hook (the client API ships first).

## Design

### Backend choice: Postgres `bytea`, no abstraction

Three backends were considered.

**A. `bytea` in a per-db `storage` table (recommended, adopted).** Postgres
TOASTs any value over ~2 KB out of line and compresses it transparently, so a
50 MB blob does not bloat the main heap the way "a 50 MB column" naively
suggests — it lives in the table's TOAST side storage. Zero new dependencies:
sqlx already binds Postgres binary as `Vec<u8>`, and `sha2` / `hex` / `uuid`
(v7) are already in `server/Cargo.toml`. `pg_dump` covers blobs for free (`bytea`
is just column data), and the table slots into the exact
side-table lifecycle (`mutations`, `scheduled_txns`) the codebase already uses.

**B. Postgres Large Objects (`pg_largeobject`).** Rejected. Large Objects live
in a single global catalog referenced by OID, which breaks the per-db snapshot
model (an export of `db_kanban` cannot cleanly include its LOs), and streaming
LOs through sqlx is awkward. The server-side streaming benefit is not worth the
snapshot/ownership complexity at ≤50 MB.

**C. Disk or object-store backend behind a `StorageBackend` trait.** Rejected per
the user's "vendor-lock to Postgres" steer, and YAGNI: a trait we would never
swap is pure ceremony. (If a disk/S3 backend is ever wanted, the wire contract
in this spec does not change — only the `storage.rs` accessors do.)

### Table placement: per-db table + a global index

A fork specific to a multi-database instance: a public serve URL carrying only an
opaque id must still resolve to the owning database to read its `bytea`.

**Per-db `storage` + global `rtdb.storage_index` (recommended, adopted).** The
bytes and metadata live in `db_<db>.storage`, co-located with the rest of the
database's data and isolated per-db — matching the `mutations` /
`scheduled_txns` side-table pattern. A tiny global `rtdb.storage_index (id →
db_name)` lets the public serve route resolve an opaque id to its db without
leaking the db name in the URL.

*Alternative: a single global `rtdb.storage(id, db_name, …)`.* Simpler serve (one
table, no index lookup) but diverges from the per-db side-table pattern and would
complicate any future per-db storage export (a `WHERE db_name = …` filter plus a
separate storage section). Rejected for pattern consistency.

### Data model

Per-db `storage` table — created eagerly inside the `db::create_database`
transaction alongside `meta` / `mutations` / `scheduled_txns` (for new dbs), and
lazily via `CREATE TABLE IF NOT EXISTS` at committer startup for dbs that predate
this feature — exactly the `mutation_log::ensure_table` / `scheduler::ensure_table`
lifecycle.

```sql
CREATE TABLE "<schema>".storage (
    id           text PRIMARY KEY,   -- uuid v7 (db::new_id), server-generated, returned to client
    sha256       text NOT NULL,      -- content hash, computed on upload
    size         bigint NOT NULL,    -- bytes
    content_type text,               -- from the upload's Content-Type header; NULL → application/octet-stream on serve
    bytes        bytea NOT NULL,     -- Postgres TOAST-managed
    created_at   bigint NOT NULL     -- db::now_ms()
);
```

Global index — created at server startup in the same bootstrap that creates the
`rtdb_auth` schema (`db.rs`), so the public serve route can resolve an opaque id:

```sql
CREATE TABLE rtdb.storage_index (
    id      text PRIMARY KEY,
    db_name text NOT NULL
);
```

The side table is bare-named (`storage`, not `t_storage`) in the per-db schema,
consistent with the other system side tables; the `pg_table` / `pg_col` helpers in
`ddl.rs` apply only to *user* tables and are not used here.

### Upload model: direct authenticated upload (v1)

Convex uses a two-step flow (`generateUploadUrl` → POST to a short-lived,
self-credentialing URL) because the browser uploads directly to a separate
storage backend without the function's auth context. par-rt-db has no separate
storage backend — uploads hit the same server, and the SPA already holds a
db-scoped bearer token — so a **direct authenticated upload** is the natural,
one-round-trip fit:

- `POST /api/storage/{db}`, `Authorization: Bearer <token>`, request body = raw
  bytes, `Content-Type` carried in the header. The db is in the path because the
  raw body cannot carry it and session principals are not db-scoped; `authorize`
  confirms the principal may access that db (machine token must match it, session
  user must be allowlisted for it).
- The handler streams the body, updating a `Sha256` digest and a byte counter per
  chunk, and rejects with `BadRequest` the moment the running size exceeds
  `RTDB_MAX_FILE_SIZE` (so an oversized upload is not buffered in full before
  rejection). It then `INSERT`s the `bytea` + metadata into `db_<db>.storage`
  and a row into `rtdb.storage_index`.
- Returns `{ id, sha256, size, contentType }`.

**Body limit — a new pattern for this codebase.** There is no
`DefaultBodyLimit` / `RequestBodyLimit` / tower `LimitLayer` anywhere today, so
axum 0.8's 2 MiB default caps every request. The upload route is layered with an
explicit `DefaultBodyLimit::max(RTDB_MAX_FILE_SIZE)` (default 50 MB, from
`config.rs`). This is the first per-route body-limit layer in the server; it is
scoped to the upload route only so existing JSON routes keep the 2 MiB default.

The Convex two-step `generateUploadUrl` flow is a documented future parity
follow-up, not v1 — it adds a round trip and a short-lived-token table for no
benefit on a single-server, bearer-authed architecture.

### Serve — both public and authenticated

Both paths return the stored bytes via `Result<Response, RtDbError>` with
`Body::from(bytes)` and the stored `Content-Type`, mirroring `export_db`
(`admin/dbs.rs`). `Content-Type` defaults to `application/octet-stream` when the
upload omitted it.

- **Public** — `GET /storage/{id}`, **no authentication**. Resolve `id → db_name`
  via `rtdb.storage_index` (404 `NotFound` if absent), read the `bytea` from
  `db_<db>.storage`, respond with the bytes. This is Convex parity: anyone with
  the URL can fetch, the id is the only credential, and revocation is by delete.
  The id is an unguessable uuid v7 (122 bits of randomness), so the only exposure
  surface is ids the app intentionally puts in e.g. `<img src>`. This is the route
  the kanban SPA's `<img src={getUrl(id)}>` uses.
- **Authenticated** — `GET /api/storage/{db}/{id}`, `Authorization: Bearer
  <token>`. `authorize` confirms the principal may access `{db}`, then reads
  *that* db's `storage` row — a 404 if the id belongs to a different database.
  For sensitive files the app does not want on a public bearer URL. (The db is
  in the path for the same reason as upload; the client injects its own db, so
  the client API takes no db parameter.)

### Delete and metadata

- `DELETE /api/storage/{db}/{id}`, bearer → deletes the `db_<db>.storage` row and
  the `rtdb.storage_index` row → `{ ok: true }`. This revokes the public serve
  URL immediately (the next `GET /storage/{id}` 404s).
- `GET /api/storage/{db}/{id}/metadata`, bearer → `{ id, sha256, size,
  contentType, creationTime }`. `contentType` is omitted on the wire when null,
  like other optional fields (`github_login`, `cron`).

### Authorization

Upload, delete, metadata, and the authenticated serve path reuse the existing
`bearer_token` → `resolve_bearer` → `authorize` triple used by `/api/query` and
`/api/mutate`. `authorize` is a single gate — a machine token must match the db
and not be revoked; a session user must be unexpired and allowlisted — so all
four authed routes pass it identically (there is no read/write level
distinction to apply). The db under test comes from the `{db}` path segment on
the authed routes — the raw upload/serve bodies cannot carry it, and session
principals are not db-scoped. **The public serve route `GET /storage/{id}` is
the one unauthenticated route in the server, by design.** No new auth mechanism
is introduced.

### Failure handling

Every failure is the `RtDbError` `{code, message}` envelope. Unknown id →
`NotFound`; upload exceeding the size limit → `BadRequest` (rejected mid-stream,
before the bytes are stored); a missing/invalid bearer on the authed routes →
`Unauthorized` / `Forbidden` exactly as today. Binary responses
(upload/serve/delete) use `Result<Response, RtDbError>` so the existing
`IntoResponse` impl maps errors to the envelope the same way JSON handlers do.
As elsewhere, no `unwrap()`/`expect()` outside `#[cfg(test)]`.

### Wire surface — HTTP only, no WS variants

Storage is inherently request/response, not reactive — a file does not "live
update" — so it does **not** get `ClientMessage` / `ServerMessage` WebSocket
variants. This deliberately makes the storage surface *smaller* than the
scheduled-txns surface (no `protocol.rs` / `protocol.ts` / `wire.rs` message
types, no WS handlers, no reactive re-run). Both transports already route
mutations through the committer; file storage is not a mutation of a user table
and does not touch the committer or subscriptions at all.

HTTP routes (added to `http_api_routes()` in `http_api.rs`, except the public
serve route which is unauthenticated):

```
POST   /api/storage/{db}               bearer  body=raw bytes, Content-Type  → { id, sha256, size, contentType }
GET    /api/storage/{db}/{id}          bearer                                  → bytes (authed serve)
GET    /api/storage/{db}/{id}/metadata bearer                                  → { id, sha256, size, contentType, creationTime }
DELETE /api/storage/{db}/{id}          bearer                                  → { ok: true }
GET    /storage/{id}                   (none)                                  → bytes (public serve)
```

`{db}` and `{id}` use axum 0.8 path-param syntax (as
`/api/schedule/{id}/cancel` already does). The public serve route is registered
on the same router but takes no auth extractor.

### Clients

**TS SDK.** `upload` / `deleteFile` / `getFileMetadata` / `getUrl` are added to
the shared client interface and implemented on `RtDbHttpClient` (real HTTP:
`upload(bytes: Uint8Array, contentType?: string)` →
`Promise<{ id, sha256, size, contentType }>`; `deleteFile(id)` → `Promise<void>`;
`getFileMetadata(id)` → `Promise<FileMetadata>`; `getUrl(id)` → `string`
constructing `${baseUrl}/storage/${id}` with no fetch — the browser consumes it
in e.g. `<img src>`). The reactive `RtDbClient` and the in-memory harness
(`InMemoryRtDbClient`) implement the same interface: the reactive client
performs the HTTP calls over its HTTP transport, and the in-memory harness
stores bytes in a `Map<id, {bytes, contentType}>` with no network (its `getUrl`
returns a synthetic `memory://` URL).

**Rust client.** The `http` module gains `upload(bytes: &[u8], content_type:
Option<&str>)`, `delete_file(id)`, `get_file_metadata(id)`, and `get_url(id)`
(constructs the public URL string).

## Testing

- New `server/tests/storage_test.rs` (mirrors the module→binary convention):
  - Upload → public serve round trip: bytes, sha256, size, and content-type all
    match what was uploaded.
  - Upload → authed serve round trip on the same db.
  - Content-Type defaults to `application/octet-stream` when omitted on upload.
  - Oversized upload (just over `RTDB_MAX_FILE_SIZE`) is rejected with
    `BadRequest` and stores nothing.
  - Delete removes the row; the public serve URL then 404s (revocation).
  - Metadata `GET` returns the correct fields; `contentType` is omitted when null.
  - Cross-db isolation: db A's authed serve of db B's id is a 404; db A's public
    id still resolves (public serve is db-agnostic by id).
  - Auth: a revoked machine token cannot upload, delete, or read metadata;
    public serve needs no token.
  - Per-db `ensure_table`: a database created before the feature (simulated by
    dropping `storage` then hitting the committer-startup ensure path) gains the
    table and works.
- `ts-client` tests: `storage.test.ts` — upload/delete/metadata/getUrl over the
  in-memory harness and a wiremock-backed `RtDbHttpClient`, asserting the wire
  shapes and that `getUrl` produces the public URL.
- `rust-client` tests: `http` module storage methods over wiremock, including
  the raw-bytes upload body and the public-URL construction.
- Opt-in live-server E2E (`ts-client/tests/integration/storage.test.ts`,
  `rust-client` `#[ignore]` + `RTDB_TEST_SERVER_URL`), mirroring the other
  features' E2E pattern.

## Verification

`make checkall` from the repo root (fmt-check + clippy `-D warnings` + typecheck
+ tests for server, ts-client, and rust-client). Integration tests require
`make dev-db-up`. This is the definition of done. `FEATURE_MATRIX.md` row #16
flips ❌ → ✅ (noting the client-mirror status), and this surface is added to the
relevant README(s) and `CLAUDE.md`.

## Out of scope

- The Convex two-step `generateUploadUrl` upload-URL flow (v1 is direct
  authenticated upload).
- Reference-based orphan garbage collection (scanning user jsonb for storage ids
  is unreliable and expensive; deletion is explicit).
- Snapshot export/import of storage blobs — the snapshot JSONL covers schema
  tables and their documents today (as it does for `mutations` /
  `scheduled_txns`); exporting `bytea` blobs is a separate enhancement, not v1.
- Full query-DSL queryability of `storage` (Convex's `_storage` system table); a
  metadata `GET` covers v1 needs.
- HTTP range requests / `206 Partial Content` (video seeking).
- Per-row authorization of file access (the public URL is a bearer credential by
  design; the authed path is db-scoped, not row-scoped).
- A React storage helper hook.
