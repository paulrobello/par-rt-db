# ENH-021 — Streaming storage upload and download

> **Source**: kanban card `[ENH-021]`, project `par-rt-db`. Derived from the 2026-08-09 Opus audit.
> **Impact**: medium-high · **Effort**: medium · **Breaking**: no
> **Subsumes**: audit finding `SEC-123` (Range requests full-load) — coordinate, do not duplicate.

## Goal

Remove the "whole blob in memory" constraint from both ends of the storage path so par-rt-db can host
files meaningfully larger than the current 50 MiB default without the server's memory ceiling being the
limit.

## Current state

Verified in `server/src/http_api.rs`:

- **Upload** (`:413-419`): the route disables axum's 2 MiB default and calls
  `axum::body::to_bytes(request.into_body(), limit)` where
  `limit = HARD_MAX_FILE_SIZE.min(hot.max_file_size)`. **The entire file is buffered in RAM** before
  `storage::put` writes it. N concurrent uploads = N × filesize resident.
- **Download** (`:626-641`, `:723`, `:741`): the blob is `SELECT`ed whole out of the `bytea` column,
  and the Range path then **slices a copy** — the audit's `SEC-123`: a 1-byte range on a 100 MB blob
  allocates 100 MB plus a `to_vec`.
- Storage is deliberately Postgres-native (`bytea` in a per-db `storage` table + a global
  `rtdb.storage_index`), per the user's recorded steer against disk/S3/object-store backends. **This
  enhancement does not revisit that decision** — it makes the Postgres path streaming.

Supporting context: `RTDB_MAX_FILE_SIZE` defaults to 50 MiB and is admin-mutable via
`PATCH /admin/config`, clamped by a compile-time `HARD_MAX_FILE_SIZE` a compromised admin token cannot
raise. That clamp is a good design and should survive.

## Implementation

### Step 1 — Chunked storage layout

Add a `storage_chunks` table per database, alongside the existing `storage` metadata row:

```sql
CREATE TABLE storage_chunks (
  blob_id  TEXT NOT NULL,
  seq      INT  NOT NULL,
  bytes    BYTEA NOT NULL,
  PRIMARY KEY (blob_id, seq)
);
```

Keep the existing `storage` row as the metadata record (`id`, `sha256`, `size`, `contentType`,
`createdAt`, and `owner_id` if `SEC-118` has landed), and drop its inline `bytes` column for new blobs.

**Migrate existing blobs lazily, not eagerly.** Keep reading the legacy inline `bytes` column when
`storage_chunks` has no rows for that id. A one-shot admin endpoint can convert on demand. An eager
migration of every blob on every database at boot is exactly the kind of unbounded work `ARC-102`
criticizes.

Chunk size: 1 MiB. Small enough to bound memory, large enough that a 100 MB file is 100 rows rather
than 100,000.

### Step 2 — Streaming upload

Replace `to_bytes` with a streaming consumer in `upload_handler`:

1. Take `request.into_body().into_data_stream()`.
2. Accumulate into a 1 MiB buffer; on each full buffer, `INSERT` one `storage_chunks` row and update
   a running SHA-256 and byte count.
3. Enforce `limit` **incrementally** — abort the moment the running total exceeds it, rather than
   after buffering. This is strictly better than today: an over-limit upload currently still buffers
   up to the limit before rejecting.
4. Run the whole insert sequence in one transaction so a failed upload leaves no partial blob.
5. Write the `storage` metadata row last, with the final `sha256` and `size`.

**Preserve ENH-008 content-addressed dedup.** Today `storage::put` dedups via
`INSERT … ON CONFLICT (sha256) DO NOTHING RETURNING id`, which needs the hash *before* the insert.
Streaming computes the hash *during*. Resolution: write chunks under a provisional id, compute the
final hash, then attempt the metadata insert with `ON CONFLICT (sha256)`; on a content hit, **delete
the provisional chunks and return the existing id**. Slightly more I/O on a duplicate upload, but it
keeps dedup working and it is the honest tradeoff for not knowing the hash up front.

**Preserve quota enforcement.** `upload_handler` currently checks `used + blob > cap` via
`quota::current_usage` before writing. With streaming, the size is not known up front — check
incrementally against the cap as chunks land and abort with `QUOTA_EXCEEDED` (507) mid-stream. Do not
skip the check because the size is unknown.

### Step 3 — Streaming download and a real Range path *(this is `SEC-123`)*

- Serve the body as a stream over `storage_chunks` ordered by `seq`, rather than one `SELECT` of the
  whole blob.
- For a `Range` request, compute the chunk span covering `[start, end]` and fetch **only those chunks**,
  trimming the first and last. A 1-byte range on a 100 MB file then reads one 1 MiB chunk.
- For a legacy inline blob, use `substring(bytes FROM $start FOR $len)` — TOAST-aware, reads only the
  needed pages. This is the `SEC-123` remedy for un-migrated rows.
- Keep the existing 206 / `Content-Range` / 416 (`bytes */<total>`) semantics exactly. `octet_length`
  or the metadata row supplies the total cheaply.

**Transformed images keep their current behavior.** `image_transform.rs` cache-keys whole renders and
deliberately skips Range; it needs the full bytes to decode. Fetch all chunks for that path. Do not
try to stream into the decoder.

### Step 4 — Raise the ceiling

With memory decoupled from file size, raise `HARD_MAX_FILE_SIZE` (suggest 2 GiB) and document that
`RTDB_MAX_FILE_SIZE` is now a policy limit rather than a memory-safety limit. Keep the compile-time
clamp — its purpose (a compromised admin token cannot raise it arbitrarily) is unchanged.

### Step 5 — Client mirrors

The HTTP surface is unchanged (same routes, same semantics), so **no wire or protocol change**. But
the clients buffer too, and a 1 GiB upload through a client that reads the file into memory defeats
the point:

- `ts-client` — accept a `ReadableStream`/`Blob` in `upload`, not just an `ArrayBuffer`.
- `rust-client` — accept an `impl Stream<Item = Result<Bytes>>` or an `AsyncRead`.
- `python-client` — accept a file-like object in both the sync and async clients.

Keep the existing buffer-accepting overloads; add streaming variants alongside.

## Files to touch

- `server/src/storage.rs` — `storage_chunks` DDL, chunked `put`, chunked/ranged read, lazy legacy fallback
- `server/src/db.rs` — create the table for new databases (mirror how ENH-008's `sha256` index is created)
- `server/src/http_api.rs` — streaming `upload_handler`; streaming `serve_bytes`/`build_serve_response`;
  the Range path
- `server/src/quota.rs` — incremental cap check during upload
- `server/src/config.rs` — raise `HARD_MAX_FILE_SIZE`
- `ts-client/src/http.ts`, `rust-client/src/http.rs`, `python-client/.../{http_client.py,aio_http_client.py}`
- `README.md`, `server/README.md`, `deploy/README.md`, `FEATURE_MATRIX.md` (#16 storage row), `CHANGELOG.md`

**Coordinate with the Phase 1 `http_api.rs` batch.** `SEC-101`, `SEC-112`, `SEC-113`, `SEC-118`,
`SEC-119`, and `SEC-123` all edit this file and the audit assigns them to **one agent**. This
enhancement rewrites the same functions — land it **after** that batch and rebase onto it, or the
merge is a guess.

## Verify

```bash
make -C /Users/probello/Repos/par-rt-db dev-db-up
make -C /Users/probello/Repos/par-rt-db checkall > /tmp/enh021.log 2>&1; echo "EXIT=$?" >> /tmp/enh021.log
grep '^EXIT=' /tmp/enh021.log
cargo test --manifest-path /Users/probello/Repos/par-rt-db/server/Cargo.toml storage
cargo test --manifest-path /Users/probello/Repos/par-rt-db/server/Cargo.toml range
cargo test --manifest-path /Users/probello/Repos/par-rt-db/server/Cargo.toml image_transform
cargo test --manifest-path /Users/probello/Repos/par-rt-db/server/Cargo.toml quota
```

**Acceptance criteria** (mirror these onto the card):
1. `make checkall` green.
2. A blob larger than the old in-memory ceiling round-trips byte-identically (upload → download →
   SHA-256 matches).
3. A `Range` request for 1 byte of a large blob reads **only** the covering chunk — asserted by a test
   that counts fetched chunk rows or bytes, not by timing.
4. Legacy inline blobs (written before this change) still serve correctly, including ranged requests.
5. ENH-008 content-addressed dedup still returns the existing id on a re-upload of identical bytes, and
   leaves no orphaned provisional chunks.
6. An upload exceeding `RTDB_MAX_FILE_SIZE` is rejected **without** buffering the whole body, and
   commits no partial blob.
7. An upload exceeding a configured storage quota returns `QUOTA_EXCEEDED` (507) and commits nothing.
8. Image transforms still work on a chunked blob.
9. Existing 206 / `Content-Range` / 416 semantics unchanged.

## Rollback

The legacy inline-`bytes` read path is retained permanently, so a server revert still serves every
blob written before the change — but **blobs written as chunks after the change would not be readable
by a reverted server**. Mitigate by shipping the *read* side (chunk-aware reads + legacy fallback) one
release before the *write* side, so a rollback window exists where both layouts are readable. Note
that ordering explicitly in the PR.

`storage_chunks` is additive; no existing table is altered destructively.

## Risks

- **Dedup ordering.** The hash-after-write inversion is the subtlest part. Get the provisional-chunk
  cleanup right or a duplicate upload leaks rows into `storage_chunks` that nothing references — and
  they will count against the storage quota (`quota.rs` sums `pg_total_relation_size` over the db's
  tables, so orphans are billed).
- **Transaction size.** A 2 GiB upload is 2,048 chunk inserts in one transaction. Verify Postgres
  handles it acceptably and consider a periodic commit with a cleanup sweep for abandoned uploads.
- **`SEC-118` interaction.** If per-row blob authorization lands first, `owner_id` lives on the
  metadata row — make sure the chunked path still writes it.
