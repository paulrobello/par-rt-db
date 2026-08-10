# On-the-Fly Image Transforms on Storage Serve

**Date:** 2026-08-05
**Status:** Implemented (2026-08-10)
**Scope:** Adds server-side image transforms (resize / quality / format) to the
storage serve routes, so a single stored image yields any derived size on demand
with an in-memory cache. Convex file-storage image-transform parity
(FEATURE_MATRIX #16). Backlog item **ENH-014**.

## Background & motivation

`GET /storage/{id}` (public, unauthenticated) and `GET /api/storage/{db}/{id}`
(authed) both funnel through `serve_bytes` (`http_api.rs`), which streams the
stored bytes verbatim. Apps that need thumbnails or resized variants today must
store *N* copies at upload time or transform client-side. ENH-014 adds optional
query-param transforms processed server-side: one stored image → any derived
size. This is a feature Convex ships natively on its file URLs.

### Hard constraints (from the architecture)

- **Storage is HTTP-only and bypasses the committer** (`storage::put` writes
  directly; blobs never touch document tables or subscriptions). Transforms are
  read-time-only — they must not introduce a write path or a new committer arm.
- **No embedded JS runtime, no per-app server code.** Transforms are a fixed,
  declarative server capability applied via a pure-Rust image library — there is
  no plan to make them scriptable.
- **Postgres is the source of truth; no disk/S3/object-store** (the user's
  vendor-lock-to-Postgres steer). The *derived-image cache* is in-memory and
  ephemeral by design (derived bytes are reproducible from source + params); it
  does **not** persist to Postgres (that would bloat the DB with regenerable
  data and add a storage write path). Source bytes remain the only durable copy.
- **The public route is unauthenticated.** Anyone who knows an opaque URL can
  mint unbounded decode work by varying params (`?w=1&q=1`, `?w=2&q=1`, …).
  Safety guards (§Safety) are therefore load-bearing, not optional.

### Decision summary

- **Library:** the `image` crate (pure Rust, no native deps) — keeps the lean
  Docker image and the x86-host build simple. Decode PNG/JPEG/GIF/BMP/WebP;
  output **JPEG + PNG** only in v1 (no WebP/AVIF encode — those need native
  libs; clean follow-up).
- **Cache:** in-memory, byte-bounded LRU (`moka::future::Cache`) with
  single-flight dedup, keyed by `(id, params)`. Default 256 MiB.
- **Scope:** server feature + `transformUrl`/`transform_url` helpers on all
  three clients (ts/rust/python) + a dashboard storage-browser control, tests,
  and docs.

## API

Both serve routes accept these optional query params (added to the existing
`GET /storage/{id}` and `GET /api/storage/{db}/{id}`):

| param   | type           | range / values                       | meaning |
|---------|----------------|--------------------------------------|---------|
| `w`     | u32            | 1..=`MAX_DIM`                         | target width |
| `h`     | u32            | 1..=`MAX_DIM`                         | target height |
| `fit`   | enum string    | `cover` \| `contain` \| `scale-down`  | resize fit mode |
| `q`     | u8             | 1..=100                               | JPEG output quality (ignored for PNG) |
| `format`| enum string    | `jpeg` \| `png` \| `auto`             | output format; `auto` (default) keeps source if jpeg/png, else jpeg |

This mirrors Convex's transform param set (`w`/`h`/`fit`/`q`).

### Behavior rules

1. **Passthrough (fast path).** When *no* effective transform is requested
   (`w`/`h`/`q` all absent **and** `format` is absent or `auto`), serve the raw
   bytes unchanged with zero decode overhead — today's behavior, preserved for
   backward compatibility and bandwidth parity.
2. **Transform requested** (any of `w`/`h`/`q` set, or `format` explicitly
   `jpeg`/`png`): decode → resize/re-encode → serve.
3. **Invalid params** (`w=0`, `w>MAX_DIM`, `q=200`, unknown `fit`/`format`,
   non-numeric) → `400` with the standard `{code:"BAD_REQUEST", message}`
   envelope. No silent clamping — the contract is explicit.
4. **Source not a decodable image** (e.g. a PDF, or `image/*` with corrupt
   bytes) **when a transform was requested** → serve the **raw bytes unchanged**
   (ignore the transform). Best-effort: an existing URL never 500s just because
   a transform was requested on a non-image; `content-type` may be wrong/missing
   and the bytes still serve. Decode failure is logged at `debug`.
5. **Source exceeds the pixel cap** (§Safety) → `400` `BAD_REQUEST` with a clear
   message ("image exceeds max pixels for transform"). The caller can drop the
   params to get the raw bytes. This prevents decode-cost DoS.

### Resize semantics (`fit`)

Given source dimensions `(ow, oh)` and a target box derived from `w`/`h` (an
unset dimension means "unbounded" on that axis — only-`w` scales preserving
aspect to that width):

- **`contain`** — fit entirely within the box, preserve aspect, no crop, no
  upscale beyond the box. (`image::imageops::resize` with a `Triangle` filter —
  a good speed/quality tradeoff for downscaling on a shared server.)
- **`scale-down`** — like `contain`, but never upscale (if the source is already
  smaller than the box, keep source dimensions; only re-encode).
- **`cover`** — fill the box exactly: scale by `max(w/ow, h/oh)`, then
  center-crop to exact `w×h`. Implemented as a `resize_exact` to the scaled
  cover dimensions followed by a centered `crop` (the `image` crate has no
  single cover primitive).

When `fit` is omitted but `w` and/or `h` are present: default is `cover` if both
given, else `contain`-to-the-single-dimension (scale to the given axis). When
neither `w` nor `h` is given but `q`/`format` is: no resize, only re-encode.

### HTTP caching

Blobs are write-once (content-deduped by sha256 on upload; no update path — a
re-upload of identical bytes returns the same id, and a changed image is a new
id). Both raw and transformed serve responses are therefore immutable, so
`serve_bytes` sets:

```
Cache-Control: public, max-age=31536000, immutable
```

This is a small adjacent win on the path we are rewriting: it lets the browser
and any CDN cache derived images permanently, complementing the in-memory cache.
Raw serve gains the same header (safe, since bytes never change for an id).

## Safety (the unauthenticated route)

These guards bound the cost an anonymous client can inflict:

1. **Decode concurrency semaphore.** `RTDB_IMAGE_CONCURRENCY` (default 4) permits.
   The decode→resize→encode runs under `tokio::task::spawn_blocking` (CPU-bound —
   never on the async reactor) and only while holding a permit. A permit acquire
   that does not succeed within a short deadline returns `429` (`RATE_LIMITED`,
   `Retry-After`) so the queue cannot be used to tie up connections.
2. **Decode dimension / allocation cap.** The `image` decoder is given `Limits`
   (`max_image_width`, `max_image_height`, `max_alloc ≈ 4 × MAX_PIXELS`). A
   source over `RTDB_IMAGE_MAX_PIXELS` (default 25 MP) fails decode with a
   `Limits` error → mapped to rule 5 above (`400`). This bounds per-request
   memory and CPU.
3. **Output dimension cap.** `w`/`h` are rejected above `RTDB_IMAGE_MAX_DIM`
   (default 2048) — rule 3. Bounded output → bounded encode cost.
4. **Derived cache.** Repeat requests for the same `(id, params)` are served
   from the in-memory cache at near-zero cost (no decode). With single-flight
   (below), a thundering herd of identical requests computes the transform once.
5. **Kill switch.** `RTDB_IMAGE_TRANSFORMS_ENABLED=false` disables transforms
   entirely (every serve is passthrough), for an emergency or opt-out.

The existing per-token / per-db HTTP rate limiter does **not** cover the public
route (no principal), so these transform-specific guards are the primary defense.
The existing per-connection WS flood cap is unrelated.

## Cache & concurrency design

`moka::future::Cache<String, CachedImage>` where `CachedImage = { bytes:
Arc<[u8]>, content_type: &'static str }`:

- **Key:** canonical `"{id}|{w}|{h}|{fit}|{q}|{format}"`.
- **Weigher:** entry weight = `bytes.len()`; `max_capacity` =
  `RTDB_IMAGE_CACHE_BYTES` (default 256 MiB) — moka evicts to stay under the
  byte budget (with a weigher set, `max_capacity` is in weight units).
- **Single-flight:** `try_get_with(key, async { …compute… })` coalesces
  concurrent identical misses into one computation; waiters share the result.
- **Compute closure** (runs only on miss): acquire the concurrency permit
  (inside the closure, so only the single computing task holds one) →
  `storage::get(bytes)` → `spawn_blocking(apply(…))` → return `CachedImage`. On
  `Err`, moka does **not** cache the failure (transient decode issues never
  poison the key).

The error returned from the closure distinguishes kinds the handler maps
differently:

```rust
enum TransformError {
    NotImage,      // decode failed / not an image → handler serves raw bytes
    TooLarge,      // source over MAX_PIXELS                    → 400
    InvalidParams, // shouldn't reach here (validated pre-cache)→ 400
    Internal,      // storage/encode failure                     → 500
}
```

`AppState` holds `image: Arc<TransformCache>`, constructed once at app setup
from the boot `Config` (always constructed; usage gated by `enabled`).

## Configuration (boot `Config`, `RTDB_IMAGE_*`)

New fields on `Config` (`config.rs`), parsed in `from_env` like the existing
`RTDB_TTL_*` / `RTDB_RATE_LIMIT_*` knobs (boot-time, not hot-config — these are
operational capacity knobs, not admin-mutable behavior):

| env var | default | meaning |
|---------|---------|---------|
| `RTDB_IMAGE_TRANSFORMS_ENABLED` | `true` | kill switch |
| `RTDB_IMAGE_MAX_DIM`            | `2048` | max output width/height |
| `RTDB_IMAGE_MAX_PIXELS`         | `25_000_000` | refuse transforming sources above this |
| `RTDB_IMAGE_CACHE_BYTES`        | `268_435_456` (256 MiB) | derived cache byte budget |
| `RTDB_IMAGE_CONCURRENCY`        | `4`    | simultaneous decodes |
| `RTDB_IMAGE_DEFAULT_QUALITY`    | `80`   | default JPEG quality |

All optional with defaults. New `RTDB_*` vars are added to `.env.example`
(commented) **and** the `docker-compose.yml` `environment:` block — the
repo's env-drift check requires compose forwarding for any `RTDB_*` var.

## Module layout (server)

New `server/src/image_transform.rs`:

- `TransformConfig` — built from `Config` (`enabled`, `max_dim`, `max_pixels`,
  `default_quality`, `filter`).
- `TransformParams { w: Option<u32>, h: Option<u32>, fit: Fit, q: Option<u8>,
  format: OutFormat }` + `TransformParams::parse(&HashMap<String,String>) ->
  Result<Option<Self>, RtDbError>` (`None` ⇒ no transform ⇒ passthrough).
- `TransformCache` — wraps the `moka` cache + semaphore + config; `async fn
  get_or_transform(state, db, id, params) -> Result<ServedImage, RtDbError>`.
- `apply(bytes, content_type, params, cfg) -> Result<(Vec<u8>, &'static str),
  TransformError>` — the pure, sync, unit-testable transform (decode → resize →
  encode), using the decoder `Limits` for the pixel cap.
- `TransformError` + mapping to `RtDbError` / status.

`http_api.rs`: `serve_public_handler` and `serve_authed_handler` extract
`Query<HashMap<String,String>>`, parse params; `serve_bytes` gains a
transform-aware path (passthrough when `None`). `AppState` (`lib.rs`) gains the
`image` field, constructed in app setup. `config.rs` gains the six fields. New
deps: `image` (default-features off, features `png`/`jpeg`/`gif`/`bmp`/`webp`),
`moka = "0.12"`.

This is **HTTP-only** (storage is not reactive): no protocol/wire change, no WS
peer, no committer involvement. The four wire implementations are untouched.

## Client mirror (no wire change)

Since this is a query-param convention (not a wire type), the mirror is a thin
URL helper on each client + tests (mirrors how `getUrl` works today):

- **ts-client:** `transformUrl(getUrl(id), { w, h, fit, q, format })` (or
  `storage.transformUrl(id, opts)`) building the canonical query string.
- **rust-client:** `transform_url(id, &TransformOpts)` / `get_transformed_url`.
- **python-client:** `transform_url(id, **opts)`.
- **dashboard:** in the storage browser, a "size" control that copies/preview a
  transformed URL (e.g. a thumbnail) using the helper.

Each client keeps a typed `TransformOpts` (`fit` as an enum) so invalid values
are caught client-side; the server remains the authority and re-validates.

## Metrics

Two Prometheus counters on the transform path (the dashboard metrics page
surfaces these): `rtdb_image_transforms_total{result="hit"|"miss"|"error"}` and
`rtdb_image_transform_bytes_total`. Passthrough serves are not counted.

## Testing (TDD)

**Unit (`image_transform.rs` `#[cfg(test)]`):**
- `parse`: none → `None`; valid → `Some`; out-of-range/unknown → `Err(BadRequest)`.
- `apply`: build a source image with the `image` crate (PNG + JPEG), assert
  `cover`/`contain`/`scale-down` output dimensions; quality reduces JPEG size;
  `format` conversion (png→jpeg); source over `MAX_PIXELS` → `TooLarge`;
  non-image bytes → `NotImage`.

**Integration (dev-db required):**
- Upload a real image; `GET /storage/{id}?w=100` → 200, correct `content-Type`,
  decoded dims ≤ box; second request served (cache idempotent).
- Invalid params (`?w=99999`, `?fit=bogus`) → 400 envelope.
- Non-image upload + `?w=100` → 200 raw bytes, original content-type.
- `Cache-Control: immutable` header present on raw and transformed serves.

**Clients:** unit tests for the URL helper on all three (query-string shape,
enum handling, omit-empty).

## Docs to keep in sync

- `FEATURE_MATRIX.md` row #16 (note transforms).
- `.env.example` + `docker-compose.yml` (the six `RTDB_IMAGE_*` vars).
- `CLAUDE.md` storage bullet + `server/src/storage.rs` module doc (new surface).
- `dashboard/README.md` if the storage-browser control needs it.
- This spec + the implementation plan.

## Non-goals (v1)

- WebP/AVIF output (native deps), animated-GIF frame preservation (first frame
  only), EXIF orientation auto-apply, rotation/crop-offset/blur/sharpen,
  persistent/on-disk cache, client-side transform. These are documented
  follow-ups.
