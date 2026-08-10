# Signed, time-limited storage URLs — design

**Status:** Implemented (2026-08-10) — `?exp=&sig=` HMAC-verified URLs on `GET /storage/{id}`; minted by `GET /api/storage/{db}/{id}/signed-url`. ENH-017.

## Problem

Storage blobs are served two ways today (`server/src/http_api.rs`):

- `GET /api/storage/{db}/{id}` — authenticated; the caller's principal must be
  authorized for `{db}`.
- `GET /storage/{id}` — **the one unauthenticated route in the server.** Anyone
  holding the opaque id fetches the bytes, forever. The id is not enumerable, but
  once it has left the server (handed to a browser, emailed, embedded in an
  `<img>`), it grants permanent, irrevocable read access until the blob is
  deleted. There is no way to say "this URL works for one hour, then stops."

That is the gap: a **signed, time-limited URL** — a capability a client mints that
grants read access to one blob until an absolute expiry, verifiable by the server
with no database auth lookup. It is the standard "presigned URL" pattern (S3, GCS)
and the backlog item "Signed, time-limited storage URLs." It lets an app hand a
third party (or a browser `<img>`, or an email link) temporary access without
disclosing a permanent public id and without standing up its own proxy.

This design adds signed URLs as a **new, stricter, additive** access path. The
existing public route keeps working unchanged; signed URLs are an opt-in capability
a client chooses to mint. It does **not** introduce a per-database "private storage
mode" that gates the existing public route behind a signature — that is a larger,
behavior-changing feature and is explicitly out of scope (see below).

## Scope

Server: a new `server/src/signed_url.rs` module (HMAC key derivation, sign,
constant-time verify), a boot-derived signing key held on `AppState`, a new
**mint** endpoint `GET /api/storage/{db}/{id}/signed-url`, and an additive
signature-verify step inside the existing public serve handler
`serve_public_handler` (`http_api.rs`). No committer, subscription, protocol, or
WebSocket involvement — storage is HTTP-only and bypasses the committer today, and
signed URLs change none of that.

Clients: all four clients (TS, Rust, Python, and the TS WS client that delegates to
its HTTP surface) gain `getSignedUrl(id, ttlSeconds?) → { url, expiresAt }`, which
calls the mint endpoint. Minting is a server round-trip — the signing key derives
from the server's `admin_key`, which clients never hold.

Out of scope:

- **Per-database private-storage mode** — flipping the public route to require a
  valid signature per db. That changes existing behavior and needs a db-level
  privacy flag, migration, and dashboard UI; it is a follow-up, not this feature.
  Signed URLs here are purely additive.
- **Signing image-transform parameters.** The signature covers only `{id, exp}`;
  `?w=&h=&q=&format=` compose with a signed URL but are not themselves signed. A
  signed-URL holder can request transforms, bounded by the existing per-IP rate
  limit and decode caps (SEC-004). Signing transforms would add round-trips and
  complexity for little gain; deferred.
- **Per-IP / per-recipient binding** of the token. Signed URLs are bearer
  capabilities: anyone who obtains one can use it until expiry. Binding to an IP
  would break shared CDN caches and `<img>` use. The contract is time-limited
  bearer access, same as S3/GCS presigned URLs.
- **A dedicated opaque-token route** (`/storage/signed/{token}`). Considered and
  rejected in favor of query params on the existing route (see "Token transport").
- **Existence / cross-db validation at mint time.** The id is opaque; existence is
  checked only at serve (404 otherwise). See "Mint semantics."

## Design

### Signing key — derived from `admin_key`, zero-config

The HMAC key is derived once at boot from the already-required `admin_key`:

```
signing_key = HMAC-SHA256(key = admin_key, msg = "rtdb-storage-signing-v1")
```

This is the user-approved choice. Consequences:

- **Always available.** `admin_key` is a required boot value, so the feature is on
  by default with no new environment variable. No `RTDB_*` knob, hence no
  `.env.example` / `docker-compose.yml` change and no env-drift gate breakage.
- **Key separation in practice.** The derived key is domain-separated from the raw
  `admin_key` by the fixed `"rtdb-storage-signing-v1"` label, so a leaked signed
  URL (which exposes only `id`, `exp`, and the *signature* — never the key) cannot
  be turned into an admin credential, and `admin_key` is never placed directly on
  the public serve path's verify code.
- **Rotation revokes.** Rotating `admin_key` changes the derived key and
  invalidates every outstanding signed URL. This is desirable and doubles as
  "revoke all signed access."
- **Held as `Arc<ring::hmac::Key>` on `AppState`**, computed once during `AppState`
  construction (where `config.admin_key` is in scope) and shared by every request.
  `ring` is already a dependency (the Apple ES256 JWT backend uses it), so no new
  crate. `ring::hmac::verify` performs the constant-time comparison internally.

### Token transport — query params on the existing public route

Three transports were considered.

**A. Query params on the existing public route (adopted).**

```
GET /storage/{id}?exp=<unix-ms>&sig=<hex-hmac>
```

The public handler checks for `exp` + `sig`: if both are present it verifies;
otherwise it serves exactly as today. Backward compatible, no new route, image
transforms compose naturally (`?exp=&sig=&w=&h=`), and `serve_bytes` is reused
verbatim — the verify step only decides *whether* to serve, then hands off to the
existing resolve-db → `serve_bytes` path.

**B. A dedicated opaque-token route** — `GET /storage/signed/{base64url(id.exp.sig)}`.
Cleaner separation and an opaque token that hides the id, but a new route, a new
token encoding, and a parallel serve path. More surface to build and test for no
functional gain over A.

**C. Token replaces the id in the path** — collides with the opaque-id route
semantics (`resolve_db` keys on the path segment). Ambiguous and brittle. Rejected.

### Token format

```
sig = hex( HMAC-SHA256(signing_key, "{id}.{exp}") )
exp = absolute expiry, unix milliseconds (server clock)
```

- The signature covers only `{id}.{exp}` — the two values that define the
  capability. Transform query params are intentionally excluded (see scope).
- `exp` is absolute (not a relative TTL) so the URL is self-describing and the
  server needs only its own clock to decide validity — no state, no DB lookup.
- Hex (not base64) for the signature, to keep the URL free of `+/=` URL-encoding
  hazards. The id and exp travel as plain query values.

### Verify path (`serve_public_handler`, additive)

Inside `serve_public_handler`, before the existing resolve-db + serve:

1. Read `exp` and `sig` from the query map.
2. **Both absent** → today's public behavior (unchanged). No regression.
3. **One present, other absent, or `exp` unparseable** → `403 FORBIDDEN`
   ("invalid or expired signature"). A partial signature is never valid.
4. **`exp` present and `now > exp`** → `403 FORBIDDEN`.
5. **Recompute** `HMAC-SHA256(signing_key, "{id}.{exp}")` and constant-time-compare
   to `sig` via `ring::hmac::verify`. Mismatch → `403 FORBIDDEN`.
6. On success → proceed to the existing `resolve_db` → `serve_bytes` (transforms,
   Range, immutable cache headers all apply unchanged).

All failure cases use the existing `RtDbError::forbidden` (HTTP 403, code
`FORBIDDEN`) with a generic message. A valid signature does **not** bypass the
per-IP public-serve rate limit (SEC-004): the rate limit stays, so a signed-URL
holder cannot amplify transform cost beyond today's bounds.

The public route remains unauthenticated in the bearer/session sense — the
signature *is* the capability. The CLAUDE.md invariant "`GET /storage/{id}` is the
one unauthenticated route" still holds; this only adds an optional capability check
on it.

### Mint endpoint

```
GET /api/storage/{db}/{id}/signed-url?ttlSeconds=<n>
```

- **Auth:** the same `bearer_token` → `resolve_bearer` → `authorize(pool, &principal, &db)`
  triple as the authed serve route, so only a principal authorized for `{db}` can
  mint a URL for its blobs. `check_http_rate_limits` applies.
- **`ttlSeconds`** optional, default `3600` (1 hour), clamped to
  `[1, MAX_SIGNED_URL_TTL_SECS]`. `MAX_SIGNED_URL_TTL_SECS` is a **compile-time
  `const` = 7 days (604_800)** — not an env knob, to keep the feature zero-config
  (raising it is a code change).
- Computes `exp = now_ms + ttl_seconds * 1000` and `sig`, then returns:

  ```json
  { "url": "<public_url>/storage/<id>?exp=<ms>&sig=<hex>", "expiresAt": <ms> }
  ```

  using `config.public_url` as the base so the returned URL is externally
  resolvable (mirrors how the clients already build `${public_url}/storage/{id}`).
- **GET, not POST:** it returns data (a URL for an id) and is effectively a read;
  idempotent for the same `ttlSeconds`. No state is written (minting is pure
  computation — no committer, no DB write).

### Mint semantics — no existence / cross-db check

The mint endpoint does **not** verify the blob exists or that `{id}` belongs to
`{db}`. Rationale:

- The id is opaque and existence is checked at serve (404 otherwise). A signed URL
  minted for a non-existent id simply yields 404 when fetched — harmless.
- A db-A principal minting a URL for an id they happen to know from db-B is **no
  new leak**: the public route already serves any id to anyone, regardless of db.
  The auth on the mint endpoint protects the *act of minting* (it is a db-authorized
  capability), not knowledge of an id.
- Skipping the lookup keeps minting a pure, stateless computation — no DB
  round-trip on the mint path.

### Client surface (all four clients)

A single new async method each, since minting is a server round-trip:

| Client | Method |
|---|---|
| TS (`RtDbHttpClient` / `RtDbClient`) | `getSignedUrl(id: string, ttlSeconds?: number): Promise<{ url: string; expiresAt: number }>` |
| Rust (`RtDbHttpClient` / `RtDbClient`) | `get_signed_url(&self, id: &str, ttl_seconds: Option<u64>) -> Result<SignedUrl, RtDbError>` |
| Python (`HttpClient` / `RtDbClient`) | `get_signed_url(id: str, *, ttl_seconds: int | None = None) -> SignedUrl` |

Each issues `GET /api/storage/{db}/{id}/signed-url?ttlSeconds=` and parses
`{ url, expiresAt }` (camelCase on the wire). The TS reactive client delegates to
its HTTP surface, exactly as `getUrl` / `getFileMetadata` do today.

Clients do **not** perform any HMAC — the key is server-side. Existing `getUrl(id)`
/ `get_url(id)` are unchanged (still the permanent public URL).

### Why mint server-side, not client-side

A client cannot mint locally because it does not hold (and must not hold) the
signing key, which derives from `admin_key`. The only alternatives — distributing a
per-database signing key to authorized clients, or having clients hold `admin_key`
— are both unacceptable security regressions. So minting is necessarily a server
round-trip. This matches S3/GCS presigned-URL SDKs, where the SDK holds credentials
and mints by calling into signing logic, not by the application holding a raw
server secret.

## Components touched

**Server**
- `server/src/signed_url.rs` (new): `derive_signing_key(admin_key) -> ring::hmac::Key`,
  `sign(key, id, exp_ms) -> String` (hex), `verify(key, id, exp_ms, sig_hex) -> bool`.
- `server/src/lib.rs`: add `pub signed_url_key: Arc<ring::hmac::Key>` to `AppState`,
  derived once at construction.
- `server/src/http_api.rs`: `signed_url_handler` (mint, new route), an additive
  `verify_signed` step in `serve_public_handler`, and route registration of
  `/api/storage/{db}/{id}/signed-url`. Reuse `RtDbError::forbidden`.
- `server/tests/storage_signed_url_test.rs` (new): integration tests (see Testing).

**Clients (mirror)**
- TS: `ts-client/src/http.ts` (`getSignedUrl` + `SignedUrl` type), `client.ts`
  (delegate), tests.
- Rust: `rust-client/src/http.rs` (`get_signed_url` + `SignedUrl` struct), `lib.rs`
  (delegate), tests.
- Python: `python-client/src/par_rt_db/http_client.py` (`get_signed_url` +
  `SignedUrl` dataclass), `__init__.py` (export), tests.

**Docs**
- `FEATURE_MATRIX.md` (storage row), `server/README.md`, the three client READMEs,
  a `CLAUDE.md` sentence noting the public route now optionally honors a signature,
  and this spec. **No `.env.example` / `docker-compose.yml` change** (zero new env
  vars).

## Invariants preserved

- **Single-writer committer** — untouched. Minting is pure computation; the verify
  step is a read-side check on the existing serve path. No new write path.
- **Public route stays public** — additive only. A request with no `exp`/`sig`
  behaves exactly as before.
- **Constant-time compare** — `ring::hmac::verify`.
- **SQL / errors** — no new SQL; failures reuse `RtDbError::forbidden` (403). No
  internal error stringified into a response body.
- **Client parity** — server is source of truth; all four clients mirror the new
  endpoint and `{url, expiresAt}` shape.
- **Op-feed / audit / webhook tap sites** — unaffected (no document writes).

## Testing

**Server integration** (`server/tests/storage_signed_url_test.rs`, real dev Postgres
via the existing test harness):
- Mint then fetch `/storage/{id}?exp=&sig=` → `200`, correct bytes + content type.
- Expired (`exp` in the past) → `403`.
- Tampered `sig` (one hex char flipped) → `403`.
- Tampered `id` or `exp` with original `sig` → `403`.
- `exp` present without `sig` (and vice versa) → `403`.
- No `exp`/`sig` → public `200` (unchanged behavior).
- TTL clamping: `ttlSeconds` above the 7-day cap clamps; `0`/negative clamps to the
  minimum; default applied when omitted.
- Image-transform params compose with a signed URL (`?exp=&sig=&w=&h=` → `200`,
  transformed).
- Auth on mint: a bearer authorized for db-A mints a db-A URL (`200`); a bearer
  **not** authorized for db-A minting a db-A id → `403`; missing/invalid bearer →
  `401`.

**Client tests** (each client's existing test harness, mocked HTTP): verify
`getSignedUrl` issues the correct request (path + `ttlSeconds` query, including
default-omitted vs. explicit) and parses `{url, expiresAt}` into the right shape.
No crypto in clients.

**Gate.** `make checkall` (fmt-check + clippy `-D warnings` + typecheck + tests)
must pass across all six packages before merge.
