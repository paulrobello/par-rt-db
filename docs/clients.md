# Clients

par-rt-db ships six client SDKs — TypeScript, Rust, Python, Swift, Go, and
Kotlin — that each mirror the server's wire contract directly. The server is
the source of truth for the protocol, the query DSL, step-result shapes, and
behavior; the SDKs are no-codegen (a schema object is both pushed to the
server and the source of inferred types). This page is the hub: what each SDK
covers and where its detailed documentation lives.

## At a glance

| SDK | Package | Path | Detailed docs |
| --- | --- | --- | --- |
| TypeScript | `@par-rt-db/client` | [`ts-client/`](../ts-client) | [`ts-client/README.md`](../ts-client/README.md) |
| Rust | `par-rt-db-client` | [`rust-client/`](../rust-client) | [`rust-client/README.md`](../rust-client/README.md) |
| Python | `par-rt-db` | [`python-client/`](../python-client) | [`python-client/README.md`](../python-client/README.md) |
| Swift | `ParRtDbClient` / `ParRtDbUI` | [`swift-client/`](../swift-client) | [`swift-client/README.md`](../swift-client/README.md) |
| Go | `github.com/paulrobello/par-rt-db/go-client` | [`go-client/`](../go-client) | [`go-client/README.md`](../go-client/README.md) |
| Kotlin | `dev.parrtdb:par-rt-db-kotlin-client` | [`kotlin-client/`](../kotlin-client) | [`kotlin-client/README.md`](../kotlin-client/README.md) |

No package has been published to a registry yet; consume
each SDK from this repo — see its README for workspace setup. Registry
publication is tag-driven by [`release.yml`](../.github/workflows/release.yml)
and runs only after the full verification gate. Swift is the exception to local
consumption: the repo's root `Package.swift` lets a Swift consumer add the
package as a remote git dependency with `exact:` semantics — the declared
semantic version (no `v` prefix) resolves against the corresponding `v`-tagged
release.

## Surface comparison

| Capability | TypeScript | Rust | Python | Swift | Go | Kotlin |
| --- | --- | --- | --- | --- | --- | --- |
| Schema builder (no codegen) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Reactive WebSocket client | ✅ | ✅ (`ws` feature) | ✅ (`par-rt-db[ws]` extra) | ✅ | ✅ | ✅ (`Flow`-based, pluggable transport) |
| One-shot HTTP client | ✅ | ✅ (`http` feature) | ✅ (`par-rt-db[http]` extra) | ✅ | ✅ | ✅ (pluggable transport) |
| Admin API client | ✅ | ✅ (`admin` feature) | ✅ (via `[http]` extra) | ✅ | ✅ | ✅ |
| File storage (upload, delete, serve/signed URLs) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Query DSL builders (`.filter()` / `.search()` / `.vector_search()`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| In-memory test harness | ✅ | ✅ (`in_memory` feature) | ✅ (`par_rt_db.in_memory`) | ✅ | ✅ | ✅ |
| Optimistic updates | ✅ | ✅ | ✅ | ✅ | ✅ (manual `optimistic.Layer` composition) | — |
| React bindings (`@par-rt-db/client/react`) | ✅ | — | — | — | — | — |
| SwiftUI bindings (`ParRtDbUI`) | — | — | — | ✅ | — | — |

Feature-level parity (which clients mirror which server capability) is
tracked per row in [`FEATURE_MATRIX.md`](../FEATURE_MATRIX.md).

## The parity contract

Seven implementations of one protocol must stay byte-identical —
[`server/src/protocol.rs`](../server/src/protocol.rs),
[`ts-client/src/protocol.ts`](../ts-client/src/protocol.ts),
[`rust-client/src/wire.rs`](../rust-client/src/wire.rs),
[`python-client/src/par_rt_db/wire.py`](../python-client/src/par_rt_db/wire.py),
[`swift-client/Sources/ParRtDbClient/Wire.swift`](../swift-client/Sources/ParRtDbClient/Wire.swift),
[`go-client/wire/`](../go-client/wire), and
[`kotlin-client/src/main/kotlin/dev/parrtdb/wire/`](../kotlin-client/src/main/kotlin/dev/parrtdb/wire)
(serde tags and field names — the casing is deliberately non-uniform and
load-bearing). Two mechanisms keep them honest:

- **Any server behavior change must be mirrored in all six clients** — wire
  types, DSL builders, and their tests. See the wire-mirror rule in
  [`CONTRIBUTING.md`](../CONTRIBUTING.md).
- **The [`wire-corpus/`](../wire-corpus/README.md) semantics corpus** — every
  case is executed by all seven runners (the live server plus the six
  in-memory engines), and every behavior-changing change ships with a case.

The in-memory harnesses double as offline test engines: code that passes
against them behaves the same against the real server, which is what the
corpus asserts.

## Built on the clients

- [`dashboard/`](../dashboard) — the operator console SPA, consumes
  `@par-rt-db/client`.
- [`cli/`](../cli) — the `rtdb` operator/CI binary, wraps
  `par-rt-db-client` (its README reference is generated and drift-gated).
