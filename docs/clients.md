# Clients

par-rt-db ships three client SDKs — TypeScript, Rust, and Python — that each
mirror the server's wire contract directly. The server is the source of truth
for the protocol, the query DSL, step-result shapes, and behavior; the SDKs
are no-codegen (a schema object is both pushed to the server and the source of
inferred types). This page is the hub: what each SDK covers and where its
detailed documentation lives.

## At a glance

| SDK | Package | Path | Detailed docs |
| --- | --- | --- | --- |
| TypeScript | `@par-rt-db/client` | [`ts-client/`](../ts-client) | [`ts-client/README.md`](../ts-client/README.md) |
| Rust | `par-rt-db-client` | [`rust-client/`](../rust-client) | [`rust-client/README.md`](../rust-client/README.md) |
| Python | `par-rt-db` | [`python-client/`](../python-client) | [`python-client/README.md`](../python-client/README.md) |

No package has been published to a registry yet (tracked as ENH-026); consume
each SDK from this repo — see its README for workspace setup.

## Surface comparison

| Capability | TypeScript | Rust | Python |
| --- | --- | --- | --- |
| Schema builder (no codegen) | ✅ | ✅ | ✅ |
| Reactive WebSocket client | ✅ | ✅ (`ws` feature) | ✅ |
| One-shot HTTP client | ✅ | ✅ (`http` feature) | ✅ (`par-rt-db[http]` extra) |
| Admin API client | ✅ | ✅ (`admin` feature) | ✅ |
| File storage upload/download | ✅ | ✅ | ✅ |
| Query DSL builders (`.filter()` / `.search()` / `.vector_search()`) | ✅ | ✅ | ✅ |
| In-memory test harness | ✅ | ✅ (`in_memory` feature) | ✅ (`par_rt_db.in_memory`) |
| Optimistic updates | — | ✅ | ✅ |
| React bindings (`@par-rt-db/client/react`) | ✅ | — | — |

Feature-level parity (which clients mirror which server capability) is
tracked per row in [`FEATURE_MATRIX.md`](../FEATURE_MATRIX.md).

## The parity contract

Four implementations of one protocol must stay byte-identical —
[`server/src/protocol.rs`](../server/src/protocol.rs),
[`ts-client/src/protocol.ts`](../ts-client/src/protocol.ts),
[`rust-client/src/wire.rs`](../rust-client/src/wire.rs), and
[`python-client/src/par_rt_db/wire.py`](../python-client/src/par_rt_db/wire.py)
(serde tags and field names — the casing is deliberately non-uniform and
load-bearing). Two mechanisms keep them honest:

- **Any server behavior change must be mirrored in all three clients** — wire
  types, DSL builders, and their tests. See the wire-mirror rule in
  [`CONTRIBUTING.md`](../CONTRIBUTING.md).
- **The [`wire-corpus/`](../wire-corpus/README.md) semantics corpus** — every
  case is executed by all four runners (the live server plus the three
  in-memory engines), and every behavior-changing change ships with a case.

The in-memory harnesses double as offline test engines: code that passes
against them behaves the same against the real server, which is what the
corpus asserts.

## Built on the clients

- [`dashboard/`](../dashboard) — the operator console SPA, consumes
  `@par-rt-db/client`.
- [`cli/`](../cli) — the `rtdb` operator/CI binary, wraps
  `par-rt-db-client` (its README reference is generated and drift-gated).
