# par-rt-db documentation

Index of documentation across the par-rt-db repository. Start here to find the
right doc for a task — operator setup, client integration, design history, or
contribution.

## Project guides

- [`DOCUMENTATION_STYLE_GUIDE.md`](DOCUMENTATION_STYLE_GUIDE.md) — formatting, tone, and structure conventions for every doc in this repo and across the eight workspace packages.
- [`ARCHITECTURE.md`](ARCHITECTURE.md) — server internals: the single-writer committer, per-database background tasks, data pipeline, transports, auth, storage, quotas, and the admin surface — with the reasoning behind each invariant.
- [`OAUTH_SETUP.md`](OAUTH_SETUP.md) — register OAuth apps and wire them into par-rt-db (GitHub, Google, GitLab, Microsoft, Apple, generic OIDC). Each provider is independently optional.
- [`RELEASING.md`](RELEASING.md) — the repeatable release procedure: lockstep version bump, CHANGELOG heading, gate, annotated tag, push (owner-confirmed).
- [`clients.md`](clients.md) — the four client SDKs (TypeScript/Rust/Python/Swift) at a glance: surface comparison, the five-way wire parity contract, and links to each package's detailed README.

## Generated API references

The four SDK reference sites are generated from source and checked by
`make docs-api`. The local outputs are `target/doc/par_rt_db_client`,
`ts-client/docs-api`, `python-client/docs-api`, and `swift-client/docs-api`.
Tagged releases publish the same sites together on GitHub Pages.

- [TypeScript API reference](https://paulrobello.github.io/par-rt-db/ts/)
- [Rust API reference](https://paulrobello.github.io/par-rt-db/rust/par_rt_db_client/)
- [Python API reference](https://paulrobello.github.io/par-rt-db/python/par_rt_db.html)
- [Swift API reference](https://paulrobello.github.io/par-rt-db/swift/)

## Top-level docs (repo root)

- [`../README.md`](../README.md) — project overview, quickstart, configuration, and wire protocol.
- [`../CLAUDE.md`](../CLAUDE.md) — authoritative agent guidance: what the project is, the workspace layout, the invariants the codebase depends on, and how to verify work.
- [`../CONTRIBUTING.md`](../CONTRIBUTING.md) — development environment, test/lint/format commands, commit and PR conventions, and the pre-commit hook setup.
- [`../FEATURE_MATRIX.md`](../FEATURE_MATRIX.md) — Convex-parity contract; the source of truth for which features are implemented and mirrored across the four clients.
- [`../CHANGELOG.md`](../CHANGELOG.md) — released changes, following [Keep a Changelog](https://keepachangelog.com/). The historical enhancement candidate list (`ENHANCEMENTS.md`, ENH-001–022) was retired once all enhancements shipped; the backlog now lives on the project kanban board (tagged `enhancement`), and the `ENH-*` ids are preserved there and in `docs/superpowers/plans/*-enh-NNN-*.md` filenames.
- [`../DESIGN.md`](../DESIGN.md) — dashboard visual design system (mode, palette, typography, components, motion).
- [`../PRODUCT.md`](../PRODUCT.md) — product framing for the operator console.
- [`../deploy/README.md`](../deploy/README.md) — production deployment runbook (standalone Docker host: build on the x86_64 host, Cloudflare tunnel, secrets, backups, monitoring, rollback).

## Design specs and plans

The [`superpowers/`](superpowers/) directory holds the design history of the project — written with the `superpowers` skill's brainstorm → spec → plan → implement cycle. Specs are the durable design record; plans are the execution breakdown that landed the spec.

- [`superpowers/SPEC_STATUS.md`](superpowers/SPEC_STATUS.md) — at-a-glance status of every spec (implemented, in-progress, or shelved).
- [`superpowers/specs/`](superpowers/specs/) — design specs. `2026-07-21-par-rt-db-design.md` is the original design, kept as a historical record (the root README, `FEATURE_MATRIX.md`, and `wire-corpus/` define current protocol and semantics); later specs cover clients (rust, python, dashboard), per-row authorization, fine-grained subscription invalidation, file storage, scheduling, schema migration, presence, quotas, schema history, image transforms, signed URLs, durable workflows, full-text search, cascade delete, field defaults, and more.
- [`superpowers/plans/`](superpowers/plans/) — implementation plans: per-spec execution breakdowns including the seven-phase realtime-dashboard series (`2026-07-24-realtime-dashboard-phase{1-auth,2-metadata,3a-metrics,3b-opfeed,4-config,5-admin-docs,6-static}.md`).
- [`fable/`](fable/) — enhancement plan docs from the fable-audit cycle; currently `ENH-029-multi-instance-test-harness.md` covers a reusable two-replica test harness with failure injection. Completed plans are removed once shipped, with outcomes recorded in [`../CHANGELOG.md`](../CHANGELOG.md).

When the code and a spec disagree, the code wins; fix the spec.

## Per-package READMEs

The workspace holds eight packages (`core`, `server`, `ts-client`, `rust-client`,
`python-client`, `swift-client`, `dashboard`, `cli`); seven have their own README
covering install, usage, and development (`core` is an internal wire-types crate
with no consumer-facing surface of its own):

- [`../server/README.md`](../server/README.md) — Rust axum/tokio server (the committer, transports, auth, storage, admin API).
- [`../ts-client/README.md`](../ts-client/README.md) — `@par-rt-db/client` (browser/Node): schema builder, reactive WebSocket client, React bindings, HTTP/admin clients, in-memory test harness.
- [`../rust-client/README.md`](../rust-client/README.md) — `par-rt-db-client` (Rust): `http` + reactive `ws` + `admin` features, `.filter()`/`.search()`/`.vector_search()`/`.hybrid_search()` builders.
- [`../python-client/README.md`](../python-client/README.md) — `par-rt-db` (Python): wire contract + schema/mutation/query DSL + sync HTTP/admin/storage + async HTTP twin + reactive WS + in-memory harness.
- [`../swift-client/README.md`](../swift-client/README.md) — `ParRtDbClient` / `ParRtDbUI` (Swift): wire contract + schema/mutation/query DSL + HTTP/admin/storage clients + reactive WS + in-memory harness + SwiftUI `LiveQuery` bindings.
- [`../dashboard/README.md`](../dashboard/README.md) — Vite + React operator console SPA (databases, schema, live data browser, metrics, op feed, hot config, admin allowlist).
- [`../cli/README.md`](../cli/README.md) — `rtdb` CLI for CI/operator workflows (list/create dbs, push schema, query/mutate, mint/revoke tokens).
