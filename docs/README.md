# par-rt-db documentation

Index of documentation across the par-rt-db repository. Start here to find the
right doc for a task — operator setup, client integration, design history, or
contribution.

## Project guides

- [`DOCUMENTATION_STYLE_GUIDE.md`](DOCUMENTATION_STYLE_GUIDE.md) — formatting, tone, and structure conventions for every doc in this repo and across the six packages.
- [`OAUTH_SETUP.md`](OAUTH_SETUP.md) — register OAuth apps and wire them into par-rt-db (GitHub, Google, GitLab, Microsoft, Apple, generic OIDC). Each provider is independently optional.

## Top-level docs (repo root)

- [`../README.md`](../README.md) — project overview, quickstart, configuration, and wire protocol.
- [`../CLAUDE.md`](../CLAUDE.md) — authoritative agent guidance: what the project is, the workspace layout, the invariants the codebase depends on, and how to verify work.
- [`../CONTRIBUTING.md`](../CONTRIBUTING.md) — development environment, test/lint/format commands, commit and PR conventions, and the pre-commit hook setup.
- [`../FEATURE_MATRIX.md`](../FEATURE_MATRIX.md) — Convex-parity contract; the source of truth for which features are implemented and mirrored across the four clients.
- [`../CHANGELOG.md`](../CHANGELOG.md) — released changes, following [Keep a Changelog](https://keepachangelog.com/). The historical enhancement candidate list (`ENHANCEMENTS.md`, ENH-001–022) was retired once all enhancements shipped; the backlog now lives on the project kanban board (tagged `enhancement`), and the `ENH-*` ids are preserved there and in `docs/superpowers/plans/*-enh-NNN-*.md` filenames.
- [`../DESIGN.md`](../DESIGN.md) — dashboard visual design system and binding design.
- [`../PRODUCT.md`](../PRODUCT.md) — product framing for the operator console.
- [`../deploy/README.md`](../deploy/README.md) — production deployment runbook for the lenny2 Docker host (build on the x86_64 host, Cloudflare tunnel, secrets, backups, monitoring, rollback).

## Design specs and plans

The [`superpowers/`](superpowers/) directory holds the design history of the project — written with the `superpowers` skill's brainstorm → spec → plan → implement cycle. Specs are the durable design record; plans are the execution breakdown that landed the spec.

- [`superpowers/SPEC_STATUS.md`](superpowers/SPEC_STATUS.md) — at-a-glance status of every spec (implemented, in-progress, or shelved).
- [`superpowers/specs/`](superpowers/specs/) — design specs (33 files): the `2026-07-21-par-rt-db-design.md` main spec is the authoritative protocol/semantics source; later specs cover clients (rust, python, dashboard), per-row authorization, fine-grained subscription invalidation, file storage, scheduling, schema migration, presence, quotas, schema history, image transforms, signed URLs, undo, and more.
- [`superpowers/plans/`](superpowers/plans/) — implementation plans (58 files): per-spec execution breakdowns including the seven-phase realtime-dashboard series (`2026-07-24-realtime-dashboard-phase{1-auth,2-metadata,3a-metrics,3b-opfeed,4-config,5-admin-docs,6-static}.md`).

When the code and a spec disagree, the code wins; fix the spec.

## Per-package READMEs

Each of the six packages has its own README covering install, usage, and development:

- [`../server/README.md`](../server/README.md) — Rust axum/tokio server (the committer, transports, auth, storage, admin API).
- [`../ts-client/README.md`](../ts-client/README.md) — `@par-rt-db/client` (browser/Node): schema builder, reactive WebSocket client, React bindings, HTTP/admin clients, in-memory test harness.
- [`../rust-client/README.md`](../rust-client/README.md) — `par-rt-db-client` (Rust): `http` + reactive `ws` + `admin` features, `.filter()`/`.search()`/`.vector_search()`/`.hybrid_search()` builders.
- [`../python-client/README.md`](../python-client/README.md) — `par-rt-db` (Python): wire contract + schema/mutation/query DSL + sync HTTP/admin/storage + async HTTP twin + reactive WS + in-memory harness.
- [`../dashboard/README.md`](../dashboard/README.md) — Vite + React operator console SPA (databases, schema, live data browser, metrics, op feed, hot config, admin allowlist).
- [`../cli/README.md`](../cli/README.md) — `rtdb` CLI for CI/operator workflows (list/create dbs, push schema, query/mutate, mint/revoke tokens).
