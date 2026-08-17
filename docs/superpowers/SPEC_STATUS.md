# Spec Status Index

A single index mapping every design spec in `docs/superpowers/specs/` to its
ship status, FEATURE_MATRIX row, and follow-on spec (if any). Update this file
when a spec's Status line flips or a new spec lands.

**Convention:** "Implemented" means the feature is live and the code is the
authoritative source of truth; the spec is a historical design record. Point at
`FEATURE_MATRIX.md` for current capability/parity state, never at the spec.

## Legend

| Column | Meaning |
| --- | --- |
| Spec | File in `docs/superpowers/specs/` |
| Shipped | First verified-live date (matches the FEATURE_MATRIX "last updated" or commit history) |
| FEATURE_MATRIX | Row in `FEATURE_MATRIX.md` §1 ("At parity today") or §2 (ranked gap matrix, #1–#35) |
| Follow-on | Later spec that extended or completed this one |

## Status table

| Spec | Status | Shipped | FEATURE_MATRIX | Follow-on |
| --- | --- | --- | --- | --- |
| `2026-07-21-par-rt-db-design.md` | Implemented (MVP server + expansions) | 2026-07-21 (server); see expansions below | §1 (all rows) + §2 (#1–#35) | All specs below are follow-ons |
| `2026-07-22-mutation-idempotency-design.md` | Implemented | shipped with FEATURE_MATRIX #4 | §2 #4 | — |
| `2026-07-22-rust-client-design.md` | Implemented | v0.1.0 published | §1 ("Admin control plane" names the Rust client) + §2 rows noting "Mirrored end-to-end … rust-client" (#1, #2, #3, #5, #6, #9, #11, #15, #16, #17) | `2026-07-25-python-client-design.md` |
| `2026-07-22-typed-mutation-builder-design.md` | Implemented | shipped | §2 #6 (`replace` step) and the mutation DSL across rows | — |
| `2026-07-23-file-storage-design.md` | Implemented | shipped | §2 #16 | — |
| `2026-07-23-scheduled-cron-transactions-design.md` | Implemented | shipped | §2 #9 (scheduler) + #10 (cron) | — |
| `2026-07-23-vector-search-design.md` | Implemented (verified live in prod) | 2026-07-25 | §2 #17 | — |
| `2026-07-24-fine-grained-subscription-invalidation-design.md` | Implemented (v3: point reads + eq-prefix/range + ordered top-N) | shipped (v3 2026-07-29) | §2 #21 | — |
| `2026-07-24-per-row-authorization-design.md` | Implemented (v1–v3) | shipped | §2 #20 | — |
| `2026-07-24-realtime-dashboard-design.md` | Implemented (backend phases 1–6 + frontend SPA) | shipped | §2 #18 | — |
| `2026-07-25-python-client-design.md` | Implemented (core DSL + sync HTTP/admin/storage + reactive WS) | 2026-07-25 | §1 (the "fourth client"; per-row "Mirrored across: ✅ts ✅rust ✅python" tracks parity) | — |
| `2026-07-26-client-completeness-sweep-design.md` | Implemented (2026-08-10) | shipped | §1 ("four clients") + §2 mirror rows ("Mirrored end-to-end" across ts/rust/python) | — |
| `2026-07-27-metrics-graphs-design.md` | Implemented (2026-08-10) | shipped | §2 #18 (dashboard) | — |
| `2026-07-28-int64-indexable-and-inmemory-schema-evolution-design.md` | Implemented (2026-08-10) | shipped | §2 #13 (int64 + extra validators); in-memory additive schema evolution | — |
| `2026-07-31-schema-migration-backfill-design.md` | Implemented (2026-08-10) | shipped | Schema migrate (admin expansion; `CommitterRequest::RunMigrate`) | — |
| `2026-08-01-document-ttl-design.md` | Implemented (2026-08-10) | shipped | §2 #23 (Document TTL / auto-expiry) | — |
| `2026-08-01-oauth-relay-redesign.md` | Implemented (2026-08-10) | shipped | Auth (SEC-012 popup `noopener` relay) | — |
| `2026-08-01-unique-indexes-design.md` | Implemented (2026-08-10) | shipped | §2 #22 (Unique + partial `WHERE` index constraints) | — |
| `2026-08-01-ws-rate-limit-design.md` | Implemented (2026-08-10) | shipped | Auth / hardening (WS message-level rate limiting) | — |
| `2026-08-02-login-csrf-hardening-design.md` | Implemented | 2026-08-02 | Auth section | — |
| `2026-08-02-per-row-auth-predicate-dsl-design.md` | Implemented (2026-08-10) | shipped | §2 #20 (Per-row auth — `authorize` predicate DSL, v3) | — |
| `2026-08-03-close-expect-version-absent-side-channel-design.md` | Implemented (2026-08-10) | shipped | §2 #20 follow-on (closes ExpectVersion/ExpectAbsent side-channel) | — |
| `2026-08-04-backups-dashboard-design.md` | Implemented (2026-08-10) | shipped | ENH-002 (Backups dashboard) | — |
| `2026-08-04-operator-console-and-token-scoping-design.md` | Implemented (2026-08-10) | shipped | ENH-003 / ENH-004 / ENH-005 / ENH-010 (operator console, webhooks, audit log, sub-inspector, scoped tokens) | — |
| `2026-08-04-test-db-raii-teardown-design.md` | Implemented (2026-08-10) | shipped | Test infra (`TestDb` RAII drop in `server/tests/common/`) | — |
| `2026-08-05-image-transforms-design.md` | Implemented (2026-08-10) | shipped | ENH-014 (On-the-fly image transforms) | — |
| `2026-08-05-schema-change-history-design.md` | Implemented (2026-08-10) | shipped | §2 #24 (Schema change history); ENH-013 | — |
| `2026-08-06-presence-design.md` | Implemented (2026-08-10) | shipped | §2 #25 (Realtime presence); ENH-015 | `2026-08-06-presence-ttl-design.md` |
| `2026-08-06-presence-ttl-design.md` | Implemented (2026-08-10) | shipped | §2 #25 follow-on (per-state presence TTL); ENH-015 | — |
| `2026-08-07-per-db-resource-quotas-design.md` | Implemented (2026-08-10) | shipped | §2 #26 (Per-database resource quotas); ENH-011 | — |
| `2026-08-08-active-session-management-design.md` | Implemented (2026-08-10) | shipped | Admin (active-session list + revoke) | — |
| `2026-08-08-signed-storage-urls-design.md` | Implemented (2026-08-10) | shipped | ENH-017 (Signed, time-limited storage URLs) | — |
| `2026-08-09-search-filter-design.md` | Implemented (2026-08-10) | shipped | §2 #11 follow-on (full `FilterExpr` on `search`) | — |
| `2026-08-14-anon-merge-design.md` | Implemented | shipped (recorded 2026-08-16) | §2 #35 (Anonymous → real account merge; FM-27 + FM-35) | — |
| `2026-08-14-step-schedule-design.md` | Implemented | shipped (recorded 2026-08-16) | §2 #28 (`Step::Schedule`, FM-28) | — |
| `2026-08-15-phrase-search-snippets-design.md` | Implemented | shipped (recorded 2026-08-16) | §2 #31 (Phrase / operator search + snippets, FM-31) | — |
| `2026-08-15-trgm-search-design.md` | Implemented | shipped (recorded 2026-08-16) | §2 #30 (Substring / autocomplete search, FM-30) | — |
| `2026-08-15-workflows-design.md` | Implemented | shipped (recorded 2026-08-16) | §2 #29 (Durable declarative workflows, FM-29) | — |
| `2026-08-16-cascade-delete-soft-delete-design.md` | Implemented | shipped (recorded 2026-08-16) | §2 #33 (Cascade delete + soft delete, FM-33) | — |
| `2026-08-16-field-defaults-design.md` | Implemented | shipped (recorded 2026-08-16) | §2 #32 (Field-level default values, FM-32) | — |

## Notes

- **MVP spec expansions.** The MVP spec (`2026-07-21-par-rt-db-design.md`)
  listed **eleven** features as "out of scope" in 2026-07-21: actions, file
  storage, scheduler, cron jobs, pagination, db-side `.filter()`, `.first()`,
  `.replace()`, text/vector search, optimistic updates, per-row authorization
  rules. The list in the spec body cross-references the FEATURE_MATRIX row and
  follow-on spec for each. "Actions" remains a deliberate non-goal
  (FEATURE_MATRIX §3); see `FEATURE_MATRIX.md` for the current ship state of the
  remaining ten.
- **Per-row auth / fine-grained invalidation.** Per-row auth has shipped through
  v3 (owner-field match, then collaborator/role fields, then the `authorize`
  general declarative predicate DSL, 2026-08-02); fine-grained invalidation has
  shipped through v3 (count/collect/unique on eq-prefix + range, 2026-07-28;
  take/first/paginate via top-N boundary tracking, 2026-07-29). Invalidation's
  remaining deferrals are documented in its own spec — cursor-aware page lower
  bounds, and the value-sensitive / ranking shapes (distinct, aggregate,
  search, vector, hybrid), which no window-plus-boundary reasoning can soundly
  cover.
- **Python client.** The wire + schema + query + mutation DSL, a sync `httpx`
  client (query/mutate, admin, storage — `pip install par-rt-db[http]`), and
  the reactive WebSocket client (`pip install par-rt-db[ws]`) all ship; the
  four clients are at feature parity.
- **The code is the source of truth.** When a spec and the implementation
  disagree, the implementation (and `FEATURE_MATRIX.md`) wins. Specs are linked
  from READMEs as historical design context, not as authoritative behavior
  references.
