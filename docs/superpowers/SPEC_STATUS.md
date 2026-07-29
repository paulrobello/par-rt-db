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
| FEATURE_MATRIX | Row in `FEATURE_MATRIX.md` §1 ("At parity today") or §2 (ranked gap matrix, #1–#21) |
| Follow-on | Later spec that extended or completed this one |

## Status table

| Spec | Status | Shipped | FEATURE_MATRIX | Follow-on |
| --- | --- | --- | --- | --- |
| `2026-07-21-par-rt-db-design.md` | Implemented (MVP server + expansions) | 2026-07-21 (server); see expansions below | §1 (all rows) + §2 (#1–#21) | All specs below are follow-ons |
| `2026-07-22-mutation-idempotency-design.md` | Implemented | shipped with FEATURE_MATRIX #4 | §2 #4 | — |
| `2026-07-22-rust-client-design.md` | Implemented | v0.1.0 published | §1 ("Admin control plane" names the Rust client) + §2 rows noting "Mirrored end-to-end … rust-client" (#1, #2, #3, #5, #6, #9, #11, #15, #16, #17) | `2026-07-25-python-client-design.md` |
| `2026-07-22-typed-mutation-builder-design.md` | Implemented | shipped | §2 #6 (`replace` step) and the mutation DSL across rows | — |
| `2026-07-23-file-storage-design.md` | Implemented | shipped | §2 #16 | — |
| `2026-07-23-scheduled-cron-transactions-design.md` | Implemented | shipped | §2 #9 (scheduler) + #10 (cron) | — |
| `2026-07-23-vector-search-design.md` | Implemented (verified live in prod) | 2026-07-25 | §2 #17 | — |
| `2026-07-24-fine-grained-subscription-invalidation-design.md` | Implemented (v3: point reads + eq-prefix/range + ordered top-N) | shipped (v3 2026-07-29) | §2 #21 | — |
| `2026-07-24-per-row-authorization-design.md` | Implemented (v1: owner-field match) | shipped | §2 #20 | v2 (collaborator/role fields) and v3 (general declarative predicate DSL) not yet specced |
| `2026-07-24-realtime-dashboard-design.md` | Implemented (backend phases 1–6 + frontend SPA) | shipped | §2 #18 | — |
| `2026-07-25-python-client-design.md` | Implemented (core DSL + sync HTTP/admin/storage); reactive WS client pending | 2026-07-25 | §1 (the "fourth client"; per-row "Mirrored across: ✅ts ✅rust ✅python" tracks parity) | follow-on plan TBD for the reactive WS surface |

## Notes

- **MVP spec expansions.** The MVP spec (`2026-07-21-par-rt-db-design.md`)
  listed nine features as "out of scope" in 2026-07-21: file storage, scheduler,
  cron, pagination, db-side `.filter()`, `.first()`, `.replace()`, text/vector
  search, per-row auth. Eight of the nine have since shipped; the list in the
  spec body now cross-references the FEATURE_MATRIX row and follow-on spec for
  each. "Actions" remains a deliberate non-goal (FEATURE_MATRIX §3).
- **Per-row auth / fine-grained invalidation.** Per-row auth has shipped through
  v2 (collaborator/role fields); fine-grained invalidation has shipped through
  v3 (count/collect/unique on eq-prefix + range, 2026-07-28; take/first/paginate
  via top-N boundary tracking, 2026-07-29). Remaining unspecced: per-row auth v3
  (general declarative predicate DSL). Invalidation's remaining deferrals are
  documented in its own spec — cursor-aware page lower bounds, and the
  value-sensitive / ranking shapes (distinct, aggregate, search, vector,
  hybrid), which no window-plus-boundary reasoning can soundly cover.
- **Python client.** The wire + schema + query + mutation DSL and a sync
  `httpx` client (query/mutate, admin, storage — `pip install par-rt-db[http]`)
  are the implemented surface today; the reactive WebSocket client is the one
  remaining item and ships in a follow-on plan. Until then, Python users
  subscribe by building wire payloads with the DSL and sending them over their
  own WS client (see `python-client/README.md`).
- **The code is the source of truth.** When a spec and the implementation
  disagree, the implementation (and `FEATURE_MATRIX.md`) wins. Specs are linked
  from READMEs as historical design context, not as authoritative behavior
  references.
