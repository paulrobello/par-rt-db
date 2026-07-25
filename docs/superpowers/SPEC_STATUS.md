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
| `2026-07-24-fine-grained-subscription-invalidation-design.md` | Implemented (v1: point reads) | shipped | §2 #21 | v2 (range/boundary) not yet specced |
| `2026-07-24-per-row-authorization-design.md` | Implemented (v1: owner-field match) | shipped | §2 #20 | v2 (collaborator/role fields) and v3 (general declarative predicate DSL) not yet specced |
| `2026-07-24-realtime-dashboard-design.md` | Implemented (backend phases 1–6 + frontend SPA) | shipped | §2 #18 | — |
| `2026-07-25-python-client-design.md` | Implemented (core DSL); HTTP/WS/admin clients planned | 2026-07-25 | §1 (the "fourth client"; per-row "Mirrored across: ✅ts ✅rust ✅python" tracks parity) | follow-on plan TBD for HTTP/WS/admin |

## Notes

- **MVP spec expansions.** The MVP spec (`2026-07-21-par-rt-db-design.md`)
  listed nine features as "out of scope" in 2026-07-21: file storage, scheduler,
  cron, pagination, db-side `.filter()`, `.first()`, `.replace()`, text/vector
  search, per-row auth. Eight of the nine have since shipped; the list in the
  spec body now cross-references the FEATURE_MATRIX row and follow-on spec for
  each. "Actions" remains a deliberate non-goal (FEATURE_MATRIX §3).
- **Per-row auth / fine-grained invalidation.** Both specs shipped a v1
  (owner-field match; point-read skip). Their v2/v3 successors
  (collaborator/role fields; range/boundary invalidation) are not yet specced.
- **Python client.** The wire + schema + query + mutation DSL is the
  implemented surface today; the HTTP, reactive WebSocket, and admin clients
  ship in a follow-on plan. Until then, Python users build wire payloads with
  the DSL and send them via their own HTTP/WS client (see `python-client/README.md`).
- **The code is the source of truth.** When a spec and the implementation
  disagree, the implementation (and `FEATURE_MATRIX.md`) wins. Specs are linked
  from READMEs as historical design context, not as authoritative behavior
  references.
