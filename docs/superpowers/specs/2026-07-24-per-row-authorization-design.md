# Per-Row Authorization Rules (v1: Owner-Field Match) — Design

- **Status:** Implemented (v1: owner-field match). Board item #20 ("Add per-row authorization rules") shipped as the opt-in `ownerField` table declaration enforced server-side on query, mutate, and subscription re-run.
- **Date:** 2026-07-24
- **Related:** FEATURE_MATRIX #20; main design spec `2026-07-21-par-rt-db-design.md` ("Auth"); fine-grained invalidation `2026-07-24-fine-grained-subscription-invalidation-design.md`; implementation plan `docs/superpowers/plans/2026-07-24-per-row-authorization.md`.
- **Scope:** This remains the design specification for the feature; what shipped under v1 is summarized in the Implementation-sketch cell of FEATURE_MATRIX #20 and detailed in the plan doc above.

## Summary

Add an opt-in, per-table **owner-field** authorization rule: a table may declare an
owner field, after which an authenticated **user** may read only rows they own and
may mutate only rows they own (inserts are stamped with their identity). The rule is
**declarative** (the server has no embedded JS runtime, so it cannot host Convex-style
arbitrary auth code) and **enforced server-side** on query, mutation, and subscription
re-run. Machine tokens and the admin/master key bypass per-row rules (full access).

This is the minimal sound design that serves the sole motivating use case —
multi-user apps where users must not see or touch each other's rows.

## Background & motivation

Today, authorization is a single coarse layer: a per-database email allowlist (a user
on the allowlist gets full read/write on the whole database) plus per-database machine
tokens (full read/write). `authorize(pool, principal, db)` (`server/src/auth/mod.rs`)
is the only check, applied per-operation. There are deliberately **no per-row rules**
in the MVP — sufficient for single-user / trusted-team apps (e.g. the kanban model)
but not for multi-user apps with mutual row isolation.

The architecture constraint that shapes this design: **there is no embedded JS runtime
and no per-app server code.** Convex implements per-row auth as arbitrary code inside
query/mutation functions; par-rt-db cannot. Rules must therefore be a **declarative
DSL** the generic server can compile and enforce. v1 restricts that DSL to the simplest
sound rule — owner-field equality — which covers the stated need.

## Non-goals

- **Collaborator/role fields** (an `editors: [userId]` list, role fields) — model B.
- **A general declarative predicate DSL** (per-table read/write rules as predicates
  over doc fields + principal: `eq`, `in`, comparisons) — model C.
- **Implementation.** This document is the design; building it is a separate effort.
- **Field-level** or **column-level** authorization (owner-field is row-granular only).
- Changing the existing allowlist/token model — per-row is an **additional** layer
  applied after `authorize`, not a replacement.

## Principal & bypass model

- The owner value is the authenticated user's stable id: `Principal::User.user_id`
  (`server/src/auth/mod.rs`). A row is "owned" by the user whose `user_id` equals the
  row's owner field.
- Per-row rules apply **only to `Principal::User`**.
- **`Principal::Machine` and the admin/master key bypass per-row rules entirely**
  (full read/write, as today). Machine tokens have no user identity and are trusted
  service principals; the admin key is the trusted operator. (Accepted risk: a
  compromised machine token or admin key grants full access — same posture as today.)
- The db-level `authorize` (allowlist / token validity / session expiry) **still runs
  first** on every operation; per-row is a second layer, evaluated only after the
  caller is authorized to reach the database at all.

## Schema declaration (opt-in per table)

A table opts into per-row auth by declaring its owner field:

```jsonc
{
  "tables": {
    "notes": {
      "fields": {
        "title": { "type": "string" },
        "userId": { "type": "string" }      // holds an owner's user_id
      },
      "indexes": [{ "name": "by_user", "fields": ["userId"] }],
      "ownerField": "userId"                 // NEW: opt into per-row auth
    }
  }
}
```

- `ownerField` names a **declared field** on the table (schema-validated identifier),
  recommended **indexed** so the read filter and ownership lookups use the typed column
  rather than jsonb extraction. The field holds an owner's `user_id`.
- It is **additive** (`skip_serializing_if = Option::is_none`), so existing schemas
  deserialize unchanged — pushing an `ownerField` is the only change needed to turn it on.
- Tables **without** `ownerField` keep today's behavior (allowlist-gated full access).
  Mixing `ownerField` and non-`ownerField` tables in one database is fine.
- This declaration is a **schema-DSL extension mirrored across** server `schema.rs`
  (`TableDef.owner_field: Option<String>`) **and both client schema builders**
  (`ts-client`, `rust-client`) — the three schema representations must stay
  byte-identical on the wire. **Enforcement is server-side only**; clients only declare.

## Read enforcement — filtering (query + subscription)

For a `User` reading an `ownerField` table, the server **injects an equality predicate**
`doc.<ownerField> == principal.user_id` into the query, composed with whatever
`filter`/`eq`/range the client supplied. Unauthorized rows never reach the client.

- **Unforgeable:** the predicate is appended server-side; the client cannot remove or
  weaken it. (Implementation note: it can be synthesized as an additional
  `FilterExpr::Eq { field: ownerField, value: user_id }` AND-ed into the query, reusing
  the existing indexed-vs-jsonb filter compilation in `query.rs` — no new SQL path.)
- **No principal, no filter:** `Machine`/admin principals get no injected predicate
  (full access). Tables without `ownerField` get no predicate.
- **Subscriptions:** the owner filter is captured in the query at subscribe time
  (parameterized by **that subscriber's** `user_id`) and stored on the subscription, so
  every `fan_out` re-run only ever matches the subscriber's own rows. A write by user B
  to B's rows re-runs A's subscription query, but A's owner-filtered result is unchanged
  → **no cross-user data is ever pushed.** This composes with the fine-grained
  invalidation work (#21): table-level re-runs still fire; the owner filter + canonical
  diff handle correctness, and the `get(id)` point-read skip is unaffected (a `get` by
  another user's id simply returns null after filtering).

## Write enforcement (insert / patch / replace / delete / upsert)

- **Insert:** the server **forces** `doc.<ownerField> = principal.user_id`. Any
  client-supplied value for that field is overwritten — server-authoritative, so a user
  cannot forge ownership of another user's id. (A user inserting into an `ownerField`
  table always creates a row they own.)
- **Patch / Replace / Delete:** the target document must already satisfy
  `doc.<ownerField> == principal.user_id`; otherwise the step fails `FORBIDDEN` and the
  whole transaction aborts (atomicity preserved — no partial write).
- **Upsert:** if an existing doc matches, that doc must be owned (else `FORBIDDEN`);
  the patch is then applied to an owned row. If no doc matches, the insert branch
  auto-stamps the owner (as above).
- **Serialized with the write:** the ownership check runs **inside the committer's
  serialized transaction** (the same single-writer path all writes take), so the
  check-then-write is atomic — no TOCTOU window where ownership could change between
  the check and the apply.

## Architectural seams (what implementation must thread)

Today `execute_query` and `execute_txn` take **no principal**. Enforcement requires the
caller's identity to reach both, along two paths:

1. **Reads:** both WS `Subscribe` and HTTP one-shot query resolve the `Principal` in
   their transport handler. The principal (or a small `OwnerAuth` view carrying just
   `Option<user_id>` + bypass flag) is passed into `execute_query`, which injects the
   owner filter for `ownerField` tables.
2. **Writes:** `CommitterRequest::Mutate` carries the principal; the committer passes
   it to `execute_txn`, which (a) stamps the owner on inserts and (b) runs the
   ownership pre-check inside the txn for patch/replace/delete/upsert.
3. **Scheduled transactions** have **no interactive principal** — they run server-side.
   They must therefore execute as a **bypass** principal (full access), like a machine.
   The spec calls this out: scheduled/cron jobs are trusted server-side code and are
   not subject to per-row rules (a scheduled job that needs to touch an `ownerField`
   table operates with full access; if per-row semantics are required there, that is a
   future concern).

`authorize` itself is unchanged (still the db-level gate); per-row lives in the
query/txn executors.

## Interaction with existing systems

- **Allowlist / tokens:** unchanged; still the first gate. Per-row is additive.
- **Single-writer invariant:** preserved — all writes (including the ownership check)
  go through the one per-db committer. No second writer is introduced.
- **Schema evolution:** additive-only (`ownerField` is optional); existing schemas and
  databases are unaffected until a schema opts in.
- **Invalidation (#21):** owner-filtered subscriptions remain correct under table-level
  re-runs; no new invalidation code is needed. A subscription simply cannot observe
  another user's rows because its stored query already filters them out.
- **Errors:** unauthorized reads **filter silently** (no error — the user just sees
  fewer rows, Convex-like). Unauthorized writes return `FORBIDDEN` (envelope
  `{code, message}`), aborting the transaction atomically.

## Security invariants & threat model

1. The read predicate is **server-injected and unforgeable** — clients cannot query
   around it.
2. The owner field on insert is **server-authoritative** — clients cannot stamp
   another user's id.
3. Ownership checks are **serialized with the write** (single-writer) — no race.
4. **Subscriptions cannot leak cross-user data** — the stored query filters to the
   subscriber's own rows before any push.
5. **Bypass is limited** to `Machine` and admin/master principals; no user identity,
   no bypass.
6. **Threat not covered (accepted):** owner-field auth is row-granular only. It does
   not prevent a determined owner from putting another user's id in a *non-owner*
   field, nor does it provide field-level or role-based access — those are models B/C,
   out of scope. Mutual **row** isolation between users is the guarantee.
7. **Side-channel not closed in v1 (accepted):** `ExpectVersion` / `ExpectAbsent`
   step preconditions are **not** owner-checked, so a user can probe whether
   another user's doc exists or learn its current version from a precondition
   outcome (`NotFound` when the doc is absent vs `PreconditionFailed` when the
   version mismatches vs `Ok` when it matches). This is an existence/version
   oracle only — no cross-user read or write of doc **bodies**, and the write
   target itself remains owner-gated (a precondition that succeeds still cannot
   apply a patch/replace/delete to a doc the caller does not own). Closing the
   oracle by owner-checking preconditions is a future concern, deferred from
   v1's write-enforcement list.

## Testing (when implemented)

- **Read filtering:** User A queries an `ownerField` table seeded with A's and B's rows
  → only A's rows returned; B writes → A's subscription receives no push for B's rows.
- **Writes:** insert auto-stamps `owner = A`; A patch/delete on B's doc → `FORBIDDEN`
  and the txn aborts atomically; upsert ownership check on another user's matched doc
  → `FORBIDDEN`.
- **Bypass:** machine token and admin key get full read/write on an `ownerField` table.
- **No ownerField:** a table without `ownerField` behaves exactly as today for all
  principals.
- **Scheduled txn:** runs with bypass (full access) on an `ownerField` table.
- **Wire/schema:** `ownerField` round-trips through server + both client schema
  builders; an absent `ownerField` deserializes unchanged.

## Files (when implemented)

- `server/src/schema.rs` — `TableDef.owner_field` (+ ts-client / rust-client mirror).
- `server/src/query.rs` — inject the owner `FilterExpr` for `User` principals.
- `server/src/txn.rs` — stamp owner on insert; ownership pre-check on patch/replace/
  delete/upsert.
- `server/src/committer.rs` + `ws.rs` + `http_api.rs` — thread the principal (or
  `OwnerAuth` view) to the executors; scheduled txns run as bypass.
- `server/tests/*` — the cases above.

## Future (explicitly deferred — models B / C)

- **B:** owner + collaborator/role fields (e.g. `editors: [userId]`, a roles field) for
  shared rows.
- **C:** a general declarative predicate DSL (per-table read/write rules as predicates
  over doc fields + principal). The `ownerField` declaration is the seed; B/C extend it
  additively (e.g. an `authorize` block) rather than replacing it.
