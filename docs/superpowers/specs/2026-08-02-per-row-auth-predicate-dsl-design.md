# Per-Row Authorization Predicate DSL (Model C) — Design

**Date:** 2026-08-02
**Status:** Approved (design) → plan
**Kanban:** `par-rt-db` — "Per-row auth general predicate DSL (model C)" (`019fbe207c807252a55bbbc19f8b457e`)
**Severity:** Medium (authorization surface; additive, no change to existing ownerField/collaboratorsField semantics)
**Related:** v1/v2 per-row auth spec `2026-07-24-per-row-authorization-design.md`; FEATURE_MATRIX #20; fine-grained invalidation `2026-07-24-fine-grained-subscription-invalidation-design.md`.

## Motivation

Per-row authorization today is two fixed, opt-in declarations: `ownerField` (a row is
visible/mutable iff `doc.<ownerField> == caller.user_id`) and `collaboratorsField`
(visible/mutable iff owner OR `caller.user_id ∈ doc.<collaboratorsField>[]`). Both are
enforced server-side on read, write, and subscription re-run. They cover "users must not
see or touch each other's rows" and "shared rows via a collaborators list."

What they cannot express is any rule that is not literally owner-equality or
owner-OR-collaborator: a row readable iff `visibility == "public" OR owner == caller`;
tenant scoping (`doc.tenantId == caller.tenantId`); scalar role fields; "editable iff
`doc.editors[]` contains the caller AND `doc.archivedAt` is null." Apps that need these
today have no declarative option — they must fall back to coarse db-level access and
enforce in client code (unforgeable only if the server enforces it, which it cannot here).

Model C is the general declarative predicate that covers this long tail. It reuses and
extends the existing query `FilterExpr` DSL as the authorization predicate, evaluated
against the document **and the authenticated principal**, enforced on the same seams
ownerField/collaboratorsField already occupy.

## Goals

- A per-table, opt-in `authorize` declaration: a `FilterExpr` predicate over document
  fields **and principal attributes** that governs row visibility and mutability.
- Extend `FilterExpr` with the leaves it lacks for authorization: a principal binding
  (`$user` / `$email`), array-membership (`Contains`), negation (`Not`), and a null/exists
  test (`Exists`) — available to both the auth predicate and client `.filter()` queries.
- Enforce the predicate on reads (scan terminals + point-read), writes (pre-check +
  all-writes stamp/verify), and subscription re-runs — the same four seams ownerField uses.
- Mirror the new declaration and `FilterExpr` variants across all four clients
  (server, ts-client, rust-client, python-client).

## Non-goals

- **Replacing** `ownerField` / `collaboratorsField`. They remain as the simple shipped
  fast paths; `authorize` is a third opt-in. A table picks whichever fits (or composes
  `ownerField` with `authorize`).
- **Field-level / column-level** authorization (predicate is row-granular).
- **A Rust/JS function sandbox.** The server has no embedded runtime; the predicate is
  declarative, compiled to SQL for scans and evaluated in Rust for point-reads/writes.
- **Multi-tenant principal attributes** beyond `user_id` and `email` (e.g. a `tenantId`
  on the principal). `$user` resolves to `user_id`; `$email` to the session email. More
  attributes are a future addition if/when the `Principal` type grows them.
- **Changing the db-level `authorize` gate** or the Machine/admin/scheduled bypass. The
  predicate is a second layer, evaluated only after the caller is admitted to the db, and
  bypassed entirely for non-`User` principals (same choke points as today).

## Decisions (confirmed in design review)

- **Expressiveness:** full — extend `FilterExpr` with `Not`, `Contains`, `Exists`, and
  principal-binding value markers. One predicate language subsumes owner + collaborator
  rules (ownerField/collaboratorsField remain as convenience shortcuts).
- **Write authorization:** auto-stamp from the predicate — on every write, stamp every
  `Eq { field, $user }` leaf's field with the caller's `user_id`, then verify the
  predicate on the resulting doc (`Forbidden` on failure). Extended from insert-only to
  all writes after a security review (ownerField parity; see §7/§8).
- **Coexistence:** additive — `authorize` coexists with `ownerField`/`collaboratorsField`.
- **DSL locus:** extend the shared `FilterExpr` enum (one language for query filters and
  auth), not a separate `AuthPredicate` enum.

## Approach

`ownerField` is already enforced by injecting a `FilterExpr::Eq { field: owner_field,
value: uid }` into the query (`owner_filter`, `query.rs:2140`) and a bespoke Rust predicate
on fetched docs (`row_visible_to`, `txn.rs:892`). Model C generalizes both: the table
declares the predicate, and the server compiles/evaluates it with the principal's
attributes substituted in, at the same four enforcement points. The principal binding is
the one thing `FilterExpr` cannot express today (its leaves are `doc.field OP literal`
only); adding it turns `FilterExpr` into a complete auth predicate language.

Three things must be built that do not exist today:
1. **Principal-binding value markers** in `FilterExpr` (`$user`, `$email`), valid only in
   a server-declared `authorize` predicate.
2. **A Rust doc-level evaluator** `filter_matches(doc, expr, principal)` — `FilterExpr` is
   SQL-compiled only today; point-reads and write pre-checks need an in-memory eval.
3. **Auto-stamp on every write** — introspect the predicate for `Eq { field, $user }`
   leaves and stamp them on the resulting doc, then verify (all five write paths).

## Detailed design

### 1. Declaration — `TableDef.authorize`

`server/src/schema.rs`:

```rust
/// Opt-in general per-row authorization predicate (model C). A FilterExpr over
/// document fields AND principal attributes (`$user`/`$email`) that a row must
/// satisfy to be visible/mutable to a User principal. Additive with `owner_field`
/// / `collaborators_field` (which remain as simpler opt-ins). Enforced on read,
/// write, and subscription re-run; bypassed for Machine/admin/scheduled.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub authorize: Option<FilterExpr>,
```

Additive — existing schemas deserialize unchanged. A table may declare `authorize`
alone, `ownerField`/`collaboratorsField` alone, or compose (e.g. `ownerField` for insert
stamping + `authorize` for a broader visibility rule).

### 2. Principal binding — value markers

A `FilterExpr` comparison leaf's `value` (and `Contains`/`In` values) may be a principal
reference instead of a literal:

```jsonc
{ "op": "eq", "field": "owner",     "value": { "$user": true } }   // doc.owner == caller.user_id
{ "op": "contains", "field": "editors", "value": { "$user": true } } // caller.user_id ∈ doc.editors[]
{ "op": "eq", "field": "ownerEmail", "value": { "$email": true } } // doc.ownerEmail == caller.email
```

Wire shape: `{"$user": true}` / `{"$email": true}`. Resolved server-side to the caller's
`user_id` / `email` at SQL-compile time (bound via `$n`) and at Rust-eval time.

**Constraint:** principal refs are valid ONLY in a server-declared `authorize` predicate.
Client-supplied query `.filter()` expressions are validated to contain no `$user`/`$email`
markers (a client cannot reference the principal; the marker is meaningless there and
could confuse). This keeps the query-filter and auth-predicate DSLs the same enum while
restricting where principal refs may appear.

### 3. New `FilterExpr` variants

Added to the enum at `query.rs:199` (serde `tag = "op"`, `deny_unknown_fields`):

```rust
Not { expr: Box<FilterExpr> },                 // op = "not"
Contains { field: String, value: Value },      // op = "contains" — value ∈ doc.field[]
Exists { field: String },                      // op = "exists"  — field present and non-null
```

- `Contains` is the reverse of `In`: `In` is `doc.field ∈ literal[]`; `Contains` is
  `literal ∈ doc.field[]`. It is exactly the jsonb `?` membership test that
  `collaboratorsField` hand-stitches in raw SQL today (`query.rs:2189`).
- `Exists` + `Not` cover null/absence tests (`Not(Exists { archivedAt })` ⇐ "archivedAt
  is null/absent").

These are available to client `.filter()` queries too (genuinely useful, and keeps one
DSL). SQL compilation (`compile_filter_node`, `query.rs:1462`):
- `Not(e)` → `NOT (<e>)`
- `Contains { field, value }` → `(doc->'<field>') ? $n` (jsonb array membership; `field`
  must be array-typed, validated)
- `Exists { field }` → `(doc ? '<field>') AND (doc->>'<field>' IS NOT NULL)`

All values bind via `$n` (identifiers are schema-validated and double-quoted/emitted
exactly as today). The DDL literal-render path (`render_filter_literal_node`, used for
partial-index `WHERE`) also gains these variants for completeness.

### 4. Read enforcement — scan terminals (generalizes `owner_filter`)

At `query.rs:856-883`, today's `effective_filter` / `row_auth_predicate_body` logic
becomes: if the table declares `authorize` AND the caller is a `User` (owner is `Some`),
resolve `$user`/`$email` in the predicate to the caller's bound literals, `AND`-compose
it onto `where_conditions` (and the client `filter`). The owner-only `owner_filter` and
the owner+collab `row_auth_predicate_body` paths remain for tables using those
declarations; `authorize` is a third branch. Applied uniformly at every SQL-emitting
terminal — `count` (886), `distinct` (930), `take`/`collect`/`unique`/`first`/`paginate`,
and the search terminals `execute_search` (1703), `execute_vector_search` (1774),
`execute_hybrid_search` (1940), mirroring where `row_auth_predicate_body` is appended
today. `None` owner (Machine/admin/scheduled) appends no predicate.

### 5. Read enforcement — point-read (`get`)

`point_read` (`query.rs:2099`) fetches the doc, then evaluates the predicate in Rust
(`filter_matches`, below) instead of `row_visible_to`. A forbidden doc returns
`Doc(None)` — silent, Convex-style, unchanged from today.

### 6. Rust doc-level evaluator (new)

`FilterExpr` has no Rust evaluator today (SQL-only). Add:

```rust
/// Evaluates `expr` against a fetched `doc` (jsonb Value) with `principal`'s
/// attributes substituted for `$user`/`$email`. Returns whether the doc satisfies
/// the predicate. Used by point-read, write pre-checks, and insert verification.
fn filter_matches(
    doc: &serde_json::Value,
    expr: &FilterExpr,
    principal: &PrincipalCtx,
) -> bool
```

`PrincipalCtx` carries the caller's `{ user_id: Option<&str>, email: Option<&str> }`.
Today the executors take `owner: Option<&str>` (user_id only); model C threads email too,
so the principal view passed into `execute_query` / `execute_txn` — via `CommitterRequest`
and the WS/HTTP handlers, the same seams at `ws.rs:448` and `http_api.rs:88` — grows from
`Option<&str>` to this small view. `None` user_id = bypass (Machine/admin/scheduled),
unchanged. Evaluates all variants: `Eq`/`Neq`/
`Gt`/`Gte`/`Lt`/`Lte` (typed compare via `serde_json::Value` ordering, matching the SQL
casts), `In`, `And`/`Or`, `Not`, `Contains` (array membership), `Exists`, with principal
markers resolved. A missing/untyped field over-approximates to the same result the SQL
path would yield (and any type-mismatch doubt on a write defaults to `Forbidden`, never
to a silent allow). This evaluator is the single source of truth for in-memory predicate
checks; `row_visible_to` is retired in favor of it once owner/collab tables also route
through `authorize`-equivalent predicates (or kept as a fast path — see §11).

### 7. Write enforcement — pre-check (patch/replace/delete/upsert-update)

Inside `execute_txn` (`txn.rs:1000`), the existing `check_owner` (`txn.rs:913`) /
`check_owner_doc` (`txn.rs:951`) become: if the table declares `authorize` and the caller
is a `User`, fetch the target doc and evaluate `filter_matches(doc, authorize,
principal)`; on failure raise `RtDbError::forbidden("document '{id}' is not accessible to
the caller")` (403), aborting the whole txn atomically (single-writer, no TOCTOU). Missing
doc → `Ok(())` so the subsequent op reports `NotFound`. ownerField/collaboratorsField
tables keep their existing `check_owner` path.

**Stamp + post-write verify on every write (Task 8.5, security-review extension):**
pre-check is not the whole story. On **all five** write paths — Insert, Upsert-insert,
Patch, Replace, Upsert-update — the server also runs `stamp_authorize` (after `stamp_owner`:
re-stamp every `Eq { field, $user }` leaf to the caller's `user_id`) and
`verify_authorize_doc` (`filter_matches` on the resulting doc → `Forbidden`/403, atomic
rollback) before the write commits. (Delete is pre-check only — there is no resulting doc
to stamp or verify.) This achieves `ownerField` parity — `ownerField` re-stamps its owner
field on every write, and now `authorize` does the same for its stampable leaves — and
closes a patch-injection vector: without an all-writes stamp, a `patch` could flip an
owner-ish field to another user and pass a pre-check evaluated against the pre-patch
state. The post-write verify is the authoritative gate; the pre-check is an early
fast-fail. The mechanism is §8.

### 8. Write enforcement — auto-stamp + verify on every write

For every write step — `Step::Insert`, the insert branch of `Step::Upsert`, `Step::Patch`,
`Step::Replace`, and the update branch of `Step::Upsert` — when the table declares
`authorize` and the caller is a `User`:

1. **Auto-stamp.** Walk the `authorize` predicate (recurse `And`/`Or`; `Not` does not
   stamp — a negated equality is not a stampable ownership). For each leaf
   `Eq { field: F, value: {"$user": true} }`, force `doc[F] = principal.user_id`,
   overwriting any client value (unforgeable — identical guarantee to `stamp_owner`,
   `txn.rs:856`). On non-insert writes this re-asserts the field against the post-write
   doc, so a `patch`/`replace`/`upsert`-update that tries to flip an owner-ish field to
   another user is stamped back to the caller. This runs after `stamp_owner`, subsuming
   ownerField's stamping: a table with
   `authorize: {op:"eq", field:"owner", value:{"$user":true}}` gets the same behavior on
   every write, derived from its predicate.
2. **Verify.** Evaluate `filter_matches(doc, authorize, principal)` on the resulting doc.
   If it fails (a non-stampable requirement the client didn't satisfy, e.g.
   `visibility == "public"` not set, or an attempted flip of a field the predicate
   requires), reject with `RtDbError::forbidden(...)` (403) and roll the txn back
   atomically.

Edge cases (well-defined — apply per-write, not insert-only):
- An `Or` containing an `owner == $user` arm always passes (owner stamped) — the common
  "public OR owned" rule writes freely.
- A predicate with no `Eq { field, $user }` leaf stamps nothing; the doc must satisfy the
  predicate from client-provided values alone.
- Array-membership-only rules (`Contains { editors, $user }`) are not stampable; the
  client must include its own `user_id` in the array (the SDK exposes the caller's id) or
  the write fails verification.
- If the table also declares `ownerField`, `stamp_owner` still runs first (owner stamped),
  then `authorize` auto-stamp + verify compose on top.

> **Note (post-design extension).** The original design scoped auto-stamp + verify to
> insert and upsert-insert only. A security review (Task 8.5) extended it to all five
> write paths to achieve `ownerField` parity (`ownerField` re-stamps its owner field on
> every write) and to close the patch-injection vector described in §7.

### 9. Subscription re-filter — automatic

`fan_out` re-runs each affected subscription via `execute_query(pool, db, schema, &entry.query, entry.owner.as_deref())`
(`subs.rs:998`). Because `authorize` is read from `TableDef` (not the subscriber) and the
subscriber's principal is `SubEntry.owner` (`subs.rs:744`), every re-run re-applies the
predicate filtered to the subscriber. No new subscription code — transitive coverage
identical to ownerField. A write by user B to B's rows re-runs A's subscription, but A's
authorize-filtered result is unchanged ⇒ no cross-principal data is ever pushed.

### 10. Bypass — unchanged choke points

`Machine` tokens, admin, and scheduled jobs pass `owner = None` ⇒ `row_auth_enforced_uid`
returns `None` ⇒ no predicate compiled, no pre-check, no stamp (full access). Identical to
today; `authorize` simply is not evaluated for bypass principals. The db-level `authorize`
gate still runs first on every operation.

### 11. Schema validation (`TableDef::validate_structure`, `schema.rs:333`)

- `authorize`, if present, references only declared fields; `Contains` requires an
  array-typed field (`is_string_array_field`); comparison ops require a type-compatible
  field.
- Principal-ref markers (`$user`/`$email`) appear ONLY in `authorize` — rejected in
  client-supplied query `.filter()` at the `ClientMessage::Subscribe`/HTTP query boundary.
- `Eq { field, $user }` stampable fields must be string-compatible (the stamped value is a
  `user_id` string), so auto-stamp can't write a typed mismatch.
- `authorize` is additive-only schema metadata (no DDL column); `migrate.rs` field-rename
  rewrites field references inside it (mirroring ownerField handling, `migrate.rs:147`);
  field-drop clears/invalidates it.

### 12. Client mirror

- **Schema declaration:** all four schema representations carry `authorize: Option<FilterExpr>`
  (server `schema.rs`, ts-client, rust-client, python-client). Additive (`skip_serializing_if
  = None`); clients only **declare** (server enforces).
- **FilterExpr variants + principal markers:** mirrored across the four protocol files
  (server `query.rs`/`protocol.rs`, ts `protocol.ts`, rust `wire.rs`, python `wire.py`)
  since the new variants extend the query `.filter()` DSL too. Tag/field names byte-identical
  (`op`/`eq`/`contains`/`not`/`exists`/`exprs`/`field`/`value`, `{"$user":true}`/`{"$email":true}`).

### 13. Interaction with existing systems

- **ownerField / collaboratorsField:** unchanged; `authorize` is a third opt-in. (Future:
  owner/collab could be compiled to an equivalent `authorize` predicate internally, making
  `row_visible_to`/`row_auth_predicate_body` special cases of the general path — but that
  refactor is NOT required to ship model C and is deferred to avoid touching working code.)
- **Single-writer invariant:** preserved — the pre-check and stamp run inside the one
  per-db committer's `execute_txn`. No second writer.
- **Invalidation (#21):** authorize-filtered subscriptions remain correct under table-level
  re-runs; no new invalidation code.
- **Errors:** unauthorized reads filter silently (`Doc(None)`); unauthorized writes/inserts
  return `Forbidden`/403, aborting the txn atomically.
- **Hot config / CORS / rate limiting / storage:** unaffected.

## Security invariants & threat model

1. The read predicate is **server-injected and unforgeable** — clients cannot remove it
   (it is composed from `TableDef`, not the client query).
2. Auto-stamped fields are **server-authoritative** — a client cannot forge a different
   principal's `user_id` into a stampable field.
3. Pre-check + stamp run **serialized with the write** (single-writer) — no TOCTOU.
4. **Subscriptions cannot leak cross-principal data** — the stored query re-applies the
   subscriber's predicate on every fan-out.
5. **Bypass is limited** to Machine/admin/scheduled (`owner = None`).
6. **Type-mismatch / missing-field doubt on a write defaults to `Forbidden`** (never a
   silent allow); on a read it defaults to "row not visible" (over-approximate filtering,
   never under-approximate) — same posture as ownerField.
7. **Side-channel (carried forward from v1, accepted):** `ExpectVersion`/`ExpectAbsent`
   preconditions are not predicate-checked; the existence/version oracle remains. The write
   target itself is still predicate-gated. Closing the oracle is a separate future concern.

## Testing

- **Read filtering:** User A queries an `authorize` table seeded with A's, B's, and public
  rows → only predicate-matching rows returned; B writes → A's subscription receives no
  push for rows A's predicate excludes.
- **Predicate variants:** each new leaf (`Not`, `Contains`, `Exists`) exercised in both an
  auth role and a client `.filter()` role; principal markers `$user`/`$email` resolve
  correctly.
- **Writes:** patch/replace/delete on a non-matching doc → `Forbidden`, txn aborts
  atomically; upsert-update on a non-matching matched doc → `Forbidden`.
- **All-writes auto-stamp + verify:** `Eq{owner,$user}` stamped on insert (client value
  overwritten) and re-stamped on `patch`/`replace`/`upsert`-update (an attempted flip to
  another user is reverted); `Or[owner==$user, visibility=="public"]` writes freely; a
  predicate with an unsatisfied non-stampable requirement → write `Forbidden`;
  array-membership-only predicate requires the client to supply the value.
- **Bypass:** machine token, admin, and scheduled job get full access on an `authorize`
  table.
- **Subscription no-leak:** cross-principal writes do not push rows the subscriber's
  predicate excludes.
- **Schema validation:** unknown field / non-array `Contains` / principal ref in a client
  filter → rejected; absent `authorize` deserializes unchanged.
- **Clients:** schema `authorize` round-trips; new `FilterExpr` variants + markers
  round-trip through all four protocol files.

## Files

- `server/src/schema.rs` — `TableDef.authorize` + validation (+ 3-client schema mirror).
- `server/src/query.rs` — new `FilterExpr` variants + principal markers; SQL compilation;
  `filter_matches` Rust evaluator; `authorize` branch in `execute_query` scan terminals +
  `point_read`.
- `server/src/txn.rs` — `authorize` pre-check (check_owner path) + all-writes auto-stamp/verify.
- `server/src/protocol.rs` (+ `ts-client`, `rust-client`, `python-client` protocol files) —
  new `FilterExpr` variants + markers.
- `server/tests/*` — the cases above.

## Verification

`make checkall` (fmt-check + clippy `-D warnings` + typecheck + full test suite; `make
dev-db-up` required). Dashboard typecheck needs `make ts-client-build` on a fresh checkout.

## Open questions

None. Expressiveness (full), insert auth (auto-stamp), coexistence (additive), and DSL
locus (extend `FilterExpr`) were confirmed in design review.
