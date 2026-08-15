# Anonymous → Real Account Merge Design

**Date:** 2026-08-14
**Status:** approved design (board card `[FM-27]`, id `01a00347811e743095ba735d549b7049`)
**Effort:** M · server-only (no wire, protocol, or client changes)

## Problem

`POST /auth/anonymous` mints an ephemeral `rtdb_auth.users` row (`anonymous = TRUE`,
no email, no provider identity) plus a session. When that guest later signs in via
OAuth, the provider callback's user-linking logic (provider-id match → email link →
create) can **never** match the anon row — it has neither a provider id nor an email —
so a fresh real user row is always created. Every document the anon user owns via
per-row `ownerField` (or shares via `collaboratorsField`, or stamped through an
`authorize` `$user` marker) still references the now-orphaned anon `user_id`, and the
session(s) minted for the anon row dangle until they expire.

Convex parity: Convex Auth merges an anonymous user into the authenticated account on
sign-in. This spec is the par-rt-db equivalent.

## Goals

1. On OAuth sign-in initiated from an anonymous session, re-stamp the anon user's
   document footprint to the real `user_id` across **every** database, through each
   db's committer (single-writer invariant preserved), with subscription fan-out and
   the op-feed tap sites firing.
2. Re-point (not delete) the anon user's sessions so an already-open WS connection
   promotes to the real principal on its next op — the SDK's stored anon token keeps
   working with no client change.
3. Retire the anon `rtdb_auth.users` row (hard delete).
4. Crash-safe by ordering: any interruption is recovered by simply signing in again.

## Non-goals

- No wire/protocol/DSL change; none of the four clients change (the merge is entirely
  server-side; the anon token the SDK holds remains valid and resolves as the real
  user after the session re-point because `resolve_session` re-joins the users row on
  every op).
- No merge in the *reverse* direction (real → anon): the real row is the stable
  identity (email-keyed cross-provider linking).
- `$email` markers are not rewritten — anonymous users have no email, so no
  `$email`-stamped value can reference the anon identity.
- Anon→anon merges, bulk admin user consolidation, and a dashboard UI for merge
  history are out of scope.

## Trigger and identity binding

The merge candidate can only be discovered via the **session**, not the email. The
binding rides the existing single-use OAuth state machinery (`rtdb_auth.oauth_states`,
Postgres-backed since ENH-022 Stage 1, so the flow is cross-replica-safe):

- **`GET /auth/{provider}/begin`**: after minting the state token, resolve the
  caller's bearer/session cookie. If it resolves to a `Principal::User` with
  `anonymous = true`, record that `user_id` in a new nullable `anon_user_id` column
  on the `oauth_states` row (boot migration: `ALTER TABLE rtdb_auth.oauth_states
  ADD COLUMN IF NOT EXISTS anon_user_id text` at the existing table-creation site in
  `db.rs`).
- **`GET/POST /auth/{provider}/callback`**: after `claim_pending` succeeds and
  `complete_login` resolves/creates the real user (real `user_id` known), read the
  claimed state row's `anon_user_id`. If present, and the referenced users row still
  exists with `anonymous = TRUE`, and it differs from the real `user_id` (it always
  will — an anon row carries no provider identity), run the merge **synchronously**
  before `set_outcome` records the terminal state. Merge failures are logged at
  ERROR but do **not** fail the login (the login's own outcome is independent; the
  ordering below makes an incomplete merge retry-safe).

A login begun from a non-anon session records no `anon_user_id` — provider linking
for existing real users is unchanged.

## Merge order (crash-safety)

The steps run in a strict order chosen so every crash window is recovered by the
user signing in again:

1. **Document re-stamps** — per database, inside each db's committer turn
   (`RunMergeUsers`, below). Enumerate databases with `SELECT name FROM
   rtdb_auth.databases`, send the request to each db's committer channel
   (`Committers::channel_for`), and await every reply.
2. **Storage blobs** — per database, direct SQL outside the committer (storage
   bypasses the committer by design; blobs touch no document tables or
   subscriptions): `UPDATE <schema>.storage SET owner_id = $real WHERE owner_id =
   $anon`.
3. **Sessions re-pointed** — `UPDATE rtdb_auth.sessions SET user_id = $real WHERE
   user_id = $anon`. Re-point, not delete: `resolve_session` re-queries the session +
   users join on every op, so an open WS connection (or a stored SDK bearer) holding
   the anon token promotes to the real principal on its next op. `session_still_valid`
  's live re-check is what makes this seamless.
4. **Anon user row deleted** — `DELETE FROM rtdb_auth.users WHERE id = $anon AND
   anonymous = TRUE` (the `anonymous = TRUE` guard is idempotency: a re-run can
   never delete a real user).

Crash windows:

- **Mid-step-1**: anon row + anon sessions intact → the user's next `/begin`
  resolves an anon session → the new state row records the same `anon_user_id` →
  the whole merge re-runs; every UPDATE is idempotent (second run matches zero rows).
- **Between 3 and 4**: an inert orphan user row (no sessions, no docs, no blobs).
  Harmless; the admin escape hatch below removes it.
- **Before 3 completes**: covered by the mid-step-1 case.

## `RunMergeUsers` committer arm

New `CommitterRequest::RunMergeUsers { anon_id, real_id, reply }`, modeled on the
existing system arms (`RunMigrate` / `RunReaper` / `RunRestoreSchema`): handled
inside the committer's serialized turn for that db.

**Principal-bearing field derivation.** From the db's pushed schema, per table,
collect the fields that can carry a user principal, each with a rewrite kind:

| Source | Declaration | Rewrite kind |
|---|---|---|
| `ownerField` | table def | scalar swap (doc field + typed `f_` column) |
| `collaboratorsField` | table def | array-element swap + dedupe |
| `authorize` `$user` markers | predicate walk | per the field's declared type: string → scalar swap; array-of-strings → element swap |

The predicate walk traverses the table's `authorize` `FilterExpr` tree (`Eq`/`Neq`/
`Gt`/`Gte`/`Lt`/`Lte`/`In`/`And`/`Or`/`Not`/`Contains`/`Exists` — the full variant
set, so a new variant is a compile-visible change site) and collects every **field
name** whose comparison value contains a `{"$user": true}` marker anywhere (including
nested inside an `In` array). `$email` markers are ignored. A collected field not of
string or array-of-strings declared type is skipped with a warning (over-approximate
to skipping, never fail the merge).

The rewrite semantics are identical in all three cases: **replace occurrences of the
anon `user_id` string with the real one, and nothing else** — other rows' values are
untouched, so the rewrite can never touch another user's footprint. `ownerField`
immutability (re-stamped on every user write) does not apply: this is the
system-initiated path, the same bypass TTL deletes and scheduled jobs use.

**Execution.** For each table with ≥1 principal-bearing field, one batched UPDATE
shaped like `handle_reaper`'s delete:

```sql
UPDATE <table>
SET doc = <jsonb transform per field>, f_<owner-col> = $real, version = version + 1
WHERE <principal-field matches $anon>
RETURNING id, doc, version  -- + whatever before-state the WriteSet needs
```

- The jsonb transform per field: scalar — `jsonb_set(doc, '{field}', $real)` where
  `doc->'field' = $anon`; array — replace the anon element with the real one and
  dedupe (`(doc->'field') - $anon) || to_jsonb($real)` with a distinct pass).
- `version = version + 1` — a merge re-stamp is a durable document write and must
  behave like one (client-side OCC sees the bump).
- DocOps and `WriteSet.doc_values` are built from the RETURNING rows so
  `subs.fan_out` re-runs affected subscriptions (the re-stamp is content-bearing —
  the new owner's subscriptions pick the docs up) and `publish_taps` fires with
  `source = "merge"`, `owner = None` (system-initiated). This extends the
  "every durable write publishes here" guarantee to the merge; the tap-site list in
  CLAUDE.md gains `handle_merge_users`.
- **Unique-index collision**: a re-stamped row may collide with a doc the real user
  already owns under a unique/partial index (a UNIQUE index covers its declared
  fields only — e.g. one row per `(owner, slug)` if both are index fields).
  Strategy: catch SQLSTATE 23505 per row — implemented as a per-row fallback UPDATE
  when the batched UPDATE raises — skip the conflicting row, and include its id in a
  `conflicts` list on the reply.
  Conflicting rows keep the anon owner; the operator (and the user, via the client)
  sees them in the op-feed absence and the log WARN. Never fail the whole merge over
  one row.
- The reply carries per-table re-stamped counts + the conflicts list (logged;
  metric `rtdb_merge_docs_total` incremented by the total).

## Admin escape hatch

`POST /admin/merge-users` (admin-only, `require_admin`): body
`{ anonUserId, realUserId, confirm }` where `confirm` must equal `realUserId` (the
typed-confirm guard pattern from `delete-db`/`restore`). Runs the identical merge
synchronously and returns the per-db/per-table counts + conflicts. Use: crash-window
cleanup (the inert-orphan case), manual consolidation, and testing. Refuses when the
anon row does not exist or is not `anonymous = TRUE`.

## Security considerations

- The `anon_user_id` binding is recorded **server-side** at `/begin` from a
  verified session (cookie or bearer resolves through `resolve_bearer`); it is not
  caller-supplied at callback time. An attacker cannot merge someone else's anon
  footprint without that anon session's token/cookie. The state token remains
  single-use + TTL-bounded + CSRF-cookie-gated (SEC-121).
- The merge arm bypasses per-row auth as a system principal (`owner = None`),
  exactly like TTL/scheduled/migrate arms; it is only reachable from the callback
  path and the admin endpoint, never from client Subscribe/Mutate frames.
- The re-pointed session's `expires_at` is unchanged (the anon session's short
  TTL, SEC-103, carries over — the real login's fresh session has the standard TTL;
  the re-pointed one simply expires sooner, which is conservative).
- Deleting the anon users row does not grant or remove any authorization.

## Edge cases

- **Concurrent provider logins from one anon session** (e.g. GitHub popup and
  Google popup): each flow has its own single-use state row; both callbacks may run
  merges. Committer turns serialize per db; every step is idempotent; the second
  session re-point/delete matches zero rows. Safe.
- **Returning real user vs newly created**: both are just the merge target; the
  callback resolves the row before the merge runs.
- **Two anon users → same real user over time**: independent merges, each
  idempotent.
- **Anonymous auth disabled after minting** (`RTDB_AUTH_ANONYMOUS_ENABLED=false`):
  the boot gate stops *minting*; a login from an existing anon session still merges
  (the merge is cleanup, not new anon capability).
- **Live subscriptions registered under the anon owner**: the subscription's
  captured owner filter no longer matches after the re-stamp, so it goes empty.
  Accepted for v1: apps gate queries on auth state (`useRtDbAuth`), so sign-in
  re-subscribes; the promoted connection re-runs `authorize` per op. Noted as a
  documented behavior, not fixed.
- **`ExpectVersion` on a merged doc**: the version bump is visible OCC state; a
  stale client gets `PRECONDITION_FAILED` as designed.
- **Databases created after the merge**: nothing to merge; `RunMergeUsers` on a
  fresh db is a no-op.

## Testing

Integration (GitHub wiremock e2e pattern, `oauth_test.rs` or a new `merge_test.rs`):

1. Full flow: enable anon auth → mint anon → write an owned doc, a collaborator doc
   (anon in another user's `collaboratorsField`), and an `authorize`-guarded doc
   stamped via `$user` → begin GitHub login with the anon cookie → complete wiremock
   callback → assert: real session reads/writes all three docs; the anon token
   resolves as the real (non-anonymous) user; the anon users row is gone; the
   op-feed carries `source = "merge"` ops; a subscription held by the real user
   pushed the re-stamped doc (fan-out fired).
2. Idempotency: the admin merge-users endpoint re-run over the same pair touches
   zero rows and reports zero conflicts.
3. Unique-collision: a unique index the real user already satisfies → the anon row
   is skipped, listed in `conflicts`, the rest of the merge completes.
4. Unit: predicate walk collects the right `$user` fields across the FilterExpr
   variants (incl. `In`-nested and `Not`-wrapped); session re-point promotes an
   open-connection principal (existing `session_still_valid` harness).
5. `make checkall` green.

## Documentation updates on ship

- `FEATURE_MATRIX.md` row 7/39 note: anon→real merge shipped (strike the
  "follow-up" language).
- `CLAUDE.md` committer tap-site list + this spec cross-reference.
- `README`/auth docs: the merge behavior and the admin endpoint.
