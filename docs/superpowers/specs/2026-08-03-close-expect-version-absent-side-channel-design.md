# Close the `ExpectVersion`/`ExpectAbsent` Per-Row-Auth Side-Channel — Design

## Motivation

`ExpectVersion { table, id, version }` and `ExpectAbsent { table, index, eq }` are
read-only precondition steps (they perform no write; the transaction aborts on a
mismatch). They have shipped since the original protocol, and the client SDKs use
`expectVersion` for optimistic-concurrency retries.

Both per-row-authorization specs flag one residual issue and explicitly defer it:

- `2026-07-24-per-row-authorization-design.md`, threat-model item 7 — *"Side-channel not
  closed in v1 (accepted)"*.
- `2026-08-02-per-row-auth-predicate-dsl-design.md`, security-invariant item 7 —
  *"Side-channel (carried forward from v1, accepted)"*: `ExpectVersion`/`ExpectAbsent`
  preconditions are not predicate-checked; the existence/version oracle remains. The write
  target itself is still predicate-gated. **Closing the oracle is a separate future
  concern.** This spec is that future concern.

**The oracle.** Neither step is subject to the per-row visibility gate
(`ownerField` / `collaboratorsField` / `authorize`) that every read and write terminal
enforces. A user can therefore include one of these steps in a transaction to learn
something about a document they cannot otherwise see:

- `ExpectVersion` on another user's doc returns `NotFound` (absent), `PreconditionFailed`
  (version mismatch), or `Ok` (match) — leaking both **existence** and the **current
  version number**.
- `ExpectAbsent` matching another user's doc returns `PreconditionFailed` ("already has a
  matching document") vs `Ok` — leaking **existence**.

No document **body** is ever returned, and a follow-up write to a doc the caller does not
own is still `Forbidden` via `check_owner` — so this is an information-disclosure oracle,
not a read/write bypass. Still, an existence/version oracle is a real leak worth closing.

## Goals

- Close the existence/version oracle for both steps, for every per-row-auth model
  (`ownerField`, `collaboratorsField`, `authorize`, and any combination).
- Do it in a way consistent with the system's existing posture that **unauthorized reads
  filter silently** — the caller simply sees fewer rows, with no error that would itself
  reveal a row exists.
- Preserve the legitimate optimistic-concurrency use (a user guarding a mutation to their
  **own** doc with `expectVersion`) byte-for-byte.
- Leave bypass callers (machine tokens, scheduled jobs, admin) and tables with no per-row
  declaration byte-identical to today.

## Non-goals

- No wire/protocol change. The two steps already exist on the wire and in all four
  clients; this is **server-side enforcement only**. No client mirror work.
- No change to the `Forbidden` semantics of the write path (`check_owner` is untouched).
- No change to `Upsert`, whose update branch already owner-checks via `check_owner_doc`.
- No new precondition kinds, no new error codes.

## Approach (Option A — silent/absent)

Extend the same silent-filtering model the read path uses to these two precondition steps:
**from the caller's point of view, the table contains only rows visible to them.** A
non-visible document is therefore indistinguishable from a genuinely-absent document:

- `ExpectVersion` on a doc the caller cannot see → `NotFound` (the existing absent
  outcome) — never `PreconditionFailed`, never `Ok`, so no version is ever leaked.
- `ExpectAbsent` matching only docs the caller cannot see → succeeds (`Ok`) — those docs
  are "absent" from the caller's view, so the precondition holds.

This **collapses the oracle**: for a doc the caller cannot see, every probe returns the
same thing it would return for a doc that does not exist at all. It also matches the spec's
stated read model ("the user just sees fewer rows") rather than introducing a louder
`Forbidden` that would itself be a bigger oracle ("exists, but not yours").

Own-doc behavior is unchanged: a caller probing a doc they *can* see still gets
`NotFound` / `PreconditionFailed` / `Ok` exactly as today.

## Detailed design

All changes are in `server/src/txn.rs`. The per-row visibility primitives already exist —
`row_visible_to` (owner OR collaborator) and `filter_matches` (the `authorize` predicate) —
and `check_owner` / `check_owner_doc` already compose them for the write path. This design
factors that composition into one boolean predicate and applies it to the two steps.

### 1. A shared visibility predicate

Add a function that mirrors `check_owner_doc`'s gate composition but returns `bool`
("visible") instead of `Result`:

```rust
/// True iff `doc` is visible to `ctx` under the table's per-row gates.
/// Same composition as `check_owner_doc`: a user caller must pass both the
/// `ownerField`/`collaboratorsField` gate and the `authorize` predicate when
/// the table declares them. Bypass callers (`None` uid) and tables that
/// declare neither gate are always visible — byte-identical to today.
fn doc_visible_to(
    doc: &serde_json::Value,
    table_def: &TableDef,
    ctx: &PrincipalCtx,
) -> bool {
    let owner_uid = row_auth_enforced_uid(table_def, ctx.user_id.as_deref());
    let authorize = table_def.authorize.as_ref();
    let user_is_some = ctx.user_id.is_some();
    if owner_uid.is_none() && !(authorize.is_some() && user_is_some) {
        return true; // no gate applies
    }
    let mut visible = true;
    if let Some(uid) = owner_uid {
        visible &= row_visible_to(
            doc,
            table_def.owner_field.as_deref(),
            table_def.collaborators_field.as_deref(),
            uid,
        );
    }
    if let Some(authorize) = authorize
        && user_is_some
    {
        visible &= filter_matches(doc, authorize, ctx);
    }
    visible
}
```

This is deliberately the bool twin of `check_owner_doc`, so the two cannot drift: if a
future per-row model adds a gate, it must be added in both places (worth a code comment
linking them). It over-approximates invisibility on typing/missing-field doubt exactly as
the read path does (a doubtful row is treated as not visible), never under-approximating.

### 2. `ExpectVersion` enforcement

`do_expect_version` currently `SELECT "version"` and maps a missing row to `NotFound`. It
gains `table_def: &TableDef` and `ctx: &PrincipalCtx` parameters, and selects `doc` too:

```rust
let row: Option<(i64, serde_json::Value)> = /* SELECT "version", "doc" WHERE id = $1 */;
let Some((actual, doc)) = row else {
    return Err(RtDbError::not_found(format!("document '{id}' not found")));
};
// Oracle closure: a doc the caller cannot see is indistinguishable from absent.
if !doc_visible_to(&doc, table_def, ctx) {
    return Err(RtDbError::not_found(format!("document '{id}' not found")));
}
if actual != expected {
    return Err(RtDbError::precondition(format!(
        "version mismatch: expected {expected}, actual {actual}"
    )));
}
Ok(())
```

The non-visible path uses the **same** `not_found` constructor and message as the
genuinely-absent path, so the two outcomes are byte-identical on the wire. The dispatch arm
passes the already-in-scope `ctx` and resolves `table_def` (it currently calls
`schema.table(table)?` and discards the def — it now keeps it):

```rust
Step::ExpectVersion { table, id, version } => {
    let table_def = schema.table(table)?;
    do_expect_version(&mut tx, &pg_schema_name, table_def, table, id, *version, ctx).await?;
    results.push(serde_json::Value::Null);
}
```

### 3. `ExpectAbsent` enforcement

`eq_lookup` already returns `(id, doc, created_at)` triples, so the doc is in hand. The arm
filters the matches to those visible to the caller before deciding "present":

```rust
Step::ExpectAbsent { table, index, eq } => {
    let table_def = schema.table(table)?;
    let rows = eq_lookup(&mut tx, &pg_schema_name, table_def, table, index, eq).await?;
    let present = rows
        .iter()
        .any(|(_id, doc, _created)| doc_visible_to(doc, table_def, ctx));
    if present {
        return Err(RtDbError::precondition(format!(
            "index '{index}' already has a matching document"
        )));
    }
    results.push(serde_json::Value::Null);
}
```

### 4. Bypass / no-gate is a no-op

`doc_visible_to` returns `true` whenever no gate applies — i.e. a bypass caller
(`ctx.user_id` is `None`: machine token, scheduled job, admin-as-`owner=None`) or a table
that declares neither `ownerField`/`collaboratorsField` nor `authorize`. In those cases the
new code paths reduce exactly to today's behavior (`ExpectVersion`: version compare;
`ExpectAbsent`: any-match check). This is the same no-op posture `check_owner` uses, and it
keeps machine/admin and per-row-less tables byte-identical.

## Interaction: `ExpectAbsent` becomes per-user vs. the global unique index

Under this change `ExpectAbsent` is **per-user**: User A's `expectAbsent(index, eq=[K])`
succeeds even when User B owns a row with key `K`, because B's row is invisible to A. This
is the *correct* behavior under row isolation — each user's logical keyspace is independent
for existence checks, matching how the read path already shows each user only their own
rows.

The subtlety: a **real unique index** (the `unique: true` btree, or `CONFLICT`/409) is
still **global** — it rejects A's insert if B's row occupies key `K`. So `ExpectAbsent` can
now return a soft `Ok` that a hard unique constraint then trips on insert. This is
acceptable and already the contract: `ExpectAbsent` is documented as a *soft* precondition
(the friendly pre-check), and the unique index is the authoritative race-free guarantee
(see `2026-08-01-unique-indexes-design.md`). The change does not weaken that guarantee; it
only stops `ExpectAbsent` from being a cross-user oracle. No code handles this — it is
noted in the per-row spec and `FEATURE_MATRIX.md` row #20 as the intended semantics.

## Security invariants & threat model

1. The visibility predicate is **server-side and unforgeable**, derived from the same
   `ctx`/`table_def` the write path trusts.
2. The non-visible outcome is **indistinguishable from absent** (identical `not_found`
   error for `ExpectVersion`; `Ok` for `ExpectAbsent`) — the oracle is collapsed, not
   merely re-labeled.
3. **Typing/missing-field doubt over-approximates to invisible** (never leaks) — same
   posture as the read path's "row not visible".
4. **Bypass is unchanged** — Machine/admin/scheduled (`owner = None`) and per-row-less
   tables see no behavior change.
5. **Own-doc optimistic concurrency is preserved** — a caller guarding a mutation to their
   own doc still observes `NotFound`/`PreconditionFailed`/`Ok` exactly as before.
6. **No body is disclosed** — these steps never returned doc bodies and still do not.

## Testing

Add coverage alongside the existing per-row-auth write tests (mirror their A/B/machine
setup). New cases:

- **`ExpectVersion` oracle closed:** on an `ownerField` table seeded with A's and B's docs,
  A running `expectVersion(B's id, *)` for several candidate versions always yields
  `NotFound` — never `PreconditionFailed`, never `Ok` (cannot learn B's version). A on
  A's own doc still yields `NotFound`/`PreconditionFailed`/`Ok` correctly.
- **`ExpectAbsent` oracle closed:** A's `expectAbsent(index, eq=[B's key])` succeeds (`Ok`)
  even though B's doc matches; A's `expectAbsent(eq=[A's own key])` yields
  `PreconditionFailed`; A's `expectAbsent(eq=[unused key])` yields `Ok`.
- **`authorize`-only table** (no `ownerField`): same two probes against a doc the
  `authorize` predicate hides from A → oracle closed.
- **`collaboratorsField`:** a collaborator's `expectVersion`/`expectAbsent` treat the row
  as visible (owner OR collaborator), a non-collaborator does not.
- **Bypass / no-gate control:** a machine token on an `ownerField` table, and any caller on
  a table with no per-row declaration, behave byte-identically to today (version compare;
  any-match check) — the regression guard.
- **Composability:** `expectVersion` on an unowned doc composed in the same txn with a
  legitimate own-doc write still aborts cleanly at the precondition without leaking which
  step failed beyond the existing error shape.

## Files

- `server/src/txn.rs` — new `doc_visible_to`; `do_expect_version` signature + body; the two
  dispatch arms (`ExpectVersion`, `ExpectAbsent`). No other file changes.
- `server/tests/` — new test cases (in the per-row-auth test module that already covers
  `ownerField`/`collaboratorsField`/`authorize`).

No changes to `protocol.rs`, `query.rs`, `subs.rs`, any client (`ts-client` /
`rust-client` / `python-client`), or the dashboard.

## Follow-on doc updates (after implementation)

- `FEATURE_MATRIX.md` row #20 — change *"Still deferred: `ExpectVersion`/`ExpectAbsent`"*
  to implemented, with a one-line note on the silent/absent semantics and the per-user
  `ExpectAbsent` interaction with the global unique index.
- `2026-07-24-per-row-authorization-design.md` threat-model item 7 and
  `2026-08-02-per-row-auth-predicate-dsl-design.md` security-invariant item 7 — mark the
  carried-forward side-channel **resolved**, pointing here.
- `CLAUDE.md` Auth section if it mentions the deferral (audit at implementation time).

## Verification

- `make dev-db-up` then `make checkall` (fmt-check + clippy `-D warnings` + typecheck +
  the full test suite). The new tests require the real Postgres (per repo convention).
- No clippy warnings, no `unwrap`/`expect` outside `#[cfg(test)]`.

## Open questions

None. The one design choice (silent/absent mapping) is settled — Option A, per design
review — and the scope is server-only.
