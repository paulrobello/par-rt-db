# FM-32 — Field-level default values

**Status:** implemented 2026-08-16
**Card:** par-rt-db backlog `[FM-32] Field-level default values`
**Scope:** server schema DSL + write path; mirrored in ts/rust/python clients (schema DSL + in-memory harness). Dashboard displays only (no wire duplicate).

## Wire shape

A table may declare `defaults`, a map of field name → literal JSON value:

```json
{
  "fields": {
    "status": { "type": "union", "variants": [ ... ] },
    "priority": { "type": "number" }
  },
  "indexes": [ ... ],
  "defaults": { "status": "backlog", "priority": 0 }
}
```

Rust (`schema::TableDef`):

```rust
#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
pub defaults: BTreeMap<String, serde_json::Value>,
```

Additive — a schema without `defaults` deserializes unchanged and is omitted from
the wire, so the wire corpus and every existing schema round-trip byte-identical.

**Deviation from the card's inline sketch** (noted per the card's contract): the
card sketched `defaultValue` inline on each field definition. That would require
wrapping every `FieldType` site (~225 across server + 3 clients) in a
`FieldDef { type, default? }` envelope — a protocol-wide breaking change. The
table-level `defaults` map achieves identical semantics additively. Recorded in
the card on closure.

## Push-time validation (`TableDef::validate_defaults`)

Runs inside `validate_structure`, so it fires on every push path (admin
push-schema, `ddl::push_schema`, and migrate's derived-schema re-validation).
Three rules, each a `SCHEMA_VIOLATION`:

1. Every key must name a declared field of the table.
2. Values must not be JSON `null` (a null default is indistinguishable from
   "unset" on the wire and would fight the Optional-null-stripping rules).
3. Every value must satisfy `validate_value(field_type, value)` — the same
   checker the write path uses, so a default can never make an insert invalid.

`detect_destructive_changes` compares only fields and indexes, so `defaults`
is freely re-declarable on additive pushes (same treatment as `ttl`/
`ownerField`/`authorize`).

## Apply semantics (`txn::apply_defaults`)

Applied to a **new document** when it omits the key. Exactly three call sites:
`step_insert`, `step_replace`, and `step_upsert`'s insert branch. Never applied
on `patch`, upsert-update, `patchByQuery`, or snapshot replay (`insert_snapshot_row`
replays exact rows) — so **clearing an optional field stays cleared**: a later
patch that nulls/removes the field is never re-defaulted.

Stamp ordering (a default on a field a server stamp also targets): the server
stamp wins.

```
insert:  stamp_ttl_default → apply_defaults → stamp_owner → stamp_authorize
replace: apply_defaults    → stamp_owner    → stamp_authorize
upsert-insert: apply_defaults → stamp_owner → stamp_authorize
```

`stamp_ttl_default` runs first, so a ttl `defaultDurationMs` on the same field
beats a `defaults` entry (ttl is a reaper contract — it must not be
overridable); `stamp_owner`/`stamp_authorize` run after, so principal-stamped
values (`ownerField`, authorize `$user` leaves) beat a `defaults` entry on the
same field (identity is unforgeable by design).

## Migrate interaction (`migrate.rs`)

`defaults` keys are field references, so the directive arms that rewrite field
references maintain them:

- **RenameField** re-keys a defaults entry (same treatment as `ownerField`,
  index fields, and authorize leaves).
- **DropField** removes the dropped field's entry (same as owner/collaborators
  clearing).
- **ChangeType** drops the entry — it was validated against the OLD type; the
  retyped field may no longer accept it. Re-declare on a later additive push.

(`RenameField`'s `ttl.field` is a pre-existing gap NOT addressed here — out of
scope for FM-32.)

## Client mirrors

Same wire shape and same apply seam in each client's in-memory harness
(new-document paths only, after the ttl stamp, before owner stamps):

- **ts-client**: `TableJson.defaults?: Record<string, unknown>`,
  `TableDefinition.defaults(...)` builder, `toDict()` emits it;
  `in_memory.ts` applies in `doInsert`/`doReplace` (upsert-insert inherits via
  `doInsert`).
- **rust-client**: `schema.rs` wire field + builder; `in_memory` applies at the
  `stamp_ttl_default` seam.
- **python-client**: `TableBuilder.defaults(...)` + `to_dict()`; `in_memory.py`
  mirrors the seam. (`wire.py` carries frames only — no TableDef there.)
- **CLI**: no change — `rtdb push-schema` deserializes the rust-client
  `SchemaDef` and forwards it.
- **Dashboard**: display-only; the schema page shows a table's declared
  defaults.
