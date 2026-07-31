# rtdb CLI

A small operator CLI for [par-rt-db](../README.md), wrapping
[`par-rt-db-client`](../rust-client) for CI and admin workflows against a running
instance: list/create databases, push schema, query/mutate, mint/revoke tokens,
and run schema migrations. Cargo binary name `rtdb`.

## Install

```sh
cargo install --path cli      # from the repo root
# or: cd cli && cargo build
```

## Usage

```sh
rtdb --url https://rtdb.pardev.net --admin-key $RTDB_ADMIN_KEY list-dbs
rtdb --url https://rtdb.pardev.net --admin-key $RTDB_ADMIN_KEY create-db kanban
rtdb --url https://rtdb.pardev.net --admin-key $RTDB_ADMIN_KEY push-schema --db kanban schema.json
rtdb --url https://rtdb.pardev.net --db kanban --token $RTDB_TOKEN mutate '@seed.json'
rtdb --url https://rtdb.pardev.net --db kanban --token $RTDB_TOKEN query '{"table":"items","take":10}'
```

`@<path>` reads a JSON argument from a file; otherwise the argument is parsed as
JSON inline. `--url` defaults to `http://127.0.0.1:8300`; `--db` selects the
target database for data (query/mutate) commands; `--token` is a per-db machine
token and `--admin-key` is the boot `RTDB_ADMIN_KEY` for admin commands.

## Schema migration (`rtdb migrate`)

Apply (or preview with `--dry-run`) a declarative migration directives file to
`--db`. This is the destructive/type-changing counterpart to the additive
`push-schema` — rename, type-coerce, drop, backfill a default, or run a scoped
raw-SQL doc rewrite. Admin-only.

```sh
# Preview first (always recommended, especially for changeType/evalExpr):
rtdb --url $URL --admin-key $KEY --db kanban migrate --dry-run migrate.json

# Apply:
rtdb --url $URL --admin-key $KEY --db kanban migrate migrate.json
```

`migrate.json` is a `MigrateRequestOwned` body — `{"directives":[...], "dryRun"?: bool}`.
The `--dry-run` flag forces preview on; a `dryRun: true` in the file is also
honored (a checked-in preview request can never be silently applied). The command
prints the `MigrateResult` (per-directive `affectedRows`/`castFailures`/
`sampleChanges` + the derived schema) and warns on stderr when nothing was applied.

Example directives file:

```json
{
  "directives": [
    {"op": "renameField", "table": "items", "from": "title", "to": "summary"},
    {"op": "changeType", "table": "items", "field": "order",
     "to": {"type": "string"}, "cast": "toString", "default": "0"},
    {"op": "setDefault", "table": "items", "field": "status", "value": "backlog"}
  ]
}
```

The closed `cast` set is `toString`/`toNumber`/`toInt64`/`toBoolean`; the optional
`default` substitutes for un-coercible rows (without it a single bad value rolls
the whole migrate back atomically). `evalExpr` is the scoped raw-SQL escape hatch
(one table's `doc` jsonb, no joins/DDL verbs). See the design spec at
[`../docs/superpowers/specs/2026-07-31-schema-migration-backfill-design.md`](../docs/superpowers/specs/2026-07-31-schema-migration-backfill-design.md).

## Develop

```sh
cd cli && cargo test --all-features
```

The repo-wide gate is `make -C .. checkall`.
