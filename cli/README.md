# rtdb CLI

A small operator CLI for [par-rt-db](../README.md), wrapping
[`par-rt-db-client`](../rust-client) for CI and admin workflows against a running
instance: list/create/clone databases, push schema, query/mutate, mint/revoke
tokens, manage sessions, run schema migrations, and observe/cancel durable
workflow runs. Cargo binary name `rtdb`.

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
JSON inline. `--url` is **required** (there is no default) and falls back to the
`RTDB_URL` env var; `--db` selects the target database for data
(query/mutate) commands; `--token` is a per-db machine token and `--admin-key`
is the boot `RTDB_ADMIN_KEY` for admin commands.

## Configuration

Every flag has an environment-variable fallback — the preferred path, since
flag values are visible in `ps` output and shell history (SEC-204; the CLI
prints a warning when it sees a credential on the command line):

| Env var | Flag | Used by |
| --- | --- | --- |
| `RTDB_URL` | `--url` | Every command (required — no default). |
| `RTDB_DB` | `--db` | `query`, `mutate`, `push-schema`, `migrate`, `explain`, `workflows`. |
| `RTDB_TOKEN` | `--token` | `query` / `mutate` (machine token). |
| `RTDB_ADMIN_KEY` | `--admin-key` | Every admin subcommand. |

## Commands

`rtdb --help` (authoritative output, kept in sync with the CLI's argument
definitions):

```text
Operator + CI CLI for par-rt-db

Usage: rtdb [OPTIONS] --url <URL> <COMMAND>

Commands:
  list-dbs      List every database on the instance. (admin)
  create-db     Create a new database. (admin)
  clone-db      Clone a database (schema + documents) into a new one. (admin)
  push-schema   Push a SchemaDef JSON file to `--db`. (admin)
  mint-token    Mint a machine token for a database. (admin)
  revoke-token  Revoke a machine token by id. (admin)
  sessions      Manage active interactive sessions. (admin)
  merge-users   Merge an anonymous user into a real one, synchronously. (admin)
  query         Run a Query JSON against `--db` and print the result. (machine token)
  mutate        Run a Transaction JSON against `--db` and print step results. (machine token)
  migrate       Apply (or preview with `--dry-run`) a migration directives JSON file to `--db`. (admin)
  explain       Explain a Query's compiled SQL against `--db` without running it. (admin)
  slow-queries  List recent slow queries across the instance. (admin)
  workflows     Manage durable workflow runs in `--db`. (admin)
  help          Print this message or the help of the given subcommand(s)

Options:
      --url <URL>              Server base URL (e.g. https://rtdb.pardev.net). [env: RTDB_URL=]
      --db <DB>                Database name — used by `query`, `mutate`, and `push-schema`. [env: RTDB_DB=]
      --token <TOKEN>          Machine token for `query` / `mutate`. [env: RTDB_TOKEN]
      --admin-key <ADMIN_KEY>  Instance admin key — bearer for every admin subcommand. [env: RTDB_ADMIN_KEY]
  -h, --help                   Print help
  -V, --version                Print version
```

Per-command notes:

| Command | Auth | Shape |
| --- | --- | --- |
| `list-dbs` | admin | Lists every database on the instance. |
| `create-db <NAME>` | admin | Creates a new database. |
| `clone-db <FROM> <TO>` | admin | Clones schema + documents; `TO` must not already exist. |
| `push-schema <FILE>` | admin | Pushes a `SchemaDef` JSON file (additive) to `--db`. |
| `mint-token <DB> <NAME>` | admin | Mints a machine token; prints `{tokenId, token}` — the plaintext token is shown once. |
| `revoke-token <ID>` | admin | Revokes a machine token by id (`tok_…`). |
| `sessions list [--user] [--limit]` | admin | Active interactive sessions, newest-first; `--user` filters by id or email, `--limit` caps (server default 200). |
| `sessions revoke (--token-hash \| --user)` | admin | Revokes one session by sha256 token hash, or every session for a user (mutually exclusive flags). |
| `merge-users --anon --real --confirm` | admin | Merges an anonymous user into a real one synchronously; `--confirm` must equal `--real`. |
| `query <QUERY>` | machine token | Runs a Query JSON (or `@file`) against `--db`, prints the result. |
| `mutate <TXN>` | machine token | Runs a Transaction JSON (or `@file`), prints step results. |
| `migrate [--dry-run] <FILE>` | admin | See [Schema migration](#schema-migration-rtdb-migrate) below. |
| `explain <QUERY>` | admin | Prints the compiled SQL + bind params for a Query JSON against `--db` — no rows. |
| `slow-queries [--db] [--limit]` | admin | Recent slow queries across the instance (from the server's bounded ring). |
| `workflows <sub>` | admin | See [Workflow runs](#workflow-runs-rtdb-workflows) below. |

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

## Workflow runs (`rtdb workflows`)

Observe and drive durable declarative workflow runs (FM-29) in `--db`.
Admin-only; every command prints pretty JSON.

```sh
rtdb --url $URL --admin-key $KEY --db kanban workflows list --status running --limit 20
rtdb --url $URL --admin-key $KEY --db kanban workflows get --id <runId>
rtdb --url $URL --admin-key $KEY --db kanban workflows start --file spec.json
rtdb --url $URL --admin-key $KEY --db kanban workflows cancel --id <runId>
```

`list` filters by `--status` — validated client-side against exactly
`pending`|`running`|`success`|`failed`|`cancelled` — and pages with `--limit`
(server default 100, capped at 500). `get` prints the full run: the
info fields plus the per-step outcome trail. `start` reads a `WorkflowSpec`
JSON file (`{"name": .., "steps": [{"txn": <Transaction>, "retry"?: ..,
"sleepBeforeMs"?: ..}]}`, `@`-prefix supported) and prints the new run id.
`cancel` prints `{ok}`; `ok: false` means the run was unknown or already
terminal — a legitimate no-op, not an error — and warns on stderr.

## Develop

```sh
cd cli && cargo test --all-features
```

The repo-wide gate is `make -C .. checkall`.
