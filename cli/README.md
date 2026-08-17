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

Which commands consume each credential:

- `RTDB_URL` / `--url` — every command (required; there is no default).
- `RTDB_DB` / `--db` — `query`, `mutate`, `push-schema`, `migrate`, `explain`, `workflows`.
- `RTDB_TOKEN` / `--token` — `query` / `mutate` (machine token).
- `RTDB_ADMIN_KEY` / `--admin-key` — every admin subcommand.

The full flag ↔ env mapping (kept in sync with the CLI definitions) is
generated in
[Global flags and environment variables](#global-flags-and-environment-variables)
below.

## Commands

The command reference in this section is generated from the CLI's own argument
definitions (`cli/src/args.rs`) by `make cli-docs`, and `make cli-docs-check`
(part of `make checkall`) fails when it is stale — a subcommand cannot land
undocumented. Do not edit between the markers by hand.

<!-- cli-reference:begin -->
Full `rtdb --help` output:

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
      --url <URL>              Server base URL (e.g. https://rtdb.pardev.net) [env: RTDB_URL=]
      --db <DB>                Database name — used by `query`, `mutate`, and `push-schema` [env: RTDB_DB=]
      --token <TOKEN>          Machine token for `query` / `mutate` [env: RTDB_TOKEN]
      --admin-key <ADMIN_KEY>  Instance admin key — bearer for every admin subcommand [env: RTDB_ADMIN_KEY]
  -h, --help                   Print help
  -V, --version                Print version
```

### Global flags and environment variables

| Flag | Env var | Description |
| --- | --- | --- |
| `--url <URL>` | `RTDB_URL` | Server base URL (e.g. https://rtdb.pardev.net) **(required)** |
| `--db <DB>` | `RTDB_DB` | Database name — used by `query`, `mutate`, and `push-schema` |
| `--token <TOKEN>` | `RTDB_TOKEN` | Machine token for `query` / `mutate` |
| `--admin-key <ADMIN_KEY>` | `RTDB_ADMIN_KEY` | Instance admin key — bearer for every admin subcommand |

### `rtdb list-dbs`

```text
List every database on the instance. (admin)

Usage: rtdb list-dbs

Options:
  -h, --help  Print help
```

### `rtdb create-db`

```text
Create a new database. (admin)

Usage: rtdb create-db <NAME>

Arguments:
  <NAME>  Database name to create

Options:
  -h, --help  Print help
```

### `rtdb clone-db`

```text
Clone a database (schema + documents) into a new one. (admin)

Usage: rtdb clone-db <FROM> <TO>

Arguments:
  <FROM>  Source database to clone from
  <TO>    Destination database name (must not already exist)

Options:
  -h, --help  Print help
```

### `rtdb push-schema`

```text
Push a SchemaDef JSON file to `--db`. (admin)

Usage: rtdb push-schema <FILE>

Arguments:
  <FILE>  Path to a JSON file containing a `SchemaDef` (wire shape: `{"tables": {<name>: {"fields": {..}}}}`)

Options:
  -h, --help  Print help
```

### `rtdb mint-token`

```text
Mint a machine token for a database. (admin)

Usage: rtdb mint-token <DB> <NAME>

Arguments:
  <DB>    Database to mint the token for
  <NAME>  Human-readable token name (e.g. "ci-seed")

Options:
  -h, --help  Print help
```

### `rtdb revoke-token`

```text
Revoke a machine token by id. (admin)

Usage: rtdb revoke-token <ID>

Arguments:
  <ID>  Token id (`tok_…`) to revoke

Options:
  -h, --help  Print help
```

### `rtdb sessions`

```text
Manage active interactive sessions. (admin)

Usage: rtdb sessions <COMMAND>

Commands:
  list    List active interactive sessions, newest-first
  revoke  Revoke a single session by token hash, or every session for a user
  help    Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
```

#### `rtdb sessions list`

```text
List active interactive sessions, newest-first

Usage: rtdb sessions list [OPTIONS]

Options:
      --user <USER>    Filter by user id or email
      --limit <LIMIT>  Cap the result count (server default 200, clamped to [1, 1000])
  -h, --help           Print help
```

#### `rtdb sessions revoke`

```text
Revoke a single session by token hash, or every session for a user

Usage: rtdb sessions revoke [OPTIONS]

Options:
      --token-hash <TOKEN_HASH>  Token hash (sha256 digest) of the session to revoke. Mutually exclusive with `--user`
      --user <USER>              Revoke every session for this user id. Mutually exclusive with `--token-hash`
  -h, --help                     Print help
```

### `rtdb merge-users`

```text
Merge an anonymous user into a real one, synchronously. (admin)

Usage: rtdb merge-users --anon <ANON> --real <REAL> --confirm <CONFIRM>

Options:
      --anon <ANON>        Anonymous user id whose data is merged away
      --real <REAL>        Real user id that receives the anon user's data
      --confirm <CONFIRM>  Typed confirmation — must equal `--real`
  -h, --help               Print help
```

### `rtdb query`

```text
Run a Query JSON against `--db` and print the result. (machine token)

Usage: rtdb query <QUERY>

Arguments:
  <QUERY>  Query JSON, e.g. `{"table":"items","take":10}`. Prefix with `@` to read from a file (`@query.json`)

Options:
  -h, --help  Print help
```

### `rtdb mutate`

```text
Run a Transaction JSON against `--db` and print step results. (machine token)

Usage: rtdb mutate <TXN>

Arguments:
  <TXN>  Transaction JSON (`{"steps":[..]}`). Prefix with `@` to read from a file (`@seed.json`)

Options:
  -h, --help  Print help
```

### `rtdb migrate`

```text
Apply (or preview with `--dry-run`) a migration directives JSON file to `--db`. (admin)

Usage: rtdb migrate [OPTIONS] <FILE>

Arguments:
  <FILE>  Path to a JSON file containing a `MigrateRequestOwned` body (wire shape: `{"directives":[...], "dryRun"?: bool}`)

Options:
      --dry-run  Preview only — nothing is applied. The request's `dryRun` field is also honored; this flag forces it on
  -h, --help     Print help
```

Directive reference and examples: [Schema migration (`rtdb migrate`)](#schema-migration-rtdb-migrate).

### `rtdb explain`

```text
Explain a Query's compiled SQL against `--db` without running it. (admin)

Usage: rtdb explain <QUERY>

Arguments:
  <QUERY>  Query JSON, e.g. `{"table":"items","take":10}`. Prefix with `@` to read from a file (`@query.json`)

Options:
  -h, --help  Print help
```

### `rtdb slow-queries`

```text
List recent slow queries across the instance. (admin)

Usage: rtdb slow-queries [OPTIONS]

Options:
      --db <DB>        Filter to one database
      --limit <LIMIT>  Cap the result count
  -h, --help           Print help
```

### `rtdb workflows`

```text
Manage durable workflow runs in `--db`. (admin)

Usage: rtdb workflows <COMMAND>

Commands:
  list    List workflow runs in `--db`, newest first
  get     Print one workflow run: the info row plus the per-step outcome trail
  start   Start a new workflow run from a WorkflowSpec JSON file
  cancel  Cancel a workflow run
  help    Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
```

Spec format and semantics: [Workflow runs (`rtdb workflows`)](#workflow-runs-rtdb-workflows).

#### `rtdb workflows list`

```text
List workflow runs in `--db`, newest first

Usage: rtdb workflows list [OPTIONS]

Options:
      --status <STATUS>  Filter by run status: pending|running|success|failed|cancelled
      --limit <LIMIT>    Cap the result count (server default 100, capped at 500)
  -h, --help             Print help
```

#### `rtdb workflows get`

```text
Print one workflow run: the info row plus the per-step outcome trail

Usage: rtdb workflows get --id <ID>

Options:
      --id <ID>  Workflow run id to fetch
  -h, --help     Print help
```

#### `rtdb workflows start`

```text
Start a new workflow run from a WorkflowSpec JSON file

Usage: rtdb workflows start --file <FILE>

Options:
      --file <FILE>  Path to a JSON file containing a `WorkflowSpec` (wire shape: `{"name": .., "steps": [{"txn": ..}]}`). An optional `@` prefix matches the `query`/`mutate` file convention
  -h, --help         Print help
```

#### `rtdb workflows cancel`

```text
Cancel a workflow run

Usage: rtdb workflows cancel --id <ID>

Options:
      --id <ID>  Workflow run id to cancel
  -h, --help     Print help
```
<!-- cli-reference:end -->

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
