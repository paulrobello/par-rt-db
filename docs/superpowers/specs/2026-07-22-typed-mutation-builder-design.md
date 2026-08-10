# Schema-typed mutation builder — design

**Status:** Implemented (2026-08-10) — schema-typed `TxnBuilder` mirrored across the ts/rust/python clients; FEATURE_MATRIX #6.

## Problem

`client/src/mutation.ts`'s `TxnBuilder` (`insert`, `patch`, `replace`, `delete`,
`expectVersion`, `expectAbsent`, `upsert`) is completely untyped against the
schema: `table` is a bare `string`, and doc/fields params are
`Record<string, unknown>`. This is unlike the query side, where
`client/src/query.ts`'s `TableQuery<DocT, Indexes>` and the `ClientApi<S>`
mapped type over `TableNames<S>` give full schema-inferred typing via
`createApi(schema)`. The gap blocks downstream consumers (e.g. the kanban
board project, a separate repo) from getting type safety and autocomplete on
mutations when porting to par-rt-db, mirroring how their queries are already
typed against `createApi`.

## Finding: no existing "client constructs a TxnBuilder" entry point

The obvious assumption — that `RtDbClient`/`RtDbHttpClient` construct or
thread a schema into a `TxnBuilder` the way `createApi(schema)` threads one
into `ClientApi<S>` — does not hold. `RtDbClient.mutate()` and
`RtDbHttpClient.mutate()` both take an already-built plain `TransactionJson`,
never a builder instance. `mutation()` (in `mutation.ts`) is a fully
standalone factory, decoupled from any client, that returns `new
TxnBuilder()` with zero schema involvement. The real analog to
`createApi(schema)` is this standalone `mutation()` factory, not anything in
`client.ts`.

## Scope

`client/src/mutation.ts` only. No changes to `schema.ts` — `TableNames`,
`WithoutSystemFields`, and `IndexNamesOf` already exist and already encode
per-field optionality (via `Validator<T, Optional>`'s phantom `Optional`
param), so they're reused as-is.

## Design

### `TxnBuilder` becomes generic over the schema

```ts
export class TxnBuilder<S extends SchemaDefinition<any> = SchemaDefinition<any>> {
  insert<T extends TableNames<S>>(table: T, doc: WithoutSystemFields<S, T>): this
  patch<T extends TableNames<S>>(table: T, id: string, fields: Partial<WithoutSystemFields<S, T>>): this
  replace<T extends TableNames<S>>(table: T, id: string, doc: WithoutSystemFields<S, T>): this
  delete<T extends TableNames<S>>(table: T, id: string): this
  expectVersion<T extends TableNames<S>>(table: T, id: string, version: number): this
  expectAbsent<T extends TableNames<S>>(table: T, index: IndexNamesOf<S, T>, eq: unknown[]): this
  upsert<T extends TableNames<S>>(table: T, args: {
    index: IndexNamesOf<S, T>
    eq: unknown[]
    insert: WithoutSystemFields<S, T>
    patch: Partial<WithoutSystemFields<S, T>>
  }): this
  build(): TransactionJson // unchanged, not schema-dependent
}
```

All seven step methods fall out of the same `T extends TableNames<S>`
substitution with no additional design work, so all get typed for
consistency — including `index` name typing on `expectAbsent`/`upsert`,
mirroring `TableQuery.withIndex`'s existing `Indexes` param. `eq: unknown[]`
stays untyped on both methods, matching `query.ts`'s `withIndex` — tuple-typing
per-index values against each indexed field's declared type would be new
design work beyond this task's scope.

`patch`'s `fields` (and `upsert`'s `patch`) is `Partial<WithoutSystemFields<S,
T>>` — every declared field becomes optional (a true subset, matching patch's
merge semantics), while `_id`/`_creationTime`/`_version` stay excluded
because they were never part of `WithoutSystemFields` in the first place.

None of `TxnBuilder`'s methods need the schema's runtime *value* — `S` is a
purely phantom type parameter, the same pattern `RtQuery<Result>` already
uses for its `__result` marker. The class's internals (`steps` array,
`build()`) do not change at all.

### Entry point: overloaded `mutation()`

```ts
export function mutation(): TxnBuilder<SchemaDefinition<any>>;
export function mutation<S extends SchemaDefinition<any>>(schema: S): TxnBuilder<S>;
export function mutation<S extends SchemaDefinition<any>>(_schema?: S): TxnBuilder<S> {
  return new TxnBuilder<S>();
}
```

The schema parameter on the typed overload exists purely for type inference
(`typeof schema` flows into `S`) and is discarded at runtime, since
`TxnBuilder` never reads it.

This was a genuine product decision (three options were weighed: overload the
existing `mutation()`, require a schema always as an exact mirror of
`createApi` — breaking — or add a new distinct `createMutation(schema)`
export). The overload was chosen and confirmed with the user: it keeps every
existing call site (`mutation()` with no args, across `http.test.ts`,
`mutation.test.ts`, `integration/e2e.test.ts`) compiling completely
unchanged, while making the typed path opt-in.

**Backward-compatibility verified empirically**, not just reasoned about: for
`S` defaulted to `SchemaDefinition<any>`,
- `TableNames<S>` resolves to `string` (same as today's bare `table: string`)
- `WithoutSystemFields<S, T>` resolves to `{ [x: string]: unknown }`, i.e.
  structurally identical to today's `Record<string, unknown>`
- `IndexNamesOf<S, T>` resolves to a type that accepts arbitrary string index
  names, same as today's bare `index: string`

confirmed via standalone `tsc --noEmit --strict` probes against the compiled
type aliases before writing this doc. The untyped `mutation()` path is not an
approximation of today's behavior — it is behaviorally identical.

## Testing

- New `client/tests/mutation.types.test.ts`, mirroring the existing
  `client/tests/schema.types.test.ts` pattern (`describe`/`it`/`expectTypeOf`
  from `vitest`, a local `defineSchema` fixture with at least one required
  field, one optional field, and one indexed table).
  - Positive: build a `mutation(schema)` instance and assert
    `expectTypeOf(builder.insert).toBeCallableWith("table", validDoc)` (same
    for `patch`), proving valid inserts/patches type-check, that an optional
    field may be omitted from `insert`, and that a single-field partial is
    accepted by `patch`.
  - Negative: `// @ts-expect-error` immediately above the failing call.
    **Verified empirically before writing the plan** (staged the real
    implementation as scratch `.probe.ts` files, ran the project's actual
    `bun run typecheck`, then deleted the scratch files): `expectTypeOf(fn).
    toBeCallableWith(...)` correctly rejects an invalid *first* argument
    (e.g. an unknown table name) but is too permissive on a *second*
    argument whose type depends on the first (e.g. `doc: WithoutSystemFields
    <S, T>` where `T` is inferred from the table argument) — a known
    `expect-type` limitation with dependent generic parameters, not a defect
    in `insert`/`patch`'s typing (a direct, non-`toBeCallableWith` call with
    `@ts-expect-error` correctly rejects the same invalid shapes). So: table-
    name rejection uses `expectTypeOf(builder.insert).toBeCallableWith(...)`
    with `@ts-expect-error`; wrong-field-type, unknown-field, missing-
    required-field, and system-field-on-patch rejection use direct calls
    (`builder.insert(...)` / `builder.patch(...)`) with `@ts-expect-error`
    instead, since that is the mechanism confirmed to actually catch them.
- `client/tests/mutation.test.ts` gets one new `it()` exercising the typed
  builder (`mutation(schema).insert(...).patch(...).build()`) against a
  schema fixture, asserting the produced `TransactionJson` is identical in
  shape to what the untyped builder already produces — confirming this
  change is purely compile-time, not a wire-format change.

## Verification

`make checkall` from the repo root (fmt-check + clippy `-D warnings` +
typecheck + tests for both `server/` and `client/`) must be fully green.

## Out of scope

- Typing `eq` value tuples against each index's declared field types.
- Any change to `client.ts`, `http.ts`, `react.tsx`, or `schema.ts`.
- Touching the kanban board repo (`~/Repos/projects`) — this task is scoped
  to par-rt-db's client SDK only; the kanban port itself is tracked
  separately.
