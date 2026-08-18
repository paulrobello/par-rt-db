# Schema-Typed Mutation Builder Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `client/src/mutation.ts`'s `TxnBuilder` generic over a schema, so `insert`/`patch`/`replace`/`delete`/`expectVersion`/`expectAbsent`/`upsert` are all typed against `TableNames<S>`/`WithoutSystemFields<S, T>`/`IndexNamesOf<S, T>`, mirroring how `client/src/query.ts`'s `createApi(schema)` already types the query side — with zero breaking changes to the existing untyped call sites.

**Architecture:** `TxnBuilder<S extends SchemaDefinition<any> = SchemaDefinition<any>>` becomes generic with a phantom (never read at runtime) schema type parameter. Its factory `mutation()` gets a second, typed overload: `mutation<S extends SchemaDefinition<any>>(schema: S): TxnBuilder<S>`, alongside the existing zero-arg untyped overload. No changes to `schema.ts`, `client.ts`, `http.ts`, `react.tsx`, or the wire protocol — this is a compile-time-only change. Full design rationale, the three options considered for the entry point, and empirical verification of backward compatibility live in `docs/superpowers/specs/2026-07-22-typed-mutation-builder-design.md` — read it if anything below is unclear on the "why."

**Tech Stack:** TypeScript (strict, `verbatimModuleSyntax`), Vitest 2.1.9 + `expectTypeOf` (backed by the `expect-type` package) for compile-time type tests, Biome for lint/format.

## Global Constraints

- No changes to `client/src/protocol.ts`'s `StepJson`/`TransactionJson` shapes — this is a typing-only change, not a wire change.
- No changes to `client/src/schema.ts` — `TableNames`, `WithoutSystemFields`, `IndexNamesOf` already exist and already encode per-field optionality; reuse them as-is.
- Every existing call site that calls `mutation()` with zero arguments (`client/tests/http.test.ts`, `client/tests/mutation.test.ts`, `client/tests/integration/e2e.test.ts`) must keep compiling and passing unchanged — do not edit those bare `mutation()` call sites.
- `eq: unknown[]` on `expectAbsent`/`upsert` stays untyped (matches `query.ts`'s `TableQuery.withIndex`) — do not attempt to type individual `eq` tuple values against index field types; that is out of scope.
- `make checkall` (fmt-check + clippy `-D warnings` + typecheck + tests, both `server/` and `client/` packages) must be fully green before either task is considered done.
- Do not touch the `~/Repos/projects` kanban board repo.

---

### Task 1: Generic `TxnBuilder` + typed `mutation()` entry point

**Files:**
- Create: `client/tests/mutation.types.test.ts`
- Modify: `client/src/mutation.ts`

**Interfaces:**
- Consumes: `SchemaDefinition`, `TableNames`, `WithoutSystemFields`, `IndexNamesOf` from `client/src/schema.ts` (all pre-existing, unchanged); `StepJson`, `TransactionJson` from `client/src/protocol.ts` (pre-existing, unchanged); `defineSchema`, `defineTable`, `t`, and the `Id<T>` type, also from `client/src/schema.ts`.
- Produces: `TxnBuilder<S extends SchemaDefinition<any> = SchemaDefinition<any>>` (exported class) with generic methods `insert<T extends TableNames<S>>`, `patch<T extends TableNames<S>>`, `replace<T extends TableNames<S>>`, `delete<T extends TableNames<S>>`, `expectVersion<T extends TableNames<S>>`, `expectAbsent<T extends TableNames<S>>`, `upsert<T extends TableNames<S>>`, and unchanged `build(): TransactionJson`. `mutation()` (zero-arg, untyped) and `mutation<S extends SchemaDefinition<any>>(schema: S): TxnBuilder<S>` (typed), both exported. Task 2 consumes these exact names unchanged.

- [ ] **Step 1: Write the failing type test file**

Create `client/tests/mutation.types.test.ts`:

```ts
import { describe, expectTypeOf, it } from "vitest";
import { mutation } from "../src/mutation.js";
import type { Id } from "../src/schema.js";
import { defineSchema, defineTable, t } from "../src/schema.js";

const schema = defineSchema({
  projects: defineTable({
    name: t.string(),
    status: t.union(t.literal("active"), t.literal("paused")),
    order: t.number(),
    archived: t.optional(t.boolean()),
  }).index("by_name", ["name"]),
  items: defineTable({
    projectId: t.id("projects"),
    title: t.string(),
  }).index("by_project", ["projectId"]),
});

const builder = mutation(schema);

describe("typed mutation builder", () => {
  it("accepts a valid insert doc, including omitting optional fields", () => {
    expectTypeOf(builder.insert).toBeCallableWith("projects", {
      name: "p1",
      status: "active",
      order: 1,
    });
    expectTypeOf(builder.insert).toBeCallableWith("items", {
      projectId: "proj1" as Id<"projects">,
      title: "a",
    });
  });

  it("rejects an insert against an unknown table", () => {
    // @ts-expect-error - "bogus" is not a table in the schema
    expectTypeOf(builder.insert).toBeCallableWith("bogus", { name: "p1" });
  });

  it("rejects an insert with a wrong field type", () => {
    // @ts-expect-error - order must be a number
    builder.insert("projects", { name: "p1", status: "active", order: "one" });
  });

  it("rejects an insert with an unknown field", () => {
    // @ts-expect-error - "nickname" is not a declared field
    builder.insert("projects", { name: "p1", status: "active", order: 1, nickname: "x" });
  });

  it("rejects an insert missing a required field", () => {
    // @ts-expect-error - "order" is required
    builder.insert("projects", { name: "p1", status: "active" });
  });

  it("accepts a partial patch", () => {
    expectTypeOf(builder.patch).toBeCallableWith("projects", "id1", { archived: true });
    expectTypeOf(builder.patch).toBeCallableWith("projects", "id1", { name: "renamed" });
  });

  it("rejects a patch with a wrong field type", () => {
    // @ts-expect-error - archived must be a boolean
    builder.patch("projects", "id1", { archived: "yes" });
  });

  it("rejects a patch with an unknown field", () => {
    // @ts-expect-error - "nickname" is not a declared field
    builder.patch("projects", "id1", { nickname: "x" });
  });

  it("rejects a patch that writes a system field", () => {
    // @ts-expect-error - "_id" cannot be patched directly
    builder.patch("projects", "id1", { _id: "x" });
  });

  it("keeps the untyped entry point permissive for arbitrary tables and docs", () => {
    const untyped = mutation();
    expectTypeOf(untyped.insert).toBeCallableWith("anything", { any: "shape" });
  });
});
```

Note on the mix of `expectTypeOf(...).toBeCallableWith(...)` vs. direct calls: `expect-type`'s `toBeCallableWith` correctly rejects an invalid *first* argument (the unknown-table case) but is too permissive on a *second* argument whose type depends on the first (a known `expect-type` limitation with dependent generic parameters) — confirmed empirically while writing this plan, not a stylistic choice. Direct calls with `@ts-expect-error` correctly catch those cases instead. Do not "clean this up" to be uniform — the non-uniformity is load-bearing.

- [ ] **Step 2: Run typecheck to verify it fails**

Run: `cd client && bun run typecheck`

Expected: FAIL, with exactly these errors (line numbers assume the file exactly as written above):

```
tests/mutation.types.test.ts(19,26): error TS2554: Expected 0 arguments, but got 1.
tests/mutation.types.test.ts(35,5): error TS2578: Unused '@ts-expect-error' directive.
tests/mutation.types.test.ts(40,5): error TS2578: Unused '@ts-expect-error' directive.
tests/mutation.types.test.ts(45,5): error TS2578: Unused '@ts-expect-error' directive.
tests/mutation.types.test.ts(50,5): error TS2578: Unused '@ts-expect-error' directive.
tests/mutation.types.test.ts(60,5): error TS2578: Unused '@ts-expect-error' directive.
tests/mutation.types.test.ts(65,5): error TS2578: Unused '@ts-expect-error' directive.
tests/mutation.types.test.ts(70,5): error TS2578: Unused '@ts-expect-error' directive.
```

(Line 19 fails because `mutation()` doesn't accept an argument yet; the `@ts-expect-error` directives are "unused" because the untyped builder currently accepts anything, so those calls don't actually error yet.)

- [ ] **Step 3: Implement the generic `TxnBuilder` and overloaded `mutation()`**

Replace the full contents of `client/src/mutation.ts`:

```ts
import type { StepJson, TransactionJson } from "./protocol.js";
import type { IndexNamesOf, SchemaDefinition, TableNames, WithoutSystemFields } from "./schema.js";

/**
 * Chainable builder for an atomic multi-step transaction. `S` is a phantom
 * schema type used only to type-check table/field names — never read at
 * runtime, the same pattern `RtQuery<Result>` uses for its result type.
 */
export class TxnBuilder<S extends SchemaDefinition<any> = SchemaDefinition<any>> {
  private readonly steps: StepJson[] = [];

  insert<T extends TableNames<S>>(table: T, doc: WithoutSystemFields<S, T>): this {
    this.steps.push({ op: "insert", table, doc });
    return this;
  }

  patch<T extends TableNames<S>>(
    table: T,
    id: string,
    fields: Partial<WithoutSystemFields<S, T>>,
  ): this {
    this.steps.push({ op: "patch", table, id, fields });
    return this;
  }

  replace<T extends TableNames<S>>(table: T, id: string, doc: WithoutSystemFields<S, T>): this {
    this.steps.push({ op: "replace", table, id, doc });
    return this;
  }

  delete<T extends TableNames<S>>(table: T, id: string): this {
    this.steps.push({ op: "delete", table, id });
    return this;
  }

  expectVersion<T extends TableNames<S>>(table: T, id: string, version: number): this {
    this.steps.push({ op: "expectVersion", table, id, version });
    return this;
  }

  expectAbsent<T extends TableNames<S>>(
    table: T,
    index: IndexNamesOf<S, T>,
    eq: unknown[],
  ): this {
    this.steps.push({ op: "expectAbsent", table, index, eq });
    return this;
  }

  upsert<T extends TableNames<S>>(
    table: T,
    args: {
      index: IndexNamesOf<S, T>;
      eq: unknown[];
      insert: WithoutSystemFields<S, T>;
      patch: Partial<WithoutSystemFields<S, T>>;
    },
  ): this {
    this.steps.push({ op: "upsert", table, ...args });
    return this;
  }

  build(): TransactionJson {
    return { steps: [...this.steps] };
  }
}

export function mutation(): TxnBuilder<SchemaDefinition<any>>;
export function mutation<S extends SchemaDefinition<any>>(schema: S): TxnBuilder<S>;
export function mutation<S extends SchemaDefinition<any>>(_schema?: S): TxnBuilder<S> {
  return new TxnBuilder<S>();
}
```

- [ ] **Step 4: Run typecheck to verify it passes**

Run: `cd client && bun run typecheck`
Expected: exits 0, no output beyond the `tsc -p tsconfig.json --noEmit` command echo.

- [ ] **Step 5: Run the new type tests and the existing untyped-call-site tests to confirm no regression**

Run: `cd client && bunx vitest run tests/mutation.types.test.ts tests/mutation.test.ts tests/http.test.ts`
Expected: all test files pass (`tests/mutation.types.test.ts` has 10 passing tests; `tests/mutation.test.ts` and `tests/http.test.ts` are unchanged in this task and must still pass exactly as before — they exercise the bare zero-arg `mutation()` call sites this task must not break).

- [ ] **Step 6: Run lint**

Run: `cd client && bun run lint`
Expected: exits 0, no Biome errors (in particular, no complaint about the unused `_schema` parameter in the implementation signature, or about the `@ts-expect-error` comments).

- [ ] **Step 7: Commit**

```bash
cd ~/Repos/par-rt-db
git add client/src/mutation.ts client/tests/mutation.types.test.ts
git commit -m "$(cat <<'EOF'
feat(client): type TxnBuilder against a schema

TxnBuilder<S> now constrains table names to TableNames<S> and types
insert/replace/upsert.insert against WithoutSystemFields<S, T> and
patch/upsert.patch against Partial<WithoutSystemFields<S, T>>, mirroring
how createApi(schema) already types the query side. mutation() gains a
typed mutation(schema) overload alongside the existing zero-arg one, so
every current untyped call site keeps compiling unchanged.
EOF
)"
```

---

### Task 2: Runtime JSON-shape parity test + full verification

**Files:**
- Modify: `client/tests/mutation.test.ts`

**Interfaces:**
- Consumes: `mutation`, `TxnBuilder` from `client/src/mutation.ts` (produced by Task 1, unchanged); `defineSchema`, `defineTable`, `t`, `Id` from `client/src/schema.ts`.
- Produces: nothing new consumed by later tasks — this is the last task.

- [ ] **Step 1: Read the current file to confirm exact append point**

Run: `cat -n client/tests/mutation.test.ts` (from `~/Repos/par-rt-db`)

Confirm the file currently has exactly two `it()` blocks inside one `describe("transaction builder", ...)` block (as of this plan being written): `"builds an ordered multi-step txn with table on every step"` and `"produces an empty txn when nothing is added"`, closed by a final `});` on the last line. The new test is added as a third `it()` inside that same `describe` block, immediately before its closing `});`.

- [ ] **Step 2: Add the runtime parity test**

Update the top of `client/tests/mutation.test.ts` — replace:

```ts
import { describe, expect, it } from "vitest";
import { mutation } from "../src/mutation.js";
```

with:

```ts
import { describe, expect, it } from "vitest";
import { mutation } from "../src/mutation.js";
import type { Id } from "../src/schema.js";
import { defineSchema, defineTable, t } from "../src/schema.js";
```

Then, immediately before the final `});` that closes the `describe("transaction builder", ...)` block, insert this new `it()`:

```ts
  it("produces the same JSON step shape when built with a typed schema", () => {
    const schema = defineSchema({
      projects: defineTable({
        name: t.string(),
        archived: t.optional(t.boolean()),
      }).index("by_name", ["name"]),
      items: defineTable({
        projectId: t.id("projects"),
        title: t.string(),
      }).index("by_project", ["projectId"]),
    });

    const txn = mutation(schema)
      .insert("items", { projectId: "p1" as Id<"projects">, title: "a" })
      .patch("projects", "id1", { archived: true })
      .replace("items", "i4", { projectId: "p1" as Id<"projects">, title: "c" })
      .delete("items", "i2")
      .expectVersion("items", "i3", 7)
      .expectAbsent("items", "by_project", ["p1"])
      .upsert("items", {
        index: "by_project",
        eq: ["p1"],
        insert: { projectId: "p1" as Id<"projects">, title: "x" },
        patch: { title: "x2" },
      })
      .build();

    expect(txn).toEqual({
      steps: [
        { op: "insert", table: "items", doc: { projectId: "p1", title: "a" } },
        { op: "patch", table: "projects", id: "id1", fields: { archived: true } },
        { op: "replace", table: "items", id: "i4", doc: { projectId: "p1", title: "c" } },
        { op: "delete", table: "items", id: "i2" },
        { op: "expectVersion", table: "items", id: "i3", version: 7 },
        { op: "expectAbsent", table: "items", index: "by_project", eq: ["p1"] },
        {
          op: "upsert",
          table: "items",
          index: "by_project",
          eq: ["p1"],
          insert: { projectId: "p1", title: "x" },
          patch: { title: "x2" },
        },
      ],
    });
  });
```

The resulting file must have exactly one `describe` block containing three `it()` blocks: the two pre-existing ones, unmodified, followed by this new one.

- [ ] **Step 3: Run the file's tests to verify the new test passes and nothing else broke**

Run: `cd client && bunx vitest run tests/mutation.test.ts`
Expected: 3 tests passed (the 2 pre-existing plus the new one), 0 failed.

- [ ] **Step 4: Run the full project gate**

Run: `cd ~/Repos/par-rt-db && make checkall`

Expected: fully green — `fmt-check`, `lint` (clippy `-D warnings` for `server/`, Biome for `client/`), `typecheck` (`cargo check --all-targets` for `server/`, `tsc --noEmit` for `client/`), and `test` (this starts the dev Postgres via `dev-db-up` then runs `cargo test` and `bun run test`) all pass with no errors.

If this fails specifically because `dev-db-up` cannot bind `127.0.0.1:55434` (a port conflict with another running worktree's dev database), do not force-stop another worktree's database. Instead verify equivalently: run `cd server && cargo test` pointed at whichever dev Postgres is already reachable (confirm via `docker compose -f docker-compose.dev.yml ps` from repo root, or by checking `RTDB_TEST_DATABASE_URL`), and separately run `cd client && bun run fmt-check && bun run lint && bun run typecheck && bun run test`, then note the substitution when reporting results — this task does not touch `server/` at all, so `server`'s tests are not expected to be affected either way, but the full-suite gate should still be confirmed green through some path before calling this done.

- [ ] **Step 5: Commit**

```bash
cd ~/Repos/par-rt-db
git add client/tests/mutation.test.ts
git commit -m "$(cat <<'EOF'
test(client): confirm typed TxnBuilder produces identical wire JSON

Adds a runtime assertion that mutation(schema)'s output steps are
byte-identical in shape to the untyped builder's — the schema typing
added in the previous commit is purely compile-time.
EOF
)"
```
