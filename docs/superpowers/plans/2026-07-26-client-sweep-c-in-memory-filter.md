# Client Sweep — Item C: In-Memory `filter` Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the `InMemoryRtDbClient` bug where a query carrying a `filter` silently returns unfiltered results — add a `FilterExpr` evaluator with type-coercion semantics matching the server, and wire it into the row-filter loop.

**Architecture:** Two new pure functions in `ts-client/src/in_memory.ts` — `validateFilter(node, fields)` (structural validation against declared fields, called once) and `evalFilterExpr(node, doc)` (predicate evaluation, called per candidate row) — mirroring server `query::FilterExpr` (`server/src/query.rs` `field_lhs_and_bind`/`jsonb_lhs_and_bind`). Wired into `executeQuery`'s row-filter loop at `in_memory.ts:930-964`. `search`/`vectorSearch` stay as their honest `[]` stubs (out of scope — not buggy).

**Tech Stack:** TypeScript, Vitest, biome. ts-client is a bun workspace.

## Global Constraints

- **No wire/protocol changes** — `protocol.ts` is untouched; `FilterExpr` already exists there (`:36-45`).
- **Mirror server `query::FilterExpr`** (`server/src/query.rs:1012-1055`): filter value-kind picks the comparison domain — string → compare the doc field's `->>` text; number → compare as `float8`; boolean → compare as `boolean`. A null/absent doc field never matches (SQL NULL exclusion). Unknown field / empty `and`/`or` / empty `in` / non-string-number-boolean value → `RtDbError("BAD_REQUEST", …)` matching the server's messages.
- ESM with `.js` import specifiers; biome formatting.
- Tests are pure unit tests — no server, no Postgres. The in-memory harness is a no-network fake.
- **Scope is `filter` only** — do not implement in-memory `search`/`vectorSearch` ranking; they stay as their existing honest `[]` stubs.
- Verification: each task runs `cd ts-client && bunx vitest run tests/in_memory.test.ts`; the final task runs the ts-client gate (full `make checkall` runs at branch finish). Add `bunx biome check` on touched files before commit (Tasks 1–3 of item B omitted biome — fold it in here).
- Commits: one atomic commit per task, conventional style.

## Reference: server filter semantics (the contract to mirror)

From `server/src/query.rs`:
- `field_lhs_and_bind` (`:1012`): unknown field → `BAD_REQUEST "filter references unknown field '{field}'"`. Indexed field compares against its typed column; non-indexed declared field → `jsonb_lhs_and_bind`.
- `jsonb_lhs_and_bind` (`:1038`): string value → `(doc->>'{field}')` as `Text`; number value → `(doc->>'{field}')::float8` as `Num`; boolean value → `(doc->>'{field}')::boolean` as `Bool`; otherwise → `BAD_REQUEST "filter value must be a string, number, or boolean"`.
- `compile_filter_node` (`:924`): `and`/`or` with empty `exprs` → `BAD_REQUEST "{and|or} filter requires at least one expr"`; `in` with empty `values` → `BAD_REQUEST "in filter requires at least one value"`; `in` values must share a type.
- Null/absent doc field → `doc->>'{field}'` is SQL `NULL` → every comparison yields `NULL` → row excluded (never matches).

---

## File Structure

- `ts-client/src/in_memory.ts` — add `validateFilter` + `evalFilterExpr` (+ small coercion helpers), export them; wire into the `executeQuery` row-filter loop.
- `ts-client/tests/in_memory.test.ts` — direct unit tests for the two functions (Task 1) + end-to-end query tests (Task 2).
- `ts-client/README.md` — note that the in-memory harness now evaluates `filter` correctly (if the harness is documented there).
- `FEATURE_MATRIX.md` — row #19 note: the ts in-memory `filter` bug is fixed.

---

## Task 1: `validateFilter` + `evalFilterExpr` (pure functions)

**Files:**
- Modify: `ts-client/src/in_memory.ts` (add + export two functions near the other helpers)
- Test: `ts-client/tests/in_memory.test.ts` (add a `describe` for the two functions)

**Interfaces:**
- Consumes: `FilterExpr` from `./protocol.js` (`:36-45`), `RtDbError` from `./errors.js`.
- Produces: `validateFilter(node: FilterExpr, fields: ReadonlySet<string>): void` and `evalFilterExpr(node: FilterExpr, doc: Record<string, unknown>): boolean`, both exported from `in_memory.js`.

- [ ] **Step 1: Write the failing tests**

Add to `ts-client/tests/in_memory.test.ts` (import `validateFilter`, `evalFilterExpr` from `../src/in_memory.js`; the existing file already imports from there). Add a `describe("evalFilterExpr + validateFilter", …)` block:

```ts
describe("evalFilterExpr + validateFilter", () => {
  const fields = new Set(["name", "age", "active", "score"]);

  it("eq/neq on strings compare the doc field's text", () => {
    expect(evalFilterExpr({ op: "eq", field: "name", value: "ada" }, { name: "ada" })).toBe(true);
    expect(evalFilterExpr({ op: "eq", field: "name", value: "ada" }, { name: "bob" })).toBe(false);
    expect(evalFilterExpr({ op: "neq", field: "name", value: "ada" }, { name: "bob" })).toBe(true);
  });

  it("number domain compares numerically (gt/gte/lt/lte)", () => {
    expect(evalFilterExpr({ op: "gt", field: "age", value: 30 }, { age: 42 })).toBe(true);
    expect(evalFilterExpr({ op: "gt", field: "age", value: 50 }, { age: 42 })).toBe(false);
    expect(evalFilterExpr({ op: "lte", field: "age", value: 42 }, { age: 42 })).toBe(true);
  });

  it("string ordering is lexicographic", () => {
    expect(evalFilterExpr({ op: "lt", field: "name", value: "b" }, { name: "ada" })).toBe(true);
    expect(evalFilterExpr({ op: "gte", field: "name", value: "a" }, { name: "ada" })).toBe(true);
  });

  it("boolean domain compares booleans", () => {
    expect(evalFilterExpr({ op: "eq", field: "active", value: true }, { active: true })).toBe(true);
    expect(evalFilterExpr({ op: "eq", field: "active", value: true }, { active: false })).toBe(false);
  });

  it("a number filter value matches a numeric string field (float8 cast)", () => {
    expect(evalFilterExpr({ op: "eq", field: "score", value: 5 }, { score: "5" })).toBe(true);
  });

  it("null/absent doc field never matches (SQL NULL exclusion)", () => {
    expect(evalFilterExpr({ op: "eq", field: "name", value: "ada" }, { name: null })).toBe(false);
    expect(evalFilterExpr({ op: "eq", field: "name", value: "ada" }, {})).toBe(false);
    expect(evalFilterExpr({ op: "neq", field: "name", value: "ada" }, {})).toBe(false);
  });

  it("and / or nest recursively", () => {
    const expr = { op: "and", exprs: [
      { op: "gte", field: "age", value: 30 },
      { op: "or", exprs: [
        { op: "eq", field: "name", value: "ada" },
        { op: "eq", field: "name", value: "bob" },
      ] },
    ] } as const;
    expect(evalFilterExpr(expr, { age: 42, name: "ada" })).toBe(true);
    expect(evalFilterExpr(expr, { age: 42, name: "zed" })).toBe(false);
    expect(evalFilterExpr(expr, { age: 10, name: "ada" })).toBe(false);
  });

  it("in matches membership", () => {
    expect(evalFilterExpr({ op: "in", field: "name", values: ["ada", "bob"] }, { name: "bob" })).toBe(true);
    expect(evalFilterExpr({ op: "in", field: "name", values: ["ada", "bob"] }, { name: "zed" })).toBe(false);
  });

  it("validateFilter rejects an unknown field", () => {
    expect(() => validateFilter({ op: "eq", field: "missing", value: "x" }, fields)).toThrow(/unknown field/);
  });

  it("validateFilter rejects empty and/or and empty in", () => {
    expect(() => validateFilter({ op: "and", exprs: [] }, fields)).toThrow(/at least one expr/);
    expect(() => validateFilter({ op: "or", exprs: [] }, fields)).toThrow(/at least one expr/);
    expect(() => validateFilter({ op: "in", field: "name", values: [] }, fields)).toThrow(/at least one value/);
  });

  it("validateFilter rejects a non-string/number/boolean value", () => {
    expect(() => validateFilter({ op: "eq", field: "name", value: null }, fields)).toThrow(/string, number, or boolean/);
    expect(() => validateFilter({ op: "eq", field: "tags", value: ["a"] }, fields)).toThrow(/string, number, or boolean/);
  });

  it("validateFilter accepts a well-formed nested filter", () => {
    expect(() => validateFilter({ op: "and", exprs: [
      { op: "eq", field: "name", value: "ada" },
      { op: "in", field: "age", values: [1, 2] },
    ] }, fields)).not.toThrow();
  });
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd ts-client && bunx vitest run tests/in_memory.test.ts -t "evalFilterExpr"`
Expected: FAIL — `validateFilter`/`evalFilterExpr` are not exported (import resolves to `undefined`).

- [ ] **Step 3: Implement the two functions**

Add to `ts-client/src/in_memory.ts` (near the other helpers, e.g. above the `InMemoryRtDbClient` class), and export them:

```ts
type FilterLeafOp = "eq" | "neq" | "gt" | "gte" | "lt" | "lte";

/**
 * Structural validation of a `FilterExpr` against a table's declared fields,
 * mirroring server `query::compile_filter_node` / `field_lhs_and_bind`
 * (`query.rs`). Throws `BAD_REQUEST` for an unknown field, an empty `and`/`or`,
 * an empty `in`, or a non-string/number/boolean leaf value. Call once before
 * evaluating per row.
 */
export function validateFilter(node: FilterExpr, fields: ReadonlySet<string>): void {
  switch (node.op) {
    case "and":
    case "or":
      if (node.exprs.length === 0) {
        throw new RtDbError("BAD_REQUEST", `${node.op} filter requires at least one expr`);
      }
      for (const e of node.exprs) validateFilter(e, fields);
      return;
    case "in":
      if (node.values.length === 0) {
        throw new RtDbError("BAD_REQUEST", "in filter requires at least one value");
      }
      for (const v of node.values) checkLeafValue(node.field, v, fields);
      return;
    default:
      checkLeafValue(node.field, node.value, fields);
  }
}

function checkLeafValue(field: string, value: unknown, fields: ReadonlySet<string>): void {
  if (!fields.has(field)) {
    throw new RtDbError("BAD_REQUEST", `filter references unknown field '${field}'`);
  }
  if (typeof value !== "string" && typeof value !== "number" && typeof value !== "boolean") {
    throw new RtDbError("BAD_REQUEST", "filter value must be a string, number, or boolean");
  }
}

/**
 * Evaluate a `FilterExpr` predicate against a stored doc, mirroring server
 * `query::jsonb_lhs_and_bind` (`query.rs`): the filter value's kind picks the
 * comparison domain — string compares the doc field's `->>` text, number
 * compares it as `float8`, boolean as `boolean`. A null/absent field never
 * matches (SQL NULL exclusion). Assumes `validateFilter` already passed.
 */
export function evalFilterExpr(node: FilterExpr, doc: Record<string, unknown>): boolean {
  switch (node.op) {
    case "and":
      return node.exprs.every((e) => evalFilterExpr(e, doc));
    case "or":
      return node.exprs.some((e) => evalFilterExpr(e, doc));
    case "in":
      return node.values.some((v) => compareLeaf("eq", node.field, v, doc));
    default:
      return compareLeaf(node.op, node.field, node.value, doc);
  }
}

function compareLeaf(op: FilterLeafOp, field: string, filterValue: unknown, doc: Record<string, unknown>): boolean {
  const docVal = doc[field];
  if (docVal === null || docVal === undefined) {
    return false;
  }
  if (typeof filterValue === "string") {
    return compareValues(op, docToText(docVal), filterValue);
  }
  if (typeof filterValue === "number") {
    const lhs = docToNumber(docVal);
    return lhs === null ? false : compareValues(op, lhs, filterValue);
  }
  if (typeof docVal === "boolean") {
    return compareValues(op, docVal, filterValue as boolean);
  }
  return false;
}

/** Mirrors Postgres `doc->>'field'`: the JSON text of the value. */
function docToText(docVal: unknown): string {
  if (typeof docVal === "string") return docVal;
  if (typeof docVal === "number") return JSON.stringify(docVal);
  if (typeof docVal === "boolean") return docVal ? "true" : "false";
  return JSON.stringify(docVal);
}

/** Mirrors Postgres `(doc->>'field')::float8`: a number, or a numeric string. */
function docToNumber(docVal: unknown): number | null {
  if (typeof docVal === "number") return Number.isFinite(docVal) ? docVal : null;
  if (typeof docVal === "string" && docVal.trim() !== "") {
    const n = Number(docVal);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function compareValues(op: FilterLeafOp, lhs: string | number | boolean, rhs: string | number | boolean): boolean {
  switch (op) {
    case "eq":
      return lhs === rhs;
    case "neq":
      return lhs !== rhs;
    case "gt":
      return lhs > rhs;
    case "gte":
      return lhs >= rhs;
    case "lt":
      return lhs < rhs;
    case "lte":
      return lhs <= rhs;
  }
}
```

Add `FilterExpr` to the existing `./protocol.js` import in `in_memory.ts` if it is not already imported (it is referenced by the `Query` type but confirm the named import is present).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd ts-client && bunx vitest run tests/in_memory.test.ts`
Expected: PASS — the new `evalFilterExpr + validateFilter` cases pass and all pre-existing in-memory tests still pass.

- [ ] **Step 5: Format + commit**

```
cd ts-client && bunx biome check --write src/in_memory.ts tests/in_memory.test.ts
git add ts-client/src/in_memory.ts ts-client/tests/in_memory.test.ts
git commit -m "feat(ts-client): in-memory FilterExpr evaluator (validateFilter + evalFilterExpr)"
```

---

## Task 2: Wire into `executeQuery` + end-to-end tests

**Files:**
- Modify: `ts-client/src/in_memory.ts` (the `executeQuery` row-filter loop at `:930-964`; add a `validateFilter` call before the loop)
- Test: `ts-client/tests/in_memory.test.ts` (end-to-end query tests)

**Interfaces:**
- Consumes: `validateFilter`, `evalFilterExpr` from Task 1; the existing `executeQuery` row-filter loop (`:930-964`) and `tableDef.fields`.

- [ ] **Step 1: Write the failing end-to-end tests**

Add a `describe("InMemoryRtDbClient filter", …)` block (the test file already constructs a client + pushes a schema elsewhere — mirror that harness). A representative setup:

```ts
describe("InMemoryRtDbClient filter", () => {
  async function seed() {
    const client = new InMemoryRtDbClient();
    await client.pushSchema({
      tables: {
        users: {
          fields: {
            name: { type: "string" },
            age: { type: "number" },
            active: { type: "boolean" },
          },
          indexes: [{ name: "by_name", fields: ["name"] }],
        },
      },
    });
    await client.mutate(mutation().insert("users", { name: "ada", age: 42, active: true }).build());
    await client.mutate(mutation().insert("users", { name: "bob", age: 17, active: false }).build());
    await client.mutate(mutation().insert("users", { name: "cy", age: 65, active: true }).build());
    return client;
  }

  it("a filter reduces the result set to matching docs", async () => {
    const client = await seed();
    const rows = await client.query({
      table: "users",
      filter: { op: "gt", field: "age", value: 20 },
    });
    const names = (rows as Array<{ name: string }>).map((r) => r.name).sort();
    expect(names).toEqual(["ada", "cy"]);
  });

  it("a filter composes with an index eq prefix and take", async () => {
    const client = await seed();
    const rows = await client.query({
      table: "users",
      index: "by_name",
      eq: ["ada"],
      filter: { op: "eq", field: "active", value: true },
    });
    expect(rows).toHaveLength(1);
    expect((rows as Array<{ name: string }>)[0].name).toBe("ada");
  });

  it("an and/or filter evaluates correctly end-to-end", async () => {
    const client = await seed();
    const rows = await client.query({
      table: "users",
      filter: { op: "or", exprs: [
        { op: "lt", field: "age", value: 18 },
        { op: "gte", field: "age", value: 65 },
      ] },
    });
    const names = (rows as Array<{ name: string }>).map((r) => r.name).sort();
    expect(names).toEqual(["bob", "cy"]);
  });

  it("an unknown filter field throws BAD_REQUEST", async () => {
    const client = await seed();
    await expect(
      client.query({ table: "users", filter: { op: "eq", field: "nope", value: "x" } }),
    ).rejects.toMatchObject({ code: "BAD_REQUEST" });
  });
});
```

(Confirm the exact `InMemoryRtDbClient` query/mutate/pushSchema shapes against the existing tests in the file — they may use a builder like `createApi(...)` rather than raw `client.query`. Match whatever the existing tests use.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd ts-client && bunx vitest run tests/in_memory.test.ts -t "InMemoryRtDbClient filter"`
Expected: FAIL — the first test returns all 3 docs (filter ignored), not the 2-doc subset.

- [ ] **Step 3: Wire the evaluator into the row-filter loop**

In `ts-client/src/in_memory.ts`, in `executeQuery`, add a one-time validation before the loop (after `typedEq`/range setup, around line 929) and a per-row evaluation inside the loop (after the range checks pass, before `filtered.push(row)` at `:963`):

```ts
    // Validate the filter against declared fields once (mirrors server compile_filter).
    const fieldSet = new Set(Object.keys(tableDef.fields));
    if (q.filter) {
      validateFilter(q.filter, fieldSet);
    }
    const filtered: StoredRow[] = [];
    for (const row of this.rowsFor(q.table).values()) {
      // … existing index-eq and range checks (unchanged) …
      if (q.filter && !evalFilterExpr(q.filter, row.doc)) {
        continue;
      }
      filtered.push(row);
    }
```

Also: confirm whether the harness rejects `filter` combined with `get` (the server does — filter composes with every terminal except `get`). If the harness's `get` path does not already reject `filter`, add a `BAD_REQUEST` guard mirroring the server. Check the existing `get` handling in `executeQuery` and only add the guard if it is missing.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd ts-client && bunx vitest run tests/in_memory.test.ts`
Expected: PASS — the end-to-end filter tests pass and all pre-existing in-memory tests still pass.

- [ ] **Step 5: Format + commit**

```
cd ts-client && bunx biome check --write src/in_memory.ts tests/in_memory.test.ts
git add ts-client/src/in_memory.ts ts-client/tests/in_memory.test.ts
git commit -m "fix(ts-client): evaluate filter in InMemoryRtDbClient (was silently ignored)"
```

---

## Task 3: Docs + ts-client gate

**Files:**
- Modify: `ts-client/README.md` (if it documents the in-memory harness), `FEATURE_MATRIX.md` (row #19 note)

- [ ] **Step 1: Doc touches**

In `FEATURE_MATRIX.md` row #19 ("Client test harness"), the note currently calls out the ts in-memory `filter` gap. Update that note to reflect that the ts harness now evaluates `filter` correctly (e.g. strike/remove the "filter silently ignored" caveat for the ts client; keep any `search`/`vector` caveats since those still return `[]`). Only the ts-client column — the rust harness (item E) is still pending.

In `ts-client/README.md`, if there is a sentence describing the in-memory harness's limitations (e.g. "filter/search/vector return empty"), update the `filter` part to say it is now evaluated; leave `search`/`vector` as returning empty. If the README does not mention the harness's filter behavior, make no change.

- [ ] **Step 2: Run the ts-client gate**

```
cd ts-client
bunx vitest run          # full ts-client unit suite
bunx tsc --noEmit        # typecheck
bunx biome check src/in_memory.ts tests/in_memory.test.ts
```

All must pass. (Full cross-package `make checkall` runs at branch finish.)

- [ ] **Step 3: Commit**

```
git add ts-client/README.md FEATURE_MATRIX.md
git commit -m "docs(ts-client): in-memory harness now evaluates filter (#19 filter bug fixed)"
```

(Stage only files actually changed.)

---

## Self-Review (completed during authoring)

- **Spec coverage:** C = filter evaluator + wiring. Task 1 = the evaluator (validateFilter + evalFilterExpr), Task 2 = the wiring + end-to-end, Task 3 = docs/gate. ✅
- **Placeholders:** the test code uses `mutation()`/`createApi`/`client.query` shapes that the implementer must confirm against the existing `in_memory.test.ts` harness (flagged in Task 2 Step 1) — the one place a real value must be confirmed rather than guessed. ✅ (flagged, not placeholdered)
- **Type consistency:** `validateFilter(node, fields: ReadonlySet<string>)` and `evalFilterExpr(node, doc)` signatures match across Task 1 (definition) and Task 2 (call sites). `FilterLeafOp` is internal. ✅
- **Server parity:** value-kind domain (string/number/boolean), null/absent exclusion, and the four `BAD_REQUEST` cases all trace to `query.rs` lines cited in the Reference section. ✅
- **Scope:** `search`/`vectorSearch` untouched (stay as `[]` stubs). ✅
