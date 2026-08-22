// Execution-time-relative `olderThan` predicates in by-query steps — the
// engine-level mirror of `server/tests/relative_filter_test.rs`. The op's
// cutoff (`now − ms`) is derived from the injected clock AT EXECUTION, so a
// stored txn stays fresh on every fire instead of freezing a literal. The
// match margins make the clock's exact value irrelevant: OLD (1) is below
// `now − SWEEP_MS` for centuries (epoch-ms today is ~1.8e12; the cutoff is
// ~0.7e12 and rising by 1/year), FUTURE (9e15, f64-exact, within i64) is
// above `now − 0` effectively forever. The clock starts at ~1.7e12 and ticks
// per read, like the semantics-corpus runner.

import { describe, expect, it } from "vitest";

import { RtDbError } from "../src/errors.js";
import { InMemoryRtDbClient } from "../src/in_memory/index.js";
import type { FilterExpr, QueryJson, SchemaJson, StepJson } from "../src/protocol.js";

const OLD = 1;
const FUTURE = 9_000_000_000_000_000;
const SWEEP_MS = 1_000_000_000_000;

function numberSchema(): SchemaJson {
  return {
    tables: {
      tasks: {
        fields: {
          title: { type: "string" },
          updatedAt: { type: "number" },
        },
        indexes: [{ name: "by_title", fields: ["title"] }],
      },
    },
  };
}

/** `updatedAt` as int64 and indexed, so scans compare the decimal-string wire
 * form exactly (the server's typed bigint column path, `EqBind::I64`). */
function int64IndexedSchema(): SchemaJson {
  return {
    tables: {
      tasks: {
        fields: {
          title: { type: "string" },
          updatedAt: { type: "int64" },
        },
        indexes: [
          { name: "by_title", fields: ["title"] },
          { name: "by_updatedAt", fields: ["updatedAt"] },
        ],
      },
    },
  };
}

/** `updatedAt` optional, so a row may legitimately carry no stamp — the
 * absent-value never-matches case (the server unwraps `optional` for the
 * field-kind check). */
function optionalNumberSchema(): SchemaJson {
  return {
    tables: {
      tasks: {
        fields: {
          title: { type: "string" },
          updatedAt: { type: "optional", inner: { type: "number" } },
        },
        indexes: [{ name: "by_title", fields: ["title"] }],
      },
    },
  };
}

function newClient(schema: SchemaJson): InMemoryRtDbClient {
  let ms = 1_700_000_000_000;
  const client = new InMemoryRtDbClient({ now: () => ms++, random: () => 0 });
  client.pushSchema(schema);
  return client;
}

/** `updatedAt` accepts `number | string` so the int64 schema seeds the field's
 * decimal-string wire form; `undefined` seeds an absent stamp. */
async function seed(c: InMemoryRtDbClient, title: string, updatedAt: number | string | undefined) {
  const doc = updatedAt === undefined ? { title } : { title, updatedAt };
  await c.mutate({ steps: [{ op: "insert", table: "tasks", doc }] });
}

async function countTitles(c: InMemoryRtDbClient, title: string): Promise<number> {
  const q: QueryJson = { table: "tasks", index: "by_title", eq: [title], count: true };
  return (await c.query({ json: q })) as number;
}

function olderThan(field: string, ms: number): FilterExpr {
  return { op: "olderThan", field, ms };
}

/** Asserts `mutate` throws BAD_REQUEST with `message` (the by-query validation
 * chokepoint surfaces as BAD_REQUEST, like every txn-boundary error). */
async function mutateShouldReject(
  c: InMemoryRtDbClient,
  steps: StepJson[],
  message: RegExp,
): Promise<void> {
  try {
    await c.mutate({ steps });
  } catch (err) {
    expect(err).toBeInstanceOf(RtDbError);
    expect((err as RtDbError).code).toBe("BAD_REQUEST");
    expect((err as RtDbError).message).toMatch(message);
    return;
  }
  throw new Error(`expected mutate to reject with /${message.source}/`);
}

/** Push-time validation assertion — SCHEMA_VIOLATION with the server's message
 * (the `validateStructure`/partial-index seams). */
function pushShouldReject(schema: SchemaJson, message: RegExp): void {
  const c = new InMemoryRtDbClient();
  try {
    c.pushSchema(schema);
  } catch (err) {
    expect(err).toBeInstanceOf(RtDbError);
    expect((err as RtDbError).code).toBe("SCHEMA_VIOLATION");
    expect((err as RtDbError).message).toMatch(message);
    return;
  }
  throw new Error(`expected pushSchema to reject with /${message.source}/`);
}

describe("olderThan in by-query steps (execution-time cutoff)", () => {
  it("patchByQuery patches only rows strictly older than now − ms", async () => {
    const c = newClient(numberSchema());
    await seed(c, "old", OLD);
    await seed(c, "future", FUTURE);

    const [res] = await c.mutate({
      steps: [
        {
          op: "patchByQuery",
          table: "tasks",
          filter: olderThan("updatedAt", SWEEP_MS),
          patch: { title: "swept" },
        },
      ],
    });
    expect(res).toEqual({ patched: 1, truncated: false });
    expect(await countTitles(c, "swept")).toBe(1);
    expect(await countTitles(c, "future")).toBe(1);
  });

  it("deleteByQuery deletes only rows strictly older than now − ms", async () => {
    const c = newClient(numberSchema());
    await seed(c, "old", OLD);
    await seed(c, "future", FUTURE);

    const [res] = await c.mutate({
      steps: [{ op: "deleteByQuery", table: "tasks", filter: olderThan("updatedAt", SWEEP_MS) }],
    });
    expect(res).toEqual({ deleted: 1, truncated: false });
    expect(await countTitles(c, "old")).toBe(0);
    expect(await countTitles(c, "future")).toBe(1);
  });

  it("patchByQuery compares the int64 decimal-string wire form numerically", async () => {
    const c = newClient(int64IndexedSchema());
    // int64 wire form is a decimal string, end to end.
    await seed(c, "old", String(OLD));
    await seed(c, "future", String(FUTURE));

    const [res] = await c.mutate({
      steps: [
        {
          op: "patchByQuery",
          table: "tasks",
          filter: olderThan("updatedAt", SWEEP_MS),
          patch: { title: "swept" },
        },
      ],
    });
    expect(res).toEqual({ patched: 1, truncated: false });
    expect(await countTitles(c, "future")).toBe(1);
  });

  it("never matches an absent value and composes inside and/or/not", async () => {
    const c = newClient(optionalNumberSchema());
    await seed(c, "nostamp", undefined);
    await seed(c, "old", OLD);

    // `not olderThan` keeps only the unstamped row (an absent value never
    // matches the inner leaf, so `not` inverts it to a match; the old row
    // matches the leaf and is excluded).
    const [res] = await c.mutate({
      steps: [
        {
          op: "patchByQuery",
          table: "tasks",
          filter: { op: "not", expr: olderThan("updatedAt", SWEEP_MS) },
          patch: { title: "kept" },
        },
      ],
    });
    expect(res).toEqual({ patched: 1, truncated: false });
    expect(await countTitles(c, "kept")).toBe(1);
    // the old row matched the inner leaf, so `not` excluded it — untouched
    expect(await countTitles(c, "old")).toBe(1);
  });

  it("rejects olderThan in a read/query filter (BAD_REQUEST)", async () => {
    const c = newClient(numberSchema());
    await seed(c, "a", OLD);

    const q: QueryJson = { table: "tasks", filter: olderThan("updatedAt", SWEEP_MS) };
    await expect(c.query({ json: q })).rejects.toThrow(
      /only allowed in patchByQuery\/deleteByQuery/,
    );
  });

  it("rejects a negative ms at the by-query validation chokepoint", async () => {
    const c = newClient(numberSchema());
    await mutateShouldReject(
      c,
      [
        {
          op: "patchByQuery",
          table: "tasks",
          filter: olderThan("updatedAt", -1),
          patch: { title: "swept" },
        },
      ],
      /ms must be >= 0/,
    );
  });

  it("rejects a string field and an undeclared field", async () => {
    const schema = numberSchema();
    schema.tables.tasks.fields.updatedAt = { type: "string" };
    const c = newClient(schema);

    await mutateShouldReject(
      c,
      [
        {
          op: "patchByQuery",
          table: "tasks",
          filter: olderThan("updatedAt", SWEEP_MS),
          patch: { title: "swept" },
        },
      ],
      /must be a number or int64 field for olderThan/,
    );
    await mutateShouldReject(
      c,
      [{ op: "deleteByQuery", table: "tasks", filter: olderThan("missing", SWEEP_MS) }],
      /filter references undeclared field 'missing'/,
    );
  });

  it("rejects olderThan in authorize predicates and partial-index where at push", () => {
    const withAuthorize = numberSchema();
    withAuthorize.tables.tasks.authorize = olderThan("updatedAt", SWEEP_MS);
    pushShouldReject(withAuthorize, /only allowed in patchByQuery\/deleteByQuery/);

    // The partial-index seam carries the DDL compile path's own code —
    // BAD_REQUEST, not the authorize arm's SCHEMA_VIOLATION.
    const withWhere = numberSchema();
    withWhere.tables.tasks.indexes = [
      { name: "by_title", fields: ["title"] },
      { name: "by_updatedAt", fields: ["updatedAt"], where: olderThan("updatedAt", SWEEP_MS) },
    ];
    const c = new InMemoryRtDbClient();
    try {
      c.pushSchema(withWhere);
      throw new Error("expected pushSchema to reject");
    } catch (err) {
      expect(err).toBeInstanceOf(RtDbError);
      expect((err as RtDbError).code).toBe("BAD_REQUEST");
      expect((err as RtDbError).message).toMatch(/not allowed in a partial-index predicate/);
    }
  });
});
