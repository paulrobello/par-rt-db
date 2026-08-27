import { describe, expect, it } from "vitest";
import { RtDbError } from "../src/errors.js";
import { InMemoryRtDbClient } from "../src/in_memory/index.js";
import { evalValueExpr } from "../src/in_memory/validate.js";
import type { CaseWhenJson, FieldTypeJson, SchemaJson, ValueExprJson } from "../src/protocol.js";
import { defineSchema, defineTable, t, ve } from "../src/schema.js";

// ENH-028 computed fields — engine-level mirror of the server's
// `server/src/value_expr.rs` interpreter tests and
// `server/tests/computed_test.rs`. The interpreter tests pin the
// "ValueExpr interpreter semantics" table (docs/superpowers/plans/
// 2026-08-21-computed-fields.md); the engine tests pin the write-path
// stamping, push validation, and migrate interplay. The injected clock makes
// `now()` and updatedAt interplay deterministic.

const usersSchema = defineSchema({
  users: defineTable({
    first: t.string(),
    last: t.string(),
    fullName: t.string(),
  })
    .index("by_fullName", ["fullName"])
    .computed("fullName", ve.concat(ve.field("first"), ve.literal(" "), ve.field("last"))),
});

function makeClient(
  schema: SchemaJson,
  startMs = 1_000,
): { c: InMemoryRtDbClient; tick: () => void } {
  let ms = startMs;
  const c = new InMemoryRtDbClient({ now: () => ms, random: () => 0 });
  c.pushSchema(schema);
  return { c, tick: () => (ms += 1_000) };
}

async function insert(c: InMemoryRtDbClient, doc: Record<string, unknown>): Promise<string> {
  const [res] = await c.mutate({ steps: [{ op: "insert", table: "users", doc }] });
  return (res as { id: string }).id;
}

async function getDoc(c: InMemoryRtDbClient, id: string): Promise<Record<string, unknown>> {
  return (await c.query({ json: { table: "users", get: id } })) as Record<string, unknown>;
}

/** Push-time validation assertions — BAD_REQUEST, the code the server's
 * `validate_computed` throws (message shapes are engine-local). */
function pushShouldReject(schema: SchemaJson, message: RegExp): void {
  const c = new InMemoryRtDbClient();
  try {
    c.pushSchema(schema);
  } catch (err) {
    expect(err).toBeInstanceOf(RtDbError);
    expect((err as RtDbError).code).toBe("BAD_REQUEST");
    expect((err as RtDbError).message).toMatch(message);
    return;
  }
  throw new Error(`expected pushSchema to reject with /${message.source}/`);
}

/** evalValueExpr with a fixed clock (now matters only in the explicit `now` test). */
function evalExpr(expr: ValueExprJson, doc: Record<string, unknown>): unknown {
  return evalValueExpr(expr, doc, 0);
}

/** One `case` branch. `then` is the server's serde wire key for a CaseWhen —
 * a value expression, never a thenable. */
function whenThen(when: CaseWhenJson["when"], then: ValueExprJson): CaseWhenJson {
  return { when, then };
}

describe("evalValueExpr — ValueExpr interpreter semantics", () => {
  const doc: Record<string, unknown> = {
    s: "x",
    n: 42,
    f: 42.5,
    b: true,
    o: { a: 1 },
    nil: null,
  };

  it("field reads are text extraction and absent/null are null", () => {
    expect(evalExpr(ve.field("s"), doc)).toBe("x");
    expect(evalExpr(ve.field("n"), doc)).toBe("42");
    expect(evalExpr(ve.field("f"), doc)).toBe("42.5");
    expect(evalExpr(ve.field("b"), doc)).toBe("true");
    // Objects use COMPACT JSON text — the pinned five-implementation
    // convention, deliberately not Postgres's spaced jsonb text.
    expect(evalExpr(ve.field("o"), doc)).toBe('{"a":1}');
    expect(evalExpr(ve.field("nil"), doc)).toBeNull();
    expect(evalExpr(ve.field("missing"), doc)).toBeNull();
  });

  it("literal passes through", () => {
    for (const v of ["s", 42, 42.5, true, { a: [1, 2] }, null]) {
      expect(evalExpr(ve.literal(v), {})).toEqual(v);
    }
  });

  it("concat skips nulls and casts numbers to text", () => {
    expect(
      evalExpr(ve.concat(ve.field("first"), ve.field("missing"), ve.field("n")), {
        first: "Ada",
        n: 42,
      }),
    ).toBe("Ada42");
  });

  it("concat of only null parts is the empty string", () => {
    expect(evalExpr(ve.concat(ve.field("missing"), ve.literal(null)), {})).toBe("");
  });

  it("add coerces string fields to numeric", () => {
    expect(evalExpr(ve.add(ve.field("a"), ve.field("b")), { a: "42", b: "1" })).toBe(43);
  });

  it("arithmetic propagates null over both operands, before the zero check", () => {
    const one = ve.literal(1);
    const missing = ve.field("missing");
    expect(evalExpr(ve.add(missing, one), {})).toBeNull();
    expect(evalExpr(ve.sub(one, missing), {})).toBeNull();
    expect(evalExpr(ve.mul(missing, one), {})).toBeNull();
    expect(evalExpr(ve.div(one, missing), {})).toBeNull();
    // null / 0 is null, not an error — propagation precedes the zero check.
    expect(evalExpr(ve.div(missing, ve.literal(0)), {})).toBeNull();
  });

  it("div by zero errors (both zero spellings)", () => {
    for (const zero of [0, -0]) {
      expect(() => evalExpr(ve.div(ve.literal(1), ve.literal(zero)), {})).toThrowError(RtDbError);
    }
    try {
      evalExpr(ve.div(ve.literal(1), ve.literal(0)), {});
      expect.unreachable("div by zero must throw");
    } catch (err) {
      expect((err as RtDbError).message).toBe("division by zero");
    }
  });

  it("div non-finite result errors", () => {
    try {
      evalExpr(ve.div(ve.literal(1e308), ve.literal(1e-10)), {});
      expect.unreachable("non-finite div must throw");
    } catch (err) {
      expect((err as RtDbError).message).toBe("numeric result is not finite");
    }
  });

  it("coalesce returns the first non-null else null", () => {
    expect(evalExpr(ve.coalesce(ve.field("missing"), ve.literal(7)), {})).toBe(7);
    expect(evalExpr(ve.coalesce(ve.field("a"), ve.field("b")), {})).toBeNull();
  });

  it("lower/upper/trim — trim strips spaces only", () => {
    const d = { mixed: "MiXeD", padded: "  x  ", tabbed: "  \tx  " };
    expect(evalExpr(ve.lower(ve.field("mixed")), d)).toBe("mixed");
    expect(evalExpr(ve.upper(ve.field("mixed")), d)).toBe("MIXED");
    expect(evalExpr(ve.trim(ve.field("padded")), d)).toBe("x");
    // Spaces only — the tab survives btrim's default.
    expect(evalExpr(ve.trim(ve.field("tabbed")), d)).toBe("\tx");
    expect(evalExpr(ve.lower(ve.field("missing")), d)).toBeNull();
  });

  it("cast toString uses text extraction", () => {
    expect(evalExpr(ve.cast(ve.field("n"), "toString"), doc)).toBe("42");
    expect(evalExpr(ve.cast(ve.field("o"), "toString"), doc)).toBe('{"a":1}');
    expect(evalExpr(ve.cast(ve.field("missing"), "toString"), doc)).toBeNull();
  });

  it("cast toNumber parses trimmed strings and rejects bad input", () => {
    expect(evalExpr(ve.cast(ve.field("s"), "toNumber"), { s: "  3.5 " })).toBe(3.5);
    expect(() => evalExpr(ve.cast(ve.field("s"), "toNumber"), { s: "abc" })).toThrowError(
      RtDbError,
    );
    // A bool FIELD reaches the cast as its text form ("true"), so it fails the
    // string parse; a bool LITERAL hits the type-error arm directly.
    expect(() => evalExpr(ve.cast(ve.field("b"), "toNumber"), { b: true })).toThrowError(RtDbError);
    try {
      evalExpr(ve.cast(ve.literal(true), "toNumber"), {});
      expect.unreachable("bool literal toNumber must throw");
    } catch (err) {
      expect((err as RtDbError).message).toBe("cannot cast to number");
    }
    expect(evalExpr(ve.cast(ve.field("missing"), "toNumber"), doc)).toBeNull();
  });

  it("cast toInt64 requires integral in-range numbers", () => {
    expect(evalExpr(ve.cast(ve.field("i"), "toInt64"), { i: 42 })).toBe(42);
    expect(evalExpr(ve.cast(ve.field("s"), "toInt64"), { s: "  7 " })).toBe(7);
    expect(() => evalExpr(ve.cast(ve.field("f"), "toInt64"), { f: 3.5 })).toThrowError(RtDbError);
    expect(() => evalExpr(ve.cast(ve.field("s"), "toInt64"), { s: "8x" })).toThrowError(RtDbError);
    expect(() => evalExpr(ve.cast(ve.literal(true), "toInt64"), {})).toThrowError(RtDbError);
    expect(evalExpr(ve.cast(ve.field("missing"), "toInt64"), doc)).toBeNull();
  });

  it("cast toBoolean accepts the Postgres literal word set", () => {
    expect(evalExpr(ve.cast(ve.literal(true), "toBoolean"), {})).toBe(true);
    expect(evalExpr(ve.cast(ve.literal(1), "toBoolean"), {})).toBe(true);
    expect(evalExpr(ve.cast(ve.literal(0), "toBoolean"), {})).toBe(false);
    const words: Array<[string, boolean]> = [
      ["TRUE", true],
      ["t", true],
      ["Yes", true],
      ["on", true],
      ["1", true],
      ["False", false],
      ["f", false],
      ["No", false],
      ["OFF", false],
      ["0", false],
    ];
    for (const [word, want] of words) {
      expect(evalExpr(ve.cast(ve.literal(word), "toBoolean"), {})).toBe(want);
    }
    expect(() => evalExpr(ve.cast(ve.literal("maybe"), "toBoolean"), {})).toThrowError(RtDbError);
    expect(() => evalExpr(ve.cast(ve.literal(2), "toBoolean"), {})).toThrowError(RtDbError);
    expect(evalExpr(ve.cast(ve.field("missing"), "toBoolean"), doc)).toBeNull();
  });

  it("now yields the epoch-ms argument as a JSON number", () => {
    expect(evalValueExpr(ve.now(), {}, 1_234_567_890)).toBe(1_234_567_890);
  });

  it("case takes the first matching when, else otherwise", () => {
    const matched = ve.case(
      [
        whenThen({ op: "eq", field: "status", value: "user" }, ve.literal(1)),
        whenThen({ op: "eq", field: "status", value: "admin" }, ve.literal(2)),
      ],
      ve.literal(4),
    );
    expect(evalExpr(matched, { status: "admin" })).toBe(2);
    const otherwise = ve.case(
      [whenThen({ op: "gt", field: "n", value: 10 }, ve.literal(3))],
      ve.field("status"),
    );
    expect(evalExpr(otherwise, { status: "admin", n: 5 })).toBe("admin");
  });
});

describe("computed DSL — wire shape", () => {
  it("emits computed on the wire when declared", () => {
    expect(usersSchema.toJSON()).toEqual({
      tables: {
        users: {
          fields: {
            first: { type: "string" },
            last: { type: "string" },
            fullName: { type: "string" },
          },
          indexes: [{ name: "by_fullName", fields: ["fullName"] }],
          computed: {
            fullName: {
              op: "concat",
              parts: [
                { op: "field", field: "first" },
                { op: "literal", value: " " },
                { op: "field", field: "last" },
              ],
            },
          },
        },
      },
    });
  });

  it("omits computed on the wire when the table declares none", () => {
    const s = defineSchema({ things: defineTable({ n: t.number() }) });
    expect(s.toJSON().tables.things).toEqual({ fields: { n: { type: "number" } } });
    expect(s.toJSON().tables.things).not.toHaveProperty("computed");
  });

  it("keeps earlier builder instances intact when .computed() is chained", () => {
    // The immutable-builder convention: a TableDefinition instance is never
    // mutated by a later chain call.
    const base = defineTable({ first: t.string(), slug: t.string() });
    const withComputed = base.computed("slug", ve.lower(ve.trim(ve.field("first"))));
    expect(base.toJSON()).not.toHaveProperty("computed");
    expect(withComputed.toJSON().computed).toEqual({
      slug: { op: "lower", value: { op: "trim", value: { op: "field", field: "first" } } },
    });
  });
});

describe("InMemoryRtDbClient — computed-field stamping", () => {
  it("insert overwrites a client-supplied value", async () => {
    const { c } = makeClient(usersSchema.toJSON());
    const id = await insert(c, { first: "Ada", last: "Lovelace", fullName: "WRONG" });
    expect(await getDoc(c, id)).toMatchObject({ fullName: "Ada Lovelace", first: "Ada" });
  });

  it("insert drops a wrong-typed client-supplied value before validation", async () => {
    // The stamp overwrites it before validateDoc, so a garbage payload cannot
    // fail the write — the server's do_insert contract.
    const { c } = makeClient(usersSchema.toJSON());
    const id = await insert(c, {
      first: "Ada",
      last: "Lovelace",
      fullName: { nested: true },
    } as Record<string, unknown>);
    expect(await getDoc(c, id)).toMatchObject({ fullName: "Ada Lovelace" });
  });

  it("patch recomputes from the merged doc", async () => {
    const { c } = makeClient(usersSchema.toJSON());
    const id = await insert(c, { first: "Gracie", last: "Hopper" });
    await c.mutate({
      steps: [{ op: "patch", table: "users", id, fields: { first: "Grace" } }],
    });
    expect(await getDoc(c, id)).toMatchObject({ fullName: "Grace Hopper" });
  });

  it("patch drops a client-supplied computed value (skip+drop)", async () => {
    const { c } = makeClient(usersSchema.toJSON());
    const id = await insert(c, { first: "Ada", last: "Lovelace" });
    await c.mutate({
      steps: [
        {
          op: "patch",
          table: "users",
          id,
          fields: { fullName: { nope: 1 } } as unknown as Record<string, unknown>,
        },
      ],
    });
    // The patch's computed key was dropped pre-merge and the post-merge stamp
    // re-derived the unchanged value — no SCHEMA_VIOLATION escaped.
    expect(await getDoc(c, id)).toMatchObject({ fullName: "Ada Lovelace" });
  });

  it("replace recomputes", async () => {
    const { c } = makeClient(usersSchema.toJSON());
    const id = await insert(c, { first: "Ada", last: "Lovelace" });
    await c.mutate({
      steps: [{ op: "replace", table: "users", id, doc: { first: "Alan", last: "Turing" } }],
    });
    expect(await getDoc(c, id)).toMatchObject({ fullName: "Alan Turing" });
  });

  it("upsert recomputes on both branches", async () => {
    const { c } = makeClient(usersSchema.toJSON());
    const [first] = await c.mutate({
      steps: [
        {
          op: "upsert",
          table: "users",
          index: "by_fullName",
          eq: ["Ada Lovelace"],
          insert: { first: "Ada", last: "Lovelace", fullName: "WRONG" },
          patch: { first: "Ada" },
        },
      ],
    });
    expect(first).toEqual({ id: expect.any(String), inserted: true });
    const id = (first as { id: string }).id;
    expect(await getDoc(c, id)).toMatchObject({ fullName: "Ada Lovelace" });

    const [second] = await c.mutate({
      steps: [
        {
          op: "upsert",
          table: "users",
          index: "by_fullName",
          eq: ["Ada Lovelace"],
          insert: { first: "Ada", last: "Lovelace" },
          patch: { last: "L." },
        },
      ],
    });
    expect(second).toEqual({ id, inserted: false });
    expect(await getDoc(c, id)).toMatchObject({ fullName: "Ada L." });
  });

  it("serves order over the indexed computed field", async () => {
    const { c } = makeClient(usersSchema.toJSON());
    await insert(c, { first: "Ada", last: "Lovelace" });
    await insert(c, { first: "Alan", last: "Turing" });
    await insert(c, { first: "Grace", last: "Hopper" });
    const rows = (await c.query({
      json: { table: "users", index: "by_fullName", order: "desc" },
    })) as Record<string, unknown>[];
    expect(rows.map((r) => r.fullName)).toEqual(["Grace Hopper", "Alan Turing", "Ada Lovelace"]);
  });

  it("patchByQuery recomputes matched rows", async () => {
    const { c } = makeClient(usersSchema.toJSON());
    const ada = await insert(c, { first: "Ada", last: "Lovelace" });
    const alan = await insert(c, { first: "Alan", last: "Turing" });
    const [res] = await c.mutate({
      steps: [
        {
          op: "patchByQuery",
          table: "users",
          filter: { op: "eq", field: "first", value: "Ada" },
          patch: { first: "Ada2" },
        },
      ],
    });
    expect(res).toEqual({ patched: 1, truncated: false });
    expect(await getDoc(c, ada)).toMatchObject({ fullName: "Ada2 Lovelace" });
    expect(await getDoc(c, alan)).toMatchObject({ fullName: "Alan Turing" });
  });

  it("an optional computed field with a null result stores no key", async () => {
    const schema = defineSchema({
      users: defineTable({
        name: t.string(),
        nickname: t.optional(t.string()),
        nick: t.optional(t.string()),
      })
        .index("by_name", ["name"])
        .computed("nick", ve.coalesce(ve.field("nickname"))),
    });
    const { c } = makeClient(schema.toJSON());
    const ada = await insert(c, { name: "Ada", nickname: "Ace" });
    expect(await getDoc(c, ada)).toMatchObject({ nick: "Ace" });

    // Nulling the input strips the optional key AND the recomputed null
    // removes the computed key.
    await c.mutate({
      steps: [{ op: "patch", table: "users", id: ada, fields: { nickname: null } }],
    });
    const doc = await getDoc(c, ada);
    expect(doc).not.toHaveProperty("nickname");
    expect(doc).not.toHaveProperty("nick");
  });

  it("a required computed field with a null result fails the write", async () => {
    // Null removes the key; a REQUIRED field then fails validateDoc
    // ("is required") — the server's validate_doc backstop.
    const schema = defineSchema({
      users: defineTable({
        first: t.optional(t.string()),
        slug: t.string(),
      }).computed("slug", ve.field("first")),
    });
    const { c } = makeClient(schema.toJSON());
    await expect(
      c.mutate({ steps: [{ op: "insert", table: "users", doc: {} }] }),
    ).rejects.toThrowError(/required/);
  });

  it("an evaluation error fails the write as BAD_REQUEST naming the field, doc unchanged", async () => {
    const schema = defineSchema({
      metrics: defineTable({
        num: t.number(),
        denom: t.number(),
        ratio: t.number(),
      })
        .index("by_num", ["num"])
        .computed("ratio", ve.div(ve.field("num"), ve.field("denom"))),
    });
    const { c } = makeClient(schema.toJSON());
    const [row] = (await c.mutate({
      steps: [{ op: "insert", table: "metrics", doc: { num: 6, denom: 3 } }],
    })) as Array<{ id: string }>;
    expect(await c.query({ json: { table: "metrics", get: row.id } })).toMatchObject({ ratio: 2 });

    try {
      await c.mutate({
        steps: [{ op: "patch", table: "metrics", id: row.id, fields: { denom: 0 } }],
      });
      expect.unreachable("div-by-zero patch must fail");
    } catch (err) {
      expect(err).toBeInstanceOf(RtDbError);
      expect((err as RtDbError).code).toBe("BAD_REQUEST");
      expect((err as RtDbError).message).toContain("ratio");
    }
    // The failed write left the doc unchanged.
    expect(await c.query({ json: { table: "metrics", get: row.id } })).toMatchObject({
      denom: 3,
      ratio: 2,
    });
  });

  it("cascade setNull recomputes the child's computed fields", async () => {
    const schema = defineSchema({
      parents: defineTable({ name: t.string() }).index("by_name", ["name"]),
      children: defineTable({
        parentId: t.optional(t.id("parents", { onDelete: "setNull" })),
        label: t.string(),
        parentNote: t.optional(t.string()),
      })
        .index("by_parentId", ["parentId"])
        .computed("parentNote", ve.coalesce(ve.field("parentId"))),
    });
    const { c } = makeClient(schema.toJSON());
    const [p] = (await c.mutate({
      steps: [{ op: "insert", table: "parents", doc: { name: "P" } }],
    })) as Array<{ id: string }>;
    const [child] = (await c.mutate({
      steps: [{ op: "insert", table: "children", doc: { parentId: p.id, label: "C" } }],
    })) as Array<{ id: string }>;
    const before = (await c.query({ json: { table: "children", get: child.id } })) as Record<
      string,
      unknown
    >;
    expect(before.parentNote).toBe(p.id);

    await c.mutate({ steps: [{ op: "delete", table: "parents", id: p.id }] });
    const after = (await c.query({ json: { table: "children", get: child.id } })) as Record<
      string,
      unknown
    >;
    // The ref was setNull'd and the recomputed null REMOVED the computed key.
    expect(after).not.toHaveProperty("parentId");
    expect(after).not.toHaveProperty("parentNote");
  });

  it("a computed expr over the updatedAtField sees the fresh stamp", async () => {
    const schema = defineSchema({
      tasks: defineTable({
        title: t.string(),
        touchedAt: t.number(),
        summary: t.string(),
      })
        .index("by_title", ["title"])
        .updatedAtField("touchedAt")
        .computed("summary", ve.concat(ve.field("title"), ve.literal("@"), ve.field("touchedAt"))),
    });
    const { c, tick } = makeClient(schema.toJSON());
    const [task] = (await c.mutate({
      steps: [{ op: "insert", table: "tasks", doc: { title: "A" } }],
    })) as Array<{ id: string }>;
    expect(await c.query({ json: { table: "tasks", get: task.id } })).toMatchObject({
      summary: "A@1000",
    });
    tick();
    await c.mutate({
      steps: [{ op: "patch", table: "tasks", id: task.id, fields: { title: "B" } }],
    });
    expect(await c.query({ json: { table: "tasks", get: task.id } })).toMatchObject({
      summary: "B@2000",
    });
  });

  it("now() stamps the write-time epoch-ms", async () => {
    const schema = defineSchema({
      events: defineTable({ name: t.string(), seenAt: t.number() })
        .index("by_name", ["name"])
        .computed("seenAt", ve.now()),
    });
    const { c, tick } = makeClient(schema.toJSON());
    const [row] = (await c.mutate({
      steps: [{ op: "insert", table: "events", doc: { name: "e" } }],
    })) as Array<{ id: string }>;
    expect(await c.query({ json: { table: "events", get: row.id } })).toMatchObject({
      seenAt: 1_000,
    });
    tick();
    await c.mutate({
      steps: [{ op: "patch", table: "events", id: row.id, fields: { name: "e2" } }],
    });
    expect(await c.query({ json: { table: "events", get: row.id } })).toMatchObject({
      seenAt: 2_000,
    });
  });
});

describe("computed push validation (ENH-028)", () => {
  const baseFields: Record<string, FieldTypeJson> = {
    first: { type: "string" },
    last: { type: "string" },
    fullName: { type: "string" },
  };
  const concatExpr: ValueExprJson = {
    op: "concat",
    parts: [
      { op: "field", field: "first" },
      { op: "literal", value: " " },
      { op: "field", field: "last" },
    ],
  };

  it("rejects a computed key that is not a declared field", () => {
    pushShouldReject(
      {
        tables: {
          users: { fields: baseFields, computed: { nickname: { op: "field", field: "first" } } },
        },
      },
      /computed field 'users.nickname' is not a declared field/,
    );
  });

  it("rejects computed on ownerField, collaboratorsField, and autoIncrementField", () => {
    pushShouldReject(
      {
        tables: {
          users: {
            fields: { ...baseFields, ownerId: { type: "string" } },
            ownerField: "ownerId",
            computed: { ownerId: { op: "field", field: "first" } },
          },
        },
      },
      /must not be the table's ownerField/,
    );
    pushShouldReject(
      {
        tables: {
          users: {
            fields: { ...baseFields, memberIds: { type: "array", element: { type: "string" } } },
            collaboratorsField: "memberIds",
            computed: { memberIds: { op: "field", field: "first" } },
          },
        },
      },
      /must not be the table's collaboratorsField/,
    );
    pushShouldReject(
      {
        tables: {
          users: {
            fields: { ...baseFields, num: { type: "int64" } },
            autoIncrementField: "num",
            computed: { num: ve.cast(ve.literal("1"), "toString") },
          },
        },
      },
      /must not be the table's autoIncrementField/,
    );
  });

  it("rejects a reference to an undeclared field", () => {
    pushShouldReject(
      {
        tables: {
          users: {
            fields: baseFields,
            computed: {
              fullName: {
                op: "concat",
                parts: [
                  { op: "field", field: "first" },
                  { op: "field", field: "middle" },
                ],
              },
            },
          },
        },
      },
      /references undeclared field 'middle'/,
    );
  });

  it("rejects a reference to another computed field", () => {
    pushShouldReject(
      {
        tables: {
          users: {
            fields: { ...baseFields, shout: { type: "string" } },
            computed: {
              fullName: concatExpr,
              shout: { op: "upper", value: { op: "field", field: "fullName" } },
            },
          },
        },
      },
      /references computed field 'fullName' \(computed fields may not reference each other\)/,
    );
  });

  it("rejects an unknown value-expr op at push time (the walker's trailing throw)", () => {
    pushShouldReject(
      {
        tables: {
          users: {
            fields: baseFields,
            computed: {
              fullName: { op: "frobnicate", field: "first" } as unknown as ValueExprJson,
            },
          },
        },
      },
      /unknown value expr op 'frobnicate'/,
    );
    // An unknown op inside a Case.when filter hits the FilterExpr walker's
    // trailing throw — the server's deny_unknown_fields rejects both at
    // deserialize; the engine rejects them at push.
    pushShouldReject(
      {
        tables: {
          users: {
            fields: { ...baseFields, role: { type: "string" } },
            computed: {
              fullName: {
                op: "case",
                whens: [
                  whenThen({ op: "frobnicate", field: "role" } as unknown as CaseWhenJson["when"], {
                    op: "literal",
                    value: "x",
                  }),
                ],
                otherwise: { op: "literal", value: "y" },
              },
            },
          },
        },
      },
      /unknown filter expr op 'frobnicate'/,
    );
  });

  it("rejects prototype-named computed keys and references (Object.hasOwn, BTreeMap semantics)", () => {
    // "toString" is on Object.prototype, not in the table's fields — a
    // prototype-named computed key must not pass as declared, nor must a
    // prototype-named reference pass the declared check (the server's
    // BTreeMap lookups never see inherited keys).
    pushShouldReject(
      {
        tables: {
          users: {
            fields: baseFields,
            computed: { toString: { op: "field", field: "first" } as ValueExprJson },
          },
        },
      },
      /computed field 'users.toString' is not a declared field/,
    );
    pushShouldReject(
      {
        tables: {
          users: {
            fields: baseFields,
            computed: { fullName: { op: "field", field: "toString" } },
          },
        },
      },
      /references undeclared field 'toString'/,
    );
  });

  it("accepts a computed reference and authorize predicate over a DECLARED prototype-named field", () => {
    // "toString" here IS a declared field — the computed-reference and
    // authorize checks must use own-property lookups (Object.hasOwn), or the
    // Object.prototype member falsely trips "references computed field" /
    // "must not be referenced by authorize" (the server's BTreeMap lookups
    // never see inherited keys).
    expect(() =>
      makeClient({
        tables: {
          users: {
            fields: { ...baseFields, toString: { type: "string" } as FieldTypeJson },
            computed: { fullName: { op: "field", field: "toString" } },
          },
        },
      }),
    ).not.toThrow();
    expect(() =>
      makeClient({
        tables: {
          users: {
            fields: { ...baseFields, toString: { type: "string" } as FieldTypeJson },
            authorize: { op: "eq", field: "toString", value: "x" },
            computed: { fullName: concatExpr },
          },
        },
      }),
    ).not.toThrow();
  });

  it("rejects a principal marker inside a Case.when", () => {
    pushShouldReject(
      {
        tables: {
          users: {
            fields: { ...baseFields, role: { type: "string" } },
            computed: {
              fullName: {
                op: "case",
                whens: [
                  whenThen(
                    { op: "eq", field: "role", value: { $user: true } },
                    { op: "field", field: "first" },
                  ),
                ],
                otherwise: { op: "field", field: "last" },
              },
            },
          },
        },
      },
      /filter value must be a string, number, or boolean/,
    );
  });

  it("rejects static-kind mismatches", () => {
    // concat (string kind) into a number field
    pushShouldReject(
      {
        tables: {
          metrics: {
            fields: { denom: { type: "number" }, ratio: { type: "number" } },
            computed: {
              ratio: { op: "concat", parts: [{ op: "field", field: "denom" }] },
            },
          },
        },
      },
      /produces a string, which the field type does not accept/,
    );
    // arithmetic (number kind) into an int64 field — int64's wire form is a
    // decimal string, so a Number-kind result can never validate
    pushShouldReject(
      {
        tables: {
          metrics: {
            fields: { a: { type: "int64" }, b: { type: "int64" }, total: { type: "int64" } },
            computed: {
              total: {
                op: "add",
                left: { op: "field", field: "a" },
                right: { op: "field", field: "b" },
              },
            },
          },
        },
      },
      /produces a number, which the field type does not accept/,
    );
    // lower (string kind) into a boolean field
    pushShouldReject(
      {
        tables: {
          users: {
            fields: { name: { type: "string" }, shouty: { type: "boolean" } },
            computed: { shouty: { op: "lower", value: { op: "field", field: "name" } } },
          },
        },
      },
      /produces a string, which the field type does not accept/,
    );
  });

  it("accepts the canonical shapes, including Cast(toString) into int64", () => {
    // A fresh client per push — a second pushSchema on one client is an
    // additive push over the first (removing the first schema's tables is
    // destructive), not a clean-slate validation.
    const pushOk = (schema: SchemaJson): void => {
      expect(() => new InMemoryRtDbClient().pushSchema(schema)).not.toThrow();
    };
    // concat on a string field
    pushOk({
      tables: { users: { fields: baseFields, computed: { fullName: concatExpr } } },
    });
    // lower/trim on an optional string
    pushOk({
      tables: {
        posts: {
          fields: {
            title: { type: "string" },
            slug: { type: "optional", inner: { type: "string" } },
          },
          computed: { slug: { op: "lower", value: { op: "field", field: "title" } } },
        },
      },
    });
    // arithmetic and Now on number fields
    pushOk({
      tables: {
        metrics: {
          fields: {
            a: { type: "number" },
            b: { type: "number" },
            sum: { type: "number" },
            at: { type: "number" },
          },
          computed: {
            sum: {
              op: "add",
              left: { op: "field", field: "a" },
              right: { op: "field", field: "b" },
            },
            at: { op: "now" },
          },
        },
      },
    });
    // Case on a union field (untyped — runtime validateDoc guards)
    pushOk({
      tables: {
        events: {
          fields: {
            kind: { type: "string" },
            label: { type: "union", variants: [{ type: "string" }, { type: "number" }] },
          },
          computed: {
            label: {
              op: "case",
              whens: [
                whenThen({ op: "eq", field: "kind", value: "x" }, { op: "literal", value: 1 }),
              ],
              otherwise: { op: "literal", value: "other" },
            },
          },
        },
      },
    });
    // Cast(toString) into int64 — a String kind fits the decimal-string wire form
    pushOk({
      tables: {
        counters: {
          fields: { n: { type: "number" }, stamp: { type: "int64" } },
          computed: { stamp: { op: "cast", value: { op: "now" }, to: "toString" } },
        },
      },
    });
  });

  it("rejects an authorize predicate that references a computed field", () => {
    pushShouldReject(
      {
        tables: {
          users: {
            fields: { ...baseFields, ownerId: { type: "string" } },
            authorize: { op: "eq", field: "fullName", value: "Ada Lovelace" },
            computed: { fullName: concatExpr },
          },
        },
      },
      /must not be referenced by the table's authorize predicate/,
    );
  });
});

describe("computed migrate interplay (ENH-028)", () => {
  it("renameField rewrites Field refs; dryRun persists nothing", async () => {
    const { c } = makeClient(usersSchema.toJSON());
    const id = await insert(c, { first: "Ada", last: "Lovelace" });
    const result = c.migrate({
      directives: [{ op: "renameField", table: "users", from: "first", to: "givenName" }],
      dryRun: true,
    });
    // The derived schema's expr reads the renamed field.
    expect(result.applied).toBe(false);
    expect(result.schema.tables.users?.computed?.fullName).toEqual({
      op: "concat",
      parts: [
        { op: "field", field: "givenName" },
        { op: "literal", value: " " },
        { op: "field", field: "last" },
      ],
    });
    // dryRun persisted nothing — a write through the ORIGINAL schema (whose
    // field is still `first`) still validates and recomputes.
    await c.mutate({
      steps: [{ op: "patch", table: "users", id, fields: { first: "Grace" } }],
    });
    expect(await getDoc(c, id)).toMatchObject({ first: "Grace", fullName: "Grace Lovelace" });
  });

  it("renaming the computed field itself moves the keyed entry", () => {
    const { c } = makeClient(usersSchema.toJSON());
    const result = c.migrate({
      directives: [{ op: "renameField", table: "users", from: "fullName", to: "sortName" }],
      dryRun: true,
    });
    const users = result.schema.tables.users;
    expect(users?.computed).not.toHaveProperty("fullName");
    expect(users?.computed?.sortName).toEqual({
      op: "concat",
      parts: [
        { op: "field", field: "first" },
        { op: "literal", value: " " },
        { op: "field", field: "last" },
      ],
    });
  });

  it("renameField rewrites Case.when filter refs too", () => {
    const caseSchema = defineSchema({
      users: defineTable({
        first: t.string(),
        status: t.string(),
        label: t.string(),
      }).computed(
        "label",
        ve.case(
          [whenThen({ op: "eq", field: "status", value: "admin" }, ve.field("first"))],
          ve.literal("?"),
        ),
      ),
    });
    const { c } = makeClient(caseSchema.toJSON());
    const result = c.migrate({
      directives: [
        { op: "renameField", table: "users", from: "status", to: "role" },
        { op: "renameField", table: "users", from: "first", to: "givenName" },
      ],
      dryRun: true,
    });
    expect(result.schema.tables.users?.computed?.label).toEqual({
      op: "case",
      whens: [
        whenThen({ op: "eq", field: "role", value: "admin" }, { op: "field", field: "givenName" }),
      ],
      otherwise: { op: "literal", value: "?" },
    });
  });

  it("a post-migrate patch recomputes from the renamed field", async () => {
    const { c } = makeClient(usersSchema.toJSON());
    const id = await insert(c, { first: "Ada", last: "Lovelace" });
    c.migrate({
      directives: [{ op: "renameField", table: "users", from: "first", to: "givenName" }],
    });
    await c.mutate({
      steps: [{ op: "patch", table: "users", id, fields: { givenName: "Grace" } }],
    });
    expect(await getDoc(c, id)).toMatchObject({ fullName: "Grace Lovelace" });
  });

  it("dropField on a referenced field is rejected, naming the computed field", () => {
    const { c } = makeClient(usersSchema.toJSON());
    try {
      c.migrate({ directives: [{ op: "dropField", table: "users", field: "first" }] });
      expect.unreachable("dropField on a referenced field must fail");
    } catch (err) {
      expect(err).toBeInstanceOf(RtDbError);
      expect((err as RtDbError).code).toBe("BAD_REQUEST");
      expect((err as RtDbError).message).toContain("fullName");
    }
  });

  it("dropping an unrelated field works; dropping the computed field removes its entry", () => {
    const wide = defineSchema({
      users: defineTable({
        first: t.string(),
        last: t.string(),
        nick: t.optional(t.string()),
        fullName: t.string(),
      })
        .index("by_fullName", ["fullName"])
        .computed("fullName", ve.concat(ve.field("first"), ve.literal(" "), ve.field("last"))),
    });
    const { c } = makeClient(wide.toJSON());
    const dropped = c.migrate({
      directives: [{ op: "dropField", table: "users", field: "nick" }],
    });
    expect(dropped.schema.tables.users?.fields).not.toHaveProperty("nick");
    expect(dropped.schema.tables.users?.computed).toHaveProperty("fullName");

    const droppedComputed = c.migrate({
      directives: [{ op: "dropField", table: "users", field: "fullName" }],
    });
    expect(droppedComputed.schema.tables.users?.computed).toBeUndefined();
  });

  it("changeType of a computed field re-validates via the derived schema", () => {
    const { c } = makeClient(usersSchema.toJSON());
    try {
      c.migrate({
        directives: [
          {
            op: "changeType",
            table: "users",
            field: "fullName",
            to: { type: "number" },
            cast: "toNumber",
          },
        ],
      });
      expect.unreachable("changeType to a kind the expr cannot produce must fail");
    } catch (err) {
      expect(err).toBeInstanceOf(RtDbError);
      expect((err as RtDbError).code).toBe("BAD_REQUEST");
      expect((err as RtDbError).message).toMatch(/produces a string/);
    }
  });
});
