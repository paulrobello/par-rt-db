import { describe, expect, it } from "vitest";
import { Migration } from "../src/migration.js";

describe("Migration builder", () => {
  it("builds every directive with the exact wire shape (op tag, camelCase, cast casing)", () => {
    const req = new Migration()
      .renameField("users", "name", "fullName")
      .renameTable("old", "new")
      .changeType("users", "age", { type: "string" }, "toString")
      .dropField("users", "legacy")
      .dropTable("gone")
      .dropIndex("users", "by_email")
      .setDefault("users", "role", "member")
      .evalExpr("users", "upper", "upper(doc->>'name')", "doc ? 'name'")
      .dryRun()
      .build();

    expect(req).toEqual({
      directives: [
        { op: "renameField", table: "users", from: "name", to: "fullName" },
        { op: "renameTable", from: "old", to: "new" },
        {
          op: "changeType",
          table: "users",
          field: "age",
          to: { type: "string" },
          cast: "toString",
        },
        { op: "dropField", table: "users", field: "legacy" },
        { op: "dropTable", name: "gone" },
        { op: "dropIndex", table: "users", name: "by_email" },
        { op: "setDefault", table: "users", field: "role", value: "member" },
        {
          op: "evalExpr",
          table: "users",
          set: "upper",
          expr: "upper(doc->>'name')",
          where: "doc ? 'name'",
        },
      ],
      dryRun: true,
    });
  });

  it("preserves directive order across chained calls", () => {
    const { directives } = new Migration()
      .dropField("t", "a")
      .renameField("t", "b", "c")
      .setDefault("t", "d", 1)
      .build();
    expect(directives.map((d) => d.op)).toEqual(["dropField", "renameField", "setDefault"]);
  });

  it("aliases evalExpr's where_clause as `where` on the wire", () => {
    const [d] = new Migration().evalExpr("t", "f", "expr", "cond").build().directives;
    expect(d).toMatchObject({ op: "evalExpr", where: "cond" });
    expect(d).not.toHaveProperty("whereClause");
  });

  it("omits evalExpr.where when no where clause is given", () => {
    const [d] = new Migration().evalExpr("t", "f", "expr").build().directives;
    expect(d).not.toHaveProperty("where");
  });

  it("includes changeType.default only when supplied", () => {
    const withDefault = new Migration()
      .changeType("t", "f", { type: "number" }, "toNumber", 0)
      .build().directives[0];
    expect(withDefault).toMatchObject({ default: 0 });
    const withoutDefault = new Migration()
      .changeType("t", "f", { type: "number" }, "toNumber")
      .build().directives[0];
    expect(withoutDefault).not.toHaveProperty("default");
  });

  it("emits the camelCase cast literals verbatim", () => {
    const casts = ["toString", "toNumber", "toInt64", "toBoolean"] as const;
    for (const cast of casts) {
      const [d] = new Migration().changeType("t", "f", { type: "string" }, cast).build()
        .directives;
      expect(d).toMatchObject({ cast });
    }
  });

  it("sets dryRun false until .dryRun() flips it", () => {
    expect(new Migration().build().dryRun).toBe(false);
    expect(new Migration().dryRun().build().dryRun).toBe(true);
  });

  it("builds an empty directive list when nothing is chained", () => {
    expect(new Migration().build().directives).toEqual([]);
  });

  it("build() does not leak the internal array (mutation-safe)", () => {
    const m = new Migration().renameField("t", "a", "b");
    const first = m.build();
    first.directives.push({ op: "dropTable", name: "x" });
    const second = m.build();
    expect(second.directives).toHaveLength(1);
  });
});
