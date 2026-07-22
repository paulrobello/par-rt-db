import { describe, expect, it } from "vitest";
import { mutation } from "../src/mutation.js";

describe("transaction builder", () => {
  it("builds an ordered multi-step txn with table on every step", () => {
    const txn = mutation()
      .insert("items", { projectId: "p1", title: "a" })
      .patch("items", "i1", { title: "b" })
      .replace("items", "i4", { projectId: "p1", title: "c" })
      .delete("items", "i2")
      .expectVersion("items", "i3", 7)
      .expectAbsent("items", "by_project_and_title", ["p1", "dup"])
      .upsert("items", {
        index: "by_project_and_title",
        eq: ["p1", "x"],
        insert: { projectId: "p1", title: "x" },
        patch: { title: "x2" },
      })
      .build();

    expect(txn).toEqual({
      steps: [
        { op: "insert", table: "items", doc: { projectId: "p1", title: "a" } },
        { op: "patch", table: "items", id: "i1", fields: { title: "b" } },
        { op: "replace", table: "items", id: "i4", doc: { projectId: "p1", title: "c" } },
        { op: "delete", table: "items", id: "i2" },
        { op: "expectVersion", table: "items", id: "i3", version: 7 },
        { op: "expectAbsent", table: "items", index: "by_project_and_title", eq: ["p1", "dup"] },
        {
          op: "upsert",
          table: "items",
          index: "by_project_and_title",
          eq: ["p1", "x"],
          insert: { projectId: "p1", title: "x" },
          patch: { title: "x2" },
        },
      ],
    });
  });

  it("produces an empty txn when nothing is added", () => {
    expect(mutation().build()).toEqual({ steps: [] });
  });
});
