import { describe, expect, it } from "vitest";
import { projectOptimisticUpdate } from "../src/optimistic.js";
import type { TransactionJson } from "../src/protocol.js";

function transaction(steps: TransactionJson["steps"]): TransactionJson {
  return { steps };
}

describe("optimistic adjustCounter projection", () => {
  it("declines the affected table when adjustCounter is followed by a patch", () => {
    const projection = projectOptimisticUpdate(
      { table: "counters" },
      [{ _id: "counter-1", value: 3, label: "before" }],
      transaction([
        {
          op: "adjustCounter",
          table: "counters",
          id: "counter-1",
          field: "value",
          delta: 2,
        },
        {
          op: "patch",
          table: "counters",
          id: "counter-1",
          fields: { label: "after" },
        },
      ]),
    );

    expect(projection).toEqual({ overlaid: false });
  });

  it("still overlays a target-table patch when adjustCounter only affects another table", () => {
    const projection = projectOptimisticUpdate(
      { table: "items" },
      [{ _id: "item-1", label: "before" }],
      transaction([
        {
          op: "adjustCounter",
          table: "counters",
          id: "counter-1",
          field: "value",
          delta: 2,
        },
        {
          op: "patch",
          table: "items",
          id: "item-1",
          fields: { label: "after" },
        },
      ]),
    );

    expect(projection).toEqual({
      overlaid: true,
      value: [{ _id: "item-1", label: "after" }],
    });
  });
});
