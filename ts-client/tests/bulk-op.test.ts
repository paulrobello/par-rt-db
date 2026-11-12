import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import type { BulkOpAccepted, BulkOpInfo, FilterExpr } from "../src/protocol.js";

/** wire-corpus/http-shapes.json (repo root — the single source of truth; same fixture the other six runners consume). */
const HTTP_SHAPES_PATH = resolve(__dirname, "../../wire-corpus/http-shapes.json");

function loadHttpShapes(): Record<string, any> {
  return JSON.parse(readFileSync(HTTP_SHAPES_PATH, "utf8"));
}

describe("bulk-op wire types", () => {
  describe("wire-corpus/http-shapes.json field parity (bulk op)", () => {
    const shapes = loadHttpShapes();

    it("bulkDeleteAccepted decodes as the exported BulkOpAccepted literal", () => {
      // Typed literal: every fixture key must exist on the interface (tsc
      // rejects unknown keys on an exhaustively-typed literal).
      const accepted: BulkOpAccepted = {
        opId: "bulk_01JENQ9Z7K3M2V8R5T4W6X8Y0A",
        matched: 1500,
        state: "running",
      };
      // toEqual is order-insensitive; exact key-set equality (extra or
      // missing fixture keys fail).
      expect(accepted).toEqual(shapes.bulkDeleteAccepted);
    });

    it("bulkOpInfoRunning decodes as BulkOpInfo with error absent", () => {
      const running: BulkOpInfo = {
        id: "bulk_01JENQ9Z7K3M2V8R5T4W6X8Y0A",
        state: "running",
        matched: 1500,
        deleted: 500,
      };
      expect(running).toEqual(shapes.bulkOpInfoRunning);
      // The server skip_serializing_if's `error` off a non-failed payload —
      // the running fixture carries no error key at all.
      expect(Object.hasOwn(shapes.bulkOpInfoRunning, "error")).toBe(false);
    });

    it("bulkOpInfoFailed decodes as BulkOpInfo with error present", () => {
      const failed: BulkOpInfo = {
        id: "bulk_01JENQ9Z7K3M2V8R5T4W6X8Y0A",
        state: "failed",
        matched: 1500,
        deleted: 500,
        error: "chunk turn failed: schema changed mid-op",
      };
      expect(failed).toEqual(shapes.bulkOpInfoFailed);
      expect(Object.hasOwn(shapes.bulkOpInfoFailed, "error")).toBe(true);
    });

    it("an unknown key is rejected on decode (exact key-set compare)", () => {
      // There is no runtime decoder in the TS client (plain interfaces), so
      // strictness is asserted as key-set equality against each fixture: any
      // key the server adds that the interface lacks surfaces here as a
      // key-set mismatch, the TS analogue of `deny_unknown_fields`.
      const acceptedKeys = Object.keys(shapes.bulkDeleteAccepted).sort();
      expect(acceptedKeys).toEqual(["matched", "opId", "state"]);
      const runningKeys = Object.keys(shapes.bulkOpInfoRunning).sort();
      expect(runningKeys).toEqual(["deleted", "id", "matched", "state"]);
      const failedKeys = Object.keys(shapes.bulkOpInfoFailed).sort();
      expect(failedKeys).toEqual(["deleted", "error", "id", "matched", "state"]);
    });
  });
});
