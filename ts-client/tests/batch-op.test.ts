import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import type { BatchQueryOutcomeJson } from "../src/protocol.js";

/** wire-corpus/http-shapes.json (repo root — the single source of truth; same
 * fixture the other six runners consume). Pins the batch-transport shapes
 * (`/api/query-batch`, `/api/mutate-batch`) the WS corpus does not cover. */
const HTTP_SHAPES_PATH = resolve(__dirname, "../../wire-corpus/http-shapes.json");

function loadHttpShapes(): Record<string, any> {
  return JSON.parse(readFileSync(HTTP_SHAPES_PATH, "utf8"));
}

describe("batch-op wire types", () => {
  describe("wire-corpus/http-shapes.json field parity (batch ops)", () => {
    const shapes = loadHttpShapes();

    it("queryBatchResponse slots decode as BatchQueryOutcomeJson literals", () => {
      const slots = shapes.queryBatchResponse.results;
      expect(slots).toHaveLength(2);
      // Typed literal: every fixture key must exist on the interface (tsc
      // rejects unknown keys on an exhaustively-typed union literal).
      const okSlot: BatchQueryOutcomeJson = {
        ok: true,
        result: slots[0].result,
      };
      expect(okSlot).toEqual(slots[0]);
      const errSlot: BatchQueryOutcomeJson = {
        ok: false,
        error: slots[1].error,
      };
      expect(errSlot).toEqual(slots[1]);
    });

    it("queryBatchRequest carries exactly {db, queries}", () => {
      // Plain literal pin: exact key-set equality (extra or missing fixture
      // keys fail). The ts client builds this body at its batch call site.
      const request = { db: "app1", queries: shapes.queryBatchRequest.queries };
      expect(request).toEqual(shapes.queryBatchRequest);
    });

    it("mutateBatchResponse slots pair ok with results XOR error", () => {
      // No BatchMutateOutcomeJson type exists on the ts surface (the ts client
      // has no mutate-batch call site yet); pin the slots as plain literals —
      // toEqual is exact key-set equality, so omit-when-None drift fails.
      const slots = shapes.mutateBatchResponse.results;
      expect(slots).toHaveLength(2);
      expect(slots[0]).toEqual({
        ok: true,
        results: [
          { op: "insert", table: "items", id: "i1", version: 1 },
          { op: "insert", table: "items", id: "i2", version: 1 },
        ],
      });
      expect(slots[1]).toEqual({
        ok: false,
        error: { code: "CONFLICT", message: "committed elsewhere" },
      });
      expect(Object.hasOwn(slots[0], "error")).toBe(false);
      expect(Object.hasOwn(slots[1], "results")).toBe(false);
    });

    it("mutateBatchRequest entries carry {txn, idempotencyKey?} exactly", () => {
      const entries = shapes.mutateBatchRequest.txns;
      expect(entries).toHaveLength(2);
      expect(entries[0]).toEqual({
        txn: { steps: [] },
        idempotencyKey: "idem-1",
      });
      expect(entries[1]).toEqual({ txn: { steps: [] } });
      expect(Object.hasOwn(entries[0], "idempotencyKey")).toBe(true);
      expect(Object.hasOwn(entries[1], "idempotencyKey")).toBe(false);
    });
  });
});
