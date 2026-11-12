import { describe, expect, it } from "vitest";
import { cellText, formatBytes, formatDuration, formatFieldType, formatNumber } from "./format";

// Pure formatters — the table/chip rendering vocabulary every page shares.
// formatTime/formatDateTime are locale-dependent; pin only their stable
// contract (numeric epoch input, hour12:false output), not exact strings.

describe("formatFieldType", () => {
  it("passes scalar types through verbatim", () => {
    for (const type of ["string", "number", "boolean", "null", "int64", "bytes", "any"] as const) {
      expect(formatFieldType({ type })).toBe(type);
    }
  });

  it("renders id references with the target table, plus onDelete suffix", () => {
    expect(formatFieldType({ type: "id", table: "users" })).toBe("id<users>");
    expect(formatFieldType({ type: "id", table: "users", onDelete: "cascade" })).toBe(
      "id<users>·cascade",
    );
  });

  it("renders literals via JSON.stringify", () => {
    expect(formatFieldType({ type: "literal", value: "active" })).toBe('literal("active")');
    expect(formatFieldType({ type: "literal", value: 3 })).toBe("literal(3)");
  });

  it("marks optionals with ? and joins union variants", () => {
    expect(formatFieldType({ type: "optional", inner: { type: "string" } })).toBe("string?");
    expect(
      formatFieldType({
        type: "union",
        variants: [{ type: "string" }, { type: "int64" }],
      }),
    ).toBe("string | int64");
  });

  it("renders arrays, records, vectors, and bare objects", () => {
    expect(formatFieldType({ type: "array", element: { type: "number" } })).toBe("number[]");
    expect(formatFieldType({ type: "record", value: { type: "string" } })).toBe("record<string>");
    expect(formatFieldType({ type: "vector", dimensions: 8 })).toBe("vector(8)");
    expect(formatFieldType({ type: "object", fields: {} })).toBe("object");
  });

  it("recurses through nested composites", () => {
    expect(
      formatFieldType({
        type: "optional",
        inner: { type: "array", element: { type: "id", table: "tags" } },
      }),
    ).toBe("id<tags>[]?");
  });
});

describe("formatBytes", () => {
  it("renders sub-KB values in bytes", () => {
    expect(formatBytes(0)).toBe("0 B");
    expect(formatBytes(1023)).toBe("1023 B");
  });

  it("scales to KB/MB/GB/TB", () => {
    expect(formatBytes(1024)).toBe("1.0 KB");
    expect(formatBytes(1536)).toBe("1.5 KB");
    expect(formatBytes(1024 * 1024)).toBe("1.0 MB");
    expect(formatBytes(5 * 1024 * 1024 * 1024)).toBe("5.0 GB");
  });

  it("caps at TB and drops the decimal at 3 digits", () => {
    // 1500 TB would still say TB (cap) — just check a big value stays TB-shaped
    // and ≥100 values lose the decimal place.
    expect(formatBytes(150 * 1024)).toBe("150 KB");
    const huge = formatBytes(2048 * 1024 * 1024 * 1024 * 1024);
    expect(huge.endsWith(" TB")).toBe(true);
  });
});

describe("formatDuration", () => {
  it("trims leading zero units", () => {
    expect(formatDuration(5)).toBe("5s");
    expect(formatDuration(65)).toBe("1m 5s");
    expect(formatDuration(3661)).toBe("1h 1m 1s");
    expect(formatDuration(90061 + 2 * 86400)).toBe("3d 1h 1m 1s");
  });

  it("keeps zero units between nonzero neighbors and clamps negatives", () => {
    expect(formatDuration(3600)).toBe("1h 0m 0s");
    expect(formatDuration(-10)).toBe("0s");
  });
});

describe("formatNumber", () => {
  it("thousands-separates", () => {
    expect(formatNumber(1234567)).toBe("1,234,567");
    expect(formatNumber(7)).toBe("7");
  });
});

describe("cellText", () => {
  it("spells null out and JSON-ifies objects", () => {
    expect(cellText(null)).toBe("null");
    expect(cellText({ a: 1 })).toBe('{"a":1}');
    expect(cellText([1, 2])).toBe("[1,2]");
    expect(cellText("text")).toBe("text");
    expect(cellText(12)).toBe("12");
  });
});
