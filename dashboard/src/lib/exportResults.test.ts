import { describe, expect, it, vi } from "vitest";
import { downloadTextFile, toCsvString, toJsonString } from "./exportResults";

describe("result exports", () => {
  const rows = [
    {
      plain: "hello, world",
      quote: 'say "hi"',
      multiline: "first\nsecond",
      nested: { n: 1 },
      missing: null,
    },
  ];

  it("quotes CSV headers and embedded commas, quotes, CR and newlines using RFC4180", () => {
    expect(toCsvString(["plain", "quote", "multiline", "nested", "missing"], rows)).toBe(
      'plain,quote,multiline,nested,missing\r\n"hello, world","say ""hi""","first\nsecond","{""n"":1}",\r\n',
    );
    expect(toCsvString(["a,b", 'a"b', "a\rb"], [])).toBe('"a,b","a""b","a\rb"\r\n');
  });

  it("downloads the same rows as JSON with the right filename and MIME type", () => {
    const create = vi.fn((_blob: Blob) => "blob:export");
    const revoke = vi.fn();
    vi.stubGlobal("URL", { createObjectURL: create, revokeObjectURL: revoke });
    const click = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {});
    try {
      downloadTextFile("results.json", toJsonString(rows), "application/json");
      const blob = create.mock.calls[0]?.[0] as Blob | undefined;
      expect(blob?.type).toBe("application/json");
      expect(JSON.parse(toJsonString(rows))).toEqual(rows);
      expect(click).toHaveBeenCalledOnce();
      expect(click.mock.instances[0]).toHaveProperty("download", "results.json");
      expect(revoke).toHaveBeenCalledWith("blob:export");
    } finally {
      vi.unstubAllGlobals();
    }
  });
});
