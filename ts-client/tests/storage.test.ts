import { describe, it, expect } from "vitest";
import { InMemoryRtDbClient } from "../src/in_memory.js";

describe("in-memory storage", () => {
  it("uploads, serves-via-url-shape, deletes, and reports metadata", async () => {
    const c = new InMemoryRtDbClient();
    const bytes = new Uint8Array([1, 2, 3, 4]);
    const up = await c.upload(bytes, "image/png");
    expect(up.id).toBeTypeOf("string");
    expect(up.size).toBe(4);
    expect(up.contentType).toBe("image/png");
    expect(up.sha256).toBeTypeOf("string");

    expect(c.getUrl(up.id)).toBe(`memory://${up.id}`);

    const meta = await c.getFileMetadata(up.id);
    expect(meta.size).toBe(4);
    expect(meta.contentType).toBe("image/png");

    await c.deleteFile(up.id);
    await expect(c.getFileMetadata(up.id)).rejects.toBeTruthy();
  });

  it("getUrl against the http client builds the public URL", async () => {
    // Wiremock-free shape check: the http client constructs the public URL
    // without a fetch. Full HTTP round trip is covered by the live-server E2E.
    const { RtDbHttpClient } = await import("../src/http.js");
    const http = new RtDbHttpClient({ url: "https://rtdb.example.com/", db: "kanban", token: "t" });
    expect(http.getUrl("abc")).toBe("https://rtdb.example.com/storage/abc");
  });

  it("appendImageParams builds the canonical query string", async () => {
    const { appendImageParams } = await import("../src/http.js");
    const url = appendImageParams("https://rtdb.example/storage/abc", {
      w: 100, h: 50, fit: "cover", q: 80, format: "jpeg",
    });
    expect(url).toBe("https://rtdb.example/storage/abc?w=100&h=50&fit=cover&q=80&format=jpeg");
  });

  it("appendImageParams omits unset opts", async () => {
    const { appendImageParams } = await import("../src/http.js");
    expect(appendImageParams("https://rtdb.example/storage/abc", { w: 64 }))
      .toBe("https://rtdb.example/storage/abc?w=64");
  });

  it("transformUrl against the http client builds the URL with params", async () => {
    const { RtDbHttpClient } = await import("../src/http.js");
    const http = new RtDbHttpClient({ url: "https://rtdb.example.com/", db: "kanban", token: "t" });
    expect(http.transformUrl("abc", { w: 100, fit: "contain" }))
      .toBe("https://rtdb.example.com/storage/abc?w=100&fit=contain");
  });
});
