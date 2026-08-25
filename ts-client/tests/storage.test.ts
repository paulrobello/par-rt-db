import { describe, expect, it, vi } from "vitest";
import { RtDbHttpClient } from "../src/http.js";
import { InMemoryRtDbClient } from "../src/in_memory/index.js";
import { PROTOCOL_VERSION } from "../src/protocol.js";

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

  it("accepts a Blob upload and round-trips the same bytes (ENH-021)", async () => {
    const c = new InMemoryRtDbClient();
    const bytes = new Uint8Array([10, 20, 30, 40, 50]);
    const up = await c.upload(new Blob([bytes]), "application/octet-stream");
    expect(up.size).toBe(5);
    expect(up.sha256).toBeTypeOf("string");
    // Same bytes via Uint8Array produce the same digest.
    const ref = await c.upload(bytes);
    expect(up.sha256).toBe(ref.sha256);
  });

  it("accepts a ReadableStream upload and round-trips the same bytes (ENH-021)", async () => {
    const c = new InMemoryRtDbClient();
    const bytes = new Uint8Array([1, 2, 3]);
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(bytes);
        controller.close();
      },
    });
    const up = await c.upload(stream, "image/png");
    expect(up.size).toBe(3);
    const ref = await c.upload(bytes);
    expect(up.sha256).toBe(ref.sha256);
  });

  it("accepts ArrayBuffer and string uploads (ENH-021)", async () => {
    const c = new InMemoryRtDbClient();
    const buf = new ArrayBuffer(4);
    new Uint8Array(buf).set([1, 2, 3, 4]);
    const upBuf = await c.upload(buf);
    expect(upBuf.size).toBe(4);
    const upStr = await c.upload("hello");
    expect(upStr.size).toBe(5);
    const refStr = await c.upload(new TextEncoder().encode("hello"));
    expect(upStr.sha256).toBe(refStr.sha256);
  });

  it("forwards a Blob body to fetch verbatim (ENH-021)", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ id: "f1", sha256: "abc", size: 3 }), {
        status: 200,
        headers: { "content-type": "application/json" },
      }),
    );
    const http = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });
    const blob = new Blob([new Uint8Array([1, 2, 3])]);
    const res = await http.upload(blob, "image/png");
    expect(res).toEqual({ id: "f1", sha256: "abc", size: 3 });
    const init = fetchMock.mock.calls[0][1] as RequestInit;
    // The body is the Blob itself — NOT buffered into an ArrayBuffer/Uint8Array.
    expect(init.body).toBe(blob);
    expect(init.headers).toMatchObject({
      Authorization: "Bearer tok",
      "content-type": "image/png",
      // ARC-013: protocol header rides on every HTTP call, upload included.
      "X-Rtdb-Protocol": String(PROTOCOL_VERSION),
    });
  });

  it("deleteFile DELETEs with bearer + protocol headers (ARC-013)", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(null, { status: 204 }));
    const http = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });
    await http.deleteFile("f1");
    const [calledUrl, init] = fetchMock.mock.calls[0];
    expect(calledUrl).toBe("http://h:8300/api/storage/kanban/f1");
    expect(init.method).toBe("DELETE");
    expect(init.headers).toEqual({
      Authorization: "Bearer tok",
      "X-Rtdb-Protocol": String(PROTOCOL_VERSION),
    });
  });

  it("forwards a ReadableStream body to fetch verbatim (ENH-021)", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ id: "f1", sha256: "abc", size: 3 }), {
        status: 200,
        headers: { "content-type": "application/json" },
      }),
    );
    const http = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array([1, 2, 3]));
        controller.close();
      },
    });
    await http.upload(stream);
    const init = fetchMock.mock.calls[0][1] as RequestInit;
    expect(init.body).toBe(stream);
  });

  it("still accepts Uint8Array unchanged (ENH-021 regression)", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ id: "f1", sha256: "abc", size: 4 }), {
        status: 200,
        headers: { "content-type": "application/json" },
      }),
    );
    const http = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });
    const bytes = new Uint8Array([1, 2, 3, 4]);
    const res = await http.upload(bytes);
    expect(res).toEqual({ id: "f1", sha256: "abc", size: 4 });
    const init = fetchMock.mock.calls[0][1] as RequestInit;
    expect(init.body).toBe(bytes);
  });

  it("throws BAD_REQUEST for an unsupported upload body type", async () => {
    const http = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: vi.fn(),
    });
    await expect(http.upload(42 as unknown as never)).rejects.toMatchObject({
      code: "BAD_REQUEST",
      name: "RtDbError",
    });
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
      w: 100,
      h: 50,
      fit: "cover",
      q: 80,
      format: "jpeg",
    });
    expect(url).toBe("https://rtdb.example/storage/abc?w=100&h=50&fit=cover&q=80&format=jpeg");
  });

  it("appendImageParams omits unset opts", async () => {
    const { appendImageParams } = await import("../src/http.js");
    expect(appendImageParams("https://rtdb.example/storage/abc", { w: 64 })).toBe(
      "https://rtdb.example/storage/abc?w=64",
    );
  });

  it("appendImageParams omits format=auto (server default)", async () => {
    const { appendImageParams } = await import("../src/http.js");
    const url = appendImageParams("https://rtdb.example/storage/abc", {
      w: 100,
      format: "auto",
    });
    expect(url).toBe("https://rtdb.example/storage/abc?w=100");
  });

  it("transformUrl against the http client builds the URL with params", async () => {
    const { RtDbHttpClient } = await import("../src/http.js");
    const http = new RtDbHttpClient({ url: "https://rtdb.example.com/", db: "kanban", token: "t" });
    expect(http.transformUrl("abc", { w: 100, fit: "contain" })).toBe(
      "https://rtdb.example.com/storage/abc?w=100&fit=contain",
    );
  });
});
