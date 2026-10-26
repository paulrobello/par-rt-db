import { describe, expect, it, vi } from "vitest";
import { RtDbHttpClient } from "../src/http.js";
import type { ChangeOp } from "../src/protocol.js";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function op(seq: number): ChangeOp {
  return {
    seq,
    table: "notes",
    docId: `d${seq}`,
    kind: "insert",
    doc: { _id: `d${seq}` },
    ts: 1_700_000_000_000 + seq,
  };
}

function feed(
  ops: ChangeOp[],
  logId = "log-1",
): {
  ops: ChangeOp[];
  nextSeq: number;
  head: number;
  logId: string;
} {
  const last = ops.length > 0 ? ops[ops.length - 1].seq : 0;
  return { ops, nextSeq: last, head: last, logId };
}

function setup() {
  const fetchMock = vi.fn();
  const client = new RtDbHttpClient({
    url: "http://h:8300",
    db: "app",
    token: "machine-token",
    fetch: fetchMock,
  });
  return { fetchMock, client };
}

describe("streamChanges", () => {
  it("yields ops one by one and advances the since cursor across pages", async () => {
    const { fetchMock, client } = setup();
    fetchMock
      .mockResolvedValueOnce(jsonResponse(feed([op(1), op(2)])))
      .mockResolvedValueOnce(jsonResponse(feed([op(3)])));

    const stream = client.streamChanges({ pollIntervalMs: 1, backoffBaseMs: 1 });
    const seen: ChangeOp[] = [];
    for await (const change of stream) {
      seen.push(change);
      if (seen.length === 3) break;
    }

    expect(seen.map((o) => o.seq)).toEqual([1, 2, 3]);
    expect(stream.cursor).toBe(3);
    expect(String(fetchMock.mock.calls[0][0])).toContain("since=0");
    expect(String(fetchMock.mock.calls[1][0])).toContain("since=2");
  });

  it("resumes from the last yielded seq after a failed poll", async () => {
    const { fetchMock, client } = setup();
    fetchMock
      .mockResolvedValueOnce(jsonResponse(feed([op(1)])))
      .mockRejectedValueOnce(new Error("boom"))
      .mockResolvedValueOnce(jsonResponse(feed([op(2)])));

    const stream = client.streamChanges({ pollIntervalMs: 1, backoffBaseMs: 1, backoffMaxMs: 2 });
    const seen: ChangeOp[] = [];
    for await (const change of stream) {
      seen.push(change);
      if (seen.length === 2) break;
    }

    expect(seen.map((o) => o.seq)).toEqual([1, 2]);
    expect(fetchMock.mock.calls.length).toBe(3);
    expect(String(fetchMock.mock.calls[2][0])).toContain("since=1");
    expect(stream.cursor).toBe(2);
  });

  it("CURSOR_EXPIRED is terminal — thrown once, no retry", async () => {
    const { fetchMock, client } = setup();
    fetchMock.mockResolvedValue(
      jsonResponse({ code: "CURSOR_EXPIRED", message: "cursor too old" }, 410),
    );

    const stream = client.streamChanges({ pollIntervalMs: 1, backoffBaseMs: 1 });
    await expect(async () => {
      for await (const _ of stream) {
        // no ops are ever yielded
      }
    }).rejects.toMatchObject({ code: "CURSOR_EXPIRED" });
    expect(fetchMock.mock.calls.length).toBe(1);
  });

  it("a logId change mid-stream is terminal", async () => {
    const { fetchMock, client } = setup();
    fetchMock
      .mockResolvedValueOnce(jsonResponse(feed([op(1)])))
      .mockResolvedValue(jsonResponse(feed([op(2)], "log-2")));

    const stream = client.streamChanges({ pollIntervalMs: 1, backoffBaseMs: 1 });
    await expect(async () => {
      for await (const change of stream) {
        if (change.seq === 2) break;
      }
    }).rejects.toThrow(/logId changed/);
    // the second page's op was fetched but never yielded — the log changed
    expect(fetchMock.mock.calls.length).toBe(2);
  });

  it("stop() ends a caught-up stream without another poll", async () => {
    const { fetchMock, client } = setup();
    fetchMock.mockResolvedValue(jsonResponse(feed([])));

    const stream = client.streamChanges({ pollIntervalMs: 50, backoffBaseMs: 1 });
    setTimeout(() => stream.stop(), 5);
    const seen: ChangeOp[] = [];
    for await (const change of stream) {
      seen.push(change);
    }
    expect(seen).toEqual([]);
    expect(fetchMock.mock.calls.length).toBe(1);
  });
});
