import { describe, expect, it, vi } from "vitest";
import type { ChangeFeedResponse, ChangeOp, RtDbAdminClient } from "@par-rt-db/client";
import { HISTORY_SCAN_LIMIT, documentBody, diffDocuments, loadDocumentHistory } from "./changeFeed";

// Document-history scan (DocumentHistoryPanel): pages the change feed, filters
// to the one document, caps the scan, and diffs consecutive versions.

function op(over: Partial<ChangeOp> & { seq: number }): ChangeOp {
  return {
    logId: "log-1",
    db: "db1",
    table: "docs",
    docId: "doc-1",
    kind: "patch",
    doc: null,
    ts: 1,
    ...over,
  } as ChangeOp;
}

function page(over: {
  ops: ChangeOp[];
  nextSeq: number;
  logId?: string;
  head?: number;
}): ChangeFeedResponse {
  return {
    logId: over.logId ?? "log-1",
    head: over.head ?? 100,
    ops: over.ops,
    nextSeq: over.nextSeq,
  } as ChangeFeedResponse;
}

function makeClient(pages: ChangeFeedResponse[]) {
  const adminChanges = vi.fn<() => Promise<ChangeFeedResponse>>().mockImplementation(async () => {
    if (pages.length === 0) throw new Error("unexpected extra page fetch");
    return pages.shift() as ChangeFeedResponse;
  });
  return { adminChanges } as unknown as RtDbAdminClient;
}

describe("documentBody", () => {
  it("strips the four system fields and keeps user data", () => {
    expect(
      documentBody({ _id: "d1", _creationTime: 1, _version: 2, _deletedAt: null, name: "x" }),
    ).toEqual({ name: "x" });
  });
});

describe("diffDocuments", () => {
  it("ignores system fields and diffs added/removed/changed user fields", () => {
    const diffs = diffDocuments(
      { _id: "d1", name: "old", gone: 1 },
      { _id: "d1", name: "new", added: true },
    );
    expect(diffs.map((d) => `${d.field}:${d.kind}`)).toEqual([
      "added:added",
      "gone:removed",
      "name:changed",
    ]);
  });

  it("treats equal nested objects as unchanged regardless of key order", () => {
    expect(diffDocuments({ meta: { a: 1, b: [1, 2] } }, { meta: { b: [1, 2], a: 1 } })).toEqual([]);
  });

  it("distinguishes changed from added on deep inequality", () => {
    const diffs = diffDocuments({ meta: { a: 1 } }, { meta: { a: 2 } });
    expect(diffs).toHaveLength(1);
    expect(diffs[0].kind).toBe("changed");
  });
});

describe("loadDocumentHistory", () => {
  const signal = new AbortController().signal;

  it("pages the feed until head and keeps only this document's ops", async () => {
    const client = makeClient([
      page({ ops: [op({ seq: 1, docId: "doc-1" }), op({ seq: 2, docId: "other" })], nextSeq: 60 }),
      page({ ops: [op({ seq: 61, docId: "doc-1" })], nextSeq: 100 }),
    ]);
    const history = await loadDocumentHistory(client, "db1", "docs", "doc-1", signal);
    expect(history.entries.map((e) => e.seq)).toEqual([1, 61]);
    expect(history.bounded).toBe(false);
    expect(history.scanned).toBe(3);
    expect(client.adminChanges).toHaveBeenNthCalledWith(1, "db1", {
      since: 0,
      table: "docs",
      limit: 500,
    });
    expect(client.adminChanges).toHaveBeenNthCalledWith(2, "db1", {
      since: 60,
      table: "docs",
      limit: 500,
    });
  });

  it("reports bounded when the scan cap is exhausted before reaching head", async () => {
    const pages: ChangeFeedResponse[] = [];
    for (let i = 0; i < HISTORY_SCAN_LIMIT / 500; i++) {
      pages.push(page({ ops: [op({ seq: i + 1, docId: "doc-1" })], nextSeq: i + 1 }));
    }
    // Every page advances nextSeq by 1 and never reaches head=100 within the
    // 10-page (5000-op) cap -> bounded scan.
    const history = await loadDocumentHistory(makeClient(pages), "db1", "docs", "doc-1", signal);
    expect(history.bounded).toBe(true);
  });

  it("rejects when the database is recreated mid-scan (logId changes)", async () => {
    const client = makeClient([
      page({ ops: [], nextSeq: 60 }),
      page({ ops: [], nextSeq: 100, logId: "log-2" }),
    ]);
    await expect(loadDocumentHistory(client, "db1", "docs", "doc-1", signal)).rejects.toThrow(
      "database was recreated",
    );
  });

  it("rejects when the change-feed cursor does not advance", async () => {
    const client = makeClient([page({ ops: [], nextSeq: 0 })]);
    await expect(loadDocumentHistory(client, "db1", "docs", "doc-1", signal)).rejects.toThrow(
      "cursor did not advance",
    );
  });

  it("throws AbortError when the signal is already aborted", async () => {
    const client = makeClient([]);
    const ctl = new AbortController();
    ctl.abort();
    await expect(loadDocumentHistory(client, "db1", "docs", "doc-1", ctl.signal)).rejects.toThrow(
      "Aborted",
    );
  });
});
