/**
 * External job claims with fencing tokens (2026-09-09 spec) — TypeScript
 * client view. Covers the unit surfaces the feature adds:
 *
 * 1. Wire encode of the external `schedule` step and WS frame (flag present
 *    only when requested, byte-stable through JSON round-trips).
 * 2. The TxnBuilder's `external` flag on the `schedule` step.
 * 3. The in-memory harness: `schedule(txn, when, external)` stores the flag,
 *    `tick()` NEVER fires an external job while ordinary jobs still fire, and
 *    `listSchedules()` carries the required `external` field.
 * 4. `ClaimedSchedule` decode + the HTTP claim/finalize surface
 *    (`claimSchedules` / `completeSchedule` / `retrySchedule` /
 *    `failSchedule`).
 *
 * The in-memory harness does not model claim/finalize (no corpus case
 * requires it) — only the tick-skip and field plumbing, mirroring server
 * semantics.
 */

import { describe, expect, it, vi } from "vitest";

import { RtDbHttpClient } from "../src/http.js";
import { InMemoryRtDbClient } from "../src/in_memory/index.js";
import { mutation } from "../src/mutation.js";
import type { ClaimedSchedule, ClientMessage, StepJson, TransactionJson } from "../src/protocol.js";
import { createApi } from "../src/query.js";
import { defineSchema, defineTable, t } from "../src/schema.js";

const schema = defineSchema({
  items: defineTable({
    name: t.string(),
  }).index("by_name", ["name"]),
});

const api = createApi(schema);

/** Fixed (non-incrementing) clock so due-times are stable under `tick`. */
function newClockClient(): { c: InMemoryRtDbClient; setNow: (t: number) => void } {
  let ms = 1_700_000_000_000;
  const c = new InMemoryRtDbClient({ now: () => ms, random: () => 0 });
  c.pushSchema(schema);
  return { c, setNow: (t) => (ms = t) };
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

describe("external schedule step/frame wire encode", () => {
  const when = { type: "afterMs", ms: 100 } as const;
  const txn: TransactionJson = { steps: [] };

  it("a schedule step carries external:true verbatim on the wire", () => {
    const step: StepJson = { op: "schedule", when, txn, external: true };
    expect(JSON.parse(JSON.stringify(step))).toEqual(step);
    expect("external" in step).toBe(true);
  });

  it("an ordinary schedule step omits the flag entirely (skip-on-absent)", () => {
    const step: StepJson = { op: "schedule", when, txn };
    expect(JSON.parse(JSON.stringify(step))).toEqual(step);
    expect("external" in step).toStrictEqual(false);
  });

  it("a schedule WS frame carries external:true; omitted otherwise", () => {
    const external: ClientMessage = {
      type: "schedule",
      scheduleId: "s1",
      when,
      txn,
      external: true,
    };
    expect(JSON.parse(JSON.stringify(external))).toEqual(external);
    const ordinary: ClientMessage = { type: "schedule", scheduleId: "s1", when, txn };
    expect("external" in ordinary).toStrictEqual(false);
  });
});

describe("TxnBuilder external flag", () => {
  it("schedule(when, txn, true) produces external:true; default omits it", () => {
    const built = mutation().schedule({ type: "afterMs", ms: 100 }, { steps: [] }, true).build();
    expect(built.steps[0]).toEqual({
      op: "schedule",
      when: { type: "afterMs", ms: 100 },
      txn: { steps: [] },
      external: true,
    });
    expect(JSON.parse(JSON.stringify(built.steps[0]))).toEqual(built.steps[0]);

    const internal = mutation().schedule({ type: "afterMs", ms: 100 }, { steps: [] }).build();
    expect("external" in internal.steps[0]).toStrictEqual(false);
    // Explicit false is the same as the default: omitted on the wire.
    const explicitFalse = mutation()
      .schedule({ type: "afterMs", ms: 100 }, { steps: [] }, false)
      .build();
    expect("external" in explicitFalse.steps[0]).toStrictEqual(false);
  });
});

describe("InMemoryRtDbClient — external jobs", () => {
  const insertTxn = mutation().insert("items", { name: "a" }).build();

  it("schedule(txn, when, true) stores the external flag and listSchedules carries it", async () => {
    const { c } = newClockClient();
    const { id } = await c.schedule(insertTxn, { type: "afterMs", ms: 1000 }, true);
    const info = (await c.listSchedules()).find((s) => s.id === id);
    expect(info).toBeDefined();
    expect(info?.external).toStrictEqual(true);
  });

  it("an external job is NEVER fired by tick() while an ordinary job still fires", async () => {
    const { c, setNow } = newClockClient();
    const BASE = 1_700_000_000_000;
    setNow(BASE);
    const { id: externalId } = await c.schedule(insertTxn, { type: "afterMs", ms: 1000 }, true);
    const { id: internalId } = await c.schedule(insertTxn, { type: "afterMs", ms: 1000 });

    setNow(BASE + 2000); // past both due times
    c.tick();

    // The ordinary job fired (and, as a one-shot, left the registry); the
    // external one sat pending, unexecuted.
    expect(await c.query(api.items.query().collect())).toHaveLength(1);
    const externalInfo = (await c.listSchedules()).find((s) => s.id === externalId);
    expect(externalInfo?.status).toStrictEqual("pending");
    expect(externalInfo?.firedCount).toStrictEqual(0);
    expect(externalInfo?.external).toStrictEqual(true);
    const internalInfo = (await c.listSchedules()).find((s) => s.id === internalId);
    expect(internalInfo).toBeUndefined(); // one-shot removed after firing
  });

  it("an external job created via the schedule STEP is also skipped by tick()", async () => {
    const { c, setNow } = newClockClient();
    const BASE = 1_700_000_000_000;
    setNow(BASE);
    await c.mutate(mutation().schedule({ type: "afterMs", ms: 1000 }, insertTxn, true).build());

    setNow(BASE + 2000);
    c.tick();

    expect(await c.query(api.items.query().collect())).toHaveLength(0);
    const infos = await c.listSchedules();
    expect(infos).toHaveLength(1);
    expect(infos[0].external).toStrictEqual(true);
    expect(infos[0].status).toStrictEqual("pending");
  });
});

describe("RtDbHttpClient external job claims", () => {
  const claimed: ClaimedSchedule = {
    id: "job-x",
    kind: "oneshot",
    dueAt: 5000,
    txn: { steps: [{ op: "insert", table: "items", doc: { name: "x" } }] },
    leaseGeneration: 3,
    leaseDeadlineMs: 6000,
  };

  function newHttp(fetchMock: ReturnType<typeof vi.fn>): RtDbHttpClient {
    return new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock as unknown as typeof fetch,
    });
  }

  it("claimSchedules posts {db} to /api/schedule/claim and decodes {jobs}", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ jobs: [claimed] }));
    const client = newHttp(fetchMock);

    const jobs = await client.claimSchedules();

    expect(jobs).toEqual([claimed]);
    const [calledUrl, init] = fetchMock.mock.calls[0];
    expect(calledUrl).toBe("http://h:8300/api/schedule/claim");
    expect(init.method).toBe("POST");
    expect(init.headers.Authorization).toBe("Bearer tok");
    expect(JSON.parse(init.body)).toEqual({ db: "kanban" });
  });

  it("claimSchedules forwards limit/leaseMs opts in the body", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ jobs: [] }));
    const client = newHttp(fetchMock);

    await expect(client.claimSchedules({ limit: 4, leaseMs: 60000 })).resolves.toEqual([]);
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({
      db: "kanban",
      limit: 4,
      leaseMs: 60000,
    });
  });

  it("claimSchedules omits opt keys that are not provided (server serde defaults apply)", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ jobs: [] }));
    const client = newHttp(fetchMock);

    await client.claimSchedules({ leaseMs: 60000 });
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({
      db: "kanban",
      leaseMs: 60000,
    });
  });

  it("the claimed job decodes into ClaimedSchedule (fencing fields land)", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ jobs: [claimed] }));
    const client = newHttp(fetchMock);

    const [job] = await client.claimSchedules();
    const decoded: ClaimedSchedule = job;
    expect(decoded.id).toBe("job-x");
    expect(decoded.kind).toBe("oneshot");
    expect(decoded.leaseGeneration).toBe(3);
    expect(decoded.leaseDeadlineMs).toBe(6000);
    expect(decoded.txn).toEqual(claimed.txn);
    // Optional cron/everyMs omitted for a oneshot.
    expect(decoded.cron).toBeUndefined();
    expect(decoded.everyMs).toBeUndefined();
  });

  it("completeSchedule posts {db, lease} to the per-id complete route", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ ok: true }));
    const client = newHttp(fetchMock);

    await client.completeSchedule("job-x", 3);

    const [calledUrl, init] = fetchMock.mock.calls[0];
    expect(calledUrl).toBe("http://h:8300/api/schedule/job-x/complete");
    expect(JSON.parse(init.body)).toEqual({ db: "kanban", lease: 3 });
  });

  it("retrySchedule posts {db, lease} plus delayMs/error opts when provided", async () => {
    // A Response body can be consumed once — each call needs its own response.
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ ok: true }))
      .mockResolvedValueOnce(jsonResponse({ ok: true }));
    const client = newHttp(fetchMock);

    await client.retrySchedule("job-x", 7);
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({ db: "kanban", lease: 7 });

    await client.retrySchedule("job-x", 7, { delayMs: 30_000, error: "worker crashed" });
    expect(JSON.parse(fetchMock.mock.calls[1][1].body)).toEqual({
      db: "kanban",
      lease: 7,
      delayMs: 30_000,
      error: "worker crashed",
    });
  });

  it("failSchedule posts {db, lease, error} to the per-id fail route", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ ok: true }));
    const client = newHttp(fetchMock);

    await client.failSchedule("job-x", 9, "upstream 500");

    const [calledUrl, init] = fetchMock.mock.calls[0];
    expect(calledUrl).toBe("http://h:8300/api/schedule/job-x/fail");
    expect(JSON.parse(init.body)).toEqual({ db: "kanban", lease: 9, error: "upstream 500" });
  });

  it("a stale fencing token surfaces the server's CONFLICT envelope as RtDbError", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(jsonResponse({ code: "CONFLICT", message: "stale lease" }, 409));
    const client = newHttp(fetchMock);

    await expect(client.completeSchedule("job-x", 1)).rejects.toMatchObject({
      name: "RtDbError",
      code: "CONFLICT",
      message: "stale lease",
    });
  });
});
