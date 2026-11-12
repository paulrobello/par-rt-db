import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import type {
  ClaimedSchedule,
  ClientMessage,
  ScheduleInfo,
  ScheduleWhen,
  ServerMessage,
} from "../src/protocol.js";

/** wire-corpus/http-shapes.json (repo root — the single source of truth; same fixture the other six runners consume). */
const HTTP_SHAPES_PATH = resolve(__dirname, "../../wire-corpus/http-shapes.json");

function loadHttpShapes(): Record<string, any> {
  return JSON.parse(readFileSync(HTTP_SHAPES_PATH, "utf8"));
}

describe("schedule wire types", () => {
  it("ScheduleWhen mirrors server tags (afterMs / runAt / cron / interval)", () => {
    const afterMs: ScheduleWhen = { type: "afterMs", ms: 100 };
    const runAt: ScheduleWhen = { type: "runAt", ms: 9 };
    const cron: ScheduleWhen = { type: "cron", expr: "*/5 * * * *" };
    const interval: ScheduleWhen = { type: "interval", everyMs: 300000 };

    expect(afterMs).toEqual({ type: "afterMs", ms: 100 });
    expect(runAt).toEqual({ type: "runAt", ms: 9 });
    expect(cron).toEqual({ type: "cron", expr: "*/5 * * * *" });
    expect(interval).toEqual({ type: "interval", everyMs: 300000 });
  });

  it("ClientMessage.schedule shape", () => {
    const msg: ClientMessage = {
      type: "schedule",
      scheduleId: "s1",
      when: { type: "afterMs", ms: 100 },
      txn: { steps: [] },
    };
    expect(msg.type).toBe("schedule");
    expect((msg as any).scheduleId).toBe("s1");
    expect((msg as any).when).toEqual({ type: "afterMs", ms: 100 });
    expect((msg as any).txn).toEqual({ steps: [] });
  });

  it("cancelSchedule / pauseSchedule / resumeSchedule / listSchedules shapes", () => {
    const cancel: ClientMessage = {
      type: "cancelSchedule",
      scheduleId: "s1",
      id: "job-1",
    };
    const pause: ClientMessage = {
      type: "pauseSchedule",
      scheduleId: "s1",
      id: "job-1",
    };
    const resume: ClientMessage = {
      type: "resumeSchedule",
      scheduleId: "s1",
      id: "job-1",
    };
    const list: ClientMessage = { type: "listSchedules", scheduleId: "s1" };

    expect(cancel).toEqual({
      type: "cancelSchedule",
      scheduleId: "s1",
      id: "job-1",
    });
    expect(pause).toEqual({
      type: "pauseSchedule",
      scheduleId: "s1",
      id: "job-1",
    });
    expect(resume).toEqual({
      type: "resumeSchedule",
      scheduleId: "s1",
      id: "job-1",
    });
    expect(list).toEqual({ type: "listSchedules", scheduleId: "s1" });
  });

  it("scheduleOk / scheduleErr / scheduleAck / listSchedulesOk shapes", () => {
    const ok: ServerMessage = {
      type: "scheduleOk",
      scheduleId: "s1",
      id: "job-9",
    };
    const err: ServerMessage = {
      type: "scheduleErr",
      scheduleId: "s1",
      error: { code: "BAD_REQUEST", message: "nope" },
    };
    const ackOk: ServerMessage = {
      type: "scheduleAck",
      scheduleId: "s1",
      ok: true,
    };
    const ackErr: ServerMessage = {
      type: "scheduleAck",
      scheduleId: "s1",
      ok: false,
      error: { code: "NOT_FOUND", message: "missing" },
    };
    const list: ServerMessage = {
      type: "listSchedulesOk",
      scheduleId: "s1",
      schedules: [],
    };

    expect(ok).toEqual({
      type: "scheduleOk",
      scheduleId: "s1",
      id: "job-9",
    });
    expect(err).toEqual({
      type: "scheduleErr",
      scheduleId: "s1",
      error: { code: "BAD_REQUEST", message: "nope" },
    });
    // scheduleAck with ok=true omits the optional `error` field (wire shape).
    expect(ackOk).toEqual({ type: "scheduleAck", scheduleId: "s1", ok: true });
    expect(ackErr).toEqual({
      type: "scheduleAck",
      scheduleId: "s1",
      ok: false,
      error: { code: "NOT_FOUND", message: "missing" },
    });
    expect(list).toEqual({
      type: "listSchedulesOk",
      scheduleId: "s1",
      schedules: [],
    });
  });

  it("ScheduleInfo shape (cron/everyMs/lastError optional, external required)", () => {
    const oneshot: ScheduleInfo = {
      id: "job-1",
      kind: "oneshot",
      dueAt: 1700000000000,
      status: "pending",
      createdAt: 1700000000000,
      firedCount: 0,
      external: false,
    };
    const cronErr: ScheduleInfo = {
      id: "job-2",
      kind: "cron",
      dueAt: 1700000000000,
      cron: "*/5 * * * *",
      status: "error",
      lastError: "boom",
      createdAt: 1700000000000,
      firedCount: 3,
      external: false,
    };
    const interval: ScheduleInfo = {
      id: "job-3",
      kind: "interval",
      dueAt: 1700000000000,
      everyMs: 300000,
      status: "pending",
      createdAt: 1700000000000,
      firedCount: 1,
      external: false,
    };
    const external: ScheduleInfo = {
      id: "job-4",
      kind: "oneshot",
      dueAt: 1700000000000,
      status: "running",
      createdAt: 1700000000000,
      firedCount: 0,
      external: true,
    };

    // oneshot omits the optional cron/everyMs/lastError fields entirely.
    expect(oneshot).toEqual({
      id: "job-1",
      kind: "oneshot",
      dueAt: 1700000000000,
      status: "pending",
      createdAt: 1700000000000,
      firedCount: 0,
      external: false,
    });
    expect(cronErr).toEqual({
      id: "job-2",
      kind: "cron",
      dueAt: 1700000000000,
      cron: "*/5 * * * *",
      status: "error",
      lastError: "boom",
      createdAt: 1700000000000,
      firedCount: 3,
      external: false,
    });
    expect(interval).toEqual({
      id: "job-3",
      kind: "interval",
      dueAt: 1700000000000,
      everyMs: 300000,
      status: "pending",
      createdAt: 1700000000000,
      firedCount: 1,
      external: false,
    });
    // The external-claim flag is REQUIRED (always serialized by the server).
    expect(external).toEqual({
      id: "job-4",
      kind: "oneshot",
      dueAt: 1700000000000,
      status: "running",
      createdAt: 1700000000000,
      firedCount: 0,
      external: true,
    });
  });

  describe("wire-corpus/http-shapes.json field parity (ClaimedSchedule)", () => {
    const shapes = loadHttpShapes();

    it("claimedScheduleCron decodes as the exported ClaimedSchedule literal", () => {
      // Typed literal: every fixture key must exist on the interface (tsc
      // rejects unknown keys on an exhaustively-typed literal).
      const cron: ClaimedSchedule = {
        id: "job_cron_1",
        kind: "cron",
        dueAt: 1794470400000,
        txn: { steps: [] },
        cron: "0 5 * * *",
        tz: "America/Los_Angeles",
        leaseGeneration: 7,
        leaseDeadlineMs: 1794470500000,
      };
      // toEqual is order-insensitive; exact key-set equality (extra or
      // missing fixture keys fail).
      expect(cron).toEqual(shapes.claimedScheduleCron);
    });

    it("claimedScheduleOneshot omits cron/tz/everyMs entirely", () => {
      const oneshot: ClaimedSchedule = {
        id: "job_oneshot_1",
        kind: "oneshot",
        dueAt: 1794470400000,
        txn: { steps: [] },
        leaseGeneration: 8,
        leaseDeadlineMs: 1794470500000,
      };
      expect(oneshot).toEqual(shapes.claimedScheduleOneshot);
      // The oneshot fixture carries no cron/tz/everyMs keys at all.
      expect(Object.hasOwn(shapes.claimedScheduleOneshot, "cron")).toBe(false);
      expect(Object.hasOwn(shapes.claimedScheduleOneshot, "tz")).toBe(false);
      expect(Object.hasOwn(shapes.claimedScheduleOneshot, "everyMs")).toBe(false);
    });

    it("claimSchedulesResponse.jobs[0] equals the cron claim instance", () => {
      expect(shapes.claimSchedulesResponse).toEqual({ jobs: [shapes.claimedScheduleCron] });
    });
  });
});
