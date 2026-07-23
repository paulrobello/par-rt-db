import { describe, expect, it } from "vitest";
import type { ClientMessage, ScheduleInfo, ScheduleWhen, ServerMessage } from "../src/protocol.js";

describe("schedule wire types", () => {
  it("ScheduleWhen mirrors server tags (afterMs / runAt / cron)", () => {
    const afterMs: ScheduleWhen = { type: "afterMs", ms: 100 };
    const runAt: ScheduleWhen = { type: "runAt", ms: 9 };
    const cron: ScheduleWhen = { type: "cron", expr: "*/5 * * * *" };

    expect(afterMs).toEqual({ type: "afterMs", ms: 100 });
    expect(runAt).toEqual({ type: "runAt", ms: 9 });
    expect(cron).toEqual({ type: "cron", expr: "*/5 * * * *" });
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

  it("ScheduleInfo shape (cron + lastError optional)", () => {
    const oneshot: ScheduleInfo = {
      id: "job-1",
      kind: "oneshot",
      dueAt: 1700000000000,
      status: "pending",
      createdAt: 1700000000000,
      firedCount: 0,
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
    };

    // oneshot omits the optional cron/lastError fields entirely.
    expect(oneshot).toEqual({
      id: "job-1",
      kind: "oneshot",
      dueAt: 1700000000000,
      status: "pending",
      createdAt: 1700000000000,
      firedCount: 0,
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
    });
  });
});
