import { describe, expect, it } from "vitest";
import { RtDbClient, type WebSocketLike } from "../src/client.js";
import { mutation } from "../src/mutation.js";
import type {
  ClientMessage,
  ServerMessage,
  StepJson,
  StepOutcome,
  WorkflowInfo,
  WorkflowInfoFull,
  WorkflowSpec,
  WorkflowStatus,
} from "../src/protocol.js";

/** Asserts `value` is JSON-safe and round-trips through JSON unchanged — the
 * same technique the shared wire-corpus test uses, applied to FM-29 fixtures
 * (the shared corpus JSON is server-owned; these fixtures live here until it
 * gains workflow sections). */
function assertJsonRoundTrip(value: unknown): void {
  expect(JSON.parse(JSON.stringify(value))).toStrictEqual(value);
}

describe("workflow wire types (FM-29)", () => {
  it("WorkflowSpec carries name + steps; optional policy fields are omitted when absent", () => {
    const minimal: WorkflowSpec = {
      name: "onboard",
      steps: [{ txn: { steps: [{ op: "insert", table: "items", doc: {} }] } }],
    };
    const full: WorkflowSpec = {
      name: "onboard",
      steps: [
        {
          txn: { steps: [] },
          retry: { maxAttempts: 5, initialRetryMs: 500, maxRetryMs: 30_000 },
          sleepBeforeMs: 60_000,
        },
      ],
    };

    // Optional keys are absent (not undefined) — the server's
    // skip_serializing_if keeps them off the wire, and JSON round-trip pins it.
    expect("retry" in minimal.steps[0]).toBe(false);
    expect("sleepBeforeMs" in full.steps[0]).toBe(true);
    for (const spec of [minimal, full]) {
      assertJsonRoundTrip(spec);
    }
  });

  it("WorkflowStatus serializes snake_case", () => {
    const statuses: WorkflowStatus[] = ["pending", "running", "success", "failed", "cancelled"];
    expect(statuses).toHaveLength(5);
  });

  it("StepOutcome shapes (success omits error; failed carries it)", () => {
    const ok: StepOutcome = { stepIndex: 0, status: "success", attempts: 1, at: 1700000000000 };
    const bad: StepOutcome = {
      stepIndex: 1,
      status: "failed",
      attempts: 3,
      at: 1700000000000,
      error: "boom",
    };
    expect("error" in ok).toBe(false);
    for (const o of [ok, bad]) {
      assertJsonRoundTrip(o);
    }
  });

  it("WorkflowInfo omits optional keys when absent", () => {
    const info: WorkflowInfo = {
      id: "wf1",
      name: "onboard",
      status: "pending",
      currentStep: 0,
      stepCount: 2,
      attempts: 0,
      sleepUntil: 1700000060000,
      createdAt: 1700000000000,
      updatedAt: 1700000000000,
    };
    expect("lastError" in info).toBe(false);
    expect("startedAt" in info).toBe(false);
    expect("finishedAt" in info).toBe(false);
    assertJsonRoundTrip(info);
  });

  it("WorkflowInfoFull flattens the info row and adds stepOutcomes", () => {
    const full: WorkflowInfoFull = {
      id: "wf1",
      name: "onboard",
      status: "success",
      currentStep: 2,
      stepCount: 2,
      attempts: 0,
      createdAt: 1700000000000,
      updatedAt: 1700000060000,
      startedAt: 1700000000000,
      finishedAt: 1700000060000,
      stepOutcomes: [
        { stepIndex: 0, status: "success", attempts: 1, at: 1700000010000 },
        { stepIndex: 1, status: "success", attempts: 1, at: 1700000060000 },
      ],
    };
    // Flattened: the info fields sit at the top level, not under `info`.
    expect(full.id).toBe("wf1");
    assertJsonRoundTrip(full);
  });

  it("startWorkflow / cancelWorkflow StepJson entries (tag op, camelCase)", () => {
    const start: StepJson = {
      op: "startWorkflow",
      spec: { name: "n", steps: [{ txn: { steps: [] } }] },
    };
    const cancel: StepJson = { op: "cancelWorkflow", id: "wf1" };
    expect(start).toEqual({
      op: "startWorkflow",
      spec: { name: "n", steps: [{ txn: { steps: [] } }] },
    });
    expect(cancel).toEqual({ op: "cancelWorkflow", id: "wf1" });
  });

  it("ClientMessage startWorkflow / cancelWorkflow / listWorkflows shapes", () => {
    const start: ClientMessage = {
      type: "startWorkflow",
      workflowId: "w1",
      spec: { name: "n", steps: [{ txn: { steps: [] } }] },
    };
    const cancel: ClientMessage = { type: "cancelWorkflow", workflowId: "w1", id: "wf1" };
    const listAll: ClientMessage = { type: "listWorkflows", workflowId: "w1" };
    const listFiltered: ClientMessage = {
      type: "listWorkflows",
      workflowId: "w1",
      status: "running",
    };

    expect(start).toEqual({
      type: "startWorkflow",
      workflowId: "w1",
      spec: { name: "n", steps: [{ txn: { steps: [] } }] },
    });
    expect(cancel).toEqual({ type: "cancelWorkflow", workflowId: "w1", id: "wf1" });
    // An unfiltered list omits `status` entirely (skip_serializing_if parity).
    expect(listAll).toEqual({ type: "listWorkflows", workflowId: "w1" });
    expect(listFiltered).toEqual({ type: "listWorkflows", workflowId: "w1", status: "running" });
  });

  it("ServerMessage startWorkflowOk / startWorkflowErr / workflowAck / listWorkflowsOk shapes", () => {
    const ok: ServerMessage = {
      type: "startWorkflowOk",
      workflowId: "w1",
      info: {
        id: "wf1",
        name: "n",
        status: "pending",
        currentStep: 0,
        stepCount: 1,
        attempts: 0,
        sleepUntil: 1700000000000,
        createdAt: 1700000000000,
        updatedAt: 1700000000000,
      },
    };
    const err: ServerMessage = {
      type: "startWorkflowErr",
      workflowId: "w1",
      error: { code: "BAD_REQUEST", message: "workflow must have at least one step" },
    };
    // The server's ListWorkflows failure reply is also typed StartWorkflowErr
    // (the frame vocabulary has no listWorkflowsErr — the listSchedules
    // precedent), so the client's list error path keys off this frame.
    const listErr: ServerMessage = {
      type: "startWorkflowErr",
      workflowId: "w2",
      error: { code: "FORBIDDEN", message: "nope" },
    };
    const ackOk: ServerMessage = { type: "workflowAck", workflowId: "w1", ok: true };
    const ackFalse: ServerMessage = { type: "workflowAck", workflowId: "w1", ok: false };
    const listOk: ServerMessage = { type: "listWorkflowsOk", workflowId: "w1", workflows: [] };

    expect(ok.type).toBe("startWorkflowOk");
    expect(err).toEqual({
      type: "startWorkflowErr",
      workflowId: "w1",
      error: { code: "BAD_REQUEST", message: "workflow must have at least one step" },
    });
    expect(listErr.type).toBe("startWorkflowErr");
    // workflowAck with ok=true omits the optional `error` field.
    expect(ackOk).toEqual({ type: "workflowAck", workflowId: "w1", ok: true });
    expect(ackFalse).toEqual({ type: "workflowAck", workflowId: "w1", ok: false });
    expect(listOk).toEqual({ type: "listWorkflowsOk", workflowId: "w1", workflows: [] });
  });
});

describe("TxnBuilder workflow steps (FM-29)", () => {
  it("builds startWorkflow + cancelWorkflow steps with the wire shapes", () => {
    const spec: WorkflowSpec = {
      name: "onboard",
      steps: [{ txn: { steps: [{ op: "insert", table: "items", doc: {} }] } }],
    };
    const txn = mutation().startWorkflow(spec).cancelWorkflow("wf1").build();

    expect(txn).toEqual({
      steps: [
        { op: "startWorkflow", spec },
        { op: "cancelWorkflow", id: "wf1" },
      ],
    });
  });
});

/** A controllable fake socket (the client.test.ts pattern). */
class FakeSocket implements WebSocketLike {
  onopen: (() => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onclose: ((ev: { code: number; reason: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  readonly sent: string[] = [];
  closed = false;

  send(data: string): void {
    this.sent.push(data);
  }
  close(code = 1000, reason = ""): void {
    this.closed = true;
    this.onclose?.({ code, reason });
  }
  open(): void {
    this.onopen?.();
  }
  deliver(msg: unknown): void {
    this.onmessage?.({ data: JSON.stringify(msg) });
  }
  get sentParsed(): unknown[] {
    return this.sent.map((s) => JSON.parse(s));
  }
}

function newClient() {
  const sockets: FakeSocket[] = [];
  const client = new RtDbClient({
    url: "ws://h:8300",
    db: "kanban",
    getToken: () => "tok",
    webSocketFactory: () => {
      const s = new FakeSocket();
      sockets.push(s);
      return s;
    },
    heartbeatMs: 0,
    now: () => 0,
    random: () => 0.5,
    setTimeoutImpl: () => 0 as unknown as ReturnType<typeof setTimeout>,
    clearTimeoutImpl: () => {},
  });
  return { client, sockets };
}

const frames = (s: FakeSocket) => s.sentParsed as Array<{ type: string; [k: string]: unknown }>;

const spec: WorkflowSpec = {
  name: "onboard",
  steps: [{ txn: { steps: [{ op: "insert", table: "items", doc: {} }] } }],
};

const info: WorkflowInfo = {
  id: "wf1",
  name: "onboard",
  status: "pending",
  currentStep: 0,
  stepCount: 1,
  attempts: 0,
  sleepUntil: 0,
  createdAt: 0,
  updatedAt: 0,
};

describe("RtDbClient workflow methods (FM-29)", () => {
  it("sends startWorkflow and resolves the info on startWorkflowOk", async () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "machine" } });

    const promise = client.startWorkflow(spec);
    const frame = frames(sockets[0]).find((m) => m.type === "startWorkflow") as unknown as {
      workflowId: string;
      spec: unknown;
    };
    expect(frame.workflowId).toMatch(/^wf-\d+$/);
    expect(frame.spec).toEqual(spec);
    sockets[0].deliver({ type: "startWorkflowOk", workflowId: frame.workflowId, info });
    await expect(promise).resolves.toEqual(info);
  });

  it("rejects startWorkflow on startWorkflowErr", async () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "machine" } });

    const promise = client.startWorkflow(spec);
    const frame = frames(sockets[0]).find((m) => m.type === "startWorkflow") as unknown as {
      workflowId: string;
    };
    sockets[0].deliver({
      type: "startWorkflowErr",
      workflowId: frame.workflowId,
      error: { code: "BAD_REQUEST", message: "workflow must have at least one step" },
    });
    await expect(promise).rejects.toMatchObject({
      name: "RtDbError",
      code: "BAD_REQUEST",
    });
  });

  it("cancelWorkflow resolves true on workflowAck.ok:true and false on a bare ok:false", async () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "machine" } });

    const okPromise = client.cancelWorkflow("wf1");
    const okFrame = frames(sockets[0]).find((m) => m.type === "cancelWorkflow") as unknown as {
      workflowId: string;
      id: string;
    };
    expect(okFrame.id).toBe("wf1");
    sockets[0].deliver({ type: "workflowAck", workflowId: okFrame.workflowId, ok: true });
    await expect(okPromise).resolves.toBe(true);

    // A missing/already-terminal run is ok:false with NO error — resolve false.
    const falsePromise = client.cancelWorkflow("wf-gone");
    const falseFrame = frames(sockets[0])
      .filter((m) => m.type === "cancelWorkflow")
      .at(-1) as unknown as { workflowId: string };
    sockets[0].deliver({ type: "workflowAck", workflowId: falseFrame.workflowId, ok: false });
    await expect(falsePromise).resolves.toBe(false);
  });

  it("cancelWorkflow rejects on workflowAck ok:false with an error", async () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "machine" } });

    const promise = client.cancelWorkflow("wf1");
    const frame = frames(sockets[0]).find((m) => m.type === "cancelWorkflow") as unknown as {
      workflowId: string;
    };
    sockets[0].deliver({
      type: "workflowAck",
      workflowId: frame.workflowId,
      ok: false,
      error: { code: "FORBIDDEN", message: "read-only token cannot mutate" },
    });
    await expect(promise).rejects.toMatchObject({ name: "RtDbError", code: "FORBIDDEN" });
  });

  it("listWorkflows resolves the array on listWorkflowsOk (status omitted unless given)", async () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "machine" } });

    const allPromise = client.listWorkflows();
    const allFrame = frames(sockets[0]).find((m) => m.type === "listWorkflows") as unknown as {
      workflowId: string;
      status?: unknown;
    };
    expect("status" in allFrame).toBe(false);
    sockets[0].deliver({
      type: "listWorkflowsOk",
      workflowId: allFrame.workflowId,
      workflows: [info],
    });
    await expect(allPromise).resolves.toEqual([info]);

    const filteredPromise = client.listWorkflows("running");
    const filteredFrame = frames(sockets[0])
      .filter((m) => m.type === "listWorkflows")
      .at(-1) as unknown as { workflowId: string; status?: string };
    expect(filteredFrame.status).toBe("running");
    sockets[0].deliver({
      type: "listWorkflowsOk",
      workflowId: filteredFrame.workflowId,
      workflows: [],
    });
    await expect(filteredPromise).resolves.toEqual([]);
  });

  it("listWorkflows rejects when the failure arrives typed startWorkflowErr (no listWorkflowsErr frame)", async () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "machine" } });

    const promise = client.listWorkflows();
    const frame = frames(sockets[0]).find((m) => m.type === "listWorkflows") as unknown as {
      workflowId: string;
    };
    sockets[0].deliver({
      type: "startWorkflowErr",
      workflowId: frame.workflowId,
      error: { code: "FORBIDDEN", message: "nope" },
    });
    await expect(promise).rejects.toMatchObject({ name: "RtDbError", code: "FORBIDDEN" });
  });
});
