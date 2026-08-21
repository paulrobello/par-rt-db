import { describe, expect, it } from "vitest";
import { RtDbClient, type WebSocketLike } from "../src/client.js";
import { InMemoryRtDbClient } from "../src/in_memory/index.js";
import { awaitSignal, mutation } from "../src/mutation.js";
import type {
  ClientMessage,
  ServerMessage,
  StepJson,
  StepOutcome,
  TransactionJson,
  WorkflowInfo,
  WorkflowInfoFull,
  WorkflowSpec,
  WorkflowStatus,
} from "../src/protocol.js";
import { createApi } from "../src/query.js";
import { defineSchema, defineTable, t } from "../src/schema.js";

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
    expect("awaitSignal" in minimal.steps[0]).toBe(false);
    for (const spec of [minimal, full]) {
      assertJsonRoundTrip(spec);
    }
  });

  it("awaitSignal step: exactly one of txn/awaitSignal, timeoutMs omitted when absent", () => {
    const gated: WorkflowSpec = {
      name: "approval",
      steps: [
        { awaitSignal: { name: "approve", timeoutMs: 3_600_000 } },
        { txn: { steps: [] }, sleepBeforeMs: 5_000 },
      ],
    };
    const forever: WorkflowSpec = {
      name: "approval",
      steps: [{ awaitSignal: { name: "approve" }, retry: { maxAttempts: 2 } }],
    };
    const gatedStep = gated.steps[0];
    const foreverStep = forever.steps[0];

    expect(gatedStep.awaitSignal).toEqual({ name: "approve", timeoutMs: 3_600_000 });
    expect("txn" in gatedStep).toBe(false);
    // An omitted timeoutMs (wait indefinitely) stays off the wire.
    expect("timeoutMs" in (foreverStep.awaitSignal ?? {})).toBe(false);
    expect("txn" in foreverStep).toBe(false);
    // Policy fields coexist with an awaitSignal step like with a txn step.
    expect(foreverStep.retry).toEqual({ maxAttempts: 2 });
    expect(gated.steps[1].sleepBeforeMs).toBe(5_000);
    for (const spec of [gated, forever]) {
      assertJsonRoundTrip(spec);
    }
  });

  it("WorkflowStatus serializes snake_case", () => {
    const statuses: WorkflowStatus[] = [
      "pending",
      "running",
      "success",
      "failed",
      "cancelled",
      "waiting",
    ];
    expect(statuses).toHaveLength(6);
  });

  it("StepOutcome shapes (success omits error; failed carries it; signal only on delivery)", () => {
    const ok: StepOutcome = { stepIndex: 0, status: "success", attempts: 1, at: 1700000000000 };
    const bad: StepOutcome = {
      stepIndex: 1,
      status: "failed",
      attempts: 3,
      at: 1700000000000,
      error: "boom",
    };
    const signalled: StepOutcome = {
      stepIndex: 2,
      status: "success",
      attempts: 2,
      at: 1700000000000,
      signal: { v: 1, note: null },
    };
    expect("error" in ok).toBe(false);
    // The delivered payload rides the outcome verbatim — omitted otherwise.
    expect("signal" in ok).toBe(false);
    expect("signal" in bad).toBe(false);
    expect(signalled.signal).toEqual({ v: 1, note: null });
    for (const o of [ok, bad, signalled]) {
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
    expect("waitingFor" in info).toBe(false);
    expect("waitedSince" in info).toBe(false);
    expect("startedAt" in info).toBe(false);
    expect("finishedAt" in info).toBe(false);
    assertJsonRoundTrip(info);
  });

  it("WorkflowInfo carries waitingFor/waitedSince only while waiting", () => {
    const waiting: WorkflowInfo = {
      id: "wf1",
      name: "onboard",
      status: "waiting",
      currentStep: 1,
      stepCount: 2,
      attempts: 0,
      sleepUntil: 1700000060000,
      waitingFor: "approve",
      waitedSince: 1700000001234,
      createdAt: 1700000000000,
      updatedAt: 1700000001234,
      startedAt: 1700000000000,
    };
    expect(waiting.waitingFor).toBe("approve");
    expect(waiting.waitedSince).toBe(1700000001234);
    assertJsonRoundTrip(waiting);
  });

  it("WorkflowInfoFull flattens the info row and adds stepOutcomes", () => {
    const full: WorkflowInfoFull = {
      id: "wf1",
      name: "onboard",
      status: "success",
      currentStep: 1,
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

  it("ClientMessage startWorkflow / cancelWorkflow / signalWorkflow / listWorkflows shapes", () => {
    const start: ClientMessage = {
      type: "startWorkflow",
      workflowId: "w1",
      spec: { name: "n", steps: [{ txn: { steps: [] } }] },
    };
    const cancel: ClientMessage = { type: "cancelWorkflow", workflowId: "w1", id: "wf1" };
    const signalBare: ClientMessage = {
      type: "signalWorkflow",
      workflowId: "w1",
      id: "wf1",
      name: "approve",
    };
    const signalWithPayload: ClientMessage = {
      type: "signalWorkflow",
      workflowId: "w1",
      id: "wf1",
      name: "approve",
      payload: { v: 1 },
    };
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
    // The reply reuses workflowAck — signalWorkflow adds no server frame.
    expect(signalBare).toEqual({
      type: "signalWorkflow",
      workflowId: "w1",
      id: "wf1",
      name: "approve",
    });
    expect(signalWithPayload).toEqual({
      type: "signalWorkflow",
      workflowId: "w1",
      id: "wf1",
      name: "approve",
      payload: { v: 1 },
    });
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

  it("awaitSignal() builds wait steps with the wire shapes (timeoutMs omitted when absent)", () => {
    expect(awaitSignal("approve")).toEqual({ awaitSignal: { name: "approve" } });
    expect(awaitSignal("approve", 3_600_000)).toEqual({
      awaitSignal: { name: "approve", timeoutMs: 3_600_000 },
    });

    // Drops into a spec alongside txn steps — exactly one of txn/awaitSignal
    // per step is the server's submit-time contract.
    const spec: WorkflowSpec = {
      name: "approval",
      steps: [
        { txn: { steps: [{ op: "insert", table: "items", doc: {} }] } },
        awaitSignal("approve", 60_000),
      ],
    };
    expect(spec.steps[1]).toEqual({ awaitSignal: { name: "approve", timeoutMs: 60_000 } });
    assertJsonRoundTrip(spec);
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

  it("signalWorkflow sends the frame (payload omitted when absent) and resolves true on ack", async () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "machine" } });

    const barePromise = client.signalWorkflow("wf1", "approve");
    const bareFrame = frames(sockets[0]).find((m) => m.type === "signalWorkflow") as unknown as {
      workflowId: string;
      id: string;
      name: string;
      payload?: unknown;
    };
    expect(bareFrame.workflowId).toMatch(/^wf-\d+$/);
    expect(bareFrame.id).toBe("wf1");
    expect(bareFrame.name).toBe("approve");
    expect("payload" in bareFrame).toBe(false);
    sockets[0].deliver({ type: "workflowAck", workflowId: bareFrame.workflowId, ok: true });
    await expect(barePromise).resolves.toBe(true);

    const payloadPromise = client.signalWorkflow("wf1", "approve", { v: 2 });
    const payloadFrame = frames(sockets[0])
      .filter((m) => m.type === "signalWorkflow")
      .at(-1) as unknown as { workflowId: string; payload: unknown };
    expect(payloadFrame.payload).toEqual({ v: 2 });
    sockets[0].deliver({ type: "workflowAck", workflowId: payloadFrame.workflowId, ok: true });
    await expect(payloadPromise).resolves.toBe(true);
  });

  it("signalWorkflow rejects on the ack's typed error envelope (same convention as cancel)", async () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "machine" } });

    const promise = client.signalWorkflow("wf1", "wrong");
    const frame = frames(sockets[0]).find((m) => m.type === "signalWorkflow") as unknown as {
      workflowId: string;
    };
    sockets[0].deliver({
      type: "workflowAck",
      workflowId: frame.workflowId,
      ok: false,
      error: {
        code: "CONFLICT",
        message: "workflow waiting on 'approve', got 'wrong'",
      },
    });
    await expect(promise).rejects.toMatchObject({ name: "RtDbError", code: "CONFLICT" });
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

// ---- awaitSignal engine (in-memory harness) ----------------------------------

const engineSchema = defineSchema({
  items: defineTable({ name: t.string() }),
});
const engineApi = createApi(engineSchema);

const BASE = 1_700_000_000_000;

function newEngine() {
  let ms = BASE;
  const c = new InMemoryRtDbClient({ now: () => ms, random: () => 0 });
  c.pushSchema(engineSchema);
  return { c, setNow: (v: number) => (ms = v) };
}

const insertTxn = (name: string): TransactionJson => ({
  steps: [{ op: "insert", table: "items", doc: { name } }],
});

const itemNames = async (c: InMemoryRtDbClient) =>
  (await c.query(engineApi.items.query().collect())).map((d) => (d as { name: string }).name);

describe("InMemoryRtDbClient awaitSignal steps", () => {
  it("parks at an awaitSignal step, then a delivered payload advances the run", async () => {
    const { c, setNow } = newEngine();
    setNow(BASE);
    const { id } = await c.startWorkflow({
      name: "approval",
      steps: [
        { txn: insertTxn("one") },
        { awaitSignal: { name: "approve", timeoutMs: 30_000 } },
        { txn: insertTxn("two") },
      ],
    });

    c.tick();
    const parked = (await c.listWorkflows()).find((w) => w.id === id);
    expect(parked).toMatchObject({
      status: "waiting",
      currentStep: 1,
      waitingFor: "approve",
      waitedSince: BASE,
    });
    expect(await itemNames(c)).toEqual(["one"]);

    setNow(BASE + 1_000);
    expect(await c.signalWorkflow(id, "approve", { v: 1 })).toBe(true);
    c.tick();
    const done = await c.getWorkflow(id);
    expect(done.status).toBe("success");
    expect(done.waitingFor).toBeUndefined();
    expect("waitingFor" in done).toBe(false);
    expect(done.stepOutcomes[1]).toEqual({
      stepIndex: 1,
      status: "success",
      attempts: 1,
      at: BASE + 1_000,
      signal: { v: 1 },
    });
    expect(await itemNames(c)).toEqual(["one", "two"]);
  });

  it("times out, re-parks with a FRESH full timeout (not backoff), then succeeds on a signal", async () => {
    const { c, setNow } = newEngine();
    setNow(BASE);
    // Backoff would be 60s here — the re-park gate pins the full 5s timeout.
    const { id } = await c.startWorkflow({
      name: "slow-approval",
      steps: [
        {
          awaitSignal: { name: "approve", timeoutMs: 5_000 },
          retry: { maxAttempts: 3, initialRetryMs: 60_000, maxRetryMs: 60_000 },
        },
      ],
    });

    c.tick();
    expect((await c.listWorkflows()).find((w) => w.id === id)).toMatchObject({
      status: "waiting",
      waitedSince: BASE,
    });

    setNow(BASE + 5_000);
    c.tick(); // gate expired: timeout attempt 1
    const reParked = (await c.listWorkflows()).find((w) => w.id === id);
    expect(reParked).toMatchObject({ status: "waiting", currentStep: 0, attempts: 1 });
    expect(reParked?.waitedSince).toBe(BASE + 5_000);
    // Fresh full timeoutMs from the re-park time — never the 60s backoff.
    expect(reParked?.sleepUntil).toBe(BASE + 10_000);

    setNow(BASE + 6_000);
    expect(await c.signalWorkflow(id, "approve", "finally")).toBe(true);
    c.tick();
    const done = await c.getWorkflow(id);
    expect(done.status).toBe("success");
    expect(done.stepOutcomes[0]).toEqual({
      stepIndex: 0,
      status: "success",
      attempts: 2, // one timed-out attempt + the delivery
      at: BASE + 6_000,
      signal: "finally",
    });
  });

  it("fails terminally at timeout exhaustion; later steps never execute", async () => {
    const { c, setNow } = newEngine();
    setNow(BASE);
    const { id } = await c.startWorkflow({
      name: "never-approved",
      steps: [
        { awaitSignal: { name: "approve", timeoutMs: 5_000 }, retry: { maxAttempts: 2 } },
        { txn: insertTxn("never") },
      ],
    });

    c.tick(); // park
    setNow(BASE + 5_000);
    c.tick(); // timeout attempt 1 → re-park
    setNow(BASE + 10_000);
    c.tick(); // timeout attempt 2 → exhausted

    const failed = await c.getWorkflow(id);
    expect(failed.status).toBe("failed");
    expect(failed.lastError).toBe("awaitSignal 'approve' timed out");
    expect(failed.stepOutcomes[0]).toEqual({
      stepIndex: 0,
      status: "failed",
      attempts: 2,
      at: BASE + 10_000,
      error: "awaitSignal 'approve' timed out",
    });
    expect("signal" in failed.stepOutcomes[0]).toBe(false);
    expect("waitingFor" in failed).toBe(false);
    expect(await itemNames(c)).toEqual([]);

    // Terminal: later ticks change nothing.
    setNow(BASE + 60_000);
    c.tick();
    expect((await c.getWorkflow(id)).status).toBe("failed");
  });

  it("an omitted timeoutMs waits forever — only a delivery (or cancel) wakes the run", async () => {
    const { c, setNow } = newEngine();
    setNow(BASE);
    const { id } = await c.startWorkflow({
      name: "forever",
      steps: [{ awaitSignal: { name: "forever" } }, { txn: insertTxn("after") }],
    });

    c.tick();
    expect((await c.listWorkflows()).find((w) => w.id === id)).toMatchObject({
      status: "waiting",
      waitingFor: "forever",
    });

    setNow(BASE + 10 * 365 * 24 * 60 * 60 * 1000); // ten years later
    c.tick();
    expect((await c.listWorkflows()).find((w) => w.id === id)?.status).toBe("waiting");
    expect(await itemNames(c)).toEqual([]);

    expect(await c.signalWorkflow(id, "forever", { late: true })).toBe(true);
    c.tick();
    const done = await c.getWorkflow(id);
    expect(done.status).toBe("success");
    expect(done.stepOutcomes[0].signal).toEqual({ late: true });
  });

  it("latest-wins: two deliveries both ack and the SECOND payload is consumed", async () => {
    const { c, setNow } = newEngine();
    setNow(BASE);
    const { id } = await c.startWorkflow({
      name: "latest",
      steps: [{ awaitSignal: { name: "approve" } }],
    });

    c.tick(); // park
    expect(await c.signalWorkflow(id, "approve", { v: 1 })).toBe(true);
    expect(await c.signalWorkflow(id, "approve", { v: 2 })).toBe(true);
    c.tick();
    const done = await c.getWorkflow(id);
    expect(done.status).toBe("success");
    expect(done.stepOutcomes[0].signal).toEqual({ v: 2 });
  });

  it("cancel while waiting flips to cancelled and a late signal conflicts", async () => {
    const { c, setNow } = newEngine();
    setNow(BASE);
    const { id } = await c.startWorkflow({
      name: "cancel-wait",
      steps: [{ awaitSignal: { name: "approve", timeoutMs: 60_000 } }, { txn: insertTxn("after") }],
    });

    c.tick(); // park
    expect(await c.cancelWorkflow(id)).toBe(true);
    const cancelled = (await c.listWorkflows()).find((w) => w.id === id);
    expect(cancelled).toMatchObject({ status: "cancelled", finishedAt: BASE });
    expect("waitingFor" in (cancelled ?? {})).toBe(false);

    await expect(c.signalWorkflow(id, "approve")).rejects.toMatchObject({
      name: "RtDbError",
      code: "CONFLICT",
      message: "workflow is not waiting for a signal",
    });
    setNow(BASE + 120_000);
    c.tick(); // cancelled runs never advance, even past the gate
    expect(await itemNames(c)).toEqual([]);
  });

  it("typed delivery errors: unknown id NOT_FOUND; name mismatch CONFLICT naming both", async () => {
    const { c, setNow } = newEngine();
    setNow(BASE);
    const { id } = await c.startWorkflow({
      name: "typed",
      steps: [{ awaitSignal: { name: "approve" } }],
    });

    await expect(c.signalWorkflow("nope", "approve")).rejects.toMatchObject({
      name: "RtDbError",
      code: "NOT_FOUND",
      message: "unknown workflow",
    });

    c.tick(); // park on 'approve'
    await expect(c.signalWorkflow(id, "wrong", 1)).rejects.toMatchObject({
      name: "RtDbError",
      code: "CONFLICT",
      message: "workflow waiting on 'approve', got 'wrong'",
    });
  });

  it("signaling a non-waiting (pending, never parked) run conflicts", async () => {
    const { c, setNow } = newEngine();
    setNow(BASE);
    const { id } = await c.startWorkflow({
      name: "fresh",
      steps: [{ awaitSignal: { name: "approve" } }],
    });
    // No tick yet: the run is pending and has never parked.
    await expect(c.signalWorkflow(id, "approve")).rejects.toMatchObject({
      name: "RtDbError",
      code: "CONFLICT",
      message: "workflow is not waiting for a signal",
    });
  });

  it("submit validation ports the server's exactly-one-of and bounds checks", async () => {
    const { c } = newEngine();
    const both = {
      name: "both",
      steps: [{ txn: insertTxn("x"), awaitSignal: { name: "a" } }],
    } as unknown as WorkflowSpec;
    await expect(c.startWorkflow(both)).rejects.toMatchObject({
      code: "BAD_REQUEST",
      message: "steps[0] must carry exactly one of txn or awaitSignal",
    });

    const neither = { name: "none", steps: [{}] } as unknown as WorkflowSpec;
    await expect(c.startWorkflow(neither)).rejects.toMatchObject({
      code: "BAD_REQUEST",
      message: "steps[0] must carry exactly one of txn or awaitSignal",
    });

    const emptyName = { name: "e", steps: [awaitSignal("")] };
    await expect(c.startWorkflow(emptyName)).rejects.toMatchObject({
      code: "BAD_REQUEST",
      message: "steps[0].awaitSignal.name must be 1..=256 chars",
    });

    const longName = { name: "l", steps: [awaitSignal("a".repeat(257))] };
    await expect(c.startWorkflow(longName)).rejects.toMatchObject({
      code: "BAD_REQUEST",
      message: "steps[0].awaitSignal.name must be 1..=256 chars",
    });

    const zeroTimeout = { name: "z", steps: [awaitSignal("a", 0)] };
    await expect(c.startWorkflow(zeroTimeout)).rejects.toMatchObject({
      code: "BAD_REQUEST",
      message: "steps[0].awaitSignal.timeoutMs must be > 0",
    });
  });

  it("sleepBeforeMs before an awaitSignal step gates the wait start", async () => {
    const { c, setNow } = newEngine();
    setNow(BASE);
    const { id } = await c.startWorkflow({
      name: "gated-wait",
      steps: [{ awaitSignal: { name: "approve", timeoutMs: 5_000 }, sleepBeforeMs: 10_000 }],
    });

    c.tick();
    expect((await c.listWorkflows()).find((w) => w.id === id)).toMatchObject({
      status: "pending", // gated by sleepBeforeMs, not yet parked
    });

    setNow(BASE + 10_000);
    c.tick(); // gate due → parks now
    const parked = (await c.listWorkflows()).find((w) => w.id === id);
    expect(parked).toMatchObject({ status: "waiting", waitedSince: BASE + 10_000 });
    expect(parked?.sleepUntil).toBe(BASE + 15_000);
  });
});
