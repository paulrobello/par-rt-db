import { act, render, screen, waitFor } from "@testing-library/react";
import type { OpEvent } from "@par-rt-db/client";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

// AdminProvider owns the dashboard's admin client, the database list, and the
// /admin/stream WebSocket (op feed + gauges). These tests drive the provider
// through a fake SDK client and a fake WebSocket and assert the context value
// a consumer sees: loading/error/connection state, newest-first op feed with
// its 100-event cap, gauge updates, and the sign-out reset.

const shared = vi.hoisted(() => ({
  method: "adminkey" as "adminkey" | "oauth" | null,
  listDbs: vi.fn<() => Promise<string[]>>(),
  metrics: vi.fn<() => Promise<unknown>>(),
}));

vi.mock("../session", () => ({
  useSession: () => ({ method: shared.method }),
}));

vi.mock("@par-rt-db/client", () => {
  class RtDbAdminClient {
    listDbs = shared.listDbs;
    metrics = shared.metrics;
  }
  class RtDbError extends Error {}
  return { RtDbAdminClient, RtDbError };
});

import { AdminProvider, useAdmin } from "../admin";

function Harness() {
  const v = useAdmin();
  return (
    <div>
      <div>conn={v.connection}</div>
      <div>dbs={v.databases.join(",")}</div>
      <div>dbsLoading={String(v.databasesLoading)}</div>
      <div>dbsError={String(v.databasesError)}</div>
      <div>ops={v.ops.length}</div>
      <div>opsTop={v.ops[0]?.docId ?? "-"}</div>
      <div>queries={String(v.metrics?.queriesTotal ?? -1)}</div>
      <div>metricsError={String(v.metricsError)}</div>
      <button type="button" onClick={() => void v.refreshDatabases()}>
        refresh-dbs
      </button>
    </div>
  );
}

class MockWebSocket {
  static instances: MockWebSocket[] = [];
  onopen: (() => void) | null = null;
  onmessage: ((e: { data: string }) => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;
  constructor(public url: string) {
    MockWebSocket.instances.push(this);
  }
  close() {
    /* noop: unmount cleanup must not trigger the reconnect path */
  }
}

// jsdom's WebSocket (if present) is uncontrollable; the provider must see the
// mock so tests can drive onopen/onmessage/onclose directly.
globalThis.WebSocket = MockWebSocket as unknown as typeof WebSocket;

function opEvent(n: number): OpEvent {
  return {
    db: "db1",
    table: "docs",
    docId: `op-${n}`,
    kind: "insert",
    ts: n,
    seq: n,
    feedEpoch: "e1",
  };
}

function lastSocket(): MockWebSocket {
  const ws = MockWebSocket.instances.at(-1);
  if (!ws) throw new Error("no WebSocket constructed");
  return ws;
}

async function send(ws: MockWebSocket, payload: unknown): Promise<void> {
  await act(async () => {
    ws.onmessage?.({ data: JSON.stringify(payload) });
    await Promise.resolve();
  });
}

function mount(): ReturnType<typeof render> {
  const r = render(
    <AdminProvider>
      <Harness />
    </AdminProvider>,
  );
  // The client is created during render; the effect's initial fetches happen
  // after. Wait for them to settle so per-test assertions see stable state.
  return r;
}

beforeEach(() => {
  shared.listDbs.mockReset().mockResolvedValue([]);
  shared.metrics.mockReset().mockResolvedValue({ queriesTotal: 0 });
  MockWebSocket.instances = [];
  shared.method = "adminkey";
});

describe("AdminProvider / useAdmin", () => {
  it("throws when useAdmin is used outside the provider", () => {
    const spy = vi.spyOn(console, "error").mockImplementation(() => {});
    expect(() => render(<Harness />)).toThrow("useAdmin must be used within AdminProvider");
    spy.mockRestore();
  });

  it("stays idle and opens no stream while signed out", async () => {
    shared.method = null;
    mount();
    await waitFor(() => {
      expect(screen.getByText("conn=idle")).toBeInTheDocument();
    });
    expect(shared.listDbs).not.toHaveBeenCalled();
    expect(shared.metrics).not.toHaveBeenCalled();
    expect(MockWebSocket.instances).toHaveLength(0);
  });

  it("loads databases and metrics and reaches connected on stream open", async () => {
    shared.listDbs.mockResolvedValue(["db1", "db2"]);
    shared.metrics.mockResolvedValue({ queriesTotal: 5 });
    mount();
    expect(await screen.findByText("dbs=db1,db2")).toBeInTheDocument();
    await waitFor(() => {
      expect(screen.getByText("conn=connecting")).toBeInTheDocument();
    });
    expect(lastSocket().url).toBe("ws://localhost:3000/admin/stream");
    await act(async () => {
      lastSocket().onopen?.();
    });
    expect(await screen.findByText("conn=connected")).toBeInTheDocument();
    expect(await screen.findByText("queries=5")).toBeInTheDocument();
    expect(screen.getByText("dbsError=null")).toBeInTheDocument();
  });

  it("pushes op events newest-first and caps the feed at 100", async () => {
    mount();
    await act(async () => {
      lastSocket().onopen?.();
    });
    for (let i = 1; i <= 3; i++) {
      await send(lastSocket(), { kind: "op", event: opEvent(i) });
    }
    expect(await screen.findByText("ops=3")).toBeInTheDocument();
    expect(screen.getByText("opsTop=op-3")).toBeInTheDocument();

    for (let i = 4; i <= 105; i++) {
      await send(lastSocket(), { kind: "op", event: opEvent(i) });
    }
    expect(await screen.findByText("ops=100")).toBeInTheDocument();
    expect(screen.getByText("opsTop=op-105")).toBeInTheDocument();
  });

  it("updates metrics from gauges frames and ignores malformed frames", async () => {
    mount();
    await act(async () => {
      lastSocket().onopen?.();
    });
    await send(lastSocket(), {
      kind: "gauges",
      gauges: { queriesTotal: 9 },
    });
    expect(await screen.findByText("queries=9")).toBeInTheDocument();

    await send(lastSocket(), { kind: "op" });
    await send(lastSocket(), "not-json{");
    expect(screen.getByText("ops=0")).toBeInTheDocument();
    expect(screen.getByText("queries=9")).toBeInTheDocument();
  });

  it("closes the connection indicator when the stream drops", async () => {
    mount();
    await act(async () => {
      lastSocket().onopen?.();
    });
    expect(await screen.findByText("conn=connected")).toBeInTheDocument();
    await act(async () => {
      lastSocket().onclose?.();
    });
    expect(await screen.findByText("conn=closed")).toBeInTheDocument();
  });

  it("refreshes the database list on demand", async () => {
    const user = userEvent.setup();
    shared.listDbs.mockResolvedValue(["db1"]);
    mount();
    await screen.findByText("dbs=db1");
    shared.listDbs.mockResolvedValue(["db1", "db-new"]);
    await user.click(screen.getByRole("button", { name: "refresh-dbs" }));
    expect(await screen.findByText("dbs=db1,db-new")).toBeInTheDocument();
    expect(screen.getByText("dbsLoading=false")).toBeInTheDocument();
    expect(screen.getByText("dbsError=null")).toBeInTheDocument();
  });

  it("surfaces refresh errors and marks the connection closed", async () => {
    const user = userEvent.setup();
    mount();
    await screen.findByText("dbs=");
    shared.listDbs.mockRejectedValue(new Error("403 forbidden"));
    await user.click(screen.getByRole("button", { name: "refresh-dbs" }));
    expect(await screen.findByText("dbsError=403 forbidden")).toBeInTheDocument();
    expect(await screen.findByText("conn=closed")).toBeInTheDocument();
  });

  it("swallows the initial listDbs failure with a debug log", async () => {
    const debug = vi.spyOn(console, "debug").mockImplementation(() => {});
    shared.listDbs.mockRejectedValue(new Error("network down"));
    mount();
    await waitFor(() => {
      expect(debug).toHaveBeenCalledWith("listDbs failed", expect.any(Error));
    });
    expect(screen.getByText("dbs=")).toBeInTheDocument();
    debug.mockRestore();
  });

  it("records a metrics fetch failure without losing the stream", async () => {
    shared.metrics.mockRejectedValue(new Error("metrics offline"));
    mount();
    expect(await screen.findByText("metricsError=metrics offline")).toBeInTheDocument();
    expect(screen.getByText("conn=connecting")).toBeInTheDocument();
  });

  it("resets all live state when the session signs out", async () => {
    shared.listDbs.mockResolvedValue(["db1"]);
    const r = mount();
    await act(async () => {
      lastSocket().onopen?.();
    });
    await send(lastSocket(), { kind: "op", event: opEvent(1) });
    expect(await screen.findByText("ops=1")).toBeInTheDocument();

    await act(async () => {
      shared.method = null;
      r.rerender(
        <AdminProvider>
          <Harness />
        </AdminProvider>,
      );
    });
    expect(screen.getByText("conn=idle")).toBeInTheDocument();
    expect(screen.getByText("dbs=")).toBeInTheDocument();
    expect(screen.getByText("ops=0")).toBeInTheDocument();
    expect(screen.getByText("queries=-1")).toBeInTheDocument();
    expect(screen.getByText("metricsError=null")).toBeInTheDocument();
  });
});
