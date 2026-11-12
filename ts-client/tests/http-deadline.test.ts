import { afterEach, describe, expect, it, vi } from "vitest";
import { RtDbAdminClient } from "../src/admin.js";
import { RtDbDeadlineError } from "../src/errors.js";
import { fetchWithDeadline, RtDbHttpClient } from "../src/http.js";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

/** A fetch mock whose promise never settles on its own; it rejects only when
 *  the request signal aborts, mirroring real fetch's abort behavior. */
function abortReactiveFetch(): { fetchMock: ReturnType<typeof vi.fn>; signals: AbortSignal[] } {
  const signals: AbortSignal[] = [];
  const fetchMock = vi.fn((_url: string | URL, init?: RequestInit): Promise<Response> => {
    const signal = init?.signal;
    if (signal) signals.push(signal);
    return new Promise<Response>((_resolve, reject) => {
      signal?.addEventListener("abort", () => reject(signal.reason ?? new Error("aborted")));
    });
  });
  return { fetchMock, signals };
}

describe("RtDbHttpClient request deadline", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("rejects with a typed deadline error and aborts the fetch when the timer fires first", async () => {
    vi.useFakeTimers();
    const { fetchMock, signals } = abortReactiveFetch();
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock as unknown as typeof fetch,
      requestTimeoutMs: 500,
    });

    const promise = client.query({ json: { table: "items" } });
    const assertion = expect(promise).rejects.toSatisfy(
      (e: unknown) =>
        RtDbDeadlineError.is(e) && e.code === "DEADLINE_EXCEEDED" && e.timeoutMs === 500,
    );
    await vi.advanceTimersByTimeAsync(500);
    await assertion;
    expect(signals).toHaveLength(1);
    expect(signals[0].aborted).toBe(true);
  });

  it("resolves when the fetch settles first and the deadline timer is cleared", async () => {
    vi.useFakeTimers();
    const signals: AbortSignal[] = [];
    const resolvingFetch = vi.fn(async (_url: string | URL, init?: RequestInit) => {
      if (init?.signal) signals.push(init.signal);
      return jsonResponse({ result: [{ _id: "a" }] });
    });
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: resolvingFetch as unknown as typeof fetch,
      requestTimeoutMs: 500,
    });

    const result = await client.query({ json: { table: "items" } });
    expect(result).toEqual([{ _id: "a" }]);

    // Advancing far past the deadline must produce no late rejection or abort:
    // the timer was cleared when fetch settled first.
    await vi.advanceTimersByTimeAsync(5000);
    expect(signals).toHaveLength(1);
    expect(signals[0].aborted).toBe(false);
  });

  it("composes a caller-provided signal instead of clobbering it", async () => {
    vi.useFakeTimers();
    const caller = new AbortController();
    let fetchSignal: AbortSignal | undefined;
    const fetchMock = vi.fn((_url: string | URL, init?: RequestInit): Promise<Response> => {
      fetchSignal = init?.signal ?? undefined;
      return new Promise<Response>((_resolve, reject) => {
        fetchSignal?.addEventListener("abort", () =>
          reject(fetchSignal?.reason ?? new Error("aborted")),
        );
      });
    });

    const promise = fetchWithDeadline(
      fetchMock as unknown as typeof fetch,
      "http://h:8300/api/query",
      { signal: caller.signal },
      5000,
    );
    const assertion = expect(promise).rejects.toBe("caller-stop");
    caller.abort("caller-stop");
    await assertion;
    // The deadline never fired: the rejection reason is the caller's own, and
    // the composed (internal) signal carries it to the fetch layer.
    expect(fetchSignal?.aborted).toBe(true);
    expect(fetchSignal?.reason).toBe("caller-stop");
  });

  it("still rejects with the deadline error when a caller signal is attached but the deadline fires first", async () => {
    vi.useFakeTimers();
    const caller = new AbortController();
    const { fetchMock } = abortReactiveFetch();
    const promise = fetchWithDeadline(
      fetchMock as unknown as typeof fetch,
      "http://h:8300/api/query",
      { signal: caller.signal },
      250,
    );
    const assertion = expect(promise).rejects.toSatisfy(
      (e: unknown) => RtDbDeadlineError.is(e) && e.code === "DEADLINE_EXCEEDED",
    );
    await vi.advanceTimersByTimeAsync(250);
    await assertion;
  });

  it("adds Connection: close only when connectionClose is enabled", async () => {
    const captured: Array<Record<string, string>> = [];
    const captureFetch = vi.fn(async (_url: string | URL, init?: RequestInit) => {
      captured.push(init?.headers as Record<string, string>);
      return jsonResponse({ result: [] });
    });
    const withClose = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: captureFetch as unknown as typeof fetch,
      connectionClose: true,
    });
    const withoutClose = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: captureFetch as unknown as typeof fetch,
    });

    await withClose.query({ json: { table: "items" } });
    await withoutClose.query({ json: { table: "items" } });

    expect(captured[0].Connection).toBe("close");
    expect(captured[1].Connection).toBeUndefined();
  });
});

describe("RtDbAdminClient request deadline", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("applies the deadline to admin requests", async () => {
    vi.useFakeTimers();
    const { fetchMock } = abortReactiveFetch();
    const admin = new RtDbAdminClient({
      url: "http://h:8300",
      adminKey: "key",
      fetch: fetchMock as unknown as typeof fetch,
      requestTimeoutMs: 250,
    });

    const assertion = expect(admin.listBackups()).rejects.toSatisfy(
      (e: unknown) =>
        RtDbDeadlineError.is(e) && e.code === "DEADLINE_EXCEEDED" && e.timeoutMs === 250,
    );
    await vi.advanceTimersByTimeAsync(250);
    await assertion;
  });

  it("adds Connection: close on admin requests when enabled", async () => {
    const captured: Array<Record<string, string>> = [];
    const captureFetch = vi.fn(async (_url: string | URL, init?: RequestInit) => {
      captured.push(init?.headers as Record<string, string>);
      return jsonResponse({ backups: [], running: false });
    });
    const admin = new RtDbAdminClient({
      url: "http://h:8300",
      adminKey: "key",
      fetch: captureFetch as unknown as typeof fetch,
      connectionClose: true,
    });

    await admin.listBackups();

    expect(captured[0].Connection).toBe("close");
  });

  it("keeps default headers on admin requests when connectionClose is unset", async () => {
    const captured: Array<Record<string, string>> = [];
    const captureFetch = vi.fn(async (_url: string | URL, init?: RequestInit) => {
      captured.push(init?.headers as Record<string, string>);
      return jsonResponse({ backups: [], running: false });
    });
    const admin = new RtDbAdminClient({
      url: "http://h:8300",
      adminKey: "key",
      fetch: captureFetch as unknown as typeof fetch,
    });

    await admin.listBackups();

    expect(captured[0].Connection).toBeUndefined();
  });
});
