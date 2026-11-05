export const DEFAULT_BACKOFF = { baseMs: 500, maxMs: 15_000 };
export const DEFAULT_HEARTBEAT_MS = 20_000;
export const DEFAULT_AUTH_UNREACHABLE_AFTER = 5;
/**
 * App-range close code we use for the drops WE initiate (heartbeat timeout,
 * socket error). The WebSocket spec forbids passing the reserved 1006 to
 * `close()` (it throws `InvalidAccessError`), so we must use 1000 or a
 * 4000–4999 code. `handleClose` treats any non-4401 code as a reconnectable drop.
 */
export const CLOSE_APP_DROP = 4000;

export function isThenable(value: unknown): value is Promise<unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as { then?: unknown }).then === "function"
  );
}

export function httpToWs(url: string): string {
  return url.replace(/^http/, "ws").replace(/\/+$/, "");
}
