import type { RtDbClient } from "@par-rt-db/client";

/** `http(s)://` → `ws(s)://` for a server base URL, mirroring
 * `ts-client/src/client.ts`'s private `httpToWs` (not exported, so this is a
 * small standalone copy rather than reaching into client internals). */
export function httpToWs(url: string): string {
  return url.replace(/^http/, "ws").replace(/\/+$/, "");
}

const AUTH_TIMEOUT_MS = 10_000;

/** Connects `client` and resolves once it reports `authenticated`; rejects after 10s. */
export function waitAuthenticated(client: RtDbClient): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("auth timeout")), AUTH_TIMEOUT_MS);
    const off = client.onAuthChange((state) => {
      if (state === "authenticated") {
        clearTimeout(timer);
        off();
        resolve();
      }
    });
    client.connect();
  });
}
