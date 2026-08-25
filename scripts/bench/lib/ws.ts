/** `http(s)://` → `ws(s)://` for a server base URL, mirroring
 * `ts-client/src/client.ts`'s private `httpToWs` (not exported, so this is a
 * small standalone copy rather than reaching into client internals). */
export function httpToWs(url: string): string {
  return url.replace(/^http/, "ws").replace(/\/+$/, "");
}
