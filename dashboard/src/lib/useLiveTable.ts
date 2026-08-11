import { RtDbClient } from "@par-rt-db/client";
import { useEffect, useRef, useState } from "react";
import { useAdmin } from "./admin";
import { toErrorMessage } from "./errors";
import { useSession } from "./session";

export interface Doc {
  _id: string;
  _creationTime: number;
  _version: number;
  [k: string]: unknown;
}

export interface LiveTableValue {
  docs: Doc[];
  loading: boolean;
  error: string | null;
  /** true = true realtime over /sync (OAuth); false = stream-driven refresh with polling fallback. */
  live: boolean;
  refresh: () => Promise<void>;
}

/**
 * Live document table. OAuth admins subscribe over `/sync` (true realtime, the
 * admin bypass yields every row). Non-OAuth admins refresh off the `/admin/stream`
 * op feed — every durable write arrives as an `{db, table}` event, so a matching
 * op triggers a refresh within the stream's push latency instead of a blind 2s
 * poll. The 2s poll stays as a reconnect fallback: when the stream reports
 * anything other than `"connected"`, the interval arms so a dropped socket
 * doesn't silently stale the view (ARC-123).
 */
export function useLiveTable(
  db: string,
  table: string,
  order: "asc" | "desc",
  take: number,
): LiveTableValue {
  const { client: admin, ops, connection } = useAdmin();
  const { method } = useSession();
  const [docs, setDocs] = useState<Doc[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const live = method === "oauth";
  const refresh = useRef<() => Promise<void>>(async () => {});
  // High-water mark over the `ops` array (newest-first), tracked by the server
  // `ts` of the newest op seen. `ops[0].ts` is monotonic across frames, so a
  // changed value means new frames arrived since the last scan. (Identity would
  // be fragile here: every stream frame is a fresh `JSON.parse`, so object refs
  // are not stable across renders — and a fresh ref for a prepended non-matching
  // op would make the scan walk past the old watermark and re-fire on a stale
  // matching op. `ts` is server-set and present on every OpEvent.)
  const lastSeenTs = useRef<number | null>(null);

  useEffect(() => {
    const query = { table, order, take };
    if (live) {
      // SEC-001 phase 2: cookie mode — no `getToken`. The browser's HttpOnly
      // `rtdb_session` cookie authenticates the /sync WS upgrade.
      const rtdb = new RtDbClient({
        url: window.location.origin,
        db,
      });
      rtdb.connect();
      const unsub = rtdb.subscribe<Doc[]>({ json: query }, (value) => {
        setDocs(value);
        setLoading(false);
        setError(null);
      });
      refresh.current = async () => {
        /* /sync pushes automatically; no manual refresh needed */
      };
      return () => {
        unsub();
        rtdb.close();
      };
    }

    let cancelled = false;
    const poll = async () => {
      try {
        const r = await admin.adminQuery(db, query);
        if (!cancelled) {
          setDocs(r as Doc[]);
          setError(null);
        }
      } catch (e) {
        if (!cancelled) setError(toErrorMessage(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    refresh.current = poll;
    poll();

    // Reconnect fallback: poll on a 2s cadence ONLY while the stream is not
    // connected. When it is connected, the ops-watcher below drives refreshes
    // and this interval stays off (no redundant fetches). Each transition into
    // a non-connected state re-runs this effect (connection is a dep) and arms
    // the fallback; transition back to connected stops it and re-polls once.
    if (connection !== "connected") {
      const timer = setInterval(poll, 2000);
      return () => {
        cancelled = true;
        clearInterval(timer);
      };
    }
    return () => {
      cancelled = true;
    };
  }, [admin, db, table, order, take, live, connection]);

  // Stream-driven refresh for the non-live path: when a fresh op frame matches
  // this table, re-poll. Scanned from the newest end back to the high-water `ts`
  // so a burst of writes triggers exactly one refresh, not one per event.
  useEffect(() => {
    if (live) return;
    if (!ops || ops.length === 0) {
      lastSeenTs.current = null;
      return;
    }
    const newest = ops[0].ts;
    if (lastSeenTs.current === newest) return;
    if (lastSeenTs.current === null) {
      // First sighting (mount or stream just came up): mark the high-water
      // mark without refreshing — the mount-time poll above already loaded
      // fresh data, so replaying historical ops would only double-fetch.
      lastSeenTs.current = newest;
      return;
    }
    let match = false;
    for (const op of ops) {
      if (op.ts <= lastSeenTs.current) break;
      if (op.db === db && op.table === table) {
        match = true;
        break;
      }
    }
    lastSeenTs.current = newest;
    if (match) void refresh.current();
  }, [ops, db, table, live]);

  return { docs, loading, error, live, refresh: () => refresh.current() };
}
