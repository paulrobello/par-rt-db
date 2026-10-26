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
  /** Ids of soft-deleted rows; populated only when `includeDeleted` is on. */
  deletedIds: ReadonlySet<string>;
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
  includeDeleted = false,
): LiveTableValue {
  const { client: admin, ops, connection } = useAdmin();
  const { method } = useSession();
  const [docs, setDocs] = useState<Doc[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  // /sync cannot carry the admin-only includeDeleted flag, so that view always
  // takes the admin polling path.
  const live = method === "oauth" && !includeDeleted;
  const [deletedIds, setDeletedIds] = useState<ReadonlySet<string>>(new Set());
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
        const r = (await admin.adminQuery(db, query, { includeDeleted })) as Doc[];
        // The server does not stamp deleted rows in the result. The live-only
        // window of the same size is a superset of the live rows in the mixed
        // window (same order), so ids missing from it are the soft-deleted ones.
        const deleted = new Set<string>();
        if (includeDeleted) {
          const liveRows = (await admin.adminQuery(db, query)) as Doc[];
          const liveIds = new Set(liveRows.map((d) => d._id));
          for (const d of r) if (!liveIds.has(d._id)) deleted.add(d._id);
        }
        if (!cancelled) {
          setDocs(r);
          setDeletedIds(deleted);
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
  }, [admin, db, table, order, take, live, connection, includeDeleted]);

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

  return { docs, loading, error, live, deletedIds, refresh: () => refresh.current() };
}
