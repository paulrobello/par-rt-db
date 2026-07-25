import { useEffect, useRef, useState } from "react";
import { RtDbClient } from "@par-rt-db/client";
import { useAdmin } from "./admin";
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
  /** true = true realtime over /sync (OAuth); false = ~2s polling (admin key). */
  live: boolean;
  refresh: () => Promise<void>;
}

/**
 * Live document table. OAuth admins subscribe over `/sync` (true realtime, the
 * admin bypass yields every row); admin-key mode polls `/admin/db/{db}/query`
 * ~2s, since the raw admin key is not accepted on `/sync`.
 */
export function useLiveTable(
  db: string,
  table: string,
  order: "asc" | "desc",
  take: number,
): LiveTableValue {
  const { client: admin } = useAdmin();
  const { token, method } = useSession();
  const [docs, setDocs] = useState<Doc[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const live = method === "oauth";
  const refresh = useRef<() => Promise<void>>(async () => {});

  useEffect(() => {
    const query = { table, order, take };
    if (live && token) {
      const rtdb = new RtDbClient({
        url: window.location.origin,
        db,
        getToken: () => token,
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
          setDocs(r.result as Doc[]);
          setError(null);
        }
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    refresh.current = poll;
    poll();
    const timer = setInterval(poll, 2000);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [admin, db, table, order, take, live, token]);

  return { docs, loading, error, live, refresh: () => refresh.current() };
}
