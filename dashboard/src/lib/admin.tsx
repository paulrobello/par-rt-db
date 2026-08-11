import type { BackupFile, RestoreResult } from "@par-rt-db/client";
import { RtDbAdminClient, RtDbError } from "@par-rt-db/client";
import {
  createContext,
  type ReactNode,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import type { ConnectionState } from "../components/ui";
import { useSession } from "./session";
import type { MetricsSnapshot, OpEvent } from "./types";

export type { BackupFile, RestoreResult };
/** Re-exported under the dashboard's historical name so `instanceof` checks in
 *  page error handlers continue to work without touching those call sites — the
 *  SDK throws {@link RtDbError} from every admin/HTTP path, and this IS that
 *  class (same identity, different local name). `status` is `number | undefined`
 *  on the SDK type (the HTTP status is thread-able but not always present); the
 *  dashboard coerces to `number | null` at each error-state site with `?? null`. */
export { RtDbError as RtDbRequestError };

export interface AdminValue {
  client: RtDbAdminClient;
  databases: string[];
  databasesLoading: boolean;
  databasesError: string | null;
  refreshDatabases: () => Promise<void>;
  /** Newest-first op events, capped. */
  ops: OpEvent[];
  metrics: MetricsSnapshot | null;
  connection: ConnectionState;
}

const AdminContext = createContext<AdminValue | null>(null);

export function useAdmin(): AdminValue {
  const v = useContext(AdminContext);
  if (!v) throw new Error("useAdmin must be used within AdminProvider");
  return v;
}

/**
 * Holds the admin client and the live rails. The op feed and metrics stream over
 * a single WebSocket to `/admin/stream` — the admin credential rides the HttpOnly
 * session cookie on the same-origin upgrade (no JS-readable token, no subprotocol).
 * Databases poll every 20s (the db list isn't part of the stream). The data browser
 * gets its own realtime via `/sync`.
 *
 * ARC-106: the client is the SDK's {@link RtDbAdminClient} in cookie mode (no
 * `adminKey`) — the dashboard no longer carries a parallel HTTP surface that
 * drifted from the SDK. Every request sends `credentials: "include"` and the
 * `X-Rtdb-Csrf` header (read from the readable `rtdb-admin-csrf` cookie) inside
 * the SDK client.
 */
export function AdminProvider({ children }: { children: ReactNode }) {
  const { method } = useSession();
  // Cookie mode: same-origin SPA, HttpOnly session cookie authenticates /admin/*.
  const client = useMemo(() => new RtDbAdminClient({ url: window.location.origin }), []);

  const [databases, setDatabases] = useState<string[]>([]);
  const [databasesLoading, setDatabasesLoading] = useState(false);
  const [databasesError, setDatabasesError] = useState<string | null>(null);
  const [ops, setOps] = useState<OpEvent[]>([]);
  const [metrics, setMetrics] = useState<MetricsSnapshot | null>(null);
  const [connection, setConnection] = useState<ConnectionState>("idle");

  const refreshDatabases = useRef<() => Promise<void>>(async () => {});

  useEffect(() => {
    if (!method) {
      setDatabases([]);
      setOps([]);
      setMetrics(null);
      setConnection("idle");
      return;
    }
    let cancelled = false;
    let ws: WebSocket | null = null;
    let reconnect: ReturnType<typeof setTimeout> | null = null;
    let backoff = 1000;
    const ok = () => {
      if (!cancelled) setConnection("connected");
    };
    const fail = () => {
      if (!cancelled) setConnection("closed");
    };

    const loadDbs = async () => {
      try {
        setDatabases(await client.listDbs());
      } catch (e) {
        // Outages surface via the stream's connection state; surface the detail
        // (e.g. 403 vs network) to DevTools so an operator can distinguish them.
        console.debug("listDbs failed", e);
      }
    };

    const connect = () => {
      if (cancelled) return;
      setConnection((c) => (c === "connected" ? c : "connecting"));
      const proto = window.location.protocol === "https:" ? "wss" : "ws";
      const streamUrl = `${proto}://${window.location.host}/admin/stream`;
      // SEC-001: the admin credential rides an HttpOnly cookie the browser sends
      // on the WS upgrade — no JS-readable token for either auth method, so no
      // subprotocol is offered. (CLI/automation still uses the Authorization
      // header or subprotocol; the dashboard is cookie-only.)
      ws = new WebSocket(streamUrl);
      ws.onopen = () => {
        if (cancelled) return;
        backoff = 1000;
        ok();
      };
      ws.onmessage = (e) => {
        if (cancelled) return;
        let msg: { kind: string; event?: OpEvent; gauges?: MetricsSnapshot };
        try {
          msg = JSON.parse(e.data as string);
        } catch {
          return;
        }
        if (msg.kind === "op" && msg.event) {
          const ev = msg.event;
          setOps((prev) => [ev, ...prev].slice(0, OPS_CAP));
        } else if (msg.kind === "gauges" && msg.gauges) {
          setMetrics(msg.gauges);
        }
      };
      ws.onclose = () => {
        if (cancelled) return;
        fail();
        reconnect = setTimeout(connect, backoff);
        backoff = Math.min(backoff * 2, 5000);
      };
      ws.onerror = () => {
        /* browsers hide the detail; onclose drives reconnect */
      };
    };

    refreshDatabases.current = async () => {
      setDatabasesLoading(true);
      setDatabasesError(null);
      try {
        setDatabases(await client.listDbs());
        ok();
      } catch (e) {
        setDatabasesError(e instanceof Error ? e.message : String(e));
        fail();
      } finally {
        setDatabasesLoading(false);
      }
    };

    loadDbs();
    client
      .metrics()
      .then(setMetrics)
      .catch(() => {});
    connect();
    const dbsTimer = setInterval(loadDbs, 20000);
    return () => {
      cancelled = true;
      if (reconnect) clearTimeout(reconnect);
      ws?.close();
      clearInterval(dbsTimer);
    };
  }, [method, client]);

  const value: AdminValue = {
    client,
    databases,
    databasesLoading,
    databasesError,
    refreshDatabases: () => refreshDatabases.current(),
    ops,
    metrics,
    connection,
  };

  return <AdminContext.Provider value={value}>{children}</AdminContext.Provider>;
}

const OPS_CAP = 100;
