import { useEffect, useRef, useState } from "react";
import { NavLink, Outlet } from "react-router-dom";
import { Button, ConnectionPulse, Placard } from "../components/ui";
import { useAdmin } from "../lib/admin";
import { useSession } from "../lib/session";
import type { OpEvent, OpKind } from "../lib/types";
import s from "./AppShell.module.css";

const NAV = [
  { to: "/", label: "Databases", end: true },
  { to: "/metrics", label: "Metrics" },
  { to: "/ops", label: "Op feed" },
  { to: "/subscriptions", label: "Subscriptions" },
  { to: "/scheduled", label: "Scheduled" },
  { to: "/storage", label: "Storage" },
  { to: "/tokens", label: "Tokens" },
  { to: "/webhooks", label: "Webhooks" },
  { to: "/console", label: "Console" },
  { to: "/config", label: "Config" },
  { to: "/admins", label: "Admins" },
  { to: "/audit", label: "Audit" },
  { to: "/backups", label: "Backups" },
];

const KIND_TONE: Record<OpKind, string> = {
  insert: s.kindInsert,
  patch: s.kindPatch,
  replace: s.kindReplace,
  delete: s.kindDelete,
};

function clock(ms: number): string {
  const d = new Date(ms);
  const hhmmss = d.toLocaleTimeString(undefined, { hour12: false });
  return `${hhmmss}.${String(d.getMilliseconds()).padStart(3, "0")}`;
}

function OpLine({ op, fresh }: { op: OpEvent; fresh?: boolean }) {
  return (
    <div className={`${s.opLine} ${fresh ? s.opLineFresh : ""}`}>
      <span className={`${s.opKind} ${KIND_TONE[op.kind]}`}>{op.kind[0].toUpperCase()}</span>
      <span className={s.opWhere}>
        {op.db}
        <span className={s.opSep}>·</span>
        {op.table}
      </span>
      <span className={s.opId}>{op.docId.slice(-10)}</span>
      <span className={s.opTime}>{clock(op.ts)}</span>
    </div>
  );
}

export function AppShell() {
  const { databases, ops, connection } = useAdmin();
  const { user, method, signOut } = useSession();
  const email = (user as { email?: string } | null)?.email;
  const identity = email ?? (method === "adminkey" ? "admin key" : "—");

  // Op-feed settle: flash the newest event as it lands. prevTopKey starts null
  // so the initial batch (page load / reconnect backfill) does NOT animate —
  // only a genuinely new top event does, never an orchestrated entrance.
  const opKey = (op: OpEvent) => `${op.ts}-${op.docId}-${op.kind}`;
  const topKey = ops[0] ? opKey(ops[0]) : null;
  const prevTopKey = useRef<string | null>(null);
  const [freshKey, setFreshKey] = useState<string | null>(null);
  useEffect(() => {
    if (!topKey) return;
    if (prevTopKey.current !== null && prevTopKey.current !== topKey) {
      setFreshKey(topKey);
      const t = setTimeout(() => setFreshKey((k) => (k === topKey ? null : k)), 720);
      prevTopKey.current = topKey;
      return () => clearTimeout(t);
    }
    prevTopKey.current = topKey;
  }, [topKey]);

  return (
    <div className={s.shell}>
      <header className={s.topbar}>
        <span className={`${s.brand} mono`}>par-rt-db</span>
        <span className={s.topbarRight}>
          <span className={s.identity}>{identity}</span>
          <Button onClick={signOut}>Sign out</Button>
          <ConnectionPulse state={connection} />
        </span>
      </header>
      <div className={s.body}>
        <aside className={s.rail}>
          <div>
            <Placard>Navigate</Placard>
            <nav className={s.railList}>
              {NAV.map((n) => (
                <NavLink
                  key={n.to}
                  to={n.to}
                  end={n.end}
                  className={({ isActive }) =>
                    isActive ? `${s.railLink} ${s.railLinkActive}` : s.railLink
                  }
                >
                  {n.label}
                </NavLink>
              ))}
            </nav>
          </div>
          <div>
            <Placard>Databases</Placard>
            {databases.length === 0 ? (
              <p className={s.railEmpty}>— none —</p>
            ) : (
              <nav className={s.railList}>
                {databases.slice(0, 50).map((db) => (
                  <NavLink
                    key={db}
                    to={`/dbs/${db}`}
                    className={({ isActive }) =>
                      isActive ? `${s.railLink} ${s.railLinkActive}` : s.railLink
                    }
                  >
                    {db}
                  </NavLink>
                ))}
                {databases.length > 50 && (
                  <p className={s.railMore}>+ {(databases.length - 50).toLocaleString()} more</p>
                )}
              </nav>
            )}
          </div>
        </aside>
        <main className={s.main}>
          <Outlet />
        </main>
        <aside className={s.liveRail}>
          <Placard>Op feed</Placard>
          {ops.length === 0 ? (
            <p className={s.liveEmpty}>— idle —</p>
          ) : (
            <div className={s.opList}>
              {ops.map((op) => {
                const key = opKey(op);
                return <OpLine key={key} op={op} fresh={key === freshKey} />;
              })}
            </div>
          )}
        </aside>
      </div>
    </div>
  );
}
