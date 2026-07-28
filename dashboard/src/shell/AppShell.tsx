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
  { to: "/scheduled", label: "Scheduled" },
  { to: "/console", label: "Console" },
  { to: "/config", label: "Config" },
  { to: "/admins", label: "Admins" },
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

function OpLine({ op }: { op: OpEvent }) {
  return (
    <div className={s.opLine}>
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
              {ops.map((op) => (
                <OpLine key={`${op.ts}-${op.docId}-${op.kind}`} op={op} />
              ))}
            </div>
          )}
        </aside>
      </div>
    </div>
  );
}
