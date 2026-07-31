import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { Button, Field, LiveValue, Placard } from "../components/ui";
import { useAdmin } from "../lib/admin";
import { formatNumber } from "../lib/format";
import s from "./DatabasesPage.module.css";

const SHOW = 200;

export function DatabasesPage() {
  const { databases, databasesLoading, databasesError, refreshDatabases, client } = useAdmin();
  const [q, setQ] = useState("");
  const [newName, setNewName] = useState("");
  const [creating, setCreating] = useState(false);
  const [createError, setCreateError] = useState<string | null>(null);

  const { rows, hidden } = useMemo(() => {
    const needle = q.trim().toLowerCase();
    const matches = needle ? databases.filter((d) => d.toLowerCase().includes(needle)) : databases;
    return {
      rows: matches.slice(0, SHOW),
      hidden: Math.max(0, matches.length - SHOW),
    };
  }, [databases, q]);

  async function create() {
    const name = newName.trim();
    if (!name) return;
    setCreating(true);
    setCreateError(null);
    try {
      await client.createDb(name);
      setNewName("");
      await refreshDatabases();
    } catch (e) {
      setCreateError(e instanceof Error ? e.message : String(e));
    } finally {
      setCreating(false);
    }
  }

  return (
    <section className={s.page}>
      <Placard>Databases</Placard>
      <h1 className={s.title}>
        Databases <LiveValue className={s.count} value={formatNumber(databases.length)} />
      </h1>
      <div className={s.toolbar}>
        <Field label="Search" value={q} onChange={setQ} placeholder="filter by name" mono />
        <Field label="New database" value={newName} onChange={setNewName} placeholder="name" mono />
        <Button variant="primary" onClick={create} disabled={creating || !newName.trim()}>
          Create
        </Button>
      </div>
      {createError && <p className={s.error}>{createError}</p>}
      {databasesLoading && databases.length === 0 ? (
        <p className={s.muted}>loading…</p>
      ) : databasesError ? (
        <p className={s.error}>{databasesError}</p>
      ) : rows.length === 0 ? (
        <p className={s.muted}>no databases match</p>
      ) : (
        <ul className={s.list}>
          {rows.map((db) => (
            <li key={db}>
              <Link to={`/dbs/${db}`} className={s.row}>
                <span className={s.rowMark} aria-hidden />
                <span className={s.rowName}>{db}</span>
              </Link>
            </li>
          ))}
        </ul>
      )}
      {hidden > 0 && <p className={s.more}>+ {formatNumber(hidden)} more — refine your search</p>}
    </section>
  );
}
