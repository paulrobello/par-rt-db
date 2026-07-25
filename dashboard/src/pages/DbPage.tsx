import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { Placard, Spinner } from "../components/ui";
import { useAdmin } from "../lib/admin";
import { formatBytes, formatNumber } from "../lib/format";
import type { DbStats } from "../lib/types";
import s from "./DbPage.module.css";

export function DbPage() {
  const { db = "" } = useParams();
  const { client } = useAdmin();
  const [stats, setStats] = useState<DbStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    setStats(null);
    client
      .getStats(db)
      .then((st) => {
        if (!cancelled) setStats(st);
      })
      .catch((e) => {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [client, db]);

  return (
    <section className={s.page}>
      <Placard>Database</Placard>
      <h1 className={s.title}>{db}</h1>
      <div className={s.toolbar}>
        <Link to={`/dbs/${db}/schema`} className={s.link}>
          Schema →
        </Link>
        {stats && (
          <span className={s.total}>
            {formatBytes(stats.totalSizeBytes)} · {formatNumber(stats.tables.length)} tables
          </span>
        )}
      </div>
      {loading ? (
        <Spinner label="loading stats" />
      ) : error ? (
        <p className={s.error}>{error}</p>
      ) : stats?.tables.length === 0 ? (
        <p className={s.empty}>No tables — push a schema to this database.</p>
      ) : (
        <table className={s.table}>
          <thead>
            <tr>
              <th>Table</th>
              <th>Rows</th>
              <th>Size</th>
              <th aria-label="actions"></th>
            </tr>
          </thead>
          <tbody>
            {stats?.tables.map((t) => (
              <tr key={t.name}>
                <td className={s.name}>{t.name}</td>
                <td className="tnum">{formatNumber(t.rowCount)}</td>
                <td className="tnum">{formatBytes(t.sizeBytes)}</td>
                <td>
                  <Link to={`/dbs/${db}/tables/${t.name}`} className={s.link}>
                    browse →
                  </Link>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}
