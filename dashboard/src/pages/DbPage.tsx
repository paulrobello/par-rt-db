import { useEffect, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { Button, Placard, Spinner } from "../components/ui";
import { useAdmin } from "../lib/admin";
import { formatBytes, formatNumber } from "../lib/format";
import type { DbStats } from "../lib/types";
import s from "./DbPage.module.css";

export function DbPage() {
  const { db = "" } = useParams();
  const { client, refreshDatabases } = useAdmin();
  const navigate = useNavigate();
  const [stats, setStats] = useState<DbStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [confirming, setConfirming] = useState(false);
  const [confirmText, setConfirmText] = useState("");
  const [deleting, setDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);

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

  async function deleteDb() {
    setDeleting(true);
    setDeleteError(null);
    try {
      await client.deleteDb(db, db);
      await refreshDatabases();
      navigate("/databases");
    } catch (e) {
      setDeleteError(e instanceof Error ? e.message : String(e));
    } finally {
      setDeleting(false);
    }
  }

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
      <section className={s.danger}>
        <h2 className={s.dangerTitle}>Delete database</h2>
        <p className={s.dangerBody}>
          Permanently deletes <span className={s.dangerName}>{db}</span>: its schema, every table
          and document, minted tokens, the per-db allowlist, and storage blobs. This cannot be
          undone.
        </p>
        {confirming ? (
          <div className={s.confirm}>
            <label className={s.confirmLabel}>
              Type the database name to confirm
              <input
                className={s.confirmInput}
                value={confirmText}
                onChange={(e) => setConfirmText(e.target.value)}
                placeholder={db}
                spellCheck={false}
                autoComplete="off"
              />
            </label>
            <div className={s.confirmActions}>
              <Button variant="danger" onClick={deleteDb} disabled={deleting || confirmText !== db}>
                delete forever
              </Button>
              <Button
                onClick={() => {
                  setConfirming(false);
                  setConfirmText("");
                  setDeleteError(null);
                }}
                disabled={deleting}
              >
                cancel
              </Button>
            </div>
            {deleteError && <p className={s.error}>{deleteError}</p>}
          </div>
        ) : (
          <Button variant="danger" onClick={() => setConfirming(true)} disabled={deleting}>
            Delete database
          </Button>
        )}
      </section>
    </section>
  );
}
