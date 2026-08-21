/** Schema-history page sub-panels (QA-201) — the version list and the selected
 *  version's detail panel (snapshot + diff vs current + typed-confirm restore)
 *  extracted from SchemaHistoryPage; the page keeps the list load and
 *  selection state. */
import type {
  RtDbAdminClient,
  SchemaHistoryEntry,
  SchemaHistoryEntrySummary,
  SchemaJson,
} from "@par-rt-db/client";
import { useEffect, useState } from "react";
import { RtDbRequestError } from "../lib/admin";
import { toErrorMessage } from "../lib/errors";
import { formatDateTime, formatFieldType } from "../lib/format";
import s from "../pages/SchemaHistoryPage.module.css";
import { Button, Placard, Spinner } from "./ui";

type DetailError = { code: string; status: number | null; message: string };

function toDetailError(e: unknown): DetailError {
  if (e instanceof RtDbRequestError) {
    return { code: e.code, status: e.status ?? null, message: e.message };
  }
  return {
    code: "INTERNAL",
    status: null,
    message: toErrorMessage(e),
  };
}

/** Client-side structural diff between two schema snapshots: tables & indexes
 *  added or removed going from `prev` -> `next`. Field-level changes are not
 *  detected — the server's schema model is additive-only, and field-level
 *  changes happen via migrate, which captures its own snapshot. */
export function diffSchemas(prev: SchemaJson, next: SchemaJson) {
  const removedTables = Object.keys(prev.tables).filter((t) => !next.tables[t]);
  const addedTables = Object.keys(next.tables).filter((t) => !prev.tables[t]);
  const removedIndexes: { table: string; index: string }[] = [];
  const addedIndexes: { table: string; index: string }[] = [];
  for (const t of Object.keys(prev.tables)) {
    if (!next.tables[t]) continue;
    const pi = new Set(prev.tables[t].indexes?.map((i) => i.name) ?? []);
    const ni = new Set(next.tables[t].indexes?.map((i) => i.name) ?? []);
    pi.forEach((i) => {
      if (!ni.has(i)) removedIndexes.push({ table: t, index: i });
    });
    ni.forEach((i) => {
      if (!pi.has(i)) addedIndexes.push({ table: t, index: i });
    });
  }
  return { removedTables, addedTables, removedIndexes, addedIndexes };
}

interface SchemaHistoryListProps {
  history: SchemaHistoryEntrySummary[];
  selected: number | null;
  onSelect: (version: number) => void;
}

export function SchemaHistoryList({ history, selected, onSelect }: SchemaHistoryListProps) {
  return (
    <div className={s.list}>
      {history.map((entry) => (
        <button
          key={entry.version}
          type="button"
          onClick={() => onSelect(entry.version)}
          className={`${s.row} ${selected === entry.version ? s.rowActive : ""}`}
          aria-pressed={selected === entry.version}
          aria-label={`Version ${entry.version}`}
        >
          <span className={s.version}>v{entry.version}</span>
          <span className={s.capturedAt}>{formatDateTime(entry.capturedAt)}</span>
          <span className={s.source}>{entry.source}</span>
          <span className={entry.principal === null ? s.mutedCell : s.principal}>
            {entry.principal === null ? "—" : entry.principal}
          </span>
        </button>
      ))}
    </div>
  );
}

interface SchemaVersionDetailProps {
  client: RtDbAdminClient;
  db: string;
  version: number;
  /** Called after a successful restore (the page refreshes the history list). */
  onRestored: () => void;
}

export function SchemaVersionDetail({ client, db, version, onRestored }: SchemaVersionDetailProps) {
  const [snapshot, setSnapshot] = useState<SchemaHistoryEntry | null>(null);
  const [current, setCurrent] = useState<SchemaJson | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<DetailError | null>(null);

  const [confirming, setConfirming] = useState(false);
  const [confirmText, setConfirmText] = useState("");
  const [restoring, setRestoring] = useState(false);
  const [restoreError, setRestoreError] = useState<DetailError | null>(null);
  const [restoredTo, setRestoredTo] = useState<number | null>(null);

  // Fetch the selected snapshot + the live schema together so the diff
  // recomputes on every selection. On a successful restore the live schema is
  // refetched directly (see doRestore) without re-triggering this effect.
  useEffect(() => {
    let cancelled = false;
    setDetailLoading(true);
    setDetailError(null);
    setSnapshot(null);
    setCurrent(null);
    Promise.all([client.getSchemaVersion(db, version), client.getSchema(db)])
      .then(([snap, cur]) => {
        if (!cancelled) {
          setSnapshot(snap);
          setCurrent(cur);
        }
      })
      .catch((e) => {
        if (!cancelled) setDetailError(toDetailError(e));
      })
      .finally(() => {
        if (!cancelled) setDetailLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [client, db, version]);

  async function doRestore() {
    if (confirmText !== db) return;
    setRestoring(true);
    setRestoreError(null);
    setRestoredTo(null);
    try {
      await client.restoreSchema(db, version, db);
      setRestoredTo(version);
      setConfirming(false);
      setConfirmText("");
      onRestored();
      // The live schema now matches the restored snapshot — refetch the
      // "current" side so the diff recomputes (it should be empty).
      try {
        setCurrent(await client.getSchema(db));
      } catch (e) {
        // Restore itself succeeded; the list refresh surfaces the new snapshot.
        // Surface the failure detail to DevTools (403 vs network) for diagnosis.
        console.debug("post-restore getSchema failed", e);
      }
    } catch (e) {
      setRestoreError(toDetailError(e));
    } finally {
      setRestoring(false);
    }
  }

  const diff = snapshot && current ? diffSchemas(snapshot.schema, current) : null;
  const snapTables = snapshot
    ? Object.entries(snapshot.schema.tables).sort(([a], [b]) => a.localeCompare(b))
    : [];
  const diffEmpty =
    diff !== null &&
    diff.addedTables.length === 0 &&
    diff.removedTables.length === 0 &&
    diff.addedIndexes.length === 0 &&
    diff.removedIndexes.length === 0;

  return (
    <section className={s.detail}>
      <Placard>Version {version}</Placard>
      {detailLoading ? (
        <Spinner label="loading version" />
      ) : detailError ? (
        <div className={s.resultPanel}>
          <p className={s.errorHead}>
            {detailError.code}
            {detailError.status !== null ? ` · HTTP ${detailError.status}` : ""}
          </p>
          <p className={s.errorBody}>{detailError.message}</p>
        </div>
      ) : snapshot ? (
        <>
          {restoredTo === version && <p className={s.applied}>Restored to version {version}.</p>}

          <div className={s.diffPanel}>
            <h2 className={s.diffTitle}>Diff vs current</h2>
            {diff === null ? null : diffEmpty ? (
              <p className={s.resultEmpty}>
                No changes — this snapshot matches the current schema.
              </p>
            ) : (
              <>
                {diff.addedTables.length > 0 && (
                  <div className={s.diffSection}>
                    <h3 className={`${s.diffHead} ${s.addedHead}`}>
                      tables added ({diff.addedTables.length})
                    </h3>
                    {diff.addedTables.map((t) => (
                      <div key={t} className={s.diffRow}>
                        <span className={s.diffAddedMark}>+</span>
                        <span className={s.diffTableName}>{t}</span>
                      </div>
                    ))}
                  </div>
                )}
                {diff.removedTables.length > 0 && (
                  <div className={s.diffSection}>
                    <h3 className={`${s.diffHead} ${s.removedHead}`}>
                      tables removed ({diff.removedTables.length})
                    </h3>
                    {diff.removedTables.map((t) => (
                      <div key={t} className={s.diffRow}>
                        <span className={s.diffRemovedMark}>−</span>
                        <span className={s.diffTableName}>{t}</span>
                      </div>
                    ))}
                  </div>
                )}
                {diff.addedIndexes.length > 0 && (
                  <div className={s.diffSection}>
                    <h3 className={`${s.diffHead} ${s.addedHead}`}>
                      indexes added ({diff.addedIndexes.length})
                    </h3>
                    {diff.addedIndexes.map((ix) => (
                      <div key={`${ix.table}.${ix.index}`} className={s.diffRow}>
                        <span className={s.diffAddedMark}>+</span>
                        <span className={s.diffTableName}>{ix.table}</span>
                        <span className={s.diffName}>#{ix.index}</span>
                      </div>
                    ))}
                  </div>
                )}
                {diff.removedIndexes.length > 0 && (
                  <div className={s.diffSection}>
                    <h3 className={`${s.diffHead} ${s.removedHead}`}>
                      indexes removed ({diff.removedIndexes.length})
                    </h3>
                    {diff.removedIndexes.map((ix) => (
                      <div key={`${ix.table}.${ix.index}`} className={s.diffRow}>
                        <span className={s.diffRemovedMark}>−</span>
                        <span className={s.diffTableName}>{ix.table}</span>
                        <span className={s.diffName}>#{ix.index}</span>
                      </div>
                    ))}
                  </div>
                )}
              </>
            )}
          </div>

          <div className={s.snapshotHead}>
            <h2 className={s.diffTitle}>Snapshot</h2>
            <Button
              variant="danger"
              onClick={() => setConfirming(true)}
              disabled={confirming || restoring}
            >
              Restore to this version
            </Button>
          </div>

          {confirming && (
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
                  aria-label="database name confirm"
                />
              </label>
              <div className={s.confirmActions}>
                <Button
                  variant="danger"
                  onClick={doRestore}
                  disabled={restoring || confirmText !== db}
                >
                  {restoring ? "restoring…" : "restore"}
                </Button>
                <Button
                  onClick={() => {
                    setConfirming(false);
                    setConfirmText("");
                    setRestoreError(null);
                  }}
                  disabled={restoring}
                >
                  cancel
                </Button>
                {restoring && <Spinner label="restoring" />}
              </div>
              {restoreError && (
                <div className={s.resultPanel}>
                  <p className={s.errorHead}>
                    {restoreError.code}
                    {restoreError.status !== null ? ` · HTTP ${restoreError.status}` : ""}
                  </p>
                  <p className={s.errorBody}>{restoreError.message}</p>
                </div>
              )}
            </div>
          )}

          <div className={s.tables}>
            {snapTables.map(([name, table]) => (
              <div key={name} className={s.tableBlock}>
                <div className={s.tableHead}>
                  <h3 className={s.tableName}>{name}</h3>
                  {table.ownerField && <span className={s.owner}>owner: {table.ownerField}</span>}
                  {table.updatedAtField && (
                    <span className={s.owner}>updatedAt: {table.updatedAtField}</span>
                  )}
                </div>
                <table className={s.fields}>
                  <tbody>
                    {Object.entries(table.fields).map(([fname, ftype]) => (
                      <tr key={fname}>
                        <td className={s.fieldName}>{fname}</td>
                        <td className={s.fieldType}>{formatFieldType(ftype)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {table.indexes && table.indexes.length > 0 && (
                  <div className={s.indexes}>
                    <span className={s.indexLabel}>indexes</span>
                    {table.indexes.map((idx) => (
                      <span key={idx.name} className={s.index}>
                        {idx.search ? (
                          <span className={s.indexTag}>
                            FTS{idx.language ? `·${idx.language}` : ""}
                          </span>
                        ) : idx.vector ? (
                          <span className={s.indexTag}>VEC·{idx.vector.metric ?? "cosine"}</span>
                        ) : null}
                        <span className={s.indexName}>{idx.name}</span>
                        <span className={s.indexFields}>({idx.fields.join(", ")})</span>
                      </span>
                    ))}
                  </div>
                )}
              </div>
            ))}
          </div>
        </>
      ) : null}
    </section>
  );
}
