import { useCallback, useEffect, useState } from "react";
import { Button, Placard, Spinner } from "../components/ui";
import { type BackupFile, type RestoreResult, useAdmin } from "../lib/admin";
import { formatBytes } from "../lib/format";
import s from "./BackupsPage.module.css";

function absolute(ms: number): string {
  return new Date(ms).toLocaleString(undefined, { hour12: false });
}

function relative(ms: number): string {
  const diff = Date.now() - ms;
  if (diff < 0) return "in the future";
  const sec = Math.floor(diff / 1000);
  if (sec < 60) return `${sec}s ago`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m ago`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h ago`;
  const day = Math.floor(hr / 24);
  return `${day}d ago`;
}

export function BackupsPage() {
  const { client } = useAdmin();
  const [backups, setBackups] = useState<BackupFile[]>([]);
  const [running, setRunning] = useState(false);
  const [loading, setLoading] = useState(true);
  const [listError, setListError] = useState<string | null>(null);
  const [triggering, setTriggering] = useState(false);

  // Per-row action state.
  const [actionError, setActionError] = useState<string | null>(null);
  const [pendingDelete, setPendingDelete] = useState<string | null>(null);
  const [confirmingDelete, setConfirmingDelete] = useState<string | null>(null);
  const [downloading, setDownloading] = useState<string | null>(null);

  // Restore modal state.
  const [restoreTarget, setRestoreTarget] = useState<BackupFile | null>(null);
  const [restoreConfirm, setRestoreConfirm] = useState("");
  const [restoreResult, setRestoreResult] = useState<RestoreResult | null>(null);
  const [restoreError, setRestoreError] = useState<string | null>(null);
  const [restoreBusy, setRestoreBusy] = useState(false);

  const refresh = useCallback(async () => {
    setLoading(true);
    setListError(null);
    try {
      const r = await client.listBackups();
      // newest-first by createdMs
      setBackups([...r.backups].sort((a, b) => b.createdMs - a.createdMs));
      setRunning(r.running);
    } catch (e) {
      setListError(e instanceof Error ? e.message : String(e));
      setBackups([]);
    } finally {
      setLoading(false);
    }
  }, [client]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  // Poll while a backup is running.
  useEffect(() => {
    if (!running) return;
    const t = setInterval(() => void refresh(), 2000);
    return () => clearInterval(t);
  }, [running, refresh]);

  async function backupNow() {
    setTriggering(true);
    setActionError(null);
    try {
      await client.backupNow();
      await refresh();
    } catch (e) {
      setActionError(e instanceof Error ? e.message : String(e));
    } finally {
      setTriggering(false);
    }
  }

  async function download(file: BackupFile) {
    setDownloading(file.name);
    setActionError(null);
    try {
      const resp = await client.downloadBackup(file.name);
      const blob = await resp.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = file.name;
      a.click();
      URL.revokeObjectURL(url);
    } catch (e) {
      setActionError(e instanceof Error ? e.message : String(e));
    } finally {
      setDownloading(null);
    }
  }

  async function confirmDelete(file: BackupFile) {
    setPendingDelete(file.name);
    setActionError(null);
    try {
      await client.deleteBackup(file.name);
      setConfirmingDelete(null);
      await refresh();
    } catch (e) {
      setActionError(e instanceof Error ? e.message : String(e));
    } finally {
      setPendingDelete(null);
    }
  }

  function openRestore(file: BackupFile) {
    setRestoreTarget(file);
    setRestoreConfirm("");
    setRestoreResult(null);
    setRestoreError(null);
  }

  async function submitRestore() {
    const target = restoreTarget;
    if (!target || restoreConfirm !== target.name) return;
    setRestoreBusy(true);
    setRestoreError(null);
    try {
      const r = await client.restoreBackup(target.name);
      setRestoreResult(r);
    } catch (e) {
      setRestoreError(e instanceof Error ? e.message : String(e));
    } finally {
      setRestoreBusy(false);
    }
  }

  function closeRestore() {
    setRestoreTarget(null);
    setRestoreConfirm("");
    setRestoreResult(null);
    setRestoreError(null);
  }

  const rows = backups;
  const busyRow = (name: string) => pendingDelete === name || downloading === name;

  return (
    <section className={s.page}>
      <Placard>Backups</Placard>
      <div className={s.head}>
        <h1 className={s.title}>Backups</h1>
        <span className={s.count}>{rows.length} dump(s)</span>
        <div className={s.spacer} />
        <Button variant="primary" onClick={() => void backupNow()} disabled={triggering || running}>
          {triggering || running ? "running…" : "Back up now"}
        </Button>
        {(triggering || running) && <Spinner label="backup running" />}
      </div>

      {(listError || actionError) && <p className={s.error}>{listError ?? actionError}</p>}

      {loading && rows.length === 0 ? (
        <Spinner label="loading backups" />
      ) : rows.length === 0 ? (
        <p className={s.muted}>
          No backups yet. Click Back up now, or enable RTDB_BACKUP_ENABLED for scheduled backups.
        </p>
      ) : (
        <div className={s.tableWrap}>
          <table className={s.table}>
            <thead>
              <tr>
                <th>name</th>
                <th>created</th>
                <th>size</th>
                <th>actions</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((file) => {
                const confirming = confirmingDelete === file.name;
                const busy = busyRow(file.name);
                return (
                  <tr key={file.name}>
                    <td className={s.nameCell} title={file.name}>
                      {file.name}
                    </td>
                    <td>
                      <span className={s.abs}>{absolute(file.createdMs)}</span>
                      <span className={s.rel}>{relative(file.createdMs)}</span>
                    </td>
                    <td className="tnum">{formatBytes(file.sizeBytes)}</td>
                    <td>
                      <div className={s.rowActions}>
                        <Button onClick={() => void download(file)} disabled={busy}>
                          {downloading === file.name ? "…" : "download"}
                        </Button>
                        <Button onClick={() => openRestore(file)} disabled={busy}>
                          restore
                        </Button>
                        {confirming ? (
                          <span className={s.confirmInline}>
                            <span className={s.confirmLabel}>delete?</span>
                            <Button
                              variant="danger"
                              onClick={() => void confirmDelete(file)}
                              disabled={busy}
                            >
                              {pendingDelete === file.name ? "…" : "confirm"}
                            </Button>
                            <Button onClick={() => setConfirmingDelete(null)} disabled={busy}>
                              no
                            </Button>
                          </span>
                        ) : (
                          <Button
                            variant="danger"
                            onClick={() => setConfirmingDelete(file.name)}
                            disabled={busy}
                          >
                            delete
                          </Button>
                        )}
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {restoreTarget && (
        <div className={s.overlay}>
          <div className={s.modal} role="dialog" aria-modal="true" aria-label="Restore backup">
            <div className={s.modalHead}>
              <span className={s.modalTitle}>Restore backup</span>
              <span className={s.modalName}>{restoreTarget.name}</span>
            </div>

            <p className={s.warn}>Restore is offline — type the dump name exactly to confirm.</p>

            <label className={s.field}>
              <span className={s.fieldLabel}>dump name to confirm</span>
              <input
                className={s.input}
                type="text"
                value={restoreConfirm}
                onChange={(e) => setRestoreConfirm(e.target.value)}
                autoComplete="off"
                spellCheck={false}
                disabled={restoreBusy || !!restoreResult}
              />
            </label>

            {restoreError && <p className={s.error}>{restoreError}</p>}

            {restoreResult && (
              <div className={s.resultBanner}>
                <p className={s.resultLine}>
                  <span className={s.resultLabel}>target</span>
                  <span className={s.resultValue}>{restoreResult.target}</span>
                </p>
                <p className={s.resultLine}>
                  <span className={s.resultLabel}>cutover</span>
                  <span className={s.resultValue}>{restoreResult.instructions}</span>
                </p>
              </div>
            )}

            <div className={s.modalActions}>
              <Button onClick={closeRestore} disabled={restoreBusy}>
                {restoreResult ? "done" : "cancel"}
              </Button>
              {!restoreResult && (
                <Button
                  variant="danger"
                  onClick={() => void submitRestore()}
                  disabled={restoreBusy || restoreConfirm !== restoreTarget.name}
                >
                  {restoreBusy ? "…" : "restore"}
                </Button>
              )}
            </div>
          </div>
        </div>
      )}
    </section>
  );
}
