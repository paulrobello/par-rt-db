import { useCallback, useEffect, useRef, useState } from "react";
import { Button, Placard, Spinner } from "../components/ui";
import { useAdmin } from "../lib/admin";
import { formatBytes } from "../lib/format";
import type { FileMeta } from "../lib/types";
import s from "./StoragePage.module.css";

function timeLabel(ms: number): string {
  return new Date(ms).toLocaleString(undefined, { hour12: false });
}

export function StoragePage() {
  const { client, databases } = useAdmin();
  const [db, setDb] = useState<string>("");
  const [files, setFiles] = useState<FileMeta[]>([]);
  const [loading, setLoading] = useState(false);
  const [listError, setListError] = useState<string | null>(null);

  // Upload state.
  const [selected, setSelected] = useState<File | null>(null);
  const [uploading, setUploading] = useState(false);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const [uploadOk, setUploadOk] = useState<string | null>(null);

  // Per-row action state.
  const [actionError, setActionError] = useState<string | null>(null);
  const [pendingId, setPendingId] = useState<string | null>(null);
  const [confirmingDelete, setConfirmingDelete] = useState<string | null>(null);
  const [copiedId, setCopiedId] = useState<string | null>(null);
  const copiedTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Auto-select the first database once the list arrives.
  useEffect(() => {
    if (!db && databases.length > 0) setDb(databases[0]);
  }, [db, databases]);

  const refresh = useCallback(async () => {
    if (!db) return;
    setLoading(true);
    setListError(null);
    try {
      setFiles(await client.listFiles(db));
    } catch (e) {
      setListError(e instanceof Error ? e.message : String(e));
      setFiles([]);
    } finally {
      setLoading(false);
    }
  }, [client, db]);

  useEffect(() => {
    setFiles([]);
    setActionError(null);
    setConfirmingDelete(null);
    if (db) void refresh();
  }, [db, refresh]);

  useEffect(() => {
    return () => {
      if (copiedTimer.current) clearTimeout(copiedTimer.current);
    };
  }, []);

  async function upload() {
    if (!db || !selected) return;
    setUploading(true);
    setUploadError(null);
    setUploadOk(null);
    try {
      const { id } = await client.uploadFile(db, selected);
      setUploadOk(`uploaded — id ${id.slice(0, 8)}`);
      setSelected(null);
      await refresh();
    } catch (e) {
      setUploadError(e instanceof Error ? e.message : String(e));
    } finally {
      setUploading(false);
    }
  }

  async function copyPublicUrl(file: FileMeta) {
    try {
      await navigator.clipboard.writeText(`${window.location.origin}/storage/${file.id}`);
      setCopiedId(file.id);
      if (copiedTimer.current) clearTimeout(copiedTimer.current);
      copiedTimer.current = setTimeout(() => setCopiedId(null), 1500);
    } catch {
      // clipboard may be unavailable (insecure context) — surface quietly
      setActionError("clipboard unavailable");
    }
  }

  async function remove(file: FileMeta) {
    if (!db) return;
    setPendingId(file.id);
    setActionError(null);
    try {
      await client.deleteFile(db, file.id);
      setConfirmingDelete(null);
      await refresh();
    } catch (e) {
      setActionError(e instanceof Error ? e.message : String(e));
    } finally {
      setPendingId(null);
    }
  }

  return (
    <section className={s.page}>
      <Placard>Storage</Placard>
      <div className={s.head}>
        <h1 className={s.title}>File storage</h1>
        <span className={s.count}>{files.length} file(s)</span>
      </div>

      <div className={s.toolbar}>
        <label className={s.field}>
          <span className={s.fieldLabel}>database</span>
          <select
            className={s.select}
            value={db}
            onChange={(e) => setDb(e.target.value)}
            disabled={databases.length === 0}
          >
            {databases.length === 0 && <option value="">— none —</option>}
            {databases.map((name) => (
              <option key={name} value={name}>
                {name}
              </option>
            ))}
          </select>
        </label>
        <Button variant="primary" onClick={() => void refresh()} disabled={loading || !db}>
          {loading ? "refreshing…" : "refresh"}
        </Button>
        {loading && <Spinner label="loading files" />}
      </div>

      {listError && <p className={s.error}>{listError}</p>}
      {actionError && <p className={s.error}>{actionError}</p>}

      {!db ? (
        <p className={s.muted}>select a database.</p>
      ) : loading && files.length === 0 ? (
        <p className={s.muted}>loading…</p>
      ) : files.length === 0 ? (
        <p className={s.muted}>no stored files.</p>
      ) : (
        <div className={s.tableWrap}>
          <table className={s.table}>
            <thead>
              <tr>
                <th>id</th>
                <th>size</th>
                <th>content type</th>
                <th>created</th>
                <th>sha256</th>
                <th>actions</th>
              </tr>
            </thead>
            <tbody>
              {files.map((file) => {
                const confirming = confirmingDelete === file.id;
                const busy = pendingId === file.id;
                const copied = copiedId === file.id;
                return (
                  <tr key={file.id}>
                    <td className={s.idCell} title={file.id}>
                      {file.id.slice(0, 8)}
                    </td>
                    <td className="tnum">{formatBytes(file.size)}</td>
                    <td className={s.ctCell}>{file.contentType ?? "—"}</td>
                    <td>{timeLabel(file.creationTime)}</td>
                    <td className={s.shaCell} title={file.sha256}>
                      {file.sha256.slice(0, 12)}
                    </td>
                    <td>
                      <div className={s.rowActions}>
                        <Button onClick={() => void copyPublicUrl(file)} disabled={busy}>
                          {copied ? "copied!" : "copy URL"}
                        </Button>
                        {confirming ? (
                          <span className={s.confirmInline}>
                            <span className={s.confirmLabel}>delete?</span>
                            <Button
                              variant="danger"
                              onClick={() => void remove(file)}
                              disabled={busy}
                            >
                              {busy ? "…" : "confirm"}
                            </Button>
                            <Button onClick={() => setConfirmingDelete(null)} disabled={busy}>
                              no
                            </Button>
                          </span>
                        ) : (
                          <Button
                            variant="danger"
                            onClick={() => setConfirmingDelete(file.id)}
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

      <section className={s.uploadBlock}>
        <Placard>Upload a file</Placard>
        <div className={s.toolbar}>
          <input
            className={s.fileInput}
            type="file"
            onChange={(e) => setSelected(e.target.files?.[0] ?? null)}
            disabled={!db || uploading}
          />
          <Button
            variant="primary"
            onClick={() => void upload()}
            disabled={!db || !selected || uploading}
          >
            {uploading ? "uploading…" : "upload"}
          </Button>
          {uploading && <Spinner label="uploading" />}
          <span className={s.warn}>writes a blob</span>
          {uploadOk && <span className={s.hint}>{uploadOk}</span>}
        </div>
        {uploadError && <p className={s.error}>{uploadError}</p>}
      </section>
    </section>
  );
}
