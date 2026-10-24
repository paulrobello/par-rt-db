/** File storage browser — list blobs, inspect metadata, and preview on-the-fly image transforms. */

import type { TransformOpts } from "@par-rt-db/client";
import { useEffect, useRef, useState } from "react";
import { Button, Placard, Spinner } from "../components/ui";
import { useAdmin } from "../lib/admin";
import { toErrorMessage } from "../lib/errors";
import { formatBytes } from "../lib/format";
import type { FileMeta } from "../lib/types";
import { useAsync } from "../lib/useAsync";
import s from "./StoragePage.module.css";

function timeLabel(ms: number): string {
  return new Date(ms).toLocaleString(undefined, { hour12: false });
}

type ImageSize = "orig" | "lg" | "md" | "sm";

const SIZE_OPTS: Record<Exclude<ImageSize, "orig">, TransformOpts> = {
  lg: { w: 1024, fit: "contain" },
  md: { w: 512, fit: "contain" },
  sm: { w: 128, fit: "cover" },
};

export function StoragePage() {
  const { client, databases } = useAdmin();
  const [db, setDb] = useState<string>("");

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
  // Mint-failure state per blob id: a row whose signed-URL mint failed has its
  // copy button disabled — under RTDB_STORAGE_REQUIRE_SIGNED_URLS a bare
  // /storage/{id} link 403s for the receiver, so no link is copied at all.
  const [copyErrors, setCopyErrors] = useState<Record<string, string>>({});
  // Blob id currently minting (button shows a busy label while in flight).
  const [copyingId, setCopyingId] = useState<string | null>(null);

  // Image-transform size applied to "copy URL" outputs (ENH-014). Page-scoped:
  // a single control in the actions column header sets it, and every row's
  // copy-URL uses that size, so an operator can batch-copy thumbnails at one
  // size without re-selecting per row.
  const [size, setSize] = useState<ImageSize>("orig");

  // Auto-select the first database once the list arrives.
  useEffect(() => {
    if (!db && databases.length > 0) setDb(databases[0]);
  }, [db, databases]);

  const {
    data: files,
    loading,
    error: listError,
    refresh,
    setData: setFiles,
  } = useAsync(() => client.listFiles(db), [client, db], [] as FileMeta[], { enabled: !!db });

  // Switching databases should not show the previous database's files while
  // the new list loads.
  // biome-ignore lint/correctness/useExhaustiveDependencies: deps mirror the useAsync fetcher's own dep list, not this effect's body
  useEffect(() => {
    setFiles([]);
    setActionError(null);
    setConfirmingDelete(null);
    setCopyErrors({});
  }, [db, setFiles]);

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
      setUploadError(toErrorMessage(e));
    } finally {
      setUploading(false);
    }
  }

  /** Mint a signed, time-limited URL for `id` in `db` via the server's
   *  GET /api/storage/{db}/{id}/signed-url. The same-origin HttpOnly session
   *  cookie authenticates the mint (the server's SEC-001 dashboard path); the
   *  admin client has no mint surface, and per ARC-106 this is the dashboard's
   *  one narrow call rather than a parallel HTTP client.
   *  Returns the signed URL string, or throws with a message. */
  async function mintSignedUrl(db: string, id: string, transform: TransformOpts | null) {
    const params = new URLSearchParams();
    if (transform) {
      if (transform.w !== undefined) params.set("w", String(transform.w));
      if (transform.h !== undefined) params.set("h", String(transform.h));
      if (transform.fit !== undefined) params.set("fit", transform.fit);
      if (transform.q !== undefined) params.set("q", String(transform.q));
      if (transform.format !== undefined && transform.format !== "auto")
        params.set("format", transform.format);
    }
    const qs = [...params.keys()].length > 0 ? `?${params.toString()}` : "";
    const response = await fetch(
      `${window.location.origin}/api/storage/${encodeURIComponent(db)}/${encodeURIComponent(id)}/signed-url${qs}`,
      { credentials: "include" },
    );
    const body: unknown = await response.json().catch(() => null);
    if (
      !response.ok ||
      body === null ||
      typeof body !== "object" ||
      !("url" in body) ||
      typeof (body as { url: unknown }).url !== "string"
    ) {
      throw new Error(`signed-URL mint failed (HTTP ${response.status})`);
    }
    return (body as { url: string }).url;
  }

  async function copyPublicUrl(file: FileMeta) {
    if (!db) return;
    setActionError(null);
    setCopyErrors((prev) => {
      if (!(file.id in prev)) return prev;
      const next = { ...prev };
      delete next[file.id];
      return next;
    });
    setCopyingId(file.id);
    try {
      // Mint server-side; never copy a hand-built bare link. Under
      // RTDB_STORAGE_REQUIRE_SIGNED_URLS=true a bare /storage/{id} link 403s
      // for the receiver. Transform opts ride the mint so the server binds
      // them into the signature (SEC-003) — appending them client-side to a
      // signed URL fails verification.
      const url = await mintSignedUrl(db, file.id, size === "orig" ? null : SIZE_OPTS[size]);
      await navigator.clipboard.writeText(url);
      setCopiedId(file.id);
      if (copiedTimer.current) clearTimeout(copiedTimer.current);
      copiedTimer.current = setTimeout(() => setCopiedId(null), 1500);
    } catch (e) {
      // Mint or clipboard failed: disable this row's copy with the reason
      // rather than copying a link that may 403 at the receiver.
      setCopyErrors((prev) => ({ ...prev, [file.id]: toErrorMessage(e) }));
    } finally {
      setCopyingId(null);
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
      setActionError(toErrorMessage(e));
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
                <th>
                  actions
                  <select
                    className={s.sizeSelect}
                    aria-label="copy-URL image size"
                    title="image size applied to copy-URL for every row"
                    value={size}
                    onChange={(e) => setSize(e.target.value as ImageSize)}
                  >
                    <option value="orig">original</option>
                    <option value="lg">large 1024</option>
                    <option value="md">medium 512</option>
                    <option value="sm">small 128</option>
                  </select>
                </th>
              </tr>
            </thead>
            <tbody>
              {files.map((file) => {
                const confirming = confirmingDelete === file.id;
                const busy = pendingId === file.id;
                const copied = copiedId === file.id;
                const copyError = copyErrors[file.id];
                const copying = copyingId === file.id;
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
                        <Button
                          onClick={() => void copyPublicUrl(file)}
                          disabled={busy || copying || copyError !== undefined}
                          title={
                            copyError !== undefined
                              ? `copy disabled: ${copyError} — no dead bare link is copied`
                              : "copies a signed, time-limited URL (it expires)"
                          }
                        >
                          {copying
                            ? "…"
                            : copyError !== undefined
                              ? "copy disabled"
                              : copied
                                ? "copied!"
                                : "copy signed URL"}
                        </Button>
                        {copyError && (
                          <span className={s.confirmInline} role="alert" title={copyError}>
                            <span className={s.confirmLabel}>copy disabled — {copyError}</span>
                          </span>
                        )}
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
