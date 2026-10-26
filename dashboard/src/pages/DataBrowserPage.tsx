/** Live data browser — subscribe to a table, paginate via cursor, and render each row's cells. */

import { type ChangeOp, RtDbError, type SchemaJson, type TransactionJson } from "@par-rt-db/client";
import { useEffect, useMemo, useRef, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { Button, LiveValue, Placard, Spinner, StatusLamp } from "../components/ui";
import { useAdmin } from "../lib/admin";
import {
  type DocumentHistory,
  diffDocuments,
  documentBody,
  type FieldDifference,
  loadDocumentHistory,
} from "../lib/changeFeed";
import { toErrorMessage } from "../lib/errors";
import { downloadTextFile, toCsvString, toJsonString } from "../lib/exportResults";
import { formatNumber, formatTime } from "../lib/format";
import { useLiveTable } from "../lib/useLiveTable";
import s from "./DataBrowserPage.module.css";

const TAKE_OPTIONS = [50, 100, 200];
const HISTORY_SCAN_LIMIT = 5000;

/** Surfaces the server's wire `{code, message}` envelope verbatim. */
export function formatWriteError(e: unknown): string {
  return e instanceof RtDbError ? `${e.code}: ${e.message}` : toErrorMessage(e);
}

function cellText(v: unknown): string {
  if (v === null) return "null";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

export function DataBrowserPage() {
  const { db = "", table = "" } = useParams();
  const { client } = useAdmin();
  const [schema, setSchema] = useState<SchemaJson | null>(null);
  const [schemaError, setSchemaError] = useState<string | null>(null);
  const [order, setOrder] = useState<"asc" | "desc">("desc");
  const [take, setTake] = useState(100);
  const [showDeleted, setShowDeleted] = useState(false);
  const isSoftDelete = !!schema?.tables[table]?.softDelete;
  const { docs, loading, error, live, deletedIds, refresh } = useLiveTable(
    db,
    table,
    order,
    take,
    showDeleted && isSoftDelete,
  );

  const fields = useMemo(() => {
    const t = schema?.tables[table];
    return t ? Object.keys(t.fields) : [];
  }, [schema, table]);

  // Newest-row settle: flash the head row when a realtime insert lands at the
  // top (desc order). Skips the initial load, like the op feed does.
  const prevTopId = useRef<string | null>(null);
  const [freshId, setFreshId] = useState<string | null>(null);
  useEffect(() => {
    const topId = docs[0]?._id ?? null;
    if (topId && prevTopId.current !== null && prevTopId.current !== topId) {
      setFreshId(topId);
      const t = setTimeout(() => setFreshId((id) => (id === topId ? null : id)), 700);
      prevTopId.current = topId;
      return () => clearTimeout(t);
    }
    if (topId) prevTopId.current = topId;
  }, [docs]);

  useEffect(() => {
    let cancelled = false;
    client
      .getSchema(db)
      .then((sc) => {
        if (!cancelled) {
          setSchema(sc);
          setSchemaError(null);
        }
      })
      .catch((e) => {
        // Without the schema the browser's field list and insert form can't
        // work — surface why instead of failing silently.
        if (!cancelled) setSchemaError(toErrorMessage(e));
      });
    return () => {
      cancelled = true;
    };
  }, [client, db]);

  // Editor: null = closed, "new" = create, otherwise the document id being edited.
  const [editTarget, setEditTarget] = useState<string | null>(null);
  const editDoc = docs.find((doc) => doc._id === editTarget);
  const [patchOpen, setPatchOpen] = useState(false);
  const [patchId, setPatchId] = useState("");
  const [patchFields, setPatchFields] = useState("{}");
  const [mutError, setMutError] = useState<string | null>(null);
  const [mutBusy, setMutBusy] = useState(false);
  const [confirmId, setConfirmId] = useState<string | null>(null);
  const [historyId, setHistoryId] = useState<string | null>(null);
  const historyDoc = docs.find((doc) => doc._id === historyId);

  async function mutate(steps: TransactionJson["steps"]): Promise<boolean> {
    setMutBusy(true);
    setMutError(null);
    try {
      await client.adminMutate(db, { steps });
      await refresh();
      return true;
    } catch (e) {
      setMutError(formatWriteError(e));
      return false;
    } finally {
      setMutBusy(false);
    }
  }

  async function doPatch() {
    let f: Record<string, unknown>;
    try {
      f = JSON.parse(patchFields);
    } catch {
      setMutError("invalid JSON for fields");
      return;
    }
    if (!patchId.trim()) {
      setMutError("document id is required");
      return;
    }
    if (await mutate([{ op: "patch", table, id: patchId.trim(), fields: f }])) setPatchOpen(false);
  }

  async function doUndelete(id: string) {
    await mutate([{ op: "undelete", table, id }]);
  }

  async function doDelete(id: string) {
    if (await mutate([{ op: "delete", table, id }])) setConfirmId(null);
  }

  const cols = ["_id", ...fields, "_creationTime"];

  return (
    <section className={s.page}>
      <Placard>
        Data · {db} / {table}
      </Placard>
      <h1 className={s.title}>{table}</h1>
      <div className={s.crumbs}>
        <Link to={`/dbs/${db}`} className={s.link}>
          ← {db}
        </Link>
        <Link to={`/dbs/${db}/schema`} className={s.link}>
          schema
        </Link>
      </div>

      <div className={s.toolbar}>
        <div className={s.segment}>
          {(["desc", "asc"] as const).map((o) => (
            <button
              key={o}
              type="button"
              onClick={() => setOrder(o)}
              className={`${s.segBtn} ${order === o ? s.segBtnActive : ""}`}
            >
              {o}
            </button>
          ))}
        </div>
        <label className={s.take}>
          <span>take</span>
          <select
            value={take}
            onChange={(e) => setTake(Number(e.target.value))}
            className={s.select}
          >
            {TAKE_OPTIONS.map((n) => (
              <option key={n} value={n}>
                {n}
              </option>
            ))}
          </select>
        </label>
        <StatusLamp status={live ? "ok" : "warn"} label={live ? "live · /sync" : "polling · 2s"} />
        <span className={s.count}>
          showing <LiveValue value={formatNumber(docs.length)} />
        </span>
        <span className={s.spacer} />
        <Button
          onClick={() => {
            setPatchOpen(false);
            setEditTarget((v) => (v === "new" ? null : "new"));
          }}
        >
          New document
        </Button>
        <Button
          onClick={() => {
            setEditTarget(null);
            setPatchOpen((v) => !v);
          }}
        >
          Patch
        </Button>
        {isSoftDelete && (
          <label className={s.take}>
            <input
              type="checkbox"
              checked={showDeleted}
              onChange={(e) => setShowDeleted(e.target.checked)}
            />
            <span>show deleted</span>
          </label>
        )}
        <Button onClick={() => void refresh()}>Refresh</Button>
        <Button
          disabled={loading || !!error || docs.length === 0}
          onClick={() =>
            downloadTextFile("data-results.csv", toCsvString(cols, docs), "text/csv;charset=utf-8")
          }
        >
          Export CSV
        </Button>
        <Button
          disabled={loading || !!error || docs.length === 0}
          onClick={() =>
            downloadTextFile("data-results.json", toJsonString(docs), "application/json")
          }
        >
          Export JSON
        </Button>
      </div>

      {historyId && (
        <DocumentHistoryPanel
          key={`${db}/${table}/${historyId}`}
          db={db}
          table={table}
          id={historyId}
          current={historyDoc ?? null}
          onClose={() => setHistoryId(null)}
          onRestore={mutate}
          busy={mutBusy}
        />
      )}
      {editTarget && (editTarget === "new" || editDoc) && (
        <DocumentEditorPanel
          key={`${db}/${table}/${editTarget}`}
          db={db}
          table={table}
          doc={editTarget === "new" ? null : (editDoc ?? null)}
          onClose={() => setEditTarget(null)}
          onSaved={async () => {
            await refresh();
          }}
          onOpenHistory={(id) => setHistoryId(id)}
        />
      )}
      {patchOpen && (
        <div className={s.composer}>
          <Placard>Patch document</Placard>
          <input
            className={s.input}
            value={patchId}
            onChange={(e) => setPatchId(e.target.value)}
            placeholder="document _id"
            spellCheck={false}
          />
          <textarea
            className={s.textarea}
            value={patchFields}
            onChange={(e) => setPatchFields(e.target.value)}
            spellCheck={false}
            rows={6}
            placeholder='{"field":"value"}'
          />
          <Button variant="primary" onClick={doPatch} disabled={mutBusy}>
            Patch
          </Button>
        </div>
      )}
      {mutError && <p className={s.error}>{mutError}</p>}
      {schemaError && <p className={s.error}>Failed to load schema: {schemaError}</p>}

      {loading ? (
        <Spinner label="loading documents" />
      ) : error ? (
        <p className={s.error}>{error}</p>
      ) : docs.length === 0 ? (
        <p className={s.empty}>No documents.</p>
      ) : (
        <div className={s.tableWrap}>
          <table className={s.table}>
            <thead>
              <tr>
                {cols.map((c) => {
                  const description = schema?.tables[table]?.fieldDescriptions?.[c];
                  return (
                    <th key={c} title={description}>
                      {c}
                    </th>
                  );
                })}
                <th aria-label="actions"></th>
              </tr>
            </thead>
            <tbody>
              {docs.map((doc) => (
                <tr
                  key={doc._id}
                  className={`${doc._id === freshId ? s.rowFresh : ""} ${deletedIds.has(doc._id) ? s.rowDeleted : ""}`}
                >
                  {cols.map((c) => (
                    <td
                      key={c}
                      className={c === "_id" ? s.idCell : s.cell}
                      title={String(doc[c] ?? "")}
                    >
                      {c === "_id"
                        ? `…${doc._id.slice(-12)}`
                        : c === "_creationTime"
                          ? formatTime(doc._creationTime)
                          : cellText(doc[c])}
                    </td>
                  ))}
                  <td className={s.actions}>
                    {!deletedIds.has(doc._id) && (
                      <button
                        type="button"
                        className={s.linkBtn}
                        onClick={() => {
                          setPatchOpen(false);
                          setEditTarget(doc._id);
                        }}
                      >
                        Edit
                      </button>
                    )}
                    <button
                      type="button"
                      className={s.linkBtn}
                      onClick={() => setHistoryId(doc._id)}
                    >
                      History
                    </button>
                    {deletedIds.has(doc._id) ? (
                      <button
                        type="button"
                        className={s.linkBtn}
                        onClick={() => void doUndelete(doc._id)}
                        disabled={mutBusy}
                      >
                        undelete
                      </button>
                    ) : confirmId === doc._id ? (
                      <>
                        <button
                          type="button"
                          className={s.linkBtnDanger}
                          onClick={() => void doDelete(doc._id)}
                          disabled={mutBusy}
                        >
                          confirm delete
                        </button>
                        <button
                          type="button"
                          className={s.linkBtn}
                          onClick={() => setConfirmId(null)}
                        >
                          cancel
                        </button>
                      </>
                    ) : (
                      <button
                        type="button"
                        className={s.linkBtnDanger}
                        onClick={() => setConfirmId(doc._id)}
                      >
                        delete
                      </button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

const DIFF_CLASS: Record<FieldDifference["kind"], string> = {
  added: s.diffAdded,
  removed: s.diffRemoved,
  changed: s.diffChanged,
};

function DocumentHistoryPanel({
  db,
  table,
  id,
  current,
  onClose,
  onRestore,
  busy,
}: {
  db: string;
  table: string;
  id: string;
  current: Record<string, unknown> | null;
  onClose: () => void;
  onRestore: (steps: TransactionJson["steps"]) => Promise<boolean>;
  busy: boolean;
}) {
  const { client } = useAdmin();
  const [history, setHistory] = useState<DocumentHistory | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [restoreTarget, setRestoreTarget] = useState<{ seq: number; version: number } | null>(null);
  const [restoreMessage, setRestoreMessage] = useState<string | null>(null);
  const request = useRef<AbortController | null>(null);
  // Loads on open through the admin session; the button is a manual reload.
  // The panel remounts per document (key includes db/table/id), so client,
  // db, table, and id are fixed per mount — a mount-only effect is intended.
  // biome-ignore lint/correctness/useExhaustiveDependencies: remount-per-doc
  useEffect(() => {
    void load();
    return () => {
      request.current?.abort();
    };
  }, []);
  const [left, setLeft] = useState<number | null>(null);
  const [right, setRight] = useState<number | null>(null);

  async function load() {
    setLoading(true);
    setError(null);
    request.current?.abort();
    const controller = new AbortController();
    request.current = controller;
    setHistory(null);
    setLeft(null);
    setRight(null);
    setRestoreTarget(null);
    try {
      const result = await loadDocumentHistory(client, db, table, id, controller.signal);
      if (!controller.signal.aborted) setHistory(result);
    } catch (e) {
      if (!controller.signal.aborted) setError(toErrorMessage(e));
    } finally {
      if (!controller.signal.aborted) setLoading(false);
    }
  }

  const entryBySeq = (seq: number | null): ChangeOp | null =>
    (seq !== null && history?.entries.find((entry) => entry.seq === seq)) || null;
  const leftEntry = entryBySeq(left);
  // Right defaults to the document's current state until a version is picked.
  const rightDoc: Record<string, unknown> | null =
    right !== null ? (entryBySeq(right)?.doc ?? null) : current;
  const differences = diffDocuments(leftEntry?.doc ?? null, rightDoc);

  const restore = async (entry: ChangeOp) => {
    if (!entry.doc || restoreTarget?.seq !== entry.seq) return;
    if (
      await onRestore([
        { op: "expectVersion", table, id, version: restoreTarget.version },
        { op: "replace", table, id, doc: documentBody(entry.doc) },
      ])
    ) {
      setRestoreTarget(null);
      setRestoreMessage(`Restored seq ${entry.seq}. Reload history to include the new write.`);
    }
  };

  return (
    <div className={s.composer}>
      <div className={s.historyHead}>
        <Placard>Document history</Placard>
        <button type="button" className={s.linkBtn} onClick={onClose}>
          close
        </button>
      </div>
      <p className={s.historyHint}>
        Reads the durable change feed through your admin session — no machine token. The feed keeps
        a bounded number of rows, so older history may be trimmed.
      </p>
      <div className={s.tokenRow}>
        <Button onClick={() => void load()} disabled={loading}>
          {loading ? "scanning…" : "Load history"}
        </Button>
      </div>
      {error && (
        <p className={s.error}>
          {error} Older history may be trimmed. No missing versions have been inferred.
        </p>
      )}
      {restoreMessage && (
        <p role="status" className={s.historyHint}>
          {restoreMessage}
        </p>
      )}
      {history && (
        <>
          {history.bounded && (
            <p className={s.historyNote}>
              Scan stopped after {HISTORY_SCAN_LIMIT} ops — older history may be trimmed.
            </p>
          )}
          <p className={s.historyHint}>
            Pick two versions to compare; the right side defaults to the current document.
          </p>
          <div className={s.historyCols}>
            <section className={s.historyList} aria-label="Version history">
              {history.entries.length === 0 ? (
                <p className={s.empty}>No changes found within the scanned range.</p>
              ) : (
                history.entries.map((entry) => (
                  <div key={entry.seq} className={s.historyItem}>
                    <span className={s.historyPick}>
                      <input
                        type="radio"
                        name="history-left"
                        checked={left === entry.seq}
                        onChange={() => setLeft(entry.seq)}
                        aria-label={`compare from seq ${entry.seq}`}
                      />
                      <input
                        type="radio"
                        name="history-right"
                        checked={right === entry.seq}
                        onChange={() => setRight(entry.seq)}
                        aria-label={`compare to seq ${entry.seq}`}
                      />
                    </span>
                    <span className={s.historyMeta}>
                      seq {entry.seq} · {entry.kind} · {formatTime(entry.ts)}
                    </span>
                    {restoreTarget?.seq === entry.seq ? (
                      <>
                        <p className={s.historyHint}>
                          Replace the current document body with seq {entry.seq}? Fields absent from
                          this version are removed. Identity and creation time are preserved.
                          Server-stamped fields and schema constraints may change or reject the
                          result.
                        </p>
                        <button
                          type="button"
                          className={s.linkBtnDanger}
                          disabled={busy}
                          onClick={() => void restore(entry)}
                        >
                          confirm restore
                        </button>
                        <button
                          type="button"
                          className={s.linkBtn}
                          onClick={() => setRestoreTarget(null)}
                        >
                          cancel
                        </button>
                      </>
                    ) : (
                      <button
                        type="button"
                        className={s.linkBtn}
                        disabled={!entry.doc || typeof current?._version !== "number" || busy}
                        onClick={() => {
                          if (typeof current?._version === "number")
                            setRestoreTarget({ seq: entry.seq, version: current._version });
                        }}
                      >
                        Restore this version
                      </button>
                    )}
                    <pre className={s.historyDoc}>{JSON.stringify(entry.doc, null, 2)}</pre>
                  </div>
                ))
              )}
            </section>
            <div className={s.historyDiff}>
              <Placard>Differences</Placard>
              {left === null ? (
                <p className={s.empty}>Select a version to compare.</p>
              ) : differences.length === 0 ? (
                <p className={s.empty}>No differences.</p>
              ) : (
                differences.map((d) => (
                  <div key={d.field} className={`${s.diffRow} ${DIFF_CLASS[d.kind]}`}>
                    <span className={s.diffField}>
                      {d.field} ({d.kind})
                    </span>
                    <span className={s.diffVal}>{cellText(d.before)}</span>
                    <span className={s.diffArrow}>→</span>
                    <span className={s.diffVal}>{cellText(d.after)}</span>
                  </div>
                ))
              )}
            </div>
          </div>
        </>
      )}
    </div>
  );
}

/**
 * JSON editor for one document (edit) or a new one (create). Edit saves as
 * `expectVersion` + `replace` through the admin mutate surface, so a concurrent
 * write is rejected instead of clobbered. A server rejection is shown as its
 * wire `{code, message}` envelope and nothing is saved. Every write lands in
 * the change feed, which the History panel reads as the audit trail.
 */
function DocumentEditorPanel({
  db,
  table,
  doc,
  onClose,
  onSaved,
  onOpenHistory,
}: {
  db: string;
  table: string;
  doc: Record<string, unknown> | null;
  onClose: () => void;
  onSaved: () => Promise<void>;
  onOpenHistory: (id: string) => void;
}) {
  const { client } = useAdmin();
  const original = useMemo(() => (doc ? documentBody(doc) : {}), [doc]);
  const [text, setText] = useState(() => JSON.stringify(original, null, 2));
  const [busy, setBusy] = useState(false);
  const [rejection, setRejection] = useState<string | null>(null);
  // Version at open time: the optimistic-concurrency guard for the replace.
  const [openedVersion] = useState(() => (typeof doc?._version === "number" ? doc._version : null));
  const id = typeof doc?._id === "string" ? doc._id : null;

  const parsed = useMemo<
    { ok: true; value: Record<string, unknown> } | { ok: false; reason: string }
  >(() => {
    try {
      const v: unknown = JSON.parse(text);
      if (v === null || typeof v !== "object" || Array.isArray(v))
        return { ok: false, reason: "A document must be a JSON object." };
      return { ok: true, value: v as Record<string, unknown> };
    } catch (e) {
      return { ok: false, reason: toErrorMessage(e) };
    }
  }, [text]);
  const differences = parsed.ok ? diffDocuments(original, parsed.value) : [];
  const canSave = parsed.ok && !busy && (id === null || differences.length > 0);

  async function save() {
    if (!parsed.ok) return;
    setBusy(true);
    setRejection(null);
    try {
      if (id === null) {
        await client.adminMutate(db, { steps: [{ op: "insert", table, doc: parsed.value }] });
      } else {
        await client.adminMutate(db, {
          steps: [
            ...(openedVersion !== null
              ? [{ op: "expectVersion" as const, table, id, version: openedVersion }]
              : []),
            { op: "replace", table, id, doc: parsed.value },
          ],
        });
      }
      await onSaved();
      // Close on success: reopening picks up the new _version for the next guard.
      onClose();
    } catch (e) {
      setRejection(formatWriteError(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <section className={s.composer} aria-label={id ? "Edit document" : "New document"}>
      <div className={s.historyHead}>
        <Placard>{id ? "Edit document" : "New document"}</Placard>
        <button type="button" className={s.linkBtn} onClick={onClose}>
          close
        </button>
      </div>
      {id && (
        <p className={s.historyHint}>
          {id} · v{openedVersion ?? "?"} · replaces the whole body. Fields you remove are removed.
          Saving is rejected if the document changed since you opened it.
        </p>
      )}
      <textarea
        className={`${s.textarea} ${parsed.ok ? "" : s.textareaInvalid}`}
        aria-label="Document JSON"
        aria-invalid={!parsed.ok}
        value={text}
        onChange={(e) => {
          setText(e.target.value);
        }}
        spellCheck={false}
        rows={Math.min(18, Math.max(6, text.split("\n").length + 1))}
      />
      {!parsed.ok && (
        <p className={s.error} role="alert">
          Invalid JSON: {parsed.reason}
        </p>
      )}
      {parsed.ok && id !== null && (
        <p className={s.historyHint}>
          {differences.length === 0
            ? "No changes."
            : `${differences.length} field${differences.length === 1 ? "" : "s"} changed: ${differences
                .map((d) => `${d.field} (${d.kind})`)
                .join(", ")}`}
        </p>
      )}
      {rejection && (
        <p className={s.error} role="alert">
          Rejected, not saved. {rejection}
        </p>
      )}
      <div className={s.tokenRow}>
        <Button variant="primary" onClick={() => void save()} disabled={!canSave}>
          {busy ? "saving…" : id ? "Save changes" : "Create document"}
        </Button>
        {id && (
          <button type="button" className={s.linkBtn} onClick={() => onOpenHistory(id)}>
            View history
          </button>
        )}
      </div>
    </section>
  );
}
