/** Live data browser — subscribe to a table, paginate via cursor, and render each row's cells. */

import type { SchemaJson } from "@par-rt-db/client";
import { useEffect, useMemo, useRef, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { Button, LiveValue, Placard, Spinner, StatusLamp } from "../components/ui";
import { useAdmin } from "../lib/admin";
import { toErrorMessage } from "../lib/errors";
import { downloadTextFile, toCsvString, toJsonString } from "../lib/exportResults";
import { cellText, formatNumber, formatTime } from "../lib/format";
import { useDocMutations } from "../lib/useDocMutations";
import { useLiveTable } from "../lib/useLiveTable";
import { DocumentEditorPanel } from "./DocumentEditorPanel";
import { DocumentHistoryPanel } from "./DocumentHistoryPanel";
import s from "./DataBrowserPage.module.css";

// Re-exported under the page's historical name (QA-007 moved the definition to
// lib/errors, next to toErrorMessage).
export { formatWriteError } from "../lib/errors";

const TAKE_OPTIONS = [50, 100, 200];

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
  const [historyId, setHistoryId] = useState<string | null>(null);
  const historyDoc = docs.find((doc) => doc._id === historyId);

  const {
    mutate,
    doPatch,
    doUndelete,
    doDelete,
    mutError,
    mutBusy,
    patchOpen,
    setPatchOpen,
    patchId,
    setPatchId,
    patchFields,
    setPatchFields,
    confirmId,
    setConfirmId,
  } = useDocMutations(client, db, table, refresh);

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
