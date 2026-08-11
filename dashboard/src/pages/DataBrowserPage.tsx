/** Live data browser — subscribe to a table, paginate via cursor, and render each row's cells. */

import type { SchemaJson, TransactionJson } from "@par-rt-db/client";
import { useEffect, useMemo, useRef, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { Button, LiveValue, Placard, Spinner, StatusLamp } from "../components/ui";
import { useAdmin } from "../lib/admin";
import { toErrorMessage } from "../lib/errors";
import { formatNumber, formatTime } from "../lib/format";
import { useLiveTable } from "../lib/useLiveTable";
import s from "./DataBrowserPage.module.css";

const TAKE_OPTIONS = [50, 100, 200];

function cellText(v: unknown): string {
  if (v === null) return "null";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

export function DataBrowserPage() {
  const { db = "", table = "" } = useParams();
  const { client } = useAdmin();
  const [schema, setSchema] = useState<SchemaJson | null>(null);
  const [order, setOrder] = useState<"asc" | "desc">("desc");
  const [take, setTake] = useState(100);
  const { docs, loading, error, live, refresh } = useLiveTable(db, table, order, take);

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
        if (!cancelled) setSchema(sc);
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, [client, db]);

  const [insertOpen, setInsertOpen] = useState(false);
  const [insertDoc, setInsertDoc] = useState("{}");
  const [patchOpen, setPatchOpen] = useState(false);
  const [patchId, setPatchId] = useState("");
  const [patchFields, setPatchFields] = useState("{}");
  const [mutError, setMutError] = useState<string | null>(null);
  const [mutBusy, setMutBusy] = useState(false);
  const [confirmId, setConfirmId] = useState<string | null>(null);

  async function mutate(steps: TransactionJson["steps"]): Promise<boolean> {
    setMutBusy(true);
    setMutError(null);
    try {
      await client.adminMutate(db, { steps });
      await refresh();
      return true;
    } catch (e) {
      setMutError(toErrorMessage(e));
      return false;
    } finally {
      setMutBusy(false);
    }
  }

  async function doInsert() {
    let doc: Record<string, unknown>;
    try {
      doc = JSON.parse(insertDoc);
    } catch {
      setMutError("invalid JSON for document");
      return;
    }
    if (await mutate([{ op: "insert", table, doc }])) setInsertOpen(false);
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
            setInsertOpen((v) => !v);
          }}
        >
          Insert
        </Button>
        <Button
          onClick={() => {
            setInsertOpen(false);
            setPatchOpen((v) => !v);
          }}
        >
          Patch
        </Button>
        <Button onClick={() => void refresh()}>Refresh</Button>
      </div>

      {insertOpen && (
        <div className={s.composer}>
          <Placard>Insert document</Placard>
          <textarea
            className={s.textarea}
            value={insertDoc}
            onChange={(e) => setInsertDoc(e.target.value)}
            spellCheck={false}
            rows={8}
          />
          <Button variant="primary" onClick={doInsert} disabled={mutBusy}>
            Insert
          </Button>
        </div>
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
                {cols.map((c) => (
                  <th key={c}>{c}</th>
                ))}
                <th aria-label="actions"></th>
              </tr>
            </thead>
            <tbody>
              {docs.map((doc) => (
                <tr key={doc._id} className={doc._id === freshId ? s.rowFresh : undefined}>
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
                    {confirmId === doc._id ? (
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
