/**
 * Document history panel — scans the durable change feed for one document,
 * lets the operator compare any two versions (right side defaults to the
 * current document), and restores a past version through an
 * `expectVersion` + `replace` pair so a concurrent write is rejected instead
 * of clobbered. Extracted from DataBrowserPage (QA-007).
 */

import type { ChangeOp, TransactionJson } from "@par-rt-db/client";
import { useEffect, useRef, useState } from "react";
import {
  type DocumentHistory,
  diffDocuments,
  documentBody,
  type FieldDifference,
  loadDocumentHistory,
} from "../lib/changeFeed";
import { toErrorMessage } from "../lib/errors";
import { cellText, formatTime } from "../lib/format";
import { useAdmin } from "../lib/admin";
import { Button, Placard } from "../components/ui";
import s from "./DataBrowserPage.module.css";

const HISTORY_SCAN_LIMIT = 5000;

const DIFF_CLASS: Record<FieldDifference["kind"], string> = {
  added: s.diffAdded,
  removed: s.diffRemoved,
  changed: s.diffChanged,
};

export function DocumentHistoryPanel({
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
