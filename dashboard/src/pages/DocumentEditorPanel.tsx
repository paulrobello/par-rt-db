/**
 * JSON editor for one document (edit) or a new one (create). Edit saves as
 * `expectVersion` + `replace` through the admin mutate surface, so a concurrent
 * write is rejected instead of clobbered. A server rejection is shown as its
 * wire `{code, message}` envelope and nothing is saved. Every write lands in
 * the change feed, which the History panel reads as the audit trail.
 * Extracted from DataBrowserPage (QA-007).
 */

import { useMemo, useState } from "react";
import { diffDocuments, documentBody } from "../lib/changeFeed";
import { formatWriteError, toErrorMessage } from "../lib/errors";
import { useAdmin } from "../lib/admin";
import { Button, Placard } from "../components/ui";
import s from "./DataBrowserPage.module.css";

export function DocumentEditorPanel({
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
