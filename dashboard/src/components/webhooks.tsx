/** Webhook page sub-panels (QA-201) — the create form, inline edit panel, and
 *  deliveries drill-down extracted from WebhooksPage so each owns its form
 *  state; the page keeps the db selector, list table, and row actions. */
import type { RtDbAdminClient } from "@par-rt-db/client";
import { useState } from "react";
import { toErrorMessage } from "../lib/errors";
import type { Webhook, WebhookDelivery } from "../lib/types";
import { useAsync } from "../lib/useAsync";
import s from "../pages/WebhooksPage.module.css";
import { Button, Placard, Spinner } from "./ui";

export function timeLabel(ms: number): string {
  return new Date(ms).toLocaleString(undefined, { hour12: false });
}

/** Split a comma-separated events string into a trimmed, de-duplicated list,
 *  dropping empties. Empty input yields `[]` so the caller can decide whether
 *  to send the key at all (create defaults to `["*"]` server-side). */
function parseEvents(text: string): string[] {
  const out: string[] = [];
  for (const raw of text.split(",")) {
    const v = raw.trim();
    if (v && !out.includes(v)) out.push(v);
  }
  return out;
}

/** Status badge tone for a delivery row. Tolerates unknown values by falling
 *  back to the neutral badge. */
function deliveryBadgeClass(status: string): string {
  switch (status) {
    case "delivered":
      return s.badgeDelivered;
    case "pending":
      return s.badgePending;
    case "retrying":
      return s.badgeRetrying;
    case "failed":
      return s.badgeFailed;
    default:
      return "";
  }
}

/** A one-line preview of the delivery payload — JSON-stringified and truncated
 *  for the table cell; the title attribute carries the full text on hover. */
function payloadPreview(payload: unknown): { title: string; text: string } {
  const title = (() => {
    try {
      return JSON.stringify(payload);
    } catch {
      return String(payload);
    }
  })();
  return { title, text: title.length > 60 ? `${title.slice(0, 60)}…` : title };
}

interface WebhookCreateFormProps {
  client: RtDbAdminClient;
  db: string;
  /** Called after a successful create (the page refreshes the list). */
  onCreated: () => void | Promise<void>;
}

export function WebhookCreateForm({ client, db, onCreated }: WebhookCreateFormProps) {
  const [createUrl, setCreateUrl] = useState("");
  const [createTable, setCreateTable] = useState("");
  const [createEvents, setCreateEvents] = useState("");
  const [createEnabled, setCreateEnabled] = useState(true);
  const [creating, setCreating] = useState(false);
  const [createError, setCreateError] = useState<string | null>(null);
  const [createOk, setCreateOk] = useState<string | null>(null);

  async function create() {
    const url = createUrl.trim();
    if (!url) {
      setCreateError("url is required.");
      return;
    }
    const events = parseEvents(createEvents);
    // Send only the keys that differ from the server defaults so the body stays
    // minimal: `table` only when set, `events` only when not `["*"]`, `enabled`
    // only when false (server default is true).
    const opts: {
      url: string;
      table?: string;
      events?: string[];
      enabled?: boolean;
    } = { url };
    if (createTable.trim()) opts.table = createTable.trim();
    if (!(events.length === 1 && events[0] === "*")) opts.events = events;
    if (!createEnabled) opts.enabled = false;

    setCreating(true);
    setCreateError(null);
    setCreateOk(null);
    try {
      const { id } = await client.createWebhook(db, opts);
      setCreateOk(`created — id ${id}`);
      setCreateUrl("");
      setCreateTable("");
      setCreateEvents("");
      setCreateEnabled(true);
      await onCreated();
    } catch (e) {
      setCreateError(toErrorMessage(e));
    } finally {
      setCreating(false);
    }
  }

  return (
    <section className={s.createBlock}>
      <Placard>Create a webhook</Placard>
      <div className={s.toolbar}>
        <label className={s.field}>
          <span className={s.fieldLabel}>url</span>
          <input
            className={s.input}
            value={createUrl}
            onChange={(e) => setCreateUrl(e.target.value)}
            placeholder="https://example.com/hook"
            aria-label="url"
          />
        </label>
        <label className={s.field}>
          <span className={s.fieldLabel}>table (empty = all tables)</span>
          <input
            className={s.input}
            value={createTable}
            onChange={(e) => setCreateTable(e.target.value)}
            placeholder="users, audit, …"
            aria-label="create table"
          />
        </label>
        <label className={s.field}>
          <span className={s.fieldLabel}>events (comma-separated, empty = *)</span>
          <input
            className={s.input}
            value={createEvents}
            onChange={(e) => setCreateEvents(e.target.value)}
            placeholder="insert, patch, …"
            aria-label="create events"
          />
        </label>
        <label className={s.field}>
          <span className={s.fieldLabel}>enabled</span>
          <div className={s.segment}>
            <button
              type="button"
              onClick={() => setCreateEnabled(true)}
              className={`${s.segBtn} ${createEnabled ? s.segBtnActive : ""}`}
              aria-pressed={createEnabled}
            >
              enabled
            </button>
            <button
              type="button"
              onClick={() => setCreateEnabled(false)}
              className={`${s.segBtn} ${!createEnabled ? s.segBtnActive : ""}`}
              aria-pressed={!createEnabled}
            >
              disabled
            </button>
          </div>
        </label>
      </div>

      <div className={s.actions}>
        <Button variant="primary" onClick={() => void create()} disabled={creating || !db}>
          {creating ? "creating…" : "create"}
        </Button>
        {creating && <Spinner label="creating" />}
        <span className={s.warn}>writes on matching doc changes</span>
        {createOk && <span className={s.hint}>{createOk}</span>}
      </div>
      {createError && <p className={s.error}>{createError}</p>}
    </section>
  );
}

interface WebhookEditPanelProps {
  client: RtDbAdminClient;
  db: string;
  /** The webhook being edited — the panel's fields initialize from it. Remount
   *  (via `key`) when a different row enters edit mode. */
  webhook: Webhook;
  onClose: () => void;
  /** Called after a successful save (the page closes the panel and refreshes). */
  onSaved: () => void | Promise<void>;
}

export function WebhookEditPanel({ client, db, webhook, onClose, onSaved }: WebhookEditPanelProps) {
  const [editUrl, setEditUrl] = useState(webhook.url);
  const [editTable, setEditTable] = useState(webhook.table ?? "");
  const [editEvents, setEditEvents] = useState(webhook.events.join(", "));
  const [editEnabled, setEditEnabled] = useState(webhook.enabled);
  const [editClearTable, setEditClearTable] = useState(webhook.table === null);
  const [saving, setSaving] = useState(false);
  const [editError, setEditError] = useState<string | null>(null);

  async function saveEdit(id: number) {
    const url = editUrl.trim();
    if (!url) {
      setEditError("url must not be empty.");
      return;
    }
    // Build the partial body — only provided keys. `table` is a tri-state:
    //   editClearTable true    -> null (clear to all-tables)
    //   editClearTable false   -> the trimmed value, or omitted if blank
    // `events` is sent when it parses to anything other than `["*"]`. `enabled`
    // is always sent so toggling back to true works.
    const events = parseEvents(editEvents);
    const opts: {
      url?: string;
      table?: string | null;
      events?: string[];
      enabled?: boolean;
    } = { url };
    if (editClearTable) {
      opts.table = null;
    } else if (editTable.trim()) {
      opts.table = editTable.trim();
    }
    if (!(events.length === 1 && events[0] === "*")) opts.events = events;
    opts.enabled = editEnabled;

    setSaving(true);
    setEditError(null);
    try {
      await client.editWebhook(db, id, opts);
      await onSaved();
    } catch (e) {
      setEditError(toErrorMessage(e));
    } finally {
      setSaving(false);
    }
  }

  return (
    <section className={s.editBlock}>
      <Placard>Edit webhook #{webhook.id}</Placard>
      <div className={s.toolbar}>
        <label className={s.field}>
          <span className={s.fieldLabel}>url</span>
          <input
            className={s.input}
            value={editUrl}
            onChange={(e) => setEditUrl(e.target.value)}
            aria-label="url"
          />
        </label>
        <label className={s.field}>
          <span className={s.fieldLabel}>table (empty = all tables, or tick “clear”)</span>
          <input
            className={s.input}
            value={editTable}
            onChange={(e) => {
              setEditTable(e.target.value);
              if (e.target.value) setEditClearTable(false);
            }}
            placeholder="users, audit, …"
            aria-label="table"
            disabled={editClearTable}
          />
        </label>
        <label className={s.field}>
          <span className={s.fieldLabel}>clear table to all-tables</span>
          <input
            type="checkbox"
            checked={editClearTable}
            onChange={(e) => setEditClearTable(e.target.checked)}
            aria-label="clear table"
          />
        </label>
        <label className={s.field}>
          <span className={s.fieldLabel}>events (comma-separated)</span>
          <input
            className={s.input}
            value={editEvents}
            onChange={(e) => setEditEvents(e.target.value)}
            placeholder="insert, patch, …  (or *)"
            aria-label="events"
          />
        </label>
        <label className={s.field}>
          <span className={s.fieldLabel}>enabled</span>
          <div className={s.segment}>
            <button
              type="button"
              onClick={() => setEditEnabled(true)}
              className={`${s.segBtn} ${editEnabled ? s.segBtnActive : ""}`}
              aria-pressed={editEnabled}
            >
              enabled
            </button>
            <button
              type="button"
              onClick={() => setEditEnabled(false)}
              className={`${s.segBtn} ${!editEnabled ? s.segBtnActive : ""}`}
              aria-pressed={!editEnabled}
            >
              disabled
            </button>
          </div>
        </label>
      </div>
      <div className={s.actions}>
        <Button
          variant="primary"
          onClick={() => void saveEdit(webhook.id)}
          disabled={saving || !db}
        >
          {saving ? "saving…" : "save"}
        </Button>
        {saving && <Spinner label="saving" />}
        <Button onClick={() => onClose()} disabled={saving}>
          cancel
        </Button>
        <span className={s.hint}>
          {editClearTable
            ? "table will be cleared to all-tables"
            : "leave table blank to leave the filter unchanged"}
        </span>
      </div>
      {editError && <p className={s.error}>{editError}</p>}
    </section>
  );
}

interface WebhookDeliveriesPanelProps {
  client: RtDbAdminClient;
  db: string;
  webhookId: number;
  onClose: () => void;
}

export function WebhookDeliveriesPanel({
  client,
  db,
  webhookId,
  onClose,
}: WebhookDeliveriesPanelProps) {
  const [statusFilter, setStatusFilter] = useState("");
  const {
    data: deliveries,
    loading: deliveriesLoading,
    error: deliveriesError,
    refresh,
  } = useAsync(
    () => client.listDeliveries(db, webhookId, statusFilter ? { status: statusFilter } : {}),
    [client, db, webhookId, statusFilter],
    [] as WebhookDelivery[],
  );

  return (
    <section className={s.deliveriesBlock}>
      <Placard>Deliveries — webhook #{webhookId}</Placard>
      <div className={s.deliveriesHead}>
        <label className={s.field}>
          <span className={s.fieldLabel}>status filter</span>
          <select
            className={s.select}
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
          >
            <option value="">— any —</option>
            <option value="pending">pending</option>
            <option value="retrying">retrying</option>
            <option value="delivered">delivered</option>
            <option value="failed">failed</option>
          </select>
        </label>
        <Button onClick={() => void refresh()} disabled={deliveriesLoading}>
          {deliveriesLoading ? "refreshing…" : "refresh"}
        </Button>
        {deliveriesLoading && <Spinner label="loading deliveries" />}
        <Button onClick={() => onClose()}>close</Button>
      </div>
      {deliveriesError && <p className={s.error}>{deliveriesError}</p>}
      {deliveries.length === 0 ? (
        <p className={s.muted}>no deliveries.</p>
      ) : (
        <div className={s.subTableWrap}>
          <table className={s.table}>
            <thead>
              <tr>
                <th>id</th>
                <th>status</th>
                <th>attempts</th>
                <th>next attempt</th>
                <th>last error</th>
                <th>payload</th>
              </tr>
            </thead>
            <tbody>
              {deliveries.map((d) => {
                const preview = payloadPreview(d.payload);
                return (
                  <tr key={d.id}>
                    <td className={s.idCell}>{d.id}</td>
                    <td>
                      <span className={`${s.badge} ${deliveryBadgeClass(d.status)}`}>
                        {d.status}
                      </span>
                    </td>
                    <td className="tnum">{d.attempts}</td>
                    <td>{timeLabel(d.nextAttempt)}</td>
                    <td className={s.errCell} title={d.lastError ?? ""}>
                      {d.lastError ?? "—"}
                    </td>
                    <td className={s.payloadCell} title={preview.title}>
                      {preview.text}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
