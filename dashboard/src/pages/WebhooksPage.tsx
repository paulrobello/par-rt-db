/** Webhook management — register webhooks and inspect the delivery outbox with retry state. */
import { useCallback, useEffect, useState } from "react";
import { Button, Placard, Spinner } from "../components/ui";
import { useAdmin } from "../lib/admin";
import type { Webhook, WebhookDelivery } from "../lib/types";
import s from "./WebhooksPage.module.css";

function timeLabel(ms: number): string {
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

/** Group events for the table cell — preserves order, joins with `, `. */
function eventsLabel(events: string[]): string {
  return events.length === 0 ? "—" : events.join(", ");
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

export function WebhooksPage() {
  const { client, databases } = useAdmin();
  const [db, setDb] = useState<string>("");
  const [webhooks, setWebhooks] = useState<Webhook[]>([]);
  const [loading, setLoading] = useState(false);
  const [listError, setListError] = useState<string | null>(null);

  // Create-form state.
  const [createUrl, setCreateUrl] = useState("");
  const [createTable, setCreateTable] = useState("");
  const [createEvents, setCreateEvents] = useState("");
  const [createEnabled, setCreateEnabled] = useState(true);
  const [creating, setCreating] = useState(false);
  const [createError, setCreateError] = useState<string | null>(null);
  const [createOk, setCreateOk] = useState<string | null>(null);

  // Per-row action state.
  const [actionError, setActionError] = useState<string | null>(null);
  const [pendingId, setPendingId] = useState<number | null>(null);
  const [confirmingDelete, setConfirmingDelete] = useState<number | null>(null);

  // Inline edit state — when set, the edit panel renders above the create form.
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editUrl, setEditUrl] = useState("");
  const [editTable, setEditTable] = useState("");
  const [editEvents, setEditEvents] = useState("");
  const [editEnabled, setEditEnabled] = useState(true);
  const [editClearTable, setEditClearTable] = useState(false);
  const [saving, setSaving] = useState(false);
  const [editError, setEditError] = useState<string | null>(null);

  // Deliveries drill-down state — when set, a deliveries panel renders with a
  // status filter and a sub-table.
  const [deliveriesForId, setDeliveriesForId] = useState<number | null>(null);
  const [deliveries, setDeliveries] = useState<WebhookDelivery[]>([]);
  const [deliveriesLoading, setDeliveriesLoading] = useState(false);
  const [deliveriesError, setDeliveriesError] = useState<string | null>(null);
  const [deliveriesStatus, setDeliveriesStatus] = useState<string>("");

  // Auto-select the first database once the list arrives.
  useEffect(() => {
    if (!db && databases.length > 0) setDb(databases[0]);
  }, [db, databases]);

  const refresh = useCallback(async () => {
    if (!db) return;
    setLoading(true);
    setListError(null);
    try {
      setWebhooks(await client.listWebhooks(db));
    } catch (e) {
      setListError(e instanceof Error ? e.message : String(e));
      setWebhooks([]);
    } finally {
      setLoading(false);
    }
  }, [client, db]);

  useEffect(() => {
    setWebhooks([]);
    setActionError(null);
    setConfirmingDelete(null);
    setEditingId(null);
    setDeliveriesForId(null);
    if (db) void refresh();
  }, [db, refresh]);

  const loadDeliveries = useCallback(
    async (id: number, status: string) => {
      if (!db) return;
      setDeliveriesLoading(true);
      setDeliveriesError(null);
      try {
        const opts = status ? { status } : {};
        setDeliveries(await client.listDeliveries(db, id, opts));
      } catch (e) {
        setDeliveriesError(e instanceof Error ? e.message : String(e));
        setDeliveries([]);
      } finally {
        setDeliveriesLoading(false);
      }
    },
    [client, db],
  );

  // Refresh deliveries when the target or status filter changes.
  useEffect(() => {
    if (deliveriesForId === null) {
      setDeliveries([]);
      setDeliveriesError(null);
      return;
    }
    void loadDeliveries(deliveriesForId, deliveriesStatus);
  }, [deliveriesForId, deliveriesStatus, loadDeliveries]);

  function startEdit(wh: Webhook) {
    setEditingId(wh.id);
    setEditUrl(wh.url);
    setEditTable(wh.table ?? "");
    setEditEvents(wh.events.join(", "));
    setEditEnabled(wh.enabled);
    setEditClearTable(wh.table === null);
    setEditError(null);
    setDeliveriesForId(null);
  }

  function cancelEdit() {
    setEditingId(null);
    setEditError(null);
  }

  async function create() {
    if (!db) return;
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
      await refresh();
    } catch (e) {
      setCreateError(e instanceof Error ? e.message : String(e));
    } finally {
      setCreating(false);
    }
  }

  async function saveEdit(id: number) {
    if (!db) return;
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
      setEditingId(null);
      await refresh();
    } catch (e) {
      setEditError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  }

  async function remove(wh: Webhook) {
    if (!db) return;
    setPendingId(wh.id);
    setActionError(null);
    try {
      await client.deleteWebhook(db, wh.id);
      setConfirmingDelete(null);
      if (editingId === wh.id) setEditingId(null);
      if (deliveriesForId === wh.id) setDeliveriesForId(null);
      await refresh();
    } catch (e) {
      setActionError(e instanceof Error ? e.message : String(e));
    } finally {
      setPendingId(null);
    }
  }

  return (
    <section className={s.page}>
      <Placard>Webhooks</Placard>
      <div className={s.head}>
        <h1 className={s.title}>Webhooks</h1>
        <span className={s.count}>{webhooks.length} webhook(s)</span>
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
        {loading && <Spinner label="loading webhooks" />}
      </div>

      {listError && <p className={s.error}>{listError}</p>}
      {actionError && <p className={s.error}>{actionError}</p>}

      {!db ? (
        <p className={s.muted}>select a database.</p>
      ) : loading && webhooks.length === 0 ? (
        <p className={s.muted}>loading…</p>
      ) : webhooks.length === 0 ? (
        <p className={s.muted}>no webhooks.</p>
      ) : (
        <div className={s.tableWrap}>
          <table className={s.table}>
            <thead>
              <tr>
                <th>id</th>
                <th>url</th>
                <th>table</th>
                <th>events</th>
                <th>status</th>
                <th>created</th>
                <th>actions</th>
              </tr>
            </thead>
            <tbody>
              {webhooks.map((wh) => {
                const confirming = confirmingDelete === wh.id;
                const busy = pendingId === wh.id;
                return (
                  <tr key={wh.id} className={wh.enabled ? "" : s.rowDisabled}>
                    <td className={s.idCell} title={String(wh.id)}>
                      {wh.id}
                    </td>
                    <td className={s.urlCell} title={wh.url}>
                      {wh.url}
                    </td>
                    <td className={s.tableCell}>{wh.table ?? "all"}</td>
                    <td className={s.eventsCell} title={eventsLabel(wh.events)}>
                      {eventsLabel(wh.events)}
                    </td>
                    <td>
                      <span
                        className={`${s.badge} ${wh.enabled ? s.badgeEnabled : s.badgeDisabled}`}
                      >
                        {wh.enabled ? "enabled" : "disabled"}
                      </span>
                    </td>
                    <td>{timeLabel(wh.createdAt)}</td>
                    <td>
                      <div className={s.rowActions}>
                        <Button onClick={() => startEdit(wh)} disabled={busy}>
                          edit
                        </Button>
                        <Button
                          onClick={() =>
                            setDeliveriesForId(deliveriesForId === wh.id ? null : wh.id)
                          }
                          disabled={busy}
                        >
                          {deliveriesForId === wh.id ? "hide deliveries" : "deliveries"}
                        </Button>
                        {confirming ? (
                          <span className={s.confirmInline}>
                            <span className={s.confirmLabel}>delete?</span>
                            <Button
                              variant="danger"
                              onClick={() => void remove(wh)}
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
                            onClick={() => setConfirmingDelete(wh.id)}
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

      {editingId !== null && (
        <section className={s.editBlock}>
          <Placard>Edit webhook #{editingId}</Placard>
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
              onClick={() => void saveEdit(editingId)}
              disabled={saving || !db}
            >
              {saving ? "saving…" : "save"}
            </Button>
            {saving && <Spinner label="saving" />}
            <Button onClick={() => cancelEdit()} disabled={saving}>
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
      )}

      {deliveriesForId !== null && (
        <section className={s.deliveriesBlock}>
          <Placard>Deliveries — webhook #{deliveriesForId}</Placard>
          <div className={s.deliveriesHead}>
            <label className={s.field}>
              <span className={s.fieldLabel}>status filter</span>
              <select
                className={s.select}
                value={deliveriesStatus}
                onChange={(e) => setDeliveriesStatus(e.target.value)}
              >
                <option value="">— any —</option>
                <option value="pending">pending</option>
                <option value="retrying">retrying</option>
                <option value="delivered">delivered</option>
                <option value="failed">failed</option>
              </select>
            </label>
            <Button
              onClick={() => void loadDeliveries(deliveriesForId, deliveriesStatus)}
              disabled={deliveriesLoading}
            >
              {deliveriesLoading ? "refreshing…" : "refresh"}
            </Button>
            {deliveriesLoading && <Spinner label="loading deliveries" />}
            <Button onClick={() => setDeliveriesForId(null)}>close</Button>
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
      )}

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
    </section>
  );
}
