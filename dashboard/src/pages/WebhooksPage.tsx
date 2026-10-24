import type { RtDbAdminClient } from "@par-rt-db/client";
/** Webhook management — register webhooks and inspect the delivery outbox with retry state. */
import { useEffect, useState } from "react";
import { Button, Placard, Spinner } from "../components/ui";
import { timeLabel, WebhookCreateForm, WebhookEditPanel } from "../components/webhooks";
import { useAdmin } from "../lib/admin";
import { toErrorMessage } from "../lib/errors";
import type { Webhook, WebhookDelivery } from "../lib/types";
import { useAsync } from "../lib/useAsync";
import s from "./WebhooksPage.module.css";

/** Group events for the table cell — preserves order, joins with `, `. */
function eventsLabel(events: string[]): string {
  return events.length === 0 ? "—" : events.join(", ");
}

export function WebhooksPage() {
  const { client, databases } = useAdmin();
  const [db, setDb] = useState<string>("");

  // Per-row action state.
  const [actionError, setActionError] = useState<string | null>(null);
  const [pendingId, setPendingId] = useState<number | null>(null);
  const [confirmingDelete, setConfirmingDelete] = useState<number | null>(null);

  // Inline edit / deliveries drill-down targets — the panels own their form
  // and fetch state; the page tracks only which webhook each is bound to.
  const [editingId, setEditingId] = useState<number | null>(null);
  const [deliveriesForId, setDeliveriesForId] = useState<number | null>(null);

  // Auto-select the first database once the list arrives.
  useEffect(() => {
    if (!db && databases.length > 0) setDb(databases[0]);
  }, [db, databases]);

  const {
    data: webhooks,
    loading,
    error: listError,
    refresh,
    setData: setWebhooks,
  } = useAsync(() => client.listWebhooks(db), [client, db], [] as Webhook[], { enabled: !!db });

  // Switching databases should not show the previous database's webhooks
  // while the new list loads.
  // biome-ignore lint/correctness/useExhaustiveDependencies: deps mirror the useAsync fetcher's own dep list, not this effect's body
  useEffect(() => {
    setWebhooks([]);
    setActionError(null);
    setConfirmingDelete(null);
    setEditingId(null);
    setDeliveriesForId(null);
  }, [db, setWebhooks]);

  function startEdit(wh: Webhook) {
    setEditingId(wh.id);
    setDeliveriesForId(null);
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
      setActionError(toErrorMessage(e));
    } finally {
      setPendingId(null);
    }
  }

  const editing = editingId === null ? null : (webhooks.find((wh) => wh.id === editingId) ?? null);

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

      {editing !== null && (
        <WebhookEditPanel
          key={editing.id}
          client={client}
          db={db}
          webhook={editing}
          onClose={() => setEditingId(null)}
          onSaved={async () => {
            setEditingId(null);
            await refresh();
          }}
        />
      )}

      {deliveriesForId !== null && (
        <WebhookDeliveriesPanel
          client={client}
          db={db}
          webhookId={deliveriesForId}
          onClose={() => setDeliveriesForId(null)}
        />
      )}

      <WebhookCreateForm client={client} db={db} onCreated={refresh} />
    </section>
  );
}

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

interface WebhookDeliveriesPanelProps {
  client: RtDbAdminClient;
  db: string;
  webhookId: number;
  onClose: () => void;
}

function WebhookDeliveriesPanel({ client, db, webhookId, onClose }: WebhookDeliveriesPanelProps) {
  const [statusFilter, setStatusFilter] = useState("");
  const [redelivering, setRedelivering] = useState<number | "all" | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  async function redeliver(id: number | "all"): Promise<void> {
    setRedelivering(id);
    setActionError(null);
    try {
      if (id === "all") await client.redeliverFailed(db, webhookId);
      else await client.redeliverDelivery(db, webhookId, id);
      await refresh();
    } catch (e) {
      setActionError(toErrorMessage(e));
    } finally {
      setRedelivering(null);
    }
  }
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
        <Button
          onClick={() => void redeliver("all")}
          disabled={redelivering !== null || deliveriesLoading}
        >
          {redelivering === "all" ? "redelivering…" : "redeliver all failed"}
        </Button>
        {deliveriesLoading && <Spinner label="loading deliveries" />}
        <Button onClick={() => onClose()}>close</Button>
      </div>
      {actionError && (
        <p role="alert" className={s.error}>
          {actionError}
        </p>
      )}
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
                <th>actions</th>
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
                    <td>
                      {d.status === "failed" && (
                        <Button
                          onClick={() => void redeliver(d.id)}
                          disabled={redelivering !== null}
                        >
                          {redelivering === d.id ? "redelivering…" : "redeliver"}
                        </Button>
                      )}
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
