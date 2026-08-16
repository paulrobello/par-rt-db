/** Workflows — durable run list with per-step timeline, cancel, and start. */

import { Fragment, useCallback, useEffect, useState } from "react";
import { Button, Placard, Spinner } from "../components/ui";
import { useAdmin } from "../lib/admin";
import { toErrorMessage } from "../lib/errors";
import { formatDateTime, formatDuration } from "../lib/format";
import type { WorkflowInfo, WorkflowInfoFull, WorkflowSpec } from "../lib/types";
import s from "./WorkflowsPage.module.css";

/** Auto-refresh interval, the subscription inspector's cadence: runs advance
 * on step boundaries and retry backoffs, so the list re-polls (one cheap
 * admin GET) to keep countdowns and statuses current without a manual click. */
const REFRESH_MS = 3000;

/** `stuck` spans two server statuses (terminal `failed`, or a non-terminal run
 * that has burned attempts of its current step), so it is filtered here, not
 * server-side. A retrying run sits in `pending`/`running` with attempts > 0. */
const STATUS_FILTERS = [
  "all",
  "stuck",
  "pending",
  "running",
  "success",
  "failed",
  "cancelled",
] as const;
type StatusFilter = (typeof STATUS_FILTERS)[number];

const DEFAULT_SPEC = `{
  "name": "example",
  "steps": [
    {
      "txn": {
        "steps": [
          { "op": "insert", "table": "tasks", "doc": { "done": false } }
        ]
      },
      "retry": { "maxAttempts": 3 }
    }
  ]
}`;

function isStuck(w: WorkflowInfo): boolean {
  return (
    w.status === "failed" || ((w.status === "pending" || w.status === "running") && w.attempts > 0)
  );
}

function isTerminal(w: WorkflowInfo): boolean {
  return w.status === "success" || w.status === "failed" || w.status === "cancelled";
}

function statusClass(w: WorkflowInfo): string {
  let extra = "";
  if (w.status === "running") extra = s.statusRunning;
  else if (w.status === "failed") extra = s.statusFailed;
  else if (w.status === "success") extra = s.statusSuccess;
  else if (w.status === "cancelled") extra = s.statusCancelled;
  else if (w.attempts > 0) extra = s.statusRetry;
  return `${s.status} ${extra}`;
}

function statusLabel(w: WorkflowInfo): string {
  if (w.attempts > 0 && (w.status === "pending" || w.status === "running")) {
    return `${w.status}·retry`;
  }
  return w.status;
}

function stepLabel(w: WorkflowInfo): string {
  return `${Math.min(w.currentStep + 1, w.stepCount)}/${w.stepCount}`;
}

function sleepLabel(w: WorkflowInfo, now: number): string {
  if (w.sleepUntil === undefined) return "—";
  const remaining = w.sleepUntil - now;
  return remaining > 0 ? `in ${formatDuration(remaining / 1000)}` : "due";
}

/** Label for the current (not-yet-finished) step of a non-terminal run. */
function currentStepLabel(w: WorkflowInfoFull): string {
  if (w.status === "running") return "in flight";
  if (w.attempts > 0) return `waiting · retry ${w.attempts + 1}`;
  return "waiting";
}

export function WorkflowsPage() {
  const { client, databases } = useAdmin();
  const [db, setDb] = useState<string>("");
  const [filter, setFilter] = useState<StatusFilter>("all");
  const [runs, setRuns] = useState<WorkflowInfo[]>([]);
  const [loading, setLoading] = useState(false);
  const [listError, setListError] = useState<string | null>(null);

  // Expanded row → full run (info + step outcome trail), lazy-fetched on expand.
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [detail, setDetail] = useState<WorkflowInfoFull | null>(null);
  const [detailError, setDetailError] = useState<string | null>(null);

  // Per-row action state. `confirming` arms one inline confirm at a time.
  const [actionError, setActionError] = useState<string | null>(null);
  const [pendingId, setPendingId] = useState<string | null>(null);
  const [confirming, setConfirming] = useState<{ id: string; act: "cancel" | "delete" } | null>(
    null,
  );

  // Start-form state.
  const [specText, setSpecText] = useState(DEFAULT_SPEC);
  const [starting, setStarting] = useState(false);
  const [startError, setStartError] = useState<string | null>(null);
  const [startOk, setStartOk] = useState<string | null>(null);

  // Auto-select the first database once the list arrives.
  useEffect(() => {
    if (!db && databases.length > 0) setDb(databases[0]);
  }, [db, databases]);

  const refresh = useCallback(async () => {
    if (!db) return;
    setLoading(true);
    setListError(null);
    try {
      const opts = filter === "all" || filter === "stuck" ? {} : { status: filter };
      let list = await client.adminListWorkflows(db, opts);
      if (filter === "stuck") list = list.filter(isStuck);
      setRuns(list);
    } catch (e) {
      setListError(toErrorMessage(e));
      setRuns([]);
    } finally {
      setLoading(false);
    }
  }, [client, db, filter]);

  // Fetch on db/filter change (`refresh` is keyed on both, so its identity
  // changes re-run this effect); live: re-poll on the shared cadence.
  useEffect(() => {
    setRuns([]);
    setActionError(null);
    setConfirming(null);
    setExpandedId(null);
    setDetail(null);
    setDetailError(null);
    if (db) void refresh();
  }, [db, refresh]);
  useEffect(() => {
    const id = setInterval(() => void refresh(), REFRESH_MS);
    return () => clearInterval(id);
  }, [refresh]);

  const refreshDetail = useCallback(
    async (id: string) => {
      setDetailError(null);
      try {
        setDetail(await client.adminGetWorkflow(db, id));
      } catch (e) {
        setDetailError(toErrorMessage(e));
        setDetail(null);
      }
    },
    [client, db],
  );

  // Keep an expanded timeline live on the same cadence as the list.
  useEffect(() => {
    if (!expandedId) return;
    const id = setInterval(() => void refreshDetail(expandedId), REFRESH_MS);
    return () => clearInterval(id);
  }, [expandedId, refreshDetail]);

  function toggleExpand(id: string) {
    if (expandedId === id) {
      setExpandedId(null);
      setDetail(null);
      setDetailError(null);
      return;
    }
    setExpandedId(id);
    setDetail(null);
    setDetailError(null);
    void refreshDetail(id);
  }

  async function actOn(id: string, fn: (db: string, id: string) => Promise<unknown>) {
    setPendingId(id);
    setActionError(null);
    try {
      await fn(db, id);
      setConfirming(null);
      await refresh();
      if (expandedId === id) await refreshDetail(id);
    } catch (e) {
      setActionError(toErrorMessage(e));
    } finally {
      setPendingId(null);
    }
  }

  async function start() {
    if (!db) return;
    let spec: WorkflowSpec;
    try {
      spec = JSON.parse(specText) as WorkflowSpec;
    } catch (e) {
      setStartError(toErrorMessage(e));
      return;
    }
    setStarting(true);
    setStartError(null);
    setStartOk(null);
    try {
      const { id } = await client.adminStartWorkflow(db, spec);
      setStartOk(`started — id ${id.slice(0, 8)}`);
      await refresh();
    } catch (e) {
      setStartError(toErrorMessage(e));
    } finally {
      setStarting(false);
    }
  }

  const now = Date.now();

  return (
    <section className={s.page}>
      <Placard>Workflows</Placard>
      <div className={s.head}>
        <h1 className={s.title}>Workflows</h1>
        <span className={s.count}>{runs.length} run(s)</span>
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
        <label className={s.field}>
          <span className={s.fieldLabel}>status</span>
          <select
            className={s.select}
            value={filter}
            onChange={(e) => setFilter(e.target.value as StatusFilter)}
          >
            {STATUS_FILTERS.map((f) => (
              <option key={f} value={f}>
                {f}
              </option>
            ))}
          </select>
        </label>
        <Button variant="primary" onClick={() => void refresh()} disabled={loading || !db}>
          {loading ? "refreshing…" : "refresh"}
        </Button>
        {loading && <Spinner label="loading runs" />}
      </div>

      {listError && <p className={s.error}>{listError}</p>}
      {actionError && <p className={s.error}>{actionError}</p>}

      {!db ? (
        <p className={s.muted}>select a database.</p>
      ) : loading && runs.length === 0 ? (
        <p className={s.muted}>loading…</p>
      ) : runs.length === 0 ? (
        <p className={s.muted}>no workflow runs.</p>
      ) : (
        <div className={s.tableWrap}>
          <table className={s.table}>
            <thead>
              <tr>
                <th>name</th>
                <th>status</th>
                <th>step</th>
                <th>attempts</th>
                <th>sleep until</th>
                <th>last error</th>
                <th>actions</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((w) => {
                const expanded = expandedId === w.id;
                const busy = pendingId === w.id;
                const confirmingThis =
                  confirming !== null && confirming.id === w.id ? confirming.act : null;
                return (
                  <Fragment key={w.id}>
                    <tr>
                      <td className={s.nameCell}>
                        <span className={s.name} title={w.id}>
                          {w.name}
                        </span>
                        <br />
                        <span className={s.hint}>{w.id.slice(0, 8)}</span>
                      </td>
                      <td className={statusClass(w)}>{statusLabel(w)}</td>
                      <td className="tnum">{stepLabel(w)}</td>
                      <td className="tnum">{w.attempts}</td>
                      <td>{sleepLabel(w, now)}</td>
                      <td className={s.errCell} title={w.lastError ?? ""}>
                        {w.lastError ?? "—"}
                      </td>
                      <td>
                        <div className={s.rowActions}>
                          <Button onClick={() => toggleExpand(w.id)} disabled={busy}>
                            {expanded ? "hide" : "steps"}
                          </Button>
                          {confirmingThis ? (
                            <span className={s.confirmInline}>
                              <span className={s.confirmLabel}>{confirmingThis}?</span>
                              <Button
                                variant="danger"
                                onClick={() => {
                                  if (confirmingThis === "cancel") {
                                    void actOn(w.id, (d, i) => client.adminCancelWorkflow(d, i));
                                  } else {
                                    void actOn(w.id, (d, i) => client.adminDeleteWorkflow(d, i));
                                  }
                                }}
                                disabled={busy}
                              >
                                {busy ? "…" : "confirm"}
                              </Button>
                              <Button onClick={() => setConfirming(null)} disabled={busy}>
                                no
                              </Button>
                            </span>
                          ) : (
                            <>
                              <Button
                                variant="danger"
                                onClick={() => setConfirming({ id: w.id, act: "cancel" })}
                                disabled={busy || isTerminal(w)}
                              >
                                cancel
                              </Button>
                              <Button
                                variant="danger"
                                onClick={() => setConfirming({ id: w.id, act: "delete" })}
                                disabled={busy}
                              >
                                delete
                              </Button>
                            </>
                          )}
                        </div>
                      </td>
                    </tr>
                    {expanded && (
                      <tr className={s.detailRow}>
                        <td colSpan={7}>
                          {detailError ? (
                            <p className={s.error}>{detailError}</p>
                          ) : !detail ? (
                            <p className={s.muted}>loading run…</p>
                          ) : (
                            <div className={s.timeline}>
                              {detail.stepOutcomes.map((o) => (
                                <div key={o.stepIndex} className={s.timelineRow}>
                                  <span className={s.tlStep}>step {o.stepIndex + 1}</span>
                                  <span
                                    className={`${s.tlStatus} ${
                                      o.status === "failed" ? s.tlFailed : s.tlSuccess
                                    }`}
                                  >
                                    {o.status}
                                  </span>
                                  <span className={s.tlAttempts}>{o.attempts} attempt(s)</span>
                                  <span className={s.tlAt}>{formatDateTime(o.at)}</span>
                                  {o.error !== undefined && (
                                    <span className={s.tlError} title={o.error}>
                                      {o.error}
                                    </span>
                                  )}
                                </div>
                              ))}
                              {!isTerminal(detail) &&
                                detail.currentStep === detail.stepOutcomes.length && (
                                  <div className={s.timelineRow}>
                                    <span className={s.tlStep}>step {detail.currentStep + 1}</span>
                                    <span className={`${s.tlStatus} ${s.tlRunning}`}>
                                      {currentStepLabel(detail)}
                                    </span>
                                    {detail.sleepUntil !== undefined && (
                                      <span className={s.tlAt}>{sleepLabel(detail, now)}</span>
                                    )}
                                  </div>
                                )}
                            </div>
                          )}
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      <section className={s.createBlock}>
        <Placard>Start a workflow run</Placard>
        <textarea
          className={s.editor}
          value={specText}
          onChange={(e) => setSpecText(e.target.value)}
          spellCheck={false}
          rows={10}
          aria-label="workflow spec"
        />
        <div className={s.actions}>
          <Button variant="primary" onClick={() => void start()} disabled={starting || !db}>
            {starting ? "starting…" : "start"}
          </Button>
          {starting && <Spinner label="starting" />}
          <span className={s.warn}>runs immediately on the selected database</span>
          {startOk && <span className={s.hint}>{startOk}</span>}
        </div>
        {startError && <p className={s.error}>{startError}</p>}
      </section>
    </section>
  );
}
