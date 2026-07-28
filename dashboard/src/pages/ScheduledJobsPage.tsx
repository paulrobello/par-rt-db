import { useCallback, useEffect, useState } from "react";
import { Button, Placard, Spinner } from "../components/ui";
import { useAdmin } from "../lib/admin";
import type { ScheduleInfo, ScheduleKind, ScheduleStatus, ScheduleWhen } from "../lib/types";
import type { TransactionJson } from "@par-rt-db/client";
import s from "./ScheduledJobsPage.module.css";

type CreateMode = "afterMs" | "cron";

const DEFAULT_TXN = `{
  "steps": [
    { "op": "patch", "table": "users", "id": "k1...", "doc": { "ping": true } }
  ]
}`;

function nextFireLabel(job: ScheduleInfo): string {
  return new Date(job.dueAt).toLocaleString(undefined, { hour12: false });
}

function kindClass(kind: ScheduleKind): string {
  return `${s.kind} ${kind === "cron" ? s.kindCron : ""}`;
}

function statusClass(status: ScheduleStatus): string {
  let extra = "";
  if (status === "paused") extra = s.statusPaused;
  else if (status === "error") extra = s.statusError;
  else if (status === "running") extra = s.statusRunning;
  return `${s.status} ${extra}`;
}

export function ScheduledJobsPage() {
  const { client, databases } = useAdmin();
  const [db, setDb] = useState<string>("");
  const [jobs, setJobs] = useState<ScheduleInfo[]>([]);
  const [loading, setLoading] = useState(false);
  const [listError, setListError] = useState<string | null>(null);

  // Create-form state.
  const [mode, setMode] = useState<CreateMode>("afterMs");
  const [afterMs, setAfterMs] = useState("60000");
  const [cronExpr, setCronExpr] = useState("*/5 * * * *");
  const [txnText, setTxnText] = useState(DEFAULT_TXN);
  const [creating, setCreating] = useState(false);
  const [createError, setCreateError] = useState<string | null>(null);
  const [createOk, setCreateOk] = useState<string | null>(null);

  // Per-row action state.
  const [actionError, setActionError] = useState<string | null>(null);
  const [pendingId, setPendingId] = useState<string | null>(null);
  const [confirmingCancel, setConfirmingCancel] = useState<string | null>(null);

  // Auto-select the first database once the list arrives.
  useEffect(() => {
    if (!db && databases.length > 0) setDb(databases[0]);
  }, [db, databases]);

  const refresh = useCallback(async () => {
    if (!db) return;
    setLoading(true);
    setListError(null);
    try {
      setJobs(await client.listSchedules(db));
    } catch (e) {
      setListError(e instanceof Error ? e.message : String(e));
      setJobs([]);
    } finally {
      setLoading(false);
    }
  }, [client, db]);

  useEffect(() => {
    setJobs([]);
    setActionError(null);
    setConfirmingCancel(null);
    if (db) void refresh();
  }, [db, refresh]);

  function buildWhen(): ScheduleWhen | { error: string } {
    if (mode === "afterMs") {
      const ms = Number(afterMs);
      if (!Number.isFinite(ms) || ms < 0) {
        return { error: "afterMs must be a non-negative number of milliseconds." };
      }
      return { type: "afterMs", ms };
    }
    const expr = cronExpr.trim();
    if (!expr) return { error: "cron expression is required." };
    return { type: "cron", expr };
  }

  async function create() {
    if (!db) return;
    const when = buildWhen();
    if ("error" in when) {
      setCreateError(when.error);
      return;
    }
    let txn: TransactionJson;
    try {
      txn = JSON.parse(txnText) as TransactionJson;
    } catch (e) {
      setCreateError(e instanceof Error ? e.message : String(e));
      return;
    }
    setCreating(true);
    setCreateError(null);
    setCreateOk(null);
    try {
      const { id } = await client.createSchedule(db, when, txn);
      setCreateOk(`scheduled — id ${id.slice(0, 8)}`);
      await refresh();
    } catch (e) {
      setCreateError(e instanceof Error ? e.message : String(e));
    } finally {
      setCreating(false);
    }
  }

  async function pauseOrResume(job: ScheduleInfo) {
    if (!db) return;
    setPendingId(job.id);
    setActionError(null);
    try {
      if (job.status === "paused") {
        await client.resumeSchedule(db, job.id);
      } else {
        await client.pauseSchedule(db, job.id);
      }
      await refresh();
    } catch (e) {
      setActionError(e instanceof Error ? e.message : String(e));
    } finally {
      setPendingId(null);
    }
  }

  async function cancel(job: ScheduleInfo) {
    if (!db) return;
    setPendingId(job.id);
    setActionError(null);
    try {
      await client.cancelSchedule(db, job.id);
      setConfirmingCancel(null);
      await refresh();
    } catch (e) {
      setActionError(e instanceof Error ? e.message : String(e));
    } finally {
      setPendingId(null);
    }
  }

  return (
    <section className={s.page}>
      <Placard>Scheduled jobs</Placard>
      <div className={s.head}>
        <h1 className={s.title}>Scheduled jobs</h1>
        <span className={s.count}>{jobs.length} job(s)</span>
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
        {loading && <Spinner label="loading schedules" />}
      </div>

      {listError && <p className={s.error}>{listError}</p>}
      {actionError && <p className={s.error}>{actionError}</p>}

      {!db ? (
        <p className={s.muted}>select a database.</p>
      ) : loading && jobs.length === 0 ? (
        <p className={s.muted}>loading…</p>
      ) : jobs.length === 0 ? (
        <p className={s.muted}>no scheduled jobs.</p>
      ) : (
        <div className={s.tableWrap}>
          <table className={s.table}>
            <thead>
              <tr>
                <th>id</th>
                <th>kind</th>
                <th>schedule / next fire</th>
                <th>status</th>
                <th>fired</th>
                <th>last error</th>
                <th>actions</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((job) => {
                const paused = job.status === "paused";
                const confirming = confirmingCancel === job.id;
                const busy = pendingId === job.id;
                return (
                  <tr key={job.id}>
                    <td className={s.idCell} title={job.id}>
                      {job.id.slice(0, 8)}
                    </td>
                    <td className={kindClass(job.kind)}>{job.kind}</td>
                    <td>
                      {job.kind === "cron" ? (
                        <>
                          <span className={s.cronExpr}>{job.cron ?? "—"}</span>
                          <br />
                          <span className={s.hint}>next: {nextFireLabel(job)}</span>
                        </>
                      ) : (
                        nextFireLabel(job)
                      )}
                    </td>
                    <td className={statusClass(job.status)}>{job.status}</td>
                    <td className="tnum">{job.firedCount}</td>
                    <td className={s.errCell} title={job.lastError ?? ""}>
                      {job.lastError ?? "—"}
                    </td>
                    <td>
                      <div className={s.rowActions}>
                        <Button
                          onClick={() => void pauseOrResume(job)}
                          disabled={busy || job.status === "running" || job.status === "error"}
                        >
                          {paused ? "resume" : "pause"}
                        </Button>
                        {confirming ? (
                          <span className={s.confirmInline}>
                            <span className={s.confirmLabel}>cancel?</span>
                            <Button
                              variant="danger"
                              onClick={() => void cancel(job)}
                              disabled={busy}
                            >
                              {busy ? "…" : "confirm"}
                            </Button>
                            <Button onClick={() => setConfirmingCancel(null)} disabled={busy}>
                              no
                            </Button>
                          </span>
                        ) : (
                          <Button
                            variant="danger"
                            onClick={() => setConfirmingCancel(job.id)}
                            disabled={busy}
                          >
                            cancel
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

      <section className={s.createBlock}>
        <Placard>Create a scheduled job</Placard>
        <div className={s.toolbar}>
          <div className={s.segment}>
            {(["afterMs", "cron"] as const).map((m) => (
              <button
                key={m}
                type="button"
                onClick={() => setMode(m)}
                className={`${s.segBtn} ${mode === m ? s.segBtnActive : ""}`}
                aria-pressed={mode === m}
              >
                {m === "afterMs" ? "one-shot" : "cron"}
              </button>
            ))}
          </div>
          {mode === "afterMs" ? (
            <label className={s.field}>
              <span className={s.fieldLabel}>afterMs (ms)</span>
              <input
                className={s.input}
                value={afterMs}
                onChange={(e) => setAfterMs(e.target.value)}
              />
            </label>
          ) : (
            <label className={s.field}>
              <span className={s.fieldLabel}>cron (5-field, UTC)</span>
              <input
                className={s.input}
                value={cronExpr}
                onChange={(e) => setCronExpr(e.target.value)}
              />
            </label>
          )}
        </div>

        <textarea
          className={s.editor}
          value={txnText}
          onChange={(e) => setTxnText(e.target.value)}
          spellCheck={false}
          rows={8}
          aria-label="transaction DSL"
        />

        <div className={s.actions}>
          <Button variant="primary" onClick={() => void create()} disabled={creating || !db}>
            {creating ? "scheduling…" : "create"}
          </Button>
          {creating && <Spinner label="scheduling" />}
          <span className={s.warn}>writes when it fires</span>
          {createOk && <span className={s.hint}>{createOk}</span>}
        </div>
        {createError && <p className={s.error}>{createError}</p>}
      </section>
    </section>
  );
}
