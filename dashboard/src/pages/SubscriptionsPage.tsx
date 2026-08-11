import { useCallback, useEffect, useState } from "react";
import { Placard, Spinner } from "../components/ui";
import { useAdmin } from "../lib/admin";
import type { DbSubCounters, SubscriptionsResponse } from "../lib/types";
import s from "./SubscriptionsPage.module.css";

/** Auto-refresh interval: the inspector is a *live* view of the registry, so an
 * operator sees subscriptions come and go and counters tick without a manual
 * refresh. Cheap (one admin GET) and read-only. */
const REFRESH_MS = 3000;

/** A subscriber's identity, or "system" for the bypass principals (machine
 * tokens, scheduled jobs, admin) that carry no user identity. */
function principalLabel(p: SubscriptionsResponse["subscriptions"][number]["principal"]): string {
  if (!p) return "system";
  return p.email ?? p.userId ?? "user";
}

export function SubscriptionsPage() {
  const { client, databases } = useAdmin();
  // "" = all databases (the inspector accepts an optional db filter).
  const [db, setDb] = useState<string>("");
  const [data, setData] = useState<SubscriptionsResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      setData(await client.listSubscriptions(db ? { db } : {}));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setData(null);
    } finally {
      setLoading(false);
    }
  }, [client, db]);

  // Fetch on mount and whenever the db filter changes.
  useEffect(() => {
    void refresh();
  }, [refresh]);
  // Live: re-poll so the view stays current without a manual click.
  useEffect(() => {
    const id = setInterval(() => void refresh(), REFRESH_MS);
    return () => clearInterval(id);
  }, [refresh]);

  const subs = data?.subscriptions ?? [];
  const perDb = data?.perDb ?? [];

  return (
    <section className={s.page}>
      <Placard>Subscriptions</Placard>
      <div className={s.head}>
        <h1 className={s.title}>Live subscriptions</h1>
        <span className={s.count}>{subs.length} active</span>
      </div>

      <div className={s.toolbar}>
        <label className={s.field}>
          <span className={s.fieldLabel}>database</span>
          <select className={s.select} value={db} onChange={(e) => setDb(e.target.value)}>
            <option value="">all databases</option>
            {databases.map((name) => (
              <option key={name} value={name}>
                {name}
              </option>
            ))}
          </select>
        </label>
        <button
          type="button"
          className={s.refresh}
          onClick={() => void refresh()}
          disabled={loading}
        >
          {loading && subs.length === 0 ? "refreshing…" : "refresh"}
        </button>
        {loading && subs.length === 0 && <Spinner label="loading subscriptions" />}
      </div>

      {error && <p className={s.error}>{error}</p>}

      {loading && subs.length === 0 ? (
        <p className={s.muted}>loading…</p>
      ) : subs.length === 0 ? (
        <p className={s.muted}>no active subscriptions{db ? ` for ${db}` : ""}.</p>
      ) : (
        <div className={s.tableWrap}>
          <table className={s.table}>
            <thead>
              <tr>
                <th>database</th>
                <th>table</th>
                <th>terminal</th>
                <th>read set</th>
                <th>principal</th>
              </tr>
            </thead>
            <tbody>
              {subs.map((sub, i) => (
                // biome-ignore lint/suspicious/noArrayIndexKey: snapshot rows carry no unique id (the registry keys on connection/queryId, which are not surfaced), so duplicate rows are possible and index is the honest key
                <tr key={`${sub.db}:${sub.table}:${i}`}>
                  <td className={s.mono}>{sub.db}</td>
                  <td className={s.mono}>{sub.table}</td>
                  <td className={s.mono}>{sub.terminal}</td>
                  <td>
                    <span className={s.pill} data-class={sub.readSetClass}>
                      {sub.readSetClass}
                    </span>
                  </td>
                  <td className={s.mono}>{principalLabel(sub.principal)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <h2 className={s.sectionTitle}>invalidation counters</h2>
      <p className={s.lede}>
        Re-runs vs. proven-irrelevant skips per read-set class. The registry keys on (connection,
        queryId), so per-subscriber counts are not tracked — these are the global and per-db totals.{" "}
        <code className={s.code}>missed</code> &gt; 0 is a correctness defect, not a tuning signal.
      </p>

      <div className={s.counters}>
        <Counter label="re-runs" value={data?.subsRerunsTotal} loading={loading && !data} />
        <Counter
          label="skip · point"
          value={data?.subsSkipsPointTotal}
          loading={loading && !data}
        />
        <Counter
          label="skip · indexed"
          value={data?.subsSkipsIndexedTotal}
          loading={loading && !data}
        />
        <Counter
          label="skip · ordered"
          value={data?.subsSkipsOrderedTotal}
          loading={loading && !data}
        />
        <Counter
          label="missed pushes"
          value={data?.subsMissedPushesTotal}
          alarm={(data?.subsMissedPushesTotal ?? 0) > 0}
          loading={loading && !data}
        />
      </div>

      <h3 className={s.subTitle}>per database</h3>
      {perDb.length === 0 ? (
        <p className={s.muted}>no fan-out decisions recorded yet.</p>
      ) : (
        <div className={s.tableWrap}>
          <table className={s.table}>
            <thead>
              <tr>
                <th>database</th>
                <th>re-runs</th>
                <th>skip · point</th>
                <th>skip · indexed</th>
                <th>skip · ordered</th>
                <th>missed</th>
              </tr>
            </thead>
            <tbody>
              {perDb.map((row) => (
                <PerDbRow key={row.db} row={row} />
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

function Counter({
  label,
  value,
  alarm,
  loading,
}: {
  label: string;
  value: number | undefined;
  alarm?: boolean;
  loading: boolean;
}) {
  return (
    <div className={`${s.counter} ${alarm ? s.alarm : ""}`}>
      <span className={s.counterLabel}>{label}</span>
      <span className={s.counterValue}>{loading ? "—" : (value ?? 0).toLocaleString()}</span>
    </div>
  );
}

function PerDbRow({ row }: { row: DbSubCounters }) {
  return (
    <tr>
      <td className={s.mono}>{row.db}</td>
      <td className={s.num}>{row.reruns.toLocaleString()}</td>
      <td className={s.num}>{row.skipsPoint.toLocaleString()}</td>
      <td className={s.num}>{row.skipsIndexed.toLocaleString()}</td>
      <td className={s.num}>{row.skipsOrdered.toLocaleString()}</td>
      <td className={`${s.num} ${row.missed > 0 ? s.alarmText : ""}`}>
        {row.missed.toLocaleString()}
      </td>
    </tr>
  );
}
