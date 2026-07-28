import type { ReactNode } from "react";
import { Sparkline } from "../components/Sparkline";
import { Placard, Spinner, StatusLamp } from "../components/ui";
import { useAdmin } from "../lib/admin";
import { formatDuration, formatNumber } from "../lib/format";
import { formatRate, lastValue, levelSeries, type Point, rateSeries } from "../lib/metrics-series";
import type { MetricsSnapshot } from "../lib/types";
import { useMetricsHistory } from "../lib/useMetricsHistory";
import s from "./MetricsPage.module.css";

function Instrument({
  label,
  value,
  sub,
  sparkline,
}: {
  label: string;
  value: string;
  sub?: string;
  sparkline?: ReactNode;
}) {
  return (
    <div className={s.instrument}>
      <span className={s.instrumentLabel}>{label}</span>
      <span className={s.instrumentValue}>{value}</span>
      {sub && <span className={s.instrumentSub}>{sub}</span>}
      {sparkline && <div className={s.spark}>{sparkline}</div>}
    </div>
  );
}

function Panel({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className={s.panel}>
      <Placard>{title}</Placard>
      <div className={s.grid}>{children}</div>
    </section>
  );
}

export function MetricsPage() {
  const { metrics } = useAdmin();
  const { samples } = useMetricsHistory();
  if (!metrics) return <Spinner label="reading instruments" />;
  const m: MetricsSnapshot = metrics;
  const poolInUse = Math.max(0, m.poolSize - m.poolIdle);

  const qRate: Point[] = rateSeries(samples, (s) => s.queriesTotal);
  const mRate: Point[] = rateSeries(samples, (s) => s.mutationsTotal);
  const uRate: Point[] = rateSeries(samples, (s) => s.uploadsTotal);
  const wsLevel: Point[] = levelSeries(samples, (s) => s.wsConnections);
  const subsLevel: Point[] = levelSeries(samples, (s) => s.activeSubscriptions);
  const poolBusy: Point[] = levelSeries(samples, (s) => s.poolSize - s.poolIdle);

  return (
    <section className={s.page}>
      <div className={s.head}>
        <h1 className={s.title}>Live instruments</h1>
        <StatusLamp status="ok" label="live · 1s" />
      </div>

      <Panel title="Activity · since start">
        <Instrument
          label="queries"
          value={formatRate(lastValue(qRate) ?? Number.NaN)}
          sub={`${formatNumber(m.queriesTotal)} total`}
          sparkline={
            <Sparkline values={qRate} ariaLabel="queries per second over the last minute" />
          }
        />
        <Instrument
          label="mutations"
          value={formatRate(lastValue(mRate) ?? Number.NaN)}
          sub={`${formatNumber(m.mutationsTotal)} total`}
          sparkline={
            <Sparkline values={mRate} ariaLabel="mutations per second over the last minute" />
          }
        />
        <Instrument
          label="uploads"
          value={formatRate(lastValue(uRate) ?? Number.NaN)}
          sub={`${formatNumber(m.uploadsTotal)} total`}
          sparkline={
            <Sparkline values={uRate} ariaLabel="uploads per second over the last minute" />
          }
        />
      </Panel>

      <Panel title="Live">
        <Instrument
          label="ws connections"
          value={formatNumber(m.wsConnections)}
          sparkline={
            <Sparkline
              values={wsLevel}
              ariaLabel="open websocket connections over the last minute"
            />
          }
        />
        <Instrument
          label="subscriptions"
          value={formatNumber(m.activeSubscriptions)}
          sparkline={
            <Sparkline values={subsLevel} ariaLabel="active subscriptions over the last minute" />
          }
        />
        <Instrument
          label="pool"
          value={formatNumber(m.poolSize)}
          sub={`${formatNumber(poolInUse)} busy · ${formatNumber(m.poolIdle)} idle`}
          sparkline={
            <Sparkline
              values={poolBusy}
              min={0}
              max={m.poolSize || 1}
              ariaLabel="busy connections out of pool size over the last minute"
            />
          }
        />
      </Panel>

      <Panel title="System">
        <Instrument label="uptime" value={formatDuration(m.uptimeSeconds)} />
      </Panel>
    </section>
  );
}
