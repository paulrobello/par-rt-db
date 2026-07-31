import type { ReactNode } from "react";
import { Sparkline } from "../components/Sparkline";
import { LiveValue, Placard, Spinner, StatusLamp } from "../components/ui";
import { useAdmin } from "../lib/admin";
import { formatDuration, formatNumber } from "../lib/format";
import {
  formatPercent,
  formatRate,
  lastValue,
  levelSeries,
  type Point,
  rateSeries,
  subsSkipRate,
} from "../lib/metrics-series";
import type { LatencyStats, MetricsSnapshot } from "../lib/types";
import { useMetricsHistory } from "../lib/useMetricsHistory";
import s from "./MetricsPage.module.css";

/** Format a micros latency as milliseconds with two decimals (e.g. 1230us -> "1.23ms"). */
function formatMs(us: number): string {
  return `${(us / 1000).toFixed(2)}ms`;
}

function Instrument({
  label,
  value,
  sub,
  sparkline,
  alarm,
  muted,
}: {
  label: string;
  value: string;
  sub?: string;
  sparkline?: ReactNode;
  /** Unmissable: red border + value, for a defect signal (e.g. a missed push). */
  alarm?: boolean;
  /** Quiet: dims the value, for "not currently measuring" rather than a zero result. */
  muted?: boolean;
}) {
  return (
    <div className={`${s.instrument} ${alarm ? s.instrument_alarm : ""}`}>
      <span className={s.instrumentLabel}>{label}</span>
      <LiveValue
        className={`${s.instrumentValue} ${alarm ? s.instrumentValue_alarm : ""} ${
          muted ? s.instrumentValue_muted : ""
        }`}
        value={value}
      />
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

  const skipsTotal = m.subsSkipsPointTotal + m.subsSkipsIndexedTotal + m.subsSkipsOrderedTotal;
  const skipRate = subsSkipRate(m);
  const verifying = m.subsSkipVerificationsTotal > 0;
  const missedPushes = m.subsMissedPushesTotal;

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

      <Panel title="Subscription invalidation">
        <Instrument
          label="skip rate"
          value={formatPercent(skipRate)}
          sub={`${formatNumber(skipsTotal)} skipped · ${formatNumber(m.subsRerunsTotal)} rerun`}
        />
        <Instrument label="reruns" value={formatNumber(m.subsRerunsTotal)} />
        <Instrument label="point skips" value={formatNumber(m.subsSkipsPointTotal)} />
        <Instrument label="indexed skips" value={formatNumber(m.subsSkipsIndexedTotal)} />
        <Instrument label="ordered skips" value={formatNumber(m.subsSkipsOrderedTotal)} />
        <Instrument
          label="skip verification"
          value={verifying ? formatNumber(m.subsSkipVerificationsTotal) : "off"}
          sub={verifying ? "sampled shadow checks" : "RTDB_SUBS_VERIFY_SKIP_EVERY unset"}
          muted={!verifying}
        />
        <Instrument
          label="missed pushes"
          value={formatNumber(missedPushes)}
          sub={
            verifying ? `of ${formatNumber(m.subsSkipVerificationsTotal)} verified` : "sampling off"
          }
          alarm={missedPushes > 0}
        />
      </Panel>

      <Panel title="System">
        <Instrument label="uptime" value={formatDuration(m.uptimeSeconds)} />
      </Panel>

      <Panel title="Latency · p50 / p95 / p99">
        <LatencyInstrument label="query latency" stats={m.queryLatency} />
        <LatencyInstrument label="mutate latency" stats={m.mutateLatency} />
        <LatencyInstrument label="subscribe latency" stats={m.subscribeLatency} />
      </Panel>
    </section>
  );
}

function LatencyInstrument({ label, stats }: { label: string; stats: LatencyStats }) {
  return (
    <Instrument
      label={label}
      value={formatMs(stats.p50)}
      sub={`p95 ${formatMs(stats.p95)} · p99 ${formatMs(stats.p99)}`}
    />
  );
}
