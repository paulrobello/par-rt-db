import type { ReactNode } from "react";
import { Placard, Spinner, StatusLamp } from "../components/ui";
import { useAdmin } from "../lib/admin";
import { formatDuration, formatNumber } from "../lib/format";
import type { MetricsSnapshot } from "../lib/types";
import s from "./MetricsPage.module.css";

function Instrument({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className={s.instrument}>
      <span className={s.instrumentLabel}>{label}</span>
      <span className={s.instrumentValue}>{value}</span>
      {sub && <span className={s.instrumentSub}>{sub}</span>}
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
  if (!metrics) return <Spinner label="reading instruments" />;
  const m: MetricsSnapshot = metrics;
  const poolInUse = Math.max(0, m.poolSize - m.poolIdle);

  return (
    <section className={s.page}>
      <div className={s.head}>
        <h1 className={s.title}>Live instruments</h1>
        <StatusLamp status="ok" label="live · 1s" />
      </div>

      <Panel title="Activity · since start">
        <Instrument label="queries" value={formatNumber(m.queriesTotal)} />
        <Instrument label="mutations" value={formatNumber(m.mutationsTotal)} />
        <Instrument label="uploads" value={formatNumber(m.uploadsTotal)} />
      </Panel>

      <Panel title="Live">
        <Instrument label="ws connections" value={formatNumber(m.wsConnections)} />
        <Instrument label="subscriptions" value={formatNumber(m.activeSubscriptions)} />
        <Instrument
          label="pool"
          value={formatNumber(m.poolSize)}
          sub={`${formatNumber(poolInUse)} busy · ${formatNumber(m.poolIdle)} idle`}
        />
      </Panel>

      <Panel title="System">
        <Instrument label="uptime" value={formatDuration(m.uptimeSeconds)} />
      </Panel>
    </section>
  );
}
