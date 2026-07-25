import type { ReactNode } from "react";
import { Placard } from "./components/ui";
import p from "./pages.module.css";

function Page({ placard, title, lede }: { placard: string; title: string; lede: ReactNode }) {
  return (
    <section className={p.page}>
      <Placard>{placard}</Placard>
      <h1 className={p.pageTitle}>{title}</h1>
      <p className={p.pageLede}>{lede}</p>
    </section>
  );
}

export function DatabasesPage() {
  return (
    <Page
      placard="Databases"
      title="Databases"
      lede="Every database on this instance, with its tables and row counts. The data browser loads next."
    />
  );
}

export function DbPage() {
  return (
    <Page
      placard="Database"
      title="Database"
      lede="The data browser and schema for this database load next."
    />
  );
}

export function MetricsPage() {
  return (
    <Page
      placard="Metrics"
      title="Live instruments"
      lede="Connections, subscriptions, the pool, throughput, and uptime — live."
    />
  );
}

export function OpsPage() {
  return (
    <Page
      placard="Op feed"
      title="Operation feed"
      lede="Every durable document mutation as it commits."
    />
  );
}

export function ConfigPage() {
  return (
    <Page
      placard="Config"
      title="Hot configuration"
      lede="Runtime-mutable knobs (allowed origins, session TTL, max file size) and the admin allowlist."
    />
  );
}

export function AdminsPage() {
  return (
    <Page
      placard="Admins"
      title="Admin allowlist"
      lede="The server-wide admin allowlist — who can open this console."
    />
  );
}

export function NotFound() {
  return <Page placard="404" title="Not found" lede="That path is not a known surface." />;
}
