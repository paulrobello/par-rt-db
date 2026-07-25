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

export function NotFound() {
  return <Page placard="404" title="Not found" lede="That path is not a known surface." />;
}
