/** Live op feed — streaming view of durable document mutations across all tap sites. */
import { Link } from "react-router-dom";
import { Placard, StatusLamp } from "../components/ui";
import { useAdmin } from "../lib/admin";
import { formatTime } from "../lib/format";
import type { OpKind } from "../lib/types";
import s from "./OpsPage.module.css";

const KIND_GLYPH: Record<OpKind, string> = {
  insert: "I",
  patch: "P",
  replace: "R",
  delete: "D",
  upsert: "U",
};

export function OpsPage() {
  const { ops } = useAdmin();

  return (
    <section className={s.page}>
      <div className={s.head}>
        <h1 className={s.title}>Operation feed</h1>
        <StatusLamp status="ok" label="live · 1s" />
        <span className={s.count}>{ops.length} recent</span>
      </div>
      <Placard>Every durable document mutation, newest first</Placard>

      {ops.length === 0 ? (
        <p className={s.empty}>No operations yet — mutate a document to see it stream in.</p>
      ) : (
        <div className={s.tableWrap}>
          <table className={s.table}>
            <thead>
              <tr>
                <th aria-label="kind"></th>
                <th>db · table</th>
                <th>doc id</th>
                <th>owner</th>
                <th>time</th>
              </tr>
            </thead>
            <tbody>
              {ops.map((op) => (
                <tr key={`${op.ts}-${op.docId}-${op.kind}`}>
                  <td>
                    <span className={`${s.kind} ${op.kind === "delete" ? s.kindDanger : ""}`}>
                      {KIND_GLYPH[op.kind]}
                    </span>
                  </td>
                  <td className={s.loc}>
                    <Link to={`/dbs/${op.db}`} className={s.link}>
                      {op.db}
                    </Link>
                    <span className={s.sep}>·</span>
                    <Link to={`/dbs/${op.db}/tables/${op.table}`} className={s.link}>
                      {op.table}
                    </Link>
                  </td>
                  <td className={s.doc} title={op.docId}>
                    …{op.docId.slice(-12)}
                  </td>
                  <td className={s.owner}>{op.owner ?? "—"}</td>
                  <td className="tnum">{formatTime(op.ts)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
