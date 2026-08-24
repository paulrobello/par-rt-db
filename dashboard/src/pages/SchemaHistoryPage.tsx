/** Schema history — browse past pushed schemas and diff them against the current one. */
import type { SchemaHistoryEntrySummary } from "@par-rt-db/client";
import { useState } from "react";
import { Link, useParams } from "react-router-dom";
import { SchemaHistoryList, SchemaVersionDetail } from "../components/schema-history";
import { Placard, Spinner } from "../components/ui";
import { useAdmin } from "../lib/admin";
import { useAsync } from "../lib/useAsync";
import s from "./SchemaHistoryPage.module.css";

export function SchemaHistoryPage() {
  const { db = "" } = useParams();
  const { client } = useAdmin();

  const {
    data: history,
    loading,
    error: listError,
    refresh,
  } = useAsync(() => client.getSchemaHistory(db), [client, db], [] as SchemaHistoryEntrySummary[]);

  const [selected, setSelected] = useState<number | null>(null);
  // Bumped on every selection (including re-selecting the same version) so the
  // detail panel remounts and its confirm/restore state resets — the behavior
  // the pre-extraction page implemented by resetting that state inline.
  const [selectionEpoch, setSelectionEpoch] = useState(0);

  function selectVersion(v: number) {
    setSelected(v);
    setSelectionEpoch((n) => n + 1);
  }

  return (
    <section className={s.page}>
      <Placard>Schema history · {db}</Placard>
      <h1 className={s.title}>Schema history</h1>
      <div className={s.nav}>
        <Link to={`/dbs/${db}`} className={s.back}>
          ← {db}
        </Link>
        <Link to={`/dbs/${db}/schema`} className={s.back}>
          schema →
        </Link>
      </div>

      {loading ? (
        <Spinner label="loading history" />
      ) : listError ? (
        <p className={s.error}>{listError}</p>
      ) : history.length === 0 ? (
        <p className={s.empty}>No schema history yet.</p>
      ) : (
        <SchemaHistoryList history={history} selected={selected} onSelect={selectVersion} />
      )}

      {selected !== null && (
        <SchemaVersionDetail
          key={`${selected}:${selectionEpoch}`}
          client={client}
          db={db}
          version={selected}
          onRestored={refresh}
        />
      )}
      <span className={s.hint}>
        newest-first · every push, migrate, and restore captures a snapshot
      </span>
    </section>
  );
}
