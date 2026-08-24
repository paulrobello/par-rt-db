/** Durable audit log viewer — filter the optional audit_log by database, op kind, and write source. */
import { useEffect, useState } from "react";
import { Button, Placard, Spinner } from "../components/ui";
import { useAdmin } from "../lib/admin";
import { formatDateTime } from "../lib/format";
import type { AuditEntry, GetAuditOptions } from "../lib/types";
import { useAsync } from "../lib/useAsync";
import s from "./AuditPage.module.css";

// Known op kinds the server labels rows with. `op` may also be null (system
// writes the server could not label), but the filter is equality-only so the
// dropdown offers just the four named kinds plus "any".
const OP_FILTERS = ["insert", "patch", "replace", "delete"] as const;

// Known write sources (CLAUDE.md op-feed tap sites). The server may emit others
// — the select covers the documented set; fall back to leaving it at "any" and
// the row's literal source still renders in the table.
const SOURCE_FILTERS = ["mutate", "scheduled", "ttl", "migrate"] as const;

const DEFAULT_LIMIT = 100;

/** Op badge tone — matches the AppShell op-feed kind colors so an `insert` in
 *  the audit table reads the same as an `insert` in the live feed. */
function opBadgeClass(op: string): string {
  switch (op) {
    case "insert":
      return s.badgeInsert;
    case "patch":
      return s.badgePatch;
    case "replace":
      return s.badgeReplace;
    case "delete":
      return s.badgeDelete;
    default:
      return "";
  }
}

export function AuditPage() {
  const { client, databases } = useAdmin();
  const [db, setDb] = useState<string>("");

  // Filter state. `table`/`principal` are free-form text, committed (debounced)
  // into `filters` so typing doesn't fire a request per keystroke. `op`/`source`
  // are selects, committed immediately.
  const [filters, setFilters] = useState({
    table: "",
    op: "",
    principal: "",
    source: "",
  });
  const [tableInput, setTableInput] = useState("");
  const [principalInput, setPrincipalInput] = useState("");

  const [limit, setLimit] = useState(DEFAULT_LIMIT);
  const [offset, setOffset] = useState(0);

  // Auto-select the first database once the list arrives.
  useEffect(() => {
    if (!db && databases.length > 0) setDb(databases[0]);
  }, [db, databases]);

  // Debounce the text inputs into the committed filter object, and reset offset
  // to the first page when they settle. 250ms is enough to keep typing smooth
  // without delaying the fetch noticeably.
  useEffect(() => {
    const t = setTimeout(() => {
      setFilters((f) => ({ ...f, table: tableInput, principal: principalInput }));
      setOffset(0);
    }, 250);
    return () => clearTimeout(t);
  }, [tableInput, principalInput]);

  const {
    data: entries,
    loading,
    error: listError,
    refresh,
    setData: setEntries,
  } = useAsync(
    () => {
      const opts: GetAuditOptions = { db, limit, offset };
      if (filters.table) opts.table = filters.table;
      if (filters.op) opts.op = filters.op;
      if (filters.principal) opts.principal = filters.principal;
      if (filters.source) opts.source = filters.source;
      return client.getAudit(opts);
    },
    [client, db, filters, limit, offset],
    [] as AuditEntry[],
    { enabled: !!db },
  );

  // Switching databases (or any other filter/paging change) should not show
  // the previous page's rows while the new one loads.
  // biome-ignore lint/correctness/useExhaustiveDependencies: deps mirror the useAsync fetcher's own dep list, not this effect's body
  useEffect(() => {
    setEntries([]);
  }, [db, filters, limit, offset, setEntries]);

  function selectDb(name: string) {
    setDb(name);
    setOffset(0);
  }
  function selectOp(op: string) {
    setFilters((f) => ({ ...f, op }));
    setOffset(0);
  }
  function selectSource(source: string) {
    setFilters((f) => ({ ...f, source }));
    setOffset(0);
  }
  function selectLimit(next: number) {
    setLimit(next);
    setOffset(0);
  }

  const page = Math.floor(offset / limit) + 1;
  const canPrev = offset > 0;
  // If the page came back short, there are no more rows beyond it.
  const canNext = entries.length >= limit;

  return (
    <section className={s.page}>
      <Placard>Audit</Placard>
      <div className={s.head}>
        <h1 className={s.title}>Audit log</h1>
        <span className={s.count}>{entries.length} row(s)</span>
      </div>

      <div className={s.toolbar}>
        <label className={s.field}>
          <span className={s.fieldLabel}>database</span>
          <select
            className={s.select}
            value={db}
            onChange={(e) => selectDb(e.target.value)}
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
        {loading && <Spinner label="loading audit log" />}
      </div>

      <div className={s.filters}>
        <label className={s.field}>
          <span className={s.fieldLabel}>table</span>
          <input
            className={s.input}
            value={tableInput}
            onChange={(e) => setTableInput(e.target.value)}
            placeholder="users, audit, …"
            aria-label="table filter"
          />
        </label>
        <label className={s.field}>
          <span className={s.fieldLabel}>op</span>
          <select
            className={s.select}
            value={filters.op}
            onChange={(e) => selectOp(e.target.value)}
            aria-label="op filter"
          >
            <option value="">— any —</option>
            {OP_FILTERS.map((op) => (
              <option key={op} value={op}>
                {op}
              </option>
            ))}
          </select>
        </label>
        <label className={s.field}>
          <span className={s.fieldLabel}>principal</span>
          <input
            className={s.input}
            value={principalInput}
            onChange={(e) => setPrincipalInput(e.target.value)}
            placeholder="user@host"
            aria-label="principal filter"
          />
        </label>
        <label className={s.field}>
          <span className={s.fieldLabel}>source</span>
          <select
            className={s.select}
            value={filters.source}
            onChange={(e) => selectSource(e.target.value)}
            aria-label="source filter"
          >
            <option value="">— any —</option>
            {SOURCE_FILTERS.map((src) => (
              <option key={src} value={src}>
                {src}
              </option>
            ))}
          </select>
        </label>
      </div>

      <div className={s.pager}>
        <label className={s.field}>
          <span className={s.fieldLabel}>page size</span>
          <select
            className={s.select}
            value={limit}
            onChange={(e) => selectLimit(Number(e.target.value))}
            aria-label="page size"
          >
            <option value="50">50</option>
            <option value="100">100</option>
            <option value="200">200</option>
          </select>
        </label>
        <Button onClick={() => setOffset((o) => Math.max(0, o - limit))} disabled={!canPrev}>
          prev
        </Button>
        <span className={s.pageInfo}>page {page}</span>
        <Button onClick={() => setOffset((o) => o + limit)} disabled={!canNext}>
          next
        </Button>
      </div>

      {listError && <p className={s.error}>{listError}</p>}

      {!db ? (
        <p className={s.muted}>select a database.</p>
      ) : loading && entries.length === 0 ? (
        <p className={s.muted}>loading…</p>
      ) : entries.length === 0 ? (
        <p className={s.muted}>no audit entries.</p>
      ) : (
        <div className={s.tableWrap}>
          <table className={s.table}>
            <thead>
              <tr>
                <th>time</th>
                <th>db</th>
                <th>table</th>
                <th>op</th>
                <th>principal</th>
                <th>source</th>
                <th>doc id</th>
              </tr>
            </thead>
            <tbody>
              {entries.map((row) => (
                <tr key={row.id}>
                  <td>{formatDateTime(row.tsMs)}</td>
                  <td className={s.nameCell}>{row.db}</td>
                  <td className={s.nameCell}>{row.table}</td>
                  <td>
                    {row.op === null ? (
                      <span className={s.mutedCell}>—</span>
                    ) : (
                      <span className={`${s.badge} ${opBadgeClass(row.op)}`}>{row.op}</span>
                    )}
                  </td>
                  <td className={row.principal === null ? s.mutedCell : s.nameCell}>
                    {row.principal === null ? "—" : row.principal}
                  </td>
                  <td className={s.sourceCell}>{row.source}</td>
                  <td className={s.docIdCell} title={row.docId}>
                    {row.docId}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <span className={s.hint}>
        newest-first · audit logging is opt-in (RTDB_AUDIT_LOG_ENABLED)
      </span>
    </section>
  );
}
