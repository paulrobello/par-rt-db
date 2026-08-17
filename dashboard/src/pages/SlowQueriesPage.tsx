/** Slow-query log + query plan introspection (ENH-019).
 *  Surfaces the server's bounded ring buffer of queries that exceeded
 *  RTDB_SLOW_QUERY_MS, and an inline explain panel that compiles a Query JSON
 *  DSL to its SQL plan + bind values + compile-time warnings. */
import type { ExplainResult, QueryJson, SlowQueriesResponse } from "@par-rt-db/client";
import { useEffect, useState } from "react";
import { Button, Placard, Spinner } from "../components/ui";
import { RtDbRequestError, useAdmin } from "../lib/admin";
import { toErrorMessage } from "../lib/errors";
import { formatDateTime } from "../lib/format";
import { useAsync } from "../lib/useAsync";
import s from "./SlowQueriesPage.module.css";

export function SlowQueriesPage() {
  return (
    <section className={s.page}>
      <SlowQueriesList />
      <ExplainPanel />
    </section>
  );
}

/** Formats a millisecond duration; colors durations >= 1s as a warning so an
 *  operator can scan for the costly outliers at a glance. */
function durationCell(ms: number): { text: string; warn: boolean } {
  return { text: ms >= 1000 ? `${(ms / 1000).toFixed(2)} s` : `${ms} ms`, warn: ms >= 1000 };
}

function SlowQueriesList() {
  const { client, databases } = useAdmin();
  // "" = all databases (the endpoint accepts an optional db filter).
  const [db, setDb] = useState<string>("");
  const [expanded, setExpanded] = useState<string | null>(null);

  // Fetch on mount and whenever the db filter changes.
  const { data, loading, error, refresh } = useAsync(
    () => client.getSlowQueries(db ? { db } : {}),
    [client, db],
    null as SlowQueriesResponse | null,
  );

  const queries = data?.queries ?? [];
  const disabled = data?.thresholdMs === 0;

  return (
    <div className={s.block}>
      <Placard>Introspection</Placard>
      <div className={s.head}>
        <h1 className={s.title}>Slow queries</h1>
        <span className={s.count}>{queries.length} recorded</span>
      </div>

      {data && (
        <p className={s.lede}>
          threshold <code className={s.code}>{data.thresholdMs} ms</code>
          {" · "}ring capacity <code className={s.code}>{data.capacity}</code>
          {" · "}newest-first
        </p>
      )}

      {disabled && (
        <p className={s.banner}>
          Slow-query logging is disabled (<code className={s.code}>RTDB_SLOW_QUERY_MS=0</code>). Set
          a non-zero threshold to start recording.
        </p>
      )}

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
        <Button variant="primary" onClick={() => void refresh()} disabled={loading}>
          {loading ? "refreshing…" : "refresh"}
        </Button>
        {loading && <Spinner label="loading slow queries" />}
      </div>

      {error && <p className={s.error}>{error}</p>}

      {loading && queries.length === 0 ? (
        <p className={s.muted}>loading…</p>
      ) : queries.length === 0 ? (
        <p className={s.muted}>
          {disabled
            ? "slow-query logging is disabled."
            : `no slow queries recorded${db ? ` for ${db}` : ""}.`}
        </p>
      ) : (
        <div className={s.tableWrap}>
          <table className={s.table}>
            <thead>
              <tr>
                <th aria-label="expand" />
                <th>time</th>
                <th>db</th>
                <th>table</th>
                <th>terminal</th>
                <th>duration</th>
              </tr>
            </thead>
            <tbody>
              {queries.map((row, i) => {
                const key = `${row.startedAtMs}-${row.db}-${i}`;
                const isOpen = expanded === key;
                const dur = durationCell(row.durationMs);
                return (
                  <RowFragment
                    key={key}
                    row={row}
                    isOpen={isOpen}
                    onToggle={() => setExpanded(isOpen ? null : key)}
                    durationText={dur.text}
                    durationWarn={dur.warn}
                  />
                );
              })}
            </tbody>
          </table>
        </div>
      )}
      <span className={s.hint}>
        ring buffer · params are redacted by default (RTDB_SLOW_QUERY_LOG_PARAMS=false)
      </span>
    </div>
  );
}

function RowFragment({
  row,
  isOpen,
  onToggle,
  durationText,
  durationWarn,
}: {
  row: SlowQueriesResponse["queries"][number];
  isOpen: boolean;
  onToggle: () => void;
  durationText: string;
  durationWarn: boolean;
}) {
  return (
    <>
      <tr className={s.dataRow} onClick={onToggle}>
        <td className={s.chevron}>{isOpen ? "▾" : "▸"}</td>
        <td>{formatDateTime(row.startedAtMs)}</td>
        <td className={s.nameCell}>{row.db}</td>
        <td className={s.nameCell}>{row.table}</td>
        <td className={s.monoCell}>{row.terminal}</td>
        <td className={`${s.num} ${durationWarn ? s.warnText : ""}`}>{durationText}</td>
      </tr>
      {isOpen && (
        <tr>
          <td colSpan={6} className={s.detailCell}>
            <div className={s.detailBody}>
              <div className={s.detailSection}>
                <span className={s.detailLabel}>SQL</span>
                <pre className={s.sql}>
                  <code>{row.sql}</code>
                </pre>
              </div>
              <div className={s.detailSection}>
                <span className={s.detailLabel}>params</span>
                {row.params ? (
                  <pre className={s.params}>
                    <code>{row.params.map((p, i) => `$${i + 1} = ${p}`).join("\n")}</code>
                  </pre>
                ) : (
                  <p className={s.mutedDetail}>
                    params redacted (
                    <code className={s.code}>RTDB_SLOW_QUERY_LOG_PARAMS=false</code>)
                  </p>
                )}
              </div>
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

const DEFAULT_QUERY = `{
  "table": "users",
  "take": 100
}`;

type ExplainOutput =
  | { kind: "result"; data: ExplainResult }
  | { kind: "error"; code: string; status: number | null; message: string }
  | null;

function ExplainPanel() {
  const { client, databases } = useAdmin();
  const [db, setDb] = useState<string>("");
  const [text, setText] = useState<string>(DEFAULT_QUERY);
  const [output, setOutput] = useState<ExplainOutput>(null);
  const [loading, setLoading] = useState(false);

  // Auto-select the first database once the list arrives.
  useEffect(() => {
    if (!db && databases.length > 0) setDb(databases[0]);
  }, [db, databases]);

  async function run() {
    if (!db) {
      setOutput({
        kind: "error",
        code: "NO_DATABASE",
        status: null,
        message: "Select a database first.",
      });
      return;
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(text);
    } catch (e) {
      setOutput({
        kind: "error",
        code: "INVALID_JSON",
        status: null,
        message: toErrorMessage(e),
      });
      return;
    }
    setLoading(true);
    try {
      const result = await client.explainQuery(db, parsed as QueryJson);
      setOutput({ kind: "result", data: result });
    } catch (e) {
      if (e instanceof RtDbRequestError) {
        setOutput({
          kind: "error",
          code: e.code,
          status: e.status ?? null,
          message: e.message,
        });
      } else {
        setOutput({
          kind: "error",
          code: "INTERNAL",
          status: null,
          message: toErrorMessage(e),
        });
      }
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className={s.block}>
      <Placard>Explain</Placard>
      <h2 className={s.sectionTitle}>Compile a query plan</h2>
      <p className={s.lede}>
        Paste a Query JSON DSL to compile it server-side and inspect the generated SQL, bind values,
        and compile-time warnings (e.g. unindexed-filter). No rows are returned — this is a plan,
        not a read.
      </p>

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
      </div>

      <textarea
        className={s.editor}
        value={text}
        onChange={(e) => setText(e.target.value)}
        spellCheck={false}
        rows={8}
        aria-label="Query DSL JSON"
      />

      <div className={s.actions}>
        <Button variant="primary" onClick={run} disabled={loading || !db}>
          {loading ? "explaining…" : "explain"}
        </Button>
        {loading && <Spinner label="compiling plan" />}
      </div>

      <div className={s.resultPanel}>
        {output === null ? (
          <p className={s.resultEmpty}>Run an explain to see the compiled plan.</p>
        ) : output.kind === "error" ? (
          <>
            <p className={s.errorHead}>
              {output.code}
              {output.status !== null ? ` · HTTP ${output.status}` : ""}
            </p>
            <p className={s.errorBody}>{output.message}</p>
          </>
        ) : (
          <ExplainResultView result={output.data} />
        )}
      </div>
    </div>
  );
}

function ExplainResultView({ result }: { result: ExplainResult }) {
  return (
    <div className={s.explainBody}>
      <div className={s.detailSection}>
        <span className={s.detailLabel}>terminal</span>
        <span className={s.monoCell}>{result.terminal}</span>
      </div>
      {result.warnings.length > 0 && (
        <div className={s.detailSection}>
          <span className={s.detailLabel}>warnings</span>
          <ul className={s.warnings}>
            {result.warnings.map((w, i) => (
              // biome-ignore lint/suspicious/noArrayIndexKey: warnings carry no identity
              <li key={i} className={s.warnText}>
                {w}
              </li>
            ))}
          </ul>
        </div>
      )}
      <div className={s.detailSection}>
        <span className={s.detailLabel}>SQL</span>
        <pre className={s.sql}>
          <code>{result.sql}</code>
        </pre>
      </div>
      <div className={s.detailSection}>
        <span className={s.detailLabel}>params</span>
        <pre className={s.params}>
          <code>{result.params.map((p, i) => `$${i + 1} = ${p}`).join("\n")}</code>
        </pre>
      </div>
    </div>
  );
}
