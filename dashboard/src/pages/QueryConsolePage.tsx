/** Ad-hoc query and mutate console — compose JSON DSL and inspect the raw server response. */
import type { QueryJson, TransactionJson } from "@par-rt-db/client";
import { useEffect, useState } from "react";
import { Button, Placard, Spinner } from "../components/ui";
import { RtDbRequestError, useAdmin } from "../lib/admin";
import { toErrorMessage } from "../lib/errors";
import { downloadTextFile, toCsvString, toJsonString, type ExportRow } from "../lib/exportResults";
import {
  loadRecentQueries,
  loadSavedQueries,
  pushRecentQuery,
  saveRecentQueries,
  saveSavedQueries,
  type RecentQuery,
  type SavedQuery,
} from "../lib/queryConsoleStorage";
import s from "./QueryConsolePage.module.css";

type Mode = "query" | "mutate";

const DEFAULTS: Record<Mode, string> = {
  query: `{
  "table": "users",
  "take": 100
}`,
  mutate: `{
  "steps": [
    {
      "op": "insert",
      "table": "users",
      "doc": { "name": "Ada Lovelace", "email": "ada@example.com" }
    }
  ]
}`,
};

type Output =
  | { kind: "result"; data: unknown }
  | { kind: "error"; code: string; status: number | null; message: string }
  | null;

export function QueryConsolePage() {
  const { client, databases } = useAdmin();
  const [db, setDb] = useState<string>("");
  const [mode, setMode] = useState<Mode>("query");
  const [buffers, setBuffers] = useState<Record<Mode, string>>({ ...DEFAULTS });
  const [output, setOutput] = useState<Output>(null);
  const [loading, setLoading] = useState(false);
  const [saved, setSaved] = useState<SavedQuery[]>(loadSavedQueries);
  const [recent, setRecent] = useState<RecentQuery[]>(loadRecentQueries);
  const [saveName, setSaveName] = useState<string>("");
  const [storageError, setStorageError] = useState<string | null>(null);
  const rows =
    output?.kind === "result" &&
    Array.isArray(output.data) &&
    output.data.every(
      (row: unknown) => typeof row === "object" && row !== null && !Array.isArray(row),
    )
      ? (output.data as ExportRow[])
      : null;
  const columns = rows ? [...new Set(rows.flatMap((row) => Object.keys(row)))] : [];

  function saveQuery(): void {
    try {
      JSON.parse(buffers.query);
      const next = [
        ...saved.filter((q) => q.name !== saveName.trim()),
        { name: saveName.trim(), db, text: buffers.query },
      ];
      saveSavedQueries(next);
      setSaved(next);
      setSaveName("");
      setStorageError(null);
    } catch (e) {
      setStorageError(`Could not save query: ${toErrorMessage(e)}`);
    }
  }

  function replay(query: RecentQuery): void {
    setMode("query");
    setDb(query.db);
    setBuffers((prev) => ({ ...prev, query: query.text }));
    void run(query.text, query.db, "query");
  }

  // Auto-select the first database once the list arrives.
  useEffect(() => {
    if (!db && databases.length > 0) setDb(databases[0]);
  }, [db, databases]);

  const text = buffers[mode];

  function setText(next: string) {
    setBuffers((prev) => ({ ...prev, [mode]: next }));
  }

  async function run(
    queryText: string = text,
    database: string = db,
    runMode: Mode = mode,
  ): Promise<void> {
    if (!database) {
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
      parsed = JSON.parse(queryText);
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
      if (runMode === "query") {
        const next = pushRecentQuery(recent, { db: database, text: queryText });
        setRecent(next);
        try {
          saveRecentQueries(next);
          setStorageError(null);
        } catch (e) {
          setStorageError(`Could not persist history: ${toErrorMessage(e)}`);
        }
        const body = await client.adminQuery(database, parsed as QueryJson);
        setOutput({ kind: "result", data: body });
      } else {
        const body = await client.adminMutate(database, parsed as TransactionJson);
        setOutput({ kind: "result", data: body });
      }
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
    <section className={s.page}>
      <h1 className={s.title}>Query console</h1>
      <Placard>
        Compose an ad-hoc query or mutation DSL and run it against a database. Writes are durable.
      </Placard>

      <section className={s.block}>
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
          <div className={s.segment}>
            {(["query", "mutate"] as const).map((m) => (
              <button
                key={m}
                type="button"
                onClick={() => setMode(m)}
                className={`${s.segBtn} ${mode === m ? s.segBtnActive : ""}`}
                aria-pressed={mode === m}
              >
                {m}
              </button>
            ))}
          </div>
        </div>

        <textarea
          className={s.editor}
          value={text}
          onChange={(e) => setText(e.target.value)}
          spellCheck={false}
          rows={12}
          aria-label={`${mode} DSL`}
        />

        <div className={s.actions}>
          <Button variant="primary" onClick={() => void run()} disabled={loading || !db}>
            {loading ? "running…" : "run"}
          </Button>
          {loading && <Spinner label="running" />}
          {mode === "mutate" && !loading && <span className={s.warn}>mutate writes data</span>}
        </div>

        {mode === "query" && (
          <div className={s.actions}>
            <label className={s.field}>
              <span className={s.fieldLabel}>Query name</span>
              <input
                className={s.select}
                value={saveName}
                onChange={(e) => setSaveName(e.target.value)}
              />
            </label>
            <Button onClick={saveQuery} disabled={!saveName.trim() || !db}>
              Save query
            </Button>
          </div>
        )}
        {storageError && (
          <p className={s.errorBody} role="alert">
            {storageError}
          </p>
        )}
        <section aria-label="Saved queries">
          <Placard>Saved queries</Placard>
          {saved.length === 0 ? (
            <p className={s.hint}>No saved queries.</p>
          ) : (
            <ul className={s.queryList}>
              {saved.map((q) => (
                <li key={q.name}>
                  <Button onClick={() => replay(q)} disabled={loading || !databases.includes(q.db)}>
                    {q.name}
                  </Button>
                  <span className={s.hint}>{q.db}</span>
                </li>
              ))}
            </ul>
          )}
        </section>
        <section aria-label="Recent queries">
          <Placard>Recent queries · last 20 distinct</Placard>
          {recent.length === 0 ? (
            <p className={s.hint}>No recent queries.</p>
          ) : (
            <ol className={s.queryList}>
              {recent.map((q) => (
                <li key={q.text}>
                  <Button onClick={() => replay(q)} disabled={loading || !databases.includes(q.db)}>
                    {q.text}
                  </Button>
                  <span className={s.hint}>{q.db}</span>
                </li>
              ))}
            </ol>
          )}
        </section>

        <section>
          <div className={s.actions}>
            <Placard>Result</Placard>
            {rows && (
              <>
                <Button
                  onClick={() =>
                    downloadTextFile(
                      "query-results.csv",
                      toCsvString(columns, rows),
                      "text/csv;charset=utf-8",
                    )
                  }
                >
                  Export CSV
                </Button>
                <Button
                  onClick={() =>
                    downloadTextFile("query-results.json", toJsonString(rows), "application/json")
                  }
                >
                  Export JSON
                </Button>
              </>
            )}
          </div>
          <div className={s.resultPanel}>
            {output === null ? (
              <p className={s.resultEmpty}>Run a {mode} to see the result.</p>
            ) : output.kind === "error" ? (
              <>
                <p className={s.errorHead}>
                  {output.code}
                  {output.status !== null ? ` · HTTP ${output.status}` : ""}
                </p>
                <p className={s.errorBody}>{output.message}</p>
              </>
            ) : (
              <pre className={s.resultOk}>
                <code>{JSON.stringify(output.data, null, 2)}</code>
              </pre>
            )}
          </div>
        </section>
      </section>
    </section>
  );
}
