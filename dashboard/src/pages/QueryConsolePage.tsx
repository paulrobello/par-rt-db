/** Ad-hoc query and mutate console — compose JSON DSL and inspect the raw server response. */
import type { QueryJson, TransactionJson } from "@par-rt-db/client";
import { useEffect, useState } from "react";
import { Button, Placard, Spinner } from "../components/ui";
import { RtDbRequestError, useAdmin } from "../lib/admin";
import { toErrorMessage } from "../lib/errors";
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

  // Auto-select the first database once the list arrives.
  useEffect(() => {
    if (!db && databases.length > 0) setDb(databases[0]);
  }, [db, databases]);

  const text = buffers[mode];

  function setText(next: string) {
    setBuffers((prev) => ({ ...prev, [mode]: next }));
  }

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
      if (mode === "query") {
        const body = await client.adminQuery(db, parsed as QueryJson);
        setOutput({ kind: "result", data: body });
      } else {
        const body = await client.adminMutate(db, parsed as TransactionJson);
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
          <Button variant="primary" onClick={run} disabled={loading || !db}>
            {loading ? "running…" : "run"}
          </Button>
          {loading && <Spinner label="running" />}
          {mode === "mutate" && !loading && <span className={s.warn}>mutate writes data</span>}
        </div>

        <section>
          <Placard>Result</Placard>
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
