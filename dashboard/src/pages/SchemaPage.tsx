/** Schema viewer and push-schema editor with an additive-only diff preview before apply. */
import type { SchemaJson } from "@par-rt-db/client";
import { useCallback, useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { Button, Placard, Spinner } from "../components/ui";
import { RtDbRequestError, useAdmin } from "../lib/admin";
import { toErrorMessage } from "../lib/errors";
import { formatFieldType } from "../lib/format";
import type { SchemaDiff } from "../lib/types";
import s from "./SchemaPage.module.css";

type Mode = "view" | "push";

type Preview =
  | { kind: "diff"; diff: SchemaDiff }
  | { kind: "error"; code: string; status: number | null; message: string }
  | null;

const EXAMPLE_SCHEMA = `{
  "tables": {
    "items": {
      "fields": { "name": { "type": "string" } },
      "indexes": [{ "name": "by_name", "fields": ["name"] }]
    }
  }
}`;

export function SchemaPage() {
  const { db = "" } = useParams();
  const { client } = useAdmin();
  const [mode, setMode] = useState<Mode>("view");
  const [schema, setSchema] = useState<SchemaJson | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [text, setText] = useState(EXAMPLE_SCHEMA);
  const [preview, setPreview] = useState<Preview>(null);
  const [busy, setBusy] = useState(false);

  const refresh = useCallback(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    setSchema(null);
    client
      .getSchema(db)
      .then((sc) => {
        if (!cancelled) setSchema(sc);
      })
      .catch((e) => {
        if (!cancelled) setError(toErrorMessage(e));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [client, db]);

  useEffect(() => refresh(), [refresh]);

  const tables = schema ? Object.entries(schema.tables).sort(([a], [b]) => a.localeCompare(b)) : [];

  async function runPreview() {
    let parsed: SchemaJson;
    try {
      parsed = JSON.parse(text) as SchemaJson;
    } catch (e) {
      setPreview({
        kind: "error",
        code: "INVALID_JSON",
        status: null,
        message: toErrorMessage(e),
      });
      return;
    }
    setBusy(true);
    try {
      const diff = await client.previewSchema(db, parsed);
      setPreview({ kind: "diff", diff });
    } catch (e) {
      if (e instanceof RtDbRequestError) {
        setPreview({
          kind: "error",
          code: e.code,
          status: e.status ?? null,
          message: e.message,
        });
      } else {
        setPreview({
          kind: "error",
          code: "INTERNAL",
          status: null,
          message: toErrorMessage(e),
        });
      }
    } finally {
      setBusy(false);
    }
  }

  async function apply() {
    let parsed: SchemaJson;
    try {
      parsed = JSON.parse(text) as SchemaJson;
    } catch (e) {
      setPreview({
        kind: "error",
        code: "INVALID_JSON",
        status: null,
        message: toErrorMessage(e),
      });
      return;
    }
    setBusy(true);
    try {
      await client.pushSchema(db, parsed);
      // Applied — refresh the view and switch to it so the operator sees the result.
      setPreview(null);
      setMode("view");
      refresh();
    } catch (e) {
      if (e instanceof RtDbRequestError) {
        setPreview({
          kind: "error",
          code: e.code,
          status: e.status ?? null,
          message: e.message,
        });
      } else {
        setPreview({
          kind: "error",
          code: "INTERNAL",
          status: null,
          message: toErrorMessage(e),
        });
      }
    } finally {
      setBusy(false);
    }
  }

  return (
    <section className={s.page}>
      <Placard>Schema · {db}</Placard>
      <h1 className={s.title}>Schema</h1>
      <Link to={`/dbs/${db}`} className={s.back}>
        ← {db}
      </Link>
      <Link to={`/dbs/${db}/schema/history`} className={s.back}>
        history →
      </Link>

      <div className={s.segment}>
        {(["view", "push"] as const).map((m) => (
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

      {mode === "view" ? (
        loading ? (
          <Spinner label="loading schema" />
        ) : error ? (
          <p className={s.error}>{error}</p>
        ) : tables.length === 0 ? (
          <p className={s.empty}>Empty schema.</p>
        ) : (
          <div className={s.tables}>
            {tables.map(([name, table]) => (
              <div key={name} className={s.tableBlock}>
                <div className={s.tableHead}>
                  <h2 className={s.tableName}>{name}</h2>
                  {table.softDelete && <span className={s.owner}>soft delete</span>}
                  {table.ownerField && <span className={s.owner}>owner: {table.ownerField}</span>}
                </div>
                <table className={s.fields}>
                  <tbody>
                    {Object.entries(table.fields).map(([fname, ftype]) => (
                      <tr key={fname}>
                        <td className={s.fieldName}>{fname}</td>
                        <td className={s.fieldType}>{formatFieldType(ftype)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {table.defaults && Object.keys(table.defaults).length > 0 && (
                  <div className={s.indexes}>
                    <span className={s.indexLabel}>defaults</span>
                    {Object.entries(table.defaults).map(([fname, value]) => (
                      <span key={fname} className={s.index}>
                        <span className={s.indexName}>{fname}</span>
                        <span className={s.indexFields}>= {JSON.stringify(value)}</span>
                      </span>
                    ))}
                  </div>
                )}
                {table.indexes && table.indexes.length > 0 && (
                  <div className={s.indexes}>
                    <span className={s.indexLabel}>indexes</span>
                    {table.indexes.map((idx) => (
                      <span key={idx.name} className={s.index}>
                        {idx.search ? (
                          <span className={s.indexTag}>
                            FTS{idx.language ? `·${idx.language}` : ""}
                          </span>
                        ) : idx.vector ? (
                          <span className={s.indexTag}>VEC·{idx.vector.metric ?? "cosine"}</span>
                        ) : null}
                        <span className={s.indexName}>{idx.name}</span>
                        <span className={s.indexFields}>({idx.fields.join(", ")})</span>
                      </span>
                    ))}
                  </div>
                )}
              </div>
            ))}
          </div>
        )
      ) : (
        <section className={s.pushBlock}>
          <Placard>
            Paste a schema and preview the additive-only diff. Drops and type changes are rejected
            by the server.
          </Placard>
          <textarea
            className={s.editor}
            value={text}
            onChange={(e) => setText(e.target.value)}
            spellCheck={false}
            rows={14}
            aria-label="schema JSON"
          />
          <div className={s.actions}>
            <Button variant="primary" onClick={runPreview} disabled={busy || !db}>
              {busy ? "working…" : "preview"}
            </Button>
            <Button variant="secondary" onClick={apply} disabled={busy || !db}>
              apply
            </Button>
            {busy && <Spinner label="working" />}
            <span className={s.hint}>apply writes the schema (additive only)</span>
          </div>

          {preview !== null && (
            <section>
              <Placard>Preview</Placard>
              <div className={s.resultPanel}>
                {preview.kind === "error" ? (
                  <>
                    <p className={s.errorHead}>
                      {preview.code}
                      {preview.status !== null ? ` · HTTP ${preview.status}` : ""}
                    </p>
                    <p className={s.errorBody}>{preview.message}</p>
                  </>
                ) : preview.diff.added.length === 0 && preview.diff.rejected.length === 0 ? (
                  <p className={s.resultEmpty}>
                    No changes — the pending schema matches the applied one.
                  </p>
                ) : (
                  <>
                    {preview.diff.added.length > 0 && (
                      <div className={s.diffSection}>
                        <h3 className={`${s.diffHead} ${s.addedHead}`}>
                          will add ({preview.diff.added.length})
                        </h3>
                        {preview.diff.added.map((t) => (
                          <div key={t.table} className={s.diffTable}>
                            <span className={s.diffTableName}>{t.table}</span>
                            {t.columns.map((c) => (
                              <div key={c.name} className={s.diffRow}>
                                <span className={s.diffAddedMark}>+</span>
                                <span className={s.diffName}>{c.name}</span>
                                <span className={s.diffType}>{c.fieldType}</span>
                              </div>
                            ))}
                            {t.indexes.map((idx) => (
                              <div key={idx.name} className={s.diffRow}>
                                <span className={s.diffAddedMark}>+</span>
                                <span className={s.diffName}>#{idx.name}</span>
                                <span className={s.diffType}>({idx.fields.join(", ")})</span>
                              </div>
                            ))}
                          </div>
                        ))}
                      </div>
                    )}
                    {preview.diff.rejected.length > 0 && (
                      <div className={s.diffSection}>
                        <h3 className={`${s.diffHead} ${s.rejectedHead}`}>
                          will reject ({preview.diff.rejected.length})
                        </h3>
                        {preview.diff.rejected.map((r) => (
                          <div key={`${r.table}.${r.item}.${r.reason}`} className={s.diffTable}>
                            <span className={s.diffTableName}>{r.table}</span>
                            <div className={s.diffRow}>
                              <span className={s.diffRejectedMark}>✕</span>
                              <span className={s.diffName}>{r.item}</span>
                              <span className={s.diffType}>{r.reason}</span>
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
                  </>
                )}
              </div>
            </section>
          )}
        </section>
      )}
    </section>
  );
}
