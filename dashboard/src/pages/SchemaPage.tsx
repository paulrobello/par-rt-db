import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import type { SchemaJson } from "@par-rt-db/client";
import { Placard, Spinner } from "../components/ui";
import { useAdmin } from "../lib/admin";
import { formatFieldType } from "../lib/format";
import s from "./SchemaPage.module.css";

export function SchemaPage() {
  const { db = "" } = useParams();
  const { client } = useAdmin();
  const [schema, setSchema] = useState<SchemaJson | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
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
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [client, db]);

  const tables = schema ? Object.entries(schema.tables).sort(([a], [b]) => a.localeCompare(b)) : [];

  return (
    <section className={s.page}>
      <Placard>Schema · {db}</Placard>
      <h1 className={s.title}>Schema</h1>
      <Link to={`/dbs/${db}`} className={s.back}>
        ← {db}
      </Link>
      {loading ? (
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
              {table.indexes && table.indexes.length > 0 && (
                <div className={s.indexes}>
                  <span className={s.indexLabel}>indexes</span>
                  {table.indexes.map((idx) => (
                    <span key={idx.name} className={s.index}>
                      {idx.search ? (
                        <span className={s.indexTag}>FTS</span>
                      ) : idx.vector ? (
                        <span className={s.indexTag}>VEC</span>
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
      )}
    </section>
  );
}
