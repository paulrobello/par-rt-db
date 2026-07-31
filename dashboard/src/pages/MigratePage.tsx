import { useState } from "react";
import { Link, useParams } from "react-router-dom";
import type {
  DirectiveJson,
  DirectiveReportJson,
  MigrateRequestJson,
  MigrateResultJson,
} from "@par-rt-db/client";
import { Button, Placard, Spinner } from "../components/ui";
import { RtDbRequestError, useAdmin } from "../lib/admin";
import s from "./MigratePage.module.css";

type Result =
  | { kind: "dryRun"; report: MigrateResultJson; reviewedText: string }
  | { kind: "applied"; report: MigrateResultJson }
  | { kind: "error"; code: string; status: number | null; message: string }
  | null;

const EXAMPLE_DIRECTIVES = `[
  { "op": "renameField", "table": "items", "from": "name", "to": "title" }
]`;

function parseDirectives(
  text: string,
): { ok: true; directives: DirectiveJson[] } | { ok: false; message: string } {
  try {
    const parsed = JSON.parse(text);
    if (!Array.isArray(parsed)) {
      return { ok: false, message: "directives must be a JSON array" };
    }
    return { ok: true, directives: parsed as DirectiveJson[] };
  } catch (e) {
    return { ok: false, message: e instanceof Error ? e.message : String(e) };
  }
}

export function MigratePage() {
  const { db = "" } = useParams();
  const { client } = useAdmin();
  const [text, setText] = useState(EXAMPLE_DIRECTIVES);
  const [result, setResult] = useState<Result>(null);
  const [busy, setBusy] = useState(false);

  function onTextChange(next: string) {
    setText(next);
    // A pending dry-run no longer reflects the editor — force a re-preview
    // before apply is allowed again.
    if (result?.kind === "dryRun") setResult(null);
  }

  function toError(e: unknown): Result {
    if (e instanceof RtDbRequestError) {
      return { kind: "error", code: e.code, status: e.status, message: e.message };
    }
    return {
      kind: "error",
      code: "INTERNAL",
      status: null,
      message: e instanceof Error ? e.message : String(e),
    };
  }

  async function runDryRun() {
    const parsed = parseDirectives(text);
    if (!parsed.ok) {
      setResult({ kind: "error", code: "INVALID_JSON", status: null, message: parsed.message });
      return;
    }
    const req: MigrateRequestJson = { directives: parsed.directives, dryRun: true };
    setBusy(true);
    try {
      const report = await client.migrate(db, req);
      // Snapshot the editor text at the moment of the request so apply can be
      // gated on an exact match — defends against drift during and after the
      // dry-run (a keystroke that lands while this await is pending, or after).
      setResult({ kind: "dryRun", report, reviewedText: text });
    } catch (e) {
      setResult(toError(e));
    } finally {
      setBusy(false);
    }
  }

  async function apply() {
    const parsed = parseDirectives(text);
    if (!parsed.ok) {
      setResult({ kind: "error", code: "INVALID_JSON", status: null, message: parsed.message });
      return;
    }
    const req: MigrateRequestJson = { directives: parsed.directives, dryRun: false };
    setBusy(true);
    try {
      const report = await client.migrate(db, req);
      setResult({ kind: "applied", report });
    } catch (e) {
      setResult(toError(e));
    } finally {
      setBusy(false);
    }
  }

  // Apply is gated on a reviewed dry-run whose previewed text exactly matches
  // the current editor contents. Any drift — during or after the dry-run —
  // forces a re-preview before apply can fire.
  const reviewed = result?.kind === "dryRun" && result.reviewedText === text;

  return (
    <section className={s.page}>
      <Placard>Migrate · {db}</Placard>
      <h1 className={s.title}>Migrate</h1>
      <Link to={`/dbs/${db}`} className={s.back}>
        ← {db}
      </Link>

      <Placard>
        Paste a directives array. Dry-run reports affected rows, casts, and sample changes per
        directive; apply commits the migration.
      </Placard>
      <textarea
        className={s.editor}
        value={text}
        onChange={(e) => onTextChange(e.target.value)}
        spellCheck={false}
        rows={14}
        aria-label="directives JSON"
      />
      <div className={s.actions}>
        <Button variant="primary" onClick={runDryRun} disabled={busy || !db}>
          {busy ? "working…" : "dry-run"}
        </Button>
        <Button variant="secondary" onClick={apply} disabled={busy || !db || !reviewed}>
          apply
        </Button>
        {busy && <Spinner label="working" />}
        <span className={s.hint}>review the dry-run before applying</span>
      </div>

      {result !== null && (
        <section>
          <Placard>{result.kind === "applied" ? "Applied" : "Report"}</Placard>
          <div className={s.resultPanel}>
            {result.kind === "error" ? (
              <>
                <p className={s.errorHead}>
                  {result.code}
                  {result.status !== null ? ` · HTTP ${result.status}` : ""}
                </p>
                <p className={s.errorBody}>{result.message}</p>
              </>
            ) : result.report.directives.length === 0 ? (
              <p className={s.resultEmpty}>No directives — nothing to report.</p>
            ) : (
              <>
                {result.kind === "applied" && (
                  <p className={s.applied}>
                    Applied — {result.report.directives.length} directive(s) committed.
                  </p>
                )}
                {result.report.directives.map((d, i) => (
                  // DirectiveReportJson carries no unique id — one report per
                  // directive, in positional order — so the index is the stable
                  // key for this immutable per-result list.
                  // biome-ignore lint/suspicious/noArrayIndexKey: see above
                  <DirectiveReportView key={`${d.op}-${i}`} d={d} />
                ))}
              </>
            )}
          </div>
        </section>
      )}
    </section>
  );
}

function DirectiveReportView({ d }: { d: DirectiveReportJson }) {
  return (
    <div className={s.directive}>
      <h3 className={s.directiveHead}>
        <span className={s.op}>{d.op}</span>
        <span className={s.affected}>{d.affectedRows} row(s)</span>
      </h3>
      {d.castFailures && d.castFailures.length > 0 && (
        <div className={s.subSection}>
          <span className={s.subHead}>cast failures ({d.castFailures.length})</span>
          {d.castFailures.slice(0, 10).map((f) => (
            <div key={f.id} className={s.subRow}>
              <span className={s.cellId}>{f.id}</span>
              <span className={s.cellVal}>{JSON.stringify(f.value)}</span>
            </div>
          ))}
        </div>
      )}
      {d.sampleChanges && d.sampleChanges.length > 0 && (
        <div className={s.subSection}>
          <span className={s.subHead}>sample changes ({d.sampleChanges.length})</span>
          {d.sampleChanges.slice(0, 5).map((c) => (
            <div key={c.id} className={s.subRow}>
              <span className={s.cellId}>{c.id}</span>
              <span className={s.cellBefore}>{JSON.stringify(c.before)}</span>
              <span className={s.arrow}>→</span>
              <span className={s.cellAfter}>{JSON.stringify(c.after)}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
