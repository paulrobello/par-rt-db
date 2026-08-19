/**
 * ENH-023: behavioral-semantics corpus runner (ts-client in-memory view).
 *
 * Enumerates every `*.json` case in `wire-corpus/semantics/` (repo root — the
 * single source of truth; one self-contained case per file carrying its own
 * schema, seed, operation, and expected result) and executes each against a
 * fresh in-memory engine instance, comparing normalized results. The same
 * fixture files are consumed by the server (against Postgres), the rust-client,
 * and the python-client; the server is the source of truth for every expected
 * value, so a divergence here is a ts-engine bug (or a stale fixture).
 *
 * The runner implements `wire-corpus/README.md`'s "How a runner executes a
 * case" algorithm exactly, mirroring the server's reference runner
 * (`server/tests/semantics_corpus_test.rs`): runtime directory enumeration (the
 * directory IS the case count — no hardcoded constant), per-case fresh client,
 * seed inserts through `mutate` with `$id` label capture, `{"$idRef": ...}`
 * substitution throughout `op`/`then.query`, the `"$prev"` paginate-cursor
 * sentinel, error cases asserting the error `code` only, `normalize`
 * projection applied recursively to both trees, `unordered` multiset
 * comparison via canonical-JSON sort, numeric-tolerant equality, and structural
 * `expect_next_cursor` presence. The injected clock makes id minting and
 * `_creationTime` deterministic; no time is advanced and no scheduler/TTL
 * reaper runs between seeding and the op — the corpus pins synchronous
 * semantics only.
 */

import { readdirSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

import { RtDbError } from "../src/errors.js";
import { InMemoryRtDbClient } from "../src/in_memory/index.js";
import type { Paginate, QueryJson, SchemaJson, TransactionJson } from "../src/protocol.js";

/** System fields minted at run time and projected out of both sides unless a
 * case's `normalize` list replaces the default (README "Semantics corpus
 * format"). */
const DEFAULT_NORMALIZE = ["_id", "_creationTime", "_version"];

const CORPUS_DIR = resolve(__dirname, "../../wire-corpus/semantics");

interface ThenBlock {
  query: QueryJson;
  expect: unknown;
  unordered?: boolean;
  normalize?: string[];
}

interface SemanticsCase {
  name: string;
  $comment?: string;
  schema: SchemaJson;
  seed: unknown[];
  op: { query?: QueryJson; txn?: TransactionJson };
  expect: unknown;
  unordered?: boolean;
  normalize?: string[];
  expect_next_cursor?: boolean;
  then?: ThenBlock;
  skip?: Partial<Record<"ts" | "rust" | "python" | "server" | "swift", string>>;
}

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

/** The expected error `code` when `expect` is an error object, else undefined
 * (server: `expect.pointer("/error/code").is_some()`). */
function errorCodeOf(expect: unknown): string | undefined {
  if (isPlainObject(expect) && isPlainObject(expect.error)) {
    const code = expect.error.code;
    if (typeof code === "string") return code;
  }
  return undefined;
}

/** Resolve one `seed` entry into `{table, doc, label}`. A wrapped entry is an
 * object with a `doc` key whose value is an object (with optional `table` and
 * `$id` siblings); any other object is a plain doc, legal only when the schema
 * declares exactly one table (the disambiguation rule the corpus README
 * states). */
function parseSeedEntry(
  entry: unknown,
  singleTable: string | undefined,
  caseName: string,
): { table: string; doc: Record<string, unknown>; label?: string } {
  if (!isPlainObject(entry)) {
    throw new Error(`${caseName}: seed entry must be a JSON object`);
  }
  if (isPlainObject(entry.doc)) {
    let table: string;
    if (typeof entry.table === "string") {
      table = entry.table;
    } else if (singleTable !== undefined) {
      table = singleTable;
    } else {
      throw new Error(
        `${caseName}: wrapped seed entry without \`table\` requires a single-table schema`,
      );
    }
    if (typeof entry.$id === "string") {
      return { table, doc: entry.doc, label: entry.$id };
    }
    return { table, doc: entry.doc };
  }
  if (singleTable === undefined) {
    throw new Error(`${caseName}: plain-doc seed requires a single-table schema`);
  }
  return { table: singleTable, doc: entry };
}

/** Replace every `{"$idRef": "<label>"}` object anywhere in the tree with the
 * minted id recorded for that seed label (README "Substitution placeholders"). */
function substitute(node: unknown, ids: Map<string, string>, caseName: string): unknown {
  if (Array.isArray(node)) {
    return node.map((v) => substitute(v, ids, caseName));
  }
  if (isPlainObject(node)) {
    const keys = Object.keys(node);
    if (keys.length === 1 && keys[0] === "$idRef") {
      const label = node.$idRef;
      if (typeof label !== "string") {
        throw new Error(`${caseName}: $idRef label must be a string`);
      }
      const id = ids.get(label);
      if (id === undefined) {
        throw new Error(`${caseName}: $idRef references unknown seed label '${label}'`);
      }
      return id;
    }
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(node)) {
      out[k] = substitute(v, ids, caseName);
    }
    return out;
  }
  return node;
}

/** Remove every `keys` member from every object in the tree, recursively — the
 * README's `normalize` projection applies to every object in both the actual
 * and expected trees (docs inside `paginate.docs`, step results, ...). */
function projectRecursive(node: unknown, keys: string[]): unknown {
  if (Array.isArray(node)) {
    return node.map((v) => projectRecursive(v, keys));
  }
  if (isPlainObject(node)) {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(node)) {
      if (!keys.includes(k)) {
        out[k] = projectRecursive(v, keys);
      }
    }
    return out;
  }
  return node;
}

/** Canonical JSON for the unordered multiset sort: compact serialization with
 * object keys sorted recursively (README determinism ruling 2). */
function canonical(v: unknown): string {
  return JSON.stringify(v, (_k, val) => {
    if (isPlainObject(val)) {
      return Object.fromEntries(
        Object.keys(val)
          .sort()
          .map((k) => [k, val[k]]),
      );
    }
    return val;
  });
}

/** Numeric-tolerant equality so the SQL-numeric server result and the JS number
 * result agree (e.g. `6` == `6.0`) — the same tolerance golden-vector applies.
 * Recurses into arrays/objects. */
function jsonEqNumeric(a: unknown, b: unknown): boolean {
  if (typeof a === "number" && typeof b === "number") {
    return a === b || Math.abs(a - b) < 1e-9;
  }
  if (a === null && b === null) return true;
  if (Array.isArray(a) && Array.isArray(b)) {
    return a.length === b.length && a.every((x, i) => jsonEqNumeric(x, b[i]));
  }
  if (isPlainObject(a) && isPlainObject(b)) {
    const ak = Object.keys(a);
    const bk = Object.keys(b);
    return ak.length === bk.length && ak.every((k) => k in b && jsonEqNumeric(a[k], b[k]));
  }
  return a === b;
}

/** Assert actual == expected under `normalize` projection already applied:
 * `unordered` compares the two arrays as multisets (each side sorted by
 * canonical JSON, then element-wise numeric-tolerant), otherwise the values
 * compare in place, recursively numeric-tolerant. Mirrors the server runner's
 * `assert_expected`. */
function assertExpected(got: unknown, want: unknown, unordered: boolean, msg: string): void {
  if (jsonEqNumeric(got, want)) {
    return; // equal as sequences — also covers every unordered case
  }
  if (!unordered) {
    throw new Error(`${msg}\n got ${JSON.stringify(got)}\nwant ${JSON.stringify(want)}`);
  }
  if (!Array.isArray(got) || !Array.isArray(want)) {
    throw new Error(
      `${msg}: unordered comparison requires arrays — got ${JSON.stringify(got)}, want ${JSON.stringify(want)}`,
    );
  }
  if (got.length !== want.length) {
    throw new Error(
      `${msg}: row count mismatch (unordered) — got ${got.length}, want ${want.length}`,
    );
  }
  const byCanonical = (a: unknown, b: unknown) => {
    const ca = canonical(a);
    const cb = canonical(b);
    return ca < cb ? -1 : ca > cb ? 1 : 0;
  };
  const gs = [...got].sort(byCanonical);
  const ws = [...want].sort(byCanonical);
  for (let i = 0; i < gs.length; i++) {
    if (!jsonEqNumeric(gs[i], ws[i])) {
      throw new Error(
        `${msg}: row ${i} mismatch (unordered compare)\n got ${JSON.stringify(got)}\nwant ${JSON.stringify(want)}`,
      );
    }
  }
  // Lengths equal and every sorted row matched: the multisets agree, so the
  // values differ only in order — exactly what `unordered` forgives.
}

/** The effective `normalize` key list for an expect block: a present list
 * REPLACES the default; absent falls back to `fallback` (the case-level list,
 * itself defaulted — `then` inherits the case's list unless it overrides). */
function normalizeKeys(block: { normalize?: string[] }, fallback: string[]): string[] {
  return block.normalize ?? fallback;
}

/** Error-case assertion: only the code is compared, never the message. */
function assertErrorCode(err: unknown, wantCode: string, caseName: string): void {
  if (!(err instanceof RtDbError)) {
    throw new Error(
      `${caseName}: expected RtDbError ${wantCode}, got non-RtDbError throw: ${String(err)}`,
    );
  }
  expect(err.code).toBe(wantCode);
}

/** Compare an op/then success result against its `expect` block: apply the
 * `normalize` projection to both trees, structurally assert `nextCursor`
 * presence when the block pins it (paginate), then ordered/unordered compare. */
function assertResult(
  caseName: string,
  actual: unknown,
  block: { expect: unknown; expect_next_cursor?: boolean },
  keys: string[],
  unordered: boolean,
): void {
  let got = actual;
  let want = block.expect;
  if (typeof block.expect_next_cursor === "boolean") {
    const has = isPlainObject(got) && "nextCursor" in got;
    expect(has).toBe(block.expect_next_cursor);
    const projected = [...keys, "nextCursor"];
    got = projectRecursive(got, projected);
    want = projectRecursive(want, projected);
  } else {
    got = projectRecursive(got, keys);
    want = projectRecursive(want, keys);
  }
  assertExpected(got, want, unordered, `${caseName}: result mismatch`);
}

/** Execute one corpus case end to end against a fresh in-memory instance.
 * Every failure names the case. */
async function runCase(caseName: string, caseData: SemanticsCase): Promise<void> {
  // Fresh instance per case; deterministic clock and RNG so id minting and
  // `_creationTime` are stable. No time is advanced between seed and op.
  let ms = 1_700_000_000_000;
  const client = new InMemoryRtDbClient({ now: () => ms++, random: () => 0 });
  client.pushSchema(caseData.schema);

  const tableNames = Object.keys(caseData.schema.tables);
  const singleTable = tableNames.length === 1 ? tableNames[0] : undefined;

  // Seed in array order through the normal insert path (mutate), recording
  // `label -> minted id` for `$id`-labeled entries.
  const ids = new Map<string, string>();
  for (const [i, entry] of caseData.seed.entries()) {
    const { table, doc, label } = parseSeedEntry(entry, singleTable, caseName);
    const results = await client.mutate({ steps: [{ op: "insert", table, doc }] });
    if (label !== undefined) {
      const first = results[0];
      if (first === null || typeof first !== "object" || !("id" in first)) {
        throw new Error(`${caseName}: seed #${i}: insert result missing id`);
      }
      ids.set(label, first.id as string);
    }
  }

  const expectErr = errorCodeOf(caseData.expect);
  const caseKeys = normalizeKeys(caseData, DEFAULT_NORMALIZE);

  // Execute the op. A query op first resolves the `"$prev"` paginate-cursor
  // sentinel (README step 4): run the cursor-less query, take its nextCursor,
  // then run the real query with it. `expect` describes the SECOND page.
  let opResult: unknown;
  if (caseData.op.txn !== undefined) {
    const txn = substitute(caseData.op.txn, ids, caseName) as TransactionJson;
    try {
      opResult = await client.mutate(txn);
    } catch (err) {
      if (expectErr === undefined) {
        throw new Error(
          `${caseName}: unexpected txn error: ${err instanceof Error ? err.message : String(err)}`,
        );
      }
      assertErrorCode(err, expectErr, caseName);
      return; // a failed op has no `then` follow-up
    }
  } else if (caseData.op.query !== undefined) {
    let q = substitute(caseData.op.query, ids, caseName) as QueryJson;
    if (q.paginate?.cursor === "$prev") {
      const firstPaginate: Paginate = { ...q.paginate };
      delete firstPaginate.cursor;
      const firstJson: QueryJson = { ...q, paginate: firstPaginate };
      const firstPage = (await client.query({ json: firstJson })) as {
        docs: unknown[];
        nextCursor?: string;
      };
      if (firstPage.nextCursor === undefined) {
        throw new Error(`${caseName}: $prev: first page has no nextCursor`);
      }
      q = { ...q, paginate: { ...q.paginate, cursor: firstPage.nextCursor } };
    }
    try {
      opResult = await client.query({ json: q });
    } catch (err) {
      if (expectErr === undefined) {
        throw new Error(
          `${caseName}: unexpected query error: ${err instanceof Error ? err.message : String(err)}`,
        );
      }
      assertErrorCode(err, expectErr, caseName);
      return; // a failed op has no `then` follow-up
    }
  } else {
    throw new Error(`${caseName}: op must carry \`query\` or \`txn\``);
  }

  if (expectErr !== undefined) {
    throw new Error(
      `${caseName}: expected error ${expectErr}, got success ${JSON.stringify(opResult)}`,
    );
  }
  assertResult(caseName, opResult, caseData, caseKeys, caseData.unordered ?? false);

  // Follow-up read after a successful op (write-then-read visibility cases).
  if (caseData.then !== undefined) {
    const q = substitute(caseData.then.query, ids, caseName) as QueryJson;
    const actual = await client.query({ json: q });
    const keys = normalizeKeys(caseData.then, caseKeys);
    assertResult(caseName, actual, caseData.then, keys, caseData.then.unordered ?? false);
  }
}

describe("ENH-023 behavioral-semantics corpus (ts-client in-memory engine)", () => {
  // Enumerate the corpus at RUNTIME — the directory IS the count ("bumped only
  // by adding files", never by editing a constant here).
  const files = readdirSync(CORPUS_DIR)
    .filter((f) => f.endsWith(".json"))
    .sort();
  const loaded: string[] = [];
  const cases: Array<{ stem: string; case: SemanticsCase }> = [];
  for (const file of files) {
    const stem = file.slice(0, -".json".length);
    const caseData = JSON.parse(readFileSync(resolve(CORPUS_DIR, file), "utf8")) as SemanticsCase;
    if (caseData.name !== stem) {
      throw new Error(
        `semantics/${file}: case \`name\` ('${caseData.name}') must equal the filename stem ('${stem}')`,
      );
    }
    loaded.push(file);
    cases.push({ stem, case: caseData });
  }

  let executed = 0;
  let skipped = 0;

  it("enumerates a non-empty corpus", () => {
    expect(files.length).toBeGreaterThan(0);
    expect(loaded.length).toBe(files.length);
  });

  for (const { stem, case: caseData } of cases) {
    // A named runner may skip loudly — the reason rides the test name so the
    // reporter prints it. Skipped cases still count in the accounting below.
    const skipReason = caseData.skip?.ts;
    if (skipReason !== undefined) {
      skipped += 1;
      it.skip(`[skip:ts] ${stem} — ${skipReason}`, () => {});
      continue;
    }
    it(stem, async () => {
      executed += 1;
      await runCase(stem, caseData);
    });
  }

  it("accounts for every corpus file (executed + skipped == files)", () => {
    expect(executed + skipped).toBe(files.length);
  });
});
