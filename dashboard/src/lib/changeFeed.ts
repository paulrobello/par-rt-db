import type { ChangeFeedResponse, ChangeOp, RtDbAdminClient } from "@par-rt-db/client";

export const HISTORY_SCAN_LIMIT = 5000;
const PAGE_SIZE = 500;

export interface DocumentHistory {
  entries: ChangeOp[];
  scanned: number;
  bounded: boolean;
}

export async function loadDocumentHistory(
  client: RtDbAdminClient,
  db: string,
  table: string,
  id: string,
  signal: AbortSignal,
): Promise<DocumentHistory> {
  const entries: ChangeOp[] = [];
  let since = 0;
  let scanned = 0;
  let logId: string | undefined;
  let snapshotHead: number | undefined;
  for (let pageNumber = 0; pageNumber < HISTORY_SCAN_LIMIT / PAGE_SIZE; pageNumber++) {
    if (signal.aborted) throw new DOMException("Aborted", "AbortError");
    const page: ChangeFeedResponse = await client.adminChanges(db, {
      since,
      table,
      limit: PAGE_SIZE,
    });
    if (logId !== undefined && logId !== page.logId) {
      throw new Error("The database was recreated during the scan. Load history again.");
    }
    logId = page.logId;
    snapshotHead ??= page.head;
    scanned += page.ops.length;
    const head = snapshotHead;
    entries.push(...page.ops.filter((op) => op.docId === id && op.seq <= head));
    if (page.nextSeq >= snapshotHead) return { entries, scanned, bounded: false };
    if (page.nextSeq <= since) throw new Error("Change feed cursor did not advance.");
    since = page.nextSeq;
  }
  return { entries, scanned, bounded: true };
}

const SYSTEM_FIELDS = new Set(["_id", "_creationTime", "_version", "_deletedAt"]);
export function documentBody(doc: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(doc).filter(([key]) => !SYSTEM_FIELDS.has(key)));
}

export interface FieldDifference {
  field: string;
  kind: "added" | "removed" | "changed";
  before: unknown;
  after: unknown;
}

function equal(a: unknown, b: unknown): boolean {
  if (Object.is(a, b)) return true;
  if (!a || !b || typeof a !== "object" || typeof b !== "object") return false;
  if (Array.isArray(a) !== Array.isArray(b)) return false;
  const left = Object.entries(a);
  const right = Object.entries(b);
  return (
    left.length === right.length &&
    left.every(
      ([key, value]) => Object.hasOwn(b, key) && equal(value, (b as Record<string, unknown>)[key]),
    )
  );
}

export function diffDocuments(
  before: Record<string, unknown> | null,
  after: Record<string, unknown> | null,
): FieldDifference[] {
  const a = before ? documentBody(before) : {};
  const b = after ? documentBody(after) : {};
  return [...new Set([...Object.keys(a), ...Object.keys(b)])].sort().flatMap((field) => {
    if (equal(a[field], b[field]) && Object.hasOwn(a, field) === Object.hasOwn(b, field)) return [];
    return [
      {
        field,
        kind: !Object.hasOwn(a, field)
          ? ("added" as const)
          : !Object.hasOwn(b, field)
            ? ("removed" as const)
            : ("changed" as const),
        before: a[field],
        after: b[field],
      },
    ];
  });
}
