/** Browser-local query console history. Mutations are never saved or replayed. */
export type RecentQuery = { db: string; text: string };
export type SavedQuery = RecentQuery & { name: string };

const SAVED_KEY = "rtdb.queryConsole.saved";
const RECENT_KEY = "rtdb.queryConsole.recent";

function isQuery(value: unknown): value is RecentQuery {
  return (
    typeof value === "object" &&
    value !== null &&
    "db" in value &&
    typeof value.db === "string" &&
    "text" in value &&
    typeof value.text === "string"
  );
}

function readList(key: string): unknown[] {
  try {
    const parsed: unknown = JSON.parse(localStorage.getItem(key) ?? "[]");
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function writeList(key: string, list: readonly RecentQuery[]): void {
  localStorage.setItem(key, JSON.stringify(list));
}

export function loadSavedQueries(): SavedQuery[] {
  return readList(SAVED_KEY).filter(
    (q): q is SavedQuery => isQuery(q) && "name" in q && typeof q.name === "string",
  );
}

export function saveSavedQueries(list: readonly SavedQuery[]): void {
  writeList(SAVED_KEY, list);
}

export function loadRecentQueries(): RecentQuery[] {
  return readList(RECENT_KEY).filter(isQuery).slice(0, 20);
}

export function saveRecentQueries(list: readonly RecentQuery[]): void {
  writeList(RECENT_KEY, list);
}

export function pushRecentQuery(list: readonly RecentQuery[], entry: RecentQuery): RecentQuery[] {
  return [entry, ...list.filter((q) => q.text !== entry.text)].slice(0, 20);
}
