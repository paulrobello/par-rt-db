/** Client-side export of already-fetched result rows to CSV or JSON — no server surface. */

export type ExportRow = Record<string, unknown>;

/** RFC4180: fields containing comma, quote, or newline are quoted; quotes double. */
function csvCell(v: unknown): string {
  if (v === null || v === undefined) return "";
  const s = typeof v === "object" ? JSON.stringify(v) : String(v);
  if (/[",\r\n]/.test(s)) return `"${s.replaceAll('"', '""')}"`;
  return s;
}

export function toCsvString(columns: readonly string[], rows: readonly ExportRow[]): string {
  const lines = [columns.map(csvCell).join(",")];
  for (const row of rows) {
    lines.push(columns.map((c) => csvCell(row[c])).join(","));
  }
  return `${lines.join("\r\n")}\r\n`;
}

export function toJsonString(rows: readonly ExportRow[]): string {
  return JSON.stringify(rows, null, 2);
}

/** Trigger a client-side download of `content` as `filename`. */
export function downloadTextFile(filename: string, content: string, mime: string): void {
  const blob = new Blob([content], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}
