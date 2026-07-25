import type { FieldTypeJson } from "@par-rt-db/client";

/** Renders a field type as a compact, mono-friendly string. */
export function formatFieldType(f: FieldTypeJson): string {
  switch (f.type) {
    case "string":
    case "number":
    case "boolean":
    case "null":
    case "int64":
    case "bytes":
    case "any":
      return f.type;
    case "id":
      return `id<${f.table}>`;
    case "literal":
      return `literal(${JSON.stringify(f.value)})`;
    case "optional":
      return `${formatFieldType(f.inner)}?`;
    case "union":
      return f.variants.map(formatFieldType).join(" | ");
    case "array":
      return `${formatFieldType(f.element)}[]`;
    case "object":
      return "object";
    case "record":
      return `record<${formatFieldType(f.value)}>`;
    case "vector":
      return `vector(${f.dimensions})`;
  }
}

export function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let v = n / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(v >= 100 ? 0 : 1)} ${units[i]}`;
}

export function formatNumber(n: number): string {
  return n.toLocaleString();
}
