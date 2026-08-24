/**
 * Canonical string form for change detection.
 *
 * Object keys are sorted so two structurally equal values compare equal
 * regardless of insertion order. Shared by the in-memory engine's subscription
 * diffing and the optimistic-update no-op check; both must agree byte for byte
 * or a write that changed nothing would still push.
 */
export function canonical(value: unknown): string {
  return JSON.stringify(value, (_k, v) => {
    if (v && typeof v === "object" && !Array.isArray(v)) {
      const sorted: Record<string, unknown> = {};
      for (const key of Object.keys(v as Record<string, unknown>).sort()) {
        sorted[key] = (v as Record<string, unknown>)[key];
      }
      return sorted;
    }
    return v;
  });
}
