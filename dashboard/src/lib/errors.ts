/**
 * Coerce a caught value (the `e` in `catch (e)`) into a human-readable string.
 *
 * Every dashboard error surface used to inline `e instanceof Error ? e.message :
 * String(e)`; that idiom was duplicated ~49 times across the pages and drifted
 * in subtle ways. This is the single source of truth. It preserves the exact
 * behavior of the inline form: an `Error`'s `.message`, otherwise `String(e)`.
 */
export function toErrorMessage(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}
