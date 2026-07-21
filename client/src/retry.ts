import { RtDbError } from "./errors.js";

/**
 * Runs a read-compute-write flow, retrying only on `PRECONDITION_FAILED` (an
 * `expectVersion` conflict from a concurrent write). Bounded — never an infinite
 * loop, and never retries any other failure. This is the SDK's ONLY auto-retry.
 */
export async function retryOnPrecondition<T>(
  fn: () => Promise<T>,
  opts: { retries?: number } = {},
): Promise<T> {
  const retries = opts.retries ?? 4;
  let attempt = 0;
  for (;;) {
    try {
      return await fn();
    } catch (error) {
      const isConflict = error instanceof RtDbError && error.code === "PRECONDITION_FAILED";
      if (!isConflict || attempt >= retries) {
        throw error;
      }
      attempt += 1;
    }
  }
}
