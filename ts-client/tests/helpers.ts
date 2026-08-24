/** Shared test polling helper. Mirrors `server/tests/common/mod.rs::wait_until`
 * (25ms poll) and `python-client/tests/test_presence.py::_wait_until` — use
 * this instead of a fixed `setTimeout`-then-assert wait for an asynchronous
 * condition so the test is neither flaky under load nor slower than it needs
 * to be. Not for advancing a deliberate interval/TTL — those stay fixed
 * sleeps or fake-timer advances.
 */
export async function waitFor(predicate: () => boolean, timeoutMs = 5000): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error("waitFor timed out");
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
}
