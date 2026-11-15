import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globals: true,
    environment: "happy-dom",
    // Unit tests only by default (the `test` script adds --exclude tests/integration/**);
    // the integration suite is opt-in via `bun run test:integration`, which targets that
    // directory directly and must not have it excluded here.
    exclude: ["node_modules/**", "dist/**"],
    coverage: {
      thresholds: {
        // ENH-054: in-memory engine floor (docs/coverage-floors.md) — ratchet
        // only, and only `lines` (branch/function floors are not measured here).
        "src/in_memory/**": { lines: 90 },
      },
    },
  },
});
