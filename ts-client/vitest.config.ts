import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globals: true,
    environment: "happy-dom",
    // Unit tests only by default (the `test` script adds --exclude tests/integration/**);
    // the integration suite is opt-in via `bun run test:integration`, which targets that
    // directory directly and must not have it excluded here.
    exclude: ["node_modules/**", "dist/**"],
  },
});
