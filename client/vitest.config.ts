import { defineConfig } from "vitest/config";

export default defineConfig({
  esbuild: { jsx: "automatic" },
  test: {
    globals: true,
    environment: "happy-dom",
    // Unit tests only by default; the integration suite is opt-in via `bun run test:integration`.
    exclude: ["tests/integration/**", "node_modules/**", "dist/**"],
  },
});
