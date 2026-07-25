import { defineConfig } from "vitest/config";

// jsdom because the dashboard renders real React components (Login, AppShell,
// pages) that need window/document APIs. Mirrors the ts-client vitest config
// shape but swaps happy-dom (sufficient for the SDK's react bindings) for
// jsdom (needed for full DOM event/layout behavior in the SPA).
export default defineConfig({
  esbuild: { jsx: "automatic" },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["./src/test-setup.ts"],
    exclude: ["node_modules/**", "dist/**"],
  },
});
