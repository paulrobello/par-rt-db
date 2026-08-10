import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// Same-origin in production (the server serves this SPA from RTDB_STATIC_DIR).
// In dev, Vite proxies the API/WS/auth routes to a backend so the OAuth session
// cookie and /sync + /admin/stream WebSockets behave as same-origin.
const backend = process.env.RTDB_BACKEND ?? "http://127.0.0.1:8300";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 8310,
    strictPort: true,
    proxy: {
      "/sync": { target: backend, ws: true, changeOrigin: true },
      // /admin/... is the control-plane API (+ the /admin/stream WebSocket).
      // Anchored so it does NOT swallow client routes like /admins (which the
      // same-origin SPA fallback serves in production; the dev server would
      // otherwise forward it to the backend and 404).
      "^/admin($|/)": { target: backend, ws: true, changeOrigin: true },
      "/api": backend,
      "/auth": { target: backend, changeOrigin: true },
      "/storage": backend,
      "/healthz": backend,
    },
  },
  build: {
    outDir: "dist",
    // SEC-137: never ship source maps — the dashboard SPA is served from a
    // private repo via ServeDir, and a public /assets/*.js.map would recover
    // the full TypeScript source of the operator console.
    sourcemap: false,
  },
});
