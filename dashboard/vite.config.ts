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
      // /admin covers HTTP admin routes AND the /admin/stream WebSocket.
      "/admin": { target: backend, ws: true, changeOrigin: true },
      "/api": backend,
      "/auth": { target: backend, changeOrigin: true },
      "/storage": backend,
      "/healthz": backend,
    },
  },
  build: {
    outDir: "dist",
    sourcemap: true,
  },
});
