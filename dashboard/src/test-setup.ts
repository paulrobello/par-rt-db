import "@testing-library/jest-dom/vitest";
import { afterEach, vi } from "vitest";

// jsdom lacks window.matchMedia / WebSocket; nothing in the tested units uses
// them, but keep a global guard so importing the lib modules never crashes.
afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  localStorage.clear();
});
