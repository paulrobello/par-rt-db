import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";
import type { OpEvent } from "../lib/types";

// Live op feed: renders the newest-first op table from the admin provider's
// stream buffer (kind glyph, db·table links, truncated doc id), plus the empty
// state. Pure render — no client calls.

const opsHolder = vi.hoisted(() => ({ ops: [] as OpEvent[] }));

vi.mock("../lib/admin", () => ({
  useAdmin: () => ({ ops: opsHolder.ops }),
}));

import { OpsPage } from "./OpsPage";

function renderPage() {
  return render(
    <MemoryRouter>
      <OpsPage />
    </MemoryRouter>,
  );
}

describe("OpsPage", () => {
  it("shows the empty state before any ops stream in", () => {
    opsHolder.ops = [];
    renderPage();
    expect(screen.getByText(/No operations yet/)).toBeTruthy();
  });

  it("renders the op feed with kind glyphs, links, and truncated doc ids", () => {
    opsHolder.ops = [
      {
        kind: "insert",
        seq: 1,
        feedEpoch: "1",
        db: "kanban",
        table: "cards",
        docId: "card_01ABCDEF",
        owner: "alice@example.com",
        ts: 1_750_000_000_000,
      },
      {
        kind: "delete",
        seq: 2,
        feedEpoch: "1",
        db: "kanban",
        table: "cards",
        docId: "card_01ZZZZZZ",
        owner: null,
        ts: 1_750_000_001_000,
      },
    ];
    const { unmount } = renderPage();
    expect(screen.getByText("Operation feed")).toBeTruthy();
    // insert glyph "I", delete glyph "D" (danger styling is visual only)
    expect(screen.getByText("I")).toBeTruthy();
    expect(screen.getByText("D")).toBeTruthy();
    // db·table links route into the data browser
    const links = screen.getAllByRole("link");
    expect(links.some((l) => l.getAttribute("href") === "/dbs/kanban")).toBe(true);
    expect(links.some((l) => l.getAttribute("href") === "/dbs/kanban/tables/cards")).toBe(true);
    // doc id truncated to the last 12 chars with an ellipsis prefix
    expect(screen.getByTitle("card_01ABCDEF").textContent).toBe("…ard_01ABCDEF");
    // owners: email shown, null renders the em dash
    expect(screen.getByText("alice@example.com")).toBeTruthy();
    unmount();
  });
});
