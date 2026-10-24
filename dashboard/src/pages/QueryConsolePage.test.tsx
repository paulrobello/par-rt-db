import { cleanup, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

// QueryConsolePage owns JSON parsing, mode dispatch, and error surfacing for
// the ad-hoc admin query/mutate console. The contract:
//   - valid JSON  -> client.adminQuery / adminMutate called with parsed body
//   - invalid JSON -> local error, client never called
//   - server error (RtDbRequestError) -> code + status + message shown

const adminClientMock = vi.hoisted(() => ({
  adminQuery: vi.fn(),
  adminMutate: vi.fn(),
}));

vi.mock("../lib/admin", async (importActual) => {
  const actual = await importActual<typeof import("../lib/admin")>();
  return {
    ...actual,
    useAdmin: () => ({ client: adminClientMock, databases: ["test-db"] }),
  };
});

import { RtDbRequestError } from "../lib/admin";
import { QueryConsolePage } from "./QueryConsolePage";

describe("QueryConsolePage", () => {
  beforeEach(() => {
    adminClientMock.adminQuery.mockReset();
    adminClientMock.adminMutate.mockReset();
  });

  it("runs the default query and shows the pretty-printed result", async () => {
    const user = userEvent.setup();
    adminClientMock.adminQuery.mockResolvedValue([{ _id: "k1", name: "Ada" }]);
    render(<QueryConsolePage />);

    await user.click(screen.getByRole("button", { name: "run" }));

    expect(adminClientMock.adminQuery).toHaveBeenCalledWith("test-db", {
      table: "users",
      take: 100,
    });
    expect(adminClientMock.adminMutate).not.toHaveBeenCalled();
    expect(await screen.findByText(/"_id": "k1"/)).toBeInTheDocument();
    expect(screen.getByText(/"name": "Ada"/)).toBeInTheDocument();
  });

  it("switches to mutate mode and runs the default transaction", async () => {
    const user = userEvent.setup();
    adminClientMock.adminMutate.mockResolvedValue(["new-id"]);
    render(<QueryConsolePage />);

    await user.click(screen.getByRole("button", { name: "mutate" }));
    await user.click(screen.getByRole("button", { name: "run" }));

    expect(adminClientMock.adminMutate).toHaveBeenCalledWith("test-db", {
      steps: [
        {
          op: "insert",
          table: "users",
          doc: { name: "Ada Lovelace", email: "ada@example.com" },
        },
      ],
    });
    expect(adminClientMock.adminQuery).not.toHaveBeenCalled();
    expect(screen.getByText(/"new-id"/)).toBeInTheDocument();
  });

  it("shows a local error for invalid JSON and does not call the client", async () => {
    const user = userEvent.setup();
    render(<QueryConsolePage />);

    const editor = screen.getByRole("textbox", { name: "query DSL" }) as HTMLTextAreaElement;
    await user.clear(editor);
    await user.type(editor, "not json");
    await user.click(screen.getByRole("button", { name: "run" }));

    expect(await screen.findByText("INVALID_JSON")).toBeInTheDocument();
    expect(adminClientMock.adminQuery).not.toHaveBeenCalled();
    expect(adminClientMock.adminMutate).not.toHaveBeenCalled();
  });

  it("surfaces a server error envelope (code + HTTP status + message)", async () => {
    const user = userEvent.setup();
    adminClientMock.adminQuery.mockRejectedValue(
      new RtDbRequestError("BAD_REQUEST", "unknown table: widgets", undefined, 400),
    );
    render(<QueryConsolePage />);

    await user.click(screen.getByRole("button", { name: "run" }));

    expect(await screen.findByText(/BAD_REQUEST/)).toBeInTheDocument();
    expect(screen.getByText(/HTTP 400/)).toBeInTheDocument();
    expect(screen.getByText(/unknown table: widgets/)).toBeInTheDocument();
  });

  it("saves a query by name, survives a simulated reload, and re-runs it with one click", async () => {
    const user = userEvent.setup();
    adminClientMock.adminQuery.mockResolvedValue([]);
    render(<QueryConsolePage />);
    await user.type(screen.getByLabelText("Query name"), "winners");
    await user.click(screen.getByRole("button", { name: "Save query" }));
    expect(JSON.parse(localStorage.getItem("rtdb.queryConsole.saved") ?? "[]")).toHaveLength(1);

    cleanup();
    render(<QueryConsolePage />);
    await user.click(screen.getByRole("button", { name: "winners" }));
    expect(adminClientMock.adminQuery).toHaveBeenCalledWith("test-db", {
      table: "users",
      take: 100,
    });
  });

  it("lists recent executed queries deduped and most-recent-first", async () => {
    const user = userEvent.setup();
    adminClientMock.adminQuery.mockResolvedValue([]);
    render(<QueryConsolePage />);

    const runQuery = async (body: string): Promise<void> => {
      const editor = screen.getByRole("textbox", { name: "query DSL" }) as HTMLTextAreaElement;
      await user.clear(editor);
      await user.type(editor, body.replaceAll("{", "{{"));
      await user.click(screen.getByRole("button", { name: "run" }));
      await screen.findByRole("button", { name: '{ "table": "a" }' });
    };

    await runQuery('{ "table": "a" }');
    await user.clear(screen.getByRole("textbox", { name: "query DSL" }));
    await user.type(screen.getByRole("textbox", { name: "query DSL" }), '{{ "other": "b" }');
    await user.click(screen.getByRole("button", { name: "run" }));
    await screen.findByRole("button", { name: '{ "other": "b" }' });
    await runQuery('{ "table": "a" }');
    const recentSection = screen.getByLabelText("Recent queries");
    const chips = within(recentSection).getAllByRole("button");
    expect(chips.map((c) => c.textContent)).toEqual(['{ "table": "a" }', '{ "other": "b" }']);
  });

  it("exports the current result grid as CSV and JSON downloads", async () => {
    const user = userEvent.setup();
    adminClientMock.adminQuery.mockResolvedValue([{ _id: "k1", name: "Ada" }]);
    render(<QueryConsolePage />);

    await user.click(screen.getByRole("button", { name: "run" }));
    await screen.findByText(/_id/);

    const create = vi.fn(() => "blob:page-export");
    vi.stubGlobal("URL", { createObjectURL: create, revokeObjectURL: vi.fn() });
    const click = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {});
    try {
      await user.click(screen.getByRole("button", { name: "Export CSV" }));
      expect(create).toHaveBeenCalledOnce();
      const anchor = click.mock.instances[0] as HTMLAnchorElement;
      expect(anchor.download).toBe("query-results.csv");
      await user.click(screen.getByRole("button", { name: "Export JSON" }));
      expect(click.mock.instances[1]).toHaveProperty("download", "query-results.json");
    } finally {
      vi.unstubAllGlobals();
    }
  });
});
