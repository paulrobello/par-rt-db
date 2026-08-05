import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

// SchemaPage renders the applied schema and, in push mode, runs an
// additive-only preview against `client.previewSchema`. The contract:
//   - view mode renders the applied schema's tables/fields/indexes
//   - preview renders the diff: ADDED tables/columns and REJECTED drops
//   - preview surfaces server errors (code + status + message)
//   - apply calls pushSchema and refreshes the view

const adminClientMock = vi.hoisted(() => ({
  getSchema: vi.fn(),
  previewSchema: vi.fn(),
  pushSchema: vi.fn(),
}));

vi.mock("react-router-dom", async (importActual) => {
  const actual = await importActual<typeof import("react-router-dom")>();
  return {
    ...actual,
    useParams: () => ({ db: "test-db" }),
    // Link needs a Router context we don't mount in the test; stub it.
    Link: ({ children, to }: { children: React.ReactNode; to: string }) => (
      <a href={to}>{children}</a>
    ),
  };
});

vi.mock("../lib/admin", async (importActual) => {
  const actual = await importActual<typeof import("../lib/admin")>();
  return {
    ...actual,
    useAdmin: () => ({ client: adminClientMock }),
  };
});

import { RtDbRequestError } from "../lib/admin";
import { SchemaPage } from "./SchemaPage";

const APPLIED_SCHEMA = {
  tables: {
    items: {
      fields: { name: { type: "string" } },
      indexes: [{ name: "by_name", fields: ["name"] }],
    },
  },
};

// A vector index carries its distance metric on `vector.metric` (ENH-007);
// cosine is omitted on the wire, so a cosine index has no `metric` key.
const VECTOR_SCHEMA = {
  tables: {
    docs: {
      fields: { embedding: { type: "vector", dimensions: 4 } },
      indexes: [{ name: "by_emb", fields: ["embedding"], vector: { dimensions: 4, metric: "l2" } }],
    },
  },
};

describe("SchemaPage", () => {
  beforeEach(() => {
    adminClientMock.getSchema.mockReset();
    adminClientMock.previewSchema.mockReset();
    adminClientMock.pushSchema.mockReset();
    adminClientMock.getSchema.mockResolvedValue(APPLIED_SCHEMA);
  });

  it("renders the applied schema in view mode", async () => {
    render(<SchemaPage />);
    expect(await screen.findByText("items")).toBeInTheDocument();
    expect(screen.getByText("name")).toBeInTheDocument();
    expect(screen.getByText(/string/)).toBeInTheDocument();
    expect(screen.getByText("by_name")).toBeInTheDocument();
  });

  it("renders a vector index with its declared distance metric", async () => {
    adminClientMock.getSchema.mockResolvedValue(VECTOR_SCHEMA);
    render(<SchemaPage />);
    expect(await screen.findByText("by_emb")).toBeInTheDocument();
    expect(screen.getByText("VEC·l2")).toBeInTheDocument();
  });

  it("defaults an unspecified vector index metric to cosine", async () => {
    adminClientMock.getSchema.mockResolvedValue({
      tables: {
        docs: {
          fields: { embedding: { type: "vector", dimensions: 4 } },
          indexes: [{ name: "by_emb", fields: ["embedding"], vector: { dimensions: 4 } }],
        },
      },
    });
    render(<SchemaPage />);
    expect(await screen.findByText("VEC·cosine")).toBeInTheDocument();
  });

  it("switches to push mode and previews an additive diff (added column)", async () => {
    const user = userEvent.setup();
    adminClientMock.previewSchema.mockResolvedValue({
      added: [
        {
          table: "items",
          columns: [{ name: "count", fieldType: "number" }],
          indexes: [],
        },
      ],
      rejected: [],
    });
    render(<SchemaPage />);
    await screen.findByText("items");

    await user.click(screen.getByRole("button", { name: "push" }));
    await user.click(screen.getByRole("button", { name: "preview" }));

    // The added column appears under the "will add" section.
    expect(await screen.findByText(/will add/i)).toBeInTheDocument();
    expect(screen.getByText("count")).toBeInTheDocument();
    expect(screen.getByText("number")).toBeInTheDocument();
    expect(adminClientMock.previewSchema).toHaveBeenCalled();
  });

  it("renders rejections in the preview panel", async () => {
    const user = userEvent.setup();
    adminClientMock.previewSchema.mockResolvedValue({
      added: [],
      rejected: [
        {
          table: "items",
          item: "name",
          reason: "column 'name' cannot be dropped (pushes are additive)",
        },
      ],
    });
    render(<SchemaPage />);
    await screen.findByText("items");

    await user.click(screen.getByRole("button", { name: "push" }));
    await user.click(screen.getByRole("button", { name: "preview" }));

    expect(await screen.findByText(/will reject/i)).toBeInTheDocument();
    expect(screen.getByText(/cannot be dropped/)).toBeInTheDocument();
  });

  it("surfaces a preview server error envelope", async () => {
    const user = userEvent.setup();
    adminClientMock.previewSchema.mockRejectedValue(
      new RtDbRequestError("SCHEMA_VIOLATION", 400, "invalid table name"),
    );
    render(<SchemaPage />);
    await screen.findByText("items");

    await user.click(screen.getByRole("button", { name: "push" }));
    await user.click(screen.getByRole("button", { name: "preview" }));

    expect(await screen.findByText(/SCHEMA_VIOLATION/)).toBeInTheDocument();
    expect(screen.getByText(/invalid table name/)).toBeInTheDocument();
  });

  it("shows a local error for invalid JSON and does not call previewSchema", async () => {
    const user = userEvent.setup();
    render(<SchemaPage />);
    await screen.findByText("items");

    await user.click(screen.getByRole("button", { name: "push" }));
    const editor = screen.getByRole("textbox", { name: "schema JSON" }) as HTMLTextAreaElement;
    await user.clear(editor);
    await user.type(editor, "not json");
    await user.click(screen.getByRole("button", { name: "preview" }));

    expect(await screen.findByText("INVALID_JSON")).toBeInTheDocument();
    expect(adminClientMock.previewSchema).not.toHaveBeenCalled();
  });

  it("applies the schema via pushSchema and refreshes the view", async () => {
    const user = userEvent.setup();
    adminClientMock.pushSchema.mockResolvedValue({ ok: true });
    render(<SchemaPage />);
    await screen.findByText("items");

    await user.click(screen.getByRole("button", { name: "push" }));
    await user.click(screen.getByRole("button", { name: "apply" }));

    await waitFor(() => {
      expect(adminClientMock.pushSchema).toHaveBeenCalled();
    });
    // The applied schema is re-fetched after a successful push.
    await waitFor(() => {
      expect(adminClientMock.getSchema.mock.calls.length).toBeGreaterThanOrEqual(2);
    });
  });
});
