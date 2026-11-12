import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

// Admin allowlist: list with two-step remove confirm, add-member form wired to
// the client (email + optional numeric github id), form validation.

const adminClientMock = vi.hoisted(() => ({
  adminsList: vi.fn(),
  addAdmin: vi.fn(),
  removeAdmin: vi.fn(),
}));

vi.mock("../lib/admin", () => ({
  useAdmin: () => ({ client: adminClientMock }),
}));

import { AdminsPage } from "./AdminsPage";

const member = { email: "ops@example.com", githubId: 42 };

describe("AdminsPage", () => {
  beforeEach(() => {
    adminClientMock.adminsList.mockReset().mockResolvedValue([member]);
    adminClientMock.addAdmin.mockReset().mockResolvedValue(undefined);
    adminClientMock.removeAdmin.mockReset().mockResolvedValue(undefined);
  });

  it("lists allowlisted admins with their github id", async () => {
    render(<AdminsPage />);
    await waitFor(() => expect(screen.getByText("ops@example.com")).toBeTruthy());
    expect(screen.getByText("42")).toBeTruthy();
  });

  it("shows the empty state when nobody is allowlisted", async () => {
    adminClientMock.adminsList.mockResolvedValue([]);
    render(<AdminsPage />);
    expect(await screen.findByText("No allowlisted admins.")).toBeTruthy();
  });

  it("adds a member with email and optional numeric github id, then reloads", async () => {
    adminClientMock.adminsList
      .mockResolvedValueOnce([member])
      .mockResolvedValueOnce([member, { email: "new@example.com" }]);
    render(<AdminsPage />);
    await screen.findByText("ops@example.com");

    await userEvent.type(screen.getByPlaceholderText("email"), "new@example.com");
    await userEvent.type(screen.getByPlaceholderText("github id (optional)"), "77");
    await userEvent.click(screen.getByRole("button", { name: "add" }));

    await waitFor(() => {
      expect(adminClientMock.addAdmin).toHaveBeenCalledWith("new@example.com", 77);
    });
    expect(await screen.findByText("new@example.com")).toBeTruthy();
  });

  it("requires an email to add", async () => {
    render(<AdminsPage />);
    await screen.findByText("ops@example.com");
    await userEvent.click(screen.getByRole("button", { name: "add" }));
    expect(await screen.findByText("email is required")).toBeTruthy();
    expect(adminClientMock.addAdmin).not.toHaveBeenCalled();
  });

  it("removes a member only after the two-step confirm, then reloads", async () => {
    adminClientMock.adminsList.mockResolvedValueOnce([member]).mockResolvedValueOnce([]);
    render(<AdminsPage />);
    await screen.findByText("ops@example.com");

    await userEvent.click(screen.getByRole("button", { name: "remove" }));
    await userEvent.click(screen.getByRole("button", { name: "confirm remove" }));

    await waitFor(() => {
      expect(adminClientMock.removeAdmin).toHaveBeenCalledWith("ops@example.com");
    });
    expect(await screen.findByText("No allowlisted admins.")).toBeTruthy();
  });

  it("cancel backs out of the remove confirm without removing", async () => {
    render(<AdminsPage />);
    await screen.findByText("ops@example.com");
    await userEvent.click(screen.getByRole("button", { name: "remove" }));
    await userEvent.click(screen.getByRole("button", { name: "cancel" }));
    expect(screen.getByRole("button", { name: "remove" })).toBeTruthy();
    expect(adminClientMock.removeAdmin).not.toHaveBeenCalled();
  });

  it("surfaces toErrorMessage when adding fails", async () => {
    adminClientMock.addAdmin.mockRejectedValue(new Error("server said no"));
    render(<AdminsPage />);
    await screen.findByText("ops@example.com");
    await userEvent.type(screen.getByPlaceholderText("email"), "x@example.com");
    await userEvent.click(screen.getByRole("button", { name: "add" }));
    expect(await screen.findByText("server said no")).toBeTruthy();
  });
});
