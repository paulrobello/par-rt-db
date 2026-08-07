import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ConfigResponse } from "../lib/types";

// ConfigPage owns the runtime-mutable hot-config form (allowed origins, session
// TTL, max file size). The validation cascade must reject non-numeric /
// negative numbers BEFORE the PATCH — a bad value must never reach the server.

const adminClientMock = vi.hoisted(() => ({
  getConfig: vi.fn(),
  patchConfig: vi.fn(),
}));

vi.mock("../lib/admin", () => ({
  useAdmin: () => ({ client: adminClientMock }),
}));

import { ConfigPage } from "./ConfigPage";

const baseConfig: ConfigResponse = {
  port: 8300,
  publicUrl: "",
  githubBaseUrl: "",
  githubApiUrl: "",
  databaseUrlConfigured: true,
  adminKeyConfigured: true,
  githubConfigured: false,
  googleConfigured: false,
  gitlabConfigured: false,
  oidcConfigured: false,
  hot: {
    allowedOrigins: ["https://app.example.com"],
    sessionTtlDays: 7,
    maxFileSize: 10_485_760,
    idempotencyTtlMs: 300_000,
    maxTablesPerDb: 0,
    maxStorageBytesPerDb: 0,
    maxSubsPerDb: 0,
  },
  version: "0.1.0",
  gitCommit: "abcdef0123456789",
  admins: [],
};

describe("ConfigPage form validation", () => {
  beforeEach(() => {
    adminClientMock.getConfig.mockReset();
    adminClientMock.patchConfig.mockReset();
    adminClientMock.getConfig.mockResolvedValue(baseConfig);
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("rejects a negative session TTL without calling patchConfig", async () => {
    const user = userEvent.setup();
    render(<ConfigPage />);
    expect(await screen.findByDisplayValue("7")).toBeInTheDocument();

    const ttl = screen.getByDisplayValue("7");
    await user.clear(ttl);
    await user.type(ttl, "-3");
    await user.click(screen.getByRole("button", { name: "save" }));

    expect(
      await screen.findByText(/session TTL must be a non-negative number/i),
    ).toBeInTheDocument();
    expect(adminClientMock.patchConfig).not.toHaveBeenCalled();
  });

  it("rejects a non-numeric max file size without calling patchConfig", async () => {
    const user = userEvent.setup();
    render(<ConfigPage />);
    expect(await screen.findByDisplayValue("7")).toBeInTheDocument();

    const max = screen.getByDisplayValue(String(10_485_760));
    await user.clear(max);
    await user.type(max, "big");
    await user.click(screen.getByRole("button", { name: "save" }));

    expect(
      await screen.findByText(/max file size must be a non-negative number/i),
    ).toBeInTheDocument();
    expect(adminClientMock.patchConfig).not.toHaveBeenCalled();
  });

  it("submits a valid patch and confirms it applied live", async () => {
    adminClientMock.patchConfig.mockResolvedValue(baseConfig);
    const user = userEvent.setup();
    render(<ConfigPage />);
    expect(await screen.findByDisplayValue("7")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "save" }));

    expect(await screen.findByText("applied live")).toBeInTheDocument();
    expect(adminClientMock.patchConfig).toHaveBeenCalledWith({
      allowedOrigins: ["https://app.example.com"],
      sessionTtlDays: 7,
      maxFileSize: 10_485_760,
      idempotencyTtlMs: 300_000,
      maxTablesPerDb: 0,
      maxStorageBytesPerDb: 0,
      maxSubsPerDb: 0,
    });
  });
});
