import { describe, expect, it, vi } from "vitest";
import type { EnrollmentGrant, HostCapability, MintTokenOptions, TokenInfo } from "../src/admin.js";
import { RtDbAdminClient } from "../src/admin.js";
import { RtDbHttpClient } from "../src/http.js";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function makeAdmin(fetchMock: ReturnType<typeof vi.fn>): RtDbAdminClient {
  return new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock as never });
}

const CAPABILITY: HostCapability = { type: "parMeshV1", host: "macdaddy" };

describe("host machine authorization (host-capability mirror)", () => {
  it("round-trips the parMeshV1 capability shape", () => {
    const wire = JSON.parse(JSON.stringify(CAPABILITY)) as HostCapability;
    expect(wire).toEqual({ type: "parMeshV1", host: "macdaddy" });
  });

  it("legacy mintToken serializes WITHOUT hostCapability (regression proof)", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ tokenId: "id1", token: "secret" }));
    const admin = makeAdmin(fetchMock);

    await admin.mintToken("kanban", "cli");

    const [, init] = fetchMock.mock.calls[0];
    const body = JSON.parse(init.body) as Record<string, unknown>;
    expect(body).toEqual({ db: "kanban", name: "cli" });
    expect("hostCapability" in body).toBe(false);
  });

  it("mintToken sends hostCapability when the options carry one", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ tokenId: "id2", token: "s2" }));
    const admin = makeAdmin(fetchMock);

    const opts: MintTokenOptions = { hostCapability: CAPABILITY };
    await admin.mintToken("dbx", "host-token", opts);

    const [, init] = fetchMock.mock.calls[0];
    expect(JSON.parse(init.body)).toEqual({
      db: "dbx",
      name: "host-token",
      hostCapability: { type: "parMeshV1", host: "macdaddy" },
    });
  });

  it("mintToken leaves hostCapability absent when the options omit it", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ tokenId: "id3", token: "s3" }));
    const admin = makeAdmin(fetchMock);

    await admin.mintToken("dbx", "plain", { readOnly: true });

    const [, init] = fetchMock.mock.calls[0];
    const body = JSON.parse(init.body) as Record<string, unknown>;
    expect(body).toEqual({ db: "dbx", name: "plain", readOnly: true });
    expect("hostCapability" in body).toBe(false);
  });

  it("listTokens parses an optional hostCapability and tolerates its absence", async () => {
    const withCap: TokenInfo = {
      id: "t1",
      name: "host-token",
      createdAt: 1000,
      revoked: false,
      expiresAt: null,
      readOnly: false,
      tables: null,
      hostCapability: CAPABILITY,
    };
    const withoutCap: Record<string, unknown> = {
      id: "t2",
      name: "plain",
      createdAt: 2000,
      revoked: true,
      expiresAt: null,
      readOnly: false,
      tables: null,
      // hostCapability deliberately ABSENT (server skip_serializing_if), not null
    };
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ tokens: [withCap, withoutCap] }));
    const admin = makeAdmin(fetchMock);

    const tokens = await admin.listTokens("dbx");
    expect(tokens).toHaveLength(2);
    expect(tokens[0].hostCapability).toEqual({ type: "parMeshV1", host: "macdaddy" });
    expect("hostCapability" in tokens[1]).toBe(false);
    expect(tokens[1].hostCapability).toBeUndefined();
  });

  it("createEnrollmentGrant posts the frozen camelCase shape and parses the one-time grant", async () => {
    const response: EnrollmentGrant = {
      grantId: "g1",
      grant: "one-time-secret",
      db: "app",
      capability: CAPABILITY,
      expiresAt: 1761100000000,
    };
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(response));
    const admin = makeAdmin(fetchMock);

    await expect(
      admin.createEnrollmentGrant({
        db: "app",
        name: "macdaddy-enroll",
        capability: CAPABILITY,
        expiresAt: 1761100000000,
        tokenExpiresAt: 1761100900000,
      }),
    ).resolves.toEqual(response);

    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/enrollment-grants");
    expect(init.method).toBe("POST");
    expect(init.headers.Authorization).toBe("Bearer k");
    expect(JSON.parse(init.body)).toEqual({
      db: "app",
      name: "macdaddy-enroll",
      capability: { type: "parMeshV1", host: "macdaddy" },
      expiresAt: 1761100000000,
      tokenExpiresAt: 1761100900000,
    });
  });

  it("createEnrollmentGrant omits tokenExpiresAt when not provided", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        grantId: "g2",
        grant: "s",
        db: "app",
        capability: CAPABILITY,
        expiresAt: 1761100000000,
      }),
    );
    const admin = makeAdmin(fetchMock);

    await admin.createEnrollmentGrant({
      db: "app",
      name: "macdaddy-enroll",
      capability: CAPABILITY,
      expiresAt: 1761100000000,
    });

    const [, init] = fetchMock.mock.calls[0];
    const body = JSON.parse(init.body) as Record<string, unknown>;
    expect("tokenExpiresAt" in body).toBe(false);
  });

  it("revokeEnrollmentGrant posts only the grantId", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ ok: true }));
    const admin = makeAdmin(fetchMock);

    await expect(admin.revokeEnrollmentGrant("g1")).resolves.toBeUndefined();

    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/revoke-enrollment-grant");
    expect(init.method).toBe("POST");
    expect(JSON.parse(init.body)).toEqual({ grantId: "g1" });
  });

  it("enroll posts ONLY the grant with no auth header and parses the mint result", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        tokenId: "t9",
        token: "machine-secret",
        db: "app",
        capability: CAPABILITY,
      }),
    );
    const admin = makeAdmin(fetchMock);

    await expect(admin.enroll("grant-secret")).resolves.toEqual({
      tokenId: "t9",
      token: "machine-secret",
      db: "app",
      capability: { type: "parMeshV1", host: "macdaddy" },
    });

    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/auth/enroll");
    expect(init.method).toBe("POST");
    expect(JSON.parse(init.body)).toEqual({ grant: "grant-secret" });
    expect(init.headers.Authorization).toBeUndefined();
  });

  it("RtDbHttpClient.enroll works with no admin credential and sends no Authorization header", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        tokenId: "t10",
        token: "machine-secret",
        db: "app",
        capability: CAPABILITY,
      }),
    );
    // No admin credential exists on this client at all; the data-plane token is
    // an unused placeholder during bootstrap (redemption yields the real one).
    const http = new RtDbHttpClient({
      url: "http://h:8300",
      db: "app",
      token: "",
      fetch: fetchMock as never,
    });

    await expect(http.enroll("grant-secret")).resolves.toEqual({
      tokenId: "t10",
      token: "machine-secret",
      db: "app",
      capability: { type: "parMeshV1", host: "macdaddy" },
    });

    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/auth/enroll");
    expect(init.method).toBe("POST");
    expect(JSON.parse(init.body)).toEqual({ grant: "grant-secret" });
    expect(init.headers.Authorization).toBeUndefined();
  });

  it("admin enroll remains available as a delegating compatibility alias", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        tokenId: "t11",
        token: "machine-secret",
        db: "app",
        capability: CAPABILITY,
      }),
    );
    const admin = makeAdmin(fetchMock);

    await expect(admin.enroll("grant-secret")).resolves.toEqual({
      tokenId: "t11",
      token: "machine-secret",
      db: "app",
      capability: { type: "parMeshV1", host: "macdaddy" },
    });

    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/auth/enroll");
    // The alias rides the same no-auth request path — no credential leaks.
    expect(init.headers.Authorization).toBeUndefined();
  });
});
