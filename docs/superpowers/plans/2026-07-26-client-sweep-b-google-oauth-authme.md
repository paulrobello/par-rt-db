# Client Sweep — Item B: Google OAuth + `/auth/me` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Google sign-in (`signInWithGoogle`) and an `authMe()` HTTP helper to the ts-client, completing its auth surface to match the server and the rust client.

**Architecture:** Two surgical additions, no wire/protocol changes. (1) An `authMe()` method on `RtDbHttpClient` that reuses the existing private `get(path, bearer)` helper to hit `GET /auth/me` with the client's own token. (2) A shared `signInWithOAuth(baseUrl, provider)` popup core extracted from `signInWithGitHub` so `signInWithGoogle` is a one-line variant; `useRtDbAuth().signIn` grows an optional `provider` argument (`"github" | "google"`, default `"github"`).

**Tech Stack:** TypeScript, React 19, Vitest + @testing-library/react, biome. ts-client is a bun workspace.

## Global Constraints

- **No wire/protocol changes** — B only adds client surface that calls routes the server already mounts (`/auth/google` GET, `/auth/me` GET, both in `server/src/auth/provider.rs:429,435`). `protocol.ts` is untouched.
- **Reuse `AuthedUser`** (`ts-client/src/protocol.ts:120`) — do not redefine it.
- **Style:** ESM with `.js` import specifiers, biome formatting (matches the surrounding files).
- **Tests are pure unit tests** — no server, no Postgres. HTTP tests inject a `fetch` mock; React tests use the existing `FakeSocket` harness in `react.test.tsx`.
- **Verification:** each task runs its ts-client test file; the final task runs `make checkall` (the repo gate). `cd ts-client && bunx vitest run tests/<file>` runs one file.
- **Commits:** one atomic commit per task, conventional style (`feat(ts-client): …`).

---

## File Structure

- `ts-client/src/http.ts` — add `authMe()` method (reuses private `get`).
- `ts-client/src/react.tsx` — extract `signInWithOAuth(baseUrl, provider)`; add `signInWithGoogle`; widen `useRtDbAuth().signIn` signature.
- `ts-client/tests/http.test.ts` — `authMe` coverage mirroring the existing `validateSessionToken` tests.
- `ts-client/tests/react.test.tsx` — `signInWithGoogle` block + a `useRtDbAuth` provider-routing test.
- `ts-client/README.md` — one-line auth mention update.
- `FEATURE_MATRIX.md` — verify row #14 (OAuth providers) reflects that ts-client now genuinely ships Google sign-in.

---

## Task 1: `authMe()` on RtDbHttpClient

**Files:**
- Modify: `ts-client/src/http.ts` (add method after `validateSessionToken`, ~line 97)
- Test: `ts-client/tests/http.test.ts` (add cases inside the existing `describe("RtDbHttpClient", …)`)

**Interfaces:**
- Consumes: private `get(path: string, bearer: string)` (`http.ts:146`), `this.token`, `AuthedUser` (`protocol.ts:120`).
- Produces: `RtDbHttpClient.authMe(): Promise<AuthedUser>` — distinct from `validateSessionToken(token)` because it uses the client's own bearer, not an argument.

- [ ] **Step 1: Write the failing tests**

Add these inside the existing `describe("RtDbHttpClient", () => { … })` block in `ts-client/tests/http.test.ts` (after the `validateSessionToken tolerates…` test, before the closing `});` at line 251):

```ts
  it("authMe GETs /auth/me with the client's own bearer and returns the user", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        user: {
          kind: "user",
          email: "player@example.com",
          name: null,
          githubLogin: "player",
          githubId: 42,
        },
      }),
    );
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "client-own-token",
      fetch: fetchMock,
    });

    const user = await client.authMe();

    expect(user).toEqual({
      kind: "user",
      email: "player@example.com",
      name: null,
      githubLogin: "player",
      githubId: 42,
    });
    const [calledUrl, init] = fetchMock.mock.calls[0];
    expect(calledUrl).toBe("http://h:8300/auth/me");
    expect(init.method).toBe("GET");
    // authMe uses the client's own token, not an argument like validateSessionToken.
    expect(init.headers.Authorization).toBe("Bearer client-own-token");
  });

  it("authMe surfaces a 401 as an RtDbError envelope", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(jsonResponse({ code: "UNAUTHORIZED", message: "machine token rejected" }, 401));
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });

    await expect(client.authMe()).rejects.toMatchObject({
      name: "RtDbError",
      code: "UNAUTHORIZED",
      message: "machine token rejected",
    });
  });
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd ts-client && bunx vitest run tests/http.test.ts -t "authMe"`
Expected: FAIL — `client.authMe is not a function` (TypeError).

- [ ] **Step 3: Implement `authMe()`**

In `ts-client/src/http.ts`, add this method immediately after `validateSessionToken` (after line 97, before the `upload` method):

```ts
  /**
   * Resolve the principal the client is authenticated as, via `GET /auth/me`
   * with the client's own bearer. Session-only — unlike `validateSessionToken`,
   * which validates an arbitrary token passed as an argument. Machine tokens are
   * rejected by the server (401) and surface as the standard `RtDbError` envelope.
   */
  async authMe(): Promise<AuthedUser> {
    const body = await this.get("/auth/me", this.token);
    return (body as { user: AuthedUser }).user;
  }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd ts-client && bunx vitest run tests/http.test.ts`
Expected: PASS — all http tests green, including the two new `authMe` cases.

- [ ] **Step 5: Commit**

```bash
git add ts-client/src/http.ts ts-client/tests/http.test.ts
git commit -m "feat(ts-client): add authMe() HTTP helper (GET /auth/me)"
```

---

## Task 2: `signInWithGoogle` + shared OAuth popup core

**Files:**
- Modify: `ts-client/src/react.tsx` (replace `signInWithGitHub` at lines 169-207 with a shared core + two thin wrappers)
- Test: `ts-client/tests/react.test.tsx` (add a `signInWithGoogle` describe block; extend the import)

**Interfaces:**
- Consumes: `window.open`, `window` message events (existing `signInWithGitHub` pattern at `react.tsx:170-207`).
- Produces: `signInWithGoogle(baseUrl: string): Promise<string>` exported from `react.tsx`, plus an internal `signInWithOAuth(baseUrl, provider)`. `signInWithGitHub` keeps its existing export and behavior (backward compatible).

- [ ] **Step 1: Write the failing test**

In `ts-client/tests/react.test.tsx`, first extend the import from `../src/react.js` (line 5-14) to include `signInWithGoogle`:

```ts
import {
  Authenticated,
  AuthLoading,
  RtDbProvider,
  signInWithGitHub,
  signInWithGoogle,
  Unauthenticated,
  useConnectionState,
  usePaginatedQuery,
  useQuery,
} from "../src/react.js";
```

Then add this `describe` block immediately after the existing `signInWithGitHub` block (after line 207, before `describe("usePaginatedQuery", …)`):

```ts
describe("signInWithGoogle", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("opens the /auth/google popup and resolves with the token from a valid rtdb-auth message", async () => {
    const popup = { closed: false };
    const openSpy = vi.spyOn(window, "open").mockReturnValue(popup as unknown as Window);

    const promise = signInWithGoogle("http://h:8300");
    window.dispatchEvent(
      new MessageEvent("message", {
        origin: "http://h:8300",
        data: { type: "rtdb-auth", token: "goog-tok" },
      }),
    );

    await expect(promise).resolves.toBe("goog-tok");
    expect(openSpy).toHaveBeenCalledWith(
      expect.stringContaining("/auth/google?origin="),
      "rtdb-auth",
      "width=600,height=700",
    );
  });

  it("rejects immediately when the popup is blocked", async () => {
    vi.spyOn(window, "open").mockReturnValue(null);
    await expect(signInWithGoogle("http://h:8300")).rejects.toThrow("popup blocked");
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd ts-client && bunx vitest run tests/react.test.tsx -t "signInWithGoogle"`
Expected: FAIL — `signInWithGoogle is not a function` (import resolves to `undefined`).

- [ ] **Step 3: Extract the shared core and add `signInWithGoogle`**

In `ts-client/src/react.tsx`, replace the entire `signInWithGitHub` function (the JSDoc comment + function, lines 169-207) with:

```ts
/** Opens the server's OAuth popup for `provider` and resolves with the session token it posts back. */
function signInWithOAuth(baseUrl: string, provider: "github" | "google"): Promise<string> {
  const origin = new URL(baseUrl).origin;
  const spaOrigin = window.location.origin;
  const popup = window.open(
    `${baseUrl.replace(/\/+$/, "")}/auth/${provider}?origin=${encodeURIComponent(spaOrigin)}`,
    "rtdb-auth",
    "width=600,height=700",
  );

  return new Promise<string>((resolve, reject) => {
    if (!popup) {
      reject(new Error("popup blocked"));
      return;
    }
    const cleanup = () => {
      window.removeEventListener("message", onMessage);
      clearInterval(closedPoll);
    };
    const onMessage = (event: MessageEvent) => {
      if (event.origin !== origin) {
        return;
      }
      const data = event.data as { type?: string; token?: string };
      if (data?.type !== "rtdb-auth" || typeof data.token !== "string") {
        return;
      }
      cleanup();
      resolve(data.token);
    };
    window.addEventListener("message", onMessage);
    const closedPoll = setInterval(() => {
      if (popup.closed) {
        cleanup();
        reject(new Error("popup closed before completing sign-in"));
      }
    }, 500);
  });
}

/** Opens the server's GitHub OAuth popup and resolves with the session token it posts back. */
export function signInWithGitHub(baseUrl: string): Promise<string> {
  return signInWithOAuth(baseUrl, "github");
}

/** Opens the server's Google OAuth popup and resolves with the session token it posts back. */
export function signInWithGoogle(baseUrl: string): Promise<string> {
  return signInWithOAuth(baseUrl, "google");
}
```

- [ ] **Step 4: Run the tests to verify they pass (both providers)**

Run: `cd ts-client && bunx vitest run tests/react.test.tsx`
Expected: PASS — the new `signInWithGoogle` cases pass AND the existing `signInWithGitHub` cases still pass (the refactor preserved GitHub behavior).

- [ ] **Step 5: Commit**

```bash
git add ts-client/src/react.tsx ts-client/tests/react.test.tsx
git commit -m "feat(ts-client): add signInWithGoogle via shared OAuth popup core"
```

---

## Task 3: Provider-aware `useRtDbAuth().signIn`

**Files:**
- Modify: `ts-client/src/react.tsx` (widen the `signIn` signature and routing in `useRtDbAuth`, lines 123-137)
- Test: `ts-client/tests/react.test.tsx` (add a `useRtDbAuth` routing test; extend the import)

**Interfaces:**
- Consumes: `signInWithGitHub`, `signInWithGoogle` (from Task 2), the `setup()` harness in `react.test.tsx:38`.
- Produces: `useRtDbAuth().signIn(provider?: "github" | "google"): Promise<void>` — default `"github"` preserves the existing call shape.

- [ ] **Step 1: Write the failing test**

Extend the `react.js` import in `ts-client/tests/react.test.tsx` (the block edited in Task 2) to also include `useRtDbAuth`, and add `fireEvent` to the `@testing-library/react` import on line 1:

```ts
import { act, fireEvent, render, screen } from "@testing-library/react";
```

```ts
import {
  Authenticated,
  AuthLoading,
  RtDbProvider,
  signInWithGitHub,
  signInWithGoogle,
  Unauthenticated,
  useConnectionState,
  usePaginatedQuery,
  useQuery,
  useRtDbAuth,
} from "../src/react.js";
```

Add this `describe` block after the `signInWithGoogle` block from Task 2:

```ts
describe("useRtDbAuth signIn routing", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("signIn('google') opens the /auth/google popup", async () => {
    const { client } = setup();
    const popup = { closed: false };
    const openSpy = vi.spyOn(window, "open").mockReturnValue(popup as unknown as Window);

    function View() {
      const { signIn } = useRtDbAuth();
      return (
        <button type="button" onClick={() => void signIn("google")}>
          google
        </button>
      );
    }

    render(
      <RtDbProvider client={client} authBaseUrl="http://h:8300">
        <View />
      </RtDbProvider>,
    );

    await act(async () => {
      fireEvent.click(screen.getByText("google"));
    });

    expect(openSpy).toHaveBeenCalledWith(
      expect.stringContaining("/auth/google?origin="),
      "rtdb-auth",
      "width=600,height=700",
    );
  });

  it("signIn() with no argument opens the /auth/github popup (default)", async () => {
    const { client } = setup();
    const popup = { closed: false };
    const openSpy = vi.spyOn(window, "open").mockReturnValue(popup as unknown as Window);

    function View() {
      const { signIn } = useRtDbAuth();
      return (
        <button type="button" onClick={() => void signIn()}>
          default
        </button>
      );
    }

    render(
      <RtDbProvider client={client} authBaseUrl="http://h:8300">
        <View />
      </RtDbProvider>,
    );

    await act(async () => {
      fireEvent.click(screen.getByText("default"));
    });

    expect(openSpy).toHaveBeenCalledWith(
      expect.stringContaining("/auth/github?origin="),
      "rtdb-auth",
      "width=600,height=700",
    );
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd ts-client && bunx vitest run tests/react.test.tsx -t "signIn routing"`
Expected: FAIL — TypeScript error: `signIn` accepts 0 arguments, so `signIn("google")` is a type error (and the google-popup assertion fails).

- [ ] **Step 3: Widen `signIn` to route by provider**

In `ts-client/src/react.tsx`, update the `useRtDbAuth` return type and the `signIn` callback (lines 123-137). Replace the signature and the `signIn = useCallback(…)` body:

```ts
export function useRtDbAuth(): {
  state: AuthState;
  user: AuthedUser | null;
  signIn: (provider?: "github" | "google") => Promise<void>;
  signOut: () => Promise<void>;
} {
  const { client, authBaseUrl, state, user } = useContextValue();

  const signIn = useCallback(
    async (provider: "github" | "google" = "github") => {
      const token =
        provider === "google"
          ? await signInWithGoogle(authBaseUrl)
          : await signInWithGitHub(authBaseUrl);
      if (typeof localStorage !== "undefined") {
        localStorage.setItem(TOKEN_STORAGE_KEY, token);
      }
      client.setToken(token);
    },
    [client, authBaseUrl],
  );
```

Leave the rest of `useRtDbAuth` (`signOut` and the `return { state, user, signIn, signOut }`) unchanged.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd ts-client && bunx vitest run tests/react.test.tsx`
Expected: PASS — both `signIn routing` cases pass and all prior react tests still pass.

- [ ] **Step 5: Commit**

```bash
git add ts-client/src/react.tsx ts-client/tests/react.test.tsx
git commit -m "feat(ts-client): provider-aware useRtDbAuth().signIn (github | google)"
```

---

## Task 4: Docs touch + full gate

**Files:**
- Modify: `ts-client/README.md` (line 55 auth mention)
- Verify: `FEATURE_MATRIX.md` row #14 wording

- [ ] **Step 1: Update the README auth mention**

In `ts-client/README.md`, the line that reads approximately `// is the server's HTTP origin, used for the GitHub sign-in popup and logout.` (line 55) — change `GitHub sign-in popup` to `GitHub/Google sign-in popup`. If the README documents the auth helpers elsewhere, add a one-line mention of `signInWithGoogle` and `authMe()` next to the existing `signInWithGitHub` / `validateSessionToken` mention (do not invent a section if none exists).

- [ ] **Step 2: Verify FEATURE_MATRIX row #14**

Open `FEATURE_MATRIX.md` and read row #14 ("Additional OAuth providers"). It already claims Google is implemented across clients. Confirm its wording does not overstate ts-client before this change; if it implied ts-client shipped a Google helper it did not have, no correction is now needed (B just made it true). No edit required unless the row is inaccurate — if so, make it accurate.

- [ ] **Step 3: Run the full repo gate**

Run: `make checkall`
Expected: PASS — fmt-check + clippy + typecheck + tests across all five packages green. (B touches only ts-client, but the gate is the definition of done.)

- [ ] **Step 4: Commit**

```bash
git add ts-client/README.md FEATURE_MATRIX.md
git commit -m "docs(ts-client): note Google sign-in + authMe in auth surface"
```

(If `FEATURE_MATRIX.md` needed no edit, stage only the README.)

---

## Self-Review (completed during authoring)

- **Spec coverage:** B's three deliverables — `signInWithGoogle`, provider-aware `signIn`, `authMe` — are Tasks 2, 3, 1 respectively. ✅
- **Placeholders:** none; every step has real code or a real command. ✅
- **Type consistency:** `authMe(): Promise<AuthedUser>` (Task 1) matches the `AuthedUser` import already in `http.ts:2`. `signIn(provider?: "github" | "google")` (Task 3) matches the `signInWithOAuth` provider union (Task 2). ✅
- **Backward compatibility:** `signInWithGitHub` export and `signIn()` no-arg call shape are preserved. ✅
