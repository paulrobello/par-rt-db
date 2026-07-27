# OAuth Provider Setup

How to register OAuth apps and wire them into par-rt-db. Each provider is
**independently optional**: leave its env vars blank and the provider is disabled
(its `/auth/...` routes return `503`); machine tokens and the admin key keep
working with zero providers configured.

> **Source of truth:** the provider implementations in `server/src/auth/`
> (`provider.rs` for the shared trait/plumbing, `github.rs`, `google.rs`). This
> guide mirrors those files — if the code and this doc disagree, the code wins;
> fix this doc.

## How it works

- The browser starts a login at `GET /auth/{provider}?origin=<app-origin>`. The
  `origin` is validated against the **live** `RTDB_ALLOWED_ORIGINS` (the same
  list hot-reloadable via `PATCH /admin/config`); an origin not on the list is
  `403`. The server then `302`s to the provider's authorize URL.
- The provider redirects back to `/auth/{provider}/callback?code=&state=`, the
  server exchanges the code for an access token, fetches the user's **verified**
  email, upserts `rtdb_auth.users`, and mints a session (HttpOnly cookie).
- Identity is **email-keyed with cross-provider linking**: a user who first
  logs in via GitHub and later via Google (same verified email) resolves to the
  same account. Enabling more providers is additive, not fragmenting.
- Both providers **require a verified email**. Google requires `email_verified`;
  GitHub picks the primary verified address (falling back to any verified one)
  and never trusts the unverified profile-level email.
- A provider is "configured" only when **both** its `CLIENT_ID` and
  `CLIENT_SECRET` are non-empty (`from_config` returns `None` otherwise).

## Prerequisites (all providers)

1. **`RTDB_PUBLIC_URL`** must be set to the public origin of the deployment
   (e.g. `https://rtdb.pardev.net`). It forms the base of every callback URL.
2. **`RTDB_ALLOWED_ORIGINS`** must include every frontend origin that will open
   the login popup — this is checked by the OAuth start endpoint, *independently
   of CORS*. A same-origin dashboard served from `RTDB_PUBLIC_URL` needs that
   origin listed too; a separate SPA needs its own origin. (Adding an origin is
   hot-reloadable via `PATCH /admin/config` — no restart needed for that part.)

## Quick reference

| Provider | Env vars | Callback URL (register with provider) | Scopes par-rt-db requests |
|---|---|---|---|
| GitHub | `RTDB_GITHUB_CLIENT_ID` / `RTDB_GITHUB_CLIENT_SECRET` | `RTDB_PUBLIC_URL` + `/auth/callback` | `read:user user:email` |
| Google | `RTDB_GOOGLE_CLIENT_ID` / `RTDB_GOOGLE_CLIENT_SECRET` | `RTDB_PUBLIC_URL` + `/auth/google/callback` | `openid email profile` |

---

## GitHub

GitHub serves the whole instance with one **OAuth App** (a GitHub App also
works; use an OAuth App unless you have a reason not to).

1. Go to **GitHub → Settings → Developer settings → OAuth Apps → New OAuth App**.
2. Fill in:
   - **Application name:** your choice (e.g. `par-rt-db`).
   - **Homepage URL:** `RTDB_PUBLIC_URL` (e.g. `https://rtdb.pardev.net`).
   - **Authorization callback URL:** `RTDB_PUBLIC_URL` + `/auth/callback`
     (e.g. `https://rtdb.pardev.net/auth/callback`).
3. **Generate a new client secret** and copy it immediately (GitHub only shows
   it once). Note the **Client ID** too.
4. Set `RTDB_GITHUB_CLIENT_ID` and `RTDB_GITHUB_CLIENT_SECRET` (see
   [Applying changes](#applying-changes)).

The consent screen will request `read:user` and `user:email` — these let
par-rt-db read the profile and the verified emails list. par-rt-db never reads
or writes repos.

**GitHub Enterprise:** set `RTDB_GITHUB_BASE_URL` (default
`https://github.com`) and `RTDB_GITHUB_API_URL` (default
`https://api.github.com`) to your instance's user-facing and API roots.

---

## Google

par-rt-db uses Google's standard OIDC authorization-code flow against the fixed
endpoints `accounts.google.com/o/oauth2/v2/auth` → `token` → `/userinfo`.

Google has migrated OAuth setup from the classic *APIs & Services → OAuth
consent screen* to the new **Auth platform** at `console.cloud.google.com/auth`
(the old consent-screen page redirects there). The consent options are now split
across the Auth section's left sidebar — **Audience → Brand information →
Data access → Clients** — all scoped with `?project=par-rt-db`.

1. **Audience** (`/auth/audience`) — set **User type: External** (*Internal*
   only works inside a Google Workspace org, so for a personal account use
   External). This is the consent "who can sign in" gate.
2. **Brand information** (`/auth/branding`) — **App name**, **User support
   email**, and **Developer contact information** (all you). This is what users
   see on the consent screen.
3. **Data access** (`/auth/data-access`) — add the three scopes par-rt-db
   requests (Google lists them under Google APIs as):
   - `.../auth/userinfo.email`
   - `.../auth/userinfo.profile`
   - `openid`
   - These are all **non-sensitive**, so moving the app to *In production*
     does **not** require Google's verification — that only applies if you add
     sensitive/restricted scopes later.
4. **Clients** (`/auth/clients`) → **Create client**:
   - **Application type:** *Web application*.
   - **Authorized redirect URIs:** add `RTDB_PUBLIC_URL` + `/auth/google/callback`
     (e.g. `https://rtdb.pardev.net/auth/google/callback`). This must match
     byte-for-byte, including `https://`.
   - Create, then copy the **Client ID** and **Client secret**.
5. Set `RTDB_GOOGLE_CLIENT_ID` and `RTDB_GOOGLE_CLIENT_SECRET`
   (see [Applying changes](#applying-changes)).

**Publishing status gotcha:** while the app is in *Testing*, only emails you add
under *Test users* (on the Audience page) can complete login. For anyone
(including yourself from any account) to sign in, **Publish app** → *In
production* on the Audience page. Because the scopes are non-sensitive, this is
immediate — no review.

---

## Applying changes

OAuth client IDs/secrets are **boot-time config** (held on the server's `Config`,
not the hot-reloadable `HotConfig`), so unlike `allowed_origins` they take effect
only on a container recreate, **not** via `PATCH /admin/config`.

On the deploy host (`/docker/par-rt-db/.env`, mode 600 — store the secrets in
**parvault**, matching how the GitHub secrets are already kept):

```sh
# edit .env on lenny2, then recreate the server container to inject the new env:
cd /docker/par-rt-db
$EDITOR .env            # set RTDB_GOOGLE_CLIENT_ID / _SECRET
docker compose up -d    # no --build needed for an env-only change
```

Locally / in dev, put the same vars in your `.env` and restart the dev server.

## Verifying

Hit the start endpoint and check the status code — a configured provider
`302`s to the provider; an unconfigured one `503`s; a disallowed origin `403`s:

```sh
# expect 302 (→ Google). 503 = secrets not applied; 403 = origin not allowed.
curl -s -o /dev/null -w "%{http_code}\n" \
  "https://rtdb.pardev.net/auth/google?origin=https://projects.pardev.net"
```

Then do a real end-to-end login from the frontend, and confirm the session with:

```sh
curl -s https://rtdb.pardev.net/auth/me -H "Authorization: Bearer <session-token>" | jq .
```

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `503 {… "oauth not configured"}` | Both `CLIENT_ID` and `CLIENT_SECRET` not set, or the container wasn't recreated after editing `.env`. |
| `403 "origin not allowed"` | The `origin=` you passed to `/auth/{provider}` is not in `RTDB_ALLOWED_ORIGINS`. Add it (hot-reloadable via `PATCH /admin/config`). |
| Provider error page: `redirect_uri_mismatch` | The callback URL registered at the provider doesn't exactly match `RTDB_PUBLIC_URL` + the callback path (see the [quick reference](#quick-reference)). Watch the scheme (`https://`) and trailing slash. |
| `403 "no verified email"` | The account's email isn't verified at the provider. Verify it, or (Google) ensure the account is a *Test user* while the consent screen is in Testing. |
| Google login works only for one account | Consent screen still in *Testing*. **Publish app** → *In production*. |

## Adding a new provider (GitLab, Microsoft, …)

Each new provider is a small, self-contained implementation behind the
`OAuthProvider` trait (`server/src/auth/provider.rs`):

- Implement `OAuthProvider` in a new module under `server/src/auth/`
  (`name`, `from_config` reading two new `Config` fields, `callback_path`,
  `authorize_url`, and `complete_login` doing the code-for-token exchange +
  verified-email fetch + `session::create_session`).
- Add the two `Config` fields (env: `RTDB_<PROVIDER>_CLIENT_ID` /
  `_SECRET`) and the two routes in `auth_routes()`.
- Register an OAuth app with that provider; its callback is
  `RTDB_PUBLIC_URL` + the new `callback_path`.

`FEATURE_MATRIX.md` rates each additional provider **S effort** — the shared
plumbing (state tokens, redirects, HttpOnly cookie sessions, `/auth/me`,
`/auth/logout`, per-Subscribe/Mutate `authorize`, cross-provider email linking)
is provider-agnostic and reused unchanged; only the authorize-URL and
code-exchange dance is provider-specific.
