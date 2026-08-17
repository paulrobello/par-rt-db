# OAuth Provider Setup

How to register OAuth apps and wire them into par-rt-db. Each provider is
**independently optional**: leave its env vars blank and the provider is disabled
(its `/auth/...` routes return `503`); machine tokens and the admin key keep
working with zero providers configured.

> **Source of truth:** the provider implementations in `server/src/auth/`
> (`provider.rs` for the shared trait/plumbing, then `github.rs`, `google.rs`,
> `gitlab.rs`, `microsoft.rs`, `apple.rs`, `oidc.rs`). This guide mirrors those
> files — if the code and this doc disagree, the code wins; fix this doc.

## Table of contents

- [How it works](#how-it-works)
- [Prerequisites (all providers)](#prerequisites-all-providers)
- [Quick reference](#quick-reference)
- [GitHub](#github)
- [Google](#google)
- [GitLab](#gitlab)
- [OIDC (generic provider)](#oidc-generic-provider)
- [Microsoft (Entra ID / Azure AD v2.0)](#microsoft-entra-id--azure-ad-v20)
- [Sign in with Apple](#sign-in-with-apple)
- [Applying changes](#applying-changes)
- [Verifying](#verifying)
- [Hardening](#hardening)
- [Troubleshooting](#troubleshooting)
- [Adding a new provider (e.g. a future IdP)](#adding-a-new-provider-eg-a-future-idp)

## How it works

- The browser starts a login at `GET /auth/{provider}/begin?origin=<app-origin>`.
  The `origin` is validated against the **live** `RTDB_ALLOWED_ORIGINS` (the same
  list hot-reloadable via `PATCH /admin/config`); an origin not on the list is
  `403`. The server mints a single-use `state` token, sets a `SameSite=None;HttpOnly`
  `rtdb-oauth-csrf` nonce cookie (Login-CSRF defense — see [Hardening](#hardening)),
  and returns the provider authorize URL + the state as JSON
  (`{ authorizeUrl, state }`). The caller opens `authorizeUrl` in a `noopener`
  popup and polls `GET /auth/state?state=…` for completion. If the caller holds
  an anonymous session, `/begin` resolves it server-side so the callback merges
  the anonymous footprint into the real account (anonymous sign-in and the
  anon→real merge are documented on the
  [`POST /auth/anonymous` endpoint](../README.md#endpoints)).
- The provider redirects back to the callback (`/auth/callback` for GitHub,
  `/auth/{provider}/callback` for Google/GitLab/Microsoft/OIDC) with
  `?code=&state=` (Apple is the exception — it POSTs `code` + `state` as a form
  body to a dedicated `POST /auth/apple/callback`; see
  [Sign in with Apple](#sign-in-with-apple)). The server constant-time-verifies
  the CSRF nonce cookie against `state`, claims
  the pending entry (a replay rejects with `400`), exchanges the code for an
  access token, fetches the user's **verified** email, upserts `rtdb_auth.users`,
  and returns popup-closing HTML. The session token is delivered twice — via the
  HttpOnly session cookie **and** via the one-shot `/auth/state` poll. The
  `state` token (not the cookie) is the poll capability, which is what makes
  cross-origin SDK login work where the `SameSite=Lax` session cookie would not
  be sent.
- Identity is **email-keyed with cross-provider linking**: a user who first
  logs in via GitHub and later via Google (same verified email) resolves to the
  same account. Enabling more providers is additive, not fragmenting.
- All providers **require a verified email**. Google requires `email_verified`;
  GitHub picks the primary verified address (falling back to any verified one)
  and never trusts the unverified profile-level email; GitLab requires a
  confirmed email (`confirmed_at` set on `/api/v4/user`). The generic OIDC
  provider trusts the configured IdP's assertion and rejects only when userinfo
  explicitly says `email_verified: false` (many IdPs omit the field).
- A provider is "configured" only when its required env vars are all non-empty
  (`from_config` returns `None` otherwise) — the two credentials for
  GitHub/Google/GitLab, `RTDB_MICROSOFT_CLIENT_ID`/`_SECRET` for Microsoft
  (`_TENANT` defaults to `common`), all four `RTDB_APPLE_*` fields for Apple,
  and all five `RTDB_OIDC_*` fields for the generic OIDC provider.

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
| GitLab | `RTDB_GITLAB_CLIENT_ID` / `RTDB_GITLAB_CLIENT_SECRET` | `RTDB_PUBLIC_URL` + `/auth/gitlab/callback` | `read_user email` |
| Microsoft | `RTDB_MICROSOFT_CLIENT_ID` / `RTDB_MICROSOFT_CLIENT_SECRET` (`_TENANT` optional, default `common`) | `RTDB_PUBLIC_URL` + `/auth/microsoft/callback` | `openid email profile` |
| Apple | `RTDB_APPLE_CLIENT_ID` / `_TEAM_ID` / `_KEY_ID` / `_PRIVATE_KEY` | `RTDB_PUBLIC_URL` + `/auth/apple/callback` (Apple POSTs via `response_mode=form_post`) | `name email` |
| OIDC (generic) | `RTDB_OIDC_CLIENT_ID` / `_SECRET` / `_AUTHORIZE_URL` / `_TOKEN_URL` / `_USERINFO_URL` | `RTDB_PUBLIC_URL` + `/auth/oidc/callback` | `openid email profile` |

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
   email**, and **Developer contact information** (all you). For the required
   **Privacy link**, the server serves its own policy at `RTDB_PUBLIC_URL` +
   `/privacy` (e.g. `https://rtdb.pardev.net/privacy`) — use that, or any
   reachable HTTPS policy page.
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

## GitLab

par-rt-db uses GitLab's standard authorization-code flow: `/oauth/authorize` →
`/oauth/token` → `/api/v4/user`. It works against gitlab.com or any self-hosted
GitLab.

One application serves the whole instance (create it under the account that owns
the deploy, or a dedicated service account):

1. In GitLab go to **User settings → Applications** (or **Admin Area →
   Applications** for an instance-wide app on a self-hosted server):
   `https://gitlab.com/-/user_settings/applications`.
2. Fill in:
   - **Name:** your choice (e.g. `par-rt-db`).
   - **Redirect URI:** `RTDB_PUBLIC_URL` + `/auth/gitlab/callback`
     (e.g. `https://rtdb.pardev.net/auth/gitlab/callback`). Must match
     byte-for-byte, including `https://`.
   - **Confidential:** leave checked (the secret stays server-side).
   - **Scopes:** select **`read_user`** and **`email`** — the `email` scope is
     required for `/api/v4/user` to return the primary email address.
3. **Save application**, then copy the **Application ID** (client id) and
   **Secret**. GitLab shows the secret only once.
4. Set `RTDB_GITLAB_CLIENT_ID` and `RTDB_GITLAB_CLIENT_SECRET`
   (see [Applying changes](#applying-changes)).

GitLab's `/api/v4/user` exposes `confirmed_at` (a timestamp when the address is
confirmed, `null` otherwise). par-rt-db admits only users whose email is
confirmed — an unconfirmed GitLab email is rejected with
`403 "no verified email"`, so have users confirm their address first.

**Self-hosted GitLab:** set `RTDB_GITLAB_BASE_URL` (default `https://gitlab.com`)
to your instance root, e.g. `https://gitlab.example.com`. par-rt-db then uses
`{base_url}/oauth/authorize`, `/oauth/token`, and `/api/v4/user`.

---

## OIDC (generic provider)

par-rt-db ships one generic OpenID Connect provider that serves any
standards-compliant IdP — Azure AD, Keycloak, Auth0, Okta, self-hosted, etc.
Unlike the per-IdP modules above, the authorize/token/userinfo endpoints are
not hardcoded: the operator supplies them from their IdP's
`/.well-known/openid-configuration`. (The `OAuthProvider` trait's `authorize_url`
is sync, so it cannot do live OIDC discovery at request time — endpoints are
configuration, not discovered per login.)

The provider is **active only when all five** `RTDB_OIDC_*` vars are set; with
any one blank, the `/auth/oidc/*` routes return `503`, exactly like an
unconfigured google/gitlab.

1. Fetch your IdP's `/.well-known/openid-configuration` and note the
   `authorization_endpoint`, `token_endpoint`, and `userinfo_endpoint` URLs.
2. Register a confidential OAuth/OIDC client at your IdP with the redirect URI
   `RTDB_PUBLIC_URL` + `/auth/oidc/callback`
   (e.g. `https://rtdb.pardev.net/auth/oidc/callback`). This must match
   byte-for-byte, including `https://`.
3. Set all five env vars — `RTDB_OIDC_CLIENT_ID`, `RTDB_OIDC_CLIENT_SECRET`,
   `RTDB_OIDC_AUTHORIZE_URL`, `RTDB_OIDC_TOKEN_URL`, `RTDB_OIDC_USERINFO_URL`
   (see [Applying changes](#applying-changes)).

par-rt-db requests the `openid email profile` scopes. A present email is
required; the IdP must positively assert `email_verified: true` (boolean or
the string `"true"`) — a missing `email_verified` is rejected as unverified
(SEC-122). If your IdP genuinely verifies mail but omits the claim, patch its
userinfo endpoint to emit it rather than relaxing this gate.

---

## Microsoft (Entra ID / Azure AD v2.0)

This is OIDC against Microsoft's well-known endpoints (derived from
`RTDB_MICROSOFT_TENANT`), so unlike the generic OIDC provider you supply
**credentials + tenant only** — no four-URL paste. Works for work/school
(Entra ID) and personal (MSA) accounts.

1. In the [Microsoft Entra admin center](https://entra.microsoft.com/) go to
   **Applications → App registrations → New registration**.
2. **Supported account types** chooses the audience — and sets what you'll use
   for `RTDB_MICROSOFT_TENANT`:
   - *Accounts in this organizational directory only* → a single Entra tenant
     (work/school only); use that directory's **Directory (tenant) ID**.
   - *Accounts in any organizational directory* → `organizations`.
   - *Accounts in any Microsoft Entra ID or personal Microsoft accounts* →
     `common` (the default par-rt-db uses when `_TENANT` is unset).
   - Note the **Application (client) ID** (`RTDB_MICROSOFT_CLIENT_ID`).
3. **Authentication → Add a platform → Web** — *not* Single-page app or
   Mobile/desktop. par-rt-db is a **confidential client**: it holds
   `CLIENT_SECRET` and performs the code-for-token exchange server-side, which
   is the *Web* platform. SPA and Public-client platforms are for secret-less
   browser/native flows and do not match this server's design.
   - **Redirect URI:** `RTDB_PUBLIC_URL` + `/auth/microsoft/callback`
     (e.g. `https://rtdb.pardev.net/auth/microsoft/callback`). Must match
     byte-for-byte, including `https://`.
4. **Certificates & secrets → New client secret** → copy the secret **Value**
   (not the Secret ID). The Value is hidden once you leave the page.
5. Set the env vars (see [Applying changes](#applying-changes)):
   - `RTDB_MICROSOFT_CLIENT_ID` — the application (client) id.
   - `RTDB_MICROSOFT_CLIENT_SECRET` — the secret **value**.
   - `RTDB_MICROSOFT_TENANT` — **set this to your specific tenant GUID.**
     The default `common` accepts any Entra tenant and is unsafe: a tenant
     admin can set any user's `mail` attribute to any address, and Microsoft
     emits that address as the `email` claim. With `common`, an attacker who
     controls an Entra tenant can spoof a victim's email and (without the
     SEC-102 identity fix) adopt their account. Pinning a single tenant GUID
     restricts sign-in to that one organization so only your own admins can
     set mail attributes. `organizations`/`consumers` remain multi-tenant and
     carry the same caveat — use a GUID unless you have a specific reason.

The authorize URL uses `response_mode=query`, so the standard GET callback
handles the redirect (no `form_post`, unlike Apple).

**Identity & email sourcing (SEC-102 — read this if a Microsoft login behaves
unexpectedly):** Microsoft identity is keyed on the immutable `sub`+`tid` pair
from a **signature-verified id_token**, NOT on the `email` claim. The id_token
is verified against Microsoft's published JWKS for the issuing tenant
(cached for one hour; a JWKS fetch failure rejects the login — fail-closed),
and `iss`/`aud`/`exp`/`tid` are all validated. The `email` claim is used for
the contact address and for cross-provider account linking **only when
`xms_edov == true`** (Microsoft's "email domain owner verified" signal — set
when the domain was validated via a DNS TXT record). When `xms_edov` is
absent (common for tenant-admin-set mail), the tenant-constrained UPN
(`preferred_username`, e.g. `you@org.com`) is used as the contact address
instead, and the spoofable `email` claim is ignored. This is the "nOAuth"
defense: a tenant admin can set a victim's address as a user's `mail`
attribute, but they cannot forge the victim's `sub` — so the spoofed email
creates a fresh account instead of adopting the victim's row. If your IdP
omits both `xms_edov` and `preferred_username` the login fails with
`403 "no verified email"`; the fix is to emit one of them (the UPN is
sufficient).

## Sign in with Apple

Sign in with Apple requires a paid Apple Developer account. You create a
**Services ID** (not an App ID) plus a **Sign in with Apple key**; par-rt-db
derives its four config pieces from them.

1. First create an **App ID** (if you don't have one): **Certificates,
   Identifiers & Profiles → Identifiers → + → App IDs**, enable **Sign In with
   Apple** (this is the *Primary App ID* the Services ID binds to).
2. Create the **Services ID**: **Identifiers → + → Services ID**, give it an
   identifier string (e.g. `com.example.rtdb`) — this is your
   `RTDB_APPLE_CLIENT_ID`. Enable **Sign In with Apple → Configure**:
   - **Primary App ID:** the App ID from step 1.
   - **Domains:** the host of `RTDB_PUBLIC_URL` (e.g. `rtdb.pardev.net`).
   - **Return URLs:** `RTDB_PUBLIC_URL` + `/auth/apple/callback`
     (e.g. `https://rtdb.pardev.net/auth/apple/callback`). Must match
     byte-for-byte. Apple does **not** allow `http`/`localhost` in production
     (Sandbox allows `localhost` for testing).
3. Note your **Team ID** (10 chars, top-right of the portal) →
   `RTDB_APPLE_TEAM_ID`.
4. Create the **key**: **Keys → + → Sign in with Apple → Configure** (select the
   Primary App ID from step 1) → **Register**. Note the **Key ID**
   (`RTDB_APPLE_KEY_ID`) and **download the private key once** (`.p8` PEM — you
   cannot re-download it) → `RTDB_APPLE_PRIVATE_KEY`.
5. Apply the four env vars (see [Applying changes](#applying-changes)).

Two protocol-mandated differences from the other providers:

1. **No static client secret.** Apple requires the `client_secret` sent to its
   token endpoint to be a short-lived **ES256 JWT** signed with the EC private
   key you register with Apple. par-rt-db generates and signs it per exchange
   from four config pieces, so you configure a *key*, not a password:
   - `RTDB_APPLE_CLIENT_ID` — the **Services ID** (not an App ID).
   - `RTDB_APPLE_TEAM_ID` — your 10-character Apple Developer team id.
   - `RTDB_APPLE_KEY_ID` — the key id of the Sign in with Apple key you created.
   - `RTDB_APPLE_PRIVATE_KEY` — the PEM you download with that key. Env stores
     that can't carry real newlines (most of them) accept `\n`-escaped PEMs;
     par-rt-db unescapes them.

2. **`response_mode=form_post`.** Apple POSTs `code` + `state` to the redirect
   URI as a form body rather than query params, so par-rt-db serves Apple's
   callback with a dedicated `POST /auth/apple/callback` (the `rtdb-oauth-csrf`
   nonce cookie is `SameSite=None`, so it survives the cross-site POST).

Identity keys on Apple's stable **`sub`** (not email): Apple may relay the email
through `@privaterelay.appleid.com` and rotate it if a user re-hides their
address. par-rt-db stores `apple_sub` (mirroring `github_id`) and links to an
existing account only when the Apple-reported email matches; a hidden-email
user whose real address exists under another provider gets a separate account
(the opaque relay address can't be reliably matched). Both real and relay
emails are accepted.

Register the redirect URI exactly — `$RTDB_PUBLIC_URL/auth/apple/callback` —
under your Services ID's "Return URLs". Apple does not allow `http`/`localhost`
redirects in production (Sandbox allows `localhost` for testing).

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

Hit the begin endpoint and check the status code — a configured provider
returns `200` with the JSON body `{ authorizeUrl, state }`; an unconfigured one
`503`s; a disallowed origin `403`s:

```sh
# expect 200 (JSON body with authorizeUrl + state). 503 = secrets not applied; 403 = origin not allowed.
curl -s -o /dev/null -w "%{http_code}\n" \
  "https://rtdb.pardev.net/auth/google/begin?origin=https://projects.pardev.net"
```

Then do a real end-to-end login from the frontend, and confirm the session with:

```sh
curl -s https://rtdb.pardev.net/auth/me -H "Authorization: Bearer <session-token>" | jq .
```

## Hardening

Two security properties of the login flow are worth knowing when you operate
or integrate with par-rt-db:

- **Login-CSRF (double-submit nonce).** `/auth/{provider}/begin` sets a
  `SameSite=None;HttpOnly` cookie named `rtdb-oauth-csrf` whose value is the
  minted `state` token; `/auth/{provider}/callback` rejects (400) any callback
  whose cookie does not constant-time-equal the `state` query param. This binds
  a callback to the browser that started the login, so an attacker can't induce a
  victim into completing the attacker's own exchange. `SameSite=None` is
  required so the cookie survives the provider → callback cross-site redirect;
  cross-origin SDK consumers must therefore send `credentials: "include"` on the
  `/begin` fetch (the same-origin dashboard does this by default). Disable as
  break-glass via `RTDB_OAUTH_LOGIN_CSRF=false` (default `true`).
- **Reverse-tabnabbing + cross-origin poll.** The popup opens with
  `noopener,noreferrer`, so the authorize page has no `window.opener` handle.
  Completion is relayed by the **parent** polling `GET /auth/state?state=` keyed
  on the single-use state token — not by `window.opener.postMessage`. The state
  token, not the session cookie, is the poll capability; this is what makes
  cross-origin SDK login work where the `SameSite=Lax` session cookie would not
  be sent on the poll request.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `503 {… "oauth not configured"}` | `CLIENT_ID`/`CLIENT_SECRET` not set in the **container's** env. Three causes: blank in `.env`, the container wasn't recreated after editing `.env`, or `docker-compose.yml` doesn't pass that provider's vars into the server `environment:` block (so they sit in `.env` unused — the bug that blocked Google on first enable). |
| `403 "origin not allowed"` | The `origin=` you passed to `/auth/{provider}/begin` is not in `RTDB_ALLOWED_ORIGINS`. Add it (hot-reloadable via `PATCH /admin/config`). |
| Provider error page: `redirect_uri_mismatch` | The callback URL registered at the provider doesn't exactly match `RTDB_PUBLIC_URL` + the callback path (see the [quick reference](#quick-reference)). Watch the scheme (`https://`) and trailing slash. |
| `403 "no verified email"` | The account's email isn't verified at the provider. Verify it, or (Google) ensure the account is a *Test user* while the consent screen is in Testing. |
| Google login works only for one account | Consent screen still in *Testing*. **Publish app** → *In production*. |

## Adding a new provider (e.g. a future IdP)

Each new provider is a small, self-contained implementation behind the
`OAuthProvider` trait (`server/src/auth/provider.rs`):

- Implement `OAuthProvider` in a new module under `server/src/auth/`
  (`name`, `from_config` reading the provider's `Config` fields, `callback_path`,
  `authorize_url`, and `complete_login` doing the code-for-token exchange +
  verified-email fetch + `session::create_session`). `gitlab.rs` is a worked
  example for a hosted IdP with an overridable base URL; `oidc.rs` is the most
  recent and shows the fully config-supplied-endpoint case.
- Add the `Config` fields — at minimum `RTDB_<PROVIDER>_CLIENT_ID` /
  `_SECRET`, plus a base/API URL field when the provider is self-hostable
  (`gitlab.rs`/`github.rs`) — and the provider's route pair (`/auth/<provider>/begin`
  + `/auth/<provider>/callback`) in `auth_routes()`.
- **Wire the env vars into the deploy** — add each new var to the server
  service's `environment:` block in `docker-compose.yml` (mirroring the
  GitHub/Google/OIDC entries) and to `.env.example`. Without the compose line the
  vars sit in `.env` unused: the container never sees them, `from_config` returns
  `None`, and the provider's routes return `503` even after a container
  recreate.
- Register an OAuth app with that provider; its callback is
  `RTDB_PUBLIC_URL` + the new `callback_path`.

`FEATURE_MATRIX.md` rates each additional provider **S effort** — the shared
plumbing (state tokens, the `rtdb-oauth-csrf` nonce, `/auth/state` polling,
HttpOnly cookie sessions, `/auth/me`, `/auth/logout`, per-Subscribe/Mutate
`authorize`, cross-provider email linking) is provider-agnostic and reused
unchanged; only the authorize-URL and code-exchange dance is provider-specific.
