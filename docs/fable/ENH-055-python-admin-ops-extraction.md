# ENH-055 — Finish the python transport-agnostic admin ops extraction

**Consumer**: `/enhancement-all` (Opus 5 implementer). **Source**: Fable audit 2026-11-13, finding QA-004.

## Goal

Every admin endpoint's request logic exists twice in the python client (sync + async, ~85 methods ×2). Finish the started `_admin_ops_*` extraction: transport-agnostic op functions that build the request and parse the response, with thin sync/async wrapper methods that only supply the transport call.

## Current-state context

- `python-client/src/par_rt_db/admin.py` — 2298 lines: `RtDbAdminClient` (sync) + `AsyncRtDbAdminClient`, method bodies near-identical.
- Partial extraction already exists: `_admin_ops_db.py`, `_admin_ops_auth.py` hold some ops in transport-agnostic form. Study their exact shape first and extend it — do not invent a second pattern.
- `http_client.py` (1048) and `aio_http_client.py` (937) share near-identical structure; `transform_url` is byte-identical (QA-005 hoists it — land QA-005 first or take the hoist with this work, one owner).
- `admin_models.py` pydantic models are shared by both clients already — the response-parsing half of each op is already deduplicated; the duplication is request-building + transport glue.

## Implementation steps

1. **Read the existing pattern**: `_admin_ops_db.py`, `_admin_ops_auth.py`, and how both client classes call into them. Extract the module layout convention (naming, signature shape: what the op function takes — probably `(http, ...)`, returns parsed model).
2. **Inventory the un-extracted ops**: diff the method sets of `RtDbAdminClient` and `AsyncRtDbAdminClient` against what the two `_admin_ops_*` modules already cover. Group the remainder by domain (tokens, purge, workflows, scheduler, metrics, …).
3. **Extract domain by domain**, adding `_admin_ops_<domain>.py` modules that follow the existing convention exactly. Each op function: builds the request (path, params, body) and parses the response into the shared model; takes the transport as a parameter.
4. **Thin the wrappers**: sync and async client methods become one-liners passing their transport. Public signatures and docstrings are unchanged — this is a pure internal refactor; the clients' public API surface must not move.
5. **Dedupe request glue only where truly identical**: method bodies that exist in 4-5 copies (`mint_token`, `ops_recent`, `purge`, `admin_mutate`) collapse through the op functions automatically. Do NOT force-merge genuinely different sync/async behavior — if a pair differs, leave the difference visible in the two wrappers.

## Files to touch

- `python-client/src/par_rt_db/admin.py` (wrappers thin out)
- New `python-client/src/par_rt_db/_admin_ops_<domain>.py` modules
- `python-client/src/par_rt_db/http_client.py` / `aio_http_client.py` only if shared glue is hoisted (coordinate with QA-005's `transform_url` hoist)

## Verify

- `cd python-client && uv run pytest` — green, no test edits needed (public behavior unchanged).
- `cd python-client && uv run mypy` or the project's typecheck leg if configured (check `make checkall`'s python leg).
- Line-count sanity: `admin.py` shrinks materially; `wc -l` before/after in the commit message.
- `make checkall` green.

## Rollback

One atomic commit; revert restores the fat-client shape. No wire or API change, so rollback is risk-free.

## Acceptance criteria (for the card)

- No admin op's request-building logic exists in two hand-maintained copies.
- `uv run pytest` green unchanged; public API surface unchanged (diff of public names empty).
- `make checkall` green.
