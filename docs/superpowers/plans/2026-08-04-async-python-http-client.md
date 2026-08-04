# Async Python HTTP/Admin/Storage Client — Implementation Plan (ENH-012)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `RtDbAsyncHttpClient` — a one-to-one async mirror of `RtDbHttpClient` over `httpx.AsyncClient` — covering the data-plane, storage, and admin control-plane surfaces, under a new `par-rt-db[aio]` extra, bringing the Python client to full async parity.

**Architecture:** A single new module `aio_http_client.py` mirrors `http_client.py` method-for-method. The sync client + its test suite ARE the spec: every method body is byte-identical except `def`→`async def` and `self._send(...)`→`await self._send(...)`. Wire types, DSL builders, and the ~17 response models + 3 `TypeAdapter`s stay defined in `http_client.py`/`wire.py`/etc. and are **re-imported, never redefined**. `httpx` is imported lazily inside `__init__` so the module imports without `[aio]` installed (mirrors both `http_client.py` and `ws_client.py`). The constructor is sync (httpx `AsyncClient` construction is sync); only requests are awaited — same pattern as `ws_client.RtDbClient`.

**Tech Stack:** Python 3.12+, `httpx>=0.27` `AsyncClient` (the `[aio]` extra), Pydantic v2, pytest + pytest-asyncio (`asyncio_mode="auto"`). No new runtime dependencies — `httpx>=0.27` already ships `AsyncClient`, so `aio` is the same pin as `http`.

## Global Constraints

- **Mirror, don't redesign.** The sync `RtDbHttpClient` (`python-client/src/par_rt_db/http_client.py`) is the source of truth. For every mirrored method the ONLY changes are: (1) `def m(...)` → `async def m(...)`; (2) `resp = self._send(...)` → `resp = await self._send(...)`; (3) any direct `self._client.request(...)` → awaited. URLs, JSON body shapes, query params, parsing, return types, and error handling are **byte-identical**. Do not "improve" anything.
- **Re-import shared types — never redefine.** Response models (`MintedToken`, `UploadResult`, `FileMetadata`, `AdminMember`, `TokenInfo`, `TableStat`, `DbStats`, `LatencyStats`, `MetricsSnapshot`, `HotConfig`, `ConfigResponse`, `HotConfigPatch`, `OpEvent`, `CastFailure`, `SampleChange`, `DirectiveReport`, `MigrateResult`) and adapters (`_STEP_RESULT_ADAPTER`, `_SCHEDULES_ADAPTER`, `_BATCH_ADAPTER`) come from `from .http_client import (...)`. Import only what this module references (ruff `F401` flags the rest). Wire/DSL types come from `.wire`, `.query`, `.mutation`, `.migration`, `.schema`, `.errors` unchanged.
- **Single error type:** every failure is `RtDbError(code, message)` from `errors.py`. The async `_send` reuses `RtDbError.from_http(resp.status_code, resp.content)` unchanged. (`errors.retry_on_precondition` is already async — free for callers who want OCC retry.)
- **Lazy `httpx` import.** No `import httpx` at module top. Import it inside `__init__` (raise a friendly `ImportError` naming the `[aio]` extra if missing). A `TYPE_CHECKING` import provides the `httpx.AsyncBaseTransport` / `httpx.Response` annotations. The module must import cleanly with neither `[aio]` nor `[http]` installed.
- **No bare `except`, no `unwrap`/`expect`** outside `#[cfg(test)]`-equivalent (test files). `from __future__ import annotations` at every module top.
- **Async tests need no decorator:** `asyncio_mode = "auto"` (`pyproject.toml:36`) collects `async def test_*` automatically.
- **`httpx.MockTransport` serves `AsyncClient` too** — the existing test harness (`_client`/`_handler_map`) ports almost verbatim. The mock handler stays a plain sync callable.
- **Verification gate (every task):** `make python-client-checkall` (runs `ruff format --check` → `ruff check` → `pyright` → `pytest -q` in `python-client/`). First-time setup: `make python-client-install` (`uv sync --all-extras`).
- **Lint/type floors:** pyright `reportMissingImports="error"`; ruff line-length 100, select `["E","F","I","UP","B","SIM"]`, target `py312`.
- **Commit after every task** (atomic, conventional message). The repo is trunk-based — commit directly on `main`.

## File Structure

- **Create** `python-client/src/par_rt_db/aio_http_client.py` — `RtDbAsyncHttpClient` (one cohesive module mirroring `http_client.py`).
- **Create** `python-client/tests/test_aio_http_client.py` — async tests mirroring `test_http_client.py` (one-to-one port of its ~50 tests).
- **Modify** `python-client/src/par_rt_db/__init__.py` — add `RtDbAsyncHttpClient` to `__all__`, `TYPE_CHECKING`, and the lazy `__getattr__` (:64-78).
- **Modify** `python-client/pyproject.toml` — add `aio = ["httpx>=0.27"]` (:11-13).
- **Modify** `python-client/README.md` — `[aio]` extra install + an async quickstart snippet.
- **Modify** `FEATURE_MATRIX.md` — *only if* a Python-client async/client-completeness row exists; otherwise no change (ENH-012 is beyond the Convex-parity matrix).

---

## Task 1: Core client skeleton + data-plane methods + ported data-plane tests

**Files:**
- Create: `python-client/src/par_rt_db/aio_http_client.py`
- Create: `python-client/tests/test_aio_http_client.py`

**Interfaces:**
- Consumes: `RtDbError` (`.errors`); `Query`, `TableQuery`, `parse_result`, `_terminal_of`, `_dump_query` (`.query`); `StepResult`, `Transaction` (`.mutation`); `ScheduleInfo`, `ScheduleWhen`, `BatchQueryOutcome` (`.wire`); `_STEP_RESULT_ADAPTER`, `_BATCH_ADAPTER`, `_SCHEDULES_ADAPTER` and the response models this task's methods return (`.http_client`).
- Produces: `RtDbAsyncHttpClient` with sync `__init__(url, db, token, *, transport=None)`, `async close()`/`aclose()`, `async __aenter__`/`__aexit__`, `async _send(method, path, **kwargs)`, `_expect_ok`, and the data-plane methods `run` (+ `query` alias), `get`, `find_one_by_index`, `mutate`, `upsert_by_index`, `schedule`, `cancel_schedule`, `pause_schedule`, `resume_schedule`, `list_schedules`, `batch_query`. Produces the async test harness (`_client`, `_handler_map`, constants) used by Tasks 2–3.

- [ ] **Step 1: Create the client module skeleton + data-plane methods**

Create `python-client/src/par_rt_db/aio_http_client.py`. Module header + imports + class shell (the `_send`/lifecycle plumbing is exact; the data-plane methods are produced by applying the mirror transform from Global Constraints to the named sync methods):

```python
"""Async HTTP/admin/storage client for par-rt-db (the ``[aio]`` extra).

A one-to-one async mirror of :class:`par_rt_db.http_client.RtDbHttpClient` over
:class:`httpx.AsyncClient`: every public method is ``async def`` and every
request is ``await``-ed. Wire types, DSL builders, and the response models are
re-imported from :mod:`par_rt_db.http_client` and the shared modules — nothing
is redefined. ``httpx`` is imported lazily inside ``__init__`` so this module
imports without the ``[aio]`` extra installed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .errors import RtDbError
from .mutation import StepResult, Transaction
from .query import Query, TableQuery, _dump_query, _terminal_of, parse_result

if TYPE_CHECKING:
    import httpx

# Re-import the shared response models + adapters this module references.
# Add to this block as later tasks add methods that return more models.
from .http_client import _BATCH_ADAPTER, _SCHEDULES_ADAPTER, _STEP_RESULT_ADAPTER

from .wire import BatchQueryOutcome, ScheduleInfo, ScheduleWhen


class RtDbAsyncHttpClient:
    """Async twin of :class:`RtDbHttpClient`. See module docstring."""

    def __init__(
        self,
        url: str,
        db: str,
        token: str,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        try:
            import httpx
        except ImportError as e:  # pragma: no cover
            raise ImportError(
                "httpx is required for RtDbAsyncHttpClient: "
                "install with `pip install par-rt-db[aio]`"
            ) from e
        self._httpx = httpx
        self._base = url.rstrip("/")
        self._db = db
        self._token = token
        self._client = httpx.AsyncClient(
            base_url=self._base,
            headers={"Authorization": f"Bearer {token}"},
            transport=transport,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def close(self) -> None:
        await self.aclose()

    async def __aenter__(self) -> RtDbAsyncHttpClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def _send(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        resp = await self._client.request(method, path, **kwargs)
        if resp.status_code >= 400:
            raise RtDbError.from_http(resp.status_code, resp.content)
        return resp
```

Then add the data-plane methods by **mirroring** these exact sync methods from `http_client.py`, applying the transform in Global Constraints (copy each body verbatim, change only `def`→`async def` and `self._send(`→`await self._send(`):

- `run` / `query` alias (`http_client.py:314-331`)
- `get` (`:333`)
- `find_one_by_index` (`:337`)
- `mutate` (`:356`)
- `upsert_by_index` (`:377`)
- `schedule` (`:403`)
- `cancel_schedule`, `pause_schedule`, `resume_schedule` (`:418-428`)
- `list_schedules` (`:435`)
- `batch_query` (`:442`)

Preserve the `query = run` class-level alias (`:331`). If any of these methods reference a response model or helper not yet imported (e.g. `parse_result` is already imported), add it to the `from .http_client import (...)` block — ruff `F401`/pyright will tell you if you imported something unused.

- [ ] **Step 2: Write the failing data-plane tests**

Create `python-client/tests/test_aio_http_client.py`. Port the test harness and the construction + data-plane tests from `tests/test_http_client.py`. Harness (ports verbatim — `MockTransport` works with `AsyncClient`):

```python
"""Async tests for ``par_rt_db.aio_http_client.RtDbAsyncHttpClient``.

A one-to-one port of ``tests/test_http_client.py``: same routes, same
assertions, with ``await`` on client calls and ``async with`` around the client.
``httpx.MockTransport`` drives ``httpx.AsyncClient`` the same way it drives the
sync client, so ``_handler_map`` is unchanged.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from par_rt_db import (
    Mutation,
    RtDbError,
    TableQuery,
    t,
)
from par_rt_db.aio_http_client import RtDbAsyncHttpClient
from par_rt_db.http_client import FileMetadata, MintedToken, UploadResult
from par_rt_db.schema import Schema

BEARER = "Bearer machine-token"
ADMIN_BEARER = "Bearer admin-key"
DB = "t<uuid>"

RouteResponse = httpx.Response | Callable[[httpx.Request], httpx.Response]


def _client(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    url: str = "https://rtdb.example",
    db: str = DB,
    token: str = "machine-token",
) -> RtDbAsyncHttpClient:
    """Build an async client whose ``AsyncClient`` uses an in-process ``MockTransport``."""
    return RtDbAsyncHttpClient(url, db, token, transport=httpx.MockTransport(handler))


def _handler_map(
    routes: dict[tuple[str, str, str], RouteResponse],
) -> Callable[[httpx.Request], httpx.Response]:
    """Build a MockTransport handler from a route table (unchanged from sync)."""

    def handler(request: httpx.Request) -> httpx.Response:
        key_path = request.url.path
        for (method, path, body_contains), response in routes.items():
            if request.method != method:
                continue
            if path != key_path:
                continue
            if body_contains and body_contains not in request.content.decode("utf-8", "replace"):
                continue
            if callable(response):
                return response(request)
            return response
        return httpx.Response(404, text=f"no mock for {request.method} {key_path}")

    return handler
```

Then port **every** test in `test_http_client.py` that covers construction/`close`/context-manager behavior and the data-plane methods (`run`/`query`/`get`/`find_one_by_index`/`mutate`/`upsert_by_index`/`schedule`/`cancel_schedule`/`pause_schedule`/`resume_schedule`/`list_schedules`/`batch_query`). For each ported test:
- make the function `async def`;
- wrap the body in `async with _client(handler) as c:` (so the `AsyncClient` is closed — avoids unclosed-client warnings); keep multiple calls on one client inside the same `async with`;
- change `c.method(...)` → `await c.method(...)`;
- keep every assertion, route stub, and captured-request check identical.

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd python-client && uv run pytest tests/test_aio_http_client.py -q`
Expected: FAIL — `ImportError` / `AttributeError` for not-yet-mirrored methods, or assertion failures because methods are missing. (At minimum the harness + any finished data-plane tests must collect.)

- [ ] **Step 4: Complete any missing data-plane methods**

If Step 1 left a data-plane method unimplemented that a ported test exercises, add it (mirror transform). Re-run until the data-plane tests pass.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd python-client && uv run pytest tests/test_aio_http_client.py -q`
Expected: PASS for all construction + data-plane tests.

- [ ] **Step 6: Lint + typecheck**

Run: `cd python-client && uv run ruff check . && uv run ruff format --check . && uv run pyright`
Expected: clean. (Fix any `F401` by pruning unused re-imports.)

- [ ] **Step 7: Commit**

```bash
git add python-client/src/par_rt_db/aio_http_client.py python-client/tests/test_aio_http_client.py
git commit -m "feat(python-client): async HTTP client skeleton + data-plane (ENH-012)"
```

---

## Task 2: Storage methods + ported storage tests

**Files:**
- Modify: `python-client/src/par_rt_db/aio_http_client.py`
- Modify: `python-client/tests/test_aio_http_client.py`

**Interfaces:**
- Consumes: `UploadResult`, `FileMetadata` (`.http_client`); the Task-1 harness.
- Produces: `upload`, `delete_file`, `get_file_metadata`, `get_url`.

- [ ] **Step 1: Mirror the storage methods**

Add to `RtDbAsyncHttpClient` by mirroring these sync methods (`http_client.py`), same transform as Task 1:
- `upload` (`:463`) — note the raw-body `content=data` and conditional `Content-Type` header; keep them byte-identical, only `await self._send(...)`.
- `delete_file` (`:480`)
- `get_file_metadata` (`:486`)
- `get_url` (`:491`) — pure URL builder, no request → stays a plain sync method (do **not** make it async; mirror exactly).

Add `UploadResult`, `FileMetadata` to the `from .http_client import (...)` block.

- [ ] **Step 2: Port the storage tests**

Port every storage test from `test_http_client.py` (upload raw body + content-type, delete, metadata, public-URL builder) into `test_aio_http_client.py` with the same async transform (`async with`, `await`). `get_url` has no `await` — its test calls it directly inside the `async with` block.

- [ ] **Step 3: Run tests to verify they pass**

Run: `cd python-client && uv run pytest tests/test_aio_http_client.py -q`
Expected: PASS (construction + data-plane + storage).

- [ ] **Step 4: Lint + typecheck**

Run: `cd python-client && uv run ruff check . && uv run ruff format --check . && uv run pyright`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add python-client/src/par_rt_db/aio_http_client.py python-client/tests/test_aio_http_client.py
git commit -m "feat(python-client): async storage surface — upload/delete/metadata/url (ENH-012)"
```

---

## Task 3: Admin control-plane methods + ported admin tests

**Files:**
- Modify: `python-client/src/par_rt_db/aio_http_client.py`
- Modify: `python-client/tests/test_aio_http_client.py`

**Interfaces:**
- Consumes: the response models these methods return (`MintedToken`, `AdminMember`, `TokenInfo`, `DbStats`, `MetricsSnapshot`, `ConfigResponse`, `HotConfigPatch`, `OpEvent`, `MigrateResult`, etc. from `.http_client`); `Directive` / `MigrateRequest` (`.migration`); `SchemaDef` (`.schema`); the Task-1 harness.
- Produces: all admin methods listed below.

- [ ] **Step 1: Mirror the admin methods**

Add to `RtDbAsyncHttpClient` by mirroring each sync admin method (`http_client.py`), same transform. Source line refs:
- `create_db` (`:497`), `delete_db` (`:502`), `push_schema` (`:515`), `list_dbs` (`:524`)
- `mint_token` (`:529`), `revoke_token` (`:534`)
- `export_db` (`:539`), `import_db` (`:543`)
- `allowlist_add` (`:554`), `allowlist_remove` (`:563`), `allowlist_list` (`:572`)
- `admins_list` (`:577`), `admins_add` (`:582`), `admins_remove` (`:594`)
- `list_tokens` (`:603`), `get_schema` (`:608`), `db_stats` (`:613`)
- `metrics` (`:618`), `get_config` (`:623`), `patch_config` (`:628`), `ops_recent` (`:644`)
- `admin_query` (`:668`), `admin_mutate` (`:681`), `migrate_schema` (`:701`)
- `backup_now` (`:734`), `list_backups` (`:744`), `download_backup` (`:754`), `delete_backup` (`:764`), `restore_backup` (`:772`)

Add every response model these reference to the `from .http_client import (...)` block; add `Directive`/`MigrateRequest` (`.migration`) and `SchemaDef` (`.schema`) imports as needed. Prune unused imports (ruff `F401`).

- [ ] **Step 2: Port the admin tests**

Port every admin test from `test_http_client.py` (create/delete db, push/get schema, mint/revoke/list tokens, export/import, allowlist CRUD, admins CRUD, stats/metrics/config/ops, admin query/mutate, migrate, backup lifecycle) with the same async transform.

- [ ] **Step 3: Run tests to verify they pass**

Run: `cd python-client && uv run pytest tests/test_aio_http_client.py -q`
Expected: PASS — the full ported suite (construction + data-plane + storage + admin) is green. This is the parity proof: the async client passes the same assertions as the sync client.

- [ ] **Step 4: Lint + typecheck**

Run: `cd python-client && uv run ruff check . && uv run ruff format --check . && uv run pyright`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add python-client/src/par_rt_db/aio_http_client.py python-client/tests/test_aio_http_client.py
git commit -m "feat(python-client): async admin control-plane — full method mirror (ENH-012)"
```

---

## Task 4: Public export, `[aio]` extra, docs, parity check, full gate

**Files:**
- Modify: `python-client/src/par_rt_db/__init__.py`
- Modify: `python-client/pyproject.toml`
- Modify: `python-client/README.md`
- Modify: `FEATURE_MATRIX.md` (conditional)

**Interfaces:**
- Consumes: the finished `RtDbAsyncHttpClient` from Task 3.

- [ ] **Step 1: Register the `[aio]` extra**

In `python-client/pyproject.toml`, extend the optional-dependencies block (`:11-13`):
```toml
[project.optional-dependencies]
ws = ["websockets>=13"]
http = ["httpx>=0.27"]
aio = ["httpx>=0.27"]        # async HTTP/admin/storage client (ENH-012)
```

- [ ] **Step 2: Export the async client lazily**

In `python-client/src/par_rt_db/__init__.py`:
- add `RtDbAsyncHttpClient` to the `TYPE_CHECKING` block (`:37-39`): `from .aio_http_client import RtDbAsyncHttpClient`;
- add `"RtDbAsyncHttpClient",` to `__all__` (`:41-61`);
- add a branch to `__getattr__` (`:64-78`):
```python
    if name == "RtDbAsyncHttpClient":
        from . import aio_http_client

        return aio_http_client.RtDbAsyncHttpClient
```
- update the module/`__getattr__` docstrings to mention the `[aio]` extra alongside `[http]`/`[ws]`.

- [ ] **Step 3: Document the `[aio]` extra**

In `python-client/README.md`:
- add `par-rt-db[aio]` to the extras/install snippets (alongside `[http]` and `[ws]`);
- add an async quickstart:
```python
import asyncio
from par_rt_db import Mutation, TableQuery
from par_rt_db import RtDbAsyncHttpClient

async def main() -> None:
    async with RtDbAsyncHttpClient("https://rtdb.pardev.net", "mydb", "<token>") as c:
        rows = await c.run(TableQuery("items").collect())
        await c.mutate(Mutation().insert("items", {"_id": "i1", "n": 1}).build())

asyncio.run(main())
```

- [ ] **Step 4: Parity-matrix check**

`grep -n -i "async\|aio\|python" FEATURE_MATRIX.md`. If there is a Python-client row tracking async/client-completeness, flip it ❌→✅ with a note "async HTTP/admin/storage via `par-rt-db[aio]`". If no such row exists (ENH-012 is beyond the Convex-parity matrix), make no change and note that.

- [ ] **Step 5: Run the full Python gate**

Run: `cd python-client && make checkall` (equivalently `make python-client-checkall` from the repo root).
Expected: clean — `ruff format --check` + `ruff check` + `pyright` + `pytest -q` all green.

- [ ] **Step 6: Commit**

```bash
git add python-client/src/par_rt_db/__init__.py python-client/pyproject.toml python-client/README.md FEATURE_MATRIX.md
git commit -m "feat(python-client): export async client + [aio] extra + docs (ENH-012)"
```

---

## Self-Review

- **Spec coverage:** ENH-012 (ENHANCEMENTS.md:40) asks for "an async variant (`par-rt-db[aio]` over `httpx.AsyncClient`) mirroring the sync method set" for HTTP/admin/storage. Task 1 covers construction + data-plane; Task 2 storage; Task 3 admin (the full sync method set is mirrored — see line refs); Task 4 the extra + export + docs. `retry_on_precondition` already being async is noted (callers get OCC retry free, no task needed). ✓
- **Placeholder scan:** none — Task 1 gives exact skeleton + harness code and an exact mirror transform; Tasks 2–3 reference exact sync source line ranges and restate the transform; Task 4 gives exact `pyproject`/`__init__`/README edits. No "TBD"/"add error handling"/"similar to Task N". ✓
- **Type consistency:** `RtDbAsyncHttpClient` named consistently across all tasks; `_send`/`_client`/`aclose`/`close` names match; `query = run` alias preserved; re-import block extended (not redefined) per task; async test harness (`_client`/`_handler_map`) defined once in Task 1 and reused by Tasks 2–3. `get_url` correctly stays sync (no request). ✓
- **Mirror-fidelity risk:** the only judgment the implementer exercises is selecting which `test_http_client.py` tests belong to each surface — mitigated by naming the method groups per task. The transform rule is mechanical and the sync bodies are copied verbatim. ✓
