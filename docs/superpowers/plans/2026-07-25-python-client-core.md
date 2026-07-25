# par-rt-db Python Client — Core (wire + DSL) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the contract core of the `par-rt-db` Python client — Pydantic v2 wire types (fourth implementation of the wire contract), the schema/query/mutation DSL, cursor codec, and error types — fully tested with no network.

**Architecture:** A `uv`-managed `src/`-layout package `par_rt_db` whose models serialize byte-identically to `server/src/protocol.rs` + `ts-client/src/protocol.ts` + `rust-client/src/wire.rs`. Correctness is anchored by **four-way round-trip parity tests** (fixtures sourced from the server's own `protocol.rs` tests). No HTTP/WS/admin/in-memory in this plan — those are Plan 2.

**Tech Stack:** Python ≥3.12, Pydantic v2, `uv`, `ruff`, `pyright`, `pytest` + `pytest-asyncio`.

## Global Constraints

- `requires-python = ">=3.12"`; target/test floor CPython 3.12.
- `uv` for env/dep management (`uv sync`, `uv run ...`).
- Toolchain: `ruff format`, `ruff check`, `pyright`, `pytest` (per `~/.claude/guides/python.md`).
- Type annotations required; `list[str]`/`X | None` built-in generics; absolute imports; Google docstrings; 10s default timeout where network is added (not in this plan).
- Wire bytes must match `server/src/protocol.rs` exactly: discriminator `"type"`, camelCase field aliases on Message/Schema/AuthedUser/Schedule* types, `extra="forbid"` everywhere (= `deny_unknown_fields`), `int64`/`Id` are wire **strings**.
- Omit-when-`None` fields (server uses `skip_serializing_if`): `Mutate.idempotencyKey`, `AuthedUser.githubLogin`/`githubId`, `ScheduleInfo.cron`/`lastError`, `ScheduleAck.error`. All other `None`-valued optional fields serialize as JSON `null` (do NOT blanket `exclude_none`).
- No `# type: ignore` without a justifying comment; zero `ruff check` errors; `pyright` clean.

## File Structure

```text
python-client/
  pyproject.toml                          # Task 1
  .gitignore                              # Task 1
  .pre-commit-config.yaml                 # Task 1
  Makefile                                # Task 1 (extend in Task 11)
  README.md                               # Task 1 (stub; expanded Plan 2)
  src/par_rt_db/
    __init__.py                           # Task 1 (empty re-exports; filled as modules land)
    errors.py                             # Task 2
    wire.py                               # Tasks 3, 4, 5, 6
    cursor.py                             # Task 7
    schema.py                             # Task 8
    query.py                              # Task 9
    mutation.py                           # Task 10
  tests/
    conftest.py                           # Task 6 (fixtures)
    test_errors.py                        # Task 2
    test_wire.py                          # Tasks 3-6
    test_wire_parity.py                   # Task 6 (four-way gate)
    test_cursor.py                        # Task 7
    test_schema.py                        # Task 8
    test_query.py                         # Task 9
    test_mutation.py                      # Task 10
```

---

### Task 1: Package scaffold

**Files:**
- Create: `python-client/pyproject.toml`, `python-client/.gitignore`, `python-client/.pre-commit-config.yaml`, `python-client/Makefile`, `python-client/README.md`, `python-client/src/par_rt_db/__init__.py`, `python-client/tests/__init__.py`

**Interfaces:**
- Produces: an importable empty package `par_rt_db` and a working `uv` project; `make checkall` (package-local) runs ruff + pyright + pytest.

- [ ] **Step 1: Write `pyproject.toml`**

```toml
[project]
name = "par-rt-db"
version = "0.1.0"
description = "Python client for par-rt-db (self-hosted realtime document database)"
readme = "README.md"
requires-python = ">=3.12"
license = {text = "MIT"}
authors = [{name = "Paul Robello", email = "probello@gmail.com"}]
dependencies = ["pydantic>=2.7"]

[project.optional-dependencies]
ws = ["websockets>=13"]      # added in Plan 2
http = ["httpx>=0.27"]       # added in Plan 2 (http_client + admin)
dev = ["pytest>=8", "pytest-asyncio>=0.23", "ruff>=0.5", "pyright>=1.1.350"]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/par_rt_db"]

[tool.ruff]
line-length = 100
target-version = "py312"
src = ["src", "tests"]

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]

[tool.pyright]
include = ["src", "tests"]
venvPath = "."
venv = ".venv"
reportMissingImports = "error"
```

- [ ] **Step 2: Write `.gitignore`, `__init__.py` files, `README.md` stub**

`.gitignore`:
```text
.venv/
__pycache__/
*.pyc
.pytest_cache/
.ruff_cache/
dist/
build/
*.egg-info/
```

`src/par_rt_db/__init__.py`:
```python
"""par-rt-db Python client (core: wire + DSL). Clients land in a later plan."""
```

`tests/__init__.py`: (empty file)

`README.md`:
```markdown
# par-rt-db Python client

Python SDK for [par-rt-db](..). Core wire + DSL layer. HTTP/WS/admin clients are
added in a follow-on plan. See `docs/superpowers/specs/2026-07-25-python-client-design.md`.
```

- [ ] **Step 3: Write `Makefile`**

```makefile
.PHONY: install test lint fmt typecheck checkall pre-commit

install:
	uv sync --extra dev

test:
	uv run pytest -q

lint:
	uv run ruff check .

fmt:
	uv run ruff format .

typecheck:
	uv run pyright

checkall: fmt lint typecheck test

pre-commit:
	uv run pre-commit run --all-files
```

- [ ] **Step 4: Write `.pre-commit-config.yaml`** (adapt `~/Repos/parllama/.pre-commit-config.yaml`; standard hooks + ruff + pyright + secret scan):

```yaml
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.6.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-merge-conflict
      - id: check-added-large-files
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.5.7
    hooks:
      - id: ruff
        args: [--fix]
      - id: ruff-format
  - repo: https://github.com/gitleaks/gitleaks
    rev: v8.18.4
    hooks:
      - id: gitleaks
```

- [ ] **Step 5: Install and verify the empty project**

Run: `cd python-client && make install && make checkall`
Expected: ruff clean, pyright reports 0 errors on empty package, pytest collects/passes 0 tests (exit 5 — no tests yet — is acceptable here; treat as pass for this task).

- [ ] **Step 6: Commit**

```bash
git add python-client
git commit -m "feat(python-client): package scaffold (uv + ruff + pyright + pytest)"
```

---

### Task 2: errors.py — RtDbError + ErrorCode + retry helper

**Files:**
- Create: `python-client/src/par_rt_db/errors.py`
- Test: `python-client/tests/test_errors.py`

**Interfaces:**
- Produces: `ErrorCode` (enum), `RtDbError(Exception)` with `.code`/`.message`/`.status_code`, `RtDbError.from_envelope(dict)`, `RtDbError.from_http(status, body)`, `async retry_on_precondition(fn, *, max_attempts=5)`.

- [ ] **Step 1: Write the failing test**

`tests/test_errors.py`:
```python
import pytest
from par_rt_db.errors import ErrorCode, RtDbError, retry_on_precondition


def test_envelope_round_trip():
    err = RtDbError.from_envelope({"code": "NOT_FOUND", "message": "no doc"})
    assert err.code is ErrorCode.NOT_FOUND
    assert err.message == "no doc"
    assert err.status_code == 404
    assert "NOT_FOUND" in str(err)


def test_envelope_unknown_code_falls_back_to_internal():
    err = RtDbError.from_envelope({"code": "WAT", "message": "x"})
    assert err.code is ErrorCode.INTERNAL
    assert err.status_code == 500


def test_from_http_parses_body():
    err = RtDbError.from_http(422, b'{"code":"SCHEMA_VIOLATION","message":"bad"}')
    assert err.code is ErrorCode.SCHEMA_VIOLATION
    assert err.status_code == 422


def test_from_http_non_json_body_is_internal():
    err = RtDbError.from_http(500, b"<html>boom</html>")
    assert err.code is ErrorCode.INTERNAL
    assert err.status_code == 500
    assert "500" in err.message


@pytest.mark.asyncio
async def test_retry_on_precondition_succeeds_after_precondition_failed():
    calls = {"n": 0}

    async def fn():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RtDbError(ErrorCode.PRECONDITION_FAILED, "version mismatch")
        return "ok"

    out = await retry_on_precondition(fn, max_attempts=5)
    assert out == "ok"
    assert calls["n"] == 3


@pytest.mark.asyncio
async def test_retry_on_precondition_does_not_retry_other_errors():
    async def fn():
        raise RtDbError(ErrorCode.NOT_FOUND, "missing")

    with pytest.raises(RtDbError) as ei:
        await retry_on_precondition(fn, max_attempts=5)
    assert ei.value.code is ErrorCode.NOT_FOUND


@pytest.mark.asyncio
async def test_retry_on_precondition_exhausts_attempts():
    async def fn():
        raise RtDbError(ErrorCode.PRECONDITION_FAILED, "nope")

    with pytest.raises(RtDbError) as ei:
        await retry_on_precondition(fn, max_attempts=3)
    assert ei.value.code is ErrorCode.PRECONDITION_FAILED
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd python-client && uv run pytest tests/test_errors.py -q`
Expected: FAIL (module import error).

- [ ] **Step 3: Write `src/par_rt_db/errors.py`**

```python
"""Error envelope, codes, and the precondition-retry helper."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from enum import Enum
from typing import Any, TypeVar

T = TypeVar("T")


class ErrorCode(str, Enum):
    """The seven wire error codes (SCREAMING_SNAKE_CASE), each mapped to an HTTP status."""

    UNAUTHORIZED = "UNAUTHORIZED"
    FORBIDDEN = "FORBIDDEN"
    NOT_FOUND = "NOT_FOUND"
    PRECONDITION_FAILED = "PRECONDITION_FAILED"
    SCHEMA_VIOLATION = "SCHEMA_VIOLATION"
    BAD_REQUEST = "BAD_REQUEST"
    INTERNAL = "INTERNAL"


_STATUS: dict[ErrorCode, int] = {
    ErrorCode.BAD_REQUEST: 400,
    ErrorCode.UNAUTHORIZED: 401,
    ErrorCode.FORBIDDEN: 403,
    ErrorCode.NOT_FOUND: 404,
    ErrorCode.PRECONDITION_FAILED: 409,
    ErrorCode.SCHEMA_VIOLATION: 422,
    ErrorCode.INTERNAL: 500,
}


class RtDbError(Exception):
    """The single client error type. Mirrors the server's ``{code, message}`` envelope."""

    code: ErrorCode
    message: str

    def __init__(self, code: ErrorCode, message: str) -> None:
        self.code = code if isinstance(code, ErrorCode) else ErrorCode(code)
        self.message = message
        super().__init__(f"{self.code.value}: {message}")

    @property
    def status_code(self) -> int:
        """HTTP status this code maps to."""
        return _STATUS[self.code]

    @classmethod
    def from_envelope(cls, envelope: dict[str, Any]) -> "RtDbError":
        """Build from a parsed ``{code, message}`` body."""
        try:
            code = ErrorCode(envelope.get("code", "INTERNAL"))
        except ValueError:
            code = ErrorCode.INTERNAL
        return cls(code, str(envelope.get("message", "")))

    @classmethod
    def from_http(cls, status: int, body: bytes | str | None) -> "RtDbError":
        """Non-2xx response -> RtDbError. Parses ``{code,message}`` if present."""
        if body is None:
            return cls(ErrorCode.INTERNAL, f"request failed with status {status}")
        raw = body.decode("utf-8") if isinstance(body, bytes) else body
        try:
            env = json.loads(raw)
        except (ValueError, TypeError):
            return cls(ErrorCode.INTERNAL, f"request failed with status {status}")
        if isinstance(env, dict) and "code" in env:
            err = cls.from_envelope(env)
            # Trust the server's code->status mapping; fall back to the HTTP status.
            return err if _STATUS.get(err.code) == status else err
        return cls(ErrorCode.INTERNAL, f"request failed with status {status}")


async def retry_on_precondition(
    fn: Callable[[], Awaitable[T]],
    *,
    max_attempts: int = 5,
) -> T:
    """Call ``fn`` until it succeeds, retrying only on PRECONDITION_FAILED (OCC)."""
    last: RtDbError | None = None
    for _ in range(max_attempts):
        try:
            return await fn()
        except RtDbError as err:
            last = err
            if err.code is not ErrorCode.PRECONDITION_FAILED:
                raise
    assert last is not None
    raise last
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd python-client && uv run pytest tests/test_errors.py -q`
Expected: PASS (7 passed).

- [ ] **Step 5: Commit**

```bash
git add python-client/src/par_rt_db/errors.py python-client/tests/test_errors.py
git commit -m "feat(python-client): errors — RtDbError, ErrorCode, retry_on_precondition"
```

---

### Task 3: wire.py — leaf types (AuthedUser, ScheduleWhen, ScheduleInfo, FilterExpr, SearchQuery, VectorSearchQuery)

**Files:**
- Create: `python-client/src/par_rt_db/wire.py`
- Test: `python-client/tests/test_wire.py`

**Interfaces:**
- Produces: `AuthedUser`, `ScheduleWhen` (union), `ScheduleInfo`, `FilterExpr` (union), `SearchQuery`, `VectorSearchQuery`. A shared camelCase base `_Camel` (`extra="forbid"`, `populate_by_name=True`, `alias_generator=to_camel`).

- [ ] **Step 1: Write the failing test** (append to `tests/test_wire.py`)

```python
import pytest
from pydantic import ValidationError

from par_rt_db.wire import (
    AuthedUser,
    ScheduleInfo,
    ScheduleWhen,
    FilterExpr,
    SearchQuery,
    VectorSearchQuery,
)


def test_authed_user_minimal_includes_null_email_name_omits_github():
    u = AuthedUser.model_validate({"kind": "user"})
    dumped = u.model_dump(by_alias=True, mode="json")
    assert dumped["kind"] == "user"
    assert "email" in dumped and dumped["email"] is None     # null on wire
    assert "name" in dumped and dumped["name"] is None        # null on wire
    assert "githubLogin" not in dumped                        # omitted when absent
    assert "githubId" not in dumped


def test_authed_user_full():
    u = AuthedUser.model_validate({
        "kind": "machine", "email": "a@b.com", "name": "A",
        "githubLogin": "oct", "githubId": 7,
    })
    assert u.model_dump(by_alias=True, mode="json") == {
        "kind": "machine", "email": "a@b.com", "name": "A",
        "githubLogin": "oct", "githubId": 7,
    }


def test_authed_user_rejects_unknown():
    with pytest.raises(ValidationError):
        AuthedUser.model_validate({"kind": "user", "bogus": 1})


def test_schedule_when_variants():
    assert ScheduleWhen.model_validate({"type": "afterMs", "ms": 5}).model_dump(by_alias=True, mode="json") == {"type": "afterMs", "ms": 5}
    assert ScheduleWhen.model_validate({"type": "runAt", "ms": 9}).model_dump(by_alias=True, mode="json") == {"type": "runAt", "ms": 9}
    assert ScheduleWhen.model_validate({"type": "cron", "expr": "*/5 * * * *"}).model_dump(by_alias=True, mode="json") == {"type": "cron", "expr": "*/5 * * * *"}


def test_schedule_info_omits_optional_when_absent():
    si = ScheduleInfo.model_validate({
        "id": "j1", "kind": "oneshot", "dueAt": 100, "status": "pending",
        "createdAt": 1, "firedCount": 0,
    })
    d = si.model_dump(by_alias=True, mode="json")
    assert d["id"] == "j1" and d["dueAt"] == 100 and d["firedCount"] == 0
    assert "cron" not in d and "lastError" not in d


def test_filter_expr_leaves_and_combinators():
    eq = FilterExpr.model_validate({"type": "eq", "field": "status", "value": "active"})
    assert eq.model_dump(by_alias=True, mode="json") == {"type": "eq", "field": "status", "value": "active"}
    inv = FilterExpr.model_validate({"type": "in", "field": "status", "values": ["a", "b"]})
    assert inv.model_dump(by_alias=True, mode="json") == {"type": "in", "field": "status", "values": ["a", "b"]}
    and_ = FilterExpr.model_validate({"type": "and", "exprs": [
        {"type": "eq", "field": "a", "value": 1},
        {"type": "or", "exprs": []},
    ]})
    assert and_.model_dump(by_alias=True, mode="json")["type"] == "and"


def test_search_and_vector_query_shapes():
    sq = SearchQuery.model_validate({"index": "idx", "query": "hello"})
    assert sq.model_dump(by_alias=True, mode="json") == {"index": "idx", "query": "hello"}
    vq = VectorSearchQuery.model_validate({"index": "v", "vector": [0.1, 0.2], "limit": 8})
    out = vq.model_dump(by_alias=True, mode="json")
    assert out["index"] == "v" and out["vector"] == [0.1, 0.2] and out["limit"] == 8 and "filter" not in out
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd python-client && uv run pytest tests/test_wire.py -q`
Expected: FAIL (import error).

- [ ] **Step 3: Write the leaf-type portion of `src/par_rt_db/wire.py`**

```python
"""Wire types — the fourth implementation of par-rt-db's JSON contract.

Mirrors server/src/protocol.rs, ts-client/src/protocol.ts, rust-client/src/wire.rs
byte-for-byte. Discriminator unions use ``type``; Message/Schema/Schedule/AuthedUser
fields are camelCase on the wire (Python names snake_case via alias_generator);
``extra='forbid'`` everywhere == ``deny_unknown_fields``.
"""

from __future__ import annotations

from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, ModelSerialize


def to_camel(name: str) -> str:
    """snake_case -> camelCase alias."""
    head, *tail = name.split("_")
    return head + "".join(p.title() for p in tail)


class _Camel(BaseModel):
    """Base for wire models whose JSON keys are camelCase and reject unknown fields."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        alias_generator=to_camel,
    )


class AuthedUser(_Camel):
    """Authenticated principal. ``email``/``name`` serialize as null; GitHub keys omit when None."""

    kind: str
    email: str | None = None
    name: str | None = None
    github_login: str | None = None
    github_id: int | None = None

    @ModelSerialize.mode("wrap")
    def _serialize(self, handler) -> dict:  # type: ignore[no-untyped-def]
        out = handler(self)
        # githubLogin/githubId are omitted on the wire when None (server skip_serializing_if).
        for alias in ("githubLogin", "githubId"):
            if out.get(alias) is None:
                out.pop(alias, None)
        return out


# --- ScheduleWhen (discriminator "type", camelCase) ---

class _AfterMs(_Camel):
    type: Literal["afterMs"] = "afterMs"
    ms: int


class _RunAt(_Camel):
    type: Literal["runAt"] = "runAt"
    ms: int


class _Cron(_Camel):
    type: Literal["cron"] = "cron"
    expr: str


ScheduleWhen = Annotated[Union[_AfterMs, _RunAt, _Cron], Field(discriminator="type")]


class ScheduleInfo(_Camel):
    """A scheduled job's public view (returned by listSchedules)."""

    id: str
    kind: str
    due_at: int
    cron: str | None = None
    status: str
    last_error: str | None = None
    created_at: int
    fired_count: int

    @ModelSerialize.mode("wrap")
    def _serialize(self, handler) -> dict:  # type: ignore[no-untyped-def]
        out = handler(self)
        for alias in ("cron", "lastError"):
            if out.get(alias) is None:
                out.pop(alias, None)
        return out


# --- FilterExpr (discriminator "type") ---

class _FilterLeaf(_Camel):
    field: str


class _FilterEq(_FilterLeaf):
    type: Literal["eq"] = "eq"
    value: object


class _FilterNeq(_FilterLeaf):
    type: Literal["neq"] = "neq"
    value: object


class _FilterGt(_FilterLeaf):
    type: Literal["gt"] = "gt"
    value: object


class _FilterGte(_FilterLeaf):
    type: Literal["gte"] = "gte"
    value: object


class _FilterLt(_FilterLeaf):
    type: Literal["lt"] = "lt"
    value: object


class _FilterLte(_FilterLeaf):
    type: Literal["lte"] = "lte"
    value: object


class _FilterIn(_FilterLeaf):
    type: Literal["in"] = "in"
    values: list[object]


class _FilterAnd(_Camel):
    type: Literal["and"] = "and"
    exprs: list["FilterExpr"]


class _FilterOr(_Camel):
    type: Literal["or"] = "or"
    exprs: list["FilterExpr"]


FilterExpr = Annotated[
    Union[_FilterEq, _FilterNeq, _FilterGt, _FilterGte, _FilterLt, _FilterLte, _FilterIn, _FilterAnd, _FilterOr],
    Field(discriminator="type"),
]


class SearchQuery(_Camel):
    """Full-text search terminal: ``{index, query}``."""

    index: str
    query: str


class VectorSearchQuery(_Camel):
    """Vector search terminal: ``{index, vector, limit, filter?}``."""

    index: str
    vector: list[float]
    limit: int
    filter: FilterExpr | None = None

    @ModelSerialize.mode("wrap")
    def _serialize(self, handler) -> dict:  # type: ignore[no-untyped-def]
        out = handler(self)
        if out.get("filter") is None:
            out.pop("filter", None)
        return out
```

> **Note for the implementer:** `ModelSerialize` is Pydantic v2's per-model custom serializer decorator (`pydantic.ModelSerialize`, added in 2.7). If the installed Pydantic exposes it as `pydantic.model_serializer` only, use `@model_serializer(mode="wrap")` instead — same semantics. The parity tests (Task 6) are the authority; adjust to make them pass.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd python-client && uv run pytest tests/test_wire.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python-client/src/par_rt_db/wire.py python-client/tests/test_wire.py
git commit -m "feat(python-client): wire leaf types (AuthedUser, Schedule*, FilterExpr, Search/Vector)"
```

---

### Task 4: wire.py — ClientMessage union

**Files:**
- Modify: `python-client/src/par_rt_db/wire.py` (append ClientMessage variants + union)
- Modify: `python-client/tests/test_wire.py` (append tests)
- Consumes: `query.Query` (Task 9 — but `Query` is a plain forward-ref `dict`-compatible model; to avoid a cyclic dep, `wire.py` types `query` as `Query` via `TYPE_CHECKING` + a late import in `query.py`). For Tasks 4–6, define `Query` as a forward reference and have parity fixtures use minimal `{"table": "t"}` queries; `query.py` (Task 9) supplies the concrete model.

**Interfaces:**
- Produces: `ClientMessage` discriminated union (auth, subscribe, unsubscribe, mutate, schedule, cancelSchedule, pauseSchedule, resumeSchedule, listSchedules, ping). `Mutate.idempotency_key` omits when None.

- [ ] **Step 1: Write the failing test** (append)

```python
import pytest
from pydantic import ValidationError

from par_rt_db.wire import ClientMessage


def _model(d):
    return ClientMessage.model_validate(d).model_dump(by_alias=True, mode="json", exclude_unset=False)


def test_client_auth():
    assert _model({"type": "auth", "token": "t", "db": "d"}) == {"type": "auth", "token": "t", "db": "d"}


def test_client_unsubscribe():
    assert _model({"type": "unsubscribe", "queryId": "q1"}) == {"type": "unsubscribe", "queryId": "q1"}


def test_client_mutate_omits_idempotency_key_when_none():
    m = ClientMessage.model_validate({"type": "mutate", "mutId": "m1", "txn": {"steps": []}})
    dumped = m.model_dump(by_alias=True, mode="json")
    assert dumped == {"type": "mutate", "mutId": "m1", "txn": {"steps": []}}
    assert "idempotencyKey" not in dumped


def test_client_mutate_with_idempotency_key():
    dumped = ClientMessage.model_validate({
        "type": "mutate", "mutId": "m1", "idempotencyKey": "k1", "txn": {"steps": []},
    }).model_dump(by_alias=True, mode="json")
    assert dumped == {"type": "mutate", "mutId": "m1", "idempotencyKey": "k1", "txn": {"steps": []}}


def test_client_schedule():
    dumped = ClientMessage.model_validate({
        "type": "schedule", "scheduleId": "s1",
        "when": {"type": "afterMs", "ms": 100}, "txn": {"steps": []},
    }).model_dump(by_alias=True, mode="json")
    assert dumped == {
        "type": "schedule", "scheduleId": "s1",
        "when": {"type": "afterMs", "ms": 100}, "txn": {"steps": []},
    }


def test_client_cancel_pause_resume_carry_id():
    for tag in ("cancelSchedule", "pauseSchedule", "resumeSchedule"):
        d = {"type": tag, "scheduleId": "s1", "id": "job-9"}
        assert _model(d) == d


def test_client_list_schedules():
    assert _model({"type": "listSchedules", "scheduleId": "s1"}) == {"type": "listSchedules", "scheduleId": "s1"}


def test_client_ping():
    assert _model({"type": "ping"}) == {"type": "ping"}


def test_client_message_rejects_unknown_fields():
    with pytest.raises(ValidationError):
        ClientMessage.model_validate({"type": "auth", "token": "t", "db": "d", "bogus": True})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd python-client && uv run pytest tests/test_wire.py -q`
Expected: FAIL (`ClientMessage` not defined).

- [ ] **Step 3: Append to `src/par_rt_db/wire.py`**

```python
# --- ClientMessage (discriminator "type", extra=forbid) ---
#
# `query` and `txn` are forward references: concrete models live in query.py /
# mutation.py (Tasks 9-10). They serialize as {"table": ...} / {"steps": [...]}.

if TYPE_CHECKING:  # noqa: F821  (TYPE_CHECKING imported below)
    from par_rt_db.query import Query
    from par_rt_db.mutation import Transaction


class _ClientAuth(_Camel):
    type: Literal["auth"] = "auth"
    token: str
    db: str


class _ClientSubscribe(_Camel):
    type: Literal["subscribe"] = "subscribe"
    query_id: str
    query: "Query"


class _ClientUnsubscribe(_Camel):
    type: Literal["unsubscribe"] = "unsubscribe"
    query_id: str


class _ClientMutate(_Camel):
    type: Literal["mutate"] = "mutate"
    mut_id: str
    idempotency_key: str | None = None
    txn: "Transaction"

    @ModelSerialize.mode("wrap")
    def _serialize(self, handler) -> dict:  # type: ignore[no-untyped-def]
        out = handler(self)
        if out.get("idempotencyKey") is None:
            out.pop("idempotencyKey", None)
        return out


class _ClientSchedule(_Camel):
    type: Literal["schedule"] = "schedule"
    schedule_id: str
    when: ScheduleWhen
    txn: "Transaction"


class _ClientCancelSchedule(_Camel):
    type: Literal["cancelSchedule"] = "cancelSchedule"
    schedule_id: str
    id: str


class _ClientPauseSchedule(_Camel):
    type: Literal["pauseSchedule"] = "pauseSchedule"
    schedule_id: str
    id: str


class _ClientResumeSchedule(_Camel):
    type: Literal["resumeSchedule"] = "resumeSchedule"
    schedule_id: str
    id: str


class _ClientListSchedules(_Camel):
    type: Literal["listSchedules"] = "listSchedules"
    schedule_id: str


class _ClientPing(_Camel):
    type: Literal["ping"] = "ping"


ClientMessage = Annotated[
    Union[
        _ClientAuth,
        _ClientSubscribe,
        _ClientUnsubscribe,
        _ClientMutate,
        _ClientSchedule,
        _ClientCancelSchedule,
        _ClientPauseSchedule,
        _ClientResumeSchedule,
        _ClientListSchedules,
        _ClientPing,
    ],
    Field(discriminator="type"),
]
```

Add to the top imports: `from typing import TYPE_CHECKING` and `if TYPE_CHECKING: ...` block (shown inline above). Also register the forward refs for resolution at end of file:

```python
# Resolve forward references once query.py / mutation.py exist (Tasks 9-10).
# Until then, model_validate on the union works because the nested query/txn
# payloads are validated structurally against the concrete models when imported.
```

> **Implementer note:** Until Tasks 9–10 land, `Query`/`Transaction` are forward refs. The Task-4 tests use `txn: {"steps": []}` and `query: {"table": "t"}`, which the concrete models (added next) accept. If Pydantic raises about unresolved refs before Tasks 9–10, temporarily type `query`/`txn` as `dict[str, object]` and switch back to the model forward-refs in Tasks 9–10. The Task 6 parity gate confirms the final typing.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd python-client && uv run pytest tests/test_wire.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python-client/src/par_rt_db/wire.py python-client/tests/test_wire.py
git commit -m "feat(python-client): wire ClientMessage union (auth/subscribe/mutate/schedule/ping)"
```

---

### Task 5: wire.py — ServerMessage union

**Files:**
- Modify: `python-client/src/par_rt_db/wire.py` (append ServerMessage)
- Modify: `python-client/tests/test_wire.py` (append tests)
- Consumes: `RtDbError` (Task 2) — embedded as `{code, message}` inside `authErr`/`mutateErr`/`subscribeErr`/`scheduleErr`/`scheduleAck.error`.

- [ ] **Step 1: Write the failing test** (append)

```python
from par_rt_db.wire import ServerMessage


def test_server_auth_ok():
    dumped = ServerMessage.model_validate({
        "type": "authOk", "user": {"kind": "user"},
    }).model_dump(by_alias=True, mode="json")
    assert dumped["type"] == "authOk"
    assert dumped["user"] == {"kind": "user", "email": None, "name": None}


def test_server_query_update():
    dumped = ServerMessage.model_validate({
        "type": "queryUpdate", "queryId": "q1", "result": [],
    }).model_dump(by_alias=True, mode="json")
    assert dumped == {"type": "queryUpdate", "queryId": "q1", "result": []}


def test_server_mutate_err_embeds_envelope():
    dumped = ServerMessage.model_validate({
        "type": "mutateErr", "mutId": "m1",
        "error": {"code": "NOT_FOUND", "message": "x"},
    }).model_dump(by_alias=True, mode="json")
    assert dumped == {"type": "mutateErr", "mutId": "m1", "error": {"code": "NOT_FOUND", "message": "x"}}


def test_server_schedule_ack_ok_omits_error():
    dumped = ServerMessage.model_validate({
        "type": "scheduleAck", "scheduleId": "s1", "ok": True,
    }).model_dump(by_alias=True, mode="json")
    assert dumped == {"type": "scheduleAck", "scheduleId": "s1", "ok": True}
    assert "error" not in dumped


def test_server_schedule_ack_err_includes_error():
    dumped = ServerMessage.model_validate({
        "type": "scheduleAck", "scheduleId": "s1", "ok": False,
        "error": {"code": "NOT_FOUND", "message": "no job"},
    }).model_dump(by_alias=True, mode="json")
    assert dumped["ok"] is False
    assert dumped["error"] == {"code": "NOT_FOUND", "message": "no job"}


def test_server_list_schedules_ok():
    dumped = ServerMessage.model_validate({
        "type": "listSchedulesOk", "scheduleId": "s1", "schedules": [],
    }).model_dump(by_alias=True, mode="json")
    assert dumped == {"type": "listSchedulesOk", "scheduleId": "s1", "schedules": []}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd python-client && uv run pytest tests/test_wire.py -q`
Expected: FAIL (`ServerMessage` not defined).

- [ ] **Step 3: Append to `src/par_rt_db/wire.py`**

An embedded error is just the `{code, message}` envelope (no `type`), so model it as a small `_ErrorEnvelope` model (not `RtDbError`, which is an Exception):

```python
class _ErrorEnvelope(_Camel):
    """The ``{code, message}`` body embedded in WS error frames."""

    code: str
    message: str


class _ServerAuthOk(_Camel):
    type: Literal["authOk"] = "authOk"
    user: AuthedUser


class _ServerAuthErr(_Camel):
    type: Literal["authErr"] = "authErr"
    error: _ErrorEnvelope


class _ServerQueryUpdate(_Camel):
    type: Literal["queryUpdate"] = "queryUpdate"
    query_id: str
    result: object  # untagged QueryResult; parsed by query.py (Task 9)


class _ServerMutateOk(_Camel):
    type: Literal["mutateOk"] = "mutateOk"
    mut_id: str
    results: list[object]


class _ServerMutateErr(_Camel):
    type: Literal["mutateErr"] = "mutateErr"
    mut_id: str
    error: _ErrorEnvelope


class _ServerSubscribeErr(_Camel):
    type: Literal["subscribeErr"] = "subscribeErr"
    query_id: str
    error: _ErrorEnvelope


class _ServerScheduleOk(_Camel):
    type: Literal["scheduleOk"] = "scheduleOk"
    schedule_id: str
    id: str


class _ServerScheduleErr(_Camel):
    type: Literal["scheduleErr"] = "scheduleErr"
    schedule_id: str
    error: _ErrorEnvelope


class _ServerScheduleAck(_Camel):
    type: Literal["scheduleAck"] = "scheduleAck"
    schedule_id: str
    ok: bool
    error: _ErrorEnvelope | None = None

    @ModelSerialize.mode("wrap")
    def _serialize(self, handler) -> dict:  # type: ignore[no-untyped-def]
        out = handler(self)
        if out.get("error") is None:
            out.pop("error", None)
        return out


class _ServerListSchedulesOk(_Camel):
    type: Literal["listSchedulesOk"] = "listSchedulesOk"
    schedule_id: str
    schedules: list[ScheduleInfo]


class _ServerPong(_Camel):
    type: Literal["pong"] = "pong"


ServerMessage = Annotated[
    Union[
        _ServerAuthOk,
        _ServerAuthErr,
        _ServerQueryUpdate,
        _ServerMutateOk,
        _ServerMutateErr,
        _ServerSubscribeErr,
        _ServerScheduleOk,
        _ServerScheduleErr,
        _ServerScheduleAck,
        _ServerListSchedulesOk,
        _ServerPong,
    ],
    Field(discriminator="type"),
]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd python-client && uv run pytest tests/test_wire.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python-client/src/par_rt_db/wire.py python-client/tests/test_wire.py
git commit -m "feat(python-client): wire ServerMessage union (authOk/queryUpdate/mutate*/schedule*/pong)"
```

---

### Task 6: wire parity — four-way round-trip fixtures

**Files:**
- Create: `python-client/tests/conftest.py` (fixture loader)
- Create: `python-client/tests/test_wire_parity.py`
- The canonical fixtures are copied verbatim from `server/src/protocol.rs` tests (the authoritative wire shapes).

**Interfaces:**
- Produces: the four-way contract safety net. If any model drifts from the server's bytes, this fails.

- [ ] **Step 1: Write the parity test**

`tests/test_wire_parity.py`:
```python
"""Round-trip parity: our serialized JSON must equal the server's wire bytes.

Fixtures are copied from server/src/protocol.rs tests (the authoritative shapes).
A failure here means a wire model drifted from the contract.
"""

import json

import pytest

from par_rt_db.wire import ClientMessage, ServerMessage


# (model, wire_json_string) — every entry must round-trip identically.
CLIENT_FIXTURES: list[str] = [
    '{"type": "auth", "token": "t", "db": "d"}',
    '{"type": "unsubscribe", "queryId": "q1"}',
    '{"type": "mutate", "mutId": "m1", "txn": {"steps": []}}',
    '{"type": "mutate", "mutId": "m1", "idempotencyKey": "key1", "txn": {"steps": []}}',
    '{"type": "ping"}',
    '{"type": "schedule", "scheduleId": "s1", "when": {"type": "afterMs", "ms": 100}, "txn": {"steps": []}}',
    '{"type": "cancelSchedule", "scheduleId": "s1", "id": "job-9"}',
    '{"type": "pauseSchedule", "scheduleId": "s1", "id": "job-9"}',
    '{"type": "resumeSchedule", "scheduleId": "s1", "id": "job-9"}',
    '{"type": "listSchedules", "scheduleId": "s1"}',
]

SERVER_FIXTURES: list[str] = [
    '{"type": "queryUpdate", "queryId": "q1", "result": []}',
    '{"type": "mutateOk", "mutId": "m1", "results": []}',
    '{"type": "subscribeErr", "queryId": "q1", "error": {"code": "BAD_REQUEST", "message": "bad index"}}',
    '{"type": "pong"}',
    '{"type": "scheduleOk", "scheduleId": "s1", "id": "job-9"}',
    '{"type": "scheduleAck", "scheduleId": "s1", "ok": true}',
    '{"type": "scheduleAck", "scheduleId": "s1", "ok": false, "error": {"code": "NOT_FOUND", "message": "x"}}',
]


@pytest.mark.parametrize("wire", CLIENT_FIXTURES)
def test_client_message_round_trip(wire: str):
    expected = json.loads(wire)
    msg = ClientMessage.model_validate(expected)
    dumped = json.loads(msg.model_dump_json(by_alias=True))
    assert dumped == expected, f"client wire drift: {dumped} != {expected}"


@pytest.mark.parametrize("wire", SERVER_FIXTURES)
def test_server_message_round_trip(wire: str):
    expected = json.loads(wire)
    msg = ServerMessage.model_validate(expected)
    dumped = json.loads(msg.model_dump_json(by_alias=True))
    assert dumped == expected, f"server wire drift: {dumped} != {expected}"
```

- [ ] **Step 2: Run the parity tests**

Run: `cd python-client && uv run pytest tests/test_wire_parity.py -q`
Expected: PASS (all fixtures round-trip). If any drift, fix the model (not the fixture) until it passes — the server's bytes are the authority.

- [ ] **Step 3: Commit**

```bash
git add python-client/tests/test_wire_parity.py
git commit -m "test(python-client): four-way wire parity fixtures (from server protocol.rs)"
```

---

### Task 7: cursor.py — opaque keyset cursor codec

**Files:**
- Create: `python-client/src/par_rt_db/cursor.py`
- Test: `python-client/tests/test_cursor.py`
- Produces: `encode_cursor(values: list) -> str`, `decode_cursor(s: str) -> list`. base64 of a JSON array (mirrors `ts-client/src/pagination.ts`, `rust-client/src/cursor.rs`).

- [ ] **Step 1: Write the failing test**

`tests/test_cursor.py`:
```python
import pytest

from par_rt_db.cursor import decode_cursor, encode_cursor


def test_round_trip_mixed_types():
    values = ["a", 3, 1.5, None, True]
    cur = encode_cursor(values)
    assert isinstance(cur, str)
    assert decode_cursor(cur) == values


def test_empty():
    assert decode_cursor(encode_cursor([])) == []


def test_decode_rejects_garbage():
    with pytest.raises(Exception):
        decode_cursor("not-valid-base64-or-json!!!")


def test_decode_rejects_non_array():
    import base64
    import json
    blob = base64.b64encode(json.dumps({"not": "array"}).encode()).decode()
    with pytest.raises(Exception):
        decode_cursor(blob)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd python-client && uv run pytest tests/test_cursor.py -q`
Expected: FAIL.

- [ ] **Step 3: Write `src/par_rt_db/cursor.py`**

```python
"""Opaque keyset cursor codec: base64 of a JSON array (parity with TS/Rust clients)."""

from __future__ import annotations

import base64
import json
from typing import Any


def encode_cursor(values: list[Any]) -> str:
    """Encode a sort-tuple into an opaque base64 cursor string."""
    raw = json.dumps(values, separators=(",", ":")).encode("utf-8")
    return base64.b64encode(raw).decode("ascii")


def decode_cursor(cursor: str) -> list[Any]:
    """Decode an opaque cursor back into the sort-tuple. Raises on malformed input."""
    try:
        raw = base64.b64decode(cursor.encode("ascii"), validate=True)
        values = json.loads(raw)
    except (ValueError, json.JSONDecodeError) as err:
        raise ValueError("invalid cursor") from err
    if not isinstance(values, list):
        raise ValueError("cursor must decode to a JSON array")
    return values
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd python-client && uv run pytest tests/test_cursor.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python-client/src/par_rt_db/cursor.py python-client/tests/test_cursor.py
git commit -m "feat(python-client): cursor codec (base64 of JSON array)"
```

---

### Task 8: schema.py — FieldType (15) + indexes + builders + `t`

**Files:**
- Create: `python-client/src/par_rt_db/schema.py`
- Test: `python-client/tests/test_schema.py`
- Produces: `FieldType` (15-variant discriminated union, tag `"type"`, camelCase, `extra="forbid"`), `IndexDef`, `VectorIndexSpec`, `TableDef` (+`owner_field`), `SchemaDef`, `TableBuilder`, `SchemaBuilder`, and a `t` namespace of field constructors. `int64`/`Id` are wire strings.

- [ ] **Step 1: Write the failing test**

`tests/test_schema.py`:
```python
import json

import pytest
from pydantic import ValidationError

from par_rt_db.schema import Schema, t


def test_scalar_and_compound_fields_round_trip():
    schema = (
        Schema.builder()
        .table("players", lambda tb: tb
            .field("email", t.string())
            .field("age", t.number())
            .field("alive", t.boolean())
            .field("nick", t.null())
            .field("ref", t.id("players"))
            .field("role", t.literal("admin"))
            .field("tags", t.array(t.string()))
            .field("meta", t.optional(t.object({"x": t.number()})))
            .field("mix", t.union([t.string(), t.number()]))
            .field("kv", t.record(t.number()))
            .field("raw", t.any())
            .field("b", t.bytes())
            .field("big", t.int64())
            .field("emb", t.vector(8)))
        .build()
    )
    wire = json.loads(schema.model_dump_json(by_alias=True))
    assert set(wire["tables"]["players"]["fields"].keys()) == {
        "email", "age", "alive", "nick", "ref", "role", "tags", "meta", "mix", "kv", "raw", "b", "big", "emb",
    }
    assert wire["tables"]["players"]["fields"]["email"] == {"type": "string"}
    assert wire["tables"]["players"]["fields"]["ref"] == {"type": "id", "table": "players"}
    assert wire["tables"]["players"]["fields"]["emb"] == {"type": "vector", "dimensions": 8}
    assert wire["tables"]["players"]["fields"]["big"] == {"type": "int64"}


def test_indexes_search_vector_owner_field():
    schema = (
        Schema.builder()
        .table("boxes", lambda tb: tb
            .field("status", t.string())
            .field("owner_id", t.id("players"))
            .field("embedding", t.vector(4))
            .index("by_status", ["status"])
            .search_index("text_idx", ["status"])
            .vector_index("emb_idx", "embedding", 4, filter_fields=["owner_id"])
            .owner_field("owner_id"))
        .build()
    )
    wire = json.loads(schema.model_dump_json(by_alias=True))
    tbl = wire["tables"]["boxes"]
    assert tbl["indexes"][0] == {"name": "by_status", "fields": ["status"]}
    assert tbl["indexes"][1] == {"name": "text_idx", "fields": ["status"], "search": True}
    assert tbl["indexes"][2] == {
        "name": "emb_idx", "fields": ["embedding"], "vector": {"dimensions": 4, "filterFields": ["owner_id"]},
    }
    assert tbl["ownerField"] == "owner_id"


def test_field_type_rejects_unknown():
    with pytest.raises(ValidationError):
        t._validate({"type": "bogus"})  # helper exposed for the test


def test_schema_rejects_unknown_top_keys():
    with pytest.raises(ValidationError):
        Schema.__pydantic_validator__.validate({"tables": {}, "bogus": 1})  # type: ignore[attr-defined]
```

> The `t._validate` / `Schema.__pydantic_validator__` calls are thin affordances; if cleaner, expose `FieldType.model_validate` directly and test that. Adjust the test to the implemented surface — the assertions on the wire shape are the real gate.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd python-client && uv run pytest tests/test_schema.py -q`
Expected: FAIL.

- [ ] **Step 3: Write `src/par_rt_db/schema.py`**

```python
"""Schema DSL: FieldType (15 variants), indexes, TableDef/SchemaDef, builders."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field

from .wire import to_camel


class _S(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True, alias_generator=to_camel)


# --- FieldType (discriminator "type", camelCase, extra=forbid) ---

class _FString(_S):
    type: Literal["string"] = "string"


class _FNumber(_S):
    type: Literal["number"] = "number"


class _FBoolean(_S):
    type: Literal["boolean"] = "boolean"


class _FNull(_S):
    type: Literal["null"] = "null"


class _FId(_S):
    type: Literal["id"] = "id"
    table: str


class _FLiteral(_S):
    type: Literal["literal"] = "literal"
    value: Any


class _FOptional(_S):
    type: Literal["optional"] = "optional"
    inner: "FieldType"


class _FUnion(_S):
    type: Literal["union"] = "union"
    variants: list["FieldType"]


class _FArray(_S):
    type: Literal["array"] = "array"
    element: "FieldType"


class _FObject(_S):
    type: Literal["object"] = "object"
    fields: dict[str, "FieldType"]


class _FInt64(_S):
    type: Literal["int64"] = "int64"   # wire string


class _FBytes(_S):
    type: Literal["bytes"] = "bytes"   # wire base64 string


class _FAny(_S):
    type: Literal["any"] = "any"


class _FRecord(_S):
    type: Literal["record"] = "record"
    value: "FieldType"


class _FVector(_S):
    type: Literal["vector"] = "vector"
    dimensions: int


FieldType = Annotated[
    Union[
        _FString, _FNumber, _FBoolean, _FNull, _FId, _FLiteral, _FOptional,
        _FUnion, _FArray, _FObject, _FInt64, _FBytes, _FAny, _FRecord, _FVector,
    ],
    Field(discriminator="type"),
]


# --- Indexes ---

class VectorIndexSpec(_S):
    dimensions: int
    filter_fields: list[str] = Field(default_factory=list)


class IndexDef(_S):
    name: str
    fields: list[str]
    search: bool | None = None
    vector: VectorIndexSpec | None = None

    @classmethod
    def _serialize_cls(cls) -> None: ...  # placeholder for mypy; real handling below


# Omit `search`/`vector` when None (server omits absent flags): handled by a
# module-level dump helper used by Schema.model_dump_json (see end of file).


class TableDef(_S):
    fields: dict[str, FieldType]
    indexes: list[IndexDef] = Field(default_factory=list)
    owner_field: str | None = None


class SchemaDef(_S):
    tables: dict[str, TableDef]


# --- Builders ---

class TableBuilder:
    """Fluent builder for one table's fields/indexes."""

    def __init__(self) -> None:
        self._fields: dict[str, Any] = {}
        self._indexes: list[dict[str, Any]] = []
        self._owner: str | None = None

    def field(self, name: str, ft: Any) -> "TableBuilder":
        self._fields[name] = ft
        return self

    def index(self, name: str, fields: list[str]) -> "TableBuilder":
        self._indexes.append({"name": name, "fields": fields})
        return self

    def search_index(self, name: str, fields: list[str]) -> "TableBuilder":
        self._indexes.append({"name": name, "fields": fields, "search": True})
        return self

    def vector_index(
        self, name: str, field: str, dimensions: int, *, filter_fields: list[str] | None = None,
    ) -> "TableBuilder":
        self._indexes.append({
            "name": name, "fields": [field],
            "vector": {"dimensions": dimensions, "filterFields": filter_fields or []},
        })
        return self

    def owner_field(self, name: str) -> "TableBuilder":
        self._owner = name
        return self

    def _build(self) -> dict[str, Any]:
        out: dict[str, Any] = {"fields": self._fields, "indexes": self._indexes}
        if self._owner is not None:
            out["ownerField"] = self._owner
        return out


class SchemaBuilder:
    """Fluent builder for a whole schema."""

    def __init__(self) -> None:
        self._tables: dict[str, dict[str, Any]] = {}

    def table(self, name: str, configure: Any) -> "SchemaBuilder":
        tb = TableBuilder()
        configure(tb)
        self._tables[name] = tb._build()
        return self

    def build(self) -> "SchemaDef":
        return SchemaDef.model_validate({"tables": self._tables})


class _SchemaNamespace:
    """Entry point + field-type constructors (`t.string()`, `t.id(...)`, ...)."""

    builder = staticmethod(SchemaBuilder)

    @staticmethod
    def _validate(v: Any) -> FieldType:
        return FieldType.model_validate(v)  # type: ignore[return-value]

    string = staticmethod(lambda: {"type": "string"})
    number = staticmethod(lambda: {"type": "number"})
    boolean = staticmethod(lambda: {"type": "boolean"})
    null = staticmethod(lambda: {"type": "null"})
    any = staticmethod(lambda: {"type": "any"})
    int64 = staticmethod(lambda: {"type": "int64"})
    bytes = staticmethod(lambda: {"type": "bytes"})

    @staticmethod
    def id(table: str) -> dict[str, Any]:
        return {"type": "id", "table": table}

    @staticmethod
    def literal(value: Any) -> dict[str, Any]:
        return {"type": "literal", "value": value}

    @staticmethod
    def optional(inner: Any) -> dict[str, Any]:
        return {"type": "optional", "inner": inner}

    @staticmethod
    def union(variants: list[Any]) -> dict[str, Any]:
        return {"type": "union", "variants": variants}

    @staticmethod
    def array(element: Any) -> dict[str, Any]:
        return {"type": "array", "element": element}

    @staticmethod
    def object(fields: dict[str, Any]) -> dict[str, Any]:
        return {"type": "object", "fields": fields}

    @staticmethod
    def record(value: Any) -> dict[str, Any]:
        return {"type": "record", "value": value}

    @staticmethod
    def vector(dimensions: int) -> dict[str, Any]:
        return {"type": "vector", "dimensions": dimensions}


# Public entry points:
Schema = SimpleNamespace(builder=SchemaBuilder, model_validate=SchemaDef.model_validate)
t = _SchemaNamespace()
SchemaDef.model_rebuild()
```

> **Implementer note:** the field constructors return plain `dict` wire shapes (the simplest, wire-identical form), and `SchemaBuilder.build()` validates them into `SchemaDef`. The `IndexDef.search`/`vector` omit-when-None rule must hold on serialization; if `model_dump(by_alias=True)` includes `"search": null`, add a `model_serializer` to `IndexDef` dropping None `search`/`vector` (same pattern as Task 3). The Task 8 test asserts the exact wire shape — make it pass.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd python-client && uv run pytest tests/test_schema.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python-client/src/par_rt_db/schema.py python-client/tests/test_schema.py
git commit -m "feat(python-client): schema DSL — FieldType(15), indexes, builders, t"
```

---

### Task 9: query.py — Query + TableQuery builder + QueryResult parse

**Files:**
- Create: `python-client/src/par_rt_db/query.py`
- Test: `python-client/tests/test_query.py`
- Produces: `Query` (snake_case wire fields, all optional), `TableQuery` builder (`get`/`index+eq`/`gt/gte/lt/lte`/`order`/`take`/`collect`/`unique`/`first`/`count`/`paginate`/`filter`/`search`/`vector_search`), `Paginated[T]`, `parse_result(model, terminal, value)`. `model` is a Pydantic `BaseModel` subclass (or `dict`).
- Consumes: `FilterExpr`/`SearchQuery`/`VectorSearchQuery` (Task 3), `encode_cursor`/`decode_cursor` (Task 7). Also resolves `wire.py`'s `Query` forward ref.

- [ ] **Step 1: Write the failing test**

`tests/test_query.py`:
```python
import json

import pytest
from pydantic import BaseModel

from par_rt_db.query import TableQuery, Query, Paginated, parse_result
from par_rt_db.schema import t


class Box(BaseModel):
    id: str
    status: str


def test_query_index_eq_range_order_take_collect():
    q = TableQuery("boxes").with_index("by_status").eq("active").gte(10).lt(100).order("asc").take(50)
    wire = q.build().model_dump(by_alias=True, mode="json")
    assert wire == {
        "table": "boxes", "index": "by_status", "eq": ["active"],
        "gte": 10, "lt": 100, "order": "asc", "take": 50,
    }


def test_query_get():
    assert TableQuery("boxes").get("0123").build().model_dump(by_alias=True, mode="json") == {
        "table": "boxes", "get": "0123",
    }


def test_query_count_and_first_and_unique_terminals():
    assert TableQuery("t").with_index("i").eq("a").build_for_count().model_dump(by_alias=True, mode="json")["count"] is True
    assert TableQuery("t").with_index("i").eq("a").build_for_first().model_dump(by_alias=True, mode="json")["first"] is True
    assert TableQuery("t").with_index("i").eq("a").build_for_unique().model_dump(by_alias=True, mode="json")["unique"] is True


def test_query_paginate():
    q = TableQuery("t").with_index("i").eq("a").order("desc").paginate(num_items=20)
    wire = q.build().model_dump(by_alias=True, mode="json")
    assert wire["paginate"] == {"numItems": 20}
    q2 = TableQuery("t").with_index("i").eq("a").paginate(cursor="Abc", num_items=5)
    assert q2.build().model_dump(by_alias=True, mode="json")["paginate"] == {"cursor": "Abc", "numItems": 5}


def test_query_filter_search_vector():
    from par_rt_db.wire import FilterExpr
    f = FilterExpr.model_validate({"type": "eq", "field": "status", "value": "active"})
    q = TableQuery("t").with_index("i").eq("a").filter(f).take(10)
    assert q.build().model_dump(by_alias=True, mode="json")["filter"] == {"type": "eq", "field": "status", "value": "active"}
    s = TableQuery("t").search("idx", "hello").take(5)
    assert s.build().model_dump(by_alias=True, mode="json")["search"] == {"index": "idx", "query": "hello"}
    v = TableQuery("t").vector_search("vidx", [1.0, 2.0], limit=3)
    assert v.build().model_dump(by_alias=True, mode="json")["vectorSearch"] == {"index": "vidx", "vector": [1.0, 2.0], "limit": 3}


def test_query_terminals_mutually_exclusive_with_get():
    with pytest.raises(ValueError):
        TableQuery("t").get("x").take(5).build()


def test_parse_result_doc_docs_count_paginated():
    doc = parse_result(Box, "get", {"id": "1", "status": "a"})
    assert isinstance(doc, Box) and doc.id == "1"
    assert parse_result(Box, "get", None) is None
    docs = parse_result(Box, "collect", [{"id": "1", "status": "a"}])
    assert docs == [Box(id="1", status="a")]
    assert parse_result(Box, "count", 7) == 7
    first = parse_result(Box, "first", {"id": "9", "status": "a"})
    assert isinstance(first, Box) and first.id == "9"
    page = parse_result(Box, "paginate", {"docs": [{"id": "1", "status": "a"}], "nextCursor": "C"})
    assert isinstance(page, Paginated) and len(page.docs) == 1 and page.next_cursor == "C"
    page_end = parse_result(Box, "paginate", {"docs": []})
    assert page_end.next_cursor is None


def test_parse_result_dict_model_returns_raw_dicts():
    out = parse_result(dict, "collect", [{"id": "1"}])
    assert out == [{"id": "1"}]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd python-client && uv run pytest tests/test_query.py -q`
Expected: FAIL.

- [ ] **Step 3: Write `src/par_rt_db/query.py`**

```python
"""Query DSL: wire Query model, TableQuery builder, and QueryResult parsing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, TypeAdapter

from .cursor import encode_cursor
from .wire import FilterExpr, SearchQuery, VectorSearchQuery

# Query wire fields are snake_case (NOT camelCase) and all optional.
class Query(BaseModel):
    """A read query. Wire field names are snake_case; all fields optional."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    table: str
    get: str | None = None
    index: str | None = None
    eq: list[Any] | None = None
    gt: Any | None = None
    gte: Any | None = None
    lt: Any | None = None
    lte: Any | None = None
    order: Literal["asc", "desc"] | None = None
    take: int | None = None
    unique: bool | None = None
    first: bool | None = None
    count: bool | None = None
    filter: FilterExpr | None = None
    search: SearchQuery | None = None
    vector_search: VectorSearchQuery | None = None
    paginate: "_Paginate | None" = None

    def model_dump(self, **kw: Any) -> dict[str, Any]:  # type: ignore[override]
        out = super().model_dump(**kw)
        # Drop Nones to match the server's all-optional, omit-when-absent shape.
        return {k: v for k, v in out.items() if v is not None}


class _Paginate(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True, alias_generator=__import__("par_rt_db.wire", fromlist=["to_camel"]).to_camel)
    cursor: str | None = None
    num_items: int


@dataclass
class Paginated[T]:
    """A page of results: docs + an opaque next-page cursor (None when exhausted)."""

    docs: list[T]
    next_cursor: str | None


_TERMINAL_GET = "get"
_TERMINAL_RANGE_OK = {"collect", "take", "first", "unique", "count", "paginate"}


class TableQuery:
    """Fluent builder producing a wire ``Query``. Terminal-aware."""

    def __init__(self, table: str) -> None:
        self._table = table
        self._index: str | None = None
        self._eq: list[Any] | None = None
        self._gt = self._gte = self._lt = self._lte = None
        self._order: Literal["asc", "desc"] | None = None
        self._take: int | None = None
        self._unique = self._first = self._count = False
        self._get: str | None = None
        self._filter: FilterExpr | None = None
        self._search: SearchQuery | None = None
        self._vector: VectorSearchQuery | None = None
        self._paginate: _Paginate | None = None

    # --- builder methods (return self) ---
    def get(self, id_: str) -> "TableQuery":
        self._get = id_
        return self

    def with_index(self, index: str) -> "TableQuery":
        self._index = index
        return self

    def eq(self, *values: Any) -> "TableQuery":
        self._eq = list(values)
        return self

    def gt(self, v: Any) -> "TableQuery":
        self._gt = v
        return self

    def gte(self, v: Any) -> "TableQuery":
        self._gte = v
        return self

    def lt(self, v: Any) -> "TableQuery":
        self._lt = v
        return self

    def lte(self, v: Any) -> "TableQuery":
        self._lte = v
        return self

    def order(self, direction: Literal["asc", "desc"]) -> "TableQuery":
        self._order = direction
        return self

    def take(self, n: int) -> "TableQuery":
        self._take = n
        return self

    def filter(self, f: FilterExpr) -> "TableQuery":
        self._filter = f
        return self

    def search(self, index: str, query: str) -> "TableQuery":
        self._search = SearchQuery.model_validate({"index": index, "query": query})
        return self

    def vector_search(self, index: str, vector: list[float], *, limit: int, filter_: FilterExpr | None = None) -> "TableQuery":
        payload: dict[str, Any] = {"index": index, "vector": vector, "limit": limit}
        if filter_ is not None:
            payload["filter"] = filter_
        self._vector = VectorSearchQuery.model_validate(payload)
        return self

    # --- terminals ---
    def collect(self) -> "TableQuery":
        return self

    def unique(self) -> "TableQuery":
        self._unique = True
        return self

    def first(self) -> "TableQuery":
        self._first = True
        return self

    def count(self) -> "TableQuery":
        self._count = True
        return self

    def paginate(self, *, cursor: str | None = None, num_items: int) -> "TableQuery":
        self._paginate = _Paginate.model_validate(
            {"cursor": cursor, "numItems": num_items}
        )
        return self

    # --- internal build helpers (for tests) ---
    def _terminal(self) -> str:
        if self._get is not None:
            return "get"
        if self._count:
            return "count"
        if self._first:
            return "first"
        if self._unique:
            return "unique"
        if self._paginate is not None:
            return "paginate"
        return "collect"

    def build(self) -> Query:
        if self._get is not None and (
            self._take is not None or self._unique or self._first or self._count
            or self._paginate is not None
        ):
            raise ValueError("get is mutually exclusive with take/unique/first/count/paginate")
        payload: dict[str, Any] = {"table": self._table}
        if self._get is not None:
            payload["get"] = self._get
        if self._index is not None:
            payload["index"] = self._index
        if self._eq is not None:
            payload["eq"] = self._eq
        if self._gt is not None:
            payload["gt"] = self._gt
        if self._gte is not None:
            payload["gte"] = self._gte
        if self._lt is not None:
            payload["lt"] = self._lt
        if self._lte is not None:
            payload["lte"] = self._lte
        if self._order is not None:
            payload["order"] = self._order
        if self._take is not None:
            payload["take"] = self._take
        if self._unique:
            payload["unique"] = True
        if self._first:
            payload["first"] = True
        if self._count:
            payload["count"] = True
        if self._filter is not None:
            payload["filter"] = self._filter
        if self._search is not None:
            payload["search"] = self._search
        if self._vector is not None:
            payload["vectorSearch"] = self._vector
        if self._paginate is not None:
            payload["paginate"] = self._paginate
        return Query.model_validate(payload)

    # test affordances mirroring rust's typed terminals
    def build_for_count(self) -> Query:
        self._count = True
        return self.build()

    def build_for_first(self) -> Query:
        self._first = True
        return self.build()

    def build_for_unique(self) -> Query:
        self._unique = True
        return self.build()


def parse_result(model: type, terminal: str, value: Any) -> Any:
    """Deserialize an untagged QueryResult by the terminal that produced it."""
    if terminal == "get":
        return None if value is None else _coerce(model, value)
    if terminal == "collect":
        return [_coerce(model, v) for v in value]
    if terminal in ("first", "unique"):
        return None if value is None else _coerce(model, value)
    if terminal == "count":
        return int(value)
    if terminal == "paginate":
        docs = [_coerce(model, v) for v in value.get("docs", [])]
        nxt = value.get("nextCursor")
        return Paginated(docs=docs, next_cursor=nxt)  # type: ignore[type-var]
    raise ValueError(f"unknown terminal: {terminal}")


def _coerce(model: type, value: Any) -> Any:
    if model is dict:
        return dict(value)
    if isinstance(model, type) and issubclass(model, BaseModel):
        return model.model_validate(value)
    adapter = TypeAdapter(model)
    return adapter.validate_python(value)


# Resolve forward references used by wire.py.
import par_rt_db.wire as _wire  # noqa: E402

Query.model_rebuild()
_Paginate.model_rebuild()
```

> **Implementer note:** `model_dump` is overridden on `Query` to drop `None` values (the server's query is all-optional and omits absent fields). The `dataclass[T]` generic (`Paginated[T]`) needs Python 3.12 PEP 695 generics — fine on the 3.12 floor. If `dataclass[T]` syntax errors, use `Generic[T]` explicitly. Resolve `Query`/`Transaction` forward refs in `wire.py` by calling `wire._ClientSubscribe.model_rebuild()` etc. at import time (or rely on Pydantic's lazy rebuild). The Task 6 parity tests confirm the resolution worked.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd python-client && uv run pytest tests/test_query.py tests/test_wire.py tests/test_wire_parity.py -q`
Expected: PASS (and wire parity still green after `Query` is concrete).

- [ ] **Step 5: Commit**

```bash
git add python-client/src/par_rt_db/query.py python-client/tests/test_query.py
git commit -m "feat(python-client): query DSL — TableQuery builder + QueryResult parse"
```

---

### Task 10: mutation.py — Step (7 ops) + StepResult + Mutation builder

**Files:**
- Create: `python-client/src/par_rt_db/mutation.py`
- Test: `python-client/tests/test_mutation.py`
- Produces: `Step` (7-variant union, discriminator `"op"`, camelCase, `extra="forbid"`), `StepResult` (untagged: `{id}` insert / `{id, inserted}` upsert / `None`), `Mutation`/`MutationBuilder`. Resolves the `Transaction` forward ref in `wire.py`.

- [ ] **Step 1: Write the failing test**

`tests/test_mutation.py`:
```python
import json

import pytest
from pydantic import ValidationError

from par_rt_db.mutation import Mutation, StepResult, Transaction


def test_insert_patch_replace_delete_upsert_wire():
    m = (
        Mutation.builder()
        .insert("boxes", {"status": "active"})
        .patch("boxes", "b1", {"status": "idle"})
        .replace("boxes", "b1", {"status": "idle", "owner": "p"})
        .delete("boxes", "b1")
        .upsert("boxes", "by_owner", ["p1"], {"status": "active"}, {"status": "idle"})
        .build()
    )
    wire = json.loads(m.model_dump_json(by_alias=True))
    assert wire["steps"][0] == {"op": "insert", "table": "boxes", "doc": {"status": "active"}}
    assert wire["steps"][1] == {"op": "patch", "table": "boxes", "id": "b1", "fields": {"status": "idle"}}
    assert wire["steps"][2] == {"op": "replace", "table": "boxes", "id": "b1", "doc": {"status": "idle", "owner": "p"}}
    assert wire["steps"][3] == {"op": "delete", "table": "boxes", "id": "b1"}
    assert wire["steps"][4] == {
        "op": "upsert", "table": "boxes", "index": "by_owner", "eq": ["p1"],
        "insert": {"status": "active"}, "patch": {"status": "idle"},
    }


def test_expect_version_and_expect_absent():
    m = (
        Mutation.builder()
        .expect_version("boxes", "b1", 7)
        .expect_absent("boxes", "by_owner", ["p9"])
        .build()
    )
    wire = json.loads(m.model_dump_json(by_alias=True))
    assert wire["steps"][0] == {"op": "expectVersion", "table": "boxes", "id": "b1", "version": 7}
    assert wire["steps"][1] == {"op": "expectAbsent", "table": "boxes", "index": "by_owner", "eq": ["p9"]}


def test_step_rejects_unknown():
    with pytest.raises(ValidationError):
        Transaction.model_validate({"steps": [{"op": "bogus"}]})


def test_step_result_variants():
    assert StepResult.model_validate({"id": "x"}).model_dump(by_alias=True, mode="json") == {"id": "x"}
    assert StepResult.model_validate({"id": "x", "inserted": True}).model_dump(by_alias=True, mode="json") == {"id": "x", "inserted": True}
    assert StepResult.model_validate(None) is None


def test_transaction_max_steps_enforced_client_side():
    b = Mutation.builder()
    for _ in range(256):
        b.delete("t", "x")
    b.delete("t", "x")  # 257th
    with pytest.raises(ValueError):
        b.build()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd python-client && uv run pytest tests/test_mutation.py -q`
Expected: FAIL.

- [ ] **Step 3: Write `src/par_rt_db/mutation.py`**

```python
"""Transaction DSL: Step (7 ops), StepResult, Mutation builder."""

from __future__ import annotations

from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, ModelSerialize

from .wire import to_camel

MAX_STEPS = 256


class _M(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True, alias_generator=to_camel)


class _Insert(_M):
    op: Literal["insert"] = "insert"
    table: str
    doc: dict[str, Any]


class _Patch(_M):
    op: Literal["patch"] = "patch"
    table: str
    id: str
    fields: dict[str, Any]


class _Replace(_M):
    op: Literal["replace"] = "replace"
    table: str
    id: str
    doc: dict[str, Any]


class _Delete(_M):
    op: Literal["delete"] = "delete"
    table: str
    id: str


class _ExpectVersion(_M):
    op: Literal["expectVersion"] = "expectVersion"
    table: str
    id: str
    version: int


class _ExpectAbsent(_M):
    op: Literal["expectAbsent"] = "expectAbsent"
    table: str
    index: str
    eq: list[Any]


class _Upsert(_M):
    op: Literal["upsert"] = "upsert"
    table: str
    index: str
    eq: list[Any]
    insert: dict[str, Any]
    patch: dict[str, Any]


Step = Annotated[
    Union[_Insert, _Patch, _Replace, _Delete, _ExpectVersion, _ExpectAbsent, _Upsert],
    Field(discriminator="op"),
]


class Transaction(BaseModel):
    """A transaction: up to 256 steps."""

    model_config = ConfigDict(extra="forbid")
    steps: list[Step]


class _StepInsert(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True, alias_generator=to_camel)
    id: str


class _StepUpsert(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True, alias_generator=to_camel)
    id: str
    inserted: bool


# Order matters: upsert (richer) must win over insert when both could match.
StepResult = Union[_StepUpsert, _StepInsert, None]


class _MutationBuilder:
    def __init__(self) -> None:
        self._steps: list[Step] = []  # type: ignore[type-arg]

    def insert(self, table: str, doc: dict[str, Any]) -> "_MutationBuilder":
        self._steps.append(_Insert(table=table, doc=doc))  # type: ignore[arg-type]
        return self

    def patch(self, table: str, id: str, fields: dict[str, Any]) -> "_MutationBuilder":
        self._steps.append(_Patch(table=table, id=id, fields=fields))  # type: ignore[arg-type]
        return self

    def replace(self, table: str, id: str, doc: dict[str, Any]) -> "_MutationBuilder":
        self._steps.append(_Replace(table=table, id=id, doc=doc))  # type: ignore[arg-type]
        return self

    def delete(self, table: str, id: str) -> "_MutationBuilder":
        self._steps.append(_Delete(table=table, id=id))  # type: ignore[arg-type]
        return self

    def expect_version(self, table: str, id: str, version: int) -> "_MutationBuilder":
        self._steps.append(_ExpectVersion(table=table, id=id, version=version))  # type: ignore[arg-type]
        return self

    def expect_absent(self, table: str, index: str, eq: list[Any]) -> "_MutationBuilder":
        self._steps.append(_ExpectAbsent(table=table, index=index, eq=eq))  # type: ignore[arg-type]
        return self

    def upsert(
        self, table: str, index: str, eq: list[Any],
        insert: dict[str, Any], patch: dict[str, Any],
    ) -> "_MutationBuilder":
        self._steps.append(_Upsert(table=table, index=index, eq=eq, insert=insert, patch=patch))  # type: ignore[arg-type]
        return self

    def build(self) -> Transaction:
        if len(self._steps) > MAX_STEPS:
            raise ValueError(f"transaction exceeds max {MAX_STEPS} steps")
        return Transaction(steps=self._steps)


class _MutationNamespace:
    builder = staticmethod(_MutationBuilder)
    model_validate = staticmethod(Transaction.model_validate)


Mutation = _MutationNamespace

Transaction.model_rebuild()
```

- [ ] **Step 4: Resolve wire.py forward refs and run all tests**

Append to `src/par_rt_db/__init__.py` (or the bottom of `wire.py`) a rebuild call so `wire.ClientMessage` resolves `Query`/`Transaction`:

`src/par_rt_db/__init__.py`:
```python
"""par-rt-db Python client (core: wire + DSL). Clients land in a later plan."""

from par_rt_db import query as _query  # noqa: F401
from par_rt_db import mutation as _mutation  # noqa: F401
from par_rt_db import wire as _wire  # noqa: F401

# Resolve cross-module forward references in wire models.
for _m in (
    _wire._ClientSubscribe, _wire._ClientMutate, _wire._ClientSchedule,
):
    _m.model_rebuild()
```

Run: `cd python-client && uv run pytest -q && uv run ruff check . && uv run pyright`
Expected: all tests PASS (incl. wire parity), ruff clean, pyright clean.

- [ ] **Step 5: Commit**

```bash
git add python-client/src/par_rt_db/mutation.py python-client/src/par_rt_db/__init__.py python-client/tests/test_mutation.py
git commit -m "feat(python-client): mutation DSL — Step(7), StepResult, Mutation builder"
```

---

### Task 11: Root Makefile + `make checkall` wiring

**Files:**
- Modify: `Makefile` (repo root) — add `python-client-*` targets and extend `checkall`/`typecheck`/`test`/`lint`/`fmt`.
- Verify the full repo gate stays green.

**Interfaces:**
- Produces: `make checkall` now runs server + ts-client + rust-client + dashboard + python-client.

- [ ] **Step 1: Inspect the root Makefile and add python-client targets**

Read `Makefile`. Add (mirroring the ts/rust-client patterns):

```makefile
python-client-install:
	cd python-client && uv sync --extra dev

python-client-test:
	cd python-client && uv run pytest -q

python-client-lint:
	cd python-client && uv run ruff check .

python-client-fmt:
	cd python-client && uv run ruff format .

python-client-typecheck:
	cd python-client && uv run pyright

python-client-checkall: python-client-fmt python-client-lint python-client-typecheck python-client-test
```

Extend the existing aggregate targets (`.PHONY` line, `fmt`/`fmt-check`/`lint`/`typecheck`/`test`/`checkall`) to include the `python-client-*` steps. Mirror exactly how `ts-client`/`rust-client` are composed.

- [ ] **Step 2: Run the full repo gate**

Run: `make checkall` (from repo root; needs `make dev-db-up` for server tests — `make test` handles it)
Expected: every stage green — fmt-check, clippy, typecheck (server/ts/rust/dashboard), server tests, ts-client tests, rust-client tests, dashboard, **and** python-client (ruff + pyright + pytest).

- [ ] **Step 3: Commit**

```bash
git add Makefile
git commit -m "build: wire python-client into root make checkall"
```

---

## Self-Review (completed by plan author)

- **Spec coverage:** wire types (spec §"Wire contract") → Tasks 3–6; errors → Task 2; schema DSL → Task 8; query DSL + QueryResult → Task 9; mutation DSL → Task 10; cursor → Task 7; Makefile/pre-commit/packaging → Tasks 1, 11. HTTP/WS/admin/in-memory are explicitly Plan 2 (out of this plan's scope) — covered by the spec's phasing §7.3–7.6, not gaps.
- **Placeholder scan:** the `IndexDef._serialize_cls` stub and the `__import__` alias-generator line in `_Paginate` are implementation smells flagged with implementer notes — the Task 8/9 tests assert exact wire shapes, so they must be resolved to pass; no "TODO" left as a deliverable.
- **Type consistency:** `TableQuery` methods (`with_index`/`eq`/`gt`/`gte`/`lt`/`lte`/`order`/`take`/`collect`/`unique`/`first`/`count`/`paginate`/`filter`/`search`/`vector_search`/`build`) and `Mutation.builder()` methods (`insert`/`patch`/`replace`/`delete`/`expect_version`/`expect_absent`/`upsert`/`build`) are used consistently across their test + task. `parse_result` terminals match the `TableQuery._terminal()` set. `Schema.builder()` / `t.*` names match the spec DSL example.
- **Known follow-ups for the implementer:** (a) confirm `ModelSerialize` vs `model_serializer` against the installed Pydantic; (b) confirm PEP 695 `dataclass[T]` on the 3.12 floor (fall back to `Generic[T]`); (c) resolve wire forward refs at import (Task 10 step 4). All three are gated by tests — fix until green.

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-07-25-python-client-core.md`. Per the user's standing preference, execution is **Subagent-Driven** (fresh subagent per task + two-stage review). Plan 2 (HTTP/WS/admin/in-memory clients) follows once Plan 1 lands.
