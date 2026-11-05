"""Transport-agnostic admin request layer (ARC-108), split from ``admin.py``.

The :class:`_AdminRequest` description every admin operation builds, the shared
response ``_parse_*`` helpers, and the sync/async executors that perform the
HTTP call. ``par_rt_db.admin`` imports and re-exports every name defined here,
so the ``par_rt_db.admin`` attribute surface is unchanged (QA-003b pure move).
"""

from __future__ import annotations

from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Callable,
    Generator,
    Iterator,
)
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from .admin_models import _STEP_RESULT_ADAPTER
from .errors import ErrorCode, RtDbError
from .mutation import StepResult

if TYPE_CHECKING:
    import httpx


# ---------------------------------------------------------------------------
#
# An ``_AdminRequest`` is a transport-agnostic description of one admin HTTP
# call: the method, path, per-request kwargs (``json``/``params``/``content``/
# ``headers``), and a ``parse`` closure that turns the successful response into
# the method's return value. The sync/async executors below perform the actual
# HTTP; the builders + parse closures are shared, collapsing the former
# four-way duplication of the admin surface.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _AdminRequest:
    """One admin HTTP call, described independent of sync/async transport."""

    method: str
    path: str
    request_kwargs: dict[str, Any]
    parse: Callable[[httpx.Response], Any]
    # Raw-body ops (binary dump download, JSONL export) legitimately answer
    # with a non-JSON 2xx body; the executors skip the JSON-object guard for
    # them. Void routes need no flag — their body-less 202/204 statuses are
    # exempted directly.
    raw_body: bool = False


# --- response parse helpers (shared by every builder of the same shape) ---


def _parse_none(resp: httpx.Response) -> None:
    """No body interpretation — fire-and-forget (e.g. ``DELETE /admin/backups/{n}``)."""
    return None


def _parse_expect_ok(resp: httpx.Response) -> None:
    """Assert the body is ``{ok: true}``; raise :class:`RtDbError` otherwise."""
    if not resp.json().get("ok"):
        raise RtDbError(ErrorCode.INTERNAL, "admin request returned ok=false")


def _parse_text(resp: httpx.Response) -> str:
    return resp.text


def _parse_bytes(resp: httpx.Response) -> bytes:
    return resp.content


def _parse_json_object(resp: httpx.Response) -> dict[str, Any]:
    return dict(resp.json())


def _parse_databases(resp: httpx.Response) -> list[str]:
    return list(resp.json()["databases"])


def _parse_emails(resp: httpx.Response) -> list[str]:
    return list(resp.json()["emails"])


def _parse_revoked_count(resp: httpx.Response) -> int:
    return int(resp.json()["revoked"])


def _parse_restored_version(resp: httpx.Response) -> int:
    return int(resp.json()["restoredTo"])


def _parse_webhook_id(resp: httpx.Response) -> int:
    return int(resp.json()["id"])


def _parse_ok_bool(resp: httpx.Response) -> bool:
    """Read a ``{ok: bool}`` body as a return value — for routes where
    ``ok:false`` is a legitimate outcome (e.g. cancelling/deleting a missing or
    already-terminal workflow run), not an error."""
    return bool(resp.json()["ok"])


def _parse_step_results(resp: httpx.Response) -> list[StepResult]:
    return [_STEP_RESULT_ADAPTER.validate_python(r) for r in resp.json()["results"]]


def _parse_model[T: BaseModel](model_cls: type[T]) -> Callable[[httpx.Response], T]:
    """``model_cls.model_validate(resp.json())`` as a parse closure."""

    def parse(resp: httpx.Response) -> T:
        return model_cls.model_validate(resp.json())

    return parse


def _parse_model_list[T: BaseModel](
    model_cls: type[T], key: str
) -> Callable[[httpx.Response], list[T]]:
    """``[model_cls.model_validate(x) for x in resp.json()[key]]`` as a parse closure."""

    def parse(resp: httpx.Response) -> list[T]:
        return [model_cls.model_validate(x) for x in resp.json()[key]]

    return parse


def _require_json_object(resp: httpx.Response, path: str) -> None:
    """Raise :class:`RtDbError` unless a 2xx ``resp`` carries a JSON object body.

    ts/rust/swift parity: an unparseable 2xx body (empty body, HTML gateway
    page, invalid JSON) must surface as ``RtDbError`` INTERNAL naming the
    route — otherwise the caller's ``resp.json()[...]`` raises a raw
    ``JSONDecodeError`` far from the request that caused it. Shared by the
    admin executors and both data-plane ``_send`` helpers.
    """
    try:
        body = resp.json()
    except ValueError as e:
        raise RtDbError(ErrorCode.INTERNAL, f"{path} returned 2xx with no JSON object body") from e
    if not isinstance(body, dict):
        raise RtDbError(ErrorCode.INTERNAL, f"{path} returned 2xx with no JSON object body")


# ---------------------------------------------------------------------------
# Executors — one sync, one async. Each performs the HTTP call described by an
# ``_AdminRequest`` and hands the successful response to its ``parse`` closure.
# Non-2xx raises ``RtDbError`` exactly as the former per-class ``_send``/``_req``
# helpers did. These are the only place sync vs async differ.
# ---------------------------------------------------------------------------


class _SyncAdminExecutor:
    """Sync admin request executor over a long-lived :class:`httpx.Client`.

    The bearer header is set on the underlying client at construction time (by
    the owning façade), so the executor passes only the request description
    through.
    """

    def __init__(self, client: httpx.Client) -> None:
        self._client = client

    def run(self, req: _AdminRequest) -> Any:
        resp = self._client.request(req.method, req.path, **req.request_kwargs)
        if not resp.is_success:
            raise RtDbError.from_http(resp.status_code, resp.content)
        # 202 (backupNow) and 204 (backup delete) carry no body; raw_body ops
        # (dump download, export) carry non-JSON bodies. Every other 2xx must
        # be a JSON object — ts admin parity.
        if not req.raw_body and resp.status_code not in (202, 204):
            _require_json_object(resp, f"admin request to {req.path}")
        return req.parse(resp)

    @contextmanager
    def stream(self, req: _AdminRequest) -> Generator[Iterator[bytes]]:
        """Open ``req`` as a streamed response and yield its body chunks.

        A non-2xx status is read in full and raised as :class:`RtDbError`
        before anything is yielded; the connection closes on context exit.
        """
        with self._client.stream(req.method, req.path, **req.request_kwargs) as resp:
            if not resp.is_success:
                resp.read()
                raise RtDbError.from_http(resp.status_code, resp.content)
            yield resp.iter_bytes()


class _AsyncAdminExecutor:
    """Async admin request executor over a long-lived :class:`httpx.AsyncClient`.

    Async twin of :class:`_SyncAdminExecutor`; behavior is identical apart from
    the ``await``.
    """

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def run(self, req: _AdminRequest) -> Any:
        resp = await self._client.request(req.method, req.path, **req.request_kwargs)
        if not resp.is_success:
            raise RtDbError.from_http(resp.status_code, resp.content)
        # Same guard as the sync executor: body-less 202/204 and raw-body ops
        # are exempt; every other 2xx must be a JSON object.
        if not req.raw_body and resp.status_code not in (202, 204):
            _require_json_object(resp, f"admin request to {req.path}")
        return req.parse(resp)

    @asynccontextmanager
    async def stream(self, req: _AdminRequest) -> AsyncGenerator[AsyncIterator[bytes]]:
        """Async twin of :meth:`_SyncAdminExecutor.stream`."""
        async with self._client.stream(req.method, req.path, **req.request_kwargs) as resp:
            if not resp.is_success:
                await resp.aread()
                raise RtDbError.from_http(resp.status_code, resp.content)
            yield resp.aiter_bytes()
