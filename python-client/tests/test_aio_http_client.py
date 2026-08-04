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
)
from par_rt_db.aio_http_client import RtDbAsyncHttpClient

BEARER = "Bearer machine-token"
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


# --- data plane: query ------------------------------------------------------


async def test_run_collect_posts_query_and_parses_list_result() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["request"] = request
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={"result": [{"_id": "a", "n": 1}, {"_id": "b", "n": 2}]},
        )

    async with _client(handler) as c:
        q = TableQuery("items").with_index("by_status").eq("active").take(2)
        got: list[dict[str, Any]] = await c.run(q)
    assert isinstance(got, list)
    assert len(got) == 2
    # route + bearer
    assert captured["request"].method == "POST"
    assert captured["request"].url.path == "/api/query"
    assert captured["request"].headers["authorization"] == BEARER
    # body shape: {db, query: {...}} with snake_case Query keys
    assert captured["body"]["db"] == DB
    assert captured["body"]["query"]["table"] == "items"
    assert captured["body"]["query"]["index"] == "by_status"
    assert captured["body"]["query"]["eq"] == ["active"]
    assert captured["body"]["query"]["take"] == 2


async def test_run_count_parses_int() -> None:
    async with _client(
        _handler_map({("POST", "/api/query", ""): httpx.Response(200, json={"result": 5})})
    ) as c:
        n: int = await c.run(TableQuery("items").count())
    assert n == 5


def test_query_alias_matches_run() -> None:
    assert RtDbAsyncHttpClient.query is RtDbAsyncHttpClient.run


async def test_get_returns_optional_doc() -> None:
    async with _client(
        _handler_map(
            {("POST", "/api/query", '"get"'): httpx.Response(200, json={"result": {"_id": "a"}})}
        )
    ) as c:
        some: dict[str, Any] | None = await c.get("items", "a")
    assert some == {"_id": "a"}


async def test_get_miss_returns_none() -> None:
    async with _client(
        _handler_map({("POST", "/api/query", ""): httpx.Response(200, json={"result": None})})
    ) as c:
        assert await c.get("items", "missing") is None


async def test_find_one_by_index_hit_returns_doc() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"result": {"_id": "u1", "email": "a@b.com"}})

    async with _client(handler) as c:
        got: dict[str, Any] | None = await c.find_one_by_index("users", "by_email", "a@b.com")
    assert got == {"_id": "u1", "email": "a@b.com"}
    # first terminal is set on the wire
    assert captured["body"]["query"]["first"] is True
    assert captured["body"]["query"]["eq"] == ["a@b.com"]


async def test_run_parses_first_terminal_none() -> None:
    async with _client(
        _handler_map({("POST", "/api/query", ""): httpx.Response(200, json={"result": None})})
    ) as c:
        assert await c.run(TableQuery("items").with_index("i").eq("a").first()) is None


# --- data plane: mutate -----------------------------------------------------


async def test_mutate_posts_and_parses_step_results() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"results": [{"id": "new1"}, None]})

    async with _client(handler) as c:
        txn = (
            Mutation.builder().insert("items", {"name": "x"}).patch("items", "i1", {"y": 1}).build()
        )
        res = await c.mutate(txn)
    assert len(res) == 2
    # ``{id}`` with no ``inserted`` parses as the Insert variant.
    from par_rt_db.mutation import _StepInsert

    assert isinstance(res[0], _StepInsert)
    assert res[0].id == "new1"
    assert res[1] is None
    # body shape: {db, txn: {steps: [...]}}; no idempotencyKey when None.
    assert captured["body"]["db"] == DB
    assert "idempotencyKey" not in captured["body"]
    assert captured["body"]["txn"]["steps"][0]["op"] == "insert"


async def test_mutate_sends_idempotency_key_when_given() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"results": []})

    async with _client(handler) as c:
        txn = Mutation.builder().delete("items", "i1").build()
        await c.mutate(txn, idempotency_key="k1")
    assert captured["body"]["idempotencyKey"] == "k1"


async def test_upsert_by_index_inserts_when_no_match() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={"results": [{"id": "new1", "inserted": True}]},
        )

    async with _client(handler) as c:
        res = await c.upsert_by_index(
            "users", "by_email", "a@b.com", {"email": "a@b.com"}, {"n": 1}
        )
    from par_rt_db.mutation import _StepUpsert

    assert isinstance(res, _StepUpsert)
    assert res.id == "new1"
    assert res.inserted is True
    # the one-step txn uses the upsert op
    step = captured["body"]["txn"]["steps"][0]
    assert step["op"] == "upsert"
    assert step["eq"] == ["a@b.com"]


# --- data plane: scheduling + batch query -----------------------------------


async def test_schedule_posts_when_and_txn_returns_id() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"id": "sch-1"})

    from par_rt_db.wire import _AfterMs

    async with _client(handler) as c:
        txn = Mutation.builder().insert("items", {"name": "x"}).build()
        sid = await c.schedule(txn, _AfterMs(ms=5000))
    assert sid == "sch-1"
    assert captured["body"]["db"] == DB
    assert captured["body"]["when"] == {"type": "afterMs", "ms": 5000}
    assert captured["body"]["txn"]["steps"][0]["op"] == "insert"


async def test_schedule_manage_ops_post_to_id_paths() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        return httpx.Response(200, json={"ok": True})

    async with _client(handler) as c:
        await c.cancel_schedule("sch-1")
        await c.pause_schedule("sch-1")
        await c.resume_schedule("sch-1")
    assert seen == [
        "/api/schedule/sch-1/cancel",
        "/api/schedule/sch-1/pause",
        "/api/schedule/sch-1/resume",
    ]


async def test_list_schedules_returns_schedule_info_list() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert json.loads(request.content) == {"db": DB}
        return httpx.Response(
            200,
            json={
                "schedules": [
                    {
                        "id": "sch-1",
                        "kind": "oneshot",
                        "dueAt": 1000,
                        "status": "pending",
                        "createdAt": 500,
                        "firedCount": 0,
                    },
                    {
                        "id": "sch-2",
                        "kind": "cron",
                        "dueAt": 2000,
                        "status": "running",
                        "cron": "0 9 * * *",
                        "createdAt": 500,
                        "firedCount": 3,
                    },
                ]
            },
        )

    async with _client(handler) as c:
        schedules = await c.list_schedules()
    assert [s.id for s in schedules] == ["sch-1", "sch-2"]
    assert schedules[0].kind == "oneshot"
    assert schedules[0].fired_count == 0
    assert schedules[1].cron == "0 9 * * *"


async def test_batch_query_returns_one_outcome_per_input() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "results": [
                    {"ok": True, "result": [{"_id": "i1"}]},
                    {"ok": False, "error": {"code": "NOT_FOUND", "message": "no index"}},
                ]
            },
        )

    async with _client(handler) as c:
        outcomes = await c.batch_query([TableQuery("items").collect(), TableQuery("items").first()])
    assert captured["body"]["db"] == DB
    assert len(captured["body"]["queries"]) == 2
    assert len(outcomes) == 2
    assert outcomes[0].ok is True
    assert outcomes[0].result == [{"_id": "i1"}]
    assert outcomes[1].ok is False
    assert outcomes[1].error is not None
    assert outcomes[1].error.code == "NOT_FOUND"


# --- error envelope ---------------------------------------------------------


async def test_error_envelope_becomes_rtdb_error() -> None:
    async with _client(
        _handler_map(
            {
                ("POST", "/api/query", ""): httpx.Response(
                    409,
                    json={"code": "PRECONDITION_FAILED", "message": "version mismatch"},
                )
            }
        )
    ) as c:
        with pytest.raises(RtDbError) as ei:
            await c.run(TableQuery("items").count())
    err = ei.value
    assert err.code.value == "PRECONDITION_FAILED"
    assert err.message == "version mismatch"
    assert err.status_code == 409


async def test_non_envelope_error_is_internal_with_status_in_message() -> None:
    async with _client(
        _handler_map({("POST", "/api/query", ""): httpx.Response(500, text="gateway down")})
    ) as c:
        with pytest.raises(RtDbError) as ei:
            await c.run(TableQuery("items").count())
    assert err_internal(ei.value)
    assert "500" in ei.value.message


def err_internal(err: RtDbError) -> bool:
    from par_rt_db.errors import ErrorCode

    return err.code is ErrorCode.INTERNAL


# --- lifecycle --------------------------------------------------------------


async def test_context_manager_closes_client() -> None:
    client = _client(lambda r: httpx.Response(200, json={"result": 0}))
    async with client as c:
        assert c is client
        assert await c.run(TableQuery("items").count()) == 0
    # After exit the underlying httpx.AsyncClient is closed; further requests raise.
    with pytest.raises(RuntimeError):
        await client.run(TableQuery("items").count())
