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

    from par_rt_db.wire import AfterMs

    async with _client(handler) as c:
        txn = Mutation.builder().insert("items", {"name": "x"}).build()
        sid = await c.schedule(txn, AfterMs(ms=5000))
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
        assert await c.cancel_schedule("sch-1") is True
        assert await c.pause_schedule("sch-1") is True
        assert await c.resume_schedule("sch-1") is True
    assert seen == [
        "/api/schedule/sch-1/cancel",
        "/api/schedule/sch-1/pause",
        "/api/schedule/sch-1/resume",
    ]


async def test_schedule_manage_op_no_op_resolves_false() -> None:
    # 200 {ok: false} = unknown/terminal id: a no-op that resolves False —
    # same contract as the WS scheduleAck bare ok:false arm, never a raise.
    async with _client(lambda r: httpx.Response(200, json={"ok": False})) as c:
        assert await c.cancel_schedule("sch-gone") is False
        assert await c.pause_schedule("sch-gone") is False
        assert await c.resume_schedule("sch-gone") is False


async def test_schedule_manage_op_error_envelope_raises() -> None:
    async with _client(
        lambda r: httpx.Response(404, json={"code": "NOT_FOUND", "message": "missing job"})
    ) as c:
        with pytest.raises(RtDbError) as ei:
            await c.cancel_schedule("sch-1")
    assert ei.value.code.value == "NOT_FOUND"
    assert ei.value.status_code == 404


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


# --- storage ----------------------------------------------------------------


async def test_upload_posts_raw_bytes_and_returns_metadata() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["content"] = request.content
        captured["headers"] = dict(request.headers)
        return httpx.Response(
            200,
            json={"id": "f1", "sha256": "abc", "size": 9, "contentType": "image/png"},
        )

    async with _client(handler) as c:
        up = await c.upload(b"raw-bytes", content_type="image/png")
    assert isinstance(up, UploadResult)
    assert up.id == "f1"
    assert up.size == 9
    assert up.content_type == "image/png"
    # raw body (NOT json-wrapped)
    assert captured["content"] == b"raw-bytes"
    assert captured["headers"]["content-type"] == "image/png"
    assert captured["headers"]["authorization"] == BEARER


async def test_upload_streams_file_like_object() -> None:
    """ENH-021: an ``io.BytesIO`` (file-like) round-trips and streams."""
    import io

    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["content"] = request.content
        captured["headers"] = dict(request.headers)
        return httpx.Response(
            200,
            json={"id": "f2", "sha256": "dead", "size": 23, "contentType": "image/png"},
        )

    async with _client(handler) as c:
        up = await c.upload(io.BytesIO(b"file-like-streamed-body"), content_type="image/png")
    assert isinstance(up, UploadResult)
    assert up.id == "f2"
    assert up.size == 23
    assert captured["content"] == b"file-like-streamed-body"
    assert captured["headers"]["content-type"] == "image/png"
    # The async client adapts sync file-likes into an async generator, so httpx
    # uses chunked transfer-encoding (no Content-Length) — bytes still arrive whole.
    assert captured["headers"]["transfer-encoding"] == "chunked"
    assert "content-length" not in captured["headers"]


async def test_upload_streams_iterable_of_bytes() -> None:
    """ENH-021: a sync iterable of bytes chunks round-trips and streams (chunked)."""

    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["content"] = request.content
        captured["headers"] = dict(request.headers)
        return httpx.Response(
            200,
            json={"id": "f3", "sha256": "beef", "size": 14, "contentType": "image/png"},
        )

    async with _client(handler) as c:
        up = await c.upload(iter([b"chunk1-", b"chunk2"]), content_type="image/png")
    assert isinstance(up, UploadResult)
    assert up.id == "f3"
    assert up.size == 14
    assert captured["content"] == b"chunk1-chunk2"
    assert captured["headers"]["transfer-encoding"] == "chunked"
    assert "content-length" not in captured["headers"]


async def test_upload_streams_async_iterable_of_bytes() -> None:
    """ENH-021: an async iterable of bytes chunks round-trips via httpx's async path."""

    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["content"] = request.content
        captured["headers"] = dict(request.headers)
        return httpx.Response(
            200,
            json={"id": "f4", "sha256": "cafe", "size": 8, "contentType": "image/png"},
        )

    async def aiter_chunks():
        yield b"foo"
        yield b"bar"

    async with _client(handler) as c:
        up = await c.upload(aiter_chunks(), content_type="image/png")
    assert isinstance(up, UploadResult)
    assert up.id == "f4"
    assert up.size == 8
    assert captured["content"] == b"foobar"
    assert captured["headers"]["transfer-encoding"] == "chunked"


async def test_upload_rejects_wrong_type() -> None:
    """A wrong-type input raises RtDbError(BAD_REQUEST) before any request."""
    from par_rt_db import ErrorCode

    async with _client(lambda req: httpx.Response(200, json={})) as c:
        for bad in ("not-bytes", 42, 3.14, {"oops": True}, None):
            with pytest.raises(RtDbError) as ei:
                await c.upload(bad)  # type: ignore[arg-type]
            assert ei.value.code is ErrorCode.BAD_REQUEST


async def test_delete_file_posts_to_storage_path() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        return httpx.Response(200, json={"ok": True})

    async with _client(handler) as c:
        await c.delete_file("f1")
    assert captured["method"] == "DELETE"
    assert captured["path"] == f"/api/storage/{DB}/f1"


async def test_get_file_metadata_returns_file_metadata_model() -> None:
    async with _client(
        _handler_map(
            {
                ("GET", f"/api/storage/{DB}/f1/metadata", ""): httpx.Response(
                    200,
                    json={
                        "id": "f1",
                        "sha256": "abc",
                        "size": 9,
                        "creationTime": 5,
                    },
                )
            }
        )
    ) as c:
        meta = await c.get_file_metadata("f1")
    assert isinstance(meta, FileMetadata)
    assert meta.size == 9
    assert meta.creation_time == 5
    assert meta.content_type is None  # omitted by the server → default


async def test_async_get_signed_url_passes_ttl() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["ttl"] = request.url.params.get("ttlSeconds")
        return httpx.Response(200, json={"url": "u", "expiresAt": 9})

    async with _client(handler) as c:
        r = await c.get_signed_url("f1", ttl_seconds=90)
    assert r.expires_at == 9
    assert seen["ttl"] == "90"


def test_get_url_is_base_plus_storage_id_no_request() -> None:
    # No mock handler installed → any request would fail; get_url makes none.
    client = _client(lambda r: httpx.Response(500))
    assert client.get_url("f1") == "https://rtdb.example/storage/f1"


def test_transform_url_emits_params_in_order() -> None:
    from urllib.parse import parse_qs, urlparse

    client = _client(lambda r: httpx.Response(500))
    url = client.transform_url("f1", w=100, h=50, fit="cover", q=80, format="jpeg")
    assert url == "https://rtdb.example/storage/f1?w=100&h=50&fit=cover&q=80&format=jpeg"
    qs = parse_qs(urlparse(url).query)
    assert list(qs) == ["w", "h", "fit", "q", "format"]


def test_transform_url_omits_unset_opts() -> None:
    from urllib.parse import parse_qs, urlparse

    client = _client(lambda r: httpx.Response(500))
    qs = parse_qs(urlparse(client.transform_url("f1", w=64)).query)
    assert list(qs) == ["w"]


def test_transform_url_omits_format_auto() -> None:
    from urllib.parse import urlparse

    client = _client(lambda r: httpx.Response(500))
    # "auto" is the server default — omitted to keep the URL minimal.
    url = client.transform_url("f1", w=100, format="auto")
    assert url == "https://rtdb.example/storage/f1?w=100"
    assert "format" not in urlparse(url).query


# --- admin control plane ----------------------------------------------------


def _admin_client(handler: Any) -> RtDbAsyncHttpClient:
    return _client(handler, token="admin-key")


async def test_admin_create_db_posts_name() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        captured["auth"] = request.headers["authorization"]
        return httpx.Response(200, json={"ok": True})

    async with _admin_client(handler) as c:
        await c.create_db("kanban")
    assert captured["body"] == {"name": "kanban"}
    assert captured["auth"] == ADMIN_BEARER


async def test_admin_delete_db_posts_name_and_confirm() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    async with _admin_client(handler) as c:
        await c.delete_db("kanban", "kanban")
    assert captured["body"] == {"name": "kanban", "confirm": "kanban"}


async def test_admin_delete_db_surfaces_confirmation_mismatch_envelope() -> None:
    async with _admin_client(
        _handler_map(
            {
                ("POST", "/admin/delete-db", ""): httpx.Response(
                    400,
                    json={
                        "code": "BAD_REQUEST",
                        "message": "confirmation does not match database name",
                    },
                )
            }
        )
    ) as c:
        with pytest.raises(RtDbError) as ei:
            await c.delete_db("kanban", "wrong")
    assert ei.value.code.value == "BAD_REQUEST"
    assert ei.value.message == "confirmation does not match database name"


async def test_admin_push_schema_serializes_schema_json() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    async with _admin_client(handler) as c:
        schema = Schema.builder().table("notes", lambda tb: tb.field("body", t.string())).build()
        await c.push_schema("kanban", schema)
    assert captured["body"]["db"] == "kanban"
    assert captured["body"]["schema"]["tables"]["notes"]["fields"]["body"] == {"type": "string"}


async def test_admin_list_dbs_returns_databases() -> None:
    async with _admin_client(
        _handler_map(
            {("GET", "/admin/dbs", ""): httpx.Response(200, json={"databases": ["kanban", "demo"]})}
        )
    ) as c:
        assert await c.list_dbs() == ["kanban", "demo"]


async def test_admin_mint_token_returns_token_id_and_token() -> None:
    async with _admin_client(
        _handler_map(
            {
                ("POST", "/admin/mint-token", ""): httpx.Response(
                    200, json={"tokenId": "id1", "token": "secret"}
                )
            }
        )
    ) as c:
        minted = await c.mint_token("kanban", "cli")
    assert isinstance(minted, MintedToken)
    assert minted.token_id == "id1"
    assert minted.token == "secret"


async def test_admin_mint_token_sends_capability_body() -> None:
    """Async twin: forwards ``readOnly``/``expiresAt``/``tables`` to the wire
    body (capability parity with ``AsyncRtDbAdminClient.mint_token``)."""
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"tokenId": "id2", "token": "secret2"})

    async with _admin_client(handler) as c:
        minted = await c.mint_token(
            "kanban", "scraper", read_only=True, tables=["users"], expires_at=1700000000000
        )
    assert isinstance(minted, MintedToken)
    assert minted.token_id == "id2"
    assert captured["body"] == {
        "db": "kanban",
        "name": "scraper",
        "readOnly": True,
        "expiresAt": 1700000000000,
        "tables": ["users"],
    }


async def test_admin_revoke_token_posts_token_id() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    async with _admin_client(handler) as c:
        await c.revoke_token("tid")
    assert captured["body"] == {"tokenId": "tid"}


async def test_admin_export_db_returns_jsonl_text() -> None:
    jsonl = '{"kind":"schema","schema":{"tables":{}}}\n'
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["query"] = dict(request.url.params)
        return httpx.Response(200, text=jsonl)

    async with _admin_client(handler) as c:
        assert await c.export_db("kanban") == jsonl
    assert captured["query"] == {"db": "kanban"}


async def test_admin_import_db_posts_ndjson_body() -> None:
    jsonl = '{"kind":"schema","schema":{"tables":{}}}\n'
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["query"] = dict(request.url.params)
        captured["content"] = request.content.decode("utf-8")
        captured["content_type"] = request.headers["content-type"]
        return httpx.Response(200, json={"ok": True})

    async with _admin_client(handler) as c:
        await c.import_db("kanban", jsonl)
    assert captured["query"] == {"db": "kanban"}
    assert captured["content"] == jsonl
    assert captured["content_type"] == "application/x-ndjson"


async def test_admin_allowlist_add_posts_db_action_add_email() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    async with _admin_client(handler) as c:
        await c.allowlist_add("kanban", "user@example.com")
    assert captured["body"] == {
        "db": "kanban",
        "action": "add",
        "email": "user@example.com",
    }


async def test_admin_allowlist_remove_posts_db_action_remove_email() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    async with _admin_client(handler) as c:
        await c.allowlist_remove("kanban", "user@example.com")
    assert captured["body"] == {
        "db": "kanban",
        "action": "remove",
        "email": "user@example.com",
    }


async def test_admin_allowlist_list_returns_emails() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["query"] = dict(request.url.params)
        return httpx.Response(200, json={"emails": ["a@b.com", "c@d.com"]})

    async with _admin_client(handler) as c:
        assert await c.allowlist_list("kanban") == ["a@b.com", "c@d.com"]
    assert captured["query"] == {"db": "kanban"}


async def test_admin_admins_list_returns_members() -> None:
    async with _admin_client(
        _handler_map(
            {
                ("GET", "/admin/admins", ""): httpx.Response(
                    200,
                    json={
                        "admins": [
                            {"email": "a@b.com", "githubId": 123},
                            {"email": "c@d.com"},
                        ]
                    },
                )
            }
        )
    ) as c:
        members = await c.admins_list()
    from par_rt_db.http_client import AdminMember

    assert len(members) == 2
    assert isinstance(members[0], AdminMember)
    assert members[0].email == "a@b.com"
    assert members[0].github_id == 123
    assert members[1].github_id is None


async def test_admin_admins_add_posts_email_and_optional_github_id() -> None:
    captured: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(json.loads(request.content))
        return httpx.Response(200, json={"ok": True})

    async with _admin_client(handler) as c:
        await c.admins_add("a@b.com", github_id=123)
        await c.admins_add("c@d.com")
    assert captured[0] == {"email": "a@b.com", "githubId": 123}
    # githubId omitted entirely when None
    assert captured[1] == {"email": "c@d.com"}


async def test_admin_admins_remove_uses_delete_with_body() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    async with _admin_client(handler) as c:
        await c.admins_remove("a@b.com")
    assert captured["method"] == "DELETE"
    assert captured["body"] == {"email": "a@b.com"}


async def test_admin_list_tokens_returns_token_info() -> None:
    """Async mirror of the sync test — the 7-field wire shape must deserialize
    on the ``extra="forbid"`` model, exercising ``expiresAt``/``readOnly``/
    ``tables`` on both a restricted and a full-access row."""
    async with _admin_client(
        _handler_map(
            {
                ("GET", "/admin/tokens", ""): httpx.Response(
                    200,
                    json={
                        "tokens": [
                            {
                                "id": "t1",
                                "name": "scraper",
                                "createdAt": 500,
                                "revoked": False,
                                "expiresAt": 1700000000000,
                                "readOnly": True,
                                "tables": ["users"],
                            },
                            {
                                "id": "t2",
                                "name": "ci",
                                "createdAt": 600,
                                "revoked": True,
                                "expiresAt": None,
                                "readOnly": False,
                                "tables": None,
                            },
                        ]
                    },
                )
            }
        )
    ) as c:
        tokens = await c.list_tokens("kanban")
    from par_rt_db.http_client import TokenInfo

    assert len(tokens) == 2
    assert isinstance(tokens[0], TokenInfo)
    assert tokens[0].id == "t1"
    assert tokens[0].expires_at == 1700000000000
    assert tokens[0].read_only is True
    assert tokens[0].tables == ["users"]
    assert tokens[1].revoked is True
    assert tokens[1].expires_at is None
    assert tokens[1].read_only is False
    assert tokens[1].tables is None


async def test_admin_get_schema_returns_schema_def() -> None:
    async with _admin_client(
        _handler_map(
            {
                ("GET", "/admin/dbs/kanban/schema", ""): httpx.Response(
                    200,
                    json={"tables": {"notes": {"fields": {"body": {"type": "string"}}}}},
                )
            }
        )
    ) as c:
        schema = await c.get_schema("kanban")
    assert "notes" in schema.tables


async def test_admin_db_stats_returns_table_stats() -> None:
    async with _admin_client(
        _handler_map(
            {
                ("GET", "/admin/dbs/kanban/stats", ""): httpx.Response(
                    200,
                    json={
                        "tables": [{"name": "notes", "rowCount": 5, "sizeBytes": 4096}],
                        "totalSizeBytes": 4096,
                        "tablesQuota": 10,
                        "tablesUsed": 1,
                        "storageQuotaBytes": 1048576,
                        "storageUsedBytes": 4096,
                        "subsQuota": 50,
                        "subsUsed": 3,
                    },
                )
            }
        )
    ) as c:
        stats = await c.db_stats("kanban")
    from par_rt_db.http_client import DbStats

    assert isinstance(stats, DbStats)
    assert stats.total_size_bytes == 4096
    assert stats.tables[0].row_count == 5
    assert stats.tables[0].size_bytes == 4096


async def test_admin_metrics_returns_snapshot_with_subs_counters() -> None:
    async with _admin_client(
        _handler_map(
            {
                ("GET", "/admin/metrics", ""): httpx.Response(
                    200,
                    json={
                        "queriesTotal": 10,
                        "mutationsTotal": 3,
                        "uploadsTotal": 1,
                        "wsConnections": 2,
                        "activeSubscriptions": 5,
                        "poolSize": 10,
                        "poolIdle": 8,
                        "uptimeSeconds": 99,
                        "queryLatency": {"p50": 100, "p95": 200, "p99": 300},
                        "mutateLatency": {"p50": 110, "p95": 210, "p99": 310},
                        "subscribeLatency": {"p50": 120, "p95": 220, "p99": 320},
                        "subsRerunsTotal": 7,
                        "subsSkipsPointTotal": 1,
                        "subsSkipsIndexedTotal": 2,
                        "subsSkipsOrderedTotal": 3,
                        "subsSkipVerificationsTotal": 4,
                        "subsMissedPushesTotal": 0,
                    },
                )
            }
        )
    ) as c:
        snap = await c.metrics()
    from par_rt_db.http_client import MetricsSnapshot

    assert isinstance(snap, MetricsSnapshot)
    assert snap.queries_total == 10
    assert snap.query_latency.p99 == 300
    assert snap.subs_skips_ordered_total == 3
    assert snap.subs_missed_pushes_total == 0


async def test_admin_metrics_defaults_subs_counters_when_omitted() -> None:
    # An older server (pre-2026-07-29) omits the subs_* counters; they default to 0
    # so a newer client still deserializes the response (mirrors rust-client serde(default)).
    async with _admin_client(
        _handler_map(
            {
                ("GET", "/admin/metrics", ""): httpx.Response(
                    200,
                    json={
                        "queriesTotal": 1,
                        "mutationsTotal": 0,
                        "uploadsTotal": 0,
                        "wsConnections": 0,
                        "activeSubscriptions": 0,
                        "poolSize": 1,
                        "poolIdle": 1,
                        "uptimeSeconds": 1,
                        "queryLatency": {"p50": 0, "p95": 0, "p99": 0},
                        "mutateLatency": {"p50": 0, "p95": 0, "p99": 0},
                        "subscribeLatency": {"p50": 0, "p95": 0, "p99": 0},
                    },
                )
            }
        )
    ) as c:
        snap = await c.metrics()
    assert snap.subs_reruns_total == 0
    assert snap.subs_missed_pushes_total == 0


_CONFIG_RESPONSE_BODY: dict[str, Any] = {
    "port": 8080,
    "publicUrl": "https://rtdb.example",
    "githubBaseUrl": "https://github.com",
    "githubApiUrl": "https://api.github.com",
    "databaseUrlConfigured": True,
    "adminKeyConfigured": True,
    "githubConfigured": False,
    "googleConfigured": False,
    "gitlabConfigured": False,
    "oidcConfigured": False,
    "hot": {
        "allowedOrigins": ["https://app.example"],
        "sessionTtlDays": 30,
        "maxFileSize": 52428800,
        "idempotencyTtlMs": 300000,
    },
    "version": "0.1.0",
    "gitCommit": "abc1234",
    "admins": [{"email": "admin@example.com"}],
}


async def test_admin_get_config_returns_config_response() -> None:
    async with _admin_client(
        _handler_map(
            {("GET", "/admin/config", ""): httpx.Response(200, json=_CONFIG_RESPONSE_BODY)}
        )
    ) as c:
        cfg = await c.get_config()
    from par_rt_db.http_client import ConfigResponse

    assert isinstance(cfg, ConfigResponse)
    assert cfg.admin_key_configured is True
    assert cfg.github_configured is False
    assert cfg.hot.session_ttl_days == 30
    assert cfg.hot.allowed_origins == ["https://app.example"]
    assert cfg.admins[0].email == "admin@example.com"


async def test_admin_patch_config_posts_camelcase_body_and_returns_config() -> None:
    captured: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append({"method": request.method, "body": json.loads(request.content)})
        # Echo back a config where sessionTtlDays reflects the patch.
        body = dict(_CONFIG_RESPONSE_BODY)
        ttl = json.loads(request.content).get("sessionTtlDays")
        if ttl is not None:
            body = {**body, "hot": {**body["hot"], "sessionTtlDays": ttl}}
        return httpx.Response(200, json=body)

    from par_rt_db.http_client import HotConfigPatch

    async with _admin_client(handler) as c:
        # Model input: snake_case field → camelCase wire key, None fields omitted.
        cfg = await c.patch_config(HotConfigPatch(session_ttl_days=60))
        assert cfg.hot.session_ttl_days == 60
        assert captured[0]["method"] == "PATCH"
        assert captured[0]["body"] == {"sessionTtlDays": 60}
        # Dict input: passed through as-is (caller provides wire camelCase keys).
        await c.patch_config({"maxFileSize": 104857600})
    assert captured[1]["body"] == {"maxFileSize": 104857600}


async def test_admin_ops_recent_returns_events_and_sends_filters() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["query"] = dict(request.url.params)
        return httpx.Response(
            200,
            json={
                "ops": [
                    {
                        "db": "kanban",
                        "table": "items",
                        "docId": "i1",
                        "kind": "insert",
                        "ts": 1000,
                        "owner": "user@example.com",
                    },
                    {
                        "db": "kanban",
                        "table": "items",
                        "docId": "i2",
                        "kind": "delete",
                        "ts": 2000,
                        "owner": None,
                    },
                ]
            },
        )

    async with _admin_client(handler) as c:
        ops = await c.ops_recent(db="kanban", table="items", n=50)
    from par_rt_db.http_client import OpEvent

    assert captured["query"] == {"db": "kanban", "table": "items", "n": "50"}
    assert len(ops) == 2
    assert isinstance(ops[0], OpEvent)
    assert ops[0].doc_id == "i1"
    assert ops[0].kind == "insert"
    assert ops[0].owner == "user@example.com"
    assert ops[1].owner is None


# --- admin data access (owner bypass) ---------------------------------------


async def test_admin_query_posts_to_singular_db_path_and_parses_result() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"result": [{"_id": "a"}, {"_id": "b"}]})

    async with _admin_client(handler) as c:
        got: list[dict[str, Any]] = await c.admin_query("kanban", TableQuery("items").take(2))
    assert len(got) == 2
    # db rides in the path (singular ``db``), NOT in the body
    assert captured["path"] == "/admin/db/kanban/query"
    assert "db" not in captured["body"]
    assert "query" in captured["body"]


async def test_admin_mutate_posts_to_singular_db_path_and_returns_step_results() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"results": [{"id": "new1"}, None]})

    async with _admin_client(handler) as c:
        txn = (
            Mutation.builder().insert("items", {"name": "x"}).patch("items", "i1", {"y": 1}).build()
        )
        res = await c.admin_mutate("kanban", txn)
    assert len(res) == 2
    assert captured["path"] == "/admin/db/kanban/mutate"
    # db rides in the path; idempotencyKey omitted when None
    assert "db" not in captured["body"]
    assert "idempotencyKey" not in captured["body"]
    assert "txn" in captured["body"]


async def test_admin_mutate_includes_idempotency_key_when_some() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"results": []})

    async with _admin_client(handler) as c:
        txn = Mutation.builder().delete("items", "i1").build()
        await c.admin_mutate("kanban", txn, idempotency_key="k1")
    assert captured["body"]["idempotencyKey"] == "k1"


async def test_admin_migrate_schema_posts_directives_and_dry_run() -> None:
    """``POST /admin/db/{db}/migrate`` sends ``{directives, dryRun}`` and parses
    the ``MigrateResult`` response (``applied``, derived ``schema``, per-directive
    ``reports``). Mirrors ``rust-client``'s ``migrate_schema_posts_directives_and_dry_run``.
    """
    from par_rt_db import Migration
    from par_rt_db.http_client import MigrateResult

    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        captured["path"] = request.url.path
        return httpx.Response(
            200,
            json={
                "applied": True,
                "schema": {
                    "tables": {
                        "users": {
                            "fields": {"fullName": {"type": "string"}},
                            "indexes": [],
                        }
                    }
                },
                "directives": [{"op": "renameField", "affectedRows": 3}],
            },
        )

    async with _admin_client(handler) as c:
        directives = (
            Migration.builder()
            .rename_field("users", "name", "fullName")
            .dry_run()
            .build()
            .directives
        )
        result = await c.migrate_schema("kanban", directives, dry_run=True)

    assert captured["path"] == "/admin/db/kanban/migrate"
    assert captured["body"]["dryRun"] is True
    assert captured["body"]["directives"][0] == {
        "op": "renameField",
        "table": "users",
        "from": "name",
        "to": "fullName",
    }
    # MigrateResult parsed correctly.
    assert isinstance(result, MigrateResult)
    assert result.applied is True
    assert "fullName" in result.schema_.tables["users"].fields
    assert len(result.directives) == 1
    assert result.directives[0].op == "renameField"
    assert result.directives[0].affected_rows == 3


async def test_admin_migrate_schema_accepts_migrate_request() -> None:
    """``migrate_schema`` also accepts a full ``MigrateRequest``."""
    from par_rt_db import Migration

    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "applied": False,
                "schema": {"tables": {}},
                "directives": [],
            },
        )

    async with _admin_client(handler) as c:
        req = Migration.builder().drop_table("gone").build()
        result = await c.migrate_schema("kanban", req)
    assert captured["body"]["directives"][0]["op"] == "dropTable"
    assert result.applied is False


# --- admin control plane: backups ------------------------------------------


async def test_admin_backup_now_posts_to_admin_backup() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["auth"] = request.headers["authorization"]
        return httpx.Response(202, json={"ok": True})

    async with _admin_client(handler) as c:
        await c.backup_now()
    assert captured["method"] == "POST"
    assert captured["path"] == "/admin/backup"
    assert captured["auth"] == ADMIN_BEARER


async def test_admin_list_backups_returns_running_flag_and_entries() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        return httpx.Response(
            200,
            json={
                "running": False,
                "backups": [
                    {
                        "name": "rtdb-20260728T143045Z.dump",
                        "sizeBytes": 4096,
                        "createdMs": 1753713045000,
                    }
                ],
            },
        )

    async with _admin_client(handler) as c:
        res = await c.list_backups()
    assert captured["method"] == "GET"
    assert captured["path"] == "/admin/backups"
    assert res["running"] is False
    assert len(res["backups"]) == 1
    entry = res["backups"][0]
    assert entry["name"] == "rtdb-20260728T143045Z.dump"
    assert entry["sizeBytes"] == 4096
    assert entry["createdMs"] == 1753713045000


async def test_admin_download_backup_returns_raw_bytes() -> None:
    captured: dict[str, Any] = {}
    payload = b"\x1f\x8b\x08\x00binary-dump-bytes"

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        return httpx.Response(
            200,
            content=payload,
            headers={"Content-Type": "application/octet-stream"},
        )

    async with _admin_client(handler) as c:
        data = await c.download_backup("rtdb-20260728T143045Z.dump")
    assert captured["method"] == "GET"
    assert captured["path"] == "/admin/backups/rtdb-20260728T143045Z.dump"
    assert isinstance(data, bytes)
    assert data == payload


async def test_admin_delete_backup_uses_delete_with_name_in_path() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        return httpx.Response(204)

    async with _admin_client(handler) as c:
        await c.delete_backup("rtdb-20260728T143045Z.dump")
    assert captured["method"] == "DELETE"
    assert captured["path"] == "/admin/backups/rtdb-20260728T143045Z.dump"


async def test_admin_restore_backup_posts_name_and_confirm() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "target": "rtdb_restored_20260728T143045Z",
                "instructions": (
                    "Restore complete into database "
                    "'rtdb_restored_20260728T143045Z'. To cut over: set "
                    "RTDB_DATABASE_URL to connect to "
                    "'rtdb_restored_20260728T143045Z', then restart the server."
                ),
            },
        )

    async with _admin_client(handler) as c:
        res = await c.restore_backup("rtdb-20260728T143045Z.dump")
    assert captured["method"] == "POST"
    assert captured["path"] == "/admin/restore"
    assert captured["body"] == {
        "name": "rtdb-20260728T143045Z.dump",
        "confirm": "rtdb-20260728T143045Z.dump",
    }
    assert res["target"] == "rtdb_restored_20260728T143045Z"
    assert "rtdb_restored_20260728T143045Z" in res["instructions"]


# --- data plane: workflows (FM-29) ------------------------------------------


async def test_workflow_ops_round_trip() -> None:
    from par_rt_db.wire import WorkflowSpec, WorkflowStepSpec

    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.setdefault("paths", []).append(request.url.path)
        captured["body"] = json.loads(request.content)
        if request.url.path == "/api/workflows":
            return httpx.Response(200, json={"id": "wf-1"})
        if request.url.path == "/api/workflows/list":
            return httpx.Response(
                200,
                json={
                    "workflows": [
                        {
                            "id": "wf-1",
                            "name": "drip",
                            "status": "running",
                            "currentStep": 1,
                            "stepCount": 2,
                            "attempts": 0,
                            "sleepUntil": 9000,
                            "createdAt": 100,
                            "updatedAt": 150,
                        }
                    ]
                },
            )
        if request.url.path == "/api/workflows/wf-1/cancel":
            return httpx.Response(200, json={"cancelled": True})
        return httpx.Response(404, text=f"no mock for {request.url.path}")

    txn = Mutation.builder().insert("items", {"name": "x"}).build()
    spec = WorkflowSpec(
        name="drip", steps=[WorkflowStepSpec(txn=txn.model_dump(by_alias=True), sleep_before_ms=60)]
    )
    async with _client(handler) as c:
        wid = await c.start_workflow(spec)
        wfs = await c.list_workflows(status="running")
        ok = await c.cancel_workflow(wid)
    assert wid == "wf-1"
    assert captured["paths"] == [
        "/api/workflows",
        "/api/workflows/list",
        "/api/workflows/wf-1/cancel",
    ]
    assert captured["body"] == {"db": DB}
    assert [w.id for w in wfs] == ["wf-1"]
    assert wfs[0].sleep_until == 9000
    assert ok is True
