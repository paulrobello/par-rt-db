"""Tests for ``par_rt_db.http_client.RtDbHttpClient``.

Mirrors ``rust-client/src/http.rs``'s test coverage at a high level: data-plane
query/mutate result parsing, the ``{code, message}`` error envelope, the admin
control plane (``/admin/*``), owner-bypass admin data access
(``/admin/db/{db}/*``), and the storage surface (raw-body upload, DELETE,
metadata, public URL). Uses ``httpx.MockTransport`` (in-process, no port).

The mock-server pattern mirrors ``rust-client``'s ``wiremock`` setup: each test
stubs the routes the client should hit and asserts on the wire request the
client actually sent.
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
    RtDbHttpClient,
    TableQuery,
    t,
)
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
) -> RtDbHttpClient:
    """Build a client whose httpx.Client uses an in-process ``MockTransport``."""
    return RtDbHttpClient(url, db, token, transport=httpx.MockTransport(handler))


def _handler_map(
    routes: dict[tuple[str, str, str], RouteResponse],
) -> Callable[[httpx.Request], httpx.Response]:
    """Build a MockTransport handler from a route table.

    Keys are ``(method, path, body_contains)`` triples (``body_contains=""``
    skips the body check, matching on method + path only). Values are either an
    ``httpx.Response`` or a ``(request) -> httpx.Response`` callable.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        # request.url.path excludes the query string; compare on path only.
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


def test_run_collect_posts_query_and_parses_list_result() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["request"] = request
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={"result": [{"_id": "a", "n": 1}, {"_id": "b", "n": 2}]},
        )

    client = _client(handler)
    q = TableQuery("items").with_index("by_status").eq("active").take(2)
    got: list[dict[str, Any]] = client.run(q)
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


def test_run_count_parses_int() -> None:
    client = _client(
        _handler_map({("POST", "/api/query", ""): httpx.Response(200, json={"result": 5})})
    )
    n: int = client.run(TableQuery("items").count())
    assert n == 5


def test_query_alias_matches_run() -> None:
    assert RtDbHttpClient.query is RtDbHttpClient.run


def test_get_returns_optional_doc() -> None:
    client = _client(
        _handler_map(
            {("POST", "/api/query", '"get"'): httpx.Response(200, json={"result": {"_id": "a"}})}
        )
    )
    some: dict[str, Any] | None = client.get("items", "a")
    assert some == {"_id": "a"}


def test_get_miss_returns_none() -> None:
    client = _client(
        _handler_map({("POST", "/api/query", ""): httpx.Response(200, json={"result": None})})
    )
    assert client.get("items", "missing") is None


def test_find_one_by_index_hit_returns_doc() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"result": {"_id": "u1", "email": "a@b.com"}})

    client = _client(handler)
    got: dict[str, Any] | None = client.find_one_by_index("users", "by_email", "a@b.com")
    assert got == {"_id": "u1", "email": "a@b.com"}
    # first terminal is set on the wire
    assert captured["body"]["query"]["first"] is True
    assert captured["body"]["query"]["eq"] == ["a@b.com"]


def test_run_parses_first_terminal_none() -> None:
    client = _client(
        _handler_map({("POST", "/api/query", ""): httpx.Response(200, json={"result": None})})
    )
    assert client.run(TableQuery("items").with_index("i").eq("a").first()) is None


# --- data plane: mutate -----------------------------------------------------


def test_mutate_posts_and_parses_step_results() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"results": [{"id": "new1"}, None]})

    client = _client(handler)
    txn = Mutation.builder().insert("items", {"name": "x"}).patch("items", "i1", {"y": 1}).build()
    res = client.mutate(txn)
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


def test_mutate_sends_idempotency_key_when_given() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"results": []})

    client = _client(handler)
    txn = Mutation.builder().delete("items", "i1").build()
    client.mutate(txn, idempotency_key="k1")
    assert captured["body"]["idempotencyKey"] == "k1"


def test_upsert_by_index_inserts_when_no_match() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={"results": [{"id": "new1", "inserted": True}]},
        )

    client = _client(handler)
    res = client.upsert_by_index("users", "by_email", "a@b.com", {"email": "a@b.com"}, {"n": 1})
    from par_rt_db.mutation import _StepUpsert

    assert isinstance(res, _StepUpsert)
    assert res.id == "new1"
    assert res.inserted is True
    # the one-step txn uses the upsert op
    step = captured["body"]["txn"]["steps"][0]
    assert step["op"] == "upsert"
    assert step["eq"] == ["a@b.com"]


# --- data plane: scheduling + batch query -----------------------------------


def test_schedule_posts_when_and_txn_returns_id() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"id": "sch-1"})

    from par_rt_db.wire import _AfterMs

    client = _client(handler)
    txn = Mutation.builder().insert("items", {"name": "x"}).build()
    sid = client.schedule(txn, _AfterMs(ms=5000))
    assert sid == "sch-1"
    assert captured["body"]["db"] == DB
    assert captured["body"]["when"] == {"type": "afterMs", "ms": 5000}
    assert captured["body"]["txn"]["steps"][0]["op"] == "insert"


def test_schedule_manage_ops_post_to_id_paths() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        return httpx.Response(200, json={"ok": True})

    client = _client(handler)
    client.cancel_schedule("sch-1")
    client.pause_schedule("sch-1")
    client.resume_schedule("sch-1")
    assert seen == [
        "/api/schedule/sch-1/cancel",
        "/api/schedule/sch-1/pause",
        "/api/schedule/sch-1/resume",
    ]


def test_list_schedules_returns_schedule_info_list() -> None:
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

    client = _client(handler)
    schedules = client.list_schedules()
    assert [s.id for s in schedules] == ["sch-1", "sch-2"]
    assert schedules[0].kind == "oneshot"
    assert schedules[0].fired_count == 0
    assert schedules[1].cron == "0 9 * * *"


def test_batch_query_returns_one_outcome_per_input() -> None:
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

    client = _client(handler)
    outcomes = client.batch_query([TableQuery("items").collect(), TableQuery("items").first()])
    assert captured["body"]["db"] == DB
    assert len(captured["body"]["queries"]) == 2
    assert len(outcomes) == 2
    assert outcomes[0].ok is True
    assert outcomes[0].result == [{"_id": "i1"}]
    assert outcomes[1].ok is False
    assert outcomes[1].error is not None
    assert outcomes[1].error.code == "NOT_FOUND"


# --- error envelope ---------------------------------------------------------


def test_error_envelope_becomes_rtdb_error() -> None:
    client = _client(
        _handler_map(
            {
                ("POST", "/api/query", ""): httpx.Response(
                    409,
                    json={"code": "PRECONDITION_FAILED", "message": "version mismatch"},
                )
            }
        )
    )
    with pytest.raises(RtDbError) as ei:
        client.run(TableQuery("items").count())
    err = ei.value
    assert err.code.value == "PRECONDITION_FAILED"
    assert err.message == "version mismatch"
    assert err.status_code == 409


def test_non_envelope_error_is_internal_with_status_in_message() -> None:
    client = _client(
        _handler_map({("POST", "/api/query", ""): httpx.Response(500, text="gateway down")})
    )
    with pytest.raises(RtDbError) as ei:
        client.run(TableQuery("items").count())
    assert err_internal(ei.value)
    assert "500" in ei.value.message


def err_internal(err: RtDbError) -> bool:
    from par_rt_db.errors import ErrorCode

    return err.code is ErrorCode.INTERNAL


# --- storage ----------------------------------------------------------------


def test_upload_posts_raw_bytes_and_returns_metadata() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["content"] = request.content
        captured["headers"] = dict(request.headers)
        return httpx.Response(
            200,
            json={"id": "f1", "sha256": "abc", "size": 9, "contentType": "image/png"},
        )

    client = _client(handler)
    up = client.upload(b"raw-bytes", content_type="image/png")
    assert isinstance(up, UploadResult)
    assert up.id == "f1"
    assert up.size == 9
    assert up.content_type == "image/png"
    # raw body (NOT json-wrapped)
    assert captured["content"] == b"raw-bytes"
    assert captured["headers"]["content-type"] == "image/png"
    assert captured["headers"]["authorization"] == BEARER


def test_delete_file_posts_to_storage_path() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        return httpx.Response(200, json={"ok": True})

    client = _client(handler)
    client.delete_file("f1")
    assert captured["method"] == "DELETE"
    assert captured["path"] == f"/api/storage/{DB}/f1"


def test_get_file_metadata_returns_file_metadata_model() -> None:
    client = _client(
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
    )
    meta = client.get_file_metadata("f1")
    assert isinstance(meta, FileMetadata)
    assert meta.size == 9
    assert meta.creation_time == 5
    assert meta.content_type is None  # omitted by the server → default


def test_get_url_is_base_plus_storage_id_no_request() -> None:
    # No mock handler installed → any request would fail; get_url makes none.
    client = _client(lambda r: httpx.Response(500))
    assert client.get_url("f1") == "https://rtdb.example/storage/f1"


# --- admin control plane ----------------------------------------------------


def _admin_client(handler: Any) -> RtDbHttpClient:
    return _client(handler, token="admin-key")


def test_admin_create_db_posts_name() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        captured["auth"] = request.headers["authorization"]
        return httpx.Response(200, json={"ok": True})

    client = _admin_client(handler)
    client.create_db("kanban")
    assert captured["body"] == {"name": "kanban"}
    assert captured["auth"] == ADMIN_BEARER


def test_admin_delete_db_posts_name_and_confirm() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    client = _admin_client(handler)
    client.delete_db("kanban", "kanban")
    assert captured["body"] == {"name": "kanban", "confirm": "kanban"}


def test_admin_delete_db_surfaces_confirmation_mismatch_envelope() -> None:
    client = _admin_client(
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
    )
    with pytest.raises(RtDbError) as ei:
        client.delete_db("kanban", "wrong")
    assert ei.value.code.value == "BAD_REQUEST"
    assert ei.value.message == "confirmation does not match database name"


def test_admin_push_schema_serializes_schema_json() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    client = _admin_client(handler)
    schema = Schema.builder().table("notes", lambda tb: tb.field("body", t.string())).build()
    client.push_schema("kanban", schema)
    assert captured["body"]["db"] == "kanban"
    assert captured["body"]["schema"]["tables"]["notes"]["fields"]["body"] == {"type": "string"}


def test_admin_list_dbs_returns_databases() -> None:
    client = _admin_client(
        _handler_map(
            {("GET", "/admin/dbs", ""): httpx.Response(200, json={"databases": ["kanban", "demo"]})}
        )
    )
    assert client.list_dbs() == ["kanban", "demo"]


def test_admin_mint_token_returns_token_id_and_token() -> None:
    client = _admin_client(
        _handler_map(
            {
                ("POST", "/admin/mint-token", ""): httpx.Response(
                    200, json={"tokenId": "id1", "token": "secret"}
                )
            }
        )
    )
    minted = client.mint_token("kanban", "cli")
    assert isinstance(minted, MintedToken)
    assert minted.token_id == "id1"
    assert minted.token == "secret"


def test_admin_revoke_token_posts_token_id() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    client = _admin_client(handler)
    client.revoke_token("tid")
    assert captured["body"] == {"tokenId": "tid"}


def test_admin_export_db_returns_jsonl_text() -> None:
    jsonl = '{"kind":"schema","schema":{"tables":{}}}\n'
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["query"] = dict(request.url.params)
        return httpx.Response(200, text=jsonl)

    client = _admin_client(handler)
    assert client.export_db("kanban") == jsonl
    assert captured["query"] == {"db": "kanban"}


def test_admin_import_db_posts_ndjson_body() -> None:
    jsonl = '{"kind":"schema","schema":{"tables":{}}}\n'
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["query"] = dict(request.url.params)
        captured["content"] = request.content.decode("utf-8")
        captured["content_type"] = request.headers["content-type"]
        return httpx.Response(200, json={"ok": True})

    client = _admin_client(handler)
    client.import_db("kanban", jsonl)
    assert captured["query"] == {"db": "kanban"}
    assert captured["content"] == jsonl
    assert captured["content_type"] == "application/x-ndjson"


# --- admin data access (owner bypass) ---------------------------------------


def test_admin_query_posts_to_singular_db_path_and_parses_result() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"result": [{"_id": "a"}, {"_id": "b"}]})

    client = _admin_client(handler)
    got: list[dict[str, Any]] = client.admin_query("kanban", TableQuery("items").take(2))
    assert len(got) == 2
    # db rides in the path (singular ``db``), NOT in the body
    assert captured["path"] == "/admin/db/kanban/query"
    assert "db" not in captured["body"]
    assert "query" in captured["body"]


def test_admin_mutate_posts_to_singular_db_path_and_returns_step_results() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"results": [{"id": "new1"}, None]})

    client = _admin_client(handler)
    txn = Mutation.builder().insert("items", {"name": "x"}).patch("items", "i1", {"y": 1}).build()
    res = client.admin_mutate("kanban", txn)
    assert len(res) == 2
    assert captured["path"] == "/admin/db/kanban/mutate"
    # db rides in the path; idempotencyKey omitted when None
    assert "db" not in captured["body"]
    assert "idempotencyKey" not in captured["body"]
    assert "txn" in captured["body"]


def test_admin_mutate_includes_idempotency_key_when_some() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"results": []})

    client = _admin_client(handler)
    txn = Mutation.builder().delete("items", "i1").build()
    client.admin_mutate("kanban", txn, idempotency_key="k1")
    assert captured["body"]["idempotencyKey"] == "k1"


# --- lifecycle --------------------------------------------------------------


def test_context_manager_closes_client() -> None:
    client = _client(lambda r: httpx.Response(200, json={"databases": []}))
    with client as c:
        assert c is client
        assert c.list_dbs() == []
    # After exit the underlying httpx.Client is closed; further requests raise.
    with pytest.raises(RuntimeError):
        client.list_dbs()
