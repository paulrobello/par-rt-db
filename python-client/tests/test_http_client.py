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


def test_admin_allowlist_add_posts_db_action_add_email() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    client = _admin_client(handler)
    client.allowlist_add("kanban", "user@example.com")
    assert captured["body"] == {
        "db": "kanban",
        "action": "add",
        "email": "user@example.com",
    }


def test_admin_allowlist_remove_posts_db_action_remove_email() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    client = _admin_client(handler)
    client.allowlist_remove("kanban", "user@example.com")
    assert captured["body"] == {
        "db": "kanban",
        "action": "remove",
        "email": "user@example.com",
    }


def test_admin_allowlist_list_returns_emails() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["query"] = dict(request.url.params)
        return httpx.Response(200, json={"emails": ["a@b.com", "c@d.com"]})

    client = _admin_client(handler)
    assert client.allowlist_list("kanban") == ["a@b.com", "c@d.com"]
    assert captured["query"] == {"db": "kanban"}


def test_admin_admins_list_returns_members() -> None:
    client = _admin_client(
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
    )
    from par_rt_db.http_client import AdminMember

    members = client.admins_list()
    assert len(members) == 2
    assert isinstance(members[0], AdminMember)
    assert members[0].email == "a@b.com"
    assert members[0].github_id == 123
    assert members[1].github_id is None


def test_admin_admins_add_posts_email_and_optional_github_id() -> None:
    captured: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(json.loads(request.content))
        return httpx.Response(200, json={"ok": True})

    client = _admin_client(handler)
    client.admins_add("a@b.com", github_id=123)
    client.admins_add("c@d.com")
    assert captured[0] == {"email": "a@b.com", "githubId": 123}
    # githubId omitted entirely when None
    assert captured[1] == {"email": "c@d.com"}


def test_admin_admins_remove_uses_delete_with_body() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    client = _admin_client(handler)
    client.admins_remove("a@b.com")
    assert captured["method"] == "DELETE"
    assert captured["body"] == {"email": "a@b.com"}


def test_admin_list_tokens_returns_token_info() -> None:
    client = _admin_client(
        _handler_map(
            {
                ("GET", "/admin/tokens", ""): httpx.Response(
                    200,
                    json={
                        "tokens": [
                            {"id": "t1", "name": "cli", "createdAt": 500, "revoked": False},
                            {"id": "t2", "name": "ci", "createdAt": 600, "revoked": True},
                        ]
                    },
                )
            }
        )
    )
    from par_rt_db.http_client import TokenInfo

    tokens = client.list_tokens("kanban")
    assert len(tokens) == 2
    assert isinstance(tokens[0], TokenInfo)
    assert tokens[0].id == "t1"
    assert tokens[0].created_at == 500
    assert tokens[1].revoked is True


def test_admin_get_schema_returns_schema_def() -> None:
    client = _admin_client(
        _handler_map(
            {
                ("GET", "/admin/dbs/kanban/schema", ""): httpx.Response(
                    200,
                    json={"tables": {"notes": {"fields": {"body": {"type": "string"}}}}},
                )
            }
        )
    )
    schema = client.get_schema("kanban")
    assert "notes" in schema.tables


def test_admin_db_stats_returns_table_stats() -> None:
    client = _admin_client(
        _handler_map(
            {
                ("GET", "/admin/dbs/kanban/stats", ""): httpx.Response(
                    200,
                    json={
                        "tables": [{"name": "notes", "rowCount": 5, "sizeBytes": 4096}],
                        "totalSizeBytes": 4096,
                    },
                )
            }
        )
    )
    from par_rt_db.http_client import DbStats

    stats = client.db_stats("kanban")
    assert isinstance(stats, DbStats)
    assert stats.total_size_bytes == 4096
    assert stats.tables[0].row_count == 5
    assert stats.tables[0].size_bytes == 4096


def test_admin_metrics_returns_snapshot_with_subs_counters() -> None:
    client = _admin_client(
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
    )
    from par_rt_db.http_client import MetricsSnapshot

    snap = client.metrics()
    assert isinstance(snap, MetricsSnapshot)
    assert snap.queries_total == 10
    assert snap.query_latency.p99 == 300
    assert snap.subs_skips_ordered_total == 3
    assert snap.subs_missed_pushes_total == 0


def test_admin_metrics_defaults_subs_counters_when_omitted() -> None:
    # An older server (pre-2026-07-29) omits the subs_* counters; they default to 0
    # so a newer client still deserializes the response (mirrors rust-client serde(default)).
    client = _admin_client(
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
    )
    snap = client.metrics()
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


def test_admin_get_config_returns_config_response() -> None:
    client = _admin_client(
        _handler_map(
            {("GET", "/admin/config", ""): httpx.Response(200, json=_CONFIG_RESPONSE_BODY)}
        )
    )
    from par_rt_db.http_client import ConfigResponse

    cfg = client.get_config()
    assert isinstance(cfg, ConfigResponse)
    assert cfg.admin_key_configured is True
    assert cfg.github_configured is False
    assert cfg.hot.session_ttl_days == 30
    assert cfg.hot.allowed_origins == ["https://app.example"]
    assert cfg.admins[0].email == "admin@example.com"


def test_admin_patch_config_posts_camelcase_body_and_returns_config() -> None:
    captured: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append({"method": request.method, "body": json.loads(request.content)})
        # Echo back a config where sessionTtlDays reflects the patch.
        body = dict(_CONFIG_RESPONSE_BODY)
        ttl = json.loads(request.content).get("sessionTtlDays")
        if ttl is not None:
            body = {**body, "hot": {**body["hot"], "sessionTtlDays": ttl}}
        return httpx.Response(200, json=body)

    client = _admin_client(handler)
    from par_rt_db.http_client import HotConfigPatch

    # Model input: snake_case field → camelCase wire key, None fields omitted.
    cfg = client.patch_config(HotConfigPatch(session_ttl_days=60))
    assert cfg.hot.session_ttl_days == 60
    assert captured[0]["method"] == "PATCH"
    assert captured[0]["body"] == {"sessionTtlDays": 60}
    # Dict input: passed through as-is (caller provides wire camelCase keys).
    client.patch_config({"maxFileSize": 104857600})
    assert captured[1]["body"] == {"maxFileSize": 104857600}


def test_admin_ops_recent_returns_events_and_sends_filters() -> None:
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

    client = _admin_client(handler)
    from par_rt_db.http_client import OpEvent

    ops = client.ops_recent(db="kanban", table="items", n=50)
    assert captured["query"] == {"db": "kanban", "table": "items", "n": "50"}
    assert len(ops) == 2
    assert isinstance(ops[0], OpEvent)
    assert ops[0].doc_id == "i1"
    assert ops[0].kind == "insert"
    assert ops[0].owner == "user@example.com"
    assert ops[1].owner is None


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
