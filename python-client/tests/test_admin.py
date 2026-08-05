"""Tests for ``par_rt_db.admin`` — the dedicated admin control-plane client.

Covers the ENH-005 token surface (``mint_token`` / ``revoke_token`` /
``list_tokens``) on both :class:`RtDbAdminClient` (sync) and
:class:`AsyncRtDbAdminClient` (async), using ``httpx.MockTransport`` for
in-process, no-port testing (same pattern as ``test_http_client.py`` /
``test_aio_http_client.py``).

Each test stubs the route the client should hit and asserts on the wire
request (method, path, bearer, camelCase body) the client actually sent, plus
the pydantic model mapping on the response.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from par_rt_db import Mutation, TableQuery, t
from par_rt_db.admin import (
    AdminMember,
    AsyncRtDbAdminClient,
    ConfigResponse,
    DbStats,
    HotConfigPatch,
    MetricsSnapshot,
    MintedToken,
    OpEvent,
    RtDbAdminClient,
    TokenInfo,
)
from par_rt_db.errors import ErrorCode, RtDbError
from par_rt_db.schema import Schema

ADMIN_BEARER = "Bearer admin-key"
URL = "https://rtdb.example"

RouteResponse = httpx.Response | Callable[[httpx.Request], httpx.Response]


def _sync_client(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    admin_key: str = "admin-key",
) -> RtDbAdminClient:
    return RtDbAdminClient(URL, admin_key, transport=httpx.MockTransport(handler))


def _async_client(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    admin_key: str = "admin-key",
) -> AsyncRtDbAdminClient:
    return AsyncRtDbAdminClient(URL, admin_key, transport=httpx.MockTransport(handler))


def _handler_map(
    routes: dict[tuple[str, str, str], RouteResponse],
) -> Callable[[httpx.Request], httpx.Response]:
    """Build a MockTransport handler from a route table.

    Keys are ``(method, path, body_contains)`` triples (``body_contains=""``
    skips the body check). Values are either an ``httpx.Response`` or a
    ``(request) -> httpx.Response`` callable.
    """

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


# --- pydantic wire mapping ------------------------------------------------


def test_minted_token_model_validate_maps_camelcase() -> None:
    mt = MintedToken.model_validate({"tokenId": "tid", "token": "secret"})
    assert mt.token_id == "tid"
    assert mt.token == "secret"


def test_token_info_model_validate_restricted_row() -> None:
    row = TokenInfo.model_validate(
        {
            "id": "t1",
            "name": "scraper",
            "createdAt": 500,
            "revoked": False,
            "expiresAt": 1700000000000,
            "readOnly": True,
            "tables": ["users"],
        }
    )
    assert row.id == "t1"
    assert row.created_at == 500
    assert row.revoked is False
    assert row.expires_at == 1700000000000
    assert row.read_only is True
    assert row.tables == ["users"]


def test_token_info_model_validate_full_access_row() -> None:
    """Full-access token: ``expiresAt:null, readOnly:false, tables:null``."""
    row = TokenInfo.model_validate(
        {
            "id": "t2",
            "name": "ci",
            "createdAt": 600,
            "revoked": True,
            "expiresAt": None,
            "readOnly": False,
            "tables": None,
        }
    )
    assert row.expires_at is None
    assert row.read_only is False
    assert row.tables is None
    assert row.revoked is True


def test_token_info_model_validate_omitted_optional_fields_default() -> None:
    """An older server omitting ``expiresAt``/``readOnly``/``tables`` still
    deserializes (matches the server's ``#[serde(default)]``)."""
    row = TokenInfo.model_validate(
        {"id": "t3", "name": "legacy", "createdAt": 700, "revoked": False}
    )
    assert row.expires_at is None
    assert row.read_only is False
    assert row.tables is None


# --- sync: mint_token -----------------------------------------------------


def test_mint_token_posts_capabilities_and_parses_response() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["request"] = request
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"tokenId": "id1", "token": "secret"})

    with _sync_client(handler) as c:
        minted = c.mint_token(
            "dbx",
            "scraper",
            read_only=True,
            tables=["users"],
            expires_at=1700000000000,
        )
    assert isinstance(minted, MintedToken)
    assert minted.token_id == "id1"
    assert minted.token == "secret"
    # route + admin bearer
    assert captured["request"].method == "POST"
    assert captured["request"].url.path == "/admin/mint-token"
    assert captured["request"].headers["authorization"] == ADMIN_BEARER
    # body: camelCase keys, all three capabilities present when set
    assert captured["body"] == {
        "db": "dbx",
        "name": "scraper",
        "readOnly": True,
        "expiresAt": 1700000000000,
        "tables": ["users"],
    }


def test_mint_token_omits_expiresat_and_tables_when_none() -> None:
    """When ``expires_at``/``tables`` are ``None`` they are omitted from the
    body so the server applies its defaults; ``readOnly`` is always sent."""
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"tokenId": "id2", "token": "t"})

    with _sync_client(handler) as c:
        c.mint_token("dbx", "ci")
    assert captured["body"] == {"db": "dbx", "name": "ci", "readOnly": False}
    assert "expiresAt" not in captured["body"]
    assert "tables" not in captured["body"]


# --- sync: revoke_token ---------------------------------------------------


def test_revoke_token_posts_token_id() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    with _sync_client(handler) as c:
        c.revoke_token("tid")
    assert captured["body"] == {"tokenId": "tid"}


# --- sync: list_tokens ----------------------------------------------------


def test_list_tokens_parses_restricted_and_full_access_rows() -> None:
    client = _sync_client(
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
                                "revoked": False,
                                "expiresAt": None,
                                "readOnly": False,
                                "tables": None,
                            },
                        ]
                    },
                )
            }
        )
    )
    with client as c:
        rows = c.list_tokens("dbx")
    assert len(rows) == 2
    assert all(isinstance(r, TokenInfo) for r in rows)
    # restricted row
    assert rows[0].id == "t1"
    assert rows[0].read_only is True
    assert rows[0].tables == ["users"]
    assert rows[0].expires_at == 1700000000000
    # full-access row
    assert rows[1].id == "t2"
    assert rows[1].read_only is False
    assert rows[1].tables is None
    assert rows[1].expires_at is None


def test_list_tokens_sends_db_query_param() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = request.url
        return httpx.Response(200, json={"tokens": []})

    with _sync_client(handler) as c:
        rows = c.list_tokens("kanban")
    assert rows == []
    assert captured["url"].path == "/admin/tokens"
    assert captured["url"].params["db"] == "kanban"


# --- sync: error envelope -------------------------------------------------


def test_non_2xx_raises_rtdb_error_with_code() -> None:
    client = _sync_client(
        _handler_map(
            {
                ("POST", "/admin/mint-token", ""): httpx.Response(
                    401, json={"code": "UNAUTHORIZED", "message": "bad admin key"}
                )
            }
        )
    )
    with client as c, pytest.raises(RtDbError) as ei:
        c.mint_token("dbx", "x")
    assert ei.value.code is ErrorCode.UNAUTHORIZED


# --- async: mint / revoke / list mirror -----------------------------------


async def test_async_mint_token_posts_capabilities_and_parses_response() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        captured["auth"] = request.headers["authorization"]
        return httpx.Response(200, json={"tokenId": "aid", "token": "atok"})

    async with _async_client(handler) as c:
        minted = await c.mint_token(
            "dbx", "scraper", read_only=True, tables=["users"], expires_at=1700000000000
        )
    assert isinstance(minted, MintedToken)
    assert minted.token_id == "aid"
    assert minted.token == "atok"
    assert captured["auth"] == ADMIN_BEARER
    assert captured["body"] == {
        "db": "dbx",
        "name": "scraper",
        "readOnly": True,
        "expiresAt": 1700000000000,
        "tables": ["users"],
    }


async def test_async_revoke_token_posts_token_id() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    async with _async_client(handler) as c:
        await c.revoke_token("tid")
    assert captured["body"] == {"tokenId": "tid"}


async def test_async_list_tokens_parses_rows() -> None:
    async with _async_client(
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
        rows = await c.list_tokens("dbx")
    assert len(rows) == 2
    assert rows[0].read_only is True
    assert rows[0].tables == ["users"]
    assert rows[1].read_only is False
    assert rows[1].tables is None
    assert rows[1].revoked is True


# --- mirrored admin surface (ENH-005 parity sweep) -----------------------
#
# Representative subset across categories proving the mirrored methods hit the
# right route with the right camelCase body + admin bearer and parse the
# response into the shared pydantic models. Exhaustive per-method coverage
# lives in test_http_client.py; here we prove the mirroring pattern.


def test_create_db_posts_name_and_expects_ok() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["auth"] = request.headers["authorization"]
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    with _sync_client(handler) as c:
        c.create_db("mydb")
    assert captured["method"] == "POST"
    assert captured["path"] == "/admin/create-db"
    assert captured["auth"] == ADMIN_BEARER
    assert captured["body"] == {"name": "mydb"}


def test_list_dbs_gets_databases_list() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["method"] = request.method
        return httpx.Response(200, json={"databases": ["a", "b", "c"]})

    with _sync_client(handler) as c:
        dbs = c.list_dbs()
    assert dbs == ["a", "b", "c"]
    assert captured["method"] == "GET"
    assert captured["path"] == "/admin/dbs"


def test_push_schema_posts_db_and_schema() -> None:
    captured: dict[str, Any] = {}
    schema = Schema.builder().table("items", lambda tb: tb.field("sku", t.string())).build()

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    with _sync_client(handler) as c:
        c.push_schema("dbx", schema)
    assert captured["path"] == "/admin/push-schema"
    assert captured["body"]["db"] == "dbx"
    assert "schema" in captured["body"]


def test_allowlist_add_posts_action_and_email() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    with _sync_client(handler) as c:
        c.allowlist_add("dbx", "user@example.com")
    assert captured["path"] == "/admin/allowlist"
    assert captured["body"] == {
        "db": "dbx",
        "action": "add",
        "email": "user@example.com",
    }


def test_get_config_parses_config_response() -> None:
    with _sync_client(
        _handler_map(
            {
                ("GET", "/admin/config", ""): httpx.Response(
                    200,
                    json={
                        "port": 8300,
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
                            "allowedOrigins": ["https://rtdb.example"],
                            "sessionTtlDays": 30,
                            "maxFileSize": 10485760,
                            "idempotencyTtlMs": 86400000,
                        },
                        "version": "0.1.0",
                        "gitCommit": "abc1234",
                        "admins": [{"email": "admin@example.com"}],
                    },
                )
            }
        )
    ) as c:
        cfg = c.get_config()
    assert isinstance(cfg, ConfigResponse)
    assert cfg.port == 8300
    assert cfg.hot.session_ttl_days == 30
    assert cfg.admins == [AdminMember(email="admin@example.com")]


def test_patch_config_sends_camel_case_and_parses_response() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "port": 8300,
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
                    "allowedOrigins": ["https://rtdb.example"],
                    "sessionTtlDays": 60,
                    "maxFileSize": 10485760,
                    "idempotencyTtlMs": 86400000,
                },
                "version": "0.1.0",
                "gitCommit": "abc1234",
                "admins": [],
            },
        )

    with _sync_client(handler) as c:
        cfg = c.patch_config(HotConfigPatch(session_ttl_days=60))
    assert captured["method"] == "PATCH"
    assert captured["path"] == "/admin/config"
    assert captured["body"] == {"sessionTtlDays": 60}
    assert isinstance(cfg, ConfigResponse)
    assert cfg.hot.session_ttl_days == 60


def test_backup_now_posts_empty_body() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    with _sync_client(handler) as c:
        c.backup_now()
    assert captured["path"] == "/admin/backup"
    assert captured["body"] == {}


def test_admin_query_posts_query_and_parses_result() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"result": [{"_id": "a"}, {"_id": "b"}]})

    with _sync_client(handler) as c:
        docs = c.admin_query("dbx", TableQuery("items").take(2))
    assert captured["path"] == "/admin/db/dbx/query"
    # db rides in the URL, not the body
    assert "db" not in captured["body"]
    assert "query" in captured["body"]
    assert len(docs) == 2


def test_admin_mutate_posts_txn_and_parses_step_result() -> None:
    captured: dict[str, Any] = {}
    txn = Mutation.builder().insert("items", {"_id": "i1", "sku": "A"}).build()

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"results": [{"id": "i1"}, None]})

    with _sync_client(handler) as c:
        results = c.admin_mutate("dbx", txn)
    assert captured["path"] == "/admin/db/dbx/mutate"
    assert "db" not in captured["body"]
    assert "txn" in captured["body"]
    assert len(results) == 2


def test_db_stats_parses_table_stats() -> None:
    with _sync_client(
        _handler_map(
            {
                ("GET", "/admin/dbs/dbx/stats", ""): httpx.Response(
                    200,
                    json={
                        "tables": [{"name": "items", "rowCount": 5, "sizeBytes": 4096}],
                        "totalSizeBytes": 4096,
                    },
                )
            }
        )
    ) as c:
        stats = c.db_stats("dbx")
    assert isinstance(stats, DbStats)
    assert stats.tables[0].row_count == 5
    assert stats.total_size_bytes == 4096


def test_metrics_parses_snapshot() -> None:
    with _sync_client(
        _handler_map(
            {
                ("GET", "/admin/metrics", ""): httpx.Response(
                    200,
                    json={
                        "queriesTotal": 10,
                        "mutationsTotal": 3,
                        "uploadsTotal": 0,
                        "wsConnections": 2,
                        "activeSubscriptions": 4,
                        "poolSize": 8,
                        "poolIdle": 5,
                        "uptimeSeconds": 99,
                        "queryLatency": {"p50": 1, "p95": 2, "p99": 3},
                        "mutateLatency": {"p50": 4, "p95": 5, "p99": 6},
                        "subscribeLatency": {"p50": 7, "p95": 8, "p99": 9},
                    },
                )
            }
        )
    ) as c:
        snap = c.metrics()
    assert isinstance(snap, MetricsSnapshot)
    assert snap.queries_total == 10
    assert snap.query_latency.p99 == 3


def test_ops_recent_parses_events() -> None:
    with _sync_client(
        _handler_map(
            {
                ("GET", "/admin/ops/recent", ""): httpx.Response(
                    200,
                    json={
                        "ops": [
                            {
                                "db": "dbx",
                                "table": "items",
                                "docId": "i1",
                                "kind": "insert",
                                "ts": 1700000000000,
                                "owner": "u@e.com",
                            }
                        ]
                    },
                )
            }
        )
    ) as c:
        ops = c.ops_recent(db="dbx", n=5)
    assert len(ops) == 1
    assert isinstance(ops[0], OpEvent)
    assert ops[0].kind == "insert"
    assert ops[0].owner == "u@e.com"


# --- async: mirrored admin surface (representative) ----------------------


async def test_async_list_dbs_gets_databases_list() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["auth"] = request.headers["authorization"]
        return httpx.Response(200, json={"databases": ["x", "y"]})

    async with _async_client(handler) as c:
        dbs = await c.list_dbs()
    assert dbs == ["x", "y"]
    assert captured["auth"] == ADMIN_BEARER


async def test_async_create_db_posts_name() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    async with _async_client(handler) as c:
        await c.create_db("adb")
    assert captured["path"] == "/admin/create-db"
    assert captured["body"] == {"name": "adb"}


async def test_async_admin_query_posts_query_and_parses_result() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        return httpx.Response(200, json={"result": [{"_id": "a"}, {"_id": "b"}]})

    async with _async_client(handler) as c:
        docs = await c.admin_query("dbx", TableQuery("items").take(2))
    assert captured["path"] == "/admin/db/dbx/query"
    assert len(docs) == 2


async def test_async_backup_now_posts_empty_body() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    async with _async_client(handler) as c:
        await c.backup_now()
    assert captured["path"] == "/admin/backup"
    assert captured["body"] == {}
