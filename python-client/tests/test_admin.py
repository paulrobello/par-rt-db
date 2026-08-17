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

from par_rt_db import AfterMs, Cron, Mutation, TableQuery, t
from par_rt_db.admin import (
    AdminMember,
    AsyncRtDbAdminClient,
    AuditEntry,
    ConfigResponse,
    DbStats,
    ExplainResult,
    HotConfigPatch,
    MergeReport,
    MetricsSnapshot,
    MintedToken,
    OpEvent,
    RtDbAdminClient,
    SchemaHistoryEntry,
    SchemaHistorySummary,
    SchemaPreviewDiff,
    SessionInfo,
    SlowQueriesResponse,
    SubscriptionsResponse,
    TokenInfo,
    Webhook,
    WebhookDelivery,
)
from par_rt_db.admin_models import MergeConflict, MergeDbResult
from par_rt_db.errors import ErrorCode, RtDbError
from par_rt_db.http_client import FileMetadata, SubscriptionInfo
from par_rt_db.schema import Schema
from par_rt_db.wire import ScheduleInfo, WorkflowInfo, WorkflowInfoFull

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


# --- sync: interactive sessions (GET/DELETE /admin/sessions) --------------


def test_list_sessions_parses_rows_and_sends_filters() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = request.url
        return httpx.Response(
            200,
            json={
                "sessions": [
                    {
                        "tokenHash": "abc",
                        "userId": "u1",
                        "email": "a@x.example",
                        "login": None,
                        "anonymous": False,
                        "createdAt": 1000,
                        "expiresAt": 2000,
                    },
                    {
                        "tokenHash": "def",
                        "userId": "u1",
                        "email": None,
                        "login": None,
                        "anonymous": True,
                        "createdAt": 1100,
                        "expiresAt": 2100,
                    },
                ]
            },
        )

    with _sync_client(handler) as c:
        rows = c.list_sessions(user="u1", limit=50)
    assert len(rows) == 2
    assert all(isinstance(r, SessionInfo) for r in rows)
    assert rows[0].token_hash == "abc"
    assert rows[0].user_id == "u1"
    assert rows[0].email == "a@x.example"
    assert rows[0].login is None
    assert rows[0].anonymous is False
    assert rows[1].email is None
    assert rows[1].anonymous is True
    assert captured["url"].path == "/admin/sessions"
    assert captured["url"].params["user"] == "u1"
    assert captured["url"].params["limit"] == "50"


def test_list_sessions_omits_unset_filters() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = request.url
        return httpx.Response(200, json={"sessions": []})

    with _sync_client(handler) as c:
        rows = c.list_sessions()
    assert rows == []
    assert "user" not in captured["url"].params
    assert "limit" not in captured["url"].params


def test_revoke_session_deletes_by_token_hash() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = request.url
        return httpx.Response(200, json={"ok": True})

    with _sync_client(handler) as c:
        c.revoke_session("abc123")
    assert captured["method"] == "DELETE"
    assert captured["url"].path == "/admin/sessions/abc123"


def test_revoke_user_sessions_deletes_with_user_query_and_parses_count() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = request.url
        return httpx.Response(200, json={"ok": True, "revoked": 3})

    with _sync_client(handler) as c:
        count = c.revoke_user_sessions("u1")
    assert count == 3
    assert captured["method"] == "DELETE"
    assert captured["url"].path == "/admin/sessions"
    assert captured["url"].params["user"] == "u1"


# --- sync: user merge (POST /admin/merge-users) ---------------------------


def test_merge_users_posts_typed_confirm_and_parses_report() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = request.url
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "dbs": {
                    "kanban": {
                        "tables": {"cards": 3, "sessions": 1},
                        "conflicts": [{"table": "cards", "id": "c9"}],
                    }
                },
                "storageRepointed": 2,
                "sessionsRepointed": 1,
                "anonDeleted": True,
            },
        )

    with _sync_client(handler) as c:
        report = c.merge_users("anon1", "real1")
    assert captured["method"] == "POST"
    assert captured["url"].path == "/admin/merge-users"
    assert captured["body"] == {
        "anonUserId": "anon1",
        "realUserId": "real1",
        "confirm": "real1",
    }
    assert isinstance(report, MergeReport)
    assert isinstance(report.dbs["kanban"], MergeDbResult)
    assert report.dbs["kanban"].tables == {"cards": 3, "sessions": 1}
    conflict = report.dbs["kanban"].conflicts[0]
    assert isinstance(conflict, MergeConflict)
    assert conflict.table == "cards"
    assert conflict.id == "c9"
    assert report.storage_repointed == 2
    assert report.sessions_repointed == 1
    assert report.anon_deleted is True


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


async def test_async_list_revoke_sessions_mirror_sync() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.setdefault("urls", []).append(
            (request.method, request.url.path, dict(request.url.params))
        )
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "sessions": [
                        {
                            "tokenHash": "abc",
                            "userId": "u1",
                            "email": None,
                            "login": None,
                            "anonymous": False,
                            "createdAt": 1,
                            "expiresAt": 2,
                        }
                    ]
                },
            )
        if request.url.path == "/admin/sessions/abc":
            return httpx.Response(200, json={"ok": True})
        return httpx.Response(200, json={"ok": True, "revoked": 2})

    async with _async_client(handler) as c:
        rows = await c.list_sessions(user="u1")
        await c.revoke_session("abc")
        count = await c.revoke_user_sessions("u1")
    assert len(rows) == 1
    assert rows[0].token_hash == "abc"
    assert count == 2
    assert ("GET", "/admin/sessions", {"user": "u1"}) in captured["urls"]
    assert ("DELETE", "/admin/sessions/abc", {}) in captured["urls"]
    assert ("DELETE", "/admin/sessions", {"user": "u1"}) in captured["urls"]


async def test_async_merge_users_mirrors_sync() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={"dbs": {}, "storageRepointed": 0, "sessionsRepointed": 0, "anonDeleted": False},
        )

    async with _async_client(handler) as c:
        report = await c.merge_users("anon1", "real1")
    assert isinstance(report, MergeReport)
    assert report.dbs == {}
    assert report.storage_repointed == 0
    assert report.sessions_repointed == 0
    assert report.anon_deleted is False
    assert captured["method"] == "POST"
    assert captured["path"] == "/admin/merge-users"
    assert captured["body"] == {
        "anonUserId": "anon1",
        "realUserId": "real1",
        "confirm": "real1",
    }


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


def test_list_schema_history_parses_summaries() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["params"] = dict(request.url.params)
        return httpx.Response(
            200,
            json={
                "entries": [
                    {"version": 3, "capturedAt": 30, "source": "migrate", "principal": "u@x"},
                    {"version": 2, "capturedAt": 20, "source": "push", "principal": None},
                ]
            },
        )

    with _sync_client(handler) as c:
        entries = c.list_schema_history("kanban", limit=5)
    assert captured["path"] == "/admin/db/kanban/schema/history"
    assert captured["params"] == {"limit": "5"}
    assert len(entries) == 2
    assert all(isinstance(e, SchemaHistorySummary) for e in entries)
    assert entries[0].version == 3
    assert entries[0].source == "migrate"
    assert entries[0].principal == "u@x"
    assert entries[1].principal is None


def test_get_schema_version_returns_entry_with_schema_blob() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/admin/db/kanban/schema/history/3"
        return httpx.Response(
            200,
            json={
                "version": 3,
                "capturedAt": 30,
                "source": "restore",
                "principal": None,
                "schema": {"tables": {"notes": {"fields": {"body": {"type": "string"}}}}},
            },
        )

    with _sync_client(handler) as c:
        entry = c.get_schema_version("kanban", 3)
    assert isinstance(entry, SchemaHistoryEntry)
    assert entry.version == 3
    assert entry.source == "restore"
    assert entry.principal is None
    assert entry.schema_["tables"]["notes"]["fields"]["body"]["type"] == "string"


def test_restore_schema_posts_version_and_confirm() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True, "restoredTo": 2})

    with _sync_client(handler) as c:
        restored_to = c.restore_schema("kanban", 2, confirm="kanban")
    assert restored_to == 2
    assert captured["path"] == "/admin/db/kanban/schema/restore"
    assert captured["body"] == {"version": 2, "confirm": "kanban"}


async def test_async_restore_schema_posts_version_and_confirm() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True, "restoredTo": 4})

    async with _async_client(handler) as c:
        restored_to = await c.restore_schema("kanban", 4, confirm="kanban")
    assert restored_to == 4
    assert captured["path"] == "/admin/db/kanban/schema/restore"
    assert captured["body"] == {"version": 4, "confirm": "kanban"}


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
        stats = c.db_stats("dbx")
    assert isinstance(stats, DbStats)
    assert stats.tables[0].row_count == 5
    assert stats.total_size_bytes == 4096
    # ENH-011 quota/usage fields (ARC-112): all six parse to their snake_case attrs.
    assert stats.tables_quota == 10
    assert stats.tables_used == 1
    assert stats.storage_quota_bytes == 1048576
    assert stats.storage_used_bytes == 4096
    assert stats.subs_quota == 50
    assert stats.subs_used == 3


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


# --- webhook surface (ENH-003) -------------------------------------------
#
# ``MockTransport`` tests mirroring ``ts-client``'s admin.test.ts webhook
# suite. Asserts method/path/body for each of the five methods, especially
# the ``edit_webhook`` ``table`` tri-state (omit / null / string), and the
# ``list_deliveries`` query-string build. Parses both ``Webhook`` rows
# (``table:null`` and ``table:"x"``) and ``WebhookDelivery`` fixture rows
# (including ``lastError:null`` and opaque ``payload``).


_WEBHOOK_ROW_ALL_TABLES = {
    "id": 7,
    "db": "kanban",
    "table": None,
    "url": "https://hooks.example/all",
    "events": ["*"],
    "createdAt": 1700000000000,
    "enabled": True,
}
_WEBHOOK_ROW_TABLED = {
    "id": 11,
    "db": "kanban",
    "table": "items",
    "url": "https://hooks.example/items",
    "events": ["insert", "patch"],
    "createdAt": 1700000000001,
    "enabled": False,
}
_DELIVERY_ROW_OK = {
    "id": 201,
    "attempts": 1,
    "status": "delivered",
    "nextAttempt": 1700000005000,
    "lastError": None,
    "payload": {
        "db": "kanban",
        "table": "items",
        "docId": "i1",
        "kind": "insert",
        "ts": 1700000004000,
        "owner": "u@e.com",
    },
}
_DELIVERY_ROW_FAILED = {
    "id": 202,
    "attempts": 5,
    "status": "failed",
    "nextAttempt": 1700000010000,
    "lastError": "HTTP 503",
    "payload": {"arbitrary": ["nested", {"obj": True}]},
}


def test_webhook_model_validate_table_null_means_all_tables() -> None:
    wh = Webhook.model_validate(_WEBHOOK_ROW_ALL_TABLES)
    assert isinstance(wh, Webhook)
    assert wh.id == 7
    assert wh.table is None  # all-tables
    assert wh.events == ["*"]
    assert wh.enabled is True
    assert wh.created_at == 1700000000000


def test_webhook_model_validate_table_string_is_tabled() -> None:
    wh = Webhook.model_validate(_WEBHOOK_ROW_TABLED)
    assert wh.table == "items"
    assert wh.events == ["insert", "patch"]
    assert wh.enabled is False


def test_webhook_delivery_model_validate_last_error_null() -> None:
    d = WebhookDelivery.model_validate(_DELIVERY_ROW_OK)
    assert isinstance(d, WebhookDelivery)
    assert d.id == 201
    assert d.attempts == 1
    assert d.status == "delivered"
    assert d.last_error is None
    assert d.payload["docId"] == "i1"


def test_webhook_delivery_model_validate_failed_with_error_and_opaque_payload() -> None:
    d = WebhookDelivery.model_validate(_DELIVERY_ROW_FAILED)
    assert d.status == "failed"
    assert d.attempts == 5
    assert d.last_error == "HTTP 503"
    assert d.payload == {"arbitrary": ["nested", {"obj": True}]}


def test_list_webhooks_parses_mixed_rows() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        return httpx.Response(
            200,
            json={"webhooks": [_WEBHOOK_ROW_ALL_TABLES, _WEBHOOK_ROW_TABLED]},
        )

    with _sync_client(handler) as c:
        rows = c.list_webhooks("kanban")
    assert captured["method"] == "GET"
    assert captured["path"] == "/admin/db/kanban/webhooks"
    assert len(rows) == 2
    assert all(isinstance(r, Webhook) for r in rows)
    assert rows[0].table is None
    assert rows[1].table == "items"
    assert rows[1].enabled is False


def test_list_webhooks_empty_when_disabled_or_none() -> None:
    with _sync_client(
        _handler_map(
            {("GET", "/admin/db/kanban/webhooks", ""): httpx.Response(200, json={"webhooks": []})}
        )
    ) as c:
        rows = c.list_webhooks("kanban")
    assert rows == []


def test_create_webhook_posts_only_url_when_no_optionals() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"id": 42})

    with _sync_client(handler) as c:
        new_id = c.create_webhook("kanban", url="https://hooks.example/x")
    assert new_id == 42
    assert captured["method"] == "POST"
    assert captured["path"] == "/admin/db/kanban/webhooks"
    assert captured["body"] == {"url": "https://hooks.example/x"}
    assert "table" not in captured["body"]
    assert "events" not in captured["body"]
    assert "enabled" not in captured["body"]


def test_create_webhook_posts_table_events_enabled_when_set() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"id": 99})

    with _sync_client(handler) as c:
        new_id = c.create_webhook(
            "kanban",
            url="https://hooks.example/y",
            table="items",
            events=["insert", "patch"],
            enabled=False,
        )
    assert new_id == 99
    assert captured["body"] == {
        "url": "https://hooks.example/y",
        "table": "items",
        "events": ["insert", "patch"],
        "enabled": False,
    }


def test_edit_webhook_omits_table_when_not_passed() -> None:
    """``table`` not passed → omit from body → server leaves it unchanged."""
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json=_WEBHOOK_ROW_TABLED)

    with _sync_client(handler) as c:
        wh = c.edit_webhook("kanban", 11, enabled=False)
    assert captured["method"] == "PUT"
    assert captured["path"] == "/admin/db/kanban/webhooks/11"
    assert captured["body"] == {"enabled": False}
    assert "table" not in captured["body"]
    assert isinstance(wh, Webhook)


def test_edit_webhook_sends_table_null_to_clear() -> None:
    """``table=None`` → JSON ``null`` → server clears to all-tables."""
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json=_WEBHOOK_ROW_ALL_TABLES)

    with _sync_client(handler) as c:
        wh = c.edit_webhook("kanban", 11, table=None)
    assert captured["body"] == {"table": None}
    assert isinstance(wh, Webhook)
    assert wh.table is None


def test_edit_webhook_sends_table_string_to_set() -> None:
    """``table="items"`` → JSON ``"items"`` → set to that table."""
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json=_WEBHOOK_ROW_TABLED)

    with _sync_client(handler) as c:
        wh = c.edit_webhook("kanban", 11, table="items")
    assert captured["body"] == {"table": "items"}
    assert wh.table == "items"


def test_edit_webhook_sends_multiple_fields_together() -> None:
    """Multiple kwargs at once compose into one body."""
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json=_WEBHOOK_ROW_TABLED)

    with _sync_client(handler) as c:
        c.edit_webhook(
            "kanban",
            11,
            url="https://hooks.example/z",
            table=None,
            events=["delete"],
            enabled=True,
        )
    assert captured["body"] == {
        "url": "https://hooks.example/z",
        "table": None,
        "events": ["delete"],
        "enabled": True,
    }


def test_edit_webhook_rotate_secret_sends_camel_case_flag() -> None:
    """`rotate_secret=True` is sent as the camelCase `rotateSecret` flag and
    omitted when not passed (SEC-115 client mirror of the server's
    `rotate_secret: bool` PUT body field)."""
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json=_WEBHOOK_ROW_TABLED)

    with _sync_client(handler) as c:
        c.edit_webhook("kanban", 11, rotate_secret=True)
    assert captured["body"] == {"rotateSecret": True}

    # Omitted → absent from body (server default `false` applies).
    captured.clear()
    with _sync_client(handler) as c:
        c.edit_webhook("kanban", 11, enabled=False)
    assert "rotateSecret" not in captured["body"]


def test_delete_webhook_returns_none_on_ok() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        return httpx.Response(200, json={"ok": True})

    with _sync_client(handler) as c:
        result = c.delete_webhook("kanban", 11)
    assert result is None
    assert captured["method"] == "DELETE"
    assert captured["path"] == "/admin/db/kanban/webhooks/11"


def test_delete_webhook_raises_on_ok_false() -> None:
    """Missing webhook id → server returns ``ok:false`` (404 body) → raise."""
    with (
        _sync_client(
            _handler_map(
                {
                    ("DELETE", "/admin/db/kanban/webhooks/999", ""): httpx.Response(
                        404, json={"code": "NOT_FOUND", "message": "no such webhook"}
                    )
                }
            )
        ) as c,
        pytest.raises(RtDbError),
    ):
        c.delete_webhook("kanban", 999)


def test_list_deliveries_builds_query_params_when_all_set() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["params"] = dict(request.url.params)
        return httpx.Response(200, json={"deliveries": [_DELIVERY_ROW_OK, _DELIVERY_ROW_FAILED]})

    with _sync_client(handler) as c:
        rows = c.list_deliveries("kanban", 11, status="retrying", limit=50, offset=10)
    assert captured["path"] == "/admin/db/kanban/webhooks/11/deliveries"
    assert captured["params"] == {"status": "retrying", "limit": "50", "offset": "10"}
    assert len(rows) == 2
    assert all(isinstance(r, WebhookDelivery) for r in rows)
    assert rows[0].last_error is None
    assert rows[1].status == "failed"
    assert rows[1].last_error == "HTTP 503"


def test_list_deliveries_omits_unset_query_params() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["params"] = dict(request.url.params)
        return httpx.Response(200, json={"deliveries": []})

    with _sync_client(handler) as c:
        rows = c.list_deliveries("kanban", 11)
    assert rows == []
    assert captured["params"] == {}


def test_list_deliveries_parses_fixture_with_opaque_payload() -> None:
    with _sync_client(
        _handler_map(
            {
                ("GET", "/admin/db/kanban/webhooks/11/deliveries", ""): httpx.Response(
                    200, json={"deliveries": [_DELIVERY_ROW_FAILED]}
                )
            }
        )
    ) as c:
        rows = c.list_deliveries("kanban", 11)
    assert len(rows) == 1
    d = rows[0]
    assert d.attempts == 5
    assert d.next_attempt == 1700000010000
    assert d.payload == {"arbitrary": ["nested", {"obj": True}]}


# --- async: webhook mirror (representative) -------------------------------


async def test_async_list_webhooks_parses_rows() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["auth"] = request.headers["authorization"]
        captured["path"] = request.url.path
        return httpx.Response(
            200,
            json={"webhooks": [_WEBHOOK_ROW_ALL_TABLES, _WEBHOOK_ROW_TABLED]},
        )

    async with _async_client(handler) as c:
        rows = await c.list_webhooks("kanban")
    assert captured["auth"] == ADMIN_BEARER
    assert captured["path"] == "/admin/db/kanban/webhooks"
    assert len(rows) == 2
    assert rows[0].table is None
    assert rows[1].table == "items"


async def test_async_create_webhook_posts_body_and_returns_id() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"id": 7})

    async with _async_client(handler) as c:
        new_id = await c.create_webhook("kanban", url="https://hooks.example/x", events=["insert"])
    assert new_id == 7
    assert captured["body"] == {"url": "https://hooks.example/x", "events": ["insert"]}


async def test_async_edit_webhook_omits_unset_table() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json=_WEBHOOK_ROW_TABLED)

    async with _async_client(handler) as c:
        wh = await c.edit_webhook("kanban", 11, enabled=False)
    assert captured["body"] == {"enabled": False}
    assert "table" not in captured["body"]
    assert isinstance(wh, Webhook)


async def test_async_edit_webhook_sends_table_null_to_clear() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json=_WEBHOOK_ROW_ALL_TABLES)

    async with _async_client(handler) as c:
        wh = await c.edit_webhook("kanban", 11, table=None)
    assert captured["body"] == {"table": None}
    assert wh.table is None


async def test_async_delete_webhook_hits_correct_route() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        return httpx.Response(200, json={"ok": True})

    async with _async_client(handler) as c:
        await c.delete_webhook("kanban", 11)
    assert captured["method"] == "DELETE"
    assert captured["path"] == "/admin/db/kanban/webhooks/11"


async def test_async_list_deliveries_builds_query_params() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["params"] = dict(request.url.params)
        return httpx.Response(200, json={"deliveries": [_DELIVERY_ROW_OK]})

    async with _async_client(handler) as c:
        rows = await c.list_deliveries("kanban", 11, status="delivered", limit=10)
    assert captured["path"] == "/admin/db/kanban/webhooks/11/deliveries"
    assert captured["params"] == {"status": "delivered", "limit": "10"}
    assert len(rows) == 1
    assert rows[0].status == "delivered"


# --- audit log surface (ENH-004) -----------------------------------------
#
# ``MockTransport`` tests mirroring the webhook/deliveries suite. Asserts the
# ``GET /admin/audit`` route + the query-string build (omit ``None`` filters,
# explicit ``0`` for limit/offset survives), and parses an ``entries`` fixture
# including a row with ``op:null``/``principal:null`` (system-initiated write).


_AUDIT_ROW_USER_WRITE = {
    "id": 301,
    "tsMs": 1700000004000,
    "db": "kanban",
    "table": "items",
    "op": "insert",
    "docId": "i1",
    "principal": "u@e.com",
    "source": "client",
}
_AUDIT_ROW_SYSTEM_WRITE = {
    "id": 302,
    "tsMs": 1700000005000,
    "db": "kanban",
    "table": "items",
    "op": None,
    "docId": "i2",
    "principal": None,
    "source": "ttl",
}


def test_audit_entry_model_validate_maps_camelcase() -> None:
    e = AuditEntry.model_validate(_AUDIT_ROW_USER_WRITE)
    assert isinstance(e, AuditEntry)
    assert e.id == 301
    assert e.ts_ms == 1700000004000
    assert e.db == "kanban"
    assert e.table == "items"
    assert e.op == "insert"
    assert e.doc_id == "i1"
    assert e.principal == "u@e.com"
    assert e.source == "client"


def test_audit_entry_model_validate_system_write_nones() -> None:
    """System-initiated write (TTL/scheduler): ``op:null`` and ``principal:null``."""
    e = AuditEntry.model_validate(_AUDIT_ROW_SYSTEM_WRITE)
    assert e.op is None
    assert e.principal is None
    assert e.source == "ttl"
    assert e.doc_id == "i2"


def test_get_audit_builds_query_params_when_all_set() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["params"] = dict(request.url.params)
        return httpx.Response(
            200,
            json={"entries": [_AUDIT_ROW_USER_WRITE, _AUDIT_ROW_SYSTEM_WRITE]},
        )

    with _sync_client(handler) as c:
        rows = c.get_audit(
            "kanban",
            table="items",
            op="insert",
            principal="u@e.com",
            source="client",
            limit=50,
            offset=10,
        )
    assert captured["method"] == "GET"
    assert captured["path"] == "/admin/audit"
    assert captured["params"] == {
        "db": "kanban",
        "table": "items",
        "op": "insert",
        "principal": "u@e.com",
        "source": "client",
        "limit": "50",
        "offset": "10",
    }
    assert len(rows) == 2
    assert all(isinstance(r, AuditEntry) for r in rows)
    assert rows[0].op == "insert"
    assert rows[1].op is None
    assert rows[1].principal is None


def test_get_audit_omits_unset_filters() -> None:
    """Omitted filter opts are absent from the query string; ``db`` is always sent."""
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["params"] = dict(request.url.params)
        return httpx.Response(200, json={"entries": []})

    with _sync_client(handler) as c:
        rows = c.get_audit("kanban")
    assert rows == []
    assert captured["params"] == {"db": "kanban"}


def test_get_audit_explicit_zero_limit_offset_survives() -> None:
    """An explicit ``0`` for limit/offset must be sent (not omitted as a falsy)."""
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["params"] = dict(request.url.params)
        return httpx.Response(200, json={"entries": []})

    with _sync_client(handler) as c:
        c.get_audit("kanban", limit=0, offset=0)
    assert captured["params"] == {"db": "kanban", "limit": "0", "offset": "0"}


def test_get_audit_parses_fixture_with_none_op_principal() -> None:
    with _sync_client(
        _handler_map(
            {
                ("GET", "/admin/audit", ""): httpx.Response(
                    200, json={"entries": [_AUDIT_ROW_SYSTEM_WRITE]}
                )
            }
        )
    ) as c:
        rows = c.get_audit("kanban", source="ttl")
    assert len(rows) == 1
    e = rows[0]
    assert e.id == 302
    assert e.ts_ms == 1700000005000
    assert e.op is None
    assert e.principal is None
    assert e.source == "ttl"


def test_get_audit_empty_when_disabled() -> None:
    """Audit disabled at boot → server short-circuits to ``{entries:[]}``."""
    with _sync_client(
        _handler_map({("GET", "/admin/audit", ""): httpx.Response(200, json={"entries": []})})
    ) as c:
        rows = c.get_audit("kanban")
    assert rows == []


async def test_async_get_audit_builds_query_params() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["auth"] = request.headers["authorization"]
        captured["path"] = request.url.path
        captured["params"] = dict(request.url.params)
        return httpx.Response(
            200,
            json={"entries": [_AUDIT_ROW_USER_WRITE]},
        )

    async with _async_client(handler) as c:
        rows = await c.get_audit("kanban", table="items", op="insert", limit=10, offset=5)
    assert captured["auth"] == ADMIN_BEARER
    assert captured["path"] == "/admin/audit"
    assert captured["params"] == {
        "db": "kanban",
        "table": "items",
        "op": "insert",
        "limit": "10",
        "offset": "5",
    }
    assert len(rows) == 1
    assert isinstance(rows[0], AuditEntry)
    assert rows[0].doc_id == "i1"


# --- ENH-010: live subscription inspector ---------------------------------

_SUBS_ROW_INTERACTIVE = {
    "db": "kanban",
    "table": "items",
    "terminal": "collect",
    "readSetClass": "indexed",
    "principal": {"userId": "u1", "email": "u@e.com"},
}
_SUBS_ROW_MACHINE = {
    "db": "kanban",
    "table": "items",
    "terminal": "count",
    "readSetClass": "point",
    "principal": None,
}
_SUBS_RESPONSE = {
    "subscriptions": [_SUBS_ROW_INTERACTIVE, _SUBS_ROW_MACHINE],
    "subsRerunsTotal": 7,
    "subsSkipsPointTotal": 3,
    "subsSkipsIndexedTotal": 2,
    "subsSkipsOrderedTotal": 1,
    "subsMissedPushesTotal": 0,
    "perDb": [
        {
            "db": "kanban",
            "reruns": 7,
            "skipsPoint": 3,
            "skipsIndexed": 2,
            "skipsOrdered": 1,
            "missed": 0,
            "skips": 6,
            "rerunRatio": 0.5384615384615384,
        }
    ],
}


def test_subscriptions_response_model_validate_maps_camelcase() -> None:
    r = SubscriptionsResponse.model_validate(_SUBS_RESPONSE)
    assert isinstance(r, SubscriptionsResponse)
    assert r.subs_reruns_total == 7
    assert r.subs_skips_point_total == 3
    assert r.subs_skips_indexed_total == 2
    assert r.subs_skips_ordered_total == 1
    assert r.subs_missed_pushes_total == 0
    assert len(r.subscriptions) == 2
    interactive = r.subscriptions[0]
    assert isinstance(interactive, SubscriptionInfo)
    assert interactive.table == "items"
    assert interactive.terminal == "collect"
    assert interactive.read_set_class == "indexed"
    assert interactive.principal is not None
    assert interactive.principal.user_id == "u1"
    assert interactive.principal.email == "u@e.com"
    machine = r.subscriptions[1]
    assert machine.read_set_class == "point"
    assert machine.principal is None
    assert len(r.per_db) == 1
    assert r.per_db[0].db == "kanban"
    assert r.per_db[0].skips_indexed == 2
    # ENH-024 totals: skips sums the classes, rerun_ratio = 7/(7+6).
    assert r.per_db[0].skips == 6
    assert r.per_db[0].rerun_ratio == 7 / 13


def test_get_subscriptions_no_db_filter_omits_param() -> None:
    """With no ``db`` filter the query string carries no params."""
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["params"] = dict(request.url.params)
        return httpx.Response(200, json=_SUBS_RESPONSE)

    with _sync_client(handler) as c:
        r = c.get_subscriptions()
    assert captured["method"] == "GET"
    assert captured["path"] == "/admin/subscriptions"
    assert captured["params"] == {}
    assert isinstance(r, SubscriptionsResponse)
    assert len(r.subscriptions) == 2


def test_get_subscriptions_with_db_filter_sends_param() -> None:
    """A ``db`` filter is sent as the ``db`` query param."""
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["params"] = dict(request.url.params)
        return httpx.Response(200, json=_SUBS_RESPONSE)

    with _sync_client(handler) as c:
        r = c.get_subscriptions("kanban")
    assert captured["params"] == {"db": "kanban"}
    assert isinstance(r, SubscriptionsResponse)
    assert all(s.db == "kanban" for s in r.subscriptions)


async def test_async_get_subscriptions_builds_request() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["auth"] = request.headers["authorization"]
        captured["path"] = request.url.path
        captured["params"] = dict(request.url.params)
        return httpx.Response(200, json=_SUBS_RESPONSE)

    async with _async_client(handler) as c:
        r = await c.get_subscriptions("kanban")
    assert captured["auth"] == ADMIN_BEARER
    assert captured["path"] == "/admin/subscriptions"
    assert captured["params"] == {"db": "kanban"}
    assert isinstance(r, SubscriptionsResponse)
    assert r.subs_reruns_total == 7
    assert r.per_db[0].reruns == 7


def test_metrics_snapshot_includes_per_db_subs() -> None:
    """``perDbSubs`` on ``/admin/metrics`` parses into ``per_db_subs`` and stays
    optional (older servers omit it → empty list)."""
    with _sync_client(
        _handler_map(
            {
                ("GET", "/admin/metrics", ""): httpx.Response(
                    200,
                    json={
                        "queriesTotal": 1,
                        "mutationsTotal": 0,
                        "uploadsTotal": 0,
                        "wsConnections": 1,
                        "activeSubscriptions": 2,
                        "poolSize": 4,
                        "poolIdle": 3,
                        "uptimeSeconds": 10,
                        "queryLatency": {"p50": 1, "p95": 2, "p99": 3},
                        "mutateLatency": {"p50": 4, "p95": 5, "p99": 6},
                        "subscribeLatency": {"p50": 7, "p95": 8, "p99": 9},
                        "perDbSubs": [
                            {
                                "db": "kanban",
                                "reruns": 5,
                                "skipsPoint": 1,
                                "skipsIndexed": 1,
                                "skipsOrdered": 0,
                                "missed": 0,
                            }
                        ],
                    },
                )
            }
        )
    ) as c:
        snap = c.metrics()
    assert isinstance(snap, MetricsSnapshot)
    assert len(snap.per_db_subs) == 1
    assert snap.per_db_subs[0].db == "kanban"
    assert snap.per_db_subs[0].reruns == 5


# --- ENH-019: query introspection (explain + slow-queries) ----------------


_EXPLAIN_RESPONSE = {
    "sql": 'SELECT "doc" FROM "items" WHERE ("doc" @> $1) LIMIT $2',
    "params": ["active", "50"],
    "terminal": "collect",
    "warnings": ["over-approximated: membership check"],
}


def test_explain_result_model_validate_maps_camelcase() -> None:
    r = ExplainResult.model_validate(_EXPLAIN_RESPONSE)
    assert isinstance(r, ExplainResult)
    assert r.sql.startswith("SELECT")
    assert r.params == ["active", "50"]
    assert r.terminal == "collect"
    assert r.warnings == ["over-approximated: membership check"]


def test_explain_query_posts_query_body_and_parses_result() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json=_EXPLAIN_RESPONSE)

    with _sync_client(handler) as c:
        result = c.explain_query("dbx", TableQuery("items").take(50))
    assert captured["method"] == "POST"
    assert captured["path"] == "/admin/db/dbx/explain"
    # db rides in the URL, not the body — mirrors admin_query
    assert "db" not in captured["body"]
    assert "query" in captured["body"]
    assert isinstance(result, ExplainResult)
    assert result.terminal == "collect"
    assert result.params == ["active", "50"]


def test_explain_query_accepts_built_query() -> None:
    """A pre-built ``Query`` (not a ``TableQuery``) is sent as-is."""
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json=_EXPLAIN_RESPONSE)

    built = TableQuery("items").take(2).build()
    with _sync_client(handler) as c:
        c.explain_query("dbx", built)
    assert "query" in captured["body"]


async def test_async_explain_query_builds_request() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["auth"] = request.headers["authorization"]
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json=_EXPLAIN_RESPONSE)

    async with _async_client(handler) as c:
        result = await c.explain_query("dbx", TableQuery("items").take(2))
    assert captured["auth"] == ADMIN_BEARER
    assert captured["path"] == "/admin/db/dbx/explain"
    assert isinstance(result, ExplainResult)
    assert result.sql.startswith("SELECT")


_SLOW_QUERY_ROW_WITH_PARAMS = {
    "startedAtMs": 1700000000000,
    "durationMs": 42,
    "db": "kanban",
    "table": "items",
    "terminal": "collect",
    "sql": 'SELECT "doc" FROM "items"',
    "params": ["active"],
}
_SLOW_QUERY_ROW_REDACTED = {
    "startedAtMs": 1700000001000,
    "durationMs": 99,
    "db": "other",
    "table": "projects",
    "terminal": "first",
    "sql": 'SELECT "doc" FROM "projects" LIMIT 1',
    # params omitted — redacted by the server
}
_SLOW_QUERIES_RESPONSE = {
    "queries": [_SLOW_QUERY_ROW_WITH_PARAMS, _SLOW_QUERY_ROW_REDACTED],
    "thresholdMs": 0,
    "capacity": 200,
}


def test_slow_queries_response_model_validate_maps_camelcase() -> None:
    r = SlowQueriesResponse.model_validate(_SLOW_QUERIES_RESPONSE)
    assert isinstance(r, SlowQueriesResponse)
    assert r.threshold_ms == 0
    assert r.capacity == 200
    assert len(r.queries) == 2
    with_params = r.queries[0]
    assert with_params.started_at_ms == 1700000000000
    assert with_params.duration_ms == 42
    assert with_params.db == "kanban"
    assert with_params.table == "items"
    assert with_params.terminal == "collect"
    assert with_params.params == ["active"]
    redacted = r.queries[1]
    assert redacted.params is None  # omitted on the wire -> None
    assert redacted.terminal == "first"


def test_get_slow_queries_no_filters_omits_params() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["params"] = dict(request.url.params)
        return httpx.Response(200, json=_SLOW_QUERIES_RESPONSE)

    with _sync_client(handler) as c:
        r = c.get_slow_queries()
    assert captured["method"] == "GET"
    assert captured["path"] == "/admin/slow-queries"
    assert captured["params"] == {}
    assert isinstance(r, SlowQueriesResponse)
    assert r.capacity == 200


def test_get_slow_queries_with_db_and_limit_sends_params() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["params"] = dict(request.url.params)
        return httpx.Response(200, json=_SLOW_QUERIES_RESPONSE)

    with _sync_client(handler) as c:
        r = c.get_slow_queries(db="kanban", limit=5)
    assert captured["params"] == {"db": "kanban", "limit": "5"}
    assert isinstance(r, SlowQueriesResponse)
    assert len(r.queries) == 2  # fixture has two rows; server-side filtering is not mocked


async def test_async_get_slow_queries_builds_request() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["auth"] = request.headers["authorization"]
        captured["path"] = request.url.path
        captured["params"] = dict(request.url.params)
        return httpx.Response(200, json=_SLOW_QUERIES_RESPONSE)

    async with _async_client(handler) as c:
        r = await c.get_slow_queries(db="kanban")
    assert captured["auth"] == ADMIN_BEARER
    assert captured["path"] == "/admin/slow-queries"
    assert captured["params"] == {"db": "kanban"}
    assert isinstance(r, SlowQueriesResponse)
    assert r.threshold_ms == 0


# --- workflow surface (FM-29) -------------------------------------------------

_WF_ROW = {
    "id": "wf-1",
    "name": "drip",
    "status": "pending",
    "currentStep": 0,
    "stepCount": 2,
    "attempts": 0,
    "createdAt": 100,
    "updatedAt": 100,
}


def test_admin_list_workflows_gets_with_query_params() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["query"] = str(request.url.params)
        return httpx.Response(200, json={"workflows": [_WF_ROW]})

    with _sync_client(handler) as c:
        rows = c.admin_list_workflows("kanban", status="pending", limit=5)
    assert captured["method"] == "GET"
    assert captured["path"] == "/admin/db/kanban/workflows"
    assert "status=pending" in captured["query"] and "limit=5" in captured["query"]
    assert len(rows) == 1
    assert isinstance(rows[0], WorkflowInfo)
    assert rows[0].step_count == 2

    with _sync_client(handler) as c:
        c.admin_list_workflows("kanban")
    assert captured["query"] == ""


def test_admin_start_workflow_posts_spec_returns_id() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"id": "wf-9"})

    from par_rt_db.wire import WorkflowSpec, WorkflowStepSpec

    txn = Mutation.builder().insert("items", {"name": "x"}).build()
    spec = WorkflowSpec(name="drip", steps=[WorkflowStepSpec(txn=txn.model_dump(by_alias=True))])
    with _sync_client(handler) as c:
        wid = c.admin_start_workflow("kanban", spec)
    assert wid == "wf-9"
    assert captured["method"] == "POST"
    assert captured["path"] == "/admin/db/kanban/workflows"
    assert captured["body"] == {
        "name": "drip",
        "steps": [{"txn": {"steps": [{"op": "insert", "table": "items", "doc": {"name": "x"}}]}}],
    }


def test_admin_get_workflow_returns_full_row() -> None:
    full_row = {
        **_WF_ROW,
        "startedAt": 150,
        "stepOutcomes": [{"stepIndex": 0, "status": "success", "attempts": 1, "at": 150}],
    }
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        return httpx.Response(200, json=full_row)

    with _sync_client(handler) as c:
        full = c.admin_get_workflow("kanban", "wf-1")
    assert captured["path"] == "/admin/db/kanban/workflows/wf-1"
    assert isinstance(full, WorkflowInfoFull)
    assert full.step_outcomes[0].status == "success"
    assert full.started_at == 150


def test_admin_cancel_and_delete_workflow_return_bools() -> None:
    seen: list[tuple[str, str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        seen.append((request.method, request.url.path, body))
        ok = request.method == "POST"  # cancel true, delete false
        return httpx.Response(200, json={"ok": ok})

    with _sync_client(handler) as c:
        cancelled = c.admin_cancel_workflow("kanban", "wf-1")
        deleted = c.admin_delete_workflow("kanban", "wf-1")
    assert cancelled is True
    assert deleted is False  # already-gone delete is a legitimate ok:false
    assert seen == [
        ("POST", "/admin/db/kanban/workflows/wf-1/cancel", {}),
        ("DELETE", "/admin/db/kanban/workflows/wf-1", None),
    ]


async def test_async_admin_workflow_ops() -> None:
    from par_rt_db.wire import WorkflowSpec, WorkflowStepSpec

    txn = Mutation.builder().insert("items", {"name": "x"}).build()
    spec = WorkflowSpec(name="drip", steps=[WorkflowStepSpec(txn=txn.model_dump(by_alias=True))])

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/admin/db/kanban/workflows":
            return httpx.Response(200, json={"workflows": [_WF_ROW]})
        if request.method == "POST" and request.url.path == "/admin/db/kanban/workflows":
            return httpx.Response(200, json={"id": "wf-9"})
        if request.method == "GET" and request.url.path == "/admin/db/kanban/workflows/wf-1":
            return httpx.Response(
                200,
                json={
                    **_WF_ROW,
                    "stepOutcomes": [
                        {"stepIndex": 0, "status": "success", "attempts": 1, "at": 150}
                    ],
                },
            )
        if request.method == "POST" and request.url.path.endswith("/cancel"):
            return httpx.Response(200, json={"ok": True})
        if request.method == "DELETE":
            return httpx.Response(200, json={"ok": False})
        return httpx.Response(404, text=f"no mock for {request.url.path}")

    async with _async_client(handler) as c:
        rows = await c.admin_list_workflows("kanban")
        wid = await c.admin_start_workflow("kanban", spec)
        full = await c.admin_get_workflow("kanban", "wf-1")
        cancelled = await c.admin_cancel_workflow("kanban", "wf-1")
        deleted = await c.admin_delete_workflow("kanban", "wf-1")
    assert [r.id for r in rows] == ["wf-1"]
    assert wid == "wf-9"
    assert full.step_outcomes[0].attempts == 1
    assert cancelled is True
    assert deleted is False


# --- schema preview (dry-run diff) ----------------------------------------


def test_preview_schema_posts_schema_and_parses_diff() -> None:
    captured: dict[str, Any] = {}
    schema = Schema.builder().table("items", lambda tb: tb.field("sku", t.string())).build()

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "added": [
                    {
                        "table": "items",
                        "columns": [{"name": "sku", "fieldType": "string"}],
                        "indexes": [{"name": "by_sku", "fields": ["sku"]}],
                    }
                ],
                "rejected": [{"table": "old", "item": "drop table", "reason": "destructive"}],
            },
        )

    with _sync_client(handler) as c:
        diff = c.preview_schema("dbx", schema)
    assert captured["method"] == "POST"
    assert captured["path"] == "/admin/db/dbx/schema/preview"
    # db rides in the URL, not the body — same shape as push_schema minus the db key
    assert "db" not in captured["body"]
    assert "schema" in captured["body"]
    assert isinstance(diff, SchemaPreviewDiff)
    assert diff.added[0].table == "items"
    assert diff.added[0].columns[0].name == "sku"
    assert diff.added[0].columns[0].field_type == "string"
    assert diff.added[0].indexes[0].fields == ["sku"]
    assert diff.rejected[0].reason == "destructive"


async def test_async_preview_schema_mirrors_sync() -> None:
    captured: dict[str, Any] = {}
    schema = Schema.builder().table("items", lambda tb: tb.field("sku", t.string())).build()

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["auth"] = request.headers["authorization"]
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"added": [], "rejected": []})

    async with _async_client(handler) as c:
        diff = await c.preview_schema("dbx", schema)
    assert captured["auth"] == ADMIN_BEARER
    assert captured["path"] == "/admin/db/dbx/schema/preview"
    assert isinstance(diff, SchemaPreviewDiff)
    assert diff.added == []
    assert diff.rejected == []


# --- admin_query includeDeleted (soft-delete visibility) ------------------


def test_admin_query_omits_include_deleted_when_unset_or_false() -> None:
    """``includeDeleted`` must be ABSENT from the body unless truthy-set — never
    ``null`` (corpus-parity with the server's internal param)."""
    bodies: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(json.loads(request.content))
        return httpx.Response(200, json={"result": []})

    with _sync_client(handler) as c:
        c.admin_query("dbx", TableQuery("items").take(2))
        c.admin_query("dbx", TableQuery("items").take(2), include_deleted=False)
    assert len(bodies) == 2
    for body in bodies:
        assert "includeDeleted" not in body
        assert "query" in body


def test_admin_query_sends_include_deleted_when_true() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"result": [{"_id": "gone", "deleted_at": 5}]})

    with _sync_client(handler) as c:
        docs = c.admin_query("dbx", TableQuery("items").take(2), include_deleted=True)
    assert captured["body"]["includeDeleted"] is True
    assert len(docs) == 1


async def test_async_admin_query_include_deleted_pair() -> None:
    bodies: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(json.loads(request.content))
        return httpx.Response(200, json={"result": []})

    async with _async_client(handler) as c:
        await c.admin_query("dbx", TableQuery("items").take(2))
        await c.admin_query("dbx", TableQuery("items").take(2), include_deleted=True)
    assert "includeDeleted" not in bodies[0]
    assert bodies[1]["includeDeleted"] is True


# --- admin schedule management (GET|POST /admin/db/{db}/schedules) --------


_SCHEDULE_ROW = {
    "id": "sch-1",
    "kind": "oneshot",
    "dueAt": 1700000000000,
    "cron": None,
    "status": "pending",
    "lastError": None,
    "createdAt": 1699999000000,
    "firedCount": 0,
}


def test_admin_list_schedules_parses_rows() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        return httpx.Response(200, json={"schedules": [_SCHEDULE_ROW]})

    with _sync_client(handler) as c:
        rows = c.admin_list_schedules("kanban")
    assert captured["method"] == "GET"
    assert captured["path"] == "/admin/db/kanban/schedules"
    assert len(rows) == 1
    assert isinstance(rows[0], ScheduleInfo)
    assert rows[0].id == "sch-1"
    assert rows[0].kind == "oneshot"
    assert rows[0].due_at == 1700000000000
    assert rows[0].status == "pending"
    assert rows[0].cron is None
    assert rows[0].fired_count == 0


def test_admin_create_schedule_posts_when_and_txn() -> None:
    captured: dict[str, Any] = {}
    txn = Mutation.builder().insert("items", {"name": "x"}).build()

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"id": "sch-9"})

    with _sync_client(handler) as c:
        sid = c.admin_create_schedule("kanban", AfterMs(ms=5000), txn)
    assert sid == "sch-9"
    assert captured["method"] == "POST"
    assert captured["path"] == "/admin/db/kanban/schedules"
    assert captured["body"] == {
        "when": {"type": "afterMs", "ms": 5000},
        "txn": {"steps": [{"op": "insert", "table": "items", "doc": {"name": "x"}}]},
    }


def test_admin_cancel_pause_resume_schedules_hit_paths_and_return_bools() -> None:
    """Three bodyless POSTs; ``ok:false`` (unknown/terminal id) is a legitimate
    ``False`` return, not an error."""
    seen: list[tuple[str, str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        seen.append((request.method, request.url.path, body))
        ok = not request.url.path.endswith("/resume")  # resume misses → ok:false
        return httpx.Response(200, json={"ok": ok})

    with _sync_client(handler) as c:
        cancelled = c.admin_cancel_schedule("kanban", "sch-1")
        paused = c.admin_pause_schedule("kanban", "sch-1")
        resumed = c.admin_resume_schedule("kanban", "sch-1")
    assert cancelled is True
    assert paused is True
    assert resumed is False
    assert seen == [
        ("POST", "/admin/db/kanban/schedules/sch-1/cancel", None),
        ("POST", "/admin/db/kanban/schedules/sch-1/pause", None),
        ("POST", "/admin/db/kanban/schedules/sch-1/resume", None),
    ]


async def test_async_admin_schedule_ops() -> None:
    txn = Mutation.builder().insert("items", {"name": "x"}).build()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/admin/db/kanban/schedules":
            return httpx.Response(200, json={"schedules": [_SCHEDULE_ROW]})
        if request.method == "POST" and request.url.path == "/admin/db/kanban/schedules":
            return httpx.Response(200, json={"id": "sch-9"})
        if request.method == "POST" and request.url.path.endswith("/cancel"):
            return httpx.Response(200, json={"ok": True})
        return httpx.Response(404, text=f"no mock for {request.method} {request.url.path}")

    async with _async_client(handler) as c:
        rows = await c.admin_list_schedules("kanban")
        sid = await c.admin_create_schedule("kanban", Cron(expr="0 * * * *"), txn)
        cancelled = await c.admin_cancel_schedule("kanban", "sch-1")
    assert [r.id for r in rows] == ["sch-1"]
    assert sid == "sch-9"
    assert cancelled is True


# --- admin file storage (GET|POST /admin/db/{db}/storage) -----------------


_FILE_ROW = {
    "id": "f1",
    "sha256": "abc123def",
    "size": 9,
    "contentType": "text/plain",
    "creationTime": 1700000000000,
}


def test_admin_list_files_parses_rows() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        return httpx.Response(200, json={"files": [_FILE_ROW]})

    with _sync_client(handler) as c:
        rows = c.admin_list_files("kanban")
    assert captured["method"] == "GET"
    assert captured["path"] == "/admin/db/kanban/storage"
    assert len(rows) == 1
    assert isinstance(rows[0], FileMetadata)
    assert rows[0].id == "f1"
    assert rows[0].sha256 == "abc123def"
    assert rows[0].size == 9
    assert rows[0].content_type == "text/plain"
    assert rows[0].creation_time == 1700000000000


def test_admin_upload_file_posts_raw_bytes_with_content_type() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["content"] = request.content
        captured["content_type"] = request.headers["content-type"]
        return httpx.Response(200, json={"id": "f9"})

    with _sync_client(handler) as c:
        fid = c.admin_upload_file("kanban", b"raw-bytes", content_type="image/png")
    assert fid == "f9"
    assert captured["method"] == "POST"
    assert captured["path"] == "/admin/db/kanban/storage"
    # the body is the file itself, verbatim — NOT a JSON envelope
    assert captured["content"] == b"raw-bytes"
    assert captured["content_type"] == "image/png"


def test_admin_upload_file_omits_content_type_when_none() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["content"] = request.content
        captured["has_content_type"] = "content-type" in request.headers
        return httpx.Response(200, json={"id": "f10"})

    with _sync_client(handler) as c:
        fid = c.admin_upload_file("kanban", b"untyped")
    assert fid == "f10"
    assert captured["content"] == b"untyped"
    assert captured["has_content_type"] is False


def test_admin_delete_file_hits_route() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        return httpx.Response(200, json={"ok": True})

    with _sync_client(handler) as c:
        result = c.admin_delete_file("kanban", "f1")
    assert result is None
    assert captured["method"] == "DELETE"
    assert captured["path"] == "/admin/db/kanban/storage/f1"


# --- per-db anonymous-access toggle (SEC-103) ------------------------------


def test_anonymous_access_get_and_set() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.setdefault("calls", []).append((request.method, request.url.path))
        if request.method == "GET":
            return httpx.Response(200, json={"enabled": True})
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    with _sync_client(handler) as c:
        enabled = c.get_anonymous_access("kanban")
        result = c.set_anonymous_access("kanban", False)
    assert enabled is True
    assert result is None
    assert captured["calls"] == [
        ("GET", "/admin/db/kanban/anonymous-access"),
        ("PATCH", "/admin/db/kanban/anonymous-access"),
    ]
    assert captured["body"] == {"enabled": False}


async def test_async_anonymous_access_ops() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json={"enabled": False})
        return httpx.Response(200, json={"ok": True})

    async with _async_client(handler) as c:
        enabled = await c.get_anonymous_access("kanban")
        await c.set_anonymous_access("kanban", True)
    assert enabled is False
