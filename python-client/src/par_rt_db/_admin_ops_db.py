"""Database admin request builders — lifecycle (create/delete/list/clone),
export/import, allowlist + admin members, hot config, owner-bypass
query/mutate/changes, per-db toggles (anonymous-access, read-only), and the
two-phase insertOnly purge. Split from ``admin.py`` (QA-003b pure move)."""

from __future__ import annotations

from collections.abc import AsyncIterable, Iterable, Mapping
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from ._admin_request import (
    _AdminRequest,
    _parse_databases,
    _parse_emails,
    _parse_expect_ok,
    _parse_model,
    _parse_model_list,
    _parse_step_results,
    _parse_text,
)
from .admin_models import (
    AdminMember,
    ConfigResponse,
    DbStats,
    HotConfigPatch,
    MetricsSnapshot,
    OpEvent,
    PresenceRoomsResponse,
    PurgePreviewResult,
    PurgeResult,
)
from .mutation import Transaction
from .query import Query, TableQuery, _terminal_of, parse_result
from .wire import ChangeFeedResponse, FilterExpr

if TYPE_CHECKING:
    import httpx


# --- request builders (one per admin operation) ---
#
# Each builder captures the operation's request construction (path interpolation,
# conditional body fields, tri-state sentinels, query-param assembly) AND its
# response parse, so the former four copies collapse to one. The sync/async
# façade classes below delegate to these via the executors.


def _parse_create_db(resp: httpx.Response) -> str:
    _parse_expect_ok(resp)
    return str(resp.json()["creationId"])


def _op_create_db(name: str) -> _AdminRequest:
    return _AdminRequest("POST", "/admin/create-db", {"json": {"name": name}}, _parse_create_db)


def _op_delete_db(
    name: str, confirm: str, expected_creation_id: str | None = None
) -> _AdminRequest:
    body: dict[str, Any] = {"name": name, "confirm": confirm}
    if expected_creation_id is not None:
        body["expectedCreationId"] = expected_creation_id
    return _AdminRequest("POST", "/admin/delete-db", {"json": body}, _parse_expect_ok)


def _op_list_dbs() -> _AdminRequest:
    return _AdminRequest("GET", "/admin/dbs", {}, _parse_databases)


def _op_export_db(db: str) -> _AdminRequest:
    return _AdminRequest(
        "GET", "/admin/export-db", {"params": {"db": db}}, _parse_text, raw_body=True
    )


def _op_import_db(db: str, jsonl: str) -> _AdminRequest:
    return _AdminRequest(
        "POST",
        "/admin/import-db",
        {
            "params": {"db": db},
            "content": jsonl,
            "headers": {"Content-Type": "application/x-ndjson"},
        },
        _parse_expect_ok,
    )


def _op_import_db_stream(db: str, body: Iterable[bytes] | AsyncIterable[bytes]) -> _AdminRequest:
    """Streaming twin of :func:`_op_import_db`: same endpoint, headers and
    response; ``content`` is an iterable of byte chunks sent chunked instead of
    one materialized string."""
    return _AdminRequest(
        "POST",
        "/admin/import-db",
        {
            "params": {"db": db},
            "content": body,
            "headers": {"Content-Type": "application/x-ndjson"},
        },
        _parse_expect_ok,
    )


def _op_clone_db(from_: str, to: str) -> _AdminRequest:
    return _AdminRequest(
        "POST",
        "/admin/clone-db",
        {"params": {"from": from_, "to": to}},
        _parse_expect_ok,
    )


def _op_allowlist_add(db: str, email: str) -> _AdminRequest:
    return _AdminRequest(
        "POST",
        "/admin/allowlist",
        {"json": {"db": db, "action": "add", "email": email}},
        _parse_expect_ok,
    )


def _op_allowlist_remove(db: str, email: str) -> _AdminRequest:
    return _AdminRequest(
        "POST",
        "/admin/allowlist",
        {"json": {"db": db, "action": "remove", "email": email}},
        _parse_expect_ok,
    )


def _op_allowlist_list(db: str) -> _AdminRequest:
    return _AdminRequest("GET", "/admin/allowlist", {"params": {"db": db}}, _parse_emails)


def _op_admins_list() -> _AdminRequest:
    return _AdminRequest("GET", "/admin/admins", {}, _parse_model_list(AdminMember, "admins"))


def _op_admins_add(email: str, github_id: int | None = None) -> _AdminRequest:
    body: dict[str, Any] = {"email": email}
    if github_id is not None:
        body["githubId"] = github_id
    return _AdminRequest("POST", "/admin/admins", {"json": body}, _parse_expect_ok)


def _op_admins_remove(email: str) -> _AdminRequest:
    return _AdminRequest("DELETE", "/admin/admins", {"json": {"email": email}}, _parse_expect_ok)


def _op_db_stats(db: str) -> _AdminRequest:
    return _AdminRequest("GET", f"/admin/dbs/{db}/stats", {}, _parse_model(DbStats))


def _op_metrics() -> _AdminRequest:
    return _AdminRequest("GET", "/admin/metrics", {}, _parse_model(MetricsSnapshot))


def _op_presence_rooms() -> _AdminRequest:
    return _AdminRequest("GET", "/admin/presence", {}, _parse_model(PresenceRoomsResponse))


def _op_ops_recent(
    *,
    db: str | None = None,
    table: str | None = None,
    n: int | None = None,
) -> _AdminRequest:
    params: dict[str, Any] = {}
    if db is not None:
        params["db"] = db
    if table is not None:
        params["table"] = table
    if n is not None:
        params["n"] = n
    return _AdminRequest(
        "GET", "/admin/ops/recent", {"params": params}, _parse_model_list(OpEvent, "ops")
    )


def _op_get_config() -> _AdminRequest:
    return _AdminRequest("GET", "/admin/config", {}, _parse_model(ConfigResponse))


def _op_patch_config(patch: HotConfigPatch | Mapping[str, Any]) -> _AdminRequest:
    if isinstance(patch, Mapping):
        body: dict[str, Any] = dict(patch)
    else:
        body = patch.model_dump(by_alias=True, mode="json", exclude_none=True)
    return _AdminRequest("PATCH", "/admin/config", {"json": body}, _parse_model(ConfigResponse))


def _op_admin_query(
    db: str,
    query: Query | TableQuery,
    *,
    model: type,
    include_deleted: bool | None = None,
) -> _AdminRequest:
    built = query.build() if isinstance(query, TableQuery) else query
    body: dict[str, Any] = {"query": built.model_dump(by_alias=True, mode="json")}
    # ``includeDeleted`` is an internal admin-route param (NOT a wire ``Query``
    # field): only a truthy value puts the key on the wire — never ``null``,
    # absent by default so the server's live-rows-only default applies.
    if include_deleted:
        body["includeDeleted"] = True
    terminal = _terminal_of(built)

    def parse(resp: httpx.Response) -> Any:
        return parse_result(model, terminal, resp.json()["result"])

    return _AdminRequest("POST", f"/admin/db/{db}/query", {"json": body}, parse)


def _op_admin_mutate(
    db: str,
    txn: Transaction,
    *,
    idempotency_key: str | None = None,
) -> _AdminRequest:
    body: dict[str, Any] = {"txn": txn.model_dump(by_alias=True, mode="json")}
    if idempotency_key is not None:
        body["idempotencyKey"] = idempotency_key
    return _AdminRequest("POST", f"/admin/db/{db}/mutate", {"json": body}, _parse_step_results)


def _op_admin_changes(
    db: str,
    since: int = 0,
    *,
    table: str | None = None,
    limit: int | None = None,
) -> _AdminRequest:
    # ``since``/``limit`` default server-side (0 and 500); omitted keys are not
    # sent so the defaults apply. httpx percent-encodes the query params; the
    # path segment is encoded explicitly.
    params: dict[str, Any] = {"since": since}
    if table is not None:
        params["table"] = table
    if limit is not None:
        params["limit"] = limit
    return _AdminRequest(
        "GET",
        f"/admin/db/{quote(db, safe='')}/changes",
        {"params": params},
        _parse_model(ChangeFeedResponse),
    )


# --- per-db anonymous-access toggle (SEC-103) ---


def _op_get_anonymous_access(db: str) -> _AdminRequest:
    return _AdminRequest(
        "GET",
        f"/admin/db/{db}/anonymous-access",
        {},
        lambda resp: bool(resp.json()["enabled"]),
    )


def _op_set_anonymous_access(db: str, enabled: bool) -> _AdminRequest:
    return _AdminRequest(
        "PATCH",
        f"/admin/db/{db}/anonymous-access",
        {"json": {"enabled": enabled}},
        _parse_expect_ok,
    )


# --- per-db read-only freeze ---


def _op_get_read_only(db: str) -> _AdminRequest:
    return _AdminRequest(
        "GET",
        f"/admin/db/{db}/readonly",
        {},
        lambda resp: bool(resp.json()["readOnly"]),
    )


def _op_set_read_only(db: str, read_only: bool) -> _AdminRequest:
    return _AdminRequest(
        "PATCH",
        f"/admin/db/{db}/readonly",
        {"json": {"readOnly": read_only}},
        _parse_expect_ok,
    )


# --- two-phase insertOnly purge (POST /admin/db/{db}/purge) ------------------
#
# Preview (no ``confirm``) → {matching, confirmToken, limit}; confirm (with the
# token) → {deleted, truncated}; repeat while ``truncated`` is True. ``filter``
# is the same wire ``FilterExpr`` a Query's ``filter`` uses; omitted keys are
# not sent so the server defaults apply (whole-table match, limit 10000).


def _purge_body(
    table: str,
    *,
    filter: FilterExpr | None = None,
    limit: int | None = None,
    confirm: str | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {"table": table}
    if filter is not None:
        body["filter"] = filter.model_dump(by_alias=True, mode="json")
    if limit is not None:
        body["limit"] = limit
    if confirm is not None:
        body["confirm"] = confirm
    return body


def _op_purge_preview(
    db: str,
    table: str,
    *,
    filter: FilterExpr | None = None,
    limit: int | None = None,
) -> _AdminRequest:
    return _AdminRequest(
        "POST",
        f"/admin/db/{db}/purge",
        {"json": _purge_body(table, filter=filter, limit=limit)},
        _parse_model(PurgePreviewResult),
    )


def _op_purge_confirm(
    db: str,
    table: str,
    *,
    confirm: str,
    filter: FilterExpr | None = None,
    limit: int | None = None,
) -> _AdminRequest:
    return _AdminRequest(
        "POST",
        f"/admin/db/{db}/purge",
        {"json": _purge_body(table, filter=filter, limit=limit, confirm=confirm)},
        _parse_model(PurgeResult),
    )
